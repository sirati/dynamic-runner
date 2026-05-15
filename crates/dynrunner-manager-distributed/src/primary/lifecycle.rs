use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use dynrunner_core::{ErrorType, Identifier, ResourceMap, TaskInfo};
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, DistributedMessage, PeerTransport,
    SecondaryTransport,
};
use dynrunner_scheduler_api::{
    ResourceEstimator, Scheduler,
};


use crate::cluster_state::apply_locally_for_broadcast;
use super::{PrimaryCoordinator, RemoteWorkerState};
use super::wire::{binary_to_distributed, compute_task_hash, timestamp_now};

/// Order idle worker indices for a single dispatch tick, biasing
/// toward secondaries with fewer currently-running tasks. Stable
/// tie-break by `worker_id` so equal-loaded secondaries fall through
/// to the existing iteration order.
///
/// Pre-fix the flat `0..workers.len()` scan iterated workers grouped
/// by secondary (the order initial-assignment populates them), giving
/// the first-iterated secondary's idle workers systematic priority
/// when both sides had idle capacity. Tail-of-phase dispatches —
/// where the pool has fewer items than there are idle workers — then
/// concentrated remaining work on the already-loaded secondary
/// instead of spreading across the fleet.
pub(super) fn dispatch_order<I: Identifier>(workers: &[RemoteWorkerState<I>]) -> Vec<usize> {
    let mut load_per_secondary: HashMap<&str, usize> = HashMap::new();
    for w in workers {
        if w.current_task.is_some() {
            *load_per_secondary
                .entry(w.secondary_id.as_str())
                .or_default() += 1;
        }
    }
    let mut idle: Vec<usize> = workers
        .iter()
        .enumerate()
        .filter(|(_, w)| w.is_idle)
        .map(|(i, _)| i)
        .collect();
    idle.sort_by_key(|&i| {
        (
            load_per_secondary
                .get(workers[i].secondary_id.as_str())
                .copied()
                .unwrap_or(0),
            workers[i].worker_id,
        )
    });
    idle
}

impl<T: SecondaryTransport<I>, P: PeerTransport<I>, S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<T, P, S, E, I> {
    /// Apply each mutation locally and broadcast the same batch so every
    /// secondary mirrors the change. Per-secondary delivery failures are
    /// logged at warn — the CRDT is idempotent, so a missed mutation is
    /// recoverable from the next snapshot RPC (Phase B); we never block
    /// dispatch on universal delivery.
    pub(super) async fn apply_and_broadcast_cluster_mutations(
        &mut self,
        mutations: Vec<ClusterMutation<I>>,
    ) {
        if mutations.is_empty() {
            return;
        }
        // Apply locally and keep only mutations the CRDT actually
        // changed state for. Pre-fix every mutation was re-broadcast
        // unconditionally; under #50's peer-forwarding redundancy
        // (every peer secondary forwards observed-via-peer-mesh
        // terminal events to the primary), that would amplify each
        // unique TaskComplete into N re-broadcasts to N secondaries
        // = N² messages per event. The CRDT's terminal-lock semantics
        // turn duplicate applies into NoOp; skipping the NoOp arm
        // keeps the wire fan-out at one broadcast per genuinely-new
        // state transition regardless of how many peer-forward
        // paths converge on us. The apply+filter primitive lives in
        // `cluster_state::apply_locally_for_broadcast` so this
        // originator path and the promoted-secondary's mirror
        // (`secondary::primary::apply_and_broadcast_mutations`) share
        // one canonical filter; the broadcast step stays at each call
        // site because the two transports have different error shapes.
        let applied = apply_locally_for_broadcast(&mut self.cluster_state, mutations);
        if applied.is_empty() {
            return;
        }
        let msg = DistributedMessage::ClusterMutation {
            sender_id: self.config.node_id.clone(),
            timestamp: timestamp_now(),
            mutations: applied,
        };
        if let Err(failures) = self.transport.broadcast(msg).await {
            for (secondary_id, error) in &failures {
                tracing::warn!(
                    secondary = %secondary_id,
                    error = %error,
                    "ClusterMutation broadcast delivery failed"
                );
            }
        }
    }

    /// Phase-S/B: seed the replicated cluster ledger with the run's
    /// task graph and phase-dependency graph. Emits one
    /// `PhaseDepsSet` (carrying the canonical per-run dep graph)
    /// followed by one `TaskAdded` per binary in `all_binaries`; the
    /// originator-side `apply_and_broadcast_cluster_mutations` applies
    /// locally and ships the batch to every secondary.
    ///
    /// `PhaseDepsSet` rides ahead of `TaskAdded` so receivers'
    /// `cluster_state.phase_deps()` is populated before any
    /// post-promotion hydration that consults it. The mutation is
    /// idempotent (re-application is a no-op when local is non-empty),
    /// so multiple snapshot sources or duplicate broadcasts are safe.
    ///
    /// Called once at run start, after every secondary has connected
    /// (so `transport.broadcast` reaches the full fleet) and before
    /// `perform_initial_assignment` runs (so the originator's mirror
    /// is non-empty when the first dispatch happens).
    pub(super) async fn seed_cluster_state(&mut self) {
        let mut mutations: Vec<ClusterMutation<I>> = Vec::with_capacity(
            self.all_binaries.len() + 1,
        );
        mutations.push(ClusterMutation::PhaseDepsSet {
            deps: self.phase_deps.clone(),
        });
        mutations.extend(
            self.all_binaries
                .iter()
                .map(|b| ClusterMutation::TaskAdded {
                    hash: compute_task_hash(b),
                    task: b.clone(),
                }),
        );
        let task_count = self.all_binaries.len();
        self.apply_and_broadcast_cluster_mutations(mutations).await;
        tracing::info!(tasks = task_count, "seeded cluster ledger");
    }

    pub(super) async fn send_transfer_complete(&mut self) -> Result<(), String> {
        let msg = DistributedMessage::TransferComplete {
            sender_id: self.config.node_id.clone(),
            timestamp: timestamp_now(),
            total_files: 0,
            total_bytes: 0,
        };
        if let Err(failures) = self.transport.broadcast(msg).await {
            for (secondary_id, error) in &failures {
                tracing::warn!(
                    secondary = %secondary_id,
                    error = %error,
                    "TransferComplete delivery failed"
                );
            }
            return Err(format!(
                "TransferComplete broadcast failed for {} secondaries",
                failures.len()
            ));
        }
        tracing::info!("transfer complete sent to all secondaries");
        Ok(())
    }

    // ── Phase 7: Operational Loop ──

    pub(super) async fn operational_loop(&mut self) -> Result<(), String> {
        tracing::info!("entering operational loop");

        let mut heartbeat_tick =
            tokio::time::interval(self.config.keepalive_interval);
        // Skip the immediate first tick — secondaries might not have sent
        // their first keepalive yet at the moment we enter the loop.
        heartbeat_tick.tick().await;

        // One-shot gates on the two recv arms. Each flips true the
        // first time its channel returns `None`. Mirrors
        // `SecondaryCoordinator.primary_disconnected` (see
        // `secondary/processing.rs:75`): a closed mpsc receiver
        // resolves immediately on every subsequent poll, so leaving
        // an arm enabled after the first None would hot-loop the
        // select!. The timer arms still drive every subsequent loop
        // iteration so the top-of-loop exit checks (counter-based,
        // pool-drained, `cluster_state.run_complete()`) can still
        // trip. Resets are intentionally absent — once a bridge has
        // exited it cannot re-open mid-run.
        let mut transport_closed = false;
        let mut peer_transport_closed = false;

        // Cross-thread command-channel receiver. Owned locally for the
        // duration of the loop so the `&mut self.command_rx` borrow
        // doesn't conflict with the per-arm handlers' `&mut self`. Put
        // back on `self` at loop exit so subsequent operational-loop
        // entries (retry passes) re-attach to the same channel — the
        // PyO3 `PrimaryHandle` only clones the sender once before
        // `run()` starts and expects its commands to keep being
        // serviced across retry pass boundaries.
        let mut command_rx = self.command_rx.take();

        // Matcher-trigger receiver. Same shape + lifetime as
        // `command_rx`: taken out for the loop's duration so the
        // `drain_matcher_batch` await can borrow it without
        // conflicting with the per-arm `&mut self` borrows, then put
        // back at loop exit so subsequent operational-loop entries
        // (retry passes) keep draining the same channel. `None` when
        // a previous run already consumed it (single-shot lifecycle
        // — same handling as `command_rx`).
        let mut matcher_trigger_rx = self.matcher_trigger_rx.take();
        // One-shot gate on the matcher arm. Flips true on
        // `rx.recv() == None` (every sender dropped); subsequent
        // poll attempts would resolve immediately and hot-loop the
        // select. Mirrors the `transport_closed` / `peer_transport_closed`
        // gates above.
        let mut matcher_arm_closed = false;

        loop {
            // Check termination: all tasks accounted for AND no
            // worker is mid-dispatch. Both halves of the check are
            // necessary — counting `completed + failed >= total`
            // alone would orphan in-flight tasks if the bookkeeping
            // ever inflates (e.g. a TaskComplete arriving for a task
            // primary doesn't currently track as in-flight on a
            // worker — the insert grows the set while the in-flight
            // ledger stays as-is, so the counter check trips while a
            // sibling worker is still mid-dispatch and primary tears
            // down before that sibling's TaskComplete arrives).
            // Pairing the counter check with `active_workers == 0`
            // guarantees we only exit when every dispatched
            // assignment has been reconciled.
            let active_workers = self.workers.iter().filter(|w| w.current_task.is_some()).count();
            // Counter-based exit. Gates:
            //
            //   (a) `!self.setup_pending` — the historical setup-defer
            //       guard: in setup-promote mode (`required_setup_on_promote
            //       = true`) the local enters the loop with `total_tasks
            //       = 0` and the chosen secondary still owes its first
            //       TaskAdded broadcast; without this gate `0+0 >= 0`
            //       trips immediately. Cleared by the first TaskAdded
            //       or RunComplete mirror — see
            //       `mirror_mutation_to_accounting`.
            //
            //   (b) `!(self.demoted && self.config.required_setup_on_promote)`
            //       — the LMU CIP partial-CRDT-view guard. The local
            //       submitter is always `demoted = true` post-bootstrap
            //       (`promote_primary` unconditionally hands off to the
            //       first secondary, see `lifecycle.rs:981`), so the
            //       demoted-flag alone cannot gate the counter exit
            //       without breaking every normal run. The bug only
            //       manifests when the demoted's view is also PARTIAL
            //       — i.e. `required_setup_on_promote = true` so
            //       `seed_cluster_state` never ran locally and the
            //       local learns task counts only from out-of-order
            //       TaskAdded broadcasts. In that regime
            //       `total_tasks` and `completed_tasks.len()` can
            //       transiently align (e.g. 50 TaskAddeds arrive,
            //       then 50 TaskCompleteds arrive before the next
            //       TaskAdded batch — `50 + 0 >= 50` trips while
            //       185 items are still unaccounted-for upstream).
            //       For these setup-promoted demoted primaries the
            //       ONLY safe exit is `cluster_state.run_complete()`
            //       (the authoritative primary's terminal broadcast).
            //
            //       Pre-seeded mode (`required_setup_on_promote =
            //       false` — local does discovery + seeds
            //       `cluster_state` BEFORE handing off to the
            //       promoted secondary; a fully production-supported
            //       path, not a deprecated one) is unaffected: even
            //       when demoted, the local's view was fully seeded
            //       by `seed_cluster_state` before the operational
            //       loop started, so `total_tasks = binaries.len()`
            //       is set once at run start and never drifts under
            //       partial CRDT updates. The counter exit is the
            //       load-bearing happy-path exit for every
            //       pre-seeded run; gating it on `!self.demoted`
            //       would break every distributed run.
            //
            // Concrete bug this guard kills (asm-tokenizer LMU CIP
            // `--jobs 15`, 50ms+ tunnel RTT, `--source-already-staged`):
            // the setup-promoted secondary discovers 235 items,
            // broadcasts batched TaskAdded + interleaved
            // TaskCompleted; the demoted local's counter check
            // momentarily aligns and the loop exits with `total=N,
            // succeeded=N` before the upstream run is anywhere near
            // done. The local dispatcher then chains to Phase 2 and
            // tears down Phase 1's tunnels — killing Phase 1.
            let partial_view = self.demoted && self.config.required_setup_on_promote;
            if !partial_view
                && !self.setup_pending
                && self.completed_tasks.len() + self.failed_tasks.len() >= self.total_tasks
                && active_workers == 0
            {
                tracing::info!("all tasks completed or failed");
                break;
            }

            // Drain check: pool's `is_run_complete` returns true iff
            // queued + in-flight is zero AND no phase is Active or
            // Draining. The active-workers guard catches the edge
            // where in-flight is zero but a worker hasn't reported
            // completion yet (mostly defensive — `on_item_finished`
            // runs synchronously off the wire message).
            //
            // Same `partial_view` guard as the counter exit above:
            // a setup-promoted demoted primary's pool is also a
            // stale local view (the authoritative pool lives on the
            // promoted secondary, and the local pool stays empty
            // because TaskAdded mirrors into cluster_state, not into
            // the local pool — only the live primary's pool ingests
            // staged items). Legacy bootstrap's pool was seeded
            // pre-loop and drains normally.
            if !partial_view && !self.setup_pending && self.pool().is_run_complete() && active_workers == 0 {
                tracing::info!("pool drained and no active workers");
                break;
            }

            // Replicated-ledger run-complete signal. The promoted
            // primary broadcasts `ClusterMutation::RunComplete` as the
            // last act before its own `run()` returns; `handle_cluster_mutation`
            // applies it to our `cluster_state` mirror.
            //
            // For a setup-promoted demoted primary (`partial_view`
            // = true above) this is the SOLE exit cue — the local
            // counter / pool views are partial and unreliable until
            // RunComplete proves the authoritative primary has
            // accounted for every task. RunComplete is causally
            // ordered after every TaskCompleted / TaskFailed in the
            // run, so by the time we apply it to our mirror those
            // mutations have already updated `completed_tasks` /
            // `failed_tasks` — the "primary finished succeeded=X
            // fail_retry=X ..." log line at the demoted exit reflects
            // the true final state.
            //
            // Pre-seeded demoted primary (`required_setup_on_promote
            // = false`): RunComplete is a redundant exit (the
            // counter check above trips first, since the local was
            // fully seeded by `seed_cluster_state` and TaskCompleteds
            // from every peer's worker arrive on the per-peer
            // SecondaryTransport / peer transport before the promoted
            // primary itself decides RunComplete). Keeping this arm
            // unguarded is harmless and serves as a uniform fallback.
            //
            // Sticky monotonic flag, so this fires at most once
            // per run.
            if self.cluster_state.run_complete() && active_workers == 0 {
                tracing::info!("RunComplete signal received from cluster; exiting");
                break;
            }

            // Fleet-dead detection. When every secondary has been
            // declared dead (via `requeue_dead_secondary`) and the
            // pool still has pending work, the loop would otherwise
            // sit forever waiting for events that no living
            // secondary can send. Track the first moment the fleet
            // is empty-but-pool-has-work; after
            // `config.fleet_dead_timeout` of continuous emptiness,
            // exit cleanly with pending tasks marked failed so the
            // operator gets a clear failure rather than a silent
            // idle. Cleared the moment a secondary is present
            // again (re-handshake / partial fleet survival).
            //
            // Tokenizer surfaced this on cohort-3 where SSH-tunnel
            // blips killed all 5 secondaries at once and the run
            // sat idle until manually killed.
            if self.secondaries.is_empty() && !self.pool().is_empty() {
                let now = Instant::now();
                let since = *self.fleet_dead_since.get_or_insert(now);
                let elapsed = now.duration_since(since);
                if elapsed >= self.config.fleet_dead_timeout {
                    // Drain the queued pool so the pool's own bookkeeping
                    // doesn't pretend work is still pending after we
                    // exit. The drained binaries deliberately do NOT
                    // land in `failed_tasks` — they were never
                    // dispatched, no secondary attempted them, and no
                    // worker reported a failure. Final accounting in
                    // `run()` classifies anything that's neither
                    // completed nor failed as `stranded`, which is
                    // exactly the right category for "cluster died
                    // before this task could even be tried". Pre-fix,
                    // pushing into `failed_tasks` here conflated two
                    // distinct outcomes (worker-reported failure vs
                    // never-dispatched) and exhausted the retry budget
                    // for tasks that hadn't actually failed.
                    let pending = self.pool_mut().drain_queued();
                    tracing::error!(
                        elapsed_s = elapsed.as_secs_f64(),
                        timeout_s = self.config.fleet_dead_timeout.as_secs_f64(),
                        marking_stranded = pending.len(),
                        "fleet-dead timeout: every secondary gone with non-empty pool; \
                         pending tasks left stranded and exiting operational loop"
                    );
                    break;
                }
            } else {
                // Fleet recovered (or never went empty); clear the
                // grace-period clock so a subsequent fleet-dead
                // event measures from its own start, not an old one.
                self.fleet_dead_since = None;
            }

            // Both inbound paths closed: no further mutations can
            // arrive on either source, so the demoted-primary view is
            // frozen. The pre-Step-6 behaviour (transport-closed →
            // immediate break) is preserved structurally for the
            // pathological "every channel died" case. The
            // `cluster_state.run_complete()` check above is the
            // happy-path exit; this guard is only reached when the
            // mesh itself has collapsed.
            if transport_closed && peer_transport_closed {
                tracing::info!(
                    "both transport and peer_transport closed; exiting operational loop"
                );
                break;
            }

            // Use a timeout on recv to avoid stalling indefinitely if a
            // secondary disconnects while processing a task. The timeout
            // is generous — if no message arrives in 5 minutes and there
            // are in-flight tasks, something is wrong.
            //
            // Cancellation safety: `transport.recv` is the mpsc-bridged
            // `NetworkServer::recv` (cancel-safe — see `MessageReceiver`
            // doc). `peer_transport.recv_peer` is the mpsc-backed
            // tunneled inbound queue (also cancel-safe — see
            // `TunneledPeerTransport::recv_peer`). The two timer arms
            // (heartbeat tick + 5-min sleep) are tokio time primitives
            // which are themselves cancel-safe.
            tokio::select! {
                msg = self.transport.recv(), if !transport_closed => {
                    match msg {
                        Some(m) => self.dispatch_message(m).await?,
                        None => {
                            // Legacy `transport.recv()` returned None —
                            // the per-secondary SecondaryTransport bridge
                            // exited. Two structural cases:
                            //
                            // 1. Pre-demotion / no mesh: the legacy
                            //    transport is the only inbound path. The
                            //    historical "transport close = end of
                            //    run" semantics apply; exit cleanly.
                            //
                            // 2. Post-demotion (`self.demoted == true`)
                            //    with a live peer mesh: the legacy
                            //    transport's writer task to the promoted
                            //    secondary has shut down per the
                            //    PromotePrimary contract, but the demoted
                            //    local is still a real mesh member (Step
                            //    5b `TunneledPeerTransport`). The new
                            //    primary's ClusterMutation /
                            //    Keepalive / TaskCompleted broadcasts
                            //    arrive on the peer_transport arm below;
                            //    the loop's exit cues (counter check,
                            //    pool-drained, RunComplete) are all
                            //    driven by mutations the peer arm feeds
                            //    through `dispatch_message`. Breaking
                            //    here would re-introduce bug class #1
                            //    (asm-tokenizer "succeeded=0 + 235 CSVs
                            //    landed") and #79 (chain-gate reading
                            //    stale 0/0/0).
                            //
                            // The architectural invariant
                            // (`feedback_mesh_independent_of_role_and_membership`):
                            // mesh state is transport-independent of any
                            // single legacy channel.
                            transport_closed = true;
                            if self.peer_transport.peer_count() > 0 {
                                tracing::info!(
                                    peer_count = self.peer_transport.peer_count(),
                                    demoted = self.demoted,
                                    "legacy transport closed; staying in operational \
                                     loop — peer mesh still active, mutations and \
                                     RunComplete will arrive via peer_transport"
                                );
                                continue;
                            }
                            tracing::info!("transport closed");
                            break;
                        }
                    }
                }
                cmd = async {
                    match command_rx.as_mut() {
                        Some(rx) => rx.recv().await,
                        // No command channel attached — park forever
                        // so this arm never fires. A `None` from the
                        // `recv()` future would otherwise hot-loop the
                        // select! the same way a closed mpsc would.
                        None => std::future::pending().await,
                    }
                } => {
                    match cmd {
                        Some(command) => {
                            // Delegate to the per-variant handler.
                            // Each handler owns its CRDT broadcast and
                            // its oneshot reply, so the call site stays
                            // a single line and the operational loop's
                            // arm shape stays transport-pure.
                            super::command_channel::handle_primary_command(
                                self,
                                command,
                            )
                            .await;
                        }
                        None => {
                            // All senders dropped. Drop the receiver
                            // locally; the loop's other arms keep
                            // driving exit conditions. Pre-Step-N this
                            // arm didn't exist, so a `None` here is
                            // semantically the same as the pre-Step-N
                            // behaviour (no external control plane).
                            command_rx = None;
                            tracing::debug!(
                                "command channel closed; disabling \
                                 PrimaryCommand arm for the remainder \
                                 of the loop"
                            );
                        }
                    }
                }
                batch = async {
                    match matcher_trigger_rx.as_mut() {
                        Some(rx) => {
                            crate::fulfillability_matcher::drain_matcher_batch(
                                rx,
                                crate::fulfillability_matcher::MATCHER_BATCH_IDLE_WINDOW,
                            ).await
                        }
                        // No receiver attached — park forever so the
                        // arm never fires. Mirrors the command_rx
                        // arm's `pending().await` for the same
                        // closed-channel hot-loop reason.
                        None => std::future::pending().await,
                    }
                }, if !matcher_arm_closed => {
                    match batch {
                        Some(batch) => {
                            // Single-line delegation: the walk +
                            // matcher invocation + auto-fire of
                            // ReinjectTask lives in
                            // `primary/fulfillability_matcher.rs`.
                            // This arm's only concern is "a batch
                            // arrived; hand it off".
                            self.invoke_fulfillability_matcher_batch(batch).await;
                        }
                        None => {
                            // Every sender dropped. Same as the
                            // command channel's None arm: disable
                            // this arm and let the timer / counter
                            // exit cues take over.
                            matcher_arm_closed = true;
                            tracing::debug!(
                                "matcher-trigger channel closed; disabling \
                                 the fulfillability-matcher arm for the \
                                 remainder of the loop"
                            );
                        }
                    }
                }
                peer_msg = self.peer_transport.recv_peer(), if !peer_transport_closed => {
                    match peer_msg {
                        Some(m) => {
                            // Same dispatcher the legacy arm uses. Post-
                            // demotion the new primary's broadcasts
                            // (`ClusterMutation::TaskCompleted`,
                            // `ClusterMutation::RunComplete`, Keepalive,
                            // etc.) arrive here; threading them through
                            // `dispatch_message` keeps a single source
                            // of truth for wire-shape handling.
                            //
                            // Idempotency: every mutation a peer might
                            // also forward via `transport` is dedup-
                            // gated downstream — `cluster_state.apply`
                            // is CRDT-idempotent, the `completed_tasks`
                            // / `failed_tasks` HashSet inserts are
                            // idempotent, and `handle_task_complete`
                            // already short-circuits on
                            // `completed_tasks.contains(hash)`. Safe
                            // by construction; no extra dedup needed
                            // at this layer.
                            self.dispatch_message(m).await?;
                        }
                        None => {
                            // Peer transport closed (only when every
                            // TunneledPeerTransport writer has gone
                            // away, or for `NoPeerTransport` the future
                            // never resolves so this branch is
                            // unreachable). Gate the arm so subsequent
                            // select! iterations don't hot-poll a
                            // permanently-resolved future. The legacy
                            // arm and the timer arms still drive exit
                            // conditions; the top-of-loop checks
                            // (`run_complete`, counter-based) can still
                            // break.
                            peer_transport_closed = true;
                            tracing::debug!(
                                "peer_transport.recv_peer() returned None; \
                                 disabling the arm for the remainder of the loop"
                            );
                        }
                    }
                }
                _ = heartbeat_tick.tick() => {
                    self.broadcast_primary_keepalive().await;
                    // Demoted observer mode: the promoted
                    // primary owns dead-secondary detection and the
                    // associated requeue. If the local primary also
                    // requeued, the same in-flight task would be
                    // re-dispatched twice (once from each primary's
                    // pool view) — duplicating work and racing on
                    // ledger state. We still send keepalives so peers
                    // know we're alive, but skip the requeue path.
                    //
                    // `process_heartbeat_tick` runs the per-tick
                    // mass-death-aware death evaluator: resolve any
                    // already-deferred secondaries (recovery or grace
                    // expiry), then categorise newly-dead ones as
                    // either correlated (defer) or independent
                    // (requeue). See `process_heartbeat_tick` for
                    // detail and `PrimaryConfig.mass_death_grace` for
                    // the disable knob.
                    if !self.demoted {
                        self.process_heartbeat_tick().await?;
                    }
                }
                _ = tokio::time::sleep(Duration::from_secs(300)) => {
                    let active = self.workers.iter().filter(|w| w.current_task.is_some()).count();
                    if active > 0 {
                        let outcome = self.outcome_summary();
                        tracing::warn!(
                            active_workers = active,
                            succeeded = outcome.succeeded,
                            fail_retry = outcome.fail_retry,
                            fail_oom = outcome.fail_oom,
                            fail_final = outcome.fail_final,
                            total = self.total_tasks,
                            "operational loop timeout; marking in-flight tasks failed"
                        );
                        // Mark all in-flight tasks as failed. These
                        // are workers that didn't ack progress for the
                        // operational-loop timeout window — typically
                        // a stuck worker or wedged transport. Classify
                        // as Recoverable so `run_retry_passes` gets a
                        // chance to re-dispatch them; if they wedge
                        // the same way on retry, the retry-budget
                        // exhaustion path takes over.
                        for worker in &mut self.workers {
                            if let Some(binary) = worker.current_task.take() {
                                let hash = compute_task_hash(&binary);
                                self.failed_tasks
                                    .insert(hash, ErrorType::Recoverable);
                                worker.estimated_resources = ResourceMap::new();
                                worker.is_idle = true;
                            }
                        }
                    }
                }
            }
        }

        // Return the command-channel receiver to `self` so subsequent
        // operational-loop entries (retry passes) re-attach. Without
        // this, the second pass would see `command_rx = None` and the
        // PyO3 `PrimaryHandle` calls would be silently dropped from the
        // moment the first pass exits.
        self.command_rx = command_rx;
        // Same rationale for the matcher-trigger receiver: retry
        // passes re-enter the operational loop and must keep draining
        // the same channel so holdings-update bursts during retry
        // passes still drive the matcher.
        self.matcher_trigger_rx = matcher_trigger_rx;

        Ok(())
    }

    /// After the main operational loop drains, run up to
    /// `config.retry_max_passes` retry passes. Each pass drains the
    /// retriable subset of `failed_tasks` (everything except
    /// `ErrorType::Unfulfillable`) into the pool, kicks dispatch to
    /// all currently-idle workers, then runs the operational loop
    /// again. Tasks that fail on the retry pass land back in
    /// `failed_tasks` for the next iteration — or stay there
    /// permanently if the retry budget is exhausted. No-op when
    /// `failed_tasks` is empty or contains only Unfulfillable entries.
    ///
    /// `ErrorType::Unfulfillable` is the operator-resolvable failure
    /// class (CRDT terminal state `TaskState::Unfulfillable`) and is
    /// reinjected exclusively via `PrimaryCommand::ReinjectTask` —
    /// the auto-retry pass deliberately leaves those entries in
    /// `failed_tasks` so the run-done counter check still trips and
    /// the operator's reinject command remains the sole path back to
    /// `Pending`. Worker-reported Unfulfillable failures and
    /// `apply_fail_permanent`-broadcast Unfulfillable cascades both
    /// land in `failed_tasks` with this kind; the filter here is the
    /// single chokepoint that keeps them isolated from the pass-
    /// counter retry channel.
    ///
    /// The proactive idle-worker dispatch step is required because
    /// secondaries only send `TaskRequest` after they finish a task;
    /// after the main pass drains every worker is idle but already
    /// sent its last TaskRequest (which got `nothing-to-do` because
    /// the failed task wasn't in the pool yet). Without the
    /// kickstart the re-injected binaries would sit in the pool
    /// forever waiting for a TaskRequest that never comes.
    pub(super) async fn run_retry_passes(&mut self) -> Result<(), String> {
        // Demoted observer mode: the promoted primary owns
        // retry orchestration. The local primary's `failed_tasks`
        // set still receives forwarded outcomes (so the run-done
        // counter check trips when everything is accounted for),
        // but re-injecting tasks into the local pool and
        // dispatching them would create the very parallel-dispatch
        // race demotion is meant to eliminate. See `demoted` doc
        // on `PrimaryCoordinator`.
        if self.demoted {
            return Ok(());
        }
        for pass_idx in 0..self.config.retry_max_passes {
            // Partition `failed_tasks` into retriable vs operator-
            // only buckets. Unfulfillable entries (operator-resolvable
            // failures) stay in `failed_tasks` so end-of-run
            // accounting and the loop-exit counter check still see
            // them; only the retriable subset (Recoverable,
            // ResourceExhausted, NonRecoverable) feeds the pool
            // reinjection step. Walking the HashMap with `drain` keeps
            // the partition atomic with respect to the operational
            // loop (no concurrent producer modifies `failed_tasks`
            // between the partition and the operational_loop call —
            // the loop's `select!` is the only producer and it hasn't
            // started yet for this pass).
            //
            // ErrorType from the previous pass is discarded for
            // retriable entries; a task's terminal classification is
            // the one from its last observed failure. Unfulfillable
            // entries retain their ErrorType because they're skipped
            // entirely.
            let mut retriable: HashMap<String, ErrorType> = HashMap::new();
            let mut unfulfillable: HashMap<String, ErrorType> = HashMap::new();
            for (hash, kind) in self.failed_tasks.drain() {
                if matches!(kind, ErrorType::Unfulfillable { .. }) {
                    unfulfillable.insert(hash, kind);
                } else {
                    retriable.insert(hash, kind);
                }
            }
            // Restore the operator-only entries before any early-exit
            // path so end-of-run accounting still observes them.
            self.failed_tasks = unfulfillable;

            if retriable.is_empty() {
                break;
            }

            let to_retry: Vec<TaskInfo<I>> = self
                .all_binaries
                .iter()
                .filter(|b| retriable.contains_key(&compute_task_hash(b)))
                .cloned()
                .collect();

            tracing::info!(
                pass = pass_idx + 1,
                count = to_retry.len(),
                "retry pass: re-injecting failed tasks"
            );

            for binary in to_retry {
                // Re-inject preserves phase state (flips Drained/Done
                // back to Active for this phase) so the operational
                // loop re-engages with the items.
                self.pool_mut().reinject(binary);
            }

            // Proactively dispatch to every idle worker before
            // entering the operational loop — see method-level
            // comment for the rationale.
            self.dispatch_to_idle_workers().await?;

            // The operational loop will dispatch the re-injected
            // tasks, observe their TaskComplete / TaskFailed
            // outcomes, and exit when the pool drains again. Tasks
            // that fail in this pass land in `failed_tasks` for the
            // next iteration of THIS for-loop.
            self.operational_loop().await?;
        }

        if !self.failed_tasks.is_empty() {
            tracing::warn!(
                permanent_failures = self.failed_tasks.len(),
                passes = self.config.retry_max_passes,
                "retry budget exhausted; tasks permanently failed"
            );
        }

        Ok(())
    }

    /// Final-stage drain: between the operational loop's exit and the
    /// stranded-task accounting in `run()`, any `TaskComplete` /
    /// `TaskFailed` messages that crossed the wire while the loop was
    /// winding down (transport.recv hadn't yielded them yet, or they
    /// arrived in the gap between an exit-condition trip and the next
    /// recv tick) must be dispatched through the same handlers the
    /// loop used so `completed_tasks` / `failed_tasks` reflect every
    /// outcome the cluster actually produced. Without this drain, the
    /// counter-based stranded computation (`total - completed -
    /// failed`) reads pre-drain values and surfaces successful
    /// completions as `stranded`, flipping clean runs into
    /// `RunError::ClusterCollapsed` with a non-zero exit at the PyO3
    /// boundary.
    ///
    /// Drain shape: peek at the transport with a short per-iteration
    /// timeout; quiet means "nothing else in transit" and the drain
    /// terminates. Total time is bounded by `budget` (overall) AND
    /// the per-iteration `quiet_window` (each empty poll). On a
    /// happy-path run with no in-flight messages this returns after
    /// one `quiet_window` (~50ms); on a heavily-pipelined run with
    /// dozens of pending TaskCompletes it processes them sequentially
    /// at network speed.
    ///
    /// Reuses `dispatch_message` so every message type the operational
    /// loop knew how to handle is handled identically here — no parallel
    /// switch-statement, no special-cased subset. The drain is a
    /// post-loop continuation, not a different code path.
    pub(super) async fn drain_pending_messages(
        &mut self,
        budget: Duration,
    ) -> Result<(), String> {
        let deadline = Instant::now() + budget;
        let quiet_window = Duration::from_millis(50);
        let mut drained = 0usize;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            let poll_window = std::cmp::min(quiet_window, remaining);
            match tokio::time::timeout(poll_window, self.transport.recv()).await {
                Ok(Some(msg)) => {
                    self.dispatch_message(msg).await?;
                    drained += 1;
                }
                Ok(None) => {
                    // Transport closed — no more messages will ever
                    // arrive. Drain is complete.
                    break;
                }
                Err(_) => {
                    // Quiet window elapsed without a message. Treat as
                    // drained: the operational loop's prior recv tick
                    // already pulled everything in flight, the brief
                    // quiet window confirms nothing else is racing in.
                    break;
                }
            }
        }
        if drained > 0 {
            let outcome = self.outcome_summary();
            tracing::info!(
                drained,
                succeeded = outcome.succeeded,
                fail_retry = outcome.fail_retry,
                fail_oom = outcome.fail_oom,
                fail_final = outcome.fail_final,
                "drained pending wire messages before final accounting"
            );
        }
        Ok(())
    }

    /// Iterate every idle worker and dispatch a task from the pool
    /// if one fits. Used by `run_retry_passes` to kickstart dispatch
    /// after re-injection (workers won't send a fresh TaskRequest on
    /// their own — see the run_retry_passes comment). Mirrors the
    /// per-worker logic in `handle_task_request` minus the
    /// primary relay (which is irrelevant for the
    /// non-promoted-primary at this stage).
    pub(super) async fn dispatch_to_idle_workers(&mut self) -> Result<(), String> {
        // Demoted observer mode: the promoted primary is the
        // sole authority for dispatch. Returning here covers every
        // call site (kickstart from handle_task_complete /
        // handle_task_failed, plus the retry-pass kickstart) without
        // sprinkling `if !self.demoted` across the message-handling
        // code. See `demoted` doc on `PrimaryCoordinator`.
        if self.demoted {
            return Ok(());
        }
        // Visit idle workers in load-aware order so a secondary with
        // many in-flight tasks doesn't keep winning tail-of-phase
        // dispatches against an idler peer. `dispatch_order` filters
        // to idle and sorts by (busy-workers-on-secondary, worker_id).
        let order = dispatch_order(&self.workers);
        for worker_idx in order {
            // Skip workers belonging to a secondary that's currently
            // in backpressure backoff — see
            // `backpressured_secondaries` doc on `PrimaryCoordinator`.
            // Without this, the kickstart would re-target the same
            // unresponsive secondary in a tight loop, which is
            // exactly the failure storm 07ae301-followup is
            // designed to break. Re-checked inline (not pre-filtered
            // in `dispatch_order`) so a backpressure window that
            // opens mid-tick takes effect immediately.
            if self.is_backpressured(&self.workers[worker_idx].secondary_id) {
                continue;
            }
            let global_wid = self.workers[worker_idx].worker_id;
            let view = self.cap_filter_view(self.pool().view_for_worker(global_wid));
            if view.is_empty() {
                continue;
            }
            let worker_info = self.workers[worker_idx].budget_info();
            let all_infos: Vec<dynrunner_scheduler_api::WorkerBudgetInfo<I>> =
                self.workers.iter().map(|w| w.budget_info()).collect();
            let max_res = self.workers[worker_idx].resource_budgets.clone();

            let decision = self.scheduler.assign_normal(
                &worker_info,
                &all_infos,
                view.as_slice(),
                &max_res,
                &self.estimator,
                false,
            );

            if let dynrunner_scheduler_api::AssignmentDecision::Assign {
                binary_index,
                estimated_usage,
                ..
            } = decision
            {
                let binary = self.pool_mut().take_from_view(view, binary_index);
                self.reserve_type_slot(&binary.type_id);
                let sec_id = self.workers[worker_idx].secondary_id.clone();
                let local_worker_id = self.workers[..worker_idx + 1]
                    .iter()
                    .filter(|w| w.secondary_id == sec_id)
                    .count() as u32
                    - 1;
                self.workers[worker_idx].current_task = Some(binary.clone());
                self.workers[worker_idx].estimated_resources = estimated_usage.clone();
                self.workers[worker_idx].is_idle = false;

                let task_hash = compute_task_hash(&binary);
                let assignment_msg = DistributedMessage::TaskAssignment {
                    sender_id: self.config.node_id.clone(),
                    timestamp: timestamp_now(),
                    secondary_id: sec_id.clone(),
                    worker_id: local_worker_id,
                    zip_file: None,
                    binary_info: binary_to_distributed(&binary),
                    local_path: self.config.wire_local_path(&binary),
                    file_hash: task_hash.clone(),
                };

                // Transport-send failure rollback: pre-fix the
                // `await?` returned Err with the worker's
                // `current_task` set, `is_idle = false`, and the
                // pool's `in_flight_per_phase` bumped (via the
                // earlier `take_from_view` call) — but the task
                // itself never reached the peer. The primary's
                // view permanently believed the slot was busy,
                // `dispatch_order` skipped it forever, and the
                // leaked in_flight slot never decremented (no
                // TaskComplete / TaskFailed will ever arrive for
                // a task that wasn't sent). Cumulative leaks
                // explain asm-tokenizer's "33 in_flight with
                // active=0" jam at 84f669c.
                //
                // Rollback symmetry: revert worker state,
                // requeue the binary back to the FRONT of its
                // bucket (matches `handle_primary_peer_rejection`),
                // release the type slot, and `continue` the
                // dispatch loop so other idle workers still get a
                // chance this tick. WARN so an operator grepping
                // for the jam symptom sees the proximate cause.
                if let Err(send_err) =
                    self.transport.send_to(&sec_id, assignment_msg).await
                {
                    tracing::warn!(
                        secondary = %sec_id,
                        worker_id = local_worker_id,
                        task_hash = %task_hash,
                        error = %send_err,
                        "task-assignment send failed; rolling back worker state and requeuing binary"
                    );
                    self.workers[worker_idx].current_task = None;
                    self.workers[worker_idx].estimated_resources =
                        ResourceMap::new();
                    self.workers[worker_idx].is_idle = true;
                    self.release_type_slot(&binary.type_id);
                    self.pool_mut().requeue(binary);
                    continue;
                }

                // Operator-facing INFO: which secondary/worker just
                // took the task. Per-task identity (task_id /
                // phase / type) → DEBUG sibling.
                tracing::info!(
                    secondary = %sec_id,
                    worker_id = local_worker_id,
                    task_hash = %task_hash,
                    "task assigned"
                );
                tracing::debug!(
                    secondary = %sec_id,
                    worker_id = local_worker_id,
                    task_id = ?binary.task_id,
                    phase = %binary.phase_id,
                    task_type = %binary.type_id,
                    task_hash = %task_hash,
                    "task assigned: identity"
                );
            }
        }
        Ok(())
    }

    // ── Phase 6.5: Wait for secondary peer-meshes to settle ──

    /// Block on every connected secondary reporting `MeshReady`
    /// before letting `promote_primary` fire. The 750µs gap
    /// between "all secondaries cert-exchanged" and the previous
    /// promotion call left the promoted secondary
    /// authoritative against a still-forming peer mesh — every
    /// pre-mesh-formation message went into the void for the
    /// 30s peer-dial budget. Closing the gap means waiting until
    /// each secondary has signalled its mesh has settled (mesh
    /// formed, watchdog elapsed, or no peers were expected for
    /// single-secondary).
    ///
    /// Bounded by `config.mesh_ready_timeout` (default 60s):
    /// stragglers past the deadline log a warning and the run
    /// proceeds anyway. A buggy secondary that never emits
    /// `MeshReady` must not be able to deadlock the entire
    /// dispatch pipeline; the post-promotion paths are all
    /// already failure-tolerant against an absent peer.
    ///
    /// Cancellation safety: `transport.recv` is the cancel-safe
    /// mpsc bridge; `sleep_until` is one-shot cancel-safe per
    /// tokio docs. The `select!` here mirrors the same shape
    /// `wait_for_connections` uses one phase up.
    pub(super) async fn wait_for_mesh_ready(&mut self) -> Result<(), String> {
        // The expected set is the live-secondaries set captured
        // AT this moment (post-quorum, post-cert-exchange). It is
        // not `config.num_secondaries` because the connect phase
        // may have dropped no-show secondaries on its own
        // timeout — we only wait for who's actually here.
        let expected: HashSet<String> = self.secondaries.keys().cloned().collect();
        if expected.is_empty() {
            tracing::debug!("no secondaries connected; skipping wait_for_mesh_ready");
            return Ok(());
        }

        // Fast path: messages may have already arrived before this
        // step ran (the welcome/cert-exchange/peer-info loop above
        // is event-driven and a fast secondary can emit MeshReady
        // before we enter the wait).
        if expected.is_subset(&self.mesh_ready_secondaries) {
            tracing::info!(
                count = expected.len(),
                "all secondaries reported MeshReady before wait step"
            );
            return Ok(());
        }

        let deadline = tokio::time::Instant::now() + self.config.mesh_ready_timeout;
        tracing::info!(
            expected = expected.len(),
            already_reported = self.mesh_ready_secondaries.len(),
            timeout_s = self.config.mesh_ready_timeout.as_secs_f64(),
            "waiting for peer-mesh formation across secondary fleet before \
             promoting primary"
        );

        loop {
            if expected.is_subset(&self.mesh_ready_secondaries) {
                tracing::info!(
                    count = expected.len(),
                    "all secondaries reported MeshReady; releasing PromotePrimary"
                );
                return Ok(());
            }

            tokio::select! {
                msg = self.transport.recv() => {
                    match msg {
                        Some(m) => self.dispatch_message(m).await?,
                        None => return Err("transport closed during wait_for_mesh_ready".into()),
                    }
                }
                _ = tokio::time::sleep_until(deadline) => {
                    let missing: Vec<String> = expected
                        .difference(&self.mesh_ready_secondaries)
                        .cloned()
                        .collect();
                    tracing::warn!(
                        missing = ?missing,
                        reported = self.mesh_ready_secondaries.len(),
                        expected = expected.len(),
                        timeout_s = self.config.mesh_ready_timeout.as_secs_f64(),
                        "mesh-ready timeout: some secondaries never reported \
                         MeshReady; proceeding with PromotePrimary anyway. The \
                         promoted secondary may briefly route into a \
                         partially-formed peer mesh until those secondaries \
                         finish (or fail) their dials."
                    );
                    return Ok(());
                }
            }
        }
    }

    // ── Promote primary (atomic role-flip) ──

    pub(super) async fn promote_primary(&mut self) -> Result<(), String> {
        if let Some(first_id) = self.secondaries.keys().next().cloned() {
            self.primary_id = Some(first_id.clone());
            // Monotonic per-promotion epoch carried on the wire and
            // fed into `ClusterState::PrimaryChanged`'s last-writer-
            // wins resolver. Starting from the local mirror's current
            // epoch + 1 is sufficient at the bootstrap promotion
            // (epoch starts at 0 cluster-wide); the failover election
            // protocol's own `round` will supersede this when it
            // re-elects.
            let new_epoch = self.cluster_state.primary_epoch() + 1;
            tracing::info!(primary = %first_id, epoch = new_epoch, "promoting secondary to primary");

            // Apply locally so the originator's mirror flips atomically
            // with the broadcast, not after the broadcast round-trips
            // back to us. Lower-epoch races no-op against the higher
            // epoch we just installed.
            self.cluster_state.apply(ClusterMutation::PrimaryChanged {
                new: first_id.clone(),
                epoch: new_epoch,
            });

            let msg = DistributedMessage::<I>::PromotePrimary {
                sender_id: self.config.node_id.clone(),
                timestamp: timestamp_now(),
                new_primary_id: first_id.clone(),
                epoch: new_epoch,
                // Bootstrap-promote discriminator: when this primary
                // skipped `seed_cluster_state` + `perform_initial_assignment`
                // (setup-defer mode driven by `--source-already-staged`,
                // i.e. `required_setup_on_promote = true`), the chosen
                // secondary needs to know it's the one doing discovery
                // + ledger seed after promotion. The election/failover
                // sites in `secondary/election.rs` unconditionally
                // pass `false` because, by election time, the local
                // ledger is already non-empty (seeded either by the
                // original setup-defer secondary or by a pre-seeded
                // submitter — `required_setup_on_promote = false`,
                // a fully production-supported path), so re-running
                // discovery would double-seed.
                required_setup: self.config.required_setup_on_promote,
            };
            // Broadcast to every secondary, not unicast to the elected
            // node: every secondary needs the role-change to update
            // its `primary_link` routing target and clear its per-
            // worker backoff so idle workers re-issue at the new
            // primary on their next tick (otherwise the new primary's
            // peers stay quiet for one stale-window). The pre-Phase-P
            // unicast was Bug 2 in the trace at `feb1052` — only the
            // elected node logged the role change because only it
            // received the message.
            if let Err(failures) = self.transport.broadcast(msg).await {
                for (secondary_id, error) in &failures {
                    tracing::warn!(
                        secondary = %secondary_id,
                        error = %error,
                        "PromotePrimary broadcast delivery failed"
                    );
                }
            }

            // Hand-off complete: the local primary stops being
            // authoritative the moment `PromotePrimary` is on the
            // wire. We stay alive (transport open, message loop
            // still runs) so completion forwards keep
            // `completed_tasks` accurate for the run-done counter
            // check in `operational_loop`, but we no longer
            // dispatch, kickstart, or drive heartbeat-based
            // requeue — the promoted secondary owns all of that.
            // Without this, the local primary and the promoted
            // secondary both act as primaries simultaneously and
            // their parallel dispatch paths race for the same
            // workers. See `demoted` doc on `PrimaryCoordinator`.
            self.demoted = true;
            // The demoted local primary is NOT an observer — observers
            // are first-class members of `RoleTable.observers` (Step 7,
            // Decision G) with `is_observer=true` set at startup. A
            // demoted primary stays a regular member; it just no
            // longer drives dispatch. Prior log wording conflated the
            // two concepts.
            tracing::info!(
                primary = %first_id,
                "local primary demoted; promoted secondary is sole authoritative primary"
            );
        }
        Ok(())
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::time::Duration;

    use dynrunner_core::ErrorType;
    use dynrunner_scheduler::ResourceStealingScheduler;
    use dynrunner_scheduler_api::PendingPool;
    use dynrunner_transport_channel::ChannelSecondaryTransportEnd;
    use tokio::sync::oneshot;

    use crate::cluster_state::TaskState;
    use crate::primary::command_channel::{
        handle_primary_command, PrimaryCommand,
    };
    use crate::primary::test_helpers::{
        make_binary, setup_test, FixedEstimator, NoPeers, TestId,
    };
    use crate::primary::wire::compute_task_hash;
    use crate::primary::{PrimaryConfig, PrimaryCoordinator};

    /// Stand-alone fixture matching the shape used by
    /// `command_channel::tests::make_coordinator`: a `PrimaryCoordinator`
    /// over an in-process channel transport stub with zero connected
    /// secondaries, suitable for driving `run_retry_passes` /
    /// `apply_reinject_task` directly without a full operational loop.
    fn make_coordinator(
        retry_max_passes: u32,
    ) -> PrimaryCoordinator<
        ChannelSecondaryTransportEnd<TestId>,
        NoPeers,
        ResourceStealingScheduler,
        FixedEstimator,
        TestId,
    > {
        let (transport, _secondary_ends) = setup_test(0);
        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: 0,
            connect_timeout: Duration::from_secs(1),
            peer_timeout: Duration::from_secs(1),
            keepalive_interval: Duration::from_millis(100),
            keepalive_miss_threshold: 3,
            source_pre_staged_root: None,
            uses_file_based_items: false,
            required_setup_on_promote: false,
            max_concurrent_per_type: HashMap::new(),
            retry_max_passes,
            fleet_dead_timeout: Duration::from_secs(1),
            mesh_ready_timeout: Duration::from_secs(1),
            mass_death_grace: Duration::from_secs(1),
            mass_death_min_count: 2,
            source_dir: None,
            unfulfillable_reinject_max_per_task: None,
        };
        PrimaryCoordinator::new(
            config,
            transport,
            NoPeers,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        )
    }

    /// Seed `coordinator.pending` with a default-phase pool that owns
    /// the supplied binary's phase. Required so `pool.reinject(binary)`
    /// has a phase entry to flip back to Active.
    fn install_pool_for_phase(
        coordinator: &mut PrimaryCoordinator<
            ChannelSecondaryTransportEnd<TestId>,
            NoPeers,
            ResourceStealingScheduler,
            FixedEstimator,
            TestId,
        >,
        binary: &dynrunner_core::TaskInfo<TestId>,
    ) {
        let mut phase_set = std::collections::HashSet::new();
        phase_set.insert(binary.phase_id.clone());
        coordinator.pending = Some(
            PendingPool::<TestId>::new(phase_set, HashMap::new())
                .expect("pool init"),
        );
        coordinator
            .phase_completed
            .insert(binary.phase_id.clone(), 0);
        coordinator
            .phase_failed
            .insert(binary.phase_id.clone(), 0);
    }

    /// `run_retry_passes` must NOT reinject entries whose
    /// `ErrorType` is `Unfulfillable { .. }`. Those are the operator-
    /// resolvable failure class — `TaskState::Unfulfillable` in the
    /// CRDT — and reinjection is reserved for the explicit
    /// `PrimaryCommand::ReinjectTask` path. Pre-fix, the snapshot
    /// `mem::take(&mut self.failed_tasks)` drained EVERY entry
    /// (including Unfulfillable) into the pool, sidestepping the
    /// per-task `unfulfillable_reinject_max_per_task` budget that
    /// gates the operator path. This test pins the partition.
    #[tokio::test(flavor = "current_thread")]
    async fn retry_pass_skips_unfulfillable_failures() {
        let local = tokio::task::LocalSet::new();
        local.run_until(async {
            let mut coordinator = make_coordinator(/* retry_max_passes = */ 3);

            // Seed two binaries: one Unfulfillable, one Recoverable.
            // `all_binaries` is the lookup table `run_retry_passes`
            // uses to map hashes back to dispatchable `TaskInfo`s.
            let unfulfillable_bin = make_binary("operator-only", 50);
            let recoverable_bin = make_binary("retriable", 40);
            let unfulfillable_hash = compute_task_hash(&unfulfillable_bin);
            let recoverable_hash = compute_task_hash(&recoverable_bin);
            coordinator.all_binaries =
                vec![unfulfillable_bin.clone(), recoverable_bin.clone()];

            install_pool_for_phase(&mut coordinator, &unfulfillable_bin);

            coordinator.failed_tasks.insert(
                unfulfillable_hash.clone(),
                ErrorType::Unfulfillable {
                    reason: "missing toolchain".to_string().into(),
                },
            );
            coordinator
                .failed_tasks
                .insert(recoverable_hash.clone(), ErrorType::Recoverable);

            // No connected secondaries + no in-flight workers ⇒ the
            // operational loop run inside the retry pass returns
            // immediately (counter check trips at
            // `0 + len(failed) >= 0` for total_tasks=0 with
            // active_workers=0). That's enough to observe the
            // partition behaviour: the recoverable entry is drained
            // into the pool, the Unfulfillable entry stays in
            // `failed_tasks`.
            coordinator.run_retry_passes().await.unwrap();

            // Retriable entry was drained and reinjected (would land
            // back in `failed_tasks` only if the operational loop
            // observed another failure — with zero workers no such
            // observation can happen).
            assert!(
                !coordinator.failed_tasks.contains_key(&recoverable_hash),
                "retry pass should drain Recoverable entries from \
                 failed_tasks before reinjecting"
            );

            // Unfulfillable entry stayed — and kept its ErrorType so
            // end-of-run accounting still classifies it correctly.
            match coordinator.failed_tasks.get(&unfulfillable_hash) {
                Some(ErrorType::Unfulfillable { reason }) => {
                    assert_eq!(reason.as_ref(), "missing toolchain");
                }
                other => panic!(
                    "Unfulfillable entry must remain in failed_tasks; \
                     got {other:?}"
                ),
            }
        }).await;
    }

    /// Pin the cleanup invariant ReinjectTask depends on: the local
    /// `failed_tasks` HashMap entry for a hash is removed when
    /// `apply_reinject_task` transitions the CRDT from
    /// `TaskState::Unfulfillable` back to `Pending`. Without this,
    /// the operational loop's `completed + failed >= total` exit
    /// would trip on a hash that's been re-armed for dispatch — and
    /// any subsequent `run_retry_passes` pass would see a stale entry
    /// claiming the task still owes a retry.
    #[tokio::test(flavor = "current_thread")]
    async fn reinject_clears_failed_tasks_entry_for_hash() {
        let local = tokio::task::LocalSet::new();
        local.run_until(async {
            let mut coordinator = make_coordinator(/* retry_max_passes = */ 0);

            let binary = make_binary("op-resolvable", 50);
            let hash = compute_task_hash(&binary);
            install_pool_for_phase(&mut coordinator, &binary);
            coordinator.all_binaries = vec![binary.clone()];

            // Pre-state: worker reported Unfulfillable. The CRDT
            // lands in `TaskState::Unfulfillable`; the local
            // `failed_tasks` mirror records the same kind.
            coordinator.cluster_state.apply(
                dynrunner_protocol_primary_secondary::ClusterMutation::TaskAdded {
                    hash: hash.clone(),
                    task: binary.clone(),
                },
            );
            coordinator.cluster_state.apply(
                dynrunner_protocol_primary_secondary::ClusterMutation::TaskFailed {
                    hash: hash.clone(),
                    kind: ErrorType::Unfulfillable {
                        reason: "missing toolchain".to_string().into(),
                    },
                    error: "unfulfillable".into(),
                },
            );
            coordinator.failed_tasks.insert(
                hash.clone(),
                ErrorType::Unfulfillable {
                    reason: "missing toolchain".to_string().into(),
                },
            );

            // Operator dispatches the reinject command.
            let (reply_tx, reply_rx) = oneshot::channel();
            handle_primary_command(
                &mut coordinator,
                PrimaryCommand::ReinjectTask {
                    hash: hash.clone(),
                    reply: reply_tx,
                },
            )
            .await;
            assert!(
                reply_rx.await.unwrap().is_ok(),
                "reinject should accept Unfulfillable entry"
            );

            // Post-state: the local `failed_tasks` mirror no longer
            // claims this hash failed — the operational loop's
            // exit-counter sees the entry as in-flight / pending
            // again, matching the CRDT's transition to Pending.
            assert!(
                !coordinator.failed_tasks.contains_key(&hash),
                "reinject must clear failed_tasks[hash]"
            );
            assert!(matches!(
                coordinator.cluster_state.task_state(&hash),
                Some(TaskState::Pending { .. })
            ));
        }).await;
    }

    /// Full round-trip: Unfulfillable failure → operator reinjects →
    /// task fails again as Recoverable → next `run_retry_passes` pass
    /// picks the hash up exactly like any other Recoverable failure.
    /// Pins that the per-task retry channel is independent of the
    /// per-task ReinjectTask channel — burning the operator's
    /// `unfulfillable_reinject_remaining` ticket does not consume
    /// any of the run-wide pass-counter budget, and the new
    /// `ErrorType::Recoverable` entry rides the retry pass with a
    /// fresh ledger ErrorType (no Unfulfillable carry-over).
    #[tokio::test(flavor = "current_thread")]
    async fn unfulfillable_reinjected_task_can_use_retry_pass() {
        let local = tokio::task::LocalSet::new();
        local.run_until(async {
            let mut coordinator = make_coordinator(/* retry_max_passes = */ 2);

            let binary = make_binary("round-trip", 50);
            let hash = compute_task_hash(&binary);
            install_pool_for_phase(&mut coordinator, &binary);
            coordinator.all_binaries = vec![binary.clone()];

            // Step 1: Unfulfillable failure observed.
            coordinator.cluster_state.apply(
                dynrunner_protocol_primary_secondary::ClusterMutation::TaskAdded {
                    hash: hash.clone(),
                    task: binary.clone(),
                },
            );
            coordinator.cluster_state.apply(
                dynrunner_protocol_primary_secondary::ClusterMutation::TaskFailed {
                    hash: hash.clone(),
                    kind: ErrorType::Unfulfillable {
                        reason: "missing toolchain".to_string().into(),
                    },
                    error: "unfulfillable".into(),
                },
            );
            coordinator.failed_tasks.insert(
                hash.clone(),
                ErrorType::Unfulfillable {
                    reason: "missing toolchain".to_string().into(),
                },
            );

            // Step 2: operator reinjects.
            let (reply_tx, reply_rx) = oneshot::channel();
            handle_primary_command(
                &mut coordinator,
                PrimaryCommand::ReinjectTask {
                    hash: hash.clone(),
                    reply: reply_tx,
                },
            )
            .await;
            assert!(reply_rx.await.unwrap().is_ok());
            assert!(!coordinator.failed_tasks.contains_key(&hash));

            // Step 3: task re-runs and fails Recoverably this time
            // (the operator's resource provisioning worked, but the
            // re-attempted execution hit a generic transient error).
            // The CRDT-side state machine takes Pending → Failed{
            // Recoverable } via the Pending arm of TaskFailed.
            coordinator.cluster_state.apply(
                dynrunner_protocol_primary_secondary::ClusterMutation::TaskFailed {
                    hash: hash.clone(),
                    kind: ErrorType::Recoverable,
                    error: "transient".into(),
                },
            );
            coordinator
                .failed_tasks
                .insert(hash.clone(), ErrorType::Recoverable);

            // Step 4: retry-pass drains the Recoverable entry into
            // the pool. The fresh Recoverable kind — not the carried
            // Unfulfillable — is what determines retry-pass
            // eligibility.
            coordinator.run_retry_passes().await.unwrap();

            // The hash was drained (no zero-worker re-failure can
            // re-populate it), confirming the retry pass picked it up.
            assert!(
                !coordinator.failed_tasks.contains_key(&hash),
                "Recoverable failure on a previously-reinjected hash \
                 must still be retry-pass-eligible"
            );
        }).await;
    }
}
