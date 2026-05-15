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
            //       Legacy bootstrap (`required_setup_on_promote =
            //       false`) is unaffected: even when demoted, the
            //       local's view was fully seeded by
            //       `seed_cluster_state` before the operational loop
            //       started, so `total_tasks = binaries.len()` is
            //       set once at run start and never drifts under
            //       partial CRDT updates. The counter exit is the
            //       load-bearing happy-path exit for every legacy-
            //       mode run; gating it on `!self.demoted` would
            //       break every distributed run.
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
            // Legacy-bootstrap demoted primary: RunComplete is a
            // redundant exit (the counter check above trips first,
            // since the local was fully seeded by
            // `seed_cluster_state` and TaskCompleteds from every
            // peer's worker arrive on the legacy / peer transport
            // before the promoted primary itself decides
            // RunComplete). Keeping this arm unguarded is harmless
            // and serves as a uniform fallback.
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

        Ok(())
    }

    /// After the main operational loop drains, run up to
    /// `config.retry_max_passes` retry passes. Each pass takes the
    /// current `failed_tasks` set, re-injects every matching binary
    /// from `all_binaries` into the pool (clearing the set), kicks
    /// dispatch to all currently-idle workers, then runs the
    /// operational loop again. Tasks that fail on the retry pass
    /// land back in `failed_tasks` for the next iteration — or stay
    /// there permanently if the retry budget is exhausted. No-op
    /// when `failed_tasks` is empty.
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
            if self.failed_tasks.is_empty() {
                break;
            }

            // Snapshot the failed-task hashes, clear the ledger,
            // then re-inject each matching binary into the pool.
            // The `failed_tasks` map tracks "currently considered
            // failed" (with last-observed ErrorType) — by clearing
            // it here we let the upcoming operational loop populate
            // it fresh with the (smaller) set of tasks that fail in
            // THIS retry pass, each carrying that pass's outcome
            // class. ErrorType from the previous pass is discarded;
            // a task's terminal classification is the one from its
            // last observed failure.
            let to_retry_entries: HashMap<String, ErrorType> =
                std::mem::take(&mut self.failed_tasks);
            let to_retry: Vec<TaskInfo<I>> = self
                .all_binaries
                .iter()
                .filter(|b| to_retry_entries.contains_key(&compute_task_hash(b)))
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
                // (setup-defer mode driven by `--source-already-staged`),
                // the chosen secondary needs to know it's the one
                // doing discovery + ledger seed after promotion. The
                // election/failover sites in `secondary/election.rs`
                // unconditionally pass `false` because, by election
                // time, the local ledger is already non-empty (seeded
                // either by the original setup-defer secondary or by
                // a legacy submitter), so re-running discovery would
                // double-seed.
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
