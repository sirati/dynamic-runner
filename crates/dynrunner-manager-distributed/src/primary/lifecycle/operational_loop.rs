use std::time::{Duration, Instant};

use dynrunner_core::{ErrorType, Identifier};
use dynrunner_protocol_primary_secondary::{
    PeerTransport,
    SecondaryTransport,
};
use dynrunner_scheduler_api::{
    ResourceEstimator, Scheduler,
};

use crate::primary::PrimaryCoordinator;
use crate::primary::wire::compute_task_hash;



impl<T: SecondaryTransport<I>, P: PeerTransport<I>, S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<T, P, S, E, I> {

    /// Run-completion exit decision for one operational-loop iteration.
    ///
    /// Returns `true` iff the loop should break this iteration on a
    /// "the run is done" condition. This is a FAITHFUL extraction of
    /// the three run-completion branches that previously lived inline
    /// at the top of `operational_loop` — the counter exit, the
    /// pool-drain exit, and the replicated-ledger `RunComplete` exit.
    /// Their semantics are unchanged: each is re-evaluated against
    /// live coordinator state (`total_tasks`, the pool, `cluster_state`)
    /// every call, so lazy `spawn_tasks` / `TasksSpawned` growth is
    /// naturally absorbed.
    ///
    /// The failure/abort exits (fleet-dead timeout, both-transports-
    /// closed, panik, setup-promote deadline, stuck-worker watchdog)
    /// are a DIFFERENT concern (the run did not complete normally) and
    /// stay inline in the loop — they are not run-completion decisions.
    ///
    /// `partial_view` (see the extensive rationale below) keys off
    /// `self.demoted`, NOT off `required_setup_on_promote` alone and
    /// NOT off a sticky latch: a composed AUTHORITATIVE primary is by
    /// definition `!self.demoted`, so it evaluates `partial_view =
    /// false` and uses the real counter / pool-drain exits even when
    /// `required_setup_on_promote = true`. Only a DEMOTED submitter
    /// that also deferred discovery holds a genuinely partial CRDT
    /// mirror and must wait for the authoritative `RunComplete`.
    pub(crate) fn run_complete_check(&self) -> bool {
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
        let active_workers = self.workers.iter().filter(|w| !w.is_idle()).count();
        // Counter-based exit: every task accounted for (completed or
        // failed) and no worker mid-dispatch. Re-read every iteration
        // so lazy `spawn_tasks` / `TasksSpawned` growth is absorbed.
        if self.completed_tasks.len() + self.failed_tasks.len() >= self.total_tasks
            && active_workers == 0
        {
            tracing::info!("all tasks completed or failed");
            return true;
        }

        // Drain check: pool's `is_run_complete` returns true iff
        // queued + in-flight is zero AND no phase is Active or
        // Draining. The active-workers guard catches the edge
        // where in-flight is zero but a worker hasn't reported
        // completion yet (mostly defensive — `on_item_finished`
        // runs synchronously off the wire message).
        if self.pool().is_run_complete() && active_workers == 0 {
            tracing::info!("pool drained and no active workers");
            return true;
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
            return true;
        }

        false
    }

    pub(crate) async fn operational_loop(&mut self) -> Result<(), String> {
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

        // Respawn-request receiver. Same shape + lifetime as
        // `command_rx`: taken out for the duration of the loop so the
        // arm's `recv().await` can borrow it without conflicting with
        // the per-arm `&mut self` borrows. `None` when the respawn
        // policy is disabled at construction (no spawner, no budget,
        // no channel) — the arm parks on `pending().await` in that
        // case, matching the command-channel disabled-arm shape.
        let mut respawn_request_rx = self.respawn_request_rx.take();

        // Panik-watcher signal receiver. Same shape as the closed-
        // channel arms above: taken out for the loop's duration so
        // the awaiting arm owns the receiver across `select!`
        // iterations, parked on `pending().await` when None
        // (operator passed no `--panik-file` paths) or once the
        // signal has already fired+consumed.
        //
        // Unlike `command_rx` / `matcher_trigger_rx`, this is a
        // ONESHOT receiver — the watcher resolves exactly once
        // (matched-file detection OR sender drop on abort). After
        // resolution we set the local to None so subsequent
        // iterations re-park on `pending().await`, mirroring the
        // secondary's panik arm.
        let mut panik_signal_rx = self.panik_signal_rx.take();

        loop {
            // Run-completion exit decision. The counter exit, the
            // pool-drain exit, and the replicated-ledger RunComplete
            // exit are extracted into `run_complete_check` (a single-
            // concern predicate, byte-for-byte the same conditions
            // that previously lived inline here). The failure/abort
            // exits below (fleet-dead, both-transports-closed, panik,
            // setup-promote deadline, stuck-worker watchdog) are a
            // different concern and stay in the loop.
            if self.run_complete_check() {
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
                        Some(m) => self.dispatch_message(m, &mut command_rx).await?,
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
                            crate::primary::command_channel::handle_primary_command(
                                self,
                                command,
                                &mut command_rx,
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
                            self.dispatch_message(m, &mut command_rx).await?;
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
                    // `process_heartbeat_tick` runs the per-tick
                    // mass-death-aware death evaluator: resolve any
                    // already-deferred secondaries (recovery or grace
                    // expiry), then categorise newly-dead ones as
                    // either correlated (defer) or independent
                    // (requeue). See `process_heartbeat_tick` for
                    // detail and `PrimaryConfig.mass_death_grace` for
                    // the disable knob.
                    self.process_heartbeat_tick().await?;
                }
                req = async {
                    match respawn_request_rx.as_mut() {
                        Some(rx) => rx.recv().await,
                        // Respawn policy disabled (or rx already
                        // consumed by a prior loop entry): park
                        // forever so this arm never fires. Mirrors
                        // the `command_rx` / `matcher_trigger_rx`
                        // closed-channel hot-loop guard.
                        None => std::future::pending().await,
                    }
                } => {
                    match req {
                        Some(request) => {
                            // Single-line delegation: the dispatch
                            // logic (budget check + id mint +
                            // spawner invocation + JoinSet push)
                            // lives in `primary::respawn` /
                            // `dispatch_respawn_request`. This arm
                            // only translates "a request arrived"
                            // into the call.
                            self.dispatch_respawn_request(request);
                        }
                        None => {
                            // Every sender dropped. Drop the
                            // receiver locally; the loop's other
                            // arms keep driving exit conditions.
                            // Same shape as the command-channel
                            // None arm.
                            respawn_request_rx = None;
                            tracing::debug!(
                                "respawn request channel closed; disabling \
                                 the respawn-request arm for the remainder \
                                 of the loop"
                            );
                        }
                    }
                }
                outcome = async {
                    // `JoinSet::join_next` returns `None` when the
                    // set is empty. To avoid hot-looping the select!
                    // when no respawn is in flight, park on
                    // `pending().await` while the JoinSet is empty.
                    // The arm wakes again on the next iteration as
                    // soon as a respawn task is pushed.
                    if self.respawn_tasks.is_empty() {
                        std::future::pending().await
                    } else {
                        self.respawn_tasks.join_next().await
                    }
                } => {
                    self.handle_respawn_join(outcome);
                }
                // Panik (operator-initiated emergency stop) arm. The
                // watcher's `oneshot::Receiver<PanikSignal>` resolves
                // exactly once: with `Ok(signal)` on first-matching
                // panik file, or with `Err(_)` if the watcher's
                // sender was dropped (empty paths config or task
                // abort on coordinator drop). On `Ok` we announce a
                // self-authored `ClusterMutation::PeerRemoved
                // { SelfDeparture }` (observability only) and stash the
                // (matched_path, reason) on `self.panik_outcome` so
                // the outer `run_pipeline` can translate it into
                // `RunError::PanikShutdown`. Breaking out of the
                // loop here mirrors the `transport_closed` /
                // `peer_transport_closed` exit shape: the operational
                // loop's `Result<(), String>` signature does not need
                // to change.
                //
                // `Err(_)` is treated as a no-op (watcher disabled or
                // gracefully stopped); the loop continues. Setting
                // the local to None on either branch prevents the
                // resolved-already future from hot-looping the
                // select.
                panik = async {
                    match panik_signal_rx.as_mut() {
                        Some(rx) => rx.await,
                        None => std::future::pending().await,
                    }
                } => {
                    panik_signal_rx = None;
                    if let Ok(signal) = panik {
                        let outcome = self
                            .handle_panik_signal(signal.matched_path)
                            .await;
                        self.panik_outcome = Some(outcome);
                        // Break the operational loop. The outer
                        // `run_pipeline` consumes `panik_outcome`
                        // after the loop returns Ok and surfaces
                        // `RunError::PanikShutdown` to the caller.
                        tracing::warn!(
                            "primary operational loop exiting via panik path"
                        );
                        break;
                    }
                }
                _ = tokio::time::sleep(Duration::from_secs(300)),
                    if !self.single_worker_mode() => {
                    // 5-min stuck-worker watchdog. Disabled while
                    // the OOM retry bucket is active: a single-
                    // worker-per-secondary pass on a memory-pressed
                    // workload can legitimately exceed 5 minutes,
                    // and the blanket Recoverable re-tag here would
                    // poison the bucket's hand-tuned dispatch shape
                    // mid-flight (every in-flight task gets re-
                    // classified Recoverable, the OOM-bucket
                    // accounting goes off the rails). The flag
                    // [`single_worker_mode`] is the gate;
                    // `try_run_phase_retry_bucket` is its sole
                    // writer. Outside the OOM bucket the arm runs
                    // unchanged.
                    let active = self.workers.iter().filter(|w| !w.is_idle()).count();
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
                        // as Recoverable so a later retry gets a chance
                        // to re-dispatch them; if they wedge the same
                        // way on retry, the retry-budget exhaustion path
                        // takes over.
                        //
                        // Snapshot every held slot's
                        // `(secondary, local_worker_id, hash)` first
                        // (an immutable borrow), then free each through
                        // the single `free_slot_on_terminal` helper so
                        // the slot, the `in_flight` ledger entry, and
                        // the per-type concurrency slot all release
                        // together — the same terminal path TaskComplete
                        // / TaskFailed use, keyed by the held hash. The
                        // re-tag into `failed_tasks` preserves the
                        // pre-existing "mark Recoverable, don't decrement
                        // the phase counter at loop-exit" behaviour
                        // (this arm only fires on the timeout-exit edge).
                        let held: Vec<(String, u32, String)> = {
                            let mut out = Vec::new();
                            let mut local_idx: std::collections::HashMap<&str, u32> =
                                std::collections::HashMap::new();
                            for w in &self.workers {
                                let lw = local_idx
                                    .entry(w.secondary_id.as_str())
                                    .or_insert(0);
                                let this_lw = *lw;
                                *lw += 1;
                                if let Some(task) = w.held_task() {
                                    out.push((
                                        w.secondary_id.clone(),
                                        this_lw,
                                        compute_task_hash(task),
                                    ));
                                }
                            }
                            out
                        };
                        for (secondary_id, local_worker_id, hash) in held {
                            self.failed_tasks
                                .insert(hash.clone(), ErrorType::Recoverable);
                            self.free_slot_on_terminal(
                                &secondary_id,
                                local_worker_id,
                                &hash,
                            );
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
        // Same rationale for the respawn-request receiver: retry
        // passes re-enter the operational loop and a death observed
        // during a retry pass should still drive the dispatcher.
        self.respawn_request_rx = respawn_request_rx;
        // Same rationale for the panik-signal receiver: a retry
        // pass that re-enters the operational loop must keep its
        // panik arm wired up. The receiver is `Some` only while the
        // watcher hasn't fired yet — once the panik arm consumed
        // it, the local was set to None and we round-trip None
        // through `self` (a benign noop). The outer `run_pipeline`
        // owns "did panik fire?" via `self.panik_outcome`, which
        // an entry into `run_retry_passes` checks BEFORE re-entering
        // the loop so the retry pass is bypassed on panik.
        self.panik_signal_rx = panik_signal_rx;

        // Drain any in-flight respawn tasks so the operational loop
        // never exits with a tokio task quietly outliving the
        // coordinator. The drain only fires when at least one task
        // is in flight; an empty JoinSet is a fast no-op.
        self.drain_respawn_tasks().await;

        Ok(())
    }

    /// Post-pipeline retry pass — now a no-op.
    ///
    /// Pre-redesign this drove ONE lumped retry pass after the main
    /// operational loop drained every phase. The 2026-05-17 user-
    /// spec'd redesign moved retry semantics INTO the per-phase
    /// lifecycle cascade so phase B never advances while phase A
    /// still has retriable failures — see
    /// [`crate::primary::retry_bucket`] and the
    /// `try_run_phase_retry_bucket` call sites inside
    /// [`crate::primary::PrimaryCoordinator::process_phase_lifecycle`].
    ///
    /// The function body is kept as a no-op (rather than removed)
    /// so the existing call site in `run_pipeline` stays compiling
    /// without a churning structural edit; cleanup of the call
    /// site is a follow-up. The panik short-circuit stays so a
    /// future re-introduction of post-pipeline behaviour doesn't
    /// silently re-arm it.
    pub(crate) async fn run_retry_passes(&mut self) -> Result<(), String> {
        if self.panik_outcome.is_some() {
            return Ok(());
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
    pub(crate) async fn drain_pending_messages(
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
                    // Post-loop drain: no operational loop is running to
                    // service callback-queued spawn_tasks, so passing
                    // &mut None here avoids broadcasting CRDT mutations
                    // for tasks that would never get dispatched. A
                    // callback that issues spawn_tasks at this point has
                    // its command silently dropped when the coordinator
                    // is torn down — same behaviour as any other post-
                    // run handle write.
                    self.dispatch_message(msg, &mut None).await?;
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

}
