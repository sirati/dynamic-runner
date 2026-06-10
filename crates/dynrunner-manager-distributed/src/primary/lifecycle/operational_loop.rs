use std::collections::HashMap;
use std::time::{Duration, Instant};

use dynrunner_core::{ErrorType, Identifier};
use dynrunner_protocol_primary_secondary::{DiscoveryDebt, MessageType};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use crate::primary::PrimaryCoordinator;
use crate::primary::wire::compute_task_hash;

// ── Operational-loop `select!` arm ids (oploop instrumentation) ──
//
// One id per arm of the big `select!` below, in source order. Passed to
// [`crate::oploop_instrumentation::OpLoopArmStats`] so a future production
// wedge names its hot arm in one log line. `ARM_INBOX` is the INBOUND (ingest)
// arm whose starvation IS the wedge signature. Kept adjacent to
// [`OP_LOOP_ARM_NAMES`] (index == id) so the two never drift; each arm body
// records its own id as its first statement.
const ARM_COMMAND: usize = 0;
const ARM_MATCHER: usize = 1;
const ARM_WORKER_MGMT: usize = 2;
const ARM_INBOX: usize = 3;
const ARM_HEARTBEAT: usize = 4;
const ARM_ANTI_ENTROPY: usize = 5;
const ARM_RESPAWN_REQUEST: usize = 6;
const ARM_LIVENESS_PING: usize = 7;
const ARM_RESPAWN_JOIN: usize = 8;
const ARM_PANIK: usize = 9;
const ARM_STUCK_WATCHDOG: usize = 10;

/// Arm names, index-aligned with the `ARM_*` ids above. The render order of
/// the compact stats line.
const OP_LOOP_ARM_NAMES: &[&str] = &[
    "command",
    "matcher",
    "worker_mgmt",
    "inbox",
    "heartbeat",
    "anti_entropy",
    "respawn_request",
    "liveness_ping",
    "respawn_join",
    "panik",
    "stuck_watchdog",
];

/// Render a drain-by-`MessageType` tally as a single diagnostic string:
/// `[TaskRequest=N, Keepalive=M, ...]`, only non-zero types, sorted by
/// count descending (ties broken by the type's `Debug` name for a
/// stable order) so the dominant type a 570k+ drain is made of is the
/// first field. Pure formatter — no I/O, no `&self` — so the drain loop
/// can accumulate the map cheaply (one increment per frame) and hand it
/// here once, and the format is unit-testable in isolation.
fn format_drain_tally(by_type: &HashMap<MessageType, usize>) -> String {
    let mut entries: Vec<(&MessageType, usize)> = by_type.iter().map(|(ty, &n)| (ty, n)).collect();
    // Count descending; `Debug`-name ascending as the stable tie-break.
    entries.sort_by(|(a_ty, a_n), (b_ty, b_n)| {
        b_n.cmp(a_n)
            .then_with(|| format!("{a_ty:?}").cmp(&format!("{b_ty:?}")))
    });
    let body = entries
        .iter()
        .map(|(ty, n)| format!("{ty:?}={n}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{body}]")
}

impl<S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<S, E, I> {
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
    /// All three exits are evaluated against live CRDT / pool / counter
    /// state, so there is no demoted-vs-authoritative `partial_view`
    /// special-case: a node running this loop is the authoritative
    /// primary by construction (the demoted node is a pure observer
    /// running the secondary observe loop, NOT this loop). The
    /// replicated-ledger `RunComplete` arm is a uniform fallback that
    /// is redundant on the fully-seeded path (the counter arm trips
    /// first) and load-bearing only when an external authority's
    /// `RunComplete` broadcast is the first complete signal this node
    /// observes.
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
        // Discovery-owed gate (V6): while the CRDT declares discovery `Owed`
        // the ledger has not yet been seeded (`total_tasks == 0` and every
        // declared phase is a transiently-empty `Active`), so the counter
        // exit (`0+0 >= 0`) and the pool-drain exit would both false-fire
        // "the run is done" before any task exists. Both are skipped together
        // until `discover_on_promotion` originates `DiscoverySettled`
        // (flipping `Owed → Settled`). On every cold mode-1 / legacy /
        // already-seeded path the marker is `Undeclared`/`Settled` (`!= Owed`),
        // so these exits run unchanged. Once `discover_on_promotion` settles
        // the debt and re-hydrates, an all-skipped / empty mode-2 corpus exits
        // through the SAME counter arm a fully-completed run does (its skips
        // are projected into `completed_tasks` by hydrate) — there is no
        // discovery-originated run-terminal to special-case. The
        // replicated-ledger `RunComplete` arm below is a real terminal cue and
        // is NOT gated — an external authority's `RunComplete` must still exit
        // even mid-debt.
        let discovery_owed = self.cluster_state.discovery_debt() == DiscoveryDebt::Owed;
        // Counter-based exit: every task accounted for (completed or
        // failed) and no worker mid-dispatch. Re-read every iteration
        // so lazy `spawn_tasks` / `TasksSpawned` growth is absorbed.
        if !discovery_owed
            && self.completed_tasks.len() + self.failed_tasks.len() >= self.total_tasks
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
        if !discovery_owed && self.pool().is_run_complete() && active_workers == 0 {
            tracing::info!("pool drained and no active workers");
            return true;
        }

        // Replicated-ledger run-complete signal. Whichever node holds
        // authority broadcasts `ClusterMutation::RunComplete` as the
        // last act before its own `run()` returns; `dispatch_message`
        // applies it to this node's `cluster_state` mirror.
        //
        // On the fully-seeded path this is a redundant exit — the
        // counter check above trips first (the local ledger was seeded
        // by `seed_cluster_state` and every peer's TaskCompleted arrives
        // before the authority itself decides RunComplete). Keeping this
        // arm unguarded is harmless and serves as a uniform fallback for
        // any path where an external `RunComplete` is the first complete
        // signal observed. `cluster_state.run_complete()` is a sticky
        // monotonic flag, so this fires at most once per run.
        if self.cluster_state.run_complete() && active_workers == 0 {
            tracing::info!("RunComplete signal received from cluster; exiting");
            return true;
        }

        false
    }

    pub(crate) async fn operational_loop(&mut self) -> Result<(), String> {
        tracing::info!("entering operational loop");

        // Per-iteration arm accounting (observation only — see
        // `crate::oploop_instrumentation`). One [`OpLoopArmStats`] per loop
        // entry, naming each `select!` arm in id order and which one is the
        // INBOUND (ingest) arm whose starvation is the production wedge
        // signature. Published on `self.op_loop_arm_stats` so the off-runtime
        // runtime-watchdog can dump the live arm breakdown when it fires; each
        // arm body calls `arm_stats.record(ARM_*)` as its first statement. The
        // hot-path cost is a handful of relaxed atomic stores per iteration.
        let arm_stats =
            crate::oploop_instrumentation::OpLoopArmStats::new(OP_LOOP_ARM_NAMES, ARM_INBOX);
        self.op_loop_arm_stats = Some(std::sync::Arc::clone(&arm_stats));
        // If a watchdog bridge cell is wired, publish the SAME Arc into it
        // (labelled "primary") so the off-runtime checker dumps this loop's
        // arms on a freeze. One Arc, two observers (the local field for
        // in-process fixtures, the cell for the production watchdog) — no
        // duplicated recording. The returned guard clears the "primary" entry
        // on EVERY exit of this function (break/return/unwind), so the entry's
        // lifetime tracks the loop exactly with no per-exit bookkeeping.
        let _arm_stats_guard = self
            .op_loop_arm_stats_cell
            .as_ref()
            .map(|cell| cell.publish_scoped("primary", std::sync::Arc::clone(&arm_stats)));

        let mut heartbeat_tick = tokio::time::interval(self.config.keepalive_interval);
        // Skip (not Burst) missed ticks: a host suspend/resume would otherwise
        // make the default Burst behaviour fire one catch-up heartbeat per
        // missed interval all at once. Skip collapses the backlog to a single
        // catch-up tick so the post-resume heartbeat sweep runs once, not in a
        // storm.
        heartbeat_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // Skip the immediate first tick — secondaries might not have sent
        // their first keepalive yet at the moment we enter the loop.
        heartbeat_tick.tick().await;

        // Anti-entropy cadence. On each tick the primary broadcasts its
        // `StateDigest` so any follower behind the authoritative ledger
        // pulls a snapshot to converge. The period carries a deterministic
        // per-node jitter (from `node_id`) so the fleet's digests spread
        // across the window. `Skip` collapses a post-suspend backlog to one
        // catch-up tick; the immediate first tick is consumed so the first
        // broadcast lands one full period in.
        let mut anti_entropy_tick =
            tokio::time::interval(crate::anti_entropy::tick_period(&self.config.node_id));
        anti_entropy_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        anti_entropy_tick.tick().await;

        // One-shot gate on the single recv arm. Flips true the first
        // time `transport.recv_peer()` returns `None`. Mirrors
        // `SecondaryCoordinator.primary_disconnected` (see
        // `secondary/processing.rs:75`): a closed mpsc receiver
        // resolves immediately on every subsequent poll, so leaving
        // the arm enabled after the first None would hot-loop the
        // select!. The timer arms still drive every subsequent loop
        // iteration so the top-of-loop exit checks (counter-based,
        // pool-drained, `cluster_state.run_complete()`) can still
        // trip. Resets are intentionally absent — once the transport's
        // inbound has closed it cannot re-open mid-run.
        let mut transport_closed = false;

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
        // select. Mirrors the `transport_closed` gate above.
        let mut matcher_arm_closed = false;

        // Worker-management signal receiver. Same shape + lifetime as
        // `matcher_trigger_rx`: taken out for the loop's duration so the
        // `recv_worker_signal_batch` await can borrow it without
        // conflicting with the per-arm `&mut self` borrows, then put
        // back at loop exit so retry-pass re-entries keep draining the
        // same channel. `None` when a previous run already consumed it
        // (single-shot lifecycle — same handling as `matcher_trigger_rx`).
        let mut worker_mgmt_rx = self.worker_mgmt_rx.take();
        // One-shot gate on the worker-management arm. Flips true on
        // `rx.recv() == None` (every sender dropped); mirrors
        // `matcher_arm_closed`.
        let mut worker_mgmt_arm_closed = false;

        // Respawn-request receiver. Same shape + lifetime as
        // `command_rx`: taken out for the duration of the loop so the
        // arm's `recv().await` can borrow it without conflicting with
        // the per-arm `&mut self` borrows. `None` when the respawn
        // policy is disabled at construction (no spawner, no budget,
        // no channel) — the arm parks on `pending().await` in that
        // case, matching the command-channel disabled-arm shape.
        let mut respawn_request_rx = self.respawn_request_rx.take();

        // Liveness-beacon ping receiver. Same disabled-arm shape: `None`
        // when no listener was wired (channel-only fixtures) → the arm
        // parks on `pending().await`. Each forwarded node-id refreshes
        // that secondary's death-clock as the UNION half of the reaper
        // (beacon OR mesh frame), so a busy secondary whose tokio runtime
        // is CPU-starved by a build still beacons and is NOT reaped.
        let mut liveness_ping_rx = self.liveness_ping_rx.take();

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

        // Entry sweep — dispatch is a pure function of state, asserted
        // ONCE the moment the loop is entered. The pre-loop chain may have
        // left a ready-task ∩ idle-worker match unfilled: a secondary
        // whose `SecondaryCapacity` landed AFTER `perform_initial_assignment`
        // is now in the reconstructed roster as an idle slot (the pre-loop
        // waits' inline reaction rebuilt it), but if its `TasksAdded` was
        // already consumed by a wait's drain — or the worker freed with no
        // pending signal — nothing has re-dispatched against the live
        // (pool ∩ idle-worker) state since. Run the same idempotent recheck
        // the worker-management arm runs, so steady state is reached with
        // every dispatchable task already placed on a free worker rather
        // than waiting for the next bus event to first act on a backlog.
        // `bypass_backpressure = true`: this is a circumstances-changed
        // recheck (the run just transitioned into the operational loop),
        // the same class as a genuine `TasksAdded`. Send failures are
        // logged + rolled back inside the recheck; `.ok()` swallows the
        // transient so the sweep can't abort loop entry.
        self.dispatch_to_idle_workers(true).await.ok();

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
                // Completion-gate fatal pre-drain. A `RunShouldFail` /
                // `PolicyFatalExit` (e.g. a consumer `on_phase_end` hook
                // that RAISED) emitted onto the worker-management bus
                // DURING the same iteration that finished the last task
                // is already queued but has not yet been selected by the
                // parked worker-management arm — so a naive clean exit
                // here would mask the failure as exit-0. Before
                // concluding the run is clean, synchronously drain
                // whatever is queued right now (no idle-window wait) and
                // run the SAME worker-management reaction the parked arm
                // uses. Scoped to the completion gate so the normal
                // `TasksAdded` recheck cadence (run-not-complete) is
                // untouched. The decoupling law holds: the loop owns the
                // drive, the phase layer only emitted. If the drain
                // recorded a fatal outcome, fall through to the
                // run-should-fail break below; otherwise the run really
                // is clean.
                if let Some(batch) = worker_mgmt_rx
                    .as_mut()
                    .and_then(crate::worker_signal::try_collect_worker_signal_batch)
                {
                    self.react_to_worker_signal_batch(batch).await;
                }
                if self.worker_mgmt_fail_outcome.is_none() {
                    break;
                }
            }

            // Worker-management run-should-fail exit. The
            // worker-management `select!` arm (or the completion-gate
            // pre-drain above) records a break outcome on
            // `worker_mgmt_fail_outcome` when it drains a `RunShouldFail`
            // / `PolicyFatalExit` (emitted by the phase layer onto the
            // decoupled bus, OR by the phase-floor liveness check). The
            // worker arm OWNS the clean-shutdown drive; breaking here —
            // not from the emit path — keeps phase/task management fully
            // decoupled from worker management (the dispatch-decoupling
            // law). `run_pipeline` consumes the outcome after the loop
            // returns Ok and surfaces the failure. Same write-by-arm /
            // read-by-loop discipline as `panik_outcome`.
            if self.worker_mgmt_fail_outcome.is_some() {
                tracing::warn!(
                    "primary operational loop exiting via worker-management \
                     run-should-fail signal"
                );
                break;
            }

            // Fleet-dead detection. The arming quantity is the count of
            // alive worker-secondaries OTHER than the host this node
            // recognizes as primary
            // (`cluster_state.alive_remote_secondary_count()`): when it
            // reaches zero and the pool still has pending work, no living
            // secondary can dispatch the queued tasks and the loop would
            // otherwise sit forever waiting for events that never arrive.
            // Track the first moment that count is zero-but-pool-has-work;
            // after `config.fleet_dead_timeout` of continuous emptiness,
            // exit cleanly with pending tasks left stranded so the
            // operator gets a clear failure rather than a silent idle.
            // Cleared the moment a remote secondary is present again
            // (re-handshake / partial fleet survival).
            //
            // Counting REMOTE secondaries (excluding the recognized
            // primary by identity) is what makes the arming honest when
            // the recognized primary also runs its own secondary: that
            // secondary never counts, so a primary partitioned from every
            // remote secondary arms fleet-dead and strands — it does NOT
            // hang on the strength of its own secondary, and it does NOT
            // stay alive against a freshly-elected primary (split-brain).
            // For a submitter primary the recognized primary is not a
            // worker-secondary, so the count is just "all alive
            // worker-secondaries" — unchanged behaviour.
            //
            // Tokenizer surfaced the original failure on cohort-3 where
            // SSH-tunnel blips killed all 5 secondaries at once and the
            // run sat idle until manually killed.
            if self.cluster_state.alive_remote_secondary_count() == 0 && !self.pool().is_empty() {
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
                        "fleet-dead timeout: every remote secondary gone with non-empty pool; \
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

            // Per-task reconciliation-probe tick (#308). Polled at the
            // top of the loop — the same ≤keepalive-interval cadence
            // the fleet-dead check above rides — because the prober's
            // deadlines are PERSISTENT stored `Instant`s on its own
            // clock (the 1e914505 lesson: a `select!`-arm sleep is
            // rebuilt every iteration and never elapses on a live
            // cluster). The prober internally throttles full polls to
            // its 1s cadence, so a hot mesh iteration pays one
            // `Instant` compare. Probes that fire ask the holder
            // secondary "do you still hold task X?"; the verdict
            // arrives through the inbox arm's `TaskHoldResponse`
            // handler. Accounting reconciliation only — no liveness
            // input is touched here.
            self.reconciliation_probe_tick().await;

            // Inbound closed: no further mutations can arrive, so this
            // node's view is frozen. The pre-collapse behaviour
            // (transport-closed → break) is preserved for the
            // pathological "mesh died" case. The
            // `cluster_state.run_complete()` check above is the
            // happy-path exit; this guard is only reached when the
            // transport's inbound itself has closed.
            if transport_closed {
                tracing::info!("transport inbound closed; exiting operational loop");
                break;
            }

            // Use a timeout on recv to avoid stalling indefinitely if a
            // secondary disconnects while processing a task. The timeout
            // is generous — if no message arrives in 5 minutes and there
            // are in-flight tasks, something is wrong.
            //
            // Cancellation safety: `transport.recv_peer` is the
            // mpsc-backed unified inbound demux (cancel-safe — see
            // `TunneledPeerTransport::recv_peer`). The two timer
            // arms (heartbeat tick + 5-min sleep) are tokio time
            // primitives which are themselves cancel-safe.
            //
            // There is exactly ONE inbound arm: the unification deleted
            // the legacy `transport.recv()` arm + its
            // "legacy-closed-but-mesh-live" special case. The folded
            // `NetworkServer` demux means `recv_peer` IS the real
            // inbound (no duplicate frames, no separate uplink to keep
            // alive), so the single arm carries every welcome / cert /
            // request / completion / ClusterMutation through
            // `dispatch_message` — the same dispatcher the deleted arm
            // used, idempotent on every wire shape.
            tokio::select! {
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
                    arm_stats.record(ARM_COMMAND);
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
                    arm_stats.record(ARM_MATCHER);
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
                wm_batch = async {
                    match worker_mgmt_rx.as_mut() {
                        // CANCEL-SAFE by contract: `recv_worker_signal_batch`
                        // consumes signals only on the poll that COMPLETES
                        // it (one cancel-safe recv + a synchronous sweep,
                        // no await while holding consumed signals), so a
                        // sibling arm winning this iteration cannot destroy
                        // a `TasksAdded`. Its idle-window predecessor was
                        // NOT cancel-safe and silently lost injected-batch
                        // signals on every busy-mesh phase boundary (the
                        // run_20260610_145529 starve→pack).
                        Some(rx) => {
                            crate::worker_signal::recv_worker_signal_batch(rx).await
                        }
                        // No receiver attached — park forever so the
                        // arm never fires. Mirrors the matcher arm's
                        // `pending().await` for the same closed-channel
                        // hot-loop reason.
                        None => std::future::pending().await,
                    }
                }, if !worker_mgmt_arm_closed => {
                    arm_stats.record(ARM_WORKER_MGMT);
                    match wm_batch {
                        Some(batch) => {
                            // Worker management's PARKED RECHECK: a batch
                            // of decoupled signals arrived. The reaction
                            // (dispatch recheck over every free worker,
                            // phase-floor liveness check, run-should-fail
                            // break) lives in `lifecycle::worker_mgmt`;
                            // this arm's only concern is "a batch
                            // arrived; hand it off". The phase/task code
                            // that emitted the signals never touched
                            // worker management directly (the
                            // dispatch-decoupling law).
                            self.react_to_worker_signal_batch(batch).await;
                        }
                        None => {
                            // Every sender dropped. Same as the matcher
                            // channel's None arm: disable this arm and
                            // let the timer / counter exit cues take over.
                            worker_mgmt_arm_closed = true;
                            tracing::debug!(
                                "worker-management signal channel closed; disabling \
                                 the worker-management arm for the remainder of \
                                 the loop"
                            );
                        }
                    }
                }
                msg = self.inbox.recv(), if !transport_closed => {
                    arm_stats.record(ARM_INBOX);
                    match msg {
                        Some(m) => {
                            // THE single inbound arm. Every wire shape —
                            // welcome / cert / TaskRequest / TaskComplete
                            // / TaskFailed / Keepalive / ClusterMutation
                            // (incl. a promoted peer's broadcasts post-
                            // demotion) — arrives here and threads through
                            // `dispatch_message`, the one source of truth
                            // for wire-shape handling.
                            //
                            // Idempotency: `cluster_state.apply` is
                            // CRDT-idempotent, the `completed_tasks` /
                            // `failed_tasks` HashSet inserts are
                            // idempotent, and `handle_task_complete`
                            // short-circuits on
                            // `completed_tasks.contains(hash)` — so a
                            // mutation that reaches the primary via more
                            // than one peer-forward path is absorbed.
                            self.dispatch_message(m, &mut command_rx).await?;
                        }
                        None => {
                            // The operational inbox closed: every sender —
                            // i.e. the role slot the mesh-pump delivers
                            // through — is gone. On a PRIMARY this is
                            // fatal-worthy, never routine: the mesh pump is
                            // the ONLY router into this inbox, so no task
                            // terminal, request, or mutation can ever
                            // arrive again. Gate the arm so subsequent
                            // select! iterations don't hot-poll a
                            // permanently-resolved future (the closed-mpsc
                            // hazard); the top-of-loop `transport_closed`
                            // guard then BREAKS the loop into the run's
                            // final accounting, where unresolved work is
                            // classified stranded and surfaces as
                            // `RunError::ClusterCollapsed` (non-zero exit)
                            // — loud and terminal, never a silently
                            // disabled arm that zombies the run.
                            transport_closed = true;
                            tracing::error!(
                                "operational inbox closed — the mesh pump is \
                                 gone; the primary cannot ingest any further \
                                 frames. Exiting the operational loop into \
                                 final accounting (outstanding work will be \
                                 classified stranded)"
                            );
                        }
                    }
                }
                _ = heartbeat_tick.tick() => {
                    arm_stats.record(ARM_HEARTBEAT);
                    self.broadcast_primary_keepalive().await;
                    // Refresh the PRIMARY→secondaries liveness-beacon target
                    // set on the SAME cadence as the mesh keepalive — the
                    // transport-independent twin. Any roster change since the
                    // last tick (welcome, hydrate, dead-secondary requeue) is
                    // reflected here, well inside the death thresholds; the
                    // off-runtime beacon thread re-reads the published set
                    // each of its own ticks. A single placement (not one per
                    // roster-mutation site) keeps the concern in one spot.
                    self.publish_beacon_targets();
                    // `process_heartbeat_tick` collects the per-tick
                    // death report and hands it to the dead-secondary
                    // declaration/requeue policy. See
                    // `process_heartbeat_tick` for detail.
                    self.process_heartbeat_tick().await?;
                }
                // Anti-entropy tick: broadcast the primary's digest so any
                // follower behind the authoritative ledger pulls + converges.
                // Pure EMIT of the role-agnostic frame from
                // `crate::anti_entropy`; the receive-side compare+pull is in
                // the primary `dispatch_message` `StateDigest` arm.
                // `interval.tick` is cancel-safe (tokio docs).
                _ = anti_entropy_tick.tick() => {
                    arm_stats.record(ARM_ANTI_ENTROPY);
                    let digest = self.cluster_state.digest();
                    let frame = crate::anti_entropy::digest_broadcast(
                        &self.config.node_id,
                        crate::primary::wire::timestamp_now(),
                        digest,
                    );
                    let _ = self
                        .send_to(
                            dynrunner_protocol_primary_secondary::Destination::All,
                            frame,
                        )
                        .await;
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
                    arm_stats.record(ARM_RESPAWN_REQUEST);
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
                // Liveness-beacon ping drain. A secondary's dedicated
                // beacon thread (independent of its CPU-starvable tokio
                // runtime) sends a UDP datagram; this node's
                // `LivenessListener` decoded it and forwarded the node-id
                // here. Refresh that secondary's death-clock — the UNION
                // half: `record_keepalive` is the SAME refresh the inbound
                // mesh-frame path (`dispatch_message`) calls, so the reaper
                // declares a secondary dead only when BOTH its beacon and
                // its frames have been silent past the threshold. This is
                // what keeps a busy, build-CPU-starved-but-alive secondary
                // from being false-reaped. Disabled-arm shape mirrors the
                // respawn arm: `None` rx → parks on `pending()`.
                ping = async {
                    match liveness_ping_rx.as_mut() {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    arm_stats.record(ARM_LIVENESS_PING);
                    match ping {
                        Some(node_id) => {
                            // `record_keepalive` is a no-op for an
                            // id not in `self.secondaries` (a stray /
                            // already-removed node), so a beacon racing a
                            // removal can't resurrect a reaped entry here.
                            self.record_keepalive(&node_id);
                        }
                        None => {
                            // Listener task gone (run winding down). Drop
                            // the receiver; the mesh-frame refresh half of
                            // the union keeps the death-clock honest.
                            liveness_ping_rx = None;
                            tracing::debug!(
                                "liveness ping channel closed; disabling \
                                 the liveness-beacon arm for the remainder \
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
                    arm_stats.record(ARM_RESPAWN_JOIN);
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
                // loop here mirrors the `transport_closed` exit shape:
                // the operational loop's `Result<(), String>` signature
                // does not need to change.
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
                    arm_stats.record(ARM_PANIK);
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
                        tracing::error!(
                            "primary operational loop exiting via panik path"
                        );
                        break;
                    }
                }
                _ = tokio::time::sleep(Duration::from_secs(300)),
                    if !self.single_worker_mode() => {
                    arm_stats.record(ARM_STUCK_WATCHDOG);
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
                            for idx in 0..self.workers.len() {
                                let w = &self.workers[idx];
                                if let Some(task) = w.held_task() {
                                    out.push((
                                        w.secondary_id.clone(),
                                        self.local_worker_id_in_secondary(idx),
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
        // Same rationale for the worker-management signal receiver:
        // retry passes re-enter the operational loop and must keep
        // draining the same bus so a `TasksAdded` emitted during a
        // retry pass still drives the dispatch recheck.
        self.worker_mgmt_rx = worker_mgmt_rx;
        // Same rationale for the respawn-request receiver: retry
        // passes re-enter the operational loop and a death observed
        // during a retry pass should still drive the dispatcher.
        self.respawn_request_rx = respawn_request_rx;
        // Same rationale for the liveness-beacon ping receiver: a retry
        // pass that re-enters the operational loop must keep refreshing
        // death-clocks from beacon datagrams (the union half), or a busy
        // secondary's beacon would stop counting during retry passes.
        self.liveness_ping_rx = liveness_ping_rx;
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

        // Unpublish the local arm-stats handle now the loop has exited: a
        // stale snapshot read after the loop is gone would be misleading (the
        // loop is no longer the thing to diagnose). A retry-pass re-entry
        // rebuilds a fresh block. The watchdog-cell entry is cleared
        // automatically by `_arm_stats_guard`'s drop at function exit (it
        // covers the early-return paths too). Observation-only — no
        // control-flow depends on this.
        self.op_loop_arm_stats = None;

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
    pub(crate) async fn drain_pending_messages(&mut self, budget: Duration) -> Result<(), String> {
        let deadline = Instant::now() + budget;
        let quiet_window = Duration::from_millis(50);
        let mut drained = 0usize;
        // Per-`MessageType` tally, diagnostics-only: a consumer hit a
        // 570k-640k drain with no way to see WHAT was drained. Accumulated
        // here (one increment per frame) and rendered once after the drain
        // — does not touch the drain count or behaviour.
        let mut drained_by_type: HashMap<MessageType, usize> = HashMap::new();
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            let poll_window = std::cmp::min(quiet_window, remaining);
            match tokio::time::timeout(poll_window, self.inbox.recv()).await {
                Ok(Some(msg)) => {
                    // Post-loop drain: no operational loop is running to
                    // service callback-queued spawn_tasks, so passing
                    // &mut None here avoids broadcasting CRDT mutations
                    // for tasks that would never get dispatched. A
                    // callback that issues spawn_tasks at this point has
                    // its command silently dropped when the coordinator
                    // is torn down — same behaviour as any other post-
                    // run handle write.
                    //
                    // Read the type BEFORE dispatch consumes `msg` —
                    // diagnostics-only tally, no effect on dispatch.
                    *drained_by_type.entry(msg.msg_type()).or_insert(0) += 1;
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
            // Additive diagnostic: per-MessageType breakdown of the same
            // drain, so a dominant type (the 570k-640k case) is visible.
            tracing::info!(
                drained_by_type = %format_drain_tally(&drained_by_type),
                "drained pending wire messages by type"
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{HashMap, MessageType, format_drain_tally};

    fn tally(pairs: &[(MessageType, usize)]) -> HashMap<MessageType, usize> {
        pairs.iter().cloned().collect()
    }

    #[test]
    fn breakdown_reflects_counts_sorted_by_count_desc() {
        // 3 TaskRequest + 1 Keepalive → both present, dominant first.
        let by_type = tally(&[(MessageType::TaskRequest, 3), (MessageType::Keepalive, 1)]);
        assert_eq!(format_drain_tally(&by_type), "[TaskRequest=3, Keepalive=1]");
    }

    #[test]
    fn dominant_type_is_first_regardless_of_insertion() {
        let by_type = tally(&[
            (MessageType::Keepalive, 2),
            (MessageType::ClusterMutation, 640_000),
            (MessageType::TaskRequest, 5),
        ]);
        // The 640k dominant type leads; the rest follow by count desc.
        assert_eq!(
            format_drain_tally(&by_type),
            "[ClusterMutation=640000, TaskRequest=5, Keepalive=2]"
        );
    }

    #[test]
    fn equal_counts_break_ties_by_name_for_stable_order() {
        let by_type = tally(&[(MessageType::TaskRequest, 4), (MessageType::Keepalive, 4)]);
        // Equal counts → ascending Debug-name: Keepalive before TaskRequest.
        assert_eq!(format_drain_tally(&by_type), "[Keepalive=4, TaskRequest=4]");
    }

    #[test]
    fn empty_tally_renders_empty_brackets() {
        assert_eq!(format_drain_tally(&HashMap::new()), "[]");
    }
}
