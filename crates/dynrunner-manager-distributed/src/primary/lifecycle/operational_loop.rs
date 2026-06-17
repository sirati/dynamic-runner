use std::collections::HashMap;
use std::time::{Duration, Instant};

use dynrunner_core::{IMPORTANT_TARGET, Identifier};
use dynrunner_protocol_primary_secondary::{DiscoveryDebt, MessageType};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};
use tokio::sync::mpsc as tokio_mpsc;

use crate::primary::PrimaryCoordinator;
use crate::primary::command_channel::PrimaryCommand;

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
pub(crate) const ARM_INBOX: usize = 3;
const ARM_HEARTBEAT: usize = 4;
const ARM_ANTI_ENTROPY: usize = 5;
const ARM_RESPAWN_REQUEST: usize = 6;
const ARM_LIVENESS_PING: usize = 7;
const ARM_RESPAWN_JOIN: usize = 8;
const ARM_PANIK: usize = 9;
const ARM_GRACEFUL_ABORT: usize = 10;
const ARM_TASK_BACKOFF: usize = 11;
const ARM_SNAPSHOT_STREAM: usize = 12;
const ARM_SETTLED_SPILL: usize = 13;
const ARM_PULL: usize = 14;
const ARM_PERSISTENT_DIAL_FAILURE: usize = 15;
const ARM_DISCOVERY: usize = 16;

/// Arm names, index-aligned with the `ARM_*` ids above. The render order of
/// the compact stats line.
pub(crate) const OP_LOOP_ARM_NAMES: &[&str] = &[
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
    "graceful_abort",
    "task_backoff",
    "snapshot_stream",
    "settled_spill",
    "pull",
    "persistent_dial_failure",
    "discovery",
];

/// Follow-on batch-drain cap for the inbox arm (#491, mirrored from the
/// secondary's `process_tasks::INBOX_BATCH_DRAIN_CAP`). At affine scale the
/// O(tasks) pull-model TaskRequest/TaskComplete arrival rate floods a
/// single-frame-per-iteration drain, pinning the inbox arm ~95% and starving
/// heartbeat/worker_mgmt → runtime-starvation freeze. After the awaited
/// `recv()` yields, the arm synchronously sweeps up to this many MORE
/// already-queued frames so the loop keeps up; the cap bounds the burst so
/// every sibling arm still gets a turn each pass (a deeper backlog drains down
/// across iterations). Held primary-side at the same 256 the secondary uses —
/// the two loops are independent consumers of the same `RoleInbox` primitive,
/// so a mirrored local const keeps each role's tuning self-contained rather
/// than coupling them through a shared symbol.
const INBOX_BATCH_DRAIN_CAP: usize = 256;

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
    /// closed, panik, setup-promote deadline)
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

    /// Run the full ARM_HEARTBEAT body — the five per-cadence calls the
    /// `heartbeat_tick.tick()` arm makes — out-of-band of the select!.
    /// Called from both the normal arm and the heartbeat-deadline fairness
    /// gate ([`Self::fire_heartbeat_if_overdue`]) so the body's call order
    /// stays in ONE place. Returns the same `Result<(), String>` shape the
    /// arm body propagates from `process_heartbeat_tick`.
    ///
    /// The five calls are idempotent against being invoked at a higher
    /// rate than the configured `keepalive_interval`:
    ///   * `dispatch_unhandled_custom_messages` no-ops when the inbox
    ///     holds no `Unhandled` entry (the steady-state hot path);
    ///   * `observe_custom_backlog` is a WARN observer with its own
    ///     rate-limit gate;
    ///   * `broadcast_primary_keepalive` is a fan-out broadcast — peers
    ///     bump their per-secondary keepalive timestamps and any extra
    ///     emit just refreshes them earlier;
    ///   * `publish_beacon_targets` republishes the same target set;
    ///   * `process_heartbeat_tick` self-defers via `own_tick_health`
    ///     when called too close to the prior fire (the tick-lag
    ///     guard), so a fairness-gate double-fire is absorbed without
    ///     spurious sweeps.
    pub(crate) async fn service_heartbeat_tick(
        &mut self,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
    ) -> Result<(), String> {
        self.dispatch_unhandled_custom_messages(command_rx).await;
        self.observe_custom_backlog();
        self.broadcast_primary_keepalive().await;
        self.publish_beacon_targets();
        self.process_heartbeat_tick().await
    }

    /// Heartbeat-deadline fairness gate (#582). Called once per
    /// operational-loop iteration BEFORE the `select!`. If the wall-clock
    /// elapsed since the last heartbeat fire exceeds `keepalive_interval`,
    /// synchronously runs the ARM_HEARTBEAT body and resets the cadence
    /// tick.
    ///
    /// ## Why this exists
    ///
    /// `tokio::select! { biased; ... }` resolves WAKE-ORDER ties — when
    /// multiple arms are ready in the same poll, the higher-priority arm
    /// wins. It does NOT bound how long a winning arm's body runs; a body
    /// that does not yield monopolises the loop until it returns. Under
    /// sustained data-arm load (#582: 10 batches/sec spawn-stream into
    /// the COMMAND arm), ARM_HEARTBEAT (id=4 in the biased order, below
    /// the data arms) can lose every tiebreak indefinitely → keepalive
    /// deafness → 75 s collective-silence episode → false departures.
    ///
    /// The contract is: every data-arm body MUST yield to siblings
    /// between work units (continuation queue per #547/#582, or
    /// `yield_now().await` for arms that lack a natural queue). This gate
    /// is the LAST-LINE defence: even if a future regression re-introduces
    /// a non-yielding hot-arm body, the heartbeat still fires within
    /// `keepalive_interval` of its prior fire.
    ///
    /// ## Cadence interaction
    ///
    /// After firing, `heartbeat_tick.reset()` rescheds the interval to
    /// `now + keepalive_interval`, so the next normal-cadence fire is one
    /// full period away — the gate does not double-count against the
    /// schedule. `process_heartbeat_tick` internally defers via
    /// `own_tick_health` if the call rate gets too tight, so the fairness
    /// fire never causes a spurious dead-secondary sweep.
    ///
    /// ## Observability
    ///
    /// An overdue fire emits one `IMPORTANT_TARGET` info line naming the
    /// elapsed silence so operators see fan-in saturation episodes in the
    /// important-stdio sink. Steady-state (no starvation) the gate is a
    /// single `Instant::elapsed()` comparison per iteration — no logging,
    /// no allocations.
    pub(crate) async fn fire_heartbeat_if_overdue(
        &mut self,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
        last_heartbeat_fire: &mut Instant,
        heartbeat_tick: &mut tokio::time::Interval,
        arm_stats: &crate::oploop_instrumentation::OpLoopArmStats,
    ) -> Result<(), String> {
        // Threshold: fire the gate at 2× `keepalive_interval` since the last
        // heartbeat fire. The 2× margin separates GENUINE STARVATION from
        // normal-cadence tick jitter: the periodic `heartbeat_tick` fires
        // every 1× interval on a healthy loop (small scheduler jitter
        // around that deadline), so a 1× threshold would race the tick
        // and spuriously fire the gate, suppressing the normal ARM_HEARTBEAT
        // record via the `heartbeat_tick.reset()` below. 2× is comfortably
        // above the worst-case jitter on a healthy loop AND comfortably
        // BELOW the first silence-WARN multiple (`silence_warn_multiples[0]`,
        // typically 3-4×) so peers never observe missed keepalives. Under
        // the 5 s production cadence this is a 10 s starvation budget — the
        // consumer's run_20260615_192743 burst monopolised the loop for
        // ~75 s before #582 even noticed, so 10 s before the fairness gate
        // intervenes is generous headroom for a recoverable transient
        // (e.g. a single chunk that runs long under load) while still
        // bounding the steady-state worst case.
        let starvation_threshold = self
            .config
            .keepalive_interval
            .saturating_mul(2);
        let elapsed = last_heartbeat_fire.elapsed();
        if elapsed <= starvation_threshold {
            return Ok(());
        }
        tracing::info!(
            target: IMPORTANT_TARGET,
            elapsed_ms = elapsed.as_millis() as u64,
            keepalive_interval_ms = self.config.keepalive_interval.as_millis() as u64,
            "heartbeat-deadline fairness gate firing — a hot arm body \
             (likely ARM_COMMAND or ARM_INBOX under sustained spawn-batch \
             load) monopolised the operational loop past the keepalive \
             cadence; firing the heartbeat body out-of-band so peers do \
             not falsely depart"
        );
        // Record the gate-fire as an ARM_HEARTBEAT observation in the
        // arm-stats counter: the body genuinely ran (every per-cadence
        // call the on-cadence arm makes happened here), so the
        // observability counter must reflect the fire. Without this an
        // operator reading the stats line would see "0 heartbeat ticks
        // across the burst" — exactly the wedge signature — while the
        // gate had been firing the body all along, masking the real
        // problem (the hot arm's monopolisation), since downstream
        // assertions on the counter (e.g.
        // `oploop_arm_hunt::probe_herd_over_large_ledger_does_not_stall_oploop`)
        // count the body fires, not the select! arm wins.
        arm_stats.record(ARM_HEARTBEAT);
        self.service_heartbeat_tick(command_rx).await?;
        *last_heartbeat_fire = Instant::now();
        // Reset the periodic tick so the next on-cadence fire is one full
        // `keepalive_interval` from now — without this the just-deferred
        // tick would fire IMMEDIATELY on the next select! poll (Skip
        // missed-tick behaviour returns instantly when the deadline has
        // already passed), double-firing the body.
        heartbeat_tick.reset();
        Ok(())
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
        // Wall-clock anchor for the heartbeat-deadline fairness gate
        // (#582). Updated at every actual fire of the heartbeat-arm body
        // (either via the on-cadence ARM_HEARTBEAT arm OR
        // `fire_heartbeat_if_overdue`). The pre-loop init treats the
        // dispatcher entry as the implicit fire-zero — the first
        // operational-loop iteration only invokes the fairness gate if a
        // hot arm body monopolised the loop for the entire first
        // `keepalive_interval`, exactly the starvation shape the gate
        // defends against. See `fire_heartbeat_if_overdue` for the full
        // contract.
        let mut last_heartbeat_fire = Instant::now();

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
        let mut respawn_lifecycle_rx = self.respawn_lifecycle_rx.take();

        // Liveness-beacon ping receiver. Same disabled-arm shape: `None`
        // when no listener was wired (channel-only fixtures) → the arm
        // parks on `pending().await`. Each forwarded node-id refreshes
        // that secondary's death-clock as the UNION half of the reaper
        // (beacon OR mesh frame), so a busy secondary whose tokio runtime
        // is CPU-starved by a build still beacons and is NOT reaped.
        let mut liveness_ping_rx = self.liveness_ping_rx.take();

        // Persistent-dial-failure receiver (#542 cause-B). Same
        // disabled-arm shape: `None` when no transport→coordinator wire
        // was installed (channel-only fixtures, the in-process
        // multi-computer-local manager, anyone who doesn't run on QUIC)
        // → the arm parks on `pending().await`. Each forwarded peer id is
        // one the QUIC transport has tried + failed to dial for
        // `DIAL_SUMMARY_THRESHOLD` consecutive sweeps without a connect;
        // the arm's handler decides whether to originate a
        // `ClusterMutation::PeerRemoved { cause: PersistentDialFailure }`
        // for the id — OBSERVER-only (see the handler).
        let mut persistent_dial_failure_rx = self.persistent_dial_failure_rx.take();

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

        // Mode-2 discovery in-flight (the concurrent-arm fix). `start_discovery
        // _if_owed` (run in the bring-up pre-loop) parked the STARTED
        // `discover_items` future here when the CRDT declares `DiscoveryDebt::
        // Owed`; `None` on every non-relocated / already-settled primary (the
        // discovery arm is then inert). Taken out for the loop's duration so the
        // arm can `await` the future without conflicting with the per-arm `&mut
        // self` borrows (the same take-local discipline as `command_rx`). On
        // resolve the arm calls `finish_discovery_arm` (seed + fire starts +
        // cascade + dispatch trigger) and leaves this `None`, so the arm goes
        // inert after the single fire. NOT round-tripped back to `self` at loop
        // exit: discovery fires at most once per run, so a retry-pass re-entry
        // must not re-discover (the `Settled` marker would no-op `start_
        // discovery_if_owed` anyway, but leaving the local consumed is the
        // explicit fire-once latch).
        let mut discovery_in_flight = self.discovery_in_flight.take();

        // Background peer-mesh-formation deadline (the decoupled-bring-up
        // replacement for the old blocking pre-loop wait). Dispatch
        // does NOT block on mesh formation — the op-loop runs + dispatches
        // immediately — but a ≥2-node fleet whose peer mesh never forms is
        // still a RUN-ABORT condition: the peer mesh is the failover
        // substrate and IS required. Arm a one-shot deadline relative to
        // operational-start; the top-of-loop check below fires the abort if
        // the mesh has not formed by then. `None` = not yet evaluated /
        // satisfied; armed lazily on the first iteration that observes an
        // unmet ≥2-node requirement (so a fleet that grows to ≥2 MID-RUN, or
        // forms its mesh before this point, is handled uniformly by the live
        // `mesh_formation_missing` predicate). Persistent `Instant` (not a
        // `select!`-arm sleep, which is rebuilt every iteration and never
        // elapses on a live cluster — the 1e914505 lesson the fleet-dead /
        // reconciliation ticks above also follow).
        let mut mesh_formation_deadline: Option<Instant> = None;

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
            // setup-promote deadline) are a
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
                    self.react_to_worker_signal_batch(batch, &mut command_rx).await;
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

            // Replicated run-terminal STAND-DOWN. `run_aborted` is the
            // CRDT-resident terminal verdict (#313): sticky, carried by
            // mutation broadcasts, anti-entropy digests, and snapshots. A
            // latched verdict on THIS node's mirror is either FOREIGN
            // (another authority's broadcast) or SELF-authored mid-loop
            // (the #3b run-wide invalidation latches the verdict inside
            // the command handler, BEFORE wiping the ledger). Either way
            // the run is over cluster-wide and continuing to author —
            // dispatch, retries, a later contradictory verdict — is the
            // zombie split-brain (run_20260610_221140: the deposed
            // epoch-2 primary ran 2 minutes past the epoch-9 RunAborted
            // and exited rc=0 with divergent totals). Break into the
            // finalize tail: the worker-mgmt gate surfaces a typed
            // self-authored outcome; the verdict gate adopts a foreign
            // abort as a structured non-zero exit.
            if let Some(reason) = self.cluster_state.run_aborted() {
                tracing::error!(
                    reason = %reason,
                    "replicated RunAborted verdict observed: the run is \
                     over cluster-wide — standing down out of the \
                     operational loop"
                );
                // Stand-down fatal pre-drain (mirror of the completion
                // gate's pre-drain above): the SELF-authored case (the
                // #3b invalidation) emitted `PolicyFatalExit` onto the
                // worker-management bus in the same handler that latched
                // the verdict, and this break runs BEFORE the parked
                // worker-management arm has consumed it. Without the
                // drain the typed break outcome would be lost and the
                // finalize tail would run the retry passes against an
                // aborted run. Same decoupling-law shape: the loop owns
                // the drive, the emit site never broke the loop directly.
                if let Some(batch) = worker_mgmt_rx
                    .as_mut()
                    .and_then(crate::worker_signal::try_collect_worker_signal_batch)
                {
                    self.react_to_worker_signal_batch(batch, &mut command_rx).await;
                }
                break;
            }

            // Graceful-abort drain decision (ONE seam in the loop; the
            // whole protocol lives in `lifecycle::graceful_abort`). A
            // steady-state run pays one latched-bool read. Under the latch
            // it breaks on full fleet drain (→ the finalize tail's
            // graceful-abort verdict) or relocates the primary role to the
            // busiest secondary when this node's own work has drained
            // while others still run (the demote arm then cancels this
            // loop). Placed BEFORE the fleet-dead arming below: a frozen
            // pool with a drained fleet is the graceful terminal, never a
            // strand.
            if self.graceful_abort_tick().await {
                break;
            }

            // Fleet-dead detection. The arming quantity is the count of
            // ALL alive worker-secondary members
            // (`cluster_state.alive_worker_secondary_count()`): when it
            // reaches zero and the pool still has pending work, no living
            // secondary can dispatch the queued tasks and the loop would
            // otherwise sit forever waiting for events that never arrive.
            // Track the first moment that count is zero-but-pool-has-work;
            // after `config.fleet_dead_timeout` of continuous emptiness,
            // exit cleanly with pending tasks left stranded so the
            // operator gets a clear failure rather than a silent idle.
            // Cleared the moment an alive secondary is present again
            // (re-handshake / partial fleet survival).
            //
            // The count includes the recognized primary's OWN co-located
            // worker-secondary: in-process dispatch is dispatch, so a
            // primary whose own host carries the last live workers (the
            // lone-survivor self-quorum path) keeps working its pool
            // instead of falsely stranding it (run_20260612_035452: the
            // old remote-only count read a healthy one-host fleet as
            // permanently zero and aborted mid-task). A genuinely-dead
            // co-located secondary is removed by the keepalive sweep's
            // unfiltered hard backstop like any other member, after which
            // the count honestly reads zero and this arm fires. The
            // split-brain stand-down of a superseded primary is owned by
            // the epoch mechanisms (the `run_aborted` gate above + the
            // demote hook on any self→other `PrimaryChanged`), never by
            // this progress detector.
            //
            // Tokenizer surfaced the original failure on cohort-3 where
            // SSH-tunnel blips killed all 5 secondaries at once and the
            // run sat idle until manually killed.
            // Gated on the graceful-abort latch: under the freeze the
            // queued pool is DELIBERATELY un-dispatchable (nothing will
            // ever dispatch it again, by design) and the remote
            // secondaries legitimately drain away one by one, so the
            // "no living secondary can dispatch the queued tasks" premise
            // is vacuous — arming fleet-dead here would misclassify the
            // graceful tail as a strand. The graceful tick above owns the
            // drain terminal.
            if !self.cluster_state.graceful_abort_requested()
                && self.cluster_state.alive_worker_secondary_count() == 0
                && !self.pool().is_empty()
            {
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
                        "fleet-dead timeout: every worker-secondary gone with non-empty pool; \
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

            // Background peer-mesh-formation deadline. Dispatch is decoupled
            // from mesh formation (the run dispatches+executes regardless),
            // but a ≥2-node fleet whose peer mesh never forms is a RUN-ABORT
            // condition — the peer mesh is the required failover substrate.
            // `mesh_formation_missing` is the LIVE predicate: `None` when the
            // requirement is satisfied (mesh formed) or not applicable (<2
            // compute nodes), `Some(missing)` for a ≥2-node fleet whose mesh
            // has not formed. Arm the one-shot deadline the first iteration the
            // requirement is observed unmet; clear it the moment the mesh forms
            // (slow-but-forms never aborts); on a genuine never-form, the
            // deadline elapses and we record the TYPED abort outcome
            // (`RunError::PeerMeshNotFormed`) + break — the SAME structured
            // write-by-arm / read-by-pipeline channel `worker_mgmt_fail_outcome`
            // uses for `RunShouldFail`/`PolicyFatalExit`. `run_operational_and_finalize`
            // then broadcasts the `RunAborted` verdict (each secondary tears down
            // its workers on the terminal CRDT flag — the existing run-abort
            // teardown) AND surfaces the typed error so the PyO3 boundary RAISES
            // non-zero. Returning a bare `Err(String)` here would map to
            // `RunError::Other`, which is SWALLOWED to exit 0 — the false-green
            // class the typed variant exists to prevent. Persistent `Instant`
            // (the fleet-dead / 1e914505 idiom), so it actually elapses on a
            // live cluster.
            match self.mesh_formation_missing() {
                None => {
                    // Satisfied or not-applicable: disarm so a later mesh
                    // degradation re-arms from its own start, not a stale one.
                    mesh_formation_deadline = None;
                }
                Some(missing) => {
                    let now = Instant::now();
                    let deadline = *mesh_formation_deadline
                        .get_or_insert_with(|| now + self.config.mesh_ready_timeout);
                    if now >= deadline {
                        let reason = self.mesh_formation_abort_reason(&missing);
                        tracing::error!(
                            missing = ?missing,
                            formed = self.mesh_ready_secondaries.len(),
                            timeout_s = self.config.mesh_ready_timeout.as_secs_f64(),
                            "mesh-formation timeout: the peer mesh (>=2 compute \
                             nodes, required for failover) never formed within \
                             the deadline — {} expected secondar(ies) never \
                             reported a formed mesh; ABORTING the run and killing \
                             all workers",
                            missing.len(),
                        );
                        self.record_run_fail_outcome(
                            crate::primary::error::RunError::PeerMeshNotFormed { reason },
                        );
                        break;
                    }
                }
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

            // Heartbeat-deadline fairness gate (#582). Defends against
            // hot-arm body monopolisation of the operational loop — a
            // body that does not yield between work units would otherwise
            // starve ARM_HEARTBEAT under #586's biased select! priority.
            // See `fire_heartbeat_if_overdue` for the full rationale.
            // Steady-state cost is one `Instant::elapsed()` comparison
            // per iteration when the heartbeat is not overdue.
            self.fire_heartbeat_if_overdue(
                &mut command_rx,
                &mut last_heartbeat_fire,
                &mut heartbeat_tick,
                &arm_stats,
            )
            .await?;

            // Per-task re-dispatch backoff wake deadline, recomputed
            // each iteration from the pool's PERSISTENT stored
            // eligible-at stamps (never derived relative to "now" at
            // the arm — the persistent-deadline law). Computed OUTSIDE
            // the `select!` so the backoff arm's future does not
            // borrow `self`.
            let task_backoff_due = self.next_task_dispatch_backoff_expiry();

            // Cancellation safety: `transport.recv_peer` is the
            // mpsc-backed unified inbound demux (cancel-safe — see
            // `TunneledPeerTransport::recv_peer`). The two timer
            // arms (heartbeat + anti-entropy ticks) are tokio time
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
            // ── Arm-priority discipline (#586) ─────────────────────────
            // `biased;` polls arms top-to-bottom in source order; the
            // FIRST READY arm wins each iteration. The order below is
            // the explicit priority: (1) terminal emergency signals
            // (panik / graceful_abort — rare oneshots that must always
            // win when they fire), (2) ARM_INBOX (data path: starvation
            // here = drain-strand and dep_graph-burst silence per
            // #545/#566 RCA), (3) ARM_COMMAND (external control: same
            // priority as inbox under the local manager's existing
            // pattern), (4) operational arms in source order. There is
            // no ARM_OOM_SWEEP on the primary (the OOM watcher runs
            // only inside `process_tasks` on the secondary side),
            // but the same arm-priority discipline applies: data-path
            // arms must never lose tiebreaks to forensic / periodic
            // work.
            //
            // ── #582 amendment: `biased;` is RACE-WINNER ordering only ──
            // `biased;` resolves ties at the moment of the select! wake:
            // when multiple arms are ready in the SAME poll, the
            // higher-priority arm wins. It does NOT bound how long the
            // winning arm's body runs; a body that does not yield to
            // siblings monopolises the loop until it returns. Under
            // sustained data-arm load (e.g. a streamed spawn batch into
            // ARM_COMMAND at 10 batches/sec), a non-yielding body in a
            // higher-priority arm starves every lower-priority arm —
            // including ARM_HEARTBEAT (id=4), whose silence then triggers
            // false-departure cascades (#582: 75 s collective-silence
            // episode, 11 false departures).
            //
            // The CONTRACT every higher-priority data-arm body must hold:
            // yield to siblings between work units. The two established
            // primitives:
            //   * the `spawn_continuation_queue` chunking pattern (#547,
            //     extended in #582 to drop the single-shot fast path) —
            //     for COMMAND-arm SpawnTasks, each chunk apply RETURNS to
            //     `select!` and rides a later iteration;
            //   * `tokio::task::yield_now().await` between bounded work
            //     units — for arms that lack a natural queue.
            //
            // The `fire_heartbeat_if_overdue` gate at the top of each
            // loop iteration is the LAST-LINE defence: even if a future
            // regression re-introduces a non-yielding hot body, the
            // heartbeat still fires within `keepalive_interval` of its
            // prior fire — peers never falsely depart.
            //
            // Mark the IDLE-window open immediately before awaiting the
            // `select!`: the span until the winning arm's body records its
            // id is the loop's idle (select!-wait) time; the span from that
            // record to this next `begin_select` is the prior arm's body
            // time (an out-of-band `fire_heartbeat_if_overdue` record above
            // is correctly attributed as a HEARTBEAT body span). Oploop
            // time-instrumentation — observation only.
            arm_stats.begin_select(std::time::Instant::now());
            tokio::select! {
                biased;
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
                // Operator graceful-abort trigger (SIGUSR2). A PRIMARY that
                // receives the operator's SIGUSR2 IS the abort authority — it
                // does NOT send a `GracefulAbortRequest` to itself (the
                // observer's arm does that); it short-circuits straight into
                // the SAME `initiate_graceful_abort` latch the wire handler
                // drives. Idempotent: a re-sent signal against an
                // already-latched freeze is a NoOp.
                //
                // The trigger is consumed through a DISJOINT-FIELD borrow of
                // `self.graceful_abort_trigger` (the `self.inbox.recv()`
                // pattern), NOT taken out into a loop local: a graceful-abort
                // relocation (`graceful_abort_tick` → `relocate_primary_to`)
                // cancels this loop mid-flight, and a taken-out trigger would
                // be dropped on that cancellation; left on `self`, it rides
                // `into_observer_handoff` onto the standalone observer so the
                // relocated node keeps responding to operator SIGUSR2.
                // `recv() == None` (stream closed) or `None` trigger
                // (un-injected) both PARK inside the trigger — never a
                // hot-loop. Cancel-safety: `GracefulAbortTrigger::recv` is
                // cancel-safe (see its doc); a sibling arm winning drops and
                // rebuilds the recv future without losing a queued signal.
                sig = async {
                    match self.graceful_abort_trigger.as_mut() {
                        Some(trigger) => trigger.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    arm_stats.record(ARM_GRACEFUL_ABORT);
                    if sig.is_some() {
                        let own_id = self.config.node_id.clone();
                        self.initiate_graceful_abort(&own_id).await;
                    }
                }
                // Single mesh inbound arm. Priority (#586): placed FIRST
                // among data arms under `biased;` — when the inbox is
                // ready, no lower-priority arm (matcher, worker_mgmt,
                // snapshot_stream, heartbeat, etc.) ever beats it to the
                // iteration. The pre-#586 unbiased select! placed this
                // arm after COMMAND/MATCHER/WORKER_MGMT/SNAPSHOT_STREAM
                // in source order, which under steady-state load gave
                // the periodic arms a structural tiebreak edge over the
                // data path — same shape as the secondary's
                // OOM_SWEEP-vs-INBOX starvation, now uniformly fixed
                // across both loops.
                //
                // Reframes the #545/#566 dep_graph-burst 6min silence:
                // under high spawn-batch arrival rate, the unbiased
                // select! let MATCHER / WORKER_MGMT win racing ties
                // over the inbox, the same arm-priority disease as
                // #586's OOM_SWEEP-vs-INBOX. The biased order below
                // resolves both.
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
                            // Batch-drain relief (#491, ported from the
                            // secondary): the awaited `recv` above is the ONE
                            // cancel-safe wait — the only future a sibling arm
                            // can cancel. Now committed to THIS iteration,
                            // synchronously sweep up to `INBOX_BATCH_DRAIN_CAP`
                            // MORE frames the channel already holds, in arrival
                            // order. `drain_ready` never awaits, so the whole
                            // sweep runs inside the arm body where no
                            // cancellation can occur (each frame is already
                            // removed from the channel, so the per-frame
                            // `.await?` processes an already-owned frame — no
                            // consume-then-await hazard). The cap bounds the
                            // burst so the loop still yields to every sibling
                            // arm each pass; a deeper backlog drains down across
                            // iterations. At affine scale the O(tasks)
                            // pull-model TaskRequest/TaskComplete arrival rate
                            // floods a single-frame-per-iteration drain, pinning
                            // the inbox arm ~95% and starving
                            // heartbeat/worker_mgmt → runtime-starvation freeze;
                            // the batch drain restores parity with the
                            // secondary's inbox arm. `arm_stats` counts ONE
                            // selection regardless of batch size (it measures
                            // select! wins, not frames).
                            let follow_on =
                                self.inbox.drain_ready(INBOX_BATCH_DRAIN_CAP);
                            for m in follow_on {
                                self.dispatch_message(m, &mut command_rx).await?;
                            }
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
                            self.react_to_worker_signal_batch(batch, &mut command_rx).await;
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
                stream_id = self.snapshot_streams.next_wake() => {
                    arm_stats.record(ARM_SNAPSHOT_STREAM);
                    // Snapshot-stream production arm: ONE bounded package
                    // per wakeup (the driver re-enqueues its own token
                    // when the stream has more), so a 100 MB ledger
                    // streams out interleaved with every other arm
                    // instead of serializing monolithically on the loop.
                    // The send awaits like any other egress; a send
                    // failure drops the stream (the requester resumes
                    // from its cursor on its next pull).
                    if let Some((dst, frame)) = self.snapshot_streams.emit_next(
                        &stream_id,
                        &self.cluster_state,
                        crate::primary::wire::timestamp_now(),
                    ) && let Err(e) = self.send_to(dst, frame).await
                    {
                        tracing::warn!(
                            stream_id = %stream_id,
                            error = %e,
                            "snapshot-stream package send failed; dropping stream \
                             (the requester's pull cadence resumes from its cursor)"
                        );
                        // The direct leg to the requester dropped; signal it
                        // via a PullFail (delivered INDIRECTLY through the
                        // relay) so its pull driver falls to the next target.
                        if let Some((requester, requester_is_observer)) =
                            self.snapshot_streams.abort_stream(&stream_id)
                        {
                            let (fail_dst, fail) = crate::pull_coordinator::pull_fail(
                                &self.config.node_id,
                                crate::primary::wire::timestamp_now(),
                                &requester,
                                requester_is_observer,
                                &stream_id,
                            );
                            let _ = self.send_to(fail_dst, fail).await;
                        }
                    }
                }
                // Disciplined-pull WAKE arm (#491 storm-killer): drives the
                // `pull_coordinator`'s probe/selection/rebalance timers off
                // its PERSISTENT `wake_deadline` (an absolute instant from
                // STORED state), NOT a relative sleep, so it fires under
                // constant sibling-arm activity (the watchdog law). `None`
                // (Idle — the authoritative primary's steady state) parks the
                // arm. Cancel-safe: `sleep_until` consumes nothing and the
                // deadline is recomputed each iteration from stored state.
                _ = async {
                    match self.pull_coordinator.wake_deadline() {
                        Some(due) => {
                            tokio::time::sleep_until(tokio::time::Instant::from_std(due)).await
                        }
                        None => std::future::pending().await,
                    }
                } => {
                    arm_stats.record(ARM_PULL);
                    for directive in self.pull_coordinator.tick(Instant::now()) {
                        self.drive_pull_directive(directive).await;
                    }
                }
                event = self.settled_spill.next_event() => {
                    arm_stats.record(ARM_SETTLED_SPILL);
                    // Settled-CRDT spill arm: cadence sweep (collect a
                    // batch of join-fixed-point entries, kick ONE
                    // spawn_blocking write) or a write completion
                    // (commit: evict fat bodies into the slim index).
                    // Cancel-safe (interval tick / mpsc recv); bounded
                    // per-wakeup work (one batch clone or one receipt).
                    self.settled_spill.handle(event, &mut self.cluster_state);
                }
                _ = heartbeat_tick.tick() => {
                    arm_stats.record(ARM_HEARTBEAT);
                    // The full per-cadence heartbeat body — F5 dispatch
                    // trigger + backlog observer + keepalive broadcast +
                    // beacon-target refresh + dead-secondary sweep — is
                    // owned by `service_heartbeat_tick` so the SAME call
                    // sequence runs whether reached via this on-cadence
                    // arm or via the heartbeat-deadline fairness gate
                    // (`fire_heartbeat_if_overdue`, #582). The
                    // `last_heartbeat_fire` stamp anchors that gate; updating
                    // it here keeps the gate accurate against the actual
                    // fires (not just the missed-cadence fires).
                    self.service_heartbeat_tick(&mut command_rx).await?;
                    last_heartbeat_fire = Instant::now();
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
                        // A PrimaryCoordinator is never an observer.
                        false,
                    );
                    let _ = self
                        .send_to(
                            dynrunner_protocol_primary_secondary::Destination::All,
                            frame,
                        )
                        .await;
                }
                event = async {
                    match respawn_lifecycle_rx.as_mut() {
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
                    match event {
                        Some(event) => {
                            // Single-line delegation: the routing
                            // (Removed → budget check + id mint +
                            // spawner invocation + JoinSet push;
                            // Added → pending-replacement
                            // reconciliation / revocation) lives in
                            // `primary::respawn` /
                            // `dispatch_respawn_lifecycle`. This arm
                            // only translates "an event arrived"
                            // into the call.
                            self.dispatch_respawn_lifecycle(event);
                        }
                        None => {
                            // Every sender dropped. Drop the
                            // receiver locally; the loop's other
                            // arms keep driving exit conditions.
                            // Same shape as the command-channel
                            // None arm.
                            respawn_lifecycle_rx = None;
                            tracing::debug!(
                                "respawn lifecycle channel closed; disabling \
                                 the respawn-lifecycle arm for the remainder \
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
                // Persistent-dial-failure drain (#542 cause-B). The QUIC
                // transport's reconnect tick crossed
                // `DIAL_SUMMARY_THRESHOLD` consecutive failed sweeps for
                // a peer and gave up; the operational-loop arm consumes
                // the id and, if the peer is an OBSERVER (in
                // `role_table.observers`), originates a
                // `ClusterMutation::PeerRemoved { cause:
                // PersistentDialFailure }`. Observer-only: secondaries
                // have an authoritative heartbeat-miss dead-declaration
                // path (`requeue_dead_secondary`), and adding a second
                // source would race that path; observers run no tasks
                // and emit no keepalives, so the dial-give-up signal IS
                // their authoritative removal trigger. A non-observer id
                // (a secondary, an unknown peer) is logged at DEBUG and
                // dropped — the dial-give-up is informational for
                // anyone-not-an-observer. Disabled-arm shape mirrors
                // `liveness_ping`: `None` rx → parks on `pending()`.
                dial_failure = async {
                    match persistent_dial_failure_rx.as_mut() {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    arm_stats.record(ARM_PERSISTENT_DIAL_FAILURE);
                    match dial_failure {
                        Some(peer_id) => {
                            self.handle_persistent_dial_failure(peer_id).await;
                        }
                        None => {
                            // Transport sender dropped (run winding
                            // down). Drop the receiver; without a wire
                            // to dial there's no leg to give up on.
                            persistent_dial_failure_rx = None;
                            tracing::debug!(
                                "persistent-dial-failure channel closed; \
                                 disabling the dial-failure arm for the \
                                 remainder of the loop"
                            );
                        }
                    }
                }
                // Mode-2 discovery completion arm (the concurrent-arm fix).
                // While `discovery_in_flight` holds the started `discover_items`
                // future (a relocated / pre-staged primary that owes discovery),
                // this arm awaits it CONCURRENTLY with every sibling arm — so
                // the ~6min collect-all does NOT park the loop's keepalive /
                // setup-servicing / inbox arms. `None` (every non-relocated /
                // already-settled primary, and after the single fire) parks the
                // arm on `pending().await`, the closed-channel hot-loop guard.
                //
                // Cancel-safety: the awaited `&mut inflight.future` is a
                // consumer future the driver only polls; a sibling arm winning
                // an iteration drops + re-creates the borrow on the next poll
                // WITHOUT losing progress, because the `Pin<Box<..>>` future
                // lives on `discovery_in_flight` (the loop-local), not in the
                // arm. We `take()` the in-flight only AFTER the await resolves,
                // so a cancelled poll leaves it intact for the next iteration.
                discovered = async {
                    match discovery_in_flight.as_mut() {
                        Some(inflight) => (&mut inflight.future).await,
                        None => std::future::pending().await,
                    }
                } => {
                    arm_stats.record(ARM_DISCOVERY);
                    // Fire-once: take the in-flight so the arm goes inert and a
                    // re-poll parks. `phase_deps` rides out with it for the seed.
                    let phase_deps = discovery_in_flight
                        .take()
                        .map(|i| i.phase_deps)
                        .unwrap_or_default();
                    match discovered {
                        Ok(binaries) => {
                            // ON-RESOLVE: seed the ledger + re-hydrate + fire
                            // phase starts + cascade + signal dispatch. Owns the
                            // whole post-await tail the sequential driver ran in
                            // the pre-loop (now deferred to here). A composition
                            // `Err` surfaces as the loop's `Err` exactly as the
                            // sequential `discover_on_promotion().await?` did —
                            // the abort verdict was latched + broadcast inside.
                            self.finish_discovery_arm(binaries, phase_deps, &mut command_rx)
                                .await
                                .map_err(|e| e.to_string())?;
                        }
                        Err(reason) => {
                            // The consumer `discover_items` policy errored — the
                            // run cannot seed. Same outcome as the sequential
                            // driver's `(sd.discover)().await.map_err(..)?`:
                            // surface as the loop's `Err`, which the pipeline
                            // tail classifies into the run-fatal exit.
                            tracing::error!(
                                error = %reason,
                                "mode-2 discovery policy failed; aborting the run"
                            );
                            return Err(reason);
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
                // Per-task re-dispatch backoff wake. `task_backoff_due`
                // (computed at the top of this iteration) is the pool's
                // earliest STORED eligible-at instant across queued
                // backed-off tasks — a persistent deadline, so parking
                // on the absolute instant survives sibling arms winning
                // every iteration (the watchdog law; a relative sleep
                // would re-arm forever on a busy mesh). When it fires,
                // a previously-hidden task just became dispatch-
                // eligible while every worker may already be parked
                // ("no work" was the answer the whole window) — EMIT a
                // `TasksAdded` onto the decoupled worker-management bus
                // (never a direct dispatch call) so the worker-
                // management arm coalesces it into one batched recheck.
                // The wake is LEVEL-triggered (see `pending_pool::backoff`):
                // if that single recheck MISSES (the eligible task could
                // not be placed — no idle worker, transport-gate skip,
                // affine-dep), the next iteration's recompute returns a
                // BOUNDED re-poll instant (`now + interval`), so this arm
                // re-fires until the task is actually dispatched instead
                // of parking on `pending()` forever after one fire (the
                // #640 25-min dispatch deadlock). The interval is bounded,
                // so an undispatchable task is re-checked once per
                // interval — never a hot-spin.
                _ = async {
                    match task_backoff_due {
                        Some(due) => tokio::time::sleep_until(due.into()).await,
                        None => std::future::pending().await,
                    }
                } => {
                    arm_stats.record(ARM_TASK_BACKOFF);
                    self.cluster_state
                        .emit_worker_mgmt(crate::worker_signal::WorkerMgmtSignal::TasksAdded);
                }
            }
        }

        // Unconditional final arm-stats emit. Every `break` above converges
        // here, so this fires exactly once per loop run regardless of how the
        // loop terminated (run-complete, run-should-fail, RunAborted,
        // graceful-abort, transport-closed). The periodic 120 s emit only fires
        // for long runs; a short burst that completes inside one interval would
        // otherwise leave ZERO `"oploop arm stats"` lines, blinding operators
        // to the arm distribution / control-plane starvation on exactly the
        // runs the line exists to diagnose. Observation-only — the emit body +
        // line shape are owned by `OpLoopArmStats` (see `emit_final`); this site
        // only calls the public method.
        arm_stats.emit_final();

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
        // Same rationale for the respawn lifecycle receiver: retry
        // passes re-enter the operational loop and a death (or a
        // reconciling join) observed during a retry pass should still
        // drive the dispatcher.
        self.respawn_lifecycle_rx = respawn_lifecycle_rx;
        // Same rationale for the liveness-beacon ping receiver: a retry
        // pass that re-enters the operational loop must keep refreshing
        // death-clocks from beacon datagrams (the union half), or a busy
        // secondary's beacon would stop counting during retry passes.
        self.liveness_ping_rx = liveness_ping_rx;
        // Same rationale for the persistent-dial-failure receiver: a
        // retry pass that re-enters the operational loop must keep
        // listening for dial-give-up signals so a stale observer
        // entry detected during a retry still drives the PeerRemoved
        // (#542 cause-B).
        self.persistent_dial_failure_rx = persistent_dial_failure_rx;
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
