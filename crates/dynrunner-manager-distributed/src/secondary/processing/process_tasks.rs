//! The secondary's operational `select!` loop body.
//!
//! Single concern: drive one secondary's tick-driven event loop.
//! Inbound primary messages, peer messages, worker events, and timers
//! all funnel through this select! and route to the appropriate
//! handler in sibling modules. The body is intentionally large
//! because the select! arm-set IS the loop's API surface; per-arm
//! extraction would force every arm to plumb the loop-state (timers,
//! interval handles, OOM watcher) back through method parameters.
//!
//! Length exception: this file is over the 500-line threshold (the
//! select! loop runs ~700 lines including doc/comments). Documented
//! in `secondary/processing/mod.rs`.

use std::collections::HashSet;

use dynrunner_core::{Identifier, WorkerId};
use dynrunner_manager_local::WorkerFactory;
use dynrunner_manager_local::oom::{
    DEFAULT_HEARTBEAT_INTERVAL, OomWatcher, OomWatcherConfig, SAMPLE_SWEEP_INTERVAL,
};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{Destination, PeerId};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use super::super::{RunOutcome, SecondaryCoordinator};

// ── Secondary `process_tasks` `select!` arm ids (oploop instrumentation) ──
//
// One id per arm of the `select!` below, in source order. `ARM_INBOX` is the
// INBOUND (mesh ingress) arm. Index == id with [`PROCESS_TASKS_ARM_NAMES`];
// each arm body records its id as its first statement. Observation-only — see
// [`crate::oploop_instrumentation`].
const ARM_POOL_EVENT: usize = 0;
const ARM_INBOX: usize = 1;
const ARM_ANNOUNCER_OUTBOX: usize = 2;
const ARM_KEEPALIVE: usize = 3;
const ARM_OOM_SWEEP: usize = 4;
const ARM_PANIK: usize = 5;
const ARM_FATAL_EXIT: usize = 6;
const ARM_ANTI_ENTROPY: usize = 7;
const ARM_SECONDARY_CONTROL: usize = 8;
const ARM_REPORT_REPLAY: usize = 9;
const ARM_WORKER_RESTART: usize = 10;
const ARM_SNAPSHOT_STREAM: usize = 11;
const ARM_SETTLED_SPILL: usize = 12;
const ARM_PULL: usize = 13;
const ARM_AFFINE_IMPORT: usize = 14;

/// Per-iteration inbox batch-drain bound (#491). After the awaited
/// `inbox.recv()` arm yields ONE frame, the arm body synchronously sweeps
/// up to this many MORE already-queued frames before yielding back to the
/// `select!`. This is the drain-rate relief: the operational loop's other
/// arms run O(ledger) work per iteration (digest folds on inbound
/// `StateDigest`, fresh snapshot-stream plan builds on
/// `RequestSnapshotStream`), so a one-frame-per-iteration drain lost to a
/// faster ingress rate let the unbounded mesh inbox grow without bound (the
/// relocated-primary RSS leak). Draining a bounded BATCH each pass
/// multiplies the drain throughput so the loop keeps up; the cap bounds the
/// burst so the loop still yields to every sibling arm (keepalive,
/// election, OOM sweep, the pull arm) within the iteration. A backlog
/// larger than the cap simply drains DOWN across consecutive iterations.
/// Nothing is dropped — the cap length-slices, it does not shed. Honest
/// per-iteration backpressure that survives independent of the storm; it is
/// folded in here with the pull-model rather than shipped separately.
const INBOX_BATCH_DRAIN_CAP: usize = 256;

/// Arm names, index-aligned with the `ARM_*` ids above (render order of the
/// compact stats line). The single `oom_sweep` arm counts SWEEPS (one
/// self-paced read-all-workers + decide pass per ~50ms), NOT the former
/// per-worker sample / decision fires.
const PROCESS_TASKS_ARM_NAMES: &[&str] = &[
    "pool_event",
    "inbox",
    "announcer_outbox",
    "keepalive",
    "oom_sweep",
    "panik",
    "fatal_exit",
    "anti_entropy",
    "secondary_control",
    "report_replay",
    "worker_restart",
    "snapshot_stream",
    "settled_spill",
    "pull",
    "affine_import",
];

impl<M, S, E, I> SecondaryCoordinator<M, S, E, I>
where
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    pub(in crate::secondary) async fn process_tasks(
        &mut self,
        factory: &mut impl WorkerFactory<M>,
    ) -> Result<RunOutcome, String> {
        tracing::info!("entering task processing loop");

        // SINGLE `Configuring → Operational` boundary. On the normal path
        // `wait_for_setup` left the lifecycle in `Configuring` (pool
        // spawned, got_* trio landed); this transition moves the pool +
        // the carried-forward config flags + the freshly-populated
        // `active_tasks` into `OperationalState` and stands up the
        // operational runtime (election `Normal`, no primary seen yet,
        // empty peer-keepalives, a fresh `PrimaryLink` from config, empty
        // pending collections). It is ALSO the single fire-once site where
        // the take-once runtime latches are surrendered: we build
        // `OperationalLatches` from the coordinator's `Option` slots and
        // get the unwrapped handles back to drive the `select!` below.
        //
        // The late-joiner observer (which `restore_from_snapshot_and_skip_setup`
        // landed directly in `Operational`) finds the lifecycle already
        // `Operational`, so the transition is a no-op on the state — and the
        // three fire-once latches' `Option::take()`s yield `None`, so the
        // loop's latch locals park exactly as the pre-typed flat-field flow
        // did. The carrier ferries only the receivers
        // the operational `select!` actually polls. `lifecycle_rx` /
        // `task_completed_rx` were already taken in
        // `run_until_setup_or_done_inner` to spawn their dispatchers, and the
        // `command_rx` ingress is re-homed to the authoritative primary —
        // none is drained here, so none rides this carrier. The panik signal
        // receiver is taken straight off its coordinator slot into a
        // loop-local below (not via this carrier).
        let latches = super::super::lifecycle::OperationalLatches {
            announcer_outbox_rx: self.announcer_outbox_rx.take(),
            fatal_exit_signal_rx: self.fatal_exit_signal_rx.take(),
        };
        let primary_link = super::super::primary_link::PrimaryLink::with_failover_threshold(
            self.config.primary_link_failure_threshold,
            self.config.primary_link_failure_window,
        );
        let lifecycle = std::mem::replace(
            &mut self.lifecycle,
            super::super::lifecycle::SecondaryLifecycle::connecting(),
        );
        let (lifecycle, latches) = lifecycle.enter_operational(
            latches,
            super::super::election::ElectionState::Normal,
            None,
            std::collections::HashMap::new(),
            primary_link,
            Vec::new(),
            std::collections::HashMap::new(),
            std::collections::HashMap::new(),
        );
        self.lifecycle = lifecycle;
        let super::super::lifecycle::OperationalLatches {
            mut announcer_outbox_rx,
            mut fatal_exit_signal_rx,
        } = latches;
        // Operational-entry restart sweep: a SETUP-phase typed-spawn
        // death (the initial-assignment `ensure_worker_for_type` Err
        // arm) leaves its slot startup-dead with no
        // `pending_worker_restarts` entry — the restart machinery
        // lives in operational state and is unreachable during setup.
        // Sweep every startup-dead slot into the standard backed-off
        // restart schedule NOW, so no slot enters the operational loop
        // permanently dead (it would otherwise bounce every future
        // assignment as "no idle worker" forever).
        let startup_dead: Vec<dynrunner_core::WorkerId> = self
            .op_mut()
            .pool
            .workers
            .iter()
            .filter(|w| w.is_startup_dead())
            .map(|w| w.worker_id)
            .collect();
        for wid in startup_dead {
            tracing::warn!(
                worker_id = wid,
                "slot entered the operational loop startup-dead (setup-phase \
                 typed-spawn death); scheduling its backed-off restart"
            );
            self.schedule_worker_restart(wid);
        }

        // Take the panik-watcher signal receiver out of `self` into a
        // loop-local so the panik arm's `await` can own it across `select!`
        // iterations. A loop-local is required here (it cannot be polled
        // in-place off the coordinator) because the pool arm already holds a
        // `&mut self.lifecycle` partial borrow for its own recv future; a
        // second `&mut self` for the panik arm would conflict. `None` when no
        // panik paths were configured (the arm parks on `pending()`).
        let mut panik_signal_rx = self.panik_signal_rx.take();
        // Take the secondary control-plane ingress receiver into a
        // loop-local for the same partial-borrow reason as
        // `panik_signal_rx`. `None` only if a previous `process_tasks`
        // entry already took it (the loop runs once per operational
        // span) — the arm then parks on `pending()`.
        let mut secondary_control_rx = self.secondary_control_rx.take();
        // Off-loop SecondaryAffine-import completion receiver into a loop-local
        // for the same partial-borrow reason as `secondary_control_rx` (#497
        // P5). `None` only if a previous `process_tasks` entry already took it;
        // the arm then parks on `pending()`.
        let mut affine_import_rx = self.affine_import_rx.take();

        let mut keepalive_interval = tokio::time::interval(self.config.keepalive_interval);
        // Skip (not Burst) missed ticks: after a host suspend/resume the
        // default Burst would fire one catch-up tick per missed interval,
        // bursting a flurry of keepalives at once. Skip collapses the backlog
        // to exactly one catch-up tick, so liveness resumes at the normal
        // cadence instead of a post-resume storm.
        keepalive_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // The OOM memory accounting runs as ONE self-paced sweep (read
        // all workers' cgroup charges off the async runtime via
        // `spawn_blocking`, apply + decide inline), not per-fire timer
        // arms. See `crate::oom` in dynrunner-manager-local. Activate
        // kernel-OOM detection by passing the workers cgroup
        // `memory.events` path (when the nested workers subgroup was
        // materialised; flat-layout fallback leaves it `None`).
        let workers_memory_events_path = self
            .op_mut()
            .pool
            .workers_cgroup()
            .map(|h| h.workers_path().join("memory.events"));
        let mut oom_watcher = OomWatcher::new_with_workers_cgroup(
            OomWatcherConfig {
                sample_interval: SAMPLE_SWEEP_INTERVAL,
                heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
                log_enabled: self.config.log_oom_watcher,
            },
            workers_memory_events_path,
        );
        let oom_sweep_interval = oom_watcher.sweep_interval();

        // Tell the primary the peer-mesh has settled so it can release
        // its `PrimaryChanged` announcement. For the single-secondary /
        // no-peers case
        // (`peer_dial_count == 0`) this is the only place the signal
        // gets emitted — `check_peer_mesh_watchdog` has nothing to do
        // (no deadline armed) and would never fire MeshReady.
        // For the multi-secondary case, this is racy with the keepalive
        // tick's watchdog call: whichever observes a settled state
        // first wins, the other becomes a no-op via `mesh_ready_sent`.
        // peer.rs owns the decision; we just call.
        self.report_mesh_ready_if_needed().await;

        // Request tasks only for workers that didn't get initial assignments
        let worker_count = self.op_mut().pool.workers.len();
        for i in 0..worker_count {
            if self.op_mut().pool.workers[i].is_idle_state() {
                self.request_task_for_worker(i as WorkerId).await?;
            }
        }

        // `announcer_outbox_rx` and `fatal_exit_signal_rx` were surrendered
        // into the loop's locals at the single `enter_operational` latch
        // boundary above (the take-once carrier), so they own their receivers
        // across the `select!` iterations exactly as the pre-typed flat-field
        // flow did with per-field `Option::take()`. Each is `None` when its
        // registration never happened (no observer announcer / no fatal-exit
        // policy) and the matching arm parks on `pending()`. `panik_signal_rx`
        // is the loop-local taken off the coordinator slot above; it is `None`
        // only when no panik paths were configured.
        //
        // Grace for the SIGTERM → SIGKILL escalation on the worker pool is
        // 5s — same window the SubprocessWorkerFactory uses for its own
        // teardown ladder.
        const PANIK_KILL_GRACE: std::time::Duration = std::time::Duration::from_secs(5);

        // Anti-entropy cadence. On each tick this secondary broadcasts its
        // `StateDigest` so peers behind it pull a snapshot (and vice
        // versa). The period carries a deterministic per-node jitter (from
        // `secondary_id`) so the fleet's digests spread across the window
        // instead of bursting together. `Skip` collapses a post-suspend
        // backlog to one catch-up tick; `reset()` drops the immediate first
        // tick so the first broadcast lands one full period in.
        let mut anti_entropy_interval =
            tokio::time::interval(crate::anti_entropy::tick_period(&self.config.secondary_id));
        anti_entropy_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        anti_entropy_interval.reset();

        // The secondary holds NO authority and processes NO command
        // channel: the externally-issued `PrimaryCommand`s
        // (FailPermanent / ReinjectTask / UpdatePreferredSecondaries /
        // SpawnTasks) are authority mutations whose only correct owner
        // is the same-peer `PrimaryCoordinator` (which runs the command
        // arm in its own operational loop). The `command_tx` /
        // `command_rx` fields stay on the struct as the registration
        // anchor keeping the PyO3 `PrimaryHandle` clone a stable type;
        // the composed-primary runtime hands the receiver to the
        // primary's command loop. This secondary loop never drains it.

        // Per-iteration arm accounting (observation only — see
        // `crate::oploop_instrumentation`). The secondary twin of the
        // primary's `op_loop_arm_stats`: the co-located topology runs both
        // loops on one runtime, so the watchdog must be able to name a wedged
        // arm on either. Each arm body records its id as its first statement;
        // published on `self.op_loop_arm_stats` for the runtime-watchdog dump
        // path.
        let arm_stats = crate::oploop_instrumentation::OpLoopArmStats::new(
            PROCESS_TASKS_ARM_NAMES,
            ARM_INBOX,
        );
        self.op_loop_arm_stats = Some(std::sync::Arc::clone(&arm_stats));
        // Publish into the watchdog bridge cell (labelled "secondary") if
        // wired; the returned guard clears the "secondary" entry on EVERY exit
        // of this function — the clean `break` tail AND the panik / fatal /
        // run-aborted early returns — without a per-exit `clear` call. The
        // co-located primary's "primary" entry is untouched (role-keyed cell).
        let _arm_stats_guard = self
            .op_loop_arm_stats_cell
            .as_ref()
            .map(|cell| cell.publish_scoped("secondary", std::sync::Arc::clone(&arm_stats)));

        // OOM sweep wake deadline. Seeded to NOW so the first sweep
        // fires immediately; re-armed `oom_sweep_interval` after EACH
        // sweep completes (await-before-resleep — a slow sweep cannot
        // pile, the next starts a full interval after the previous
        // returned). Persistent local state like the replay / restart
        // deadlines, so a busy loop cannot starve the sweep.
        let mut next_sweep_due = tokio::time::Instant::now();

        loop {
            // Worker-restart wake deadline: the EARLIEST due instant
            // across the scheduled slot restarts (None parks the arm —
            // nothing pending). Same persistent-deadline shape as the
            // replay arm below: the per-slot due instant is STORED at
            // schedule time (`schedule_worker_restart`), so the arm's
            // re-created `sleep_until` always re-targets the same
            // stored instant — sibling arms firing cannot push a
            // backed-off respawn out (the watchdog-fires-under-load
            // law), and a zero-delay (healthy-death) restart is due
            // immediately, preserving the historical
            // restart-on-the-same-iteration semantics.
            let next_restart_due = self.next_worker_restart_due();

            // Replay wake deadline: the EARLIEST `next_due` across the
            // retained confirmable reports (None parks the arm —
            // nothing pending). Recomputed each iteration from the
            // PERSISTENT per-entry schedule state, so the arm's
            // re-created `sleep_until` always targets the same stored
            // instant — sibling arms firing cannot push the deadline
            // back (the watchdog-fires-under-load law). Buffer changes
            // (a new retention from any arm's send, an inbound
            // TerminalAck dropping an entry) are picked up here on the
            // next iteration.
            let next_replay_due = self.next_report_replay_due();

            // Cancellation safety note: every awaiting arm here must be
            // cancel-safe because the periodic ticks (keepalive, oom)
            // will cancel the in-flight recv/event futures whenever
            // they fire. `pool.recv_event` is `mpsc::Receiver::recv`
            // (documented cancel-safe). `inbox.recv` is the role's mesh
            // inbound stream, an `mpsc::UnboundedReceiver::recv` fed by the
            // mesh-pump's demux (cancel-safe).
            // `interval.tick` is itself cancel-safe per tokio docs.
            tokio::select! {
                // The pool lives inside `OperationalState`. The recv
                // future is built via the `self.lifecycle.operational_mut()`
                // FIELD path (a partial borrow of `self.lifecycle`), NOT
                // the `op_mut()` coordinator method (which borrows ALL of
                // `self` and would conflict with the sibling
                // `self.inbox.recv()` arm). The two recv futures then borrow
                // disjoint fields (`self.lifecycle` vs `self.inbox`), and
                // both borrows release the moment `select!` picks a winner,
                // so the arm BODIES are free to use `&mut self` methods. The
                // `expect` is sound: this loop runs only after the
                // `enter_operational` transition above.
                event = self
                    .lifecycle
                    .operational_mut()
                    .expect("process_tasks loop runs only when Operational")
                    .pool
                    .recv_event() =>
                {
                    arm_stats.record(ARM_POOL_EVENT);
                    if let Some(event) = event {
                        // The full per-event sequence (causal fence +
                        // handler + restart scheduling) is owned by
                        // [`Self::process_worker_pool_event`] — one seam
                        // the loop arm and the seam's tests share.
                        self.process_worker_pool_event(
                            event,
                            &oom_watcher,
                            &mut secondary_control_rx,
                        )
                        .await?;
                    }
                }
                // Snapshot-stream production arm: ONE bounded package per
                // wakeup (the driver re-enqueues its own token while the
                // stream has more), so serving a joiner's bootstrap pull
                // interleaves with every other arm instead of serializing
                // a 100 MB ledger monolithically on the loop. Borrows
                // `self.snapshot_streams` only (disjoint from the sibling
                // arms' fields). Cancel-safe: a single mpsc recv.
                stream_id = self.snapshot_streams.next_wake() => {
                    arm_stats.record(ARM_SNAPSHOT_STREAM);
                    if let Some((dst, frame)) = self.snapshot_streams.emit_next(
                        &stream_id,
                        &self.cluster_state,
                        crate::secondary::wire::timestamp_now(),
                    ) && let Err(e) = self.send_to(dst, frame).await
                    {
                        tracing::warn!(
                            stream_id = %stream_id,
                            error = %e,
                            "snapshot-stream package send failed; dropping stream \
                             (the requester's pull cadence resumes from its cursor)"
                        );
                        // The direct leg to the requester dropped mid-transfer;
                        // signal it via a PullFail (delivered INDIRECTLY through
                        // the relay) so its pull driver falls to the next target
                        // instead of waiting out the 30s re-probe.
                        if let Some((requester, requester_is_observer)) =
                            self.snapshot_streams.abort_stream(&stream_id)
                        {
                            let (fail_dst, fail) = crate::pull_coordinator::pull_fail(
                                &self.config.secondary_id,
                                crate::secondary::wire::timestamp_now(),
                                &requester,
                                requester_is_observer,
                                &stream_id,
                            );
                            let _ = self.send_to(fail_dst, fail).await;
                        }
                    }
                }
                // Settled-CRDT spill arm: cadence sweep (collect a batch
                // of join-fixed-point entries, kick ONE spawn_blocking
                // write) or a write completion (commit: evict fat bodies
                // into the slim index). Cancel-safe (interval tick / mpsc
                // recv); bounded per-wakeup work.
                event = self.settled_spill.next_event() => {
                    arm_stats.record(ARM_SETTLED_SPILL);
                    self.settled_spill.handle(event, &mut self.cluster_state);
                }
                // Single mesh inbound arm. There is one role inbound
                // stream — the mesh-pump demuxes every wire frame addressed
                // to this secondary's slot onto `self.inbox`. Failover-health
                // is driven by the send-side no-route probe plus the
                // keepalive time-axis in `check_primary_link_threshold`. A
                // `None` here means every write end of the slot's inbound
                // dropped (role teardown): no more frames can ever arrive,
                // so exit cleanly — the historical "inbound closed = end of
                // run" contract.
                msg = self.inbox.recv() => {
                    arm_stats.record(ARM_INBOX);
                    match msg {
                        Some(m) => {
                            self.handle_inbound(m, factory).await;
                            // Batch-drain relief (#491): the awaited `recv`
                            // above is the ONE cancel-safe wait — the only
                            // future a sibling arm can cancel. Now that it
                            // yielded a frame we are committed to THIS
                            // iteration, so synchronously sweep up to
                            // `INBOX_BATCH_DRAIN_CAP` MORE frames the channel
                            // already holds and handle them in arrival order.
                            // `drain_ready` never awaits, so the whole sweep
                            // runs inside the arm body where no cancellation
                            // can occur (no consume-then-await hazard). The
                            // cap bounds the burst so the loop still yields to
                            // every sibling arm each pass; a deeper backlog
                            // drains down across iterations. The arm-stat
                            // counts ONE selection regardless of batch size
                            // (it measures select! wins, not frames).
                            for m in self.inbox.drain_ready(INBOX_BATCH_DRAIN_CAP) {
                                self.handle_inbound(m, factory).await;
                            }
                        }
                        None => {
                            tracing::info!(
                                "mesh inbound stream closed; exiting cleanly"
                            );
                            break;
                        }
                    }
                }
                // Announcer-outbox drain. The observer-mode
                // [`crate::observer::announcer::PeerMeshAnnouncerSender`]
                // posts each holdings-announce DistributedMessage onto
                // the bounded outbox; this arm drains one item per
                // iteration, issues the actual `peer_transport.send`,
                // and replies via the item's oneshot so the
                // announcer task's `send_holdings` resolves with the
                // delivery outcome.
                //
                // # Why an arm rather than a non-select drain
                //
                // The drain has to await `peer_transport.send`, which
                // takes `&mut self`. Hoisting it out of `select!`
                // would mean serialising every iteration on a send
                // call that's only relevant when the outbox is non-
                // empty; placing it inside the select preserves the
                // structure-of-control the other &mut-self arms
                // already use (one wire-touch per iteration, paired
                // with `recv` to gate).
                //
                // # Cancel-safety
                //
                // `mpsc::Receiver::recv` is documented cancel-safe.
                // When a sibling arm wins the race, the in-flight
                // `recv` future is dropped and re-built on the next
                // iteration; no item is consumed without being
                // handled.
                //
                // # Parked when no announcer attached
                //
                // Non-observer secondaries (and pre-attach observer
                // coordinators) leave `announcer_outbox_rx = None`;
                // the arm parks on `pending().await` and never fires.
                outbox_item = async {
                    match announcer_outbox_rx.as_mut() {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    arm_stats.record(ARM_ANNOUNCER_OUTBOX);
                    if let Some(item) = outbox_item {
                        // Observer holdings update → the primary. Route
                        // through the `Destination::Primary` egress edge
                        // (NOT `send_to_primary`, whose failover-health
                        // probe is for the secondary's own primary-link
                        // liveness, not an observer announce). The edge
                        // resolves the concrete primary peer; cache-cold
                        // is surfaced back through `reply` as `Err`,
                        // tripping the announcer's retry-with-backoff.
                        let send_result =
                            self.send_to(Destination::Primary, item.msg).await;
                        // A dropped `item.reply` receiver means the
                        // announcer task was aborted between
                        // `send_holdings`'s outbox push and the
                        // drain — drop the result, the next trigger
                        // (or task shutdown) will reconcile.
                        let _ = item.reply.send(send_result);
                    }
                    // None from `rx.recv()` means every announcer-
                    // side sender (the announcer task's clone and
                    // any other sites holding `announcer_outbox_tx`)
                    // has been dropped. The `self.announcer_outbox_tx`
                    // clone on the coordinator keeps the channel
                    // alive across that drop, so a `None` here would
                    // only fire after the coordinator's own clone is
                    // also released — which never happens inside
                    // `process_tasks`. Drop the value (no-op).
                }
                _ = keepalive_interval.tick() => {
                    arm_stats.record(ARM_KEEPALIVE);
                    // Own-tick-health gate FIRST, before any silence-based
                    // judgment this arm drives. A keepalive tick that fires
                    // long past its cadence means THIS node's runtime was
                    // frozen/starved (the wake-from-freeze face: the
                    // `MissedTickBehavior::Skip` arm fires one catch-up tick
                    // the instant the runtime unfreezes, BEFORE the mesh pump
                    // has drained the inbound backlog into the liveness
                    // clocks). Feeding the tick here re-bases the shared
                    // trustworthy floor, so the peer-keepalive reaper
                    // (`check_peer_timeouts`) and the primary-silence legs
                    // (`run_election_tick`) below read `now -
                    // trustworthy_anchor(last_seen)` and measure ZERO silence
                    // across the frozen window — judging peers from fresh,
                    // post-lag evidence instead of declaring live peers dead
                    // off our own stall (#423).
                    self.own_tick_health.observe_tick(std::time::Instant::now());
                    // Periodic collection-stats line (accumulation
                    // visibility for the unbounded collections) — cheap
                    // off-cadence, emits every COLLECTION_STATS_INTERVAL.
                    self.observe_collection_stats();
                    self.send_keepalive().await;
                    self.check_peer_timeouts();
                    self.check_peer_mesh_watchdog().await;
                    // R1 time-axis arming: once a recv-None has been
                    // recorded, the count-axis trigger may not fire
                    // (e.g. recv was gated immediately so no further
                    // probes accrue, or the recv future stays
                    // pending). The time-axis half of the threshold
                    // catches that case: every tick, ask the
                    // primary-link health sub-state whether the
                    // failure window has elapsed. If yes, arm
                    // failover the same way the recv-arm path does.
                    self.check_primary_link_threshold();
                    // Re-poll any worker that's been idle since its
                    // last unsatisfied request. The secondary holds no
                    // retry machine: per-phase retry re-injection is the
                    // AUTHORITY's concern (the live primary, or this
                    // node's same-peer primary once promoted), driven by
                    // its phase-drain cascade — so this keepalive arm
                    // needs no retry-pass call, only the safety-net
                    // idle-worker re-poll.
                    // Re-poll any worker that's been idle since its
                    // last unsatisfied request. The per-worker rate
                    // limit (in `primary_link`, doubles on each
                    // empty-response, capped at 60s) keeps this
                    // cheap; without the periodic call, an idle
                    // worker that got "no work" once sits forever
                    // because the only other re-poll trigger is its
                    // OWN task completion (processing.rs:193) and an
                    // idle worker by definition has no task to
                    // complete. Most-load case: regular primary fires
                    // `dispatch_to_idle_workers` after every other
                    // worker's TaskComplete to push assignments,
                    // which mostly shadows this — but the
                    // primary path doesn't track per-peer worker
                    // idleness, so the periodic re-poll is the
                    // failover-safe wakeup.
                    self.repoll_idle_workers().await;
                    // #556 — drive the secondary mesh-consensus FSM on
                    // the keepalive cadence (~1s, same as this arm)
                    // so per-target probe deadlines fire on time even
                    // when no consensus frames are arriving. A no-op in
                    // `Idle`; bounded per-target probe-fan work in the
                    // active rounds.
                    self.drive_consensus_fsm().await;
                    let actions = self.run_election_tick();
                    for msg in actions.broadcast {
                        let _ = self.send_to(Destination::All, msg).await;
                    }
                    // Lone-survivor self-quorum commit: the tick transitioned
                    // the election to `Promoted` because this candidate already
                    // met quorum with its own single confirm (no peer
                    // `PromotionConfirm` will ever arrive). Drive the SAME
                    // terminal action the peer-confirm path drives off
                    // `record_promotion_confirm` — originate + locally apply +
                    // broadcast `PrimaryChanged { new = self }`.
                    if actions.promoted {
                        self.fire_local_promotion().await;
                    }
                }
                // Self-paced OOM sweep arm. Parks on the stored
                // `next_sweep_due` deadline; on fire it reads ALL
                // workers' cgroup charges off the async runtime
                // (`spawn_blocking`), applies them, runs the pressure
                // decision inline, then re-arms the deadline. ONE
                // operational-loop wakeup per sweep replaces the former
                // per-fire sample + decision ticks (the 58%-of-wakeups
                // blocking-IO hot path). The arm-stats `oom_sweep` count
                // is therefore SWEEPS, not per-worker fires.
                //
                // Cancel-safety: `sleep_until` consumes nothing and the
                // deadline is PERSISTENT local state, so a sibling arm
                // winning merely re-creates the future against the SAME
                // instant next iteration — the sweep cannot be starved
                // into never firing by a busy loop.
                _ = tokio::time::sleep_until(next_sweep_due) => {
                    arm_stats.record(ARM_OOM_SWEEP);
                    // Collect the CURRENT worker set's read inputs
                    // (respawns / type-shifts picked up each sweep),
                    // then read off the runtime — no pool borrow held
                    // across the blocking call.
                    let inputs = oom_watcher.collect_sweep_inputs(&self.op_mut().pool);
                    let sweep = tokio::task::spawn_blocking(move || inputs.read())
                        .await
                        .expect("oom charge sweep read panicked");
                    oom_watcher.apply_sweep(&mut self.op_mut().pool, sweep);
                    self.check_resource_pressure_via_watcher(&mut oom_watcher, factory).await;
                    // Per-worker phase-progress observability — the
                    // SAME shared seam LocalManager fires off ITS sweep
                    // (manager/worker_loop.rs). A long quiet task (deep
                    // in a native op, emitting no keepalive/phase) now
                    // produces an escalating "worker N in phase X for
                    // 60s/120s/..." WARN ON THE SECONDARY too, so the
                    // operator sees alive-and-churning instead of a
                    // silent freeze. LOGGING ONLY: no force-fail /
                    // timeout / kill is wired here (the secondary's
                    // userland-kill path stays gated off; kernel
                    // cgroup-OOM owns death). The config borrow is taken
                    // disjoint from the `op_mut()` pool borrow.
                    let intervals = self.config.phase_status_log_intervals.clone();
                    self.op_mut().pool.report_stuck_workers(&intervals);
                    // Await-before-resleep: arm the next sweep a full
                    // interval after THIS one completed.
                    next_sweep_due = tokio::time::Instant::now() + oom_sweep_interval;
                }
                // Panik (operator-initiated emergency stop) arm. The
                // watcher's `oneshot::Receiver<PanikSignal>` resolves
                // with `Ok(signal)` the moment the watcher task
                // observes any of its configured sentinel paths. `Err`
                // means the sender dropped (watcher task aborted or
                // configured with empty paths); we ignore the Err
                // arm and let the future never re-fire — taking the
                // `Option<_>` to `None` after the first resolution
                // would otherwise hot-loop the select. The
                // `pending().await` closure is the same idiom the
                // announcer arm uses for the "no-receiver-attached"
                // case.
                //
                // Cancel-safety: `oneshot::Receiver` IS cancel-safe by
                // construction (it owns a single slot, no mid-send
                // partial state to lose); when a sibling arm wins the
                // race the in-flight await is dropped and re-built
                // next iteration, against the same receiver.
                panik = async {
                    match panik_signal_rx.as_mut() {
                        Some(rx) => rx.await,
                        None => std::future::pending().await,
                    }
                } => {
                    arm_stats.record(ARM_PANIK);
                    // Drop the rx slot so a subsequent loop iteration
                    // (if any — the panik handler returns immediately
                    // below) finds it None and re-parks on
                    // `pending().await`.
                    panik_signal_rx = None;
                    if let Ok(signal) = panik {
                        let (matched_path, reason) = self
                            .handle_panik_signal(
                                signal.matched_path,
                                signal.sender_pid,
                                PANIK_KILL_GRACE,
                            )
                            .await;
                        // Record the per-secondary terminal on the lifecycle
                        // (single source of truth); the PyO3 boundary reads
                        // it back via `coordinator.terminal()` for exit(137).
                        self.enter_terminal_panik(matched_path, reason);
                        return Ok(RunOutcome::Terminal);
                    }
                    // Err(_) from the receiver — the watcher's sender
                    // dropped before firing. This is the normal
                    // shape on "watcher disabled" (empty paths
                    // config) and a benign one on "watcher task
                    // aborted by drop": no panik happened, the loop
                    // continues as if the arm hadn't fired. Without
                    // the take-to-None above this would resolve
                    // Err immediately on every subsequent poll.
                }
                // Externally-armed fatal-exit arm. A run-loop-external
                // policy (the observer's invalid_task monitor) sends a
                // reason string when its collection window elapses; we
                // latch it into `self.fatal_exit` and let the loop's own
                // exit check (below) propagate it as a non-zero `Err`
                // exit. Single-concern wiring identical to every other
                // fatal-exit setter: the arm only WRITES the flag, the
                // loop owns its exit. `None`/`Some` parking mirrors the
                // panik arm; `mpsc::Receiver::recv` is cancel-safe.
                signal = async {
                    match fatal_exit_signal_rx.as_mut() {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    arm_stats.record(ARM_FATAL_EXIT);
                    match signal {
                        Some(reason) => {
                            // First (and only consumed) signal: latch it.
                            // Drop the rx so a later iteration re-parks on
                            // `pending()` — the loop is about to exit on
                            // the `fatal_exit.take()` check anyway.
                            self.fatal_exit = Some(reason);
                            fatal_exit_signal_rx = None;
                        }
                        None => {
                            // All senders dropped (the policy / driver was
                            // torn down without firing). Benign — re-park
                            // by dropping the rx, exactly like the panik
                            // Err arm.
                            fatal_exit_signal_rx = None;
                        }
                    }
                }
                // Secondary control-plane ingress: externally-issued
                // commands against this node's own workers (today the
                // PyO3 `SecondaryHandle.send_to_worker` reply path of
                // the worker↔secondary custom-message channel). The
                // listener side never touches the pool — it queues
                // here and THIS loop (which owns the pool) acts.
                //
                // Cancel-safety: `mpsc::UnboundedReceiver::recv` is
                // documented cancel-safe; parking on `pending()` when
                // the receiver was never installed mirrors the
                // announcer / fatal-exit arms.
                control = async {
                    match secondary_control_rx.as_mut() {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    arm_stats.record(ARM_SECONDARY_CONTROL);
                    match control {
                        Some(cmd) => {
                            // Delegate to the single control-command
                            // handler the worker-event arm's causal
                            // pre-drain shares — one apply path, no
                            // shape divergence.
                            self.handle_secondary_control_command(cmd).await;
                        }
                        None => {
                            // All senders dropped (no SecondaryHandle
                            // alive). Benign — re-park on `pending()`.
                            secondary_control_rx = None;
                        }
                    }
                }
                // Off-loop SecondaryAffine-import completion arm (#497 P5).
                // A detached `spawn_local` import task (driven off a
                // `StartedRun` by `drive_affine_import`, so a multi-GB
                // `nix-store --import` never blocks this loop) posts its
                // classified completion here; the arm body runs the ON-loop
                // release: drain the dependents queued behind the import, mark
                // the hash locally-done on success, send one
                // `LocalDependencyReleased` per dependent, and DISPATCH each
                // released `B` onto its worker (the assignment the gate
                // withheld). This MIRRORS the worker-completion mechanism (the
                // pool arm receiving a `WorkerEvent` a worker monitor task
                // posted through `event_tx`).
                //
                // Cancel-safety: `mpsc::UnboundedReceiver::recv` is documented
                // cancel-safe; parking on `pending()` when the receiver was
                // already taken mirrors the announcer / fatal-exit / secondary-
                // control arms. The coordinator's own `affine_import_tx` clone
                // keeps the channel alive, so a `None` here never fires inside
                // `process_tasks`.
                completion = async {
                    match affine_import_rx.as_mut() {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    arm_stats.record(ARM_AFFINE_IMPORT);
                    if let Some(crate::secondary::affine_exec::AffineImportComplete {
                        affine_hash,
                        outcome,
                    }) = completion
                    {
                        // The on-loop release is owned by the executor seam —
                        // one body shared with the inline `run_affine_import_once`
                        // (the run-once latch / queue stays inside the executor;
                        // this arm only delivers the off-loop outcome + the
                        // factory the deferred dispatch needs).
                        if let Err(e) = self
                            .complete_affine_import(affine_hash, outcome, factory)
                            .await
                        {
                            tracing::warn!(
                                error = %e,
                                "affine-import completion release failed"
                            );
                        }
                    }
                }
                // Anti-entropy tick: broadcast this secondary's digest so
                // every peer can detect divergence and pull. Pure EMIT of
                // the role-agnostic frame built by `crate::anti_entropy`;
                // the receive-side compare+pull lives in the `StateDigest`
                // router arm. `interval.tick` is cancel-safe (tokio docs).
                _ = anti_entropy_interval.tick() => {
                    arm_stats.record(ARM_ANTI_ENTROPY);
                    let digest = self.cluster_state.digest();
                    let frame = crate::anti_entropy::digest_broadcast(
                        &self.config.secondary_id,
                        crate::secondary::wire::timestamp_now(),
                        digest,
                        // A compute SecondaryCoordinator is never an observer.
                        false,
                    );
                    let _ = self.send_to(Destination::All, frame).await;
                }
                // Disciplined-pull WAKE arm (#491 storm-killer): drives the
                // `pull_coordinator`'s probe/selection/rebalance timers. Parks
                // on the coordinator's PERSISTENT `wake_deadline` (an absolute
                // instant derived from STORED state — the window end, the
                // re-probe deadline, or the rebalance deadline), NOT a relative
                // sleep, so it fires under constant sibling-arm activity (the
                // watchdog-fires-under-load law). On fire, `tick` resolves the
                // due transition and returns the directives this node must send
                // (a re-probe, or the committed `RequestSnapshotStream`); each
                // is translated by the role-owned `drive_pull_directive` edge.
                // `None` deadline (Idle — no divergence noticed) parks the arm.
                // Cancel-safe: `sleep_until` consumes nothing and the deadline
                // is recomputed each iteration from the coordinator's stored
                // state, so a sibling arm winning merely re-creates the future
                // against the SAME instant.
                _ = async {
                    match self.pull_coordinator.wake_deadline() {
                        Some(due) => {
                            tokio::time::sleep_until(tokio::time::Instant::from_std(due)).await
                        }
                        None => std::future::pending().await,
                    }
                } => {
                    arm_stats.record(ARM_PULL);
                    for directive in self.pull_coordinator.tick(std::time::Instant::now()) {
                        self.drive_pull_directive(directive).await;
                    }
                }
                // Buffered-report-replay WAKE arm: re-deliver a retained
                // confirmable report (terminal / IMPORTANT custom) when
                // its per-entry backoff slot comes due — `sleep_until`
                // the buffer-wide minimum deadline computed above, parked
                // on `pending()` while nothing is retained. This replaces
                // the pre-backoff per-iteration drain call (which re-sent
                // a never-ackable report at loop speed — the ~61/s
                // production replay flood): the drain now runs only when
                // something is DUE, plus the `record_primary_message`
                // route-restored edge (`drain_report_replays_now`).
                //
                // Cancel-safety: `sleep_until` consumes nothing, and the
                // deadline is PERSISTENT per-entry state (`next_due`), so
                // a sibling arm winning the race merely re-creates the
                // future against the SAME instant next iteration — the
                // arm cannot be starved into never firing by a busy loop.
                _ = async {
                    match next_replay_due {
                        Some(due) => {
                            tokio::time::sleep_until(tokio::time::Instant::from_std(due)).await
                        }
                        None => std::future::pending().await,
                    }
                } => {
                    arm_stats.record(ARM_REPORT_REPLAY);
                    self.drain_report_replays().await;
                }
                // Worker-restart WAKE arm: rouse the loop when the
                // earliest scheduled slot restart comes due, so the
                // restart-execution tail below runs it. The body is
                // deliberately empty — execution lives in exactly one
                // place (the tail), this arm only defeats the park.
                // Cancel-safety: `sleep_until` consumes nothing and the
                // deadline is PERSISTENT per-slot state, so a sibling
                // arm winning merely re-creates the future against the
                // SAME stored instant next iteration.
                _ = async {
                    match next_restart_due {
                        Some(due) => {
                            tokio::time::sleep_until(tokio::time::Instant::from_std(due)).await
                        }
                        None => std::future::pending().await,
                    }
                } => {
                    arm_stats.record(ARM_WORKER_RESTART);
                }
            }

            // Flush any deferred peer messages — each names a concrete
            // peer secondary by id; the edge resolves `Secondary(id)` to
            // that host's by-id transport send. Drain into an owned Vec
            // (the `OperationalState` accessor `std::mem::take`s the
            // queue) so the operational borrow ends before the per-message
            // `send_to` (which re-borrows `self`).
            let pending = self.op_mut().drain_pending_peer_messages();
            for (peer_id, msg) in pending {
                let _ = self
                    .send_to(Destination::Secondary(PeerId::from(peer_id)), msg)
                    .await;
            }

            // NOTE: buffered-report-replay re-delivery is NOT a
            // per-iteration call here any more — it is the dedicated
            // wake arm in the `select!` above (parked on the per-entry
            // backoff deadlines via `next_report_replay_due`), plus the
            // `record_primary_message` route-restored edge. The old
            // per-iteration drain re-sent a never-ackable report at
            // loop speed (the production replay flood).

            // Hard-error exit path: a sub-handler (e.g. the peer-mesh
            // watchdog) detected an unrecoverable fault, queued the
            // notification to the primary, and asked us to exit. The
            // notification is already on the wire by this point (the
            // handler awaited the send before setting the flag); we
            // just need to break out of the loop with the reason as
            // the Err so `run()` propagates it and the process exits
            // non-zero. Loop owns its own exit; sub-handlers never
            // call `break` directly.
            if let Some(reason) = self.fatal_exit.take() {
                tracing::error!(reason = %reason, "secondary exiting with fatal error");
                // Record the per-secondary `Failed` terminal on the
                // lifecycle (single source of truth) before propagating the
                // reason as the run loop's `Err` — the Err is what the
                // boundary acts on; the terminal is the recorded outcome.
                self.enter_terminal_failed(reason.clone());
                return Err(reason);
            }

            // Run-aborted exit: the primary broadcast
            // `ClusterMutation::RunAborted { reason }` — the failure
            // twin of RunComplete. Checked BEFORE the `run_complete`
            // break because an abort is a HARD cluster shutdown: unlike
            // the clean-completion path, we do NOT wait for
            // `active_tasks` to drain (the run is being torn down, not
            // finished). Returns `RunOutcome::Terminal` projecting to
            // `SecondaryTerminal::Aborted` so the PyO3 secondary/observer
            // wrappers translate it to `std::process::exit(1)`. Originators
            // (#313): every deliberate fail-loud primary terminal — the
            // duplicate-task-id cases (#3a pre-phase, #3b run-wide
            // invalidation — latched BEFORE the wipe), the routing-collapse
            // strand, the wholesale spawn rejection, the worker-management
            // fail latch (`RunShouldFail` / `PolicyFatalExit`), and the
            // no-relocation-target topology. See the primary's
            // `broadcast_terminal_verdict` for the full classification.
            if let Some(reason) = self.cluster_state.run_aborted() {
                let reason = reason.to_string();
                tracing::error!(
                    reason = %reason,
                    "RunAborted signal received from primary; exiting non-zero"
                );
                // Record the per-secondary `Aborted` terminal on the
                // lifecycle (single source of truth); the PyO3 boundary
                // reads it back via `coordinator.terminal()` for exit(1).
                self.enter_terminal_aborted(reason);
                return Ok(RunOutcome::Terminal);
            }

            // Run-complete exit: the primary broadcast
            // `ClusterMutation::RunComplete` just before returning
            // from its `run()` (see primary/mod.rs). Once that flag
            // lands on this node and our local pool has no active
            // work, we can exit even if peers are still up. Without
            // this, non-promoted secondaries on a peer mesh sit
            // forever in failover-detection mode after a clean run
            // finishes — they have no way to distinguish "run is
            // genuinely over" from "primary just crashed", so they
            // hold their SLURM job slots indefinitely.
            // `active_tasks` now lives in `OperationalState`; the loop is
            // always `Operational` here, so `op_ref()` is `Some`. (Read
            // `run_complete()` first — it borrows `cluster_state`, a
            // disjoint field from the operational state.)
            let run_complete = self.cluster_state.run_complete();
            let no_active_tasks = self
                .op_ref()
                .map(|op| op.active_tasks.is_empty())
                .unwrap_or(true);
            if run_complete && no_active_tasks {
                tracing::info!("RunComplete signal received from primary; exiting");
                break;
            }

            // Graceful-abort drain exit: the replicated dispatch freeze is
            // latched and THIS secondary's last running task has completed,
            // so it tears down NOW — mid-run, before the primary's terminal
            // `RunComplete` — announcing a DELIBERATE self-departure first
            // (the existing graceful-leave path; see
            // `announce_graceful_drain_departure` for why this never trips
            // the failover/respawn machinery). EXCLUDED while this host is
            // the recognized PRIMARY's node: the co-resident secondary and
            // the primary share the node id, so a self-`PeerRemoved` here
            // would bury the live primary's membership entry. That node
            // parks instead until the primary either relocates away (the
            // `current_primary` re-point releases this gate) or finalizes
            // the drained run (`RunComplete` → the break above) — exactly
            // the graceful protocol's last-holder shape.
            if self.cluster_state.graceful_abort_requested()
                && no_active_tasks
                && self.cluster_state.current_primary() != Some(self.config.secondary_id.as_str())
            {
                tracing::info!(
                    secondary = %self.config.secondary_id,
                    "graceful abort: local work drained; announcing deliberate \
                     departure and exiting cleanly"
                );
                self.announce_graceful_drain_departure().await;
                break;
            }

            // Per-peer wind-down drain exit (#467): the primary marked THIS
            // exact secondary incarnation for graceful wind-down (its
            // re-admitted original re-seated while this respawn replacement
            // was already operational → double-occupancy of SLURM jobs), so
            // — once its last running task has drained — it tears down NOW,
            // mid-run, with the SAME deliberate self-departure path the
            // global graceful-abort drain uses (so it never trips the
            // failover/respawn machinery: a `SelfDeparture` is suppressed by
            // the respawn-admission gate, closing the wind-down→respawn
            // loop). The directive is read against this node's OWN id and
            // its CURRENT membership generation, so a stale directive minted
            // for a prior incarnation never matches. Same quiescence
            // predicate (`no_active_tasks`) and same recognized-primary-node
            // exclusion as the graceful-abort drain above (defensive: a
            // wound-down replacement is never the recognized primary's node,
            // but the guard keeps the two drain gates uniform — a self-
            // `PeerRemoved` must never bury a live primary's entry).
            if no_active_tasks
                && self.cluster_state.wind_down_requested(
                    &self.config.secondary_id,
                    self.cluster_state
                        .peer_member_gen(&self.config.secondary_id),
                )
                && self.cluster_state.current_primary() != Some(self.config.secondary_id.as_str())
            {
                tracing::info!(
                    secondary = %self.config.secondary_id,
                    "wind-down requested by primary (#467 double-occupancy \
                     heal); local work drained — announcing deliberate \
                     departure and exiting cleanly (releasing the SLURM job)"
                );
                self.announce_graceful_drain_departure().await;
                break;
            }

            // Execute the DUE scheduled worker restarts — the slots
            // whose stored deadline has arrived. Sources: the
            // pool-event arm's disconnect handling (typical when a
            // poll_loop observed pipe-EOF mid-task) and the
            // assignment-time paths that flagged a respawn (no
            // poll_loop was running, so no Disconnected event arrived;
            // the dispatch attempt itself saw the dead pipe) — both go
            // through `schedule_worker_restart`, so a healthy worker's
            // zero-delay entry executes on this very iteration
            // (historical semantics) while a startup-crashing one
            // waits out its backoff slot (the #370 brake; the wake arm
            // above rouses the loop when it comes due). Duplicates are
            // harmless: `restart_worker_async` first stops the slot,
            // which is a no-op on an already-stopped one.
            //
            // NON-BLOCKING respawn: `restart_worker_async` pushes the
            // wait-for-Ready into a background watcher and returns
            // immediately, leaving the slot `Transitioning`. The
            // operational loop's `WorkerEvent::Ready` arm reclaims the
            // slot and re-issues its `TaskRequest` once the new
            // subprocess reports `Response::Ready`. Using the inline-wait
            // `restart_worker` here held the whole `select!` open for the
            // entire slow-worker startup window — no keepalives fired —
            // so a busy-but-alive secondary was falsely declared dead by
            // the primary (the keepalive-starvation wedge). The repoll
            // therefore rides the Ready arm, NOT a post-restart call here
            // (the slot is not yet assignable when this returns).
            let now = std::time::Instant::now();
            let restart_set: HashSet<WorkerId> = self
                .op_mut()
                .pending_worker_restarts
                .iter()
                .filter(|(_, due)| **due <= now)
                .map(|(wid, _)| *wid)
                .collect();
            for wid in restart_set {
                self.op_mut().pending_worker_restarts.remove(&wid);
                // Active SIGKILL before restart so a stuck or
                // otherwise non-responsive worker is dead BEFORE
                // the replacement comes up. Prior behaviour relied
                // entirely on `pool.restart_worker` dropping the
                // old `WorkerHandle` (closing the transport from
                // our side) — but a worker that's wedged inside
                // `subprocess.run` (blocking on a non-cancellable
                // syscall) won't notice the EOF until its
                // subprocess returns. SIGKILL is the no-graceful-
                // shutdown lever; per CLAUDE.md the framework
                // already decided this slot is going to be
                // replaced, so the worker doesn't get a chance
                // to react.
                self.op_mut().pool.workers[wid as usize].kill_subprocess();
                // Sweep any task still bound to this slot in `active_tasks`
                // into the reinject path BEFORE the restart replaces the
                // subprocess (a new generation), so the replaced generation
                // cannot strand it. The Disconnected / OOM paths that fed
                // `restart_set` already swept with their specific failure
                // classification, so this is a no-op for them; the
                // `pending_worker_restarts`-sourced restarts (assignment-
                // failure respawns) are the ones a residual entry could
                // otherwise outlive. Belt-and-braces with the generation
                // gate, which prevents the drift in the first place.
                self.sweep_replaced_worker_task(wid).await?;
                if let Err(e) = self
                    .op_mut()
                    .pool
                    .restart_worker_async(wid, factory, false)
                    .await
                {
                    tracing::error!(worker_id = wid, error = %e, "secondary worker restart failed");
                    // RE-SCHEDULE the slot: a restart that failed BEFORE
                    // any subprocess existed (cgroup leaf prep, the spawn
                    // syscall itself — e.g. exec ENOENT into a gutted
                    // container rootfs, asm-dataset run_20260611_115429)
                    // must keep the slot under restart management, not
                    // silently drop it from the pool forever (the entry
                    // was removed above). The pool recorded the failure
                    // on the slot (`note_spawn_failure`), so the delay
                    // this schedule reads is the escalating startup-crash
                    // backoff — the retry runs on the calm #370 cadence
                    // and heals the slot when the exec context heals.
                    self.schedule_worker_restart(wid);
                    continue;
                }
            }
        }

        // All `break` statements above represent terminal exits
        // (RunComplete observed, drain-down complete after primary
        // disconnect, single-secondary clean shutdown), so reaching here
        // means we're done. Record the `Done` terminal on the lifecycle
        // (single source of truth) and report the `Terminal` control
        // signal.
        self.enter_terminal_done();
        Ok(RunOutcome::Terminal)
    }

    /// Process ONE worker pool event end to end — the single seam the
    /// operational loop's pool-event arm routes through (and the seam
    /// the causal-ordering tests drive), so an event is processed
    /// identically in production and under test.
    ///
    /// CAUSAL FENCE (the terminal-ordering gate's secondary half): a
    /// task terminal report must never be stamped/sent before the
    /// task's own prior sends, whichever leg of the relay they are on:
    ///
    /// 1. PIPELINE FLUSH (terminal-shaped events only — the asm-dataset
    ///    run_20260611_182745 tail race): the worker's last custom
    ///    messages may still be crossing the worker-message
    ///    dispatcher-task → `worker_message_listener` → control-queue
    ///    hop when its exit lands here. The pool channel orders the
    ///    customs BEFORE the terminal event, so a flush barrier sent
    ///    through the same FIFO dispatcher channel — acknowledged only
    ///    after every prior item's listeners returned — proves their
    ///    relay commands are in the control queue.
    /// 2. CONTROL PRE-DRAIN (every event — the asm-dataset
    ///    run_20260611_005220 race): apply every control-plane command
    ///    queued by the consumer's listener BEFORE the event is
    ///    processed, so the relayed customs are `msg_seq`-stamped and
    ///    sent (or retained) first, and the terminal's
    ///    `msgs_posted_through` watermark covers them.
    ///
    /// Step 1 keys on
    /// [`dynrunner_manager_local::worker::WorkerEvent::is_task_terminal`]
    /// — flushing on
    /// every event would put a dispatcher round-trip (listener
    /// execution included) on the loop's hot path for keepalives and
    /// the custom messages themselves, serialising exactly what the
    /// dispatcher task exists to decouple.
    pub(in crate::secondary) async fn process_worker_pool_event(
        &mut self,
        event: dynrunner_manager_local::worker::WorkerEvent<I>,
        oom_watcher: &OomWatcher,
        control_rx: &mut Option<
            tokio::sync::mpsc::UnboundedReceiver<super::super::control::SecondaryControlCommand>,
        >,
    ) -> Result<(), String> {
        if event.is_task_terminal() {
            self.flush_worker_message_pipeline().await;
        }
        self.drain_pending_control_commands(control_rx).await;
        let restart = self.handle_worker_event(event, oom_watcher).await?;
        if let Some(wid) = restart {
            // SCHEDULE, never execute inline: the due
            // instant comes from the pool's startup-crash
            // backoff (zero for a healthy worker that died
            // mid-task — those still restart at this very
            // iteration's tail). Executing here at event
            // speed was the #370 respawn-crash loop: an
            // instantly-dying subprocess re-entered
            // kill+spawn per loop iteration and starved
            // the whole single-threaded runtime.
            self.schedule_worker_restart(wid);
        }
        Ok(())
    }

    /// Apply ONE secondary control-plane ingress command — the single
    /// handler both the control `select!` arm and the worker-event
    /// arm's causal pre-drain route through, so a command queued by the
    /// PyO3 `SecondaryHandle` is applied identically whichever site
    /// drains it.
    pub(in crate::secondary) async fn handle_secondary_control_command(
        &mut self,
        cmd: super::super::control::SecondaryControlCommand,
    ) {
        match cmd {
            super::super::control::SecondaryControlCommand::SendToWorker {
                worker_id,
                topic,
                data,
            } => {
                let pool = &mut self.op_mut().pool;
                match pool.workers.get_mut(worker_id as usize) {
                    Some(worker) => {
                        if let Err(e) = worker.send_custom(topic, data).await {
                            tracing::warn!(
                                worker_id,
                                error = %e,
                                "send_to_worker custom message failed; \
                                 dropping (the worker slot's restart \
                                 machinery owns recovery)"
                            );
                        }
                    }
                    None => {
                        tracing::warn!(
                            worker_id,
                            "send_to_worker named a worker id with no \
                             pool slot; dropping custom message"
                        );
                    }
                }
            }
            super::super::control::SecondaryControlCommand::SendToPrimary {
                topic,
                data,
                important,
            } => {
                // The only Err class is the size gate, which the API
                // call site already rejected with a ValueError — this
                // is the defensive re-check.
                if let Err(e) = self.send_custom_to_primary(topic, data, important).await {
                    tracing::warn!(
                        error = %e,
                        "send_to_primary custom message rejected at \
                         the send seam; dropping"
                    );
                }
            }
        }
    }

    /// Synchronously drain every control-plane command CURRENTLY queued
    /// (non-blocking `try_recv` sweep) through the same
    /// [`Self::handle_secondary_control_command`] the parked arm uses.
    ///
    /// The one caller is the worker-event arm's CAUSAL PRE-DRAIN:
    /// commands enqueued before a worker event must be applied before
    /// the event's own reports go out, so a task's important custom
    /// messages are stamped-and-sent (or retained) BEFORE its terminal
    /// is stamped with the `msgs_posted_through` causal watermark.
    /// Cancel-safe by shape: each command is consumed and fully applied
    /// within this call — nothing is held across a `select!` poll.
    pub(in crate::secondary) async fn drain_pending_control_commands(
        &mut self,
        rx: &mut Option<
            tokio::sync::mpsc::UnboundedReceiver<super::super::control::SecondaryControlCommand>,
        >,
    ) {
        while let Some(cmd) = rx.as_mut().and_then(|rx| rx.try_recv().ok()) {
            self.handle_secondary_control_command(cmd).await;
        }
    }

    /// Causal-fence step 1: wait until the worker-message dispatcher
    /// has fully processed every item enqueued before NOW — i.e. every
    /// worker custom message that preceded the in-hand terminal event
    /// on the pool channel has been through the consumer's
    /// `worker_message_listener`, whose relay commands are therefore
    /// in the control queue for the pre-drain (step 2) to stamp+send
    /// before the terminal report.
    ///
    /// # Liveness
    ///
    /// * The await yields the loop, which is what lets the same-thread
    ///   (`spawn_local`) dispatcher task run the backlog at all. The
    ///   listeners' relay sends are non-blocking queue pushes
    ///   (`SecondaryHandle` → unbounded control channel), so the
    ///   round-trip is bounded by the in-flight backlog — the exact
    ///   messages the terminal must causally follow.
    /// * Dispatcher never spawned (`worker_message_rx` still unspent —
    ///   pre-run paths and unit harnesses): nothing can run a listener
    ///   before this terminal, so there is no pipeline to flush.
    /// * Dispatcher gone (channel closed / barrier dropped on abort):
    ///   no listener will ever relay again — proceed; the loss class
    ///   is the same as a consumer that never relayed.
    /// * A pathological listener that wedges the dispatcher is cut off
    ///   by [`WORKER_MESSAGE_FLUSH_TIMEOUT`]: the terminal proceeds
    ///   with the watermark it can see (the pre-fence behavior, under
    ///   a WARN) rather than wedging the operational loop.
    async fn flush_worker_message_pipeline(&mut self) {
        if self.worker_message_rx.is_some() {
            return;
        }
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        if self
            .worker_message_tx
            .send(crate::worker_messages::WorkerMessageItem::FlushBarrier(
                ack_tx,
            ))
            .is_err()
        {
            return;
        }
        match tokio::time::timeout(WORKER_MESSAGE_FLUSH_TIMEOUT, ack_rx).await {
            // Ok(Ok(())): barrier acknowledged. Ok(Err(_)): the
            // dispatcher dropped the barrier un-acked (task aborted
            // mid-drain) — nothing further will ever be relayed.
            Ok(_) => {}
            Err(_) => {
                tracing::warn!(
                    timeout_secs = WORKER_MESSAGE_FLUSH_TIMEOUT.as_secs(),
                    "worker-message pipeline flush timed out before a task \
                     terminal; a worker_message_listener is stalling the \
                     dispatcher — the terminal's msgs_posted_through stamp \
                     may miss the task's last sends"
                );
            }
        }
    }
}

/// Upper bound on the pre-terminal pipeline flush
/// ([`SecondaryCoordinator::flush_worker_message_pipeline`]): the
/// dispatcher backlog is normally a handful of relay-shaped listener
/// calls (milliseconds); a consumer listener that holds the dispatcher
/// longer than this is treated as wedged and the terminal proceeds
/// under-stamped (WARN) instead of parking the operational loop.
const WORKER_MESSAGE_FLUSH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
