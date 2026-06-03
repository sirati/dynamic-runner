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
    DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_SAMPLE_INTERVAL, OomWatcher, OomWatcherConfig,
};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{Destination, PeerId, PeerTransport};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use super::super::{RunOutcome, SecondaryCoordinator};

/// Cadence for the externally-registered cluster-state refresh callback
/// (see [`SecondaryCoordinator::register_cluster_state_refresh`]). A
/// modest 30s tick: the only consumer is the observer's live-snapshot
/// feed, whose downstream reporter runs on a 10-minute / 1-minute
/// cadence, so 30s keeps the published projection comfortably fresh
/// between reporter reads while the per-tick `O(ledger)` projection cost
/// stays negligible. Deliberately NOT per-`ClusterMutation` (which would
/// make the projection `O(ledger × mutations)`).
const CLUSTER_STATE_REFRESH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

impl<Tr, M, S, E, I> SecondaryCoordinator<Tr, M, S, E, I>
where
    Tr: PeerTransport<I>,
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
        // A `SetupPending` re-entry (and the late-joiner observer, which
        // `restore_from_snapshot_and_skip_setup` landed directly in
        // `Operational`) finds the lifecycle already `Operational`, so the
        // transition is a no-op on the state — and the `Option::take()`s
        // yield `None` the second time, so the loop's latch locals re-park
        // exactly as the pre-typed flat-field flow did across the yield.
        // The carrier ferries only the receivers the operational `select!`
        // actually polls. `lifecycle_rx` / `task_completed_rx` were already
        // taken in `run_until_setup_or_done_inner` to spawn their
        // dispatchers, and `command_rx` is transferred to the co-located
        // primary — none is drained here, so none rides this carrier.
        let latches = super::super::lifecycle::OperationalLatches {
            announcer_outbox_rx: self.announcer_outbox_rx.take(),
            panik_signal_rx: self.panik_signal_rx.take(),
            fatal_exit_signal_rx: self.fatal_exit_signal_rx.take(),
            on_cluster_state_refresh: self.on_cluster_state_refresh.take(),
            colocated_loopback_inbound_rx: self.colocated_loopback_inbound_rx.take(),
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
            std::collections::HashSet::new(),
            std::collections::HashMap::new(),
        );
        self.lifecycle = lifecycle;
        let super::super::lifecycle::OperationalLatches {
            mut announcer_outbox_rx,
            mut panik_signal_rx,
            mut fatal_exit_signal_rx,
            on_cluster_state_refresh,
            mut colocated_loopback_inbound_rx,
        } = latches;

        let mut keepalive_interval = tokio::time::interval(self.config.keepalive_interval);
        // Decouple sample cadence (50ms, 20Hz) from decision cadence
        // (config-driven, default 100ms). Pre-extraction the decision
        // cadence was a hardcoded 100ms literal here; now it reads
        // from `SecondaryConfig::resource_check_interval` so the
        // secondary and LocalManager use the same operator knob.
        // Activate kernel-OOM detection by passing the workers cgroup
        // `memory.events` path (when the nested workers subgroup was
        // materialised; flat-layout fallback leaves it `None`).
        let workers_memory_events_path = self
            .op_mut()
            .pool
            .workers_cgroup()
            .map(|h| h.workers_path().join("memory.events"));
        let mut oom_watcher = OomWatcher::new_with_workers_cgroup(
            OomWatcherConfig {
                sample_interval: DEFAULT_SAMPLE_INTERVAL,
                decision_interval: self.config.resource_check_interval,
                heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
                log_enabled: self.config.log_oom_watcher,
            },
            workers_memory_events_path,
        );
        let mut oom_sample_interval = oom_watcher.sample_interval_ticker();
        let mut oom_decision_interval = oom_watcher.decision_interval_ticker();

        // Tell the primary the peer-mesh has settled so it can release
        // `PromotePrimary`. For the single-secondary / no-peers case
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

        // `announcer_outbox_rx`, `panik_signal_rx`, `fatal_exit_signal_rx`,
        // and `on_cluster_state_refresh` were surrendered into the loop's
        // locals at the single `enter_operational` latch boundary above
        // (the take-once carrier), so they own their receivers across the
        // `select!` iterations exactly as the pre-typed flat-field flow
        // did with per-field `Option::take()`. Each is `None` when its
        // registration never happened (no observer announcer / no
        // `--panik-file` / no fatal-exit policy / no refresh consumer) and
        // the matching arm parks on `pending()`.
        //
        // Grace for the SIGTERM → SIGKILL escalation on the worker pool is
        // 5s — same window the SubprocessWorkerFactory uses for its own
        // teardown ladder.
        const PANIK_KILL_GRACE: std::time::Duration = std::time::Duration::from_secs(5);

        // The cluster-state refresh interval is built here so the first
        // tick lands one full period in (the just-restored snapshot is
        // already published by the integration site before the loop
        // starts, so re-publishing it at t=0 would be redundant);
        // `MissedTickBehavior::Skip` keeps a stalled loop from bursting
        // catch-up ticks.
        let mut cluster_state_refresh_interval =
            tokio::time::interval(CLUSTER_STATE_REFRESH_INTERVAL);
        cluster_state_refresh_interval
            .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // Consume the immediate first tick so the refresh lands one full
        // period in rather than firing on entry.
        cluster_state_refresh_interval.reset();

        // The secondary holds NO authority and processes NO command
        // channel: the externally-issued `PrimaryCommand`s
        // (FailPermanent / ReinjectTask / UpdatePreferredSecondaries /
        // SpawnTasks) are authority mutations whose only correct owner
        // is the co-located `PrimaryCoordinator` (which runs the command
        // arm in its own operational loop). The `command_tx` /
        // `command_rx` fields stay on the struct as the registration
        // anchor keeping the PyO3 `PrimaryHandle` clone a stable type;
        // the composed-primary runtime hands the receiver to the
        // primary's command loop. This secondary loop never drains it.

        loop {
            // Workers that need restart after disconnect
            let mut workers_to_restart: Vec<WorkerId> = Vec::new();

            // Cancellation safety note: every awaiting arm here must be
            // cancel-safe because the periodic ticks (keepalive, oom)
            // will cancel the in-flight recv/event futures whenever
            // they fire. `pool.recv_event` is `mpsc::Receiver::recv`
            // (documented cancel-safe). `transport.recv_peer` is the one
            // mesh inbound stream, backed by a per-connection bridge-task
            // mpsc (cancel-safe; see `MessageReceiver` doc).
            // `interval.tick` is itself cancel-safe per tokio docs.
            tokio::select! {
                // The pool lives inside `OperationalState`. The recv
                // future is built via the `self.lifecycle.operational_mut()`
                // FIELD path (a partial borrow of `self.lifecycle`), NOT
                // the `op_mut()` coordinator method (which borrows ALL of
                // `self` and would conflict with the sibling
                // `self.transport.recv_peer()` arm). The two recv futures
                // then borrow disjoint fields (`self.lifecycle` vs
                // `self.transport`), and both borrows release the moment
                // `select!` picks a winner, so the arm BODIES are free to
                // use `&mut self` methods. The `expect` is sound: this loop
                // runs only after the `enter_operational` transition above.
                event = self
                    .lifecycle
                    .operational_mut()
                    .expect("process_tasks loop runs only when Operational")
                    .pool
                    .recv_event() =>
                {
                    if let Some(event) = event {
                        let restart = self.handle_worker_event(event, &oom_watcher).await?;
                        if let Some(wid) = restart {
                            workers_to_restart.push(wid);
                        }
                    }
                }
                // Single mesh inbound arm. There is one mesh inbound
                // stream — the manager addresses peers by id and the mesh
                // relays. Failover-health is driven by the send-side
                // no-route probe plus the keepalive time-axis in
                // `check_primary_link_threshold`. A `None` here means the
                // mesh inbound stream closed: no more frames can ever
                // arrive, so exit cleanly — the historical "inbound
                // closed = end of run" contract.
                msg = self.transport.recv_peer() => {
                    match msg {
                        Some(m) => {
                            self.handle_inbound(m, factory).await;
                        }
                        None => {
                            tracing::info!(
                                "mesh inbound stream closed; exiting cleanly"
                            );
                            break;
                        }
                    }
                }
                // Co-located primary LOOPBACK inbound (channel CH1). On a
                // multi-role host the co-located `PrimaryCoordinator`'s
                // egress (own-host `TaskAssignment` loopback + its
                // `Destination::All` broadcast leg) sends into the matching
                // sender; this arm drains the receiver and feeds each frame
                // through `handle_inbound` EXACTLY as a wire frame, so a
                // loopback `TaskAssignment` / `ClusterMutation` /
                // `RunComplete` is processed identically to a
                // mesh-delivered one. `None` (no co-located primary
                // composed — every non-pyo3 path) ⇒ the arm parks on
                // `pending()` and never fires, same idiom as the announcer
                // / panik arms. A `Some(None)` (channel closed: the
                // co-located primary dropped its sender) drops the rx so a
                // later iteration re-parks. `mpsc::Receiver::recv` is
                // cancel-safe.
                loopback = async {
                    match colocated_loopback_inbound_rx.as_mut() {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    match loopback {
                        Some(frame) => {
                            self.handle_inbound(frame, factory).await;
                        }
                        None => {
                            colocated_loopback_inbound_rx = None;
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
                    // node's co-located primary once promoted), driven by
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
                    let actions = self.run_election_tick();
                    for msg in actions.broadcast {
                        let _ = self.send_to(Destination::All, msg).await;
                    }
                }
                _ = oom_sample_interval.tick() => {
                    // Fast sample tick: refresh per-worker RSS, read
                    // host + cgroup state, evaluate structured-log
                    // triggers. No scheduler call.
                    oom_watcher.on_sample(&mut self.op_mut().pool);
                }
                _ = oom_decision_interval.tick() => {
                    self.check_resource_pressure_via_watcher(&mut oom_watcher, factory).await;
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
                    // Drop the rx slot so a subsequent loop iteration
                    // (if any — the panik handler returns immediately
                    // below) finds it None and re-parks on
                    // `pending().await`.
                    panik_signal_rx = None;
                    if let Ok(signal) = panik {
                        let (matched_path, reason) = self
                            .handle_panik_signal(
                                signal.matched_path,
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
                // Cluster-state refresh tick. The one in-loop moment a
                // concurrently-running consumer (the observer's
                // live-snapshot feed) can observe the freshening,
                // post-apply CRDT: every other arm has just finished its
                // apply (an inbound `ClusterMutation` routes through
                // `handle_inbound` → the apply path), so `self.cluster_state`
                // here reflects the latest converged ledger. Invoke the
                // registered callback with a read-only borrow; the consumer
                // PROJECTS it and publishes to its own cell. Periodic (not
                // per-mutation) so the projection cost is O(ledger) per
                // tick. When no consumer registered the closure is `None`
                // and the tick is a no-op.
                //
                // Cancel-safety: `interval.tick` is cancel-safe (tokio
                // docs); a sibling arm winning the race abandons the
                // in-flight tick future, rebuilt next iteration against
                // the same interval.
                _ = cluster_state_refresh_interval.tick() => {
                    if let Some(callback) = on_cluster_state_refresh.as_ref() {
                        callback(&self.cluster_state);
                    }
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

            // Setup-discovery yield: in pre-staged mode the authority
            // deferred task discovery to the corpus-mounting secondaries
            // (it sent an empty `InitialAssignment { pre_staged_mode: true }`
            // rather than seeding the ledger). With the ledger still
            // empty and no discovery run on this node yet, yield back to
            // the caller (the PyO3 secondary wrapper) so it can
            // re-acquire the GIL, run Python's `task.discover_items`
            // against the locally bind-mounted corpus, and feed the
            // result back via `ingest_setup_discovery` — which broadcasts
            // `PhaseDepsSet + TaskAdded` onto the mesh (the co-located
            // primary picks them up as a mesh member, refreshes
            // `total_tasks`, and its CRDT-derived `setup_pending()` gate
            // flips false) and latches the fire-once guard. On re-entry
            // `setup_discovery_pending()` is false (ledger seeded OR the
            // latch is set), so the loop proceeds normally.
            //
            // The discriminator lives entirely in
            // `setup_discovery_pending()` (`coordinator.rs`) — this site
            // only reads the predicate. Placed AFTER `fatal_exit`
            // (genuine errors take priority) but BEFORE the run-complete
            // / drain checks, which would all be no-ops here anyway (the
            // ledger is empty, the mesh is alive, nothing to drain).
            //
            // Cancel-safety: every awaiting arm in the `select!` above is
            // cancel-safe (mpsc recv + tokio interval ticks), so breaking
            // out here just abandons whichever in-flight future was being
            // awaited; re-entry rebuilds a fresh `select!`.
            if self.setup_discovery_pending() {
                tracing::info!(
                    "setup-discovery pending (pre-staged mode, empty ledger); \
                     yielding from process_tasks so caller can run \
                     task.discover_items and call ingest_setup_discovery"
                );
                return Ok(RunOutcome::SetupPending);
            }

            // Run-aborted exit: the primary broadcast
            // `ClusterMutation::RunAborted { reason }` — the failure
            // twin of RunComplete. Checked BEFORE the `run_complete`
            // break because an abort is a HARD cluster shutdown: unlike
            // the clean-completion path, we do NOT wait for
            // `active_tasks` to drain (the run is being torn down, not
            // finished). Returns `RunOutcome::Aborted` so the PyO3
            // secondary/observer wrappers translate it to
            // `std::process::exit(1)`. Single originator today: the
            // pre-phase duplicate-task-id case (#3a).
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
            let no_active_tasks = self.op_ref().map(|op| op.active_tasks.is_empty()).unwrap_or(true);
            if run_complete && no_active_tasks {
                tracing::info!("RunComplete signal received from primary; exiting");
                break;
            }

            // Restart workers that disconnected (via the
            // pool-event channel — typical when a poll_loop
            // observed pipe-EOF mid-task) OR workers that the
            // assignment-time paths flagged for respawn (no
            // poll_loop was running, so no Disconnected event
            // arrived; the dispatch attempt itself saw the dead
            // pipe). The two sources are unioned so duplicates are
            // harmless: `restart_worker` first stops the slot,
            // which is a no-op on an already-stopped one.
            //
            // Per Bug B's contract (worker process is restarted on
            // the next assignment after NonRecoverableError), the
            // respawn happens here, BEFORE `request_task_for_worker`
            // re-engages the fresh subprocess with the primary's
            // pool.
            let mut restart_set: HashSet<WorkerId> = workers_to_restart.into_iter().collect();
            restart_set.extend(self.op_mut().pending_worker_restarts.drain());
            for wid in restart_set {
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
                if let Err(e) = self.op_mut().pool.restart_worker(wid, factory, false).await {
                    tracing::error!(worker_id = wid, error = %e, "secondary worker restart failed");
                    continue;
                }
                let _ = self.request_task_for_worker(wid).await;
            }
        }

        // All `break` statements above represent terminal exits
        // (RunComplete observed, drain-down complete after primary
        // disconnect, single-secondary clean shutdown). The
        // SetupPending yield uses `return Ok(RunOutcome::SetupPending)`
        // directly, so reaching here means we're done. Record the `Done`
        // terminal on the lifecycle (single source of truth) and report
        // the `Terminal` control signal.
        self.enter_terminal_done();
        Ok(RunOutcome::Terminal)
    }
}
