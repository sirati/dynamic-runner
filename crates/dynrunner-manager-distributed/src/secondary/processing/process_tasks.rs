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
use std::time::{Duration, Instant};

use dynrunner_core::{Identifier, MessageReceiver, MessageSender, WorkerId};
use dynrunner_manager_local::oom::{
    OomWatcher, OomWatcherConfig, DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_SAMPLE_INTERVAL,
};
use dynrunner_manager_local::WorkerFactory;
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{ClusterMutation, DistributedMessage, PeerTransport};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use super::super::wire::timestamp_now;
use super::super::{RunOutcome, SecondaryCoordinator};

impl<PT, P, M, S, E, I> SecondaryCoordinator<PT, P, M, S, E, I>
where
    PT: MessageSender<DistributedMessage<I>> + MessageReceiver<DistributedMessage<I>>,
    P: PeerTransport<I>,
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

        let mut keepalive_interval = tokio::time::interval(self.config.keepalive_interval);
        // Decouple sample cadence (50ms, 20Hz) from decision cadence
        // (config-driven, default 100ms). Pre-extraction the decision
        // cadence was a hardcoded 100ms literal here; now it reads
        // from `SecondaryConfig::resource_check_interval` so the
        // secondary and LocalManager use the same operator knob.
        let mut oom_watcher = OomWatcher::new(OomWatcherConfig {
            sample_interval: DEFAULT_SAMPLE_INTERVAL,
            decision_interval: self.config.resource_check_interval,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            log_enabled: self.config.log_oom_watcher,
        });
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
        for i in 0..self.pool.workers.len() {
            if self.pool.workers[i].is_idle_state() {
                self.request_task_for_worker(i as WorkerId, factory).await?;
            }
        }

        // Take the announcer outbox receiver out of `self` for the
        // duration of the loop so the drain arm's `recv().await` can
        // borrow it without conflicting with the per-arm `&mut self`
        // borrows that the other arms (peer_transport, primary_transport)
        // require. `None` when no observer-mode caller has invoked
        // `attach_observer_announcer` on this coordinator — the arm
        // then parks on `pending().await` and is structurally a no-op,
        // matching the `command_rx` / `matcher_trigger_rx` shape on
        // the primary. Put back on `self` at loop exit so a future
        // re-entry (test-driven; the production single-shot path
        // exits via Done) re-attaches the same channel.
        let mut announcer_outbox_rx = self.announcer_outbox_rx.take();

        // Same shape for the panik-watcher signal: taken out of `self`
        // for the duration of the loop so the panik arm's `await` can
        // own the receiver. `None` when the PyO3 wrapper did not call
        // `register_panik_signal_rx` (operator passed no `--panik-file`
        // flags / Rust-only tests skip the watcher); the arm parks on
        // `pending().await` and never fires in that case. `Some` only
        // fires once (oneshot); after the signal arrives the receiver
        // is dropped — subsequent iterations of the loop find the local
        // `Option` empty and re-park, but the panik handler returns
        // `RunOutcome::PanikShutdown` immediately so the loop is about
        // to exit anyway.
        //
        // Grace for the SIGTERM → SIGKILL escalation on the worker
        // pool is 5s — same window the SubprocessWorkerFactory uses
        // for its own teardown ladder, so the framework's two
        // shutdown paths (clean exit and panik) give workers the
        // same amount of time to flush.
        let mut panik_signal_rx = self.panik_signal_rx.take();
        const PANIK_KILL_GRACE: std::time::Duration =
            std::time::Duration::from_secs(5);

        // Cross-thread command-channel receiver. Owned locally for the
        // duration of the loop so the `&mut self.command_rx` borrow
        // doesn't conflict with the per-arm handlers' `&mut self`. Put
        // back on `self` at loop exit so subsequent `process_tasks`
        // re-entries (`SetupPending` yield path) re-attach to the same
        // channel — the PyO3 `PrimaryHandle` only clones the sender
        // once before `run_until_setup_or_done()` starts and expects
        // its commands to keep being serviced across the
        // `SetupPending`-driven re-entry boundary. Mirrors the
        // `command_rx` discipline on `PrimaryCoordinator::operational_loop`.
        let mut command_rx = self.command_rx.take();

        loop {
            // Workers that need restart after disconnect
            let mut workers_to_restart: Vec<WorkerId> = Vec::new();

            // Cancellation safety note: every awaiting arm here must be
            // cancel-safe because the periodic ticks (keepalive, oom)
            // will cancel the in-flight recv/event futures whenever
            // they fire. `pool.recv_event` is `mpsc::Receiver::recv`
            // (documented cancel-safe). `primary_transport.recv` and
            // `peer_transport.recv_peer` go through the per-connection
            // bridge tasks (see `MessageReceiver` doc) which expose
            // mpsc receivers underneath. `interval.tick` is itself
            // cancel-safe per tokio docs.
            tokio::select! {
                event = self.pool.recv_event() => {
                    if let Some(event) = event {
                        let restart = self.handle_worker_event(event, &mut command_rx, factory).await?;
                        if let Some(wid) = restart {
                            workers_to_restart.push(wid);
                        }
                    }
                }
                msg = self.primary_transport.recv(), if !self.primary_disconnected => {
                    match msg {
                        Some(m) => {
                            self.dispatch_message(m, &mut command_rx, factory).await?;
                        }
                        None => {
                            // Primary's transport returned None — the
                            // bridge writer or reader task exited. Two
                            // structural cases live here:
                            //
                            // 1. Single-secondary / no-peer-mesh
                            //    runs (the test fixture drops primary
                            //    explicitly to signal shutdown;
                            //    production single-jobs runs do the
                            //    same when the local primary's run()
                            //    returns). There's no failover
                            //    candidate, so exit cleanly to
                            //    preserve the historical "primary
                            //    close = end of run" contract.
                            //
                            // 2. Multi-secondary failover. The peer
                            //    mesh is alive, an election can pick
                            //    a primary, and dispatch can keep
                            //    flowing.
                            //
                            // Pre-R1 case 2 fired the failover-arm
                            // logic IMMEDIATELY on the first recv-None
                            // event: a single dropped TCP packet that
                            // killed the WSS bridge writer would have
                            // armed an election even though the primary
                            // was perfectly alive (just briefly
                            // unreachable). R1 introduces a primary-link
                            // health sub-state in `primary_link.rs`
                            // that requires N=5 probes OR 30s of sustained
                            // failure before arming — so a one-off
                            // glitch is benign, but a sustained outage
                            // still arms within a bounded window. The
                            // architectural invariant "transport-level
                            // mesh state is independent of the
                            // primary-vs-secondary role" is preserved:
                            // recording a probe doesn't mutate the peer
                            // mesh in any way.
                            let peers = self.peer_transport.peer_count();

                            // primary path: once this secondary has
                            // been promoted, the local-machine primary's
                            // transport closing is BENIGN regardless of peer
                            // count. The promoted secondary owns the task
                            // pool authoritatively (per the post-demotion
                            // contract: `lifecycle.rs` demotes the local
                            // primary on `PromotePrimary`, after which the
                            // local side is purely advisory).
                            //
                            // Pull the `is_primary` check OUT of the
                            // threshold path so a promoted secondary
                            // skips it entirely — it has no use for
                            // failover and termination is owned by the
                            // pool-drained checks elsewhere in the
                            // loop.
                            if self.is_primary {
                                tracing::info!(
                                    connected_peers = peers,
                                    in_flight = self.primary_in_flight.len(),
                                    active = self.active_tasks.len(),
                                    pending = self.primary_pending_len(),
                                    "local primary disconnected; primary continues \
                                     independently — this node owns the pool, local \
                                     primary's exit is benign post-promotion (no failover \
                                     election needed; this node IS the promoted primary)"
                                );
                                self.primary_disconnected = true;
                                continue;
                            }

                            if peers == 0 {
                                tracing::info!(
                                    "primary disconnected and no peer mesh; exiting cleanly \
                                     (no failover candidate to take authority)"
                                );
                                break;
                            }

                            // Multi-secondary case: feed one probe
                            // into the primary-link health sub-state.
                            // It records the first-failure timestamp,
                            // bumps the counter, and tells us whether
                            // the threshold has breached. If yes, we
                            // arm failover here (existing path:
                            // backdate `primary_last_seen` so the
                            // next keepalive tick's election fires).
                            // If no, we just gate the recv arm so the
                            // persistently-None future doesn't
                            // hot-loop on the dead bridge — the
                            // keepalive-tick path below will check
                            // `should_arm_failover` again on the time
                            // axis once the window elapses.
                            let armed_now = self.primary_link.record_recv_failure();
                            self.primary_disconnected = true;
                            if armed_now {
                                tracing::warn!(
                                    connected_peers = peers,
                                    "primary transport closed (threshold breached); \
                                     switching to failover detection (election will run \
                                     via peer mesh; further dispatch routes through \
                                     primary_link.current_primary() once a peer is promoted)"
                                );
                                let backdate = self
                                    .config
                                    .keepalive_interval
                                    .saturating_mul(self.config.keepalive_miss_threshold + 1);
                                self.primary_last_seen = Some(
                                    Instant::now()
                                        .checked_sub(backdate)
                                        .unwrap_or_else(Instant::now),
                                );
                            } else {
                                tracing::info!(
                                    connected_peers = peers,
                                    "primary transport recv returned None; recording \
                                     probe (failover threshold not yet breached, holding \
                                     election)"
                                );
                                // Don't backdate yet — the keepalive
                                // tick will re-check `should_arm_failover`
                                // on each iteration and arm once the
                                // time window elapses.
                            }
                        }
                    }
                }
                peer_msg = self.peer_transport.recv_peer() => {
                    if let Some(m) = peer_msg {
                        self.handle_peer_message(m, &mut command_rx, factory).await;
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
                        let send_result = self
                            .peer_transport
                            .send(
                                dynrunner_protocol_primary_secondary::Address::Role(
                                    dynrunner_protocol_primary_secondary::Role::Primary,
                                ),
                                item.msg,
                            )
                            .await;
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
                    // primary retry pass. When this node is
                    // acting as primary and the main pass has
                    // drained with Recoverable failures still
                    // pending, re-inject them into `primary_pending`
                    // and bump the pass counter (no-op for
                    // non-promoted secondaries or when the budget
                    // is exhausted). Runs BEFORE `repoll_idle_workers`
                    // so re-injected items are seen by the same
                    // tick's re-poll without waiting for the next
                    // keepalive cycle. Mirrors the local primary's
                    // `run_retry_passes` — see primary.rs.
                    self.primary_drain_check_and_retry(factory).await;
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
                    self.repoll_idle_workers(factory).await;
                    let actions = self.run_election_tick();
                    for msg in actions.broadcast {
                        let _ = self.peer_transport.broadcast(msg).await;
                    }
                }
                _ = oom_sample_interval.tick() => {
                    // Fast sample tick: refresh per-worker RSS, read
                    // host + cgroup state, evaluate structured-log
                    // triggers. No scheduler call.
                    oom_watcher.on_sample(&mut self.pool);
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
                        return Ok(crate::secondary::RunOutcome::PanikShutdown {
                            matched_path,
                            reason,
                        });
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
                // Cross-thread command-channel arm. Mirrors the
                // `command_rx` arm on
                // `PrimaryCoordinator::operational_loop`. Drained
                // unconditionally regardless of `is_primary` — the
                // wire boundary cannot tell whether the recipient has
                // been promoted yet. Pre-promotion the per-variant
                // handlers either short-circuit (Err on unknown hash
                // / wrong state) or silently skip pool-side mirror
                // steps that require `primary_pending`; the
                // originator's CRDT broadcast still fires so the
                // ledger converges. Documented per-handler at
                // `secondary/primary/{fail_permanent,reinject_task,
                // update_preferred_secondaries}.rs`. The production
                // caller (`PySecondaryCoordinator`'s `on_run_start`
                // captured handle) only issues commands from
                // `on_phase_end`, which fires exclusively
                // post-promotion.
                cmd = async {
                    match command_rx.as_mut() {
                        Some(rx) => rx.recv().await,
                        // No command channel attached — park forever
                        // so this arm never fires. A `None` from the
                        // `recv()` future would otherwise hot-loop
                        // the select! the same way a closed mpsc
                        // would.
                        None => std::future::pending().await,
                    }
                } => {
                    match cmd {
                        Some(command) => {
                            // Delegate to the per-variant handler.
                            // Each handler owns its CRDT broadcast
                            // and its oneshot reply, so the call site
                            // stays a single line and the arm shape
                            // stays transport-shape-pure.
                            crate::secondary::command_channel::handle_secondary_command(
                                self,
                                command,
                                &mut command_rx,
                            )
                            .await;
                        }
                        None => {
                            // All senders dropped. Drop the receiver
                            // locally; the loop's other arms keep
                            // driving exit conditions. Same shape as
                            // the primary's same-case arm.
                            command_rx = None;
                            tracing::debug!(
                                "secondary command channel closed; disabling \
                                 PrimaryCommand arm for the remainder \
                                 of the loop"
                            );
                        }
                    }
                }
            }

            // Flush any deferred peer messages
            for (peer_id, msg) in std::mem::take(&mut self.pending_peer_messages) {
                let _ = self.peer_transport.send_to_peer(&peer_id, msg).await;
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
                return Err(reason);
            }

            // Setup-promote yield: the `PromotePrimary { required_setup:
            // true }` arm in `dispatch.rs` flipped `setup_pending` true
            // during the message dispatch this tick. The cluster ledger
            // is intentionally empty (the submitter deferred discovery
            // to us) and we cannot seed it from Rust because
            // `task.discover_items` lives on the Python side. Yield
            // back to the caller (the PyO3 secondary wrapper) so it
            // can re-acquire the GIL, run Python discovery against the
            // locally-mounted staged source, and feed the result back
            // via `ingest_setup_discovery` — which clears this flag
            // and hydrates the primary pool from the now-populated
            // ledger. On re-entry the next iteration of this same loop
            // sees `setup_pending == false` and proceeds normally.
            //
            // Placed AFTER `fatal_exit` (genuine errors take priority)
            // but BEFORE the drain-down / run-complete checks. Those
            // checks would all be no-ops here anyway — the ledger is
            // empty, the primary is still alive, there's nothing to
            // drain — but the explicit ordering makes "setup-pending
            // is the reason we're leaving" obvious from the source.
            //
            // Cancel-safety: every awaiting arm in the `select!` above
            // is cancel-safe (mpsc recv + tokio interval ticks), so
            // breaking out here just abandons whichever in-flight
            // future was being awaited. On re-entry the loop rebuilds
            // a fresh `select!` future from scratch; no state is lost.
            if self.setup_pending {
                tracing::info!(
                    "setup_pending observed; yielding from process_tasks \
                     so caller can run discovery and call \
                     ingest_setup_discovery"
                );
                // SetupPending is a re-entrant yield — the caller will
                // call `run_until_setup_or_done` again, which will
                // re-enter `process_tasks`. The announcer outbox
                // receiver must be put back on `self` so the next
                // iteration's `take()` finds it; otherwise the
                // announcer's outbox would fill up against a
                // perpetually-parked drain arm. Other exit paths (Err,
                // break + Ok(Done)) don't restore — the dispatcher
                // teardown in the outer wrapper aborts the announcer
                // task before its next outbox push.
                self.announcer_outbox_rx = announcer_outbox_rx;
                // Same shape for the panik-signal receiver: a
                // SetupPending yield can be followed by a re-entry
                // that needs the same watcher signal channel still
                // wired up. Put it back; the next `process_tasks`
                // re-entry takes it again. `None` here is the
                // common case (watcher disabled OR signal already
                // fired+consumed) and round-trips through the
                // field unchanged.
                self.panik_signal_rx = panik_signal_rx;
                // Same shape for the command-channel receiver: a
                // SetupPending yield can be followed by a re-entry
                // that needs the same channel still wired up. Put
                // it back; the next `process_tasks` re-entry takes
                // it again. Mirrors the primary's same discipline at
                // `operational_loop` exit.
                self.command_rx = command_rx;
                return Ok(RunOutcome::SetupPending);
            }

            // primary drain-down exit: when the live primary
            // disconnected (we suppressed the eager break above) and
            // every per-task ledger on this node has settled — pool
            // empty, in-flight empty, no active local tasks, retry
            // budget exhausted (`primary_failed` empty or
            // budget consumed) — the run is genuinely done. Break
            // here so `run()` returns and the process exits cleanly.
            // No-op while the live primary is alive (which is the
            // common case) since `primary_disconnected` is false.
            if self.primary_disconnected
                && self.is_primary
                && self.peer_transport.peer_count() == 0
                && self.primary_in_flight.is_empty()
                && self.active_tasks.is_empty()
                && self.primary_pending_is_empty()
                && (self.primary_failed.is_empty()
                    || !self.primary_retry_budget.should_retry())
            {
                // `primary_failed` at this point is either drained
                // (retry passes succeeded) or still populated but
                // the retry budget is exhausted (handled by the
                // surrounding condition). Either way, what remains
                // is terminal — surface as `fail_final` to match
                // the project-wide outcome-class log shape.
                tracing::info!(
                    fail_final = self.primary_failed.len(),
                    "primary drained after live-primary disconnect; exiting"
                );
                break;
            }

            // Promoted-secondary RunComplete broadcast: when this
            // secondary IS the promoted primary, its primary-pending +
            // in-flight + active are all empty (the work is done), and
            // the local-primary disconnected — broadcast `RunComplete`
            // so the OTHER secondaries on the peer mesh exit their
            // own loops. The peer_count==0 path above is the
            // single-secondary case; in the multi-secondary case the
            // promoted primary needs to actively signal teardown
            // because peers can't tell "run over" from "primary
            // crashed" otherwise.
            if self.primary_disconnected
                && self.is_primary
                && self.peer_transport.peer_count() > 0
                && self.primary_in_flight.is_empty()
                && self.active_tasks.is_empty()
                && self.primary_pending_is_empty()
                && (self.primary_failed.is_empty()
                    || !self.primary_retry_budget.should_retry())
                && !self.cluster_state.run_complete()
            {
                tracing::info!(
                    "promoted primary done; broadcasting RunComplete \
                     so peers exit"
                );
                self.cluster_state.apply(ClusterMutation::RunComplete);
                let msg = DistributedMessage::ClusterMutation {
                    sender_id: self.config.secondary_id.clone(),
                    timestamp: timestamp_now(),
                    mutations: vec![ClusterMutation::RunComplete],
                };
                // Fan out to BOTH the peer mesh and the demoted local
                // primary's transport. peer_transport.broadcast covers
                // surviving secondaries; primary_transport.send is the
                // only path that reaches the demoted primary (it's not
                // a peer, it sits on the other end of the secondary→
                // primary channel, mute-routing is asymmetric). Without
                // the loopback the demoted primary stays in its
                // operational loop indefinitely waiting for a counter
                // tick that will never come — asm-dataset-nix R2 / T3
                // 1200s hang. Errors are swallowed: if the loopback
                // tx is closed the demoted primary already exited
                // some other way, which is the desired terminal state.
                let _ = self.primary_transport.send(msg.clone()).await;
                let _ = self.peer_transport.broadcast(msg).await;
                // Keep iterating; the next loop tick's run-complete
                // exit (below) will fire on this node now that the
                // flag is set locally.
            }

            // Promoted-secondary RunComplete broadcast on natural
            // quiesce — alive-demoted-primary path (asm-tokenizer LMU
            // 2-of-235 retry hang). Distinct from the
            // `primary_disconnected`-gated branch above: that branch is
            // the dead-demoted backup (the demoted exited before all
            // outcomes were accounted, so the local cluster_state
            // mirror can never converge via loopback). This branch is
            // the alive-demoted path: the loopback round-trip is
            // expected to complete, so we WAIT for the local
            // `cluster_state` mirror to converge (every task terminal
            // in the ledger) before declaring run-done.
            //
            // Why this branch is needed at all: with
            // `required_setup_on_promote = true` the demoted local
            // primary's operational loop is gated off the counter exit
            // (`partial_view` guard in `lifecycle.rs`) and can only
            // exit via the authoritative `cluster_state.run_complete()`
            // signal. The pre-fix code only originated `RunComplete`
            // when the demoted disconnected first; the demoted, in
            // turn, only disconnected after its operational loop
            // exited — circular wait, deadlock. Originating
            // `RunComplete` on natural quiesce breaks the cycle
            // without depending on the demoted exiting first.
            //
            // Three-level gate to suppress spurious fires:
            //
            //   (a) Local-pool drained — primary_pending empty,
            //       primary_in_flight empty, own active_tasks empty,
            //       and either no Recoverable failures pending OR
            //       retry budget exhausted. Necessary because a
            //       retry-in-progress task transiently appears in
            //       cluster_state as `Failed { Recoverable }`
            //       (terminal-looking) while a worker is actively
            //       running it; without the local-pool guard, the
            //       cluster_state's terminal partition would fire the
            //       branch mid-retry.
            //
            //   (b) cluster_state converged — every ledger entry is in
            //       a terminal state (Completed or Failed). Necessary
            //       because the promoted secondary itself doesn't
            //       originate `ClusterMutation::TaskCompleted` —
            //       it forwards `DistributedMessage::TaskComplete` to
            //       the demoted-local primary, which applies +
            //       broadcasts, and the loopback round-trip updates
            //       this node's mirror. The local-pool empties one
            //       round-trip BEFORE the broadcast arrives back, so
            //       without (b) the branch fires before the
            //       cross-node convergence is observable and the
            //       cluster_state mirrors on OTHER nodes (which
            //       observe RunComplete strictly after the inbound
            //       roundtrip on this node) skip the last
            //       TaskCompleted, leaving them with inflated
            //       Pending / InFlight counts. (Concrete regression:
            //       `cluster_state_converges_on_primary_and_secondary`
            //       failed without this gate at sec_counts.completed
            //       = 2/5 because the natural-quiesce branch was
            //       firing on the last worker's local completion
            //       BEFORE the inbound loopback for the previous 3
            //       tasks had arrived.)
            //
            // Both gates use the cluster_state's own counts; reading
            // through that single source keeps the predicate stable
            // under arbitrary task-graph shapes (no hard-coded total
            // — `task_count` is the CRDT-authoritative size of the
            // ledger).
            // The (a) + (b) + (c) gate set described above is owned
            // by `promoted_primary_natural_quiesce_eligible` (in
            // `primary/ledger_ops.rs`). Lifted out of the call site
            // so the test suite can pin gate semantics without
            // driving the full operational loop; the branch keeps
            // its side effects (cluster_state.apply, fan-out, flush)
            // local because they sequence with the loop's exit
            // condition immediately below.
            if self.promoted_primary_natural_quiesce_eligible() {
                let cluster_counts = self.cluster_state.counts();
                tracing::info!(
                    completed = cluster_counts.completed,
                    failed = cluster_counts.failed,
                    "promoted primary observed cluster quiesce with alive \
                     demoted primary; broadcasting RunComplete so the \
                     demoted operational loop can exit"
                );
                self.cluster_state.apply(ClusterMutation::RunComplete);
                let msg = DistributedMessage::ClusterMutation {
                    sender_id: self.config.secondary_id.clone(),
                    timestamp: timestamp_now(),
                    mutations: vec![ClusterMutation::RunComplete],
                };
                // Fan out symmetric to the dead-demoted branch above:
                // the loopback delivers to the alive demoted submitter
                // (which is gated off its counter exit by the
                // `partial_view` guard and waiting on this very
                // signal); the peer broadcast covers any observers /
                // other secondaries on the mesh. Errors swallowed for
                // the same idempotency rationale.
                let _ = self.primary_transport.send(msg.clone()).await;
                let _ = self.peer_transport.broadcast(msg).await;
                // Write-task termination race (asm-tokenizer Tier-2):
                // for bridged transports (production `NetworkClient`),
                // `send.await` only confirms the mpsc enqueue; the
                // wire write happens asynchronously in the writer task
                // and the `BridgedConnection::Drop` (triggered when
                // `process_tasks` returns and the runtime tears down
                // `SecondaryCoordinator`) aborts that writer
                // mid-flush. The 20 prior TaskCompletes survived
                // because subsequent loop iterations kept the runtime
                // alive long enough for the writer to drain; the
                // RunComplete is uniquely the FINAL message before
                // exit, so the race always fires on it. The
                // workstation primary then never sees `run_complete`
                // and the submitter's operational loop hangs past
                // its bounded timeout.
                //
                // `flush()` is the rendezvous primitive on the
                // transport — for `NetworkClient` it round-trips a
                // FIFO marker through the writer task and only
                // returns after every previously-enqueued message
                // has been pushed to the underlying socket. For
                // direct-wire transports (channel transports, in-
                // process WSS) the default no-op is correct.
                //
                // Bounded so a wedged writer task can't deadlock
                // exit: indefinite/idle wait is the load-bearing
                // bug class to eliminate, so any finite ceiling
                // beats none. 10 s is comfortable headroom over
                // worst-case TCP-over-SSH-tunnel flush latency
                // (single-digit ms typical, hundreds of ms under
                // network stress) while still bounded enough that
                // a wedged writer task surfaces operationally
                // rather than hangs the dispatch.
                const FLUSH_DEADLINE: Duration = Duration::from_secs(10);
                match tokio::time::timeout(
                    FLUSH_DEADLINE,
                    self.primary_transport.flush(),
                )
                .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => tracing::warn!(
                        error = %e,
                        "primary_transport.flush after RunComplete \
                         broadcast returned an error; writer task may \
                         have already torn down — proceeding to exit"
                    ),
                    Err(_) => tracing::warn!(
                        deadline_s = FLUSH_DEADLINE.as_secs_f64(),
                        "primary_transport.flush after RunComplete \
                         broadcast did not return within deadline; \
                         proceeding to exit anyway to avoid blocking \
                         on a wedged writer"
                    ),
                }
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
            if self.cluster_state.run_complete()
                && self.active_tasks.is_empty()
            {
                tracing::info!(
                    "RunComplete signal received from primary; exiting"
                );
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
            let mut restart_set: HashSet<WorkerId> =
                workers_to_restart.into_iter().collect();
            restart_set.extend(self.pending_worker_restarts.drain());
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
                self.pool.workers[wid as usize].kill_subprocess();
                if let Err(e) = self.pool.restart_worker(wid, factory, false).await {
                    tracing::error!(worker_id = wid, error = %e, "secondary worker restart failed");
                    continue;
                }
                let _ = self.request_task_for_worker(wid, factory).await;
            }
        }

        // Put the command-channel receiver back on `self` so a
        // hypothetical post-`Done` inspection (test fixtures, future
        // re-entry paths) finds it intact. The wrapper's terminal
        // cleanup (`stop_all_workers` + dispatcher abort) does not
        // depend on the field's state, and the natural Done exit
        // here cannot be followed by another `process_tasks` invocation
        // on the same coordinator instance in production — but the
        // round-trip discipline keeps the field's lifecycle
        // structurally identical to the SetupPending yield path
        // (single source of truth: the take-at-entry happens once
        // per loop entry, the put-back happens at every loop exit).
        self.command_rx = command_rx;

        // All `break` statements above represent terminal exits
        // (RunComplete observed, drain-down complete after primary
        // disconnect, single-secondary clean shutdown). The
        // SetupPending yield uses `return Ok(RunOutcome::SetupPending)`
        // directly, so reaching here means we're done.
        Ok(RunOutcome::Done)
    }
}
