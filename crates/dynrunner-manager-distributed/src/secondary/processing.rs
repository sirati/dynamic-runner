use std::collections::HashSet;
use std::time::{Duration, Instant};

use dynrunner_core::{ErrorType, Identifier, MessageReceiver, MessageSender, WorkerId};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_manager_local::worker::WorkerEvent;
use dynrunner_manager_local::WorkerFactory;
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, DistributedMessage, PeerTransport,
};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};


use super::{RunOutcome, SecondaryCoordinator};
use super::wire::timestamp_now;

impl<PT, P, M, S, E, I> SecondaryCoordinator<PT, P, M, S, E, I>
where
    PT: MessageSender<DistributedMessage<I>> + MessageReceiver<DistributedMessage<I>>,
    P: PeerTransport<I>,
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    pub(super) async fn process_tasks(
        &mut self,
        factory: &mut impl WorkerFactory<M>,
    ) -> Result<RunOutcome, String> {
        tracing::info!("entering task processing loop");

        let mut keepalive_interval = tokio::time::interval(self.config.keepalive_interval);
        let mut oom_interval = tokio::time::interval(Duration::from_millis(100));

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
                self.request_task_for_worker(i as WorkerId).await?;
            }
        }

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
                        let restart = self.handle_worker_event(event).await?;
                        if let Some(wid) = restart {
                            workers_to_restart.push(wid);
                        }
                    }
                }
                msg = self.primary_transport.recv(), if !self.primary_disconnected => {
                    match msg {
                        Some(m) => {
                            self.dispatch_message(m).await?;
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
                        self.handle_peer_message(m).await;
                    }
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
                    self.primary_drain_check_and_retry().await;
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
                        let _ = self.peer_transport.broadcast(msg).await;
                    }
                }
                _ = oom_interval.tick() => {
                    self.check_resource_pressure(factory).await;
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
            // Two-level gate to suppress spurious fires:
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
            let cluster_counts = self.cluster_state.counts();
            let cluster_quiesced = self.cluster_state.task_count() > 0
                && cluster_counts.pending == 0
                && cluster_counts.in_flight == 0;
            if self.is_primary
                && !self.primary_disconnected
                && self.primary_in_flight.is_empty()
                && self.active_tasks.is_empty()
                && self.primary_pending_is_empty()
                && (self.primary_failed.is_empty()
                    || !self.primary_retry_budget.should_retry())
                && cluster_quiesced
                && !self.cluster_state.run_complete()
            {
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
                // Latch the originator flag so the run-complete exit
                // below waits for the submitter's ack (its WSS close
                // after applying RunComplete) instead of breaking
                // immediately and racing the writer-task flush. See
                // `run_complete_originated` doc on `SecondaryCoordinator`
                // for the full rationale.
                self.run_complete_originated = true;
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
            //
            // `run_complete_originated` gate: when THIS node fired
            // the natural-quiesce branch above, the broadcast is
            // enqueued in `primary_transport`'s writer-task mpsc but
            // hasn't necessarily hit the wire yet. Exiting here
            // immediately would drop the SecondaryCoordinator,
            // triggering `BridgedConnection::drop` which `abort`s
            // the writer task and loses the in-flight RunComplete —
            // the demoted submitter then sits forever in its
            // operational_loop waiting for an exit cue that never
            // arrives. Wait for `primary_disconnected` (set by the
            // recv-None branch when the submitter exits and closes
            // its WSS) before exiting; that confirms the submitter
            // observed and processed the broadcast. The
            // `select!.await` during the wait gives the writer task
            // its needed CPU slice to drain its mpsc and flush.
            //
            // Followers (peer secondaries that merely OBSERVED the
            // broadcast on their inbound mesh arm) take the fast
            // path — `run_complete_originated` stays false on them
            // and the gate collapses to the original check. They
            // were never the originator of the broadcast and have
            // nothing in flight on their side to drain.
            if self.cluster_state.run_complete()
                && self.active_tasks.is_empty()
                && (!self.run_complete_originated || self.primary_disconnected)
            {
                tracing::info!(
                    originator = self.run_complete_originated,
                    primary_disconnected = self.primary_disconnected,
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
                let _ = self.request_task_for_worker(wid).await;
            }
        }

        // All `break` statements above represent terminal exits
        // (RunComplete observed, drain-down complete after primary
        // disconnect, single-secondary clean shutdown). The
        // SetupPending yield uses `return Ok(RunOutcome::SetupPending)`
        // directly, so reaching here means we're done.
        Ok(RunOutcome::Done)
    }

    /// Tick-driven re-check of the primary-link failover threshold.
    /// Called once per keepalive tick from `process_tasks`. The
    /// recv-None branch only triggers on a NEW recv-None event;
    /// since the bridge architecture turns the recv future permanently
    /// pending after a single None, a single dropped-bridge event would
    /// otherwise never re-evaluate the time axis. This method bridges
    /// the gap by polling `primary_link.should_arm_failover()` on
    /// every tick and arming once the time window elapses.
    ///
    /// Idempotent: harmless when the link is healthy (the predicate
    /// short-circuits on `first_failure_at.is_none()`), and harmless
    /// when failover is already armed (`primary_disconnected` short-
    /// circuits the body so we don't re-backdate `primary_last_seen`).
    /// `is_primary` short-circuits as well — a promoted secondary has
    /// no use for failover.
    pub(super) fn check_primary_link_threshold(&mut self) {
        if self.is_primary {
            return;
        }
        if !self.primary_link.is_link_failing() {
            return;
        }
        if !self.primary_link.should_arm_failover() {
            return;
        }
        // Already-armed: nothing to do — election is in flight.
        // We still want to gate the recv arm if it hasn't been
        // gated yet (first iteration of the time-elapsed branch
        // before any recv-None observation).
        if self.primary_disconnected {
            return;
        }
        let peers = self.peer_transport.peer_count();
        if peers == 0 {
            // The recv-arm None branch handles the no-mesh case via
            // `break`; we shouldn't be reachable here without at
            // least one peer (since the time-axis arming requires
            // a prior recv-None which would have taken the no-peer
            // exit). Defensive: exit cleanly if we somehow are.
            tracing::info!(
                "primary-link threshold breached and no peer mesh; \
                 deferring exit to natural termination path"
            );
            self.primary_disconnected = true;
            return;
        }
        tracing::warn!(
            connected_peers = peers,
            "primary-link failure-window elapsed; arming failover \
             (election will run via peer mesh)"
        );
        self.primary_disconnected = true;
        let backdate = self
            .config
            .keepalive_interval
            .saturating_mul(self.config.keepalive_miss_threshold + 1);
        self.primary_last_seen = Some(
            Instant::now()
                .checked_sub(backdate)
                .unwrap_or_else(Instant::now),
        );
    }

    /// Send keepalive to the current primary and broadcast to peers.
    /// In degraded-mesh mode (`peer_mesh_degraded`) the peer
    /// broadcast is skipped — there's nothing to broadcast to. The
    /// primary→secondary keepalive over WSS still fires so the
    /// primary keeps seeing us as alive.
    pub(super) async fn send_keepalive(&mut self) {
        let active_count = self
            .pool.workers
            .iter()
            .filter(|w| w.current_binary.is_some())
            .count() as u32;
        let msg = DistributedMessage::Keepalive {
            sender_id: self.config.secondary_id.clone(),
            timestamp: timestamp_now(),
            secondary_id: self.config.secondary_id.clone(),
            active_workers: active_count,
        };
        // Send to whoever is currently primary (local at run start;
        // the promoted peer after PromotePrimary).
        let _ = self.send_to_current_primary(msg.clone()).await;
        if self.peer_mesh_degraded {
            return;
        }
        // Broadcast to peers (including the primary if it's a peer —
        // duplicate but idempotent).
        let _ = self.peer_transport.broadcast(msg).await;
    }

    pub(super) async fn handle_worker_event(
        &mut self,
        event: WorkerEvent<I>,
    ) -> Result<Option<WorkerId>, String> {
        match event {
            WorkerEvent::TaskCompleted {
                worker_id,
                result,
                binary,
                ..
            } => {
                // Test-only: track tasks run on this secondary's own
                // workers. See `local_tasks_run` doc on
                // `SecondaryCoordinator`.
                #[cfg(test)]
                {
                    self.local_tasks_run += 1;
                }
                // Reclaim protocol state from the spawned poll task
                self.pool.workers[worker_id as usize].reclaim_protocol().await;
                self.pool.workers[worker_id as usize].clear_task();

                // Find the file hash for this worker's task
                let file_hash = self
                    .active_tasks
                    .iter()
                    .find(|&(_, &wid)| wid == worker_id)
                    .map(|(hash, _)| hash.clone());
                let log_task_hash = file_hash.clone();

                if let Some(hash) = file_hash {
                    self.active_tasks.remove(&hash);
                    // `completed_tasks` is the "saw it terminate" set;
                    // the primary's dispatch path uses it to
                    // avoid redispatching tasks the cluster has
                    // already finished. For Recoverable failures we
                    // intend to retry, so the hash must NOT land here
                    // — otherwise `handle_primary_task_request` would
                    // filter the re-injected binary out via its
                    // `completed_tasks` retain, and retry silently
                    // becomes a no-op. Mirrors the pre-existing
                    // dispatch.rs::TaskFailed forward and peer.rs::
                    // TaskFailed wire paths, both of which already
                    // skip `completed_tasks` insertion for
                    // Recoverable. The terminal-failure / success
                    // branches still insert below.
                    let recoverable_failure = !result.success
                        && result
                            .error_type
                            .as_ref()
                            .is_some_and(|e| matches!(
                                e,
                                dynrunner_core::ErrorType::Recoverable
                            ));
                    if !recoverable_failure {
                        self.completed_tasks.insert(hash.clone());
                    }

                    if result.success {
                        // Drive the primary's phase machine if
                        // this node is acting as one and dispatched
                        // the task — a no-op otherwise. Mid-run
                        // firing is what unblocks chained phases in
                        // the primary pool.
                        self.note_primary_item_completed(&hash);
                        // Report completion to the current primary
                        // (whichever node currently holds authority).
                        let msg = DistributedMessage::TaskComplete {
                            sender_id: self.config.secondary_id.clone(),
                            timestamp: timestamp_now(),
                            secondary_id: self.config.secondary_id.clone(),
                            worker_id,
                            task_hash: hash.clone(),
                            result_data: None,
                        };
                        self.send_to_current_primary(msg.clone()).await?;
                        let _ = self.peer_transport.broadcast(msg).await;
                    } else {
                        // Reuse the upstream classification from the
                        // worker protocol; default to NonRecoverable
                        // only when the worker layer didn't tag the
                        // result (genuine "unknown" — distinct from
                        // the disconnect path which now reports
                        // Recoverable explicitly via worker.rs /
                        // protocol-manager-worker).
                        let error_type = result
                            .error_type
                            .clone()
                            .unwrap_or(ErrorType::NonRecoverable);
                        // Failure-aware variant: Recoverable failures
                        // land in `primary_failed` for the
                        // retry pass. Phase-machine in-flight
                        // bookkeeping is identical to the success
                        // case (decrement + cascade).
                        self.note_primary_item_failed(&hash, &error_type);
                        // Synchronous drain-check (see peer.rs for
                        // rationale): immediately re-inject if this
                        // was the last in-flight task and there's
                        // retry budget left.
                        self.primary_drain_check_and_retry().await;
                        // Report error to the current primary.
                        let msg = DistributedMessage::TaskFailed {
                            sender_id: self.config.secondary_id.clone(),
                            timestamp: timestamp_now(),
                            secondary_id: self.config.secondary_id.clone(),
                            worker_id,
                            task_hash: hash.clone(),
                            error_type,
                            error_message: result
                                .error_message
                                .unwrap_or_else(|| "Unknown error".into()),
                        };
                        self.send_to_current_primary(msg.clone()).await?;
                        let _ = self.peer_transport.broadcast(msg).await;
                    }

                    // Request next task for this worker
                    self.request_task_for_worker(worker_id).await?;
                }

                // Operator-facing INFO: did the worker finish a
                // task, and did it succeed? The `secondary_id` is
                // redundant in per-secondary slurm_*.out files
                // (the file IS the secondary). Per-task identity
                // (task_id / phase / task_type) moves to the
                // sibling DEBUG so the operator log stays terse
                // while diagnostic context is preserved.
                tracing::info!(
                    worker_id,
                    task_hash = ?log_task_hash,
                    success = result.success,
                    "task done"
                );
                tracing::debug!(
                    secondary = %self.config.secondary_id,
                    worker_id,
                    task_id = ?binary.as_ref().and_then(|b| b.task_id.as_deref()),
                    phase = ?binary.as_ref().map(|b| b.phase_id.to_string()),
                    task_type = ?binary.as_ref().map(|b| b.type_id.to_string()),
                    task_hash = ?log_task_hash,
                    success = result.success,
                    "task done: identity"
                );

                Ok(None)
            }
            WorkerEvent::Disconnected {
                worker_id,
                result,
                binary,
            } => {
                // Reap the subprocess BEFORE reclaim_protocol so the
                // exit status rides the same log line as the
                // disconnect. `try_reap_exit` is non-blocking; `None`
                // means PID was untracked, kernel hasn't reaped yet
                // (SIGCHLD race), or the child was already reaped by
                // another path. See WorkerHandle::try_reap_exit for
                // the full set of None conditions.
                let exit_status = self.pool.workers[worker_id as usize].try_reap_exit();

                // Reclaim protocol state from the spawned poll task
                self.pool.workers[worker_id as usize].reclaim_protocol().await;
                self.pool.workers[worker_id as usize].clear_task();

                tracing::warn!(
                    worker_id,
                    error = ?result.error_message,
                    exit_status = exit_status.as_ref().map(|s| s.to_string()),
                    "worker disconnected"
                );

                // Find and report the task as failed
                let file_hash = self
                    .active_tasks
                    .iter()
                    .find(|&(_, &wid)| wid == worker_id)
                    .map(|(hash, _)| hash.clone());

                if let Some(hash) = file_hash {
                    self.active_tasks.remove(&hash);

                    // Discriminate two Disconnected-event shapes:
                    //
                    //   A. Worker explicitly reported a real
                    //      task failure on the wire — Response::
                    //      Error(NonRecoverable, msg). The
                    //      communication SUCCEEDED; the message
                    //      WAS delivered. The task ran (or at
                    //      least the worker attempted it) and
                    //      reported a non-recoverable failure.
                    //      → forward as TaskFailed(NonRecoverable)
                    //        so the primary records the real
                    //        failure (consumes retry budget if
                    //        applicable; surfaces in fail_final
                    //        per the outcome-class breakdown).
                    //
                    //   B. Pure transport-level disconnect — no
                    //      wire-level error response, just an
                    //      EOF on the manager's read end (the
                    //      protocol layer at `state.rs:152`
                    //      synthesises `Recoverable +
                    //      "transport disconnected"` for this
                    //      case; `worker.rs:154` synthesises
                    //      `Recoverable + "Disconnected before
                    //      Ready"` for the pre-Ready variant).
                    //      The communication FAILED; the task
                    //      may not have run at all.
                    //      → forward as backpressure-shaped
                    //        TaskFailed (Recoverable + the
                    //        marker the primary's
                    //        `is_backpressure` predicate
                    //        recognises) so the primary REQUEUES
                    //        the task at the pool without
                    //        consuming retry budget. The next
                    //        TaskRequest from the respawned
                    //        worker (or another peer) picks it
                    //        back up.
                    //
                    // Discriminator is `result.error_type`:
                    // NonRecoverable → real failure (A); anything
                    // else (Recoverable + transport-disconnected
                    // synthesis) → comm failure (B).
                    let error_type = result
                        .error_type
                        .clone()
                        .unwrap_or(ErrorType::Recoverable);
                    let is_comm_failure =
                        !matches!(error_type, ErrorType::NonRecoverable);

                    let (wire_error_type, wire_error_message) = if is_comm_failure {
                        (
                            ErrorType::Recoverable,
                            "worker pipe broken; respawning".to_string(),
                        )
                    } else {
                        (
                            error_type,
                            result
                                .error_message
                                .unwrap_or_else(|| "Worker disconnected".into()),
                        )
                    };

                    let msg = DistributedMessage::TaskFailed {
                        sender_id: self.config.secondary_id.clone(),
                        timestamp: timestamp_now(),
                        secondary_id: self.config.secondary_id.clone(),
                        worker_id,
                        task_hash: hash,
                        error_type: wire_error_type,
                        error_message: wire_error_message,
                    };
                    let _ = self.send_to_current_primary(msg.clone()).await;
                    let _ = self.peer_transport.broadcast(msg).await;
                }

                let _ = binary; // binary info already reported

                // Signal that this worker needs restart
                Ok(Some(worker_id))
            }
            WorkerEvent::PhaseUpdate {
                worker_id,
                phase_name,
            } => {
                tracing::debug!(worker_id, phase = %phase_name, "phase update");
                Ok(None)
            }
            WorkerEvent::Keepalive { worker_id } => {
                tracing::trace!(worker_id, "worker keepalive");
                Ok(None)
            }
            WorkerEvent::Ready { worker_id } => {
                tracing::debug!(worker_id, "worker ready");
                Ok(None)
            }
        }
    }

    pub(super) async fn stop_all_workers(&mut self) {
        self.pool.stop_all().await;
    }
}
