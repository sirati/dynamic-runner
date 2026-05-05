use std::time::{Duration, Instant};

use dynrunner_core::{Identifier, WorkerId};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_manager_local::worker::WorkerEvent;
use dynrunner_manager_local::WorkerFactory;
use dynrunner_protocol_primary_secondary::{
    DistributedMessage, PeerTransport, PrimaryTransport,
};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};


use super::SecondaryCoordinator;
use super::wire::timestamp_now;

impl<PT, P, M, S, E, I> SecondaryCoordinator<PT, P, M, S, E, I>
where
    PT: PrimaryTransport<I>,
    P: PeerTransport<I>,
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    pub(super) async fn process_tasks(&mut self, factory: &mut impl WorkerFactory<M>) -> Result<(), String> {
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
                            // Primary's transport closed. Two
                            // distinct cases:
                            //
                            // 1. Single-secondary / no-peer-mesh
                            //    runs (the test fixture drops
                            //    primary explicitly to signal
                            //    shutdown; production single-jobs
                            //    runs do the same when the local
                            //    primary's run() returns). There's
                            //    no failover candidate, so exit
                            //    cleanly to preserve the historical
                            //    "primary close = end of run"
                            //    contract.
                            //
                            // 2. Multi-secondary failover. The peer
                            //    mesh is alive, an election can
                            //    pick a primary, and dispatch
                            //    can keep flowing. Backdating
                            //    `primary_last_seen` past the miss
                            //    threshold makes the next keepalive
                            //    tick's `run_election_tick` enter
                            //    Suspecting immediately. The
                            //    `primary_disconnected` guard above
                            //    suppresses this arm on subsequent
                            //    iterations so the persistently-None
                            //    recv future doesn't hot-loop.
                            //
                            // Pre-fix this arm bare-broke the loop
                            // for both cases and the secondary
                            // exited cleanly with completed=0 the
                            // moment the local primary's transport
                            // closed — losing every task the
                            // primary peer was about to
                            // dispatch. Dataset peer reported this
                            // on the dev-box-primary scenario.
                            let peers = self.peer_transport.peer_count();

                            // primary path: once this secondary has
                            // been promoted, the local-machine primary's
                            // transport closing is BENIGN regardless of peer
                            // count. The promoted secondary owns the task
                            // pool authoritatively (per the post-demotion
                            // contract: `lifecycle.rs` demotes the local
                            // primary on `PromotePrimary`, after which the
                            // local side is purely advisory). Two failure
                            // modes the framework hit before this guard:
                            //   1. `peers == 0` branch: tokenizer's cohort-5
                            //      run exited with `completed=19/216` when
                            //      the local primary went silent ~30min in
                            //      — the legacy `pending_local` heuristic
                            //      conditioned termination on WORK STATE
                            //      (primary_in_flight / active_tasks /
                            //      primary_pending) instead of ROLE STATE
                            //      and went wrong when those were
                            //      momentarily empty. Fixed in 5f6a267.
                            //   2. `peers > 0` branch: dataset's K=2 run
                            //      had sec-0 (the promoted primary)
                            //      enter "switching to failover detection
                            //      (election will run via peer mesh ...
                            //      once a peer is promoted)" — i.e. it
                            //      tried to elect a primary as if it
                            //      weren't already the elected one. ~3min
                            //      later sec-0 itself died with "transport
                            //      writer task exited" (likely from the
                            //      bogus self-election state churn) and
                            //      cascaded its peers into bail. Fixed
                            //      here, in this commit.
                            //
                            // Pull the `is_primary` check OUT of the
                            // peer-count branches so BOTH cases benefit:
                            // a promoted secondary should never re-enter
                            // election just because its bootstrap-time
                            // primary went away. Termination is owned by
                            // the natural terminal conditions
                            // (total-tasks / fleet-dead-analogue), NOT by
                            // the local primary's transport state.
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
                                // Don't break: keep iterating so worker
                                // events and any reconnecting peers land.
                                // Termination is owned by the pool-drained
                                // / total-tasks checks elsewhere in the
                                // loop.
                                continue;
                            }

                            if peers == 0 {
                                tracing::info!(
                                    "primary disconnected and no peer mesh; exiting cleanly \
                                     (no failover candidate to take authority)"
                                );
                                break;
                            }
                            tracing::warn!(
                                connected_peers = peers,
                                "primary transport closed; switching to failover detection \
                                 (election will run via peer mesh; further dispatch routes \
                                 through primary_peer_id once a peer is promoted)"
                            );
                            self.primary_disconnected = true;
                            let backdate = self
                                .config
                                .keepalive_interval
                                .saturating_mul(self.config.keepalive_miss_threshold + 1);
                            self.primary_last_seen =
                                Some(Instant::now().checked_sub(backdate).unwrap_or_else(Instant::now));
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
                    // limit (`request_backoff` doubles on each
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
                    || self.primary_retry_passes_used
                        >= self.config.retry_max_passes)
            {
                tracing::info!(
                    permanent_failures = self.primary_failed.len(),
                    "primary drained after live-primary disconnect; exiting"
                );
                break;
            }

            // Restart any workers that disconnected
            for wid in workers_to_restart {
                if let Err(e) = self.pool.restart_worker(wid, factory, false).await {
                    tracing::error!(worker_id = wid, error = %e, "secondary worker restart failed");
                    continue;
                }
                let _ = self.request_task_for_worker(wid).await;
            }
        }

        Ok(())
    }

    /// Send keepalive to the current primary and broadcast to peers.
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
                // Reclaim protocol state from the spawned poll task
                self.pool.workers[worker_id as usize].reclaim_protocol().await;
                self.pool.workers[worker_id as usize].clear_task();

                // Find the file hash for this worker's task
                let file_hash = self
                    .active_tasks
                    .iter()
                    .find(|&(_, &wid)| wid == worker_id)
                    .map(|(hash, _)| hash.clone());

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
                        // Compute the wire-format error_type once so
                        // the primary failure ledger and the
                        // outbound TaskFailed agree on the string.
                        let error_type = result
                            .error_type
                            .map(|e| format!("{:?}", e))
                            .unwrap_or_else(|| "Unknown".into());
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

                tracing::info!(
                    worker_id,
                    binary = ?binary.as_ref().map(|b| &b.identifier),
                    success = result.success,
                    "task completed"
                );

                Ok(None)
            }
            WorkerEvent::Disconnected {
                worker_id,
                result,
                binary,
            } => {
                // Reclaim protocol state from the spawned poll task
                self.pool.workers[worker_id as usize].reclaim_protocol().await;
                self.pool.workers[worker_id as usize].clear_task();

                tracing::warn!(
                    worker_id,
                    error = ?result.error_message,
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

                    let msg = DistributedMessage::TaskFailed {
                        sender_id: self.config.secondary_id.clone(),
                        timestamp: timestamp_now(),
                        secondary_id: self.config.secondary_id.clone(),
                        worker_id,
                        task_hash: hash,
                        error_type: "NonRecoverable".into(),
                        error_message: result
                            .error_message
                            .unwrap_or_else(|| "Worker disconnected".into()),
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

    pub(super) const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
    pub(super) const MAX_BACKOFF: Duration = Duration::from_secs(60);

    pub(super) async fn stop_all_workers(&mut self) {
        self.pool.stop_all().await;
    }
}
