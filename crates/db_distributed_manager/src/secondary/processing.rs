use std::time::Duration;

use db_comm_api_base::{Identifier, WorkerId};
use db_manager_runner_comm::ManagerEndpoint;
use db_local_manager::worker::WorkerEvent;
use db_local_manager::WorkerFactory;
use db_primary_secondary_comm::{
    DistributedMessage, PeerTransport, PrimaryTransport,
};
use db_scheduler_api::{ResourceEstimator, Scheduler};


use super::SecondaryCoordinator;
use super::wire::timestamp_now;

impl<PT, P, M, S, E, I> SecondaryCoordinator<PT, P, M, S, E, I>
where
    PT: PrimaryTransport<I>,
    P: PeerTransport<I>,
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator + Clone,
    I: Identifier,
{
    pub(super) async fn process_tasks(&mut self, factory: &mut impl WorkerFactory<M>) -> Result<(), String> {
        tracing::info!("entering task processing loop");

        let mut keepalive_interval = tokio::time::interval(self.config.keepalive_interval);
        let mut oom_interval = tokio::time::interval(Duration::from_millis(100));

        // Request tasks only for workers that didn't get initial assignments
        for i in 0..self.pool.workers.len() {
            if self.pool.workers[i].is_idle_state() {
                self.request_task_for_worker(i as WorkerId).await?;
            }
        }

        loop {
            // Workers that need restart after disconnect
            let mut workers_to_restart: Vec<WorkerId> = Vec::new();

            tokio::select! {
                event = self.pool.recv_event() => {
                    if let Some(event) = event {
                        let restart = self.handle_worker_event(event).await?;
                        if let Some(wid) = restart {
                            workers_to_restart.push(wid);
                        }
                    }
                }
                msg = self.primary_transport.recv() => {
                    match msg {
                        Some(m) => {
                            self.dispatch_message(m).await?;
                        }
                        None => {
                            tracing::info!("primary disconnected");
                            break;
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

    /// Send keepalive to both primary and all peers.
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
        // Send to primary
        let _ = self.primary_transport.send(msg.clone()).await;
        // Broadcast to peers
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
                    self.completed_tasks.insert(hash.clone());

                    if result.success {
                        // Report completion to primary
                        let msg = DistributedMessage::TaskComplete {
                            sender_id: self.config.secondary_id.clone(),
                            timestamp: timestamp_now(),
                            secondary_id: self.config.secondary_id.clone(),
                            worker_id,
                            task_hash: hash.clone(),
                            result_data: None,
                        };
                        self.primary_transport.send(msg.clone()).await?;
                        // Broadcast to peers
                        let _ = self.peer_transport.broadcast(msg).await;
                    } else {
                        // Report error to primary
                        let msg = DistributedMessage::TaskFailed {
                            sender_id: self.config.secondary_id.clone(),
                            timestamp: timestamp_now(),
                            secondary_id: self.config.secondary_id.clone(),
                            worker_id,
                            task_hash: hash.clone(),
                            error_type: result
                                .error_type
                                .map(|e| format!("{:?}", e))
                                .unwrap_or_else(|| "Unknown".into()),
                            error_message: result
                                .error_message
                                .unwrap_or_else(|| "Unknown error".into()),
                        };
                        self.primary_transport.send(msg.clone()).await?;
                        // Broadcast to peers
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
                    let _ = self.primary_transport.send(msg.clone()).await;
                    // Broadcast failure to peers
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
