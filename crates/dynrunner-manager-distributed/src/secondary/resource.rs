use std::time::Instant;

use dynrunner_core::{Identifier, WorkerId};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_manager_local::pool::ResourcePressureResult;
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
    E: ResourceEstimator + Clone,
    I: Identifier,
{
    pub(super) async fn check_resource_pressure(&mut self, factory: &mut impl WorkerFactory<M>) {
        let max = self.max_resources();
        match self.pool.check_resource_pressure(&self.scheduler, &max, false) {
            ResourcePressureResult::Killed {
                worker_id,
                reason,
                ..
            } => {
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
                        error_type: "OutOfMemory".into(),
                        error_message: reason,
                    };
                    let _ = self.primary_transport.send(msg.clone()).await;
                    let _ = self.peer_transport.broadcast(msg).await;
                }

                // Restart the worker and request a new task
                if let Err(e) = self.pool.restart_worker(worker_id, factory, false).await {
                    tracing::error!(worker_id, error = %e, "secondary OOM-restart failed");
                    return;
                }
                let _ = self.request_task_for_worker(worker_id).await;
            }
            ResourcePressureResult::NoAction => {}
        }
    }

    /// Handle a worker event (completion, disconnection, etc.)
    ///
    /// Returns `Some(worker_id)` if the worker needs to be restarted (e.g.
    /// after disconnect). The caller is responsible for calling
    pub(super) async fn request_task_for_worker(&mut self, worker_id: WorkerId) -> Result<(), String> {
        // When SLURM-primary, handle task requests locally
        if self.is_slurm_primary && !self.slurm_pending_binaries.is_empty() {
            let available_memory = if (worker_id as usize) < self.pool.workers.len() {
                self.pool.workers[worker_id as usize].reserved_budgets.get(&dynrunner_core::ResourceKind::memory())
            } else {
                self.config.max_resources.get(&dynrunner_core::ResourceKind::memory()) / self.config.num_workers as u64
            };
            return self
                .handle_slurm_task_request(
                    self.config.secondary_id.clone(),
                    worker_id,
                    available_memory,
                )
                .await;
        }

        let now = Instant::now();
        let backoff = self.request_backoff.get(&worker_id).copied()
            .unwrap_or(Self::INITIAL_BACKOFF);

        if let Some(last) = self.last_request_time.get(&worker_id) {
            if now.duration_since(*last) < backoff {
                return Ok(());
            }
        }

        let available_memory = if (worker_id as usize) < self.pool.workers.len() {
            self.pool.workers[worker_id as usize].reserved_budgets.get(&dynrunner_core::ResourceKind::memory())
        } else {
            self.config.max_resources.get(&dynrunner_core::ResourceKind::memory()) / self.config.num_workers as u64
        };

        let msg = DistributedMessage::TaskRequest {
            sender_id: self.config.secondary_id.clone(),
            timestamp: timestamp_now(),
            secondary_id: self.config.secondary_id.clone(),
            worker_id,
            available_resources: vec![dynrunner_core::ResourceAmount {
                kind: dynrunner_core::ResourceKind::memory(),
                amount: available_memory,
            }],
        };
        self.last_request_time.insert(worker_id, now);

        // Double the backoff for next time (capped)
        let next_backoff = (backoff * 2).min(Self::MAX_BACKOFF);
        self.request_backoff.insert(worker_id, next_backoff);

        // If the original primary is dead and an election has named a new
        // SLURM-primary peer, route the request there over the peer
        // transport instead of the (likely dead) primary_transport.
        if let Some(new_primary) = &self.slurm_primary_peer_id {
            if new_primary != &self.config.secondary_id {
                let peer = new_primary.clone();
                return self.peer_transport.send_to_peer(&peer, msg).await;
            }
            // new_primary == us means is_slurm_primary should already be true
            // and the local-handle path above handled the request.
        }
        self.primary_transport.send(msg).await
    }

    /// Reset rate limiting for a worker after a successful task assignment.
    pub(super) fn reset_request_backoff(&mut self, worker_id: WorkerId) {
        self.request_backoff.remove(&worker_id);
        self.last_request_time.remove(&worker_id);
    }

}
