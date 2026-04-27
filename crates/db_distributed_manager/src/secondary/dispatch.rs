use std::collections::HashSet;

use db_comm_api_base::{BinaryInfo, Identifier};
use db_manager_runner_comm::ManagerEndpoint;
use db_primary_secondary_comm::{
    DistributedMessage, PeerTransport, PrimaryTransport,
};
use db_scheduler_api::{ResourceEstimator, Scheduler};


use super::SecondaryCoordinator;
use super::wire::{distributed_to_binary, timestamp_now};

impl<PT, P, M, S, E, I> SecondaryCoordinator<PT, P, M, S, E, I>
where
    PT: PrimaryTransport<I>,
    P: PeerTransport<I>,
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator + Clone,
    I: Identifier,
{
    pub(super) async fn dispatch_message(&mut self, msg: DistributedMessage<I>) -> Result<(), String> {
        // Any message from the primary side resets the election state and
        // bumps the keepalive timestamp (F2).
        self.record_primary_message();

        match msg {
            DistributedMessage::TaskAssignment {
                worker_id,
                file_hash,
                binary_info,
                zip_file,
                local_path,
                ..
            } => {
                // Resolve binary path: file-ready or ZIP extraction
                let zip_ref = zip_file.as_deref().filter(|z| !z.is_empty());
                let resolved_path = self
                    .extraction_cache
                    .resolve_binary(zip_ref, &local_path, &file_hash);

                let binary = match resolved_path {
                    Some(path) => BinaryInfo {
                        path,
                        size: binary_info.size,
                        identifier: binary_info.identifier.clone(),
                    },
                    None => distributed_to_binary(&binary_info),
                };
                let estimated = self.estimator.estimate(binary.size);
                let wid = worker_id.min(self.pool.workers.len() as u32 - 1);

                // Find the target worker — prefer the requested one, fall back to any idle
                let target_wid = if self.pool.workers[wid as usize].is_idle_state() {
                    wid
                } else {
                    self.pool.workers
                        .iter()
                        .position(|w| w.is_idle_state())
                        .map(|i| i as u32)
                        .unwrap_or(wid)
                };

                let worker = &mut self.pool.workers[target_wid as usize];
                if worker.is_idle_state() {
                    let estimated_mb = estimated.get(&db_comm_api_base::ResourceKind::memory()) / (1024 * 1024);
                    match worker.assign_task(binary, estimated, false).await {
                        Ok(()) => {
                            self.active_tasks.insert(file_hash, target_wid);
                            self.reset_request_backoff(target_wid);
                            tracing::info!(
                                worker_id = target_wid,
                                binary = ?binary_info.identifier,
                                estimated_mb,
                                "assigned task from primary"
                            );
                        }
                        Err(e) => {
                            tracing::error!(
                                worker_id = target_wid,
                                error = %e,
                                "failed to assign task"
                            );
                            let msg = DistributedMessage::TaskFailed {
                                sender_id: self.config.secondary_id.clone(),
                                timestamp: timestamp_now(),
                                secondary_id: self.config.secondary_id.clone(),
                                worker_id: target_wid,
                                task_hash: file_hash,
                                error_type: "NonRecoverable".into(),
                                error_message: e,
                            };
                            self.primary_transport.send(msg).await?;
                        }
                    }
                } else {
                    tracing::warn!(
                        worker_id = target_wid,
                        "no idle worker available for task assignment"
                    );
                    let msg = DistributedMessage::TaskFailed {
                        sender_id: self.config.secondary_id.clone(),
                        timestamp: timestamp_now(),
                        secondary_id: self.config.secondary_id.clone(),
                        worker_id: target_wid,
                        task_hash: file_hash,
                        error_type: "Recoverable".into(),
                        error_message: "No idle worker available".into(),
                    };
                    self.primary_transport.send(msg).await?;
                }
                Ok(())
            }
            DistributedMessage::PromotePrimary { new_primary_id, .. } => {
                self.is_slurm_primary = new_primary_id == self.config.secondary_id;
                if self.is_slurm_primary {
                    tracing::info!("this secondary has been promoted to SLURM-primary");
                } else {
                    tracing::info!(
                        new_primary = %new_primary_id,
                        "another secondary promoted to SLURM-primary"
                    );
                }
                Ok(())
            }
            DistributedMessage::FullTaskList {
                all_tasks,
                completed_tasks,
                pending_tasks,
                ..
            } => {
                let completed_set: HashSet<String> = completed_tasks.into_iter().collect();
                tracing::info!(
                    total = all_tasks.len(),
                    completed = completed_set.len(),
                    pending = pending_tasks.len(),
                    "received full task list"
                );

                if self.is_slurm_primary {
                    self.populate_slurm_tasks(all_tasks, completed_set);
                }
                Ok(())
            }
            DistributedMessage::TaskRequest {
                secondary_id,
                worker_id,
                available_resources,
                ..
            } if self.is_slurm_primary => {
                let available_memory = available_resources.iter()
                    .find(|r| r.kind == db_comm_api_base::ResourceKind::memory())
                    .map(|r| r.amount)
                    .unwrap_or(0);
                self.handle_slurm_task_request(secondary_id, worker_id, available_memory)
                    .await
            }
            _ => {
                tracing::debug!(msg_type = ?msg.msg_type(), "unhandled message in secondary");
                Ok(())
            }
        }
    }

}
