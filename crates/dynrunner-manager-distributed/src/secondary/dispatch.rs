use std::collections::HashSet;

use dynrunner_core::{TaskInfo, Identifier, PhaseId, TypeId};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{
    DistributedMessage, PeerTransport, PrimaryTransport,
};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};


use super::SecondaryCoordinator;
use super::wire::{distributed_to_binary, timestamp_now};

impl<PT, P, M, S, E, I> SecondaryCoordinator<PT, P, M, S, E, I>
where
    PT: PrimaryTransport<I>,
    P: PeerTransport<I>,
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
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

                // When the secondary is configured with a staging
                // directory (`src_network` set) and resolution failed,
                // the file is genuinely missing — fail loudly here
                // instead of silently falling through to the primary's
                // filesystem-view path (which won't exist on this
                // secondary either, surfacing only at worker exec
                // time as a confusing crash).
                if resolved_path.is_none() && self.config.src_network.is_some() {
                    let wid = worker_id.min(self.pool.workers.len() as u32 - 1);
                    let msg = DistributedMessage::TaskFailed {
                        sender_id: self.config.secondary_id.clone(),
                        timestamp: timestamp_now(),
                        secondary_id: self.config.secondary_id.clone(),
                        worker_id: wid,
                        task_hash: file_hash.clone(),
                        error_type: "NonRecoverable".into(),
                        error_message: format!(
                            "file_hash {file_hash} not pre-staged at {local_path}; \
                             expected StageFile notification first"
                        ),
                    };
                    self.primary_transport.send(msg).await?;
                    return Ok(());
                }

                let binary = match resolved_path {
                    // TODO(phases-4b): wire phase_id/type_id/affinity_id/payload through
                    // DistributedBinaryInfo so the secondary preserves them.
                    Some(path) => TaskInfo {
                        path,
                        size: binary_info.size,
                        identifier: binary_info.identifier.clone(),
                        phase_id: PhaseId::from("default"),
                        type_id: TypeId::from("default"),
                        affinity_id: None,
                        payload: serde_json::Value::Null,
                    },
                    None => distributed_to_binary(&binary_info),
                };
                let estimated = self.estimator.estimate(&binary);
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
                    let estimated_mb = estimated.get(&dynrunner_core::ResourceKind::memory()) / (1024 * 1024);
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
            DistributedMessage::StageFile {
                secondary_id,
                file_hash,
                src_path,
                dest_path,
                ..
            } => {
                // Only act if addressed to us. The wire is broadcast-shaped
                // but each StageFile names exactly one secondary.
                if secondary_id != self.config.secondary_id {
                    tracing::debug!(
                        target = %secondary_id,
                        self_id = %self.config.secondary_id,
                        "ignoring StageFile addressed to another secondary"
                    );
                    return Ok(());
                }
                let src_tmp = self
                    .extraction_cache
                    .tmp_dir()
                    .to_path_buf();
                match super::staging::stage_file(
                    self.config.src_network.as_deref(),
                    &src_tmp,
                    &src_path,
                    &dest_path,
                    &file_hash,
                ) {
                    Ok(outcome) => {
                        self.extraction_cache
                            .register_path(&file_hash, outcome.dest);
                        tracing::info!(
                            file_hash = %file_hash,
                            "staged file registered"
                        );
                    }
                    Err(e) => {
                        tracing::error!(
                            file_hash = %file_hash,
                            error = %e,
                            "stage_file failed; the next TaskAssignment for this hash will be reported as TaskFailed"
                        );
                    }
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

                // Cache on every secondary: if we get promoted later we
                // can populate slurm_pending_binaries directly from this
                // snapshot without depending on a (then-dead) primary.
                self.cached_full_task_list = Some((all_tasks.clone(), completed_set.clone()));

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
                    .find(|r| r.kind == dynrunner_core::ResourceKind::memory())
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
