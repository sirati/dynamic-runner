use std::collections::HashSet;
use std::time::Duration;

use db_comm_api_base::{Identifier, ResourceMap};
use db_primary_secondary_comm::{
    DistributedMessage,
    SecondaryTransport, TaskInfo,
};
use db_scheduler_api::{
    ResourceEstimator, Scheduler,
};


use super::PrimaryCoordinator;
use super::wire::{binary_to_distributed, compute_task_hash, timestamp_now};

impl<T: SecondaryTransport<I>, S: Scheduler<I>, E: ResourceEstimator, I: Identifier> PrimaryCoordinator<T, S, E, I> {
    pub(super) async fn send_transfer_complete(&mut self) -> Result<(), String> {
        let secondary_ids: Vec<String> = self.secondaries.keys().cloned().collect();
        for secondary_id in secondary_ids {
            let msg = DistributedMessage::TransferComplete {
                sender_id: self.config.node_id.clone(),
                timestamp: timestamp_now(),
                total_files: 0,
                total_bytes: 0,
            };
            self.transport.send_to(&secondary_id, msg).await?;
        }
        tracing::info!("transfer complete sent to all secondaries");
        Ok(())
    }

    // ── Phase 7: Operational Loop ──

    pub(super) async fn operational_loop(&mut self) -> Result<(), String> {
        tracing::info!("entering operational loop");

        loop {
            // Check termination: all tasks accounted for
            if self.completed_tasks.len() + self.failed_tasks.len() >= self.total_tasks {
                tracing::info!("all tasks completed or failed");
                break;
            }

            let active_workers = self.workers.iter().filter(|w| w.current_task.is_some()).count();
            if self.pending_binaries.is_empty() && active_workers == 0 {
                tracing::info!("no pending binaries and no active workers");
                break;
            }

            // Use a timeout on recv to avoid stalling indefinitely if a
            // secondary disconnects while processing a task. The timeout
            // is generous — if no message arrives in 5 minutes and there
            // are in-flight tasks, something is wrong.
            tokio::select! {
                msg = self.transport.recv() => {
                    match msg {
                        Some(m) => self.dispatch_message(m).await?,
                        None => {
                            tracing::info!("transport closed");
                            break;
                        }
                    }
                }
                _ = tokio::time::sleep(Duration::from_secs(300)) => {
                    let active = self.workers.iter().filter(|w| w.current_task.is_some()).count();
                    if active > 0 {
                        tracing::warn!(
                            active_workers = active,
                            completed = self.completed_tasks.len(),
                            failed = self.failed_tasks.len(),
                            total = self.total_tasks,
                            "operational loop timeout with active workers, marking in-flight tasks as failed"
                        );
                        // Mark all in-flight tasks as failed
                        for worker in &mut self.workers {
                            if let Some(binary) = worker.current_task.take() {
                                let hash = compute_task_hash(&binary);
                                self.failed_tasks.insert(hash);
                                worker.estimated_resources = ResourceMap::new();
                                worker.is_idle = true;
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    // ── Phase 7: Promote SLURM-primary ──

    pub(super) async fn promote_slurm_primary(&mut self) -> Result<(), String> {
        if let Some(first_id) = self.secondaries.keys().next().cloned() {
            self.slurm_primary_id = Some(first_id.clone());
            tracing::info!(slurm_primary = %first_id, "promoting secondary to SLURM-primary");

            let msg = DistributedMessage::<I>::PromotePrimary {
                sender_id: self.config.node_id.clone(),
                timestamp: timestamp_now(),
                new_primary_id: first_id.clone(),
            };
            self.transport.send_to(&first_id, msg).await?;
        }
        Ok(())
    }

    // ── Phase 8: Send full task list ──

    pub(super) async fn send_full_task_list(&mut self) -> Result<(), String> {
        let slurm_id = match &self.slurm_primary_id {
            Some(id) => id.clone(),
            None => return Ok(()),
        };

        let all_tasks: Vec<TaskInfo<I>> = self
            .all_binaries
            .iter()
            .map(|binary| {
                let hash = compute_task_hash(binary);
                TaskInfo {
                    local_path: binary.path.to_string_lossy().into_owned(),
                    binary_info: binary_to_distributed(binary),
                    hash: hash.clone(),
                    file_path: Some(binary.path.to_string_lossy().into_owned()),
                }
            })
            .collect();

        // Include both completed tasks and currently in-flight tasks as "completed"
        // so the SLURM-primary doesn't re-assign tasks that are already being processed
        let active_hashes: HashSet<String> = self
            .workers
            .iter()
            .filter_map(|w| w.current_task.as_ref().map(compute_task_hash))
            .collect();
        let excluded: HashSet<String> = self
            .completed_tasks
            .union(&active_hashes)
            .cloned()
            .collect();

        let completed_list: Vec<String> = excluded.iter().cloned().collect();
        let pending_list: Vec<String> = all_tasks
            .iter()
            .filter(|t| !excluded.contains(&t.hash))
            .map(|t| t.hash.clone())
            .collect();

        let msg = DistributedMessage::FullTaskList {
            sender_id: self.config.node_id.clone(),
            timestamp: timestamp_now(),
            all_tasks,
            completed_tasks: completed_list,
            pending_tasks: pending_list,
        };
        self.transport.send_to(&slurm_id, msg).await?;

        tracing::info!(
            slurm_primary = %slurm_id,
            total = self.all_binaries.len(),
            "sent full task list"
        );
        Ok(())
    }

}
