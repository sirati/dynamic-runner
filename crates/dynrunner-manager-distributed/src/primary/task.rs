
use dynrunner_core::{TaskInfo, Identifier, ResourceMap};
use dynrunner_protocol_primary_secondary::{
    DistributedMessage,
    SecondaryTransport,
};
use dynrunner_scheduler_api::{
    AssignmentDecision, ResourceEstimator, Scheduler, WorkerBudgetInfo,
};


use super::PrimaryCoordinator;
use super::wire::{binary_to_distributed, compute_task_hash, timestamp_now};

impl<T: SecondaryTransport<I>, S: Scheduler<I>, E: ResourceEstimator, I: Identifier> PrimaryCoordinator<T, S, E, I> {
    pub(super) async fn handle_task_request(&mut self, msg: DistributedMessage<I>) -> Result<(), String> {
        if let DistributedMessage::TaskRequest {
            ref secondary_id,
            worker_id,
            ref available_resources,
            ..
        } = msg
        {
            let available_res: ResourceMap = available_resources.iter()
                .map(|r| (r.kind.clone(), r.amount))
                .collect();
            // Find matching worker
            let mut target_idx = None;
            let mut local_idx: u32 = 0;
            for (idx, w) in self.workers.iter().enumerate() {
                if w.secondary_id == *secondary_id {
                    if local_idx == worker_id {
                        target_idx = Some(idx);
                        break;
                    }
                    local_idx += 1;
                }
            }

            let mut assigned = false;

            if let Some(idx) = target_idx {
                // Mark worker idle
                self.workers[idx].current_task = None;
                self.workers[idx].estimated_resources = ResourceMap::new();
                self.workers[idx].is_idle = true;
                if !available_res.is_empty() {
                    self.workers[idx].resource_budgets = available_res.clone();
                }

                // Try to assign from local pending
                if !self.pending_binaries.is_empty() {
                    let worker_info = self.workers[idx].budget_info();
                    let all_infos: Vec<WorkerBudgetInfo<I>> =
                        self.workers.iter().map(|w| w.budget_info()).collect();
                    let max_res = self.workers[idx].resource_budgets.clone();

                    let decision = self.scheduler.assign_normal(
                        &worker_info,
                        &all_infos,
                        &self.pending_binaries,
                        &max_res,
                        &self.estimator,
                        false,
                    );

                    if let AssignmentDecision::Assign {
                        binary_index,
                        estimated_usage,
                        ..
                    } = decision
                    {
                        let binary = self.pending_binaries.remove(binary_index);
                        let sec_id = self.workers[idx].secondary_id.clone();
                        self.workers[idx].current_task = Some(binary.clone());
                        self.workers[idx].estimated_resources = estimated_usage;
                        self.workers[idx].is_idle = false;

                        let assignment_msg = DistributedMessage::TaskAssignment {
                            sender_id: self.config.node_id.clone(),
                            timestamp: timestamp_now(),
                            secondary_id: sec_id.clone(),
                            worker_id,
                            zip_file: None,
                            binary_info: binary_to_distributed(&binary),
                            local_path: binary.path.to_string_lossy().into_owned(),
                            file_hash: compute_task_hash(&binary),
                        };
                        self.transport.send_to(&sec_id, assignment_msg).await?;

                        tracing::debug!(
                            secondary = %sec_id,
                            worker_id,
                            binary = ?binary.identifier,
                            "task assigned"
                        );
                        assigned = true;
                    }
                }
            }

            // If no local assignment was made, relay to SLURM-primary
            if !assigned {
                if let Some(slurm_id) = self.slurm_primary_id.clone() {
                    self.transport.send_to(&slurm_id, msg).await?;
                }
            }
        }
        Ok(())
    }

    pub(super) fn handle_task_complete(&mut self, msg: DistributedMessage<I>) {
        if let DistributedMessage::TaskComplete {
            secondary_id,
            worker_id,
            task_hash,
            ..
        } = msg
        {
            self.completed_tasks.insert(task_hash);

            // Mark the specific worker idle using secondary_id + local worker_id
            let mut local_idx: u32 = 0;
            for w in &mut self.workers {
                if w.secondary_id == secondary_id {
                    if local_idx == worker_id {
                        w.current_task = None;
                        w.estimated_resources = ResourceMap::new();
                        w.is_idle = true;
                        break;
                    }
                    local_idx += 1;
                }
            }

            tracing::debug!(
                secondary = %secondary_id,
                worker_id,
                completed = self.completed_tasks.len(),
                "task complete"
            );
        }
    }

    pub(super) fn handle_task_failed(&mut self, msg: DistributedMessage<I>) {
        if let DistributedMessage::TaskFailed {
            secondary_id,
            worker_id,
            task_hash,
            error_type,
            error_message,
            ..
        } = msg
        {
            // Find the specific worker and recover the binary if it's a
            // recoverable error so it can be re-assigned to another worker.
            let mut recovered_binary: Option<TaskInfo<I>> = None;
            let mut local_idx: u32 = 0;
            for w in &mut self.workers {
                if w.secondary_id == secondary_id {
                    if local_idx == worker_id {
                        recovered_binary = w.current_task.take();
                        w.estimated_resources = ResourceMap::new();
                        w.is_idle = true;
                        break;
                    }
                    local_idx += 1;
                }
            }

            if error_type == "Recoverable" {
                // Re-enqueue recoverable failures for assignment to another worker
                if let Some(binary) = recovered_binary {
                    tracing::info!(
                        secondary = %secondary_id,
                        worker_id,
                        error = %error_message,
                        "recoverable failure, re-enqueuing task"
                    );
                    self.pending_binaries.push(binary);
                } else {
                    // Can't recover — no binary info available
                    self.failed_tasks.insert(task_hash);
                }
            } else {
                // Non-recoverable: permanently mark as failed
                self.failed_tasks.insert(task_hash);
            }

            tracing::warn!(
                secondary = %secondary_id,
                worker_id,
                error_type = %error_type,
                error = %error_message,
                "task failed"
            );
        }
    }
}
