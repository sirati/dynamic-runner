use std::collections::HashMap;

use dynrunner_core::{TaskInfo, Identifier, ResourceMap};
use dynrunner_protocol_primary_secondary::{
    DistributedMessage,
    SecondaryTransport, WorkerReadyInfo, ZipBinaryEntry, ZipFileAssignment,
};
use dynrunner_scheduler_api::{
    AssignmentDecision, ResourceEstimator, Scheduler,
};

use crate::state::SecondaryConnectionState;

use super::{PrimaryCoordinator, RemoteWorkerState};
use super::wire::{binary_to_distributed, compute_task_hash, timestamp_now};

impl<T: SecondaryTransport<I>, S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<T, S, E, I> {
    pub(super) async fn perform_initial_assignment(&mut self) -> Result<(), String> {
        tracing::info!("performing initial assignment");

        let mut global_worker_id: u32 = 0;
        let secondary_ids: Vec<String> = self.secondaries.keys().cloned().collect();

        for secondary_id in &secondary_ids {
            let state = self.secondaries.get(secondary_id).unwrap();
            let num_workers = state.num_workers();
            let ram_bytes = state.resources().iter()
                .find(|r| r.kind == dynrunner_core::ResourceKind::memory())
                .map(|r| r.amount)
                .unwrap_or(0);
            let max_res = dynrunner_core::ResourceMap::from([(dynrunner_core::ResourceKind::memory(), ram_bytes)]);

            for local_idx in 0..num_workers {
                let budget = self.scheduler.initial_budget(local_idx, &max_res);
                self.workers.push(RemoteWorkerState {
                    worker_id: global_worker_id,
                    secondary_id: secondary_id.clone(),
                    resource_budgets: budget,
                    current_task: None,
                    estimated_resources: ResourceMap::new(),
                    is_idle: true,
                });
                global_worker_id += 1;
            }
        }

        // Perform initial assignment for each worker. The pool is
        // pre-sorted by `run()` (size DESC) and bucketed by
        // `(phase, type, affinity)`; per-worker visibility is the
        // `view_for_worker` slice the scheduler chooses from.
        let mut assignments_per_secondary: HashMap<String, Vec<(u32, TaskInfo<I>, ResourceMap)>> =
            HashMap::new();
        let mut total_assigned_resources = ResourceMap::new();

        for worker_idx in 0..self.workers.len() {
            let worker_info = self.workers[worker_idx].budget_info();
            let max_res = self.workers[worker_idx].resource_budgets.clone();
            let global_wid = self.workers[worker_idx].worker_id;
            let view = self.pool().view_for_worker(global_wid);
            if view.is_empty() {
                continue;
            }
            let decision = self.scheduler.assign_initial(
                &worker_info,
                view.tasks(),
                &total_assigned_resources,
                &max_res,
                &self.estimator,
            );

            if let AssignmentDecision::Assign {
                binary_index,
                estimated_usage,
                ..
            } = decision
            {
                let binary = match self.pool_mut().take_from_view(view, binary_index) {
                    Some(b) => b,
                    None => continue, // scheduler returned an out-of-range index
                };
                total_assigned_resources.add(&estimated_usage);

                let secondary_id = self.workers[worker_idx].secondary_id.clone();
                // Compute local worker index within that secondary
                let local_worker_id = self.workers[..worker_idx + 1]
                    .iter()
                    .filter(|w| w.secondary_id == secondary_id)
                    .count() as u32
                    - 1;

                self.workers[worker_idx].current_task = Some(binary.clone());
                self.workers[worker_idx].estimated_resources = estimated_usage.clone();
                self.workers[worker_idx].is_idle = false;

                assignments_per_secondary
                    .entry(secondary_id)
                    .or_default()
                    .push((local_worker_id, binary, estimated_usage));
            }
        }

        // Send initial assignments to each secondary
        for (secondary_id, assignments) in &assignments_per_secondary {
            let zip_files = vec![ZipFileAssignment {
                zip_name: String::new(),
                binaries: assignments
                    .iter()
                    .map(|(_, binary, _)| ZipBinaryEntry {
                        local_path: binary.path.to_string_lossy().into_owned(),
                        binary_info: binary_to_distributed(binary),
                        hash: compute_task_hash(binary),
                    })
                    .collect(),
            }];

            let workers_ready: Vec<WorkerReadyInfo> = assignments
                .iter()
                .map(|(worker_id, _, est_res)| WorkerReadyInfo {
                    worker_id: *worker_id,
                    resource_budgets: est_res.iter()
                        .map(|(kind, amount)| dynrunner_core::ResourceAmount {
                            kind: kind.clone(),
                            amount,
                        })
                        .collect(),
                })
                .collect();

            let msg = DistributedMessage::InitialAssignment {
                sender_id: self.config.node_id.clone(),
                timestamp: timestamp_now(),
                secondary_id: secondary_id.clone(),
                zip_files,
                workers_ready,
            };
            self.transport.send_to(secondary_id, msg).await?;
        }

        // Transition all to Operational
        for secondary_id in &secondary_ids {
            if let Some(state) = self.secondaries.remove(secondary_id) {
                let new_state = match state {
                    SecondaryConnectionState::InitialAssigning(conn) => {
                        SecondaryConnectionState::Operational(conn.assignments_sent())
                    }
                    other => other,
                };
                self.secondaries.insert(secondary_id.clone(), new_state);
            }
        }

        let assigned: usize = assignments_per_secondary.values().map(|v| v.len()).sum();
        tracing::info!(
            assigned,
            remaining = self.pool().len(),
            "initial assignment complete"
        );

        Ok(())
    }

    // ── Phase 6: Transfer Complete ──

}
