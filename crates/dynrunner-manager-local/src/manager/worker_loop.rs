use std::collections::HashSet;

use db_comm_api_base::{
    BinaryInfo, ErrorType, FailedTask, Identifier, ResourceKind, WorkerId,
};
use db_manager_runner_comm::ManagerEndpoint;
use db_scheduler_api::{
    AssignmentDecision, ResourceEstimator, ProcessingPhase, Scheduler,
};

use super::{LocalManager, WorkerFactory};

impl<M: ManagerEndpoint + 'static, S: Scheduler<I>, E: ResourceEstimator, I: Identifier> LocalManager<M, S, E, I> {
    pub(super) async fn process_worker_loop(
        &mut self,
        active_workers: &mut HashSet<WorkerId>,
        allow_stop: bool,
        on_failure_increment_failed: bool,
        phase: ProcessingPhase,
        factory: &mut impl WorkerFactory<M>,
    ) {
        let mut pressure_check_interval =
            tokio::time::interval(self.config.resource_check_interval);

        while !active_workers.is_empty() {
            // Try to assign tasks to any idle workers
            self.assign_idle_workers(active_workers, allow_stop, phase, factory).await;

            // If no workers are processing and no pending assignments, we're done
            let any_processing = active_workers.iter().any(|&wid| {
                let idx = wid as usize;
                self.pool.workers[idx].is_processing()
            });
            if !any_processing && self.pending_worker_assignments.is_empty() {
                break;
            }

            // Wait for either a worker event or the OOM check timer
            tokio::select! {
                event = self.pool.recv_event() => {
                    if let Some(event) = event {
                        self.handle_event(
                            event,
                            active_workers,
                            allow_stop,
                            on_failure_increment_failed,
                            phase,
                            factory,
                        )
                        .await;
                    }
                }
                _ = pressure_check_interval.tick() => {
                    // Periodic maintenance: resource pressure checks, usage updates, timeouts
                    self.pool.update_all_resource_usage();
                    if !self.pending_binaries.is_empty() {
                        self.check_resource_pressure();
                    }
                    self.check_timeouts(active_workers, on_failure_increment_failed, factory).await;
                    self.report_stuck_workers();
                }
            }

            // Handle pending worker reassignments after events
            if !self.pending_worker_assignments.is_empty() {
                let pending: Vec<WorkerId> =
                    self.pending_worker_assignments.iter().copied().collect();
                for worker_id in pending {
                    let idx = worker_id as usize;
                    if self.pool.workers[idx].current_binary.is_none() && self.pool.workers[idx].is_ready() {
                        self.try_assign_normal(worker_id, factory).await;
                        self.pending_worker_assignments.remove(&worker_id);
                    }
                }
            }
        }

        // Move remaining pending to resource pressure queue at end of retry phase
        // (Main phase leftovers go to unassigned_tasks in run_main_phase)
        if phase == ProcessingPhase::RetryPhase {
            if !self.pending_binaries.is_empty() {
                let remaining: Vec<BinaryInfo<I>> = self.pending_binaries.drain(..).collect();
                for binary in remaining {
                    self.resource_pressure_tasks.push(FailedTask {
                        binary,
                        error_type: ErrorType::ResourceExhausted(ResourceKind::memory()),
                        error_message: "Could not fit in any worker budget".into(),
                        retry_count: 0,
                    });
                }
            }
        }
    }

    /// Try to assign tasks to all idle active workers.
    pub(super) async fn assign_idle_workers(
        &mut self,
        active_workers: &mut HashSet<WorkerId>,
        allow_stop: bool,
        phase: ProcessingPhase,
        factory: &mut impl WorkerFactory<M>,
    ) {
        let worker_ids: Vec<WorkerId> = active_workers.iter().copied().collect();
        for worker_id in worker_ids {
            let idx = worker_id as usize;

            // Poll not-yet-ready workers (still in WaitingForReady state)
            if !self.pool.workers[idx].is_ready() {
                self.pool.workers[idx].poll_ready().await;
                if !self.pool.workers[idx].is_ready() {
                    if self.pending_binaries.is_empty() && allow_stop {
                        active_workers.remove(&worker_id);
                    }
                    continue;
                }
            }

            // Skip workers that are already processing
            if self.pool.workers[idx].is_processing() {
                continue;
            }

            if self.pool.workers[idx].current_binary.is_none() {
                // Worker has no task — try to assign
                if !self.handle_worker_without_task(worker_id, active_workers, allow_stop, phase) {
                    continue;
                }
                // If marked for assignment, do it now
                if self.pending_worker_assignments.contains(&worker_id) {
                    self.try_assign_normal(worker_id, factory).await;
                    self.pending_worker_assignments.remove(&worker_id);
                }
            }
        }
    }

    pub(super) fn handle_worker_without_task(
        &mut self,
        worker_id: WorkerId,
        active_workers: &mut HashSet<WorkerId>,
        allow_stop: bool,
        _phase: ProcessingPhase,
    ) -> bool {
        // Synchronous decision, async assign handled via pending_worker_assignments
        let worker_info = self.pool.workers[worker_id as usize].budget_info();
        let all_infos = self.pool.budget_infos();
        let max = self.max_resources();
        let decision = self.scheduler.assign_normal(
            &worker_info,
            &all_infos,
            &self.pending_binaries,
            max,
            &self.estimator,
            false,
        );

        match decision {
            AssignmentDecision::Assign { binary_index, .. } => {
                // Mark for async assignment
                self.pending_worker_assignments.insert(worker_id);
                // We'll do the actual assign in the pending_worker_assignments loop
                // But we can't await here (not async fn), so let's just return true
                // to keep the worker active. The assignment happens next iteration.
                let _ = binary_index;
                true
            }
            AssignmentDecision::NoFit => {
                // Retry with retry_attempt=true
                let decision2 = self.scheduler.assign_normal(
                    &worker_info,
                    &all_infos,
                    &self.pending_binaries,
                    max,
                    &self.estimator,
                    true,
                );
                match decision2 {
                    AssignmentDecision::Assign { .. } => {
                        self.pending_worker_assignments.insert(worker_id);
                        true
                    }
                    _ => {
                        if self.pending_binaries.is_empty() {
                            if allow_stop {
                                tracing::info!(worker_id, "stopping (no more tasks)");
                            }
                            active_workers.remove(&worker_id);
                            false
                        } else {
                            true // idle but binaries remain
                        }
                    }
                }
            }
            AssignmentDecision::NoPendingTasks => {
                if allow_stop {
                    tracing::info!(worker_id, "stopping (no more tasks)");
                }
                active_workers.remove(&worker_id);
                false
            }
        }
    }

    pub(super) async fn try_assign_normal(&mut self, worker_id: WorkerId, factory: &mut impl WorkerFactory<M>) {
        let worker_info = self.pool.workers[worker_id as usize].budget_info();
        let all_infos = self.pool.budget_infos();
        let max = self.max_resources();
        let decision = self.scheduler.assign_normal(
            &worker_info,
            &all_infos,
            &self.pending_binaries,
            max,
            &self.estimator,
            false,
        );

        match decision {
            AssignmentDecision::Assign {
                binary_index,
                estimated_usage,
                opportunistic,
                ..
            } => {
                let binary = self.pending_binaries.remove(binary_index);
                let name = binary.path.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
                let estimated_mb = estimated_usage.get(&ResourceKind::memory()) / (1024 * 1024);
                let worker = &mut self.pool.workers[worker_id as usize];
                match worker
                    .assign_task(binary.clone(), estimated_usage.clone(), opportunistic)
                    .await
                {
                    Ok(()) => {
                        self.total_assigned_resources.add(&estimated_usage);
                        tracing::info!(
                            worker_id,
                            binary = %name,
                            estimated_mb,
                            "assigned task"
                        );
                        // Reset failure count on success
                        self.pool.workers[worker_id as usize].assignment_failure_count = 0;
                    }
                    Err(e) => {
                        // Put binary back
                        self.pending_binaries.insert(0, binary);
                        self.handle_assignment_failure(worker_id, &e, factory).await;
                    }
                }
            }
            AssignmentDecision::NoFit | AssignmentDecision::NoPendingTasks => {}
        }
    }

    /// Handle assignment failure with restart and 3-attempt limit.
    pub(super) async fn handle_assignment_failure(
        &mut self,
        worker_id: WorkerId,
        error_msg: &str,
        factory: &mut impl WorkerFactory<M>,
    ) {
        let worker = &mut self.pool.workers[worker_id as usize];
        worker.assignment_failure_count += 1;
        let count = worker.assignment_failure_count;

        tracing::warn!(
            worker_id,
            failure_count = count,
            error = %error_msg,
            "assignment failure"
        );

        if count >= 3 {
            tracing::error!(
                worker_id,
                attempts = count,
                "worker failed to receive assignments after 3 attempts, communication broken"
            );
            // In Python this raises RuntimeError, crashing the manager.
            // Here we panic to match that behavior.
            panic!(
                "Worker {worker_id} failed to receive assignments after {count} attempts. \
                 Communication channel is broken."
            );
        }

        // Restart the worker
        tracing::info!(worker_id, attempt = count, "restarting worker after assignment failure");
        self.restart_worker(worker_id, factory).await;
        self.pending_worker_assignments.insert(worker_id);
    }

    /// Restart a worker: stop the old one, spawn a new transport via factory.
    /// Mid-run respawn failures are logged and the worker is left stopped;
    /// the orchestrator continues with the remaining workers rather than
    /// aborting the whole run for one slot.
    pub(super) async fn restart_worker(&mut self, worker_id: WorkerId, factory: &mut impl WorkerFactory<M>) {
        if let Err(e) = self
            .pool
            .restart_worker(worker_id, factory, self.config.print_pid)
            .await
        {
            tracing::error!(worker_id, error = %e, "worker restart failed; slot will remain stopped");
        }
    }
}
