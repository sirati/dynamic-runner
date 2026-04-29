use std::collections::HashSet;

use dynrunner_core::{
    ErrorType, FailedTask, Identifier, ResourceKind, TaskInfo, WorkerId,
};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_scheduler_api::{
    AssignmentDecision, ProcessingPhase, ResourceEstimator, Scheduler,
};

use super::{LocalManager, WorkerFactory};

impl<M: ManagerEndpoint + 'static, S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> LocalManager<M, S, E, I> {
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
                        // Per-event drain-transition flush. A finishing
                        // task may have just emptied its phase's queue
                        // and zeroed its in-flight count; mid-run firing
                        // is what unblocks dependents (`Blocked → Active`
                        // happens inside `mark_phase_done`'s cascade).
                        // Phases whose items still live in a side queue
                        // are deferred — see
                        // `process_drain_transitions`.
                        self.process_drain_transitions();
                    }
                }
                _ = pressure_check_interval.tick() => {
                    // Periodic maintenance: resource pressure checks, usage updates, timeouts
                    self.pool.update_all_resource_usage();
                    if !self.pool_ref().is_empty() {
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
        if phase == ProcessingPhase::RetryPhase
            && !self.pool_ref().is_empty()
        {
            let remaining: Vec<TaskInfo<I>> = self.pool_mut().drain_queued();
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
                    if self.pool_ref().is_empty() && allow_stop {
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
        // Synchronous decision: peek at the affinity-ordered candidate
        // slice for this worker and ask the scheduler whether anything
        // fits. The actual `take` happens asynchronously in
        // `try_assign_normal` below; we just decide here whether to keep
        // the worker active or stop it.
        let worker_info = self.pool.workers[worker_id as usize].budget_info();
        let all_infos = self.pool.budget_infos();
        let max = self.max_resources();
        let view = self.pool_ref().view_for_worker(worker_id);
        let decision = self.scheduler.assign_normal(
            &worker_info,
            &all_infos,
            view.as_slice(),
            max,
            &self.estimator,
            false,
        );

        match decision {
            AssignmentDecision::Assign { .. } => {
                // Mark for async assignment; try_assign_normal will
                // re-run the scheduler against a fresh view and take.
                self.pending_worker_assignments.insert(worker_id);
                true
            }
            AssignmentDecision::NoFit => {
                // Retry with retry_attempt=true
                let view2 = self.pool_ref().view_for_worker(worker_id);
                let decision2 = self.scheduler.assign_normal(
                    &worker_info,
                    &all_infos,
                    view2.as_slice(),
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
                        if self.pool_ref().is_empty() {
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
        let view = self.pool_ref().view_for_worker(worker_id);
        let decision = self.scheduler.assign_normal(
            &worker_info,
            &all_infos,
            view.as_slice(),
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
                let binary = self.pool_mut().take_from_view(view, binary_index);
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
                        // Put binary back at the front of its bucket — it
                        // was in-flight (take_from_view bumped in-flight)
                        // and now needs to be re-attempted.
                        self.pool_mut().requeue(binary);
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
