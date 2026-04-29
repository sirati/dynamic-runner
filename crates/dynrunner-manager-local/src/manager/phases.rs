use std::collections::HashSet;

use dynrunner_core::{
    TaskInfo, FailedTask, Identifier, ResourceKind, WorkerId,
};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_scheduler_api::{
    AssignmentDecision, ResourceEstimator, ProcessingPhase, Scheduler,
};


use super::{LocalManager, WorkerFactory};

impl<M: ManagerEndpoint + 'static, S: Scheduler<I>, E: ResourceEstimator, I: Identifier> LocalManager<M, S, E, I> {
    pub(super) async fn run_initial_assignments(&mut self, factory: &mut impl WorkerFactory<M>) {
        tracing::info!("starting initial assignment phase");

        loop {
            let all_assigned = self
                .pool.workers
                .iter()
                .all(|w| w.has_initial_assignment);
            if all_assigned {
                break;
            }

            for i in 0..self.pool.workers.len() {
                if self.pool.workers[i].has_initial_assignment || !self.pool.workers[i].is_ready() {
                    continue;
                }
                self.try_assign_initial(i as WorkerId, factory).await;
            }
            tokio::task::yield_now().await;
        }

        let opp_mem: u64 = self
            .pool.workers
            .iter()
            .filter(|w| w.opportunistic && w.current_binary.is_some())
            .map(|w| w.estimated_resources.get(&ResourceKind::memory()))
            .sum();
        let non_opp_mem: u64 = self
            .pool.workers
            .iter()
            .filter(|w| !w.opportunistic && w.current_binary.is_some())
            .map(|w| w.estimated_resources.get(&ResourceKind::memory()))
            .sum();
        tracing::info!(
            total_assigned_mb = self.total_assigned_resources.get(&ResourceKind::memory()) / (1024 * 1024),
            non_opportunistic_mb = non_opp_mem / (1024 * 1024),
            opportunistic_mb = opp_mem / (1024 * 1024),
            "initial assignments complete"
        );
    }
    pub(super) async fn try_assign_initial(&mut self, worker_id: WorkerId, factory: &mut impl WorkerFactory<M>) {
        let worker_info = self.pool.workers[worker_id as usize].budget_info();
        let max = self.max_resources();
        let decision = self.scheduler.assign_initial(
            &worker_info,
            &self.pending_binaries,
            &self.total_assigned_resources,
            max,
            &self.estimator,
        );

        match decision {
            AssignmentDecision::Assign {
                binary_index,
                estimated_usage,
                opportunistic,
                ..
            } => {
                let binary = self.pending_binaries.remove(binary_index);
                self.total_assigned_resources.add(&estimated_usage);
                let estimated_mb = estimated_usage.get(&ResourceKind::memory()) / (1024 * 1024);
                let name = binary.path.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();

                let worker = &mut self.pool.workers[worker_id as usize];
                match worker.assign_task(binary.clone(), estimated_usage.clone(), opportunistic).await {
                    Ok(()) => {
                        tracing::info!(
                            worker_id,
                            binary = %name,
                            estimated_mb,
                            opportunistic,
                            "initial assignment"
                        );
                        self.pool.workers[worker_id as usize].assignment_failure_count = 0;
                    }
                    Err(e) => {
                        // Put binary back and undo resource increment
                        self.pending_binaries.insert(0, binary);
                        self.total_assigned_resources.sub(&estimated_usage);
                        self.handle_assignment_failure(worker_id, &e, factory).await;
                    }
                }
            }
            AssignmentDecision::NoFit => {
                self.pool.workers[worker_id as usize].idle = true;
                self.pool.workers[worker_id as usize].has_initial_assignment = true;
            }
            AssignmentDecision::NoPendingTasks => {
                self.pool.workers[worker_id as usize].idle = true;
                self.pool.workers[worker_id as usize].has_initial_assignment = true;
            }
        }
    }

    // ── Phase 2: Main Phase ──
    pub(super) async fn run_main_phase(&mut self, factory: &mut impl WorkerFactory<M>) {
        tracing::info!("starting main phase");

        let mut active_workers: HashSet<WorkerId> =
            (0..self.config.num_workers).collect();

        self.process_worker_loop(&mut active_workers, false, true, ProcessingPhase::MainPhase, factory)
            .await;

        // Move remaining pending to unassigned
        if !self.pending_binaries.is_empty() {
            let remaining: Vec<TaskInfo<I>> = self.pending_binaries.drain(..).collect();
            self.unassigned_tasks.extend(remaining);
        }

        tracing::info!(
            completed = self.stats.completed,
            errored = self.failed_tasks.len(),
            resource_pressure = self.resource_pressure_tasks.len(),
            "main phase complete"
        );
    }

    // ── Phase 3: Retry Phase ──

    pub(super) async fn run_retry_phase(&mut self, factory: &mut impl WorkerFactory<M>) {
        if self.failed_tasks.is_empty() {
            tracing::info!("retry phase skipped - no failed tasks");
            return;
        }

        let max_passes = self.config.retry_max_attempts;
        tracing::info!(
            count = self.failed_tasks.len(),
            max_passes,
            "starting retry phase"
        );

        for pass in 0..max_passes {
            if self.failed_tasks.is_empty() {
                break;
            }

            let retry_tasks: Vec<FailedTask<I>> = self.failed_tasks.drain(..).collect();
            tracing::info!(
                pass = pass + 1,
                count = retry_tasks.len(),
                "retry pass"
            );
            for task in retry_tasks {
                self.pending_binaries.push(task.binary);
            }

            // Restart any stopped/dead workers before retry (matching Python behavior)
            for i in 0..self.config.num_workers {
                if self.pool.workers[i as usize].is_stopped()
                    || !self.pool.workers[i as usize].is_ready()
                {
                    tracing::info!(worker_id = i, "restarting worker for retry phase");
                    self.restart_worker(i, factory).await;
                    self.pending_worker_assignments.insert(i);
                }
            }

            let mut active_workers: HashSet<WorkerId> = (0..self.config.num_workers).collect();

            self.process_worker_loop(
                &mut active_workers,
                true,
                true,
                ProcessingPhase::RetryPhase,
                factory,
            )
            .await;
        }

        tracing::info!(
            completed = self.stats.completed,
            errored = self.failed_tasks.len(),
            resource_pressure = self.resource_pressure_tasks.len(),
            "retry phase complete"
        );
    }

    // ── Phase 4: Resource Pressure Phase ──

    pub(super) async fn run_resource_pressure_phase(&mut self, factory: &mut impl WorkerFactory<M>) {
        if self.resource_pressure_tasks.is_empty() {
            tracing::info!("resource pressure phase skipped - no pressure tasks");
            return;
        }

        tracing::info!(count = self.resource_pressure_tasks.len(), "starting resource pressure phase");

        self.in_pressure_phase = true;

        let pressure_tasks: Vec<FailedTask<I>> = self.resource_pressure_tasks.drain(..).collect();
        for task in pressure_tasks {
            self.pending_binaries.push(task.binary);
        }

        // Process with only worker 0
        let mut active_workers: HashSet<WorkerId> = HashSet::new();
        active_workers.insert(0);

        self.process_worker_loop(&mut active_workers, false, false, ProcessingPhase::ResourcePressurePhase, factory)
            .await;

        self.in_pressure_phase = false;

        tracing::info!(
            completed = self.stats.completed,
            errored = self.failed_tasks.len(),
            resource_pressure = self.resource_pressure_tasks.len(),
            "resource pressure phase complete"
        );
    }

    // ── Phase 5: Unassigned Phase ──

    pub(super) async fn run_unassigned_phase(&mut self, factory: &mut impl WorkerFactory<M>) {
        if self.unassigned_tasks.is_empty() {
            return;
        }

        tracing::info!(
            count = self.unassigned_tasks.len(),
            "starting unassigned phase"
        );

        // Sort by size (smallest first) matching Python behavior
        self.unassigned_tasks.sort_by_key(|b| b.size);

        let low_mem_threshold = self.config.low_resource_thresholds.get(&ResourceKind::memory());
        let mut kept = Vec::new();
        for task in self.unassigned_tasks.drain(..) {
            let free_mem = Self::get_free_system_memory();
            if free_mem > 0 && free_mem < low_mem_threshold {
                let name = task.path.file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_default();
                tracing::warn!(
                    binary = %name,
                    free_mb = free_mem / (1024 * 1024),
                    "skipping unassigned binary due to low system memory"
                );
                self.stats.skipped += 1;
                continue;
            }
            kept.push(task);
        }

        if kept.is_empty() {
            tracing::info!("all unassigned tasks skipped due to low memory");
            return;
        }

        for task in kept {
            self.pending_binaries.push(task);
        }

        let mut active_workers: HashSet<WorkerId> = HashSet::new();
        active_workers.insert(0);

        self.process_worker_loop(
            &mut active_workers,
            false,
            false,
            ProcessingPhase::UnassignedPhase,
            factory,
        )
        .await;
    }
}
