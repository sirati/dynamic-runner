use std::collections::HashSet;

use dynrunner_core::{
    ErrorType, FailedTask, Identifier, ResourceKind, TaskInfo, WorkerId, gather_predecessor_outputs,
};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_scheduler_api::{AssignmentDecision, ProcessingPhase, ResourceEstimator, Scheduler};

use super::{LocalManager, WorkerFactory};

impl<M: ManagerEndpoint + 'static, S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier>
    LocalManager<M, S, E, I>
{
    pub(super) async fn run_initial_assignments(&mut self, factory: &mut impl WorkerFactory<M>) {
        tracing::info!("starting initial assignment phase");

        loop {
            let all_assigned = self.pool.workers.iter().all(|w| w.has_initial_assignment);
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
            .pool
            .workers
            .iter()
            .filter(|w| w.opportunistic && w.current_binary.is_some())
            .map(|w| w.estimated_resources.get(&ResourceKind::memory()))
            .sum();
        let non_opp_mem: u64 = self
            .pool
            .workers
            .iter()
            .filter(|w| !w.opportunistic && w.current_binary.is_some())
            .map(|w| w.estimated_resources.get(&ResourceKind::memory()))
            .sum();
        tracing::info!(
            total_assigned_mb =
                self.total_assigned_resources.get(&ResourceKind::memory()) / (1024 * 1024),
            non_opportunistic_mb = non_opp_mem / (1024 * 1024),
            opportunistic_mb = opp_mem / (1024 * 1024),
            "initial assignments complete"
        );
    }
    pub(super) async fn try_assign_initial(
        &mut self,
        worker_id: WorkerId,
        factory: &mut impl WorkerFactory<M>,
    ) {
        let worker_info = self.pool.workers[worker_id as usize].budget_info();
        let max = self.max_resources();
        let view = self.pool_ref().view_for_worker(worker_id, None);
        let decision = self.scheduler.assign_initial(
            &worker_info,
            view.as_slice(),
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
                // Owned consumption ticket — the view's last use,
                // releasing the pool borrow for the take below.
                let selection = view.select(binary_index);
                // Pop-time readiness re-check (#652 D.1): a not-ready item is
                // re-blocked and yields `None`. The local manager never pushes
                // a not-ready item to a bucket head (no reconcile arm — that is
                // distributed-primary-only), so this is always `Some` here; the
                // guard is the type contract, returning a no-op if it ever fired.
                let Some(binary) = self.pool_mut().take_selected(selection) else {
                    return;
                };
                self.total_assigned_resources.add(&estimated_usage);
                let estimated_mb = estimated_usage.get(&ResourceKind::memory()) / (1024 * 1024);
                let name = binary
                    .path
                    .file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_default();

                // Per-type subprocess dispatch: see worker_loop's
                // `try_assign_normal` for the full rationale. Initial
                // assignment binds each freshly-spawned worker to the
                // first task's `type_id`; the same-type fast path
                // covers every subsequent assignment to that slot
                // until a `type_id` shift.
                // Initial-assignment dispatches at run start, before
                // any task has completed, so `task_outputs_cache` is
                // empty; the gather returns either an empty map (no
                // deps) or present-but-empty entries (deps that have
                // not produced outputs yet). Uniform shape with
                // `try_assign_normal` so the contract is identical
                // across both local-mode dispatch sites.
                let predecessor_outputs = gather_predecessor_outputs(
                    &binary.task_depends_on,
                    |phase_id, task_id| {
                        self.task_outputs_cache
                            .get(&(phase_id.clone(), task_id.to_string()))
                            .cloned()
                    },
                    |phase_id, task_id| {
                        self.task_payloads
                            .iter()
                            .find(|(t, _)| t.task_id == task_id && &t.phase_id == phase_id)
                            .map(|(t, _)| t.task_depends_on.clone())
                    },
                );
                let print_pid = self.config.print_pid;
                if let Err(e) = self
                    .pool
                    .ensure_worker_for_type(worker_id, &binary.type_id, factory, print_pid)
                    .await
                {
                    self.pool_mut().requeue(binary);
                    self.total_assigned_resources.sub(&estimated_usage);
                    self.handle_assignment_failure(worker_id, &e, factory).await;
                    return;
                }
                let worker = &mut self.pool.workers[worker_id as usize];
                match worker
                    .assign_task(
                        crate::manager::own_task(binary.clone()),
                        estimated_usage.clone(),
                        opportunistic,
                        predecessor_outputs,
                    )
                    .await
                {
                    Ok(()) => {
                        self.notify_sampler_assigned(worker_id, &binary);
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
                        // Put binary back at the front of its bucket and undo
                        // resource increment. Item was in-flight (take_selected
                        // bumped in-flight) — `requeue` decrements it again.
                        self.pool_mut().requeue(binary);
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

        let mut active_workers: HashSet<WorkerId> = (0..self.config.num_workers).collect();

        self.process_worker_loop(
            &mut active_workers,
            false,
            true,
            ProcessingPhase::MainPhase,
            factory,
        )
        .await;

        // Move any items still queued in the pool into `unassigned_tasks`.
        // These are tasks the scheduler couldn't fit during the main phase
        // (NoFit decisions across all idle attempts).
        if !self.pool_ref().is_empty() {
            let remaining = self.pool_mut().drain_queued();
            self.unassigned_tasks
                .extend(remaining.into_iter().map(crate::manager::own_task));
        }

        tracing::info!(
            completed = self.stats.completed,
            errored = self.failed_tasks.len(),
            resource_pressure = self.resource_pressure_tasks.len(),
            unassigned = self.unassigned_tasks.len(),
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

            // `InvalidTask` is a TERMINAL, non-reinjectable failure
            // (missing dep / duplicate id — no future state makes the
            // task runnable). It must NOT be retried: partition it out
            // and leave it in `failed_tasks` so it stays the run's
            // surfaced terminal result. Every other class in
            // `failed_tasks` is retry-eligible and drained for
            // reinjection. (Mirrors the distributed side's exclusion of
            // `InvalidTask` from the reinject gate / terminal lockout.)
            let (retry_tasks, terminal_invalid): (Vec<FailedTask<I>>, Vec<FailedTask<I>>) = self
                .failed_tasks
                .drain(..)
                .partition(|t| !matches!(t.error_type, ErrorType::InvalidTask { .. }));
            self.failed_tasks = terminal_invalid;
            if retry_tasks.is_empty() {
                // Nothing retry-eligible left (only terminal
                // invalid_tasks remain) — stop the passes.
                break;
            }
            tracing::info!(pass = pass + 1, count = retry_tasks.len(), "retry pass");
            for task in retry_tasks {
                // Re-inject preserves in-flight counts (these tasks were
                // never `on_item_finished`'d) and reactivates the phase
                // if it had drained or was draining.
                self.pool_mut().reinject(std::sync::Arc::new(task.binary));
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

    pub(super) async fn run_resource_pressure_phase(
        &mut self,
        factory: &mut impl WorkerFactory<M>,
    ) {
        if self.resource_pressure_tasks.is_empty() {
            tracing::info!("resource pressure phase skipped - no pressure tasks");
            return;
        }

        tracing::info!(
            count = self.resource_pressure_tasks.len(),
            "starting resource pressure phase"
        );

        self.in_pressure_phase = true;

        // Worker 0 is the only active worker for the rest of the run
        // (this phase + the unassigned phase). Its scheduling cap is the
        // full cluster pool, not the per-worker fraction it received at
        // startup — `assign_normal` would otherwise refuse oversized
        // tasks here too, and they'd silently drop out.
        self.boost_worker0_budget_to_max();

        let pressure_tasks: Vec<FailedTask<I>> = self.resource_pressure_tasks.drain(..).collect();
        for task in pressure_tasks {
            self.pool_mut().reinject(std::sync::Arc::new(task.binary));
        }

        // Process with only worker 0
        let mut active_workers: HashSet<WorkerId> = HashSet::new();
        active_workers.insert(0);

        self.process_worker_loop(
            &mut active_workers,
            false,
            false,
            ProcessingPhase::ResourcePressurePhase,
            factory,
        )
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

        let low_mem_threshold = self
            .config
            .low_resource_thresholds
            .get(&ResourceKind::memory());
        let mut kept = Vec::new();
        for task in self.unassigned_tasks.drain(..) {
            let free_mem = Self::get_free_system_memory();
            if free_mem > 0 && free_mem < low_mem_threshold {
                let name = task
                    .path
                    .file_name()
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

        // The unassigned phase exists exactly to catch tasks whose
        // estimator value exceeded any single worker's per-worker share.
        // Worker 0 is the only active worker, so its scheduling cap is
        // the full cluster pool. Without this boost, `assign_normal`
        // refuses every oversized task and they drop silently — the
        // class of "1291/1386 tasks never ran" silent-data-loss bug.
        self.boost_worker0_budget_to_max();

        for task in kept {
            self.pool_mut().reinject(std::sync::Arc::new(task));
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

        // Anything still queued is genuinely unfittable: estimator
        // value exceeds even the boosted (cluster-wide) cap. Surface
        // each one as a permanent failure with the offending estimate
        // vs. the cluster cap so the user can fix the estimator or
        // grow the cluster — never drop silently.
        if !self.pool_ref().is_empty() {
            let max = self.max_resources().clone();
            let max_mb = max.get(&ResourceKind::memory()) / (1024 * 1024);
            let remaining: Vec<TaskInfo<I>> = self
                .pool_mut()
                .drain_queued()
                .into_iter()
                .map(crate::manager::own_task)
                .collect();
            tracing::error!(
                count = remaining.len(),
                cluster_max_mb = max_mb,
                "unassigned phase: tasks exceed cluster-wide resource pool; \
                 reporting as permanent failures (likely estimator overshoot \
                 or cluster sized too small for this workload)"
            );
            for binary in remaining {
                let estimated = self.estimator.estimate(&binary);
                let est_mb = estimated.get(&ResourceKind::memory()) / (1024 * 1024);
                let name = binary
                    .path
                    .file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_default();
                tracing::error!(
                    binary = %name,
                    estimated_mb = est_mb,
                    cluster_max_mb = max_mb,
                    "unfittable task: estimator value exceeds cluster pool"
                );
                self.failed_tasks.push(FailedTask {
                    binary,
                    error_type: ErrorType::ResourceExhausted(ResourceKind::memory()),
                    error_message: format!(
                        "estimator value {est_mb} MB exceeds cluster-wide pool {max_mb} MB; \
                         task cannot run on this cluster"
                    ),
                    retry_count: 0,
                });
            }
        }
    }

    /// Boost worker 0's `reserved_budgets` to the full cluster pool.
    ///
    /// Worker 0 is the only active worker during the resource-pressure
    /// and unassigned phases. The reserved-budget value it received at
    /// startup was `max / num_workers` — a sensible cap for parallel
    /// scheduling, but the wrong cap when it owns the cluster alone.
    /// Without this boost, `Scheduler::assign_normal` rejects any task
    /// whose estimate exceeds the per-worker fraction even though the
    /// global pool would fit it; the rejected tasks silently drop out
    /// of the run (the class-of-bug seq=15 reported).
    ///
    /// Idempotent. Workers stop after the unassigned phase, so the
    /// mutation is never observed by parallel-scheduling phases.
    fn boost_worker0_budget_to_max(&mut self) {
        if self.pool.workers.is_empty() {
            return;
        }
        let max = self.max_resources().clone();
        self.pool.workers[0].reserved_budgets = max;
    }
}
