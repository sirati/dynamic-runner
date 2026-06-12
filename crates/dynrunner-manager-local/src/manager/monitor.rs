use std::collections::HashSet;
use std::time::Instant;

use dynrunner_core::{ErrorType, FailedTask, Identifier, ResourceKind, TaskResult, WorkerId};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_scheduler_api::{KillReason, ResourceEstimator, Scheduler};

use crate::oom::OomWatcher;
use crate::pool::ResourcePressureResult;

use super::{LocalManager, WorkerFactory};

impl<M: ManagerEndpoint + 'static, S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier>
    LocalManager<M, S, E, I>
{
    pub(super) async fn check_timeouts(
        &mut self,
        _active_workers: &mut HashSet<WorkerId>,
        on_failure_increment_failed: bool,
        factory: &mut impl WorkerFactory<M>,
    ) {
        if self.config.stage_timeouts.is_empty() {
            return;
        }

        let mut timed_out = Vec::new();
        for worker in &self.pool.workers {
            if let (Some(phase), Some(last_keepalive)) = (&worker.phase, worker.last_keepalive)
                && let Some(timeout) = self.config.stage_timeouts.get(phase)
                && last_keepalive.elapsed() > *timeout
            {
                timed_out.push((worker.worker_id, phase.clone()));
            }
        }

        for (worker_id, phase) in timed_out {
            let binary_name = self.pool.workers[worker_id as usize]
                .current_binary
                .as_ref()
                .and_then(|b| b.path.file_name())
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_else(|| "unknown".into());

            tracing::warn!(
                worker_id,
                phase = %phase,
                binary = %binary_name,
                "worker timed out"
            );

            // Record as recoverable error
            let binary = self.pool.workers[worker_id as usize].current_binary.clone();
            let actual_usage = self.pool.workers[worker_id as usize].actual_usage.clone();
            let estimated = self.pool.workers[worker_id as usize]
                .estimated_resources
                .clone();

            self.log_resource_usage(binary.as_ref(), &estimated, &actual_usage, true);

            let result = TaskResult::error(
                ErrorType::Recoverable,
                format!("Worker timeout in phase {phase}"),
            );
            self.record_result(&result, binary.as_ref());

            if on_failure_increment_failed {
                self.stats.errored += 1;
            }

            // Release estimated resources
            let worker = &self.pool.workers[worker_id as usize];
            if worker.has_initial_assignment && !worker.opportunistic {
                self.total_assigned_resources.sub(&estimated);
            }

            // Flush the in-flight memprofile BEFORE restart_worker drops
            // the SubcgroupHandle (rmdir of the leaf): this is a third
            // worker-kill path that bypasses the events.rs Disconnected
            // route, so the sampler needs an explicit nudge.
            self.notify_sampler_disconnected(worker_id);

            // Restart the worker
            self.restart_worker(worker_id, factory).await;
            self.pending_worker_assignments.insert(worker_id);
        }
    }

    /// Walk all workers and emit a status log for any that has been in their
    /// current phase longer than the next configured interval. Each worker
    /// fires at most once per interval until it transitions phases.
    pub(super) fn report_stuck_workers(&mut self) {
        if self.config.phase_status_log_intervals.is_empty() {
            return;
        }
        let intervals = &self.config.phase_status_log_intervals;
        let now = Instant::now();
        for worker in &mut self.pool.workers {
            let Some(started_at) = worker.phase_started_at else {
                continue;
            };
            let elapsed = now.duration_since(started_at);
            while worker.phase_status_log_idx < intervals.len()
                && elapsed >= intervals[worker.phase_status_log_idx]
            {
                let phase = worker.phase.as_deref().unwrap_or("(unknown)");
                let task = worker
                    .current_binary
                    .as_ref()
                    .map(|b| b.path.display().to_string())
                    .unwrap_or_else(|| "(no task)".into());
                tracing::warn!(
                    worker_id = worker.worker_id,
                    phase,
                    elapsed_s = elapsed.as_secs_f64(),
                    task = %task,
                    "worker has been in the same phase for {:.0}s",
                    elapsed.as_secs_f64()
                );
                worker.phase_status_log_idx += 1;
            }
        }
    }

    /// Route the resource-pressure decision tick through the OOM
    /// watcher. The watcher invokes
    /// `WorkerPool::check_resource_pressure` itself (so kill events
    /// can be recorded for the structured-log trigger) and returns
    /// the verdict; the per-mode kill-outcome handling stays here in
    /// monitor.rs, untouched from the pre-extraction shape.
    pub(super) fn check_resource_pressure_via_watcher(&mut self, watcher: &mut OomWatcher) {
        let max = self.config.max_resources.clone();
        let result = watcher.on_decision(
            &mut self.pool,
            &self.scheduler,
            &max,
            self.in_pressure_phase,
        );
        self.handle_resource_pressure_result(result);
    }

    /// Caller-specific outcome handler for a [`ResourcePressureResult`].
    ///
    /// Routing is keyed on [`KillReason`], not worker id. The previous
    /// shape special-cased `worker_id == 0` because worker 0 holds the
    /// largest reserved budget (see [`Scheduler::initial_budget`]) so a
    /// kill there was a proxy for "no smaller candidate, truly OOM";
    /// `KillReason::OomLastResort` now carries that signal directly, and
    /// `worker_id` is no longer a routing input.
    ///
    /// Four buckets:
    ///   * `NoFaultMemoryStealing` / `NoFaultUnderBudget` — requeue at
    ///     the pool front, no `resource_pressure_tasks` entry. The task
    ///     is innocent; retry budget is preserved. Runs in both normal
    ///     and pressure phases because the task may still fit elsewhere.
    ///   * `OomLastResort` — push to `resource_pressure_tasks` even in
    ///     pressure phase (last-resort always retries via that channel).
    ///   * `OomOverBudget` — push to `resource_pressure_tasks` in normal
    ///     phase; drop in pressure phase (matches Python's skip of
    ///     `_handle_oom_killed_task` for non-last-resort kills during
    ///     OOM phase, which would otherwise loop forever).
    pub(super) fn handle_resource_pressure_result(&mut self, result: ResourcePressureResult<I>) {
        match result {
            ResourcePressureResult::Killed {
                worker_id: _,
                binary,
                reason,
            } => {
                let Some(binary) = binary else { return };
                // `binary` is `Box<TaskInfo<I>>` (boxed in
                // `ResourcePressureResult::Killed` to shrink the
                // variant). Field access auto-derefs through the box;
                // we unbox at the FailedTask / requeue boundary where
                // the by-value `TaskInfo<I>` is required.
                if reason.is_no_fault() {
                    // Pool's `requeue` decrements in-flight (set by
                    // `take_selected`) and pushes to the bucket front.
                    self.pool_mut().requeue(*binary);
                    return;
                }
                // At-fault kill (OomOverBudget / OomLastResort).
                let drop_in_pressure_phase =
                    self.in_pressure_phase && reason == KillReason::OomOverBudget;
                if drop_in_pressure_phase {
                    self.record_phase_completion(
                        &binary.phase_id,
                        false,
                        Some(binary.task_id.as_str()),
                    );
                    return;
                }
                self.record_phase_completion(
                    &binary.phase_id,
                    false,
                    Some(binary.task_id.as_str()),
                );
                self.resource_pressure_tasks.push(FailedTask {
                    binary: *binary,
                    error_type: ErrorType::ResourceExhausted(ResourceKind::memory()),
                    error_message: reason.as_str().into(),
                    retry_count: 0,
                });
            }
            ResourcePressureResult::NoAction => {}
        }
    }

    /// Read free system memory from /proc/meminfo (Linux only).
    /// Returns 0 on non-Linux or if the file can't be read.
    pub(super) fn get_free_system_memory() -> u64 {
        #[cfg(target_os = "linux")]
        {
            if let Ok(contents) = std::fs::read_to_string("/proc/meminfo") {
                for line in contents.lines() {
                    if let Some(rest) = line.strip_prefix("MemAvailable:") {
                        let rest = rest.trim();
                        if let Some(kb_str) =
                            rest.strip_suffix("kB").or_else(|| rest.strip_suffix(" kB"))
                            && let Ok(kb) = kb_str.trim().parse::<u64>()
                        {
                            return kb * 1024;
                        }
                    }
                }
            }
            0
        }
        #[cfg(not(target_os = "linux"))]
        {
            0
        }
    }

    pub(super) async fn stop_all_workers(&mut self) {
        self.pool.stop_all().await;
    }
}
