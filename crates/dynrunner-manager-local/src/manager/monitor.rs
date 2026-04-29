use std::collections::HashSet;
use std::time::Instant;

use dynrunner_core::{
    ErrorType, FailedTask, Identifier, ResourceKind, TaskResult, WorkerId,
};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_scheduler_api::{
    ResourceEstimator, Scheduler,
};

use crate::pool::ResourcePressureResult;

use super::{LocalManager, WorkerFactory};

impl<M: ManagerEndpoint + 'static, S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> LocalManager<M, S, E, I> {
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
            if let (Some(phase), Some(last_keepalive)) = (&worker.phase, worker.last_keepalive) {
                if let Some(timeout) = self.config.stage_timeouts.get(phase) {
                    if last_keepalive.elapsed() > *timeout {
                        timed_out.push((worker.worker_id, phase.clone()));
                    }
                }
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
            let estimated = self.pool.workers[worker_id as usize].estimated_resources.clone();

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

    pub(super) fn check_resource_pressure(&mut self) {
        let max = self.config.max_resources.clone();
        match self.pool.check_resource_pressure(&self.scheduler, &max, self.in_pressure_phase) {
            ResourcePressureResult::Killed {
                worker_id,
                binary,
                reason,
            } => {
                if let Some(binary) = binary {
                    if worker_id == 0 {
                        // Worker 0 is the last resort — if it can't fit, the task
                        // is truly OOM and goes to the resource_pressure_tasks queue.
                        // This happens even during OOM phase (matching Python).
                        self.resource_pressure_tasks.push(FailedTask {
                            binary,
                            error_type: ErrorType::ResourceExhausted(ResourceKind::memory()),
                            error_message: reason,
                            retry_count: 0,
                        });
                    } else if !self.in_pressure_phase {
                        // Other workers: requeue for local retry.
                        // During OOM phase, Python skips _handle_oom_killed_task
                        // (which does the requeue), so we also skip requeuing.
                        self.pending_binaries.insert(0, binary);
                    }
                    // During OOM phase for non-worker-0: task is dropped (not requeued)
                    // matching Python's behavior where _handle_oom_killed_task is skipped.
                }
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
                        if let Some(kb_str) = rest.strip_suffix("kB").or_else(|| rest.strip_suffix(" kB")) {
                            if let Ok(kb) = kb_str.trim().parse::<u64>() {
                                return kb * 1024;
                            }
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
