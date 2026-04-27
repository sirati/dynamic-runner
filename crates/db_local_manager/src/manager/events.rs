use std::collections::HashSet;
use std::time::{Duration, Instant};

use db_comm_api_base::{
    BinaryInfo, ErrorType, FailedTask, Identifier, ResourceKind, ResourceMap, TaskResult, WorkerId,
};
use db_manager_runner_comm::ManagerEndpoint;
use db_scheduler_api::{
    AssignmentDecision, ResourceEstimator, ProcessingPhase, Scheduler,
};

use crate::pool::ResourcePressureResult;
use crate::worker::WorkerEvent;

use super::{LocalManager, WorkerFactory};

impl<M: ManagerEndpoint + 'static, S: Scheduler<I>, E: ResourceEstimator, I: Identifier> LocalManager<M, S, E, I> {
    pub(super) async fn handle_event(
        &mut self,
        event: WorkerEvent<I>,
        active_workers: &mut HashSet<WorkerId>,
        allow_stop: bool,
        on_failure_increment_failed: bool,
        phase: ProcessingPhase,
        factory: &mut impl WorkerFactory<M>,
    ) {
        match event {
            WorkerEvent::TaskCompleted {
                worker_id,
                result,
                result_data,
                binary,
                estimated_resources,
            } => {
                // Reclaim protocol state from the spawned poll task
                self.pool.workers[worker_id as usize].reclaim_protocol().await;
                self.pool.workers[worker_id as usize].clear_task();

                if result.success {
                    if let Some(b) = binary.as_ref() {
                        self.task_payloads.push((b.clone(), result_data));
                    }
                }

                self.handle_task_completed(
                    worker_id,
                    result,
                    binary,
                    estimated_resources,
                    active_workers,
                    allow_stop,
                    on_failure_increment_failed,
                    phase,
                    factory,
                )
                .await;
            }
            WorkerEvent::Disconnected {
                worker_id,
                result,
                binary,
            } => {
                // Reclaim protocol state from the spawned poll task
                self.pool.workers[worker_id as usize].reclaim_protocol().await;
                self.pool.workers[worker_id as usize].clear_task();

                tracing::warn!(
                    worker_id,
                    error = ?result.error_message,
                    "worker disconnected"
                );
                // Log resource usage before recording result
                let actual_usage = self.pool.workers[worker_id as usize].actual_usage.clone();
                self.log_resource_usage(binary.as_ref(), &ResourceMap::new(), &actual_usage, true);

                // Release estimated resources (matching handle_task_completed logic)
                let worker = &self.pool.workers[worker_id as usize];
                if worker.has_initial_assignment && !worker.opportunistic {
                    let est = worker.estimated_resources.clone();
                    self.total_assigned_resources.sub(&est);
                }

                self.record_result(&result, binary.as_ref());

                if on_failure_increment_failed {
                    self.stats.errored += 1;
                }

                // Restart worker and keep it in the active set (matching Python's
                // _handle_monitor_result which restarts on NonRecoverable errors)
                tracing::info!(worker_id, "restarting worker after disconnect/non-recoverable error");
                self.restart_worker(worker_id, factory).await;
                self.pending_worker_assignments.insert(worker_id);
            }
            WorkerEvent::Ready { worker_id } => {
                tracing::info!(worker_id, "worker became ready");
                self.pending_worker_assignments.remove(&worker_id);
            }
            WorkerEvent::PhaseUpdate {
                worker_id,
                phase_name,
            } => {
                tracing::debug!(worker_id, phase = %phase_name, "phase update");
                let worker = &mut self.pool.workers[worker_id as usize];
                worker.phase = Some(phase_name);
                worker.last_keepalive = Some(Instant::now());
                worker.phase_started_at = Some(Instant::now());
                worker.phase_status_log_idx = 0;
            }
            WorkerEvent::Keepalive { worker_id } => {
                tracing::trace!(worker_id, "keepalive");
                self.pool.workers[worker_id as usize].last_keepalive = Some(Instant::now());
            }
        }
    }

    pub(super) async fn handle_task_completed(
        &mut self,
        worker_id: WorkerId,
        result: TaskResult,
        binary: Option<BinaryInfo<I>>,
        estimated_resources: ResourceMap,
        active_workers: &mut HashSet<WorkerId>,
        allow_stop: bool,
        on_failure_increment_failed: bool,
        _phase: ProcessingPhase,
        factory: &mut impl WorkerFactory<M>,
    ) {
        // Log resource usage before recording result (capture actual before clearing)
        let actual_usage = self.pool.workers[worker_id as usize].actual_usage.clone();
        self.log_resource_usage(binary.as_ref(), &estimated_resources, &actual_usage, !result.success);

        // Release estimated resources from total (only for non-opportunistic workers
        // that received an initial assignment, matching Python behavior)
        let worker = &self.pool.workers[worker_id as usize];
        if worker.has_initial_assignment && !worker.opportunistic {
            self.total_assigned_resources.sub(&estimated_resources);
        }

        self.record_result(&result, binary.as_ref());

        if on_failure_increment_failed && !result.success {
            self.stats.errored += 1;
        }

        // Restart worker after successful completion if always_restart_worker
        // is enabled and there are still binaries to process
        if self.config.always_restart_worker && result.success && !self.pending_binaries.is_empty() {
            tracing::info!(worker_id, "restarting worker after successful completion (always_restart_worker)");
            self.restart_worker(worker_id, factory).await;
            self.pending_worker_assignments.insert(worker_id);
            return;
        }

        // Try to assign next task
        if !self.pending_binaries.is_empty() {
            self.try_assign_normal(worker_id, factory).await;
        }

        // If still no task and no pending, remove from active
        if self.pool.workers[worker_id as usize].current_binary.is_none()
            && self.pending_binaries.is_empty()
        {
            if allow_stop {
                tracing::info!(worker_id, "stopping (no more tasks after completion)");
            }
            active_workers.remove(&worker_id);
        }
    }

    /// Log resource usage to memuse.log in CSV format: size,estimated_mem,actual_mem,filename,status
    pub(super) fn log_resource_usage(
        &self,
        binary: Option<&BinaryInfo<I>>,
        estimated: &ResourceMap,
        actual: &ResourceMap,
        errored: bool,
    ) {
        let log_path = match &self.config.memuse_log_path {
            Some(p) => p,
            None => return,
        };
        let binary = match binary {
            Some(b) => b,
            None => return,
        };

        let status = if errored { "ERROR" } else { "OK" };
        let filename = binary
            .path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();

        let estimated_mem = estimated.get(&ResourceKind::memory());
        let actual_mem = actual.get(&ResourceKind::memory());

        use std::io::Write;
        match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_path)
        {
            Ok(mut f) => {
                // Format: size,estimated,actual,filename,status
                let _ = writeln!(f, "{},{},{},{},{}", binary.size, estimated_mem, actual_mem, filename, status);
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to write memuse log");
            }
        }
    }

    pub(super) fn record_result(&mut self, result: &TaskResult, binary: Option<&BinaryInfo<I>>) {
        if result.success {
            self.stats.completed += 1;
        } else {
            match &result.error_type {
                Some(ErrorType::ResourceExhausted(kind)) if kind.as_str() == "memory" => {
                    if let Some(binary) = binary {
                        self.resource_pressure_tasks.push(FailedTask {
                            binary: binary.clone(),
                            error_type: ErrorType::ResourceExhausted(ResourceKind::memory()),
                            error_message: result
                                .error_message
                                .clone()
                                .unwrap_or_default(),
                            retry_count: 0,
                        });
                    }
                }
                _ => {
                    if let Some(binary) = binary {
                        self.failed_tasks.push(FailedTask {
                            binary: binary.clone(),
                            error_type: result
                                .error_type
                                .clone()
                                .unwrap_or(ErrorType::Recoverable),
                            error_message: result
                                .error_message
                                .clone()
                                .unwrap_or_default(),
                            retry_count: 0,
                        });
                    }
                }
            }
        }
    }
}
