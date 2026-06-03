use std::collections::HashSet;
use std::time::{Duration, Instant};

use dynrunner_core::{
    ErrorType, FailedTask, Identifier, ResourceKind, ResourceMap, TaskInfo, TaskResult, WorkerId,
};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_scheduler_api::{ProcessingPhase, ResourceEstimator, Scheduler};

use crate::oom::{OomWatcher, classify_disconnect};
use crate::worker::WorkerEvent;

use super::{LocalManager, WorkerFactory};

/// Wall-clock window the disconnect reclassifier uses to correlate a
/// worker pipe-EOF with a kernel cgroup-OOM event. At the production
/// 50ms sample cadence this is ~10 samples of tolerance — long
/// enough to absorb scheduling jitter on either side of the EOF, short
/// enough to avoid attributing a later disconnect to an earlier
/// kernel-OOM that already killed a different worker.
const KERNEL_OOM_CORRELATION_WINDOW: Duration = Duration::from_millis(500);

impl<M: ManagerEndpoint + 'static, S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier>
    LocalManager<M, S, E, I>
{
    // Same justification as `handle_task_completed` below — every
    // argument either destructures one variant of `WorkerEvent` or
    // threads loop-scoped state (active_workers / phase / factory /
    // oom_watcher) that the per-event handlers reference exactly
    // once. Bundling into a struct just shifts the unpack frame.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn handle_event(
        &mut self,
        event: WorkerEvent<I>,
        active_workers: &mut HashSet<WorkerId>,
        allow_stop: bool,
        on_failure_increment_failed: bool,
        phase: ProcessingPhase,
        factory: &mut impl WorkerFactory<M>,
        oom_watcher: &OomWatcher,
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
                self.pool.workers[worker_id as usize]
                    .reclaim_protocol()
                    .await;
                self.pool.workers[worker_id as usize].clear_task();

                if result.success
                    && let Some(b) = binary.as_ref()
                {
                    // Populate the keyed-outputs cache BEFORE
                    // `handle_task_completed` — that helper releases
                    // dependents for dispatch via the scheduler, and
                    // `try_assign_normal` reads from this cache to
                    // assemble each dispatched task's
                    // `predecessor_outputs`. The cache write must
                    // happen first to avoid a dispatch-before-cache
                    // race. Decode contract mirrors the distributed
                    // `cluster_state/apply_tasks.rs` `TaskCompleted`
                    // arm: deserialise the Python worker's
                    // `DonePayload` wrapper and extract the inner
                    // `outputs` (the wrapper's `warnings`/`filtered`
                    // counters are not consumed in the local manager
                    // path either — serde drops them silently). A
                    // present-but-undecodable payload still inserts
                    // an empty entry so dependents can rely on key
                    // presence.
                    if let Some(bytes) = &result_data {
                        // The cache key is the predecessor's full
                        // `(phase_id, task_id)` identity; `task_id` is
                        // non-optional by the framework's boundary
                        // contract.
                        let key = (b.phase_id.clone(), b.task_id.clone());
                        match serde_json::from_slice::<dynrunner_core::DonePayload>(bytes) {
                            Ok(body) => {
                                self.task_outputs_cache.insert(key, body.outputs);
                            }
                            Err(e) => {
                                tracing::warn!(
                                    error = %e,
                                    task_id = %b.task_id,
                                    "TaskCompleted result_data failed to decode as DonePayload in local manager; \
                                     storing empty entry",
                                );
                                self.task_outputs_cache
                                    .insert(key, dynrunner_core::TaskOutputs::default());
                            }
                        }
                    }
                    self.task_payloads.push((b.clone(), result_data));
                }

                // Flush the per-task memprofile writer (if any).
                // Sampler hook is fire-and-forget mpsc-send; ordering
                // relative to `handle_task_completed` (which may
                // restart the worker subprocess and trigger the next
                // assignment) does not matter for correctness — the
                // sampler's command queue serialises events.
                if let Some(b) = binary.as_ref() {
                    // `task_id` is non-optional by the framework's
                    // boundary contract; clone the verbatim value.
                    self.notify_sampler_completed(b.task_id.clone());
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
                // Reap the subprocess BEFORE reclaim_protocol so the
                // exit status rides the same log line as the
                // disconnect. After restart_worker the factory may
                // drop the old `Child`; reaping then races that drop.
                // `try_reap_exit` is non-blocking; `None` carries the
                // operationally-useful "framework lost diagnostic
                // visibility" signal (vs. "definitely clean exit").
                let exit_status = self.pool.workers[worker_id as usize].try_reap_exit();

                // Reclaim protocol state from the spawned poll task
                self.pool.workers[worker_id as usize]
                    .reclaim_protocol()
                    .await;
                self.pool.workers[worker_id as usize].clear_task();

                tracing::warn!(
                    worker_id,
                    error = ?result.error_message,
                    exit_status = exit_status.as_ref().map(|s| s.to_string()),
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

                // Disconnect reclassifier — upgrade the protocol's
                // default `Recoverable` synthesis when the kernel-OOM
                // watcher saw an `oom_kill` increment on the workers
                // subgroup, or downgrade to `NonRecoverable` for
                // deterministic-bug signals (SIGSEGV, etc.). See
                // [`crate::oom::disconnect`].
                let kernel_oom_recent =
                    oom_watcher.kernel_oom_recent(KERNEL_OOM_CORRELATION_WINDOW);
                let reclassified_type = classify_disconnect(
                    result.error_type.clone().unwrap_or(ErrorType::Recoverable),
                    exit_status.as_ref(),
                    kernel_oom_recent,
                );
                let result = TaskResult {
                    error_type: Some(reclassified_type),
                    ..result
                };

                self.record_result(&result, binary.as_ref());

                if on_failure_increment_failed {
                    self.stats.errored += 1;
                }

                // Flush any per-task memprofile writers attached to
                // this worker BEFORE `restart_worker` replaces the
                // slot. Restart drops the prior `WorkerHandle`, whose
                // `SubcgroupHandle::Drop` best-effort rmdirs the
                // per-worker cgroup leaf — if the sampler ran after
                // the rmdir, its last tick would fail to read
                // `memory.current`. No matching `TaskCompleted` will
                // arrive on a transport disconnect; this is the only
                // flush opportunity.
                self.notify_sampler_disconnected(worker_id);

                // Restart worker and keep it in the active set (matching Python's
                // _handle_monitor_result which restarts on NonRecoverable errors)
                tracing::info!(
                    worker_id,
                    "restarting worker after disconnect/non-recoverable error"
                );
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

    // Internal event handler — every arg comes from the destructured
    // `WorkerEvent::TaskCompleted` variant or its immediate context;
    // bundling them into a config struct would just move the unpack
    // one frame out.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn handle_task_completed(
        &mut self,
        worker_id: WorkerId,
        result: TaskResult,
        binary: Option<TaskInfo<I>>,
        estimated_resources: ResourceMap,
        active_workers: &mut HashSet<WorkerId>,
        allow_stop: bool,
        on_failure_increment_failed: bool,
        _phase: ProcessingPhase,
        factory: &mut impl WorkerFactory<M>,
    ) {
        // Log resource usage before recording result (capture actual before clearing)
        let actual_usage = self.pool.workers[worker_id as usize].actual_usage.clone();
        self.log_resource_usage(
            binary.as_ref(),
            &estimated_resources,
            &actual_usage,
            !result.success,
        );

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

        // Restart worker after successful completion unless the coarse
        // `reuse_workers` opt-in keeps the slot — the optional
        // restart_predicate still forces a restart even when reusing;
        // only when there's still pending work.
        if result.success && !self.pool_ref().is_empty() {
            let predicate_says_restart = if let (Some(predicate), Some(b)) =
                (self.config.restart_predicate.as_ref(), binary.as_ref())
            {
                let ctx = super::RestartContext {
                    success: true,
                    binary_path: &b.path,
                    binary_size: b.size,
                    estimated_resources: &estimated_resources,
                    actual_resources: &actual_usage,
                };
                predicate(&ctx)
            } else {
                false
            };
            if !self.config.reuse_workers || predicate_says_restart {
                tracing::info!(worker_id, "restarting worker after successful completion");
                self.restart_worker(worker_id, factory).await;
                self.pending_worker_assignments.insert(worker_id);
                return;
            }
        }

        // Try to assign next task
        if !self.pool_ref().is_empty() {
            self.try_assign_normal(worker_id, factory).await;
        }

        // If still no task and no pending, remove from active
        if self.pool.workers[worker_id as usize]
            .current_binary
            .is_none()
            && self.pool_ref().is_empty()
        {
            if allow_stop {
                tracing::info!(worker_id, "stopping (no more tasks after completion)");
            }
            active_workers.remove(&worker_id);
        }
    }

    /// Log resource usage to memuse.log in CSV format: size,estimated_mem,actual_mem,filename,status.
    /// Thin wrapper that forwards to the crate-level shared writer
    /// in [`crate::memuse::log_resource_usage`]; the secondary
    /// coordinator calls the same primitive from its own
    /// `TaskCompleted` handler so both dispatch paths produce
    /// identical row shapes.
    pub(super) fn log_resource_usage(
        &self,
        binary: Option<&TaskInfo<I>>,
        estimated: &ResourceMap,
        actual: &ResourceMap,
        errored: bool,
    ) {
        let Some(log_path) = self.config.memuse_log_path.as_deref() else {
            return;
        };
        crate::memuse::log_resource_usage(log_path, binary, estimated, actual, errored);
    }

    pub(super) fn record_result(&mut self, result: &TaskResult, binary: Option<&TaskInfo<I>>) {
        // Update the per-phase counter and notify the pool the item is no
        // longer in-flight. The post-event `process_drain_transitions`
        // call in the worker loop surfaces this through `on_phase_end`
        // (deferred when the item lands in a side queue; see
        // `LocalManager::process_drain_transitions`).
        if let Some(b) = binary {
            // `task_id` is non-optional per the framework's boundary
            // contract; pass the borrowed view to the pool helper
            // (which still accepts `Option<&str>` because passing
            // `None` is the documented "transient-failure" signal —
            // a separate semantic concern from "task carried no id").
            self.record_phase_completion(&b.phase_id, result.success, Some(b.task_id.as_str()));
        }
        if result.success {
            self.stats.completed += 1;
        } else {
            match &result.error_type {
                Some(ErrorType::ResourceExhausted(kind)) if kind.as_str() == "memory" => {
                    if let Some(binary) = binary {
                        self.resource_pressure_tasks.push(FailedTask {
                            binary: binary.clone(),
                            error_type: ErrorType::ResourceExhausted(ResourceKind::memory()),
                            error_message: result.error_message.clone().unwrap_or_default(),
                            retry_count: 0,
                        });
                    }
                }
                _ => {
                    if let Some(binary) = binary {
                        self.failed_tasks.push(FailedTask {
                            binary: binary.clone(),
                            error_type: result.error_type.clone().unwrap_or(ErrorType::Recoverable),
                            error_message: result.error_message.clone().unwrap_or_default(),
                            retry_count: 0,
                        });
                    }
                }
            }
        }
    }
}
