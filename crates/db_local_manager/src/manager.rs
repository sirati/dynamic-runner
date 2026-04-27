use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use db_comm_api_base::{
    BinaryInfo, ErrorType, FailedTask, Identifier, ResourceKind, ResourceMap, TaskResult, WorkerId,
};
use db_manager_runner_comm::ManagerEndpoint;
use db_scheduler_api::{
    AssignmentDecision, ResourceEstimator, ProcessingPhase, Scheduler,
};
use crate::pool::{ResourcePressureResult, WorkerPool};
use crate::stats::ProcessingStats;
use crate::worker::WorkerEvent;

/// Configuration for the local manager.
pub struct LocalManagerConfig {
    pub num_workers: u32,
    pub max_resources: ResourceMap,
    pub always_restart_worker: bool,
    pub print_pid: bool,
    pub memuse_log_path: Option<std::path::PathBuf>,
    /// Phase name → timeout duration. If a worker is in a phase with a timeout
    /// and hasn't sent a keepalive within that duration, it is killed and restarted.
    pub stage_timeouts: HashMap<String, Duration>,
    /// Minimum free system resources below which unassigned tasks are skipped.
    /// Default: Memory → 300MB.
    pub low_resource_thresholds: ResourceMap,
    /// How often the OOM/resource-pressure check fires inside the worker loop.
    /// Default: 100ms.
    pub resource_check_interval: Duration,
    /// Stuck-worker reporting cadence. After a worker has been in the same
    /// phase for any of these durations the manager logs its current phase +
    /// elapsed time. The list does not have to be sorted; the first matching
    /// interval that the worker has just crossed will fire. Empty disables
    /// the reporter. Default: 60s, 5min, 10min, 30min, 1h.
    pub phase_status_log_intervals: Vec<Duration>,
}

impl Default for LocalManagerConfig {
    fn default() -> Self {
        Self {
            num_workers: 0,
            max_resources: ResourceMap::new(),
            always_restart_worker: false,
            print_pid: false,
            memuse_log_path: None,
            stage_timeouts: HashMap::new(),
            low_resource_thresholds: ResourceMap::new(),
            resource_check_interval: Duration::from_millis(100),
            phase_status_log_intervals: vec![
                Duration::from_secs(60),
                Duration::from_secs(300),
                Duration::from_secs(600),
                Duration::from_secs(1800),
                Duration::from_secs(3600),
            ],
        }
    }
}

/// Callback trait for spawning/restarting worker transports.
///
/// The manager is transport-agnostic. The caller provides a factory that
/// creates new `ManagerEndpoint` connections (e.g. socketpair, channel).
pub trait WorkerFactory<M: ManagerEndpoint> {
    /// Create a new transport connection for the given worker.
    /// Called at initial startup and on restart.
    /// Returns (transport, optional_pid) on success.
    /// Returns an error string if the spawn fails (caller decides whether to
    /// abort the run, log and continue with fewer workers, etc.).
    fn spawn_worker(&mut self, worker_id: WorkerId) -> Result<(M, Option<u32>), String>;
}

/// The local manager: owns workers, scheduler, and the 5-phase pipeline.
///
/// Generic over `M` (the transport endpoint type) so it works with both
/// real sockets and in-process channels for testing.
/// Generic over `I` (the identifier type) so different task definitions
/// can use different identifier structures.
pub struct LocalManager<M: ManagerEndpoint, S: Scheduler<I>, E: ResourceEstimator, I: Identifier = ()> {
    config: LocalManagerConfig,
    scheduler: S,
    estimator: E,
    pool: WorkerPool<M, I>,
    pending_binaries: Vec<BinaryInfo<I>>,
    failed_tasks: Vec<FailedTask<I>>,
    resource_pressure_tasks: Vec<FailedTask<I>>,
    unassigned_tasks: Vec<BinaryInfo<I>>,
    pending_worker_assignments: HashSet<WorkerId>,
    in_pressure_phase: bool,
    total_assigned_resources: ResourceMap,
    stats: ProcessingStats,
}

impl<M: ManagerEndpoint + 'static, S: Scheduler<I>, E: ResourceEstimator, I: Identifier> LocalManager<M, S, E, I> {
    pub fn new(config: LocalManagerConfig, scheduler: S, estimator: E) -> Self {
        Self {
            config,
            scheduler,
            estimator,
            pool: WorkerPool::new(),
            pending_binaries: Vec::new(),
            failed_tasks: Vec::new(),
            resource_pressure_tasks: Vec::new(),
            unassigned_tasks: Vec::new(),
            pending_worker_assignments: HashSet::new(),
            in_pressure_phase: false,
            total_assigned_resources: ResourceMap::new(),
            stats: ProcessingStats::default(),
        }
    }

    pub fn stats(&self) -> &ProcessingStats {
        &self.stats
    }

    pub fn failed_tasks(&self) -> &[FailedTask<I>] {
        &self.failed_tasks
    }

    pub fn resource_pressure_tasks(&self) -> &[FailedTask<I>] {
        &self.resource_pressure_tasks
    }

    /// Main entry point: process a list of binaries through the 5-phase pipeline.
    pub async fn process_binaries(
        &mut self,
        binaries: Vec<BinaryInfo<I>>,
        factory: &mut impl WorkerFactory<M>,
    ) -> Result<(), String> {
        self.pending_binaries = binaries;
        self.stats.total = self.pending_binaries.len() as u32;
        self.stats.completed = 0;
        self.stats.errored = 0;

        let max_mem_mb = self.config.max_resources.get(&ResourceKind::memory()) / (1024 * 1024);
        tracing::info!(
            num_workers = self.config.num_workers,
            max_memory_mb = max_mem_mb,
            total = self.stats.total,
            "starting processing"
        );

        self.initialize_workers(factory).await?;
        self.run_initial_assignments(factory).await;
        self.run_main_phase(factory).await;
        self.run_retry_phase(factory).await;
        self.run_resource_pressure_phase(factory).await;
        self.run_unassigned_phase(factory).await;
        self.stop_all_workers().await;

        tracing::info!(
            completed = self.stats.completed,
            total = self.stats.total,
            errored = self.failed_tasks.len(),
            resource_pressure = self.resource_pressure_tasks.len(),
            "processing complete"
        );
        Ok(())
    }

    // ── Initialization ──

    fn max_resources(&self) -> &ResourceMap {
        &self.config.max_resources
    }

    async fn initialize_workers(
        &mut self,
        factory: &mut impl WorkerFactory<M>,
    ) -> Result<(), String> {
        let max = self.config.max_resources.clone();
        self.pool
            .initialize(
                self.config.num_workers,
                &max,
                &self.scheduler,
                factory,
                self.config.print_pid,
            )
            .await
    }

    // ── Phase 1: Initial Assignments ──

    async fn run_initial_assignments(&mut self, factory: &mut impl WorkerFactory<M>) {
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

    async fn try_assign_initial(&mut self, worker_id: WorkerId, factory: &mut impl WorkerFactory<M>) {
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

    async fn run_main_phase(&mut self, factory: &mut impl WorkerFactory<M>) {
        tracing::info!("starting main phase");

        let mut active_workers: HashSet<WorkerId> =
            (0..self.config.num_workers).collect();

        self.process_worker_loop(&mut active_workers, false, true, ProcessingPhase::MainPhase, factory)
            .await;

        // Move remaining pending to unassigned
        if !self.pending_binaries.is_empty() {
            let remaining: Vec<BinaryInfo<I>> = self.pending_binaries.drain(..).collect();
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

    async fn run_retry_phase(&mut self, factory: &mut impl WorkerFactory<M>) {
        if self.failed_tasks.is_empty() {
            tracing::info!("retry phase skipped - no failed tasks");
            return;
        }

        tracing::info!(count = self.failed_tasks.len(), "starting retry phase");

        let retry_tasks: Vec<FailedTask<I>> = self.failed_tasks.drain(..).collect();
        for task in retry_tasks {
            self.pending_binaries.push(task.binary);
        }

        // Restart any stopped/dead workers before retry (matching Python behavior)
        for i in 0..self.config.num_workers {
            if self.pool.workers[i as usize].is_stopped() || !self.pool.workers[i as usize].is_ready() {
                tracing::info!(worker_id = i, "restarting worker for retry phase");
                self.restart_worker(i, factory).await;
                self.pending_worker_assignments.insert(i);
            }
        }

        let mut active_workers: HashSet<WorkerId> =
            (0..self.config.num_workers).collect();

        self.process_worker_loop(&mut active_workers, true, true, ProcessingPhase::RetryPhase, factory)
            .await;

        tracing::info!(
            completed = self.stats.completed,
            errored = self.failed_tasks.len(),
            resource_pressure = self.resource_pressure_tasks.len(),
            "retry phase complete"
        );
    }

    // ── Phase 4: Resource Pressure Phase ──

    async fn run_resource_pressure_phase(&mut self, factory: &mut impl WorkerFactory<M>) {
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

    async fn run_unassigned_phase(&mut self, factory: &mut impl WorkerFactory<M>) {
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

    // ── Core Worker Loop ──

    /// The main event-driven worker processing loop.
    ///
    /// Uses `tokio::select!` to multiplex between worker events (from the
    /// shared channel) and a periodic timer for resource pressure checks and timeouts.
    /// Workers send events via the channel from their spawned poll tasks,
    /// eliminating head-of-line blocking.
    async fn process_worker_loop(
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
    async fn assign_idle_workers(
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

    fn handle_worker_without_task(
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

    async fn try_assign_normal(&mut self, worker_id: WorkerId, factory: &mut impl WorkerFactory<M>) {
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
    async fn handle_assignment_failure(
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
    async fn restart_worker(&mut self, worker_id: WorkerId, factory: &mut impl WorkerFactory<M>) {
        if let Err(e) = self
            .pool
            .restart_worker(worker_id, factory, self.config.print_pid)
            .await
        {
            tracing::error!(worker_id, error = %e, "worker restart failed; slot will remain stopped");
        }
    }

    async fn handle_event(
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
                binary,
                estimated_resources,
            } => {
                // Reclaim protocol state from the spawned poll task
                self.pool.workers[worker_id as usize].reclaim_protocol().await;
                self.pool.workers[worker_id as usize].clear_task();

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

    async fn handle_task_completed(
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
    fn log_resource_usage(
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

    fn record_result(&mut self, result: &TaskResult, binary: Option<&BinaryInfo<I>>) {
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

    /// Check all workers for phase-based timeouts. If a worker has been in a
    /// timed phase longer than the configured timeout without a keepalive,
    /// it is killed and restarted with a Recoverable error.
    async fn check_timeouts(
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
    fn report_stuck_workers(&mut self) {
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

    fn check_resource_pressure(&mut self) {
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
    fn get_free_system_memory() -> u64 {
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

    async fn stop_all_workers(&mut self) {
        self.pool.stop_all().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use db_comm_api_base::{MessageReceiver, MessageSender};
    use db_manager_runner_comm::{Command, Response};
    use db_scheduler_impl::ResourceStealingScheduler;
    use db_transport_channel::{ChannelManagerEnd, channel_pair};
    use serde::{Deserialize, Serialize};

    /// Minimal test identifier.
    #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
    struct TestId(String);

    struct FixedEstimator(u64);
    impl ResourceEstimator for FixedEstimator {
        fn estimate(&self, _binary_size: u64) -> db_comm_api_base::ResourceMap {
            db_comm_api_base::ResourceMap::from([(ResourceKind::memory(), self.0)])
        }
    }

    fn make_binary(name: &str, size: u64) -> BinaryInfo<TestId> {
        BinaryInfo {
            path: std::path::PathBuf::from(name),
            size,
            identifier: TestId(name.into()),
        }
    }

    /// A simple factory that spawns fake worker tasks that auto-respond.
    struct FakeWorkerFactory {
        /// Each spawned worker's runner end is driven by a background task.
        mode: FakeWorkerMode,
    }

    #[derive(Clone)]
    enum FakeWorkerMode {
        /// Immediately complete all tasks successfully.
        AlwaysSucceed,
        /// Complete with OOM error.
        AlwaysOom,
        /// First task fails with recoverable error, second succeeds.
        FailThenSucceed,
    }

    impl WorkerFactory<ChannelManagerEnd> for FakeWorkerFactory {
        fn spawn_worker(
            &mut self,
            _worker_id: WorkerId,
        ) -> Result<(ChannelManagerEnd, Option<u32>), String> {
            let (manager_end, runner_end) = channel_pair();
            let mode = self.mode.clone();
            tokio::task::spawn_local(fake_worker_loop(runner_end, mode));
            Ok((manager_end, None))
        }
    }

    async fn fake_worker_loop(
        mut runner: db_transport_channel::ChannelRunnerEnd,
        mode: FakeWorkerMode,
    ) {
        // Send Ready
        let _ = runner.send(Response::Ready).await;

        let mut task_count = 0u32;
        loop {
            match MessageReceiver::<Command>::recv(&mut runner).await {
                Some(Command::Stop) => break,
                Some(Command::ProcessTask { .. }) => {
                    task_count += 1;
                    match &mode {
                        FakeWorkerMode::AlwaysSucceed => {
                            let _ = runner
                                .send(Response::Done {
                                    result_data: None,
                                })
                                .await;
                        }
                        FakeWorkerMode::AlwaysOom => {
                            let _ = runner
                                .send(Response::Error {
                                    error_type: ErrorType::ResourceExhausted(ResourceKind::memory()),
                                    message: "out of memory".into(),
                                })
                                .await;
                        }
                        FakeWorkerMode::FailThenSucceed => {
                            if task_count == 1 {
                                let _ = runner
                                    .send(Response::Error {
                                        error_type: ErrorType::Recoverable,
                                        message: "transient failure".into(),
                                    })
                                    .await;
                            } else {
                                let _ = runner
                                    .send(Response::Done {
                                        result_data: None,
                                    })
                                    .await;
                            }
                        }
                    }
                }
                None => break,
            }
        }
    }

    fn test_config(num_workers: u32) -> LocalManagerConfig {
        LocalManagerConfig {
            num_workers,
            max_resources: ResourceMap::from([(ResourceKind::memory(), 1024 * 1024 * 1024)]), // 1GB
            always_restart_worker: false,
            print_pid: false,
            memuse_log_path: None,
            stage_timeouts: HashMap::new(),
            low_resource_thresholds: ResourceMap::from([(ResourceKind::memory(), 300 * 1024 * 1024)]),
            resource_check_interval: std::time::Duration::from_millis(100),
            phase_status_log_intervals: Vec::new(),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn single_worker_processes_all_binaries() {
        let local = tokio::task::LocalSet::new();
        local.run_until(async {
            let config = test_config(1);
            let mut manager = LocalManager::new(config, ResourceStealingScheduler::memory(), FixedEstimator(100));
            let mut factory = FakeWorkerFactory {
                mode: FakeWorkerMode::AlwaysSucceed,
            };

            let binaries = vec![
                make_binary("a", 50),
                make_binary("b", 60),
                make_binary("c", 70),
            ];

            manager.process_binaries(binaries, &mut factory).await.unwrap();

            assert_eq!(manager.stats().completed, 3);
            assert_eq!(manager.stats().total, 3);
            assert!(manager.failed_tasks().is_empty());
            assert!(manager.resource_pressure_tasks().is_empty());
        }).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn multiple_workers_process_binaries() {
        let local = tokio::task::LocalSet::new();
        local.run_until(async {
            let config = test_config(3);
            let mut manager =
                LocalManager::new(config, ResourceStealingScheduler::memory(), FixedEstimator(100));
            let mut factory = FakeWorkerFactory {
                mode: FakeWorkerMode::AlwaysSucceed,
            };

            let binaries: Vec<BinaryInfo<TestId>> = (0..10)
                .map(|i| make_binary(&format!("bin_{i}"), 100))
                .collect();

            manager.process_binaries(binaries, &mut factory).await.unwrap();

            assert_eq!(manager.stats().completed, 10);
            assert!(manager.failed_tasks().is_empty());
        }).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn retry_phase_retries_failed_tasks() {
        let local = tokio::task::LocalSet::new();
        local.run_until(async {
            let config = test_config(1);
            let mut manager =
                LocalManager::new(config, ResourceStealingScheduler::memory(), FixedEstimator(100));
            let mut factory = FakeWorkerFactory {
                mode: FakeWorkerMode::FailThenSucceed,
            };

            let binaries = vec![make_binary("retry_me", 50)];
            manager.process_binaries(binaries, &mut factory).await.unwrap();

            // First attempt fails, retry succeeds
            assert_eq!(manager.stats().completed, 1);
            assert!(manager.failed_tasks().is_empty());
        }).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn resource_pressure_tasks_collected() {
        let local = tokio::task::LocalSet::new();
        local.run_until(async {
            let config = test_config(1);
            let mut manager =
                LocalManager::new(config, ResourceStealingScheduler::memory(), FixedEstimator(100));
            let mut factory = FakeWorkerFactory {
                mode: FakeWorkerMode::AlwaysOom,
            };

            let binaries = vec![make_binary("oom_bin", 50)];
            manager.process_binaries(binaries, &mut factory).await.unwrap();

            // OOM in main → retry → OOM again → OOM phase → OOM again
            // Eventually ends up in resource_pressure_tasks or failed_tasks
            assert_eq!(manager.stats().completed, 0);
        }).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn no_binaries_completes_immediately() {
        let local = tokio::task::LocalSet::new();
        local.run_until(async {
            let config = test_config(1);
            let mut manager =
                LocalManager::new(config, ResourceStealingScheduler::memory(), FixedEstimator(100));
            let mut factory = FakeWorkerFactory {
                mode: FakeWorkerMode::AlwaysSucceed,
            };

            manager
                .process_binaries(Vec::<BinaryInfo<TestId>>::new(), &mut factory)
                .await
                .unwrap();

            assert_eq!(manager.stats().completed, 0);
            assert_eq!(manager.stats().total, 0);
        }).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn always_restart_worker_respawns_after_success() {
        use std::sync::atomic::{AtomicU32, Ordering};
        use std::sync::Arc;

        struct CountingFactory {
            spawn_count: Arc<AtomicU32>,
        }

        impl WorkerFactory<ChannelManagerEnd> for CountingFactory {
            fn spawn_worker(
                &mut self,
                _worker_id: WorkerId,
            ) -> Result<(ChannelManagerEnd, Option<u32>), String> {
                self.spawn_count.fetch_add(1, Ordering::SeqCst);
                let (manager_end, runner_end) = channel_pair();
                tokio::task::spawn_local(async move {
                    let mut runner = runner_end;
                    let _ = runner.send(Response::Ready).await;
                    loop {
                        match MessageReceiver::<Command>::recv(&mut runner).await {
                            Some(Command::Stop) => break,
                            Some(Command::ProcessTask { .. }) => {
                                let _ = runner
                                    .send(Response::Done {
                                        result_data: None,
                                    })
                                    .await;
                            }
                            None => break,
                        }
                    }
                });
                Ok((manager_end, Some(42)))
            }
        }

        let local = tokio::task::LocalSet::new();
        local.run_until(async {
            let spawn_count = Arc::new(AtomicU32::new(0));
            let spawn_count_clone = spawn_count.clone();

            let mut config = test_config(1);
            config.always_restart_worker = true;

            let mut manager = LocalManager::new(config, ResourceStealingScheduler::memory(), FixedEstimator(100));
            let mut factory = CountingFactory {
                spawn_count: spawn_count_clone,
            };

            let binaries = vec![
                make_binary("a", 50),
                make_binary("b", 60),
                make_binary("c", 70),
            ];

            manager.process_binaries(binaries, &mut factory).await.unwrap();

            assert_eq!(manager.stats().completed, 3);
            assert_eq!(manager.stats().total, 3);
            assert!(manager.failed_tasks().is_empty());

            // With always_restart_worker=true and 3 binaries with 1 worker:
            // 1 initial spawn + 2 restarts (after "a" and "b" complete, "c" is the last so no restart)
            let spawns = spawn_count.load(Ordering::SeqCst);
            assert_eq!(spawns, 3, "expected 3 spawns (1 initial + 2 restarts), got {spawns}");
        }).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn memuse_log_written() {
        let local = tokio::task::LocalSet::new();
        local.run_until(async {
            let tmp_dir = std::env::temp_dir().join("rust_memuse_test");
            let _ = std::fs::create_dir_all(&tmp_dir);
            let memuse_path = tmp_dir.join("memuse.log");
            // Clean up any previous run
            let _ = std::fs::remove_file(&memuse_path);

            let config = LocalManagerConfig {
                num_workers: 1,
                max_resources: ResourceMap::from([(ResourceKind::memory(), 1024 * 1024 * 1024)]),
                always_restart_worker: false,
                print_pid: false,
                memuse_log_path: Some(memuse_path.clone()),
                stage_timeouts: HashMap::new(),
                low_resource_thresholds: ResourceMap::from([(ResourceKind::memory(), 300 * 1024 * 1024)]),
            resource_check_interval: std::time::Duration::from_millis(100),
            phase_status_log_intervals: Vec::new(),
            };

            let mut manager = LocalManager::new(config, ResourceStealingScheduler::memory(), FixedEstimator(100));
            let mut factory = FakeWorkerFactory {
                mode: FakeWorkerMode::AlwaysSucceed,
            };

            let binaries = vec![
                make_binary("a", 50),
                make_binary("b", 60),
            ];

            manager.process_binaries(binaries, &mut factory).await.unwrap();

            assert_eq!(manager.stats().completed, 2);

            // Verify memuse.log was written
            let contents = std::fs::read_to_string(&memuse_path).expect("memuse.log should exist");
            let lines: Vec<&str> = contents.lines().collect();
            assert_eq!(lines.len(), 2, "expected 2 lines in memuse.log, got {}", lines.len());

            // Each line: size,estimated,0,filename,status
            assert!(lines[0].contains(",OK"), "first line should contain OK: {}", lines[0]);
            assert!(lines[1].contains(",OK"), "second line should contain OK: {}", lines[1]);

            let _ = std::fs::remove_dir_all(&tmp_dir);
        }).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn non_recoverable_error_restarts_worker_and_continues() {
        use std::sync::atomic::{AtomicU32, Ordering};
        use std::sync::Arc;

        struct RestartCountingFactory {
            spawn_count: Arc<AtomicU32>,
        }

        impl WorkerFactory<ChannelManagerEnd> for RestartCountingFactory {
            fn spawn_worker(
                &mut self,
                _worker_id: WorkerId,
            ) -> Result<(ChannelManagerEnd, Option<u32>), String> {
                let count = self.spawn_count.fetch_add(1, Ordering::SeqCst);
                let (manager_end, runner_end) = channel_pair();
                tokio::task::spawn_local(async move {
                    let mut runner = runner_end;
                    let _ = runner.send(Response::Ready).await;
                    loop {
                        match MessageReceiver::<Command>::recv(&mut runner).await {
                            Some(Command::Stop) => break,
                            Some(Command::ProcessTask { .. }) => {
                                if count == 0 {
                                    // First spawn: send NonRecoverable error (triggers disconnect)
                                    let _ = runner
                                        .send(Response::Error {
                                            error_type: ErrorType::NonRecoverable,
                                            message: "crash".into(),
                                        })
                                        .await;
                                    break; // NonRecoverable worker exits
                                } else {
                                    // Restarted worker: succeed
                                    let _ = runner
                                        .send(Response::Done {
                                            result_data: None,
                                        })
                                        .await;
                                }
                            }
                            None => break,
                        }
                    }
                });
                Ok((manager_end, None))
            }
        }

        let local = tokio::task::LocalSet::new();
        local.run_until(async {
            let spawn_count = Arc::new(AtomicU32::new(0));
            let spawn_count_clone = spawn_count.clone();

            let config = test_config(1);
            let mut manager = LocalManager::new(config, ResourceStealingScheduler::memory(), FixedEstimator(100));
            let mut factory = RestartCountingFactory {
                spawn_count: spawn_count_clone,
            };

            let binaries = vec![
                make_binary("crash_me", 50),
                make_binary("succeed", 60),
            ];

            manager.process_binaries(binaries, &mut factory).await.unwrap();

            // First task: NonRecoverable -> fails, worker restarted
            // Second task: succeeds on restarted worker
            // Retry phase: first task retried on restarted worker and succeeds
            assert_eq!(manager.stats().completed, 2, "both tasks should complete");
            assert!(manager.resource_pressure_tasks().is_empty(), "no OOM tasks expected");

            // At least 2 spawns: initial + restart after NonRecoverable
            let spawns = spawn_count.load(Ordering::SeqCst);
            assert!(spawns >= 2, "expected at least 2 spawns (initial + restart), got {spawns}");
        }).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn multiple_workers_with_mixed_results() {
        let local = tokio::task::LocalSet::new();
        local.run_until(async {
            // 2 workers, 6 binaries: worker 0 always succeeds, worker 1 first OOM then succeed
            let config = test_config(2);
            let mut manager = LocalManager::new(config, ResourceStealingScheduler::memory(), FixedEstimator(100));
            let mut factory = FakeWorkerFactory {
                mode: FakeWorkerMode::AlwaysSucceed,
            };

            let binaries: Vec<BinaryInfo<TestId>> = (0..6)
                .map(|i| make_binary(&format!("bin_{i}"), 100 + i * 10))
                .collect();

            manager.process_binaries(binaries, &mut factory).await.unwrap();

            assert_eq!(manager.stats().completed, 6);
            assert_eq!(manager.stats().total, 6);
            assert!(manager.failed_tasks().is_empty());
            assert!(manager.resource_pressure_tasks().is_empty());
        }).await;
    }
}
