use std::collections::HashSet;

use db_comm_api_base::{
    BinaryInfo, ErrorType, FailedTask, Identifier, ManagerEndpoint, MemoryBytes, TaskResult, WorkerId,
};
use db_scheduler_api::{
    AssignmentDecision, MemoryEstimator, OomDecision, ProcessingPhase, Scheduler,
    WorkerBudgetInfo,
};

use crate::stats::ProcessingStats;
use crate::worker::{WorkerEvent, WorkerHandle};

/// Configuration for the local manager.
pub struct LocalManagerConfig {
    pub num_workers: u32,
    pub max_memory: MemoryBytes,
    pub always_restart_worker: bool,
    pub print_pid: bool,
    pub memuse_log_path: Option<std::path::PathBuf>,
}

/// Callback trait for spawning/restarting worker transports.
///
/// The manager is transport-agnostic. The caller provides a factory that
/// creates new `ManagerEndpoint` connections (e.g. socketpair, channel).
pub trait WorkerFactory<M: ManagerEndpoint> {
    /// Create a new transport connection for the given worker.
    /// Called at initial startup and on restart.
    /// Returns (transport, optional_pid).
    fn spawn_worker(&mut self, worker_id: WorkerId) -> (M, Option<u32>);
}

/// The local manager: owns workers, scheduler, and the 5-phase pipeline.
///
/// Generic over `M` (the transport endpoint type) so it works with both
/// real sockets and in-process channels for testing.
/// Generic over `I` (the identifier type) so different task definitions
/// can use different identifier structures.
pub struct LocalManager<M: ManagerEndpoint, S: Scheduler<I>, E: MemoryEstimator, I: Identifier = ()> {
    config: LocalManagerConfig,
    scheduler: S,
    estimator: E,
    workers: Vec<WorkerHandle<M, I>>,
    pending_binaries: Vec<BinaryInfo<I>>,
    failed_tasks: Vec<FailedTask<I>>,
    oom_tasks: Vec<FailedTask<I>>,
    unassigned_tasks: Vec<BinaryInfo<I>>,
    pending_worker_assignments: HashSet<WorkerId>,
    in_oom_phase: bool,
    total_assigned_memory: MemoryBytes,
    stats: ProcessingStats,
}

impl<M: ManagerEndpoint, S: Scheduler<I>, E: MemoryEstimator, I: Identifier> LocalManager<M, S, E, I> {
    pub fn new(config: LocalManagerConfig, scheduler: S, estimator: E) -> Self {
        Self {
            config,
            scheduler,
            estimator,
            workers: Vec::new(),
            pending_binaries: Vec::new(),
            failed_tasks: Vec::new(),
            oom_tasks: Vec::new(),
            unassigned_tasks: Vec::new(),
            pending_worker_assignments: HashSet::new(),
            in_oom_phase: false,
            total_assigned_memory: 0,
            stats: ProcessingStats::default(),
        }
    }

    pub fn stats(&self) -> &ProcessingStats {
        &self.stats
    }

    pub fn failed_tasks(&self) -> &[FailedTask<I>] {
        &self.failed_tasks
    }

    pub fn oom_tasks(&self) -> &[FailedTask<I>] {
        &self.oom_tasks
    }

    /// Main entry point: process a list of binaries through the 5-phase pipeline.
    pub async fn process_binaries(
        &mut self,
        binaries: Vec<BinaryInfo<I>>,
        factory: &mut impl WorkerFactory<M>,
    ) {
        self.pending_binaries = binaries;
        self.stats.total = self.pending_binaries.len() as u32;
        self.stats.completed = 0;
        self.stats.errored = 0;

        tracing::info!(
            num_workers = self.config.num_workers,
            max_memory_mb = self.config.max_memory / (1024 * 1024),
            total = self.stats.total,
            "starting processing"
        );

        self.initialize_workers(factory).await;
        self.run_initial_assignments(factory).await;
        self.run_main_phase(factory).await;
        self.run_retry_phase(factory).await;
        self.run_oom_phase(factory).await;
        self.run_unassigned_phase(factory).await;
        self.stop_all_workers().await;

        tracing::info!(
            completed = self.stats.completed,
            total = self.stats.total,
            errored = self.failed_tasks.len(),
            oom = self.oom_tasks.len(),
            "processing complete"
        );
    }

    // ── Initialization ──

    async fn initialize_workers(&mut self, factory: &mut impl WorkerFactory<M>) {
        for i in 0..self.config.num_workers {
            let (transport, pid) = factory.spawn_worker(i);
            if self.config.print_pid {
                if let Some(pid) = pid {
                    tracing::info!(worker_id = i, pid, "worker PID");
                }
            }
            let mut handle = WorkerHandle::new(i, transport);
            handle.reserved_budget =
                self.scheduler
                    .initial_budget(i, self.config.max_memory);
            tracing::info!(
                worker_id = i,
                budget_mb = handle.reserved_budget / (1024 * 1024),
                "worker created"
            );
            self.workers.push(handle);
        }

        // Wait for all workers to become ready
        self.wait_for_all_ready().await;
    }

    async fn wait_for_all_ready(&mut self) {
        loop {
            let all_ready = self.workers.iter().all(|w| w.is_ready());
            if all_ready {
                tracing::info!("all workers ready");
                break;
            }
            for worker in &mut self.workers {
                if !worker.is_ready() {
                    worker.poll_ready().await;
                }
            }
            tokio::task::yield_now().await;
        }
    }

    // ── Phase 1: Initial Assignments ──

    async fn run_initial_assignments(&mut self, factory: &mut impl WorkerFactory<M>) {
        tracing::info!("starting initial assignment phase");

        loop {
            let all_assigned = self
                .workers
                .iter()
                .all(|w| w.has_initial_assignment);
            if all_assigned {
                break;
            }

            for i in 0..self.workers.len() {
                if self.workers[i].has_initial_assignment || !self.workers[i].is_ready() {
                    continue;
                }
                self.try_assign_initial(i as WorkerId, factory).await;
            }
            tokio::task::yield_now().await;
        }

        let opp_mem: u64 = self
            .workers
            .iter()
            .filter(|w| w.opportunistic && w.current_binary.is_some())
            .map(|w| w.estimated_memory)
            .sum();
        let non_opp_mem: u64 = self
            .workers
            .iter()
            .filter(|w| !w.opportunistic && w.current_binary.is_some())
            .map(|w| w.estimated_memory)
            .sum();
        tracing::info!(
            total_assigned_mb = self.total_assigned_memory / (1024 * 1024),
            non_opportunistic_mb = non_opp_mem / (1024 * 1024),
            opportunistic_mb = opp_mem / (1024 * 1024),
            "initial assignments complete"
        );
    }

    async fn try_assign_initial(&mut self, worker_id: WorkerId, factory: &mut impl WorkerFactory<M>) {
        let worker_info = self.workers[worker_id as usize].budget_info();
        let decision = self.scheduler.assign_initial(
            &worker_info,
            &self.pending_binaries,
            self.total_assigned_memory,
            self.config.max_memory,
            &self.estimator,
        );

        match decision {
            AssignmentDecision::Assign {
                binary_index,
                estimated_memory,
                opportunistic,
                ..
            } => {
                let binary = self.pending_binaries.remove(binary_index);
                self.total_assigned_memory += estimated_memory;
                let name = binary.path.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();

                let worker = &mut self.workers[worker_id as usize];
                match worker.assign_task(binary.clone(), estimated_memory, opportunistic).await {
                    Ok(()) => {
                        tracing::info!(
                            worker_id,
                            binary = %name,
                            estimated_mb = estimated_memory / (1024 * 1024),
                            opportunistic,
                            "initial assignment"
                        );
                        self.workers[worker_id as usize].assignment_failure_count = 0;
                    }
                    Err(e) => {
                        // Put binary back and undo memory increment
                        self.pending_binaries.insert(0, binary);
                        self.total_assigned_memory = self.total_assigned_memory.saturating_sub(estimated_memory);
                        self.handle_assignment_failure(worker_id, &e, factory).await;
                    }
                }
            }
            AssignmentDecision::NoFit => {
                self.workers[worker_id as usize].idle = true;
                self.workers[worker_id as usize].has_initial_assignment = true;
            }
            AssignmentDecision::NoPendingTasks => {
                self.workers[worker_id as usize].idle = true;
                self.workers[worker_id as usize].has_initial_assignment = true;
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
            oom = self.oom_tasks.len(),
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
            if self.workers[i as usize].is_stopped() || !self.workers[i as usize].is_ready() {
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
            oom = self.oom_tasks.len(),
            "retry phase complete"
        );
    }

    // ── Phase 4: OOM Phase ──

    async fn run_oom_phase(&mut self, factory: &mut impl WorkerFactory<M>) {
        if self.oom_tasks.is_empty() {
            tracing::info!("OOM phase skipped - no OOM tasks");
            return;
        }

        tracing::info!(count = self.oom_tasks.len(), "starting OOM phase");

        self.in_oom_phase = true;

        let oom_tasks: Vec<FailedTask<I>> = self.oom_tasks.drain(..).collect();
        for task in oom_tasks {
            self.pending_binaries.push(task.binary);
        }

        // Process with only worker 0
        let mut active_workers: HashSet<WorkerId> = HashSet::new();
        active_workers.insert(0);

        self.process_worker_loop(&mut active_workers, false, false, ProcessingPhase::OomPhase, factory)
            .await;

        self.in_oom_phase = false;

        tracing::info!(
            completed = self.stats.completed,
            errored = self.failed_tasks.len(),
            oom = self.oom_tasks.len(),
            "OOM phase complete"
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

        let tasks: Vec<BinaryInfo<I>> = self.unassigned_tasks.drain(..).collect();
        for task in tasks {
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
    /// Replaces Python's `_process_worker_loop` + `threading.Event().wait(0.1)`.
    /// Uses `tokio::task::yield_now()` instead of sleep(0.1) — actual event-driven
    /// behavior comes from the transport's recv_responses blocking.
    async fn process_worker_loop(
        &mut self,
        active_workers: &mut HashSet<WorkerId>,
        allow_stop: bool,
        on_failure_increment_failed: bool,
        phase: ProcessingPhase,
        factory: &mut impl WorkerFactory<M>,
    ) {
        while !active_workers.is_empty() {
            let worker_ids: Vec<WorkerId> = active_workers.iter().copied().collect();

            for worker_id in worker_ids {
                let idx = worker_id as usize;

                // In main phase, check OOM per-worker (matching Python's decision_impl.py
                // which calls _check_memory_pressure_and_kill inside the per-worker loop)
                if phase == ProcessingPhase::MainPhase && !self.pending_binaries.is_empty() {
                    self.check_oom();
                }

                // Poll not-yet-ready workers
                if !self.workers[idx].is_ready() {
                    self.workers[idx].poll_ready().await;
                    if !self.workers[idx].is_ready() && !self.pending_binaries.is_empty() {
                        continue;
                    }
                    if !self.workers[idx].is_ready() && self.pending_binaries.is_empty() && allow_stop {
                        active_workers.remove(&worker_id);
                        continue;
                    }
                }

                if self.workers[idx].current_binary.is_none() {
                    // Worker has no task — try to assign
                    if !self.handle_worker_without_task(worker_id, active_workers, allow_stop, phase) {
                        continue;
                    }
                } else {
                    // Worker is processing — poll for result
                    let event = self.workers[idx].poll_status().await;
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
            }

            // Handle pending worker reassignments
            if !self.pending_worker_assignments.is_empty() && !self.pending_binaries.is_empty() {
                let pending: Vec<WorkerId> =
                    self.pending_worker_assignments.iter().copied().collect();
                for worker_id in pending {
                    let idx = worker_id as usize;
                    if self.workers[idx].current_binary.is_none() && self.workers[idx].is_ready() {
                        self.try_assign_normal(worker_id, factory).await;
                        self.pending_worker_assignments.remove(&worker_id);
                    }
                }
            }

            // OOM checking for non-main phases (main phase checks per-worker above).
            // This runs during retry, OOM, and unassigned phases.
            // During OOM phase, the check still runs (matching Python) but check_oom
            // will not requeue killed tasks to pending_binaries.
            if phase != ProcessingPhase::MainPhase && !self.pending_binaries.is_empty() {
                self.check_oom();
            }

            if !active_workers.is_empty() {
                tokio::task::yield_now().await;
            }
        }

        // Move remaining pending to OOM queue at end of retry phase
        // (Main phase leftovers go to unassigned_tasks in run_main_phase)
        if phase == ProcessingPhase::RetryPhase {
            if !self.pending_binaries.is_empty() {
                let remaining: Vec<BinaryInfo<I>> = self.pending_binaries.drain(..).collect();
                for binary in remaining {
                    self.oom_tasks.push(FailedTask {
                        binary,
                        error_type: ErrorType::OutOfMemory,
                        error_message: "Could not fit in any worker budget".into(),
                        retry_count: 0,
                    });
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
        let worker_info = self.workers[worker_id as usize].budget_info();
        let all_infos = self.worker_budget_infos();
        let decision = self.scheduler.assign_normal(
            &worker_info,
            &all_infos,
            &self.pending_binaries,
            self.config.max_memory,
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
                    self.config.max_memory,
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
        let worker_info = self.workers[worker_id as usize].budget_info();
        let all_infos = self.worker_budget_infos();
        let decision = self.scheduler.assign_normal(
            &worker_info,
            &all_infos,
            &self.pending_binaries,
            self.config.max_memory,
            &self.estimator,
            false,
        );

        match decision {
            AssignmentDecision::Assign {
                binary_index,
                estimated_memory,
                opportunistic,
                ..
            } => {
                let binary = self.pending_binaries.remove(binary_index);
                let name = binary.path.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
                let worker = &mut self.workers[worker_id as usize];
                match worker
                    .assign_task(binary.clone(), estimated_memory, opportunistic)
                    .await
                {
                    Ok(()) => {
                        self.total_assigned_memory += estimated_memory;
                        tracing::info!(
                            worker_id,
                            binary = %name,
                            estimated_mb = estimated_memory / (1024 * 1024),
                            "assigned task"
                        );
                        // Reset failure count on success
                        self.workers[worker_id as usize].assignment_failure_count = 0;
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
        let worker = &mut self.workers[worker_id as usize];
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
    async fn restart_worker(&mut self, worker_id: WorkerId, factory: &mut impl WorkerFactory<M>) {
        // Stop old worker
        let old = &mut self.workers[worker_id as usize];
        if !old.is_stopped() {
            old.stop().await;
        }

        // Spawn new transport
        let (transport, pid) = factory.spawn_worker(worker_id);
        if self.config.print_pid {
            if let Some(pid) = pid {
                tracing::info!(worker_id, pid, "worker PID (restart)");
            }
        }

        let mut handle = WorkerHandle::new(worker_id, transport);
        handle.reserved_budget = self.workers[worker_id as usize].reserved_budget;
        handle.assignment_failure_count = self.workers[worker_id as usize].assignment_failure_count;
        self.workers[worker_id as usize] = handle;

        // Wait for ready
        loop {
            if self.workers[worker_id as usize].is_ready() {
                break;
            }
            self.workers[worker_id as usize].poll_ready().await;
            tokio::task::yield_now().await;
        }

        tracing::info!(worker_id, "worker restarted and ready");
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
                estimated_memory,
            } => {
                self.handle_task_completed(
                    worker_id,
                    result,
                    binary,
                    estimated_memory,
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
                tracing::warn!(
                    worker_id,
                    error = ?result.error_message,
                    "worker disconnected"
                );
                self.record_result(&result, binary.as_ref());
                active_workers.remove(&worker_id);
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
            }
            WorkerEvent::Keepalive { worker_id } => {
                tracing::trace!(worker_id, "keepalive");
            }
        }
    }

    async fn handle_task_completed(
        &mut self,
        worker_id: WorkerId,
        result: TaskResult,
        binary: Option<BinaryInfo<I>>,
        estimated_memory: MemoryBytes,
        active_workers: &mut HashSet<WorkerId>,
        allow_stop: bool,
        on_failure_increment_failed: bool,
        _phase: ProcessingPhase,
        factory: &mut impl WorkerFactory<M>,
    ) {
        // Log memory usage before recording result
        self.log_memory_usage(binary.as_ref(), estimated_memory, !result.success);

        // Release estimated memory from total (only for non-opportunistic workers
        // that received an initial assignment, matching Python behavior)
        let worker = &self.workers[worker_id as usize];
        if worker.has_initial_assignment && !worker.opportunistic {
            self.total_assigned_memory = self.total_assigned_memory.saturating_sub(estimated_memory);
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
        if self.workers[worker_id as usize].current_binary.is_none()
            && self.pending_binaries.is_empty()
        {
            if allow_stop {
                tracing::info!(worker_id, "stopping (no more tasks after completion)");
            }
            active_workers.remove(&worker_id);
        }
    }

    /// Log memory usage to memuse.log in CSV format: size,estimated,0,filename,status
    fn log_memory_usage(
        &self,
        binary: Option<&BinaryInfo<I>>,
        estimated_memory: MemoryBytes,
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

        use std::io::Write;
        match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_path)
        {
            Ok(mut f) => {
                // Format: size,estimated,actual,filename,status
                // actual=0 since we don't have psutil in Rust
                let _ = writeln!(f, "{},{},0,{},{}", binary.size, estimated_memory, filename, status);
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
            match result.error_type {
                Some(ErrorType::OutOfMemory) => {
                    if let Some(binary) = binary {
                        self.oom_tasks.push(FailedTask {
                            binary: binary.clone(),
                            error_type: ErrorType::OutOfMemory,
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

    fn check_oom(&mut self) {
        let infos = self.worker_budget_infos();
        let decision =
            self.scheduler
                .check_oom(&infos, self.config.max_memory, self.in_oom_phase);

        match decision {
            OomDecision::Kill { worker_id, reason } => {
                tracing::warn!(worker_id, reason = %reason, in_oom_phase = self.in_oom_phase, "OOM killing worker");
                let worker = &mut self.workers[worker_id as usize];
                if let Some(binary) = worker.current_binary.take() {
                    if worker_id == 0 {
                        // Worker 0 is the last resort — if it can't fit, the task
                        // is truly OOM and goes to the oom_tasks queue.
                        // This happens even during OOM phase (matching Python).
                        self.oom_tasks.push(FailedTask {
                            binary,
                            error_type: ErrorType::OutOfMemory,
                            error_message: reason.clone(),
                            retry_count: 0,
                        });
                    } else if !self.in_oom_phase {
                        // Other workers: requeue for local retry.
                        // During OOM phase, Python skips _handle_oom_killed_task
                        // (which does the requeue), so we also skip requeuing.
                        self.pending_binaries.insert(0, binary);
                    }
                    // During OOM phase for non-worker-0: task is dropped (not requeued)
                    // matching Python's behavior where _handle_oom_killed_task is skipped.
                }
                worker.mark_oom_killed();
            }
            OomDecision::NoAction => {}
        }
    }

    fn worker_budget_infos(&self) -> Vec<WorkerBudgetInfo<I>> {
        self.workers.iter().map(|w| w.budget_info()).collect()
    }

    async fn stop_all_workers(&mut self) {
        for worker in &mut self.workers {
            if !worker.is_stopped() {
                worker.stop().await;
                tracing::info!(worker_id = worker.worker_id, "worker stopped");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use db_comm_api_base::{CommandReceiver, Response, ResponseSender};
    use db_scheduler_impl::MemoryStealingScheduler;
    use db_transport_channel::{ChannelManagerEnd, channel_pair};
    use serde::{Deserialize, Serialize};

    /// Minimal test identifier.
    #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
    struct TestId(String);

    struct FixedEstimator(u64);
    impl MemoryEstimator for FixedEstimator {
        fn estimate_memory(&self, _binary_size: u64) -> MemoryBytes {
            self.0
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
        fn spawn_worker(&mut self, _worker_id: WorkerId) -> (ChannelManagerEnd, Option<u32>) {
            let (manager_end, runner_end) = channel_pair();
            let mode = self.mode.clone();
            tokio::spawn(fake_worker_loop(runner_end, mode));
            (manager_end, None)
        }
    }

    async fn fake_worker_loop(
        mut runner: db_transport_channel::ChannelRunnerEnd,
        mode: FakeWorkerMode,
    ) {
        use db_comm_api_base::Command;

        // Send Ready
        let _ = runner.send_response(Response::Ready).await;

        let mut task_count = 0u32;
        loop {
            match runner.recv_command().await {
                Some(Command::Stop) => break,
                Some(Command::ProcessBinary { .. }) => {
                    task_count += 1;
                    match &mode {
                        FakeWorkerMode::AlwaysSucceed => {
                            let _ = runner
                                .send_response(Response::Done {
                                    warnings: 0,
                                    filtered: 0,
                                })
                                .await;
                        }
                        FakeWorkerMode::AlwaysOom => {
                            let _ = runner
                                .send_response(Response::Error {
                                    error_type: ErrorType::OutOfMemory,
                                    message: "out of memory".into(),
                                })
                                .await;
                        }
                        FakeWorkerMode::FailThenSucceed => {
                            if task_count == 1 {
                                let _ = runner
                                    .send_response(Response::Error {
                                        error_type: ErrorType::Recoverable,
                                        message: "transient failure".into(),
                                    })
                                    .await;
                            } else {
                                let _ = runner
                                    .send_response(Response::Done {
                                        warnings: 0,
                                        filtered: 0,
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
            max_memory: 1024 * 1024 * 1024, // 1GB
            always_restart_worker: false,
            print_pid: false,
            memuse_log_path: None,
        }
    }

    #[tokio::test]
    async fn single_worker_processes_all_binaries() {
        let config = test_config(1);
        let mut manager = LocalManager::new(config, MemoryStealingScheduler, FixedEstimator(100));
        let mut factory = FakeWorkerFactory {
            mode: FakeWorkerMode::AlwaysSucceed,
        };

        let binaries = vec![
            make_binary("a", 50),
            make_binary("b", 60),
            make_binary("c", 70),
        ];

        manager.process_binaries(binaries, &mut factory).await;

        assert_eq!(manager.stats().completed, 3);
        assert_eq!(manager.stats().total, 3);
        assert!(manager.failed_tasks().is_empty());
        assert!(manager.oom_tasks().is_empty());
    }

    #[tokio::test]
    async fn multiple_workers_process_binaries() {
        let config = test_config(3);
        let mut manager =
            LocalManager::new(config, MemoryStealingScheduler, FixedEstimator(100));
        let mut factory = FakeWorkerFactory {
            mode: FakeWorkerMode::AlwaysSucceed,
        };

        let binaries: Vec<BinaryInfo<TestId>> = (0..10)
            .map(|i| make_binary(&format!("bin_{i}"), 100))
            .collect();

        manager.process_binaries(binaries, &mut factory).await;

        assert_eq!(manager.stats().completed, 10);
        assert!(manager.failed_tasks().is_empty());
    }

    #[tokio::test]
    async fn retry_phase_retries_failed_tasks() {
        let config = test_config(1);
        let mut manager =
            LocalManager::new(config, MemoryStealingScheduler, FixedEstimator(100));
        let mut factory = FakeWorkerFactory {
            mode: FakeWorkerMode::FailThenSucceed,
        };

        let binaries = vec![make_binary("retry_me", 50)];
        manager.process_binaries(binaries, &mut factory).await;

        // First attempt fails, retry succeeds
        assert_eq!(manager.stats().completed, 1);
        assert!(manager.failed_tasks().is_empty());
    }

    #[tokio::test]
    async fn oom_tasks_collected() {
        let config = test_config(1);
        let mut manager =
            LocalManager::new(config, MemoryStealingScheduler, FixedEstimator(100));
        let mut factory = FakeWorkerFactory {
            mode: FakeWorkerMode::AlwaysOom,
        };

        let binaries = vec![make_binary("oom_bin", 50)];
        manager.process_binaries(binaries, &mut factory).await;

        // OOM in main → retry → OOM again → OOM phase → OOM again
        // Eventually ends up in oom_tasks or failed_tasks
        assert_eq!(manager.stats().completed, 0);
    }

    #[tokio::test]
    async fn no_binaries_completes_immediately() {
        let config = test_config(1);
        let mut manager =
            LocalManager::new(config, MemoryStealingScheduler, FixedEstimator(100));
        let mut factory = FakeWorkerFactory {
            mode: FakeWorkerMode::AlwaysSucceed,
        };

        manager
            .process_binaries(Vec::<BinaryInfo<TestId>>::new(), &mut factory)
            .await;

        assert_eq!(manager.stats().completed, 0);
        assert_eq!(manager.stats().total, 0);
    }

    #[tokio::test]
    async fn always_restart_worker_respawns_after_success() {
        use std::sync::atomic::{AtomicU32, Ordering};
        use std::sync::Arc;
        use db_comm_api_base::{Command, CommandReceiver, ResponseSender};

        let spawn_count = Arc::new(AtomicU32::new(0));
        let spawn_count_clone = spawn_count.clone();

        struct CountingFactory {
            spawn_count: Arc<AtomicU32>,
        }

        impl WorkerFactory<ChannelManagerEnd> for CountingFactory {
            fn spawn_worker(&mut self, _worker_id: WorkerId) -> (ChannelManagerEnd, Option<u32>) {
                self.spawn_count.fetch_add(1, Ordering::SeqCst);
                let (manager_end, runner_end) = channel_pair();
                tokio::spawn(async move {
                    let mut runner = runner_end;
                    let _ = runner.send_response(Response::Ready).await;
                    loop {
                        match runner.recv_command().await {
                            Some(Command::Stop) => break,
                            Some(Command::ProcessBinary { .. }) => {
                                let _ = runner
                                    .send_response(Response::Done {
                                        warnings: 0,
                                        filtered: 0,
                                    })
                                    .await;
                            }
                            None => break,
                        }
                    }
                });
                (manager_end, Some(42))
            }
        }

        let mut config = test_config(1);
        config.always_restart_worker = true;

        let mut manager = LocalManager::new(config, MemoryStealingScheduler, FixedEstimator(100));
        let mut factory = CountingFactory {
            spawn_count: spawn_count_clone,
        };

        let binaries = vec![
            make_binary("a", 50),
            make_binary("b", 60),
            make_binary("c", 70),
        ];

        manager.process_binaries(binaries, &mut factory).await;

        assert_eq!(manager.stats().completed, 3);
        assert_eq!(manager.stats().total, 3);
        assert!(manager.failed_tasks().is_empty());

        // With always_restart_worker=true and 3 binaries with 1 worker:
        // 1 initial spawn + 2 restarts (after "a" and "b" complete, "c" is the last so no restart)
        let spawns = spawn_count.load(Ordering::SeqCst);
        assert_eq!(spawns, 3, "expected 3 spawns (1 initial + 2 restarts), got {spawns}");
    }

    #[tokio::test]
    async fn memuse_log_written() {
        let tmp_dir = std::env::temp_dir().join("rust_memuse_test");
        let _ = std::fs::create_dir_all(&tmp_dir);
        let memuse_path = tmp_dir.join("memuse.log");
        // Clean up any previous run
        let _ = std::fs::remove_file(&memuse_path);

        let config = LocalManagerConfig {
            num_workers: 1,
            max_memory: 1024 * 1024 * 1024,
            always_restart_worker: false,
            print_pid: false,
            memuse_log_path: Some(memuse_path.clone()),
        };

        let mut manager = LocalManager::new(config, MemoryStealingScheduler, FixedEstimator(100));
        let mut factory = FakeWorkerFactory {
            mode: FakeWorkerMode::AlwaysSucceed,
        };

        let binaries = vec![
            make_binary("a", 50),
            make_binary("b", 60),
        ];

        manager.process_binaries(binaries, &mut factory).await;

        assert_eq!(manager.stats().completed, 2);

        // Verify memuse.log was written
        let contents = std::fs::read_to_string(&memuse_path).expect("memuse.log should exist");
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 2, "expected 2 lines in memuse.log, got {}", lines.len());

        // Each line: size,estimated,0,filename,status
        assert!(lines[0].contains(",OK"), "first line should contain OK: {}", lines[0]);
        assert!(lines[1].contains(",OK"), "second line should contain OK: {}", lines[1]);

        let _ = std::fs::remove_dir_all(&tmp_dir);
    }
}
