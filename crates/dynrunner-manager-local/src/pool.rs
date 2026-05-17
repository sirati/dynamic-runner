use dynrunner_core::{Identifier, ResourceMap, TypeId, WorkerId};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_scheduler_api::{ResourcePressureDecision, Scheduler, WorkerBudgetInfo};
use tokio::sync::mpsc;

use crate::manager::WorkerFactory;
use crate::worker::{WorkerEvent, WorkerHandle};

/// Result of a resource pressure check — tells the caller what happened so it
/// can take manager-specific action (requeue task, report to primary, etc.).
pub enum ResourcePressureResult<I: Identifier> {
    /// A worker was killed. The caller should handle the displaced binary
    /// (e.g. requeue locally or report failure to primary).
    ///
    /// `binary` is boxed because `TaskInfo<I>` is large enough that the
    /// inlined variant blew this enum out to ~236 bytes against
    /// `NoAction`'s zero (clippy::large_enum_variant). Consumers unbox
    /// once when passing the displaced task to its next destination.
    Killed {
        worker_id: WorkerId,
        binary: Option<Box<dynrunner_core::TaskInfo<I>>>,
        reason: String,
    },
    /// No action needed — resources are within limits.
    NoAction,
}

/// Shared worker pool used by both `LocalManager` and `SecondaryCoordinator`.
///
/// Owns the workers, the event channel, and provides lifecycle operations
/// (initialize, restart, OOM check, stop). Does NOT own scheduling decisions
/// or task queues — those remain with the specific manager.
pub struct WorkerPool<M: ManagerEndpoint, I: Identifier> {
    pub workers: Vec<WorkerHandle<M, I>>,
    event_tx: mpsc::UnboundedSender<WorkerEvent<I>>,
    event_rx: mpsc::UnboundedReceiver<WorkerEvent<I>>,
}

impl<M: ManagerEndpoint + 'static, I: Identifier> Default for WorkerPool<M, I> {
    fn default() -> Self {
        Self::new()
    }
}

impl<M: ManagerEndpoint + 'static, I: Identifier> WorkerPool<M, I> {
    pub fn new() -> Self {
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        Self {
            workers: Vec::new(),
            event_tx,
            event_rx,
        }
    }

    /// Shared event sender — needed when constructing WorkerHandles externally.
    pub fn event_tx(&self) -> &mpsc::UnboundedSender<WorkerEvent<I>> {
        &self.event_tx
    }

    /// Receive the next worker event (async, blocks until one arrives).
    pub async fn recv_event(&mut self) -> Option<WorkerEvent<I>> {
        self.event_rx.recv().await
    }

    /// Initialize N workers using the factory, assigning budgets via the scheduler.
    /// Returns an error if any spawn fails — the caller should abort the run.
    pub async fn initialize<S: Scheduler<I>>(
        &mut self,
        num_workers: u32,
        max_resources: &ResourceMap,
        scheduler: &S,
        factory: &mut impl WorkerFactory<M>,
        print_pid: bool,
    ) -> Result<(), String> {
        for i in 0..num_workers {
            let (transport, pid) = factory
                .spawn_worker(i)
                .map_err(|e| format!("failed to spawn worker {i}: {e}"))?;
            if print_pid
                && let Some(pid) = pid
            {
                tracing::info!(worker_id = i, pid, "worker PID");
            }
            let mut handle = WorkerHandle::new(i, transport, self.event_tx.clone());
            handle.pid = pid;
            let budget = scheduler.initial_budget(i, max_resources);
            let budget_mb = budget.get(&dynrunner_core::ResourceKind::memory()) / (1024 * 1024);
            handle.reserved_budgets = budget;
            tracing::info!(
                worker_id = i,
                budget_mb,
                "worker created"
            );
            self.workers.push(handle);
        }

        self.wait_for_all_ready().await;
        Ok(())
    }

    /// Block until every worker has reported Ready.
    pub async fn wait_for_all_ready(&mut self) {
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

    /// Restart a single worker: stop the old one, spawn a fresh transport,
    /// preserve budget and assignment_failure_count, wait for Ready.
    /// Returns an error if the spawn fails — the caller decides how to react
    /// (typically: log, mark the slot dead, continue with remaining workers).
    ///
    /// Preserves the worker's recorded `loaded_type_id` across the
    /// respawn: if the slot was bound to a particular `TypeId` before
    /// the restart, the replacement is spawned via
    /// `WorkerFactory::spawn_worker_for_type` for the same type so the
    /// fresh subprocess's argv matches. Without this, the next
    /// `ensure_worker_for_type` would see `loaded_type_id == None`,
    /// pessimistically assume mismatch, and trigger a redundant
    /// kill+respawn — turning every always_restart-driven cycle into
    /// two spawns instead of one. The fallback to `spawn_worker`
    /// preserves the legacy initial-spawn semantic for slots that
    /// were never bound (e.g. a restart triggered before any
    /// assignment landed).
    pub async fn restart_worker(
        &mut self,
        worker_id: WorkerId,
        factory: &mut impl WorkerFactory<M>,
        print_pid: bool,
    ) -> Result<(), String> {
        let old = &mut self.workers[worker_id as usize];
        if !old.is_stopped() {
            old.stop().await;
        }

        let preserved_type = self.workers[worker_id as usize].loaded_type_id.clone();
        let (transport, pid) = match &preserved_type {
            Some(type_id) => factory
                .spawn_worker_for_type(worker_id, type_id)
                .map_err(|e| {
                    format!("failed to respawn worker {worker_id} for type {type_id}: {e}")
                })?,
            None => factory
                .spawn_worker(worker_id)
                .map_err(|e| format!("failed to respawn worker {worker_id}: {e}"))?,
        };
        if print_pid
            && let Some(pid) = pid
        {
            tracing::info!(worker_id, pid, "worker PID (restart)");
        }

        let reserved_budgets = self.workers[worker_id as usize].reserved_budgets.clone();
        let failure_count = self.workers[worker_id as usize].assignment_failure_count;

        let mut handle = WorkerHandle::new(worker_id, transport, self.event_tx.clone());
        handle.pid = pid;
        handle.reserved_budgets = reserved_budgets;
        handle.assignment_failure_count = failure_count;
        handle.loaded_type_id = preserved_type;
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
        Ok(())
    }

    /// Ensure the given worker's subprocess is bound to `required_type`.
    ///
    /// Per-type subprocess dispatch primitive: when a multi-phase
    /// `TaskDefinition` declares one `TaskTypeSpec` (and therefore one
    /// `worker_module`) per phase, the worker subprocess that ran
    /// phase N's tasks cannot execute phase N+1's tasks — its argv
    /// loaded the wrong module. This call compares the worker's
    /// recorded `loaded_type_id` against the next task's `type_id`
    /// and, on mismatch, kills + respawns the slot through
    /// [`WorkerFactory::spawn_worker_for_type`] before the assignment
    /// proceeds.
    ///
    /// Same-type fast path: when the recorded `loaded_type_id`
    /// matches `required_type` (the dominant case in a single-type
    /// run), this is a no-op — no kill, no spawn, no Ready wait.
    /// The pre-existing single-type observable behaviour (one
    /// process per slot for the lifetime of the run) is preserved
    /// bit-for-bit.
    ///
    /// Empty-state path: a freshly-initialised worker has
    /// `loaded_type_id == None` because [`initialize`] cannot
    /// generally know which `TypeId` the first assignment will pick.
    /// The mismatch arm fires once on the first assignment per slot,
    /// binding the worker to that type. Subsequent same-type
    /// assignments hit the fast path.
    ///
    /// Preserves `reserved_budgets` and `assignment_failure_count`
    /// across the respawn — same contract as
    /// [`restart_worker`] — and waits for the freshly-spawned worker
    /// to reach Ready before returning.
    pub async fn ensure_worker_for_type(
        &mut self,
        worker_id: WorkerId,
        required_type: &TypeId,
        factory: &mut impl WorkerFactory<M>,
        print_pid: bool,
    ) -> Result<(), String> {
        let idx = worker_id as usize;
        if self
            .workers
            .get(idx)
            .and_then(|w| w.loaded_type_id.as_ref())
            == Some(required_type)
        {
            return Ok(());
        }

        let old = &mut self.workers[idx];
        if !old.is_stopped() {
            old.stop().await;
        }
        // Eagerly SIGKILL the prior subprocess so the type-shift
        // respawn does not race against a still-running worker
        // continuing to load the previous type's worker_module. The
        // restart-pre-respawn SIGKILL on `WorkerHandle` is the same
        // primitive `worker_loop::handle_assignment_failure`'s
        // restart path implicitly relies on via the factory's child
        // tracking; surfacing it explicitly here also covers
        // factories whose `spawn_worker_for_type` overwrites slot
        // tracking without reaping the prior `Child` (no zombie
        // race window).
        old.kill_subprocess();

        let (transport, pid) = factory
            .spawn_worker_for_type(worker_id, required_type)
            .map_err(|e| {
                format!(
                    "failed to respawn worker {worker_id} for type {required_type}: {e}"
                )
            })?;
        if print_pid
            && let Some(pid) = pid
        {
            tracing::info!(
                worker_id,
                pid,
                type_id = %required_type,
                "worker PID (type-shift respawn)"
            );
        }

        let reserved_budgets = self.workers[idx].reserved_budgets.clone();
        let failure_count = self.workers[idx].assignment_failure_count;

        let mut handle = WorkerHandle::new(worker_id, transport, self.event_tx.clone());
        handle.pid = pid;
        handle.reserved_budgets = reserved_budgets;
        handle.assignment_failure_count = failure_count;
        handle.loaded_type_id = Some(required_type.clone());
        self.workers[idx] = handle;

        // Wait for ready — same shape as `restart_worker`.
        loop {
            if self.workers[idx].is_ready() {
                break;
            }
            self.workers[idx].poll_ready().await;
            tokio::task::yield_now().await;
        }

        tracing::info!(
            worker_id,
            type_id = %required_type,
            "worker respawned for type-shift and ready"
        );
        Ok(())
    }

    /// Update actual resource usage for all workers from /proc/[pid]/statm.
    pub fn update_all_resource_usage(&mut self) {
        for worker in &mut self.workers {
            worker.update_resource_usage();
        }
    }

    /// Check resource pressure via the scheduler, kill if needed.
    ///
    /// Returns `ResourcePressureResult::Killed` with the displaced binary so
    /// the caller can decide what to do (requeue locally, report to primary, etc.).
    /// The worker is marked as killed but NOT restarted — the caller
    /// must call `restart_worker` if it wants the worker back.
    pub fn check_resource_pressure<S: Scheduler<I>>(
        &mut self,
        scheduler: &S,
        max_resources: &ResourceMap,
        in_pressure_phase: bool,
    ) -> ResourcePressureResult<I> {
        self.update_all_resource_usage();
        let infos = self.budget_infos();
        let decision = scheduler.check_resource_pressure(&infos, max_resources, in_pressure_phase);

        match decision {
            ResourcePressureDecision::Kill { worker_id, reason } => {
                tracing::warn!(
                    worker_id,
                    reason = %reason,
                    in_pressure_phase,
                    "killing worker under resource pressure"
                );
                let worker = &mut self.workers[worker_id as usize];
                let binary = worker.current_binary.take().map(Box::new);
                worker.mark_oom_killed();
                ResourcePressureResult::Killed {
                    worker_id,
                    binary,
                    reason,
                }
            }
            ResourcePressureDecision::NoAction => ResourcePressureResult::NoAction,
        }
    }

    /// Build budget info snapshots for all workers.
    pub fn budget_infos(&self) -> Vec<WorkerBudgetInfo<I>> {
        self.workers.iter().map(|w| w.budget_info()).collect()
    }

    /// Stop all workers that aren't already stopped.
    pub async fn stop_all(&mut self) {
        for worker in &mut self.workers {
            if !worker.is_stopped() {
                worker.stop().await;
                tracing::info!(worker_id = worker.worker_id, "worker stopped");
            }
        }
    }

    /// Emergency-stop every worker AND its child process tree with
    /// a SIGTERM → grace → SIGKILL ladder.
    ///
    /// Single concern: "take down every worker pgid the pool owns,
    /// with a bounded escalation so a stuck child can't block the
    /// shutdown". Used by the coordinator panik-react path; the
    /// regular shutdown path goes through `stop_all` (which sends
    /// a clean protocol Stop and lets the worker exit on its own).
    ///
    /// Sequence:
    ///   1. SIGTERM to every worker's process group in one fan-out
    ///      pass. This signals the worker AND every descendant
    ///      sharing its pgid; workers that installed a SIGTERM
    ///      handler (`runtime.py::_install_term_handler`) translate
    ///      it into a clean shutdown.
    ///   2. Sleep `grace` (bounded by the caller — typically a few
    ///      seconds). The sleep is a single `tokio::time::sleep`
    ///      across the pool rather than per-worker, so the total
    ///      wait time is `grace`, not `grace * num_workers`.
    ///   3. SIGKILL to every process group still alive. The
    ///      `process_tree_alive` probe (`kill(-pgid, 0)`) tells us
    ///      whether SIGTERM already drained the group; live ones
    ///      escalate, dead ones are skipped.
    ///
    /// Idempotent: workers without a tracked pid, workers whose
    /// process tree has already exited, and workers where pgid
    /// signalling fails for any other reason are all no-ops on
    /// each step. The pool's `workers` vec is left intact — the
    /// caller is responsible for any subsequent state mutation
    /// (e.g. setting the protocol state to Stopped).
    pub async fn kill_all_workers_with_grace(
        &self,
        grace: std::time::Duration,
    ) {
        let count = self.workers.len();
        if count == 0 {
            return;
        }
        tracing::info!(
            workers = count,
            grace_ms = grace.as_millis() as u64,
            "kill_all_workers_with_grace: sending SIGTERM to every worker pgid"
        );
        for worker in &self.workers {
            worker.sigterm_process_tree();
        }
        // Single sleep across the pool — wall-clock teardown is
        // bounded by `grace`, not `grace * num_workers`.
        tokio::time::sleep(grace).await;
        let mut escalated = 0usize;
        for worker in &self.workers {
            if worker.process_tree_alive() {
                worker.sigkill_process_tree();
                escalated += 1;
            }
        }
        if escalated > 0 {
            tracing::warn!(
                escalated,
                workers = count,
                "kill_all_workers_with_grace: SIGKILL escalation fired \
                 for {escalated}/{count} worker pgid(s) that ignored \
                 SIGTERM within the grace window"
            );
        }
    }
}
