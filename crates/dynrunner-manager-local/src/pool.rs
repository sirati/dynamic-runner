use dynrunner_core::{Identifier, ResourceMap, WorkerId};
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

        let (transport, pid) = factory
            .spawn_worker(worker_id)
            .map_err(|e| format!("failed to respawn worker {worker_id}: {e}"))?;
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
