use db_comm_api_base::{Identifier, ResourceMap, WorkerId};
use db_manager_runner_comm::ManagerEndpoint;
use db_scheduler_api::{ResourcePressureDecision, Scheduler, WorkerBudgetInfo};
use tokio::sync::mpsc;

use crate::manager::WorkerFactory;
use crate::worker::{WorkerEvent, WorkerHandle};

/// Result of a resource pressure check — tells the caller what happened so it
/// can take manager-specific action (requeue task, report to primary, etc.).
pub enum ResourcePressureResult<I: Identifier> {
    /// A worker was killed. The caller should handle the displaced binary
    /// (e.g. requeue locally or report failure to primary).
    Killed {
        worker_id: WorkerId,
        binary: Option<db_comm_api_base::BinaryInfo<I>>,
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
            if print_pid {
                if let Some(pid) = pid {
                    tracing::info!(worker_id = i, pid, "worker PID");
                }
            }
            let mut handle = WorkerHandle::new(i, transport, self.event_tx.clone());
            handle.pid = pid;
            let budget = scheduler.initial_budget(i, max_resources);
            let budget_mb = budget.get(&db_comm_api_base::ResourceKind::memory()) / (1024 * 1024);
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
        if print_pid {
            if let Some(pid) = pid {
                tracing::info!(worker_id, pid, "worker PID (restart)");
            }
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
                let binary = worker.current_binary.take();
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
}
