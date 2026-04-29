use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::Duration;

use dynrunner_core::{
    FailedTask, Identifier, PhaseId, ResourceKind, ResourceMap, TaskInfo, WorkerId,
};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_scheduler_api::{PendingPool, ResourceEstimator, Scheduler};
use crate::pool::WorkerPool;
use crate::stats::ProcessingStats;

/// Per-completion context handed to a `RestartPredicate`. References borrow
/// from the manager's per-worker state and live only for the predicate call.
pub struct RestartContext<'a> {
    pub success: bool,
    pub binary_path: &'a Path,
    pub binary_size: u64,
    pub estimated_resources: &'a ResourceMap,
    pub actual_resources: &'a ResourceMap,
}

/// Decide whether to recycle a worker after a task completes. Used in
/// addition to the coarse `always_restart_worker` flag — if either is true,
/// the worker is restarted (when there's still pending work).
///
/// `Send` so that callers may construct the predicate before crossing a
/// thread boundary (e.g. `pyo3::Python::detach`); the predicate itself runs
/// on the manager's single-threaded LocalSet.
pub type RestartPredicate = Box<dyn Fn(&RestartContext<'_>) -> bool + Send>;

/// Callback invoked when a user-visible phase first enters `Active` state.
///
/// `Send` for the same reason as `RestartPredicate`. The callback runs on the
/// manager's single-threaded LocalSet so it must not block.
pub type OnPhaseStart = Box<dyn FnMut(&PhaseId) + Send>;

/// Callback invoked when a user-visible phase has fully drained (queue
/// empty, no in-flight items). The two `u32` arguments are the phase's
/// completed and failed counters as tracked by the manager.
pub type OnPhaseEnd = Box<dyn FnMut(&PhaseId, u32, u32) + Send>;

/// Configuration for the local manager.
pub struct LocalManagerConfig {
    pub num_workers: u32,
    pub max_resources: ResourceMap,
    pub always_restart_worker: bool,
    /// Optional fine-grained predicate. Considered alongside (OR'd with)
    /// `always_restart_worker`. Receives per-completion stats; returning
    /// `true` triggers a restart.
    pub restart_predicate: Option<RestartPredicate>,
    /// Maximum number of times a single binary will be retried after a
    /// recoverable failure. The first attempt counts; default `1` means
    /// "no retry" (a binary fails after the first attempt). Setting to `2`
    /// gives one retry after the initial failure, etc.
    pub retry_max_attempts: u32,
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
            restart_predicate: None,
            retry_max_attempts: 1,
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
pub struct LocalManager<M: ManagerEndpoint, S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier = ()> {
    config: LocalManagerConfig,
    scheduler: S,
    estimator: E,
    pool: WorkerPool<M, I>,
    /// Affinity-aware pending-task pool. `None` outside of an active
    /// `process_binaries` run; populated at run-start with the current
    /// batch's phase set + dependency graph and torn down at run-end.
    pending: Option<PendingPool<I>>,
    failed_tasks: Vec<FailedTask<I>>,
    resource_pressure_tasks: Vec<FailedTask<I>>,
    unassigned_tasks: Vec<TaskInfo<I>>,
    pending_worker_assignments: HashSet<WorkerId>,
    in_pressure_phase: bool,
    total_assigned_resources: ResourceMap,
    stats: ProcessingStats,
    /// Per-`PhaseId` (completed, failed) counters surfaced through the
    /// `on_phase_end` callback when a phase drains.
    phase_completion_counts: HashMap<PhaseId, (u32, u32)>,
    /// Phases for which `on_phase_start` has already fired during the
    /// current run. Prevents duplicate notifications when a phase
    /// transitions Active → Draining → Active (e.g. via `requeue`).
    phase_started: HashSet<PhaseId>,
    /// User-visible phase-lifecycle hooks installed at the start of
    /// each `process_binaries` call.
    on_phase_start_cb: Option<OnPhaseStart>,
    on_phase_end_cb: Option<OnPhaseEnd>,
    /// Successful per-task opaque payloads, surfaced for the Python-side
    /// task-specific aggregator. Populated as TaskCompleted events arrive.
    task_payloads: Vec<(TaskInfo<I>, Option<Vec<u8>>)>,
}

impl<M: ManagerEndpoint + 'static, S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> LocalManager<M, S, E, I> {
    pub fn new(config: LocalManagerConfig, scheduler: S, estimator: E) -> Self {
        Self {
            config,
            scheduler,
            estimator,
            pool: WorkerPool::new(),
            pending: None,
            failed_tasks: Vec::new(),
            resource_pressure_tasks: Vec::new(),
            unassigned_tasks: Vec::new(),
            pending_worker_assignments: HashSet::new(),
            in_pressure_phase: false,
            total_assigned_resources: ResourceMap::new(),
            stats: ProcessingStats::default(),
            phase_completion_counts: HashMap::new(),
            phase_started: HashSet::new(),
            on_phase_start_cb: None,
            on_phase_end_cb: None,
            task_payloads: Vec::new(),
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

    /// Successful per-task opaque payloads in completion order.
    pub fn task_payloads(&self) -> &[(TaskInfo<I>, Option<Vec<u8>>)] {
        &self.task_payloads
    }

    /// Main entry point: process a list of binaries through the 5-phase pipeline.
    ///
    /// `phase_deps` is the user-declared phase dependency graph; pass an
    /// empty map for a single-phase or independent-phase run. `on_phase_start`
    /// fires once per `PhaseId` the first time the pool transitions it to
    /// `Active`; `on_phase_end` fires once per `PhaseId` once the pool
    /// reports it `Drained`, with per-phase `(completed, failed)` counters.
    pub async fn process_binaries(
        &mut self,
        binaries: Vec<TaskInfo<I>>,
        phase_deps: HashMap<PhaseId, Vec<PhaseId>>,
        on_phase_start: impl FnMut(&PhaseId) + Send + 'static,
        on_phase_end: impl FnMut(&PhaseId, u32, u32) + Send + 'static,
        factory: &mut impl WorkerFactory<M>,
    ) -> Result<(), String> {
        // Snapshot the phase set from the binaries' `phase_id`s. Any phase
        // that appears as a dep but not in the items must still be in the
        // pool's phase set, so merge in dep-graph keys/values too.
        let mut phase_ids: HashSet<PhaseId> =
            binaries.iter().map(|t| t.phase_id.clone()).collect();
        for (child, parents) in &phase_deps {
            phase_ids.insert(child.clone());
            for parent in parents {
                phase_ids.insert(parent.clone());
            }
        }

        let total = binaries.len() as u32;
        self.stats.total = total;
        self.stats.completed = 0;
        self.stats.errored = 0;
        self.phase_completion_counts.clear();
        self.phase_started.clear();
        self.on_phase_start_cb = Some(Box::new(on_phase_start));
        self.on_phase_end_cb = Some(Box::new(on_phase_end));

        let pool = PendingPool::new(phase_ids, phase_deps)
            .map_err(|e| e.to_string())?;
        self.pending = Some(pool);
        self.pool_mut().extend(binaries);

        // Fire on_phase_start for any phase that started life Active.
        self.fire_on_phase_start_for_newly_active();

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

        // Surface any drain transitions accumulated during the run
        // (the drained_pending vec on the pool collects them as phases
        // empty during scheduling-side phases). Fires `on_phase_end`
        // for each newly-drained user-visible phase.
        self.process_drain_transitions();

        // Reset run-scoped state.
        self.pending = None;
        self.on_phase_start_cb = None;
        self.on_phase_end_cb = None;

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

    pub(super) fn max_resources(&self) -> &ResourceMap {
        &self.config.max_resources
    }

    /// Borrow the active pool. Panics if called outside a run.
    pub(super) fn pool_ref(&self) -> &PendingPool<I> {
        self.pending
            .as_ref()
            .expect("pending pool not initialised; called outside process_binaries")
    }

    /// Mutably borrow the active pool. Panics if called outside a run.
    pub(super) fn pool_mut(&mut self) -> &mut PendingPool<I> {
        self.pending
            .as_mut()
            .expect("pending pool not initialised; called outside process_binaries")
    }

    /// Bookkeeping for a finished task: bumps the per-phase counter and
    /// notifies the pool. Drives `on_phase_end` indirectly via the next
    /// `process_drain_transitions` flush.
    pub(super) fn record_phase_completion(&mut self, phase_id: &PhaseId, success: bool) {
        let entry = self.phase_completion_counts.entry(phase_id.clone()).or_insert((0, 0));
        if success {
            entry.0 += 1;
        } else {
            entry.1 += 1;
        }
        if let Some(pool) = self.pending.as_mut() {
            pool.on_item_finished(phase_id);
        }
    }

    /// Drain any pending phase-drained notifications from the pool, fire
    /// the `on_phase_end` callback for each (with the per-phase counters),
    /// mark the phase done, then fire `on_phase_start` for any phase that
    /// just became active as a consequence.
    pub(super) fn process_drain_transitions(&mut self) {
        let drained = match self.pending.as_mut() {
            Some(pool) => pool.poll_drain_transitions(),
            None => return,
        };
        for phase_id in drained {
            let (completed, failed) = self
                .phase_completion_counts
                .get(&phase_id)
                .copied()
                .unwrap_or((0, 0));
            if let Some(cb) = self.on_phase_end_cb.as_mut() {
                cb(&phase_id, completed, failed);
            }
            if let Some(pool) = self.pending.as_mut() {
                pool.mark_phase_done(&phase_id);
            }
        }
        self.fire_on_phase_start_for_newly_active();
    }

    /// Fire `on_phase_start` for every phase that is currently `Active`
    /// and has not yet fired its on_phase_start.
    pub(super) fn fire_on_phase_start_for_newly_active(&mut self) {
        let active = match self.pending.as_ref() {
            Some(pool) => pool.active_phases(),
            None => return,
        };
        for phase_id in active {
            if self.phase_started.insert(phase_id.clone())
                && let Some(cb) = self.on_phase_start_cb.as_mut()
            {
                cb(&phase_id);
            }
        }
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
}

mod events;
mod monitor;
mod phases;
mod worker_loop;

#[cfg(test)]
mod test_helpers;

#[cfg(test)]
mod tests;
