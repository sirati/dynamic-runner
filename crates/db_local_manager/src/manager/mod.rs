use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::Duration;

use db_comm_api_base::{
    BinaryInfo, FailedTask, Identifier, ResourceKind, ResourceMap, WorkerId,
};
use db_manager_runner_comm::ManagerEndpoint;
use db_scheduler_api::{
    ResourceEstimator, Scheduler,
};
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
    /// Successful per-task opaque payloads, surfaced for the Python-side
    /// task-specific aggregator. Populated as TaskCompleted events arrive.
    task_payloads: Vec<(BinaryInfo<I>, Option<Vec<u8>>)>,
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
    pub fn task_payloads(&self) -> &[(BinaryInfo<I>, Option<Vec<u8>>)] {
        &self.task_payloads
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
}

mod events;
mod monitor;
mod phases;
mod worker_loop;

#[cfg(test)]
mod test_helpers;

#[cfg(test)]
mod tests;
