use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use dynrunner_core::{TaskInfo, Identifier, ResourceMap};
use dynrunner_protocol_primary_secondary::SecondaryTransport;
use dynrunner_scheduler_api::{
    ResourceEstimator, Scheduler, WorkerBudgetInfo,
};

use crate::state::SecondaryConnectionState;

/// Configuration for the primary coordinator.
pub struct PrimaryConfig {
    pub node_id: String,
    pub num_secondaries: u32,
    pub connect_timeout: Duration,
    pub peer_timeout: Duration,
    /// Cadence at which the operational loop checks for missed keepalives
    /// from secondaries. A secondary is declared dead after
    /// `keepalive_miss_threshold * keepalive_interval` of silence.
    pub keepalive_interval: Duration,
    /// Number of missed keepalives that constitute a death (default 3).
    pub keepalive_miss_threshold: u32,
}

impl Default for PrimaryConfig {
    fn default() -> Self {
        Self {
            node_id: "primary".into(),
            num_secondaries: 1,
            connect_timeout: Duration::from_secs(600),
            peer_timeout: Duration::from_secs(300),
            keepalive_interval: Duration::from_secs(5),
            keepalive_miss_threshold: 3,
        }
    }
}

/// Virtual worker tracked by the authoritative primary for each remote worker.
#[derive(Debug, Clone)]
pub(super) struct RemoteWorkerState<I: Identifier> {
    pub(super) worker_id: u32,
    pub(super) secondary_id: String,
    pub(super) resource_budgets: ResourceMap,
    pub(super) current_task: Option<TaskInfo<I>>,
    pub(super) estimated_resources: ResourceMap,
    pub(super) is_idle: bool,
}

impl<I: Identifier> RemoteWorkerState<I> {
    pub(super) fn budget_info(&self) -> WorkerBudgetInfo<I> {
        WorkerBudgetInfo {
            worker_id: self.worker_id,
            reserved_budgets: self.resource_budgets.clone(),
            actual_usage: ResourceMap::new(),
            is_idle: self.is_idle,
            is_opportunistic: false,
            has_initial_assignment: self.current_task.is_some(),
            current_task: self.current_task.clone(),
            estimated_usage: self.estimated_resources.clone(),
        }
    }
}

/// The primary coordinator: orchestrates work across secondaries.
///
/// Generic over `T: SecondaryTransport<I>` so it works with both QUIC connections
/// and in-process channels for testing.
pub struct PrimaryCoordinator<T: SecondaryTransport<I>, S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> {
    pub(super) config: PrimaryConfig,
    pub(super) transport: T,
    pub(super) scheduler: S,
    pub(super) estimator: E,

    // Secondary state
    pub(super) secondaries: HashMap<String, SecondaryConnectionState>,

    // Worker tracking (virtual workers across all secondaries)
    pub(super) workers: Vec<RemoteWorkerState<I>>,

    // Task state
    pub(super) total_tasks: usize,
    pub(super) all_binaries: Vec<TaskInfo<I>>,
    pub(super) pending_binaries: Vec<TaskInfo<I>>,
    pub(super) completed_tasks: HashSet<String>,
    pub(super) failed_tasks: HashSet<String>,

    // Per-secondary last-keepalive tracking for failover detection (F1).
    pub(super) secondary_keepalives: HashMap<String, Instant>,

    // SLURM-primary promotion
    pub(super) slurm_primary_id: Option<String>,

    // Stage-file notifications queued before `run()` (or during init,
    // before secondary connections are up). Flushed once the welcome
    // + peer-connect handshake completes — at that point `send_to`
    // can route to a known secondary. Each entry is
    // (secondary_id, file_hash, src_path, dest_path).
    pub(super) pending_stage_files: Vec<(String, String, String, String)>,
}

impl<T: SecondaryTransport<I>, S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<T, S, E, I> {
    pub fn new(config: PrimaryConfig, transport: T, scheduler: S, estimator: E) -> Self {
        Self {
            config,
            transport,
            scheduler,
            estimator,
            secondaries: HashMap::new(),
            workers: Vec::new(),
            total_tasks: 0,
            all_binaries: Vec::new(),
            pending_binaries: Vec::new(),
            completed_tasks: HashSet::new(),
            failed_tasks: HashSet::new(),
            secondary_keepalives: HashMap::new(),
            slurm_primary_id: None,
            pending_stage_files: Vec::new(),
        }
    }

    /// Queue a `StageFile` notification to be sent to `secondary_id`
    /// once the secondary handshake completes. Must be called BEFORE
    /// `run()` (or from outside the run-loop) — once flushed,
    /// subsequent calls happen inline via `notify_stage_file`.
    pub fn queue_stage_file(
        &mut self,
        secondary_id: String,
        file_hash: String,
        src_path: String,
        dest_path: String,
    ) {
        self.pending_stage_files
            .push((secondary_id, file_hash, src_path, dest_path));
    }

    pub fn completed_count(&self) -> usize {
        self.completed_tasks.len()
    }

    pub fn failed_count(&self) -> usize {
        self.failed_tasks.len()
    }

    pub fn secondary_count(&self) -> usize {
        self.secondaries.len()
    }

    /// Run the full coordination pipeline.
    pub async fn run(&mut self, binaries: Vec<TaskInfo<I>>) -> Result<(), String> {
        self.all_binaries = binaries.clone();
        self.pending_binaries = binaries;
        self.total_tasks = self.pending_binaries.len();
        let total = self.total_tasks;
        tracing::info!(total, num_secondaries = self.config.num_secondaries, "primary starting");

        // Phase 1+2: Wait for all secondaries to send welcome + cert exchange
        self.wait_for_connections().await?;

        // Phase 3: Send peer lists
        self.send_peer_lists().await?;

        // Phase 4: Wait for peer connections (skip for single secondary)
        self.wait_for_peer_connections().await?;

        // Phase 4.5: Flush any queued StageFile notifications. Done
        // after handshake (so send_to has a target) and before
        // initial assignment (so the secondary's ExtractionCache is
        // primed before any TaskAssignment for those hashes).
        self.flush_pending_stage_files().await?;

        // Phase 5: Initial assignment
        self.perform_initial_assignment().await?;

        // Phase 6: Send transfer complete
        self.send_transfer_complete().await?;

        // Phase 7: Promote SLURM-primary
        self.promote_slurm_primary().await?;

        // Phase 8: Send full task list to SLURM-primary
        self.send_full_task_list().await?;

        // Phase 9: Operational loop
        self.operational_loop().await?;

        tracing::info!(
            completed = self.completed_tasks.len(),
            failed = self.failed_tasks.len(),
            total,
            "primary finished"
        );

        Ok(())
    }
}

mod assignment;
mod connect;
mod heartbeat;
mod lifecycle;
mod peer_setup;
mod staging;
mod task;
pub mod wire;

#[cfg(test)]
mod test_helpers;

#[cfg(test)]
mod tests;

