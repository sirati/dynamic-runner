use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use dynrunner_core::{TaskInfo, Identifier, PhaseId, ResourceMap};
use dynrunner_protocol_primary_secondary::SecondaryTransport;
use dynrunner_scheduler_api::{
    PendingPool, ResourceEstimator, Scheduler, WorkerBudgetInfo,
};

use crate::state::SecondaryConnectionState;

/// Per-phase lifecycle hook invoked by the coordinator when a phase
/// flips Blocked → Active. The pyo3 layer (Phase 5B) wires this to the
/// Python `TaskDefinition.on_phase_start` so user code can spin up
/// per-phase resources (e.g. dedicated worker pools, dataset shards)
/// before items dispatch.
pub type OnPhaseStart = Box<dyn FnMut(&PhaseId) + Send>;

/// Per-phase lifecycle hook invoked when a phase reaches Drained
/// (`queued == 0` and `in_flight == 0`). Receives the phase id, plus
/// counts of completed and failed items in that phase. The pyo3 layer
/// (Phase 5B) wires this to `TaskDefinition.on_phase_end` so user code
/// can finalise per-phase aggregates before the next phase activates.
pub type OnPhaseEnd = Box<dyn FnMut(&PhaseId, u32, u32) + Send>;

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
    /// Pre-staged source mode (`--source-already-staged`): when set,
    /// the data is bind-mounted into each secondary container at
    /// `src_network` from this gateway-side host path. No
    /// primary-driven staging or hash verification is needed. The
    /// secondary resolves files directly via `src_network/<rel>`
    /// where `<rel>` is what the primary computes by stripping this
    /// prefix from `TaskInfo.path` before sending the wire's
    /// `local_path` (see `wire_local_path`). `None` outside
    /// pre-staged mode.
    pub source_pre_staged_root: Option<std::path::PathBuf>,
    /// Whether the dispatched task items are backed by real files
    /// on the secondary's filesystem (the historical contract).
    /// When `false`, the framework passes `local_path` through to
    /// the worker as an opaque identifier — no `stat()`, no content
    /// hashing, no extraction-cache resolution. Workers that read
    /// their payload via JSON/stdin/comm-fd (not by opening a file
    /// at `TaskInfo.path`) flip this to `false` via
    /// `TaskDefinition.uses_file_based_items=False` so the framework
    /// doesn't perform load-bearing IO on a path the worker never
    /// touches.
    ///
    /// `true` outside the opt-out (default).
    pub uses_file_based_items: bool,
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
            source_pre_staged_root: None,
            uses_file_based_items: true,
        }
    }
}

impl PrimaryConfig {
    /// Compute the wire-side `local_path` for a TaskInfo. In normal
    /// mode it's the absolute path verbatim. In pre-staged mode it's
    /// the absolute path with `source_pre_staged_root` stripped, so
    /// the secondary's `src_network.join(<wire local_path>)` resolves
    /// to the in-container bind-mount path. Paths that don't sit
    /// under the root (consumer misconfiguration) pass through
    /// unchanged — the secondary's `resolve_pre_staged` then fails
    /// NonRecoverable with the offending path, surfacing the
    /// mismatch instead of silently routing the wrong file.
    pub fn wire_local_path<I: Identifier>(&self, binary: &TaskInfo<I>) -> String {
        match &self.source_pre_staged_root {
            None => binary.path.to_string_lossy().into_owned(),
            Some(root) => match binary.path.strip_prefix(root) {
                Ok(rel) => rel.to_string_lossy().into_owned(),
                Err(_) => {
                    tracing::warn!(
                        path = %binary.path.display(),
                        root = %root.display(),
                        "wire_local_path: TaskInfo path doesn't sit under \
                         source_pre_staged_root; passing through unchanged \
                         — secondary will fail NonRecoverable"
                    );
                    binary.path.to_string_lossy().into_owned()
                }
            },
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
    /// Phase-aware pending pool. Lazily initialised at `run()` start so
    /// the constructor doesn't need the phase set / dependency graph;
    /// `pool_mut()` / `pool()` accessors expose it after that. `None`
    /// before `run()` is called.
    pub(super) pending: Option<PendingPool<I>>,
    /// Canonical phase dependency graph for the run, captured at
    /// `run()` start. Sent to the SLURM-primary alongside the task
    /// list (`send_full_task_list`) so the promoted secondary can
    /// rebuild its `PendingPool` with the same phase-state machine the
    /// primary used. Empty between runs.
    pub(super) phase_deps: HashMap<PhaseId, Vec<PhaseId>>,
    pub(super) completed_tasks: HashSet<String>,
    pub(super) failed_tasks: HashSet<String>,
    /// Per-phase completion counters fed to `on_phase_end`. Incremented
    /// inside the same code paths that update `completed_tasks` /
    /// `failed_tasks`.
    pub(super) phase_completed: HashMap<PhaseId, u32>,
    pub(super) phase_failed: HashMap<PhaseId, u32>,
    /// Lifecycle hooks. `None` outside the run window or when the
    /// caller didn't supply a hook.
    pub(super) on_phase_start: Option<OnPhaseStart>,
    pub(super) on_phase_end: Option<OnPhaseEnd>,
    /// Phases that have already had `on_phase_start` fired. The pool's
    /// state machine doesn't track "did we observe this transition" —
    /// that's the manager's bookkeeping, kept here so the pool stays
    /// purely about queue + dependency state.
    pub(super) phase_started_emitted: HashSet<PhaseId>,

    // Per-secondary last-keepalive tracking for failover detection (F1).
    pub(super) secondary_keepalives: HashMap<String, Instant>,

    // SLURM-primary promotion
    pub(super) slurm_primary_id: Option<String>,

    // Stage-file notifications queued before `run()` (or during init,
    // before secondary connections are up). Flushed once the welcome
    // + peer-connect handshake completes — at that point `send_to`
    // can route to a known secondary. Each entry is
    // `(secondary_id, file_hash, content_hash, src_path, dest_path)`
    // where `file_hash` is the task identifier (cache lookup key)
    // and `content_hash` is the SHA256 of the file contents (used
    // by the secondary's staging integrity check).
    pub(super) pending_stage_files: Vec<(String, String, String, String, String)>,
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
            pending: None,
            phase_deps: HashMap::new(),
            completed_tasks: HashSet::new(),
            failed_tasks: HashSet::new(),
            phase_completed: HashMap::new(),
            phase_failed: HashMap::new(),
            on_phase_start: None,
            on_phase_end: None,
            phase_started_emitted: HashSet::new(),
            secondary_keepalives: HashMap::new(),
            slurm_primary_id: None,
            pending_stage_files: Vec::new(),
        }
    }

    /// Borrow the pending pool. Panics if called before `run()` has
    /// initialised it — every internal call site is inside the run
    /// pipeline so this is a contract violation, not a runtime path.
    pub(super) fn pool(&self) -> &PendingPool<I> {
        self.pending
            .as_ref()
            .expect("PendingPool initialised at run() start")
    }

    /// Mutably borrow the pending pool.
    pub(super) fn pool_mut(&mut self) -> &mut PendingPool<I> {
        self.pending
            .as_mut()
            .expect("PendingPool initialised at run() start")
    }

    /// Queue a `StageFile` notification to be sent to `secondary_id`
    /// once the secondary handshake completes. Must be called BEFORE
    /// `run()` (or from outside the run-loop) — once flushed,
    /// subsequent calls happen inline via `notify_stage_file`.
    pub fn queue_stage_file(
        &mut self,
        secondary_id: String,
        file_hash: String,
        content_hash: String,
        src_path: String,
        dest_path: String,
    ) {
        self.pending_stage_files
            .push((secondary_id, file_hash, content_hash, src_path, dest_path));
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
    ///
    /// `phase_deps` declares the per-phase `depends_on` graph. Items in
    /// `binaries` whose `phase_id` doesn't appear in `phase_deps` are
    /// treated as a single zero-deps phase (the framework still
    /// registers it). `on_phase_start` / `on_phase_end` fire as the
    /// pool's state machine transitions phases through
    /// `Blocked → Active → Drained → Done` — Phase 5B wires these
    /// closures to the Python `TaskDefinition` lifecycle hooks.
    pub async fn run(
        &mut self,
        binaries: Vec<TaskInfo<I>>,
        phase_deps: HashMap<PhaseId, Vec<PhaseId>>,
        on_phase_start: OnPhaseStart,
        on_phase_end: OnPhaseEnd,
    ) -> Result<(), String> {
        // Discover the phase set: union of (1) every phase referenced
        // by an item, (2) every phase mentioned as a key or parent in
        // the deps map. The pool's constructor validates that every
        // dep references a known phase.
        let mut phase_set: HashSet<PhaseId> =
            binaries.iter().map(|b| b.phase_id.clone()).collect();
        for (k, v) in &phase_deps {
            phase_set.insert(k.clone());
            for p in v {
                phase_set.insert(p.clone());
            }
        }
        // Capture the canonical phase-deps graph for the run before
        // handing it to the pool — `send_full_task_list` relays it to
        // the promoted SLURM-primary so the post-promotion pool has
        // the same dependency machine.
        self.phase_deps = phase_deps.clone();
        // PendingPool::new wants an iterator yielding owned PhaseIds.
        let pool = PendingPool::new(phase_set.clone(), phase_deps)
            .map_err(|e| format!("PendingPool: {e:?}"))?;
        self.pending = Some(pool);
        self.on_phase_start = Some(on_phase_start);
        self.on_phase_end = Some(on_phase_end);
        self.phase_completed.clear();
        self.phase_failed.clear();
        self.phase_started_emitted.clear();
        for p in &phase_set {
            self.phase_completed.insert(p.clone(), 0);
            self.phase_failed.insert(p.clone(), 0);
        }

        // Sort by size descending for better packing — same intent as
        // pre-Phase-4b. The pool preserves insertion order within a
        // bucket, so we pre-sort here and `extend` once.
        let mut sorted = binaries;
        sorted.sort_by_key(|b| std::cmp::Reverse(b.size));
        self.all_binaries = sorted.clone();
        self.total_tasks = sorted.len();
        self.pool_mut().extend(sorted);

        let total = self.total_tasks;
        tracing::info!(total, num_secondaries = self.config.num_secondaries, "primary starting");

        // Fire on_phase_start for every phase the pool initialised as
        // Active (zero-deps phases). Subsequent activations triggered
        // by `mark_phase_done` are observed via `process_phase_lifecycle`.
        self.fire_initial_phase_starts();

        // Phase 1+2: Wait for all secondaries to send welcome + cert exchange
        self.wait_for_connections().await?;

        // Phase 3: Send peer lists
        self.send_peer_lists().await?;

        // Phase 4: Wait for peer connections (skip for single secondary)
        self.wait_for_peer_connections().await?;

        // Phase 5: Initial assignment.
        // `perform_initial_assignment` now drains
        // `self.pending_stage_files` and inlines them into each
        // recipient's `InitialAssignment.staged_files`, so the
        // secondary registers them in its ExtractionCache atomically
        // with processing the per-task assignments. Replaces the
        // earlier separate `flush_pending_stage_files()` step, which
        // sent `DistributedMessage::StageFile` messages that
        // `wait_for_setup` then dropped (its match had no arm for
        // them), wedging every dispatch.
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

    /// Fire `on_phase_start` for every phase the pool currently
    /// reports as `Active` that we haven't notified yet. Idempotent:
    /// re-running visits only newly-active phases. Called once at
    /// run start (for zero-deps phases) and again from
    /// `process_phase_lifecycle` after `mark_phase_done` cascades.
    pub(super) fn fire_initial_phase_starts(&mut self) {
        let active: Vec<PhaseId> = self.pool().active_phases();
        for p in active {
            if self.phase_started_emitted.insert(p.clone())
                && let Some(cb) = self.on_phase_start.as_mut()
            {
                cb(&p);
            }
        }
    }

    /// Drive `Drained` phases through `on_phase_end` → `mark_phase_done`
    /// → newly-Active phases through `on_phase_start`. Called from
    /// the same code paths that update `completed_tasks` / `failed_tasks`
    /// (i.e. after `pool.on_item_finished` runs). The cascade keeps
    /// running until no phase is in `Drained` — phases with empty
    /// dependency chains can transition through several states in
    /// one tick.
    pub(super) fn process_phase_lifecycle(&mut self) {
        loop {
            let drained = self.pool_mut().poll_drain_transitions();
            if drained.is_empty() {
                break;
            }
            for p in &drained {
                let completed = self.phase_completed.get(p).copied().unwrap_or(0);
                let failed = self.phase_failed.get(p).copied().unwrap_or(0);
                if let Some(cb) = self.on_phase_end.as_mut() {
                    cb(p, completed, failed);
                }
                self.pool_mut().mark_phase_done(p);
            }
            // mark_phase_done may have flipped Blocked → Active for
            // dependents; emit on_phase_start for them.
            self.fire_initial_phase_starts();
        }
    }

    /// Per-completion bookkeeping shared between `handle_task_complete`
    /// and the failover path: increments per-phase counters and runs
    /// the lifecycle cascade. Decoupled so the call sites stay focused
    /// on their wire-message logic.
    pub(super) fn note_item_completed(&mut self, phase_id: &PhaseId) {
        *self.phase_completed.entry(phase_id.clone()).or_insert(0) += 1;
        self.pool_mut().on_item_finished(phase_id);
        self.process_phase_lifecycle();
    }

    /// Per-failure bookkeeping. Same shape as `note_item_completed`.
    pub(super) fn note_item_failed(&mut self, phase_id: &PhaseId) {
        *self.phase_failed.entry(phase_id.clone()).or_insert(0) += 1;
        self.pool_mut().on_item_finished(phase_id);
        self.process_phase_lifecycle();
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

