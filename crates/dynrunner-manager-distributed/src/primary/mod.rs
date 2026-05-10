use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use dynrunner_core::{resolve_against_root, TaskInfo, Identifier, PhaseId, ResourceMap};
use dynrunner_protocol_primary_secondary::{ClusterMutation, SecondaryTransport};
use dynrunner_scheduler_api::{
    PendingPool, ResourceEstimator, Scheduler, WorkerBudgetInfo,
};

use crate::cluster_state::ClusterState;
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

    /// Per-type global concurrency caps. When a `TypeId` is present
    /// with capacity `N`, the scheduler refuses to dispatch more than
    /// `N` items of that type concurrently across all workers.
    /// Absent type → unconstrained (the historical behaviour). Set
    /// from `TaskTypeSpec.max_concurrent` per type.
    ///
    /// Use case: cap compile-heavy phases (e.g. `cores/4`) while
    /// letting cheap IO-bound phases run at the full `--jobs` width
    /// without rewriting the estimator API.
    pub max_concurrent_per_type: HashMap<dynrunner_core::TypeId, u32>,

    /// Number of retry passes to run after the main operational loop
    /// drains. Default `1` (one retry pass; matches the local
    /// manager's `retry_max_attempts` semantics).
    ///
    /// Each pass re-injects the tasks that failed in the previous
    /// pass and runs the operational loop again. A task that fails
    /// in a pass and fails again in the next stays in `failed_tasks`
    /// permanently. Set to `0` to disable retries (every Recoverable
    /// failure is terminal — useful for fail-fast CI).
    ///
    /// Why a pass-based retry instead of per-task counter: a worker
    /// that mis-classifies a permanent error as Recoverable (EROFS,
    /// missing config, etc.) would otherwise retry the same task
    /// hundreds of times per second until the SLURM time budget
    /// expires. The pass-based model bounds the cost to one extra
    /// dispatch per failed task. Secondary-died-then-requeue
    /// (handled in `requeue_dead_secondary`) does NOT count as a
    /// failure — those tasks were never actually failed, just lost
    /// their worker.
    pub retry_max_passes: u32,

    /// Grace period after every secondary in the fleet has been
    /// declared dead (via `requeue_dead_secondary`) before the
    /// operational loop gives up and exits cleanly with the still-
    /// pending tasks marked failed. Default `30s`.
    ///
    /// Without this timer the framework idles forever when
    /// `surviving_secondaries == 0 && pool not empty` — the
    /// existing exit conditions (counter-based + pool-drained)
    /// never trip because no events arrive (no secondaries left
    /// to send TaskComplete/TaskRequest). Operator pain: have to
    /// `kill` the primary process by hand. Surfaced in tokenizer's
    /// cohort-3 runs where SSH-tunnel blips killed all 5
    /// secondaries simultaneously and the run sat idle for
    /// minutes before the operator noticed.
    ///
    /// Set to `Duration::ZERO` for fail-fast (exit at the moment
    /// the fleet first goes empty). Set to a long value if a
    /// re-sbatch path is wired into `spawn_secondary` (none today)
    /// and you want time for replacement secondaries to come up.
    pub fleet_dead_timeout: Duration,

    /// Maximum time to wait for every connected secondary to send
    /// `MeshReady` before issuing `PromotePrimary`. Secondaries
    /// emit `MeshReady` once their peer-mesh has settled (mesh
    /// formed, watchdog elapsed, or no peers were expected for
    /// single-secondary runs). Without the wait, the primary
    /// previously fired `PromotePrimary` ~750µs after every
    /// secondary completed cert-exchange — that left the
    /// promoted secondary "authoritative" against an empty peer
    /// mesh for the full per-peer dial budget (10s QUIC + 10s
    /// WSS), with every pre-mesh-formation message routed into
    /// the void. Default `60s` — comfortably larger than the
    /// secondary-side 30s peer-mesh watchdog plus a slack for
    /// scheduling jitter. Stragglers past this deadline log a
    /// warning and the run proceeds anyway (so a bug in one
    /// secondary's mesh signalling can't deadlock the entire
    /// dispatch).
    pub mesh_ready_timeout: Duration,

    /// Mass-death grace window: when ALL currently-connected
    /// secondaries appear in the dead list at the same heartbeat
    /// tick (and there are at least `mass_death_min_count` of them),
    /// infer a *correlated* cause — gateway-side SSH tunnel
    /// collapse, network partition, or similar single-point-of-
    /// failure — rather than per-secondary failures, and DEFER the
    /// requeue for this duration to give the network a chance to
    /// recover. Secondaries whose keepalives resume during the
    /// grace are silently un-deferred (the fleet is back). Only
    /// after the grace expires without recovery do we fall through
    /// to the standard `requeue_dead_secondary` death sequence.
    ///
    /// Without this, a transient ~15-30s SSH tunnel blip causes the
    /// primary to declare every secondary dead, requeue every in-
    /// flight task (often hundreds), exhaust the retry budget on
    /// the next pass (the secondaries reconnect in time but the
    /// damage is done), and surface the entire wave as
    /// `permanent_failures` — observed in tokenizer's cohort-5 z3
    /// dispatch where 197 in-flight tasks were lost to a 15-second
    /// tunnel hiccup despite the secondaries themselves being
    /// healthy.
    ///
    /// Set to `Duration::ZERO` to disable (revert to legacy
    /// behaviour where every dead secondary is requeued
    /// immediately, regardless of correlation). Default `60s` —
    /// covers the typical SSH ControlMaster reconnect window
    /// (`ServerAliveInterval=30` × 2) plus slack.
    pub mass_death_grace: Duration,

    /// Minimum number of simultaneous deaths required to trigger
    /// mass-death detection. Single-secondary runs and small
    /// fleets shouldn't bias toward "treat as correlated" — the
    /// signal is meaningful only when several secondaries are
    /// affected at once. A run with `< mass_death_min_count`
    /// connected secondaries always falls through to the standard
    /// per-secondary requeue path. Default `2`.
    pub mass_death_min_count: u32,

    /// Local source-tree root the primary uses to read file
    /// contents for the initial staging walk (content-hash + per-
    /// secondary StageFile fan-out). Threaded by every pyo3-side
    /// caller that has it (SLURM pipeline, in-process distributed
    /// manager, network primary with local secondaries) so a
    /// single field tells the manager whether it can read source
    /// files from the primary's filesystem. `None` for callers
    /// that don't (pre-staged-source mode bind-mounts the source
    /// into each secondary; `uses_file_based_items=false` makes
    /// `local_path` opaque; tests with absolute on-disk paths and
    /// fake workers that never open them).
    pub source_dir: Option<std::path::PathBuf>,
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
            max_concurrent_per_type: HashMap::new(),
            retry_max_passes: 1,
            fleet_dead_timeout: Duration::from_secs(30),
            mesh_ready_timeout: Duration::from_secs(60),
            mass_death_grace: Duration::from_secs(60),
            mass_death_min_count: 2,
            source_dir: None,
        }
    }
}

impl PrimaryConfig {
    /// Compute the wire-side `local_path` for a TaskInfo. In normal
    /// mode it's `binary.path` verbatim; in pre-staged mode it's
    /// the path's tail relative to `source_pre_staged_root`, so the
    /// secondary's `src_network.join(<wire>)` resolves to the
    /// in-container bind-mount. The three legitimate `binary.path`
    /// shapes (see [`resolve_against_root`]) collapse to the right
    /// wire form here; out-of-tree paths fall through with a warn —
    /// the secondary's `resolve_pre_staged` will then fail
    /// NonRecoverable, surfacing the misconfiguration instead of
    /// silently routing the wrong file.
    pub fn wire_local_path<I: Identifier>(&self, binary: &TaskInfo<I>) -> String {
        let Some(root) = self.source_pre_staged_root.as_deref() else {
            return binary.path.to_string_lossy().into_owned();
        };
        let resolved = resolve_against_root(&binary.path, root);
        match resolved.relative {
            Some(rel) => rel.to_string_lossy().into_owned(),
            None => {
                tracing::warn!(
                    path = %binary.path.display(),
                    resolved = %resolved.absolute.display(),
                    root = %root.display(),
                    "wire_local_path: TaskInfo path doesn't sit under \
                     source_pre_staged_root; passing through unchanged \
                     — secondary will fail NonRecoverable"
                );
                binary.path.to_string_lossy().into_owned()
            }
        }
    }
}

/// Per-secondary state for a deferred mass-death event. Recorded
/// when a correlated mass-death is detected; each subsequent
/// heartbeat tick consults it to decide whether the secondary has
/// recovered (its keepalive timestamp advanced past the
/// defer-moment one) or the grace window has expired (escalate to
/// actual death). See `PrimaryConfig.mass_death_grace`.
#[derive(Debug, Clone)]
pub(super) struct PendingMassDeath {
    /// Wall-clock instant when we deferred this secondary. Compared
    /// against `mass_death_grace` to decide whether grace has
    /// expired without recovery.
    pub(super) deferred_at: Instant,
    /// The secondary's `last_keepalive` value at the moment we
    /// deferred. The recovery test is "current keepalive
    /// timestamp > this value" — recovered means a new keepalive
    /// arrived AFTER we deferred, not just that the old one is
    /// still around.
    pub(super) last_keepalive_at_defer: Instant,
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
    /// `run()` start. Broadcast as `ClusterMutation::PhaseDepsSet`
    /// from `seed_cluster_state` so every node's `cluster_state.phase_deps`
    /// mirrors the same map; the post-promotion hydration on a
    /// secondary then reads it from there to rebuild its `PendingPool`
    /// with the same phase-state machine the primary used. Empty
    /// between runs.
    pub(super) phase_deps: HashMap<PhaseId, Vec<PhaseId>>,
    pub(super) completed_tasks: HashSet<String>,
    pub(super) failed_tasks: HashSet<String>,
    /// Per-phase completion counters fed to `on_phase_end`. Incremented
    /// inside the same code paths that update `completed_tasks` /
    /// `failed_tasks`.
    pub(super) phase_completed: HashMap<PhaseId, u32>,
    pub(super) phase_failed: HashMap<PhaseId, u32>,
    /// Currently in-flight count per `TypeId`, against
    /// `config.max_concurrent_per_type`. Incremented on dispatch
    /// (in both `assign_initial` and `assign_normal` paths),
    /// decremented on TaskComplete / TaskFailed. Capacity check
    /// is "current count + 1 <= cap" (next dispatch must fit
    /// after taking this slot).
    pub(super) in_flight_per_type: HashMap<dynrunner_core::TypeId, u32>,
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

    /// Per-secondary backoff timestamps. When a secondary returns
    /// "No idle worker available" Recoverable (its dispatch.rs
    /// `is_idle_state()` check found every worker non-idle —
    /// either real saturation or a stuck-pool state-sync issue),
    /// primary records the secondary as backpressured with an
    /// expiry timestamp; until expiry, `dispatch_to_idle_workers`
    /// and `handle_task_request` skip workers belonging to that
    /// secondary so the kickstart amplifier doesn't spin tasks
    /// against an unresponsive node. Cleared on the next
    /// successful TaskComplete from that secondary (proves it's
    /// healthy).
    pub(super) backpressured_secondaries: HashMap<String, Instant>,

    /// First moment (operational-loop iteration) where
    /// `self.secondaries` became empty while the pool still has
    /// pending work. Cleared whenever a secondary is present
    /// (handle_welcome reconnect, etc.). After
    /// `config.fleet_dead_timeout` of continuous emptiness, the
    /// operational loop exits cleanly with pending tasks moved
    /// into `failed_tasks`. See `fleet_dead_timeout` docs for the
    /// rationale.
    pub(super) fleet_dead_since: Option<Instant>,

    /// Set of secondary ids that have reported `MeshReady`. The
    /// primary's `wait_for_mesh_ready` step blocks on this set
    /// growing to the connected-secondaries set before it issues
    /// `PromotePrimary` — without that wait, the promoted
    /// secondary becomes authoritative against a still-forming
    /// peer mesh and every pre-mesh-formation message goes
    /// nowhere. Recorded by `handle_mesh_ready`; consumed by
    /// `wait_for_mesh_ready`.
    pub(super) mesh_ready_secondaries: HashSet<String>,

    /// Secondaries currently in mass-death deferred state. Populated
    /// by the heartbeat tick when a correlated mass-death event is
    /// detected (every connected secondary appears dead at the
    /// same tick). Each entry's value records the moment we
    /// deferred plus the keepalive timestamp seen at that moment;
    /// each subsequent tick checks whether the live keepalive has
    /// advanced past the defer-time keepalive (= secondary
    /// recovered) or the `mass_death_grace` window has elapsed
    /// (= escalate to actual death via `requeue_dead_secondary`).
    pub(super) pending_mass_death: HashMap<String, PendingMassDeath>,

    // primary promotion
    pub(super) primary_id: Option<String>,

    /// True after this primary has handed off authority via
    /// `PromotePrimary`. While demoted, the operational loop runs in
    /// observer mode: it still receives messages (so completion
    /// forwards keep `completed_tasks` accurate for the run-done
    /// counter check), but stops dispatching, stops kickstarting,
    /// and stops running heartbeat-driven requeue. The promoted
    /// secondary is the sole authoritative primary thereafter.
    pub(super) demoted: bool,

    // Stage-file notifications queued before `run()` (or during init,
    // before secondary connections are up). Flushed once the welcome
    // + peer-connect handshake completes — at that point `send_to`
    // can route to a known secondary. Each entry is
    // `(secondary_id, file_hash, content_hash, src_path, dest_path)`
    // where `file_hash` is the task identifier (cache lookup key)
    // and `content_hash` is the SHA256 of the file contents (used
    // by the secondary's staging integrity check).
    pub(super) pending_stage_files: Vec<(String, String, String, String, String)>,

    /// Replicated cluster ledger. The primary originates `TaskAdded`,
    /// `TaskCompleted`, `TaskFailed`, and (post-Phase-L) `TaskAssigned`
    /// mutations; each one is applied locally and broadcast so every
    /// secondary's mirror converges to the same view. CRDT semantics
    /// (idempotent under repetition + reorder within the per-task
    /// happens-before constraint) live in `cluster_state.rs`.
    pub(super) cluster_state: ClusterState<I>,
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
            in_flight_per_type: HashMap::new(),
            on_phase_start: None,
            on_phase_end: None,
            phase_started_emitted: HashSet::new(),
            secondary_keepalives: HashMap::new(),
            backpressured_secondaries: HashMap::new(),
            fleet_dead_since: None,
            mesh_ready_secondaries: HashSet::new(),
            pending_mass_death: HashMap::new(),
            primary_id: None,
            demoted: false,
            pending_stage_files: Vec::new(),
            cluster_state: ClusterState::new(),
        }
    }

    /// True iff `secondary_id` is currently in backpressure backoff
    /// (recently returned "No idle worker available" and the backoff
    /// hasn't expired). Used by both the kickstart and the
    /// TaskRequest path to skip dispatch onto unresponsive
    /// secondaries.
    pub(super) fn is_backpressured(&self, secondary_id: &str) -> bool {
        self.backpressured_secondaries
            .get(secondary_id)
            .is_some_and(|t| Instant::now() < *t)
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

    /// Drop the worker view down to the per-type-cap-eligible items.
    /// `None` for an axis means unconstrained; `Some(N)` means at
    /// most `N` items of that type can be in-flight across all
    /// workers. Items whose type's capacity is already reached are
    /// removed from the view so the scheduler never sees them.
    pub(super) fn cap_filter_view(
        &self,
        view: dynrunner_scheduler_api::WorkerView<I>,
    ) -> dynrunner_scheduler_api::WorkerView<I> {
        if self.config.max_concurrent_per_type.is_empty() {
            return view;
        }
        let caps = &self.config.max_concurrent_per_type;
        let in_flight = &self.in_flight_per_type;
        view.filter(|item| match caps.get(&item.type_id) {
            None => true,
            Some(cap) => in_flight.get(&item.type_id).copied().unwrap_or(0) < *cap,
        })
    }

    /// Account for a freshly-dispatched item against its type's
    /// concurrency budget. Paired with `release_type_slot` on
    /// TaskComplete / TaskFailed.
    pub(super) fn reserve_type_slot(&mut self, type_id: &dynrunner_core::TypeId) {
        if !self.config.max_concurrent_per_type.contains_key(type_id) {
            return;
        }
        *self.in_flight_per_type.entry(type_id.clone()).or_insert(0) += 1;
    }

    pub(super) fn release_type_slot(&mut self, type_id: &dynrunner_core::TypeId) {
        if !self.config.max_concurrent_per_type.contains_key(type_id) {
            return;
        }
        if let Some(count) = self.in_flight_per_type.get_mut(type_id) {
            *count = count.saturating_sub(1);
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

    /// Test-only inspector for the primary's replicated cluster
    /// ledger. Returns the per-state counts so tests can assert
    /// convergence with secondaries' mirrors.
    #[cfg(test)]
    pub fn cluster_state_counts_for_test(&self) -> crate::cluster_state::StateCounts {
        self.cluster_state.counts()
    }

    /// Test-only borrow of the primary's replicated cluster ledger.
    /// Lets tests read failure reasons (`TaskState::Failed.last_error`)
    /// to pin specific regression-mode error strings without parsing
    /// log output.
    #[cfg(test)]
    pub fn cluster_state_for_test(&self) -> &crate::cluster_state::ClusterState<I> {
        &self.cluster_state
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
        // handing it to the pool. `seed_cluster_state` will then
        // broadcast it as `ClusterMutation::PhaseDepsSet` so every
        // secondary's `cluster_state.phase_deps` mirrors the same
        // map — the post-promotion hydration consults it to rebuild
        // a `PendingPool` with the same dependency machine.
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
        self.pool_mut()
            .extend(sorted)
            .map_err(|e| format!("PendingPool::extend rejected task graph: {e}"))?;

        let total = self.total_tasks;
        tracing::info!(total, num_secondaries = self.config.num_secondaries, "primary starting");

        // Fire on_phase_start for every phase the pool initialised as
        // Active (zero-deps phases). Subsequent activations triggered
        // by `mark_phase_done` are observed via `process_phase_lifecycle`.
        self.fire_initial_phase_starts();

        // Trivially-empty Active phases (no items at all) need to drain
        // and cascade Done before initial assignment, otherwise their
        // `Blocked` dependents — which may hold all the run's actual
        // work — never become visible to `view_for_worker`. Triggers
        // `on_phase_end(.., 0, 0)` for each empty phase via the
        // lifecycle cascade.
        self.pool_mut().drain_empty_active_phases();
        self.process_phase_lifecycle();

        // Phase 1+2: Wait for all secondaries to send welcome + cert exchange
        self.wait_for_connections().await?;

        // Phase 3: Send peer lists
        self.send_peer_lists().await?;

        // Phase 4: Wait for peer connections (skip for single secondary)
        self.wait_for_peer_connections().await?;

        // Phase 4.5: Seed the replicated cluster ledger with `TaskAdded`
        // for every binary in the run. Every secondary applies the
        // batch and now mirrors the same `Pending` set the primary
        // sees, so subsequent CRDT broadcasts (TaskCompleted, TaskFailed,
        // and post-Phase-L TaskAssigned) compose against a coherent
        // baseline on every node.
        self.seed_cluster_state().await;

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

        // Phase 6.5: Wait for every connected secondary to report
        // its peer-mesh has settled before promoting one of them
        // to primary. Pre-fix `PromotePrimary` fired ~750µs
        // after cert-exchange completed — the promoted
        // secondary then became authoritative against a still-
        // forming peer mesh (per-peer dial budget: 10s QUIC + 10s
        // WSS) and every pre-mesh-formation peer-broadcast routed
        // into the void for the duration. Holding the promotion
        // until every secondary signals `MeshReady` (mesh formed,
        // watchdog elapsed, or single-secondary instant) is the
        // event-driven equivalent of "wait until the mesh is
        // real". Bounded by `config.mesh_ready_timeout` (warning
        // + proceed on straggler, never deadlock — a buggy
        // secondary's silence must not stall the run).
        self.wait_for_mesh_ready().await?;

        // Promote primary (atomic role-flip). The chosen secondary's
        // role-change is broadcast to every node; each node's
        // `cluster_state` mirror applies `PrimaryChanged` and each
        // node's `primary_link` re-routes operational sends. Post-
        // Phase-B the promoted secondary draws its pending pool
        // straight from `cluster_state` — no separate state-transfer
        // wire path. The continuously-replicated ledger (seeded by
        // `seed_cluster_state` and maintained by ClusterMutation
        // broadcasts) is the only source of truth.
        self.promote_primary().await?;

        // Operational loop (main pass).
        self.operational_loop().await?;

        // Phase 10: Retry pass(es). Each Recoverable / NonRecoverable
        // failure in the main pass terminated its dispatch slot and
        // landed the task hash in `failed_tasks`. Re-inject those
        // tasks and run the operational loop again so they get one
        // more chance — bounded by `config.retry_max_passes` (default
        // 1). Tasks that fail again stay permanently in
        // `failed_tasks`. Without this loop a Recoverable failure
        // either retries forever (the legacy busy-loop bug) or never
        // retries at all; the pass-based shape gives task-level
        // retry that matches the local manager's behaviour.
        self.run_retry_passes().await?;

        tracing::info!(
            completed = self.completed_tasks.len(),
            failed = self.failed_tasks.len(),
            total,
            "primary finished"
        );

        // Broadcast `RunComplete` so non-promoted secondaries on the
        // peer mesh know the run is genuinely over and can exit. Without
        // this, after a post-promotion handoff scenario, the local
        // primary disconnects but peers can't tell whether the run
        // finished or the primary just crashed — they sit in failover
        // detection holding SLURM job slots indefinitely. Idempotent on
        // re-application; failures here are non-fatal (the run already
        // succeeded, this is a cleanup signal).
        self.apply_and_broadcast_cluster_mutations(vec![ClusterMutation::RunComplete]).await;

        // Brief settle window so the broadcast lands on every
        // secondary before the dispatcher tears down its transport.
        // Without this, fast dispatcher exits race the broadcast and
        // some peers miss the signal — the symptom is leftover SLURM
        // jobs in CG state for the wrappers whose secondaries didn't
        // see RunComplete. 500ms is far more than the QUIC delivery
        // latency of an in-process / podman-bridge mesh; the cost on
        // happy-path exit is negligible.
        tokio::time::sleep(Duration::from_millis(500)).await;

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
            // Newly-Active dependents may themselves be empty (a phase
            // chain like 0→1→2→3 with all items in phase 3 cascades
            // through this branch on every iteration). Re-drain so the
            // next poll_drain_transitions catches them and the loop
            // continues; without this the cascade stops one phase
            // short and items in the final phase never dispatch.
            self.pool_mut().drain_empty_active_phases();
        }
    }

    /// Per-completion bookkeeping shared between `handle_task_complete`
    /// and the failover path: increments per-phase counters and runs
    /// the lifecycle cascade. Decoupled so the call sites stay focused
    /// on their wire-message logic.
    ///
    /// `task_id` carries the per-task identifier so the pool can resolve
    /// `task_depends_on` edges. Pass `Some(id)` for successful
    /// completions; transient failures should call `note_item_failed`
    /// instead (which suppresses the dep-resolution side-effect).
    pub(super) fn note_item_completed(
        &mut self,
        phase_id: &PhaseId,
        task_id: Option<&str>,
    ) {
        *self.phase_completed.entry(phase_id.clone()).or_insert(0) += 1;
        self.pool_mut().on_item_finished(phase_id, task_id);
        self.process_phase_lifecycle();
    }

    /// Per-failure bookkeeping. Same shape as `note_item_completed`.
    ///
    /// `task_id` is accepted for symmetry with the success path but is
    /// NOT forwarded to the pool's per-task completion ledger:
    /// recoverable failures land in `failed_tasks` and may be
    /// reinjected on a later retry pass, so dependents must remain
    /// blocked until the task either succeeds (→ `note_item_completed`
    /// with `Some(id)`) or is declared permanently failed
    /// (→ `pool.on_item_failed_permanent`, owned by the retry-budget
    /// exhaustion path). Param kept so future callers can route via a
    /// uniform helper without a signature change.
    pub(super) fn note_item_failed(
        &mut self,
        phase_id: &PhaseId,
        _task_id: Option<&str>,
    ) {
        *self.phase_failed.entry(phase_id.clone()).or_insert(0) += 1;
        self.pool_mut().on_item_finished(phase_id, None);
        self.process_phase_lifecycle();
    }
}

mod assignment;
mod connect;
mod heartbeat;
mod lifecycle;
mod peer_setup;
pub mod staging;
mod task;
pub mod wire;

#[cfg(test)]
mod test_helpers;

#[cfg(test)]
mod tests;

