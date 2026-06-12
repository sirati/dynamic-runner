use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::Duration;

use crate::pool::WorkerPool;
use crate::stats::ProcessingStats;
use dynrunner_core::{
    COMMAND_CHANNEL_CAPACITY, FailedTask, Identifier, PhaseId, PrimaryCommand, ResourceKind,
    ResourceMap, TaskInfo, TaskOutputs, TypeId, WorkerId, compute_task_hash,
};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_scheduler_api::{PendingPool, PhaseState, ResourceEstimator, Scheduler};
use tokio::sync::mpsc as tokio_mpsc;

/// Per-completion context handed to a `RestartPredicate`. References borrow
/// from the manager's per-worker state and live only for the predicate call.
pub struct RestartContext<'a> {
    pub success: bool,
    pub binary_path: &'a Path,
    pub binary_size: u64,
    pub estimated_resources: &'a ResourceMap,
    pub actual_resources: &'a ResourceMap,
}

/// Decide whether to recycle a worker after a task completes. Forces a
/// restart even when the coarse `reuse_workers` opt-in would otherwise
/// keep the worker — if the predicate returns `true`, the worker is
/// restarted (when there's still pending work).
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
/// completed and failed counters as tracked by the manager; the final
/// argument is the phase's PUBLISHED task outputs keyed by `task_id`
/// (`{ task_id: TaskOutputs }`, from each producer's `publish_string` /
/// `publish(.., key=..)` accumulator) so the hook can read a finished
/// task's output WITHOUT a filesystem path. Empty for a phase whose
/// tasks published nothing. Uniform with the distributed primary's
/// `OnPhaseEnd` so the pyo3 bridge wires ONE callback shape.
pub type OnPhaseEnd =
    Box<dyn FnMut(&PhaseId, u32, u32, &std::collections::BTreeMap<String, TaskOutputs>) + Send>;

/// Configuration for the local manager.
pub struct LocalManagerConfig {
    pub num_workers: u32,
    pub max_resources: ResourceMap,
    /// Opt-in to reusing worker processes across tasks. Default `false`
    /// means the manager restarts the worker after every successful task
    /// (kernel page-cache locality is sacrificed for a clean per-task
    /// process). Set `true` to recycle the worker slot in place, only
    /// restarting on failure / type-shift / `restart_predicate`.
    pub reuse_workers: bool,
    /// Optional fine-grained predicate. Forces a restart even when
    /// `reuse_workers` is set. Receives per-completion stats; returning
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
    /// Master switch for the structured OOM-watcher JSON log
    /// (`target = "oom_watcher"`). When `true`, the watcher emits a
    /// heartbeat line every 10s plus delta-under-pressure and kill
    /// events. When `false` (default), the watcher still samples +
    /// drives the scheduler decision but emits no log events. Surfaced
    /// to operators via the `--log-oom-watcher` CLI flag.
    pub log_oom_watcher: bool,
    /// Run-level output directory for memprofile artifacts. When
    /// `Some(path)`, the manager constructs a
    /// [`crate::memprofile::MemProfileSampler`] whose per-task files
    /// land under
    /// `path/{task_id}.worker-{N}.memprofile.jsonl.zst`. The caller
    /// is responsible for pre-joining the `memprofile/`
    /// subdirectory onto its run output dir — the sampler does NOT
    /// inject `memprofile/` itself (single concern: it writes
    /// wherever it's told).
    ///
    /// `None` disables profiling entirely; no sampler is constructed
    /// and the assign / complete / disconnect hooks short-circuit.
    pub output_dir: Option<std::path::PathBuf>,
    /// Per-task budget cap for `PrimaryCommand::ReinjectTask`. `None`
    /// disables the cap (unbounded reinjections, the same default as
    /// the distributed primary's `PrimaryConfig::
    /// unfulfillable_reinject_max_per_task`). `Some(n)` allows at most
    /// `n` reinjections per task hash before the handler refuses with
    /// the `unfulfillable_reinject_budget_exhausted` structured-log
    /// event. The same per-handle setter (`PyPrimaryHandle::
    /// set_unfulfillable_reinject_max_per_task`) seeds this value from
    /// Python so the local backend mirrors the distributed surface.
    pub unfulfillable_reinject_max_per_task: Option<u32>,
}

impl Default for LocalManagerConfig {
    fn default() -> Self {
        Self {
            num_workers: 0,
            max_resources: ResourceMap::new(),
            reuse_workers: false,
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
            log_oom_watcher: false,
            output_dir: None,
            unfulfillable_reinject_max_per_task: None,
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
    ///
    /// `subcgroup` is the per-worker sub-cgroup leaf the pool prepared
    /// (or `None` if the pool is running without nested cgroups: graceful
    /// fallback, in-process channel test factories, operator opt-out).
    /// Subprocess-spawning factories thread the handle into a `pre_exec`
    /// closure that writes the post-fork child pid to
    /// `<subcgroup>/cgroup.procs`; factories that don't spawn OS
    /// subprocesses ignore it. The pool retains ownership of the
    /// handle for the worker's lifetime — the factory only borrows it
    /// for the duration of the spawn call.
    ///
    /// Single concern: hand the cgroup boundary across the
    /// pool/factory interface, per spawn. The factory does NOT learn
    /// anything about cgroup-v2 detection, controller probing, or
    /// `memory.max` math — those live entirely in the
    /// [`crate::cgroup`] module.
    fn spawn_worker(
        &mut self,
        worker_id: WorkerId,
        subcgroup: Option<&crate::cgroup::SubcgroupHandle>,
    ) -> Result<(M, Option<u32>), String>;

    /// Spawn (or respawn) a worker bound to a specific `TypeId`.
    ///
    /// Called by [`WorkerPool::ensure_worker_for_type`] when an
    /// upcoming task's `type_id` does not match the worker's currently
    /// loaded type — typically because a multi-phase run is
    /// transitioning into a phase whose `TaskTypeSpec` has a distinct
    /// `worker_module`. The factory is expected to look the type up in
    /// whatever per-type registry it maintains and spawn the matching
    /// argv; factories that don't distinguish per-type argv (the
    /// in-process channel-based test factories, single-type real
    /// factories) inherit the default impl that delegates to
    /// [`spawn_worker`], which keeps single-type runs and the test
    /// matrix observably unchanged.
    ///
    /// `subcgroup` has the same meaning as in [`Self::spawn_worker`].
    ///
    /// Returning an error here mirrors `spawn_worker`'s contract:
    /// the caller decides whether the slot is fatally dead (abort
    /// the run) or recoverable on the next pass.
    fn spawn_worker_for_type(
        &mut self,
        worker_id: WorkerId,
        _type_id: &TypeId,
        subcgroup: Option<&crate::cgroup::SubcgroupHandle>,
    ) -> Result<(M, Option<u32>), String> {
        self.spawn_worker(worker_id, subcgroup)
    }

    /// Tear down any OS-level worker resources the factory owns
    /// (subprocesses, process groups) at end of run.
    ///
    /// Single concern: the factory provides the teardown mechanism (HOW); the
    /// caller decides WHEN to invoke it. A factory whose `spawn_worker` minted
    /// real OS subprocesses (the subprocess factory) implements this to run
    /// its SIGTERM→grace→SIGKILL ladder over the children it tracked, so they
    /// are not leaked when the factory is dropped (a bare
    /// `std::process::Child` drop does NOT kill). In-process / channel-backed
    /// test factories own no OS subprocesses and inherit the default no-op.
    ///
    /// The caller (`Node::run`'s secondary arm) invokes this AFTER the
    /// secondary's run returns, GATED on the terminal NOT being a panik — a
    /// panik already killed every worker pgid inside the coordinator's own
    /// teardown, so a second grace ladder would only delay the `exit(137)`.
    ///
    /// `async fn` in a public trait: the returned future is deliberately NOT
    /// `Send`-bounded — it is awaited on the `Node`'s single-threaded
    /// `LocalSet` (a `!Send` task), so an auto-`Send` bound is neither
    /// available nor needed. The only implementor that does real work
    /// (`SubprocessWorkerFactory`) runs a brief blocking teardown at
    /// end-of-run; the rest inherit this no-op.
    #[allow(async_fn_in_trait)]
    async fn cleanup(&mut self) {}
}

/// The local manager: owns workers, scheduler, and the 5-phase pipeline.
///
/// Generic over `M` (the transport endpoint type) so it works with both
/// real sockets and in-process channels for testing.
/// Generic over `I` (the identifier type) so different task definitions
/// can use different identifier structures.
pub struct LocalManager<
    M: ManagerEndpoint,
    S: Scheduler<I>,
    E: ResourceEstimator<I>,
    I: Identifier = (),
> {
    pub(crate) config: LocalManagerConfig,
    scheduler: S,
    estimator: E,
    pool: WorkerPool<M, I>,
    /// Affinity-aware pending-task pool. `None` outside of an active
    /// `process_binaries` run; populated at run-start with the current
    /// batch's phase set + dependency graph and torn down at run-end.
    pending: Option<PendingPool<I>>,
    pub(crate) failed_tasks: Vec<FailedTask<I>>,
    pub(crate) resource_pressure_tasks: Vec<FailedTask<I>>,
    pub(crate) unassigned_tasks: Vec<TaskInfo<I>>,
    pending_worker_assignments: HashSet<WorkerId>,
    in_pressure_phase: bool,
    total_assigned_resources: ResourceMap,
    pub(crate) stats: ProcessingStats,
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
    /// Drain notifications observed by the manager (via
    /// `pool.poll_drain_transitions`) but not yet surfaced as
    /// `on_phase_end` because the manager still holds items for that
    /// phase in a side queue (`failed_tasks`,
    /// `resource_pressure_tasks`, or `unassigned_tasks`). The pool's
    /// `drained_pending` is one-shot — once polled it is gone — so the
    /// manager owns the deferral. Each entry is re-evaluated on the
    /// next `process_drain_transitions` call: dropped if the phase has
    /// since reactivated (a `reinject` flipped `Drained → Active`),
    /// fired if the side queues are now empty for that phase, kept
    /// otherwise.
    deferred_drain_notifications: Vec<PhaseId>,
    /// Successful per-task opaque payloads, surfaced for the Python-side
    /// task-specific aggregator. Populated as TaskCompleted events arrive.
    task_payloads: Vec<(TaskInfo<I>, Option<Vec<u8>>)>,
    /// Per-manager keyed-outputs cache for the local-mode dispatch path.
    ///
    /// Keyed by `task_id` (same shape as
    /// `ClusterState.task_outputs` in distributed mode). Populated by
    /// the [`super::manager::events`] `TaskCompleted` arm (decoding
    /// `result_data` as `TaskOutputs` JSON) BEFORE
    /// `handle_task_completed` releases dependents for dispatch.
    /// Read by [`super::manager::worker_loop::try_assign_normal`] to
    /// assemble each dispatched task's `predecessor_outputs` via the
    /// shared [`dynrunner_core::gather_predecessor_outputs`] helper.
    ///
    /// When `manager-local` runs INSIDE a secondary in distributed
    /// mode, the cache is populated by the same seam but is NOT
    /// READ by the secondary's dispatch path — the secondary does
    /// not run `try_assign_normal`; the primary writes
    /// `predecessor_outputs` directly into `TaskAssignment` and the
    /// secondary forwards it verbatim. The cache population in
    /// distributed mode is cheap-but-unused; no conditional gate.
    /// Keyed by the predecessor's full `(phase_id, task_id)` identity
    /// so the same `task_id` in two different phases caches two distinct
    /// output entries (no cross-phase collision) — mirrors the
    /// distributed primary's hash-keyed CRDT output cache, which folds
    /// `phase_id` into the hash.
    pub(crate) task_outputs_cache: HashMap<(PhaseId, String), TaskOutputs>,
    /// Per-task memory-profile sampler. `Some` iff
    /// [`LocalManagerConfig::output_dir`] was set when
    /// `process_binaries` started — sampler construction defers to
    /// the start of the run because `MemProfileSampler::spawn`
    /// requires a running tokio runtime (the `LocalManager::new`
    /// caller may not be inside one).
    ///
    /// Owns one background tokio task that ticks at the configured
    /// `sample_interval` (1 s by default), reads each active worker's
    /// cgroup memory stats, and writes zstd-framed JSONL through the
    /// sampler's writers. Drained + joined via `shutdown` at the
    /// start of the per-run teardown sequence, BEFORE the pool's
    /// `SubcgroupHandle::drop` rmdir's the leaf cgroups the sampler
    /// would otherwise still be sampling from.
    pub(crate) sampler: Option<crate::memprofile::MemProfileSampler>,

    /// Cross-thread command-channel sender — clone-source for every
    /// `PyPrimaryHandle` minted by the wrapping PyO3 layer.
    ///
    /// Both the local and the distributed backends feed the same
    /// `tokio::sync::mpsc::Sender<PrimaryCommand<I>>` wire type into
    /// `PyPrimaryHandle::from_sender`; only the receiver-side handler
    /// differs. See [`crate::manager::command_channel`] for the local
    /// per-variant dispatch.
    ///
    /// Cloned (not moved) on every `command_sender()` call so multiple
    /// handles can share one manager. The sender stays alive for the
    /// manager's full lifetime; closing only happens when the manager
    /// is dropped, at which point every outstanding handle's
    /// `send().await` returns `SendError` and the Python side surfaces
    /// `PyRuntimeError`.
    command_tx: tokio_mpsc::Sender<PrimaryCommand<I>>,

    /// Command-channel receiver. `Option` so `process_binaries` can
    /// `take()` it into a stack-local for the duration of the run
    /// (the receiver is passed by `&mut` through the phase functions
    /// down into `process_worker_loop`'s `select!`) and put it back
    /// afterwards. The single-`take` invariant catches accidental
    /// re-entrant `process_binaries` calls early.
    pub(crate) command_rx: Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,

    /// Mirror of every task ever passed through `pool.extend` /
    /// `PrimaryCommand::SpawnTasks` keyed by wire-canonical content
    /// hash. Used by the command-channel handler to resolve
    /// `PrimaryHandle::fail_permanent(hash, ...)` and
    /// `update_preferred_secondaries(hash, ...)` calls against the
    /// task's `(phase_id, task_id)` metadata without walking the
    /// pool's buckets.
    ///
    /// Persistent for the run's lifetime: never shrinks even on
    /// terminal events. The pool's buckets / blocked map / failed
    /// queues are the authoritative dispatch state; this mirror only
    /// needs to keep enough metadata to resolve a hash to a
    /// `TaskInfo<I>` clone. Outer-loop reinvocation of the 5-phase
    /// pipeline keys off `task_by_hash.len()` growth — strict
    /// monotonic growth means the break condition is a length
    /// comparison.
    pub(crate) task_by_hash: HashMap<String, TaskInfo<I>>,

    /// Per-task remaining reinjection budget. Lazily initialised on
    /// the first `PrimaryCommand::ReinjectTask` for a hash from
    /// `config.unfulfillable_reinject_max_per_task` (if `Some(n)`,
    /// the entry starts at `n`); each subsequent reinject decrements
    /// by one. When the entry hits 0 the handler refuses with the
    /// `unfulfillable_reinject_budget_exhausted` structured-log
    /// event. Mirrors the distributed primary's per-task budget map
    /// shape so the contract on the Python side is uniform across
    /// backends. Empty when `config.unfulfillable_reinject_max_per_task`
    /// is `None` (unbounded).
    pub(crate) unfulfillable_reinject_remaining: HashMap<String, u32>,
}

impl<M: ManagerEndpoint + 'static, S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier>
    LocalManager<M, S, E, I>
{
    /// Construct a manager with a freshly-minted command channel.
    /// The Rust-test surface uses this form — the test never needs to
    /// reach into the channel sender, so a self-built pair is the
    /// minimal-friction shape. PyO3 callers that need to hand the
    /// sender to a `PyPrimaryHandle` BEFORE entering
    /// `process_binaries` use [`Self::with_command_channel`] and
    /// pre-mint the pair on the Python side.
    pub fn new(config: LocalManagerConfig, scheduler: S, estimator: E) -> Self {
        let (command_tx, command_rx) =
            tokio_mpsc::channel::<PrimaryCommand<I>>(COMMAND_CHANNEL_CAPACITY);
        Self::with_command_channel(config, scheduler, estimator, command_tx, command_rx)
    }

    /// Construct a manager wired to an externally-minted command-channel
    /// pair. PyO3 callers use this so the sender can be cloned into a
    /// `PyPrimaryHandle` synchronously at `RustLocalManager.__new__`
    /// time — long before `process_binaries` actually constructs the
    /// inner `LocalManager` inside its detached tokio runtime. The
    /// receiver is held in `Self::command_rx` (Option-wrapped) and
    /// `take()`n into a stack-local by `process_binaries` for the
    /// duration of the run.
    pub fn with_command_channel(
        config: LocalManagerConfig,
        scheduler: S,
        estimator: E,
        command_tx: tokio_mpsc::Sender<PrimaryCommand<I>>,
        command_rx: tokio_mpsc::Receiver<PrimaryCommand<I>>,
    ) -> Self {
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
            deferred_drain_notifications: Vec::new(),
            task_payloads: Vec::new(),
            task_outputs_cache: HashMap::new(),
            sampler: None,
            command_tx,
            command_rx: Some(command_rx),
            task_by_hash: HashMap::new(),
            unfulfillable_reinject_remaining: HashMap::new(),
        }
    }

    /// Clone of the command-channel sender. Symmetric with
    /// `PrimaryCoordinator::command_sender` on the distributed
    /// backend — both feed the same `PyPrimaryHandle::from_sender`
    /// constructor on the PyO3 layer. Callable at any time; the
    /// returned `Sender<PrimaryCommand<I>>` stays valid as long as
    /// the manager is alive.
    pub fn command_sender(&self) -> tokio_mpsc::Sender<PrimaryCommand<I>> {
        self.command_tx.clone()
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
        on_phase_end: impl FnMut(&PhaseId, u32, u32, &std::collections::BTreeMap<String, TaskOutputs>)
        + Send
        + 'static,
        factory: &mut impl WorkerFactory<M>,
    ) -> Result<(), String> {
        // Snapshot the phase set from the binaries' `phase_id`s. Any phase
        // that appears as a dep but not in the items must still be in the
        // pool's phase set, so merge in dep-graph keys/values too.
        let mut phase_ids: HashSet<PhaseId> = binaries.iter().map(|t| t.phase_id.clone()).collect();
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
        self.deferred_drain_notifications.clear();
        self.on_phase_start_cb = Some(Box::new(on_phase_start));
        self.on_phase_end_cb = Some(Box::new(on_phase_end));

        let mut pool = PendingPool::new(phase_ids, phase_deps).map_err(|e| e.to_string())?;
        // The pool's per-task re-dispatch backoff exists for the
        // EVENT-DRIVEN dispatch loop (the distributed primary), where
        // a requeued task is otherwise re-assignable at memory speed.
        // The LocalManager's phase-sequenced loop has no such edge:
        // every failure is CHARGED by `record_result` (bounded by
        // `retry_max_attempts`), the retry channel re-runs via its own
        // phase pass, and its worker loop STOPS workers when a view is
        // empty — a hidden-under-backoff item would end the phase
        // early instead of pacing it. Disable the stamps here.
        pool.set_dispatch_backoff_params(
            std::time::Duration::ZERO,
            std::time::Duration::ZERO,
        );
        self.pending = Some(pool);
        // Mirror the initial batch into `task_by_hash` BEFORE
        // `pool.extend`. The mirror is the command-channel handler's
        // hash → TaskInfo lookup table (FailPermanent /
        // UpdatePreferredSecondaries / SpawnTasks duplicate detection
        // all key against it). Doing this before `extend` keeps the
        // invariant "every task the pool knows about is mirrored"
        // valid throughout the run; `extend` is also fallible so
        // mirroring first means a rejected batch still tracks the
        // intended-to-be-injected tasks, which is the diagnostic
        // shape we want (FailPermanent against a hash that the pool
        // rejected returns "unknown hash" today via the side queue
        // path; the mirror keeps it resolvable via a `task_by_hash`
        // lookup if a future handler wants to surface it).
        for task in &binaries {
            self.task_by_hash
                .insert(compute_task_hash(task), task.clone());
        }
        // #2 dependency-existence validation (local parity with the
        // distributed primary). Run the pool's non-mutating
        // `partition_ingest` keyed on `(phase_id, task_id)`: tasks whose
        // `task_depends_on` names a literally-absent `(phase_id,
        // task_id)` are recorded as terminal `invalid_task` failures
        // (the manager keeps running on the rest) instead of failing the
        // whole `extend` as a hard `UnknownTaskDep`. The valid subset is
        // extended; `extend`'s atomic contract is preserved there (a
        // cycle among valid tasks is still a hard error). 3a/3b
        // duplicate-id semantics are distributed-only (no peer/cluster
        // concept here); a within-batch / against-pool duplicate stays
        // the hard `extend`-side `DuplicateTaskId` it always was, so the
        // `duplicates` partition is left to `extend` to reject.
        self.ingest_partition_local(binaries)?;

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

        // Construct the per-task memory-profile sampler if the
        // operator enabled it via `config.output_dir`. Deferred to
        // here (and not `LocalManager::new`) because the sampler
        // spawns a background tokio task on the current runtime — at
        // construction time the caller may not be inside one.
        // Constructed AFTER `initialize_workers` so an error there
        // returns without leaking a sampler background task. The
        // same `output_dir.is_some()` predicate also drove
        // `mem_manager_reserved_bytes` to `Some(0)` in
        // `initialize_workers`, BUT cgroup setup may still
        // gracefully return `Ok(None)` on a host that doesn't
        // expose delegated cgroup-v2 (no v2, no memory controller,
        // non-writable subtree). In that case `WorkerHandle.subcgroup`
        // is `None` and `notify_sampler_assigned` silently skips
        // per-task profile creation — the operator sees the warn
        // line emitted at setup time and no `.jsonl.zst` files
        // appear. We construct the sampler regardless so its event
        // queue exists for non-cgroup messages (Disconnected fan-out)
        // and so the local-mode integration test can pin lifecycle
        // semantics independently of cgroup-v2 availability.
        self.sampler = self.config.output_dir.as_ref().map(|dir| {
            crate::memprofile::MemProfileSampler::spawn(crate::memprofile::MemProfileConfig::new(
                dir.clone(),
            ))
        });

        // Outer loop: every iteration runs the full 5-phase pipeline
        // and breaks when `task_by_hash` did not grow during the pass.
        // Growth happens iff a `PrimaryCommand::SpawnTasks` (issued
        // from a Python `on_phase_end` callback for the lazy-phase-
        // chain idiom) extended the task set mid-run; the pipeline
        // restart catches the new phase's initial-assignment + main +
        // retry + pressure + unassigned coverage that the just-
        // finished pass missed. Defensive cap at 1000 iterations
        // surfaces a pathological recursive `on_phase_end` as a
        // tracing::error rather than an infinite loop. Persistent
        // `task_by_hash` (no shrink on terminal events) means the
        // length comparison is the cheap correct break condition;
        // strict-monotonic-growth via `SpawnTasks` is the only path
        // that flips the inequality.
        const OUTER_LOOP_ITERATION_CAP: usize = 1000;
        for loop_iteration in 0..OUTER_LOOP_ITERATION_CAP {
            let prior_total = self.task_by_hash.len();
            self.run_initial_assignments(factory).await;
            self.run_main_phase(factory).await;
            self.run_retry_phase(factory).await;
            self.run_resource_pressure_phase(factory).await;
            self.run_unassigned_phase(factory).await;
            // Drain any commands queued during the unassigned phase's
            // tail. The worker-loop drain only fires while a worker is
            // running; once the loop exits, a late `SpawnTasks`
            // command would otherwise sit in the channel until the
            // next pass — or never if there isn't one. Pulling the
            // receiver out for one pass keeps the borrow checker happy
            // while we still hold `&mut self` for the handler.
            let mut command_rx = self
                .command_rx
                .take()
                .expect("command_rx absent; process_binaries re-entrant?");
            while let Ok(cmd) = command_rx.try_recv() {
                crate::manager::command_channel::handle_local_command(self, cmd).await;
            }
            self.command_rx = Some(command_rx);
            if self.task_by_hash.len() == prior_total {
                break;
            }
            tracing::info!(
                loop_iteration,
                prior_total,
                new_total = self.task_by_hash.len(),
                "LocalManager: spawn_tasks grew the task set during pass; \
                 restarting 5-phase pipeline"
            );
            if loop_iteration + 1 == OUTER_LOOP_ITERATION_CAP {
                tracing::error!(
                    cap = OUTER_LOOP_ITERATION_CAP,
                    new_total = self.task_by_hash.len(),
                    "LocalManager: outer pipeline restart cap exceeded; \
                     aborting to surface pathological recursive spawn"
                );
            }
        }
        // Drain + flush the sampler BEFORE `stop_all_workers` so the
        // last tick's `memory.current` reads still see the per-worker
        // cgroup leaves the pool's teardown is about to Drop-rmdir.
        // After this returns the sampler is fully gone (background
        // task joined, every writer's last frame finalised).
        if let Some(sampler) = self.sampler.take() {
            sampler.shutdown().await;
        }
        self.stop_all_workers().await;

        // Surface any drain transitions accumulated during the run.
        // Mid-run drain processing happens inside the worker loop;
        // this final flush picks up phases that drained during a
        // scheduling-phase boundary plus any deferred notifications
        // that were waiting on side-queue cleanup. End-of-run
        // semantics are "all retries exhausted": items still resident
        // in `failed_tasks` / `resource_pressure_tasks` /
        // `unassigned_tasks` are permanently failed, so deferred
        // notifications should fire regardless of side-queue
        // emptiness.
        self.flush_drain_transitions_final();

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
    pub(crate) fn pool_ref(&self) -> &PendingPool<I> {
        self.pending
            .as_ref()
            .expect("pending pool not initialised; called outside process_binaries")
    }

    /// Mutably borrow the active pool. Panics if called outside a run.
    pub(crate) fn pool_mut(&mut self) -> &mut PendingPool<I> {
        self.pending
            .as_mut()
            .expect("pending pool not initialised; called outside process_binaries")
    }

    /// Initial-batch ingest with #2 dependency-existence validation —
    /// the local-manager parity of the distributed primary's
    /// `ingest_initial_batch` (minus the distributed-only 3a/3b
    /// duplicate-abort split).
    ///
    /// Runs the pool's non-mutating `partition_ingest` keyed on
    /// `(phase_id, task_id)`:
    ///   * **duplicates** → hard error, preserving the pre-feature
    ///     behaviour (the local `extend` rejected a duplicate `task_id`
    ///     with `PendingPoolError::DuplicateTaskId`). Local mode has no
    ///     cluster to abort; a duplicate in the single ingest is a
    ///     producer bug surfaced as a `process_binaries` error.
    ///   * **invalid_deps** (#2 missing-dep) → recorded as terminal
    ///     `FailedTask { error_type: InvalidTask }` (surfaced to Python
    ///     via `manager.failed_tasks()` with the `invalid_task` kind) +
    ///     `stats.errored` bumped; their `task_id` is pre-seeded into
    ///     the pool's failed set so a valid dependent's `extend`
    ///     resolves + cascade-drops (matching the runtime cascade).
    ///   * **valid** → handed to `extend`, preserving its atomic
    ///     contract (a cycle among valid tasks is still a hard error).
    fn ingest_partition_local(&mut self, binaries: Vec<TaskInfo<I>>) -> Result<(), String> {
        let partition = self.pool_ref().partition_ingest(binaries);

        // Duplicates: hard error (no cluster-abort concept in local
        // mode). Surface the first colliding identity in the message —
        // same diagnostic shape `extend`'s `DuplicateTaskId` produced.
        if let Some((task, reason)) = partition.duplicates.first() {
            return Err(format!(
                "duplicate task identity rejected at ingest: {reason} \
                 (task_id={}, phase={})",
                task.task_id, task.phase_id
            ));
        }

        // Pre-seed the pool's failed set with the missing-dep ids so the
        // valid survivors' dep-existence + extend-time cascade stay
        // correct.
        let invalid_ids: Vec<String> = partition
            .invalid_deps
            .iter()
            .map(|(task, _)| task.task_id.clone())
            .collect();
        self.pool_mut().mark_tasks_failed(invalid_ids);

        // Record each missing-dep task as a terminal invalid_task
        // failure so it surfaces to Python and counts as errored.
        for (task, reason) in &partition.invalid_deps {
            tracing::warn!(
                task_id = %task.task_id,
                phase = %task.phase_id,
                reason = %reason,
                "task has a missing dependency; marking invalid_task"
            );
            self.failed_tasks.push(FailedTask {
                binary: task.clone(),
                error_type: dynrunner_core::ErrorType::InvalidTask {
                    reason: dynrunner_core::BoundedString::from(reason.clone()),
                },
                error_message: reason.clone(),
                retry_count: 0,
            });
            self.stats.errored += 1;
        }

        self.pool_mut()
            .extend(partition.valid)
            .map_err(|e| format!("PendingPool::extend rejected task graph: {e}"))
    }

    /// Test seam: install a pre-built [`PendingPool`] so unit tests
    /// of the per-event handlers can exercise the routing logic
    /// without bootstrapping a full `process_binaries` run.
    /// Compiled only under `#[cfg(test)]` so the dead-code lint
    /// doesn't surface it in release builds.
    #[cfg(test)]
    #[doc(hidden)]
    pub(super) fn install_pool_for_test(&mut self, pool: PendingPool<I>) {
        self.pending = Some(pool);
    }

    /// Test seam mirroring `install_pool_for_test`: stand up the
    /// memprofile sampler on a manager built outside the
    /// `process_binaries` runtime-context dance. Lets sampler-hook
    /// integration tests fire `notify_sampler_assigned` /
    /// `notify_sampler_completed` directly against a manager whose
    /// `WorkerPool` was populated by alternate means (e.g. injected
    /// `WorkerHandle::subcgroup` pointing at a tempdir-rooted fake
    /// cgroup leaf).
    #[cfg(test)]
    #[doc(hidden)]
    pub(super) fn install_sampler_for_test(
        &mut self,
        sampler: crate::memprofile::MemProfileSampler,
    ) {
        self.sampler = Some(sampler);
    }

    /// Test seam: inject a [`crate::cgroup::SubcgroupHandle`] onto an
    /// existing worker slot so the sampler-hook integration test can
    /// hand the sampler a tempdir-rooted leaf path. In production the
    /// pool's spawn site materialises the handle before
    /// `factory.spawn_worker`; tests that use the in-process channel
    /// factories never enter that code path, hence this seam.
    #[cfg(test)]
    #[doc(hidden)]
    pub(super) fn install_worker_subcgroup_for_test(
        &mut self,
        worker_id: WorkerId,
        handle: crate::cgroup::SubcgroupHandle,
    ) {
        self.pool.workers[worker_id as usize].subcgroup = Some(handle);
    }

    /// Test accessor: snapshot of `self.sampler.is_some()`. Used by
    /// the run-level smoke that asserts the manager constructs the
    /// sampler when `output_dir` is set and tears it down by the end
    /// of `process_binaries`.
    #[cfg(test)]
    #[doc(hidden)]
    pub(super) fn sampler_is_some(&self) -> bool {
        self.sampler.is_some()
    }

    /// Bookkeeping for a finished task: bumps the per-phase counter and
    /// notifies the pool. Drives `on_phase_end` indirectly via the
    /// `process_drain_transitions` call inside the worker loop (which
    /// runs immediately after every task event) and the final flush
    /// at end-of-run.
    ///
    /// `task_id` carries the per-task identifier so the pool can resolve
    /// `task_depends_on` edges; pass `None` for transient/recoverable
    /// failures (the binary will be reinjected later — dependents must
    /// not unblock yet) and `Some(id)` for successful completions. The
    /// permanent-failure cascade is owned by the retry-budget exhaustion
    /// path, which calls `on_item_failed_permanent` directly rather
    /// than going through this helper.
    pub(super) fn record_phase_completion(
        &mut self,
        phase_id: &PhaseId,
        success: bool,
        task_id: Option<&str>,
    ) {
        let entry = self
            .phase_completion_counts
            .entry(phase_id.clone())
            .or_insert((0, 0));
        if success {
            entry.0 += 1;
        } else {
            entry.1 += 1;
        }
        if let Some(pool) = self.pending.as_mut() {
            // Only forward the task_id on success — recording a per-task
            // completion against a not-yet-permanently-failed task
            // would prematurely unblock its dependents.
            let id_for_pool = if success { task_id } else { None };
            pool.on_item_finished(phase_id, id_for_pool);
        }
    }

    /// Drain any pending phase-drained notifications from the pool, fire
    /// the `on_phase_end` callback for each (with the per-phase counters),
    /// mark the phase done, then fire `on_phase_start` for any phase that
    /// just became active as a consequence.
    ///
    /// Safe to call at any point in the run loop: phases whose items
    /// still live in a manager-owned side queue (`failed_tasks`,
    /// `resource_pressure_tasks`, `unassigned_tasks`) are deferred —
    /// firing `mark_phase_done` for them would race with the upcoming
    /// `pool.reinject` (which only reactivates `Drained → Active`, not
    /// `Done → Active`). Deferred phases are re-evaluated on every
    /// subsequent call until either their side queue is empty (fire) or
    /// a `reinject` reactivates them (drop). Phases that were drained
    /// and have already been overtaken by a `reinject` (i.e. now
    /// `Active`) are likewise dropped without firing.
    pub(super) fn process_drain_transitions(&mut self) {
        if self.pending.is_none() {
            return;
        }
        // Pull any newly-drained phases from the pool and merge into the
        // manager-side deferral queue. Dedup so a phase that drains
        // twice in one loop tick fires once.
        let fresh = self.pool_mut().poll_drain_transitions();
        self.deferred_drain_notifications.extend(fresh);
        self.deferred_drain_notifications.sort();
        self.deferred_drain_notifications.dedup();

        let mut still_deferred = Vec::with_capacity(self.deferred_drain_notifications.len());
        for phase_id in std::mem::take(&mut self.deferred_drain_notifications) {
            // A `reinject` since the phase was queued may have flipped
            // it back to `Active`; drop the stale notification.
            if self.pool_ref().phase_state(&phase_id) != Some(PhaseState::Drained) {
                continue;
            }
            // Items for this phase still live in a side queue — defer.
            // The phase will be reinjected at the matching scheduling-
            // phase boundary; if its workers fail it again the pool
            // will re-emit the `Drained` transition.
            if self.phase_has_pending_side_queue_items(&phase_id) {
                still_deferred.push(phase_id);
                continue;
            }
            let (completed, failed) = self
                .phase_completion_counts
                .get(&phase_id)
                .copied()
                .unwrap_or((0, 0));
            // Gather the phase's published outputs BEFORE the
            // `&mut on_phase_end_cb` borrow (the gather is an immutable
            // `&self.task_outputs_cache` read).
            let phase_outputs = self.phase_task_outputs(&phase_id);
            if let Some(cb) = self.on_phase_end_cb.as_mut() {
                cb(&phase_id, completed, failed, &phase_outputs);
            }
            self.pool_mut().mark_phase_done(&phase_id);
        }
        self.deferred_drain_notifications = still_deferred;
        self.fire_on_phase_start_for_newly_active();
    }

    /// Gather every recorded [`TaskOutputs`] for the tasks of `phase_id`,
    /// keyed by `task_id`. The local-mode twin of
    /// `ClusterState::phase_task_outputs`: it lets the `on_phase_end` hook
    /// hand a consumer's callback the just-completed phase's PUBLISHED
    /// outputs (`publish_string` / `publish(.., key=..)`) WITHOUT a
    /// filesystem path. Reads the `(phase_id, task_id)`-keyed
    /// `task_outputs_cache` the `TaskCompleted` arm populates before
    /// dependents dispatch, so by the time a phase drains and this hook
    /// fires, its outputs are present. Returns owned clones so the
    /// callback holds no borrow against the `&mut self` manager.
    fn phase_task_outputs(
        &self,
        phase_id: &PhaseId,
    ) -> std::collections::BTreeMap<String, TaskOutputs> {
        self.task_outputs_cache
            .iter()
            .filter(|((p, _), _)| p == phase_id)
            .map(|((_, task_id), outputs)| (task_id.clone(), outputs.clone()))
            .collect()
    }

    /// `true` iff one of the manager's side queues still holds an item
    /// belonging to `phase_id`. A non-empty side queue means a
    /// `pool.reinject` is still pending for this phase, so the pool's
    /// `Drained` transition must not be promoted to `Done` yet.
    fn phase_has_pending_side_queue_items(&self, phase_id: &PhaseId) -> bool {
        self.failed_tasks
            .iter()
            .any(|t| &t.binary.phase_id == phase_id)
            || self
                .resource_pressure_tasks
                .iter()
                .any(|t| &t.binary.phase_id == phase_id)
            || self
                .unassigned_tasks
                .iter()
                .any(|t| &t.phase_id == phase_id)
    }

    /// End-of-run drain flush. Same shape as `process_drain_transitions`
    /// but ignores the side-queue deferral predicate: at this point all
    /// scheduling phases (main → retry → pressure → unassigned) have
    /// run, so any item still in a side queue is permanently failed
    /// and the phase should still surface its `on_phase_end`.
    pub(super) fn flush_drain_transitions_final(&mut self) {
        if self.pending.is_none() {
            return;
        }
        let fresh = self.pool_mut().poll_drain_transitions();
        self.deferred_drain_notifications.extend(fresh);
        self.deferred_drain_notifications.sort();
        self.deferred_drain_notifications.dedup();

        for phase_id in std::mem::take(&mut self.deferred_drain_notifications) {
            // A reinject during the run may have reactivated the phase;
            // those drains are stale and would re-emit later if they
            // re-drained. After end-of-run they cannot, so drop them.
            if self.pool_ref().phase_state(&phase_id) != Some(PhaseState::Drained) {
                continue;
            }
            let (completed, failed) = self
                .phase_completion_counts
                .get(&phase_id)
                .copied()
                .unwrap_or((0, 0));
            // Gather the phase's published outputs BEFORE the
            // `&mut on_phase_end_cb` borrow (immutable cache read).
            let phase_outputs = self.phase_task_outputs(&phase_id);
            if let Some(cb) = self.on_phase_end_cb.as_mut() {
                cb(&phase_id, completed, failed, &phase_outputs);
            }
            self.pool_mut().mark_phase_done(&phase_id);
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
        // The cgroup-nesting knob has two distinct callers here:
        //   * `Some(n)` — the SecondaryCoordinator path's classic
        //     "reserve n bytes for the manager process so a worker
        //     kernel-OOM doesn't reap it"; LocalManager doesn't
        //     surface that yet (one-process local mode shares its
        //     cgroup with its workers, so the kernel-OOM-isolation
        //     benefit is moot).
        //   * `Some(0)` — "create the cgroup leaves but don't tighten
        //     `memory.max`". This is what the memprofile sampler
        //     needs: per-worker subgroups exist (`WorkerHandle.
        //     subcgroup` becomes `Some(...)`) so the sampler can read
        //     `memory.current`, but no enforcement changes.
        // We pick the latter when memprofile is enabled (`output_dir`
        // set) and leave `None` (legacy flat layout) otherwise. The
        // sampler hooks shipped previously fire either way, but
        // without the per-worker cgroup they have nothing to read.
        //
        // When the cgroup setup itself fails (no cgroup-v2, missing
        // memory controller, non-delegated subtree, any errno from
        // the subgroup writes) the run proceeds on the flat layout:
        // `cgroup::setup_worker_cgroup` degrades EVERY failure to
        // `None` + one warn line naming the refused operation. The
        // memprofile sampler then has no per-worker leaves to read —
        // the warn line is the operator's signal to fix the
        // environment if profile files matter to them.
        let mem_manager_reserved_bytes = if self.config.output_dir.is_some() {
            Some(0)
        } else {
            None
        };
        self.pool
            .initialize(
                self.config.num_workers,
                &max,
                &self.scheduler,
                factory,
                self.config.print_pid,
                mem_manager_reserved_bytes,
            )
            .await
    }
}

mod command_channel;
mod events;
mod monitor;
mod phases;
mod sampler_hooks;
mod worker_loop;

#[cfg(test)]
mod command_channel_tests;

#[cfg(test)]
mod test_helpers;

#[cfg(test)]
mod tests;
