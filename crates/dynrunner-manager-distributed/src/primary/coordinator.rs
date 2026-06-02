use std::any::Any;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::task::JoinSet;

use dynrunner_core::{ErrorType, Identifier, PhaseId, ResourceMap, TaskInfo};
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, PeerTransport, SecondaryTransport,
};
use dynrunner_scheduler_api::{
    PendingPool, ResourceEstimator, Scheduler, WorkerBudgetInfo,
};
use tokio::sync::mpsc as tokio_mpsc;

use super::command_channel::{PrimaryCommand, COMMAND_CHANNEL_CAPACITY};
use super::config::{OnPhaseEnd, OnPhaseStart, PrimaryConfig};
use super::error::RunError;
use super::preferred_secondaries;
use super::respawn::{
    respawn_dispatcher_listener, RespawnBudget, RespawnEvent, RespawnOutcome, RespawnRequest,
    SecondarySpawner,
};

use crate::cluster_state::{ClusterState, OutcomeSummary};
use crate::state::SecondaryConnectionState;


/// Per-secondary state for a deferred mass-death event. Recorded
/// when a correlated mass-death is detected; each subsequent
/// heartbeat tick consults it to decide whether the secondary has
/// recovered (its keepalive timestamp advanced past the
/// defer-moment one) or the grace window has expired (escalate to
/// actual death). See `PrimaryConfig.mass_death_grace`.
#[derive(Debug, Clone)]
pub(crate) struct PendingMassDeath {
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
pub(crate) struct RemoteWorkerState<I: Identifier> {
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
/// Generic over `T: SecondaryTransport<I>` for the per-secondary
/// writer-based path (handshake, initial assignment, task fan-out) AND
/// over `P: PeerTransport<I>` for role-aware addressing that survives
/// promotion (TaskRequest relay after demotion, keepalive fan-out as
/// `Scope::AllSecondaries`). The two transports coexist for the
/// duration of the unification refactor; `transport` retires in Step
/// 11 once every call site has migrated through `peer_transport`.
pub struct PrimaryCoordinator<T: SecondaryTransport<I>, P: PeerTransport<I>, S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> {
    pub(super) config: PrimaryConfig,
    pub(super) transport: T,
    /// Role-aware mesh transport. Owns the write-through `RoleTable`
    /// cache attached to `cluster_state` at construction. Used today
    /// only for primary-bound sends after promotion (relay arm in
    /// `task::handle_task_request`) and the operational-keepalive fan-
    /// out via `Address::Broadcast(Scope::AllSecondaries)`; future
    /// steps migrate the remaining per-secondary writers off
    /// `transport`.
    pub(super) peer_transport: P,
    pub(super) scheduler: S,
    pub(super) estimator: E,

    // Secondary state
    pub(super) secondaries: HashMap<String, SecondaryConnectionState>,

    // Worker tracking (virtual workers across all secondaries)
    pub(super) workers: Vec<RemoteWorkerState<I>>,

    // Task state
    pub(super) total_tasks: usize,
    /// Number of tasks left unaccounted for at the end of the most
    /// recent `run()` call: `total - completed - failed`. Populated
    /// inside `run()` after the operational loop and the retry passes
    /// have both drained, so it reflects the final accounting that
    /// `RunError::ClusterCollapsed` carries on the wire. Zero on a
    /// clean run; `>0` on the cluster-collapse path the tokenizer hit
    /// on 2026-05-10. Reset to 0 at the start of every `run()`.
    pub(super) stranded_count: usize,
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
    /// Pre-owned in-flight ledger seeded at hydration, keyed by task
    /// hash. Each value is the `(phase_id, target_secondary_id, binary)`
    /// of an `InFlight` task this coordinator inherited from the
    /// replicated `cluster_state` rather than dispatching itself.
    ///
    /// Why it exists: the normal `TaskComplete` / `TaskFailed`
    /// counter-decrement path keys off the local `RemoteWorkerState`
    /// holding the task (`workers[*].current_task`). A pre-owned
    /// in-flight task was dispatched by a different node before this
    /// coordinator became authoritative, so no local worker holds it;
    /// when its broadcast completion lands, the worker scan finds
    /// nothing and the phase in-flight counter would never drop from
    /// N+1 to N. The handlers consult this map as a fallback so the
    /// CORRECT phase's `note_item_completed` / `note_item_failed`
    /// fires. Entries are removed on first terminal observation
    /// (idempotent with the `completed_tasks` / `failed_tasks` dedup
    /// gate). Empty for a coordinator that built its pool from a
    /// local task list rather than hydrating.
    pub(super) pre_owned_in_flight:
        HashMap<String, (PhaseId, String, TaskInfo<I>)>,
    /// Failed-task ledger keyed by task hash. The value carries the
    /// most-recent ErrorType so the dispatcher can report per-class
    /// failure counts (Recoverable → fail_retry, ResourceExhausted
    /// (memory) → fail_oom, NonRecoverable / non-memory exhaustion →
    /// fail_final) without re-scanning the task pool.
    ///
    /// A retry-success removes the entry; a retry-fail overwrites
    /// the ErrorType with the new failure's classification (the same
    /// retry can shift from Recoverable to ResourceExhausted etc.).
    /// At end-of-run, the entries that remain are the permanent
    /// failures; their ErrorType classification is the operator's
    /// post-mortem signal.
    pub(super) failed_tasks: HashMap<String, ErrorType>,
    /// Per-phase completion counters fed to `on_phase_end`. Incremented
    /// inside the same code paths that update `completed_tasks` /
    /// `failed_tasks`.
    pub(super) phase_completed: HashMap<PhaseId, u32>,
    pub(super) phase_failed: HashMap<PhaseId, u32>,
    /// Per-(phase, retry-bucket) pass counter, owned by
    /// `primary::retry_bucket`. Each entry tracks how many passes
    /// have been consumed by the matching bucket for that phase;
    /// caps are read from `config.retry_max_passes` /
    /// `config.oom_retry_max_passes`. See
    /// [`crate::primary::retry_bucket`] for the surface this is
    /// keyed against.
    pub(super) retry_passes_used:
        crate::primary::retry_bucket::RetryPassesUsed,
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

    /// Gate the operational-loop counter-based exit check while a
    /// setup-promoted secondary is still discovering items and seeding
    /// the cluster ledger. Initialised from
    /// `config.required_setup_on_promote` at `run()` start; cleared the
    /// moment either (1) the first `ClusterMutation::TaskAdded` arrives
    /// via the mirror path (proves discovery happened and seeded at
    /// least one task) or (2) `ClusterMutation::RunComplete` arrives
    /// (proves the chosen secondary legitimately discovered zero items
    /// and finished the run cleanly).
    ///
    /// Without this gate, the setup-promote path's
    /// `emit_setup_defer_handshake` leaves `total_tasks = 0` on the
    /// demoted submitter; the operational loop's exit-check
    /// (`completed + failed >= total_tasks && active_workers == 0`)
    /// trips at `0 + 0 >= 0` the moment the demoted primary enters the
    /// loop — BEFORE the chosen secondary has had a chance to run
    /// `discover_items` and broadcast its first `TaskAdded`. Step 3's
    /// reactive `total_tasks` refresh in
    /// `task::mirror_mutation_to_accounting` only fires WHEN a
    /// `TaskAdded` arrives; this gate covers the window before that
    /// first arrival. Legacy bootstrap (`required_setup_on_promote =
    /// false`) starts with `setup_pending = false`, so the gate is a
    /// strict superset of the historical exit semantics — no
    /// regression on the path where `seed_cluster_state` ran locally
    /// and `total_tasks` is non-zero at startup.
    pub(super) setup_pending: bool,

    /// Cross-thread / cross-runtime ingress for the
    /// `PrimaryHandle` PyO3 surface. Each handler is co-located
    /// with the coordinator's per-mutation semantics; the receiver
    /// is read inside the operational loop's `select!` and the
    /// sender is cloned out via `command_sender()` before `run()`
    /// starts.
    ///
    /// Held as `Option` so the operational loop can take the
    /// receiver out for the duration of the select-driven phase
    /// (Rust's borrow checker won't let us hold a `&mut Receiver`
    /// inside the same `&mut self` that the per-arm handlers need)
    /// and put it back when the loop exits. Outside the loop, the
    /// option is `Some` so cloned senders keep working between runs.
    pub(super) command_rx: Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,

    /// Sender side of the command channel, cloned to consumers via
    /// `command_sender()`. Stored on `Self` so the lifetime is tied
    /// to the coordinator — when the coordinator is dropped, all
    /// cloned senders return `SendError` on subsequent `send()`
    /// calls and the PyO3 side surfaces that as a Python exception.
    pub(super) command_tx: tokio_mpsc::Sender<PrimaryCommand<I>>,

    /// Per-task reinject counter, paired with
    /// `PrimaryConfig::unfulfillable_reinject_max_per_task`. Lazily
    /// initialised on first reinject for a hash; counts DOWN from
    /// the configured cap (so 0 means "exhausted, refuse"). The
    /// map is keyed by task hash, not task_id, because external-
    /// control callers use the hash as the canonical identifier
    /// (mirroring the rest of the wire protocol).
    pub(super) unfulfillable_reinject_remaining: HashMap<String, u32>,

    /// Peer-lifecycle dispatcher channel receiver, paired with the
    /// `lifecycle_tx` installed on `cluster_state` at construction.
    /// Taken out at `run()` start and handed to
    /// [`crate::peer_lifecycle::run_peer_lifecycle_dispatcher`] inside
    /// the operational LocalSet so the dispatcher's lifetime tracks
    /// the operational loop's tokio runtime.
    pub(super) lifecycle_rx: Option<
        tokio::sync::mpsc::UnboundedReceiver<crate::peer_lifecycle::PeerLifecycleEvent>,
    >,
    /// Consumers of peer-lifecycle events. Appended to via
    /// [`Self::register_lifecycle_listener`] before `run()` enters;
    /// `std::mem::take` moves the whole vector into the spawned
    /// dispatcher at `run()` start, after which the field is empty
    /// and any post-run `register_lifecycle_listener` calls are
    /// silently appending to a dead-letter list (no dispatcher will
    /// see them). The single-shot lifecycle is consistent with the
    /// rest of the coordinator's `run()`-once contract.
    pub(super) peer_lifecycle_listeners:
        Vec<Box<dyn crate::peer_lifecycle::LifecycleListener>>,

    /// Handle to the peer-lifecycle dispatcher task spawned at
    /// `run()` start. `Some` between the dispatcher's spawn and its
    /// abort+await at run exit; `None` outside an active run (the
    /// `cleanup_lifecycle_dispatcher` helper takes it and joins).
    ///
    /// Owning the handle is the load-bearing piece that distinguishes
    /// "dispatcher exits on its own" (the `cluster_state` drop path,
    /// which only happens when the whole coordinator drops) from
    /// "dispatcher exits when `run()` returns" (this field's
    /// abort+await). The dispatcher's input channel sender lives on
    /// `cluster_state`, so a `run()` returning Err while the
    /// coordinator object stays alive (the PyO3 wrapper keeps the
    /// handle, the SLURM pipeline may inspect it) would leave the
    /// dispatcher blocked on `rx.recv().await` forever — never seeing
    /// a closed-channel `None`. The abort fires its
    /// `JoinHandle::abort()`; the await catches the dispatcher's exit
    /// (or the `JoinError::Cancelled` outcome) so cleanup is
    /// synchronous with `run()` returning.
    pub(super) lifecycle_dispatcher_handle: Option<tokio::task::JoinHandle<()>>,

    /// Task-completion dispatcher channel receiver, paired with the
    /// `task_completed_tx` installed on `cluster_state` at construction.
    /// Taken out at `run()` start and handed to
    /// [`crate::task_completed::run_task_completed_dispatcher`] inside
    /// the operational LocalSet so the dispatcher's lifetime tracks
    /// the operational loop's tokio runtime. Mirrors `lifecycle_rx`
    /// exactly; the two dispatchers are independent modules with
    /// independent channels and independent listener vectors.
    pub(super) task_completed_rx: Option<
        tokio::sync::mpsc::UnboundedReceiver<crate::task_completed::TaskCompletedEvent>,
    >,

    /// Consumers of task-completion events. Appended to via
    /// [`Self::register_task_completed_listener`] before `run()`
    /// enters; `std::mem::take` moves the whole vector into the
    /// spawned dispatcher at `run()` start, after which the field is
    /// empty and any post-run `register_task_completed_listener` calls
    /// are silently appending to a dead-letter list. Mirrors
    /// `peer_lifecycle_listeners`.
    pub(super) task_completed_listeners:
        Vec<Box<dyn crate::task_completed::TaskCompletedListener>>,

    /// Handle to the task-completion dispatcher task spawned at
    /// `run()` start. Same shape + cleanup discipline as
    /// `lifecycle_dispatcher_handle`; the
    /// `cleanup_task_completed_dispatcher` helper takes it and joins
    /// on every exit path of `run()`.
    pub(super) task_completed_dispatcher_handle: Option<tokio::task::JoinHandle<()>>,

    /// Matcher-trigger receiver, paired with the
    /// `matcher_trigger_tx` installed on `cluster_state` at
    /// construction. Taken out at `run()` start so the operational
    /// `select!` arm can `drain_matcher_batch` against it. `None`
    /// once the loop has taken ownership; subsequent runs against the
    /// same coordinator are not supported (single-shot lifecycle).
    pub(super) matcher_trigger_rx: Option<
        tokio::sync::mpsc::UnboundedReceiver<
            crate::fulfillability_matcher::MatcherTriggerEvent,
        >,
    >,

    /// Optional consumer-supplied fulfillability matcher. `None`
    /// (the default) disables the matcher pipeline entirely — the
    /// `select!` arm collapses to `pending::<Never>` shape and never
    /// fires. `Some(m)` installs the matcher; the operational loop
    /// calls `m.should_reinject(...)` once per `Unfulfillable` task
    /// per batch of holdings-update events.
    ///
    /// Registered via [`Self::set_fulfillability_matcher`] BEFORE
    /// `run()` enters (same pre-run-only contract as
    /// `register_lifecycle_listener`; the field is `mem::take`-d into
    /// the operational loop at run start so post-run registration is
    /// silently dropped).
    pub(super) fulfillability_matcher: Option<
        Box<dyn crate::fulfillability_matcher::FulfillabilityMatcher<I>>,
    >,

    /// Monotonic identity allocator for newly spawned secondaries.
    /// Initialised to `config.num_secondaries` so the IDs the
    /// preparation phase already minted (`secondary-0..secondary-N-1`)
    /// are reserved; the first respawn returns `secondary-N`. Mutated
    /// exclusively from the operational loop via
    /// [`Self::mint_secondary_id`].
    pub(super) next_secondary_id: u32,

    /// Optional opaque handle to the deployment-mode job manager
    /// (today: `Arc<Mutex<SlurmJobManager<…>>>` parked here by the
    /// SLURM PyO3 pipeline). Stored as `Arc<dyn Any + Send + Sync>`
    /// so `manager-distributed` stays decoupled from `dynrunner-slurm`;
    /// the respawn caller downcasts at the call site. Setter is
    /// callable after preparation but before `run()` enters.
    pub(super) slurm_job_manager: Option<Arc<dyn Any + Send + Sync>>,

    /// In-flight respawn tasks. The operational `select!` loop drains
    /// finished tasks here to apply each [`respawn::RespawnOutcome`].
    /// Not cloned, snapshotted, or restored — fresh coordinators
    /// start with an empty `JoinSet`.
    pub(super) respawn_tasks: JoinSet<RespawnOutcome>,

    /// FIFO ring of completed-or-attempted respawn events, capped at
    /// [`respawn::RESPAWN_EVENTS_CAP`] entries (oldest dropped on
    /// overflow). For operator forensics and per-secondary cap
    /// consultation. Not cloned, snapshotted, or restored.
    pub(super) respawn_events: VecDeque<RespawnEvent>,

    /// Per-provider respawn implementation, supplied by the
    /// deployment layer (multi-process / SLURM). `None` disables the
    /// respawn pipeline at construction; the operational `select!`
    /// arm short-circuits (no dispatcher listener registered, no
    /// `respawn_request_rx` to poll). The trait object is `Send +
    /// Sync` so the operational arm can clone the `Arc` across
    /// `spawn_local` boundaries.
    pub(super) respawn_spawner: Option<Arc<dyn SecondarySpawner>>,

    /// Active respawn budget. `None` mirrors `respawn_spawner = None`
    /// — the policy is disabled at construction and the operational
    /// arm never consults it.
    pub(super) respawn_budget: Option<RespawnBudget>,

    /// Sender side of the dispatcher → operational-loop respawn
    /// request channel. Cloned into the registered listener at
    /// `run()` start so synchronous `on_event` calls have a place
    /// to enqueue. Held as `Option` so the channel is only
    /// constructed when the respawn policy is enabled (avoids an
    /// idle channel sitting on every coordinator).
    ///
    /// Unbounded shape so the synchronous lifecycle-dispatcher
    /// `on_event` arm never blocks and never drops: mass-death-grace
    /// finalize bursts that previously blew past a bounded cap now
    /// enqueue every death; the total-budget cap on
    /// `RespawnBudget::max_total` is what bounds the memory cost in
    /// practice (the operational loop reject-accepts beyond it).
    pub(super) respawn_request_tx:
        Option<tokio::sync::mpsc::UnboundedSender<RespawnRequest>>,

    /// Receiver side of the dispatcher → operational-loop respawn
    /// request channel. Taken out for the duration of the
    /// operational loop, the same shape as `command_rx` /
    /// `matcher_trigger_rx`. `None` outside an active loop (or
    /// when the respawn policy is disabled).
    pub(super) respawn_request_rx:
        Option<tokio::sync::mpsc::UnboundedReceiver<RespawnRequest>>,

    /// Construction-time primary endpoint and pubkey snapshot used
    /// to build [`SecondarySpawnSpec`]. The per-provider spawner
    /// adapters cache their own copies (see
    /// `PyMultiProcessSpawner` constructor) and ignore the spec's
    /// equivalent fields; carrying them on the spec keeps the trait
    /// contract honest for future providers that don't have
    /// adapter-side cache.
    pub(super) respawn_primary_endpoint: String,
    pub(super) respawn_primary_pubkey_pem: String,

    /// Dedup state for "task names a preferred secondary id we have
    /// never heard of" warnings. The validator does not own the
    /// known-secondaries set nor the task list; the call sites in
    /// `lifecycle.rs::seed_cluster_state` (initial validation) and
    /// `task.rs::handle_cluster_mutation` (post-PeerJoined revalidation)
    /// supply both per invocation. Single concern lives in
    /// [`preferred_secondaries::PreferredSecondariesValidator`].
    pub(super) preferred_secondaries_validator:
        preferred_secondaries::PreferredSecondariesValidator,

    /// Panik-watcher signal receiver. Installed via
    /// [`Self::register_panik_signal_rx`] before `run()`; `None`
    /// when the operator did not pass any `--panik-file` paths. The
    /// operational `select!` arm in
    /// `lifecycle/operational_loop.rs` reads this slot, parks on
    /// `pending().await` when None, and on `Ok(signal)` announces a
    /// self-authored `ClusterMutation::PeerRemoved { SelfDeparture }`
    /// (membership/observability only — peers LOG it, the run is not
    /// terminated on peers) then returns `RunError::PanikShutdown` for
    /// the PyO3 wrapper to translate into `std::process::exit(137)`.
    ///
    /// Unlike the secondary, the primary owns no local worker pool
    /// (workers run on secondaries, accessed remotely via the
    /// `RemoteWorkerState` ledger), so the primary's panik-react
    /// path has no `kill_all_workers_with_grace` step — just the
    /// broadcast + exit. Worker teardown is each secondary's
    /// concern; the broadcast is what tells every other node to
    /// run its own teardown.
    pub(super) panik_signal_rx:
        Option<tokio::sync::oneshot::Receiver<crate::panik_watcher::PanikSignal>>,

    /// Set by the panik arm in the operational `select!` loop when
    /// the watcher signal fires. Carries the (matched_path, reason)
    /// pair the panik handler produced.
    ///
    /// One-concern wiring identical to the secondary's `fatal_exit`
    /// pattern: the arm only WRITES this; the outer `run_pipeline`
    /// only READS. Avoids changing the inner loop's `Result<(),
    /// String>` signature into `Result<(), RunError>` (which would
    /// ripple through every `?` site, every `From<String>`
    /// conversion, and several helper methods). The outer wrapper
    /// observes a Some here after the operational loop returns Ok
    /// and translates it into `Err(RunError::PanikShutdown { … })`
    /// so the PyO3 boundary can match the structured variant and
    /// call `exit(137)`.
    pub(super) panik_outcome: Option<(std::path::PathBuf, String)>,

    /// Set by the setup-promote-deadline arm in the operational
    /// `select!` loop when the deadline fires while `setup_pending`
    /// is still true. Carries the wall-clock elapsed since
    /// operational-loop entry so the outer `run_pipeline` can surface
    /// `RunError::SetupDeadlineExpired { elapsed }` with the diagnostic
    /// duration.
    ///
    /// Same write-only/read-only discipline as `panik_outcome`: the
    /// arm WRITES, the outer wrapper READS. Avoids touching the
    /// `Result<(), String>` signature of the inner loop.
    pub(super) setup_deadline_outcome: Option<std::time::Duration>,

    /// OOM-bucket dispatch-shape gate. `true` only while a per-phase
    /// OOM retry bucket is actively reinjecting and draining; `false`
    /// otherwise. The retry-bucket primitive
    /// ([`crate::primary::retry_bucket`]) is the sole writer:
    /// it flips this `true` on `BucketKind::Oom` entry, and back to
    /// `false` on every `Ok(false)` return of the OOM bucket (no
    /// candidates left OR budget exhausted).
    ///
    /// Read by dispatch-shape sites (`dispatch_to_idle_workers`,
    /// `handle_task_request`, the operational-loop 5-min timeout arm)
    /// through the accessor [`Self::single_worker_mode`] / the
    /// composed predicate [`Self::should_skip_worker_for_dispatch`].
    /// Call sites never branch on this directly — the masking + the
    /// strict-preferred-secondaries filter live behind a single
    /// dispatch-shape pipeline so the rest of the coordinator stays
    /// agnostic to OOM-bucket semantics.
    ///
    /// User spec (2026-05-17): during the OOM bucket the retries
    /// should run with 1 worker per secondary and tasks ordered by
    /// node memory DESC so memory-pressed work gets a fresh shot
    /// against maximum RAM headroom. Concurrent normal-pass workers
    /// share the masking for the duration of the OOM bucket; this
    /// is documented as acceptable throughput tax (concurrent normal
    /// dispatch tends to share secondaries with the OOM-bucket
    /// retries anyway).
    pub(super) single_worker_mode: bool,
}

impl<T: SecondaryTransport<I>, P: PeerTransport<I>, S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<T, P, S, E, I> {
    pub fn new(
        config: PrimaryConfig,
        transport: T,
        peer_transport: P,
        scheduler: S,
        estimator: E,
    ) -> Self {
        let setup_pending = config.required_setup_on_promote;
        let (command_tx, command_rx) = tokio_mpsc::channel(COMMAND_CHANNEL_CAPACITY);
        // Peer-lifecycle dispatcher channel: built at construction so
        // the apply path on `cluster_state` has a sender to enqueue
        // through from the very first `PeerJoined`/`PeerRemoved`
        // mutation. The receiver waits on `self` until `run()`
        // spawns the dispatcher; events emitted in the interim
        // queue on the unbounded channel and drain on the first
        // dispatcher poll.
        let (lifecycle_tx, lifecycle_rx) = tokio::sync::mpsc::unbounded_channel();
        // Matcher-trigger dispatcher channel. Built at construction
        // for the same reason as `lifecycle_tx`: the apply path on
        // `cluster_state` needs a sender ready from the very first
        // `PeerResourceHoldingsUpdated` apply (E1). The receiver
        // waits on `self` until `run()` enters the operational
        // `select!` and drains it via
        // `crate::fulfillability_matcher::drain_matcher_batch`.
        let (matcher_trigger_tx, matcher_trigger_rx) =
            tokio::sync::mpsc::unbounded_channel();
        // Task-completion dispatcher channel. Same construction-time
        // motivation as `lifecycle_tx`: the apply path on
        // `cluster_state` needs a sender ready from the very first
        // `TaskCompleted`/`TaskFailed` apply. The receiver waits on
        // `self` until `run()` spawns the dispatcher; events emitted
        // in the interim queue on the unbounded channel and drain on
        // the first dispatcher poll.
        let (task_completed_tx, task_completed_rx) =
            tokio::sync::mpsc::unbounded_channel();
        // Seed the monotonic id allocator past the IDs the prep phase
        // already minted (`secondary-0..secondary-{num_secondaries - 1}`)
        // so the first respawn lands on `secondary-{num_secondaries}`.
        let next_secondary_id = config.num_secondaries;
        let mut this = Self {
            config,
            transport,
            peer_transport,
            scheduler,
            estimator,
            secondaries: HashMap::new(),
            workers: Vec::new(),
            total_tasks: 0,
            stranded_count: 0,
            all_binaries: Vec::new(),
            pending: None,
            phase_deps: HashMap::new(),
            completed_tasks: HashSet::new(),
            pre_owned_in_flight: HashMap::new(),
            failed_tasks: HashMap::new(),
            phase_completed: HashMap::new(),
            phase_failed: HashMap::new(),
            retry_passes_used: HashMap::new(),
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
            setup_pending,
            command_rx: Some(command_rx),
            command_tx,
            unfulfillable_reinject_remaining: HashMap::new(),
            lifecycle_rx: Some(lifecycle_rx),
            peer_lifecycle_listeners: Vec::new(),
            lifecycle_dispatcher_handle: None,
            task_completed_rx: Some(task_completed_rx),
            task_completed_listeners: Vec::new(),
            task_completed_dispatcher_handle: None,
            matcher_trigger_rx: Some(matcher_trigger_rx),
            fulfillability_matcher: None,
            next_secondary_id,
            slurm_job_manager: None,
            respawn_tasks: JoinSet::new(),
            respawn_events: VecDeque::new(),
            respawn_spawner: None,
            respawn_budget: None,
            respawn_request_tx: None,
            respawn_request_rx: None,
            respawn_primary_endpoint: String::new(),
            respawn_primary_pubkey_pem: String::new(),
            preferred_secondaries_validator:
                preferred_secondaries::PreferredSecondariesValidator::new(),
            panik_signal_rx: None,
            panik_outcome: None,
            setup_deadline_outcome: None,
            single_worker_mode: false,
        };
        // Install the peer-lifecycle sender on `cluster_state` so the
        // `PeerJoined` / `PeerRemoved` apply rules' emit calls route
        // through the dispatcher channel from this point onward.
        // Done before any other registration so a mutation that
        // somehow lands during construction still has a sender to
        // enqueue against (defensive: today no mutation is applied
        // pre-`run()`, but the contract should not depend on that).
        this.cluster_state.install_lifecycle_sender(lifecycle_tx);
        // Same shape as the lifecycle sender install: the apply path
        // on `cluster_state` now has a sender to enqueue trigger
        // events through; the operational `select!` will own the
        // receiver from `run()` onward.
        this.cluster_state
            .install_matcher_trigger_sender(matcher_trigger_tx);
        // Same shape: install the task-completion sender so the
        // `TaskCompleted` / `TaskFailed` apply rules' emit calls route
        // through the dispatcher channel from this point onward.
        this.cluster_state
            .install_task_completed_sender(task_completed_tx);
        // Mirror `SecondaryCoordinator::new`'s registration: attach the
        // peer transport's write-through `RoleTable` cache to the
        // authoritative `cluster_state.role_table`. The hook fires on
        // every applied `PrimaryChanged` mutation; the cache serves
        // Step 3's `Address::Role(_)` dispatch on the send hot path.
        // Transports that don't override `register_with_cluster_state`
        // (`NoPeerTransport`, the `NoPeers` test stub) get the default
        // no-op — safe by construction. `Role::Self_` is seeded by
        // the transport's own constructor (`new_role_cache` +
        // `seed_self_role` for `ChannelPeerTransport` and
        // `PeerNetwork`), not here, because that's a strictly
        // transport-local fact.
        this.peer_transport
            .register_with_cluster_state(&mut this.cluster_state);
        this
    }

    /// Register a [`crate::peer_lifecycle::LifecycleListener`] to be
    /// invoked off the apply path for every `PeerJoined`/`PeerRemoved`
    /// state transition. Must be called BEFORE `run()` enters; calls
    /// after `run()` has consumed the listener vector are dropped
    /// silently (the field is `mem::take`-d into the dispatcher at
    /// run start, and the dispatcher is the only reader).
    ///
    /// Single concern: own the registration surface; the dispatcher
    /// task in `crate::peer_lifecycle::dispatcher` owns the
    /// invocation semantics.
    pub fn register_lifecycle_listener(
        &mut self,
        listener: Box<dyn crate::peer_lifecycle::LifecycleListener>,
    ) {
        self.peer_lifecycle_listeners.push(listener);
    }

    /// Register the panik-watcher signal receiver. Must be called
    /// BEFORE `run()` enters; calls afterwards have no effect on
    /// the active loop (the field is `Option::take`-n into the
    /// operational loop's local state on first entry).
    ///
    /// Mirrors `SecondaryCoordinator::register_panik_signal_rx`.
    /// The PyO3 wrapper owns spawning
    /// [`crate::panik_watcher::spawn_panik_watcher`] and threading
    /// its `take_signal_rx()` here.
    pub fn register_panik_signal_rx(
        &mut self,
        rx: tokio::sync::oneshot::Receiver<crate::panik_watcher::PanikSignal>,
    ) {
        self.panik_signal_rx = Some(rx);
    }

    /// Tear down the peer-lifecycle dispatcher task spawned at
    /// `run()` start. No-op when the dispatcher was never spawned
    /// (e.g. the early-return path before the spawn site, or a
    /// coordinator whose `run()` was never called).
    ///
    /// # Why explicit rather than Drop
    ///
    /// A `Drop` guard cannot abort + await — it has no async
    /// context, and the host tokio runtime may already be torn down
    /// by the time the coordinator is dropped (the PyO3 LocalSet is
    /// scoped to the `py.detach` block). Calling `abort()` from
    /// `Drop` without an awaiting reaper risks a runtime-gone panic
    /// in the dispatcher's last-poll cleanup. Explicit
    /// invocation from the `run()` outer wrapper keeps the abort and
    /// the join inside the live LocalSet.
    pub(super) async fn cleanup_lifecycle_dispatcher(&mut self) {
        if let Some(handle) = self.lifecycle_dispatcher_handle.take() {
            handle.abort();
            // Ignore the `JoinError` — abort-cancelled is the
            // expected shape (`JoinError::is_cancelled() == true`).
            // The body of the task itself never returns a fallible
            // value (it's `Future<Output = ()>`), so the only thing
            // an Ok branch could carry is the unit value — nothing
            // to consume.
            let _ = handle.await;
        }
    }

    /// Register a [`crate::task_completed::TaskCompletedListener`] to
    /// be invoked off the apply path for every `TaskCompleted` /
    /// `TaskFailed` (state-changing) apply rule. Must be called BEFORE
    /// `run()` enters; calls after `run()` has consumed the listener
    /// vector are dropped silently (the field is `mem::take`-d into
    /// the dispatcher at run start, and the dispatcher is the only
    /// reader). Mirrors [`Self::register_lifecycle_listener`].
    pub fn register_task_completed_listener(
        &mut self,
        listener: Box<dyn crate::task_completed::TaskCompletedListener>,
    ) {
        self.task_completed_listeners.push(listener);
    }

    /// Tear down the task-completion dispatcher task spawned at
    /// `run()` start. Mirrors [`Self::cleanup_lifecycle_dispatcher`]
    /// exactly — the same abort+await dance with the same Drop-vs-
    /// LocalSet rationale.
    pub(super) async fn cleanup_task_completed_dispatcher(&mut self) {
        if let Some(handle) = self.task_completed_dispatcher_handle.take() {
            handle.abort();
            let _ = handle.await;
        }
    }

    /// Install the consumer-supplied fulfillability matcher. Must be
    /// called BEFORE `run()` enters; the operational loop reads the
    /// field directly from `self` and a post-run setter call has no
    /// effect (the loop has already captured the trait object).
    ///
    /// At most one matcher per coordinator — re-installation replaces
    /// the prior matcher silently. Consumer policy lives entirely
    /// behind the trait method; the coordinator's only job is "fire
    /// `ReinjectTask` when `should_reinject` returns true".
    pub fn set_fulfillability_matcher(
        &mut self,
        matcher: Box<dyn crate::fulfillability_matcher::FulfillabilityMatcher<I>>,
    ) {
        self.fulfillability_matcher = Some(matcher);
    }

    /// Hand out a never-before-used secondary id and advance the
    /// monotonic counter. The first call on a freshly-constructed
    /// coordinator returns `secondary-{num_secondaries}` (the prep
    /// phase owns `secondary-0..secondary-{num_secondaries - 1}`).
    ///
    /// Single concern: identity allocation. The caller is responsible
    /// for invoking this from the operational loop (the single
    /// `&mut self` writer); minting from a spawned task would race
    /// against the loop's own borrow and is rejected by the borrow
    /// checker anyway — the doc-line is a reminder for future maintainers
    /// who might be tempted to clone the coordinator into a task.
    pub fn mint_secondary_id(&mut self) -> String {
        let n = self.next_secondary_id;
        self.next_secondary_id += 1;
        format!("secondary-{}", n)
    }

    /// Park the deployment-mode job manager on the coordinator so the
    /// respawn path can submit a fresh secondary job from inside the
    /// operational loop. Must be called AFTER the preparation phase
    /// returns (so the job manager is live) and BEFORE `run()` enters
    /// (so the operational loop sees `Some(_)` from the first iteration).
    ///
    /// Stored type-erased through `Arc<dyn Any + Send + Sync>` to keep
    /// `manager-distributed` decoupled from any specific batch-system
    /// crate. The respawn caller downcasts via
    /// [`Self::slurm_job_manager`] back to the concrete handle it parked.
    pub fn set_slurm_job_manager(&mut self, jm: Arc<dyn Any + Send + Sync>) {
        self.slurm_job_manager = Some(jm);
    }

    /// Read the parked deployment-mode job manager. Returns `None`
    /// outside the SLURM-pipeline path (in-process / local-channel
    /// pipelines never call [`Self::set_slurm_job_manager`]); the
    /// respawn caller downcasts the inner `Arc<dyn Any + Send + Sync>`
    /// back to its concrete type at the call site.
    pub fn slurm_job_manager(&self) -> Option<&Arc<dyn Any + Send + Sync>> {
        self.slurm_job_manager.as_ref()
    }

    /// Enable the secondary respawn pipeline. `spawner` is the
    /// per-provider [`SecondarySpawner`] (multi-process or SLURM);
    /// `budget` is the per-coordinator caps; `primary_endpoint` and
    /// `primary_pubkey_pem` populate the [`SecondarySpawnSpec`]
    /// fields handed to the spawner per respawn (today's adapters
    /// cache their own copies and ignore the spec values; the
    /// snapshot is held for forward-compat).
    ///
    /// Single concern: install the policy + provider on the
    /// coordinator. Must be called BEFORE `run()` enters (same
    /// pre-run contract as the other registration setters); the
    /// operational loop captures the wiring at run start and never
    /// looks for it elsewhere.
    ///
    /// Absence of this setter leaves the respawn pipeline disabled:
    /// no peer-lifecycle listener is registered and the operational
    /// `select!` arm is structurally unreachable. This matches the
    /// CCD-5 contract — no hot-site `if policy_enabled` checks.
    pub fn enable_respawn(
        &mut self,
        spawner: Arc<dyn SecondarySpawner>,
        budget: RespawnBudget,
        primary_endpoint: String,
        primary_pubkey_pem: String,
    ) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        self.respawn_spawner = Some(spawner);
        self.respawn_budget = Some(budget);
        self.respawn_request_tx = Some(tx.clone());
        self.respawn_request_rx = Some(rx);
        self.respawn_primary_endpoint = primary_endpoint;
        self.respawn_primary_pubkey_pem = primary_pubkey_pem;
        // Register the dispatcher listener up-front; the
        // peer-lifecycle dispatcher consumes the listener vector at
        // `run()` start, so the registration MUST land before the
        // run is entered. Same contract as
        // `register_lifecycle_listener` (which this call delegates
        // to under the hood).
        self.register_lifecycle_listener(respawn_dispatcher_listener(tx));
    }

    /// Clone of the cross-thread `PrimaryCommand` sender. Callers
    /// (PyO3 `PrimaryHandle`, future Rust-side control planes)
    /// clone this BEFORE invoking `run()` so they have an ingress
    /// for "from outside the operational loop, please apply this
    /// mutation". The sender itself is `Clone` and `Send` so the
    /// returned handle is freely passable across threads / async
    /// runtimes.
    pub fn command_sender(&self) -> tokio_mpsc::Sender<PrimaryCommand<I>> {
        self.command_tx.clone()
    }

    /// Swap the internal command-channel pair for an externally-
    /// supplied one. The PyO3 layer uses this so the
    /// `PrimaryHandle` it exposes to Python at `__init__` time is
    /// the same channel the (later-constructed) `PrimaryCoordinator`
    /// reads from — without this, the channel created in `new()`
    /// can't be reached from Python before `run()` starts because
    /// the coordinator itself is built inside the detached tokio
    /// runtime.
    ///
    /// Must be called BEFORE `run()` enters the operational loop;
    /// calling it after the loop has taken the receiver out (via
    /// `command_rx.take()`) replaces the stored-back receiver but
    /// the loop has already moved on to the local copy. The PyO3
    /// surface enforces this with the
    /// `set_unfulfillable_reinject_max_per_task` setter's
    /// "before run() only" contract; the channel-swap is on the
    /// same contract.
    pub fn replace_command_channel(
        &mut self,
        tx: tokio_mpsc::Sender<PrimaryCommand<I>>,
        rx: tokio_mpsc::Receiver<PrimaryCommand<I>>,
    ) {
        self.command_tx = tx;
        self.command_rx = Some(rx);
    }

    /// Set the per-task budget cap for
    /// `PrimaryCommand::ReinjectTask` after construction. The CLI and
    /// PyO3 surfaces wire this through to the underlying
    /// `PrimaryConfig` field so the live coordinator and the
    /// CLI-supplied `--unfulfillable-reinject-max-per-task=N` flag
    /// stay in lockstep. Idempotent if the existing value matches;
    /// callable any time before `run()` enters the operational
    /// loop (the loop's `select!` reads the field directly).
    pub fn set_unfulfillable_reinject_max_per_task(&mut self, max: Option<u32>) {
        self.config.unfulfillable_reinject_max_per_task = max;
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

    /// True while the per-phase OOM retry bucket is actively
    /// reinjecting + draining. Sole writer: `try_run_phase_retry_bucket`
    /// (set true on `BucketKind::Oom` entry, reset on its `Ok(false)`
    /// returns). Read by the dispatch-shape pipeline and the
    /// operational-loop 5-min timeout arm. See the field doc on
    /// `single_worker_mode` for the user-spec rationale.
    pub(super) fn single_worker_mode(&self) -> bool {
        self.single_worker_mode
    }

    /// Secondary-local worker id (0-based) for the worker at index
    /// `worker_idx` in `self.workers`. Workers are stored grouped by
    /// secondary in `self.workers` (initial-assignment populated
    /// order); the local id is "position among same-secondary
    /// predecessors in the Vec".
    ///
    /// Single concern: index translation. Used by the dispatch-shape
    /// pipeline so OOM-bucket single-worker masking can read the
    /// secondary-local id without each call site re-doing the linear
    /// scan. The two existing call sites — `dispatch_to_idle_workers`
    /// and `handle_task_request` — already computed the same value
    /// inline; centralising keeps the masking site and the wire-
    /// emitted `local_worker_id` in lockstep.
    pub(super) fn local_worker_id_in_secondary(&self, worker_idx: usize) -> u32 {
        let sec_id = self.workers[worker_idx].secondary_id.as_str();
        self.workers[..worker_idx + 1]
            .iter()
            .filter(|w| w.secondary_id == sec_id)
            .count() as u32
            - 1
    }

    /// True iff the worker at `worker_idx` must be skipped this
    /// dispatch tick. Composes the two reasons-to-skip the dispatch
    /// pipeline knows about today:
    ///
    ///   * The worker's secondary is in backpressure backoff
    ///     ([`Self::is_backpressured`]).
    ///   * OOM-bucket single-worker mode is active and this is not
    ///     worker 0 of its secondary ([`Self::single_worker_mode`]).
    ///
    /// Single concern: the dispatch-pipeline's "skip this worker"
    /// decision. Adding another reason-to-skip lands here, not as a
    /// parallel `if` at every call site. The two call sites
    /// (`dispatch_to_idle_workers` + `handle_task_request`) stay
    /// agnostic to either policy.
    pub(super) fn should_skip_worker_for_dispatch(&self, worker_idx: usize) -> bool {
        let sec_id = self.workers[worker_idx].secondary_id.as_str();
        if self.is_backpressured(sec_id) {
            return true;
        }
        if self.single_worker_mode && self.local_worker_id_in_secondary(worker_idx) != 0 {
            return true;
        }
        false
    }

    /// Secondary ids ordered by total advertised memory descending.
    /// Ties broken stably by id (lexicographic) so the OOM-bucket
    /// per-task `preferred_secondaries` assignment is reproducible
    /// across re-entries. Secondaries with no `memory` resource
    /// advertised sort last (treated as zero).
    ///
    /// Single concern: snapshot the cluster's per-node memory
    /// ranking at OOM-bucket entry. Re-sorting per iteration is
    /// explicitly NOT done — a secondary that dies mid-bucket will
    /// naturally fail dispatch, and the next bucket entry re-samples.
    /// Returns owned `String`s so callers can carry the snapshot
    /// across `&mut self` reinject/dispatch calls without lifetime
    /// surgery.
    pub(super) fn secondaries_sorted_by_memory_desc(&self) -> Vec<String> {
        let mem_kind = dynrunner_core::ResourceKind::memory();
        let mut entries: Vec<(String, u64)> = self
            .secondaries
            .iter()
            .map(|(id, state)| {
                let mem = state
                    .resources()
                    .iter()
                    .find(|r| r.kind == mem_kind)
                    .map(|r| r.amount)
                    .unwrap_or(0);
                (id.clone(), mem)
            })
            .collect();
        // Sort by (memory DESC, id ASC). Stable on id so a fleet with
        // multiple equal-memory nodes assigns retries deterministically
        // across re-runs.
        entries.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        entries.into_iter().map(|(id, _)| id).collect()
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

    /// Build the dispatch-shape worker view for the worker at
    /// `worker_idx`. The pipeline is:
    ///
    ///   1. `pool.view_for_worker(global_wid, Some(&soft_pred))` —
    ///      priority-ordered eligible items with soft
    ///      `preferred_secondaries` tie-break.
    ///   2. Strict `preferred_secondaries` filter — ACTIVE iff
    ///      `single_worker_mode()` is true. Drops items whose
    ///      non-empty `preferred_secondaries` list omits this
    ///      worker's secondary.
    ///   3. `cap_filter_view` — drops items over a per-type cap.
    ///
    /// Single concern: the dispatch-pipeline's view-construction
    /// shape. Outside the OOM bucket step (2) is a no-op; inside
    /// it is load-bearing. Both call sites
    /// (`dispatch_to_idle_workers` and `handle_task_request`) call
    /// this once and consume the returned view directly.
    pub(super) fn dispatch_view_for_worker(
        &self,
        worker_idx: usize,
    ) -> dynrunner_scheduler_api::WorkerView<I> {
        let global_wid = self.workers[worker_idx].worker_id;
        let secondary_id = self.workers[worker_idx].secondary_id.as_str();
        let soft_predicate = preferred_secondaries::apply_preferred_secondaries_predicate::<I>(
            secondary_id,
        );
        let view = self
            .pool()
            .view_for_worker(global_wid, Some(&soft_predicate));
        let view = self.apply_strict_preferred_secondaries(view, secondary_id);
        self.cap_filter_view(view)
    }

    /// Apply the strict-preferred-secondaries filter to `view` iff
    /// the coordinator is in OOM-bucket single-worker mode; otherwise
    /// return `view` unchanged.
    ///
    /// Kept as a tiny standalone helper so the active-vs-inactive
    /// gating lives in exactly one place and the dispatch-pipeline
    /// helper above reads as a flat sequence of steps.
    fn apply_strict_preferred_secondaries(
        &self,
        view: dynrunner_scheduler_api::WorkerView<I>,
        secondary_id: &str,
    ) -> dynrunner_scheduler_api::WorkerView<I> {
        if !self.single_worker_mode() {
            return view;
        }
        view.filter(
            preferred_secondaries::filter_strict_preferred_secondaries::<I>(secondary_id),
        )
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

    /// Number of tasks the cluster recorded as successfully completed.
    ///
    /// Reads through `cluster_state.outcome_counts().succeeded` so the
    /// count is the CRDT-authoritative tally rather than the per-node
    /// `completed_tasks` HashSet — analogous to the existing
    /// [`Self::outcome_summary`] which routes through the same CRDT
    /// reader for cosmetic #88. The `completed_tasks` HashSet stays
    /// authoritative for per-task identity decisions (dedup on a
    /// re-applied `TaskComplete`, the operational-loop exit gate, the
    /// kickstart-suppression check); cross-class *counts* live one
    /// layer up, on the replicated ledger every replica converges to.
    ///
    /// Without this routing the demoted primary's PyO3-facing
    /// `succeeded=N` stdout undercounted whenever a cross-secondary
    /// completion's mirror hop (`mirror_mutation_to_accounting`) was
    /// bypassed — the same divergence class #88 fixed at the
    /// terminal-log site but missed at the dispatcher's PyO3
    /// `completed_count()` read site. Concrete symptom: setup-promote
    /// single-task phases (e.g. asm-tokenizer's unify-vocab) reporting
    /// `succeeded=0` while the promoted secondary's count is the
    /// real value (which is also what `cluster_state` records on every
    /// replica, including this one).
    ///
    /// Residual concern: the mirror divergence itself is documented at
    /// `mirror_mutation_to_accounting`'s call sites but not yet
    /// root-caused. In-process tests cover the mirror path end-to-end
    /// (see `demoted_primary_applies_cluster_mutation_taskcompleted`
    /// and the `setup_promote_*` integration suite — both observe
    /// `completed_count()` == CRDT succeeded in test fixtures). The
    /// production bypass is likely a real-QUIC writer-task race on the
    /// loopback path that the in-process channel fixture doesn't
    /// exercise. Until the bypass is root-caused, the CRDT read is
    /// the authoritative source for cross-class count reporting on
    /// the demoted observer. See the demoted-observer divergence
    /// trace in `lifecycle/operational_loop.rs`.
    pub fn completed_count(&self) -> usize {
        self.cluster_state.outcome_counts().succeeded
    }

    /// Number of tasks the cluster recorded as terminally failed
    /// (any failure class — `Recoverable` whose retry budget is
    /// exhausted, `ResourceExhausted`, `NonRecoverable`, or
    /// `Unfulfillable`).
    ///
    /// Same CRDT-routing rationale as [`Self::completed_count`]:
    /// reads through `cluster_state.outcome_counts()` for the
    /// CRDT-authoritative tally rather than the per-node
    /// `failed_tasks` HashSet. Sums the three failure buckets
    /// (`fail_retry + fail_oom + fail_final`) to preserve the
    /// pre-migration semantics of "any task currently in a terminal
    /// failure state".
    pub fn failed_count(&self) -> usize {
        let o = self.cluster_state.outcome_counts();
        o.fail_retry + o.fail_oom + o.fail_final
    }

    /// Per-class outcome breakdown for the coordinator-facing log
    /// lines (`succeeded=… fail_retry=… fail_oom=… fail_final=…`).
    ///
    /// Reads through `cluster_state.outcome_counts()` so the count is
    /// the CRDT-authoritative tally rather than the per-node
    /// `completed_tasks`/`failed_tasks` HashSets. The HashSets stay
    /// authoritative for per-task identity decisions (dedup on a
    /// re-applied TaskComplete, the operational-loop exit gate, the
    /// kickstart-suppression check); cross-class *counts* live one
    /// layer up, on the replicated ledger every replica converges
    /// to. Without this routing the demoted primary's terminal log
    /// undercounted whenever a cross-secondary completion's mirror
    /// hop (`mirror_mutation_to_accounting`) was bypassed (#88).
    ///
    /// O(n) over the cluster_state task ledger — same bound as the
    /// older `failed_tasks`-only walk; the additional Completed/
    /// Pending/InFlight inspections are constant-time per entry.
    pub fn outcome_summary(&self) -> OutcomeSummary {
        self.cluster_state.outcome_counts()
    }

    /// Tasks the run loop never accounted for (neither completed nor
    /// failed). Populated by `run()` after the loop exits — common
    /// causes are transport collapse before every task dispatched,
    /// secondaries dying mid-run, or any exit path that left items
    /// queued / in-flight without a recorded outcome.
    ///
    /// Reset to 0 at the start of every `run()`. Zero on a clean run
    /// (the loop exits via the `completed + failed >= total` arm).
    /// `>0` is the structured-error case that surfaces as
    /// `RunError::ClusterCollapsed` on the wire — read either via the
    /// matched error variant or via this getter post-call.
    pub fn stranded_count(&self) -> usize {
        self.stranded_count
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

    /// Test-only mutable borrow of the replicated cluster ledger, used
    /// by the hydration tests to seed task states (`TaskAdded` →
    /// Pending, `TaskAssigned` → InFlight, `TaskCompleted` → terminal)
    /// directly via `ClusterState::apply` before
    /// `hydrate_from_cluster_state` runs — without going through the
    /// broadcast path (which needs an initialised pool for the
    /// auto-resume re-inject step).
    #[cfg(test)]
    pub fn cluster_state_mut_for_test(
        &mut self,
    ) -> &mut crate::cluster_state::ClusterState<I> {
        &mut self.cluster_state
    }

    /// Test-only inspector for the pre-owned in-flight ledger seeded by
    /// hydration. Returns the count of inherited in-flight entries.
    #[cfg(test)]
    pub fn pre_owned_in_flight_len_for_test(&self) -> usize {
        self.pre_owned_in_flight.len()
    }

    /// Test-only inspector for whether the peer-lifecycle dispatcher
    /// handle is still held by the coordinator. After a clean `run()`
    /// exit (Ok OR Err), [`Self::cleanup_lifecycle_dispatcher`] must
    /// have taken + aborted + joined the handle, leaving this `false`.
    /// Used by `lifecycle_dispatcher_joinhandle_aborted_on_run_exit`
    /// to pin the cleanup contract.
    #[cfg(test)]
    pub fn lifecycle_dispatcher_handle_present_for_test(&self) -> bool {
        self.lifecycle_dispatcher_handle.is_some()
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
    ///
    /// # Cleanup discipline
    ///
    /// Thin wrapper around [`Self::run_pipeline`] whose secondary
    /// concern is to drive `run()`'s cleanup contract — every exit
    /// path (happy-path `Ok`, structured `RunError`, `?`-propagated
    /// error) flows through `cleanup_lifecycle_dispatcher` before
    /// returning, so the peer-lifecycle dispatcher task spawned in
    /// `run_pipeline` is always aborted and joined before this method
    /// returns. Without the wrapper, an error-return from inside the
    /// pipeline would leave the dispatcher blocked on its input
    /// channel forever (the channel's sender lives on `cluster_state`,
    /// which the coordinator still owns post-`run`).
    pub async fn run(
        &mut self,
        binaries: Vec<TaskInfo<I>>,
        phase_deps: HashMap<PhaseId, Vec<PhaseId>>,
        on_phase_start: OnPhaseStart,
        on_phase_end: OnPhaseEnd,
    ) -> Result<(), RunError> {
        let result = self
            .run_pipeline(binaries, phase_deps, on_phase_start, on_phase_end)
            .await;
        self.cleanup_lifecycle_dispatcher().await;
        // Independent of `cleanup_lifecycle_dispatcher` — the two
        // dispatchers own independent channels + listener vectors;
        // both run from spawn-at-`run()`-start to abort-at-`run()`-
        // exit and both must be joined before `run()` returns so the
        // PyO3 wrapper / SLURM pipeline don't leak them.
        self.cleanup_task_completed_dispatcher().await;
        result
    }

    /// Original `run()` body, factored out so the public `run` wrapper
    /// can drive cleanup-on-exit regardless of how this function
    /// returns. See [`Self::run`] for the rationale.
    async fn run_pipeline(
        &mut self,
        binaries: Vec<TaskInfo<I>>,
        phase_deps: HashMap<PhaseId, Vec<PhaseId>>,
        on_phase_start: OnPhaseStart,
        on_phase_end: OnPhaseEnd,
    ) -> Result<(), RunError> {
        // Reset the stranded counter so a previous run's residue
        // can't leak into this one. Populated below after both loops
        // drain; the structured-error path consults it.
        self.stranded_count = 0;
        // Reset the setup-promote-deadline outcome so a previous
        // run's residue (the field is only written when the deadline
        // arm fires; a clean run leaves it untouched, but a coordinator
        // re-used across runs must not inherit a stale outcome).
        self.setup_deadline_outcome = None;

        // Refresh the setup-pending gate from the current config so a
        // reused coordinator (or one whose config was mutated between
        // runs) starts each run with the correct exit-gate state. In
        // setup-promote mode this defers the counter-based exit-check
        // until the chosen secondary has either broadcast its first
        // `TaskAdded` (discovery succeeded) or `RunComplete` (no items
        // to discover) — see the `setup_pending` field doc.
        self.setup_pending = self.config.required_setup_on_promote;

        // Spawn the peer-lifecycle dispatcher BEFORE any wire mutation
        // can land. The (sender, receiver) pair was built in `new()`
        // and the sender already installed on `cluster_state`; here
        // we hand the receiver and the registered listeners to the
        // dispatcher task. `spawn_local` matches the rest of the
        // coordinator's LocalSet-bound spawn pattern. If the receiver
        // has already been taken (defensive — `run()` is single-shot
        // by contract; this branch covers a future caller that
        // re-enters), the dispatcher is silently skipped.
        //
        // The returned `JoinHandle` is stored on `self` so
        // `cleanup_lifecycle_dispatcher` (called from the `run()`
        // outer wrapper on every exit path) can abort the task and
        // await its termination, preventing a leaked dispatcher
        // when `run()` returns Err with the coordinator still alive
        // (the dispatcher's input channel sender lives on
        // `cluster_state`, so it would otherwise never observe a
        // closed-channel `None`).
        if let Some(rx) = self.lifecycle_rx.take() {
            let listeners = std::mem::take(&mut self.peer_lifecycle_listeners);
            let handle = tokio::task::spawn_local(
                crate::peer_lifecycle::run_peer_lifecycle_dispatcher(rx, listeners),
            );
            self.lifecycle_dispatcher_handle = Some(handle);
        }

        // Same shape as the peer-lifecycle dispatcher spawn: hand the
        // (rx, listeners) pair to the task-completion dispatcher
        // BEFORE any wire mutation can land. The (sender, receiver)
        // pair was built in `new()` and the sender already installed
        // on `cluster_state`. The returned `JoinHandle` is stored on
        // `self` so `cleanup_task_completed_dispatcher` aborts + joins
        // on every `run()` exit path (same dispatcher-leak defence
        // documented for the peer-lifecycle handle).
        if let Some(rx) = self.task_completed_rx.take() {
            let listeners = std::mem::take(&mut self.task_completed_listeners);
            let handle = tokio::task::spawn_local(
                crate::task_completed::run_task_completed_dispatcher(rx, listeners),
            );
            self.task_completed_dispatcher_handle = Some(handle);
        }

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
        // Per-(phase, bucket) retry counters: cleared at run-start
        // so a coordinator reused across runs (no production path
        // does this today, but the single-shot contract should not
        // implicitly depend on `new()`-only init) starts every
        // bucket from zero.
        self.retry_passes_used.clear();
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
        //
        // Gated on `!self.setup_pending` because in setup-defer mode
        // the local primary enters `run()` with `binaries = []` and
        // every declared phase is `Active` with zero items — but only
        // transiently: the setup-promoted secondary will broadcast
        // `TaskAdded` once its discovery completes and populate the
        // cluster ledger. Running the initial-empty-phase cascade now
        // would mark every phase `Drained` and fire spurious
        // `on_phase_end(.., 0, 0)` callbacks for phases that haven't
        // had a chance to receive items yet. Both halves of the
        // cascade (`drain_empty_active_phases` flipping `Active` →
        // `Drained`, and `process_phase_lifecycle` firing
        // `on_phase_end`) must be skipped together to keep the pool
        // in a coherent state while setup is pending; gating only the
        // cascade would leave phases stuck in `Drained` with no
        // organic re-activation path on a setup-defer demoted primary
        // (TaskAdded mirrors into `cluster_state`, not the local pool,
        // so `reinject` never runs to unwind `Drained` → `Active`).
        //
        // `process_phase_lifecycle` carries its own `setup_pending`
        // early-return for defence-in-depth at every other call site
        // (note_item_completed / note_item_failed), but here we keep
        // the explicit pre-call `drain_empty_active_phases` and the
        // ALSO-redundant cascade invocation paired under a single
        // gate — the two are one logical unit (the pre-call exists
        // only to feed the cascade).
        //
        // Take/put-back the command-channel receiver around the cascade
        // call: the cascade's per-iteration drain step needs `&mut
        // Receiver` AND the cascade itself needs `&mut self`, which
        // would alias if we passed `&mut self.command_rx` directly.
        // Mirrors the discipline `operational_loop` uses (see
        // `lifecycle/operational_loop.rs:51`); the brief window between
        // take and put-back is benign here because we're still in `run`
        // before the operational loop has started — no concurrent
        // sender access path exists.
        //
        // Required at this pre-loop site (not optional): a consumer
        // `on_phase_end` callback fired by the initial-empty-phase
        // cascade can itself queue `spawn_tasks(next_phase_items)`,
        // and the cascade's next `drain_empty_active_phases` poll
        // would otherwise false-fire `on_phase_end(.., 0, 0)` on the
        // successor phase exactly the way the in-loop bug class did.
        // Take the command-channel receiver out of `self` for the
        // duration of every pre-operational-loop step that can fire
        // `on_phase_end` (via `process_phase_lifecycle`'s in-cascade
        // dispatch path). Each step is then a pass-through caller
        // that can hand the receiver into `dispatch_message` so the
        // cascade's per-iteration drain step picks up callback-queued
        // `SpawnTasks` / `FailPermanent` / `ReinjectTask` / `Update
        // PreferredSecondaries` commands inline.
        //
        // Required because `operational_loop`'s entry-time exit
        // check (`completed + failed >= total_tasks && active_workers
        // == 0`) trips IMMEDIATELY on entry if every pre-loop-dispatched
        // task happens to finish (and have its on_phase_end fire)
        // during a pre-loop wait — without inline drain, the
        // SpawnTasks command sits on the channel until the entry-
        // time check that exits the loop without ever polling it.
        // Asm-tokenizer's lazy-spawn consumer pattern
        // (`FullPipelineTask.on_phase_end → primary_handle.spawn_tasks`)
        // is the live consumer of this contract.
        //
        // Put-back semantics: returned to `self.command_rx` before
        // `operational_loop` is called so the loop's own
        // `self.command_rx.take()` re-acquires the same receiver.
        // The window between take-here and put-back is `Send`-bound
        // by the same `LocalSet` the rest of the coordinator runs
        // on; no concurrent producer exists pre-loop.
        let mut command_rx = self.command_rx.take();

        if !self.setup_pending {
            self.pool_mut().drain_empty_active_phases();
            self.process_phase_lifecycle(&mut command_rx).await;
        }

        // Phase 1+2: Wait for all secondaries to send welcome + cert exchange
        self.wait_for_connections(&mut command_rx).await?;

        // Phase 2.5: Auto-stage. Run the staging walk on behalf of
        // callers that didn't pre-queue via `queue_stage_file` /
        // `queue_initial_staging_from_binaries`. Gate semantics
        // (and the rationale for each one) live on
        // `staging::maybe_auto_stage_initial`. The four-way gate
        // collapses to "we have a root to walk, items are file-
        // backed, we're not in pre-staged mode, and no caller pre-
        // populated the queue" — any one false skips silently.
        //
        // Performed AFTER `wait_for_connections` so `self.secondaries`
        // has the welcome-registered IDs (the staging fan-out is
        // per-secondary), and BEFORE `perform_initial_assignment`
        // which drains `pending_stage_files` into each recipient's
        // `InitialAssignment.staged_files`. This is the single
        // Rust-side call site for the staging walk; pyo3 wrappers
        // just thread `source_dir` into config.
        //
        // Without this, the network-primary + local-secondaries
        // pipeline (`--multi-computer local`) had no staging call
        // site at all and lost every task to "expected StageFile
        // notification first" on the secondary's
        // unresolvable-task guard. The in-process distributed
        // pipeline kept its explicit pre-call from #7, which the
        // gate detects (non-empty queue) and skips — consistent
        // SLURM-pipeline semantics where the explicit pre-call
        // also wins.
        self.maybe_auto_stage_initial()?;

        // Phase 3: Send peer lists
        self.send_peer_lists().await?;

        // Phase 4: Wait for peer connections (skip for single secondary)
        self.wait_for_peer_connections().await?;

        // Phase 4.5 + Phase 5: Seed the replicated cluster ledger and
        // perform the initial per-secondary assignment. Both steps are
        // skipped when this primary is operating in setup-defer mode
        // (`required_setup_on_promote` — i.e. the submitter was
        // launched with `--source-already-staged` and has no local
        // view of the corpus to discover or seed); in that mode the
        // chosen secondary runs task discovery + ledger seed after the
        // bootstrap promotion (see `PromotePrimary { required_setup:
        // true }` below). To keep the secondaries' `wait_for_setup`
        // loop unchanged in either mode, `emit_setup_defer_handshake`
        // sends the degenerate InitialAssignment + state transitions
        // the legacy path would have sent — empty payloads but the
        // same wire-frame triple. The non-defer path keeps the
        // legacy `seed_cluster_state` (TaskAdded fan-out, PhaseDepsSet)
        // + `perform_initial_assignment` (round-robin worker
        // assignments, staged-files inline) pairing.
        if self.config.required_setup_on_promote {
            self.emit_setup_defer_handshake().await?;
        } else {
            self.seed_cluster_state().await;
            self.perform_initial_assignment().await?;
        }

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
        self.wait_for_mesh_ready(&mut command_rx).await?;

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

        // Put the command-channel receiver back on `self` so
        // `operational_loop`'s own `self.command_rx.take()` picks
        // it up again. Symmetric with the take at the top of the
        // pre-loop chain.
        self.command_rx = command_rx;

        // Operational loop (main pass).
        self.operational_loop().await?;

        // Panik check: if the operational loop's panik arm fired,
        // the cluster has already been instructed (via the broadcast)
        // to shut down. Surface as `RunError::PanikShutdown` and
        // skip every remaining phase (retry passes, drain, accounting,
        // RunComplete settle window). The PyO3 wrapper translates
        // PanikShutdown into `std::process::exit(137)` so the SLURM
        // wrapper reaps the container.
        if let Some((matched_path, reason)) = self.panik_outcome.take() {
            tracing::warn!(
                matched_path = %matched_path.display(),
                reason = %reason,
                "primary run aborted by panik signal; surfacing PanikShutdown"
            );
            return Err(RunError::PanikShutdown {
                matched_path,
                reason,
            });
        }

        // Setup-promote-deadline check: if the operational loop's
        // deadline arm fired (the promoted secondary never broadcast
        // TaskAdded / TasksSpawned / RunComplete within
        // `config.setup_promote_deadline`), surface as
        // `RunError::SetupDeadlineExpired`. Skip the retry-pass /
        // drain / accounting tail — no task ever entered the pool, so
        // there's nothing to retry, drain, or account for. The RunComplete
        // broadcast tail is also skipped: the cluster never reached an
        // operational state to begin with, so no peers are sitting on
        // a "run-is-over" cue.
        if let Some(elapsed) = self.setup_deadline_outcome.take() {
            tracing::error!(
                elapsed_s = elapsed.as_secs_f64(),
                "primary run aborted by setup-promote deadline expiry; \
                 surfacing SetupDeadlineExpired"
            );
            return Err(RunError::SetupDeadlineExpired { elapsed });
        }

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

        // Same panik check, post-retry-passes. If panik fired during
        // a retry pass's operational-loop re-entry, `panik_outcome`
        // is Some and `run_retry_passes` bailed at the top of its
        // next iteration. Pick it up here.
        if let Some((matched_path, reason)) = self.panik_outcome.take() {
            tracing::warn!(
                matched_path = %matched_path.display(),
                reason = %reason,
                "primary run aborted by panik signal during retry passes; \
                 surfacing PanikShutdown"
            );
            return Err(RunError::PanikShutdown {
                matched_path,
                reason,
            });
        }

        // Drain any TaskComplete / TaskFailed messages that crossed the
        // wire while the operational loop was winding down but hadn't
        // been pulled by `transport.recv` yet. Without this, the
        // accounting below sees pre-drain counts and classifies
        // successful completions as `stranded`, false-positiving clean
        // runs into `RunError::ClusterCollapsed`. Bounded by 500ms so
        // the cost on a fully-quiesced happy-path exit is one
        // 50ms quiet-window probe; the longer ceiling covers
        // heavily-pipelined teardowns where a burst of TaskCompletes
        // is still in flight as the loop exits.
        self.drain_pending_messages(Duration::from_millis(500)).await?;

        // Final accounting: any task in `total_tasks` that is neither
        // in `completed_tasks` nor in `failed_tasks` is *stranded* —
        // the run loop exited (transport closed, all secondaries dead,
        // inactivity timeout, etc.) before the per-task outcome could
        // be recorded. Surfacing this category as a distinct counter
        // (rather than silently letting it vanish into "total -
        // completed - failed = unaccounted") is the load-bearing
        // observability fix: pre-fix, asm-tokenizer's primary returned
        // exit 0 with `Completed: 10 / Failed: 0 / Total: 484` and CI
        // / ops scripts checking exit code saw green when 474 tasks
        // had never even been dispatched. Post-fix, the same scenario
        // produces a structured `RunError::ClusterCollapsed` with the
        // per-category counts, the diagnostic log line below, and a
        // non-zero exit at the PyO3 boundary.
        let outcome = self.outcome_summary();
        self.stranded_count = total.saturating_sub(outcome.total_terminal());
        let stranded = self.stranded_count;

        // Broadcast `RunComplete` so non-promoted secondaries on the
        // peer mesh know the run is genuinely over and can exit. Without
        // this, after a post-promotion handoff scenario, the local
        // primary disconnects but peers can't tell whether the run
        // finished or the primary just crashed — they sit in failover
        // detection holding SLURM job slots indefinitely. Idempotent on
        // re-application; failures here are non-fatal (the run already
        // succeeded, this is a cleanup signal).
        //
        // Issued whether or not stranded > 0: even on the cluster-
        // collapse path, any peer still on the mesh deserves the same
        // run-is-over signal so it can release its SLURM slot.
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

        if stranded > 0 {
            tracing::error!(
                succeeded = outcome.succeeded,
                fail_retry = outcome.fail_retry,
                fail_oom = outcome.fail_oom,
                fail_final = outcome.fail_final,
                stranded,
                total,
                "{stranded} tasks left unassigned because cluster routing collapsed \
                 (succeeded={s} fail_retry={r} fail_oom={o} fail_final={fi} stranded={stranded})",
                s = outcome.succeeded,
                r = outcome.fail_retry,
                o = outcome.fail_oom,
                fi = outcome.fail_final,
            );
            return Err(RunError::ClusterCollapsed { stranded, outcome });
        }

        tracing::info!(
            succeeded = outcome.succeeded,
            fail_retry = outcome.fail_retry,
            fail_oom = outcome.fail_oom,
            fail_final = outcome.fail_final,
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
    ///
    /// `command_rx` carries the operational-loop's command-channel
    /// receiver (the `take`n local; see `operational_loop.rs:51`). After
    /// each cascade iteration's `on_phase_end` fires, we drain any
    /// commands the user callback queued via the in-runtime
    /// `PrimaryHandle` path (e.g. `spawn_tasks(next_phase_items)`) and
    /// dispatch each through the existing `handle_primary_command`
    /// chokepoint BEFORE the next `drain_empty_active_phases` poll. The
    /// drain is the load-bearing step: without it the cascade's next
    /// poll observes the not-yet-applied spawn as an empty successor
    /// phase and false-fires `on_phase_end(.., 0, 0)` for it,
    /// dropping every callback-injected task.
    ///
    /// Pre-loop / post-loop callers (`coordinator.rs:1258`,
    /// `drain_pending_messages`, `wait_for_connections`,
    /// `wait_for_mesh_ready`) pass `&mut None` — at those moments
    /// PyPrimaryHandle is either dormant (run hasn't started yet) or
    /// the operational loop has already exited and won't re-enter, so
    /// there is no in-runtime callback path to drain.
    pub(super) async fn process_phase_lifecycle(
        &mut self,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
    ) {
        // Pre-discovery transient state in setup-defer mode. While
        // `setup_pending == true` the local primary has `total_tasks = 0`
        // and every declared phase is `Active` with zero items — not
        // because they're truly empty, but because the
        // setup-promoted secondary has not yet broadcast its first
        // `TaskAdded` / `TasksSpawned`. Firing `on_phase_end(.., 0, 0)`
        // now would surface a spurious "empty drain" for every phase
        // before the chosen secondary has had a chance to populate them
        // (a consumer callback walking just-discovered outputs would
        // OSError on missing paths). The `setup_pending` latch flips
        // false the moment a `TaskAdded`, `TasksSpawned`, or
        // `RunComplete` mutation lands via the mirror path; subsequent
        // cascade calls resume normal operation. See the
        // `setup_pending` field doc on `PrimaryCoordinator`.
        //
        // Idempotent on the legacy bootstrap path: `setup_pending`
        // starts `false` there, so the gate is always satisfied.
        if self.setup_pending {
            return;
        }
        loop {
            let drained = self.pool_mut().poll_drain_transitions();
            if drained.is_empty() {
                break;
            }
            for p in &drained {
                // Per-phase retry-bucket cascade — runs BEFORE
                // `on_phase_end` so phase B (which depends on A)
                // doesn't activate until phase A's retry buckets
                // are exhausted. See `crate::primary::retry_bucket`
                // for the partition and counter semantics.
                //
                // Recoverable bucket first: a Recoverable failure
                // that succeeds on retry leaves no entry in
                // `failed_tasks`, so the subsequent OOM-bucket
                // probe finds nothing and falls through cleanly.
                // OOM bucket second: the dispatch modifiers (when
                // wired) constrain memory-heavy work to a single
                // worker per secondary in memory-DESC order, so
                // running it AFTER the Recoverable bucket has
                // settled keeps the constraint scoped to actually-
                // over-budget tasks.
                //
                // On `Ok(true)`: the bucket reinjected at least one
                // task; the phase has flipped Drained → Active and
                // `drained_pending` no longer contains it. Skip
                // `on_phase_end` and `mark_phase_done` for this
                // phase; the next drain edge will revisit it.
                if self
                    .try_run_phase_retry_bucket(
                        p,
                        crate::primary::retry_bucket::BucketKind::Recoverable,
                        command_rx,
                    )
                    .await
                    .unwrap_or(false)
                {
                    continue;
                }
                if self
                    .try_run_phase_retry_bucket(
                        p,
                        crate::primary::retry_bucket::BucketKind::Oom,
                        command_rx,
                    )
                    .await
                    .unwrap_or(false)
                {
                    continue;
                }
                let completed = self.phase_completed.get(p).copied().unwrap_or(0);
                let failed = self.phase_failed.get(p).copied().unwrap_or(0);
                if let Some(cb) = self.on_phase_end.as_mut() {
                    cb(p, completed, failed);
                }
                // Apply any commands the on_phase_end callback queued
                // via the in-runtime PrimaryHandle path. Without this,
                // a queued SpawnTasks would sit on the channel until
                // the next operational-loop select! tick — but the
                // cascade's next drain_empty_active_phases poll runs
                // BEFORE that tick and would see the not-yet-applied
                // next phase as empty, false-firing on_phase_end(.., 0,
                // 0) and dropping every callback-injected task. Drain-
                // dispatch is the same handler the operational loop's
                // command arm uses, so the per-command CRDT broadcast
                // + pool reinjection semantics are identical to a
                // channel-delivered command (no parallel apply path,
                // no shape divergence).
                // Drain one command at a time so each `try_recv` borrow
                // releases before the dispatch re-borrows `command_rx`
                // (the recursive cascade fired by e.g.
                // `apply_fail_permanent` needs `&mut command_rx` to
                // drain its OWN post-callback queue). Using
                // `.ok()` collapses the recv result into an
                // `Option<Cmd>` so the match-borrow on `command_rx`
                // doesn't escape the let-binding.
                //
                // `Box::pin` breaks the async-recursion cycle
                // (process_phase_lifecycle → handle_primary_command →
                // apply_fail_permanent → note_item_failed →
                // process_phase_lifecycle); without it the compiler
                // can't size the future. Pinned at THIS site (rather
                // than e.g. on `apply_fail_permanent`) because the
                // cascade re-entry only happens via this dispatch
                // call — so the box allocation is gated on a
                // callback actually queueing a command.
                loop {
                    let cmd = match command_rx.as_mut() {
                        Some(rx) => rx.try_recv().ok(),
                        None => None,
                    };
                    let Some(cmd) = cmd else { break };
                    Box::pin(
                        crate::primary::command_channel::handle_primary_command(
                            self, cmd, command_rx,
                        ),
                    )
                    .await;
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
    ///
    /// `command_rx` threads the operational-loop's command-channel
    /// receiver into the cascade so callback-issued in-runtime
    /// `PrimaryHandle` commands apply inline (see
    /// `process_phase_lifecycle` doc).
    pub(super) async fn note_item_completed(
        &mut self,
        phase_id: &PhaseId,
        task_id: Option<&str>,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
    ) {
        *self.phase_completed.entry(phase_id.clone()).or_insert(0) += 1;
        self.pool_mut().on_item_finished(phase_id, task_id);
        self.process_phase_lifecycle(command_rx).await;
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
    pub(super) async fn note_item_failed(
        &mut self,
        phase_id: &PhaseId,
        _task_id: Option<&str>,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
    ) {
        *self.phase_failed.entry(phase_id.clone()).or_insert(0) += 1;
        self.pool_mut().on_item_finished(phase_id, None);
        self.process_phase_lifecycle(command_rx).await;
    }
}
