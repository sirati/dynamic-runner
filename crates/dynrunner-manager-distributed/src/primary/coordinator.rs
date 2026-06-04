use std::any::Any;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::task::JoinSet;

use dynrunner_core::{ErrorType, Identifier, PhaseId, ResourceMap, TaskInfo};
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, DistributedMessage, KeepaliveRole, MessageType, PeerId, PeerTransport,
};
use dynrunner_scheduler_api::{PendingPool, ResourceEstimator, Scheduler, WorkerBudgetInfo};
use tokio::sync::mpsc as tokio_mpsc;

use super::command_channel::{COMMAND_CHANNEL_CAPACITY, PrimaryCommand};
use super::config::{OnPhaseEnd, OnPhaseStart, PrimaryConfig};
use super::error::RunError;
use super::lifecycle::RelocationOutcome;
use super::preferred_secondaries;
use super::respawn::{
    RespawnBudget, RespawnEvent, RespawnOutcome, RespawnRequest, SecondarySpawner,
    respawn_dispatcher_listener,
};

use crate::cluster_state::{ClusterState, OutcomeSummary};
use crate::state::SecondaryConnectionState;
use crate::worker_signal::WorkerMgmtSignal;

/// The single-task lifecycle typestate of a remote worker slot.
///
/// Replaces the removed `(current_task: Option<TaskInfo>, is_idle:
/// bool)` two-source-of-truth pair. The held task — its identity hash
/// included — lives INSIDE the `Assigned` variant, so a slot can never
/// be simultaneously "idle but holding a task" or "busy but holding
/// nothing": the divergence class is gone by construction.
///
/// Assignment is reachable ONLY from `Idle` (every assign site goes
/// through [`RemoteWorkerState::assign`], which `debug_assert`s the
/// pre-state and overwrites unconditionally only after the caller has
/// established idleness). A slot returns to `Idle` ONLY through a
/// terminal outcome keyed by the held `task_hash`
/// ([`PrimaryCoordinator::free_slot_on_terminal`]) — never on a bare
/// `TaskRequest`. This makes reassignment-before-terminal
/// architecturally impossible: the `task_hash` is the slot's held-task
/// IDENTITY (the ledger key), not a reorder-detector.
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum SlotState<I: Identifier> {
    /// No task held; the only state from which assignment is legal.
    Idle,
    /// Holds exactly one task. `task_hash` is the canonical
    /// `compute_task_hash(&task)` identity recorded at dispatch and
    /// matched against an inbound terminal's `task_hash` before the
    /// slot frees.
    Assigned {
        task_hash: String,
        task: TaskInfo<I>,
        estimated: ResourceMap,
    },
}

impl<I: Identifier> SlotState<I> {
    fn is_idle(&self) -> bool {
        matches!(self, SlotState::Idle)
    }

    /// The held task, if any. `None` for an `Idle` slot.
    fn task(&self) -> Option<&TaskInfo<I>> {
        match self {
            SlotState::Idle => None,
            SlotState::Assigned { task, .. } => Some(task),
        }
    }

    /// The estimated resource footprint of the held task; empty when
    /// `Idle`. Feeds the scheduler's `estimated_usage` budget view.
    fn estimated(&self) -> ResourceMap {
        match self {
            SlotState::Idle => ResourceMap::new(),
            SlotState::Assigned { estimated, .. } => estimated.clone(),
        }
    }
}

/// Virtual worker tracked by the authoritative primary for each remote worker.
///
/// R1 replaces the removed `(current_task, is_idle)` pair with a single
/// [`SlotState<I>`] typestate field: assignment reachable ONLY from
/// `Idle`, the held task (and its hash) carried inside the `Assigned`
/// variant. The pair is removed here so the slot-keyed attribution it
/// enabled cannot survive the rebuild.
#[derive(Debug, Clone)]
pub(crate) struct RemoteWorkerState<I: Identifier> {
    pub(super) worker_id: u32,
    pub(super) secondary_id: String,
    pub(super) resource_budgets: ResourceMap,
    /// The slot's single-task lifecycle state. Sole source of truth
    /// for "is this worker idle?" and "what does it hold?".
    pub(super) state: SlotState<I>,
}

impl<I: Identifier> RemoteWorkerState<I> {
    /// True iff no task is held — the only state from which assignment
    /// is legal.
    pub(super) fn is_idle(&self) -> bool {
        self.state.is_idle()
    }

    /// The held task, if any.
    pub(super) fn held_task(&self) -> Option<&TaskInfo<I>> {
        self.state.task()
    }

    /// Move the slot `Idle -> Assigned`. The slot MUST be `Idle`; the
    /// `debug_assert` makes a reassign-before-terminal bug a test-time
    /// panic, while production faithfully overwrites (the caller has
    /// already gated on idleness through the dispatch view / scheduler
    /// decision). Mirrors `WorkerHandle::assign_task`'s
    /// `take_idle().ok_or(...)` contract on the worker-process side.
    pub(super) fn assign(&mut self, task_hash: String, task: TaskInfo<I>, estimated: ResourceMap) {
        debug_assert!(
            self.state.is_idle(),
            "slot assigned while not Idle (reassignment-before-terminal)"
        );
        self.state = SlotState::Assigned {
            task_hash,
            task,
            estimated,
        };
    }

    /// Force the slot back to `Idle`, returning the previously-held
    /// task (if any). Used only by the dead-secondary requeue path
    /// (the worker is being dropped) and the dispatch-send rollback;
    /// the routine terminal path goes through
    /// [`PrimaryCoordinator::free_slot_on_terminal`] which gates on the
    /// hash.
    pub(super) fn vacate(&mut self) -> Option<TaskInfo<I>> {
        match std::mem::replace(&mut self.state, SlotState::Idle) {
            SlotState::Idle => None,
            SlotState::Assigned { task, .. } => Some(task),
        }
    }

    pub(super) fn budget_info(&self) -> WorkerBudgetInfo<I> {
        WorkerBudgetInfo {
            worker_id: self.worker_id,
            reserved_budgets: self.resource_budgets.clone(),
            actual_usage: ResourceMap::new(),
            is_idle: self.state.is_idle(),
            is_opportunistic: false,
            has_initial_assignment: !self.state.is_idle(),
            current_task: self.state.task().cloned(),
            estimated_usage: self.state.estimated(),
        }
    }
}

/// One entry in the primary's single hash-keyed in-flight ledger.
///
/// Records every task the authoritative primary believes is currently
/// executing somewhere in the cluster — whether this coordinator
/// dispatched it (a local `RemoteWorkerState` slot holds it) or
/// inherited it from the replicated `cluster_state` at hydration. In
/// BOTH cases the entry carries `local_worker_id = Some(..)`: the live
/// path records the slot's secondary-local id at `commit_assignment`,
/// and the failover-resume path now reconstructs the holding slot from
/// the replicated capacity × `TaskState::InFlight { worker }` occupancy
/// (`reconstruct_workers_from_cluster_state`) and seeds the same id.
/// Folds in and replaces the deleted `pre_owned_in_flight` two-tier
/// fallback: there is now ONE ledger, consulted BY HASH on every
/// terminal, so attribution is unambiguous regardless of dispatch
/// origin.
///
/// The holding slot is keyed by STABLE identity `(secondary_id,
/// local_worker_id)`, never by a positional `Vec` index. A positional
/// index desyncs the instant `self.workers.retain(..)` compacts the Vec
/// on a sibling secondary's death, shifting every survivor after the
/// removed group; the stable id survives compaction because a worker's
/// `local_worker_id` (its position WITHIN its own secondary's
/// contiguous group) is unaffected by removing a DIFFERENT secondary's
/// group. `free_slot_on_terminal` re-resolves the id to a live index
/// via [`PrimaryCoordinator::worker_idx_for`] on every terminal.
#[derive(Debug, Clone)]
pub(crate) struct InFlightEntry<I: Identifier> {
    /// Phase whose in-flight counter this entry holds open; the
    /// terminal cascade decrements it via `note_item_*`.
    pub(super) phase: PhaseId,
    /// Secondary the task was dispatched to (or inherited as targeting).
    /// Half of the stable `(secondary_id, local_worker_id)` holder key.
    pub(super) secondary_id: String,
    /// Secondary-local worker id of the holding slot (the wire
    /// `worker_id`, stable under Vec compaction). The other half of the
    /// stable holder key; resolved to a live `self.workers` index through
    /// [`PrimaryCoordinator::worker_idx_for`]. Always `Some(..)` on every
    /// origination path today (live dispatch via `commit_assignment`,
    /// failover resume via `seed_inflight`); the `Option` and the
    /// matching `free_slot_on_terminal` `None` arm survive as a defensive
    /// safe-no-op guard for a slot that no longer exists.
    pub(super) local_worker_id: Option<u32>,
    /// The full task — its `task_id` resolves dep edges, its `type_id`
    /// releases the per-type concurrency slot.
    pub(super) task: TaskInfo<I>,
}

/// The primary coordinator: orchestrates work across secondaries.
///
/// Generic over ONE `Tr: PeerTransport<I>`. Every primary send goes
/// through the [`Self::send_to`] egress edge, which resolves a typed
/// `Destination` to a concrete peer-id — `Destination::Secondary(id)`
/// for per-secondary writes (initial assignment, task fan-out),
/// `Destination::All` for the keepalive + CRDT fan-out — then calls the
/// `PeerId`-only transport; `recv_peer()` is the unified inbound. The
/// transport is real-by-construction in every primary construction path
/// (`TunneledPeerTransport` for the submitter, `ChannelPeerTransport`
/// in-process / tests, the role-blind `MeshHandleTransport` over the
/// host's shared mesh for the on-demand co-located authority) — there is no
/// no-op send path and no per-site "which transport is real" hazard. The
/// on-demand co-located primary's own-secondary loopback is NOT a transport
/// leg: it is delivered at the egress edge (`SendTarget::Loopback` +
/// the `Destination::All` broadcast loopback leg), so the transport
/// itself stays role-blind. This mirrors the secondary side's collapse
/// onto a single `Tr: PeerTransport`.
pub struct PrimaryCoordinator<
    Tr: PeerTransport<I>,
    S: Scheduler<I>,
    E: ResourceEstimator<I>,
    I: Identifier,
> {
    pub(super) config: PrimaryConfig,
    /// THE single mesh transport. Owns the write-through `RoleTable`
    /// cache attached to `cluster_state` at construction; drives every
    /// primary send (`Address`-routed) and the single `recv_peer()`
    /// inbound surface. For the submitter primary its backend is the
    /// per-secondary tunnel writers + the relocated `NetworkServer`
    /// inbound demux; for the on-demand co-located primary its backend is
    /// the co-located loopback + shared mesh — in both cases a real
    /// send path.
    pub(super) transport: Tr,
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
    /// THE single hash-keyed in-flight ledger. Records every task the
    /// primary believes is executing in the cluster, keyed by its
    /// canonical `compute_task_hash`. Populated identically at dispatch
    /// (locally-assigned via `commit_assignment`) AND at hydration
    /// (inherited from `cluster_state` via `seed_inflight`) — both carry
    /// `local_worker_id = Some(..)` against a holding `RemoteWorkerState`
    /// slot (live-built at dispatch; failover-rebuilt from the replicated
    /// capacity × InFlight occupancy). Drained BY HASH on every terminal
    /// outcome through [`Self::free_slot_on_terminal`]. Folds in and
    /// replaces the deleted `pre_owned_in_flight` two-tier fallback —
    /// there is one ledger, so a completion is attributed unambiguously
    /// to the held task regardless of whether the dispatch was local or
    /// inherited.
    pub(super) in_flight: HashMap<String, InFlightEntry<I>>,
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
    pub(super) retry_passes_used: crate::primary::retry_bucket::RetryPassesUsed,
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

    /// Per-secondary count of staged silence WARN stages already logged
    /// for the secondary's CURRENT silence streak. Owned by the liveness
    /// module (`primary::heartbeat`); the heartbeat tick reads it to fire
    /// each WARN stage at most once, and clears the entry on keepalive
    /// recovery, welcome, and requeue so a fresh streak re-warns from the
    /// first stage. Absent entry == zero stages warned. Private to the
    /// liveness concern: dispatch never reads it (the silent-id set is the
    /// only liveness fact dispatch consumes, via the two boundary methods).
    pub(super) silence_warn_stage: HashMap<String, usize>,

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
    /// growing to the connected-secondaries set before it issues its
    /// `PrimaryChanged` announcement — without that wait, the
    /// newly-named primary becomes authoritative against a still-
    /// forming peer mesh and every pre-mesh-formation message goes
    /// nowhere. Recorded by `handle_mesh_ready`; consumed by
    /// `wait_for_mesh_ready`.
    pub(super) mesh_ready_secondaries: HashSet<String>,

    // primary promotion
    pub(super) primary_id: Option<String>,

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
    pub(super) lifecycle_rx:
        Option<tokio::sync::mpsc::UnboundedReceiver<crate::peer_lifecycle::PeerLifecycleEvent>>,
    /// Consumers of peer-lifecycle events. Appended to via
    /// [`Self::register_lifecycle_listener`] before `run()` enters;
    /// `std::mem::take` moves the whole vector into the spawned
    /// dispatcher at `run()` start, after which the field is empty
    /// and any post-run `register_lifecycle_listener` calls are
    /// silently appending to a dead-letter list (no dispatcher will
    /// see them). The single-shot lifecycle is consistent with the
    /// rest of the coordinator's `run()`-once contract.
    pub(super) peer_lifecycle_listeners: Vec<Box<dyn crate::peer_lifecycle::LifecycleListener>>,

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
    pub(super) task_completed_rx:
        Option<tokio::sync::mpsc::UnboundedReceiver<crate::task_completed::TaskCompletedEvent>>,

    /// Consumers of task-completion events. Appended to via
    /// [`Self::register_task_completed_listener`] before `run()`
    /// enters; `std::mem::take` moves the whole vector into the
    /// spawned dispatcher at `run()` start, after which the field is
    /// empty and any post-run `register_task_completed_listener` calls
    /// are silently appending to a dead-letter list. Mirrors
    /// `peer_lifecycle_listeners`.
    pub(super) task_completed_listeners: Vec<Box<dyn crate::task_completed::TaskCompletedListener>>,

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
        tokio::sync::mpsc::UnboundedReceiver<crate::fulfillability_matcher::MatcherTriggerEvent>,
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
    pub(super) fulfillability_matcher:
        Option<Box<dyn crate::fulfillability_matcher::FulfillabilityMatcher<I>>>,

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
    pub(super) respawn_request_tx: Option<tokio::sync::mpsc::UnboundedSender<RespawnRequest>>,

    /// Receiver side of the dispatcher → operational-loop respawn
    /// request channel. Taken out for the duration of the
    /// operational loop, the same shape as `command_rx` /
    /// `matcher_trigger_rx`. `None` outside an active loop (or
    /// when the respawn policy is disabled).
    pub(super) respawn_request_rx: Option<tokio::sync::mpsc::UnboundedReceiver<RespawnRequest>>,

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

    /// Worker-management signal receiver, paired with the
    /// `worker_mgmt_tx` installed on `cluster_state` at construction.
    /// Taken out at the operational loop's start so its `select!` arm
    /// can `drain_worker_signal_batch` against it; put back at loop
    /// exit so retry-pass re-entries keep draining the same channel.
    /// Same `take()`/restore lifecycle as `matcher_trigger_rx`. `None`
    /// once a previous loop entry already consumed it AND the local was
    /// dropped (closed-channel gate) — single-shot per channel.
    pub(super) worker_mgmt_rx:
        Option<tokio::sync::mpsc::UnboundedReceiver<crate::worker_signal::WorkerMgmtSignal>>,

    /// Set by the operational `select!` loop's worker-management arm
    /// when it drains a [`WorkerMgmtSignal::RunShouldFail`]. Carries the
    /// emit-time reason so the outer `run_pipeline` can surface the run
    /// failure. Same write-only/read-only discipline as `panik_outcome`
    /// / `setup_deadline_outcome`: the arm WRITES, the outer wrapper
    /// READS — keeping the inner loop's `Result<(), String>` signature
    /// untouched. The worker arm OWNS the clean-shutdown drive; the
    /// phase layer that emitted the signal never breaks the loop
    /// directly (decoupling law).
    pub(super) worker_mgmt_fail_outcome: Option<String>,

    /// Set at INITIAL-batch ingest (`ingest_initial_batch`) when the
    /// dependency-existence partition found a `(phase_id, task_id)`
    /// DUPLICATE before any phase started (#3a). Carries the abort
    /// reason. The bootstrap proceeds far enough to connect secondaries
    /// (so the `RunAborted` broadcast reaches them), then `run_pipeline`
    /// reads this directly after `wait_for_connections`, broadcasts
    /// `ClusterMutation::RunAborted { reason }`, and returns
    /// `RunError::DuplicateTaskIdPrePhase` — a hard cluster shutdown.
    /// `None` on a clean ingest. Write-only at ingest, read-once at the
    /// abort gate (same discipline as `setup_deadline_outcome`).
    pub(super) pending_run_abort: Option<String>,

    /// Set at INITIAL-batch ingest: the tasks the dependency-existence
    /// partition flagged as having a literally-absent `(phase_id,
    /// task_id)` dep (#2 missing-dep), each paired with the reason
    /// naming the absent ids. They are EXCLUDED from the pool `extend`
    /// (their `task_id` is pre-seeded into the pool's `failed_tasks`
    /// so the survivors' dep resolution + cascade stay correct) but
    /// KEPT in `all_binaries` so `seed_cluster_state` adds them to the
    /// CRDT as `Pending`; `run_pipeline` then drains this and emits
    /// `TaskFailed { kind: InvalidTask }` for each through the canonical
    /// broadcast/apply pipeline (`Pending → InvalidTask`). Empty on a
    /// clean ingest.
    pub(super) pending_invalid_dep_tasks: Vec<(TaskInfo<I>, String)>,

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

    /// Loopback sender into a CO-LOCATED secondary's inbound, present
    /// ONLY when this primary shares a host with a secondary it must
    /// deliver to in-process (the on-demand co-located primary). The
    /// egress edge ([`Self::send_to`]) writes here whenever
    /// [`dynrunner_protocol_primary_secondary::SendTarget::Loopback`]
    /// resolves (a `Destination` whose host id == this node's own id —
    /// e.g. a `TaskAssignment` to the co-located secondary's own workers)
    /// and ADDITIONALLY on every `Destination::All` broadcast (the
    /// co-located secondary is not a mesh peer of itself, so it observes
    /// the primary's CRDT / `RunComplete` / keepalive fan-out only via
    /// this leg).
    ///
    /// `None` on every other primary (the submitter primary, in-process
    /// tests) — those have no co-located secondary, so `Loopback`
    /// resolution is the benign live-primary self-relay no-op and the
    /// broadcast leg is the plain mesh fan-out. Registered pre-run via
    /// [`Self::register_colocated_loopback`]; this is purely an egress
    /// concern (it never resolves a role) and the transport stays
    /// role-blind.
    pub(super) colocated_loopback_tx:
        Option<tokio::sync::mpsc::UnboundedSender<DistributedMessage<I>>>,
}

impl<Tr: PeerTransport<I>, S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier>
    PrimaryCoordinator<Tr, S, E, I>
{
    pub fn new(config: PrimaryConfig, transport: Tr, scheduler: S, estimator: E) -> Self {
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
        let (matcher_trigger_tx, matcher_trigger_rx) = tokio::sync::mpsc::unbounded_channel();
        // Worker-management signal bus. Built at construction for the
        // same reason as `matcher_trigger_tx`: the phase/task layer's
        // emit calls (`fire_initial_phase_starts` →
        // `PhaseStartedNeedsWorkers`; the per-phase proceed-or-fail
        // decision → `RunShouldFail`; every pool-entry / worker-free
        // edge → `TasksAdded`) need a sender ready from the very first
        // mutation. The receiver waits on `self` until the operational
        // loop takes it and drains coalesced batches via
        // `crate::worker_signal::drain_worker_signal_batch`. No longer
        // test-only: this is the PRODUCTION sender wire-up.
        let (worker_mgmt_tx, worker_mgmt_rx) = tokio::sync::mpsc::unbounded_channel();
        // Task-completion dispatcher channel. Same construction-time
        // motivation as `lifecycle_tx`: the apply path on
        // `cluster_state` needs a sender ready from the very first
        // `TaskCompleted`/`TaskFailed` apply. The receiver waits on
        // `self` until `run()` spawns the dispatcher; events emitted
        // in the interim queue on the unbounded channel and drain on
        // the first dispatcher poll.
        let (task_completed_tx, task_completed_rx) = tokio::sync::mpsc::unbounded_channel();
        // Seed the monotonic id allocator past the IDs the prep phase
        // already minted (`secondary-0..secondary-{num_secondaries - 1}`)
        // so the first respawn lands on `secondary-{num_secondaries}`.
        let next_secondary_id = config.num_secondaries;
        let mut this = Self {
            config,
            transport,
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
            in_flight: HashMap::new(),
            failed_tasks: HashMap::new(),
            phase_completed: HashMap::new(),
            phase_failed: HashMap::new(),
            retry_passes_used: HashMap::new(),
            in_flight_per_type: HashMap::new(),
            on_phase_start: None,
            on_phase_end: None,
            phase_started_emitted: HashSet::new(),
            secondary_keepalives: HashMap::new(),
            silence_warn_stage: HashMap::new(),
            backpressured_secondaries: HashMap::new(),
            fleet_dead_since: None,
            mesh_ready_secondaries: HashSet::new(),
            primary_id: None,
            pending_stage_files: Vec::new(),
            cluster_state: ClusterState::new(),
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
            worker_mgmt_rx: Some(worker_mgmt_rx),
            worker_mgmt_fail_outcome: None,
            pending_run_abort: None,
            pending_invalid_dep_tasks: Vec::new(),
            single_worker_mode: false,
            colocated_loopback_tx: None,
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
        // Same shape as the matcher-trigger sender install: the
        // phase/task apply + emit path on `cluster_state` now has a
        // worker-management bus sender to enqueue signals through; the
        // operational `select!` loop owns the receiver from this point
        // onward and reacts off the emit path.
        this.cluster_state
            .install_worker_mgmt_sender(worker_mgmt_tx);
        // Same shape: install the task-completion sender so the
        // `TaskCompleted` / `TaskFailed` apply rules' emit calls route
        // through the dispatcher channel from this point onward.
        this.cluster_state
            .install_task_completed_sender(task_completed_tx);
        // NOTE: no transport role-cache attachment (mirrors
        // `SecondaryCoordinator::new`). `Destination::Primary` is
        // resolved at THIS edge (`Self::send_to` reads
        // `cluster_state.current_primary()` with the primary's own
        // `node_id` as the bootstrap fallback); the transport is
        // `PeerId`-only and never mirrors the role table. The former
        // `transport.register_with_cluster_state(..)` wiring is removed.
        // Subscribe the primary-side "important" (LLM-wake-worthy)
        // emission for `PrimaryChanged` to the same role-change hook
        // fabric. Self-contained observability concern: it reads only
        // the post-mutation `RoleTable` the hook is handed and emits at
        // `target: dynrunner_important`, so the CRDT apply path stays
        // free of any logging coupling. A promoted secondary runs its
        // co-located primary coordinator, so the hook rides promotion.
        super::important_events::register_primary_changed_important_hook(&mut this.cluster_state);
        this
    }

    /// THE egress edge: resolve a typed
    /// [`dynrunner_protocol_primary_secondary::Destination`] to a
    /// concrete transport target by reading this coordinator's own role
    /// facts, then dispatch the `PeerId`-only transport. The transport
    /// never resolves a role.
    ///
    /// Resolution reads `cluster_state.current_primary()` with this
    /// primary's own `config.node_id` as the bootstrap fallback (the
    /// submitter primary IS the bootstrap primary), so
    /// `Destination::Primary` is always resolvable on the primary.
    /// `Secondary`/`Observer` destinations resolve to their carried host
    /// id; `All` is the mesh broadcast. A resolved host id equal to this
    /// node's own id is [`dynrunner_protocol_primary_secondary::SendTarget::Loopback`].
    pub(super) async fn send_to(
        &mut self,
        dst: dynrunner_protocol_primary_secondary::Destination,
        msg: dynrunner_protocol_primary_secondary::DistributedMessage<I>,
    ) -> Result<(), String> {
        use dynrunner_protocol_primary_secondary::{SendTarget, resolve_destination};
        let target = resolve_destination(
            dst,
            self.cluster_state.current_primary(),
            Some(&self.config.node_id),
            &self.config.node_id,
        )
        .ok_or_else(|| {
            "Destination unresolvable: no current primary in the role table".to_string()
        })?;
        match target {
            SendTarget::Peer(peer) => self.transport.send_to_peer(peer.as_str(), msg).await,
            // Mesh fan-out to every wire peer; ADDITIONALLY the co-located
            // own-secondary leg (when composed). The co-located secondary
            // is not a mesh peer of itself, so a `Destination::All`
            // broadcast — the CRDT mutation / `RunComplete` / keepalive
            // fan-out — reaches it ONLY through the loopback. The mesh
            // result is authoritative; a closed loopback is logged (the
            // own-secondary tore down) but does not fail the broadcast,
            // matching the per-leg-failure tolerance the keepalive emitter
            // already relies on.
            SendTarget::Broadcast => {
                if let Some(tx) = &self.colocated_loopback_tx
                    && tx.send(msg.clone()).is_err()
                {
                    tracing::debug!(
                        "co-located secondary inbound loopback closed during broadcast; \
                         own-secondary leg dropped (secondary torn down)"
                    );
                }
                self.transport.broadcast(msg).await
            }
            // Loopback: the resolved host id == this primary's own id.
            //
            // With a CO-LOCATED secondary composed (the on-demand
            // co-located primary): deliver in-process to the own-secondary's inbound.
            // This is the dominant own-host path — a `TaskAssignment` to
            // `Destination::Secondary(own_id)` resolves here, so dropping
            // it would lose the co-located secondary's work.
            //
            // Without a co-located secondary (the submitter primary,
            // in-process tests): the only reachable case is the
            // demoted-vs-live relay arm (`task/request.rs`) — a LIVE
            // primary that couldn't assign a `TaskRequest` locally falls
            // through to relay it to `Destination::Primary`, which is
            // itself. Re-delivering it to the node already holding (and
            // unable to assign) it is a benign no-op: the task stays in
            // this primary's pool and the secondary retries on its next
            // backoff tick. Faithful to the prior behaviour, where the
            // self-relay resolved to `send_to_peer(own_id)` → NoRoute →
            // swallowed.
            SendTarget::Loopback => match &self.colocated_loopback_tx {
                Some(tx) => tx.send(msg).map_err(|_| {
                    "co-located secondary inbound loopback closed".to_string()
                }),
                None => {
                    tracing::debug!(
                        "Destination resolved to self with no co-located secondary \
                         (live-primary self-relay); no-op — the message is already at this host"
                    );
                    Ok(())
                }
            },
        }
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

    /// Register the in-process loopback sender into a CO-LOCATED
    /// secondary's inbound. Set ONLY by the multi-role-host composition
    /// (the on-demand co-located primary), where this primary shares a host
    /// with a secondary it must deliver to without a wire hop. Pre-run
    /// contract, same one-shot shape as the other `register_*` setters.
    ///
    /// Once set, the egress edge ([`Self::send_to`]) delivers a resolved
    /// [`dynrunner_protocol_primary_secondary::SendTarget::Loopback`]
    /// (own-host unicast) AND the own-secondary leg of every
    /// `Destination::All` broadcast through this sender. Absent
    /// registration (the submitter primary, in-process tests) leaves the
    /// loopback `None`: own-host `Loopback` resolution is the benign
    /// live-primary self-relay no-op and broadcasts are the plain mesh
    /// fan-out. The sender carries the SAME `DistributedMessage` a wire
    /// frame would, so the receiving secondary processes it identically.
    pub fn register_colocated_loopback(
        &mut self,
        tx: tokio::sync::mpsc::UnboundedSender<DistributedMessage<I>>,
    ) {
        self.colocated_loopback_tx = Some(tx);
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

    /// Pre-register the per-phase lifecycle callbacks for an ON-DEMAND
    /// primary ([`Self::run_activated`]).
    ///
    /// The bootstrap path takes these as `run()` arguments;
    /// `run_activated` has no such arguments (it resumes off a snapshot,
    /// not a fresh pool-build), so the activator closure sets them on the
    /// freshly-built coordinator before spawning it. The operational loop
    /// and finalize tail read `self.on_phase_*` directly, so an
    /// on-demand-built primary fires the same `on_phase_start` /
    /// `on_phase_end` callbacks the bootstrap primary would.
    pub fn register_phase_lifecycle_callbacks(
        &mut self,
        on_phase_start: OnPhaseStart,
        on_phase_end: OnPhaseEnd,
    ) {
        self.on_phase_start = Some(on_phase_start);
        self.on_phase_end = Some(on_phase_end);
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

    /// Inverse of [`Self::local_worker_id_in_secondary`]: resolve the
    /// stable `(secondary_id, local_worker_id)` identity to the worker's
    /// CURRENT Vec index, or `None` if no such slot exists (the secondary
    /// died, or the local id is out of range). This is THE single
    /// identity-to-position resolution path: every consumer that holds a
    /// stable `(secondary_id, local_worker_id)` and needs to touch the
    /// live `self.workers[..]` entry routes through here rather than
    /// re-deriving the per-secondary running count inline.
    ///
    /// Single concern: identity translation. Critically, the result is
    /// recomputed against the LIVE Vec on every call, so a positional
    /// index can never be cached past a `self.workers.retain(..)` death
    /// compaction — the desync that a stored `Vec` index suffered.
    pub(super) fn worker_idx_for(&self, secondary_id: &str, local_worker_id: u32) -> Option<usize> {
        let mut local_idx: u32 = 0;
        for (idx, w) in self.workers.iter().enumerate() {
            if w.secondary_id == secondary_id {
                if local_idx == local_worker_id {
                    return Some(idx);
                }
                local_idx += 1;
            }
        }
        None
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
    /// parallel `if` at every call site. The call sites
    /// (`dispatch_to_idle_workers` + `handle_task_request`) stay
    /// agnostic to either policy.
    ///
    /// `bypass_backpressure` lifts ONLY the per-secondary backoff
    /// reason — never the OOM single-worker mask (that one is
    /// correctness for memory-pressed retries, not a transient
    /// rate-limit). A recheck driven by a genuine
    /// [`crate::worker_signal::WorkerMgmtSignal::TasksAdded`] passes
    /// `true`: circumstances changed (new work entered the pool, or a
    /// worker freed elsewhere), so a freed slot on a recently-
    /// backpressured secondary is a legitimate dispatch target again.
    /// The per-`TaskRequest` path and the periodic/non-signal kickstart
    /// pass `false` so a secondary that just said "no idle worker"
    /// isn't immediately re-hammered by its own request retry.
    pub(super) fn should_skip_worker_for_dispatch(
        &self,
        worker_idx: usize,
        bypass_backpressure: bool,
    ) -> bool {
        let sec_id = self.workers[worker_idx].secondary_id.as_str();
        if !bypass_backpressure && self.is_backpressured(sec_id) {
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
        let soft_predicate =
            preferred_secondaries::apply_preferred_secondaries_predicate::<I>(secondary_id);
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
        view.filter(preferred_secondaries::filter_strict_preferred_secondaries::<I>(secondary_id))
    }

    /// Drop the worker view down to the per-type-cap-eligible items.
    /// `None` for an axis means unconstrained; `Some(N)` means at
    /// most `N` items of that type can be in-flight across all
    /// workers. Items whose type's capacity is already reached are
    /// removed from the view so the scheduler never sees them.
    /// Commit a freshly-scheduled LOCAL assignment as one atomic
    /// in-flight-bookkeeping event: reserve the per-type concurrency
    /// slot, move the holding slot `Idle -> Assigned{task_hash}`, AND
    /// record the task in the hash-keyed `in_flight` ledger. Every
    /// local dispatch site (`handle_task_request`,
    /// `dispatch_to_idle_workers`, `perform_initial_assignment`) routes
    /// through here so the three pieces of "this task is now in flight"
    /// state can never diverge — they are written together or not at
    /// all. The `take_from_view` that removes the binary from the pool
    /// precedes this call (it yields the owned `task`).
    ///
    /// Reachable only from an `Idle` slot (enforced by
    /// `RemoteWorkerState::assign`'s `debug_assert`); the caller has
    /// already established idleness via the dispatch view / scheduler
    /// decision.
    pub(super) fn commit_assignment(
        &mut self,
        worker_idx: usize,
        task: TaskInfo<I>,
        task_hash: String,
        estimated: ResourceMap,
    ) {
        let secondary_id = self.workers[worker_idx].secondary_id.clone();
        // Record the STABLE secondary-local id (retain-immune), NOT the
        // positional Vec index — the terminal path re-resolves it via
        // `worker_idx_for` against the live Vec.
        let local_worker_id = self.local_worker_id_in_secondary(worker_idx);
        let phase = task.phase_id.clone();
        self.reserve_type_slot(&task.type_id);
        self.workers[worker_idx].assign(task_hash.clone(), task.clone(), estimated);
        self.in_flight.insert(
            task_hash,
            InFlightEntry {
                phase,
                secondary_id,
                local_worker_id: Some(local_worker_id),
                task,
            },
        );
    }

    /// Undo a `commit_assignment` whose `TaskAssignment` send failed:
    /// the task was never delivered, so no terminal will ever arrive
    /// for it. Symmetric inverse of `commit_assignment` — release the
    /// type slot, vacate the slot to `Idle`, drop the ledger entry —
    /// then the caller requeues the binary into the pool. Leaving any
    /// of the three would strand the slot, the ledger, or the type
    /// budget (the asm-tokenizer "33 in_flight / active=0" jam class).
    pub(super) fn rollback_assignment(
        &mut self,
        worker_idx: usize,
        task_hash: &str,
        type_id: &dynrunner_core::TypeId,
    ) {
        self.release_type_slot(type_id);
        self.workers[worker_idx].vacate();
        self.in_flight.remove(task_hash);
    }

    /// Seed an inherited in-flight task into the SAME hash-keyed ledger
    /// at hydration. `local_worker_id` is the secondary-local worker id
    /// the replicated `TaskState::InFlight { worker }` recorded at the
    /// originating dispatch — the SAME id `commit_assignment` writes on
    /// the live path (`local_worker_id_in_secondary`), so the inherited
    /// entry's stable `(secondary_id, local_worker_id)` holder key
    /// resolves through [`Self::worker_idx_for`] onto the matching
    /// reconstructed `RemoteWorkerState` slot. The slot itself is moved
    /// `Idle -> Assigned` by `reconstruct_workers_from_cluster_state`
    /// (the roster × occupancy crossing); this records the ledger half so
    /// the broadcast `TaskComplete` / `TaskFailed` finds the entry BY
    /// HASH and `free_slot_on_terminal` frees the held slot. Folds in the
    /// deleted `pre_owned_in_flight` concept: the terminal cascade reads
    /// this entry exactly like a locally-dispatched one.
    // Reached from `hydrate_from_cluster_state` (the composed primary's
    // seeded resume on failover activation).
    pub(super) fn seed_inflight(
        &mut self,
        task_hash: String,
        phase: PhaseId,
        secondary_id: String,
        local_worker_id: u32,
        task: TaskInfo<I>,
    ) {
        self.in_flight.insert(
            task_hash,
            InFlightEntry {
                phase,
                secondary_id,
                local_worker_id: Some(local_worker_id),
                task,
            },
        );
    }

    /// THE single terminal-free helper. Given an inbound terminal's
    /// `(secondary_id, worker_id, task_hash)`, free the holding slot
    /// back to `Idle`, release the per-type concurrency slot, AND
    /// remove the `in_flight` ledger entry — the symmetric inverse of
    /// `commit_assignment`. Returns the freed `InFlightEntry` (phase /
    /// secondary / task) so the caller runs ONLY the per-phase cascade
    /// (`note_item_*`); the type-slot release is owned here so the
    /// caller never has to know whether a slot was reserved.
    ///
    /// Returns `None` (a safe no-op) when:
    ///   - the addressed slot holds a DIFFERENT hash (the worker was
    ///     already reassigned to a later task `Y`; this terminal is a
    ///     stale/reordered completion for the prior task `X`), or
    ///   - the hash is absent from the ledger entirely (already
    ///     terminal / never tracked).
    ///
    /// Every live ledger entry now carries `local_worker_id = Some(..)`
    /// against a holding slot — locally-dispatched (`commit_assignment`)
    /// and inherited-on-failover (`seed_inflight`, whose slot is
    /// reconstructed by `reconstruct_workers_from_cluster_state`) alike.
    /// The `local_worker_id = None` arm therefore survives only as a
    /// defensive safe-no-op: an entry with no resolvable holder is
    /// removed from the ledger and returned without a slot vacate or a
    /// type-slot release — the deleted `pre_owned_in_flight` branch's
    /// "no local type-slot was ever taken" contract, now expressed
    /// through the one ledger.
    ///
    /// Because the slot's `task_hash` IS the held-task identity (the
    /// ledger key), a reassigned slot can NEVER be freed by a prior
    /// task's terminal: reassignment-before-terminal is unreachable.
    ///
    /// The holding slot is found by re-resolving the ledger entry's
    /// STABLE `(secondary_id, local_worker_id)` to a live Vec index via
    /// [`Self::worker_idx_for`] — recomputed on every call, so a
    /// sibling secondary's death (`self.workers.retain(..)` compacts the
    /// Vec) can never leave this pointing at the wrong worker or out of
    /// bounds. The inbound wire `worker_id` is retained for diagnostics;
    /// the ledger entry is the authoritative holder record.
    pub(super) fn free_slot_on_terminal(
        &mut self,
        secondary_id: &str,
        worker_id: u32,
        task_hash: &str,
    ) -> Option<InFlightEntry<I>> {
        // The ledger is the single source of truth. If the hash isn't
        // tracked, the task is not (or no longer) in flight — nothing
        // to free.
        let holder = match self.in_flight.get(task_hash) {
            Some(e) => e.local_worker_id.map(|lw| (e.secondary_id.clone(), lw)),
            None => {
                tracing::trace!(
                    secondary = %secondary_id,
                    worker_id,
                    task_hash = %task_hash,
                    "terminal for non-tracked hash; ignoring"
                );
                return None;
            }
        };

        match holder {
            // Locally-dispatched entry: a slot holds it. Resolve the
            // STABLE holder identity to a live index, then verify the
            // addressed slot still holds THIS hash before freeing — a
            // slot that has moved on to a later task must not be
            // vacated by a stale terminal.
            Some((holder_secondary, holder_local_id)) => {
                let idx = match self.worker_idx_for(&holder_secondary, holder_local_id) {
                    Some(idx) => idx,
                    // The holding worker is gone (its secondary died and
                    // the slot was dropped by the requeue path) yet a
                    // ledger entry survived. This is not the routine
                    // recovery path (which removes the entry), so treat
                    // it as a stale terminal for a slot that no longer
                    // exists: leave the ledger untouched and no-op.
                    None => {
                        tracing::trace!(
                            secondary = %secondary_id,
                            worker_id,
                            task_hash = %task_hash,
                            "terminal for hash whose holding slot no longer exists; ignoring"
                        );
                        return None;
                    }
                };
                let held_matches = matches!(
                    &self.workers[idx].state,
                    SlotState::Assigned { task_hash: h, .. } if h == task_hash
                );
                if !held_matches {
                    tracing::trace!(
                        secondary = %secondary_id,
                        worker_id,
                        task_hash = %task_hash,
                        "terminal for non-held hash; ignoring"
                    );
                    return None;
                }
                self.workers[idx].state = SlotState::Idle;
                let entry = self.in_flight.remove(task_hash)?;
                self.release_type_slot(&entry.task.type_id);
                Some(entry)
            }
            // Inherited (pre-owned) entry: no local slot, no reserved
            // type slot. Remove the ledger entry and return it — the
            // cascade still decrements the correct phase's counter.
            None => self.in_flight.remove(task_hash),
        }
    }

    /// Recover every in-flight task targeting `secondary_id` when that
    /// secondary dies: requeue each task into the pool (which
    /// decrements its phase's in-flight counter) and drop the ledger
    /// entry. Covers BOTH locally-dispatched (a slot held it — the
    /// slot is dropped separately by the caller) and inherited
    /// (pre-owned, no slot) tasks through the ONE ledger, mirroring the
    /// reference `check_peer_timeouts` recovery.
    ///
    /// Returns one `ClusterMutation::TaskRequeued { hash }` per requeued
    /// task so the async caller broadcasts the `InFlight → Pending`
    /// transition through `apply_and_broadcast_cluster_mutations` in
    /// lockstep with the local pool requeue. This method owns the
    /// in-flight-ledger + pool-side recovery (a sync, data-only concern);
    /// the CRDT replication is owned by the broadcast helper, so the
    /// mutation set is RETURNED rather than emitted here (mirroring the
    /// pool-returns-data / manager-broadcasts split). Without the
    /// returned mutation the local requeue would leave a stale CRDT
    /// `InFlight` that strands the task on failover (`hydrate` routes
    /// `InFlight` to the ledger, not the pool).
    ///
    /// Requeue is NOT a terminal outcome — the task re-enters
    /// `Pending` — so it never touches `completed_tasks`/`failed_tasks`.
    /// The per-type slot IS released (a requeued task no longer occupies
    /// concurrency budget) and `pool.requeue` decrements the phase
    /// in-flight counter, keeping the ledger, the type budget, and the
    /// pool counters consistent.
    pub(super) fn recover_inflight_for_dead_secondary(
        &mut self,
        secondary_id: &str,
    ) -> Vec<ClusterMutation<I>> {
        let hashes: Vec<String> = self
            .in_flight
            .iter()
            .filter(|(_, e)| e.secondary_id == secondary_id)
            .map(|(h, _)| h.clone())
            .collect();
        let mut requeue_mutations = Vec::with_capacity(hashes.len());
        for hash in hashes {
            if let Some(entry) = self.in_flight.remove(&hash) {
                self.release_type_slot(&entry.task.type_id);
                self.pool_mut().requeue(entry.task);
                requeue_mutations.push(ClusterMutation::TaskRequeued { hash });
            }
        }
        requeue_mutations
    }

    /// Starvation oracle for the lazy on-demand dead-secondary requeue.
    /// True IFF the ONLY outstanding work is in-flight on silent
    /// secondaries — i.e. an idle worker has nothing it could dispatch,
    /// and the only reason the run isn't done is that silent holders are
    /// sitting on inherited/dispatched in-flight tasks.
    ///
    /// Composed of single-concern reads — no liveness/dispatch policy
    /// `if`-hacks:
    ///   1. `∃ silent secondary` — the liveness module's silent-id set
    ///      ([`Self::silent_secondary_ids`]); empty ⇒ false.
    ///   2. `no queued dispatchable work for active phases` —
    ///      [`PendingPool::has_queued_dispatchable`]. (`is_empty()`/`len()`
    ///      are NOT usable: they fold in-flight + blocked, so they would be
    ///      false/`>0` precisely when silent in-flight work exists.)
    ///   3. `blocked == 0` — [`PendingPool::blocked_len`]. A blocked item
    ///      will become dispatchable once its prereq resolves, so evicting
    ///      a holder now would be premature.
    ///   4. `in_flight non-empty` — there is something to recover (and the
    ///      guard against evicting a healthy secondary near run completion,
    ///      paired with blocked==0).
    ///   5. `every in_flight entry's secondary is silent` — so a non-silent
    ///      secondary making progress is never evicted; if any in-flight
    ///      task is held by a live secondary, the run is still advancing.
    ///
    /// The boundary the dispatch consumer sees is this predicate plus
    /// [`Self::declare_silent_secondaries_dead`]; it never learns how
    /// "silent" or "dispatchable" are computed.
    pub(super) fn only_silent_held_work_remains(&self) -> bool {
        let silent = self.silent_secondary_ids();
        if silent.is_empty() {
            return false;
        }
        if self.pool().has_queued_dispatchable() {
            return false;
        }
        if self.pool().blocked_len() != 0 {
            return false;
        }
        if self.in_flight.is_empty() {
            return false;
        }
        self.in_flight
            .values()
            .all(|e| silent.contains(&e.secondary_id))
    }

    /// The silent secondaries currently holding the only remaining work,
    /// packaged as [`DeadSecondary`] declarations for
    /// [`Self::declare_silent_secondaries_dead`]. Pairs with
    /// [`Self::only_silent_held_work_remains`]: the oracle gates whether to
    /// declare; this enumerates WHOM, reusing the liveness silent-id set
    /// and the recorded keepalive timestamps.
    pub(super) fn silent_held_dead_declarations(&self) -> Vec<super::heartbeat::DeadSecondary> {
        let now = Instant::now();
        self.silent_secondary_ids()
            .into_iter()
            .map(|id| {
                let last_keepalive = self
                    .secondary_keepalives
                    .get(&id)
                    .copied()
                    .unwrap_or(now);
                super::heartbeat::DeadSecondary {
                    secondary_id: id,
                    last_keepalive,
                }
            })
            .collect()
    }

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
    /// `completed_tasks` HashSet — analogous to [`Self::outcome_summary`]
    /// which routes through the same CRDT reader. The `completed_tasks`
    /// HashSet stays authoritative for per-task identity decisions
    /// (dedup on a re-applied `TaskComplete`, the operational-loop exit
    /// gate, the kickstart-suppression check); cross-class *counts* live
    /// one layer up, on the replicated ledger every replica converges
    /// to. This is the same CRDT read every node (authority, peer, or
    /// observer) uses for cross-class count reporting — the per-node
    /// counter mirror that used to diverge from it is gone.
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
    /// to.
    ///
    /// O(n) over the cluster_state task ledger — same bound as the
    /// older `failed_tasks`-only walk; the additional Completed/
    /// Pending/InFlight inspections are constant-time per entry.
    pub fn outcome_summary(&self) -> OutcomeSummary {
        self.cluster_state.outcome_counts()
    }

    /// Setup-defer gate (CRDT-derived).
    ///
    /// True while this primary deferred discovery (`config.
    /// required_setup_on_promote`) AND the chosen secondary has not yet
    /// broadcast its first task into the replicated ledger
    /// (`cluster_state.task_count() == 0`). While true, `total_tasks`
    /// is 0 and every declared phase is `Active` with zero items — not
    /// because the run is empty but because discovery hasn't seeded the
    /// ledger yet. The counter-based run-complete exit and the
    /// per-phase `on_phase_end` drain must NOT fire in this window
    /// (a `0+0 >= 0` counter trip or a spurious empty-phase drain would
    /// declare the run done before any task exists). The gate clears the
    /// moment the first `TaskAdded` lands — exactly the flip condition
    /// the pre-demolition `setup_pending` latch used, now read off the
    /// CRDT every replica converges to.
    ///
    /// On the legacy bootstrap path (`required_setup_on_promote =
    /// false`) this is always `false`, so the gate is permanently
    /// satisfied and the normal exit/drain logic runs.
    ///
    /// The authoritative co-located primary owns this gate: it suppresses
    /// `run_complete_check`'s counter / pool-drain exits while discovery
    /// is pending (see `lifecycle/operational_loop.rs`) and arms the
    /// setup-promote-deadline backstop. The setup-deferred discovery feed
    /// (`ingest_setup_discovery`) seeds the ledger; the first `TaskAdded`
    /// flips this predicate false and normal dispatch resumes.
    pub(super) fn setup_pending(&self) -> bool {
        self.config.required_setup_on_promote && self.cluster_state.task_count() == 0
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

    /// Bootstrap primary-selection policy: which compute peer the
    /// submitter hands full primary authority off to once the mesh is
    /// warm. A pure `&self` accessor — no mutation, no side effects.
    ///
    /// # Policy
    ///
    /// Deterministic lowest-id among the candidate set, matching the
    /// failover election tie-break so bootstrap and failover share one
    /// selection *concept* (every replica that re-derives the choice gets
    /// the same answer; only the submitter actually originates the
    /// epoch-2 hand-off, but determinism removes any race ambiguity).
    ///
    /// # Candidate set
    ///
    /// A peer is eligible IFF it is, all positively:
    /// - an **alive worker-secondary** ([`alive_secondary_members`] —
    ///   already filters `worker_count > 0`, which structurally excludes
    ///   observers by capacity, AND `is_peer_alive`), AND
    /// - **mesh-ready** (it has reported `MeshReady`, recorded in
    ///   `mesh_ready_secondaries`), AND
    /// - a **confirmed mesh peer** ([`PeerTransport::has_peer`] — the
    ///   transport holds a live connection to it right now), AND
    /// - **primary-capable** — its EXPLICIT replicated `can_be_primary`
    ///   marker ([`ClusterState::can_be_primary`]) is set. This is the
    ///   single authoritative capability source: it is READ from the
    ///   first-class CRDT field (set by the peer at join via `PeerJoined {
    ///   can_be_primary }`, and updatable by a client via
    ///   `SetCanBePrimary`), NEVER re-derived from membership / liveness /
    ///   mesh-readiness. A peer that cannot construct a primary on demand
    ///   (`disable_peer_overlay`, no mesh, observer) joined with the
    ///   marker `false` and is thus never selected.
    ///
    /// and is NOT in the `RoleTable::observers` set (a defensive cut even
    /// though `worker_count > 0` already excludes observers — selection
    /// must never name an observer, mirroring the election's own guard).
    ///
    /// # Degenerate topologies
    ///
    /// Returns `None` when the candidate set is empty — single-node /
    /// submitter-only fleets, all-observer fleets, AND fleets where no
    /// peer set its `can_be_primary` marker (every `disable_peer_overlay`
    /// / no-mesh peer joined with `false`, or a client cleared it). The
    /// caller stays primary (`activate_local_primary`) in that case;
    /// "primary fully on one peer" holds trivially on the sole host, and a
    /// `disable_peer_overlay` cluster correctly keeps the submitter
    /// primary ("primary loss = job loss").
    ///
    /// [`alive_secondary_members`]: crate::cluster_state::ClusterState::alive_secondary_members
    /// [`ClusterState::can_be_primary`]: crate::cluster_state::ClusterState::can_be_primary
    pub fn select_bootstrap_primary(&self) -> Option<PeerId> {
        let observers = &self.cluster_state.role_table().observers;
        self.cluster_state
            .alive_secondary_members()
            .filter(|id| self.mesh_ready_secondaries.contains(*id))
            .filter(|id| self.transport.has_peer(&PeerId::from(*id)))
            .filter(|id| !observers.contains(*id))
            // POSITIVE capability cut on the EXPLICIT replicated marker —
            // never re-derived from the liveness/membership filters above.
            // A peer is a hand-off target only if it declared it can host
            // the primary on demand.
            .filter(|id| self.cluster_state.can_be_primary(id))
            .min()
            .map(PeerId::from)
    }

    /// Test-only inspector for the primary's replicated cluster
    /// ledger. Returns the per-state counts so tests can assert
    /// convergence with secondaries' mirrors.
    #[cfg(test)]
    pub fn cluster_state_counts_for_test(&self) -> crate::cluster_state::StateCounts {
        self.cluster_state.counts()
    }

    /// Test-only inspector for the total retry passes consumed across
    /// all `(phase, bucket)` keys. The authoritative retry cascade
    /// lives here on the primary (the secondary is a pure reporter), so
    /// retry tests assert on this counter rather than a secondary-side
    /// mirror (which no longer exists).
    #[cfg(test)]
    pub fn retry_passes_used_for_test(&self) -> u32 {
        self.retry_passes_used.values().sum()
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
    pub fn cluster_state_mut_for_test(&mut self) -> &mut crate::cluster_state::ClusterState<I> {
        &mut self.cluster_state
    }

    /// Test-only mutable borrow of the single mesh transport. Lets the
    /// composition routing hazard test drive a
    /// `send(Address::Peer(local_secondary_id), ..)` directly and assert
    /// it reaches the loopback secondary's inbound without echoing back
    /// to the primary.
    #[cfg(test)]
    pub fn transport_mut_for_test(&mut self) -> &mut Tr {
        &mut self.transport
    }

    /// Test-only count of workers the primary currently tracks as
    /// mid-dispatch (slot `Assigned`). Used by the composition hazard
    /// tests to assert a hydrated remote-in-flight task is NOT also
    /// counted as a local-active worker (the double-count hazard).
    #[cfg(test)]
    pub fn active_workers_for_test(&self) -> usize {
        self.workers.iter().filter(|w| !w.is_idle()).count()
    }

    /// Test-only count of ALIVE worker slots (idle + busy) — the same
    /// value the phase-floor liveness check's `alive_worker_count()`
    /// reads (`self.workers.len()`). Used by the roster-reconstruction
    /// tests to assert a promoted primary holds the full roster and is
    /// dispatch-capable (`> 0`) where it previously started empty.
    #[cfg(test)]
    pub fn alive_worker_count_for_test(&self) -> usize {
        self.workers.len()
    }

    /// Test-only inspector for the per-secondary staged-WARN counter
    /// (number of WARN stages already logged this silence streak). `None`
    /// when no stage has fired (or the streak was reset). Used by the
    /// fire-once / reset-on-recovery policy test.
    #[cfg(test)]
    pub fn silence_warn_stage_for_test(&self, secondary_id: &str) -> Option<usize> {
        self.silence_warn_stage.get(secondary_id).copied()
    }

    /// Test-only length of the hash-keyed in-flight ledger. Replaces
    /// the removed `pre_owned_in_flight_len_for_test`: the ledger now
    /// unifies locally-dispatched and inherited (pre-owned) in-flight
    /// tasks, so hydration tests assert against this single count.
    #[cfg(test)]
    pub fn in_flight_len_for_test(&self) -> usize {
        self.in_flight.len()
    }

    /// Test-only inspector: does the `(secondary_id, worker_id)` slot
    /// currently hold a task whose hash equals `task_hash`? Lets the
    /// reorder/reassignment tests assert the slot's held-task identity
    /// directly without reaching into `SlotState` internals.
    #[cfg(test)]
    pub fn slot_holds_hash_for_test(
        &self,
        secondary_id: &str,
        worker_id: u32,
        task_hash: &str,
    ) -> bool {
        self.worker_idx_for(secondary_id, worker_id)
            .map(|idx| {
                matches!(
                    &self.workers[idx].state,
                    SlotState::Assigned { task_hash: h, .. } if h == task_hash
                )
            })
            .unwrap_or(false)
    }

    /// Test-only inspector: is the `(secondary_id, worker_id)` slot
    /// idle? Mirrors `slot_holds_hash_for_test` for the negative
    /// assertion (a stale terminal must NOT free a reassigned slot).
    #[cfg(test)]
    pub fn slot_is_idle_for_test(&self, secondary_id: &str, worker_id: u32) -> bool {
        self.worker_idx_for(secondary_id, worker_id)
            .map(|idx| self.workers[idx].is_idle())
            .unwrap_or(false)
    }

    /// Test-only seam: register one idle remote worker owned by
    /// `secondary_id`. The composition flow's worker registration runs
    /// through the welcome / initial-assignment handshake the composed
    /// primary deliberately skips (it picks up a cluster that already
    /// handshaked pre-promotion); the dispatch hazard test seeds a
    /// worker directly so it can drive `dispatch_to_idle_workers` and
    /// assert the resulting `TaskAssignment` routes over the loopback.
    #[cfg(test)]
    pub fn register_idle_worker_for_test(
        &mut self,
        secondary_id: String,
        worker_id: u32,
        resource_budgets: ResourceMap,
    ) {
        self.workers.push(RemoteWorkerState {
            worker_id,
            secondary_id,
            resource_budgets,
            state: SlotState::Idle,
        });
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

    /// Test-only: register a worker and immediately stage one
    /// in-flight task on it, routed through the real
    /// `commit_assignment` lifecycle so the slot, the `in_flight`
    /// ledger, and the per-type slot are all seeded consistently
    /// (replaces the manual `RemoteWorkerState { current_task: Some(..)
    /// }` construction the removed two-field model allowed). Returns the
    /// computed task hash so the caller can drive a matching terminal.
    #[cfg(test)]
    pub fn stage_in_flight_for_test(
        &mut self,
        secondary_id: String,
        worker_id: u32,
        task: TaskInfo<I>,
    ) -> String {
        self.workers.push(RemoteWorkerState {
            worker_id,
            secondary_id,
            resource_budgets: ResourceMap::new(),
            state: SlotState::Idle,
        });
        let idx = self.workers.len() - 1;
        let task_hash = crate::primary::wire::compute_task_hash(&task);
        self.commit_assignment(idx, task, task_hash.clone(), ResourceMap::new());
        task_hash
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
        // Same per-run reset for the worker-management run-should-fail
        // outcome: only written when the worker-management arm drains a
        // `RunShouldFail`; a coordinator re-used across runs must not
        // inherit a stale outcome.
        self.worker_mgmt_fail_outcome = None;
        // Per-run reset for the ingest-time invalid-task directives:
        // `pending_run_abort` (3a) and `pending_invalid_dep_tasks` (#2)
        // are written by `ingest_initial_batch` below and read once at
        // their respective gates; a coordinator re-used across runs
        // must not inherit a previous run's ingest residue.
        self.pending_run_abort = None;
        self.pending_invalid_dep_tasks.clear();

        // The setup-pending gate is a CRDT-derived predicate
        // (`Self::setup_pending`) rather than a stateful latch field: in
        // setup-promote mode (`config.required_setup_on_promote`) it
        // stays true until the first task lands in the replicated ledger
        // (`cluster_state.task_count() > 0`), derived from the CRDT every
        // replica converges to. No per-run reset is needed because the
        // predicate reads live state. The co-located authoritative
        // primary owns this gate: `run_complete_check` suppresses its
        // exits while it holds and the operational loop arms the
        // setup-promote-deadline backstop; the setup-deferred discovery
        // feed (`ingest_setup_discovery`) seeds the ledger that flips it.

        // Spawn the peer-lifecycle + task-completion dispatchers BEFORE
        // any wire mutation can land. See `spawn_run_dispatchers`.
        self.spawn_run_dispatchers();

        // Discover the phase set: union of (1) every phase referenced
        // by an item, (2) every phase mentioned as a key or parent in
        // the deps map. The pool's constructor validates that every
        // dep references a known phase.
        let mut phase_set: HashSet<PhaseId> = binaries.iter().map(|b| b.phase_id.clone()).collect();
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
        // bucket, so we pre-sort here and ingest once.
        let mut sorted = binaries;
        sorted.sort_by_key(|b| std::cmp::Reverse(b.size));
        // INITIAL-batch ingest. Runs BEFORE `fire_initial_phase_starts`
        // below, so `ingest_initial_batch` is unconditionally the
        // pre-phase (#3a) side of the duplicate split. It runs the
        // dependency-existence partition (#2), pre-seeds the pool's
        // failed set for the missing-dep ids, extends the pool with the
        // VALID subset (preserving `extend`'s atomic contract — a cycle
        // among valid tasks stays a hard error), sets `all_binaries` /
        // `total_tasks`, and records the #3a abort + #2 invalid-dep
        // directives for `run_pipeline` to fire at their gates below.
        self.ingest_initial_batch(sorted)?;

        let total = self.total_tasks;
        tracing::info!(
            total,
            num_secondaries = self.config.num_secondaries,
            "primary starting"
        );

        // Take/put-back the command-channel receiver for the whole
        // pre-operational-loop chain: `wait_for_connections` below and the
        // initial-phase-start + empty-phase cascade (relocated to AFTER
        // connect — see the block past `wait_for_connections`) both need
        // `&mut Receiver` while also holding `&mut self`, which would alias
        // if we passed `&mut self.command_rx` directly. Mirrors the
        // discipline `operational_loop` uses (see
        // `lifecycle/operational_loop.rs:51`); the window between take and
        // put-back is benign here because we're still in `run` before the
        // operational loop has started — no concurrent sender access path
        // exists. Put-back happens after `wait_for_mesh_ready` so the
        // loop's own `self.command_rx.take()` re-acquires the same receiver.
        let mut command_rx = self.command_rx.take();

        // Phase 1+2: Wait for all secondaries to send welcome + cert exchange
        self.wait_for_connections(&mut command_rx).await?;

        // Fire on_phase_start for every phase the pool initialised as
        // Active (zero-deps phases), THEN cascade trivially-empty phases.
        // Subsequent activations triggered by `mark_phase_done` are
        // observed via `process_phase_lifecycle`.
        //
        // Ordering: the "all secondaries connected" milestone must precede
        // the first "starting job phase" milestone. This MUST run AFTER
        // `wait_for_connections` so the operator's "all secondaries
        // connected" milestone (`primary/connect.rs`) prints BEFORE the
        // "starting job phase" milestone `fire_initial_phase_starts` emits.
        // The relocation here is behaviour-preserving:
        //   * The fire-before-cascade COUPLING is kept intact (the cascade's
        //     `on_phase_end(.., 0, 0)` for an empty initial phase must come
        //     AFTER that phase's `on_phase_start`, so `fire_initial_phase_starts`
        //     stays immediately before the cascade).
        //   * `wait_for_connections` reads no phase-start state (only
        //     `self.secondaries` connection states + `num_secondaries`), so
        //     seeding phases after it changes nothing it observes.
        //   * The 3a/3b duplicate discriminator is STRUCTURAL (the code
        //     path — `ingest_initial_batch` vs `apply_spawn_tasks`), not a
        //     runtime read of `phase_started_emitted`; the only runtime read
        //     of `phase_started_emitted` is `fire_initial_phase_starts`' own
        //     `insert` guard, and `ingest_initial_batch`'s `debug_assert!`
        //     (must run before fire) is satisfied — ingest still runs far
        //     above, before connect.
        //   * No `on_phase_end` can fire DURING `wait_for_connections`: no
        //     task has been assigned yet (`perform_initial_assignment` runs
        //     below, after connect), and because `drain_empty_active_phases`
        //     now runs only AFTER connect, `drained_pending` is empty during
        //     connect — so any inbound-driven `process_phase_lifecycle`
        //     (`dispatch_message`'s TaskComplete/TaskFailed cascade) is a
        //     guaranteed no-op there. The on_phase_start-before-on_phase_end
        //     contract is therefore preserved.
        self.fire_initial_phase_starts();

        // Trivially-empty Active phases (no items at all) need to drain
        // and cascade Done before initial assignment, otherwise their
        // `Blocked` dependents — which may hold all the run's actual
        // work — never become visible to `view_for_worker`. Triggers
        // `on_phase_end(.., 0, 0)` for each empty phase via the
        // lifecycle cascade. Runs after connect but BEFORE the
        // seed/assignment step below, preserving the load-bearing
        // "cascade before initial assignment" ordering.
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
        // Required at this pre-loop site (not optional): a consumer
        // `on_phase_end` callback fired by the initial-empty-phase
        // cascade can itself queue `spawn_tasks(next_phase_items)`,
        // and the cascade's next `drain_empty_active_phases` poll
        // would otherwise false-fire `on_phase_end(.., 0, 0)` on the
        // successor phase exactly the way the in-loop bug class did.
        // The `command_rx` taken above is handed into
        // `process_phase_lifecycle` so the cascade's per-iteration drain
        // step picks up callback-queued `SpawnTasks` / `FailPermanent` /
        // `ReinjectTask` / `UpdatePreferredSecondaries` commands inline.
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
        if !self.setup_pending() {
            self.pool_mut().drain_empty_active_phases();
            self.process_phase_lifecycle(&mut command_rx).await;
        }

        // #3a abort gate. `ingest_initial_batch` recorded a pending
        // abort iff the INITIAL batch had a `(phase_id, task_id)`
        // duplicate (pre-phase). Fire it HERE — the first point the
        // secondaries are connected — so the `RunAborted` broadcast
        // reaches them (at ingest time none were connected). Returns
        // `Err(RunError::DuplicateTaskIdPrePhase)` on the abort path
        // (the primary's PyO3 boundary surfaces a non-zero exit); a
        // no-op on the clean path. Hard cluster shutdown — short-
        // circuits before any seeding / assignment.
        self.fire_pending_run_abort().await?;

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

        // The primary is a first-class mesh member: register its own
        // host-id in every replica's `peer_state` / `RoleTable` / relay
        // membership via a self-authored `PeerJoined`, the same CRDT path
        // the secondary accept site uses for each secondary. Originated
        // here — after the fleet is connected (so the broadcast reaches
        // every secondary) and BEFORE the seed/setup-defer branch below
        // (so membership is recorded uniformly in both modes). Membership
        // only: this does NOT announce `PrimaryChanged` and does NOT add
        // the primary to the `PeerInfo` dial-list.
        self.originate_primary_membership().await;

        // Phase 4.5 + Phase 5: Seed the replicated cluster ledger and
        // perform the initial per-secondary assignment. Both steps are
        // skipped when this primary is operating in setup-defer mode
        // (`required_setup_on_promote` — i.e. the submitter was
        // launched with `--source-already-staged` and has no local
        // view of the corpus to discover or seed); in that mode the
        // chosen secondary runs task discovery + ledger seed after the
        // bootstrap promotion (the discovery-yield rides
        // `InitialAssignment { pre_staged_mode: true }`, emitted by
        // `emit_setup_defer_handshake` below). To keep the secondaries'
        // `wait_for_setup`
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
            // #2 missing-dep emit. `seed_cluster_state` has now added
            // every initial task — including the missing-dep ones — to
            // the CRDT as `Pending`, so the `TaskFailed { InvalidTask }`
            // emit transitions each `Pending → InvalidTask` (the apply
            // rule also fans a `TaskCompletedEvent` carrying the
            // `invalid_task:<reason>` kind, the framework's emission for
            // the observer monitor). No-op when the partition found no
            // missing-dep tasks. The cluster continues; these tasks are
            // never dispatched (the pool pre-seeded them as failed at
            // ingest), so `perform_initial_assignment` skips them.
            self.emit_invalid_dep_tasks().await;
            self.perform_initial_assignment().await?;
        }

        // Phase 6: Send transfer complete
        self.send_transfer_complete().await?;

        // Phase 6.5: Wait for every connected secondary to report
        // its peer-mesh has settled before announcing the primary.
        // Pre-fix the `PrimaryChanged` announcement fired ~750µs
        // after cert-exchange completed — the newly-named
        // primary then became authoritative against a still-
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

        // Put the command-channel receiver back on `self` so
        // `operational_loop`'s own `self.command_rx.take()` picks
        // it up again (on the paths that run it). Symmetric with the
        // take at the top of the pre-loop chain. The observer tail does
        // not consult `command_rx`; restoring it here keeps the field's
        // ownership symmetric across every fork branch.
        self.command_rx = command_rx;

        // Initial-setup-done important event. This is the honest
        // once-per-run "all initial setup complete, entering steady-state"
        // milestone for the OPERATOR's submitter process: the fleet is
        // connected (`wait_for_connections`), staged
        // (`maybe_auto_stage_initial`), peer-linked (`send_peer_lists` +
        // `wait_for_peer_connections`), the primary's own membership is
        // recorded (`originate_primary_membership`), the ledger is seeded +
        // tasks assigned (`seed_cluster_state` + `perform_initial_assignment`)
        // OR the setup-defer handshake is emitted (`emit_setup_defer_handshake`),
        // transfer-complete is sent (`send_transfer_complete`), and the peer
        // mesh has settled (`wait_for_mesh_ready`). Both modes reconverge here
        // (the `required_setup_on_promote` if/else above rejoins before
        // `send_transfer_complete`), so a single emit at this point fires
        // EXACTLY ONCE regardless of non-defer vs setup-defer mode.
        //
        // Placed BEFORE the bootstrap hand-off fork so it is NOT
        // double-emitted after a relocation: the chosen peer's on-demand
        // primary enters via `run_activated_pipeline`, which bypasses this
        // whole connect/seed/mesh-ready chain (it inherits the already-formed
        // mesh and resumes from the restored snapshot), so it never reaches
        // this line. The submitter — whether it stays primary
        // (`activate_local_primary`), relocates and observes
        // (`run_as_observer`), or falls back to local — emits this once and
        // only once, before any of those forks run.
        tracing::info!(
            target: super::important_events::IMPORTANT_TARGET,
            "initial setup done",
        );

        // Bootstrap hand-off fork. The submitter bootstrapped the mesh as
        // the temporary primary WITHOUT a self-announce (it never
        // originated a `PrimaryChanged` for itself — the bootstrap pin is
        // not an announce, so `primary_epoch()` is still 0 here); now it
        // relocates FULL authority to a chosen compute peer and becomes an
        // observer, so the cluster never splits primary work across two
        // hosts and is never pinned to the submitter (invariant 4). The
        // fork is on `select_bootstrap_primary` (the deterministic
        // lowest-id, positively `can_be_primary`-marked candidate policy):
        //
        //   * `None` (degenerate single-node / all-observer fleet, or no
        //     peer is `can_be_primary` — e.g. a `disable_peer_overlay`
        //     cluster) — there is no hand-off target, so the submitter
        //     STAYS the full primary: `activate_local_primary` (originates
        //     its first self-announce at epoch 1, warms the role cache,
        //     emits a keepalive) then the shared
        //     operational-loop-and-finalize tail, UNCHANGED from the
        //     pre-fork bootstrap behaviour. This is the ONLY path that
        //     keeps `activate_local_primary` on the submitter.
        //   * `Some(chosen)` — relocate authority onto `chosen`
        //     (`relocate_primary_to` originates `PrimaryChanged { chosen,
        //     reason: Transferred }` at epoch `primary_epoch()+1 = 1`; the
        //     submitter's own apply drops `Role::Primary`). On a successful
        //     relocation the submitter enters the observer tail
        //     (`run_as_observer`), which watches the replicated
        //     `cluster_state` for `RunComplete` and never runs the pool /
        //     operational loop. If `chosen` evaporated between selection
        //     and origination, `relocate_primary_to` falls back to local
        //     primary and the submitter runs the normal operational path
        //     so it never strands on a vanished candidate.
        //
        // `wait_for_mesh_ready` above already held until the peer mesh
        // settled, so every PrimaryChanged announce (self or chosen) warms
        // each replica's role cache to a real connection.
        match self.select_bootstrap_primary() {
            None => {
                self.activate_local_primary().await?;
                self.run_operational_and_finalize(total).await
            }
            Some(chosen) => match self.relocate_primary_to(chosen).await? {
                RelocationOutcome::Relocated => self.run_as_observer().await.map_err(RunError::from),
                RelocationOutcome::FellBackToLocal => self.run_operational_and_finalize(total).await,
            },
        }
    }

    /// Shared operational-loop-and-finalize tail. The single mechanism
    /// both handoff sides converge on once their pool is seeded:
    /// bootstrap (`run_pipeline`, pool built from `binaries`) and
    /// on-demand activation (`run_activated`, pool hydrated from the
    /// restored CRDT snapshot at `activate_local_primary`). Runs the main
    /// operational loop,
    /// the structured-abort checks (panik / worker-mgmt-fail /
    /// setup-deadline), the retry passes, final accounting, and the
    /// `RunComplete` broadcast + settle window.
    ///
    /// `total` is the run's task count, captured by the caller from
    /// `self.total_tasks` after seeding so the stranded accounting is
    /// identical on both paths.
    async fn run_operational_and_finalize(&mut self, total: usize) -> Result<(), RunError> {
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

        // Worker-management run-should-fail check: if the operational
        // loop's worker-management arm recorded a break outcome (a
        // `RunShouldFail` signal — emitted by the phase layer's
        // proceed-or-fail decision OR the phase-floor liveness check),
        // surface it as a typed failure and skip the retry-pass / drain
        // / accounting tail. The worker arm OWNS the clean-shutdown
        // drive; the phase/task layer that emitted the signal never
        // broke the loop directly (decoupling law). Same write-by-arm /
        // read-by-pipeline discipline as `panik_outcome`.
        if let Some(reason) = self.worker_mgmt_fail_outcome.take() {
            tracing::error!(
                reason = %reason,
                "primary run aborted by worker-management run-should-fail signal"
            );
            return Err(RunError::Other(reason));
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
        self.drain_pending_messages(Duration::from_millis(500))
            .await?;

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
        self.apply_and_broadcast_cluster_mutations(vec![ClusterMutation::RunComplete])
            .await;

        // Brief settle window so the broadcast lands on every
        // secondary before the dispatcher tears down its transport.
        // Without this, fast dispatcher exits race the broadcast and
        // some peers miss the signal — the symptom is leftover SLURM
        // jobs in CG state for the wrappers whose secondaries didn't
        // see RunComplete. See `PRIMARY_BROADCAST_SETTLE` for the
        // rationale (shared with the `RunAborted` path).
        tokio::time::sleep(super::PRIMARY_BROADCAST_SETTLE).await;

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

    /// The submitter's observer tail, entered after `relocate_primary_to`
    /// has handed full primary authority off to a chosen compute peer.
    ///
    /// # Concern
    ///
    /// Observer-ness is a CAPABILITY set, not a fourth role/FSM
    /// (invariant 2): post-relocation the submitter holds no
    /// `Role::Primary`, runs no dispatch pool, and drives no phase
    /// machine — it only WATCHES the replicated `cluster_state` for the
    /// authoritative primary's `RunComplete` and returns. It is NOT the
    /// full-SecondaryCoordinator observer late-joiner
    /// (`managers/observer_late_joiner/run.rs`); the submitter is
    /// primary-only with no composed secondary, so this is a thin
    /// purpose-built select-loop on the PrimaryCoordinator.
    ///
    /// # The select
    ///
    ///   * `recv_peer` — every inbound frame. ONLY `ClusterMutation`
    ///     batches are acted on, through the existing RECEIVE-apply path
    ///     (`handle_cluster_mutation`): apply-only, NEVER re-broadcast,
    ///     because the observer is not the authority. Any other wire shape
    ///     (TaskRequest etc.) is ignored — the observer must not assign or
    ///     coordinate. A `None` (transport inbound closed) ends the watch:
    ///     no further mutation can arrive, so the run's outcome is frozen.
    ///   * `setup_promote_deadline` — copied verbatim from the operational
    ///     loop, gated on `self.setup_pending()`. A hung discovery on the
    ///     chosen peer (no `TaskAdded` / `TasksSpawned` / `RunComplete`
    ///     within `config.setup_promote_deadline`) must still terminate the
    ///     observer rather than hang forever; non-setup-promote relinquish
    ///     never arms it (the gate is permanently false).
    ///   * `fleet_dead_timeout` grace — a dead-fleet backstop. The
    ///     operational loop's fleet-dead arm reads pool/authority state the
    ///     relinquished observer no longer owns, so it is NOT copied here
    ///     (RESIDUAL-RISK-#3). Instead the observer's dead-fleet grace is
    ///     gated purely on the Phase-A role-blind `transport.peer_count()`
    ///     reaching zero: with every peer gone there is no authority left
    ///     to ever broadcast `RunComplete`, so after `config.
    ///     fleet_dead_timeout` of continuous emptiness the observer exits
    ///     with a stranded-fleet error (reusing the existing grace value,
    ///     not a new timer).
    ///   * `primary_silence` backstop — the half-open-peer case the
    ///     dead-fleet grace cannot see. In the relocation model the
    ///     relocated primary's death is abnormal (SIGTERM / exit 137 / a
    ///     dropped mesh-send handle) with NO clean `RunComplete`. The
    ///     `peer_count()` is raw QUIC connection cardinality, pruned ONLY
    ///     on an outbound-send failure — and the observer is apply-only,
    ///     never sends, so a dead-primary connection stays resident,
    ///     `peer_count()` stays `> 0`, and the dead-fleet arm never arms.
    ///     The observer therefore tracks `primary_last_seen` directly
    ///     (mirroring the secondary's `record_primary_message` →
    ///     `primary_last_seen`): it is refreshed by a `Primary`-tagged
    ///     keepalive whose originator IS `cluster_state.current_primary()`
    ///     (surfaced from the recv arm, inspect-only, never re-broadcast)
    ///     and by any `PrimaryChanged` that re-points `current_primary()`
    ///     (a new/refreshed primary is live). If the current primary is
    ///     silent for `config.peer_timeout` with no `RunComplete`, the run
    ///     is stranded — either the whole fleet died or a failover failed —
    ///     so the observer exits `Err`. `peer_timeout` (default 300s) is
    ///     GENEROUS relative to a surviving-secondary failover cycle
    ///     (`keepalive_miss_threshold * keepalive_interval` ≈ 15s, then a
    ///     re-election whose new primary's keepalives refresh
    ///     `primary_last_seen`), so a legitimate failover rides through and
    ///     this backstop only fires when failover did NOT rescue the run.
    ///     Independent of `peer_count()` (un-pruned / unreliable here).
    ///
    /// # Termination
    ///
    /// `cluster_state.run_complete()` is checked at the TOP of every
    /// iteration, so a `RunComplete` the submitter already applied (e.g.
    /// during the hand-off window) returns `Ok(())` immediately without
    /// blocking on a recv. The check is epoch-agnostic — a chosen primary
    /// that died and was superseded by a re-elected epoch-≥3 primary still
    /// produces the same `RunComplete` the observer watches for.
    pub(crate) async fn run_as_observer(&mut self) -> Result<(), String> {
        // `setup_promote_loop_start` is captured locally so the deadline
        // measures from observer-tail entry — the same shape the
        // operational loop's deadline arm uses (see `operational_loop`).
        let setup_promote_loop_start = Instant::now();
        let setup_promote_deadline_at =
            setup_promote_loop_start + self.config.setup_promote_deadline;
        // One-shot gate so the setup-promote arm fires at most once.
        let mut setup_promote_deadline_consumed = false;

        // Dead-fleet grace clock: the first moment `peer_count()` is
        // observed at zero. Cleared the moment a peer is present again
        // (partial fleet survival / re-handshake). Mirrors the operational
        // loop's `fleet_dead_since` discipline but is gated on the
        // role-blind transport peer-count, never a pool read.
        let mut fleet_dead_since: Option<Instant> = None;

        // Inbound-closed latch: once `recv_peer` returns `None` the arm is
        // disabled (so the select! does not hot-poll a resolved future)
        // and the loop exits at the top of the next iteration. Mirrors the
        // operational loop's `transport_closed` shape.
        let mut transport_closed = false;

        // Primary-liveness clock for the half-open-peer backstop. Mirrors
        // the secondary's `OperationalState::primary_last_seen` (refreshed
        // by `record_primary_message`). The relocated primary was just
        // alive at hand-off, so initialise to `now`. Refreshed in the recv
        // arm by a `Primary`-tagged keepalive from the CURRENT primary and
        // by any `PrimaryChanged` that re-points `current_primary()`. When
        // the current primary is silent past `config.peer_timeout` with no
        // `RunComplete`, the run is stranded and the observer exits — the
        // dead-fleet `peer_count()==0` arm cannot catch this because a
        // SIGTERM'd primary's connection stays resident (the observer never
        // sends, so the send-failure prune never runs).
        let mut primary_last_seen = Instant::now();

        // Dead-fleet poll tick. The dead-fleet grace is accumulated at the
        // TOP of the loop, but when every peer is gone there is no recv
        // traffic to re-iterate the loop and re-evaluate it. This tick is
        // the wake source: it fires on the `fleet_dead_timeout` cadence so
        // a fully-silent observer still re-checks (and exits) within one
        // grace window. Cadence-only — it carries no work itself; the
        // exit decision stays at the top-of-loop check. The immediate
        // first tick is consumed so the cadence starts one interval out.
        let mut fleet_dead_poll = tokio::time::interval(self.config.fleet_dead_timeout);
        fleet_dead_poll.tick().await;

        // Operator run-narration over the replicated CRDT. After this
        // bootstrap relocation the operator's process IS this observer, so
        // the run narrative (phase started / complete, the one-shot
        // run-complete-or-aborted summary) must be emitted HERE — reading
        // the replicated `cluster_state` directly — rather than by the new
        // primary on a different node's stdout. The narrator is a pure,
        // idempotent differ; `observe()` is called at the TOP of every
        // iteration BEFORE the `run_complete()` early-return below, so the
        // iteration that detects completion emits the summary before the
        // loop returns.
        let mut narrator = crate::run_narrator::RunNarrator::new();

        loop {
            narrator.observe(&self.cluster_state);

            // Happy-path exit: the authoritative primary declared the run
            // over (sticky monotonic flag). Checked first so a RunComplete
            // applied before this loop even entered returns immediately.
            if self.cluster_state.run_complete() {
                tracing::info!(
                    "observer tail: cluster_state.run_complete() — run is over, \
                     submitter observer exiting"
                );
                return Ok(());
            }

            // Inbound closed WITH the fleet still present: no further
            // mutation can arrive yet peers remain (a clean drain where the
            // inbound senders were dropped but the mesh is otherwise live),
            // so the run's outcome is frozen and the watch cannot make
            // progress — exit cleanly. When the fleet is ALSO dead
            // (`peer_count() == 0`), a closed inbound is NOT a clean exit:
            // it is the stranded-run condition the dead-fleet grace below
            // owns (every peer gone with no `RunComplete` ⇒ Err), so fall
            // through to it rather than masking a strand as `Ok`.
            if transport_closed && self.transport.peer_count() > 0 {
                tracing::info!(
                    "observer tail: transport inbound closed while peers remain; \
                     submitter observer exiting"
                );
                return Ok(());
            }

            // Dead-fleet grace (RESIDUAL-RISK-#3): gated purely on the
            // role-blind `peer_count()`, NOT on the pool the observer
            // relinquished. With every peer gone, no authority remains to
            // ever broadcast `RunComplete`.
            if self.transport.peer_count() == 0 {
                let now = Instant::now();
                let since = *fleet_dead_since.get_or_insert(now);
                let elapsed = now.duration_since(since);
                if elapsed >= self.config.fleet_dead_timeout {
                    tracing::error!(
                        elapsed_s = elapsed.as_secs_f64(),
                        timeout_s = self.config.fleet_dead_timeout.as_secs_f64(),
                        "observer tail: every peer gone with no RunComplete; \
                         exiting on the fleet-dead grace"
                    );
                    return Err(format!(
                        "observer fleet-dead: every peer left the mesh and no \
                         RunComplete was broadcast within {:.1}s",
                        self.config.fleet_dead_timeout.as_secs_f64()
                    ));
                }
            } else {
                // Fleet present (or recovered); reset the grace clock so a
                // later emptiness measures from its own start.
                fleet_dead_since = None;
            }

            // Primary-silence backstop (the half-open-peer strand). When a
            // primary is named but its keepalives have stopped for longer
            // than `peer_timeout` — with no `RunComplete` — either the whole
            // fleet died or a failover failed to produce a live successor,
            // so the run is stranded and the observer must exit rather than
            // block forever on a recv that will never deliver. Gated on
            // `current_primary().is_some()` so a pre-relocation observer
            // (no primary yet) never trips it. Independent of `peer_count()`
            // (un-pruned for an apply-only observer, so unreliable here).
            // `peer_timeout` (default 300s) >> a failover cycle (~15s), so a
            // legitimate failover refreshes `primary_last_seen` (via the new
            // primary's keepalives / `PrimaryChanged`) well before this
            // fires — it only fires when failover did NOT rescue the run.
            if let Some(primary) = self.cluster_state.current_primary() {
                let silent_for = Instant::now().duration_since(primary_last_seen);
                if silent_for > self.config.peer_timeout && !self.cluster_state.run_complete() {
                    tracing::error!(
                        primary,
                        silent_s = silent_for.as_secs_f64(),
                        timeout_s = self.config.peer_timeout.as_secs_f64(),
                        "observer tail: current primary silent past peer_timeout with no \
                         RunComplete — run stranded"
                    );
                    return Err(format!(
                        "observer: current primary {primary} silent for {:.0}s with no \
                         RunComplete — run stranded",
                        silent_for.as_secs_f64()
                    ));
                }
            }

            tokio::select! {
                msg = self.transport.recv_peer(), if !transport_closed => {
                    match msg {
                        Some(m) => {
                            // Apply-only RECEIVE path: the observer is NOT
                            // the authority, so it mirrors `ClusterMutation`
                            // batches into its `cluster_state` (the source
                            // for `run_complete()` and the replicated result
                            // ledger) and NEVER re-broadcasts. A `Primary`
                            // keepalive from the current primary is
                            // INSPECTED (never re-broadcast) to refresh the
                            // primary-liveness clock. Every other wire shape
                            // is ignored — the observer assigns no tasks and
                            // coordinates nothing.
                            match m.msg_type() {
                                MessageType::ClusterMutation => {
                                    // A `PrimaryChanged` riding this batch
                                    // re-points `current_primary()`; the
                                    // newly-named primary is live by
                                    // construction (the relocate/election
                                    // that announced it), so detect the
                                    // re-point across the apply and refresh
                                    // the liveness clock. Snapshot the id
                                    // BEFORE the `&mut self` apply (it
                                    // borrows `cluster_state`).
                                    let before =
                                        self.cluster_state.current_primary().map(str::to_owned);
                                    self.handle_cluster_mutation(m).await;
                                    let after =
                                        self.cluster_state.current_primary().map(str::to_owned);
                                    if after.is_some() && after != before {
                                        primary_last_seen = Instant::now();
                                        tracing::debug!(
                                            primary = ?after,
                                            "observer tail: PrimaryChanged repointed the \
                                             current primary — refreshing primary-liveness clock"
                                        );
                                    }
                                }
                                MessageType::Keepalive => {
                                    // Inspect-only primary-liveness refresh.
                                    // A `Primary`-tagged keepalive whose
                                    // ORIGINATOR (`secondary_id`) IS the
                                    // current primary is a primary-liveness
                                    // assertion — mirrors the secondary's
                                    // `handle_inbound` Keepalive arm
                                    // (`record_primary_message`). A stray
                                    // `Primary` keepalive from a non-current
                                    // id, or any `Secondary` keepalive, is
                                    // not primary liveness and is ignored.
                                    // The observer NEVER re-broadcasts.
                                    if let DistributedMessage::Keepalive {
                                        secondary_id,
                                        emitter_role: KeepaliveRole::Primary,
                                        ..
                                    } = &m
                                        && self.cluster_state.current_primary()
                                            == Some(secondary_id.as_str())
                                    {
                                        primary_last_seen = Instant::now();
                                        tracing::trace!(
                                            primary = %secondary_id,
                                            "observer tail: primary keepalive — \
                                             refreshing primary-liveness clock"
                                        );
                                    }
                                }
                                other => {
                                    tracing::trace!(
                                        msg_type = ?other,
                                        "observer tail: ignoring non-ClusterMutation frame"
                                    );
                                }
                            }
                        }
                        None => {
                            transport_closed = true;
                            tracing::debug!(
                                "observer tail: transport.recv_peer() returned None; \
                                 disabling the inbound arm"
                            );
                        }
                    }
                }
                // Setup-promote-deadline arm. Copied from the operational
                // loop (`operational_loop`): gated on `self.setup_pending()
                // && !setup_promote_deadline_consumed` so it is disabled the
                // moment discovery seeds the ledger, and re-checked at fire
                // time so a TaskAdded landing in the same tick is not raced.
                // The submitter no longer runs the operational loop, so the
                // backstop against a hung setup-defer discovery must live
                // here.
                _ = tokio::time::sleep_until(setup_promote_deadline_at.into()),
                    if self.setup_pending() && !setup_promote_deadline_consumed => {
                    setup_promote_deadline_consumed = true;
                    if self.setup_pending() {
                        let elapsed = setup_promote_loop_start.elapsed();
                        tracing::error!(
                            elapsed_s = elapsed.as_secs_f64(),
                            deadline_s = self.config.setup_promote_deadline.as_secs_f64(),
                            "observer tail: setup-promote deadline expired — the \
                             chosen primary's discovery feed never seeded the ledger \
                             (no TaskAdded / TasksSpawned / RunComplete)"
                        );
                        self.setup_deadline_outcome = Some(elapsed);
                        return Err(RunError::SetupDeadlineExpired { elapsed }.to_string());
                    }
                }
                // Dead-fleet poll wake. Carries no work — it only re-drives
                // the loop so the top-of-loop dead-fleet grace check
                // re-evaluates when there is no recv traffic to do so.
                _ = fleet_dead_poll.tick() => {}
            }
        }
    }

    /// Spawn the peer-lifecycle + task-completion dispatcher tasks.
    ///
    /// The (sender, receiver) pairs were built in `new()` and the
    /// senders already installed on `cluster_state`; here we hand each
    /// receiver and its registered listeners to a `spawn_local`'d
    /// dispatcher task. The returned `JoinHandle`s are stored on `self`
    /// so the `run`/`run_activated` outer wrappers can abort + join them
    /// on every exit path (a leaked dispatcher would otherwise block
    /// forever on its input channel, whose sender lives on
    /// `cluster_state` which the coordinator still owns post-run).
    ///
    /// Single-shot by contract: the `take()`s leave `None` behind, so a
    /// re-entrant caller silently skips. Called by both the bootstrap
    /// (`run_pipeline`) and on-demand activation (`run_activated`) paths
    /// BEFORE any wire mutation can land.
    fn spawn_run_dispatchers(&mut self) {
        if let Some(rx) = self.lifecycle_rx.take() {
            let listeners = std::mem::take(&mut self.peer_lifecycle_listeners);
            let handle = tokio::task::spawn_local(
                crate::peer_lifecycle::run_peer_lifecycle_dispatcher(rx, listeners),
            );
            self.lifecycle_dispatcher_handle = Some(handle);
        }
        if let Some(rx) = self.task_completed_rx.take() {
            let listeners = std::mem::take(&mut self.task_completed_listeners);
            let handle = tokio::task::spawn_local(
                crate::task_completed::run_task_completed_dispatcher(rx, listeners),
            );
            self.task_completed_dispatcher_handle = Some(handle);
        }
    }

    /// Run a co-located primary ACTIVATED ON DEMAND: the moment a peer
    /// becomes the primary (failover-self election win OR a bootstrap
    /// transfer naming it), its `SecondaryCoordinator`'s apply / setup-FSM
    /// site CONSTRUCTS this `PrimaryCoordinator` and immediately drives it
    /// here — there is no pre-parked object and no promotion gate. The two
    /// coordinators share one `LocalSet`; this future is `spawn_local`'d
    /// the instant authority lands.
    ///
    /// This enters the SEEDED resume directly: `restore` the
    /// `snapshot` the secondary captured from its continuously-mirrored
    /// `cluster_state` at the activation moment, then
    /// `activate_local_primary` hydrates the pool + unified `in_flight`
    /// ledger from that restored CRDT, then the SAME
    /// operational-loop-and-finalize tail runs. The connect / mesh-ready
    /// handshake is BYPASSED entirely — an on-demand-built primary
    /// inherits the already-formed mesh its host's secondary established
    /// (it sends/receives over the host's shared mesh transport), so there
    /// is nothing to wait for.
    ///
    /// The freshly-built primary's own `cluster_state` is empty (it never
    /// bootstrapped). The `snapshot` IS the ledger it resumes from: for a
    /// failover-self build it is the surviving secondary's mirror; for a
    /// bootstrap-transfer build it is the chosen peer's setup-seeded mirror
    /// (the submitter's `seed_cluster_state` `PhaseDepsSet` + `TaskAdded`
    /// batch). CRDT `restore` is idempotent / last-writer-wins per field.
    pub async fn run_activated(
        &mut self,
        snapshot: crate::cluster_state::ClusterStateSnapshot<I>,
    ) -> Result<(), RunError> {
        let result = self.run_activated_pipeline(snapshot).await;
        self.cleanup_lifecycle_dispatcher().await;
        self.cleanup_task_completed_dispatcher().await;
        result
    }

    /// Body of [`Self::run_activated`], factored out so the wrapper can
    /// drive dispatcher cleanup regardless of how this returns (mirrors
    /// the `run` / `run_pipeline` split).
    async fn run_activated_pipeline(
        &mut self,
        snapshot: crate::cluster_state::ClusterStateSnapshot<I>,
    ) -> Result<(), RunError> {
        // Per-run resets — identical to `run_pipeline`'s, so an
        // on-demand-built primary starts every counter from zero.
        self.stranded_count = 0;
        self.setup_deadline_outcome = None;
        self.worker_mgmt_fail_outcome = None;

        // Dispatchers must be live BEFORE activation so the first
        // `PeerJoined` / `TaskCompleted` mutation the replicated ledger
        // applies post-hydration is observed. See `spawn_run_dispatchers`.
        self.spawn_run_dispatchers();

        tracing::info!(
            node = %self.config.node_id,
            "co-located primary activated on demand; restoring the replicated \
             ledger snapshot and activating (seeded resume, bypassing \
             connect/mesh-ready)"
        );

        // Restore the cluster-state snapshot the secondary captured at
        // the activation moment. The freshly-built primary's own
        // cluster_state was empty, so this restore is what seeds the
        // ledger that `hydrate_from_cluster_state` (inside
        // `activate_local_primary`) rebuilds the pool + in-flight ledger
        // from. CRDT `restore` is idempotent / last-writer-wins per field.
        self.cluster_state.restore(snapshot);

        // Seeded resume: hydrate the pool + unified in-flight ledger
        // from the now-restored CRDT and set `total_tasks`. The connect /
        // mesh-ready handshake is bypassed — an on-demand-built primary
        // inherited a formed mesh. See `activate_local_primary`.
        self.activate_local_primary()
            .await
            .map_err(RunError::Other)?;

        // Converge on the shared operational-loop-and-finalize tail.
        // `total` comes from `self.total_tasks`, which
        // `hydrate_from_cluster_state` set from the restored CRDT.
        let total = self.total_tasks;
        self.run_operational_and_finalize(total).await
    }

    /// Fire `on_phase_start` for every phase the pool currently
    /// reports as `Active` that we haven't notified yet. Idempotent:
    /// re-running visits only newly-active phases. Called once at
    /// run start (for zero-deps phases) and again from
    /// `process_phase_lifecycle` after `mark_phase_done` cascades.
    pub(super) fn fire_initial_phase_starts(&mut self) {
        let active: Vec<PhaseId> = self.pool().active_phases();
        for p in active {
            if self.phase_started_emitted.insert(p.clone()) {
                // Starting-job-phase / phase-transition (phase start)
                // important event. This `insert` guard is the single
                // once-per-phase edge for both the initial-active phases
                // and the runtime activations cascaded by
                // `mark_phase_done`, so it is the canonical phase-start
                // occurrence point. Emitted at the importance target;
                // task spawning the consumer drives off `on_phase_start`
                // below rides the same transition.
                tracing::info!(
                    target: super::important_events::IMPORTANT_TARGET,
                    phase = %p,
                    "starting job phase",
                );
                // Tell worker management a phase started and how many
                // workers it minimally needs to make progress. This is a
                // pure EMIT onto the decoupled worker-management bus — the
                // phase layer states demand and knows nothing of how (or
                // whether) worker management scales up; the consuming arm
                // counts alive workers and drives respawn / RunShouldFail.
                // An empty phase (one that will cascade-drain with no
                // items) makes no worker demand, so we skip the emit.
                let min = self.phase_min_workers(&p);
                if min > 0 {
                    self.cluster_state.emit_worker_mgmt(
                        WorkerMgmtSignal::PhaseStartedNeedsWorkers {
                            phase: p.clone(),
                            min,
                        },
                    );
                }
                if let Some(cb) = self.on_phase_start.as_mut() {
                    cb(&p);
                }
            }
        }
    }

    /// Minimum worker count a phase needs to make progress: `1` if the
    /// phase has any pending or in-flight work, else `0`. A pure query on
    /// the pool — the floor is "at least one worker to dispatch the
    /// phase's work"; additional workers are throughput, not correctness,
    /// and that scale-up policy is worker management's concern. Used by
    /// [`Self::fire_initial_phase_starts`] to populate
    /// [`WorkerMgmtSignal::PhaseStartedNeedsWorkers`].
    fn phase_min_workers(&self, phase: &PhaseId) -> usize {
        // Consult the optional pool directly: before the pool is built
        // (pre-run) no phase owns work, so the floor is 0. Once built, a
        // phase needs a worker iff it has pending or in-flight items.
        let Some(pool) = self.pending.as_ref() else {
            return 0;
        };
        let pending = pool.iter().any(|t| &t.phase_id == phase);
        let in_flight = pool.in_flight(phase) > 0;
        usize::from(pending || in_flight)
    }

    /// Per-phase proceed-or-fail policy, evaluated once a phase has
    /// drained AND its retry buckets are exhausted, immediately before
    /// `mark_phase_done`. A pure, synchronous predicate on the phase's
    /// terminal counters — no I/O, no mutation, no worker-management call
    /// (the caller routes a FAIL through the decoupled signal bus).
    ///
    /// Default policy:
    /// - PROCEED when `completed > 0` — the phase produced output its
    ///   dependents can consume.
    /// - PROCEED when the phase produced zero items (`completed == 0 &&
    ///   failed == 0`) — an empty / cascade-through phase makes no demand
    ///   and blocks nothing.
    /// - PROCEED when the phase's items reached a terminal FAILED outcome
    ///   (`failed > 0`). This is load-bearing and is NOT a "fail the run"
    ///   case: by the time control reaches this point the per-phase retry
    ///   buckets (Recoverable then OOM) have already run and exhausted
    ///   every reinjection path, so any surviving failure is PERMANENT and
    ///   already recorded. The canonical contract for an exhausted bucket
    ///   is "the phase advances; the fail_* count in the run's outcome
    ///   summary surfaces these to the operator" (see
    ///   [`crate::primary::retry_bucket`] — the budget-exhausted branch).
    ///   Aborting the whole run on a permanently-failed task would defeat
    ///   the retry-bucket machinery and regress
    ///   `sequential_phase_advance_after_oom_bucket_exhausts`.
    ///
    /// The FAIL branch is therefore reserved for a phase that reached the
    /// drain having owned items yet recorded NO terminal outcome of any
    /// kind for them — a genuine wedge where advancing would silently run
    /// dependents on absent, never-resolved inputs. `phase_min_workers`
    /// observing residual work for an otherwise-drained phase is exactly
    /// that signal. The phase-layer veto here is the structural backstop;
    /// the live no-progress decision (a phase that started, needs workers,
    /// and has none) is the worker arm's, reached via
    /// `PhaseStartedNeedsWorkers`.
    ///
    /// `phase` is consulted only to confirm no residual work survives for
    /// a phase that produced no terminal accounting; the policy is
    /// otherwise phase-agnostic.
    pub(super) fn phase_can_proceed(&self, phase: &PhaseId, completed: u32, failed: u32) -> bool {
        // Any terminal accounting (success OR recorded permanent failure)
        // means the phase resolved its work — advance per the canonical
        // retry-bucket-exhaustion contract.
        if completed > 0 || failed > 0 {
            return true;
        }
        // No terminal accounting: proceed only if the phase genuinely had
        // no work. If residual items remain for a phase the drain logic
        // surfaced, advancing would strand dependents — veto.
        self.phase_min_workers(phase) == 0
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
    /// The pre-loop waits `wait_for_connections` and `wait_for_mesh_ready`
    /// pass the LIVE `command_rx` (the `take`n receiver, `Some`): the
    /// PyPrimaryHandle IS reachable before operational-loop entry (it
    /// shares the pre-`run` `command_sender()` clone), so an
    /// `on_phase_end` fired by a TaskComplete arriving during either wait
    /// can queue `SpawnTasks` and have it drain inline via the same
    /// `dispatch_message` → cascade path. The post-loop drain
    /// (`drain_pending_messages`) passes `&mut None` — by then the
    /// operational loop has already exited and won't re-enter, so there is
    /// no in-runtime callback path left to drain.
    pub(super) async fn process_phase_lifecycle(
        &mut self,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
    ) {
        // Pre-discovery transient state in setup-defer mode. While
        // `setup_pending()` is true the local primary has `total_tasks
        // = 0` and every declared phase is `Active` with zero items —
        // not because they're truly empty, but because the
        // setup-promoted secondary has not yet broadcast its first
        // `TaskAdded` / `TasksSpawned`. Firing `on_phase_end(.., 0, 0)`
        // now would surface a spurious "empty drain" for every phase
        // before the chosen secondary has had a chance to populate them
        // (a consumer callback walking just-discovered outputs would
        // OSError on missing paths). The gate clears the moment the
        // first task lands in the replicated ledger; subsequent cascade
        // calls resume normal operation. See `Self::setup_pending`.
        //
        // Idempotent on the legacy bootstrap path: `setup_pending()`
        // is always false there, so the gate is always satisfied.
        if self.setup_pending() {
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
                    Box::pin(crate::primary::command_channel::handle_primary_command(
                        self, cmd, command_rx,
                    ))
                    .await;
                }
                // Per-phase proceed-or-fail decision, evaluated AFTER the
                // retry-bucket cascade has exhausted every reinjection
                // path (both buckets above returned `false`) and BEFORE
                // the phase is marked done. On PROCEED the phase advances
                // exactly as before (mark_phase_done flips dependents
                // Blocked → Active) — this is the path taken by every
                // phase that produced a completion OR whose failures are
                // permanent-and-recorded (the canonical retry-bucket-
                // exhaustion contract: advance, surface fail_* in the
                // outcome summary). On FAIL — the genuine wedge where a
                // phase reached this drain with NO terminal accounting yet
                // still owns residual work — we EMIT RunShouldFail onto
                // the decoupled worker-management bus (which owns the
                // clean-shutdown drive) and leave the phase un-done. The
                // emit is a pure signal; the phase layer never drives
                // shutdown directly (decoupling law). See
                // `phase_can_proceed` for the exact policy.
                if self.phase_can_proceed(p, completed, failed) {
                    self.pool_mut().mark_phase_done(p);
                } else {
                    self.cluster_state
                        .emit_worker_mgmt(WorkerMgmtSignal::RunShouldFail {
                            reason: format!(
                                "phase {p} reached drain with no terminal \
                                 outcome ({completed} completed, {failed} \
                                 failed) yet still owns residual work"
                            ),
                        });
                }
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
