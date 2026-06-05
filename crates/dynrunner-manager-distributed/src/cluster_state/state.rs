//! The `ClusterState<I>` struct, its trait impls, and minimal
//! constructors.
//!
//! Single concern: storage shape + identity. Field semantics
//! (clone-skip, snapshot-skip, dispatcher-channel forward contracts)
//! are documented inline on each field; the behavior that reads or
//! mutates the fields lives in sibling sub-modules (`accessors`,
//! `apply`, `apply_peer`, `apply_tasks`, `events`, `snapshot`,
//! `broadcast`).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use dynrunner_core::{Identifier, PhaseId, TaskOutputs};
use dynrunner_protocol_primary_secondary::{RoleTable, SecondaryCapacityRecord};

use crate::fulfillability_matcher::MatcherTriggerEvent;
use crate::peer_lifecycle::PeerLifecycleEvent;
use crate::task_completed::TaskCompletedEvent;
use crate::worker_signal::WorkerMgmtSignal;

use super::types::{CapabilityEntry, PeerEntry, RoleChangeHook, TaskState};

/// The replicated cluster-state CRDT.
pub struct ClusterState<I> {
    pub(super) tasks: HashMap<String, TaskState<I>>,
    pub(super) current_primary: Option<String>,
    pub(super) primary_epoch: u64,
    /// Lock-free mirror of `primary_epoch` exposed to off-`apply`
    /// readers (e.g. the observer's resource-holdings announcer task
    /// — see [`crate::observer::announcer::run_observer_announcer`]).
    /// Written synchronously by the `apply` path (and `restore`)
    /// **before** `fire_role_change_hooks` runs, so any hook
    /// observer that reads the mirror in response to a role-change
    /// notification sees the post-mutation value.
    ///
    /// Cloned (cheap — `Arc::clone`) on `Clone` rather than reset:
    /// unlike `role_change_hooks` / `peer_state`, the mirror has no
    /// runtime-handle semantics (it's an atomic counter, not a
    /// channel sender), and snapshot-restore paths overwrite the
    /// value to match the restored `primary_epoch` anyway, so
    /// preserving the Arc is consistent with the field's read-only
    /// downstream consumer contract.
    pub(super) primary_epoch_mirror: Arc<std::sync::atomic::AtomicU64>,
    /// Per-run static phase dependency graph. Set once at run start
    /// via `ClusterMutation::PhaseDepsSet` (originated by the primary,
    /// applied on every node) and never overwritten — the deps are
    /// derived from the consumer's `TaskDefinition` declaration and
    /// don't change for the duration of a run.
    pub(super) phase_deps: HashMap<PhaseId, Vec<PhaseId>>,
    /// Set by `ClusterMutation::RunComplete`. Sticky monotonic flag —
    /// once true, the run is over and every node should drain and
    /// exit. Read by the secondary's operational loop to break out
    /// even when peers haven't disconnected.
    pub(super) run_complete: bool,
    /// Set by `ClusterMutation::RunAborted { reason }`. The failure
    /// twin of `run_complete`: sticky monotonic — once `Some`, the run
    /// has been aborted cluster-wide and every node should exit
    /// non-zero. `None` until the first abort lands. The secondary's
    /// `process_tasks` loop checks this BEFORE the `run_complete` break
    /// and returns `RunOutcome::Terminal` (projecting to
    /// `SecondaryTerminal::Aborted`); the `mesh_watchdog` disarms
    /// on it too (failover has nothing left to guard once the run is
    /// aborting). Carries the abort reason for the PyO3-boundary log.
    pub(super) run_aborted: Option<String>,
    /// Replicated role bookkeeping. Updated in lockstep with
    /// `current_primary` on every `PrimaryChanged` apply so the
    /// transport-layer cache (registered via `role_change_hooks`)
    /// always observes a coherent snapshot.
    pub(super) role_table: RoleTable,
    /// Hooks fired AFTER a `RoleTable` mutation. The cluster_state
    /// owns the hooks; transports register their write-through
    /// cache here at construction time. Stored as `Vec` for future-
    /// proofing — a single registrant covers today's `PeerTransport`
    /// cache use case.
    ///
    /// Skipped from `Clone` (and reset on snapshot/restore paths): a
    /// cloned `ClusterState` is conceptually a separate replica and
    /// has no transport attached, so carrying the source replica's
    /// hooks would fire a remote transport's cache from a state it
    /// does not own. Tests that need hooks on a cloned state must
    /// re-register on the clone.
    pub(super) role_change_hooks: Vec<RoleChangeHook>,
    /// Per-id LIVENESS ledger (only) maintained by the `PeerJoined` and
    /// `PeerRemoved` apply rules. Holds the alive/dead bit; the role
    /// capabilities (`is_observer` / `can_be_primary`) live in the
    /// separate replicated `capabilities` 2P-set (C6 — one source of
    /// truth, no CRD-4). The `RoleTable.observers` / `RoleTable.can_be_primary`
    /// sets are READ-TIME projections of `capabilities × this map's Alive
    /// bit` (`reproject_roles`); this map is the authoritative "have we
    /// ever seen this id, and is it currently alive or dead-forever"
    /// liveness answer, composed with — never merged into — the capability
    /// set at projection time.
    ///
    /// Skipped from `Clone` (a cloned replica is a fresh node-local view
    /// paired with the node-local `lifecycle_tx` dispatcher channel, and
    /// has no reason to inherit the source's runtime peer view).
    ///
    /// Snapshot/restore: the ALIVE subset DOES cross the wire — `snapshot`
    /// projects the `Alive` ids out of this map into
    /// `ClusterStateSnapshot::alive_members` (only the `HashSet<String>`,
    /// not the module-private `PeerEntry`), and `restore` reconstructs a
    /// fresh `Alive` `PeerEntry` for each incoming id into a VACANT slot
    /// (Dead-wins / sticky: a local `Dead` is never resurrected; absence
    /// is not read as Dead — honest-liveness). Dead ids are NOT
    /// snapshotted. The steady-state writers remain the live
    /// `PeerJoined`/`PeerRemoved` broadcasts. Liveness is INTENTIONALLY
    /// excluded from the digest (each node owns its own view); only
    /// capability converges via anti-entropy.
    pub(super) peer_state: HashMap<String, PeerEntry>,
    /// Replicated role-capability 2P-set (C6) — the SINGLE source of
    /// truth for `is_observer` / `can_be_primary`, decoupled from
    /// liveness. Keyed by peer id; each entry is `Advertised { .. } |
    /// Departed` (a tombstone written on a genuine `PeerRemoved`). Merged
    /// monotonically via `merge_capability` (apply's peer arms +
    /// restore's per-id loop) and folded into the digest
    /// (`capabilities_hash`) — the 2P-set IS snapshot-healable, so a
    /// flagged divergence is one a pull's `restore` actually heals
    /// (detect-WITH-heal). The `RoleTable.observers` /
    /// `RoleTable.can_be_primary` sets are read-time projections of this
    /// map AND the local `peer_state` Alive bit (`reproject_roles`); this
    /// map alone is replicated, the projections are node-local derived.
    ///
    /// Replicated CRDT data — clone preserves it (matches `tasks`,
    /// `peer_holdings`, `task_outputs`). Round-trips through
    /// `snapshot`/`restore`.
    pub(super) capabilities: HashMap<String, CapabilityEntry>,
    /// Sender end of the peer-lifecycle dispatcher mpsc. Installed
    /// via [`Self::install_lifecycle_sender`] when the coordinator
    /// wires its dispatcher task; `None` while no coordinator has
    /// attached (tests that exercise the apply path in isolation
    /// observe the same `None` state and the emit becomes a silent
    /// drop). The receiver end is owned by
    /// [`crate::peer_lifecycle::run_peer_lifecycle_dispatcher`].
    ///
    /// Skipped from `Clone`, snapshot, and restore — same rationale
    /// as `role_change_hooks` and `peer_state`: a cloned replica is
    /// a fresh node-local view and inheriting the source's sender
    /// would route this replica's events into the source's
    /// dispatcher, violating the CCD-9 "apply path never crosses
    /// node boundaries" invariant.
    pub(super) lifecycle_tx: Option<tokio::sync::mpsc::UnboundedSender<PeerLifecycleEvent>>,
    /// Sender end of the fulfillability-matcher trigger mpsc. Installed
    /// via [`Self::install_matcher_trigger_sender`] when the
    /// coordinator wires its matcher pipeline; `None` while no
    /// coordinator has attached. Receiver is consumed by
    /// [`crate::fulfillability_matcher::drain_matcher_batch`] from
    /// inside the operational `select!` loop. Skipped from Clone /
    /// snapshot / restore for the same reason as `lifecycle_tx`.
    pub(super) matcher_trigger_tx: Option<tokio::sync::mpsc::UnboundedSender<MatcherTriggerEvent>>,
    /// Sender end of the worker-management signal bus mpsc. Installed
    /// via [`Self::install_worker_mgmt_sender`] when worker management
    /// wires its operational loop; `None` while nothing has attached.
    /// Receiver is consumed by
    /// [`crate::worker_signal::drain_worker_signal_batch`] from inside
    /// worker management's operational `select!` loop. Skipped from
    /// Clone / snapshot / restore for the same reason as `lifecycle_tx`.
    pub(super) worker_mgmt_tx: Option<tokio::sync::mpsc::UnboundedSender<WorkerMgmtSignal>>,
    /// Sender end of the task-completion dispatcher mpsc. Installed
    /// via [`Self::install_task_completed_sender`] when the
    /// coordinator wires its dispatcher task; `None` while no
    /// coordinator has attached (the apply path in isolation observes
    /// the same `None` state and the emit becomes a silent drop).
    /// Receiver is owned by
    /// [`crate::task_completed::run_task_completed_dispatcher`].
    ///
    /// Skipped from `Clone`, snapshot, and restore — same rationale as
    /// `lifecycle_tx` / `matcher_trigger_tx`: a cloned replica is a
    /// fresh node-local view and inheriting the source's sender would
    /// route this replica's events into the source's dispatcher,
    /// violating the CCD-9 "apply path never crosses node boundaries"
    /// invariant.
    pub(super) task_completed_tx: Option<tokio::sync::mpsc::UnboundedSender<TaskCompletedEvent>>,
    /// Per-peer set of opaque resource strings each peer announces
    /// it currently holds locally. Maintained by the
    /// `PeerResourceHoldingsUpdated` apply rule and round-tripped via
    /// `ClusterStateSnapshot::peer_holdings` so a late-joiner sees
    /// current holdings before the next per-peer announce arrives.
    /// Opaque to the CRDT: the framework does not interpret the
    /// strings; the fulfillability-matcher hook attaches meaning.
    pub(super) peer_holdings: HashMap<String, HashSet<String>>,
    /// Replicated keyed-output cache. One entry per task that has
    /// reached `Completed` and committed a non-empty `TaskOutputs`
    /// via its `TaskCompleted` mutation's `result_data` payload.
    ///
    /// Keyed by the wire-canonical CONTENT HASH (the same key as the
    /// `tasks` ledger), NOT `task_id`. The content hash folds in
    /// `phase_id`, so the same `task_id` in two different phases keys to
    /// two distinct cache entries (no cross-phase output collision). The
    /// dispatch-time predecessor assembler resolves a dep's full
    /// `(phase_id, task_id)` identity to its hash, then reads this cache
    /// by that hash (`co_present_outputs_for` / `record_task_outputs_value`
    /// both key by hash).
    ///
    /// Replicated CRDT data — clone preserves it (matches `tasks`,
    /// `peer_holdings`, and `phase_deps` semantics). Included in
    /// `snapshot` / `restore` so a late-joiner sees every committed
    /// output set before the next live `TaskCompleted` broadcast
    /// reaches it. Populated by the `TaskCompleted` apply arm (see
    /// the `record_task_outputs` helper in `apply_tasks.rs`).
    ///
    /// A hash with no `tasks` ledger entry (a late-arriving mutation for
    /// a task this replica never saw) is skipped — there is no anchor to
    /// key the cache against. Malformed `result_data` (failed JSON decode)
    /// logs a `tracing::warn!` and stores an empty `TaskOutputs` so
    /// dependents that hard-require a key see a controlled-empty view
    /// rather than racing the cache between "populated" and "absent".
    pub(super) task_outputs: HashMap<String, TaskOutputs>,
    /// Per-secondary static capacity (worker-slot count + advertised
    /// resource amounts). Set once per secondary by the
    /// `SecondaryCapacity` apply rule (originated by the primary at the
    /// `SecondaryWelcome` accept in `primary/connect.rs`) and never
    /// overwritten — capacity is static for a secondary's lifetime in
    /// the run.
    ///
    /// Replicated CRDT data — clone preserves it (matches `tasks`,
    /// `peer_holdings`, and `task_outputs` semantics). Included in
    /// `snapshot` / `restore` so a freshly-promoted primary and late-
    /// joining observers hold the FULL per-secondary roster the moment
    /// they restore a snapshot, before any live `SecondaryCapacity`
    /// broadcast reaches them. This is the failover-correctness fix for
    /// the worker roster being 100% primary-local: a promoted primary
    /// reconstructs `alive_worker_count()` / `self.workers` from this
    /// replicated source rather than starting empty.
    pub(super) secondary_capacities: HashMap<String, SecondaryCapacityRecord>,
    /// Node-local per-task monotone "next seq" counter — the originator's
    /// half of the `TaskVersion` stamp. The originating primary (or a
    /// promoted secondary holding a `ClusterState`) bumps this at the
    /// version-stamp choke point (`broadcast::stamp_versions`) to mint a
    /// strictly-increasing `(primary_epoch, seq)` per hash.
    ///
    /// NOT replicated: it is the local originator's counter, not part of
    /// the converged ledger — skipped from `Clone`, snapshot, restore, and
    /// the digest (classified node-local in the exhaustive-destructure
    /// guards, like the dispatcher senders). A replica that never
    /// originates a mutation for a hash never reads it; a freshly-promoted
    /// primary cold-starts the counter, and `next_task_version` mints
    /// against the (already-advanced) `primary_epoch`, so a post-promotion
    /// stamp still strictly exceeds every pre-promotion version.
    pub(super) task_seq: HashMap<String, u32>,
}

impl<I> Clone for ClusterState<I>
where
    I: Clone,
{
    fn clone(&self) -> Self {
        Self {
            tasks: self.tasks.clone(),
            current_primary: self.current_primary.clone(),
            primary_epoch: self.primary_epoch,
            // Arc-clone is the right semantics here — see field doc.
            primary_epoch_mirror: Arc::clone(&self.primary_epoch_mirror),
            phase_deps: self.phase_deps.clone(),
            run_complete: self.run_complete,
            run_aborted: self.run_aborted.clone(),
            role_table: self.role_table.clone(),
            // Deliberately not cloned — see field doc.
            role_change_hooks: Vec::new(),
            // Deliberately not cloned — see field doc.
            peer_state: HashMap::new(),
            // Replicated CRDT data — clone preserves it.
            capabilities: self.capabilities.clone(),
            // Deliberately not cloned — see field doc.
            lifecycle_tx: None,
            // Deliberately not cloned — same rationale as `lifecycle_tx`.
            matcher_trigger_tx: None,
            // Deliberately not cloned — same rationale as `lifecycle_tx`.
            worker_mgmt_tx: None,
            // Deliberately not cloned — same rationale as `lifecycle_tx`.
            task_completed_tx: None,
            // Replicated CRDT data — clone preserves it.
            peer_holdings: self.peer_holdings.clone(),
            // Replicated CRDT data — clone preserves it.
            task_outputs: self.task_outputs.clone(),
            // Replicated CRDT data — clone preserves it.
            secondary_capacities: self.secondary_capacities.clone(),
            // Node-local originator counter — reset on clone (a cloned
            // replica originates nothing inherited from the source).
            task_seq: HashMap::new(),
        }
    }
}

impl<I> std::fmt::Debug for ClusterState<I>
where
    I: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterState")
            .field("tasks", &self.tasks)
            .field("current_primary", &self.current_primary)
            .field("primary_epoch", &self.primary_epoch)
            .field("phase_deps", &self.phase_deps)
            .field("run_complete", &self.run_complete)
            .field("run_aborted", &self.run_aborted)
            .field("role_table", &self.role_table)
            .field("role_change_hooks", &self.role_change_hooks.len())
            .field("peer_state", &self.peer_state)
            .field("capabilities", &self.capabilities)
            .field("lifecycle_tx", &self.lifecycle_tx.is_some())
            .field("matcher_trigger_tx", &self.matcher_trigger_tx.is_some())
            .field("worker_mgmt_tx", &self.worker_mgmt_tx.is_some())
            .field("task_completed_tx", &self.task_completed_tx.is_some())
            .field("peer_holdings", &self.peer_holdings)
            .field("task_outputs", &self.task_outputs.len())
            .field("secondary_capacities", &self.secondary_capacities)
            .field("task_seq", &self.task_seq.len())
            .finish()
    }
}

impl<I> Default for ClusterState<I> {
    fn default() -> Self {
        Self {
            tasks: HashMap::new(),
            current_primary: None,
            primary_epoch: 0,
            primary_epoch_mirror: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            phase_deps: HashMap::new(),
            run_complete: false,
            run_aborted: None,
            role_table: RoleTable::default(),
            role_change_hooks: Vec::new(),
            peer_state: HashMap::new(),
            capabilities: HashMap::new(),
            lifecycle_tx: None,
            matcher_trigger_tx: None,
            worker_mgmt_tx: None,
            task_completed_tx: None,
            peer_holdings: HashMap::new(),
            task_outputs: HashMap::new(),
            secondary_capacities: HashMap::new(),
            task_seq: HashMap::new(),
        }
    }
}

impl<I: Identifier> ClusterState<I> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn task_count(&self) -> usize {
        self.tasks.len()
    }
}
