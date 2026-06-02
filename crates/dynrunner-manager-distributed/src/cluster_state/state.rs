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

use super::types::{PeerEntry, RoleChangeHook, TaskState};

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
    /// Per-id liveness ledger maintained by the `PeerJoined` and
    /// `PeerRemoved` apply rules. The `RoleTable.observers` set is a
    /// projection of this map (the subset whose entries are
    /// `Alive { is_observer: true }`); the map itself is the
    /// authoritative "have we ever seen this id, and is it currently
    /// alive or dead-forever" answer.
    ///
    /// Skipped from `Clone`, snapshot, and restore — same rationale
    /// as `role_change_hooks`: the map is paired with the node-local
    /// `lifecycle_tx` dispatcher channel and a cloned replica has
    /// neither the channel nor any reason to inherit the source's
    /// runtime peer view. Receivers rebuild the map by re-applying
    /// broadcast `PeerJoined`/`PeerRemoved` mutations after restore.
    pub(super) peer_state: HashMap<String, PeerEntry>,
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
    /// Keyed by `task_id` (not the CRDT-internal task hash) — dependents
    /// reference predecessors by `task_id` (see `TaskDep.task_id`), and
    /// `ClusterState` does not maintain a `task_id → hash` reverse
    /// index. Keying the cache by `task_id` lets the dispatch-side
    /// resolver look up a dependent's predecessor outputs in O(1)
    /// without a linear scan, and matches the shape downstream consumers
    /// see (Python-side task bindings dereference by user-chosen
    /// `task_id` strings, never by internal hashes).
    ///
    /// Replicated CRDT data — clone preserves it (matches `tasks`,
    /// `peer_holdings`, and `phase_deps` semantics). Included in
    /// `snapshot` / `restore` so a late-joiner sees every committed
    /// output set before the next live `TaskCompleted` broadcast
    /// reaches it. Populated by the `TaskCompleted` apply arm (see
    /// the `record_task_outputs` helper in `apply_tasks.rs`).
    ///
    /// Anonymous tasks (`TaskInfo.task_id == None`) cannot be
    /// referenced as dependencies and are skipped — they have no key
    /// to insert under. Malformed `result_data` (failed JSON decode)
    /// logs a `tracing::warn!` and stores an empty `TaskOutputs` so
    /// dependents that hard-require a key see a controlled-empty
    /// view rather than racing the cache between "populated" and
    /// "absent".
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
            role_table: self.role_table.clone(),
            // Deliberately not cloned — see field doc.
            role_change_hooks: Vec::new(),
            // Deliberately not cloned — see field doc.
            peer_state: HashMap::new(),
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
            .field("role_table", &self.role_table)
            .field("role_change_hooks", &self.role_change_hooks.len())
            .field("peer_state", &self.peer_state)
            .field("lifecycle_tx", &self.lifecycle_tx.is_some())
            .field("matcher_trigger_tx", &self.matcher_trigger_tx.is_some())
            .field("worker_mgmt_tx", &self.worker_mgmt_tx.is_some())
            .field("task_completed_tx", &self.task_completed_tx.is_some())
            .field("peer_holdings", &self.peer_holdings)
            .field("task_outputs", &self.task_outputs.len())
            .field("secondary_capacities", &self.secondary_capacities)
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
            role_table: RoleTable::default(),
            role_change_hooks: Vec::new(),
            peer_state: HashMap::new(),
            lifecycle_tx: None,
            matcher_trigger_tx: None,
            worker_mgmt_tx: None,
            task_completed_tx: None,
            peer_holdings: HashMap::new(),
            task_outputs: HashMap::new(),
            secondary_capacities: HashMap::new(),
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
