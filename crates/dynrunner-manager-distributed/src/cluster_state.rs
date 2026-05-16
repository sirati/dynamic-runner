//! Replicated cluster ledger.
//!
//! Single concern: every node holds a continuously-coherent view of the
//! cluster's task ledger and the current primary identity, maintained by
//! applying CRDT-style mutations broadcast across the mesh.
//!
//! The dispatcher (primary) is the only node that *originates* `TaskAdded`
//! and `TaskAssigned` mutations; every node — primary included — applies
//! every mutation that flows through. `TaskCompleted` / `TaskFailed` are
//! originated by whichever node observes the worker outcome (typically
//! the secondary that owns the worker), and `PrimaryChanged` by the
//! election protocol.
//!
//! Idempotency-by-precondition: each mutation describes the state it
//! applies against, and re-application against the post-state is a
//! `NoOp`. This makes out-of-order delivery and at-least-once delivery
//! safe: terminal states (`Completed` / `Failed`) lock out non-terminal
//! transitions, so a `TaskCompleted` that lands before the matching
//! `TaskAssigned` correctly leaves the entry terminal even when the
//! late `TaskAssigned` arrives next.
//!
//! Asymmetry between the two terminal states: `Completed` is the
//! strongest terminal (success). A `TaskCompleted` superseding a prior
//! `Failed { Recoverable }` is the retry-pass mechanism's normal
//! shape — the same binary is re-injected, re-dispatched, and runs
//! to success. The CRDT must propagate that supersession or the
//! `outcome_counts()` partition stays stuck reporting the retry-
//! succeeded task as `fail_retry`. `Completed` never regresses: a
//! `TaskFailed` against a `Completed` entry is a NoOp (the late
//! failure from a redundant dispatch path can't undo a recorded
//! success). Commutativity is preserved — see `apply`'s TaskCompleted
//! arm doc.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use dynrunner_core::{ErrorType, Identifier, PhaseId, SoftPreferredSecondaries, TaskInfo, WorkerId};
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, RemovalCause, RoleChangeHookRegistrar, RoleTable,
};
use serde::{Deserialize, Serialize};

use crate::fulfillability_matcher::MatcherTriggerEvent;
use crate::peer_lifecycle::PeerLifecycleEvent;

/// Per-task state in the replicated ledger.
///
/// Every variant carries the `TaskInfo`. Pre-Phase-B terminal variants
/// dropped it on the assumption that nothing downstream needed it; that
/// assumption broke for the post-promotion hydration path, which needs
/// the `task_id` of completed tasks to seed `PendingPool::mark_tasks_completed`
/// so surviving variants' `task_depends_on` references resolve correctly.
/// Keeping `TaskInfo` everywhere costs O(N) extra clones but removes the
/// need for a parallel completed-task-id ledger; the alternative would
/// reintroduce duplicated state across `cluster_state` and a side cache.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound(
    serialize = "I: Serialize",
    deserialize = "I: for<'a> Deserialize<'a>",
))]
pub enum TaskState<I> {
    Pending {
        task: TaskInfo<I>,
    },
    InFlight {
        task: TaskInfo<I>,
        secondary: String,
        worker: WorkerId,
    },
    Completed {
        task: TaskInfo<I>,
    },
    Failed {
        task: TaskInfo<I>,
        kind: ErrorType,
        last_error: String,
        attempts: u32,
    },
    /// The task hit `ErrorType::Unfulfillable` — a required cluster
    /// resource (e.g. a toolchain outpath) is not held by any peer.
    /// Discrete variant (rather than `Failed { kind: Unfulfillable, .. }`)
    /// so downstream matcher / state-filter logic (the reinject command,
    /// consumer-side state introspection) can dispatch on the
    /// discriminant rather than parsing an inner `ErrorType`. `reason`
    /// mirrors the `BoundedString<2048>` body from the wire mutation
    /// (stored here as `String`; the cap is the wire-codec's concern,
    /// not the in-memory ledger's).
    ///
    /// Reinjectable: `PrimaryCommand::ReinjectTask` accepts this state
    /// and transitions it back to `Pending` via the `TaskReinjected`
    /// apply rule; until then it behaves as a stable terminal for
    /// counter / partition purposes (folded into `fail_final` by
    /// `outcome_counts` for operator-readable buckets).
    Unfulfillable {
        task: TaskInfo<I>,
        reason: String,
    },
    /// The task is a transitive dependent of a task currently in
    /// `Unfulfillable`. Dormant until the prerequisite (identified by
    /// `on`, the prereq's task hash) leaves Unfulfillable via the
    /// reinject + complete path; the `TaskCompleted` apply rule
    /// auto-resumes every `Blocked { on, .. }` entry whose `on` matches
    /// the completing hash back to `Pending`.
    ///
    /// Discrete variant (rather than `Failed { .. }`) for the same
    /// reason as `Unfulfillable`: dependents of an unfulfillable task
    /// are NOT terminal-failed — they're cascade-paused, and the
    /// auto-resume mechanism needs to identify them by discriminant +
    /// `on` field rather than by parsing an error message.
    Blocked {
        task: TaskInfo<I>,
        on: String,
    },
}

/// Outcome of `ClusterState::apply`. `NoOp` is the normal silent-merge
/// case (duplicate, late delivery, terminal-locked); not an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyOutcome {
    Applied,
    NoOp,
}

/// Counts of tasks per top-level state. For tests / metrics.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct StateCounts {
    pub pending: usize,
    pub in_flight: usize,
    pub completed: usize,
    pub failed: usize,
    /// Tasks in `TaskState::Unfulfillable { .. }` — reinjectable
    /// resource-availability failures awaiting external reinjection.
    pub unfulfillable: usize,
    /// Tasks in `TaskState::Blocked { .. }` — cascade-paused dependents
    /// of an unfulfillable prerequisite, dormant until the prereq
    /// completes via the reinject + re-run path.
    pub blocked: usize,
}

/// Per-class outcome breakdown the primary emits on every
/// counter-bearing log line. Replaces the older single-number
/// `completed=N failed=N` shape: `succeeded` is the unique-hash
/// completion count, and the three `fail_*` fields partition the
/// failed-task ledger by ErrorType.
///
/// Mapping from ErrorType:
///   - `Recoverable`                       → `fail_retry`
///   - `ResourceExhausted("memory")`       → `fail_oom`
///   - `ResourceExhausted(other)` |
///     `NonRecoverable`                    → `fail_final`
///
/// Semantics note: classification is by the task's last-observed
/// ErrorType, not by retry-eligibility. A Recoverable failure
/// that exhausts the retry budget stays in `fail_retry` at
/// terminal report — the operator reads the bucket name as
/// "what class of error did this task hit", not "is this
/// task going to retry". Retry-budget exhaustion is reported
/// via the existing "retry budget exhausted" log line; the
/// numeric breakdown is class-of-failure.
///
/// Lives on `ClusterState` because the CRDT-replicated `tasks` ledger
/// is the authoritative source: every node converges to the same
/// `(Completed, Failed { kind, .. })` view via mutation broadcast, so
/// reading the counts from `cluster_state` gives every observer
/// (live primary, demoted primary, late-joining observer) the same
/// answer. Pre-this-move the counter was assembled from per-node
/// `completed_tasks`/`failed_tasks` HashSets the coordinator
/// maintained alongside `cluster_state` — those sets are still kept
/// for per-task identity / dedup decisions, but the *count*-shaped
/// reads route here.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OutcomeSummary {
    pub succeeded: usize,
    pub fail_retry: usize,
    pub fail_oom: usize,
    pub fail_final: usize,
}

impl OutcomeSummary {
    /// Sum across all four buckets — the total tasks that reached
    /// a terminal state (success or failure). Distinct from
    /// `total_tasks` on the coordinator, which counts the input
    /// batch; `total_terminal()` reaches `total_tasks` exactly
    /// when the run is fully accounted for.
    pub fn total_terminal(&self) -> usize {
        self.succeeded + self.fail_retry + self.fail_oom + self.fail_final
    }
}

/// Callback fired (synchronously, inside `apply`) after the
/// [`RoleTable`] mutates. Stored on `ClusterState`; the
/// [`crate::transport`]-layer write-through cache is the single
/// expected registrant in production. Boxed `Fn` so multiple
/// independent observers can coexist if a future feature needs them
/// (Vec storage is future-proof at trivial cost — one hook is enough
/// today). The callback receives a borrow of the *post*-mutation
/// table; never the pre-mutation snapshot.
pub type RoleChangeHook = Arc<dyn Fn(&RoleTable) + Send + Sync + 'static>;

/// Liveness bit on a `PeerEntry`. `Dead` is sticky-per-id: once a peer
/// is `Dead`, no subsequent `PeerJoined`/`PeerRemoved` mutation for the
/// same id may mutate the entry (re-application is silent). Respawn
/// requires a fresh id.
///
/// Internal — the `peer_state` map is module-private and the apply
/// rules are the only writers, so the variant set need not be `pub`.
#[derive(Debug, Clone, PartialEq, Eq)]
enum PeerState {
    Alive,
    Dead,
}

/// One entry in `ClusterState::peer_state`. Holds the liveness bit plus
/// scaffolding metadata that future mutations (`PeerInfo`/`PeerCert`
/// broadcasts) will populate — today `pubkey`/`endpoint` start `None`
/// and stay `None` because no mutation writes them yet. `is_observer`
/// mirrors the `RoleTable.observers` projection so reads that need the
/// flag without going through the observer set have a single source.
///
/// Internal — exposed nowhere; the apply rules are the only writers
/// and `peer_state` is module-private.
#[derive(Debug, Clone)]
struct PeerEntry {
    state: PeerState,
    /// Populated by a future `PeerInfo`-shaped mutation; today no
    /// mutation writes this and reads never fire. Kept as a stable
    /// field so the future wiring lands as an in-place update rather
    /// than a struct shape change.
    #[allow(dead_code)]
    pubkey: Option<String>,
    /// See `pubkey` — same forward-looking scaffolding.
    #[allow(dead_code)]
    endpoint: Option<String>,
    is_observer: bool,
}

/// The replicated cluster-state CRDT.
pub struct ClusterState<I> {
    tasks: HashMap<String, TaskState<I>>,
    current_primary: Option<String>,
    primary_epoch: u64,
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
    primary_epoch_mirror: Arc<std::sync::atomic::AtomicU64>,
    /// Per-run static phase dependency graph. Set once at run start
    /// via `ClusterMutation::PhaseDepsSet` (originated by the primary,
    /// applied on every node) and never overwritten — the deps are
    /// derived from the consumer's `TaskDefinition` declaration and
    /// don't change for the duration of a run.
    phase_deps: HashMap<PhaseId, Vec<PhaseId>>,
    /// Set by `ClusterMutation::RunComplete`. Sticky monotonic flag —
    /// once true, the run is over and every node should drain and
    /// exit. Read by the secondary's operational loop to break out
    /// even when peers haven't disconnected.
    run_complete: bool,
    /// Replicated role bookkeeping. Updated in lockstep with
    /// `current_primary` on every `PrimaryChanged` apply so the
    /// transport-layer cache (registered via `role_change_hooks`)
    /// always observes a coherent snapshot.
    role_table: RoleTable,
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
    role_change_hooks: Vec<RoleChangeHook>,
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
    peer_state: HashMap<String, PeerEntry>,
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
    lifecycle_tx: Option<tokio::sync::mpsc::UnboundedSender<PeerLifecycleEvent>>,
    /// Sender end of the fulfillability-matcher trigger mpsc. Installed
    /// via [`Self::install_matcher_trigger_sender`] when the
    /// coordinator wires its matcher pipeline; `None` while no
    /// coordinator has attached. Receiver is consumed by
    /// [`crate::fulfillability_matcher::drain_matcher_batch`] from
    /// inside the operational `select!` loop. Skipped from Clone /
    /// snapshot / restore for the same reason as `lifecycle_tx`.
    matcher_trigger_tx: Option<tokio::sync::mpsc::UnboundedSender<MatcherTriggerEvent>>,
    /// Per-peer set of opaque resource strings each peer announces
    /// it currently holds locally. Maintained by the
    /// `PeerResourceHoldingsUpdated` apply rule and round-tripped via
    /// `ClusterStateSnapshot::peer_holdings` so a late-joiner sees
    /// current holdings before the next per-peer announce arrives.
    /// Opaque to the CRDT: the framework does not interpret the
    /// strings; the fulfillability-matcher hook attaches meaning.
    peer_holdings: HashMap<String, HashSet<String>>,
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
            // Replicated CRDT data — clone preserves it.
            peer_holdings: self.peer_holdings.clone(),
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
            .field("peer_holdings", &self.peer_holdings)
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
            peer_holdings: HashMap::new(),
        }
    }
}

/// Serializable snapshot of an entire `ClusterState`. Used by the
/// snapshot RPC (`RequestClusterSnapshot` → `ClusterSnapshot`) so a
/// late-joining or reconnecting node can bootstrap its replicated
/// ledger from any peer.
///
/// Merge semantics on the receiver side (see `ClusterState::restore`):
///
/// - Per task: terminal states (`Completed` / `Failed`) win over
///   non-terminal; among non-terminals, `InFlight` wins over `Pending`.
///   When local and incoming are both terminal, local wins (first-seen
///   terminal is canonical, mirroring the live `apply` rules).
/// - `(current_primary, primary_epoch)`: higher epoch wins.
/// - `phase_deps`: replaced if local is empty, otherwise kept (the
///   graph is static for the run's lifetime).
/// - `observers`: replaced if local is empty, otherwise kept. The
///   live mutation path (`ClusterMutation::PeerJoined { is_observer
///   = true }` broadcasts) inserts into the set with set semantics,
///   so a snapshot is authoritative for the late-joiner's first-
///   bootstrap and inert thereafter. A broader merge rule (union,
///   or epoch-tagged replace) would be over-engineering today —
///   subsequent `PeerJoined` broadcasts converge any divergence
///   between snapshot-restored and live-applied observers via the
///   apply rule's idempotent insert. `#[serde(default)]` keeps wire
///   compat with pre-Step-8 senders (snapshots from a peer running
///   an older crate omit the field; deserialize defaults to an
///   empty set, identical to the pre-Step-8 shape).
/// - `peer_holdings`: replaced if local is empty, otherwise kept —
///   same first-bootstrap-only contract as `observers`. The live
///   `PeerResourceHoldingsUpdated` apply path is the steady-state
///   writer; the snapshot field exists so a late-joiner sees
///   current per-peer holdings before any live announce arrives.
///
/// These rules make `restore` an idempotent CRDT merge — applying the
/// same snapshot twice is a no-op, applying overlapping snapshots
/// converges to the same state regardless of order.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound(
    serialize = "I: Serialize",
    deserialize = "I: for<'a> Deserialize<'a>",
))]
pub struct ClusterStateSnapshot<I> {
    pub tasks: HashMap<String, TaskState<I>>,
    pub current_primary: Option<String>,
    pub primary_epoch: u64,
    pub phase_deps: HashMap<PhaseId, Vec<PhaseId>>,
    /// Replicated observer set (Step 7's `RoleTable.observers`). A
    /// late-joiner needs this immediately to apply the election
    /// filter (`secondary::election::lowest_alive` skips observers);
    /// the live PeerInfo broadcast that arrives shortly after will
    /// supersede it, but in the gap between snapshot-restore and
    /// the next PeerInfo broadcast, the joiner would otherwise
    /// promote an observer candidate.
    #[serde(default)]
    pub observers: HashSet<String>,
    /// Replicated per-peer holdings map. Carried so a late-joiner
    /// sees the current set of opaque resource strings each peer
    /// announces before any live `PeerResourceHoldingsUpdated`
    /// broadcast arrives. Replaced on `restore` when local is
    /// empty; otherwise kept (the live apply path is the steady-
    /// state writer; the snapshot is authoritative for first-
    /// bootstrap only — same shape as `observers` and `phase_deps`).
    /// `#[serde(default)]` keeps wire compat with senders running an
    /// older crate (missing field deserializes as an empty map,
    /// identical to the pre-variant shape).
    #[serde(default)]
    pub peer_holdings: HashMap<String, HashSet<String>>,
}

fn task_state_rank<I>(s: &TaskState<I>) -> u8 {
    match s {
        // Pending and Blocked are both non-dispatching states; Blocked
        // ranks above Pending because it carries the cascade prereq
        // identity (`on`) — a snapshot's Blocked should not be silently
        // overwritten by a stale peer's Pending observation.
        TaskState::Pending { .. } => 0,
        TaskState::Blocked { .. } => 1,
        // An active dispatch (InFlight) supersedes cascade-paused
        // observers — if any peer saw the worker pick the task up, that
        // happens-after the cascade decision.
        TaskState::InFlight { .. } => 2,
        // All terminals share the strongest rank. Convergence among
        // terminals follows the per-arm rules in `apply` (Completed
        // never regresses; Failed/Unfulfillable lock out incoming
        // TaskFailed for their own hash).
        TaskState::Completed { .. }
        | TaskState::Failed { .. }
        | TaskState::Unfulfillable { .. } => 3,
    }
}

impl<I: Identifier> ClusterState<I> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn task_count(&self) -> usize {
        self.tasks.len()
    }

    pub fn task_state(&self, hash: &str) -> Option<&TaskState<I>> {
        self.tasks.get(hash)
    }

    /// Iterator over `(&hash, &TaskState)` for every entry in the
    /// ledger. Used by post-promotion hydration that needs to make
    /// state-dependent decisions per task (Pending → into pool;
    /// terminal → contribute task_id to completed-deps seed;
    /// InFlight → skip).
    pub fn tasks_iter(&self) -> impl Iterator<Item = (&String, &TaskState<I>)> {
        self.tasks.iter()
    }

    pub fn iter_pending(&self) -> impl Iterator<Item = (&String, &TaskInfo<I>)> {
        self.tasks.iter().filter_map(|(h, s)| match s {
            TaskState::Pending { task } => Some((h, task)),
            _ => None,
        })
    }

    pub fn iter_in_flight(&self) -> impl Iterator<Item = (&String, &str, WorkerId)> {
        self.tasks.iter().filter_map(|(h, s)| match s {
            TaskState::InFlight {
                secondary, worker, ..
            } => Some((h, secondary.as_str(), *worker)),
            _ => None,
        })
    }

    pub fn counts(&self) -> StateCounts {
        let mut c = StateCounts::default();
        for s in self.tasks.values() {
            match s {
                TaskState::Pending { .. } => c.pending += 1,
                TaskState::InFlight { .. } => c.in_flight += 1,
                TaskState::Completed { .. } => c.completed += 1,
                TaskState::Failed { .. } => c.failed += 1,
                TaskState::Unfulfillable { .. } => c.unfulfillable += 1,
                TaskState::Blocked { .. } => c.blocked += 1,
            }
        }
        c
    }

    /// Per-ErrorType partition of terminal-state tasks, in the shape
    /// the operator-facing log lines consume (`succeeded` / `fail_retry`
    /// / `fail_oom` / `fail_final`). Iterates the CRDT-replicated
    /// `tasks` map once; `O(n)` over the ledger.
    ///
    /// Distinguished from [`Self::counts`] by the failure-class
    /// breakdown: `counts().failed` collapses every `Failed { kind, .. }`
    /// into a single number, whereas `outcome_counts()` partitions
    /// the same set by `kind` per the [`OutcomeSummary`] mapping rules.
    /// `counts()` stays the small-and-fast accessor for tests / state-
    /// machine assertions; `outcome_counts()` is the operator-readable
    /// shape.
    ///
    /// CRDT-authoritative: every replica observes the same partition
    /// after the same mutation set lands, so this is the correct read
    /// for any "what did the cluster as a whole achieve" log line —
    /// including the demoted primary's terminal log, which the
    /// per-node `completed_tasks`/`failed_tasks` HashSets historically
    /// undercounted whenever cross-secondary completions reached the
    /// CRDT but not the local mirror (the asm-tokenizer "0/0/0/0/0"
    /// post-Step-6 cosmetic).
    pub fn outcome_counts(&self) -> OutcomeSummary {
        let mut o = OutcomeSummary::default();
        for s in self.tasks.values() {
            match s {
                TaskState::Completed { .. } => o.succeeded += 1,
                TaskState::Failed { kind, .. } => match kind {
                    ErrorType::Recoverable => o.fail_retry += 1,
                    ErrorType::ResourceExhausted(k) if k.as_str() == "memory" => {
                        o.fail_oom += 1
                    }
                    // Defensive: the apply rule for `TaskFailed` routes
                    // `Unfulfillable` straight to `TaskState::Unfulfillable`
                    // (the discrete state below), so this arm is unreachable
                    // in practice. Kept for exhaustiveness — if a legacy
                    // wire path ever lands a `Failed { Unfulfillable, .. }`
                    // entry the count still partitions correctly.
                    ErrorType::ResourceExhausted(_)
                    | ErrorType::NonRecoverable
                    | ErrorType::Unfulfillable { .. } => o.fail_final += 1,
                },
                // Discrete `Unfulfillable` state: reinjectable resource-
                // availability failure. Tallied as `fail_final` for the
                // operator-readable buckets until the dedicated
                // reinject/blocked bucket lands; same mapping as the
                // legacy `Failed { Unfulfillable, .. }` arm above so the
                // total partition stays stable across the variant cutover.
                TaskState::Unfulfillable { .. } => o.fail_final += 1,
                // Non-terminal: Pending, InFlight, and Blocked all
                // contribute to neither bucket. Blocked tasks are
                // cascade-paused dependents that will auto-resume to
                // Pending when their prereq completes; they're not a
                // terminal outcome and counting them as one would
                // double-tally on the eventual resumed run.
                TaskState::Pending { .. }
                | TaskState::InFlight { .. }
                | TaskState::Blocked { .. } => {}
            }
        }
        o
    }

    /// Iterator over `(task_hash, &TaskInfo)` for every entry in the
    /// ledger, regardless of state. Used by the post-promotion
    /// hydration path to seed `mark_tasks_completed` with the
    /// task_ids of terminal tasks (so surviving Pending tasks'
    /// `task_depends_on` references resolve).
    pub fn iter_all(&self) -> impl Iterator<Item = (&String, &TaskInfo<I>)> {
        self.tasks.iter().map(|(h, s)| {
            let t = match s {
                TaskState::Pending { task }
                | TaskState::InFlight { task, .. }
                | TaskState::Completed { task }
                | TaskState::Failed { task, .. }
                | TaskState::Unfulfillable { task, .. }
                | TaskState::Blocked { task, .. } => task,
            };
            (h, t)
        })
    }

    /// Iterator over `(task_hash, &TaskInfo)` for terminal entries
    /// (`Completed`, `Failed`, `Unfulfillable`). `Blocked` is non-
    /// terminal (auto-resumes to `Pending` when its prereq completes)
    /// and is excluded.
    pub fn iter_terminal(&self) -> impl Iterator<Item = (&String, &TaskInfo<I>)> {
        self.tasks.iter().filter_map(|(h, s)| match s {
            TaskState::Completed { task }
            | TaskState::Failed { task, .. }
            | TaskState::Unfulfillable { task, .. } => Some((h, task)),
            _ => None,
        })
    }

    pub fn current_primary(&self) -> Option<&str> {
        self.current_primary.as_deref()
    }

    pub fn primary_epoch(&self) -> u64 {
        self.primary_epoch
    }

    /// Clone the shared [`Arc<AtomicU64>`] mirror of `primary_epoch`
    /// for an off-`apply` reader to install into a long-lived task.
    /// The mirror is updated synchronously by every `apply` /
    /// `restore` arm that bumps `primary_epoch`, **before** role-
    /// change hooks fire — see field doc on `primary_epoch_mirror`
    /// for the memory-ordering contract.
    ///
    /// The one production reader today is the observer's resource-
    /// holdings announcer (`crate::observer::announcer`), which reads
    /// the mirror at send time so a broadcast that retries past a
    /// further `PrimaryChanged` automatically picks up the newer
    /// epoch instead of carrying the stale trigger-time value.
    pub fn primary_epoch_mirror(&self) -> Arc<std::sync::atomic::AtomicU64> {
        Arc::clone(&self.primary_epoch_mirror)
    }

    pub fn phase_deps(&self) -> &HashMap<PhaseId, Vec<PhaseId>> {
        &self.phase_deps
    }

    /// Borrow the replicated per-peer holdings map. Each entry is
    /// the set of opaque resource strings the named peer most
    /// recently announced (via `ClusterMutation::PeerResourceHoldingsUpdated`).
    /// The framework does not interpret the strings; downstream
    /// consumers attach meaning.
    pub fn peer_holdings(&self) -> &HashMap<String, HashSet<String>> {
        &self.peer_holdings
    }

    /// Snapshot of the replicated [`RoleTable`]. Borrowed for the
    /// lifetime of `&self`; callers wanting an owned copy should
    /// `.clone()` the returned reference. The transport-side
    /// write-through cache (registered via the
    /// [`RoleChangeHookRegistrar`] impl) is the expected reader on
    /// the hot path — `role_table()` is for cluster-state inspect
    /// callers like tests / metrics.
    pub fn role_table(&self) -> &RoleTable {
        &self.role_table
    }

    /// Fire every registered hook against the current [`RoleTable`].
    /// Invoked from `apply` immediately AFTER any mutation that
    /// touches the table, so registrants see post-state values.
    /// Private — `apply` is the only legitimate caller; external
    /// triggering would let a hook fire on a state it does not
    /// describe.
    fn fire_role_change_hooks(&self) {
        for hook in &self.role_change_hooks {
            hook(&self.role_table);
        }
    }

    /// Auto-resume helper: scan every entry in `tasks`, transition any
    /// `TaskState::Blocked { on, task }` whose `on` matches `prereq_hash`
    /// back to `Pending { task }`.
    ///
    /// Invoked from the `TaskCompleted` apply arm — completion of a
    /// prerequisite is the event that unblocks every cascade-paused
    /// dependent. Linear scan over `tasks` because the CRDT does not
    /// maintain a hash-keyed reverse index (the PendingPool's
    /// `dependents_of` is task-id-keyed and lives only on the primary;
    /// every replica must run this auto-resume locally to converge,
    /// and the scan keeps the dependency-tracking concern self-
    /// contained inside cluster_state).
    ///
    /// Single-pass and self-contained: the resumed entries land in
    /// `Pending` immediately, so a chain of blocked dependents waiting
    /// on the same prereq all resume in one call. Further chained
    /// resumes (a now-resumed task itself completing later) fire on
    /// their own `TaskCompleted` apply arm; no recursion here.
    ///
    /// Implementation note: two-pass (collect hashes, then mutate) so
    /// the inner `TaskInfo<I>` can be moved out by value without
    /// requiring `I: Default` or unsafe placeholder construction. The
    /// hashmap-key clone is the only allocation; the `TaskInfo` move
    /// itself is in-place.
    fn resume_blocked_on(&mut self, prereq_hash: &str) {
        let to_resume: Vec<String> = self
            .tasks
            .iter()
            .filter_map(|(h, s)| match s {
                TaskState::Blocked { on, .. } if on == prereq_hash => Some(h.clone()),
                _ => None,
            })
            .collect();
        for h in to_resume {
            if let Some(TaskState::Blocked { task, .. }) = self.tasks.remove(&h) {
                self.tasks.insert(h, TaskState::Pending { task });
            }
        }
    }

    /// Enqueue a [`PeerLifecycleEvent`] onto the dispatcher channel.
    ///
    /// Non-blocking, infallible (errors are silently dropped): the
    /// receiver-gone case happens during clean shutdown when the
    /// coordinator's dispatcher task has already exited, and the
    /// no-sender-installed case happens in unit tests that exercise
    /// the apply path in isolation. Both are non-events from the
    /// apply path's perspective — it MUST NOT panic, block, or
    /// surface a user-visible error on emit, because the broadcast
    /// happens-before observation is the only contract the CRDT
    /// promises and the dispatcher channel is strictly best-effort
    /// observation on top.
    ///
    /// CCD-9 invariant: this method must never invoke a listener
    /// directly. Listener invocation happens off the apply task on
    /// the dispatcher; the channel is the only synchronization
    /// crossing.
    pub(crate) fn emit_lifecycle_event(&self, event: PeerLifecycleEvent) {
        if let Some(tx) = &self.lifecycle_tx {
            // `send` on `UnboundedSender` only fails when the
            // receiver is dropped; silent drop matches the
            // "best-effort observation" contract documented above.
            let _ = tx.send(event);
        }
    }

    /// Attach the dispatcher's sender end so subsequent
    /// `emit_lifecycle_event` calls route events through the
    /// coordinator's dispatcher task.
    ///
    /// Called by the coordinator at `new()` time after building the
    /// (sender, receiver) pair; the receiver is then handed to
    /// [`crate::peer_lifecycle::run_peer_lifecycle_dispatcher`] when
    /// the coordinator's tokio runtime is live. Re-installation
    /// replaces the prior sender silently — the only legitimate
    /// caller is the owning coordinator, and a coordinator that
    /// re-installs is signalling "the prior dispatcher is gone, use
    /// the new one"; we have no use case for stacking dispatchers.
    pub fn install_lifecycle_sender(
        &mut self,
        tx: tokio::sync::mpsc::UnboundedSender<PeerLifecycleEvent>,
    ) {
        self.lifecycle_tx = Some(tx);
    }

    /// Enqueue a [`MatcherTriggerEvent`] onto the matcher-pipeline channel.
    ///
    /// Same best-effort / non-blocking / non-panicking contract as
    /// [`Self::emit_lifecycle_event`] — no installed sender or a
    /// closed receiver is a silent drop; the matcher pipeline is a
    /// strictly-observational layer on top of the CRDT.
    ///
    /// CCD-9 invariant: this method must never invoke the matcher
    /// directly. Matcher invocation happens off the apply task in the
    /// operational `select!` loop; the channel is the only
    /// synchronization crossing.
    ///
    /// TODO (E1): the apply rule for
    /// `ClusterMutation::PeerResourceHoldingsUpdated` is the only
    /// legitimate production caller. Until that variant + its apply
    /// rule land, the only paths that invoke this are tests (via the
    /// `trigger_fulfillability_matcher_for_test` shim below).
    #[allow(dead_code)]
    pub(crate) fn emit_matcher_trigger(&self, event: MatcherTriggerEvent) {
        if let Some(tx) = &self.matcher_trigger_tx {
            let _ = tx.send(event);
        }
    }

    /// Attach the matcher-pipeline sender end so subsequent
    /// `emit_matcher_trigger` calls route events through the
    /// coordinator's operational-loop drain.
    ///
    /// Called by the coordinator at `new()` time after building the
    /// (sender, receiver) pair; the receiver is then consumed by
    /// [`crate::fulfillability_matcher::drain_matcher_batch`] from
    /// inside the `select!` loop. Same re-installation semantics as
    /// [`Self::install_lifecycle_sender`].
    pub fn install_matcher_trigger_sender(
        &mut self,
        tx: tokio::sync::mpsc::UnboundedSender<MatcherTriggerEvent>,
    ) {
        self.matcher_trigger_tx = Some(tx);
    }


    /// Take a snapshot of the whole state. The snapshot is a deep
    /// clone — applying further mutations to `self` after this call
    /// does not affect the returned snapshot. Used as the response
    /// payload to `RequestClusterSnapshot`.
    pub fn snapshot(&self) -> ClusterStateSnapshot<I> {
        ClusterStateSnapshot {
            tasks: self.tasks.clone(),
            current_primary: self.current_primary.clone(),
            primary_epoch: self.primary_epoch,
            phase_deps: self.phase_deps.clone(),
            // Carry the replicated observer set through the snapshot
            // so a late-joiner can populate `RoleTable.observers`
            // before any `PeerJoined` mutation arrives. The set is
            // the same one the `PeerJoined { is_observer = true }`
            // apply rule writes; the snapshot is authoritative for
            // first-bootstrap and inert thereafter.
            observers: self.role_table.observers.clone(),
            // Per-peer holdings — same first-bootstrap-only
            // contract as `observers` (replaced on restore when
            // local is empty, otherwise kept).
            peer_holdings: self.peer_holdings.clone(),
        }
    }

    /// Merge a snapshot into local state per the CRDT lattice
    /// described on `ClusterStateSnapshot`. Idempotent: applying the
    /// same snapshot twice produces the same state as applying it
    /// once; applying overlapping snapshots converges regardless of
    /// order.
    ///
    /// Why merge (not replace): a node may have already applied
    /// live broadcasts before the snapshot RPC response arrives —
    /// for example, peer B's `TaskCompleted` reaches the joiner
    /// before peer A's snapshot does. Replacing would lose B's
    /// mutation; merging keeps the strictly stronger of (local,
    /// snapshot) per the lattice and stays correct under arbitrary
    /// interleaving of live broadcasts and snapshot delivery.
    pub fn restore(&mut self, snap: ClusterStateSnapshot<I>) {
        for (hash, incoming) in snap.tasks {
            match self.tasks.get(&hash) {
                None => {
                    self.tasks.insert(hash, incoming);
                }
                Some(local) => {
                    if task_state_rank(&incoming) > task_state_rank(local) {
                        self.tasks.insert(hash, incoming);
                    }
                }
            }
        }
        if snap.primary_epoch > self.primary_epoch {
            self.primary_epoch = snap.primary_epoch;
            // Mirror update on the snapshot-merge path mirrors the live
            // `PrimaryChanged` apply rule — same `Release` ordering, same
            // pre-`fire_role_change_hooks` write — so a late-joiner's
            // announcer wakes from the restore-time trigger and reads the
            // restored epoch, not the cold-start 0.
            self.primary_epoch_mirror
                .store(snap.primary_epoch, std::sync::atomic::Ordering::Release);
            self.current_primary = snap.current_primary.clone();
            // Keep the replicated `RoleTable` in lockstep with
            // `current_primary` even when the new value lands via
            // the snapshot-merge path (late joiner / reconnect),
            // not just via live `PrimaryChanged` mutations. The
            // role-change hook fires AFTER the table update so any
            // registered write-through cache stays coherent with
            // the post-merge state.
            self.role_table.primary = snap.current_primary;
            self.fire_role_change_hooks();
        }
        if self.phase_deps.is_empty() {
            self.phase_deps = snap.phase_deps;
        }
        // Observer set: replace if local is empty (first-bootstrap
        // case), otherwise keep local. The live `PeerJoined` apply
        // path is the steady-state writer (set-semantics insert);
        // this branch only fires on the late-joiner's very first
        // restore, before any `PeerJoined` mutation arrives. Firing
        // the role-change hooks when the set actually changes keeps
        // the transport's write-through cache coherent on the
        // snapshot path the same way `PeerJoined` does on the live
        // path.
        if self.role_table.observers.is_empty() && !snap.observers.is_empty() {
            self.role_table.observers = snap.observers;
            self.fire_role_change_hooks();
        }
        // Peer-holdings map: same first-bootstrap-only contract
        // as `observers` and `phase_deps`. The live
        // `PeerResourceHoldingsUpdated` apply path is the steady-
        // state writer; the snapshot field is authoritative only
        // before any live announce reaches this replica. No hook
        // fire here: holdings-change hooks (wired by the sibling
        // E3 subtask via the lifecycle dispatcher mpsc) are
        // per-peer-announce signals, not snapshot-bootstrap signals.
        if self.peer_holdings.is_empty() && !snap.peer_holdings.is_empty() {
            self.peer_holdings = snap.peer_holdings;
        }
    }

    pub fn apply(&mut self, m: ClusterMutation<I>) -> ApplyOutcome {
        match m {
            ClusterMutation::TaskAdded { hash, task } => {
                if self.tasks.contains_key(&hash) {
                    ApplyOutcome::NoOp
                } else {
                    self.tasks.insert(hash, TaskState::Pending { task });
                    ApplyOutcome::Applied
                }
            }
            ClusterMutation::TaskAssigned {
                hash,
                secondary,
                worker,
            } => {
                let Some(state) = self.tasks.get_mut(&hash) else {
                    return ApplyOutcome::NoOp;
                };
                let task = match state {
                    TaskState::Pending { task } => task.clone(),
                    _ => return ApplyOutcome::NoOp,
                };
                *state = TaskState::InFlight {
                    task,
                    secondary,
                    worker,
                };
                ApplyOutcome::Applied
            }
            ClusterMutation::TaskCompleted { hash } => {
                let Some(state) = self.tasks.get_mut(&hash) else {
                    return ApplyOutcome::NoOp;
                };
                let task = match state {
                    // Idempotent dedup on a redundant TaskCompleted (the
                    // same hash arrives twice via peer-forwarding
                    // redundancy or snapshot replay).
                    TaskState::Completed { .. } => return ApplyOutcome::NoOp,
                    // Retry-success supersedes a prior Recoverable
                    // failure: the retry pass re-injects the binary,
                    // a worker picks it up, and the next TaskCompleted
                    // for the same hash legitimately transitions
                    // Failed → Completed. Pre-fix this branch NoOp'd,
                    // leaving the ledger stuck at `Failed { Recoverable }`
                    // even though the task ultimately succeeded — so
                    // `outcome_counts().succeeded` undercounted the
                    // retry-successes and the run-done logic that reads
                    // it never saw the cluster reach "all terminal as
                    // succeeded". The HashSet-side bookkeeping in
                    // `primary/task.rs::handle_task_complete` already
                    // implements this same supersession; this arm
                    // brings the CRDT into agreement so cross-node
                    // mirrors converge to the right terminal state.
                    //
                    // Commutativity: if peer A observes
                    // (TaskFailed, TaskCompleted) for the same hash and
                    // peer B observes (TaskCompleted, TaskFailed), both
                    // converge to `Completed` — A applies Failed then
                    // transitions to Completed here; B applies Completed
                    // then NoOps the late TaskFailed (the Completed
                    // arm in `TaskFailed` below). Success is the
                    // strongest terminal regardless of arrival order;
                    // the prior `attempts` / `last_error` are dropped
                    // because the cluster's authoritative outcome for
                    // this hash is now success.
                    //
                    // `Unfulfillable` and `Blocked` both yield the same
                    // way: if the run somehow reaches Completed for the
                    // hash (worker raced ahead of the cascade decision,
                    // or external resolver dispatched the binary out-of-
                    // band), success is still the strongest terminal.
                    TaskState::Failed { task, .. }
                    | TaskState::Unfulfillable { task, .. }
                    | TaskState::Blocked { task, .. } => task.clone(),
                    TaskState::Pending { task } | TaskState::InFlight { task, .. } => task.clone(),
                };
                *state = TaskState::Completed { task };
                // Auto-resume: any `Blocked { on: <this hash>, .. }`
                // dependent transitions back to `Pending` so the next
                // dispatch tick on the live primary picks it up. Event-
                // driven (apply-rule-local) rather than retry-pass
                // wall-clock; the same broadcast that converges this
                // hash to Completed converges every blocked dependent
                // to Pending on the same apply call across every
                // replica.
                self.resume_blocked_on(&hash);
                ApplyOutcome::Applied
            }
            ClusterMutation::TaskFailed { hash, kind, error } => {
                let Some(state) = self.tasks.get_mut(&hash) else {
                    return ApplyOutcome::NoOp;
                };
                match state {
                    // Strongest terminals lock out incoming TaskFailed.
                    // `Completed` never regresses; `Unfulfillable` is a
                    // stable reinjectable terminal — a late generic
                    // worker-originated TaskFailed must not regress it.
                    TaskState::Completed { .. }
                    | TaskState::Unfulfillable { .. } => ApplyOutcome::NoOp,
                    TaskState::Failed {
                        kind: k,
                        last_error,
                        attempts,
                        ..
                    } => {
                        *attempts += 1;
                        *k = kind;
                        *last_error = error;
                        ApplyOutcome::Applied
                    }
                    // Non-terminal states transition based on the
                    // error class: `Unfulfillable` routes to the
                    // discrete state so downstream matcher / reinject
                    // logic can dispatch on the discriminant; every
                    // other `ErrorType` lands in the generic `Failed`
                    // bucket preserving the legacy attempts/last_error
                    // shape.
                    TaskState::Pending { task }
                    | TaskState::InFlight { task, .. }
                    | TaskState::Blocked { task, .. } => {
                        let task = task.clone();
                        *state = match kind {
                            ErrorType::Unfulfillable { reason } => {
                                TaskState::Unfulfillable {
                                    task,
                                    reason: reason.to_string(),
                                }
                            }
                            other => TaskState::Failed {
                                task,
                                kind: other,
                                last_error: error,
                                attempts: 1,
                            },
                        };
                        ApplyOutcome::Applied
                    }
                }
            }
            ClusterMutation::PrimaryChanged { new, epoch } => {
                if epoch < self.primary_epoch {
                    return ApplyOutcome::NoOp;
                }
                if epoch == self.primary_epoch && self.current_primary.as_deref() == Some(&new) {
                    return ApplyOutcome::NoOp;
                }
                self.current_primary = Some(new.clone());
                self.primary_epoch = epoch;
                // Keep the lock-free epoch mirror in lockstep with the
                // field so off-`apply` readers (the observer
                // resource-holdings announcer) see the post-mutation
                // value when their hook is fired below. `Release`
                // pairs with the announcer's `Acquire` load. Writing
                // BEFORE `fire_role_change_hooks` ensures any hook
                // observer that synchronously reads the mirror
                // observes the new value.
                self.primary_epoch_mirror
                    .store(epoch, std::sync::atomic::Ordering::Release);
                // Replicated `RoleTable` mutation: kept in lockstep
                // with `current_primary` so the transport-layer
                // write-through cache (Step 2) observes a coherent
                // snapshot. Hook fires AFTER the field update, so
                // registrants see the post-mutation value.
                self.role_table.primary = Some(new);
                self.fire_role_change_hooks();
                ApplyOutcome::Applied
            }
            ClusterMutation::PhaseDepsSet { deps } => {
                if !self.phase_deps.is_empty() {
                    // Static config: re-application is silent.
                    return ApplyOutcome::NoOp;
                }
                self.phase_deps = deps;
                ApplyOutcome::Applied
            }
            ClusterMutation::RunComplete => {
                if self.run_complete {
                    return ApplyOutcome::NoOp;
                }
                self.run_complete = true;
                ApplyOutcome::Applied
            }
            ClusterMutation::TaskReinjected { hash } => {
                // External-control reinjection moves a
                // `Unfulfillable { .. }` entry back to `Pending`. Any
                // other state is a NoOp so out-of-order delivery and
                // post-completion re-applies can't regress the ledger.
                //
                // Tightened from the pre-variant matcher
                // (`Failed { NonRecoverable, .. }`) in lockstep with
                // `apply_reinject_task` in the command channel: the
                // operator-resolvable-failure class now has its own
                // discrete state, so the apply rule rejects anything
                // outside it.
                let Some(state) = self.tasks.get_mut(&hash) else {
                    return ApplyOutcome::NoOp;
                };
                let task = match state {
                    TaskState::Unfulfillable { task, .. } => task.clone(),
                    _ => return ApplyOutcome::NoOp,
                };
                *state = TaskState::Pending { task };
                ApplyOutcome::Applied
            }
            ClusterMutation::TaskBlocked { hash, on } => {
                // Cascade-paused dependent: transition Pending →
                // Blocked, preserving the TaskInfo so auto-resume can
                // re-dispatch the same binary. Idempotent under
                // re-application against a `Blocked` entry whose `on`
                // matches; mismatched `on` (peer A says blocked-on-X,
                // peer B has blocked-on-Y) keeps local — the first
                // observed cascade root wins. Terminal states
                // (Completed, Failed, Unfulfillable) and an active
                // dispatch (InFlight) lock out the cascade decision:
                // a late TaskBlocked must not regress a worker's
                // observed outcome.
                let Some(state) = self.tasks.get_mut(&hash) else {
                    return ApplyOutcome::NoOp;
                };
                match state {
                    TaskState::Completed { .. }
                    | TaskState::Failed { .. }
                    | TaskState::Unfulfillable { .. }
                    | TaskState::InFlight { .. } => ApplyOutcome::NoOp,
                    TaskState::Blocked { on: existing_on, .. } => {
                        if existing_on == &on {
                            ApplyOutcome::NoOp
                        } else {
                            // First observed cascade root wins; a
                            // divergent re-cascade against the same
                            // dependent is silent.
                            ApplyOutcome::NoOp
                        }
                    }
                    TaskState::Pending { task } => {
                        let task = task.clone();
                        *state = TaskState::Blocked { task, on };
                        ApplyOutcome::Applied
                    }
                }
            }
            ClusterMutation::TaskPreferredSecondariesUpdated { hash, secondaries } => {
                let Some(state) = self.tasks.get_mut(&hash) else {
                    return ApplyOutcome::NoOp;
                };
                let task = match state {
                    TaskState::Pending { task }
                    | TaskState::InFlight { task, .. }
                    | TaskState::Completed { task }
                    | TaskState::Failed { task, .. }
                    | TaskState::Unfulfillable { task, .. }
                    | TaskState::Blocked { task, .. } => task,
                };
                task.preferred_secondaries = SoftPreferredSecondaries::new(secondaries);
                ApplyOutcome::Applied
            }
            ClusterMutation::PeerJoined {
                peer_id,
                is_observer,
            } => self.apply_peer_joined(peer_id, is_observer),
            ClusterMutation::PeerRemoved { id, cause } => {
                self.apply_peer_removed(id, cause)
            }
            ClusterMutation::PeerResourceHoldingsUpdated {
                peer_id,
                holdings,
                epoch,
            } => self.apply_peer_resource_holdings_updated(peer_id, holdings, epoch),
        }
    }

    /// Apply a `ClusterMutation::PeerJoined`.
    ///
    /// Sticky-per-id removal wins: if the id is currently `Dead` in
    /// `peer_state`, the broadcast is logged at `warn` and dropped
    /// (NoOp). Otherwise the entry is brought to `Alive` (insert or
    /// in-place ratchet of `is_observer` upward; the observer flag
    /// never regresses true→false via `PeerJoined`, only the matching
    /// `PeerRemoved` can clear it). The `RoleTable.observers`
    /// projection is updated in lockstep and role-change hooks fire
    /// when the set actually changes. A `PeerLifecycleEvent::Added`
    /// is emitted on every state-changing apply; pure-idempotent
    /// re-deliveries return NoOp and emit nothing.
    fn apply_peer_joined(&mut self, peer_id: String, is_observer: bool) -> ApplyOutcome {
        match self.peer_state.get(&peer_id) {
            Some(entry) if entry.state == PeerState::Dead => {
                tracing::warn!(
                    target: "dynrunner_cluster_state",
                    peer_id = %peer_id,
                    "PeerJoined for dead id ignored",
                );
                return ApplyOutcome::NoOp;
            }
            _ => {}
        }
        let (entry_was_new, observer_set_changed) = match self.peer_state.get_mut(&peer_id) {
            None => {
                self.peer_state.insert(
                    peer_id.clone(),
                    PeerEntry {
                        state: PeerState::Alive,
                        pubkey: None,
                        endpoint: None,
                        is_observer,
                    },
                );
                let observer_set_changed =
                    is_observer && self.role_table.observers.insert(peer_id.clone());
                (true, observer_set_changed)
            }
            Some(entry) => {
                // Ratchet the observer flag upward only. Stale flip-
                // back broadcasts (`is_observer = false` for an
                // already-observed peer) must not regress the
                // projection — only `PeerRemoved` clears observer
                // status.
                if is_observer && !entry.is_observer {
                    entry.is_observer = true;
                    let inserted = self.role_table.observers.insert(peer_id.clone());
                    (false, inserted)
                } else {
                    (false, false)
                }
            }
        };
        if observer_set_changed {
            self.fire_role_change_hooks();
        }
        if !entry_was_new && !observer_set_changed {
            return ApplyOutcome::NoOp;
        }
        self.emit_lifecycle_event(PeerLifecycleEvent::Added {
            id: peer_id,
            is_observer,
        });
        ApplyOutcome::Applied
    }

    /// Apply a `ClusterMutation::PeerRemoved`.
    ///
    /// Sticky-per-id: once `peer_state[id]` is `Dead`, any further
    /// `PeerRemoved` for the same id is a silent NoOp. An `Absent`
    /// id is inserted as `Dead` so the entry blocks any late
    /// out-of-order `PeerJoined` for the same id. Observers lose
    /// their projection on removal; role-change hooks fire when the
    /// set actually shrinks. A `PeerLifecycleEvent::Removed` is
    /// emitted on every state-changing apply.
    fn apply_peer_removed(&mut self, id: String, cause: RemovalCause) -> ApplyOutcome {
        if let Some(entry) = self.peer_state.get(&id) {
            if entry.state == PeerState::Dead {
                return ApplyOutcome::NoOp;
            }
        }
        let observer_set_changed = match self.peer_state.get_mut(&id) {
            None => {
                self.peer_state.insert(
                    id.clone(),
                    PeerEntry {
                        state: PeerState::Dead,
                        pubkey: None,
                        endpoint: None,
                        is_observer: false,
                    },
                );
                false
            }
            Some(entry) => {
                entry.state = PeerState::Dead;
                let was_observer = entry.is_observer;
                entry.is_observer = false;
                was_observer && self.role_table.observers.remove(&id)
            }
        };
        if observer_set_changed {
            self.fire_role_change_hooks();
        }
        self.emit_lifecycle_event(PeerLifecycleEvent::Removed { id, cause });
        ApplyOutcome::Applied
    }

    /// Apply a `ClusterMutation::PeerResourceHoldingsUpdated`.
    ///
    /// Supersede-old-pending semantics on `epoch`: an announce whose
    /// `epoch` is strictly older than the current `primary_epoch` is
    /// dropped as stale (the announcing peer hadn't yet learned of
    /// the current primary when it sent). Same-or-newer epoch is
    /// accepted — the announce is about per-peer holdings, not
    /// about primary identity, and a peer that already learned of a
    /// newer primary before its announce reached us is still
    /// authoritative about its own holdings.
    ///
    /// Replace-if-changed: the incoming `Vec<String>` is collected
    /// into a `HashSet<String>` (so duplicate strings inside a
    /// single announce collapse) and compared against the stored
    /// set for the same `peer_id`. Unchanged → NoOp; changed (or
    /// first-time insertion) → replace and return `Applied`.
    ///
    /// No `peer_state` liveness gate today: a `PeerResourceHoldingsUpdated`
    /// for a peer the CRDT has never seen `PeerJoined` for is
    /// accepted (the announce IS evidence the peer is alive enough
    /// to send); for a peer marked `Dead` in `peer_state` the
    /// announce is still recorded but downstream consumers reading
    /// `peer_state` alongside `peer_holdings` can apply their own
    /// liveness filter. The CRDT layer's only contract is "store
    /// what was announced under the supersede-by-epoch rule"; a
    /// liveness filter belongs to the consumer policy.
    fn apply_peer_resource_holdings_updated(
        &mut self,
        peer_id: String,
        holdings: Vec<String>,
        epoch: u64,
    ) -> ApplyOutcome {
        if epoch < self.primary_epoch {
            return ApplyOutcome::NoOp;
        }
        let incoming: HashSet<String> = holdings.into_iter().collect();
        match self.peer_holdings.get(&peer_id) {
            Some(existing) if existing == &incoming => ApplyOutcome::NoOp,
            _ => {
                self.peer_holdings.insert(peer_id, incoming);
                ApplyOutcome::Applied
            }
        }
    }

    /// Whether the run has been declared finished by the primary.
    /// Sticky monotonic flag: once set, never clears for the lifetime
    /// of this state. Secondaries read this to break their main loop
    /// when the peer mesh is still up but the run is genuinely over.
    pub fn run_complete(&self) -> bool {
        self.run_complete
    }
}

/// `ClusterState` is the authoritative role-table owner; transports
/// register their write-through cache through this boundary trait.
///
/// The implementation appends to the internal `Vec<RoleChangeHook>`;
/// hooks accumulate across calls and are fired (in registration
/// order) by `apply` whenever a mutation actually changes the table.
/// Today the only registrant is the `PeerTransport` write-through
/// cache, one per node.
impl<I: Identifier> RoleChangeHookRegistrar for ClusterState<I> {
    fn register_role_change_hook(
        &mut self,
        hook: Box<dyn Fn(&RoleTable) + Send + Sync + 'static>,
    ) {
        self.role_change_hooks.push(Arc::from(hook));
    }
}

/// Apply each mutation to `state` locally and return the subset that
/// actually changed state (`ApplyOutcome::Applied`). `NoOp` mutations
/// are dropped — under the CRDT's idempotency contract a re-application
/// against the post-state is silent, and re-broadcasting a NoOp would
/// amplify under peer-forward redundancy (every peer forwarding observed
/// terminal events to the primary would turn one TaskComplete into N
/// re-broadcasts = N² messages).
///
/// Single concern: apply-locally + filter to applied. The broadcast
/// step is transport-specific (primary uses `SecondaryTransport`,
/// promoted-secondary uses `PeerTransport`; the two have different
/// error shapes) so it stays at the call site. This free function is
/// the canonical place to perform the apply+filter so the two
/// originator paths can't drift on the filter semantics.
///
/// Callers:
///   - `primary::lifecycle::apply_and_broadcast_cluster_mutations`
///     (the live primary's originator path).
///   - `secondary::primary::apply_and_broadcast_mutations` (the
///     promoted-secondary's originator path, used by
///     `ingest_setup_discovery` to seed the ledger with the
///     discovery-time `TaskAdded` batch + `PhaseDepsSet`).
pub(crate) fn apply_locally_for_broadcast<I: Identifier>(
    state: &mut ClusterState<I>,
    mutations: Vec<ClusterMutation<I>>,
) -> Vec<ClusterMutation<I>> {
    let mut applied: Vec<ClusterMutation<I>> = Vec::with_capacity(mutations.len());
    for m in mutations {
        let outcome = state.apply(m.clone());
        if outcome == ApplyOutcome::Applied {
            applied.push(m);
        }
    }
    applied
}

#[cfg(test)]
mod tests {
    use super::*;
    use dynrunner_core::{PhaseId, RunnerIdentifier, SoftPreferredSecondaries, TypeId};
    use std::path::PathBuf;

    fn mk_task(name: &str) -> TaskInfo<RunnerIdentifier> {
        TaskInfo {
            path: PathBuf::from(format!("/tasks/{name}")),
            size: 0,
            identifier: RunnerIdentifier::from(name),
            phase_id: PhaseId::from("p0"),
            type_id: TypeId::from("t0"),
            affinity_id: None,
            payload: serde_json::Value::Null,
            task_id: Some(name.into()),
            task_depends_on: Vec::new(),
            preferred_secondaries: SoftPreferredSecondaries::default(),
            resolved_path: None,
        }
    }

    #[test]
    fn task_added_idempotent() {
        let mut s = ClusterState::<RunnerIdentifier>::new();
        let t = mk_task("a");
        assert_eq!(
            s.apply(ClusterMutation::TaskAdded {
                hash: "h".into(),
                task: t.clone(),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            s.apply(ClusterMutation::TaskAdded {
                hash: "h".into(),
                task: t,
            }),
            ApplyOutcome::NoOp
        );
        assert_eq!(s.task_count(), 1);
        assert_eq!(s.counts().pending, 1);
    }

    #[test]
    fn assigned_late_after_completed_is_noop() {
        // Demonstrates terminal-states-win: TaskCompleted lands first
        // (out-of-order delivery), the late TaskAssigned must NOT
        // resurrect the entry to InFlight.
        let mut s = ClusterState::<RunnerIdentifier>::new();
        s.apply(ClusterMutation::TaskAdded {
            hash: "h".into(),
            task: mk_task("a"),
        });
        assert_eq!(
            s.apply(ClusterMutation::TaskCompleted { hash: "h".into() }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            s.apply(ClusterMutation::TaskAssigned {
                hash: "h".into(),
                secondary: "s1".into(),
                worker: 0,
            }),
            ApplyOutcome::NoOp
        );
        assert!(matches!(s.task_state("h"), Some(TaskState::Completed { .. })));
    }

    #[test]
    fn duplicate_completed_is_noop() {
        let mut s = ClusterState::<RunnerIdentifier>::new();
        s.apply(ClusterMutation::TaskAdded {
            hash: "h".into(),
            task: mk_task("a"),
        });
        s.apply(ClusterMutation::TaskCompleted { hash: "h".into() });
        assert_eq!(
            s.apply(ClusterMutation::TaskCompleted { hash: "h".into() }),
            ApplyOutcome::NoOp
        );
    }

    #[test]
    fn failed_then_completed_transitions_to_completed_retry_success() {
        // Retry-success path: the retry pass re-injects a previously
        // Recoverable-failed binary, a worker picks it up, runs to
        // success, and emits TaskCompleted for the same hash. The CRDT
        // must transition Failed → Completed (success is the strongest
        // terminal); pre-fix this branch NoOp'd, leaving the ledger
        // stuck reporting the retry-succeeded task as `fail_retry` and
        // breaking the asm-tokenizer LMU run-done detection (2-of-235
        // retried successes hung the demoted primary in RunComplete-
        // wait). Asymmetric with the `Completed`-locks-out-`Failed`
        // direction below: a late TaskFailed against a Completed entry
        // is still NoOp (success never regresses).
        let mut s = ClusterState::<RunnerIdentifier>::new();
        s.apply(ClusterMutation::TaskAdded {
            hash: "h".into(),
            task: mk_task("a"),
        });
        s.apply(ClusterMutation::TaskFailed {
            hash: "h".into(),
            kind: ErrorType::Recoverable,
            error: "x".into(),
        });
        assert_eq!(
            s.apply(ClusterMutation::TaskCompleted { hash: "h".into() }),
            ApplyOutcome::Applied
        );
        assert!(matches!(s.task_state("h"), Some(TaskState::Completed { .. })));
    }

    #[test]
    fn completed_then_failed_stays_completed_success_never_regresses() {
        // Symmetric inverse of the retry-success path: once a node has
        // observed `TaskCompleted`, a late `TaskFailed` for the same
        // hash (typically a stale redundant-dispatch path that lost the
        // race) must not regress the ledger. `Completed` is the
        // strongest terminal; the late `TaskFailed` is the NoOp side.
        //
        // Together with `failed_then_completed_transitions_to_completed_retry_success`
        // these two pins prove the lattice converges to `Completed`
        // regardless of (TaskFailed, TaskCompleted) arrival order —
        // commutativity is preserved across the asymmetric transition.
        let mut s = ClusterState::<RunnerIdentifier>::new();
        s.apply(ClusterMutation::TaskAdded {
            hash: "h".into(),
            task: mk_task("a"),
        });
        s.apply(ClusterMutation::TaskCompleted { hash: "h".into() });
        assert_eq!(
            s.apply(ClusterMutation::TaskFailed {
                hash: "h".into(),
                kind: ErrorType::Recoverable,
                error: "late".into(),
            }),
            ApplyOutcome::NoOp
        );
        assert!(matches!(s.task_state("h"), Some(TaskState::Completed { .. })));
    }

    /// Cosmetic #88 regression pin: a demoted-primary terminal log
    /// line reads `cluster_state.outcome_counts()` to get the
    /// CRDT-authoritative per-class tally. Mixed Completed +
    /// Failed-by-kind population must partition into the four
    /// `OutcomeSummary` buckets per the documented mapping
    /// (`Recoverable → fail_retry`, `ResourceExhausted("memory") →
    /// fail_oom`, other `ResourceExhausted` + `NonRecoverable` →
    /// `fail_final`); Pending / InFlight entries contribute to neither.
    #[test]
    fn outcome_counts_partitions_terminal_states_by_error_class() {
        let mut s = ClusterState::<RunnerIdentifier>::new();
        // 2 succeeded
        for hash in ["a", "b"] {
            s.apply(ClusterMutation::TaskAdded {
                hash: hash.into(),
                task: mk_task(hash),
            });
            s.apply(ClusterMutation::TaskCompleted { hash: hash.into() });
        }
        // 1 fail_retry (Recoverable)
        s.apply(ClusterMutation::TaskAdded {
            hash: "c".into(),
            task: mk_task("c"),
        });
        s.apply(ClusterMutation::TaskFailed {
            hash: "c".into(),
            kind: ErrorType::Recoverable,
            error: "x".into(),
        });
        // 1 fail_oom (ResourceExhausted("memory"))
        s.apply(ClusterMutation::TaskAdded {
            hash: "d".into(),
            task: mk_task("d"),
        });
        s.apply(ClusterMutation::TaskFailed {
            hash: "d".into(),
            kind: ErrorType::ResourceExhausted("memory".into()),
            error: "oom".into(),
        });
        // 1 fail_final (ResourceExhausted("disk") falls through)
        s.apply(ClusterMutation::TaskAdded {
            hash: "e".into(),
            task: mk_task("e"),
        });
        s.apply(ClusterMutation::TaskFailed {
            hash: "e".into(),
            kind: ErrorType::ResourceExhausted("disk".into()),
            error: "no space".into(),
        });
        // 1 fail_final (NonRecoverable)
        s.apply(ClusterMutation::TaskAdded {
            hash: "f".into(),
            task: mk_task("f"),
        });
        s.apply(ClusterMutation::TaskFailed {
            hash: "f".into(),
            kind: ErrorType::NonRecoverable,
            error: "panic".into(),
        });
        // 1 Pending (uncounted)
        s.apply(ClusterMutation::TaskAdded {
            hash: "g".into(),
            task: mk_task("g"),
        });

        let o = s.outcome_counts();
        assert_eq!(o.succeeded, 2, "two TaskCompleted entries");
        assert_eq!(o.fail_retry, 1, "Recoverable → fail_retry");
        assert_eq!(o.fail_oom, 1, "ResourceExhausted(\"memory\") → fail_oom");
        assert_eq!(
            o.fail_final, 2,
            "ResourceExhausted(other) + NonRecoverable → fail_final"
        );
        assert_eq!(o.total_terminal(), 6, "sum across all four buckets");
    }

    #[test]
    fn failed_attempts_counter_increments() {
        let mut s = ClusterState::<RunnerIdentifier>::new();
        s.apply(ClusterMutation::TaskAdded {
            hash: "h".into(),
            task: mk_task("a"),
        });
        s.apply(ClusterMutation::TaskFailed {
            hash: "h".into(),
            kind: ErrorType::Recoverable,
            error: "first".into(),
        });
        s.apply(ClusterMutation::TaskFailed {
            hash: "h".into(),
            kind: ErrorType::NonRecoverable,
            error: "second".into(),
        });
        match s.task_state("h") {
            Some(TaskState::Failed {
                kind,
                last_error,
                attempts,
                ..
            }) => {
                assert_eq!(*attempts, 2);
                assert_eq!(*kind, ErrorType::NonRecoverable);
                assert_eq!(last_error, "second");
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[test]
    fn primary_changed_higher_epoch_wins() {
        let mut s = ClusterState::<RunnerIdentifier>::new();
        s.apply(ClusterMutation::PrimaryChanged {
            new: "a".into(),
            epoch: 1,
        });
        // Lower epoch is rejected.
        assert_eq!(
            s.apply(ClusterMutation::PrimaryChanged {
                new: "b".into(),
                epoch: 0,
            }),
            ApplyOutcome::NoOp
        );
        assert_eq!(s.current_primary(), Some("a"));
        // Higher epoch wins.
        s.apply(ClusterMutation::PrimaryChanged {
            new: "c".into(),
            epoch: 5,
        });
        assert_eq!(s.current_primary(), Some("c"));
        assert_eq!(s.primary_epoch(), 5);
    }

    /// `primary_epoch_mirror` tracks `primary_epoch` on every applied
    /// `PrimaryChanged` mutation. Pins the lock-free reader contract
    /// the observer-side announcer task depends on: a `mirror.load()`
    /// in response to a role-change hook fire observes the
    /// post-mutation value, not the pre-mutation one. Reject paths
    /// (lower epoch, same epoch + same id) leave the mirror
    /// unchanged because the underlying field is also unchanged.
    #[test]
    fn primary_epoch_mirror_tracks_apply() {
        use std::sync::atomic::Ordering;
        let mut s = ClusterState::<RunnerIdentifier>::new();
        let mirror = s.primary_epoch_mirror();
        assert_eq!(mirror.load(Ordering::Acquire), 0);

        s.apply(ClusterMutation::PrimaryChanged {
            new: "a".into(),
            epoch: 3,
        });
        assert_eq!(mirror.load(Ordering::Acquire), 3);

        // NoOp branch (lower epoch) leaves the mirror untouched.
        s.apply(ClusterMutation::PrimaryChanged {
            new: "b".into(),
            epoch: 1,
        });
        assert_eq!(mirror.load(Ordering::Acquire), 3);

        s.apply(ClusterMutation::PrimaryChanged {
            new: "c".into(),
            epoch: 7,
        });
        assert_eq!(mirror.load(Ordering::Acquire), 7);
    }

    /// Snapshot-restore path keeps the mirror in lockstep with the
    /// restored epoch. The late-joiner observer's first trigger is the
    /// role-change hook firing from inside `restore`'s
    /// `primary_epoch > local` branch — that branch writes the mirror
    /// BEFORE `fire_role_change_hooks` so the announcer's first
    /// awoken read sees the restored epoch, not 0.
    #[test]
    fn primary_epoch_mirror_tracks_restore() {
        use std::sync::atomic::Ordering;
        let mut origin = ClusterState::<RunnerIdentifier>::new();
        origin.apply(ClusterMutation::PrimaryChanged {
            new: "primary-id".into(),
            epoch: 11,
        });
        let snap = origin.snapshot();

        let mut joiner = ClusterState::<RunnerIdentifier>::new();
        let mirror = joiner.primary_epoch_mirror();
        assert_eq!(mirror.load(Ordering::Acquire), 0);
        joiner.restore(snap);
        assert_eq!(mirror.load(Ordering::Acquire), 11);
    }

    #[test]
    fn iter_pending_only_returns_pending() {
        let mut s = ClusterState::<RunnerIdentifier>::new();
        s.apply(ClusterMutation::TaskAdded {
            hash: "p".into(),
            task: mk_task("p"),
        });
        s.apply(ClusterMutation::TaskAdded {
            hash: "i".into(),
            task: mk_task("i"),
        });
        s.apply(ClusterMutation::TaskAssigned {
            hash: "i".into(),
            secondary: "s".into(),
            worker: 0,
        });
        s.apply(ClusterMutation::TaskAdded {
            hash: "c".into(),
            task: mk_task("c"),
        });
        s.apply(ClusterMutation::TaskCompleted { hash: "c".into() });

        let mut pending: Vec<&str> = s.iter_pending().map(|(h, _)| h.as_str()).collect();
        pending.sort();
        assert_eq!(pending, vec!["p"]);

        let in_flight: Vec<&str> = s.iter_in_flight().map(|(h, _, _)| h.as_str()).collect();
        assert_eq!(in_flight, vec!["i"]);
    }

    /// Convergence under the realistic reordering: `TaskAdded` always
    /// happens-before `TaskAssigned` and `TaskCompleted` (the primary
    /// originates `TaskAdded` before issuing the assignment, which is
    /// itself a strict prerequisite of the worker completing). Within
    /// that constraint, `TaskCompleted` can race ahead of the matching
    /// `TaskAssigned` at a third-party receiver — both orderings must
    /// converge to `Completed`.
    #[test]
    fn convergence_completed_can_race_assigned() {
        let added = ClusterMutation::TaskAdded {
            hash: "h".into(),
            task: mk_task("a"),
        };
        let assigned = ClusterMutation::TaskAssigned {
            hash: "h".into(),
            secondary: "s".into(),
            worker: 0,
        };
        let completed: ClusterMutation<RunnerIdentifier> =
            ClusterMutation::TaskCompleted { hash: "h".into() };

        let mut a = ClusterState::<RunnerIdentifier>::new();
        a.apply(added.clone());
        a.apply(assigned.clone());
        a.apply(completed.clone());

        let mut b = ClusterState::<RunnerIdentifier>::new();
        b.apply(added);
        b.apply(completed);
        b.apply(assigned);

        assert!(matches!(a.task_state("h"), Some(TaskState::Completed { .. })));
        assert!(matches!(b.task_state("h"), Some(TaskState::Completed { .. })));
    }

    /// Convergence under duplicates: applying every mutation twice
    /// in interleaved order produces the same final state as
    /// applying each once in their natural order.
    #[test]
    fn convergence_under_duplicates() {
        let muts: Vec<ClusterMutation<RunnerIdentifier>> = vec![
            ClusterMutation::TaskAdded {
                hash: "h1".into(),
                task: mk_task("h1"),
            },
            ClusterMutation::TaskAdded {
                hash: "h2".into(),
                task: mk_task("h2"),
            },
            ClusterMutation::TaskAssigned {
                hash: "h1".into(),
                secondary: "s".into(),
                worker: 0,
            },
            ClusterMutation::TaskCompleted { hash: "h1".into() },
            ClusterMutation::TaskFailed {
                hash: "h2".into(),
                kind: ErrorType::Recoverable,
                error: "boom".into(),
            },
        ];
        let mut once = ClusterState::<RunnerIdentifier>::new();
        for m in muts.iter().cloned() {
            once.apply(m);
        }
        let mut twice = ClusterState::<RunnerIdentifier>::new();
        for m in muts.iter().cloned() {
            twice.apply(m.clone());
            twice.apply(m);
        }
        assert_eq!(once.counts(), twice.counts());
        assert!(matches!(once.task_state("h1"), Some(TaskState::Completed { .. })));
        assert!(matches!(twice.task_state("h1"), Some(TaskState::Completed { .. })));
        // Failed got applied twice; second TaskFailed bumps attempts.
        match twice.task_state("h2") {
            Some(TaskState::Failed { attempts, .. }) => assert_eq!(*attempts, 2),
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[test]
    fn phase_deps_set_then_re_set_is_noop() {
        let mut s = ClusterState::<RunnerIdentifier>::new();
        let deps: HashMap<PhaseId, Vec<PhaseId>> =
            [(PhaseId::from("p1"), vec![PhaseId::from("p0")])]
                .into_iter()
                .collect();
        assert_eq!(
            s.apply(ClusterMutation::PhaseDepsSet { deps: deps.clone() }),
            ApplyOutcome::Applied
        );
        assert_eq!(s.phase_deps(), &deps);
        // Re-application is silent — the per-run graph is static.
        let other: HashMap<PhaseId, Vec<PhaseId>> = HashMap::new();
        assert_eq!(
            s.apply(ClusterMutation::PhaseDepsSet { deps: other }),
            ApplyOutcome::NoOp
        );
        assert_eq!(s.phase_deps(), &deps);
    }

    #[test]
    fn snapshot_round_trip_preserves_state() {
        let mut s = ClusterState::<RunnerIdentifier>::new();
        s.apply(ClusterMutation::TaskAdded {
            hash: "p".into(),
            task: mk_task("p"),
        });
        s.apply(ClusterMutation::TaskAdded {
            hash: "i".into(),
            task: mk_task("i"),
        });
        s.apply(ClusterMutation::TaskAssigned {
            hash: "i".into(),
            secondary: "s1".into(),
            worker: 7,
        });
        s.apply(ClusterMutation::TaskAdded {
            hash: "c".into(),
            task: mk_task("c"),
        });
        s.apply(ClusterMutation::TaskCompleted { hash: "c".into() });
        s.apply(ClusterMutation::PrimaryChanged {
            new: "s1".into(),
            epoch: 3,
        });
        let deps: HashMap<PhaseId, Vec<PhaseId>> =
            [(PhaseId::from("p1"), vec![PhaseId::from("p0")])]
                .into_iter()
                .collect();
        s.apply(ClusterMutation::PhaseDepsSet { deps: deps.clone() });

        let snap = s.snapshot();
        let mut joiner = ClusterState::<RunnerIdentifier>::new();
        joiner.restore(snap);

        assert_eq!(joiner.counts(), s.counts());
        assert_eq!(joiner.current_primary(), Some("s1"));
        assert_eq!(joiner.primary_epoch(), 3);
        assert_eq!(joiner.phase_deps(), &deps);
        assert!(matches!(
            joiner.task_state("p"),
            Some(TaskState::Pending { .. })
        ));
        assert!(matches!(
            joiner.task_state("i"),
            Some(TaskState::InFlight { .. })
        ));
        assert!(matches!(
            joiner.task_state("c"),
            Some(TaskState::Completed { .. })
        ));
    }

    /// Pins the Step 8 contract that `ClusterStateSnapshot` carries
    /// the replicated observer set so a late-joiner's first restore
    /// populates `RoleTable.observers` before any `PeerJoined`
    /// mutation arrives. Without this the joiner's election filter
    /// (`secondary::election::lowest_alive` skips observers) could
    /// fire against an empty set and promote an observer candidate
    /// in the gap between snapshot-restore and the next live broadcast.
    #[test]
    fn snapshot_round_trip_preserves_observers() {
        use std::collections::HashSet;
        let mut s = ClusterState::<RunnerIdentifier>::new();
        s.apply(ClusterMutation::PeerJoined {
            peer_id: "obs-1".into(),
            is_observer: true,
        });
        s.apply(ClusterMutation::PeerJoined {
            peer_id: "obs-2".into(),
            is_observer: true,
        });

        let snap = s.snapshot();
        let mut joiner = ClusterState::<RunnerIdentifier>::new();
        // Joiner is empty: snapshot's observers REPLACE the empty
        // local set per the first-bootstrap branch in `restore`.
        joiner.restore(snap);
        assert_eq!(
            joiner.role_table().observers,
            HashSet::from(["obs-1".to_string(), "obs-2".to_string()])
        );
    }

    /// Pins the Step 8 "first-bootstrap only" branch on `restore`:
    /// a joiner that has already observed a live `PeerJoined`
    /// broadcast (so `observers` is non-empty) keeps its local set
    /// rather than overwriting from a (possibly stale) snapshot.
    /// Mirrors the `phase_deps` "replaced if local empty, else kept"
    /// shape.
    #[test]
    fn restore_keeps_local_observers_when_already_populated() {
        use std::collections::HashSet;
        let mut joiner = ClusterState::<RunnerIdentifier>::new();
        joiner.apply(ClusterMutation::PeerJoined {
            peer_id: "live-obs".into(),
            is_observer: true,
        });

        let mut peer = ClusterState::<RunnerIdentifier>::new();
        peer.apply(ClusterMutation::PeerJoined {
            peer_id: "stale-obs".into(),
            is_observer: true,
        });

        joiner.restore(peer.snapshot());
        // Local set wins (live `PeerJoined` path is authoritative
        // once it has fired); snapshot's observers field is inert.
        assert_eq!(
            joiner.role_table().observers,
            HashSet::from(["live-obs".to_string()])
        );
    }

    #[test]
    fn restore_lattice_merge_preserves_local_terminal() {
        // Joiner has already observed TaskCompleted via a live broadcast
        // before the snapshot RPC response arrives. The snapshot's
        // weaker InFlight state must NOT override the local terminal.
        let mut joiner = ClusterState::<RunnerIdentifier>::new();
        joiner.apply(ClusterMutation::TaskAdded {
            hash: "h".into(),
            task: mk_task("h"),
        });
        joiner.apply(ClusterMutation::TaskCompleted { hash: "h".into() });

        let mut peer = ClusterState::<RunnerIdentifier>::new();
        peer.apply(ClusterMutation::TaskAdded {
            hash: "h".into(),
            task: mk_task("h"),
        });
        peer.apply(ClusterMutation::TaskAssigned {
            hash: "h".into(),
            secondary: "s".into(),
            worker: 0,
        });

        joiner.restore(peer.snapshot());
        assert!(matches!(
            joiner.task_state("h"),
            Some(TaskState::Completed { .. })
        ));
    }

    #[test]
    fn restore_lattice_merge_promotes_pending_to_in_flight() {
        // Joiner has only seen TaskAdded; snapshot has the InFlight
        // entry. The stronger lattice element (InFlight) wins.
        let mut joiner = ClusterState::<RunnerIdentifier>::new();
        joiner.apply(ClusterMutation::TaskAdded {
            hash: "h".into(),
            task: mk_task("h"),
        });

        let mut peer = ClusterState::<RunnerIdentifier>::new();
        peer.apply(ClusterMutation::TaskAdded {
            hash: "h".into(),
            task: mk_task("h"),
        });
        peer.apply(ClusterMutation::TaskAssigned {
            hash: "h".into(),
            secondary: "s".into(),
            worker: 4,
        });

        joiner.restore(peer.snapshot());
        match joiner.task_state("h") {
            Some(TaskState::InFlight { worker, .. }) => assert_eq!(*worker, 4),
            other => panic!("expected InFlight, got {other:?}"),
        }
    }

    #[test]
    fn restore_higher_epoch_wins_for_primary() {
        let mut joiner = ClusterState::<RunnerIdentifier>::new();
        joiner.apply(ClusterMutation::PrimaryChanged {
            new: "old".into(),
            epoch: 1,
        });
        let mut peer = ClusterState::<RunnerIdentifier>::new();
        peer.apply(ClusterMutation::PrimaryChanged {
            new: "new".into(),
            epoch: 5,
        });
        joiner.restore(peer.snapshot());
        assert_eq!(joiner.current_primary(), Some("new"));
        assert_eq!(joiner.primary_epoch(), 5);

        // Reverse direction: a stale snapshot must not regress epoch.
        let mut stale_peer = ClusterState::<RunnerIdentifier>::new();
        stale_peer.apply(ClusterMutation::PrimaryChanged {
            new: "ancient".into(),
            epoch: 2,
        });
        joiner.restore(stale_peer.snapshot());
        assert_eq!(joiner.current_primary(), Some("new"));
        assert_eq!(joiner.primary_epoch(), 5);
    }

    #[test]
    fn restore_idempotent_under_double_apply() {
        let mut peer = ClusterState::<RunnerIdentifier>::new();
        peer.apply(ClusterMutation::TaskAdded {
            hash: "h".into(),
            task: mk_task("h"),
        });
        peer.apply(ClusterMutation::TaskCompleted { hash: "h".into() });

        let snap = peer.snapshot();
        let mut joiner = ClusterState::<RunnerIdentifier>::new();
        joiner.restore(snap.clone());
        let counts_once = joiner.counts();
        joiner.restore(snap);
        assert_eq!(joiner.counts(), counts_once);
    }

    // ── RoleTable + role-change hook tests ──
    //
    // These pin the Step 2 contract: every `PrimaryChanged` that
    // returns `Applied` mutates the replicated `RoleTable` AND fires
    // every registered hook against the post-mutation table — never
    // the pre-mutation snapshot. NoOp paths (lower epoch, same value
    // at same epoch) must NOT fire hooks; otherwise a transport-side
    // cache could observe spurious updates on idempotent re-delivery.

    /// `PrimaryChanged` mutation updates the replicated `RoleTable`
    /// in lockstep with `current_primary`. Pins the cross-field
    /// invariant the transport-side cache will rely on.
    #[test]
    fn role_table_updates_on_primary_changed() {
        let mut s = ClusterState::<RunnerIdentifier>::new();
        assert_eq!(s.role_table().primary, None);

        s.apply(ClusterMutation::PrimaryChanged {
            new: "sec-2".into(),
            epoch: 1,
        });
        assert_eq!(s.role_table().primary, Some("sec-2".to_string()));

        // Higher epoch wins → table tracks the new holder.
        s.apply(ClusterMutation::PrimaryChanged {
            new: "sec-7".into(),
            epoch: 5,
        });
        assert_eq!(s.role_table().primary, Some("sec-7".to_string()));

        // Lower epoch is a NoOp and must NOT regress the table.
        s.apply(ClusterMutation::PrimaryChanged {
            new: "sec-stale".into(),
            epoch: 2,
        });
        assert_eq!(s.role_table().primary, Some("sec-7".to_string()));
    }

    /// Hook callbacks fire AFTER each `Applied` `PrimaryChanged`,
    /// observing the post-mutation `RoleTable`. NoOp applies (lower
    /// epoch / duplicate at same epoch) must NOT fire the hook —
    /// the transport cache would otherwise see spurious updates on
    /// idempotent re-delivery and could trigger needless cache
    /// invalidation downstream.
    #[test]
    fn role_change_hook_fires_after_apply() {
        use std::sync::Mutex;
        let mut s = ClusterState::<RunnerIdentifier>::new();
        let observed: Arc<Mutex<Vec<Option<String>>>> = Arc::new(Mutex::new(Vec::new()));
        {
            let observed = Arc::clone(&observed);
            s.register_role_change_hook(Box::new(move |table: &RoleTable| {
                observed.lock().unwrap().push(table.primary.clone());
            }));
        }

        s.apply(ClusterMutation::PrimaryChanged {
            new: "sec-a".into(),
            epoch: 1,
        });
        s.apply(ClusterMutation::PrimaryChanged {
            new: "sec-b".into(),
            epoch: 2,
        });
        s.apply(ClusterMutation::PrimaryChanged {
            new: "sec-c".into(),
            epoch: 3,
        });

        // Three Applied mutations → three callback fires, in order.
        let obs = observed.lock().unwrap().clone();
        assert_eq!(
            obs,
            vec![
                Some("sec-a".to_string()),
                Some("sec-b".to_string()),
                Some("sec-c".to_string())
            ],
        );

        // A NoOp re-delivery (same holder at same epoch) does NOT
        // fire the hook.
        s.apply(ClusterMutation::PrimaryChanged {
            new: "sec-c".into(),
            epoch: 3,
        });
        let obs_after_noop = observed.lock().unwrap().clone();
        assert_eq!(obs_after_noop.len(), 3, "NoOp must not fire hook");
    }

    /// `ClusterMutation::PeerJoined { is_observer: true }` inserts
    /// the peer into the replicated observer set with set semantics
    /// (idempotent) and fires role-change hooks when (and only when)
    /// the set actually changes. Pins the "observer-set replicated
    /// through RoleTable" contract that election filtering and
    /// PromotePrimary defense both rely on, now flowing through the
    /// single-writer CRDT apply path.
    #[test]
    fn peer_joined_observer_inserts_into_role_table_and_fires_hooks_on_change() {
        use std::collections::HashSet;
        use std::sync::Mutex;

        let mut s = ClusterState::<RunnerIdentifier>::new();
        assert!(s.role_table().observers.is_empty());

        let observed: Arc<Mutex<Vec<HashSet<String>>>> = Arc::new(Mutex::new(Vec::new()));
        {
            let observed = Arc::clone(&observed);
            s.register_role_change_hook(Box::new(move |table: &RoleTable| {
                observed.lock().unwrap().push(table.observers.clone());
            }));
        }

        // First insert fires the hook with the new set.
        assert_eq!(
            s.apply(ClusterMutation::PeerJoined {
                peer_id: "obs-1".into(),
                is_observer: true,
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            s.role_table().observers,
            HashSet::from(["obs-1".to_string()])
        );

        // Re-apply the same `PeerJoined { is_observer: true }`:
        // set-semantics NoOp, no hook fire.
        assert_eq!(
            s.apply(ClusterMutation::PeerJoined {
                peer_id: "obs-1".into(),
                is_observer: true,
            }),
            ApplyOutcome::NoOp
        );

        // Add a second observer: hook fires with the union.
        assert_eq!(
            s.apply(ClusterMutation::PeerJoined {
                peer_id: "obs-2".into(),
                is_observer: true,
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            s.role_table().observers,
            HashSet::from(["obs-1".to_string(), "obs-2".to_string()])
        );

        // Hook history: 2 actual changes (two distinct inserts);
        // the duplicate `PeerJoined` was a silent NoOp.
        let obs = observed.lock().unwrap().clone();
        assert_eq!(obs.len(), 2, "expected 2 fires, got {}", obs.len());
        assert_eq!(obs[0], HashSet::from(["obs-1".to_string()]));
        assert_eq!(
            obs[1],
            HashSet::from(["obs-1".to_string(), "obs-2".to_string()])
        );
    }

    /// `ClusterMutation::PeerJoined { is_observer: false }` for a peer
    /// already in `RoleTable.observers` MUST NOT regress the projection
    /// (only `PeerRemoved` may remove peers from the set). A first-seen
    /// non-observer peer is recorded in `peer_state` — that is the
    /// widened apply rule's tracking contract — but the observer set
    /// stays untouched. This pins the "stale flip-back does not regress
    /// the observer set" guarantee the receiver-side relies on.
    #[test]
    fn peer_joined_non_observer_does_not_remove_existing_observer() {
        let mut s = ClusterState::<RunnerIdentifier>::new();
        assert_eq!(
            s.apply(ClusterMutation::PeerJoined {
                peer_id: "obs-1".into(),
                is_observer: true,
            }),
            ApplyOutcome::Applied
        );
        assert!(s.role_table().observers.contains("obs-1"));

        // `is_observer: false` for an already-Alive observer is a
        // NoOp under the non-regression rule — neither peer_state nor
        // the observer projection mutate.
        assert_eq!(
            s.apply(ClusterMutation::PeerJoined {
                peer_id: "obs-1".into(),
                is_observer: false,
            }),
            ApplyOutcome::NoOp
        );
        assert!(
            s.role_table().observers.contains("obs-1"),
            "obs-1 must remain in role_table.observers (only PeerRemoved \
             removes peers from the projection)"
        );

        // A first-seen non-observer peer is now tracked in peer_state
        // (Applied), but does not enter the observer projection.
        assert_eq!(
            s.apply(ClusterMutation::PeerJoined {
                peer_id: "never-joined".into(),
                is_observer: false,
            }),
            ApplyOutcome::Applied
        );
        assert!(!s.role_table().observers.contains("never-joined"));
    }

    /// `restore` going through the snapshot-merge path also mutates
    /// the `RoleTable` AND fires hooks when `current_primary` flips.
    /// Pins the late-joiner / reconnect path; without this, a node
    /// that learns its first primary identity via snapshot RPC
    /// would leave the transport cache stuck at `None`.
    #[test]
    fn role_change_hook_fires_on_restore_when_primary_advances() {
        use std::sync::Mutex;
        let mut joiner = ClusterState::<RunnerIdentifier>::new();
        let observed: Arc<Mutex<Vec<Option<String>>>> = Arc::new(Mutex::new(Vec::new()));
        {
            let observed = Arc::clone(&observed);
            joiner.register_role_change_hook(Box::new(move |table: &RoleTable| {
                observed.lock().unwrap().push(table.primary.clone());
            }));
        }

        let mut peer = ClusterState::<RunnerIdentifier>::new();
        peer.apply(ClusterMutation::PrimaryChanged {
            new: "lead".into(),
            epoch: 7,
        });
        joiner.restore(peer.snapshot());

        assert_eq!(joiner.role_table().primary, Some("lead".to_string()));
        let obs = observed.lock().unwrap().clone();
        assert_eq!(obs, vec![Some("lead".to_string())]);
    }

    #[test]
    fn task_preferred_secondaries_updated_apply_writes_to_task() {
        let mut s = ClusterState::<RunnerIdentifier>::new();
        s.apply(ClusterMutation::TaskAdded {
            hash: "h".into(),
            task: mk_task("h"),
        });
        assert_eq!(
            s.apply(ClusterMutation::TaskPreferredSecondariesUpdated {
                hash: "h".into(),
                secondaries: vec!["secondary-2".into(), "secondary-5".into()],
            }),
            ApplyOutcome::Applied
        );
        let Some(TaskState::Pending { task }) = s.task_state("h") else {
            panic!("expected Pending");
        };
        assert_eq!(task.preferred_secondaries.as_slice(), &["secondary-2", "secondary-5"]);
    }

    #[test]
    fn task_preferred_secondaries_updated_apply_unknown_hash_is_noop() {
        let mut s = ClusterState::<RunnerIdentifier>::new();
        assert_eq!(
            s.apply(ClusterMutation::TaskPreferredSecondariesUpdated {
                hash: "nope".into(),
                secondaries: vec!["secondary-1".into()],
            }),
            ApplyOutcome::NoOp
        );
    }

    #[test]
    fn task_preferred_secondaries_updated_apply_preserves_state() {
        let mut s = ClusterState::<RunnerIdentifier>::new();
        s.apply(ClusterMutation::TaskAdded {
            hash: "h".into(),
            task: mk_task("h"),
        });
        s.apply(ClusterMutation::TaskFailed {
            hash: "h".into(),
            kind: ErrorType::Unfulfillable {
                reason: "missing".to_string().into(),
            },
            error: "missing".into(),
        });
        assert_eq!(
            s.apply(ClusterMutation::TaskPreferredSecondariesUpdated {
                hash: "h".into(),
                secondaries: vec!["secondary-7".into()],
            }),
            ApplyOutcome::Applied
        );
        let Some(TaskState::Unfulfillable { task, reason }) = s.task_state("h") else {
            panic!("state must stay Unfulfillable across preferred-secondaries update");
        };
        assert_eq!(reason, "missing");
        assert_eq!(task.preferred_secondaries.as_slice(), &["secondary-7"]);
    }

    // ── Discrete Unfulfillable / Blocked state pins ──

    /// `TaskFailed { kind: ErrorType::Unfulfillable, .. }` lands in the
    /// discrete `TaskState::Unfulfillable { reason, task }` variant,
    /// NOT in `TaskState::Failed { kind: Unfulfillable, .. }`. The
    /// `reason` field carries the inner `BoundedString` body verbatim
    /// (stored as `String` in the in-memory ledger).
    #[test]
    fn task_failed_with_unfulfillable_lands_in_unfulfillable_variant() {
        let mut s = ClusterState::<RunnerIdentifier>::new();
        s.apply(ClusterMutation::TaskAdded {
            hash: "h".into(),
            task: mk_task("h"),
        });
        assert_eq!(
            s.apply(ClusterMutation::TaskFailed {
                hash: "h".into(),
                kind: ErrorType::Unfulfillable {
                    reason: "missing toolchain xyz".to_string().into(),
                },
                error: "unfulfillable".into(),
            }),
            ApplyOutcome::Applied
        );
        match s.task_state("h") {
            Some(TaskState::Unfulfillable { reason, .. }) => {
                assert_eq!(reason, "missing toolchain xyz");
            }
            other => panic!("expected Unfulfillable, got {other:?}"),
        }
    }

    /// Regression pin for the dispatcher in the `TaskFailed` apply
    /// arm: generic non-recoverable errors must still land in
    /// `TaskState::Failed`, NOT in `Unfulfillable`. Pins that the
    /// kind-based routing only fires for `Unfulfillable` and every
    /// other `ErrorType` keeps the legacy shape.
    #[test]
    fn task_failed_with_generic_nonrecoverable_lands_in_failed_variant() {
        let mut s = ClusterState::<RunnerIdentifier>::new();
        s.apply(ClusterMutation::TaskAdded {
            hash: "h".into(),
            task: mk_task("h"),
        });
        s.apply(ClusterMutation::TaskFailed {
            hash: "h".into(),
            kind: ErrorType::NonRecoverable,
            error: "panic".into(),
        });
        assert!(matches!(
            s.task_state("h"),
            Some(TaskState::Failed { kind: ErrorType::NonRecoverable, .. })
        ));
        // And Recoverable also stays in Failed (sanity check the
        // dispatcher routes ONLY Unfulfillable to the new variant).
        let mut s2 = ClusterState::<RunnerIdentifier>::new();
        s2.apply(ClusterMutation::TaskAdded {
            hash: "h2".into(),
            task: mk_task("h2"),
        });
        s2.apply(ClusterMutation::TaskFailed {
            hash: "h2".into(),
            kind: ErrorType::Recoverable,
            error: "transient".into(),
        });
        assert!(matches!(
            s2.task_state("h2"),
            Some(TaskState::Failed { kind: ErrorType::Recoverable, .. })
        ));
    }

    /// `ClusterMutation::TaskBlocked { hash, on }` lands a `Pending`
    /// entry in `TaskState::Blocked { on, task }`. Pins the cascade
    /// broadcast shape: dependents of an Unfulfillable prereq mirror
    /// across every replica as Blocked (not Failed), carrying the
    /// prereq's hash so auto-resume can identify them.
    #[test]
    fn cascade_on_unfulfillable_marks_dependents_blocked() {
        let mut s = ClusterState::<RunnerIdentifier>::new();
        // Prereq enters Unfulfillable.
        s.apply(ClusterMutation::TaskAdded {
            hash: "prereq".into(),
            task: mk_task("prereq"),
        });
        s.apply(ClusterMutation::TaskFailed {
            hash: "prereq".into(),
            kind: ErrorType::Unfulfillable {
                reason: "missing".to_string().into(),
            },
            error: "missing".into(),
        });
        // Dependent enters Blocked-on-prereq via cascade broadcast.
        s.apply(ClusterMutation::TaskAdded {
            hash: "dep".into(),
            task: mk_task("dep"),
        });
        assert_eq!(
            s.apply(ClusterMutation::TaskBlocked {
                hash: "dep".into(),
                on: "prereq".into(),
            }),
            ApplyOutcome::Applied
        );
        match s.task_state("dep") {
            Some(TaskState::Blocked { on, .. }) => assert_eq!(on, "prereq"),
            other => panic!("expected Blocked, got {other:?}"),
        }
        // Re-apply against an already-Blocked entry with the same
        // `on` is a silent NoOp (idempotent under at-least-once
        // delivery).
        assert_eq!(
            s.apply(ClusterMutation::TaskBlocked {
                hash: "dep".into(),
                on: "prereq".into(),
            }),
            ApplyOutcome::NoOp
        );
    }

    // ── PeerRemoved + widened PeerJoined apply-rule tests ──
    //
    // These pin the peer-lifecycle contract on `ClusterState`:
    //
    //  1. `PeerRemoved` is sticky-per-id: once Dead, always Dead. A
    //     duplicate broadcast is a NoOp; a late `PeerJoined` for the
    //     same id is dropped with a warn log (no resurrection).
    //  2. The observer-set projection is maintained in lockstep with
    //     the `peer_state` map — removal of an observer drops them
    //     from `RoleTable.observers`.

    /// Local capture layer for warn-level tracing events. Scoped to
    /// the cluster_state test module — we only need it for the
    /// `peer_joined_dead_is_noop` warn-log assertion, so keep it
    /// module-private rather than lifting into a shared test util.
    struct WarnCapture {
        records: Arc<std::sync::Mutex<Vec<String>>>,
    }

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for WarnCapture {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            if *event.metadata().level() != tracing::Level::WARN {
                return;
            }
            if event.metadata().target() != "dynrunner_cluster_state" {
                return;
            }
            struct Visitor(String);
            impl tracing::field::Visit for Visitor {
                fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                    if field.name() == "message" {
                        self.0 = value.to_string();
                    }
                }
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    if field.name() == "message" {
                        self.0 = format!("{value:?}");
                    }
                }
            }
            let mut visitor = Visitor(String::new());
            event.record(&mut visitor);
            if let Ok(mut buf) = self.records.lock() {
                buf.push(visitor.0);
            }
        }
    }

    /// Run `body` against a scoped subscriber that captures every
    /// warn-level `dynrunner_cluster_state` event.
    fn with_warn_capture<F, R>(body: F) -> (R, Vec<String>)
    where
        F: FnOnce() -> R,
    {
        use tracing_subscriber::layer::SubscriberExt;
        let records: Arc<std::sync::Mutex<Vec<String>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let layer = WarnCapture {
            records: Arc::clone(&records),
        };
        let subscriber = tracing_subscriber::Registry::default().with(layer);
        let out = tracing::subscriber::with_default(subscriber, body);
        let captured = records.lock().unwrap().clone();
        (out, captured)
    }

    /// Idempotent removal: a second `PeerRemoved` for the same id is
    /// a silent NoOp under sticky-per-id semantics.
    #[test]
    fn peer_removed_is_sticky() {
        let mut s = ClusterState::<RunnerIdentifier>::new();
        assert_eq!(
            s.apply(ClusterMutation::PeerJoined {
                peer_id: "p1".into(),
                is_observer: false,
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            s.apply(ClusterMutation::PeerRemoved {
                id: "p1".into(),
                cause: RemovalCause::KeepaliveMiss,
            }),
            ApplyOutcome::Applied
        );
        // Re-applying PeerRemoved for the same id is a silent NoOp —
        // the entry is already Dead.
        assert_eq!(
            s.apply(ClusterMutation::PeerRemoved {
                id: "p1".into(),
                cause: RemovalCause::MassDeathEscalation,
            }),
            ApplyOutcome::NoOp
        );
    }

    /// `TaskCompleted` apply arm auto-resumes every Blocked dependent
    /// whose `on` matches the completing hash back to `Pending`.
    /// Event-driven: the same broadcast that converges the prereq to
    /// Completed converges every blocked dependent to Pending in one
    /// apply call across every replica.
    #[test]
    fn task_completed_auto_resumes_blocked_dependents() {
        let mut s = ClusterState::<RunnerIdentifier>::new();
        // Prereq landed Unfulfillable then was reinjected (Unfulfillable→Pending).
        s.apply(ClusterMutation::TaskAdded {
            hash: "prereq".into(),
            task: mk_task("prereq"),
        });
        s.apply(ClusterMutation::TaskFailed {
            hash: "prereq".into(),
            kind: ErrorType::Unfulfillable {
                reason: "missing".to_string().into(),
            },
            error: "missing".into(),
        });
        s.apply(ClusterMutation::TaskReinjected { hash: "prereq".into() });
        // Two dependents Blocked-on-prereq.
        for h in ["d1", "d2"] {
            s.apply(ClusterMutation::TaskAdded {
                hash: h.into(),
                task: mk_task(h),
            });
            s.apply(ClusterMutation::TaskBlocked {
                hash: h.into(),
                on: "prereq".into(),
            });
        }
        // An unrelated Blocked-on-other-prereq dependent must NOT auto-resume.
        s.apply(ClusterMutation::TaskAdded {
            hash: "unrelated".into(),
            task: mk_task("unrelated"),
        });
        s.apply(ClusterMutation::TaskBlocked {
            hash: "unrelated".into(),
            on: "some-other-prereq".into(),
        });
        // Prereq completes — every Blocked-on-prereq entry resumes.
        assert_eq!(
            s.apply(ClusterMutation::TaskCompleted { hash: "prereq".into() }),
            ApplyOutcome::Applied
        );
        assert!(matches!(
            s.task_state("d1"),
            Some(TaskState::Pending { .. })
        ));
        assert!(matches!(
            s.task_state("d2"),
            Some(TaskState::Pending { .. })
        ));
        // Unrelated stays Blocked — the auto-resume keys on the `on`
        // field, not blanket-resumes every Blocked entry.
        assert!(matches!(
            s.task_state("unrelated"),
            Some(TaskState::Blocked { .. })
        ));
    }

    /// `TaskReinjected` apply rule tightening: post-variant, only
    /// `TaskState::Unfulfillable { .. }` transitions to `Pending`.
    /// Other states (including the legacy `Failed { NonRecoverable, .. }`
    /// the pre-variant matcher accepted) are NoOp.
    #[test]
    fn reinject_task_command_filters_to_unfulfillable_only() {
        let mut s = ClusterState::<RunnerIdentifier>::new();
        // Unfulfillable → Pending: accepted.
        s.apply(ClusterMutation::TaskAdded {
            hash: "u".into(),
            task: mk_task("u"),
        });
        s.apply(ClusterMutation::TaskFailed {
            hash: "u".into(),
            kind: ErrorType::Unfulfillable {
                reason: "missing".to_string().into(),
            },
            error: "missing".into(),
        });
        assert_eq!(
            s.apply(ClusterMutation::TaskReinjected { hash: "u".into() }),
            ApplyOutcome::Applied
        );
        assert!(matches!(
            s.task_state("u"),
            Some(TaskState::Pending { .. })
        ));

        // Failed{NonRecoverable} → reinject: NoOp (pre-variant
        // matcher accepted this; the tightened rule rejects).
        s.apply(ClusterMutation::TaskAdded {
            hash: "f".into(),
            task: mk_task("f"),
        });
        s.apply(ClusterMutation::TaskFailed {
            hash: "f".into(),
            kind: ErrorType::NonRecoverable,
            error: "panic".into(),
        });
        assert_eq!(
            s.apply(ClusterMutation::TaskReinjected { hash: "f".into() }),
            ApplyOutcome::NoOp
        );
        assert!(matches!(
            s.task_state("f"),
            Some(TaskState::Failed { .. })
        ));
    }

    /// `ClusterStateSnapshot` round-trips the new Unfulfillable and
    /// Blocked variants without loss; the late-joiner / reconnect
    /// path observes the same state the originating replica recorded.
    #[test]
    fn pending_pool_unfulfillable_state_round_trips_via_snapshot() {
        let mut s = ClusterState::<RunnerIdentifier>::new();
        s.apply(ClusterMutation::TaskAdded {
            hash: "u".into(),
            task: mk_task("u"),
        });
        s.apply(ClusterMutation::TaskFailed {
            hash: "u".into(),
            kind: ErrorType::Unfulfillable {
                reason: "missing dep".to_string().into(),
            },
            error: "missing".into(),
        });
        s.apply(ClusterMutation::TaskAdded {
            hash: "b".into(),
            task: mk_task("b"),
        });
        s.apply(ClusterMutation::TaskBlocked {
            hash: "b".into(),
            on: "u".into(),
        });
        let snap = s.snapshot();
        let mut joiner = ClusterState::<RunnerIdentifier>::new();
        joiner.restore(snap);
        match joiner.task_state("u") {
            Some(TaskState::Unfulfillable { reason, .. }) => {
                assert_eq!(reason, "missing dep");
            }
            other => panic!("expected Unfulfillable, got {other:?}"),
        }
        match joiner.task_state("b") {
            Some(TaskState::Blocked { on, .. }) => assert_eq!(on, "u"),
            other => panic!("expected Blocked, got {other:?}"),
        }
    }

    /// Sticky-per-id under the cross-direction race: once a peer is
    /// Dead, a late `PeerJoined` for the same id is a NoOp and emits
    /// a warn log. Respawn requires a fresh id.
    #[test]
    fn peer_joined_dead_is_noop() {
        let ((), records) = with_warn_capture(|| {
            let mut s = ClusterState::<RunnerIdentifier>::new();
            s.apply(ClusterMutation::PeerJoined {
                peer_id: "p1".into(),
                is_observer: false,
            });
            s.apply(ClusterMutation::PeerRemoved {
                id: "p1".into(),
                cause: RemovalCause::KeepaliveMiss,
            });
            assert_eq!(
                s.apply(ClusterMutation::PeerJoined {
                    peer_id: "p1".into(),
                    is_observer: true,
                }),
                ApplyOutcome::NoOp,
                "PeerJoined for a Dead id must be NoOp"
            );
            assert!(
                !s.role_table().observers.contains("p1"),
                "Dead peer must not appear in the observer projection",
            );
        });
        assert!(
            records.iter().any(|m| m.contains("PeerJoined for dead id ignored")),
            "expected warn log on PeerJoined for dead id, captured: {records:?}",
        );
    }

    /// The widened `PeerJoined` apply rule preserves the observer-set
    /// extension semantics: a new observer peer enters the projection,
    /// re-application is silent, and a subsequent distinct observer
    /// extends the set.
    #[test]
    fn peer_joined_alive_extends_observer_set() {
        use std::collections::HashSet;
        let mut s = ClusterState::<RunnerIdentifier>::new();
        assert_eq!(
            s.apply(ClusterMutation::PeerJoined {
                peer_id: "obs-a".into(),
                is_observer: true,
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            s.apply(ClusterMutation::PeerJoined {
                peer_id: "obs-a".into(),
                is_observer: true,
            }),
            ApplyOutcome::NoOp,
            "re-applying the same PeerJoined is idempotent NoOp"
        );
        assert_eq!(
            s.apply(ClusterMutation::PeerJoined {
                peer_id: "obs-b".into(),
                is_observer: true,
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            s.role_table().observers,
            HashSet::from(["obs-a".to_string(), "obs-b".to_string()]),
        );
    }

    /// Removing an observer drops it from `RoleTable.observers` and
    /// fires role-change hooks against the post-mutation projection.
    #[test]
    fn peer_removed_observer_drops_from_role_table() {
        use std::sync::Mutex;
        let mut s = ClusterState::<RunnerIdentifier>::new();
        s.apply(ClusterMutation::PeerJoined {
            peer_id: "obs-1".into(),
            is_observer: true,
        });
        assert!(s.role_table().observers.contains("obs-1"));

        let observed: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
        {
            let observed = Arc::clone(&observed);
            s.register_role_change_hook(Box::new(move |table: &RoleTable| {
                observed.lock().unwrap().push(table.observers.len());
            }));
        }

        assert_eq!(
            s.apply(ClusterMutation::PeerRemoved {
                id: "obs-1".into(),
                cause: RemovalCause::KeepaliveMiss,
            }),
            ApplyOutcome::Applied
        );
        assert!(
            !s.role_table().observers.contains("obs-1"),
            "PeerRemoved on an observer must drop it from RoleTable.observers"
        );
        let hook_fires = observed.lock().unwrap().clone();
        assert_eq!(
            hook_fires,
            vec![0],
            "role-change hook must fire once with the shrunk set"
        );
    }

    /// End-to-end: a state-changing `PeerJoined` apply, with a
    /// dispatcher sender installed, MUST deliver a corresponding
    /// `PeerLifecycleEvent::Added` on the channel. This pins the
    /// "apply emits, dispatcher rx receives" contract — the
    /// boundary that replaces the prior stub `emit_lifecycle_event`.
    #[tokio::test]
    async fn apply_peer_joined_emits_event_through_dispatcher() {
        let mut s = ClusterState::<RunnerIdentifier>::new();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        s.install_lifecycle_sender(tx);

        assert_eq!(
            s.apply(ClusterMutation::PeerJoined {
                peer_id: "peer-x".into(),
                is_observer: false,
            }),
            ApplyOutcome::Applied
        );
        // The receiver MUST observe exactly one event with the
        // matching id / observer flag. `try_recv` confirms the
        // emit was non-blocking from the apply path's side.
        match rx.try_recv() {
            Ok(crate::peer_lifecycle::PeerLifecycleEvent::Added { id, is_observer }) => {
                assert_eq!(id, "peer-x");
                assert!(!is_observer);
            }
            other => panic!("expected Added event, got {other:?}"),
        }

        // Apply a removal as well to confirm the channel keeps
        // accepting subsequent events.
        assert_eq!(
            s.apply(ClusterMutation::PeerRemoved {
                id: "peer-x".into(),
                cause: RemovalCause::KeepaliveMiss,
            }),
            ApplyOutcome::Applied
        );
        match rx.try_recv() {
            Ok(crate::peer_lifecycle::PeerLifecycleEvent::Removed { id, cause }) => {
                assert_eq!(id, "peer-x");
                assert_eq!(cause, RemovalCause::KeepaliveMiss);
            }
            other => panic!("expected Removed event, got {other:?}"),
        }
    }

    // ── PeerResourceHoldingsUpdated apply-rule + snapshot tests ──

    /// First-time announce for an unseen peer inserts the holdings
    /// set into `peer_holdings`. The wire `Vec<String>` collects to
    /// a `HashSet<String>` so equality checks and dedup are
    /// set-based.
    #[test]
    fn peer_resource_holdings_updated_apply_inserts_holdings() {
        let mut s = ClusterState::<RunnerIdentifier>::new();
        assert!(s.peer_holdings().is_empty());
        assert_eq!(
            s.apply(ClusterMutation::PeerResourceHoldingsUpdated {
                peer_id: "peer-a".into(),
                holdings: vec!["res-1".into(), "res-2".into()],
                epoch: 0,
            }),
            ApplyOutcome::Applied
        );
        let stored = s.peer_holdings().get("peer-a").expect("entry present");
        assert_eq!(
            *stored,
            HashSet::from(["res-1".to_string(), "res-2".to_string()])
        );
    }

    /// An announce whose `epoch` is strictly older than the local
    /// `primary_epoch` is a NoOp — supersede-old-pending defends
    /// against a stale pre-failover announce overwriting holdings
    /// observed under the current primary. Equal-or-newer epoch
    /// applies; only `epoch < primary_epoch` is rejected.
    #[test]
    fn peer_resource_holdings_updated_stale_epoch_is_noop() {
        let mut s = ClusterState::<RunnerIdentifier>::new();
        // Advance primary_epoch to 5.
        s.apply(ClusterMutation::PrimaryChanged {
            new: "lead".into(),
            epoch: 5,
        });
        assert_eq!(s.primary_epoch(), 5);

        // epoch < primary_epoch → NoOp, ledger untouched.
        assert_eq!(
            s.apply(ClusterMutation::PeerResourceHoldingsUpdated {
                peer_id: "peer-a".into(),
                holdings: vec!["stale".into()],
                epoch: 4,
            }),
            ApplyOutcome::NoOp
        );
        assert!(s.peer_holdings().get("peer-a").is_none());

        // epoch == primary_epoch → Applied (same-epoch announces are
        // legitimate within the current primary's reign).
        assert_eq!(
            s.apply(ClusterMutation::PeerResourceHoldingsUpdated {
                peer_id: "peer-a".into(),
                holdings: vec!["fresh".into()],
                epoch: 5,
            }),
            ApplyOutcome::Applied
        );
        assert!(
            s.peer_holdings()
                .get("peer-a")
                .unwrap()
                .contains("fresh")
        );

        // epoch > primary_epoch → Applied (an announce from a peer
        // that already learned of a newer primary is still
        // authoritative about its own holdings).
        assert_eq!(
            s.apply(ClusterMutation::PeerResourceHoldingsUpdated {
                peer_id: "peer-b".into(),
                holdings: vec!["future".into()],
                epoch: 6,
            }),
            ApplyOutcome::Applied
        );
        assert!(
            s.peer_holdings()
                .get("peer-b")
                .unwrap()
                .contains("future")
        );
    }

    /// Re-application of a `PeerResourceHoldingsUpdated` whose
    /// `holdings` set (as collected to a HashSet) equals the
    /// already-stored set is a NoOp. Different ordering of the same
    /// strings on the wire is still equal under HashSet semantics —
    /// the apply rule does not depend on wire order.
    #[test]
    fn peer_resource_holdings_updated_same_set_is_noop() {
        let mut s = ClusterState::<RunnerIdentifier>::new();
        assert_eq!(
            s.apply(ClusterMutation::PeerResourceHoldingsUpdated {
                peer_id: "peer-a".into(),
                holdings: vec!["r1".into(), "r2".into()],
                epoch: 0,
            }),
            ApplyOutcome::Applied
        );
        // Same set, ordering swapped on the wire.
        assert_eq!(
            s.apply(ClusterMutation::PeerResourceHoldingsUpdated {
                peer_id: "peer-a".into(),
                holdings: vec!["r2".into(), "r1".into()],
                epoch: 0,
            }),
            ApplyOutcome::NoOp
        );
        // Duplicate string in incoming Vec collapses on collect; still
        // equal to the stored set → NoOp.
        assert_eq!(
            s.apply(ClusterMutation::PeerResourceHoldingsUpdated {
                peer_id: "peer-a".into(),
                holdings: vec!["r1".into(), "r2".into(), "r1".into()],
                epoch: 0,
            }),
            ApplyOutcome::NoOp
        );
        // A different set (superset) Applies.
        assert_eq!(
            s.apply(ClusterMutation::PeerResourceHoldingsUpdated {
                peer_id: "peer-a".into(),
                holdings: vec!["r1".into(), "r2".into(), "r3".into()],
                epoch: 0,
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            *s.peer_holdings().get("peer-a").unwrap(),
            HashSet::from(["r1".to_string(), "r2".to_string(), "r3".to_string()])
        );
        // A strictly smaller set also Applies (the announce is
        // authoritative for the announcing peer's current holdings;
        // shrinking is a real event when the peer evicts).
        assert_eq!(
            s.apply(ClusterMutation::PeerResourceHoldingsUpdated {
                peer_id: "peer-a".into(),
                holdings: vec!["r1".into()],
                epoch: 0,
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            *s.peer_holdings().get("peer-a").unwrap(),
            HashSet::from(["r1".to_string()])
        );
    }

    /// `ClusterStateSnapshot` round-trips the per-peer holdings map
    /// so a late-joiner sees current holdings before the next live
    /// `PeerResourceHoldingsUpdated` broadcast arrives. Pins the
    /// "snapshot carries replicated CRDT data" contract for the new
    /// field.
    #[test]
    fn peer_resource_holdings_snapshot_round_trip() {
        let mut s = ClusterState::<RunnerIdentifier>::new();
        s.apply(ClusterMutation::PeerResourceHoldingsUpdated {
            peer_id: "peer-a".into(),
            holdings: vec!["r1".into(), "r2".into()],
            epoch: 0,
        });
        s.apply(ClusterMutation::PeerResourceHoldingsUpdated {
            peer_id: "peer-b".into(),
            holdings: vec!["r3".into()],
            epoch: 0,
        });

        let snap = s.snapshot();
        let mut joiner = ClusterState::<RunnerIdentifier>::new();
        joiner.restore(snap);

        assert_eq!(
            *joiner.peer_holdings().get("peer-a").unwrap(),
            HashSet::from(["r1".to_string(), "r2".to_string()])
        );
        assert_eq!(
            *joiner.peer_holdings().get("peer-b").unwrap(),
            HashSet::from(["r3".to_string()])
        );
    }

    /// Pins the first-bootstrap-only contract on `restore`: a joiner
    /// that has already observed a live `PeerResourceHoldingsUpdated`
    /// broadcast (so `peer_holdings` is non-empty) keeps its local
    /// map rather than overwriting from a (possibly stale) snapshot.
    /// Mirrors the `observers` and `phase_deps` "replaced if local
    /// empty, else kept" shape.
    #[test]
    fn peer_resource_holdings_restore_keeps_local_when_non_empty() {
        let mut joiner = ClusterState::<RunnerIdentifier>::new();
        joiner.apply(ClusterMutation::PeerResourceHoldingsUpdated {
            peer_id: "live-peer".into(),
            holdings: vec!["live-res".into()],
            epoch: 0,
        });

        let mut peer = ClusterState::<RunnerIdentifier>::new();
        peer.apply(ClusterMutation::PeerResourceHoldingsUpdated {
            peer_id: "stale-peer".into(),
            holdings: vec!["stale-res".into()],
            epoch: 0,
        });

        joiner.restore(peer.snapshot());
        // Local map wins (live apply path is authoritative once it
        // has fired); snapshot's peer_holdings field is inert.
        assert!(joiner.peer_holdings().contains_key("live-peer"));
        assert!(!joiner.peer_holdings().contains_key("stale-peer"));
    }
}
