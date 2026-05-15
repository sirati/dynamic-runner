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

use std::collections::HashMap;
use std::sync::Arc;

use dynrunner_core::{ErrorType, Identifier, PhaseId, TaskInfo, WorkerId};
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, RoleChangeHookRegistrar, RoleTable,
};
use serde::{Deserialize, Serialize};

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

/// The replicated cluster-state CRDT.
pub struct ClusterState<I> {
    tasks: HashMap<String, TaskState<I>>,
    current_primary: Option<String>,
    primary_epoch: u64,
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
            phase_deps: self.phase_deps.clone(),
            run_complete: self.run_complete,
            role_table: self.role_table.clone(),
            // Deliberately not cloned — see field doc.
            role_change_hooks: Vec::new(),
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
            .finish()
    }
}

impl<I> Default for ClusterState<I> {
    fn default() -> Self {
        Self {
            tasks: HashMap::new(),
            current_primary: None,
            primary_epoch: 0,
            phase_deps: HashMap::new(),
            run_complete: false,
            role_table: RoleTable::default(),
            role_change_hooks: Vec::new(),
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
///   live mutation path (PeerInfo broadcasts → `set_observers`)
///   replaces the set atomically, so a snapshot is authoritative for
///   the late-joiner's first-bootstrap and inert thereafter. A
///   broader merge rule (union, or epoch-tagged replace) would be
///   over-engineering today — the live PeerInfo broadcast that
///   arrives shortly after the snapshot supersedes the snapshot's
///   `observers` anyway. `#[serde(default)]` keeps wire compat with
///   pre-Step-8 senders (snapshots from a peer running an older
///   crate omit the field; deserialize defaults to an empty set,
///   identical to the pre-Step-8 shape).
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
    pub observers: std::collections::HashSet<String>,
}

fn task_state_rank<I>(s: &TaskState<I>) -> u8 {
    match s {
        TaskState::Pending { .. } => 0,
        TaskState::InFlight { .. } => 1,
        TaskState::Completed { .. } | TaskState::Failed { .. } => 2,
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
                    ErrorType::ResourceExhausted(_) | ErrorType::NonRecoverable => {
                        o.fail_final += 1
                    }
                },
                TaskState::Pending { .. } | TaskState::InFlight { .. } => {}
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
                | TaskState::Failed { task, .. } => task,
            };
            (h, t)
        })
    }

    /// Iterator over `(task_hash, &TaskInfo)` for terminal entries
    /// (`Completed` or `Failed`).
    pub fn iter_terminal(&self) -> impl Iterator<Item = (&String, &TaskInfo<I>)> {
        self.tasks.iter().filter_map(|(h, s)| match s {
            TaskState::Completed { task } | TaskState::Failed { task, .. } => Some((h, task)),
            _ => None,
        })
    }

    pub fn current_primary(&self) -> Option<&str> {
        self.current_primary.as_deref()
    }

    pub fn primary_epoch(&self) -> u64 {
        self.primary_epoch
    }

    pub fn phase_deps(&self) -> &HashMap<PhaseId, Vec<PhaseId>> {
        &self.phase_deps
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

    /// Replace the replicated `observers` set with the given peers.
    /// Fires registered role-change hooks if the set actually changed
    /// so the transport-layer cache stays coherent (Step 7+ of the
    /// transport-unification refactor; Decision G option α).
    ///
    /// Observer-membership rides the `PeerInfo` wire frame, not a
    /// dedicated `ClusterMutation` — the receiver's PeerInfo handler
    /// (`secondary/setup.rs`) calls this method when it processes
    /// the incoming peers vector. The set is `Replace`-shaped (not
    /// incremental insert/remove) so a re-broadcast that drops a
    /// peer correctly removes it from the local view; observers
    /// joining mid-run still appear because PeerInfo is broadcast
    /// every time the primary's peer set changes.
    ///
    /// Idempotent: no-op + no hook fire when the new set equals the
    /// current set. Hooks fire AFTER the field update so registrants
    /// see the post-mutation table.
    pub fn set_observers(&mut self, observers: std::collections::HashSet<String>) {
        if self.role_table.observers == observers {
            return;
        }
        self.role_table.observers = observers;
        self.fire_role_change_hooks();
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
            // before the next PeerInfo broadcast arrives. The set is
            // the same one `set_observers` writes; the snapshot is
            // authoritative for first-bootstrap and inert thereafter.
            observers: self.role_table.observers.clone(),
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
        // case), otherwise keep local. The live PeerInfo broadcast
        // path (`set_observers`) is the steady-state writer; this
        // branch only fires on the late-joiner's very first restore,
        // before any PeerInfo arrives. Firing the role-change hooks
        // when the set actually changes keeps the transport's
        // write-through cache coherent on the snapshot path the same
        // way `set_observers` does on the live path.
        if self.role_table.observers.is_empty() && !snap.observers.is_empty() {
            self.role_table.observers = snap.observers;
            self.fire_role_change_hooks();
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
                    TaskState::Failed { task, .. } => task.clone(),
                    TaskState::Pending { task } | TaskState::InFlight { task, .. } => task.clone(),
                };
                *state = TaskState::Completed { task };
                ApplyOutcome::Applied
            }
            ClusterMutation::TaskFailed { hash, kind, error } => {
                let Some(state) = self.tasks.get_mut(&hash) else {
                    return ApplyOutcome::NoOp;
                };
                match state {
                    TaskState::Completed { .. } => ApplyOutcome::NoOp,
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
                    TaskState::Pending { task } | TaskState::InFlight { task, .. } => {
                        let task = task.clone();
                        *state = TaskState::Failed {
                            task,
                            kind,
                            last_error: error,
                            attempts: 1,
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
    use dynrunner_core::{PhaseId, RunnerIdentifier, TypeId};
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
    /// populates `RoleTable.observers` before the next PeerInfo
    /// broadcast arrives. Without this the joiner's election filter
    /// (`secondary::election::lowest_alive` skips observers) could
    /// fire against an empty set and promote an observer candidate
    /// in the gap between snapshot-restore and the next PeerInfo.
    #[test]
    fn snapshot_round_trip_preserves_observers() {
        use std::collections::HashSet;
        let mut s = ClusterState::<RunnerIdentifier>::new();
        s.set_observers(HashSet::from([
            "obs-1".to_string(),
            "obs-2".to_string(),
        ]));

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
    /// a joiner that has already observed a live PeerInfo broadcast
    /// (so `observers` is non-empty) keeps its local set rather than
    /// overwriting from a (possibly stale) snapshot. Mirrors the
    /// `phase_deps` "replaced if local empty, else kept" shape.
    #[test]
    fn restore_keeps_local_observers_when_already_populated() {
        use std::collections::HashSet;
        let mut joiner = ClusterState::<RunnerIdentifier>::new();
        joiner.set_observers(HashSet::from(["live-obs".to_string()]));

        let mut peer = ClusterState::<RunnerIdentifier>::new();
        peer.set_observers(HashSet::from(["stale-obs".to_string()]));

        joiner.restore(peer.snapshot());
        // Local set wins (live PeerInfo path is authoritative once it
        // has fired); snapshot's observers field is inert.
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

    /// Step 7 (Decision G): `set_observers` replaces the replicated
    /// observer set and fires role-change hooks when (and only when)
    /// the set actually changes. Idempotent re-application is silent.
    /// Pins the "observer-set replicated through RoleTable" contract
    /// that election filtering and PromotePrimary defense both rely on.
    #[test]
    fn set_observers_updates_role_table_and_fires_hooks_on_change() {
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
        s.set_observers(HashSet::from(["obs-1".to_string()]));
        assert_eq!(
            s.role_table().observers,
            HashSet::from(["obs-1".to_string()])
        );

        // Re-apply with the same set: idempotent, no hook fire.
        s.set_observers(HashSet::from(["obs-1".to_string()]));

        // Add a second observer: hook fires with the union.
        s.set_observers(HashSet::from(["obs-1".to_string(), "obs-2".to_string()]));
        assert_eq!(
            s.role_table().observers,
            HashSet::from(["obs-1".to_string(), "obs-2".to_string()])
        );

        // Remove obs-1 (Replace-shape): hook fires with the
        // contracted set. Pins the "PeerInfo broadcast that drops
        // a peer correctly removes it" path.
        s.set_observers(HashSet::from(["obs-2".to_string()]));
        assert_eq!(
            s.role_table().observers,
            HashSet::from(["obs-2".to_string()])
        );

        // Clear: hook fires with empty set.
        s.set_observers(HashSet::new());
        assert!(s.role_table().observers.is_empty());

        // Hook history: 4 actual changes (insert, union, contract,
        // clear); the no-op re-apply was silent.
        let obs = observed.lock().unwrap().clone();
        assert_eq!(obs.len(), 4, "expected 4 fires, got {}", obs.len());
        assert_eq!(obs[0], HashSet::from(["obs-1".to_string()]));
        assert_eq!(
            obs[1],
            HashSet::from(["obs-1".to_string(), "obs-2".to_string()])
        );
        assert_eq!(obs[2], HashSet::from(["obs-2".to_string()]));
        assert!(obs[3].is_empty());
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
}
