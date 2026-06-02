//! Replicated-ledger value types.
//!
//! Single concern: data shapes used by the `cluster_state` CRDT — the
//! per-task `TaskState` enum, the `ApplyOutcome` return marker, the
//! `StateCounts` / `OutcomeSummary` aggregate views, the `RoleChangeHook`
//! alias, and the module-private `PeerState` / `PeerEntry` liveness
//! ledger entry. The behavior (the `apply` rules, the snapshot/restore
//! merge, the event emit) lives in sibling sub-modules.

use std::sync::Arc;

use dynrunner_core::{ErrorType, TaskInfo, WorkerId};
use dynrunner_protocol_primary_secondary::RoleTable;
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
    /// The task hit `ErrorType::InvalidTask` — it is structurally
    /// invalid (e.g. a `task_depends_on` reference to a literally-
    /// absent id, or a duplicate `(phase_id, task_id)`), so it can
    /// never legitimately execute. Discrete variant (rather than
    /// `Failed { kind: InvalidTask, .. }`) for the same reason as
    /// `Unfulfillable`: downstream matcher / state-filter logic can
    /// dispatch on the discriminant. `reason` mirrors the
    /// `BoundedString<2048>` body from the wire mutation (stored
    /// here as `String`; the cap is the wire-codec's concern).
    ///
    /// **Terminal and NON-reinjectable** — this is the load-bearing
    /// divergence from `Unfulfillable`. An unfulfillable task awaits
    /// a cluster resource that may later appear, so `ReinjectTask`
    /// transitions it back to `Pending`; an invalid task is wrong by
    /// construction and no external action can make it valid, so the
    /// `ReinjectTask` gate rejects it and the terminal-lockout NoOp
    /// guards (the strongest-terminal arms in `TaskCompleted` /
    /// `TaskFailed` / `TaskBlocked`) refuse to overwrite it. Folded
    /// into `fail_final` by `outcome_counts` for operator-readable
    /// buckets, sibling to `Unfulfillable`.
    InvalidTask {
        task: TaskInfo<I>,
        reason: String,
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
    /// Tasks in `TaskState::InvalidTask { .. }` — terminal, non-
    /// reinjectable structural failures (missing dep / duplicate id).
    pub invalid_task: usize,
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
    /// Sum across all buckets — the total tasks that reached a
    /// terminal state (success or any failure). Distinct from
    /// `total_tasks` on the coordinator, which counts the input
    /// batch; `total_terminal()` reaches `total_tasks` exactly when
    /// the run is fully accounted for.
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
/// `pub(super)` so sibling sub-modules (`state.rs` referencing
/// `PeerEntry`, `apply_peer.rs` mutating it) can name it; external
/// callers of the `cluster_state` module never see it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum PeerState {
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
/// Internal — exposed nowhere outside `cluster_state`'s sub-modules.
/// `pub(super)` visibility so the `ClusterState` struct in
/// sibling `state.rs` can name the field type and `apply_peer.rs`
/// can read/write the fields; external callers never see it.
#[derive(Debug, Clone)]
pub(super) struct PeerEntry {
    pub(super) state: PeerState,
    /// Populated by a future `PeerInfo`-shaped mutation; today no
    /// mutation writes this and reads never fire. Kept as a stable
    /// field so the future wiring lands as an in-place update rather
    /// than a struct shape change.
    #[allow(dead_code)]
    pub(super) pubkey: Option<String>,
    /// See `pubkey` — same forward-looking scaffolding.
    #[allow(dead_code)]
    pub(super) endpoint: Option<String>,
    pub(super) is_observer: bool,
}
