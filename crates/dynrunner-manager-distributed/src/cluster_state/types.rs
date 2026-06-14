//! Replicated-ledger value types.
//!
//! Single concern: data shapes used by the `cluster_state` CRDT — the
//! per-task `TaskState` enum, the `ApplyOutcome` return marker, the
//! `StateCounts` / `OutcomeSummary` aggregate views, the `RoleChangeHook`
//! alias, and the module-private `PeerState` / `PeerEntry` liveness
//! ledger entry. The behavior (the `apply` rules, the snapshot/restore
//! merge, the event emit) lives in sibling sub-modules.

use std::sync::Arc;

use dynrunner_core::{ErrorType, TaskInfo, TaskVersion, WorkerId};
use dynrunner_protocol_primary_secondary::{RemovalCause, RoleTable};
use serde::{Deserialize, Serialize};

use crate::task_completed::TaskCompletedEvent;

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
#[serde(bound(serialize = "I: Serialize", deserialize = "I: for<'a> Deserialize<'a>",))]
pub enum TaskState<I> {
    Pending {
        task: TaskInfo<I>,
        /// Assignment-lifecycle version (C3). Carried so a reset's
        /// higher version beats a stale pre-reset `InFlight` within the
        /// non-terminal band even though `Pending`'s rank is lower.
        version: TaskVersion,
        /// Retry-attempt generation (F2). The TOP of [`TaskJoinKey`], above
        /// `band`: a retry reset mints `Pending { attempt: n+1 }` against a
        /// `Failed { attempt: n }`, so the reset out-ranks the prior Failed
        /// across EVERY merge path (apply, restore, anti-entropy) including
        /// the band boundary version cannot cross. Defaults 0 (the cold
        /// generation). See `TaskRetried`.
        attempt: u32,
    },
    InFlight {
        task: TaskInfo<I>,
        secondary: String,
        worker: WorkerId,
        /// Assignment-lifecycle version (C3). Set from the stamped
        /// `TaskAssigned` mutation; a genuine post-reset re-assignment
        /// mints a still-higher version so it beats the reset `Pending`.
        version: TaskVersion,
        /// Retry-attempt generation (F2). See `Pending::attempt`. Stamped
        /// from the `TaskAssigned` mutation's `attempt` (the choke point
        /// reads the task's current attempt), so a worker outcome for
        /// attempt n+1 out-ranks the reset `Pending { attempt: n+1 }` within
        /// that generation, and a stale attempt-n assignment LOSES.
        attempt: u32,
    },
    Completed {
        task: TaskInfo<I>,
        /// Retry-attempt generation (F2). See `Pending::attempt`. A
        /// completion preserves the attempt it completed under so a late
        /// stale attempt-n outcome cannot resurrect a higher-generation
        /// reset.
        attempt: u32,
    },
    Failed {
        task: TaskInfo<I>,
        kind: ErrorType,
        last_error: String,
        /// Terminal-payload version (D-V / AE-2). Two divergent failure
        /// records converge on the higher version; an equal-version
        /// divergence is settled by the payload content hash in the join
        /// key. Replaces the dropped `attempts` counter (which had no
        /// authoritative source and no reader).
        version: TaskVersion,
        /// Retry-attempt generation (F2). See `Pending::attempt`. The
        /// originator reads THIS to mint the `TaskRetried { attempt: n+1 }`
        /// reset that supersedes it.
        attempt: u32,
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
        /// The wire `error` message body (TS-4). Stored ALONGSIDE the
        /// typed `reason` so a restore-path emit's `last_error` is
        /// byte-identical to the apply-path emit's — the divergent
        /// `last_error` between the two paths (which split a consumer's
        /// dedup bucket) is killed at the source.
        last_error: String,
        /// Terminal-payload version (D-V / AE-2). See `Failed::version`.
        version: TaskVersion,
        /// Retry-attempt generation (F2). See `Pending::attempt`. Carried
        /// for uniformity (a reset never targets Unfulfillable — the F2-β
        /// gate is Failed-only — so the attempt is inert here, preserved
        /// across the Unfulfillable→reinject→Pending transition).
        attempt: u32,
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
        /// Retry-attempt generation (F2). See `Pending::attempt`. Carried
        /// for uniformity and preserved across the cascade-pause →
        /// auto-resume (`Blocked → Pending`) transition.
        attempt: u32,
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
        /// The wire `error` message body (TS-4). See `Unfulfillable::last_error`.
        last_error: String,
        /// Terminal-payload version (D-V / AE-2). See `Failed::version`.
        version: TaskVersion,
        /// Retry-attempt generation (F2). See `Pending::attempt`. Carried
        /// for uniformity; InvalidTask is terminal and non-reinjectable, so
        /// no reset ever bumps it (the InvalidTask-TOP invariant is
        /// preserved WITHIN an attempt by `TerminalRank`).
        attempt: u32,
    },
    /// The discovery originator determined the item's outputs already
    /// exist on the shared filesystem, so it was materialized DIRECTLY
    /// terminal (the `--skip-existing` "nothing left to do" case) and is
    /// never dispatched. SUCCESS-LIKE (it counts toward phase-done) but a
    /// DISTINCT accounting category — NOT folded into `Completed`, NOT into
    /// the `fail_*` buckets: `outcome_counts` ignores it and `counts()`
    /// tallies it in its OWN field. The item being a real ledger entry is
    /// the whole point — a phase whose items were all skipped now HAS tasks,
    /// so it is no longer "a phase without tasks that should error".
    ///
    /// A spawn-time terminal that never transitions / reinjects / re-fails,
    /// so it carries NO `version` and NO error payload. It is the WEAKEST
    /// terminal in [`TerminalRank`] — a skip never out-ranks a real outcome
    /// in a hypothetical hash collision. Carries `task` (uniform; the
    /// `task()`/`task_mut()`/`iter_*` accessors need it) and `attempt`
    /// (uniform with the F2 generation accessor). LOAD-BEARING: its
    /// `to_completed_event` is `None` so the skip stays silent on the
    /// completion channel.
    SkippedAlreadyDone { task: TaskInfo<I>, attempt: u32 },
    /// A `TaskKind::Setup` task that SUCCEEDED. Distinct terminal variant
    /// (rather than `Completed` carrying the kind) for the SAME reason as
    /// `SkippedAlreadyDone`: it is SUCCESS-LIKE — terminal, satisfies a
    /// dependent's `TaskDep`, counts toward phase completion — but a
    /// DISTINCT accounting category. It is NOT folded into `Completed` /
    /// `succeeded`: a succeeded setup task is counted in its OWN
    /// `setup_succeeded` bucket so the run-complete success line reports
    /// only worker WORK. Holding the kind on `Completed` instead would
    /// force `counts()` / `outcome_counts()` / the narrator to peek inside
    /// `task.kind` — scattered kind-special-casing the four-seam design
    /// forbids; a discrete variant lets every counter dispatch on the
    /// discriminant exactly as it already does for
    /// `SkippedAlreadyDone` / `Unfulfillable` / `InvalidTask`.
    ///
    /// A setup task is executed IN-PROCESS by its affinity member (never
    /// dispatched to a worker), so no worker outcome ever competes for its
    /// hash — like `SkippedAlreadyDone`, it carries NO `version` and NO
    /// error payload, and ranks as a spawn/in-process terminal in
    /// [`TerminalRank`]. The originating mutation (the in-process
    /// executor's success) is added by the executor phase; this variant +
    /// its terminal/dep/counter wiring is the primitive that consumes it.
    SetupCompleted { task: TaskInfo<I>, attempt: u32 },
    /// A `TaskKind::SecondaryAffine` gate `I` that became dependency-
    /// SATISFIED (all of ITS OWN deps resolved). READY-not-EXECUTED: it is
    /// terminal for DEPENDENCY-RESOLUTION + phase-completion purposes (so
    /// its dependents — the build tasks — unblock and the phase rolls up),
    /// but the primary NEVER executes it and it is NEVER counted in any
    /// outcome class (success / fail / setup_succeeded). The per-secondary
    /// IMPORT it represents runs once-per-secondary, locally, OFF the CRDT
    /// graph (Phase 4); this gate's only job is "are my dependents
    /// schedulable yet", answered yes the moment my own deps are done.
    ///
    /// Distinct terminal variant (rather than folding the kind onto
    /// `Completed` or reusing `SetupCompleted`) for the SAME reason
    /// `SkippedAlreadyDone` / `SetupCompleted` are distinct: every counter
    /// dispatches on the discriminant — `counts()` tallies it in its OWN
    /// inert `affine_ready` bucket, `outcome_counts()` keeps it OUT of
    /// `succeeded`/`fail_*`/`setup_succeeded` yet INCLUDED in
    /// `total_terminal()` (so a resolved gate is not mis-classified STRANDED
    /// at finalize), and the narrator never folds it into worker WORK. A
    /// gate is born `Pending` and transitions here authoritatively at the
    /// originator's read of "all deps resolved" — never worker-dispatched,
    /// so it carries NO `version` and NO error payload, and ranks as the
    /// weakest non-skip terminal in [`TerminalRank`]. LOAD-BEARING: its
    /// `to_completed_event` is `None` so the gate stays silent on the
    /// completion channel (it is neither a success nor a failure
    /// observation a downstream Policy could fold).
    AffineReady { task: TaskInfo<I>, attempt: u32 },
    /// A work task `B` assigned to secondary `secondary` and WAITING on that
    /// secondary's LOCAL `TaskKind::SecondaryAffine` import (#497). A
    /// NON-TERMINAL state — `B` will run on `secondary` once the local
    /// import finishes — that is CRDT-replicated + observable so primary and
    /// observer SEE `B` waiting on a local dep (not lost, not silently stuck
    /// mid-`InFlight`). Transition: `InFlight | Pending →
    /// QueuedAfterLocalDependency` (the secondary's
    /// `TaskQueuedAfterLocalDependency` report → the primary's
    /// `QueuedAfterLocalDependencySet`); → `InFlight` when the local import
    /// finishes (the secondary's `LocalDependencyReleased` report → the
    /// primary's EXISTING `TaskAssigned` originator).
    ///
    /// Discrete variant (rather than a flag on `InFlight`) so the observer
    /// stats arm and the death-seam recovery dispatch on the discriminant and
    /// read `secondary` directly, never parsing an inner field. It carries
    /// `version` and `attempt` (preserved from the source `InFlight`/`Pending`)
    /// so the join places it coherently in the non-terminal band
    /// ([`NonTerminalRank::QueuedAfterLocalDependency`], between `Pending`
    /// and `InFlight`) and the subsequent release `TaskAssigned` (minting a
    /// strictly-higher version) dominates it. On the holding secondary's
    /// death the death seam requeues it back to `Pending` (re-routable per
    /// #495), like `InFlight`.
    QueuedAfterLocalDependency {
        task: TaskInfo<I>,
        secondary: String,
        version: TaskVersion,
        attempt: u32,
    },
}

impl<I> TaskState<I> {
    /// Mutable borrow of the carried `TaskInfo`, regardless of variant.
    /// One canonical accessor so callers that need to mutate the task
    /// payload (e.g. the snapshot migration shim filling phase-less
    /// deps) do not each re-spell the all-variants match.
    pub(crate) fn task_mut(&mut self) -> &mut TaskInfo<I> {
        match self {
            TaskState::Pending { task, .. }
            | TaskState::InFlight { task, .. }
            | TaskState::Completed { task, .. }
            | TaskState::Failed { task, .. }
            | TaskState::Unfulfillable { task, .. }
            | TaskState::InvalidTask { task, .. }
            | TaskState::SkippedAlreadyDone { task, .. }
            | TaskState::SetupCompleted { task, .. }
            | TaskState::AffineReady { task, .. }
            | TaskState::QueuedAfterLocalDependency { task, .. }
            | TaskState::Blocked { task, .. } => task,
        }
    }

    /// Shared (`&self`) borrow of the carried `TaskInfo`, regardless of
    /// variant. The read-only twin of [`Self::task_mut`] — callers that
    /// need the `task_id` (e.g. the terminal-event projection) read it
    /// here without re-spelling the all-variants match.
    pub(crate) fn task(&self) -> &TaskInfo<I> {
        match self {
            TaskState::Pending { task, .. }
            | TaskState::InFlight { task, .. }
            | TaskState::Completed { task, .. }
            | TaskState::Failed { task, .. }
            | TaskState::Unfulfillable { task, .. }
            | TaskState::InvalidTask { task, .. }
            | TaskState::SkippedAlreadyDone { task, .. }
            | TaskState::SetupCompleted { task, .. }
            | TaskState::AffineReady { task, .. }
            | TaskState::QueuedAfterLocalDependency { task, .. }
            | TaskState::Blocked { task, .. } => task,
        }
    }

    /// The retry-attempt generation this state carries (F2). One canonical
    /// accessor so the version-stamp choke point (`broadcast.rs`) reads the
    /// task's CURRENT attempt without re-spelling the all-variants match —
    /// it stamps that attempt onto the copy-current `TaskAssigned` /
    /// `TaskCompleted` / `TaskFailed` candidate exactly as it stamps the
    /// version.
    pub(crate) fn attempt(&self) -> u32 {
        match self {
            TaskState::Pending { attempt, .. }
            | TaskState::InFlight { attempt, .. }
            | TaskState::Completed { attempt, .. }
            | TaskState::Failed { attempt, .. }
            | TaskState::Unfulfillable { attempt, .. }
            | TaskState::InvalidTask { attempt, .. }
            | TaskState::SkippedAlreadyDone { attempt, .. }
            | TaskState::SetupCompleted { attempt, .. }
            | TaskState::AffineReady { attempt, .. }
            | TaskState::QueuedAfterLocalDependency { attempt, .. }
            | TaskState::Blocked { attempt, .. } => *attempt,
        }
    }

    /// The retry-attempt generation iff this is the `Failed` state, else
    /// `None` (F2-β gate). The retry-bucket originator reads this to mint
    /// `TaskRetried { attempt: n+1 }` ONLY against a `Failed { attempt: n }`
    /// — a reset can never resurrect a `Completed` / `InvalidTask` /
    /// `Unfulfillable` / `InFlight` task, mirroring `apply_reinject_task`'s
    /// `Unfulfillable`-only gate. `None` here means "do not originate a
    /// reset for this hash".
    pub(crate) fn attempt_if_failed(&self) -> Option<u32> {
        match self {
            TaskState::Failed { attempt, .. } => Some(*attempt),
            _ => None,
        }
    }

    /// True iff this is a terminal state for dependency-resolution and
    /// phase-completion purposes. One canonical predicate so the CRDT
    /// phase-rollup derivation and the pyo3 stats projection share the
    /// permanent-failure set rather than each re-spelling the match: the
    /// pool resolves a dep once its prereq is `Completed` OR permanently
    /// failed, and in the CRDT the terminal set is `Completed` ∪ `Failed` ∪
    /// `Unfulfillable` ∪ `InvalidTask` ∪ `SkippedAlreadyDone`. `Blocked` is
    /// cascade-paused (auto-resumes to `Pending`), so it is NOT terminal. A
    /// `SkippedAlreadyDone` IS terminal — a dependent of a skipped task is
    /// unblocked exactly like a dependent of a completed one (the outputs
    /// the skip validated as already-present are what the dependent reads).
    /// `SetupCompleted` IS terminal for the same reason: a build task that
    /// gates on a setup task (`TaskDep`) unblocks the moment that setup
    /// task succeeds — overlapping, per the setup-task primitive's design.
    /// `AffineReady` IS terminal for dependency-resolution purposes for the
    /// same reason: a build task that gates on a SecondaryAffine gate
    /// unblocks the moment that gate becomes dependency-satisfied (the
    /// READY-not-EXECUTED resolution — the primary never runs the gate, but
    /// its dependents are schedulable). `QueuedAfterLocalDependency` is NOT
    /// terminal — `B` is an active assignment still pending execution on its
    /// secondary, exactly like `InFlight`.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TaskState::Completed { .. }
                | TaskState::Failed { .. }
                | TaskState::Unfulfillable { .. }
                | TaskState::InvalidTask { .. }
                | TaskState::SkippedAlreadyDone { .. }
                | TaskState::SetupCompleted { .. }
                | TaskState::AffineReady { .. }
        )
    }

    /// Project a terminal `TaskState` onto the dispatcher
    /// [`TaskCompletedEvent`]. The SINGLE event-shape projection both
    /// emit paths use (apply's monotone arms and restore's merge loop),
    /// so a success/failure event built from the POST-merge state is
    /// byte-identical regardless of which path produced it — killing the
    /// apply-emit-vs-restore-emit `last_error` divergence at the source.
    ///
    /// Returns `None` for a non-terminal state (no terminal event to
    /// emit). The `error_kind` strings mirror [`ErrorType::wire_value`]
    /// so consumer Policy bucketing (`starts_with("invalid_task:")`,
    /// `unfulfillable:` dedup) is byte-identical to the apply path.
    pub(crate) fn to_completed_event(&self, task_hash: &str) -> Option<TaskCompletedEvent> {
        let task_id = self.task().task_id.clone();
        match self {
            TaskState::Completed { .. } => Some(TaskCompletedEvent {
                task_id,
                task_hash: task_hash.to_string(),
                success: true,
                error_kind: None,
                last_error: None,
            }),
            TaskState::Failed {
                kind, last_error, ..
            } => Some(TaskCompletedEvent {
                task_id,
                task_hash: task_hash.to_string(),
                success: false,
                error_kind: Some(kind.wire_value()),
                last_error: Some(last_error.clone()),
            }),
            TaskState::Unfulfillable {
                reason, last_error, ..
            } => Some(TaskCompletedEvent {
                task_id,
                task_hash: task_hash.to_string(),
                success: false,
                error_kind: Some(format!("unfulfillable:{reason}")),
                last_error: Some(last_error.clone()),
            }),
            TaskState::InvalidTask {
                reason, last_error, ..
            } => Some(TaskCompletedEvent {
                task_id,
                task_hash: task_hash.to_string(),
                success: false,
                error_kind: Some(format!("invalid_task:{reason}")),
                last_error: Some(last_error.clone()),
            }),
            // A `SkippedAlreadyDone` is neither a success nor a failure
            // observation, so it fires NO `task_completed_listener` — it
            // stays silent on the completion channel (LOAD-BEARING: a skip
            // must not be counted as a completion by a downstream consumer
            // bucket). Grouped with the non-terminal `None` arms because it,
            // like them, projects to no terminal event.
            //
            // A `SetupCompleted` is also silent on the completion channel
            // for the SAME load-bearing reason: a succeeded setup task is
            // counted in its own `setup_succeeded` bucket, NEVER the success
            // bucket, so it must not surface as a `success: true` completion
            // a downstream consumer Policy could fold into its success
            // tally. Its dependents are unblocked through the cascade-resume
            // path (the terminal-resume seam), not this event projection.
            //
            // An `AffineReady` is silent for the STRONGEST form of this
            // reason: it is neither a success nor a failure — the primary
            // never executed the gate, it merely became dependency-
            // satisfied — so it is NEVER counted in any outcome class and
            // must surface no completion event. Its dependents unblock via
            // the cascade-resume path (the apply arm runs `resume_blocked_on`),
            // not this projection. A `QueuedAfterLocalDependency` is
            // non-terminal (an active assignment awaiting its secondary's
            // local import), so like `InFlight`/`Pending` it projects to no
            // terminal event.
            TaskState::Pending { .. }
            | TaskState::InFlight { .. }
            | TaskState::Blocked { .. }
            | TaskState::SkippedAlreadyDone { .. }
            | TaskState::SetupCompleted { .. }
            | TaskState::AffineReady { .. }
            | TaskState::QueuedAfterLocalDependency { .. } => None,
        }
    }
}

/// Which per-phase EVENT counter a [`ClusterState::phase_event_tally_for`]
/// key addresses (F4). The map keys `(PhaseId, PhaseTally)` mirror the
/// node-local event counters they replace (`phase_completed` /
/// `phase_failed`) — ONE replicated field, several keys, NOT one field per
/// counter and NOT a per-class breakdown.
///
/// EVENT-shaped, not terminal-shaped: a fail → reinject → succeed task
/// increments BOTH `Failed` and `Completed` (each terminal OBSERVATION is
/// one event), so this is a grow-only-MAX of a monotone event count — NOT a
/// projection of the single terminal `TaskState` the ledger converges to.
/// `PhaseRollup` / `outcome_counts` stay terminal-shaped (a distinct
/// concern).
///
/// Derives `Serialize`/`Deserialize` because it crosses the wire INSIDE the
/// snapshot's `(PhaseId, PhaseTally)` map key; `Copy`/`Eq`/`Hash` so it is a
/// cheap `HashMap` key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PhaseTally {
    Completed,
    Failed,
}

/// One accepted respawn event in the replicated respawn ledger (F7).
///
/// The replicated ledger is a grow-only SET `HashMap<String,
/// RespawnEventRecord>` keyed by `new_id` (the minted replacement
/// secondary id, globally unique per accepted event — `mint_secondary_id`
/// monotone), so the `new_id` is NOT carried in the value (it is the map
/// key). The record carries the chain root the budget family-walk needs
/// (`original_id`), the removal `cause` (operator forensics), and the
/// timestamp the cooldown check reads (`at`).
///
/// Replacing the node-local `VecDeque<RespawnEvent>` ring: the respawn
/// admission budget (`max_per_secondary` / `max_total` / `cooldown`) is a
/// failover decision input, so the ledger it consults must be replicated.
/// Grow-only SET (keyed insert, never remove, value written exactly once
/// per unique `new_id`), so it converges under union-by-key on `restore`
/// and rides the snapshot + anti-entropy digest path with ZERO wire
/// surface — the same channel `secondary_capacities` uses.
///
/// Derives `Serialize`/`Deserialize` because it crosses the wire as the
/// snapshot map VALUE; `Hash`/`Eq` so the digest can fold the
/// `(new_id, record)` PAIR (`RemovalCause` and `SystemTime` are both
/// `Hash`). The set is UNcapped — the total budget bounds its growth
/// (once `max_total` events land every further request is rejected, so the
/// set never exceeds `max_total + in-flight`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RespawnEventRecord {
    pub original_id: String,
    pub cause: RemovalCause,
    pub at: std::time::SystemTime,
}

/// The replicated respawn-policy CAPS (the `--respawn-policy=
/// on-secondary-death` knobs), set once per run by the submitter
/// primary via `ClusterMutation::RespawnPolicySet` in the same seed
/// batch as `PhaseDepsSet` (same run-constant lifecycle).
///
/// The sibling [`RespawnEventRecord`] ledger replicates the budget's
/// SPEND; this replicates the budget's CAPS — together they make the
/// respawn DECISION fully failover-portable: a promoted primary reads
/// `Some(policy)` at hydrate and re-arms the respawn pipeline (its
/// execution delegated over the mesh to the provider-host process).
/// `None` everywhere means the run launched with the policy disabled.
///
/// Merge: set-once / first-write-wins (the policy is run-constant) —
/// the apply rule NoOps a re-application and `restore` adopts the
/// snapshot value only when local is `None`, mirroring
/// `phase_may_be_empty`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicatedRespawnPolicy {
    pub max_per_secondary: u32,
    pub max_total: u32,
    /// Cooldown between accepted respawns of the same family, in
    /// milliseconds (the integer wire shape `RespawnPolicySet` carries).
    pub cooldown_ms: u64,
}

/// Per-message state in the replicated custom-message inbox (F5),
/// keyed by the per-origin `(origin, seq)` idempotency pair.
///
/// A sticky lattice `Unhandled ⊑ {Handled, Failed}` (the implicit
/// BOTTOM — "unposted" — is map ABSENCE; the `DiscoveryDebt`
/// precedent's bottom erased to absence because, unlike
/// `DiscoveryDebt`, the key space is unbounded and an explicit bottom
/// would never be stored). BOTH terminals are LATCHES that win
/// regardless of arrival order — each terminal's apply rule inserts it
/// directly into an absent slot so a late `Posted` NoOps. The two
/// terminals are siblings, not ordered by the lattice; their
/// theoretical join is deterministic Handled-wins (`Failed → Handled`
/// Applied, `Handled → Failed` NoOp), documented-but-never-exercised:
/// the primary originates exactly ONE terminal per message. (Under
/// watermark compaction the Handled/Failed label is erased entirely —
/// a compacted key reads as "terminal" — so a label-divergent replica
/// pair still converges physically: same pruned entries, same
/// watermark.) The payload lives ONLY on `Unhandled`; either terminal
/// transition DROPS it (tombstone, a few bytes), and the per-origin
/// contiguous-prefix watermark (`custom_terminal_watermarks`)
/// physically prunes terminal tombstones of both kinds (the GC story —
/// the ≤100 KB bodies never accumulate).
///
/// Derives `Serialize`/`Deserialize` because it crosses the wire as the
/// snapshot map VALUE; `Hash`/`Eq` so the digest can fold the
/// `((origin, seq), state)` PAIR.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CustomMsgState {
    /// Posted at the authority but not yet consumed by a
    /// `custom_message_handler` — the promoted-primary hydrate replays
    /// every entry in this state (and ONLY this state).
    Unhandled { topic: String, data: Vec<u8> },
    /// Consumed by a clean handler return — payload dropped; sticky.
    Handled,
    /// The handler RAISED — a terminal USER ERROR, never retried, the
    /// handler's partial effect discarded; payload dropped; sticky. A
    /// promoted primary never re-dispatches a `Failed` entry.
    Failed,
}

/// Coarse convergence band. The band dominates FIRST in the
/// [`TaskJoinKey`] ordering, so any terminal beats any non-terminal
/// regardless of version (C3 req-a: a worker outcome that raced a reset
/// is never resurrected to an assignment), and `Blocked` sits between
/// (it carries the cascade-prereq identity and must not be overwritten
/// by a stale `Pending` observation).
///
/// Discriminant values encode the order (`NonTerminal < Blocked <
/// Terminal`); derived `Ord` follows the discriminants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum JoinBand {
    NonTerminal = 0,
    Blocked = 1,
    Terminal = 2,
}

/// Within the `Terminal` band: `SkippedAlreadyDone < SetupCompleted <
/// AffineReady < {Failed, Unfulfillable} < Completed < InvalidTask` (D-T —
/// InvalidTask is the unique TOP among WORK terminals). `SkippedAlreadyDone`
/// is the WEAKEST terminal so a skip never out-ranks a real outcome in a
/// hypothetical hash collision (a real
/// Completed/Failed/Unfulfillable/InvalidTask for the same hash always
/// wins the join over the spawn-time skip). `SetupCompleted` is the next-
/// weakest, and like the skip it is a NON-COMPETING terminal: a setup-kind
/// task's hash is only ever originated terminal by its in-process executor
/// (never worker-dispatched), so its rank never decides a real collision —
/// it sits low purely for a total, deterministic order. `AffineReady` is a
/// NON-COMPETING terminal for the SAME reason: a SecondaryAffine gate's
/// hash is only ever originated terminal by the primary's ready-resolution
/// hook (it is NEVER worker-dispatched and never re-failed), so no real
/// worker outcome competes for it — it ranks just above `SetupCompleted`
/// purely for a total, deterministic order. `FailedLike` covers `Failed |
/// Unfulfillable`; they tie-break below by a fixed `failedlike`
/// discriminant then the payload content hash, but only when both are
/// `FailedLike` at equal version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum TerminalRank {
    SkippedAlreadyDone = 0,
    SetupCompleted = 1,
    AffineReady = 2,
    FailedLike = 3,
    Completed = 4,
    InvalidTask = 5,
}

/// Within the `NonTerminal` band, the rank sub-key (`Pending <
/// QueuedAfterLocalDependency < InFlight`), consulted ONLY as the last
/// tiebreak at EQUAL version. `QueuedAfterLocalDependency` sits between
/// `Pending` and `InFlight`: a work task queued behind a secondary's local
/// import is MORE committed than a bare `Pending` (it is assigned to a
/// specific secondary) but LESS than a running `InFlight`, so the release
/// `TaskAssigned` (minting a strictly-higher version) cleanly dominates it
/// and a stale re-delivery never resurrects an `InFlight` over it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum NonTerminalRank {
    Pending = 0,
    QueuedAfterLocalDependency = 1,
    InFlight = 2,
}

/// Within `TerminalRank::FailedLike` at EQUAL version, a fixed total
/// order between `Failed` and `Unfulfillable` so two DIFFERENT
/// non-success terminals converge deterministically. `Unfulfillable` is
/// the more-specific, reinjectable verdict; `Failed` is the generic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum FailedLikeRank {
    Failed = 0,
    Unfulfillable = 1,
}

/// The ONE canonical per-task convergence key. Replaces BOTH the two
/// hand-rolled per-state rank fns that previously lived in `snapshot.rs`
/// and `digest.rs` (the duplicated logic). It is a
/// TOTAL comparator tuple compared lexicographically: the per-task
/// `attempt` generation (F2) dominates FIRST (above `band`), then the
/// [`JoinBand`], then — within the non-terminal band — `version`
/// arbitrates BEFORE rank (C3), and within the terminal band the
/// [`TerminalRank`] separates the D-T order before `version` and the
/// payload hash. Constructed by `task_join_key` (logic in `merge.rs`);
/// the comparison logic lives there too, so apply, restore, and the
/// digest fold all derive their order from this single key.
///
/// `attempt` at the top is the F2 retry-reset survival mechanism: a
/// `TaskRetried` mints `Pending { attempt: n+1 }` against a `Failed {
/// attempt: n }`, and because `attempt` dominates `band`, the attempt-
/// (n+1) `Pending` out-ranks the attempt-n `Failed` across EVERY merge
/// path — including restore / anti-entropy, where `version` (a per-band
/// arbiter) cannot cross the band boundary. The InvalidTask-TOP invariant
/// (D-T) is preserved WITHIN an attempt; a reset never targets InvalidTask
/// (the originator's `Failed`-only gate), so no attempt-bump races it.
///
/// The fields are ordered so that `#[derive(Ord)]`'s field-by-field
/// lexicographic comparison IS the convergence order. For the
/// non-terminal band the discriminating fields are `(attempt, band,
/// version, nonterminal_rank)`; for terminals `(attempt, band,
/// terminal_rank, version, failedlike, payload_content_hash)`. The unused
/// sub-keys for a given band are filled with their minimum so they never
/// perturb the order (a band's own discriminator already separates it).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct TaskJoinKey {
    /// Retry-attempt generation (F2) — dominates FIRST, above `band`, so a
    /// retry reset's higher-attempt `Pending` out-ranks the prior-attempt
    /// `Failed` across the band boundary and survives anti-entropy.
    pub(super) attempt: u32,
    /// Coarse band — dominates after `attempt`.
    pub(super) band: JoinBand,
    /// Terminal ordering (D-T). Minimum (`FailedLike`) for non-terminals
    /// and `Blocked`, where it is inert (the band already separates them).
    pub(super) terminal_rank: TerminalRank,
    /// The version: assignment version for non-terminals; terminal-payload
    /// version for the three versioned terminals; default `(0,0)` for
    /// `Completed`/`Blocked` (which carry no version — their band/terminal
    /// rank already places them). For non-terminals this is compared
    /// BEFORE `nonterminal_rank`, which is exactly the C3 ordering.
    pub(super) version: TaskVersion,
    /// Non-terminal rank (`Pending < InFlight`), the LAST tiebreak within
    /// the non-terminal band (consulted only at equal version). Minimum
    /// (`Pending`) for terminals/Blocked where it is inert.
    pub(super) nonterminal_rank: NonTerminalRank,
    /// FailedLike tiebreak (`Failed < Unfulfillable`) at equal terminal
    /// version. Minimum (`Failed`) outside the FailedLike sub-band.
    pub(super) failedlike: FailedLikeRank,
    /// Content hash of the terminal payload (`(kind/discriminant,
    /// error/reason, last_error)`), NON-OPTIONAL for terminals so two
    /// divergent failure records at equal `(terminal_rank, version)`
    /// compare/fold differently (C4). `0` for non-terminals.
    pub(super) payload_content_hash: u64,
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
    /// Tasks in `TaskState::SkippedAlreadyDone { .. }` — discovery-time
    /// skips whose outputs already existed. SUCCESS-LIKE terminal kept in
    /// its OWN category (NOT folded into `completed` nor any `fail_*`).
    pub skipped_already_done: usize,
    /// Tasks in `TaskState::SetupCompleted { .. }` — succeeded setup-kind
    /// tasks. SUCCESS-LIKE terminal kept in its OWN category (NOT folded
    /// into `completed` nor any `fail_*`), so the worker-work `completed`
    /// count reports only worker work.
    pub setup_succeeded: usize,
    /// Tasks in `TaskState::AffineReady { .. }` — SecondaryAffine gates that
    /// became dependency-satisfied (READY-not-EXECUTED). An INERT terminal
    /// kept in its OWN category: the primary never executed the gate, so it
    /// is NOT folded into `completed`, `setup_succeeded`, nor any `fail_*` —
    /// it is a schedulability gate only. It IS a terminal ledger entry, so
    /// `total_terminal()` counts it (no STRANDED false-abort at finalize).
    pub affine_ready: usize,
    /// Tasks in `TaskState::QueuedAfterLocalDependency { .. }` — work tasks
    /// assigned to a secondary but waiting on that secondary's local
    /// SecondaryAffine import. A NON-TERMINAL live state kept in its OWN
    /// category (NOT folded into `in_flight`): the task is committed to a
    /// secondary but not yet running, an honest live-work count distinct
    /// from a running `InFlight`.
    pub queued_after_local_dependency: usize,
}

/// Per-phase task partition over the replicated ledger — the value shape
/// of [`ClusterState::phase_task_partition`], the SINGLE owner of the
/// "what is each of this phase's tasks, operationally" classification.
/// Four mutually-exclusive buckets covering every `TaskState` variant:
///
///   - `to_run`: still-live work — `Pending` / `InFlight` / `Blocked`
///     (`Blocked` is cascade-paused and auto-resumes, so it is honest
///     remaining work, mirroring [`TaskState::is_terminal`]).
///   - `done`: `Completed` — work this run actually performed.
///   - `failed`: the non-success terminals — `Failed` / `Unfulfillable` /
///     `InvalidTask` (the same fold `OutcomeSummary` applies to its
///     `fail_*` classes, collapsed to one operator-readable number).
///   - `skipped`: `SkippedAlreadyDone` — the discovery-time skip kept in
///     its OWN bucket, exactly as `counts()` / `outcome_counts()` keep it.
///
/// `to_run + done + failed + skipped` is the phase's total ledger entries.
/// At a phase's spawn edge `done == failed == 0`, so `to_run` there equals
/// the old "every non-skipped entry" reading — but mid-run the partition
/// keeps telling the truth (a completed task is `done`, never `to_run`).
///
/// `Add` so a reader aggregating across phases (the narrator's running
/// "overall" line) sums partitions without re-spelling the bucket shape.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PhaseTaskPartition {
    pub to_run: usize,
    pub done: usize,
    pub failed: usize,
    pub skipped: usize,
}

impl std::ops::Add for PhaseTaskPartition {
    type Output = Self;
    fn add(self, rhs: Self) -> Self {
        Self {
            to_run: self.to_run + rhs.to_run,
            done: self.done + rhs.done,
            failed: self.failed + rhs.failed,
            skipped: self.skipped + rhs.skipped,
        }
    }
}

/// Per-phase derived view used by every reader that needs the phase
/// state machine recomputed from the CRDT rather than from the
/// primary-only `PendingPool`. Pure projection of the replicated
/// `tasks` ledger + static `phase_deps`; carries no authority, no pool,
/// no wall-clock — exactly the inputs a zero-authority observer holds.
///
/// Single source for the "is this phase started / done / dispatchable"
/// rule: both the operator run-narrator (`crate::run_narrator`) and the
/// pyo3 stats snapshot (`StatsSnapshot::from_cluster_state`) read this
/// off [`ClusterState::phase_rollups`] instead of each re-deriving the
/// terminal-set + dispatchability walk.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PhaseRollup {
    /// The phase has ≥1 task entry in the ledger (in any state).
    pub has_any: bool,
    /// The phase has ≥1 NON-terminal task. Terminal == `Completed |
    /// Failed | Unfulfillable | InvalidTask`; `Pending` / `InFlight` /
    /// `Blocked` are live. A phase with `has_any && !has_live` has fully
    /// terminated (every task reached a terminal state). A phase with no
    /// entries reads `has_live == false` (vacuously satisfied, so a
    /// dependent of an empty upstream phase is never wedged).
    pub has_live: bool,
    /// Every phase this phase (transitively) depends on has fully
    /// terminated (`!has_live`), so this phase is eligible to dispatch.
    /// Mirrors the pool's activation cascade.
    pub dispatchable: bool,
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
    /// Discovery-time `SkippedAlreadyDone` terminals — a SUCCESS-LIKE
    /// terminal kept in its OWN bucket: NOT folded into `succeeded` (the
    /// run-complete summary / narrator success count must report only
    /// work this run actually performed) and NOT any failure class. It IS
    /// a terminal, fully-accounted outcome, so [`Self::total_terminal`]
    /// includes it — without that, every skip would be mis-classified as
    /// STRANDED by the finalize accounting (`stranded = total -
    /// total_terminal()`) and a clean skip-bearing run would false-abort
    /// as `ClusterCollapsed`.
    pub skipped: usize,
    /// Succeeded `TaskState::SetupCompleted` terminals — a SUCCESS-LIKE
    /// terminal kept in its OWN bucket: NOT folded into `succeeded` (the
    /// run-complete summary / narrator success count must report only
    /// worker WORK this run performed) and NOT any failure class. It IS a
    /// terminal, fully-accounted outcome, so [`Self::total_terminal`]
    /// includes it — sibling to `skipped`, for the same finalize-accounting
    /// reason (a setup task left out of `total_terminal` would be
    /// mis-classified as STRANDED).
    pub setup_succeeded: usize,
    /// `TaskState::AffineReady` terminals — SecondaryAffine gates that
    /// became dependency-satisfied (READY-not-EXECUTED). An INERT terminal
    /// in its OWN bucket: NEVER folded into `succeeded` / `setup_succeeded`
    /// / any failure class (the primary never executed the gate — it is a
    /// schedulability gate, not work). It IS a terminal, fully-accounted
    /// ledger outcome, so [`Self::total_terminal`] includes it — sibling to
    /// `skipped`/`setup_succeeded`, for the same finalize-accounting reason
    /// (a gate left out of `total_terminal` would be mis-classified STRANDED
    /// and false-abort a clean gate-bearing run as `ClusterCollapsed`).
    pub affine_ready: usize,
}

impl OutcomeSummary {
    /// Sum across all buckets — the total tasks that reached a
    /// terminal state (success, any failure, a discovery-time skip, or a
    /// succeeded setup task). Distinct from `total_tasks` on the
    /// coordinator, which counts the input batch; `total_terminal()`
    /// reaches `total_tasks` exactly when the run is fully accounted for.
    pub fn total_terminal(&self) -> usize {
        self.succeeded
            + self.fail_retry
            + self.fail_oom
            + self.fail_final
            + self.skipped
            + self.setup_succeeded
            + self.affine_ready
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

/// Liveness bit on a `PeerEntry`. `Dead` is sticky-per-GENERATION: once
/// a peer is `Dead` at generation N, no `PeerJoined`/`PeerRemoved`
/// mutation for the same id at a generation `<= N` may mutate the entry
/// (re-application is silent) — the original sticky-per-id rule, scoped
/// to one membership incarnation. A `PeerJoined` at generation `N+1`
/// re-admits the id (the primary's frame-ingest re-admission seam is the
/// sole originator of that bump). Respawn still mints a fresh id.
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

/// The replicated-membership view of one peer id, projected for
/// diagnostics consumers (the egress no-route message split). Distinct
/// from the transport `MembershipView` (the wire view): a peer can be a
/// LIVE replicated member while this node has no transport wire to it —
/// the two states an honest "no route" line must distinguish.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerMembership {
    /// `peer_state[id]` is `Alive` — a live replicated member.
    AliveMember,
    /// `peer_state[id]` is `Dead` — authoritatively removed
    /// (`PeerRemoved` ledger) and not (yet) re-admitted.
    RemovedMember,
    /// No `peer_state` entry — the id never joined the replicated
    /// membership (or this node has not yet observed its join).
    NeverJoined,
}

/// The re-admission ticket [`ClusterState::removed_peer_readmission`]
/// returns for a REMOVED peer: the generation a re-admitting
/// `PeerJoined` must carry (`dead generation + 1`) plus the
/// advertisement preserved on the capability `Departed` tombstone, so
/// the primary's frame-ingest re-admission seam restores the exact
/// capability the member departed with.
///
/// [`ClusterState::removed_peer_readmission`]: super::ClusterState::removed_peer_readmission
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerReadmission {
    /// The generation the re-admitting `PeerJoined` carries — strictly
    /// above the `Dead` entry's, so the apply rule's sticky gate opens.
    pub member_gen: u64,
    /// The `is_observer` bit preserved at departure.
    pub is_observer: bool,
    /// The `can_be_primary` bit preserved at departure.
    pub can_be_primary: bool,
}

/// One entry in `ClusterState::peer_state`. Holds ONLY the liveness bit
/// plus scaffolding metadata that future mutations (`PeerInfo`/`PeerCert`
/// broadcasts) will populate — today `pubkey`/`endpoint` start `None`
/// and stay `None` because no mutation writes them yet.
///
/// Liveness ONLY (C6): the role-capability bits (`is_observer`,
/// `can_be_primary`) used to live here, duplicating the `RoleTable`
/// projection (CRD-4: two sources of truth). They now live in EXACTLY
/// ONE replicated place — the `capabilities` 2P-set on `ClusterState` —
/// and the `RoleTable.observers` / `RoleTable.can_be_primary` sets are
/// read-time projections of `capability × this map's Alive bit`
/// (`reproject_roles`). This entry composes with the capability set at
/// projection time; it is never MERGED with it. Liveness is node-local
/// and honest (never resurrected by anti-entropy); capability converges
/// as a proper CRDT.
///
/// Internal — exposed nowhere outside `cluster_state`'s sub-modules.
/// `pub(super)` visibility so the `ClusterState` struct in
/// sibling `state.rs` can name the field type and `apply_peer.rs`
/// can read/write the fields; external callers never see it.
#[derive(Debug, Clone)]
pub(super) struct PeerEntry {
    pub(super) state: PeerState,
    /// Membership-incarnation generation (the re-admission lattice).
    /// `0` is the cold first join. Advanced ONLY by a generation-
    /// advancing `PeerJoined` (the primary's re-admission seam) or a
    /// `PeerRemoved` for a generation this node has not yet seen join
    /// (reorder tolerance). Both apply rules gate on it: a mutation at
    /// a generation strictly below the entry's is stale and NoOps.
    pub(super) member_gen: u64,
    /// Populated by a future `PeerInfo`-shaped mutation; today no
    /// mutation writes this and reads never fire. Kept as a stable
    /// field so the future wiring lands as an in-place update rather
    /// than a struct shape change.
    #[allow(dead_code)]
    pub(super) pubkey: Option<String>,
    /// See `pubkey` — same forward-looking scaffolding.
    #[allow(dead_code)]
    pub(super) endpoint: Option<String>,
}

/// One entry in `ClusterState::capabilities` — the replicated 2P-set
/// that is the SINGLE source of truth for a peer's role capabilities
/// (`is_observer` / `can_be_primary`), decoupled from liveness (C6).
///
/// Two-phase-set semantics: `Advertised` grows on a join / capability
/// advertisement; `Departed` is the tombstone written on a genuine
/// departure (`PeerRemoved`) and DOMINATES any `Advertised`. The merge
/// (`merge_capability`, `merge.rs`) is commutative/associative/idempotent:
///   * `Departed ∨ _ = Departed`;
///   * `Advertised ∨ Advertised = Advertised { is_observer: a || b,
///     can_be_primary: <bit of the higher cap_version>,
///     cap_version: max(...) }`.
///
/// `is_observer` is a pure upward ratchet (OR — an observer never
/// un-observes mid-run); `can_be_primary` follows the higher `cap_version`
/// so a later `SetCanBePrimary(false)` wins over an earlier `true`.
///
/// `Serialize`/`Deserialize` so the map round-trips through the snapshot
/// (the 2P-set IS snapshot-healable — a divergence the digest flags is
/// one a snapshot pull's `restore` actually heals; see `merge_capability`
/// + the digest `capabilities_hash`).
///
/// `pub` (unlike the module-private `PeerEntry`/`PeerState`) because it
/// crosses the WIRE inside the `pub` `ClusterStateSnapshot` — the same
/// category as `SecondaryCapacityRecord`. Liveness (`PeerEntry`) projects
/// to a bare `HashSet<String>` on the wire and stays private; the
/// capability 2P-set carries structured per-id state, so its value type is
/// part of the snapshot's public serialization contract. The variants are
/// only CONSTRUCTED inside `cluster_state`; external callers round-trip the
/// snapshot opaquely (decode + `restore`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CapabilityEntry {
    Advertised {
        is_observer: bool,
        can_be_primary: bool,
        cap_version: TaskVersion,
        /// Membership-incarnation generation (the re-admission lattice).
        /// Dominates FIRST in `merge_capability`: a re-admitted member's
        /// `Advertised` at gen N+1 beats the gen-N `Departed` tombstone
        /// on EVERY merge path (apply, snapshot restore, digest heal),
        /// so a stale replica's snapshot can never re-bury a re-admitted
        /// peer's capability. `#[serde(default)]` decodes a pre-field
        /// snapshot entry to generation 0.
        #[serde(default)]
        member_gen: u64,
    },
    /// 2P-set tombstone (genuine departure). Dominates any `Advertised`
    /// OF THE SAME GENERATION; a strictly-higher-generation `Advertised`
    /// (a re-admission) supersedes it. Preserves the advertisement that
    /// was current at departure so the primary's re-admission seam can
    /// restore the EXACT capability without guessing (the removed node
    /// never re-advertises — it does not know it was removed).
    Departed {
        #[serde(default)]
        member_gen: u64,
        /// The `is_observer` bit current at departure.
        #[serde(default)]
        is_observer: bool,
        /// The `can_be_primary` bit current at departure.
        #[serde(default)]
        can_be_primary: bool,
        /// The capability version current at departure (tie-breaks two
        /// divergent same-generation tombstones deterministically).
        #[serde(default)]
        cap_version: TaskVersion,
    },
}
