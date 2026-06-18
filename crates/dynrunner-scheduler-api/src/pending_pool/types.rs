//! Pure data types and helpers for [`super::PendingPool`].
//!
//! This module owns:
//! * [`PhaseState`] — the phase lifecycle enum.
//! * [`Bucket`] — one `(phase, type, affinity)` FIFO with its
//!   pinned-workers list.
//! * [`BucketKey`] / [`PreferencePredicate`] — type aliases.
//! * [`PendingPoolError`] — error variants surfaced by `new`/`extend`.
//! * Affinity sentinel helpers `no_affinity` / `affinity_key`.
//!
//! It owns NO behaviour beyond `Bucket::new`; every operational concern
//! lives in a sibling submodule of `pending_pool/`.

use std::collections::VecDeque;
use std::sync::Arc;

use dynrunner_core::{AffinityId, Identifier, PhaseId, TaskInfo, TypeId, WorkerId};

/// Sentinel affinity id for items that have no pinning preference.
///
/// `TaskInfo::affinity_id` is `Option<AffinityId>`; the pool keys its
/// buckets on the non-optional `AffinityId`, mapping `None` to this
/// empty-string sentinel so the free pool is just another bucket
/// rather than a special case.
pub(super) fn no_affinity() -> AffinityId {
    AffinityId::from("")
}

/// Effective affinity for a task: `affinity_id` if `Some`, else the sentinel.
pub(super) fn affinity_key<I>(item: &TaskInfo<I>) -> AffinityId {
    item.affinity_id.clone().unwrap_or_else(no_affinity)
}

/// Composite bucket key.
pub type BucketKey = (PhaseId, TypeId, AffinityId);

/// The would-be dispatch standing of the work task(s) a setup (upload)
/// task gates — the ordering key the primary uses to route the upload
/// whose dependents we most want to start next FIRST (instead of FIFO).
///
/// Lower sorts BETTER (sooner). The three components are compared
/// lexicographically by the derived `Ord`, so the ranking is exactly:
/// dispatchable-now dependents beat will-activate-later ones; within a
/// tier a typed (pinned) dependent beats a free-pool one; and a tie is
/// broken toward the upload with MORE dependents (a `group_common` file
/// shared by many builds beats an equal-tier single-dependent `delta`).
///
/// The field semantics MIRROR the dispatch view (`view_for_worker`) so a
/// setup task derives its priority from the SAME phase-state + affinity
/// classification real worker dispatch uses — no soft-pin logic is
/// duplicated here; this is purely an `Ord` projection of those reads.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct DispatchRank {
    /// 0 = a dependent's phase is `Active` (dispatchable right now),
    /// 1 = `Draining`, 2 = any non-dispatchable state the dependent will
    /// still reach dispatch from (`Blocked` will-activate). Mirrors the
    /// `Active`/`Draining` gate the dispatch view applies for its classes.
    pub phase_tier: u8,
    /// 0 = a typed (non-free-pool) dependent, 1 = a free-pool dependent —
    /// mirrors `view_for_worker`'s class ordering (typed class < free-pool
    /// class) via the `no_affinity` sentinel.
    pub class_tier: u8,
    /// Negated dependent count so that, with all else equal, MORE
    /// dependents sorts BETTER under the plain `Ord` (a shared upload
    /// pulls ahead of a single-dependent one).
    pub neg_dependent_count: i32,
}

impl DispatchRank {
    /// All-max sentinel: sorts LAST. Used for a setup task with no known
    /// dependent yet (its work task has not spawned) so a discovered-
    /// dependent upload always routes ahead of it — it is never starved,
    /// only deferred until a dependent appears.
    pub const WORST: DispatchRank = DispatchRank {
        phase_tier: u8::MAX,
        class_tier: u8::MAX,
        neg_dependent_count: i32::MAX,
    };
}

/// Caller-supplied preference predicate for [`super::PendingPool::view_for_worker`].
///
/// The view's emission order is fixed at four priority classes (pin,
/// typed, free-pool, co-pin) — this predicate orders items *within*
/// each class only. Items mapped to `Ordering::Less` come first inside
/// their class, then `Equal`, then `Greater`; equal-ordering items keep
/// their construction-time FIFO order because the sort is stable. The
/// predicate is never invoked to compare items from different classes.
pub type PreferencePredicate<'a, I> = dyn Fn(&TaskInfo<I>) -> std::cmp::Ordering + 'a;

/// Phase lifecycle. Transitions are monotonic in this order
/// (with the one exception that `requeue` can flip
/// `Draining → Active` when an item comes back into the queue).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhaseState {
    /// One or more `depends_on` phases haven't reached `Done`.
    Blocked,
    /// Items can be dispatched.
    Active,
    /// Pool empty for this phase, but in-flight items haven't all returned.
    Draining,
    /// Pool empty AND in-flight count zero. Awaiting `on_phase_end`.
    Drained,
    /// `on_phase_end` returned. Dependents may activate.
    Done,
}

/// One `(phase, type, affinity)` bucket: a FIFO of queued items plus
/// the workers currently pinned to it. Soft pin: a pinned worker
/// prefers this bucket but never refuses other work.
#[derive(Debug)]
pub(crate) struct Bucket<I: Identifier> {
    pub items: VecDeque<Arc<TaskInfo<I>>>,
    pub pinned_workers: Vec<WorkerId>,
}

impl<I: Identifier> Bucket<I> {
    pub(super) fn new() -> Self {
        Self {
            items: VecDeque::new(),
            pinned_workers: Vec::new(),
        }
    }
}

/// DIAGNOSTIC (throwaway): a per-phase, per-term snapshot of every input
/// [`super::PendingPool::phases_stuck_drainable`] evaluates for ONE phase
/// that is not yet [`PhaseState::Done`]. Pure read-only data — the pool
/// composes it from the SAME private gate accessors the drain transition
/// uses (`queued_count`, `in_flight`, `live_blocked_count`,
/// `predecessors_done`, `phase_has_live_affine_prereq`), so the manager
/// can LOG the exact vetoing term without learning any pool internals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DrainEligibilityRow {
    pub phase_id: PhaseId,
    pub phase_state: PhaseState,
    pub queued_count: usize,
    pub in_flight: u32,
    pub live_blocked_count: usize,
    pub predecessors_done: bool,
    /// Each declared predecessor `(phase_id, its current PhaseState)`, so a
    /// reader sees WHICH predecessor (if any) is not yet `Done`.
    pub predecessors: Vec<(PhaseId, PhaseState)>,
    pub phase_has_live_affine_prereq: bool,
    /// The FIRST live affine token found across ALL buckets (not just this
    /// phase's) — `(token_task_id, token_bucket_phase_id, in_completed,
    /// in_failed)`. Exposes whether the held token is an own-phase barrier
    /// or evidence of cross-phase leakage, and whether the pool mirror lost
    /// its terminal. `None` when no live affine token exists anywhere.
    pub first_live_affine_token: Option<DrainAffineToken>,
    /// Whether this phase is in
    /// [`super::PendingPool::phases_stuck_drainable`] (the resurface arm's
    /// input). The final included/excluded verdict.
    pub stuck_drainable: bool,
}

/// DIAGNOSTIC: the identity of one live affine ledger token, surfaced by
/// [`DrainEligibilityRow::first_live_affine_token`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DrainAffineToken {
    pub task_id: String,
    pub bucket_phase_id: PhaseId,
    pub in_completed: bool,
    pub in_failed: bool,
}

/// Errors produced by `PendingPool::new` (phase-graph validation) and
/// `PendingPool::extend` (per-task dependency validation).
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PendingPoolError {
    #[error("phase dependency cycle detected starting at {0}")]
    DependencyCycle(PhaseId),
    #[error("phase {0} declared as a dependency but not in the phase set")]
    UnknownDependency(PhaseId),
    /// Two `TaskInfo`s share the same `task_id`. Both already-known
    /// (in pool / completed / failed) and within-batch collisions
    /// surface as this variant.
    #[error("duplicate task_id `{0}` in pool")]
    DuplicateTaskId(String),
    /// A task's `task_depends_on` references an id that does not match
    /// any existing, queued, blocked, completed, or failed task.
    #[error("task `{referenced_by}` depends on unknown task `{task}`")]
    UnknownTaskDep { task: String, referenced_by: String },
    /// A `task_depends_on` graph cycle was detected on extend. The
    /// `Vec` is a deterministic walk of the offending cycle (smallest
    /// task_id first, then DFS).
    #[error("task dependency cycle: {0:?}")]
    TaskDepCycle(Vec<String>),
}
