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
    pub items: VecDeque<TaskInfo<I>>,
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
    UnknownTaskDep {
        task: String,
        referenced_by: String,
    },
    /// A `task_depends_on` graph cycle was detected on extend. The
    /// `Vec` is a deterministic walk of the offending cycle (smallest
    /// task_id first, then DFS).
    #[error("task dependency cycle: {0:?}")]
    TaskDepCycle(Vec<String>),
}
