//! [`WorkerView`] — affinity-ordered snapshot of one worker's eligible
//! items, suitable as input to a `Scheduler::assign_normal` call.
//!
//! Built by `PendingPool::view_for_worker` and consumed by
//! `PendingPool::take_from_view` (both in the `dispatch` submodule).
//! This file owns the value type and its caller-side combinators
//! (`filter`, `sort_by_key`); construction and consumption live in
//! `dispatch.rs` where the bucketed state lives.
//!
//! The struct fields are `pub(super)` so sibling submodules can pair
//! `items` with `locators` directly. The crate-external API surface
//! exposes only the accessor methods on this impl.

use dynrunner_core::{Identifier, TaskInfo, WorkerId};

use super::types::BucketKey;

/// Affinity-ordered snapshot of a worker's eligible items, suitable as
/// input to a `Scheduler::assign_normal` call.
///
/// Built by [`super::PendingPool::view_for_worker`]; consumed by
/// [`super::PendingPool::take_from_view`]. The `items` slice exposes cloned
/// `TaskInfo<I>` values so the scheduler does not borrow the pool. The
/// internal `locators` vector preserves `(bucket_key, index)` pointers
/// so `take_from_view` can remove the chosen item from its actual
/// bucket.
#[derive(Debug)]
pub struct WorkerView<I: Identifier> {
    pub(super) items: Vec<TaskInfo<I>>,
    pub(super) locators: Vec<(BucketKey, usize)>,
    pub(super) worker_id: WorkerId,
}

impl<I: Identifier> WorkerView<I> {
    /// The affinity-ordered slice of cloned candidate items.
    /// Indexed positionally by `take_from_view`.
    pub fn as_slice(&self) -> &[TaskInfo<I>] {
        &self.items
    }

    /// Number of candidate items in the view.
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// `true` iff the view has no candidate items.
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// The worker the view was built for.
    pub fn worker_id(&self) -> WorkerId {
        self.worker_id
    }

    /// Drop items the predicate returns `false` for, returning a new
    /// view that keeps the original locators paired with the kept
    /// items. Use to apply caller-side constraints (per-type
    /// concurrency caps, per-resource budgets, etc.) without teaching
    /// the scheduler about them.
    pub fn filter<F: FnMut(&TaskInfo<I>) -> bool>(self, mut pred: F) -> Self {
        let WorkerView {
            items,
            locators,
            worker_id,
        } = self;
        let mut kept_items = Vec::with_capacity(items.len());
        let mut kept_locators = Vec::with_capacity(locators.len());
        for (item, locator) in items.into_iter().zip(locators) {
            if pred(&item) {
                kept_items.push(item);
                kept_locators.push(locator);
            }
        }
        WorkerView {
            items: kept_items,
            locators: kept_locators,
            worker_id,
        }
    }

    /// Stably reorder the view's items by the key returned from `f`,
    /// keeping every item paired with the locator that was produced
    /// for it at view-construction time. Use to apply a caller-side
    /// tie-break (preferred secondaries, recency, anything sortable)
    /// without teaching the scheduler about that concern.
    ///
    /// Pairing invariant: `items[i]` and `locators[i]` are always
    /// rearranged together, so a subsequent `take_from_view(view, i)`
    /// removes precisely the item the caller sees at `as_slice()[i]`.
    /// Stable sort (`slice::sort_by_key`) means equal-key items keep
    /// their pre-sort relative order — the caller can layer this on
    /// top of the construction-time priority order without scrambling
    /// FIFO within a tie class.
    pub fn sort_by_key<K, F>(self, mut f: F) -> Self
    where
        K: Ord,
        F: FnMut(&TaskInfo<I>) -> K,
    {
        let WorkerView {
            items,
            locators,
            worker_id,
        } = self;
        let mut paired: Vec<(TaskInfo<I>, (BucketKey, usize))> =
            items.into_iter().zip(locators).collect();
        paired.sort_by_key(|(t, _)| f(t));
        let mut sorted_items = Vec::with_capacity(paired.len());
        let mut sorted_locators = Vec::with_capacity(paired.len());
        for (item, locator) in paired {
            sorted_items.push(item);
            sorted_locators.push(locator);
        }
        WorkerView {
            items: sorted_items,
            locators: sorted_locators,
            worker_id,
        }
    }
}
