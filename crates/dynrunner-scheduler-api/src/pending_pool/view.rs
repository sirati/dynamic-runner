//! [`WorkerView`] — affinity-ordered snapshot of one worker's eligible
//! items, suitable as input to a `Scheduler::assign_normal` call.
//!
//! Built by `PendingPool::view_for_worker` and consumed via
//! [`ViewSelection`] / `PendingPool::take_selected` (both in the
//! `dispatch` submodule). This file owns the value types and the
//! view's caller-side combinators (`filter`, `sort_by_key`);
//! construction and consumption live in `dispatch.rs` where the
//! bucketed state lives.
//!
//! The struct fields are `pub(super)` so sibling submodules can pair
//! `items` with `locators` directly. The crate-external API surface
//! exposes only the accessor methods on this impl.

use dynrunner_core::{Identifier, TaskInfo, WorkerId};

use super::types::BucketKey;

/// Affinity-ordered snapshot of a worker's eligible items, suitable as
/// input to a `Scheduler::assign_normal` call.
///
/// Built by [`super::PendingPool::view_for_worker`]. The `items` slice
/// BORROWS the candidate `TaskInfo<I>` values straight from the pool —
/// no candidate is ever cloned for a view — so the borrow checker
/// enforces what used to be a documented-only contract: the pool
/// cannot be mutated while a view is alive. To consume the chosen
/// item, extract an owned [`ViewSelection`] with [`WorkerView::select`]
/// (the view's last use, releasing the pool borrow) and hand it to
/// [`super::PendingPool::take_selected`].
#[derive(Debug)]
pub struct WorkerView<'p, I: Identifier> {
    pub(super) items: Vec<&'p TaskInfo<I>>,
    pub(super) locators: Vec<(BucketKey, usize)>,
    pub(super) worker_id: WorkerId,
}

/// Owned ticket for one chosen slot of a [`WorkerView`]: the bucket
/// locator of the item the scheduler picked, plus the worker the view
/// was built for. Produced by [`WorkerView::select`]; consumed by
/// [`super::PendingPool::take_selected`]. Owning (no pool borrow) so
/// the caller can drop the view — releasing the pool borrow — before
/// taking. The take must still run before any other pool mutation;
/// `take_selected` debug-asserts the locator is intact.
#[derive(Debug)]
pub struct ViewSelection {
    pub(super) bucket_key: BucketKey,
    pub(super) item_idx: usize,
    pub(super) worker_id: WorkerId,
}

impl<'p, I: Identifier> WorkerView<'p, I> {
    /// The affinity-ordered slice of borrowed candidate items.
    /// Indexed positionally by [`WorkerView::select`].
    pub fn as_slice(&self) -> &[&'p TaskInfo<I>] {
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

    /// Extract the owned consumption ticket for the item at
    /// `slice_idx` — the positional index into [`Self::as_slice`] the
    /// scheduler chose. Panics if `slice_idx` is out of range.
    pub fn select(&self, slice_idx: usize) -> ViewSelection {
        let (bucket_key, item_idx) = self
            .locators
            .get(slice_idx)
            .cloned()
            .expect("slice_idx out of range for WorkerView");
        ViewSelection {
            bucket_key,
            item_idx,
            worker_id: self.worker_id,
        }
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
            if pred(item) {
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
    /// rearranged together, so a subsequent `select(i)` ticket removes
    /// precisely the item the caller sees at `as_slice()[i]`. Stable
    /// sort (`slice::sort_by_key`) means equal-key items keep their
    /// pre-sort relative order — the caller can layer this on top of
    /// the construction-time priority order without scrambling FIFO
    /// within a tie class.
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
        let mut paired: Vec<(&'p TaskInfo<I>, (BucketKey, usize))> =
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
