//! Read-only accessors plus the two non-dispatch *queued-side*
//! mutation primitives (`retain`, `update_first_match_in_place`,
//! `take_first_match`) that read or rewrite the queued portion of
//! the pool without touching the in-flight counters.
//!
//! Entry points:
//! * [`PendingPool::is_run_complete`] — terminal-state predicate.
//! * [`PendingPool::len`] / [`PendingPool::is_empty`] — outstanding-work
//!   counters.
//! * [`PendingPool::iter`] — iteration over queued items (diagnostic).
//! * [`PendingPool::retain`] — drop queued items by predicate.
//! * [`PendingPool::update_first_match_in_place`] — first-match in-place
//!   edit (queued ∪ blocked).
//! * [`PendingPool::take_first_match`] — first-match removal from a
//!   dispatchable bucket (does NOT increment in-flight; for callers
//!   that pair with `mark_in_flight`).
//! * [`PendingPool::active_phases`] / [`PendingPool::phase_state`] /
//!   [`PendingPool::in_flight`] — phase-state accessors.

use dynrunner_core::{Identifier, PhaseId, TaskInfo};

use super::pool::PendingPool;
use super::types::{BucketKey, PhaseState};

impl<I: Identifier> PendingPool<I> {
    /// True iff the entire pool is empty AND no phase is `Active` or
    /// `Draining`. Manager loop predicate.
    pub fn is_run_complete(&self) -> bool {
        if !self.is_empty() {
            return false;
        }
        !self
            .phase_state
            .values()
            .any(|s| matches!(s, PhaseState::Active | PhaseState::Draining))
    }

    /// Total items remaining: queued + in-flight + blocked, all phases.
    /// Blocked items are part of the run's outstanding work even though
    /// they're not in any bucket — they will become queued once their
    /// task-level prereqs resolve.
    pub fn len(&self) -> usize {
        let queued: usize = self.buckets.values().map(|b| b.items.len()).sum();
        let in_flight: usize = self
            .in_flight_per_phase
            .values()
            .map(|c| *c as usize)
            .sum();
        let blocked: usize = self.blocked.len();
        queued + in_flight + blocked
    }

    /// True iff `len() == 0`.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Iterate over all queued items (does not include in-flight).
    /// Used for diagnostic logging in the managers.
    pub fn iter(&self) -> impl Iterator<Item = &TaskInfo<I>> {
        self.buckets.values().flat_map(|b| b.items.iter())
    }

    /// Drop every queued item for which `pred` returns `false`. Iterates
    /// every bucket in deterministic key order; empty buckets are left
    /// in place (cheap, and matches the lazy-creation pattern in
    /// `extend`). Does NOT touch in-flight items: removal of an item
    /// already handed to a worker is the manager's concern, surfaced
    /// via `release_worker` / `on_item_finished`.
    ///
    /// Used by callers (e.g. primary) to drop items completed
    /// elsewhere in the cluster without disturbing the phase machine.
    pub fn retain<F>(&mut self, mut pred: F)
    where
        F: FnMut(&TaskInfo<I>) -> bool,
    {
        for bucket in self.buckets.values_mut() {
            bucket.items.retain(|item| pred(item));
        }
    }

    /// Apply `update` in-place to the FIRST queued or blocked
    /// `TaskInfo<I>` for which `pred` returns `true`. Returns `true`
    /// iff a match was found and updated.
    ///
    /// Scan order: queued buckets in `BucketKey` order, FIFO within
    /// each bucket; then the `blocked` map in its `HashMap` iteration
    /// order (deterministic enough for "first match wins" semantics
    /// when at most one task can match — the intended use case).
    /// In-flight items are NOT visited: they've already been
    /// dispatched, and the per-task metadata snapshot they carry on
    /// the wire was taken at dispatch time. The pool's
    /// post-dispatch mutation cannot retroactively reshape an
    /// in-flight task.
    ///
    /// Used by callers that own out-of-band per-task metadata
    /// updates (e.g. preferred-secondaries change applied via the
    /// CRDT command channel) and need the live pool's clone to
    /// reflect the change before the next scheduler tick reads it.
    /// The pool stays generic over what "match" means: the predicate
    /// closes over whatever identity key the caller cares about
    /// (task hash, task_id, identifier) so the pool doesn't have to
    /// learn about wire-canonical hashing.
    pub fn update_first_match_in_place<F, U>(&mut self, pred: F, mut update: U) -> bool
    where
        F: Fn(&TaskInfo<I>) -> bool,
        U: FnMut(&mut TaskInfo<I>),
    {
        for bucket in self.buckets.values_mut() {
            if let Some(item) = bucket.items.iter_mut().find(|t| pred(t)) {
                update(item);
                return true;
            }
        }
        for item in self.blocked.values_mut() {
            if pred(item) {
                update(item);
                return true;
            }
        }
        false
    }

    /// Find the first queued item (in bucket-key order, FIFO within a
    /// bucket) for which `pred` returns `true`, remove it from its
    /// bucket and return it. Returns `None` if no item matches.
    ///
    /// Buckets whose phase is not `Active` or `Draining` are skipped
    /// — a `Blocked` phase's items must not be dispatched out of order,
    /// and `Drained`/`Done` phases hold no live work. `Draining` is
    /// included so items pushed back into a draining phase via
    /// `requeue` (which flips it back to `Active`) and `reinject` paths
    /// remain reachable through this primitive.
    ///
    /// Does NOT update in-flight counts or worker affinity — this is
    /// a *removal* primitive, not a *dispatch* primitive. Intended for
    /// callers (promoted primary) that need to extract a task
    /// matching a runtime predicate (e.g. memory-fit) without going
    /// through the soft-pin algorithm `pop_for_worker` implements.
    ///
    /// If a matched item leaves its bucket empty, the bucket's pinned
    /// workers are unpinned (mirroring `take_from_bucket`'s behaviour),
    /// so soft-pin invariants stay correct for any later dispatch.
    pub fn take_first_match<F>(&mut self, mut pred: F) -> Option<TaskInfo<I>>
    where
        F: FnMut(&TaskInfo<I>) -> bool,
    {
        let mut hit_key: Option<BucketKey> = None;
        let mut hit_idx: usize = 0;
        for (key, bucket) in &self.buckets {
            // Skip buckets whose phase is not currently dispatchable.
            // Active = items may dispatch; Draining = items requeued/
            // reinjected after a drain transition flipped back to
            // Active are dispatchable too. Blocked / Drained / Done
            // phases must not have items pulled out of order.
            if !matches!(
                self.phase_state.get(&key.0),
                Some(PhaseState::Active | PhaseState::Draining)
            ) {
                continue;
            }
            if let Some(idx) = bucket.items.iter().position(&mut pred) {
                hit_key = Some(key.clone());
                hit_idx = idx;
                break;
            }
        }
        let key = hit_key?;
        let bucket = self.buckets.get_mut(&key)?;
        let item = bucket.items.remove(hit_idx)?;

        // Clear soft-pin slots if this drained the bucket, mirroring
        // the bookkeeping in `take_from_bucket` so a later dispatch
        // doesn't see stale pin state.
        if bucket.items.is_empty() {
            let drained_pinners = std::mem::take(&mut bucket.pinned_workers);
            for w in drained_pinners {
                if let Some(slot) = self.worker_affinity.get_mut(&w)
                    && slot.as_ref() == Some(&key)
                {
                    *slot = None;
                }
            }
        }
        Some(item)
    }

    /// Phases currently in `Active` state (callers may need this to
    /// filter scheduling decisions).
    pub fn active_phases(&self) -> Vec<PhaseId> {
        self.phase_state
            .iter()
            .filter(|(_, s)| **s == PhaseState::Active)
            .map(|(p, _)| p.clone())
            .collect()
    }

    /// State of one phase. Useful for tests and diagnostic logging.
    pub fn phase_state(&self, phase_id: &PhaseId) -> Option<PhaseState> {
        self.phase_state.get(phase_id).copied()
    }

    /// Number of in-flight items for a phase. Useful for tests.
    pub fn in_flight(&self, phase_id: &PhaseId) -> u32 {
        self.in_flight_per_phase.get(phase_id).copied().unwrap_or(0)
    }

    /// Count of queued items that are READY to dispatch right now: items
    /// sitting in a bucket whose phase is `Active`. This is the
    /// "tasks-ready-in-queue" primitive — distinct from [`Self::len`]
    /// (which folds in in-flight + blocked) and from [`Self::iter`]
    /// (which ignores phase state and so also surfaces items parked in
    /// `Blocked`/`Drained`/`Done` phases that are NOT dispatchable).
    ///
    /// Phase filter = `Active` only. A `Blocked` phase's items are not
    /// yet eligible; `Drained`/`Done` phases hold no live work; and a
    /// `Draining` phase is — by the state machine's definition — one
    /// whose queued buckets have already emptied (the empty-pool
    /// transition is what flipped it to `Draining`), so it contributes
    /// nothing to a queued count in steady state. Items requeued back
    /// into a draining phase flip it to `Active` first (see
    /// `lifecycle::requeue`), so they are counted under `Active` rather
    /// than being lost. Restricting to `Active` keeps this accessor's
    /// semantics exactly "items a worker could be handed this instant".
    ///
    /// Task-level blocked items (waiting on unresolved `task_depends_on`
    /// prereqs) live in `self.blocked`, never in a bucket, so they are
    /// already excluded — this counts only genuinely-ready work.
    pub fn ready_in_active_phase(&self) -> usize {
        self.buckets
            .iter()
            .filter(|(key, _)| {
                matches!(self.phase_state.get(&key.0), Some(PhaseState::Active))
            })
            .map(|(_, bucket)| bucket.items.len())
            .sum()
    }
}
