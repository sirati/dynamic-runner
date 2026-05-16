//! Dispatch path: building a `WorkerView` for a worker, popping the
//! next item via the soft-pin algorithm, consuming a chosen item from
//! a view, and the internal helpers that share the affinity/in-flight
//! bookkeeping between those entry points.
//!
//! Methods in this module:
//! * [`PendingPool::pop_for_worker`] — single-shot, returns the next
//!   item the soft-pin algorithm chooses for the worker.
//! * [`PendingPool::view_for_worker`] — affinity-ordered snapshot for
//!   the scheduler; pairs items with internal locators.
//! * [`PendingPool::take_from_view`] — consume one slot of a view.
//! * `take_at` (private) — the shared write that updates affinity and
//!   in-flight counters; called by both entry points.
//! * `choose_bucket_for` (private) — the read-only soft-pin algorithm.

use std::collections::HashSet;

use dynrunner_core::{Identifier, PhaseId, TaskInfo, WorkerId};

use super::pool::PendingPool;
use super::types::{Bucket, BucketKey, PhaseState, PreferencePredicate, no_affinity};
use super::view::WorkerView;

impl<I: Identifier> PendingPool<I> {
    /// Return the next item this worker should process, or `None`.
    ///
    /// Soft-pin algorithm:
    /// 1. If worker has affinity to `(P, T, A)` whose phase is
    ///    Active or Draining and that bucket has items — return front.
    /// 2. Otherwise prefer an unpinned typed (non-free-pool) bucket
    ///    in an Active phase: claim it for this worker, return front.
    /// 3. Otherwise the free-pool bucket (`AffinityId::""`) of any
    ///    Active phase if any has items — return front (no pinning,
    ///    by definition).
    /// 4. Otherwise any bucket with items in an Active phase: co-pin
    ///    this worker, return front.
    /// 5. Otherwise `None`.
    ///
    /// On take: `in_flight_per_phase[phase]` increments. If the bucket
    /// becomes empty, its pinned workers' affinity records are
    /// cleared so they fall back to the free pool on subsequent calls.
    pub fn pop_for_worker(&mut self, worker_id: WorkerId) -> Option<TaskInfo<I>> {
        let key = self.choose_bucket_for(worker_id)?;
        // Always pop the front item of the chosen bucket. take_at handles
        // affinity / in-flight bookkeeping and drain transitions.
        let bucket = self.buckets.get(&key)?;
        if bucket.items.is_empty() {
            return None;
        }
        Some(self.take_at(&key, 0, worker_id))
    }

    /// Affinity-ordered view of items currently eligible for `worker_id`.
    ///
    /// The returned [`WorkerView`] holds **cloned** `TaskInfo<I>` values so
    /// the borrow on the pool is released by the time the caller hands the
    /// slice to a `Scheduler`. To consume the view (and remove the chosen
    /// item from the underlying bucket) use [`take_from_view`].
    ///
    /// Ordering matches the soft-pin priority of [`pop_for_worker`]:
    /// 1. items in the worker's currently-pinned bucket (if its phase is
    ///    `Active` or `Draining`),
    /// 2. items in unpinned typed (non-free-pool) buckets of `Active` phases
    ///    (BTreeMap key order across buckets, FIFO within each bucket),
    /// 3. items in free-pool (`AffinityId::""`) buckets of `Active` phases,
    /// 4. items in any remaining bucket of an `Active` phase (co-pin
    ///    candidates).
    ///
    /// Buckets in `Blocked`, `Drained`, or `Done` phases are skipped. A
    /// bucket appears in at most one priority class.
    ///
    /// **Concurrency note**: this method does not record any reservation;
    /// the corresponding `take_from_view` must run before any other
    /// mutation to the pool, otherwise the locator indices stored in the
    /// view may become stale. The local manager's single-threaded loop
    /// satisfies this; multi-threaded callers must guard the pair with a
    /// lock.
    ///
    /// When `preference_predicate` is `Some`, the items emitted for each
    /// of the four priority classes (pin, typed, free-pool, co-pin) are
    /// stably reordered by the predicate *within that class only* before
    /// being appended to the output — the class ordering itself is
    /// invariant, so a pin-class item is never displaced by a typed-class
    /// item the predicate would otherwise prefer. `None` skips the sort
    /// step entirely and produces the same byte-for-byte view a
    /// no-predicate call would have built before this parameter was
    /// introduced.
    pub fn view_for_worker(
        &self,
        worker_id: WorkerId,
        preference_predicate: Option<&PreferencePredicate<'_, I>>,
    ) -> WorkerView<I> {
        // Local alias for the paired `(item, locator)` scratch shape.
        // Kept as a local type binding so it does not leak into the
        // public surface of the module.
        type Paired<I> = (TaskInfo<I>, (BucketKey, usize));

        let no_aff = no_affinity();
        let mut emitted: HashSet<BucketKey> = HashSet::new();
        // Per-class chunks of paired (item, locator) entries. The four
        // chunks correspond, in order, to: pin, typed, free-pool, co-pin.
        let mut chunks: [Vec<Paired<I>>; 4] =
            [Vec::new(), Vec::new(), Vec::new(), Vec::new()];

        let collect_bucket = |key: &BucketKey,
                              bucket: &Bucket<I>,
                              emitted: &mut HashSet<BucketKey>,
                              sink: &mut Vec<Paired<I>>| {
            for (idx, item) in bucket.items.iter().enumerate() {
                sink.push((item.clone(), (key.clone(), idx)));
            }
            emitted.insert(key.clone());
        };

        let phase_active_or_draining = |phase: &PhaseId| {
            matches!(
                self.phase_state.get(phase),
                Some(PhaseState::Active | PhaseState::Draining)
            )
        };
        let phase_active = |phase: &PhaseId| {
            self.phase_state.get(phase) == Some(&PhaseState::Active)
        };

        // Class 0 (pin): worker's pinned bucket if eligible.
        if let Some(Some(key)) = self.worker_affinity.get(&worker_id)
            && phase_active_or_draining(&key.0)
            && let Some(bucket) = self.buckets.get(key)
            && !bucket.items.is_empty()
        {
            collect_bucket(key, bucket, &mut emitted, &mut chunks[0]);
        }

        // Class 1 (typed): unpinned typed buckets in Active phases.
        for (key, bucket) in &self.buckets {
            if emitted.contains(key) || bucket.items.is_empty() {
                continue;
            }
            if key.2 == no_aff {
                continue;
            }
            if !phase_active(&key.0) {
                continue;
            }
            if !bucket.pinned_workers.is_empty() {
                continue;
            }
            collect_bucket(key, bucket, &mut emitted, &mut chunks[1]);
        }

        // Class 2 (free-pool): free-pool buckets in Active phases.
        for (key, bucket) in &self.buckets {
            if emitted.contains(key) || bucket.items.is_empty() {
                continue;
            }
            if key.2 != no_aff {
                continue;
            }
            if !phase_active(&key.0) {
                continue;
            }
            collect_bucket(key, bucket, &mut emitted, &mut chunks[2]);
        }

        // Class 3 (co-pin): any remaining bucket with items in an Active phase.
        for (key, bucket) in &self.buckets {
            if emitted.contains(key) || bucket.items.is_empty() {
                continue;
            }
            if !phase_active(&key.0) {
                continue;
            }
            collect_bucket(key, bucket, &mut emitted, &mut chunks[3]);
        }

        // Per-class stable sort with the caller's predicate, applied
        // uniformly across every class. The class boundaries themselves
        // are immutable — the predicate never lets a lower-class item
        // overtake a higher-class one — because sorting happens inside
        // each chunk before flattening.
        if let Some(pred) = preference_predicate {
            for chunk in &mut chunks {
                chunk.sort_by_key(|(item, _)| pred(item));
            }
        }

        let total: usize = chunks.iter().map(|c| c.len()).sum();
        let mut items: Vec<TaskInfo<I>> = Vec::with_capacity(total);
        let mut locators: Vec<(BucketKey, usize)> = Vec::with_capacity(total);
        for chunk in chunks {
            for (item, locator) in chunk {
                items.push(item);
                locators.push(locator);
            }
        }

        WorkerView {
            items,
            locators,
            worker_id,
        }
    }

    /// Remove the item at `slice_idx` of `view` from its bucket, recording
    /// the worker's affinity claim and incrementing the in-flight count
    /// for the phase. Returns the owned `TaskInfo<I>`.
    ///
    /// Panics if `slice_idx` is out of range, or if the underlying bucket
    /// has shrunk (debug builds only) since the view was constructed —
    /// callers are required to consume the view before any other pool
    /// mutation. See [`view_for_worker`].
    pub fn take_from_view(
        &mut self,
        view: WorkerView<I>,
        slice_idx: usize,
    ) -> TaskInfo<I> {
        let (bucket_key, item_idx) = view
            .locators
            .get(slice_idx)
            .cloned()
            .expect("slice_idx out of range for WorkerView");
        let worker_id = view.worker_id;
        // The bucket must still hold the same item at the recorded index.
        // This invariant is required for correctness; any caller that
        // mutated the pool between view construction and take_from_view
        // is buggy.
        debug_assert!(
            self.buckets
                .get(&bucket_key)
                .map(|b| item_idx < b.items.len())
                .unwrap_or(false),
            "WorkerView locator points past end of bucket; pool was \
             mutated between view construction and take_from_view"
        );
        self.take_at(&bucket_key, item_idx, worker_id)
    }

    // ---- internals shared by pop_for_worker and take_from_view ----

    /// Remove the item at `index` of bucket `key`, run the same
    /// affinity / in-flight bookkeeping as `take_from_bucket`, and
    /// return the owned item. Internal helper — bounds and existence are
    /// trusted; callers must have verified them.
    pub(super) fn take_at(
        &mut self,
        key: &BucketKey,
        index: usize,
        worker_id: WorkerId,
    ) -> TaskInfo<I> {
        let no_aff = no_affinity();
        let bucket = self
            .buckets
            .get_mut(key)
            .expect("take_at called on missing bucket");
        let item = bucket
            .items
            .remove(index)
            .expect("take_at called with out-of-range index");

        if key.2 != no_aff {
            if !bucket.pinned_workers.contains(&worker_id) {
                bucket.pinned_workers.push(worker_id);
            }
            self.worker_affinity.insert(worker_id, Some(key.clone()));
        } else {
            self.worker_affinity.entry(worker_id).or_insert(None);
        }

        let in_flight_count = self
            .in_flight_per_phase
            .entry(key.0.clone())
            .or_insert(0);
        *in_flight_count += 1;
        tracing::debug!(
            phase = %key.0,
            new_in_flight = *in_flight_count,
            "pool: in_flight +1 (take_from_view)"
        );

        if bucket.items.is_empty() {
            let drained_pinners = std::mem::take(&mut bucket.pinned_workers);
            for w in drained_pinners {
                if let Some(slot) = self.worker_affinity.get_mut(&w)
                    && slot.as_ref() == Some(key)
                {
                    *slot = None;
                }
            }
            self.maybe_transition_drain(&key.0);
        }

        item
    }

    /// Pick a bucket for `worker_id` per the soft-pin algorithm,
    /// returning the bucket key (or `None` if nothing is dispatchable).
    /// Pure: doesn't mutate state — `take_at` performs the actual claim.
    fn choose_bucket_for(&self, worker_id: WorkerId) -> Option<BucketKey> {
        let no_aff = no_affinity();

        // Step 1: existing affinity, if its phase is Active or Draining
        // and items remain.
        if let Some(Some(key)) = self.worker_affinity.get(&worker_id) {
            let phase_ok = matches!(
                self.phase_state.get(&key.0),
                Some(PhaseState::Active | PhaseState::Draining)
            );
            if phase_ok
                && let Some(bucket) = self.buckets.get(key)
                && !bucket.items.is_empty()
            {
                return Some(key.clone());
            }
        }

        // Step 2: unpinned, non-free-pool, Active-phase bucket with items.
        for (key, bucket) in &self.buckets {
            if bucket.items.is_empty() {
                continue;
            }
            if key.2 == no_aff {
                continue;
            }
            if self.phase_state.get(&key.0) != Some(&PhaseState::Active) {
                continue;
            }
            if bucket.pinned_workers.is_empty() {
                return Some(key.clone());
            }
        }

        // Step 3: free-pool bucket of any Active phase.
        for (key, bucket) in &self.buckets {
            if bucket.items.is_empty() {
                continue;
            }
            if key.2 != no_aff {
                continue;
            }
            if self.phase_state.get(&key.0) != Some(&PhaseState::Active) {
                continue;
            }
            return Some(key.clone());
        }

        // Step 4: any bucket with items in an Active phase (co-pin).
        for (key, bucket) in &self.buckets {
            if bucket.items.is_empty() {
                continue;
            }
            if self.phase_state.get(&key.0) != Some(&PhaseState::Active) {
                continue;
            }
            return Some(key.clone());
        }

        None
    }
}
