//! Dispatch path: building a `WorkerView` for a worker, popping the
//! next item via the soft-pin algorithm, consuming a chosen item from
//! a view, and the internal helpers that share the affinity/in-flight
//! bookkeeping between those entry points.
//!
//! Methods in this module:
//! * [`PendingPool::pop_for_worker`] — single-shot, returns the next
//!   item the soft-pin algorithm chooses for the worker.
//! * [`PendingPool::view_for_worker`] — affinity-ordered borrowed view
//!   for the scheduler; pairs items with internal locators.
//! * [`PendingPool::take_selected`] — consume the view slot a
//!   [`ViewSelection`] ticket names.
//! * `take_at` (private) — the shared write that updates affinity and
//!   in-flight counters; called by both entry points.
//! * `choose_bucket_for` (private) — the read-only soft-pin algorithm.

use std::collections::HashSet;

use dynrunner_core::{Identifier, PhaseId, TaskInfo, WorkerId};

use super::pool::PendingPool;
use super::types::{Bucket, BucketKey, PhaseState, PreferencePredicate, no_affinity};
use super::view::{ViewSelection, WorkerView};

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
        let now = std::time::Instant::now();
        let key = self.choose_bucket_for(worker_id, now)?;
        // Pop the first dispatch-ELIGIBLE item of the chosen bucket (a
        // backed-off item at the front must not block its eligible
        // siblings, nor be dispatched early). `choose_bucket_for` only
        // returns buckets with at least one eligible item. take_at
        // handles affinity / in-flight bookkeeping and drain
        // transitions.
        let bucket = self.buckets.get(&key)?;
        let index = self.first_eligible_index(bucket, now)?;
        Some(self.take_at(&key, index, worker_id))
    }

    /// Affinity-ordered view of items currently eligible for `worker_id`.
    ///
    /// The returned [`WorkerView`] BORROWS the candidate `TaskInfo<I>`
    /// values from the pool — view construction clones nothing — so the
    /// borrow checker keeps the pool immutable while the caller hands
    /// the slice to a `Scheduler`. To consume the chosen item (and
    /// remove it from the underlying bucket) extract an owned ticket
    /// with [`WorkerView::select`] — the view's last use, releasing the
    /// pool borrow — and pass it to [`take_selected`].
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
    /// the corresponding `take_selected` must run before any other
    /// mutation to the pool, otherwise the locator indices stored in the
    /// selection may become stale. The borrow checker enforces this for
    /// the view itself; the window between `select` and `take_selected`
    /// is the caller's (single-threaded-loop) responsibility, guarded by
    /// `take_selected`'s debug assert.
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
    pub fn view_for_worker<'p>(
        &'p self,
        worker_id: WorkerId,
        preference_predicate: Option<&PreferencePredicate<'_, I>>,
    ) -> WorkerView<'p, I> {
        // Local alias for the paired `(item, locator)` scratch shape.
        // Kept as a local type binding so it does not leak into the
        // public surface of the module.
        type Paired<'p, I> = (&'p TaskInfo<I>, (BucketKey, usize));

        let no_aff = no_affinity();
        let mut emitted: HashSet<BucketKey> = HashSet::new();
        // Per-class chunks of paired (item, locator) entries. The four
        // chunks correspond, in order, to: pin, typed, free-pool, co-pin.
        let mut chunks: [Vec<Paired<'p, I>>; 4] = [Vec::new(), Vec::new(), Vec::new(), Vec::new()];

        // Re-dispatch backoff filter: an item parked under an
        // unexpired backoff stamp is invisible to dispatch (it still
        // counts as queued for the phase machine). Locators index the
        // ORIGINAL bucket positions, so skipping an item here keeps
        // every emitted locator valid for `take_selected`.
        let now = std::time::Instant::now();
        let collect_bucket = |key: &BucketKey,
                              bucket: &'p Bucket<I>,
                              emitted: &mut HashSet<BucketKey>,
                              sink: &mut Vec<Paired<'p, I>>| {
            for (idx, item) in bucket.items.iter().enumerate() {
                // The SINGLE dispatch-eligibility gate (re-dispatch backoff
                // AND worker-assignable kind) — a `Setup` task is invisible
                // to the worker view here, never via a scattered kind check.
                if !self.dispatch_eligible(item, now) {
                    continue;
                }
                sink.push((item, (key.clone(), idx)));
            }
            emitted.insert(key.clone());
        };

        let phase_active_or_draining = |phase: &PhaseId| {
            matches!(
                self.phase_state.get(phase),
                Some(PhaseState::Active | PhaseState::Draining)
            )
        };
        let phase_active =
            |phase: &PhaseId| self.phase_state.get(phase) == Some(&PhaseState::Active);

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
        let mut items: Vec<&'p TaskInfo<I>> = Vec::with_capacity(total);
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

    /// Remove the item a [`ViewSelection`] ticket names from its bucket,
    /// recording the worker's affinity claim and incrementing the
    /// in-flight count for the phase. Returns the owned `TaskInfo<I>`.
    ///
    /// Panics (debug builds only) if the underlying bucket has shrunk
    /// since the view was constructed — callers are required to consume
    /// the selection before any other pool mutation. See
    /// [`view_for_worker`].
    pub fn take_selected(&mut self, selection: ViewSelection) -> TaskInfo<I> {
        let ViewSelection {
            bucket_key,
            item_idx,
            worker_id,
        } = selection;
        // The bucket must still hold the same item at the recorded index.
        // This invariant is required for correctness; any caller that
        // mutated the pool between view construction and take_selected
        // is buggy.
        debug_assert!(
            self.buckets
                .get(&bucket_key)
                .map(|b| item_idx < b.items.len())
                .unwrap_or(false),
            "ViewSelection locator points past end of bucket; pool was \
             mutated between view construction and take_selected"
        );
        self.take_at(&bucket_key, item_idx, worker_id)
    }

    // ---- internals shared by pop_for_worker and take_selected ----

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

        // The item left the queue: drop its backoff stamp (the streak
        // persists — a later requeue keeps doubling).
        self.dispatch_backoff.note_taken(&item.task_id);
        // Drop its bring-up reservation holder entry (the holder confirmed
        // and a worker took its share); the formation window closes itself
        // when the last reserved task drains. Inert outside the window.
        // Disjoint field borrow (`reservation`, not whole `self`) so the
        // live `bucket` borrow below stays valid — mirrors the backoff
        // line above.
        self.reservation
            .note_taken(&(item.phase_id.clone(), item.task_id.clone()));

        if key.2 != no_aff {
            if !bucket.pinned_workers.contains(&worker_id) {
                bucket.pinned_workers.push(worker_id);
            }
            self.worker_affinity.insert(worker_id, Some(key.clone()));
        } else {
            self.worker_affinity.entry(worker_id).or_insert(None);
        }

        let in_flight_count = self.in_flight_per_phase.entry(key.0.clone()).or_insert(0);
        *in_flight_count += 1;
        tracing::debug!(
            phase = %key.0,
            new_in_flight = *in_flight_count,
            "pool: in_flight +1 (take_at)"
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
    ///
    /// A bucket qualifies only when it holds at least one
    /// dispatch-ELIGIBLE item at `now` (an item parked under an
    /// unexpired re-dispatch backoff is invisible here, same as in
    /// [`Self::view_for_worker`]).
    fn choose_bucket_for(&self, worker_id: WorkerId, now: std::time::Instant) -> Option<BucketKey> {
        let no_aff = no_affinity();

        // Step 1: existing affinity, if its phase is Active or Draining
        // and eligible items remain.
        if let Some(Some(key)) = self.worker_affinity.get(&worker_id) {
            let phase_ok = matches!(
                self.phase_state.get(&key.0),
                Some(PhaseState::Active | PhaseState::Draining)
            );
            if phase_ok
                && let Some(bucket) = self.buckets.get(key)
                && self.first_eligible_index(bucket, now).is_some()
            {
                return Some(key.clone());
            }
        }

        // Step 2: unpinned, non-free-pool, Active-phase bucket with
        // eligible items.
        for (key, bucket) in &self.buckets {
            if self.first_eligible_index(bucket, now).is_none() {
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
            if self.first_eligible_index(bucket, now).is_none() {
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

        // Step 4: any bucket with eligible items in an Active phase
        // (co-pin).
        for (key, bucket) in &self.buckets {
            if self.first_eligible_index(bucket, now).is_none() {
                continue;
            }
            if self.phase_state.get(&key.0) != Some(&PhaseState::Active) {
                continue;
            }
            return Some(key.clone());
        }

        None
    }

    /// Index of the first dispatch-eligible item in `bucket` at `now`
    /// (`None` when the bucket is empty or every item is parked under
    /// an unexpired re-dispatch backoff stamp, or is a non-worker-
    /// assignable kind).
    fn first_eligible_index(&self, bucket: &Bucket<I>, now: std::time::Instant) -> Option<usize> {
        bucket
            .items
            .iter()
            .position(|item| self.dispatch_eligible(item, now))
    }

    /// Whether `item` may be dispatched to a WORKER at `now`. The SINGLE
    /// worker-dispatch-eligibility predicate, consulted by every dispatch
    /// read path (`view_for_worker`'s per-item filter and
    /// [`Self::first_eligible_index`], which backs `pop_for_worker` /
    /// `choose_bucket_for`). It is the conjunction of two independent
    /// gates over the same "can this go to a worker right now" concern:
    ///
    ///   * the per-task re-dispatch BACKOFF (timing — a recently-bounced
    ///     task is parked until its stamp expires), and
    ///   * the task KIND (structural — only a `TaskKind::Work` task is
    ///     worker-assignable; a `TaskKind::Setup` task is executed
    ///     in-process by its affinity member and must NEVER appear in a
    ///     worker dispatch view).
    ///
    /// A `Setup` task therefore sits in its bucket invisible to workers
    /// (still counted as queued, so it holds its phase open) until its
    /// in-process executor consumes it — the scheduling seam of the
    /// setup-task primitive. Folding the kind gate in here (rather than
    /// scattering `if kind == Setup` across the four soft-pin classes)
    /// keeps the kind→behavior mapping at one seam.
    fn dispatch_eligible(&self, item: &TaskInfo<I>, now: std::time::Instant) -> bool {
        item.kind.is_worker_assignable() && self.dispatch_backoff.is_eligible(&item.task_id, now)
    }

    /// [`Self::dispatch_eligible`] sampled at the current instant — the
    /// public seam for callers OUTSIDE the dispatch read paths that need
    /// the SAME worker-dispatch-eligibility gate without threading a
    /// `now`. Used by the primary's estimate-escalation pass to decide
    /// which queued tasks the best-effort rescue should consider (it must
    /// see exactly the tasks a worker view would, never a re-implemented
    /// filter). Stays on the SINGLE eligibility seam so the two never
    /// diverge.
    pub fn dispatch_eligible_now(&self, item: &TaskInfo<I>) -> bool {
        self.dispatch_eligible(item, std::time::Instant::now())
    }
}
