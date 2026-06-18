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
use std::sync::Arc;

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
    pub fn pop_for_worker(&mut self, worker_id: WorkerId) -> Option<Arc<TaskInfo<I>>> {
        let key = self.choose_bucket_for(worker_id)?;
        // Pop the first dispatch-ELIGIBLE item of the chosen bucket (a
        // non-dispatchable item — `Setup` kind or affine-dep — at the front
        // must not block its eligible siblings). `choose_bucket_for` only
        // returns buckets with at least one eligible item. take_at
        // handles affinity / in-flight bookkeeping and drain
        // transitions.
        let bucket = self.buckets.get(&key)?;
        let index = self.first_eligible_index(bucket)?;
        // Idempotent pop-time re-check (#652 D.1): a reconcile-pushed
        // (concern C) item at a bucket head may NOT actually be ready, so the
        // old "bucketed ⇒ ready" invariant no longer holds at the consume
        // point. `take_at_if_ready` re-blocks a not-ready item (re-routing it
        // through `commit_item`) and returns `None`; the worker simply gets no
        // dispatch this slot (a later dep-completion re-surfaces the item).
        self.take_at_if_ready(&key, index, worker_id)
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

        // Dispatch-eligibility filter: a non-worker-assignable item (a
        // `Setup` kind, or a work task with an affine dep) is invisible to
        // dispatch (it still counts as queued for the phase machine).
        // Locators index the ORIGINAL bucket positions, so skipping an item
        // here keeps every emitted locator valid for `take_selected`.
        let collect_bucket = |key: &BucketKey,
                              bucket: &'p Bucket<I>,
                              emitted: &mut HashSet<BucketKey>,
                              sink: &mut Vec<Paired<'p, I>>| {
            for (idx, item) in bucket.items.iter().enumerate() {
                // `bucket.items` holds `Arc<TaskInfo>`; the view borrows the
                // inner `&TaskInfo` (deref the Arc) so the view's value
                // shape is unchanged — it still BORROWS, clones nothing.
                let item: &TaskInfo<I> = item.as_ref();
                // The SINGLE dispatch-eligibility gate (worker-assignable
                // kind AND no affine dep) — a `Setup` task is invisible to
                // the worker view here, never via a scattered kind check.
                if !self.dispatch_eligible(item) {
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
    /// in-flight count for the phase. Returns the shared
    /// `Arc<TaskInfo<I>>` the pool held — the dispatch path clones this
    /// Arc into the in-flight ledger / worker slot, never deep-cloning
    /// the `TaskInfo`.
    ///
    /// Panics (debug builds only) if the underlying bucket has shrunk
    /// since the view was constructed — callers are required to consume
    /// the selection before any other pool mutation. See
    /// [`view_for_worker`].
    pub fn take_selected(&mut self, selection: ViewSelection) -> Option<Arc<TaskInfo<I>>> {
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
        // Idempotent pop-time re-check (#652 D.1): see `take_at_if_ready`. A
        // reconcile-pushed (concern C) not-ready item selected from the view is
        // re-blocked rather than dispatched, so `None` here means "the view's
        // chosen item turned out not-ready; skip this slot". The view itself
        // never sees a reconcile-pushed item as not-ready (the view is built
        // from the same buckets), so on the steady-state path this is always
        // `Some` — byte-identical to the pre-#652 behaviour.
        self.take_at_if_ready(&bucket_key, item_idx, worker_id)
    }

    // ---- internals shared by pop_for_worker and take_selected ----

    /// Idempotent pop-time readiness gate (#652 D.1) — the shared consume-point
    /// guard for BOTH [`Self::pop_for_worker`] and [`Self::take_selected`].
    ///
    /// The 5-min reconcile arm (concern C) pushes a possibly-not-ready item to
    /// a bucket HEAD ([`Self::push_to_queue_head`]), which breaks the old
    /// "every bucketed item is dep-ready" invariant the take paths trusted. This
    /// gate restores it at the consume point: it re-derives the item's
    /// readiness through the SINGLE [`Self::unresolved_deps`] authority (the
    /// SAME computation [`Self::commit_item`] uses at ingest — zero duplicated
    /// dep logic), and:
    ///
    ///   * READY (no unresolved dep) → delegate to the pure-write [`Self::take_at`]
    ///     and return the item (the steady-state path: every non-reconcile item
    ///     is ready, so this is the common case and is byte-identical to the
    ///     pre-#652 direct `take_at`).
    ///   * NOT READY → REMOVE the item from its bucket and route it back through
    ///     `commit_item`, which re-registers it as `blocked` (rebuilding its
    ///     `dependents_of` / `task_deps` / `blocked_per_phase` edges identically
    ///     to ingest); return `None`. The item leaves the dispatchable bucket and
    ///     waits for its final dep's completion to re-surface it (the affine twin
    ///     is the per-secondary readiness gate, D.2). No in-flight bump (the item
    ///     never dispatched), so the accounting stays balanced.
    fn take_at_if_ready(
        &mut self,
        key: &BucketKey,
        index: usize,
        worker_id: WorkerId,
    ) -> Option<Arc<TaskInfo<I>>> {
        // Re-derive readiness on the item still sitting in its bucket. A bucket
        // / index that no longer resolves (a concurrent mutation) yields `None`
        // — the caller treats it as "no dispatch this slot".
        let item = self.buckets.get(key)?.items.get(index)?.clone();
        if self.unresolved_deps(&item).is_empty() {
            // Ready: pure-write take (unchanged accounting).
            return Some(self.take_at(key, index, worker_id));
        }
        // Not ready (a reconcile-pushed item whose deps are not yet met):
        // evict it from the bucket and re-block it through the single edge
        // builder. NOTE: removing the item shifts later indices, but the
        // caller dispatches at most one item per call, so no stale index is
        // reused after this.
        let bucket = self.buckets.get_mut(key)?;
        let evicted = bucket.items.remove(index)?;
        self.commit_item(evicted);
        None
    }

    /// Remove the item at `index` of bucket `key`, run the same
    /// affinity / in-flight bookkeeping as `take_from_bucket`, and
    /// return the owned item. Internal helper — bounds and existence are
    /// trusted; callers must have verified them.
    pub(super) fn take_at(
        &mut self,
        key: &BucketKey,
        index: usize,
        worker_id: WorkerId,
    ) -> Arc<TaskInfo<I>> {
        let no_aff = no_affinity();
        let bucket = self
            .buckets
            .get_mut(key)
            .expect("take_at called on missing bucket");
        let item = bucket
            .items
            .remove(index)
            .expect("take_at called with out-of-range index");

        // Drop its bring-up reservation holder entry (the holder confirmed
        // and a worker took its share); the formation window closes itself
        // when the last reserved task drains. Inert outside the window.
        // Disjoint field borrow (`reservation`, not whole `self`) so the
        // live `bucket` borrow below stays valid.
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
    /// dispatch-ELIGIBLE item (a non-worker-assignable item — `Setup` kind
    /// or affine-dep — is invisible here, same as in
    /// [`Self::view_for_worker`]).
    fn choose_bucket_for(&self, worker_id: WorkerId) -> Option<BucketKey> {
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
                && self.first_eligible_index(bucket).is_some()
            {
                return Some(key.clone());
            }
        }

        // Step 2: unpinned, non-free-pool, Active-phase bucket with
        // eligible items.
        for (key, bucket) in &self.buckets {
            if self.first_eligible_index(bucket).is_none() {
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
            if self.first_eligible_index(bucket).is_none() {
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
            if self.first_eligible_index(bucket).is_none() {
                continue;
            }
            if self.phase_state.get(&key.0) != Some(&PhaseState::Active) {
                continue;
            }
            return Some(key.clone());
        }

        None
    }

    /// Index of the first dispatch-eligible item in `bucket` (`None` when
    /// the bucket is empty or every item is non-worker-assignable — a
    /// `Setup` kind or a work task with an affine dep).
    fn first_eligible_index(&self, bucket: &Bucket<I>) -> Option<usize> {
        bucket
            .items
            .iter()
            .position(|item| self.dispatch_eligible(item))
    }

    /// Whether `item` may be dispatched to a WORKER. The SINGLE
    /// worker-dispatch-eligibility predicate, consulted by every dispatch
    /// read path (`view_for_worker`'s per-item filter and
    /// [`Self::first_eligible_index`], which backs `pop_for_worker` /
    /// `choose_bucket_for`). It is the conjunction of two independent
    /// structural gates over the same "can this go to a worker" concern:
    ///
    ///   * the task KIND (only a `TaskKind::Work` task is worker-assignable;
    ///     a `TaskKind::Setup` task is executed in-process by its affinity
    ///     member and must NEVER appear in a worker dispatch view), and
    ///   * the AFFINE-DEP hide (a work task depending on a `SecondaryAffine`
    ///     import dispatches ONLY through the primary's per-secondary affine
    ///     queue, never the global worker view — see [`Self::has_affine_dep`]).
    ///
    /// A `Setup` task therefore sits in its bucket invisible to workers
    /// (still counted as queued, so it holds its phase open) until its
    /// in-process executor consumes it — the scheduling seam of the
    /// setup-task primitive. Folding both gates in here (rather than
    /// scattering kind / affine-dep checks across the four soft-pin classes)
    /// keeps the eligibility mapping at one seam.
    ///
    /// `pub` for the callers OUTSIDE the dispatch read paths that need the
    /// SAME worker-dispatch-eligibility gate (the primary's
    /// estimate-escalation rescue, and the pool's own
    /// `ready_dispatchable_below` depth read) — they must see exactly the
    /// tasks a worker view would, never a re-implemented filter.
    pub fn dispatch_eligible(&self, item: &TaskInfo<I>) -> bool {
        item.kind.is_worker_assignable() && !self.has_affine_dep(item)
    }

    /// Whether `item` depends on a `TaskKind::SecondaryAffine` prereq — the
    /// affine-dep WORK-task gate. Such a task must NOT dispatch from the GLOBAL
    /// worker view: its toolchain import is per-secondary (the affine
    /// bitvector), so global dispatch could send it to a secondary that never
    /// imported and it would run without its toolchain. It dispatches ONLY
    /// through the primary's per-secondary affine queue (the import runs THEN
    /// the work, in order, on the chosen secondary). The `affine_prereq_ids`
    /// set is empty on a run with no affine task, so this is `false` for every
    /// task and the view is byte-identical to the pre-affine behaviour.
    ///
    /// `pub` for the SECOND consumer of this exact predicate: the primary's
    /// requeue-recovery seam (`requeue_affine_aware`) must, on requeue of an
    /// affine-DEPENDENT work task, clear the affine scheduler's placement-dedup
    /// guard so the per-secondary queue unit re-derives — and it discriminates
    /// "affine-dep work" by reading THIS predicate, never a re-implemented copy
    /// (the pool is the sole owner of `affine_prereq_ids`).
    pub fn has_affine_dep(&self, item: &TaskInfo<I>) -> bool {
        if self.affine_prereq_ids.is_empty() {
            return false;
        }
        item.task_depends_on
            .iter()
            .any(|d| self.affine_prereq_ids.contains(d.task_id.as_str()))
    }

}
