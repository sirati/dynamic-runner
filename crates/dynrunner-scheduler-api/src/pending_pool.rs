//! `PendingPool<I>`: the scheduling-side data structure that owns the
//! queued and in-flight `TaskInfo<I>` items, grouped by
//! `(PhaseId, TypeId, AffinityId)`, plus a per-phase state machine
//! that gates dispatch on `depends_on` barriers.
//!
//! ## Concerns owned by this module
//! * Bucketing items by `(phase, type, affinity_or_sentinel)`.
//! * Tracking which workers are soft-pinned to which bucket.
//! * Tracking in-flight counts per phase.
//! * Validating the phase dependency graph at construction time
//!   (no cycles, no unknown deps).
//! * Driving the phase state machine
//!   `Blocked → Active → Draining → Drained → Done`.
//!
//! ## Concerns NOT owned by this module
//! * Worker selection / scheduler decisions (the `Scheduler` trait).
//! * Resource estimation.
//! * `on_phase_end` callbacks — managers fire those after polling
//!   drain transitions.
//! * Sorting items by size — callers extend the pool in the order
//!   they want items dispatched (the pool preserves insertion order
//!   within a bucket).

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use dynrunner_core::{AffinityId, Identifier, PhaseId, TaskInfo, TypeId, WorkerId};

/// Sentinel affinity id for items that have no pinning preference.
///
/// `TaskInfo::affinity_id` is `Option<AffinityId>`; the pool keys its
/// buckets on the non-optional `AffinityId`, mapping `None` to this
/// empty-string sentinel so the free pool is just another bucket
/// rather than a special case.
fn no_affinity() -> AffinityId {
    AffinityId::from("")
}

/// Effective affinity for a task: `affinity_id` if `Some`, else the sentinel.
fn affinity_key<I>(item: &TaskInfo<I>) -> AffinityId {
    item.affinity_id.clone().unwrap_or_else(no_affinity)
}

/// Composite bucket key.
pub type BucketKey = (PhaseId, TypeId, AffinityId);

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
    fn new() -> Self {
        Self {
            items: VecDeque::new(),
            pinned_workers: Vec::new(),
        }
    }
}

/// Affinity-ordered snapshot of a worker's eligible items, suitable as
/// input to a `Scheduler::assign_normal` call.
///
/// Built by [`PendingPool::view_for_worker`]; consumed by
/// [`PendingPool::take_from_view`]. The `items` slice exposes cloned
/// `TaskInfo<I>` values so the scheduler does not borrow the pool. The
/// internal `locators` vector preserves `(bucket_key, index)` pointers
/// so `take_from_view` can remove the chosen item from its actual
/// bucket.
#[derive(Debug)]
pub struct WorkerView<I: Identifier> {
    items: Vec<TaskInfo<I>>,
    locators: Vec<(BucketKey, usize)>,
    worker_id: WorkerId,
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
}

/// Errors validation produces at `PendingPool::new`.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PendingPoolError {
    #[error("phase dependency cycle detected starting at {0}")]
    DependencyCycle(PhaseId),
    #[error("phase {0} declared as a dependency but not in the phase set")]
    UnknownDependency(PhaseId),
}

/// Items grouped by `(phase, type, affinity)` plus the phase state
/// machine. See module-level docs for ownership boundaries.
#[derive(Debug)]
pub struct PendingPool<I: Identifier> {
    /// `BTreeMap` (not `HashMap`) so iteration order is deterministic
    /// — useful for tests and for diagnostic logging in managers.
    buckets: BTreeMap<BucketKey, Bucket<I>>,
    phase_state: HashMap<PhaseId, PhaseState>,
    phase_deps: HashMap<PhaseId, Vec<PhaseId>>,
    in_flight_per_phase: HashMap<PhaseId, u32>,
    /// Worker → currently affine bucket. `None` slot means the
    /// worker is in the pool's worker set but free of any pin.
    worker_affinity: HashMap<WorkerId, Option<BucketKey>>,
    /// Phases that transitioned to `Drained` since the last
    /// `poll_drain_transitions` call. Drained transitions are
    /// one-shot per phase: once polled they aren't re-emitted until
    /// the phase makes a fresh trip through the state machine
    /// (which does not happen in the standard lifecycle).
    drained_pending: Vec<PhaseId>,
}

impl<I: Identifier> PendingPool<I> {
    /// Build a new pool with the given phase set + dependency graph.
    ///
    /// Validates the graph (no cycles, all referenced deps known) before
    /// producing the pool. Phases with zero `depends_on` are initialised
    /// `Active`; the rest are `Blocked`.
    pub fn new(
        phases: impl IntoIterator<Item = PhaseId>,
        deps: HashMap<PhaseId, Vec<PhaseId>>,
    ) -> Result<Self, PendingPoolError> {
        let phase_set: HashSet<PhaseId> = phases.into_iter().collect();

        // Validate all deps reference known phases.
        for parents in deps.values() {
            for parent in parents {
                if !phase_set.contains(parent) {
                    return Err(PendingPoolError::UnknownDependency(parent.clone()));
                }
            }
        }
        // Validate dep keys reference known phases too.
        for child in deps.keys() {
            if !phase_set.contains(child) {
                return Err(PendingPoolError::UnknownDependency(child.clone()));
            }
        }

        // Cycle detection via Kahn's algorithm on the induced subgraph
        // of `phase_set`. Indegree is the count of parents per child
        // (each entry in `deps[child]` is one incoming edge).
        let mut indegree: HashMap<PhaseId, usize> = phase_set
            .iter()
            .map(|p| (p.clone(), 0usize))
            .collect();
        for (child, parents) in &deps {
            *indegree.entry(child.clone()).or_insert(0) += parents.len();
        }

        // Children-of map for traversal.
        let mut children_of: HashMap<PhaseId, Vec<PhaseId>> = HashMap::new();
        for (child, parents) in &deps {
            for parent in parents {
                children_of
                    .entry(parent.clone())
                    .or_default()
                    .push(child.clone());
            }
        }

        let mut queue: VecDeque<PhaseId> = indegree
            .iter()
            .filter_map(|(p, &d)| if d == 0 { Some(p.clone()) } else { None })
            .collect();
        let mut visited = 0usize;
        while let Some(p) = queue.pop_front() {
            visited += 1;
            if let Some(children) = children_of.get(&p) {
                for child in children {
                    let entry = indegree.get_mut(child).expect("child in indegree map");
                    *entry -= 1;
                    if *entry == 0 {
                        queue.push_back(child.clone());
                    }
                }
            }
        }
        if visited != phase_set.len() {
            // Pick any node with non-zero indegree as the cycle representative.
            let culprit = indegree
                .into_iter()
                .find(|(_, d)| *d != 0)
                .map(|(p, _)| p)
                .unwrap_or_else(|| {
                    phase_set.iter().next().cloned().expect("non-empty phases")
                });
            return Err(PendingPoolError::DependencyCycle(culprit));
        }

        // Initial state: Active iff the phase has zero deps.
        let mut phase_state = HashMap::with_capacity(phase_set.len());
        for p in &phase_set {
            let blocked = deps.get(p).is_some_and(|v| !v.is_empty());
            phase_state.insert(
                p.clone(),
                if blocked { PhaseState::Blocked } else { PhaseState::Active },
            );
        }

        let in_flight_per_phase = phase_set.iter().map(|p| (p.clone(), 0)).collect();

        Ok(Self {
            buckets: BTreeMap::new(),
            phase_state,
            phase_deps: deps,
            in_flight_per_phase,
            worker_affinity: HashMap::new(),
            drained_pending: Vec::new(),
        })
    }

    /// Insert items into the pool. Each item is bucketed by
    /// `(phase_id, type_id, affinity_id-or-sentinel)`. Items are
    /// pushed FIFO — caller is responsible for the order it wants
    /// dispatched (typically size-DESC).
    pub fn extend(&mut self, items: impl IntoIterator<Item = TaskInfo<I>>) {
        for item in items {
            let key = (
                item.phase_id.clone(),
                item.type_id.clone(),
                affinity_key(&item),
            );
            self.buckets
                .entry(key)
                .or_insert_with(Bucket::new)
                .items
                .push_back(item);
        }
    }

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
    pub fn view_for_worker(&self, worker_id: WorkerId) -> WorkerView<I> {
        let no_aff = no_affinity();
        let mut emitted: HashSet<BucketKey> = HashSet::new();
        let mut items: Vec<TaskInfo<I>> = Vec::new();
        let mut locators: Vec<(BucketKey, usize)> = Vec::new();

        let emit_bucket = |key: &BucketKey,
                           bucket: &Bucket<I>,
                           emitted: &mut HashSet<BucketKey>,
                           items: &mut Vec<TaskInfo<I>>,
                           locators: &mut Vec<(BucketKey, usize)>| {
            for (idx, item) in bucket.items.iter().enumerate() {
                items.push(item.clone());
                locators.push((key.clone(), idx));
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

        // Step 1: worker's pinned bucket if eligible.
        if let Some(Some(key)) = self.worker_affinity.get(&worker_id)
            && phase_active_or_draining(&key.0)
            && let Some(bucket) = self.buckets.get(key)
            && !bucket.items.is_empty()
        {
            emit_bucket(key, bucket, &mut emitted, &mut items, &mut locators);
        }

        // Step 2: unpinned typed buckets in Active phases.
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
            emit_bucket(key, bucket, &mut emitted, &mut items, &mut locators);
        }

        // Step 3: free-pool buckets in Active phases.
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
            emit_bucket(key, bucket, &mut emitted, &mut items, &mut locators);
        }

        // Step 4: any remaining bucket with items in an Active phase.
        for (key, bucket) in &self.buckets {
            if emitted.contains(key) || bucket.items.is_empty() {
                continue;
            }
            if !phase_active(&key.0) {
                continue;
            }
            emit_bucket(key, bucket, &mut emitted, &mut items, &mut locators);
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
    fn take_at(
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

        *self.in_flight_per_phase.entry(key.0.clone()).or_insert(0) += 1;

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


    /// Notify the pool that an item finished (success or failure).
    /// Decrements in-flight count; may transition the phase
    /// `Draining → Drained` (queued for `poll_drain_transitions`).
    pub fn on_item_finished(&mut self, phase_id: &PhaseId) {
        if let Some(c) = self.in_flight_per_phase.get_mut(phase_id) {
            *c = c.saturating_sub(1);
        }
        self.maybe_transition_drain(phase_id);
    }

    /// Re-queue an item that needs retry (worker death, transient
    /// failure). Inserts at the FRONT of its `(phase, type, affinity)`
    /// bucket. Decrements the phase's in-flight count (the item was
    /// in-flight and is now back in the queue) and flips the phase
    /// `Draining → Active` if needed.
    pub fn requeue(&mut self, item: TaskInfo<I>) {
        let phase_id = item.phase_id.clone();
        if let Some(c) = self.in_flight_per_phase.get_mut(&phase_id) {
            *c = c.saturating_sub(1);
        }
        let key = (
            item.phase_id.clone(),
            item.type_id.clone(),
            affinity_key(&item),
        );
        self.buckets
            .entry(key)
            .or_insert_with(Bucket::new)
            .items
            .push_front(item);
        if self.phase_state.get(&phase_id) == Some(&PhaseState::Draining) {
            self.phase_state.insert(phase_id, PhaseState::Active);
        }
    }

    /// Re-inject an item whose previous attempt has already been
    /// finalised via `on_item_finished` (so it is no longer counted as
    /// in-flight). Pushes to the BACK of its bucket and, if the phase
    /// has reached `Draining` or `Drained`, flips it back to `Active`
    /// so the newly-injected item is dispatchable. Any pending drained
    /// notification for the phase is cancelled (the phase is no longer
    /// drained).
    ///
    /// This is the right hook for manager-side retry queues that
    /// re-introduce already-finished tasks: the in-flight count is
    /// untouched, only the queue contents and phase state move.
    pub fn reinject(&mut self, item: TaskInfo<I>) {
        let phase_id = item.phase_id.clone();
        let key = (
            item.phase_id.clone(),
            item.type_id.clone(),
            affinity_key(&item),
        );
        self.buckets
            .entry(key)
            .or_insert_with(Bucket::new)
            .items
            .push_back(item);
        let current = self.phase_state.get(&phase_id).copied();
        if matches!(
            current,
            Some(PhaseState::Draining | PhaseState::Drained)
        ) {
            self.phase_state.insert(phase_id.clone(), PhaseState::Active);
            // If it was queued for drain notification, drop that entry —
            // the phase is no longer drained.
            self.drained_pending.retain(|p| p != &phase_id);
        }
    }

    /// Drain all currently queued items from the pool (without touching
    /// in-flight counts or phase state). Used by managers that need to
    /// move leftover queued items into a side queue between manager-
    /// internal phase transitions (e.g. moving NoFit items from the
    /// main phase queue into an "unassigned" bucket).
    pub fn drain_queued(&mut self) -> Vec<TaskInfo<I>> {
        let mut out = Vec::new();
        for bucket in self.buckets.values_mut() {
            while let Some(item) = bucket.items.pop_front() {
                out.push(item);
            }
        }
        out
    }

    /// Worker died / left — clear its affinity record and remove it
    /// from any bucket's `pinned_workers`.
    ///
    /// Items the worker was processing are re-queued via separate
    /// `requeue` calls from the manager — that concern is not the
    /// pool's.
    pub fn release_worker(&mut self, worker_id: WorkerId) {
        if let Some(Some(key)) = self.worker_affinity.remove(&worker_id) {
            if let Some(bucket) = self.buckets.get_mut(&key) {
                bucket.pinned_workers.retain(|w| *w != worker_id);
            }
        } else {
            // Worker had no recorded affinity; ensure no bucket holds
            // a stale reference to it (defensive, cheap given the
            // soft-pin invariant only writes via take_from_bucket).
            for bucket in self.buckets.values_mut() {
                bucket.pinned_workers.retain(|w| *w != worker_id);
            }
        }
    }

    /// Return the set of phases that just transitioned to `Drained`
    /// since the last call. One-shot per phase: once a phase is
    /// returned here, it is not re-emitted on subsequent polls
    /// (the phase stays in `Drained` until `mark_phase_done`).
    pub fn poll_drain_transitions(&mut self) -> Vec<PhaseId> {
        std::mem::take(&mut self.drained_pending)
    }

    /// Mark a phase `Done` after the manager has fired
    /// `on_phase_end` for it. Activates any `Blocked` phase whose
    /// `depends_on` set is now fully `Done`.
    pub fn mark_phase_done(&mut self, phase_id: &PhaseId) {
        self.phase_state.insert(phase_id.clone(), PhaseState::Done);
        // Activation pass: any Blocked phase whose deps are all Done
        // becomes Active. We do not recurse — a phase can only be
        // Done by an explicit `mark_phase_done` call, which the
        // manager will issue per phase.
        let candidates: Vec<PhaseId> = self
            .phase_state
            .iter()
            .filter(|(_, s)| **s == PhaseState::Blocked)
            .map(|(p, _)| p.clone())
            .collect();
        for p in candidates {
            let all_done = self
                .phase_deps
                .get(&p)
                .map(|deps| {
                    deps.iter()
                        .all(|d| self.phase_state.get(d) == Some(&PhaseState::Done))
                })
                .unwrap_or(true);
            if all_done {
                self.phase_state.insert(p, PhaseState::Active);
            }
        }
    }

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

    /// Total items remaining: queued + in-flight, all phases.
    pub fn len(&self) -> usize {
        let queued: usize = self.buckets.values().map(|b| b.items.len()).sum();
        let in_flight: usize = self
            .in_flight_per_phase
            .values()
            .map(|c| *c as usize)
            .sum();
        queued + in_flight
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

    // ---- internals ----

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

    /// Inspect a phase to decide if it should transition between
    /// `Active`, `Draining`, and `Drained`. Idempotent — safe to call
    /// from anywhere a relevant counter changed.
    fn maybe_transition_drain(&mut self, phase_id: &PhaseId) {
        let current = match self.phase_state.get(phase_id).copied() {
            Some(s) => s,
            None => return,
        };
        // Only meaningful transitions are out of Active or Draining.
        if !matches!(current, PhaseState::Active | PhaseState::Draining) {
            return;
        }
        let queued = self.queued_count(phase_id);
        let in_flight = self.in_flight(phase_id);

        let next = match (queued, in_flight) {
            (0, 0) => PhaseState::Drained,
            (0, _) => PhaseState::Draining,
            (_, _) => PhaseState::Active,
        };
        if next != current {
            self.phase_state.insert(phase_id.clone(), next);
            if next == PhaseState::Drained {
                // One-shot record. Avoid duplicates if this method
                // somehow runs twice in a row (it shouldn't, but
                // be defensive).
                if !self.drained_pending.contains(phase_id) {
                    self.drained_pending.push(phase_id.clone());
                }
            }
        }
    }

    /// Sum of queued items across all buckets of `phase_id`.
    fn queued_count(&self, phase_id: &PhaseId) -> usize {
        self.buckets
            .iter()
            .filter(|((p, _, _), _)| p == phase_id)
            .map(|(_, b)| b.items.len())
            .sum()
    }
}

#[cfg(test)]
#[path = "pending_pool_tests.rs"]
mod tests;
