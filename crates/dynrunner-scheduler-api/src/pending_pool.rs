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
type BucketKey = (PhaseId, TypeId, AffinityId);

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
        let item = self.take_from_bucket(&key, worker_id)?;
        Some(item)
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

    /// Drop every queued item for which `pred` returns `false`. Iterates
    /// every bucket in deterministic key order; empty buckets are left
    /// in place (cheap, and matches the lazy-creation pattern in
    /// `extend`). Does NOT touch in-flight items: removal of an item
    /// already handed to a worker is the manager's concern, surfaced
    /// via `release_worker` / `on_item_finished`.
    ///
    /// Used by callers (e.g. SLURM-primary) to drop items completed
    /// elsewhere in the cluster without disturbing the phase machine.
    pub fn retain<F>(&mut self, mut pred: F)
    where
        F: FnMut(&TaskInfo<I>) -> bool,
    {
        for bucket in self.buckets.values_mut() {
            bucket.items.retain(|item| pred(item));
        }
    }

    /// Find the first queued item (in bucket-key order, FIFO within a
    /// bucket) for which `pred` returns `true`, remove it from its
    /// bucket and return it. Returns `None` if no item matches.
    ///
    /// Does NOT update in-flight counts or worker affinity — this is
    /// a *removal* primitive, not a *dispatch* primitive. Intended for
    /// callers (SLURM-promoted primary) that need to extract a task
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

    // ---- internals ----

    /// Pick a bucket for `worker_id` per the soft-pin algorithm,
    /// returning the bucket key (or `None` if nothing is dispatchable).
    /// Pure: doesn't mutate state — `take_from_bucket` performs
    /// the actual claim.
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

    /// Pop the front item of `key` for `worker_id`, updating affinity,
    /// in-flight count, and phase state as required.
    fn take_from_bucket(
        &mut self,
        key: &BucketKey,
        worker_id: WorkerId,
    ) -> Option<TaskInfo<I>> {
        let no_aff = no_affinity();
        let bucket = self.buckets.get_mut(key)?;
        let item = bucket.items.pop_front()?;

        // Soft-pin update: only typed (non-free-pool) buckets get pinned.
        if key.2 != no_aff {
            if !bucket.pinned_workers.contains(&worker_id) {
                bucket.pinned_workers.push(worker_id);
            }
            self.worker_affinity.insert(worker_id, Some(key.clone()));
        } else {
            // Free-pool dispatch does not record affinity. If the worker
            // had affinity to a now-empty (or vanished) bucket, leave
            // its slot as None or absent — both behave identically in
            // the choose path.
            self.worker_affinity.entry(worker_id).or_insert(None);
        }

        // In-flight bookkeeping.
        *self.in_flight_per_phase.entry(key.0.clone()).or_insert(0) += 1;

        // If the bucket is now empty, clear its pinned workers'
        // affinity slots so they're free-pool eligible next call.
        if bucket.items.is_empty() {
            let drained_pinners = std::mem::take(&mut bucket.pinned_workers);
            for w in drained_pinners {
                if let Some(slot) = self.worker_affinity.get_mut(&w)
                    && slot.as_ref() == Some(key)
                {
                    *slot = None;
                }
            }
            // Phase may have just emptied of queued work.
            self.maybe_transition_drain(&key.0);
        }

        Some(item)
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
