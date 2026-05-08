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
        for (item, locator) in items.into_iter().zip(locators.into_iter()) {
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

    // ---- task-level dependency tracking (intra-phase, cross-phase) ----
    /// `task_id → set of unresolved prereq task_ids`. An empty set is
    /// never represented here (the entry is removed and the task moves
    /// from `blocked` into a bucket). Tasks with no `task_id` or
    /// no `task_depends_on` are not represented at all.
    task_deps: HashMap<String, HashSet<String>>,
    /// Items waiting for at least one unresolved prereq. They live
    /// here instead of in any bucket and are invisible to
    /// `view_for_worker` / `take_first_match`. On final-prereq
    /// resolution an item moves to the FRONT of its bucket (matching
    /// `requeue` semantics).
    blocked: HashMap<String, TaskInfo<I>>,
    /// Reverse index: `dep_task_id → list of dependent task_ids`.
    /// Lets `on_item_finished` and `on_item_failed_permanent` walk
    /// dependents in O(deps_per_task) instead of an O(N) scan of
    /// the whole `task_deps` map.
    dependents_of: HashMap<String, Vec<String>>,
    /// Task ids the pool has observed completing successfully via
    /// `on_item_finished(phase, Some(id))`. Used at `extend` time to
    /// pre-resolve deps already satisfied earlier in the run, and to
    /// reject duplicate `task_id`s reusing a finished one.
    completed_tasks: HashSet<String>,
    /// Task ids the pool has observed failing permanently via
    /// `on_item_failed_permanent` (or, at extend time, items whose
    /// `task_depends_on` references an already-failed task — those
    /// cascade-fail before reaching a bucket). Used by the cascade
    /// walk and by extend-time validation.
    failed_tasks: HashSet<String>,
    /// Task ids that have been dispatched (popped from a bucket) and
    /// not yet observed as terminal. Two write sites:
    ///   * `take_at` — when this pool dispatches a task with a
    ///     non-empty `task_id`.
    ///   * `mark_tasks_in_flight` — used by the post-promotion
    ///     hydration path (`populate_primary_from_cluster_state`)
    ///     to seed task_ids that are in flight on OTHER nodes,
    ///     learnt from the replicated cluster ledger.
    /// Cleared by `on_item_finished` / `on_item_failed_permanent` on
    /// terminal observation.
    ///
    /// Necessary because `extend()`'s dep-validation `known` set was
    /// previously the union of (queued ∪ blocked ∪ completed ∪
    /// failed) — which excludes in-flight tasks (popped, not yet
    /// terminal). A late `extend` whose new items reference an
    /// in-flight task_id would fail `UnknownTaskDep`. The live
    /// primary historically avoided this because `extend` is called
    /// once at startup, but the post-promotion path calls
    /// `mark_tasks_in_flight` + `extend` after some tasks have
    /// already been popped on the originating dispatcher. Including
    /// in-flight ids in the `known` set lets dependents land in
    /// `blocked` (waiting for completion) instead of failing
    /// validation.
    in_flight_tasks: HashSet<String>,
    /// Per-phase count of items currently sitting in `blocked` (not
    /// yet dispatched, waiting on unresolved prereqs). Mirrors
    /// `in_flight_per_phase` so `maybe_transition_drain` correctly
    /// distinguishes "phase truly empty" from "phase has blocked
    /// items waiting for unresolved prereqs in another phase".
    blocked_per_phase: HashMap<PhaseId, u32>,
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
            task_deps: HashMap::new(),
            blocked: HashMap::new(),
            dependents_of: HashMap::new(),
            completed_tasks: HashSet::new(),
            failed_tasks: HashSet::new(),
            in_flight_tasks: HashSet::new(),
            blocked_per_phase: HashMap::new(),
        })
    }

    /// Pre-seed `completed_tasks` with task ids the cluster has
    /// already finished. Used by the failover-resume path: when a
    /// promoted secondary rebuilds its `PendingPool` from the
    /// replicated `cluster_state` mirror, completed prereqs are
    /// filtered out of the items vec but their ids must still
    /// resolve `task_depends_on` references in the surviving items.
    /// Without this, every variant whose toolchain finished
    /// pre-promotion would land in `extend()` as `UnknownTaskDep`
    /// and the new primary would degrade to "no pending tasks".
    ///
    /// Idempotent. Must be called BEFORE `extend()` for the seeded
    /// ids to be visible to validation. Calling it later affects
    /// only future extends and the dependent-walk on subsequent
    /// `on_item_finished`.
    pub fn mark_tasks_completed(&mut self, ids: impl IntoIterator<Item = String>) {
        self.completed_tasks.extend(ids);
    }

    /// Pre-seed `in_flight_tasks` (and bump `in_flight_per_phase`) with
    /// task ids the cluster ledger reports as in flight on OTHER nodes.
    /// Used by the post-promotion path: when a secondary becomes primary,
    /// `populate_primary_from_cluster_state` walks the replicated ledger
    /// and finds tasks in the `InFlight` state — already dispatched by
    /// the previous primary to some secondary, completion not yet
    /// observed on this node. Those task_ids must satisfy
    /// `task_depends_on` validation in `extend()` so dependent variants
    /// land in `blocked` (waiting for completion) rather than fail with
    /// `UnknownTaskDep`. The phase counter is bumped so phase-lifecycle
    /// drain semantics still work — when `on_item_finished` is later
    /// called for these tasks (TaskComplete arriving via broadcast and
    /// surfacing through `note_primary_item_completed`), the counter
    /// correctly decrements and dependent phases unblock.
    ///
    /// Idempotent on repeated task_ids. Must be called BEFORE `extend()`
    /// for the seeded ids to participate in dep validation.
    pub fn mark_tasks_in_flight(
        &mut self,
        items: impl IntoIterator<Item = (String, PhaseId)>,
    ) {
        for (task_id, phase_id) in items {
            if self.in_flight_tasks.insert(task_id) {
                *self.in_flight_per_phase.entry(phase_id).or_insert(0) += 1;
            }
        }
    }

    /// Insert items into the pool. Each item is bucketed by
    /// `(phase_id, type_id, affinity_id-or-sentinel)`. Items are
    /// pushed FIFO — caller is responsible for the order it wants
    /// dispatched (typically size-DESC).
    ///
    /// Validates `task_id` uniqueness and `task_depends_on`
    /// well-formedness:
    /// * `DuplicateTaskId` — a new item's `task_id` collides with
    ///   another in the same batch, or with an existing
    ///   queued / blocked / completed / failed task.
    /// * `UnknownTaskDep` — a `task_depends_on` entry references an id
    ///   that is not present in the union of (existing pool tasks,
    ///   batch tasks, completed tasks, failed tasks).
    /// * `TaskDepCycle` — the union dep graph (existing blocked entries
    ///   + new batch) contains a cycle.
    ///
    /// On error the pool is unchanged (atomic validate-then-commit).
    /// Items whose every `task_depends_on` entry is already in
    /// `completed_tasks` are pre-resolved and pushed straight into
    /// their bucket. Items whose deps include a `failed_tasks` entry
    /// cascade-fail at extend time: their id is recorded in
    /// `failed_tasks` and the `TaskInfo` is dropped — same semantics
    /// as `on_item_failed_permanent`'s cascade.
    pub fn extend(
        &mut self,
        items: impl IntoIterator<Item = TaskInfo<I>>,
    ) -> Result<(), PendingPoolError> {
        let new_items: Vec<TaskInfo<I>> = items.into_iter().collect();

        // ---------- 1. Validate duplicate task_ids ----------
        // Duplicate within batch.
        let mut seen_in_batch: HashSet<&str> = HashSet::new();
        for item in &new_items {
            if let Some(id) = item.task_id.as_deref() {
                if !seen_in_batch.insert(id) {
                    return Err(PendingPoolError::DuplicateTaskId(id.to_string()));
                }
            }
        }
        // Duplicate against existing state.
        let existing_ids = self.collect_known_task_ids();
        for item in &new_items {
            if let Some(id) = item.task_id.as_deref()
                && existing_ids.contains(id)
            {
                return Err(PendingPoolError::DuplicateTaskId(id.to_string()));
            }
        }

        // ---------- 2. Validate every dep references a known id ----------
        // Known = existing pool tasks ∪ batch tasks ∪ completed ∪ failed.
        let mut known: HashSet<String> = existing_ids;
        for item in &new_items {
            if let Some(id) = item.task_id.as_deref() {
                known.insert(id.to_string());
            }
        }
        for item in &new_items {
            let referenced_by = match item.task_id.as_deref() {
                Some(id) => id.to_string(),
                // Anonymous task with deps: validation still applies, but
                // we have no id to report; use the path as a best-effort
                // identifier so the error message is debuggable.
                None => item.path.display().to_string(),
            };
            for dep in &item.task_depends_on {
                if !known.contains(dep) {
                    return Err(PendingPoolError::UnknownTaskDep {
                        task: dep.clone(),
                        referenced_by,
                    });
                }
            }
        }

        // ---------- 3. Cycle check (Kahn's on the union graph) ----------
        // Nodes: union of (existing blocked task_ids, batch task_ids).
        // Edges: dep → dependent. Already-completed deps are pre-resolved
        // and excluded; already-failed deps will cascade-fail (no edge).
        // Within-batch items contribute their full task_depends_on; existing
        // blocked items contribute their current `task_deps[id]` set
        // (which already excludes resolved/completed entries by construction).
        let mut indegree: HashMap<String, usize> = HashMap::new();
        let mut children_of: HashMap<String, Vec<String>> = HashMap::new();
        let pre_resolved = |dep: &str| {
            self.completed_tasks.contains(dep) || self.failed_tasks.contains(dep)
        };
        // Existing blocked nodes.
        for (id, deps) in &self.task_deps {
            indegree.entry(id.clone()).or_insert(0);
            for dep in deps {
                if pre_resolved(dep) {
                    continue;
                }
                *indegree.entry(id.clone()).or_insert(0) += 1;
                children_of
                    .entry(dep.clone())
                    .or_default()
                    .push(id.clone());
                indegree.entry(dep.clone()).or_insert(0);
            }
        }
        // New batch nodes.
        for item in &new_items {
            let id = match item.task_id.as_deref() {
                Some(s) => s.to_string(),
                None => continue, // anonymous tasks aren't graph nodes
            };
            indegree.entry(id.clone()).or_insert(0);
            for dep in &item.task_depends_on {
                if pre_resolved(dep) {
                    continue;
                }
                *indegree.entry(id.clone()).or_insert(0) += 1;
                children_of
                    .entry(dep.clone())
                    .or_default()
                    .push(id.clone());
                indegree.entry(dep.clone()).or_insert(0);
            }
        }
        // Kahn's: drain zero-indegree, decrement children, count.
        let mut queue: VecDeque<String> = indegree
            .iter()
            .filter_map(|(id, &d)| if d == 0 { Some(id.clone()) } else { None })
            .collect();
        // Deterministic order: lowest id first.
        let mut queue_vec: Vec<String> = queue.drain(..).collect();
        queue_vec.sort();
        queue.extend(queue_vec);
        let mut visited = 0usize;
        let mut residual = indegree.clone();
        while let Some(p) = queue.pop_front() {
            visited += 1;
            if let Some(children) = children_of.get(&p) {
                let mut newly_zero = Vec::new();
                for child in children {
                    let entry = residual.get_mut(child).expect("child in indegree map");
                    *entry -= 1;
                    if *entry == 0 {
                        newly_zero.push(child.clone());
                    }
                }
                newly_zero.sort();
                queue.extend(newly_zero);
            }
        }
        if visited != residual.len() {
            // Pick the lowest-id node with non-zero residual indegree as
            // the cycle start; report the SCC walk reachable from it.
            let mut start: Vec<String> = residual
                .iter()
                .filter_map(|(id, &d)| if d != 0 { Some(id.clone()) } else { None })
                .collect();
            start.sort();
            let mut cycle_walk: Vec<String> = Vec::new();
            let mut visited_walk: HashSet<String> = HashSet::new();
            if let Some(first) = start.first() {
                let mut cur = first.clone();
                while visited_walk.insert(cur.clone()) {
                    cycle_walk.push(cur.clone());
                    let next = children_of
                        .get(&cur)
                        .and_then(|cs| {
                            // Pick the smallest still-unresolved child to
                            // make the walk deterministic.
                            cs.iter()
                                .filter(|c| residual.get(*c).copied().unwrap_or(0) != 0)
                                .min()
                                .cloned()
                        });
                    match next {
                        Some(n) => cur = n,
                        None => break,
                    }
                }
            }
            return Err(PendingPoolError::TaskDepCycle(cycle_walk));
        }

        // ---------- 4. Commit: insert each item into bucket OR blocked ----------
        for item in new_items {
            self.commit_item(item);
        }
        Ok(())
    }

    /// Commit one validated item: pre-resolve `task_depends_on` against
    /// `completed_tasks` / `failed_tasks`; route to bucket, blocked,
    /// or cascaded-fail accordingly.
    fn commit_item(&mut self, item: TaskInfo<I>) {
        // Cascade-fail at extend time: if any prereq is already in
        // `failed_tasks`, this item is itself a cascaded failure.
        let any_failed_dep = item
            .task_depends_on
            .iter()
            .any(|d| self.failed_tasks.contains(d));
        if any_failed_dep {
            if let Some(id) = item.task_id.as_deref() {
                self.failed_tasks.insert(id.to_string());
            }
            // Drop the TaskInfo — extend-time cascade does not surface
            // it (the consumer hasn't given us a place to land it
            // because it specified a hard prereq that's already failed).
            return;
        }

        // Compute unresolved prereqs (ones not yet in `completed_tasks`).
        let unresolved: HashSet<String> = item
            .task_depends_on
            .iter()
            .filter(|d| !self.completed_tasks.contains(d.as_str()))
            .cloned()
            .collect();

        let task_id = item.task_id.clone();
        let phase_id = item.phase_id.clone();
        if unresolved.is_empty() || task_id.is_none() {
            // Ready (or anonymous): straight into the bucket.
            let key = (phase_id, item.type_id.clone(), affinity_key(&item));
            self.buckets
                .entry(key)
                .or_insert_with(Bucket::new)
                .items
                .push_back(item);
            return;
        }
        // Blocked: register in the dep maps and counters, NOT in any bucket.
        let id = task_id.expect("checked above");
        for dep in &unresolved {
            self.dependents_of
                .entry(dep.clone())
                .or_default()
                .push(id.clone());
        }
        self.task_deps.insert(id.clone(), unresolved);
        *self.blocked_per_phase.entry(phase_id).or_insert(0) += 1;
        self.blocked.insert(id, item);
    }

    /// Return the union of every task_id the pool currently knows
    /// about (queued in any bucket, blocked waiting on prereqs,
    /// completed, or failed). Used by `extend`'s duplicate-id check.
    fn collect_known_task_ids(&self) -> HashSet<String> {
        let mut out: HashSet<String> = HashSet::new();
        for bucket in self.buckets.values() {
            for item in &bucket.items {
                if let Some(id) = item.task_id.as_deref() {
                    out.insert(id.to_string());
                }
            }
        }
        for id in self.blocked.keys() {
            out.insert(id.clone());
        }
        for id in &self.completed_tasks {
            out.insert(id.clone());
        }
        for id in &self.failed_tasks {
            out.insert(id.clone());
        }
        for id in &self.in_flight_tasks {
            out.insert(id.clone());
        }
        out
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


    /// Notify the pool that an item completed successfully (or that
    /// the caller wants the in-flight count decremented without
    /// recording a per-task completion — pass `task_id = None`).
    ///
    /// * Decrements `in_flight_per_phase` and may transition the phase
    ///   `Draining → Drained`.
    /// * If `task_id` is `Some(id)`: marks that task as completed and
    ///   walks `dependents_of[id]`. Any dependent whose final
    ///   unresolved prereq this resolves moves from `blocked` to the
    ///   FRONT of its bucket (matching `requeue` semantics so freshly
    ///   unblocked tasks dispatch ahead of newly-extended items in the
    ///   same bucket). Dependent phases that had been `Draining` due
    ///   to all queued items being blocked elsewhere flip back to
    ///   `Active`.
    ///
    /// Pass `None` for transient failures (Recoverable retry pending):
    /// the in-flight count drops so the phase machine progresses, but
    /// no per-task completion is recorded — dependents stay blocked
    /// until either a successful retry calls this method with
    /// `Some(id)` or a permanent-fail cascade is invoked via
    /// `on_item_failed_permanent`.
    pub fn on_item_finished(&mut self, phase_id: &PhaseId, task_id: Option<&str>) {
        if let Some(c) = self.in_flight_per_phase.get_mut(phase_id) {
            *c = c.saturating_sub(1);
        }
        if let Some(id) = task_id {
            self.in_flight_tasks.remove(id);
            self.completed_tasks.insert(id.to_string());
            // Walk dependents and possibly unblock them. Collect ids
            // first to avoid borrowing `self.dependents_of` while we
            // mutate `self.blocked` / `self.task_deps`.
            let dependents = self
                .dependents_of
                .remove(id)
                .unwrap_or_default();
            for dep_id in dependents {
                let still_blocked = if let Some(remaining) =
                    self.task_deps.get_mut(&dep_id)
                {
                    remaining.remove(id);
                    !remaining.is_empty()
                } else {
                    // Already unblocked / not present — defensive no-op.
                    continue;
                };
                if still_blocked {
                    continue;
                }
                self.task_deps.remove(&dep_id);
                let item = match self.blocked.remove(&dep_id) {
                    Some(it) => it,
                    None => continue,
                };
                let dep_phase = item.phase_id.clone();
                if let Some(c) = self.blocked_per_phase.get_mut(&dep_phase) {
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
                // Unblocking grew this phase's queue: if it was
                // `Draining` only because everything was blocked, flip
                // it back to `Active`. Mirrors `requeue` behaviour.
                if self.phase_state.get(&dep_phase) == Some(&PhaseState::Draining) {
                    self.phase_state.insert(dep_phase.clone(), PhaseState::Active);
                }
                // A drained-pending entry for this phase is now stale —
                // the phase is no longer drained.
                self.drained_pending.retain(|p| p != &dep_phase);
            }
        }
        self.maybe_transition_drain(phase_id);
    }

    /// Notify the pool that a task has terminated PERMANENTLY (e.g.
    /// retry budget exhausted or a NonRecoverable error). Cascades
    /// the failure to every transitive dependent so dependents that
    /// can never succeed do not sit in `blocked` forever.
    ///
    /// Returns the `TaskInfo` of every cascaded dependent so the
    /// caller can update its own per-task ledgers (failed-tasks set,
    /// metrics, observability hooks). The caller's own task whose
    /// failure triggered this is NOT in the returned vec — it has
    /// already been removed from in-flight via the normal task-event
    /// path; this method just records its id and walks the cascade.
    ///
    /// Side effects:
    /// * `task_id` and every cascaded dependent id are added to
    ///   `failed_tasks`.
    /// * `in_flight_per_phase[phase_id]` is decremented by one (the
    ///   originating task was in-flight).
    /// * Cascaded dependents are removed from `blocked` and their
    ///   `blocked_per_phase` entries decremented.
    /// * Drain transitions fire for every phase whose blocked-set
    ///   was reduced (the originating phase plus every distinct
    ///   cascaded phase).
    pub fn on_item_failed_permanent(
        &mut self,
        phase_id: &PhaseId,
        task_id: &str,
    ) -> Vec<TaskInfo<I>> {
        if let Some(c) = self.in_flight_per_phase.get_mut(phase_id) {
            *c = c.saturating_sub(1);
        }
        self.in_flight_tasks.remove(task_id);
        self.failed_tasks.insert(task_id.to_string());

        let mut cascaded: Vec<TaskInfo<I>> = Vec::new();
        let mut affected_phases: HashSet<PhaseId> = HashSet::new();
        affected_phases.insert(phase_id.clone());

        // BFS over `dependents_of`. Every dependent we reach is
        // unreachable for any successful path — it cannot satisfy its
        // dep on a permanently-failed prereq. Cascade-fail it.
        let mut frontier: VecDeque<String> = VecDeque::new();
        frontier.push_back(task_id.to_string());
        while let Some(failed_id) = frontier.pop_front() {
            let dependents = self.dependents_of.remove(&failed_id).unwrap_or_default();
            for dep_id in dependents {
                if !self.failed_tasks.insert(dep_id.clone()) {
                    // Already cascaded via a different path — its
                    // blocked entry is gone too; skip.
                    continue;
                }
                self.task_deps.remove(&dep_id);
                if let Some(item) = self.blocked.remove(&dep_id) {
                    let dep_phase = item.phase_id.clone();
                    if let Some(c) = self.blocked_per_phase.get_mut(&dep_phase) {
                        *c = c.saturating_sub(1);
                    }
                    affected_phases.insert(dep_phase);
                    cascaded.push(item);
                }
                frontier.push_back(dep_id);
            }
        }

        for ph in &affected_phases {
            self.maybe_transition_drain(ph);
        }
        cascaded
    }

    /// Notify the pool that a task has been dispatched outside the
    /// `pop_for_worker` / `take_from_view` path (which already do the
    /// in-flight bookkeeping). Pair with [`on_item_finished`] when the
    /// task completes. Used by the promoted secondary, which
    /// extracts items via [`take_first_match`] (a removal primitive
    /// that does not touch in-flight counters) but needs the phase
    /// machine to observe the dispatch so a `Draining` transition
    /// fires only after the cluster reports the item finished.
    pub fn mark_in_flight(&mut self, phase_id: &PhaseId) {
        *self.in_flight_per_phase.entry(phase_id.clone()).or_insert(0) += 1;
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
    /// has progressed past `Active` (`Draining`, `Drained`, or `Done`),
    /// flips it back to `Active` so the newly-injected item is
    /// dispatchable. Any pending drained notification for the phase
    /// is cancelled (the phase is no longer drained).
    ///
    /// This is the right hook for manager-side retry queues that
    /// re-introduce already-finished tasks: the in-flight count is
    /// untouched, only the queue contents and phase state move.
    /// Reinjecting after `Done` unwinds the phase into `Active`
    /// without re-firing `on_phase_start` — the manager owns
    /// lifecycle bookkeeping (phase_started_emitted) and decides
    /// whether the second-pass dispatch is observable to consumers.
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
            Some(PhaseState::Draining | PhaseState::Drained | PhaseState::Done)
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

    /// Mark every currently-`Active` or `Draining` phase that has no
    /// queued AND no in-flight items as `Drained`, pushing each onto
    /// `drained_pending` so the manager's `process_phase_lifecycle`
    /// pass observes them and cascades into `mark_phase_done` plus
    /// dependent-phase activation. Idempotent.
    ///
    /// Why this exists: `maybe_transition_drain` only runs when an
    /// item is removed from the pool (`take_at`) or finished
    /// (`on_item_finished`). A phase that started `Active` (because
    /// it had no upstream deps) but never received any items would
    /// otherwise stay `Active` forever, holding `Blocked` dependents
    /// that own the actual work. Multi-phase task definitions where
    /// every item lives in a non-zero-indexed phase trip this on
    /// startup; so does any run where `--skip-existing` (or
    /// equivalent task-side filtering) leaves an early phase
    /// completely empty.
    ///
    /// Callers should invoke this after the initial `extend()` and
    /// inside the lifecycle cascade in the manager — newly-`Active`
    /// dependents may themselves be empty and require the same
    /// transition before the cascade can continue.
    pub fn drain_empty_active_phases(&mut self) {
        let candidates: Vec<PhaseId> = self
            .phase_state
            .iter()
            .filter(|(_, s)| matches!(**s, PhaseState::Active | PhaseState::Draining))
            .map(|(p, _)| p.clone())
            .collect();
        for p in &candidates {
            self.maybe_transition_drain(p);
        }
    }

    /// Inspect a phase to decide if it should transition between
    /// `Active`, `Draining`, and `Drained`. Idempotent — safe to call
    /// from anywhere a relevant counter changed.
    ///
    /// A phase is `Drained` only when ALL three of `queued`,
    /// `in_flight`, AND `blocked_per_phase` are zero — a non-zero
    /// blocked count means the phase still has items waiting on
    /// unresolved task-level prereqs (typically in another phase) and
    /// must not be considered done. `Draining` covers the case where
    /// the queue is empty but in-flight or blocked items remain.
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
        let blocked = self
            .blocked_per_phase
            .get(phase_id)
            .copied()
            .unwrap_or(0);

        let next = match (queued, in_flight, blocked) {
            (0, 0, 0) => PhaseState::Drained,
            (0, _, _) => PhaseState::Draining,
            (_, _, _) => PhaseState::Active,
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
