//! [`PendingPool`] struct definition + constructor + cluster-state
//! pre-seeding (`mark_tasks_completed`, `mark_tasks_in_flight`).
//!
//! This module owns the data layout and graph-validation pass that
//! turns a `(phases, deps)` pair into a fresh empty pool. Every other
//! operation against an existing pool lives in a sibling submodule
//! that adds an `impl PendingPool<I>` block.
//!
//! The struct fields are `pub(super)` so sibling submodules can mutate
//! them without going through accessors — there is no abstraction
//! boundary internally between (e.g.) the dispatch path and the
//! lifecycle path; both freely operate on the same private state.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use dynrunner_core::{Identifier, PhaseId, TaskInfo};

use super::types::{Bucket, BucketKey, PendingPoolError, PhaseState};

/// Items grouped by `(phase, type, affinity)` plus the phase state
/// machine. See module-level docs for ownership boundaries.
#[derive(Debug)]
pub struct PendingPool<I: Identifier> {
    /// `BTreeMap` (not `HashMap`) so iteration order is deterministic
    /// — useful for tests and for diagnostic logging in managers.
    pub(super) buckets: BTreeMap<BucketKey, Bucket<I>>,
    pub(super) phase_state: HashMap<PhaseId, PhaseState>,
    pub(super) phase_deps: HashMap<PhaseId, Vec<PhaseId>>,
    pub(super) in_flight_per_phase: HashMap<PhaseId, u32>,
    /// Worker → currently affine bucket. `None` slot means the
    /// worker is in the pool's worker set but free of any pin.
    pub(super) worker_affinity: HashMap<dynrunner_core::WorkerId, Option<BucketKey>>,
    /// Phases that transitioned to `Drained` since the last
    /// `poll_drain_transitions` call. Drained transitions are
    /// one-shot per phase: once polled they aren't re-emitted until
    /// the phase makes a fresh trip through the state machine
    /// (which does not happen in the standard lifecycle).
    pub(super) drained_pending: Vec<PhaseId>,

    // ---- task-level dependency tracking (intra-phase, cross-phase) ----
    /// `task_id → set of unresolved prereq task_ids`. An empty set is
    /// never represented here (the entry is removed and the task moves
    /// from `blocked` into a bucket). Tasks with no `task_id` or
    /// no `task_depends_on` are not represented at all.
    pub(super) task_deps: HashMap<String, HashSet<String>>,
    /// Items waiting for at least one unresolved prereq. They live
    /// here instead of in any bucket and are invisible to
    /// `view_for_worker` / `take_first_match`. On final-prereq
    /// resolution an item moves to the FRONT of its bucket (matching
    /// `requeue` semantics).
    pub(super) blocked: HashMap<String, TaskInfo<I>>,
    /// Reverse index: `dep_task_id → list of dependent task_ids`.
    /// Lets `on_item_finished` and `on_item_failed_permanent` walk
    /// dependents in O(deps_per_task) instead of an O(N) scan of
    /// the whole `task_deps` map.
    pub(super) dependents_of: HashMap<String, Vec<String>>,
    /// Task ids the pool has observed completing successfully via
    /// `on_item_finished(phase, Some(id))`. Used at `extend` time to
    /// pre-resolve deps already satisfied earlier in the run, and to
    /// reject duplicate `task_id`s reusing a finished one.
    pub(super) completed_tasks: HashSet<String>,
    /// Task ids the pool has observed failing permanently via
    /// `on_item_failed_permanent` (or, at extend time, items whose
    /// `task_depends_on` references an already-failed task — those
    /// cascade-fail before reaching a bucket). Used by the cascade
    /// walk and by extend-time validation.
    pub(super) failed_tasks: HashSet<String>,
    /// Task ids that have been dispatched (popped from a bucket) and
    /// not yet observed as terminal. Two write sites:
    ///   * `take_at` — when this pool dispatches a task with a
    ///     non-empty `task_id`.
    ///   * `mark_tasks_in_flight` — used by the post-promotion
    ///     hydration path (`populate_primary_from_cluster_state`)
    ///     to seed task_ids that are in flight on OTHER nodes,
    ///     learnt from the replicated cluster ledger.
    ///
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
    pub(super) in_flight_tasks: HashSet<String>,
    /// Per-phase count of items currently sitting in `blocked` (not
    /// yet dispatched, waiting on unresolved prereqs). Mirrors
    /// `in_flight_per_phase` so `maybe_transition_drain` correctly
    /// distinguishes "phase truly empty" from "phase has blocked
    /// items waiting for unresolved prereqs in another phase".
    pub(super) blocked_per_phase: HashMap<PhaseId, u32>,
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

    /// Pre-seed `failed_tasks` with task ids that have terminated
    /// permanently (or are about to — e.g. the manager classified them
    /// `invalid_task` during ingest). Sibling of `mark_tasks_completed`
    /// for the failure side of the pre-seed contract.
    ///
    /// Two coupled effects on a SUBSEQUENT `extend`:
    ///   * The seeded ids count as "known", so a survivor whose
    ///     `task_depends_on` references one of them passes
    ///     dep-existence validation instead of failing `UnknownTaskDep`.
    ///   * `commit_item`'s extend-time cascade fires: any survivor whose
    ///     `task_depends_on` references a seeded id is itself recorded as
    ///     failed and dropped (it can never satisfy a prereq that has
    ///     already failed). Same semantics as the runtime
    ///     `on_item_failed_permanent` cascade, applied at ingest.
    ///
    /// Used by the manager's ingest path: tasks the dependency-existence
    /// partition flagged `invalid_deps` are seeded here BEFORE
    /// `extend(valid)` so their dependents cascade-drop locally (the
    /// manager broadcasts the terminal `InvalidTask` + the cascade into
    /// the CRDT separately). Idempotent on repeated ids.
    pub fn mark_tasks_failed(&mut self, ids: impl IntoIterator<Item = String>) {
        self.failed_tasks.extend(ids);
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
                let count = self
                    .in_flight_per_phase
                    .entry(phase_id.clone())
                    .or_insert(0);
                *count += 1;
                tracing::debug!(
                    phase = %phase_id,
                    new_in_flight = *count,
                    "pool: in_flight +1 (mark_tasks_in_flight; post-promotion hydration)"
                );
            }
        }
    }
}
