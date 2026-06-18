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
use std::sync::Arc;

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
    pub(super) blocked: HashMap<String, Arc<TaskInfo<I>>>,
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
    /// Task ids whose latest attempt terminally failed at the manager
    /// level but whose PERMANENCE is still pending the manager's
    /// per-phase retry decision (the retry buckets run at the phase's
    /// drain edge). Value = the task's phase, so the drain-edge
    /// finalization can promote exactly the roots whose phase is
    /// deciding. Written by [`PendingPool::on_item_failed_pending_retry`];
    /// cleared by [`PendingPool::reinject`] (revival — the bucket gave
    /// the task another pass) and by the promotions
    /// ([`PendingPool::finalize_soft_failures`] /
    /// [`PendingPool::on_item_failed_permanent`], which move the id
    /// into `failed_tasks`). Read by the drain gate
    /// (`maybe_transition_drain`): a blocked dependent doomed by a
    /// same-phase soft-failed prereq must not hold the phase open —
    /// otherwise the drain edge (where the retry-or-cascade decision
    /// lives) is unreachable and the run wedges forever (the
    /// blocked-dependent hang this field exists to break).
    pub(super) soft_failed: HashMap<String, PhaseId>,
    /// Task ids the pool knows ONLY as terminal-DORMANT roots: tasks
    /// whose latest attempt terminated in the operator-resolvable
    /// class (`Unfulfillable`) — neither completed (their dependents'
    /// deps stay UNRESOLVED, so dependents land in `blocked`), nor
    /// failed (no extend-time cascade, no drain-gate doom: their
    /// dependents are LIVE blocked work that legitimately holds the
    /// run open), nor in flight. Written by
    /// [`PendingPool::mark_tasks_dormant`] (the hydration pre-seed —
    /// on the live path the dormant root's id was already known to
    /// `extend` from its time in a bucket, so no marker was needed);
    /// cleared by [`PendingPool::reinject`] (revival — the operator
    /// reinject / fulfillability matcher gave the root another pass,
    /// so its id is once again known through its bucket entry). Read
    /// only by the `extend`/`partition_ingest` known-id collector so
    /// dependents resolve their `task_depends_on` reference instead
    /// of failing `UnknownTaskDep`.
    pub(super) dormant_tasks: HashSet<String>,
    /// Task ids of `TaskKind::SecondaryAffine` prereqs — the AFFINE-DEP
    /// exclusion set. A `SecondaryAffine` task is the per-secondary toolchain
    /// IMPORT primitive: it runs PER-SECONDARY (each node imports locally) and
    /// its readiness is tracked by the primary's per-secondary affine BITVECTOR
    /// plus the per-secondary queue order, NOT a single global terminal. So an
    /// affine dep must NOT block its dependent work task in this global pool
    /// (no global terminal satisfies it) — the work task is POOL-READY when its
    /// NON-affine deps are satisfied, and is then routed PER-SECONDARY by the
    /// primary's affine scheduler. Two coupled read effects, both keyed on this
    /// set (populated by [`PendingPool::mark_affine_prereqs`] from the primary's
    /// `cluster_state` def kinds at spawn + hydrate, BEFORE the referencing
    /// `extend`):
    ///   - [`PendingPool::commit_item`] EXCLUDES an affine dep from a work
    ///     task's unresolved blocking set (so the work task is ready on its
    ///     non-affine deps alone, never blocked-forever on an affine dep with no
    ///     global terminal); and
    ///   - the worker dispatch view ([`PendingPool::dispatch_eligible`])
    ///     EXCLUDES a work task that HAS an affine dep (so the global pool never
    ///     dispatches it to a secondary that has not imported its toolchain —
    ///     the head-of-line-blocking bug the per-secondary model fixes). Such a
    ///     work task dispatches ONLY through the primary's per-secondary affine
    ///     queue, which runs the import THEN the work in order on the chosen
    ///     secondary.
    ///
    /// An affine TASK itself is already non-worker-assignable (its kind), so it
    /// never enters the worker view nor `in_flight_per_phase` here (it is
    /// dispatched by-hash from the per-secondary queue, never `take_selected`
    /// from a bucket) — the phase drains on its WORK tasks, the import being a
    /// transitively-required dep that ran as needed.
    pub(super) affine_prereq_ids: HashSet<String>,
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
    /// Bring-up FORMATION-WINDOW reservation overlay: tags each queued
    /// task with the member it is reserved for, so a first-confirmed
    /// member's idle workers drain only their own pre-computed share
    /// instead of the whole global pool while late members are still
    /// forming (the #494 14/14/0 pack). Inert (`active == false`) outside
    /// the bring-up window — the local single-node manager never opens
    /// it. See [`super::reservation`] for the contract.
    pub(super) reservation: super::reservation::TaskReservation,
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
        let mut indegree: HashMap<PhaseId, usize> =
            phase_set.iter().map(|p| (p.clone(), 0usize)).collect();
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
                .unwrap_or_else(|| phase_set.iter().next().cloned().expect("non-empty phases"));
            return Err(PendingPoolError::DependencyCycle(culprit));
        }

        // Initial state: Active iff the phase has zero deps.
        let mut phase_state = HashMap::with_capacity(phase_set.len());
        for p in &phase_set {
            let blocked = deps.get(p).is_some_and(|v| !v.is_empty());
            phase_state.insert(
                p.clone(),
                if blocked {
                    PhaseState::Blocked
                } else {
                    PhaseState::Active
                },
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
            soft_failed: HashMap::new(),
            dormant_tasks: HashSet::new(),
            affine_prereq_ids: HashSet::new(),
            in_flight_tasks: HashSet::new(),
            blocked_per_phase: HashMap::new(),
            reservation: super::reservation::TaskReservation::default(),
        })
    }

    /// Mark the listed phases as `PhaseSpec.barrier=False`: their initial
    /// state flips from `Blocked` → `Active` regardless of `depends_on`,
    /// authorizing the scheduler to dispatch tasks from them as soon as
    /// each task's per-task `task_depends_on` resolves (the per-task
    /// graph still gates individual readiness; only the EXTRA
    /// phase-level wait is dropped). Idempotent. Unknown phase ids are
    /// silently ignored (a barrier flag for a phase not in the pool's
    /// tracked set is not the pool's concern).
    ///
    /// Single concern: an initial-state override the manager applies once
    /// at pool-construction time, reflecting the consumer's per-phase
    /// `PhaseSpec.barrier` declaration. The pool itself does not own the
    /// barrier-set source of truth — the manager carries the flags from
    /// `LoadedTopology` (the pyo3 extractor) and replicates them via
    /// `ClusterMutation::PhaseNoBarrierSet` for failover symmetry. Phases
    /// not flipped here keep their default initial state (`Active` iff
    /// no deps, else `Blocked`) — the documented strict-barrier
    /// behaviour every existing consumer relies on.
    ///
    /// Caller contract: call BEFORE the pool sees any items (`extend` /
    /// `mark_in_flight` / `seed_completed_phases`). Calling later is
    /// defensively-safe (only `Blocked` flips, no in-flight bookkeeping
    /// disturbed) but conceptually outside the per-phase-config
    /// initialisation window.
    pub fn set_no_barrier_phases(&mut self, phases: impl IntoIterator<Item = PhaseId>) {
        for phase_id in phases {
            // Only flip phases the pool actually tracks; an unknown phase
            // is silently ignored (matches `seed_completed_phases`'s
            // defensive shape — barrier flags from the topology and the
            // pool's phase set are both derived from `get_phases()` so a
            // mismatch is a defensive guard, not an expected path).
            if let Some(state) = self.phase_state.get_mut(&phase_id)
                && *state == super::types::PhaseState::Blocked
            {
                *state = super::types::PhaseState::Active;
            }
        }
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

    /// Pre-seed `soft_failed` with task ids whose latest attempt
    /// terminally FAILED with the failure's PERMANENCE still pending the
    /// per-phase drain-edge retry decision. The hydration twin of
    /// [`PendingPool::on_item_failed_pending_retry`] (which additionally
    /// owns the in-flight decrement the live wire-terminal path needs;
    /// a hydrated `Failed` entry was never counted in flight here, so
    /// this seed touches only the marker).
    ///
    /// Effects on a SUBSEQUENT `extend`, completing the per-class
    /// pre-seed contract (`mark_tasks_completed` = dep-satisfying,
    /// `mark_tasks_failed` = extend-time cascade, this = decision
    /// PENDING):
    ///   * The seeded ids count as "known", so a dependent whose
    ///     `task_depends_on` references one passes dep-existence
    ///     validation instead of failing `UnknownTaskDep`.
    ///   * The dep stays UNRESOLVED, so the dependent lands in
    ///     `blocked` — neither dispatchable (the prereq never
    ///     succeeded) nor cascade-failed (the drain edge may yet
    ///     revive the prereq).
    ///
    /// From there the standard drain-edge machinery owns the outcome,
    /// identically to a live-observed soft failure: the drain gate
    /// discounts the doomed dependents so the edge is reachable, a
    /// retry-bucket [`PendingPool::reinject`] revives the root
    /// (clearing the marker), and a declined budget promotes it through
    /// [`PendingPool::finalize_soft_failures`] (cascading the
    /// dependents).
    ///
    /// An id already PERMANENTLY failed keeps its `failed_tasks`
    /// membership and gets no marker — the same idempotence rule the
    /// live `on_item_failed_pending_retry` applies. Must be called
    /// BEFORE `extend()` for the seeded ids to participate in dep
    /// validation.
    pub fn mark_tasks_failed_pending_retry(
        &mut self,
        items: impl IntoIterator<Item = (String, PhaseId)>,
    ) {
        for (task_id, phase_id) in items {
            if !self.failed_tasks.contains(&task_id) {
                self.soft_failed.insert(task_id, phase_id);
            }
        }
    }

    /// Pre-seed the terminal-DORMANT known set with task ids whose
    /// latest attempt terminated in the operator-resolvable class
    /// (`Unfulfillable`): revivable via [`PendingPool::reinject`], with
    /// dependents deliberately held BLOCKED until then. Sibling of
    /// [`Self::mark_tasks_failed_pending_retry`] for the failure class
    /// whose revival decision belongs to the OPERATOR (reinject command
    /// / fulfillability matcher), not the phase's drain-edge buckets.
    ///
    /// Single effect: the seeded ids count as "known" to a subsequent
    /// `extend`, so a dependent's `task_depends_on` reference resolves
    /// (landing it in `blocked`) instead of failing `UnknownTaskDep`.
    /// No marker enters `soft_failed` or `failed_tasks`: the blocked
    /// dependents are LIVE work — they hold their phase open (the
    /// dormancy contract) and are never doomed by the drain gate or
    /// the finalize cascade. Must be called BEFORE `extend()`.
    pub fn mark_tasks_dormant(&mut self, ids: impl IntoIterator<Item = String>) {
        self.dormant_tasks.extend(ids);
    }

    /// Register `ids` as `TaskKind::SecondaryAffine` PREREQ task ids — the
    /// affine-dep exclusion set (see the `affine_prereq_ids` field doc). The
    /// per-class pre-seed sibling of [`Self::mark_tasks_completed`] /
    /// [`Self::mark_tasks_dormant`]: the primary populates it from its
    /// `cluster_state` def kinds at spawn + hydrate, BEFORE the referencing
    /// [`Self::extend`], so a work task's affine deps are EXCLUDED from its
    /// blocking set at commit (the work task is ready on its non-affine deps)
    /// AND a work task that HAS an affine dep is withheld from the global
    /// worker view (it dispatches only via the primary's per-secondary affine
    /// queue).
    ///
    /// Idempotent on repeated ids. Additive: a run with no `SecondaryAffine`
    /// task seeds an empty set, so every non-affine work task's blocking set +
    /// view eligibility are byte-identical to the pre-affine behaviour (the
    /// baseline-preservation guarantee).
    pub fn mark_affine_prereqs(&mut self, ids: impl IntoIterator<Item = String>) {
        self.affine_prereq_ids.extend(ids);
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
    pub fn mark_tasks_in_flight(&mut self, items: impl IntoIterator<Item = (String, PhaseId)>) {
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
