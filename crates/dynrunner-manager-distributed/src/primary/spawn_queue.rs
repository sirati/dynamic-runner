//! Publication-ordering queue for runtime `spawn_tasks` (CL-A8).
//!
//! Single concern: ORDER the publication of runtime-spawned tasks so a
//! task's definition is published (broadcast as part of a `TasksSpawned`
//! batch) only once EVERY task it depends on is already DEFINED — i.e. its
//! prerequisite's `(phase_id, task_id)` identity resolves, either in the
//! replicated ledger or in the same admission pass.
//!
//! Why this exists (CL-A8): the L5 def-id stamp resolves an INTRA-batch
//! forward-ref (a dependent listed before its prerequisite IN THE SAME
//! batch) because the originate choke point reserves every task's def id
//! before resolving any dep. A CROSS-batch forward-ref — a task published
//! in batch K whose prerequisite is defined only in a LATER batch K+1 —
//! has no def to resolve against at batch K's publication, so the spawn
//! validator (`validate_spawn_tasks`) would reject it as
//! `UnknownDependency`. For a topologically-spawned graph (deps published
//! before dependents) this never happens and the queue is a transparent
//! pass-through; this module makes the framework robust to out-of-order /
//! incremental spawning by PARKING the early dependent until its
//! prerequisite lands, then releasing it for publication.
//!
//! Module boundary:
//!   * OWNS: the parked-task set and the release-on-definition ordering.
//!     The public surface is two operations — [`PendingSpawnQueue::admit`]
//!     (the per-batch publication gate) and [`PendingSpawnQueue::drain`]
//!     (the run-end completeness sweep). Neither operation knows anything
//!     about the ledger, the broadcast, the pool, or the validator: the
//!     caller supplies a pure `is_defined(&PhaseId, &str) -> bool`
//!     predicate (its own ledger probe) and consumes the returned
//!     publish-now / still-parked task lists.
//!   * DOES NOT OWN: what "defined" means (the caller's ledger probe), the
//!     `TasksSpawned` broadcast, the duplicate/cycle/barrier validation
//!     (`validate_spawn_tasks` still runs on the released set), or the
//!     loud-fail surfacing of a never-defined dep (the caller routes the
//!     drained leftovers into the existing spawn-rejection backstop).
//!
//! NOT a silent strand (CL-A8): this is a publication-ORDERING mechanism
//! over an already-validated set, not a new admission policy. A task whose
//! prerequisite is GENUINELY never defined (a producer bug) stays parked
//! and is surfaced — LOUD — by [`PendingSpawnQueue::drain`] at the run's
//! drain edge, which the caller feeds into the same
//! `RunError::SpawnRejected` net a wholesale-rejected batch already takes.
//! A cross-batch dependency CYCLE (A waits on B, B waits on A, neither ever
//! defined) likewise never releases and is caught by the same drain sweep.
//!
//! Primary-LOCAL, NOT a CRDT field: like the dispatch `PendingPool` and the
//! chunked-spawn continuation queue, the parked set is reconstructable
//! scheduling state, not authoritative cluster state. It only ever holds
//! tasks that have NOT yet been published (no `TasksSpawned` broadcast, no
//! ledger entry, no def id), so a promoted primary rebuilds an EMPTY queue:
//! every task the failed primary had already published is in the
//! replicated ledger (and re-spawned idempotently by the consumer's
//! `on_phase_end` replay → `DuplicateTaskHash`, dropped), and every task
//! still parked was never broadcast and is re-originated by that same
//! consumer replay through the fresh queue. There is no parked state to
//! inherit, so promotion starts the queue empty (the constructor default).

use std::collections::HashSet;

use dynrunner_core::{Identifier, PhaseId, TaskInfo};

/// A still-parked task paired with the `(phase_id, task_id)` identities of
/// the deps it is still waiting on — the [`PendingSpawnQueue::drain`]
/// element shape the run-end loud-fail sweep consumes.
pub(crate) type StrandedSpawn<I> = (TaskInfo<I>, Vec<(PhaseId, String)>);

/// One task parked awaiting the definition of its prerequisites.
struct Parked<I: Identifier> {
    task: TaskInfo<I>,
    /// The subset of this task's deps whose `(phase_id, task_id)` identity
    /// was NOT yet defined when the task was admitted. A dep leaves this
    /// set the moment its identity becomes defined (resolves in the ledger
    /// OR is released earlier in the same admission pass). Empty ⇒ the task
    /// is ready and is released for publication.
    unmet: HashSet<(PhaseId, String)>,
}

/// Primary-local queue ordering runtime-spawn publication so a task
/// publishes only once all its deps are defined. See the module doc.
pub(crate) struct PendingSpawnQueue<I: Identifier> {
    parked: Vec<Parked<I>>,
}

impl<I: Identifier> Default for PendingSpawnQueue<I> {
    fn default() -> Self {
        Self { parked: Vec::new() }
    }
}

impl<I: Identifier> PendingSpawnQueue<I> {
    /// A fresh, empty queue. The promotion path rebuilds via this default
    /// (see the module doc: no parked state is authoritative, so a promoted
    /// primary starts empty).
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// No task is currently parked. Test-only observability — production
    /// callers act on `admit`'s released set and `drain`'s leftovers, never
    /// on a bare emptiness probe.
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.parked.is_empty()
    }

    /// Admit a fresh batch of runtime-spawned tasks and return the subset
    /// that is READY TO PUBLISH right now — every task whose deps are all
    /// defined, in a stable order (already-parked releases first, then the
    /// incoming batch in arrival order). Tasks with a not-yet-defined dep
    /// are PARKED internally and released by a later `admit` once the
    /// missing prerequisite lands.
    ///
    /// `is_defined(&PhaseId, &str) -> bool` is the caller's ledger probe:
    /// does the replicated ledger already carry a task with that full
    /// `(phase_id, task_id)` identity? (The caller passes its own
    /// `task_hash_for_dep(..).is_some()` here, the SAME predicate the spawn
    /// validator's `is_known_task_id` closure uses, so the queue's "defined"
    /// notion and the validator's agree.)
    ///
    /// Release is a fixpoint over the candidate pool (currently-parked ∪
    /// `incoming`): a dep counts as defined if `is_defined` holds OR its
    /// identity is among the tasks released THIS pass. So a batch carrying
    /// `[dependent, prerequisite]` (or even just the prerequisite that a
    /// PRIOR batch's dependent was waiting on) releases both — the
    /// cross-batch forward-ref resolves the moment the prerequisite is
    /// admitted. The common topological case (deps admitted before
    /// dependents) releases the whole batch in one pass: the queue is a
    /// transparent pass-through.
    ///
    /// The released tasks are NOT yet validated — the caller still runs
    /// `validate_spawn_tasks` (duplicate / barrier / and, for any dep STILL
    /// unresolved at publication, `UnknownDependency`) on the returned set.
    /// The queue only guarantees that a released task's deps are all defined
    /// per `is_defined`, so no released task fails the forward-ref class.
    pub(crate) fn admit<F>(&mut self, incoming: Vec<TaskInfo<I>>, is_defined: F) -> Vec<TaskInfo<I>>
    where
        F: Fn(&PhaseId, &str) -> bool,
    {
        // Candidate pool: the incoming batch FIRST, then every
        // currently-parked task. Incoming-first is the index-stability
        // contract: in the common case where the whole incoming batch
        // releases (nothing parks), the emitted released set leads with the
        // incoming tasks IN INPUT ORDER, so the per-index `SpawnError` reply
        // (`base_index + i`) maps byte-identically to the caller's input —
        // the pre-CL-A8 contract. A released-from-park task (its caller's
        // reply already fired Ok at park time) lands AFTER, never shifting an
        // incoming task's index. Parked entries are re-evaluated each admit
        // because a dep unmet at their original admission may now be defined
        // in the ledger (a prior batch — or the cold seed — published it
        // off-band) or by THIS incoming batch.
        let parked = std::mem::take(&mut self.parked);
        let mut candidates: Vec<Parked<I>> = Vec::with_capacity(parked.len() + incoming.len());
        for task in incoming {
            // Compute the as-yet-undefined dep identities against the LEDGER
            // probe only; the within-pass release loop below removes the
            // ones the pass itself defines.
            let unmet: HashSet<(PhaseId, String)> = task
                .task_depends_on
                .iter()
                .filter(|dep| !is_defined(&dep.phase_id, &dep.task_id))
                .map(|dep| (dep.phase_id.clone(), dep.task_id.clone()))
                .collect();
            candidates.push(Parked { task, unmet });
        }
        // Append the already-parked entries AFTER the incoming batch.
        candidates.extend(parked);
        // Re-probe every candidate against the current ledger: a dep unmet at
        // a parked entry's original admission may have since been defined
        // off-band (a prior published batch / the cold seed).
        for c in candidates.iter_mut() {
            c.unmet
                .retain(|(phase, id)| !is_defined(phase, id.as_str()));
        }

        // Fixpoint to discover which candidates will EVER release: a
        // candidate releases once its `unmet` is empty; releasing it then
        // satisfies that dep for the others. Iterate to a fixpoint over a
        // SCRATCH copy of the unmet sets so the release-membership decision
        // is order-independent, then emit in the ORIGINAL candidate order
        // below. (Releasing in readiness order would reorder the batch and
        // shift the per-index `SpawnError` reply indices the caller's
        // `spawn_tasks` reply carries — for the common no-park case the
        // emitted order must stay byte-identical to the input so the
        // pre-CL-A8 index contract holds.)
        let mut scratch: Vec<HashSet<(PhaseId, String)>> =
            candidates.iter().map(|c| c.unmet.clone()).collect();
        loop {
            // Identities that just became defined this pass (their `unmet`
            // emptied). Collected before mutating so a single pass releases
            // every now-ready task; chained dependents fall out next pass.
            let newly_ready: Vec<(PhaseId, String)> = candidates
                .iter()
                .zip(scratch.iter())
                .filter(|(_, unmet)| unmet.is_empty())
                .map(|(c, _)| (c.task.phase_id.clone(), c.task.task_id.clone()))
                .collect();
            // Remove every now-ready identity from every still-unmet set.
            // `before`/`after` total counts detect whether the pass made
            // progress (fixpoint when no `unmet` shrank).
            let before: usize = scratch.iter().map(HashSet::len).sum();
            for unmet in scratch.iter_mut() {
                if !unmet.is_empty() {
                    for identity in &newly_ready {
                        unmet.remove(identity);
                    }
                }
            }
            let after: usize = scratch.iter().map(HashSet::len).sum();
            if after == before {
                // Fixpoint: no further dep can be satisfied. Every `unmet`
                // that is still non-empty names a dep nothing in the pool
                // (nor the ledger) defines — those candidates stay parked.
                break;
            }
        }

        // Emit in ORIGINAL candidate order: a candidate whose scratch
        // `unmet` is now empty releases; the rest stay parked.
        let mut released: Vec<TaskInfo<I>> = Vec::new();
        let mut still_parked: Vec<Parked<I>> = Vec::with_capacity(candidates.len());
        for (mut c, final_unmet) in candidates.into_iter().zip(scratch) {
            if final_unmet.is_empty() {
                released.push(c.task);
            } else {
                // Persist the narrowed unmet set so a later `admit`'s
                // re-probe starts from the still-missing deps.
                c.unmet = final_unmet;
                still_parked.push(c);
            }
        }
        self.parked = still_parked;
        released
    }

    /// Take EVERY still-parked task out of the queue, leaving it empty.
    ///
    /// Called at the run's drain edge: anything still parked has a
    /// prerequisite that was NEVER defined (a producer bug) or sits in a
    /// never-landing cross-batch cycle. The caller surfaces these LOUD via
    /// the existing spawn-rejection backstop (`RunError::SpawnRejected`) —
    /// a never-defined dep is NOT a silent forever-park (CL-A8).
    ///
    /// Returns each leftover task paired with the identities of the deps it
    /// is still waiting on, so the caller can name both the stranded task
    /// and its undefinable prerequisites in the failure diagnosis.
    pub(crate) fn drain(&mut self) -> Vec<StrandedSpawn<I>> {
        std::mem::take(&mut self.parked)
            .into_iter()
            .map(|p| (p.task, p.unmet.into_iter().collect()))
            .collect()
    }

    /// Drop every parked task without surfacing it. Per-run reset only — a
    /// coordinator reused across runs must not inherit a previous run's
    /// parked set (mirrors the `spawn_continuation_queue` per-run clear).
    pub(crate) fn clear(&mut self) {
        self.parked.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primary::test_helpers::{TestId, make_binary};
    use dynrunner_core::TaskDep;

    /// Build a `TaskInfo` named `id` with deps on each `(phase, dep_id)`.
    fn task_with_deps(id: &str, deps: &[(&str, &str)]) -> TaskInfo<TestId> {
        let mut t = make_binary(id, 100);
        t.task_id = id.into();
        t.task_depends_on = deps
            .iter()
            .map(|(phase, dep_id)| TaskDep {
                task_id: (*dep_id).into(),
                phase_id: PhaseId::from(*phase),
                inherit_outputs: false,
                def_id: None,
            })
            .collect();
        t
    }

    /// No deps ⇒ released immediately; the queue stays empty (the common
    /// topological pass-through case — the queue is a no-op).
    #[test]
    fn no_deps_releases_immediately_no_op() {
        let mut q: PendingSpawnQueue<TestId> = PendingSpawnQueue::new();
        let released = q.admit(vec![task_with_deps("a", &[])], |_, _| false);
        assert_eq!(released.len(), 1);
        assert_eq!(released[0].task_id, "a");
        assert!(q.is_empty(), "no-dep task must not park");
    }

    /// A dep already defined in the ledger ⇒ released immediately, queue
    /// empty (deps-first topological spawn).
    #[test]
    fn ledger_defined_dep_releases_immediately() {
        let mut q: PendingSpawnQueue<TestId> = PendingSpawnQueue::new();
        // `is_defined` true for ("default","b") models b already in ledger.
        let released = q.admit(vec![task_with_deps("a", &[("default", "b")])], |p, i| {
            p.as_str() == "default" && i == "b"
        });
        assert_eq!(released.len(), 1, "dep defined in ledger ⇒ publish now");
        assert!(q.is_empty());
    }

    /// CROSS-batch forward-ref: batch 1 carries the dependent whose
    /// prerequisite is undefined ⇒ it PARKS. Batch 2 carries the
    /// prerequisite ⇒ BOTH release (the prerequisite first, the dependent
    /// after, in one fixpoint pass).
    #[test]
    fn cross_batch_forward_ref_parks_then_releases() {
        let mut q: PendingSpawnQueue<TestId> = PendingSpawnQueue::new();
        // Batch 1: dependent `a` on not-yet-defined `b`. Nothing in ledger.
        let released = q.admit(vec![task_with_deps("a", &[("default", "b")])], |_, _| false);
        assert!(released.is_empty(), "early forward-ref must park, not publish");
        assert!(!q.is_empty(), "dependent stays parked awaiting its prereq");

        // Batch 2: the prerequisite `b` lands. Candidates = parked `[a]` ∪
        // incoming `[b]`; both release this pass. Emitted in candidate order
        // (parked-first, then incoming), so `[a, b]` — the emit order is the
        // stable candidate order, NOT readiness order (the index-stability
        // contract). Both must publish; order within the released set does
        // not affect the apply classification.
        let released = q.admit(vec![task_with_deps("b", &[])], |_, _| false);
        let mut ids: Vec<&str> = released.iter().map(|t| t.task_id.as_str()).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec!["a", "b"], "prereq and dependent both release");
        assert!(q.is_empty(), "both published ⇒ queue drains empty");
    }

    /// A never-defined dep keeps the task parked forever; `drain` surfaces
    /// it (with its undefined dep identity) for the loud spawn-rejection
    /// backstop — NOT a silent forever-park.
    #[test]
    fn never_defined_dep_surfaces_at_drain() {
        let mut q: PendingSpawnQueue<TestId> = PendingSpawnQueue::new();
        let released = q.admit(
            vec![task_with_deps("orphan", &[("default", "ghost")])],
            |_, _| false,
        );
        assert!(released.is_empty());
        // Subsequent unrelated batches never define `ghost`.
        let _ = q.admit(vec![task_with_deps("other", &[])], |_, _| false);
        assert!(!q.is_empty(), "orphan with never-defined dep stays parked");

        let leftovers = q.drain();
        assert_eq!(leftovers.len(), 1, "the orphan must surface at drain");
        assert_eq!(leftovers[0].0.task_id, "orphan");
        assert!(
            leftovers[0]
                .1
                .contains(&(PhaseId::from("default"), "ghost".to_string())),
            "drain names the never-defined prerequisite"
        );
        assert!(q.is_empty(), "drain empties the queue");
    }

    /// A cross-batch dependency CYCLE (a→b, b→a, neither ever defined
    /// elsewhere) never releases — neither task can satisfy the other's
    /// dep first — so both surface at `drain` for the loud backstop. The
    /// existing `PendingPool::extend` cycle-check stays the publish-time
    /// guard; this is the never-lands backstop.
    #[test]
    fn cross_batch_cycle_never_releases_surfaces_at_drain() {
        let mut q: PendingSpawnQueue<TestId> = PendingSpawnQueue::new();
        let released = q.admit(
            vec![
                task_with_deps("a", &[("default", "b")]),
                task_with_deps("b", &[("default", "a")]),
            ],
            |_, _| false,
        );
        assert!(released.is_empty(), "a cycle can never release");
        assert!(!q.is_empty());
        let leftovers = q.drain();
        let mut ids: Vec<String> = leftovers.iter().map(|(t, _)| t.task_id.clone()).collect();
        ids.sort();
        assert_eq!(ids, vec!["a".to_string(), "b".to_string()]);
    }
}
