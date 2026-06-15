//! Tests for the `blocked_by` reverse-index (#547).
//!
//! Single concern: pin the invariant that the incrementally-maintained
//! `blocked_by` index (`prereq_hash -> { dependent_hash, … }`, populated by
//! the SOLE `set_task_state` write seam) is equal to a fresh scan over
//! `self.tasks` for `Blocked { on: prereq_hash, .. }` entries. Cover the
//! lifecycle through spawn (Blocked-on-pending-dep) → completion (resume
//! cascade clears the index).
//!
//! The invariant is load-bearing: `resume_blocked_on` reads the index
//! directly (O(|dependents|)) instead of scanning `self.tasks` (O(|tasks|)).
//! If the index ever diverges from the live `Blocked` entries, a missed
//! prereq's dependents would never be woken (silent stall) or non-dependents
//! would be erroneously transitioned to Pending (state corruption).

use super::*;
use crate::cluster_state::ApplyOutcome;

/// Walk `self.tasks` for every `Blocked { on, .. }` entry and produce the
/// canonical reverse-index (`prereq_hash -> { dependent_hash, … }`) — the
/// shape `blocked_by` must equal by construction. Test-only.
fn fresh_blocked_by(state: &ClusterState<RunnerIdentifier>) -> HashMap<String, HashSet<String>> {
    let mut out: HashMap<String, HashSet<String>> = HashMap::new();
    for (hash, s) in state.tasks_iter() {
        if let TaskState::Blocked { on, .. } = s {
            out.entry(on.clone()).or_default().insert(hash.clone());
        }
    }
    out
}

/// Assert the index matches a fresh ledger scan at the current instant.
/// Called after every apply in the lifecycle tests.
fn assert_index_matches(state: &ClusterState<RunnerIdentifier>) {
    let fresh = fresh_blocked_by(state);
    assert_eq!(
        state.blocked_by_for_test(),
        &fresh,
        "blocked_by index diverged from a fresh tasks scan"
    );
}

/// A no-deps spawn lands `Pending`, never `Blocked` — the index stays empty.
#[test]
fn no_deps_spawn_leaves_index_empty() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let t = mk_task("t");
    s.apply(ClusterMutation::TasksSpawned { tasks: vec![t] });
    assert!(s.blocked_by_for_test().is_empty());
    assert_index_matches(&s);
}

/// A `TasksSpawned` entry whose dep is `Pending` lands `Blocked { on: <dep>
/// }` — the index records (dep_hash → {entry_hash}).
#[test]
fn spawn_with_pending_dep_indexes_blocked() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    // Seed a Pending prereq via TaskAdded (the cold-seed apply path) —
    // simpler than running through the spawn classifier.
    let prereq = mk_task("p");
    let prereq_hash = crate::primary::wire::compute_task_hash(&prereq);
    s.apply(ClusterMutation::TaskAdded {
        hash: prereq_hash.clone(),
        task: prereq,
    });

    let mut dep = mk_task("d");
    dep.task_depends_on = vec![dynrunner_core::TaskDep {
        task_id: "p".into(),
        phase_id: PhaseId::from("p0"),
        inherit_outputs: false,
    }];
    let dep_hash = crate::primary::wire::compute_task_hash(&dep);
    s.apply(ClusterMutation::TasksSpawned { tasks: vec![dep] });

    // Sanity: dependent is Blocked-on-prereq.
    match s.task_state(&dep_hash) {
        Some(TaskState::Blocked { on, .. }) => assert_eq!(on, &prereq_hash),
        other => panic!("expected Blocked, got {other:?}"),
    }

    // Index shape.
    let index = s.blocked_by_for_test();
    let expected: HashSet<String> = [dep_hash.clone()].into_iter().collect();
    assert_eq!(index.get(&prereq_hash), Some(&expected));
    assert_index_matches(&s);
}

/// When the prereq completes, `resume_blocked_on(prereq)` reads the index,
/// transitions every dependent `Blocked → Pending`, and the index entry for
/// the prereq is drained.
#[test]
fn complete_cascade_drains_index_entry() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    // Seed prereq as Pending → assigned → completed via the full apply chain.
    let prereq = mk_task("p");
    let prereq_hash = crate::primary::wire::compute_task_hash(&prereq);
    s.apply(ClusterMutation::TaskAdded {
        hash: prereq_hash.clone(),
        task: prereq,
    });
    // Spawn two dependents both Blocked-on-prereq.
    for name in ["d1", "d2"] {
        let mut dep = mk_task(name);
        dep.task_depends_on = vec![dynrunner_core::TaskDep {
            task_id: "p".into(),
            phase_id: PhaseId::from("p0"),
            inherit_outputs: false,
        }];
        s.apply(ClusterMutation::TasksSpawned { tasks: vec![dep] });
    }
    assert_eq!(s.blocked_by_for_test().get(&prereq_hash).map(|s| s.len()), Some(2));
    assert_index_matches(&s);

    // Assign + complete the prereq. The TaskCompleted apply arm fires
    // resume_blocked_on(prereq_hash) which now consults the index.
    s.apply(ClusterMutation::TaskAssigned {
        hash: prereq_hash.clone(),
        secondary: "secA".to_string(),
        worker: 0,
        version: Default::default(),
        attempt: Default::default(),
    });
    let outcome = s.apply(ClusterMutation::TaskCompleted {
        hash: prereq_hash.clone(),
        result_data: None,
        attempt: 0,
    });
    assert_eq!(outcome, ApplyOutcome::Applied);

    // Index entry for prereq drained; dependents Pending.
    assert!(s.blocked_by_for_test().get(&prereq_hash).is_none());
    for name in ["d1", "d2"] {
        let mut dep = mk_task(name);
        dep.task_depends_on = vec![dynrunner_core::TaskDep {
            task_id: "p".into(),
            phase_id: PhaseId::from("p0"),
            inherit_outputs: false,
        }];
        let h = crate::primary::wire::compute_task_hash(&dep);
        assert!(matches!(s.task_state(&h), Some(TaskState::Pending { .. })));
    }
    assert_index_matches(&s);
}

/// `TaskBlocked` against an already-Blocked entry is a NoOp at the apply
/// level — "first observed cascade root wins" (`apply.rs`'s `TaskBlocked`
/// arm). The index must NOT change on the NoOp, so the invariant remains
/// satisfied.
#[test]
fn blocked_idempotent_apply_leaves_index_unchanged() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let p1 = mk_task("p1");
    let p1_hash = crate::primary::wire::compute_task_hash(&p1);
    s.apply(ClusterMutation::TaskAdded {
        hash: p1_hash.clone(),
        task: p1,
    });
    let mut dep = mk_task("d");
    dep.task_depends_on = vec![dynrunner_core::TaskDep {
        task_id: "p1".into(),
        phase_id: PhaseId::from("p0"),
        inherit_outputs: false,
    }];
    let dep_hash = crate::primary::wire::compute_task_hash(&dep);
    s.apply(ClusterMutation::TasksSpawned { tasks: vec![dep] });
    assert_index_matches(&s);

    // Same-`on` re-broadcast: NoOp at the apply level. Index unchanged.
    let outcome = s.apply(ClusterMutation::TaskBlocked {
        hash: dep_hash.clone(),
        on: p1_hash.clone(),
    });
    assert_eq!(outcome, ApplyOutcome::NoOp);
    assert_index_matches(&s);

    // Mismatched-`on` cascade: the first observed cascade root wins, so this
    // is also a NoOp — the index must stay pinned to the ORIGINAL `on`.
    let outcome = s.apply(ClusterMutation::TaskBlocked {
        hash: dep_hash.clone(),
        on: "some_other_hash".to_string(),
    });
    assert_eq!(outcome, ApplyOutcome::NoOp);
    assert_index_matches(&s);
}

/// A direct `set_task_state` rewrite of `Blocked { on: A }` → `Blocked { on:
/// B }` correctly re-buckets the dependent in the reverse-index: drops from
/// the OLD prereq's set + inserts into the NEW prereq's set. This exercises
/// the set_task_state seam's "different-on" branch directly (no production
/// public mutation triggers this today — the closest equivalent is the
/// snapshot-restore convergence path through `merge_task_state` — so the
/// test reaches into the seam via the same-crate internal entry point).
#[test]
fn set_task_state_blocked_to_blocked_different_on_rebuckets() {
    use dynrunner_core::TaskDep;
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let p1 = mk_task("p1");
    let p1_hash = crate::primary::wire::compute_task_hash(&p1);
    s.apply(ClusterMutation::TaskAdded {
        hash: p1_hash.clone(),
        task: p1,
    });
    let p2 = mk_task("p2");
    let p2_hash = crate::primary::wire::compute_task_hash(&p2);
    s.apply(ClusterMutation::TaskAdded {
        hash: p2_hash.clone(),
        task: p2,
    });
    let mut dep = mk_task("d");
    dep.task_depends_on = vec![TaskDep {
        task_id: "p1".into(),
        phase_id: PhaseId::from("p0"),
        inherit_outputs: false,
    }];
    let dep_hash = crate::primary::wire::compute_task_hash(&dep);
    s.apply(ClusterMutation::TasksSpawned {
        tasks: vec![dep.clone()],
    });
    assert!(
        s.blocked_by_for_test()
            .get(&p1_hash)
            .is_some_and(|set| set.contains(&dep_hash)),
        "spawn should index dep under p1"
    );
    assert_index_matches(&s);

    // Rewrite the slot to Blocked-on-p2 via the internal seam — the same
    // entry every apply arm routes through. We use `rewrite_task_state`
    // (the presence-guarded wrapper) so the test does not duplicate the
    // memo-maintaining write site.
    s.rewrite_blocked_for_test(&dep_hash, p2_hash.clone(), dep.clone(), 0);

    assert!(
        s.blocked_by_for_test().get(&p1_hash).is_none(),
        "p1's bucket should be drained after the rewrite"
    );
    assert!(
        s.blocked_by_for_test()
            .get(&p2_hash)
            .is_some_and(|set| set.contains(&dep_hash)),
        "p2's bucket should hold the dep after the rewrite"
    );
    assert_index_matches(&s);
}

/// The `resume_blocked_on` fast path returns immediately when the prereq has
/// no dependents — empty/missing bucket maps to an empty `Vec`, no scan.
#[test]
fn resume_blocked_on_empty_bucket_returns_empty() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    // Seed a Pending task with no dependents.
    let t = mk_task("solo");
    let h = crate::primary::wire::compute_task_hash(&t);
    s.apply(ClusterMutation::TaskAdded {
        hash: h.clone(),
        task: t,
    });
    // Manually call resume_blocked_on with a hash that has no entry. The
    // function is `pub(super)`; reach it through the apply path: completing
    // `h` fires `resume_blocked_on(h)` which finds nothing.
    s.apply(ClusterMutation::TaskAssigned {
        hash: h.clone(),
        secondary: "secA".to_string(),
        worker: 0,
        version: Default::default(),
        attempt: Default::default(),
    });
    let outcome = s.apply(ClusterMutation::TaskCompleted {
        hash: h.clone(),
        result_data: None,
        attempt: 0,
    });
    assert_eq!(outcome, ApplyOutcome::Applied);
    assert_index_matches(&s);
}
