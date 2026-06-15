//! Integration tests for the closure-based `validate_spawn_tasks`
//! shared between every backend that processes
//! `PrimaryCommand::SpawnTasks`.
//!
//! Single concern: pin the per-task rules (duplicate-hash, unknown-
//! dep, within-batch dep resolution) against the closure-based
//! signature. The distributed-crate tests cover the same rules
//! against the live `ClusterState`; these tests run against opaque
//! closures, which is the surface the local manager's
//! `manager::command_channel::handle_local_command` also consumes.

use std::path::PathBuf;
use std::sync::Arc;

use dynrunner_core::{
    AffinityId, PhaseId, SoftPreferredSecondaries, SpawnError, TaskDep, TaskInfo, TypeId,
    compute_task_hash, validate_spawn_tasks,
};

/// Build a minimal `TaskInfo` with a unique `task_id` and an
/// optional dep list.
fn task(task_id: &str, deps: Vec<&str>) -> TaskInfo<Arc<str>> {
    TaskInfo {
        path: PathBuf::from(format!("/t/{task_id}")),
        size: 1,
        identifier: Arc::<str>::from(task_id),
        phase_id: PhaseId::from("p"),
        type_id: TypeId::from("t"),
        affinity_id: Some(AffinityId::from(task_id)),
        payload: serde_json::Value::Null,
        task_id: task_id.into(),
        task_depends_on: deps
            .into_iter()
            .map(|d| TaskDep {
                task_id: d.to_string(),
                phase_id: PhaseId::from("p"),
                inherit_outputs: false,
            })
            .collect(),
        preferred_secondaries: SoftPreferredSecondaries::default(),
        preferred_version: Default::default(),
        kind: Default::default(),
        setup_affinity: None,
        upload_file: None,
        required_files: None,
        resolved_path: None,
    }
}

/// Build a `TaskInfo` with an explicit phase + caller-chosen `task_id`
/// and a list of fully-qualified `(dep_phase, dep_task_id)` deps so
/// cross-phase identity can be expressed.
fn task_in(phase: &str, task_id: &str, deps: &[(&str, &str)]) -> TaskInfo<Arc<str>> {
    let mut t = task(task_id, vec![]);
    t.phase_id = PhaseId::from(phase);
    t.task_depends_on = deps
        .iter()
        .map(|(dp, dt)| TaskDep {
            task_id: (*dt).to_string(),
            phase_id: PhaseId::from(*dp),
            inherit_outputs: false,
        })
        .collect();
    t
}

/// Closures that say "nothing is known on the receiver side": every
/// task is fresh, no `(phase, task_id)` exists in the ledger, AND every
/// phase accepts runtime spawn (the barrier interlock is opt-in via the
/// third closure; the default for these legacy tests preserves the
/// pre-barrier behaviour). The validator must still treat within-batch
/// `(phase, task_id)` identities as known for dep resolution.
// The closure-triple return type is exactly the shape the validator
// takes; collapsing it into a `type` alias would push the same set of
// `impl Trait`s one level down without changing the surface. Suppress
// the lint here — a test fixture is the legitimate place to spell the
// callable triple inline.
#[allow(clippy::type_complexity)]
fn empty_receiver() -> (
    impl Fn(&str) -> bool,
    impl Fn(&PhaseId, &str) -> bool,
    impl Fn(&PhaseId) -> bool,
) {
    (
        |_h: &str| false,
        |_p: &PhaseId, _id: &str| false,
        |_p: &PhaseId| true,
    )
}

/// `phase_accepts_runtime_spawn` closure that accepts every phase.
/// Reusable shorthand for the legacy tests whose subject is duplicate /
/// dep classification, not the barrier interlock.
fn accept_all_phases() -> impl Fn(&PhaseId) -> bool {
    |_p: &PhaseId| true
}

#[test]
fn empty_batch_returns_empty_pair() {
    let (present, known, accepts) = empty_receiver();
    let (valid, errors) =
        validate_spawn_tasks(present, known, accepts, Vec::<TaskInfo<Arc<str>>>::new());
    assert!(valid.is_empty());
    assert!(errors.is_empty());
}

#[test]
fn single_task_with_no_deps_validates() {
    let (present, known, accepts) = empty_receiver();
    let (valid, errors) = validate_spawn_tasks(present, known, accepts, vec![task("a", vec![])]);
    assert_eq!(valid.len(), 1);
    assert!(errors.is_empty());
    assert_eq!(valid[0].task_id, "a");
}

#[test]
fn within_batch_dep_resolves() {
    // task `b` depends on `a`, both in the same batch — the validator
    // must union the batch's own task_ids with the receiver's known
    // set so the dep resolves without any receiver-side knowledge.
    let (present, known, accepts) = empty_receiver();
    let (valid, errors) = validate_spawn_tasks(
        present,
        known,
        accepts,
        vec![task("a", vec![]), task("b", vec!["a"])],
    );
    assert_eq!(valid.len(), 2);
    assert!(errors.is_empty());
}

#[test]
fn unknown_dep_surfaces_as_per_task_error() {
    // `b` depends on `missing` — neither in the batch nor on the
    // receiver side. Expect a per-index error; `a` still validates.
    let (present, known, accepts) = empty_receiver();
    let (valid, errors) = validate_spawn_tasks(
        present,
        known,
        accepts,
        vec![task("a", vec![]), task("b", vec!["missing"])],
    );
    assert_eq!(valid.len(), 1);
    assert_eq!(valid[0].task_id, "a");
    assert_eq!(errors.len(), 1);
    let (idx, err) = &errors[0];
    assert_eq!(*idx, 1);
    match err {
        SpawnError::UnknownDependency { dep_task_id, .. } => {
            assert_eq!(dep_task_id, "missing");
        }
        other => panic!("expected UnknownDependency, got {other:?}"),
    }
}

#[test]
fn duplicate_hash_surfaces_as_per_task_error() {
    // The receiver reports the hash of `a` is already present. Expect
    // a per-index error tagging the duplicate hash.
    let a = task("a", vec![]);
    let a_hash = compute_task_hash(&a);
    let dup_hash = a_hash.clone();
    let is_present = move |h: &str| h == dup_hash;
    let is_known = |_p: &PhaseId, _id: &str| false;
    let (valid, errors) = validate_spawn_tasks(is_present, is_known, accept_all_phases(), vec![a]);
    assert!(valid.is_empty());
    assert_eq!(errors.len(), 1);
    let (idx, err) = &errors[0];
    assert_eq!(*idx, 0);
    match err {
        SpawnError::DuplicateTaskHash(h) => assert_eq!(h, &a_hash),
        other => panic!("expected DuplicateTaskHash, got {other:?}"),
    }
}

#[test]
fn dep_on_receiver_side_resolves_via_known_closure() {
    // `b` depends on `ledger_only` — the receiver-side `is_known`
    // closure must accept it; nothing else (no within-batch token,
    // no hash entry) should be needed.
    let is_present = |_h: &str| false;
    let is_known = |_p: &PhaseId, id: &str| id == "ledger_only";
    let (valid, errors) = validate_spawn_tasks(
        is_present,
        is_known,
        accept_all_phases(),
        vec![task("b", vec!["ledger_only"])],
    );
    assert_eq!(valid.len(), 1);
    assert!(errors.is_empty());
}

#[test]
fn dep_resolution_is_phase_aware_on_receiver_side() {
    // The receiver knows `foo` only in phase A. A spawned task in
    // phase B depends on (phase=B, foo) — absent in the NAMED phase —
    // so it must be minted UnknownDependency, NOT silently passed.
    // Pre-fix the phase-blind closure accepted any phase carrying `foo`.
    let is_present = |_h: &str| false;
    let is_known = |p: &PhaseId, id: &str| p == &PhaseId::from("A") && id == "foo";
    let (valid, errors) = validate_spawn_tasks(
        is_present,
        is_known,
        accept_all_phases(),
        vec![task_in("B", "child", &[("B", "foo")])],
    );
    assert!(valid.is_empty(), "the phase-B dep is unsatisfiable");
    assert_eq!(errors.len(), 1);
    match &errors[0].1 {
        SpawnError::UnknownDependency { dep_task_id, .. } => assert_eq!(dep_task_id, "foo"),
        other => panic!("expected UnknownDependency, got {other:?}"),
    }
}

#[test]
fn cross_phase_dep_naming_right_phase_resolves_on_receiver_side() {
    // The receiver knows `foo` in phase A; a phase-B task depending on
    // (phase=A, foo) names the right phase → resolves.
    let is_present = |_h: &str| false;
    let is_known = |p: &PhaseId, id: &str| p == &PhaseId::from("A") && id == "foo";
    let (valid, errors) = validate_spawn_tasks(
        is_present,
        is_known,
        accept_all_phases(),
        vec![task_in("B", "child", &[("A", "foo")])],
    );
    assert_eq!(valid.len(), 1);
    assert!(errors.is_empty());
}

#[test]
fn within_batch_dep_resolution_is_phase_aware() {
    // `parent` is in the batch under phase A only. `child` (phase B)
    // depends on (phase=B, parent) — the within-batch known set is
    // keyed on (phase, task_id), so the phase-B identity is absent and
    // `child` is UnknownDependency. `parent` itself validates.
    let (present, known, accepts) = empty_receiver();
    let (valid, errors) = validate_spawn_tasks(
        present,
        known,
        accepts,
        vec![
            task_in("A", "parent", &[]),
            task_in("B", "child", &[("B", "parent")]),
        ],
    );
    assert_eq!(valid.len(), 1);
    assert_eq!(valid[0].task_id, "parent");
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].0, 1);
    match &errors[0].1 {
        SpawnError::UnknownDependency { dep_task_id, .. } => assert_eq!(dep_task_id, "parent"),
        other => panic!("expected UnknownDependency, got {other:?}"),
    }
}

#[test]
fn first_failure_short_circuits_per_task_checks() {
    // Duplicate-hash is checked before unknown-dep; a task that
    // would trip both should surface only the duplicate error
    // (the documented per-task short-circuit shape).
    let a = task("a", vec!["missing"]);
    let a_hash = compute_task_hash(&a);
    let is_present = move |h: &str| h == a_hash;
    let is_known = |_p: &PhaseId, _id: &str| false;
    let (valid, errors) = validate_spawn_tasks(is_present, is_known, accept_all_phases(), vec![a]);
    assert!(valid.is_empty());
    assert_eq!(errors.len(), 1);
    let (_, err) = &errors[0];
    assert!(matches!(err, SpawnError::DuplicateTaskHash(_)));
}

#[test]
fn within_batch_duplicate_is_distinct_from_already_in_ledger() {
    // Two copies of the SAME fresh identity in ONE batch (neither in the
    // ledger): the FIRST is valid, the SECOND is the fatal
    // `DuplicateInBatch` (a genuine ambiguous producer batch). This is the
    // class fix (b) keeps invalidating run-wide.
    let (present, known, accepts) = empty_receiver();
    let dup = task("dup", vec![]);
    let dup_hash = compute_task_hash(&dup);
    let (valid, errors) =
        validate_spawn_tasks(present, known, accepts, vec![dup.clone(), dup.clone()]);
    assert_eq!(valid.len(), 1, "the first occurrence validates");
    assert_eq!(valid[0].task_id, "dup");
    assert_eq!(errors.len(), 1);
    let (idx, err) = &errors[0];
    assert_eq!(*idx, 1, "the SECOND occurrence is the within-batch dup");
    match err {
        SpawnError::DuplicateInBatch(h) => assert_eq!(h, &dup_hash),
        other => panic!("expected DuplicateInBatch, got {other:?}"),
    }
}

#[test]
fn already_in_ledger_is_duplicate_task_hash_not_in_batch() {
    // A SINGLE occurrence whose hash is already in the ledger (a failover
    // re-spawn): the idempotent `DuplicateTaskHash`, NOT `DuplicateInBatch`.
    // Pins that the two classes do not collapse — the within-batch tracker
    // only fires on a SECOND in-batch occurrence, never on a first-and-only
    // item that merely collides with the ledger.
    let a = task("a", vec![]);
    let a_hash = compute_task_hash(&a);
    let present_hash = a_hash.clone();
    let is_present = move |h: &str| h == present_hash;
    let is_known = |_p: &PhaseId, _id: &str| false;
    let (valid, errors) = validate_spawn_tasks(is_present, is_known, accept_all_phases(), vec![a]);
    assert!(valid.is_empty());
    assert_eq!(errors.len(), 1);
    match &errors[0].1 {
        SpawnError::DuplicateTaskHash(h) => assert_eq!(h, &a_hash),
        other => panic!("expected DuplicateTaskHash, got {other:?}"),
    }
}

#[test]
fn barrier_violation_surfaces_when_phase_rejects_runtime_spawn() {
    // Single task in phase `late` whose phase rejects runtime spawn
    // (phase still `Blocked`, barrier=True default). The validator must
    // surface `SpawnError::BarrierViolation` per index, naming both the
    // task hash and the target phase. The legacy duplicate/dep classes
    // still validate independently — this is a fourth peer.
    let t = task_in("late", "x", &[]);
    let t_hash = compute_task_hash(&t);
    let is_present = |_h: &str| false;
    let is_known = |_p: &PhaseId, _id: &str| false;
    // Only `early` accepts spawn; `late` rejects (barrier=True, Blocked).
    let phase_accepts = |p: &PhaseId| p == &PhaseId::from("early");
    let (valid, errors) = validate_spawn_tasks(is_present, is_known, phase_accepts, vec![t]);
    assert!(valid.is_empty());
    assert_eq!(errors.len(), 1);
    let (idx, err) = &errors[0];
    assert_eq!(*idx, 0);
    match err {
        SpawnError::BarrierViolation {
            task_hash,
            phase_id,
        } => {
            assert_eq!(task_hash, &t_hash);
            assert_eq!(phase_id, &PhaseId::from("late"));
        }
        other => panic!("expected BarrierViolation, got {other:?}"),
    }
}

#[test]
fn barrier_violation_does_not_fire_when_phase_accepts() {
    // Same task as above but the phase accepts runtime spawn (either
    // already-active, OR PhaseSpec(barrier=False) declared by the
    // consumer). The barrier interlock is silent — the task validates
    // normally. This pins that the closure is consulted PER TASK, not
    // globally, and that an accepting phase doesn't trigger the error.
    let t = task_in("late", "x", &[]);
    let is_present = |_h: &str| false;
    let is_known = |_p: &PhaseId, _id: &str| false;
    let phase_accepts = |_p: &PhaseId| true;
    let (valid, errors) = validate_spawn_tasks(is_present, is_known, phase_accepts, vec![t]);
    assert_eq!(valid.len(), 1);
    assert_eq!(valid[0].task_id, "x");
    assert!(errors.is_empty());
}

#[test]
fn barrier_violation_per_task_in_mixed_batch() {
    // Two tasks: `early.a` targets an accepting phase, `late.b` targets
    // a rejecting phase. The accepting one validates, the rejecting one
    // surfaces `BarrierViolation` — the per-index shape is preserved
    // exactly like the existing dep/duplicate classes.
    let a = task_in("early", "a", &[]);
    let b = task_in("late", "b", &[]);
    let b_hash = compute_task_hash(&b);
    let is_present = |_h: &str| false;
    let is_known = |_p: &PhaseId, _id: &str| false;
    let phase_accepts = |p: &PhaseId| p == &PhaseId::from("early");
    let (valid, errors) = validate_spawn_tasks(is_present, is_known, phase_accepts, vec![a, b]);
    assert_eq!(valid.len(), 1);
    assert_eq!(valid[0].task_id, "a");
    assert_eq!(errors.len(), 1);
    let (idx, err) = &errors[0];
    assert_eq!(*idx, 1);
    match err {
        SpawnError::BarrierViolation {
            task_hash,
            phase_id,
        } => {
            assert_eq!(task_hash, &b_hash);
            assert_eq!(phase_id, &PhaseId::from("late"));
        }
        other => panic!("expected BarrierViolation, got {other:?}"),
    }
}

#[test]
fn unknown_dep_takes_precedence_over_barrier_violation() {
    // A task that fails BOTH the dep check AND the barrier interlock
    // surfaces only the FIRST class checked (UnknownDependency) — the
    // documented per-task short-circuit shape. Pins that BarrierViolation
    // doesn't reorder the per-task error precedence.
    let t = task_in("late", "x", &[("late", "missing")]);
    let is_present = |_h: &str| false;
    let is_known = |_p: &PhaseId, _id: &str| false;
    let phase_accepts = |_p: &PhaseId| false;
    let (valid, errors) = validate_spawn_tasks(is_present, is_known, phase_accepts, vec![t]);
    assert!(valid.is_empty());
    assert_eq!(errors.len(), 1);
    match &errors[0].1 {
        SpawnError::UnknownDependency { dep_task_id, .. } => assert_eq!(dep_task_id, "missing"),
        other => panic!("expected UnknownDependency, got {other:?}"),
    }
}
