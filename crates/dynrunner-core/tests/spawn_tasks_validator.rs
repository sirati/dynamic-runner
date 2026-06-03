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
    compute_task_hash, validate_spawn_tasks, AffinityId, PhaseId,
    SoftPreferredSecondaries, SpawnError, TaskDep, TaskInfo, TypeId,
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
        resolved_path: None,
    }
}

/// Closures that say "nothing is known on the receiver side": every
/// task is fresh, no task_ids exist in the ledger. The validator must
/// still treat within-batch task_ids as known for dep resolution.
fn empty_receiver() -> (impl Fn(&str) -> bool, impl Fn(&str) -> bool) {
    (|_h: &str| false, |_id: &str| false)
}

#[test]
fn empty_batch_returns_empty_pair() {
    let (present, known) = empty_receiver();
    let (valid, errors) = validate_spawn_tasks(present, known, Vec::<TaskInfo<Arc<str>>>::new());
    assert!(valid.is_empty());
    assert!(errors.is_empty());
}

#[test]
fn single_task_with_no_deps_validates() {
    let (present, known) = empty_receiver();
    let (valid, errors) = validate_spawn_tasks(present, known, vec![task("a", vec![])]);
    assert_eq!(valid.len(), 1);
    assert!(errors.is_empty());
    assert_eq!(valid[0].task_id, "a");
}

#[test]
fn within_batch_dep_resolves() {
    // task `b` depends on `a`, both in the same batch — the validator
    // must union the batch's own task_ids with the receiver's known
    // set so the dep resolves without any receiver-side knowledge.
    let (present, known) = empty_receiver();
    let (valid, errors) =
        validate_spawn_tasks(present, known, vec![task("a", vec![]), task("b", vec!["a"])]);
    assert_eq!(valid.len(), 2);
    assert!(errors.is_empty());
}

#[test]
fn unknown_dep_surfaces_as_per_task_error() {
    // `b` depends on `missing` — neither in the batch nor on the
    // receiver side. Expect a per-index error; `a` still validates.
    let (present, known) = empty_receiver();
    let (valid, errors) = validate_spawn_tasks(
        present,
        known,
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
    let is_known = |_id: &str| false;
    let (valid, errors) = validate_spawn_tasks(is_present, is_known, vec![a]);
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
    let is_known = |id: &str| id == "ledger_only";
    let (valid, errors) =
        validate_spawn_tasks(is_present, is_known, vec![task("b", vec!["ledger_only"])]);
    assert_eq!(valid.len(), 1);
    assert!(errors.is_empty());
}

#[test]
fn first_failure_short_circuits_per_task_checks() {
    // Duplicate-hash is checked before unknown-dep; a task that
    // would trip both should surface only the duplicate error
    // (the documented per-task short-circuit shape).
    let a = task("a", vec!["missing"]);
    let a_hash = compute_task_hash(&a);
    let is_present = move |h: &str| h == a_hash;
    let is_known = |_id: &str| false;
    let (valid, errors) = validate_spawn_tasks(is_present, is_known, vec![a]);
    assert!(valid.is_empty());
    assert_eq!(errors.len(), 1);
    let (_, err) = &errors[0];
    assert!(matches!(err, SpawnError::DuplicateTaskHash(_)));
}
