//! Tests for the non-mutating `partition_ingest` primitive: the
//! `{ valid, invalid_deps, duplicates }` classification keyed on the
//! full `(phase_id, task_id)` identity.
//!
//! Pins the load-bearing semantics #2/#3 depend on:
//!   * a literally-absent dep `(phase, task_id)` → `invalid_deps`;
//!   * a dep present in the batch (even an invalid sibling) or the
//!     pool → resolves;
//!   * a `(phase, task_id)` collision within the batch or against a
//!     pool entry → `duplicates`;
//!   * the SAME `task_id` in a DIFFERENT phase is NOT a duplicate;
//!   * the pool is never mutated by the call.

use dynrunner_core::{PhaseId, TaskDep, TaskInfo};

use super::{pool_with, t};

/// Build a task with a caller-chosen `(phase, task_id)` and a list of
/// fully-qualified `(dep_phase, dep_task_id)` deps so cross-phase deps
/// can be expressed (the shared `t_with_id` in `task_deps.rs` pins the
/// dep phase to the item's phase).
fn task(phase_id: &str, id: &str, deps: &[(&str, &str)]) -> TaskInfo<()> {
    let mut item = t(phase_id, "T", "", 1);
    item.task_id = id.to_string();
    item.task_depends_on = deps
        .iter()
        .map(|(dp, dt)| TaskDep {
            task_id: dt.to_string(),
            phase_id: PhaseId::from(*dp),
            inherit_outputs: false,
        })
        .collect();
    item
}

#[test]
fn partition_all_valid_no_deps() {
    let p = pool_with(&["P"], &[]);
    let part = p.partition_ingest([task("P", "a", &[]), task("P", "b", &[])]);
    let ids: Vec<_> = part.valid.iter().map(|t| t.task_id.clone()).collect();
    assert_eq!(ids, vec!["a".to_string(), "b".to_string()]);
    assert!(part.invalid_deps.is_empty());
    assert!(part.duplicates.is_empty());
}

#[test]
fn partition_within_batch_dep_resolves() {
    let p = pool_with(&["P"], &[]);
    // b depends on a, both in the batch → both valid.
    let part = p.partition_ingest([task("P", "a", &[]), task("P", "b", &[("P", "a")])]);
    assert_eq!(part.valid.len(), 2);
    assert!(part.invalid_deps.is_empty());
    assert!(part.duplicates.is_empty());
}

#[test]
fn partition_missing_dep_is_invalid_and_names_the_absent_identity() {
    let p = pool_with(&["P"], &[]);
    let part = p.partition_ingest([task("P", "child", &[("P", "ghost")])]);
    assert!(part.valid.is_empty());
    assert!(part.duplicates.is_empty());
    assert_eq!(part.invalid_deps.len(), 1);
    let (item, reason) = &part.invalid_deps[0];
    assert_eq!(item.task_id, "child");
    // The reason names the literally-absent (phase, task_id).
    assert!(reason.contains("ghost"), "reason should name the absent dep: {reason}");
    assert!(reason.contains('P'), "reason should name the absent dep's phase: {reason}");
}

#[test]
fn partition_cross_phase_dep_missing_in_named_phase_is_invalid() {
    // `parent` exists in phase A, but the dep names phase B → absent
    // in the named phase → invalid. (Full-identity resolution: the
    // dep's phase must match.)
    let p = pool_with(&["A", "B"], &[]);
    let part = p.partition_ingest([
        task("A", "parent", &[]),
        task("B", "child", &[("B", "parent")]),
    ]);
    let valid_ids: Vec<_> = part.valid.iter().map(|t| t.task_id.clone()).collect();
    assert_eq!(valid_ids, vec!["parent".to_string()]);
    assert_eq!(part.invalid_deps.len(), 1);
    assert_eq!(part.invalid_deps[0].0.task_id, "child");
}

#[test]
fn partition_cross_phase_dep_present_in_named_phase_resolves() {
    let p = pool_with(&["A", "B"], &[]);
    // child(B) depends on parent in phase A — names the right phase.
    let part = p.partition_ingest([
        task("A", "parent", &[]),
        task("B", "child", &[("A", "parent")]),
    ]);
    assert_eq!(part.valid.len(), 2);
    assert!(part.invalid_deps.is_empty());
}

#[test]
fn partition_dep_on_invalid_sibling_still_resolves() {
    // `bad` is itself invalid (missing dep), but it is PRESENT in the
    // batch. `good` depends on `bad` → `good` resolves (presence, not
    // validity, is the missing-dep test). The cascade is the manager's
    // concern, not a fresh missing-dep here.
    let p = pool_with(&["P"], &[]);
    let part = p.partition_ingest([
        task("P", "bad", &[("P", "ghost")]),
        task("P", "good", &[("P", "bad")]),
    ]);
    let valid_ids: Vec<_> = part.valid.iter().map(|t| t.task_id.clone()).collect();
    assert_eq!(valid_ids, vec!["good".to_string()]);
    assert_eq!(part.invalid_deps.len(), 1);
    assert_eq!(part.invalid_deps[0].0.task_id, "bad");
}

#[test]
fn partition_within_batch_duplicate_identity() {
    let p = pool_with(&["P"], &[]);
    let part = p.partition_ingest([task("P", "dup", &[]), task("P", "dup", &[])]);
    // First occurrence is valid; the second is a duplicate.
    let valid_ids: Vec<_> = part.valid.iter().map(|t| t.task_id.clone()).collect();
    assert_eq!(valid_ids, vec!["dup".to_string()]);
    assert_eq!(part.duplicates.len(), 1);
    assert_eq!(part.duplicates[0].0.task_id, "dup");
}

#[test]
fn partition_cross_phase_same_task_id_is_not_a_duplicate() {
    // The SAME task_id in two DIFFERENT phases is a DISTINCT task.
    let p = pool_with(&["A", "B"], &[]);
    let part = p.partition_ingest([task("A", "shared", &[]), task("B", "shared", &[])]);
    assert_eq!(part.valid.len(), 2, "cross-phase same id is not a duplicate");
    assert!(part.duplicates.is_empty());
}

#[test]
fn partition_duplicate_against_existing_pool_entry() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([t_id("P", "already")]).expect("seed");
    let part = p.partition_ingest([task("P", "already", &[]), task("P", "fresh", &[])]);
    let valid_ids: Vec<_> = part.valid.iter().map(|t| t.task_id.clone()).collect();
    assert_eq!(valid_ids, vec!["fresh".to_string()]);
    assert_eq!(part.duplicates.len(), 1);
    assert_eq!(part.duplicates[0].0.task_id, "already");
}

#[test]
fn partition_resolves_dep_against_existing_pool_entry() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([t_id("P", "seeded")]).expect("seed");
    // child depends on the already-pooled `seeded` → resolves.
    let part = p.partition_ingest([task("P", "child", &[("P", "seeded")])]);
    assert_eq!(part.valid.len(), 1);
    assert!(part.invalid_deps.is_empty());
}

#[test]
fn partition_does_not_mutate_the_pool() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([t_id("P", "x")]).expect("seed");
    let len_before = p.len();
    let _ = p.partition_ingest([
        task("P", "dup_x_is_a_duplicate", &[]),
        task("P", "missing_dep", &[("P", "ghost")]),
        task("P", "fine", &[]),
    ]);
    assert_eq!(p.len(), len_before, "partition_ingest must not mutate the pool");
}

/// `task(..)` with no deps, fixed id.
fn t_id(phase_id: &str, id: &str) -> TaskInfo<()> {
    task(phase_id, id, &[])
}
