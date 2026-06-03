//! Task-level dependency tests (`task_id` + `task_depends_on`):
//! extend-time validation (unknown deps, cycles, duplicate ids),
//! blocked-then-unblocked dispatch, FRONT-of-bucket unblocking
//! position, pre-seeded completion via `mark_tasks_completed`, the
//! permanent-failure cascade, and the `update_first_match_in_place`
//! primitive (which scans both queued buckets and the blocked map).

use dynrunner_core::{PhaseId, SoftPreferredSecondaries, TaskDep, TaskInfo};

use super::{PendingPoolError, phase, pool_with, t};

/// Test fixture variant carrying a caller-chosen task_id and
/// (optional) deps. Overrides the synthetic id `t(...)` assigns so
/// tests that assert against the id can pin a stable value.
fn t_with_id(
    phase: &str,
    ty: &str,
    affinity: &str,
    size: u64,
    id: &str,
    deps: &[&str],
) -> TaskInfo<()> {
    let mut item = t(phase, ty, affinity, size);
    item.task_id = id.to_string();
    item.task_depends_on = deps
        .iter()
        .map(|d| TaskDep {
            task_id: d.to_string(),
            phase_id: PhaseId::from(phase),
            inherit_outputs: false,
        })
        .collect();
    item
}

#[test]
fn task_deps_unknown_id_fails_extend() {
    let mut p = pool_with(&["P"], &[]);
    let res = p.extend([t_with_id("P", "T", "", 1, "child", &["nope"])]);
    match res {
        Err(PendingPoolError::UnknownTaskDep {
            task,
            referenced_by,
        }) => {
            assert_eq!(task, "nope");
            assert_eq!(referenced_by, "child");
        }
        other => panic!("expected UnknownTaskDep, got {:?}", other),
    }
}

#[test]
fn task_deps_cycle_fails_extend() {
    let mut p = pool_with(&["P"], &[]);
    let res = p.extend([
        t_with_id("P", "T", "", 1, "a", &["b"]),
        t_with_id("P", "T", "", 1, "b", &["a"]),
    ]);
    assert!(matches!(res, Err(PendingPoolError::TaskDepCycle(_))));
}

#[test]
fn task_deps_duplicate_id_fails_extend() {
    let mut p = pool_with(&["P"], &[]);
    let res = p.extend([
        t_with_id("P", "T", "", 1, "dup", &[]),
        t_with_id("P", "T", "", 1, "dup", &[]),
    ]);
    assert!(matches!(res, Err(PendingPoolError::DuplicateTaskId(_))));
}

#[test]
fn task_deps_blocked_until_dep_completes() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([
        t_with_id("P", "T", "", 1, "a", &[]),
        t_with_id("P", "T", "", 1, "b", &["a"]),
    ])
    .expect("valid extend");
    // Bucket iteration only sees A; B is in the blocked map.
    let queued_ids: Vec<_> = p.iter().map(|i| i.task_id.clone()).collect();
    assert_eq!(queued_ids, vec!["a".to_string()]);
    let first = p.pop_for_worker(1).expect("a is dispatchable");
    assert_eq!(first.task_id, "a");
    // No more queued items; B still blocked, phase still has work.
    assert!(p.pop_for_worker(1).is_none());
    p.on_item_finished(&phase("P"), Some("a"));
    // Now B is unblocked.
    let second = p.pop_for_worker(1).expect("b unblocked");
    assert_eq!(second.task_id, "b");
}

#[test]
fn task_deps_unblocked_lands_at_bucket_front() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([
        t_with_id("P", "T", "alpha", 1, "a", &[]),
        t_with_id("P", "T", "alpha", 1, "b", &["a"]),
        t_with_id("P", "T", "alpha", 1, "c", &[]),
    ])
    .expect("valid extend");
    // A is dispatched first (it's in front of C; B is blocked).
    let a = p.pop_for_worker(1).expect("a");
    assert_eq!(a.task_id, "a");
    // Finish A → B unblocks and lands at the FRONT of the bucket,
    // ahead of C which has been queued behind A all along.
    p.on_item_finished(&phase("P"), Some("a"));
    let next = p.pop_for_worker(1).expect("b before c");
    assert_eq!(next.task_id, "b");
    let last = p.pop_for_worker(1).expect("c last");
    assert_eq!(last.task_id, "c");
}

#[test]
fn task_deps_seeded_completed_id_resolves_unknown_dep() {
    // Failover-resume regression: a promoted secondary rebuilds its
    // pool from a wire snapshot that has dropped pre-completed items
    // from the items vec. Surviving items that declared
    // `task_depends_on` against those completions used to fail
    // `extend()` with `UnknownTaskDep`. `mark_tasks_completed`
    // seeds the completion set so validation resolves.
    let mut p = pool_with(&["P"], &[]);
    p.mark_tasks_completed(["toolchain".to_string()]);
    p.extend([t_with_id("P", "T", "", 1, "variant", &["toolchain"])])
        .expect("variant resolves against pre-seeded toolchain completion");
    // Variant is dispatchable immediately because its only dep is
    // already considered complete.
    let item = p.pop_for_worker(1).expect("variant ready");
    assert_eq!(item.task_id, "variant");
}

#[test]
fn task_deps_cascade_fail_on_permanent_prereq_failure() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([
        t_with_id("P", "T", "", 1, "a", &[]),
        t_with_id("P", "T", "", 1, "b", &["a"]),
        t_with_id("P", "T", "", 1, "c", &["b"]),
    ])
    .expect("valid extend");
    let a = p.pop_for_worker(1).expect("a");
    assert_eq!(a.task_id, "a");
    let cascaded = p.on_item_failed_permanent(&phase("P"), "a");
    let mut cascaded_ids: Vec<_> = cascaded.iter().map(|i| i.task_id.clone()).collect();
    cascaded_ids.sort();
    assert_eq!(cascaded_ids, vec!["b".to_string(), "c".to_string()]);
    // No queued items remain — B and C never made it into a bucket.
    assert!(p.pop_for_worker(1).is_none());
}

#[test]
fn update_first_match_in_place_mutates_queued_match() {
    // Three items in two buckets; predicate matches the middle one.
    // The closure flips `preferred_secondaries` on the matched
    // entry and leaves the rest alone.
    let mut p = pool_with(&["P"], &[]);
    p.extend([
        t_with_id("P", "T", "", 1, "a", &[]),
        t_with_id("P", "T", "", 1, "b", &[]),
        t_with_id("P", "T", "", 1, "c", &[]),
    ])
    .expect("valid extend");
    let updated = p.update_first_match_in_place(
        |t| t.task_id == "b",
        |t| {
            t.preferred_secondaries = SoftPreferredSecondaries::new(vec!["sec-x".into()]);
        },
    );
    assert!(updated, "predicate must match `b`");
    let prefs: Vec<_> = p
        .iter()
        .map(|t| {
            (
                t.task_id.clone(),
                t.preferred_secondaries.as_slice().to_vec(),
            )
        })
        .collect();
    // Only `b` mutated; `a` and `c` stay default-empty.
    let by_id: std::collections::HashMap<_, _> = prefs.into_iter().collect();
    assert!(by_id["a"].is_empty());
    assert_eq!(by_id["b"], vec!["sec-x".to_string()]);
    assert!(by_id["c"].is_empty());
}

#[test]
fn update_first_match_in_place_visits_blocked_items() {
    // `b` depends on the still-pending `a`, so `b` lives in
    // `blocked` (not in any bucket). The update primitive must
    // still find and mutate it — operator-side preference updates
    // should land on blocked entries the moment they're queued
    // back into a bucket, not on a stale clone.
    let mut p = pool_with(&["P"], &[]);
    p.extend([
        t_with_id("P", "T", "", 1, "a", &[]),
        t_with_id("P", "T", "", 1, "b", &["a"]),
    ])
    .expect("valid extend");
    // Sanity: `b` is not in the bucket-side iter (it's in `blocked`).
    let ids_in_buckets: Vec<_> = p.iter().map(|t| t.task_id.clone()).collect();
    assert_eq!(ids_in_buckets, vec!["a".to_string()]);
    let updated = p.update_first_match_in_place(
        |t| t.task_id == "b",
        |t| {
            t.preferred_secondaries = SoftPreferredSecondaries::new(vec!["sec-y".into()]);
        },
    );
    assert!(updated, "predicate must match blocked `b`");
    // Pop `a` to unblock `b`, then verify `b`'s preference survived.
    p.pop_for_worker(1).expect("a");
    p.on_item_finished(&phase("P"), Some("a"));
    let b = p.pop_for_worker(1).expect("b unblocked");
    assert_eq!(b.task_id, "b");
    assert_eq!(
        b.preferred_secondaries.as_slice(),
        &["sec-y".to_string()][..]
    );
}

#[test]
fn update_first_match_in_place_returns_false_on_no_match() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([t_with_id("P", "T", "", 1, "a", &[])])
        .expect("valid");
    let updated = p.update_first_match_in_place(
        |t| t.task_id == "nonexistent",
        |t| {
            t.preferred_secondaries = SoftPreferredSecondaries::new(vec!["never".into()]);
        },
    );
    assert!(!updated, "no match → false");
    // `a` untouched.
    let a = p.pop_for_worker(1).expect("a");
    assert!(a.preferred_secondaries.as_slice().is_empty());
}
