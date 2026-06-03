//! Tests for [`PendingPool::ready_in_active_phase`] — the
//! "queued-and-dispatchable-right-now" count primitive consumed by the
//! observer reporter's ready-in-queue stat and the idle-secondary
//! trigger.

use super::{PhaseState, phase, pool_with, t};

/// Items queued in an `Active` phase are counted.
#[test]
fn counts_queued_items_in_active_phase() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([
        t("P", "T", "alpha", 1),
        t("P", "T", "beta", 2),
        t("P", "T", "", 3),
    ])
    .expect("valid extend");
    assert_eq!(p.phase_state(&phase("P")), Some(PhaseState::Active));
    assert_eq!(p.ready_in_active_phase(), 3);
}

/// In-flight items are NOT ready (they've left the bucket); the count
/// drops as items are popped for workers.
#[test]
fn excludes_in_flight_items() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([t("P", "T", "alpha", 1), t("P", "T", "alpha", 2)])
        .expect("valid extend");
    assert_eq!(p.ready_in_active_phase(), 2);
    let _ = p.pop_for_worker(1).unwrap();
    assert_eq!(p.ready_in_active_phase(), 1);
}

/// Items in a `Blocked` phase (deps not yet satisfied) are NOT ready —
/// they sit in a bucket whose phase is not dispatchable.
#[test]
fn excludes_items_in_blocked_phase() {
    let mut p = pool_with(&["A", "B"], &[("B", &["A"])]);
    // B is Blocked until A is Done; its queued items must not count.
    p.extend([t("B", "T", "", 1)]).expect("valid extend");
    assert_eq!(p.phase_state(&phase("B")), Some(PhaseState::Blocked));
    assert_eq!(p.ready_in_active_phase(), 0);
    // Activate B; now its item is ready.
    p.mark_phase_done(&phase("A"));
    assert_eq!(p.phase_state(&phase("B")), Some(PhaseState::Active));
    assert_eq!(p.ready_in_active_phase(), 1);
}

/// Task-level blocked items (unresolved `task_depends_on`) live in the
/// `blocked` map, never in a bucket, so they are never counted as ready
/// — even while their phase is `Active`.
#[test]
fn excludes_task_dep_blocked_items() {
    let mut p = pool_with(&["P"], &[]);
    // `dep` is a prereq for `child`; `child` is blocked until `dep`
    // finishes. Only `dep` is queued/ready.
    let dep = {
        let mut task = t("P", "T", "", 1);
        task.task_id = "dep".to_string();
        task
    };
    let child = {
        let mut task = t("P", "T", "", 2);
        task.task_id = "child".to_string();
        task.task_depends_on = vec![dynrunner_core::TaskDep {
            task_id: "dep".to_string(),
            phase_id: dynrunner_core::PhaseId::from("P"),
            inherit_outputs: false,
        }];
        task
    };
    p.extend([dep, child]).expect("valid extend");
    // Only `dep` is in a bucket; `child` is in the blocked map.
    assert_eq!(p.ready_in_active_phase(), 1);
}

/// A `Drained` phase holds no queued work; the count is zero.
#[test]
fn excludes_drained_phase() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([t("P", "T", "", 1)]).expect("valid extend");
    let _ = p.pop_for_worker(1).unwrap();
    p.on_item_finished(&phase("P"), None);
    assert_eq!(p.phase_state(&phase("P")), Some(PhaseState::Drained));
    assert_eq!(p.ready_in_active_phase(), 0);
}

/// Empty pool → zero.
#[test]
fn empty_pool_is_zero() {
    let p = pool_with(&["P"], &[]);
    assert_eq!(p.ready_in_active_phase(), 0);
}
