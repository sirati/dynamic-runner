//! `has_queued_dispatchable` / `blocked_len` query tests: the queued-side
//! dispatchability and task-level-blocked reads the distributed primary's
//! starvation oracle composes. Distinct from `len()` / `is_empty()`, which
//! fold in-flight + blocked and so cannot tell "nothing left to hand out"
//! from "everything left is in-flight/blocked".

use dynrunner_core::{PhaseId, TaskDep, TaskInfo};

use super::{PhaseState, phase, pool_with, t};

/// Override the synthetic id `t(...)` assigns and attach same-phase deps,
/// so a dependent can be parked in the task-level `blocked` map.
fn t_with_id(phase: &str, ty: &str, size: u64, id: &str, deps: &[&str]) -> TaskInfo<()> {
    let mut item = t(phase, ty, "", size);
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
fn has_queued_dispatchable_false_on_empty_pool() {
    let p = pool_with(&["P"], &[]);
    assert!(!p.has_queued_dispatchable());
}

#[test]
fn has_queued_dispatchable_true_for_queued_item_in_active_phase() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([t("P", "T", "", 1)]).expect("valid extend");
    assert!(
        p.has_queued_dispatchable(),
        "a queued item in an Active phase is dispatchable"
    );
}

#[test]
fn has_queued_dispatchable_false_when_only_item_is_in_a_blocked_phase() {
    // B depends on A → B starts Blocked; its queued item is NOT
    // dispatchable until A drains.
    let mut p = pool_with(&["A", "B"], &[("B", &["A"])]);
    p.extend([t("B", "T", "", 1)]).expect("valid extend");
    assert_eq!(p.phase_state(&phase("B")), Some(PhaseState::Blocked));
    assert!(
        !p.has_queued_dispatchable(),
        "an item parked in a Blocked phase is not dispatchable"
    );
    // Once A drains, B activates and the item becomes dispatchable.
    p.mark_phase_done(&phase("A"));
    assert_eq!(p.phase_state(&phase("B")), Some(PhaseState::Active));
    assert!(p.has_queued_dispatchable());
}

#[test]
fn has_queued_dispatchable_false_when_only_remaining_item_is_task_blocked() {
    // `b` depends on `a`; after `a` dispatches, `b` sits in the task-level
    // `blocked` map (no queued bucket entry) until `a` finishes.
    let mut p = pool_with(&["P"], &[]);
    p.extend([
        t_with_id("P", "T", 1, "a", &[]),
        t_with_id("P", "T", 1, "b", &["a"]),
    ])
    .expect("valid extend");
    // `a` queued, `b` blocked → still dispatchable (a is queued).
    assert!(p.has_queued_dispatchable());
    assert_eq!(p.blocked_len(), 1, "b waits in the blocked map");
    // Dispatch `a`; now nothing is queued and `b` is still blocked.
    let a = p.pop_for_worker(1).expect("a dispatchable");
    assert_eq!(a.task_id, "a");
    assert!(
        !p.has_queued_dispatchable(),
        "only the task-blocked `b` remains — nothing dispatchable"
    );
    assert_eq!(p.blocked_len(), 1);
    // Finishing `a` unblocks `b` into a queued bucket → dispatchable again.
    p.on_item_finished(&phase("P"), Some("a"));
    assert_eq!(p.blocked_len(), 0);
    assert!(p.has_queued_dispatchable());
}
