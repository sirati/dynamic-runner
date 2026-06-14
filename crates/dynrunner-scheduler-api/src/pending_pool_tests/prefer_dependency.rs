//! #519 dispatch-bias pool queries: the three single-concern reads the
//! distributed primary's `prefer_dependency_gate_holds` + per-candidate bias
//! compose, plus the canonical `is_dead_ended` dead-end leaf.
//!
//! * `ready_dispatchable_below(threshold)` — SHORT-CIRCUITING "is the ready
//!   (deps-met, worker-dispatchable) pool shallower than `threshold`". Stops
//!   the moment it has counted `threshold` eligible items (O(threshold), never
//!   O(queued)).
//! * `has_live_blocked()` — ∃ a blocked task none of whose unmet deps is
//!   dead-ended (clause-2). A `failed_tasks` dep dooms; a retry-eligible
//!   `soft_failed` dep does NOT (the dependent stays LIVE — guardrail).
//! * `is_ready_prerequisite_of_live_blocked(task_id)` — the per-candidate bias:
//!   a ready DIRECT prerequisite of a live blocked task, via the `dependents_of`
//!   reverse index.

use dynrunner_core::{PhaseId, TaskDep, TaskInfo, TaskKind};

use super::{PhaseState, phase, pool_with, setup_t, t};

/// Build a `TaskInfo<()>` with an explicit id + same-phase deps so a dependent
/// can be parked in the task-level `blocked` map.
fn t_id(phase: &str, size: u64, id: &str, deps: &[&str]) -> TaskInfo<()> {
    let mut item = t(phase, "T", "", size);
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

// ───────────────────────── ready_dispatchable_below ─────────────────────────

#[test]
fn ready_below_zero_threshold_is_vacuously_false() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([t("P", "T", "", 1)]).expect("valid extend");
    assert!(
        !p.ready_dispatchable_below(0),
        "zero items is not 'below 0' — the gate is off for a 0-worker fleet"
    );
}

#[test]
fn ready_below_counts_only_eligible_queued_items() {
    let mut p = pool_with(&["P"], &[]);
    // 3 ready Work items.
    p.extend([t("P", "T", "", 1), t("P", "T", "", 1), t("P", "T", "", 1)])
        .expect("valid extend");
    assert!(p.ready_dispatchable_below(4), "3 ready < 4");
    assert!(!p.ready_dispatchable_below(3), "3 ready is NOT < 3");
    assert!(!p.ready_dispatchable_below(2), "3 ready is NOT < 2");
    assert!(p.ready_dispatchable_below(5), "3 ready < 5");
}

#[test]
fn ready_below_excludes_setup_tasks() {
    // A Setup task is queued but NOT worker-dispatchable — it must not count
    // toward the ready pool (same `dispatch_eligible` gate the view uses).
    let mut p = pool_with(&["P"], &[]);
    p.extend([setup_t("P", "T", "", 1), t("P", "T", "", 1)])
        .expect("valid extend");
    // Only the 1 Work item is ready.
    assert!(p.ready_dispatchable_below(2), "1 Work ready < 2");
    assert!(
        !p.ready_dispatchable_below(1),
        "1 Work ready is NOT < 1 (the Setup task does not count)"
    );
}

#[test]
fn ready_below_excludes_blocked_phase_items() {
    // B depends on A → B starts Blocked; its queued item is not ready.
    let mut p = pool_with(&["A", "B"], &[("B", &["A"])]);
    p.extend([t("B", "T", "", 1)]).expect("valid extend");
    assert_eq!(p.phase_state(&phase("B")), Some(PhaseState::Blocked));
    // 0 ready (the Blocked-phase item does not count) → the pool IS below
    // any positive threshold; and is never AT a positive threshold.
    assert!(
        p.ready_dispatchable_below(1),
        "0 ready (Blocked-phase item excluded) is below threshold 1"
    );
    assert!(
        p.ready_dispatchable_below(100),
        "0 ready is below threshold 100"
    );
    // Sanity: once B activates, its item becomes ready and counts.
    p.mark_phase_done(&phase("A"));
    assert_eq!(p.phase_state(&phase("B")), Some(PhaseState::Active));
    assert!(
        !p.ready_dispatchable_below(1),
        "1 ready (B now Active) is NOT below 1"
    );
}

#[test]
fn ready_below_excludes_task_blocked_items() {
    // `b` depends on `a`; before `a` finishes, `b` sits in the blocked map and
    // is not ready. Only `a` counts.
    let mut p = pool_with(&["P"], &[]);
    p.extend([t_id("P", 1, "a", &[]), t_id("P", 1, "b", &["a"])])
        .expect("valid extend");
    assert!(p.ready_dispatchable_below(2), "only `a` ready: 1 < 2");
    assert!(!p.ready_dispatchable_below(1), "1 ready (`a`) is NOT < 1");
}

/// PERF: `ready_dispatchable_below(threshold)` must NOT scan past `threshold`.
/// Seed `threshold` eligible items followed by MANY more; the call returns
/// `false` (not below) via the early `return false` the moment the
/// `threshold`-th eligible item is seen — it never folds the whole queue (the
/// #504-class O(66k) sweep). The result is identical whether or not it
/// short-circuits, but the cheap-return is the contract: with a huge tail the
/// call still completes instantly. We also pin that a DIFFERENT bucket order
/// can't change the answer once threshold is met.
#[test]
fn ready_below_short_circuits_at_threshold() {
    let mut p = pool_with(&["P"], &[]);
    let big = 5_000usize;
    let items: Vec<TaskInfo<()>> = (0..big).map(|_| t("P", "T", "", 1)).collect();
    p.extend(items).expect("valid extend");
    // Exactly at a small threshold: NOT below (the early return fires after
    // counting `threshold` of the 5000 — the remaining ~4996 are never read).
    assert!(
        !p.ready_dispatchable_below(4),
        "5000 ready is NOT < 4 — short-circuit returns false at the 4th item"
    );
    // Far below the total: still NOT below (short-circuit again).
    assert!(!p.ready_dispatchable_below(100), "5000 ready is NOT < 100");
    // Above the total: must scan all 5000 and report below.
    assert!(
        p.ready_dispatchable_below(big + 1),
        "5000 ready < 5001 (the only path that legitimately reads the whole queue)"
    );
}

// ───────────────────────────── has_live_blocked ─────────────────────────────

#[test]
fn has_live_blocked_false_when_nothing_blocked() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([t("P", "T", "", 1)]).expect("valid extend");
    assert!(!p.has_live_blocked(), "no blocked tasks → no live blocked");
}

#[test]
fn has_live_blocked_true_for_blocked_on_live_prereq() {
    // `b` blocked on `a` (a is ready, in-flight-able — not dead). b is LIVE.
    let mut p = pool_with(&["P"], &[]);
    p.extend([t_id("P", 1, "a", &[]), t_id("P", 1, "b", &["a"])])
        .expect("valid extend");
    assert!(
        p.has_live_blocked(),
        "b is blocked on the live prereq a → b is a live blocked task"
    );
}

#[test]
fn has_live_blocked_excludes_blocked_on_failed_prereq() {
    // Two independent chains so the pool is non-empty after the cascade:
    //   a → b  (a will fail permanently → b cascade-evicted, doomed)
    //   c → d  (c stays live → d is a live blocked task)
    let mut p = pool_with(&["P"], &[]);
    p.extend([
        t_id("P", 1, "a", &[]),
        t_id("P", 1, "b", &["a"]),
        t_id("P", 1, "c", &[]),
        t_id("P", 1, "d", &["c"]),
    ])
    .expect("valid extend");
    // Dispatch then permanently fail `a`: the cascade evicts `b` from blocked.
    let _ = p.pop_for_worker(1);
    let _ = p.pop_for_worker(2); // some queued item leaves; harmless
    // Fail `a` permanently — `b` cascades into failed_tasks (no longer blocked).
    p.on_item_failed_permanent(&phase("P"), "a");
    assert!(
        p.has_live_blocked(),
        "b is gone (cascaded); d is still blocked on the live c → live blocked exists"
    );
    // Now also fail `c`: `d` cascades out → no live blocked remains.
    p.on_item_failed_permanent(&phase("P"), "c");
    assert!(
        !p.has_live_blocked(),
        "both dependents cascade-failed → no live blocked task remains"
    );
}

/// GUARDRAIL: a `soft_failed` (retry-decision-pending) prereq is NOT
/// dead-ended — it may yet be reinjected — so its dependent stays a LIVE
/// blocked task. Only the DEFINITELY-doomed (`failed_tasks`) prereq excludes.
#[test]
fn has_live_blocked_keeps_blocked_on_soft_failed_prereq() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([t_id("P", 1, "a", &[]), t_id("P", 1, "b", &["a"])])
        .expect("valid extend");
    // Dispatch `a`, then mark it soft-failed (retry pending) — NOT permanent.
    let _ = p.pop_for_worker(1);
    p.on_item_failed_pending_retry(&phase("P"), "a");
    // `b` is still blocked on `a`; `a` is soft_failed but revivable.
    assert!(
        p.has_live_blocked(),
        "a is soft_failed (retry-eligible), not dead-ended → b stays a LIVE \
         blocked task (its prerequisite a still matters)"
    );
}

// ──────────────────── is_ready_prerequisite_of_live_blocked ──────────────────

#[test]
fn prereq_true_for_ready_direct_prerequisite_of_live_blocked() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([t_id("P", 1, "a", &[]), t_id("P", 1, "b", &["a"])])
        .expect("valid extend");
    // `a` is ready (queued) AND a direct prerequisite of the live blocked `b`.
    assert!(
        p.is_ready_prerequisite_of_live_blocked("a"),
        "a is a ready direct prerequisite of the live blocked b"
    );
    // `b` itself has no dependents → not a prerequisite of anything blocked.
    assert!(
        !p.is_ready_prerequisite_of_live_blocked("b"),
        "b has no dependent blocked task"
    );
}

#[test]
fn prereq_false_for_task_with_no_dependents() {
    // Two unrelated ready tasks, nothing blocked.
    let mut p = pool_with(&["P"], &[]);
    p.extend([t_id("P", 1, "x", &[]), t_id("P", 1, "y", &[])])
        .expect("valid extend");
    assert!(!p.is_ready_prerequisite_of_live_blocked("x"));
    assert!(!p.is_ready_prerequisite_of_live_blocked("y"));
}

#[test]
fn prereq_false_when_only_dependent_is_doomed() {
    // a → b, where b is also blocked on a permanently-failed task f. Once f
    // fails, b cascade-evicts, so a is no longer a prerequisite of any LIVE
    // blocked task even though a itself is still ready.
    let mut p = pool_with(&["P"], &[]);
    p.extend([
        t_id("P", 1, "f", &[]),
        t_id("P", 1, "a", &[]),
        t_id("P", 1, "b", &["a", "f"]),
    ])
    .expect("valid extend");
    // Before any failure: a IS a prerequisite of the live blocked b.
    assert!(
        p.is_ready_prerequisite_of_live_blocked("a"),
        "pre-failure: b is live, blocked on both a and f"
    );
    // Dispatch + permanently fail f → b cascade-fails out of the blocked map.
    let _ = p.pop_for_worker(1);
    p.on_item_failed_permanent(&phase("P"), "f");
    assert!(
        !p.is_ready_prerequisite_of_live_blocked("a"),
        "b cascaded (doomed by failed f); a's only dependent is gone → a is \
         not a prerequisite of any live blocked task"
    );
}

#[test]
fn prereq_resolves_through_kinds_via_direct_dependents_only() {
    // DIRECT only: a → mid (Setup, blocked on a) → leaf (blocked on mid).
    // `a` is a DIRECT prerequisite of `mid` (a Setup gate, but still a blocked
    // task here), so the direct check on `a` finds `mid`. The transitive
    // `leaf` is NOT consulted (no transitive walk) — but `a` is already true
    // via `mid`, and `mid` (ready? it's blocked) is the dependent.
    let mut p = pool_with(&["P"], &[]);
    let mut mid = setup_t("P", "T", "", 1);
    mid.task_id = "mid".into();
    mid.task_depends_on = vec![TaskDep {
        task_id: "a".into(),
        phase_id: phase("P"),
        inherit_outputs: false,
    }];
    let mut leaf = t_id("P", 1, "leaf", &["mid"]);
    leaf.kind = TaskKind::Work;
    p.extend([t_id("P", 1, "a", &[]), mid, leaf])
        .expect("valid extend");
    // `a`'s direct dependent `mid` is a live blocked task → true.
    assert!(
        p.is_ready_prerequisite_of_live_blocked("a"),
        "a is a direct prerequisite of the live blocked mid"
    );
}
