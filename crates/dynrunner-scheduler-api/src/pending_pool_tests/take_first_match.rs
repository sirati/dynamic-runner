//! `retain` and `take_first_match` tests: queue-side predicate-driven
//! mutations that bypass the soft-pin dispatch path. Includes the
//! regression for Bug #23 (blocked-phase items must not be served
//! through `take_first_match`).

use super::{PhaseState, phase, pool_with, t};

#[test]
fn retain_drops_unmatched_items_across_buckets() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([
        t("P", "T1", "alpha", 10),
        t("P", "T1", "alpha", 20),
        t("P", "T2", "beta", 30),
        t("P", "T2", "", 40),
    ]).expect("valid extend");
    assert_eq!(p.len(), 4);
    p.retain(|item| item.size >= 25);
    // BTreeMap key order: (P, T2, "") sorts before (P, T2, "beta") because
    // empty < non-empty for AffinityId; so the free-pool item (size 40)
    // appears before the beta item (size 30) in iteration order.
    let remaining: Vec<u64> = p.iter().map(|i| i.size).collect();
    assert_eq!(remaining, vec![40, 30]);
}

#[test]
fn take_first_match_removes_and_returns_first_hit() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([
        t("P", "T", "alpha", 10),
        t("P", "T", "alpha", 20),
        t("P", "T", "beta", 30),
    ]).expect("valid extend");
    let taken = p.take_first_match(|i| i.size >= 15).expect("hit");
    assert_eq!(taken.size, 20);
    let rest: Vec<u64> = p.iter().map(|i| i.size).collect();
    assert_eq!(rest, vec![10, 30]);
}

#[test]
fn take_first_match_returns_none_when_no_match() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([t("P", "T", "alpha", 10)]).expect("valid extend");
    assert!(p.take_first_match(|i| i.size > 100).is_none());
    assert_eq!(p.len(), 1);
}

#[test]
fn take_first_match_empties_bucket_clears_pin_state() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([
        t("P", "T", "alpha", 10),
        t("P", "T", "beta", 30),
    ]).expect("valid extend");
    // Worker 1 claims alpha bucket via normal dispatch.
    let _ = p.pop_for_worker(1).unwrap();
    // alpha is now drained-by-dispatch; take a beta item via predicate.
    let taken = p.take_first_match(|i| i.size == 30).expect("hit");
    assert_eq!(taken.size, 30);
    // Worker 1 has no items left to consume — beta drained, alpha drained.
    assert!(p.pop_for_worker(1).is_none());
}

/// Regression for Bug #23: `take_first_match` walked all buckets in
/// `BTreeMap` order regardless of `phase_state`, so items belonging to
/// a `Blocked` phase could get dispatched (the primary's
/// `handle_primary_task_request` hit this on every request). The fix
/// filters the candidate set to phases in `Active` or `Draining` state
/// (Draining still serves to support reinject / requeue revival).
#[test]
fn take_first_match_skips_blocked_phases() {
    // Two phases A, B with B depending on A. A has no items but B has one.
    // B is Blocked because A hasn't been marked Done.
    let mut p = pool_with(&["A", "B"], &[("B", &["A"])]);
    p.extend([t("B", "T", "alpha", 1)]).expect("valid extend");
    assert_eq!(p.phase_state(&phase("B")), Some(PhaseState::Blocked));
    let got = p.take_first_match(|_| true);
    assert!(got.is_none(), "Blocked phase B's item must not dispatch");
    // The item is still in the pool — it must not have been removed.
    assert_eq!(p.len(), 1);

    // After A is marked done, B becomes Active and serves.
    p.mark_phase_done(&phase("A"));
    assert_eq!(p.phase_state(&phase("B")), Some(PhaseState::Active));
    let got = p.take_first_match(|_| true).expect("B is now Active");
    assert_eq!(got.phase_id.as_str(), "B");
}
