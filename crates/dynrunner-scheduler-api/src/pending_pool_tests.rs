//! Unit tests for `PendingPool`. Kept in a sibling file via
//! `#[path = ...]` to keep the module readable (matches the
//! `types_tests.rs` pattern in `dynrunner-core`).

use std::collections::HashMap;

use super::{PendingPool, PendingPoolError, PhaseState};
use dynrunner_core::{AffinityId, PhaseId, TaskInfo, TypeId};

/// Test fixture: build a `TaskInfo<()>` with the provided phase / type / affinity.
/// An empty affinity string is mapped to `None` so the bucket falls into the
/// free-pool sentinel inside the pool.
fn t(phase: &str, ty: &str, affinity: &str, size: u64) -> TaskInfo<()> {
    TaskInfo {
        path: std::path::PathBuf::from(format!("/tmp/{phase}_{ty}_{affinity}_{size}")),
        size,
        identifier: (),
        phase_id: PhaseId::from(phase),
        type_id: TypeId::from(ty),
        affinity_id: if affinity.is_empty() {
            None
        } else {
            Some(AffinityId::from(affinity))
        },
        payload: serde_json::Value::Null,
    }
}

fn phase(s: &str) -> PhaseId {
    PhaseId::from(s)
}

fn pool_with(phases: &[&str], deps: &[(&str, &[&str])]) -> PendingPool<()> {
    let phases: Vec<PhaseId> = phases.iter().map(|p| phase(p)).collect();
    let mut deps_map: HashMap<PhaseId, Vec<PhaseId>> = HashMap::new();
    for (child, parents) in deps {
        deps_map.insert(
            phase(child),
            parents.iter().map(|p| phase(p)).collect(),
        );
    }
    PendingPool::new(phases, deps_map).expect("valid graph")
}

#[test]
fn new_rejects_dependency_cycle() {
    let mut deps: HashMap<PhaseId, Vec<PhaseId>> = HashMap::new();
    deps.insert(phase("B"), vec![phase("A")]);
    deps.insert(phase("C"), vec![phase("B")]);
    deps.insert(phase("A"), vec![phase("C")]);
    let res =
        PendingPool::<()>::new([phase("A"), phase("B"), phase("C")], deps);
    assert!(matches!(res, Err(PendingPoolError::DependencyCycle(_))));
}

#[test]
fn new_rejects_unknown_dependency() {
    let mut deps: HashMap<PhaseId, Vec<PhaseId>> = HashMap::new();
    deps.insert(phase("B"), vec![phase("Z")]);
    let res = PendingPool::<()>::new([phase("A"), phase("B")], deps);
    assert!(matches!(res, Err(PendingPoolError::UnknownDependency(_))));
}

#[test]
fn new_initial_states_active_for_zero_deps_blocked_otherwise() {
    let p = pool_with(&["A", "B", "C"], &[("B", &["A"]), ("C", &["B"])]);
    assert_eq!(p.phase_state(&phase("A")), Some(PhaseState::Active));
    assert_eq!(p.phase_state(&phase("B")), Some(PhaseState::Blocked));
    assert_eq!(p.phase_state(&phase("C")), Some(PhaseState::Blocked));
}

#[test]
fn extend_distributes_items_into_buckets() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([
        t("P", "T1", "alpha", 10),
        t("P", "T1", "alpha", 20),
        t("P", "T1", "beta", 30),
        t("P", "T2", "", 40),
    ]);
    // Total queued: 4
    assert_eq!(p.len(), 4);
    // Distinct buckets in iteration order (BTreeMap by key):
    let paths: Vec<_> = p.iter().map(|i| i.path.clone()).collect();
    assert_eq!(paths.len(), 4);
}

#[test]
fn pop_for_worker_returns_none_when_empty() {
    let mut p = pool_with(&["P"], &[]);
    assert!(p.pop_for_worker(1).is_none());
}

#[test]
fn pop_honors_affinity_until_bucket_drains() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([
        t("P", "T", "alpha", 1),
        t("P", "T", "alpha", 2),
        t("P", "T", "alpha", 3),
        t("P", "T", "alpha", 4),
        t("P", "T", "beta", 5),
        t("P", "T", "beta", 6),
        t("P", "T", "beta", 7),
        t("P", "T", "beta", 8),
    ]);
    // Worker A claims one bucket. BTreeMap key order makes "alpha" < "beta",
    // so worker 1 picks alpha first.
    let first = p.pop_for_worker(1).expect("first");
    let claimed = first.affinity_id.clone().unwrap();
    // Subsequent pops by worker 1 stay in the same bucket.
    for _ in 0..3 {
        let it = p.pop_for_worker(1).expect("bucketed item");
        assert_eq!(it.affinity_id.as_ref().unwrap(), &claimed);
    }
}

#[test]
fn affinity_clears_when_bucket_drains_then_pulls_other_bucket() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([
        t("P", "T", "alpha", 1),
        t("P", "T", "alpha", 2),
        t("P", "T", "beta", 5),
    ]);
    let _ = p.pop_for_worker(1).unwrap(); // alpha #1 (claim)
    let _ = p.pop_for_worker(1).unwrap(); // alpha #2 (drain alpha)
    let next = p.pop_for_worker(1).expect("from beta now");
    assert_eq!(next.affinity_id.as_ref().unwrap().as_str(), "beta");
}

#[test]
fn free_pool_served_only_after_typed_buckets_but_never_starved() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([
        t("P", "T", "", 9),       // free-pool item
        t("P", "T", "alpha", 10), // typed
    ]);
    // Step 2 prefers typed over free pool — worker 1 gets alpha first.
    let first = p.pop_for_worker(1).unwrap();
    assert_eq!(first.affinity_id.as_ref().unwrap().as_str(), "alpha");
    // Worker 2 then gets the free-pool item (no other typed bucket left).
    let second = p.pop_for_worker(2).unwrap();
    assert!(second.affinity_id.is_none());
}

#[test]
fn on_item_finished_drains_phase() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([t("P", "T", "alpha", 1)]);
    let _ = p.pop_for_worker(1).unwrap();
    // Phase is Draining now (queue empty, in_flight = 1).
    assert_eq!(p.phase_state(&phase("P")), Some(PhaseState::Draining));
    p.on_item_finished(&phase("P"));
    assert_eq!(p.phase_state(&phase("P")), Some(PhaseState::Drained));
    assert_eq!(p.in_flight(&phase("P")), 0);
}

#[test]
fn requeue_inserts_at_front_and_flips_draining_back_to_active() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([t("P", "T", "alpha", 1)]);
    let item = p.pop_for_worker(1).unwrap();
    assert_eq!(p.phase_state(&phase("P")), Some(PhaseState::Draining));
    p.requeue(item);
    assert_eq!(p.phase_state(&phase("P")), Some(PhaseState::Active));
    // Front of bucket is the requeued item.
    let again = p.pop_for_worker(1).unwrap();
    assert_eq!(again.size, 1);
}

#[test]
fn release_worker_unpins_only_if_last_pin() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([
        t("P", "T", "alpha", 1),
        t("P", "T", "alpha", 2),
        t("P", "T", "alpha", 3),
    ]);
    // Worker 1 claims alpha. Worker 2 also picks (co-pin via step 4 after
    // the only typed bucket is already pinned).
    let _ = p.pop_for_worker(1).unwrap();
    let _ = p.pop_for_worker(2).unwrap();
    // Release worker 1 — bucket still has items, worker 2 still pinned.
    p.release_worker(1);
    // Worker 2's next pop should still come from alpha.
    let it = p.pop_for_worker(2).unwrap();
    assert_eq!(it.affinity_id.as_ref().unwrap().as_str(), "alpha");
}

#[test]
fn poll_drain_transitions_is_one_shot() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([t("P", "T", "alpha", 1)]);
    let _ = p.pop_for_worker(1).unwrap();
    p.on_item_finished(&phase("P"));
    let first = p.poll_drain_transitions();
    assert_eq!(first, vec![phase("P")]);
    let second = p.poll_drain_transitions();
    assert!(second.is_empty());
}

#[test]
fn mark_phase_done_activates_dependents() {
    let mut p = pool_with(&["A", "B"], &[("B", &["A"])]);
    assert_eq!(p.phase_state(&phase("B")), Some(PhaseState::Blocked));
    p.mark_phase_done(&phase("A"));
    assert_eq!(p.phase_state(&phase("A")), Some(PhaseState::Done));
    assert_eq!(p.phase_state(&phase("B")), Some(PhaseState::Active));
}

/// `view_for_worker` produces the same priority order as `pop_for_worker`
/// for a fresh worker (no affinity, no pins) — typed buckets first,
/// free-pool last. The scheduler's chosen index commits via
/// `take_from_view`.
#[test]
fn view_for_worker_orders_typed_then_free_pool() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([
        t("P", "T", "", 9),       // free-pool item
        t("P", "T", "alpha", 10), // typed
    ]);
    let view = p.view_for_worker(1);
    assert_eq!(view.len(), 2);
    // First entry is from the typed bucket (step 2 wins over step 3).
    assert_eq!(view.as_slice()[0].affinity_id.as_ref().unwrap().as_str(), "alpha");
    // Second is the free-pool item.
    assert!(view.as_slice()[1].affinity_id.is_none());
}

/// `take_from_view` commits the scheduler's chosen index — soft-pin,
/// in-flight, and drain bookkeeping fire just like `pop_for_worker`.
#[test]
fn take_from_view_commits_chosen_index() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([
        t("P", "T", "alpha", 1),
        t("P", "T", "alpha", 2),
        t("P", "T", "beta", 3),
    ]);
    // Worker 1 sees both typed buckets.
    let view = p.view_for_worker(1);
    // Find the beta entry (BTreeMap key order: alpha < beta).
    let beta_idx = view
        .as_slice()
        .iter()
        .position(|t| t.affinity_id.as_ref().unwrap().as_str() == "beta")
        .expect("beta visible");
    let item = p.take_from_view(view, beta_idx);
    assert_eq!(item.affinity_id.as_ref().unwrap().as_str(), "beta");
    // Worker 1 is now pinned to beta; subsequent pop stays in beta until
    // it drains.
    assert_eq!(p.in_flight(&phase("P")), 1);
}

/// Empty pool → empty view; `tasks()` is `&[]`.
#[test]
fn view_for_worker_empty_pool() {
    let p = pool_with(&["P"], &[]);
    let view = p.view_for_worker(1);
    assert!(view.is_empty());
    assert_eq!(view.len(), 0);
}

#[test]
fn reinject_revives_drained_phase_to_active() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([t("P", "T", "alpha", 1)]);
    let item = p.pop_for_worker(1).unwrap();
    p.on_item_finished(&phase("P"));
    assert_eq!(p.phase_state(&phase("P")), Some(PhaseState::Drained));
    p.reinject(item);
    assert_eq!(p.phase_state(&phase("P")), Some(PhaseState::Active));
    // No drained notification leaks through since reinject cleared it.
    assert!(p.poll_drain_transitions().is_empty());
    // The reinjected item is at the back of its bucket and dispatchable.
    let again = p.pop_for_worker(1).unwrap();
    assert_eq!(again.size, 1);
}

#[test]
fn drain_queued_empties_buckets_without_touching_inflight() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([
        t("P", "T", "alpha", 1),
        t("P", "T", "beta", 2),
        t("P", "T", "", 3),
    ]);
    // Take one to bump in-flight.
    let _ = p.pop_for_worker(1).unwrap();
    let in_flight_before = p.in_flight(&phase("P"));
    let drained = p.drain_queued();
    assert_eq!(drained.len(), 2, "two queued items expected");
    assert_eq!(p.in_flight(&phase("P")), in_flight_before);
    // Bucket totals are now zero queued.
    assert_eq!(p.iter().count(), 0);
}

#[test]
fn view_for_worker_orders_pinned_then_typed_then_free_then_copin() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([
        // alpha: typed bucket, will become worker 1's pin
        t("P", "T", "alpha", 1),
        t("P", "T", "alpha", 2),
        // beta: another typed bucket, unpinned at start
        t("P", "T", "beta", 3),
        // free pool
        t("P", "T", "", 4),
    ]);

    // First, worker 1 grabs alpha (Step 2). After this, the view for
    // worker 1 should put alpha first (Step 1: pinned), then beta
    // (Step 2: unpinned typed), then free pool.
    let _ = p.pop_for_worker(1).unwrap();

    let view = p.view_for_worker(1);
    let sizes: Vec<u64> = view.as_slice().iter().map(|t| t.size).collect();
    // alpha #2 (the remaining alpha item) → beta #3 → free #4
    assert_eq!(sizes, vec![2, 3, 4], "got {sizes:?}");
}

#[test]
fn view_for_worker_skips_blocked_phases() {
    let mut p = pool_with(&["A", "B"], &[("B", &["A"])]);
    p.extend([
        t("A", "T", "", 1),
        t("B", "T", "", 2),
    ]);
    let view = p.view_for_worker(1);
    // Only A's item is visible; B is Blocked.
    assert_eq!(view.len(), 1);
    assert_eq!(view.as_slice()[0].size, 1);
}

#[test]
fn take_from_view_removes_chosen_item_and_records_affinity() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([
        t("P", "T", "alpha", 1),
        t("P", "T", "beta", 2),
    ]);
    let view = p.view_for_worker(1);
    // View order: alpha (BTreeMap "alpha" < "beta") then beta. Pick beta
    // to verify non-zero index removal.
    assert_eq!(view.as_slice()[0].size, 1);
    assert_eq!(view.as_slice()[1].size, 2);
    let taken = p.take_from_view(view, 1);
    assert_eq!(taken.size, 2);
    assert_eq!(taken.affinity_id.as_ref().unwrap().as_str(), "beta");
    // Worker 1 is now pinned to beta. Next view starts with the alpha
    // bucket only (alpha #1 still present, beta drained).
    let view2 = p.view_for_worker(1);
    let sizes: Vec<u64> = view2.as_slice().iter().map(|t| t.size).collect();
    assert_eq!(sizes, vec![1]);
    // In-flight count for P incremented to 1.
    assert_eq!(p.in_flight(&phase("P")), 1);
}

#[test]
fn take_from_view_increments_in_flight_and_drains_phase() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([t("P", "T", "alpha", 1)]);
    let view = p.view_for_worker(1);
    assert_eq!(view.len(), 1);
    let _ = p.take_from_view(view, 0);
    assert_eq!(p.phase_state(&phase("P")), Some(PhaseState::Draining));
    assert_eq!(p.in_flight(&phase("P")), 1);
}

#[test]
fn view_for_worker_empty_when_no_eligible_items() {
    let mut p = pool_with(&["P"], &[]);
    let view = p.view_for_worker(0);
    assert!(view.is_empty());
    p.extend([t("P", "T", "", 1)]);
    let view = p.view_for_worker(0);
    assert_eq!(view.len(), 1);
}

#[test]
fn retain_drops_unmatched_items_across_buckets() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([
        t("P", "T1", "alpha", 10),
        t("P", "T1", "alpha", 20),
        t("P", "T2", "beta", 30),
        t("P", "T2", "", 40),
    ]);
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
    ]);
    let taken = p.take_first_match(|i| i.size >= 15).expect("hit");
    assert_eq!(taken.size, 20);
    let rest: Vec<u64> = p.iter().map(|i| i.size).collect();
    assert_eq!(rest, vec![10, 30]);
}

#[test]
fn take_first_match_returns_none_when_no_match() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([t("P", "T", "alpha", 10)]);
    assert!(p.take_first_match(|i| i.size > 100).is_none());
    assert_eq!(p.len(), 1);
}

#[test]
fn take_first_match_empties_bucket_clears_pin_state() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([
        t("P", "T", "alpha", 10),
        t("P", "T", "beta", 30),
    ]);
    // Worker 1 claims alpha bucket via normal dispatch.
    let _ = p.pop_for_worker(1).unwrap();
    // alpha is now drained-by-dispatch; take a beta item via predicate.
    let taken = p.take_first_match(|i| i.size == 30).expect("hit");
    assert_eq!(taken.size, 30);
    // Worker 1 has no items left to consume — beta drained, alpha drained.
    assert!(p.pop_for_worker(1).is_none());
}

#[test]
fn activation_cascade_through_chain() {
    let mut p = pool_with(
        &["A", "B", "C"],
        &[("B", &["A"]), ("C", &["B"])],
    );
    p.mark_phase_done(&phase("A"));
    assert_eq!(p.phase_state(&phase("B")), Some(PhaseState::Active));
    assert_eq!(p.phase_state(&phase("C")), Some(PhaseState::Blocked));
    p.mark_phase_done(&phase("B"));
    assert_eq!(p.phase_state(&phase("C")), Some(PhaseState::Active));
}
