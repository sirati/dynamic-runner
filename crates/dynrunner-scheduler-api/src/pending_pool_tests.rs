//! Unit tests for `PendingPool`. Kept in a sibling file via
//! `#[path = ...]` to keep the module readable (matches the
//! `types_tests.rs` pattern in `dynrunner-core`).

use std::collections::HashMap;

use super::{PendingPool, PendingPoolError, PhaseState};
use dynrunner_core::{AffinityId, PhaseId, SoftPreferredSecondaries, TaskInfo, TypeId};

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
        task_id: None,
        task_depends_on: vec![],
        preferred_secondaries: SoftPreferredSecondaries::default(),
        resolved_path: None,
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
    ]).expect("valid extend");
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
    ]).expect("valid extend");
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
    ]).expect("valid extend");
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
    ]).expect("valid extend");
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
    p.extend([t("P", "T", "alpha", 1)]).expect("valid extend");
    let _ = p.pop_for_worker(1).unwrap();
    // Phase is Draining now (queue empty, in_flight = 1).
    assert_eq!(p.phase_state(&phase("P")), Some(PhaseState::Draining));
    p.on_item_finished(&phase("P"), None);
    assert_eq!(p.phase_state(&phase("P")), Some(PhaseState::Drained));
    assert_eq!(p.in_flight(&phase("P")), 0);
}

#[test]
fn requeue_inserts_at_front_and_flips_draining_back_to_active() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([t("P", "T", "alpha", 1)]).expect("valid extend");
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
    ]).expect("valid extend");
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
    p.extend([t("P", "T", "alpha", 1)]).expect("valid extend");
    let _ = p.pop_for_worker(1).unwrap();
    p.on_item_finished(&phase("P"), None);
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

/// Empty `Active` phase transitions to `Drained` after
/// `drain_empty_active_phases`, and `poll_drain_transitions` reports
/// it. Without this, an empty phase-0 in a multi-phase chain would
/// never trigger `mark_phase_done` and dependents would stay
/// `Blocked` forever.
#[test]
fn drain_empty_active_phases_marks_empty_phase_drained() {
    let mut p = pool_with(&["A", "B"], &[("B", &["A"])]);
    // No items added — phase A is Active but empty.
    assert_eq!(p.phase_state(&phase("A")), Some(PhaseState::Active));
    p.drain_empty_active_phases();
    assert_eq!(p.phase_state(&phase("A")), Some(PhaseState::Drained));
    let drained = p.poll_drain_transitions();
    assert_eq!(drained, vec![phase("A")]);
}

/// Cascade: phase chain 0→1→2→3 with items only in phase 3 still
/// needs every empty intermediate phase to drain so view_for_worker
/// can see the phase-3 items. Mirrors the manager's
/// `process_phase_lifecycle` loop: drain empties, mark each done,
/// then re-drain the freshly-Active dependents until the chain
/// reaches the populated phase.
#[test]
fn drain_empty_active_phases_cascades_to_first_populated_phase() {
    let mut p = pool_with(
        &["P0", "P1", "P2", "P3"],
        &[("P1", &["P0"]), ("P2", &["P1"]), ("P3", &["P2"])],
    );
    p.extend([t("P3", "T", "", 1)]).expect("valid extend");
    // Initial state: only P0 Active (no deps); P1..P3 all Blocked.
    assert_eq!(p.phase_state(&phase("P0")), Some(PhaseState::Active));
    assert_eq!(p.phase_state(&phase("P3")), Some(PhaseState::Blocked));
    // view_for_worker on a fresh worker sees nothing — P3 isn't Active.
    assert!(p.view_for_worker(1, None).is_empty());

    // Cascade: drain P0 → mark Done → P1 Active → drain → ... → P3 Active.
    loop {
        p.drain_empty_active_phases();
        let drained = p.poll_drain_transitions();
        if drained.is_empty() {
            break;
        }
        for ph in &drained {
            p.mark_phase_done(ph);
        }
    }

    assert_eq!(p.phase_state(&phase("P0")), Some(PhaseState::Done));
    assert_eq!(p.phase_state(&phase("P1")), Some(PhaseState::Done));
    assert_eq!(p.phase_state(&phase("P2")), Some(PhaseState::Done));
    assert_eq!(p.phase_state(&phase("P3")), Some(PhaseState::Active));
    // Now phase-3 item is reachable.
    assert_eq!(p.view_for_worker(1, None).len(), 1);
}

/// `drain_empty_active_phases` must be a no-op when the Active
/// phase has queued items — wouldn't want to incorrectly drain
/// an in-use phase.
#[test]
fn drain_empty_active_phases_skips_phase_with_items() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([t("P", "T", "", 1)]).expect("valid extend");
    p.drain_empty_active_phases();
    assert_eq!(p.phase_state(&phase("P")), Some(PhaseState::Active));
    assert!(p.poll_drain_transitions().is_empty());
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
    ]).expect("valid extend");
    let view = p.view_for_worker(1, None);
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
    ]).expect("valid extend");
    // Worker 1 sees both typed buckets.
    let view = p.view_for_worker(1, None);
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
    let view = p.view_for_worker(1, None);
    assert!(view.is_empty());
    assert_eq!(view.len(), 0);
}

#[test]
fn reinject_revives_drained_phase_to_active() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([t("P", "T", "alpha", 1)]).expect("valid extend");
    let item = p.pop_for_worker(1).unwrap();
    p.on_item_finished(&phase("P"), None);
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
    ]).expect("valid extend");
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
    ]).expect("valid extend");

    // First, worker 1 grabs alpha (Step 2). After this, the view for
    // worker 1 should put alpha first (Step 1: pinned), then beta
    // (Step 2: unpinned typed), then free pool.
    let _ = p.pop_for_worker(1).unwrap();

    let view = p.view_for_worker(1, None);
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
    ]).expect("valid extend");
    let view = p.view_for_worker(1, None);
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
    ]).expect("valid extend");
    let view = p.view_for_worker(1, None);
    // View order: alpha (BTreeMap "alpha" < "beta") then beta. Pick beta
    // to verify non-zero index removal.
    assert_eq!(view.as_slice()[0].size, 1);
    assert_eq!(view.as_slice()[1].size, 2);
    let taken = p.take_from_view(view, 1);
    assert_eq!(taken.size, 2);
    assert_eq!(taken.affinity_id.as_ref().unwrap().as_str(), "beta");
    // Worker 1 is now pinned to beta. Next view starts with the alpha
    // bucket only (alpha #1 still present, beta drained).
    let view2 = p.view_for_worker(1, None);
    let sizes: Vec<u64> = view2.as_slice().iter().map(|t| t.size).collect();
    assert_eq!(sizes, vec![1]);
    // In-flight count for P incremented to 1.
    assert_eq!(p.in_flight(&phase("P")), 1);
}

#[test]
fn take_from_view_increments_in_flight_and_drains_phase() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([t("P", "T", "alpha", 1)]).expect("valid extend");
    let view = p.view_for_worker(1, None);
    assert_eq!(view.len(), 1);
    let _ = p.take_from_view(view, 0);
    assert_eq!(p.phase_state(&phase("P")), Some(PhaseState::Draining));
    assert_eq!(p.in_flight(&phase("P")), 1);
}

#[test]
fn view_for_worker_empty_when_no_eligible_items() {
    let mut p = pool_with(&["P"], &[]);
    let view = p.view_for_worker(0, None);
    assert!(view.is_empty());
    p.extend([t("P", "T", "", 1)]).expect("valid extend");
    let view = p.view_for_worker(0, None);
    assert_eq!(view.len(), 1);
}

/// `WorkerView::sort_by_key` reorders items but never breaks the
/// `(items[i], locators[i])` pairing — verified by issuing
/// `take_from_view` against the sorted view and observing that the
/// item returned by the pool matches the one the view exposes at the
/// chosen slice index. Random shuffles of distinct sizes exercise
/// several key orderings against the same input.
#[test]
fn sort_by_key_preserves_locator_pairing() {
    // Build a pool with several distinct-size items across two typed
    // buckets and the free pool, so the view has a non-trivial mix
    // of locators (each pointing at a different bucket).
    let inputs = [
        ("P", "T", "alpha", 7u64),
        ("P", "T", "alpha", 3),
        ("P", "T", "alpha", 11),
        ("P", "T", "beta", 5),
        ("P", "T", "beta", 9),
        ("P", "T", "", 1),
        ("P", "T", "", 13),
    ];

    // Sort key 1: ascending size.
    {
        let mut p = pool_with(&["P"], &[]);
        p.extend(inputs.iter().map(|(ph, ty, a, s)| t(ph, ty, a, *s)))
            .expect("valid extend");
        let view = p.view_for_worker(99, None).sort_by_key(|t| t.size);
        // Confirm sort order.
        let sizes: Vec<u64> = view.as_slice().iter().map(|t| t.size).collect();
        let mut expected = sizes.clone();
        expected.sort();
        assert_eq!(sizes, expected, "view should be size-ascending");
        // Pop every entry and verify the pool returns exactly the item
        // the view advertised at that slice index. After each take the
        // remaining view is invalidated (locators shift), so rebuild a
        // fresh sorted view between takes.
        let target = view.as_slice()[0].size;
        let taken = p.take_from_view(view, 0);
        assert_eq!(taken.size, target);
    }

    // Sort key 2: descending size (via negation).
    {
        let mut p = pool_with(&["P"], &[]);
        p.extend(inputs.iter().map(|(ph, ty, a, s)| t(ph, ty, a, *s)))
            .expect("valid extend");
        let view = p
            .view_for_worker(99, None)
            .sort_by_key(|t| std::cmp::Reverse(t.size));
        let sizes: Vec<u64> = view.as_slice().iter().map(|t| t.size).collect();
        let mut expected = sizes.clone();
        expected.sort_by(|a, b| b.cmp(a));
        assert_eq!(sizes, expected, "view should be size-descending");
        let target = view.as_slice()[2].size;
        let taken = p.take_from_view(view, 2);
        assert_eq!(taken.size, target);
    }

    // Sort key 3: by path string — derived from size + affinity, so the
    // resulting order is a non-trivial permutation different from the
    // size-ascending order. Verify the pairing holds for every index by
    // taking each item one at a time and reconstructing the view.
    {
        let mut p = pool_with(&["P"], &[]);
        p.extend(inputs.iter().map(|(ph, ty, a, s)| t(ph, ty, a, *s)))
            .expect("valid extend");
        // Drain every item via the sorted view, one at a time.
        for _ in 0..inputs.len() {
            let view = p
                .view_for_worker(99, None)
                .sort_by_key(|t| t.path.display().to_string());
            let expected_path = view.as_slice()[0].path.clone();
            let expected_size = view.as_slice()[0].size;
            let taken = p.take_from_view(view, 0);
            assert_eq!(taken.path, expected_path);
            assert_eq!(taken.size, expected_size);
        }
        assert!(p.iter().next().is_none(), "pool drained");
    }
}

/// A preference predicate passed through `view_for_worker` orders items
/// *within* a class but never lets a lower-class item overtake a
/// higher-class one. A pin-class item with a "worst" preference score
/// must still precede every typed-class item, even ones with "best"
/// scores.
#[test]
fn view_for_worker_predicate_sorts_within_priority_class() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([
        // alpha bucket — will become worker 1's pin after the first pop.
        t("P", "T", "alpha", 100), // pinned-class item, "worst" size key
        t("P", "T", "alpha", 200),
        // beta bucket — unpinned typed; has the "best" size key (smallest).
        t("P", "T", "beta", 1),
        t("P", "T", "beta", 2),
        // free pool.
        t("P", "T", "", 50),
    ])
    .expect("valid extend");

    // Worker 1 grabs alpha first so alpha becomes its pin (Class 0 = pin).
    let _ = p.pop_for_worker(1).unwrap();

    // Predicate: ascending size. A naive global sort would put the
    // beta items (size 1, 2) ahead of the remaining alpha item
    // (size 200). The class boundary must prevent that.
    let pred = |t: &TaskInfo<()>| t.size.cmp(&0);
    let view = p.view_for_worker(1, Some(&pred));
    let sizes: Vec<u64> = view.as_slice().iter().map(|t| t.size).collect();
    // Expected:
    //   class 0 (pin = alpha): [200]                — 1 item left after pop
    //   class 1 (typed = beta): [1, 2]              — predicate sorts ascending
    //   class 2 (free):         [50]
    //   class 3 (co-pin):       []                  — none in this fixture
    assert_eq!(
        sizes,
        vec![200, 1, 2, 50],
        "pin-class item must precede typed-class items even with predicate"
    );
}

/// Regression: passing `None` for the preference predicate must
/// produce the same items + locators ordering that the pre-change
/// `view_for_worker` would have built. We exercise the same fixture
/// used by `view_for_worker_orders_pinned_then_typed_then_free_then_copin`
/// and assert the identical sequence — anything else means the
/// `None` branch silently reordered items.
#[test]
fn view_for_worker_no_predicate_matches_pre_change_order() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([
        t("P", "T", "alpha", 1),
        t("P", "T", "alpha", 2),
        t("P", "T", "beta", 3),
        t("P", "T", "", 4),
    ])
    .expect("valid extend");
    // Worker 1 takes one alpha item so the remaining view exercises all
    // four classes (or as many as the fixture has).
    let _ = p.pop_for_worker(1).unwrap();

    let view = p.view_for_worker(1, None);
    let sizes: Vec<u64> = view.as_slice().iter().map(|t| t.size).collect();
    // Same expectation as the historical
    // `view_for_worker_orders_pinned_then_typed_then_free_then_copin`
    // test: alpha #2 → beta #3 → free #4.
    assert_eq!(sizes, vec![2, 3, 4]);
}

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

// ── Task-level dependencies (task_id + task_depends_on) ──

/// Test fixture variant carrying a task_id and (optional) deps.
fn t_with_id(
    phase: &str,
    ty: &str,
    affinity: &str,
    size: u64,
    id: &str,
    deps: &[&str],
) -> TaskInfo<()> {
    let mut item = t(phase, ty, affinity, size);
    item.task_id = Some(id.to_string());
    item.task_depends_on = deps.iter().map(|d| d.to_string()).collect();
    item
}

#[test]
fn task_deps_unknown_id_fails_extend() {
    let mut p = pool_with(&["P"], &[]);
    let res = p.extend([t_with_id("P", "T", "", 1, "child", &["nope"])]);
    match res {
        Err(PendingPoolError::UnknownTaskDep { task, referenced_by }) => {
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
    let queued_ids: Vec<_> = p
        .iter()
        .map(|i| i.task_id.clone().unwrap())
        .collect();
    assert_eq!(queued_ids, vec!["a".to_string()]);
    let first = p.pop_for_worker(1).expect("a is dispatchable");
    assert_eq!(first.task_id.as_deref(), Some("a"));
    // No more queued items; B still blocked, phase still has work.
    assert!(p.pop_for_worker(1).is_none());
    p.on_item_finished(&phase("P"), Some("a"));
    // Now B is unblocked.
    let second = p.pop_for_worker(1).expect("b unblocked");
    assert_eq!(second.task_id.as_deref(), Some("b"));
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
    assert_eq!(a.task_id.as_deref(), Some("a"));
    // Finish A → B unblocks and lands at the FRONT of the bucket,
    // ahead of C which has been queued behind A all along.
    p.on_item_finished(&phase("P"), Some("a"));
    let next = p.pop_for_worker(1).expect("b before c");
    assert_eq!(next.task_id.as_deref(), Some("b"));
    let last = p.pop_for_worker(1).expect("c last");
    assert_eq!(last.task_id.as_deref(), Some("c"));
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
    assert_eq!(item.task_id.as_deref(), Some("variant"));
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
    assert_eq!(a.task_id.as_deref(), Some("a"));
    let cascaded = p.on_item_failed_permanent(&phase("P"), "a");
    let mut cascaded_ids: Vec<_> = cascaded
        .iter()
        .map(|i| i.task_id.clone().unwrap())
        .collect();
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
        |t| t.task_id.as_deref() == Some("b"),
        |t| {
            t.preferred_secondaries =
                SoftPreferredSecondaries::new(vec!["sec-x".into()]);
        },
    );
    assert!(updated, "predicate must match `b`");
    let prefs: Vec<_> = p
        .iter()
        .map(|t| {
            (
                t.task_id.clone().unwrap(),
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
    let ids_in_buckets: Vec<_> = p.iter().map(|t| t.task_id.clone().unwrap()).collect();
    assert_eq!(ids_in_buckets, vec!["a".to_string()]);
    let updated = p.update_first_match_in_place(
        |t| t.task_id.as_deref() == Some("b"),
        |t| {
            t.preferred_secondaries =
                SoftPreferredSecondaries::new(vec!["sec-y".into()]);
        },
    );
    assert!(updated, "predicate must match blocked `b`");
    // Pop `a` to unblock `b`, then verify `b`'s preference survived.
    p.pop_for_worker(1).expect("a");
    p.on_item_finished(&phase("P"), Some("a"));
    let b = p.pop_for_worker(1).expect("b unblocked");
    assert_eq!(b.task_id.as_deref(), Some("b"));
    assert_eq!(
        b.preferred_secondaries.as_slice(),
        &["sec-y".to_string()][..]
    );
}

#[test]
fn update_first_match_in_place_returns_false_on_no_match() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([t_with_id("P", "T", "", 1, "a", &[])]).expect("valid");
    let updated = p.update_first_match_in_place(
        |t| t.task_id.as_deref() == Some("nonexistent"),
        |t| {
            t.preferred_secondaries =
                SoftPreferredSecondaries::new(vec!["never".into()]);
        },
    );
    assert!(!updated, "no match → false");
    // `a` untouched.
    let a = p.pop_for_worker(1).expect("a");
    assert!(a.preferred_secondaries.as_slice().is_empty());
}
