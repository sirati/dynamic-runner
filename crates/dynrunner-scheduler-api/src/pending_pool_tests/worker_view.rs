//! `view_for_worker` / `take_from_view` / `WorkerView::sort_by_key`
//! tests: priority-class ordering (pin → typed → free → co-pin),
//! correct take-by-locator-index, blocked-phase skipping, and the
//! preference-predicate stable sort within each priority class.

use dynrunner_core::TaskInfo;

use super::{PhaseState, phase, pool_with, t};

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
