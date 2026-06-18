//! `view_for_worker` / `select`+`take_selected` / `WorkerView::sort_by_key`
//! tests: priority-class ordering (pin → typed → free → co-pin),
//! correct take-by-locator-index, blocked-phase skipping, and the
//! preference-predicate stable sort within each priority class.

use dynrunner_core::TaskInfo;

use super::{PhaseState, phase, pool_with, t};

/// `view_for_worker` produces the same priority order as `pop_for_worker`
/// for a fresh worker (no affinity, no pins) — typed buckets first,
/// free-pool last. The scheduler's chosen index commits via
/// `select` + `take_selected`.
#[test]
fn view_for_worker_orders_typed_then_free_pool() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([
        t("P", "T", "", 9),       // free-pool item
        t("P", "T", "alpha", 10), // typed
    ])
    .expect("valid extend");
    let view = p.view_for_worker(1, None);
    assert_eq!(view.len(), 2);
    // First entry is from the typed bucket (step 2 wins over step 3).
    assert_eq!(
        view.as_slice()[0].affinity_id.as_ref().unwrap().as_str(),
        "alpha"
    );
    // Second is the free-pool item.
    assert!(view.as_slice()[1].affinity_id.is_none());
}

/// `take_selected` commits the scheduler's chosen index — soft-pin,
/// in-flight, and drain bookkeeping fire just like `pop_for_worker`.
#[test]
fn take_selected_commits_chosen_index() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([
        t("P", "T", "alpha", 1),
        t("P", "T", "alpha", 2),
        t("P", "T", "beta", 3),
    ])
    .expect("valid extend");
    // Worker 1 sees both typed buckets.
    let view = p.view_for_worker(1, None);
    // Find the beta entry (BTreeMap key order: alpha < beta).
    let beta_idx = view
        .as_slice()
        .iter()
        .position(|t| t.affinity_id.as_ref().unwrap().as_str() == "beta")
        .expect("beta visible");
    let selection = view.select(beta_idx);
    let item = p.take_selected(selection).expect("ready item dispatches (D.1)");
    assert_eq!(item.affinity_id.as_ref().unwrap().as_str(), "beta");
    // Worker 1 is now pinned to beta; subsequent pop stays in beta until
    // it drains.
    assert_eq!(p.in_flight(&phase("P")), 1);
}

/// L4 Arc-sharing: the pool holds each queued item as an `Arc<TaskInfo>`,
/// and a dispatch (`take_selected`) hands back the SAME allocation it held
/// in the bucket — not a deep clone. This is the seam the distributed
/// primary relies on to flow ONE Arc pool → in-flight ledger → worker slot
/// (the 2×16GB doubling fix): if take deep-cloned here, every dispatch
/// would re-pay the full `TaskInfo`.
#[test]
fn take_selected_returns_the_same_arc_the_pool_held() {
    use std::sync::Arc;
    let mut p = pool_with(&["P"], &[]);
    p.extend([t("P", "T", "alpha", 1)]).expect("valid extend");
    // The pool wrapped the item in an Arc at the ingest boundary; capture
    // its allocation identity straight from the bucket.
    let key = (
        phase("P"),
        dynrunner_core::TypeId::from("T"),
        dynrunner_core::AffinityId::from("alpha"),
    );
    let bucket_arc: Arc<TaskInfo<_>> = p.buckets[&key].items[0].clone();
    let view = p.view_for_worker(1, None);
    let selection = view.select(0);
    let taken = p.take_selected(selection).expect("ready item dispatches (D.1)");
    assert!(
        Arc::ptr_eq(&bucket_arc, &taken),
        "take_selected must return the SAME Arc allocation the bucket held \
         (shared, not deep-cloned)"
    );
}

/// The same Arc-identity invariant for the single-shot `pop_for_worker`
/// dispatch entry point.
#[test]
fn pop_for_worker_returns_the_same_arc_the_pool_held() {
    use std::sync::Arc;
    let mut p = pool_with(&["P"], &[]);
    p.extend([t("P", "T", "", 1)]).expect("valid extend");
    let key = (
        phase("P"),
        dynrunner_core::TypeId::from("T"),
        dynrunner_core::AffinityId::from(""),
    );
    let bucket_arc: Arc<TaskInfo<_>> = p.buckets[&key].items[0].clone();
    let popped = p.pop_for_worker(1).expect("dispatchable");
    assert!(
        Arc::ptr_eq(&bucket_arc, &popped),
        "pop_for_worker must return the SAME Arc allocation the bucket held"
    );
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
    ])
    .expect("valid extend");

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
    p.extend([t("A", "T", "", 1), t("B", "T", "", 2)])
        .expect("valid extend");
    let view = p.view_for_worker(1, None);
    // Only A's item is visible; B is Blocked.
    assert_eq!(view.len(), 1);
    assert_eq!(view.as_slice()[0].size, 1);
}

#[test]
fn take_selected_removes_chosen_item_and_records_affinity() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([t("P", "T", "alpha", 1), t("P", "T", "beta", 2)])
        .expect("valid extend");
    let view = p.view_for_worker(1, None);
    // View order: alpha (BTreeMap "alpha" < "beta") then beta. Pick beta
    // to verify non-zero index removal.
    assert_eq!(view.as_slice()[0].size, 1);
    assert_eq!(view.as_slice()[1].size, 2);
    let selection = view.select(1);
    let taken = p.take_selected(selection).expect("ready item dispatches (D.1)");
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
fn take_selected_increments_in_flight_and_drains_phase() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([t("P", "T", "alpha", 1)]).expect("valid extend");
    let view = p.view_for_worker(1, None);
    assert_eq!(view.len(), 1);
    let selection = view.select(0);
    let _ = p.take_selected(selection);
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
/// `take_selected` against the sorted view and observing that the
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
        let selection = view.select(0);
        let taken = p.take_selected(selection).expect("ready item dispatches (D.1)");
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
        let selection = view.select(2);
        let taken = p.take_selected(selection).expect("ready item dispatches (D.1)");
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
            let selection = view.select(0);
            let taken = p.take_selected(selection).expect("ready item dispatches (D.1)");
            assert_eq!(taken.path, expected_path);
            assert_eq!(taken.size, expected_size);
        }
        assert!(p.iter().next().is_none(), "pool drained");
    }
}

/// Differential pin for the dispatch RECHECK shape: several workers in
/// sequence each build a view and take its first item, with the takes
/// interleaved between the view constructions (exactly what
/// `dispatch_to_idle_workers` does per idle worker). The taken-item
/// sequence pins the order-evolution semantics — each worker's view
/// must observe every pin/affinity mutation the previous workers'
/// takes performed:
///   * worker 1 takes alpha#1 and PINS alpha — alpha leaves the typed
///     class for everyone else;
///   * worker 2 therefore leads with beta (the only unpinned typed
///     bucket), takes beta#1, pins beta;
///   * worker 3 sees no unpinned typed bucket and leads with the free
///     pool;
///   * worker 1's second view leads with its pin-class alpha remainder.
///
/// Any view implementation that snapshots classification at recheck
/// start (instead of per worker) breaks this sequence.
#[test]
fn sequential_views_with_takes_observe_pin_evolution() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([
        t("P", "T", "alpha", 10),
        t("P", "T", "alpha", 20),
        t("P", "T", "beta", 30),
        t("P", "T", "beta", 40),
        t("P", "T", "", 50),
        t("P", "T", "", 60),
    ])
    .expect("valid extend");

    let mut taken = Vec::new();
    for worker in [1, 2, 3, 1] {
        let view = p.view_for_worker(worker, None);
        assert!(!view.is_empty(), "worker {worker} must see candidates");
        let selection = view.select(0);
        taken.push(p.take_selected(selection).expect("ready item dispatches (D.1)").size);
    }
    assert_eq!(
        taken,
        vec![10, 30, 50, 20],
        "the per-worker first-fit walk must observe each prior take's \
         pin/affinity mutation"
    );
    // Remaining: alpha drained, beta#2 + free#2 still queued.
    let mut left: Vec<u64> = p.iter().map(|t| t.size).collect();
    left.sort();
    assert_eq!(left, vec![40, 60]);
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

/// Perf-shape pin: building a `WorkerView` (and running its
/// caller-side combinators) clones NO candidate `TaskInfo` — the view
/// borrows items straight from the pool. Counted through the
/// identifier's `Clone` impl, which every `TaskInfo` clone must run.
/// The dispatch recheck builds one view per idle worker per pass, so
/// a per-candidate clone here multiplies into
/// O(idle workers × pool size) per recheck — the regression this pins
/// out.
#[test]
fn view_construction_clones_no_candidates() {
    use std::sync::atomic::{AtomicU64, Ordering};
    static ID_CLONES: AtomicU64 = AtomicU64::new(0);

    #[derive(Debug, Hash, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    struct CountingId(u64);
    impl Clone for CountingId {
        fn clone(&self) -> Self {
            ID_CLONES.fetch_add(1, Ordering::Relaxed);
            CountingId(self.0)
        }
    }

    let counted = |n: u64, affinity: &str| {
        let base = t("P", "T", affinity, n);
        TaskInfo {
            path: base.path,
            size: base.size,
            identifier: CountingId(n),
            phase_id: base.phase_id,
            type_id: base.type_id,
            affinity_id: base.affinity_id,
            payload: base.payload,
            task_id: base.task_id,
            task_depends_on: base.task_depends_on,
            preferred_secondaries: base.preferred_secondaries,
            preferred_version: base.preferred_version,
            kind: base.kind,
            setup_affinity: None,
            upload_file: None,
            required_files: None,
            resolved_path: base.resolved_path,
        }
    };

    let mut p = super::PendingPool::<CountingId>::new(
        vec![phase("P")],
        std::collections::HashMap::new(),
    )
    .expect("valid graph");
    p.extend([
        counted(1, "alpha"),
        counted(2, "alpha"),
        counted(3, ""),
    ])
    .expect("valid extend");

    let before = ID_CLONES.load(Ordering::Relaxed);
    let view = p
        .view_for_worker(1, None)
        .sort_by_key(|t| t.size)
        .filter(|_| true);
    assert_eq!(view.len(), 3, "all candidates visible");
    let selection = view.select(0);
    let after = ID_CLONES.load(Ordering::Relaxed);
    assert_eq!(
        after - before,
        0,
        "view construction + combinators + select must not clone any candidate"
    );
    // The take hands out the ORIGINAL pool item — still no clone.
    let _ = p.take_selected(selection);
    assert_eq!(
        ID_CLONES.load(Ordering::Relaxed) - before,
        0,
        "take_selected moves the item out of the bucket without cloning"
    );
}

/// #652 D.1 — idempotent pop-time re-check: a NOT-READY item pushed to a
/// bucket head (the shape the 5-min reconcile arm, concern C, produces via
/// [`PendingPool::push_to_queue_head`]) is RE-BLOCKED on pop, never dispatched.
///
/// This pins the invariant the reconcile arm relies on: reconcile can move a
/// possibly-not-ready dependent to the general-queue head WITHOUT first
/// re-deriving its readiness, because the consume-point gate restores the
/// "bucketed ⇒ ready" invariant — a not-ready item is evicted + re-routed
/// through `commit_item` (so it re-registers as `blocked`), and the worker gets
/// no dispatch this slot. Covers BOTH consume entry points (`pop_for_worker`
/// and the view-path `take_selected`).
#[test]
fn d1_not_ready_pushed_to_head_is_reblocked_on_pop_not_dispatched() {
    use dynrunner_core::TaskDep;
    use std::sync::Arc;

    // A task whose prereq "dep" has NOT completed → not ready.
    let mut blocked_item = t("P", "T", "alpha", 5);
    blocked_item.task_id = "blocked".into();
    blocked_item.task_depends_on = vec![TaskDep {
        task_id: "dep".into(),
        phase_id: phase("P"),
        inherit_outputs: false,
        def_id: None,
    }];

    // pop_for_worker entry point.
    {
        let mut p = pool_with(&["P"], &[]);
        // Reconcile shape: move the not-ready item straight to its bucket head.
        p.push_to_queue_head(Arc::new(blocked_item.clone()));
        // The pop-time gate re-derives readiness, finds the unmet "dep", evicts
        // + re-blocks the item, and returns nothing for this worker.
        assert!(
            p.pop_for_worker(1).is_none(),
            "a not-ready reconcile-pushed item must NOT be dispatched by pop_for_worker"
        );
        // It is now BLOCKED (re-routed through commit_item), not in any bucket.
        assert!(
            p.blocked.contains_key("blocked"),
            "the not-ready item must be re-registered as blocked"
        );
        assert_eq!(p.in_flight(&phase("P")), 0, "no in-flight bump for a re-blocked item");
        // Resolving the dep unblocks it into a bucket → now dispatchable.
        p.on_item_finished(&phase("P"), Some("dep"));
        assert!(
            p.pop_for_worker(1).is_some(),
            "once its dep completes, the formerly-blocked item dispatches"
        );
    }

    // view_for_worker + take_selected entry point.
    {
        let mut p = pool_with(&["P"], &[]);
        p.push_to_queue_head(Arc::new(blocked_item.clone()));
        // The view is built from the bucket, so it offers the not-ready item;
        // take_selected re-checks at the consume point and re-blocks it.
        let view = p.view_for_worker(1, None);
        assert_eq!(view.len(), 1, "the bucketed item is visible to the view");
        let selection = view.select(0);
        assert!(
            p.take_selected(selection).is_none(),
            "a not-ready reconcile-pushed item must NOT be dispatched by take_selected"
        );
        assert!(
            p.blocked.contains_key("blocked"),
            "take_selected must re-block the not-ready item"
        );
    }
}
