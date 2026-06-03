//! Tests for the bucketing of `extend` items and the soft-pin
//! algorithm in `pop_for_worker` (affinity claim → typed buckets →
//! free pool fairness).

use super::{pool_with, t};

#[test]
fn extend_distributes_items_into_buckets() {
    let mut p = pool_with(&["P"], &[]);
    p.extend([
        t("P", "T1", "alpha", 10),
        t("P", "T1", "alpha", 20),
        t("P", "T1", "beta", 30),
        t("P", "T2", "", 40),
    ])
    .expect("valid extend");
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
    ])
    .expect("valid extend");
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
    ])
    .expect("valid extend");
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
    ])
    .expect("valid extend");
    // Step 2 prefers typed over free pool — worker 1 gets alpha first.
    let first = p.pop_for_worker(1).unwrap();
    assert_eq!(first.affinity_id.as_ref().unwrap().as_str(), "alpha");
    // Worker 2 then gets the free-pool item (no other typed bucket left).
    let second = p.pop_for_worker(2).unwrap();
    assert!(second.affinity_id.is_none());
}
