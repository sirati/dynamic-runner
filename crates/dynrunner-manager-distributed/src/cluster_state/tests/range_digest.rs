//! Tests for the P1 range-digest projection (`tasks_range_digest`) and its
//! two correctness invariants.
//!
//! Covers:
//! - `XOR(range-folds) == StateDigest::tasks_hash` and `sum(counts) ==
//!   tasks_count` — the reconstruction invariant the delta's correctness
//!   rests on (the headline `range_digest_folds_match_scalar` test), across
//!   the fat-only AND fat+settled ledger.
//! - A one-task change moves exactly ONE bucket — so a delta pull
//!   re-streams ~one bucket, not the whole ledger.
//! - `divergent_ranges` per-bucket detection: count-ahead, equal-count-
//!   divergent-fold (the `count==`/`hash!=` case WITHIN a bucket),
//!   no-divergence (quiesce), and the all-divergent extreme.

use super::*;
use dynrunner_protocol_primary_secondary::{RANGE_COUNT, StateDigest};

/// Fold every bucket together — must reconstruct the scalar `tasks_hash`.
fn xor_all_folds(d: &dynrunner_protocol_primary_secondary::RangeDigest) -> u64 {
    d.folds.iter().fold(0u64, |acc, f| acc ^ f)
}

fn sum_all_counts(d: &dynrunner_protocol_primary_secondary::RangeDigest) -> u64 {
    d.counts.iter().map(|&c| c as u64).sum()
}

/// THE HEADLINE INVARIANT: the cross-bucket fold reconstructs the scalar
/// `StateDigest::tasks_hash` EXACTLY, and the cross-bucket count sum equals
/// `tasks_count`. By construction (every entry's term lands in exactly one
/// bucket; XOR is associative + commutative), so the range digest is a
/// faithful refinement of the scalar fold the `is_behind` detector already
/// trusts — a wrong split here would silently lose CRDT entries on a delta
/// pull. Pinned across many task states (Pending / Completed / Failed) so
/// the per-entry TERM (not just the key) is exercised.
#[test]
fn range_digest_folds_match_scalar() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    for i in 0..500 {
        s.apply(ClusterMutation::TaskAdded {
            hash: format!("task-{i:04}"),
            task: mk_task(&format!("task-{i:04}")),
        });
    }
    // Advance a spread of tasks to terminal states so the per-entry fold
    // term varies (not just the key): completions + failures.
    for i in (0..500).step_by(3) {
        s.apply(ClusterMutation::TaskCompleted {
            attempt: 0,
            hash: format!("task-{i:04}"),
            result_data: None,
        });
    }
    for i in (1..500).step_by(7) {
        s.apply(ClusterMutation::TaskFailed {
            attempt: 0,
            hash: format!("task-{i:04}"),
            kind: ErrorType::NonRecoverable,
            error: "boom".into(),
            version: Default::default(),
        });
    }

    let scalar: StateDigest = s.digest();
    let ranges = s.tasks_range_digest();
    assert_eq!(
        xor_all_folds(&ranges),
        scalar.tasks_hash,
        "XOR(range-folds) must reconstruct the scalar tasks_hash EXACTLY \
         (the delta-correctness anchor)"
    );
    assert_eq!(
        sum_all_counts(&ranges),
        scalar.tasks_count,
        "sum(range-counts) must equal the scalar tasks_count"
    );
}

/// The invariant holds across the fat/settled split: after spilling a slice
/// of the ledger to settled, the range fold (fat via the live term + settled
/// via the persisted `digest_contribution`) STILL reconstructs the scalar
/// `tasks_hash`. This is the half a naive "iterate `tasks`" implementation
/// would miss — the settled entries' terms must land in the right buckets.
#[test]
fn range_digest_folds_match_scalar_across_settled_split() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut s = ClusterState::<RunnerIdentifier>::new();
    // Build a ledger and terminalize every entry (Completed entries are
    // settle-eligible), then spill the slice to disk — moving each entry's
    // fold term out of the live loop and into the settled accumulator.
    for i in 0..200 {
        s.apply(ClusterMutation::TaskAdded {
            hash: format!("k-{i:04}"),
            task: mk_task(&format!("k-{i:04}")),
        });
        s.apply(ClusterMutation::TaskCompleted {
            attempt: 0,
            hash: format!("k-{i:04}"),
            result_data: None,
        });
    }
    let evicted = s.test_spill_all(&dir.path().join("spill.cbor"));
    assert!(
        evicted > 0,
        "the fixture must actually spill some entries for this test to mean anything"
    );
    assert_eq!(s.tasks_in_memory(), s.task_count() - evicted);

    let scalar = s.digest();
    let ranges = s.tasks_range_digest();
    assert_eq!(
        xor_all_folds(&ranges),
        scalar.tasks_hash,
        "XOR(range-folds) must reconstruct tasks_hash even with a fat/settled split"
    );
    assert_eq!(sum_all_counts(&ranges), scalar.tasks_count);
}

/// A one-task change moves exactly ONE bucket: the bucket of the changed
/// key. Every other bucket's fold + count is byte-identical, so a delta pull
/// re-streams ~one bucket, not the ledger. This is the whole point of P1.
#[test]
fn one_task_change_isolates_to_one_range() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    for i in 0..300 {
        s.apply(ClusterMutation::TaskAdded {
            hash: format!("t-{i:04}"),
            task: mk_task(&format!("t-{i:04}")),
        });
    }
    let before = s.tasks_range_digest();
    // Advance ONE task to Completed.
    s.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
        hash: "t-0123".into(),
        result_data: None,
    });
    let after = s.tasks_range_digest();

    // Exactly one bucket differs, and it is the changed key's bucket.
    let changed: Vec<usize> = (0..RANGE_COUNT)
        .filter(|&r| before.folds[r] != after.folds[r] || before.counts[r] != after.counts[r])
        .collect();
    assert_eq!(
        changed.len(),
        1,
        "a one-task change must move exactly ONE bucket, moved: {changed:?}"
    );
    // The divergent-range comparison (the production path) flags exactly
    // that one bucket — the streamed key-set ⊆ that range.
    let divergent = before.divergent_ranges(&after);
    assert_eq!(
        divergent,
        vec![changed[0] as u16],
        "divergent_ranges must flag exactly the changed bucket"
    );
}

/// No divergence (two converged replicas) → an empty divergent set: a
/// quiesced node pulls nothing. The per-bucket image of the digest's
/// self-quiescing property.
#[test]
fn converged_replicas_have_no_divergent_ranges() {
    let mut a = ClusterState::<RunnerIdentifier>::new();
    for i in 0..100 {
        a.apply(ClusterMutation::TaskAdded {
            hash: format!("c-{i:03}"),
            task: mk_task(&format!("c-{i:03}")),
        });
    }
    let mut b = ClusterState::<RunnerIdentifier>::new();
    b.restore(a.snapshot());

    let ra = a.tasks_range_digest();
    let rb = b.tasks_range_digest();
    assert_eq!(ra.counts, rb.counts);
    assert_eq!(ra.folds, rb.folds);
    assert!(ra.divergent_ranges(&rb).is_empty());
    assert!(rb.divergent_ranges(&ra).is_empty());
}

/// Equal-count-different-content WITHIN a bucket (the `field_behind`
/// count==/hash!= case at the bucket grain): a replica that advanced a task
/// the peer has not (same count, divergent fold in that bucket) is detected
/// per-range and the heal pulls exactly that bucket. This is the edge the
/// task brief calls out explicitly.
#[test]
fn equal_count_divergent_content_in_bucket_is_detected() {
    let mut stale = ClusterState::<RunnerIdentifier>::new();
    for i in 0..50 {
        stale.apply(ClusterMutation::TaskAdded {
            hash: format!("e-{i:03}"),
            task: mk_task(&format!("e-{i:03}")),
        });
    }
    // The advanced replica is the stale one + one Completed (SAME key set,
    // SAME count everywhere, one bucket's fold differs).
    let mut advanced = ClusterState::<RunnerIdentifier>::new();
    advanced.restore(stale.snapshot());
    advanced.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
        hash: "e-021".into(),
        result_data: None,
    });

    let r_stale = stale.tasks_range_digest();
    let r_adv = advanced.tasks_range_digest();
    // The total task COUNT is unchanged — so the scalar count compare alone
    // could not see the divergence; the per-bucket FOLD does.
    assert_eq!(sum_all_counts(&r_stale), sum_all_counts(&r_adv));
    let divergent = r_stale.divergent_ranges(&r_adv);
    assert_eq!(divergent.len(), 1, "exactly the advanced key's bucket diverges");
    // And the bucket is `e-021`'s bucket.
    let expected = (crate::cluster_state::range_index_for_test("e-021")) as u16;
    assert_eq!(divergent, vec![expected]);
}

/// The all-divergent extreme: a fresh (empty) local vs a fully-populated
/// peer flags every NON-empty peer bucket — the delta degrades gracefully to
/// "pull everything the peer has", and the streamed union is the whole peer
/// ledger (no entry is skipped). Bounds the other end from the quiesce case.
#[test]
fn empty_local_is_behind_every_populated_peer_bucket() {
    let empty = ClusterState::<RunnerIdentifier>::new();
    let mut peer = ClusterState::<RunnerIdentifier>::new();
    for i in 0..400 {
        peer.apply(ClusterMutation::TaskAdded {
            hash: format!("a-{i:04}"),
            task: mk_task(&format!("a-{i:04}")),
        });
    }
    let r_empty = empty.tasks_range_digest();
    let r_peer = peer.tasks_range_digest();

    let divergent = r_empty.divergent_ranges(&r_peer);
    // Every bucket the peer has entries in is divergent; empty buckets are
    // not (the peer holds nothing there to pull).
    let peer_nonempty: Vec<u16> = (0..RANGE_COUNT)
        .filter(|&r| r_peer.counts[r] > 0)
        .map(|r| r as u16)
        .collect();
    assert_eq!(divergent, peer_nonempty);
    // The summed counts over the divergent buckets equal the whole peer
    // ledger — no peer entry is left unpullable.
    let pulled: u64 = divergent
        .iter()
        .map(|&r| r_peer.counts[r as usize] as u64)
        .sum();
    assert_eq!(pulled, peer.task_count() as u64);
}
