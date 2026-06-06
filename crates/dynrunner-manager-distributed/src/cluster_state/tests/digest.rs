//! Tests for the anti-entropy `digest()` projection.
//!
//! Covers: a converged replica produces an equal digest; the digest is
//! order-independent (insertion order does not change the fold); the
//! `is_behind` detector fires when a replica is missing data and goes
//! quiescent once a snapshot/restore converges the two — the
//! detect → pull → converge → quiesce cycle the cadence relies on.

use super::*;

/// Two replicas that converged to the same state (one built directly, one
/// built then snapshot/restored onto a fresh replica) produce IDENTICAL
/// digests. This is the self-quiescing precondition: a converged mesh
/// exchanges matching digests and pulls nothing.
#[test]
fn converged_replicas_produce_equal_digests() {
    let mut a = ClusterState::<RunnerIdentifier>::new();
    a.apply(ClusterMutation::TaskAdded {
        hash: "p".into(),
        task: mk_task("p"),
    });
    a.apply(ClusterMutation::TaskAdded {
        hash: "c".into(),
        task: mk_task("c"),
    });
    a.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
        hash: "c".into(),
        result_data: None,
    });
    a.apply(ClusterMutation::PrimaryChanged {
        new: "s1".into(),
        epoch: 4,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
    });

    let mut b = ClusterState::<RunnerIdentifier>::new();
    b.restore(a.snapshot());

    // The two replicas hold the same replicated ledger, so their digests
    // are byte-identical and neither is "behind" the other.
    assert_eq!(a.digest(), b.digest());
    assert!(!a.digest().is_behind(&b.digest()));
    assert!(!b.digest().is_behind(&a.digest()));
}

/// The task fold is order-independent: applying the same set of
/// `TaskAdded`s in a different order yields the same digest.
#[test]
fn digest_is_order_independent() {
    let mut a = ClusterState::<RunnerIdentifier>::new();
    for name in ["alpha", "beta", "gamma", "delta"] {
        a.apply(ClusterMutation::TaskAdded {
            hash: name.into(),
            task: mk_task(name),
        });
    }
    let mut b = ClusterState::<RunnerIdentifier>::new();
    for name in ["delta", "alpha", "gamma", "beta"] {
        b.apply(ClusterMutation::TaskAdded {
            hash: name.into(),
            task: mk_task(name),
        });
    }
    assert_eq!(a.digest(), b.digest());
}

/// A task advancing to a stronger state (same key, same count) changes
/// the fold — so a replica that has NOT seen the advance is detected as
/// behind even though the task COUNT is unchanged.
#[test]
fn same_count_state_advance_changes_fold() {
    let mut a = ClusterState::<RunnerIdentifier>::new();
    a.apply(ClusterMutation::TaskAdded {
        hash: "t".into(),
        task: mk_task("t"),
    });
    let stale = a.digest();
    a.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
        hash: "t".into(),
        result_data: None,
    });
    let advanced = a.digest();
    // Count unchanged (one task), but the fold differs because the entry
    // advanced Pending → Completed.
    assert_eq!(stale.tasks_count, advanced.tasks_count);
    assert_ne!(stale.tasks_hash, advanced.tasks_hash);
    // The replica still on `Pending` is behind the one that saw Completed.
    assert!(stale.is_behind(&advanced));
}

/// A divergence in the role-capability 2P-set IS detected by the
/// anti-entropy detector (C6 — reversed from the pre-C6 detector, which
/// excluded role sets). The capability lattice is a proper CRDT (merged
/// monotonically by `merge_capability` in `restore`), so folding it is
/// detect-WITH-heal: a flagged divergence is one a snapshot pull resolves,
/// NOT the R2 no-op loop the old projection-only design had to avoid.
#[test]
fn observer_capability_divergence_detected_and_heals() {
    // Two replicas with the SAME task ledger.
    let mut a = ClusterState::<RunnerIdentifier>::new();
    let mut b = ClusterState::<RunnerIdentifier>::new();
    for s in [&mut a, &mut b] {
        s.apply(ClusterMutation::TaskAdded {
            hash: "t".into(),
            task: mk_task("t"),
        });
    }
    // `a` knows an observer capability that `b` does not — a capability
    // 2P-set divergence with identical tasks.
    a.apply(ClusterMutation::PeerJoined {
        peer_id: "obs-x".into(),
        is_observer: true,
        can_be_primary: false,
        cap_version: Default::default(),
    });

    // The digests DIFFER (the capability fold is summarised) and `b` is
    // behind `a` — the missing capability drives a pull.
    assert_ne!(a.digest(), b.digest());
    assert!(b.digest().is_behind(&a.digest()));

    // Pull: the snapshot's capability 2P-set merges into `b` and converges
    // the digest (detect-WITH-heal — no permanent no-op loop).
    b.restore(a.snapshot());
    assert_eq!(a.digest(), b.digest());
    assert!(!b.digest().is_behind(&a.digest()));
}

/// A divergence in the `can_be_primary` capability is detected and heals,
/// the same way the observer capability does (C6) — the capability 2P-set
/// is the SINGLE replicated source and is snapshot-healable.
#[test]
fn can_be_primary_capability_divergence_detected_and_heals() {
    let mut a = ClusterState::<RunnerIdentifier>::new();
    let mut b = ClusterState::<RunnerIdentifier>::new();
    for s in [&mut a, &mut b] {
        s.apply(ClusterMutation::TaskAdded {
            hash: "t".into(),
            task: mk_task("t"),
        });
    }
    // `a` granted a peer primary-capability that `b` has not seen — a
    // capability 2P-set divergence with identical tasks.
    a.apply(ClusterMutation::SetCanBePrimary {
        peer_id: "cap-x".into(),
        can_be_primary: true,
        cap_version: Default::default(),
    });

    assert_ne!(a.digest(), b.digest());
    assert!(b.digest().is_behind(&a.digest()));

    b.restore(a.snapshot());
    assert_eq!(a.digest(), b.digest());
    assert!(!b.digest().is_behind(&a.digest()));
}

/// The residual LIVENESS divergence after a capability tombstone converges
/// does NOT drive further pulls — honest-liveness (C6). A `PeerRemoved`
/// writes BOTH a capability `Departed` tombstone (a converging fact, folded
/// into the digest) AND a `peer_state` Dead bit (node-local, NOT folded).
/// Once the tombstone converges via pull, the two replicas still disagree
/// on the `peer_state` alive/dead bit — `a` holds w1 Alive, `b` holds it
/// Dead — and anti-entropy must NEVER force-converge that (it must never
/// resurrect a peer `b` correctly buried). So post-convergence the digests
/// match and neither is behind, even though the liveness views diverge.
#[test]
fn residual_liveness_divergence_is_not_behind() {
    let mut a = ClusterState::<RunnerIdentifier>::new();
    let mut b = ClusterState::<RunnerIdentifier>::new();
    for s in [&mut a, &mut b] {
        s.apply(ClusterMutation::TaskAdded {
            hash: "t".into(),
            task: mk_task("t"),
        });
        // Both saw the peer join (Alive + an Advertised capability entry).
        s.apply(ClusterMutation::PeerJoined {
            peer_id: "w1".into(),
            is_observer: false,
            can_be_primary: false,
            cap_version: Default::default(),
        });
    }
    // `b` locally detected `w1` as dead and buried it: this writes a
    // capability `Departed` tombstone AND marks `peer_state` Dead.
    b.apply(ClusterMutation::PeerRemoved {
        id: "w1".into(),
        cause: RemovalCause::KeepaliveMiss,
    });
    // The capability tombstone is a converging fact, so `a` IS behind `b`
    // until it pulls (detect-WITH-heal).
    assert!(a.digest().is_behind(&b.digest()));
    a.restore(b.snapshot());

    // After convergence: capability matches (both hold w1 Departed). The
    // LIVENESS still diverges — `a` holds w1 Alive (restore never buries),
    // `b` holds it Dead — but liveness is NOT folded, so the digests match
    // and neither drives a further pull.
    assert!(a.is_peer_alive("w1"), "restore must NOT bury a's live peer");
    assert!(!b.is_peer_alive("w1"), "b correctly holds w1 dead");
    assert_eq!(a.digest(), b.digest());
    assert!(!a.digest().is_behind(&b.digest()));
    assert!(!b.digest().is_behind(&a.digest()));
}

/// The full detect → pull → converge → quiesce cycle the cadence drives:
/// an incomplete replica is detected as behind a complete one; pulling +
/// restoring the snapshot converges it; the SECOND digest comparison now
/// matches and triggers no further pull.
#[test]
fn divergence_detected_then_quiescent_after_restore() {
    // Complete replica.
    let mut complete = ClusterState::<RunnerIdentifier>::new();
    complete.apply(ClusterMutation::TaskAdded {
        hash: "a".into(),
        task: mk_task("a"),
    });
    complete.apply(ClusterMutation::TaskAdded {
        hash: "b".into(),
        task: mk_task("b"),
    });
    complete.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
        hash: "b".into(),
        result_data: None,
    });

    // Incomplete replica: only saw task "a".
    let mut incomplete = ClusterState::<RunnerIdentifier>::new();
    incomplete.apply(ClusterMutation::TaskAdded {
        hash: "a".into(),
        task: mk_task("a"),
    });

    // First round: the incomplete replica is behind the complete one.
    assert!(incomplete.digest().is_behind(&complete.digest()));

    // Pull: the existing snapshot RPC + restore lattice converges it.
    incomplete.restore(complete.snapshot());

    // Second round: digests now match → no further pull (self-quiescing).
    assert_eq!(incomplete.digest(), complete.digest());
    assert!(!incomplete.digest().is_behind(&complete.digest()));
}
