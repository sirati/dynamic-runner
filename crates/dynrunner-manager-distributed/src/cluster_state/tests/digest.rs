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

/// A divergence that exists ONLY in the observer set does NOT make a
/// replica "behind": the membership/role sets are excluded from the
/// anti-entropy detector. They are non-monotone-via-removal and the
/// additive/sticky `restore()` cannot reconcile a stale entry, so flagging
/// them would loop a no-op pull forever; their additions converge over the
/// live `PeerJoined`/`PeerRemoved` broadcasts + roster re-emit instead.
///
/// (Under the older detector this case flagged `is_behind` and triggered a
/// permanent pull loop — this test pins that it no longer does.)
#[test]
fn observer_only_divergence_is_not_behind() {
    // Two replicas with the SAME task ledger.
    let mut a = ClusterState::<RunnerIdentifier>::new();
    let mut b = ClusterState::<RunnerIdentifier>::new();
    for s in [&mut a, &mut b] {
        s.apply(ClusterMutation::TaskAdded {
            hash: "t".into(),
            task: mk_task("t"),
        });
    }
    // `a` knows an observer that `b` does not — a pure observer-set
    // divergence with identical tasks.
    a.apply(ClusterMutation::PeerJoined {
        peer_id: "obs-x".into(),
        is_observer: true,
        can_be_primary: false,
    });
    assert!(a.role_table().observers.contains("obs-x"));
    assert!(!b.role_table().observers.contains("obs-x"));

    // The digests are equal (observers are not summarised) and neither side
    // is behind — no pull on an observer-only difference.
    assert_eq!(a.digest(), b.digest());
    assert!(!b.digest().is_behind(&a.digest()));
    assert!(!a.digest().is_behind(&b.digest()));
}

/// A divergence that exists ONLY in the primary-capability set does NOT
/// make a replica "behind", for the same reason as observers: the live
/// `SetCanBePrimary(false)`/`PeerRemoved` path REMOVES ids, and `restore()`
/// replaces the capability set only when local is empty — a stale entry is
/// never snapshot-healable, so anti-entropy must not flag it.
///
/// (The older detector flagged this and looped a no-op pull; pinned fixed.)
#[test]
fn can_be_primary_only_divergence_is_not_behind() {
    let mut a = ClusterState::<RunnerIdentifier>::new();
    let mut b = ClusterState::<RunnerIdentifier>::new();
    for s in [&mut a, &mut b] {
        s.apply(ClusterMutation::TaskAdded {
            hash: "t".into(),
            task: mk_task("t"),
        });
    }
    // `a` granted a peer primary-capability that `b` has not seen — a pure
    // can_be_primary divergence with identical tasks.
    a.apply(ClusterMutation::SetCanBePrimary {
        peer_id: "cap-x".into(),
        can_be_primary: true,
    });
    assert!(a.can_be_primary("cap-x"));
    assert!(!b.can_be_primary("cap-x"));

    assert_eq!(a.digest(), b.digest());
    assert!(!b.digest().is_behind(&a.digest()));
    assert!(!a.digest().is_behind(&b.digest()));
}

/// A divergence that exists ONLY in the alive-member set does NOT make a
/// replica "behind" — and this exclusion is INTENTIONAL per the honest-
/// liveness design, not merely a snapshot-healability concession. Each node
/// owns its own liveness view: a node that locally buried a peer as Dead
/// legitimately disagrees with a node still holding it Alive. Anti-entropy
/// must NEVER force-converge that — it must never resurrect a peer a node
/// correctly detected as dead. So a same-`tasks` state that differs ONLY in
/// `alive_members` is NOT behind. This documents the known, deliberate
/// limitation: alive-member divergence never drives a pull.
#[test]
fn alive_member_only_divergence_is_not_behind() {
    let mut a = ClusterState::<RunnerIdentifier>::new();
    let mut b = ClusterState::<RunnerIdentifier>::new();
    for s in [&mut a, &mut b] {
        s.apply(ClusterMutation::TaskAdded {
            hash: "t".into(),
            task: mk_task("t"),
        });
        // Both saw the peer join (Alive).
        s.apply(ClusterMutation::PeerJoined {
            peer_id: "w1".into(),
            is_observer: false,
            can_be_primary: false,
        });
    }
    // `b` locally detected `w1` as dead (its own liveness view) and buried
    // it; `a` still holds it Alive. The alive-member sets now diverge while
    // the task ledger is identical.
    b.apply(ClusterMutation::PeerRemoved {
        id: "w1".into(),
        cause: RemovalCause::KeepaliveMiss,
    });

    // `b` (fewer alive) must NOT be behind `a` (still holds w1 Alive):
    // anti-entropy must never resurrect the peer `b` correctly buried.
    assert_eq!(a.digest(), b.digest());
    assert!(!b.digest().is_behind(&a.digest()));
    // And symmetrically `a` is not driven to drop a live peer either.
    assert!(!a.digest().is_behind(&b.digest()));
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
