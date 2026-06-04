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
