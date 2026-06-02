//! Tests for the `SecondaryCapacity` apply rule, the snapshot
//! round-trip of the per-secondary capacity map, and the roster /
//! capacity / total accessors.
//!
//! Pins:
//!
//!   - First apply records a secondary's static capacity; a second
//!     apply for the same id is an idempotent NoOp (set-once — capacity
//!     is static for the run, so re-application via snapshot replay or
//!     the idempotent PeerJoined re-emit never clobbers it).
//!   - The `secondary_capacities` map round-trips through
//!     snapshot/restore so a freshly-promoted primary and late-joining
//!     observers reconstruct the full roster on restore.
//!   - The accessors return the per-secondary capacity, the
//!     known-secondary roster set, and the total worker count.

use super::*;
use dynrunner_core::{ResourceAmount, ResourceKind};

fn mem(amount: u64) -> ResourceAmount {
    ResourceAmount {
        kind: ResourceKind::memory(),
        amount,
    }
}

/// First apply records the capacity; the accessor returns the stored
/// worker count + resources.
#[test]
fn secondary_capacity_apply_records_capacity() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    assert!(s.secondary_capacity("sec-0").is_none());
    assert_eq!(
        s.apply(ClusterMutation::SecondaryCapacity {
            secondary: "sec-0".into(),
            worker_count: 4,
            resources: vec![mem(2 * 1024 * 1024 * 1024)],
        }),
        ApplyOutcome::Applied
    );
    let cap = s.secondary_capacity("sec-0").expect("entry present");
    assert_eq!(cap.worker_count, 4);
    assert_eq!(cap.resources, vec![mem(2 * 1024 * 1024 * 1024)]);
}

/// Set-once: a second `SecondaryCapacity` apply for the SAME secondary
/// is an idempotent NoOp — it must NOT clobber the first-recorded
/// value, even when the second apply carries a different worker count /
/// resource set (a stale re-broadcast or snapshot replay must not
/// regress the canonical first-recorded capacity).
#[test]
fn secondary_capacity_apply_is_set_once() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    assert_eq!(
        s.apply(ClusterMutation::SecondaryCapacity {
            secondary: "sec-0".into(),
            worker_count: 4,
            resources: vec![mem(2 * 1024 * 1024 * 1024)],
        }),
        ApplyOutcome::Applied
    );
    // Second apply for the same id — different payload — must NoOp.
    assert_eq!(
        s.apply(ClusterMutation::SecondaryCapacity {
            secondary: "sec-0".into(),
            worker_count: 99,
            resources: vec![mem(1)],
        }),
        ApplyOutcome::NoOp
    );
    // The first-recorded value survives unchanged.
    let cap = s.secondary_capacity("sec-0").expect("entry present");
    assert_eq!(cap.worker_count, 4);
    assert_eq!(cap.resources, vec![mem(2 * 1024 * 1024 * 1024)]);

    // A DIFFERENT secondary still applies (set-once is per-id).
    assert_eq!(
        s.apply(ClusterMutation::SecondaryCapacity {
            secondary: "sec-1".into(),
            worker_count: 2,
            resources: vec![],
        }),
        ApplyOutcome::Applied
    );
    assert_eq!(s.secondary_capacity("sec-1").unwrap().worker_count, 2);
}

/// The roster + total accessors aggregate over every recorded
/// secondary: `known_secondaries` enumerates the ids, `total_worker_count`
/// sums their advertised worker counts.
#[test]
fn secondary_capacity_accessors_return_roster_and_totals() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    assert_eq!(s.total_worker_count(), 0);
    assert_eq!(s.known_secondaries().count(), 0);

    s.apply(ClusterMutation::SecondaryCapacity {
        secondary: "sec-0".into(),
        worker_count: 4,
        resources: vec![mem(1024)],
    });
    s.apply(ClusterMutation::SecondaryCapacity {
        secondary: "sec-1".into(),
        worker_count: 3,
        resources: vec![],
    });

    assert_eq!(s.total_worker_count(), 7);
    let roster: HashSet<&str> = s.known_secondaries().collect();
    assert_eq!(roster, HashSet::from(["sec-0", "sec-1"]));
}

/// The `secondary_capacities` map round-trips through snapshot/restore
/// so a freshly-promoted primary and late-joining observers hold the
/// full per-secondary roster on restore, before any live
/// `SecondaryCapacity` broadcast reaches them.
#[test]
fn secondary_capacity_snapshot_round_trip() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::SecondaryCapacity {
        secondary: "sec-0".into(),
        worker_count: 4,
        resources: vec![mem(2 * 1024 * 1024 * 1024)],
    });
    s.apply(ClusterMutation::SecondaryCapacity {
        secondary: "sec-1".into(),
        worker_count: 3,
        resources: vec![],
    });

    let snap = s.snapshot();
    let mut joiner = ClusterState::<RunnerIdentifier>::new();
    joiner.restore(snap);

    assert_eq!(joiner.total_worker_count(), 7);
    let roster: HashSet<&str> = joiner.known_secondaries().collect();
    assert_eq!(roster, HashSet::from(["sec-0", "sec-1"]));
    let cap = joiner.secondary_capacity("sec-0").expect("entry present");
    assert_eq!(cap.worker_count, 4);
    assert_eq!(cap.resources, vec![mem(2 * 1024 * 1024 * 1024)]);
}

/// Per-secondary first-write-wins merge on `restore`: a snapshot entry
/// for a secondary the joiner already recorded (via a live broadcast)
/// must NOT overwrite the local entry — same monotonic-insertion shape
/// as `task_outputs`. Each capacity is set exactly once anyway (set-once
/// apply), so the carried value matches; the rule defends against a
/// blanket replace clobbering a legitimately-applied local entry when
/// the snapshot interleaves with live broadcasts.
#[test]
fn secondary_capacity_restore_keeps_local_when_present() {
    let mut joiner = ClusterState::<RunnerIdentifier>::new();
    joiner.apply(ClusterMutation::SecondaryCapacity {
        secondary: "sec-0".into(),
        worker_count: 4,
        resources: vec![],
    });

    // A divergent snapshot for the same id (different worker_count) must
    // not regress the locally-applied entry; a fresh id is inserted.
    let mut peer = ClusterState::<RunnerIdentifier>::new();
    peer.apply(ClusterMutation::SecondaryCapacity {
        secondary: "sec-0".into(),
        worker_count: 99,
        resources: vec![],
    });
    peer.apply(ClusterMutation::SecondaryCapacity {
        secondary: "sec-1".into(),
        worker_count: 2,
        resources: vec![],
    });

    joiner.restore(peer.snapshot());
    assert_eq!(joiner.secondary_capacity("sec-0").unwrap().worker_count, 4);
    assert_eq!(joiner.secondary_capacity("sec-1").unwrap().worker_count, 2);
}
