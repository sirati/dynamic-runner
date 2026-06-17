//! Tests for the panik → self-departure CRDT contract.
//!
//! Single concern: a node observing its OWN panik signal announces its
//! departure from the mesh via a self-authored
//! `ClusterMutation::PeerRemoved { id: <self>, cause:
//! SelfDeparture(reason) }`. The apply rule for that mutation projects
//! the peer OUT of membership/roles via the convergent `Departed`
//! capability tombstone, fires `PeerLifecycleEvent::Removed`, and leaves
//! the task ledger UNTOUCHED. Unlike a genuine-death removal it does NOT
//! flip the node-local `peer_state` liveness bit to `Dead` (that would
//! shrink the perceived live fleet and arm a spurious election). It does
//! NOT cancel cluster work or move any task to a terminal state on a peer.
//!
//! These pin the post-`PanikRequested` contract: there is no longer a
//! cluster-wide cancel-sweep, no `panik_active` latch, and no
//! `TaskState::Cancelled`. A peer's departure never touches another
//! node's task accounting.

use super::*;
use dynrunner_core::BoundedString;

fn seed_with_one_pending() -> (ClusterState<RunnerIdentifier>, String) {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_task("a"),
        def_id: None,
    });
    (s, "h".to_string())
}

/// A self-authored `PeerRemoved { SelfDeparture(reason) }` for an
/// observer-capable node's OWN id is accepted and applied: the peer is
/// projected OUT of the role table via the `Departed` tombstone, a
/// `PeerLifecycleEvent::Removed` carrying the cause is emitted — and
/// critically, the node-local `peer_state` liveness bit stays `Alive`
/// (NOT flipped Dead). The departing peer remains a first-class CRDT
/// participant so its final mutations converge; only the role/setup
/// projection drops it. This is the membership-layer rule that a node
/// may author its own departure (vs. the historically primary-authored-
/// only removal).
#[tokio::test]
async fn self_authored_departure_projects_out_without_dead_flip() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    s.install_lifecycle_sender(tx);
    // The node is alive in the mesh and an observer (so we can observe
    // the role projection drop it).
    s.apply(ClusterMutation::PeerJoined {
        peer_id: "self-node".into(),
        is_observer: true,
        can_be_primary: false,
        cap_version: Default::default(),
        member_gen: 0,
    });
    assert!(s.is_peer_alive("self-node"));
    assert!(s.role_table().observers.contains("self-node"));
    // Drain the Added event.
    let _ = rx.try_recv();

    let reason = "panik file: /tmp/asm.panik".to_string();
    let outcome = s.apply(ClusterMutation::PeerRemoved {
        id: "self-node".into(),
        cause: RemovalCause::SelfDeparture(BoundedString::from(reason.clone())),
        member_gen: 0,
    });
    assert_eq!(outcome, ApplyOutcome::Applied);

    // The CONTRACT: liveness UNTOUCHED (still Alive), role projection
    // dropped via the Departed tombstone.
    assert!(
        s.is_peer_alive("self-node"),
        "a self-departure must NOT flip peer_state to Dead",
    );
    assert!(
        !s.role_table().observers.contains("self-node"),
        "the Departed tombstone must project the peer out of the role table",
    );

    match rx.try_recv() {
        Ok(crate::peer_lifecycle::PeerLifecycleEvent::Removed { id, cause }) => {
            assert_eq!(id, "self-node");
            assert_eq!(
                cause,
                RemovalCause::SelfDeparture(BoundedString::from(reason)),
            );
        }
        other => panic!("expected Removed event with SelfDeparture cause, got {other:?}"),
    }

    // Idempotent re-delivery: a second SelfDeparture for the same
    // still-Alive, already-tombstoned id moves nothing → NoOp.
    assert_eq!(
        s.apply(ClusterMutation::PeerRemoved {
            id: "self-node".into(),
            cause: RemovalCause::SelfDeparture(BoundedString::from("panik file: /tmp/asm.panik")),
            member_gen: 0,
        }),
        ApplyOutcome::NoOp,
        "a duplicate self-departure must be a NoOp",
    );
}

/// A genuine-death removal (any cause OTHER than `SelfDeparture`) still
/// flips `peer_state` to `Dead` — the exemption is scoped to
/// `SelfDeparture` ONLY, so the authoritative-death path is intact.
#[test]
fn genuine_death_removal_still_flips_dead() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::PeerJoined {
        peer_id: "p1".into(),
        is_observer: true,
        can_be_primary: false,
        cap_version: Default::default(),
        member_gen: 0,
    });
    assert!(s.is_peer_alive("p1"));
    let outcome = s.apply(ClusterMutation::PeerRemoved {
        id: "p1".into(),
        cause: RemovalCause::KeepaliveMiss,
        member_gen: 0,
    });
    assert_eq!(outcome, ApplyOutcome::Applied);
    assert!(
        !s.is_peer_alive("p1"),
        "a genuine-death removal MUST flip peer_state to Dead",
    );
    assert!(!s.role_table().observers.contains("p1"));
}

/// Negative control: a peer applying another node's self-departure
/// `PeerRemoved` must NOT move any task to a terminal / cancelled
/// state. The task ledger is observably unchanged by the departure.
#[tokio::test]
async fn peer_departure_does_not_touch_task_ledger() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    // Seed a mix of non-terminal task states.
    s.apply(ClusterMutation::TaskAdded {
        hash: "h-pending".into(),
        task: mk_task("a"),
        def_id: None,
    });
    s.apply(ClusterMutation::TaskAdded {
        hash: "h-inflight".into(),
        task: mk_task("b"),
        def_id: None,
    });
    s.apply(ClusterMutation::TaskAssigned {
        attempt: 0,
        hash: "h-inflight".into(),
        secondary: "departing".into(),
        worker: 0,
        version: Default::default(),
    });
    s.apply(ClusterMutation::TaskAdded {
        hash: "h-blocked".into(),
        task: mk_task("c"),
        def_id: None,
    });
    s.apply(ClusterMutation::TaskBlocked {
        hash: "h-blocked".into(),
        on: "h-prereq".into(),
    });

    let counts_before = s.counts();

    let outcome = s.apply(ClusterMutation::PeerRemoved {
        id: "departing".into(),
        cause: RemovalCause::SelfDeparture(BoundedString::from("panik file: /var/run/panik")),
        member_gen: 0,
    });
    assert_eq!(outcome, ApplyOutcome::Applied);

    // Negative control: NOTHING in the task ledger moved. The
    // departure is membership-only.
    let counts_after = s.counts();
    assert_eq!(
        counts_before, counts_after,
        "a peer's self-departure must not change any task's state",
    );
    assert!(matches!(
        s.task_state("h-pending"),
        Some(TaskState::Pending { .. })
    ));
    assert!(matches!(
        s.task_state("h-inflight"),
        Some(TaskState::InFlight { .. })
    ));
    assert!(matches!(
        s.task_state("h-blocked"),
        Some(TaskState::Blocked { .. })
    ));
}

/// `SelfDeparture` round-trips through the `PeerRemoved` apply on a
/// previously-unseen id (the departing node need not have a prior
/// `PeerJoined` in this replica's view): the absent id gets a `Departed`
/// capability tombstone (NOT a `Dead` `peer_state` entry), and the
/// ledger is preserved. A late same-generation `PeerJoined` may bring
/// the liveness bit Alive (honest — the frames ARE arriving), but the
/// same-generation tombstone ABSORBS the advertise so the peer stays
/// projected OUT of the role table; re-admission requires the next
/// generation (the convergent membership-incarnation rule).
#[test]
fn self_departure_for_unseen_id_tombstones_without_dead_and_preserves_ledger() {
    let (mut s, hash) = seed_with_one_pending();
    let outcome = s.apply(ClusterMutation::PeerRemoved {
        id: "never-joined".into(),
        cause: RemovalCause::SelfDeparture(BoundedString::from("panik file: /x")),
        member_gen: 0,
    });
    assert_eq!(outcome, ApplyOutcome::Applied);
    // Ledger untouched.
    assert!(matches!(
        s.task_state(&hash),
        Some(TaskState::Pending { .. })
    ));
    // No Dead flip: the id was never marked a live member, and the
    // departure did NOT insert a Dead peer_state entry.
    assert!(!s.is_peer_alive("never-joined"));
    assert_eq!(
        s.peer_membership("never-joined"),
        crate::cluster_state::PeerMembership::NeverJoined,
        "a self-departure must NOT insert a Dead peer_state entry",
    );
    // A late same-generation PeerJoined applies (no sticky-Dead block),
    // but the same-gen Departed tombstone keeps the peer out of roles.
    assert_eq!(
        s.apply(ClusterMutation::PeerJoined {
            peer_id: "never-joined".into(),
            is_observer: true,
            can_be_primary: false,
            cap_version: Default::default(),
            member_gen: 0,
        }),
        ApplyOutcome::Applied,
    );
    assert!(
        !s.role_table().observers.contains("never-joined"),
        "the same-generation Departed tombstone must keep the peer projected out",
    );
}
