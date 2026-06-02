//! Tests for the panik → self-departure CRDT contract.
//!
//! Single concern: a node observing its OWN panik signal announces its
//! departure from the mesh via a self-authored
//! `ClusterMutation::PeerRemoved { id: <self>, cause:
//! SelfDeparture(reason) }`. The apply rule for that mutation is
//! observability-only — it marks the peer `Dead`, fires
//! `PeerLifecycleEvent::Removed`, and leaves the task ledger UNTOUCHED.
//! It does NOT cancel cluster work or move any task to a terminal
//! state on a peer.
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
    });
    (s, "h".to_string())
}

/// A self-authored `PeerRemoved { SelfDeparture(reason) }` for the
/// node's OWN id is accepted and applied: the peer is marked Dead and
/// a `PeerLifecycleEvent::Removed` carrying the cause is emitted. This
/// is the membership-layer rule that a node may author its own
/// departure (vs. the historically primary-authored-only removal).
#[tokio::test]
async fn self_authored_departure_marks_peer_dead_and_emits_removed_event() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    s.install_lifecycle_sender(tx);
    // The node is alive in the mesh.
    s.apply(ClusterMutation::PeerJoined {
        peer_id: "self-node".into(),
        is_observer: false,
    });
    // Drain the Added event.
    let _ = rx.try_recv();

    let reason = "panik file: /tmp/asm.panik".to_string();
    let outcome = s.apply(ClusterMutation::PeerRemoved {
        id: "self-node".into(),
        cause: RemovalCause::SelfDeparture(BoundedString::from(reason.clone())),
    });
    assert_eq!(outcome, ApplyOutcome::Applied);

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
    });
    s.apply(ClusterMutation::TaskAdded {
        hash: "h-inflight".into(),
        task: mk_task("b"),
    });
    s.apply(ClusterMutation::TaskAssigned {
        hash: "h-inflight".into(),
        secondary: "departing".into(),
        worker: 0,
    });
    s.apply(ClusterMutation::TaskAdded {
        hash: "h-blocked".into(),
        task: mk_task("c"),
    });
    s.apply(ClusterMutation::TaskBlocked {
        hash: "h-blocked".into(),
        on: "h-prereq".into(),
    });

    let counts_before = s.counts();

    let outcome = s.apply(ClusterMutation::PeerRemoved {
        id: "departing".into(),
        cause: RemovalCause::SelfDeparture(BoundedString::from("panik SIGTERM (per-host)")),
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
/// `PeerJoined` in this replica's view): an absent id is inserted as
/// Dead so a late `PeerJoined` for the same id is blocked.
#[test]
fn self_departure_for_unseen_id_inserts_dead_and_preserves_ledger() {
    let (mut s, hash) = seed_with_one_pending();
    let outcome = s.apply(ClusterMutation::PeerRemoved {
        id: "never-joined".into(),
        cause: RemovalCause::SelfDeparture(BoundedString::from("panik file: /x")),
    });
    assert_eq!(outcome, ApplyOutcome::Applied);
    // Ledger untouched.
    assert!(matches!(
        s.task_state(&hash),
        Some(TaskState::Pending { .. })
    ));
    // Sticky-per-id still holds: a late PeerJoined for the same id is
    // a NoOp.
    assert_eq!(
        s.apply(ClusterMutation::PeerJoined {
            peer_id: "never-joined".into(),
            is_observer: false,
        }),
        ApplyOutcome::NoOp,
    );
}
