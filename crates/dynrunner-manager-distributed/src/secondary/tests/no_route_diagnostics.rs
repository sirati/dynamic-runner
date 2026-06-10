//! Resolution honesty at the egress no-route gate (`send_to`).
//!
//! "Resolved host is not a connected mesh member" used to conflate two
//! very different states the next incident must distinguish:
//!   - the host is ABSENT FROM THE REPLICATED MEMBERSHIP (removed via
//!     the `PeerRemoved` ledger, or never joined) — a membership
//!     decision;
//!   - the host IS a live replicated member this node merely has no
//!     transport wire to right now — a transport gap (redial /
//!     idle-timeout / blackhole), NOT a removal.
//!
//! These tests pin the split: the same wire-less target produces a
//! diagnostic naming whichever replicated-membership state it is in.
//! The probe semantics (a no-route `Err` either way) are unchanged.

use dynrunner_protocol_primary_secondary::address::{Destination, PeerId};
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, DistributedMessage, KeepaliveRole, RemovalCause,
};

use super::super::test_helpers::{TestId, election_config, make_secondary_membership};

/// A droppable frame to push through the egress gate.
fn frame(from: &str) -> DistributedMessage<TestId> {
    DistributedMessage::Keepalive {
        target: None,
        sender_id: from.to_string(),
        timestamp: 0.0,
        secondary_id: from.to_string(),
        active_workers: 0,
        emitter_role: KeepaliveRole::Secondary,
    }
}

/// One secondary whose transport membership view is EMPTY (no wire to
/// anyone), so every directed send hits the no-route gate and the
/// diagnostic split is observable per replicated-membership state.
#[tokio::test(flavor = "current_thread")]
async fn no_route_diagnostic_names_the_membership_state() {
    let (mut sec, _members) = make_secondary_membership(election_config("sec-a"), vec![]);
    sec.publish_membership();

    // (1) NEVER JOINED: no replicated entry for the target.
    let err = sec
        .send_to(
            Destination::Secondary(PeerId::from("ghost")),
            frame("sec-a"),
        )
        .await
        .expect_err("wire-less target must no-route");
    assert!(
        err.contains("not in the replicated membership"),
        "never-joined diagnostic, got: {err}"
    );

    // (2) LIVE MEMBER, NO WIRE: the target is Alive in the replicated
    // ledger but absent from the transport view — a transport gap, and
    // the diagnostic must say so (NOT a removal).
    sec.cluster_state.apply(ClusterMutation::PeerJoined {
        peer_id: "sec-b".into(),
        is_observer: false,
        can_be_primary: true,
        cap_version: Default::default(),
        member_gen: 0,
    });
    let err = sec
        .send_to(
            Destination::Secondary(PeerId::from("sec-b")),
            frame("sec-a"),
        )
        .await
        .expect_err("wire-less target must no-route");
    assert!(
        err.contains("live replicated cluster member") && err.contains("transport gap"),
        "live-member-no-wire diagnostic, got: {err}"
    );
    assert!(
        !err.contains("REMOVED"),
        "a transport gap must NOT read as a membership removal: {err}"
    );

    // (3) REMOVED MEMBER: the target was authoritatively removed.
    sec.cluster_state.apply(ClusterMutation::PeerRemoved {
        id: "sec-b".into(),
        cause: RemovalCause::KeepaliveMiss,
        member_gen: 0,
    });
    let err = sec
        .send_to(
            Destination::Secondary(PeerId::from("sec-b")),
            frame("sec-a"),
        )
        .await
        .expect_err("wire-less target must no-route");
    assert!(
        err.contains("REMOVED from the replicated membership"),
        "removed-member diagnostic, got: {err}"
    );

    // All three remain failover-health-probe shaped (the consumer key).
    assert!(err.contains("failover-health probe"));
}
