//! Standalone unit tests for the `process/` mesh primitives.
//!
//! These prove the C0 types in isolation (no `Process`, no coordinators).
//! This file holds the shared fixtures + the registration / loopback-prune
//! / live-membership tests; [`routing`] holds the dispatch / broadcast /
//! queued-send / retag tests.
//!
//! The remote side is a real [`ChannelPeerTransport`] — a genuine
//! role-agnostic `PeerTransport` whose `outgoing` outboxes feed receivers
//! we drain to assert what reached the wire. We do NOT bypass the trait.

mod routing;

use std::collections::HashMap;

use dynrunner_protocol_primary_secondary::DistributedMessage;
use dynrunner_protocol_primary_secondary::KeepaliveRole;
use dynrunner_protocol_primary_secondary::address::PeerId;
use dynrunner_transport_channel::ChannelPeerTransport;
use tokio::sync::mpsc;

use super::mesh::Mesh;
use super::role::LocalRole;

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub(super) struct TestId(String);

/// A keepalive frame tagged with `sender` so tests can identify which
/// frame arrived where.
pub(super) fn frame(sender: &str) -> DistributedMessage<TestId> {
    DistributedMessage::Keepalive {
        target: None,
        sender_id: sender.to_string(),
        timestamp: 1.0,
        secondary_id: sender.to_string(),
        active_workers: 0,
        emitter_role: KeepaliveRole::Secondary,
    }
}

/// As [`frame`], but with the Phase-C routing `target` stamped — the shape
/// the egress edge produces and the mesh-pump's `route_incoming` reads.
pub(super) fn frame_to(
    sender: &str,
    target: dynrunner_protocol_primary_secondary::address::Destination,
) -> DistributedMessage<TestId> {
    frame(sender).with_target(target)
}

pub(super) fn sender_of(msg: &DistributedMessage<TestId>) -> &str {
    match msg {
        DistributedMessage::Keepalive { sender_id, .. } => sender_id,
        other => panic!("unexpected frame: {other:?}"),
    }
}

/// Build a channel transport for `local_id` wired to `remotes`, returning
/// the transport plus a receiver per remote id so a test can drain what
/// the mesh sent to the wire.
pub(super) fn transport_with_remotes(
    local_id: &str,
    remotes: &[&str],
) -> (
    ChannelPeerTransport<TestId>,
    HashMap<String, mpsc::UnboundedReceiver<DistributedMessage<TestId>>>,
) {
    let (_in_tx, in_rx) = mpsc::unbounded_channel();
    let mut outgoing = HashMap::new();
    let mut receivers = HashMap::new();
    for r in remotes {
        let (tx, rx) = mpsc::unbounded_channel();
        outgoing.insert(r.to_string(), tx);
        receivers.insert(r.to_string(), rx);
    }
    let transport = ChannelPeerTransport::from_raw_channels(local_id.to_string(), outgoing, in_rx);
    (transport, receivers)
}

/// `register_local_role` mints the `(Arc<RoleSlot>, MeshClient, RoleInbox)`
/// trio together (M3): the slot reports the role + host, and a frame
/// delivered locally to that role surfaces on the paired inbox.
#[tokio::test]
async fn register_local_role_mints_matched_trio() {
    let (transport, _r) = transport_with_remotes("host-a", &[]);
    let mut mesh = Mesh::<TestId, _>::new(transport);

    let (slot, client, mut inbox) =
        mesh.register_local_role(LocalRole::Secondary, PeerId::from("host-a"));

    assert_eq!(slot.role(), LocalRole::Secondary);
    assert_eq!(slot.peer_id(), &PeerId::from("host-a"));
    assert_eq!(client.origin(), LocalRole::Secondary);

    // A loopback delivery to the secondary surfaces on this inbox.
    assert!(mesh.deliver_local(LocalRole::Secondary, frame("loop")));
    assert_eq!(sender_of(&inbox.try_recv().expect("delivered")), "loop");
}

/// `deliver_local` reaches the right slot, and SELF-PRUNES when the
/// role's `Arc` is dropped (H4 teardown / BUG-2 prune): the dropped `Arc`
/// makes the `Weak` fail to upgrade; `deliver_local` returns `false` and
/// prunes the slot — no panic. A later delivery to the pruned role is a
/// clean `false`.
#[tokio::test]
async fn deliver_local_self_prunes_on_dropped_arc() {
    let (transport, _r) = transport_with_remotes("host-a", &[]);
    let mut mesh = Mesh::<TestId, _>::new(transport);

    let (slot, _client, mut inbox) =
        mesh.register_local_role(LocalRole::Primary, PeerId::from("host-a"));

    // Live: delivery lands.
    assert!(mesh.deliver_local(LocalRole::Primary, frame("alive")));
    assert_eq!(sender_of(&inbox.try_recv().expect("delivered")), "alive");

    // Drop the owning `Arc` — role death.
    drop(slot);

    // Upgrade now fails: delivery returns false, the slot self-prunes,
    // and no panic occurs.
    assert!(
        !mesh.deliver_local(LocalRole::Primary, frame("after-death")),
        "delivery to a dead slot must fail, not panic"
    );
    // A second attempt is a clean false (the Weak was pruned).
    assert!(!mesh.deliver_local(LocalRole::Primary, frame("again")));
}

/// `peer_count` / `has_peer` reflect the LIVE transport membership
/// (`connections.len()` / the connection table) — never a shadow. The
/// `MeshClient` reads the pump-published view, which equals the live read
/// after `publish_membership`.
#[tokio::test]
async fn peer_count_reflects_live_membership_no_shadow() {
    let (transport, _r) = transport_with_remotes("host-a", &["remote-1", "remote-2"]);
    let mut mesh = Mesh::<TestId, _>::new(transport);

    // The mesh reads the transport live: two remotes.
    assert_eq!(mesh.peer_count(), 2);
    assert!(mesh.has_peer(&PeerId::from("remote-1")));
    assert!(!mesh.has_peer(&PeerId::from("absent")));

    let (_slot, client, _inbox) =
        mesh.register_local_role(LocalRole::Secondary, PeerId::from("host-a"));

    // Before the pump publishes, the detached client view is empty
    // (honest: it has not been told yet) — it is NOT a fabricated count.
    assert_eq!(client.peer_count(), 0);

    // The pump publishes the live transport read; the client now sees it.
    mesh.publish_membership();
    assert_eq!(
        client.peer_count(),
        2,
        "client reads the live-published count"
    );
    assert!(client.has_peer(&PeerId::from("remote-1")));
    assert!(client.has_peer(&PeerId::from("remote-2")));
    assert!(!client.has_peer(&PeerId::from("absent")));
}
