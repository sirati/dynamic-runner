//! Unit tests for the channel peer transport's basics: full-mesh
//! and partial-mesh constructors, the `broadcast` / `send_to_peer`
//! fan-out contract, and per-id membership (`has_peer`).

use dynrunner_protocol_primary_secondary::{DistributedMessage, KeepaliveRole, PeerTransport};

use crate::mesh::peer_mesh;

/// `peer_mesh` wires N transports with all-to-all senders. A broadcast
/// from one peer should reach every other peer's inbox; nothing should
/// loop back to the sender.
#[tokio::test]
async fn peer_mesh_broadcasts_to_all_others() {
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
    struct TestId(String);

    let ids = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    let mut transports = peer_mesh::<TestId>(&ids);

    assert_eq!(transports.len(), 3);
    assert_eq!(transports[0].peer_count(), 2);
    assert_eq!(transports[1].peer_count(), 2);
    assert_eq!(transports[2].peer_count(), 2);

    let msg = DistributedMessage::Keepalive {
        target: None,
        sender_id: "a".into(),
        timestamp: 1.0,
        secondary_id: "a".into(),
        active_workers: 0,
        emitter_role: KeepaliveRole::Secondary,
    };
    transports[0].broadcast(msg).await.unwrap();

    // a does not receive its own broadcast
    assert!(transports[0].try_recv_peer().is_none());
    // b and c do
    assert!(transports[1].try_recv_peer().is_some());
    assert!(transports[2].try_recv_peer().is_some());
}

/// `send_to_peer` reaches exactly one inbox.
#[tokio::test]
async fn peer_mesh_send_to_specific_peer() {
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
    struct TestId(String);

    let ids = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    let mut transports = peer_mesh::<TestId>(&ids);

    let msg = DistributedMessage::Keepalive {
        target: None,
        sender_id: "a".into(),
        timestamp: 1.0,
        secondary_id: "a".into(),
        active_workers: 0,
        emitter_role: KeepaliveRole::Secondary,
    };
    transports[0].send_to_peer("b", msg).await.unwrap();

    assert!(transports[1].try_recv_peer().is_some());
    assert!(transports[2].try_recv_peer().is_none());
    assert!(transports[0].try_recv_peer().is_none());
}

// ── PeerTransport unicast / broadcast fan-out tests ──
//
// These pin the by-id `send_to_peer` and mesh `broadcast` delivery
// contract the coordinator edge rests on after resolving a typed
// `Destination` to a concrete peer-id (role-blind: the transport
// never sees a role).

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct SendTestId(pub(crate) String);

pub(crate) fn keepalive(sender: &str) -> DistributedMessage<SendTestId> {
    DistributedMessage::Keepalive {
        target: None,
        sender_id: sender.into(),
        timestamp: 1.0,
        secondary_id: sender.into(),
        active_workers: 0,
        emitter_role: KeepaliveRole::Secondary,
    }
}

/// `has_peer` reports REAL per-id membership against the `outgoing`
/// writer table — not a constant. In a pre-wired all-to-all mesh
/// every other id is a member and a never-registered id is not;
/// `connect_to` flips a fresh id false→true and `disconnect_from`
/// flips it true→false. Pinning both flips proves the predicate
/// tracks the table rather than returning a fixed answer.
#[tokio::test]
async fn has_peer_tracks_outgoing_membership() {
    use dynrunner_protocol_primary_secondary::PeerId;

    let ids = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    let mut transports = peer_mesh::<SendTestId>(&ids);

    // Pre-wired mesh: peer `a` knows `b` and `c`, not itself, not a
    // stranger.
    assert!(transports[0].has_peer(&PeerId::from("b")));
    assert!(transports[0].has_peer(&PeerId::from("c")));
    assert!(!transports[0].has_peer(&PeerId::from("a")));
    assert!(!transports[0].has_peer(&PeerId::from("d")));

    // A fresh id is not a member until a writer is registered for it…
    assert!(!transports[0].has_peer(&PeerId::from("d")));
    let (d_tx, _d_rx) = tokio::sync::mpsc::unbounded_channel::<DistributedMessage<SendTestId>>();
    transports[0].connect_to("d".to_string(), d_tx);
    // …then `has_peer` flips false → true.
    assert!(transports[0].has_peer(&PeerId::from("d")));

    // Removing the writer flips it back true → false (the partition
    // path the relay tests use).
    transports[0].disconnect_from("b");
    assert!(!transports[0].has_peer(&PeerId::from("b")));
}

/// `send_to_peer(id, msg)` reaches exactly that peer and nobody else.
#[tokio::test]
async fn send_to_peer_reaches_recipient() {
    let ids = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    let mut transports = peer_mesh::<SendTestId>(&ids);

    transports[0]
        .send_to_peer("b", keepalive("a"))
        .await
        .unwrap();

    assert!(transports[1].try_recv_peer().is_some());
    assert!(transports[2].try_recv_peer().is_none());
    assert!(transports[0].try_recv_peer().is_none());
}

/// `broadcast(msg)` fans out to every other peer; nothing loops back
/// to the sender.
#[tokio::test]
async fn broadcast_fans_out_to_all_others() {
    let ids = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    let mut transports = peer_mesh::<SendTestId>(&ids);

    transports[0].broadcast(keepalive("a")).await.unwrap();

    assert!(transports[0].try_recv_peer().is_none());
    assert!(transports[1].try_recv_peer().is_some());
    assert!(transports[2].try_recv_peer().is_some());
}

// ── forward_to_peer: transparent forwarding ──
//
// `forward_to_peer(to, package)` routes an already-received package
// toward `to` while preserving its ORIGINAL origin attribution — the
// package's own `sender_id`. "Transparent" means the destination sees
// the original sender, not whoever forwarded. The two tests below pin
// that guarantee on the DIRECT path (forwarder adjacent to target) and
// the VIA-RELAY path (forwarder must hop through an intermediary),
// because the relay envelope wraps the package and the relay machinery
// must hand the inner package back to the receiver with its origin
// untouched.

/// DIRECT path: `b` forwards to its direct neighbour `c` a package
/// authored by a THIRD party `origin-x`. `c` receives the package with
/// `sender_id == "origin-x"` — not `"b"` (the forwarder), not `"c"`.
#[tokio::test]
async fn forward_to_peer_preserves_origin_direct() {
    let ids = vec!["b".to_string(), "c".to_string()];
    let mut transports = peer_mesh::<SendTestId>(&ids);
    // index 0 = b, index 1 = c (peer_mesh returns in input order)

    // A package whose origin is a third party, received earlier by `b`
    // and now forwarded on toward `c`.
    let package = keepalive("origin-x");
    transports[0].forward_to_peer("c", package).await.unwrap();

    let delivered = transports[1]
        .try_recv_peer()
        .expect("c receives the forwarded package");
    assert_eq!(
        delivered.sender_id(),
        "origin-x",
        "the destination must see the ORIGINAL sender, not the forwarder"
    );
}

/// VIA-RELAY path: topology `a — b — c` (a and c are NOT directly
/// linked). `a` forwards a package authored by `origin-x` toward `c`;
/// the router has no direct a→c link so it wraps the package in a relay
/// envelope and hops it through `b`. `c` must still see `origin-x` —
/// the relay wrapping/unwrapping must leave the inner package's origin
/// intact end-to-end.
#[tokio::test]
async fn forward_to_peer_preserves_origin_via_relay() {
    let ids = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    // Linear chain: a—b and b—c, but NO a—c link.
    let links = vec![
        ("a".to_string(), "b".to_string()),
        ("b".to_string(), "c".to_string()),
    ];
    let mut transports =
        crate::mesh::peer_mesh_with_adjacency::<SendTestId>(&ids, &links);
    // index 0 = a, 1 = b, 2 = c

    // a has no direct route to c, so forward must relay through b.
    assert!(!transports[0].has_peer(&dynrunner_protocol_primary_secondary::PeerId::from("c")));

    let package = keepalive("origin-x");
    transports[0].forward_to_peer("c", package).await.unwrap();

    // Pump the relay hop through b's ASYNC recv path: b receives the
    // relay envelope addressed through it and forwards it on toward c
    // inside `process_inbound` (which dispatches the forward via b's
    // outgoing table). The sync `try_recv_peer` path CANNOT forward a
    // relay-for-others (it drops with a warn — see
    // `Router::process_inbound_sync`), so this hop must use `recv_peer`.
    // The envelope is forwarded internally, never delivered to b's
    // application layer, so b's recv yields nothing and we time it out.
    let b_drain = tokio::time::timeout(
        std::time::Duration::from_millis(50),
        transports[1].recv_peer(),
    )
    .await;
    assert!(
        b_drain.is_err(),
        "the relay envelope is forwarded by b, not delivered to b"
    );

    // c receives the unwrapped package with its origin preserved.
    let delivered = transports[2]
        .try_recv_peer()
        .expect("c receives the relayed package");
    assert_eq!(
        delivered.sender_id(),
        "origin-x",
        "relay wrap/unwrap must preserve the ORIGINAL sender end-to-end"
    );
}
