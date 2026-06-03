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
