//! Unit tests for the channel peer transport's basics: full-mesh
//! and partial-mesh constructors, plus the `send(Address::…)`
//! dispatch contract covered by the trait's default impl (Peer /
//! Broadcast(Mesh) / Broadcast(AllSecondaries) plus the unresolved-
//! Role error case).

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

// ── PeerTransport::send default-impl contract tests ──
//
// These pin the Step 1 default impl so Step 3 (which will replace
// the Role/AllSecondaries error arms with real dispatch) has a
// regression net. Each test exercises exactly one Address variant
// through the trait's default body — the channel transport itself
// does not override `send`, so what we observe here is the protocol-
// crate default routing through `send_to_peer` / `broadcast`.

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

/// `send(Address::Peer(id), msg)` routes through the default impl
/// to `send_to_peer` and reaches exactly that peer.
#[tokio::test]
async fn send_address_peer_reaches_recipient() {
    use dynrunner_protocol_primary_secondary::Address;

    let ids = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    let mut transports = peer_mesh::<SendTestId>(&ids);

    transports[0]
        .send(Address::Peer("b".to_string()), keepalive("a"))
        .await
        .unwrap();

    assert!(transports[1].try_recv_peer().is_some());
    assert!(transports[2].try_recv_peer().is_none());
    assert!(transports[0].try_recv_peer().is_none());
}

/// `send(Address::Broadcast(Scope::Mesh), msg)` routes through the
/// default impl to `broadcast` and fans out to every other peer.
#[tokio::test]
async fn send_address_broadcast_mesh_fans_out() {
    use dynrunner_protocol_primary_secondary::{Address, Scope};

    let ids = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    let mut transports = peer_mesh::<SendTestId>(&ids);

    transports[0]
        .send(Address::Broadcast(Scope::Mesh), keepalive("a"))
        .await
        .unwrap();

    assert!(transports[0].try_recv_peer().is_none());
    assert!(transports[1].try_recv_peer().is_some());
    assert!(transports[2].try_recv_peer().is_some());
}

/// Post-Step 3: `send(Address::Role(Role::Primary), msg)`
/// against a cold role-table cache returns an `Err` that names
/// "Role" and the missing cache state. Pins the cache-cold
/// contract for `Role::Primary`; `Role::Self_` has its own
/// cache-seeded behavior (Step 4 — see
/// [`role_self_cache_populated_at_init`]) and is covered there.
/// Pre-Step-3 this test asserted "not yet supported"; the
/// assertion shifted to the new contract when the real
/// dispatch landed.
#[tokio::test]
async fn send_address_role_returns_err() {
    use dynrunner_protocol_primary_secondary::{Address, Role};

    let ids = vec!["a".to_string(), "b".to_string()];
    let mut transports = peer_mesh::<SendTestId>(&ids);

    let err = transports[0]
        .send(Address::Role(Role::Primary), keepalive("a"))
        .await
        .expect_err("Role(Primary) with cold cache must error");
    assert!(
        err.contains("Role"),
        "error must reference Role; got: {err}"
    );
    assert!(
        err.contains("cache"),
        "error must reference cache state; got: {err}"
    );

    // No message must have been delivered to any peer's inbox.
    assert!(transports[0].try_recv_peer().is_none());
    assert!(transports[1].try_recv_peer().is_none());
}

/// Post-Step 5: `send(Address::Broadcast(Scope::AllSecondaries), msg)`
/// fans out via the default impl's `broadcast` delegation. From a
/// primary caller's vantage (the only Step-5 caller), every
/// peer-mesh member is by definition a secondary, so `AllSecondaries`
/// and `Mesh` produce the same wire effect today; the Scope variant
/// is preserved for the future case of a secondary broadcasting
/// only-to-non-primary peers (which would override the default
/// impl with a per-impl `outgoing.iter().filter(|id| id !=
/// primary_holder)` walk).
#[tokio::test]
async fn send_address_broadcast_all_secondaries_fans_out() {
    use dynrunner_protocol_primary_secondary::{Address, Scope};

    let ids = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    let mut transports = peer_mesh::<SendTestId>(&ids);

    transports[0]
        .send(Address::Broadcast(Scope::AllSecondaries), keepalive("a"))
        .await
        .unwrap();

    // Same delivery pattern as `Scope::Mesh`: peer 0 keeps nothing,
    // peers 1 and 2 both received.
    assert!(transports[0].try_recv_peer().is_none());
    assert!(transports[1].try_recv_peer().is_some());
    assert!(transports[2].try_recv_peer().is_some());
}
