//! Tests for the bootstrap-primary mesh link
//! (`PeerNetwork::register_primary_link`).
//!
//! The secondary folds its dialed primary bootstrap wire into the mesh
//! so the primary is a routable mesh member from its side. After the
//! de-role pass the primary is a PLAIN mesh peer — the transport keeps
//! no notion of "which connection is the primary", so a folded primary
//! is reachable via `send_to_peer(primary)` / `has_peer(primary)` and
//! is counted/broadcast-to exactly like any other connection. Any
//! "exclude the primary" mesh-health/quorum policy is now a role
//! concern resolved at the coordinator edge (the secondary's election
//! quorum reads `live_peer_ids`), NOT in the transport.
//!
//! `register_primary_link` itself consumes the whole `NetworkClient`
//! (owning BOTH directions of the bootstrap wire); the real-wire fan-in
//! is covered by `network/tests.rs::mesh_writer_fans_into_the_same_wire`.
//! Here we exercise only the routing LOGIC, which is independent of the
//! wire — so the link is staged directly into the `connections` table
//! (exactly the state `register_primary_link` installs for the outbound
//! side) with a plain channel sender, no real wire needed.

use super::super::PeerNetwork;
use super::TestId;
use dynrunner_protocol_primary_secondary::{
    DistributedMessage, KeepaliveRole, PeerId, PeerTransport,
};
use tokio::sync::mpsc;

fn keepalive(sender: &str) -> DistributedMessage<TestId> {
    DistributedMessage::Keepalive {
        sender_id: sender.into(),
        timestamp: 1.0,
        secondary_id: sender.into(),
        active_workers: 0,
        emitter_role: KeepaliveRole::Secondary,
    }
}

/// `send_to_peer(primary)` routes the frame over the folded primary
/// connection (direct, no relay), and `has_peer(primary)` is true.
#[tokio::test(flavor = "current_thread")]
async fn send_to_primary_routes_over_registered_link() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut net: PeerNetwork<TestId> = PeerNetwork::start("sec-0").await.unwrap();
            let (primary_tx, mut primary_rx) = mpsc::unbounded_channel();
            net.connections.insert("primary".to_string(), primary_tx);

            assert!(
                PeerTransport::<TestId>::has_peer(&net, &PeerId::from("primary")),
                "the registered primary must be a reachable mesh member",
            );

            net.send_to_peer("primary", keepalive("sec-0"))
                .await
                .expect("send to the registered primary link must succeed");

            let got = primary_rx
                .try_recv()
                .expect("the primary writer must have received the directed send");
            assert!(matches!(got, DistributedMessage::Keepalive { .. }));
        })
        .await;
}

/// The folded primary is a PLAIN mesh peer: it counts toward
/// `peer_count` and receives a mesh `broadcast` like any other
/// connection. The transport no longer special-cases the primary —
/// "exclude the primary" is resolved at the coordinator edge.
#[tokio::test(flavor = "current_thread")]
async fn folded_primary_is_a_plain_mesh_peer() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut net: PeerNetwork<TestId> = PeerNetwork::start("sec-0").await.unwrap();

            // A real peer (registered the ordinary way) AND the folded
            // primary link — both plain connections.
            let (peer_tx, mut peer_rx) = mpsc::unbounded_channel();
            net.connections.insert("sec-1".to_string(), peer_tx);
            let (primary_tx, mut primary_rx) = mpsc::unbounded_channel();
            net.connections.insert("primary".to_string(), primary_tx);

            // peer_count is pure cardinality: both connections count.
            assert_eq!(
                PeerTransport::<TestId>::peer_count(&net),
                2,
                "peer_count is pure membership cardinality — the primary is not special-cased",
            );

            net.broadcast(keepalive("sec-0")).await.unwrap();

            // Both the real peer AND the folded primary receive the
            // broadcast — uniform fan-out, no role exclusion.
            assert!(
                matches!(peer_rx.try_recv(), Ok(DistributedMessage::Keepalive { .. })),
                "a real peer must receive the mesh broadcast",
            );
            assert!(
                matches!(primary_rx.try_recv(), Ok(DistributedMessage::Keepalive { .. })),
                "the folded primary receives the mesh broadcast like any other peer",
            );
        })
        .await;
}
