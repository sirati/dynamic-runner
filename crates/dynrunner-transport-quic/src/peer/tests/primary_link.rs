//! Tests for the directed bootstrap-primary mesh link
//! (`PeerNetwork::register_primary_link`).
//!
//! The secondary registers its dialed primary connection into the mesh
//! so the primary is a routable mesh member from its side. These tests
//! pin the directed-only contract: `send_to_peer(primary)` /
//! `has_peer(primary)` resolve over the registered writer, while the
//! primary is EXCLUDED from the `broadcast` fan-out and from the
//! `peer_count` mesh-health cardinality (so the registration does not
//! prematurely change broadcast topology or mesh-watchdog behaviour).
//!
//! The "writer" is a plain channel sender — the registration API takes
//! a `mpsc::UnboundedSender<DistributedMessage<I>>` (in production the
//! fan-in handle from `NetworkClient::mesh_writer`), so no real wire is
//! needed to exercise the routing/exclusion logic.

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

/// `send_to_peer(primary)` routes the frame over the registered primary
/// writer (direct, no relay), and `has_peer(primary)` is true.
#[tokio::test(flavor = "current_thread")]
async fn send_to_primary_routes_over_registered_link() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut net: PeerNetwork<TestId> = PeerNetwork::start("sec-0").await.unwrap();
            let (primary_tx, mut primary_rx) = mpsc::unbounded_channel();
            net.register_primary_link("primary".to_string(), primary_tx);

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

/// The primary is a DIRECTED-only member: a mesh `broadcast` does NOT
/// fan out to it (preserving the bootstrap behaviour where the
/// secondary's broadcast reaches peers only), and `peer_count` excludes
/// it (so the mesh-watchdog/MeshReady count is not inflated).
#[tokio::test(flavor = "current_thread")]
async fn primary_link_excluded_from_broadcast_and_peer_count() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut net: PeerNetwork<TestId> = PeerNetwork::start("sec-0").await.unwrap();

            // A real peer (registered the ordinary way) AND the
            // directed primary link.
            let (peer_tx, mut peer_rx) = mpsc::unbounded_channel();
            net.connections.insert("sec-1".to_string(), peer_tx);
            let (primary_tx, mut primary_rx) = mpsc::unbounded_channel();
            net.register_primary_link("primary".to_string(), primary_tx);

            // peer_count reports the real peer only — the primary link
            // is excluded so a firewalled fleet isn't reported as
            // "mesh-formed" just because the primary is registered.
            assert_eq!(
                PeerTransport::<TestId>::peer_count(&net),
                1,
                "peer_count must exclude the directed primary link",
            );

            net.broadcast(keepalive("sec-0")).await.unwrap();

            // The real peer received the broadcast; the primary did NOT.
            assert!(
                matches!(peer_rx.try_recv(), Ok(DistributedMessage::Keepalive { .. })),
                "a real peer must receive the mesh broadcast",
            );
            assert!(
                primary_rx.try_recv().is_err(),
                "the primary must NOT receive the mesh broadcast (directed-only member)",
            );
        })
        .await;
}
