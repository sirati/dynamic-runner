//! Ingest-edge clock recording over a REAL wire (run_20260611_115429):
//! the arrival clock must be stamped by the connection read loops —
//! independently of anything draining `recv_peer` — and the drained
//! clock only when `recv_peer` actually pulls the frame. Built on the
//! `two_peers` localhost fixture.

use std::time::Duration;

use super::super::PeerNetwork;
use super::TestId;
use dynrunner_protocol_primary_secondary::{
    DistributedMessage, KeepaliveRole, PeerConnectionInfo, PeerTransport,
};

fn keepalive(sender: &str) -> DistributedMessage<TestId> {
    DistributedMessage::Keepalive {
        target: None,
        sender_id: sender.into(),
        timestamp: 1.0,
        secondary_id: sender.into(),
        active_workers: 2,
        emitter_role: KeepaliveRole::Secondary,
    }
}

/// The starved-consumer face, on a real localhost QUIC/WSS pair: peer-a
/// sends while NOTHING drives peer-b's `recv_peer`. The frame must
/// still stamp peer-b's ARRIVAL clock (the accept-side read loop runs
/// as its own task — this is exactly the honesty that survives a
/// starved mesh pump), while the DRAINED clock stays empty until
/// `recv_peer` is finally driven. Also pins the unidentified-window
/// boundary: before any complete frame from peer-a decoded, NEITHER
/// clock knows the peer.
#[tokio::test(flavor = "current_thread")]
async fn read_loop_stamps_arrival_without_recv_peer() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Start two peer networks
            let mut peer_a: PeerNetwork<TestId> = PeerNetwork::start("peer-a", None).await.unwrap();
            let mut peer_b: PeerNetwork<TestId> = PeerNetwork::start("peer-b", None).await.unwrap();

            let port_a = peer_a.port();
            let port_b = peer_b.port();
            let cert_pem_a = peer_a.cert_pem().to_string();
            let cert_pem_b = peer_b.cert_pem().to_string();

            // Create peer info for both
            let peers = vec![
                PeerConnectionInfo {
                    secondary_id: "peer-a".into(),
                    cert: cert_pem_a,
                    ipv4: Some("127.0.0.1".into()),
                    ipv6: None,
                    port: port_a,
                    is_observer: false,
                    liveness_port: None,
                },
                PeerConnectionInfo {
                    secondary_id: "peer-b".into(),
                    cert: cert_pem_b,
                    ipv4: Some("127.0.0.1".into()),
                    ipv6: None,
                    port: port_b,
                    is_observer: false,
                    liveness_port: None,
                },
            ];

            let edges_b = PeerTransport::<TestId>::ingest_edges(&peer_b)
                .expect("PeerNetwork publishes ingest-edge clocks");

            // Each peer kicks off outgoing dials. Non-blocking — the
            // actual handshakes run on spawned tasks; the sleep below
            // gives them time to complete before we drain.
            peer_a.connect_to_peers(&peers);
            peer_b.connect_to_peers(&peers);

            // Give accept loops time to register incoming connections.
            tokio::time::sleep(Duration::from_millis(100)).await;
            peer_a.drain_new_connections();
            peer_b.drain_new_connections();

            // Unidentified window: no complete frame from peer-a has
            // decoded at peer-b yet (connection-level handshakes don't
            // count) — neither clock may know the peer.
            assert_eq!(
                edges_b.arrival.last_seen("peer-a"),
                None,
                "attribution exists only from the first DECODED frame"
            );

            // Peer A sends; peer B's recv_peer is NEVER driven in this
            // window — the production starved-pump shape.
            peer_a.broadcast(keepalive("peer-a")).await.unwrap();

            // The accept-side read loop runs as its own spawned task on
            // this LocalSet: poll (yielding, never calling recv_peer)
            // until it has decoded + stamped the arrival.
            let mut arrived = None;
            for _ in 0..200 {
                tokio::time::sleep(Duration::from_millis(10)).await;
                arrived = edges_b.arrival.last_seen("peer-a");
                if arrived.is_some() {
                    break;
                }
            }
            assert!(
                arrived.is_some(),
                "the connection read loop stamps the ARRIVAL clock without \
                 anyone driving recv_peer — the honesty that survives a \
                 starved mesh pump"
            );
            assert_eq!(
                edges_b.drained.last_seen("peer-a"),
                None,
                "nothing pulled the frame out yet: the DRAINED edge must \
                 not be stamped before recv_peer runs"
            );

            // Now drive recv_peer once: the frame surfaces and the
            // drained edge is stamped with the same peer key.
            let received = tokio::time::timeout(Duration::from_secs(5), peer_b.recv_peer())
                .await
                .expect("timeout waiting for peer message")
                .expect("no message received");
            assert_eq!(received.sender_id(), "peer-a");
            assert!(
                edges_b.drained.last_seen("peer-a").is_some(),
                "recv_peer stamps the DRAINED edge as it pulls the frame"
            );
        })
        .await;
}
