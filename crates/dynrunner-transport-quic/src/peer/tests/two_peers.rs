//! Basic peer-exchange + tie-break tests. `two_peers_exchange_messages`
//! pins the round-trip wire shape; `higher_id_does_not_dial_lower_id`
//! pins the dial-direction lexicographic tie-break that prevents the
//! "all peers disconnected during broadcast" duplicate-connection
//! cascade observed pre-fix.

use std::time::Duration;

use super::super::PeerNetwork;
use super::TestId;
use dynrunner_protocol_primary_secondary::{DistributedMessage, PeerConnectionInfo, PeerTransport};

#[tokio::test(flavor = "current_thread")]
async fn two_peers_exchange_messages() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Start two peer networks
            let mut peer_a: PeerNetwork<TestId> = PeerNetwork::start("peer-a").await.unwrap();
            let mut peer_b: PeerNetwork<TestId> = PeerNetwork::start("peer-b").await.unwrap();

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
                },
                PeerConnectionInfo {
                    secondary_id: "peer-b".into(),
                    cert: cert_pem_b,
                    ipv4: Some("127.0.0.1".into()),
                    ipv6: None,
                    port: port_b,
                    is_observer: false,
                },
            ];

            // Each peer kicks off outgoing dials. Non-blocking — the
            // actual handshakes run on spawned tasks; the sleep below
            // gives them time to complete before we drain.
            peer_a.connect_to_peers(&peers);
            peer_b.connect_to_peers(&peers);

            // Give accept loops time to register incoming connections
            tokio::time::sleep(Duration::from_millis(100)).await;
            peer_a.drain_new_connections();
            peer_b.drain_new_connections();

            // Peer A broadcasts a message
            let msg: DistributedMessage<TestId> = DistributedMessage::Keepalive {
                sender_id: "peer-a".into(),
                timestamp: 1.0,
                secondary_id: "peer-a".into(),
                active_workers: 2,
            };
            peer_a.broadcast(msg).await.unwrap();

            // Peer B should receive it
            let received = tokio::time::timeout(Duration::from_secs(5), peer_b.recv_peer())
                .await
                .expect("timeout waiting for peer message")
                .expect("no message received");

            assert_eq!(received.sender_id(), "peer-a");
            match received {
                DistributedMessage::Keepalive { active_workers, .. } => {
                    assert_eq!(active_workers, 2);
                }
                _ => panic!("expected Keepalive"),
            }
        })
        .await;
}

/// Lower-id-dials: only the lexicographically-lower peer initiates
/// the connection; the higher-id peer relies on its accept loop. This
/// test exercises the asymmetry by having a HIGHER-id peer call
/// `connect_to_peers` first — it must NOT dial, and the connection
/// must still establish via the LOWER-id peer's later dial. Pre-fix
/// both peers dialed concurrently, leaving the duplicate connection
/// (and the resulting drop-tear-down cascade) as the root cause of
/// the "all peers disconnected during broadcast" bug both consumers
/// hit on Krater.
#[tokio::test(flavor = "current_thread")]
async fn higher_id_does_not_dial_lower_id() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut peer_low: PeerNetwork<TestId> = PeerNetwork::start("peer-a").await.unwrap();
            let mut peer_high: PeerNetwork<TestId> = PeerNetwork::start("peer-b").await.unwrap();
            let port_low = peer_low.port();
            let port_high = peer_high.port();
            let cert_low = peer_low.cert_pem().to_string();
            let cert_high = peer_high.cert_pem().to_string();

            let peers = vec![
                PeerConnectionInfo {
                    secondary_id: "peer-a".into(),
                    cert: cert_low,
                    ipv4: Some("127.0.0.1".into()),
                    ipv6: None,
                    port: port_low,
                    is_observer: false,
                },
                PeerConnectionInfo {
                    secondary_id: "peer-b".into(),
                    cert: cert_high,
                    ipv4: Some("127.0.0.1".into()),
                    ipv6: None,
                    port: port_high,
                    is_observer: false,
                },
            ];

            // Higher-id peer attempts connect_to_peers FIRST. The
            // skip-on-higher-id rule must keep it from dialing
            // peer-a, otherwise both sides would race.
            peer_high.connect_to_peers(&peers);
            // Brief pause so any (incorrect) dial-spawn would have
            // a chance to land before peer-a starts its dial.
            tokio::time::sleep(Duration::from_millis(50)).await;
            peer_low.connect_to_peers(&peers);

            // Settle the dial; peer-low's connect-side AcceptedPeer
            // (no message exchange needed) lands in its own
            // new_conn_rx and gets drained immediately. peer-high's
            // accept-side blocks on the first incoming message — its
            // AcceptedPeer doesn't surface until peer-low actually
            // sends data over the WSS pipe. So we have to broadcast
            // before peer-high's peer_count can reflect the
            // accepted connection.
            tokio::time::sleep(Duration::from_millis(100)).await;
            peer_low.drain_new_connections();
            assert_eq!(
                PeerTransport::<TestId>::peer_count(&peer_low),
                1,
                "lower-id peer should have one connection (its outbound dial)"
            );

            // Broadcast triggers peer-high's accept-side to read the
            // first message, identify peer-low, and queue its
            // AcceptedPeer. recv_peer's internal drain then
            // populates peer-high's connections.
            let msg: DistributedMessage<TestId> = DistributedMessage::Keepalive {
                sender_id: "peer-a".into(),
                timestamp: 1.0,
                secondary_id: "peer-a".into(),
                active_workers: 2,
            };
            peer_low.broadcast(msg).await.unwrap();
            let received = tokio::time::timeout(Duration::from_secs(5), peer_high.recv_peer())
                .await
                .expect("timeout waiting for peer message")
                .expect("no message received");
            assert_eq!(received.sender_id(), "peer-a");

            assert_eq!(
                PeerTransport::<TestId>::peer_count(&peer_high),
                1,
                "higher-id peer should have one connection (accepted from lower-id)"
            );
            // No "peer disconnected during broadcast" warns: the
            // single-WSS topology has nothing to tear down. peer_low
            // still sees its single connection after broadcasting.
            assert_eq!(
                PeerTransport::<TestId>::peer_count(&peer_low),
                1,
                "lower-id peer's connection survived the broadcast"
            );
        })
        .await;
}
