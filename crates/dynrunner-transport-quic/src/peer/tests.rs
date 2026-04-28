use super::util::parse_cert_pem;
use super::{NoPeerTransport, PeerNetwork};
use crate::certs::CertPair;
use dynrunner_protocol_primary_secondary::{DistributedMessage, PeerConnectionInfo, PeerTransport};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct TestId(String);

#[test]
fn parse_cert_pem_works() {
    let cert = CertPair::generate("test").unwrap();
    let der = parse_cert_pem(&cert.cert_pem);
    assert!(der.is_some());
    assert_eq!(der.unwrap().as_ref(), cert.cert_der.as_ref());
}

#[test]
fn parse_cert_pem_empty_returns_none() {
    assert!(parse_cert_pem("").is_none());
    assert!(parse_cert_pem("not a cert").is_none());
}

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
                },
                PeerConnectionInfo {
                    secondary_id: "peer-b".into(),
                    cert: cert_pem_b,
                    ipv4: Some("127.0.0.1".into()),
                    ipv6: None,
                    port: port_b,
                },
            ];

            // Each peer connects to the other
            peer_a.connect_to_peers(&peers).await;
            peer_b.connect_to_peers(&peers).await;

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

#[tokio::test(flavor = "current_thread")]
async fn no_peer_transport_never_receives() {
    let mut noop = NoPeerTransport;
    noop.broadcast(DistributedMessage::<TestId>::Keepalive {
        sender_id: "x".into(),
        timestamp: 0.0,
        secondary_id: "x".into(),
        active_workers: 0,
    })
    .await
    .unwrap();
    assert_eq!(PeerTransport::<TestId>::peer_count(&noop), 0);
    assert!(PeerTransport::<TestId>::try_recv_peer(&mut noop).is_none());
}
