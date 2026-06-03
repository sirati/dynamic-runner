//! `EitherPeerTransport` / `NoPeerTransport` parity tests. These pin
//! the "disabled overlay" code path: a `NoPeerTransport` and a
//! `Disabled` variant of `EitherPeerTransport` must behave
//! identically (recv pending, send/connect no-op). The third test
//! exercises the `Real` variant end-to-end mirror of
//! `two_peers_exchange_messages`.

use std::time::Duration;

use super::super::{EitherPeerTransport, NoPeerTransport, PeerNetwork};
use super::TestId;
use dynrunner_protocol_primary_secondary::{DistributedMessage, PeerConnectionInfo, PeerTransport};

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

#[tokio::test(flavor = "current_thread")]
async fn either_peer_transport_disabled_acts_like_no_peer() {
    // `EitherPeerTransport::Disabled` must behave identically to a
    // bare `NoPeerTransport`: zero peers, broadcasts succeed silently,
    // try_recv_peer returns None. This is the contract the secondary
    // relies on when `--disable-peer-overlay` is set.
    let mut either: EitherPeerTransport<TestId> = EitherPeerTransport::Disabled(NoPeerTransport);

    either
        .broadcast(DistributedMessage::Keepalive {
            sender_id: "x".into(),
            timestamp: 0.0,
            secondary_id: "x".into(),
            active_workers: 0,
        })
        .await
        .unwrap();
    assert_eq!(PeerTransport::<TestId>::peer_count(&either), 0);
    assert!(PeerTransport::<TestId>::try_recv_peer(&mut either).is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn either_peer_transport_real_round_trips_a_message() {
    // Mirror `two_peers_exchange_messages` but route everything through
    // `EitherPeerTransport::Real(...)` to prove the enum doesn't drop
    // the active variant's behavior.
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let pn_a: PeerNetwork<TestId> = PeerNetwork::start("peer-a").await.unwrap();
            let pn_b: PeerNetwork<TestId> = PeerNetwork::start("peer-b").await.unwrap();

            let port_a = pn_a.port();
            let port_b = pn_b.port();
            let cert_pem_a = pn_a.cert_pem().to_string();
            let cert_pem_b = pn_b.cert_pem().to_string();

            let mut peer_a: EitherPeerTransport<TestId> = EitherPeerTransport::Real(Box::new(pn_a));
            let mut peer_b: EitherPeerTransport<TestId> = EitherPeerTransport::Real(Box::new(pn_b));

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

            // Per-peer dials run as spawned tasks; the sleep gives them
            // time to land before we broadcast.
            peer_a.connect_to_peers(&peers).await;
            peer_b.connect_to_peers(&peers).await;
            tokio::time::sleep(Duration::from_millis(100)).await;

            let msg: DistributedMessage<TestId> = DistributedMessage::Keepalive {
                sender_id: "peer-a".into(),
                timestamp: 1.0,
                secondary_id: "peer-a".into(),
                active_workers: 2,
            };
            peer_a.broadcast(msg).await.unwrap();

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
