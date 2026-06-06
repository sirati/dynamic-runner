//! `EitherPeerTransport` / `NoPeerTransport` parity tests. These pin
//! the "disabled overlay" code path: a `NoPeerTransport` and a
//! `Disabled` variant of `EitherPeerTransport` must behave
//! identically (recv pending, send/connect no-op). A further test
//! exercises the `Real` variant end-to-end mirror of
//! `two_peers_exchange_messages`, and the last pins the no-mesh
//! (firewalled) primary-routing path: once the bootstrap wire is folded
//! into the `Disabled` arm, the primary is the sole reachable member.

use std::time::Duration;

use super::super::{EitherPeerTransport, NoPeerTransport, PeerNetwork};
use super::TestId;
use dynrunner_protocol_primary_secondary::{
    DistributedMessage, KeepaliveRole, PeerConnectionInfo, PeerId, PeerTransport,
};
use tokio::sync::mpsc;

#[tokio::test(flavor = "current_thread")]
async fn no_peer_transport_never_receives() {
    let mut noop = NoPeerTransport;
    noop.broadcast(DistributedMessage::<TestId>::Keepalive {
        target: None,
        sender_id: "x".into(),
        timestamp: 0.0,
        secondary_id: "x".into(),
        active_workers: 0,
        emitter_role: KeepaliveRole::Secondary,
    })
    .await
    .unwrap();
    assert_eq!(PeerTransport::<TestId>::peer_count(&noop), 0);
    assert!(PeerTransport::<TestId>::try_recv_peer(&mut noop).is_none());
    // No id is ever a member of the no-op overlay — `has_peer` is a
    // constant `false`, consistent with `peer_count == 0`.
    assert!(!PeerTransport::<TestId>::has_peer(
        &noop,
        &PeerId::from("x")
    ));
    assert!(!PeerTransport::<TestId>::has_peer(
        &noop,
        &PeerId::from("anyone")
    ));
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
            target: None,
            sender_id: "x".into(),
            timestamp: 0.0,
            secondary_id: "x".into(),
            active_workers: 0,
            emitter_role: KeepaliveRole::Secondary,
        })
        .await
        .unwrap();
    assert_eq!(PeerTransport::<TestId>::peer_count(&either), 0);
    assert!(PeerTransport::<TestId>::try_recv_peer(&mut either).is_none());
    // `has_peer` delegates to the disabled arm: always `false`, matching
    // the bare `NoPeerTransport`.
    assert!(!PeerTransport::<TestId>::has_peer(
        &either,
        &PeerId::from("x")
    ));
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

            // Before any dial, the real mesh has no peers — `has_peer`
            // reflects the empty connection table.
            assert!(!PeerTransport::<TestId>::has_peer(
                &peer_a,
                &PeerId::from("peer-b")
            ));

            // Per-peer dials run as spawned tasks; the sleep gives them
            // time to land before we broadcast.
            peer_a.connect_to_peers(&peers).await;
            peer_b.connect_to_peers(&peers).await;
            tokio::time::sleep(Duration::from_millis(100)).await;

            let msg: DistributedMessage<TestId> = DistributedMessage::Keepalive {
                target: None,
                sender_id: "peer-a".into(),
                timestamp: 1.0,
                secondary_id: "peer-a".into(),
                active_workers: 2,
                emitter_role: KeepaliveRole::Secondary,
            };
            peer_a.broadcast(msg).await.unwrap();

            // `broadcast` drained the dialed connection into the table,
            // so `has_peer` now flips false → true against the live QUIC
            // connection table — the per-id companion to the
            // `peer_count` the disabled-arm tests pin at 0.
            assert!(
                PeerTransport::<TestId>::has_peer(&peer_a, &PeerId::from("peer-b")),
                "peer-b must be a member of peer-a's mesh once the dial lands"
            );

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
async fn disabled_with_primary_routes_only_to_the_folded_primary() {
    // A firewalled fleet has no peer mesh, but the bootstrap primary
    // wire is folded into the `Disabled` arm so the primary is the SOLE
    // reachable member. Pin the directed-only routing / exclusion
    // contract (mirrors `primary_link.rs` for the `Real` arm), wire-free.
    let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel();
    let (incoming_tx, incoming_rx) = mpsc::unbounded_channel();
    let mut either: EitherPeerTransport<TestId> = EitherPeerTransport::disabled_with_staged_primary(
        "primary".to_string(),
        outbound_tx,
        incoming_rx,
    );

    // The primary is a reachable member; no other id is.
    assert!(PeerTransport::<TestId>::has_peer(
        &either,
        &PeerId::from("primary")
    ));
    assert!(!PeerTransport::<TestId>::has_peer(
        &either,
        &PeerId::from("sec-1")
    ));

    // Role-blind cardinality: the folded primary is a member, so it is
    // counted (`1`), exactly as the `Real` arm counts the primary folded
    // into its `connections`. The role-aware "how many alive secondaries"
    // policy is the coordinator edge's `alive_secondary_count()` over
    // global state, not the transport's.
    assert_eq!(PeerTransport::<TestId>::peer_count(&either), 1);

    // `send_to_peer(primary)` routes over the folded outbound wire.
    either
        .send_to_peer(
            "primary",
            DistributedMessage::Keepalive {
                target: None,
                sender_id: "sec-0".into(),
                timestamp: 1.0,
                secondary_id: "sec-0".into(),
                active_workers: 7,
                emitter_role: KeepaliveRole::Secondary,
            },
        )
        .await
        .expect("send to the folded primary must succeed");
    match outbound_rx
        .try_recv()
        .expect("primary must receive the send")
    {
        DistributedMessage::Keepalive { active_workers, .. } => assert_eq!(active_workers, 7),
        _ => panic!("expected Keepalive"),
    }

    // Any non-primary id is a no-route error (no mesh to fall back on).
    assert!(
        either
            .send_to_peer(
                "sec-1",
                DistributedMessage::Keepalive {
                    target: None,
                    sender_id: "sec-0".into(),
                    timestamp: 1.0,
                    secondary_id: "sec-0".into(),
                    active_workers: 0,
                    emitter_role: KeepaliveRole::Secondary,
                },
            )
            .await
            .is_err(),
        "a firewalled fleet has no route to a non-primary peer",
    );

    // `broadcast` (`Destination::All`) reaches the folded primary — the
    // sole reachable member — EXACTLY ONCE, mirroring the `Real` arm
    // where the folded primary is a plain `connections` entry the
    // fan-out hits. This is what makes a firewalled secondary's keepalive
    // reach the primary; a no-op here would starve it and trip false
    // primary-death.
    either
        .broadcast(DistributedMessage::Keepalive {
            target: None,
            sender_id: "sec-0".into(),
            timestamp: 1.0,
            secondary_id: "sec-0".into(),
            active_workers: 5,
            emitter_role: KeepaliveRole::Secondary,
        })
        .await
        .unwrap();
    match outbound_rx
        .try_recv()
        .expect("the folded primary MUST receive the mesh broadcast (sole member)")
    {
        DistributedMessage::Keepalive { active_workers, .. } => assert_eq!(active_workers, 5),
        _ => panic!("expected Keepalive"),
    }
    // EXACTLY once — no second copy on the wire (no double-fan-out).
    assert!(
        outbound_rx.try_recv().is_err(),
        "a single broadcast must deliver to the folded primary exactly once",
    );

    // Inbound: a primary frame arriving on the folded wire surfaces via
    // `recv_peer` like any other peer's.
    incoming_tx
        .send(DistributedMessage::Keepalive {
            target: None,
            sender_id: "primary".into(),
            timestamp: 2.0,
            secondary_id: "primary".into(),
            active_workers: 3,
            emitter_role: KeepaliveRole::Primary,
        })
        .unwrap();
    let received = tokio::time::timeout(Duration::from_secs(1), either.recv_peer())
        .await
        .expect("timeout waiting for inbound primary frame")
        .expect("inbound primary frame must surface");
    assert_eq!(received.sender_id(), "primary");
}
