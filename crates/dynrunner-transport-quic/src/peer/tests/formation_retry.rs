//! Mesh-FORMATION retry: a leg whose INITIAL dial never landed (the
//! peer was unreachable during a startup-load window — the
//! run_20260611_200548 shape) must keep being re-dialed by the
//! reconnect ticker and establish the moment the peer becomes
//! reachable, WITHOUT any further membership event (`connect_to_peers`
//! is called exactly once, off the one `PeerInfo` sweep).
//!
//! This pins the transport half of the formation-retry contract: the
//! reconnect-tick reconciliation (`process_reconnect_tick`) tracks ANY
//! roster peer without a live `connections` entry — never-formed legs
//! included, not just established-then-died ones — so failed initial
//! formation feeds the SAME continuous heal path leg-death redials
//! use. There is exactly ONE owner of "this leg should exist but
//! doesn't, keep trying": the tracker reconciliation against
//! `peer_dial_info`.

use std::time::Duration;

use super::super::PeerNetwork;
use super::TestId;
use super::alloc_dual_free_port;
use dynrunner_protocol_primary_secondary::{
    DistributedMessage, KeepaliveRole, PeerConnectionInfo, PeerTransport,
};

/// All initial dials fail (the peer's advertised port has no listener —
/// unreachable under startup load); the reconnect ticker keeps retrying;
/// when the peer finally binds its advertised port the leg establishes
/// in BOTH directions — driven by ticks alone, no second
/// `connect_to_peers`, no promotion, no membership change.
#[tokio::test(flavor = "current_thread")]
async fn never_formed_leg_establishes_once_peer_becomes_reachable() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut peer_a: PeerNetwork<TestId> =
                PeerNetwork::start("peer-a", None).await.unwrap();
            // Reserve the port peer-b will eventually bind. Nothing
            // listens on it yet — every dial toward it fails (the
            // QUIC race is skipped on the cert-less entry; the WSS
            // race gets a fast TCP refusal).
            let port = alloc_dual_free_port();
            let roster = vec![
                PeerConnectionInfo {
                    secondary_id: "peer-a".into(),
                    cert: peer_a.cert_pem().to_string(),
                    ipv4: Some("127.0.0.1".into()),
                    ipv6: None,
                    port: peer_a.port(),
                    is_observer: false,
                    liveness_port: None,
                    slurm_job_id: None,
                },
                PeerConnectionInfo {
                    secondary_id: "peer-b".into(),
                    // Cert-less entry: the dial goes straight to WSS,
                    // which fails fast (TCP refused) while peer-b is
                    // down and connects fast once it is up.
                    cert: String::new(),
                    ipv4: Some("127.0.0.1".into()),
                    ipv6: None,
                    port,
                    is_observer: false,
                    liveness_port: None,
                    slurm_job_id: None,
                },
            ];

            // The ONE membership event of this test: the initial dial
            // sweep fires while peer-b is unreachable. peer-a is the
            // dial owner of the a↔b leg (lower-id-dials).
            peer_a.connect_to_peers(&roster);

            // A few reconnect ticks while peer-b stays down: the
            // reconciliation must TRACK the never-formed leg (initial
            // dial failures feed the heal path) and keep it tracked
            // across failing redials.
            for _ in 0..3 {
                peer_a
                    .reconnect_tick_tx_for_test
                    .send(())
                    .expect("tick channel open");
                let _ = tokio::time::timeout(Duration::from_millis(50), peer_a.recv_peer()).await;
            }
            assert_eq!(
                peer_a.reconnect_tracker.tracked_count(),
                1,
                "a never-formed roster leg must be tracked by the reconnect \
                 reconciliation (formation failures feed the heal path)"
            );
            assert!(
                !peer_a.connections.contains_key("peer-b"),
                "sanity: the leg cannot have formed while peer-b is down"
            );

            // peer-b becomes reachable NOW, on exactly the port the
            // roster advertised.
            let mut peer_b: PeerNetwork<TestId> =
                PeerNetwork::start("peer-b", Some(port)).await.unwrap();

            // Heal loop: reconnect ticks alone must establish the leg.
            // peer-a's redial lands on peer-b's fresh WSS listener;
            // peer-a's keepalive broadcast then writes the first frame
            // that identifies the wire at peer-b's accept loop
            // (production: the keepalive/anti-entropy cadence).
            let deadline = std::time::Instant::now() + Duration::from_secs(15);
            let mut ka_ts = 1.0f64;
            loop {
                peer_a
                    .reconnect_tick_tx_for_test
                    .send(())
                    .expect("tick channel open");
                let _ = tokio::time::timeout(Duration::from_millis(50), peer_a.recv_peer()).await;
                // Broadcast drains completed dial registrations first
                // (its internal `drain_new_connections`) and then
                // writes the identifying first frame on every live
                // wire.
                ka_ts += 1.0;
                let _ = peer_a
                    .broadcast(DistributedMessage::Keepalive {
                        target: None,
                        sender_id: "peer-a".into(),
                        timestamp: ka_ts,
                        secondary_id: "peer-a".into(),
                        active_workers: 0,
                        emitter_role: KeepaliveRole::Secondary,
                    })
                    .await;
                let _ = tokio::time::timeout(Duration::from_millis(50), peer_b.recv_peer()).await;
                peer_b.drain_new_connections();
                if peer_a.connections.contains_key("peer-b")
                    && peer_b.connections.contains_key("peer-a")
                {
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "the never-formed leg did not establish within 15s of \
                     peer-b becoming reachable — formation retry abandoned \
                     the leg (the run_20260611_200548 shape); \
                     a_has_b={} b_has_a={} tracked={}",
                    peer_a.connections.contains_key("peer-b"),
                    peer_b.connections.contains_key("peer-a"),
                    peer_a.reconnect_tracker.tracked_count(),
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }

            // The heal ends the tracked outage (reconciliation clears it
            // on the next tick once the entry is live).
            peer_a
                .reconnect_tick_tx_for_test
                .send(())
                .expect("tick channel open");
            let _ = tokio::time::timeout(Duration::from_millis(50), peer_a.recv_peer()).await;
            assert_eq!(
                peer_a.reconnect_tracker.tracked_count(),
                0,
                "an established leg must leave the reconnect tracker"
            );
        })
        .await;
}
