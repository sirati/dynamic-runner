//! Accept-loop resilience — the run_20260611_200548 observer-reconnect
//! wedge, transport half.
//!
//! Production shape: a long-lived node's reconnect machinery rebuilt
//! its tunnels endlessly while NOT ONE leg re-seated for 90+ minutes —
//! yet a FRESH process on the same machine connected to the same peers
//! within seconds. The tunnels were never the problem: the ACCEPTOR's
//! listener had been killed (or wedged) by a single bad inbound
//! connection, so no amount of redialing above it could ever land.
//!
//! Two listener-level failure shapes produce that wedge, and a mass
//! tunnel collapse produces both in numbers:
//!
//! - **aborted handshake** — a TCP connect that dies before/during the
//!   WebSocket upgrade (a collapsing ssh forward, a force-rebuilt
//!   tunnel RST-ing its in-flight dial, a port probe). The listener's
//!   inline handshake surfaces it as an accept `Err`, and a
//!   break-on-error accept loop then exits FOREVER — every later
//!   redial gets a TCP refusal.
//! - **stalled handshake** — a TCP connect that completes and then
//!   blackholes (a half-dead forward). An inline handshake with no
//!   timeout parks the accept loop on that one connection FOREVER —
//!   later redials connect at the kernel backlog and hang unanswered.
//!
//! These tests replay both shapes (plus the QUIC TLS-failure analog of
//! the first) against a live `PeerNetwork` acceptor and pin the
//! contract a reconnecting mesh needs: ONE bad inbound connection must
//! never kill or wedge the listener — the next legitimate (re)dial
//! still seats and frames flow.

use std::time::Duration;

use tokio::io::AsyncWriteExt;

use super::super::PeerNetwork;
use super::TestId;
use super::alloc_dual_free_port;
use super::log_capture::{CaptureLayer, CapturedEvent};
use dynrunner_protocol_primary_secondary::{
    DistributedMessage, KeepaliveRole, PeerConnectionInfo, PeerTransport,
};

/// Roster entry for `id` at `127.0.0.1:<port>`. An empty `cert` makes
/// the dialer skip the QUIC race and go straight to WSS (the
/// production gateway/tunnel dial shape).
fn roster_entry(id: &str, cert: &str, port: u16) -> PeerConnectionInfo {
    PeerConnectionInfo {
        secondary_id: id.into(),
        cert: cert.into(),
        ipv4: Some("127.0.0.1".into()),
        ipv6: None,
        port,
        is_observer: false,
        liveness_port: None,
    }
}

/// Drive peer-a's reconnect ticks + identifying broadcasts until the
/// a↔b leg is live in BOTH directions (the formation_retry heal pump),
/// panicking with `what` after 15s.
async fn pump_until_leg_live(
    peer_a: &mut PeerNetwork<TestId>,
    peer_b: &mut PeerNetwork<TestId>,
    what: &str,
) {
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    let mut ka_ts = 1.0f64;
    loop {
        peer_a
            .reconnect_tick_tx_for_test
            .send(())
            .expect("tick channel open");
        let _ = tokio::time::timeout(Duration::from_millis(50), peer_a.recv_peer()).await;
        // Broadcast drains completed dial registrations and writes the
        // identifying first frame on every live wire (production: the
        // keepalive / anti-entropy cadence).
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
        if peer_a.connections.contains_key("peer-b") && peer_b.connections.contains_key("peer-a") {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{what}: the leg did not (re)establish within 15s; \
             a_has_b={} b_has_a={}",
            peer_a.connections.contains_key("peer-b"),
            peer_b.connections.contains_key("peer-a"),
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// THE replay (specimen-2 single-leg shape): an ESTABLISHED leg dies;
/// during the outage the acceptor's WSS listener sees one aborted
/// mid-handshake connection (what a collapsing/force-rebuilt tunnel
/// produces); the dialer's 5s reconnect ticker then redials the SAME
/// endpoint. The rebuilt leg must re-seat and frames must flow — the
/// one bad connection must not have killed the listener.
#[tokio::test(flavor = "current_thread")]
async fn rebuilt_leg_reseats_after_acceptor_saw_aborted_handshake() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut peer_a: PeerNetwork<TestId> =
                PeerNetwork::start("peer-a", None).await.unwrap();
            let port = alloc_dual_free_port();
            let mut peer_b: PeerNetwork<TestId> =
                PeerNetwork::start("peer-b", Some(port)).await.unwrap();

            // Cert-less roster: WSS-only dials (the tunnel/gateway shape).
            let roster = vec![
                roster_entry("peer-a", "", peer_a.port()),
                roster_entry("peer-b", "", port),
            ];
            peer_a.connect_to_peers(&roster);
            pump_until_leg_live(&mut peer_a, &mut peer_b, "initial formation").await;

            // Kill the leg (both sides — the established wire is gone;
            // dropping the outgoing_tx ends the writer task, which tears
            // the socket down under the reader).
            assert!(peer_a.connections.remove("peer-b").is_some());
            assert!(peer_b.connections.remove("peer-a").is_some());

            // POISON: one aborted mid-handshake connection at the
            // acceptor — garbage bytes instead of a WebSocket upgrade,
            // then a hard close.
            {
                let mut raw = tokio::net::TcpStream::connect(("127.0.0.1", port))
                    .await
                    .expect("poison TCP connect");
                let _ = raw.write_all(b"\x16\x03\x01 not a websocket upgrade\r\n\r\n").await;
                let _ = raw.shutdown().await;
            }
            // Let the acceptor's accept loop observe the aborted
            // connection before the redial fires.
            tokio::time::sleep(Duration::from_millis(200)).await;

            // The reconnect machinery rebuilds the leg: ticker redials
            // the SAME endpoint; the rebuilt leg must carry frames.
            pump_until_leg_live(&mut peer_a, &mut peer_b, "post-poison reconnect").await;

            // Frames flow on the rebuilt leg: a directed send lands.
            peer_a
                .send_to_peer(
                    "peer-b",
                    DistributedMessage::Keepalive {
                        target: None,
                        sender_id: "peer-a".into(),
                        timestamp: 99.0,
                        secondary_id: "peer-a".into(),
                        active_workers: 7,
                        emitter_role: KeepaliveRole::Secondary,
                    },
                )
                .await
                .expect("directed send over the rebuilt leg");
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            loop {
                if let Ok(Some(DistributedMessage::Keepalive { active_workers, .. })) =
                    tokio::time::timeout(Duration::from_millis(200), peer_b.recv_peer()).await
                    && active_workers == 7
                {
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "the directed frame must arrive over the rebuilt leg"
                );
            }
        })
        .await;
}

/// The wedge shape: a TCP connect that completes and then BLACKHOLES
/// (sends no handshake bytes, stays open — the half-dead forward). The
/// listener must keep accepting other connections while that one
/// stalls; a legitimate dial seats normally.
#[tokio::test(flavor = "current_thread")]
async fn accept_loop_not_wedged_by_stalled_handshake() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut peer_a: PeerNetwork<TestId> =
                PeerNetwork::start("peer-a", None).await.unwrap();
            let port = alloc_dual_free_port();
            let mut peer_b: PeerNetwork<TestId> =
                PeerNetwork::start("peer-b", Some(port)).await.unwrap();

            // The stalled connection: connected, silent, HELD OPEN for
            // the whole test.
            let _stalled = tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .expect("stall TCP connect");
            tokio::time::sleep(Duration::from_millis(100)).await;

            let roster = vec![
                roster_entry("peer-a", "", peer_a.port()),
                roster_entry("peer-b", "", port),
            ];
            peer_a.connect_to_peers(&roster);
            pump_until_leg_live(&mut peer_a, &mut peer_b, "formation behind a stalled handshake")
                .await;
        })
        .await;
}

/// The QUIC analog of the aborted handshake: a dialer whose TLS trust
/// is wrong aborts the QUIC handshake mid-flight; the acceptor's QUIC
/// listener must survive it, so the next legitimate dial still
/// connects VIA QUIC (a WSS fallback would mask a dead QUIC loop —
/// the assertion pins the transport line in the log).
#[tokio::test(flavor = "current_thread")]
async fn quic_accept_survives_failed_tls_handshake() {
    let records: std::sync::Arc<std::sync::Mutex<Vec<CapturedEvent>>> =
        std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let layer = CaptureLayer {
        records: records.clone(),
    };
    let subscriber =
        tracing_subscriber::layer::SubscriberExt::with(tracing_subscriber::Registry::default(), layer);
    let _guard = tracing::subscriber::set_default(subscriber);

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut peer_a: PeerNetwork<TestId> =
                PeerNetwork::start("peer-a", None).await.unwrap();
            let port = alloc_dual_free_port();
            let mut peer_b: PeerNetwork<TestId> =
                PeerNetwork::start("peer-b", Some(port)).await.unwrap();
            let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

            // POISON: a QUIC dial that trusts the WRONG cert. The client
            // aborts the TLS handshake; the acceptor observes a failed
            // incoming handshake.
            let wrong = crate::certs::CertPair::generate("peer-b").expect("wrong cert");
            let poison = crate::transport::connect(addr, "peer-b", &wrong.cert_der).await;
            assert!(
                poison.is_err(),
                "the wrong-trust dial must fail (it is the poison, not a leg)"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;

            // A legitimate QUIC dial (correct cert) must still seat.
            let roster = vec![
                roster_entry("peer-a", peer_a.cert_pem(), peer_a.port()),
                roster_entry("peer-b", peer_b.cert_pem(), port),
            ];
            peer_a.connect_to_peers(&roster);
            pump_until_leg_live(&mut peer_a, &mut peer_b, "QUIC formation after failed handshake")
                .await;

            // The leg must have seated over QUIC — a WSS fallback here
            // would mean the QUIC accept loop died on the poison.
            let captured = records.lock().unwrap();
            assert!(
                captured
                    .iter()
                    .any(|e| e.message.contains("connected to peer via QUIC")),
                "the post-poison dial must connect via QUIC (a WSS fallback \
                 masks a dead QUIC accept loop); captured: {captured:#?}"
            );
        })
        .await;
}
