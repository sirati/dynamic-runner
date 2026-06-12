//! REPRO (run_20260611_202345 / #434-class): a simultaneous connection
//! reset (one gateway event RST-ing many TCP/tunnel sessions in the
//! same second) must not PERMANENTLY kill a node's ability to accept
//! re-dialed peer sessions — the report path must re-establish.
//!
//! # The production sequence being replayed
//!
//! The relocated primary's host held established mesh sessions to its
//! secondaries. At 18:39:31 a gateway event reset many sessions at
//! once: established wires died abruptly (RST, no close handshake) and
//! any connection attempt that was MID-HANDSHAKE at either listener
//! aborted in the same second. From then on, secondaries replayed
//! their confirmable task reports ("UNACKED past the ack timeout …
//! replaying with the same seq", attempts up to 18) — but the primary
//! NEVER ingested them, for 17+ minutes, while only sporadic frames
//! (the odd keepalive) got through.
//!
//! # The defect these tests pin
//!
//! Both accept loops treated EVERY `accept()` error as loop-fatal
//! (`break`) — and `WssListener::accept` / `QuicListener::accept`
//! folded PER-CONNECTION failures (a TCP connection aborted mid
//! WS-handshake, a failed QUIC TLS handshake, a peer that never opened
//! its bi stream) into that same `Err`. One aborted handshake at the
//! reset instant and the listener was dropped for the rest of the run:
//! every later re-dial — the heal the #416 accept-replace machinery
//! and the redial-request nudges depend on — had nowhere to land, so
//! the re-sent reports could never re-register a session and ingest
//! stayed dead forever. A per-connection failure must be dropped
//! per-connection; only "the listener itself is gone" may end an
//! accept loop.
//!
//! Assertions, per the incident brief:
//!   (a) the demux does not busy-spin / monopolise the executor after
//!       the reset (co-scheduled work still drains), and the errored
//!       accept path is contained — the listener survives;
//!   (b) a peer's re-sent confirmable report after re-establishment IS
//!       delivered, and the reply (ack) path over the re-registered
//!       writer works.

use std::net::SocketAddr;
use std::time::Duration;

use dynrunner_core::{MessageReceiver, MessageSender};
use dynrunner_protocol_primary_secondary::{
    DistributedMessage, KeepaliveRole, MessageType, PeerId, PeerTransport,
};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use super::super::PeerNetwork;
use super::TestId;
use crate::certs::CertPair;
use crate::wss::connect_wss;

/// Arm an abrupt-RST close on `stream`: SO_LINGER(0) makes the
/// subsequent drop send a TCP RST instead of a FIN — the shape a
/// gateway-level session reset delivers. Via `socket2::SockRef`
/// (tokio's own `set_linger` is deprecated for the blocking-drop
/// caveat, which is irrelevant for a test socket).
fn arm_rst(stream: &TcpStream) {
    socket2::SockRef::from(stream)
        .set_linger(Some(Duration::ZERO))
        .expect("set SO_LINGER(0)");
}

fn keepalive(sender: &str) -> DistributedMessage<TestId> {
    DistributedMessage::Keepalive {
        target: None,
        sender_id: sender.into(),
        timestamp: 1.0,
        secondary_id: sender.into(),
        active_workers: 0,
        emitter_role: KeepaliveRole::Secondary,
    }
}

/// A confirmable task report carrying a sticky `delivery_seq` — the
/// frame shape the secondary's buffered-report-replay re-sends with
/// the SAME seq until the authority's `TerminalAck` lands.
fn report(sender: &str, seq: u64) -> DistributedMessage<TestId> {
    DistributedMessage::TaskComplete {
        target: None,
        sender_id: sender.into(),
        timestamp: 2.0,
        secondary_id: sender.into(),
        worker_id: 0,
        task_hash: "b65fda0d4c6a671b".into(),
        result_data: None,
        delivery_seq: Some(seq),
        msgs_posted_through: None,
    }
}

/// Drive `recv_peer` with a bound; `None` when nothing was delivered
/// inside the window.
async fn recv_within(
    peer: &mut PeerNetwork<TestId>,
    window: Duration,
) -> Option<DistributedMessage<TestId>> {
    tokio::time::timeout(window, peer.recv_peer()).await.ok()?
}

/// Abort one connection attempt MID-WS-HANDSHAKE at the WSS listener,
/// the way the gateway reset aborted in-flight sessions: connect at
/// TCP, push bytes that are not a WebSocket upgrade, and RST (linger 0,
/// no close handshake).
async fn abort_wss_handshake(addr: SocketAddr) {
    let mut stream = TcpStream::connect(addr)
        .await
        .expect("TCP connect to the WSS listener");
    arm_rst(&stream);
    // Not an HTTP upgrade: the server-side WS handshake errors out.
    let _ = stream.write_all(b"\x16\x03\x01 not-a-ws-upgrade\r\n\r\n").await;
    drop(stream); // linger 0 ⇒ RST
}

/// Fail one QUIC handshake at the listener: a dialer that does not
/// trust the server's cert aborts the TLS handshake, which surfaces as
/// a per-connection error on the accept side.
async fn abort_quic_handshake(addr: SocketAddr, server_name: &str) {
    let stranger = CertPair::generate("stranger").expect("stranger cert");
    let outcome = crate::transport::connect(addr, server_name, &stranger.cert_der).await;
    assert!(
        outcome.is_err(),
        "a dial distrusting the server cert must fail its handshake"
    );
}

/// THE replay (RED before the fix): established session + simultaneous
/// reset (session RST + aborted handshakes at BOTH listeners) → the
/// peer re-dials and re-sends its seq-stamped report → the report must
/// be DELIVERED and the ack path back over the re-registered writer
/// must work.
#[tokio::test(flavor = "current_thread")]
async fn reset_then_redialed_replayed_report_is_delivered_and_ackable() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut a: PeerNetwork<TestId> = PeerNetwork::start("peer-a", None).await.unwrap();
            let addr: SocketAddr = format!("127.0.0.1:{}", a.port()).parse().unwrap();

            // 1. ESTABLISH: peer-b dials in over WSS and identifies with
            //    its first frame (the accept loop registers it).
            let mut wire1 = connect_wss(addr).await.expect("initial dial");
            MessageSender::send(&mut wire1, keepalive("peer-b"))
                .await
                .expect("identification frame");
            let first = recv_within(&mut a, Duration::from_secs(5))
                .await
                .expect("identification frame must surface");
            assert_eq!(first.sender_id(), "peer-b");
            // Let the registration (pushed right after the first frame)
            // land, then confirm membership.
            tokio::time::sleep(Duration::from_millis(50)).await;
            a.sync_membership();
            assert!(a.has_peer(&PeerId::from("peer-b")), "session registered");

            // 2. THE RESET — one gateway event, same second:
            //    (i) the established session dies abruptly (RST);
            let ws = wire1.into_inner();
            if let tokio_tungstenite::MaybeTlsStream::Plain(tcp) = ws.get_ref() {
                arm_rst(tcp);
            } else {
                panic!("test wire must be a plain TCP WebSocket");
            }
            drop(ws);
            //    (ii) in-flight handshakes abort at BOTH listeners.
            abort_wss_handshake(addr).await;
            abort_quic_handshake(addr, "peer-a").await;
            // Give the accept loops + the RST a chance to be observed
            // (drive the demux; nothing should be delivered).
            let _ = recv_within(&mut a, Duration::from_millis(300)).await;

            // 3. RE-ESTABLISHMENT: peer-b re-dials (the reconnect-ticker
            //    shape: bounded retries) and REPLAYS the report, same seq.
            let mut wire2 = None;
            for _ in 0..40 {
                match connect_wss(addr).await {
                    Ok(w) => {
                        wire2 = Some(w);
                        break;
                    }
                    Err(_) => tokio::time::sleep(Duration::from_millis(250)).await,
                }
            }
            let mut wire2 = wire2.expect(
                "re-dial must eventually connect: the WSS listener must \
                 survive the reset's aborted handshakes (pre-fix the accept \
                 loop died and the listener was dropped)",
            );
            MessageSender::send(&mut wire2, keepalive("peer-b"))
                .await
                .expect("re-identification frame");
            MessageSender::send(&mut wire2, report("peer-b", 7))
                .await
                .expect("replayed confirmable report");

            // 4. (b): the replayed report IS delivered to the demux.
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            let mut delivered_seq = None;
            while tokio::time::Instant::now() < deadline {
                match recv_within(&mut a, Duration::from_millis(500)).await {
                    Some(msg) if msg.msg_type() == MessageType::TaskComplete => {
                        delivered_seq = msg.delivery_seq();
                        break;
                    }
                    _ => {}
                }
            }
            assert_eq!(
                delivered_seq,
                Some(7),
                "the re-sent confirmable report (same seq) must be ingested \
                 after re-establishment"
            );

            // 5. the ACK path rides the re-registered writer back to the
            //    reporter.
            a.send_to_peer(
                "peer-b",
                DistributedMessage::TerminalAck {
                    target: None,
                    sender_id: "peer-a".into(),
                    timestamp: 3.0,
                    seq: 7,
                },
            )
            .await
            .expect("ack send over the re-registered session");
            let ack = tokio::time::timeout(
                Duration::from_secs(5),
                MessageReceiver::<DistributedMessage<TestId>>::recv(&mut wire2),
            )
            .await
            .expect("ack must arrive within the window")
            .expect("ack frame");
            match ack {
                DistributedMessage::TerminalAck { seq, .. } => assert_eq!(seq, 7),
                other => panic!("expected TerminalAck, got {:?}", other.msg_type()),
            }
        })
        .await;
}

/// The QUIC twin: a failed QUIC handshake (a dialer that aborts TLS)
/// must not end the QUIC accept loop — a subsequent GOOD dial still
/// registers and its frames are delivered. RED before the fix: the
/// accept loop broke on the per-connection error and dropped the
/// endpoint.
#[tokio::test(flavor = "current_thread")]
async fn quic_accept_survives_failed_handshake() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut a: PeerNetwork<TestId> = PeerNetwork::start("peer-a", None).await.unwrap();
            let addr: SocketAddr = format!("127.0.0.1:{}", a.port()).parse().unwrap();

            abort_quic_handshake(addr, "peer-a").await;
            // Let the accept loop process the aborted attempt.
            tokio::time::sleep(Duration::from_millis(100)).await;

            let mut good = crate::transport::connect(addr, "peer-a", a.cert_der())
                .await
                .expect(
                    "a good dial must still connect: the QUIC accept loop \
                     must survive the aborted handshake",
                );
            MessageSender::send(&mut good, keepalive("peer-q"))
                .await
                .expect("identification frame");
            let got = recv_within(&mut a, Duration::from_secs(5))
                .await
                .expect("frame from the good dial must surface");
            assert_eq!(got.sender_id(), "peer-q");
        })
        .await;
}

/// (a)-shape probe: an aborted-handshake STORM at the WSS listener
/// neither monopolises the single-thread executor (a co-scheduled
/// consumer still drains everything — no busy-spin) nor kills the
/// listener (a good dial afterwards still registers and delivers).
#[tokio::test(flavor = "current_thread")]
async fn wss_accept_survives_handshake_storm_without_monopolising() {
    const ITEMS: usize = 50_000;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut a: PeerNetwork<TestId> = PeerNetwork::start("peer-a", None).await.unwrap();
            let addr: SocketAddr = format!("127.0.0.1:{}", a.port()).parse().unwrap();

            for _ in 0..5 {
                abort_wss_handshake(addr).await;
            }

            // Co-scheduled consumer (the recv_tick_closed_spins probe
            // shape): if the errored accept path spun the executor, this
            // would starve.
            let (work_tx, mut work_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
            for _ in 0..ITEMS {
                work_tx.send(()).unwrap();
            }
            drop(work_tx);
            let consumer = tokio::task::spawn_local(async move {
                let mut drained = 0usize;
                while work_rx.recv().await.is_some() {
                    drained += 1;
                }
                drained
            });
            tokio::select! {
                msg = a.recv_peer() => {
                    panic!("no frame expected during the storm window, got {msg:?}");
                }
                _ = tokio::time::sleep(Duration::from_millis(300)) => {}
            }
            let drained = consumer.await.unwrap_or_default();
            assert_eq!(
                drained, ITEMS,
                "the errored accept path must not monopolise the executor"
            );

            // The listener survived: a good dial registers + delivers.
            let mut good = connect_wss(addr)
                .await
                .expect("good dial after the storm: the WSS listener must survive");
            MessageSender::send(&mut good, keepalive("peer-w"))
                .await
                .expect("identification frame");
            let got = recv_within(&mut a, Duration::from_secs(5))
                .await
                .expect("frame from the good dial must surface");
            assert_eq!(got.sender_id(), "peer-w");
        })
        .await;
}
