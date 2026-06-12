//! THE mesh-runtime-split pin: wire QoS survives a BLOCKED coordinator.
//!
//! Production evidence (the coordinator-saturation incident: an hour of
//! unacked report replays; the earlier total-collapse RCA) traced to the
//! transport sharing ONE `current_thread` runtime with the coordinators —
//! a stalled coordinator froze the WIRE itself: no frame ingest, no accept
//! servicing, no keepalive egress. The structural fix hosts the transport +
//! mesh-pump on a dedicated runtime thread
//! (`MeshHost::on_dedicated_thread`), and THIS test is the guarantee the
//! whole change exists for: with the coordinator-side thread blocked in a
//! hard `std::thread::sleep` (the saturation stall, replayed), a live peer
//! observes that the blocked node's wire keeps being serviced.
//!
//! Two peer-observable facts are asserted for the blocked window:
//!
//! 1. **Ingest continues** — the probe peer's frames are delivered into the
//!    blocked coordinator's `RoleInbox` DURING the stall. Discriminator:
//!    the inbox's ingest clock (recorded by the pump at slot delivery) is
//!    read at the INSTANT the coordinator wakes, BEFORE it yields to its
//!    runtime — co-resident hosting cannot have delivered anything while
//!    the thread slept (no task ran), so the pre-split architecture reads
//!    the pre-block timestamp here and FAILS.
//! 2. **Accept servicing continues** — a fresh WSS dial to the blocked
//!    node's advertised mesh port completes its server-side handshake
//!    (the accept loop must respond — TCP's kernel backlog alone cannot
//!    answer the WebSocket upgrade) mid-stall, within a bounded window.

use std::time::{Duration, Instant};

use dynrunner_manager_distributed::process::{LocalRole, MeshHost};
use dynrunner_protocol_primary_secondary::address::{Destination, PeerId};
use dynrunner_protocol_primary_secondary::{
    DistributedMessage, KeepaliveRole, PeerConnectionInfo, PeerTransport,
};
use dynrunner_transport_quic::{NetworkClient, PeerNetwork};

type TestId = String;

/// How long the "coordinator" thread blocks — the replayed stall.
const BLOCK: Duration = Duration::from_secs(3);
/// The probe peer stops sending at this fraction of the block, so the
/// final ingest timestamp provably falls INSIDE the blocked window (not in
/// a catch-up burst at wake).
const SEND_WINDOW: Duration = Duration::from_millis(1800);
/// Cadence of the probe peer's sends during the block.
const SEND_INTERVAL: Duration = Duration::from_millis(50);

/// What the probe peer (its own thread + runtime) observed while the
/// coordinator thread was blocked.
struct ProbeReport {
    /// Frames sent during the blocked window (every send returned `Ok`).
    frames_sent: u32,
    /// Send errors during the blocked window.
    send_errors: u32,
    /// Whether a FRESH WSS dial to the blocked node's mesh port completed
    /// its handshake mid-stall.
    accept_probe_connected: bool,
}

fn probe_frame(seq: u32) -> DistributedMessage<TestId> {
    DistributedMessage::Keepalive {
        target: Some(Destination::Secondary(PeerId::from("b-blocked"))),
        sender_id: "a-probe".to_string(),
        timestamp: seq as f64,
        secondary_id: "a-probe".to_string(),
        active_workers: 0,
        emitter_role: KeepaliveRole::Secondary,
    }
}

/// The probe peer's whole life, on its OWN thread + `current_thread`
/// runtime + `LocalSet` (so it keeps running while the test's main thread —
/// the blocked "coordinator" — sleeps): dial the blocked node, deliver one
/// hello frame, wait for the go-signal, then send frames + run the WSS
/// accept-probe DURING the blocked window, and report.
fn probe_peer_main(
    b_port: u16,
    b_cert: String,
    go_rx: std::sync::mpsc::Receiver<()>,
    report_tx: std::sync::mpsc::Sender<ProbeReport>,
) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("probe runtime");
    let local = tokio::task::LocalSet::new();
    rt.block_on(local.run_until(async move {
        let mut probe: PeerNetwork<TestId> = PeerNetwork::start("a-probe", None)
            .await
            .expect("probe peer network");
        let peers = vec![PeerConnectionInfo {
            secondary_id: "b-blocked".to_string(),
            cert: b_cert,
            ipv4: Some("127.0.0.1".to_string()),
            ipv6: None,
            port: b_port,
            is_observer: false,
            liveness_port: None,
        }];
        // "a-probe" < "b-blocked": the lower id dials (the production
        // dial-direction tie-break), so the blocked node only ACCEPTS.
        probe.connect_to_peers(&peers);
        // Promote the dialed connection and deliver the hello frame the
        // main thread gates its block on. `recv_peer`'s select loop is the
        // public path that drains the dial-registration sink into the
        // connections table (nothing inbound is expected from the blocked
        // node — the timeout just bounds each drain pass).
        let hello_deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let _ = tokio::time::timeout(Duration::from_millis(100), probe.recv_peer()).await;
            if probe
                .send_to_peer("b-blocked", probe_frame(0))
                .await
                .is_ok()
            {
                break;
            }
            assert!(
                Instant::now() < hello_deadline,
                "probe leg to the blocked node never formed"
            );
        }

        // Await the go-signal WITHOUT blocking this runtime (the probe's
        // own QUIC driver tasks must keep running).
        while go_rx.try_recv().is_err() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // The blocked window: send on a steady cadence, and mid-window run
        // the accept-probe (a fresh WSS dial — its server-side handshake
        // needs the blocked node's accept loop to RESPOND).
        let start = Instant::now();
        let mut frames_sent = 0u32;
        let mut send_errors = 0u32;
        let mut accept_probe_connected = false;
        let mut seq = 0u32;
        while start.elapsed() < SEND_WINDOW {
            seq += 1;
            match probe.send_to_peer("b-blocked", probe_frame(seq)).await {
                Ok(()) => frames_sent += 1,
                Err(_) => send_errors += 1,
            }
            if !accept_probe_connected && start.elapsed() >= SEND_WINDOW / 2 {
                let addr: std::net::SocketAddr =
                    format!("127.0.0.1:{b_port}").parse().expect("probe addr");
                accept_probe_connected = tokio::time::timeout(
                    Duration::from_secs(1),
                    NetworkClient::<TestId>::connect_wss_only(addr),
                )
                .await
                .map(|r| r.is_ok())
                .unwrap_or(false);
            }
            tokio::time::sleep(SEND_INTERVAL).await;
        }
        report_tx
            .send(ProbeReport {
                frames_sent,
                send_errors,
                accept_probe_connected,
            })
            .expect("report channel");
        // Keep the probe's wire alive until the main thread has read the
        // report and drained the inbox (dropping the network here would
        // race the tail of the assertion window for no benefit).
        tokio::time::sleep(Duration::from_secs(10)).await;
    }));
}

#[test]
fn wire_ingest_and_accept_survive_a_blocked_coordinator() {
    // The coordinator side: THIS thread's runtime — the one that blocks.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("coordinator runtime");
    let local = tokio::task::LocalSet::new();
    rt.block_on(local.run_until(async {
        // The system under test: transport + mesh-pump on the dedicated
        // mesh runtime thread; only channel handles live here.
        let (host, (b_port, b_cert)) = MeshHost::<TestId>::on_dedicated_thread(|| async {
            let network: PeerNetwork<TestId> = PeerNetwork::start("b-blocked", None).await?;
            let port = network.port();
            let cert = network.cert_pem().to_string();
            Ok((network, (port, cert)))
        })
        .await
        .expect("mesh runtime");
        let (slot, _client, mut inbox) = host
            .control()
            .register(LocalRole::Secondary, PeerId::from("b-blocked"))
            .await
            .expect("register the blocked coordinator's role");
        // Keep the slot alive for the whole scenario (delivery lever).
        let _slot = slot;

        let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
        let (report_tx, report_rx) = std::sync::mpsc::channel::<ProbeReport>();
        let probe_thread = std::thread::Builder::new()
            .name("probe-peer".to_string())
            .spawn(move || probe_peer_main(b_port, b_cert, go_rx, report_tx))
            .expect("spawn probe thread");

        // Gate on the hello frame: the probe leg is up and ingest works
        // BEFORE the stall.
        let hello = tokio::time::timeout(Duration::from_secs(30), inbox.recv())
            .await
            .expect("probe leg never delivered the hello frame")
            .expect("inbox closed");
        assert_eq!(hello.sender_id(), "a-probe");

        // ── THE STALL ────────────────────────────────────────────────
        // Signal the probe, then hard-block this thread — the replayed
        // coordinator saturation. While this thread sleeps, NO task on
        // this runtime runs; only the dedicated mesh runtime can service
        // the wire.
        go_tx.send(()).expect("go signal");
        let block_start = Instant::now();
        std::thread::sleep(BLOCK);

        // ── WAKE: read the ingest clock BEFORE yielding ──────────────
        // `last_ingest_from` is recorded by the PUMP at slot delivery. At
        // this instant the coordinator runtime has not polled a single
        // task since `block_start`, so any timestamp inside the blocked
        // window can ONLY have been written by the dedicated mesh
        // runtime. Pre-split (pump co-resident on this runtime) this read
        // returns the hello frame's pre-block timestamp and fails.
        let last_ingest = inbox
            .last_ingest_from("a-probe")
            .expect("ingest clock must know the probe peer");
        assert!(
            last_ingest > block_start,
            "no frame was ingested during the coordinator stall — the wire \
             was starved with the coordinator (the pre-split failure mode)"
        );
        assert!(
            last_ingest < block_start + BLOCK,
            "ingest timestamp after wake — the read must happen before this \
             runtime resumes delivering"
        );

        // The probe's peer-side observations for the same window.
        let report = report_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("probe report");
        assert_eq!(
            report.send_errors, 0,
            "the probe's sends must keep succeeding against the blocked node"
        );
        assert!(
            report.frames_sent >= 10,
            "the probe must have sent a steady stream during the stall \
             (got {})",
            report.frames_sent
        );
        assert!(
            report.accept_probe_connected,
            "a fresh WSS dial mid-stall must complete its handshake — the \
             accept loop lives on the mesh runtime, not the blocked \
             coordinator"
        );

        // Every frame sent during the stall is sitting in the inbox.
        for n in 0..report.frames_sent {
            tokio::time::timeout(Duration::from_secs(5), inbox.recv())
                .await
                .unwrap_or_else(|_| panic!("frame {n} missing from the inbox"))
                .expect("inbox closed");
        }

        host.stop().await;
        probe_thread.join().expect("probe thread");
    }));
}
