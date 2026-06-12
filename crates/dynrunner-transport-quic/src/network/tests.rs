use super::*;
use crate::wss::WssListener;
use dynrunner_core::{MessageReceiver, MessageSender};
use dynrunner_protocol_primary_secondary::{DistributedMessage, KeepaliveRole, PeerTransport};
use dynrunner_transport_tunnel::TunneledPeerTransport;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct TestId(String);

/// Test: WSS client connects to NetworkServer, sends a message, the
/// unified transport (fed by the accept loops) receives it and can send
/// back via the registered writer.
///
/// Post-Shape-A: the accept loops feed the `TunneledPeerTransport`
/// directly (its `recv_peer` owns the inbound demux + writer-table
/// registration), so the test drives `transport.recv_peer()` and
/// `transport.send_to_peer(..)` rather than the deleted
/// `NetworkServer::recv` / `send_to`.
#[tokio::test(flavor = "current_thread")]
async fn server_accepts_wss_bidirectional() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let (mut transport, _outgoing, inbound, registration) =
                TunneledPeerTransport::<TestId>::new("primary".into());
            let server: NetworkServer = NetworkServer::bind::<TestId>(addr, "primary", inbound, registration)
                .await
                .unwrap();
            let port = server.port();
            let server_addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

            let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();

            let client_task = tokio::task::spawn_local(async move {
                let mut client = NetworkClient::connect_wss_only(server_addr)
                    .await
                    .expect("WSS connect failed");

                let welcome: DistributedMessage<TestId> = DistributedMessage::SecondaryWelcome {
                    target: None,
                    sender_id: "sec-0".into(),
                    timestamp: 1.0,
                    secondary_id: "sec-0".into(),
                    resources: vec![dynrunner_core::ResourceAmount {
                        kind: dynrunner_core::ResourceKind::memory(),
                        amount: 1024,
                    }],
                    worker_count: 1,
                    hostname: "test".into(),
                    is_observer: false,
                    can_be_primary: false,
                };
                MessageSender::send(&mut client, welcome).await.unwrap();

                let reply: DistributedMessage<TestId> =
                    MessageReceiver::recv(&mut client).await.expect("no reply");
                done_tx.send(()).ok();
                reply
            });

            // `recv_peer` demuxes the registration (writer inserted) and
            // yields the welcome frame — the FIFO welcome-before-reply
            // ordering the accept loop establishes.
            let msg = transport.recv_peer().await.expect("no message received");
            assert_eq!(msg.sender_id(), "sec-0");

            let reply: DistributedMessage<TestId> = DistributedMessage::Keepalive {
                target: None,
                sender_id: "primary".into(),
                timestamp: 2.0,
                secondary_id: "primary".into(),
                active_workers: 0,
                emitter_role: KeepaliveRole::Secondary,
            };
            transport.send_to_peer("sec-0", reply).await.unwrap();

            done_rx.await.unwrap();
            let echoed = client_task.await.unwrap();
            assert_eq!(echoed.sender_id(), "primary");
        })
        .await;
}

/// Test: QUIC client connects to NetworkServer, the unified transport
/// receives and replies. Same Shape-A shape as the WSS sibling.
#[tokio::test(flavor = "current_thread")]
async fn server_accepts_quic_bidirectional() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let (mut transport, _outgoing, inbound, registration) =
                TunneledPeerTransport::<TestId>::new("primary".into());
            let server: NetworkServer = NetworkServer::bind::<TestId>(addr, "primary", inbound, registration)
                .await
                .unwrap();
            let port = server.port();
            let cert_der = server.cert_der().clone();
            let server_addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

            let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();

            let client_task = tokio::task::spawn_local(async move {
                let mut client = NetworkClient::connect(
                    server_addr,
                    "primary",
                    &cert_der,
                    Duration::from_secs(5),
                )
                .await
                .expect("connect failed");

                assert!(matches!(client, NetworkClient::Quic(_)));

                let msg: DistributedMessage<TestId> = DistributedMessage::Keepalive {
                    target: None,
                    sender_id: "sec-1".into(),
                    timestamp: 2.0,
                    secondary_id: "sec-1".into(),
                    active_workers: 3,
                    emitter_role: KeepaliveRole::Secondary,
                };
                MessageSender::send(&mut client, msg).await.unwrap();

                let reply: DistributedMessage<TestId> =
                    MessageReceiver::recv(&mut client).await.expect("no reply");
                done_tx.send(()).ok();
                reply
            });

            let msg = transport.recv_peer().await.expect("no message received");
            assert_eq!(msg.sender_id(), "sec-1");

            let reply: DistributedMessage<TestId> = DistributedMessage::Keepalive {
                target: None,
                sender_id: "primary".into(),
                timestamp: 3.0,
                secondary_id: "primary".into(),
                active_workers: 0,
                emitter_role: KeepaliveRole::Secondary,
            };
            transport.send_to_peer("sec-1", reply).await.unwrap();

            done_rx.await.unwrap();
            let echoed = client_task.await.unwrap();
            assert_eq!(echoed.sender_id(), "primary");
        })
        .await;
}

/// Test: NetworkClient falls back to WSS when QUIC is unavailable.
/// Independent of the transport ownership move — exercises the client
/// dial fallback against a bare WSS listener.
#[tokio::test(flavor = "current_thread")]
async fn client_falls_back_to_wss() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let wss_listener = WssListener::bind("127.0.0.1:0".parse().unwrap())
                .await
                .unwrap();
            let port = wss_listener.port();
            let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

            let server_task = tokio::task::spawn_local(async move {
                let mut conn = wss_listener.accept().await.unwrap();
                let msg: DistributedMessage<TestId> =
                    MessageReceiver::recv(&mut conn).await.expect("no msg");
                msg
            });

            let bogus_cert = CertPair::generate("bogus").unwrap();
            let mut client = NetworkClient::connect(
                addr,
                "bogus",
                &bogus_cert.cert_der,
                Duration::from_millis(500),
            )
            .await
            .expect("should fall back to WSS");

            assert!(matches!(client, NetworkClient::Wss(_)));

            let msg: DistributedMessage<TestId> = DistributedMessage::Keepalive {
                target: None,
                sender_id: "fallback".into(),
                timestamp: 1.0,
                secondary_id: "fallback".into(),
                active_workers: 0,
                emitter_role: KeepaliveRole::Secondary,
            };
            MessageSender::send(&mut client, msg).await.unwrap();

            let received = server_task.await.unwrap();
            assert_eq!(received.sender_id(), "fallback");
        })
        .await;
}

/// Shape-A FIFO invariant: a secondary that sends `SecondaryWelcome`
/// then `CertExchange` over one connection surfaces them in that order
/// through the unified transport's `recv_peer`, with the writer
/// registration applied between (so a reply could be sent to the
/// secondary before its CertExchange is even processed).
///
/// This pins the `SecondaryWelcome → registration → CertExchange`
/// ordering the manager's `wait_for_connections` depends on: the
/// welcome registers the secondary in `self.secondaries`, the cert
/// exchange looks it up — so welcome MUST precede cert exchange. Both
/// ride the single `incoming_rx`, so their order is FIFO-preserved; the
/// registration is interleaved on `new_conn_rx` but only mutates the
/// writer table, never the application stream order.
#[tokio::test(flavor = "current_thread")]
async fn tap_forwards_welcome_and_cert_before_cert_exchange_completes() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let (mut transport, _outgoing, inbound, registration) =
                TunneledPeerTransport::<TestId>::new("primary".into());
            let server: NetworkServer = NetworkServer::bind::<TestId>(addr, "primary", inbound, registration)
                .await
                .unwrap();
            let port = server.port();
            let server_addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

            let _client_task = tokio::task::spawn_local(async move {
                let mut client = NetworkClient::connect_wss_only(server_addr)
                    .await
                    .expect("WSS connect failed");
                let welcome: DistributedMessage<TestId> = DistributedMessage::SecondaryWelcome {
                    target: None,
                    sender_id: "sec-0".into(),
                    timestamp: 1.0,
                    secondary_id: "sec-0".into(),
                    resources: vec![dynrunner_core::ResourceAmount {
                        kind: dynrunner_core::ResourceKind::memory(),
                        amount: 1024,
                    }],
                    worker_count: 1,
                    hostname: "test".into(),
                    is_observer: false,
                    can_be_primary: false,
                };
                MessageSender::send(&mut client, welcome).await.unwrap();
                let cert: DistributedMessage<TestId> = DistributedMessage::CertExchange {
                    target: None,
                    sender_id: "sec-0".into(),
                    timestamp: 2.0,
                    secondary_id: "sec-0".into(),
                    public_cert_pem: "FAKE".into(),
                    ipv4_address: Some("127.0.0.1".into()),
                    ipv6_address: None,
                    quic_port: 5000,
                    liveness_port: None,
                };
                MessageSender::send(&mut client, cert).await.unwrap();
                // Keep the connection (and thus the writer task) alive
                // until the test drops this handle.
                let _ = MessageReceiver::<DistributedMessage<TestId>>::recv(&mut client).await;
            });

            let first = transport.recv_peer().await.expect("welcome");
            assert!(
                matches!(
                    first,
                    DistributedMessage::SecondaryWelcome { target: None, .. }
                ),
                "first frame must be the welcome, got {first:?}"
            );
            // The writer must be registered by the time the welcome
            // surfaces (the demux applied the registration). A
            // send_to_peer to sec-0 therefore finds a route.
            transport
                .send_to_peer(
                    "sec-0",
                    DistributedMessage::Keepalive {
                        target: None,
                        sender_id: "primary".into(),
                        timestamp: 3.0,
                        secondary_id: "primary".into(),
                        active_workers: 0,
                        emitter_role: KeepaliveRole::Secondary,
                    },
                )
                .await
                .expect("writer registered before cert-exchange processed");

            let second = transport.recv_peer().await.expect("cert");
            assert!(
                matches!(
                    second,
                    DistributedMessage::CertExchange { target: None, .. }
                ),
                "second frame must be the cert exchange, got {second:?}"
            );
        })
        .await;
}

/// `NetworkClient::mesh_writer` is a fan-in into the SAME wire: a frame
/// sent through the minted handle reaches the server over the existing
/// connection, alongside the client's own `MessageSender::send`. This
/// is the handle the secondary mesh registers as the directed primary
/// link, so `mesh.send_to_peer(primary)` reaches the primary over the
/// bootstrap connection rather than opening a second one.
#[tokio::test(flavor = "current_thread")]
async fn mesh_writer_fans_into_the_same_wire() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let (mut transport, _outgoing, inbound, registration) =
                TunneledPeerTransport::<TestId>::new("primary".into());
            let server: NetworkServer = NetworkServer::bind::<TestId>(addr, "primary", inbound, registration)
                .await
                .unwrap();
            let port = server.port();
            let server_addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

            let _client_task = tokio::task::spawn_local(async move {
                let mut client = NetworkClient::connect_wss_only(server_addr)
                    .await
                    .expect("WSS connect failed");
                // First frame on a fresh connection identifies the
                // sender to the accept side (it reads `sender_id` from
                // the first frame); send it via the client's own path.
                let welcome: DistributedMessage<TestId> = DistributedMessage::SecondaryWelcome {
                    target: None,
                    sender_id: "sec-0".into(),
                    timestamp: 1.0,
                    secondary_id: "sec-0".into(),
                    resources: vec![],
                    worker_count: 1,
                    hostname: "test".into(),
                    is_observer: false,
                    can_be_primary: false,
                };
                MessageSender::send(&mut client, welcome).await.unwrap();

                // Now mint a mesh_writer and send a SECOND frame through
                // it — it must travel the same wire and surface at the
                // server after the welcome (FIFO with the client's send).
                let mesh_writer = client.mesh_writer();
                mesh_writer
                    .send(DistributedMessage::Keepalive {
                        target: None,
                        sender_id: "sec-0".into(),
                        timestamp: 2.0,
                        secondary_id: "sec-0".into(),
                        active_workers: 7,
                        emitter_role: KeepaliveRole::Secondary,
                    })
                    .expect("mesh_writer send must enqueue");

                // Keep the connection alive until the test finishes.
                let _ = MessageReceiver::<DistributedMessage<TestId>>::recv(&mut client).await;
            });

            let first = transport.recv_peer().await.expect("welcome");
            assert!(matches!(
                first,
                DistributedMessage::SecondaryWelcome { target: None, .. }
            ));
            let second = transport.recv_peer().await.expect("mesh_writer frame");
            match second {
                DistributedMessage::Keepalive { active_workers, .. } => {
                    assert_eq!(
                        active_workers, 7,
                        "the frame sent via mesh_writer must reach the server over the same wire",
                    );
                }
                other => panic!("expected the mesh_writer Keepalive, got {other:?}"),
            }
        })
        .await;
}

/// Shape-A buffering invariant: frames accepted before the first
/// `recv_peer()` poll are not lost — the unbounded inbound mpsc buffers
/// them, and the first poll drains them in FIFO order. Pins that a
/// fast secondary which speaks before the manager enters its recv loop
/// is handled (the historical "0/N" / dropped-welcome hazard).
#[tokio::test(flavor = "current_thread")]
async fn recv_peer_drains_buffered_frames_accepted_before_first_poll() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let (mut transport, _outgoing, inbound, registration) =
                TunneledPeerTransport::<TestId>::new("primary".into());
            let server: NetworkServer = NetworkServer::bind::<TestId>(addr, "primary", inbound, registration)
                .await
                .unwrap();
            let port = server.port();
            let server_addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

            let _client_task = tokio::task::spawn_local(async move {
                let mut client = NetworkClient::connect_wss_only(server_addr)
                    .await
                    .expect("WSS connect failed");
                for i in 0..3u32 {
                    let ka: DistributedMessage<TestId> = DistributedMessage::Keepalive {
                        target: None,
                        sender_id: "sec-0".into(),
                        timestamp: i as f64,
                        secondary_id: "sec-0".into(),
                        active_workers: i,
                        emitter_role: KeepaliveRole::Secondary,
                    };
                    MessageSender::send(&mut client, ka).await.unwrap();
                }
                let _ = MessageReceiver::<DistributedMessage<TestId>>::recv(&mut client).await;
            });

            // Give the accept loop + reader task time to land all three
            // frames into the inbound mpsc BEFORE the first recv_peer
            // poll, exercising the "buffered before first poll" path.
            tokio::time::sleep(Duration::from_millis(100)).await;

            for i in 0..3u32 {
                let msg = transport.recv_peer().await.expect("buffered frame");
                match msg {
                    DistributedMessage::Keepalive { active_workers, .. } => {
                        assert_eq!(active_workers, i, "FIFO order preserved");
                    }
                    other => panic!("expected Keepalive, got {other:?}"),
                }
            }
        })
        .await;
}

/// #370 transport substrate of the relocation-handoff heal, end-to-end
/// over real WSS through a flappable TCP proxy (the `-R` tunnel stand-in):
///
/// 1. the secondary dials + folds the bootstrap wire and REGISTERS on the
///    submitter via its first frame (the production SecondaryWelcome);
/// 2. the "tunnel" FLAPS (both pipe ends cut) — the secondary's
///    bootstrap-redial supervisor re-dials the SAME fixed address and the
///    fresh wire re-folds, but the submitter's accept loop holds it
///    UNIDENTIFIED: submitter broadcasts reach NOBODY (the production
///    "observer ticks, secondaries receive nothing" wedge shape);
/// 3. the secondary SPEAKS on the re-folded wire (the setup-phase
///    anti-entropy digest is exactly this frame in production) — the
///    accept loop registers `sec-0` off the frame's sender-id, and the
///    submitter's next broadcast REACHES the secondary again.
///
/// This pins the contract the manager-level heal depends on: a re-dialed
/// wire re-enters the submitter's writer table ONLY via a first inbound
/// frame, and once it does, broadcast reach is fully restored.
#[tokio::test(flavor = "current_thread")]
async fn redialed_bootstrap_wire_reregisters_on_first_frame_and_receives_broadcasts() {
    use dynrunner_protocol_primary_secondary::StateDigest as WireDigest;

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // ── Submitter: unified transport + real accept loops. ──
            let (mut transport, _outgoing, inbound, registration) =
                TunneledPeerTransport::<TestId>::new("setup".into());
            let server: NetworkServer = NetworkServer::bind::<TestId>(
                "127.0.0.1:0".parse().unwrap(),
                "setup",
                inbound,
                registration,
            )
            .await
            .unwrap();
            let server_addr: SocketAddr = format!("127.0.0.1:{}", server.port()).parse().unwrap();

            // ── The "tunnel": a TCP proxy whose live pipes the test cuts. ──
            let proxy_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let proxy_addr = proxy_listener.local_addr().unwrap();
            let (pipe_tx, mut pipe_rx) =
                tokio::sync::mpsc::unbounded_channel::<tokio::task::JoinHandle<()>>();
            tokio::task::spawn_local(async move {
                loop {
                    let Ok((mut downstream, _)) = proxy_listener.accept().await else {
                        break;
                    };
                    let pipe = tokio::task::spawn_local(async move {
                        let Ok(mut upstream) = tokio::net::TcpStream::connect(server_addr).await
                        else {
                            return;
                        };
                        let _ = tokio::io::copy_bidirectional(&mut downstream, &mut upstream).await;
                    });
                    let _ = pipe_tx.send(pipe);
                }
            });

            // ── Secondary: peer mesh + the bootstrap dial THROUGH the proxy. ──
            let mut net: crate::PeerNetwork<TestId> =
                crate::PeerNetwork::start("sec-0", None).await.unwrap();
            let client = NetworkClient::<TestId>::connect_wss_only(proxy_addr)
                .await
                .expect("initial bootstrap dial through the proxy");
            net.register_primary_link("setup".to_string(), proxy_addr, client);
            let pipe1 = tokio::time::timeout(Duration::from_secs(5), pipe_rx.recv())
                .await
                .expect("the initial dial must traverse the proxy")
                .expect("proxy alive");

            let welcome: DistributedMessage<TestId> = DistributedMessage::SecondaryWelcome {
                target: None,
                sender_id: "sec-0".into(),
                timestamp: 1.0,
                secondary_id: "sec-0".into(),
                resources: vec![dynrunner_core::ResourceAmount {
                    kind: dynrunner_core::ResourceKind::memory(),
                    amount: 1024,
                }],
                worker_count: 1,
                hostname: "test".into(),
                is_observer: false,
                can_be_primary: true,
            };
            net.send_to_peer("setup", welcome)
                .await
                .expect("welcome over the folded wire");

            // The first frame registers the secondary on the submitter.
            let got = tokio::time::timeout(Duration::from_secs(5), transport.recv_peer())
                .await
                .expect("the welcome must reach the submitter")
                .expect("transport open");
            assert!(matches!(got, DistributedMessage::SecondaryWelcome { .. }));
            assert!(
                PeerTransport::<TestId>::has_peer(&transport, &dynrunner_protocol_primary_secondary::PeerId::from("sec-0")),
                "the first frame registers the secondary's writer"
            );

            // Baseline: a submitter broadcast reaches the secondary.
            let submitter_digest = |ts: f64| DistributedMessage::<TestId>::StateDigest {
                target: None,
                sender_id: "setup".into(),
                timestamp: ts,
                digest: WireDigest::default(),
            };
            transport.broadcast(submitter_digest(1.0)).await.unwrap();
            let mut baseline = false;
            for _ in 0..25 {
                if let Ok(Some(DistributedMessage::StateDigest { sender_id, .. })) =
                    tokio::time::timeout(Duration::from_millis(200), net.recv_peer()).await
                    && sender_id == "setup"
                {
                    baseline = true;
                    break;
                }
            }
            assert!(baseline, "pre-flap broadcast must reach the secondary");

            // ── FLAP: cut the live pipe. Both wire ends drop; the
            // secondary's redial supervisor re-dials the fixed proxy addr. ──
            pipe1.abort();
            let _pipe2 = tokio::time::timeout(Duration::from_secs(10), pipe_rx.recv())
                .await
                .expect("the secondary must RE-DIAL through the (rebuilt) tunnel")
                .expect("proxy alive");

            // Post-flap, pre-identification: submitter broadcasts go NOWHERE.
            // (The first broadcast may also prune the dead pre-flap writer —
            // the production silent-shrink, now WARN-narrated.)
            transport.broadcast(submitter_digest(2.0)).await.unwrap();
            transport.broadcast(submitter_digest(3.0)).await.unwrap();
            let mut leaked = false;
            for _ in 0..5 {
                if let Ok(Some(DistributedMessage::StateDigest { sender_id, .. })) =
                    tokio::time::timeout(Duration::from_millis(100), net.recv_peer()).await
                    && sender_id == "setup"
                {
                    leaked = true;
                    break;
                }
            }
            assert!(
                !leaked,
                "an unidentified re-dialed wire must not receive submitter \
                 broadcasts (the accept loop has no writer registered for it)"
            );

            // ── The heal: the secondary SPEAKS on the re-folded wire (the
            // setup-phase anti-entropy digest). Retry on a compressed
            // cadence — the production cadence is the ~20s jittered tick;
            // the re-fold lands asynchronously via the secondary's own
            // recv_peer turns, exactly as in production. ──
            let sec_digest: DistributedMessage<TestId> = DistributedMessage::StateDigest {
                target: None,
                sender_id: "sec-0".into(),
                timestamp: 4.0,
                digest: WireDigest::default(),
                sender_is_observer: false,
            };
            let mut reregistered = false;
            'outer: for _ in 0..50 {
                // Drive the secondary's network turn (re-fold arm included).
                let _ =
                    tokio::time::timeout(Duration::from_millis(100), net.recv_peer()).await;
                let _ = net.send_to_peer("setup", sec_digest.clone()).await;
                // Did the digest land (⇒ the accept loop identified the
                // wire and registered the fresh writer)?
                while let Ok(Some(frame)) =
                    tokio::time::timeout(Duration::from_millis(100), transport.recv_peer()).await
                {
                    if matches!(
                        &frame,
                        DistributedMessage::StateDigest { sender_id, .. } if sender_id == "sec-0"
                    ) {
                        reregistered = true;
                        break 'outer;
                    }
                }
            }
            assert!(
                reregistered,
                "the secondary's first frame on the re-folded wire must reach \
                 the submitter and re-register it"
            );
            assert!(
                PeerTransport::<TestId>::has_peer(&transport, &dynrunner_protocol_primary_secondary::PeerId::from("sec-0")),
                "re-registration restores the writer-table membership"
            );

            // Reach restored: the submitter's next broadcast arrives.
            transport.broadcast(submitter_digest(5.0)).await.unwrap();
            let mut healed = false;
            for _ in 0..50 {
                if let Ok(Some(DistributedMessage::StateDigest {
                    sender_id,
                    timestamp,
                    ..
                })) = tokio::time::timeout(Duration::from_millis(200), net.recv_peer()).await
                    && sender_id == "setup"
                    && timestamp == 5.0
                {
                    healed = true;
                    break;
                }
            }
            assert!(
                healed,
                "post-re-registration broadcasts must reach the secondary — \
                 the digest heal's transport substrate is restored"
            );
        })
        .await;
}

/// Specimen-1 replay (run_20260611_200548, mass-disconnect shape): the
/// relocated submitter-observer's whole transport view collapses, and
/// the collapse itself throws aborted mid-handshake connections at the
/// submitter's WSS listener (dying tunnels, force-rebuilt forwards).
/// When the secondaries later re-dial through the rebuilt tunnels,
/// every leg must re-seat and frames must flow — ONE aborted inbound
/// must not have killed the listener (pre-fix: the inline-handshake
/// accept loop broke on the first aborted upgrade, the listener
/// dropped, and every redial got a TCP refusal for 90+ minutes while a
/// FRESH process bound a fresh listener and connected in seconds).
#[tokio::test(flavor = "current_thread")]
async fn submitter_reaccepts_redials_after_aborted_handshake_poisons_listener() {
    use tokio::io::AsyncWriteExt;

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // ── Submitter-observer seat: unified transport + accept loops. ──
            let (mut transport, _outgoing, inbound, registration) =
                TunneledPeerTransport::<TestId>::new("setup".into());
            let server: NetworkServer = NetworkServer::bind::<TestId>(
                "127.0.0.1:0".parse().unwrap(),
                "setup",
                inbound,
                registration,
            )
            .await
            .unwrap();
            let server_addr: SocketAddr = format!("127.0.0.1:{}", server.port()).parse().unwrap();

            let keepalive = |sender: &str, ts: f64| -> DistributedMessage<TestId> {
                DistributedMessage::Keepalive {
                    target: None,
                    sender_id: sender.into(),
                    timestamp: ts,
                    secondary_id: sender.into(),
                    active_workers: 1,
                    emitter_role: KeepaliveRole::Secondary,
                }
            };

            // ── Two live "secondaries" dial in and identify. ──
            let mut clients = Vec::new();
            for sec in ["sec-0", "sec-1"] {
                let mut client = NetworkClient::<TestId>::connect_wss_only(server_addr)
                    .await
                    .expect("initial dial");
                MessageSender::send(&mut client, keepalive(sec, 1.0))
                    .await
                    .expect("identifying frame");
                let got = tokio::time::timeout(Duration::from_secs(5), transport.recv_peer())
                    .await
                    .expect("frame must arrive")
                    .expect("transport open");
                assert_eq!(got.sender_id(), sec);
                clients.push(client);
            }
            assert_eq!(PeerTransport::<TestId>::peer_count(&transport), 2);

            // ── 18:19 — MASS DISCONNECT: every wire dies at once. ──
            drop(clients);
            // The broadcast observes the dead writers and prunes them
            // (the production "broadcast found dead peer writers" sweep).
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            loop {
                let _ = transport.broadcast(keepalive("setup", 2.0)).await;
                if PeerTransport::<TestId>::peer_count(&transport) == 0 {
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "dead writers must be pruned — the transport view empties"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }

            // ── The collapse poisons the listener: an aborted
            //    mid-handshake connection (garbage instead of the
            //    WebSocket upgrade, then a hard close). ──
            {
                let mut raw = tokio::net::TcpStream::connect(server_addr)
                    .await
                    .expect("poison TCP connect");
                let _ = raw
                    .write_all(b"\x16\x03\x01 not a websocket upgrade\r\n\r\n")
                    .await;
                let _ = raw.shutdown().await;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;

            // ── The secondaries' redial supervisors come back through
            //    the rebuilt tunnels: every leg must RE-SEAT. ──
            let mut reclients = Vec::new();
            for sec in ["sec-0", "sec-1"] {
                let mut client = NetworkClient::<TestId>::connect_wss_only(server_addr)
                    .await
                    .unwrap_or_else(|e| {
                        panic!(
                            "the redial after the poisoned accept must connect \
                             (a fresh process can — so must the reconnect path): {e}"
                        )
                    });
                MessageSender::send(&mut client, keepalive(sec, 3.0))
                    .await
                    .expect("re-identifying frame");
                let got = tokio::time::timeout(Duration::from_secs(5), transport.recv_peer())
                    .await
                    .expect("the re-dialed leg's frame must reach the transport")
                    .expect("transport open");
                assert_eq!(got.sender_id(), sec);
                reclients.push(client);
            }
            assert_eq!(
                PeerTransport::<TestId>::peer_count(&transport),
                2,
                "both rebuilt legs must be live sessions again"
            );
        })
        .await;
}
