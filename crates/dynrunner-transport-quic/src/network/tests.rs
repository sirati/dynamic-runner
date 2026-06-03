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
            let server: NetworkServer = NetworkServer::bind::<TestId>(addr, inbound, registration)
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
            let server: NetworkServer = NetworkServer::bind::<TestId>(addr, inbound, registration)
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
            let server: NetworkServer = NetworkServer::bind::<TestId>(addr, inbound, registration)
                .await
                .unwrap();
            let port = server.port();
            let server_addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

            let _client_task = tokio::task::spawn_local(async move {
                let mut client = NetworkClient::connect_wss_only(server_addr)
                    .await
                    .expect("WSS connect failed");
                let welcome: DistributedMessage<TestId> = DistributedMessage::SecondaryWelcome {
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
                    sender_id: "sec-0".into(),
                    timestamp: 2.0,
                    secondary_id: "sec-0".into(),
                    public_cert_pem: "FAKE".into(),
                    ipv4_address: Some("127.0.0.1".into()),
                    ipv6_address: None,
                    quic_port: 5000,
                };
                MessageSender::send(&mut client, cert).await.unwrap();
                // Keep the connection (and thus the writer task) alive
                // until the test drops this handle.
                let _ = MessageReceiver::<DistributedMessage<TestId>>::recv(&mut client).await;
            });

            let first = transport.recv_peer().await.expect("welcome");
            assert!(
                matches!(first, DistributedMessage::SecondaryWelcome { .. }),
                "first frame must be the welcome, got {first:?}"
            );
            // The writer must be registered by the time the welcome
            // surfaces (the demux applied the registration). A
            // send_to_peer to sec-0 therefore finds a route.
            transport
                .send_to_peer(
                    "sec-0",
                    DistributedMessage::Keepalive {
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
                matches!(second, DistributedMessage::CertExchange { .. }),
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
            let server: NetworkServer = NetworkServer::bind::<TestId>(addr, inbound, registration)
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
            assert!(matches!(first, DistributedMessage::SecondaryWelcome { .. }));
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
            let server: NetworkServer = NetworkServer::bind::<TestId>(addr, inbound, registration)
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
