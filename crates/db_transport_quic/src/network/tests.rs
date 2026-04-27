use super::*;
use crate::wss::WssListener;
use db_comm_api_base::{MessageReceiver, MessageSender};
use db_primary_secondary_comm::{DistributedMessage, SecondaryTransport};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct TestId(String);

/// Test: WSS client connects to NetworkServer, sends a message, server
/// receives it and can send back via the registered connection.
#[tokio::test(flavor = "current_thread")]
async fn server_accepts_wss_bidirectional() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let mut server: NetworkServer<TestId> = NetworkServer::bind(addr).await.unwrap();
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
                    resources: vec![db_comm_api_base::ResourceAmount {
                        kind: db_comm_api_base::ResourceKind::memory(),
                        amount: 1024,
                    }],
                    worker_count: 1,
                    hostname: "test".into(),
                };
                MessageSender::send(&mut client, welcome).await.unwrap();

                let reply: DistributedMessage<TestId> =
                    MessageReceiver::recv(&mut client).await.expect("no reply");
                done_tx.send(()).ok();
                reply
            });

            let msg = server.recv().await.expect("no message received");
            assert_eq!(msg.sender_id(), "sec-0");

            tokio::time::sleep(Duration::from_millis(50)).await;
            server.drain_new_connections();

            let reply: DistributedMessage<TestId> = DistributedMessage::Keepalive {
                sender_id: "primary".into(),
                timestamp: 2.0,
                secondary_id: "primary".into(),
                active_workers: 0,
            };
            server.send_to("sec-0", reply).await.unwrap();

            done_rx.await.unwrap();
            let echoed = client_task.await.unwrap();
            assert_eq!(echoed.sender_id(), "primary");
        })
        .await;
}

/// Test: QUIC client connects to NetworkServer, sends and receives.
#[tokio::test(flavor = "current_thread")]
async fn server_accepts_quic_bidirectional() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let mut server: NetworkServer<TestId> = NetworkServer::bind(addr).await.unwrap();
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
                };
                MessageSender::send(&mut client, msg).await.unwrap();

                let reply: DistributedMessage<TestId> =
                    MessageReceiver::recv(&mut client).await.expect("no reply");
                done_tx.send(()).ok();
                reply
            });

            let msg = server.recv().await.expect("no message received");
            assert_eq!(msg.sender_id(), "sec-1");

            tokio::time::sleep(Duration::from_millis(50)).await;
            server.drain_new_connections();

            let reply: DistributedMessage<TestId> = DistributedMessage::Keepalive {
                sender_id: "primary".into(),
                timestamp: 3.0,
                secondary_id: "primary".into(),
                active_workers: 0,
            };
            server.send_to("sec-1", reply).await.unwrap();

            done_rx.await.unwrap();
            let echoed = client_task.await.unwrap();
            assert_eq!(echoed.sender_id(), "primary");
        })
        .await;
}

/// Test: NetworkClient falls back to WSS when QUIC is unavailable.
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
            };
            MessageSender::send(&mut client, msg).await.unwrap();

            let received = server_task.await.unwrap();
            assert_eq!(received.sender_id(), "fallback");
        })
        .await;
}
