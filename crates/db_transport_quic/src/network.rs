//! Multiplexing network transport: QUIC (preferred) + WSS (fallback).
//!
//! Provides two entry points:
//!
//! - [`NetworkServer`] — primary-side transport. Listens on a single port for
//!   both QUIC (UDP) and WSS (TCP) connections from secondaries. Implements
//!   [`SecondaryTransport<I>`] for use with `PrimaryCoordinator`.
//!
//! - [`NetworkClient`] — secondary-side transport. Connects to a peer (primary
//!   or another secondary) via QUIC, falling back to WSS if QUIC fails.
//!   Implements `PrimaryTransport<I>` via the blanket impl.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use db_comm_api_base::{Identifier, MessageReceiver, MessageSender};
use db_primary_secondary_comm::{DistributedMessage, SecondaryTransport, codec};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use crate::certs::CertPair;
use crate::transport::{QuicConnection, QuicListener};
use crate::wss::{WssConnection, WssListener, connect_wss};

/// A new connection accepted by the server: the secondary_id (from the first
/// message) and a channel for sending messages back through this connection.
struct AcceptedConnection<I: Identifier> {
    secondary_id: String,
    outgoing_tx: mpsc::UnboundedSender<DistributedMessage<I>>,
}

/// Primary-side network transport that accepts QUIC and WSS connections.
///
/// Runs background accept loops for both QUIC (UDP) and WSS (TCP) on the same
/// port number. Incoming messages from all secondaries are funneled into a
/// single `mpsc` channel. Outgoing messages are routed by `secondary_id`.
pub struct NetworkServer<I: Identifier> {
    /// Per-secondary outgoing channels, keyed by secondary_id.
    connections: HashMap<String, mpsc::UnboundedSender<DistributedMessage<I>>>,
    /// Incoming messages from all secondaries.
    incoming_rx: mpsc::UnboundedReceiver<DistributedMessage<I>>,
    /// New connections that need to be registered (from accept loops).
    new_conn_rx: mpsc::UnboundedReceiver<AcceptedConnection<I>>,
    /// Port the server is listening on (same for QUIC/UDP and WSS/TCP).
    port: u16,
    /// Our cert pair for QUIC.
    cert: CertPair,
}

impl<I: Identifier> NetworkServer<I> {
    /// Start listening on `addr` for both QUIC and WSS.
    ///
    /// Uses the same port number for both protocols (QUIC on UDP, WSS on TCP).
    /// If `addr` uses port 0, an OS-allocated port is used.
    pub async fn bind(addr: SocketAddr) -> Result<Self, String> {
        let cert = CertPair::generate("primary")?;

        // Bind QUIC (UDP) first to get the actual port
        let quic_listener = QuicListener::bind(&cert).await?;
        let port = quic_listener.port();

        // Bind WSS (TCP) on the same port
        let wss_addr = SocketAddr::new(addr.ip(), port);
        let wss_listener = WssListener::bind(wss_addr).await?;

        let (incoming_tx, incoming_rx) = mpsc::unbounded_channel();
        let (new_conn_tx, new_conn_rx) = mpsc::unbounded_channel();

        // Spawn QUIC accept loop
        {
            let incoming_tx = incoming_tx.clone();
            let new_conn_tx = new_conn_tx.clone();
            tokio::task::spawn_local(async move {
                Self::quic_accept_loop(quic_listener, incoming_tx, new_conn_tx).await;
            });
        }

        // Spawn WSS accept loop
        {
            let incoming_tx = incoming_tx.clone();
            let new_conn_tx = new_conn_tx.clone();
            tokio::task::spawn_local(async move {
                Self::wss_accept_loop(wss_listener, incoming_tx, new_conn_tx).await;
            });
        }

        tracing::info!(port, "network server listening (QUIC/UDP + WSS/TCP)");

        Ok(Self {
            connections: HashMap::new(),
            incoming_rx,
            new_conn_rx,
            port,
            cert,
        })
    }

    /// The port the server is listening on.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// The server's certificate DER (for clients to verify QUIC connections).
    pub fn cert_der(&self) -> &rustls::pki_types::CertificateDer<'static> {
        &self.cert.cert_der
    }

    /// The server's public certificate PEM (for distribution to secondaries).
    pub fn cert_pem(&self) -> &str {
        &self.cert.cert_pem
    }

    /// Drain new connections from the accept loops and register them.
    fn drain_new_connections(&mut self) {
        while let Ok(accepted) = self.new_conn_rx.try_recv() {
            tracing::info!(secondary = %accepted.secondary_id, "secondary registered");
            self.connections
                .insert(accepted.secondary_id.clone(), accepted.outgoing_tx);
        }
    }

    /// QUIC accept loop.
    async fn quic_accept_loop(
        listener: QuicListener,
        incoming_tx: mpsc::UnboundedSender<DistributedMessage<I>>,
        new_conn_tx: mpsc::UnboundedSender<AcceptedConnection<I>>,
    ) {
        loop {
            match listener.accept().await {
                Ok(conn) => {
                    let incoming_tx = incoming_tx.clone();
                    let new_conn_tx = new_conn_tx.clone();
                    tokio::task::spawn_local(async move {
                        Self::handle_new_quic_connection(conn, incoming_tx, new_conn_tx).await;
                    });
                }
                Err(e) => {
                    tracing::error!(error = %e, "QUIC accept error");
                    break;
                }
            }
        }
    }

    /// WSS accept loop.
    async fn wss_accept_loop(
        listener: WssListener,
        incoming_tx: mpsc::UnboundedSender<DistributedMessage<I>>,
        new_conn_tx: mpsc::UnboundedSender<AcceptedConnection<I>>,
    ) {
        loop {
            match listener.accept().await {
                Ok(conn) => {
                    let incoming_tx = incoming_tx.clone();
                    let new_conn_tx = new_conn_tx.clone();
                    tokio::task::spawn_local(async move {
                        Self::handle_new_wss_connection(conn, incoming_tx, new_conn_tx).await;
                    });
                }
                Err(e) => {
                    tracing::error!(error = %e, "WSS accept error");
                    break;
                }
            }
        }
    }

    /// Handle a new QUIC connection: read first message to identify secondary,
    /// then split into separate reader/writer tasks.
    async fn handle_new_quic_connection(
        mut conn: QuicConnection,
        incoming_tx: mpsc::UnboundedSender<DistributedMessage<I>>,
        new_conn_tx: mpsc::UnboundedSender<AcceptedConnection<I>>,
    ) {
        // Read first message to identify the secondary
        let first_msg = match MessageReceiver::<DistributedMessage<I>>::recv(&mut conn).await {
            Some(msg) => msg,
            None => return,
        };
        let secondary_id = first_msg.sender_id().to_string();

        // Forward first message
        if incoming_tx.send(first_msg).is_err() {
            return;
        }

        // Create per-connection write channel
        let (outgoing_tx, mut outgoing_rx) = mpsc::unbounded_channel::<DistributedMessage<I>>();

        // Register
        if new_conn_tx
            .send(AcceptedConnection {
                secondary_id: secondary_id.clone(),
                outgoing_tx,
            })
            .is_err()
        {
            return;
        }

        // QUIC has separate send/recv streams, so we can split safely.
        // Extract the inner streams along with any already-buffered data.
        let (send_stream, recv_stream, existing_buf) = conn.into_parts();

        // Reader task: read from QUIC recv stream, forward to incoming
        let reader_tx = incoming_tx;
        let reader_id = secondary_id.clone();
        let mut reader = tokio::task::spawn_local(async move {
            let mut recv_buf = existing_buf;
            let mut recv = recv_stream;
            loop {
                // Try to decode a complete frame from the buffer
                match codec::decode_frame::<I>(&recv_buf) {
                    Ok(Some((msg, consumed))) => {
                        recv_buf.drain(..consumed);
                        if reader_tx.send(msg).is_err() {
                            break;
                        }
                    }
                    Ok(None) => {
                        // Need more data
                        let mut tmp = [0u8; 8192];
                        match recv.read(&mut tmp).await {
                            Ok(Some(n)) => recv_buf.extend_from_slice(&tmp[..n]),
                            _ => break,
                        }
                    }
                    Err(_) => break,
                }
            }
            tracing::debug!(secondary = %reader_id, "QUIC reader done");
        });

        // Writer task: drain outgoing channel, write to QUIC send stream
        let writer_id = secondary_id;
        let mut writer = tokio::task::spawn_local(async move {
            let mut send = send_stream;
            while let Some(msg) = outgoing_rx.recv().await {
                match codec::serialize_message(&msg) {
                    Ok(frame) => {
                        if send.write_all(&frame).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
            tracing::debug!(secondary = %writer_id, "QUIC writer done");
        });

        // Wait for either task to finish, then abort the other
        tokio::select! {
            _ = &mut reader => { writer.abort(); }
            _ = &mut writer => { reader.abort(); }
        }
    }

    /// Handle a new WSS connection: read first message to identify secondary,
    /// then split the WebSocket stream into reader/writer halves.
    async fn handle_new_wss_connection(
        mut conn: WssConnection,
        incoming_tx: mpsc::UnboundedSender<DistributedMessage<I>>,
        new_conn_tx: mpsc::UnboundedSender<AcceptedConnection<I>>,
    ) {
        // Read first message to identify the secondary
        let first_msg = match MessageReceiver::<DistributedMessage<I>>::recv(&mut conn).await {
            Some(msg) => msg,
            None => return,
        };
        let secondary_id = first_msg.sender_id().to_string();

        // Forward first message
        if incoming_tx.send(first_msg).is_err() {
            return;
        }

        // Create per-connection write channel
        let (outgoing_tx, mut outgoing_rx) = mpsc::unbounded_channel::<DistributedMessage<I>>();

        // Register
        if new_conn_tx
            .send(AcceptedConnection {
                secondary_id: secondary_id.clone(),
                outgoing_tx,
            })
            .is_err()
        {
            return;
        }

        // Split the WebSocket stream into independent read/write halves
        let (mut ws_write, mut ws_read) = conn.into_inner().split();

        // Reader task: read from WebSocket, decode, forward to incoming
        let reader_tx = incoming_tx;
        let reader_id = secondary_id.clone();
        let mut reader = tokio::task::spawn_local(async move {
            loop {
                match ws_read.next().await {
                    Some(Ok(Message::Binary(data))) => {
                        match codec::decode_frame::<I>(&data) {
                            Ok(Some((msg, _))) => {
                                if reader_tx.send(msg).is_err() {
                                    break;
                                }
                            }
                            Ok(None) => continue,
                            Err(_) => break,
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => continue, // skip ping/pong/text
                    Some(Err(_)) => break,
                }
            }
            tracing::debug!(secondary = %reader_id, "WSS reader done");
        });

        // Writer task: drain outgoing channel, write to WebSocket
        let writer_id = secondary_id;
        let mut writer = tokio::task::spawn_local(async move {
            while let Some(msg) = outgoing_rx.recv().await {
                match codec::serialize_message(&msg) {
                    Ok(frame) => {
                        if ws_write.send(Message::Binary(frame.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
            tracing::debug!(secondary = %writer_id, "WSS writer done");
        });

        // Wait for either task to finish, then abort the other
        tokio::select! {
            _ = &mut reader => { writer.abort(); }
            _ = &mut writer => { reader.abort(); }
        }
    }
}

impl<I: Identifier> MessageReceiver<DistributedMessage<I>> for NetworkServer<I> {
    async fn recv(&mut self) -> Option<DistributedMessage<I>> {
        // Drain any new connections before checking for messages
        self.drain_new_connections();

        // Use select to also drain new connections that arrive while waiting
        loop {
            tokio::select! {
                msg = self.incoming_rx.recv() => {
                    self.drain_new_connections();
                    return msg;
                }
                accepted = self.new_conn_rx.recv() => {
                    if let Some(accepted) = accepted {
                        tracing::info!(secondary = %accepted.secondary_id, "secondary registered (during recv)");
                        self.connections.insert(accepted.secondary_id, accepted.outgoing_tx);
                    }
                }
            }
        }
    }
}

impl<I: Identifier> SecondaryTransport<I> for NetworkServer<I> {
    async fn send_to(
        &mut self,
        secondary_id: &str,
        msg: DistributedMessage<I>,
    ) -> Result<(), String> {
        // Drain any pending new connections first
        self.drain_new_connections();

        if let Some(tx) = self.connections.get(secondary_id) {
            tx.send(msg).map_err(|e| e.to_string())
        } else {
            Err(format!("no connection for secondary '{secondary_id}'"))
        }
    }
}

/// Secondary-side network client: connects to a peer via QUIC, falling back
/// to WSS if QUIC fails.
///
/// Implements `PrimaryTransport<I>` via the blanket impl (since it implements
/// both `MessageSender<DistributedMessage<I>>` and `MessageReceiver<DistributedMessage<I>>`).
pub enum NetworkClient {
    Quic(QuicConnection),
    Wss(WssConnection),
}

impl NetworkClient {
    /// Connect to `addr` using QUIC (with `peer_cert` for TLS verification),
    /// falling back to WSS if QUIC fails within `timeout`.
    pub async fn connect(
        addr: SocketAddr,
        server_name: &str,
        peer_cert: &rustls::pki_types::CertificateDer<'_>,
        timeout: Duration,
    ) -> Result<Self, String> {
        // Try QUIC first
        match tokio::time::timeout(timeout, crate::transport::connect(addr, server_name, peer_cert))
            .await
        {
            Ok(Ok(conn)) => {
                tracing::info!(%addr, "connected via QUIC (UDP)");
                return Ok(NetworkClient::Quic(conn));
            }
            Ok(Err(e)) => {
                tracing::warn!(%addr, error = %e, "QUIC failed, falling back to WSS");
            }
            Err(_) => {
                tracing::warn!(%addr, "QUIC timed out, falling back to WSS");
            }
        }

        // Fallback to WSS
        match tokio::time::timeout(timeout, connect_wss(addr)).await {
            Ok(Ok(conn)) => {
                tracing::info!(%addr, "connected via WSS (TCP)");
                Ok(NetworkClient::Wss(conn))
            }
            Ok(Err(e)) => Err(format!("both QUIC and WSS failed for {addr}: WSS error: {e}")),
            Err(_) => Err(format!("both QUIC and WSS timed out for {addr}")),
        }
    }

    /// Connect using WSS only (no QUIC attempt).
    pub async fn connect_wss_only(addr: SocketAddr) -> Result<Self, String> {
        let conn = connect_wss(addr).await?;
        tracing::info!(%addr, "connected via WSS (TCP)");
        Ok(NetworkClient::Wss(conn))
    }
}

impl<I: Identifier> MessageSender<DistributedMessage<I>> for NetworkClient {
    async fn send(&mut self, msg: DistributedMessage<I>) -> Result<(), String> {
        match self {
            NetworkClient::Quic(c) => c.send(msg).await,
            NetworkClient::Wss(c) => c.send(msg).await,
        }
    }
}

impl<I: Identifier> MessageReceiver<DistributedMessage<I>> for NetworkClient {
    async fn recv(&mut self) -> Option<DistributedMessage<I>> {
        match self {
            NetworkClient::Quic(c) => MessageReceiver::<DistributedMessage<I>>::recv(c).await,
            NetworkClient::Wss(c) => MessageReceiver::<DistributedMessage<I>>::recv(c).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
    struct TestId(String);

    /// Test: WSS client connects to NetworkServer, sends a message, server
    /// receives it and can send back via the registered connection.
    #[tokio::test(flavor = "current_thread")]
    async fn server_accepts_wss_bidirectional() {
        let local = tokio::task::LocalSet::new();
        local.run_until(async {
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
                    ram_bytes: 1024,
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
        }).await;
    }

    /// Test: QUIC client connects to NetworkServer, sends and receives.
    #[tokio::test(flavor = "current_thread")]
    async fn server_accepts_quic_bidirectional() {
        let local = tokio::task::LocalSet::new();
        local.run_until(async {
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
        }).await;
    }

    /// Test: NetworkClient falls back to WSS when QUIC is unavailable.
    #[tokio::test(flavor = "current_thread")]
    async fn client_falls_back_to_wss() {
        let local = tokio::task::LocalSet::new();
        local.run_until(async {
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
        }).await;
    }
}
