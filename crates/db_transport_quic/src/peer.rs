//! Peer-to-peer network transport between secondaries.
//!
//! Each secondary runs a [`PeerNetwork`] that:
//! 1. Starts a local QUIC+WSS server for incoming peer connections
//! 2. Connects to other peers using their cert/address from the PeerInfo message
//! 3. Broadcasts messages to all connected peers
//! 4. Receives messages from any peer into a single channel

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use db_comm_api_base::{Identifier, MessageReceiver};
use db_primary_secondary_comm::{
    codec, DistributedMessage, PeerConnectionInfo, PeerTransport,
};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use crate::certs::CertPair;
use crate::transport::{QuicConnection, QuicListener};
use crate::wss::{WssConnection, WssListener, connect_wss};

/// A peer connection accepted by this node's server.
struct AcceptedPeer<I: Identifier> {
    peer_id: String,
    outgoing_tx: mpsc::UnboundedSender<DistributedMessage<I>>,
}

/// Peer-to-peer network transport for secondary coordinators.
///
/// Manages bidirectional connections to all peer secondaries. Uses QUIC (UDP)
/// with WSS (TCP) fallback, same as the primary-secondary transport.
pub struct PeerNetwork<I: Identifier> {
    /// Our secondary ID.
    peer_id: String,
    /// Our certificate for QUIC server.
    cert: CertPair,
    /// The port we're listening on.
    port: u16,
    /// Per-peer outgoing channels, keyed by peer_id.
    connections: HashMap<String, mpsc::UnboundedSender<DistributedMessage<I>>>,
    /// Incoming messages from all peers.
    incoming_rx: mpsc::UnboundedReceiver<DistributedMessage<I>>,
    /// Sender side (kept for spawning new connection handlers).
    incoming_tx: mpsc::UnboundedSender<DistributedMessage<I>>,
    /// New connections from the accept loop that need registration.
    new_conn_rx: mpsc::UnboundedReceiver<AcceptedPeer<I>>,
    /// Sender side for accept loop (kept alive to avoid channel closure).
    _new_conn_tx: mpsc::UnboundedSender<AcceptedPeer<I>>,
}

impl<I: Identifier> PeerNetwork<I> {
    /// Create a new peer network: generate a certificate and start listening.
    pub async fn start(peer_id: &str) -> Result<Self, String> {
        let cert = CertPair::generate(peer_id)?;

        // Bind QUIC (UDP)
        let quic_listener = QuicListener::bind(&cert).await?;
        let port = quic_listener.port();

        // Bind WSS (TCP) on the same port
        let wss_addr: SocketAddr = format!("0.0.0.0:{port}").parse().unwrap();
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

        tracing::info!(peer_id, port, "peer network listening (QUIC/UDP + WSS/TCP)");

        Ok(Self {
            peer_id: peer_id.to_string(),
            cert,
            port,
            connections: HashMap::new(),
            incoming_rx,
            incoming_tx,
            new_conn_rx,
            _new_conn_tx: new_conn_tx,
        })
    }

    /// The port this peer network is listening on.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// The public certificate PEM for sharing with other peers.
    pub fn cert_pem(&self) -> &str {
        &self.cert.cert_pem
    }

    /// The certificate DER for QUIC client connections.
    pub fn cert_der(&self) -> &rustls::pki_types::CertificateDer<'static> {
        &self.cert.cert_der
    }

    /// Connect to all peers from the peer list received from primary.
    ///
    /// Skips our own ID. For each peer, tries QUIC first, then falls back
    /// to WSS. Spawns background reader/writer tasks for each connection.
    pub async fn connect_to_peers(&mut self, peers: &[PeerConnectionInfo]) {
        for peer_info in peers {
            if peer_info.secondary_id == self.peer_id {
                continue; // Skip self
            }

            if self.connections.contains_key(&peer_info.secondary_id) {
                continue; // Already connected (from accept loop)
            }

            let peer_id = peer_info.secondary_id.clone();
            let addr_str = peer_info
                .ipv4
                .as_deref()
                .unwrap_or("127.0.0.1");
            let addr: SocketAddr = match format!("{addr_str}:{}", peer_info.port).parse() {
                Ok(a) => a,
                Err(e) => {
                    tracing::error!(peer = %peer_id, error = %e, "invalid peer address");
                    continue;
                }
            };

            // Parse the peer's certificate PEM to get DER for QUIC verification
            let peer_cert_der = parse_cert_pem(&peer_info.cert);

            let timeout = Duration::from_secs(10);

            // Try QUIC first, fall back to WSS
            let connection_result = if let Some(cert_der) = &peer_cert_der {
                match tokio::time::timeout(
                    timeout,
                    crate::transport::connect(addr, &peer_id, cert_der),
                )
                .await
                {
                    Ok(Ok(conn)) => {
                        tracing::info!(peer = %peer_id, %addr, "connected to peer via QUIC");
                        Ok(PeerConnection::Quic(conn))
                    }
                    Ok(Err(e)) => {
                        tracing::warn!(peer = %peer_id, error = %e, "QUIC to peer failed, trying WSS");
                        Err(())
                    }
                    Err(_) => {
                        tracing::warn!(peer = %peer_id, "QUIC to peer timed out, trying WSS");
                        Err(())
                    }
                }
            } else {
                tracing::warn!(peer = %peer_id, "no valid cert for peer, trying WSS");
                Err(())
            };

            let connection = match connection_result {
                Ok(conn) => conn,
                Err(()) => {
                    // Fallback to WSS
                    match tokio::time::timeout(timeout, connect_wss(addr)).await {
                        Ok(Ok(conn)) => {
                            tracing::info!(peer = %peer_id, %addr, "connected to peer via WSS");
                            PeerConnection::Wss(conn)
                        }
                        Ok(Err(e)) => {
                            tracing::error!(peer = %peer_id, error = %e, "WSS to peer also failed");
                            continue;
                        }
                        Err(_) => {
                            tracing::error!(peer = %peer_id, "WSS to peer timed out");
                            continue;
                        }
                    }
                }
            };

            // Set up reader/writer for this outgoing connection
            let outgoing_tx = self.spawn_connection_handler(peer_id.clone(), connection);
            self.connections.insert(peer_id, outgoing_tx);
        }
    }

    /// Drain any newly accepted incoming connections and register them.
    fn drain_new_connections(&mut self) {
        while let Ok(accepted) = self.new_conn_rx.try_recv() {
            if !self.connections.contains_key(&accepted.peer_id) {
                tracing::info!(peer = %accepted.peer_id, "incoming peer registered");
                self.connections
                    .insert(accepted.peer_id, accepted.outgoing_tx);
            }
        }
    }

    /// Spawn reader/writer tasks for a connection (either outgoing or accepted).
    /// Returns the outgoing sender channel.
    fn spawn_connection_handler(
        &self,
        peer_id: String,
        connection: PeerConnection,
    ) -> mpsc::UnboundedSender<DistributedMessage<I>> {
        let (outgoing_tx, mut outgoing_rx) = mpsc::unbounded_channel::<DistributedMessage<I>>();

        match connection {
            PeerConnection::Quic(conn) => {
                let (send_stream, recv_stream, existing_buf) = conn.into_parts();
                let incoming_tx = self.incoming_tx.clone();
                let reader_id = peer_id.clone();

                // Reader
                let mut reader = tokio::task::spawn_local(async move {
                    let mut recv_buf = existing_buf;
                    let mut recv = recv_stream;
                    loop {
                        match codec::decode_frame::<I>(&recv_buf) {
                            Ok(Some((msg, consumed))) => {
                                recv_buf.drain(..consumed);
                                if incoming_tx.send(msg).is_err() {
                                    break;
                                }
                            }
                            Ok(None) => {
                                let mut tmp = [0u8; 8192];
                                match recv.read(&mut tmp).await {
                                    Ok(Some(n)) => recv_buf.extend_from_slice(&tmp[..n]),
                                    _ => break,
                                }
                            }
                            Err(_) => break,
                        }
                    }
                    tracing::debug!(peer = %reader_id, "peer QUIC reader done");
                });

                // Writer
                let writer_id = peer_id;
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
                    tracing::debug!(peer = %writer_id, "peer QUIC writer done");
                });

                tokio::task::spawn_local(async move {
                    tokio::select! {
                        _ = &mut reader => { writer.abort(); }
                        _ = &mut writer => { reader.abort(); }
                    }
                });
            }
            PeerConnection::Wss(conn) => {
                let (mut ws_write, mut ws_read) = conn.into_inner().split();
                let incoming_tx = self.incoming_tx.clone();
                let reader_id = peer_id.clone();

                // Reader
                let mut reader = tokio::task::spawn_local(async move {
                    loop {
                        match ws_read.next().await {
                            Some(Ok(Message::Binary(data))) => {
                                match codec::decode_frame::<I>(&data) {
                                    Ok(Some((msg, _))) => {
                                        if incoming_tx.send(msg).is_err() {
                                            break;
                                        }
                                    }
                                    Ok(None) => continue,
                                    Err(_) => break,
                                }
                            }
                            Some(Ok(Message::Close(_))) | None => break,
                            Some(Ok(_)) => continue,
                            Some(Err(_)) => break,
                        }
                    }
                    tracing::debug!(peer = %reader_id, "peer WSS reader done");
                });

                // Writer
                let writer_id = peer_id;
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
                    tracing::debug!(peer = %writer_id, "peer WSS writer done");
                });

                tokio::task::spawn_local(async move {
                    tokio::select! {
                        _ = &mut reader => { writer.abort(); }
                        _ = &mut writer => { reader.abort(); }
                    }
                });
            }
        }

        outgoing_tx
    }

    // ── Accept loops ──

    async fn quic_accept_loop(
        listener: QuicListener,
        incoming_tx: mpsc::UnboundedSender<DistributedMessage<I>>,
        new_conn_tx: mpsc::UnboundedSender<AcceptedPeer<I>>,
    ) {
        loop {
            match listener.accept().await {
                Ok(conn) => {
                    let incoming_tx = incoming_tx.clone();
                    let new_conn_tx = new_conn_tx.clone();
                    tokio::task::spawn_local(async move {
                        Self::handle_accepted_quic(conn, incoming_tx, new_conn_tx).await;
                    });
                }
                Err(e) => {
                    tracing::debug!(error = %e, "peer QUIC accept loop ended");
                    break;
                }
            }
        }
    }

    async fn wss_accept_loop(
        listener: WssListener,
        incoming_tx: mpsc::UnboundedSender<DistributedMessage<I>>,
        new_conn_tx: mpsc::UnboundedSender<AcceptedPeer<I>>,
    ) {
        loop {
            match listener.accept().await {
                Ok(conn) => {
                    let incoming_tx = incoming_tx.clone();
                    let new_conn_tx = new_conn_tx.clone();
                    tokio::task::spawn_local(async move {
                        Self::handle_accepted_wss(conn, incoming_tx, new_conn_tx).await;
                    });
                }
                Err(e) => {
                    tracing::debug!(error = %e, "peer WSS accept loop ended");
                    break;
                }
            }
        }
    }

    async fn handle_accepted_quic(
        mut conn: QuicConnection,
        incoming_tx: mpsc::UnboundedSender<DistributedMessage<I>>,
        new_conn_tx: mpsc::UnboundedSender<AcceptedPeer<I>>,
    ) {
        // Read first message to identify peer
        let first_msg = match MessageReceiver::<DistributedMessage<I>>::recv(&mut conn).await {
            Some(msg) => msg,
            None => return,
        };
        let peer_id = first_msg.sender_id().to_string();

        if incoming_tx.send(first_msg).is_err() {
            return;
        }

        let (outgoing_tx, mut outgoing_rx) = mpsc::unbounded_channel::<DistributedMessage<I>>();

        if new_conn_tx
            .send(AcceptedPeer {
                peer_id: peer_id.clone(),
                outgoing_tx,
            })
            .is_err()
        {
            return;
        }

        let (send_stream, recv_stream, existing_buf) = conn.into_parts();

        let reader_tx = incoming_tx;
        let reader_id = peer_id.clone();
        let mut reader = tokio::task::spawn_local(async move {
            let mut recv_buf = existing_buf;
            let mut recv = recv_stream;
            loop {
                match codec::decode_frame::<I>(&recv_buf) {
                    Ok(Some((msg, consumed))) => {
                        recv_buf.drain(..consumed);
                        if reader_tx.send(msg).is_err() {
                            break;
                        }
                    }
                    Ok(None) => {
                        let mut tmp = [0u8; 8192];
                        match recv.read(&mut tmp).await {
                            Ok(Some(n)) => recv_buf.extend_from_slice(&tmp[..n]),
                            _ => break,
                        }
                    }
                    Err(_) => break,
                }
            }
            tracing::debug!(peer = %reader_id, "accepted peer QUIC reader done");
        });

        let writer_id = peer_id;
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
            tracing::debug!(peer = %writer_id, "accepted peer QUIC writer done");
        });

        tokio::select! {
            _ = &mut reader => { writer.abort(); }
            _ = &mut writer => { reader.abort(); }
        }
    }

    async fn handle_accepted_wss(
        mut conn: WssConnection,
        incoming_tx: mpsc::UnboundedSender<DistributedMessage<I>>,
        new_conn_tx: mpsc::UnboundedSender<AcceptedPeer<I>>,
    ) {
        // Read first message to identify peer
        let first_msg = match MessageReceiver::<DistributedMessage<I>>::recv(&mut conn).await {
            Some(msg) => msg,
            None => return,
        };
        let peer_id = first_msg.sender_id().to_string();

        if incoming_tx.send(first_msg).is_err() {
            return;
        }

        let (outgoing_tx, mut outgoing_rx) = mpsc::unbounded_channel::<DistributedMessage<I>>();

        if new_conn_tx
            .send(AcceptedPeer {
                peer_id: peer_id.clone(),
                outgoing_tx,
            })
            .is_err()
        {
            return;
        }

        let (mut ws_write, mut ws_read) = conn.into_inner().split();

        let reader_tx = incoming_tx;
        let reader_id = peer_id.clone();
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
                    Some(Ok(_)) => continue,
                    Some(Err(_)) => break,
                }
            }
            tracing::debug!(peer = %reader_id, "accepted peer WSS reader done");
        });

        let writer_id = peer_id;
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
            tracing::debug!(peer = %writer_id, "accepted peer WSS writer done");
        });

        tokio::select! {
            _ = &mut reader => { writer.abort(); }
            _ = &mut writer => { reader.abort(); }
        }
    }
}

impl<I: Identifier> PeerTransport<I> for PeerNetwork<I> {
    async fn broadcast(&mut self, msg: DistributedMessage<I>) -> Result<(), String> {
        self.drain_new_connections();
        let mut errors = Vec::new();
        for (peer_id, tx) in &self.connections {
            if tx.send(msg.clone()).is_err() {
                errors.push(peer_id.clone());
            }
        }
        for peer_id in &errors {
            self.connections.remove(peer_id);
            tracing::warn!(peer = %peer_id, "peer disconnected during broadcast");
        }
        Ok(())
    }

    async fn send_to_peer(
        &mut self,
        peer_id: &str,
        msg: DistributedMessage<I>,
    ) -> Result<(), String> {
        self.drain_new_connections();
        if let Some(tx) = self.connections.get(peer_id) {
            tx.send(msg).map_err(|e| e.to_string())
        } else {
            Err(format!("no connection to peer '{peer_id}'"))
        }
    }

    async fn recv_peer(&mut self) -> Option<DistributedMessage<I>> {
        self.drain_new_connections();
        loop {
            tokio::select! {
                msg = self.incoming_rx.recv() => {
                    self.drain_new_connections();
                    return msg;
                }
                accepted = self.new_conn_rx.recv() => {
                    if let Some(accepted) = accepted {
                        if !self.connections.contains_key(&accepted.peer_id) {
                            tracing::info!(peer = %accepted.peer_id, "incoming peer registered (during recv)");
                            self.connections.insert(accepted.peer_id, accepted.outgoing_tx);
                        }
                    }
                }
            }
        }
    }

    fn try_recv_peer(&mut self) -> Option<DistributedMessage<I>> {
        self.drain_new_connections();
        self.incoming_rx.try_recv().ok()
    }

    fn peer_count(&self) -> usize {
        self.connections.len()
    }

    async fn connect_to_peers(&mut self, peers: &[PeerConnectionInfo]) {
        // Delegate to the inherent method
        PeerNetwork::connect_to_peers(self, peers).await;
    }
}

/// Internal enum for either QUIC or WSS peer connection.
enum PeerConnection {
    Quic(QuicConnection),
    Wss(WssConnection),
}

/// Parse a PEM certificate string to get the DER-encoded certificate.
fn parse_cert_pem(pem: &str) -> Option<rustls::pki_types::CertificateDer<'static>> {
    if pem.is_empty() {
        return None;
    }
    // Simple PEM parser: extract base64 between BEGIN/END markers
    let mut in_cert = false;
    let mut b64 = String::new();
    for line in pem.lines() {
        if line.contains("BEGIN CERTIFICATE") {
            in_cert = true;
            continue;
        }
        if line.contains("END CERTIFICATE") {
            break;
        }
        if in_cert {
            b64.push_str(line.trim());
        }
    }
    if b64.is_empty() {
        return None;
    }
    use base64::Engine;
    let der = base64::engine::general_purpose::STANDARD.decode(&b64).ok()?;
    Some(rustls::pki_types::CertificateDer::from(der))
}

/// A no-op peer transport for when peer-to-peer is not needed (single secondary mode).
pub struct NoPeerTransport;

impl<I: Identifier> PeerTransport<I> for NoPeerTransport {
    async fn broadcast(&mut self, _msg: DistributedMessage<I>) -> Result<(), String> {
        Ok(())
    }

    async fn send_to_peer(
        &mut self,
        _peer_id: &str,
        _msg: DistributedMessage<I>,
    ) -> Result<(), String> {
        Ok(())
    }

    async fn recv_peer(&mut self) -> Option<DistributedMessage<I>> {
        // Never returns — no peers
        std::future::pending().await
    }

    fn try_recv_peer(&mut self) -> Option<DistributedMessage<I>> {
        None
    }

    fn peer_count(&self) -> usize {
        0
    }

    async fn connect_to_peers(&mut self, _peers: &[PeerConnectionInfo]) {
        // No-op
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
    struct TestId(String);

    #[test]
    fn parse_cert_pem_works() {
        let cert = CertPair::generate("test").unwrap();
        let der = parse_cert_pem(&cert.cert_pem);
        assert!(der.is_some());
        assert_eq!(der.unwrap().as_ref(), cert.cert_der.as_ref());
    }

    #[test]
    fn parse_cert_pem_empty_returns_none() {
        assert!(parse_cert_pem("").is_none());
        assert!(parse_cert_pem("not a cert").is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn two_peers_exchange_messages() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                // Start two peer networks
                let mut peer_a: PeerNetwork<TestId> =
                    PeerNetwork::start("peer-a").await.unwrap();
                let mut peer_b: PeerNetwork<TestId> =
                    PeerNetwork::start("peer-b").await.unwrap();

                let port_a = peer_a.port();
                let port_b = peer_b.port();
                let cert_pem_a = peer_a.cert_pem().to_string();
                let cert_pem_b = peer_b.cert_pem().to_string();

                // Create peer info for both
                let peers = vec![
                    PeerConnectionInfo {
                        secondary_id: "peer-a".into(),
                        cert: cert_pem_a,
                        ipv4: Some("127.0.0.1".into()),
                        ipv6: None,
                        port: port_a,
                    },
                    PeerConnectionInfo {
                        secondary_id: "peer-b".into(),
                        cert: cert_pem_b,
                        ipv4: Some("127.0.0.1".into()),
                        ipv6: None,
                        port: port_b,
                    },
                ];

                // Each peer connects to the other
                peer_a.connect_to_peers(&peers).await;
                peer_b.connect_to_peers(&peers).await;

                // Give accept loops time to register incoming connections
                tokio::time::sleep(Duration::from_millis(100)).await;
                peer_a.drain_new_connections();
                peer_b.drain_new_connections();

                // Peer A broadcasts a message
                let msg: DistributedMessage<TestId> = DistributedMessage::Keepalive {
                    sender_id: "peer-a".into(),
                    timestamp: 1.0,
                    secondary_id: "peer-a".into(),
                    active_workers: 2,
                };
                peer_a.broadcast(msg).await.unwrap();

                // Peer B should receive it
                let received = tokio::time::timeout(
                    Duration::from_secs(5),
                    peer_b.recv_peer(),
                )
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
}
