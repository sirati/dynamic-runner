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

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{DistributedMessage, PeerConnectionInfo};
use tokio::sync::mpsc;

use crate::certs::CertPair;
use crate::transport::QuicListener;
use crate::wss::{WssListener, connect_wss};

mod accept;
mod either;
mod handler;
mod no_peer;
mod transport_impl;
mod util;

#[cfg(test)]
mod tests;

pub use either::EitherPeerTransport;
pub use no_peer::NoPeerTransport;
use util::{PeerConnection, parse_cert_pem};

/// A peer connection accepted by this node's server.
pub(super) struct AcceptedPeer<I: Identifier> {
    pub(super) peer_id: String,
    pub(super) outgoing_tx: mpsc::UnboundedSender<DistributedMessage<I>>,
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
    /// Sender side for accept loop AND per-peer outgoing-dial tasks
    /// (see `connect_to_peers`). Cloning this sender lets a spawned
    /// dial task hand off a successful connection through the same
    /// registration channel the accept loop uses, so callers don't
    /// have to await per-peer dials and miss tokio::select! tick
    /// budgets while connect_to_peers drains.
    new_conn_tx: mpsc::UnboundedSender<AcceptedPeer<I>>,
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
                accept::quic_accept_loop::<I>(quic_listener, incoming_tx, new_conn_tx).await;
            });
        }

        // Spawn WSS accept loop
        {
            let incoming_tx = incoming_tx.clone();
            let new_conn_tx = new_conn_tx.clone();
            tokio::task::spawn_local(async move {
                accept::wss_accept_loop::<I>(wss_listener, incoming_tx, new_conn_tx).await;
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
            new_conn_tx,
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

    /// Initiate connections to all peers from the peer list received
    /// from primary. **Non-blocking**: spawns one task per peer to do
    /// the actual QUIC/WSS dial, then returns immediately. Successful
    /// dials register through `new_conn_tx` (the same channel the
    /// accept loop uses for incoming connections); failed dials log
    /// and exit silently. Callers can observe completion via
    /// `peer_count()` (which calls `drain_new_connections` first) or
    /// by simply going on with their work — incoming peer messages
    /// route through `recv_peer` regardless.
    ///
    /// Why non-blocking: the previous shape (await each per-peer
    /// dial sequentially) blocked `wait_for_setup`'s `tokio::select!`
    /// for up to 10s × num_peers when the per-peer QUIC handshake
    /// timed out. That's fatal on clusters where compute nodes can't
    /// reach each other directly (most institutional SLURM setups —
    /// firewalled / NAT'd compute fabric): all peer dials hit their
    /// 10s timeout, the secondary's keepalive ticker can't fire from
    /// inside the blocked select arm, and the primary declares the
    /// secondary dead before peer-setup returns.
    pub fn connect_to_peers(&mut self, peers: &[PeerConnectionInfo]) {
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
                .unwrap_or("127.0.0.1")
                .to_string();
            let port = peer_info.port;
            let cert_pem = peer_info.cert.clone();
            let incoming_tx = self.incoming_tx.clone();
            let new_conn_tx = self.new_conn_tx.clone();

            tokio::task::spawn_local(async move {
                let addr: SocketAddr = match format!("{addr_str}:{port}").parse() {
                    Ok(a) => a,
                    Err(e) => {
                        tracing::error!(peer = %peer_id, error = %e, "invalid peer address");
                        return;
                    }
                };

                // Parse the peer's certificate PEM to get DER for QUIC verification
                let peer_cert_der = parse_cert_pem(&cert_pem);

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
                    Err(()) => match tokio::time::timeout(timeout, connect_wss(addr)).await {
                        Ok(Ok(conn)) => {
                            tracing::info!(peer = %peer_id, %addr, "connected to peer via WSS");
                            PeerConnection::Wss(conn)
                        }
                        Ok(Err(e)) => {
                            tracing::error!(peer = %peer_id, error = %e, "WSS to peer also failed");
                            return;
                        }
                        Err(_) => {
                            tracing::error!(peer = %peer_id, "WSS to peer timed out");
                            return;
                        }
                    },
                };

                let outgoing_tx = handler::spawn_outgoing_handler(
                    peer_id.clone(),
                    connection,
                    incoming_tx,
                );
                let _ = new_conn_tx.send(AcceptedPeer {
                    peer_id,
                    outgoing_tx,
                });
            });
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

}

