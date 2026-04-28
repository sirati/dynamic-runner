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

use db_comm_api_base::Identifier;
use db_primary_secondary_comm::DistributedMessage;
use tokio::sync::mpsc;

use crate::certs::CertPair;
use crate::transport::QuicListener;
use crate::wss::WssListener;

mod accept;
mod client;
mod transport_impl;

#[cfg(test)]
mod tests;

pub use client::NetworkClient;

/// A new connection accepted by the server: the secondary_id (from the first
/// message) and a channel for sending messages back through this connection.
pub(super) struct AcceptedConnection<I: Identifier> {
    pub(super) secondary_id: String,
    pub(super) outgoing_tx: mpsc::UnboundedSender<DistributedMessage<I>>,
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

        // Bind QUIC (UDP) on the requested address. If the caller
        // passed port 0 the OS picks; if they passed a fixed port we
        // honour it (so a primary that already published a URL to
        // its secondaries can bind to that exact port).
        let quic_listener = QuicListener::bind_addr(&cert, addr).await?;
        let port = quic_listener.port();

        // Bind WSS (TCP) on the same port. Use the QUIC-resolved port
        // (which equals the requested port when non-zero, or the
        // OS-assigned port when zero) so both protocols match.
        let wss_addr = SocketAddr::new(addr.ip(), port);
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
}

