//! Secondary-side network client: connects to a peer via QUIC, falling
//! back to WSS if QUIC fails.
//!
//! Implements `PrimaryTransport<I>` via the blanket impl in
//! `db_primary_secondary_comm::transport` (since it implements both
//! `MessageSender<DistributedMessage<I>>` and
//! `MessageReceiver<DistributedMessage<I>>`).

use std::net::SocketAddr;
use std::time::Duration;

use db_comm_api_base::{Identifier, MessageReceiver, MessageSender};
use db_primary_secondary_comm::DistributedMessage;

use crate::transport::QuicConnection;
use crate::wss::{WssConnection, connect_wss};

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
