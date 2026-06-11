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
//!   Carries the bidirectional `MessageSender<DistributedMessage<I>> +
//!   MessageReceiver<DistributedMessage<I>>` shape (pre-Step-11 this
//!   was satisfied via a marker trait `PrimaryTransport<I>`; the
//!   trait retired, the underlying contract is unchanged).

use std::net::SocketAddr;

use dynrunner_core::Identifier;
use dynrunner_transport_tunnel::{InboundTap, RegistrationSink};

use crate::certs::CertPair;

mod accept;
mod client;

#[cfg(test)]
mod tests;

pub use client::NetworkClient;

/// Primary-side network listener that accepts QUIC and WSS connections.
///
/// Runs background accept loops for both QUIC (UDP) and WSS (TCP) on the
/// same port number. The accept loops feed the unified
/// [`dynrunner_transport_tunnel::TunneledPeerTransport`] directly: every
/// inbound frame is pushed into the transport's inbound sink, and every
/// handshaked secondary's writer is pushed into the transport's
/// registration sink. The transport's `recv_peer` owns the inbound
/// demux + writer-table population; this type's only remaining
/// responsibilities are bind + accept-loops + handing the cert/port to
/// callers that publish the connection URL to secondaries.
///
/// The accept loops are spawned (`spawn_local`) at [`bind`] time and
/// own the QUIC/WSS listeners outright, so they keep running for the
/// lifetime of the `LocalSet` even after this handle is dropped — the
/// caller keeps it only for `cert_der` / `cert_pem` / `port`.
pub struct NetworkServer {
    /// Port the server is listening on (same for QUIC/UDP and WSS/TCP).
    port: u16,
    /// Our cert pair for QUIC.
    cert: CertPair,
}

impl NetworkServer {
    /// Start listening on `addr` for both QUIC and WSS, feeding the
    /// unified transport's `inbound` + `registration` sinks.
    ///
    /// Uses the same port number for both protocols (QUIC on UDP, WSS on
    /// TCP). If `addr` uses port 0, an OS-allocated port is used.
    ///
    /// `inbound` is the [`TunneledPeerTransport`]'s inbound sink (the
    /// accept loops' reader tasks push every frame into it);
    /// `registration` is its registration sink (the accept loops push
    /// each handshaked secondary's [`dynrunner_transport_tunnel::PeerRegistration`]
    /// through it). Construct the transport first, then pass its two
    /// sinks here — the accept loops then feed the transport directly,
    /// with no separate legacy inbound consumer.
    ///
    /// `server_name` is this server's own node-id: it becomes the QUIC
    /// certificate's subject CN, which a QUIC-dialing peer validates the
    /// connection against (`connect(addr, server_name)`). It MUST equal the
    /// id the dialer addresses this server by — for the submitter mesh that
    /// is the bootstrap/submitter node-id (`SETUP_NODE_ID`), the same id
    /// secondaries register the bootstrap link under.
    pub async fn bind<I: Identifier>(
        addr: SocketAddr,
        server_name: &str,
        inbound: InboundTap<I>,
        registration: RegistrationSink<I>,
    ) -> Result<Self, String> {
        let cert = CertPair::generate(server_name)?;

        // Acquire the QUIC(UDP)+WSS(TCP) pair on one port number. A
        // port-0 `addr` lets the OS pick, with the pairing helper
        // retrying the whole pair past a TCP-twin collision (#422); a
        // fixed port is bound fail-fast (a primary that already
        // published its URL must bind that exact port).
        let (quic_listener, wss_listener) =
            crate::listener_pair::bind_listener_pair(&cert, addr).await?;
        let port = quic_listener.port();

        // Spawn QUIC accept loop
        {
            let inbound = inbound.clone();
            let registration = registration.clone();
            tokio::task::spawn_local(async move {
                accept::quic_accept_loop::<I>(quic_listener, inbound, registration).await;
            });
        }

        // Spawn WSS accept loop
        {
            let inbound = inbound.clone();
            let registration = registration.clone();
            tokio::task::spawn_local(async move {
                accept::wss_accept_loop::<I>(wss_listener, inbound, registration).await;
            });
        }

        tracing::info!(port, "network server listening (QUIC/UDP + WSS/TCP)");

        Ok(Self { port, cert })
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
}
