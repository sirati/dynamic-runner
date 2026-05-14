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

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::DistributedMessage;
use dynrunner_transport_tunnel::{InboundTap, SharedOutgoing};
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
    /// Shared writer-table view: when set,
    /// [`drain_new_connections`] mirrors each newly-accepted
    /// secondary's outgoing sender into this Rc-shared map so a
    /// paired [`dynrunner_transport_tunnel::TunneledPeerTransport`]
    /// sees the same per-secondary writers and can dispatch
    /// `Address::Peer(id)` / role-resolved sends through the same
    /// underlying tunnel as the legacy `SecondaryTransport::send_to`.
    /// `None` for the legacy single-transport path (tests, callers
    /// that don't want a peer-mesh view of the primary's tunnels).
    /// Step 5b wires this on production primary call sites.
    shared_outgoing: Option<SharedOutgoing<I>>,
    /// Inbound fan-out tap: when set, [`MessageReceiver::recv`]
    /// (in `transport_impl.rs`) clone-forwards every yielded message
    /// into this sender so the paired `TunneledPeerTransport`'s
    /// `recv_peer()` observes the same inbound stream. Set together
    /// with `shared_outgoing` via [`attach_tunnel`]; `None` for the
    /// legacy single-transport path. Step 5b leaves the peer queue
    /// drainless in production (no consumer yet); Step 6 attaches
    /// the demoted-primary `select! { peer_transport.recv_peer() }`
    /// arm that consumes it.
    inbound_tap: Option<InboundTap<I>>,
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
            shared_outgoing: None,
            inbound_tap: None,
        })
    }

    /// Wire the legacy server into a paired
    /// [`dynrunner_transport_tunnel::TunneledPeerTransport`] view.
    /// Idempotent on the present fields (latest call wins); intended
    /// to be called once at primary construction in
    /// `PyPrimaryCoordinator::run` / `PyDistributedManager::run` after
    /// [`TunneledPeerTransport::new`] returned the matching
    /// `outgoing` + `inbound_tap` handles.
    ///
    /// From this point on:
    /// - every newly-accepted secondary is registered in BOTH the
    ///   server's owned [`connections`] map (so the legacy
    ///   `SecondaryTransport::send_to` keeps working) AND in the
    ///   shared `outgoing` map (so the paired peer view's
    ///   `send_to_peer` / `Address::Role(_)` dispatch can find the
    ///   same writer);
    /// - every inbound message yielded by [`MessageReceiver::recv`]
    ///   is clone-forwarded into the paired peer view's queue so
    ///   `recv_peer()` observes it.
    ///
    /// Pre-existing connections (registered before `attach_tunnel`)
    /// are also mirrored into `outgoing` at attach time so a primary
    /// that accepts secondaries before attaching the tunnel view
    /// doesn't lose any writers. Step 5b doesn't hit this branch in
    /// practice (the tunnel view is constructed before
    /// `wait_for_connections`) but the contract holds for any
    /// future ordering.
    pub fn attach_tunnel(
        &mut self,
        outgoing: SharedOutgoing<I>,
        inbound_tap: InboundTap<I>,
    ) {
        // Mirror any pre-existing connections into the shared map
        // first — see the doc-comment above.
        for (id, tx) in &self.connections {
            outgoing.borrow_mut().insert(id.clone(), tx.clone());
        }
        self.shared_outgoing = Some(outgoing);
        self.inbound_tap = Some(inbound_tap);
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
    ///
    /// When the legacy server has been paired with a
    /// [`dynrunner_transport_tunnel::TunneledPeerTransport`] via
    /// [`attach_tunnel`], every newly-accepted secondary's outgoing
    /// sender is ALSO inserted into the shared writer table so the
    /// peer view's `send_to_peer` / role-addressed dispatch reaches
    /// the same wire. The single concern of `drain_new_connections`
    /// is unchanged: surface freshly-handshaked secondaries to the
    /// primary's send map; the tunnel-view mirror just stacks on
    /// top via the optional handle.
    fn drain_new_connections(&mut self) {
        while let Ok(accepted) = self.new_conn_rx.try_recv() {
            tracing::info!(secondary = %accepted.secondary_id, "secondary registered");
            if let Some(shared) = self.shared_outgoing.as_ref() {
                shared
                    .borrow_mut()
                    .insert(accepted.secondary_id.clone(), accepted.outgoing_tx.clone());
            }
            self.connections
                .insert(accepted.secondary_id.clone(), accepted.outgoing_tx);
        }
    }

    /// Optional inbound-tap handle. Cloning yields a fresh sender
    /// that pushes into the paired peer view's queue. Reserved for
    /// `transport_impl.rs`'s `recv()` implementation — the single
    /// site responsible for fan-out — and exposed at `pub(super)`
    /// rather than `pub` so external callers can't bypass the
    /// `recv()` interception.
    pub(super) fn inbound_tap(&self) -> Option<&InboundTap<I>> {
        self.inbound_tap.as_ref()
    }

    /// Optional shared-outgoing handle. Used by `transport_impl.rs`'s
    /// in-`recv()` accept-registration path to mirror late-arriving
    /// secondaries into the peer view's writer table — the same
    /// mirror `drain_new_connections` performs on the
    /// try-receive path. Exposed at `pub(super)` for the same reason
    /// as [`inbound_tap`]: external callers must not see the
    /// shared table directly; they reach the writers through the
    /// `TunneledPeerTransport`'s `PeerTransport` surface.
    pub(super) fn shared_outgoing_for_tap(&self) -> Option<&SharedOutgoing<I>> {
        self.shared_outgoing.as_ref()
    }
}

