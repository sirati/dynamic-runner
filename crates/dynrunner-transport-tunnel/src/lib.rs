//! Tunneled peer transport: a [`PeerTransport`] over the primary's
//! per-secondary tunnel connections.
//!
//! # What this crate gives the rest of the workspace
//!
//! The submitter-side primary keeps a per-secondary writer + a demuxed
//! inbound channel via the existing SSH-tunneled (or in-process
//! channel) [`SecondaryTransport`]. Those connections are alive,
//! peer-routable, and survive promotion — but they sat behind the
//! [`SecondaryTransport`] trait and were therefore invisible to the
//! mesh-level [`PeerTransport`] surface.
//!
//! [`TunneledPeerTransport`] closes that gap. It is a *peer-mesh-only*
//! view over the same writer table + inbound channel the legacy
//! [`SecondaryTransport`] already produces. At the mesh-level
//! abstraction the primary is just another peer; the networking
//! implementation underneath happens to be SSH tunnels (or channels in
//! test fixtures) instead of QUIC. The transport is role-blind
//! (transport ⊥ roles): it routes by `PeerId` only, exactly as
//! `dynrunner_transport_quic::PeerNetwork` does. Resolving
//! [`dynrunner_protocol_primary_secondary::Destination::Primary`] to a
//! host peer-id is the coordinator edge's job, never the transport's.
//!
//! # Module boundary
//!
//! The crate exposes exactly one trait impl ([`PeerTransport`] for
//! [`TunneledPeerTransport`]) and one builder
//! ([`TunneledPeerTransport::new`]) that returns the transport plus
//! three handles the caller wires to the accept loops: a shared
//! outgoing-writer table (for direct registration on the in-process /
//! test paths), an inbound sink (the accept-loop reader tasks push
//! every frame into it), and a registration sink (the accept loops
//! push each handshaked secondary's [`PeerRegistration`] through it).
//! `TunneledPeerTransport` OWNS the real inbound demux: its
//! `recv_peer` drives the single `incoming_rx` + `new_conn_rx`
//! `select!` that the legacy `NetworkServer::recv` used to own. The
//! `NetworkServer` is reduced to bind + accept-loops + writer-table
//! population — it no longer consumes inbound or fans out a tap.
//! Beyond that, `TunneledPeerTransport` owns its mesh-level state: the
//! Router (seeded with the local id), inbound + registration mpscs.
//!
//! # Single-threaded by construction
//!
//! `Rc<RefCell<_>>` is fine here because the primary coordinator runs
//! on a [`tokio::task::LocalSet`] (PyO3 manager + integration tests
//! both use the `current_thread` flavour and `spawn_local`). The
//! workspace's `clippy::await_holding_refcell_ref = "deny"` lint
//! catches any future regression that holds a borrow across an await.

mod transport;

#[cfg(test)]
mod tests;

pub use transport::{
    InboundTap, PeerRegistration, RegistrationSink, SharedOutgoing, TunneledPeerTransport,
};
