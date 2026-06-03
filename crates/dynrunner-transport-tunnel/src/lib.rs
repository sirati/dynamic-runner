//! Tunneled peer transport: a [`PeerTransport`] over the primary's
//! per-secondary tunnel connections.
//!
//! # What this crate gives the rest of the workspace
//!
//! The submitter-side primary keeps a per-secondary writer + a demuxed
//! inbound channel via the existing SSH-tunneled (or in-process
//! channel) [`SecondaryTransport`]. Those connections are alive,
//! peer-routable, and survive promotion — but until now they sat
//! behind the [`SecondaryTransport`] trait and were therefore invisible
//! to the mesh-level [`PeerTransport`] surface. Step 5 of the
//! transport-unification refactor added a `peer_transport: P` field on
//! `PrimaryCoordinator` and routed primary-bound relays through
//! `peer_transport.send(Address::Role(Role::Primary), msg)`, but the
//! production constructors still passed
//! `dynrunner_transport_quic::NoPeerTransport` — so the role-addressed
//! send always errored "no holder", the `Err` was swallowed, and the
//! relay arm was inert.
//!
//! [`TunneledPeerTransport`] closes that gap. It is a *peer-mesh-only*
//! view over the same writer table + inbound channel the legacy
//! [`SecondaryTransport`] already produces. At the mesh-level
//! abstraction the primary is just another peer; the networking
//! implementation underneath happens to be SSH tunnels (or channels in
//! test fixtures) instead of QUIC. No special-casing at the mesh
//! layer — Step 4's role routing, Step 3's [`Address::Role(_)`]
//! dispatch, and Step 2's [`RoleTable`] write-through cache all work
//! against this transport identically to how they work against
//! `dynrunner_transport_quic::PeerNetwork`.
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
//! Beyond that, `TunneledPeerTransport` owns its mesh-level state:
//! local-id, role-cache, inbound + registration mpscs.
//!
//! # Single-threaded by construction
//!
//! `Rc<RefCell<_>>` is fine here because the primary coordinator runs
//! on a [`tokio::task::LocalSet`] (PyO3 manager + integration tests
//! both use the `current_thread` flavour and `spawn_local`). The
//! workspace's `clippy::await_holding_refcell_ref = "deny"` lint
//! catches any future regression that holds a borrow across an await.
//!
//! # What stays available on the SECONDARY side
//!
//! `dynrunner_transport_quic::NoPeerTransport` is unaffected by this
//! crate and remains the right choice for the
//! `disable_peer_overlay` path (firewalled inter-compute fabrics like
//! LMU SLURM). The primary's call sites stop using `NoPeerTransport`
//! once their constructors thread [`TunneledPeerTransport`] through;
//! secondary call sites keep it for as long as that disable path
//! exists.

mod transport;

#[cfg(test)]
mod tests;

pub use transport::{
    InboundTap, PeerRegistration, RegistrationSink, SharedOutgoing, TunneledPeerTransport,
};
