//! [`MeshHandleTransport`] — a ROLE-BLIND `Tr: PeerTransport` view over
//! the host's single peer mesh, held by a co-located coordinator that
//! does not own the mesh by value.
//!
//! # Concern
//!
//! A host that runs BOTH a `SecondaryCoordinator` and a co-located
//! `PrimaryCoordinator` on one `LocalSet` owns exactly ONE
//! [`crate::PeerNetwork`] mesh — held by value inside the secondary.
//! The co-located primary still needs a `Tr: PeerTransport` to reach the
//! cluster's other peers and to receive its operational inbound. This
//! type is that `Tr`:
//!
//!   * `send_to_peer(id, msg)` / `broadcast(msg)` — queue on the
//!     cloneable [`MeshSendHandle`], which the owning `PeerNetwork`'s
//!     `recv_peer` drains and dispatches relay-aware. Routes EVERY peer
//!     id uniformly over the mesh — there is no own-id / loopback branch
//!     in this transport.
//!   * `recv_peer()` — drains an inbound `mpsc` channel fed by the
//!     co-located secondary's ingress demux (the secondary owns the mesh
//!     `recv_peer` and forwards the frames the co-located primary must
//!     process into this channel).
//!
//! # Why this is NOT the deleted `ColocatedPrimaryTransport` antipattern
//!
//! The deleted `ColocatedPrimaryTransport` was a PER-ROLE transport leg:
//! its `send_to`/`broadcast` branched on `secondary_id ==
//! own_secondary_id` to pick loopback-vs-mesh, putting a role/locality
//! decision INSIDE the transport — the exact `TRANSPORT ⊥ ROLES`
//! violation the one-mesh refactor removes. `MeshHandleTransport` holds
//! NO own-id and makes NO loopback decision: the resolution
//! `Destination::Secondary(own_id) → SendTarget::Loopback` happens at the
//! coordinator's EGRESS edge (`resolve_destination`), BEFORE the
//! transport is consulted, so `send_to_peer` is only ever called with a
//! remote id and the own-secondary loopback is delivered by the egress
//! `SendTarget::Loopback` arm (a coordinator-held loopback sender), never
//! here. The mesh-vs-loopback split is therefore a coordinator-edge
//! concern; this transport is a plain, role-blind mesh view any
//! coordinator could hold.
//!
//! # Single-threaded by construction
//!
//! One `LocalSet`; all channels are `tokio::sync::mpsc`. Every send is
//! synchronous (the [`MeshSendHandle`] proxy-queue handle), so no borrow
//! is held across an await.

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{
    DistributedMessage, PeerConnectionInfo, PeerId, PeerTransport,
};
use tokio::sync::mpsc;

use crate::MeshSendHandle;

/// A role-blind `Tr: PeerTransport` over a [`MeshSendHandle`] + a
/// demuxed inbound channel. See module docs for the routing model and
/// why it is not the `ColocatedPrimaryTransport` antipattern.
pub struct MeshHandleTransport<I: Identifier> {
    /// Cloneable mesh-send capability (relay-aware; drained by the
    /// owning `PeerNetwork::recv_peer`). Every send/broadcast goes here,
    /// uniformly for all peer ids.
    mesh: MeshSendHandle<I>,
    /// Inbound stream the co-located secondary's ingress demux forwards
    /// the frames this coordinator must process into. `None` only when
    /// the secondary's forwarding sender has been dropped (its transport
    /// torn down) — the operational loop treats a closed inbound as
    /// end-of-inbound exactly as it does for the network/channel
    /// transports.
    inbound_rx: mpsc::UnboundedReceiver<DistributedMessage<I>>,
}

impl<I: Identifier> MeshHandleTransport<I> {
    /// Compose from the two handles the host's mesh + composition expose:
    ///   * `mesh` — `EitherPeerTransport::mesh_send_handle()` (the mesh's
    ///     cloneable send capability).
    ///   * `inbound_rx` — the receiver of the channel whose sender the
    ///     composition registers on the co-located secondary
    ///     (`register_colocated_primary_inbound`) so the secondary's
    ///     ingress demux feeds this coordinator's inbound.
    pub fn new(
        mesh: MeshSendHandle<I>,
        inbound_rx: mpsc::UnboundedReceiver<DistributedMessage<I>>,
    ) -> Self {
        Self { mesh, inbound_rx }
    }
}

impl<I: Identifier> PeerTransport<I> for MeshHandleTransport<I> {
    /// Mesh unicast to `peer_id` over the shared [`MeshSendHandle`].
    /// Role-blind: `peer_id` is always a remote id here — the egress
    /// edge resolved own-id to `SendTarget::Loopback` before reaching
    /// this transport, so own-secondary delivery never arrives here.
    async fn send_to_peer(
        &mut self,
        peer_id: &str,
        msg: DistributedMessage<I>,
    ) -> Result<(), String> {
        self.mesh.send_to_peer(peer_id, msg)
    }

    /// Mesh broadcast over the shared [`MeshSendHandle`] (every connected
    /// peer). The co-located own-secondary is NOT a mesh peer of itself,
    /// so its copy of a `Destination::All` fan-out is delivered by the
    /// coordinator's egress broadcast leg (mesh + loopback), not here —
    /// this transport only fans out to the wire mesh.
    async fn broadcast(&mut self, msg: DistributedMessage<I>) -> Result<(), String> {
        self.mesh.broadcast(msg)
    }

    /// Drain the demuxed inbound channel. `None` once the secondary's
    /// forwarding sender is dropped (its transport torn down); the
    /// operational loop treats that as end-of-inbound exactly as for the
    /// network/channel transports.
    async fn recv_peer(&mut self) -> Option<DistributedMessage<I>> {
        self.inbound_rx.recv().await
    }

    /// Non-blocking peek of the demuxed inbound channel.
    fn try_recv_peer(&mut self) -> Option<DistributedMessage<I>> {
        self.inbound_rx.try_recv().ok()
    }

    /// Mesh-health cardinality is owned by the co-located secondary's
    /// `PeerNetwork` (which owns the real connection table); this view
    /// sends THROUGH the shared [`MeshSendHandle`] but holds no peer
    /// table of its own. Report 0 — the coordinator reads peer health off
    /// `cluster_state` / its own tracked secondaries, never off this
    /// transport's `peer_count`.
    fn peer_count(&self) -> usize {
        0
    }

    /// Same rationale as [`Self::peer_count`]: this view holds NO peer
    /// table. The [`MeshSendHandle`] it sends through is a write-only
    /// send proxy that cannot be queried for membership; the real
    /// connection table lives on the co-located secondary's
    /// `PeerNetwork`. The faithful answer is `false` for every id —
    /// membership is read off `cluster_state`, never off this transport.
    fn has_peer(&self, _id: &PeerId) -> bool {
        false
    }

    /// No-op: the mesh this transport sends through is dialed and owned
    /// by the co-located secondary's `PeerNetwork`; this view never
    /// originates peer dials.
    async fn connect_to_peers(&mut self, _peers: &[PeerConnectionInfo]) {}
}
