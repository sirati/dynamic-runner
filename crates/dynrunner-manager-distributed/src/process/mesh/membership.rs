//! Live membership reads for [`Mesh`].
//!
//! # Concern
//!
//! ONE concern: surface the transport's LIVE membership — publish it into
//! the [`super::super::membership::MembershipView`] the detached
//! [`super::super::mesh_client::MeshClient`]s read, and answer
//! `peer_count`/`has_peer` from the transport directly. ALWAYS a direct
//! transport read — never a shadow counter (the SETTLED no-shadow rule).

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::address::PeerId;
use dynrunner_protocol_primary_secondary::PeerTransport;

use super::Mesh;

impl<I: Identifier, Tr: PeerTransport<I>> Mesh<I, Tr> {
    /// Publish the LIVE transport membership into the [`MembershipView`]
    /// the detached [`MeshClient`]s read.
    ///
    /// Called by the mesh-pump once per drain cycle. The published value
    /// is ALWAYS a direct transport read ([`PeerTransport::peer_count`] +
    /// the connected id-set) — never a hand-maintained delta (the SETTLED
    /// no-shadow rule).
    pub fn publish_membership(&self) {
        self.membership
            .publish(self.transport.peer_count(), self.transport.connected_ids());
    }

    /// Live mesh cardinality — the transport's `connections.len()`. The
    /// single source of truth; never a shadow.
    pub fn peer_count(&self) -> usize {
        self.transport.peer_count()
    }

    /// Live per-id membership — delegates to the transport's connection
    /// table.
    pub fn has_peer(&self, id: &PeerId) -> bool {
        self.transport.has_peer(id)
    }
}
