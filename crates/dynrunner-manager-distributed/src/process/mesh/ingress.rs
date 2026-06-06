//! The wire-facing ingress of [`Mesh`] for the mesh-pump.
//!
//! # Concern
//!
//! ONE concern: receive a frame off the transport, fold any `PeerInfo`
//! seed list into the transport mesh (RV-2 peer discovery, observed not
//! shadowed), and route the frame to the right local slot(s). A thin
//! adapter (H6): the pump only drains; all routing lives in
//! [`super::Mesh::route_incoming`], all membership in the transport.

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{DistributedMessage, PeerTransport};

use super::Mesh;

impl<I: Identifier, Tr: PeerTransport<I>> Mesh<I, Tr> {
    /// Receive ONE inbound wire frame, dial off it if it is a `PeerInfo`
    /// (RV-2 peer-mesh discovery), then route it to the right local slot(s)
    /// — a single self-contained `&mut Mesh` call whose borrow is released on
    /// return (so a sibling `select!` arm may then borrow the mesh for an
    /// egress apply).
    ///
    /// Returns `true` if a frame was handled, `false` once the transport's
    /// inbound is closed (`recv_peer → None`) — the pump's ingress-side
    /// teardown signal. Both the dial and the route are delegated wholesale
    /// (the pump is a thin adapter — H6); the dial is observed-not-shadowed
    /// (it folds the listed peers into the transport, which stays the single
    /// membership source). A `PeerInfo` is dialed AND ALSO routed to the
    /// local slots, so the coordinator still observes the frame as today
    /// (e.g. the secondary's watchdog arming) — the pump only ADDS the dial.
    pub async fn recv_dial_and_route(&mut self) -> bool {
        let Some(frame) = self.recv_peer().await else {
            return false;
        };
        if let DistributedMessage::PeerInfo { peers, .. } = &frame {
            // Clone the seed list out of the borrowed frame so the dial's
            // `&mut self` does not alias the frame we still route below.
            let peers = peers.clone();
            self.connect_to_peers(&peers).await;
        }
        self.route_incoming(frame);
        true
    }

    /// Receive the next frame from any remote peer. Thin pass-through to
    /// the transport for the mesh-pump's ingress drain.
    pub async fn recv_peer(&mut self) -> Option<DistributedMessage<I>> {
        self.transport.recv_peer().await
    }

    /// Dial the peers in a `PeerInfo` list, folding each into the
    /// transport's mesh (RV-2). Thin pass-through to
    /// [`PeerTransport::connect_to_peers`]: peer discovery is a
    /// transport/membership concern the mesh-pump owns, NOT the
    /// coordinator's (the coordinator holds only a `MeshClient`/`RoleInbox`,
    /// neither of which dials — see the `PHASE-C-SEAM[C-NODE]` at
    /// `secondary/setup.rs`). The pump observes every inbound `PeerInfo`
    /// frame, so it dials off the same list the coordinator would have, with
    /// no manager-layer `connect_to_peers` call. Membership re-derivation
    /// stays in the transport (the pump never shadows it).
    pub async fn connect_to_peers(
        &mut self,
        peers: &[dynrunner_protocol_primary_secondary::PeerConnectionInfo],
    ) {
        self.transport.connect_to_peers(peers).await;
    }
}
