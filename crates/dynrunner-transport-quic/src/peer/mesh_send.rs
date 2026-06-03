//! [`MeshSendHandle`] — a cloneable mesh-send capability over a
//! [`PeerNetwork`]'s existing per-peer connections.
//!
//! # Concern
//!
//! A node that runs BOTH a `SecondaryCoordinator` and an on-demand
//! co-located `PrimaryCoordinator` on one `LocalSet` owns a SINGLE
//! [`PeerNetwork`] mesh. The secondary holds that mesh by value (the
//! `EitherPeerTransport`); the co-located primary's role-blind
//! `Tr: PeerTransport` (`MeshHandleTransport`) still needs to reach
//! remote peers over the same mesh once this node is promoted.
//!
//! This handle is the cloneable mesh-send capability that makes that
//! possible WITHOUT changing `PeerNetwork`'s ownership of its
//! `connections` table or rewriting its send path. It generalizes the
//! `dynrunner_transport_tunnel::SharedOutgoing` writer-table pattern (a
//! cloneable handle every transport view holds) to the real QUIC mesh:
//! instead of a shared `Rc<RefCell<HashMap>>` (which would alias the
//! audited `connections` ownership), the handle is a cloneable
//! [`mpsc::UnboundedSender`] feeding a forwarding queue that
//! [`PeerNetwork::recv_peer`] drains and dispatches through its OWN
//! relay-aware `send_to_peer` / `broadcast` path. The forwarding drain
//! lives entirely in the transport (`recv_peer`'s select loop), so the
//! router's relay/blacklist/redial logic still applies to every
//! handle-issued send and no manager-visible drain is introduced.
//!
//! # Ownership & threading
//!
//! Like the rest of the QUIC transport, the mesh runs on a
//! `current_thread` `LocalSet`. The proxy channel is a plain
//! `tokio::sync::mpsc`; `send` on the handle is synchronous (no await),
//! so cloning the handle into a co-located primary's `MeshHandleTransport`
//! while the secondary keeps its own access through the owned mesh
//! never aliases a borrow across an await.

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::DistributedMessage;
use tokio::sync::mpsc;

/// One queued mesh send, drained inside [`super::PeerNetwork::recv_peer`]
/// and dispatched through the network's own relay-aware send path.
pub(crate) enum MeshSend<I: Identifier> {
    /// Unicast to a specific peer id — forwarded via the network's
    /// `send_to_peer` (router relay applies).
    ToPeer(String, DistributedMessage<I>),
    /// Fan-out to every connected peer — forwarded via the network's
    /// `broadcast`.
    Broadcast(DistributedMessage<I>),
}

/// A cloneable mesh-send capability over a [`super::PeerNetwork`].
///
/// Obtain via [`super::PeerNetwork::mesh_send_handle`]. Every clone
/// shares the same forwarding queue; sends are dispatched in FIFO order
/// by the network's `recv_peer` drain. A send returns `Err` only when
/// the network has been dropped (the receiver is gone) — the same
/// "transport torn down" signal callers already handle on a closed
/// channel.
pub struct MeshSendHandle<I: Identifier> {
    proxy_tx: mpsc::UnboundedSender<MeshSend<I>>,
}

impl<I: Identifier> Clone for MeshSendHandle<I> {
    fn clone(&self) -> Self {
        Self {
            proxy_tx: self.proxy_tx.clone(),
        }
    }
}

impl<I: Identifier> MeshSendHandle<I> {
    pub(crate) fn new(proxy_tx: mpsc::UnboundedSender<MeshSend<I>>) -> Self {
        Self { proxy_tx }
    }

    /// Queue a unicast send to `peer_id`. Dispatched (relay-aware) by the
    /// owning network's `recv_peer` drain. `Err` iff the network was
    /// dropped.
    pub fn send_to_peer(&self, peer_id: &str, msg: DistributedMessage<I>) -> Result<(), String> {
        self.proxy_tx
            .send(MeshSend::ToPeer(peer_id.to_string(), msg))
            .map_err(|_| "mesh-send handle: owning PeerNetwork dropped".to_string())
    }

    /// Queue a mesh broadcast. Dispatched by the owning network's
    /// `recv_peer` drain. `Err` iff the network was dropped.
    pub fn broadcast(&self, msg: DistributedMessage<I>) -> Result<(), String> {
        self.proxy_tx
            .send(MeshSend::Broadcast(msg))
            .map_err(|_| "mesh-send handle: owning PeerNetwork dropped".to_string())
    }
}
