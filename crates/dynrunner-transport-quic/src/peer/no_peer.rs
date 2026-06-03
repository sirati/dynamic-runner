//! No-op peer transport for single-secondary deployments and tests.

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{
    DistributedMessage, PeerConnectionInfo, PeerId, PeerTransport,
};

/// A no-op peer transport for when peer-to-peer is not needed
/// (single-secondary or in-process distributed mode).
pub struct NoPeerTransport;

impl<I: Identifier> PeerTransport<I> for NoPeerTransport {
    async fn broadcast(&mut self, _msg: DistributedMessage<I>) -> Result<(), String> {
        Ok(())
    }

    async fn send_to_peer(
        &mut self,
        _peer_id: &str,
        _msg: DistributedMessage<I>,
    ) -> Result<(), String> {
        Ok(())
    }

    async fn recv_peer(&mut self) -> Option<DistributedMessage<I>> {
        // Never returns — no peers
        std::future::pending().await
    }

    fn try_recv_peer(&mut self) -> Option<DistributedMessage<I>> {
        None
    }

    fn peer_count(&self) -> usize {
        0
    }

    fn has_peer(&self, _id: &PeerId) -> bool {
        // No peers ever — single-secondary / in-process mode has no
        // mesh, so no id is ever a member. Consistent with
        // `peer_count == 0`.
        false
    }

    async fn connect_to_peers(&mut self, _peers: &[PeerConnectionInfo]) {
        // No-op
    }
}
