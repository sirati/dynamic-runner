//! No-op peer transport for single-secondary deployments and tests.

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{DistributedMessage, PeerConnectionInfo, PeerTransport};

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

    async fn connect_to_peers(&mut self, _peers: &[PeerConnectionInfo]) {
        // No-op
    }
}
