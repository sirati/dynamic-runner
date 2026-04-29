
use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{
    DistributedMessage, PeerConnectionInfo,
    SecondaryTransport,
};
use dynrunner_scheduler_api::{
    ResourceEstimator, Scheduler,
};

use crate::state::SecondaryConnectionState;

use super::PrimaryCoordinator;
use super::wire::timestamp_now;

impl<T: SecondaryTransport<I>, S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<T, S, E, I> {
    pub(super) async fn send_peer_lists(&mut self) -> Result<(), String> {
        tracing::info!("sending peer lists");

        let peers: Vec<PeerConnectionInfo> = self
            .secondaries
            .values()
            .map(|s| PeerConnectionInfo {
                secondary_id: s.id().to_string(),
                cert: s.cert_pem().unwrap_or("").to_string(),
                ipv4: s.ipv4().map(|s| s.to_string()),
                ipv6: None,
                port: s.quic_port(),
            })
            .collect();

        let secondary_ids: Vec<String> = self.secondaries.keys().cloned().collect();
        for secondary_id in &secondary_ids {
            let msg = DistributedMessage::PeerInfo {
                sender_id: self.config.node_id.clone(),
                timestamp: timestamp_now(),
                peers: peers.clone(),
            };
            self.transport.send_to(secondary_id, msg).await?;
        }

        // Transition all from CertExchanging -> PeerDiscovery
        for secondary_id in &secondary_ids {
            if let Some(state) = self.secondaries.remove(secondary_id) {
                if let SecondaryConnectionState::CertExchanging(conn) = state {
                    self.secondaries.insert(
                        secondary_id.clone(),
                        SecondaryConnectionState::PeerDiscovery(conn.begin_peer_discovery()),
                    );
                } else {
                    self.secondaries.insert(secondary_id.clone(), state);
                }
            }
        }

        Ok(())
    }

    // ── Phase 4: Wait for Peer Connections ──

    pub(super) async fn wait_for_peer_connections(&mut self) -> Result<(), String> {
        // For single-secondary, skip peer connection wait
        let secondary_ids: Vec<String> = self.secondaries.keys().cloned().collect();

        for secondary_id in secondary_ids {
            if let Some(state) = self.secondaries.remove(&secondary_id) {
                if let SecondaryConnectionState::PeerDiscovery(conn) = state {
                    self.secondaries.insert(
                        secondary_id,
                        SecondaryConnectionState::InitialAssigning(conn.peers_ready()),
                    );
                } else {
                    self.secondaries.insert(secondary_id, state);
                }
            }
        }

        Ok(())
    }

    // ── Phase 5: Initial Assignment ──

}
