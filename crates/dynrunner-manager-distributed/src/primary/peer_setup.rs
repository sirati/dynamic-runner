
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

        // Both address families travel from the originating
        // secondary's `CertExchange` (see `secondary::setup::
        // send_cert_exchange`) through `primary::connect::
        // handle_cert_exchange` which stashes them on the typestate
        // (`state::SecondaryConnection::receive_cert_exchange`). Read
        // both back here so the broadcast `PeerInfo` carries every
        // candidate the per-peer happy-eyeballs dialer can race —
        // dropping `ipv6` here was the cause of the empty-candidate-
        // set bug that surfaced as "WSS race to peer failed across
        // all addresses; attempted=N connected=0" on the consumer
        // side after the dialer was made dual-family-aware.
        let peers: Vec<PeerConnectionInfo> = self
            .secondaries
            .values()
            .map(|s| PeerConnectionInfo {
                secondary_id: s.id().to_string(),
                cert: s.cert_pem().unwrap_or("").to_string(),
                ipv4: s.ipv4().map(|s| s.to_string()),
                ipv6: s.ipv6().map(|s| s.to_string()),
                port: s.quic_port(),
                // Task #36: fan out per-peer observer status so each
                // receiving secondary can populate its peer_observers
                // set and filter observers from `lowest_alive`
                // candidate selection in `election.rs`.
                is_observer: s.is_observer(),
            })
            .collect();

        let secondary_ids: Vec<String> = self.secondaries.keys().cloned().collect();
        let msg = DistributedMessage::PeerInfo {
            sender_id: self.config.node_id.clone(),
            timestamp: timestamp_now(),
            peers,
        };
        if let Err(failures) = self.transport.broadcast(msg).await {
            for (secondary_id, error) in &failures {
                tracing::warn!(
                    secondary = %secondary_id,
                    error = %error,
                    "PeerInfo delivery failed"
                );
            }
            return Err(format!(
                "PeerInfo broadcast failed for {} secondaries",
                failures.len()
            ));
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
