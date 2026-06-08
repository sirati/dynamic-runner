use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, PeerConnectionInfo, SetupBootstrapMessage,
};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use crate::state::SecondaryConnectionState;

use super::PrimaryCoordinator;
use super::wire::timestamp_now;

impl<S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<S, E, I> {
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
                // Task #36 / Step 7: fan out per-peer observer status
                // so each receiving secondary can apply
                // `ClusterMutation::PeerJoined { is_observer: true }`
                // (originated below from this same view) to populate
                // its replicated `RoleTable.observers` and filter
                // observers from `lowest_alive` candidate selection
                // in `election.rs`.
                is_observer: s.is_observer(),
            })
            .collect();

        // Originate one `PeerJoined { is_observer: true }` mutation
        // per observer secondary, applied locally and broadcast over
        // the same CRDT path as `seed_cluster_state`'s `PhaseDepsSet`
        // / `TaskAdded` batch. This is the single writer to
        // `RoleTable.observers` — receivers no longer derive the set
        // from their PeerInfo handler, so a single ordered apply path
        // owns the field. `is_observer = false` peers don't need a
        // mutation under the minimal apply rule (non-observer
        // membership is Batch D's `PeerRemoved`/`PeerJoined { is_observer
        // = false }` semantics). `send_peer_lists` is the earliest
        // call site where the primary's view of observers is being
        // shared with secondaries; gathering the originator here keeps
        // observer replication on a single call site.
        let observer_mutations: Vec<ClusterMutation<I>> = self
            .secondaries
            .values()
            .filter(|s| s.is_observer())
            .map(|s| ClusterMutation::PeerJoined {
                peer_id: s.id().to_string(),
                is_observer: true,
                // Observers are never primary-capable; the conservative
                // `false` is also the correct steady-state value here.
                can_be_primary: false,
                // Stamped at the origination choke point.
                cap_version: Default::default(),
            })
            .collect();

        let secondary_ids: Vec<String> = self.secondaries.keys().cloned().collect();
        // The PeerInfo fan-out rides the unified mesh egress: build the
        // narrow setup-typed `SetupBootstrapMessage` (so an accidental
        // operational variant fails to type-check here), then losslessly
        // convert it to the wire `DistributedMessage` and broadcast it
        // through `send_to(Destination::All, ..)` — the same egress every
        // other primary broadcast uses. The wire bytes are identical to the
        // pre-mesh `PrimaryPeerSetupBootstrap` path (the `From` conversion
        // is the same one the adapter used internally); only the routing
        // surface changed, from a transport-direct adapter to the queued
        // mesh send. Per-secondary delivery failures are not folded into a
        // synchronous result anymore (the queued send has none): a silent
        // secondary surfaces through the heartbeat monitor, exactly as the
        // adapter's own docs noted the per-secondary signal already did.
        let msg: dynrunner_protocol_primary_secondary::DistributedMessage<I> =
            SetupBootstrapMessage::PeerInfo {
                sender_id: self.config.node_id.clone(),
                timestamp: timestamp_now(),
                peers,
            }
            .into();
        self.send_to(dynrunner_protocol_primary_secondary::Destination::All, msg)
            .await?;

        // Broadcast the observer-join CRDT batch immediately after
        // the PeerInfo fan-out. Secondaries' `wait_for_setup` accepts
        // `ClusterMutation` frames during setup, and the apply rule
        // for `PeerJoined { is_observer: true }` is set-semantics
        // idempotent — a re-application against an already-populated
        // role table is a silent NoOp.
        self.apply_and_broadcast_cluster_mutations(observer_mutations)
            .await;

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
