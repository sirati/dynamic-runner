
use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, PeerConnectionInfo, PeerTransport, PrimaryPeerSetupBootstrap,
    SetupBootstrapBroadcast, SetupBootstrapMessage,
};
use dynrunner_scheduler_api::{
    ResourceEstimator, Scheduler,
};

use crate::state::SecondaryConnectionState;

use super::PrimaryCoordinator;
use super::wire::timestamp_now;

impl<Tr: PeerTransport<I>, S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<Tr, S, E, I> {
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
        // shared with secondaries; gathering the originator here
        // means the setup-defer path (`emit_setup_defer_handshake`)
        // inherits observer replication without a parallel call site.
        let observer_mutations: Vec<ClusterMutation<I>> = self
            .secondaries
            .values()
            .filter(|s| s.is_observer())
            .map(|s| ClusterMutation::PeerJoined {
                peer_id: s.id().to_string(),
                is_observer: true,
            })
            .collect();

        let secondary_ids: Vec<String> = self.secondaries.keys().cloned().collect();
        // Step 10: route the PeerInfo fan-out through the narrow
        // `SetupBootstrapBroadcast` surface. As on the secondary side,
        // the underlying wire stays the same `transport`; the
        // call-site type is what changed — `SetupBootstrapMessage`
        // accepts only the three setup variants, so an accidental
        // runtime broadcast fails to type-check here.
        //
        // The narrower broadcast result is `Result<(), String>`
        // (partial-failure summary folded into one diagnostic). The
        // pre-Step-10 wire path emitted the same warn-log per failing
        // secondary; we preserve that detail by routing the structured
        // form through the underlying transport ONLY for the partial-
        // failure log loop below, while the actual delivery goes
        // through the bootstrap. This keeps the per-secondary diagnostic
        // breadcrumb that operators rely on without re-introducing the
        // structured-error type on the bootstrap trait surface.
        let msg = SetupBootstrapMessage::PeerInfo {
            sender_id: self.config.node_id.clone(),
            timestamp: timestamp_now(),
            peers,
        };
        // Per-secondary structured warn lines for partial-failure
        // cases are emitted by the bootstrap adapter (it walks the
        // underlying transport's partial-failure list before folding
        // it into the summary). The pre-Step-10 path emitted those
        // lines from here directly; the post-Step-10 arrangement
        // moves the emission into the adapter so every
        // `SetupBootstrapBroadcast::broadcast` caller gets the same
        // observability for free, without each call site
        // re-implementing the walk. The `?` here propagates the
        // summary string the adapter folds up — same exit semantics
        // as the pre-refactor `return Err(format!(…))`.
        let mut bootstrap = PrimaryPeerSetupBootstrap::new(&mut self.transport);
        SetupBootstrapBroadcast::<I>::broadcast(&mut bootstrap, msg).await?;

        // Broadcast the observer-join CRDT batch immediately after
        // the PeerInfo fan-out. Secondaries' `wait_for_setup` accepts
        // `ClusterMutation` frames during setup, and the apply rule
        // for `PeerJoined { is_observer: true }` is set-semantics
        // idempotent — a re-application against an already-populated
        // role table is a silent NoOp.
        self.apply_and_broadcast_cluster_mutations(observer_mutations).await;

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
