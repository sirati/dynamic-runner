use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, PeerConnectionInfo, SetupBootstrapMessage,
};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use crate::state::SecondaryConnectionState;

use super::PrimaryCoordinator;
use super::wire::timestamp_now;

impl<S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<S, E, I> {
    /// Build the `PeerConnectionInfo` roster — the SOLE roster builder
    /// for the `PeerInfo` fan-out, shared by the incremental per-member
    /// serve ([`Self::serve_setup_on_cert_exchange`]) and the batch
    /// [`Self::send_peer_lists`].
    ///
    /// Only members that have completed cert-exchange are listed: a
    /// pre-cert member has no cert/addresses yet, so its entry would be
    /// an undialable husk. At the batch call site this filter is
    /// vacuous (the connect wait's quorum drop already removed every
    /// non-cert-exchanged member); on the incremental path it is what
    /// keeps a welcomed-but-not-yet-cert-exchanged sibling out of the
    /// broadcast until its own cert edge serves it.
    fn peer_roster(&self) -> Vec<PeerConnectionInfo> {
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
        self.secondaries
            .values()
            .filter(|s| s.is_at_least_cert_exchanged())
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
                // Fan out each peer's liveness-beacon UDP port (carried
                // from its `CertExchange`) so every node knows where to
                // beacon a peer that becomes primary. A peer that
                // advertised none (older sender) rides `None` and is
                // simply not beaconed.
                liveness_port: s.liveness_port(),
            })
            .collect()
    }

    /// Broadcast the CURRENT peer roster to the fleet (and persist its
    /// credentials + originate the observer-join CRDT batch) — the ONE
    /// `PeerInfo` emission path.
    ///
    /// Two callers, one behaviour:
    ///   * the incremental per-member serve
    ///     ([`Self::serve_setup_on_cert_exchange`]) — fired on each
    ///     member's cert-exchange edge, so the new member receives its
    ///     first peer list the moment it is servable AND every
    ///     earlier-welcomed member receives the GROWN roster (the
    ///     mesh-pump re-runs its peer-dial sweep off the same frame, so
    ///     the mesh converges as the fleet fills in — late peers are
    ///     never permanently unknown to early peers);
    ///   * the batch [`Self::send_peer_lists`] at the connect-wait
    ///     resolution — the final convergence broadcast over the settled
    ///     (possibly quorum-shrunk) roster.
    ///
    /// Receiver-idempotent end to end: the secondary's setup loop just
    /// re-arms its mesh watchdog off the latest count, the mesh-pump
    /// skips already-connected legs, and the observer-join batch NoOps
    /// on re-application.
    async fn broadcast_peer_roster(&mut self) {
        let peers = self.peer_roster();

        // Persist the roster's connection credentials to LOCAL state
        // when configured (the setup/submitter primary) — the SAME
        // `peers` list the broadcast below fans out, captured at the
        // moment it exists instead of dropping it with process memory.
        // A late-joiner observer on this host overlays the cert pins
        // onto its `.info`-derived seed and dials QUIC with valid
        // certs. Never fatal; see `peer_credentials::store_if_configured`.
        crate::peer_credentials::store_if_configured(
            self.config.peer_credentials_path.as_deref(),
            &peers,
        );

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
                // The id's current membership incarnation (idempotent
                // re-emit; see `apply_peer_joined`).
                member_gen: self.cluster_state.peer_member_gen(s.id()),
            })
            .collect();

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
        // `Destination::All` always resolves, so the only way this `send_to`
        // errors is the local mesh-pump being gone (the node winding down) —
        // a cluster collapse, which `send_to` latches on `self.mesh_pump_gone`
        // for `run_pipeline`'s pre-loop gate to route into the
        // strand-classification finalize tail. Warn-and-continue here (uniform
        // with the sibling `Destination::All` broadcasts
        // `broadcast_cold_seed` / `rebroadcast_full_roster` /
        // `apply_and_broadcast_cluster_mutations`) instead of `?`-escaping as a
        // raw `RunError::Other` that would bypass the classification.
        if let Err(error) = self
            .send_to(dynrunner_protocol_primary_secondary::Destination::All, msg)
            .await
        {
            tracing::warn!(error = %error, "PeerInfo broadcast delivery failed");
        }

        // Broadcast the observer-join CRDT batch immediately after
        // the PeerInfo fan-out. Secondaries' `wait_for_setup` accepts
        // `ClusterMutation` frames during setup, and the apply rule
        // for `PeerJoined { is_observer: true }` is set-semantics
        // idempotent — a re-application against an already-populated
        // role table is a silent NoOp.
        self.apply_and_broadcast_cluster_mutations(observer_mutations)
            .await;
    }

    /// The ONE per-member peer-list typestate walk:
    /// `CertExchanging → PeerDiscovery` ("its peer list has been sent").
    /// No-op from any other state, so the incremental serve and the
    /// batch [`Self::send_peer_lists`] can both walk a member without
    /// either knowing whether the other already did.
    fn advance_member_peer_listed(&mut self, secondary_id: &str) {
        if let Some(state) = self.secondaries.remove(secondary_id) {
            if let SecondaryConnectionState::CertExchanging(conn) = state {
                self.secondaries.insert(
                    secondary_id.to_string(),
                    SecondaryConnectionState::PeerDiscovery(conn.begin_peer_discovery()),
                );
            } else {
                self.secondaries.insert(secondary_id.to_string(), state);
            }
        }
    }

    /// The ONE per-member peers-ready typestate walk:
    /// `PeerDiscovery → InitialAssigning` (the orchestration has decided
    /// the peer-connection window is over). No-op from any other state.
    fn advance_member_peers_ready(&mut self, secondary_id: &str) {
        if let Some(state) = self.secondaries.remove(secondary_id) {
            if let SecondaryConnectionState::PeerDiscovery(conn) = state {
                self.secondaries.insert(
                    secondary_id.to_string(),
                    SecondaryConnectionState::InitialAssigning(conn.peers_ready()),
                );
            } else {
                self.secondaries.insert(secondary_id.to_string(), state);
            }
        }
    }

    /// Incremental setup delivery — serve a member the moment it
    /// completes its handshake (the `Handshaking → CertExchanging` edge
    /// in `connect::handle_cert_exchange`), instead of holding every
    /// welcomed member hostage until the WHOLE fleet has welcomed.
    ///
    /// Pre-fix, the setup payloads were all-or-nothing: nothing flowed
    /// until the connect wait resolved (full fleet or quorum-proceed
    /// timeout), so a fleet with one slow/missing member kept every
    /// already-welcomed member parked in `AwaitingPrimary` — workers
    /// unspawned, peer mesh unformed, bootstrap wires carrying nothing
    /// but beacons — for up to the whole straggler window. Now each
    /// arrival broadcasts the grown roster (serving the newcomer its
    /// first peer list AND converging every earlier member onto the
    /// fleet so far) and walks the newcomer's typestate.
    ///
    /// The run-start halves of the secondary's setup gate
    /// (`InitialAssignment` / `TransferComplete`) are served on a
    /// run-started discriminator (`run_start_batch_fired`, latched by
    /// `run_pipeline` once the run-start batch has fired):
    ///
    ///   * BEFORE the run starts (bring-up), they are deliberately NOT
    ///     served here — they stay on the post-connect-wait fan-out
    ///     (`perform_initial_assignment` / `send_transfer_complete`), so
    ///     the quorum-proceed policy still governs WHEN THE RUN STARTS — a
    ///     served member enters `Configuring` (worker pool spawned, mesh
    ///     forming, run config already pushed at its welcome) but cannot go
    ///     Operational or pull work early.
    ///   * AFTER the run has started, this serve is the ONLY thing that can
    ///     release the member's setup gate: the batch fan-out already fired
    ///     over the roster known at run start and never re-runs, so a
    ///     mid-run joiner (a respawned replacement, or any member welcoming
    ///     post-start) would otherwise park in `wait_for_setup` forever —
    ///     handshake-retrying, never emitting `MeshReady`, never assignable
    ///     (the run_20260612_045106 secondary-4 zombie). The bring-up
    ///     objections don't apply: the run HAS started, so there is no
    ///     pool-draining-ahead-of-the-initial-assignment hazard, and the
    ///     trio's directed frames are exactly the welcome-receipt proof
    ///     that stops the member's handshake retry.
    ///
    /// Fires only on the `Handshaking → CertExchanging` edge (the caller,
    /// `connect::handle_cert_exchange`, gates on it), so a duplicate
    /// welcome/cert-exchange from the same incarnation never re-serves —
    /// the serve is once-per-incarnation, consistent with the existing
    /// duplicate-welcome handling.
    pub(super) async fn serve_setup_on_cert_exchange(&mut self, secondary_id: &str) {
        tracing::info!(
            secondary = %secondary_id,
            "serving setup incrementally: broadcasting the grown peer \
             roster (newcomer gets its first peer list; earlier members \
             converge onto the fleet so far)"
        );
        self.broadcast_peer_roster().await;
        self.advance_member_peer_listed(secondary_id);
        if self.run_start_batch_fired {
            self.serve_run_start_trio_remainder(secondary_id).await;
        }
    }

    /// Serve a MID-RUN joiner the run-start halves of its setup trio (the
    /// roster half was just broadcast by the caller,
    /// [`Self::serve_setup_on_cert_exchange`]) and walk it to
    /// `Operational` on this primary's side — the per-member equivalent of
    /// what the run-start batch did for the bring-up fleet.
    ///
    ///   * The `InitialAssignment` is EMPTY by construction: a mid-run
    ///     joiner is a fresh incarnation holding no pre-assigned work — it
    ///     pulls via `TaskRequest` like every member post-start (and the
    ///     `MeshReady` it emits on entering its operational loop fires the
    ///     confirmation-edge dispatch wakeup in `handle_mesh_ready`, so
    ///     proactive dispatch reaches it too). An empty payload clobbers
    ///     nothing on the receiver: the secondary's handler just stamps the
    ///     dispatch-context flags and dispatches zero tasks. A REJOINING
    ///     member with in-flight history never crosses this path — it is
    ///     handled by frame-ingest re-admission (`primary::readmission`)
    ///     and, being operational, never re-sends the welcome/cert
    ///     handshake that leads here.
    ///   * The typestate walk reuses the existing per-member edges
    ///     ([`Self::advance_member_peers_ready`] /
    ///     `mark_member_operational`), so the member is keepalive-seeded at
    ///     the same "became operational" instant the batch path seeds —
    ///     once its operational keepalives flow it is silence-judgeable
    ///     like any member.
    ///
    /// Send failures are the mesh-pump-gone collapse (warn-and-continue,
    /// uniform with the batch fan-outs — the `mesh_pump_gone` latch is the
    /// collapse signal the operational loop consults).
    async fn serve_run_start_trio_remainder(&mut self, secondary_id: &str) {
        tracing::info!(
            secondary = %secondary_id,
            "run already started: serving the mid-run joiner the run-start \
             halves of its setup trio (empty InitialAssignment + \
             TransferComplete) so it can go operational and pull work"
        );
        self.advance_member_peers_ready(secondary_id);
        if let Err(error) = self
            .send_initial_assignment_to(secondary_id, Vec::new(), Vec::new(), Vec::new())
            .await
        {
            tracing::warn!(
                secondary_id = %secondary_id,
                error = %error,
                "mid-run InitialAssignment delivery failed"
            );
        }
        self.send_transfer_complete_to(secondary_id).await;
        self.mark_member_operational(secondary_id);
    }

    /// Batch peer-list delivery at the connect-wait resolution: the
    /// final convergence broadcast over the settled roster, plus the
    /// peer-list walk for any member the incremental serve has not
    /// already walked (none in the common path; the loop is the
    /// belt-and-braces over the same ONE per-member walk).
    pub(super) async fn send_peer_lists(&mut self) -> Result<(), String> {
        tracing::info!("sending peer lists");
        self.broadcast_peer_roster().await;

        // Transition all from CertExchanging -> PeerDiscovery
        let secondary_ids: Vec<String> = self.secondaries.keys().cloned().collect();
        for secondary_id in &secondary_ids {
            self.advance_member_peer_listed(secondary_id);
        }

        Ok(())
    }

    // ── Phase 4: Wait for Peer Connections ──

    pub(super) async fn wait_for_peer_connections(&mut self) -> Result<(), String> {
        // For single-secondary, skip peer connection wait
        let secondary_ids: Vec<String> = self.secondaries.keys().cloned().collect();

        for secondary_id in secondary_ids {
            self.advance_member_peers_ready(&secondary_id);
        }

        Ok(())
    }

    // ── Phase 5: Initial Assignment ──
}
