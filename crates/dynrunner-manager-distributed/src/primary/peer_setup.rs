use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, Destination, DistributedMessage, PeerConnectionInfo, PeerId,
    SetupBootstrapMessage,
};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use crate::state::SecondaryConnectionState;

use super::PrimaryCoordinator;
use super::wire::timestamp_now;

/// Minimum spacing between two duplicate-welcome re-serves to the SAME
/// member-incarnation (see
/// [`PrimaryCoordinator::re_serve_setup_on_duplicate_welcome`] and the
/// `reserve_backoff` field doc). A genuinely-lost trio frame is still
/// re-served promptly (the first duplicate welcome after a fresh
/// incarnation passes immediately — the map is cleared per incarnation),
/// but a member blocked downstream of delivery (wedged on mesh-settle,
/// never earning operational proof) cannot drive an unbounded re-serve
/// storm: at most one re-serve per this interval per member, however
/// fast its handshake retries (or wire-relayed duplicates) arrive.
///
/// Comfortably under the secondary's capped handshake-retry ceiling
/// (`secondary::setup::HANDSHAKE_RETRY_MAX` = 30s) so a steady-state
/// retry from a genuinely-lost-frame member is NEVER suppressed — the
/// gate bites only on the pathological burst the wire fan-in produces.
const RESERVE_BACKOFF: std::time::Duration = std::time::Duration::from_secs(5);

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
                // #556: the per-peer SLURM job id from the typestate
                // record. `None` for the pre-upgrade peer + the non-
                // SLURM-launched secondary (local/in-process, observer);
                // the respawn pipeline's scancel step skips `None`.
                slurm_job_id: s.slurm_job_id().map(str::to_string),
            })
            .collect()
    }

    /// Test-only view of the dial-driving roster `peer_roster()` builds —
    /// the SOLE `PeerInfo` candidate set. Lets the reap regression assert
    /// directly on what the transport is told to dial.
    #[cfg(test)]
    pub(crate) fn peer_roster_for_test(&self) -> Vec<PeerConnectionInfo> {
        self.peer_roster()
    }

    /// Build the wire `PeerInfo` frame over `peers` — the ONE frame
    /// construction, shared by the fleet broadcast
    /// ([`Self::broadcast_peer_roster`]) and the directed per-member
    /// delivery ([`Self::send_peer_roster_to`]). One builder, two
    /// addressings: the two paths can never drift in frame shape.
    fn peer_roster_frame(&self, peers: Vec<PeerConnectionInfo>) -> DistributedMessage<I> {
        SetupBootstrapMessage::PeerInfo {
            sender_id: self.config.node_id.clone(),
            timestamp: timestamp_now(),
            peers,
        }
        .into()
    }

    /// Send the CURRENT roster to ONE member, DIRECTED
    /// (`Destination::Secondary(id)`) — the same reliability class as the
    /// trio's other two frames (`InitialAssignment` / `TransferComplete`).
    ///
    /// Why a directed copy exists at all: the fleet broadcast resolves
    /// `Destination::All` to the transport legs REGISTERED AT THAT
    /// INSTANT, and nothing retransmits a missed broadcast on reconnect
    /// (see the transport's broadcast-honesty WARN). A joining member
    /// whose leg registration races the broadcast therefore never sees
    /// its roster — without it, no `ingest_peer_liveness_addrs`, no
    /// keepalives, and the member is silence-judged dead (the
    /// run_20260612_105712 replacement). A directed send instead rides
    /// the relay-capable router path (forwarded through any connected
    /// sibling, redial on relay), exactly like the two trio frames that
    /// DID arrive in that run.
    ///
    /// Send failures are the mesh-pump-gone collapse (warn-and-continue,
    /// uniform with the sibling setup sends).
    async fn send_peer_roster_to(&mut self, secondary_id: &str) {
        let msg = self.peer_roster_frame(self.peer_roster());
        if let Err(error) = self
            .send_to(
                Destination::Secondary(PeerId::from(secondary_id.to_string())),
                msg,
            )
            .await
        {
            tracing::warn!(
                secondary_id = %secondary_id,
                error = %error,
                "directed PeerInfo delivery failed"
            );
        }
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
        // operational variant fails to type-check here — see
        // `peer_roster_frame`, the ONE construction site), then broadcast
        // it through `send_to(Destination::All, ..)` — the same egress
        // every other primary broadcast uses. The wire bytes are identical
        // to the pre-mesh `PrimaryPeerSetupBootstrap` path (the `From`
        // conversion is the same one the adapter used internally); only the
        // routing surface changed, from a transport-direct adapter to the
        // queued mesh send. Per-secondary delivery failures are not folded
        // into a synchronous result anymore (the queued send has none): a
        // silent secondary surfaces through the heartbeat monitor, exactly
        // as the adapter's own docs noted the per-secondary signal already
        // did.
        let msg = self.peer_roster_frame(peers);
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
    ///     trio completing the member's setup gate is what ends its
    ///     handshake retry (the retry persists for the whole trio-wait;
    ///     see `secondary::setup::wait_for_setup`).
    ///
    /// Fires only on the `Handshaking → CertExchanging` edge (the caller,
    /// `connect::handle_cert_exchange`, gates on it), so a duplicate
    /// cert-exchange from the same incarnation never re-runs this serve.
    /// Retransmission for a member whose served frames were LOST is the
    /// duplicate-WELCOME path ([`Self::re_serve_setup_on_duplicate_welcome`]):
    /// once-per-incarnation here, re-served on demand there.
    ///
    /// The newcomer's roster is sent DIRECTED in addition to the fleet
    /// broadcast: the broadcast races the newcomer's mesh-leg
    /// registration (a `Destination::All` fan reaches only the legs
    /// registered at that instant, and a missed broadcast is never
    /// retransmitted), while the directed copy rides the relay-capable
    /// router path — the same delivery class that got the trio's other
    /// two frames through in run_20260612_105712 while the broadcast
    /// PeerInfo vanished. The broadcast stays: it is what converges every
    /// EARLIER member onto the grown roster.
    pub(super) async fn serve_setup_on_cert_exchange(&mut self, secondary_id: &str) {
        tracing::info!(
            secondary = %secondary_id,
            "serving setup incrementally: directed peer roster to the \
             newcomer + broadcasting the grown roster (earlier members \
             converge onto the fleet so far)"
        );
        self.send_peer_roster_to(secondary_id).await;
        self.broadcast_peer_roster().await;
        self.advance_member_peer_listed(secondary_id);
        if self.run_start_batch_fired {
            self.serve_run_start_trio_remainder(secondary_id).await;
        }
    }

    /// Re-serve the setup trio to a member that sent a DUPLICATE welcome
    /// — the retransmission half of the incremental serve (the
    /// cert-exchange edge above is once-per-incarnation by design, so
    /// this is the ONLY path that can replace a served-but-LOST frame).
    ///
    /// A duplicate welcome is the member's own statement that its setup
    /// gate has not released: the secondary's `wait_for_setup` re-offers
    /// the handshake on a capped backoff until the WHOLE trio has landed,
    /// so each re-welcome doubles as a trio-retransmit request (the
    /// run_20260612_105712 shape: the directed halves landed, the roster
    /// broadcast was lost to the leg-registration race, and the member
    /// sat at `got_peer_info=false` with nothing left to retransmit it).
    ///
    /// Suppression is keyed on per-incarnation OPERATIONAL PROOF, not on
    /// this primary's connection typestate: the mid-run serve walks a
    /// member `Operational` on the send side the instant the trio is
    /// SENT, so the typestate cannot distinguish "served and running"
    /// from "served and wedged". `keepalive_proven` can — it is inserted
    /// only when the member's post-`wait_for_setup` keepalive emitter
    /// provably runs, and cleared per incarnation (`seed_keepalive`). A
    /// proven member's straggler duplicate (a welcome already in flight
    /// when its gate released) is therefore NOT re-served — re-serving it
    /// would clear its proof via `mark_member_operational`'s keepalive
    /// re-seed and regress the silence sweep's judgment bound. An
    /// UNPROVEN member is re-served the full trio, all idempotent on the
    /// receiver (roster re-ingest, dispatch-flag re-stamp, gate re-set;
    /// an already-operational receiver debug-drops the run-start halves).
    ///
    /// Pre-cert members are skipped: their roster entry does not exist
    /// yet, and their own cert edge is the first serve. Pre-run-start the
    /// re-serve is roster-only, mirroring the first serve's governance
    /// (the quorum-proceed policy owns the run start).
    pub(super) async fn re_serve_setup_on_duplicate_welcome(&mut self, secondary_id: &str) {
        if self.keepalive_proven.contains(secondary_id) {
            return;
        }
        let servable = self
            .secondaries
            .get(secondary_id)
            .map(|s| s.is_at_least_cert_exchanged())
            .unwrap_or(false);
        if !servable {
            return;
        }
        // Per-incarnation re-serve backoff: a member blocked downstream of
        // delivery (wedged on mesh-settle, so it never earns operational
        // proof) keeps re-welcoming, and the wire can re-inject those
        // welcomes faster than its capped retry cadence. Re-serving an
        // already-delivered trio cannot unblock it, so an unbounded
        // re-serve per duplicate welcome is a CPU-burning livelock + log
        // flood. Suppress until `RESERVE_BACKOFF` after the last re-serve;
        // the map is cleared per incarnation (`seed_keepalive` / requeue
        // purge), so a genuinely-lost frame on a fresh incarnation is
        // still re-served at once. See the `reserve_backoff` field doc.
        let now = tokio::time::Instant::now();
        if self
            .reserve_backoff
            .get(secondary_id)
            .is_some_and(|due| now < *due)
        {
            tracing::trace!(
                secondary = %secondary_id,
                "duplicate welcome from an unproven member within the \
                 re-serve backoff window — the trio was already re-served \
                 recently; suppressing this re-serve (the member is blocked \
                 downstream of delivery, not missing a frame)"
            );
            return;
        }
        self.reserve_backoff
            .insert(secondary_id.to_string(), now + RESERVE_BACKOFF);
        tracing::info!(
            secondary = %secondary_id,
            run_started = self.run_start_batch_fired,
            "duplicate welcome from a member without operational proof: \
             re-serving its setup trio (directed roster{}) — its gate has \
             not released, so a served frame was lost in flight",
            if self.run_start_batch_fired {
                " + run-start halves"
            } else {
                "; run-start halves stay on the batch fan-out"
            }
        );
        self.send_peer_roster_to(secondary_id).await;
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
