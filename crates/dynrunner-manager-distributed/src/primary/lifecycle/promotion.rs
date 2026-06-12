use std::collections::HashSet;

use dynrunner_core::Identifier;
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};
use tokio::sync::mpsc as tokio_mpsc;

use crate::primary::PrimaryCoordinator;
use crate::primary::command_channel::PrimaryCommand;

/// The final-pick rule [`PrimaryCoordinator::select_relocation_target`]
/// applies within the shared eligibility set (alive ∩ can_be_primary −
/// observers − self). One selection function, policy-keyed — never a
/// forked second selector. See the method doc for the per-policy
/// semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RelocationPolicy {
    /// Deterministic lowest-id pick — the bootstrap handoff's original
    /// behaviour.
    LowestId,
    /// Highest CRDT-derived `InFlight` occupancy, ties lex-lowest id,
    /// zero-occupancy candidates excluded — the graceful-abort drain
    /// relocation ("the secondary with the most active workers").
    MostActiveWorkers,
}

impl<S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<S, E, I> {
    /// Block on every connected secondary reporting `MeshReady`
    /// before this operational primary asserts authority
    /// (`activate_local_primary`) and starts driving dispatch over
    /// the peer mesh. The 750µs gap
    /// between "all secondaries cert-exchanged" and the previous
    /// promotion call left the promoted secondary
    /// authoritative against a still-forming peer mesh — every
    /// pre-mesh-formation message went into the void for the
    /// 30s peer-dial budget. Closing the gap means waiting until
    /// each secondary has signalled its mesh has settled (mesh
    /// formed, watchdog elapsed, or no peers were expected for
    /// single-secondary).
    ///
    /// CALLED ONLY on the `BootstrapRole::PromotedDestination` arm of
    /// `run_pipeline`, AFTER `perform_initial_assignment` +
    /// `send_transfer_complete`. `MeshReady` is the secondary's report
    /// from its OPERATIONAL loop, which it reaches only after consuming an
    /// `InitialAssignment` — so the report is satisfiable only once an
    /// operational primary has sent one. The SETUP PEER (which relocates
    /// the role away without ever sending an assignment) must NOT call
    /// this: gating its relocate on a signal it never triggers is a
    /// circular deadlock. The setup peer relies instead on the
    /// transport-level peer-mesh formation the Node pump drives off
    /// `PeerInfo` (transport ⊥ role/operational), which makes its
    /// `PrimaryChanged { Transferred }` route over the live links without
    /// any operational handshake.
    ///
    /// Bounded by `config.mesh_ready_timeout` (default 60s):
    /// stragglers past the deadline log a warning and the run
    /// proceeds anyway. A buggy secondary that never emits
    /// `MeshReady` must not be able to deadlock the entire
    /// dispatch pipeline; the post-promotion paths are all
    /// already failure-tolerant against an absent peer.
    ///
    /// Cancellation safety: `transport.recv_peer` is the cancel-safe
    /// unified inbound demux; `sleep_until` is one-shot cancel-safe per
    /// tokio docs. The `select!` here mirrors the same shape
    /// `wait_for_connections` uses one phase up.
    pub(crate) async fn wait_for_mesh_ready(
        &mut self,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
    ) -> Result<(), String> {
        // The expected set is the LIVE PEER-secondary roster captured
        // AT this moment (post-quorum, post-cert-exchange). It is
        // not `config.num_secondaries` because the connect phase
        // may have dropped no-show secondaries on its own
        // timeout — we only wait for who's actually here.
        //
        // Two ids that `known_secondaries()` (the raw capacity-record
        // roster) carries must NOT be in the expected set, because neither
        // can ever satisfy the `MeshReady` this wait blocks on:
        //   * SELF. The promoted primary is itself a worker-secondary, so
        //     it holds a `SecondaryCapacity` record and appears in
        //     `known_secondaries()` — but a node never emits `MeshReady`
        //     ABOUT ITSELF to itself. Waiting on it always burns the full
        //     `mesh_ready_timeout`.
        //   * The DEPARTED ex-primary. `PeerRemoved` marks a dead node
        //     `peer_state = Dead` but LEAVES its `SecondaryCapacity`
        //     record in place (the record is sticky; see
        //     `apply_peer_removed`), so the just-scancelled ex-primary that
        //     triggered this election still appears in
        //     `known_secondaries()` — yet it is dead and will never report.
        // `alive_secondary_members()` is the authoritative live-membership
        // roster (worker_count > 0 ∧ `is_peer_alive`), which structurally
        // excludes the dead ex-primary; the `id != node_id` filter excludes
        // self. So the expected set is exactly the LIVE PEER secondaries —
        // the only nodes that can emit a `MeshReady` this wait can observe.
        // (On the cold path this is a strict no-op-or-improvement: it can
        // only remove ids that would never have reported anyway.)
        let own_id = self.config.node_id.as_str();
        let expected: HashSet<String> = self
            .cluster_state
            .alive_secondary_members()
            .filter(|id| *id != own_id)
            .map(String::from)
            .collect();
        if expected.is_empty() {
            tracing::debug!("no secondaries connected; skipping wait_for_mesh_ready");
            return Ok(());
        }

        // Fast path: messages may have already arrived before this
        // step ran (the welcome/cert-exchange/peer-info loop above
        // is event-driven and a fast secondary can emit MeshReady
        // before we enter the wait).
        if expected.is_subset(&self.mesh_ready_secondaries) {
            tracing::info!(
                count = expected.len(),
                "all secondaries reported MeshReady before wait step"
            );
            return Ok(());
        }

        let deadline = tokio::time::Instant::now() + self.config.mesh_ready_timeout;
        tracing::info!(
            expected = expected.len(),
            already_reported = self.mesh_ready_secondaries.len(),
            timeout_s = self.config.mesh_ready_timeout.as_secs_f64(),
            "waiting for peer-mesh formation across secondary fleet before \
             promoting primary"
        );

        // Pre-operational keepalive cadence. The bootstrap region
        // (`perform_initial_assignment` → here → `activate_local_primary`
        // → `operational_loop`) can outlast the secondary's
        // primary-silence deadline (`keepalive_interval *
        // keepalive_miss_threshold`) while waiting for the mesh to form,
        // yet the operational loop — the only place keepalives ticked —
        // hasn't started. Tick the SAME emitter here so liveness is
        // asserted across the whole pre-operational window. Same shape
        // as `operational_loop`'s `heartbeat_tick`: the immediate first
        // tick is skipped (secondaries may not have sent their own first
        // keepalive yet), the cadence is `keepalive_interval`.
        let mut heartbeat_tick = tokio::time::interval(self.config.keepalive_interval);
        // Skip (not Burst) missed ticks: collapse a suspend/resume backlog to a
        // single catch-up tick rather than bursting one keepalive per missed
        // interval. Same rationale as the operational loop's `heartbeat_tick`.
        heartbeat_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        heartbeat_tick.tick().await;

        loop {
            if expected.is_subset(&self.mesh_ready_secondaries) {
                tracing::info!(
                    count = expected.len(),
                    "all secondaries reported MeshReady; releasing PrimaryChanged announcement"
                );
                return Ok(());
            }

            tokio::select! {
                msg = self.inbox.recv() => {
                    match msg {
                        // Pre-operational-loop site. See
                        // `wait_for_connections` for the matching
                        // rationale: thread `command_rx` through so an
                        // `on_phase_end` callback fired by a
                        // TaskComplete arriving during this wait can
                        // queue `SpawnTasks` and have it applied
                        // inline, refreshing `total_tasks` BEFORE
                        // `operational_loop`'s entry-time exit check
                        // sees the post-spawn ledger.
                        Some(m) => {
                            self.dispatch_message(m, command_rx).await?;
                            // In-wait dispatch servicing: `TasksAdded`
                            // signals queued DURING this wait would
                            // otherwise park until the operational loop
                            // starts (it hasn't). Two emitters fire here:
                            //
                            //   * a `SecondaryCapacity` landing mid-wait
                            //     (`react_to_capacity_growth`) grows the
                            //     roster with fresh idle slots;
                            //   * a `MeshReady` landing mid-wait
                            //     (`handle_mesh_ready`) confirms its member
                            //     into the assignable set — the
                            //     confirmation-edge wakeup.
                            //
                            // Servicing the bus inline runs the dispatch
                            // recheck NOW, so ready work flows to every
                            // member the readiness gate
                            // (`member_mesh_confirmed`) admits, as each
                            // confirmation arrives — instead of pooling
                            // until after the wait. The `MeshReady` this
                            // wait blocks on is NOT dispatch-driven: a
                            // secondary reaches its operational loop by
                            // consuming the setup trio (whose
                            // `InitialAssignment` fan-out is ungated and
                            // already ran above) and reports from there
                            // (entry hook / watchdog / keepalive tick).
                            // (rc-C's decoupling and the post-assignment
                            // placement of this wait stay intact — the
                            // recovery dispatches, it does not move the
                            // wait.)
                            self.drain_and_react_to_pending_worker_signals().await;
                        }
                        None => return Err("transport closed during wait_for_mesh_ready".into()),
                    }
                }
                _ = heartbeat_tick.tick() => {
                    // Reuse the heartbeat module's sole emitter so the
                    // pre-operational window asserts liveness on the
                    // same cadence the operational loop uses. No
                    // spawned task, no second send path — one keepalive
                    // origination point shared across the lifecycle.
                    self.broadcast_primary_keepalive().await;
                }
                _ = tokio::time::sleep_until(deadline) => {
                    let missing: Vec<String> = expected
                        .difference(&self.mesh_ready_secondaries)
                        .cloned()
                        .collect();
                    tracing::warn!(
                        missing = ?missing,
                        reported = self.mesh_ready_secondaries.len(),
                        expected = expected.len(),
                        timeout_s = self.config.mesh_ready_timeout.as_secs_f64(),
                        "mesh-ready timeout: some secondaries never reported \
                         MeshReady; proceeding with the PrimaryChanged \
                         announcement anyway. The newly-named primary may \
                         briefly route into a partially-formed peer mesh until \
                         those secondaries finish (or fail) their dials."
                    );
                    return Ok(());
                }
            }
        }
    }

    /// Activate THIS node as the authoritative primary. The single
    /// mechanism both handoff sides converge on (the brief's
    /// `activate_local_primary`): bootstrap (the run pipeline reaches its
    /// operational loop) and failover (the election's terminal `Promoted`
    /// transition) both call this.
    ///
    /// # Uniform primary announcement
    ///
    /// Every primary — bootstrap and promoted alike — originates
    /// `ClusterMutation::PrimaryChanged { new = self, epoch }` here, so
    /// `current_primary()` resolves to this host uniformly cluster-wide
    /// through the one peer mesh. The submitter is now a routable mesh
    /// peer (registered in every replica's `connections` under its
    /// host-id), so warming the `Role::Primary` cache to its id routes
    /// correctly — there is no longer a "sole authority" special case
    /// that suppresses the announce. The epoch is a SINGLE transition (see
    /// `originate_primary_changed`): on the promoted paths the converged
    /// snapshot already names this host, so the announce RE-ASSERTS at the
    /// epoch already held rather than bumping again — the relocate→promotion
    /// is one epoch step, not two. The re-announce names the same holder the
    /// upstream `PrimaryChanged` already installed, so the primary-changed
    /// important-event hook (which fires only on a genuine holder transition)
    /// stays silent.
    ///
    /// `primary_id` is set to this node's own id for the heartbeat
    /// requeue path's "did the primary just die?" check — which can
    /// never match a secondary id, so the authority never self-clears
    /// the pointer.
    pub(crate) async fn activate_local_primary(&mut self) -> Result<(), String> {
        self.primary_id = Some(self.config.node_id.clone());

        // Uniform primary announcement: originate `PrimaryChanged { new
        // = self }` so `current_primary()` resolves to this host
        // cluster-wide through the one mesh. This is THE single
        // bootstrap+failover convergence point at which this node asserts
        // primary authority; the announce warms the `Role::Primary` cache
        // to this now-routable mesh peer (replacing the old "sole
        // authority" suppression). `primary_epoch() + 1` strictly
        // supersedes the prior identity via epoch-LWW. The replicated
        // `PrimaryChanged` apply drives the primary-changed important
        // event (registered as a role-change hook at construction), so
        // the LLM-wake milestone is emitted uniformly on every primary
        // transition without a hand-written per-call-site log line.
        self.originate_primary_changed().await;

        // Liveness spans authority, not the operational phase. This is
        // the single bootstrap+failover convergence point (the brief's
        // `activate_local_primary`), so emit one keepalive here the
        // moment authority is asserted — before `operational_loop`'s
        // `heartbeat_tick` is even constructed. Without this a just-
        // promoted/just-bootstrapped primary is silent over the
        // authority↔worker link until the first operational tick fires,
        // and a secondary that stopped seeing keepalives during the
        // handoff window would trip `primary_silent` and re-elect.
        // Reuses the heartbeat module's sole emitter (no spawned task,
        // no second send path) so there remains exactly one keepalive
        // origination point.
        self.broadcast_primary_keepalive().await;

        Ok(())
    }

    /// Choose the compute peer this primary relocates its role to.
    ///
    /// Single concern: the deterministic selection over the ONE shared
    /// eligibility set — `alive_secondary_members() ∩ can_be_primary −
    /// observers`, computed off this primary's replicated `cluster_state`,
    /// with this primary's own id excluded (defensively on the bootstrap
    /// path — the submitter advertises no worker capacity; load-bearing on
    /// the graceful-abort path — a compute-peer primary IS in the set). A
    /// peer is eligible iff it is an alive worker-secondary
    /// (worker_count > 0, the structural exclusion of observers in
    /// `alive_secondary_members`) that carries the explicit
    /// `RoleTable.can_be_primary` capability and is NOT in the observers
    /// set. The `observers` filter is belt-and-suspenders over the
    /// worker-capacity exclusion.
    ///
    /// The FINAL pick within the eligible set is the caller-supplied
    /// [`RelocationPolicy`] — one selection function, two policies, never a
    /// forked sibling:
    ///
    ///   * [`RelocationPolicy::LowestId`] — the bootstrap handoff's
    ///     deterministic `.min()` (byte-identical to the pre-policy
    ///     behaviour).
    ///   * [`RelocationPolicy::MostActiveWorkers`] — the graceful-abort
    ///     drain relocation: the eligible secondary with the MOST
    ///     replicated `InFlight` assignments (CRDT-derived occupancy, so
    ///     every replica agrees), ties broken lex-lowest id for
    ///     determinism; candidates with ZERO active work are excluded (a
    ///     drained secondary is about to tear itself down — relocating
    ///     onto it would strand the role).
    ///
    /// `None` when no eligible peer satisfies the policy; the bootstrap
    /// caller maps that to a hard
    /// [`crate::primary::RunError::NoRelocationTarget`] (pillar 2: the
    /// submitter must never stay the run's primary), the graceful-abort
    /// caller simply stays put and drains in place.
    pub(crate) fn select_relocation_target(&self, policy: RelocationPolicy) -> Option<String> {
        let observers = &self.cluster_state.role_table().observers;
        let own_id = self.config.node_id.as_str();
        let eligible = self
            .cluster_state
            .alive_secondary_members()
            .filter(|id| *id != own_id)
            .filter(|id| !observers.contains(*id))
            .filter(|id| self.cluster_state.can_be_primary(id));
        match policy {
            RelocationPolicy::LowestId => eligible.min().map(str::to_string),
            RelocationPolicy::MostActiveWorkers => eligible
                .map(|id| (self.cluster_state.inflight_count_for_secondary(id), id))
                .filter(|(active, _)| *active > 0)
                // Max by active count; ties go to the LEX-LOWEST id (the
                // reversed id compare makes the lower id the `max_by`
                // winner), so the pick is deterministic across replicas.
                .max_by(|(na, ia), (nb, ib)| na.cmp(nb).then_with(|| ib.cmp(ia)))
                .map(|(_, id)| id.to_string()),
        }
    }

    /// Hand the primary role to `chosen` by originating
    /// `PrimaryChanged { new: chosen, epoch: primary_epoch()+1, reason:
    /// Transferred }`.
    ///
    /// Single concern: the bootstrap-tail role TRANSFER origination. Modelled
    /// on [`Self::originate_primary_changed`] (`new = self`) but with the
    /// opposite intent — it names ANOTHER peer the primary, so it must NOT
    /// set `primary_id = self` and must NOT self-keepalive (this host is
    /// stepping DOWN, not asserting authority). The local apply of the
    /// mutation advances `current_primary` to `chosen`; the role-change hook
    /// installed by `register_demote_on_displaced` observes the self→other
    /// flip and fires the demote signal, so `run_consuming` relocates this
    /// coordinator by-value into a standalone observer
    /// (`PrimaryRunOutcome::Relocated`). The fan-out reaches the chosen peer,
    /// whose secondary self-names primary and fires its `PromotionSignal` —
    /// the Node builds the snapshot-seeded promoted primary.
    ///
    /// # Convergence — `Transferred` is advisory; `chosen` is not asserted to win
    ///
    /// The `Transferred` reason is advisory routing metadata only: the
    /// epoch-LWW register-adopt rule ("higher epoch wins; equal epoch →
    /// lex-lower id wins") in `apply.rs` is reason-blind. So this origination
    /// MUST tolerate the `RoleTable` converging on a DIFFERENT (lex-lower)
    /// successor than `chosen`: a concurrent failover election at the SAME
    /// `epoch+1` can win the equal-epoch lex tiebreak, making this local
    /// `PrimaryChanged { chosen }` apply a NoOp. That is correct + convergent
    /// — the hook still fires (on the WINNING apply, which names winner ≠
    /// self), so the submitter still demotes, just toward the winner rather
    /// than `chosen`. This method deliberately adds NO logic asserting
    /// `chosen` won; it only originates the handoff intent.
    ///
    /// # Keepalive window — no spurious failover against the relocating-away primary
    ///
    /// Between originating `PrimaryChanged { chosen }` and `chosen` announcing
    /// its own primary keepalives, this host has stepped down. No surviving
    /// secondary arms a failover in that window: (a) `chosen` is drawn from
    /// `alive_secondary_members()` — already a connected mesh peer — so
    /// `send_to_primary` resolves and never no-routes, and the in-place
    /// primary→observer retag (`swap_primary_to_observer`) keeps THIS host's
    /// QUIC connection alive, so the fast failover leg
    /// (`primary_link::should_arm_failover`, armed only on a send no-route)
    /// never arms on either side of the apply; (b) applying `PrimaryChanged`
    /// resets any mid-Suspecting secondary to Normal; (c) the patient silence
    /// backstop (~120s) is far beyond the sub-second relocation window. So the
    /// relocate path introduces no NEW window — it reuses the same
    /// mesh-routable-observer dynamics as the already-proven failover-demote.
    pub(crate) async fn relocate_primary_to(&mut self, chosen: String) {
        let epoch = self.cluster_state.primary_epoch() + 1;
        tracing::info!(
            target: super::super::important_events::IMPORTANT_TARGET,
            chosen = %chosen,
            epoch,
            "relocating primary role to compute peer (bootstrap handoff)"
        );
        let repoint = dynrunner_protocol_primary_secondary::ClusterMutation::PrimaryChanged {
            new: chosen,
            epoch,
            reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Transferred,
        };
        self.apply_and_broadcast_cluster_mutations(vec![repoint.clone()])
            .await;
        // Directed observer fan of the handoff re-point — the `All`
        // broadcast is the direct-leg fan only, so a relay-only observer
        // would otherwise keep recognizing (and silence-judging) the
        // OLD primary forever. See `fan_repoint_to_observers`.
        self.fan_repoint_to_observers(repoint).await;
    }
}
