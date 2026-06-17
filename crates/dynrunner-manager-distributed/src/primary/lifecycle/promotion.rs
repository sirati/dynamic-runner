use std::collections::HashSet;

use dynrunner_core::Identifier;
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use crate::primary::PrimaryCoordinator;

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
    /// The LIVE expected peer-mesh roster: the alive PEER secondaries
    /// (worker_count > 0 ∧ `is_peer_alive`) other than self. NOT
    /// `config.num_secondaries` — the connect phase may have dropped
    /// no-show secondaries, so the requirement is keyed on who is actually
    /// alive RIGHT NOW.
    ///
    /// Two ids that `known_secondaries()` (the raw capacity-record roster)
    /// carries are structurally excluded here because neither can ever
    /// emit a `MeshReady` this gate observes:
    ///   * SELF. The promoted primary is itself a worker-secondary, so it
    ///     holds a `SecondaryCapacity` record — but a node never emits
    ///     `MeshReady` ABOUT ITSELF to itself (the `id != node_id` filter).
    ///   * The DEPARTED ex-primary. `PeerRemoved` leaves the sticky
    ///     `SecondaryCapacity` record in place, but `alive_secondary_members()`
    ///     excludes it (not `is_peer_alive`).
    fn expected_mesh_roster(&self) -> HashSet<String> {
        let own_id = self.config.node_id.as_str();
        self.cluster_state
            .alive_secondary_members()
            .filter(|id| *id != own_id)
            .map(String::from)
            .collect()
    }

    /// Non-blocking peer-mesh-formation predicate — the BACKGROUND deadline
    /// the operational loop polls, NOT a blocking pre-operational wait.
    ///
    /// # Decoupled bring-up (mesh-ready does NOT stop running)
    ///
    /// The promoted primary announces `PrimaryChanged` + enters its
    /// operational loop + dispatches IMMEDIATELY — it does NOT block on
    /// peer-mesh formation. Dispatch + terminals ride the independent
    /// primary↔secondary leg (`should_skip_worker_for_dispatch`: dispatch
    /// ⊥ peer mesh) and terminals self-heal via confirmable-replay, so the
    /// peer mesh is needed ONLY as the failover substrate, never to run
    /// work. Blocking bring-up on it idled the fleet through the whole
    /// formation window — the symptom this decouple removes.
    ///
    /// # The ≥2-node requirement, enforced as a background deadline
    ///
    /// Returns:
    ///   * `None` — the requirement is SATISFIED or NOT APPLICABLE:
    ///       - a <2-node fleet (lone secondary, with or without the folded
    ///         primary-host secondary) has NO peer mesh to form and NO
    ///         failover possible — no requirement; OR
    ///       - every expected secondary has reported a FORMED mesh
    ///         (`peer_count >= 1`; `expected ⊆ mesh_ready_secondaries`).
    ///   * `Some(missing)` — a ≥2-node fleet whose mesh is NOT yet formed;
    ///     `missing` is the expected secondaries that have not reported.
    ///     The op-loop's background deadline arms on this and, if it is
    ///     still `Some` at the deadline, returns the abort `Err` — routed
    ///     through `run_pipeline`'s #563-Seam-0 chokepoint to the
    ///     `RunAborted` verdict + the run-wrapper's worker teardown (the
    ///     SAME terminal path the old blocking-wait Err used). A mesh that
    ///     forms before the deadline flips this to `None` and clears the
    ///     deadline — slow-but-forms never aborts.
    pub(crate) fn mesh_formation_missing(&self) -> Option<Vec<String>> {
        // <2 compute nodes: no peer mesh, no failover, no requirement.
        // `alive_secondary_members().count()` is the TOTAL compute-secondary
        // count INCLUDING this primary's own folded worker-secondary, so it
        // is the faithful "how many compute nodes can mesh" quantity.
        if self.cluster_state.alive_secondary_members().count() < 2 {
            return None;
        }
        let expected = self.expected_mesh_roster();
        if expected.is_subset(&self.mesh_ready_secondaries) {
            return None;
        }
        Some(
            expected
                .difference(&self.mesh_ready_secondaries)
                .cloned()
                .collect(),
        )
    }

    /// The abort reason a ≥2-node fleet whose peer mesh never formed within
    /// the background deadline surfaces — the SAME wording the old blocking
    /// wait returned, so log-grepping ops scripts and the `RunAborted`
    /// reason text are unchanged across the blocking→background relocation.
    pub(crate) fn mesh_formation_abort_reason(&self, missing: &[String]) -> String {
        let expected_len = self.expected_mesh_roster().len();
        format!(
            "peer mesh did not form within {:?}: {} of {} expected \
             secondaries never reported a formed peer mesh ({:?}); \
             the failover substrate is unavailable — aborting the run",
            self.config.mesh_ready_timeout,
            missing.len(),
            expected_len,
            missing,
        )
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
