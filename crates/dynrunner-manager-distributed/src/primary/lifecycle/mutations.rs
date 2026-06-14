use dynrunner_core::{BoundedString, Identifier};
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, Destination, DistributedMessage, PeerId, RemovalCause,
};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use crate::cluster_state::apply_locally_for_broadcast;
use crate::primary::PrimaryCoordinator;
use crate::primary::wire::timestamp_now;
use crate::worker_signal::WorkerMgmtSignal;

impl<S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<S, E, I> {
    /// Apply each mutation locally and broadcast the same batch so every
    /// secondary mirrors the change. Per-secondary delivery failures are
    /// logged at warn — the CRDT is idempotent, so a missed mutation is
    /// recoverable from the next snapshot RPC (Phase B); we never block
    /// dispatch on universal delivery.
    pub(crate) async fn apply_and_broadcast_cluster_mutations(
        &mut self,
        mutations: Vec<ClusterMutation<I>>,
    ) {
        if mutations.is_empty() {
            return;
        }
        // Apply locally and keep only mutations the CRDT actually
        // changed state for. Pre-fix every mutation was re-broadcast
        // unconditionally; under #50's peer-forwarding redundancy
        // (every peer secondary forwards observed-via-peer-mesh
        // terminal events to the primary), that would amplify each
        // unique TaskComplete into N re-broadcasts to N secondaries
        // = N² messages per event. The CRDT's terminal-lock semantics
        // turn duplicate applies into NoOp; skipping the NoOp arm
        // keeps the wire fan-out at one broadcast per genuinely-new
        // state transition regardless of how many peer-forward
        // paths converge on us. The apply+filter primitive lives in
        // `cluster_state::apply_locally_for_broadcast` so this
        // originator path and the secondary-side originator
        // (`secondary::origination::apply_and_broadcast_mutations`, used
        // by the panik self-departure) share one
        // canonical filter; the broadcast step stays at each call site
        // because the two transports have different error shapes.
        //
        // `apply_locally_for_broadcast` also surfaces any `TaskInfo`s
        // the apply pass auto-resumed from `Blocked → Pending` (a
        // `TaskCompleted` arm side-effect — every dependent whose
        // `Blocked { on: <this hash> }` matches transitions back to
        // `Pending` in the CRDT). On the live primary, those binaries
        // were dropped from the pool by `pool.on_item_failed_permanent`
        // when the cascade originally fired, so the pool needs them
        // re-introduced; the broadcast itself carries no `TaskInfo`
        // for these dependents, only the CRDT side knows. Re-inject
        // each resumed binary into the live pool so the next dispatch
        // tick picks them up.
        let batch = apply_locally_for_broadcast(&mut self.cluster_state, mutations);
        let crate::cluster_state::AppliedBatch {
            applied,
            resumed_for_dispatch,
            became_pending,
        } = batch;
        let resumed_any = !resumed_for_dispatch.is_empty();
        for binary in resumed_for_dispatch {
            tracing::debug!(
                phase = %binary.phase_id,
                task_id = ?binary.task_id,
                "pool: re-inject auto-resumed Blocked dependent"
            );
            self.pool_mut().reinject(binary);
        }
        // Auto-resumed Blocked dependents are a pool-entry edge: their
        // prereq just completed and they became dispatchable, but the
        // free worker that would run them won't re-poll on its own. EMIT
        // a `TasksAdded` so the worker-management recheck dispatches
        // them (decoupled emit, never a direct dispatch call — the
        // dispatch-decoupling law).
        if resumed_any {
            self.cluster_state
                .emit_worker_mgmt(WorkerMgmtSignal::TasksAdded);
        }
        // SecondaryAffine ready-resolution originator (#497, the WHEN). A
        // `TaskKind::SecondaryAffine` gate that JUST became Pending-all-
        // resolved — either born so at spawn (no deps) or resumed so when
        // its upload dep completed — must transition to the terminal
        // `AffineReady` (READY-not-EXECUTED) so its dependents unblock
        // without the primary ever executing it. `became_pending` carries
        // the union of the resume + spawn surfaces; the detection
        // (Pending-gate ∧ all-deps-resolved) is owned by `cluster_state`.
        // Broadcast through THIS method recursively so the follow-up
        // inherits the identical apply+filter+capture+settle semantics AND
        // so a CHAIN of gates (I1 depends on I2, both SecondaryAffine)
        // converges: applying I2's `AffineReady` resumes I1 `Blocked →
        // Pending`, which rides the recursive call's own `became_pending`
        // and originates I1's `AffineReady` in turn. The recursion
        // terminates: each step strictly drains the finite set of
        // not-yet-ready gates (a gate goes `Pending → AffineReady` exactly
        // once, and the apply arm NoOps a re-application).
        if !became_pending.is_empty() {
            let affine_ready = self
                .cluster_state
                .affine_ready_mutations_for(became_pending);
            if !affine_ready.is_empty() {
                Box::pin(self.apply_and_broadcast_cluster_mutations(affine_ready)).await;
            }
        }
        // Worker-roster growth edge — the symmetric twin of the
        // pool-entry edge above. A `SecondaryCapacity` this batch ACTUALLY
        // applied (in `applied`, so the set-once CRDT record was vacant —
        // a genuinely new secondary, not a redundant re-emit) means a
        // worker became ready: a new idle slot now exists in the
        // replicated ledger but not yet in `self.workers`. Rebuild the
        // roster from the now-grown capacity set and emit `TasksAdded` so
        // the dispatch recheck re-evaluates the new idle slot against the
        // ready pool. Owns the same originator-side derived-cache coherence
        // this method already does for resumed pool entries. The
        // `applied`-gated detection makes it one-shot: the set-once
        // capacity apply NoOps on every re-emit, so a re-delivered
        // `SecondaryCapacity` never re-triggers a rebuild.
        let capacity_grew = applied
            .iter()
            .any(|m| matches!(m, ClusterMutation::SecondaryCapacity { .. }));
        if applied.is_empty() {
            return;
        }
        if capacity_grew {
            self.react_to_capacity_growth();
        }
        // CAPTURE seam (F5 atomic effect+terminal batching): while a
        // mutation capture is armed (`begin_mutation_capture`), every
        // locally-applied batch is DIVERTED off the wire into the
        // capture buffer instead of broadcasting per-call — the local
        // apply (and every local side effect above: pool re-injection,
        // capacity reaction, buffered worker-mgmt emits) already
        // happened identically to the uncaptured path, so the captured
        // batch is exactly "what would have been broadcast", already
        // origination-stamped and NoOp-filtered. The capture owner
        // flushes the accumulated batch as ONE wire frame via
        // [`Self::broadcast_applied_mutations`], so every replica
        // applies the whole batch or none of it (the replica-side
        // batch apply is a synchronous per-frame loop).
        if let Some(capture) = self.mutation_capture.as_mut() {
            capture.extend(applied);
            return;
        }
        self.broadcast_applied_mutations(applied).await;
    }

    /// Arm the mutation-capture sink: until [`Self::take_mutation_capture`],
    /// every `apply_and_broadcast_cluster_mutations` call applies locally
    /// as usual but appends its NoOp-filtered batch to the capture buffer
    /// instead of broadcasting. NOT re-entrant — there is exactly one
    /// capture owner at a time (the F5 atomic effect+terminal flush).
    pub(crate) fn begin_mutation_capture(&mut self) {
        debug_assert!(
            self.mutation_capture.is_none(),
            "mutation capture is not re-entrant"
        );
        self.mutation_capture = Some(Vec::new());
    }

    /// Disarm the capture sink and return everything captured since
    /// [`Self::begin_mutation_capture`], in apply order. The caller owns
    /// flushing the batch (via [`Self::broadcast_applied_mutations`]) —
    /// or dropping it, in which case the mutations were still applied
    /// LOCALLY (capture only diverts the wire leg), so dropping is only
    /// sound for batches that never contained anything (the F5 discard
    /// path never opens a capture window at all for exactly this
    /// reason: discarded commands are rejected UNEXECUTED).
    pub(crate) fn take_mutation_capture(&mut self) -> Vec<ClusterMutation<I>> {
        self.mutation_capture
            .take()
            .expect("take_mutation_capture without begin_mutation_capture")
    }

    /// THE single wire egress for primary-originated, already-locally-
    /// applied mutation batches: ship the batch as ONE
    /// `DistributedMessage::ClusterMutation` frame over the
    /// `Destination::All` egress edge, matching the primary keepalive
    /// path (`broadcast_primary_keepalive`) — one mesh broadcast to
    /// every member. Callers: the uncaptured tail of
    /// `apply_and_broadcast_cluster_mutations`, and the F5 atomic
    /// effect+terminal flush (a captured batch). The single mesh
    /// transport collapses per-secondary delivery failures into one
    /// `String` (the per-secondary signal is the heartbeat monitor, not
    /// this log line). The CRDT is idempotent, so a missed mutation is
    /// recoverable from the next snapshot RPC; we never block dispatch
    /// on universal delivery.
    pub(crate) async fn broadcast_applied_mutations(
        &mut self,
        mutations: Vec<ClusterMutation<I>>,
    ) {
        if mutations.is_empty() {
            return;
        }
        let msg = DistributedMessage::ClusterMutation {
            target: None,
            sender_id: self.config.node_id.clone(),
            timestamp: timestamp_now(),
            mutations,
        };
        if let Err(error) = self.send_to(Destination::All, msg).await {
            tracing::warn!(
                error = %error,
                "ClusterMutation broadcast delivery failed"
            );
        }
    }

    /// Broadcast a TERMINAL run verdict and hold the settle window so it
    /// lands on every connected peer before the dispatchers tear the
    /// transport down.
    ///
    /// THE single origination mechanism for the run's replicated terminal
    /// fact (#313): `ClusterMutation::RunComplete` (the success latch) and
    /// `ClusterMutation::RunAborted { reason }` (the failure twin) both
    /// ship through here — same `apply_and_broadcast_cluster_mutations`
    /// pipeline, same `PRIMARY_BROADCAST_SETTLE` window. Consumers: a
    /// secondary's `process_tasks` loop (and its setup-wait twin) exits on
    /// either latch (`RunAborted` → `SecondaryTerminal::Aborted`, non-zero
    /// at the PyO3 boundary); the observer's `evaluate_exit` relays the
    /// verdict to the operator (aborted checked FIRST, never narrated as
    /// complete). Best-effort by design: the primary is about to exit, so
    /// delivery failures are logged, never propagated.
    ///
    /// # Which exit paths broadcast a verdict (the #313 classification)
    ///
    /// A verdict fires ONLY on a DELIBERATE terminal exit — the run is
    /// over (cleanly, or unsalvageably failed) and the fleet must tear
    /// down. It must NEVER fire where the election should win instead: a
    /// primary that dies WITHOUT a verdict is exactly the case failover
    /// exists for (the survivors elect + promote from the replicated
    /// CRDT and the run continues).
    ///
    /// VERDICT (deliberate terminal — the fleet tears down):
    /// - clean counter finish → `RunComplete`
    ///   (`finalize_terminal_accounting`);
    /// - routing collapse, `stranded > 0` → `RunAborted`
    ///   (`finalize_terminal_accounting`, also reached by the pre-loop
    ///   `bail_to_finalize` collapse gates);
    /// - wholesale runtime spawn rejection → `RunAborted`
    ///   (`finalize_terminal_accounting`; pre-#313 this path broadcast a
    ///   FALSE `RunComplete` and only the local return was honest);
    /// - the worker-management fail latch (`RunShouldFail` /
    ///   `PolicyFatalExit` → `Other` / `FatalPolicyExit`) → `RunAborted`
    ///   (`run_operational_and_finalize`): a deliberate policy decision
    ///   that the run must fail — a promoted successor would inherit the
    ///   same replicated facts and re-decide it;
    /// - pre-phase duplicate task id (#3a) → `RunAborted`
    ///   (`fire_pending_run_abort`);
    /// - post-phase duplicate task id (#3b) → `RunAborted`
    ///   (`invalidate_all_pending`, latched BEFORE the run-wide
    ///   `TaskFailed` wipe so the duplicate-identity reason is the FIRST
    ///   writer and a later hook-raise render can never overwrite it);
    /// - `NoRelocationTarget` → `RunAborted` (`run_pipeline`'s SetupPeer
    ///   arm): NO peer advertised `can_be_primary`, so an election can
    ///   never produce a primary — without the verdict the connected
    ///   non-promotable fleet idles into its timeouts;
    /// - graceful-abort full drain → `RunComplete` WITH the replicated
    ///   `graceful_abort_requested` latch already set
    ///   (`finalize_terminal_accounting`'s graceful branch): the COMPOSED
    ///   pair of sticky facts is the graceful-abort verdict every node
    ///   derives — deliberately NO third terminal mutation, and never
    ///   `RunAborted` (nothing failed; the frozen pool residue is not a
    ///   strand).
    ///
    /// NO VERDICT (failover's jurisdiction, or unreachable):
    /// - panik (`PanikShutdown`): operator killed THIS node, not the run —
    ///   the self-authored `PeerRemoved { SelfDeparture }` is the
    ///   broadcast; peers continue / re-elect;
    /// - every generic `Other` from the bootstrap chain / drain dispatch
    ///   errors: transport-shaped failures a promoted successor may well
    ///   survive — a verdict here would tear down a salvageable run;
    /// - crashes / panics / SIGKILL: no code runs; failover handles it.
    pub(crate) async fn broadcast_terminal_verdict(&mut self, mutation: ClusterMutation<I>) {
        self.apply_and_broadcast_cluster_mutations(vec![mutation.clone()])
            .await;
        // Brief settle window so the broadcast lands on every peer before
        // the dispatcher tears down its transport. Without this, fast
        // dispatcher exits race the broadcast and some peers miss the
        // signal — the symptom is leftover SLURM jobs in CG state for the
        // wrappers whose secondaries didn't see the verdict. See
        // `PRIMARY_BROADCAST_SETTLE` for the rationale.
        tokio::time::sleep(crate::primary::PRIMARY_BROADCAST_SETTLE).await;
        // Then HOLD (re-broadcasting) until every observer the roster names
        // is reachable again, bounded by the grace cap (#415 face (b1)).
        // The fixed settle above delivers to a healthy mesh; this covers the
        // observer leg that was DOWN at broadcast time and is re-folding.
        self.await_terminal_observer_delivery(mutation).await;
    }

    /// Hold the authority alive — re-broadcasting `mutation` — until every
    /// observer the roster names has a reachable transport leg, bounded by
    /// [`crate::primary::TERMINAL_OBSERVER_DELIVERY_GRACE`].
    ///
    /// # Single concern (#415 face (b1))
    ///
    /// The terminal verdict must reach the OBSERVER before the fleet tears
    /// down. A zero-authority observer exits ONLY on observing the CRDT
    /// terminal (it never strands on visibility loss — BUG-B), so a verdict
    /// that misses its leg is unrecoverable once the compute peers exit and
    /// no peer is left to anti-entropy-pull from. The compute peers self-heal
    /// a missed terminal (fail over / time out, release their slot), so this
    /// gate is specifically about the observer leg — read off the
    /// `RoleTable.observers` projection the primary already holds, against the
    /// pump-published reachability (`MeshClient::has_route`).
    ///
    /// No-op when the roster names no observer (the common single-process /
    /// no-observer topology returns after the fixed settle alone). The
    /// re-broadcast is idempotent (`apply_and_broadcast_cluster_mutations`
    /// filters the CRDT NoOp), so a leg that recovers mid-wait re-receives
    /// the verdict; a leg that never recovers (the observer host genuinely
    /// died) gives up at the cap and tears down exactly as the pre-#415
    /// fixed settle did — the bound is what keeps a finished run from
    /// stalling forever on a gone observer.
    async fn await_terminal_observer_delivery(&mut self, mutation: ClusterMutation<I>) {
        let observers: Vec<PeerId> = self
            .cluster_state
            .role_table()
            .observers
            .iter()
            .map(|id| PeerId::from(id.as_str()))
            .collect();
        if observers.is_empty() {
            return;
        }
        let all_reachable = |client: &crate::process::MeshClient<I>| {
            observers.iter().all(|id| client.has_route(id))
        };
        if all_reachable(&self.client) {
            return;
        }
        let deadline =
            tokio::time::Instant::now() + crate::primary::TERMINAL_OBSERVER_DELIVERY_GRACE;
        tracing::info!(
            observers = observers.len(),
            grace_secs = crate::primary::TERMINAL_OBSERVER_DELIVERY_GRACE.as_secs(),
            "terminal verdict broadcast but an observer leg is not yet reachable; \
             holding + re-broadcasting until it re-folds (or the grace cap)"
        );
        loop {
            // Re-broadcast each poll so a leg that re-folds mid-wait
            // re-receives the verdict (the pump only fans NEW sends to a
            // freshly-registered connection — there is no retransmit-on-
            // reconnect for the original broadcast). `broadcast_applied_
            // mutations` (NOT `apply_and_broadcast_cluster_mutations`)
            // because the verdict already latched on the FIRST broadcast's
            // local apply: the apply-and-broadcast path NoOp-filters an
            // already-applied mutation off the wire (#50's N²-amplification
            // guard), which would silently swallow every re-send. The
            // raw re-broadcast re-emits the frame; the receiving observer
            // applies it idempotently (it observes the terminal → exits).
            self.broadcast_applied_mutations(vec![mutation.clone()])
                .await;
            tokio::time::sleep(crate::primary::PRIMARY_BROADCAST_SETTLE).await;
            if all_reachable(&self.client) {
                tracing::info!("observer leg re-folded; terminal verdict delivered");
                return;
            }
            if tokio::time::Instant::now() >= deadline {
                tracing::warn!(
                    grace_secs = crate::primary::TERMINAL_OBSERVER_DELIVERY_GRACE.as_secs(),
                    "observer leg still unreachable after the terminal-delivery grace; \
                     tearing down anyway (the observer host may be gone — its fleet-death \
                     presumption is the backstop)"
                );
                return;
            }
        }
    }

    /// Originate the CRDT `Pending → InFlight` transition for a task
    /// that was just committed locally (`commit_assignment`) AND
    /// successfully sent to its secondary.
    ///
    /// THE single origination point for `ClusterMutation::TaskAssigned`
    /// on the live path: every dispatch site (initial assignment, the
    /// `TaskRequest` reply, the per-tick dispatch fan-out) calls this
    /// helper AFTER its send succeeds, so the in-flight assignment is
    /// replicated into every replica's CRDT mirror and the per-task
    /// `in_flight` ledger becomes a derived cache of `TaskState::InFlight`.
    /// Routed through the canonical
    /// `apply_and_broadcast_cluster_mutations` path so it inherits the
    /// same local-apply + wire-fan-out + apply-filter semantics as every
    /// other primary-originated mutation.
    ///
    /// Ordering (audit R-1): originate AFTER the successful `send_to`.
    /// A send-failure path (`rollback_assignment`) runs BEFORE this
    /// helper is reached, so a failed send leaves NO CRDT `InFlight` to
    /// compensate — the rollback only has to undo the local
    /// commit triple, never a replicated transition. `commit_assignment`
    /// still writes the local ledger/slot BEFORE the send (so a
    /// completion racing back is attributed by hash); the CRDT
    /// origination is the post-send half.
    ///
    /// Repairs failover hydration: a promoted primary / observer now
    /// sees the task as `InFlight` and does NOT re-dispatch it (the
    /// hydrate in-flight arm, previously dead on the live path, is now
    /// fed live). The terminal `TaskCompleted` / `TaskFailed` transitions
    /// out of `InFlight` already exist; a dead-secondary recovery
    /// transitions back via `ClusterMutation::TaskRequeued`.
    pub(crate) async fn originate_task_assigned(
        &mut self,
        task_hash: String,
        secondary: String,
        worker: u32,
    ) {
        self.apply_and_broadcast_cluster_mutations(vec![ClusterMutation::TaskAssigned {
            hash: task_hash,
            secondary,
            worker,
            // Both stamped at the origination choke point
            // (apply_locally_for_broadcast): `version` minted, `attempt`
            // read from the task's current generation (C-1).
            version: Default::default(),
            attempt: Default::default(),
        }])
        .await;
    }

    /// Register the primary's own host as a first-class cluster member.
    ///
    /// Single concern: the primary is a peer, so its host-id must land
    /// in every replica's `peer_state` / `RoleTable` / relay membership
    /// exactly as every secondary's does. This mirrors the secondary
    /// accept path (`primary::connect::handle_welcome`), which originates
    /// `PeerJoined { peer_id: <secondary_id> }` the moment a secondary is
    /// recorded as connected — here the originator records ITSELF.
    /// `is_observer: false`: the primary is never an observer (the
    /// observer projection ratchets up only from `is_observer: true`
    /// joins, so this entry never touches `RoleTable.observers`).
    ///
    /// This is MEMBERSHIP only — it does NOT originate `PrimaryChanged`
    /// and does NOT warm the primary ROLE cache (uniform primary
    /// announcement is a separate concern). It also does NOT add the
    /// primary to the `PeerInfo` dial-list (`send_peer_lists`): that list
    /// is consumed as a dial target by secondaries' `connect_to_peers`,
    /// and the submitter is reachable only over the already-registered
    /// reverse-tunnel mesh link, never by a fresh direct dial to its raw
    /// address. Membership rides the CRDT `PeerJoined` path, which is the
    /// single writer to peer membership post-observer-refactor (the
    /// runtime `PeerInfo` arm is a receiver NoOp).
    ///
    /// Idempotent: `apply_peer_joined` short-circuits NoOp on re-applies
    /// for an already-Alive id whose observer projection is unchanged, so
    /// running this in both the seed-and-assign and the setup-defer
    /// bootstrap paths is safe.
    pub(crate) async fn originate_primary_membership(&mut self) {
        self.apply_and_broadcast_cluster_mutations(vec![ClusterMutation::PeerJoined {
            peer_id: self.config.node_id.clone(),
            is_observer: false,
            // Foundation leaf: capability stays the conservative `false`.
            // A node setting its own primary-capability marker from its
            // lifecycle is Leaf 3's concern.
            can_be_primary: false,
            // Stamped at the origination choke point.
            cap_version: Default::default(),
            // This node's current membership incarnation (0 cold).
            member_gen: self.cluster_state.peer_member_gen(&self.config.node_id),
        }])
        .await;
    }

    /// Re-emit the FULL per-secondary roster after the peer mesh has
    /// converged — the post-mesh anti-entropy backstop for the membership
    /// records originated pre-mesh at `handle_welcome`.
    ///
    /// Single concern: close the `secondary_capacities` desync. Each
    /// `SecondaryCapacity` (and the non-observer `PeerJoined`) is
    /// originated per-secondary at `handle_welcome` — PRE-mesh, before
    /// the later-welcoming secondaries' peer links exist — and never
    /// re-emitted. A secondary that welcomed before a sibling therefore
    /// holds an incomplete capacity roster; only the live primary's
    /// `cluster_state` is complete. The blast radius: an observer's
    /// occupancy undercounts, and — worse — a failover-promoted secondary
    /// rebuilds an INCOMPLETE worker roster off its own
    /// `known_secondaries()` (`reconstruct_workers_from_cluster_state`),
    /// undercounting `alive_worker_secondary_count` into a premature
    /// fleet-dead exit.
    ///
    /// The fix is one post-mesh re-broadcast of the records the primary
    /// already holds (it IS the complete source). Called once, right
    /// after `originate_primary_membership` — POST `wait_for_peer_connections`
    /// (so the batch reaches the fully-formed mesh) and BEFORE the
    /// seed/setup-defer branch (so both modes pass through). Iterates
    /// `self.secondaries`, reading each connection's welcome-advertised
    /// capabilities straight off the `SecondaryConnectionState` typestate
    /// (the single record of `num_workers` / `resources` / `is_observer`
    /// / `can_be_primary`), and emits ONE batch carrying a
    /// `SecondaryCapacity` + a `PeerJoined` per secondary.
    ///
    /// Re-emitting the non-observer `PeerJoined` too is deliberate: it
    /// shares the same pre-mesh-no-re-emit shape as `SecondaryCapacity`
    /// (`send_peer_lists` only re-emits the OBSERVER `PeerJoined` batch),
    /// so its membership record has the identical desync blast boundary
    /// and heals in the same pass.
    ///
    /// This is a pure RE-EMISSION of records the primary ALREADY holds
    /// locally — NOT a fresh origination. It therefore does NOT route
    /// through `apply_and_broadcast_cluster_mutations`: that path
    /// re-applies each mutation against the primary's OWN `cluster_state`
    /// first and filters out everything that NoOps. Since every record
    /// here is already present in the primary's complete mirror (it
    /// originated them at `handle_welcome`), the apply-and-filter would
    /// classify the ENTIRE batch as NoOp and drop it off the wire — a
    /// silent no-op, defeating the re-broadcast. Instead it ships the
    /// batch straight over the `Destination::All` mesh edge (the same
    /// egress `apply_and_broadcast_cluster_mutations` uses for its final
    /// send). The idempotency that makes this safe lives at the
    /// RECEIVER: a secondary that already holds a record NoOps it on
    /// apply and never re-broadcasts (`handle_cluster_mutation` /
    /// `apply_cluster_mutations` are apply-only); a secondary missing the
    /// record applies it and converges. Zero new merge logic — the
    /// existing lattice does all the reconciliation.
    pub(crate) async fn rebroadcast_full_roster(&mut self) {
        // Collect the AUTHORITATIVE departure view (the `capabilities`
        // 2P-set's Departed tombstones — NOT `self.secondaries`, which has
        // already dropped them) before the `self.secondaries` borrow below.
        // A reconnecting node that missed a `PeerRemoved` learns the
        // departure from this re-emit (the LIVENESS catch-up); capability
        // correctness already rides the snapshot-healable 2P-set + digest.
        let departed_ids: Vec<String> = self
            .cluster_state
            .departed_capability_ids()
            .map(|id| id.to_string())
            .collect();
        // Build the full roster batch under the immutable borrow of
        // `self.secondaries`. Two mutations per secondary: the membership
        // `PeerJoined` and the static `SecondaryCapacity`. A `PeerRemoved`
        // per Departed-tombstoned id is appended after.
        let mut mutations: Vec<ClusterMutation<I>> =
            Vec::with_capacity(self.secondaries.len() * 2 + departed_ids.len());
        for conn in self.secondaries.values() {
            mutations.push(ClusterMutation::PeerJoined {
                peer_id: conn.id().to_string(),
                is_observer: conn.is_observer(),
                can_be_primary: conn.can_be_primary(),
                // Pure RE-EMISSION (does NOT route through the choke
                // point), so the conservative `(0, 0)` minimum: a
                // converged capability holds a strictly-higher stamped
                // version, so `merge_capability` keeps it and the receiver
                // NoOps (no amplification). A node missing the capability
                // entirely adopts this baseline and converges the rest via
                // the digest + snapshot pull.
                cap_version: Default::default(),
                // Re-emit at the id's CURRENT incarnation so a receiver
                // that already converged NoOps and one that missed a
                // re-admission catches up.
                member_gen: self.cluster_state.peer_member_gen(conn.id()),
            });
            mutations.push(ClusterMutation::SecondaryCapacity {
                secondary: conn.id().to_string(),
                worker_count: conn.num_workers(),
                resources: conn.resources().to_vec(),
            });
        }
        // Re-emit a `PeerRemoved` per Departed-tombstoned id (LIVENESS
        // catch-up). The receiver's `apply_peer_removed` is sticky/
        // idempotent — a node that already buried the id NoOps it.
        for id in departed_ids {
            let member_gen = self.cluster_state.peer_member_gen(&id);
            mutations.push(ClusterMutation::PeerRemoved {
                id,
                cause: RemovalCause::RosterReemit,
                // Re-emit at the generation the local tombstone holds: a
                // receiver that re-admitted the id at a HIGHER generation
                // correctly drops this stale catch-up.
                member_gen,
            });
        }
        if mutations.is_empty() {
            return;
        }
        let count = self.secondaries.len();
        let msg = DistributedMessage::ClusterMutation {
            target: None,
            sender_id: self.config.node_id.clone(),
            timestamp: timestamp_now(),
            mutations,
        };
        if let Err(error) = self.send_to(Destination::All, msg).await {
            tracing::warn!(
                error = %error,
                "full-roster re-broadcast delivery failed"
            );
        }
        tracing::info!(
            secondaries = count,
            "re-broadcast full secondary roster (capacity + membership) post-mesh"
        );
    }

    /// Originate the uniform primary announcement: `PrimaryChanged { new
    /// = self }` at the bootstrap/failover convergence point
    /// (`activate_local_primary`).
    ///
    /// Single concern: assert THIS host as the primary in every replica's
    /// `RoleTable` / role cache, so `current_primary()` resolves to it
    /// uniformly cluster-wide through the one mesh — the SAME mechanism
    /// every primary uses, replacing the old "sole authority" special
    /// case. Sibling to `originate_primary_membership` (which records
    /// MEMBERSHIP); this records the ROLE. The epoch is a SINGLE transition:
    /// it re-asserts at the epoch ALREADY held when the converged snapshot
    /// already names this host (the relocate-target / election-winner promoted
    /// paths), and bumps `primary_epoch() + 1` only on a genuine first
    /// assertion — so a relocate→promotion is one epoch step, not two (see the
    /// body). Routed through
    /// `apply_and_broadcast_cluster_mutations`, so it inherits the same
    /// local-apply and wire fan-out as every other primary-originated
    /// mutation. The local apply warms the transport `Role::Primary`
    /// write-through cache and fires the primary-changed important-event
    /// hook on a genuine holder transition.
    pub(crate) async fn originate_primary_changed(&mut self) {
        // SINGLE epoch transition across relocate→promotion / election→promotion.
        //
        // This convergence point is reached ONLY on the
        // `BootstrapRole::PromotedDestination` arm (a `SeedSource::PromotionSnapshot`
        // primary — the relocate target or the failover-election winner; the
        // submitter ALWAYS relocates via the `SetupPeer` arm and never reaches
        // here). On BOTH promoted paths the converged snapshot this node was
        // `seed_from_promotion_snapshot`-restored from ALREADY names this host the
        // primary at the epoch the upstream transition committed:
        //   - relocate:  the submitter broadcast `PrimaryChanged { chosen, E+1,
        //                 Transferred }`, which the chosen peer applied (epoch E+1,
        //                 current_primary = chosen) before capturing the snapshot;
        //   - election:  `fire_local_promotion` broadcast `PrimaryChanged { self,
        //                 E+1, Election }`, applied locally before the snapshot.
        // A blind `primary_epoch() + 1` here would then announce a SECOND
        // `PrimaryChanged` for the SAME holder one epoch higher (E+2), leaving a
        // submitter-observer that disconnected at the relocate pinned at E+1 while
        // the cluster ran at E+2 — a double-bump for an unchanged primary.
        //
        // So when `current_primary()` ALREADY names this host, RE-ASSERT at the
        // epoch already held (no +1): a single transition, convergent under the
        // epoch-LWW register-adopt rule. Re-emitting the SAME (id, epoch) is an
        // apply NoOp on every replica that already converged (no spurious wire
        // fan-out, no duplicate important-event), and on any replica still behind
        // it lands the authoritative identity exactly once. The genuine first
        // assertion (no prior `current_primary`, e.g. a future direct-bootstrap
        // path) still bumps `primary_epoch() + 1` to supersede the prior identity.
        let already_self = self.cluster_state.current_primary() == Some(self.config.node_id.as_str());
        let epoch = if already_self {
            self.cluster_state.primary_epoch()
        } else {
            self.cluster_state.primary_epoch() + 1
        };
        let repoint = ClusterMutation::PrimaryChanged {
            new: self.config.node_id.clone(),
            epoch,
            // Self-announce (`new == self`): this host names ITSELF the
            // primary at the bootstrap/failover convergence point.
            reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
        };
        self.apply_and_broadcast_cluster_mutations(vec![repoint.clone()])
            .await;
        // Directed observer fan of the re-point (see `fan_repoint_to_
        // observers`). Unconditional — even on the already-self re-assert,
        // which the apply-and-broadcast path NoOp-filters off the wire
        // entirely: a relay-only observer never received the upstream
        // one-shot broadcast, so this directed re-emit is its ONLY path
        // to the authoritative identity from the authority itself.
        self.fan_repoint_to_observers(repoint).await;
    }

    /// Directed re-emit of one `PrimaryChanged` re-point to every
    /// observer-role member (`send_to_each_observer`), as a raw
    /// already-applied `ClusterMutation` frame — the same receiver-side
    /// idempotency contract as `rebroadcast_full_roster` (an observer
    /// that already converged NoOps the apply; one that was behind — or
    /// that missed the one-shot direct-leg broadcast, the relay-only
    /// observer face — adopts the identity and refreshes its
    /// primary-liveness clock). Shared by both re-point origination
    /// sites: `originate_primary_changed` (bootstrap + every promotion)
    /// and `relocate_primary_to` (the submitter handoff).
    pub(crate) async fn fan_repoint_to_observers(&mut self, repoint: ClusterMutation<I>) {
        let msg = DistributedMessage::ClusterMutation {
            target: None,
            sender_id: self.config.node_id.clone(),
            timestamp: timestamp_now(),
            mutations: vec![repoint],
        };
        // The re-point's per-observer failures are best-effort, already
        // narrated by the per-send debug line inside the fan; the throttled
        // egress WARN is the KEEPALIVE concern's, so drop them here.
        let _ = self.send_to_each_observer(msg).await;
    }

    /// React to a panik-watcher signal on the primary.
    ///
    /// Single concern: a node observing its OWN panik signal announces
    /// its departure from the mesh and exits locally. It broadcasts a
    /// self-authored `ClusterMutation::PeerRemoved { id: <self_id>,
    /// cause: SelfDeparture(reason) }` so peers LOG the departure and
    /// mark this peer Dead — observability only. The departure does
    /// NOT cancel cluster work or terminate the run on peers; phase /
    /// task management stays decoupled from membership.
    ///
    /// Returns the (matched_path, reason) pair for the caller to
    /// surface as `RunError::PanikShutdown` (the primary's local
    /// self-exit). Unlike a worker-bearing node, the primary owns no
    /// local worker pool — workers run on secondaries via the
    /// `RemoteWorkerState` ledger — so there is nothing to tear down
    /// here beyond the announcement; the primary's exit(137) is owned
    /// by the PyO3 wrapper once it sees `RunError::PanikShutdown`.
    ///
    /// Apply errors / broadcast failures are best-effort: logged at
    /// warn, never propagated. The panik-react path must always
    /// finish — operators rely on the SLURM container reaping via
    /// exit(137), and a degraded broadcast is no worse than the
    /// pre-panik baseline.
    pub(crate) async fn handle_panik_signal(
        &mut self,
        matched_path: std::path::PathBuf,
    ) -> (std::path::PathBuf, String) {
        let reason = format!("panik file: {}", matched_path.display());
        tracing::error!(
            node_id = %self.config.node_id,
            matched_path = %matched_path.display(),
            "panik signal observed on primary; announcing self-departure and exiting locally"
        );
        // Self-authored departure announcement. `BoundedString::from`
        // truncates at the 1 KiB cap `SelfDeparture` carries.
        let mutation = ClusterMutation::PeerRemoved {
            id: self.config.node_id.clone(),
            cause: RemovalCause::SelfDeparture(BoundedString::from(reason.clone())),
            // Kills THIS node's current membership incarnation.
            member_gen: self.cluster_state.peer_member_gen(&self.config.node_id),
        };
        self.apply_and_broadcast_cluster_mutations(vec![mutation])
            .await;
        (matched_path, reason)
    }

    pub(crate) async fn send_transfer_complete(&mut self) -> Result<(), String> {
        // Per-secondary directed delivery over `Destination::Secondary(id)` —
        // the SAME router/relay path `perform_initial_assignment` uses for the
        // `InitialAssignment` frame, and over the SAME CRDT-derived roster
        // (`known_secondaries()`). This is the setup-GATING frame for
        // `wait_for_setup` (`secondary::setup`): a secondary's
        // `Configuring → Operational` gate hard-blocks on
        // `got_peer_info && got_assignment && got_transfer`, and the only
        // in-setup escape is the kill-on-deadline. The pre-fix `Destination::All`
        // broadcast was a FIRE-ONCE snapshot over the currently-registered mesh
        // connections (`PeerNetwork::broadcast` / `ChannelPeerTransport::broadcast`):
        // no relay, no replay, no per-peer guarantee. A secondary whose peer-mesh
        // link registered AFTER the broadcast instant (observed live: a 156 ms
        // gap between the broadcast and the late peer's mesh registration)
        // PERMANENTLY missed it and wedged in `wait_for_setup` until its setup
        // deadline killed it — even though that SAME secondary had already
        // received its `InitialAssignment`, because that frame rode the directed
        // `send_to_peer` path which relays through any connected sibling to a
        // not-yet-directly-connected target. Routing TransferComplete the same
        // way makes its delivery CONVERGE with the assignment it gates: every
        // secondary that received an `InitialAssignment` receives the
        // TransferComplete that releases its setup gate, independent of WHEN its
        // mesh link registers (Directive 1: the setup-gating frame's delivery is
        // independent of role/membership/registration timing).
        //
        // Collect the roster to an owned `Vec` first so the `&self.cluster_state`
        // borrow is dropped before the `&mut self` `send_to` calls (mirrors
        // `perform_initial_assignment`'s name-sorted collect). Name-sorted for a
        // deterministic, log-diffable fan-out order.
        let mut secondary_ids: Vec<String> = self
            .cluster_state
            .known_secondaries()
            .map(String::from)
            .collect();
        secondary_ids.sort();

        for secondary_id in &secondary_ids {
            self.send_transfer_complete_to(secondary_id).await;
        }
        tracing::info!(
            secondaries = secondary_ids.len(),
            "transfer complete sent to all secondaries"
        );
        Ok(())
    }

    /// The SOLE per-member `TransferComplete` construction + send site,
    /// shared by the run-start batch fan-out
    /// ([`Self::send_transfer_complete`]) and the mid-run incremental serve
    /// (`peer_setup::serve_setup_on_cert_exchange`'s post-run-start
    /// variant).
    ///
    /// A `Destination::Secondary(id)` send is QUEUED to the mesh-pump
    /// (`MeshClient::send`); its only error mode is the egress receiver —
    /// the local mesh-pump — being dropped, i.e. THIS node winding down
    /// (a cluster collapse). The per-peer routing outcome
    /// (direct / relayed / no-route) is resolved LATER inside the pump
    /// and never surfaces here, so a transient "no route to this one
    /// secondary" is NOT a send error — it is logged at the pump and the
    /// peer is recovered via the snapshot/anti-entropy backstop, exactly
    /// as for the directed `InitialAssignment` send. So the `Err` arm is
    /// uniformly the mesh-pump-gone collapse, which `send_to` latches on
    /// `self.mesh_pump_gone` for `run_pipeline`'s post-transfer gate to
    /// route into the strand-classification finalize tail. Warn-and-
    /// continue (uniform with the sibling broadcasts) instead of
    /// `?`-escaping as a raw `RunError::Other`: a node winding down in the
    /// window between a successful initial assignment and transfer-complete
    /// must surface as a clean `ClusterCollapsed` + `RunAborted`, not an
    /// unclassified `Other`. The batch caller continues its fan-out so the
    /// remaining secondaries still receive their gate-release on the same
    /// dead-pump pass — the latch is the single collapse signal the caller
    /// consults.
    pub(crate) async fn send_transfer_complete_to(&mut self, secondary_id: &str) {
        let msg = DistributedMessage::TransferComplete {
            target: None,
            sender_id: self.config.node_id.clone(),
            timestamp: timestamp_now(),
            total_files: 0,
            total_bytes: 0,
        };
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
                "TransferComplete delivery failed"
            );
        }
    }
}
