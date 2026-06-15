use dynrunner_core::{BoundedString, Identifier};
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, Destination, DistributedMessage, PeerId, RemovalCause,
};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

/// Which terminal verdict the primary is broadcasting — the caller's intent,
/// WITHOUT the count payload. [`PrimaryCoordinator::broadcast_terminal_verdict`]
/// is the SINGLE owner of stamping the authoritative finalized counts onto
/// the verdict (so no call site has to know about — or could diverge on —
/// the count payload). `Complete` is the clean / graceful terminal;
/// `Aborted` carries the abort reason for the PyO3-boundary log.
pub(crate) enum TerminalVerdict {
    Complete,
    Aborted(String),
}

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
            mut applied,
            resumed_for_dispatch,
            became_pending,
        } = batch;
        let resumed_any = !resumed_for_dispatch.is_empty();
        // A SecondaryAffine gate that auto-resumed Blocked → Pending in this
        // batch must NOT enter the pool: a gate is `is_worker_assignable=false`
        // and is never worker-dispatched, and the AffineReady resolution
        // below transitions it to its terminal `AffineReady` state inside
        // this same call. Reinjecting it would leave an inert non-
        // dispatchable item in the pool with no path to remove it (the
        // `resolve_dependency_satisfied_affine_gates` pass only detects
        // Pending+resolved gates, not the AffineReady gate this resolution
        // produces). Skip gates here; the AffineReady recursion below owns
        // the gate's terminal transition AND the cascade resume of its
        // dependents (those dependents are the real worker-assignable
        // tasks that DO need pool re-injection — they ride the recursive
        // call's own `resumed_for_dispatch`).
        for binary in resumed_for_dispatch {
            if binary.kind.is_secondary_affine() {
                continue;
            }
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
        //
        // BROADCAST-ORDER INVARIANT (#591): the originating frame in
        // `applied` (TasksSpawned / TaskCompleted / SetupCompleted) must
        // reach every secondary BEFORE the AffineReady frame this resolution
        // produces. Otherwise the secondary's apply of the originating frame
        // hasn't yet inserted/resumed the gate to `Pending`, so when
        // AffineReady arrives the gate is still `Blocked` (or absent), the
        // `Pending`-precondition arm misses, and the `_ => None` NoOp arm
        // wins — leaving the gate forever non-`AffineReady` on the secondary.
        // `unmet_local_affine_dep` then returns `None` for every dependent
        // (it only fires on `AffineReady` gates), so the secondary's affine
        // executor never dispatches the gate body, and the dependents either
        // run without their import (with broken outputs) or fail-and-retry
        // forever (the consumer's 272-reinject + 0-body-execution deadlock).
        //
        // The pre-#591 inline recursion broadcast AffineReady FIRST (during
        // the recursive `broadcast_applied_mutations`) and the outer frame
        // SECOND, exactly the wrong order. The cold-seed path
        // (`originate_cold_seed`) is unaffected because it STAGES every
        // resolved-gate AffineReady into one post-connect broadcast vec
        // alongside the seed's TaskAdded — same in-order semantics, single
        // frame.
        //
        // The fix here: DRAIN the AffineReady chain ITERATIVELY against
        // `apply_locally_for_broadcast` (apply locally + collect cascade
        // surfaces, NO broadcast), append every applied AffineReady mutation
        // to the OUTER `applied` vec in topological order (parent before
        // child), and let the OUTER's single `broadcast_applied_mutations`
        // ship the originating frame AND the whole chain in ONE wire frame
        // in the right order. Chain convergence: each iteration resumes the
        // next layer's gates Blocked → Pending and their hashes ride the
        // next iteration's `became_pending`. Terminates: a gate goes
        // `Pending → AffineReady` exactly once and the apply arm NoOps a
        // re-application.
        let mut pending_to_resolve = became_pending;
        while !pending_to_resolve.is_empty() {
            let affine_ready = self
                .cluster_state
                .affine_ready_mutations_for(pending_to_resolve);
            if affine_ready.is_empty() {
                break;
            }
            let chain_batch =
                apply_locally_for_broadcast(&mut self.cluster_state, affine_ready);
            // The chain step's cascade-resumed dependents (e.g. the
            // `build` tasks unblocked by the gate's `AffineReady`): real
            // worker-assignable work that must enter the pool. Skip
            // gates here too — a chained downstream gate's terminal will be
            // applied on this same loop's next iteration. Reinject the
            // real work tasks straight away so the post-loop
            // `TasksAdded` emit reaches a pool that already holds them.
            let chain_resumed_any = !chain_batch.resumed_for_dispatch.is_empty();
            for binary in chain_batch.resumed_for_dispatch {
                if binary.kind.is_secondary_affine() {
                    continue;
                }
                tracing::debug!(
                    phase = %binary.phase_id,
                    task_id = ?binary.task_id,
                    "pool: re-inject auto-resumed Blocked dependent (affine chain)"
                );
                self.pool_mut().reinject(binary);
            }
            if chain_resumed_any {
                self.cluster_state
                    .emit_worker_mgmt(WorkerMgmtSignal::TasksAdded);
            }
            applied.extend(chain_batch.applied);
            // Re-arm the next iteration over any chained gates that THIS
            // chain step just resumed Blocked → Pending. A non-gate
            // `became_pending` hash filters to no AffineReady mutation
            // (see `affine_ready_mutations_for`), so the chain naturally
            // terminates the iteration in O(distinct-gate-depth).
            pending_to_resolve = chain_batch.became_pending;
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

    /// Drive the AffineReady originator over the SEED-convergence surface:
    /// scan the seeded ledger for `Pending` SecondaryAffine gates that are
    /// already dependency-resolved and originate one
    /// [`ClusterMutation::AffineReady`] per gate.
    ///
    /// The SEED twin of the delta driver in
    /// [`Self::apply_and_broadcast_cluster_mutations`] (which fires on the
    /// `became_pending` resume + `TasksSpawned` spawn surfaces). A gate
    /// seeded directly into the ledger via `TaskAdded` — the cold-seed,
    /// discover-on-promotion, and promotion-snapshot originators all seed
    /// tasks that way — never rides an apply-pass delta surface, so a no-dep
    /// (or all-deps-already-terminal) gate would otherwise stay `Pending`
    /// forever and wedge its dependents Blocked (the #502 deadlock root:
    /// the build-phase initial-assignment then finds zero worker-assignable
    /// tasks). Both drivers consult the SAME detection rule
    /// (`is_pending_resolved_affine_gate`, owned by `cluster_state`) — one
    /// detection owner, two structurally-distinct firing surfaces (live
    /// delta vs seed convergence), exactly as the delta driver already
    /// invokes it from BOTH the resume and the spawn surfaces.
    ///
    /// Routed through the canonical
    /// [`Self::apply_and_broadcast_cluster_mutations`] path, so it inherits
    /// the identical apply+filter+broadcast semantics AND the recursive
    /// chain convergence: applying a gate's `AffineReady` auto-resumes a
    /// dependent SecondaryAffine gate `Blocked → Pending`, which rides the
    /// recursive call's own `became_pending` and originates in turn.
    ///
    /// ORDERING (load-bearing): the seed originator must drive this BEFORE
    /// `hydrate_from_cluster_state` builds the pool. The live runtime-spawn
    /// path keeps a resolved gate OUT of the pool by firing the originator
    /// DURING the spawn apply (so the gate is already `AffineReady` — never
    /// `Pending` — when `apply_spawn_tasks`'s post-apply pool walk runs). The
    /// seed path must mirror that: a `Pending` gate hydrated into the pool is
    /// never worker-dispatched (`is_worker_assignable()` is false for a gate,
    /// like a `Setup` task) yet still counts as QUEUED, wedging its phase
    /// from draining. Resolving the gate to `AffineReady` before hydrate lets
    /// hydrate's `AffineReady` arm seed it terminal (its `task_id` resolves
    /// dependents in `extend()`, the gate is NOT pushed into the pool) — so
    /// the dependents enter the pool dispatchable and no inert gate item is
    /// ever queued. The `discover_on_promotion` originator drives this AFTER
    /// its `TaskAdded` seed broadcast and BEFORE its own hydrate; the
    /// cold-seed originator stages the AffineReady frames alongside its seed
    /// (applied locally before the pre-connection hydrate, broadcast
    /// post-connect); the promotion-snapshot path inherits the already-
    /// resolved gates via the snapshot.
    ///
    /// Idempotent: an already-`AffineReady` gate is not `Pending`, so a
    /// re-run emits nothing.
    pub(crate) async fn originate_affine_ready_for_seeded_gates(&mut self) {
        let mutations = self.cluster_state.affine_ready_mutations_for_ledger();
        if !mutations.is_empty() {
            self.apply_and_broadcast_cluster_mutations(mutations).await;
        }
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
    pub(crate) async fn broadcast_terminal_verdict(&mut self, verdict: TerminalVerdict) {
        // STAMP the authoritative counts onto the verdict HERE — the single
        // owner, so no call site knows about (or could diverge on) the count
        // payload. The read is `&self.outcome_summary()` at the instant of
        // broadcast; for the finalize path this equals the count the verdict
        // DECISION was computed from (coordinator.rs `finalize_terminal_
        // accounting`: the `outcome` read and this broadcast are the SAME
        // `&self`, the SAME synchronous oploop turn, with NO `.await` and NO
        // cluster_state mutation between them — verified). For the
        // pre-dispatch abort sites (bring-up / pre-phase duplicate / no-
        // relocation-target) the live read is the honest zero, exactly what
        // those verdicts should carry. So `verdict-counts == verdict-
        // decision-counts` holds at every site.
        let counts = self.outcome_summary().into();
        let mutation = match verdict {
            TerminalVerdict::Complete => ClusterMutation::RunComplete { counts },
            TerminalVerdict::Aborted(reason) => ClusterMutation::RunAborted { reason, counts },
        };
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
    /// observer the roster names has a reachable transport leg AND any node
    /// still RELOCATING into an observer has announced itself, bounded by
    /// [`crate::primary::TERMINAL_OBSERVER_DELIVERY_GRACE`] (the known-leg
    /// cap) and [`crate::primary::PENDING_OBSERVER_ANNOUNCE_GRACE`] (the
    /// relocating-in cap).
    ///
    /// # Single concern (#415 face (b1) + the relocation-in race)
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
    /// TWO ways an observer can miss the broadcast, both covered by the SAME
    /// re-broadcast loop:
    ///   1. a KNOWN observer (already in the role-table projection) whose
    ///      transport leg is transiently DOWN and re-folding — waited up to
    ///      the 60s known-leg cap; and
    ///   2. a node that GRACEFULLY RELOCATED its primary role onto this host
    ///      and is becoming a standalone observer ([`Self::pending_observer`],
    ///      from `PromotionSignal::relocating_from`) but has NOT YET announced
    ///      itself — in a fast relocation the verdict is decided before its
    ///      swap→announce completes, so its id is not yet in the role-table
    ///      projection and the known-leg check alone would skip the hold
    ///      entirely. Its leg SURVIVES the retag (the slot's stable channel),
    ///      so once it announces the known-leg machinery covers actual
    ///      delivery; this just keeps re-broadcasting (the observer's inbox
    ///      buffers the frame) until it joins the projection, bounded by the
    ///      shorter 5s announce cap (a node that died mid-swap is the only way
    ///      that cap is reached, and a dead node has nothing to deliver to).
    ///
    /// No-op when the roster names no observer AND no relocation is pending
    /// (the common single-process / no-observer topology returns after the
    /// fixed settle alone). The re-broadcast is idempotent
    /// (`broadcast_applied_mutations` re-emits the already-latched verdict),
    /// so a leg that recovers — or an observer that announces — mid-wait
    /// re-receives the verdict; a leg/announce that never lands gives up at
    /// its cap and tears down exactly as the pre-fix fixed settle did.
    async fn await_terminal_observer_delivery(&mut self, mutation: ClusterMutation<I>) {
        // The relocating-in node (becoming an observer) still being awaited:
        // dropped from the wait the instant it ANNOUNCES (joins the role-table
        // observer projection) OR its shorter announce cap expires. Cleared so
        // a second terminal broadcast (idempotent re-fire) never re-waits.
        let pending = self.pending_observer.take().map(|id| {
            (
                PeerId::from(id.as_str()),
                tokio::time::Instant::now() + crate::primary::PENDING_OBSERVER_ANNOUNCE_GRACE,
            )
        });

        // Delivery is complete when every KNOWN observer (the live role-table
        // projection, which the pending node JOINS on announce) is reachable
        // AND the pending node is no longer being awaited (announced, so it is
        // itself a known observer the reachability check now covers, or its
        // cap passed). Re-reads the projection each call so an announce that
        // landed between polls is observed.
        let delivery_complete = |client: &crate::process::MeshClient<I>,
                                  state: &crate::cluster_state::ClusterState<I>,
                                  pending: &Option<(PeerId, tokio::time::Instant)>|
         -> bool {
            let known_all_reachable = state
                .role_table()
                .observers
                .iter()
                .all(|id| client.has_route(&PeerId::from(id.as_str())));
            let pending_settled = match pending {
                None => true,
                Some((id, cap)) => {
                    state.role_table().observers.contains(id.as_str())
                        || tokio::time::Instant::now() >= *cap
                }
            };
            known_all_reachable && pending_settled
        };

        if pending.is_none() && self.cluster_state.role_table().observers.is_empty() {
            return;
        }
        if delivery_complete(&self.client, &self.cluster_state, &pending) {
            return;
        }
        let deadline =
            tokio::time::Instant::now() + crate::primary::TERMINAL_OBSERVER_DELIVERY_GRACE;
        tracing::info!(
            known_observers = self.cluster_state.role_table().observers.len(),
            awaiting_relocating_in = pending.is_some(),
            grace_secs = crate::primary::TERMINAL_OBSERVER_DELIVERY_GRACE.as_secs(),
            "terminal verdict broadcast but an observer leg is not yet reachable / a \
             relocating-in observer has not yet announced; holding + re-broadcasting \
             until it converges (or the grace cap)"
        );
        loop {
            // Re-broadcast each poll so a leg that re-folds — or an observer
            // that announces — mid-wait re-receives the verdict (the pump only
            // fans NEW sends to a freshly-registered connection; there is no
            // retransmit-on-reconnect for the original broadcast). A
            // relocating-in observer's inbox (an unbounded channel that
            // survives its primary→observer retag) BUFFERS each re-send and
            // applies it idempotently the moment its run loop starts draining,
            // so it converges the latch and exits. `broadcast_applied_
            // mutations` (NOT `apply_and_broadcast_cluster_mutations`) because
            // the verdict already latched on the FIRST broadcast's local apply:
            // the apply-and-broadcast path NoOp-filters an already-applied
            // mutation off the wire (#50's N²-amplification guard), which would
            // silently swallow every re-send. The raw re-broadcast re-emits the
            // frame; the receiving observer applies it idempotently.
            self.broadcast_applied_mutations(vec![mutation.clone()])
                .await;
            tokio::time::sleep(crate::primary::PRIMARY_BROADCAST_SETTLE).await;
            if delivery_complete(&self.client, &self.cluster_state, &pending) {
                tracing::info!(
                    "observer leg re-folded / relocating-in observer announced; \
                     terminal verdict delivered"
                );
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

    /// Handle one persistent-dial-failure signal from the QUIC transport
    /// (#542 cause-B). The transport calls into this path only at the
    /// `DIAL_SUMMARY_THRESHOLD` boundary (3 consecutive failed dial
    /// sweeps without a connect — the same boundary that throttles the
    /// operator "peer unreachable" WARN), so the give-up is already
    /// gated.
    ///
    /// OBSERVER-ONLY: if `peer_id` is in `role_table.observers`,
    /// originate a `ClusterMutation::PeerRemoved { cause:
    /// PersistentDialFailure }` so the role_table prunes, the next
    /// PeerInfo broadcast drops the id, and `connect_to_peers` calls
    /// `forget_departed` — the dial loop stops, and the recurring 60s
    /// unreachable WARN that #542 was about ends. Secondaries are
    /// SKIPPED: they already have an authoritative heartbeat-miss
    /// dead-declaration path (`requeue_dead_secondary`); adding a
    /// second source from the dial-failure signal would race that path
    /// and risk a double-removal interleaving on a slow / partitioned
    /// secondary that's still reachable on the keepalive path.
    /// Unknown ids (not in any role set — the dial info was stale) are
    /// dropped at debug level.
    ///
    /// FALSE-POSITIVE RECOVERY: a recoverable but flaky observer
    /// removed here recovers via the existing
    /// `PeerJoined`/`CapabilityAdvertised` re-admission path on the
    /// next successful connect — `apply_peer_removed` merges a
    /// `Departed` tombstone at the current generation, and a
    /// higher-generation `Advertised` supersedes it. No new
    /// recovery machinery is needed.
    pub(crate) async fn handle_persistent_dial_failure(&mut self, peer_id: String) {
        let role_table = self.cluster_state.role_table();
        if !role_table.observers.contains(peer_id.as_str()) {
            tracing::debug!(
                peer = %peer_id,
                "persistent-dial-failure signal for a non-observer peer; \
                 secondaries are removed via the heartbeat-miss path and \
                 unknown ids are stale dial info — taking no action"
            );
            return;
        }
        // Stamp the CURRENT membership incarnation: a removal at the
        // peer's `member_gen` kills exactly this incarnation, leaving a
        // higher-generation re-admission free to win at every receiver.
        let member_gen = self.cluster_state.peer_member_gen(&peer_id);
        tracing::warn!(
            observer = %peer_id,
            member_gen = member_gen,
            "originating PeerRemoved for an observer the QUIC transport \
             has given up dialing (DIAL_SUMMARY_THRESHOLD reached); \
             pruning role_table.observers so the dial loop stops \
             (#542 cause-B)"
        );
        let mutation = ClusterMutation::PeerRemoved {
            id: peer_id,
            cause: RemovalCause::PersistentDialFailure,
            member_gen,
        };
        self.apply_and_broadcast_cluster_mutations(vec![mutation])
            .await;
    }
}
