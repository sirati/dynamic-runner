use dynrunner_core::{BoundedString, Identifier};
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, Destination, DistributedMessage, RemovalCause,
};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use crate::cluster_state::apply_locally_for_broadcast;
use crate::primary::PrimaryCoordinator;
use crate::primary::wire::{compute_task_hash, timestamp_now};
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
        // by `ingest_setup_discovery` + panik self-departure) share one
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
        if applied.is_empty() {
            return;
        }
        let msg = DistributedMessage::ClusterMutation {
            target: None,
            sender_id: self.config.node_id.clone(),
            timestamp: timestamp_now(),
            mutations: applied,
        };
        // Route through the `Destination::All` egress edge, matching the
        // primary keepalive path (`broadcast_primary_keepalive`) — one
        // mesh broadcast to every member. The single mesh transport
        // collapses per-secondary delivery failures into one `String`
        // (the per-secondary signal is the heartbeat monitor, not this
        // log line). The CRDT is idempotent, so a missed mutation is
        // recoverable from the next snapshot RPC; we never block dispatch
        // on universal delivery.
        if let Err(error) = self.send_to(Destination::All, msg).await {
            tracing::warn!(
                error = %error,
                "ClusterMutation broadcast delivery failed"
            );
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
            // Stamped at the origination choke point (apply_locally_for_broadcast).
            version: Default::default(),
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
    /// undercounting `alive_remote_secondary_count` into a premature
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
            mutations.push(ClusterMutation::PeerRemoved {
                id,
                cause: RemovalCause::RosterReemit,
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
    /// MEMBERSHIP); this records the ROLE. The epoch is
    /// `primary_epoch() + 1`, mirroring the election winner's
    /// `fire_local_promotion`, so a failover re-announce strictly
    /// supersedes the prior identity via epoch-LWW. Routed through
    /// `apply_and_broadcast_cluster_mutations`, so it inherits the same
    /// local-apply and wire fan-out as every other primary-originated
    /// mutation. The local apply warms the transport `Role::Primary`
    /// write-through cache and fires the primary-changed important-event
    /// hook on a genuine holder transition.
    pub(crate) async fn originate_primary_changed(&mut self) {
        let epoch = self.cluster_state.primary_epoch() + 1;
        self.apply_and_broadcast_cluster_mutations(vec![ClusterMutation::PrimaryChanged {
            new: self.config.node_id.clone(),
            epoch,
            // Self-announce (`new == self`): this host names ITSELF the
            // primary at the bootstrap/failover convergence point.
            reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
        }])
        .await;
    }

    /// Phase-S/B: seed the replicated cluster ledger with the run's
    /// task graph and phase-dependency graph. Emits one
    /// `PhaseDepsSet` (carrying the canonical per-run dep graph)
    /// followed by one `TaskAdded` per binary in `all_binaries`; the
    /// originator-side `apply_and_broadcast_cluster_mutations` applies
    /// locally and ships the batch to every secondary.
    ///
    /// `PhaseDepsSet` rides ahead of `TaskAdded` so receivers'
    /// `cluster_state.phase_deps()` is populated before any
    /// post-promotion hydration that consults it. The mutation is
    /// idempotent (re-application is a no-op when local is non-empty),
    /// so multiple snapshot sources or duplicate broadcasts are safe.
    ///
    /// Called once at run start, after every secondary has connected
    /// (so `transport.broadcast` reaches the full fleet) and before
    /// `perform_initial_assignment` runs (so the originator's mirror
    /// is non-empty when the first dispatch happens).
    pub(crate) async fn seed_cluster_state(&mut self) {
        let mut mutations: Vec<ClusterMutation<I>> =
            Vec::with_capacity(self.all_binaries.len() + 1);
        mutations.push(ClusterMutation::PhaseDepsSet {
            deps: self.phase_deps.clone(),
        });
        mutations.extend(
            self.all_binaries
                .iter()
                .map(|b| ClusterMutation::TaskAdded {
                    hash: compute_task_hash(b),
                    task: b.clone(),
                }),
        );
        let task_count = self.all_binaries.len();
        self.apply_and_broadcast_cluster_mutations(mutations).await;
        // Validate `preferred_secondaries` lists against the known
        // secondary set NOW that both inputs are settled: the seed
        // batch finished applying (so every task's
        // `preferred_secondaries` is in `all_binaries`) and the
        // pre-loop handshake has populated `self.secondaries` with
        // every secondary the primary has connected to. The
        // validator emits one structured warn per unknown id; a
        // later `PeerLifecycleEvent::Added` may make a previously-
        // unknown id known and the re-validation in
        // `handle_cluster_mutation` will silence it.
        let known: std::collections::HashSet<&str> =
            self.secondaries.keys().map(|s| s.as_str()).collect();
        self.preferred_secondaries_validator
            .validate(self.all_binaries.iter(), &known);
        tracing::info!(tasks = task_count, "seeded cluster ledger");
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
        };
        self.apply_and_broadcast_cluster_mutations(vec![mutation])
            .await;
        (matched_path, reason)
    }

    pub(crate) async fn send_transfer_complete(&mut self) -> Result<(), String> {
        let msg = DistributedMessage::TransferComplete {
            target: None,
            sender_id: self.config.node_id.clone(),
            timestamp: timestamp_now(),
            total_files: 0,
            total_bytes: 0,
        };
        // Uniform `Destination::All` mesh broadcast, same as the primary
        // keepalive + CRDT-mutation fan-out.
        if let Err(error) = self.send_to(Destination::All, msg).await {
            tracing::warn!(
                error = %error,
                "TransferComplete delivery failed"
            );
            return Err(format!("TransferComplete broadcast failed: {error}"));
        }
        tracing::info!("transfer complete sent to all secondaries");
        Ok(())
    }
}
