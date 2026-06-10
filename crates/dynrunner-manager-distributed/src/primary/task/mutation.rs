use dynrunner_core::{Identifier, TaskInfo};
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, Destination, DistributedMessage, PeerId,
};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use crate::primary::PrimaryCoordinator;
use crate::primary::wire::timestamp_now;
use crate::worker_signal::WorkerMgmtSignal;

impl<S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<S, E, I> {
    /// Apply a replicated `DistributedMessage::ClusterMutation` batch.
    ///
    /// Single concern: keep the demoted primary's CRDT mirror — and the
    /// accounting sets the operational loop's exit-counter check
    /// reads — converged with the cluster's view, even when the live
    /// primary authority has handed off to a promoted secondary.
    ///
    /// Pre-fix the primary's `dispatch_message` had no arm for
    /// `MessageType::ClusterMutation`: any mutation broadcast addressed
    /// at the demoted primary fell through the catch-all. The
    /// operational loop's `completed + failed >= total` exit check
    /// reads `self.completed_tasks` / `self.failed_tasks`, which on a
    /// demoted primary are fed only by direct `TaskComplete` /
    /// `TaskFailed` forwards reaching the local primary's transport.
    /// Cross-secondary completions on the new primary's pool never
    /// arrived as direct forwards (the new primary doesn't loopback
    /// peer-observed completions to the demoted primary's transport),
    /// so the counter never reached the total and the run loop sat
    /// forever — the asm-dataset-nix R2 / T3 hang.
    ///
    /// Mirrors `secondary::dispatch::apply_cluster_mutations` in shape
    /// (idempotent fan-out over a `Vec<ClusterMutation>`); diverges in
    /// that the primary additionally maintains `completed_tasks` /
    /// `failed_tasks` because those are the sets the lifecycle
    /// exit-counter reads. The CRDT idempotency on `cluster_state`
    /// makes repeated apply safe; `HashSet::insert` is idempotent on
    /// the accounting side.
    /// Answer a late-joiner's / re-bootstrapping peer's
    /// `RequestClusterSnapshot` from the primary's authoritative ledger.
    ///
    /// Single concern: the snapshot-RPC responder on the primary side.
    /// Pre-fix only the secondary router answered this request
    /// (`secondary::dispatch::router`'s `RequestClusterSnapshot` arm);
    /// the primary's `dispatch_message` had no arm and the request fell
    /// through the catch-all. A requester that unicast to the primary
    /// (`Destination::Primary` — the primary-preferred bootstrap target,
    /// and the only responder guaranteed COMPLETE pre-mesh-convergence)
    /// got no reply and timed out. The primary's `cluster_state` is the
    /// authoritative copy of every replicated field, so its snapshot is
    /// the strongest possible bootstrap payload.
    ///
    /// Mirrors the secondary responder exactly: snapshot the local
    /// `cluster_state`, unicast `ClusterSnapshot` back to the requester
    /// (its return address rides `sender_id`), and originate the
    /// requester's `PeerJoined` over the canonical broadcast path so
    /// every replica learns about the joiner. The joiner's declared
    /// `is_observer` / `can_be_primary` ride the request frame and are
    /// recorded truthfully (the same role-carrying contract the secondary
    /// responder honours — `apply_peer_joined`'s observer ratchet keeps a
    /// re-bootstrapping worker from mis-upgrading to observer).
    ///
    /// The wire-side `snapshot_json` keeps the protocol envelope free of
    /// `ClusterStateSnapshot<I>` (the dependency direction: protocol must
    /// not depend on manager-distributed). A serialization failure is
    /// logged and the request is dropped — best-effort, exactly as the
    /// secondary responder treats its own send failure; the requester's
    /// bounded recv wait falls back to its own deadline.
    pub(crate) async fn handle_request_cluster_snapshot(&mut self, msg: DistributedMessage<I>) {
        let DistributedMessage::RequestClusterSnapshot {
            target: None,
            sender_id,
            is_observer,
            can_be_primary,
            ..
        } = msg
        else {
            return;
        };
        let snapshot = self.cluster_state.snapshot();
        match serde_json::to_string(&snapshot) {
            Ok(snapshot_json) => {
                let response = DistributedMessage::ClusterSnapshot {
                    target: None,
                    sender_id: self.config.node_id.clone(),
                    timestamp: timestamp_now(),
                    snapshot_json,
                };
                if let Err(e) = self
                    .send_to(
                        Destination::Secondary(PeerId::from(sender_id.clone())),
                        response,
                    )
                    .await
                {
                    tracing::warn!(
                        target = %sender_id,
                        error = %e,
                        "failed to deliver ClusterSnapshot response from primary"
                    );
                }
            }
            Err(e) => {
                tracing::warn!(
                    target = %sender_id,
                    error = %e,
                    "snapshot serialization failed; dropping RequestClusterSnapshot"
                );
            }
        }
        // Originate the requester's `PeerJoined` over the canonical
        // local-apply + broadcast path. The joiner declared its own
        // role + capability on the request frame; record that truth in
        // the replicated `RoleTable` (idempotent / set-once on re-apply,
        // so a duplicate from a concurrent secondary responder NoOps).
        // The requester's CURRENT membership incarnation: a frame from a
        // previously-removed id already routed through the dispatch
        // preamble's re-admission seam (which bumped the generation), so
        // this read observes the post-re-admission value; for a fresh
        // late-joiner it is 0.
        let member_gen = self.cluster_state.peer_member_gen(&sender_id);
        self.apply_and_broadcast_cluster_mutations(vec![ClusterMutation::PeerJoined {
            peer_id: sender_id,
            is_observer,
            can_be_primary,
            // Stamped at the origination choke point.
            cap_version: Default::default(),
            member_gen,
        }])
        .await;
    }

    /// Answer a peer's `RequestRunConfig` from this primary's node-local
    /// `forwarded_argv`.
    ///
    /// Single concern: the run-config-RPC responder on the primary side. It
    /// is a PURE READ-ONLY responder — it reads `self.forwarded_argv` and
    /// unicasts exactly ONE `RunConfig` back to the requester (its return
    /// address rides `sender_id`, mirroring the snapshot responder's reply
    /// edge). Unlike `handle_request_cluster_snapshot`, it does NOT
    /// originate `PeerJoined`, does NOT send `SecondaryWelcome`, and never
    /// touches roster / quorum / capacity / CRDT: the run-config is a
    /// node-local launch constant, not lattice data, and answering for it
    /// is read-only peer gossip, NOT primary authority (the work-split is
    /// preserved). The SAME read-only responder lives on the secondary
    /// router so a cold-start fetch is answerable before any primary
    /// exists / promotes.
    ///
    /// A send failure is logged best-effort, exactly as the snapshot
    /// responder treats its own; the requester's bounded recv wait falls
    /// back to its own deadline.
    pub(crate) async fn handle_request_run_config(&mut self, msg: DistributedMessage<I>) {
        let DistributedMessage::RequestRunConfig {
            target: None,
            sender_id,
            ..
        } = msg
        else {
            return;
        };
        let response = DistributedMessage::RunConfig {
            target: None,
            sender_id: self.config.node_id.clone(),
            timestamp: timestamp_now(),
            forwarded_argv: self.forwarded_argv.clone(),
        };
        if let Err(e) = self
            .send_to(
                Destination::Secondary(PeerId::from(sender_id.clone())),
                response,
            )
            .await
        {
            tracing::warn!(
                target = %sender_id,
                error = %e,
                "failed to deliver RunConfig response from primary"
            );
        }
    }

    /// Push this primary's node-local `forwarded_argv` to a freshly-
    /// welcomed secondary over its EXISTING mesh connection.
    ///
    /// Single concern: the run-config DELIVERY edge on the welcome-accept
    /// path. The welcomed secondary booted with only its boot-critical CLI
    /// args; it parses the consumer's run-config (`--task`, task filters)
    /// only AFTER it is connected, so the primary unicasts the same
    /// `RunConfig` frame the `RequestRunConfig` responder serves — but
    /// proactively, the moment the secondary is recorded as connected,
    /// rather than waiting for the secondary to ask. This reuses the
    /// already-established connection (the welcome/cert handshake that
    /// precedes it is untouched); it is purely a new SEND site for the
    /// existing message.
    ///
    /// Like the responder, this is read-only peer gossip: it reads
    /// `self.forwarded_argv` and never touches roster / capacity / CRDT.
    /// A send failure is logged best-effort — the secondary's own
    /// `RequestRunConfig` fallback (and bounded recv waits) still cover the
    /// rare drop.
    pub(crate) async fn push_run_config_to(&mut self, secondary_id: String) {
        let response = DistributedMessage::RunConfig {
            target: None,
            sender_id: self.config.node_id.clone(),
            timestamp: timestamp_now(),
            forwarded_argv: self.forwarded_argv.clone(),
        };
        if let Err(e) = self
            .send_to(
                Destination::Secondary(PeerId::from(secondary_id.clone())),
                response,
            )
            .await
        {
            tracing::warn!(
                target = %secondary_id,
                error = %e,
                "failed to push RunConfig to welcomed secondary"
            );
        }
    }

    /// Anti-entropy receive: compare a peer's `StateDigest` against the
    /// primary's own and pull a snapshot iff the primary is somehow behind.
    ///
    /// Single concern: the receive-side of the convergence cadence on the
    /// primary. The compare + target-selection + request-construction live
    /// in `crate::anti_entropy` so all three roles share ONE policy; this
    /// method only owns the primary's `send_to` edge. The authoritative
    /// primary is essentially never behind a follower's digest, so this is
    /// almost always a NoOp (a matching digest → `None`); it exists for
    /// uniformity (a freshly-promoted primary still warming its mirror, or
    /// an out-of-order seed window, could momentarily be behind a peer that
    /// already saw a mutation). The pull's reply heals via the primary's
    /// own `ClusterMutation`/snapshot apply paths. The primary declares
    /// itself non-observer + primary-capable on the request frame.
    pub(crate) async fn handle_state_digest(&mut self, msg: DistributedMessage<I>) {
        let DistributedMessage::StateDigest {
            target: None,
            digest,
            sender_id,
            ..
        } = msg
        else {
            return;
        };
        let local = self.cluster_state.digest();
        let requester = crate::anti_entropy::RequesterIdentity {
            node_id: &self.config.node_id,
            // The primary is never an observer and is always primary-capable.
            is_observer: false,
            can_be_primary: true,
        };
        if let Some((destination, request)) = crate::anti_entropy::reconcile_against_peer(
            &local,
            &digest,
            &sender_id,
            &requester,
            timestamp_now(),
        ) {
            if let Err(e) = self.send_to(destination, request).await {
                tracing::debug!(
                    error = %e,
                    peer = %sender_id,
                    "anti-entropy: primary snapshot pull request send failed; \
                     a later digest round retries"
                );
            } else {
                tracing::debug!(
                    peer = %sender_id,
                    "anti-entropy: primary behind peer digest; requested snapshot pull"
                );
            }
        }
    }

    pub(crate) async fn handle_cluster_mutation(&mut self, msg: DistributedMessage<I>) {
        if let DistributedMessage::ClusterMutation {
            target: None,
            mutations,
            ..
        } = msg
        {
            // Note whether any ledger-growing mutation rides in this
            // batch BEFORE moving the mutations into apply, so the
            // post-apply `total_tasks` refresh below absorbs runtime
            // task injection (`TaskAdded` / `TasksSpawned`) a peer
            // originated. Only those two variants grow the ledger;
            // refreshing for the hot terminal path (TaskCompleted /
            // TaskFailed) would be a wasted read.
            let has_task_added = mutations.iter().any(|m| {
                matches!(
                    m,
                    ClusterMutation::TaskAdded { .. } | ClusterMutation::TasksSpawned { .. }
                )
            });
            // Discovery-seed surface, distinct from the runtime-spawn
            // surface above. `TaskAdded` is originated ONLY as the
            // initial ledger seed — `seed_cluster_state` (run start) —
            // always paired with a `PhaseDepsSet`, never as an
            // incremental mid-run add (that is `TasksSpawned`,
            // which auto-resumes through the pool's dep machine via
            // `newly_pending`). A `TaskAdded` therefore marks the FIRST
            // ledger growth this node's pool sees. Snapshot pre-apply,
            // act post-apply (same shape as `has_task_added`).
            let carries_discovery_task_added = mutations
                .iter()
                .any(|m| matches!(m, ClusterMutation::TaskAdded { .. }));
            // Collect any PeerJoined ids riding in the batch BEFORE
            // moving the mutations into apply. After the batch
            // applies, each joined id may have resolved a previously-
            // unknown entry in some task's `preferred_secondaries`,
            // so we forget its dedup state and re-run validation.
            // Same shape as `has_task_added`: snapshot pre-apply,
            // act post-apply.
            let joined_peer_ids: Vec<String> = mutations
                .iter()
                .filter_map(|m| match m {
                    ClusterMutation::PeerJoined { peer_id, .. } => Some(peer_id.clone()),
                    _ => None,
                })
                .collect();
            // Receive-side pool growth surface: every `TasksSpawned`
            // entry the apply rule classifies as freshly `Pending`
            // (no deps, or all deps already `Completed`) is cloned
            // into `newly_pending`. A primary that still locally owns
            // a pool (`self.pending.is_some()`) reinjects each entry
            // so the pool stays coherent with the CRDT ledger across
            // wire-received batches.
            //
            // This matters for the re-promotion path: a demoted
            // primary applying a promoted-secondary's TasksSpawned
            // broadcast keeps the post-spawn tasks dispatchable in
            // its local pool, so a later re-election (the demoted
            // primary becomes live again) finds the pool already
            // aligned with the cluster's view. Without it, re-
            // election would resurrect the pool from its pre-spawn
            // snapshot and the post-spawn tasks would never
            // dispatch on this node.
            //
            // `resumed` is not consumed here for the same reason the
            // promoted-secondary's receive path discards it — the
            // pool's own dep machinery dispatches Blocked entries on
            // the prereq's `on_item_finished` event.
            let mut resumed: Vec<TaskInfo<I>> = Vec::new();
            let mut newly_pending: Vec<TaskInfo<I>> = Vec::new();
            // Worker-roster growth surface (wire-receive twin of the
            // originator-side detection in
            // `apply_and_broadcast_cluster_mutations`): track whether THIS
            // batch genuinely applied a `SecondaryCapacity` — i.e. a
            // worker became ready and a new idle slot now exists in the
            // ledger. Read the per-mutation `ApplyOutcome` inline (the
            // set-once capacity apply returns `Applied` only on the first
            // record for an id, `NoOp` on every redundant re-emit /
            // snapshot replay), so the rebuild fires exactly once per new
            // secondary. Acted on post-loop, after the roster source
            // (`cluster_state.secondary_capacities`) has grown.
            let mut capacity_grew = false;
            for m in mutations {
                let is_capacity = matches!(m, ClusterMutation::SecondaryCapacity { .. });
                let outcome =
                    self.cluster_state
                        .apply_with_resumed_blocked(m, &mut resumed, &mut newly_pending);
                if is_capacity && outcome == crate::cluster_state::ApplyOutcome::Applied {
                    capacity_grew = true;
                }
            }
            // Pool-coherence after a ledger-growing apply. Two
            // mutually-exclusive surfaces, both gated on this node still
            // owning a pool (`self.pending.is_some()`):
            //
            //   * Discovery seed at a quiescent pool (REBUILD). A
            //     `TaskAdded` Applied AND the pool currently holds no
            //     queued dispatchable work — the pool was built before
            //     discovery seeded the ledger and is drained/empty (the
            //     pre-staged `--source-already-staged` path: phases sit
            //     `Active`-empty or `Drained`, every declared phase with
            //     zero items). `TaskAdded` does NOT feed `newly_pending` (only
            //     `TasksSpawned` does, in `apply_tasks.rs`), so a plain
            //     reinject never runs for it and the discovered tasks
            //     would stay in the CRDT ledger un-dispatchable. REBUILD
            //     the pool from the now-seeded `cluster_state` via the
            //     `hydrate_from_cluster_state` primitive — it reads the
            //     batch's just-applied `phase_deps`, classifies each
            //     discovery phase Active/Blocked, queues every
            //     freshly-`Pending` discovery task, and `cascade_drain_done`s
            //     a declared-but-empty phase to `Done` (the activated-primary
            //     semantics; no `on_phase_end` re-fires). This re-activates
            //     the drained pool — the integrated form of the same
            //     dispatch-enablement the runtime-spawn reinject below gives
            //     an ACTIVE phase. The predicate is one-shot by
            //     construction: the rebuild queues the seed, so the pool
            //     then holds dispatchable work and a re-delivered batch
            //     (idempotent `TaskAdded` NoOp) does not re-trigger.
            //
            //   * Runtime spawn into the live pool (INCREMENTAL reinject).
            //     `TasksSpawned` entries the apply rule classified freshly
            //     `Pending` (no deps, or all deps already `Completed`) ride
            //     `newly_pending`. A primary that still owns a pool
            //     reinjects each so the pool stays coherent with the CRDT
            //     ledger across wire-received batches. This matters for the
            //     re-promotion path: a demoted primary applying a promoted-
            //     secondary's TasksSpawned broadcast keeps the post-spawn
            //     tasks dispatchable, so a later re-election finds the pool
            //     already aligned with the cluster's view (without it,
            //     re-election would resurrect the pool from its pre-spawn
            //     snapshot and the post-spawn tasks would never dispatch).
            //
            // The rebuild SUBSUMES the incremental reinject for the seed
            // batch (the rebuilt pool already holds every discovery task),
            // so the two are mutually exclusive. Each emits a decoupled
            // `TasksAdded` worker-mgmt signal so the recheck dispatches the
            // new work — never a direct dispatch call (the dispatch-
            // decoupling law).
            if self.pending.is_some() {
                if carries_discovery_task_added && !self.pool().has_queued_dispatchable() {
                    tracing::info!(
                        crdt_tasks = self.cluster_state.task_count(),
                        "discovery TaskAdded seeded the ledger at a quiescent \
                         pool; rebuilding from cluster_state so the discovered \
                         tasks become dispatchable"
                    );
                    self.hydrate_from_cluster_state();
                    // `hydrate_from_cluster_state` no longer self-drains empty
                    // phases (the primary's coordinator owns the narrated
                    // cascade at run-entry). At this MID-RUN rebuild there is
                    // no run-entry cascade to follow, so drain trivially-empty
                    // phases here for dependent visibility — the same silent
                    // cascade the secondary-hydration port performed in-line
                    // before. Callback narration is owned by the operational
                    // loop's `process_phase_lifecycle`.
                    crate::secondary::origination::cascade_drain_done(self.pool_mut());
                    self.cluster_state
                        .emit_worker_mgmt(WorkerMgmtSignal::TasksAdded);
                } else {
                    let reinjected_any = !newly_pending.is_empty();
                    for task in newly_pending {
                        tracing::debug!(
                            phase = %task.phase_id,
                            task_id = ?task.task_id,
                            "pool: reinject freshly-Pending task from \
                             wire-received TasksSpawned"
                        );
                        self.pool_mut().reinject(task);
                    }
                    if reinjected_any {
                        self.cluster_state
                            .emit_worker_mgmt(WorkerMgmtSignal::TasksAdded);
                    }
                }
            }
            if !joined_peer_ids.is_empty() {
                // A peer that just joined may have resolved a
                // previously-unknown `preferred_secondaries` id; drop
                // each joined id from the warn dedup set so the next
                // validation cycle re-evaluates from scratch, then
                // walk every replicated task in `cluster_state` (the
                // authoritative post-apply view — on a demoted /
                // setup-promoted primary `all_binaries` is empty and
                // `cluster_state.iter_all()` is the only source).
                for id in &joined_peer_ids {
                    self.preferred_secondaries_validator.forget(id);
                }
                let known: std::collections::HashSet<&str> =
                    self.secondaries.keys().map(|s| s.as_str()).collect();
                let tasks: Vec<TaskInfo<I>> = self
                    .cluster_state
                    .iter_all()
                    .map(|(_, t)| t.clone())
                    .collect();
                self.preferred_secondaries_validator
                    .validate(tasks.iter(), &known);
            }
            if has_task_added {
                // Refresh `total_tasks` from the post-apply CRDT view
                // so runtime task injection (`TaskAdded` / `TasksSpawned`)
                // a peer originated is absorbed into the authoritative
                // primary's count. Idempotent against duplicate-Add via
                // the CRDT's presence semantics: re-applying a TaskAdded
                // for a hash already in the ledger leaves `task_count`
                // unchanged.
                self.total_tasks = self.cluster_state.task_count();
            }
            if capacity_grew {
                // A worker became ready: a `SecondaryCapacity` this batch
                // genuinely applied grew the replicated roster. Rebuild the
                // local worker cache from the now-grown capacity set and
                // emit `TasksAdded` so the dispatch recheck re-evaluates the
                // new idle slot against the ready pool — the worker-ready
                // half of state-based dispatch (the wire-receive twin of the
                // originator-side reaction). Owned by worker management; the
                // apply path here only DETECTS the growth.
                self.react_to_capacity_growth();
            }
        }
    }
}
