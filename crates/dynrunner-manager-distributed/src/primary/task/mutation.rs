use dynrunner_core::{Identifier, TaskInfo};
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, Destination, DistributedMessage, PeerId, PeerTransport,
};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use crate::primary::PrimaryCoordinator;
use crate::primary::wire::timestamp_now;
use crate::worker_signal::WorkerMgmtSignal;

impl<Tr: PeerTransport<I>, S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier>
    PrimaryCoordinator<Tr, S, E, I>
{
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
        self.apply_and_broadcast_cluster_mutations(vec![ClusterMutation::PeerJoined {
            peer_id: sender_id,
            is_observer,
            can_be_primary,
            // Stamped at the origination choke point.
            cap_version: Default::default(),
        }])
        .await;
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
            digest, sender_id, ..
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
        if let DistributedMessage::ClusterMutation { mutations, .. } = msg {
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
            for m in mutations {
                self.cluster_state
                    .apply_with_resumed_blocked(m, &mut resumed, &mut newly_pending);
            }
            if self.pending.is_some() {
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
                // Wire-received `TasksSpawned` that grew the live pool is
                // a pool-entry edge — EMIT a `TasksAdded` so the
                // worker-management recheck dispatches the new work.
                // Decoupled emit, never a direct dispatch call (the
                // dispatch-decoupling law).
                if reinjected_any {
                    self.cluster_state
                        .emit_worker_mgmt(WorkerMgmtSignal::TasksAdded);
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
        }
    }
}
