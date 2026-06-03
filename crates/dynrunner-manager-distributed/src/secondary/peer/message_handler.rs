//! Inbound peer-message dispatch.
//!
//! Single concern: receive one `DistributedMessage` arriving via the
//! peer mesh and route it to the appropriate per-message handler. The
//! method itself is a wide `match` because the wire shape is a flat
//! enum; every arm delegates the actual state mutation to a
//! purpose-built helper elsewhere in `secondary/`. The handler also
//! covers the post-promotion convergence rules: peer-mesh-arrived
//! `TaskRequest` / `TaskAssignment` / `ClusterMutation` messages
//! re-enter the same primary / dispatch / CRDT-apply paths the
//! primary-transport variants use.

use dynrunner_core::{ErrorType, Identifier};
use dynrunner_manager_local::WorkerFactory;
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{DistributedMessage, PeerTransport};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use super::super::SecondaryCoordinator;
use super::super::wire::timestamp_now;

impl<Tr, M, S, E, I> SecondaryCoordinator<Tr, M, S, E, I>
where
    Tr: PeerTransport<I>,
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    /// The SINGLE inbound-frame entry point. Every frame the unified
    /// transport yields (uplink OR mesh — the manager never sees which)
    /// flows here. Dispatch is by (frame type, node ROLE), NEVER by
    /// which transport delivered it — that physical-origin key was the
    /// transport-locality conflation P2 eliminates.
    ///
    /// This is the role-aware, mesh-native canonical handler. Frame
    /// types it owns directly (Keepalive, election frames, role-aware
    /// TaskComplete/TaskFailed, ClusterMutation) carry the
    /// non-authoritative secondary behavior. Frame types it does not
    /// own (TaskAssignment, StageFile, PromotePrimary,
    /// RequestClusterSnapshot, ClusterSnapshot, PeerInfo) fall to the
    /// catch-all, which delegates to [`Self::dispatch_message`] — the
    /// wire-frame dispatcher, still directly callable by tests.
    ///
    /// # Per-frame error fatality
    ///
    /// Most frames are non-fatal: a transient dispatch error on a
    /// non-authoritative secondary must not kill the run (the old
    /// `.await?` that propagated was a transport-ORIGIN artifact — it
    /// fired because the frame arrived on the uplink, not because the
    /// frame is semantically fatal; that per-origin key is gone). Those
    /// are SWALLOWED+WARNED in the catch-all.
    ///
    /// The genuinely-fatal frames are made EXPLICITLY fatal per-frame:
    /// a `ClusterSnapshot` whose `restore` fails on a bootstrapping
    /// observer / late-joiner would leave the node observing a partial
    /// or empty CRDT (a P3 replication-invariant violation), so its
    /// failure is latched into `fatal_exit` and the loop aborts the run
    /// rather than silently observing a lie. See the `ClusterSnapshot`
    /// arm in `dispatch_message`.
    pub(in crate::secondary) async fn handle_inbound(
        &mut self,
        msg: DistributedMessage<I>,
        factory: &mut impl WorkerFactory<M>,
    ) {
        match msg {
            DistributedMessage::Keepalive {
                secondary_id,
                timestamp,
                active_workers,
                ..
            } => {
                // Recognition by ROLE. A keepalive whose originator IS the
                // recognized primary is a PRIMARY-liveness assertion, so it
                // refreshes `primary_last_seen` via the same
                // `record_primary_message` the dispatch path uses;
                // otherwise it is a peer's mesh keepalive and feeds
                // `peer_keepalives` (which drives this node's failover
                // timing for its peers). Without this split, primary
                // liveness was parasitic on workload dispatch and
                // `primary_silent` tripped the instant dispatch quiesced.
                //
                // The recognition IDENTITY is `recognized_primary_id()`, a
                // TOTAL function decoupled from ROUTING. Routing reads the
                // transport `RoleCache` (mirrored from `current_primary()`),
                // which is COLD on bootstrap by design so traffic flows over
                // the uplink — and that COLD state surfaces as
                // `current_primary() == None`. Keying recognition on bare
                // `current_primary()` would inherit that routing artifact
                // and NEVER recognize the bootstrap primary's keepalives
                // (filing them as peer keepalives), so `primary_last_seen`
                // would go stale once dispatch quiesced and a false election
                // would trip. `recognized_primary_id()` falls back to the
                // bootstrap primary's well-known canonical identity
                // (`primary_node_id()`, the id it stamps onto its keepalives)
                // exactly while no failover has named a concrete winner; once
                // one has, the fallback is off and a zombie old bootstrap
                // primary's keepalives no longer match.
                if self.recognized_primary_id() == secondary_id {
                    self.record_primary_message();
                    tracing::trace!(
                        primary = %secondary_id,
                        active_workers,
                        "primary keepalive received"
                    );
                } else {
                    self.peer_keepalives.insert(secondary_id.clone(), timestamp);
                    tracing::trace!(
                        peer = %secondary_id,
                        active_workers,
                        "peer keepalive received"
                    );
                }
            }
            DistributedMessage::TaskComplete {
                secondary_id,
                task_hash,
                ..
            } => {
                // A peer's own-worker TaskComplete REPORT, observed on
                // the mesh. The secondary is NEVER the authority: it
                // keeps no per-node terminal set and originates no CRDT
                // mutation. The authoritative completion (accounting +
                // keyed-outputs apply + phase-machine advance) is owned
                // by the `PrimaryCoordinator`, which is itself a mesh
                // member and receives the originator's broadcast
                // directly — so the old secondary→primary FORWARD (the
                // #50 redundant-delivery backstop) is no longer needed
                // and is dropped: a non-authority must not re-emit
                // another node's terminal report. Pure observation.
                tracing::trace!(
                    peer = %secondary_id,
                    task_hash,
                    "observed peer TaskComplete"
                );
            }
            DistributedMessage::TaskFailed {
                secondary_id,
                task_hash,
                error_type,
                ..
            } => {
                // A peer's own-worker TaskFailed REPORT, observed on the
                // mesh. As with TaskComplete, the authoritative failure
                // accounting + backpressure requeue + retry cascade are
                // the `PrimaryCoordinator`'s, reached directly as a mesh
                // member — so the secondary→primary forward is dropped.
                //
                // The one own-worker side effect KEPT: on a real
                // terminal failure (NOT a `Recoverable` backpressure
                // marker — those mean "never ran, will be requeued"),
                // re-poll OUR OWN idle workers so an authority-reinjected
                // retry dispatched back to this node reaches an idle
                // worker on the next tick rather than waiting a full
                // keepalive interval. Own-worker management, not
                // authority.
                if !matches!(error_type, ErrorType::Recoverable) {
                    self.repoll_idle_workers().await;
                }
                tracing::trace!(
                    peer = %secondary_id,
                    task_hash,
                    error_type = ?error_type,
                    "observed peer TaskFailed"
                );
            }
            DistributedMessage::TimeoutDetected {
                timed_out_secondary_id,
                last_seen,
                ..
            } => {
                tracing::warn!(
                    timed_out = %timed_out_secondary_id,
                    last_seen,
                    "peer timeout detected by another secondary"
                );
            }
            DistributedMessage::TimeoutQuery {
                query_node_id,
                sender_id,
                ..
            } => {
                // Respond with our last known keepalive for the queried node.
                let last_keepalive = self.peer_keepalives.get(&query_node_id).copied();
                let response: DistributedMessage<I> = DistributedMessage::TimeoutResponse {
                    sender_id: self.config.secondary_id.clone(),
                    timestamp: timestamp_now(),
                    query_node_id,
                    last_keepalive,
                };
                tracing::debug!(peer = %sender_id, "timeout query received, queueing response");
                // Queue for async send — will be flushed in the main loop
                self.pending_peer_messages.push((sender_id, response));
            }
            DistributedMessage::TimeoutResponse {
                sender_id,
                query_node_id: _,
                last_keepalive,
                ..
            } => {
                self.record_timeout_response(sender_id, last_keepalive);
            }
            DistributedMessage::PromotionVote {
                sender_id,
                candidate_id,
                vote_round,
                ..
            } => {
                if let Some(reply) = self.record_promotion_vote(candidate_id, vote_round) {
                    self.pending_peer_messages.push((sender_id, reply));
                }
            }
            DistributedMessage::PromotionConfirm {
                sender_id,
                new_primary_id,
                vote_round,
                ..
            } => {
                // `record_promotion_confirm` returns `true` the instant
                // this node's candidate tally crosses quorum and the
                // election transitions to its terminal `Promoted` state.
                // That `true` is the TERMINAL ACTION cue: fire the
                // co-located parked primary's activation gate (seeded
                // resume from the replicated CRDT) and broadcast
                // `PromotePrimary { new = self }` so surviving
                // secondaries re-point `Role::Primary` onto this winner.
                // Pre-fix this return was discarded, so a surviving
                // secondary that won its election could never actually
                // become primary — the failover path dead-ended here.
                if self.record_promotion_confirm(sender_id, new_primary_id, vote_round) {
                    self.fire_local_promotion().await;
                }
            }
            // Post-promotion TaskAssignment: when the new primary IS a
            // Peer-mesh CRDT replication: any node may originate a
            // `ClusterMutation` batch on the mesh (the promoted
            // secondary's `apply_and_broadcast_mutations` does this
            // for `TaskAdded` during `ingest_setup_discovery` and for
            // `RunComplete` in the natural-quiesce branch). Applied via
            // the single-concern `apply_cluster_mutations` helper; CRDT
            // idempotency makes any duplicate apply a no-op.
            DistributedMessage::ClusterMutation { mutations, .. } => {
                self.apply_cluster_mutations(mutations);
            }
            // Wire-frame / setup / snapshot frames the role-aware base
            // does not own (TaskAssignment, StageFile, PromotePrimary,
            // RequestClusterSnapshot, ClusterSnapshot, PeerInfo) delegate
            // to the wire-frame dispatcher. ONE delegation path — no
            // physical-origin key.
            //
            // Per-frame fatality: most frames are non-fatal and their
            // dispatch errors are SWALLOWED+WARNED here. The genuinely-
            // fatal ones latch `self.fatal_exit` from inside their own
            // arm (the `ClusterSnapshot` restore-failure on a
            // bootstrapping observer is the P3-critical one) — the
            // operational loop reads `fatal_exit` once per iteration and
            // aborts the run, so a fatal frame is NOT masked by this
            // swallow.
            other => {
                if let Err(e) = self.dispatch_message(other, factory).await {
                    tracing::warn!(
                        error = %e,
                        "inbound frame dispatch failed (non-fatal; swallowed)"
                    );
                }
            }
        }
    }
}
