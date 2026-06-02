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
use tokio::sync::mpsc as tokio_mpsc;

use crate::primary::PrimaryCommand;

use super::super::wire::timestamp_now;
use super::super::SecondaryCoordinator;

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
    /// # Error handling (TODO(R3))
    ///
    /// This entry SWALLOWS+WARNS dispatch errors (the canonical base's
    /// contract): post-unification the secondary is non-authoritative,
    /// so a transient dispatch error must not kill the run (the old
    /// `.await?` that propagated was a transport-ORIGIN artifact — it
    /// fired because the frame arrived on the uplink, not because the
    /// frame is semantically fatal; that per-origin key is gone). R3
    /// must make GENUINELY-fatal frames (e.g. a `ClusterSnapshot`
    /// restore failure on a bootstrapping observer, setup failures)
    /// EXPLICITLY fatal per-frame so nothing genuinely-fatal is
    /// silently swallowed.
    ///
    /// `command_rx` threads the operational-loop's command-channel
    /// receiver into the TaskComplete / TaskFailed cascade (see
    /// `process_primary_phase_lifecycle` doc). Off-loop callers pass
    /// `&mut None`.
    pub(in crate::secondary) async fn handle_inbound(
        &mut self,
        msg: DistributedMessage<I>,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
        factory: &mut impl WorkerFactory<M>,
    ) {
        match msg {
            DistributedMessage::Keepalive {
                secondary_id,
                timestamp,
                active_workers,
                ..
            } => {
                self.peer_keepalives.insert(secondary_id.clone(), timestamp);
                tracing::trace!(
                    peer = %secondary_id,
                    active_workers,
                    "peer keepalive received"
                );
            }
            DistributedMessage::TaskComplete {
                secondary_id,
                worker_id,
                task_hash,
                result_data,
                ..
            } => {
                // LIVE non-authoritative behavior — KEPT:
                //   - `completed_tasks` dedup so a duplicate observed
                //     completion isn't re-processed.
                self.completed_tasks.insert(task_hash.clone());
                //
                // STRIPPED (R0-deleted secondary primary_* authority
                // mirror — methods no longer exist): the per-peer
                // backpressure clear, the apply-task-completed-locally
                // race fix, and the `note_primary_item_completed`
                // phase-machine drive. The secondary is NEVER the
                // authority now; authoritative completion accounting +
                // phase-machine advance live in `PrimaryCoordinator`.
                //
                // TODO(R4): re-home the authoritative completion
                //   accounting (the old `note_primary_item_completed` /
                //   apply-locally) to the co-located `PrimaryCoordinator`
                //   reached over the loopback transport (P4 composition).
                // TODO(R3): under the pure-observer model, decide the
                //   `completed_tasks` dedup-vs-CRDT-counter semantics —
                //   an observer reads `cluster_state.outcome_counts()`,
                //   not its own set.
                tracing::debug!(
                    peer = %secondary_id,
                    task_hash,
                    "peer task complete"
                );
                // LIVE — KEPT: forward the observed completion to the
                // primary role. Redundant-delivery backstop for the #50
                // wire-loss symptom (the direct originator→primary
                // TaskComplete sometimes drops; every observer forwards
                // so the authority has N-1 alternate paths; its
                // handle_task_complete is dedup-gated).
                //
                // TODO(R3): is this forward still needed now the primary
                //   is a mesh member that receives the broadcast
                //   directly? Likely NO (the authority observes the
                //   originator's mesh broadcast), but that's an observer
                //   / role-model decision R3 owns. Kept verbatim for now
                //   to preserve current LIVE behavior.
                let forward = DistributedMessage::TaskComplete {
                    sender_id: self.config.secondary_id.clone(),
                    timestamp: timestamp_now(),
                    secondary_id,
                    worker_id,
                    task_hash,
                    result_data,
                };
                let _ = self.send_to_primary(forward).await;
            }
            DistributedMessage::TaskFailed {
                secondary_id,
                worker_id,
                task_hash,
                error_type,
                error_message,
                ..
            } => {
                // Backpressure-shape classification (LIVE — pure logic,
                // KEPT): a `Recoverable` failure carrying one of the
                // backpressure markers means the task never ran and must
                // be requeued at the authority, not counted as failed.
                let is_backpressure = matches!(error_type, ErrorType::Recoverable)
                    && (error_message == "No idle worker available"
                        || error_message == "worker pipe broken; respawning");
                //
                // STRIPPED (R0-deleted secondary primary_* authority
                // mirror — methods no longer exist):
                //   - `handle_primary_peer_rejection` (backpressure
                //     re-queue + per-peer backoff on the primary pool),
                //   - `note_primary_item_failed` (failure-aware in-flight
                //     decrement + per-phase retry-bucket cascade).
                // The secondary is NEVER the authority now; the
                // authoritative failure accounting + requeue + retry
                // cascade live in `PrimaryCoordinator`.
                //
                // TODO(R4): re-home the authoritative TaskFailed
                //   handling (backpressure requeue + note_item_failed +
                //   retry-bucket cascade) to the co-located
                //   `PrimaryCoordinator` over the loopback (P4). The
                //   `is_backpressure` classification + marker strings
                //   are the wire contract the authority's
                //   `handle_task_failed` already recognises, so the
                //   re-homed authority sees the same shape via the
                //   forward below.
                if !is_backpressure {
                    // LIVE — KEPT: re-poll OUR own idle workers (own
                    // worker management — not authority). A retry the
                    // authority reinjects reaches our idle worker on the
                    // next tick rather than waiting a keepalive interval.
                    self.repoll_idle_workers(factory).await;
                }
                tracing::debug!(
                    peer = %secondary_id,
                    task_hash,
                    error_type = ?error_type,
                    is_backpressure,
                    "peer task failed (observed)"
                );
                // LIVE — KEPT: forward the observed failure to the
                // primary role (the #50 redundant-delivery backstop;
                // the authority's handle_task_failed is dedup-gated and
                // recognises the backpressure markers, so a single
                // forward covers both the backpressure-requeue and the
                // terminal-failure cases at the authority).
                //
                // TODO(R3): is this forward still needed now the primary
                //   is a mesh member receiving the broadcast directly?
                //   (Same observer/role question as the TaskComplete
                //   forward.) Kept verbatim to preserve LIVE behavior.
                let forward = DistributedMessage::TaskFailed {
                    sender_id: self.config.secondary_id.clone(),
                    timestamp: timestamp_now(),
                    secondary_id,
                    worker_id,
                    task_hash,
                    error_type,
                    error_message,
                };
                let _ = self.send_to_primary(forward).await;
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
                self.record_promotion_confirm(sender_id, new_primary_id, vote_round);
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
            // physical-origin key. Errors are SWALLOWED+WARN'd (see the
            // method doc's TODO(R3) on per-frame fatality): a transient
            // dispatch error must not kill a non-authoritative
            // secondary's run.
            other => {
                if let Err(e) = self.dispatch_message(other, command_rx, factory).await {
                    tracing::warn!(
                        error = %e,
                        "inbound frame dispatch failed (swallowed; \
                         TODO(R3): per-frame fatality)"
                    );
                }
            }
        }
    }
}

