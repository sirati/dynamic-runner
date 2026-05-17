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

use dynrunner_core::{ErrorType, Identifier, MessageReceiver, MessageSender};
use dynrunner_manager_local::WorkerFactory;
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{DistributedMessage, PeerTransport};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};
use tokio::sync::mpsc as tokio_mpsc;

use crate::primary::PrimaryCommand;

use super::super::wire::timestamp_now;
use super::super::SecondaryCoordinator;

impl<PT, P, M, S, E, I> SecondaryCoordinator<PT, P, M, S, E, I>
where
    PT: MessageSender<DistributedMessage<I>> + MessageReceiver<DistributedMessage<I>>,
    P: PeerTransport<I>,
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    /// `command_rx` threads the operational-loop's command-channel
    /// receiver into the TaskComplete / TaskFailed cascade (see
    /// `process_primary_phase_lifecycle` doc). Off-loop callers pass
    /// `&mut None`.
    pub(in crate::secondary) async fn handle_peer_message(
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
                // Track peer's completed task to avoid duplicate processing
                self.completed_tasks.insert(task_hash.clone());
                // A successful TaskComplete from this peer proves it's
                // healthy — clear any primary backpressure
                // backoff so the next dispatch cycle can re-target it.
                // Mirrors regular primary's TaskComplete handler.
                self.clear_primary_peer_backpressure(&secondary_id);
                // Drive the primary's phase machine: if this
                // node dispatched the task as primary, the
                // peer's completion message is the only signal the
                // pool gets that the item is no longer in flight.
                self.note_primary_item_completed(&task_hash, command_rx).await;
                tracing::debug!(
                    peer = %secondary_id,
                    task_hash,
                    "peer task complete"
                );
                // Forward the observed peer completion to the
                // current primary, regardless of whether self
                // holds primary authority. Two distinct cases
                // converge here:
                //
                //   1. Post-promotion (self.is_primary == true):
                //      forward back to the demoted local primary
                //      so its per-task accounting catches cross-
                //      secondary completions observed via the
                //      peer mesh — pre-Phase-B this missed every
                //      such event and surfaced as inflated
                //      `stranded` counts. (Existing behaviour;
                //      preserved via the same forward.)
                //
                //   2. Live-primary case (self.is_primary == false,
                //      N>1 secondaries with active dispatcher):
                //      forward to the dispatcher so its
                //      `completed_tasks` accumulator has N-1
                //      redundant paths to learn about each
                //      completion. asm-tokenizer's persistent
                //      "secondary success count > primary
                //      success count by 17-37 events" wire-loss
                //      symptom (#50) is this exact failure mode:
                //      the direct originator→primary TaskComplete
                //      sometimes drops, and pre-fix there was no
                //      alternate path to the live primary
                //      because it isn't in the peer mesh.
                //      Post-fix every peer that observes the
                //      broadcast also forwards via primary_link.
                //      Primary's handle_task_complete is dedup-
                //      gated on `completed_tasks.contains(hash)`,
                //      and `apply_and_broadcast_cluster_mutations`
                //      only re-broadcasts mutations the CRDT
                //      actually changed state for (NoOp on
                //      dupes), so the N-fold fan-in bounds at
                //      1 broadcast per unique event regardless
                //      of how many peer-forwards converge.
                //
                // Cross-link failures swallowed: a dropped
                // forward is exactly the case the redundancy is
                // meant to survive — peers other than this one
                // cover it.
                let forward = DistributedMessage::TaskComplete {
                    sender_id: self.config.secondary_id.clone(),
                    timestamp: timestamp_now(),
                    secondary_id,
                    worker_id,
                    task_hash,
                    result_data,
                };
                let _ = self.send_to_current_primary(forward).await;
            }
            DistributedMessage::TaskFailed {
                secondary_id,
                worker_id,
                task_hash,
                error_type,
                error_message,
                ..
            } => {
                // Two TaskFailed shapes arrive on the primary
                // path:
                //   1. Backpressure rejection — peer's dispatch.rs
                //      sends `Recoverable / "No idle worker
                //      available"` when its worker pool can't accept
                //      the assignment. The task NEVER ran; the
                //      binary must be returned to the pool, the
                //      peer marked backpressured. Drives
                //      `handle_primary_peer_rejection` (re-queue +
                //      backoff). Skipping it would leak the binary
                //      from `primary_in_flight` and stall the
                //      per-phase in_flight counter.
                //   2. Terminal failure — peer's worker actually ran
                //      the binary and reported failure (Recoverable
                //      from the worker, NonRecoverable, OutOfMemory,
                //      etc.). The phase machine just needs the
                //      in-flight counter decremented.
                // Backpressure shapes — both mean "task didn't
                // actually run; requeue at the primary pool instead
                // of decrementing in_flight as failed":
                //
                //   1. "No idle worker available" — peer's worker
                //      pool full at dispatch time.
                //   2. "worker pipe broken; respawning" — peer's
                //      target worker subprocess died between
                //      tasks; pipe-write failed; the peer is
                //      respawning. The not-yet-attempted task is
                //      sent back with this marker so the primary
                //      requeues (does not mark as terminal-failed)
                //      and re-dispatches to a peer with capacity.
                //      Without this case, Bug C produced silent
                //      task loss on every Broken-pipe assign
                //      attempt at a peer secondary.
                let is_backpressure = matches!(error_type, ErrorType::Recoverable)
                    && (error_message == "No idle worker available"
                        || error_message == "worker pipe broken; respawning");
                if is_backpressure {
                    if let Some(peer) = self.handle_primary_peer_rejection(&task_hash) {
                        tracing::debug!(
                            peer = %peer,
                            task_hash,
                            "peer rejected primary assignment; re-queued + backpressure backoff applied"
                        );
                    }
                } else {
                    // Route through the failure-aware decrementer:
                    // Recoverable failures land in
                    // `primary_failed` for the retry pass,
                    // others just decrement in-flight as before.
                    self.note_primary_item_failed(&task_hash, &error_type, command_rx).await;
                    // Synchronous kickstart: `note_primary_item_failed`
                    // ran the per-phase retry-bucket cascade inline
                    // (see `secondary/primary/lifecycle.rs`); the
                    // bucket may have reinjected the failed task
                    // into the pool. Re-poll OUR own idle workers
                    // so the reinjected item reaches a worker on
                    // this tick instead of waiting up to one
                    // keepalive interval. Peer workers self-recover
                    // on their own keepalive tick. No-op when no
                    // worker is idle.
                    self.repoll_idle_workers(factory).await;
                    tracing::debug!(
                        peer = %secondary_id,
                        task_hash,
                        error_type = ?error_type,
                        "peer task failed"
                    );
                    // Mirror the TaskComplete-arm forward (#50
                    // peer-forwarding redundancy): forward the
                    // observed peer-TaskFailed to the current
                    // primary regardless of whether self holds
                    // primary authority. Two cases converge:
                    //
                    //   1. Post-promotion (self.is_primary): forward
                    //      to the demoted local primary so its
                    //      `failed_tasks` grows and the
                    //      ClusterMutation::TaskFailed broadcast
                    //      fires. (Existing behaviour.)
                    //
                    //   2. Live-primary case (self.is_primary
                    //      false): forward to the dispatcher so
                    //      its accounting has N-1 redundant paths
                    //      for terminal failures. Pre-fix, a
                    //      dropped originator→primary TaskFailed
                    //      was unrecoverable on the live-primary
                    //      path (primary isn't in the peer mesh).
                    //      Primary's handle_task_failed is dedup-
                    //      gated on `failed_tasks.contains_key`
                    //      || `completed_tasks.contains`, and
                    //      apply_and_broadcast only broadcasts
                    //      Applied mutations, so the N-fold
                    //      fan-in bounds at 1 broadcast per
                    //      unique event.
                    //
                    // Backpressure-rejection failures bypass this
                    // branch entirely (they're handled by the
                    // `is_backpressure` arm above; nothing to
                    // record on the ledger).
                    let forward = DistributedMessage::TaskFailed {
                        sender_id: self.config.secondary_id.clone(),
                        timestamp: timestamp_now(),
                        secondary_id,
                        worker_id,
                        task_hash,
                        error_type,
                        error_message,
                    };
                    let _ = self.send_to_current_primary(forward).await;
                }
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
            DistributedMessage::TaskRequest {
                secondary_id,
                worker_id,
                available_resources,
                ..
            } if self.is_primary => {
                // Peer routed this to us because we won the election. Same
                // dispatch path that the live-primary case uses, just
                // arriving over peer_transport instead of primary_transport.
                let available_memory = available_resources
                    .iter()
                    .find(|r| r.kind == dynrunner_core::ResourceKind::memory())
                    .map(|r| r.amount)
                    .unwrap_or(0);
                if let Err(e) = self
                    .handle_primary_task_request(secondary_id, worker_id, available_memory, factory)
                    .await
                {
                    tracing::warn!(error = %e, "post-promotion peer TaskRequest dispatch failed");
                }
            }
            // Post-promotion TaskAssignment: when the new primary IS a
            // peer, its TaskAssignment to this secondary arrives over
            // peer_transport, not primary_transport. The dispatch body
            // (path resolution, worker assignment, failure reporting)
            // is identical regardless of transport, so we delegate to
            // dispatch_message — keeping ONE place that handles the
            // wire shape. Pre-fix this arm was absent and the message
            // fell through the `_` catch-all below, silently dropped.
            // Observable symptom: asm-tokenizer 9ca9124 post-promotion
            // run, the promoted node's own workers ran 445/446 tasks
            // each while peer secondaries' workers stopped at 1 task
            // each (their pre-promotion initial assignment) — half the
            // cluster's compute parked.
            //
            // record_primary_message inside dispatch_message is the
            // right semantic for a promoted-peer-to-us TaskAssignment:
            // the sender IS the current primary, so its message arrival
            // IS a primary-link health signal. The reset of
            // primary-link's failure tracking is also correct (the
            // primary is reachable via the peer mesh now).
            msg @ DistributedMessage::TaskAssignment { .. } => {
                if let Err(e) = self.dispatch_message(msg, command_rx, factory).await {
                    tracing::warn!(
                        error = %e,
                        "post-promotion peer TaskAssignment dispatch failed"
                    );
                }
            }
            // Peer-mesh CRDT replication: any node may originate a
            // `ClusterMutation` batch on the peer bus (the promoted
            // secondary's `apply_and_broadcast_mutations` does this
            // for `TaskAdded` during `ingest_setup_discovery` and
            // for `RunComplete` in the `processing.rs` natural-
            // quiesce branch). Receiver-side apply is symmetric
            // with the `primary_transport` path in `dispatch.rs`:
            // both route through the same `apply_cluster_mutations`
            // helper, which is the single-concern API for "apply a
            // wire-arrived batch to the local CRDT mirror". CRDT
            // idempotency makes duplicate applies (e.g. a mutation
            // arriving both via `primary_transport` from the
            // demoted submitter and via `peer_transport` from the
            // originating peer) a no-op.
            //
            // Pre-fix this arm was absent and the message fell into
            // the `_` catch-all below. Concrete regression — the
            // promoted secondary's natural-quiesce
            // `peer_transport.broadcast(ClusterMutation::RunComplete)`
            // landed at peer secondaries but never updated their
            // `cluster_state.run_complete()` flag, so their
            // `processing.rs` run-complete exit cue never tripped
            // and they hung indefinitely (Tier-2 asm-tokenizer hang
            // post-`a78c89c`). The same gap also silently dropped
            // the `TaskAdded` peer broadcasts during
            // `ingest_setup_discovery`, leaving peer secondaries'
            // `cluster_state` empty for the lifetime of the run —
            // a CRDT-replication gap that was masked pre-`a78c89c`
            // because no run-complete exit cue rode the peer bus.
            DistributedMessage::ClusterMutation { mutations, .. } => {
                self.apply_cluster_mutations(mutations);
            }
            _ => {
                tracing::debug!(msg_type = ?msg.msg_type(), "unhandled peer message");
            }
        }
    }
}

