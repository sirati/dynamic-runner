
use dynrunner_core::Identifier;
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{
    DistributedMessage, PeerTransport, PrimaryTransport,
};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};


use super::SecondaryCoordinator;
use super::wire::timestamp_now;

impl<PT, P, M, S, E, I> SecondaryCoordinator<PT, P, M, S, E, I>
where
    PT: PrimaryTransport<I>,
    P: PeerTransport<I>,
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    pub(super) async fn handle_peer_message(&mut self, msg: DistributedMessage<I>) {
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
                task_hash,
                ..
            } => {
                // Track peer's completed task to avoid duplicate processing
                self.completed_tasks.insert(task_hash.clone());
                // A successful TaskComplete from this peer proves it's
                // healthy — clear any SLURM-primary backpressure
                // backoff so the next dispatch cycle can re-target it.
                // Mirrors regular primary's TaskComplete handler.
                self.clear_slurm_peer_backpressure(&secondary_id);
                // Drive the SLURM-primary's phase machine: if this
                // node dispatched the task as SLURM-primary, the
                // peer's completion message is the only signal the
                // pool gets that the item is no longer in flight.
                self.note_slurm_item_completed(&task_hash);
                tracing::debug!(
                    peer = %secondary_id,
                    task_hash,
                    "peer task complete"
                );
            }
            DistributedMessage::TaskFailed {
                secondary_id,
                task_hash,
                error_type,
                error_message,
                ..
            } => {
                // Two TaskFailed shapes arrive on the SLURM-primary
                // path:
                //   1. Backpressure rejection — peer's dispatch.rs
                //      sends `Recoverable / "No idle worker
                //      available"` when its worker pool can't accept
                //      the assignment. The task NEVER ran; the
                //      binary must be returned to the pool, the
                //      peer marked backpressured. Drives
                //      `handle_slurm_peer_rejection` (re-queue +
                //      backoff). Skipping it would leak the binary
                //      from `slurm_in_flight` and stall the
                //      per-phase in_flight counter.
                //   2. Terminal failure — peer's worker actually ran
                //      the binary and reported failure (Recoverable
                //      from the worker, NonRecoverable, OutOfMemory,
                //      etc.). The phase machine just needs the
                //      in-flight counter decremented.
                let is_backpressure = error_type == "Recoverable"
                    && error_message == "No idle worker available";
                if is_backpressure {
                    if let Some(peer) = self.handle_slurm_peer_rejection(&task_hash) {
                        tracing::debug!(
                            peer = %peer,
                            task_hash,
                            "peer rejected SLURM-primary assignment; re-queued + backpressure backoff applied"
                        );
                    }
                } else {
                    self.note_slurm_item_completed(&task_hash);
                    tracing::debug!(
                        peer = %secondary_id,
                        task_hash,
                        error_type,
                        "peer task failed"
                    );
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
            } if self.is_slurm_primary => {
                // Peer routed this to us because we won the election. Same
                // dispatch path that the live-primary case uses, just
                // arriving over peer_transport instead of primary_transport.
                let available_memory = available_resources
                    .iter()
                    .find(|r| r.kind == dynrunner_core::ResourceKind::memory())
                    .map(|r| r.amount)
                    .unwrap_or(0);
                if let Err(e) = self
                    .handle_slurm_task_request(secondary_id, worker_id, available_memory)
                    .await
                {
                    tracing::warn!(error = %e, "post-promotion peer TaskRequest dispatch failed");
                }
            }
            _ => {
                tracing::debug!(msg_type = ?msg.msg_type(), "unhandled peer message");
            }
        }
    }

    /// One-shot watchdog: 30s after `connect_to_peers` fired with a
    /// non-empty peer list, declare full mesh failure if zero peers
    /// connected. Self-healing if the mesh forms before the deadline
    /// (`peer_count() > 0` suppresses the fault) or partially forms
    /// after the deadline (any incoming peer connection clears
    /// `peer_mesh_check_at`, no fault).
    ///
    /// On a confirmed full-mesh failure this is HARD ERROR: the
    /// framework relies on the peer mesh for failover, post-promotion
    /// routing, and broadcasts. With zero peers, a SLURM-promoted
    /// node wedges in a tokio busy-poll (dataset peer reported 86%
    /// CPU + 54k epoll_wait/sec on a stuck SLURM-primary). The
    /// previous warn-and-continue behaviour silently degraded the
    /// run instead of failing fast. We now:
    ///   1. Send a `SecondaryFatalError` to the current primary so
    ///      it can drop us from the routable set + requeue any
    ///      in-flight tasks immediately (instead of waiting for the
    ///      keepalive miss threshold).
    ///   2. Set `self.fatal_exit` to a human-readable reason. The
    ///      main loop in `process_tasks` checks this after the
    ///      select! body and returns `Err(reason)`, so the secondary
    ///      process exits non-zero.
    ///
    /// `peer_count()` already calls `drain_new_connections` so this
    /// reads the freshest state.
    pub(super) async fn check_peer_mesh_watchdog(&mut self) {
        let deadline = match self.peer_mesh_check_at {
            Some(d) => d,
            None => return,
        };
        // peer_count drains new connections internally; calling it
        // BEFORE the deadline check lets a fresh connection clear
        // the watchdog without firing the fault.
        let connected = self.peer_transport.peer_count();
        if connected > 0 {
            self.peer_mesh_check_at = None;
            return;
        }
        if std::time::Instant::now() < deadline {
            return;
        }
        // Latch the watchdog off first so we never re-fire even if
        // the fatal-error send takes a few ticks to flush.
        self.peer_mesh_check_at = None;

        let error = format!(
            "peer mesh fully failed to form: 0 of {} peers reachable; \
             cluster routing impossible",
            self.peer_dial_count
        );
        tracing::error!(
            attempted = self.peer_dial_count,
            connected = 0,
            "{error}; reporting fatal error to primary and exiting"
        );

        let fatal = DistributedMessage::<I>::SecondaryFatalError {
            sender_id: self.config.secondary_id.clone(),
            timestamp: timestamp_now(),
            secondary_id: self.config.secondary_id.clone(),
            error: error.clone(),
        };
        // Best-effort: even if the send fails (primary already gone,
        // transport closed, ...) we still want to exit. Logging the
        // send-side error is enough — the operator will see both the
        // fault line above AND the send failure if any.
        if let Err(e) = self.send_to_current_primary(fatal).await {
            tracing::warn!(
                error = %e,
                "failed to deliver SecondaryFatalError to primary; exiting anyway"
            );
        }

        self.fatal_exit = Some(error);
    }

    /// Check for peer timeouts based on keepalive tracking. When this
    /// secondary is the SLURM-primary, a peer-timeout ALSO recovers
    /// any in-flight tasks dispatched to that peer back into the
    /// pool — without this, the slurm_in_flight ledger leaks the
    /// binary forever (the peer will never report TaskComplete /
    /// TaskFailed because it's gone) and the per-phase in_flight
    /// counter stays positive, blocking phase progression.
    /// Non-SLURM-primary peers don't have a slurm_in_flight ledger
    /// to recover, so the recovery path is a no-op for them.
    pub(super) fn check_peer_timeouts(&mut self) {
        let now = timestamp_now();
        let timeout_secs = self.config.peer_timeout.as_secs_f64();
        let mut timed_out = Vec::new();

        for (peer_id, last_seen) in &self.peer_keepalives {
            if now - last_seen > timeout_secs {
                timed_out.push(peer_id.clone());
            }
        }

        for peer_id in timed_out {
            let last_seen = self.peer_keepalives.remove(&peer_id).unwrap_or(0.0);
            // Recover any tasks the SLURM-primary dispatched to this
            // peer. Walk slurm_in_flight, collect hashes whose target
            // matches, then call `recover_in_flight_to_pool` for each
            // (which requeues the binary, decrements in_flight, and
            // clears the ledger entry).
            let recovered: Vec<String> = self
                .slurm_in_flight
                .iter()
                .filter(|(_, item)| item.target_secondary_id == peer_id)
                .map(|(hash, _)| hash.clone())
                .collect();
            let recovered_count = recovered.len();
            for hash in recovered {
                self.recover_in_flight_to_pool(&hash);
            }
            // Drop the peer's backpressure entry too — once it's
            // declared dead the backoff is meaningless.
            self.slurm_backpressured_peers.remove(&peer_id);
            tracing::warn!(
                peer = %peer_id,
                last_seen,
                elapsed = now - last_seen,
                recovered_in_flight = recovered_count,
                "peer timeout detected; recovered in-flight tasks dispatched to it"
            );
        }
    }
}
