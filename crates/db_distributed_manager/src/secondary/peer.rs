
use db_comm_api_base::Identifier;
use db_manager_runner_comm::ManagerEndpoint;
use db_primary_secondary_comm::{
    DistributedMessage, PeerTransport, PrimaryTransport,
};
use db_scheduler_api::{ResourceEstimator, Scheduler};


use super::SecondaryCoordinator;
use super::wire::timestamp_now;

impl<PT, P, M, S, E, I> SecondaryCoordinator<PT, P, M, S, E, I>
where
    PT: PrimaryTransport<I>,
    P: PeerTransport<I>,
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator + Clone,
    I: Identifier,
{
    pub(super) fn handle_peer_message(&mut self, msg: DistributedMessage<I>) {
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
                ..
            } => {
                tracing::debug!(
                    peer = %secondary_id,
                    task_hash,
                    error_type,
                    "peer task failed"
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
                self.record_promotion_confirm(sender_id, new_primary_id, vote_round);
            }
            _ => {
                tracing::debug!(msg_type = ?msg.msg_type(), "unhandled peer message");
            }
        }
    }

    /// Check for peer timeouts based on keepalive tracking.
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
            tracing::warn!(
                peer = %peer_id,
                last_seen,
                elapsed = now - last_seen,
                "peer timeout detected"
            );
        }
    }
}
