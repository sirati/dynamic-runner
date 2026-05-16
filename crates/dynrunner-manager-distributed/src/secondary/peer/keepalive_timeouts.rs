//! Peer keepalive-timeout sweep and in-flight task recovery.
//!
//! Single concern: walk `peer_keepalives` looking for entries staler
//! than `config.peer_timeout`, and for each timed-out peer recover any
//! `primary_in_flight` tasks dispatched to it (the only signal the
//! primary would otherwise get that those binaries are no longer in
//! flight is a `TaskComplete` / `TaskFailed` from a peer that's now
//! gone). Degraded-mesh short-circuit is documented inline.

use dynrunner_core::{Identifier, MessageReceiver, MessageSender};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{DistributedMessage, PeerTransport};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

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

    /// Check for peer timeouts based on keepalive tracking. When this
    /// secondary is the primary, a peer-timeout ALSO recovers
    /// any in-flight tasks dispatched to that peer back into the
    /// pool — without this, the primary_in_flight ledger leaks the
    /// binary forever (the peer will never report TaskComplete /
    /// TaskFailed because it's gone) and the per-phase in_flight
    /// counter stays positive, blocking phase progression.
    /// Non-primary peers don't have a primary_in_flight ledger
    /// to recover, so the recovery path is a no-op for them.
    pub(in crate::secondary) fn check_peer_timeouts(&mut self) {
        // Degraded-mesh skip: with no peer mesh, there's no peer
        // keepalive to time out and no in-flight peer-targeted work
        // to recover. The walk below is a no-op against an empty
        // `peer_keepalives` map anyway — short-circuiting here
        // documents the contract so a future change that buffers
        // peer state pre-connection doesn't accidentally make this
        // matter. See `peer_mesh_degraded` field doc on the
        // SecondaryCoordinator for the full set of guarded paths.
        if self.peer_mesh_degraded {
            return;
        }
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
            // Recover any tasks the primary dispatched to this
            // peer. Walk primary_in_flight, collect hashes whose target
            // matches, then call `recover_in_flight_to_pool` for each
            // (which requeues the binary, decrements in_flight, and
            // clears the ledger entry).
            let recovered: Vec<String> = self
                .primary_in_flight
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
            self.backpressured_secondaries.remove(&peer_id);
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
