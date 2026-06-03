//! Peer keepalive-timeout sweep.
//!
//! Single concern: walk `peer_keepalives` looking for entries staler
//! than `config.peer_timeout` and drop them, so this node's view of
//! which peers are alive stays current (consulted by the failover
//! election's liveness reasoning). This is per-node peer-liveness
//! tracking, NOT authority: the secondary never holds an in-flight
//! ledger to recover, so a dead peer's in-flight work is reclaimed
//! ENTIRELY by the authority's `PrimaryCoordinator::recover_inflight_for_dead_secondary`
//! (the single canonical owner of that recovery). Degraded-mesh
//! short-circuit is documented inline.

use dynrunner_core::Identifier;
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::PeerTransport;
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
    /// Sweep stale peer keepalives so the failover election's liveness
    /// view is current. Pure per-node peer-liveness tracking — the
    /// secondary holds no authority and no in-flight ledger, so a
    /// timed-out peer's outstanding work is reclaimed by the authority
    /// (`PrimaryCoordinator::recover_inflight_for_dead_secondary`), not
    /// here.
    pub(in crate::secondary) fn check_peer_timeouts(&mut self) {
        // Degraded-mesh skip: with no peer mesh, there's no peer
        // keepalive to time out. The walk below is a no-op against an
        // empty `peer_keepalives` map anyway — short-circuiting here
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

        // The current primary is NOT a peer for liveness purposes. Its
        // liveness is tracked SOLELY via `primary_last_seen` (refreshed by
        // the A-M0a recognition path in `handle_inbound`, and judged by
        // `run_election_tick`'s `primary_silent`). A just-promoted peer
        // may still carry a stale PRE-promotion `peer_keepalives` entry
        // (its mesh keepalives stopped feeding that map the moment it
        // became `current_primary`); without this skip that stale entry
        // would trip a spurious timeout WARN and prune the entry of an
        // ALIVE primary — a peer-removal of the node we depend on. Reading
        // `current_primary` is the single source of "who is primary now".
        let current_primary = self.cluster_state.current_primary();
        for (peer_id, last_seen) in &self.peer_keepalives {
            if Some(peer_id.as_str()) == current_primary {
                continue;
            }
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
                "peer keepalive timed out; dropping from liveness view"
            );
        }
    }
}
