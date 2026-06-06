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

use std::time::Instant;

use dynrunner_core::Identifier;
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use super::super::SecondaryCoordinator;

impl<M, S, E, I> SecondaryCoordinator<M, S, E, I>
where
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
        if self.is_mesh_degraded() {
            return;
        }
        // Monotonic receipt-time comparison: `peer_keepalives` now stores the
        // LOCAL `Instant` at which we last received each peer's keepalive, so
        // staleness is `now.duration_since(last_seen)` on a single monotonic
        // clock. `CLOCK_MONOTONIC` does not accrue host-suspend time, so a
        // coordinated suspend/resume wall-clock jump can no longer make every
        // peer instantly exceed `peer_timeout` and mass-prune the whole mesh
        // (the false-degraded → mass-`fatal_exit` failure this fix closes).
        let now = Instant::now();
        let timeout = self.config.peer_timeout;
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
        // Own the current-primary id before borrowing the operational
        // `peer_keepalives`: `current_primary()` borrows `cluster_state`
        // (a separate field), and the iteration borrows the pool-state
        // via `op_mut()` (a full `&mut self`) — taking the id by value
        // first keeps the two borrows disjoint.
        let current_primary = self.cluster_state.current_primary().map(str::to_owned);
        let op = self.op_mut();
        for (peer_id, last_seen) in &op.peer_keepalives {
            if Some(peer_id.as_str()) == current_primary.as_deref() {
                continue;
            }
            if now.duration_since(*last_seen) > timeout {
                timed_out.push(peer_id.clone());
            }
        }

        for peer_id in timed_out {
            let elapsed = self
                .op_mut()
                .peer_keepalives
                .remove(&peer_id)
                .map(|last_seen| now.duration_since(last_seen).as_secs_f64())
                .unwrap_or_default();
            tracing::warn!(
                peer = %peer_id,
                elapsed,
                "peer keepalive timed out; dropping from liveness view"
            );
        }
    }
}
