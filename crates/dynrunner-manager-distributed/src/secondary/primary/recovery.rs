//! Primary-side dispatch-failure recovery primitives.
//!
//! Single concern: undo a primary dispatch that didn't reach a worker
//! (self-assign race, peer rejection, peer-side route lost), apply
//! per-peer backpressure backoff on "No idle worker available"
//! rejections, and clear that backoff when the peer demonstrates
//! liveness via a successful `TaskComplete`. The methods here are
//! the only callers of `pool.requeue` on the primary side.

use std::time::{Duration, Instant};

use dynrunner_core::{Identifier, MessageReceiver, MessageSender};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{DistributedMessage, PeerTransport};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use super::super::SecondaryCoordinator;

/// Backpressure backoff window applied to a peer that just rejected a
/// `TaskAssignment` with "No idle worker available". Mirrors the
/// 500ms window used by the regular primary
/// (`PrimaryCoordinator::handle_task_failed`); a single constant
/// keeps the two paths in lockstep so promoted runs feel the
/// same as live-primary runs.
const PRIMARY_BACKPRESSURE_WINDOW: Duration = Duration::from_millis(500);

impl<PT, P, M, S, E, I> SecondaryCoordinator<PT, P, M, S, E, I>
where
    PT: MessageSender<DistributedMessage<I>> + MessageReceiver<DistributedMessage<I>>,
    P: PeerTransport<I>,
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    /// Undo a primary dispatch that didn't reach a worker
    /// (self-assign race, peer rejected, peer-side route lost). Removes
    /// the `primary_in_flight` entry, re-queues the binary at the front
    /// of its bucket via `pool.requeue` (which also decrements the
    /// per-phase in_flight counter), and clears the `active_tasks`
    /// entry if any was created. No-op if the hash isn't tracked
    /// (idempotent — peer-broadcast TaskFailed and primary-forwarded
    /// TaskFailed both arrive on the primary, and either may
    /// race the other).
    pub(in crate::secondary) fn recover_in_flight_to_pool(&mut self, file_hash: &str) {
        let item = match self.primary_in_flight.remove(file_hash) {
            Some(item) => item,
            None => return,
        };
        // `active_tasks` was inserted only on the self-assign success
        // path; remove unconditionally to keep its set in sync (no-op
        // if the hash wasn't there).
        self.active_tasks.remove(file_hash);
        if let Some(pool) = self.primary_pending.as_mut() {
            pool.requeue(item.binary);
        }
    }

    /// Apply the primary side of a peer rejection: extract the
    /// binary back to the pool and put the peer in a backoff window
    /// so the next `handle_primary_task_request` from it skips dispatch.
    /// Returns the `target_secondary_id` that was backpressured (or
    /// `None` if the hash wasn't in flight, e.g. the peer rejection
    /// arrived after a successful retry path completed it).
    pub(in crate::secondary) fn handle_primary_peer_rejection(&mut self, file_hash: &str) -> Option<String> {
        let item = self.primary_in_flight.remove(file_hash)?;
        let target = item.target_secondary_id.clone();
        self.active_tasks.remove(file_hash);
        if let Some(pool) = self.primary_pending.as_mut() {
            pool.requeue(item.binary);
        }
        self.backpressured_secondaries.insert(
            target.clone(),
            Instant::now() + PRIMARY_BACKPRESSURE_WINDOW,
        );
        Some(target)
    }

    /// Clear backpressure backoff for a peer that just reported a
    /// successful TaskComplete (proves the peer is healthy and
    /// accepting work). Called from the TaskComplete handlers in
    /// `dispatch.rs` and `peer.rs`. Mirrors the regular primary's
    /// backpressure clear on TaskComplete.
    pub(in crate::secondary) fn clear_primary_peer_backpressure(&mut self, secondary_id: &str) {
        self.backpressured_secondaries.remove(secondary_id);
    }
}
