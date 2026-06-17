//! The secondary's collection-stats wiring.
//!
//! Single concern: gather one [`CollectionStatsSnapshot`] — each number
//! through its owning structure's read surface (the CRDT inbox fold,
//! the replay buffer the reporting concern owns, the role inbox's
//! queue depth) — and emit the periodic structured line when the
//! cadence policy ([`crate::collection_stats`]) says one is due. The
//! policy (interval, thresholds, breach classification) lives in the
//! crate-level module; this file owns only the secondary's gather +
//! emit edge, driven once per keepalive tick from the operational
//! loop's keepalive arm.

use dynrunner_core::Identifier;
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use crate::collection_stats::CollectionStatsSnapshot;

use super::SecondaryCoordinator;

impl<M, S, E, I> SecondaryCoordinator<M, S, E, I>
where
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    /// Keepalive-tick observation point: emit the periodic
    /// collection-stats line when due (INFO; WARN when any growth
    /// threshold is breached). Cheap off-cadence (one `Instant`
    /// compare); the O(live-entries) inbox fold runs only on the
    /// emitting tick.
    pub(in crate::secondary) fn observe_collection_stats(&mut self) {
        let now = std::time::Instant::now();
        if !self.collection_stats.due(now) {
            return;
        }
        self.collection_stats.mark_emitted(now);
        let snapshot = CollectionStatsSnapshot {
            custom_inbox: self.cluster_state.custom_inbox_stats(),
            replay_buffered: self.pending_report_replays.len(),
            replay_oldest_secs: self
                .pending_report_replays
                .iter()
                .map(|entry| now.duration_since(entry.first_retained_at))
                .max()
                .unwrap_or_default()
                .as_secs(),
            inbox_depth: self.inbox.depth(),
        };
        let breaches = snapshot.breaches();
        if breaches.is_empty() {
            // No threshold breached: a routine periodic heartbeat of the
            // watched collections, non-actionable on its own. Keep it on
            // the forensic-complete file log at TRACE rather than the
            // operator stream — the WARN branch below is the actionable
            // signal an operator must see.
            tracing::trace!(
                custom_unhandled = snapshot.custom_inbox.unhandled,
                custom_terminal = snapshot.custom_inbox.terminal,
                custom_payload_bytes = snapshot.custom_inbox.payload_bytes,
                replay_buffered = snapshot.replay_buffered,
                replay_oldest_secs = snapshot.replay_oldest_secs,
                inbox_depth = snapshot.inbox_depth,
                "collection stats"
            );
        } else {
            tracing::warn!(
                custom_unhandled = snapshot.custom_inbox.unhandled,
                custom_terminal = snapshot.custom_inbox.terminal,
                custom_payload_bytes = snapshot.custom_inbox.payload_bytes,
                replay_buffered = snapshot.replay_buffered,
                replay_oldest_secs = snapshot.replay_oldest_secs,
                inbox_depth = snapshot.inbox_depth,
                breached = ?breaches,
                "collection stats: a watched unbounded collection grew past \
                 its threshold — accumulation in progress (stalled \
                 custom-message compaction / unacked report replays / \
                 starved inbox); memory grows for as long as this persists"
            );
        }
    }
}
