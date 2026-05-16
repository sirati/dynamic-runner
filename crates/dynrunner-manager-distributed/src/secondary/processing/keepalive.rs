//! Per-tick keepalive emission and primary-link failover-threshold
//! re-check.
//!
//! Single concern: send the periodic `Keepalive` broadcast on the
//! keepalive interval, and poll the primary-link failure-window
//! predicate on every keepalive tick so a single `recv-None` event
//! (which permanently pends the recv future) doesn't latch the
//! secondary into "primary healthy" forever.

use std::time::Instant;

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
    /// Tick-driven re-check of the primary-link failover threshold.
    /// Called once per keepalive tick from `process_tasks`. The
    /// recv-None branch only triggers on a NEW recv-None event;
    /// since the bridge architecture turns the recv future permanently
    /// pending after a single None, a single dropped-bridge event would
    /// otherwise never re-evaluate the time axis. This method bridges
    /// the gap by polling `primary_link.should_arm_failover()` on
    /// every tick and arming once the time window elapses.
    ///
    /// Idempotent: harmless when the link is healthy (the predicate
    /// short-circuits on `first_failure_at.is_none()`), and harmless
    /// when failover is already armed (`primary_disconnected` short-
    /// circuits the body so we don't re-backdate `primary_last_seen`).
    /// `is_primary` short-circuits as well — a promoted secondary has
    /// no use for failover.
    pub(in crate::secondary) fn check_primary_link_threshold(&mut self) {
        if self.is_primary {
            return;
        }
        if !self.primary_link.is_link_failing() {
            return;
        }
        if !self.primary_link.should_arm_failover() {
            return;
        }
        // Already-armed: nothing to do — election is in flight.
        // We still want to gate the recv arm if it hasn't been
        // gated yet (first iteration of the time-elapsed branch
        // before any recv-None observation).
        if self.primary_disconnected {
            return;
        }
        let peers = self.peer_transport.peer_count();
        if peers == 0 {
            // The recv-arm None branch handles the no-mesh case via
            // `break`; we shouldn't be reachable here without at
            // least one peer (since the time-axis arming requires
            // a prior recv-None which would have taken the no-peer
            // exit). Defensive: exit cleanly if we somehow are.
            tracing::info!(
                "primary-link threshold breached and no peer mesh; \
                 deferring exit to natural termination path"
            );
            self.primary_disconnected = true;
            return;
        }
        tracing::warn!(
            connected_peers = peers,
            "primary-link failure-window elapsed; arming failover \
             (election will run via peer mesh)"
        );
        self.primary_disconnected = true;
        let backdate = self
            .config
            .keepalive_interval
            .saturating_mul(self.config.keepalive_miss_threshold + 1);
        self.primary_last_seen = Some(
            Instant::now()
                .checked_sub(backdate)
                .unwrap_or_else(Instant::now),
        );
    }

    /// Send keepalive to the current primary and broadcast to peers.
    /// In degraded-mesh mode (`peer_mesh_degraded`) the peer
    /// broadcast is skipped — there's nothing to broadcast to. The
    /// primary→secondary keepalive over WSS still fires so the
    /// primary keeps seeing us as alive.
    pub(in crate::secondary) async fn send_keepalive(&mut self) {
        let active_count = self
            .pool.workers
            .iter()
            .filter(|w| w.current_binary.is_some())
            .count() as u32;
        let msg = DistributedMessage::Keepalive {
            sender_id: self.config.secondary_id.clone(),
            timestamp: timestamp_now(),
            secondary_id: self.config.secondary_id.clone(),
            active_workers: active_count,
        };
        // Send to whoever is currently primary (local at run start;
        // the promoted peer after PromotePrimary).
        let _ = self.send_to_current_primary(msg.clone()).await;
        if self.peer_mesh_degraded {
            return;
        }
        // Broadcast to peers (including the primary if it's a peer —
        // duplicate but idempotent).
        let _ = self.peer_transport.broadcast(msg).await;
    }
}
