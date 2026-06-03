//! Per-tick keepalive emission and primary-link failover-threshold
//! re-check.
//!
//! Single concern: send the periodic `Keepalive` broadcast on the
//! keepalive interval, and poll the primary-link failure-window
//! predicate on every keepalive tick so a single `recv-None` event
//! (which permanently pends the recv future) doesn't latch the
//! secondary into "primary healthy" forever.

use std::time::Instant;

use dynrunner_core::Identifier;
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{
    Destination, DistributedMessage, KeepaliveRole, PeerTransport,
};
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
    /// Tick-driven re-check of the primary-link failover threshold —
    /// the TIME axis of the failover-health window.
    ///
    /// The failover-health window is opened by the send-side no-route
    /// probe in [`Self::send_to_primary`]: when a primary-bound send
    /// returns a no-route `Err` (no peer in the mesh resolves the
    /// `Primary` destination), `record_recv_failure` anchors the window
    /// and bumps the count axis. The COUNT axis can saturate (e.g. an idle worker's
    /// backoff suppresses further `TaskRequest` sends, so no further
    /// probes accrue); this method covers the TIME axis by polling
    /// `should_arm_failover()` on every keepalive tick and backdating
    /// `primary_last_seen` once the window has elapsed, so the next
    /// `run_election_tick` enters Suspecting.
    ///
    /// Transport-agnostic: it reads only the primary-link health
    /// predicate — never `peer_count()`, never an uplink-close branch.
    /// The degraded-mesh guard lives in `run_election_tick`
    /// (`peer_mesh_degraded`), so this method need not duplicate it.
    ///
    /// Idempotent: short-circuits when the link is healthy
    /// (`first_failure_at.is_none()`); backdating to a fixed past
    /// instant is a no-op on repeat (same value re-stored).
    pub(in crate::secondary) fn check_primary_link_threshold(&mut self) {
        let op = self.op_mut();
        if !op.primary_link.is_link_failing() {
            return;
        }
        if !op.primary_link.should_arm_failover() {
            return;
        }
        tracing::warn!(
            "primary-link failure-window elapsed; arming failover \
             (election runs via the peer mesh — see run_election_tick's \
             degraded-mesh guard)"
        );
        let backdate = self
            .config
            .keepalive_interval
            .saturating_mul(self.config.keepalive_miss_threshold + 1);
        self.op_mut().primary_last_seen = Some(
            Instant::now()
                .checked_sub(backdate)
                .unwrap_or_else(Instant::now),
        );
    }

    /// Fan one keepalive out to the whole cluster on the keepalive tick.
    ///
    /// Post-fold the primary is just another peer in the one mesh, so a
    /// single `Destination::All` fan-out reaches EVERY peer — the primary
    /// included — EXACTLY ONCE (architecture invariant #5). There is no
    /// separate primary-unicast leg: the old "two distinct liveness
    /// targets" (primary-link unicast + peer broadcast) collapsed into the
    /// one mesh when the submitter primary became a first-class peer, and
    /// keeping both would double-deliver to the now-meshed primary. The
    /// degraded-mesh early-return is likewise gone: the primary is a
    /// member of the broadcast set regardless of the role-aware degraded
    /// latch (the `Real` arm still holds the folded primary in its
    /// `connections`; the firewalled arm routes `All` to the sole folded
    /// member), so skipping the broadcast when degraded would starve the
    /// primary of keepalives and trip a false primary-death.
    ///
    /// Strict-observer suppression: a pure observer (`config.is_observer`)
    /// originates NOTHING — keepalive included. A keepalive is a "my
    /// liveness matters to cluster timing" assertion; an observer is a
    /// passive bystander with zero authority and is filtered out of
    /// every election candidate set, so its silence drives no decision
    /// and its keepalive would only add noise to peers'
    /// `peer_keepalives` maps. This is the keepalive concern's own
    /// single role-gate — there is no scattered `is_observer` branch
    /// elsewhere on the emission path. The resource-holdings announcer
    /// (a SEPARATE opt-in resource-provider capability) is the only
    /// thing an observer-mode coordinator ever broadcasts, and only
    /// when a caller explicitly attaches it.
    pub(in crate::secondary) async fn send_keepalive(&mut self) {
        if self.config.is_observer {
            return;
        }
        let active_count = self
            .op_mut()
            .pool
            .workers
            .iter()
            .filter(|w| w.current_binary.is_some())
            .count() as u32;
        let msg = DistributedMessage::Keepalive {
            sender_id: self.config.secondary_id.clone(),
            timestamp: timestamp_now(),
            secondary_id: self.config.secondary_id.clone(),
            active_workers: active_count,
            emitter_role: KeepaliveRole::Secondary,
        };
        // ONE fan-out reaches every peer exactly once: the primary
        // (a first-class mesh member post-fold) refreshes its view of
        // this node's liveness, and every other secondary refreshes its
        // `peer_keepalives` entry — both from the single broadcast.
        let _ = self.send_to(Destination::All, msg).await;
    }
}
