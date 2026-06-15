//! Layer 4 coordinator-side wiring for the secondary mesh-consensus FSM.
//!
//! Single concern: the SEAM between
//! [`super::SecondaryConsensusFsm`] (the pure, `Instant`-driven state
//! machine — owner of the prober + responder discipline) and
//! [`crate::secondary::SecondaryCoordinator`] (the operational-loop
//! owner of the egress edge). The FSM never reaches into the
//! coordinator; the coordinator never reaches into the FSM's state —
//! every interaction crosses this thin API.
//!
//! ## Inbound frames
//!
//! The dispatch router (`secondary::dispatch::router::dispatch_message`)
//! has dedicated arms for the four consensus frame variants the
//! secondary handles; each routes through one of these helpers:
//!
//! - [`SecondaryCoordinator::handle_consensus_suspect_peers`] — open
//!   a fresh round / re-key the in-flight round onto a new
//!   `consensus_id`. Observes the primary's epoch on arrival.
//! - [`SecondaryCoordinator::handle_consensus_restart_request`] — the
//!   commit-frame ack. Computes `still_suspicious` / `resolved_since`
//!   off the round's accumulated `resolved` set and ships the
//!   `RestartConfirm` back to the primary.
//! - [`SecondaryCoordinator::handle_consensus_probe`] — stateless
//!   `PeerProbe`/`PeerProbeAck` responder for an inbound probe whose
//!   `probed_id == self`.
//! - [`SecondaryCoordinator::handle_consensus_probe_ack`] — credit a
//!   suspect's reply; emits the matching `ResolvedPeer` to the primary.
//!
//! ## Periodic drive
//!
//! The keepalive arm of `process_tasks` calls
//! [`SecondaryCoordinator::drive_consensus_fsm`] every ~1s so per-target
//! probe deadlines fire on schedule without an additional ticker.
//!
//! ## Output dispatch
//!
//! Every frame the FSM emits leaves with `sender_id` / `timestamp`
//! filled in from `self.config.secondary_id` / `wire::timestamp_now()`
//! and addressed per variant:
//!
//! - `PeerProbe` → [`Destination::Secondary(probed_id)`] (point-to-point
//!   to the suspect, routed via the mesh-pump's role-routing path).
//! - `PeerProbeAck` → [`Destination::Secondary(prober_id)`]
//!   (point-to-point back to the prober).
//! - `ResolvedPeer` / `RestartConfirm` → [`Destination::Primary`]
//!   (the primary tallies; no other peer consumes them).

use std::time::Instant;

use dynrunner_core::Identifier;
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{Destination, DistributedMessage, PeerId};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use super::SecondaryConsensusOutput;

use crate::secondary::SecondaryCoordinator;
use crate::secondary::wire::timestamp_now;

impl<M, S, E, I> SecondaryCoordinator<M, S, E, I>
where
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    /// Inbound `SuspectPeers` arm.
    pub(in crate::secondary) async fn handle_consensus_suspect_peers(
        &mut self,
        consensus_id: u64,
        primary_epoch: u64,
        member_gen: u64,
        suspected: Vec<String>,
    ) {
        // Observe the epoch FIRST (the FSM also observes it inside
        // `apply_suspect_peers`, but `observe_primary_epoch` is documented
        // as the seam Layer 4 calls on EVERY primary frame — the
        // consensus arms are the ones that exercise it most directly).
        self.consensus_fsm.observe_primary_epoch(primary_epoch);
        let now = Instant::now();
        let out = self.consensus_fsm.apply_suspect_peers(
            consensus_id,
            primary_epoch,
            member_gen,
            &suspected,
            now,
        );
        self.dispatch_consensus_output(out).await;
    }

    /// Inbound `RestartRequest` arm.
    pub(in crate::secondary) async fn handle_consensus_restart_request(
        &mut self,
        consensus_id: u64,
        primary_epoch: u64,
        member_gen: u64,
        candidates: Vec<String>,
    ) {
        self.consensus_fsm.observe_primary_epoch(primary_epoch);
        let now = Instant::now();
        let out = self.consensus_fsm.apply_restart_request(
            consensus_id,
            primary_epoch,
            member_gen,
            &candidates,
            now,
        );
        self.dispatch_consensus_output(out).await;
    }

    /// Inbound `PeerProbe` arm (we are the addressee — answer if it is
    /// addressed to us).
    pub(in crate::secondary) async fn handle_consensus_probe(
        &mut self,
        sender_id: &str,
        consensus_id: u64,
        probed_id: &str,
    ) {
        let now = Instant::now();
        let out = self
            .consensus_fsm
            .apply_probe_request(sender_id, consensus_id, probed_id, now);
        self.dispatch_consensus_output(out).await;
    }

    /// Inbound `PeerProbeAck` arm (we are the prober — credit the ack).
    pub(in crate::secondary) async fn handle_consensus_probe_ack(
        &mut self,
        sender_id: &str,
        consensus_id: u64,
        _prober_id: &str,
    ) {
        // The FSM self-filters on `prober_id == self.self_id` is already
        // applied at the wire level (the prober addresses the ack to its
        // own id); the FSM credits the EVIDENCE off `sender_id` (the
        // peer that answered the probe).
        let now = Instant::now();
        let out = self
            .consensus_fsm
            .apply_probe_ack(sender_id, consensus_id, now);
        self.dispatch_consensus_output(out).await;
    }

    /// Periodic drive: called on the keepalive arm of `process_tasks`
    /// so the FSM's per-target probe deadlines fire on schedule. A no-op
    /// in `Idle` (steady-state cost is one `poll(now)` per ~1s).
    pub(in crate::secondary) async fn drive_consensus_fsm(&mut self) {
        let now = Instant::now();
        let out = self.consensus_fsm.poll(now);
        self.dispatch_consensus_output(out).await;
    }

    /// Stamp + address the FSM's emitted frames and ship them. Single
    /// owner of the consensus-side egress (the dispatch arms and the
    /// periodic drive all funnel through here so the addressing rules
    /// stay in one place).
    async fn dispatch_consensus_output(&mut self, out: SecondaryConsensusOutput<I>) {
        let SecondaryConsensusOutput::EmitFrames(frames) = out else {
            return;
        };
        let sender_id = self.config.secondary_id.clone();
        for boxed in frames {
            let ts = timestamp_now();
            let (dst, frame) = match *boxed {
                DistributedMessage::PeerProbe {
                    consensus_id,
                    probed_id,
                    ..
                } => (
                    Destination::Secondary(PeerId::from(probed_id.clone())),
                    DistributedMessage::PeerProbe {
                        target: None,
                        sender_id: sender_id.clone(),
                        timestamp: ts,
                        consensus_id,
                        probed_id,
                    },
                ),
                DistributedMessage::PeerProbeAck {
                    consensus_id,
                    prober_id,
                    ..
                } => (
                    Destination::Secondary(PeerId::from(prober_id.clone())),
                    DistributedMessage::PeerProbeAck {
                        target: None,
                        sender_id: sender_id.clone(),
                        timestamp: ts,
                        consensus_id,
                        prober_id,
                    },
                ),
                DistributedMessage::ResolvedPeer {
                    consensus_id,
                    observer_id,
                    resolved,
                    ..
                } => (
                    Destination::Primary,
                    DistributedMessage::ResolvedPeer {
                        target: None,
                        sender_id: sender_id.clone(),
                        timestamp: ts,
                        consensus_id,
                        observer_id,
                        resolved,
                    },
                ),
                DistributedMessage::RestartConfirm {
                    consensus_id,
                    responder_id,
                    still_suspicious,
                    resolved_since,
                    ..
                } => (
                    Destination::Primary,
                    DistributedMessage::RestartConfirm {
                        target: None,
                        sender_id: sender_id.clone(),
                        timestamp: ts,
                        consensus_id,
                        responder_id,
                        still_suspicious,
                        resolved_since,
                    },
                ),
                other => {
                    tracing::error!(
                        msg_type = ?other.msg_type(),
                        "#556 secondary consensus FSM emitted a non-secondary-emitted \
                         frame variant — dropping; this is a Layer 3 bug"
                    );
                    continue;
                }
            };
            if let Err(e) = self.send_to(dst, frame).await {
                tracing::warn!(
                    target: "dynrunner_consensus",
                    error = %e,
                    "#556 secondary consensus frame egress failed; the next \
                     poll-tick (or the primary's round retry) will re-fire"
                );
            }
        }
    }
}
