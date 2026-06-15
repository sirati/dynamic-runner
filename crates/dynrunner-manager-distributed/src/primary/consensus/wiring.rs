//! Layer 4 coordinator-side wiring for the primary mesh-consensus FSM.
//!
//! Single concern: the SEAM between [`super::ConsensusFsm`] (the pure,
//! `Instant`-driven state machine — owner of the WHEN-to-mesh-declare-
//! dead decision) and [`crate::primary::PrimaryCoordinator`] (the
//! operational-loop owner of liveness state, the egress edge, and the
//! destructive death primitives). The FSM never reaches into the
//! coordinator; the coordinator never reaches into the FSM's state —
//! every interaction crosses this thin API:
//!
//! - **Inbound frames** ([`PrimaryCoordinator::apply_consensus_resolved`],
//!   [`PrimaryCoordinator::apply_consensus_confirm`]): the dispatcher
//!   (`primary::connect::dispatch_message`) routes
//!   `ResolvedPeer`/`RestartConfirm` arms through here. Each call hands
//!   the FSM the tally update and then drains its outputs.
//! - **Local-suspect seed** ([`PrimaryCoordinator::set_consensus_scheduling_suspect`]):
//!   both the hard-backstop sweep (in
//!   `primary::heartbeat::decide_dead_secondaries`) and the lazy
//!   dispatch-altitude path (in `primary::lifecycle::worker_mgmt::
//!   maybe_requeue_silent_held_work`) seed the FSM's local
//!   scheduling-suspect with the current silent set. The lazy path
//!   STOPS there (no escalate); the hard backstop additionally calls
//!   [`PrimaryCoordinator::consensus_escalate`].
//! - **Drive** ([`PrimaryCoordinator::drive_consensus_fsm`]): the
//!   heartbeat tick (`process_heartbeat_tick`'s ~5s cadence) calls this
//!   once per tick. It builds a fresh [`super::MeshSnapshot`] off the
//!   coordinator's live secondary count, polls the FSM, and consumes
//!   every output until the FSM returns `Idle` for the tick.
//!
//! ## Output dispatch
//!
//! On [`super::ConsensusOutput::EmitFrame`] the wiring stamps `sender_id`
//! / `timestamp` from the coordinator's own config and addresses the
//! frame: `SuspectPeers`/`RestartRequest` go `Destination::All` (the
//! existing per-receiver fan-out path); the FSM is the SOLE emitter of
//! either variant, the dispatch layer never crafts them directly.
//!
//! On [`super::ConsensusOutput::Restart`] the wiring calls
//! [`PrimaryCoordinator::declare_secondaries_confirmed_dead`] — the
//! wrapper around the original destructive
//! [`PrimaryCoordinator::requeue_dead_secondary`] primitive — and
//! dispatches one [`crate::primary::respawn::types::RespawnRequest`] per
//! target via [`PrimaryCoordinator::dispatch_respawn_request`]. Layer 4
//! does NOT yet add scancel to this path — that is Layer 5's load-bearing
//! addition, inside the existing respawn pipeline.
//!
//! On [`super::ConsensusOutput::Abort`] the wiring logs at WARN and
//! returns the FSM to `Idle`; nothing destructive fires. The same WARN
//! channel reports `DropSuspicion` at INFO.

use std::collections::BTreeSet;
use std::time::Instant;

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{Destination, DistributedMessage, RemovalCause};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use super::{ConsensusOutput, MeshSnapshot};

use crate::primary::PrimaryCoordinator;
use crate::primary::heartbeat::DeadSecondary;
use crate::primary::respawn::RespawnRequest;
use crate::primary::wire::timestamp_now;

impl<S, E, I> PrimaryCoordinator<S, E, I>
where
    S: Scheduler<I>,
    E: ResourceEstimator<I>,
    I: Identifier,
{
    /// Build the side-gate mesh snapshot off the coordinator's current
    /// view of the fleet. The FSM consults this on every `poll` at
    /// `BroadcastRestart` entry; see [`super`]'s Q1 contract for why
    /// the threshold mirrors the secondary-side `failover_quorum`.
    ///
    /// `total_known_members` is `self.secondaries.len() + 1` (the
    /// primary's own host counts toward the failover-quorum
    /// denominator — the same shape the secondary's
    /// `failover_quorum_peer_count` uses). `live_peer_count` is the
    /// non-suspected subset of `self.secondaries` (the candidate
    /// confirmers — peers we can address with a `RestartRequest` and
    /// expect a `RestartConfirm` back from). The FSM owns the suspect
    /// set; the snapshot just needs the totals.
    pub(in crate::primary) fn consensus_mesh_snapshot(&self) -> MeshSnapshot {
        // The FSM holds the in-flight suspect set; exclude it from the
        // live count. A peer not in any FSM-tracked set is "non-suspected".
        let suspect_set = self.consensus_fsm.in_flight_suspects();
        let live_peer_count = self
            .secondaries
            .keys()
            .filter(|id| !suspect_set.contains(id.as_str()))
            .count();
        let total_known_members = self.secondaries.len() + 1;
        MeshSnapshot {
            live_peer_count,
            total_known_members,
        }
    }

    /// Local scheduling-suspect seed. BOTH paths into the FSM go
    /// through this (the hard-backstop and the lazy dispatch path);
    /// only the hard-backstop calls [`Self::consensus_escalate`]
    /// afterwards. Replacing the FSM's prior suspect set with `set`
    /// matches [`super::ConsensusFsm::set_scheduling_suspect`]'s
    /// own idempotency contract; the FSM no-ops if it is already in
    /// an in-flight round (the caller must wait for round termination
    /// before re-seeding).
    pub(in crate::primary) fn set_consensus_scheduling_suspect(&mut self, set: BTreeSet<String>) {
        self.consensus_fsm.set_scheduling_suspect(set);
    }

    /// Escalate the FSM's local scheduling-suspect set into a consensus
    /// round. Reads `primary_epoch` and the per-peer `member_gen` off
    /// the cluster state; derives `expected_responders` as
    /// `self.secondaries.keys() \ suspect_ids` — the non-suspected
    /// fleet, the only valid set of `RestartConfirm` responders.
    ///
    /// Drives the FSM forward through one `poll` so the opening
    /// `SuspectPeers` frame goes out inline; subsequent rounds run on
    /// the heartbeat tick cadence via [`Self::drive_consensus_fsm`].
    pub(in crate::primary) async fn consensus_escalate(&mut self, suspect_ids: BTreeSet<String>) {
        self.consensus_escalate_at(suspect_ids, Instant::now()).await;
    }

    /// Same shape as [`Self::consensus_escalate`] but with a
    /// caller-supplied `now`. Tests pin the round-start instant so the
    /// FSM's resolution + confirmation deadlines are computed off a
    /// deterministic baseline. Production callers always use
    /// [`Self::consensus_escalate`].
    pub(in crate::primary) async fn consensus_escalate_at(
        &mut self,
        suspect_ids: BTreeSet<String>,
        now: Instant,
    ) {
        let primary_epoch = self.cluster_state.primary_epoch();
        // `member_gen` on the wire is per-frame, but the FSM stamps ONE
        // gen per round (round-id-locked, see Layer 2). Use the
        // primary's highest known `peer_member_gen` across the suspect
        // set — the round is judging THOSE peers, so the gen they were
        // last seen alive at is the correct round-stamp. Fall back to 0
        // on a never-seen id (consistent with `peer_member_gen`).
        let member_gen = suspect_ids
            .iter()
            .map(|id| self.cluster_state.peer_member_gen(id))
            .max()
            .unwrap_or(0);
        // Expected responders: non-suspected secondaries this primary
        // can address. The FSM does not compute this itself (see Layer 2
        // doc): the caller owns the roster intersection.
        let expected_responders: BTreeSet<String> = self
            .secondaries
            .keys()
            .filter(|id| !suspect_ids.contains(id.as_str()))
            .cloned()
            .collect();
        tracing::warn!(
            target: "dynrunner_consensus",
            primary_epoch,
            member_gen,
            suspects = ?suspect_ids,
            expected_responders = ?expected_responders,
            "#556 escalating local scheduling-suspect to mesh consensus \
             (hard-backstop fired; mesh-declare requires consensus)"
        );
        self.consensus_fsm
            .escalate(now, primary_epoch, member_gen, expected_responders);
        // Drive the FSM forward one tick so the opening `SuspectPeers`
        // emits inline (rather than waiting for the next heartbeat
        // tick). Subsequent rounds run off the periodic drive. Pin the
        // drive tick to the same `now` we stamped on `escalate` so the
        // test-deterministic-tick path is byte-symmetric with production.
        self.drive_consensus_fsm_at(now).await;
    }

    /// Apply a `ResolvedPeer` echo from a non-suspected secondary.
    /// Routed here by `primary::connect::dispatch_message`'s
    /// `DistributedMessage::ResolvedPeer` arm.
    pub(in crate::primary) async fn apply_consensus_resolved(
        &mut self,
        consensus_id: u64,
        observer_id: &str,
        resolved_peer: &str,
    ) {
        self.consensus_fsm
            .apply_resolved(consensus_id, observer_id, resolved_peer);
        // A `ResolvedPeer` may drop the set to empty, which converts the
        // resolution-deadline transition into an immediate `DropSuspicion`
        // — drive the FSM so the terminal is observed this tick.
        self.drive_consensus_fsm().await;
    }

    /// Apply a `RestartConfirm` reply from a non-suspected secondary.
    /// Routed here by `primary::connect::dispatch_message`'s
    /// `DistributedMessage::RestartConfirm` arm.
    pub(in crate::primary) async fn apply_consensus_confirm(
        &mut self,
        consensus_id: u64,
        responder_id: &str,
        still_suspicious: Vec<String>,
        resolved_since: Vec<String>,
    ) {
        self.consensus_fsm.apply_confirm(
            consensus_id,
            responder_id,
            still_suspicious,
            resolved_since,
        );
        // All-responded computes the intersection and may commit
        // `Restart` this tick — drive to surface the terminal.
        self.drive_consensus_fsm().await;
    }

    /// Drive the FSM one tick. Reads `now`, builds the mesh snapshot,
    /// polls, and dispatches every output until the FSM idles.
    /// Heartbeat-tick cadence is the natural drive site; the inbound
    /// frame arms call it inline so a same-tick all-responded /
    /// resolution-empty terminal does not wait a full tick.
    pub(in crate::primary) async fn drive_consensus_fsm(&mut self) {
        self.drive_consensus_fsm_at(Instant::now()).await;
    }

    /// Same shape as [`Self::drive_consensus_fsm`] but with a
    /// caller-supplied `now`. Tests inject a deterministic `Instant`
    /// (`t0 + RESOLUTION_DEADLINE`, etc.) so the FSM's deadline
    /// transitions fire without waiting on real wall-clock time;
    /// production code always uses [`Self::drive_consensus_fsm`] which
    /// reads `Instant::now()`.
    pub(in crate::primary) async fn drive_consensus_fsm_at(&mut self, now: Instant) {
        // The FSM is bounded — at most one `EmitFrame` per state edge,
        // and terminals reset to `Idle` — so this loop runs ≤ a small
        // constant per call (open-suspect emit, possible immediate
        // resolution-empty drop, possible side-gate abort, possible
        // restart commit). A defensive cap pins the loop in case a
        // future refactor relaxes that invariant.
        for _ in 0..8 {
            let snapshot = self.consensus_mesh_snapshot();
            let output = self.consensus_fsm.poll(now, &snapshot);
            match output {
                ConsensusOutput::Idle => return,
                ConsensusOutput::EmitFrame(frame) => {
                    self.dispatch_consensus_frame(*frame).await;
                }
                ConsensusOutput::DropSuspicion => {
                    tracing::info!(
                        target: "dynrunner_consensus",
                        "#556 consensus round resolved with no remaining suspects \
                         (DropSuspicion): no peer is mesh-declared dead this round"
                    );
                }
                ConsensusOutput::Restart { targets } => {
                    self.commit_consensus_restart(targets).await;
                }
                ConsensusOutput::Abort { reason } => {
                    tracing::warn!(
                        target: "dynrunner_consensus",
                        reason = %reason,
                        "#556 consensus round ABORTED — no peer is mesh-declared \
                         dead this round (side-gate failure or responder timeout \
                         after retry); next hard-backstop sweep will re-evaluate"
                    );
                }
            }
        }
        // Defensive: if we burnt the loop budget the FSM is in a state
        // it expected to emit-and-return on, but it kept handing us
        // frames. Log loudly so a regression is visible.
        tracing::warn!(
            target: "dynrunner_consensus",
            "#556 consensus drive loop reached defensive cap (8 iterations); \
             FSM may be misbehaving — next heartbeat tick will continue"
        );
    }

    /// Stamp `sender_id` + `timestamp` on a consensus frame the FSM
    /// emitted (the FSM leaves them blank; the wiring layer fills them
    /// from `self.config.node_id` and the wire timestamp). Addresses
    /// `SuspectPeers`/`RestartRequest` to `Destination::All` (broadcast
    /// to every connected peer); the FSM emits no other variants from
    /// the primary side.
    async fn dispatch_consensus_frame(&mut self, frame: DistributedMessage<I>) {
        let sender_id = self.config.node_id.clone();
        let ts = timestamp_now();
        let (dst, frame) = match frame {
            DistributedMessage::SuspectPeers {
                consensus_id,
                primary_epoch,
                member_gen,
                suspected,
                ..
            } => (
                Destination::All,
                DistributedMessage::SuspectPeers {
                    target: None,
                    sender_id,
                    timestamp: ts,
                    consensus_id,
                    primary_epoch,
                    member_gen,
                    suspected,
                },
            ),
            DistributedMessage::RestartRequest {
                consensus_id,
                primary_epoch,
                member_gen,
                candidates,
                ..
            } => (
                Destination::All,
                DistributedMessage::RestartRequest {
                    target: None,
                    sender_id,
                    timestamp: ts,
                    consensus_id,
                    primary_epoch,
                    member_gen,
                    candidates,
                },
            ),
            other => {
                tracing::error!(
                    msg_type = ?other.msg_type(),
                    "#556 primary consensus FSM emitted a non-primary-emitted \
                     frame variant — dropping; this is a Layer 2/3 bug"
                );
                return;
            }
        };
        if let Err(e) = self.send_to(dst, frame).await {
            tracing::warn!(
                target: "dynrunner_consensus",
                error = %e,
                "#556 consensus frame egress failed; the next round retry \
                 (or the next sweep) will re-fire"
            );
        }
    }

    /// FSM-Restart commit path. Builds the `DeadSecondary` records from
    /// the current `secondary_keepalives` (the operator-relevant
    /// silence-age inputs at commit time), runs the destructive
    /// declaration via [`Self::declare_secondaries_confirmed_dead`], and
    /// dispatches one respawn per target through the existing
    /// [`Self::dispatch_respawn_request`] pipeline (Layer 5 adds the
    /// scancel inside that pipeline).
    async fn commit_consensus_restart(&mut self, targets: BTreeSet<String>) {
        tracing::warn!(
            target: "dynrunner_consensus",
            targets = ?targets,
            "#556 consensus committed Restart — mesh-declaring peers dead \
             and dispatching respawns"
        );
        let now = Instant::now();
        let dead: Vec<DeadSecondary> = targets
            .iter()
            .map(|id| {
                let last_keepalive = self
                    .secondary_keepalives
                    .get(id)
                    .copied()
                    .unwrap_or(now);
                DeadSecondary {
                    secondary_id: id.clone(),
                    last_keepalive,
                }
            })
            .collect();
        // Full destructive declaration: per-peer requeue_dead_secondary
        // (PeerRemoved + TimeoutDetected + worker drop + roster clear).
        let _ = self
            .declare_secondaries_confirmed_dead(dead, RemovalCause::KeepaliveMiss)
            .await;
        // Per-target respawn: matches the existing #207 respawn-pipeline
        // contract — one `RespawnRequest` per dead peer, dispatched via
        // the budget-aware spawner. Layer 5 will gate scancel inside
        // this same path.
        for id in targets {
            self.dispatch_respawn_request(RespawnRequest {
                original_id: id,
                cause: RemovalCause::KeepaliveMiss,
            });
        }
    }
}
