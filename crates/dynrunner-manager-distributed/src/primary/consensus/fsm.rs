//! Primary-side mesh-consensus state machine.
//!
//! See the module-level doc on [`super`] for the protocol contract and
//! the owner-approved Q1/Q2/Q5 design decisions. This module owns the
//! state transitions; [`super::round`] owns the per-round bookkeeping
//! types the transitions read and write.

use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;
use std::time::Instant;

use dynrunner_protocol_primary_secondary::messages::DistributedMessage;

use super::round::{ConfirmTally, ConsensusIdAllocator};
use super::{
    CONFIRMATION_DEADLINE, CONFIRMATION_MAX_RETRIES, RESOLUTION_DEADLINE,
    failover_quorum_threshold,
};

/// Side-gate input. The caller (Layer 4 / the `PrimaryCoordinator`)
/// snapshots its current live-peer / membership counts and hands them
/// to [`ConsensusFsm::poll`] every tick; the FSM consults the snapshot
/// at `BroadcastRestart` entry to decide whether THIS primary's side
/// holds a failover-quorum majority — see [`super`]'s Q1 contract.
#[derive(Debug, Clone, Copy)]
pub struct MeshSnapshot {
    /// Count of non-suspected secondaries currently mesh-reachable from
    /// this primary's view (the candidate confirmers).
    pub live_peer_count: usize,
    /// Full membership cardinality known to the primary (used as the
    /// failover-quorum denominator — mirrors the secondary-side
    /// `failover_quorum_peer_count` shape).
    pub total_known_members: usize,
}

/// State of the consensus FSM. A single `ConsensusFsm` holds exactly
/// one of these at a time. See [`super`]'s top-of-file doc for the
/// full state-transition diagram.
#[derive(Debug, Clone)]
pub enum ConsensusState {
    /// Initial / post-terminal resting state. Only
    /// [`ConsensusFsm::set_scheduling_suspect`] populates the local
    /// set out of here.
    Idle,
    /// The local primary suspects these peers for SCHEDULING purposes
    /// (work-redistribution). This state never emits a wire frame on
    /// its own — the caller decides when to escalate to consensus by
    /// calling [`ConsensusFsm::escalate`].
    SchedulingSuspect(BTreeSet<String>),
    /// `escalate` has been called; the next `poll` will mint the
    /// `consensus_id` and emit the opening `SuspectPeers` frame.
    BroadcastSuspect {
        consensus_id: u64,
        set: BTreeSet<String>,
        started_at: Instant,
        primary_epoch: u64,
        member_gen: u64,
        expected_responders: BTreeSet<String>,
    },
    /// `SuspectPeers` has been emitted; the FSM is collecting
    /// `ResolvedPeer` echoes until `now >= deadline`.
    CollectingResolutions {
        consensus_id: u64,
        set: BTreeSet<String>,
        deadline: Instant,
        primary_epoch: u64,
        member_gen: u64,
        expected_responders: BTreeSet<String>,
        /// The set of peer ids that have been resolved (de-suspected) in
        /// this round. Recorded for auditability; the active suspect
        /// set is `set` (resolved peers are removed in-place).
        resolved: BTreeSet<String>,
    },
    /// Resolution deadline has fired with a non-empty suspect set; the
    /// next `poll` will check the side-gate and either abort or emit
    /// the `RestartRequest` frame.
    BroadcastRestart {
        consensus_id: u64,
        set: BTreeSet<String>,
        primary_epoch: u64,
        member_gen: u64,
        expected_responders: BTreeSet<String>,
        retries_left: u8,
    },
    /// `RestartRequest` has been emitted; the FSM is collecting
    /// `RestartConfirm` replies until either every expected responder
    /// has replied OR `now >= deadline`.
    CollectingConfirmations {
        consensus_id: u64,
        set: BTreeSet<String>,
        primary_epoch: u64,
        member_gen: u64,
        expected_responders: BTreeSet<String>,
        received_confirms: BTreeMap<String, ConfirmTally>,
        deadline: Instant,
        retries_left: u8,
    },
    /// Terminal: emit `Restart{targets}` to the caller (Layer 4
    /// consumes the signal) and return to `Idle`.
    Restart(BTreeSet<String>),
    /// Terminal: the suspect set drained to empty before commit
    /// (false-positives resolved by the cluster). Return to `Idle`.
    DropSuspicion,
    /// Terminal: the round was aborted (side-gate failure OR
    /// responder timeout after retry). Return to `Idle`.
    AbortRound { reason: String },
}

/// The signal `poll` returns to the caller on each tick.
#[derive(Debug, Clone)]
pub enum ConsensusOutput<I> {
    /// No transition this tick.
    Idle,
    /// The FSM has constructed a wire frame; the caller broadcasts /
    /// fans it via the existing mesh-egress path. The FSM never owns
    /// the broadcast itself — it owns the WHEN-to-broadcast and the
    /// frame contents. Boxed because [`DistributedMessage`] is one of
    /// the chunkier wire enums (#[allow(clippy::large_enum_variant)]
    /// is declared on the wire enum itself for its own reasons, but
    /// the *consumer-facing* `ConsensusOutput` shouldn't inherit the
    /// 400+ byte sizeof bloat).
    EmitFrame(Box<DistributedMessage<I>>),
    /// Terminal: the suspect set drained empty; drop suspicion.
    DropSuspicion,
    /// Terminal: consensus committed to a restart batch. Layer 4 will
    /// consume this and issue the actual `RespawnRequest` /
    /// `Slurm-scancel` (Layer 5).
    Restart { targets: BTreeSet<String> },
    /// Terminal: round aborted. The reason is logged at WARN level by
    /// the caller (Layer 4 owns the logger; the FSM stays pure).
    Abort { reason: String },
}

/// The primary-side mesh-consensus FSM.
///
/// One instance lives on the [`crate::primary::PrimaryCoordinator`]
/// (Layer 4 wires it). The FSM is single-threaded per-primary: every
/// transition is driven through the methods on this struct, never
/// through a background task. Time-dependent transitions are gated by
/// the caller-supplied `now: Instant` on every `poll` — the FSM never
/// reads wall-clock time itself, so deterministic-tick tests can
/// exercise every transition without `tokio::time::pause()`.
///
/// The generic parameter `I` is the protocol task-id type the
/// underlying [`DistributedMessage<I>`] is generic over. The consensus
/// variants don't actually USE `I` (they carry only `String` peer ids
/// in their payloads), but the wire enum is generic on `I` so the FSM
/// has to be too; we hold a `PhantomData<I>` to satisfy the type
/// system.
#[derive(Debug)]
pub struct ConsensusFsm<I> {
    state: ConsensusState,
    ids: ConsensusIdAllocator,
    _marker: PhantomData<fn() -> I>,
}

impl<I> Default for ConsensusFsm<I> {
    fn default() -> Self {
        Self::new()
    }
}

impl<I> ConsensusFsm<I> {
    /// Fresh FSM in `Idle`.
    pub fn new() -> Self {
        Self {
            state: ConsensusState::Idle,
            ids: ConsensusIdAllocator::new(),
            _marker: PhantomData,
        }
    }

    /// Populate the local `SchedulingSuspect` set. Idempotent: calling
    /// with an empty `set` from `Idle` is a no-op. Replaces (does NOT
    /// merge) the current set when called from `SchedulingSuspect` —
    /// the caller owns the union/diff arithmetic if it wants to merge.
    ///
    /// Calling this once consensus has already escalated
    /// (`BroadcastSuspect` onward) is a no-op: the in-flight round's
    /// suspect set is frozen against the `consensus_id`. The caller
    /// should wait for the round to terminate before seeding a new
    /// suspect set.
    pub fn set_scheduling_suspect(&mut self, set: BTreeSet<String>) {
        match &mut self.state {
            ConsensusState::Idle if !set.is_empty() => {
                self.state = ConsensusState::SchedulingSuspect(set);
            }
            ConsensusState::Idle => {}
            ConsensusState::SchedulingSuspect(current) => {
                if set.is_empty() {
                    self.state = ConsensusState::Idle;
                } else {
                    *current = set;
                }
            }
            // In-flight round — ignore. The caller MUST wait for
            // termination before seeding a fresh suspect set; the
            // in-flight round's set is round-id-locked.
            _ => {}
        }
    }

    /// Escalate the local `SchedulingSuspect` set into a consensus
    /// round. Mints the `consensus_id`. From `Idle` (no local
    /// suspects) this is a no-op. From a non-terminal in-flight state
    /// it is also a no-op (callers should wait for round termination).
    ///
    /// `expected_responders` is the set of non-suspected secondaries
    /// the primary expects `RestartConfirm` replies from. The FSM
    /// does NOT compute this itself — Layer 4 derives it from the
    /// current role-table / live-peer view at escalate time. (The
    /// FSM cannot know about role projections; the caller owns the
    /// roster intersection.)
    pub fn escalate(
        &mut self,
        now: Instant,
        primary_epoch: u64,
        member_gen: u64,
        expected_responders: BTreeSet<String>,
    ) {
        let suspects = match std::mem::replace(&mut self.state, ConsensusState::Idle) {
            ConsensusState::SchedulingSuspect(s) => s,
            other => {
                // Restore — escalate is a no-op from any other state.
                self.state = other;
                return;
            }
        };
        if suspects.is_empty() {
            // Defensive: an empty SchedulingSuspect shouldn't exist
            // (set_scheduling_suspect prevents it) but treat as a
            // no-op rather than minting a round-id for nothing.
            self.state = ConsensusState::Idle;
            return;
        }
        let consensus_id = self.ids.next();
        self.state = ConsensusState::BroadcastSuspect {
            consensus_id,
            set: suspects,
            started_at: now,
            primary_epoch,
            member_gen,
            expected_responders,
        };
    }

    /// Current in-flight `consensus_id`, if any. Exposed for the
    /// secondary FSM's round-stale check (consumed at the secondary
    /// layer; the primary just exposes).
    pub fn current_consensus_id(&self) -> Option<u64> {
        match &self.state {
            ConsensusState::BroadcastSuspect { consensus_id, .. }
            | ConsensusState::CollectingResolutions { consensus_id, .. }
            | ConsensusState::BroadcastRestart { consensus_id, .. }
            | ConsensusState::CollectingConfirmations { consensus_id, .. } => Some(*consensus_id),
            _ => None,
        }
    }

    /// Snapshot of the current state. Exposed for tests / observers;
    /// production code drives the FSM through `poll` + `apply_*` and
    /// should never branch on the state directly.
    pub fn state(&self) -> &ConsensusState {
        &self.state
    }

    /// Drop a single peer from the local `SchedulingSuspect` set if it
    /// is present. NO-OP in every other state — an in-flight round
    /// (`BroadcastSuspect` onward) is round-id-locked, so a peer's
    /// fresh-frame recovery during the round flows through `ResolvedPeer`
    /// echoes from the other secondaries instead of an inline drop here.
    /// Idle / terminal states return without effect.
    ///
    /// The lazy dispatch-altitude path (`requeue_silent_held_work_locally`
    /// seeded the suspect set) needs a per-peer recovery hook because the
    /// next sweep's `set_scheduling_suspect` would only narrow the set
    /// when the WHOLE silent-held condition lifts; meanwhile a single
    /// peer that re-proves itself with a fresh keepalive must exit
    /// dispatch's #556 skip gate immediately so its workers re-enter
    /// the dispatch order.
    pub fn clear_scheduling_suspect_if_present(&mut self, peer_id: &str) {
        if let ConsensusState::SchedulingSuspect(set) = &mut self.state
            && set.remove(peer_id)
            && set.is_empty()
        {
            self.state = ConsensusState::Idle;
        }
    }

    /// Read-only view of the ACTIVE suspect set across every state that
    /// carries one (`SchedulingSuspect`, `BroadcastSuspect`,
    /// `CollectingResolutions`, `BroadcastRestart`, `CollectingConfirmations`).
    /// Terminal / `Idle` states return an empty set.
    ///
    /// Layer 4 reads this on every coordinator-side
    /// [`super::MeshSnapshot`] build so the side-gate's
    /// `live_peer_count` denominator excludes the in-flight suspects
    /// (the candidate confirmers are the non-suspected secondaries by
    /// definition). Production code uses this read; tests prefer
    /// [`Self::state`] for state-shape assertions.
    pub fn in_flight_suspects(&self) -> &BTreeSet<String> {
        // The empty-set marker for non-suspect-carrying states. Returned
        // by reference so callers don't allocate; lifetime is bound to
        // `&self`, same as `state()`.
        static EMPTY: std::sync::OnceLock<BTreeSet<String>> = std::sync::OnceLock::new();
        match &self.state {
            ConsensusState::SchedulingSuspect(s)
            | ConsensusState::BroadcastSuspect { set: s, .. }
            | ConsensusState::CollectingResolutions { set: s, .. }
            | ConsensusState::BroadcastRestart { set: s, .. }
            | ConsensusState::CollectingConfirmations { set: s, .. } => s,
            ConsensusState::Idle
            | ConsensusState::Restart(_)
            | ConsensusState::DropSuspicion
            | ConsensusState::AbortRound { .. } => EMPTY.get_or_init(BTreeSet::new),
        }
    }

    /// Apply a `ResolvedPeer` echo. Stale-round / wrong-state frames
    /// are silently ignored (the wire layer drops stale-round frames
    /// upstream too, but the FSM is defensive — a delayed delivery
    /// from a prior round must not poison the current tally).
    pub fn apply_resolved(
        &mut self,
        frame_consensus_id: u64,
        _observer_id: &str,
        resolved_peer: &str,
    ) {
        match &mut self.state {
            // Cannot collapse the `if set.remove(...)` into the
            // pattern guard: pattern guards see bindings as
            // immutable, and `BTreeSet::remove` is `&mut self`.
            #[allow(clippy::collapsible_match)]
            ConsensusState::CollectingResolutions {
                consensus_id,
                set,
                resolved,
                ..
            } if *consensus_id == frame_consensus_id => {
                if set.remove(resolved_peer) {
                    resolved.insert(resolved_peer.to_owned());
                }
            }
            // A `ResolvedPeer` may legitimately arrive after the
            // resolution deadline rolled forward into `BroadcastRestart`
            // / `CollectingConfirmations` (the secondary observed the
            // probe-ack after the primary's deadline fired). Treat it
            // as a `resolved_since` retraction on the in-flight commit
            // set — the wire contract folds this into the `RestartConfirm`
            // path, but a fast-path `ResolvedPeer` echo arriving on the
            // same `consensus_id` is honored too.
            ConsensusState::CollectingConfirmations {
                consensus_id, set, ..
            } if *consensus_id == frame_consensus_id => {
                set.remove(resolved_peer);
            }
            ConsensusState::BroadcastRestart {
                consensus_id, set, ..
            } if *consensus_id == frame_consensus_id => {
                set.remove(resolved_peer);
            }
            _ => {}
        }
    }

    /// Apply a `RestartConfirm` reply. Stale-round / wrong-state
    /// frames are silently ignored.
    ///
    /// `resolved_since` from the responder removes those candidates
    /// from the in-flight set (matching the wire-protocol contract).
    /// The responder itself is recorded against `received_confirms` so
    /// the deadline check can fire as soon as every expected responder
    /// has replied.
    pub fn apply_confirm(
        &mut self,
        frame_consensus_id: u64,
        responder_id: &str,
        still_suspicious: Vec<String>,
        resolved_since: Vec<String>,
    ) {
        let ConsensusState::CollectingConfirmations {
            consensus_id,
            set,
            received_confirms,
            expected_responders,
            ..
        } = &mut self.state
        else {
            return;
        };
        if *consensus_id != frame_consensus_id {
            return;
        }
        if !expected_responders.contains(responder_id) {
            return;
        }
        for resolved in &resolved_since {
            set.remove(resolved);
        }
        received_confirms.insert(
            responder_id.to_owned(),
            ConfirmTally {
                still_suspicious,
                resolved_since,
            },
        );
    }

    /// Drive time-based transitions. Returns the signal the caller
    /// must act on this tick.
    ///
    /// The caller (Layer 4) invokes this on every coordinator loop
    /// iteration. The FSM is the SOLE source of consensus-related
    /// wire frames — the caller never emits `SuspectPeers` /
    /// `RestartRequest` itself, only fans the frame the FSM hands it.
    pub fn poll(&mut self, now: Instant, mesh: &MeshSnapshot) -> ConsensusOutput<I>
    where
        I: Clone,
    {
        // 1. `BroadcastSuspect` → emit `SuspectPeers`, transition to
        //    `CollectingResolutions`.
        if matches!(self.state, ConsensusState::BroadcastSuspect { .. }) {
            let ConsensusState::BroadcastSuspect {
                consensus_id,
                set,
                started_at,
                primary_epoch,
                member_gen,
                expected_responders,
            } = std::mem::replace(&mut self.state, ConsensusState::Idle)
            else {
                unreachable!("guarded by the matches! above")
            };
            let consensus_id_v = consensus_id;
            let primary_epoch_v = primary_epoch;
            let member_gen_v = member_gen;
            let deadline = started_at + RESOLUTION_DEADLINE;
            let suspected: Vec<String> = set.iter().cloned().collect();
            self.state = ConsensusState::CollectingResolutions {
                consensus_id: consensus_id_v,
                set,
                deadline,
                primary_epoch: primary_epoch_v,
                member_gen: member_gen_v,
                expected_responders,
                resolved: BTreeSet::new(),
            };
            return ConsensusOutput::EmitFrame(Box::new(DistributedMessage::SuspectPeers {
                target: None,
                sender_id: String::new(),
                timestamp: 0.0,
                consensus_id: consensus_id_v,
                primary_epoch: primary_epoch_v,
                member_gen: member_gen_v,
                suspected,
            }));
        }

        // 2. `CollectingResolutions` → roll forward at deadline (or
        //    immediately if the set already drained to empty).
        if let ConsensusState::CollectingResolutions { set, .. } = &self.state
            && set.is_empty()
        {
            self.state = ConsensusState::DropSuspicion;
            return self.finalize_terminal();
        }
        if let ConsensusState::CollectingResolutions { deadline, .. } = &self.state
            && now >= *deadline
        {
            // Move forward — either DropSuspicion (set empty) or
            // BroadcastRestart (set non-empty).
            let ConsensusState::CollectingResolutions {
                consensus_id,
                set,
                primary_epoch,
                member_gen,
                expected_responders,
                ..
            } = std::mem::replace(&mut self.state, ConsensusState::Idle)
            else {
                unreachable!("guarded by the if-let above")
            };
            if set.is_empty() {
                self.state = ConsensusState::DropSuspicion;
                return self.finalize_terminal();
            }
            self.state = ConsensusState::BroadcastRestart {
                consensus_id,
                set,
                primary_epoch,
                member_gen,
                expected_responders,
                retries_left: CONFIRMATION_MAX_RETRIES,
            };
            // Fall through to BroadcastRestart handling below.
        }

        // 3. `BroadcastRestart` → side-gate; emit `RestartRequest` or
        //    abort.
        if matches!(self.state, ConsensusState::BroadcastRestart { .. }) {
            if mesh.live_peer_count < failover_quorum_threshold(mesh.total_known_members) {
                let reason = format!(
                    "side-gate: live_peer_count={} < failover_quorum_threshold({}) = {}",
                    mesh.live_peer_count,
                    mesh.total_known_members,
                    failover_quorum_threshold(mesh.total_known_members),
                );
                self.state = ConsensusState::AbortRound { reason };
                return self.finalize_terminal();
            }
            let ConsensusState::BroadcastRestart {
                consensus_id,
                set,
                primary_epoch,
                member_gen,
                expected_responders,
                retries_left,
            } = std::mem::replace(&mut self.state, ConsensusState::Idle)
            else {
                unreachable!("guarded by the matches! above")
            };
            let candidates: Vec<String> = set.iter().cloned().collect();
            let deadline = now + CONFIRMATION_DEADLINE;
            let consensus_id_v = consensus_id;
            let primary_epoch_v = primary_epoch;
            let member_gen_v = member_gen;
            self.state = ConsensusState::CollectingConfirmations {
                consensus_id: consensus_id_v,
                set,
                primary_epoch: primary_epoch_v,
                member_gen: member_gen_v,
                expected_responders,
                received_confirms: BTreeMap::new(),
                deadline,
                retries_left,
            };
            return ConsensusOutput::EmitFrame(Box::new(DistributedMessage::RestartRequest {
                target: None,
                sender_id: String::new(),
                timestamp: 0.0,
                consensus_id: consensus_id_v,
                primary_epoch: primary_epoch_v,
                member_gen: member_gen_v,
                candidates,
            }));
        }

        // 4. `CollectingConfirmations` → all-responded / deadline /
        //    retry / abort.
        if let ConsensusState::CollectingConfirmations {
            expected_responders,
            received_confirms,
            set,
            deadline,
            retries_left,
            consensus_id,
            primary_epoch,
            member_gen,
            ..
        } = &self.state
        {
            let all_responded = expected_responders
                .iter()
                .all(|id| received_confirms.contains_key(id));
            if all_responded {
                // Compute the intersection: candidates remain in the
                // restart batch iff every responder reports them in
                // `still_suspicious` AND no responder reports them in
                // `resolved_since`.
                let mut targets: BTreeSet<String> = set.clone();
                for tally in received_confirms.values() {
                    let yes: BTreeSet<&str> =
                        tally.still_suspicious.iter().map(String::as_str).collect();
                    targets.retain(|id| yes.contains(id.as_str()));
                    for retraction in &tally.resolved_since {
                        targets.remove(retraction);
                    }
                }
                self.state = if targets.is_empty() {
                    ConsensusState::DropSuspicion
                } else {
                    ConsensusState::Restart(targets)
                };
                return self.finalize_terminal();
            }
            if now >= *deadline {
                if *retries_left > 0 {
                    let new_retries = *retries_left - 1;
                    let consensus_id_v = *consensus_id;
                    let primary_epoch_v = *primary_epoch;
                    let member_gen_v = *member_gen;
                    let candidates: Vec<String> = set.iter().cloned().collect();
                    let new_deadline = now + CONFIRMATION_DEADLINE;
                    let ConsensusState::CollectingConfirmations {
                        retries_left,
                        deadline,
                        ..
                    } = &mut self.state
                    else {
                        unreachable!("guarded by the outer if-let")
                    };
                    *retries_left = new_retries;
                    *deadline = new_deadline;
                    return ConsensusOutput::EmitFrame(Box::new(DistributedMessage::RestartRequest {
                        target: None,
                        sender_id: String::new(),
                        timestamp: 0.0,
                        consensus_id: consensus_id_v,
                        primary_epoch: primary_epoch_v,
                        member_gen: member_gen_v,
                        candidates,
                    }));
                }
                let non_responders: Vec<String> = expected_responders
                    .iter()
                    .filter(|id| !received_confirms.contains_key(id.as_str()))
                    .cloned()
                    .collect();
                let reason = format!(
                    "responder timeout: non_responders={non_responders:?}"
                );
                self.state = ConsensusState::AbortRound { reason };
                return self.finalize_terminal();
            }
        }

        ConsensusOutput::Idle
    }

    /// Consume a terminal state and produce the matching output,
    /// resetting the FSM back to `Idle`.
    fn finalize_terminal(&mut self) -> ConsensusOutput<I> {
        match std::mem::replace(&mut self.state, ConsensusState::Idle) {
            ConsensusState::Restart(targets) => ConsensusOutput::Restart { targets },
            ConsensusState::DropSuspicion => ConsensusOutput::DropSuspicion,
            ConsensusState::AbortRound { reason } => ConsensusOutput::Abort { reason },
            other => {
                // Not actually terminal — restore and report Idle.
                self.state = other;
                ConsensusOutput::Idle
            }
        }
    }
}
