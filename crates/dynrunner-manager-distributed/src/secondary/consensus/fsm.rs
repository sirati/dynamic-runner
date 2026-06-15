//! Secondary-side mesh-consensus state machine.
//!
//! See [`super`]'s module doc for the protocol contract and the
//! owner-approved Q4 / stale-frame discipline. This module owns the
//! state transitions; [`super::jitter`] owns the probe-fan-out
//! desynchronizer.

use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;
use std::time::Instant;

use dynrunner_protocol_primary_secondary::messages::DistributedMessage;

use super::jitter::{JitterSource, XorshiftJitter};
use super::{AWAITING_ROUND_END_TIMEOUT, PROBE_BASE_PERIOD};

/// State of the secondary-side consensus FSM.
///
/// A single `SecondaryConsensusFsm` holds exactly one of these at a
/// time. See [`super`]'s module doc for the full transition diagram.
#[derive(Debug, Clone)]
pub enum SecondaryConsensusState {
    /// Initial / post-terminal resting state. The FSM still answers
    /// inbound [`PeerProbe`]s addressed to `self_id` from `Idle` — that
    /// path is stateless w.r.t. round-id — but it never EMITS a
    /// `PeerProbe` of its own from here.
    Idle,
    /// A `SuspectPeers` has opened a round; the FSM is actively probing
    /// each still-unresolved suspect on a `PROBE_BASE_PERIOD ± jitter`
    /// cadence and tallying inbound `PeerProbeAck`s. Each ack matched
    /// against `(consensus_id, sender_id ∈ suspects)` is forwarded to
    /// the primary as a `ResolvedPeer` and removed from active probing.
    ProbingFor {
        consensus_id: u64,
        primary_epoch: u64,
        member_gen: u64,
        /// Per-target next-fire deadline. Targets are removed when the
        /// FSM observes a matching `PeerProbeAck` (the target moves
        /// into `resolved`).
        suspects: BTreeMap<String, Instant>,
        /// Peers we have already credited with a `PeerProbeAck` this
        /// round. Read by `apply_restart_request` to compute
        /// `resolved_since` (`= candidates ∩ resolved`) and to suppress
        /// a duplicate `ResolvedPeer` emit on a re-arriving ack.
        resolved: BTreeSet<String>,
    },
    /// The primary's `RestartRequest` has landed and the FSM has
    /// emitted its `RestartConfirm`. The FSM stays here, continuing to
    /// probe leftover candidates the primary asked us about, until
    /// either a fresh `SuspectPeers` opens a new round or
    /// `AWAITING_ROUND_END_TIMEOUT` fires and we fall back to `Idle`.
    AwaitingRoundEnd {
        consensus_id: u64,
        primary_epoch: u64,
        member_gen: u64,
        suspects: BTreeMap<String, Instant>,
        resolved: BTreeSet<String>,
        /// Set on entry; the `poll(now)` arm transitions to `Idle` when
        /// `now >= deadline`.
        deadline: Instant,
    },
}

/// The signal `poll` / `apply_*` calls return.
///
/// May be 0..N frames (when the FSM emits a `ResolvedPeer` AND
/// schedules its next probe in the same call, or when an
/// `apply_suspect_peers` immediately emits the opening probe). The
/// FRAMES variant is boxed-internally so the consumer-facing enum
/// stays the same shape Layer 2 chose; the `Vec` carries the boxed
/// frames in emit order.
#[derive(Debug)]
pub enum SecondaryConsensusOutput<I> {
    /// No frames produced this call.
    Idle,
    /// Frames to be broadcast / unicast by the caller. The frames are
    /// in emit order and may include any of:
    /// - `PeerProbe` (the round's probe-fan-out, addressed point-to-point
    ///   to one suspect),
    /// - `PeerProbeAck` (stateless reply to an inbound probe addressed
    ///   to us),
    /// - `ResolvedPeer` (positive-liveness echo to the primary),
    /// - `RestartConfirm` (round-2 commit reply to the primary).
    ///
    /// Boxed for the same reason Layer 2's `EmitFrame` is boxed:
    /// `DistributedMessage` is a chunky enum and the consumer-facing
    /// signal shouldn't inherit its sizeof bloat.
    EmitFrames(Vec<Box<DistributedMessage<I>>>),
}

/// The secondary-side mesh-consensus FSM.
///
/// One instance lives on each [`crate::secondary::SecondaryCoordinator`]
/// (Layer 4 wires it). The FSM is single-threaded per-secondary: every
/// transition is driven through the methods on this struct. Time-
/// dependent transitions are gated by the caller-supplied `now:
/// Instant` on every `poll` — the FSM never reads wall-clock time
/// itself, so deterministic-tick tests can exercise every transition
/// without `tokio::time::pause()`.
///
/// The generic parameter `I` is the protocol task-id type the underlying
/// [`DistributedMessage<I>`] is generic over. The consensus variants
/// don't actually USE `I` (they carry only `String` peer ids in their
/// payloads), but the wire enum is generic on `I` so the FSM has to be
/// too; we hold a `PhantomData<I>` to satisfy the type system.
pub struct SecondaryConsensusFsm<I, J: JitterSource = XorshiftJitter> {
    state: SecondaryConsensusState,
    self_id: String,
    /// Monotone view of the highest `primary_epoch` this FSM has ever
    /// observed on an inbound consensus frame. Frames from an OLDER
    /// epoch are dropped (deposed-primary defense); a NEWER epoch
    /// resets the FSM to `Idle` before processing the frame as the
    /// opening of a fresh round under the newly-elected primary.
    current_known_primary_epoch: u64,
    /// The jitter source; see [`super::jitter`]. `&mut self` on every
    /// `next_ms()` call.
    jitter: J,
    _marker: PhantomData<fn() -> I>,
}

impl<I, J: JitterSource> std::fmt::Debug for SecondaryConsensusFsm<I, J> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SecondaryConsensusFsm")
            .field("state", &self.state)
            .field("self_id", &self.self_id)
            .field(
                "current_known_primary_epoch",
                &self.current_known_primary_epoch,
            )
            .finish_non_exhaustive()
    }
}

impl<I> SecondaryConsensusFsm<I, XorshiftJitter> {
    /// Construct a fresh FSM in `Idle` with the production jitter
    /// source seeded from `(self_id, Instant::now())`. Tests should use
    /// [`Self::with_jitter`] and a [`super::jitter::FixedJitter`] for
    /// byte-exact deadline assertions.
    pub fn new(self_id: String) -> Self {
        let jitter = XorshiftJitter::new(&self_id, Instant::now());
        Self::with_jitter(self_id, jitter)
    }
}

impl<I, J: JitterSource> SecondaryConsensusFsm<I, J> {
    /// Construct a fresh FSM in `Idle` with a caller-supplied jitter
    /// source. Production code reaches for [`Self::new`]; tests pass a
    /// `FixedJitter`.
    pub fn with_jitter(self_id: String, jitter: J) -> Self {
        Self {
            state: SecondaryConsensusState::Idle,
            self_id,
            current_known_primary_epoch: 0,
            jitter,
            _marker: PhantomData,
        }
    }

    /// Current in-flight `consensus_id`, if any. Exposed for diagnostics
    /// + tests; production code should never branch on it directly.
    pub fn current_consensus_id(&self) -> Option<u64> {
        match &self.state {
            SecondaryConsensusState::Idle => None,
            SecondaryConsensusState::ProbingFor { consensus_id, .. }
            | SecondaryConsensusState::AwaitingRoundEnd { consensus_id, .. } => Some(*consensus_id),
        }
    }

    /// Read-only view of the FSM state. Exposed for tests; production
    /// code drives the FSM through `poll` + `apply_*` and never branches
    /// on the state directly.
    pub fn state(&self) -> &SecondaryConsensusState {
        &self.state
    }

    /// Observe a `primary_epoch` carried on ANY frame from the primary.
    /// Layer 4 calls this on every inbound primary frame so the FSM
    /// tracks the freshest known epoch. Caller-side seam — also called
    /// from the `apply_*` paths automatically, but exposed for the case
    /// where Layer 4 sees a non-consensus primary frame (e.g. a
    /// `Keepalive`) carrying a fresh epoch.
    pub fn observe_primary_epoch(&mut self, epoch: u64) {
        if epoch > self.current_known_primary_epoch {
            self.current_known_primary_epoch = epoch;
            // A newer primary's epoch invalidates any in-flight round
            // we were running under an older primary — its consensus
            // round has been superseded by definition.
            if !matches!(self.state, SecondaryConsensusState::Idle) {
                self.state = SecondaryConsensusState::Idle;
            }
        }
    }

    /// Apply an inbound `SuspectPeers` from the primary.
    ///
    /// `primary_epoch` discipline:
    /// - `< current_known_primary_epoch` → ignored (deposed primary).
    /// - `> current_known_primary_epoch` → epoch advances; the FSM
    ///   first resets to `Idle`, then opens a brand-new round on the
    ///   advanced epoch.
    /// - `== current_known_primary_epoch` → standard path.
    ///
    /// `consensus_id` discipline (under matching epoch): a DIFFERENT
    /// in-flight `consensus_id` (or a state already in `ProbingFor` /
    /// `AwaitingRoundEnd`) means the primary opened a new round; the
    /// FSM transitions to `ProbingFor` with the new round's set,
    /// preserving NOTHING from the prior round's `resolved` tally
    /// (the new round speaks of a different suspect set under a
    /// different id).
    ///
    /// May emit the opening per-target `PeerProbe`(s) inline so the
    /// suspects don't wait a full poll-tick for the first probe.
    pub fn apply_suspect_peers(
        &mut self,
        consensus_id: u64,
        primary_epoch: u64,
        member_gen: u64,
        suspected: &[String],
        now: Instant,
    ) -> SecondaryConsensusOutput<I> {
        if primary_epoch < self.current_known_primary_epoch {
            return SecondaryConsensusOutput::Idle;
        }
        if primary_epoch > self.current_known_primary_epoch {
            self.current_known_primary_epoch = primary_epoch;
            self.state = SecondaryConsensusState::Idle;
        }
        if suspected.is_empty() {
            // No-op: an empty suspect list opens no round. Keeps the
            // FSM out of a degenerate `ProbingFor{}` with nothing to
            // probe.
            return SecondaryConsensusOutput::Idle;
        }
        // Self-suspicion is impossible under the protocol (the primary
        // doesn't include the recipient in its own suspect set), but
        // we belt-and-braces it: never probe ourselves.
        let suspects: BTreeMap<String, Instant> = suspected
            .iter()
            .filter(|id| id.as_str() != self.self_id)
            .map(|id| (id.clone(), now))
            .collect();
        if suspects.is_empty() {
            return SecondaryConsensusOutput::Idle;
        }
        self.state = SecondaryConsensusState::ProbingFor {
            consensus_id,
            primary_epoch,
            member_gen,
            suspects,
            resolved: BTreeSet::new(),
        };
        // Fire the opening probes inline — each target's `next_fire` was
        // initialized to `now`, so a single `poll(now)` would emit them
        // too. We do it here so the caller observes the same shape as
        // Layer 2's `apply_*` returns (a synchronous tally update +
        // optional inline emission).
        self.poll(now)
    }

    /// Apply an inbound `RestartRequest` from the primary.
    ///
    /// `primary_epoch` discipline mirrors `apply_suspect_peers`:
    /// older→ignore, newer→reset+ignore (no round is open yet under
    /// the new epoch; the primary will follow with its own
    /// `SuspectPeers` to open one).
    ///
    /// `consensus_id` discipline:
    /// - In `ProbingFor{cid_state}`: a matching `cid` produces the
    ///   confirm. A mismatched `cid` at the SAME epoch is owner-flagged
    ///   "primary advanced mid-round" — the FSM absorbs the new round
    ///   id, REBUILDING `suspects = intersection(candidates, suspects)`
    ///   and `resolved = old_resolved ∩ candidates`, then emits the
    ///   confirm under the new round id. The owner-approved rationale:
    ///   "honor the LATEST cid the primary asserts".
    /// - In `Idle` (or `AwaitingRoundEnd` of a prior round): no inflight
    ///   suspects → `still_suspicious = candidates` (we have no probe
    ///   evidence either way; YES-restart for everything the primary
    ///   asked about), `resolved_since = empty`. We do NOT open a
    ///   round retroactively; the secondary just answers what it knows.
    pub fn apply_restart_request(
        &mut self,
        consensus_id: u64,
        primary_epoch: u64,
        member_gen: u64,
        candidates: &[String],
        now: Instant,
    ) -> SecondaryConsensusOutput<I> {
        if primary_epoch < self.current_known_primary_epoch {
            return SecondaryConsensusOutput::Idle;
        }
        if primary_epoch > self.current_known_primary_epoch {
            self.current_known_primary_epoch = primary_epoch;
            self.state = SecondaryConsensusState::Idle;
            // Fall through — even at the new epoch we still owe an
            // answer to THIS RestartRequest. Treat it as
            // "no per-round evidence": YES-restart for everything.
        }

        // Compute the answer under the FSM's current per-round evidence
        // (if any) and the candidate set, then ship the confirm.
        let (still_suspicious, resolved_since, new_suspects, new_resolved): (
            Vec<String>,
            Vec<String>,
            BTreeMap<String, Instant>,
            BTreeSet<String>,
        ) = match std::mem::replace(&mut self.state, SecondaryConsensusState::Idle) {
            SecondaryConsensusState::Idle => {
                // No evidence either way; YES-restart for every
                // candidate. The new round runs under `consensus_id`,
                // with `suspects = candidates`, `resolved = ∅`.
                let new_suspects: BTreeMap<String, Instant> = candidates
                    .iter()
                    .filter(|c| c.as_str() != self.self_id)
                    .map(|c| (c.clone(), now))
                    .collect();
                let still: Vec<String> = new_suspects.keys().cloned().collect();
                (still, Vec::new(), new_suspects, BTreeSet::new())
            }
            SecondaryConsensusState::ProbingFor {
                consensus_id: cid_state,
                primary_epoch: _,
                member_gen: _,
                suspects,
                resolved,
            }
            | SecondaryConsensusState::AwaitingRoundEnd {
                consensus_id: cid_state,
                primary_epoch: _,
                member_gen: _,
                suspects,
                resolved,
                ..
            } => {
                // Whether or not `cid_state == consensus_id` we honor
                // the primary's asserted cid (owner Q: "honor the LATEST
                // cid the primary asserts"). The FSM transitions onto
                // the new cid, REBUILDING `suspects` and `resolved` as
                // intersections with `candidates`.
                let _ = cid_state;
                let candidates_set: BTreeSet<&str> =
                    candidates.iter().map(String::as_str).collect();
                let mut still: Vec<String> = Vec::new();
                let mut resolved_since_vec: Vec<String> = Vec::new();
                for c in candidates {
                    if c.as_str() == self.self_id {
                        continue;
                    }
                    if resolved.contains(c) {
                        resolved_since_vec.push(c.clone());
                    } else {
                        still.push(c.clone());
                    }
                }
                // Rebuild the active scheduling set from candidates ∖
                // resolved — we keep probing the still-suspicious ones
                // until `AwaitingRoundEnd` times out.
                let new_suspects: BTreeMap<String, Instant> = suspects
                    .into_iter()
                    .filter(|(id, _)| {
                        candidates_set.contains(id.as_str()) && !resolved.contains(id)
                    })
                    .collect();
                // Seed any candidate the primary asked about that we
                // hadn't been probing — under the new round id we now
                // owe it a probe schedule too (`next_fire = now`, so
                // the next `poll` emits it).
                let mut new_suspects = new_suspects;
                for c in candidates {
                    if c.as_str() == self.self_id {
                        continue;
                    }
                    if resolved.contains(c) {
                        continue;
                    }
                    new_suspects.entry(c.clone()).or_insert(now);
                }
                let new_resolved: BTreeSet<String> = resolved
                    .into_iter()
                    .filter(|id| candidates_set.contains(id.as_str()))
                    .collect();
                (still, resolved_since_vec, new_suspects, new_resolved)
            }
        };

        // Transition to AwaitingRoundEnd (keep probing leftover
        // still-suspicious candidates in case one resolves between now
        // and the primary's commit deadline).
        let deadline = now + AWAITING_ROUND_END_TIMEOUT;
        self.state = SecondaryConsensusState::AwaitingRoundEnd {
            consensus_id,
            primary_epoch,
            member_gen,
            suspects: new_suspects,
            resolved: new_resolved,
            deadline,
        };

        let confirm = DistributedMessage::RestartConfirm {
            target: None,
            sender_id: String::new(),
            timestamp: 0.0,
            consensus_id,
            responder_id: self.self_id.clone(),
            still_suspicious,
            resolved_since,
        };
        SecondaryConsensusOutput::EmitFrames(vec![Box::new(confirm)])
    }

    /// Apply an inbound `PeerProbeAck` (we were the prober; a suspect
    /// answered).
    ///
    /// Match criteria (ALL must hold; otherwise the ack is dropped
    /// silently):
    /// - The FSM is in `ProbingFor` or `AwaitingRoundEnd` (an `Idle`
    ///   FSM has no outstanding round to credit the ack against).
    /// - The frame's `consensus_id` matches the in-flight round's id
    ///   (stale-round defense).
    /// - The frame's `sender_id` (the responding peer) is in the
    ///   round's `suspects` set AND not already in `resolved`
    ///   (defends against double-emit on a second ack from the same
    ///   peer).
    ///
    /// Emits one `ResolvedPeer{cid, observer_id=self, resolved=sender}`
    /// to the primary on match.
    pub fn apply_probe_ack(
        &mut self,
        frame_sender_id: &str,
        frame_consensus_id: u64,
        _now: Instant,
    ) -> SecondaryConsensusOutput<I> {
        let (consensus_id_state, suspects, resolved) = match &mut self.state {
            SecondaryConsensusState::ProbingFor {
                consensus_id,
                suspects,
                resolved,
                ..
            }
            | SecondaryConsensusState::AwaitingRoundEnd {
                consensus_id,
                suspects,
                resolved,
                ..
            } => (*consensus_id, suspects, resolved),
            SecondaryConsensusState::Idle => return SecondaryConsensusOutput::Idle,
        };
        if consensus_id_state != frame_consensus_id {
            return SecondaryConsensusOutput::Idle;
        }
        if resolved.contains(frame_sender_id) {
            // Already credited — defend against a duplicate ack on the
            // same round.
            return SecondaryConsensusOutput::Idle;
        }
        if !suspects.contains_key(frame_sender_id) {
            // The acknowledging peer is not in our suspect set for
            // this round; ignore (defensive — the wire layer typically
            // wouldn't deliver such a frame, but the FSM is defensive).
            return SecondaryConsensusOutput::Idle;
        }
        suspects.remove(frame_sender_id);
        resolved.insert(frame_sender_id.to_owned());
        let resolved_peer = DistributedMessage::ResolvedPeer {
            target: None,
            sender_id: String::new(),
            timestamp: 0.0,
            consensus_id: consensus_id_state,
            observer_id: self.self_id.clone(),
            resolved: frame_sender_id.to_owned(),
        };
        SecondaryConsensusOutput::EmitFrames(vec![Box::new(resolved_peer)])
    }

    /// Apply an inbound `PeerProbe` (a peer thinks we might be down
    /// and is dialling us directly).
    ///
    /// THIS IS THE STATELESS HALF: it does NOT consult the FSM's round
    /// state. A suspected peer is, from its own POV, healthy — it has
    /// no opinion about any consensus round. It just answers the
    /// probe's `consensus_id` verbatim so the prober can correlate the
    /// reply to the round whose suspect list named us.
    ///
    /// Self-filter: if `probed_id != self.self_id`, the frame is
    /// dropped silently (a misrouted fan-out or stale role-route).
    pub fn apply_probe_request(
        &mut self,
        frame_sender_id: &str,
        frame_consensus_id: u64,
        frame_probed_id: &str,
        _now: Instant,
    ) -> SecondaryConsensusOutput<I> {
        if frame_probed_id != self.self_id {
            return SecondaryConsensusOutput::Idle;
        }
        let ack = DistributedMessage::PeerProbeAck {
            target: None,
            sender_id: String::new(),
            timestamp: 0.0,
            consensus_id: frame_consensus_id,
            prober_id: frame_sender_id.to_owned(),
        };
        SecondaryConsensusOutput::EmitFrames(vec![Box::new(ack)])
    }

    /// Drive time-based transitions. Returns the frames the caller must
    /// fan out this tick.
    ///
    /// In `ProbingFor` / `AwaitingRoundEnd`, for each suspect whose
    /// next-fire deadline is `<= now`, emit a `PeerProbe` and re-schedule
    /// the suspect for `now + PROBE_BASE_PERIOD ± jitter`. In
    /// `AwaitingRoundEnd`, additionally transition to `Idle` if
    /// `now >= deadline`.
    pub fn poll(&mut self, now: Instant) -> SecondaryConsensusOutput<I> {
        // Owner contract: AwaitingRoundEnd timeout returns to Idle.
        // Check this BEFORE the probe-fire scan so a polled deadline
        // doesn't fire spurious final probes.
        if let SecondaryConsensusState::AwaitingRoundEnd { deadline, .. } = &self.state
            && now >= *deadline
        {
            self.state = SecondaryConsensusState::Idle;
            return SecondaryConsensusOutput::Idle;
        }
        let (consensus_id, suspects) = match &mut self.state {
            SecondaryConsensusState::ProbingFor {
                consensus_id,
                suspects,
                ..
            }
            | SecondaryConsensusState::AwaitingRoundEnd {
                consensus_id,
                suspects,
                ..
            } => (*consensus_id, suspects),
            SecondaryConsensusState::Idle => return SecondaryConsensusOutput::Idle,
        };
        let mut frames: Vec<Box<DistributedMessage<I>>> = Vec::new();
        // Snapshot the target ids we'll fire on this tick, then update
        // their next-fire slots — avoids a borrow-of-the-map-while-
        // iterating problem.
        let due_targets: Vec<String> = suspects
            .iter()
            .filter_map(|(id, next_fire)| if *next_fire <= now { Some(id.clone()) } else { None })
            .collect();
        for target in due_targets {
            frames.push(Box::new(DistributedMessage::PeerProbe {
                target: None,
                sender_id: String::new(),
                timestamp: 0.0,
                consensus_id,
                probed_id: target.clone(),
            }));
            let jitter_ms = self.jitter.next_ms();
            let mut next_fire = now + PROBE_BASE_PERIOD;
            // Symmetric `± jitter_ms` (jitter may be negative; saturate
            // on subtract to avoid an Instant underflow on the rare
            // very-early-process-start clock).
            if jitter_ms >= 0 {
                next_fire += std::time::Duration::from_millis(jitter_ms as u64);
            } else {
                next_fire = next_fire
                    .checked_sub(std::time::Duration::from_millis((-jitter_ms) as u64))
                    .unwrap_or(next_fire);
            }
            // Re-fetch the suspect map (the match-arm borrow was tied
            // to the scope `suspects` was bound in; we drop into the
            // FSM's state again to mutate). This is the same shape
            // Layer 2 uses in its `poll` arms.
            match &mut self.state {
                SecondaryConsensusState::ProbingFor { suspects, .. }
                | SecondaryConsensusState::AwaitingRoundEnd { suspects, .. } => {
                    suspects.insert(target, next_fire);
                }
                SecondaryConsensusState::Idle => unreachable!(
                    "we just established the FSM is in ProbingFor/AwaitingRoundEnd above"
                ),
            }
        }
        if frames.is_empty() {
            SecondaryConsensusOutput::Idle
        } else {
            SecondaryConsensusOutput::EmitFrames(frames)
        }
    }
}
