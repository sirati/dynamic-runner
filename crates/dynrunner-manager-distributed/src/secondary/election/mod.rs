//! Failover election (F2): primary-death detection on the secondary side
//! plus the lowest-live-ID + quorum election that promotes one of the
//! surviving peers to the new primary role.
//!
//! State machine:
//!
//! ```text
//!   Normal ──primary missed N keepalives──> Suspecting
//!   Suspecting ──quorum agrees + we are lowest-live-id──> Candidate
//!   Suspecting ──quorum agrees + a peer is lower──> Voting
//!   Candidate  ──majority confirms──> Promoted
//!   Voting     ──saw winner's first task list──> Normal (new primary tracked)
//!   any state  ──primary keepalive arrives──> Normal
//! ```
//!
//! Each tick of the secondary's processing loop calls
//! [`SecondaryCoordinator::run_election_tick`] which advances the state
//! machine based on the elapsed time and the messages received from peers
//! in `handle_peer_message`.

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::DistributedMessage;

mod coordinator;

#[cfg(test)]
mod observer_filter_tests;
#[cfg(test)]
mod tests;

/// The election state machine. Variants carry only the fields they need so
/// the rest of the secondary doesn't have to pattern-match optionals.
pub(super) enum ElectionState {
    /// Primary is alive (or we haven't started suspecting yet).
    Normal,
    /// We've stopped seeing primary keepalives. Querying peers about
    /// primary liveness; collecting their `TimeoutResponse` answers.
    Suspecting {
        since: Instant,
        responses: HashMap<String, Option<f64>>,
    },
    /// We are the candidate. Waiting for majority `PromotionConfirm`.
    Candidate {
        round: u32,
        confirms: HashSet<String>,
        started: Instant,
    },
    /// A peer with lower id is candidate. Waiting for the take-over to
    /// happen (we'll see the new primary start sending messages).
    Voting { round: u32, candidate: String },
    /// We've taken over and are now acting as the primary. Stops further
    /// election ticks.
    Promoted,
}

/// Output of one election tick: messages the caller should flush onto the
/// peer transport before the next iteration, plus whether the tick committed
/// THIS node's promotion (the lone-survivor self-quorum path).
pub(super) struct ElectionTickActions<I: Identifier> {
    pub(super) broadcast: Vec<DistributedMessage<I>>,
    /// Set when the tick transitioned the election to [`ElectionState::Promoted`]
    /// because this candidate ALREADY met quorum at the moment it self-promoted
    /// — i.e. a lone survivor whose `failover_quorum(0) == 1` is satisfied by
    /// its own single confirm, with no peer `PromotionConfirm` ever arriving to
    /// drive `record_promotion_confirm`. The caller drives the SAME terminal
    /// action the peer-confirm path uses (`fire_local_promotion`): originate +
    /// locally apply + broadcast `PrimaryChanged { new = self }`. The terminal
    /// action lives with the caller (it is async; the tick is sync), exactly
    /// as the broadcast flush does.
    pub(super) promoted: bool,
}

impl<I: Identifier> Default for ElectionTickActions<I> {
    fn default() -> Self {
        Self {
            broadcast: Vec::new(),
            promoted: false,
        }
    }
}

pub(super) fn next_round(state: &ElectionState) -> u32 {
    match state {
        ElectionState::Voting { round, .. } | ElectionState::Candidate { round, .. } => round + 1,
        _ => 1,
    }
}

/// The failover quorum size for a live mesh of `live_peer_count` peers
/// (the count of [`SecondaryCoordinator::live_peer_ids`], which is
/// `peer_keepalives` MINUS the current primary — so it is the set of peers
/// that could vote/become-candidate, NOT counting the node being
/// failed-over-FROM). The voter itself is NOT in `live_peer_count`; the
/// caller adds itself (`+1`) when tallying agreement/confirms against this.
///
/// SINGLE SOURCE for the rule (CLAUDE.md: no duplicated logic). It is
/// consulted at BOTH the Suspecting-tally site and the
/// PromotionConfirm-tally site in [`coordinator`]; before extraction the
/// formula `peer_count.div_ceil(2) + 1` was copy-pasted at both, and a
/// desync only manifests on a LIVE failover (not locally reproducible), so
/// the rule lives here exactly once.
///
/// ADAPTS TO THE LIVE FLEET — the denominator is the CURRENT live-peer set,
/// never a fixed `config.num_secondaries`. On a partition the live set
/// shrinks symmetrically, so a 2-survivor fleet (primary + 2 secondaries,
/// primary dies → each survivor sees `live_peer_count == 1`) computes
/// `quorum = 1.div_ceil(2) + 1 = 2`, reachable by self + the one surviving
/// peer — the "2-node trap" fix. A genuinely-lone (zero-peer) secondary
/// would compute `quorum = 0.div_ceil(2) + 1 = 1` and self-promote solo;
/// that `quorum == 1` self-promotion is a split-brain and is blocked
/// UPSTREAM by the `mesh_degraded` guard in `run_election_tick` (a lone
/// secondary never reaches a tally), NOT here — this function only states
/// the majority arithmetic.
///
/// OBSERVERS are NEITHER voters NOR counted in this denominator (F4): they
/// emit no `Secondary` keepalive, so they are structurally absent from
/// `peer_keepalives` → never in `live_peer_ids` → never in
/// `live_peer_count`. This is by design — an observer can neither reply
/// `TimeoutResponse` (so it cannot agree) nor accept a `PromotionVote` (so
/// it cannot confirm), so counting it would inflate the quorum past what
/// the agreeing/confirming set can ever reach (re-opening a quorum trap).
pub(super) fn failover_quorum(live_peer_count: usize) -> usize {
    live_peer_count.div_ceil(2) + 1
}
