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
    Voting {
        round: u32,
        candidate: String,
    },
    /// We've taken over and are now acting as the primary. Stops further
    /// election ticks.
    Promoted,
}

impl ElectionState {
    pub(super) fn is_normal(&self) -> bool {
        matches!(self, ElectionState::Normal)
    }
}

/// Output of one election tick: messages the caller should flush onto the
/// peer transport before the next iteration.
pub(super) struct ElectionTickActions<I: Identifier> {
    pub(super) broadcast: Vec<DistributedMessage<I>>,
}

impl<I: Identifier> Default for ElectionTickActions<I> {
    fn default() -> Self {
        Self {
            broadcast: Vec::new(),
        }
    }
}

/// Conventional id used inside `TimeoutQuery::query_node_id` when the
/// queried party is the primary. Keeps every secondary using the same
/// string so peers can match queries up.
pub(super) fn primary_node_id() -> String {
    "primary".into()
}

pub(super) fn next_round(state: &ElectionState) -> u32 {
    match state {
        ElectionState::Voting { round, .. } | ElectionState::Candidate { round, .. } => round + 1,
        _ => 1,
    }
}

