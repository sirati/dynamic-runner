//! Per-round bookkeeping for [`super::ConsensusFsm`].
//!
//! Holds the round-id allocator and the tally records that travel with
//! the FSM through `CollectingResolutions` → `BroadcastRestart` →
//! `CollectingConfirmations`. The state-machine transitions are in
//! [`super::fsm`]; this module owns the *data* the transitions read
//! and write.

use std::sync::atomic::{AtomicU64, Ordering};

/// One responder's view of a `RestartRequest`'s candidate set.
///
/// Populated by [`super::fsm::ConsensusFsm::apply_confirm`] when a
/// secondary's `RestartConfirm` frame arrives. The FSM uses this on
/// the deadline check to compute the final restart-target intersection:
/// candidates remain in the restart batch only if every responder
/// reports them in `still_suspicious` AND no responder reports them in
/// `resolved_since` (per the wire-protocol contract — a single
/// `resolved_since` retraction withdraws the candidate regardless of
/// `still_suspicious` tallies elsewhere).
#[derive(Debug, Clone, Default)]
pub struct ConfirmTally {
    /// Candidates this responder continues to see as unreachable. A
    /// "yes-restart" vote for each id in this set.
    pub still_suspicious: Vec<String>,
    /// Candidates this responder has heard from BETWEEN the opening
    /// `SuspectPeers` and the `RestartRequest` — a last-second
    /// retraction. Any candidate listed here is withdrawn from the
    /// restart batch regardless of `still_suspicious` tallies on other
    /// responders.
    pub resolved_since: Vec<String>,
}

/// Monotonic allocator for the per-FSM `consensus_id`.
///
/// One instance lives on [`super::fsm::ConsensusFsm`]. Allocated at the
/// `BroadcastSuspect` transition. The `primary_epoch` field on every
/// wire frame disambiguates across primary failovers (a fresh primary
/// starts its counter from zero but its epoch is higher), so per-FSM
/// monotonic suffices.
#[derive(Debug, Default)]
pub struct ConsensusIdAllocator {
    next: AtomicU64,
}

impl ConsensusIdAllocator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Mint the next round id. Starts at `1` on a fresh FSM; `0` is
    /// reserved as the "no-round-in-flight" sentinel so an apply_resolved /
    /// apply_confirm whose frame carries `consensus_id: 0` cannot
    /// accidentally collide with a real round.
    pub fn next(&self) -> u64 {
        // `fetch_add` returns the prior value; we want the FIRST mint to
        // be `1`, hence the `+ 1` adjustment. `Ordering::Relaxed` is
        // sufficient — the FSM is single-threaded per-primary; this
        // atomic exists only because the FSM holds the allocator behind
        // a shared `&self` in some accessor paths.
        self.next.fetch_add(1, Ordering::Relaxed) + 1
    }
}
