//! Primary-side mesh-consensus FSM for the #556 respawn quorum.
//!
//! This module owns the WHEN-to-mesh-declare-a-peer-dead concern on the
//! primary side. Local "SchedulingSuspect" — the existing
//! `only_silent_held_work_remains` + `maybe_requeue_silent_held_work` path
//! — is unchanged: the primary may locally suspect a secondary and
//! re-assign its held work whenever workers would otherwise idle. What
//! this FSM gates is the *destructive* next step: actually MESH-declaring
//! the peer dead, which downstream (Layer 4 + Layer 5) is what triggers
//! the restart broadcast and the SLURM scancel.
//!
//! The protocol is a two-round consensus over the non-suspected secondary
//! set:
//!
//! 1. `BroadcastSuspect` → `SuspectPeers` frame. Every non-suspected
//!    secondary that has POSITIVE liveness evidence on a suspected peer
//!    answers `ResolvedPeer`; absence of evidence is silence (the
//!    protocol is one-sided: only positive contradictions count). After
//!    [`RESOLUTION_DEADLINE`] the FSM rolls forward with whatever
//!    suspects remain.
//! 2. `BroadcastRestart` → `RestartRequest` frame. Each non-suspected
//!    secondary re-checks each candidate one final time before the
//!    destructive step and answers with a `RestartConfirm` (which
//!    carries BOTH `still_suspicious` and `resolved_since`). After
//!    [`CONFIRMATION_DEADLINE`] the FSM either fires `Restart{targets}`
//!    (if every expected responder replied AND set non-empty after
//!    intersection) or retries the broadcast once
//!    ([`CONFIRMATION_MAX_RETRIES`]); a second timeout aborts with a
//!    WARN naming the non-responders.
//!
//! ## Owner-approved design contracts (Q1, Q2, Q5)
//!
//! ### Q1 — side-gate at `BroadcastRestart` entry
//!
//! Before the FSM emits the `RestartRequest` frame, it checks that the
//! current primary's view contains at least a *failover quorum* worth of
//! live (non-suspected) peers — `live_peer_count >=
//! failover_quorum_threshold(total_known_members)` where the threshold
//! mirrors [`crate::secondary::election::failover_quorum`] verbatim
//! (`n.div_ceil(2) + 1`). The arithmetic is the same one a surviving
//! secondary would use to promote against this primary on a partition: if
//! THIS side cannot win a failover vote, it must not be allowed to
//! spuriously restart the peers on the partition-majority side. The
//! formula is duplicated rather than re-exported because the secondary's
//! `failover_quorum` lives behind a `pub(crate)` visibility on a sibling
//! module — see [`failover_quorum_threshold`] and its `debug_assert_eq!`
//! sweep test for the cross-reference proof of agreement.
//!
//! ### Q2 — no slurm-authoritative bypass
//!
//! A slurm-authoritative snapshot (`squeue` says the job is gone, etc.)
//! MAY influence the upstream WHEN-to-escalate decision — i.e. it can
//! seed [`ConsensusFsm::set_scheduling_suspect`] and trigger
//! [`ConsensusFsm::escalate`] — but it does NOT bypass this consensus
//! respond gate. A primary stranded on a full mesh-collapse partition
//! whose slurm snapshot insists every other member is gone degrades to
//! `SchedulingSuspect`-only indefinitely: it can keep redistributing its
//! own held work locally, but it cannot mesh-declare anyone dead, cannot
//! emit a `RestartRequest`, and cannot trigger a scancel — because it
//! has no consensus partners to confirm with. This is by design: a
//! local-authority bypass re-opens the bisection split-brain face the
//! Q1 side-gate was designed to close.
//!
//! ### Q5 — retry-once-then-abort
//!
//! If a non-suspected secondary times out mid-`CollectingConfirmations`,
//! the FSM retries the `RestartRequest` broadcast EXACTLY ONCE per round
//! (NOT per non-responder — the budget is for the round). On the second
//! timeout the round aborts with a WARN log naming the non-responders
//! verbatim. Escalating a non-responder into the suspected set is
//! FORBIDDEN: it creates a feedback loop where the side-gate keeps
//! shrinking as non-responders pile into the suspected set, which
//! is self-defeating (the primary loses its own quorum side faster than
//! the partition's other side).
//!
//! ## Layer 2 scope
//!
//! This is the dormant FSM module. It is NOT yet wired into
//! [`crate::primary::PrimaryCoordinator`] — that is Layer 4. The unit
//! tests drive every transition via a deterministic [`Clock`] so the
//! FSM can be exercised in isolation; the production wiring will inject
//! [`SystemClock`].

pub mod fsm;
pub mod round;
mod wiring;

#[cfg(test)]
mod tests;

use std::time::{Duration, Instant};

pub use fsm::{ConsensusFsm, ConsensusOutput, ConsensusState, MeshSnapshot};
pub use round::ConfirmTally;

/// How long the FSM stays in `CollectingResolutions` waiting for
/// `ResolvedPeer` echoes before rolling forward to `BroadcastRestart`.
///
/// Sized to a few cycles of the secondary-side `PeerProbe` cadence
/// (currently 5s ±~1s, Layer 3): three probes per target is the
/// per-target observability budget — enough that a transiently-stalled
/// peer has roughly three chances to be heard from before the round
/// commits to restart.
pub const RESOLUTION_DEADLINE: Duration = Duration::from_secs(15);

/// How long the FSM stays in `CollectingConfirmations` waiting for
/// `RestartConfirm` replies before either retrying the broadcast (if
/// the retry budget is non-zero) or aborting the round.
///
/// Sized for one full mesh round-trip plus a healthy margin: the
/// secondary handler is purely local (no I/O beyond the reply) so it
/// answers in well under a second on a healthy leg; 5s comfortably
/// covers a transient link redial without holding the round open
/// indefinitely.
pub const CONFIRMATION_DEADLINE: Duration = Duration::from_secs(5);

/// Per-round retry budget for the `RestartRequest` broadcast. Matches
/// the owner-approved Q5 contract: retry-once-then-abort. A second
/// timeout escalates to `AbortRound { reason: "responder timeout" }`
/// with a WARN log naming the non-responders.
pub const CONFIRMATION_MAX_RETRIES: u8 = 1;

/// Compute the failover-quorum threshold for a fleet of `n` known
/// members.
///
/// Mirrors [`crate::secondary::election::failover_quorum`] verbatim
/// (`n.div_ceil(2) + 1`). The single source of the rule lives on the
/// secondary side because the SECONDARY is the structural majority
/// voter on a primary failover; this primary-side mirror exists so the
/// consensus FSM can gate `BroadcastRestart` entry on the same threshold
/// WITHOUT importing through a `pub(super)` visibility. The
/// `debug_assert_eq!` sweep in [`tests::failover_quorum_sweep_agrees`]
/// pins the two implementations in lockstep.
///
/// On a 6-member fleet this returns `4` (i.e. the primary's side must
/// see ≥4 live non-suspected peers including itself to be allowed to
/// MESH-declare anyone dead). On a 2-member-survivor partition each
/// side returns `2` and only the side that still has both members can
/// proceed — the side-gate's whole point is to make this symmetric so
/// at most one side ever passes.
#[inline]
pub fn failover_quorum_threshold(n: usize) -> usize {
    n.div_ceil(2) + 1
}

/// Clock abstraction for the FSM. The FSM never reads wall-clock time
/// directly — every time-dependent transition is driven through
/// `poll(now)` with a caller-supplied `Instant` — but the seam exists
/// for callers (and tests) that want to inject a clock once and forget
/// it. Production code uses [`SystemClock`].
pub trait Clock {
    fn now(&self) -> Instant;
}

/// Production clock. Reads `Instant::now()` on every call.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}
