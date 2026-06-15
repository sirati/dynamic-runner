//! Secondary-side mesh-consensus FSM for the #556 respawn quorum.
//!
//! Sibling of [`crate::primary::consensus`]. The primary's FSM owns the
//! WHEN-to-mesh-declare-a-peer-dead concern; this one owns the
//! SECONDARY's three responsibilities under that protocol:
//!
//! 1. **Active probing of a suspected peer.** On
//!    [`DistributedMessage::SuspectPeers`](dynrunner_protocol_primary_secondary::messages::DistributedMessage)
//!    arrival, every non-suspected secondary periodically dials the
//!    suspected peers ([`PROBE_BASE_PERIOD`] ± [`PROBE_JITTER_MS`]) until
//!    either the round ends or the peer answers. A matching
//!    [`DistributedMessage::PeerProbeAck`](dynrunner_protocol_primary_secondary::messages::DistributedMessage)
//!    is positive liveness evidence and is forwarded to the primary as
//!    [`DistributedMessage::ResolvedPeer`](dynrunner_protocol_primary_secondary::messages::DistributedMessage).
//! 2. **Stateless ack of an inbound probe.** A SUSPECTED secondary (or
//!    any secondary, since the probe-fan can reach a misrouted addressee)
//!    answers an inbound `PeerProbe{probed_id == self}` with a
//!    `PeerProbeAck`. The receiver need NOT be running a consensus round
//!    at all — the suspected peer's own view of the cluster is "nothing
//!    is wrong"; only the prober's outstanding round-id is what makes the
//!    ack meaningful upstream. This is THE realisation of the owner's
//!    spec: "if at any time during the period they get an answer from
//!    the secondary".
//! 3. **Round-2 commit reply.** On
//!    [`DistributedMessage::RestartRequest`](dynrunner_protocol_primary_secondary::messages::DistributedMessage)
//!    arrival, the secondary intersects the primary's `candidates` with
//!    its own per-round `resolved_set` to compute `still_suspicious`
//!    (yes-restart) and `resolved_since` (last-second retraction), then
//!    answers with a single
//!    [`DistributedMessage::RestartConfirm`](dynrunner_protocol_primary_secondary::messages::DistributedMessage).
//!    The FSM then enters [`SecondaryConsensusState::AwaitingRoundEnd`]
//!    until the next `SuspectPeers` opens a new round, an upgraded
//!    `primary_epoch` arrives, or the [`AWAITING_ROUND_END_TIMEOUT`]
//!    elapses.
//!
//! ## Owner-approved design contracts
//!
//! ### Q4 — active probe is the contract
//!
//! Probing is an explicit [`PeerProbe`]/[`PeerProbeAck`] request-response
//! pair carrying the round's `consensus_id`. Piggy-backing positive
//! evidence on inbound `Keepalive` arrival was REJECTED because the
//! keepalive may traverse the mesh (an indirect relay leg) and is NOT
//! guaranteed to land at the prober inside the round's resolution
//! window. The active probe targets the SUSPECTED peer directly (the
//! prober self-routes via the role-routing path; an unreachable direct
//! leg falls through to a `Relay` envelope on the wire layer, invisible
//! to this FSM).
//!
//! ### Stale round / stale primary discipline
//!
//! Two stamps gate every state-mutating frame:
//!
//! - `primary_epoch` is monotone for the FSM lifetime. An inbound frame
//!   from an OLDER epoch is dropped — a deposed primary's late
//!   `SuspectPeers` must not poison the current round. A frame from a
//!   NEWER epoch is the new authority: the FSM resets to `Idle` and
//!   processes the frame as the opening of a brand-new round under the
//!   freshly-elected primary.
//! - `consensus_id` is the per-round tally key. Inbound probe-acks /
//!   confirm-requests whose `consensus_id` does not match the FSM's
//!   in-flight round are dropped (stale-round defense; the wire layer
//!   typically drops them upstream too, but the FSM is defensive).
//!   Owner-approved exception: a fresh `SuspectPeers` arriving with a
//!   different `consensus_id` at the SAME `primary_epoch` is honored as
//!   a primary-driven new round (the primary may legitimately bump
//!   `consensus_id` between Suspect and Restart in degenerate cases).
//!
//! ## Layer 3 scope
//!
//! This is the dormant FSM module. It is NOT yet wired into
//! [`crate::secondary::SecondaryCoordinator`] — that is Layer 4. The
//! unit tests drive every transition via a deterministic clock and a
//! fixed-jitter source so the FSM can be exercised in isolation;
//! production wiring will inject [`SystemClock`](super::super::primary::consensus::SystemClock)
//! and the default [`XorshiftJitter`].
//!
//! ## Module layout
//!
//! - [`fsm`] — the state machine + the [`SecondaryConsensusOutput`] enum.
//! - [`jitter`] — the [`JitterSource`] trait + [`XorshiftJitter`]
//!   production impl + [`FixedJitter`] test impl.
//!
//! [`PeerProbe`]: dynrunner_protocol_primary_secondary::messages::DistributedMessage::PeerProbe
//! [`PeerProbeAck`]: dynrunner_protocol_primary_secondary::messages::DistributedMessage::PeerProbeAck
//! [`SecondaryConsensusState::AwaitingRoundEnd`]: fsm::SecondaryConsensusState::AwaitingRoundEnd

pub mod fsm;
pub mod jitter;
mod wiring;

#[cfg(test)]
mod tests;

use std::time::Duration;

pub use fsm::{SecondaryConsensusFsm, SecondaryConsensusOutput, SecondaryConsensusState};
pub use jitter::{FixedJitter, JitterSource, XorshiftJitter};

/// Base period between probe re-fires for a single still-unresolved
/// suspect. The actual probe-fire deadline is `base ± jitter` so a fleet
/// of N secondaries all opened by the same `SuspectPeers` broadcast do
/// NOT synchronize their probe fan-out.
///
/// Matches the owner-approved spec verbatim: "all secondaries try to
/// reach the missing secondary every 5 seconds +/- a random ~1000ms".
pub const PROBE_BASE_PERIOD: Duration = Duration::from_secs(5);

/// One-sided amplitude of the per-probe jitter, in milliseconds. The
/// jitter source produces a value in `-PROBE_JITTER_MS..=PROBE_JITTER_MS`
/// which is added to [`PROBE_BASE_PERIOD`] before scheduling the next
/// fire for that target.
///
/// Matches the owner-approved spec ("~1000ms"). The actual symmetric
/// range is `[base - PROBE_JITTER_MS .. base + PROBE_JITTER_MS]`.
pub const PROBE_JITTER_MS: i32 = 1000;

/// How long the FSM stays in `AwaitingRoundEnd` after emitting its
/// `RestartConfirm` before falling back to `Idle` if no fresh
/// `SuspectPeers` (new round) arrives.
///
/// Sized comfortably above the primary's
/// [`crate::primary::consensus::CONFIRMATION_DEADLINE`] (5s) plus its
/// retry budget — a full primary-side round (5s × 2 deadline + slack)
/// finishes well inside 60s. After this window the secondary has no
/// outstanding reason to keep probing under THIS round's id: either the
/// primary committed and the cluster is moving forward, or it aborted
/// the round and a fresh `SuspectPeers` will re-open if necessary.
pub const AWAITING_ROUND_END_TIMEOUT: Duration = Duration::from_secs(60);
