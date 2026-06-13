//! Periodic anti-entropy: digest exchange cadence + snapshot-RPC addressing.
//!
//! Single concern: the role-agnostic POLICY for the convergence cadence
//! — the tick period (with per-node deterministic jitter), how to build a
//! role's digest-broadcast frame, and the snapshot-RPC addressing policy
//! shared by every responder and pull initiator. Every role drives its own
//! `tokio::time::interval` from [`tick_period`] and broadcasts the frame
//! from [`digest_broadcast`] on each tick. The receive-side pull decision
//! (whether and from whom to pull) is the concern of `crate::pull_coordinator`
//! — the single-flight probe→select→pull FSM that replaced the old eager
//! per-digest immediate-pull path.
//!
//! This module owns the REPLY half of the snapshot-RPC addressing policy:
//! [`reply_destination`] — the responder types its stream-package answers
//! off the requester's self-declared role; the package construction itself
//! lives with the stream driver, `crate::snapshot_stream`. No responder
//! re-implements either.

use std::time::Duration;

use dynrunner_protocol_primary_secondary::{Destination, DistributedMessage, PeerId, StateDigest};

/// Base anti-entropy cadence. A node broadcasts its [`StateDigest`] once
/// per (base ± jitter) window. 20s sits in the plan's 15–30s band: long
/// enough that a converged mesh's steady-state digest traffic is a
/// handful of integers per node every 20s (well under the ~200B/node
/// budget), short enough that a transiently-disconnected peer reconverges
/// within one window of regaining the mesh.
pub const ANTI_ENTROPY_BASE_PERIOD: Duration = Duration::from_secs(20);

/// Peak deterministic jitter (±) applied to [`ANTI_ENTROPY_BASE_PERIOD`].
/// Spreading each node's tick phase by up to ±5s de-synchronises the
/// fleet's digest broadcasts so a large mesh does not emit every digest
/// in the same instant. The offset is DERIVED FROM THE NODE ID (see
/// [`tick_period`]) — wall-clock / RNG jitter is unavailable in this
/// deterministic runtime and would break reproducibility.
const ANTI_ENTROPY_JITTER: Duration = Duration::from_secs(5);

/// The anti-entropy tick period for a specific node, = base ± a
/// deterministic per-node jitter folded from `node_id`. Two distinct ids
/// almost always land on different phases, so the fleet's digest
/// broadcasts spread across the window instead of bursting together; the
/// SAME id always yields the SAME period (reproducible — no wall-clock, no
/// RNG). The jitter is bounded to `±ANTI_ENTROPY_JITTER` around the base,
/// so the realised period is always within `[base - jitter, base +
/// jitter]` and never degenerates to zero.
pub fn tick_period(node_id: &str) -> Duration {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    node_id.hash(&mut hasher);
    let h = hasher.finish();
    // Map the hash into [-jitter, +jitter] in millisecond resolution.
    let span_ms = (ANTI_ENTROPY_JITTER.as_millis() as u64) * 2 + 1;
    let offset_ms = (h % span_ms) as i64 - ANTI_ENTROPY_JITTER.as_millis() as i64;
    let base_ms = ANTI_ENTROPY_BASE_PERIOD.as_millis() as i64;
    // Bounded by construction (offset ∈ [-jitter, +jitter]); the
    // `max(1)` is a defensive floor that never actually triggers for the
    // configured base/jitter (15s ≤ period ≤ 25s).
    Duration::from_millis((base_ms + offset_ms).max(1) as u64)
}

/// Build this node's anti-entropy broadcast frame from its current
/// `digest`. Sent to [`Destination::All`] on each cadence tick so every
/// peer can compare and pull if behind.
///
/// `is_observer` is the sender's SELF-DECLARED role, stamped on the frame
/// so a behind peer that initiates a pull via `crate::pull_coordinator`
/// types the pull's [`Destination`] off the sender's role (`role_destination`)
/// instead of guessing `Secondary`. The sender knows its own role
/// authoritatively and unconditionally — no CRDT convergence needed — so
/// the very first digest already carries the correct typing.
pub fn digest_broadcast<I>(
    node_id: &str,
    timestamp: f64,
    digest: StateDigest,
    is_observer: bool,
) -> DistributedMessage<I> {
    DistributedMessage::StateDigest {
        target: None,
        sender_id: node_id.to_string(),
        timestamp,
        digest,
        sender_is_observer: is_observer,
    }
}

/// The REPLY half of the snapshot-RPC addressing policy: every stream-package
/// answer to a `RequestSnapshotStream` is typed off the requester's
/// SELF-DECLARED role — the `is_observer` it stamped on the request
/// frame — `Destination::Observer(id)` for an observer requester,
/// `Destination::Secondary(id)` for a compute peer.
///
/// The PeerId in the `Destination` selects the HOST at egress; the role
/// variant is the RECEIVER-side ingress demux selector (which local role
/// slot the requester's mesh-pump delivers the reply to). A wrong role
/// still reaches the right host but misses the slot demux and falls to
/// the fan-to-live-slots WARN — the post-relocation "directed frame names
/// a role with no live local slot … kind=ClusterSnapshot
/// target=Secondary(setup)" noise on a submitter whose snapshot pulls
/// were answered as if it were still a secondary. Every responder
/// (primary, secondary router, observer) types its reply through this
/// ONE policy point.
pub fn reply_destination(requester_id: &str, requester_is_observer: bool) -> Destination {
    role_destination(requester_id, requester_is_observer)
}

/// The role-addressing typing CORE shared by every snapshot-RPC edge: a
/// peer id plus its declared observer bit → the role-bearing
/// [`Destination`] (`Observer(id)` for an observer, `Secondary(id)` for a
/// compute peer) that the receiver-side ingress demux uses to pick the
/// local role slot. The snapshot REPLY ([`reply_destination`], typed off
/// the requester's declared role) resolves through this ONE point, so the
/// `(id, is_observer) → Destination` mapping is never re-implemented. The
/// pull-model frame builders ([`crate::pull_coordinator`]) type their
/// probe-reply / pull-request / fail destinations through this SAME point
/// (a `PullCoordinator` directive names a peer by id + role bit), so the
/// disciplined pull never re-implements the addressing either.
pub(crate) fn role_destination(id: &str, is_observer: bool) -> Destination {
    let id = PeerId::from(id.to_string());
    if is_observer {
        Destination::Observer(id)
    } else {
        Destination::Secondary(id)
    }
}

// The snapshot-RPC ANSWER construction moved with the transfer model:
// the monolithic `snapshot_reply` (one `ClusterSnapshot` frame carrying
// the whole serialized ledger) is gone; a responder now answers with a
// PACKAGE STREAM driven by `crate::snapshot_stream::
// SnapshotStreamResponder`, which types every package's destination
// through [`reply_destination`] — the contract (any live peer answers;
// the request's routing stamp is irrelevant; the answer is addressed by
// the requester's id and typed off its self-declared role) is unchanged
// and now spelled once on that driver.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tick_period_is_deterministic_and_bounded() {
        let p1 = tick_period("node-alpha");
        let p2 = tick_period("node-alpha");
        // Same id → same period (reproducible).
        assert_eq!(p1, p2);
        // Within [base - jitter, base + jitter].
        let lo = ANTI_ENTROPY_BASE_PERIOD - ANTI_ENTROPY_JITTER;
        let hi = ANTI_ENTROPY_BASE_PERIOD + ANTI_ENTROPY_JITTER;
        assert!(p1 >= lo && p1 <= hi, "period {p1:?} out of band");
    }

    #[test]
    fn distinct_ids_usually_spread() {
        // Not a guarantee for every pair, but the fold should separate at
        // least some ids — a smoke check that jitter is id-derived, not a
        // constant.
        let a = tick_period("aaaa");
        let b = tick_period("zzzz");
        let c = tick_period("mid-node-7");
        assert!(a != b || b != c || a != c);
    }

    /// The reply half of the snapshot-RPC policy: the answer is typed off
    /// the requester's self-declared role (`Observer(id)` for an observer
    /// requester, `Secondary(id)` for a compute peer) — the receiver-side
    /// ingress demux selector.
    #[test]
    fn reply_destination_is_typed_off_requesters_declared_role() {
        assert_eq!(
            reply_destination("obs-1", true),
            Destination::Observer(PeerId::from("obs-1"))
        );
        assert_eq!(
            reply_destination("sec-1", false),
            Destination::Secondary(PeerId::from("sec-1"))
        );
    }
}
