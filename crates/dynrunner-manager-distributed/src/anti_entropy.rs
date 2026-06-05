//! Periodic anti-entropy: digest exchange + pull-on-divergence.
//!
//! Single concern: the role-agnostic POLICY for the convergence cadence
//! — the tick period (with per-node deterministic jitter), how to build a
//! role's digest-broadcast frame, and the receive-side decision of
//! WHETHER and WHERE to pull a snapshot when a peer's digest shows the
//! local replica is behind. Every role (primary `operational_loop`,
//! secondary `process_tasks`, the relocation observer tail) drives its own
//! `tokio::time::interval` from [`tick_period`], broadcasts the frame from
//! [`digest_broadcast`] on each tick, and feeds a received digest to
//! [`reconcile_against_peer`]. The role owns ONLY its `send_to` edge; the
//! comparison, target selection, and frame construction live here ONCE so
//! no role re-implements them.
//!
//! This module holds NO merge logic. The pull it requests is the EXISTING
//! `RequestClusterSnapshot` → `ClusterSnapshot` → `ClusterState::restore`
//! path; the digest detector ([`StateDigest::is_behind`]) only decides
//! when to engage it.

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
pub fn digest_broadcast<I>(
    node_id: &str,
    timestamp: f64,
    digest: StateDigest,
) -> DistributedMessage<I> {
    DistributedMessage::StateDigest {
        sender_id: node_id.to_string(),
        timestamp,
        digest,
    }
}

/// The role facts a pulling node stamps on its `RequestClusterSnapshot`
/// so the snapshot responder records the requester's membership truthfully
/// (the `PeerJoined` it originates) — the same fields the cold-start
/// snapshot RPC carries. Each role builds this once from its config:
/// `node_id` is the requester's own id (the snapshot return address),
/// `is_observer` / `can_be_primary` its advertised role + capability.
pub struct RequesterIdentity<'a> {
    pub node_id: &'a str,
    /// Wire role advertisement only; there is NO observer MODE on a
    /// coordinator — the observer role IS the standalone
    /// `ObserverCoordinator`, which stamps `true` here on its pulls.
    pub is_observer: bool,
    pub can_be_primary: bool,
}

/// Receive-side decision for one peer digest. Given the LOCAL digest, the
/// PEER's digest (off the wire frame), the sender's id, the currently
/// recognised primary (if any), and the requester's own
/// [`RequesterIdentity`], return `Some((destination, request))` when the
/// local replica is behind the peer and should pull a snapshot — or `None`
/// when the replicas are converged (the self-quiescing case: no pull on a
/// matching digest).
///
/// Target preference: pull from [`Destination::Primary`] when a current
/// primary is known (the authoritative-complete responder), else from the
/// digest's sender directly (the peer that just proved it holds the
/// missing data). Both targets answer `RequestClusterSnapshot` with a
/// `ClusterSnapshot` the caller restores through the existing recv arm.
pub fn reconcile_against_peer<I>(
    local: &StateDigest,
    peer: &StateDigest,
    sender_id: &str,
    current_primary: Option<&str>,
    requester: &RequesterIdentity<'_>,
    timestamp: f64,
) -> Option<(Destination, DistributedMessage<I>)> {
    if !local.is_behind(peer) {
        // Converged on every field the peer reports — nothing to pull.
        // This is the steady-state path: a matching digest exchange costs
        // one comparison and zero round-trips.
        return None;
    }
    // Pull from the primary-preferred target. When a current primary is
    // known its snapshot is the authoritative-complete bootstrap; when not
    // (pre-primary window), the digest's sender just proved it holds data
    // we lack, so it is the right responder.
    let destination = match current_primary {
        Some(_) => Destination::Primary,
        None => Destination::Secondary(PeerId::from(sender_id.to_string())),
    };
    let request = DistributedMessage::RequestClusterSnapshot {
        sender_id: requester.node_id.to_string(),
        timestamp,
        is_observer: requester.is_observer,
        can_be_primary: requester.can_be_primary,
    };
    Some((destination, request))
}

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

    #[test]
    fn converged_digests_yield_no_pull() {
        let d = StateDigest {
            tasks_count: 2,
            tasks_hash: 0x1234,
            ..Default::default()
        };
        let me = RequesterIdentity {
            node_id: "me",
            is_observer: false,
            can_be_primary: true,
        };
        let decision: Option<(Destination, DistributedMessage<u32>)> =
            reconcile_against_peer(&d, &d, "peer-1", Some("prim"), &me, 1.0);
        assert!(decision.is_none());
    }

    #[test]
    fn behind_with_primary_pulls_from_primary() {
        let local = StateDigest::default();
        let peer = StateDigest {
            tasks_count: 3,
            tasks_hash: 0xAB,
            ..Default::default()
        };
        let me = RequesterIdentity {
            node_id: "me",
            is_observer: true,
            can_be_primary: false,
        };
        let (dst, req): (Destination, DistributedMessage<u32>) =
            reconcile_against_peer(&local, &peer, "peer-1", Some("prim"), &me, 1.0)
                .expect("should pull when behind");
        assert_eq!(dst, Destination::Primary);
        match req {
            DistributedMessage::RequestClusterSnapshot {
                sender_id,
                is_observer,
                can_be_primary,
                ..
            } => {
                assert_eq!(sender_id, "me");
                assert!(is_observer);
                assert!(!can_be_primary);
            }
            _ => panic!("expected RequestClusterSnapshot"),
        }
    }

    #[test]
    fn behind_without_primary_pulls_from_sender() {
        let local = StateDigest::default();
        let peer = StateDigest {
            tasks_count: 1,
            tasks_hash: 0x9,
            ..Default::default()
        };
        let me = RequesterIdentity {
            node_id: "me",
            is_observer: false,
            can_be_primary: true,
        };
        let (dst, _req): (Destination, DistributedMessage<u32>) =
            reconcile_against_peer(&local, &peer, "peer-7", None, &me, 1.0)
                .expect("should pull when behind");
        assert_eq!(
            dst,
            Destination::Secondary(PeerId::from("peer-7".to_string()))
        );
    }
}
