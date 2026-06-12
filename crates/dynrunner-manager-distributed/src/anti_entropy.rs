//! Periodic anti-entropy: digest exchange + pull-on-divergence.
//!
//! Single concern: the role-agnostic POLICY for the convergence cadence
//! — the tick period (with per-node deterministic jitter), how to build a
//! role's digest-broadcast frame, and the receive-side decision of
//! WHETHER to pull a snapshot (and that the target is the proven-ahead
//! SENDER) when a peer's digest shows the local replica is behind. Every
//! role (primary `operational_loop`,
//! secondary `process_tasks`, the relocation observer tail) drives its own
//! `tokio::time::interval` from [`tick_period`], broadcasts the frame from
//! [`digest_broadcast`] on each tick, and feeds a received digest to
//! [`reconcile_against_peer`]. The role owns ONLY its `send_to` edge; the
//! comparison, target selection, and frame construction live here ONCE so
//! no role re-implements them.
//!
//! This module also owns BOTH halves of the snapshot-RPC addressing
//! policy: the request side ([`RequesterIdentity`] — the role facts a
//! pulling node stamps on its `RequestClusterSnapshot`) and the reply
//! side ([`reply_destination`] — the responder types its `ClusterSnapshot`
//! answer off the requester's self-declared role — composed into the full
//! answer construction by [`snapshot_reply`]), so no responder
//! re-implements either.
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
        target: None,
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

/// The REPLY half of the snapshot-RPC addressing policy
/// ([`RequesterIdentity`] is the request half): the `ClusterSnapshot`
/// answer to a `RequestClusterSnapshot` is typed off the requester's
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
    let id = PeerId::from(requester_id.to_string());
    if requester_is_observer {
        Destination::Observer(id)
    } else {
        Destination::Secondary(id)
    }
}

/// The complete snapshot-RPC ANSWER for one `RequestClusterSnapshot`:
/// the `(destination, ClusterSnapshot)` pair a responder sends back.
///
/// Single owner of the reply CONSTRUCTION, composed with
/// [`reply_destination`] (the typing policy) so the three responders
/// (primary, secondary router, observer) share ONE answer shape instead
/// of each hand-building the frame. The contract every responder
/// honours through this point:
///
///   - ANY live peer answers from its own replica — `cluster_state` is
///     replicated, so any responder's snapshot is a valid bootstrap /
///     anti-entropy payload; role never gates serving.
///   - The request's routing `target` stamp is IRRELEVANT to the answer:
///     the stamp is the wire envelope's ingress-demux header (`None` from
///     a raw transport-level joiner send, `Some(..)` from every
///     coordinator egress), never request semantics. Responders must not
///     filter on it.
///   - The reply is addressed by the requester's ID (its return address
///     rides the request's `sender_id`) and typed off its SELF-DECLARED
///     role — resolvable for a ROSTERLESS joiner too, because id-bearing
///     destinations resolve at the transport (the direct leg), not
///     through any roster.
///
/// `snapshot_json` is the responder's digest-keyed serialize-once cache
/// payload (`ClusterState::snapshot_json` — never re-serialized per
/// request); reading it stays with the caller because the cache borrow
/// is the caller's `&mut` concern. The caller owns only its `send_to`
/// edge for the returned pair.
pub fn snapshot_reply<I: dynrunner_core::Identifier>(
    responder_id: &str,
    requester_id: &str,
    requester_is_observer: bool,
    timestamp: f64,
    snapshot_json: String,
) -> (Destination, DistributedMessage<I>) {
    (
        reply_destination(requester_id, requester_is_observer),
        DistributedMessage::ClusterSnapshot {
            target: None,
            sender_id: responder_id.to_string(),
            timestamp,
            snapshot_json,
        },
    )
}

/// Receive-side decision for one peer digest. Given the LOCAL digest, the
/// PEER's digest (off the wire frame), the sender's id, and the requester's
/// own [`RequesterIdentity`], return `Some((destination, request))` when the
/// local replica is behind the peer and should pull a snapshot — or `None`
/// when the replicas are converged (the self-quiescing case: no pull on a
/// matching digest).
///
/// Pull target = the PROVEN-AHEAD SENDER (AE-6 / A1). Whenever
/// [`StateDigest::is_behind`] fires, the digest IS the proof that THIS
/// SENDER holds ledger data the local replica lacks, so the snapshot is
/// pulled from the sender directly — [`Destination::Secondary`] of its id —
/// regardless of which node (if any) is the recognised primary. There is
/// NO primary fallback: a primary that is itself lagging (e.g. mid-handoff,
/// or partitioned from a mutation a non-primary peer already saw) does NOT
/// hold the missing data, so routing the pull to it would heal nothing,
/// while a non-primary peer that is ahead MUST be pulled. The sender's
/// digest already discriminates ahead-from-behind (a behind sender's frame
/// makes `is_behind` return `false` → early return), so the proven-ahead
/// sender is always the authoritative responder. `restore` is idempotent,
/// so a redundant pull is harmless; no routability fallback is needed.
///
/// The pull TARGET does NOT depend on the recognised primary (the proven-
/// ahead sender is authoritative regardless of role), so the recognised
/// primary is not a parameter — a possibly-lagging primary is never the
/// fallback responder.
///
/// The pulled snapshot answers `RequestClusterSnapshot` with a
/// `ClusterSnapshot` the caller restores through the existing recv arm.
pub fn reconcile_against_peer<I>(
    local: &StateDigest,
    peer: &StateDigest,
    sender_id: &str,
    requester: &RequesterIdentity<'_>,
    timestamp: f64,
) -> Option<(Destination, DistributedMessage<I>)> {
    if !local.is_behind(peer) {
        // Converged on every field the peer reports — nothing to pull.
        // This is the steady-state path: a matching digest exchange costs
        // one comparison and zero round-trips. A sender that is itself
        // BEHIND us also lands here (its frame makes `is_behind` false),
        // so it is never selected as a pull target.
        return None;
    }
    // Pull from the proven-ahead SENDER (AE-6 / A1). Reaching here means
    // `is_behind(peer)` is `true`, i.e. THIS sender's digest proved it
    // holds data the local replica lacks; the sender is therefore the
    // authoritative responder regardless of role. A possibly-lagging
    // primary is NOT consulted — it may not hold the missing data — so
    // there is no primary fallback.
    let destination = Destination::Secondary(PeerId::from(sender_id.to_string()));
    let request = DistributedMessage::RequestClusterSnapshot {
        target: None,
        sender_id: requester.node_id.to_string(),
        timestamp,
        is_observer: requester.is_observer,
        can_be_primary: requester.can_be_primary,
    };
    Some((destination, request))
}

/// AE-3 recovery cadence — the TIMER-DRIVEN counterpart to
/// [`reconcile_against_peer`]. Where `reconcile_against_peer` is the
/// receive-side reaction to ONE inbound digest frame,
/// [`plan_recovery_pull`] runs on the role's own snapshot-recovery
/// interval with ZERO inbound traffic: given the local digest and the
/// LAST-SEEN digest of each KNOWN peer (the `current_primary` ∪ alive
/// secondaries roster), it decides whether the local replica is still
/// behind any of them and, if so, picks ONE rotating responder to pull a
/// fresh snapshot from. It exists so a snapshot whose decode was dropped
/// (WARN-and-keep — the steady-state decode arm never latches a stuck
/// observer) is RE-PULLED on the next tick until the replica converges.
///
/// # Single concern
///
/// The role-agnostic POLICY for the recovery cadence: the convergence
/// detection (`is_behind` against each known peer's last-seen digest), the
/// quiesce rule (C9), and the responder rotation. The caller owns ONLY the
/// `send_to` edge and the storage of `(peer_id, last_seen_digest)` it feeds
/// in; it re-implements none of the decision.
///
/// # C9 quiesce
///
/// Returns `None` (no pull) when the local replica is NOT behind ANY known
/// peer's last-seen digest — i.e. the converged-to-everything-it-has-seen
/// case. This is STRONGER than "non-empty AND decoded": a converged-but-
/// stale snapshot pulled from a lagging responder would still leave the
/// replica behind some OTHER known peer's digest, so the arm keeps re-
/// pulling until genuinely caught up. When no known peer has a recorded
/// digest yet, there is nothing to be behind, so it also quiesces.
///
/// # Different-responder rotation
///
/// The candidate list is SORTED by peer id (a total order) before the
/// cursor selects, so the rotation order is STABLE across ticks even though
/// `peer_digests` arrives from the caller's unordered membership source (a
/// `HashMap` iteration whose order may shuffle between ticks). Without the
/// sort the cursor could land back on the same bad responder next tick;
/// with it, `cursor` rotates through DISTINCT responders in a fixed order.
///
/// `cursor` is advanced on every call that has at least one candidate
/// responder (whether or not it is the one chosen this tick), so a
/// responder whose previous snapshot failed to decode — leaving the
/// replica STILL behind its digest, so it remains a candidate — is NOT
/// retried immediately: the next tick rotates to a different candidate.
/// One malformed responder therefore cannot wedge recovery while another
/// proven-ahead peer is reachable. With a single candidate the rotation
/// degenerates to re-pulling from it (the best available action).
///
/// `peer_digests` is the caller's `(known-peer-id, last-seen StateDigest)`
/// view — ALREADY intersected with the live roster by the caller, so this
/// function never consults membership itself (it owns no liveness concern).
/// The pull target is `Destination::Secondary(peer_id)` — the same snapshot
/// responder edge `reconcile_against_peer` pulls through (the responder
/// coordinator is selected at the INGRESS demux, not by this variant).
pub fn plan_recovery_pull<I>(
    local: &StateDigest,
    peer_digests: &[(String, StateDigest)],
    cursor: &mut usize,
    requester: &RequesterIdentity<'_>,
    timestamp: f64,
) -> Option<(Destination, DistributedMessage<I>)> {
    // Candidate responders = known peers whose last-seen digest proves they
    // hold ledger data the local replica lacks. A peer we are converged with
    // is not a candidate; if EVERY known peer is converged (or none has a
    // recorded digest), the candidate list is empty and we quiesce (C9).
    let mut candidates: Vec<&str> = peer_digests
        .iter()
        .filter(|(_, peer)| local.is_behind(peer))
        .map(|(id, _)| id.as_str())
        .collect();
    if candidates.is_empty() {
        return None;
    }
    // Impose a TOTAL ORDER on the candidate list (by peer id) before the
    // cursor selection. `peer_digests` arrives from the caller's
    // unordered membership source (a `HashMap` iteration), so without this
    // sort the candidate ORDER could shuffle between ticks and the cursor
    // would not reliably skip the SAME bad responder next tick — defeating
    // the wedge-prevention purpose of the rotation. Sorting by the stable
    // peer id makes `cursor` rotate through DISTINCT responders in a fixed
    // order across consecutive ticks.
    candidates.sort_unstable();
    // Pick the rotation-current candidate, then ADVANCE the cursor so the
    // next tick targets a different responder (the different-responder-on-
    // malformed rule). The modulo keeps the index in range as the candidate
    // set shrinks across ticks.
    let target = candidates[*cursor % candidates.len()];
    *cursor = cursor.wrapping_add(1);
    let destination = Destination::Secondary(PeerId::from(target.to_string()));
    let request = DistributedMessage::RequestClusterSnapshot {
        target: None,
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
    use std::collections::HashSet;

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
            reconcile_against_peer(&d, &d, "peer-1", &me, 1.0);
        assert!(decision.is_none());
    }

    /// AE-6 / A1: a node behind a peer pulls from the proven-ahead SENDER
    /// even when a (distinct) primary is recognised — the pull target is the
    /// sender whose digest proved it ahead, never a possibly-lagging primary.
    #[test]
    fn behind_with_primary_pulls_from_sender_not_primary() {
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
            reconcile_against_peer(&local, &peer, "peer-1", &me, 1.0)
                .expect("should pull when behind");
        // The proven-ahead sender, NOT `Destination::Primary`.
        assert_eq!(
            dst,
            Destination::Secondary(PeerId::from("peer-1".to_string()))
        );
        assert_ne!(dst, Destination::Primary);
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
            reconcile_against_peer(&local, &peer, "peer-7", &me, 1.0)
                .expect("should pull when behind");
        assert_eq!(
            dst,
            Destination::Secondary(PeerId::from("peer-7".to_string()))
        );
    }

    /// AE-6 / A1 (§5.3 test 15): the lagging-primary scenario. A node is
    /// behind a NON-PRIMARY peer's digest (the peer saw a mutation the node
    /// lacks) while the recognised primary is itself lagging (it has NOT seen
    /// that mutation either). The pull MUST target the ahead non-primary
    /// SENDER, not the lagging primary — routing to the primary would heal
    /// nothing because the primary does not hold the missing data. The target
    /// selection no longer even consults a recognised-primary param (it was
    /// dropped); this test pins that the proven-ahead SENDER is the target.
    #[test]
    fn pull_targets_proven_ahead_sender_not_lagging_primary() {
        // The local replica and a (notional) lagging primary share the SAME
        // stale digest shape; only the non-primary sender is ahead.
        let local = StateDigest {
            tasks_count: 2,
            tasks_hash: 0x1111,
            ..Default::default()
        };
        // The ahead non-primary peer holds one more task entry.
        let ahead_sender = StateDigest {
            tasks_count: 3,
            tasks_hash: 0x2222,
            ..Default::default()
        };
        let me = RequesterIdentity {
            node_id: "me",
            is_observer: false,
            can_be_primary: true,
        };
        let (dst, _req): (Destination, DistributedMessage<u32>) =
            reconcile_against_peer(&local, &ahead_sender, "ahead-secondary", &me, 1.0)
                .expect("should pull when behind the ahead non-primary sender");
        assert_eq!(
            dst,
            Destination::Secondary(PeerId::from("ahead-secondary".to_string())),
            "must pull from the proven-ahead non-primary sender"
        );
        assert_ne!(
            dst,
            Destination::Primary,
            "must NOT pull from the lagging primary"
        );
    }

    /// AE-6 / A1 (§5.3 test 15, A2 sub-case): the PRIMARY-caller failover
    /// path. The local node IS the recognised primary (freshly promoted,
    /// still warming its mirror) and is BEHIND a secondary's digest. It must
    /// still pull from that proven-ahead peer — the old `Some(_) =>
    /// Destination::Primary` would have routed the request to ITSELF and
    /// healed nothing. Now that the recognised-primary param is gone the
    /// target is the proven-ahead sender by construction; this pins it.
    #[test]
    fn freshly_promoted_primary_behind_peer_pulls_from_peer() {
        let local = StateDigest::default();
        let ahead_peer = StateDigest {
            tasks_count: 4,
            tasks_hash: 0x4444,
            ..Default::default()
        };
        // The local node is the (freshly-promoted, still-lagging) primary;
        // it must pull from the proven-ahead peer, not itself.
        let me = RequesterIdentity {
            node_id: "me",
            is_observer: false,
            can_be_primary: true,
        };
        let (dst, req): (Destination, DistributedMessage<u32>) =
            reconcile_against_peer(&local, &ahead_peer, "ahead-secondary", &me, 1.0)
                .expect("a behind primary must still pull from the ahead peer");
        assert_eq!(
            dst,
            Destination::Secondary(PeerId::from("ahead-secondary".to_string())),
            "the promoted-but-lagging primary must pull from the proven-ahead peer, not itself"
        );
        assert_ne!(dst, Destination::Primary);
        match req {
            DistributedMessage::RequestClusterSnapshot { sender_id, .. } => {
                assert_eq!(sender_id, "me");
            }
            _ => panic!("expected RequestClusterSnapshot"),
        }
    }

    fn requester() -> RequesterIdentity<'static> {
        RequesterIdentity {
            node_id: "obs",
            is_observer: true,
            can_be_primary: false,
        }
    }

    fn ahead(tasks: u64, hash: u64) -> StateDigest {
        StateDigest {
            tasks_count: tasks,
            tasks_hash: hash,
            ..Default::default()
        }
    }

    /// C9 quiesce: an empty peer-digest view (no known peer has reported a
    /// digest yet) yields NO recovery pull — there is nothing to be behind.
    #[test]
    fn recovery_quiesces_with_no_known_peer_digests() {
        let local = StateDigest::default();
        let mut cursor = 0usize;
        let decision: Option<(Destination, DistributedMessage<u32>)> =
            plan_recovery_pull(&local, &[], &mut cursor, &requester(), 1.0);
        assert!(decision.is_none());
        assert_eq!(cursor, 0, "no candidate ⇒ cursor untouched");
    }

    /// C9 quiesce: converged with EVERY known peer (none is ahead) ⇒ no pull.
    #[test]
    fn recovery_quiesces_when_converged_with_all_known_peers() {
        let d = ahead(3, 0xABCD);
        let peers = vec![("peer-1".to_string(), d), ("peer-2".to_string(), d)];
        let mut cursor = 0usize;
        let decision: Option<(Destination, DistributedMessage<u32>)> =
            plan_recovery_pull(&d, &peers, &mut cursor, &requester(), 1.0);
        assert!(decision.is_none(), "behind nobody ⇒ quiesce");
        assert_eq!(cursor, 0);
    }

    /// Behind a known peer ⇒ pull from it via `Destination::Secondary`, and
    /// the cursor advances (different-responder rotation).
    #[test]
    fn recovery_pulls_from_behind_peer_and_advances_cursor() {
        let local = StateDigest::default();
        let peers = vec![("peer-ahead".to_string(), ahead(2, 0x9))];
        let mut cursor = 0usize;
        let (dst, req): (Destination, DistributedMessage<u32>) =
            plan_recovery_pull(&local, &peers, &mut cursor, &requester(), 1.0)
                .expect("behind a known peer ⇒ pull");
        assert_eq!(
            dst,
            Destination::Secondary(PeerId::from("peer-ahead".to_string()))
        );
        assert_eq!(cursor, 1, "cursor advances so the next tick rotates");
        match req {
            DistributedMessage::RequestClusterSnapshot {
                sender_id,
                is_observer,
                can_be_primary,
                ..
            } => {
                assert_eq!(sender_id, "obs");
                assert!(is_observer);
                assert!(!can_be_primary);
            }
            _ => panic!("expected RequestClusterSnapshot"),
        }
    }

    /// Different-responder rotation: three ahead peers fed in a NON-sorted
    /// insertion order. Consecutive ticks must rotate through DISTINCT
    /// responders in a DETERMINISTIC (sorted-by-id) order so a single
    /// malformed responder cannot wedge recovery — and the order must be
    /// stable across ticks even though the caller's membership source is
    /// unordered (the candidate list is sorted before the cursor selects).
    #[test]
    fn recovery_rotates_responder_across_ticks() {
        let local = StateDigest::default();
        // All three peers are ahead (each holds a distinct task set the local
        // replica lacks), so all three stay candidates across ticks. The
        // insertion order is deliberately NOT sorted to prove the rotation
        // does not depend on the input order.
        let peers = vec![
            ("peer-c".to_string(), ahead(1, 0x3)),
            ("peer-a".to_string(), ahead(1, 0x1)),
            ("peer-b".to_string(), ahead(1, 0x2)),
        ];
        let target_id = |peers: &[(String, StateDigest)], cursor: &mut usize| -> String {
            let (dst, _): (Destination, DistributedMessage<u32>) =
                plan_recovery_pull(&local, peers, cursor, &requester(), 1.0)
                    .expect("still behind ⇒ pull");
            match dst {
                Destination::Secondary(p) => p.into_string(),
                other => panic!("expected Destination::Secondary, got {other:?}"),
            }
        };
        // Six consecutive ticks: the rotation must visit the candidates in
        // sorted-by-id order (a → b → c) and then wrap, deterministically.
        let mut cursor = 0usize;
        let seq: Vec<String> = (0..6).map(|_| target_id(&peers, &mut cursor)).collect();
        assert_eq!(
            seq,
            vec!["peer-a", "peer-b", "peer-c", "peer-a", "peer-b", "peer-c"],
            "rotation must visit DISTINCT responders in a deterministic \
             sorted-by-id order and wrap (the wedge-prevention guarantee)"
        );
        // Within one full cycle every candidate is visited exactly once — no
        // responder is skipped or repeated before the others are tried.
        let one_cycle: HashSet<&String> = seq[..3].iter().collect();
        assert_eq!(
            one_cycle.len(),
            3,
            "the first full cycle must cover all three distinct responders"
        );
        // Reproducible: a fresh cursor over the SAME (unordered) input yields
        // the SAME sequence — the order is a function of the peer ids, not of
        // HashMap iteration nondeterminism.
        let mut cursor2 = 0usize;
        let seq2: Vec<String> = (0..6).map(|_| target_id(&peers, &mut cursor2)).collect();
        assert_eq!(seq, seq2, "rotation order must be reproducible across runs");
    }
}
