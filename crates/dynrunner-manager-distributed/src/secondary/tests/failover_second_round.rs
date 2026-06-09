//! SECOND-failover convergence: candidate selection must exclude a peer that
//! is DEAD-but-still-lingering in `peer_keepalives` (the 300s reaper window),
//! exactly as the failover-quorum DENOMINATOR does.
//!
//! THE BUG (consumer asm-dataset, live SLURM, run_133345): a 4-node fleet
//! survives the FIRST failover (kill the relocated primary `secondary-0`;
//! `secondary-1` promotes) but WEDGES on the SECOND (kill the promoted primary
//! `secondary-1`): the survivors `secondary-2`/`secondary-3` detect the death
//! and enter Suspecting, then DEFER to `candidate=secondary-0` — the FIRST
//! failover's DEAD primary, still "live" in `peer_keepalives` because the
//! conservative `peer_timeout` (300s prod) reaper has not yet evicted it. They
//! defer to a corpse, never broadcast their OWN `PromotionVote`, and wedge for
//! the whole ~300s reaper window. (Why 4->3 worked: lowest-id-excluding-dead-s0
//! was `secondary-1`, which was LIVE.)
//!
//! ROOT: candidate selection reads `live_peers` snapshotted from
//! `live_peer_ids()` — the SAME function the lone-survivor fix (5d73f3b4) made
//! intersect the live transport `MembershipView` (`client.has_peer`). That one
//! seam feeds BOTH the quorum denominator AND candidate selection, so the
//! has_peer intersection already drops a dead-but-lingering peer (at the
//! ~QUIC-idle membership departure, not the 300s reaper) from the CANDIDATE
//! SET too: the survivor stops deferring to the corpse and self-leads.
//!
//! These tests drive the REAL path via a `MembershipControlPeer`: the dead
//! first-primary `secondary-0` is ABSENT from membership (its QUIC connection
//! tore down) yet STILL FRESH in `peer_keepalives` (peer_timeout not reached),
//! then the promoted primary `secondary-1` dies. The survivor `secondary-2`
//! (lex-lower than the live peer `secondary-3`) must NOT defer to `secondary-0`;
//! it self-leads and, with the peer's confirm, reaches `Promoted`.

#![cfg(test)]

use std::time::{Duration, Instant};

use dynrunner_protocol_primary_secondary::address::PeerId;
use dynrunner_protocol_primary_secondary::{ClusterMutation, DistributedMessage, PrimaryChangeReason};

use super::super::election::ElectionState;
use super::super::test_helpers::{
    MembershipControlPeer, SecondaryHarness, election_config, make_secondary_membership,
};

/// The FIRST failover's dead primary — lex-LOWEST id, the trap: a naive
/// lowest-live-id candidate selection that reads `peer_keepalives` alone would
/// pick this corpse.
const DEAD_FIRST_PRIMARY: &str = "secondary-0";
/// The promoted (second) primary that now dies — the role being failed-over FROM.
const PROMOTED_PRIMARY: &str = "secondary-1";
/// This survivor — lex-lower than the live peer, so it self-leads ONCE the dead
/// `secondary-0` is correctly excluded from the candidate set.
const SELF_ID: &str = "secondary-2";
/// The other live survivor — confirms the candidate.
const LIVE_PEER: &str = "secondary-3";

/// `keepalive_interval` (50ms) from `election_config` — the Suspecting gather
/// window the tally waits before counting.
const KEEPALIVE: Duration = Duration::from_millis(50);

/// Bring `secondary-2` into `Operational` AFTER the first failover, at the
/// instant the SECOND failover begins:
///   - `current_primary` = `secondary-1` (the promoted primary, applied at
///     epoch 2 over the first-failover's epoch-1 `PrimaryChanged`),
///   - `peer_keepalives` holds BOTH the dead first-primary `secondary-0` (its
///     entry lingers — the 300s reaper has not fired) AND the live peer
///     `secondary-3`,
///   - transport membership holds ONLY the live peer `secondary-3`: the dead
///     `secondary-0` and the about-to-die `secondary-1` are ABSENT (their QUIC
///     connections tore down),
///   - one observed primary message (leg-(C) `primary_last_seen.is_some()`),
///   - `mesh.degraded == false` (the mesh DID form).
///
/// Returns the harness + the membership handle so the test can drive the
/// promoted primary leaving membership (the SECOND death event).
fn survivor_after_first_failover() -> (
    SecondaryHarness<MembershipControlPeer>,
    std::rc::Rc<std::cell::RefCell<Vec<PeerId>>>,
) {
    // Membership at the moment the second failover begins: the dead first
    // primary secondary-0 is already GONE from transport (first-failover kill),
    // the promoted primary secondary-1 is still present (about to die), and the
    // live peer secondary-3 is present.
    let (mut sec, members) = make_secondary_membership(
        election_config(SELF_ID),
        vec![PeerId::from(PROMOTED_PRIMARY), PeerId::from(LIVE_PEER)],
    );
    sec.enter_operational_for_test();
    sec.mesh.degraded = false;
    let now = Instant::now();
    // The CORPSE: secondary-0 lingers in peer_keepalives (300s reaper unfired)
    // even though it left transport membership at the first failover. This is
    // the trap entry that a peer_keepalives-only candidate selection would pick.
    sec.op_mut()
        .peer_keepalives
        .insert(DEAD_FIRST_PRIMARY.into(), now);
    // The promoted primary (a same-peer primary+secondary host emits a Secondary
    // keepalive too) and the live peer are recent live entries.
    sec.op_mut()
        .peer_keepalives
        .insert(PROMOTED_PRIMARY.into(), now);
    sec.op_mut().peer_keepalives.insert(LIVE_PEER.into(), now);
    // First failover applied epoch-1 (secondary-0), then the promotion applied
    // epoch-2 (secondary-1). current_primary is now secondary-1.
    sec.cluster_state.apply(ClusterMutation::PrimaryChanged {
        new: DEAD_FIRST_PRIMARY.into(),
        epoch: 1,
        reason: PrimaryChangeReason::Election,
    });
    sec.cluster_state.apply(ClusterMutation::PrimaryChanged {
        new: PROMOTED_PRIMARY.into(),
        epoch: 2,
        reason: PrimaryChangeReason::Election,
    });
    sec.publish_membership();
    sec.record_primary_message();
    (sec, members)
}

/// THE SECOND-FAILOVER WEDGE PROBE + FIX. The promoted primary `secondary-1`
/// dies (leaves membership). The survivor `secondary-2` must NOT defer to the
/// dead first-primary `secondary-0` (gone from membership but lingering in
/// `peer_keepalives`); it must self-lead and, with the live peer's confirm,
/// reach `Promoted`.
///
/// REVERT CHECK: pre-(a) `live_peer_ids` read only `peer_keepalives`, so
/// `live_peers` still contained `secondary-0`; `lowest_alive` =
/// min(secondary-0, secondary-2, secondary-3) = `secondary-0` (a corpse) →
/// `we_lead == false` → the survivor entered `Voting { candidate: secondary-0 }`
/// and broadcast NO `PromotionVote`, wedging until the 300s reaper finally
/// evicted `secondary-0`.
#[tokio::test(flavor = "current_thread")]
async fn survivor_does_not_defer_to_dead_first_primary() {
    let (mut sec, members) = survivor_after_first_failover();

    // Pre-second-kill sanity: the candidate-relevant live-peer set already
    // EXCLUDES the dead secondary-0 (absent from membership) AND the current
    // primary secondary-1 (excluded as the failed-over-from role). Only the live
    // peer secondary-3 remains — proving the has_peer intersection drops the
    // corpse from the SAME set candidate selection reads.
    assert_eq!(
        sec.live_peer_ids().cloned().collect::<Vec<_>>(),
        vec![LIVE_PEER.to_string()],
        "the dead first-primary (gone from membership, lingering in \
         peer_keepalives) must be ABSENT from the live-peer set candidate \
         selection reads; got {:?}",
        sec.live_peer_ids().cloned().collect::<Vec<_>>(),
    );

    // The SECOND death: the promoted primary secondary-1 dies — its QUIC
    // connection tears down, so the mesh-pump republishes membership WITHOUT it.
    // peer_keepalives still holds it (peer_timeout 120s not reached).
    members
        .borrow_mut()
        .retain(|id| id != &PeerId::from(PROMOTED_PRIMARY));
    sec.publish_membership();
    sec.check_peer_timeouts();

    // Tick 1: arm Suspecting (leg C — the promoted primary left membership).
    let actions = sec.run_election_tick();
    assert!(
        matches!(sec.op_mut().election, ElectionState::Suspecting { .. }),
        "the promoted-primary departure arms Suspecting; got {:?}",
        std::mem::discriminant(&sec.op_mut().election),
    );
    assert!(
        actions
            .broadcast
            .iter()
            .any(|m| matches!(m, DistributedMessage::TimeoutQuery { .. })),
        "Suspecting broadcasts a TimeoutQuery",
    );

    // The live peer agrees the primary is silent (age past the death deadline).
    sec.record_timeout_response(LIVE_PEER.into(), Some(10.0));

    // Gather window elapses; tally. live_peers = {secondary-3}; quorum =
    // failover_quorum(1) = 2, met by self + the live peer's agreeing reply.
    // lowest_alive = min(secondary-3, secondary-2) = secondary-2 (US) — NOT the
    // dead secondary-0 — so we SELF-LEAD and broadcast our own PromotionVote.
    tokio::time::sleep(KEEPALIVE + Duration::from_millis(5)).await;
    sec.check_peer_timeouts();
    let actions = sec.run_election_tick();

    assert!(
        actions
            .broadcast
            .iter()
            .any(|m| matches!(m, DistributedMessage::PromotionVote { candidate_id, .. } if candidate_id == SELF_ID)),
        "the survivor must SELF-LEAD (broadcast its OWN PromotionVote), NOT \
         defer to the dead first-primary secondary-0; broadcast = {:?}",
        actions.broadcast,
    );
    assert!(
        !matches!(
            sec.op_mut().election,
            ElectionState::Voting { ref candidate, .. } if candidate == DEAD_FIRST_PRIMARY
        ),
        "the survivor must NEVER enter Voting deferring to the dead first-primary",
    );
    assert!(
        matches!(
            sec.op_mut().election,
            ElectionState::Candidate { .. } | ElectionState::Promoted
        ),
        "after self-leading on met quorum the survivor is Candidate (or Promoted); got {:?}",
        std::mem::discriminant(&sec.op_mut().election),
    );

    // The live peer confirms; that confirm crosses quorum (2) → Promoted.
    let promoted = sec.record_promotion_confirm(LIVE_PEER.into(), SELF_ID.into(), 1);
    assert!(
        promoted || matches!(sec.op_mut().election, ElectionState::Promoted),
        "the live peer's confirm must promote the self-led candidate to Promoted",
    );
    assert!(sec.fatal_exit.is_none(), "a meshed survivor never degraded-bails");
}

/// CONTROL / no-over-exclude: a peer that IS still a connected mesh member must
/// remain a candidate. Here the dead first-primary `secondary-0` is RESTORED to
/// membership (modelling a still-live lowest-id peer) before the second
/// failover; the survivor must then CORRECTLY defer to it (it is the genuine
/// lowest-live-id), proving the has_peer intersection drops ONLY departed peers
/// and never a still-present lower-id candidate.
#[tokio::test(flavor = "current_thread")]
async fn survivor_defers_to_lowest_id_peer_that_is_still_a_member() {
    let (mut sec, members) = survivor_after_first_failover();

    // secondary-0 is actually STILL ALIVE (a connected member) — restore it to
    // transport membership. It is genuinely the lowest-live-id candidate now.
    members.borrow_mut().push(PeerId::from(DEAD_FIRST_PRIMARY));
    sec.publish_membership();

    // The promoted primary secondary-1 dies.
    members
        .borrow_mut()
        .retain(|id| id != &PeerId::from(PROMOTED_PRIMARY));
    sec.publish_membership();
    sec.check_peer_timeouts();

    // Both live non-primary peers (secondary-0, secondary-3) are in the set.
    let mut live: Vec<String> = sec.live_peer_ids().cloned().collect();
    live.sort();
    assert_eq!(
        live,
        vec![DEAD_FIRST_PRIMARY.to_string(), LIVE_PEER.to_string()],
        "a peer still in membership remains a live candidate",
    );

    // Arm + tally. The live peers agree. lowest_alive =
    // min(secondary-0, secondary-3, secondary-2) = secondary-0 → DEFER to it.
    let _ = sec.run_election_tick();
    sec.record_timeout_response(DEAD_FIRST_PRIMARY.into(), Some(10.0));
    sec.record_timeout_response(LIVE_PEER.into(), Some(10.0));
    tokio::time::sleep(KEEPALIVE + Duration::from_millis(5)).await;
    sec.check_peer_timeouts();
    let actions = sec.run_election_tick();

    assert!(
        matches!(
            sec.op_mut().election,
            ElectionState::Voting { ref candidate, .. } if candidate == DEAD_FIRST_PRIMARY
        ),
        "when secondary-0 is a LIVE member it IS the genuine lowest-id candidate \
         — the survivor correctly defers to it (the intersection drops only \
         DEPARTED peers); got {:?}",
        std::mem::discriminant(&sec.op_mut().election),
    );
    assert!(
        !actions
            .broadcast
            .iter()
            .any(|m| matches!(m, DistributedMessage::PromotionVote { candidate_id, .. } if candidate_id == SELF_ID)),
        "deferring to a live lowest-id peer means NOT self-leading",
    );
}
