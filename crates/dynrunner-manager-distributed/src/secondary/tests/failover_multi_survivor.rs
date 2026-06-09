//! PROBE: the multi-survivor failover wedge.
//!
//! A 4-node fleet (primary `secondary-0` relocated onto a compute node +
//! 3 worker-secondaries `secondary-1/2/3`) loses the primary. The 3 survivors
//! must elect: the lex-lowest live survivor (`secondary-1`) self-leads,
//! broadcasts a `PromotionVote`, the other two confirm, and `secondary-1`
//! reaches `Promoted`.
//!
//! These tests drive the FULL handshake for `secondary-1` (the candidate)
//! against the `MembershipControlPeer` harness, injecting the peers'
//! `TimeoutResponse` / `PromotionConfirm` the way the live mesh would deliver
//! them, and model the MEMBERSHIP DIVERGENCE observed on-cluster (the dead
//! primary lingers in some peers' transport view but not others).

#![cfg(test)]

use std::time::{Duration, Instant};

use dynrunner_protocol_primary_secondary::address::PeerId;
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, DistributedMessage, PrimaryChangeReason,
};

use super::super::election::ElectionState;
use super::super::test_helpers::{
    MembershipControlPeer, SecondaryHarness, election_config, make_secondary_membership,
};

const PRIMARY_ID: &str = "secondary-0";
const SELF_ID: &str = "secondary-1";
const PEER_2: &str = "secondary-2";
const PEER_3: &str = "secondary-3";

/// `keepalive_interval` (50ms) from `election_config` — the Suspecting gather
/// window the tally waits before counting.
const KEEPALIVE: Duration = Duration::from_millis(50);

/// Bring `secondary-1` into `Operational` as the lex-lowest survivor of a
/// 4-node fleet whose primary (`secondary-0`) just died:
///   - `current_primary` = `secondary-0` (applied `PrimaryChanged`),
///   - two live peers (`secondary-2`, `secondary-3`) in `peer_keepalives`,
///   - one observed primary message (leg-(C) `primary_last_seen.is_some()`),
///   - the dead primary REMOVED from this node's transport membership
///     (`primary_left_membership = true` — the on-cluster `secondary-1` view).
///
/// Initial membership holds {primary, peer-2, peer-3}; the death event removes
/// the primary.
fn candidate_after_primary_death() -> (
    SecondaryHarness<MembershipControlPeer>,
    std::rc::Rc<std::cell::RefCell<Vec<PeerId>>>,
) {
    let (mut sec, members) = make_secondary_membership(
        election_config(SELF_ID),
        vec![
            PeerId::from(PRIMARY_ID),
            PeerId::from(PEER_2),
            PeerId::from(PEER_3),
        ],
    );
    sec.enter_operational_for_test();
    let now = Instant::now();
    sec.op_mut().peer_keepalives.insert(PEER_2.into(), now);
    sec.op_mut().peer_keepalives.insert(PEER_3.into(), now);
    sec.cluster_state.apply(ClusterMutation::PrimaryChanged {
        new: PRIMARY_ID.into(),
        epoch: 1,
        reason: PrimaryChangeReason::Election,
    });
    sec.publish_membership();
    sec.record_primary_message();
    // The primary DIES: removed from this survivor's transport membership.
    members
        .borrow_mut()
        .retain(|id| id != &PeerId::from(PRIMARY_ID));
    sec.publish_membership();
    (sec, members)
}

/// THE WEDGE PROBE: the lex-lowest survivor must reach `Promoted` once quorum
/// agrees and the peers confirm. Drives the full handshake; if it wedges
/// short of `Promoted` this test fails — pinning the stall point.
#[tokio::test(flavor = "current_thread")]
async fn lowest_survivor_promotes_after_quorum_and_confirms() {
    let (mut sec, _members) = candidate_after_primary_death();

    // Tick 1: arm the election (leg C — primary left membership, seen before).
    let actions = sec.run_election_tick();
    assert!(
        matches!(sec.op_mut().election, ElectionState::Suspecting { .. }),
        "tick 1 must arm Suspecting via leg (C); got {:?}",
        std::mem::discriminant(&sec.op_mut().election),
    );
    assert!(
        actions
            .broadcast
            .iter()
            .any(|m| matches!(m, DistributedMessage::TimeoutQuery { .. })),
        "Suspecting broadcasts a TimeoutQuery",
    );

    // The peers reply: each last saw the dead primary well past the death
    // deadline (they agree it is silent). Model the divergence — these peers
    // still saw the primary alive in their TRANSPORT view, but their
    // peer_keepalives age for the primary still exceeds the deadline because
    // the primary genuinely stopped emitting keepalives.
    sec.record_timeout_response(PEER_2.into(), Some(10.0));
    sec.record_timeout_response(PEER_3.into(), Some(10.0));

    // Wait the gather window, then tick again to tally.
    tokio::time::sleep(KEEPALIVE + Duration::from_millis(5)).await;
    let actions = sec.run_election_tick();

    // Expected: quorum met (self + 2 peers ≥ failover_quorum(2)=2), we are
    // lex-lowest → self-lead → broadcast PromotionVote, enter Candidate.
    let voted = actions
        .broadcast
        .iter()
        .any(|m| matches!(m, DistributedMessage::PromotionVote { candidate_id, .. } if candidate_id == SELF_ID));
    let state_disc = std::mem::discriminant(&sec.op_mut().election);
    assert!(
        matches!(
            sec.op_mut().election,
            ElectionState::Candidate { .. } | ElectionState::Promoted
        ),
        "after quorum the lex-lowest survivor must become Candidate (or Promoted); got {state_disc:?}",
    );
    assert!(
        voted || actions.promoted,
        "the candidate must broadcast a PromotionVote (or self-promote)",
    );

    // The peers confirm the candidate; the second confirm crosses quorum.
    let _ = sec.record_promotion_confirm(PEER_2.into(), SELF_ID.into(), 1);
    let promoted = sec.record_promotion_confirm(PEER_3.into(), SELF_ID.into(), 1);
    assert!(
        promoted || matches!(sec.op_mut().election, ElectionState::Promoted),
        "majority confirms must promote the candidate to Promoted",
    );
}

/// Bring a DEFERRER (`secondary-2`) into `Operational` modelling the
/// on-cluster divergence: the dead primary `secondary-0` is STILL a connected
/// transport member in this peer's view (`primary_left_membership = false`),
/// AND this peer last saw a primary message recently — so on its OWN view the
/// primary still looks alive. Live peers: the candidate `secondary-1` + the
/// third survivor `secondary-3`.
fn deferrer_with_primary_still_present(self_id: &str) -> SecondaryHarness<MembershipControlPeer> {
    let (mut sec, _members) = make_secondary_membership(
        election_config(self_id),
        vec![
            PeerId::from(PRIMARY_ID),
            PeerId::from(SELF_ID),
            PeerId::from(PEER_2),
            PeerId::from(PEER_3),
        ],
    );
    sec.enter_operational_for_test();
    let now = Instant::now();
    // The candidate + the other survivor are live peers in this deferrer's view.
    sec.op_mut().peer_keepalives.insert(SELF_ID.into(), now);
    sec.op_mut().peer_keepalives.insert(PEER_3.into(), now);
    sec.cluster_state.apply(ClusterMutation::PrimaryChanged {
        new: PRIMARY_ID.into(),
        epoch: 1,
        reason: PrimaryChangeReason::Election,
    });
    sec.publish_membership();
    // This deferrer last saw the primary RECENTLY — its primary_last_seen is
    // fresh (the abrupt-crash divergence: it has not yet observed the death).
    sec.record_primary_message();
    sec
}

/// Deferrer-side contract under divergence: a peer that has NOT yet observed
/// the primary's death (fresh `primary_last_seen`, primary still in its view)
/// correctly REFUSES to confirm the candidate's `PromotionVote`
/// (`record_promotion_vote` returns `None`). Confirming blindly here would let
/// a spurious election win against a still-alive primary — the safety the
/// beacon-union / split-brain guard protect. Convergence is NOT this peer's
/// job: the candidate re-polls (see `candidate_repolls_until_divergent_peers_agree`)
/// and gathers quorum from the peers that HAVE observed the death; this peer
/// confirms only once it independently observes the death too.
#[tokio::test(flavor = "current_thread")]
async fn deferrer_refuses_until_it_observes_death_then_confirms() {
    let mut def = deferrer_with_primary_still_present(PEER_2);

    // The candidate broadcasts its PromotionVote (round 1). While THIS peer
    // still sees the primary fresh, it must REFUSE — independent
    // death-observation is required before lending a confirm (safety).
    let reply = def.record_promotion_vote(SELF_ID.into(), 1);
    assert!(
        reply.is_none(),
        "a peer that has not observed the primary's death must NOT confirm \
         (split-brain safety); got a confirm",
    );

    // This peer now observes the death: its `primary_last_seen` ages past the
    // deadline. A re-sent PromotionVote (the candidate keeps campaigning) is
    // now confirmed — convergence WITHOUT ever confirming against a live
    // primary.
    def.op_mut().primary_last_seen = Some(Instant::now() - Duration::from_secs(10));
    let reply = def.record_promotion_vote(SELF_ID.into(), 1);
    assert!(
        matches!(reply, Some(DistributedMessage::PromotionConfirm { ref new_primary_id, .. }) if new_primary_id == SELF_ID),
        "once the primary is observed dead, the deferrer confirms the agreed \
         lowest-id candidate; got {reply:?}",
    );
}

/// Count the `TimeoutQuery` frames in a tick's broadcast set — the candidate's
/// re-poll signal.
fn timeout_queries(
    actions: &super::super::election::ElectionTickActions<super::super::test_helpers::TestId>,
) -> usize {
    actions
        .broadcast
        .iter()
        .filter(|m| matches!(m, DistributedMessage::TimeoutQuery { .. }))
        .count()
}

/// THE PERSISTENT WEDGE (the 6.5-min on-cluster symptom) + its fix: under
/// abrupt-crash eviction-timing divergence a peer's FIRST `TimeoutResponse`
/// arrives reporting a NON-stale age (that peer had not yet observed the
/// primary's death). Pre-fix the candidate broadcast its `TimeoutQuery`
/// exactly ONCE on entering Suspecting, cached that single disagreeing answer
/// forever (`record_timeout_response` only overwrites on a NEW response, and
/// none ever came), pinned `agreeing` below quorum, and wedged in Suspecting
/// at round 1 — never advancing, never self-promoting (the exact on-cluster
/// "round=1, 0 self-promoting" symptom).
///
/// The fix re-emits the `TimeoutQuery` on every waiting Suspecting tick, so the
/// peers re-answer with their CURRENT view; once they observe the death their
/// refreshed (agreeing) response replaces the stale one and quorum converges.
/// This test drives that loop: stale-first responses, then a re-poll, then the
/// peers' refreshed agreeing responses — and asserts the candidate advances.
#[tokio::test(flavor = "current_thread")]
async fn candidate_repolls_until_divergent_peers_agree() {
    let (mut sec, _members) = candidate_after_primary_death();

    // Tick 1: arm Suspecting, broadcast the first TimeoutQuery.
    let actions = sec.run_election_tick();
    assert!(
        matches!(sec.op_mut().election, ElectionState::Suspecting { .. }),
        "tick 1 arms Suspecting",
    );
    assert_eq!(
        timeout_queries(&actions),
        1,
        "Suspecting emits the first query"
    );

    // The peers' FIRST responses arrive while THEY still see the primary fresh
    // (divergent eviction timing): each reports a small age, under the ~100ms
    // death deadline — "the primary still looks alive to me".
    sec.record_timeout_response(PEER_2.into(), Some(0.001));
    sec.record_timeout_response(PEER_3.into(), Some(0.001));

    // Gather window elapses; tick again. Quorum is NOT met on the stale
    // answers — but the candidate must RE-POLL (re-broadcast TimeoutQuery)
    // rather than cache the stale disagreement.
    tokio::time::sleep(KEEPALIVE + Duration::from_millis(5)).await;
    let actions = sec.run_election_tick();
    assert!(
        matches!(sec.op_mut().election, ElectionState::Suspecting { .. }),
        "still Suspecting — quorum not yet met on the stale answers",
    );
    assert_eq!(
        timeout_queries(&actions),
        1,
        "PERSISTENT-WEDGE FIX: a candidate short of quorum must RE-POLL, not \
         cache the single stale disagreement forever",
    );

    // The peers have now observed the primary's death and answer the re-poll
    // with a STALE age (> deadline) — they agree. `record_timeout_response`
    // overwrites their cached entries.
    sec.record_timeout_response(PEER_2.into(), Some(10.0));
    sec.record_timeout_response(PEER_3.into(), Some(10.0));

    // Next tick tallies the refreshed responses: quorum met, we are
    // lex-lowest → advance to Candidate (or straight to Promoted).
    let actions = sec.run_election_tick();
    assert!(
        matches!(
            sec.op_mut().election,
            ElectionState::Candidate { .. } | ElectionState::Promoted
        ),
        "with the peers' refreshed agreeing responses the candidate must \
         advance past Suspecting; got {:?}",
        std::mem::discriminant(&sec.op_mut().election),
    );
    assert!(
        actions
            .broadcast
            .iter()
            .any(|m| matches!(m, DistributedMessage::PromotionVote { candidate_id, .. } if candidate_id == SELF_ID))
            || actions.promoted,
        "the advancing candidate broadcasts a PromotionVote (or self-promotes)",
    );
}
