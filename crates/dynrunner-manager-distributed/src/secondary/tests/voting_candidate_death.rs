//! #361 — VOTING-state candidate-death re-evaluation on the election tick.
//!
//! The defect: `ElectionState::Voting` had NO tick arm in
//! `run_election_tick` — a voter that lent its vote to a candidate waited
//! indefinitely for an EXTERNAL event (another candidate's `PromotionVote`,
//! or a primary frame) to leave the state. If the CANDIDATE died
//! mid-election (not the primary — the #331 work covers the primary-death
//! observation), no such event ever arrives on a small fleet where the dead
//! candidate was the only one, so the election wedged forever.
//!
//! The fix mirrors the #331 shape: a per-tick Voting re-evaluation of the
//! CANDIDATE's liveness through the single-source seen-peer death-evidence
//! rule (`peer_death_observed` — membership departure ∩ beacon-silent ∩
//! the `peer_keepalives` seen-gate, self-excluded; the SAME rule the
//! `TimeoutQuery` responder reports on). On candidate-death evidence the
//! vote is RELEASED — Voting reverts to Suspecting — and the normal
//! election machinery re-runs: the departed candidate is no longer in
//! `live_peer_ids`, so a SURVIVOR emerges as the new candidate by
//! lowest-live-id (possibly this node alone, via the single-survivor
//! self-quorum).
//!
//! These tests pin:
//!   - HEADLINE: candidate membership-departure (beacon-silent) → the voter
//!     reverts to Suspecting (with the re-poll `TimeoutQuery`) and the
//!     surviving voter promotes on the next tally;
//!   - NO CHURN on a transient blip (departure + rejoin before the tick) —
//!     the observation is a live `has_peer` read, not a latch;
//!   - BEACON UNION: a membership-departed candidate whose
//!     transport-independent beacon still flows is NOT death-observed;
//!   - a LIVE candidate's election proceeds untouched (ticks leave Voting
//!     alone; the winner's `PrimaryChanged` completes it to Normal).

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
use crate::liveness::BeaconLiveness;

/// The dying primary — the failed-over-FROM role, excluded from quorum.
const PRIMARY_ID: &str = "primary-0";
/// The lex-lowest SURVIVOR — the candidate this voter lends its vote to.
const CANDIDATE_ID: &str = "sec-a";
/// THIS node — the voter.
const SELF_ID: &str = "sec-b";

/// `keepalive_interval` (50ms) from `election_config` — the Suspecting
/// gather window the tally waits before counting.
const KEEPALIVE: Duration = Duration::from_millis(50);

/// Bring the voter `sec-b` into `Voting { round: 1, candidate: sec-a }` the
/// way production gets there — through `record_promotion_vote` — at the
/// instant the primary dies:
///   - `current_primary` = `primary-0` (applied `PrimaryChanged`), one
///     observed primary message (seen-gate satisfied),
///   - `peer_keepalives` holds the candidate (fresh — the candidate's
///     seen-gate for `peer_death_observed`),
///   - transport membership initially {primary, candidate};
///     `mesh.degraded == false` (the mesh DID form),
///   - a beacon-liveness view installed with the writer handle returned, so
///     each test controls the candidate's beacon,
///   - the PRIMARY departs membership (the death that started the
///     election), then the candidate's `PromotionVote` arrives and this
///     node confirms → `Voting`.
///
/// Returns the harness + the membership handle + the beacon writer.
fn voter_in_voting() -> (
    SecondaryHarness<MembershipControlPeer>,
    std::rc::Rc<std::cell::RefCell<Vec<PeerId>>>,
    BeaconLiveness,
) {
    let (mut sec, members) = make_secondary_membership(
        election_config(SELF_ID),
        vec![PeerId::from(PRIMARY_ID), PeerId::from(CANDIDATE_ID)],
    );
    sec.enter_operational_for_test();
    sec.mesh.degraded = false;
    sec.op_mut()
        .peer_keepalives
        .insert(CANDIDATE_ID.into(), Instant::now());
    sec.cluster_state.apply(ClusterMutation::PrimaryChanged {
        new: PRIMARY_ID.into(),
        epoch: 1,
        reason: PrimaryChangeReason::Election,
    });
    sec.publish_membership();
    sec.record_primary_message();
    let beacon = BeaconLiveness::new();
    sec.set_beacon_liveness(beacon.clone());

    // The primary dies: its QUIC connection tears down, the mesh-pump
    // republishes membership WITHOUT it.
    depart(&mut sec, &members, PRIMARY_ID);

    // The candidate's PromotionVote arrives; this node's independent death
    // observation (the primary's membership departure) lets it confirm and
    // adopt the candidate — the production entry into Voting.
    let reply = sec.record_promotion_vote(CANDIDATE_ID.into(), 1);
    assert!(
        matches!(
            reply,
            Some(DistributedMessage::PromotionConfirm { ref new_primary_id, .. })
                if new_primary_id == CANDIDATE_ID
        ),
        "fixture: the voter confirms the candidate; got {reply:?}",
    );
    assert!(
        matches!(
            sec.op_mut().election,
            ElectionState::Voting { round: 1, ref candidate } if candidate == CANDIDATE_ID
        ),
        "fixture: the voter is in Voting for the candidate",
    );
    (sec, members, beacon)
}

/// Remove `id` from the live membership set and republish the view (the
/// test analogue of the mesh-pump's `handle_peer_disconnect` republish).
fn depart(
    sec: &mut SecondaryHarness<MembershipControlPeer>,
    members: &std::rc::Rc<std::cell::RefCell<Vec<PeerId>>>,
    id: &str,
) {
    members.borrow_mut().retain(|m| m != &PeerId::from(id));
    sec.publish_membership();
}

/// THE #361 HEADLINE: the candidate this voter lent its vote to DIES
/// (departs the transport membership, beacon silent). The next election
/// tick must RELEASE the vote — revert Voting → Suspecting, re-polling the
/// fleet about the still-dead primary — and the re-run election must
/// converge: with the dead candidate gone from `live_peer_ids`, this lone
/// survivor self-leads and promotes via the single-survivor self-quorum.
///
/// REVERT CHECK: pre-#361 `run_election_tick` had no Voting arm — the first
/// tick below left the state in Voting and no later tick could ever change
/// it (no external event arrives: the candidate is dead and so is the
/// primary), the exact wedge.
#[tokio::test(flavor = "current_thread")]
async fn voter_releases_vote_and_survivor_promotes_on_candidate_death() {
    let (mut sec, members, _beacon) = voter_in_voting();

    // The CANDIDATE dies: its QUIC connection tears down, the pump
    // republishes membership without it. Its beacon was never recorded, so
    // it is not fresh (a never-seen beacon never suppresses — #317 shape).
    depart(&mut sec, &members, CANDIDATE_ID);

    // Tick 1: the candidate-death observation fires — the vote is released
    // and the voter re-enters Suspecting, re-asking about the primary.
    let actions = sec.run_election_tick();
    assert!(
        matches!(sec.op_mut().election, ElectionState::Suspecting { .. }),
        "#361: a death-observed candidate must revert Voting → Suspecting; got {:?}",
        std::mem::discriminant(&sec.op_mut().election),
    );
    assert!(
        actions
            .broadcast
            .iter()
            .any(|m| matches!(m, DistributedMessage::TimeoutQuery { .. })),
        "the revert re-enters Suspecting with the entering-Suspecting re-poll",
    );
    assert!(sec.fatal_exit.is_none(), "a meshed survivor never degraded-bails");

    // The gather window elapses; tick 2 tallies: the dead candidate is no
    // longer in `live_peer_ids` (membership intersection), so the live
    // fleet is empty — quorum = failover_quorum(0) = 1, met by self alone —
    // and this survivor self-leads and commits the single-survivor
    // self-quorum promotion. The re-run election converges.
    tokio::time::sleep(KEEPALIVE + Duration::from_millis(10)).await;
    let actions = sec.run_election_tick();
    assert!(
        matches!(sec.op_mut().election, ElectionState::Promoted),
        "the re-run election must converge — the surviving voter promotes; got {:?}",
        std::mem::discriminant(&sec.op_mut().election),
    );
    assert!(
        actions.promoted,
        "the lone-survivor self-quorum commit drives the caller's terminal action",
    );
}

/// NO CHURN on a transient blip: the candidate departs and REJOINS before
/// the next tick. `has_peer` reads `true` again at the tick (the
/// observation is a LIVE read, not a latched "once departed, forever dead"
/// flag — the #331 shape), so the vote stays lent and the election
/// proceeds untouched.
#[tokio::test(flavor = "current_thread")]
async fn candidate_blip_with_rejoin_does_not_release_the_vote() {
    let (mut sec, members, _beacon) = voter_in_voting();

    // Blip: departure + quick rejoin (reconnect), each republished.
    depart(&mut sec, &members, CANDIDATE_ID);
    members.borrow_mut().push(PeerId::from(CANDIDATE_ID));
    sec.publish_membership();

    let actions = sec.run_election_tick();
    assert!(
        matches!(
            sec.op_mut().election,
            ElectionState::Voting { round: 1, ref candidate } if candidate == CANDIDATE_ID
        ),
        "after a departure+rejoin blip the candidate is a member again — the \
         vote stays lent (no churn); got {:?}",
        std::mem::discriminant(&sec.op_mut().election),
    );
    assert!(actions.broadcast.is_empty(), "no spurious re-poll on a blip");
}

/// BEACON UNION: a membership-departed candidate whose transport-INDEPENDENT
/// beacon still flows is a CPU-starved-but-ALIVE candidate (its QUIC
/// connection idle-timed-out), NOT a dead one — the vote must stay lent,
/// exactly as the arming side refuses to elect against a beaconing primary.
#[tokio::test(flavor = "current_thread")]
async fn beaconing_candidate_is_not_death_observed() {
    let (mut sec, members, beacon) = voter_in_voting();

    // The candidate leaves membership… but its dedicated-thread beacon was
    // just heard.
    depart(&mut sec, &members, CANDIDATE_ID);
    beacon.record(CANDIDATE_ID);

    let actions = sec.run_election_tick();
    assert!(
        matches!(
            sec.op_mut().election,
            ElectionState::Voting { round: 1, ref candidate } if candidate == CANDIDATE_ID
        ),
        "a starved-but-beaconing candidate must NOT be death-observed; got {:?}",
        std::mem::discriminant(&sec.op_mut().election),
    );
    assert!(actions.broadcast.is_empty());
}

/// A LIVE candidate's election proceeds untouched: ticks (including one
/// past the gather window) leave Voting exactly as it was, and the
/// completion edge is unchanged — the winner's `PrimaryChanged` broadcast
/// applies through the unified hook and resets the voter to Normal with the
/// candidate installed as the current primary.
#[tokio::test(flavor = "current_thread")]
async fn live_candidate_election_completes_unchanged() {
    let (mut sec, _members, _beacon) = voter_in_voting();

    // Ticks pass; the candidate is alive and a member throughout.
    let actions = sec.run_election_tick();
    assert!(actions.broadcast.is_empty());
    tokio::time::sleep(KEEPALIVE + Duration::from_millis(10)).await;
    let actions = sec.run_election_tick();
    assert!(
        matches!(
            sec.op_mut().election,
            ElectionState::Voting { round: 1, ref candidate } if candidate == CANDIDATE_ID
        ),
        "a live candidate's voter stays in Voting across ticks; got {:?}",
        std::mem::discriminant(&sec.op_mut().election),
    );
    assert!(actions.broadcast.is_empty());

    // The candidate wins and broadcasts the failover re-point; the voter
    // applies it through the unified `PrimaryChanged` hook — election
    // complete, candidate installed as primary.
    sec.apply_cluster_mutations(vec![ClusterMutation::PrimaryChanged {
        new: CANDIDATE_ID.into(),
        epoch: 2,
        reason: PrimaryChangeReason::Election,
    }]);
    assert!(
        matches!(sec.op_mut().election, ElectionState::Normal),
        "the winner's PrimaryChanged completes the election to Normal; got {:?}",
        std::mem::discriminant(&sec.op_mut().election),
    );
    assert_eq!(
        sec.cluster_state.current_primary(),
        Some(CANDIDATE_ID),
        "the candidate is the installed primary",
    );
}
