//! #331 — DEFERRER-side death-observation on membership-departure.
//!
//! The arming side already treats the current primary LEAVING the transport
//! `MembershipView` as death-evidence (leg (C) of `run_election_tick`'s
//! `need_election`). Pre-#331 the DEFERRER side did not: a peer asked to
//! lend its failover agreement — its `TimeoutResponse` to a suspecter's
//! `TimeoutQuery`, and its `PromotionConfirm` to a candidate's
//! `PromotionVote` — keyed its own death-observation ONLY on its
//! frame-silence clock (`primary_last_seen` / `peer_keepalives` age crossing
//! the `keepalive_interval × keepalive_miss_threshold` death deadline,
//! ~15s prod). So even though every survivor WATCHED the primary's QUIC
//! teardown within one mesh-pump cycle, each still withheld its agreement
//! until its own silence window ran out, bounding failover convergence
//! below by the deadline.
//!
//! These tests pin the deferrer-side fast path through the single-source
//! observation (`primary_departed_membership` /
//! `queried_node_liveness_age`):
//!   - the deferrer CONFIRMS a candidate on primary-membership-departure
//!     while its primary frames are still FRESH (inside the deadline),
//!   - a NON-primary peer's departure is NOT primary-death evidence,
//!   - a transient blip (departure + rejoin before the vote/query) does
//!     NOT unblock the deferrer — no churn,
//!   - the `TimeoutQuery` responder reports `None` (no liveness evidence)
//!     for a seen-then-departed member, and `Some(age)` again on rejoin,
//!   - END-TO-END: a candidate + a real deferrer converge to `Promoted`
//!     while the deferrer's frame-age is still inside the death deadline —
//!     the sub-deadline failover #331 exists for.

#![cfg(test)]

use std::time::{Duration, Instant};

use dynrunner_protocol_primary_secondary::address::PeerId;
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, DistributedMessage, PrimaryChangeReason,
};

use super::super::election::ElectionState;
use super::super::test_helpers::{
    FakeWorkerFactory, MembershipControlPeer, SecondaryHarness, election_config,
    make_secondary_membership,
};

/// The dying primary (lex-lowest, but the failed-over-FROM role — never a
/// candidate). Runs primary+secondary under one peer-id, so its `Secondary`
/// keepalive lands in the deferrer's `peer_keepalives` (the co-located
/// topology where the responder's age-report actually gates convergence).
const PRIMARY_ID: &str = "secondary-0";
/// The lex-lowest SURVIVOR — the candidate the deferrer is asked to confirm.
const CANDIDATE_ID: &str = "secondary-1";
/// THIS node — the deferrer.
const SELF_ID: &str = "secondary-2";
/// The third survivor.
const PEER_3: &str = "secondary-3";

/// `keepalive_interval` (50ms) from `election_config` — the Suspecting gather
/// window the tally waits before counting. The death deadline is
/// `keepalive_interval × keepalive_miss_threshold(2)` = 100ms.
const KEEPALIVE: Duration = Duration::from_millis(50);

/// Bring the deferrer `secondary-2` into `Operational` at the instant the
/// primary dies, with its OWN frame evidence still FRESH:
///   - `current_primary` = `secondary-0` (applied `PrimaryChanged`),
///   - `peer_keepalives` holds the candidate, the third survivor, AND the
///     co-located primary's own `Secondary`-keepalive entry — all fresh,
///   - transport membership holds all three remote peers,
///   - one observed primary message RIGHT NOW (`primary_last_seen` fresh —
///     well inside the 100ms death deadline; the seen-gate satisfied),
///   - `mesh.degraded == false` (the mesh DID form).
///
/// Returns the harness + the membership handle so each test drives its own
/// departure event.
fn fresh_framed_deferrer() -> (
    SecondaryHarness<MembershipControlPeer>,
    std::rc::Rc<std::cell::RefCell<Vec<PeerId>>>,
) {
    let (mut sec, members) = make_secondary_membership(
        election_config(SELF_ID),
        vec![
            PeerId::from(PRIMARY_ID),
            PeerId::from(CANDIDATE_ID),
            PeerId::from(PEER_3),
        ],
    );
    sec.enter_operational_for_test();
    sec.mesh.degraded = false;
    let now = Instant::now();
    sec.op_mut().peer_keepalives.insert(PRIMARY_ID.into(), now);
    sec.op_mut()
        .peer_keepalives
        .insert(CANDIDATE_ID.into(), now);
    sec.op_mut().peer_keepalives.insert(PEER_3.into(), now);
    sec.cluster_state.apply(ClusterMutation::PrimaryChanged {
        new: PRIMARY_ID.into(),
        epoch: 1,
        reason: PrimaryChangeReason::Election,
    });
    sec.publish_membership();
    // Fresh frame evidence: the deferrer saw a primary message just now, so
    // `frame_silent` is FALSE for the next ~100ms — the window in which
    // pre-#331 it would have withheld every confirm.
    sec.record_primary_message();
    (sec, members)
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

/// THE #331 FAST PATH: the primary departs the transport membership; the
/// deferrer's frames are still FRESH (inside the death deadline), yet it
/// must CONFIRM the candidate's `PromotionVote` NOW — its independent death
/// observation is the membership departure, not the silence clock.
///
/// REVERT CHECK: pre-#331 `record_promotion_vote` gated on
/// `frame_silent && !beacon_fresh` alone; with `primary_last_seen` fresh it
/// returned `None`, so failover could not converge before the deadline.
#[tokio::test(flavor = "current_thread")]
async fn deferrer_confirms_on_primary_membership_departure_with_fresh_frames() {
    let (mut def, members) = fresh_framed_deferrer();

    // CONTROL first: while the primary IS a live member and frames are
    // fresh, the deferrer must refuse (split-brain safety unchanged).
    assert!(
        def.record_promotion_vote(CANDIDATE_ID.into(), 1).is_none(),
        "with the primary a live member and fresh frames the deferrer must \
         REFUSE to confirm",
    );

    // The primary dies: its QUIC connection tears down, the pump republishes
    // membership WITHOUT it. Frames are STILL fresh (no 100ms elapsed).
    depart(&mut def, &members, PRIMARY_ID);

    let reply = def.record_promotion_vote(CANDIDATE_ID.into(), 1);
    assert!(
        matches!(
            reply,
            Some(DistributedMessage::PromotionConfirm { ref new_primary_id, .. })
                if new_primary_id == CANDIDATE_ID
        ),
        "#331: a membership-departed primary is observed DEAD — the deferrer \
         confirms the lowest-live-id candidate immediately, without waiting \
         out its frame-silence deadline; got {reply:?}",
    );
    assert!(
        matches!(
            def.op_mut().election,
            ElectionState::Voting { ref candidate, .. } if candidate == CANDIDATE_ID
        ),
        "the confirming deferrer adopts the candidate (Voting)",
    );
}

/// A NON-primary peer's departure is NOT primary-death evidence: the
/// deferrer keeps refusing, and its own election stays Normal.
#[tokio::test(flavor = "current_thread")]
async fn non_primary_departure_is_not_primary_death_evidence() {
    let (mut def, members) = fresh_framed_deferrer();

    // The THIRD SURVIVOR departs — not the primary.
    depart(&mut def, &members, PEER_3);

    assert!(
        def.record_promotion_vote(CANDIDATE_ID.into(), 1).is_none(),
        "a non-primary departure must not unblock the deferrer's confirm",
    );
    let _ = def.run_election_tick();
    assert!(
        matches!(def.op_mut().election, ElectionState::Normal),
        "a non-primary departure must not arm the deferrer's own election",
    );
    assert!(def.fatal_exit.is_none());
}

/// NO CHURN on a transient blip: the primary departs and REJOINS before the
/// candidate's vote arrives. `has_peer` reads `true` again at the vote, so
/// the deferrer still refuses — the membership observation is a live read,
/// not a latched "once departed, forever dead" flag.
#[tokio::test(flavor = "current_thread")]
async fn membership_blip_with_rejoin_does_not_unblock_the_deferrer() {
    let (mut def, members) = fresh_framed_deferrer();

    // Blip: departure + quick rejoin (reconnect), each republished.
    depart(&mut def, &members, PRIMARY_ID);
    members.borrow_mut().push(PeerId::from(PRIMARY_ID));
    def.publish_membership();

    assert!(
        def.record_promotion_vote(CANDIDATE_ID.into(), 1).is_none(),
        "after a departure+rejoin blip the primary is a member again — the \
         deferrer must keep refusing (no churn)",
    );
    assert!(
        matches!(def.op_mut().election, ElectionState::Normal),
        "a blip leaves the deferrer's election untouched",
    );
}

/// THE RESPONDER twin, driven through the REAL inbound path
/// (`handle_inbound`'s `TimeoutQuery` arm): the queued `TimeoutResponse`
/// reports `Some(fresh age)` while the queried primary is a live member,
/// `None` (no liveness evidence — counts as agreement at the querier's
/// tally) once it departed, and `Some(age)` again after a rejoin.
#[tokio::test(flavor = "current_thread")]
async fn timeout_response_reports_no_liveness_for_a_departed_member() {
    let (mut def, members) = fresh_framed_deferrer();

    let query = |sender: &str| DistributedMessage::TimeoutQuery {
        target: None,
        sender_id: sender.to_owned(),
        timestamp: 0.0,
        query_node_id: PRIMARY_ID.to_owned(),
    };
    fn pop_reported_age(def: &mut SecondaryHarness<MembershipControlPeer>) -> Option<f64> {
        let (_, response) = def
            .op_mut()
            .pending_peer_messages
            .pop()
            .expect("the TimeoutQuery arm queues a TimeoutResponse");
        match response {
            DistributedMessage::TimeoutResponse { last_keepalive, .. } => last_keepalive,
            other => panic!("expected TimeoutResponse, got {other:?}"),
        }
    }

    // (1) Primary a live member: the response carries the fresh age.
    def.handle_inbound(query(CANDIDATE_ID), &mut FakeWorkerFactory)
        .await;
    let age = pop_reported_age(&mut def);
    assert!(
        matches!(age, Some(a) if a < 0.1),
        "a live member's keepalive age is reported as-is (fresh); got {age:?}",
    );

    // (2) Primary departs: NO liveness evidence — the corpse's lingering
    // `peer_keepalives` entry must not be reported as if it were liveness.
    depart(&mut def, &members, PRIMARY_ID);
    def.handle_inbound(query(CANDIDATE_ID), &mut FakeWorkerFactory)
        .await;
    assert_eq!(
        pop_reported_age(&mut def),
        None,
        "#331: a seen-then-departed member yields None — the responder lends \
         its agreement immediately instead of waiting out the silence window",
    );

    // (3) Rejoin (blip recovery): the age report resumes — no latched death.
    members.borrow_mut().push(PeerId::from(PRIMARY_ID));
    def.publish_membership();
    def.handle_inbound(query(CANDIDATE_ID), &mut FakeWorkerFactory)
        .await;
    assert!(
        pop_reported_age(&mut def).is_some(),
        "a rejoined member's age is reported again (no churn latch)",
    );
}

/// END-TO-END sub-deadline convergence: a CANDIDATE (`secondary-1`) and a
/// real DEFERRER (`secondary-2`) drive the full failover handshake over the
/// frames the candidate actually broadcasts, converging to `Promoted` while
/// the deferrer's primary frame-age is STILL inside the 100ms death
/// deadline — the convergence bound #331 removes. Pre-#331 this wedged
/// twice: the deferrer's `TimeoutResponse` reported a fresh age (tally
/// disagreement → the candidate re-polled until the deadline), and its
/// `PromotionConfirm` was withheld on `frame_silent == false`.
#[tokio::test(flavor = "current_thread")]
async fn failover_converges_before_the_frame_deadline() {
    // The CANDIDATE: lex-lowest survivor, fresh-framed like the deferrer.
    let (mut cand, cand_members) = make_secondary_membership(
        election_config(CANDIDATE_ID),
        vec![
            PeerId::from(PRIMARY_ID),
            PeerId::from(SELF_ID),
            PeerId::from(PEER_3),
        ],
    );
    cand.enter_operational_for_test();
    cand.mesh.degraded = false;
    let now = Instant::now();
    cand.op_mut().peer_keepalives.insert(PRIMARY_ID.into(), now);
    cand.op_mut().peer_keepalives.insert(SELF_ID.into(), now);
    cand.op_mut().peer_keepalives.insert(PEER_3.into(), now);
    cand.cluster_state.apply(ClusterMutation::PrimaryChanged {
        new: PRIMARY_ID.into(),
        epoch: 1,
        reason: PrimaryChangeReason::Election,
    });
    cand.publish_membership();
    cand.record_primary_message();

    // The DEFERRER: the harness under test.
    let (mut def, def_members) = fresh_framed_deferrer();

    // The primary dies: departs BOTH survivors' membership views within one
    // pump cycle.
    depart(&mut cand, &cand_members, PRIMARY_ID);
    depart(&mut def, &def_members, PRIMARY_ID);

    // Candidate tick 1: arms Suspecting via leg (C) (fresh frames — the
    // arming side's membership observation) and broadcasts the TimeoutQuery.
    let actions = cand.run_election_tick();
    assert!(
        matches!(cand.op_mut().election, ElectionState::Suspecting { .. }),
        "the candidate arms on the membership departure",
    );
    let timeout_query = actions
        .broadcast
        .iter()
        .find(|m| matches!(m, DistributedMessage::TimeoutQuery { .. }))
        .expect("Suspecting broadcasts a TimeoutQuery")
        .clone();

    // Deliver the candidate's ACTUAL query to the deferrer; its queued
    // TimeoutResponse must report None — agreement — even though its OWN
    // frame-age is still fresh.
    def.handle_inbound(timeout_query, &mut FakeWorkerFactory)
        .await;
    let (_, response) = def
        .op_mut()
        .pending_peer_messages
        .pop()
        .expect("the deferrer answers the query");
    let reported = match &response {
        DistributedMessage::TimeoutResponse { last_keepalive, .. } => *last_keepalive,
        other => panic!("expected TimeoutResponse, got {other:?}"),
    };
    assert_eq!(
        reported, None,
        "the deferrer's death observation is the membership departure — its \
         agreement must not wait on its frame-silence clock",
    );
    cand.record_timeout_response(SELF_ID.into(), reported);

    // Gather window elapses (50ms — still well inside the 100ms deadline);
    // the candidate tallies: quorum = failover_quorum(2) = 2 = self + the
    // deferrer's None-agreement → self-leads, broadcasts PromotionVote.
    tokio::time::sleep(KEEPALIVE + Duration::from_millis(5)).await;
    let actions = cand.run_election_tick();
    let vote = actions
        .broadcast
        .iter()
        .find(|m| matches!(m, DistributedMessage::PromotionVote { .. }))
        .expect("the lex-lowest survivor self-leads on the fast quorum")
        .clone();

    // Deliver the candidate's ACTUAL vote to the deferrer; it confirms
    // (frames still fresh — the #331 confirm fast path).
    def.handle_inbound(vote, &mut FakeWorkerFactory).await;
    let (_, confirm) = def
        .op_mut()
        .pending_peer_messages
        .pop()
        .expect("the deferrer lends its confirm immediately");
    let (confirmed_id, round) = match &confirm {
        DistributedMessage::PromotionConfirm {
            new_primary_id,
            vote_round,
            ..
        } => (new_primary_id.clone(), *vote_round),
        other => panic!("expected PromotionConfirm, got {other:?}"),
    };
    assert_eq!(confirmed_id, CANDIDATE_ID);

    // PROOF the whole handshake ran inside the silence window: the
    // deferrer's frame evidence is STILL fresh at confirm time.
    let frame_age = def
        .op_mut()
        .primary_last_seen
        .expect("seen-gate satisfied")
        .elapsed();
    assert!(
        frame_age < Duration::from_millis(100),
        "the deferrer confirmed while its frame-age ({frame_age:?}) was still \
         inside the 100ms death deadline — sub-deadline convergence",
    );

    // The confirm crosses quorum (self + deferrer = 2): the candidate
    // promotes — failover converged without any silence window expiring.
    let promoted = cand.record_promotion_confirm(SELF_ID.into(), confirmed_id, round);
    assert!(
        promoted || matches!(cand.op_mut().election, ElectionState::Promoted),
        "the deferrer's fast confirm promotes the candidate",
    );
}
