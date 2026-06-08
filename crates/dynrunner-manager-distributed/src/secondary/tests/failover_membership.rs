//! BUG H: failover election arms on the IDLE-INDEPENDENT primary-departure
//! signal (`run_election_tick` leg (C)).
//!
//! The defect: an IDLE survivor (no pending `send_to_primary`) never opens
//! leg (A)'s send-driven health window — keepalives fan `Destination::All`,
//! never no-route — so on a primary DEATH it would silently reconnect-loop
//! the dead primary for the whole ~120s leg-(B) backstop instead of electing.
//! The fix arms the election when the current primary LEAVES the transport
//! `MembershipView` (the mesh-pump's `handle_peer_disconnect` republish),
//! intersected with `current_primary()` — a direct, idle-independent death
//! signal that needs no send from this node.
//!
//! These tests drive the REAL path: a `MembershipControlPeer` whose
//! `connected_ids` the test mutates models the primary connecting, proving
//! liveness (a primary message → `primary_last_seen = Some`), then LEAVING
//! the mesh. The election must arm via leg (C) WITHOUT any `record_recv_failure`
//! (leg A) and WELL UNDER the patient backstop (leg B). The gap-closure check
//! (`primary_present_no_election`) is the revert proof: while the primary is
//! still a member, the same idle survivor stays Normal — so the election is
//! membership-armed, not firing for some other reason.

#![cfg(test)]

use std::time::{Duration, Instant};

use dynrunner_protocol_primary_secondary::address::PeerId;
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, DistributedMessage, PrimaryChangeReason,
};

use super::super::election::ElectionState;
use super::super::test_helpers::{election_config, make_secondary_membership};

const PRIMARY_ID: &str = "primary-orig";
const PEER_ID: &str = "sec-b";

/// The tiny app-silence backstop `election_config` installs (leg (B)).
/// Kept in sync with `test_helpers::election_config`.
const BACKSTOP: Duration = Duration::from_millis(100);

/// Bring a membership-controlled secondary into `Operational` with:
///   - `current_primary` = `PRIMARY_ID` (applied `PrimaryChanged`),
///   - a live peer (`PEER_ID`) so an armed election has a non-degraded path
///     to `Suspecting` (isolating "did it arm" from "did it degraded-bail"),
///   - one observed primary message so leg (C)'s `primary_last_seen.is_some()`
///     gate is satisfied (the primary PROVED liveness before leaving).
///
/// The shared membership id-set initially holds {primary, peer}.
fn operational_with_seen_primary() -> (
    super::super::test_helpers::SecondaryHarness<
        super::super::test_helpers::MembershipControlPeer,
    >,
    std::rc::Rc<std::cell::RefCell<Vec<PeerId>>>,
) {
    let (mut sec, members) = make_secondary_membership(
        election_config("sec-a"),
        vec![PeerId::from(PRIMARY_ID), PeerId::from(PEER_ID)],
    );
    sec.enter_operational_for_test();
    sec.op_mut()
        .peer_keepalives
        .insert(PEER_ID.into(), Instant::now());
    sec.cluster_state.apply(ClusterMutation::PrimaryChanged {
        new: PRIMARY_ID.into(),
        epoch: 1,
        reason: PrimaryChangeReason::Election,
    });
    // Publish the seeded membership into the view the coordinator's
    // `MeshClient::has_peer` reads (the leg-(C) source).
    sec.publish_membership();
    // The primary proved liveness once — leg (C) only fires for a primary
    // this node has actually SEEN (no relocation-window false-arm).
    sec.record_primary_message();
    (sec, members)
}

/// GAP-CLOSURE / revert proof: while the current primary is STILL a member of
/// the transport mesh, an IDLE survivor (no `record_recv_failure`, staleness
/// under the backstop) stays `Normal`. This is the control that proves the
/// election in `idle_survivor_elects_on_primary_departure` is caused by the
/// membership-departure arm and nothing else: remove the arm (revert the leg-
/// (C) wiring) and BOTH tests stay Normal — i.e. failover never fires for the
/// idle survivor, the exact pre-fix defect.
#[tokio::test(flavor = "current_thread")]
async fn primary_present_no_election() {
    let (mut sec, _members) = operational_with_seen_primary();

    // Leg (A) silent (no no-route probe), leg (B) inactive (fresh
    // `primary_last_seen`), and the primary is STILL in membership.
    assert!(
        !sec.op_mut().primary_link.should_arm_failover(),
        "no send no-route, so leg (A) must be silent",
    );
    assert!(
        sec.client.has_peer(&PeerId::from(PRIMARY_ID)),
        "precondition: the primary is a live mesh member",
    );

    let actions = sec.run_election_tick();
    assert!(
        matches!(sec.op_mut().election, ElectionState::Normal),
        "an idle survivor with a LIVE primary in membership must stay Normal; got {:?}",
        std::mem::discriminant(&sec.op_mut().election),
    );
    assert!(
        actions.broadcast.is_empty(),
        "no TimeoutQuery while the primary is still a member",
    );
    assert!(sec.fatal_exit.is_none());
}

/// THE BUG-H test: killing the current primary (removing it from the transport
/// `MembershipView`) makes an IDLE survivor — one that issues NO
/// `send_to_primary`, so leg (A) never arms, and whose `primary_last_seen` is
/// fresh, so leg (B) is nowhere near firing — ELECT via the membership-
/// departure signal (leg C). The election arms FAST (this tick), without
/// waiting the patient backstop and without any send-side probe.
#[tokio::test(flavor = "current_thread")]
async fn idle_survivor_elects_on_primary_departure() {
    let (mut sec, members) = operational_with_seen_primary();

    // Sanity: with the primary present this idle node would NOT elect
    // (covered exhaustively by `primary_present_no_election`); here we go
    // straight to the death event.

    // The primary DIES: its QUIC connection tears down, the mesh-pump's
    // `handle_peer_disconnect` re-reads the live transport and republishes
    // the `MembershipView` WITHOUT the primary. Model that by removing its id
    // from the shared set and re-publishing — the idle-independent signal,
    // produced with NO send from this node.
    members.borrow_mut().retain(|id| id != &PeerId::from(PRIMARY_ID));
    sec.publish_membership();

    // Leg (A) is still silent — we drove no `record_recv_failure`.
    assert!(
        !sec.op_mut().primary_link.should_arm_failover(),
        "an idle survivor issues no primary-bound send, so leg (A) stays silent",
    );
    // Leg (B) is inactive — `primary_last_seen` is fresh, far under backstop.
    let stale = Instant::now().duration_since(
        sec.op_mut()
            .primary_last_seen
            .expect("set by record_primary_message"),
    );
    assert!(
        stale < BACKSTOP,
        "leg (B) must be inactive — staleness {stale:?} under backstop {BACKSTOP:?}",
    );
    // The primary is gone from membership — leg (C) is the ONLY active leg.
    assert!(
        !sec.client.has_peer(&PeerId::from(PRIMARY_ID)),
        "the primary has left the transport mesh",
    );

    let actions = sec.run_election_tick();
    assert!(
        matches!(sec.op_mut().election, ElectionState::Suspecting { .. }),
        "primary departure must arm the election FAST via leg (C), without a \
         send-probe and without waiting the backstop; got {:?}",
        std::mem::discriminant(&sec.op_mut().election),
    );
    assert!(
        actions
            .broadcast
            .iter()
            .any(|m| matches!(m, DistributedMessage::TimeoutQuery { target: _, .. })),
        "leg (C) election must broadcast TimeoutQuery to gather quorum",
    );
    assert!(sec.fatal_exit.is_none(), "a peered survivor never degraded-bails");
}

/// Relocation-window guard: leg (C) is GATED on `primary_last_seen.is_some()`.
/// A freshly-named compute primary that this survivor's transport has NOT yet
/// dialled is in `current_primary()` but absent from membership — yet the
/// election must NOT arm, because the primary has never proven liveness here.
/// Without the gate, every relocation would spuriously elect during the dial
/// window. (This is the same node as the BUG-H test MINUS the
/// `record_primary_message` that seeds `primary_last_seen`.)
#[tokio::test(flavor = "current_thread")]
async fn relocation_window_does_not_false_arm() {
    let (mut sec, _members) = make_secondary_membership(
        election_config("sec-a"),
        // The relocation target is NOT yet a member (transport not dialled);
        // a live PEER is, so a (wrongly) firing election would have a
        // non-degraded path — isolating "we stayed Normal" as a genuine gate.
        vec![PeerId::from(PEER_ID)],
    );
    sec.enter_operational_for_test();
    sec.op_mut()
        .peer_keepalives
        .insert(PEER_ID.into(), Instant::now());
    sec.cluster_state.apply(ClusterMutation::PrimaryChanged {
        new: PRIMARY_ID.into(),
        epoch: 1,
        reason: PrimaryChangeReason::Election,
    });
    sec.publish_membership();
    // Deliberately NO `record_primary_message`: the relocation target has not
    // yet spoken, so `primary_last_seen` stays `None`.

    assert!(
        sec.op_ref()
            .and_then(|op| op.primary_last_seen)
            .is_none(),
        "precondition: the not-yet-dialled primary has never proven liveness",
    );
    assert!(
        !sec.client.has_peer(&PeerId::from(PRIMARY_ID)),
        "precondition: the relocation target is not yet a mesh member",
    );

    let actions = sec.run_election_tick();
    assert!(
        matches!(sec.op_mut().election, ElectionState::Normal),
        "a not-yet-seen relocation target absent from membership must NOT arm \
         leg (C) — the gate suppresses the dial window; got {:?}",
        std::mem::discriminant(&sec.op_mut().election),
    );
    assert!(
        actions.broadcast.is_empty(),
        "no spurious TimeoutQuery during the relocation dial window",
    );
    assert!(sec.fatal_exit.is_none());
}

/// A transient single-cycle membership flicker on a STILL-LIVE primary is
/// self-cancelling: the primary drops from membership for one tick (arming
/// leg (C) → Suspecting), then its next keepalive routes through
/// `record_primary_message`, which reverts the election to Normal. This pins
/// that leg (C) shares the same recovery path as legs (A)/(B) — a flicker
/// never strands the node mid-failover.
#[tokio::test(flavor = "current_thread")]
async fn membership_flicker_recovers_via_primary_message() {
    let (mut sec, members) = operational_with_seen_primary();

    // Flicker: the primary briefly leaves membership and the tick arms.
    members.borrow_mut().retain(|id| id != &PeerId::from(PRIMARY_ID));
    sec.publish_membership();
    sec.run_election_tick();
    assert!(
        matches!(sec.op_mut().election, ElectionState::Suspecting { .. }),
        "the flicker armed leg (C)",
    );

    // The primary speaks again (the connection was never really dead): its
    // keepalive cancels the in-flight election back to Normal.
    sec.record_primary_message();
    assert!(
        matches!(sec.op_mut().election, ElectionState::Normal),
        "a resumed primary message must cancel the leg-(C) election; got {:?}",
        std::mem::discriminant(&sec.op_mut().election),
    );

    // And once the primary rejoins membership, a follow-up tick stays Normal.
    members.borrow_mut().push(PeerId::from(PRIMARY_ID));
    sec.publish_membership();
    let actions = sec.run_election_tick();
    assert!(
        matches!(sec.op_mut().election, ElectionState::Normal),
        "post-recovery, with the primary back in membership, the node stays Normal",
    );
    assert!(actions.broadcast.is_empty());
}
