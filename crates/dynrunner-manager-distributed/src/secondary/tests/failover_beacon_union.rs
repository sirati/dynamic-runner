//! #325: the failover-detector UNIONs the primary's transport-INDEPENDENT
//! liveness BEACON with its mesh-frame view.
//!
//! The defect: a relocated/promoted primary's NODE keeps its co-located
//! worker-secondary running builds, so its single-threaded tokio runtime
//! CPU-starves exactly like any compute node — its OUTBOUND mesh keepalive
//! freezes and its QUIC connection idle-times-out, tripping the mesh-frame
//! death legs (A)/(C). A purely mesh-frame failover-detector then SPURIOUSLY
//! elects a successor against a still-ALIVE primary (asm-dataset
//! run_20260609_092032).
//!
//! The fix (mirror of the primary-side reaper's union): the election arms
//! only when the mesh-frame disjunction says dead AND the primary's BEACON
//! is ALSO silent. The primary's dedicated-thread beacon (its own OS thread +
//! UdpSocket, off the build-starved runtime) keeps flowing through the
//! starvation, so `run_election_tick`'s `need_election` short-circuits.
//!
//! These tests drive the REAL leg-(C) departure path (a
//! `MembershipControlPeer` whose `connected_ids` the test mutates), the same
//! harness `failover_membership` uses, and toggle ONLY the beacon view:
//!   - HEADLINE: leg (C) armed + beacon fresh → NO election;
//!   - revert-check: the SAME setup with NO beacon → the election arms (this
//!     IS `failover_membership::idle_survivor_elects_on_primary_departure`,
//!     re-pinned here to prove the beacon is what suppresses it);
//!   - GENUINE death: leg (C) armed + beacon stale/never-seen → election
//!     STILL arms promptly (#317 path intact);
//!   - peer-vote union: a peer's election cannot pull our `PromotionConfirm`
//!     while the primary's beacon is fresh.

#![cfg(test)]

use std::time::{Duration, Instant};

use dynrunner_protocol_primary_secondary::address::PeerId;
use dynrunner_protocol_primary_secondary::{ClusterMutation, DistributedMessage, PrimaryChangeReason};

use super::super::election::ElectionState;
use super::super::test_helpers::{
    MembershipControlPeer, SecondaryHarness, election_config, make_secondary_membership,
};
use crate::liveness::BeaconLiveness;

const PRIMARY_ID: &str = "primary-orig";
const PEER_ID: &str = "sec-b";

/// Bring a membership-controlled secondary into `Operational` with the
/// current primary applied, a live peer (so an armed election has a
/// non-degraded path to `Suspecting`), and one observed primary message (so
/// leg (C)'s `primary_last_seen.is_some()` gate is satisfied). Returns the
/// harness, the shared membership handle, AND the [`BeaconLiveness`] WRITER
/// the test records the primary's beacon into (a clone of the view installed
/// on the coordinator). Identical to `failover_membership`'s fixture plus the
/// beacon-view install.
fn operational_with_seen_primary_and_beacon() -> (
    SecondaryHarness<MembershipControlPeer>,
    std::rc::Rc<std::cell::RefCell<Vec<PeerId>>>,
    BeaconLiveness,
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
    sec.publish_membership();
    sec.record_primary_message();
    // Install a beacon-liveness view we hold the writer side of (the listener
    // would normally write it per decoded datagram).
    let beacon = BeaconLiveness::new();
    sec.set_beacon_liveness(beacon.clone());
    (sec, members, beacon)
}

/// Kill the primary in the transport mesh (leg (C) departure), the same model
/// as `failover_membership`: a CPU-starved primary's QUIC connection idle-
/// times-out, the mesh-pump republishes membership WITHOUT it.
fn primary_leaves_membership(
    sec: &mut SecondaryHarness<MembershipControlPeer>,
    members: &std::rc::Rc<std::cell::RefCell<Vec<PeerId>>>,
) {
    members
        .borrow_mut()
        .retain(|id| id != &PeerId::from(PRIMARY_ID));
    sec.publish_membership();
}

/// HEADLINE: a CPU-starved-but-ALIVE primary whose mesh keepalive is silent
/// (it left membership — leg (C) armed) but whose dedicated-thread BEACON
/// still flows is NOT declared dead → NO spurious failover election armed.
#[tokio::test(flavor = "current_thread")]
async fn starved_primary_beaconing_is_not_failed_over() {
    let (mut sec, members, beacon) = operational_with_seen_primary_and_beacon();

    // The primary's runtime is CPU-starved: its mesh keepalive froze and its
    // QUIC connection idle-timed-out, so it LEFT the transport mesh — leg (C)
    // would arm on the mesh-frame view alone.
    primary_leaves_membership(&mut sec, &members);
    assert!(
        !sec.client.has_peer(&PeerId::from(PRIMARY_ID)),
        "precondition: the starved primary left the transport mesh (leg C would arm)",
    );

    // …BUT its off-runtime beacon is still flowing — the listener just
    // recorded a fresh beacon from the primary.
    beacon.record(PRIMARY_ID);

    let actions = sec.run_election_tick();
    assert!(
        matches!(sec.op_mut().election, ElectionState::Normal),
        "a CPU-starved-but-beaconing primary must NOT be failed over; got {:?}",
        std::mem::discriminant(&sec.op_mut().election),
    );
    assert!(
        actions.broadcast.is_empty(),
        "no TimeoutQuery for a primary whose beacon proves it is alive",
    );
    assert!(sec.fatal_exit.is_none());
}

/// REVERT-CHECK: the EXACT same setup — primary left membership (leg C
/// armed), idle survivor, fresh `primary_last_seen` — but with NO beacon ever
/// recorded ARMS the election. This is the without-the-beacon-union control:
/// it proves the beacon is precisely what suppresses the election above (and
/// is the asm-dataset spurious-failover the fix targets).
#[tokio::test(flavor = "current_thread")]
async fn revert_check_no_beacon_silent_mesh_arms_election() {
    let (mut sec, members, _beacon) = operational_with_seen_primary_and_beacon();

    // Primary left membership — identical to the HEADLINE — but we record NO
    // beacon (the pre-fix world / the beacon coupled to the starved runtime).
    primary_leaves_membership(&mut sec, &members);

    let actions = sec.run_election_tick();
    assert!(
        matches!(sec.op_mut().election, ElectionState::Suspecting { .. }),
        "without a beacon, a silent-mesh primary departure arms the election; got {:?}",
        std::mem::discriminant(&sec.op_mut().election),
    );
    assert!(
        actions
            .broadcast
            .iter()
            .any(|m| matches!(m, DistributedMessage::TimeoutQuery { target: _, .. })),
        "the armed election broadcasts TimeoutQuery",
    );
}

/// GENUINE primary death (#317 intact): leg (C) armed AND the beacon is STALE
/// (the primary genuinely died — no frame AND no beacon past the threshold) →
/// the election STILL arms promptly. A stale beacon must never suppress a real
/// failover.
#[tokio::test(flavor = "current_thread")]
async fn genuine_death_stale_beacon_still_arms() {
    let (mut sec, members, beacon) = operational_with_seen_primary_and_beacon();

    // The primary DID beacon once long ago, then genuinely died: both its
    // frames AND its beacon have been silent past the threshold. The election
    // staleness yardstick is keepalive_interval × keepalive_miss_threshold =
    // 50ms × 2 = 100ms (`election_config`); a beacon recorded now and aged
    // past that is stale. Record, then leave membership, then sleep past the
    // deadline so the beacon is no longer fresh.
    beacon.record(PRIMARY_ID);
    primary_leaves_membership(&mut sec, &members);
    tokio::time::sleep(Duration::from_millis(140)).await;

    // The beacon is now stale (no refresh past the ~100ms deadline) and the
    // primary is gone from membership — the genuine-death shape.
    assert!(
        !sec.primary_beacon_fresh(PRIMARY_ID),
        "precondition: the beacon has gone stale (genuine death)",
    );

    let actions = sec.run_election_tick();
    assert!(
        matches!(sec.op_mut().election, ElectionState::Suspecting { .. }),
        "a genuine death (no frame AND no beacon) must still arm the election; got {:?}",
        std::mem::discriminant(&sec.op_mut().election),
    );
    assert!(
        actions
            .broadcast
            .iter()
            .any(|m| matches!(m, DistributedMessage::TimeoutQuery { target: _, .. })),
        "genuine-death election broadcasts TimeoutQuery",
    );
}

/// A primary that NEVER beaconed (no beacon path this run) but genuinely
/// departed still arms — a never-seen beacon is NOT fresh, so the union
/// degrades to the mesh-frame legs alone (the pre-#325 behaviour). Guards that
/// the union never silently disables failover when no beacon exists.
#[tokio::test(flavor = "current_thread")]
async fn never_beaconed_primary_departure_still_arms() {
    let (mut sec, members, _beacon) = operational_with_seen_primary_and_beacon();
    primary_leaves_membership(&mut sec, &members);
    assert!(
        !sec.primary_beacon_fresh(PRIMARY_ID),
        "a never-recorded beacon is not fresh",
    );

    let actions = sec.run_election_tick();
    assert!(
        matches!(sec.op_mut().election, ElectionState::Suspecting { .. }),
        "a never-beaconed departed primary arms via the mesh-frame legs; got {:?}",
        std::mem::discriminant(&sec.op_mut().election),
    );
    assert!(
        actions
            .broadcast
            .iter()
            .any(|m| matches!(m, DistributedMessage::TimeoutQuery { target: _, .. })),
    );
}

/// PEER-VOTE union: a PEER that armed an election against a CPU-starved-but-
/// alive primary must NOT be able to pull THIS node's `PromotionConfirm` while
/// the primary's beacon is fresh. `record_promotion_vote` returns `None` (we
/// refuse to confirm), so the peer's spurious election cannot reach quorum
/// through us. A stale/absent beacon (genuine death) lets the confirm proceed.
#[tokio::test(flavor = "current_thread")]
async fn peer_vote_refused_while_primary_beacon_fresh() {
    let (mut sec, _members, beacon) = operational_with_seen_primary_and_beacon();

    // Frames went silent (the starved primary's mesh keepalive froze): backdate
    // `primary_last_seen` past the deadline so `frame_silent` is true — the
    // exact condition a peer's `PromotionVote` would otherwise let us confirm.
    sec.op_mut().primary_last_seen = Some(Instant::now() - Duration::from_millis(500));
    // …but the beacon is fresh.
    beacon.record(PRIMARY_ID);

    // The candidate is the lex-lowest live id so the only thing that can block
    // the confirm is the beacon-union (not the lowest-id check). `sec-a` (us)
    // is lower than `sec-b`; vote for ourselves to keep candidate selection
    // trivially satisfied, then assert the beacon-union still refuses.
    let reply = sec.record_promotion_vote("sec-a".into(), 1);
    assert!(
        reply.is_none(),
        "the beacon proves the primary alive — must REFUSE to confirm a peer's \
         spurious election",
    );

    // Now the beacon goes stale (genuine death): the confirm proceeds.
    tokio::time::sleep(Duration::from_millis(140)).await;
    assert!(!sec.primary_beacon_fresh(PRIMARY_ID), "beacon now stale");
    let reply = sec.record_promotion_vote("sec-a".into(), 1);
    assert!(
        matches!(reply, Some(DistributedMessage::PromotionConfirm { .. })),
        "with no fresh beacon and silent frames, the confirm proceeds (#317 intact)",
    );
}
