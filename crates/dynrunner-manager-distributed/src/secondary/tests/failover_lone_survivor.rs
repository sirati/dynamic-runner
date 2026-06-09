//! LONE-SURVIVOR failover convergence: the quorum DENOMINATOR shrinks to the
//! actually-live fleet via the FAST transport-membership signal, not only the
//! SLOW `peer_timeout` keepalive reaper.
//!
//! THE BUG (consumer asm-dataset, live SLURM): a clean single-primary-kill
//! failover converges, but degrading to a LONE survivor by killing the
//! (relocated/promoted) primary AND one secondary SIMULTANEOUSLY wedges the
//! survivor forever: it loops "no quorum on primary death; agreeing=1 quorum=2"
//! and never promotes.
//!
//! ROOT CAUSE: `live_peer_ids` (the failover-quorum denominator) read ONLY
//! `peer_keepalives`, which is reaped on the conservative `peer_timeout` (300s
//! prod). On a simultaneous peer loss the dead peer's QUIC connection tears
//! down — so it leaves the transport `MembershipView` within one mesh-pump
//! cycle — but it lingers in `peer_keepalives` for the whole 300s. So the
//! denominator stayed at 1, `failover_quorum(1) == 2`, and the lone survivor
//! (whose only "peer" was dead and could never reply agreeing) never reached
//! quorum. This is the SAME fast-vs-slow asymmetry leg (C) fixed for the
//! PRIMARY (membership-departure death detection), but the PEER quorum
//! denominator never got the fast signal.
//!
//! THE FIX: `live_peer_ids` intersects `peer_keepalives` with the live
//! `MembershipView` (`client.has_peer`) — a peer counts toward quorum only
//! while it is a connected mesh member. A peer absent from membership is dead
//! NOW (the supported topology has reliable, non-firewalled inter-compute
//! networking — a simultaneous loss is DEATH, not a partition), so the
//! denominator shrinks promptly and the lone survivor self-promotes.
//!
//! These tests drive the REAL path via a `MembershipControlPeer` whose
//! `connected_ids` the test mutates: the fleet meshes (so `mesh.degraded`
//! stays FALSE — never the never-meshed split-brain case), then the primary
//! AND one peer are removed from membership while their `peer_keepalives`
//! entries are STILL FRESH (well within `peer_timeout`, so the reaper is a
//! no-op). Pre-fix the survivor wedges in Suspecting at quorum 2; post-fix it
//! reaches `Promoted`.

#![cfg(test)]

use std::time::{Duration, Instant};

use dynrunner_protocol_primary_secondary::address::PeerId;
use dynrunner_protocol_primary_secondary::{ClusterMutation, PrimaryChangeReason};

use super::super::election::{ElectionState, failover_quorum};
use super::super::test_helpers::{
    MembershipControlPeer, SecondaryHarness, election_config, make_secondary_membership,
};

const PRIMARY_ID: &str = "secondary-1"; // the relocated/promoted primary
const SELF_ID: &str = "secondary-3"; // the lone survivor
const DEAD_PEER: &str = "secondary-2"; // the simultaneously-killed secondary

/// `keepalive_interval` (50ms) from `election_config` — the Suspecting gather
/// window the tally waits before counting.
const KEEPALIVE: Duration = Duration::from_millis(50);

/// Bring `secondary-3` into `Operational` as one of three survivors of a
/// fleet whose primary is the relocated/promoted `secondary-1`:
///   - `current_primary` = `secondary-1` (applied `PrimaryChanged`),
///   - the primary `secondary-1` AND the peer `secondary-2` both live mesh
///     members, both in `peer_keepalives` (the recent-keepalive view),
///   - one observed primary message (leg-(C) `primary_last_seen.is_some()`),
///   - `mesh.degraded == false` (the mesh DID form — this survivor was
///     failover-capable, NOT the never-meshed split-brain case).
///
/// `peer_timeout` is left at the helper default (120s) and never reached
/// in-test, so the keepalive reaper is a no-op — the convergence MUST come
/// from the transport-membership departure, not the slow reaper.
fn three_survivor_member() -> (
    SecondaryHarness<MembershipControlPeer>,
    std::rc::Rc<std::cell::RefCell<Vec<PeerId>>>,
) {
    let (mut sec, members) = make_secondary_membership(
        election_config(SELF_ID),
        vec![PeerId::from(PRIMARY_ID), PeerId::from(DEAD_PEER)],
    );
    sec.enter_operational_for_test();
    sec.mesh.degraded = false;
    let now = Instant::now();
    // Both the promoted primary (a same-peer primary+secondary host emits a
    // Secondary keepalive too) and the peer secondary are recent live entries.
    sec.op_mut().peer_keepalives.insert(PRIMARY_ID.into(), now);
    sec.op_mut().peer_keepalives.insert(DEAD_PEER.into(), now);
    sec.cluster_state.apply(ClusterMutation::PrimaryChanged {
        new: PRIMARY_ID.into(),
        epoch: 1,
        reason: PrimaryChangeReason::Election,
    });
    sec.publish_membership();
    sec.record_primary_message();
    (sec, members)
}

/// THE LONE-SURVIVOR WEDGE PROBE + FIX. The promoted primary AND one peer die
/// SIMULTANEOUSLY: both leave the transport `MembershipView`, but their
/// `peer_keepalives` entries are still fresh (peer_timeout not reached, so the
/// reaper does nothing). The survivor MUST shrink its quorum denominator to
/// the live fleet (empty), compute `failover_quorum(0) == 1`, and self-promote.
///
/// REVERT CHECK: pre-fix `live_peer_ids` read only `peer_keepalives`, so it
/// still returned `["secondary-2"]` → `peer_count == 1` → `quorum == 2`,
/// reachable only by self + the dead peer's (never-arriving) agreeing reply —
/// the survivor wedged in `Suspecting` at quorum 2 forever.
#[tokio::test(flavor = "current_thread")]
async fn lone_survivor_promotes_when_quorum_denominator_shrinks_to_live_fleet() {
    let (mut sec, members) = three_survivor_member();

    // Pre-kill sanity: both peers are members AND keepalive-tracked, so the
    // denominator counts the one non-primary peer (the dead-to-be secondary-2);
    // current_primary (secondary-1) is excluded from the denominator.
    assert_eq!(
        sec.live_peer_ids().cloned().collect::<Vec<_>>(),
        vec![DEAD_PEER.to_string()],
        "pre-kill, the one non-primary peer is in the quorum denominator",
    );

    // SIMULTANEOUS kill: the promoted primary secondary-1 AND the peer
    // secondary-2 both die. Their QUIC connections tear down, so the mesh-pump
    // republishes the MembershipView WITHOUT them. peer_keepalives still holds
    // both (peer_timeout is 120s, not reached).
    members.borrow_mut().clear();
    sec.publish_membership();

    // The reaper runs (the live loop calls it every keepalive tick) but is a
    // no-op — the entries are fresh, far inside peer_timeout. The convergence
    // must come from the fast membership-departure signal, NOT the reaper.
    sec.check_peer_timeouts();

    // FIX ASSERTION: the quorum denominator shrinks to the truly-live fleet
    // (empty) via the membership intersection, so quorum is 1 — met by the
    // survivor's own single self-confirm.
    assert!(
        sec.live_peer_ids().next().is_none(),
        "a peer absent from transport membership must NOT inflate the quorum \
         denominator — even while its keepalive entry lingers within peer_timeout; \
         got {:?}",
        sec.live_peer_ids().cloned().collect::<Vec<_>>(),
    );
    assert_eq!(
        failover_quorum(sec.live_peer_ids().count()),
        1,
        "with the live fleet empty, the lone-survivor quorum is 1 (self only)",
    );

    // Tick 1: arm the election (leg C — the primary left membership, seen
    // before). The reaper has already run this tick in the live loop.
    let _ = sec.run_election_tick();
    assert!(
        matches!(sec.op_mut().election, ElectionState::Suspecting { .. }),
        "primary departure arms Suspecting; got {:?}",
        std::mem::discriminant(&sec.op_mut().election),
    );

    // Wait the gather window, then tick again to tally. With zero live peers
    // the self-quorum (quorum 1) is ALREADY met, so the tick commits the
    // promotion in-tick — no Candidate wait for a peer confirm that can never
    // arrive (the #317 single-survivor self-quorum path).
    tokio::time::sleep(KEEPALIVE + Duration::from_millis(5)).await;
    sec.check_peer_timeouts();
    let actions = sec.run_election_tick();
    assert!(
        actions.promoted,
        "the lone survivor must COMMIT the promotion in-tick once its quorum \
         denominator has shrunk to the live (empty) fleet — not wedge in \
         Suspecting at quorum 2 waiting on a dead peer's agreeing reply",
    );
    assert!(
        matches!(sec.op_mut().election, ElectionState::Promoted),
        "the committing tick leaves the election Promoted for the terminal action",
    );
    assert!(sec.fatal_exit.is_none(), "a meshed survivor never degraded-bails");
}

/// CONTROL / no-over-shrink: a peer that is STILL a connected mesh member
/// (its keepalive fresh) MUST remain in the quorum denominator — the fix only
/// drops peers that have DEPARTED membership, never a still-present peer. This
/// pins that the membership intersection has not collapsed the multi-survivor
/// denominator (which would let a survivor self-promote against a live peer —
/// a split-brain). Here only the PRIMARY dies; the peer secondary-2 stays a
/// member, so the survivor must still need a peer confirm (quorum 2), NOT
/// self-promote solo.
#[tokio::test(flavor = "current_thread")]
async fn live_peer_still_counts_when_only_primary_departs() {
    let (mut sec, members) = three_survivor_member();

    // ONLY the primary dies; the peer secondary-2 stays a connected member.
    members
        .borrow_mut()
        .retain(|id| id != &PeerId::from(PRIMARY_ID));
    sec.publish_membership();
    sec.check_peer_timeouts();

    // The still-present peer is STILL in the denominator → quorum 2.
    assert_eq!(
        sec.live_peer_ids().cloned().collect::<Vec<_>>(),
        vec![DEAD_PEER.to_string()],
        "a peer still in transport membership must remain in the quorum \
         denominator — the fix must not over-shrink",
    );
    assert_eq!(
        failover_quorum(sec.live_peer_ids().count()),
        2,
        "with one live peer the quorum is 2 (self + the live peer)",
    );

    // Arm + tally: the survivor reaches Suspecting, but with quorum 2 and no
    // agreeing peer reply yet it does NOT self-promote — it keeps re-polling.
    let _ = sec.run_election_tick();
    assert!(matches!(
        sec.op_mut().election,
        ElectionState::Suspecting { .. }
    ));
    tokio::time::sleep(KEEPALIVE + Duration::from_millis(5)).await;
    sec.check_peer_timeouts();
    let actions = sec.run_election_tick();
    assert!(
        !actions.promoted,
        "with a LIVE peer still in the denominator the survivor must NOT \
         self-promote solo (quorum 2 unmet) — split-brain safety preserved",
    );
    assert!(matches!(
        sec.op_mut().election,
        ElectionState::Suspecting { .. }
    ));
}
