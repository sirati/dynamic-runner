//! #563 Seam 1 — election arming is suppressed by the replicated
//! run-terminal latch.
//!
//! Production trace (asm-tokenizer 2026-06-15): a primary deliberately
//! aborted via [`broadcast_terminal_verdict`] (the `SpawnRejected` path)
//! and tore its mesh down; secondaries observed the mesh-death legs (link
//! drop / membership departure) and entered the failover election. A peer
//! won, promoted itself, and re-assigned already-finalized tasks (the
//! observed "task dependency_graph assigned to secondary-1-0" right after
//! the failover line). The cluster's CRDT already carried `RunAborted`
//! (the dying primary applied + broadcast it before tearing down), but
//! `run_election_tick`'s [`need_election`] predicate consulted the mesh-
//! death legs alone — never the replicated terminal verdict.
//!
//! These tests pin the gate added in
//! `secondary/election/coordinator.rs::run_election_tick`:
//!
//!   need_election = mesh_says_dead && !primary_beacon_fresh
//!                   && !run_terminal_latched
//!
//! where `run_terminal_latched = run_aborted().is_some() || run_complete()`.
//! Replays the user-reported sequence verbatim (membership-departure
//! mesh-death leg (C), with the latch already converged on the secondary's
//! mirror via a delivered `RunAborted` broadcast), and pins:
//!
//!   - `RunAborted` latch suppresses arming (Bug B1 head fix);
//!   - `RunComplete` latch suppresses arming (the symmetric clean-finish
//!     case — the existing BUG-B mentioned in the constants' doc); and
//!   - NEGATIVE control: with NO latch, the SAME mesh-death inputs still
//!     arm the election (failover-membership BUG-H behaviour preserved —
//!     this gate does NOT regress mid-run failover).

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
#[allow(dead_code)]
const BACKSTOP: Duration = Duration::from_millis(100);

/// Bring a membership-controlled secondary into `Operational` with:
///   - `current_primary` = `PRIMARY_ID` (applied `PrimaryChanged`),
///   - a live peer (`PEER_ID`) so an armed election has a non-degraded path
///     to `Suspecting` (so a suppression assertion is honest: a `Normal`
///     verdict can NOT be a degraded-bail mask),
///   - one observed primary message so leg (C)'s `primary_last_seen.is_some()`
///     gate is satisfied (the primary PROVED liveness before leaving).
///
/// The shared membership id-set initially holds {primary, peer}. Mirrors
/// `failover_membership::operational_with_seen_primary` (the BUG-H fixture)
/// so the negative-regression assertion below is the EXACT same input
/// shape modulo the latch.
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
    sec.publish_membership();
    sec.record_primary_message();
    (sec, members)
}

/// Drop the primary out of the transport mesh — the leg (C) mesh-death
/// signal. After this, `mesh_says_dead` is true; whether the election
/// arms depends purely on the new gate.
fn drop_primary_from_membership(
    sec: &mut super::super::test_helpers::SecondaryHarness<
        super::super::test_helpers::MembershipControlPeer,
    >,
    members: &std::rc::Rc<std::cell::RefCell<Vec<PeerId>>>,
) {
    members.borrow_mut().retain(|id| id != &PeerId::from(PRIMARY_ID));
    sec.publish_membership();
}

/// Verbatim reason payload mirroring the asm-tokenizer 2026-06-15
/// trace: the wholesale spawn rejection narrates the rejected-id list.
/// The string contents are not load-bearing for THIS test (the latch is
/// presence-only); kept faithful so a future grep across the test corpus
/// reproduces the user-reported sequence.
const ABORT_REASON: &str = "runtime spawn_tasks rejected 46497 task(s): \
                            [duplicate task identity dependency_graph, ...]";

/// SEAM 1 — `RunAborted` latch suppresses election arming on the leg (C)
/// membership-departure death signal (the asm-tokenizer 2026-06-15 shape).
/// Replays the production sequence: the dying primary applied + broadcast
/// `RunAborted`, the frame landed on this secondary's mirror (the latch is
/// `Some(reason)`), and then the primary's mesh leg dropped (its
/// teardown). The next `run_election_tick` MUST stay `Normal` and emit no
/// `TimeoutQuery` — the cluster's verdict is the authority's "the run is
/// over" and the process_tasks loop-tail `run_aborted()` exit
/// (`process_tasks.rs:978`) tears this node down in the same iteration.
#[tokio::test(flavor = "current_thread")]
async fn run_aborted_latch_suppresses_membership_departure_election() {
    let (mut sec, members) = operational_with_seen_primary();

    // The cluster's RunAborted verdict lands FIRST (the dying primary's
    // broadcast reached this leg before the leg dropped).
    sec.cluster_state.apply(ClusterMutation::RunAborted {
        reason: ABORT_REASON.into(),
        counts: Default::default(),
    });
    assert!(
        sec.cluster_state.run_aborted().is_some(),
        "fixture: the RunAborted latch must be in hand before the mesh-death event",
    );

    // The dying primary's mesh leg now drops — leg (C) `mesh_says_dead` is
    // true, but the latch must short-circuit the arming.
    drop_primary_from_membership(&mut sec, &members);
    assert!(
        !sec.client.has_peer(&PeerId::from(PRIMARY_ID)),
        "fixture: leg (C) input — the primary has left the transport mesh",
    );

    let actions = sec.run_election_tick();

    assert!(
        matches!(sec.op_mut().election, ElectionState::Normal),
        "the replicated RunAborted verdict must suppress election arming; \
         got {:?}",
        std::mem::discriminant(&sec.op_mut().election),
    );
    assert!(
        actions.broadcast.is_empty(),
        "no TimeoutQuery (or any other election-broadcast) under a latched \
         RunAborted; got {} broadcasts",
        actions.broadcast.len(),
    );
    assert!(
        !actions.promoted,
        "no lone-survivor self-promotion under a latched RunAborted",
    );
    assert!(
        sec.fatal_exit.is_none(),
        "the latch suppresses the election; it does NOT fatal-bail the node \
         (the process_tasks loop-tail RunAborted check is the clean exit)",
    );
}

/// SEAM 1 — `RunComplete` latch suppresses election arming on the same
/// mesh-death leg. The clean-finish twin of the abort: a primary that
/// finished cleanly broadcasts `RunComplete` then tears down its mesh; a
/// secondary that has observed the verdict must NOT elect on the
/// subsequent link drop. This pins the symmetric half of the gate (the
/// previously-flagged BUG-B "non-promoted secondaries sit forever in
/// failover-detection mode after a clean run" — addressed for
/// process_tasks.rs:1005 already; this is the matching arming-side
/// suppression).
#[tokio::test(flavor = "current_thread")]
async fn run_complete_latch_suppresses_membership_departure_election() {
    let (mut sec, members) = operational_with_seen_primary();

    sec.cluster_state.apply(ClusterMutation::RunComplete {
        counts: Default::default(),
    });
    assert!(
        sec.cluster_state.run_complete(),
        "fixture: the RunComplete latch must be set before the mesh-death event",
    );

    drop_primary_from_membership(&mut sec, &members);

    let actions = sec.run_election_tick();

    assert!(
        matches!(sec.op_mut().election, ElectionState::Normal),
        "the replicated RunComplete verdict must suppress election arming; \
         got {:?}",
        std::mem::discriminant(&sec.op_mut().election),
    );
    assert!(
        actions.broadcast.is_empty(),
        "no TimeoutQuery under a latched RunComplete; got {} broadcasts",
        actions.broadcast.len(),
    );
    assert!(sec.fatal_exit.is_none());
}

/// SEAM 1 NEGATIVE REGRESSION — with NO terminal latch, the SAME mesh-death
/// inputs (leg (C) membership departure of a seen primary, peered survivor)
/// MUST still arm the election. This is the BUG-H idle-survivor scenario
/// preserved verbatim; if this regressed, the new gate would block ALL
/// failover. Pinned here so a future refactor that misplaced the latch
/// factor (e.g. inverted, or applied to the wrong branch) is caught.
#[tokio::test(flavor = "current_thread")]
async fn no_latch_still_elects_on_membership_departure_regression() {
    let (mut sec, members) = operational_with_seen_primary();

    assert!(
        sec.cluster_state.run_aborted().is_none(),
        "fixture: no abort latch",
    );
    assert!(
        !sec.cluster_state.run_complete(),
        "fixture: no complete latch",
    );

    drop_primary_from_membership(&mut sec, &members);

    let actions = sec.run_election_tick();
    assert!(
        matches!(sec.op_mut().election, ElectionState::Suspecting { .. }),
        "without a terminal latch, leg (C) membership-departure must still arm \
         the election (BUG-H must not regress); got {:?}",
        std::mem::discriminant(&sec.op_mut().election),
    );
    assert!(
        actions
            .broadcast
            .iter()
            .any(|m| matches!(m, DistributedMessage::TimeoutQuery { target: _, .. })),
        "the regression baseline must still broadcast a TimeoutQuery to gather \
         quorum",
    );
}

/// SEAM 1 — back-to-back ticks under the latch stay quiescent. Mirrors the
/// real keepalive ticker firing every `keepalive_interval`: under a
/// converged abort latch, no number of additional ticks must promote any
/// arm of the election state machine (a regression where the gate is
/// `Normal`-only would let `Suspecting` accidentally progress; this pin
/// guards the SUM of all four possible state arms staying inert).
#[tokio::test(flavor = "current_thread")]
async fn repeated_ticks_under_aborted_latch_stay_normal() {
    let (mut sec, members) = operational_with_seen_primary();
    sec.cluster_state.apply(ClusterMutation::RunAborted {
        reason: ABORT_REASON.into(),
        counts: Default::default(),
    });
    drop_primary_from_membership(&mut sec, &members);

    for _ in 0..5 {
        let actions = sec.run_election_tick();
        assert!(
            matches!(sec.op_mut().election, ElectionState::Normal),
            "election must stay Normal across repeated ticks under a latched \
             RunAborted (no later tick may progress an unintended arm)",
        );
        assert!(actions.broadcast.is_empty());
        assert!(!actions.promoted);
    }
    assert!(sec.fatal_exit.is_none());
}
