#![cfg(test)]

//! Failover scenarios (b), (c), (d) from the migration plan, exercised
//! at the election state-machine level. The full multi-process
//! integration tests over channels would require post-promotion task
//! takeover (re-distributing pending work from the dead primary), which
//! is not yet implemented in pure Rust — these tests cover the
//! detection + voting algorithm itself.
//!
//! Scenario (a) — secondary dies → primary requeues — is covered in
//! `crate::primary::heartbeat::tests`.

use super::super::test_helpers::{
    FakeWorkerFactory, election_config, make_secondary, make_secondary_membership,
    make_secondary_recording,
};
use super::super::wire::timestamp_now;
use super::*;
use dynrunner_protocol_primary_secondary::KeepaliveRole;
use dynrunner_protocol_primary_secondary::address::PeerId;
use std::time::Duration;

/// The death deadline given the helper's keepalive_interval (50ms) and
/// keepalive_miss_threshold (2). 100ms exact; sleep slightly over.
const PAST_DEATH: Duration = Duration::from_millis(110);
/// One full keepalive interval, the gather window for `Suspecting` to
/// progress to a vote.
const ONE_INTERVAL: Duration = Duration::from_millis(60);

// ── Phase 6a: failover-B (no-route → ALWAYS elect) + adaptive quorum ──

/// Failover-B: a "no route to primary" must ALWAYS enter the election and
/// NEVER deliberately abort a VOTER. The no-route is recorded into the
/// failover-health probe and ABSORBED into `Ok(())` (it is a failover
/// signal, not a run-fatal error). Pre-fix, `send_to_primary` returned the
/// no-route `Err`, which `?`-propagated up every operational caller
/// (`request_task_for_worker`, the worker-event TaskComplete/TaskFailed
/// reports) and aborted `process_tasks` — killing a voter on primary-loss
/// instead of letting the election run. This pins: (1) `send_to_primary`
/// returns `Ok(())` on no-route (no voter-abort), (2) the probe still arms,
/// (3) the next `run_election_tick` enters Suspecting (the recovery path),
/// (4) `fatal_exit` is NOT set (the no-route abort is gone; only the
/// `mesh_degraded` guard — a SEPARATE concern, not exercised here since a
/// peer is present — would set it).
#[tokio::test(flavor = "current_thread")]
async fn no_route_enters_election_and_never_aborts_a_voter() {
    use dynrunner_protocol_primary_secondary::ClusterMutation;
    let mut sec = make_secondary(election_config("sec-a"));
    sec.enter_operational_for_test();
    // A surviving peer so the election is non-degraded (can elect) — the
    // no-route concerns the PRIMARY link, distinct from peer-mesh liveness.
    sec.op_mut()
        .peer_keepalives
        .insert("sec-b".into(), std::time::Instant::now());
    // A recognized primary so `Destination::Primary` resolves to a concrete
    // peer id; `make_secondary`'s NoPeers transport has no member for it, so
    // every primary-bound send no-routes at the egress `has_peer` gate.
    sec.cluster_state.apply(ClusterMutation::PrimaryChanged {
        new: "primary-orig".into(),
        epoch: 1,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
    });
    sec.record_primary_message();

    // Drive the no-route past the count threshold (3 in election_config).
    // EACH send must return Ok — the no-route is absorbed, never aborting
    // the voter — while arming the failover-health probe as a side effect.
    let probe = || DistributedMessage::<super::super::test_helpers::TestId>::Keepalive {
        target: None,
        sender_id: "sec-a".into(),
        timestamp: timestamp_now(),
        secondary_id: "sec-a".into(),
        active_workers: 0,
        emitter_role: KeepaliveRole::Secondary,
    };
    for _ in 0..3 {
        assert!(
            sec.send_to_primary(probe()).await.is_ok(),
            "a no-route to primary must NOT abort the voter — it is a \
             failover signal absorbed into Ok(())",
        );
    }
    assert!(
        sec.op_mut().primary_link.should_arm_failover(),
        "three no-route sends must arm the failover-health probe",
    );

    // The recovery path: the election ENTERS Suspecting (it does not abort).
    let actions = sec.run_election_tick();
    assert!(
        matches!(sec.op_mut().election, ElectionState::Suspecting { .. }),
        "a no-route must enter the election (Suspecting), never abort; got {:?}",
        std::mem::discriminant(&sec.op_mut().election),
    );
    assert!(
        actions
            .broadcast
            .iter()
            .any(|m| matches!(m, DistributedMessage::TimeoutQuery { target: _, .. })),
        "entering Suspecting must broadcast a TimeoutQuery",
    );
    assert!(
        sec.fatal_exit.is_none(),
        "no-route with a live peer must NOT fatal-exit the voter (the \
         mesh_degraded split-brain guard is a SEPARATE, un-exercised path)",
    );
}

/// Adaptive quorum — the 2-node-trap fix. A 3-node fleet (1 primary + 2
/// secondaries) loses its primary. From a survivor's view the live-peer
/// set is the ONE other survivor (`live_peer_ids` excludes the dead
/// primary), so `failover_quorum(1) == 2`, reachable by self + the one
/// surviving peer. This is the substance of pillar 5(a): the quorum
/// denominator is the CURRENT live set (which shrank symmetrically on the
/// partition), never a fixed `config.num_secondaries`. The survivor
/// reaches quorum, self-promotes, and a single peer confirm completes the
/// promotion.
#[tokio::test(flavor = "current_thread")]
async fn two_survivor_fleet_reaches_quorum_and_promotes() {
    use dynrunner_protocol_primary_secondary::ClusterMutation;
    // The one surviving peer is BOTH keepalive-tracked AND a transport member.
    let (mut sec, _members) =
        make_secondary_membership(election_config("sec-a"), vec![PeerId::from("sec-b")]);
    sec.enter_operational_for_test();
    // Exactly ONE surviving peer → live-fleet of two (self + sec-b).
    sec.op_mut()
        .peer_keepalives
        .insert("sec-b".into(), std::time::Instant::now());
    sec.cluster_state.apply(ClusterMutation::PrimaryChanged {
        new: "primary-orig".into(),
        epoch: 1,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
    });
    sec.record_primary_message();

    // Sanity-pin the adaptive rule on this fleet size BEFORE driving it: a
    // 2-survivor fleet (live_peer_count == 1) needs quorum 2, met by self +
    // one peer.
    assert_eq!(
        failover_quorum(1),
        2,
        "a 2-survivor fleet (1 live peer) must reach quorum with self + 1 peer",
    );

    tokio::time::sleep(PAST_DEATH).await;
    let actions = sec.run_election_tick();
    assert!(matches!(
        sec.op_mut().election,
        ElectionState::Suspecting { .. }
    ));
    assert!(
        actions
            .broadcast
            .iter()
            .any(|m| matches!(m, DistributedMessage::TimeoutQuery { target: _, .. }))
    );

    tokio::time::sleep(ONE_INTERVAL).await;
    // The one surviving peer agrees the primary is silent.
    sec.record_timeout_response("sec-b".into(), None);

    // Tally: agreeing = self(1) + sec-b(1) = 2 == quorum → Candidate (sec-a
    // is the lowest live id).
    sec.run_election_tick();
    assert!(
        matches!(sec.op_mut().election, ElectionState::Candidate { .. }),
        "a 2-survivor fleet reaches quorum on the adaptive (live-fleet) rule",
    );

    // One peer confirm + the candidate's own vote = quorum 2 → promote.
    let promoted = sec.record_promotion_confirm("sec-b".into(), "sec-a".into(), 1);
    assert!(
        promoted,
        "self + one peer confirm meets the adaptive quorum (2) → promote",
    );
    assert!(matches!(sec.op_mut().election, ElectionState::Promoted));
}

/// The split-brain guard HOLDS: a genuinely-lone (zero-peer) secondary must
/// NOT self-promote on `quorum == 1`. `failover_quorum(0) == 1` is the
/// majority arithmetic for a lone node, which WOULD let it elect itself
/// solo — a split-brain. That self-promotion is blocked UPSTREAM by the
/// `mesh_degraded` guard in `run_election_tick`: with the mesh degraded and
/// the primary suspected dead, the tick FATAL-EXITS (no peer to coordinate
/// with → unsalvageable) rather than transitioning the election. This is
/// the guard the failover-B "always elect" change deliberately PRESERVES —
/// the no-route → elect path applies to a fleet WITH peers, never to a lone
/// secondary.
#[tokio::test(flavor = "current_thread")]
async fn lone_secondary_does_not_self_promote_on_quorum_one() {
    use dynrunner_protocol_primary_secondary::ClusterMutation;
    let mut sec = make_secondary(election_config("sec-a"));
    sec.enter_operational_for_test();
    // ZERO peers, and the mesh is degraded (the watchdog latched it: no
    // peer-secondary ever formed a mesh).
    sec.mesh.degraded = true;
    sec.mesh.peer_dial_count = 2;
    sec.cluster_state.apply(ClusterMutation::PrimaryChanged {
        new: "primary-orig".into(),
        epoch: 1,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
    });
    sec.record_primary_message();

    // Document the arithmetic the guard intercepts: a lone node would
    // compute quorum 1 and self-promote — exactly the split-brain the
    // mesh_degraded guard exists to stop.
    assert_eq!(
        failover_quorum(0),
        1,
        "a lone secondary's bare majority arithmetic is quorum 1 (self only) \
         — the split-brain the mesh_degraded guard blocks UPSTREAM",
    );

    // Primary suspected dead (backdate past the patient backstop).
    sec.op_mut().primary_last_seen =
        Some(std::time::Instant::now() - std::time::Duration::from_secs(60));

    // The tick hits the mesh_degraded guard BEFORE any tally: it must
    // fatal-exit, NOT transition toward Candidate/Promoted.
    let _actions = sec.run_election_tick();
    assert!(
        matches!(sec.op_mut().election, ElectionState::Normal),
        "the mesh_degraded guard must NOT transition the election (no \
         Suspecting/Candidate/Promoted); a lone secondary never tallies",
    );
    let reason = sec
        .fatal_exit
        .as_ref()
        .expect("a lone (zero-peer) secondary suspecting primary death must \
                 fatal-exit the split-brain guard, never self-promote");
    assert!(
        reason.contains("peer mesh required for failover"),
        "the guard's reason must name the degraded-failover bail; got: {reason}",
    );
}

/// Single-source quorum rule (pillar 5(a)): `failover_quorum` is the ONE
/// home for the `live_peer_count.div_ceil(2) + 1` formula, so the
/// Suspecting tally and the PromotionConfirm tally cannot desync (a desync
/// only manifests on a LIVE failover — not locally reproducible). Pin the
/// values across fleet sizes (each `live_peer_count` is the count EXCLUDING
/// the voter itself, which the callers add via `+1`):
///   - 0 live peers (lone): quorum 1 (self) — gated by mesh_degraded.
///   - 1 live peer (2-survivor): quorum 2 (self + 1) — the 2-node-trap fix.
///   - 2 live peers (3-survivor): quorum 2 (self + 1 of 2).
///   - 3 live peers (4-survivor): quorum 3 (self + 2 of 3).
#[test]
fn failover_quorum_single_source_values() {
    assert_eq!(failover_quorum(0), 1);
    assert_eq!(failover_quorum(1), 2);
    assert_eq!(failover_quorum(2), 2);
    assert_eq!(failover_quorum(3), 3);
    assert_eq!(failover_quorum(4), 3);
    assert_eq!(failover_quorum(5), 4);
}

/// Single-source guard (no duplicated logic, CLAUDE.md): the quorum formula
/// `div_ceil(2) + 1` must appear EXACTLY ONCE in the secondary source — in
/// `failover_quorum`'s body in `election/mod.rs` — and NOWHERE in
/// `election/coordinator.rs` (both the Suspecting tally and the
/// PromotionConfirm tally must read the function, not re-spell the formula).
/// A re-introduced copy would desync only on a live failover (not locally
/// reproducible), so this test catches the regression at compile-test time.
/// Matches the code token `.div_ceil(2)` — robust to comments mentioning
/// the formula because those write `peer_count.div_ceil(2)` / a literal
/// digit, not the bare method-call token on its own.
#[test]
fn failover_quorum_formula_is_single_source() {
    let coordinator = include_str!("coordinator.rs");
    let coord_hits = coordinator.matches(".div_ceil(2)").count();
    assert_eq!(
        coord_hits, 0,
        "election/coordinator.rs must NOT spell the quorum formula — both \
         tally sites read `failover_quorum`; found {coord_hits} occurrence(s)",
    );

    let election_mod = include_str!("mod.rs");
    // Count the formula as a CODE token (`div_ceil(2) + 1`), not the prose
    // mentions of it in the function's doc comment.
    let mod_hits = election_mod.matches("div_ceil(2) + 1").count();
    // The body has it once; the doc comment mentions `div_ceil(2) + 1` in
    // prose, so the total is the body + the prose mentions — the load-bearing
    // assertion is the ZERO in coordinator.rs above. Here we only pin that
    // the rule still LIVES in mod.rs (≥1), so a future move that empties
    // mod.rs without updating this test fails loud.
    assert!(
        mod_hits >= 1,
        "the quorum rule must live in election/mod.rs (failover_quorum); \
         found {mod_hits} occurrence(s)",
    );
}

/// Scenario (b): primary stops sending keepalives. The lowest-id
/// secondary observes the death, runs the election, collects quorum,
/// and promotes itself.
#[tokio::test(flavor = "current_thread")]
async fn primary_dies_lowest_id_promotes() {
    use dynrunner_protocol_primary_secondary::ClusterMutation;
    // Live peers are BOTH keepalive-tracked AND transport members (the
    // production invariant `live_peer_ids` now enforces: a peer counts toward
    // quorum only while it is a connected mesh member).
    let (mut sec, _members) = make_secondary_membership(
        election_config("sec-a"),
        vec![PeerId::from("sec-b"), PeerId::from("sec-c")],
    );
    sec.enter_operational_for_test();
    sec.op_mut()
        .peer_keepalives
        .insert("sec-b".into(), std::time::Instant::now());
    sec.op_mut()
        .peer_keepalives
        .insert("sec-c".into(), std::time::Instant::now());
    // Post-uniform-announce a secondary always knows the primary's
    // identity before it can suspect that primary's death; the
    // Suspecting `TimeoutQuery` names it. Install it via the real apply
    // path so `current_primary()` is `Some`.
    sec.cluster_state.apply(ClusterMutation::PrimaryChanged {
        new: "primary-orig".into(),
        epoch: 1,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
    });
    sec.record_primary_message();

    tokio::time::sleep(PAST_DEATH).await;

    // First tick: enter Suspecting and broadcast TimeoutQuery.
    let actions = sec.run_election_tick();
    assert!(matches!(
        sec.op_mut().election,
        ElectionState::Suspecting { .. }
    ));
    assert!(
        actions
            .broadcast
            .iter()
            .any(|m| matches!(m, DistributedMessage::TimeoutQuery { target: _, .. }))
    );

    // Wait the gather window so the Suspecting tick is eligible to vote.
    tokio::time::sleep(ONE_INTERVAL).await;

    // Peers report primary silent (None means "haven't seen recently").
    sec.record_timeout_response("sec-b".into(), None);
    sec.record_timeout_response("sec-c".into(), None);

    // Second tick: tally quorum, transition Suspecting → Candidate
    // (sec-a is the lowest id), and broadcast PromotionVote.
    let actions = sec.run_election_tick();
    assert!(matches!(
        sec.op_mut().election,
        ElectionState::Candidate { .. }
    ));
    assert!(
        actions
            .broadcast
            .iter()
            .any(|m| matches!(m, DistributedMessage::PromotionVote { target: _, .. }))
    );

    // One peer confirms — combined with the candidate's own vote that
    // is the quorum (peer_count=2 → quorum=2).
    let promoted = sec.record_promotion_confirm("sec-b".into(), "sec-a".into(), 1);
    assert!(promoted, "majority confirm should promote");
    assert!(matches!(sec.op_mut().election, ElectionState::Promoted));
}

/// Scenario (c): with four peers including self, one peer is dead at
/// the same time as the primary. The election still has quorum from
/// the remaining three live secondaries.
#[tokio::test(flavor = "current_thread")]
async fn double_failure_election_still_succeeds() {
    // All three peers are still connected mesh MEMBERS; sec-d is
    // application-silent (won't answer the TimeoutQuery) but remains a
    // member, so it stays in the quorum denominator (the slow-reaper backstop
    // case, NOT a membership departure) — peer_count = 3, quorum = 3.
    let (mut sec, _members) = make_secondary_membership(
        election_config("sec-a"),
        vec![
            PeerId::from("sec-b"),
            PeerId::from("sec-c"),
            PeerId::from("sec-d"),
        ],
    );
    sec.enter_operational_for_test();
    sec.op_mut()
        .peer_keepalives
        .insert("sec-b".into(), std::time::Instant::now());
    sec.op_mut()
        .peer_keepalives
        .insert("sec-c".into(), std::time::Instant::now());
    sec.op_mut()
        .peer_keepalives
        .insert("sec-d".into(), std::time::Instant::now()); // will not respond
    sec.record_primary_message();

    tokio::time::sleep(PAST_DEATH).await;
    sec.run_election_tick();
    tokio::time::sleep(ONE_INTERVAL).await;

    // Only b and c respond; d is silent.
    sec.record_timeout_response("sec-b".into(), None);
    sec.record_timeout_response("sec-c".into(), None);

    sec.run_election_tick();
    assert!(
        matches!(sec.op_mut().election, ElectionState::Candidate { .. }),
        "quorum (3 of 4) reached even with one peer dead"
    );

    // Confirm quorum for promotion: peer_count=3 → quorum=3, candidate
    // counts itself, needs two peer confirms.
    sec.record_promotion_confirm("sec-b".into(), "sec-a".into(), 1);
    let promoted = sec.record_promotion_confirm("sec-c".into(), "sec-a".into(), 1);
    assert!(promoted, "two peer confirms + self = quorum");
    assert!(matches!(sec.op_mut().election, ElectionState::Promoted));
}

/// `record_primary_message` resets the failover election state to
/// Normal — the live primary is alive again, so the secondary stops
/// suspecting / voting. ("Who is primary" is the replicated
/// `cluster_state.current_primary()`; a live keepalive resets the
/// ELECTION, never that primary identity.)
#[tokio::test(flavor = "current_thread")]
async fn primary_recovery_resets_election_state() {
    let mut sec = make_secondary(election_config("sec-a"));
    sec.enter_operational_for_test();
    sec.op_mut().election = ElectionState::Voting {
        round: 1,
        candidate: "sec-c".into(),
    };
    sec.record_primary_message();
    assert!(matches!(sec.op_mut().election, ElectionState::Normal));
}

/// `Promoted` state survives a `record_primary_message`: once we've
/// taken over, a stray late message from the dead primary doesn't
/// dethrone us.
#[tokio::test(flavor = "current_thread")]
async fn promoted_state_survives_late_primary_message() {
    let mut sec = make_secondary(election_config("sec-a"));
    sec.enter_operational_for_test();
    sec.op_mut().election = ElectionState::Promoted;

    sec.record_primary_message();
    assert!(matches!(sec.op_mut().election, ElectionState::Promoted));
}

/// Regression: a `PrimaryChanged` routing target survives
/// subsequent live-primary keepalives. Pre-fix
/// `record_primary_message` unconditionally cleared the
/// current-primary identity whenever the live primary kept
/// sending keepalives, so `send_to_primary` on
/// non-primary secondaries fell back to `primary_transport`
/// (the demoted local primary) instead of unicasting to the
/// SLURM-primary peer.
/// Dispatch worked only as long as the local primary's
/// `handle_task_request` relay path stayed alive; once its
/// transport closed (laptop suspend / SSH idle) the relay
/// vanished and TaskRequests stopped reaching the SLURM-primary,
/// idling the entire fleet. Surfaced in dataset's K=2 hello run
/// after 95b9f32 — synchronous primary-changed state-sync was
/// correct but the very next keepalive clobbered the routing
/// target.
#[tokio::test(flavor = "current_thread")]
async fn promote_primary_routing_survives_keepalive() {
    use dynrunner_protocol_primary_secondary::ClusterMutation;
    let mut sec = make_secondary(election_config("sec-b"));
    sec.enter_operational_for_test();
    // Receive a `PrimaryChanged` naming a peer (sec-a) as the
    // SLURM-primary; sec-b is a regular peer.
    let promote = DistributedMessage::ClusterMutation {
        target: None,
        sender_id: "setup".into(),
        timestamp: 0.0,
        mutations: vec![ClusterMutation::PrimaryChanged {
            new: "sec-a".into(),
            epoch: 1,
            reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
        }],
    };
    sec.dispatch_message(promote, &mut FakeWorkerFactory)
        .await
        .expect("PrimaryChanged handler succeeds");
    assert_eq!(sec.cluster_state.current_primary(), Some("sec-a"));
    // The (still-alive, now-demoted) local primary keeps sending
    // keepalives. A live-primary keepalive resets the election but
    // must NOT clobber the replicated primary identity (the
    // PrimaryChanged apply is last-writer-wins on epoch).
    sec.record_primary_message();
    assert_eq!(
        sec.cluster_state.current_primary(),
        Some("sec-a"),
        "live-primary keepalive must not clobber the explicit handoff target",
    );
}

/// A node NAMED primary by a `PrimaryChanged { new = self }`
/// installs itself as `current_primary` AND resets its failover
/// election to `Normal` (NOT `Promoted`): a peer becomes primary by
/// its HOST spawning a primary coordinator alongside its unchanged
/// normal secondary, and after the spawn the election state resets —
/// there is no lingering `Promoted`. Post-reset, the same-peer
/// primary's OWN keepalives (recognized because `current_primary()`
/// names this node) keep `primary_last_seen` fresh, so the node stays
/// `Normal` and drives no self-re-promotion cascade.
///
/// Drives the real `dispatch_message` `ClusterMutation` arm so the
/// test exercises the unified `apply_primary_changed` hook.
#[tokio::test(flavor = "current_thread")]
async fn self_named_primary_resets_election_to_normal() {
    use dynrunner_protocol_primary_secondary::{ClusterMutation, KeepaliveRole};
    let mut sec = make_secondary(election_config("sec-a"));
    sec.enter_operational_for_test();
    // Pre-naming: Normal state, this node is not yet primary.
    assert!(matches!(sec.op_mut().election, ElectionState::Normal));
    assert!(sec.cluster_state.current_primary().is_none());

    // Receive a `PrimaryChanged` naming this node — exercises the
    // unified hook that installs the role into the CRDT (so
    // `current_primary()` now names this node) AND resets the election
    // to Normal (no lingering Promoted).
    let promote = DistributedMessage::ClusterMutation {
        target: None,
        sender_id: "setup".into(),
        timestamp: 0.0,
        mutations: vec![ClusterMutation::PrimaryChanged {
            new: "sec-a".into(),
            epoch: 1,
            reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
        }],
    };
    sec.dispatch_message(promote, &mut FakeWorkerFactory)
        .await
        .expect("PrimaryChanged handler succeeds");
    assert_eq!(sec.cluster_state.current_primary(), Some("sec-a"));
    assert!(
        matches!(sec.op_mut().election, ElectionState::Normal),
        "self-named primary resets to Normal, not Promoted"
    );

    // The same-peer primary's own keepalives (recognized: `from` ==
    // current_primary == self) keep `primary_last_seen` fresh — the
    // node stays Normal and originates no election even after the
    // keepalive cadence ticks.
    for _ in 0..5 {
        sec.handle_inbound(
            keepalive_from("sec-a", KeepaliveRole::Primary),
            &mut FakeWorkerFactory,
        )
        .await;
        let actions = sec.run_election_tick();
        assert!(
            matches!(sec.op_mut().election, ElectionState::Normal),
            "a self-named primary fed its own keepalives stays Normal; got {:?}",
            std::mem::discriminant(&sec.op_mut().election),
        );
        assert!(
            actions.broadcast.is_empty(),
            "no spurious election broadcast while the same-peer primary is healthy",
        );
    }
    assert_eq!(sec.cluster_state.current_primary(), Some("sec-a"));
}

/// C4 seam coverage (re-added `promotion_confirm_true_fires_activation_
/// and_rebroadcasts`, rewired for the one-mesh signal model): an election
/// win that names THIS node FIRES a typed `PromotionSignal` on the
/// `promotion_tx` — the secondary NEVER builds a primary itself
/// (SUPREME-LAW #3) — AND advances the CRDT primary identity AND
/// rebroadcasts the `PrimaryChanged` so surviving peers re-point. The
/// ACTIVATION half (building the primary) is now the `Node`'s concern off
/// the signal; here we assert the signal fires, the identity advances, and
/// the rebroadcast lands.
///
/// Drives `fire_local_promotion` (the election-win terminal action), which
/// routes through `apply_cluster_mutations` → `apply_primary_changed` (the
/// C4 fire site) for the local apply, then broadcasts the re-point.
#[tokio::test(flavor = "current_thread")]
async fn self_named_election_fires_promotion_signal_advances_identity_and_rebroadcasts() {
    use dynrunner_protocol_primary_secondary::{ClusterMutation, PrimaryChangeReason};

    // A recording harness so the rebroadcast (a `Destination::All`
    // `PrimaryChanged`) is observable in the log after `drain_egress`; the
    // harness's `promotion_rx` is the C4 signal receiver.
    let (mut sec, log) = make_secondary_recording(election_config("sec-a"), 1);
    sec.enter_operational_for_test();
    // Pre-promotion: Normal, no primary identity, no signal yet.
    assert!(matches!(sec.op_mut().election, ElectionState::Normal));
    assert!(sec.cluster_state.current_primary().is_none());
    assert!(
        sec.promotion_rx.try_recv().is_err(),
        "no promotion signal before the election win",
    );

    // Win the failover election: the terminal action originates + applies +
    // broadcasts `PrimaryChanged { new = self, reason = Election }`.
    sec.fire_local_promotion().await;
    sec.drain_egress().await;

    // (1) The C4 promotion signal FIRED on the `promotion_tx` — the
    // secondary signalled the `Node` to build the primary; it did NOT build
    // one itself.
    let signal = sec
        .promotion_rx
        .try_recv()
        .expect("a self-named election win must FIRE exactly one PromotionSignal");
    assert_eq!(
        signal.reason,
        PrimaryChangeReason::Election,
        "the election-win signal carries the Election reason",
    );
    assert_eq!(
        signal.epoch,
        sec.cluster_state.primary_epoch(),
        "the signal carries the role-table epoch the promotion was raised at",
    );
    assert!(
        sec.promotion_rx.try_recv().is_err(),
        "exactly ONE signal per self-named promotion",
    );

    // (2) The CRDT primary identity advanced onto this node.
    assert_eq!(
        sec.cluster_state.current_primary(),
        Some("sec-a"),
        "the self-named PrimaryChanged advances the recognized primary identity",
    );
    // ... and the election reset to Normal (a primary now exists).
    assert!(matches!(sec.op_mut().election, ElectionState::Normal));

    // (3) The re-point was REBROADCAST so surviving peers move their
    // `current_primary()` onto this winner.
    let rebroadcast = log.borrow().iter().any(|m| {
        matches!(
            m,
            DistributedMessage::ClusterMutation { target: _, mutations, .. }
                if mutations.iter().any(|mu| matches!(
                    mu,
                    ClusterMutation::PrimaryChanged { new, reason, .. }
                        if new == "sec-a" && *reason == PrimaryChangeReason::Election
                ))
        )
    });
    assert!(
        rebroadcast,
        "the election win must rebroadcast PrimaryChanged(new=self); captured: {:?}",
        log.borrow(),
    );
}

/// Phase P: a `PrimaryChanged` clears any per-worker backoff accrued
/// against the previous primary. Without this, idle workers sit
/// through a stale window before re-issuing at the new primary,
/// reproducing the dispatch-silence symptom from the trace at
/// `feb1052`.
#[tokio::test(flavor = "current_thread")]
async fn primary_changed_clears_per_worker_backoff() {
    use dynrunner_protocol_primary_secondary::ClusterMutation;
    let mut sec = make_secondary(election_config("sec-b"));
    sec.enter_operational_for_test();
    // Simulate per-worker backoff accrued against the old primary.
    sec.op_mut().primary_link.note_request_sent(0);
    sec.op_mut().primary_link.note_request_sent(1);
    assert!(!sec.op_mut().primary_link.should_request_now(0));
    assert!(!sec.op_mut().primary_link.should_request_now(1));

    let promote = DistributedMessage::ClusterMutation {
        target: None,
        sender_id: "setup".into(),
        timestamp: 0.0,
        mutations: vec![ClusterMutation::PrimaryChanged {
            new: "sec-a".into(),
            epoch: 1,
            reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
        }],
    };
    sec.dispatch_message(promote, &mut FakeWorkerFactory)
        .await
        .expect("PrimaryChanged handler succeeds");

    // Both workers can fire a fresh request immediately at the
    // new primary.
    assert!(sec.op_mut().primary_link.should_request_now(0));
    assert!(sec.op_mut().primary_link.should_request_now(1));
}

/// Phase P: a `PrimaryChanged` feeds (epoch, primary) into the
/// replicated `cluster_state`, where last-writer-wins on
/// `(epoch, primary_id)` makes a stale lower-epoch broadcast a
/// no-op against an already-installed higher-epoch change.
#[tokio::test(flavor = "current_thread")]
async fn primary_changed_applies_with_epoch_lww() {
    use dynrunner_protocol_primary_secondary::ClusterMutation;
    let mut sec = make_secondary(election_config("sec-b"));
    sec.enter_operational_for_test();

    let high = DistributedMessage::ClusterMutation {
        target: None,
        sender_id: "setup".into(),
        timestamp: 0.0,
        mutations: vec![ClusterMutation::PrimaryChanged {
            new: "sec-c".into(),
            epoch: 5,
            reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
        }],
    };
    sec.dispatch_message(high, &mut FakeWorkerFactory)
        .await
        .unwrap();
    assert_eq!(sec.cluster_state.current_primary(), Some("sec-c"));
    assert_eq!(sec.cluster_state.primary_epoch(), 5);

    // A late lower-epoch broadcast must not clobber the higher
    // epoch already installed.
    let stale = DistributedMessage::ClusterMutation {
        target: None,
        sender_id: "setup".into(),
        timestamp: 0.0,
        mutations: vec![ClusterMutation::PrimaryChanged {
            new: "sec-a".into(),
            epoch: 2,
            reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
        }],
    };
    sec.dispatch_message(stale, &mut FakeWorkerFactory)
        .await
        .unwrap();
    assert_eq!(
        sec.cluster_state.current_primary(),
        Some("sec-c"),
        "stale lower-epoch PrimaryChanged must not supersede higher epoch"
    );
    assert_eq!(sec.cluster_state.primary_epoch(), 5);
}

/// Scenario (d): two peers detect primary death simultaneously and both
/// would-be-candidates start voting. The lowest-id rule + quorum
/// resolves to a single winner; the higher-id peer defers to Voting
/// instead of becoming Candidate.
#[tokio::test(flavor = "current_thread")]
async fn split_brain_lowest_id_wins() {
    // Each survivor's peers are BOTH keepalive-tracked AND transport members.
    let (mut sec_a, _members_a) = make_secondary_membership(
        election_config("sec-a"),
        vec![PeerId::from("sec-b"), PeerId::from("sec-c")],
    );
    sec_a.enter_operational_for_test();
    sec_a
        .op_mut()
        .peer_keepalives
        .insert("sec-b".into(), std::time::Instant::now());
    sec_a
        .op_mut()
        .peer_keepalives
        .insert("sec-c".into(), std::time::Instant::now());
    sec_a.record_primary_message();

    let (mut sec_b, _members_b) = make_secondary_membership(
        election_config("sec-b"),
        vec![PeerId::from("sec-a"), PeerId::from("sec-c")],
    );
    sec_b.enter_operational_for_test();
    sec_b
        .op_mut()
        .peer_keepalives
        .insert("sec-a".into(), std::time::Instant::now());
    sec_b
        .op_mut()
        .peer_keepalives
        .insert("sec-c".into(), std::time::Instant::now());
    sec_b.record_primary_message();

    tokio::time::sleep(PAST_DEATH).await;

    // Both detect primary death simultaneously and enter Suspecting.
    sec_a.run_election_tick();
    sec_b.run_election_tick();

    tokio::time::sleep(ONE_INTERVAL).await;

    // Both gather peer responses.
    sec_a.record_timeout_response("sec-b".into(), None);
    sec_a.record_timeout_response("sec-c".into(), None);
    sec_b.record_timeout_response("sec-a".into(), None);
    sec_b.record_timeout_response("sec-c".into(), None);

    // Tally + decide: sec-a is lowest in its peer set → Candidate.
    // sec-b sees sec-a as lowest in its peer set → Voting.
    sec_a.run_election_tick();
    sec_b.run_election_tick();

    assert!(
        matches!(sec_a.op_mut().election, ElectionState::Candidate { .. }),
        "sec-a (lowest id) should self-promote"
    );
    match &sec_b.op_mut().election {
        ElectionState::Voting { candidate, .. } => assert_eq!(candidate, "sec-a"),
        other => panic!(
            "sec-b should defer to sec-a, got {:?}",
            std::mem::discriminant(other)
        ),
    }

    // sec-b confirms sec-a; quorum 2 (peer_count=2). sec-a + sec-b = 2.
    let promoted = sec_a.record_promotion_confirm("sec-b".into(), "sec-a".into(), 1);
    assert!(promoted);
    assert!(matches!(sec_a.op_mut().election, ElectionState::Promoted));
    assert!(
        !matches!(sec_b.op_mut().election, ElectionState::Promoted),
        "sec-b must NOT also promote — split-brain prevented"
    );
}

// ── Post-failover regression (A-M0a election liveness) ──
//
// After A-M0a, a keepalive whose originator IS the current primary is
// recognized in `handle_inbound` and routed through
// `record_primary_message` → `primary_last_seen` (NOT `peer_keepalives`).
// A code audit found `run_election_tick`'s former `primary_peer_silent`
// branch read the promoted primary's `peer_keepalives` entry — which is
// never populated for the current primary post-A-M0a — so its
// `unwrap_or(true)` tripped a SPURIOUS election against a HEALTHY
// just-promoted peer-primary, risking double-promotion. These tests pin
// that a healthy promoted peer-primary drives NO election while its
// keepalives flow, and a genuinely-dead one still does.

/// Build a Keepalive whose originator is `from`, tagged with the emitter
/// `role`. Fed through the real `handle_inbound` recognition path: a
/// `Primary`-tagged keepalive whose `from` IS the current primary
/// refreshes `primary_last_seen` via `record_primary_message`; a
/// `Secondary`-tagged keepalive always files a `peer_keepalives` entry —
/// exactly the production role-tagged split this regression depends on.
fn keepalive_from(
    from: &str,
    role: KeepaliveRole,
) -> DistributedMessage<super::super::test_helpers::TestId> {
    DistributedMessage::Keepalive {
        target: None,
        sender_id: from.to_string(),
        timestamp: timestamp_now(),
        secondary_id: from.to_string(),
        active_workers: 0,
        emitter_role: role,
    }
}

/// Drive a real `PrimaryChanged` naming a PEER as the new primary, then
/// stream that peer's keepalives (recognized → `primary_last_seen` kept
/// fresh): `run_election_tick` MUST NOT enter Suspecting or broadcast a
/// `TimeoutQuery` while the promoted primary is healthy. Then simulate
/// the promoted primary dying (backdate `primary_last_seen` past the
/// death deadline with no further keepalive): the election MUST fire —
/// proving `primary_silent` ALONE covers the cascade (promoted-peer-
/// died) case the deleted `primary_peer_silent` branch used to chase,
/// with no spurious election while healthy.
///
/// Deterministic time: the staleness predicate (`primary_silent`) reads
/// `std::time::Instant` (NOT a tokio timer), so death is simulated by an
/// explicit backdated `primary_last_seen` — no wall-clock racing, no
/// dependence on the tokio paused-time clock.
#[tokio::test(flavor = "current_thread")]
async fn promoted_peer_primary_healthy_no_election_then_dead_fires() {
    use dynrunner_protocol_primary_secondary::ClusterMutation;
    // Membership-backed harness with the promoted primary (`sec-a`) AND the
    // surviving peer (`sec-c`) seeded as transport members: this test asserts
    // a HEALTHY promoted primary keeps us Normal, so leg (C)
    // (primary-left-membership) must read the primary as PRESENT — the death
    // here is driven by leg (B) (backdated `primary_last_seen`), not by
    // membership. (Dedicated leg-(C) coverage lives in `failover_membership`.)
    let (mut sec, _members) = make_secondary_membership(
        election_config("sec-b"),
        vec![PeerId::from("sec-a"), PeerId::from("sec-c")],
    );
    sec.enter_operational_for_test();
    // A surviving mesh peer so the election path is non-degraded and
    // can actually broadcast `TimeoutQuery` when the primary dies.
    sec.op_mut()
        .peer_keepalives
        .insert("sec-c".into(), std::time::Instant::now());

    // A peer (sec-a) is promoted to primary via the real apply path.
    let promote = DistributedMessage::ClusterMutation {
        target: None,
        sender_id: "setup".into(),
        timestamp: 0.0,
        mutations: vec![ClusterMutation::PrimaryChanged {
            new: "sec-a".into(),
            epoch: 1,
            reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
        }],
    };
    sec.dispatch_message(promote, &mut FakeWorkerFactory)
        .await
        .expect("PrimaryChanged handler succeeds");
    assert_eq!(sec.cluster_state.current_primary(), Some("sec-a"));

    // Healthy: each beat the promoted primary's keepalive is recognized
    // (refreshes `primary_last_seen`); the tick must stay Normal and
    // originate no `TimeoutQuery`. Pre-fix this stormed `TimeoutQuery`
    // immediately because `primary_peer_silent` read the (never-
    // populated) `peer_keepalives["sec-a"]` and `unwrap_or(true)`.
    for _ in 0..5 {
        sec.handle_inbound(
            keepalive_from("sec-a", KeepaliveRole::Primary),
            &mut FakeWorkerFactory,
        )
        .await;
        let actions = sec.run_election_tick();
        assert!(
            matches!(sec.op_mut().election, ElectionState::Normal),
            "healthy promoted primary must keep us Normal; got {:?}",
            std::mem::discriminant(&sec.op_mut().election),
        );
        assert!(
            !actions
                .broadcast
                .iter()
                .any(|m| matches!(m, DistributedMessage::TimeoutQuery { target: _, .. })),
            "no spurious TimeoutQuery against a healthy promoted primary",
        );
    }

    // The promoted primary dies: NO further keepalive refreshes
    // `primary_last_seen`. Backdate it well past the death deadline
    // (keepalive_interval * miss_threshold) — the next tick must enter
    // Suspecting and broadcast a `TimeoutQuery`. The genuinely-dead
    // promoted primary IS detected, via `primary_silent`.
    sec.op_mut().primary_last_seen =
        Some(std::time::Instant::now() - std::time::Duration::from_secs(60));
    let actions = sec.run_election_tick();
    assert!(
        matches!(sec.op_mut().election, ElectionState::Suspecting { .. }),
        "a dead promoted primary must trigger the election (Suspecting); got {:?}",
        std::mem::discriminant(&sec.op_mut().election),
    );
    assert!(
        actions
            .broadcast
            .iter()
            .any(|m| matches!(m, DistributedMessage::TimeoutQuery { target: _, .. })),
        "the election must broadcast a TimeoutQuery once the promoted primary is silent",
    );
}

/// `check_peer_timeouts` must NOT prune the ALIVE promoted primary's
/// stale PRE-promotion `peer_keepalives` entry — the current primary is
/// not a peer for liveness purposes. A regular stale peer is still
/// pruned (control), proving the skip is scoped to the current primary,
/// not a blanket disable.
#[tokio::test(flavor = "current_thread")]
async fn check_peer_timeouts_skips_alive_promoted_primary() {
    use dynrunner_protocol_primary_secondary::ClusterMutation;
    use std::time::{Duration, Instant};
    // Tiny `peer_timeout` so a modestly-backdated receipt `Instant` is
    // unconditionally stale (peer liveness now keys off a monotonic receipt
    // `Instant`, not an epoch wall-clock timestamp).
    let mut cfg = election_config("sec-b");
    cfg.peer_timeout = Duration::from_millis(1);
    let mut sec = make_secondary(cfg);
    sec.enter_operational_for_test();
    // Both entries are backdated well past the (1ms) peer_timeout, so the
    // only thing that can spare one is the current-primary skip.
    let stale = Instant::now() - Duration::from_secs(60);
    sec.op_mut().peer_keepalives.insert("sec-a".into(), stale);
    sec.op_mut().peer_keepalives.insert("sec-z".into(), stale);

    // sec-a is promoted to primary via the real apply path. Its
    // pre-promotion `peer_keepalives` entry is now stale-but-alive.
    let promote = DistributedMessage::ClusterMutation {
        target: None,
        sender_id: "setup".into(),
        timestamp: 0.0,
        mutations: vec![ClusterMutation::PrimaryChanged {
            new: "sec-a".into(),
            epoch: 1,
            reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
        }],
    };
    sec.dispatch_message(promote, &mut FakeWorkerFactory)
        .await
        .expect("PrimaryChanged handler succeeds");
    assert_eq!(sec.cluster_state.current_primary(), Some("sec-a"));

    sec.check_peer_timeouts();

    assert!(
        sec.op_mut().peer_keepalives.contains_key("sec-a"),
        "the ALIVE promoted primary's entry must NOT be pruned — \
             it is not a peer for liveness purposes",
    );
    assert!(
        !sec.op_mut().peer_keepalives.contains_key("sec-z"),
        "a genuinely-stale regular peer is still pruned (skip is \
             scoped to the current primary, not a blanket disable)",
    );
}

/// Suspend/resume SIGNATURE: a coordinated host suspend makes the wall
/// clock jump forward, so a peer's last pre-suspend keepalive carries a
/// WIRE timestamp that is now ancient relative to the resumed wall clock.
/// Pre-fix, peer-liveness keyed on that wire timestamp, so every peer
/// exceeded `peer_timeout` at once → mass-prune → false mesh-degraded →
/// every secondary `fatal_exit`s the failover guard (surviving=0). With
/// liveness keyed on the LOCAL receipt-time monotonic `Instant`, an
/// ancient-wire-but-just-received keepalive does NOT prune; only a peer
/// whose RECEIPT `Instant` is genuinely old (real death) is pruned.
///
/// We cannot actually suspend in a test, so we reproduce the signature:
/// the keepalive flows through the real `handle_inbound` receipt path
/// (receipt `Instant` = now) carrying an ANCIENT wire `timestamp`
/// (`timestamp_now() - 100000.0`). Tiny `peer_timeout` proves the wire
/// staleness would have pruned under the old wall-clock keying.
#[tokio::test(flavor = "current_thread")]
async fn check_peer_timeouts_keys_on_receipt_not_wire_timestamp() {
    use std::time::{Duration, Instant};
    // Tiny peer_timeout: under the OLD wall-clock keying the ancient wire
    // timestamp (100000s in the past) would be pruned instantly. Under the
    // monotonic receipt keying, a just-received keepalive is fresh.
    let mut cfg = election_config("sec-a");
    cfg.peer_timeout = Duration::from_secs(120);
    // sec-b is a connected mesh member (the live peer); sec-z is genuinely
    // dead (departed) so it is NOT a member and must reap.
    let (mut sec, _members) = make_secondary_membership(cfg, vec![PeerId::from("sec-b")]);
    sec.enter_operational_for_test();

    // A live peer (sec-b) whose pre-suspend keepalive carries an ANCIENT
    // wire timestamp but is received RIGHT NOW. Drive it through the real
    // recognition path so the receipt `Instant` is stamped locally.
    let ancient_wire = timestamp_now() - 100_000.0;
    sec.handle_inbound(
        DistributedMessage::Keepalive {
            target: None,
            sender_id: "sec-b".into(),
            timestamp: ancient_wire,
            secondary_id: "sec-b".into(),
            active_workers: 0,
            emitter_role: KeepaliveRole::Secondary,
        },
        &mut FakeWorkerFactory,
    )
    .await;

    // A genuinely-dead peer (sec-z): backdate its receipt `Instant` past
    // the peer_timeout. This is real death — it MUST still be pruned.
    let dead_receipt = Instant::now() - Duration::from_secs(200);
    sec.op_mut()
        .peer_keepalives
        .insert("sec-z".into(), dead_receipt);

    sec.check_peer_timeouts();

    // The ancient-WIRE-but-fresh-RECEIPT peer survives: no false prune, so
    // no false mesh-degraded and the failover guard never spuriously fires.
    assert!(
        sec.op_mut().peer_keepalives.contains_key("sec-b"),
        "a peer with an ancient WIRE timestamp but a fresh RECEIPT Instant \
         must NOT be pruned — peer-liveness keys on local receipt time, so \
         a coordinated suspend/resume wall-clock jump cannot mass-prune",
    );
    assert!(
        sec.live_peer_ids().any(|id| id == "sec-b"),
        "the suspend-surviving peer must remain in the live-peer set",
    );
    assert_eq!(
        sec.alive_secondary_count(),
        1,
        "alive_secondary_count stays intact (only the genuinely-dead peer drops)",
    );
    // Genuine death is still detected: a peer with an old RECEIPT Instant
    // is pruned.
    assert!(
        !sec.op_mut().peer_keepalives.contains_key("sec-z"),
        "a peer whose RECEIPT Instant is older than peer_timeout is genuinely \
         dead and MUST still be pruned",
    );
}

/// Suspecting-tally SIGNATURE: a peer that still sees the primary alive
/// reports a SMALL staleness age in its `TimeoutResponse`, so it must NOT
/// count toward the primary-death quorum — even across a suspend/resume,
/// because the age is relative to the responder's own monotonic clock and
/// never subtracted from this node's (post-resume, jumped) wall clock.
/// A `None` (never saw the primary) still counts as agreeing.
#[tokio::test(flavor = "current_thread")]
async fn suspecting_tally_keys_on_relative_age_not_wall_clock() {
    use dynrunner_protocol_primary_secondary::ClusterMutation;
    // Two peers (both connected members) so peer_count = 2 → quorum = 2.
    let (mut sec, _members) = make_secondary_membership(
        election_config("sec-a"),
        vec![PeerId::from("sec-b"), PeerId::from("sec-c")],
    );
    sec.enter_operational_for_test();
    // Two peers so peer_count = 2 → quorum = 2 (self + one agreeing peer).
    sec.op_mut()
        .peer_keepalives
        .insert("sec-b".into(), std::time::Instant::now());
    sec.op_mut()
        .peer_keepalives
        .insert("sec-c".into(), std::time::Instant::now());
    sec.cluster_state.apply(ClusterMutation::PrimaryChanged {
        new: "primary-orig".into(),
        epoch: 1,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
    });
    sec.record_primary_message();

    tokio::time::sleep(PAST_DEATH).await;
    sec.run_election_tick(); // → Suspecting, broadcast TimeoutQuery
    assert!(matches!(
        sec.op_mut().election,
        ElectionState::Suspecting { .. }
    ));
    tokio::time::sleep(ONE_INTERVAL).await;

    // sec-b STILL sees the primary alive: it reports a tiny staleness age
    // (well within the death deadline). sec-c never saw it (None → agrees).
    sec.record_timeout_response("sec-b".into(), Some(0.0));
    sec.record_timeout_response("sec-c".into(), None);

    // agreeing = self(1) + sec-c(None→agrees) = 2; sec-b's fresh age does
    // NOT agree. quorum = 2. self+sec-c exactly meets it, so we DO proceed —
    // but the load-bearing assertion is that sec-b's SMALL age was treated
    // as "primary still alive" (not agreeing), proving the relative-age
    // comparison, not a wall-clock subtraction that a suspend would inflate.
    sec.run_election_tick();
    // With sec-c agreeing we reach quorum and move past Suspecting; the
    // fresh-age peer simply didn't inflate the tally.
    assert!(
        !matches!(sec.op_mut().election, ElectionState::Suspecting { .. }),
        "quorum (self + the None-reporting peer) is reached; tally proceeds",
    );

    // Now the contrast in isolation: a fresh age alone must NOT reach
    // quorum. Re-arm a clean Suspecting with ONLY a fresh-age responder.
    let (mut sec2, _members2) =
        make_secondary_membership(election_config("sec-a"), vec![PeerId::from("sec-b")]);
    sec2.enter_operational_for_test();
    sec2.op_mut()
        .peer_keepalives
        .insert("sec-b".into(), std::time::Instant::now());
    sec2.cluster_state.apply(ClusterMutation::PrimaryChanged {
        new: "primary-orig".into(),
        epoch: 1,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
    });
    sec2.record_primary_message();
    tokio::time::sleep(PAST_DEATH).await;
    sec2.run_election_tick(); // → Suspecting
    tokio::time::sleep(ONE_INTERVAL).await;
    // sec-b reports the primary as FRESH (small age) → does not agree.
    sec2.record_timeout_response("sec-b".into(), Some(0.0));
    sec2.run_election_tick();
    // peer_count = 1 → quorum = 2; agreeing = self(1) only (sec-b's fresh
    // age does not count). No quorum → stay Suspecting.
    assert!(
        matches!(sec2.op_mut().election, ElectionState::Suspecting { .. }),
        "a peer reporting a FRESH primary age must NOT count toward the \
         death quorum — relative-age keying, never a wall-clock subtraction",
    );
}

// ── Single-survivor convergence (de51731b "2-node can't fail over to 1"
//    family, extended to N=1) ───────────────────────────────────────────
//
// Topology these tests pin: a fleet meshed (so `mesh.degraded` stayed
// FALSE — the watchdog saw ≥1 peer and took the partial/degraded-but-capable
// branch), then ALL peer secondaries departed and the primary died. The
// lone survivor's `live_peer_ids` is now empty (`peer_count == 0`), so
// `failover_quorum(0) == 1` is met by its own single self-confirm. It MUST
// commit the promotion and converge — NOT flap Suspecting/Candidate↔Normal
// forever (the consumer-confirmed bug: every self-promotion was reverted by
// a non-primary mesh frame mis-read as "the primary is back").

/// A non-primary inbound mesh frame (a peer secondary's anti-entropy
/// `StateDigest`, the submitter's snapshot/run-config request, a relayed
/// `ClusterMutation`) routed through `dispatch_message` must NOT revert an
/// in-flight election. Pre-fix the dispatch path called the un-gated
/// `record_primary_message` for EVERY frame, so any such frame from the
/// still-connected submitter cancelled the survivor's election — the
/// flap. THE REVERT-CHECK: drive Suspecting, then feed a `StateDigest` from
/// a NON-primary sender; the election must STAY Suspecting.
#[tokio::test(flavor = "current_thread")]
async fn non_primary_frame_does_not_revert_election() {
    use dynrunner_protocol_primary_secondary::ClusterMutation;
    let mut sec = make_secondary(election_config("sec-a"));
    sec.enter_operational_for_test();
    // The mesh formed (NOT degraded) — this survivor was failover-capable.
    sec.mesh.degraded = false;
    sec.cluster_state.apply(ClusterMutation::PrimaryChanged {
        new: "primary-orig".into(),
        epoch: 1,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
    });
    sec.record_primary_message();

    // Primary suspected dead → Suspecting.
    sec.op_mut().primary_last_seen =
        Some(std::time::Instant::now() - std::time::Duration::from_secs(60));
    sec.run_election_tick();
    assert!(
        matches!(sec.op_mut().election, ElectionState::Suspecting { .. }),
        "a suspected-dead primary must enter Suspecting",
    );

    // A NON-primary frame (anti-entropy digest from a peer, sender != the
    // current primary "primary-orig") routed through dispatch_message MUST
    // NOT reset the election: it is not a primary-liveness signal.
    let digest = DistributedMessage::StateDigest {
        target: None,
        sender_id: "peer-observer".into(),
        timestamp: timestamp_now(),
        digest: sec.cluster_state.digest(),
    };
    sec.dispatch_message(digest, &mut FakeWorkerFactory)
        .await
        .expect("StateDigest handler succeeds");
    assert!(
        matches!(sec.op_mut().election, ElectionState::Suspecting { .. }),
        "a non-primary mesh frame must NOT revert the election (pre-fix it \
         reverted to Normal — the single-survivor flap)",
    );
}

/// END-TO-END convergence: a lone survivor (mesh formed, then peers gone,
/// primary dead) reaches quorum with its OWN single confirm and COMMITS the
/// promotion in-tick — `run_election_tick` returns `promoted == true`, the
/// caller's `fire_local_promotion` advances `current_primary` onto self and
/// rebroadcasts `PrimaryChanged { new = self }`. The election does NOT flap:
/// a subsequent non-primary frame can no longer revert it (it is now the
/// primary), and the node stays converged.
#[tokio::test(flavor = "current_thread")]
async fn single_survivor_election_converges_and_commits() {
    use dynrunner_protocol_primary_secondary::{ClusterMutation, PrimaryChangeReason};

    // Recording harness so the `PrimaryChanged { new = self }` re-point is
    // observable after `drain_egress`; `promotion_rx` is the C4 signal.
    let (mut sec, log) = make_secondary_recording(election_config("sec-a"), 0);
    sec.enter_operational_for_test();
    // Mesh formed (NOT degraded), no live peer secondaries
    // (`peer_keepalives` empty → live_peer_ids empty → peer_count 0 →
    // quorum failover_quorum(0) == 1).
    sec.mesh.degraded = false;
    sec.cluster_state.apply(ClusterMutation::PrimaryChanged {
        new: "primary-orig".into(),
        epoch: 1,
        reason: PrimaryChangeReason::Election,
    });
    sec.record_primary_message();
    assert_eq!(
        failover_quorum(0),
        1,
        "a lone survivor's quorum is 1 — met by its own single self-confirm",
    );

    // Primary suspected dead → Suspecting.
    sec.op_mut().primary_last_seen =
        Some(std::time::Instant::now() - std::time::Duration::from_secs(60));
    sec.run_election_tick();
    assert!(matches!(
        sec.op_mut().election,
        ElectionState::Suspecting { .. }
    ));

    // Wait the gather window, then tick again: with zero live peers the
    // self-quorum is ALREADY met, so the tick commits the promotion in-tick
    // (no Candidate wait for a peer confirm that can never arrive).
    tokio::time::sleep(ONE_INTERVAL).await;
    let actions = sec.run_election_tick();
    assert!(
        actions.promoted,
        "a single-survivor self-quorum must COMMIT the promotion in-tick \
         (the caller drives fire_local_promotion), not sit in Candidate \
         awaiting a peer confirm that never comes",
    );
    assert!(
        matches!(sec.op_mut().election, ElectionState::Promoted),
        "the committing tick leaves the election Promoted for the terminal action",
    );

    // Drive the terminal action the loop drives off `actions.promoted`.
    sec.fire_local_promotion().await;
    sec.drain_egress().await;

    // The C4 promotion signal fired, identity advanced onto self, the
    // re-point was rebroadcast, and the election reset to Normal (a primary
    // now exists — convergence, no lingering Promoted/Candidate flap).
    assert!(
        sec.promotion_rx.try_recv().is_ok(),
        "the single-survivor win fires the C4 PromotionSignal",
    );
    assert_eq!(
        sec.cluster_state.current_primary(),
        Some("sec-a"),
        "the survivor becomes the primary",
    );
    assert!(matches!(sec.op_mut().election, ElectionState::Normal));
    let rebroadcast = log.borrow().iter().any(|m| {
        matches!(
            m,
            DistributedMessage::ClusterMutation { mutations, .. }
                if mutations.iter().any(|mu| matches!(
                    mu,
                    ClusterMutation::PrimaryChanged { new, .. } if new == "sec-a"
                ))
        )
    });
    assert!(
        rebroadcast,
        "the single-survivor win rebroadcasts PrimaryChanged(new=self)",
    );

    // CONVERGED: once this node IS the primary, its same-peer primary's own
    // `Primary`-role keepalives keep `primary_last_seen` fresh (recognized:
    // sender == current_primary == self), so the election stays Normal — no
    // self-re-election flap. (In production the `Node` builds the
    // `PrimaryCoordinator` off the C4 signal and that primary emits the
    // keepalives; here we feed one directly, mirroring
    // `self_named_primary_resets_election_to_normal`.)
    for _ in 0..3 {
        sec.handle_inbound(
            keepalive_from("sec-a", KeepaliveRole::Primary),
            &mut FakeWorkerFactory,
        )
        .await;
        let after = sec.run_election_tick();
        assert!(!after.promoted);
        assert!(
            matches!(sec.op_mut().election, ElectionState::Normal),
            "a converged self-primary fed its own keepalives stays Normal — no flap",
        );
    }
}

// ── Election ping-pong (asm-dataset 2212c136: deposed ex-primary
//    self-assertion over an asymmetric dead leg) ─────────────────────────
//
// Production replay: 3 compute peers; the secondary-2↔secondary-0 mesh leg
// blackholed (secondary-2 ABSENT from secondary-0's transport membership,
// its QUIC connection torn down) and never healed. Epoch 1: secondary-0 is
// primary; epoch 2: the fleet elects secondary-1. From the DEPOSED
// secondary-0's view the live-peer set was then EMPTY — secondary-1
// excluded as the current primary, secondary-2 invisible over the dead leg
// — so `failover_quorum(0) == 1` let secondary-0 SELF-PROMOTE with ZERO
// peer agreement every time an arming leg tripped: the metronomic ~25s
// primary ping-pong (epochs 3/5/7/9 were secondary-0's self-assertions,
// 0-13s before any peer saw them) that ended in "cluster routing
// collapsed". These tests pin the fix: a peer that the MESH MEMBERSHIP
// still lists is not GONE — it counts in the failover-quorum DENOMINATOR
// even while role-excluded from the candidate set — and a deposed
// primary's re-candidacy needs positive peer agreement.

/// THE PING-PONG REPLAY (RED→GREEN): the deposed ex-primary, with the new
/// primary STILL a connected mesh member (its multi-role `Secondary`
/// keepalives arriving) and the third peer unreachable over the dead leg,
/// trips an arming leg repeatedly. It must NEVER self-promote — the
/// member-listed current primary is evidence the fleet is NOT gone, so the
/// quorum denominator stays at 2 (self + the member primary), unreachable
/// without real peer agreement. Pre-fix every cycle committed an in-tick
/// lone-survivor promotion (`actions.promoted == true`) — the flip-back.
/// GREEN: the node converges back to Normal on the next genuine primary
/// frame and `current_primary` HOLDS at secondary-1 across all cycles.
#[tokio::test(flavor = "current_thread")]
async fn deposed_ex_primary_never_self_promotes_while_new_primary_is_a_member() {
    use dynrunner_protocol_primary_secondary::ClusterMutation;
    // secondary-0's view: ONLY secondary-1 is a transport member (the
    // secondary-2 leg is blackholed; its QUIC connection tore down).
    let (mut sec, _members) = make_secondary_membership(
        election_config("secondary-0"),
        vec![PeerId::from("secondary-1")],
    );
    sec.enter_operational_for_test();
    sec.mesh.degraded = false;
    // Epoch 1: THIS node held the primary role. Epoch 2: the fleet elected
    // secondary-1 — this node is DEPOSED. Both applied through the real
    // dispatch hook so the deposition is observed where production observes it.
    for (new, epoch) in [("secondary-0", 1), ("secondary-1", 2)] {
        let promote = DistributedMessage::ClusterMutation {
            target: None,
            sender_id: "setup".into(),
            timestamp: 0.0,
            mutations: vec![ClusterMutation::PrimaryChanged {
                new: new.into(),
                epoch,
                reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
            }],
        };
        sec.dispatch_message(promote, &mut FakeWorkerFactory)
            .await
            .expect("PrimaryChanged handler succeeds");
    }
    assert_eq!(sec.cluster_state.current_primary(), Some("secondary-1"));
    sec.record_primary_message();

    // ≥2 metronome cycles (production flipped at epochs 3/5/7/9).
    for cycle in 0..3 {
        // secondary-1's multi-role SECONDARY keepalive keeps arriving over
        // the healthy leg (the real recognition path files it in
        // `peer_keepalives`; it is NOT a primary-liveness signal).
        sec.handle_inbound(
            keepalive_from("secondary-1", KeepaliveRole::Secondary),
            &mut FakeWorkerFactory,
        )
        .await;

        // The arming trip: the silent window on the PRIMARY-liveness axis
        // that armed secondary-0 in production (leg (B) backstop here —
        // deterministic stand-in for whichever leg tripped on-cluster).
        sec.op_mut().primary_last_seen =
            Some(std::time::Instant::now() - std::time::Duration::from_secs(60));
        let armed = sec.run_election_tick();
        assert!(
            !armed.promoted,
            "cycle {cycle}: the arming tick must never commit a promotion",
        );

        // Gather window elapses; the tally tick. Pre-fix: live-peer set
        // empty (secondary-1 minus'd as current primary, secondary-2 not a
        // member) → quorum 1 → in-tick self-promotion with ZERO peer
        // agreement — the flip-back.
        tokio::time::sleep(ONE_INTERVAL).await;
        let tally = sec.run_election_tick();
        assert!(
            !tally.promoted,
            "cycle {cycle}: FLIP-BACK — the deposed ex-primary self-promoted \
             with zero peer agreement while the new primary was still a \
             connected mesh member",
        );
        assert!(
            !matches!(sec.op_mut().election, ElectionState::Promoted),
            "cycle {cycle}: the election must never reach Promoted solo",
        );
        assert!(
            !tally
                .broadcast
                .iter()
                .any(|m| matches!(m, DistributedMessage::PromotionVote { target: _, .. })),
            "cycle {cycle}: no self-candidacy broadcast without quorum",
        );
        assert_eq!(
            sec.cluster_state.current_primary(),
            Some("secondary-1"),
            "cycle {cycle}: the recognized primary HOLDS at secondary-1",
        );

        // The next genuine Primary-role frame from secondary-1 cancels the
        // (still-unresolved) suspicion — the node converges back to Normal.
        sec.handle_inbound(
            keepalive_from("secondary-1", KeepaliveRole::Primary),
            &mut FakeWorkerFactory,
        )
        .await;
        assert!(
            matches!(sec.op_mut().election, ElectionState::Normal),
            "cycle {cycle}: a genuine primary frame converges the node to Normal",
        );
    }
}

/// Leg 3 — deposed-primary re-assertion needs POSITIVE peer agreement: when
/// the new primary GENUINELY departs the mesh too (the self-promotion edge
/// that survives the denominator fix), a DEPOSED ex-primary must NOT take
/// the lone-survivor in-tick fast path. It becomes a Candidate (broadcasts
/// its `PromotionVote`) and promotes only on a real peer confirm. Pre-fix
/// the tally tick committed the promotion solo (`actions.promoted == true`).
#[tokio::test(flavor = "current_thread")]
async fn deposed_lone_survivor_requires_positive_peer_agreement() {
    use dynrunner_protocol_primary_secondary::ClusterMutation;
    let (mut sec, members) = make_secondary_membership(
        election_config("secondary-0"),
        vec![PeerId::from("secondary-1")],
    );
    sec.enter_operational_for_test();
    sec.mesh.degraded = false;
    for (new, epoch) in [("secondary-0", 1), ("secondary-1", 2)] {
        let promote = DistributedMessage::ClusterMutation {
            target: None,
            sender_id: "setup".into(),
            timestamp: 0.0,
            mutations: vec![ClusterMutation::PrimaryChanged {
                new: new.into(),
                epoch,
                reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
            }],
        };
        sec.dispatch_message(promote, &mut FakeWorkerFactory)
            .await
            .expect("PrimaryChanged handler succeeds");
    }
    sec.record_primary_message();

    // The new primary secondary-1 now GENUINELY dies: its QUIC connection
    // tears down and the membership view republishes without it. Its
    // (corpse) `peer_keepalives` entry may linger — membership is the
    // death evidence.
    sec.op_mut()
        .peer_keepalives
        .insert("secondary-1".into(), std::time::Instant::now());
    members.borrow_mut().clear();
    sec.publish_membership();

    // Arm + gather + tally.
    sec.op_mut().primary_last_seen =
        Some(std::time::Instant::now() - std::time::Duration::from_secs(60));
    sec.run_election_tick();
    assert!(matches!(
        sec.op_mut().election,
        ElectionState::Suspecting { .. }
    ));
    tokio::time::sleep(ONE_INTERVAL).await;
    let tally = sec.run_election_tick();

    // The deposed ex-primary must NOT commit solo, even as a genuine lone
    // survivor: its own deposition is evidence the fleet elected around it.
    assert!(
        !tally.promoted,
        "a DEPOSED ex-primary must not take the lone-survivor in-tick \
         self-quorum — positive peer agreement is required",
    );
    assert!(
        matches!(sec.op_mut().election, ElectionState::Candidate { .. }),
        "the deposed lone survivor campaigns as a Candidate instead; got {:?}",
        std::mem::discriminant(&sec.op_mut().election),
    );
    assert!(
        tally
            .broadcast
            .iter()
            .any(|m| matches!(m, DistributedMessage::PromotionVote { target: _, candidate_id, .. } if candidate_id == "secondary-0")),
        "the candidacy is broadcast so a surviving/recovered peer can confirm",
    );

    // POSITIVE peer agreement arrives (secondary-1 recovered, or another
    // survivor heard the campaign): one real confirm promotes.
    let promoted = sec.record_promotion_confirm("secondary-1".into(), "secondary-0".into(), 1);
    assert!(
        promoted,
        "one real peer confirm IS positive agreement — the deposed candidate promotes",
    );
    assert!(matches!(sec.op_mut().election, ElectionState::Promoted));
}

/// The quorum-DENOMINATOR rule (`failover_quorum_peer_count`), pinned
/// directly: a member-listed current primary COUNTS (role-excluded from the
/// candidate set, but not gone), and drops out the moment it departs the
/// transport membership — so a genuinely-dead primary still shrinks the
/// denominator within one mesh-pump cycle (#317/#332 convergence intact).
#[tokio::test(flavor = "current_thread")]
async fn member_listed_current_primary_counts_in_quorum_denominator() {
    use dynrunner_protocol_primary_secondary::ClusterMutation;
    let (mut sec, members) = make_secondary_membership(
        election_config("sec-b"),
        vec![PeerId::from("sec-a"), PeerId::from("sec-c")],
    );
    sec.enter_operational_for_test();
    // Both peers keepalive-tracked; sec-a is the current primary (a
    // multi-role host whose Secondary keepalive files the entry).
    sec.op_mut()
        .peer_keepalives
        .insert("sec-a".into(), std::time::Instant::now());
    sec.op_mut()
        .peer_keepalives
        .insert("sec-c".into(), std::time::Instant::now());
    sec.cluster_state.apply(ClusterMutation::PrimaryChanged {
        new: "sec-a".into(),
        epoch: 1,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
    });

    // Candidate/voter eligibility NEVER contains the current primary.
    assert!(
        !sec.live_peer_ids().any(|id| id == "sec-a"),
        "the current primary is role-excluded from the candidate set",
    );
    // …but the member-listed primary COUNTS in the quorum denominator.
    assert!(sec.current_primary_in_quorum());
    assert_eq!(
        sec.failover_quorum_peer_count(),
        2,
        "denominator = live peer (sec-c) + member-listed primary (sec-a)",
    );

    // The primary GENUINELY dies: QUIC teardown → membership departure.
    members
        .borrow_mut()
        .retain(|id| id != &PeerId::from("sec-a"));
    sec.publish_membership();
    assert!(!sec.current_primary_in_quorum());
    assert_eq!(
        sec.failover_quorum_peer_count(),
        1,
        "a membership-departed primary no longer inflates the denominator \
         (lone-/below-majority-survivor convergence preserved)",
    );
}

/// First-hand reachability veto (named decision): a voter never confirms a
/// candidate absent from its OWN live transport membership — even a
/// lex-lowest one. Structurally `live_peer_ids ⊆ membership` already makes
/// a non-member unable to be the computed lowest; this pins the veto as an
/// explicit, logged contract independent of the lowest-id arithmetic.
#[tokio::test(flavor = "current_thread")]
async fn promotion_vote_for_unreachable_candidate_is_vetoed() {
    use dynrunner_protocol_primary_secondary::ClusterMutation;
    // Voter sec-z: its membership holds only sec-m. The dead primary has
    // departed; primary-silence is genuine (backdated below).
    let (mut sec, _members) =
        make_secondary_membership(election_config("sec-z"), vec![PeerId::from("sec-m")]);
    sec.enter_operational_for_test();
    sec.op_mut()
        .peer_keepalives
        .insert("sec-m".into(), std::time::Instant::now());
    sec.cluster_state.apply(ClusterMutation::PrimaryChanged {
        new: "primary-orig".into(),
        epoch: 1,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
    });
    sec.record_primary_message();
    sec.op_mut().primary_last_seen =
        Some(std::time::Instant::now() - std::time::Duration::from_secs(60));

    // "sec-a" is lex-lowest — but this voter has NO ROUTE to it (it is not
    // in the membership view). The vote must be vetoed, not confirmed.
    assert!(
        sec.record_promotion_vote("sec-a".into(), 1).is_none(),
        "a no-route candidate must never receive this voter's confirm",
    );
    assert!(
        !matches!(sec.op_mut().election, ElectionState::Voting { .. }),
        "a vetoed vote must not move the voter into Voting for the \
         unreachable candidate",
    );

    // Control: the same vote for the REACHABLE member peer is confirmed
    // (sec-m is the lowest reachable candidate).
    let reply = sec.record_promotion_vote("sec-m".into(), 1);
    assert!(
        matches!(
            reply,
            Some(DistributedMessage::PromotionConfirm { ref new_primary_id, .. })
                if new_primary_id == "sec-m"
        ),
        "a reachable lowest candidate is confirmed as before; got {reply:?}",
    );
}

/// First-hand liveness on a self-query: a node asked (via `TimeoutQuery`)
/// about ITSELF must answer with positive first-hand evidence (age 0), not
/// `None` ("no evidence" — which the querier's tally counts as AGREEMENT
/// that the node is dead). Pre-fix the responder read its own id out of
/// `peer_keepalives` (structurally absent — a node does not receive its own
/// broadcasts), reported `None`, and thereby AGREED TO ITS OWN DEATH —
/// letting a suspecting peer count the very node it suspects toward the
/// death quorum.
#[tokio::test(flavor = "current_thread")]
async fn timeout_query_about_self_reports_first_hand_liveness() {
    let mut sec = make_secondary(election_config("sec-a"));
    sec.enter_operational_for_test();

    assert_eq!(
        sec.queried_node_liveness_age("sec-a"),
        Some(0.0),
        "a self-query is answered with first-hand liveness (age 0), never None",
    );

    // Through the real inbound arm: the queued TimeoutResponse carries the
    // same first-hand answer onto the wire.
    sec.handle_inbound(
        DistributedMessage::TimeoutQuery {
            target: None,
            sender_id: "sec-z".into(),
            timestamp: timestamp_now(),
            query_node_id: "sec-a".into(),
        },
        &mut FakeWorkerFactory,
    )
    .await;
    let (_, response) = sec
        .op_mut()
        .pending_peer_messages
        .pop()
        .expect("the TimeoutQuery arm queues a TimeoutResponse");
    assert!(
        matches!(
            response,
            DistributedMessage::TimeoutResponse {
                last_keepalive: Some(age),
                ..
            } if age == 0.0
        ),
        "the wire response reports the first-hand self-liveness; got {response:?}",
    );
}

/// The REAL recovery path is NOT broken: a genuine `Primary`-role keepalive
/// from the EXPECTED current primary, fed through the real `handle_inbound`
/// recognition path, STILL aborts an in-flight election (reverts to Normal).
/// The fix narrows the revert to genuine primary-liveness signals — it must
/// not over-narrow and break the legitimate "primary came back" abort.
#[tokio::test(flavor = "current_thread")]
async fn real_primary_recovery_still_aborts_election() {
    use dynrunner_protocol_primary_secondary::{ClusterMutation, KeepaliveRole};
    let mut sec = make_secondary(election_config("sec-a"));
    sec.enter_operational_for_test();
    sec.mesh.degraded = false;
    sec.cluster_state.apply(ClusterMutation::PrimaryChanged {
        new: "primary-orig".into(),
        epoch: 1,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
    });
    sec.record_primary_message();

    // Suspected dead → Suspecting.
    sec.op_mut().primary_last_seen =
        Some(std::time::Instant::now() - std::time::Duration::from_secs(60));
    sec.run_election_tick();
    assert!(matches!(
        sec.op_mut().election,
        ElectionState::Suspecting { .. }
    ));

    // The GENUINE primary "primary-orig" is reachable again and sends a
    // Primary-role keepalive. handle_inbound recognizes it
    // (sender == current_primary) and aborts the election.
    sec.handle_inbound(
        keepalive_from("primary-orig", KeepaliveRole::Primary),
        &mut FakeWorkerFactory,
    )
    .await;
    assert!(
        matches!(sec.op_mut().election, ElectionState::Normal),
        "a genuine Primary keepalive from the current primary MUST still \
         abort the election (the real-recovery path is preserved)",
    );
}
