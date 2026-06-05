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
    FakeWorkerFactory, arm_primary_activator, election_config, make_secondary,
};
use super::super::wire::timestamp_now;
use super::*;
use dynrunner_protocol_primary_secondary::KeepaliveRole;
use std::time::Duration;

/// The death deadline given the helper's keepalive_interval (50ms) and
/// keepalive_miss_threshold (2). 100ms exact; sleep slightly over.
const PAST_DEATH: Duration = Duration::from_millis(110);
/// One full keepalive interval, the gather window for `Suspecting` to
/// progress to a vote.
const ONE_INTERVAL: Duration = Duration::from_millis(60);

/// Scenario (b): primary stops sending keepalives. The lowest-id
/// secondary observes the death, runs the election, collects quorum,
/// and promotes itself.
#[tokio::test(flavor = "current_thread")]
async fn primary_dies_lowest_id_promotes() {
    use dynrunner_protocol_primary_secondary::ClusterMutation;
    let mut sec = make_secondary(election_config("sec-a"));
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
            .any(|m| matches!(m, DistributedMessage::TimeoutQuery { .. }))
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
            .any(|m| matches!(m, DistributedMessage::PromotionVote { .. }))
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
    let mut sec = make_secondary(election_config("sec-a"));
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
        sender_id: "primary".into(),
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
/// there is no lingering `Promoted`. Post-reset, the co-located
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
        sender_id: "primary".into(),
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

    // The co-located primary's own keepalives (recognized: `from` ==
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
            "no spurious election broadcast while the co-located primary is healthy",
        );
    }
    assert_eq!(sec.cluster_state.current_primary(), Some("sec-a"));
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
        sender_id: "primary".into(),
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
        sender_id: "primary".into(),
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
        sender_id: "primary".into(),
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
    let mut sec_a = make_secondary(election_config("sec-a"));
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

    let mut sec_b = make_secondary(election_config("sec-b"));
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

// ── Failover terminal action: record_promotion_confirm → activation ──

use super::super::test_helpers::make_secondary_recording;

/// The election-win terminal action: once `record_promotion_confirm`
/// returns `true` (quorum crossed, state → `Promoted`), the message
/// handler's terminal action `fire_local_promotion` originates a single
/// `ClusterMutation::PrimaryChanged { new = self }`, applies it locally
/// through the unified hook — which MUST (1) build the co-located
/// primary on demand and reset the election to `Normal` — and
/// (2) broadcast that SAME `PrimaryChanged` so surviving secondaries
/// re-point `Role::Primary` onto this winner. Pre-fix the `true` was
/// discarded and the failover path dead-ended.
#[tokio::test(flavor = "current_thread")]
async fn promotion_confirm_true_fires_activation_and_rebroadcasts() {
    use dynrunner_protocol_primary_secondary::ClusterMutation;
    // peer_count = 2 → quorum = 2 (self + one confirm).
    let (mut sec, log) = make_secondary_recording(election_config("sec-a"), 2);
    sec.enter_operational_for_test();
    let activated = arm_primary_activator(&mut sec);

    // Drive sec-a into Candidate(round=1) so the confirm tally has a
    // matching round to credit.
    sec.op_mut()
        .peer_keepalives
        .insert("sec-b".into(), std::time::Instant::now());
    sec.op_mut()
        .peer_keepalives
        .insert("sec-c".into(), std::time::Instant::now());
    sec.record_primary_message();
    tokio::time::sleep(PAST_DEATH).await;
    sec.run_election_tick(); // → Suspecting
    tokio::time::sleep(ONE_INTERVAL).await;
    sec.record_timeout_response("sec-b".into(), None);
    sec.record_timeout_response("sec-c".into(), None);
    sec.run_election_tick(); // → Candidate { round: 1 }
    assert!(matches!(
        sec.op_mut().election,
        ElectionState::Candidate { .. }
    ));

    // One peer confirms — self + sec-b = quorum (2). The terminal
    // action mirrors the message-handler arm: on `true`, fire.
    let promoted = sec.record_promotion_confirm("sec-b".into(), "sec-a".into(), 1);
    assert!(promoted, "quorum confirm must promote");
    assert!(matches!(sec.op_mut().election, ElectionState::Promoted));
    sec.fire_local_promotion().await;

    // (1) The local self-apply of `PrimaryChanged { new = self }` BUILT
    // the co-located primary on demand (the activator closure ran),
    // installed self as the current primary, AND reset the election to
    // Normal (no lingering Promoted — the host now runs a primary
    // coordinator).
    assert!(
        activated.get(),
        "fire_local_promotion must build the co-located primary on demand \
             (the activator closure must run)",
    );
    assert_eq!(
        sec.cluster_state.current_primary(),
        Some("sec-a"),
        "the local self-apply installs self as current primary"
    );
    assert!(
        matches!(sec.op_mut().election, ElectionState::Normal),
        "the winner's own apply hook resets the election to Normal"
    );

    // (2) The SAME `PrimaryChanged { new = self }` landed on the mesh,
    // carried in a `ClusterMutation` envelope, with a strictly-
    // superseding epoch.
    let change = log.borrow().iter().find_map(|m| match m {
        DistributedMessage::ClusterMutation { mutations, .. } => {
            mutations.iter().find_map(|mu| match mu {
                ClusterMutation::PrimaryChanged { new, epoch, .. } => Some((new.clone(), *epoch)),
                _ => None,
            })
        }
        _ => None,
    });
    let (new_primary, epoch) = change.expect("a PrimaryChanged must be broadcast on promotion");
    assert_eq!(new_primary, "sec-a", "must name self as new primary");
    assert!(
        epoch >= 1,
        "epoch must supersede the prior identity (epoch+1)"
    );
}

/// The on-demand build is FIRE-ONCE across the paths that name this
/// node primary: winning the own election AND being named by a
/// `PrimaryChanged` applied through the hook. A second
/// `activate_co_located_primary_on_demand` (or a second apply of the same
/// frame) must NOT panic or double-build — the activator is `take()`-n on
/// the first build and the stored handle short-circuits the rest.
#[tokio::test(flavor = "current_thread")]
async fn activation_is_fire_once() {
    let (mut sec, _log) = make_secondary_recording(election_config("sec-a"), 1);
    let build_count = std::rc::Rc::new(std::cell::Cell::new(0u32));
    // Mark the node capable + register a counting activator (the standard
    // arming probe records a bool; here we need a count to prove fire-once).
    sec.cluster_state.apply(
        dynrunner_protocol_primary_secondary::ClusterMutation::PeerJoined {
            peer_id: "sec-a".into(),
            is_observer: false,
            can_be_primary: true,
        },
    );
    {
        let bc = build_count.clone();
        sec.register_primary_activator(Box::new(move |_snap| {
            bc.set(bc.get() + 1);
            tokio::spawn(async {})
        }));
    }

    // First activation builds (the activator runs, the handle is stored).
    sec.activate_co_located_primary_on_demand();
    assert_eq!(build_count.get(), 1, "first activation must build once");

    // Second activation (e.g. the own broadcast echoed back through the
    // `apply_cluster_mutations` hook) is a clean no-op — the handle is
    // already set, so the activator is never invoked again.
    sec.activate_co_located_primary_on_demand();
    sec.fire_local_promotion().await;
    assert_eq!(
        build_count.get(),
        1,
        "no second build; the activator was consumed on the first activation",
    );
}

// ── The unified wake frame: `apply_cluster_mutations` apply hook ──

/// The highest-value wake-frame unit test. A primary-capable secondary
/// with a registered on-demand activator, applying
/// `ClusterMutation::PrimaryChanged { reason: Election, new = self }`
/// through the unified `apply_cluster_mutations` hook:
///   (a) BUILDS the co-located primary on demand (the activator runs) AND
///       resets the failover election to `Normal` afterwards;
///   (b) NEVER went through Suspecting/Candidate first — the hook keys on
///       identity, not election history.
#[tokio::test(flavor = "current_thread")]
async fn wake_frame_self_named_builds_on_demand_and_resets_to_normal() {
    use dynrunner_protocol_primary_secondary::ClusterMutation;
    let (mut sec, _log) = make_secondary_recording(election_config("sec-a"), 1);
    sec.enter_operational_for_test();
    let activated = arm_primary_activator(&mut sec);

    // No prior election excursion — the node has never suspected/voted.
    assert!(matches!(sec.op_mut().election, ElectionState::Normal));

    let changed = sec.apply_cluster_mutations(vec![ClusterMutation::PrimaryChanged {
        new: "sec-a".into(),
        epoch: 1,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
    }]);

    assert!(changed, "a genuine PrimaryChanged advance returns true");
    assert!(
        activated.get(),
        "self-named PrimaryChanged (Election) must build the co-located \
         primary on demand",
    );
    assert_eq!(sec.cluster_state.current_primary(), Some("sec-a"));
    assert!(
        matches!(sec.op_mut().election, ElectionState::Normal),
        "the election resets to Normal after self-activation (no lingering Promoted)",
    );
}

/// `PrimaryChanged { new = other }` applied through the hook installs
/// the peer as primary but does NOT build this node's co-located primary —
/// the on-demand build runs ONLY when the frame names self.
#[tokio::test(flavor = "current_thread")]
async fn wake_frame_peer_named_does_not_build() {
    use dynrunner_protocol_primary_secondary::ClusterMutation;
    let (mut sec, _log) = make_secondary_recording(election_config("sec-a"), 1);
    sec.enter_operational_for_test();
    let activated = arm_primary_activator(&mut sec);

    let changed = sec.apply_cluster_mutations(vec![ClusterMutation::PrimaryChanged {
        new: "sec-b".into(),
        epoch: 1,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
    }]);

    assert!(
        changed,
        "a peer-named PrimaryChanged still advances the identity"
    );
    assert_eq!(sec.cluster_state.current_primary(), Some("sec-b"));
    assert!(
        !activated.get(),
        "the on-demand build must NOT run when a PEER is named primary",
    );
}

/// Applying `PrimaryChanged { new = self }` TWICE is fire-once: the
/// second apply builds nothing (the activator was already taken on the
/// first) and does not panic. The second is also a stale-epoch NoOp at
/// the CRDT level (same epoch+id), so it reports `false`.
#[tokio::test(flavor = "current_thread")]
async fn wake_frame_double_apply_is_fire_once() {
    use dynrunner_protocol_primary_secondary::ClusterMutation;
    let (mut sec, _log) = make_secondary_recording(election_config("sec-a"), 1);
    sec.enter_operational_for_test();
    let build_count = std::rc::Rc::new(std::cell::Cell::new(0u32));
    sec.cluster_state.apply(ClusterMutation::PeerJoined {
        peer_id: "sec-a".into(),
        is_observer: false,
        can_be_primary: true,
    });
    {
        let bc = build_count.clone();
        sec.register_primary_activator(Box::new(move |_snap| {
            bc.set(bc.get() + 1);
            tokio::spawn(async {})
        }));
    }

    let first = sec.apply_cluster_mutations(vec![ClusterMutation::PrimaryChanged {
        new: "sec-a".into(),
        epoch: 1,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
    }]);
    assert!(first, "first apply advances the identity");
    assert_eq!(build_count.get(), 1, "first apply builds the primary once");

    // Second apply of the identical frame: fire-once (activator already
    // taken on the first) + CRDT NoOp (same epoch+id). No panic, no second
    // build.
    let second = sec.apply_cluster_mutations(vec![ClusterMutation::PrimaryChanged {
        new: "sec-a".into(),
        epoch: 1,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
    }]);
    assert!(!second, "a re-applied identical PrimaryChanged is a NoOp");
    assert_eq!(
        build_count.get(),
        1,
        "no second build; the activator was consumed on the first apply",
    );
}

/// With no on-demand activator wired AND no `can_be_primary` marker
/// (Rust-only / legacy / no-mesh callers), the terminal action's
/// build-on-demand is a BENIGN no-op (the `(false, None)` fork — never a
/// fatal): the `PrimaryChanged` broadcast still fires so the mesh records
/// the new authority.
#[tokio::test(flavor = "current_thread")]
async fn promotion_without_activator_still_broadcasts() {
    use dynrunner_protocol_primary_secondary::ClusterMutation;
    let (mut sec, log) = make_secondary_recording(election_config("sec-a"), 1);
    // No `register_primary_activator` and no `can_be_primary` marker —
    // the `(false, None)` benign no-op fork.
    sec.fire_local_promotion().await;
    assert!(
        log.borrow().iter().any(|m| matches!(
            m,
            DistributedMessage::ClusterMutation { mutations, .. }
                if mutations
                    .iter()
                    .any(|mu| matches!(mu, ClusterMutation::PrimaryChanged { .. }))
        )),
        "PrimaryChanged must broadcast even when no co-located primary exists",
    );
}

/// Loud-reject fork `(can_be_primary = true, activator = None)`: the node
/// is marked primary-capable in the replicated CRDT but the runtime
/// registered NO on-demand activator — a programmer wiring error. The
/// activation site MUST latch `fatal_exit` (loud, never a silent strand —
/// the failure mode that produced THE HANG) and build NO primary.
#[tokio::test(flavor = "current_thread")]
async fn on_demand_capable_without_activator_latches_fatal_exit() {
    use dynrunner_protocol_primary_secondary::ClusterMutation;
    let (mut sec, _log) = make_secondary_recording(election_config("sec-a"), 1);
    sec.enter_operational_for_test();
    // Mark capable through the real CRDT apply path — but register NO
    // activator (NOT `arm_primary_activator`, which also wires one).
    sec.cluster_state.apply(ClusterMutation::PeerJoined {
        peer_id: "sec-a".into(),
        is_observer: false,
        can_be_primary: true,
    });
    assert!(sec.fatal_exit.is_none(), "no fatal_exit before activation");

    sec.activate_co_located_primary_on_demand();

    assert!(
        sec.fatal_exit.is_some(),
        "(true, None) is a wiring-error violation — it MUST latch fatal_exit, \
         never silently strand with no primary",
    );
    assert!(
        sec.activated_primary_handle.is_none(),
        "no primary may be built on the (true, None) reject arm",
    );
}

/// Loud-reject fork `(can_be_primary = false, activator = Some)`: an
/// activator was wired but the node's replicated capability marker is
/// unset — selection/election must never name a peer whose marker is
/// cleared. The activation site MUST latch `fatal_exit` (refuse to build
/// an unmarked authority) and build NO primary, never silently strand.
#[tokio::test(flavor = "current_thread")]
async fn on_demand_incapable_with_activator_latches_fatal_exit() {
    let (mut sec, _log) = make_secondary_recording(election_config("sec-a"), 1);
    sec.enter_operational_for_test();
    // Register an activator but DO NOT mark the node capable: the
    // `(false, Some)` fork. A build attempt here would be a split-brain.
    let built = std::rc::Rc::new(std::cell::Cell::new(false));
    {
        let b = built.clone();
        sec.register_primary_activator(Box::new(move |_snap| {
            b.set(true);
            tokio::spawn(async {})
        }));
    }
    assert!(sec.fatal_exit.is_none(), "no fatal_exit before activation");

    sec.activate_co_located_primary_on_demand();

    assert!(
        sec.fatal_exit.is_some(),
        "(false, Some) is a capability violation — it MUST latch fatal_exit, \
         refusing to build an unmarked authority rather than strand silently",
    );
    assert!(
        !built.get(),
        "the activator closure must NOT run on the (false, Some) reject arm",
    );
    assert!(
        sec.activated_primary_handle.is_none(),
        "no primary may be built on the (false, Some) reject arm",
    );
}

/// Transferred-during-setup race: a bootstrap `PrimaryChanged { reason:
/// Transferred, new = self }` that arrives while this node is STILL in
/// setup (NOT yet Operational — the relocate raced ahead of
/// TransferComplete) must NOT build inline. Instead it latches
/// `pending_transfer_activation`; the setup FSM (`enter_operational_on_
/// transfer`) then consumes the latch and builds the co-located primary on
/// demand — neither stranding nor panicking. Pins the full latch → setup
/// transition → on-demand build chain on a primary-capable node.
#[tokio::test(flavor = "current_thread")]
async fn transferred_during_setup_latches_then_builds_on_demand() {
    use dynrunner_protocol_primary_secondary::{ClusterMutation, PrimaryChangeReason};
    let (mut sec, _log) = make_secondary_recording(election_config("sec-a"), 1);
    // NOTE: NO `enter_operational_for_test()` — the node is still in setup
    // (the `Connecting` lifecycle), which is the whole point of this race.
    let activated = arm_primary_activator(&mut sec);
    assert!(
        sec.lifecycle.operational_mut().is_none(),
        "fixture precondition: the node is NOT yet Operational",
    );

    // The Transferred self-naming lands mid-setup. Because the node is not
    // Operational, `apply_primary_changed` must DEFER the build by latching
    // the pending flag — it must NOT build inline here.
    let changed = sec.apply_cluster_mutations(vec![ClusterMutation::PrimaryChanged {
        new: "sec-a".into(),
        epoch: 1,
        reason: PrimaryChangeReason::Transferred,
    }]);
    assert!(
        changed,
        "a genuine Transferred self-naming advances the identity"
    );
    assert!(
        sec.pending_transfer_activation,
        "Transferred-while-in-setup must latch pending_transfer_activation",
    );
    assert!(
        !activated.get(),
        "the on-demand build must be DEFERRED (not inline) while still in setup",
    );

    // The setup FSM picks the latch up in its recv loop and runs the
    // Transferred transition: it consumes the latch, builds the co-located
    // primary on demand, and returns `true` so `wait_for_setup` abandons
    // the handshake and runs this node as the follower of its own primary.
    let took = sec
        .enter_operational_on_transfer(&mut FakeWorkerFactory)
        .await
        .expect("the Transferred setup transition must not error");
    assert!(took, "the transition is taken (the latch was set)");
    assert!(
        !sec.pending_transfer_activation,
        "the latch is consumed so a re-applied frame does not re-enter",
    );
    assert!(
        activated.get(),
        "the setup transition must build the co-located primary on demand \
         (the activator closure ran)",
    );
    assert!(
        sec.activated_primary_handle.is_some(),
        "the built primary's join handle is stored for wind-down",
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
    let mut sec = make_secondary(election_config("sec-b"));
    sec.enter_operational_for_test();
    // A surviving mesh peer so the election path is non-degraded and
    // can actually broadcast `TimeoutQuery` when the primary dies.
    sec.op_mut()
        .peer_keepalives
        .insert("sec-c".into(), std::time::Instant::now());

    // A peer (sec-a) is promoted to primary via the real apply path.
    let promote = DistributedMessage::ClusterMutation {
        sender_id: "primary".into(),
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
                .any(|m| matches!(m, DistributedMessage::TimeoutQuery { .. })),
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
            .any(|m| matches!(m, DistributedMessage::TimeoutQuery { .. })),
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
        sender_id: "primary".into(),
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
    let mut sec = make_secondary(cfg);
    sec.enter_operational_for_test();

    // A live peer (sec-b) whose pre-suspend keepalive carries an ANCIENT
    // wire timestamp but is received RIGHT NOW. Drive it through the real
    // recognition path so the receipt `Instant` is stamped locally.
    let ancient_wire = timestamp_now() - 100_000.0;
    sec.handle_inbound(
        DistributedMessage::Keepalive {
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
    let mut sec = make_secondary(election_config("sec-a"));
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
    let mut sec2 = make_secondary(election_config("sec-a"));
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
