//! Deterministic-tick unit tests for [`super::ConsensusFsm`].
//!
//! Every test drives the FSM through hand-stepped `Instant`s — no
//! real-clock waits, no `tokio::time::pause()`. The 13 tests below
//! cover every transition the state diagram in [`super`] declares,
//! including the side-gate (Q1), retry-once-then-abort (Q5), and the
//! stale-round-frame defensive ignores.
//!
//! The wire-protocol crate's `DistributedMessage` is generic over a
//! task-id type `I`; the consensus variants don't use it, but the type
//! still has to be supplied. We use the same `TestId` shim the
//! protocol's own codec tests use: a unit type implementing
//! `Serialize`/`Deserialize`.

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use dynrunner_protocol_primary_secondary::messages::DistributedMessage;

use super::fsm::{ConsensusFsm, ConsensusOutput, ConsensusState, MeshSnapshot};
use super::{
    CONFIRMATION_DEADLINE, CONFIRMATION_MAX_RETRIES, RESOLUTION_DEADLINE,
    failover_quorum_threshold,
};

/// Lightweight task-id stand-in for the `DistributedMessage<I>` generic
/// parameter. The consensus variants don't carry an `I` field, so the
/// concrete type is irrelevant to the tests — it only has to exist.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
struct TestId(u64);

/// Helper: build a mesh snapshot that PASSES the side-gate.
///
/// `failover_quorum_threshold(6) = 4`, so 4 live peers out of 6 known
/// members trips exactly at the threshold (the test for "exactly at
/// threshold" passes-by-design).
fn snapshot_pass() -> MeshSnapshot {
    MeshSnapshot {
        live_peer_count: 4,
        total_known_members: 6,
    }
}

/// Helper: build a mesh snapshot that FAILS the side-gate.
///
/// `failover_quorum_threshold(6) = 4`, and `live_peer_count = 2 < 4`.
fn snapshot_fail() -> MeshSnapshot {
    MeshSnapshot {
        live_peer_count: 2,
        total_known_members: 6,
    }
}

fn ids(items: &[&str]) -> BTreeSet<String> {
    items.iter().map(|s| (*s).to_owned()).collect()
}

/// Drain the FSM through one `poll`, asserting that the result is an
/// `EmitFrame` and returning the wrapped frame for further pattern
/// matching.
fn expect_emit(out: ConsensusOutput<TestId>) -> DistributedMessage<TestId> {
    match out {
        ConsensusOutput::EmitFrame(f) => *f,
        other => panic!("expected EmitFrame, got {other:?}"),
    }
}

/// Single-suspect happy path: SchedulingSuspect → escalate → emit
/// `SuspectPeers` → no resolutions → past resolution deadline →
/// emit `RestartRequest` → all 3 responders confirm still-suspicious →
/// Restart{targets={X}}.
#[test]
fn single_suspect_happy_path_commits_restart() {
    let t0 = Instant::now();
    let mut fsm: ConsensusFsm<TestId> = ConsensusFsm::new();
    fsm.set_scheduling_suspect(ids(&["X"]));
    fsm.escalate(t0, /*epoch*/ 7, /*member_gen*/ 3, ids(&["A", "B", "C"]));

    // SuspectPeers emitted on first poll.
    let frame = expect_emit(fsm.poll(t0, &snapshot_pass()));
    match frame {
        DistributedMessage::SuspectPeers {
            consensus_id,
            primary_epoch,
            member_gen,
            suspected,
            ..
        } => {
            assert_eq!(consensus_id, 1);
            assert_eq!(primary_epoch, 7);
            assert_eq!(member_gen, 3);
            assert_eq!(suspected, vec!["X".to_owned()]);
        }
        other => panic!("expected SuspectPeers, got {other:?}"),
    }

    // Before deadline: Idle.
    assert!(matches!(
        fsm.poll(t0 + Duration::from_secs(1), &snapshot_pass()),
        ConsensusOutput::Idle
    ));

    // At resolution deadline: emit RestartRequest with candidates={X}.
    let frame = expect_emit(fsm.poll(t0 + RESOLUTION_DEADLINE, &snapshot_pass()));
    let consensus_id = match frame {
        DistributedMessage::RestartRequest {
            consensus_id,
            candidates,
            ..
        } => {
            assert_eq!(candidates, vec!["X".to_owned()]);
            consensus_id
        }
        other => panic!("expected RestartRequest, got {other:?}"),
    };

    // Each responder confirms still-suspicious=[X].
    for r in ["A", "B", "C"] {
        fsm.apply_confirm(consensus_id, r, vec!["X".to_owned()], vec![]);
    }

    // Next poll commits.
    match fsm.poll(t0 + RESOLUTION_DEADLINE + Duration::from_millis(1), &snapshot_pass()) {
        ConsensusOutput::Restart { targets } => assert_eq!(targets, ids(&["X"])),
        other => panic!("expected Restart, got {other:?}"),
    }
}

/// Single-suspect resolution: during CollectingResolutions a peer
/// resolves the suspect → next poll drops suspicion (no
/// RestartRequest).
#[test]
fn single_suspect_resolved_drops_suspicion() {
    let t0 = Instant::now();
    let mut fsm: ConsensusFsm<TestId> = ConsensusFsm::new();
    fsm.set_scheduling_suspect(ids(&["X"]));
    fsm.escalate(t0, 1, 1, ids(&["A", "B"]));

    let frame = expect_emit(fsm.poll(t0, &snapshot_pass()));
    let consensus_id = match frame {
        DistributedMessage::SuspectPeers { consensus_id, .. } => consensus_id,
        other => panic!("expected SuspectPeers, got {other:?}"),
    };

    // Mid-resolution: B reports it heard from X.
    fsm.apply_resolved(consensus_id, "B", "X");

    // Next poll (any time, even pre-deadline): DropSuspicion.
    match fsm.poll(t0 + Duration::from_millis(50), &snapshot_pass()) {
        ConsensusOutput::DropSuspicion => {}
        other => panic!("expected DropSuspicion, got {other:?}"),
    }
}

/// Multi-suspect happy path: {X,Y} both still suspicious → Restart{X,Y}.
#[test]
fn multi_suspect_happy_path() {
    let t0 = Instant::now();
    let mut fsm: ConsensusFsm<TestId> = ConsensusFsm::new();
    fsm.set_scheduling_suspect(ids(&["X", "Y"]));
    fsm.escalate(t0, 1, 1, ids(&["A", "B"]));

    let frame = expect_emit(fsm.poll(t0, &snapshot_pass()));
    match frame {
        DistributedMessage::SuspectPeers { suspected, .. } => {
            assert_eq!(suspected, vec!["X".to_owned(), "Y".to_owned()]);
        }
        other => panic!("expected SuspectPeers, got {other:?}"),
    }

    let frame = expect_emit(fsm.poll(t0 + RESOLUTION_DEADLINE, &snapshot_pass()));
    let consensus_id = match frame {
        DistributedMessage::RestartRequest {
            consensus_id,
            candidates,
            ..
        } => {
            assert_eq!(candidates, vec!["X".to_owned(), "Y".to_owned()]);
            consensus_id
        }
        other => panic!("expected RestartRequest, got {other:?}"),
    };

    for r in ["A", "B"] {
        fsm.apply_confirm(
            consensus_id,
            r,
            vec!["X".to_owned(), "Y".to_owned()],
            vec![],
        );
    }
    match fsm.poll(t0 + RESOLUTION_DEADLINE + Duration::from_millis(1), &snapshot_pass()) {
        ConsensusOutput::Restart { targets } => assert_eq!(targets, ids(&["X", "Y"])),
        other => panic!("expected Restart, got {other:?}"),
    }
}

/// Multi-suspect partial resolution mid-CollectingResolutions: Y
/// resolves; RestartRequest carries only [X]; Restart{X}.
#[test]
fn multi_suspect_partial_resolution_resolution_phase() {
    let t0 = Instant::now();
    let mut fsm: ConsensusFsm<TestId> = ConsensusFsm::new();
    fsm.set_scheduling_suspect(ids(&["X", "Y"]));
    fsm.escalate(t0, 1, 1, ids(&["A", "B"]));

    let frame = expect_emit(fsm.poll(t0, &snapshot_pass()));
    let consensus_id = match frame {
        DistributedMessage::SuspectPeers { consensus_id, .. } => consensus_id,
        other => panic!("expected SuspectPeers, got {other:?}"),
    };

    fsm.apply_resolved(consensus_id, "A", "Y");

    let frame = expect_emit(fsm.poll(t0 + RESOLUTION_DEADLINE, &snapshot_pass()));
    match &frame {
        DistributedMessage::RestartRequest { candidates, .. } => {
            assert_eq!(candidates, &vec!["X".to_owned()]);
        }
        other => panic!("expected RestartRequest, got {other:?}"),
    }

    for r in ["A", "B"] {
        fsm.apply_confirm(consensus_id, r, vec!["X".to_owned()], vec![]);
    }
    match fsm.poll(t0 + RESOLUTION_DEADLINE + Duration::from_millis(1), &snapshot_pass()) {
        ConsensusOutput::Restart { targets } => assert_eq!(targets, ids(&["X"])),
        other => panic!("expected Restart, got {other:?}"),
    }
}

/// Multi-suspect partial resolution mid-CollectingConfirmations: Y
/// resolved AFTER RestartRequest emitted; Restart{X}.
#[test]
fn multi_suspect_partial_resolution_confirm_phase() {
    let t0 = Instant::now();
    let mut fsm: ConsensusFsm<TestId> = ConsensusFsm::new();
    fsm.set_scheduling_suspect(ids(&["X", "Y"]));
    fsm.escalate(t0, 1, 1, ids(&["A", "B"]));

    let frame = expect_emit(fsm.poll(t0, &snapshot_pass()));
    let consensus_id = match frame {
        DistributedMessage::SuspectPeers { consensus_id, .. } => consensus_id,
        _ => panic!("expected SuspectPeers"),
    };
    expect_emit(fsm.poll(t0 + RESOLUTION_DEADLINE, &snapshot_pass()));

    // Mid-confirm: a stray ResolvedPeer carrying current round-id
    // arrives (e.g. a delayed probe-ack the secondary finally observed).
    fsm.apply_resolved(consensus_id, "A", "Y");

    for r in ["A", "B"] {
        fsm.apply_confirm(consensus_id, r, vec!["X".to_owned()], vec![]);
    }
    match fsm.poll(t0 + RESOLUTION_DEADLINE + Duration::from_millis(50), &snapshot_pass()) {
        ConsensusOutput::Restart { targets } => assert_eq!(targets, ids(&["X"])),
        other => panic!("expected Restart, got {other:?}"),
    }
}

/// Side-gate FAIL at BroadcastRestart entry: aborts the round.
#[test]
fn side_gate_fail_aborts_round() {
    let t0 = Instant::now();
    let mut fsm: ConsensusFsm<TestId> = ConsensusFsm::new();
    fsm.set_scheduling_suspect(ids(&["X"]));
    fsm.escalate(t0, 1, 1, ids(&["A", "B"]));

    expect_emit(fsm.poll(t0, &snapshot_pass()));
    // The side-gate is consulted only at BroadcastRestart entry; mid-
    // resolution polls pass any snapshot through harmlessly. So we
    // pass the FAILING snapshot only at the deadline transition.
    match fsm.poll(t0 + RESOLUTION_DEADLINE, &snapshot_fail()) {
        ConsensusOutput::Abort { reason } => {
            assert!(reason.contains("side-gate"), "reason was: {reason}");
            assert!(
                reason.contains("live_peer_count=2"),
                "reason should name the live count, was: {reason}"
            );
        }
        other => panic!("expected Abort, got {other:?}"),
    }
}

/// Side-gate PASS exactly at threshold: proceeds to RestartRequest.
#[test]
fn side_gate_pass_exactly_at_threshold() {
    let t0 = Instant::now();
    let mut fsm: ConsensusFsm<TestId> = ConsensusFsm::new();
    fsm.set_scheduling_suspect(ids(&["X"]));
    fsm.escalate(t0, 1, 1, ids(&["A"]));

    expect_emit(fsm.poll(t0, &snapshot_pass()));
    // total=6 → threshold=4, live=4 → exactly at threshold passes.
    let snap = MeshSnapshot {
        live_peer_count: 4,
        total_known_members: 6,
    };
    assert_eq!(failover_quorum_threshold(6), 4);
    let frame = expect_emit(fsm.poll(t0 + RESOLUTION_DEADLINE, &snap));
    assert!(matches!(frame, DistributedMessage::RestartRequest { .. }));
}

/// Retry-once-then-abort: second timeout with the same non-responders
/// triggers Abort.
#[test]
fn confirm_timeout_retries_once_then_aborts() {
    // The retry budget is a tunable constant; this test is written
    // against the owner-approved Q5 value of 1.
    assert_eq!(CONFIRMATION_MAX_RETRIES, 1);
    let t0 = Instant::now();
    let mut fsm: ConsensusFsm<TestId> = ConsensusFsm::new();
    fsm.set_scheduling_suspect(ids(&["X"]));
    fsm.escalate(t0, 1, 1, ids(&["A", "B", "C"]));

    expect_emit(fsm.poll(t0, &snapshot_pass()));
    let frame = expect_emit(fsm.poll(t0 + RESOLUTION_DEADLINE, &snapshot_pass()));
    let consensus_id = match frame {
        DistributedMessage::RestartRequest { consensus_id, .. } => consensus_id,
        _ => panic!("expected RestartRequest"),
    };

    // Only A and B respond by the first confirm deadline.
    fsm.apply_confirm(consensus_id, "A", vec!["X".to_owned()], vec![]);
    fsm.apply_confirm(consensus_id, "B", vec!["X".to_owned()], vec![]);

    let t_first_deadline = t0 + RESOLUTION_DEADLINE + CONFIRMATION_DEADLINE;
    // First confirm-deadline → retry emits a fresh RestartRequest.
    let frame = expect_emit(fsm.poll(t_first_deadline, &snapshot_pass()));
    assert!(matches!(frame, DistributedMessage::RestartRequest { .. }));

    // C still silent. Second deadline → abort.
    let t_second_deadline = t_first_deadline + CONFIRMATION_DEADLINE;
    match fsm.poll(t_second_deadline, &snapshot_pass()) {
        ConsensusOutput::Abort { reason } => {
            assert!(reason.contains("responder timeout"), "reason: {reason}");
            assert!(reason.contains("C"), "reason should name C: {reason}");
        }
        other => panic!("expected Abort, got {other:?}"),
    }
}

/// Retry succeeds: the missing responder replies between the first and
/// second confirm deadlines → Restart fires.
#[test]
fn confirm_retry_succeeds() {
    let t0 = Instant::now();
    let mut fsm: ConsensusFsm<TestId> = ConsensusFsm::new();
    fsm.set_scheduling_suspect(ids(&["X"]));
    fsm.escalate(t0, 1, 1, ids(&["A", "B", "C"]));

    expect_emit(fsm.poll(t0, &snapshot_pass()));
    let frame = expect_emit(fsm.poll(t0 + RESOLUTION_DEADLINE, &snapshot_pass()));
    let consensus_id = match frame {
        DistributedMessage::RestartRequest { consensus_id, .. } => consensus_id,
        _ => panic!("expected RestartRequest"),
    };

    fsm.apply_confirm(consensus_id, "A", vec!["X".to_owned()], vec![]);
    fsm.apply_confirm(consensus_id, "B", vec!["X".to_owned()], vec![]);

    let t_first_deadline = t0 + RESOLUTION_DEADLINE + CONFIRMATION_DEADLINE;
    expect_emit(fsm.poll(t_first_deadline, &snapshot_pass()));

    // C responds before the second deadline.
    fsm.apply_confirm(consensus_id, "C", vec!["X".to_owned()], vec![]);
    match fsm.poll(
        t_first_deadline + CONFIRMATION_DEADLINE / 2,
        &snapshot_pass(),
    ) {
        ConsensusOutput::Restart { targets } => assert_eq!(targets, ids(&["X"])),
        other => panic!("expected Restart, got {other:?}"),
    }
}

/// Stale consensus_id is ignored.
#[test]
fn stale_consensus_id_is_ignored() {
    let t0 = Instant::now();
    let mut fsm: ConsensusFsm<TestId> = ConsensusFsm::new();
    fsm.set_scheduling_suspect(ids(&["X"]));
    fsm.escalate(t0, 1, 1, ids(&["A"]));
    expect_emit(fsm.poll(t0, &snapshot_pass()));

    let current = fsm.current_consensus_id().expect("round in flight");
    fsm.apply_resolved(current + 99, "A", "X");

    // State should still hold X as a suspect.
    match fsm.state() {
        ConsensusState::CollectingResolutions { set, .. } => {
            assert!(set.contains("X"), "X must still be suspected after stale apply");
        }
        other => panic!("unexpected state: {other:?}"),
    }
}

/// A `resolved_since` retraction on a confirm frame removes the
/// candidate from the in-flight set; the responder still counts as
/// having replied.
#[test]
fn confirm_resolved_since_drops_candidate() {
    let t0 = Instant::now();
    let mut fsm: ConsensusFsm<TestId> = ConsensusFsm::new();
    fsm.set_scheduling_suspect(ids(&["X", "Y"]));
    fsm.escalate(t0, 1, 1, ids(&["A", "B"]));

    expect_emit(fsm.poll(t0, &snapshot_pass()));
    let frame = expect_emit(fsm.poll(t0 + RESOLUTION_DEADLINE, &snapshot_pass()));
    let consensus_id = match frame {
        DistributedMessage::RestartRequest { consensus_id, .. } => consensus_id,
        _ => panic!("expected RestartRequest"),
    };

    // A retracts Y; reports X still suspicious.
    fsm.apply_confirm(
        consensus_id,
        "A",
        vec!["X".to_owned()],
        vec!["Y".to_owned()],
    );
    fsm.apply_confirm(consensus_id, "B", vec!["X".to_owned()], vec![]);

    match fsm.poll(t0 + RESOLUTION_DEADLINE + Duration::from_millis(1), &snapshot_pass()) {
        ConsensusOutput::Restart { targets } => assert_eq!(targets, ids(&["X"])),
        other => panic!("expected Restart, got {other:?}"),
    }
}

/// `set_scheduling_suspect({})` on Idle is a no-op; `escalate` from
/// Idle is a no-op.
#[test]
fn empty_set_escalation_is_noop() {
    let t0 = Instant::now();
    let mut fsm: ConsensusFsm<TestId> = ConsensusFsm::new();

    fsm.set_scheduling_suspect(BTreeSet::new());
    assert!(matches!(fsm.state(), ConsensusState::Idle));

    fsm.escalate(t0, 1, 1, ids(&["A"]));
    assert!(matches!(fsm.state(), ConsensusState::Idle));

    assert!(matches!(
        fsm.poll(t0, &snapshot_pass()),
        ConsensusOutput::Idle
    ));
}

/// `failover_quorum_threshold` agrees with
/// [`crate::secondary::election::failover_quorum`] for n ∈ 0..32.
#[test]
fn failover_quorum_sweep_agrees() {
    for n in 0..32 {
        debug_assert_eq!(
            failover_quorum_threshold(n),
            crate::secondary::election::failover_quorum(n),
            "failover_quorum_threshold and secondary::election::failover_quorum disagree at n={n}"
        );
        // Also assert at release-mode level (debug_assert is no-op in
        // release; the test should fail even when --release).
        assert_eq!(
            failover_quorum_threshold(n),
            crate::secondary::election::failover_quorum(n),
            "n={n}"
        );
    }
}
