//! Deterministic-tick unit tests for [`super::SecondaryConsensusFsm`].
//!
//! Every test drives the FSM through hand-stepped `Instant`s and a
//! `FixedJitter` so probe-fire deadlines are byte-exact across CI runs.
//! The 14 tests below cover every transition the diagram in
//! [`super::super`]'s module doc declares, including the stateless
//! `apply_probe_request` answer-from-`Idle` path that realises the
//! owner's spec verbatim ("if at any time during the period they get an
//! answer from the secondary").

use std::time::{Duration, Instant};

use dynrunner_protocol_primary_secondary::messages::DistributedMessage;

use super::fsm::{SecondaryConsensusFsm, SecondaryConsensusOutput, SecondaryConsensusState};
use super::jitter::FixedJitter;
use super::{AWAITING_ROUND_END_TIMEOUT, PROBE_BASE_PERIOD};

/// Lightweight task-id stand-in for the `DistributedMessage<I>` generic
/// parameter. The consensus variants don't carry an `I` field; the
/// concrete type is irrelevant to the tests — it only has to exist.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
struct TestId(u64);

fn mk_fsm(self_id: &str) -> SecondaryConsensusFsm<TestId, FixedJitter> {
    SecondaryConsensusFsm::with_jitter(self_id.to_owned(), FixedJitter(0))
}

fn mk_fsm_with_jitter(
    self_id: &str,
    jitter_ms: i32,
) -> SecondaryConsensusFsm<TestId, FixedJitter> {
    SecondaryConsensusFsm::with_jitter(self_id.to_owned(), FixedJitter(jitter_ms))
}

fn drain(out: SecondaryConsensusOutput<TestId>) -> Vec<DistributedMessage<TestId>> {
    match out {
        SecondaryConsensusOutput::Idle => Vec::new(),
        SecondaryConsensusOutput::EmitFrames(frames) => frames.into_iter().map(|b| *b).collect(),
    }
}

/// (1) `apply_suspect_peers` from `Idle`: transitions to `ProbingFor`
/// and inline-fires the opening `PeerProbe`. With jitter=0 the next
/// probe lands at `now + PROBE_BASE_PERIOD` exactly.
#[test]
fn apply_suspect_peers_from_idle_emits_opening_probe() {
    let t0 = Instant::now();
    let mut fsm = mk_fsm("S");

    let out = fsm.apply_suspect_peers(42, 3, 1, &["X".to_owned()], t0);
    let frames = drain(out);
    assert_eq!(frames.len(), 1);
    match &frames[0] {
        DistributedMessage::PeerProbe {
            consensus_id,
            probed_id,
            ..
        } => {
            assert_eq!(*consensus_id, 42);
            assert_eq!(probed_id, "X");
        }
        other => panic!("expected PeerProbe, got {other:?}"),
    }
    // Before the next-fire deadline: poll is silent.
    let frames = drain(fsm.poll(t0 + Duration::from_secs(1)));
    assert!(frames.is_empty());
    // At now + 5s exactly (jitter=0): next probe fires.
    let frames = drain(fsm.poll(t0 + PROBE_BASE_PERIOD));
    assert_eq!(frames.len(), 1);
    assert!(matches!(&frames[0], DistributedMessage::PeerProbe { .. }));
}

/// (2) `apply_probe_ack` whose `sender_id` matches an in-flight suspect
/// emits a `ResolvedPeer` and removes the peer from active probing.
#[test]
fn apply_probe_ack_matched_emits_resolved_peer() {
    let t0 = Instant::now();
    let mut fsm = mk_fsm("S");
    let _ = drain(fsm.apply_suspect_peers(42, 1, 1, &["X".to_owned()], t0));

    let out = fsm.apply_probe_ack("X", 42, t0 + Duration::from_millis(10));
    let frames = drain(out);
    assert_eq!(frames.len(), 1);
    match &frames[0] {
        DistributedMessage::ResolvedPeer {
            consensus_id,
            observer_id,
            resolved,
            ..
        } => {
            assert_eq!(*consensus_id, 42);
            assert_eq!(observer_id, "S");
            assert_eq!(resolved, "X");
        }
        other => panic!("expected ResolvedPeer, got {other:?}"),
    }
    // X is removed from active probing — next poll at +5s emits no
    // probe (no suspects left).
    let frames = drain(fsm.poll(t0 + PROBE_BASE_PERIOD));
    assert!(frames.is_empty());
}

/// (3) `apply_probe_ack` with a stale `consensus_id` is dropped silently.
#[test]
fn apply_probe_ack_stale_consensus_id_is_ignored() {
    let t0 = Instant::now();
    let mut fsm = mk_fsm("S");
    let _ = drain(fsm.apply_suspect_peers(42, 1, 1, &["X".to_owned()], t0));

    let out = fsm.apply_probe_ack("X", 99, t0 + Duration::from_millis(10));
    assert!(drain(out).is_empty());
    match fsm.state() {
        SecondaryConsensusState::ProbingFor { suspects, .. } => {
            assert!(suspects.contains_key("X"), "X must still be probed");
        }
        other => panic!("unexpected state: {other:?}"),
    }
}

/// (4) `apply_probe_ack` whose `sender_id` is not in the suspect set is
/// dropped silently; the active suspect is untouched.
#[test]
fn apply_probe_ack_wrong_sender_is_ignored() {
    let t0 = Instant::now();
    let mut fsm = mk_fsm("S");
    let _ = drain(fsm.apply_suspect_peers(42, 1, 1, &["X".to_owned()], t0));

    let out = fsm.apply_probe_ack("Q", 42, t0 + Duration::from_millis(10));
    assert!(drain(out).is_empty());
    match fsm.state() {
        SecondaryConsensusState::ProbingFor { suspects, .. } => {
            assert!(suspects.contains_key("X"), "X must still be probed");
        }
        other => panic!("unexpected state: {other:?}"),
    }
}

/// (5) `apply_restart_request` from `ProbingFor` with a partial
/// `resolved_set` computes `still_suspicious` + `resolved_since`
/// correctly.
#[test]
fn apply_restart_request_from_probing_with_partial_resolved() {
    let t0 = Instant::now();
    let mut fsm = mk_fsm("S");
    let _ = drain(fsm.apply_suspect_peers(42, 1, 1, &["X".to_owned(), "Y".to_owned()], t0));
    // Resolve Y mid-round.
    let _ = drain(fsm.apply_probe_ack("Y", 42, t0 + Duration::from_millis(10)));

    let out = fsm.apply_restart_request(
        42,
        1,
        1,
        &["X".to_owned(), "Y".to_owned()],
        t0 + Duration::from_millis(20),
    );
    let frames = drain(out);
    assert_eq!(frames.len(), 1);
    match &frames[0] {
        DistributedMessage::RestartConfirm {
            consensus_id,
            responder_id,
            still_suspicious,
            resolved_since,
            ..
        } => {
            assert_eq!(*consensus_id, 42);
            assert_eq!(responder_id, "S");
            assert_eq!(still_suspicious, &vec!["X".to_owned()]);
            assert_eq!(resolved_since, &vec!["Y".to_owned()]);
        }
        other => panic!("expected RestartConfirm, got {other:?}"),
    }
}

/// (6) `apply_restart_request` with a stale `primary_epoch` is ignored.
#[test]
fn apply_restart_request_stale_primary_epoch_is_ignored() {
    let t0 = Instant::now();
    let mut fsm = mk_fsm("S");
    // Advance the FSM's known epoch to 3 via an opening round.
    let _ = drain(fsm.apply_suspect_peers(42, 3, 1, &["X".to_owned()], t0));

    let out = fsm.apply_restart_request(
        42,
        2, // stale
        1,
        &["X".to_owned()],
        t0 + Duration::from_millis(10),
    );
    assert!(drain(out).is_empty());
    // State is still the original ProbingFor round.
    assert_eq!(fsm.current_consensus_id(), Some(42));
}

/// (7) `apply_restart_request` with an ADVANCED `primary_epoch` resets
/// the FSM to a fresh state and answers the request under the new
/// epoch (with `still_suspicious = candidates` since no probe evidence
/// survives the reset).
#[test]
fn apply_restart_request_advanced_primary_epoch_resets_and_answers() {
    let t0 = Instant::now();
    let mut fsm = mk_fsm("S");
    // Open under epoch 3.
    let _ = drain(fsm.apply_suspect_peers(42, 3, 1, &["X".to_owned()], t0));

    // RestartRequest from a freshly-elected primary at epoch 4 about
    // candidates {Y, Z}. The FSM has zero per-round evidence on Y/Z
    // (they're new under the new epoch), so still_suspicious = {Y,Z}.
    let out = fsm.apply_restart_request(
        7,
        4,
        2,
        &["Y".to_owned(), "Z".to_owned()],
        t0 + Duration::from_millis(10),
    );
    let frames = drain(out);
    assert_eq!(frames.len(), 1);
    match &frames[0] {
        DistributedMessage::RestartConfirm {
            consensus_id,
            still_suspicious,
            resolved_since,
            ..
        } => {
            assert_eq!(*consensus_id, 7);
            assert_eq!(still_suspicious, &vec!["Y".to_owned(), "Z".to_owned()]);
            assert!(resolved_since.is_empty());
        }
        other => panic!("expected RestartConfirm, got {other:?}"),
    }
    // FSM now tracks the advanced epoch.
    assert_eq!(fsm.current_consensus_id(), Some(7));
}

/// (8) `apply_probe_request` from `Idle` emits a `PeerProbeAck` and
/// leaves the FSM in `Idle`. THIS is the stateless half: a suspected
/// secondary answers without running a round of its own.
#[test]
fn apply_probe_request_from_idle_emits_ack() {
    let t0 = Instant::now();
    let mut fsm = mk_fsm("S");
    assert!(matches!(fsm.state(), SecondaryConsensusState::Idle));

    let out = fsm.apply_probe_request("B", 99, "S", t0);
    let frames = drain(out);
    assert_eq!(frames.len(), 1);
    match &frames[0] {
        DistributedMessage::PeerProbeAck {
            consensus_id,
            prober_id,
            ..
        } => {
            assert_eq!(*consensus_id, 99);
            assert_eq!(prober_id, "B");
        }
        other => panic!("expected PeerProbeAck, got {other:?}"),
    }
    assert!(matches!(fsm.state(), SecondaryConsensusState::Idle));
}

/// (9) `apply_probe_request` with a non-self `probed_id` is dropped
/// silently — defensive against fan-out / stale role-route.
#[test]
fn apply_probe_request_wrong_target_is_ignored() {
    let t0 = Instant::now();
    let mut fsm = mk_fsm("S");

    let out = fsm.apply_probe_request("B", 99, "OTHER", t0);
    assert!(drain(out).is_empty());
}

/// (10) Jitter randomization: with `jitter_ms = +500`, the next probe
/// fires at `+5500ms`; with `jitter_ms = -1000`, at `+4000ms`.
#[test]
fn jitter_randomizes_next_fire_deadline() {
    let t0 = Instant::now();
    let positive = {
        let mut fsm = mk_fsm_with_jitter("S", 500);
        let _ = drain(fsm.apply_suspect_peers(1, 1, 1, &["X".to_owned()], t0));
        fsm
    };
    let negative = {
        let mut fsm = mk_fsm_with_jitter("S", -1000);
        let _ = drain(fsm.apply_suspect_peers(1, 1, 1, &["X".to_owned()], t0));
        fsm
    };

    // For positive jitter: at +5500 - 1ms the next probe is NOT yet
    // due; at +5500ms it fires.
    let mut p = positive;
    assert!(drain(p.poll(t0 + Duration::from_millis(5499))).is_empty());
    let frames = drain(p.poll(t0 + Duration::from_millis(5500)));
    assert_eq!(frames.len(), 1);

    // For negative jitter: at +3999ms the next probe is NOT yet due;
    // at +4000ms it fires.
    let mut n = negative;
    assert!(drain(n.poll(t0 + Duration::from_millis(3999))).is_empty());
    let frames = drain(n.poll(t0 + Duration::from_millis(4000)));
    assert_eq!(frames.len(), 1);
}

/// (11) `AwaitingRoundEnd` timeout: a `poll(now)` past
/// `AWAITING_ROUND_END_TIMEOUT` returns to `Idle`.
#[test]
fn awaiting_round_end_times_out_to_idle() {
    let t0 = Instant::now();
    let mut fsm = mk_fsm("S");
    let _ = drain(fsm.apply_suspect_peers(42, 1, 1, &["X".to_owned()], t0));
    let _ = drain(fsm.apply_restart_request(42, 1, 1, &["X".to_owned()], t0));
    assert!(matches!(
        fsm.state(),
        SecondaryConsensusState::AwaitingRoundEnd { .. }
    ));

    // Just before deadline: still AwaitingRoundEnd.
    let _ = drain(fsm.poll(t0 + AWAITING_ROUND_END_TIMEOUT - Duration::from_millis(1)));
    assert!(matches!(
        fsm.state(),
        SecondaryConsensusState::AwaitingRoundEnd { .. }
    ));

    // At deadline: returns to Idle.
    let _ = drain(fsm.poll(t0 + AWAITING_ROUND_END_TIMEOUT));
    assert!(matches!(fsm.state(), SecondaryConsensusState::Idle));
}

/// (12) New round under same `primary_epoch` with a different
/// `consensus_id`: the FSM transitions into a fresh `ProbingFor` for
/// the new round's set, abandoning the prior round's tally.
#[test]
fn new_round_same_epoch_different_cid_starts_fresh() {
    let t0 = Instant::now();
    let mut fsm = mk_fsm("S");
    let _ = drain(fsm.apply_suspect_peers(42, 1, 1, &["X".to_owned()], t0));
    assert_eq!(fsm.current_consensus_id(), Some(42));

    let _ = drain(fsm.apply_suspect_peers(43, 1, 1, &["Y".to_owned()], t0));
    match fsm.state() {
        SecondaryConsensusState::ProbingFor {
            consensus_id,
            suspects,
            resolved,
            ..
        } => {
            assert_eq!(*consensus_id, 43);
            assert!(suspects.contains_key("Y"));
            assert!(!suspects.contains_key("X"), "old suspects must be cleared");
            assert!(resolved.is_empty());
        }
        other => panic!("unexpected state: {other:?}"),
    }
}

/// (13) `primary_epoch` advance mid-round: the FSM resets and opens a
/// new round under the freshly-elected primary's epoch + cid.
#[test]
fn primary_epoch_advance_mid_round_resets() {
    let t0 = Instant::now();
    let mut fsm = mk_fsm("S");
    let _ = drain(fsm.apply_suspect_peers(42, 3, 1, &["X".to_owned()], t0));

    let _ = drain(fsm.apply_suspect_peers(1, 4, 1, &["Z".to_owned()], t0));
    match fsm.state() {
        SecondaryConsensusState::ProbingFor {
            consensus_id,
            primary_epoch,
            suspects,
            ..
        } => {
            assert_eq!(*consensus_id, 1);
            assert_eq!(*primary_epoch, 4);
            assert!(suspects.contains_key("Z"));
            assert!(!suspects.contains_key("X"));
        }
        other => panic!("unexpected state: {other:?}"),
    }
}

/// (14) A second `PeerProbeAck` from the same target does NOT emit a
/// second `ResolvedPeer` (defensive against duplicate ack delivery).
#[test]
fn double_probe_ack_emits_resolved_only_once() {
    let t0 = Instant::now();
    let mut fsm = mk_fsm("S");
    let _ = drain(fsm.apply_suspect_peers(42, 1, 1, &["X".to_owned()], t0));

    let frames = drain(fsm.apply_probe_ack("X", 42, t0 + Duration::from_millis(10)));
    assert_eq!(frames.len(), 1);
    assert!(matches!(&frames[0], DistributedMessage::ResolvedPeer { .. }));

    let frames = drain(fsm.apply_probe_ack("X", 42, t0 + Duration::from_millis(20)));
    assert!(
        frames.is_empty(),
        "second ack must not produce a second ResolvedPeer"
    );
}
