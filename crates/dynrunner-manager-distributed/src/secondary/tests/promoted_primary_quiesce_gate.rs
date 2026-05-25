#![cfg(test)]

//! Tests pinning the promotion-settle-period gate on the
//! promoted-primary natural-quiesce `RunComplete`-broadcast branch
//! (asm-dataset-nix T11 regression).
//!
//! Single concern: the gate ensures a freshly promoted secondary does
//! not declare the cluster done on the basis of a partial
//! `cluster_state` mirror — see
//! `SecondaryCoordinator::promoted_at` and
//! `SecondaryConfig::promoted_primary_quiesce_grace` for the full
//! rationale. The three tests below cover:
//!
//!   1. Immediately after promotion the gate suppresses the branch
//!      even when (a) and (b) are satisfied. After the grace elapses
//!      and no new task-state mutations have arrived, the branch
//!      becomes eligible.
//!   2. The bug scenario: a freshly promoted secondary with a partial
//!      mirror (e.g. 5 of 10 tasks observed, all 5 terminal) does NOT
//!      satisfy eligibility during the grace window; if the missing
//!      5 `TaskAdded` mutations arrive during the grace, the
//!      `cluster_quiesced` arm flips back to false and the branch
//!      stays suppressed — exactly the property the bandage protects.
//!   3. A `None` `promoted_at` (defensive: should not occur because
//!      `is_primary` is set in lockstep, but the predicate must fail
//!      closed) keeps eligibility false even when every other gate
//!      holds.

use super::super::test_helpers::{election_config, FakeWorkerFactory, FixedEstimator, RecordingPeer, TestId};
use super::super::*;
use dynrunner_core::{PhaseId, SoftPreferredSecondaries, TaskInfo, TypeId};
use dynrunner_protocol_primary_secondary::{ClusterMutation, DistributedMessage};
use dynrunner_scheduler::ResourceStealingScheduler;
use dynrunner_transport_channel::ChannelPrimaryTransportEnd;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::mpsc as tokio_mpsc;

/// Builder mirroring `setup_promote_discriminator::make_secondary_with_recording_peer`
/// but kept local so this test file owns its own fixture lifecycle.
/// The construction shape is identical so a future maintainer can
/// fold them together once a fourth caller needs the same wiring.
#[allow(clippy::type_complexity)]
fn make_secondary_with_recording_peer(
    secondary_id: &str,
    peer_count: usize,
) -> (
    SecondaryCoordinator<
        ChannelPrimaryTransportEnd<TestId>,
        RecordingPeer<TestId>,
        dynrunner_transport_channel::ChannelManagerEnd,
        ResourceStealingScheduler,
        FixedEstimator,
        TestId,
    >,
    tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
    std::rc::Rc<std::cell::RefCell<Vec<DistributedMessage<TestId>>>>,
) {
    let (sec_to_pri_tx, sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
    let (_pri_to_sec_tx, pri_to_sec_rx) =
        tokio_mpsc::unbounded_channel::<DistributedMessage<TestId>>();
    let transport = ChannelPrimaryTransportEnd {
        tx: sec_to_pri_tx,
        rx: pri_to_sec_rx,
    };
    let recorder = RecordingPeer::<TestId>::new(peer_count);
    let peer_log = recorder.log_handle();
    let sec = SecondaryCoordinator::new(
        election_config(secondary_id),
        transport,
        recorder,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    (sec, sec_to_pri_rx, peer_log)
}

fn make_task(name: &str, phase: &str) -> TaskInfo<TestId> {
    TaskInfo {
        path: PathBuf::from(format!("/tmp/{name}")),
        size: 100,
        identifier: TestId(name.into()),
        phase_id: PhaseId::from(phase),
        type_id: TypeId::from("default"),
        affinity_id: None,
        payload: serde_json::Value::Null,
        task_id: name.into(),
        task_depends_on: vec![],
        preferred_secondaries: SoftPreferredSecondaries::default(),
        resolved_path: None,
    }
}

/// Convenience: apply `TaskAdded` + `TaskCompleted` to push one task
/// to terminal state in the CRDT mirror. Mirrors the wire-order of
/// a real production task lifecycle so the resulting `counts()`
/// partition matches the production shape.
fn add_completed_task(
    sec: &mut SecondaryCoordinator<
        ChannelPrimaryTransportEnd<TestId>,
        RecordingPeer<TestId>,
        dynrunner_transport_channel::ChannelManagerEnd,
        ResourceStealingScheduler,
        FixedEstimator,
        TestId,
    >,
    name: &str,
    phase: &str,
) {
    let task = make_task(name, phase);
    let hash = format!("hash_{name}");
    sec.cluster_state.apply(ClusterMutation::<TestId>::TaskAdded {
        hash: hash.clone(),
        task,
    });
    sec.cluster_state.apply(ClusterMutation::<TestId>::TaskCompleted {
        hash,
        result_data: None,
    });
}

/// (1) After promotion the gate suppresses eligibility for at least
/// `promoted_primary_quiesce_grace`; once the grace elapses with no
/// further state changes, eligibility becomes true.
#[tokio::test(flavor = "current_thread", start_paused = false)]
async fn promotion_grace_suppresses_then_releases_eligibility() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut sec, _pri_rx, _peer_log) =
                make_secondary_with_recording_peer("sec-a", 0);

            // Use a short grace to keep the test fast. The
            // `election_config` helper already sets the grace to
            // 100 ms (see `test_helpers::election_config`); pin that
            // here so the test is robust against a future default
            // change.
            sec.config.promoted_primary_quiesce_grace = Duration::from_millis(100);

            // Pre-seed cluster_state with 2 fully-completed tasks so
            // gate (b) (cluster_quiesced) holds independent of the
            // grace.
            sec.cluster_state.apply(ClusterMutation::<TestId>::PhaseDepsSet {
                deps: HashMap::new(),
            });
            add_completed_task(&mut sec, "bin1", "default");
            add_completed_task(&mut sec, "bin2", "default");
            let counts = sec.cluster_state.counts();
            assert_eq!(counts.pending, 0);
            assert_eq!(counts.in_flight, 0);
            assert_eq!(counts.completed, 2);

            // Promote: flips is_primary + stamps promoted_at via
            // dispatch_message's PromotePrimary handler.
            let promote = DistributedMessage::PromotePrimary {
                sender_id: "primary".into(),
                timestamp: 0.0,
                new_primary_id: "sec-a".into(),
                epoch: 1,
                required_setup: false,
            };
            sec.dispatch_message(promote, &mut None, &mut FakeWorkerFactory)
                .await
                .expect("PromotePrimary handler succeeds");
            assert!(sec.is_primary, "promotion flipped is_primary");
            assert!(
                sec.promoted_at.is_some(),
                "promotion stamped promoted_at"
            );

            // Immediately post-promotion: gate (c) suppresses the
            // branch even though gates (a) and (b) hold. This is the
            // load-bearing assertion for the bug fix: pre-fix the
            // branch fires at this exact tick.
            assert!(
                !sec.promoted_primary_natural_quiesce_eligible(),
                "during the grace window the branch must NOT be eligible \
                 even when (a) local-drained and (b) cluster-quiesced hold"
            );

            // Wait the grace plus a small slack (cargo test scheduling
            // jitter); afterwards the branch becomes eligible.
            tokio::time::sleep(Duration::from_millis(150)).await;
            assert!(
                sec.promoted_primary_natural_quiesce_eligible(),
                "after the grace window has elapsed with no further \
                 state changes, the branch must be eligible to fire"
            );
        })
        .await;
}

/// (2) Bug scenario: a freshly promoted secondary with a *partial*
/// CRDT mirror (e.g. 2 of 4 tasks observed, both terminal) reproduces
/// the asm-dataset-nix T11 partial-view condition. The grace gate
/// suppresses the branch; if the missing tasks then arrive during
/// the grace via `TaskAdded`, the `cluster_quiesced` arm flips back
/// to false and the branch stays correctly suppressed even after the
/// grace elapses.
#[tokio::test(flavor = "current_thread", start_paused = false)]
async fn partial_mirror_during_grace_blocks_spurious_fire() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut sec, _pri_rx, _peer_log) =
                make_secondary_with_recording_peer("sec-a", 0);
            sec.config.promoted_primary_quiesce_grace = Duration::from_millis(100);

            // Seed only PART of the eventual ledger — exactly the
            // bug scenario. 2 tasks observed, both terminal:
            // cluster_state from sec-a's POV is "quiesced", but the
            // demoted primary will publish 2 more tasks during the
            // grace.
            sec.cluster_state.apply(ClusterMutation::<TestId>::PhaseDepsSet {
                deps: HashMap::new(),
            });
            add_completed_task(&mut sec, "bin1", "default");
            add_completed_task(&mut sec, "bin2", "default");
            let pre_counts = sec.cluster_state.counts();
            assert_eq!(pre_counts.pending, 0);
            assert_eq!(pre_counts.in_flight, 0);
            assert_eq!(pre_counts.completed, 2);

            // Promote.
            let promote = DistributedMessage::PromotePrimary {
                sender_id: "primary".into(),
                timestamp: 0.0,
                new_primary_id: "sec-a".into(),
                epoch: 1,
                required_setup: false,
            };
            sec.dispatch_message(promote, &mut None, &mut FakeWorkerFactory)
                .await
                .expect("PromotePrimary handler succeeds");

            // Mid-grace: the missing 2 tasks arrive as a delayed
            // CRDT broadcast — `TaskAdded` only, no completion. From
            // sec-a's POV the cluster transitions from "quiesced" to
            // "2 pending" because the demoted primary hadn't
            // dispatched them yet at promotion time.
            sec.cluster_state.apply(ClusterMutation::<TestId>::TaskAdded {
                hash: "hash_bin3".into(),
                task: make_task("bin3", "default"),
            });
            sec.cluster_state.apply(ClusterMutation::<TestId>::TaskAdded {
                hash: "hash_bin4".into(),
                task: make_task("bin4", "default"),
            });
            let mid_counts = sec.cluster_state.counts();
            assert_eq!(
                mid_counts.pending, 2,
                "the late `TaskAdded` arrivals flip cluster_state \
                 from quiesced to 2 pending"
            );

            // Wait past the grace. Branch must remain ineligible
            // because gate (b) (cluster_quiesced) is now false —
            // even though gate (c) is satisfied.
            tokio::time::sleep(Duration::from_millis(150)).await;
            assert!(
                !sec.promoted_primary_natural_quiesce_eligible(),
                "with cluster_state showing 2 pending tasks the branch \
                 must NOT be eligible regardless of the grace window"
            );
        })
        .await;
}

/// (3) Defensive: a `None` `promoted_at` (which should never occur
/// in production because `is_primary = true` is set in lockstep with
/// the stamp) must keep the branch ineligible. Fails closed:
/// "spurious fire" is the failure mode the gate exists to prevent,
/// so the predicate's reading of `None` must be conservative.
#[tokio::test(flavor = "current_thread", start_paused = false)]
async fn missing_promoted_at_is_treated_as_unsettled() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut sec, _pri_rx, _peer_log) =
                make_secondary_with_recording_peer("sec-a", 0);

            // Pre-seed the ledger so gates (a) and (b) would hold.
            sec.cluster_state.apply(ClusterMutation::<TestId>::PhaseDepsSet {
                deps: HashMap::new(),
            });
            add_completed_task(&mut sec, "bin1", "default");

            // Force the defensive scenario: is_primary set without
            // promoted_at being stamped. Should never happen via the
            // sanctioned promotion paths (router.rs PromotePrimary
            // arm + election/coordinator.rs record_promotion_confirm
            // both stamp promoted_at). The test exercises the
            // predicate's failure-mode contract, not the production
            // call sites.
            sec.is_primary = true;
            assert!(
                sec.promoted_at.is_none(),
                "test setup: promoted_at is None"
            );

            // Even waiting longer than any reasonable grace, the
            // branch must remain ineligible.
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert!(
                !sec.promoted_primary_natural_quiesce_eligible(),
                "promoted_at == None must short-circuit eligibility to false"
            );
        })
        .await;
}
