#![cfg(test)]

//! Anti-entropy convergence: a replica that missed a steady-state
//! mutation while "disconnected" heals on the next digest exchange.
//!
//! This is the class-of-bug test the narrow cold-start/failover catch-up
//! paths cannot pass: there is no late-join, no failover, no cold-start —
//! just a live secondary that DROPPED one `TaskCompleted` broadcast (a
//! transient mesh blip) and would otherwise stay permanently divergent.
//! Periodic digest exchange detects the divergence and pulls a snapshot to
//! heal it, then goes quiescent once converged.

use super::super::test_helpers::{
    FakeWorkerFactory, RecordingPeer, SecondaryHarness, TestId, election_config,
    make_secondary_recording,
};
use crate::cluster_state::{ClusterState, TaskState};
use dynrunner_core::{PhaseId, SoftPreferredSecondaries, TaskInfo, TypeId};
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, Destination, DistributedMessage, PrimaryChangeReason,
};
use std::path::PathBuf;

/// Minimal `TaskInfo` for a named task (mirrors the secondary test
/// helper shape).
fn mk_task(name: &str) -> TaskInfo<TestId> {
    TaskInfo {
        path: PathBuf::from(format!("/tasks/{name}")),
        size: 0,
        identifier: TestId(name.into()),
        phase_id: PhaseId::from("p0"),
        type_id: TypeId::from("t0"),
        affinity_id: None,
        payload: serde_json::Value::Null,
        task_id: name.into(),
        task_depends_on: Vec::new(),
        preferred_secondaries: SoftPreferredSecondaries::default(),
        preferred_version: Default::default(),
        kind: Default::default(),
        resolved_path: None,
    }
}

/// Build a secondary over a `RecordingPeer` so the test can inspect the
/// frames it emits (the anti-entropy snapshot pull lands in the log). The
/// coordinator's `MeshClient` QUEUES sends, so a test calls
/// [`SecondaryHarness::drain_egress`] before reading the log.
#[allow(clippy::type_complexity)]
fn make_recording_secondary(
    secondary_id: &str,
) -> (
    SecondaryHarness<RecordingPeer<TestId>>,
    std::rc::Rc<std::cell::RefCell<Vec<DistributedMessage<TestId>>>>,
) {
    make_secondary_recording(election_config(secondary_id), 1)
}

/// Count `RequestSnapshotStream` frames in the recorded peer-bus log
/// (the anti-entropy pull uses `send_to(Destination::Primary, ..)`, which
/// the `RecordingPeer` collates into the same log).
fn count_snapshot_requests(
    log: &std::rc::Rc<std::cell::RefCell<Vec<DistributedMessage<TestId>>>>,
) -> usize {
    log.borrow()
        .iter()
        .filter(|m| {
            matches!(
                m,
                DistributedMessage::RequestSnapshotStream { target: _, .. }
            )
        })
        .count()
}

/// A secondary that missed a steady-state `TaskCompleted` while
/// "disconnected" detects it is behind a peer's anti-entropy digest,
/// pulls a snapshot, restores, converges — and then a SECOND digest round
/// matches and pulls nothing (self-quiescing).
#[tokio::test(flavor = "current_thread")]
async fn transient_disconnect_heals_on_next_digest_cycle() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut sec, peer_log) = make_recording_secondary("worker-a");
            sec.enter_operational_for_test();

            // The COMPLETE peer (modelled by a donor `ClusterState`): it
            // saw the task added AND completed. Its digest + snapshot are
            // the wire data it would broadcast / answer with.
            let mut donor = ClusterState::<TestId>::new();
            donor.apply(ClusterMutation::PrimaryChanged {
                new: "setup".into(),
                epoch: 1,
                reason: PrimaryChangeReason::Election,
            });
            donor.apply(ClusterMutation::TaskAdded {
                hash: "t".into(),
                task: mk_task("t"),
            });
            donor.apply(ClusterMutation::TaskCompleted {
                attempt: 0,
                hash: "t".into(),
                result_data: None,
            });

            // The INCOMPLETE secondary: it learned the primary + saw the
            // task ADDED, but DROPPED the `TaskCompleted` broadcast during a
            // transient mesh blip — so its entry is stuck `Pending`.
            sec.cluster_state.apply(ClusterMutation::PrimaryChanged {
                new: "setup".into(),
                epoch: 1,
                reason: PrimaryChangeReason::Election,
            });
            sec.cluster_state.apply(ClusterMutation::TaskAdded {
                hash: "t".into(),
                task: mk_task("t"),
            });
            assert!(
                matches!(
                    sec.cluster_state.task_state("t"),
                    Some(TaskState::Pending { .. })
                ),
                "precondition: the secondary missed the TaskCompleted and is stuck Pending"
            );
            // The replicas genuinely diverge: same count, different fold.
            assert!(
                sec.cluster_state.digest().is_behind(&donor.digest()),
                "precondition: the incomplete secondary is behind the complete peer"
            );

            // ── Round 1: the peer's periodic digest arrives. ──
            let digest_frame = DistributedMessage::StateDigest {
                target: None,
                sender_id: "setup".into(),
                timestamp: 0.0,
                digest: donor.digest(),
                sender_is_observer: false,
            };
            sec.dispatch_message(digest_frame, &mut FakeWorkerFactory)
                .await
                .expect("StateDigest dispatch succeeds");
            // Drain the queued egress onto the RecordingPeer so the pull is
            // observable in the log (MeshClient::send is queued).
            sec.drain_egress().await;

            // The secondary detected it is behind and pulled a snapshot.
            assert_eq!(
                count_snapshot_requests(&peer_log),
                1,
                "behind a peer digest, the secondary must request exactly one snapshot"
            );

            // ── The pull reply: the peer answers with its package
            // stream (built by the SAME plan + codec a production
            // responder uses). ──
            for reply in
                crate::snapshot_stream::stream_frames_for_test(&donor, "setup", "worker-a/0")
            {
                sec.dispatch_message(reply, &mut FakeWorkerFactory)
                    .await
                    .expect("SnapshotStreamPackage dispatch succeeds");
            }

            // Healed: the dropped completion is now reflected, via the
            // EXISTING restore() lattice (no new merge logic).
            assert!(
                matches!(
                    sec.cluster_state.task_state("t"),
                    Some(TaskState::Completed { .. })
                ),
                "after the pull + restore the secondary converged to Completed"
            );
            assert_eq!(
                sec.cluster_state.digest(),
                donor.digest(),
                "the secondary's digest now matches the peer's"
            );

            // ── Round 2: the next digest round must be a NoOp (quiescent). ──
            peer_log.borrow_mut().clear();
            let digest_frame_2 = DistributedMessage::StateDigest {
                target: None,
                sender_id: "setup".into(),
                timestamp: 0.0,
                digest: donor.digest(),
                sender_is_observer: false,
            };
            sec.dispatch_message(digest_frame_2, &mut FakeWorkerFactory)
                .await
                .expect("second StateDigest dispatch succeeds");
            sec.drain_egress().await;
            assert_eq!(
                count_snapshot_requests(&peer_log),
                0,
                "once converged, a matching digest must trigger NO further pull (self-quiescing)"
            );
        })
        .await;
}

/// The cadence emit + the receive-side quiescence: a secondary that is
/// ALREADY converged with a peer does not pull on that peer's digest, and
/// `digest_broadcast` produces a well-formed `StateDigest` frame addressed
/// to the whole mesh.
#[tokio::test(flavor = "current_thread")]
async fn converged_secondary_emits_but_does_not_pull() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut sec, peer_log) = make_recording_secondary("worker-b");
            sec.enter_operational_for_test();

            sec.cluster_state.apply(ClusterMutation::TaskAdded {
                hash: "t".into(),
                task: mk_task("t"),
            });

            // A peer whose digest equals ours (converged).
            let mut peer = ClusterState::<TestId>::new();
            peer.apply(ClusterMutation::TaskAdded {
                hash: "t".into(),
                task: mk_task("t"),
            });
            assert_eq!(sec.cluster_state.digest(), peer.digest());

            // The emit path produces an All-addressed digest frame.
            let frame: DistributedMessage<TestId> = crate::anti_entropy::digest_broadcast(
                "worker-b",
                0.0,
                sec.cluster_state.digest(),
                false,
            );
            sec.send_to(Destination::All, frame)
                .await
                .expect("digest broadcast send succeeds");
            sec.drain_egress().await;
            assert!(
                peer_log
                    .borrow()
                    .iter()
                    .any(|m| matches!(m, DistributedMessage::StateDigest { target: _, .. })),
                "the cadence emit must put a StateDigest on the wire"
            );

            // Receiving the converged peer's digest pulls nothing.
            peer_log.borrow_mut().clear();
            let digest_frame = DistributedMessage::StateDigest {
                target: None,
                sender_id: "worker-c".into(),
                timestamp: 0.0,
                digest: peer.digest(),
                sender_is_observer: false,
            };
            sec.dispatch_message(digest_frame, &mut FakeWorkerFactory)
                .await
                .expect("StateDigest dispatch succeeds");
            sec.drain_egress().await;
            assert_eq!(
                count_snapshot_requests(&peer_log),
                0,
                "a converged digest must trigger no pull"
            );
        })
        .await;
}
