#![cfg(test)]

//! Post-abort work-starting gates + digest convergence (asm-dataset
//! run_20260611_112116, faces 2/3).
//!
//! Production trace: 3+ minutes AFTER the cluster's `RunAborted`
//! verdict, secondary-6 logged "pending first-bind assigned post-Ready"
//! and secondary-11 respawned workers for a type-shift — the secondary's
//! own continuations started work without consulting the replicated
//! run-terminal latch (and the two nodes had missed the verdict
//! broadcast entirely, so only anti-entropy could ever stop them).
//!
//! Pins, under fix:
//!   * the post-Ready pending-first-bind continuation consults the
//!     replicated `run_aborted` latch and DROPS the deferred bind;
//!   * the inbound `TaskAssignment` accept (the ordinary dispatch
//!     accept AND the first-bind / type-shift respawn edge — the
//!     respawn is triggered inside this arm) consults the latch and
//!     ignores the assignment;
//!   * a secondary that MISSED the `RunAborted` broadcast converges via
//!     the anti-entropy digest (#379 carries the verdict presence bit):
//!     peer digest → snapshot pull → `restore` latches `run_aborted` —
//!     the `process_tasks` loop-tail check then exits (pinned by
//!     `processing::run_aborted_yields_terminal_aborted`).

use super::super::test_helpers::{FakeWorkerFactory, make_secondary_recording};
use super::super::*;
use super::firstbind_orphan::{one_worker_config, task_assignment, test_oom_watcher};
use super::processing::make_binary;
use crate::cluster_state::ClusterState;
use dynrunner_manager_local::WorkerEvent;
use dynrunner_protocol_primary_secondary::{ClusterMutation, DistributedMessage};

/// The replicated run-terminal latch, applied to a CRDT mirror exactly
/// as a delivered `RunAborted` broadcast (or a healed snapshot) would
/// set it.
fn latch_run_aborted(cs: &mut ClusterState<test_helpers::TestId>) {
    cs.apply(ClusterMutation::RunAborted {
        reason: "1 duplicate task identity/identities within a single runtime \
                 spawn batch: duplicate task hash deadbeef"
            .into(),
    });
}

/// Face-2 RED #1 (secondary-6's zombie): a pending first-bind whose
/// fresh subprocess reaches Ready AFTER the run-terminal verdict is
/// latched must NOT be assigned ("pending first-bind assigned
/// post-Ready" on an aborted run). The deferred task is dropped — the
/// run is over; the loop-tail `run_aborted()` check tears the node down
/// in the same iteration.
#[tokio::test(flavor = "current_thread")]
async fn pending_first_bind_not_assigned_after_run_aborted() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut secondary, _log) = make_secondary_recording(one_worker_config("sec-6"), 1);
            secondary.set_bootstrap_primary_id("setup".to_string());

            let mut factory = FakeWorkerFactory;
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;

            // First-bind via the REAL dispatch path: RespawnInProgress →
            // stash in pending_first_bind (run not yet aborted).
            let binary = make_binary("build_variant-task", 50);
            let file_hash = "a11ce5e6".to_string();
            let assignment = task_assignment("setup", "sec-6", 0, &binary, &file_hash);
            secondary
                .handle_inbound(assignment, &mut FakeWorkerFactory)
                .await;
            assert!(
                secondary.op_mut().pending_first_bind.contains_key(&0),
                "fixture: the first-bind stash must land before the abort"
            );

            // The cluster's RunAborted verdict lands (broadcast or healed
            // snapshot) while the fresh subprocess is still spawning.
            latch_run_aborted(&mut secondary.cluster_state);

            // The fresh subprocess reaches Ready — the production
            // continuation point (secondary-6 logged its assignment
            // 3+ minutes post-abort here).
            let oom = test_oom_watcher();
            let ready = secondary
                .op_mut()
                .pool
                .recv_event()
                .await
                .expect("fresh subprocess must emit a Ready event");
            assert!(
                matches!(ready, WorkerEvent::Ready { worker_id: 0, .. }),
                "expected Ready for worker 0; got {ready:?}"
            );
            secondary.handle_worker_event(ready, &oom).await.unwrap();

            assert!(
                !secondary.op_mut().active_tasks.contains_key(&file_hash),
                "a pending first-bind must NOT be assigned post-Ready once the \
                 replicated run-terminal verdict is latched (the production \
                 'pending first-bind assigned post-Ready' zombie)"
            );
            assert!(
                !secondary.op_mut().pending_first_bind.contains_key(&0),
                "the deferred task is dropped — the run is over, nothing may \
                 hold it for a later bind"
            );
        })
        .await;
}

/// Face-2 RED #2 (secondary-11's zombie): an inbound `TaskAssignment`
/// arriving AFTER the run-terminal verdict is latched must be ignored —
/// no first-bind stash, no type-shift respawn (the respawn is triggered
/// inside this arm via `ensure_worker_for_type_async`), no assignment.
#[tokio::test(flavor = "current_thread")]
async fn task_assignment_ignored_after_run_aborted() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut secondary, _log) = make_secondary_recording(one_worker_config("sec-11"), 1);
            secondary.set_bootstrap_primary_id("setup".to_string());

            let mut factory = FakeWorkerFactory;
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;

            // The verdict is already latched when the assignment arrives
            // (an in-flight frame crossing the abort, or a promoted
            // survivor dispatching against a stale replica).
            latch_run_aborted(&mut secondary.cluster_state);

            let binary = make_binary("build_variant-task", 50);
            let file_hash = "b22df7c8".to_string();
            let assignment = task_assignment("setup", "sec-11", 0, &binary, &file_hash);
            secondary
                .handle_inbound(assignment, &mut FakeWorkerFactory)
                .await;

            assert!(
                !secondary.op_mut().pending_first_bind.contains_key(&0),
                "a post-abort TaskAssignment must NOT start a first-bind / \
                 type-shift respawn (the production 'respawned workers for \
                 type-shift' zombie)"
            );
            assert!(
                !secondary.op_mut().active_tasks.contains_key(&file_hash),
                "a post-abort TaskAssignment must NOT be assigned"
            );
        })
        .await;
}

/// Face-2 convergence (the missed-broadcast leg): a secondary that never
/// received the `RunAborted` broadcast converges via anti-entropy — the
/// peer's digest carries the verdict presence bit (#379), the secondary
/// detects it is behind and pulls a snapshot, and `restore` latches
/// `run_aborted` (first-writer-wins). The loop-tail exit on the latch is
/// pinned separately (`processing::run_aborted_yields_terminal_aborted`).
#[tokio::test(flavor = "current_thread")]
async fn missed_abort_broadcast_converges_via_digest() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut sec, peer_log) = make_secondary_recording(one_worker_config("sec-6"), 1);
            sec.enter_operational_for_test();

            // The donor peer that HOLDS the verdict (any survivor that saw
            // the broadcast — or the still-shutting-down primary).
            let mut donor = ClusterState::<test_helpers::TestId>::new();
            donor.apply(ClusterMutation::RunAborted {
                reason: "1 duplicate task identity/identities within a single \
                         runtime spawn batch: duplicate task hash deadbeef"
                    .into(),
            });

            assert!(
                sec.cluster_state.run_aborted().is_none(),
                "precondition: this secondary missed the RunAborted broadcast"
            );
            assert!(
                sec.cluster_state.digest().is_behind(&donor.digest()),
                "precondition: the verdict presence bit makes the laggard behind"
            );

            // The peer's periodic digest arrives → the secondary NOTES the
            // divergence to the disciplined pull driver, which broadcasts a
            // probe; the donor's probe reply then selects it and issues the
            // snapshot pull.
            sec.dispatch_message(
                DistributedMessage::StateDigest {
                    target: None,
                    sender_id: "peer-0".into(),
                    timestamp: 0.0,
                    digest: donor.digest(),
                    sender_is_observer: false,
                },
                &mut FakeWorkerFactory,
            )
            .await
            .expect("StateDigest dispatch succeeds");
            sec.drain_egress().await;
            assert!(
                peer_log
                    .borrow()
                    .iter()
                    .any(|m| matches!(m, DistributedMessage::PullProbe { .. })),
                "behind on the verdict bit, the secondary must broadcast a pull probe"
            );
            sec.complete_pull_probe_for_test("peer-0", false).await;
            sec.drain_egress().await;
            assert!(
                peer_log
                    .borrow()
                    .iter()
                    .any(|m| matches!(m, DistributedMessage::RequestSnapshotStream { .. })),
                "behind on the verdict bit, the secondary must pull a snapshot"
            );

            // The pull reply heals via restore — the verdict latches.
            for reply in
                crate::snapshot_stream::stream_frames_for_test(&donor, "peer-0", "worker-a/0")
            {
                sec.dispatch_message(reply, &mut FakeWorkerFactory)
                    .await
                    .expect("SnapshotStreamPackage dispatch succeeds");
            }
            let healed = sec
                .cluster_state
                .run_aborted()
                .expect("the snapshot heal must latch run_aborted on the laggard");
            assert!(
                healed.contains("duplicate task identity"),
                "the healed verdict carries the true abort reason: {healed}"
            );
        })
        .await;
}
