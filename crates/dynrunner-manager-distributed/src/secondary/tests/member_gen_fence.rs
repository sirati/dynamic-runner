//! Pre-start fences A + B (#530) — the secondary's router-side gates that
//! refuse to start a TaskAssignment when:
//!   A) the SUPPLANTED HOLDER (an originally-dispatched member that the
//!      primary then peer-removed) is alive again at a `peer_member_gen`
//!      that matches or exceeds the gen stamped on the dispatch hint —
//!      i.e. the peer-removal was false-dead and the LIVE original is
//!      already running the task; double-execution must be refused.
//!   B) the addressee's `peer_member_gen` on the dispatch lease does not
//!      equal this secondary's CURRENT `peer_member_gen` — i.e. a
//!      re-removal-and-re-admission crossed the dispatch in flight, and
//!      the lease is wholly invalid for the live incarnation.
//!
//! These are router-only gates. We replay each shape verbatim at the
//! wire level: build a secondary with a known cluster_state, dispatch a
//! `TaskAssignment` carrying the fence fields directly, and assert
//! whether the worker received an assign_task (the dispatch ran) or a
//! `TaskFailed` carrying the fence marker landed on the primary log
//! (the dispatch was refused). The name `member_gen_fence.rs` is
//! deliberate: this file's gates compare CRDT `peer_member_gen` values
//! and are distinct from `generation_gate.rs`, which gates worker-slot
//! REPLACEMENT generations (a different concept on a different keyspace).

#![cfg(test)]

use super::super::dispatch::{
    TASK_STALE_ADDRESSEE_GEN_WIRE_MESSAGE, TASK_SUPPLANTED_BY_LIVE_HOLDER_WIRE_MESSAGE,
};
use super::super::test_helpers::{FakeWorkerFactory, make_secondary_recording};
use super::super::*;
use super::firstbind_orphan::one_worker_config;
use super::processing::make_binary;
use dynrunner_protocol_primary_secondary::{ClusterMutation, DistributedBinaryInfo, DistributedMessage};

/// Build a wire `TaskAssignment` carrying explicit fence fields. Mirrors
/// the production primary-side stamping shape — `supplanted_holder`
/// rides on the redirect dispatch path, `secondary_id_member_gen` rides
/// on every primary that knows the addressee's recorded gen.
fn task_assignment_with_fences(
    sec_id: &str,
    binary: &dynrunner_core::TaskInfo<test_helpers::TestId>,
    file_hash: &str,
    supplanted_holder: Option<(String, u64)>,
    secondary_id_member_gen: Option<u64>,
) -> DistributedMessage<test_helpers::TestId> {
    DistributedMessage::TaskAssignment {
        target: None,
        sender_id: "setup".into(),
        timestamp: 0.0,
        secondary_id: sec_id.into(),
        worker_id: 0,
        zip_file: None,
        binary_info: DistributedBinaryInfo::from_task_info(binary),
        local_path: binary.path.to_string_lossy().into_owned(),
        file_hash: file_hash.into(),
        predecessor_outputs: std::collections::BTreeMap::new(),
        supplanted_holder,
        secondary_id_member_gen,
    }
}

/// Every `TaskFailed` reply recorded for `hash`, as `error_message`s.
fn failed_messages_for(
    log: &std::rc::Rc<std::cell::RefCell<Vec<DistributedMessage<test_helpers::TestId>>>>,
    hash: &str,
) -> Vec<String> {
    log.borrow()
        .iter()
        .filter_map(|m| match m {
            DistributedMessage::TaskFailed {
                task_hash,
                error_message,
                ..
            } if task_hash == hash => Some(error_message.clone()),
            _ => None,
        })
        .collect()
}

/// Helper: apply a `PeerJoined` to the secondary's cluster_state mirror
/// so a named peer is recorded `Alive` at the given `member_gen`. This
/// matches the apply-rule the production responder/snapshot path uses;
/// the upward-only observer ratchet and the read-back via
/// `is_peer_alive` / `peer_member_gen` are then live for our assertions.
fn seed_peer_alive(
    secondary: &mut SecondaryCoordinator<
        impl dynrunner_protocol_manager_worker::ManagerEndpoint + 'static,
        impl dynrunner_scheduler_api::Scheduler<test_helpers::TestId> + Clone,
        impl dynrunner_scheduler_api::ResourceEstimator<test_helpers::TestId> + Clone,
        test_helpers::TestId,
    >,
    peer_id: &str,
    member_gen: u64,
) {
    secondary.cluster_state.apply(ClusterMutation::PeerJoined {
        peer_id: peer_id.into(),
        is_observer: false,
        can_be_primary: true,
        cap_version: Default::default(),
        member_gen,
    });
}

/// Fence A FIRES: a task-assignment naming a SUPPLANTED HOLDER that the
/// secondary's cluster_state sees alive again at gen >= supplanted gen
/// must be REFUSED (no worker dispatch), and the reply must carry the
/// `TASK_SUPPLANTED_BY_LIVE_HOLDER_WIRE_MESSAGE` marker.
#[tokio::test(flavor = "current_thread")]
async fn fence_a_fires_when_supplanted_holder_is_alive_at_matching_or_higher_gen() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut secondary, log) = make_secondary_recording(one_worker_config("sec-b"), 1);
            secondary.set_bootstrap_primary_id("setup".to_string());

            let mut factory = FakeWorkerFactory;
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;

            // Peer A is the original holder. The primary recovered the task
            // by recording supplanted_holder=("A", 1), then `PeerRemoved`
            // for A. Then A came back alive — re-admission bumped gen to 2.
            // The new dispatch lands on B and carries the hint.
            seed_peer_alive(&mut secondary, "A", 2);

            let binary = make_binary("supplanted-task", 50);
            let file_hash = "fence-a-fires-hash".to_string();
            let assignment = task_assignment_with_fences(
                "sec-b",
                &binary,
                &file_hash,
                Some(("A".into(), 1)),
                // Pre-existing B gen=0 (never re-admitted). Fence B must
                // PASS so we observe Fence A's verdict in isolation.
                Some(secondary.cluster_state.peer_member_gen("sec-b")),
            );

            secondary
                .handle_inbound(assignment, &mut FakeWorkerFactory)
                .await;
            secondary.drain_egress().await;

            // No worker dispatch happened: active_tasks remains empty.
            assert!(
                secondary.op_mut().active_tasks.is_empty(),
                "fence A must REFUSE to start the dispatch; active_tasks must \
                 stay empty, got {:?}",
                secondary.op_mut().active_tasks
            );
            assert!(
                secondary.op_mut().pending_first_bind.is_empty(),
                "fence A must NOT park the binary for first-bind either; got \
                 {:?}",
                secondary
                    .op_mut()
                    .pending_first_bind
                    .iter()
                    .map(|(w, p)| (*w, p.file_hash.clone()))
                    .collect::<Vec<_>>()
            );

            // The reply carries the fence A marker.
            let replies = failed_messages_for(&log, &file_hash);
            assert_eq!(
                replies,
                vec![TASK_SUPPLANTED_BY_LIVE_HOLDER_WIRE_MESSAGE.to_string()],
                "fence A must reply with exactly one supplanted-by-live-holder \
                 marker; got {replies:?}"
            );
        })
        .await;
}

/// Fence A negative control: the supplanted holder is NOT alive (no peer
/// state recorded), so the dispatch must FALL THROUGH to the normal path
/// (and Fence B must also pass so we're isolating Fence A's behaviour).
#[tokio::test(flavor = "current_thread")]
async fn fence_a_does_not_fire_when_supplanted_holder_is_not_alive() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut secondary, log) = make_secondary_recording(one_worker_config("sec-b"), 1);
            secondary.set_bootstrap_primary_id("setup".to_string());

            let mut factory = FakeWorkerFactory;
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;

            // Note: we deliberately do NOT seed peer A as alive. Its gen
            // remains 0 (never-seen) AND `is_peer_alive("A") == false`.
            // Fence A requires BOTH alive AND live_gen >= supplanted_gen;
            // both predicates fail, so the fence does NOT fire.
            let binary = make_binary("supplanted-task", 50);
            let file_hash = "fence-a-quiet-hash".to_string();
            let assignment = task_assignment_with_fences(
                "sec-b",
                &binary,
                &file_hash,
                Some(("A".into(), 1)),
                Some(secondary.cluster_state.peer_member_gen("sec-b")),
            );

            secondary
                .handle_inbound(assignment, &mut FakeWorkerFactory)
                .await;
            secondary.drain_egress().await;

            // The fence A marker must NOT appear on the wire — dispatch fell
            // through. (We deliberately don't assert the worker-side outcome
            // here, because that path may park the binary in
            // `pending_first_bind` on a fresh slot; the load-bearing assertion
            // is "fence A did not refuse this dispatch".)
            let replies = failed_messages_for(&log, &file_hash);
            assert!(
                !replies.contains(&TASK_SUPPLANTED_BY_LIVE_HOLDER_WIRE_MESSAGE.to_string()),
                "fence A must NOT fire when the supplanted holder is not \
                 alive; got replies {replies:?}"
            );
        })
        .await;
}

/// Fence B FIRES: a task-assignment whose `secondary_id_member_gen` does
/// not match this secondary's current `peer_member_gen` must be REFUSED
/// (no worker dispatch), and the reply must carry the
/// `TASK_STALE_ADDRESSEE_GEN_WIRE_MESSAGE` marker.
#[tokio::test(flavor = "current_thread")]
async fn fence_b_fires_on_stale_addressee_gen() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut secondary, log) = make_secondary_recording(one_worker_config("sec-b"), 1);
            secondary.set_bootstrap_primary_id("setup".to_string());

            let mut factory = FakeWorkerFactory;
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;

            // Drive B's own member_gen up to 2 (a re-removal + re-admission
            // happened — sec-b is the LIVE incarnation at gen 2).
            seed_peer_alive(&mut secondary, "sec-b", 2);
            assert_eq!(
                secondary.cluster_state.peer_member_gen("sec-b"),
                2,
                "fixture precondition: this secondary's recorded gen is 2"
            );

            // The dispatch was minted when the primary knew B at gen 1 —
            // the lease is for the PREVIOUS incarnation.
            let binary = make_binary("stale-lease-task", 50);
            let file_hash = "fence-b-fires-hash".to_string();
            let assignment = task_assignment_with_fences(
                "sec-b",
                &binary,
                &file_hash,
                // No fence-A hint — keep this test focused on fence B.
                None,
                Some(1),
            );

            secondary
                .handle_inbound(assignment, &mut FakeWorkerFactory)
                .await;
            secondary.drain_egress().await;

            assert!(
                secondary.op_mut().active_tasks.is_empty(),
                "fence B must REFUSE to start the dispatch; active_tasks must \
                 stay empty, got {:?}",
                secondary.op_mut().active_tasks
            );
            assert!(
                secondary.op_mut().pending_first_bind.is_empty(),
                "fence B must NOT park the binary for first-bind either; got \
                 {:?}",
                secondary
                    .op_mut()
                    .pending_first_bind
                    .iter()
                    .map(|(w, p)| (*w, p.file_hash.clone()))
                    .collect::<Vec<_>>()
            );

            let replies = failed_messages_for(&log, &file_hash);
            assert_eq!(
                replies,
                vec![TASK_STALE_ADDRESSEE_GEN_WIRE_MESSAGE.to_string()],
                "fence B must reply with exactly one stale-addressee-gen \
                 marker; got {replies:?}"
            );
        })
        .await;
}

/// Fence B negative control: the lease's `secondary_id_member_gen`
/// MATCHES this secondary's current `peer_member_gen`, so the fence must
/// NOT fire (the dispatch falls through to the normal path). The fence
/// A hint is `None`, so the fence-A arm also stays quiet — we observe
/// fence B's pass-through in isolation.
#[tokio::test(flavor = "current_thread")]
async fn fence_b_does_not_fire_on_matching_addressee_gen() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut secondary, log) = make_secondary_recording(one_worker_config("sec-b"), 1);
            secondary.set_bootstrap_primary_id("setup".to_string());

            let mut factory = FakeWorkerFactory;
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;

            // sec-b at gen 2; the lease names gen 2 — the live incarnation.
            seed_peer_alive(&mut secondary, "sec-b", 2);
            let my_gen = secondary.cluster_state.peer_member_gen("sec-b");
            assert_eq!(my_gen, 2);

            let binary = make_binary("matching-lease-task", 50);
            let file_hash = "fence-b-quiet-hash".to_string();
            let assignment =
                task_assignment_with_fences("sec-b", &binary, &file_hash, None, Some(my_gen));

            secondary
                .handle_inbound(assignment, &mut FakeWorkerFactory)
                .await;
            secondary.drain_egress().await;

            let replies = failed_messages_for(&log, &file_hash);
            assert!(
                !replies.contains(&TASK_STALE_ADDRESSEE_GEN_WIRE_MESSAGE.to_string()),
                "fence B must NOT fire on a matching addressee gen; got \
                 replies {replies:?}"
            );
            assert!(
                !replies.contains(&TASK_SUPPLANTED_BY_LIVE_HOLDER_WIRE_MESSAGE.to_string()),
                "fence A must also stay quiet (hint is None); got replies \
                 {replies:?}"
            );
        })
        .await;
}

