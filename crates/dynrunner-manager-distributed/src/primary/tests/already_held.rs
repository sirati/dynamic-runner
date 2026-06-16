//! Already-held coherence handling — the primary half of the
//! post-failover assign loop (promoted primary P-secondary-1
//! re-assigning in-flight hashes onto a live survivor's busy workers
//! every ~1.5-2s, indefinitely).
//!
//! The production seeding replayed here: the originating primary died
//! between the `TaskAssignment` send and its `TaskAssigned` broadcast
//! landing anywhere, so the promoted primary's inherited CRDT records
//! the surviving secondary's in-flight tasks as `Pending` — hydrate
//! legitimately pools them and reconstructs the holder's slots idle
//! (the no-redo hydrate pins cover the facts-present shape; THIS is the
//! facts-lost shape they cannot cover). The promoted primary then
//! re-dispatches the hash at the live holder.
//!
//! The holder (post-fix; see `secondary/tests/duplicate_assignment.rs`
//! for its half) answers with the `TASK_ALREADY_HELD_WIRE_MESSAGE`
//! coherence report instead of the generic backpressure bounce. The
//! frames delivered below mirror the secondary's emission shape
//! verbatim (same constant, same `TaskFailed` field shape) per the
//! wire-shape mirror discipline.
//!
//! Pinned here:
//! 1. The reply KEEPS the task in flight on the holder — the optimistic
//!    dispatch commit (slot + ledger + replicated `InFlight`) IS the
//!    correct holder record — and a dispatch recheck does not re-assign
//!    it: the loop's primary-side end state is "InFlight, full stop".
//!    No retry budget is consumed, no terminal is recorded, the holder
//!    is not backpressure-penalised; the eventual real `TaskComplete`
//!    settles slot/ledger/accounting through the normal terminal path.
//! 2. The in-round-trip race (a false-dead recovery requeued the hash
//!    between the dispatch and the reply landing): the reply for the
//!    now-untracked hash is a safe no-op — never a terminal failure,
//!    never a reclaim — and the system converges through the existing
//!    machinery: the next recheck re-dispatches, the holder re-answers
//!    already-held, the commit sticks (one extra round trip, no loop).

use super::*;

use crate::primary::wire::compute_task_hash;
use crate::secondary::TASK_ALREADY_HELD_WIRE_MESSAGE;
use dynrunner_core::{PhaseId, ResourceAmount, ResourceKind};

/// One advertised-memory resource amount (bytes), the live welcome shape.
fn mem(bytes: u64) -> Vec<ResourceAmount> {
    vec![ResourceAmount {
        kind: ResourceKind::memory(),
        amount: bytes,
    }]
}

/// The member's `MeshReady` confirmation — without it the proactive
/// dispatch recheck withholds (the #449 mesh-confirmation gate).
fn mesh_ready_from(secondary_id: &str) -> DistributedMessage<TestId> {
    DistributedMessage::MeshReady {
        target: None,
        sender_id: secondary_id.into(),
        timestamp: 0.0,
        secondary_id: secondary_id.into(),
        peer_count: 1,
    }
}

/// QUEUED pool items only (`PendingPool::len` folds in-flight +
/// blocked, so it cannot distinguish "requeued" from "in flight").
fn queued(
    primary: &PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
) -> usize {
    primary.pool().iter().count()
}

/// Drain every `TaskAssignment` queued on a secondary's wire end,
/// returning the assigned `file_hash`es.
fn assigned_hashes(
    rx: &mut tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
) -> Vec<String> {
    let mut hashes = Vec::new();
    while let Ok(msg) = rx.try_recv() {
        if let DistributedMessage::TaskAssignment { file_hash, .. } = msg {
            hashes.push(file_hash);
        }
    }
    hashes
}

/// The already-held coherence report, mirroring the secondary router's
/// emission shape VERBATIM (`secondary/dispatch/router.rs`, the
/// duplicate-assignment arm): a `TaskFailed` frame whose
/// `error_message` is the shared marker constant and whose `worker_id`
/// names the REAL holding worker.
fn already_held_reply(secondary_id: &str, worker_id: u32, hash: &str) -> DistributedMessage<TestId> {
    DistributedMessage::TaskFailed {
        target: None,
        sender_id: secondary_id.into(),
        timestamp: 0.0,
        secondary_id: secondary_id.into(),
        worker_id,
        task_hash: hash.into(),
        error_type: dynrunner_core::ErrorType::Recoverable,
        error_message: TASK_ALREADY_HELD_WIRE_MESSAGE.into(),
        delivery_seq: None,
        msgs_posted_through: None,
    }
}

/// The holder's eventual REAL completion report (the settle leg).
fn task_complete(secondary_id: &str, worker_id: u32, hash: &str) -> DistributedMessage<TestId> {
    DistributedMessage::TaskComplete {
        target: None,
        sender_id: secondary_id.into(),
        timestamp: 0.0,
        secondary_id: secondary_id.into(),
        worker_id,
        task_hash: hash.into(),
        result_data: None,
        delivery_seq: None,
        msgs_posted_through: None,
    }
}

/// Build a promoted-shaped primary whose inherited CRDT lost the
/// `InFlight` fact for ONE task the surviving secondary is actually
/// running: the task is `Pending` in the ledger, so hydrate pools it
/// and reconstructs sec-0's single slot idle — the exact precondition
/// the production loop dispatched from. Returns the primary, the wire
/// ends, and the task hash, with sec-0 mesh-confirmed (assignable).
#[allow(clippy::type_complexity)]
async fn promoted_primary_with_lost_inflight_fact() -> (
    PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
    Vec<(
        String,
        tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
        tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
    )>,
    String,
    PrimaryMeshKeepalive,
) {
    let (transport, ends) = setup_test(1);
    let (mut primary, mesh) = build_test_primary(
        test_primary_config(),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    let task = make_binary("inflight-on-sec-0", 100);
    let hash = compute_task_hash(&task);
    {
        let cs = primary.cluster_state_mut_for_test();
        cs.apply(ClusterMutation::PhaseDepsSet {
            deps: HashMap::from([(PhaseId::from("default"), vec![])]),
        });
        cs.apply(ClusterMutation::PeerJoined {
            peer_id: "sec-0".into(),
            is_observer: false,
            can_be_primary: true,
            cap_version: Default::default(),
            member_gen: 0,
        });
        cs.apply(ClusterMutation::SecondaryCapacity {
            secondary: "sec-0".into(),
            worker_count: 1,
            resources: mem(8 * 1024 * 1024 * 1024),
        });
        // The task is recorded `Pending` ONLY: the dead primary's
        // `TaskAssigned` broadcast never landed (originate-after-send).
        cs.apply(ClusterMutation::TaskAdded {
            hash: hash.clone(),
            task,
            def_id: None,
        });
    }
    primary
        .hydrate_from_cluster_state()
        .expect("test fixture: composed task graph is valid");

    // The trunk-hole precondition, documented: the running task hydrated
    // into the POOL (not the in-flight ledger) and the holder's slot
    // reconstructed IDLE — the replicated ledger alone cannot know
    // better, only the holder's answer can.
    assert_eq!(queued(&primary), 1, "the lost-fact task pools as Pending");
    assert!(
        primary.in_flight.is_empty(),
        "no InFlight fact survived, so the ledger is empty"
    );
    assert_eq!(
        primary.active_workers_for_test(),
        0,
        "the holder's slot reconstructs idle (no occupancy fact)"
    );

    // sec-0 confirms its mesh leg, so the proactive recheck may dispatch.
    primary.handle_mesh_ready(mesh_ready_from("sec-0"));

    (primary, ends, hash, mesh)
}

/// (1) The production replay: dispatch the lost-fact hash at the live
/// holder, deliver its already-held answer, and pin the end state —
/// the task is IN FLIGHT on the holder, full stop. No requeue, no
/// retry-budget burn, no terminal, no re-dispatch on the next recheck;
/// the eventual real completion settles it.
#[tokio::test(flavor = "current_thread")]
async fn already_held_reply_keeps_task_in_flight_and_completion_settles_it() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut primary, mut ends, hash, _mesh) =
                promoted_primary_with_lost_inflight_fact().await;

            // The promoted primary re-dispatches the hash at the holder
            // (it cannot know better — the fact was lost).
            primary
                .dispatch_to_idle_workers(true)
                .await
                .expect("dispatch recheck");
            settle_pump().await;
            assert_eq!(
                assigned_hashes(&mut ends[0].1),
                vec![hash.clone()],
                "the lost-fact hash is re-dispatched at the holder"
            );
            assert!(
                primary.in_flight.contains_key(&hash),
                "the dispatch commit records the hash in flight"
            );

            // The holder's answer: already running it (worker 0).
            primary
                .handle_task_failed(already_held_reply("sec-0", 0, &hash), &mut None)
                .await;

            // End state: InFlight on the holder, FULL STOP.
            assert!(
                primary.in_flight.contains_key(&hash),
                "the already-held reply must KEEP the task in the in-flight \
                 ledger (the optimistic commit IS the holder record); \
                 requeueing it re-arms the assign/bounce loop"
            );
            assert_eq!(
                queued(&primary),
                0,
                "the already-held reply must not requeue the task into the pool"
            );
            assert!(
                !primary.failed_tasks.contains_key(&hash),
                "the already-held reply is a coherence report, not a terminal: \
                 no retry budget may be consumed"
            );
            assert_eq!(
                primary.active_workers_for_test(),
                1,
                "the holder's slot stays Assigned (busy) until the real terminal"
            );
            assert!(
                matches!(
                    primary.cluster_state_for_test().task_state(&hash),
                    Some(crate::cluster_state::TaskState::InFlight { .. })
                ),
                "the replicated state stays InFlight (no TaskFailed / \
                 TaskRequeued origination); got {:?}",
                primary.cluster_state_for_test().task_state(&hash)
            );
            assert!(
                primary.backpressured_secondaries.is_empty(),
                "an already-held answer is not capacity backpressure; the \
                 holder must not be dispatch-penalised"
            );

            // The loop observable: another recheck dispatches NOTHING for
            // this hash (it is in flight, not pending).
            primary
                .dispatch_to_idle_workers(true)
                .await
                .expect("dispatch recheck");
            settle_pump().await;
            assert_eq!(
                assigned_hashes(&mut ends[0].1),
                Vec::<String>::new(),
                "no assign/bounce cycle: the in-flight hash is never \
                 re-dispatched"
            );

            // The holder finishes for real: the normal terminal path
            // settles slot, ledger, and accounting.
            primary
                .handle_task_complete(task_complete("sec-0", 0, &hash), &mut None)
                .await;
            assert!(
                primary.completed_tasks.contains(&hash),
                "the real completion settles the task"
            );
            assert!(
                primary.in_flight.is_empty(),
                "the terminal frees the in-flight ledger entry"
            );
            assert_eq!(
                primary.active_workers_for_test(),
                0,
                "the terminal frees the holder's slot"
            );
        })
        .await;
}

/// (2) The in-round-trip race: a false-dead recovery requeues the hash
/// (ledger entry dropped, CRDT `InFlight → Pending`, pool re-queued)
/// while the holder's already-held answer to the EARLIER dispatch is
/// still in flight. The late reply must be a SAFE NO-OP — never a
/// terminal failure (pre-fix it fell through to the terminal arm:
/// retry budget burned + a false `TaskFailed` broadcast + the queued
/// copy reclaimed as failed) — and the system converges through the
/// existing machinery: the next recheck re-dispatches, the holder
/// re-answers, the commit sticks.
#[tokio::test(flavor = "current_thread")]
async fn late_already_held_reply_for_requeued_hash_is_a_noop_and_converges() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut primary, mut ends, hash, _mesh) =
                promoted_primary_with_lost_inflight_fact().await;

            // Dispatch #1 commits the hash onto the holder.
            primary
                .dispatch_to_idle_workers(true)
                .await
                .expect("dispatch recheck");
            settle_pump().await;
            assert_eq!(assigned_hashes(&mut ends[0].1), vec![hash.clone()]);

            // The false-dead recovery fires while the holder's answer is
            // in flight: ledger entry dropped + `TaskRequeued` broadcast +
            // pool re-queued; the roster rebuild re-derives the slot idle
            // (the CRDT now says Pending).
            let requeues = primary.recover_inflight_for_dead_secondary("sec-0");
            assert_eq!(requeues.len(), 1, "the in-flight hash is requeued");
            primary.apply_and_broadcast_cluster_mutations(requeues).await;
            primary.reconstruct_workers_from_cluster_state();
            assert_eq!(queued(&primary), 1);
            assert!(primary.in_flight.is_empty());

            // The LATE already-held answer (to dispatch #1) lands on the
            // now-untracked hash: a safe no-op.
            primary
                .handle_task_failed(already_held_reply("sec-0", 0, &hash), &mut None)
                .await;
            assert_eq!(
                queued(&primary),
                1,
                "the late reply must leave the queued copy in the pool \
                 (the next recheck re-dispatches it and the holder \
                 re-answers — the convergence path)"
            );
            assert!(
                !primary.failed_tasks.contains_key(&hash),
                "the late reply must never be accounted as a terminal failure"
            );
            assert!(
                !matches!(
                    primary.cluster_state_for_test().task_state(&hash),
                    Some(crate::cluster_state::TaskState::Failed { .. })
                ),
                "no false TaskFailed may be originated for the running task; \
                 got {:?}",
                primary.cluster_state_for_test().task_state(&hash)
            );

            // Convergence: recheck → re-dispatch → the holder re-answers
            // already-held → the commit sticks → the completion settles.
            primary
                .dispatch_to_idle_workers(true)
                .await
                .expect("dispatch recheck");
            settle_pump().await;
            assert_eq!(
                assigned_hashes(&mut ends[0].1),
                vec![hash.clone()],
                "the queued copy is re-dispatched on the next recheck"
            );
            primary
                .handle_task_failed(already_held_reply("sec-0", 0, &hash), &mut None)
                .await;
            assert!(
                primary.in_flight.contains_key(&hash),
                "the re-answer leaves the re-commit in place (case 1)"
            );
            assert_eq!(queued(&primary), 0);

            primary
                .handle_task_complete(task_complete("sec-0", 0, &hash), &mut None)
                .await;
            assert!(primary.completed_tasks.contains(&hash));
            assert!(primary.in_flight.is_empty());
        })
        .await;
}
