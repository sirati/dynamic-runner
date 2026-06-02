//! Tests for the P1 worker-slot lifecycle invariant: a worker, once
//! assigned, cannot be assigned again until it reaches a TERMINAL state
//! for its current task, and a stale/reordered terminal can never free
//! a reassigned slot.
//!
//! All deterministic: handlers are driven directly with synthetic wire
//! messages and asserted via synchronous predicate inspectors
//! (`slot_holds_hash_for_test`, `slot_is_idle_for_test`,
//! `in_flight_len_for_test`, `pool().in_flight(..)`). No real run loop,
//! no wall-clock `timeout` races — the failure modes are reachable by
//! constructing the exact message order, not by waiting.

use super::*;

use dynrunner_core::{PhaseId, ResourceMap, ResourceAmount, ResourceKind, TypeId};

use crate::primary::wire::compute_task_hash;

/// Build a `TaskInfo` in an explicit phase. `task_id == identifier ==
/// name`; the wire hash is `compute_task_hash` over the content.
fn phased(name: &str, phase: &str) -> TaskInfo<TestId> {
    let mut t = make_binary(name, 100);
    t.phase_id = PhaseId::from(phase);
    t.type_id = TypeId::from("default");
    t
}

/// One-secondary primary with a pool seeded from `tasks` (phase
/// `work`) and one idle worker (`sec-0`, local worker 0) carrying a
/// generous memory budget so the scheduler always fits a 100-byte
/// task. Returns the primary ready to receive handler calls.
fn primary_with_pool_and_idle_worker(
    tasks: Vec<TaskInfo<TestId>>,
) -> PrimaryCoordinator<
    ChannelSecondaryTransportEnd<TestId>,
    NoPeers,
    ResourceStealingScheduler,
    FixedEstimator,
    TestId,
> {
    let (transport, _ends) = setup_test(1);
    let mut primary: PrimaryCoordinator<_, _, _, _, TestId> = PrimaryCoordinator::new(
        PrimaryConfig::default(),
        transport,
        NoPeers,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    let phase = PhaseId::from("work");
    let pool = dynrunner_scheduler_api::PendingPool::<TestId>::new(
        [phase.clone()],
        HashMap::new(),
    )
    .expect("work-phase pool");
    primary.pending = Some(pool);
    primary.phase_completed.insert(phase.clone(), 0);
    primary.phase_failed.insert(phase, 0);
    primary.pool_mut().extend(tasks).expect("valid extend");
    primary.register_idle_worker_for_test(
        "sec-0".into(),
        0,
        ResourceMap::from([(ResourceKind::memory(), 1024 * 1024 * 1024u64)]),
    );
    primary
}

fn task_request(secondary_id: &str, worker_id: u32) -> DistributedMessage<TestId> {
    DistributedMessage::TaskRequest {
        sender_id: secondary_id.into(),
        timestamp: 0.0,
        secondary_id: secondary_id.into(),
        worker_id,
        available_resources: vec![ResourceAmount {
            kind: ResourceKind::memory(),
            amount: 1024 * 1024 * 1024u64,
        }],
    }
}

fn task_complete(secondary_id: &str, worker_id: u32, hash: &str) -> DistributedMessage<TestId> {
    DistributedMessage::TaskComplete {
        sender_id: secondary_id.into(),
        timestamp: 0.0,
        secondary_id: secondary_id.into(),
        worker_id,
        task_hash: hash.into(),
        result_data: None,
    }
}

/// (a) Reordered `TaskRequest(W)` then `TaskComplete(X)` while W holds
/// X: the bare request must NOT free or reassign the slot (it is a
/// pure capacity hint), and the later completion must credit X to its
/// phase exactly once. The pool carries a SECOND task Y so the test is
/// load-bearing: without the `Idle`-only assignment gate, the bare
/// request would assign Y to a worker already holding X — the
/// reassignment-before-terminal bug P1 makes impossible.
#[tokio::test(flavor = "current_thread")]
async fn reordered_request_then_complete_credits_correct_phase_no_double_assign() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let x = phased("task-x", "work");
            let y = phased("task-y", "work");
            let hash_x = compute_task_hash(&x);
            // Seed ONLY X first so the initial assignment deterministically
            // takes X; inject Y afterwards so the reordered bare request
            // has a second task it could (wrongly) double-assign.
            let mut primary = primary_with_pool_and_idle_worker(vec![x]);

            // Initial request assigns X to (sec-0, w0).
            primary
                .handle_task_request(task_request("sec-0", 0))
                .await
                .unwrap();
            assert!(
                primary.slot_holds_hash_for_test("sec-0", 0, &hash_x),
                "first request must assign X to the worker"
            );
            assert_eq!(primary.in_flight_len_for_test(), 1, "X tracked in-flight");
            assert_eq!(primary.pool().in_flight(&PhaseId::from("work")), 1);

            // Now Y enters the pool — a candidate the held worker must
            // NOT be allowed to pick up while still running X.
            primary.pool_mut().extend(vec![y]).expect("valid extend");
            assert_eq!(primary.pool().iter().count(), 1, "Y queued");

            // REORDER: a bare TaskRequest for the SAME worker lands
            // while it still holds X (the wire delivered the request
            // ahead of the completion). It MUST be a no-op on the slot
            // — never freeing X, never assigning Y on top of it.
            primary
                .handle_task_request(task_request("sec-0", 0))
                .await
                .unwrap();
            assert!(
                primary.slot_holds_hash_for_test("sec-0", 0, &hash_x),
                "bare request must NOT free or reassign the slot still holding X"
            );
            assert_eq!(
                primary.in_flight_len_for_test(),
                1,
                "no second in-flight entry from a bare capacity-hint request"
            );
            assert_eq!(
                primary.pool().iter().count(),
                1,
                "Y must stay queued — the held worker cannot take a second task"
            );

            // Now the completion for X lands. X is credited to phase
            // `work` (counter drops to 0 for the now-finished X — Y is
            // queued, not in-flight) and the slot frees to Idle.
            primary
                .handle_task_complete(task_complete("sec-0", 0, &hash_x), &mut None)
                .await;
            assert!(
                primary.completed_tasks.contains(&hash_x),
                "X recorded completed"
            );
            assert!(
                primary.slot_is_idle_for_test("sec-0", 0),
                "slot frees to Idle on X's terminal"
            );
            assert_eq!(
                primary.in_flight_len_for_test(),
                0,
                "X's in-flight ledger entry drained on terminal"
            );
            assert_eq!(
                *primary.phase_completed.get(&PhaseId::from("work")).unwrap(),
                1,
                "exactly one completion credited to phase work"
            );
        })
        .await;
}

/// (b) `TaskComplete(X)` arriving AFTER the worker was reassigned to a
/// later task Y: the stale terminal must be a no-op on the slot (Y
/// stays Assigned), and X's in-flight entry — already gone when the
/// slot moved to Y — yields no double-decrement. The hash IS the held
/// identity, so a terminal for a non-held hash cannot free the slot.
#[tokio::test(flavor = "current_thread")]
async fn stale_complete_after_reassignment_is_noop_on_slot() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let x = phased("task-x", "work");
            let y = phased("task-y", "work");
            let hash_x = compute_task_hash(&x);
            let hash_y = compute_task_hash(&y);
            // Seed only X; Y is injected after X completes so each
            // assignment is deterministic.
            let mut primary = primary_with_pool_and_idle_worker(vec![x]);

            // Assign X to the worker.
            primary
                .handle_task_request(task_request("sec-0", 0))
                .await
                .unwrap();
            assert!(primary.slot_holds_hash_for_test("sec-0", 0, &hash_x));

            // X completes — slot frees, X drained from the ledger.
            primary
                .handle_task_complete(task_complete("sec-0", 0, &hash_x), &mut None)
                .await;
            assert!(primary.slot_is_idle_for_test("sec-0", 0));

            // Y enters the pool; the worker requests again → reassigned
            // to Y. The slot now holds Y; X is long gone from the ledger.
            primary.pool_mut().extend(vec![y]).expect("valid extend");
            primary
                .handle_task_request(task_request("sec-0", 0))
                .await
                .unwrap();
            assert!(
                primary.slot_holds_hash_for_test("sec-0", 0, &hash_y),
                "worker reassigned to Y"
            );
            assert_eq!(primary.in_flight_len_for_test(), 1, "only Y in flight");
            let work_completed_before = *primary
                .phase_completed
                .get(&PhaseId::from("work"))
                .unwrap();

            // A STALE, duplicate TaskComplete for X lands now (a
            // delayed/redundant wire copy). The completed-dedup gate
            // catches the duplicate hash, but even bypassing that, the
            // slot holds Y (hash_y != hash_x): `free_slot_on_terminal`
            // resolves the ledger — X's entry is absent — and is a
            // no-op. Y must stay Assigned; no second phase decrement.
            primary
                .handle_task_complete(task_complete("sec-0", 0, &hash_x), &mut None)
                .await;
            assert!(
                primary.slot_holds_hash_for_test("sec-0", 0, &hash_y),
                "stale X-completion must NOT free the slot now holding Y"
            );
            assert_eq!(
                primary.in_flight_len_for_test(),
                1,
                "Y's in-flight entry untouched by the stale X terminal"
            );
            assert_eq!(
                *primary.phase_completed.get(&PhaseId::from("work")).unwrap(),
                work_completed_before,
                "no double-decrement / double-credit from the stale X terminal"
            );
        })
        .await;
}

/// (b-variant) A completion whose hash does NOT match the held slot —
/// distinct from the dedup case — must be a pure no-op. Drives the
/// `free_slot_on_terminal` "non-held hash" arm directly: the slot
/// holds Y, a TaskComplete names a hash never tracked. Nothing frees.
#[tokio::test(flavor = "current_thread")]
async fn complete_for_untracked_hash_is_noop() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let y = phased("task-y", "work");
            let hash_y = compute_task_hash(&y);
            let mut primary = primary_with_pool_and_idle_worker(vec![y]);

            primary
                .handle_task_request(task_request("sec-0", 0))
                .await
                .unwrap();
            assert!(primary.slot_holds_hash_for_test("sec-0", 0, &hash_y));

            // Completion for a hash that was never dispatched/tracked.
            primary
                .handle_task_complete(
                    task_complete("sec-0", 0, "ghost-hash-never-dispatched"),
                    &mut None,
                )
                .await;
            assert!(
                primary.slot_holds_hash_for_test("sec-0", 0, &hash_y),
                "untracked-hash completion must leave Y's slot untouched"
            );
            assert_eq!(
                primary.in_flight_len_for_test(),
                1,
                "untracked-hash completion must not touch the ledger"
            );
        })
        .await;
}
