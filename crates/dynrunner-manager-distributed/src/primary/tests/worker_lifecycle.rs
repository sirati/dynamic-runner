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

/// Build a two-secondary primary (`sec-0`, `sec-1`), each with one
/// idle worker (local id 0) carrying a generous memory budget, and a
/// pool seeded from `tasks` in phase `work`. Returns the primary plus
/// the live secondary endpoints (held so the transport channels stay
/// open across the `handle_task_request` dispatches).
#[allow(clippy::type_complexity)]
fn primary_two_secondaries_with_pool(
    tasks: Vec<TaskInfo<TestId>>,
) -> (
    PrimaryCoordinator<
        ChannelSecondaryTransportEnd<TestId>,
        NoPeers,
        ResourceStealingScheduler,
        FixedEstimator,
        TestId,
    >,
    Vec<(
        String,
        tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
        tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
    )>,
) {
    let (transport, ends) = setup_test(2);
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
    let budget = ResourceMap::from([(ResourceKind::memory(), 1024 * 1024 * 1024u64)]);
    primary.register_idle_worker_for_test("sec-0".into(), 0, budget.clone());
    primary.register_idle_worker_for_test("sec-1".into(), 0, budget);
    (primary, ends)
}

/// Regression: a SURVIVING secondary's in-flight ledger entry must
/// resolve its holding slot by STABLE `(secondary_id, local_worker_id)`
/// identity, never by a cached positional `Vec` index. Two secondaries
/// each hold an in-flight task: `A@sec-0` holds X, `B@sec-1` holds Y,
/// with `B` at Vec index 1. When `sec-0` dies, `self.workers.retain`
/// COMPACTS the Vec — `B` shifts down to index 0 — but no reindex of
/// the ledger happens. A stale positional index for Y would then point
/// at index 1 (now OUT OF BOUNDS → panic) or, on a larger fleet, at a
/// DIFFERENT worker (held-hash mismatch → terminal silently dropped,
/// the ledger/in-flight/type-slot leak class). Keyed by stable
/// identity, Y's terminal resolves correctly post-compaction.
#[tokio::test(flavor = "current_thread")]
async fn survivor_terminal_after_sibling_secondary_death_resolves_by_stable_identity() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let phase = PhaseId::from("work");
            let x = phased("task-x", "work");
            let y = phased("task-y", "work");
            let hash_x = compute_task_hash(&x);
            let hash_y = compute_task_hash(&y);
            let (mut primary, _ends) = primary_two_secondaries_with_pool(vec![x, y]);

            // Dispatch X to sec-0/w0 and Y to sec-1/w0 through the real
            // request path so each pool `in_flight` counter and type
            // slot is bumped exactly as production does. The scheduler
            // picks the highest-priority eligible item per request;
            // the two-task pool drains one per request.
            primary
                .handle_task_request(task_request("sec-0", 0))
                .await
                .unwrap();
            primary
                .handle_task_request(task_request("sec-1", 0))
                .await
                .unwrap();

            // Determine which secondary took which task — the scheduler
            // is free to order them. We need sec-0 to hold X and sec-1
            // to hold Y for the named assertions; if it landed the other
            // way the regression is identical (sec-0 dies, sec-1
            // survives), so re-bind the survivor's hash from the slot.
            let sec1_holds_y = primary.slot_holds_hash_for_test("sec-1", 0, &hash_y);
            let (survivor_hash, dead_hash) = if sec1_holds_y {
                (hash_y.clone(), hash_x.clone())
            } else {
                (hash_x.clone(), hash_y.clone())
            };
            assert!(
                primary.slot_holds_hash_for_test("sec-1", 0, &survivor_hash),
                "sec-1/w0 holds the survivor task"
            );
            assert!(
                primary.slot_holds_hash_for_test("sec-0", 0, &dead_hash),
                "sec-0/w0 holds the to-be-orphaned task"
            );
            assert_eq!(primary.in_flight_len_for_test(), 2, "both tasks in flight");
            assert_eq!(primary.pool().in_flight(&phase), 2);
            assert_eq!(
                *primary.in_flight_per_type.get(&TypeId::from("default")).unwrap(),
                2,
                "both tasks hold a type-slot"
            );
            // sec-1's worker is at Vec index 1 BEFORE the death.
            assert_eq!(primary.workers.len(), 2);
            assert_eq!(primary.workers[1].secondary_id, "sec-1");

            // sec-0 DIES: recover its in-flight task (requeue the dead
            // one, decrement its phase counter, release its type slot,
            // drop its ledger entry) then drop its workers. `retain`
            // COMPACTS the Vec so sec-1's worker shifts from index 1 to
            // index 0 — the exact desync a stored positional index hits.
            let recovered = primary.recover_inflight_for_dead_secondary("sec-0");
            assert_eq!(recovered, 1, "sec-0's one in-flight task recovered");
            primary.workers.retain(|w| w.secondary_id != "sec-0");
            assert_eq!(primary.workers.len(), 1, "only sec-1's worker remains");
            assert_eq!(
                primary.workers[0].secondary_id, "sec-1",
                "sec-1's worker compacted to index 0"
            );

            // After recovery: survivor task still in flight, dead task
            // requeued (pool in_flight 2 -> 1, type slot 2 -> 1).
            assert_eq!(primary.in_flight_len_for_test(), 1, "survivor still in flight");
            assert_eq!(primary.pool().in_flight(&phase), 1);
            assert_eq!(
                *primary.in_flight_per_type.get(&TypeId::from("default")).unwrap(),
                1
            );

            // THE terminal: sec-1's survivor task completes. With a
            // stale positional index this `handle_task_complete` would
            // index `self.workers[1]` (out of bounds → PANIC). Keyed by
            // stable identity it resolves the survivor at its new index
            // 0 and frees cleanly.
            primary
                .handle_task_complete(
                    task_complete("sec-1", 0, &survivor_hash),
                    &mut None,
                )
                .await;

            // No panic reached here. The survivor's slot is freed, its
            // ledger entry drained, the phase in-flight counter and the
            // type slot both released, and the completion credited once.
            assert!(
                primary.slot_is_idle_for_test("sec-1", 0),
                "survivor slot freed to Idle on its terminal"
            );
            assert_eq!(
                primary.in_flight_len_for_test(),
                0,
                "survivor's in-flight ledger entry drained"
            );
            assert_eq!(
                primary.pool().in_flight(&phase),
                0,
                "phase in-flight counter decremented on the survivor terminal"
            );
            assert_eq!(
                primary.in_flight_per_type.get(&TypeId::from("default")).copied().unwrap_or(0),
                0,
                "survivor's type slot released"
            );
            assert_eq!(
                *primary.phase_completed.get(&phase).unwrap(),
                1,
                "exactly one completion credited to phase work"
            );
            assert!(
                primary.completed_tasks.contains(&survivor_hash),
                "survivor recorded completed"
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
