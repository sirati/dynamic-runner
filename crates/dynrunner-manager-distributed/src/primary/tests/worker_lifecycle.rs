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

use dynrunner_core::{PhaseId, ResourceAmount, ResourceKind, ResourceMap, TypeId};

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
/// task. Returns the primary AND the live secondary channel ends — the
/// caller MUST hold the ends for the duration of the test so the
/// `TaskAssignment` wire send inside `handle_task_request` /
/// `dispatch_to_idle_workers` reaches a live receiver; dropping them
/// closes `sec-0`'s channel and every assignment send fails into the
/// rollback arm (slot stays idle), which is a transport artefact, not
/// the slot-lifecycle behaviour under test.
#[allow(clippy::type_complexity)]
fn primary_with_pool_and_idle_worker(
    tasks: Vec<TaskInfo<TestId>>,
) -> (
    PrimaryCoordinator<
        ChannelPeerTransport<TestId>,
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
    let (transport, ends) = setup_test(1);
    let mut primary: PrimaryCoordinator<_, _, _, TestId> = PrimaryCoordinator::new(
        PrimaryConfig::default(),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    let phase = PhaseId::from("work");
    let pool = dynrunner_scheduler_api::PendingPool::<TestId>::new([phase.clone()], HashMap::new())
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
    (primary, ends)
}

fn task_request(secondary_id: &str, worker_id: u32) -> DistributedMessage<TestId> {
    DistributedMessage::TaskRequest {
        target: None,
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
        target: None,
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
            // has a second task it could (wrongly) double-assign. The
            // secondary channel ends are held live (`_ends`) so the
            // `TaskAssignment` wire send inside `handle_task_request`
            // succeeds — dropping them would fail the send and trip the
            // rollback arm, leaving the slot idle.
            let (mut primary, _ends) = primary_with_pool_and_idle_worker(vec![x]);

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

            // Drain Y from the pool before X's completion. The terminal
            // now EMITS a `TasksAdded` (no inline dispatch); no
            // worker-management receiver is installed in this test, so
            // the emit is a silent no-op and nothing re-fills the freed
            // slot — keeping THIS assertion focused on the X-terminal
            // freeing the slot, not on the orthogonal (and correct,
            // separately covered) deferred re-dispatch of queued work.
            let _drained = primary.pool_mut().drain_queued();
            assert_eq!(primary.pool().iter().count(), 0, "Y drained out");

            // Now the completion for X lands. X is credited to phase
            // `work` (counter drops to 0 for the now-finished X) and the
            // slot frees to Idle (nothing queued for the kickstart to
            // re-fill it with).
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

/// Live `TaskAssigned` origination: a clean `Pending → InFlight →
/// Completed` cycle driven through the real dispatch path. Dispatching a
/// task originates `ClusterMutation::TaskAssigned` AFTER the successful
/// send, so the CRDT entry transitions `Pending → InFlight` live (not
/// only in the primary-local `in_flight` ledger). The terminal
/// completion then transitions the CRDT `InFlight → Completed` and
/// drains the local in-flight ledger back to 0. Pins that the in-flight
/// ledger is a derived cache of the replicated `TaskState::InFlight`.
#[tokio::test(flavor = "current_thread")]
async fn dispatch_originates_inflight_and_completion_clears_it() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let x = phased("task-x", "work");
            let hash_x = compute_task_hash(&x);
            let (mut primary, _ends) = primary_with_pool_and_idle_worker(vec![x.clone()]);

            // Seed the CRDT ledger so the live `TaskAssigned` origination
            // has a `Pending` entry to transition (the pool seed alone
            // does not populate `cluster_state`).
            primary
                .cluster_state_mut_for_test()
                .apply(ClusterMutation::TaskAdded {
                    hash: hash_x.clone(),
                    task: x,
                });
            assert!(
                matches!(
                    primary.cluster_state_for_test().task_state(&hash_x),
                    Some(crate::cluster_state::TaskState::Pending { .. })
                ),
                "task starts Pending in the CRDT"
            );

            // Dispatch via the real request path → originates TaskAssigned
            // after the successful send → CRDT transitions to InFlight.
            primary
                .handle_task_request(task_request("sec-0", 0))
                .await
                .unwrap();
            assert!(
                primary.slot_holds_hash_for_test("sec-0", 0, &hash_x),
                "the worker holds X"
            );
            assert_eq!(primary.in_flight_len_for_test(), 1, "X in the ledger");
            match primary.cluster_state_for_test().task_state(&hash_x) {
                Some(crate::cluster_state::TaskState::InFlight {
                    secondary, worker, ..
                }) => {
                    assert_eq!(secondary, "sec-0", "InFlight carries the target secondary");
                    assert_eq!(*worker, 0, "InFlight carries the target worker");
                }
                other => panic!("dispatch must originate CRDT InFlight, got {other:?}"),
            }

            // Completion → CRDT InFlight → Completed, local ledger drained.
            primary
                .handle_task_complete(task_complete("sec-0", 0, &hash_x), &mut None)
                .await;
            assert!(
                matches!(
                    primary.cluster_state_for_test().task_state(&hash_x),
                    Some(crate::cluster_state::TaskState::Completed { .. })
                ),
                "completion transitions the CRDT InFlight → Completed"
            );
            assert_eq!(
                primary.in_flight_len_for_test(),
                0,
                "the in-flight ledger returns to 0 once the CRDT entry is terminal"
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
            // Seed BOTH X and Y so phase `work` carries two items and
            // stays Active after X completes — its single-item drain
            // would otherwise mark the phase Done, hiding Y from
            // `view_for_worker` (Active-only) and blocking the
            // reassignment this test depends on.
            let (mut primary, _ends) = primary_with_pool_and_idle_worker(vec![x.clone(), y]);

            // Install the worker-management bus: the completion path
            // EMITS a `TasksAdded` (deferred dispatch) instead of
            // re-filling the freed slot inline. We drain that signal and
            // drive the recheck synchronously below to reach the
            // "worker reassigned to the OTHER task" precondition this
            // stale-terminal test depends on.
            let (wm_tx, mut wm_rx) =
                tokio_mpsc::unbounded_channel::<crate::worker_signal::WorkerMgmtSignal>();
            primary
                .cluster_state_mut_for_test()
                .install_worker_mgmt_sender(wm_tx);

            // Assign X to the worker. With two items queued the
            // scheduler picks one deterministically (size-equal →
            // insertion order → X first); pin the assertion to whichever
            // landed so the reassignment logic below is what's tested.
            primary
                .handle_task_request(task_request("sec-0", 0))
                .await
                .unwrap();
            // Re-derive which task the slot took, so the "complete the
            // held task, get reassigned to the other" flow is order-
            // independent. We want the slot to end holding the OTHER
            // task after completing the first.
            let (first_hash, other_hash) = if primary.slot_holds_hash_for_test("sec-0", 0, &hash_x)
            {
                (hash_x.clone(), hash_y.clone())
            } else {
                (hash_y.clone(), hash_x.clone())
            };

            // The held task completes — slot frees, its ledger entry
            // drains, and the terminal EMITS a `TasksAdded` (deferred
            // dispatch). Draining the batch and running the recheck
            // re-fills the now-idle worker with the still-queued OTHER
            // task (phase `work` stays Active because a second item
            // remains). The slot then holds `other`; `first` is long
            // gone from the ledger. This reproduces the "worker
            // reassigned" state the stale terminal below must NOT
            // disturb.
            primary
                .handle_task_complete(task_complete("sec-0", 0, &first_hash), &mut None)
                .await;
            let batch = crate::worker_signal::drain_worker_signal_batch(
                &mut wm_rx,
                std::time::Duration::from_millis(50),
            )
            .await
            .expect("completion must emit a TasksAdded batch");
            primary.react_to_worker_signal_batch(batch).await;
            assert!(
                primary.slot_holds_hash_for_test("sec-0", 0, &other_hash),
                "worker reassigned to the other task by the deferred recheck"
            );
            // Bind the reassignment-aware aliases the rest of the test
            // reads (`hash_x` plays the stale-terminal role; `hash_y`
            // the now-held role) without renaming downstream asserts.
            let (hash_x, hash_y) = (first_hash, other_hash);
            assert_eq!(primary.in_flight_len_for_test(), 1, "only Y in flight");
            let work_completed_before =
                *primary.phase_completed.get(&PhaseId::from("work")).unwrap();

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
        ChannelPeerTransport<TestId>,
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
    // Configure a generous per-type cap for `default` so the
    // `in_flight_per_type` ledger is actually populated on dispatch —
    // `reserve_type_slot` is a no-op for types absent from
    // `max_concurrent_per_type`. The cap (100) is far above the two
    // tasks this fixture dispatches, so `cap_filter_view` never trims
    // the view; the only observable effect is that the type-slot
    // accounting this test asserts on is exercised.
    let config = PrimaryConfig {
        max_concurrent_per_type: HashMap::from([(TypeId::from("default"), 100)]),
        ..PrimaryConfig::default()
    };
    let mut primary: PrimaryCoordinator<_, _, _, TestId> = PrimaryCoordinator::new(
        config,
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    let phase = PhaseId::from("work");
    let pool = dynrunner_scheduler_api::PendingPool::<TestId>::new([phase.clone()], HashMap::new())
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
                *primary
                    .in_flight_per_type
                    .get(&TypeId::from("default"))
                    .unwrap(),
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
            assert_eq!(recovered.len(), 1, "sec-0's one in-flight task recovered");
            assert!(
                matches!(
                    recovered.first(),
                    Some(ClusterMutation::TaskRequeued { hash, .. }) if hash == &dead_hash
                ),
                "recovery emits a TaskRequeued for the dead secondary's in-flight hash"
            );
            primary.workers.retain(|w| w.secondary_id != "sec-0");
            assert_eq!(primary.workers.len(), 1, "only sec-1's worker remains");
            assert_eq!(
                primary.workers[0].secondary_id, "sec-1",
                "sec-1's worker compacted to index 0"
            );

            // After recovery: survivor task still in flight, dead task
            // requeued (pool in_flight 2 -> 1, type slot 2 -> 1).
            assert_eq!(
                primary.in_flight_len_for_test(),
                1,
                "survivor still in flight"
            );
            assert_eq!(primary.pool().in_flight(&phase), 1);
            assert_eq!(
                *primary
                    .in_flight_per_type
                    .get(&TypeId::from("default"))
                    .unwrap(),
                1
            );

            // Drain the requeued dead task out of the pool before the
            // survivor's completion so the terminal's kickstart re-fill
            // (a now-idle worker legitimately picking up queued work)
            // has nothing to re-assign — keeping the assertion below
            // focused on the stable-identity slot-free, not on the
            // orthogonal re-dispatch.
            let _drained = primary.pool_mut().drain_queued();

            // THE terminal: sec-1's survivor task completes. With a
            // stale positional index this `handle_task_complete` would
            // index `self.workers[1]` (out of bounds → PANIC). Keyed by
            // stable identity it resolves the survivor at its new index
            // 0 and frees cleanly.
            primary
                .handle_task_complete(task_complete("sec-1", 0, &survivor_hash), &mut None)
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
                primary
                    .in_flight_per_type
                    .get(&TypeId::from("default"))
                    .copied()
                    .unwrap_or(0),
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
            let (mut primary, _ends) = primary_with_pool_and_idle_worker(vec![y]);

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
