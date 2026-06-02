//! Dispatch-decoupling coverage: phase/task management is fully
//! decoupled from worker management. Dispatch is a PARKED RECHECK woken
//! by a `WorkerMgmtSignal::TasksAdded`, NOT a direct phase→worker call.
//!
//! All deterministic — no operational loop raced against a wall-clock
//! `timeout`. The signal bus is driven synchronously: emit a signal,
//! drain the coalesced batch via `drain_worker_signal_batch`, and run
//! `react_to_worker_signal_batch` directly (exactly what the operational
//! loop's worker-management `select!` arm does, minus the 50ms idle
//! window). Failure modes are reached by constructing the exact message
//! order + signal sequence, never by waiting.
//!
//! Four invariants:
//!   1. POSITIVE — phase A (1 task) → phase B (1 task, dep A) with one
//!      worker: after A completes the emitted `TasksAdded` drives a
//!      recheck that dispatches B.
//!   2. NEGATIVE CONTROL — with the signal-driven recheck NOT run
//!      (the `TasksAdded` suppressed), B never dispatches. Proves the
//!      signal is load-bearing: dispatch does NOT happen inline on the
//!      completion path anymore.
//!   3. is_idle → advisory — dispatch SELECTS on the authoritative free
//!      predicate (`held_task().is_none()`), never on the advisory
//!      `is_idle` name; an Assigned slot is never a candidate, a free
//!      one always is.
//!   4. COALESCE — N `TasksAdded` in one window collapse into ONE
//!      recheck.

use super::*;

use dynrunner_core::{PhaseId, ResourceAmount, ResourceKind, ResourceMap, TaskDep, TypeId};

use crate::primary::lifecycle::dispatch_order;
use crate::primary::wire::compute_task_hash;
use crate::worker_signal::{
    drain_worker_signal_batch, WorkerMgmtSignal, WorkerSignalBatch,
};

type TestPrimary = PrimaryCoordinator<
    ChannelPeerTransport<TestId>,
    ResourceStealingScheduler,
    FixedEstimator,
    TestId,
>;

/// Build a `TaskInfo` with an explicit phase + task-level dep list.
fn dep_binary(name: &str, phase: &str, depends_on: &[&str]) -> TaskInfo<TestId> {
    let mut t = make_binary(name, 100);
    t.phase_id = PhaseId::from(phase);
    t.type_id = TypeId::from("default");
    t.task_depends_on = depends_on
        .iter()
        .map(|d| TaskDep {
            task_id: (*d).to_string(),
            inherit_outputs: false,
        })
        .collect();
    t
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

/// A single-secondary primary whose CRDT carries `a` (phase "a",
/// zero-dep) and `b` (phase "b", task-dep on `a`), hydrated into a pool
/// with one idle worker on `sec-0`. Phase "b" depends on "a" at the
/// phase level too, so "b" hydrates Blocked. Returns the primary plus
/// the live channel ends (held so wire sends succeed) and the two task
/// hashes.
#[allow(clippy::type_complexity)]
fn primary_two_phase_one_worker() -> (
    TestPrimary,
    Vec<(
        String,
        tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
        tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
    )>,
    String,
    String,
) {
    let (transport, ends) = setup_test(1);
    let mut primary: TestPrimary = PrimaryCoordinator::new(
        PrimaryConfig::default(),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );

    let a = dep_binary("a", "a", &[]);
    let b = dep_binary("b", "b", &["a"]);
    let hash_a = compute_task_hash(&a);
    let hash_b = compute_task_hash(&b);
    {
        let cs = primary.cluster_state_mut_for_test();
        cs.apply(ClusterMutation::PhaseDepsSet {
            deps: HashMap::from([(PhaseId::from("b"), vec![PhaseId::from("a")])]),
        });
        cs.apply(ClusterMutation::TaskAdded {
            hash: hash_a.clone(),
            task: a,
        });
        cs.apply(ClusterMutation::TaskAdded {
            hash: hash_b.clone(),
            task: b,
        });
    }
    primary.hydrate_from_cluster_state();
    primary.register_idle_worker_for_test(
        "sec-0".into(),
        0,
        ResourceMap::from([(ResourceKind::memory(), 1024 * 1024 * 1024u64)]),
    );
    (primary, ends, hash_a, hash_b)
}

/// Drain every `TaskAssignment` task_id on `rx` (non-blocking).
fn assigned_task_ids(
    rx: &mut tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
) -> Vec<String> {
    let mut ids = Vec::new();
    while let Ok(msg) = rx.try_recv() {
        if let DistributedMessage::TaskAssignment { binary_info, .. } = msg {
            ids.push(binary_info.task_id);
        }
    }
    ids
}

/// (1) POSITIVE. One worker drains phase A, then phase B (dep A) must
/// dispatch — but ONLY via the recheck woken by the `TasksAdded` the
/// completion path emitted. Drive the bus end-to-end: complete A, drain
/// the coalesced batch, run the reaction, observe B on the wire.
#[tokio::test(flavor = "current_thread")]
async fn tasks_added_recheck_dispatches_dependent_phase_after_predecessor_completes() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut primary, mut ends, hash_a, _hash_b) = primary_two_phase_one_worker();

            // Worker requests work; only phase A is Active (B is Blocked
            // on A), so it takes A.
            primary
                .handle_task_request(task_request("sec-0", 0))
                .await
                .unwrap();
            assert!(
                primary.slot_holds_hash_for_test("sec-0", 0, &hash_a),
                "worker must take phase-A task first (B is blocked on A)"
            );
            // Drain the A assignment off the wire so the post-completion
            // assertion sees only B.
            let _ = assigned_task_ids(&mut ends[0].1);

            // Install the worker-management bus to capture the emit.
            let (wm_tx, mut wm_rx) =
                tokio_mpsc::unbounded_channel::<WorkerMgmtSignal>();
            primary
                .cluster_state_mut_for_test()
                .install_worker_mgmt_sender(wm_tx);

            // A completes. The completion path EMITS `TasksAdded` (the
            // dep-resume of B + the freed worker), NOT an inline
            // dispatch. The slot frees; nothing dispatched yet.
            primary
                .handle_task_complete(task_complete("sec-0", 0, &hash_a), &mut None)
                .await;
            assert!(
                primary.slot_is_idle_for_test("sec-0", 0),
                "worker frees on A's terminal"
            );
            assert!(
                assigned_task_ids(&mut ends[0].1).is_empty(),
                "no inline dispatch on the completion path — dispatch is deferred \
                 to the signal-driven recheck (decoupling law)"
            );

            // Drive the parked recheck exactly as the operational loop
            // would: drain the coalesced batch, run the reaction.
            let batch = drain_worker_signal_batch(&mut wm_rx, Duration::from_millis(50))
                .await
                .expect("completion must emit a TasksAdded batch");
            assert!(
                batch.signals.contains(&WorkerMgmtSignal::TasksAdded),
                "the completion emit must carry TasksAdded; got {:?}",
                batch.signals
            );
            primary.react_to_worker_signal_batch(batch).await;

            // The recheck dispatched B to the now-free worker.
            let assigned = assigned_task_ids(&mut ends[0].1);
            assert_eq!(
                assigned,
                vec!["b".to_string()],
                "the TasksAdded recheck must dispatch phase-B task to the freed worker"
            );
        })
        .await;
}

/// (2) NEGATIVE CONTROL. Same setup, but the signal-driven recheck is
/// NOT run (the `TasksAdded` is suppressed — modelling a dropped
/// signal). B must NEVER dispatch: there is no inline dispatch on the
/// completion path anymore, so without the signal the freed worker sits
/// idle and B stays queued. Asserted over a bounded virtual-time window
/// (`start_paused` + `advance`), NOT a real wall-clock hang.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn negative_control_suppressed_tasks_added_never_dispatches_dependent() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut primary, mut ends, hash_a, _hash_b) = primary_two_phase_one_worker();

            primary
                .handle_task_request(task_request("sec-0", 0))
                .await
                .unwrap();
            assert!(primary.slot_holds_hash_for_test("sec-0", 0, &hash_a));
            let _ = assigned_task_ids(&mut ends[0].1);

            // NO worker-management sender installed: the completion
            // path's `emit_worker_mgmt(TasksAdded)` is a silent no-op
            // (the bus has no receiver). This is the SUPPRESSED-signal
            // condition the control isolates.
            primary
                .handle_task_complete(task_complete("sec-0", 0, &hash_a), &mut None)
                .await;
            assert!(
                primary.slot_is_idle_for_test("sec-0", 0),
                "worker freed on completion"
            );

            // Advance virtual time well past any plausible recheck
            // window. With the signal suppressed and no inline dispatch,
            // nothing wakes the recheck — B can never dispatch. This is
            // the load-bearing control: a regression that re-introduced
            // a direct phase→worker dispatch call would dispatch B here
            // and fail the assertion.
            tokio::time::advance(Duration::from_secs(3600)).await;
            tokio::task::yield_now().await;

            assert!(
                assigned_task_ids(&mut ends[0].1).is_empty(),
                "with the TasksAdded signal suppressed, B must NEVER dispatch — \
                 proves the signal is load-bearing and dispatch is fully decoupled"
            );
            assert!(
                primary.slot_is_idle_for_test("sec-0", 0),
                "the worker must stay idle: nothing woke the parked recheck"
            );
        })
        .await;
}

/// (3) is_idle → ADVISORY. `dispatch_order` selects dispatch candidates
/// on the authoritative free predicate (`held_task().is_none()`), never
/// on the advisory `is_idle` name. An Assigned slot is excluded; a free
/// slot is included AND the recheck dispatches to it.
///
/// In R1's `SlotState` typestate `is_idle()` and `held_task().is_none()`
/// coincide by construction (both read `self.state`), so a literal
/// "is_idle desync" is unconstructible — the divergence class P1
/// eliminated. This test pins the equivalent live contract: selection
/// authority is the held-task predicate, and a freed worker is a valid
/// recheck target regardless of any advisory bookkeeping.
#[tokio::test(flavor = "current_thread")]
async fn dispatch_selects_on_authoritative_free_predicate_not_advisory_is_idle() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // One secondary, two workers; seed two same-phase tasks so
            // there is work for the free worker to take.
            let (transport, mut ends) = setup_test(1);
            let mut primary: TestPrimary = PrimaryCoordinator::new(
                PrimaryConfig::default(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            let t0 = dep_binary("t0", "work", &[]);
            let t1 = dep_binary("t1", "work", &[]);
            let hash_t0 = compute_task_hash(&t0);
            let hash_t1 = compute_task_hash(&t1);
            {
                let cs = primary.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::TaskAdded {
                    hash: hash_t0.clone(),
                    task: t0,
                });
                cs.apply(ClusterMutation::TaskAdded {
                    hash: hash_t1.clone(),
                    task: t1,
                });
            }
            primary.hydrate_from_cluster_state();
            let budget = ResourceMap::from([(ResourceKind::memory(), 1024 * 1024 * 1024u64)]);
            primary.register_idle_worker_for_test("sec-0".into(), 0, budget.clone());
            primary.register_idle_worker_for_test("sec-0".into(), 1, budget);

            // Assign worker 0 (it becomes Assigned / held). The
            // scheduler picks one of the two same-phase tasks; record
            // which so the remaining-task assertion is order-independent.
            primary
                .handle_task_request(task_request("sec-0", 0))
                .await
                .unwrap();
            let (held_first, expect_second) =
                if primary.slot_holds_hash_for_test("sec-0", 0, &hash_t0) {
                    ("t0", "t1".to_string())
                } else {
                    assert!(
                        primary.slot_holds_hash_for_test("sec-0", 0, &hash_t1),
                        "worker 0 must hold one of the two queued tasks"
                    );
                    ("t1", "t0".to_string())
                };
            let _ = held_first;
            let _ = assigned_task_ids(&mut ends[0].1);

            // Selection authority: `dispatch_order` returns ONLY the
            // free worker (held_task().is_none()), never the Assigned
            // one — proving the predicate, not the advisory name, gates
            // candidacy. Worker 0 holds t0 (index 0); worker 1 is free
            // (index 1).
            let order = dispatch_order(&primary.workers);
            assert_eq!(
                order,
                vec![1],
                "dispatch_order must select only the worker with held_task().is_none()"
            );

            // A TasksAdded recheck dispatches the remaining task to the
            // free worker (index 1, local worker_id 1).
            let (wm_tx, mut wm_rx) =
                tokio_mpsc::unbounded_channel::<WorkerMgmtSignal>();
            primary
                .cluster_state_mut_for_test()
                .install_worker_mgmt_sender(wm_tx);
            primary
                .cluster_state_mut_for_test()
                .emit_worker_mgmt(WorkerMgmtSignal::TasksAdded);
            let batch = drain_worker_signal_batch(&mut wm_rx, Duration::from_millis(50))
                .await
                .expect("emit must produce a batch");
            primary.react_to_worker_signal_batch(batch).await;

            assert_eq!(
                assigned_task_ids(&mut ends[0].1),
                vec![expect_second],
                "the recheck must dispatch the remaining task to the free worker \
                 selected by the authoritative predicate"
            );
        })
        .await;
}

/// (4) COALESCE. Several `TasksAdded` arriving inside one idle window
/// collapse into ONE batch carrying every signal — the reaction runs the
/// dispatch recheck exactly once per batch (the recheck is idempotent
/// over the pool/worker view). Pins the burst-coalescing contract at the
/// worker-management reaction boundary.
#[tokio::test(flavor = "current_thread")]
async fn coalesce_multiple_tasks_added_into_one_recheck() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut primary, mut ends, hash_a, _hash_b) = primary_two_phase_one_worker();
            primary
                .handle_task_request(task_request("sec-0", 0))
                .await
                .unwrap();
            let _ = assigned_task_ids(&mut ends[0].1);

            let (wm_tx, mut wm_rx) =
                tokio_mpsc::unbounded_channel::<WorkerMgmtSignal>();
            primary
                .cluster_state_mut_for_test()
                .install_worker_mgmt_sender(wm_tx);

            // Complete A AND fire two extra explicit TasksAdded in the
            // same window: a burst the idle window must coalesce.
            primary
                .handle_task_complete(task_complete("sec-0", 0, &hash_a), &mut None)
                .await;
            primary
                .cluster_state_mut_for_test()
                .emit_worker_mgmt(WorkerMgmtSignal::TasksAdded);
            primary
                .cluster_state_mut_for_test()
                .emit_worker_mgmt(WorkerMgmtSignal::TasksAdded);

            // The whole burst drains as ONE batch in arrival order, none
            // dropped. Completing A also activates phase B, so the
            // completion path additionally emits one
            // `PhaseStartedNeedsWorkers { phase: "b" }` ahead of the
            // TasksAdded burst — all of it coalesces into this single
            // batch (the coalescing proof: the three TasksAdded did NOT
            // each produce their own batch).
            let batch: WorkerSignalBatch =
                drain_worker_signal_batch(&mut wm_rx, Duration::from_millis(50))
                    .await
                    .expect("burst must produce one batch");
            let tasks_added = batch
                .signals
                .iter()
                .filter(|s| **s == WorkerMgmtSignal::TasksAdded)
                .count();
            assert!(
                tasks_added >= 3,
                "the three TasksAdded (completion + two explicit) must coalesce \
                 into ONE batch, not three; got {:?}",
                batch.signals
            );

            // One reaction over the whole batch dispatches B exactly
            // once — the reaction folds every TasksAdded in the batch
            // into a single recheck (idempotent over the pool/worker
            // view), so the coalesced multi-signal batch produces a
            // single dispatch, not three.
            primary.react_to_worker_signal_batch(batch).await;
            assert_eq!(
                assigned_task_ids(&mut ends[0].1),
                vec!["b".to_string()],
                "one recheck per coalesced batch dispatches B exactly once"
            );
        })
        .await;
}
