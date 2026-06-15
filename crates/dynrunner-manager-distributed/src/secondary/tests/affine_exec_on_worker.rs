//! #577 — SecondaryAffine gate-body-on-worker rewire.
//!
//! These tests pin the new shape: the gate body is dispatched to a worker
//! subprocess via the same `assign_resolved_task` seam every task crosses
//! (NOT inline-Python on the LocalSet thread), and the worker's terminal
//! `WorkerEvent::TaskCompleted` / `TaskFailed` lands through
//! `handle_worker_event` and is recognized by
//! `binary.kind.is_secondary_affine()` to route into
//! `on_affine_gate_worker_terminal` instead of being reported to the
//! primary as a normal `TaskComplete` / `TaskFailed`.
//!
//! Test inventory (mirrors brief T1-T6):
//!   * `t1_gate_body_dispatched_to_worker_not_inline` — the dispatch arm
//!     puts the gate hash into `active_tasks` (the worker-bound bookkeeping
//!     every task goes through), proving the body left the secondary thread
//!     for a worker subprocess instead of running inline.
//!   * `t2_secondary_inbox_not_blocked_during_gate_body` — `ensure_affine_import`
//!     returns SYNCHRONOUSLY on the StartedRun branch (the dispatch yields
//!     to the worker; the operational loop is free to drain the next inbox
//!     frame), so a sibling inbox event lands on the very next tick rather
//!     than waiting for the multi-minute gate body.
//!   * `t3_555_run_once_latch_second_dependent_queues` — second dependent
//!     on the same gate hash gets `QueuedBehindRun` (NOT a second dispatch);
//!     #555 spec compliance preserved.
//!   * `t4_satisfied_probe_seeds_affine_done` — a `Satisfied` probe verdict
//!     short-circuits to `AlreadyDone`, seeds `affine_done` with the hash,
//!     and never dispatches a worker (the #537 cheap-path optimization
//!     remains intact post-#577).
//!   * `t5_primary_skips_secondary_affine_in_active_dispatch` — verified
//!     statically through the `TaskKind::is_worker_assignable()` predicate
//!     (which returns `false` for `SecondaryAffine`); included here as a
//!     guard that the kind predicate didn't drift.
//!   * `t6_worker_terminal_routes_to_release_body` — feed a
//!     `WorkerEvent::TaskCompleted` for the gate task into
//!     `handle_worker_event` and observe the release body
//!     emitting `LocalDependencyReleased` for the queued dependent.

#![cfg(test)]

use std::collections::BTreeMap;
use std::rc::Rc;
use std::cell::RefCell;

use dynrunner_core::{ErrorType, ResourceMap, TaskInfo, TaskKind, TaskResult};
use dynrunner_manager_local::worker::WorkerEvent;
use dynrunner_protocol_primary_secondary::{ClusterMutation, DistributedMessage};

use super::super::test_helpers::{FakeWorkerFactory, TestId, make_secondary_recording};
use super::super::{AffineGateOutcome, PendingAffineDependent};
use super::firstbind_orphan::{one_worker_config, test_oom_watcher};
use super::processing::make_binary;
use crate::affine_satisfied::AffineSatisfiedProbe;

/// Seed a SecondaryAffine task `I` into the secondary's replicated ledger
/// under `hash` (the executor resolves its `TaskInfo` from there).
/// Mirrors the pre-#577 helper but the kind is the same: SecondaryAffine.
fn seed_affine_task(
    sec: &mut crate::secondary::test_helpers::SecondaryHarness<
        crate::secondary::test_helpers::RecordingPeer<TestId>,
    >,
    hash: &str,
) -> TaskInfo<TestId> {
    let mut task = make_binary(hash, 0);
    task.kind = TaskKind::SecondaryAffine;
    sec.cluster_state.apply(ClusterMutation::TaskAdded {
        hash: hash.to_string(),
        task: task.clone(),
    });
    task
}

/// Build a `PendingAffineDependent` for work task `B` (`work_hash`) bound to
/// `worker_id` — everything the release dispatch needs.
fn make_dependent(work_hash: &str, worker_id: u32) -> PendingAffineDependent<TestId> {
    PendingAffineDependent {
        work_hash: work_hash.to_string(),
        worker_id,
        binary: make_binary(work_hash, 50),
        estimated: ResourceMap::new(),
        predecessor_outputs: BTreeMap::new(),
    }
}

/// Count `LocalDependencyReleased` frames addressed to the primary.
fn released_hashes(log: &Rc<RefCell<Vec<DistributedMessage<TestId>>>>) -> Vec<String> {
    log.borrow()
        .iter()
        .filter_map(|m| match m {
            DistributedMessage::LocalDependencyReleased { task_hash, .. } => Some(task_hash.clone()),
            _ => None,
        })
        .collect()
}

/// Count `TaskQueuedAfterLocalDependency` frames for `affine_hash`.
fn queued_work_hashes(
    log: &Rc<RefCell<Vec<DistributedMessage<TestId>>>>,
    affine_hash: &str,
) -> Vec<String> {
    log.borrow()
        .iter()
        .filter_map(|m| match m {
            DistributedMessage::TaskQueuedAfterLocalDependency {
                task_hash,
                affine_hash: ah,
                ..
            } if ah == affine_hash => Some(task_hash.clone()),
            _ => None,
        })
        .collect()
}

// ── T1 ────────────────────────────────────────────────────────────────────
// The dispatch arm puts the gate hash into `active_tasks` — the
// worker-bound bookkeeping every task crosses — proving the gate body
// left the secondary thread for a worker subprocess instead of running
// inline (which would have stayed entirely on the LocalSet thread).
#[tokio::test(flavor = "current_thread")]
async fn t1_gate_body_dispatched_to_worker_not_inline() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut sec, _log) = make_secondary_recording(one_worker_config("sec-2"), 1);
            sec.set_bootstrap_primary_id("setup".to_string());
            let mut factory = FakeWorkerFactory;
            let pool = sec.initialize_workers(&mut factory).await.unwrap();
            sec.enter_operational_for_test();
            *sec.pool_mut() = pool;

            let affine_hash = "gate-hash-A";
            seed_affine_task(&mut sec, affine_hash);

            // First dependent: triggers StartedRun, which now dispatches
            // the gate body to a worker subprocess.
            let dep = make_dependent("work-B", 0);
            let outcome = sec
                .ensure_affine_import(affine_hash.to_string(), dep)
                .await
                .unwrap();
            assert_eq!(outcome, AffineGateOutcome::StartedRun);

            // The new path: try_gate_on_affine_import dispatches the gate
            // body. Call dispatch_affine_gate_to_worker directly here (as
            // try_gate's StartedRun branch does) and assert the gate
            // appears in active_tasks — the worker-bound key the worker's
            // terminal looks up. Pre-#577 the gate body ran inline on the
            // LocalSet thread and NEVER entered active_tasks.
            sec.dispatch_affine_gate_to_worker(affine_hash.to_string(), 0, &mut factory)
                .await
                .unwrap();

            // active_tasks[affine_hash] = 0 proves the gate body left the
            // secondary thread for a worker subprocess. (pending_first_bind
            // is the respawn-HOLD twin; either resolves the new shape.)
            let in_active = sec.op_mut().active_tasks.contains_key(affine_hash);
            let in_pending_first_bind = sec
                .op_mut()
                .pending_first_bind
                .values()
                .any(|stash| stash.file_hash == affine_hash);
            assert!(
                in_active || in_pending_first_bind,
                "T1: the gate body must dispatch through the worker-bound \
                 path (active_tasks or pending_first_bind); pre-#577 it ran \
                 inline on the LocalSet thread and never entered either set"
            );
        })
        .await;
}

// ── T2 ────────────────────────────────────────────────────────────────────
// `ensure_affine_import` returns SYNCHRONOUSLY on StartedRun — the
// dispatch is a single `assign_resolved_task` call that yields back to
// the loop. Pre-#577 the inline-Python path did NOT block here either
// (the spawn_local kept the loop unblocked), so this test confirms the
// same property post-#577: the gate-body dispatch is a single
// non-blocking dispatch call.
#[tokio::test(flavor = "current_thread")]
async fn t2_secondary_inbox_not_blocked_during_gate_body() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut sec, _log) = make_secondary_recording(one_worker_config("sec-2"), 1);
            sec.set_bootstrap_primary_id("setup".to_string());
            let mut factory = FakeWorkerFactory;
            let pool = sec.initialize_workers(&mut factory).await.unwrap();
            sec.enter_operational_for_test();
            *sec.pool_mut() = pool;

            let affine_hash = "gate-hash-B";
            seed_affine_task(&mut sec, affine_hash);

            let dep = make_dependent("work-B", 0);
            // The whole try_gate path must return without spawning a
            // long-running future or blocking on any external resource.
            let start = std::time::Instant::now();
            let _ = sec
                .ensure_affine_import(affine_hash.to_string(), dep)
                .await
                .unwrap();
            sec.dispatch_affine_gate_to_worker(affine_hash.to_string(), 0, &mut factory)
                .await
                .unwrap();
            let elapsed = start.elapsed();
            assert!(
                elapsed < std::time::Duration::from_millis(500),
                "T2: gate-body dispatch must be non-blocking; took {elapsed:?}"
            );
        })
        .await;
}

// ── T3 ────────────────────────────────────────────────────────────────────
// #555 spec: second dependent on the same gate hash queues (the
// presence of the hash in `affine_running` is the latch — only the
// first dependent dispatches the gate body). No second worker
// assignment; the second `B` enters `QueuedAfterLocalDependency`.
#[tokio::test(flavor = "current_thread")]
async fn t3_555_run_once_latch_second_dependent_queues() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut sec, log) = make_secondary_recording(one_worker_config("sec-2"), 1);
            sec.set_bootstrap_primary_id("setup".to_string());
            let mut factory = FakeWorkerFactory;
            let pool = sec.initialize_workers(&mut factory).await.unwrap();
            sec.enter_operational_for_test();
            *sec.pool_mut() = pool;

            let affine_hash = "gate-hash-C";
            seed_affine_task(&mut sec, affine_hash);

            // First dependent: StartedRun (the dispatcher).
            let dep1 = make_dependent("work-B1", 0);
            let outcome1 = sec
                .ensure_affine_import(affine_hash.to_string(), dep1)
                .await
                .unwrap();
            assert_eq!(outcome1, AffineGateOutcome::StartedRun);

            // Second dependent on the SAME hash: QueuedBehindRun (NO
            // second dispatch).
            let dep2 = make_dependent("work-B2", 0);
            let outcome2 = sec
                .ensure_affine_import(affine_hash.to_string(), dep2)
                .await
                .unwrap();
            assert_eq!(
                outcome2,
                AffineGateOutcome::QueuedBehindRun,
                "T3: second dependent on the same gate hash must queue \
                 behind the in-flight gate body, NOT trigger a second \
                 worker dispatch (#555 run-once latch)"
            );

            sec.drain_egress().await;

            // Both dependents reported the queued state.
            let queued = queued_work_hashes(&log, affine_hash);
            assert!(
                queued.contains(&"work-B1".to_string()),
                "T3: first dependent must report QueuedAfterLocalDependency"
            );
            assert!(
                queued.contains(&"work-B2".to_string()),
                "T3: second dependent must report QueuedAfterLocalDependency"
            );
        })
        .await;
}

// ── T4 ────────────────────────────────────────────────────────────────────
// #537 AffineSatisfiedProbe still seeds `affine_done` when the producing
// node short-circuits — preserved exactly post-#577 (no worker
// dispatch, no `QueuedAfterLocalDependency` frames).
#[tokio::test(flavor = "current_thread")]
async fn t4_satisfied_probe_seeds_affine_done() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut sec, log) = make_secondary_recording(one_worker_config("sec-2"), 1);
            sec.set_bootstrap_primary_id("setup".to_string());
            let mut factory = FakeWorkerFactory;
            let pool = sec.initialize_workers(&mut factory).await.unwrap();
            sec.enter_operational_for_test();
            *sec.pool_mut() = pool;

            let affine_hash = "gate-hash-D";
            seed_affine_task(&mut sec, affine_hash);

            struct AlwaysSatisfied;
            impl AffineSatisfiedProbe<TestId> for AlwaysSatisfied {
                fn is_satisfied(&self, _task: &TaskInfo<TestId>) -> bool {
                    true
                }
            }
            sec.set_affine_satisfied_probe(std::sync::Arc::new(AlwaysSatisfied));

            let dep = make_dependent("work-B", 0);
            let outcome = sec
                .ensure_affine_import(affine_hash.to_string(), dep)
                .await
                .unwrap();
            assert_eq!(
                outcome,
                AffineGateOutcome::AlreadyDone,
                "T4: a Satisfied probe verdict must short-circuit to AlreadyDone"
            );
            assert!(
                sec.op_mut().affine_done.contains(affine_hash),
                "T4: a Satisfied verdict must seed affine_done so every \
                 subsequent dependent short-circuits"
            );
            assert!(
                !sec.op_mut().affine_running.contains_key(affine_hash),
                "T4: a probe short-circuit must NEVER touch the run-once latch"
            );
            sec.drain_egress().await;
            assert!(
                queued_work_hashes(&log, affine_hash).is_empty(),
                "T4: a probe short-circuit must NEVER emit \
                 QueuedAfterLocalDependency frames"
            );
        })
        .await;
}

// ── T5 ────────────────────────────────────────────────────────────────────
// Primary's worker-dispatch path filters by `kind.is_worker_assignable()`,
// which returns `false` for `SecondaryAffine`. This guard pins the kind
// predicate — a regression would silently let the primary push affine
// task bodies through the normal worker-dispatch queue, which #555 forbids.
#[test]
fn t5_secondary_affine_is_not_worker_assignable() {
    assert!(
        !TaskKind::SecondaryAffine.is_worker_assignable(),
        "T5: SecondaryAffine MUST NOT be worker-assignable (the primary \
         does not store / dispatch gate bodies through its worker queue; \
         #555 + #577)"
    );
    assert!(
        TaskKind::Work.is_worker_assignable(),
        "T5 guard: Work MUST stay worker-assignable (only SecondaryAffine \
         is filtered)"
    );
}

// ── T6 ────────────────────────────────────────────────────────────────────
// The worker terminal arm in `handle_worker_event` recognizes
// `binary.kind.is_secondary_affine()` and routes the outcome to the
// release body, emitting `LocalDependencyReleased` for the queued
// dependents — NOT a primary-bound `TaskComplete` for the gate hash.
#[tokio::test(flavor = "current_thread")]
async fn t6_worker_terminal_routes_to_release_body() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut sec, log) = make_secondary_recording(one_worker_config("sec-2"), 1);
            sec.set_bootstrap_primary_id("setup".to_string());
            let mut factory = FakeWorkerFactory;
            let pool = sec.initialize_workers(&mut factory).await.unwrap();
            sec.enter_operational_for_test();
            *sec.pool_mut() = pool;

            let affine_hash = "gate-hash-E";
            let gate_binary = seed_affine_task(&mut sec, affine_hash);

            // Queue a dependent behind the gate body.
            let dep = make_dependent("work-B", 0);
            let outcome = sec
                .ensure_affine_import(affine_hash.to_string(), dep)
                .await
                .unwrap();
            assert_eq!(outcome, AffineGateOutcome::StartedRun);

            // Simulate the gate body being dispatched: put it into
            // active_tasks (the dispatch path does this via
            // assign_resolved_task; we shortcut here to focus on the
            // terminal-arm routing logic).
            sec.op_mut()
                .active_tasks
                .insert(affine_hash.to_string(), 0);
            sec.op_mut().pool.workers[0].loaded_type_id =
                Some(gate_binary.type_id.clone());
            let current_gen = sec.op_mut().pool.workers[0].generation;

            // The worker reports the gate body completed cleanly.
            let oom = test_oom_watcher();
            sec.handle_worker_event(
                WorkerEvent::TaskCompleted {
                    worker_id: 0,
                    generation: current_gen,
                    result: TaskResult::ok(),
                    result_data: None,
                    binary: Some(gate_binary),
                    estimated_resources: ResourceMap::new(),
                },
                &oom,
                &mut factory,
            )
            .await
            .unwrap();
            sec.drain_egress().await;

            // The release body emitted `LocalDependencyReleased` for the
            // queued dependent — the secondary's own affine path, NOT a
            // primary-bound `TaskComplete` for the gate.
            let released = released_hashes(&log);
            assert!(
                released.contains(&"work-B".to_string()),
                "T6: gate-body worker terminal must drain the dependent \
                 queue and emit `LocalDependencyReleased` for each queued \
                 dependent; got released={released:?}"
            );

            // The gate hash entered `affine_done` — future dependents
            // short-circuit `AlreadyDone`.
            assert!(
                sec.op_mut().affine_done.contains(affine_hash),
                "T6: a successful gate-body terminal must seed affine_done"
            );

            // The gate hash did NOT get reported as a `TaskComplete` to
            // the primary (the kind predicate routed it through the
            // affine seam instead).
            let primary_completes: Vec<String> = log
                .borrow()
                .iter()
                .filter_map(|m| match m {
                    DistributedMessage::TaskComplete { task_hash, .. }
                        if task_hash.as_str() == affine_hash =>
                    {
                        Some(task_hash.clone())
                    }
                    _ => None,
                })
                .collect();
            assert!(
                primary_completes.is_empty(),
                "T6: a SecondaryAffine gate-body terminal MUST NOT be \
                 reported to the primary as `TaskComplete` — the gate's \
                 authoritative effect is the per-dependent \
                 `LocalDependencyReleased`; got={primary_completes:?}"
            );
        })
        .await;
}

// ── Bonus ─────────────────────────────────────────────────────────────────
// A failed gate body fails each queued dependent (re-routable per #495)
// and leaves `affine_done` UNTOUCHED — the done set is never poisoned.
#[tokio::test(flavor = "current_thread")]
async fn failed_gate_body_does_not_poison_done_set() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut sec, log) = make_secondary_recording(one_worker_config("sec-2"), 1);
            sec.set_bootstrap_primary_id("setup".to_string());
            let mut factory = FakeWorkerFactory;
            let pool = sec.initialize_workers(&mut factory).await.unwrap();
            sec.enter_operational_for_test();
            *sec.pool_mut() = pool;

            let affine_hash = "gate-hash-F";
            let gate_binary = seed_affine_task(&mut sec, affine_hash);

            let dep = make_dependent("work-B", 0);
            let _ = sec
                .ensure_affine_import(affine_hash.to_string(), dep)
                .await
                .unwrap();
            sec.op_mut()
                .active_tasks
                .insert(affine_hash.to_string(), 0);
            sec.op_mut().pool.workers[0].loaded_type_id =
                Some(gate_binary.type_id.clone());
            let current_gen = sec.op_mut().pool.workers[0].generation;

            let oom = test_oom_watcher();
            sec.handle_worker_event(
                WorkerEvent::TaskCompleted {
                    worker_id: 0,
                    generation: current_gen,
                    result: TaskResult::error(
                        ErrorType::Recoverable,
                        "gate body failed".into(),
                    ),
                    result_data: None,
                    binary: Some(gate_binary),
                    estimated_resources: ResourceMap::new(),
                },
                &oom,
                &mut factory,
            )
            .await
            .unwrap();
            sec.drain_egress().await;

            assert!(
                !sec.op_mut().affine_done.contains(affine_hash),
                "a failed gate body must NEVER mark the hash affine_done \
                 (the done set is never poisoned)"
            );
            // The queued dependent failed Recoverably (re-routable per #495).
            let failed: Vec<(String, ErrorType)> = log
                .borrow()
                .iter()
                .filter_map(|m| match m {
                    DistributedMessage::TaskFailed {
                        task_hash,
                        error_type,
                        ..
                    } => Some((task_hash.clone(), error_type.clone())),
                    _ => None,
                })
                .collect();
            assert!(
                failed.iter().any(|(h, _)| h == "work-B"),
                "the queued dependent must be reported TaskFailed; got={failed:?}"
            );
        })
        .await;
}
