//! Per-variant unit tests for [`super::command_channel::handle_local_command`].
//!
//! Single concern: pin the local-backend semantics of each
//! `PrimaryCommand` variant against a stubbed-pool `LocalManager`.
//! Mirrors the distributed crate's
//! `secondary/tests/command_channel.rs` test suite at the local
//! backend's symmetry point.
//!
//! Test infrastructure:
//!   * `LocalManager::install_pool_for_test` seam to skip the full
//!     `process_binaries` bootstrap.
//!   * `tokio::sync::oneshot` for the reply oneshots.
//!   * `current_thread` runtime so we don't have to deal with
//!     `Send` bounds on the inner closures.

#![cfg(test)]

use std::collections::HashSet;

use dynrunner_core::{ErrorType, PrimaryCommand, ResourceMap, TaskInfo, compute_task_hash};
use dynrunner_scheduler::ResourceStealingScheduler;
use dynrunner_scheduler_api::{PendingPool, ResourceEstimator};
use dynrunner_transport_channel::ChannelManagerEnd;
use tokio::sync::oneshot;

use super::LocalManager;
use super::command_channel::handle_local_command;
use super::test_helpers::{FixedEstimator, TestId, make_binary, test_config};

/// Build a manager + install a single-phase pool with `binaries`
/// pre-extended. Returns the assembled manager ready for command-
/// channel exercise.
fn manager_with_binaries(
    binaries: Vec<TaskInfo<TestId>>,
) -> LocalManager<ChannelManagerEnd, ResourceStealingScheduler, FixedEstimator, TestId> {
    let config = test_config(1);
    let mut manager: LocalManager<ChannelManagerEnd, _, _, TestId> = LocalManager::new(
        config,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    let mut phase_ids = HashSet::new();
    phase_ids.insert(dynrunner_core::PhaseId::from("default"));
    let pool = PendingPool::new(phase_ids, std::collections::HashMap::new()).expect("pool new");
    manager.install_pool_for_test(pool);
    // Mirror `process_binaries`' initial-batch mirror so the
    // command-channel handler can resolve hashes for tasks the
    // pool already holds.
    for t in &binaries {
        manager.task_by_hash.insert(compute_task_hash(t), t.clone());
    }
    if !binaries.is_empty() {
        manager.pool_mut().extend(binaries).expect("extend");
    }
    manager
}

/// `SpawnTasks` happy path: accept every input task, mirror into
/// `task_by_hash`, bump `stats.total`, return an empty error vec.
#[tokio::test(flavor = "current_thread")]
async fn spawn_tasks_happy_path_extends_pool_and_mirror() {
    let mut manager = manager_with_binaries(vec![]);
    let prior_total = manager.stats.total;
    let prior_hash_count = manager.task_by_hash.len();
    let tasks = vec![make_binary("a", 100), make_binary("b", 200)];
    let (tx, rx) = oneshot::channel();
    let cmd = PrimaryCommand::SpawnTasks { tasks, reply: tx };
    handle_local_command(&mut manager, cmd).await;

    let result = rx.await.expect("reply oneshot");
    let errors = result.expect("inner Ok");
    assert!(errors.is_empty(), "no per-task errors on happy path");
    assert_eq!(manager.stats.total, prior_total + 2);
    assert_eq!(manager.task_by_hash.len(), prior_hash_count + 2);
    assert!(!manager.pool_ref().is_empty(), "pool grew via extend");
}

/// `SpawnTasks` duplicate-hash: input task whose hash already lives in
/// `task_by_hash` is rejected with `DuplicateTaskHash`; the rest of
/// the batch proceeds.
#[tokio::test(flavor = "current_thread")]
async fn spawn_tasks_duplicate_hash_surfaces_per_task_error() {
    let existing = make_binary("existing", 100);
    let mut manager = manager_with_binaries(vec![existing.clone()]);

    // Build a batch with `existing` (duplicate) + `fresh` (new).
    let tasks = vec![existing.clone(), make_binary("fresh", 50)];
    let (tx, rx) = oneshot::channel();
    handle_local_command(
        &mut manager,
        PrimaryCommand::SpawnTasks { tasks, reply: tx },
    )
    .await;
    let errors = rx.await.unwrap().unwrap();
    assert_eq!(errors.len(), 1, "one duplicate error");
    let (idx, err) = &errors[0];
    assert_eq!(*idx, 0, "the duplicate was at index 0");
    assert!(
        matches!(err, dynrunner_core::SpawnError::DuplicateTaskHash(_)),
        "expected DuplicateTaskHash, got {err:?}"
    );
    // The `fresh` task still made it through.
    assert!(
        manager
            .task_by_hash
            .contains_key(&compute_task_hash(&make_binary("fresh", 50)))
    );
}

/// `FailPermanent` happy path: looked-up task is pushed to
/// `failed_tasks` with the requested ErrorType + reason; reply `Ok`.
#[tokio::test(flavor = "current_thread")]
async fn fail_permanent_pushes_to_failed_tasks() {
    let binary = make_binary("doomed", 100);
    let hash = compute_task_hash(&binary);
    let mut manager = manager_with_binaries(vec![binary]);

    let (tx, rx) = oneshot::channel();
    handle_local_command(
        &mut manager,
        PrimaryCommand::FailPermanent {
            hash: hash.clone(),
            error: ErrorType::NonRecoverable,
            reason: "operator decision".into(),
            reply: tx,
        },
    )
    .await;
    rx.await.expect("reply oneshot").expect("inner Ok");
    assert_eq!(manager.failed_tasks.len(), 1);
    let f = &manager.failed_tasks[0];
    assert!(matches!(f.error_type, ErrorType::NonRecoverable));
    assert_eq!(f.error_message, "operator decision");
    assert_eq!(f.binary.task_id, "doomed");
}

/// `FailPermanent` unknown-hash: reply is `Err(...)`, no side-queue
/// mutation.
#[tokio::test(flavor = "current_thread")]
async fn fail_permanent_unknown_hash_returns_error() {
    let mut manager = manager_with_binaries(vec![]);
    let (tx, rx) = oneshot::channel();
    handle_local_command(
        &mut manager,
        PrimaryCommand::FailPermanent {
            hash: "nonexistent".into(),
            error: ErrorType::NonRecoverable,
            reason: "test".into(),
            reply: tx,
        },
    )
    .await;
    let outcome = rx.await.expect("reply oneshot");
    assert!(outcome.is_err());
    assert!(manager.failed_tasks.is_empty());
}

/// `ReinjectTask` from `failed_tasks`: task is removed from the
/// side queue and pushed back into the pool. Reply `Ok`.
#[tokio::test(flavor = "current_thread")]
async fn reinject_task_pulls_from_failed_tasks_side_queue() {
    let binary = make_binary("retryable", 100);
    let hash = compute_task_hash(&binary);
    let mut manager = manager_with_binaries(vec![binary.clone()]);
    // Pre-stage the task in `failed_tasks` (simulating a prior worker
    // failure).
    manager.failed_tasks.push(dynrunner_core::FailedTask {
        binary: binary.clone(),
        error_type: ErrorType::Recoverable,
        error_message: "first attempt failed".into(),
        retry_count: 0,
    });
    // Take it out of the pool first (the production retry path would
    // have done this via `on_item_finished` / extend / drain — for
    // this unit test we just need the side queue to be the only home).
    let _drained: Vec<_> = manager.pool_mut().drain_queued();

    let prior_pool_empty = manager.pool_ref().is_empty();
    assert!(prior_pool_empty);

    let (tx, rx) = oneshot::channel();
    handle_local_command(
        &mut manager,
        PrimaryCommand::ReinjectTask {
            hash: hash.clone(),
            reply: tx,
        },
    )
    .await;
    rx.await.unwrap().expect("inner Ok");
    assert!(manager.failed_tasks.is_empty(), "removed from side queue");
    assert!(!manager.pool_ref().is_empty(), "pushed back into pool");
}

/// `ReinjectTask` budget exhaustion: with cap=1 and one prior
/// reinject, the second call returns `Err` and the task stays in the
/// side queue.
#[tokio::test(flavor = "current_thread")]
async fn reinject_task_budget_exhausted_refuses() {
    let binary = make_binary("capped", 100);
    let hash = compute_task_hash(&binary);
    let mut config = test_config(1);
    config.unfulfillable_reinject_max_per_task = Some(1);
    let mut manager: LocalManager<ChannelManagerEnd, _, _, TestId> = LocalManager::new(
        config,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    let mut phase_ids = HashSet::new();
    phase_ids.insert(dynrunner_core::PhaseId::from("default"));
    let pool = PendingPool::new(phase_ids, std::collections::HashMap::new()).expect("pool new");
    manager.install_pool_for_test(pool);
    manager.task_by_hash.insert(hash.clone(), binary.clone());
    // Stage two reinject attempts; only the first should succeed.
    for _ in 0..2 {
        manager.failed_tasks.push(dynrunner_core::FailedTask {
            binary: binary.clone(),
            error_type: ErrorType::Recoverable,
            error_message: "stub".into(),
            retry_count: 0,
        });
    }

    // First reinject — consumes the single ticket.
    let (tx1, rx1) = oneshot::channel();
    handle_local_command(
        &mut manager,
        PrimaryCommand::ReinjectTask {
            hash: hash.clone(),
            reply: tx1,
        },
    )
    .await;
    rx1.await.unwrap().expect("first reinject must succeed");

    // Second reinject — refused on budget.
    let (tx2, rx2) = oneshot::channel();
    handle_local_command(
        &mut manager,
        PrimaryCommand::ReinjectTask {
            hash: hash.clone(),
            reply: tx2,
        },
    )
    .await;
    let outcome = rx2.await.unwrap();
    assert!(outcome.is_err(), "second reinject refused on budget");
    // The remaining staged-failed entry stays in `failed_tasks`.
    assert_eq!(manager.failed_tasks.len(), 1);
}

/// `UpdatePreferredSecondaries` mirror: matching pool entry has its
/// `preferred_secondaries` updated; reply `Ok`.
#[tokio::test(flavor = "current_thread")]
async fn update_preferred_secondaries_mirrors_into_pool() {
    let binary = make_binary("with_pref", 100);
    let hash = compute_task_hash(&binary);
    let mut manager = manager_with_binaries(vec![binary]);

    let (tx, rx) = oneshot::channel();
    handle_local_command(
        &mut manager,
        PrimaryCommand::UpdatePreferredSecondaries {
            hash: hash.clone(),
            secondaries: vec!["sec-a".into(), "sec-b".into()],
            reply: tx,
        },
    )
    .await;
    rx.await.unwrap().expect("inner Ok");

    // The pool entry should now carry the updated preference list.
    let matched = manager.pool_mut().update_first_match_in_place(
        |t| compute_task_hash(t) == hash,
        |t| {
            assert_eq!(t.preferred_secondaries.0, vec!["sec-a", "sec-b"]);
        },
    );
    assert!(matched, "the task is still in the pool");
}

/// `UpdatePreferredSecondaries` against a hash not in the pool:
/// reply is still `Ok` (local mode has no peer concept; the no-match
/// path is debug-logged but not an error).
#[tokio::test(flavor = "current_thread")]
async fn update_preferred_secondaries_no_match_replies_ok() {
    let mut manager = manager_with_binaries(vec![]);
    let (tx, rx) = oneshot::channel();
    handle_local_command(
        &mut manager,
        PrimaryCommand::UpdatePreferredSecondaries {
            hash: "ghost".into(),
            secondaries: vec!["sec-a".into()],
            reply: tx,
        },
    )
    .await;
    rx.await
        .unwrap()
        .expect("inner Ok on no-match (local mode)");
}

// Silence dead-code warnings for the manager's generic type
// parameters (estimator, etc.) the unit tests don't touch beyond
// construction.
#[allow(dead_code)]
fn _refer_to_resource_estimator<R: ResourceEstimator<TestId>>() {}
#[allow(dead_code)]
fn _refer_to_resource_map() -> ResourceMap {
    ResourceMap::new()
}
