//! Regression: a SLURM-like secondary with the cross-thread command
//! channel wired observes `PrimaryCommand` deliveries from its
//! `command_sender()` clone, the dispatcher routes them to the
//! per-variant `apply_*` methods, and the cluster_state /
//! `primary_pending` / `primary_failed` ledgers reflect the
//! mutation just as the operational `select!` arm would.
//!
//! Scope (pins the Step 4 / Phase B brief's load-bearing primitive):
//!   1. `command_sender()` clone delivers a `SpawnTasks` end-to-end:
//!      `cluster_state.task_count()` grows by the size of the valid
//!      subset, the spawned task lands `Pending` in the ledger, the
//!      reply oneshot fires with the per-index error vec, and the
//!      reinjection step puts the new entry into `primary_pending`.
//!   2. `FailPermanent` via the command channel records the failure
//!      in `primary_failed`, drives the per-phase counter bump
//!      (`process_primary_phase_lifecycle` cascade), and broadcasts
//!      `TaskFailed`.
//!   3. `ReinjectTask` via the command channel respects the
//!      per-task `SecondaryConfig::unfulfillable_reinject_max_per_task`
//!      budget: a 0-cap config refuses the second reinject with
//!      `Err(budget exhausted)`.
//!   4. `UpdatePreferredSecondaries` via the command channel emits
//!      the CRDT mutation and updates the live `primary_pending`
//!      entry's `preferred_secondaries` field.
//!
//! Test fixture: builds a `SecondaryCoordinator` directly via
//! `make_secondary`, hand-builds the `primary_pending` pool +
//! ledger entries the per-variant handlers need, and drives the
//! dispatch via `handle_secondary_command` (the same entry the
//! operational `select!` arm calls). No tokio runtime spin-up — the
//! goal is to pin the dispatch + apply contract, not the
//! `select!`-loop integration (which the existing setup-promote
//! tests already drive end-to-end).

#![cfg(test)]

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use dynrunner_core::{ErrorType, PhaseId, SoftPreferredSecondaries, TaskInfo, TypeId};
use dynrunner_scheduler_api::PendingPool;
use tokio::sync::oneshot;

use super::super::test_helpers::{election_config, make_secondary, TestId};
use crate::cluster_state::TaskState;
use crate::primary::PrimaryCommand;

/// (1) SpawnTasks via the command-sender clone reaches the
/// secondary's `apply_spawn_tasks` and grows the cluster ledger.
#[tokio::test(flavor = "current_thread")]
async fn spawn_tasks_via_command_channel_grows_cluster_state() {
    let phase_a = PhaseId::from("phase-a");
    let mut sec = make_secondary(election_config("sec-0"));
    // Promoted-secondary scenario: the per-variant handlers route
    // through `primary_pending` post-apply. Flip the flag and seed
    // the pool so the reinject step has a destination.
    sec.is_primary = true;
    let mut phase_set = HashSet::new();
    phase_set.insert(phase_a.clone());
    sec.primary_pending = Some(
        PendingPool::<TestId>::new(phase_set, HashMap::new())
            .expect("pool graph valid"),
    );

    let initial = sec.cluster_state.task_count();

    let task = TaskInfo {
        path: PathBuf::from("/tmp/a"),
        size: 100,
        identifier: TestId("a".into()),
        phase_id: phase_a.clone(),
        type_id: TypeId::from("default"),
        affinity_id: None,
        payload: serde_json::Value::Null,
        task_id: Some("task-a".into()),
        task_depends_on: vec![],
        preferred_secondaries: SoftPreferredSecondaries::default(),
        resolved_path: None,
    };
    let task_hash = crate::primary::wire::compute_task_hash(&task);

    // Drive the dispatch exactly as the `select!` arm would: build
    // the command + reply oneshot, hand it to the dispatcher, await
    // the outcome.
    let (reply_tx, reply_rx) = oneshot::channel();
    let cmd = PrimaryCommand::SpawnTasks {
        tasks: vec![task.clone()],
        reply: reply_tx,
    };
    crate::secondary::command_channel::handle_secondary_command(
        &mut sec, cmd,
    )
    .await;

    let errors = reply_rx
        .await
        .expect("reply oneshot fires")
        .expect("apply succeeds");
    assert!(
        errors.is_empty(),
        "well-formed task has no per-index errors: {errors:?}"
    );

    // cluster_state grew by 1; the spawned task is Pending.
    assert_eq!(
        sec.cluster_state.task_count(),
        initial + 1,
        "task_count grows by the spawn batch size"
    );
    let state = sec
        .cluster_state
        .task_state(&task_hash)
        .expect("spawned task in ledger");
    assert!(
        matches!(state, TaskState::Pending { .. }),
        "spawned task lands as Pending: got {state:?}"
    );
    // The post-apply walk reinjected it into `primary_pending` —
    // the pool now has at least the one item we spawned.
    assert!(
        !sec.primary_pending
            .as_ref()
            .expect("pool present")
            .is_empty(),
        "spawned task lands in primary_pending after apply"
    );
}

/// (2) FailPermanent via the command channel records into
/// `primary_failed`, bumps `primary_phase_failed`, and broadcasts.
#[tokio::test(flavor = "current_thread")]
async fn fail_permanent_via_command_channel_records_into_primary_failed() {
    let phase_a = PhaseId::from("phase-a");
    let mut sec = make_secondary(election_config("sec-0"));
    sec.is_primary = true;
    let mut phase_set = HashSet::new();
    phase_set.insert(phase_a.clone());
    sec.primary_pending = Some(
        PendingPool::<TestId>::new(phase_set, HashMap::new())
            .expect("pool graph valid"),
    );

    // Seed a single task into the cluster ledger so the
    // hash-to-meta lookup in `apply_fail_permanent` succeeds.
    let task = TaskInfo {
        path: PathBuf::from("/tmp/a"),
        size: 100,
        identifier: TestId("a".into()),
        phase_id: phase_a.clone(),
        type_id: TypeId::from("default"),
        affinity_id: None,
        payload: serde_json::Value::Null,
        task_id: Some("task-a".into()),
        task_depends_on: vec![],
        preferred_secondaries: SoftPreferredSecondaries::default(),
        resolved_path: None,
    };
    let hash = crate::primary::wire::compute_task_hash(&task);
    sec.cluster_state.apply(
        dynrunner_protocol_primary_secondary::ClusterMutation::TaskAdded {
            hash: hash.clone(),
            task: task.clone(),
        },
    );

    assert!(sec.primary_failed.is_empty(), "fixture starts clean");

    let (reply_tx, reply_rx) = oneshot::channel();
    let cmd = PrimaryCommand::FailPermanent {
        hash: hash.clone(),
        error: ErrorType::NonRecoverable,
        reason: "operator decision".into(),
        reply: reply_tx,
    };
    crate::secondary::command_channel::handle_secondary_command(
        &mut sec, cmd,
    )
    .await;

    reply_rx
        .await
        .expect("reply oneshot fires")
        .expect("apply succeeds");

    // Local ledger grew: same shape `note_primary_item_failed`
    // produces on the worker-event path.
    assert_eq!(
        sec.primary_failed.len(),
        1,
        "FailPermanent records into primary_failed"
    );
    assert!(
        sec.primary_failed.contains_key(&hash),
        "the failed hash is the one we sent"
    );
    // Phase counter bumped — the cascade fires the `on_phase_end`
    // hook if one is registered (no hook here, but the counter
    // tracks regardless).
    assert_eq!(
        *sec.primary_phase_failed.get(&phase_a).unwrap_or(&0),
        1,
        "per-phase failure counter bumped"
    );
    // CRDT terminal state set to Failed.
    let state = sec
        .cluster_state
        .task_state(&hash)
        .expect("task still in ledger");
    assert!(
        matches!(state, TaskState::Failed { .. }),
        "ledger entry transitions to Failed: got {state:?}"
    );
}

/// (3) ReinjectTask via the command channel honours the
/// `SecondaryConfig::unfulfillable_reinject_max_per_task` budget.
#[tokio::test(flavor = "current_thread")]
async fn reinject_task_via_command_channel_respects_budget() {
    let phase_a = PhaseId::from("phase-a");
    // Cap of 1: the first reinject succeeds, the second fails with
    // `budget exhausted`.
    let mut cfg = election_config("sec-0");
    cfg.unfulfillable_reinject_max_per_task = Some(1);
    let mut sec = make_secondary(cfg);
    sec.is_primary = true;
    let mut phase_set = HashSet::new();
    phase_set.insert(phase_a.clone());
    sec.primary_pending = Some(
        PendingPool::<TestId>::new(phase_set, HashMap::new())
            .expect("pool graph valid"),
    );

    let task = TaskInfo {
        path: PathBuf::from("/tmp/a"),
        size: 100,
        identifier: TestId("a".into()),
        phase_id: phase_a.clone(),
        type_id: TypeId::from("default"),
        affinity_id: None,
        payload: serde_json::Value::Null,
        task_id: Some("task-a".into()),
        task_depends_on: vec![],
        preferred_secondaries: SoftPreferredSecondaries::default(),
        resolved_path: None,
    };
    let hash = crate::primary::wire::compute_task_hash(&task);
    // Seed as Unfulfillable directly so the reinject precondition
    // is satisfied without driving the full failure path.
    sec.cluster_state.apply(
        dynrunner_protocol_primary_secondary::ClusterMutation::TaskAdded {
            hash: hash.clone(),
            task: task.clone(),
        },
    );
    sec.cluster_state.apply(
        dynrunner_protocol_primary_secondary::ClusterMutation::TaskFailed {
            hash: hash.clone(),
            kind: ErrorType::Unfulfillable {
                reason: "test-seeded".into(),
            },
            error: "test-seeded".into(),
        },
    );
    // Sanity: the ledger says Unfulfillable.
    assert!(
        matches!(
            sec.cluster_state.task_state(&hash).unwrap(),
            TaskState::Unfulfillable { .. }
        ),
        "fixture seeded as Unfulfillable"
    );

    // First reinject: consumes 1 budget ticket, succeeds.
    let (reply_tx, reply_rx) = oneshot::channel();
    crate::secondary::command_channel::handle_secondary_command(
        &mut sec,
        PrimaryCommand::ReinjectTask {
            hash: hash.clone(),
            reply: reply_tx,
        },
    )
    .await;
    reply_rx
        .await
        .expect("reply fires")
        .expect("first reinject succeeds");

    // CRDT state is now Pending (re-injected).
    assert!(
        matches!(
            sec.cluster_state.task_state(&hash).unwrap(),
            TaskState::Pending { .. }
        ),
        "first reinject transitions Unfulfillable→Pending"
    );

    // Drive the CRDT back into Unfulfillable so the precondition
    // for a second reinject is met. This simulates a downstream
    // failure that re-emits Unfulfillable for the same hash.
    sec.cluster_state.apply(
        dynrunner_protocol_primary_secondary::ClusterMutation::TaskFailed {
            hash: hash.clone(),
            kind: ErrorType::Unfulfillable {
                reason: "test-reseeded".into(),
            },
            error: "test-reseeded".into(),
        },
    );

    // Second reinject: budget exhausted (cap=1, already consumed).
    // The handler returns `Err` and the local state stays
    // Unfulfillable.
    let (reply_tx, reply_rx) = oneshot::channel();
    crate::secondary::command_channel::handle_secondary_command(
        &mut sec,
        PrimaryCommand::ReinjectTask {
            hash: hash.clone(),
            reply: reply_tx,
        },
    )
    .await;
    let err = reply_rx
        .await
        .expect("reply fires")
        .expect_err("second reinject must hit budget");
    assert!(
        err.contains("budget exhausted"),
        "error message names the budget cause: {err}"
    );
    assert!(
        matches!(
            sec.cluster_state.task_state(&hash).unwrap(),
            TaskState::Unfulfillable { .. }
        ),
        "budget-rejected reinject leaves the state Unfulfillable"
    );
}

/// (4) UpdatePreferredSecondaries via the command channel mirrors
/// the new list onto the live `primary_pending` entry and
/// broadcasts the CRDT mutation.
#[tokio::test(flavor = "current_thread")]
async fn update_preferred_secondaries_via_command_channel_mirrors_to_pool() {
    let phase_a = PhaseId::from("phase-a");
    let mut sec = make_secondary(election_config("sec-0"));
    sec.is_primary = true;

    // Seed cluster_state + primary_pending with one task.
    let task = TaskInfo {
        path: PathBuf::from("/tmp/a"),
        size: 100,
        identifier: TestId("a".into()),
        phase_id: phase_a.clone(),
        type_id: TypeId::from("default"),
        affinity_id: None,
        payload: serde_json::Value::Null,
        task_id: Some("task-a".into()),
        task_depends_on: vec![],
        preferred_secondaries: SoftPreferredSecondaries::default(),
        resolved_path: None,
    };
    let hash = crate::primary::wire::compute_task_hash(&task);
    sec.cluster_state.apply(
        dynrunner_protocol_primary_secondary::ClusterMutation::TaskAdded {
            hash: hash.clone(),
            task: task.clone(),
        },
    );
    let mut phase_set = HashSet::new();
    phase_set.insert(phase_a.clone());
    let mut pool = PendingPool::<TestId>::new(phase_set, HashMap::new())
        .expect("pool graph valid");
    pool.extend(vec![task.clone()]).expect("pool extend valid");
    sec.primary_pending = Some(pool);

    let new_prefs = vec!["sec-1".to_string(), "sec-2".to_string()];

    let (reply_tx, reply_rx) = oneshot::channel();
    crate::secondary::command_channel::handle_secondary_command(
        &mut sec,
        PrimaryCommand::UpdatePreferredSecondaries {
            hash: hash.clone(),
            secondaries: new_prefs.clone(),
            reply: reply_tx,
        },
    )
    .await;
    reply_rx
        .await
        .expect("reply fires")
        .expect("apply succeeds");

    // The pool entry's preferred_secondaries field now reflects the
    // new list. `take_first_match` returns a clone if it matches
    // (and removes from the pool, which is fine — the test only
    // needs to read the field). The pool API exposes this take
    // primitive for ad-hoc inspection; there's no read-only
    // analogue today.
    let popped = sec
        .primary_pending
        .as_mut()
        .expect("pool present")
        .take_first_match(|t| {
            crate::primary::wire::compute_task_hash(t) == hash
        })
        .expect("entry remains in pool after update");

    assert_eq!(
        popped.preferred_secondaries.as_slice(),
        new_prefs.as_slice(),
        "live pool entry's preferred_secondaries mirror the broadcast"
    );
}
