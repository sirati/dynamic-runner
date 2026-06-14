//! #518 — the secondary side of the worker-source-of-truth
//! reconciliation: the `RequestInFlightRoster` responder and the
//! `WithdrawTask` honor path.
//!
//! A member is the source of truth for what ITS workers run. When the
//! primary re-admits this falsely-removed-but-alive member it asks for the
//! member's actual in-flight roster; the member answers off its live
//! own-worker bookkeeping (`active_tasks` + each worker's
//! `current_binary`). The primary then directs the member to withdraw any
//! DUPLICATE copy it requeued elsewhere — but only a copy that has NOT yet
//! started (a `pending_first_bind` deferral) is cleanly dropped; a copy
//! already running on a worker is left in place (no mid-run abort), and
//! the primary's terminal-dedup absorbs its terminal.

#![cfg(test)]

use super::super::test_helpers::{
    FakeWorkerFactory, election_config, make_secondary_recording_with_membership,
};
use super::super::*;
use super::processing::make_binary;
use crate::primary::wire::compute_task_hash;
use dynrunner_protocol_primary_secondary::DistributedMessage;

/// The roster entries (hash, worker_id, task identifier name) in the
/// recorded `InFlightRoster` reply, or `None` if no roster was emitted.
fn reported_roster(
    log: &std::rc::Rc<
        std::cell::RefCell<Vec<DistributedMessage<super::super::test_helpers::TestId>>>,
    >,
) -> Option<Vec<(String, u32, String)>> {
    log.borrow().iter().find_map(|m| match m {
        DistributedMessage::InFlightRoster { entries, .. } => Some(
            entries
                .iter()
                .map(|e| (e.hash.clone(), e.worker_id, e.task_id.0.clone()))
                .collect(),
        ),
        _ => None,
    })
}

/// The re-admitted member answers `RequestInFlightRoster` with the task
/// its worker is ACTUALLY running, read off `active_tasks` + the holding
/// worker's `current_binary`. A worker with no `current_binary` (a
/// transition window) contributes no entry.
#[tokio::test(flavor = "current_thread")]
async fn responder_reports_running_tasks_off_active_tasks() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut secondary, log, _membership) =
                make_secondary_recording_with_membership(election_config("sec-0"), 1);
            secondary.set_bootstrap_primary_id("setup".to_string());
            let mut factory = FakeWorkerFactory;
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;

            // Worker 0 is running a task: bind it in active_tasks AND set the
            // holding worker's current_binary (the identity source).
            let task = make_binary("running-task", 100);
            let hash = compute_task_hash(&task);
            secondary.active_tasks_mut().insert(hash.clone(), 0);
            secondary.pool_mut().workers[0].current_binary = Some(task);

            let request = DistributedMessage::RequestInFlightRoster {
                target: None,
                sender_id: "setup".into(),
                timestamp: 0.0,
            };
            secondary.handle_inbound(request, &mut FakeWorkerFactory).await;
            secondary.drain_egress().await;

            assert_eq!(
                reported_roster(&log),
                Some(vec![(hash, 0, "running-task".to_string())]),
                "the roster must name the task the worker is actually running"
            );
        })
        .await;
}

/// `WithdrawTask` for a copy still parked in `pending_first_bind` (not yet
/// dispatched to a worker) DROPS it cleanly — the cleanly-withdrawable
/// case.
#[tokio::test(flavor = "current_thread")]
async fn withdraw_drops_a_not_yet_started_deferred_copy() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut secondary, _log, _membership) =
                make_secondary_recording_with_membership(election_config("sec-1"), 1);
            secondary.set_bootstrap_primary_id("setup".to_string());
            let mut factory = FakeWorkerFactory;
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;

            // A duplicate copy parked as a first-bind deferral on worker 0.
            secondary.op_mut().pending_first_bind.insert(
                0,
                PendingFirstBind {
                    binary: make_binary("dup-task", 50),
                    file_hash: "h-dup".into(),
                    estimated: dynrunner_core::ResourceMap::new(),
                    predecessor_outputs: Default::default(),
                },
            );
            assert!(
                secondary.lifecycle.holding_worker("h-dup").is_some(),
                "fixture: the deferred copy is held bookkeeping"
            );

            let withdraw = DistributedMessage::WithdrawTask {
                target: None,
                sender_id: "setup".into(),
                timestamp: 0.0,
                secondary_id: "sec-1".into(),
                worker_id: 0,
                task_hash: "h-dup".into(),
            };
            secondary.handle_inbound(withdraw, &mut FakeWorkerFactory).await;

            assert!(
                !secondary.op_mut().pending_first_bind.contains_key(&0),
                "the not-yet-started deferred duplicate must be dropped"
            );
            assert!(
                secondary.lifecycle.holding_worker("h-dup").is_none(),
                "after withdraw the node no longer holds the duplicate"
            );
        })
        .await;
}

/// `WithdrawTask` for a copy ALREADY RUNNING on a worker (in
/// `active_tasks`) is LEFT IN PLACE — there is no mid-run worker abort, so
/// clearing its bookkeeping would orphan the worker's terminal. The
/// primary's terminal-dedup absorbs the eventual terminal instead.
#[tokio::test(flavor = "current_thread")]
async fn withdraw_leaves_an_already_running_copy_in_place() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut secondary, _log, _membership) =
                make_secondary_recording_with_membership(election_config("sec-1"), 1);
            secondary.set_bootstrap_primary_id("setup".to_string());
            let mut factory = FakeWorkerFactory;
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;

            // A duplicate copy already RUNNING on worker 0.
            let task = make_binary("running-dup", 100);
            let hash = compute_task_hash(&task);
            secondary.active_tasks_mut().insert(hash.clone(), 0);
            secondary.pool_mut().workers[0].current_binary = Some(task);

            let withdraw = DistributedMessage::WithdrawTask {
                target: None,
                sender_id: "setup".into(),
                timestamp: 0.0,
                secondary_id: "sec-1".into(),
                worker_id: 0,
                task_hash: hash.clone(),
            };
            secondary.handle_inbound(withdraw, &mut FakeWorkerFactory).await;

            // The running copy's bookkeeping is UNTOUCHED — it completes and
            // the primary's terminal-dedup absorbs its terminal.
            assert!(
                secondary.lifecycle.holding_worker(&hash).is_some(),
                "an already-running copy must NOT be torn down (no mid-run \
                 abort); its terminal is deduped at the primary"
            );
        })
        .await;
}

/// A `WithdrawTask` addressed to a DIFFERENT member is ignored (the
/// directed frame's `secondary_id` guard).
#[tokio::test(flavor = "current_thread")]
async fn withdraw_for_a_different_member_is_ignored() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut secondary, _log, _membership) =
                make_secondary_recording_with_membership(election_config("sec-1"), 1);
            secondary.set_bootstrap_primary_id("setup".to_string());
            let mut factory = FakeWorkerFactory;
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;

            secondary.op_mut().pending_first_bind.insert(
                0,
                PendingFirstBind {
                    binary: make_binary("dup-task", 50),
                    file_hash: "h-dup".into(),
                    estimated: dynrunner_core::ResourceMap::new(),
                    predecessor_outputs: Default::default(),
                },
            );

            // Addressed to sec-9, not this node (sec-1): no-op.
            let withdraw = DistributedMessage::WithdrawTask {
                target: None,
                sender_id: "setup".into(),
                timestamp: 0.0,
                secondary_id: "sec-9".into(),
                worker_id: 0,
                task_hash: "h-dup".into(),
            };
            secondary.handle_inbound(withdraw, &mut FakeWorkerFactory).await;

            assert!(
                secondary.op_mut().pending_first_bind.contains_key(&0),
                "a withdraw for a different member must not touch this node's \
                 deferrals"
            );
        })
        .await;
}
