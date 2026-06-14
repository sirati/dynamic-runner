//! #517 secondary honor-or-bounce: a `TaskAssignment` for a NON-idle
//! worker must bounce `IllegallyAssignedToNonidleWorker` (naming the
//! incumbent) and NEVER re-pick another worker, NEVER be accounted as a
//! failure.
//!
//! Production interleaving replayed at the wire level: worker 0 is busy
//! running task `H1`; the primary (its occupancy model diverged) assigns
//! a DIFFERENT task `H2` at the SAME worker 0. Pre-#517 the router fell
//! back to ANY idle worker — running `H2` on a sibling slot the primary
//! never tracked (the occupancy-drift root). Post-#517 the secondary
//! honors the assigned `worker_id`: idle ⇒ dispatch there; not idle ⇒
//! bounce the typed report so the authority reconciles + requeues.

#![cfg(test)]

use super::super::test_helpers::{FakeWorkerFactory, make_secondary_recording};
use super::super::*;
use super::firstbind_orphan::{
    drive_to_post_ready_assigned, one_worker_config, task_assignment, test_oom_watcher,
};
use super::processing::make_binary;
use dynrunner_protocol_primary_secondary::DistributedMessage;

/// `n`-worker fixture (the multi-slot shape the re-pick would have abused).
fn n_worker_config(secondary_id: &str, n: u32) -> SecondaryConfig {
    SecondaryConfig {
        num_workers: n,
        ..one_worker_config(secondary_id)
    }
}

/// Every `IllegallyAssignedToNonidleWorker` bounce recorded, as
/// `(worker_id, assigned_hash, incumbent_hash)`.
fn illegal_bounces(
    log: &std::rc::Rc<std::cell::RefCell<Vec<DistributedMessage<test_helpers::TestId>>>>,
) -> Vec<(u32, String, Option<String>)> {
    log.borrow()
        .iter()
        .filter_map(|m| match m {
            DistributedMessage::IllegallyAssignedToNonidleWorker {
                worker_id,
                assigned,
                incumbent,
                ..
            } => Some((
                *worker_id,
                assigned.hash.clone(),
                incumbent.as_ref().map(|i| i.hash.clone()),
            )),
            _ => None,
        })
        .collect()
}

/// Any `TaskFailed` recorded for `hash` (must be EMPTY — the bounce is
/// NOT a failure).
fn task_failed_count(
    log: &std::rc::Rc<std::cell::RefCell<Vec<DistributedMessage<test_helpers::TestId>>>>,
    hash: &str,
) -> usize {
    log.borrow()
        .iter()
        .filter(|m| {
            matches!(m, DistributedMessage::TaskFailed { task_hash, .. } if task_hash == hash)
        })
        .count()
}

/// TEST (a) + REVERT-CHECK of the re-pick: a DISTINCT-hash assignment for
/// a busy worker, with an idle sibling present. Pre-#517 the fallback
/// re-picked the idle sibling (ran the task on a worker the primary never
/// tracked). Post-#517: a single `IllegallyAssignedToNonidleWorker`
/// naming the incumbent, the idle sibling UNTOUCHED, and NO `TaskFailed`.
#[tokio::test(flavor = "current_thread")]
async fn busy_worker_assignment_bounces_illegal_never_repicks_or_fails() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Two workers so a re-pick WOULD have a sibling to land on.
            let (mut secondary, log) = make_secondary_recording(n_worker_config("sec-0", 2), 1);
            secondary.set_bootstrap_primary_id("setup".to_string());

            let mut factory = FakeWorkerFactory;
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;

            // Worker 0 busy running the INCUMBENT; worker 1 idle.
            let oom = test_oom_watcher();
            let incumbent = make_binary("incumbent", 50);
            let incumbent_hash = "111aaa".to_string();
            drive_to_post_ready_assigned(&mut secondary, &oom, &incumbent, &incumbent_hash).await;
            assert!(
                secondary.op_mut().pool.workers[1].is_idle_state(),
                "fixture precondition: the sibling worker is idle"
            );
            log.borrow_mut().clear();

            // The primary assigns a DIFFERENT task at the SAME busy worker 0.
            let assigned = make_binary("assigned", 50);
            let assigned_hash = "222bbb".to_string();
            let dup = task_assignment("setup", "sec-0", 0, &assigned, &assigned_hash);
            secondary
                .handle_inbound(dup, &mut FakeWorkerFactory)
                .await;
            secondary.drain_egress().await;

            // NO re-pick: the idle sibling is untouched (no second copy ran,
            // nothing stashed for its first-bind), and the incumbent's
            // bookkeeping is intact.
            assert!(
                secondary.op_mut().pending_first_bind.is_empty(),
                "the illegal assignment must NOT be stashed onto the idle \
                 sibling (the pre-#517 re-pick); got {:?}",
                secondary
                    .op_mut()
                    .pending_first_bind
                    .iter()
                    .map(|(w, p)| (*w, p.file_hash.clone()))
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                secondary.op_mut().active_tasks.get(&incumbent_hash),
                Some(&0u32),
                "the incumbent must still be tracked on worker 0"
            );
            assert!(
                !secondary.op_mut().active_tasks.contains_key(&assigned_hash),
                "the illegally-assigned task must NEVER be recorded as running here"
            );

            // Exactly one typed bounce, naming worker 0 + the incumbent; and
            // NO TaskFailed for the bounced hash (it is not a failure).
            assert_eq!(
                illegal_bounces(&log),
                vec![(0, assigned_hash.clone(), Some(incumbent_hash.clone()))],
                "a busy-worker assignment must bounce exactly one \
                 IllegallyAssignedToNonidleWorker naming the incumbent"
            );
            assert_eq!(
                task_failed_count(&log, &assigned_hash),
                0,
                "the bounce must NOT be a TaskFailed (no failure accounting)"
            );
        })
        .await;
}

/// TEST (e): an OUT-OF-RANGE `worker_id` on a running pool. The `.get()`
/// Option path must NOT panic / clamp (the cabd34ab safety) — it bounces
/// with NO incumbent (the slot does not exist), and never fails the task.
#[tokio::test(flavor = "current_thread")]
async fn out_of_range_worker_id_bounces_no_incumbent_no_panic() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // A single-worker pool; worker id 99 is out of range.
            let (mut secondary, log) = make_secondary_recording(one_worker_config("sec-0"), 1);
            secondary.set_bootstrap_primary_id("setup".to_string());

            let mut factory = FakeWorkerFactory;
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;
            log.borrow_mut().clear();

            let assigned = make_binary("oor", 50);
            let assigned_hash = "333ccc".to_string();
            let oor = task_assignment("setup", "sec-0", 99, &assigned, &assigned_hash);
            // Must not panic.
            secondary
                .handle_inbound(oor, &mut FakeWorkerFactory)
                .await;
            secondary.drain_egress().await;

            assert_eq!(
                illegal_bounces(&log),
                vec![(99, assigned_hash.clone(), None)],
                "an out-of-range worker_id must bounce IllegallyAssignedToNonidleWorker \
                 with NO incumbent — never a panic, never a clamp onto the last slot"
            );
            assert_eq!(
                task_failed_count(&log, &assigned_hash),
                0,
                "the out-of-range bounce must NOT be a TaskFailed"
            );
        })
        .await;
}
