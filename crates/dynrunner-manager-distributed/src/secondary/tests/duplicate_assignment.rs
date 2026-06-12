//! Duplicate-assignment recognition — the secondary half of the
//! post-failover assign loop (promoted primary P-secondary-1
//! re-assigning in-flight hashes onto secondary-10's busy workers every
//! ~1.5-2s, indefinitely).
//!
//! Production interleaving replayed here VERBATIM at the wire level:
//! a worker of this secondary is ALREADY EXECUTING task `H` (it was
//! in-flight when the original primary died); the promoted primary —
//! whose replicated ledger lost the `InFlight` fact — re-dispatches a
//! `TaskAssignment` for the SAME `H` at the SAME worker. Pre-fix the
//! router had no already-running recognition, so the frame fell into
//! the generic idle-target selection:
//!
//!   * no idle worker → the generic "No idle worker available"
//!     backpressure bounce, which the primary classifies as requeue →
//!     re-assign → bounce → … the indefinite loop;
//!   * an idle worker present → WORSE: the fallback DOUBLE-RAN `H` on
//!     this same node, and `active_tasks.insert(H, other_wid)`
//!     CLOBBERED the running copy's bookkeeping entry.
//!
//! Post-fix the router consults the live own-worker bookkeeping FIRST
//! (`SecondaryLifecycle::holding_worker` — the same single truth source
//! the #308 probe responder answers from) and replies with the
//! `TASK_ALREADY_HELD_WIRE_MESSAGE` coherence report naming the REAL
//! holding worker: no bounce, no double-run, no bookkeeping mutation.
//! The primary keeps the task in flight on this holder and the
//! eventual real terminal settles it.

#![cfg(test)]

use super::super::dispatch::TASK_ALREADY_HELD_WIRE_MESSAGE;
use super::super::test_helpers::{FakeWorkerFactory, make_secondary_recording};
use super::super::*;
use super::firstbind_orphan::{
    drive_to_post_ready_assigned, one_worker_config, task_assignment, test_oom_watcher,
};
use super::processing::make_binary;
use dynrunner_protocol_primary_secondary::DistributedMessage;

/// Like [`one_worker_config`] but with `n` workers — the multi-slot
/// fixture the double-run fallback shape needs.
fn n_worker_config(secondary_id: &str, n: u32) -> SecondaryConfig {
    SecondaryConfig {
        num_workers: n,
        ..one_worker_config(secondary_id)
    }
}

/// Every `TaskFailed` reply recorded for `hash`, as
/// `(worker_id, error_message)` pairs.
fn failed_replies_for(
    log: &std::rc::Rc<std::cell::RefCell<Vec<DistributedMessage<test_helpers::TestId>>>>,
    hash: &str,
) -> Vec<(u32, String)> {
    log.borrow()
        .iter()
        .filter_map(|m| match m {
            DistributedMessage::TaskFailed {
                task_hash,
                worker_id,
                error_message,
                ..
            } if task_hash == hash => Some((*worker_id, error_message.clone())),
            _ => None,
        })
        .collect()
}

/// THE production bounce leg: the duplicate assignment lands while the
/// holding worker is busy running the hash and NO other worker is idle.
/// Pre-fix reply: the generic "No idle worker available" backpressure
/// bounce — the wire frame that sustains the assign → bounce → requeue
/// loop. Post-fix: exactly one already-held coherence report naming the
/// REAL holding worker, the running copy's bookkeeping untouched.
#[tokio::test(flavor = "current_thread")]
async fn duplicate_assignment_for_running_hash_replies_already_held_not_backpressure() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut secondary, log) = make_secondary_recording(one_worker_config("sec-10"), 1);
            secondary.set_bootstrap_primary_id("setup".to_string());

            let mut factory = FakeWorkerFactory;
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;

            // Worker 0 is genuinely RUNNING the task (the pre-failover
            // in-flight state at promotion time).
            let oom = test_oom_watcher();
            let binary = make_binary("inflight-task", 50);
            let file_hash = "877a141f86d8d2c5".to_string();
            drive_to_post_ready_assigned(&mut secondary, &oom, &binary, &file_hash).await;
            // The first (legitimate) dispatch is in the log; observe only
            // frames from the duplicate onward.
            log.borrow_mut().clear();

            // The promoted primary re-assigns the SAME hash at the SAME
            // worker (its hydrated ledger lost the InFlight fact, so the
            // hash sat Pending in its pool and the slot reconstructed
            // idle).
            let duplicate = task_assignment("setup", "sec-10", 0, &binary, &file_hash);
            secondary
                .handle_inbound(duplicate, &mut FakeWorkerFactory)
                .await;
            secondary.drain_egress().await;

            // The running copy's bookkeeping is untouched.
            assert_eq!(
                secondary.op_mut().active_tasks.get(&file_hash),
                Some(&0u32),
                "the duplicate assignment must not touch the running copy's \
                 active_tasks entry"
            );

            let replies = failed_replies_for(&log, &file_hash);
            assert!(
                !replies
                    .iter()
                    .any(|(_, msg)| msg == "No idle worker available"),
                "an assignment for a hash this node is ALREADY RUNNING must \
                 never be answered with the generic backpressure bounce (the \
                 primary requeues on it → the indefinite assign/bounce loop); \
                 got {replies:?}"
            );
            assert_eq!(
                replies,
                vec![(0, TASK_ALREADY_HELD_WIRE_MESSAGE.to_string())],
                "the duplicate must be answered with exactly one already-held \
                 coherence report naming the REAL holding worker"
            );
        })
        .await;
}

/// THE double-run fallback leg: the duplicate assignment lands while an
/// idle worker EXISTS next to the busy holder. Pre-fix the idle-target
/// fallback started a SECOND copy of the hash on worker 1 (a fresh slot
/// ⇒ a first-bind stash for the SAME hash) — re-running in-flight work
/// and setting up the `active_tasks` clobber. Post-fix: the already-held
/// reply, the idle worker untouched.
#[tokio::test(flavor = "current_thread")]
async fn duplicate_assignment_never_double_runs_on_an_idle_sibling_worker() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut secondary, log) = make_secondary_recording(n_worker_config("sec-10", 2), 1);
            secondary.set_bootstrap_primary_id("setup".to_string());

            let mut factory = FakeWorkerFactory;
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;

            // Worker 0 busy running the hash; worker 1 idle.
            let oom = test_oom_watcher();
            let binary = make_binary("inflight-task", 50);
            let file_hash = "35a81d5750585ccc".to_string();
            drive_to_post_ready_assigned(&mut secondary, &oom, &binary, &file_hash).await;
            assert!(
                secondary.op_mut().pool.workers[1].is_idle_state(),
                "fixture precondition: the sibling worker is idle"
            );
            log.borrow_mut().clear();

            let duplicate = task_assignment("setup", "sec-10", 0, &binary, &file_hash);
            secondary
                .handle_inbound(duplicate, &mut FakeWorkerFactory)
                .await;
            secondary.drain_egress().await;

            // No second copy anywhere: the running entry still points at
            // worker 0 (not clobbered onto worker 1), and nothing was
            // stashed for the idle sibling's first-bind.
            assert_eq!(
                secondary.op_mut().active_tasks.get(&file_hash),
                Some(&0u32),
                "the duplicate must not clobber the running copy's \
                 active_tasks entry onto the idle sibling"
            );
            assert!(
                secondary.op_mut().pending_first_bind.is_empty(),
                "the duplicate must not be stashed for the idle sibling's \
                 first-bind (that DOUBLE-RUNS the in-flight hash); got {:?}",
                secondary
                    .op_mut()
                    .pending_first_bind
                    .iter()
                    .map(|(wid, p)| (*wid, p.file_hash.clone()))
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                failed_replies_for(&log, &file_hash),
                vec![(0, TASK_ALREADY_HELD_WIRE_MESSAGE.to_string())],
                "the duplicate must be answered with the already-held report"
            );
        })
        .await;
}

/// A hash parked in `pending_first_bind` (respawn-HOLD) is HELD live
/// bookkeeping — the same contract the #308 probe responder answers
/// `held = true` for. A duplicate assignment for it must get the
/// already-held reply (naming the parking worker) and leave the stash
/// exactly as it was — not re-stash, not bounce.
#[tokio::test(flavor = "current_thread")]
async fn duplicate_assignment_for_first_bind_parked_hash_replies_already_held() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut secondary, log) = make_secondary_recording(one_worker_config("sec-10"), 1);
            secondary.set_bootstrap_primary_id("setup".to_string());

            let mut factory = FakeWorkerFactory;
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;

            // First assignment parks the hash in pending_first_bind
            // (RespawnInProgress); the Ready is deliberately NOT pumped.
            let binary = make_binary("parked-task", 50);
            let file_hash = "2480a49184af7ba1".to_string();
            let assignment = task_assignment("setup", "sec-10", 0, &binary, &file_hash);
            secondary
                .handle_inbound(assignment, &mut FakeWorkerFactory)
                .await;
            assert!(
                secondary.op_mut().pending_first_bind.contains_key(&0),
                "fixture precondition: the hash is parked in pending_first_bind"
            );
            log.borrow_mut().clear();

            let duplicate = task_assignment("setup", "sec-10", 0, &binary, &file_hash);
            secondary
                .handle_inbound(duplicate, &mut FakeWorkerFactory)
                .await;
            secondary.drain_egress().await;

            // The stash is untouched: still exactly the one parked copy.
            let stashed: Vec<(u32, String)> = secondary
                .op_mut()
                .pending_first_bind
                .iter()
                .map(|(wid, p)| (*wid, p.file_hash.clone()))
                .collect();
            assert_eq!(
                stashed,
                vec![(0, file_hash.clone())],
                "the duplicate must leave the parked copy exactly as it was"
            );
            assert_eq!(
                failed_replies_for(&log, &file_hash),
                vec![(0, TASK_ALREADY_HELD_WIRE_MESSAGE.to_string())],
                "the duplicate for a parked hash must be answered with the \
                 already-held report naming the parking worker"
            );
        })
        .await;
}
