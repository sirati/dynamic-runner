//! Pre-Ready worker self-exit must CHARGE the deferred task's retry
//! budget; an evidence-free pre-Ready loss must NOT.
//!
//! Production replay (asm-tokenizer run_20260612_095601): every
//! memmap-typed worker raised at consumer arg-validation inside the
//! framework runtime's `on_args` (before the protocol loop), so the
//! subprocess EXITED WITH CODE 1 before `Ready` — pipe EOF, the
//! `Recoverable + "Disconnected before Ready"` synthesis, exit status
//! "exited with code 1". Pre-fix, the deferred first-bind task was
//! reinjected via the BACKPRESSURE shape ("worker pipe broken;
//! respawning"), so the authority requeued it without consuming retry
//! budget: one hash re-dispatched 24,323 times, fail counters flat at
//! zero, no termination.
//!
//! The contract pinned here, replaying that trace through the REAL
//! first-bind dispatch + `handle_worker_event` Disconnected arm:
//!   1. a pre-Ready death whose reaped exit status is a NONZERO
//!      SELF-EXIT reports the deferred task as a COUNTED terminal
//!      (`TaskFailed` with a real error message — never the
//!      backpressure marker), and
//!   2. the same death WITHOUT process-fault evidence (no reapable
//!      exit status) keeps the historical uncharged backpressure
//!      reinject — a task is not at fault for its environment dying.

#![cfg(test)]

use super::super::test_helpers::{FakeWorkerFactory, make_secondary_recording};
use super::firstbind_orphan::{one_worker_config, task_assignment, test_oom_watcher};
use super::processing::make_binary;
use dynrunner_core::{ErrorType, TaskResult};
use dynrunner_manager_local::WorkerEvent;
use dynrunner_protocol_primary_secondary::DistributedMessage;
use std::time::Duration;

/// Spawn a real `sh -c "exit 1"` child and wait for it to die WITHOUT
/// reaping it (no `wait`/`try_wait` — the framework's own
/// `try_reap_exit` must be the one to collect the exit status, exactly
/// as in production). The sleep comfortably covers the few ms `sh`
/// needs; the reaper's own WNOHANG retry budget covers the residue.
async fn spawn_dead_child_exit_1() -> u32 {
    let child = std::process::Command::new("sh")
        .arg("-c")
        .arg("exit 1")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn sh");
    let pid = child.id();
    // Dropping the handle neither kills nor reaps the child; the test
    // process stays its parent so `waitpid` works from `try_reap_exit`.
    std::mem::forget(child);
    tokio::time::sleep(Duration::from_millis(150)).await;
    pid
}

/// Drive the shared repro shape: first-bind dispatch stashes the task,
/// then the (current-generation) pre-Ready `Disconnected` lands with
/// `with_pid` controlling whether the framework can reap the worker's
/// real nonzero exit status. Returns the recorded primary-bound
/// `TaskFailed` for the task hash.
async fn drive_pre_ready_death(with_pid: bool) -> (String, ErrorType, String) {
    let (mut secondary, log) = make_secondary_recording(one_worker_config("sec-2"), 1);
    secondary.set_bootstrap_primary_id("setup".to_string());

    let mut factory = FakeWorkerFactory;
    let pool = secondary.initialize_workers(&mut factory).await.unwrap();
    secondary.enter_operational_for_test();
    *secondary.pool_mut() = pool;

    // First-bind dispatch: RespawnInProgress stashes the binary in
    // pending_first_bind (NOT active_tasks) — the production shape.
    let binary = make_binary("memmap-task", 50);
    let file_hash = "0a7ea4942c59818b".to_string();
    let assignment = task_assignment("setup", "sec-2", 0, &binary, &file_hash);
    secondary
        .handle_inbound(assignment, &mut FakeWorkerFactory)
        .await;
    assert!(
        secondary.op_mut().pending_first_bind.contains_key(&0),
        "first-bind must stash the binary in pending_first_bind"
    );

    // The worker subprocess dies at startup: consumer arg-validation
    // raised inside the runtime's `on_args`, unwinding the process
    // with exit 1 BEFORE the protocol loop — no Ready ever arrives.
    if with_pid {
        let pid = spawn_dead_child_exit_1().await;
        secondary.op_mut().pool.workers[0].pid = Some(pid);
    }
    let oom = test_oom_watcher();
    let current_gen = secondary.op_mut().pool.workers[0].generation;
    secondary
        .handle_worker_event(
            WorkerEvent::Disconnected {
                worker_id: 0,
                generation: current_gen,
                // worker.rs synthesises Recoverable + "Disconnected
                // before Ready" for the pre-Ready EOF.
                result: TaskResult::error(
                    ErrorType::Recoverable,
                    "Disconnected before Ready".to_string(),
                ),
                binary: None,
            },
            &oom,
            &mut FakeWorkerFactory,
        )
        .await
        .unwrap();
    secondary.drain_egress().await;

    // The stash must be resolved either way — never stranded.
    assert!(
        !secondary.op_mut().pending_first_bind.contains_key(&0),
        "the pre-Ready death must drain the deferred stash"
    );

    let reported = log.borrow();
    let (error_type, error_message) = reported
        .iter()
        .find_map(|m| match m {
            DistributedMessage::TaskFailed {
                task_hash,
                error_type,
                error_message,
                ..
            } if *task_hash == file_hash => Some((error_type.clone(), error_message.clone())),
            _ => None,
        })
        .expect("the deferred task must be reported to the primary as TaskFailed");
    (file_hash, error_type, error_message)
}

/// THE repro: a reaped NONZERO SELF-EXIT before Ready is an
/// executed-and-failed attempt of the deferred task — the report must
/// be a COUNTED terminal (a real error message the authority's
/// `is_backpressure` predicate does NOT recognise), so the
/// `failed_tasks` → retry-bucket → permanence accounting sees it.
#[tokio::test(flavor = "current_thread")]
async fn pre_ready_self_exit_reports_a_counted_task_failure() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (_hash, error_type, error_message) = drive_pre_ready_death(true).await;
            assert_eq!(
                error_type,
                ErrorType::Recoverable,
                "a nonzero self-exit charges the standard retry-pass budget"
            );
            assert_ne!(
                error_message, "worker pipe broken; respawning",
                "a charged failure must NEVER ride the backpressure marker \
                 (that shape requeues without consuming retry budget — the \
                 24,323-redispatch production bug)"
            );
            assert!(
                error_message.contains("exited with code 1"),
                "the report must carry the death diagnosis; got {error_message:?}"
            );
        })
        .await;
}

/// The infra distinction: the SAME pre-Ready death with NO reapable
/// exit status (framework lost diagnostic visibility — indistinguishable
/// from an environment loss) keeps the uncharged backpressure reinject.
#[tokio::test(flavor = "current_thread")]
async fn pre_ready_loss_without_fault_evidence_stays_uncharged_backpressure() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (_hash, error_type, error_message) = drive_pre_ready_death(false).await;
            assert_eq!(error_type, ErrorType::Recoverable);
            assert_eq!(
                error_message, "worker pipe broken; respawning",
                "an evidence-free loss requeues via the backpressure shape \
                 (no retry budget consumed)"
            );
        })
        .await;
}

/// Mid-task twin: a worker bound to a RUNNING task that self-exits
/// nonzero (pipe EOF before any wire error) must report a COUNTED
/// terminal for that task, not the backpressure requeue.
#[tokio::test(flavor = "current_thread")]
async fn mid_task_self_exit_reports_a_counted_task_failure() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut secondary, log) = make_secondary_recording(one_worker_config("sec-2"), 1);
            secondary.set_bootstrap_primary_id("setup".to_string());

            let mut factory = FakeWorkerFactory;
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;

            // First-bind through the real path, then consume the fresh
            // subprocess's Ready so the deferred task BINDS
            // (active_tasks) — the mid-task shape.
            let binary = make_binary("midtask", 50);
            let file_hash = "c8c632f3ac37178b".to_string();
            let assignment = task_assignment("setup", "sec-2", 0, &binary, &file_hash);
            secondary
                .handle_inbound(assignment, &mut FakeWorkerFactory)
                .await;
            let oom = test_oom_watcher();
            let ready = secondary
                .op_mut()
                .pool
                .recv_event()
                .await
                .expect("fresh subprocess must emit a Ready event");
            secondary.handle_worker_event(ready, &oom, &mut FakeWorkerFactory).await.unwrap();
            assert!(
                secondary.op_mut().active_tasks.contains_key(&file_hash),
                "Ready arm must bind the deferred task into active_tasks"
            );

            // The worker self-exits nonzero MID-TASK: pure transport
            // EOF (the state.rs `Recoverable + "transport disconnected"`
            // synthesis), with the real exit status reapable.
            let pid = spawn_dead_child_exit_1().await;
            secondary.op_mut().pool.workers[0].pid = Some(pid);
            let current_gen = secondary.op_mut().pool.workers[0].generation;
            secondary
                .handle_worker_event(
                    WorkerEvent::Disconnected {
                        worker_id: 0,
                        generation: current_gen,
                        result: TaskResult::error(
                            ErrorType::Recoverable,
                            "transport disconnected".to_string(),
                        ),
                        binary: Some(binary.clone()),
                    },
                    &oom,
                    &mut FakeWorkerFactory,
                )
                .await
                .unwrap();
            secondary.drain_egress().await;

            let reported = log.borrow();
            let (error_type, error_message) = reported
                .iter()
                .find_map(|m| match m {
                    DistributedMessage::TaskFailed {
                        task_hash,
                        error_type,
                        error_message,
                        ..
                    } if *task_hash == file_hash => {
                        Some((error_type.clone(), error_message.clone()))
                    }
                    _ => None,
                })
                .expect("the mid-task death must be reported as TaskFailed");
            assert_eq!(error_type, ErrorType::Recoverable);
            assert_ne!(
                error_message, "worker pipe broken; respawning",
                "a reaped nonzero self-exit mid-task is executed-and-failed; \
                 it must charge the budget, not requeue as backpressure"
            );
        })
        .await;
}
