//! Generation-aware worker-bind bookkeeping (ROOT fix for the
//! run-wedging orphan).
//!
//! A worker-replacement edge (type-shift respawn, OOM/disconnect
//! restart) installs a FRESH subprocess into the slot and bumps the
//! slot's monotonic `generation`. The prior subprocess's poll task can
//! leave a buffered TERMINAL on the pool's shared channel that
//! `abort_poll_task` cannot retract (the poll loop resolves
//! `poll_status` and `tx.send`s the terminal with no await between).
//! That stale event carries the OLD generation. Without a gate, the
//! secondary's terminal arms resolve the task by scanning `active_tasks`
//! for the event's `worker_id` — so the stale terminal mis-attributes
//! whatever the fresh subprocess was since bound to, orphaning the real
//! in-flight task and wedging the phase barrier.
//!
//! These tests pin the two behavioural changes:
//!   1. `handle_worker_event` drops any event whose generation != the
//!      slot's CURRENT (live handle's) generation; a current-generation
//!      event is processed normally.
//!   2. `sweep_replaced_worker_task` sweeps a still-bound `active_tasks`
//!      entry into the backpressure-shaped reinject path so a replaced
//!      generation can never strand a bound task.
//!   3. The abort-race shape: an OLD-generation buffered terminal that
//!      arrives AFTER the fresh generation's first-bind does NOT consume
//!      the new task's `active_tasks` entry.

use super::super::test_helpers::{FakeWorkerFactory, make_secondary, make_secondary_recording};
use super::super::*;
use super::processing::make_binary;
use dynrunner_core::{ResourceMap, TaskResult};
use dynrunner_manager_local::WorkerEvent;
use dynrunner_manager_local::oom::{OomWatcher, OomWatcherConfig};
use dynrunner_protocol_primary_secondary::DistributedMessage;
use std::time::Duration;

/// A disabled OOM watcher: the gate/sweep tests never exercise the
/// kernel-OOM reclassifier, so a flat-layout watcher (no workers cgroup
/// path) is sufficient — `kernel_oom_recent` always reads false.
pub(super) fn test_oom_watcher() -> OomWatcher {
    OomWatcher::new_with_workers_cgroup(
        OomWatcherConfig {
            sample_interval: Duration::from_millis(50),
            heartbeat_interval: Duration::from_secs(60),
            log_enabled: false,
        },
        None,
    )
}

/// Build a `SecondaryConfig` for a single-worker pool. The defaults are
/// the production-shaped values from `r1`'s dispatch test; only
/// `num_workers = 1` and the id matter for these direct-call tests.
pub(super) fn one_worker_config(secondary_id: &str) -> SecondaryConfig {
    SecondaryConfig {
        secondary_id: secondary_id.into(),
        num_workers: 1,
        max_resources: dynrunner_core::ResourceMap::from([(
            dynrunner_core::ResourceKind::memory(),
            1024 * 1024 * 1024,
        )]),
        hostname: "test-host".into(),
        keepalive_interval: Duration::from_secs(60),
        src_network: None,
        src_tmp: None,
        peer_timeout: Duration::from_secs(120),
        keepalive_miss_threshold: 3,
        retry_max_passes: 1,
        oom_retry_max_passes: 1,
        primary_link_failure_threshold: 5,
        primary_link_failure_window: Duration::from_secs(30),
        primary_silence_backstop: Duration::from_secs(120),
        unconfigured_deadline: Duration::from_secs(600),
        can_be_primary: false,
        resource_check_interval: Duration::from_millis(100),
        log_oom_watcher: false,
        promoted_primary_quiesce_grace: Duration::from_millis(100),
        unfulfillable_reinject_max_per_task: None,
        mem_manager_reserved_bytes: None,
        output_dir: None,
        memuse_log_path: None,
        forwarded_argv: Vec::new(),
    }
}

/// A current-generation TaskCompleted is processed: the `active_tasks`
/// entry for the worker is cleared and the success outcome is reported.
/// A stale-generation (gen N-1 against slot gen N) TaskCompleted is
/// DROPPED: `active_tasks` is left intact and NO outcome is reported.
#[tokio::test(flavor = "current_thread")]
async fn stale_generation_event_dropped_current_generation_processed() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut secondary, log) = make_secondary_recording(one_worker_config("sec-1"), 1);
            secondary.set_bootstrap_primary_id("setup".to_string());

            let mut factory = FakeWorkerFactory;
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;

            // Drive the slot to a known generation N by respawning it
            // twice through the real replacement edge. `initialize` lands
            // generation 0; two restarts bump it to 2.
            secondary
                .pool_mut()
                .restart_worker(0, &mut factory, false)
                .await
                .unwrap();
            secondary
                .pool_mut()
                .restart_worker(0, &mut factory, false)
                .await
                .unwrap();
            let current_gen = secondary.pool_mut().workers[0].generation;
            assert_eq!(current_gen, 2, "two restarts must bump generation 0 → 2");

            // Bind a task to the worker in `active_tasks`.
            let binary = make_binary("gated-task", 50);
            let file_hash = format!("hash_{}", binary.identifier.0);
            secondary
                .op_mut()
                .active_tasks
                .insert(file_hash.clone(), 0);

            let oom = test_oom_watcher();

            // STALE event: generation N-1. Must be dropped — active_tasks
            // untouched, nothing reported.
            secondary
                .handle_worker_event(
                    WorkerEvent::TaskCompleted {
                        worker_id: 0,
                        generation: current_gen - 1,
                        result: TaskResult::ok(),
                        result_data: None,
                        binary: Some(binary.clone()),
                        estimated_resources: ResourceMap::new(),
                    },
                    &oom,
                )
                .await
                .unwrap();
            secondary.drain_egress().await;
            assert!(
                secondary.op_mut().active_tasks.contains_key(&file_hash),
                "a stale-generation TaskCompleted must NOT consume the active_tasks entry"
            );
            assert!(
                log.borrow().is_empty(),
                "a stale-generation event must report nothing to the primary; got {:?}",
                log.borrow()
            );

            // CURRENT-generation event: generation N. Processed — the
            // entry is cleared and a TaskComplete is reported.
            secondary
                .handle_worker_event(
                    WorkerEvent::TaskCompleted {
                        worker_id: 0,
                        generation: current_gen,
                        result: TaskResult::ok(),
                        result_data: None,
                        binary: Some(binary),
                        estimated_resources: ResourceMap::new(),
                    },
                    &oom,
                )
                .await
                .unwrap();
            secondary.drain_egress().await;
            assert!(
                !secondary.op_mut().active_tasks.contains_key(&file_hash),
                "a current-generation TaskCompleted must clear the active_tasks entry"
            );
            let reported = log.borrow();
            assert!(
                reported
                    .iter()
                    .any(|m| matches!(m, DistributedMessage::TaskComplete { task_hash, .. } if *task_hash == file_hash)),
                "a current-generation completion must report TaskComplete for the hash; got {reported:?}"
            );
        })
        .await;
}

/// `sweep_replaced_worker_task` finds the task bound to a worker in
/// `active_tasks`, removes it, and reports a backpressure-shaped
/// TaskFailed (the reinject contract) so a replaced generation cannot
/// strand it. A worker with no bound task is a clean no-op.
#[tokio::test(flavor = "current_thread")]
async fn replacement_sweep_reinjects_bound_task_and_clears_active_tasks() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut secondary, log) = make_secondary_recording(one_worker_config("sec-1"), 1);
            secondary.set_bootstrap_primary_id("setup".to_string());

            let mut factory = FakeWorkerFactory;
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;

            // No-op case first: an unbound worker sweeps to nothing.
            secondary.sweep_replaced_worker_task(0).await.unwrap();
            secondary.drain_egress().await;
            assert!(
                log.borrow().is_empty(),
                "sweeping an unbound worker must report nothing; got {:?}",
                log.borrow()
            );

            // Bind a task, then sweep: the entry is removed AND a
            // backpressure-shaped TaskFailed is reported.
            let binary = make_binary("swept-task", 50);
            let file_hash = format!("hash_{}", binary.identifier.0);
            secondary
                .op_mut()
                .active_tasks
                .insert(file_hash.clone(), 0);

            secondary.sweep_replaced_worker_task(0).await.unwrap();
            secondary.drain_egress().await;

            assert!(
                !secondary.op_mut().active_tasks.contains_key(&file_hash),
                "sweep must remove the bound task's active_tasks entry"
            );
            let reported = log.borrow();
            assert!(
                reported.iter().any(|m| matches!(
                    m,
                    DistributedMessage::TaskFailed {
                        task_hash,
                        error_type: dynrunner_core::ErrorType::Recoverable,
                        ..
                    } if *task_hash == file_hash
                )),
                "sweep must report a backpressure-shaped (Recoverable) TaskFailed \
                 for the swept hash; got {reported:?}"
            );
        })
        .await;
}

/// The abort-race shape: a buffered terminal from the OLD generation
/// arrives AFTER the fresh generation has been bound to a NEW task. The
/// generation gate must drop the stale terminal so it does NOT consume
/// the new task's `active_tasks` entry (the precise wedge mechanism).
#[tokio::test(flavor = "current_thread")]
async fn buffered_old_generation_terminal_does_not_consume_new_task() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut secondary = make_secondary(one_worker_config("sec-1"));
            secondary.set_bootstrap_primary_id("setup".to_string());

            let mut factory = FakeWorkerFactory;
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;

            // The slot is respawned (type-shift / restart): generation
            // 0 → 1. The OLD generation 0's poll task buffered a terminal
            // for the task it WAS running.
            secondary
                .pool_mut()
                .restart_worker(0, &mut factory, false)
                .await
                .unwrap();
            let new_gen = secondary.pool_mut().workers[0].generation;
            assert_eq!(new_gen, 1);

            // The fresh generation has since been bound to a NEW task.
            let new_binary = make_binary("fresh-task", 50);
            let new_hash = format!("hash_{}", new_binary.identifier.0);
            secondary
                .op_mut()
                .active_tasks
                .insert(new_hash.clone(), 0);

            // The buffered OLD-generation (gen 0) terminal for the
            // ORIGINAL task lands now.
            let old_binary = make_binary("original-task", 50);
            let oom = test_oom_watcher();
            secondary
                .handle_worker_event(
                    WorkerEvent::TaskCompleted {
                        worker_id: 0,
                        generation: new_gen - 1,
                        result: TaskResult::ok(),
                        result_data: None,
                        binary: Some(old_binary),
                        estimated_resources: ResourceMap::new(),
                    },
                    &oom,
                )
                .await
                .unwrap();

            // The new task's entry MUST survive: the stale terminal was
            // dropped by the generation gate, not mis-attributed to it.
            assert!(
                secondary.op_mut().active_tasks.contains_key(&new_hash),
                "the buffered old-generation terminal must NOT consume the \
                 fresh generation's newly-bound task entry"
            );
        })
        .await;
}
