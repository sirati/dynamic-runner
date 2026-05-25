//! Tests for the local manager. Fixtures live in
//! `super::test_helpers`; this file holds the test scenarios.

use super::test_helpers::{
    make_binary, test_config, FakeWorkerFactory, FakeWorkerMode,
    FixedEstimator, TestId,
};
use super::*;
use dynrunner_core::{ErrorType, MessageReceiver, MessageSender, ResourceKind, ResourceMap};
use dynrunner_protocol_manager_worker::{Command, Response};
use dynrunner_scheduler::ResourceStealingScheduler;
use dynrunner_transport_channel::{channel_pair, ChannelManagerEnd};
use std::collections::HashMap;


#[tokio::test(flavor = "current_thread")]
async fn single_worker_processes_all_binaries() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let config = test_config(1);
        let mut manager = LocalManager::new(config, ResourceStealingScheduler::memory(), FixedEstimator(100));
        let mut factory = FakeWorkerFactory {
            mode: FakeWorkerMode::AlwaysSucceed,
        };

        let binaries = vec![
            make_binary("a", 50),
            make_binary("b", 60),
            make_binary("c", 70),
        ];

        manager
            .process_binaries(
                binaries,
                std::collections::HashMap::new(),
                |_phase| {},
                |_phase, _completed, _failed| {},
                &mut factory,
            )
            .await
            .unwrap();

        assert_eq!(manager.stats().completed, 3);
        assert_eq!(manager.stats().total, 3);
        assert!(manager.failed_tasks().is_empty());
        assert!(manager.resource_pressure_tasks().is_empty());
    }).await;
}

#[tokio::test(flavor = "current_thread")]
async fn multiple_workers_process_binaries() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let config = test_config(3);
        let mut manager =
            LocalManager::new(config, ResourceStealingScheduler::memory(), FixedEstimator(100));
        let mut factory = FakeWorkerFactory {
            mode: FakeWorkerMode::AlwaysSucceed,
        };

        let binaries: Vec<TaskInfo<TestId>> = (0..10)
            .map(|i| make_binary(&format!("bin_{i}"), 100))
            .collect();

        manager
            .process_binaries(
                binaries,
                std::collections::HashMap::new(),
                |_phase| {},
                |_phase, _completed, _failed| {},
                &mut factory,
            )
            .await
            .unwrap();

        assert_eq!(manager.stats().completed, 10);
        assert!(manager.failed_tasks().is_empty());
    }).await;
}

#[tokio::test(flavor = "current_thread")]
async fn retry_phase_retries_failed_tasks() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let config = test_config(1);
        let mut manager =
            LocalManager::new(config, ResourceStealingScheduler::memory(), FixedEstimator(100));
        let mut factory = FakeWorkerFactory {
            mode: FakeWorkerMode::FailThenSucceed,
        };

        let binaries = vec![make_binary("retry_me", 50)];
        manager
            .process_binaries(
                binaries,
                std::collections::HashMap::new(),
                |_phase| {},
                |_phase, _completed, _failed| {},
                &mut factory,
            )
            .await
            .unwrap();

        // First attempt fails, retry succeeds
        assert_eq!(manager.stats().completed, 1);
        assert!(manager.failed_tasks().is_empty());
    }).await;
}

#[tokio::test(flavor = "current_thread")]
async fn resource_pressure_tasks_collected() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let config = test_config(1);
        let mut manager =
            LocalManager::new(config, ResourceStealingScheduler::memory(), FixedEstimator(100));
        let mut factory = FakeWorkerFactory {
            mode: FakeWorkerMode::AlwaysOom,
        };

        let binaries = vec![make_binary("oom_bin", 50)];
        manager
            .process_binaries(
                binaries,
                std::collections::HashMap::new(),
                |_phase| {},
                |_phase, _completed, _failed| {},
                &mut factory,
            )
            .await
            .unwrap();

        // OOM in main → retry → OOM again → OOM phase → OOM again
        // Eventually ends up in resource_pressure_tasks or failed_tasks
        assert_eq!(manager.stats().completed, 0);
    }).await;
}

#[tokio::test(flavor = "current_thread")]
async fn no_binaries_completes_immediately() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let config = test_config(1);
        let mut manager =
            LocalManager::new(config, ResourceStealingScheduler::memory(), FixedEstimator(100));
        let mut factory = FakeWorkerFactory {
            mode: FakeWorkerMode::AlwaysSucceed,
        };

        manager
            .process_binaries(
                Vec::<TaskInfo<TestId>>::new(),
                std::collections::HashMap::new(),
                |_phase| {},
                |_phase, _completed, _failed| {},
                &mut factory,
            )
            .await
            .unwrap();

        assert_eq!(manager.stats().completed, 0);
        assert_eq!(manager.stats().total, 0);
    }).await;
}

#[tokio::test(flavor = "current_thread")]
async fn always_restart_worker_respawns_after_success() {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    struct CountingFactory {
        spawn_count: Arc<AtomicU32>,
    }

    impl WorkerFactory<ChannelManagerEnd> for CountingFactory {
        fn spawn_worker(
            &mut self,
            _worker_id: WorkerId,
            _subcgroup: Option<&crate::cgroup::SubcgroupHandle>,
        ) -> Result<(ChannelManagerEnd, Option<u32>), String> {
            self.spawn_count.fetch_add(1, Ordering::SeqCst);
            let (manager_end, runner_end) = channel_pair();
            tokio::task::spawn_local(async move {
                let mut runner = runner_end;
                let _ = runner.send(Response::Ready).await;
                loop {
                    match MessageReceiver::<Command>::recv(&mut runner).await {
                        Some(Command::Stop) => break,
                        Some(Command::ProcessTask { .. }) => {
                            let _ = runner
                                .send(Response::Done {
                                    result_data: None,
                                })
                                .await;
                        }
                        None => break,
                    }
                }
            });
            Ok((manager_end, Some(42)))
        }
    }

    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let spawn_count = Arc::new(AtomicU32::new(0));
        let spawn_count_clone = spawn_count.clone();

        let mut config = test_config(1);
        config.always_restart_worker = true;

        let mut manager = LocalManager::new(config, ResourceStealingScheduler::memory(), FixedEstimator(100));
        let mut factory = CountingFactory {
            spawn_count: spawn_count_clone,
        };

        let binaries = vec![
            make_binary("a", 50),
            make_binary("b", 60),
            make_binary("c", 70),
        ];

        manager
            .process_binaries(
                binaries,
                std::collections::HashMap::new(),
                |_phase| {},
                |_phase, _completed, _failed| {},
                &mut factory,
            )
            .await
            .unwrap();

        assert_eq!(manager.stats().completed, 3);
        assert_eq!(manager.stats().total, 3);
        assert!(manager.failed_tasks().is_empty());

        // With always_restart_worker=true and 3 binaries with 1 worker:
        // 1 initial spawn + 1 type-shift respawn (worker's loaded_type_id
        // starts None; `ensure_worker_for_type` cannot prove the factory
        // chose the right type so it respawns once to bind the slot)
        // + 2 restarts (after "a" and "b" complete; "c" is last → no
        // restart). The post-respawn `loaded_type_id` is preserved
        // across `restart_worker`, so subsequent same-type tasks hit
        // the no-op fast path inside `ensure_worker_for_type`.
        let spawns = spawn_count.load(Ordering::SeqCst);
        assert_eq!(
            spawns, 4,
            "expected 4 spawns (1 initial + 1 first-task type-bind + 2 restarts), got {spawns}"
        );
    }).await;
}

#[tokio::test(flavor = "current_thread")]
async fn memuse_log_written() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let tmp_dir = std::env::temp_dir().join("rust_memuse_test");
        let _ = std::fs::create_dir_all(&tmp_dir);
        let memuse_path = tmp_dir.join("memuse.log");
        // Clean up any previous run
        let _ = std::fs::remove_file(&memuse_path);

        let config = LocalManagerConfig {
            num_workers: 1,
            max_resources: ResourceMap::from([(ResourceKind::memory(), 1024 * 1024 * 1024)]),
            always_restart_worker: false,
            restart_predicate: None,
            retry_max_attempts: 1,
            print_pid: false,
            memuse_log_path: Some(memuse_path.clone()),
            stage_timeouts: HashMap::new(),
            low_resource_thresholds: ResourceMap::from([(ResourceKind::memory(), 300 * 1024 * 1024)]),
            resource_check_interval: std::time::Duration::from_millis(100),
            phase_status_log_intervals: Vec::new(),
            log_oom_watcher: false,
            output_dir: None,
        };

        let mut manager = LocalManager::new(config, ResourceStealingScheduler::memory(), FixedEstimator(100));
        let mut factory = FakeWorkerFactory {
            mode: FakeWorkerMode::AlwaysSucceed,
        };

        let binaries = vec![
            make_binary("a", 50),
            make_binary("b", 60),
        ];

        manager
            .process_binaries(
                binaries,
                std::collections::HashMap::new(),
                |_phase| {},
                |_phase, _completed, _failed| {},
                &mut factory,
            )
            .await
            .unwrap();

        assert_eq!(manager.stats().completed, 2);

        // Verify memuse.log was written
        let contents = std::fs::read_to_string(&memuse_path).expect("memuse.log should exist");
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 2, "expected 2 lines in memuse.log, got {}", lines.len());

        // Each line: size,estimated,0,filename,status
        assert!(lines[0].contains(",OK"), "first line should contain OK: {}", lines[0]);
        assert!(lines[1].contains(",OK"), "second line should contain OK: {}", lines[1]);

        let _ = std::fs::remove_dir_all(&tmp_dir);
    }).await;
}

#[tokio::test(flavor = "current_thread")]
async fn non_recoverable_error_restarts_worker_and_continues() {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    struct RestartCountingFactory {
        spawn_count: Arc<AtomicU32>,
    }

    impl WorkerFactory<ChannelManagerEnd> for RestartCountingFactory {
        fn spawn_worker(
            &mut self,
            _worker_id: WorkerId,
            _subcgroup: Option<&crate::cgroup::SubcgroupHandle>,
        ) -> Result<(ChannelManagerEnd, Option<u32>), String> {
            let count = self.spawn_count.fetch_add(1, Ordering::SeqCst);
            let (manager_end, runner_end) = channel_pair();
            tokio::task::spawn_local(async move {
                let mut runner = runner_end;
                let _ = runner.send(Response::Ready).await;
                loop {
                    match MessageReceiver::<Command>::recv(&mut runner).await {
                        Some(Command::Stop) => break,
                        Some(Command::ProcessTask { .. }) => {
                            if count == 0 {
                                // First spawn: send NonRecoverable error (triggers disconnect)
                                let _ = runner
                                    .send(Response::Error {
                                        error_type: ErrorType::NonRecoverable,
                                        message: "crash".into(),
                                    })
                                    .await;
                                break; // NonRecoverable worker exits
                            } else {
                                // Restarted worker: succeed
                                let _ = runner
                                    .send(Response::Done {
                                        result_data: None,
                                    })
                                    .await;
                            }
                        }
                        None => break,
                    }
                }
            });
            Ok((manager_end, None))
        }
    }

    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let spawn_count = Arc::new(AtomicU32::new(0));
        let spawn_count_clone = spawn_count.clone();

        let config = test_config(1);
        let mut manager = LocalManager::new(config, ResourceStealingScheduler::memory(), FixedEstimator(100));
        let mut factory = RestartCountingFactory {
            spawn_count: spawn_count_clone,
        };

        let binaries = vec![
            make_binary("crash_me", 50),
            make_binary("succeed", 60),
        ];

        manager
            .process_binaries(
                binaries,
                std::collections::HashMap::new(),
                |_phase| {},
                |_phase, _completed, _failed| {},
                &mut factory,
            )
            .await
            .unwrap();

        // First task: NonRecoverable -> fails, worker restarted
        // Second task: succeeds on restarted worker
        // Retry phase: first task retried on restarted worker and succeeds
        assert_eq!(manager.stats().completed, 2, "both tasks should complete");
        assert!(manager.resource_pressure_tasks().is_empty(), "no OOM tasks expected");

        // At least 2 spawns: initial + restart after NonRecoverable
        let spawns = spawn_count.load(Ordering::SeqCst);
        assert!(spawns >= 2, "expected at least 2 spawns (initial + restart), got {spawns}");
    }).await;
}

#[tokio::test(flavor = "current_thread")]
async fn multiple_workers_with_mixed_results() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        // 2 workers, 6 binaries: worker 0 always succeeds, worker 1 first OOM then succeed
        let config = test_config(2);
        let mut manager = LocalManager::new(config, ResourceStealingScheduler::memory(), FixedEstimator(100));
        let mut factory = FakeWorkerFactory {
            mode: FakeWorkerMode::AlwaysSucceed,
        };

        let binaries: Vec<TaskInfo<TestId>> = (0..6)
            .map(|i| make_binary(&format!("bin_{i}"), 100 + i * 10))
            .collect();

        manager
            .process_binaries(
                binaries,
                std::collections::HashMap::new(),
                |_phase| {},
                |_phase, _completed, _failed| {},
                &mut factory,
            )
            .await
            .unwrap();

        assert_eq!(manager.stats().completed, 6);
        assert_eq!(manager.stats().total, 6);
        assert!(manager.failed_tasks().is_empty());
        assert!(manager.resource_pressure_tasks().is_empty());
    }).await;
}

/// Regression pin: when a worker takes tasks of two distinct
/// `TypeId`s, `WorkerPool::ensure_worker_for_type` kills + respawns
/// the slot through `WorkerFactory::spawn_worker_for_type` on each
/// type-shift — and the same-type fast path stays a no-op. This is
/// the exact scenario the brief identifies: a multi-phase
/// `TaskDefinition` whose phases each declare a distinct
/// `worker_module`. Without per-type dispatch, phase 2's task would
/// arrive on phase 1's worker subprocess (wrong Python module
/// loaded), surfacing as the `payload['variant']` KeyError the
/// downstream pipeline saw.
///
/// We use a tracking factory that records the sequence of (spawn,
/// type_id) tuples it observes. The test asserts:
///   1. Initial `spawn_worker` (None — no type hint yet).
///   2. First `spawn_worker_for_type("tokenize")` when the first
///      "tokenize" task assigns.
///   3. Second `spawn_worker_for_type("unify_vocab")` when the
///      type-shifting task arrives.
///   4. No additional spawn for the second "unify_vocab" task — the
///      worker's `loaded_type_id` already matches.
#[tokio::test(flavor = "current_thread")]
async fn ensure_worker_for_type_respawns_on_type_shift_and_is_idempotent_on_match() {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::{Arc, Mutex};
    use dynrunner_core::TypeId;

    /// Spawn-history entry: `None` means `spawn_worker` (no type),
    /// `Some(_)` means `spawn_worker_for_type(_)`.
    type SpawnEntry = Option<TypeId>;

    struct TrackingFactory {
        spawns: Arc<Mutex<Vec<SpawnEntry>>>,
        next_pid: Arc<AtomicU32>,
    }

    impl WorkerFactory<ChannelManagerEnd> for TrackingFactory {
        fn spawn_worker(
            &mut self,
            _worker_id: WorkerId,
            _subcgroup: Option<&crate::cgroup::SubcgroupHandle>,
        ) -> Result<(ChannelManagerEnd, Option<u32>), String> {
            self.spawns.lock().unwrap().push(None);
            let pid = self.next_pid.fetch_add(1, Ordering::SeqCst);
            let (manager_end, runner_end) = channel_pair();
            tokio::task::spawn_local(fake_worker_loop_succeeds(runner_end));
            Ok((manager_end, Some(pid)))
        }

        fn spawn_worker_for_type(
            &mut self,
            _worker_id: WorkerId,
            type_id: &TypeId,
            _subcgroup: Option<&crate::cgroup::SubcgroupHandle>,
        ) -> Result<(ChannelManagerEnd, Option<u32>), String> {
            self.spawns.lock().unwrap().push(Some(type_id.clone()));
            let pid = self.next_pid.fetch_add(1, Ordering::SeqCst);
            let (manager_end, runner_end) = channel_pair();
            tokio::task::spawn_local(fake_worker_loop_succeeds(runner_end));
            Ok((manager_end, Some(pid)))
        }
    }

    async fn fake_worker_loop_succeeds(mut runner: ChannelRunnerEnd) {
        let _ = runner.send(Response::Ready).await;
        loop {
            match MessageReceiver::<Command>::recv(&mut runner).await {
                Some(Command::Stop) => break,
                Some(Command::ProcessTask { .. }) => {
                    let _ = runner.send(Response::Done { result_data: None }).await;
                }
                None => break,
            }
        }
    }

    fn make_binary_typed(name: &str, type_str: &str) -> TaskInfo<TestId> {
        TaskInfo {
            path: std::path::PathBuf::from(name),
            size: 100,
            identifier: TestId(name.into()),
            phase_id: dynrunner_core::PhaseId::from(type_str),
            type_id: TypeId::from(type_str),
            affinity_id: None,
            payload: serde_json::Value::Null,
            task_id: None,
            task_depends_on: vec![],
            preferred_secondaries: SoftPreferredSecondaries::default(),
            resolved_path: None,
        }
    }

    use dynrunner_core::SoftPreferredSecondaries;
    use dynrunner_transport_channel::ChannelRunnerEnd;

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let spawns: Arc<Mutex<Vec<SpawnEntry>>> = Arc::new(Mutex::new(Vec::new()));
            let next_pid = Arc::new(AtomicU32::new(1000));
            let mut factory = TrackingFactory {
                spawns: spawns.clone(),
                next_pid,
            };

            // Two tokenize binaries followed by two unify_vocab
            // binaries — type-shift after the first two. One worker
            // so the type-shift definitely lands on the same slot.
            let config = test_config(1);
            let mut manager = LocalManager::new(
                config,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let binaries = vec![
                make_binary_typed("tok_0", "tokenize"),
                make_binary_typed("tok_1", "tokenize"),
                make_binary_typed("uv_0", "unify_vocab"),
                make_binary_typed("uv_1", "unify_vocab"),
            ];

            // Two phase ids — same shape the brief's `FullPipelineTask`
            // declares: each phase carries one TaskTypeSpec. No
            // explicit dep graph needed for the pool's per-type
            // dispatch — the type_id alone is what drives respawn.
            manager
                .process_binaries(
                    binaries,
                    std::collections::HashMap::new(),
                    |_phase| {},
                    |_phase, _completed, _failed| {},
                    &mut factory,
                )
                .await
                .unwrap();

            assert_eq!(manager.stats().completed, 4);
            assert!(manager.failed_tasks().is_empty());

            let history = spawns.lock().unwrap().clone();
            // Initial: `spawn_worker` (no type hint).
            assert_eq!(history[0], None, "initial spawn must be type-less");
            // The slot binds to "tokenize" on the first task. Then
            // the first unify_vocab task triggers a type-shift
            // respawn through `spawn_worker_for_type("unify_vocab")`.
            // Two same-type assignments in a row hit the no-op
            // ensure-fast-path, so no extra spawn appears between
            // tok_0 and tok_1, nor between uv_0 and uv_1.
            let typed: Vec<_> = history.iter().filter_map(|e| e.clone()).collect();
            assert!(
                typed.contains(&TypeId::from("tokenize")),
                "expected a spawn_worker_for_type(tokenize); history: {history:?}"
            );
            assert!(
                typed.contains(&TypeId::from("unify_vocab")),
                "expected a spawn_worker_for_type(unify_vocab); history: {history:?}"
            );
            // Idempotence on match: exactly one spawn for each type
            // (the initial type-binding spawn). Anything beyond that
            // would mean the same-type fast path stopped firing —
            // turning every assignment into a respawn, which would
            // crush throughput.
            let tokenize_count = typed.iter().filter(|t| **t == TypeId::from("tokenize")).count();
            let unify_count = typed.iter().filter(|t| **t == TypeId::from("unify_vocab")).count();
            assert_eq!(
                tokenize_count, 1,
                "expected exactly 1 tokenize spawn (same-type fast path must be a no-op); history: {history:?}"
            );
            assert_eq!(
                unify_count, 1,
                "expected exactly 1 unify_vocab spawn (same-type fast path must be a no-op); history: {history:?}"
            );
        })
        .await;
}

/// Integration test for `KillReason`-based no-fault requeue routing.
///
/// Drives the LocalManager's `handle_resource_pressure_result` with a
/// synthesised `ResourcePressureResult::Killed` carrying each of the
/// four `KillReason` variants and asserts the routing contract:
///   * No-fault reasons → pool requeue (item back in the pool, no
///     `failed_tasks` / `resource_pressure_tasks` entry).
///   * `OomLastResort` / `OomOverBudget` outside pressure phase →
///     `resource_pressure_tasks` entry.
#[tokio::test(flavor = "current_thread")]
async fn killed_routing_by_kill_reason() {
    use dynrunner_core::PhaseId;
    use dynrunner_scheduler_api::{KillReason, PendingPool};
    use std::collections::HashSet;

    // No worker pool needed: `handle_resource_pressure_result` reads
    // only `pool_mut()` (the PendingPool, NOT the WorkerPool),
    // `failed_tasks`, `resource_pressure_tasks`, `in_pressure_phase`,
    // and `record_phase_completion`. Build a bare manager, install a
    // pre-built PendingPool via the test seam, inject synthesised
    // kill results, and assert the routing contract.

    // No-fault routing: requeue at the pool front, NO failure-side
    // entry, retry budget preserved.
    for reason in [
        KillReason::NoFaultMemoryStealing,
        KillReason::NoFaultUnderBudget,
    ] {
        let config = test_config(1);
        let mut manager: LocalManager<
            ChannelManagerEnd,
            _,
            _,
            super::test_helpers::TestId,
        > = LocalManager::new(
            config,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );
        let mut phase_ids = HashSet::new();
        phase_ids.insert(PhaseId::from("default"));
        let pool = PendingPool::new(phase_ids, std::collections::HashMap::new())
            .expect("pool new");
        manager.install_pool_for_test(pool);
        let binary = make_binary("victim", 50);
        let phase = binary.phase_id.clone();
        manager.pool_mut().extend(vec![binary.clone()]).expect("extend");
        // Simulate `take_from_view`'s in-flight bump so `requeue`
        // decrements correctly (requeue saturates at 0 either way,
        // but this matches the production sequencing).
        manager.pool_mut().mark_in_flight(&phase);
        manager.handle_resource_pressure_result(
            crate::pool::ResourcePressureResult::Killed {
                worker_id: 1,
                binary: Some(Box::new(binary)),
                reason,
            },
        );
        assert!(
            manager.failed_tasks().is_empty(),
            "{reason:?}: no failed_tasks entry expected"
        );
        assert!(
            manager.resource_pressure_tasks().is_empty(),
            "{reason:?}: no resource_pressure_tasks entry expected"
        );
        assert!(
            !manager.pool_ref().is_empty(),
            "{reason:?}: pool should hold the requeued item"
        );
    }

    // At-fault `OomOverBudget` / `OomLastResort` outside the
    // pressure phase → `resource_pressure_tasks` entry, NOT
    // `failed_tasks`.
    for reason in [KillReason::OomOverBudget, KillReason::OomLastResort] {
        let config = test_config(1);
        let mut manager: LocalManager<
            ChannelManagerEnd,
            _,
            _,
            super::test_helpers::TestId,
        > = LocalManager::new(
            config,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );
        let mut phase_ids = HashSet::new();
        phase_ids.insert(PhaseId::from("default"));
        let pool = PendingPool::new(phase_ids, std::collections::HashMap::new())
            .expect("pool new");
        manager.install_pool_for_test(pool);
        let binary = make_binary("over_budget", 50);
        manager.handle_resource_pressure_result(
            crate::pool::ResourcePressureResult::Killed {
                worker_id: 0,
                binary: Some(Box::new(binary)),
                reason,
            },
        );
        assert!(
            manager.failed_tasks().is_empty(),
            "{reason:?}: not a Recoverable failure, must not land in failed_tasks"
        );
        assert_eq!(
            manager.resource_pressure_tasks().len(),
            1,
            "{reason:?}: expected 1 resource_pressure_tasks entry"
        );
        let entry = &manager.resource_pressure_tasks()[0];
        match &entry.error_type {
            ErrorType::ResourceExhausted(k) if k.as_str() == "memory" => {}
            other => panic!(
                "{reason:?}: expected ResourceExhausted(memory), got {other:?}"
            ),
        }
    }
}

/// Regression pin for the retry-phase leftover mis-tag fix.
///
/// Before the fix, tasks that couldn't fit any worker's reserved
/// budget at the end of the retry phase were pushed to
/// `resource_pressure_tasks` with `ErrorType::ResourceExhausted(memory)`
/// — wrong bucket: the failure class is scheduling-fit, not
/// memory-pressure. The fix re-tags them as `ErrorType::Recoverable`
/// so they ride the recoverable retry channel.
///
/// Construction: 1 worker (gets the full 1 GiB reserved budget from
/// `ResourceStealingScheduler::initial_budget(0, max)`), then ask the
/// estimator to return a per-task memory request that exceeds 1 GiB.
/// No worker accepts the task; it sits in the pool through main +
/// retry phases; the retry-phase drain at the end of
/// `process_worker_loop` re-tags it.
#[tokio::test(flavor = "current_thread")]
async fn retry_phase_leftover_lands_in_failed_tasks_as_recoverable() {
    use dynrunner_core::PhaseId;
    use dynrunner_scheduler_api::PendingPool;
    use std::collections::HashSet;

    // The retry-phase leftover-drain at the tail of
    // `process_worker_loop` fires when:
    //   1. `phase == ProcessingPhase::RetryPhase`, AND
    //   2. `!pool.is_empty()` at loop exit.
    //
    // To exercise the post-fix tag without spinning the full
    // process_binaries pipeline (and risk a different code path
    // catching the leftover first), call `process_worker_loop`
    // directly on a seeded pool with zero active workers. The
    // outer `while !active_workers.is_empty()` exits immediately,
    // the pool still holds the seeded task, and the drain block
    // re-tags it.
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let config = test_config(1);
            let mut manager: LocalManager<
                ChannelManagerEnd,
                _,
                _,
                super::test_helpers::TestId,
            > = LocalManager::new(
                config,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            let mut phase_ids = HashSet::new();
            phase_ids.insert(PhaseId::from("default"));
            let pool =
                PendingPool::new(phase_ids, std::collections::HashMap::new())
                    .expect("pool new");
            manager.install_pool_for_test(pool);
            manager
                .pool_mut()
                .extend(vec![make_binary("leftover", 100)])
                .expect("extend");

            let mut active_workers: HashSet<WorkerId> = HashSet::new();
            let mut factory = FakeWorkerFactory {
                mode: FakeWorkerMode::AlwaysSucceed,
            };
            manager
                .process_worker_loop(
                    &mut active_workers,
                    false,
                    false,
                    dynrunner_scheduler_api::ProcessingPhase::RetryPhase,
                    &mut factory,
                )
                .await;

            // Pre-fix: this would have been 1 entry in
            // resource_pressure_tasks tagged ResourceExhausted(memory).
            assert!(
                manager.resource_pressure_tasks().is_empty(),
                "retry-phase leftover must NOT land in resource_pressure_tasks; got {} entries",
                manager.resource_pressure_tasks().len()
            );
            let failed = manager.failed_tasks();
            assert_eq!(
                failed.len(),
                1,
                "expected 1 task in failed_tasks after retry-phase drain"
            );
            assert!(
                matches!(failed[0].error_type, ErrorType::Recoverable),
                "expected ErrorType::Recoverable, got {:?}",
                failed[0].error_type
            );
            assert!(
                failed[0]
                    .error_message
                    .contains("Could not fit in any worker budget"),
                "error_message should preserve the scheduling-fit reason; got {:?}",
                failed[0].error_message
            );
        })
        .await;
}

// ── Memprofile sampler wiring ────────────────────────────────────────
//
// Two scopes:
//
//   1. `memprofile_run_level_smoke` — drives `process_binaries` end-to-end
//      with `output_dir = Some(tempdir)`. Asserts: the manager
//      constructs the sampler at run start, tears it down before the
//      pool teardown, and the run completes without panic on the
//      "no per-worker subcgroup" path (the current default of
//      `LocalManager` mode, which passes `None` for
//      `mem_manager_reserved_bytes` to `pool.initialize`, leaving every
//      `WorkerHandle.subcgroup == None`). The output dir stays empty
//      because the sampler's `on_task_assigned` short-circuits without
//      a leaf path — see the scope note on
//      `LocalManagerConfig::output_dir`.
//
//   2. `memprofile_hook_writes_profile_with_fake_subcgroup` — drives
//      the `notify_sampler_*` hooks directly against a manager whose
//      `WorkerHandle.subcgroup` is hand-injected to point at a
//      tempdir-rooted fake cgroup leaf. This is the test that proves
//      the wiring writes the file once the cgroup leaf is real;
//      when the production path eventually materialises real leaves
//      it will be covered by the existing `memprofile/tests.rs`
//      round-trip coverage, not by re-testing the hook here.

/// Run-level smoke: enabling profiling does not crash the standard
/// `process_binaries` happy path; the sampler is constructed at the
/// start of the run and `take()`n on the teardown path.
#[tokio::test(flavor = "current_thread")]
async fn memprofile_run_level_smoke() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let tmp = tempfile::tempdir().expect("output_dir tempdir");
        let mut config = test_config(1);
        config.output_dir = Some(tmp.path().to_path_buf());
        let mut manager =
            LocalManager::new(config, ResourceStealingScheduler::memory(), FixedEstimator(100));
        let mut factory = FakeWorkerFactory {
            mode: FakeWorkerMode::AlwaysSucceed,
        };

        let binaries = vec![make_binary("a", 50), make_binary("b", 60)];

        // Sampler is None pre-run (constructed lazily inside
        // `process_binaries` because `MemProfileSampler::spawn`
        // requires a running tokio runtime).
        assert!(!manager.sampler_is_some(), "sampler must be lazy");

        manager
            .process_binaries(
                binaries,
                std::collections::HashMap::new(),
                |_phase| {},
                |_phase, _completed, _failed| {},
                &mut factory,
            )
            .await
            .unwrap();

        // Sampler torn down by the teardown path (start of run) so
        // the next run can construct a fresh one.
        assert!(!manager.sampler_is_some(), "sampler must be torn down");
        assert_eq!(manager.stats().completed, 2);

        // Tasks ran without per-worker subcgroups (FakeWorkerFactory
        // never installs one), so the sampler's on_task_assigned
        // short-circuit fired and no profile files were written.
        // The output dir is still the tempdir we passed; assert no
        // unexpected files leaked.
        let leftovers: Vec<_> = std::fs::read_dir(tmp.path())
            .expect("read output_dir")
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert!(
            leftovers.is_empty(),
            "output_dir should be empty when no per-worker subcgroup is configured; found {leftovers:?}"
        );
    }).await;
}

/// Hook-level integration: hand-inject a fake `SubcgroupHandle`
/// pointing at a tempdir with cgroup-v2 pseudo-files, drive
/// `notify_sampler_assigned` + `notify_sampler_completed` through
/// the public hook seams, and assert the profile file lands at the
/// expected path with at least one sample. This pins the manager-
/// side wiring contract: when the pool DOES surface a per-worker
/// subcgroup, the sampler hooks fire and the file materialises.
#[tokio::test(flavor = "current_thread")]
async fn memprofile_hook_writes_profile_with_fake_subcgroup() {
    use std::io::Read;
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        // Output dir for profile files.
        let out = tempfile::tempdir().expect("out tempdir");
        // Fake cgroup leaf with the three pseudo-files the reader needs.
        let cg = tempfile::tempdir().expect("cg tempdir");
        let leaf = cg.path().join("worker-0");
        std::fs::create_dir(&leaf).unwrap();
        std::fs::write(leaf.join("memory.current"), "4096\n").unwrap();
        std::fs::write(leaf.join("memory.swap.current"), "0\n").unwrap();
        std::fs::write(leaf.join("memory.stat"), "anon 4096\nfile 0\n").unwrap();

        // Build a manager, populate its WorkerPool with one worker via
        // the existing `FakeWorkerFactory` path so the sampler hooks
        // have a real `WorkerHandle` slot to look up.
        let mut config = test_config(1);
        // Tight sampler interval so the test runs quickly.
        config.output_dir = Some(out.path().to_path_buf());
        let mut manager: LocalManager<ChannelManagerEnd, _, _, super::test_helpers::TestId> =
            LocalManager::new(config, ResourceStealingScheduler::memory(), FixedEstimator(100));
        let mut factory = FakeWorkerFactory {
            mode: FakeWorkerMode::AlwaysSucceed,
        };
        // Bring up one worker without running process_binaries (we
        // want to drive the hook surface directly, not the full
        // dispatch pipeline). `initialize_workers` is the
        // pool-bootstrap step that allocates `WorkerHandle`s.
        manager
            .initialize_workers(&mut factory)
            .await
            .expect("worker init");

        // Inject the fake subcgroup onto worker 0 — production
        // would materialise this via `prepare_worker_subgroup` at
        // pool spawn time, gated on `mem_manager_reserved_bytes`
        // being `Some(_)` (currently `None` in `LocalManager` mode);
        // injecting it directly lets us test the sampler wiring
        // end-to-end without that surface change.
        let handle = crate::cgroup::SubcgroupHandle::from_cgroup_dir_for_test(leaf.clone());
        manager.install_worker_subcgroup_for_test(0, handle);

        // Stand up the sampler with a tight sample interval so the
        // test doesn't pay the 1 s production cadence. Direct
        // construction (not via `process_binaries`) keeps the test
        // focused on the hook surface.
        let sampler = crate::memprofile::MemProfileSampler::spawn(
            crate::memprofile::MemProfileConfig {
                output_dir: out.path().to_path_buf(),
                sample_interval: std::time::Duration::from_millis(20),
            },
        );
        manager.install_sampler_for_test(sampler);

        // Drive the hooks. `binary.task_id == Some("task-A")` so the
        // expected file is `task-A.worker-0.memprofile.jsonl.zst`.
        let mut binary = make_binary("a", 50);
        binary.task_id = Some("task-A".to_string());
        manager.notify_sampler_assigned(0, &binary);

        // Let several ticks fire so the writer accumulates samples.
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;

        manager.notify_sampler_completed(Some("task-A".to_string()));

        // Shutdown drains the sampler's queue and joins the
        // background task, so the on-disk file is final by the time
        // shutdown returns.
        let sampler = manager.sampler.take().expect("sampler installed");
        sampler.shutdown().await;

        let expected = out.path().join("task-A.worker-0.memprofile.jsonl.zst");
        assert!(
            expected.exists(),
            "expected profile file at {}",
            expected.display()
        );
        // Round-trip: zstd-decode + JSONL parse. At least one
        // complete frame (sample) must have been written.
        let file = std::fs::File::open(&expected).expect("open profile");
        let mut decoder = zstd::stream::read::Decoder::new(file).expect("decoder");
        let mut decoded = Vec::new();
        let _ = decoder.read_to_end(&mut decoded);
        let text = std::str::from_utf8(&decoded).expect("utf8");
        let lines: Vec<&str> = text.split_terminator('\n').collect();
        assert!(
            !lines.is_empty(),
            "expected >= 1 sample in profile file, got 0 (raw: {decoded:?})"
        );
        let first: serde_json::Value = serde_json::from_str(lines[0]).expect("json");
        assert_eq!(first["worker_id"].as_u64(), Some(0));
        assert_eq!(first["memory_current"].as_u64(), Some(4096));
    }).await;
}
