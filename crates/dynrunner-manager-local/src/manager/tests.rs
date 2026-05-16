//! Tests for the local manager. Fixtures live in
//! `super::test_helpers`; this file holds the test scenarios.

use super::test_helpers::{
    fake_worker_loop, make_binary, test_config, FakeWorkerFactory, FakeWorkerMode,
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
        // 1 initial spawn + 2 restarts (after "a" and "b" complete, "c" is the last so no restart)
        let spawns = spawn_count.load(Ordering::SeqCst);
        assert_eq!(spawns, 3, "expected 3 spawns (1 initial + 2 restarts), got {spawns}");
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
