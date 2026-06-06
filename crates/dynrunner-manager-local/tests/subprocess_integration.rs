//! Integration test: LocalManager with real Python subprocess workers.
//!
//! This test spawns actual Python worker subprocesses via socketpair,
//! verifying the full pipeline end-to-end. Fixtures live in
//! `tests/common/mod.rs`.

mod common;

use common::{FixedEstimator, PythonWorkerFactory, TestId, make_binary, worker_module_dir};

use std::path::PathBuf;
use std::process;

use dynrunner_core::{MessageReceiver, MessageSender, TaskInfo, WorkerId};
use dynrunner_manager_local::{LocalManager, LocalManagerConfig, WorkerFactory};
use dynrunner_protocol_manager_worker::{Command, Response};
use dynrunner_scheduler::ResourceStealingScheduler;
use dynrunner_transport_socket::named_socket::NamedSocketManagerEnd;
use dynrunner_transport_socket::socketpair::SocketpairManagerEnd;

#[tokio::test(flavor = "current_thread")]
async fn single_worker_subprocess_processes_all() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let worker_dir = worker_module_dir();
            let tmp_dir = std::env::temp_dir().join("rust_integ_test_single");
            let _ = std::fs::create_dir_all(&tmp_dir);

            let config = LocalManagerConfig {
                num_workers: 1,
                max_resources: dynrunner_core::ResourceMap::from([(
                    dynrunner_core::ResourceKind::memory(),
                    1024 * 1024 * 1024,
                )]),
                reuse_workers: true,
                restart_predicate: None,
                retry_max_attempts: 1,
                print_pid: false,
                memuse_log_path: None,
                stage_timeouts: std::collections::HashMap::new(),
                low_resource_thresholds: dynrunner_core::ResourceMap::from([(
                    dynrunner_core::ResourceKind::memory(),
                    300 * 1024 * 1024,
                )]),
                resource_check_interval: std::time::Duration::from_millis(100),
                phase_status_log_intervals: Vec::new(),
                log_oom_watcher: false,
                output_dir: None,
                unfulfillable_reinject_max_per_task: None,
            };

            let mut factory = PythonWorkerFactory {
                worker_module_dir: worker_dir,
                source_dir: tmp_dir.clone(),
                output_dir: tmp_dir.clone(),
                children: Vec::new(),
            };

            let binaries = vec![
                make_binary("a.bin", 100),
                make_binary("b.bin", 200),
                make_binary("c.bin", 300),
            ];

            let mut manager = LocalManager::new(
                config,
                ResourceStealingScheduler::memory(),
                FixedEstimator(50 * 1024 * 1024), // 50MB estimate per binary
            );

            manager
                .process_binaries(
                    binaries,
                    std::collections::HashMap::new(),
                    |_phase| {},
                    |_phase, _completed, _failed, _outputs| {},
                    &mut factory,
                )
                .await
                .unwrap();

            assert_eq!(manager.stats().completed, 3);
            assert_eq!(manager.stats().total, 3);
            assert!(manager.failed_tasks().is_empty());
            assert!(manager.resource_pressure_tasks().is_empty());

            let _ = std::fs::remove_dir_all(&tmp_dir);
        })
        .await;
}

// ── Named socket integration tests ──

/// Transport enum for named socket integration tests.
#[allow(dead_code)]
enum EitherManagerEnd {
    Socketpair(SocketpairManagerEnd),
    Named {
        inner: NamedSocketManagerEnd,
        accepted: bool,
    },
}

impl MessageSender<Command> for EitherManagerEnd {
    async fn send(&mut self, msg: Command) -> Result<(), String> {
        match self {
            EitherManagerEnd::Socketpair(s) => s.send(msg).await,
            EitherManagerEnd::Named { inner, accepted } => {
                if !*accepted {
                    return Err("Named socket: not yet accepted".into());
                }
                inner.send(msg).await
            }
        }
    }
}

impl MessageReceiver<Response> for EitherManagerEnd {
    async fn recv(&mut self) -> Option<Response> {
        match self {
            EitherManagerEnd::Socketpair(s) => s.recv().await,
            EitherManagerEnd::Named { inner, accepted } => {
                if !*accepted {
                    match inner.accept().await {
                        Ok(()) => *accepted = true,
                        Err(e) => {
                            eprintln!("named socket accept failed: {e}");
                            return None;
                        }
                    }
                }
                inner.recv().await
            }
        }
    }
}

/// Factory that spawns real Python workers via named Unix domain sockets.
struct NamedSocketWorkerFactory {
    worker_module_dir: PathBuf,
    source_dir: PathBuf,
    output_dir: PathBuf,
    socket_dir: PathBuf,
    children: Vec<Option<process::Child>>,
}

impl WorkerFactory<EitherManagerEnd> for NamedSocketWorkerFactory {
    fn spawn_worker(
        &mut self,
        worker_id: WorkerId,
        _subcgroup: Option<&dynrunner_manager_local::cgroup::SubcgroupHandle>,
    ) -> Result<(EitherManagerEnd, Option<u32>), String> {
        let requested_path = self.socket_dir.join(format!("worker_{worker_id}.sock"));
        let manager_end = NamedSocketManagerEnd::bind(&requested_path)
            .map_err(|e| format!("failed to bind named socket for worker {worker_id}: {e}"))?;
        // `bind` hands back a per-bind-unique sibling of the requested
        // path (respawn-unlink fix); the worker must connect to the path
        // the endpoint actually bound, so read it back for the argv.
        let socket_path = manager_end.socket_path().to_owned();

        let mut cmd = process::Command::new("python3");
        cmd.arg("-m")
            .arg("test_worker_mod")
            .arg("--socket-path")
            .arg(&socket_path)
            .arg("--source")
            .arg(&self.source_dir)
            .arg("--output")
            .arg(&self.output_dir);

        cmd.env("PYTHONPATH", &self.worker_module_dir);

        cmd.stdin(process::Stdio::null())
            .stdout(process::Stdio::null())
            .stderr(process::Stdio::null());

        let child = cmd
            .spawn()
            .map_err(|e| format!("failed to spawn Python worker {worker_id}: {e}"))?;

        let pid = child.id();
        let idx = worker_id as usize;
        if self.children.len() <= idx {
            self.children.resize_with(idx + 1, || None);
        }
        self.children[idx] = Some(child);

        let endpoint = EitherManagerEnd::Named {
            inner: manager_end,
            accepted: false,
        };
        Ok((endpoint, Some(pid)))
    }
}

impl Drop for NamedSocketWorkerFactory {
    fn drop(&mut self) {
        for child in &mut self.children {
            if let Some(mut c) = child.take() {
                let _ = c.kill();
                let _ = c.wait();
            }
        }
    }
}

// Respawn-unlink regression pin (was the reason these two named-socket
// tests were #[ignore]'d):
//
// A worker's `loaded_type_id` is `None` until its first assignment, so the
// pool's `ensure_worker_for_type` always fires its type-shift respawn arm on
// the FIRST task per slot (see pool.rs — "Empty-state path: ... fires once on
// the first assignment per slot"). That respawn binds a fresh
// `NamedSocketManagerEnd` for the slot, then `self.workers[idx] = new_handle`
// DROPS the prior handle. Previously both binds landed on the SAME
// `worker_<id>.sock`, so the dropped prior endpoint's
// `NamedSocketManagerEnd::Drop` (which unlinks `self.socket_path`) deleted the
// freshly-bound socket out from under the new worker. Under the
// `current_thread` runtime there is no `.await` yield point between the new
// bind and the old drop, so the deletion was deterministic and the worker
// hung polling for its vanished socket.
//
// Fixed in dynrunner-transport-socket: `NamedSocketManagerEnd::bind` now owns
// a per-bind-unique on-disk filename (a `.<pid>.<gen>` sibling of the
// requested path), so a dropped endpoint's unlink can only ever target the
// path IT bound — never a newer endpoint's. The factory below reads the
// actual bound path back via `socket_path()` for the worker argv. These tests
// drive the real python3 worker over that path and assert all tasks complete.
#[tokio::test(flavor = "current_thread")]
async fn single_worker_named_socket_processes_all() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let worker_dir = worker_module_dir();
            let tmp_dir = std::env::temp_dir().join(format!("rin_{}", process::id()));
            let socket_dir = tmp_dir.join("sockets");
            let _ = std::fs::create_dir_all(&socket_dir);

            let config = LocalManagerConfig {
                num_workers: 1,
                max_resources: dynrunner_core::ResourceMap::from([(
                    dynrunner_core::ResourceKind::memory(),
                    1024 * 1024 * 1024,
                )]),
                reuse_workers: true,
                restart_predicate: None,
                retry_max_attempts: 1,
                print_pid: false,
                memuse_log_path: None,
                stage_timeouts: std::collections::HashMap::new(),
                low_resource_thresholds: dynrunner_core::ResourceMap::from([(
                    dynrunner_core::ResourceKind::memory(),
                    300 * 1024 * 1024,
                )]),
                resource_check_interval: std::time::Duration::from_millis(100),
                phase_status_log_intervals: Vec::new(),
                log_oom_watcher: false,
                output_dir: None,
                unfulfillable_reinject_max_per_task: None,
            };

            let mut factory = NamedSocketWorkerFactory {
                worker_module_dir: worker_dir,
                source_dir: tmp_dir.clone(),
                output_dir: tmp_dir.clone(),
                socket_dir,
                children: Vec::new(),
            };

            let binaries = vec![
                make_binary("a.bin", 100),
                make_binary("b.bin", 200),
                make_binary("c.bin", 300),
            ];

            let mut manager = LocalManager::new(
                config,
                ResourceStealingScheduler::memory(),
                FixedEstimator(50 * 1024 * 1024),
            );

            manager
                .process_binaries(
                    binaries,
                    std::collections::HashMap::new(),
                    |_phase| {},
                    |_phase, _completed, _failed, _outputs| {},
                    &mut factory,
                )
                .await
                .unwrap();

            assert_eq!(manager.stats().completed, 3);
            assert_eq!(manager.stats().total, 3);
            assert!(manager.failed_tasks().is_empty());
            assert!(manager.resource_pressure_tasks().is_empty());

            let _ = std::fs::remove_dir_all(&tmp_dir);
        })
        .await;
}

// Same respawn-unlink regression as `single_worker_named_socket_processes_all`
// above, across multiple worker slots: every slot hits the first-assignment
// type-shift respawn that re-binds its endpoint. See that test's comment for
// the full analysis and the per-bind-unique-path fix that resolves it.
#[tokio::test(flavor = "current_thread")]
async fn multi_worker_named_socket_processes_all() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let worker_dir = worker_module_dir();
            let tmp_dir = std::env::temp_dir().join(format!("rinm_{}", process::id()));
            let socket_dir = tmp_dir.join("sockets");
            let _ = std::fs::create_dir_all(&socket_dir);

            let config = LocalManagerConfig {
                num_workers: 3,
                max_resources: dynrunner_core::ResourceMap::from([(
                    dynrunner_core::ResourceKind::memory(),
                    2 * 1024 * 1024 * 1024,
                )]),
                reuse_workers: true,
                restart_predicate: None,
                retry_max_attempts: 1,
                print_pid: false,
                memuse_log_path: None,
                stage_timeouts: std::collections::HashMap::new(),
                low_resource_thresholds: dynrunner_core::ResourceMap::from([(
                    dynrunner_core::ResourceKind::memory(),
                    300 * 1024 * 1024,
                )]),
                resource_check_interval: std::time::Duration::from_millis(100),
                phase_status_log_intervals: Vec::new(),
                log_oom_watcher: false,
                output_dir: None,
                unfulfillable_reinject_max_per_task: None,
            };

            let mut factory = NamedSocketWorkerFactory {
                worker_module_dir: worker_dir,
                source_dir: tmp_dir.clone(),
                output_dir: tmp_dir.clone(),
                socket_dir,
                children: Vec::new(),
            };

            let binaries: Vec<TaskInfo<TestId>> = (0..8)
                .map(|i| make_binary(&format!("bin_{i}"), 100 + i * 50))
                .collect();

            let mut manager = LocalManager::new(
                config,
                ResourceStealingScheduler::memory(),
                FixedEstimator(50 * 1024 * 1024),
            );

            manager
                .process_binaries(
                    binaries,
                    std::collections::HashMap::new(),
                    |_phase| {},
                    |_phase, _completed, _failed, _outputs| {},
                    &mut factory,
                )
                .await
                .unwrap();

            assert_eq!(manager.stats().completed, 8);
            assert_eq!(manager.stats().total, 8);
            assert!(manager.failed_tasks().is_empty());
            assert!(manager.resource_pressure_tasks().is_empty());

            let _ = std::fs::remove_dir_all(&tmp_dir);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn multi_worker_subprocess_processes_all() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let worker_dir = worker_module_dir();
            let tmp_dir = std::env::temp_dir().join("rust_integ_test_multi");
            let _ = std::fs::create_dir_all(&tmp_dir);

            let config = LocalManagerConfig {
                num_workers: 3,
                max_resources: dynrunner_core::ResourceMap::from([(
                    dynrunner_core::ResourceKind::memory(),
                    2 * 1024 * 1024 * 1024,
                )]),
                reuse_workers: true,
                restart_predicate: None,
                retry_max_attempts: 1,
                print_pid: false,
                memuse_log_path: None,
                stage_timeouts: std::collections::HashMap::new(),
                low_resource_thresholds: dynrunner_core::ResourceMap::from([(
                    dynrunner_core::ResourceKind::memory(),
                    300 * 1024 * 1024,
                )]),
                resource_check_interval: std::time::Duration::from_millis(100),
                phase_status_log_intervals: Vec::new(),
                log_oom_watcher: false,
                output_dir: None,
                unfulfillable_reinject_max_per_task: None,
            };

            let mut factory = PythonWorkerFactory {
                worker_module_dir: worker_dir,
                source_dir: tmp_dir.clone(),
                output_dir: tmp_dir.clone(),
                children: Vec::new(),
            };

            let binaries: Vec<TaskInfo<TestId>> = (0..8)
                .map(|i| make_binary(&format!("bin_{i}"), 100 + i * 50))
                .collect();

            let mut manager = LocalManager::new(
                config,
                ResourceStealingScheduler::memory(),
                FixedEstimator(50 * 1024 * 1024),
            );

            manager
                .process_binaries(
                    binaries,
                    std::collections::HashMap::new(),
                    |_phase| {},
                    |_phase, _completed, _failed, _outputs| {},
                    &mut factory,
                )
                .await
                .unwrap();

            assert_eq!(manager.stats().completed, 8);
            assert_eq!(manager.stats().total, 8);
            assert!(manager.failed_tasks().is_empty());
            assert!(manager.resource_pressure_tasks().is_empty());

            let _ = std::fs::remove_dir_all(&tmp_dir);
        })
        .await;
}
