//! Integration test: LocalManager with real Python subprocess workers.
//!
//! This test spawns actual Python worker subprocesses via socketpair,
//! verifying the full pipeline end-to-end.

use std::os::fd::FromRawFd;
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process;

use db_comm_api_base::{
    BinaryInfo, MessageReceiver, MessageSender, WorkerId,
};
use db_manager_runner_comm::{Command, Response};
use serde::{Deserialize, Serialize};
use db_local_manager::{LocalManager, LocalManagerConfig, WorkerFactory};
use db_scheduler_api::ResourceEstimator;
use db_scheduler_impl::ResourceStealingScheduler;
use db_transport_socket::named_socket::NamedSocketManagerEnd;
use db_transport_socket::socketpair::{SocketpairManagerEnd, create_socketpair};

/// Minimal test identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct TestId(String);

struct FixedEstimator(u64);
impl ResourceEstimator for FixedEstimator {
    fn estimate(&self, _binary_size: u64) -> db_comm_api_base::ResourceMap {
        db_comm_api_base::ResourceMap::from([(db_comm_api_base::ResourceKind::memory(), self.0)])
    }
}

/// Factory that spawns real Python test worker subprocesses.
struct PythonWorkerFactory {
    worker_module_dir: PathBuf,
    source_dir: PathBuf,
    output_dir: PathBuf,
    children: Vec<Option<process::Child>>,
}

impl WorkerFactory<SocketpairManagerEnd> for PythonWorkerFactory {
    fn spawn_worker(
        &mut self,
        worker_id: WorkerId,
    ) -> Result<(SocketpairManagerEnd, Option<u32>), String> {
        let (manager_end, child_fd) = create_socketpair()
            .expect("failed to create socketpair");

        let mut cmd = process::Command::new("python3");
        cmd.arg("-m")
            .arg("test_worker_mod")
            .arg("--dynamic_queue")
            .arg(child_fd.to_string())
            .arg("--source")
            .arg(&self.source_dir)
            .arg("--output")
            .arg(&self.output_dir);

        // Set PYTHONPATH so the worker module can be found
        cmd.env("PYTHONPATH", &self.worker_module_dir);

        unsafe {
            cmd.pre_exec(|| Ok(()));
        }

        cmd.stdin(process::Stdio::null())
            .stdout(process::Stdio::null())
            .stderr(process::Stdio::null());

        let child = cmd
            .spawn()
            .unwrap_or_else(|e| panic!("failed to spawn Python worker {worker_id}: {e}"));

        // Close child fd on parent side
        drop(unsafe { std::os::fd::OwnedFd::from_raw_fd(child_fd) });

        let idx = worker_id as usize;
        if self.children.len() <= idx {
            self.children.resize_with(idx + 1, || None);
        }
        let pid = child.id();
        self.children[idx] = Some(child);

        Ok((manager_end, Some(pid)))
    }
}

impl Drop for PythonWorkerFactory {
    fn drop(&mut self) {
        for child in &mut self.children {
            if let Some(mut c) = child.take() {
                let _ = c.kill();
                let _ = c.wait();
            }
        }
    }
}

fn make_binary(name: &str, size: u64) -> BinaryInfo<TestId> {
    BinaryInfo {
        path: PathBuf::from(name),
        size,
        identifier: TestId(name.into()),
    }
}

/// Find the test_worker_mod directory relative to this test file.
fn worker_module_dir() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir.join("tests")
}

#[tokio::test(flavor = "current_thread")]
async fn single_worker_subprocess_processes_all() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let worker_dir = worker_module_dir();
        let tmp_dir = std::env::temp_dir().join("rust_integ_test_single");
        let _ = std::fs::create_dir_all(&tmp_dir);

        let config = LocalManagerConfig {
            num_workers: 1,
            max_resources: db_comm_api_base::ResourceMap::from([(db_comm_api_base::ResourceKind::memory(), 1024 * 1024 * 1024)]),
            always_restart_worker: false,
            print_pid: false,
            memuse_log_path: None,
            stage_timeouts: std::collections::HashMap::new(),
            low_resource_thresholds: db_comm_api_base::ResourceMap::from([(db_comm_api_base::ResourceKind::memory(), 300 * 1024 * 1024)]),
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

        manager.process_binaries(binaries, &mut factory).await.unwrap();

        assert_eq!(manager.stats().completed, 3);
        assert_eq!(manager.stats().total, 3);
        assert!(manager.failed_tasks().is_empty());
        assert!(manager.resource_pressure_tasks().is_empty());

        let _ = std::fs::remove_dir_all(&tmp_dir);
    }).await;
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
    ) -> Result<(EitherManagerEnd, Option<u32>), String> {
        let socket_path = self.socket_dir.join(format!("worker_{worker_id}.sock"));
        let manager_end = NamedSocketManagerEnd::bind(&socket_path)
            .map_err(|e| format!("failed to bind named socket for worker {worker_id}: {e}"))?;

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

#[tokio::test(flavor = "current_thread")]
async fn single_worker_named_socket_processes_all() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let worker_dir = worker_module_dir();
        let tmp_dir = std::env::temp_dir().join(format!("rin_{}", process::id()));
        let socket_dir = tmp_dir.join("sockets");
        let _ = std::fs::create_dir_all(&socket_dir);

        let config = LocalManagerConfig {
            num_workers: 1,
            max_resources: db_comm_api_base::ResourceMap::from([(db_comm_api_base::ResourceKind::memory(), 1024 * 1024 * 1024)]),
            always_restart_worker: false,
            print_pid: false,
            memuse_log_path: None,
            stage_timeouts: std::collections::HashMap::new(),
            low_resource_thresholds: db_comm_api_base::ResourceMap::from([(db_comm_api_base::ResourceKind::memory(), 300 * 1024 * 1024)]),
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

        manager.process_binaries(binaries, &mut factory).await.unwrap();

        assert_eq!(manager.stats().completed, 3);
        assert_eq!(manager.stats().total, 3);
        assert!(manager.failed_tasks().is_empty());
        assert!(manager.resource_pressure_tasks().is_empty());

        let _ = std::fs::remove_dir_all(&tmp_dir);
    }).await;
}

#[tokio::test(flavor = "current_thread")]
async fn multi_worker_named_socket_processes_all() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let worker_dir = worker_module_dir();
        let tmp_dir = std::env::temp_dir().join(format!("rinm_{}", process::id()));
        let socket_dir = tmp_dir.join("sockets");
        let _ = std::fs::create_dir_all(&socket_dir);

        let config = LocalManagerConfig {
            num_workers: 3,
            max_resources: db_comm_api_base::ResourceMap::from([(db_comm_api_base::ResourceKind::memory(), 2 * 1024 * 1024 * 1024)]),
            always_restart_worker: false,
            print_pid: false,
            memuse_log_path: None,
            stage_timeouts: std::collections::HashMap::new(),
            low_resource_thresholds: db_comm_api_base::ResourceMap::from([(db_comm_api_base::ResourceKind::memory(), 300 * 1024 * 1024)]),
        };

        let mut factory = NamedSocketWorkerFactory {
            worker_module_dir: worker_dir,
            source_dir: tmp_dir.clone(),
            output_dir: tmp_dir.clone(),
            socket_dir,
            children: Vec::new(),
        };

        let binaries: Vec<BinaryInfo<TestId>> = (0..8)
            .map(|i| make_binary(&format!("bin_{i}"), 100 + i * 50))
            .collect();

        let mut manager = LocalManager::new(
            config,
            ResourceStealingScheduler::memory(),
            FixedEstimator(50 * 1024 * 1024),
        );

        manager.process_binaries(binaries, &mut factory).await.unwrap();

        assert_eq!(manager.stats().completed, 8);
        assert_eq!(manager.stats().total, 8);
        assert!(manager.failed_tasks().is_empty());
        assert!(manager.resource_pressure_tasks().is_empty());

        let _ = std::fs::remove_dir_all(&tmp_dir);
    }).await;
}

#[tokio::test(flavor = "current_thread")]
async fn multi_worker_subprocess_processes_all() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let worker_dir = worker_module_dir();
        let tmp_dir = std::env::temp_dir().join("rust_integ_test_multi");
        let _ = std::fs::create_dir_all(&tmp_dir);

        let config = LocalManagerConfig {
            num_workers: 3,
            max_resources: db_comm_api_base::ResourceMap::from([(db_comm_api_base::ResourceKind::memory(), 2 * 1024 * 1024 * 1024)]),
            always_restart_worker: false,
            print_pid: false,
            memuse_log_path: None,
            stage_timeouts: std::collections::HashMap::new(),
            low_resource_thresholds: db_comm_api_base::ResourceMap::from([(db_comm_api_base::ResourceKind::memory(), 300 * 1024 * 1024)]),
        };

        let mut factory = PythonWorkerFactory {
            worker_module_dir: worker_dir,
            source_dir: tmp_dir.clone(),
            output_dir: tmp_dir.clone(),
            children: Vec::new(),
        };

        let binaries: Vec<BinaryInfo<TestId>> = (0..8)
            .map(|i| make_binary(&format!("bin_{i}"), 100 + i * 50))
            .collect();

        let mut manager = LocalManager::new(
            config,
            ResourceStealingScheduler::memory(),
            FixedEstimator(50 * 1024 * 1024),
        );

        manager.process_binaries(binaries, &mut factory).await.unwrap();

        assert_eq!(manager.stats().completed, 8);
        assert_eq!(manager.stats().total, 8);
        assert!(manager.failed_tasks().is_empty());
        assert!(manager.resource_pressure_tasks().is_empty());

        let _ = std::fs::remove_dir_all(&tmp_dir);
    }).await;
}
