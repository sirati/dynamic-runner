//! Shared fixtures for the integration tests in `tests/`.
//!
//! Lives under `tests/common/` so cargo doesn't compile this as a
//! standalone integration-test binary.

use std::os::fd::FromRawFd;
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process;

use dynrunner_core::{TaskInfo, PhaseId, TypeId, WorkerId};
use dynrunner_manager_local::WorkerFactory;
use dynrunner_scheduler_api::ResourceEstimator;
use dynrunner_transport_socket::socketpair::{create_socketpair, SocketpairManagerEnd};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TestId(pub String);

pub struct FixedEstimator(pub u64);

impl ResourceEstimator<TestId> for FixedEstimator {
    fn estimate(&self, _task: &TaskInfo<TestId>) -> dynrunner_core::ResourceMap {
        dynrunner_core::ResourceMap::from([(dynrunner_core::ResourceKind::memory(), self.0)])
    }
}

/// Factory that spawns real Python test worker subprocesses via socketpair.
pub struct PythonWorkerFactory {
    pub worker_module_dir: PathBuf,
    pub source_dir: PathBuf,
    pub output_dir: PathBuf,
    pub children: Vec<Option<process::Child>>,
}

impl WorkerFactory<SocketpairManagerEnd> for PythonWorkerFactory {
    fn spawn_worker(
        &mut self,
        worker_id: WorkerId,
    ) -> Result<(SocketpairManagerEnd, Option<u32>), String> {
        let (manager_end, child_fd) =
            create_socketpair().expect("failed to create socketpair");

        let mut cmd = process::Command::new("python3");
        cmd.arg("-m")
            .arg("test_worker_mod")
            .arg("--dynamic_queue")
            .arg(child_fd.to_string())
            .arg("--source")
            .arg(&self.source_dir)
            .arg("--output")
            .arg(&self.output_dir);

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

pub fn make_binary(name: &str, size: u64) -> TaskInfo<TestId> {
    TaskInfo {
        path: PathBuf::from(name),
        size,
        identifier: TestId(name.into()),
        phase_id: PhaseId::from("default"),
        type_id: TypeId::from("default"),
        affinity_id: None,
        payload: serde_json::Value::Null,
        task_id: None,
        task_depends_on: vec![],
    }
}

/// Find the test_worker_mod directory relative to this test file.
pub fn worker_module_dir() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir.join("tests")
}
