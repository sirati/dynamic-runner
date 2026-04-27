//! Python callback wrappers for the `WorkerFactory` and `ResourceMonitor`
//! traits.
//!
//! These are escape hatches: the recommended path is the in-process
//! `SubprocessWorkerFactory` and `ProcStatmMonitor`. Use the wrappers when
//! Python needs to own the subprocess lifecycle (e.g. multi-process failover
//! tests that need to SIGKILL workers, podman/docker/srun launch wrappers,
//! or a custom resource probe).

use std::collections::HashMap;
use std::os::fd::FromRawFd;
use std::path::PathBuf;

use pyo3::prelude::*;

use db_comm_api_base::{ResourceKind, ResourceMap, WorkerId};
use db_local_manager::{ResourceMonitor, WorkerFactory};
use db_transport_socket::named_socket::NamedSocketManagerEnd;
use db_transport_socket::socketpair::create_socketpair;

use crate::config::log_paths::LogPathConfig;
use crate::transport::EitherManagerEnd;

/// Python-implemented `WorkerFactory<EitherManagerEnd>`.
///
/// Rust owns the manager-side transport (socketpair or named-socket); Python
/// owns the subprocess. The callback is invoked once per spawn and receives
/// `(worker_id, comm_fd, socket_path)`. Exactly one of `comm_fd`/`socket_path`
/// is non-`None` depending on the configured connection mode. The callback
/// returns the spawned PID (or `None` when Python intentionally launches the
/// worker out-of-band).
#[pyclass(name = "PyCallbackWorkerFactory")]
pub(crate) struct PyCallbackWorkerFactory {
    spawn_callback: Py<PyAny>,
    named_socket_dir: Option<PathBuf>,
    log_paths: LogPathConfig,
}

#[pymethods]
impl PyCallbackWorkerFactory {
    #[new]
    #[pyo3(signature = (spawn_callback, named_socket_dir = None, log_paths = None))]
    fn new(
        spawn_callback: Py<PyAny>,
        named_socket_dir: Option<PathBuf>,
        log_paths: Option<LogPathConfig>,
    ) -> Self {
        Self {
            spawn_callback,
            named_socket_dir,
            log_paths: log_paths.unwrap_or_default(),
        }
    }
}

impl PyCallbackWorkerFactory {
    fn invoke_spawn(
        &self,
        worker_id: WorkerId,
        comm_fd: Option<i32>,
        socket_path: Option<String>,
    ) -> Result<Option<u32>, String> {
        Python::attach(|py| -> PyResult<Option<u32>> {
            let result = self
                .spawn_callback
                .bind(py)
                .call1((worker_id, comm_fd, socket_path))?;
            if result.is_none() {
                Ok(None)
            } else {
                Ok(Some(result.extract::<u32>()?))
            }
        })
        .map_err(|e| format!("python spawn callback failed: {e}"))
    }
}

impl WorkerFactory<EitherManagerEnd> for PyCallbackWorkerFactory {
    fn spawn_worker(
        &mut self,
        worker_id: WorkerId,
    ) -> Result<(EitherManagerEnd, Option<u32>), String> {
        match &self.named_socket_dir {
            None => {
                let (manager_end, child_fd) = create_socketpair()
                    .map_err(|e| format!("failed to create socketpair: {e}"))?;
                let pid = self.invoke_spawn(worker_id, Some(child_fd), None)?;
                // Drop the child fd on the manager side: Python's spawned process
                // has already inherited it (Python is responsible for using
                // pass_fds=[child_fd] when launching the subprocess).
                drop(unsafe { std::os::fd::OwnedFd::from_raw_fd(child_fd) });
                Ok((EitherManagerEnd::Socketpair(manager_end), pid))
            }
            Some(dir) => {
                let socket_path = self.log_paths.socket_path(dir, worker_id);
                let manager_end = NamedSocketManagerEnd::bind(&socket_path)
                    .map_err(|e| format!("failed to bind named socket: {e}"))?;
                let pid = self.invoke_spawn(
                    worker_id,
                    None,
                    Some(socket_path.to_string_lossy().into_owned()),
                )?;
                Ok((
                    EitherManagerEnd::Named {
                        inner: manager_end,
                        accepted: false,
                    },
                    pid,
                ))
            }
        }
    }
}

/// Python-implemented `ResourceMonitor`.
///
/// The callback is invoked with `(pid_or_none)` and must return a mapping of
/// resource-kind name → amount in the resource's natural unit (bytes for
/// memory). An empty mapping is treated the same as "no measurement
/// available" — the manager simply records zero usage for that tick.
#[pyclass(name = "PyCallbackResourceMonitor")]
pub(crate) struct PyCallbackResourceMonitor {
    measure_callback: Py<PyAny>,
}

#[pymethods]
impl PyCallbackResourceMonitor {
    #[new]
    fn new(measure_callback: Py<PyAny>) -> Self {
        Self { measure_callback }
    }
}

impl ResourceMonitor for PyCallbackResourceMonitor {
    fn measure(&self, pid: Option<u32>) -> ResourceMap {
        let result = Python::attach(|py| -> PyResult<HashMap<String, u64>> {
            let r = self.measure_callback.bind(py).call1((pid,))?;
            r.extract::<HashMap<String, u64>>()
        });
        match result {
            Ok(map) => map
                .into_iter()
                .map(|(k, v)| (ResourceKind::new(k), v))
                .collect(),
            Err(e) => {
                tracing::warn!(error = %e, "python resource monitor callback failed");
                ResourceMap::new()
            }
        }
    }
}
