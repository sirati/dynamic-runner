//! Synchronous Python binding for [`dynrunner_gateway::LocalGateway`].
//!
//! Bridges the async Rust API to Python's blocking-call convention by
//! building a single-threaded tokio runtime + [`tokio::task::LocalSet`]
//! per call and driving the future to completion under
//! [`pyo3::Python::detach`]. The shape is identical to the manager
//! pyclasses (see `crates/dynrunner-pyo3/src/managers/primary.rs`) — no
//! shared runtime state, no background tasks, no `pyo3-async-runtimes`
//! dependency.

use std::path::PathBuf;

use pyo3::prelude::*;
use tokio::task::LocalSet;

use dynrunner_gateway::LocalGateway;
use dynrunner_gateway::traits::Gateway;

use super::gateway_error_to_py;

/// Run an async block to completion on a fresh current-thread runtime
/// + LocalSet, with the GIL released.
///
/// The runtime is constructed and torn down per call. This is
/// deliberate: the Python `Gateway` Protocol is synchronous and
/// per-call, so there is no carry-over async state to amortise. Same
/// shape as the manager pyclasses.
fn block_on_local<F, R>(py: Python<'_>, fut: F) -> R
where
    F: std::future::Future<Output = R> + Send,
    R: Send,
{
    py.detach(|| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to create tokio runtime");
        let local = LocalSet::new();
        rt.block_on(local.run_until(fut))
    })
}

/// Python-facing wrapper for [`LocalGateway`].
///
/// Methods mirror the `dynamic_runner.packaging.gateway.Gateway`
/// Protocol 1:1. The Python thin-shim module
/// (`packaging/gateway/local_gateway.py`) constructs this class and
/// forwards each method call.
#[pyclass(name = "RustLocalGateway")]
pub(crate) struct PyLocalGateway {
    inner: LocalGateway,
}

#[pymethods]
impl PyLocalGateway {
    #[new]
    fn new() -> Self {
        Self {
            inner: LocalGateway::new(),
        }
    }

    fn connect(&mut self, py: Python<'_>) -> PyResult<()> {
        let inner = &mut self.inner;
        block_on_local(py, async { inner.connect().await }).map_err(gateway_error_to_py)
    }

    fn disconnect(&mut self, py: Python<'_>) -> PyResult<()> {
        let inner = &mut self.inner;
        block_on_local(py, async { inner.disconnect().await }).map_err(gateway_error_to_py)
    }

    /// Execute a shell command on the gateway.
    ///
    /// Returns `(returncode, stdout, stderr)` to mirror the Python
    /// Protocol. `cwd` is optional; when supplied it is the working
    /// directory of the spawned process (path semantics are
    /// gateway-defined; for local execution it must be a real
    /// filesystem path).
    #[pyo3(signature = (command, cwd=None))]
    fn execute_command(
        &self,
        py: Python<'_>,
        command: &str,
        cwd: Option<PathBuf>,
    ) -> PyResult<(i32, String, String)> {
        let inner = &self.inner;
        let cwd_str = cwd.as_ref().map(|p| p.to_string_lossy().into_owned());
        let result = block_on_local(py, async {
            inner.execute_command(command, cwd_str.as_deref()).await
        })
        .map_err(gateway_error_to_py)?;
        Ok((result.return_code, result.stdout, result.stderr))
    }

    fn transfer_file(
        &self,
        py: Python<'_>,
        local_path: PathBuf,
        remote_path: PathBuf,
    ) -> PyResult<()> {
        let inner = &self.inner;
        let remote_str = remote_path.to_string_lossy().into_owned();
        block_on_local(py, async {
            inner.transfer_file(&local_path, &remote_str).await
        })
        .map_err(gateway_error_to_py)
    }

    /// Convenience alias preserved from the Python gateway Protocol.
    /// Identical semantics to [`Self::transfer_file`].
    fn upload_file(
        &self,
        py: Python<'_>,
        local_path: PathBuf,
        remote_path: PathBuf,
    ) -> PyResult<()> {
        self.transfer_file(py, local_path, remote_path)
    }

    fn download_file(
        &self,
        py: Python<'_>,
        remote_path: PathBuf,
        local_path: PathBuf,
    ) -> PyResult<()> {
        let inner = &self.inner;
        let remote_str = remote_path.to_string_lossy().into_owned();
        block_on_local(py, async {
            inner.download_file(&remote_str, &local_path).await
        })
        .map_err(gateway_error_to_py)
    }

    fn create_directory(&self, py: Python<'_>, remote_path: PathBuf) -> PyResult<()> {
        let inner = &self.inner;
        let remote_str = remote_path.to_string_lossy().into_owned();
        block_on_local(py, async { inner.create_directory(&remote_str).await })
            .map_err(gateway_error_to_py)
    }

    fn file_exists(&self, py: Python<'_>, remote_path: PathBuf) -> PyResult<bool> {
        let inner = &self.inner;
        let remote_str = remote_path.to_string_lossy().into_owned();
        block_on_local(py, async { inner.file_exists(&remote_str).await })
            .map_err(gateway_error_to_py)
    }

    /// No-op for the local gateway; included for Protocol parity.
    /// Forwarded to [`LocalGateway::setup_port_forwarding`] which
    /// returns `Ok(())` without touching state.
    fn setup_port_forwarding(&mut self, local_port: u16, remote_port: u16) -> PyResult<()> {
        self.inner
            .setup_port_forwarding(local_port, remote_port)
            .map_err(gateway_error_to_py)
    }
}
