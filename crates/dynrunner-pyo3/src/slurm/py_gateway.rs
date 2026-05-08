//! Bridge a Python gateway object to the Rust `Gateway` trait.
//!
//! The Python `gateway` parameter passed into `SlurmJobManager.__init__`
//! is duck-typed (existing tests pass a `_StubGateway` with only
//! `remote_home` set; production passes `LocalGateway` / `SSHGateway`
//! from `dynamic_runner.packaging.gateway`). To let the Rust
//! `dynrunner_slurm::SlurmJobManager<G: Gateway>` consume that object
//! without depending on any concrete `Rust*Gateway` PyO3 binding, we
//! wrap the `Py<PyAny>` reference in an adapter that implements the
//! Rust `Gateway` trait by re-acquiring the GIL and dispatching to
//! Python.
//!
//! When the dedicated `RustSshGateway` / `RustLocalGateway` PyO3
//! bindings (units L2.A / L2.B) land, this adapter remains correct —
//! it sees them as just another Python object responding to the same
//! method names. Native paths can later short-circuit by extracting
//! the inner Rust gateway instead of round-tripping through Python,
//! but that's a follow-up optimisation, not a correctness change.
//!
//! The Python Gateway interface (sync methods, see
//! `python/dynamic_runner/packaging/gateway/local_gateway.py` and
//! `ssh_gateway.py`):
//!
//! * `execute_command(cmd: str, cwd: Path | None = None) -> tuple[int, str, str]`
//! * `transfer_file(local: Path, remote: Path | str) -> None`
//! * `download_file(remote: Path | str, local: Path | str) -> None`
//! * `create_directory(remote: Path | str) -> None`
//! * `file_exists(remote: Path) -> bool`
//! * `setup_port_forwarding(local_port: int, remote_port: int) -> None`
//! * `connect()`, `disconnect()` — lifecycle
//!
//! All methods are synchronous on the Python side (subprocess-backed).
//! The Rust `Gateway` trait is async, so each adapter method returns a
//! future that synchronously calls Python under `Python::attach` and
//! resolves immediately. No awaits straddle the GIL acquisition, so
//! the resulting future is `Send`.

use std::path::Path;

use pyo3::prelude::*;

use dynrunner_gateway::traits::{CommandResult, Gateway, GatewayError};

/// Adapter holding a refcounted Python gateway and dispatching the
/// Rust `Gateway` trait to its methods.
pub(crate) struct PyGatewayAdapter {
    inner: Py<PyAny>,
}

impl PyGatewayAdapter {
    pub(crate) fn new(inner: Py<PyAny>) -> Self {
        Self { inner }
    }
}

/// Map a PyErr produced by a gateway method into the Rust trait's
/// error type. We retain the textual form so consumers can inspect.
fn map_pyerr(e: PyErr) -> GatewayError {
    GatewayError::Other(format!("python gateway error: {e}"))
}

impl Gateway for PyGatewayAdapter {
    async fn connect(&mut self) -> Result<(), GatewayError> {
        Python::attach(|py| -> PyResult<()> {
            self.inner.bind(py).call_method0("connect")?;
            Ok(())
        })
        .map_err(map_pyerr)
    }

    async fn disconnect(&mut self) -> Result<(), GatewayError> {
        Python::attach(|py| -> PyResult<()> {
            self.inner.bind(py).call_method0("disconnect")?;
            Ok(())
        })
        .map_err(map_pyerr)
    }

    async fn execute_command(
        &self,
        cmd: &str,
        cwd: Option<&str>,
    ) -> Result<CommandResult, GatewayError> {
        let cmd_owned = cmd.to_owned();
        let cwd_owned = cwd.map(|s| s.to_owned());
        Python::attach(|py| -> PyResult<CommandResult> {
            let bound = self.inner.bind(py);
            // Python signature: execute_command(command, cwd=None) -> (int, str, str)
            let result = match cwd_owned {
                Some(dir) => bound.call_method1("execute_command", (cmd_owned, dir))?,
                None => bound.call_method1("execute_command", (cmd_owned,))?,
            };
            let (return_code, stdout, stderr): (i32, String, String) = result.extract()?;
            Ok(CommandResult {
                return_code,
                stdout,
                stderr,
            })
        })
        .map_err(map_pyerr)
    }

    async fn transfer_file(&self, local: &Path, remote: &str) -> Result<(), GatewayError> {
        let local_owned = local.to_path_buf();
        let remote_owned = remote.to_owned();
        Python::attach(|py| -> PyResult<()> {
            self.inner
                .bind(py)
                .call_method1("transfer_file", (local_owned, remote_owned))?;
            Ok(())
        })
        .map_err(map_pyerr)
    }

    async fn download_file(&self, remote: &str, local: &Path) -> Result<(), GatewayError> {
        let remote_owned = remote.to_owned();
        let local_owned = local.to_path_buf();
        Python::attach(|py| -> PyResult<()> {
            self.inner
                .bind(py)
                .call_method1("download_file", (remote_owned, local_owned))?;
            Ok(())
        })
        .map_err(map_pyerr)
    }

    async fn create_directory(&self, remote: &str) -> Result<(), GatewayError> {
        let remote_owned = remote.to_owned();
        Python::attach(|py| -> PyResult<()> {
            self.inner
                .bind(py)
                .call_method1("create_directory", (remote_owned,))?;
            Ok(())
        })
        .map_err(map_pyerr)
    }

    async fn file_exists(&self, remote: &str) -> Result<bool, GatewayError> {
        let remote_owned = remote.to_owned();
        Python::attach(|py| -> PyResult<bool> {
            self.inner
                .bind(py)
                .call_method1("file_exists", (remote_owned,))?
                .extract()
        })
        .map_err(map_pyerr)
    }

    fn setup_port_forwarding(
        &mut self,
        local_port: u16,
        remote_port: u16,
    ) -> Result<(), GatewayError> {
        Python::attach(|py| -> PyResult<()> {
            self.inner
                .bind(py)
                .call_method1("setup_port_forwarding", (local_port, remote_port))?;
            Ok(())
        })
        .map_err(map_pyerr)
    }
}
