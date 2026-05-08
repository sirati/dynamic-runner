//! PyO3 binding for [`dynrunner_gateway::SshGateway`].
//!
//! Single concern: translate Python calls into the async Rust gateway
//! API. The wrapper owns one `SshGateway` instance for its lifetime and
//! drives each call through a freshly-built current-thread tokio
//! runtime + `LocalSet` per the codebase convention (see
//! `crates/dynrunner-pyo3/src/managers/primary.rs`). PyO3 types are
//! `!Send`, so keeping the runtime local to each method (rather than
//! crossing a multi-thread runtime boundary) is the cheapest way to
//! keep cancellation hazards out — the runtime drops at the end of
//! the method, taking any in-flight task with it.
//!
//! Forward-compat shape: the constructor accepts `identity_file` and
//! `config_file` parameters mirroring the Python `SSHGateway` API.
//! These are stored on the wrapper and exposed via getters so the
//! Python thin-shim can implement `auth_options()` from them. The
//! `dynrunner-gateway::SshConfig` does not yet carry those fields;
//! once they land (L1.10), this module will plumb them into the
//! config so `connect()` / `transfer_file()` honour the explicit
//! credentials directly.

use std::path::PathBuf;

use pyo3::exceptions::{PyOSError, PyRuntimeError};
use pyo3::prelude::*;

use dynrunner_gateway::{Gateway, SshConfig, SshGateway};

/// Build a current-thread tokio runtime + `LocalSet` and run `fut` to
/// completion. Mirrors the convention used by the manager wrappers in
/// `crates/dynrunner-pyo3/src/managers/`. Each call is self-contained:
/// the runtime drops at the end of the call, so any spawned local
/// task that outlives the await chain is dropped with it (cancel-safe
/// by construction; no `select!` on non-cancel-safe futures).
fn block_on_local<F, T>(fut: F) -> T
where
    F: std::future::Future<Output = T>,
{
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to create tokio runtime");
    let local = tokio::task::LocalSet::new();
    rt.block_on(local.run_until(fut))
}

/// Map a [`dynrunner_gateway::traits::GatewayError`] to a Python
/// exception. `NotConnected` becomes `RuntimeError` to match the
/// pre-binding Python implementation, which raised `RuntimeError`
/// when callers issued a method before `connect()`. Everything else
/// is an `OSError` because the underlying cause is invariably an
/// I/O / subprocess failure.
fn map_gateway_err(e: dynrunner_gateway::traits::GatewayError) -> PyErr {
    use dynrunner_gateway::traits::GatewayError as G;
    match e {
        G::NotConnected => PyRuntimeError::new_err("Gateway not connected"),
        other => PyOSError::new_err(format!("{other}")),
    }
}

/// Python-facing wrapper around [`SshGateway`].
///
/// `identity_file` and `config_file` are stored verbatim and exposed
/// via getters; they're not yet plumbed into the Rust connection
/// machinery (see module-level docstring). The Python thin-shim reads
/// them through the getters to build `auth_options()`.
///
/// `_track_connected`, `_remote_home`, `_forwarded_ports` mirror state
/// that `SshGateway` keeps private. The wrapper updates them as it
/// drives the gateway so the Python getters can answer without
/// reaching into `SshGateway` internals. Once `SshGateway` grows
/// public accessors (or once Python-side state queries vanish
/// post-migration) the mirrors collapse to one-liner getters.
#[pyclass(name = "RustSshGateway")]
pub(crate) struct PySshGateway {
    inner: SshGateway,
    host: String,
    port: u16,
    user: Option<String>,
    identity_file: Option<PathBuf>,
    config_file: Option<PathBuf>,
    _track_connected: bool,
    _remote_home: Option<String>,
    _forwarded_ports: Vec<(u16, u16)>,
}

#[pymethods]
impl PySshGateway {
    #[new]
    #[pyo3(signature = (host, port, user=None, identity_file=None, config_file=None))]
    fn new(
        host: String,
        port: u16,
        user: Option<String>,
        identity_file: Option<PathBuf>,
        config_file: Option<PathBuf>,
    ) -> Self {
        let config = SshConfig {
            host: host.clone(),
            port,
            user: user.clone(),
        };
        Self {
            inner: SshGateway::new(config),
            host,
            port,
            user,
            identity_file,
            config_file,
            _track_connected: false,
            _remote_home: None,
            _forwarded_ports: Vec::new(),
        }
    }

    #[getter]
    fn host(&self) -> &str {
        &self.host
    }

    #[getter]
    fn port(&self) -> u16 {
        self.port
    }

    #[getter]
    fn user(&self) -> Option<&str> {
        self.user.as_deref()
    }

    #[getter]
    fn identity_file(&self) -> Option<&std::path::Path> {
        self.identity_file.as_deref()
    }

    #[getter]
    fn config_file(&self) -> Option<&std::path::Path> {
        self.config_file.as_deref()
    }

    /// Whether the master SSH connection is currently up. Read by
    /// callers (e.g. `pipeline.py`) to decide whether to call
    /// `disconnect()` on teardown.
    #[getter]
    fn connected(&self) -> bool {
        self._track_connected
    }

    /// Detected remote `$HOME` after `connect()` succeeded. `None`
    /// if `connect()` hasn't been called or the detection failed.
    #[getter]
    fn remote_home(&self) -> Option<&str> {
        self._remote_home.as_deref()
    }

    /// `Some(true)` if the SSH server has `GatewayPorts on`, `Some(false)`
    /// if confirmed off, `None` if not checked. Set by `connect()`
    /// after probing each forwarded port with `ss -tulpn` on the
    /// remote.
    #[getter]
    fn gateway_ports_enabled(&self) -> Option<bool> {
        self.inner.gateway_ports_enabled
    }

    /// Tuples of `(local_port, remote_port)` queued via
    /// `setup_port_forwarding`. Used by `pipeline.py` to verify
    /// the configured forward made it into the master connection.
    #[getter]
    fn forwarded_ports(&self) -> Vec<(u16, u16)> {
        self._forwarded_ports.clone()
    }

    fn connect(&mut self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| {
            block_on_local(async { self.inner.connect().await }).map_err(map_gateway_err)
        })?;
        self._track_connected = true;
        // Re-probe `$HOME` to populate the wrapper's mirror — `SshGateway`
        // already detected and cached it inside `connect`, but the field
        // is private. One extra round-trip through the now-established
        // control socket; matches the original Python's "warn-and-proceed"
        // behaviour by silently leaving `_remote_home` as None on failure.
        let home = py.detach(|| {
            block_on_local(async { self.inner.execute_command("echo $HOME", None).await })
        });
        if let Ok(result) = home {
            if result.success() {
                self._remote_home = Some(result.stdout.trim().to_owned());
            }
        }
        Ok(())
    }

    fn disconnect(&mut self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| {
            block_on_local(async { self.inner.disconnect().await }).map_err(map_gateway_err)
        })?;
        self._track_connected = false;
        self._remote_home = None;
        Ok(())
    }

    /// Execute `cmd` on the remote, optionally `cd`-ing into `cwd`
    /// first. Returns `(returncode, stdout, stderr)` to match the
    /// pre-binding Python signature exactly.
    #[pyo3(signature = (cmd, cwd=None))]
    fn execute_command(
        &self,
        py: Python<'_>,
        cmd: &str,
        cwd: Option<&str>,
    ) -> PyResult<(i32, String, String)> {
        let result = py.detach(|| {
            block_on_local(async { self.inner.execute_command(cmd, cwd).await })
                .map_err(map_gateway_err)
        })?;
        Ok((result.return_code, result.stdout, result.stderr))
    }

    /// Upload `local_path` to `remote_path` over the master connection
    /// (scp + ControlPath).
    fn transfer_file(
        &self,
        py: Python<'_>,
        local_path: PathBuf,
        remote_path: &str,
    ) -> PyResult<()> {
        py.detach(|| {
            block_on_local(async { self.inner.transfer_file(&local_path, remote_path).await })
                .map_err(map_gateway_err)
        })
    }

    /// Download `remote_path` to `local_path` over the master
    /// connection. The local parent directory is created if missing.
    fn download_file(
        &self,
        py: Python<'_>,
        remote_path: &str,
        local_path: PathBuf,
    ) -> PyResult<()> {
        py.detach(|| {
            block_on_local(async { self.inner.download_file(remote_path, &local_path).await })
                .map_err(map_gateway_err)
        })
    }

    fn create_directory(&self, py: Python<'_>, remote_path: &str) -> PyResult<()> {
        py.detach(|| {
            block_on_local(async { self.inner.create_directory(remote_path).await })
                .map_err(map_gateway_err)
        })
    }

    fn file_exists(&self, py: Python<'_>, remote_path: &str) -> PyResult<bool> {
        py.detach(|| {
            block_on_local(async { self.inner.file_exists(remote_path).await })
                .map_err(map_gateway_err)
        })
    }

    /// Configure an SSH `-R` forward applied at next `connect()`. Must
    /// be called before `connect()`; raises `RuntimeError` otherwise
    /// (matches the pre-binding Python contract).
    fn setup_port_forwarding(&mut self, local_port: u16, remote_port: u16) -> PyResult<()> {
        self.inner
            .setup_port_forwarding(local_port, remote_port)
            .map_err(map_gateway_err)?;
        self._forwarded_ports.push((local_port, remote_port));
        Ok(())
    }
}

