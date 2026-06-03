//! PyO3 binding for [`dynrunner_driver::SshMaster`].
//!
//! Single concern: translate Python calls into the sync Rust driver
//! API. No tokio runtime — `dynrunner-driver`'s SshMaster lifecycle
//! is sync end-to-end (per locked design point (i)).
//!
//! Surface (locked design point (d)):
//! - `SshMaster.spawn(host, port=22, user=None, identity_file=None,
//!   config_file=None, forwarded_ports=None)` — classmethod-style
//!   constructor returning a connected master.
//! - `SshMaster.adopt(control_path, target)` — classmethod for the
//!   external-master case.
//! - `disconnect()` — explicit teardown; returns None or raises.
//! - `__enter__` / `__exit__` — sync context manager. `__exit__`
//!   swallows nothing; suppression follows Python convention.
//! - Getters: `control_path`, `master_pid`, `target`, `port`,
//!   `forwarded_ports`, `is_invalidated`, `is_spawned`.
//! - `add_forward(local_port, remote_port)` — adopt-only.

use std::path::PathBuf;
use std::sync::Mutex;

use pyo3::exceptions::{PyOSError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyType};

use dynrunner_driver::{SshConfig, SshMaster, SshMasterError, SshTarget, cluster, identity};

/// Map [`SshMasterError`] to a Python exception. We surface every
/// adopt/spawn failure as `OSError` (preserving the underlying I/O
/// nature) except for `Other` and `MasterDied` which map to
/// `RuntimeError` so callers can `except RuntimeError`. Per locked
/// design: variants are typed in Rust and consumers gain access via
/// the message string, not via Python isinstance — matching the
/// pre-extraction Python ssh helper's `RuntimeError`/`OSError`
/// dichotomy.
fn map_err(e: SshMasterError) -> PyErr {
    match e {
        SshMasterError::SpawnFailed { .. }
        | SshMasterError::ControlSocketTimeout
        | SshMasterError::HandshakeRefused
        | SshMasterError::MasterPidProbeFailed => PyOSError::new_err(format!("{e}")),
        SshMasterError::MasterAdoptFailed { .. } => PyOSError::new_err(format!("{e}")),
        SshMasterError::UnkillableMaster { .. } | SshMasterError::MasterDied { .. } => {
            PyRuntimeError::new_err(format!("{e}"))
        }
        SshMasterError::Other(msg) => PyRuntimeError::new_err(msg),
    }
}

/// Python-facing wrapper around [`SshMaster`].
///
/// The inner is wrapped in `Mutex<Option<...>>` so:
/// - `Mutex`: pyclass methods are `&self`-only by default; Python's
///   GIL gives us serialisation but the Mutex lets us mutate the
///   underlying master without forcing every method to `&mut self`.
/// - `Option`: `__exit__`/`disconnect` consumes the master to run
///   its kill ladder, but we need the pyclass cell to remain valid
///   for subsequent attribute access. `take()` after disconnect
///   returns a dead handle whose getters return None / empty.
#[pyclass(name = "SshMaster", module = "dynamic_runner.driver", unsendable)]
pub(crate) struct PySshMaster {
    inner: Mutex<Option<SshMaster>>,
}

#[pymethods]
impl PySshMaster {
    /// Spawn a new SSH master via `ssh -M -N`. Sync (per locked
    /// design point (i)).
    ///
    /// `forwarded_ports` is `Optional[List[Tuple[int, int]]]` —
    /// pairs of `(local_port, remote_port)` baked into the spawn
    /// argv.
    #[classmethod]
    #[pyo3(signature = (host, port=22, user=None, identity_file=None, config_file=None, forwarded_ports=None))]
    fn spawn(
        _cls: &Bound<'_, PyType>,
        host: String,
        port: u16,
        user: Option<String>,
        identity_file: Option<PathBuf>,
        config_file: Option<PathBuf>,
        forwarded_ports: Option<Vec<(u16, u16)>>,
    ) -> PyResult<Self> {
        let target = SshTarget::from_user_host(user.as_deref(), &host);
        let cfg = SshConfig {
            port,
            target,
            identity_file,
            config_file,
            forwarded_ports: forwarded_ports.unwrap_or_default(),
        };
        let master = SshMaster::spawn(cfg).map_err(map_err)?;
        Ok(Self {
            inner: Mutex::new(Some(master)),
        })
    }

    /// Adopt an externally-spawned master via its control socket
    /// path. `target` is the `user@host` (or `host`) string the
    /// upstream driver used — it's structurally required for ssh
    /// subprocesses but is *ignored* at the master layer (the
    /// master responds via the unix socket).
    #[classmethod]
    fn adopt(_cls: &Bound<'_, PyType>, control_path: PathBuf, target: String) -> PyResult<Self> {
        let master = SshMaster::adopt(control_path, SshTarget::new(target)).map_err(map_err)?;
        Ok(Self {
            inner: Mutex::new(Some(master)),
        })
    }

    /// Explicit teardown. Spawn-master: SIGTERM→SIGKILL ladder.
    /// Adopt-master: per-forward `ssh -O cancel -R` cleanup.
    /// Idempotent — subsequent calls are no-ops.
    fn disconnect(&self) -> PyResult<()> {
        let mut guard = self.inner.lock().expect("ssh master mutex poisoned");
        if let Some(master) = guard.as_mut() {
            master.disconnect().map_err(map_err)?;
        }
        Ok(())
    }

    /// Sync context-manager enter. Returns `self`. Per locked design
    /// point (d): NO `__aenter__` — async-CM is deferred.
    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    /// Sync context-manager exit. Calls `disconnect()` and returns
    /// `None` (i.e. doesn't suppress exceptions). Per locked design
    /// point (d): NO `__aexit__`.
    #[pyo3(signature = (exc_type=None, exc_value=None, traceback=None))]
    fn __exit__(
        &self,
        exc_type: Option<Bound<'_, PyAny>>,
        exc_value: Option<Bound<'_, PyAny>>,
        traceback: Option<Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        // Touch the args to silence unused-variable warnings without
        // adding `_` underscores (they're real Python __exit__ args).
        let _ = (exc_type, exc_value, traceback);
        self.disconnect()?;
        Ok(false)
    }

    /// Absolute path to the control socket. `Some` after a
    /// successful `spawn()`/`adopt()`; `None` after the inner
    /// master has been disconnect()'d and dropped.
    #[getter]
    fn control_path(&self) -> Option<PathBuf> {
        let guard = self.inner.lock().expect("ssh master mutex poisoned");
        guard.as_ref().map(|m| m.control_path().to_path_buf())
    }

    /// Daemon PID (last-known, per locked point (h.1) — returns
    /// `Some(pid)` even after invalidation).
    #[getter]
    fn master_pid(&self) -> Option<u32> {
        let guard = self.inner.lock().expect("ssh master mutex poisoned");
        guard.as_ref().and_then(|m| m.master_pid())
    }

    /// The `user@host` target string used by ssh subprocesses.
    #[getter]
    fn target(&self) -> Option<String> {
        let guard = self.inner.lock().expect("ssh master mutex poisoned");
        guard.as_ref().map(|m| m.target().as_str().to_owned())
    }

    /// Port (22 for default).
    #[getter]
    fn port(&self) -> Option<u16> {
        let guard = self.inner.lock().expect("ssh master mutex poisoned");
        guard.as_ref().map(|m| m.port())
    }

    /// Registered `(local_port, remote_port)` forward pairs.
    #[getter]
    fn forwarded_ports(&self) -> Vec<(u16, u16)> {
        let guard = self.inner.lock().expect("ssh master mutex poisoned");
        guard
            .as_ref()
            .map(|m| m.forwarded_ports().to_vec())
            .unwrap_or_default()
    }

    /// True after the watcher observes daemon death OR after the
    /// kill ladder has completed.
    #[getter]
    fn is_invalidated(&self) -> bool {
        let guard = self.inner.lock().expect("ssh master mutex poisoned");
        guard.as_ref().map(|m| m.is_invalidated()).unwrap_or(true)
    }

    /// True if constructed via `spawn()` (vs `adopt()`).
    #[getter]
    fn is_spawned(&self) -> bool {
        let guard = self.inner.lock().expect("ssh master mutex poisoned");
        guard.as_ref().is_some_and(|m| m.is_spawned())
    }

    /// Register a runtime-added reverse forward. Adopt-only —
    /// spawn-master forwards are baked into the spawn argv.
    fn add_forward(&self, local_port: u16, remote_port: u16) -> PyResult<()> {
        let mut guard = self.inner.lock().expect("ssh master mutex poisoned");
        match guard.as_mut() {
            Some(m) => m.add_forward(local_port, remote_port).map_err(map_err),
            None => Err(PyRuntimeError::new_err("SshMaster already disconnected")),
        }
    }

    fn __repr__(&self) -> String {
        let guard = self.inner.lock().expect("ssh master mutex poisoned");
        match guard.as_ref() {
            None => "SshMaster(disconnected)".to_owned(),
            Some(m) => format!(
                "SshMaster(target={}, port={}, master_pid={:?}, \
                 is_spawned={}, is_invalidated={})",
                m.target(),
                m.port(),
                m.master_pid(),
                m.is_spawned(),
                m.is_invalidated()
            ),
        }
    }
}

// ---------- module-level helpers ----------

/// `dynamic_runner.driver.cluster_is_running(ssh_port)` — TCP probe
/// of `localhost:<ssh_port>`. Sync (no async surface).
#[pyfunction]
pub(crate) fn py_cluster_is_running(ssh_port: u16) -> bool {
    cluster::is_running(ssh_port)
}

/// `dynamic_runner.driver.ensure_dispatcher_keypair(state_dir)` —
/// generate (or return existing) ed25519 keypair under
/// `<state_dir>/keys/`. Returns `(private_key_path, public_key_path)`.
#[pyfunction]
pub(crate) fn py_ensure_dispatcher_keypair(state_dir: PathBuf) -> PyResult<(PathBuf, PathBuf)> {
    identity::ensure_dispatcher_keypair(&state_dir).map_err(|e| match e {
        identity::IdentityError::KeygenSpawn(_) | identity::IdentityError::Io(_) => {
            PyOSError::new_err(format!("{e}"))
        }
        identity::IdentityError::KeygenFailed { .. } => PyRuntimeError::new_err(format!("{e}")),
    })
}

/// `dynamic_runner.driver.write_ssh_config(...)` — emit the pinned
/// ssh_config(5) defaults; returns the absolute path.
///
/// `host_alias` and `host_name` are intentionally separate kwargs
/// (locked design point (l) doc-comment) so the SSH `Host` label
/// (also the URL host downstream) and the actual hostname can
/// differ without callers having to learn that distinction
/// elsewhere.
#[pyfunction]
#[pyo3(signature = (state_dir, host_alias, host_name, ssh_port, user, identity_file))]
pub(crate) fn py_write_ssh_config(
    state_dir: PathBuf,
    host_alias: String,
    host_name: String,
    ssh_port: u16,
    user: String,
    identity_file: PathBuf,
) -> PyResult<PathBuf> {
    let args = identity::WriteSshConfigArgs {
        state_dir,
        host_alias,
        host_name,
        ssh_port,
        user,
        identity_file,
    };
    identity::write_ssh_config(&args).map_err(|e| match e {
        identity::IdentityError::Io(_) => PyOSError::new_err(format!("{e}")),
        _ => PyValueError::new_err(format!("{e}")),
    })
}
