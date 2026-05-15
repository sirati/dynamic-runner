//! Python adapter for [`dynrunner_slurm::preparation`].
//!
//! Owned concern: bridge the Rust SSH-reverse-tunnel watcher state
//! machine to Python. The Python `SlurmPreparation` class delegates
//! `_setup_ssh_tunnels` and `cleanup` here; everything above (image
//! build, job submit, run-id bookkeeping) stays in Python because
//! it composes other higher-level Python objects. Single concern at
//! the bridge: spawn watchers, gather under timeout, teardown.
//!
//! The InfoFileReader bridge calls back into the Python gateway's
//! `execute_command(f"cat {path}")` — single source of truth for
//! the gateway connection lives on the Python side; the Rust
//! preparation crate stays gateway-impl-agnostic by accepting a
//! reader closure. GIL is re-acquired only for the cat call (small
//! window, infrequent during the 2s poll cadence).

use std::future::Future;
use std::time::Duration;

use pyo3::prelude::*;
use pyo3::types::PyDict;

use dynrunner_gateway::shell::shell_quote;
use dynrunner_slurm::preparation::{
    EstablishmentPolicy, InfoFileReader, PrepError, PreparationOptions, SlurmPreparation,
};

/// Bridge that calls back into a Python gateway's
/// `execute_command(f"cat {path}")` to read connection-info files.
///
/// `Clone` (required by `InfoFileReader`) is implemented via
/// `Py::clone_ref` under a re-acquired GIL — pyo3 doesn't expose
/// `Clone` for `Py<T>` without the `py-clone` feature, so this is
/// the explicit-GIL path. Each watcher gets its own clone at
/// spawn time.
struct PyGatewayReader {
    gateway: Py<PyAny>,
}

impl PyGatewayReader {
    fn new(gateway: Py<PyAny>) -> Self {
        Self { gateway }
    }
}

impl Clone for PyGatewayReader {
    fn clone(&self) -> Self {
        // `Python::attach` acquires the GIL momentarily for the
        // refcount bump — pyo3 lacks a non-GIL Clone for Py<T>
        // unless `py-clone` is enabled.
        Python::attach(|py| Self {
            gateway: self.gateway.clone_ref(py),
        })
    }
}

impl InfoFileReader for PyGatewayReader {
    fn read(
        &self,
        path: String,
    ) -> impl Future<Output = Result<Option<String>, PrepError>> + 'static {
        let gateway = Python::attach(|py| self.gateway.clone_ref(py));
        async move {
            // The Python gateway's `execute_command` is sync and may
            // shell out to ssh, which can block briefly on the
            // master connection. Run it on a tokio blocking thread
            // so the watcher's spawn_local task doesn't stall the
            // current_thread runtime — other watchers continue
            // polling in parallel. The closure re-acquires the GIL
            // inside the blocking thread.
            let res = tokio::task::spawn_blocking(move || {
                Python::attach(|py| -> PyResult<(i32, String)> {
                    let bound = gateway.bind(py);
                    let cmd = format!("cat {}", shell_quote(&path));
                    // execute_command(cmd) → (rc, stdout, stderr)
                    let result = bound.call_method1("execute_command", (cmd,))?;
                    let rc: i32 = result.get_item(0)?.extract()?;
                    let stdout: String = result.get_item(1)?.extract()?;
                    Ok((rc, stdout))
                })
            })
            .await
            .map_err(|e| PrepError::WatcherPanic(format!(
                "execute_command join failed: {e}"
            )))?
            .map_err(|e| PrepError::WatcherLost(format!(
                "execute_command raised: {e}"
            )))?;

            // Match Python: `returncode == 0 and stdout.strip()` →
            // we have content; otherwise still polling.
            let (rc, stdout) = res;
            if rc == 0 && !stdout.trim().is_empty() {
                Ok(Some(stdout))
            } else {
                Ok(None)
            }
        }
    }
}

/// Python-facing tunnel-lifecycle manager. The Python `SlurmPreparation`
/// thin shim instantiates one of these per-run and delegates
/// `_setup_ssh_tunnels` + `cleanup` to it.
///
/// Send-safe: both fields are `Send` (`SlurmPreparation` holds only
/// owned plain data + `Arc<Mutex<...>>`; `Py<PyAny>` is unconditionally
/// `Send` per pyo3). Cross-thread use is intentional — the Python
/// shim drives `setup_ssh_tunnels` via `asyncio.to_thread` so the
/// surrounding event loop keeps cooperating during the 600s deadline.
#[pyclass(name = "RustSlurmPreparation")]
pub(crate) struct PySlurmPreparation {
    inner: SlurmPreparation,
    gateway: Py<PyAny>,
}

#[pymethods]
impl PySlurmPreparation {
    /// Construct from the Python-side options.
    ///
    /// `gateway` must be the gateway object whose
    /// `execute_command(cmd) -> (rc, stdout, stderr)` is called for
    /// info-file polling. `gateway_host`, `gateway_user`,
    /// `gateway_port`, `auth_options` mirror the gateway's `host`,
    /// `user`, `port`, and `auth_options()` — passed in from the
    /// caller so the Rust core doesn't reach into Python attributes.
    #[new]
    #[pyo3(signature = (
        gateway,
        run_log_dir,
        gateway_host,
        gateway_port,
        auth_options,
        extra_port_forwards,
        gateway_user = None,
        setup_timeout_secs = 600.0,
        poll_interval_secs = 2.0,
        establishment_max_concurrent = None,
        establishment_attempts = None,
        establishment_backoff_secs = None,
        establishment_per_tunnel_timeout_secs = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        gateway: Py<PyAny>,
        run_log_dir: String,
        gateway_host: String,
        gateway_port: u16,
        auth_options: Vec<String>,
        extra_port_forwards: Vec<(u16, u16)>,
        gateway_user: Option<String>,
        setup_timeout_secs: f64,
        poll_interval_secs: f64,
        establishment_max_concurrent: Option<usize>,
        establishment_attempts: Option<usize>,
        establishment_backoff_secs: Option<Vec<f64>>,
        establishment_per_tunnel_timeout_secs: Option<f64>,
    ) -> PyResult<Self> {
        let mut opts = PreparationOptions::new(
            run_log_dir,
            gateway_host,
            gateway_user,
            gateway_port,
            auth_options,
            extra_port_forwards,
        );
        opts.setup_timeout = Duration::from_secs_f64(setup_timeout_secs);
        opts.poll_interval = Duration::from_secs_f64(poll_interval_secs);
        // Establishment-policy overrides. `None` for any field keeps
        // the Rust-side default — operator-friendly: callers that
        // don't care pass nothing and get the safe 4-concurrent /
        // 3-attempt / 5+15s / 90s defaults.
        let mut est = EstablishmentPolicy::default();
        if let Some(n) = establishment_max_concurrent {
            est.max_concurrent = n;
        }
        if let Some(n) = establishment_attempts {
            est.attempts = n;
        }
        if let Some(backoff) = establishment_backoff_secs {
            est.backoff = backoff
                .into_iter()
                .map(Duration::from_secs_f64)
                .collect();
        }
        if let Some(t) = establishment_per_tunnel_timeout_secs {
            est.per_tunnel_timeout = Duration::from_secs_f64(t);
        }
        opts.establishment = est;
        Ok(Self {
            inner: SlurmPreparation::new(opts),
            gateway,
        })
    }

    /// Spawn one watcher per secondary, gather all readiness reports
    /// under the configured timeout, return the populated
    /// `secondary_id -> tunnel_port` map. Raises:
    /// - `TimeoutError` on outer deadline
    /// - `RuntimeError` for ssh-tunnel-failure / IO / parse errors
    fn setup_ssh_tunnels(
        &mut self,
        py: Python<'_>,
        num_secondaries: usize,
        primary_quic_port: u16,
    ) -> PyResult<Py<PyDict>> {
        let reader = PyGatewayReader::new(self.gateway.clone_ref(py));
        // Take a Send `&mut` to the inner state machine for the
        // detached tokio runtime. `Py<PyAny>` is Send (refcounted
        // across threads) but we never touch it without re-acquiring
        // the GIL via `Python::attach` — see PyGatewayReader::read.
        let inner: &mut SlurmPreparation = &mut self.inner;

        let result: Result<std::collections::HashMap<String, u16>, PrepError> = py.detach(|| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(PrepError::Io)?;
            let local = tokio::task::LocalSet::new();
            rt.block_on(local.run_until(async {
                inner
                    .setup_ssh_tunnels(reader, num_secondaries, primary_quic_port)
                    .await
            }))
        });

        let map = result.map_err(prep_err_to_pyerr)?;
        let dict = PyDict::new(py);
        for (k, v) in map {
            dict.set_item(k, v)?;
        }
        Ok(dict.into())
    }

    /// Drain all tracked tunnel subprocesses (SIGTERM → 5s wait →
    /// SIGKILL escalation). Idempotent.
    fn cleanup(&mut self, py: Python<'_>) -> PyResult<()> {
        let inner: &mut SlurmPreparation = &mut self.inner;
        py.detach(|| -> PyResult<()> {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| pyo3::exceptions::PyOSError::new_err(format!(
                    "tokio runtime: {e}"
                )))?;
            rt.block_on(async {
                inner.cleanup().await;
            });
            Ok(())
        })
    }

    /// Read-only view of the `secondary_id -> tunnel_port` map. Useful
    /// for the Python caller to pass into downstream phases (e.g.
    /// pipeline orchestration).
    fn secondary_port_map(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let dict = PyDict::new(py);
        for (k, v) in self.inner.secondary_port_map() {
            dict.set_item(k, v)?;
        }
        Ok(dict.into())
    }
}

/// Convert a `PrepError` to the most appropriate `PyErr`.
fn prep_err_to_pyerr(e: PrepError) -> PyErr {
    match e {
        PrepError::Timeout { .. } => pyo3::exceptions::PyTimeoutError::new_err(e.to_string()),
        PrepError::Io(io) => pyo3::exceptions::PyOSError::new_err(io.to_string()),
        PrepError::InfoParse { .. }
        | PrepError::InfoRead { .. }
        | PrepError::TunnelFailed { .. }
        | PrepError::WatcherPanic(_)
        | PrepError::WatcherLost(_) => pyo3::exceptions::PyRuntimeError::new_err(e.to_string()),
    }
}

