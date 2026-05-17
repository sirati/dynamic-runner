//! PyO3 binding for `dynrunner_slurm::SlurmJobManager`.
//!
//! Wraps a `SlurmJobManager<PyGatewayAdapter>` so the Python thin
//! shim (`dynamic_runner.packaging.job_manager.SlurmJobManager`) can
//! delegate every SLURM lifecycle primitive — directory prep, job
//! submit, per-job cancel, cancel-all, status query, tracked-job-id
//! list — to Rust. After this binding only Python-bridge concerns
//! remain in the shim (run_log_dir default-arg, tilde expansion via
//! the Python gateway's `remote_home`).
//!
//! The Python `slurm_config` is a different shape from the Rust
//! `SlurmConfig` (see `python/dynamic_runner/packaging/slurm_config.py`
//! — `image_subfolder` / `output_subfolder` / `log_subfolder` /
//! `notify_email`, with `srcbins` nested under `image_subfolder`).
//! We translate at construction time so the Rust manager's path
//! computations (`image_path()`, `src_bins_path()`, `output_path()`,
//! `log_path()`) produce the same on-gateway directories the Python
//! shim has been creating in production. Future unit L2.C swaps in
//! a `RustSlurmConfig` pyclass and removes this duck-typed extraction;
//! the Rust `SlurmJobManager` API doesn't change.

use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyDict;
use tokio::sync::{Mutex, MutexGuard};

use dynrunner_slurm::{JobStatusInfo, SlurmConfig, SlurmJobManager};

use super::py_gateway::PyGatewayAdapter;

/// Build a Rust `SlurmConfig` from the duck-typed Python
/// `slurm_config` object. The Python shape uses `image_subfolder`,
/// `output_subfolder`, `log_subfolder` and nests `srcbins` under
/// the image dir. The Rust `src_bins_dir` field is set to the
/// nested form (`{image_subfolder}/srcbins`) so that the Rust
/// `SlurmConfig::src_bins_path()` getter returns the same path
/// the Python `get_srcbins_dir()` method has been producing.
fn slurm_config_from_python(
    py: Python<'_>,
    slurm_config: &Py<PyAny>,
) -> PyResult<SlurmConfig> {
    let bound = slurm_config.bind(py);
    let root_folder: String = bound.getattr("root_folder")?.str()?.extract()?;
    let image_subfolder: String = bound.getattr("image_subfolder")?.extract()?;
    let output_subfolder: String = bound.getattr("output_subfolder")?.extract()?;
    let log_subfolder: String = bound.getattr("log_subfolder")?.extract()?;

    // Optional/defaulted fields: we read them when present and fall
    // back to the Rust defaults otherwise. The Python class has
    // dataclass defaults for partition/cpus_per_task/time_limit so
    // the `getattr` calls below never miss in practice; the
    // defensive `.ok()` is there only for the duck-typed test
    // fixtures (see `tests/test_wrapper_script.py::_StubGateway`).
    let partition = bound
        .getattr("partition")
        .ok()
        .and_then(|v| v.extract::<String>().ok());
    let time_limit = bound
        .getattr("time_limit")
        .ok()
        .and_then(|v| v.extract::<String>().ok());
    let cpus_per_task = bound
        .getattr("cpus_per_task")
        .ok()
        .and_then(|v| v.extract::<u32>().ok());
    // Missing/None on the Python side maps to Rust `None` so
    // `submit_job` omits `--mem` (Python parity). Only an explicit
    // string value on the Python config produces `Some(...)` here.
    let mem = bound
        .getattr("memory_per_node")
        .ok()
        .and_then(|v| if v.is_none() { None } else { v.extract::<String>().ok() });
    let email = bound
        .getattr("notify_email")
        .ok()
        .and_then(|v| if v.is_none() { None } else { v.extract::<String>().ok() });
    let nodes = bound
        .getattr("nodes")
        .ok()
        .and_then(|v| v.extract::<u32>().ok());
    let prestaged_src_bins_path = bound
        .getattr("prestaged_src_bins_path")
        .ok()
        .and_then(|v| if v.is_none() { None } else { v.extract::<String>().ok() });
    // Pre-SIGKILL warning window — defaults to the Rust core's default
    // when the Python config doesn't carry the attribute (matches the
    // duck-typed pattern used for the other optional-shape fields).
    let signal_lead_seconds = bound
        .getattr("signal_lead_seconds")
        .ok()
        .and_then(|v| v.extract::<u32>().ok());

    Ok(SlurmConfig {
        root_folder,
        image_subfolder,
        output_subfolder,
        log_subfolder,
        partition: partition.unwrap_or_else(|| "All".into()),
        time_limit: time_limit.unwrap_or_else(|| "48:00:00".into()),
        cpus_per_task: cpus_per_task.unwrap_or(14),
        memory_per_node: mem,
        nodes: nodes.unwrap_or(1),
        notify_email: email,
        prestaged_src_bins_path,
        signal_lead_seconds: signal_lead_seconds
            .unwrap_or(SlurmConfig::default().signal_lead_seconds),
    })
}

/// Translate a Rust `JobStatusInfo` into the Python dict shape the
/// existing `SlurmJobManager.get_job_status` callers expect.
///
/// The dict has three str fields: `state`, `node`, `reason`. When
/// squeue had no record (Rust state is `None`), Python sees
/// `state="UNKNOWN"` and empty `node`/`reason` — same as the
/// pre-migration Python implementation.
fn job_status_to_dict<'py>(
    py: Python<'py>,
    info: &JobStatusInfo,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("state", info.state.as_deref().unwrap_or("UNKNOWN"))?;
    dict.set_item("node", info.node.as_str())?;
    dict.set_item("reason", info.reason.as_str())?;
    Ok(dict)
}

/// Run a future to completion under a current-thread tokio runtime
/// with a `LocalSet` — the canonical async-glue pattern in this
/// crate (see `managers/local.rs`, `managers/secondary.rs`). The
/// caller releases the GIL via `py.detach` first; the future itself
/// is free to re-acquire it via `Python::attach` for callbacks into
/// Python (the `PyGatewayAdapter` does exactly this).
fn block_on_local<F, T>(future: F) -> T
where
    F: std::future::Future<Output = T>,
{
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to create tokio runtime");
    let local = tokio::task::LocalSet::new();
    rt.block_on(local.run_until(future))
}

/// Acquire the manager mutex inside the tokio runtime.
///
/// Uses `tokio::sync::Mutex` (not `std::sync::Mutex`) because the
/// guard is held across `.await` points in every caller — every
/// `SlurmJobManager` async method runs while we hold the lock. A
/// `std::sync::MutexGuard` is `!Send` and (worse on a current-thread
/// runtime) blocks the executor thread on contention, which would
/// deadlock the moment any awaited call inside the critical section
/// yields back to the runtime. `tokio::sync::Mutex` yields cleanly
/// and never poisons, so the helper is infallible.
async fn lock_manager(
    inner: &Arc<Mutex<SlurmJobManager<PyGatewayAdapter>>>,
) -> MutexGuard<'_, SlurmJobManager<PyGatewayAdapter>> {
    inner.lock().await
}

/// Convert a `dynrunner_slurm::SlurmError` (the unified Rust error
/// returned by every async manager method) into a Python
/// `RuntimeError`. Centralises what was three identical inline
/// `map_err` calls.
fn slurm_err_to_py(e: dynrunner_slurm::SlurmError) -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err(e.to_string())
}

/// Python-facing wrapper for the Rust SLURM job manager.
///
/// Holds the inner `SlurmJobManager<PyGatewayAdapter>` behind an
/// `Arc<tokio::sync::Mutex<...>>`. Two distinct properties matter:
///
/// 1. **Interior mutability via `&self`**: cancel- and status-query
///    methods take `&self` at the trait level, while `submit_job` and
///    `cancel_all_jobs` take `&mut self` to mutate the tracked
///    `job_ids` vector. PyO3 exposes a single `&self` surface; the
///    mutex smooths the two trait-level shapes into one wrapper API
///    without requiring PyO3-level `&mut self`.
/// 2. **Async-safe locking**: every `SlurmJobManager` method we call
///    is `async` and the guard is held for the duration of the call,
///    i.e. across `.await` points. `std::sync::Mutex` is wrong here:
///    its guard is `!Send` and blocks the runtime thread, so on the
///    current-thread runtime + `LocalSet` we use, a single yielding
///    `.await` inside the critical section would deadlock the
///    executor. `tokio::sync::Mutex` yields cleanly instead.
///
/// `Arc<tokio::sync::Mutex<T>>` is `Send + Sync` whenever `T: Send`,
/// which separately satisfies the `Ungil` bound on the closure passed
/// to `py.detach` (the GIL release boundary).
///
/// The constructor accepts `packaging_method` and `deployment` to
/// preserve the Python-side `SlurmJobManager.__init__` signature,
/// but doesn't retain them — the Python thin shim still owns those
/// references directly for the non-lifecycle methods (wrapper-script
/// generation, image transfer, source-binary upload) that have yet
/// to migrate.
#[pyclass(name = "RustSlurmJobManager")]
pub(crate) struct PyRustSlurmJobManager {
    inner: Arc<Mutex<SlurmJobManager<PyGatewayAdapter>>>,
}

#[pymethods]
impl PyRustSlurmJobManager {
    #[new]
    fn new(
        py: Python<'_>,
        gateway: Py<PyAny>,
        slurm_config: Py<PyAny>,
        _packaging_method: Py<PyAny>,
        _deployment: Py<PyAny>,
    ) -> PyResult<Self> {
        let cfg = slurm_config_from_python(py, &slurm_config)?;
        let adapter = PyGatewayAdapter::new(gateway);
        let manager = SlurmJobManager::new(cfg, adapter);
        Ok(Self {
            inner: Arc::new(Mutex::new(manager)),
        })
    }

    /// Create the four SLURM working directories on the gateway:
    /// image, srcbins, output, log. Paths are derived from the
    /// Python `slurm_config` fields at construction time.
    fn prepare_directories(&self, py: Python<'_>) -> PyResult<()> {
        let inner = self.inner.clone();
        py.detach(|| {
            block_on_local(async move {
                lock_manager(&inner)
                    .await
                    .prepare_directories()
                    .await
                    .map_err(slurm_err_to_py)
            })
        })
    }

    /// Cancel a single SLURM job via `scancel`.
    fn cancel_job(&self, py: Python<'_>, job_id: String) -> PyResult<()> {
        let inner = self.inner.clone();
        py.detach(|| {
            block_on_local(async move {
                lock_manager(&inner)
                    .await
                    .cancel_job(&job_id)
                    .await
                    .map_err(slurm_err_to_py)
            })
        })
    }

    /// Query a job's state via `squeue`. Returns a Python dict with
    /// `state` / `node` / `reason` string fields — same shape as the
    /// pre-migration Python `SlurmJobManager.get_job_status`.
    fn get_job_status<'py>(
        &self,
        py: Python<'py>,
        job_id: String,
    ) -> PyResult<Bound<'py, PyDict>> {
        let inner = self.inner.clone();
        let info = py.detach(|| {
            block_on_local(async move {
                lock_manager(&inner)
                    .await
                    .get_job_status(&job_id)
                    .await
                    .map_err(slurm_err_to_py)
            })
        })?;
        job_status_to_dict(py, &info)
    }

    /// Submit a SLURM job by writing `wrapper_script` to
    /// `<root_folder>/job_<job_name>.sh` on the gateway and invoking
    /// `sbatch --parsable …`.
    ///
    /// `run_log_dir` must already be tilde-expanded by the caller — the
    /// Python shim's `_expand_path(run_log_dir or
    /// slurm_config.get_log_dir())` lives at the bridge boundary because
    /// `~/…` resolution depends on the Python gateway's `remote_home`
    /// attribute (a `PosixPath` for `LocalGateway`, `str | None` for
    /// `SSHGateway`). Tilde-bearing paths reach the Rust core only after
    /// the shim resolves them; `submit_job` itself takes the string
    /// verbatim. See the doc-comment on
    /// `dynrunner_slurm::SlurmJobManager::submit_job` for the rationale
    /// (sbatch flag-value tilde is NOT shell-expanded).
    ///
    /// Returns the submitted job ID (the `--parsable` stdout). The
    /// returned ID is also appended to the Rust-side `job_ids` vector
    /// so a later `cancel_all_jobs` call can drain it.
    fn submit_job(
        &self,
        py: Python<'_>,
        wrapper_script: String,
        job_name: String,
        nodes: u32,
        run_log_dir: String,
    ) -> PyResult<String> {
        let inner = self.inner.clone();
        py.detach(|| {
            block_on_local(async move {
                lock_manager(&inner)
                    .await
                    .submit_job(&wrapper_script, &job_name, nodes, &run_log_dir)
                    .await
                    .map_err(slurm_err_to_py)
            })
        })
    }

    /// Cancel every job tracked by the Rust manager (via `scancel`) and
    /// clear the tracked job-id list. Idempotent: a second call with no
    /// intervening `submit_job` is a no-op.
    ///
    /// Mirrors the legacy Python `SlurmJobManager.cancel_all_jobs`
    /// shape (iterate + clear). Individual `scancel` failures are
    /// logged on the Rust side and do not abort the loop, so a partial
    /// failure still drains the remaining IDs.
    fn cancel_all_jobs(&self, py: Python<'_>) -> PyResult<()> {
        let inner = self.inner.clone();
        py.detach(|| {
            block_on_local(async move {
                lock_manager(&inner)
                    .await
                    .cancel_all_jobs()
                    .await
                    .map_err(slurm_err_to_py)
            })
        })
    }

    /// Read-only view of the Rust-tracked job-id list. Returns a fresh
    /// Python `list[str]` snapshot — mutations on the returned list do
    /// NOT propagate back to the Rust state.
    ///
    /// Exposed so the Python thin shim can preserve the historical
    /// `SlurmJobManager.job_ids` attribute on its public surface
    /// without holding a duplicate Python-side list.
    #[getter]
    fn job_ids(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        let inner = self.inner.clone();
        Ok(py.detach(|| {
            block_on_local(async move {
                lock_manager(&inner).await.job_ids().to_vec()
            })
        }))
    }
}

// Rust-only surface: hands the Arc-wrapped inner manager out so the
// SLURM-pipeline orchestrator can park it on the `PrimaryCoordinator`
// for the respawn path. Not exposed to Python — the parked handle is
// the in-process Rust object, never crosses the Python boundary again
// after the SLURM-pipeline grabs it.
impl PyRustSlurmJobManager {
    /// Clone the shared `Arc` wrapping the inner Rust `SlurmJobManager`.
    /// Single concern: lend the manager to other Rust-side owners (today:
    /// `PrimaryCoordinator::set_slurm_job_manager` so the respawn path
    /// can call `submit_job` from inside the operational loop).
    pub(crate) fn arc_handle(&self) -> Arc<Mutex<SlurmJobManager<PyGatewayAdapter>>> {
        self.inner.clone()
    }
}
