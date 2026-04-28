//! Free-function entry points for the four runner modes.
//!
//! These are the recommended Python-facing API: build a typed config (e.g.
//! `LocalManagerConfig`), call `run_local(config, ...)`, get a result
//! object back. The legacy `Rust*Manager` / `Rust*Coordinator` classes
//! remain callable for one release as deprecated shims.
//!
//! Implementation strategy: the free functions construct the legacy
//! pyclass via the Python module, set the right kwargs, and return its
//! results as a dict. This keeps the actual run logic single-sourced in
//! the manager pyclasses themselves — no duplication of the
//! tokio-runtime / SubprocessWorkerFactory plumbing.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use crate::config::local_manager::PyLocalManagerConfig;
use crate::config::log_paths::LogPathConfig;
use crate::config::primary_secondary::{PyPrimaryConfig, PySecondaryConfig};
use crate::config::worker_spec::WorkerSpec;
use crate::pytypes::extract_binaries;

/// Compute the file_hash that the Rust primary will assign to a Python
/// `BinaryInfo` when it sends a `TaskAssignment`. The hash is stable
/// for a given (path, identifier) pair — pipelines pre-stage files
/// against this hash so the secondary's `ExtractionCache` accepts the
/// stage notification.
#[pyfunction]
pub(crate) fn compute_task_hash(py: Python<'_>, binary: &Bound<'_, PyAny>) -> PyResult<String> {
    let single = pyo3::types::PyList::new(py, [binary])?;
    let mut rust_binaries = extract_binaries(&single)?;
    let bin = rust_binaries.pop().ok_or_else(|| {
        pyo3::exceptions::PyValueError::new_err("compute_task_hash: failed to extract binary")
    })?;
    Ok(dynrunner_manager_distributed::compute_task_hash(&bin))
}

fn module<'py>(py: Python<'py>) -> PyResult<Bound<'py, PyModule>> {
    py.import("dynamic_runner")
}

/// Run the in-process local manager. Equivalent to constructing and using
/// `RustLocalManager` directly, but with a typed config object.
#[pyfunction]
#[pyo3(signature = (
    config,
    task_definition,
    task_args,
    source_dir,
    output_dir,
    binaries,
    skip_existing = false,
    connection_mode = "socketpair",
    socket_dir = None,
    manual_start_worker = false,
    log_paths = None,
    worker_spec = None,
))]
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_local<'py>(
    py: Python<'py>,
    config: PyRef<'py, PyLocalManagerConfig>,
    task_definition: &Bound<'py, PyAny>,
    task_args: &Bound<'py, PyAny>,
    source_dir: String,
    output_dir: String,
    binaries: &Bound<'py, PyList>,
    skip_existing: bool,
    connection_mode: &str,
    socket_dir: Option<String>,
    manual_start_worker: bool,
    log_paths: Option<LogPathConfig>,
    worker_spec: Option<WorkerSpec>,
) -> PyResult<Py<PyAny>> {
    let max_memory = config.max_resources.inner.get("memory").copied().unwrap_or(0);
    let low_memory_threshold = config.low_resource_thresholds.inner.get("memory").copied();

    let kwargs = PyDict::new(py);
    kwargs.set_item("skip_existing", skip_existing)?;
    kwargs.set_item("always_restart_worker", config.always_restart_worker)?;
    if let Some(cb) = config.restart_predicate.as_ref() {
        kwargs.set_item("restart_predicate", cb.clone_ref(py))?;
    }
    kwargs.set_item("retry_max_attempts", config.retry_max_attempts)?;
    kwargs.set_item("print_pid", config.print_pid)?;
    kwargs.set_item("connection_mode", connection_mode)?;
    if let Some(sd) = socket_dir {
        kwargs.set_item("socket_dir", sd)?;
    }
    kwargs.set_item("manual_start_worker", manual_start_worker)?;
    if let Some(lp) = log_paths {
        kwargs.set_item("log_paths", lp)?;
    }
    if let Some(ws) = worker_spec {
        kwargs.set_item("worker_spec", ws)?;
    }
    if let Some(t) = low_memory_threshold {
        kwargs.set_item("low_memory_threshold", t)?;
    }
    kwargs.set_item("scheduler_config", config.scheduler_config.clone())?;
    kwargs.set_item(
        "phase_status_log_intervals_secs",
        config.phase_status_log_intervals_secs.clone(),
    )?;

    let cls = module(py)?.getattr("RustLocalManager")?;
    let args = (
        config.num_workers,
        max_memory,
        source_dir,
        output_dir,
        task_definition.clone(),
        task_args.clone(),
    );
    let manager = cls.call(args, Some(&kwargs))?;
    manager.call_method1("process_binaries", (binaries.clone(),))?;

    let dict = PyDict::new(py);
    dict.set_item("stats", manager.getattr("stats")?)?;
    dict.set_item("failed_tasks", manager.getattr("failed_tasks")?)?;
    dict.set_item("oom_tasks", manager.getattr("oom_tasks")?)?;
    dict.set_item("task_results", manager.getattr("task_results")?)?;
    Ok(dict.into_any().unbind())
}

/// Run the network-based primary coordinator. Spawns secondaries via the
/// `spawn_secondary` callback (called once per `config.num_secondaries`).
#[pyfunction]
#[pyo3(signature = (
    config,
    task_definition,
    spawn_secondary,
    binaries,
))]
pub(crate) fn run_primary<'py>(
    py: Python<'py>,
    config: PyRef<'py, PyPrimaryConfig>,
    task_definition: &Bound<'py, PyAny>,
    spawn_secondary: Py<PyAny>,
    binaries: &Bound<'py, PyList>,
) -> PyResult<Py<PyAny>> {
    let kwargs = PyDict::new(py);
    kwargs.set_item("distributed_config", config.distributed_config.clone())?;

    let cls = module(py)?.getattr("RustPrimaryCoordinator")?;
    let args = (
        config.num_secondaries,
        task_definition.clone(),
        spawn_secondary,
    );
    let coord = cls.call(args, Some(&kwargs))?;
    coord.call_method1("run", (binaries.clone(),))?;

    let dict = PyDict::new(py);
    dict.set_item("completed", coord.getattr("completed")?)?;
    dict.set_item("failed", coord.getattr("failed")?)?;
    Ok(dict.into_any().unbind())
}

/// Run a secondary that connects to a remote primary at `primary_url`.
#[pyfunction]
#[pyo3(signature = (
    config,
    primary_url,
    task_definition,
    task_args,
    source_dir,
    output_dir,
    skip_existing = false,
    log_paths = None,
    worker_spec = None,
))]
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_secondary<'py>(
    py: Python<'py>,
    config: PyRef<'py, PySecondaryConfig>,
    primary_url: String,
    task_definition: &Bound<'py, PyAny>,
    task_args: &Bound<'py, PyAny>,
    source_dir: String,
    output_dir: String,
    skip_existing: bool,
    log_paths: Option<LogPathConfig>,
    worker_spec: Option<WorkerSpec>,
) -> PyResult<Py<PyAny>> {
    let ram_bytes = config.max_resources.inner.get("memory").copied().unwrap_or(0);
    let kwargs = PyDict::new(py);
    kwargs.set_item("skip_existing", skip_existing)?;
    if let Some(lp) = log_paths {
        kwargs.set_item("log_paths", lp)?;
    }
    if let Some(ws) = worker_spec {
        kwargs.set_item("worker_spec", ws)?;
    }
    kwargs.set_item("distributed_config", config.distributed_config.clone())?;
    if let Some(sn) = config.src_network.as_ref() {
        kwargs.set_item("src_network", sn.clone())?;
    }
    if let Some(st) = config.src_tmp.as_ref() {
        kwargs.set_item("src_tmp", st.clone())?;
    }

    let cls = module(py)?.getattr("RustSecondaryCoordinator")?;
    let args = (
        primary_url,
        config.secondary_id.clone(),
        config.num_workers,
        ram_bytes,
        source_dir,
        output_dir,
        task_definition.clone(),
        task_args.clone(),
    );
    let coord = cls.call(args, Some(&kwargs))?;
    coord.call_method0("run")?;

    let dict = PyDict::new(py);
    dict.set_item("completed", coord.getattr("completed")?)?;
    Ok(dict.into_any().unbind())
}

/// Run the in-process distributed pipeline (primary + N secondaries
/// connected via in-memory channels). Useful for single-node multi-worker
/// runs without real networking.
#[pyfunction]
#[pyo3(signature = (
    primary_config,
    secondary_template,
    task_definition,
    task_args,
    source_dir,
    output_dir,
    binaries,
    skip_existing = false,
    log_paths = None,
    worker_spec = None,
))]
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_distributed<'py>(
    py: Python<'py>,
    primary_config: PyRef<'py, PyPrimaryConfig>,
    secondary_template: PyRef<'py, PySecondaryConfig>,
    task_definition: &Bound<'py, PyAny>,
    task_args: &Bound<'py, PyAny>,
    source_dir: String,
    output_dir: String,
    binaries: &Bound<'py, PyList>,
    skip_existing: bool,
    log_paths: Option<LogPathConfig>,
    worker_spec: Option<WorkerSpec>,
) -> PyResult<Py<PyAny>> {
    let ram_per_secondary = secondary_template
        .max_resources
        .inner
        .get("memory")
        .copied()
        .unwrap_or(0);
    let kwargs = PyDict::new(py);
    kwargs.set_item("skip_existing", skip_existing)?;
    if let Some(lp) = log_paths {
        kwargs.set_item("log_paths", lp)?;
    }
    if let Some(ws) = worker_spec {
        kwargs.set_item("worker_spec", ws)?;
    }
    kwargs.set_item(
        "distributed_config",
        primary_config.distributed_config.clone(),
    )?;

    let cls = module(py)?.getattr("RustDistributedManager")?;
    let args = (
        primary_config.num_secondaries,
        secondary_template.num_workers,
        ram_per_secondary,
        source_dir,
        output_dir,
        task_definition.clone(),
        task_args.clone(),
    );
    let mgr = cls.call(args, Some(&kwargs))?;
    mgr.call_method1("run", (binaries.clone(),))?;

    let dict = PyDict::new(py);
    dict.set_item("completed", mgr.getattr("completed")?)?;
    dict.set_item("failed", mgr.getattr("failed")?)?;
    Ok(dict.into_any().unbind())
}
