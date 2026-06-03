//! `run_local` — entry point for the in-process local manager mode.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use crate::config::local_manager::PyLocalManagerConfig;
use crate::config::log_paths::LogPathConfig;
use crate::config::worker_spec::WorkerSpec;
use crate::managers::lifecycle::{fire_on_run_end, fire_on_run_start};

use super::module;

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
    log_dir = None,
    panik_watcher_paths = None,
    panik_watcher_poll_interval_secs = 10.0,
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
    log_dir: Option<String>,
    panik_watcher_paths: Option<Vec<std::path::PathBuf>>,
    panik_watcher_poll_interval_secs: f64,
) -> PyResult<Py<PyAny>> {
    // The legacy positional `max_memory` is kept for back-compat with
    // direct `RustLocalManager(...)` callers; the typed-config path
    // bypasses its single-key-memory shape via the `max_resources` and
    // `low_resource_thresholds` kwargs which the legacy class accepts and
    // prefers when present. No flattening here.
    let max_memory = config
        .max_resources
        .inner
        .get("memory")
        .copied()
        .unwrap_or(0);

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
    if let Some(ld) = log_dir {
        kwargs.set_item("log_dir", ld)?;
    }
    kwargs.set_item("max_resources", config.max_resources.clone())?;
    kwargs.set_item(
        "low_resource_thresholds",
        config.low_resource_thresholds.clone(),
    )?;
    kwargs.set_item("scheduler_config", config.scheduler_config.clone())?;
    kwargs.set_item(
        "phase_status_log_intervals_secs",
        config.phase_status_log_intervals_secs.clone(),
    )?;
    kwargs.set_item("stage_timeouts_secs", config.stage_timeouts_secs.clone())?;
    kwargs.set_item("log_oom_watcher", config.log_oom_watcher)?;
    kwargs.set_item("memprofile_enabled", config.memprofile_enabled)?;
    if let Some(paths) = panik_watcher_paths.as_ref() {
        kwargs.set_item("panik_watcher_paths", paths.clone())?;
    }
    kwargs.set_item(
        "panik_watcher_poll_interval_secs",
        panik_watcher_poll_interval_secs,
    )?;

    let cls = module(py)?.getattr("RustLocalManager")?;
    let args = (
        config.num_workers,
        max_memory,
        source_dir.clone(),
        output_dir.clone(),
        task_definition.clone(),
        task_args.clone(),
    );
    let manager = cls.call(args, Some(&kwargs))?;

    // Pre-run handle factory: the local manager now mints its own
    // command-channel pair at `__new__` time so `.handle()` is
    // callable BEFORE `process_binaries`. Symmetric with
    // `run_secondary` / `run_primary` / `run_distributed`: every
    // dispatch path produces the same `PyPrimaryHandle` shape so
    // modern tasks can drive `primary_handle.spawn_tasks(...)` from
    // inside their lifecycle hooks regardless of `--multi-computer`
    // mode. Legacy tasks (no `primary_handle` kwarg) fall back via
    // `fire_on_run_start`'s TypeError-retry path.
    let primary_handle = manager.call_method0("handle")?.unbind();

    // Phase 5B: fire `on_run_start` synchronously under the GIL before
    // any item dispatches. A failure here aborts the run — the
    // consumer's setup hasn't completed, so dispatching would race
    // half-built resources.
    fire_on_run_start(
        task_definition,
        &source_dir,
        &output_dir,
        task_args,
        Some(primary_handle),
    )?;

    let run_outcome = manager.call_method1("process_binaries", (binaries.clone(),));

    // Phase 5B: fire `on_run_end` regardless of whether the run
    // succeeded or errored. Exceptions out of the hook log and are
    // swallowed (we are already done — propagating would mask the real
    // outcome). The manager's own error, if any, is propagated below.
    fire_on_run_end(task_definition, run_outcome.is_ok());
    run_outcome?;

    let dict = PyDict::new(py);
    dict.set_item("stats", manager.getattr("stats")?)?;
    dict.set_item("failed_tasks", manager.getattr("failed_tasks")?)?;
    dict.set_item("oom_tasks", manager.getattr("oom_tasks")?)?;
    dict.set_item("task_results", manager.getattr("task_results")?)?;
    Ok(dict.into_any().unbind())
}
