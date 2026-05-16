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
use crate::managers::lifecycle::{fire_on_run_end, fire_on_run_start};
use crate::pytypes::extract_binaries;

/// Compute the file_hash that the Rust primary will assign to a Python
/// `TaskInfo` when it sends a `TaskAssignment`. The hash is stable
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
    // The legacy positional `max_memory` is kept for back-compat with
    // direct `RustLocalManager(...)` callers; the typed-config path
    // bypasses its single-key-memory shape via the `max_resources` and
    // `low_resource_thresholds` kwargs which the legacy class accepts and
    // prefers when present. No flattening here.
    let max_memory = config.max_resources.inner.get("memory").copied().unwrap_or(0);

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

    // Phase 5B: fire `on_run_start` synchronously under the GIL before
    // any item dispatches. A failure here aborts the run — the
    // consumer's setup hasn't completed, so dispatching would race
    // half-built resources.
    fire_on_run_start(task_definition, &source_dir, &output_dir, task_args)?;

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

/// Run the network-based primary coordinator. Spawns secondaries via the
/// `spawn_secondary` callback (called once per `config.num_secondaries`).
///
/// `source_dir` (optional) is the local source-tree root the primary
/// reads file contents from for the initial staging walk. Threaded
/// to `RustPrimaryCoordinator.__init__(source_dir=...)` so the
/// inner coordinator's config has the root needed for the content-
/// hash + per-secondary fan-out without each caller re-implementing
/// the staging orchestration. `None` is the right default for pre-
/// staged-source mode, `uses_file_based_items=False`, and remote-
/// only primaries.
///
/// `source_pre_staged_root` (optional) carries the
/// `--source-already-staged` signal for the `--multi-computer local`
/// path: when `Some`, the Python dispatch helper has already returned
/// an empty `binaries` list and `RustPrimaryCoordinator::run` will
/// flip `required_setup_on_promote=true` so the chosen secondary runs
/// discovery + ledger-seed on its bind-mounted `src_network`.
/// Mirrors the SLURM pipeline's direct construction of
/// `RustPrimaryCoordinator(source_pre_staged_root=...)` so all three
/// multi-computer modes use the same setup-promote handshake.
#[pyfunction]
#[pyo3(signature = (
    config,
    task_definition,
    spawn_secondary,
    binaries,
    source_dir = None,
    source_pre_staged_root = None,
    unfulfillable_reinject_max_per_task = None,
    respawn_policy = None,
    respawn_spawner = None,
))]
pub(crate) fn run_primary<'py>(
    py: Python<'py>,
    config: PyRef<'py, PyPrimaryConfig>,
    task_definition: &Bound<'py, PyAny>,
    spawn_secondary: Py<PyAny>,
    binaries: &Bound<'py, PyList>,
    source_dir: Option<std::path::PathBuf>,
    source_pre_staged_root: Option<std::path::PathBuf>,
    unfulfillable_reinject_max_per_task: Option<u32>,
    respawn_policy: Option<Py<PyAny>>,
    respawn_spawner: Option<Py<PyAny>>,
) -> PyResult<Py<PyAny>> {
    let kwargs = PyDict::new(py);
    kwargs.set_item("distributed_config", config.distributed_config.clone())?;
    if let Some(sd) = source_dir.as_ref() {
        kwargs.set_item("source_dir", sd)?;
    }
    if let Some(root) = source_pre_staged_root.as_ref() {
        kwargs.set_item("source_pre_staged_root", root)?;
    }
    if let Some(n) = unfulfillable_reinject_max_per_task {
        kwargs.set_item("unfulfillable_reinject_max_per_task", n)?;
    }
    if let Some(p) = respawn_policy.as_ref() {
        kwargs.set_item("respawn_policy", p)?;
    }
    if let Some(s) = respawn_spawner.as_ref() {
        kwargs.set_item("respawn_spawner", s)?;
    }

    let cls = module(py)?.getattr("RustPrimaryCoordinator")?;
    let args = (
        config.num_secondaries,
        task_definition.clone(),
        spawn_secondary,
    );
    let coord = cls.call(args, Some(&kwargs))?;

    // Phase 5B: `run_primary` does not invoke `on_run_start` because
    // the primary entrypoint does not own a source/output dir or
    // task_args (those live on the secondaries' nodes — see
    // `run_secondary`). The per-phase hooks still fire from inside the
    // PrimaryCoordinator. `on_run_end` is fired at the end with just
    // `success`, which is well-defined here.
    let run_outcome = coord.call_method1("run", (binaries.clone(),));
    fire_on_run_end(task_definition, run_outcome.is_ok());
    run_outcome?;

    let dict = PyDict::new(py);
    dict.set_item("completed", coord.getattr("completed")?)?;
    dict.set_item("failed", coord.getattr("failed")?)?;
    // `stranded` carries the cluster-collapse counter that the
    // underlying `RustPrimaryCoordinator.run` raises a `RuntimeError`
    // on; on every successful return it's zero, but exposing the field
    // unconditionally keeps the Python-facing dict shape consistent
    // (consumers' "Completed: / Failed: / Stranded:" log line).
    dict.set_item("stranded", coord.getattr("stranded")?)?;
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
    // Legacy positional `ram_bytes` retained for back-compat; the typed
    // path passes the full multi-resource map via the `max_resources`
    // kwarg, which the legacy class prefers when present.
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
    kwargs.set_item("max_resources", config.max_resources.clone())?;
    if let Some(sn) = config.src_network.as_ref() {
        kwargs.set_item("src_network", sn.clone())?;
    }
    // src_tmp is non-Optional on PySecondaryConfig (always
    // resolved by `__new__`); pass it through unconditionally.
    kwargs.set_item("src_tmp", config.src_tmp.clone())?;

    let cls = module(py)?.getattr("RustSecondaryCoordinator")?;
    let args = (
        primary_url,
        config.secondary_id.clone(),
        config.num_workers,
        ram_bytes,
        source_dir.clone(),
        output_dir.clone(),
        task_definition.clone(),
        task_args.clone(),
    );
    let coord = cls.call(args, Some(&kwargs))?;

    // Phase 5B: fire `on_run_start` synchronously under the GIL before
    // entering the secondary's coordination loop. The secondary, not
    // the primary, owns the source/output dirs and `task_args` (see
    // the comment in `run_primary`); this is where the original Phase
    // 5B design called for `on_run_start` to fire in network/SLURM
    // mode. Failure aborts the run — consumer setup hasn't completed,
    // dispatching would race half-built resources.
    fire_on_run_start(task_definition, &source_dir, &output_dir, task_args)?;

    let run_outcome = coord.call_method0("run");

    // Phase 5B: fire `on_run_end` regardless. Exceptions log and are
    // swallowed; the coord error (if any) is propagated below.
    fire_on_run_end(task_definition, run_outcome.is_ok());
    run_outcome?;

    let dict = PyDict::new(py);
    dict.set_item("completed", coord.getattr("completed")?)?;
    Ok(dict.into_any().unbind())
}

/// Run the in-process distributed pipeline (primary + N secondaries
/// connected via in-memory channels). Useful for single-node multi-worker
/// runs without real networking.
///
/// `source_pre_staged_root` (optional) carries the
/// `--source-already-staged` signal for the `--multi-computer
/// single-process` path: forwarded to `RustDistributedManager` which
/// threads it into its `PrimaryConfig` and derives
/// `required_setup_on_promote = source_pre_staged_root.is_some()`.
/// The Python dispatch helper has already returned an empty
/// `binaries` list in pre-staged mode, so the bootstrap
/// `PromotePrimary` defers discovery + ledger-seed to the chosen
/// secondary. Mirrors the kwarg on `run_primary` and the SLURM
/// pipeline's direct `RustPrimaryCoordinator` construction so all
/// three multi-computer modes share one signal.
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
    source_pre_staged_root = None,
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
    source_pre_staged_root: Option<std::path::PathBuf>,
) -> PyResult<Py<PyAny>> {
    // Legacy positional `ram_per_secondary` retained for back-compat; the
    // typed path passes the full multi-resource map via the
    // `max_resources_per_secondary` kwarg, which the legacy class prefers
    // when present.
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
    kwargs.set_item(
        "max_resources_per_secondary",
        secondary_template.max_resources.clone(),
    )?;
    if let Some(root) = source_pre_staged_root.as_ref() {
        kwargs.set_item("source_pre_staged_root", root)?;
    }

    let cls = module(py)?.getattr("RustDistributedManager")?;
    let args = (
        primary_config.num_secondaries,
        secondary_template.num_workers,
        ram_per_secondary,
        source_dir.clone(),
        output_dir.clone(),
        task_definition.clone(),
        task_args.clone(),
    );
    let mgr = cls.call(args, Some(&kwargs))?;

    // Phase 5B: fire `on_run_start` under the GIL. Failure aborts the
    // run (consumer's setup hasn't completed; no point dispatching).
    fire_on_run_start(task_definition, &source_dir, &output_dir, task_args)?;

    let run_outcome = mgr.call_method1("run", (binaries.clone(),));

    // Phase 5B: fire `on_run_end` regardless. Exceptions log and are
    // swallowed; the manager error (if any) is propagated below.
    fire_on_run_end(task_definition, run_outcome.is_ok());
    run_outcome?;

    let dict = PyDict::new(py);
    dict.set_item("completed", mgr.getattr("completed")?)?;
    dict.set_item("failed", mgr.getattr("failed")?)?;
    // See `run_primary` above for the rationale; the in-process
    // distributed manager mirrors the same dict shape so callers
    // don't have to special-case the network vs in-process variants.
    dict.set_item("stranded", mgr.getattr("stranded")?)?;
    Ok(dict.into_any().unbind())
}
