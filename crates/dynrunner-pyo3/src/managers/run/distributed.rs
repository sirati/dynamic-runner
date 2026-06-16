//! `run_distributed` — entry point for the in-process distributed pipeline
//! (primary + N secondaries over in-memory channels).

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use crate::config::log_paths::LogPathConfig;
use crate::config::primary_secondary::{PyPrimaryConfig, PySecondaryConfig};
use crate::config::scheduler::SchedulerConfig;
use crate::config::worker_spec::WorkerSpec;
use crate::managers::lifecycle::{fire_on_run_end, fire_on_run_start};

use super::module;

/// Run the in-process distributed pipeline (primary + N secondaries
/// connected via in-memory channels). Useful for single-node multi-worker
/// runs without real networking.
///
/// `source_pre_staged_root` (optional) carries the
/// `--source-already-staged` signal for the `--multi-computer
/// single-process` path: forwarded to `RustDistributedManager` which
/// threads it into its `PrimaryConfig`, driving the secondary's
/// pre-staged binary resolution (the bind-mount IS the contract).
/// Mirrors the kwarg on `run_primary` and the SLURM pipeline's direct
/// `RustPrimaryCoordinator` construction so all three multi-computer
/// modes share one signal.
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
    stage_via_setup_tasks = false,
    fulfillability_matcher = None,
    peer_lifecycle_listener = None,
    task_completed_listener = None,
    upload_action = None,
    unfulfillable_reinject_max_per_task = None,
    log_dir = None,
    scheduler_config = None,
    panik_watcher_paths = None,
    panik_watcher_poll_interval_secs = 10.0,
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
    stage_via_setup_tasks: bool,
    fulfillability_matcher: Option<Py<PyAny>>,
    peer_lifecycle_listener: Option<Py<PyAny>>,
    task_completed_listener: Option<Py<PyAny>>,
    upload_action: Option<Py<PyAny>>,
    unfulfillable_reinject_max_per_task: Option<u32>,
    log_dir: Option<String>,
    scheduler_config: Option<SchedulerConfig>,
    panik_watcher_paths: Option<Vec<std::path::PathBuf>>,
    panik_watcher_poll_interval_secs: f64,
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
    // Framework file-staging selector (#489 P3/P4): forward the
    // `--stage-via-setup-tasks` flag to `RustDistributedManager`, which
    // threads it into the bootstrap primary's + every promote recipe's
    // `PrimaryConfig.staging_strategy`. Only set when on (the constructor
    // defaults to `false` = the old StageFile path).
    if stage_via_setup_tasks {
        kwargs.set_item("stage_via_setup_tasks", true)?;
    }
    if let Some(m) = fulfillability_matcher.as_ref() {
        kwargs.set_item("fulfillability_matcher", m)?;
    }
    if let Some(l) = peer_lifecycle_listener.as_ref() {
        kwargs.set_item("peer_lifecycle_listener", l)?;
    }
    if let Some(l) = task_completed_listener.as_ref() {
        kwargs.set_item("task_completed_listener", l)?;
    }
    // (#577) `import_action` is GONE — gate bodies run in worker
    // subprocesses dispatched via the normal task-dispatch path. The
    // consumer registers a `TaskTypeSpec` whose `worker_module` holds the
    // `@task_function` handler.
    // The consumer's #336 P1 / #493 option-A upload callable (e.g.
    // `SlurmJobManager.upload_task_file`). Forwarded into
    // `RustDistributedManager`, which installs it via `set_upload_action` on
    // the in-process primary BEFORE `run()` enters. Mirrors the SLURM-path
    // `run_primary` plumbing; the upload-action is a PRIMARY-side concern
    // (the source-owning member is the upload affinity), distinct from the
    // per-secondary `import_action` above. Only set when on (the
    // constructor defaults to `None`, leaving any setup task that asks for
    // an upload to fail loud with a wiring error).
    if let Some(action) = upload_action.as_ref() {
        kwargs.set_item("upload_action", action)?;
    }
    if let Some(cap) = unfulfillable_reinject_max_per_task {
        kwargs.set_item("unfulfillable_reinject_max_per_task", cap)?;
    }
    if let Some(ld) = log_dir {
        kwargs.set_item("log_dir", ld)?;
    }
    if let Some(sc) = scheduler_config.as_ref() {
        kwargs.set_item("scheduler_config", sc.clone())?;
    }
    if let Some(paths) = panik_watcher_paths.as_ref() {
        kwargs.set_item("panik_watcher_paths", paths.clone())?;
    }
    kwargs.set_item(
        "panik_watcher_poll_interval_secs",
        panik_watcher_poll_interval_secs,
    )?;
    // Forward the secondary template's `--memprofile` opt-in to
    // the manager so the per-secondary `SecondaryConfig.output_dir`
    // composed at `run()` entry can be plumbed uniformly with the
    // out-of-process secondary path. Without this, the in-process
    // pipeline silently dropped the flag and `--memprofile` on
    // single-process runs produced no `.jsonl.zst` files.
    kwargs.set_item("memprofile_enabled", secondary_template.memprofile_enabled)?;
    // The operator's run-config (parsed `args.forwarded_argv`), seeded onto
    // every in-process node's node-local `forwarded_argv` so the
    // `RequestRunConfig` responder answers uniformly. Absent / not-a-list
    // (an out-of-tree caller driving the manager with a bare Namespace)
    // collapses to the constructor's empty default.
    if let Ok(fwd) = task_args.getattr("forwarded_argv")
        && let Ok(fwd) = fwd.extract::<Vec<String>>()
    {
        kwargs.set_item("forwarded_argv", fwd)?;
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
    //
    // Pre-run handle factory: the in-process distributed manager mints
    // the command-channel pair at `__init__` (mirroring
    // `PyPrimaryCoordinator`), so we fetch a `PrimaryHandle` BEFORE
    // blocking on `run()`. Modern tasks can drive
    // `primary_handle.spawn_tasks(...)` from inside their
    // `on_run_start` hook; legacy positional-only `on_run_start`
    // signatures fall back via the TypeError-retry path inside
    // `fire_on_run_start`.
    let primary_handle = mgr.call_method0("handle")?.unbind();
    fire_on_run_start(
        task_definition,
        &source_dir,
        &output_dir,
        task_args,
        Some(primary_handle),
    )?;

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
