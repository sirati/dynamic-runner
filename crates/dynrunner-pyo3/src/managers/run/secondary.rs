//! `run_secondary` — entry point for a secondary connecting to a remote primary.

use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::config::log_paths::LogPathConfig;
use crate::config::primary_secondary::PySecondaryConfig;
use crate::config::scheduler::SchedulerConfig;
use crate::config::worker_spec::WorkerSpec;
use crate::managers::lifecycle::{fire_on_run_end, fire_on_run_start};

use super::module;

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
    log_dir = None,
    scheduler_config = None,
    panik_watcher_paths = None,
    panik_watcher_poll_interval_secs = 10.0,
    finalize_run_config = None,
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
    log_dir: Option<String>,
    scheduler_config: Option<SchedulerConfig>,
    panik_watcher_paths: Option<Vec<std::path::PathBuf>>,
    panik_watcher_poll_interval_secs: f64,
    finalize_run_config: Option<Py<PyAny>>,
) -> PyResult<Py<PyAny>> {
    // Legacy positional `ram_bytes` retained for back-compat; the typed
    // path passes the full multi-resource map via the `max_resources`
    // kwarg, which the legacy class prefers when present.
    let ram_bytes = config
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
    if let Some(ld) = log_dir {
        kwargs.set_item("log_dir", ld)?;
    }
    kwargs.set_item("distributed_config", config.distributed_config.clone())?;
    kwargs.set_item("max_resources", config.max_resources.clone())?;
    if let Some(sn) = config.src_network.as_ref() {
        kwargs.set_item("src_network", sn.clone())?;
    }
    // src_tmp is non-Optional on PySecondaryConfig (always
    // resolved by `__new__`); pass it through unconditionally.
    kwargs.set_item("src_tmp", config.src_tmp.clone())?;
    // `--mem-manager-reserved` opt-in for the nested workers
    // cgroup. None means "skip nesting" so omit the kwarg and let
    // the constructor pick its default (also None); anything else
    // forwards explicitly. Mirrors the optional-kwarg shape every
    // other secondary-side opt-in flag uses (panik watcher paths,
    // worker_spec, log_dir, …).
    if let Some(reserved) = config.mem_manager_reserved_bytes {
        kwargs.set_item("mem_manager_reserved_bytes", reserved)?;
    }
    // `--memprofile` opt-in. Forwarded as a plain bool — the
    // SecondaryCoordinator resolves the actual output directory at
    // run start (via the `/app/out-network` bind-mount probe).
    kwargs.set_item("memprofile_enabled", config.memprofile_enabled)?;
    // The wrapper-pre-allocated mesh bind port (QUIC UDP + WSS TCP).
    // None means "OS-picked ephemeral" — omit the kwarg and let the
    // constructor default apply, mirroring the other optional opt-ins.
    if let Some(port) = config.quic_bind_port {
        kwargs.set_item("quic_bind_port", port)?;
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
    // The consumer's run-config — the byte-identical token sequence the
    // node re-serves on `RequestRunConfig` and threads into its promoted
    // `PrimaryConfig`. Sourced from the parsed `args.forwarded_argv`: on a
    // cold-start secondary the `_secondary_bootstrap` shim fetched it over
    // the mesh and spliced it onto `sys.argv`, so the consumer's argparse
    // re-derived the SAME value the submitter set. Absent / not-a-list (an
    // out-of-tree caller that drives `run_secondary` with a bare Namespace)
    // collapses to the constructor's empty default.
    if let Ok(fwd) = task_args.getattr("forwarded_argv")
        && let Ok(fwd) = fwd.extract::<Vec<String>>()
    {
        kwargs.set_item("forwarded_argv", fwd)?;
    }
    // The consumer's run-config finalize closure (deferred reparse). When the
    // dispatcher supplies it, the coordinator fires it after the post-welcome
    // `RunConfig` push delivers the consumer's `forwarded_argv`, BEFORE
    // workers spawn — re-deriving the per-type worker `cmd_args`. Absent
    // (out-of-tree callers) the finalize is a no-op.
    if let Some(finalize) = finalize_run_config {
        kwargs.set_item("finalize_run_config", finalize)?;
    }

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

    // Phase 5B: fire `on_run_start` synchronously under the GIL
    // before entering the secondary's coordination loop. The
    // secondary owns the source/output dirs and `task_args`; failure
    // aborts the run (consumer setup hasn't completed; dispatching
    // would race half-built resources).
    //
    // Pre-run handle factory: the secondary mints its own
    // command-channel pair at `__init__` (mirroring
    // `PyPrimaryCoordinator`), so we fetch a `PrimaryHandle` BEFORE
    // blocking on `run()`. Modern tasks can drive
    // `primary_handle.spawn_tasks(...)` from inside their
    // `on_run_start` / `on_phase_end` hooks; the commands dispatch
    // against THIS secondary's `command_rx`, so post-promotion the
    // calls land on the promoted-secondary's `primary_pending` pool.
    // Legacy positional-only `on_run_start` signatures fall back via
    // the TypeError-retry path inside `fire_on_run_start`.
    let primary_handle = coord.call_method0("handle")?.unbind();
    fire_on_run_start(
        task_definition,
        &source_dir,
        &output_dir,
        task_args,
        Some(primary_handle),
    )?;

    let run_outcome = coord.call_method0("run");

    // Phase 5B: fire `on_run_end` regardless. Exceptions log and are
    // swallowed; the coord error (if any) is propagated below.
    fire_on_run_end(task_definition, run_outcome.is_ok());
    run_outcome?;

    let dict = PyDict::new(py);
    dict.set_item("completed", coord.getattr("completed")?)?;
    Ok(dict.into_any().unbind())
}
