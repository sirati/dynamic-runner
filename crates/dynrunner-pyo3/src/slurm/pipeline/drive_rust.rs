//! `drive_rust_primary` — hand the run over to
//! `RustPrimaryCoordinator`. Ports the `_drive_rust_primary` helper
//! from pipeline.py.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

use super::attr_truthy;
use super::preparation::PreparationOutcome;
use super::respawn::build_slurm_respawn_kwargs;

/// Hand the run over to `RustPrimaryCoordinator`. Ports the
/// `_drive_rust_primary` helper from pipeline.py.
///
/// `binaries` is the already-discovered list — passed through rather
/// than re-discovered so both halves see the exact same set.
/// `outcome.num_secondaries` was previously read off a Python
/// `PreparationResult.num_secondaries` attribute; the field is the
/// same data, just carried in a Rust struct now.
#[allow(clippy::too_many_arguments)]
pub(super) fn drive_rust_primary<'py>(
    py: Python<'py>,
    task: &Bound<'py, PyAny>,
    args: &Bound<'py, PyAny>,
    outcome: &PreparationOutcome,
    primary_quic_port: u16,
    binaries: &Bound<'py, PyList>,
    slurm_config: &Bound<'py, PyAny>,
    job_manager: &Bound<'py, PyAny>,
    tunnel_manager: Option<Py<PyAny>>,
    cores_spec: &str,
    max_memory_spec: &str,
    use_reverse_connection: bool,
    mem_manager_reserved_bytes: Option<u64>,
    log: &Bound<'py, PyAny>,
) -> PyResult<()> {
    let runner_module = py.import("dynamic_runner")?;
    let shared = py.import("dynamic_runner._shared")?;
    let sel_result = shared
        .getattr("process_selection_arguments")?
        .call1((args,))?;

    // SLURM did the actual spawning; the spawn_secondary callback
    // is a no-op (defined in `pipeline.py` as
    // `_slurm_already_spawned`). Returning None tells the Rust side
    // it doesn't own a process to clean up at the end.
    let no_spawn_callback = py
        .import("dynamic_runner.packaging.pipeline")?
        .getattr("_slurm_already_spawned")?;

    let distributed_config = match args.getattr("retry_max_passes") {
        Ok(v) if !v.is_none() => {
            let dc_cls = runner_module.getattr("DistributedConfig")?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("retry_max_passes", v)?;
            Some(dc_cls.call((), Some(&kwargs))?)
        }
        _ => None,
    };

    let coord_cls = runner_module.getattr("RustPrimaryCoordinator")?;
    let coord_kwargs = PyDict::new(py);
    if let Some(dc) = distributed_config.as_ref() {
        coord_kwargs.set_item("distributed_config", dc)?;
    }
    coord_kwargs.set_item("listen_port", primary_quic_port)?;
    // The operator's run-config: the same filtered token sequence the
    // mesh-launched secondaries fetch + re-serve. Sourced from the
    // operator's `args.forwarded_argv` (the launch-path cutover dropped
    // the `--forwarded-arg` respawn-CLI plumbing, so the value now flows
    // only through this config kwarg) and threaded into the submitter
    // primary's `PrimaryConfig.forwarded_argv` so the `RequestRunConfig`
    // responder serves the real argv (not an empty default) — the single
    // source every cold-start / promoted node reconstructs from.
    let forwarded_argv: Vec<String> = args
        .getattr("forwarded_argv")
        .ok()
        .and_then(|v| v.extract::<Vec<String>>().ok())
        .unwrap_or_default();
    coord_kwargs.set_item("forwarded_argv", forwarded_argv)?;
    if attr_truthy(args, "source_already_staged") {
        let root = slurm_config.call_method0("get_srcbins_mount_source")?;
        coord_kwargs.set_item("source_pre_staged_root", root)?;
    }
    // Thread source_dir into the coordinator's config uniformly.
    // The SLURM pipeline retains its explicit
    // `queue_initial_staging` pre-call below (it depends on
    // `pre_staged_root` resolution that's unique to this caller),
    // so the field is supplied for parity with the in-process and
    // network-primary callers — keeps a single source of truth at
    // the manager boundary.
    let source_dir_str = sel_result.getattr("source_dir")?.str()?;
    coord_kwargs.set_item("source_dir", source_dir_str)?;

    // ---- SLURM respawn wiring. ----
    //
    // Single concern at this call site: build the per-deployment
    // SLURM respawn policy + spawner pair from (a) the CLI flags
    // (`--respawn-policy`, `--respawn-max-per-secondary`, …) and
    // (b) the live SLURM pipeline state (`job_manager._rust`'s
    // `Arc<Mutex<SlurmJobManager<...>>>`, the tunnel manager's
    // `Arc<SlurmPreparation>`, the deployment context the wrapper-
    // script generator needs). The coordinator's `enable_respawn`
    // call at run() entry consumes both kwargs through the same
    // boundary the in-process multi-process path uses — no
    // hot-site `if multi_computer == slurm` branches.
    //
    // Disabled (the default) leaves both kwargs unset; the
    // coordinator's CCD-5 gate keeps the respawn pipeline structurally
    // unreachable.
    let respawn_policy_name: String = args
        .getattr("respawn_policy")
        .ok()
        .and_then(|v| v.extract::<String>().ok())
        .unwrap_or_else(|| "disabled".into());
    if respawn_policy_name != "disabled" {
        let respawn_pyobjs = build_slurm_respawn_kwargs(
            py,
            args,
            job_manager,
            tunnel_manager.as_ref(),
            outcome,
            primary_quic_port,
            cores_spec,
            max_memory_spec,
            use_reverse_connection,
            mem_manager_reserved_bytes,
            log,
        )?;
        if let Some((policy, spawner)) = respawn_pyobjs {
            coord_kwargs.set_item("respawn_policy", policy)?;
            coord_kwargs.set_item("respawn_spawner", spawner)?;
        }
    }

    // ---- Task-protocol attribute pass-through. ----
    //
    // The task object owns optional `fulfillability_matcher`,
    // `peer_lifecycle_listener`, and `task_completed_listener`
    // attributes (same duck-typed shape as `discover_items` /
    // `add_task_arguments`). Forward them into the coordinator
    // kwargs so the SLURM pipeline matches the in-process local /
    // distributed paths — consumers don't have to construct
    // `RustPrimaryCoordinator` themselves to reach these hooks.
    if let Ok(matcher) = task.getattr("fulfillability_matcher")
        && !matcher.is_none()
    {
        coord_kwargs.set_item("fulfillability_matcher", matcher)?;
    }
    if let Ok(listener) = task.getattr("peer_lifecycle_listener")
        && !listener.is_none()
    {
        coord_kwargs.set_item("peer_lifecycle_listener", listener)?;
    }
    if let Ok(listener) = task.getattr("task_completed_listener")
        && !listener.is_none()
    {
        coord_kwargs.set_item("task_completed_listener", listener)?;
    }

    // Forward the OOM preempt-margin knobs through to the
    // `RustPrimaryCoordinator`'s `scheduler_config` kwarg so the SLURM
    // path tunes the inner scheduler with the same operator-supplied
    // values as the in-process / local-multi-computer paths. The
    // argparse Namespace carries the unparsed M/G-suffixed strings; we
    // parse them with the same `parse_memory` helper Python uses.
    // Missing kwargs / `None` values keep `SchedulerConfig::default()`
    // (1 GiB safety margin, 500 MiB pressure threshold) so an older
    // operator who never passes the flags still gets the safer default.
    let sc_kwargs = PyDict::new(py);
    let mut sc_kwargs_populated = false;
    if let Ok(v) = args.getattr("oom_cgroup_safety_margin")
        && !v.is_none()
    {
        let bytes = crate::system_resources::parse_memory(v.extract::<&str>()?)?;
        sc_kwargs.set_item("cgroup_safety_margin", bytes)?;
        sc_kwargs_populated = true;
    }
    if let Ok(v) = args.getattr("oom_pressure_threshold")
        && !v.is_none()
    {
        let bytes = crate::system_resources::parse_memory(v.extract::<&str>()?)?;
        sc_kwargs.set_item("pressure_threshold", bytes)?;
        sc_kwargs_populated = true;
    }
    if sc_kwargs_populated {
        let sc_cls = runner_module.getattr("SchedulerConfig")?;
        let sc = sc_cls.call((), Some(&sc_kwargs))?;
        coord_kwargs.set_item("scheduler_config", sc)?;
    }

    // Panik-watcher CLI flags from the argparse Namespace.
    // `args.panik_file_paths` is a `list[str]` (action="append" on
    // the CLI), `args.panik_poll_interval_secs` is `Optional[float]`.
    // Mirrors the OOM safety-margin plumbing above: read the
    // Namespace, parse/convert, attach to `coord_kwargs`. Missing
    // attribute / `None` value keeps the
    // `RustPrimaryCoordinator.__init__` default (10s poll, empty
    // paths = no watcher).
    if let Ok(v) = args.getattr("panik_file_paths")
        && !v.is_none()
    {
        let paths: Vec<String> = v.extract()?;
        let path_bufs: Vec<std::path::PathBuf> =
            paths.into_iter().map(std::path::PathBuf::from).collect();
        coord_kwargs.set_item("panik_watcher_paths", path_bufs)?;
    }
    if let Ok(v) = args.getattr("panik_poll_interval_secs")
        && !v.is_none()
    {
        let secs: f64 = v.extract()?;
        coord_kwargs.set_item("panik_watcher_poll_interval_secs", secs)?;
    }

    let num_secondaries = outcome.num_secondaries;
    let args_tuple = PyTuple::new(
        py,
        [
            num_secondaries.into_pyobject(py)?.into_any().unbind(),
            task.clone().unbind(),
            no_spawn_callback.unbind(),
        ],
    )?;
    let coord = coord_cls.call(args_tuple, Some(&coord_kwargs))?;

    // Park the SLURM `JobManager` on the coordinator so the respawn
    // path can submit a fresh 1-node sbatch from inside the operational
    // loop. Single concern at this call site: bridge the in-process
    // Rust manager from the SLURM pipeline into the coordinator —
    // before `coord.run()` enters, after preparation already produced
    // a live manager. Skipped silently if `job_manager` is not the
    // expected duck-typed shape (out-of-tree callers that subclass
    // the shim won't have a `_rust` attribute; logging it here would
    // be noise for those paths).
    if let Ok(rust_handle) = job_manager.getattr("_rust")
        && let Ok(rust_jm) = rust_handle.cast::<crate::slurm::PyRustSlurmJobManager>()
    {
        let arc: std::sync::Arc<dyn std::any::Any + Send + Sync> = rust_jm.borrow().arc_handle();
        coord
            .cast::<crate::managers::primary::PyPrimaryCoordinator>()?
            .borrow_mut()
            .set_slurm_job_manager_from_rust(arc);
    }

    let coord_uses_file_based: bool = coord.getattr("uses_file_based_items")?.extract()?;

    if !coord_uses_file_based {
        // Non-file-based items: framework does no primary-side
        // staging at all; secondary passes `local_path` through to
        // the worker as an opaque identifier.
        log.call_method1(
            "info",
            ("TaskDefinition.uses_file_based_items=False; \
                 skipping primary StageFile pass and starting coordinator",),
        )?;
    } else if attr_truthy(args, "source_already_staged") {
        // Pre-staged mode: secondaries see source via bind-mount;
        // no primary-side staging needed.
        let staged = args.getattr("source_already_staged")?;
        log.call_method1(
            "info",
            (
                "Pre-staged source mode (--source-already-staged=%s); \
                 skipping primary StageFile pass and starting coordinator",
                staged,
            ),
        )?;
    } else {
        // Bulk-queue StageFile notifications in Rust — single
        // PyO3 crossing for the whole binary list.
        let source_dir = sel_result.getattr("source_dir")?;
        coord.call_method1("queue_initial_staging", (binaries, source_dir.str()?))?;
        log.call_method1(
            "info",
            (
                "Queued %d StageFile notifications across %d secondaries; starting coordinator",
                binaries.len(),
                num_secondaries,
            ),
        )?;
    }

    // Fire `on_run_start(source_dir, output_dir, args,
    // primary_handle=...)` under the GIL before `coord.run(...)`
    // enters its detached tokio runtime. The handle is minted off
    // the coordinator pre-`run()` — `RustPrimaryCoordinator.handle`
    // supports pre-run construction so the Python caller can hand
    // the handle to an executor / thread before the blocking
    // `run()` starts. Legacy tasks without the `primary_handle`
    // kwarg fall back to the positional shape via the bridge
    // (see `crate::managers::lifecycle::fire_on_run_start`).
    // Failure aborts the run — consumer setup hasn't completed.
    let on_run_start_source_dir: String = sel_result.getattr("source_dir")?.str()?.extract()?;
    let on_run_start_output_dir: String = slurm_config
        .call_method0("get_output_dir")?
        .str()?
        .extract()?;
    let primary_handle = coord.call_method0("handle")?.unbind();
    crate::managers::lifecycle::fire_on_run_start(
        task,
        &on_run_start_source_dir,
        &on_run_start_output_dir,
        args,
        Some(primary_handle),
    )?;

    let run_outcome = coord.call_method1("run", (binaries,));
    crate::managers::lifecycle::fire_on_run_end(task, run_outcome.is_ok());
    run_outcome?;
    let completed = coord.getattr("completed")?;
    let failed = coord.getattr("failed")?;
    // Stranded mirrors `RustPrimaryCoordinator.stranded` and is zero on
    // every successful return — the cluster-collapse path raises a
    // `RuntimeError` that propagates through `?` above before we get
    // here. Logged unconditionally so the SLURM-pipeline output stays
    // shape-compatible with the in-process / network-primary variants.
    let stranded = coord.getattr("stranded")?;
    log.call_method1("info", (format!("Completed: {completed}"),))?;
    log.call_method1("info", (format!("Failed: {failed}"),))?;
    log.call_method1("info", (format!("Stranded: {stranded}"),))?;
    Ok(())
}
