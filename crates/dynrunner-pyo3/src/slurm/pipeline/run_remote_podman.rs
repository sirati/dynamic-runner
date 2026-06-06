//! `run_remote_podman_pipeline` — orchestrator pyfunction for
//! `--multi-computer remote-podman`.
//!
//! Single-secondary podman-on-a-remote-host dispatch. Reuses the SLURM
//! stack's gateway, packaging, wrapper-script renderer, and
//! `RustPrimaryCoordinator` driver; drops sbatch, the reverse-tunnel
//! watcher, and the shutdown-manager spawn. The wrapper's
//! `shutdown_manager_bin_path = None` branch keeps it safe on a generic
//! Linux remote (no systemd-user, no SLURM env required).
//!
//! Topology: gateway-direct. The gateway ControlMaster holds
//! `-R 0.0.0.0:port:localhost:port` (registered via
//! `gateway.setup_port_forwarding` BEFORE `connect()`); the wrapper
//! renders with `ConnectionMode::Standard{host="localhost",
//! port=primary_quic_port}` so the container — running `--network host`
//! — dials `localhost:port` and reaches the dispatcher through the
//! master's reverse-forward. Remote sshd `GatewayPorts=no` is fine
//! because the silent downgrade to `127.0.0.1:port` IS what the
//! container's `--network host` namespace sees as `localhost`.
//!
//! Per-secondary `ssh` lifecycle is owned by `RustPrimaryCoordinator`
//! through the `SubprocessSpec` spawn-callback pattern (same shape
//! `--multi-computer local` uses): the Python factory
//! `dynamic_runner.packaging.remote_podman.build_remote_podman_spawn`
//! returns a callable that synthesises the
//! `["ssh", ..., target, "bash", wrapper_remote_path]` argv. The Rust
//! primary's `Command::spawn` owns the resulting child; coordinator
//! shutdown kills the ssh process, SIGHUP propagates to the container,
//! and the wrapper's trap cleans up.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

use super::{CleanupGuard, attr_truthy};

/// `bool(getattr(obj, name, default_when_missing))` extractor for
/// argparse-style booleans where the attribute may be absent on
/// out-of-tree callers. Mirrors the local helper in
/// `super::run_pipeline`.
fn attr_bool(obj: &Bound<'_, PyAny>, name: &str, default: bool) -> bool {
    obj.getattr(name)
        .ok()
        .and_then(|v| v.extract::<bool>().ok())
        .unwrap_or(default)
}

/// Python entry point. Mirrors the signature shape of
/// `run_slurm_pipeline` (`task, args, deployment, log`) so the Python
/// dispatcher branch in `dynamic_runner.run._dispatch_remote_podman`
/// looks identical to its `_dispatch_slurm` sibling.
#[pyfunction]
#[pyo3(signature = (task, args, deployment, log))]
pub(crate) fn run_remote_podman_pipeline<'py>(
    py: Python<'py>,
    task: &Bound<'py, PyAny>,
    args: &Bound<'py, PyAny>,
    deployment: &Bound<'py, PyAny>,
    log: &Bound<'py, PyAny>,
) -> PyResult<()> {
    // ---- Pre-flight: reuse the SLURM validator. ----
    //
    // The three things it checks (`--gateway`, `--packaging`,
    // `--slurm-root-folder`) are concept-correct for remote-podman
    // too. The "slurm" naming is legacy and will be renamed in a
    // follow-up sweep; reusing it now keeps this PR focused.
    let validate = py
        .import("dynamic_runner.packaging.pipeline")?
        .getattr("_validate_slurm_args")?;
    let ok: bool = validate.call1((args, log))?.extract()?;
    if !ok {
        return Ok(());
    }

    let shared = py.import("dynamic_runner._shared")?;
    let sel_result = shared
        .getattr("process_selection_arguments")?
        .call1((args,))?;
    let source_dir = sel_result.getattr("source_dir")?;

    // Single-secondary by construction. The CLI validator
    // (`validate_parsed_args` in `cli.py`) rejects `--jobs != 1` for
    // this mode; we forward a u32 constant downstream rather than
    // reading `args.jobs` so the in-Rust contract is explicit.
    let num_secondaries: u32 = 1;
    let pkg_pipeline = py.import("dynamic_runner.packaging.pipeline")?;
    let run_id: String = pkg_pipeline.getattr("_make_run_id")?.call0()?.extract()?;
    log.call_method1("info", (format!("Run ID: {run_id}"),))?;
    log.call_method1(
        "info",
        ("remote-podman dispatch: single-secondary podman on one ssh-reachable remote host",),
    )?;

    // ---- Gateway construction. ----
    log.call_method1("info", ("Connecting to gateway...",))?;
    let pkg_gateway = py.import("dynamic_runner.packaging.gateway")?;
    let gateway_url = args.getattr("gateway")?;
    let gateway_config = pkg_gateway
        .getattr("parse_gateway_url")?
        .call1((gateway_url,))?;

    // Same auth plumbing as the SLURM pipeline — operator-supplied
    // identity/config are gateway-config concerns, attached post-parse
    // so `parse_gateway_url` signature stays URL-only.
    gateway_config.setattr(
        "ssh_identity_file",
        args.getattr("ssh_identity_file")
            .unwrap_or_else(|_| py.None().into_bound(py)),
    )?;
    gateway_config.setattr(
        "ssh_config_file",
        args.getattr("ssh_config")
            .unwrap_or_else(|_| py.None().into_bound(py)),
    )?;

    let gateway = pkg_gateway
        .getattr("create_gateway")?
        .call1((gateway_config,))?;

    // ---- Pre-pick the primary's QUIC port + register the master's
    //      reverse forward BEFORE connect(). ----
    //
    // The forward is `-R 0.0.0.0:remote:localhost:local`. On a remote
    // with `GatewayPorts=no` (the sshd default) this silently
    // downgrades to a `127.0.0.1:remote` bind — fine for this mode
    // because the secondary's container uses `--network host` and
    // therefore reads `localhost:remote` from inside the remote's own
    // 127.0.0.1. The dispatcher receives traffic on its local
    // `127.0.0.1:local` via the master's ssh session.
    let runner_module = py.import("dynamic_runner")?;
    let primary_quic_port: u16 = runner_module
        .getattr("pick_free_port")?
        .call0()?
        .extract()?;
    log.call_method1(
        "info",
        (format!(
            "Primary QUIC port: {primary_quic_port} (master holds -R \
             0.0.0.0:{primary_quic_port}:localhost:{primary_quic_port}; \
             container --network host dials localhost:{primary_quic_port})"
        ),),
    )?;
    gateway.call_method1(
        "setup_port_forwarding",
        (primary_quic_port, primary_quic_port),
    )?;

    // Consumer-supplied extra `(local, remote)` forwards on the
    // ControlMaster, same gateway-direct rationale as SLURM Standard
    // mode. Each entry becomes a second `-R 0.0.0.0:remote:localhost:local`.
    let extra_forwards = deployment.getattr("extra_port_forwards")?;
    let iter = extra_forwards.try_iter()?;
    for pair in iter {
        let pair = pair?;
        let (local_port, gw_port): (u16, u16) = pair.extract()?;
        gateway.call_method1("setup_port_forwarding", (local_port, gw_port))?;
    }

    gateway.call_method0("connect")?;
    log.call_method1(
        "info",
        (
            "Note: the gateway port-probe warning 'GatewayPorts likely disabled' is a \
          false alarm for remote-podman — container --network host reaches the \
          master's downgraded 127.0.0.1 bind transparently.",
        ),
    )?;

    // ---- Slurm config + root-folder validation/creation. ----
    //
    // `SlurmConfig` is the generic dispatch-config dataclass despite
    // its name; same path-layout (image_subfolder / output_subfolder /
    // log_subfolder under a root) applies to remote-podman.
    let slurm_config = pkg_pipeline
        .getattr("_make_slurm_config")?
        .call1((args, &gateway))?;
    let slurm_config_module = py.import("dynamic_runner.packaging.slurm_config")?;
    let validate_fn = slurm_config_module.getattr("validate_slurm_config")?;
    match validate_fn.call1((&slurm_config, &gateway)) {
        Ok(_) => {}
        Err(e) if e.is_instance_of::<pyo3::exceptions::PyValueError>(py) => {
            let root = slurm_config.getattr("root_folder")?;
            log.call_method1(
                "info",
                (format!("Creating dispatch root directory: {root}"),),
            )?;
            gateway.call_method1("create_directory", (root,))?;
        }
        Err(e) => return Err(e),
    }

    // Surface the deployment-correct output root on `args` so the
    // task's `discover_items` can drive `find_items` against the
    // same path that outputs land at.
    let resolved_output = slurm_config.call_method0("get_output_dir")?;
    args.setattr("resolved_output_root", resolved_output.str()?)?;

    // ---- Discover items (or defer to setup-promoted secondary). ----
    let binaries = PyList::empty(py);
    if !attr_truthy(args, "source_already_staged") {
        for item in task
            .call_method1("discover_items", (&source_dir, args))?
            .try_iter()?
        {
            binaries.append(item?)?;
        }
        if binaries.is_empty() {
            log.call_method1(
                "warning",
                ("No items discovered. Pipeline will still bring up the secondary container.",),
            )?;
        }
    } else {
        log.call_method1(
            "info",
            ("Pre-staged source mode: deferring task discovery to the setup-promoted secondary.",),
        )?;
    }

    // ---- Construct packaging + job_manager. ----
    //
    // No `pkill_leftover_tunnels` pre-step (unlike SLURM): the
    // dispatcher host is the operator's workstation, and the broad
    // pattern would kill unrelated ssh -R sessions they have open.
    let podman_module = py.import("dynamic_runner.packaging.podman")?;
    let podman_packaging_cls = podman_module.getattr("PodmanPackaging")?;
    let pkg_kwargs = PyDict::new(py);
    pkg_kwargs.set_item("deployment", deployment)?;
    let packaging = podman_packaging_cls.call((), Some(&pkg_kwargs))?;

    let job_manager_module = py.import("dynamic_runner.packaging.job_manager")?;
    let job_manager_cls = job_manager_module.getattr("SlurmJobManager")?;
    let job_manager = job_manager_cls.call1((&gateway, &slurm_config, &packaging, deployment))?;

    // ---- Extract per-secondary configuration (cores / memory / argv). ----
    //
    // Identical extraction shape as the SLURM pipeline so the wrapper
    // renderer sees the same kwargs from either dispatch mode.
    let cores_spec: String = args
        .getattr("cores")
        .ok()
        .and_then(|v| v.extract::<String>().ok())
        .unwrap_or_else(|| "0".into());
    let max_memory_spec: String = args
        .getattr("max_memory")
        .ok()
        .and_then(|v| v.extract::<String>().ok())
        .unwrap_or_else(|| "-2G".into());
    let mem_manager_reserved_bytes: Option<u64> = args
        .getattr("mem_manager_reserved")
        .ok()
        .and_then(|v| {
            if v.is_none() {
                None
            } else {
                v.extract::<String>().ok()
            }
        })
        .and_then(|s| crate::system_resources::parse_memory(&s).ok());
    let skip_image_build: bool = attr_bool(args, "skip_image_build", false);

    // ---- try/finally guard. Owns gateway; no tunnel manager — the
    //      `CleanupGuard::set_tunnel_manager` slot is never invoked
    //      for remote-podman because we don't run the reverse-tunnel
    //      watcher. Drop reduces to `gateway.disconnect()` (which
    //      tears down the master + its `-R`) plus the no-op
    //      `pkill_residual_tunnels` step. ----
    let guard = CleanupGuard::new(gateway.clone().unbind());

    let pipeline_result: PyResult<()> = (|| {
        // ---- Phase 1: gateway-side prep + image staging. ----
        log.call_method1("info", ("Phase 1: directory prep + image staging",))?;
        let base_log_dir: String = slurm_config.call_method0("get_log_dir")?.str()?.extract()?;
        let run_log_dir = format!("{base_log_dir}/{run_id}");

        job_manager.call_method0("prepare_directories")?;
        gateway.call_method1("create_directory", (&run_log_dir,))?;

        // Image build + transfer (or skip-build branch — same shape as
        // `crates/dynrunner-pyo3/src/slurm/pipeline/preparation.rs`).
        let image_metadata = if skip_image_build {
            log.call_method1(
                "info",
                ("Skipping image build and transfer (--skip-image-build)",),
            )?;
            let image_dir = job_manager.call_method1(
                "_expand_path",
                (slurm_config.call_method0("get_image_dir")?,),
            )?;
            let pathlib = py.import("pathlib")?;
            let image_dir_path = pathlib.getattr("Path")?.call1((image_dir,))?;
            let image_tar_basename = deployment.getattr("image_tar_basename")?;
            let image_path = image_dir_path.call_method1("__truediv__", (image_tar_basename,))?;
            log.call_method1("info", (format!("Assuming image exists at: {image_path}"),))?;
            let metadata_cls = podman_module.getattr("PodmanImageMetadata")?;
            let metadata_kwargs = PyDict::new(py);
            metadata_kwargs.set_item("remote_path", image_path)?;
            metadata_kwargs.set_item("image_hash", "")?;
            metadata_kwargs.set_item("uploaded", false)?;
            metadata_cls.call((), Some(&metadata_kwargs))?
        } else {
            let project_root = py
                .import("pathlib")?
                .getattr("Path")?
                .call0()?
                .call_method0("cwd")?;
            let metadata =
                job_manager.call_method1("build_and_transfer_images", (project_root,))?;
            let uploaded: bool = metadata.getattr("uploaded")?.extract().unwrap_or(false);
            let remote_path = metadata.getattr("remote_path")?;
            log.call_method1(
                "info",
                (format!(
                    "Image {} at: {}",
                    if uploaded { "uploaded" } else { "reused" },
                    remote_path
                ),),
            )?;
            metadata
        };

        // ---- Phase 2: render wrapper script + upload to remote. ----
        //
        // Wrapper is rendered ONCE: every value the renderer needs is
        // determined at orchestrator time (primary port pre-picked,
        // secondary_id hardcoded "secondary-0", connection-mode is
        // Standard with localhost). The spawn callback's argv contains
        // only `bash <wrapper_remote_path>` — no per-spawn dynamic
        // values flow through the SubprocessSpec.
        log.call_method1("info", ("Phase 2: rendering wrapper script",))?;
        let secondary_id = "secondary-0";
        let wrapper_kwargs = PyDict::new(py);
        wrapper_kwargs.set_item("image_metadata", &image_metadata)?;
        wrapper_kwargs.set_item("secondary_id", secondary_id)?;
        wrapper_kwargs.set_item("gateway_host", "localhost")?;
        wrapper_kwargs.set_item("gateway_port", primary_quic_port)?;
        wrapper_kwargs.set_item("cores_spec", &cores_spec)?;
        wrapper_kwargs.set_item("max_memory_spec", &max_memory_spec)?;
        wrapper_kwargs.set_item("reverse_connection", false)?;
        wrapper_kwargs.set_item("run_log_dir", &run_log_dir)?;
        // `None` for shutdown_manager_bin_path → no out-of-cgroup
        // shutdown-manager spawn block; the wrapper's cleanup trap
        // reduces to the CMD_RELAY-only teardown. Safe on a remote
        // that doesn't have systemd-user-linger configured.
        wrapper_kwargs.set_item("shutdown_manager_bin_path", py.None())?;
        if let Some(reserved) = mem_manager_reserved_bytes {
            wrapper_kwargs.set_item("mem_manager_reserved_bytes", reserved)?;
        }
        let wrapper_script: String = job_manager
            .call_method("generate_wrapper_script", (), Some(&wrapper_kwargs))?
            .extract()?;

        // Wrapper path on remote — include run_id so two concurrent
        // dispatches from the same operator host don't clobber each
        // other's wrapper (the rest of the SLURM/podman layout is
        // already per-run via `<log_subfolder>/<run_id>/`).
        let wrapper_remote_path = format!("{run_log_dir}/{secondary_id}-{run_id}.sh");
        upload_wrapper_to_remote(py, &gateway, &wrapper_script, &wrapper_remote_path, &run_id)?;
        log.call_method1(
            "info",
            (format!("Wrapper script written to: {wrapper_remote_path}"),),
        )?;

        // ---- Phase 3: source-binary upload. ----
        //
        // Same gating shape as the SLURM pipeline: skip in pre-staged
        // mode, skip when the task says items aren't file-based, skip
        // when the dispatcher discovered no items.
        let uses_file_based_items: bool = task
            .getattr("uses_file_based_items")
            .ok()
            .and_then(|v| v.extract().ok())
            .unwrap_or(true);
        if !binaries.is_empty()
            && uses_file_based_items
            && !attr_truthy(args, "source_already_staged")
        {
            job_manager.call_method1("upload_source_binaries", (&binaries, &source_dir))?;
        }

        // ---- Phase 4: hand off to the Rust primary coordinator. ----
        log.call_method1("info", ("Phase 3: starting primary coordinator",))?;
        let spawn_callback = py
            .import("dynamic_runner.packaging.remote_podman")?
            .getattr("build_remote_podman_spawn")?
            .call1((&gateway, &wrapper_remote_path))?;

        // Direct `RustPrimaryCoordinator` construction (not via the
        // `run_primary` pyfn) because we need to pass `listen_port` —
        // a kwarg only the coordinator constructor accepts. The
        // SLURM `drive_rust_primary` does the same.
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
        // The operator's run-config: the same filtered tokens the spawned
        // secondary fetches + re-serves. Sourced from the operator's
        // `args.forwarded_argv` (the launch-path cutover dropped the
        // `--forwarded-arg`/wrapper-CLI plumbing, so the value now flows
        // only through this config kwarg) and threaded into the primary's
        // node-local `forwarded_argv` so the `RequestRunConfig` responder
        // serves the real argv (same shape as `drive_rust_primary`).
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
        let source_dir_str = sel_result.getattr("source_dir")?.str()?;
        coord_kwargs.set_item("source_dir", source_dir_str)?;

        // Task-protocol attribute pass-through (same shape as
        // `drive_rust_primary`).
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

        // OOM preempt-margin knobs (scheduler_config).
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

        // Panik-watcher CLI flags.
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

        let args_tuple = PyTuple::new(
            py,
            [
                num_secondaries.into_pyobject(py)?.into_any().unbind(),
                task.clone().unbind(),
                spawn_callback.unbind(),
            ],
        )?;
        let coord = coord_cls.call(args_tuple, Some(&coord_kwargs))?;

        // ---- StageFile gating: same three-way switch as SLURM. ----
        let coord_uses_file_based: bool = coord.getattr("uses_file_based_items")?.extract()?;
        if !coord_uses_file_based {
            log.call_method1(
                "info",
                ("TaskDefinition.uses_file_based_items=False; \
                     skipping primary StageFile pass and starting coordinator",),
            )?;
        } else if attr_truthy(args, "source_already_staged") {
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
            let source_dir_again = sel_result.getattr("source_dir")?;
            coord.call_method1(
                "queue_initial_staging",
                (&binaries, source_dir_again.str()?),
            )?;
            log.call_method1(
                "info",
                (
                    "Queued %d StageFile notifications for the secondary; starting coordinator",
                    binaries.len(),
                ),
            )?;
        }

        // ---- on_run_start / coord.run / on_run_end. ----
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

        let run_outcome = coord.call_method1("run", (&binaries,));
        crate::managers::lifecycle::fire_on_run_end(task, run_outcome.is_ok());
        run_outcome?;

        let completed = coord.getattr("completed")?;
        let failed = coord.getattr("failed")?;
        let stranded = coord.getattr("stranded")?;
        log.call_method1("info", (format!("Completed: {completed}"),))?;
        log.call_method1("info", (format!("Failed: {failed}"),))?;
        log.call_method1("info", (format!("Stranded: {stranded}"),))?;

        Ok(())
    })();

    drop(guard);
    pipeline_result
}

/// Write the rendered wrapper string to a local temp file, transfer
/// it to the remote via the gateway's `transfer_file`, then `chmod +x`
/// the remote copy. Pure file movement — no orchestration.
///
/// Local temp file lives under `std::env::temp_dir()` with the run
/// id baked in so concurrent dispatches don't collide; removed on
/// success or failure (best-effort).
fn upload_wrapper_to_remote(
    py: Python<'_>,
    gateway: &Bound<'_, PyAny>,
    wrapper_script: &str,
    wrapper_remote_path: &str,
    run_id: &str,
) -> PyResult<()> {
    let local_path = std::env::temp_dir().join(format!("dynrunner-remote-podman-{run_id}.sh"));
    std::fs::write(&local_path, wrapper_script.as_bytes()).map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!(
            "failed to write local wrapper temp file {}: {e}",
            local_path.display()
        ))
    })?;

    let result = (|| -> PyResult<()> {
        let pathlib = py.import("pathlib")?;
        let local_py = pathlib
            .getattr("Path")?
            .call1((local_path.to_string_lossy().to_string(),))?;
        gateway.call_method1("transfer_file", (local_py, wrapper_remote_path))?;
        gateway.call_method1(
            "execute_command",
            (format!("chmod +x {wrapper_remote_path}"),),
        )?;
        Ok(())
    })();

    let _ = std::fs::remove_file(&local_path);
    result
}
