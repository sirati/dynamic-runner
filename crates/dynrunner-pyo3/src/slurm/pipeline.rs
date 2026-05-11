//! `_native.run_slurm_pipeline` — PyO3 entry point for SLURM mode.
//!
//! Ports `python/dynamic_runner/packaging/pipeline.py::run_slurm_pipeline`
//! step-for-step. Each step calls the public Python facade on the
//! `dynamic_runner` module by its stable public name; thin-shim
//! migration of the underlying types (gateway, job_manager,
//! preparation) leaves those names intact, so this orchestrator does
//! not need to be edited as those types switch from pure-Python to
//! pyclass-wrapped Rust.
//!
//! ## Why orchestrate at the PyO3 layer (not pure Rust)?
//!
//! `run_slurm_pipeline` composes the gateway, the podman packaging,
//! the job manager, the slurm preparation phase, and the
//! `RustPrimaryCoordinator`, plus the `TaskDefinition` Protocol and
//! the `argparse.Namespace` / `TaskDeploymentSpec` payloads. Several
//! of those types currently exist only on the Python side (their
//! Rust counterparts are landing in sibling migration units).
//! Orchestrating at the PyO3 layer lets us land the orchestration
//! itself now — faithful sequence, correct teardown ordering
//! enforced as Rust code — without blocking on those Rust types.
//!
//! See `crates/dynrunner-slurm/src/pipeline.rs` for the structural
//! skeleton of the future pure-Rust orchestrator (boundary trait,
//! cleanup-ordering invariant, shared pkill primitive). When the
//! Rust gateway / preparation / job_manager types land, the body
//! here reduces to constructing them and calling that pure-Rust
//! composition.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

/// `bool(getattr(obj, name, None))` — handles missing-attr +
/// None-attr the same way Python does. Centralises the
/// `getattr-then-truthy` pattern used at the gating sites for
/// `--source-already-staged` (and any future similarly-shaped CLI
/// flag).
fn attr_truthy(obj: &Bound<'_, PyAny>, name: &str) -> bool {
    obj.getattr(name)
        .ok()
        .map(|v| !v.is_none() && v.is_truthy().unwrap_or(false))
        .unwrap_or(false)
}

/// Drop-guard that runs the strict teardown order
/// (`preparation.cleanup()` → `gateway.disconnect()` → tightened
/// `pkill`) on scope exit. Modeled on Python's `try/finally` block
/// in `pipeline.py::run_slurm_pipeline`. The order is invariant —
/// see the `pkill_residual_reverse_tunnels` doc in `dynrunner-slurm`
/// for why disconnect MUST precede pkill.
///
/// * Holds `Py<PyAny>` references to the live `preparation` and
///   `gateway` instances. `Option<...>` shape so an early-failure
///   path can construct the guard with what it has so far and the
///   `Drop` skips the missing steps.
/// * Each step is best-effort: a failure logs but does not abort the
///   remaining steps. Same semantics as Python's `try/finally` chain
///   where the gateway disconnect runs even if preparation cleanup
///   raised.
struct CleanupGuard {
    preparation: Option<Py<PyAny>>,
    gateway: Option<Py<PyAny>>,
}

impl CleanupGuard {
    fn new(gateway: Py<PyAny>) -> Self {
        Self {
            preparation: None,
            gateway: Some(gateway),
        }
    }

    fn set_preparation(&mut self, preparation: Py<PyAny>) {
        self.preparation = Some(preparation);
    }
}

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        Python::attach(|py| {
            // Step 1: per-secondary tunnel cleanup (tracked in
            // `SlurmPreparation.ssh_tunnels`).
            if let Some(prep) = self.preparation.take() {
                if let Err(e) = prep.bind(py).call_method0("cleanup") {
                    tracing::warn!(error = ?e, "preparation.cleanup() failed");
                }
            }
            // Step 2: graceful gateway-master shutdown FIRST. This
            // takes the master and all its `-R` forwardings down via
            // `ssh -O exit`. Must happen BEFORE the targeted pkill
            // below — otherwise pkill SIGTERMs the master before its
            // graceful exit completes and we get spurious "Control
            // socket connect: No such file or directory" warnings.
            if let Some(gw) = self.gateway.take() {
                if let Err(e) = gw.bind(py).call_method0("disconnect") {
                    tracing::warn!(error = ?e, "gateway.disconnect() failed");
                }
            }
            // Step 3: targeted pkill for any per-secondary reverse
            // tunnel that escaped `preparation.cleanup()` tracking.
            // Pattern specifically matches `-R <port>:localhost...`
            // (preparation's shape); the master used
            // `-R 0.0.0.0:<port>:localhost...` so the regex
            // deliberately does NOT race the master shutdown above.
            if let Err(e) = pkill_residual_tunnels(py) {
                tracing::warn!(error = ?e, "residual-tunnel pkill failed");
            }
        });
    }
}

/// FFI for `getuid(2)`. Avoids pulling in a direct `libc` dep just
/// for one syscall — the `nix` crate already in the workspace
/// doesn't expose `getuid` in the slurm crate's feature set.
fn current_uid() -> u32 {
    unsafe extern "C" {
        fn getuid() -> u32;
    }
    unsafe { getuid() }
}

/// Sync bridge for an async pkill. Builds a single-shot
/// current-thread tokio runtime, releases the GIL, runs `op`, and
/// reattaches. Single source of truth for the runtime-construction
/// boilerplate shared by the two pkill phases.
fn block_on_detached<F, R>(py: Python<'_>, op: F) -> PyResult<R>
where
    F: FnOnce(u32) -> std::pin::Pin<Box<dyn std::future::Future<Output = PyResult<R>> + Send>>
        + Send,
    R: Send,
{
    py.detach(|| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("tokio runtime: {e}"))
            })?;
        rt.block_on(op(current_uid()))
    })
}

/// `pkill -u <uid> -f 'ssh.*-R [0-9]+:localhost'`.
///
/// Routed through `dynrunner_slurm::pipeline::pkill_residual_reverse_tunnels`
/// so a future pure-Rust preparation port (L2.F) calling the same
/// function gets the c399f5a-tightened regex by construction.
fn pkill_residual_tunnels(py: Python<'_>) -> PyResult<()> {
    block_on_detached(py, |uid| {
        Box::pin(async move {
            dynrunner_slurm::pipeline::pkill_residual_reverse_tunnels(uid)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("pkill: {e}")))
        })
    })
}

/// `pkill -u <uid> -f 'ssh.*-R.*localhost'`. Broad-pattern
/// leftover-cleanup before any new ssh master is started — there
/// is nothing yet to protect at this point in the lifecycle, so
/// the pattern is intentionally broader than the post-run
/// teardown's tightened pattern.
fn pkill_leftover_tunnels(py: Python<'_>) -> PyResult<()> {
    block_on_detached(py, |uid| {
        Box::pin(async move {
            let _ = tokio::process::Command::new("pkill")
                .arg("-u")
                .arg(uid.to_string())
                .arg("-f")
                .arg(r"ssh.*-R.*localhost")
                .stderr(std::process::Stdio::null())
                .status()
                .await;
            Ok(())
        })
    })
}

/// Python entry point. Mirrors the signature of the Python
/// `run_slurm_pipeline(task, args, deployment, log)`. The
/// implementation walks the same step sequence as
/// `python/dynamic_runner/packaging/pipeline.py::run_slurm_pipeline`,
/// dispatching each step to the existing Python facade.
#[pyfunction]
#[pyo3(signature = (task, args, deployment, log))]
pub(crate) fn run_slurm_pipeline<'py>(
    py: Python<'py>,
    task: &Bound<'py, PyAny>,
    args: &Bound<'py, PyAny>,
    deployment: &Bound<'py, PyAny>,
    log: &Bound<'py, PyAny>,
) -> PyResult<()> {
    // ---- Pre-flight: validate the slurm-specific argparse args. ----
    let validate = py
        .import("dynamic_runner.packaging.pipeline")?
        .getattr("_validate_slurm_args")?;
    let ok: bool = validate.call1((args, log))?.extract()?;
    if !ok {
        return Ok(());
    }

    // ---- Selection args: resolves the consumer's --source root etc. ----
    let shared = py.import("dynamic_runner._shared")?;
    let sel_result = shared
        .getattr("process_selection_arguments")?
        .call1((args,))?;
    let source_dir = sel_result.getattr("source_dir")?;

    let num_secondaries: u32 = args.getattr("jobs")?.extract()?;
    let pkg_pipeline = py.import("dynamic_runner.packaging.pipeline")?;
    let run_id: String = pkg_pipeline.getattr("_make_run_id")?.call0()?.extract()?;
    log.call_method1("info", (format!("Run ID: {run_id}"),))?;

    // ---- Gateway construction. ----
    log.call_method1("info", ("Connecting to gateway...",))?;
    let pkg_gateway = py.import("dynamic_runner.packaging.gateway")?;
    let gateway_url = args.getattr("gateway")?;
    let gateway_config = pkg_gateway
        .getattr("parse_gateway_url")?
        .call1((gateway_url,))?;

    // CLI-supplied auth primitives: gateway-config concerns, set
    // post-parse so `parse_gateway_url`'s URL signature stays clean.
    gateway_config.setattr(
        "ssh_identity_file",
        args.getattr("ssh_identity_file").unwrap_or_else(|_| py.None().into_bound(py)),
    )?;
    gateway_config.setattr(
        "ssh_config_file",
        args.getattr("ssh_config").unwrap_or_else(|_| py.None().into_bound(py)),
    )?;

    let gateway = pkg_gateway
        .getattr("create_gateway")?
        .call1((gateway_config,))?;

    // ---- QUIC port pick + master-side port forwarding. ----
    //
    // `forwarded_ports` MUST be set up BEFORE `gateway.connect()`:
    // SSHGateway.connect() reads the list to build its
    // `-R 0.0.0.0:remote:localhost:local` flags, and the
    // gateway-ports preflight short-circuits when the list is empty.
    let runner_module = py.import("dynamic_runner")?;
    let primary_quic_port: u16 = runner_module
        .getattr("pick_free_port")?
        .call0()?
        .extract()?;
    gateway.call_method1("setup_port_forwarding", (primary_quic_port, primary_quic_port))?;

    // Consumer-supplied extra `-R local:gateway` forwards.
    let extra_forwards = deployment.getattr("extra_port_forwards")?;
    let iter = extra_forwards.try_iter()?;
    for pair in iter {
        let pair = pair?;
        let (local_port, gw_port): (u16, u16) = pair.extract()?;
        gateway.call_method1("setup_port_forwarding", (local_port, gw_port))?;
    }

    gateway.call_method0("connect")?;

    // ---- Slurm config + root-folder validation/creation. ----
    let slurm_config = pkg_pipeline
        .getattr("_make_slurm_config")?
        .call1((args, &gateway))?;
    let slurm_config_module = py.import("dynamic_runner.packaging.slurm_config")?;
    let validate_fn = slurm_config_module.getattr("validate_slurm_config")?;
    // Mirror the Python `try: ... except ValueError: ...` semantic:
    // catch ONLY ValueError; let other exception classes (e.g.
    // KeyboardInterrupt, BaseException-derived) propagate.
    match validate_fn.call1((&slurm_config, &gateway)) {
        Ok(_) => {}
        Err(e) if e.is_instance_of::<pyo3::exceptions::PyValueError>(py) => {
            let root = slurm_config.getattr("root_folder")?;
            log.call_method1(
                "info",
                (format!("Creating SLURM root directory: {root}"),),
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

    // Discover items ONCE; reused for both pre-staging upload and
    // the coordinator-side StageFile queue.
    let binaries = PyList::empty(py);
    for item in task
        .call_method1("discover_items", (&source_dir, args))?
        .try_iter()?
    {
        binaries.append(item?)?;
    }
    if binaries.is_empty() {
        log.call_method1(
            "warning",
            ("No items discovered. Pipeline will run in test/job-submission mode.",),
        )?;
    }

    // ---- Reverse-connection mode detection. ----
    //
    // Equivalent to:
    //     hasattr(gateway, "gateway_ports_enabled")
    //         and gateway.gateway_ports_enabled is False
    let use_reverse_connection: bool = match gateway.getattr("gateway_ports_enabled") {
        Ok(v) if !v.is_none() => {
            // `is False` (not just falsy): only the explicit boolean
            // False signals reverse-mode. None / unset => forward.
            v.is_instance_of::<pyo3::types::PyBool>() && !v.is_truthy()?
        }
        _ => false,
    };
    if use_reverse_connection {
        log.call_method1(
            "info",
            ("Gateway disallows public port forwarding; switching to SSH ProxyJump tunnel mode.",),
        )?;
    }

    // ---- Clear leftover ssh tunnels from previous runs. ----
    pkill_leftover_tunnels(py)?;

    // ---- Construct the packaging + job_manager + preparation triple. ----
    let podman_module = py.import("dynamic_runner.packaging.podman")?;
    let podman_packaging_cls = podman_module.getattr("PodmanPackaging")?;
    let pkg_kwargs = PyDict::new(py);
    pkg_kwargs.set_item("deployment", deployment)?;
    let packaging = podman_packaging_cls.call((), Some(&pkg_kwargs))?;

    let job_manager_module = py.import("dynamic_runner.packaging.job_manager")?;
    let job_manager_cls = job_manager_module.getattr("SlurmJobManager")?;
    let job_manager = job_manager_cls.call1((&gateway, &slurm_config, &packaging, deployment))?;

    let cert_dir_str = format!("/tmp/db-runner-cert-{run_id}");
    let pathlib = py.import("pathlib")?;
    let cert_dir = pathlib.getattr("Path")?.call1((cert_dir_str,))?;
    let mkdir_kwargs = PyDict::new(py);
    mkdir_kwargs.set_item("parents", true)?;
    mkdir_kwargs.set_item("exist_ok", true)?;
    cert_dir.call_method("mkdir", (), Some(&mkdir_kwargs))?;

    // `args.cores` is the verbatim `--cores` spec string the user
    // passed (or its argparse default `"0"`). Forward it to
    // SlurmPreparation so each SLURM wrapper appends `--cores <spec>`
    // to the secondary's container_command — symmetric with the
    // `--multi-computer local` fix at spawn_secondary.py (commit
    // 38a0c30 / task #26). Without this, the secondary subprocess
    // inside the SLURM container's cgroup-CPU-quota auto-detects
    // host CPU count from `available_parallelism` (returns 32 on
    // a 32-core host even when the container is quota'd to 2 CPUs)
    // and oversaturates the per-job cgroup with worker spawns.
    let cores_spec: String = args
        .getattr("cores")
        .ok()
        .and_then(|v| v.extract::<String>().ok())
        .unwrap_or_else(|| "0".into());

    let preparation_module = py.import("dynamic_runner.packaging.preparation")?;
    let preparation_cls = preparation_module.getattr("SlurmPreparation")?;
    let prep_kwargs = PyDict::new(py);
    prep_kwargs.set_item("slurm_config", &slurm_config)?;
    prep_kwargs.set_item("job_manager", &job_manager)?;
    prep_kwargs.set_item("gateway", &gateway)?;
    prep_kwargs.set_item("deployment", deployment)?;
    prep_kwargs.set_item("use_reverse_connection", use_reverse_connection)?;
    prep_kwargs.set_item("run_id", &run_id)?;
    prep_kwargs.set_item("cores_spec", cores_spec)?;
    let preparation = preparation_cls.call((), Some(&prep_kwargs))?;

    // ---- try/finally guard. Owns gateway + (post-prep) preparation. ----
    let mut guard = CleanupGuard::new(gateway.clone().unbind());

    // Inner block whose error short-circuits to the guard's Drop.
    let pipeline_result: PyResult<()> = (|| {
        // Bridge `preparation.prepare(...)` (an async coroutine) back
        // through asyncio.run so we keep the same execution model the
        // Python pipeline used. Switching to a tokio runtime here
        // would require porting `_setup_ssh_tunnels` to async-Rust,
        // which is the preparation-port unit's concern — not this one.
        let asyncio = py.import("asyncio")?;
        let prep_kwargs = PyDict::new(py);
        prep_kwargs.set_item("num_secondaries", num_secondaries)?;
        prep_kwargs.set_item("quic_port", primary_quic_port)?;
        prep_kwargs.set_item("primary_quic_port", primary_quic_port)?;
        prep_kwargs.set_item("cert_dir", &cert_dir)?;
        let skip_image_build: bool = args
            .getattr("skip_image_build")
            .ok()
            .and_then(|v| v.extract().ok())
            .unwrap_or(false);
        prep_kwargs.set_item("skip_image_build", skip_image_build)?;
        let prep_coro = preparation.call_method("prepare", (), Some(&prep_kwargs))?;
        let prep_result = asyncio.getattr("run")?.call1((prep_coro,))?;

        // Mark the guard's `preparation` only AFTER `prepare()`
        // returns: if `prepare()` itself raised, there's no tracked
        // tunnel state to clean up (any partial spawn dies with the
        // exception's traceback before our control returns here).
        guard.set_preparation(preparation.clone().unbind());

        let prep_run_id: String = prep_result.getattr("run_id")?.extract()?;
        log.call_method1(
            "info",
            (format!("SLURM jobs submitted; run_id={prep_run_id}"),),
        )?;

        // ---- Source-binary upload. ----
        //
        // Gating mirrors pipeline.py exactly: file-based items, NOT
        // pre-staged. Runs after sbatch so secondaries are already
        // starting; the primary's InitialAssignment isn't sent until
        // coord.run() reaches its peer-mesh-ready gate, so a slow
        // upload simply delays dispatch rather than racing.
        let uses_file_based_items: bool = task
            .getattr("uses_file_based_items")
            .ok()
            .and_then(|v| v.extract().ok())
            .unwrap_or(true);
        if !binaries.is_empty()
            && uses_file_based_items
            && !attr_truthy(args, "source_already_staged")
        {
            job_manager.call_method1(
                "upload_source_binaries",
                (&binaries, &source_dir),
            )?;
        }

        // ---- Hand-off to the Rust primary coordinator. ----
        drive_rust_primary(
            py,
            task,
            args,
            &prep_result,
            primary_quic_port,
            &binaries,
            &slurm_config,
            log,
        )?;

        Ok(())
    })();

    drop(guard);
    pipeline_result
}

/// Hand the run over to `RustPrimaryCoordinator`. Ports the
/// `_drive_rust_primary` helper from pipeline.py.
///
/// `binaries` is the already-discovered list — passed through rather
/// than re-discovered so both halves see the exact same set.
#[allow(clippy::too_many_arguments)]
fn drive_rust_primary<'py>(
    py: Python<'py>,
    task: &Bound<'py, PyAny>,
    args: &Bound<'py, PyAny>,
    prep_result: &Bound<'py, PyAny>,
    primary_quic_port: u16,
    binaries: &Bound<'py, PyList>,
    slurm_config: &Bound<'py, PyAny>,
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
    let num_secondaries = prep_result.getattr("num_secondaries")?;
    let coord = coord_cls.call(
        PyTuple::new(py, [&num_secondaries, task, &no_spawn_callback])?,
        Some(&coord_kwargs),
    )?;

    let coord_uses_file_based: bool = coord.getattr("uses_file_based_items")?.extract()?;

    if !coord_uses_file_based {
        // Non-file-based items: framework does no primary-side
        // staging at all; secondary passes `local_path` through to
        // the worker as an opaque identifier.
        log.call_method1(
            "info",
            (
                "TaskDefinition.uses_file_based_items=False; \
                 skipping primary StageFile pass and starting coordinator",
            ),
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
        coord.call_method1(
            "queue_initial_staging",
            (binaries, source_dir.str()?),
        )?;
        log.call_method1(
            "info",
            (
                "Queued %d StageFile notifications across %d secondaries; starting coordinator",
                binaries.len(),
                num_secondaries,
            ),
        )?;
    }

    coord.call_method1("run", (binaries,))?;
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
