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
/// (`tunnel_manager.cleanup()` → `gateway.disconnect()` → tightened
/// `pkill`) on scope exit. Modeled on Python's `try/finally` block
/// in `pipeline.py::run_slurm_pipeline`. The order is invariant —
/// see the `pkill_residual_reverse_tunnels` doc in `dynrunner-slurm`
/// for why disconnect MUST precede pkill.
///
/// * Holds `Py<PyAny>` references to the live `tunnel_manager` (a
///   `RustSlurmPreparation` pyclass; only present in reverse-connection
///   mode) and `gateway` instances. `Option<...>` shape so an
///   early-failure path can construct the guard with what it has so
///   far and the `Drop` skips the missing steps.
/// * Each step is best-effort: a failure logs but does not abort the
///   remaining steps. Same semantics as Python's `try/finally` chain
///   where the gateway disconnect runs even if preparation cleanup
///   raised.
struct CleanupGuard {
    tunnel_manager: Option<Py<PyAny>>,
    gateway: Option<Py<PyAny>>,
}

impl CleanupGuard {
    fn new(gateway: Py<PyAny>) -> Self {
        Self {
            tunnel_manager: None,
            gateway: Some(gateway),
        }
    }

    fn set_tunnel_manager(&mut self, tunnel_manager: Py<PyAny>) {
        self.tunnel_manager = Some(tunnel_manager);
    }
}

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        Python::attach(|py| {
            // Step 1: per-secondary tunnel cleanup (tracked in the
            // RustSlurmPreparation tunnel manager). Only present if
            // reverse-connection mode constructed one — non-reverse
            // runs skip this step.
            if let Some(prep) = self.tunnel_manager.take() {
                if let Err(e) = prep.bind(py).call_method0("cleanup") {
                    tracing::warn!(error = ?e, "tunnel_manager.cleanup() failed");
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

    // ---- Connection topology decision. ----
    //
    // SLURM dispatch unconditionally uses ProxyJump-into-secondaries
    // (`use_reverse_connection = true`): primary dials each secondary
    // via `ssh -J gateway secondary -R tunnel_port:localhost:primary_quic`,
    // using only outbound SSH from the submitter. This works on every
    // cluster because no `-R` reverse-forward on the gateway is required.
    //
    // The gateway-direct alternative (secondaries dial `gateway:port`
    // through an `-R` reverse-forward hosted by the gateway) would
    // save one SSH tunnel per secondary, but is not safe to enable
    // automatically. Two distinct things must hold for it to work:
    //   1. The gateway's sshd must honour `-R *:port` against its
    //      external interface (not silently downgrade to loopback).
    //   2. The gateway's external IP/interface must be TCP-reachable
    //      from compute nodes (not on a separate network segment).
    // The `check_gateway_ports` probe in `dynrunner-gateway::ssh`
    // confirms (1) by reading `ss` output on the gateway after a
    // forward exists, but cannot prove (2) — and "binds publicly
    // on the gateway" is necessary-but-not-sufficient for cluster
    // reachability. LMU Krater is the load-bearing counter-example:
    // brasilianit.cip.ifi.lmu.de happily binds 0.0.0.0:port, but
    // kraterNN compute nodes cannot route to brasilianit's external
    // IP (segmented network). The framework cannot validate (2)
    // without actually issuing a sacrificial sbatch against the
    // target partition, which is too heavy to gate every dispatch on.
    //
    // So: ProxyJump is the only auto-default that works everywhere.
    // Anyone who has hand-verified that their cluster supports
    // gateway-direct outbound can introduce an explicit opt-in
    // (CLI flag / SlurmConfig field) later — but the framework
    // must not infer that opt-in from any subset of the available
    // probes. The decision is computed here (before the port-forward
    // registration) because gateway-side `-R` registration is itself
    // topology-dependent (see below).
    let use_reverse_connection: bool = true;
    log.call_method1(
        "info",
        ("SLURM connection topology: SSH ProxyJump (primary tunnels to each secondary via gateway)",),
    )?;

    // ---- QUIC port pick + topology-conditional master-side port forwarding. ----
    //
    // The gateway ControlMaster's `-R 0.0.0.0:remote:localhost:local`
    // flags exist for the gateway-direct topology: they let compute
    // nodes dial the gateway on a public-bound port and reach
    // services on the submitter. In ProxyJump topology those
    // forwards are redundant — every per-secondary
    // `ssh -J gateway secondary -R <tunnel>:localhost:<primary>` from
    // `dynrunner-slurm::preparation::build_ssh_argv` carries the QUIC
    // forward AND the `extra_port_forwards` fan-out on the SAME ssh
    // session, terminating on each secondary's loopback. From the
    // secondary's POV, `localhost:<gateway_port>` reaches the
    // submitter's `localhost:<local_port>` via that per-secondary
    // tunnel — no gateway-side bind needed.
    //
    // Why this matters beyond redundancy: at GatewayPorts=no sshds
    // (LMU's `remote.cip.ifi.lmu.de` being the canonical example),
    // the gateway-side `-R 0.0.0.0:port:localhost:port` silently
    // downgrades to a 127.0.0.1 bind, and `check_gateway_ports`
    // logs a "GatewayPorts likely disabled" warning. In ProxyJump
    // topology that warning is a false alarm — the working path is
    // the per-secondary one — but the warning has historically
    // misled consumers into building substituter / peer URLs
    // against `<gateway_host>:<port>`, which is unreachable from
    // compute nodes. Skipping the gateway-side `-R` in ProxyJump
    // topology removes the false alarm and the misleading
    // implication that `<gateway>:<port>` is a valid endpoint.
    //
    // `forwarded_ports` MUST be set up BEFORE `gateway.connect()`
    // because `SSHGateway.connect()` reads the list once to build
    // its master-spawn argv; registering after connect would be a
    // no-op for the master argv.
    let runner_module = py.import("dynamic_runner")?;
    let primary_quic_port: u16 = runner_module
        .getattr("pick_free_port")?
        .call0()?
        .extract()?;
    if !use_reverse_connection {
        // Gateway-direct topology: secondaries dial `gateway:primary_quic_port`,
        // so the gateway ControlMaster must hold the `-R` for it.
        gateway.call_method1("setup_port_forwarding", (primary_quic_port, primary_quic_port))?;

        // Consumer-supplied extra `-R local:gateway` forwards on the
        // ControlMaster, same gateway-direct topology rationale.
        let extra_forwards = deployment.getattr("extra_port_forwards")?;
        let iter = extra_forwards.try_iter()?;
        for pair in iter {
            let pair = pair?;
            let (local_port, gw_port): (u16, u16) = pair.extract()?;
            gateway.call_method1("setup_port_forwarding", (local_port, gw_port))?;
        }
    }
    // In ProxyJump topology, `extra_port_forwards` is plumbed via
    // `deployment` into `SlurmPreparation` (Python) → `RustSlurmPreparation`
    // (PyO3) → `dynrunner-slurm::preparation::build_ssh_argv` (Rust),
    // which adds them as `-R gateway_port:localhost:local_port` on
    // each per-secondary ssh-with-ProxyJump. That's the only forward
    // the secondary needs.

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
    //
    // Pre-staged-source mode (`--source-already-staged <path>`): the
    // submitter has no local view of the staged corpus — those files
    // live on the cluster filesystem the secondaries see, not on the
    // dispatcher. Skip the discovery walk here and hand the
    // coordinator an empty list; the Step 6 PyO3 wrapper reads
    // `binaries.is_empty() && source_pre_staged_root.is_some()` to
    // derive `required_setup_on_promote = true`, which in turn makes
    // the bootstrap `PromotePrimary` carry `required_setup=true` so
    // the chosen secondary runs `task.discover_items` against its
    // bind-mounted `src_network` and seeds the cluster ledger.
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
                ("No items discovered. Pipeline will run in test/job-submission mode.",),
            )?;
        }
    } else {
        log.call_method1(
            "info",
            ("Pre-staged source mode: deferring task discovery to the setup-promoted secondary.",),
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
    // Symmetric with cores: `args.max_memory` is the verbatim
    // `--max-memory` spec string (`"16G"`, `"-2G"`, `"+1G"`, …)
    // passed by the user or its argparse default `"-2G"`. The
    // SlurmPreparation Python class accepts `max_memory_spec` and
    // forwards it to job_manager.generate_wrapper_script, which
    // emits `--max-memory={spec}` in the secondary's container_command.
    //
    // BUG-FIX (asm-dataset-nix repro at 3aa9920): the #30 commit
    // (57d7ee8) added `cores_spec` extraction here but FORGOT the
    // symmetric `max_memory_spec` line + set_item. The secondary's
    // wrapper then rendered `--max-memory=-2G` (the default) even
    // when the user passed `--max-memory 2G` on the dispatcher. The
    // `worker_id=0 budget_mb=4096` over-allocation that #30 was
    // meant to close stayed open because the plumbing dead-ended
    // one hop early. This commit closes the gap.
    let max_memory_spec: String = args
        .getattr("max_memory")
        .ok()
        .and_then(|v| v.extract::<String>().ok())
        .unwrap_or_else(|| "-2G".into());

    // `args.forwarded_argv` is the dispatcher's `sys.argv[1:]` minus
    // the framework-regenerated flags (`--secondary`, `--cores`, …)
    // that the wrapper emits afresh per job. Threaded opaquely through
    // SlurmPreparation → SlurmJobManager → the Rust wrapper-script
    // generator, which bash-quotes each entry into the secondary's
    // container-command argv. Empty default keeps the field optional
    // for legacy callers constructing SlurmPreparation directly
    // (programmatic test fixtures); `run.py` always populates it.
    let mut forwarded_argv: Vec<String> = args
        .getattr("forwarded_argv")
        .ok()
        .and_then(|v| v.extract::<Vec<String>>().ok())
        .unwrap_or_default();

    // Scale-aware setup-deadline override. Drives the secondary's
    // `SecondaryConfig.setup_deadline` (via the secondary's argparse
    // → `_dispatch_secondary` → `DistributedConfig`). When the
    // operator passed `--slurm-setup-deadline-secs N` the value is
    // already present in `forwarded_argv` (preserved verbatim by
    // `filter_framework_argv`) AND on `args.slurm_setup_deadline_secs`
    // — we feed the parsed-int value through `compute_setup_deadline_secs`
    // as the explicit-override branch so the formula stays the single
    // source of truth. When the operator left it unset, the formula's
    // `max(60, num_secondaries * 15)` branch fires and we INJECT the
    // computed value into `forwarded_argv` so every secondary's argparse
    // re-derives the same effective deadline as the dispatcher — the
    // alternative (re-running the formula on the secondary side) would
    // duplicate the heuristic across the language boundary and risk
    // drift. See `compute_setup_deadline_secs` for the formula's
    // rationale and the `--slurm-setup-deadline-secs` help text for
    // the operator-facing knob.
    let explicit_deadline_secs: Option<u64> = args
        .getattr("slurm_setup_deadline_secs")
        .ok()
        .and_then(|v| if v.is_none() { None } else { v.extract::<u64>().ok() });
    let effective_deadline_secs =
        dynrunner_slurm::pipeline::compute_setup_deadline_secs(
            explicit_deadline_secs,
            num_secondaries,
        );
    if explicit_deadline_secs.is_none() {
        // Inject the computed default so the secondary's argparse
        // sees it. Operator-supplied values are already in
        // `forwarded_argv` verbatim, so we only push when WE derived
        // the value — avoids a duplicated flag the secondary's
        // argparse would resolve to whichever appears last (correct
        // either way, but the diagnostic-noise penalty is real).
        forwarded_argv.push(format!(
            "--slurm-setup-deadline-secs={}",
            effective_deadline_secs
        ));
        log.call_method1(
            "info",
            (format!(
                "SLURM setup-deadline: derived {}s for {} secondaries (formula \
                 max(60, jobs*15); override with --slurm-setup-deadline-secs)",
                effective_deadline_secs, num_secondaries
            ),),
        )?;
    } else {
        log.call_method1(
            "info",
            (format!(
                "SLURM setup-deadline: operator override {}s (--slurm-setup-deadline-secs)",
                effective_deadline_secs
            ),),
        )?;
    }

    let skip_image_build: bool = args
        .getattr("skip_image_build")
        .ok()
        .and_then(|v| v.extract().ok())
        .unwrap_or(false);

    // ---- try/finally guard. Owns gateway + (post-prep) tunnel_manager. ----
    let mut guard = CleanupGuard::new(gateway.clone().unbind());

    // Inner block whose error short-circuits to the guard's Drop.
    let pipeline_result: PyResult<()> = (|| {
        // ---- Preparation phase (ported from
        //      `python/dynamic_runner/packaging/preparation.py::SlurmPreparation.prepare`). ----
        //
        // Drives the SLURM preparation sequence directly from the Rust
        // orchestrator: directory prep, image build+transfer, sbatch
        // submit-loop, optional reverse-tunnel watcher. Python
        // `SlurmPreparation` retains a thin back-compat stub for any
        // out-of-tree caller; the framework itself no longer touches
        // it.
        log.call_method1("info", ("Phase 1: SLURM Preparation",))?;
        let (outcome, tunnel_manager) = run_preparation(
            py,
            &gateway,
            &job_manager,
            &slurm_config,
            deployment,
            &run_id,
            &cert_dir,
            &cores_spec,
            &max_memory_spec,
            &forwarded_argv,
            num_secondaries,
            primary_quic_port,
            use_reverse_connection,
            skip_image_build,
            log,
        )?;
        // Install the tunnel manager (if reverse mode produced one) on
        // the guard AFTER `run_preparation` returns so any later
        // failure in this pipeline still tears the established tunnels
        // down before the gateway disconnect. The pre-return install
        // path is exercised by the Python pyfunction wrapper for
        // callers that drive the prep outside the pipeline (the Python
        // `SlurmPreparation` shim owns its own cleanup hand-off).
        if let Some(mgr) = tunnel_manager {
            guard.set_tunnel_manager(mgr);
        }

        log.call_method1(
            "info",
            (format!("SLURM jobs submitted; run_id={}", outcome.run_id),),
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
            &outcome,
            primary_quic_port,
            &binaries,
            &slurm_config,
            &job_manager,
            log,
        )?;

        Ok(())
    })();

    drop(guard);
    pipeline_result
}

/// Mirrors the shape of the legacy Python
/// ``packaging.preparation.PreparationResult`` so downstream consumers
/// of the SLURM-pipeline (today: ``drive_rust_primary``) keep reading
/// the same fields off the same struct.
///
/// `mode_specific_data` is intentionally untyped — the legacy Python
/// shape was ``dict[str, Any]`` and consumers picked individual keys
/// out of it. We carry the same dict verbatim so behavioural parity
/// is one ``.get_item`` away.
///
/// `cert_dir`, `primary_entropy`, and `mode_specific_data` are
/// currently unread by the in-tree consumer (`drive_rust_primary`
/// only needs `num_secondaries` and `run_id`); they remain on the
/// struct so the post-prepare object shape stays a one-to-one mirror
/// of the legacy `PreparationResult` dataclass. Removing them would
/// be a separate, narrowly-scoped cleanup once we have evidence no
/// out-of-tree caller depends on the shape.
#[allow(dead_code)]
struct PreparationOutcome {
    num_secondaries: u32,
    run_id: String,
    cert_dir: Py<PyAny>,
    primary_entropy: Py<PyAny>,
    mode_specific_data: Py<PyDict>,
}

/// Drive the SLURM preparation steps in order, directly from the
/// PyO3 orchestrator. Replaces the legacy Python
/// `SlurmPreparation.prepare(...)` body — every step here was inlined
/// from that method without changing the sequence or the semantics.
///
/// On entry:
/// * `gateway`, `job_manager`, `slurm_config`, `deployment` are the
///   Python-side facade objects the orchestrator already constructed.
///
/// Returns a `PreparationOutcome` carrying the five fields the legacy
/// `PreparationResult` dataclass exposed plus an
/// `Option<RustSlurmPreparation>` handle: `Some(...)` whenever the
/// reverse-connection branch fired and spawned per-secondary tunnels,
/// `None` otherwise. The caller decides where to register the
/// tunnel-manager for `cleanup()` — the in-pipeline orchestrator
/// installs it on its `CleanupGuard`; the Python shim stores it on
/// the `SlurmPreparation` instance.
///
/// `drive_rust_primary` reads the legacy fields off the
/// `PreparationOutcome` by name.
#[allow(clippy::too_many_arguments)]
fn run_preparation<'py>(
    py: Python<'py>,
    gateway: &Bound<'py, PyAny>,
    job_manager: &Bound<'py, PyAny>,
    slurm_config: &Bound<'py, PyAny>,
    deployment: &Bound<'py, PyAny>,
    run_id: &str,
    cert_dir: &Bound<'py, PyAny>,
    cores_spec: &str,
    max_memory_spec: &str,
    forwarded_argv: &[String],
    num_secondaries: u32,
    primary_quic_port: u16,
    use_reverse_connection: bool,
    skip_image_build: bool,
    log: &Bound<'py, PyAny>,
) -> PyResult<(PreparationOutcome, Option<Py<PyAny>>)> {
    let base_log_dir: String = slurm_config
        .call_method0("get_log_dir")?
        .str()?
        .extract()?;
    let run_log_dir = format!("{base_log_dir}/{run_id}");

    // Directory prep (gateway-side mkdir for image / srcbins / output
    // / log roots) — delegated to the Rust job_manager.
    job_manager.call_method0("prepare_directories")?;
    gateway.call_method1("create_directory", (&run_log_dir,))?;

    // Image build + transfer, or skip-build path. Mirrors the legacy
    // `_prepare_docker_images` helper: both paths produce a
    // `PodmanImageMetadata` instance with `remote_path`, `image_hash`,
    // `uploaded` fields that the wrapper-script generator reads.
    let image_metadata = if skip_image_build {
        log.call_method1(
            "info",
            ("Skipping image build and transfer (--skip-image-build)",),
        )?;
        let image_dir = job_manager
            .call_method1(
                "_expand_path",
                (slurm_config.call_method0("get_image_dir")?,),
            )?;
        let pathlib = py.import("pathlib")?;
        let image_dir_path = pathlib.getattr("Path")?.call1((image_dir,))?;
        let image_tar_basename = deployment.getattr("image_tar_basename")?;
        let image_path = image_dir_path.call_method1("__truediv__", (image_tar_basename,))?;
        log.call_method1(
            "info",
            (format!("Assuming image exists at: {image_path}"),),
        )?;
        let podman_module = py.import("dynamic_runner.packaging.podman")?;
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
        let metadata = job_manager.call_method1("build_and_transfer_images", (project_root,))?;
        let uploaded: bool = metadata.getattr("uploaded")?.extract().unwrap_or(false);
        let remote_path = metadata.getattr("remote_path")?;
        let image_hash: String = metadata
            .getattr("image_hash")
            .and_then(|v| v.extract())
            .unwrap_or_default();
        log.call_method1(
            "info",
            (format!(
                "Image {} at: {}",
                if uploaded { "uploaded" } else { "reused" },
                remote_path
            ),),
        )?;
        log.call_method1("info", (format!("Image hash: {image_hash}"),))?;
        metadata
    };

    // Sbatch submit-loop. The gateway host the secondaries dial back
    // through is the user-given `gateway.host` verbatim (no
    // `hostname -f` substitution — load-balancer aliases must stay
    // intact, see legacy `_determine_gateway_host` for the full
    // rationale).
    let gateway_host: String = match gateway.getattr("host") {
        Ok(v) if !v.is_none() => {
            let h: String = v.extract()?;
            if h.is_empty() {
                "localhost".to_string()
            } else {
                h
            }
        }
        _ => "localhost".to_string(),
    };
    log.call_method1(
        "info",
        (format!("Using gateway hostname (as configured by user): {gateway_host}"),),
    )?;

    log.call_method1("info", ("Submitting SLURM jobs...",))?;
    let job_name_prefix: String = deployment
        .getattr("effective_job_name_prefix")?
        .extract()?;
    for i in 0..num_secondaries {
        let secondary_id = format!("secondary-{i}");
        let job_name = format!("{job_name_prefix}-{secondary_id}");

        let wrapper_kwargs = PyDict::new(py);
        wrapper_kwargs.set_item("image_metadata", &image_metadata)?;
        wrapper_kwargs.set_item("secondary_id", &secondary_id)?;
        wrapper_kwargs.set_item("gateway_host", &gateway_host)?;
        wrapper_kwargs.set_item("gateway_port", primary_quic_port)?;
        wrapper_kwargs.set_item("cores_spec", cores_spec)?;
        wrapper_kwargs.set_item("max_memory_spec", max_memory_spec)?;
        wrapper_kwargs.set_item("forwarded_argv", forwarded_argv.to_vec())?;
        wrapper_kwargs.set_item("reverse_connection", use_reverse_connection)?;
        wrapper_kwargs.set_item("run_log_dir", &run_log_dir)?;
        let wrapper =
            job_manager.call_method("generate_wrapper_script", (), Some(&wrapper_kwargs))?;

        let submit_kwargs = PyDict::new(py);
        submit_kwargs.set_item("run_log_dir", &run_log_dir)?;
        let job_id =
            job_manager.call_method("submit_job", (&wrapper, &job_name), Some(&submit_kwargs))?;
        log.call_method1(
            "info",
            (format!("Submitted job {job_id} for {secondary_id}"),),
        )?;
    }
    log.call_method1("info", (format!("All {num_secondaries} jobs submitted"),))?;

    // Optional reverse-tunnel setup. Owns the per-secondary
    // `ssh -N -R` lifecycle; the watcher state machine + subprocess
    // teardown live in the `RustSlurmPreparation` pyclass.
    let secondary_port_map = PyDict::new(py);
    let ssh_tunnels = pyo3::types::PyList::empty(py);
    let mut tunnel_manager_handle: Option<Py<PyAny>> = None;
    if use_reverse_connection {
        log.call_method1(
            "info",
            ("Setting up SSH reverse tunnels for reverse connections...",),
        )?;

        let connection_info_dir = format!("{run_log_dir}/connection_info");
        gateway.call_method1("create_directory", (&connection_info_dir,))?;

        let gateway_user: Option<String> = match gateway.getattr("user") {
            Ok(v) if !v.is_none() => v.extract().ok(),
            _ => None,
        };
        let gateway_port: u16 = match gateway.getattr("port") {
            Ok(v) if !v.is_none() => v.extract().unwrap_or(22),
            _ => 22,
        };
        let auth_options: Vec<String> = match gateway.call_method0("auth_options") {
            Ok(v) => v.extract().unwrap_or_default(),
            Err(_) => Vec::new(),
        };
        let extra_forwards: Vec<(u16, u16)> = deployment
            .getattr("extra_port_forwards")?
            .extract()
            .unwrap_or_default();

        let native = py.import("dynamic_runner._native")?;
        let tunnel_cls = native.getattr("RustSlurmPreparation")?;
        let tunnel_manager = tunnel_cls.call1((
            gateway,
            &run_log_dir,
            &gateway_host,
            gateway_port,
            auth_options,
            extra_forwards,
            gateway_user,
        ))?;

        // Record the tunnel-manager handle BEFORE invoking setup so
        // a caller that wires the returned handle to a cleanup path
        // can tear down any partially-spawned tunnels even if
        // `setup_ssh_tunnels` raises mid-flight.
        tunnel_manager_handle = Some(tunnel_manager.clone().unbind());

        let port_map = tunnel_manager
            .call_method1("setup_ssh_tunnels", (num_secondaries, primary_quic_port))?;
        // Reflect the port map into the outcome shape; values are
        // ints in the original Python dataclass.
        for (k, v) in port_map.cast::<PyDict>()?.iter() {
            let port: u16 = v.extract()?;
            secondary_port_map.set_item(k, port)?;
        }
        log.call_method1(
            "info",
            (format!("All {num_secondaries} SSH tunnels established"),),
        )?;
    }

    // Random primary entropy. The `secrets` module is the legacy
    // source of truth (cryptographic strength matters for the
    // primary-secondary handshake); we keep it on the Python side
    // rather than pulling in a fresh Rust CSPRNG dependency for the
    // ~32 bytes we need here.
    let secrets_mod = py.import("secrets")?;
    let primary_entropy = secrets_mod.getattr("token_bytes")?.call1((32u32,))?;

    let mode_specific_data = PyDict::new(py);
    mode_specific_data.set_item("image_metadata", &image_metadata)?;
    mode_specific_data.set_item("run_log_dir", &run_log_dir)?;
    mode_specific_data.set_item("secondary_port_map", &secondary_port_map)?;
    mode_specific_data.set_item("ssh_tunnels", &ssh_tunnels)?;

    Ok((
        PreparationOutcome {
            num_secondaries,
            run_id: run_id.to_owned(),
            cert_dir: cert_dir.clone().unbind(),
            primary_entropy: primary_entropy.unbind(),
            mode_specific_data: mode_specific_data.unbind(),
        },
        tunnel_manager_handle,
    ))
}

/// Python entry point for the SLURM preparation phase.
///
/// Single concern: drive the prep sequence end-to-end and hand back
/// the legacy `PreparationResult` shape plus the tunnel-manager
/// handle (if reverse-connection mode constructed one). The Python
/// `SlurmPreparation.prepare(...)` shim is a 1-line delegate over
/// this function so out-of-tree callers that import the class
/// keep seeing the same dataclass return shape.
///
/// Arguments mirror the legacy `SlurmPreparation.__init__` + `.prepare`
/// kwarg surface flattened to a single call site — the Python shim
/// re-assembles them.
///
/// Returns a `(PreparationResult, tunnel_manager)` tuple. The
/// `tunnel_manager` slot is `None` in non-reverse mode; otherwise it
/// is the `RustSlurmPreparation` pyclass instance the caller must
/// `.cleanup()` on teardown.
#[pyfunction(name = "run_preparation")]
#[pyo3(signature = (
    slurm_config,
    job_manager,
    gateway,
    deployment,
    run_id,
    num_secondaries,
    primary_quic_port,
    cert_dir,
    use_reverse_connection,
    skip_image_build,
    cores_spec,
    max_memory_spec,
    forwarded_argv,
    log,
))]
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_preparation_py<'py>(
    py: Python<'py>,
    slurm_config: &Bound<'py, PyAny>,
    job_manager: &Bound<'py, PyAny>,
    gateway: &Bound<'py, PyAny>,
    deployment: &Bound<'py, PyAny>,
    run_id: String,
    num_secondaries: u32,
    primary_quic_port: u16,
    cert_dir: &Bound<'py, PyAny>,
    use_reverse_connection: bool,
    skip_image_build: bool,
    cores_spec: String,
    max_memory_spec: String,
    forwarded_argv: Vec<String>,
    log: &Bound<'py, PyAny>,
) -> PyResult<Py<PyTuple>> {
    let (outcome, tunnel_manager) = run_preparation(
        py,
        gateway,
        job_manager,
        slurm_config,
        deployment,
        &run_id,
        cert_dir,
        &cores_spec,
        &max_memory_spec,
        &forwarded_argv,
        num_secondaries,
        primary_quic_port,
        use_reverse_connection,
        skip_image_build,
        log,
    )?;

    // Construct the legacy `PreparationResult` dataclass — single
    // source of truth for the field shape lives in
    // `python/dynamic_runner/packaging/preparation.py`, so we import
    // it here rather than maintaining a parallel pyclass.
    let prep_module = py.import("dynamic_runner.packaging.preparation")?;
    let result_cls = prep_module.getattr("PreparationResult")?;
    let result_kwargs = PyDict::new(py);
    result_kwargs.set_item("num_secondaries", outcome.num_secondaries)?;
    result_kwargs.set_item("run_id", outcome.run_id)?;
    result_kwargs.set_item("cert_dir", outcome.cert_dir.bind(py))?;
    result_kwargs.set_item("primary_entropy", outcome.primary_entropy.bind(py))?;
    result_kwargs.set_item("mode_specific_data", outcome.mode_specific_data.bind(py))?;
    let prep_result = result_cls.call((), Some(&result_kwargs))?;

    let tunnel = match tunnel_manager {
        Some(m) => m.bind(py).clone().into_any(),
        None => py.None().into_bound(py),
    };
    let tuple = PyTuple::new(py, [prep_result, tunnel])?;
    Ok(tuple.unbind())
}

/// Hand the run over to `RustPrimaryCoordinator`. Ports the
/// `_drive_rust_primary` helper from pipeline.py.
///
/// `binaries` is the already-discovered list — passed through rather
/// than re-discovered so both halves see the exact same set.
/// `outcome.num_secondaries` was previously read off a Python
/// `PreparationResult.num_secondaries` attribute; the field is the
/// same data, just carried in a Rust struct now.
#[allow(clippy::too_many_arguments)]
fn drive_rust_primary<'py>(
    py: Python<'py>,
    task: &Bound<'py, PyAny>,
    args: &Bound<'py, PyAny>,
    outcome: &PreparationOutcome,
    primary_quic_port: u16,
    binaries: &Bound<'py, PyList>,
    slurm_config: &Bound<'py, PyAny>,
    job_manager: &Bound<'py, PyAny>,
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
    let num_secondaries = outcome.num_secondaries;
    let args_tuple = PyTuple::new(py, [
        num_secondaries.into_pyobject(py)?.into_any().unbind(),
        task.clone().unbind(),
        no_spawn_callback.unbind(),
    ])?;
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
    if let Ok(rust_handle) = job_manager.getattr("_rust") {
        if let Ok(rust_jm) = rust_handle.cast::<crate::slurm::PyRustSlurmJobManager>() {
            let arc: std::sync::Arc<dyn std::any::Any + Send + Sync> =
                rust_jm.borrow().arc_handle();
            coord
                .cast::<crate::managers::primary::PyPrimaryCoordinator>()?
                .borrow_mut()
                .set_slurm_job_manager_from_rust(arc);
        }
    }

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
