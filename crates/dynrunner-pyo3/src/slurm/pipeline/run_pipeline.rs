//! `run_slurm_pipeline` — orchestrator pyfunction. Composes the
//! gateway, packaging, preparation, and `RustPrimaryCoordinator`
//! step-for-step in the same order as the legacy Python
//! `pipeline.py::run_slurm_pipeline`.

use dynrunner_core::IMPORTANT_TARGET;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use super::drive_rust::drive_rust_primary;
use super::preparation::run_preparation;
use super::{CleanupGuard, attr_truthy, pkill_leftover_tunnels, should_upload_source_binaries};

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
    // A1 gateway-connect milestone (LLM-wake): direct importance emit so
    // the dual-sink surfaces it on stdio under `--important-stdio-only`.
    // Additive to the full-log `log.info` below.
    tracing::info!(target: IMPORTANT_TARGET, "Connecting to gateway...");
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
        gateway.call_method1(
            "setup_port_forwarding",
            (primary_quic_port, primary_quic_port),
        )?;

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
            log.call_method1("info", (format!("Creating SLURM root directory: {root}"),))?;
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
    // live on the cluster filesystem. Skip the discovery walk here and
    // hand the coordinator an empty list; the submitter originates
    // `SeedSource::RelocatedSeed` (DiscoveryDebt=Owed) and relocates the
    // primary onto a compute peer, whose `discover_on_promotion` walks the
    // staged corpus on its filesystem and seeds the tasks.
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
            ("Pre-staged source mode: deferring task discovery to the relocated compute-peer primary.",),
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

    // `--mem-manager-reserved` flows verbatim from the dispatcher
    // argparse through here into the rendered wrapper-script flag.
    // Argparse default `"500M"` parses via `_native.parse_memory`;
    // `None` (operator passed an empty / null override) collapses to
    // skipping the flag, in which case the secondary's argparse
    // default takes over. Symmetric with `cores_spec` /
    // `max_memory_spec` extraction above; same dispatcher → wrapper
    // → secondary plumbing pattern.
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

    let skip_image_build: bool = args
        .getattr("skip_image_build")
        .ok()
        .and_then(|v| v.extract().ok())
        .unwrap_or(false);

    // ---- try/finally guard. Owns gateway + (post-prep) tunnel_manager. ----
    let mut guard = CleanupGuard::new(gateway.clone().unbind());

    // Arm setup-abort job rollback BEFORE preparation begins. The
    // sbatch submit-loop runs INSIDE `run_preparation`, and the steps
    // after it in there (reverse-tunnel setup, connection-info dir
    // prep) can fail — arming only after `run_preparation` returned
    // left that window uncovered, orphaning the whole just-submitted
    // cohort in the queue (asm-dataset run_20260611: a tunnel
    // establishment failure aborted dispatch with 15 jobs stranded,
    // twice). Pre-submission arming is safe by the guard's own
    // contract: `cancel_all_jobs` drains ONLY the job manager's
    // tracked `job_ids` — empty before the first sbatch, exactly the
    // already-submitted subset at any abort point after. See
    // `CleanupGuard`'s arm/disarm doc.
    guard.arm_job_cancel(job_manager.clone().unbind());

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
            num_secondaries,
            primary_quic_port,
            use_reverse_connection,
            skip_image_build,
            mem_manager_reserved_bytes,
            log,
        )?;
        // Snapshot the tunnel manager Py-ref for the respawn wiring
        // BEFORE moving the original into the cleanup guard. The
        // tunnel manager is the only Python-side holder of the
        // `Arc<SlurmPreparation>` the respawn `TunnelEstablisher`
        // needs; passing both halves (the Py-ref AND the guard's
        // owned move below) keeps the cleanup contract intact (the
        // guard's Drop tears down the same preparation instance the
        // respawn spawner shares).
        let respawn_tunnel_manager: Option<Py<PyAny>> =
            tunnel_manager.as_ref().map(|m| m.clone_ref(py));

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

        // Setup-abort job rollback is already armed (before
        // `run_preparation` above), so a failure in any remaining setup
        // step below (source-binary upload, coordinator construction,
        // the consumer's on_run_start hook) scancels the just-submitted
        // jobs rather than orphaning them. `drive_rust_primary` disarms
        // the instant it hands the run to `coord.run()`. See
        // `CleanupGuard`'s arm/disarm doc.

        log.call_method1(
            "info",
            (format!("SLURM jobs submitted; run_id={}", outcome.run_id),),
        )?;

        // ---- Source-binary upload. ----
        //
        // Gated on discovered-binaries + NOT pre-staged. Upload
        // stageability is per-item (resolved under `--source` by the
        // upload walk's strip-prefix skip), so the gate deliberately
        // does NOT consult the task-class `uses_file_based_items` flag:
        // a mixed composite (real-file items + opaque sentinels spawned
        // later, never in `binaries` here) must upload its real files.
        // See `should_upload_source_binaries`. Runs after sbatch so
        // secondaries are already starting; the primary's
        // InitialAssignment isn't sent until coord.run() reaches its
        // peer-mesh-ready gate, so a slow upload simply delays dispatch
        // rather than racing.
        if should_upload_source_binaries(
            binaries.is_empty(),
            attr_truthy(args, "source_already_staged"),
        ) {
            job_manager.call_method1("upload_source_binaries", (&binaries, &source_dir))?;
        }

        // ---- Hand-off to the Rust primary coordinator. ----
        //
        // `&mut guard` so the hand-off can disarm setup-abort job
        // rollback at the exact instant `coord.run()` takes ownership of
        // the run — see `drive_rust_primary` and `CleanupGuard`.
        drive_rust_primary(
            py,
            task,
            args,
            &outcome,
            primary_quic_port,
            &binaries,
            &slurm_config,
            &job_manager,
            respawn_tunnel_manager,
            &cores_spec,
            &max_memory_spec,
            use_reverse_connection,
            mem_manager_reserved_bytes,
            &mut guard,
            log,
        )?;

        Ok(())
    })();

    drop(guard);
    pipeline_result
}
