//! `PreparationOutcome` + `run_preparation` + `run_preparation_py` —
//! drives the SLURM preparation steps in order, replacing the legacy
//! Python `SlurmPreparation.prepare(...)` body. The `_py` suffix
//! variant is the PyO3-exported entry point Python `pipeline.py`
//! calls; the inner `run_preparation` is the Rust-callable signature
//! the orchestrator uses directly.

use std::collections::HashMap;

use dynrunner_core::IMPORTANT_TARGET;
use dynrunner_slurm::preparation::TunnelSetupSummary;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};

use super::image_build::ImageBuild;

/// Whether the run is building the image (backgrounded) or skipping the
/// build (`--skip-image-build`). Lets `run_preparation` defer the
/// metadata resolution to a single match arm at the submit-loop's edge
/// without re-checking `skip_image_build` there.
enum ImageBuildPhase {
    /// Real-build path: the build is in flight on a background thread;
    /// `join` yields the metadata or propagates the build error.
    Building(ImageBuild),
    /// `--skip-image-build`: metadata is synthesised inline from local
    /// paths at the resolution site (no background work).
    Skipped,
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
pub(super) struct PreparationOutcome {
    pub(super) num_secondaries: u32,
    pub(super) run_id: String,
    pub(super) cert_dir: Py<PyAny>,
    pub(super) primary_entropy: Py<PyAny>,
    pub(super) mode_specific_data: Py<PyDict>,
    /// Image metadata kept after preparation so the SLURM respawn
    /// wiring (`drive_rust_primary`) can re-render a per-respawn
    /// wrapper script with the same image path the initial cohort
    /// uses. None outside reverse-connection mode (no respawn path is
    /// reachable there today either, but the field is shape-compatible).
    pub(super) image_metadata: Option<Py<PyAny>>,
    /// Gateway hostname secondaries dial back through. Captured here
    /// so the per-respawn wrapper-script generator closure in
    /// `drive_rust_primary` doesn't need to re-import the gateway
    /// reference.
    pub(super) gateway_host: String,
    /// Consumer program-identity prefix (deployment spec
    /// `effective_job_name_prefix`) for the scratch dir + container
    /// name. Captured here so the per-respawn wrapper-script generator
    /// closure threads the same prefix the initial cohort used.
    pub(super) name_prefix: String,
    /// Run-scoped log dir. The per-respawn `submit_job` call uses it
    /// for the regenerated `--output=`/`--error=` paths.
    pub(super) run_log_dir: String,
    /// Gateway-side path of the uploaded `dynrunner-slurm-shutdown`
    /// binary. Always populated on a successful preparation run:
    /// the Python bridge raises on missing source binary rather than
    /// skipping silently. Modelled as `Option` only because the field
    /// type matches the wrapper-renderer's
    /// `shutdown_manager_bin_path: Option<&Path>` kwarg shape
    /// downstream. Captured here so the per-respawn wrapper-script
    /// generator closure threads the same path the initial cohort
    /// received into every respawned secondary's wrapper, without
    /// re-uploading the binary.
    pub(super) shutdown_manager_remote_path: Option<String>,
    /// Gateway-side path of the uploaded `dynrunner-slurm-wrapper`
    /// binary. Always populated on a successful preparation run (the
    /// Python bridge raises on missing source rather than skipping).
    /// Threaded into every wrapper render (initial cohort + respawn)
    /// as the `wrapper_bin_path` kwarg so each per-job stub `exec`s the
    /// binary at the same path. Mirrors
    /// `shutdown_manager_remote_path`.
    pub(super) wrapper_bin_remote_path: Option<String>,
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
pub(super) fn run_preparation<'py>(
    py: Python<'py>,
    gateway: &Bound<'py, PyAny>,
    job_manager: &Bound<'py, PyAny>,
    slurm_config: &Bound<'py, PyAny>,
    deployment: &Bound<'py, PyAny>,
    run_id: &str,
    cert_dir: &Bound<'py, PyAny>,
    cores_spec: &str,
    max_memory_spec: &str,
    num_secondaries: u32,
    primary_quic_port: u16,
    use_reverse_connection: bool,
    skip_image_build: bool,
    mem_manager_reserved_bytes: Option<u64>,
    log: &Bound<'py, PyAny>,
) -> PyResult<(PreparationOutcome, Option<Py<PyAny>>)> {
    let base_log_dir: String = slurm_config.call_method0("get_log_dir")?.str()?.extract()?;
    let run_log_dir = format!("{base_log_dir}/{run_id}");

    // Gateway run-log path milestone (LLM-wake): emit as soon as the
    // run-log dir is decided — the earliest point it is known — so an
    // operator watching `--important-stdio-only` learns where the run's
    // gateway-side logs land before any of the longer setup steps run.
    // Dual-sink: the importance target also reaches the normal log.
    tracing::info!(
        target: IMPORTANT_TARGET,
        "run logs: {run_log_dir}",
    );

    // Kick off the container image build/transfer on a background
    // thread BEFORE the gateway-independent setup work below, so the
    // long-pole local nix build overlaps the binary uploads + dir prep.
    // The real-build branch is the only one worth backgrounding; the
    // `--skip-image-build` branch just synthesises metadata from local
    // paths (cheap, GIL-bound, no importance emit) so it stays inline
    // and is joined immediately. See `image_build::ImageBuild`.
    let image_build = if skip_image_build {
        ImageBuildPhase::Skipped
    } else {
        let project_root = py
            .import("pathlib")?
            .getattr("Path")?
            .call0()?
            .call_method0("cwd")?;
        ImageBuildPhase::Building(ImageBuild::spawn(
            job_manager.clone().unbind(),
            project_root.unbind(),
            log.clone().unbind(),
        ))
    };

    // ---- Gateway-independent setup work, overlapping the build. ----
    //
    // Run as a fallible inner block so that — whatever its outcome —
    // the background build is joined BEFORE we leave the function. A
    // bare `?` here would short-circuit out while the build thread is
    // still in flight, detaching it onto a gateway that teardown is
    // about to disconnect. Joining first reaps the thread cleanly. The
    // independent-work error takes precedence over the build's result
    // (the build outcome is irrelevant once setup has already failed).
    let independent: PyResult<(Option<String>, Option<String>)> = (|| {
        // Directory prep (gateway-side mkdir for image / srcbins /
        // output / log roots) — delegated to the Rust job_manager.
        job_manager.call_method0("prepare_directories")?;
        gateway.call_method1("create_directory", (&run_log_dir,))?;

        // Stage the `dynrunner-slurm-shutdown` musl-static binary on the
        // gateway so per-job wrapper scripts can spawn it via
        // `systemd-run --user --unit` (service mode) and have it survive
        // cgroup teardown. The Python bridge resolves the local source
        // path (`DYNRUNNER_SLURM_SHUTDOWN_BIN_SOURCE` override >
        // wheel-bundled artifact under `dynamic_runner/_shutdown_manager/`)
        // and raises a `RuntimeError` when neither is available —
        // orphan-container cleanup is no longer opt-in.
        //
        // The resolved gateway-side path is stored on the Rust manager
        // and surfaced via the `shutdown_manager_remote_path` getter so
        // every wrapper render in this run (initial cohort + respawn)
        // sees the same path without re-uploading the binary.
        let shutdown_manager_remote_path: Option<String> = job_manager
            .call_method0("upload_shutdown_manager_binary")?
            .extract()?;

        // Stage the `dynrunner-slurm-wrapper` musl-static binary on the
        // gateway so each per-job wrapper-script stub can `exec` it to
        // run the full secondary lifecycle (replacing the legacy inline
        // bash). Same bridge contract as the shutdown-manager upload:
        // the Python side resolves the local source path
        // (`DYNRUNNER_SLURM_WRAPPER_BIN_SOURCE` override > wheel-bundled
        // artifact under `dynamic_runner/_wrapper_manager/`) and raises
        // on missing source. The resolved gateway-side path is stored on
        // the Rust manager and surfaced via `wrapper_bin_remote_path` so
        // every wrapper render in this run (initial cohort + respawn)
        // emits the stub against the same binary path.
        let wrapper_bin_remote_path: Option<String> = job_manager
            .call_method0("upload_wrapper_binary")?
            .extract()?;

        Ok((shutdown_manager_remote_path, wrapper_bin_remote_path))
    })();

    // Resolve the image metadata. The background build (real-build
    // path) is awaited HERE — right after the independent work above,
    // before the submit-loop (its first consumer) — so the build has
    // overlapped the dir prep + binary uploads, while a build failure
    // still aborts preparation at the same point the synchronous
    // version did (the join re-raises the build's `PyErr`). Joining
    // unconditionally (even when `independent` errored) reaps the build
    // thread before teardown. The skip-build path synthesises the
    // metadata from local paths inline. Both produce a
    // `PodmanImageMetadata` with `remote_path`, `image_hash`,
    // `uploaded` fields that the wrapper-script generator reads.
    let image_result: PyResult<Bound<'py, PyAny>> = match image_build {
        ImageBuildPhase::Building(build) => build.join(py).map(|m| m.into_bound(py)),
        ImageBuildPhase::Skipped => (|| {
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
            let podman_module = py.import("dynamic_runner.packaging.podman")?;
            let metadata_cls = podman_module.getattr("PodmanImageMetadata")?;
            let metadata_kwargs = PyDict::new(py);
            metadata_kwargs.set_item("remote_path", image_path)?;
            metadata_kwargs.set_item("image_hash", "")?;
            metadata_kwargs.set_item("uploaded", false)?;
            metadata_cls.call((), Some(&metadata_kwargs))
        })(),
    };

    // Independent-work failure is primary; the build outcome (whether
    // it succeeded or failed) is moot once setup has already failed, so
    // its result is dropped here after the join above reaped the thread.
    let (shutdown_manager_remote_path, wrapper_bin_remote_path) = independent?;
    let image_metadata = image_result?;

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
        (format!(
            "Using gateway hostname (as configured by user): {gateway_host}"
        ),),
    )?;

    log.call_method1("info", ("Submitting SLURM jobs...",))?;
    let job_name_prefix: String = deployment.getattr("effective_job_name_prefix")?.extract()?;
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
        wrapper_kwargs.set_item("reverse_connection", use_reverse_connection)?;
        wrapper_kwargs.set_item("run_log_dir", &run_log_dir)?;
        wrapper_kwargs.set_item(
            "shutdown_manager_bin_path",
            shutdown_manager_remote_path.as_deref(),
        )?;
        // Consumer program-identity prefix for the scratch dir +
        // container name. Sourced from the deployment spec's
        // `effective_job_name_prefix` (`slurm_job_name_prefix` or
        // `image_name`) — the same field that already names the SLURM
        // job `{prefix}-{secondary_id}`, so the prefix the operator
        // sees in squeue matches the one in `/tmp/<prefix>-…` and the
        // container name. Replaces the framework's old hardcoded `asm`.
        wrapper_kwargs.set_item("name_prefix", &job_name_prefix)?;
        // Gateway-side path of the uploaded wrapper binary → renderer
        // emits the `exec`-stub body. Always Some on a successful prep.
        wrapper_kwargs.set_item("wrapper_bin_path", wrapper_bin_remote_path.as_deref())?;
        if let Some(reserved) = mem_manager_reserved_bytes {
            wrapper_kwargs.set_item("mem_manager_reserved_bytes", reserved)?;
        }
        let wrapper =
            job_manager.call_method("generate_wrapper_script", (), Some(&wrapper_kwargs))?;

        let submit_kwargs = PyDict::new(py);
        submit_kwargs.set_item("run_log_dir", &run_log_dir)?;
        let job_id = job_manager.call_method(
            "submit_job",
            (&wrapper, &job_name, &secondary_id),
            Some(&submit_kwargs),
        )?;
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
        let mut established: HashMap<String, u16> = HashMap::new();
        for (k, v) in port_map.cast::<PyDict>()?.iter() {
            let id: String = k.extract()?;
            let port: u16 = v.extract()?;
            secondary_port_map.set_item(&id, port)?;
            established.insert(id, port);
        }
        // HONEST summary (#278): `setup_ssh_tunnels` allows K-of-N
        // partial success by design (late-joiners attach via PeerJoined),
        // so the headline must report what was actually VERIFIED — never
        // claim "All N" from the requested count. Partial fleets are
        // WARNed with the missing ids named; the summary type is the
        // same one the Rust preparation layer renders, so the two
        // layers cannot drift.
        let summary = TunnelSetupSummary::new(&established, num_secondaries as usize);
        let level = if summary.is_complete() {
            "info"
        } else {
            "warning"
        };
        log.call_method1(level, (summary.to_string(),))?;
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
            image_metadata: Some(image_metadata.clone().unbind()),
            gateway_host: gateway_host.clone(),
            name_prefix: job_name_prefix.clone(),
            run_log_dir: run_log_dir.clone(),
            shutdown_manager_remote_path: shutdown_manager_remote_path.clone(),
            wrapper_bin_remote_path: wrapper_bin_remote_path.clone(),
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
    log,
    mem_manager_reserved_bytes = None,
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
    log: &Bound<'py, PyAny>,
    mem_manager_reserved_bytes: Option<u64>,
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
        num_secondaries,
        primary_quic_port,
        use_reverse_connection,
        skip_image_build,
        mem_manager_reserved_bytes,
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
