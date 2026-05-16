//! `PreparationOutcome` + `run_preparation` + `run_preparation_py` —
//! drives the SLURM preparation steps in order, replacing the legacy
//! Python `SlurmPreparation.prepare(...)` body. The `_py` suffix
//! variant is the PyO3-exported entry point Python `pipeline.py`
//! calls; the inner `run_preparation` is the Rust-callable signature
//! the orchestrator uses directly.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};

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
    /// Run-scoped log dir. The per-respawn `submit_job` call uses it
    /// for the regenerated `--output=`/`--error=` paths.
    pub(super) run_log_dir: String,
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
            image_metadata: Some(image_metadata.clone().unbind()),
            gateway_host: gateway_host.clone(),
            run_log_dir: run_log_dir.clone(),
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
