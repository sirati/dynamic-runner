//! PyO3 bindings for `dynrunner_slurm::wrapper_script::*`.
//!
//! Boundary contract:
//!   - Python pre-resolves its object graph (tilde-expanded paths,
//!     `PodmanPackaging.get_load_command(...)` already-substituted
//!     bash snippet, etc.) into flat strings.
//!   - These functions extract the kwargs into Rust types and call
//!     into the generator. No object types cross the boundary.
//!
//! The thin Python shim in
//! `python/dynamic_runner/packaging/job_manager.py` is the only
//! caller; it preserves the public Python signature for back-compat.

use std::path::Path;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use dynrunner_slurm::config::SlurmConfig;
use dynrunner_slurm::wrapper_script::{
    generate_test_wrapper_script as rust_generate_test_wrapper_script,
    generate_wrapper_script as rust_generate_wrapper_script, ConnectionMode,
    TestWrapperScriptConfig, WrapperScriptConfig,
};

/// Build a `SlurmConfig` from just the `root_folder` — that's the
/// only field the wrapper-script generator reads (via
/// `src_bins_path`, `output_path`, `log_path`). The richer fields
/// (`partition`, `time_limit`, …) are sbatch-submission concerns
/// owned by `SlurmJobManager`, not the wrapper-render concern. We
/// intentionally do NOT extract the full Python `SlurmConfig`
/// object here — keeping the boundary minimal means changes to the
/// Python config shape don't ripple into the generator API.
fn build_slurm_config(root_folder: &str) -> SlurmConfig {
    SlurmConfig {
        root_folder: root_folder.to_string(),
        ..SlurmConfig::default()
    }
}

/// Render the SLURM secondary-job wrapper script. Mirrors the
/// kwargs of the Python `SlurmJobManager.generate_wrapper_script`
/// method, modulo the object → string flattening done by the
/// Python shim.
#[pyfunction]
#[pyo3(signature = (
    *,
    root_folder,
    image_path,
    secondary_id,
    image_name,
    image_tag,
    image_tar_basename,
    load_command,
    container_command,
    srcbins_mount_source,
    output_dir,
    cores_spec,
    max_memory_spec,
    forwarded_argv = Vec::new(),
    run_log_dir = None,
    dynrunner_network_dir = None,
    extra_run_args = Vec::new(),
    gateway_host = None,
    gateway_port = None,
    reverse_connection = false,
    connection_info_dir = None,
    is_observer = false,
    shutdown_manager_bin_path = None,
    mem_manager_reserved_bytes = None,
))]
#[allow(clippy::too_many_arguments)]
pub fn generate_wrapper_script(
    root_folder: &str,
    image_path: &str,
    secondary_id: &str,
    image_name: &str,
    image_tag: &str,
    image_tar_basename: &str,
    load_command: &str,
    container_command: &str,
    srcbins_mount_source: &str,
    output_dir: &str,
    cores_spec: &str,
    max_memory_spec: &str,
    forwarded_argv: Vec<String>,
    run_log_dir: Option<&str>,
    dynrunner_network_dir: Option<&str>,
    extra_run_args: Vec<String>,
    gateway_host: Option<&str>,
    gateway_port: Option<u16>,
    reverse_connection: bool,
    connection_info_dir: Option<&str>,
    is_observer: bool,
    shutdown_manager_bin_path: Option<&str>,
    mem_manager_reserved_bytes: Option<u64>,
) -> PyResult<String> {
    let slurm_config = build_slurm_config(root_folder);

    // Connection-mode selection. The two arms have disjoint required
    // kwargs; we surface a clear error if the caller supplies the
    // wrong combination instead of silently defaulting.
    let connection = if reverse_connection {
        let dir = connection_info_dir.ok_or_else(|| {
            PyValueError::new_err(
                "reverse_connection=True requires `connection_info_dir`",
            )
        })?;
        ConnectionMode::Reverse {
            connection_info_dir: dir,
        }
    } else {
        let host = gateway_host.ok_or_else(|| {
            PyValueError::new_err(
                "reverse_connection=False requires `gateway_host` and `gateway_port`",
            )
        })?;
        let port = gateway_port.ok_or_else(|| {
            PyValueError::new_err(
                "reverse_connection=False requires `gateway_host` and `gateway_port`",
            )
        })?;
        ConnectionMode::Standard {
            gateway_host: host,
            gateway_port: port,
        }
    };

    // Resolve the shutdown-manager binary path the wrapper renderer
    // expects (`Option<&Path>`) from the kwarg's string shape. The
    // Python side passes the value the Rust job-manager recorded
    // after `upload_shutdown_manager_binary_from` ran. In production
    // the SLURM dispatch path always populates this (the Python
    // bridge raises on missing source binary rather than skipping);
    // the `None` branch exists for renderer-internal unit tests and
    // back-compat callers that do not exercise the SLURM dispatch
    // path.
    let shutdown_manager_bin_path = shutdown_manager_bin_path.map(Path::new);

    let cfg = WrapperScriptConfig {
        slurm_config: &slurm_config,
        image_path,
        secondary_id,
        image_name,
        image_tag,
        image_tar_basename,
        load_command,
        container_command,
        cores_spec,
        max_memory_spec,
        connection,
        run_log_dir,
        dynrunner_network_dir,
        srcbins_mount_source: Some(srcbins_mount_source),
        output_dir: Some(output_dir),
        extra_run_args: &extra_run_args,
        forwarded_argv: &forwarded_argv,
        is_observer,
        shutdown_manager_bin_path,
        mem_manager_reserved_bytes,
    };
    Ok(rust_generate_wrapper_script(&cfg))
}

/// Render the image-validation test wrapper script.
#[pyfunction]
#[pyo3(signature = (
    *,
    image_path,
    image_name,
    image_tag,
    image_tar_basename,
    load_command,
    container_command,
))]
pub fn generate_test_wrapper_script(
    image_path: &str,
    image_name: &str,
    image_tag: &str,
    image_tar_basename: &str,
    load_command: &str,
    container_command: &str,
) -> PyResult<String> {
    let cfg = TestWrapperScriptConfig {
        image_path,
        image_name,
        image_tag,
        image_tar_basename,
        load_command,
        container_command,
    };
    Ok(rust_generate_test_wrapper_script(&cfg))
}
