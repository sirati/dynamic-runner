//! `build_slurm_respawn_kwargs` — assembles the SLURM respawn
//! collaborators (`Arc<Mutex<SlurmJobManager>>`,
//! `Arc<SlurmPreparation>`, wrapper-script generator closure) into a
//! `PySlurmSpawner` pyclass + `PyRespawnPolicy`, returning `None`
//! when any required input is missing.

use pyo3::prelude::*;

use super::preparation::PreparationOutcome;

/// Build the (`respawn_policy`, `respawn_spawner`) pair the SLURM
/// pipeline hands to `RustPrimaryCoordinator(...)` at coordinator
/// construction.
///
/// Single concern: assemble the SLURM respawn collaborators
/// (`Arc<Mutex<SlurmJobManager>>`, `Arc<SlurmPreparation>`,
/// wrapper-script generator closure capturing the deployment
/// context) into a `PySlurmSpawner` pyclass + a `PyRespawnPolicy`,
/// returning `None` if any required input is missing (out-of-tree
/// callers subclassing `SlurmJobManager` without the `_rust` attr,
/// or non-reverse-connection topologies with no tunnel manager).
/// The caller short-circuits the respawn wiring in that case —
/// preserving the legacy "respawn pipeline disabled, no crash"
/// behaviour while logging the reason.
#[allow(clippy::too_many_arguments)]
pub(super) fn build_slurm_respawn_kwargs<'py>(
    py: Python<'py>,
    args: &Bound<'py, PyAny>,
    job_manager: &Bound<'py, PyAny>,
    tunnel_manager: Option<&Py<PyAny>>,
    outcome: &PreparationOutcome,
    primary_quic_port: u16,
    cores_spec: &str,
    max_memory_spec: &str,
    use_reverse_connection: bool,
    mem_manager_reserved_bytes: Option<u64>,
    log: &Bound<'py, PyAny>,
) -> PyResult<Option<(Py<PyAny>, Py<PyAny>)>> {
    // --- Budget from CLI flags. ---
    let max_per_secondary: u32 = args
        .getattr("respawn_max_per_secondary")
        .ok()
        .and_then(|v| v.extract().ok())
        .unwrap_or(3);
    let max_total: u32 = args
        .getattr("respawn_max_total")
        .ok()
        .and_then(|v| v.extract().ok())
        .unwrap_or(10);
    // `--respawn-cooldown` is a duration string ("30s", "1m", …);
    // delegate parsing to the existing `parse_duration_secs` helper
    // on the CLI module so the SLURM path and the in-process path
    // share one source of truth.
    let cooldown_secs: f64 = match args.getattr("respawn_cooldown") {
        Ok(v) if !v.is_none() => {
            let cli_module = py.import("dynamic_runner.cli")?;
            cli_module
                .getattr("parse_duration_secs")?
                .call1((v,))?
                .extract()?
        }
        _ => 30.0,
    };
    let respawn_module = py.import("dynamic_runner")?;
    let policy_cls = respawn_module.getattr("RespawnPolicy")?;
    let policy = policy_cls.call_method1(
        "on_secondary_death",
        (max_per_secondary, max_total, cooldown_secs),
    )?;

    // --- Job manager Arc (must be the pyclass `_rust` handle). ---
    let job_manager_arc = match job_manager.getattr("_rust") {
        Ok(rust_handle) => match rust_handle.cast::<crate::slurm::PyRustSlurmJobManager>() {
            Ok(rust_jm) => rust_jm.borrow().arc_handle(),
            Err(_) => {
                log.call_method1(
                    "warning",
                    (
                        "SLURM respawn pipeline NOT wired: job_manager._rust is not a \
                         RustSlurmJobManager pyclass (out-of-tree subclass?). \
                         Respawn requests will be silently dropped — the operator \
                         flag is honoured at the policy level but no spawner exists \
                         to fulfil them.",
                    ),
                )?;
                return Ok(None);
            }
        },
        Err(_) => {
            log.call_method1(
                "warning",
                (
                    "SLURM respawn pipeline NOT wired: job_manager has no _rust \
                     attribute. The respawn policy will be policy-only (no spawner).",
                ),
            )?;
            return Ok(None);
        }
    };

    // --- Preparation Arc + gateway reader (from the tunnel manager). ---
    let (preparation_arc, info_reader) = match tunnel_manager {
        Some(mgr) => {
            let bound = mgr.bind(py);
            match bound.cast::<crate::slurm::preparation::PySlurmPreparation>() {
                Ok(prep) => {
                    let prep_borrow = prep.borrow();
                    (
                        prep_borrow.arc_handle(),
                        crate::slurm::preparation::PyGatewayReader::new(
                            prep_borrow.gateway_handle(py),
                        ),
                    )
                }
                Err(_) => {
                    log.call_method1(
                        "warning",
                        ("SLURM respawn pipeline NOT wired: tunnel_manager is \
                             not a RustSlurmPreparation pyclass. Cannot share the \
                             tunnel-establisher pool with the initial cohort.",),
                    )?;
                    return Ok(None);
                }
            }
        }
        None => {
            log.call_method1(
                "warning",
                (
                    "SLURM respawn pipeline NOT wired: no tunnel manager in the \
                     current topology. Respawn requires reverse-connection mode.",
                ),
            )?;
            return Ok(None);
        }
    };

    // --- Wrapper-script generator closure. ---
    let image_metadata = match outcome.image_metadata.as_ref() {
        Some(m) => m.clone_ref(py),
        None => {
            log.call_method1(
                "warning",
                (
                    "SLURM respawn pipeline NOT wired: no image metadata recorded \
                     by preparation. This usually means preparation did not run \
                     to completion.",
                ),
            )?;
            return Ok(None);
        }
    };
    let wrapper_gen = crate::slurm::respawn_bridge::wrapper_script_generator_from_pyobj(
        job_manager.clone().unbind(),
        image_metadata,
        outcome.gateway_host.clone(),
        primary_quic_port,
        cores_spec.to_owned(),
        max_memory_spec.to_owned(),
        use_reverse_connection,
        outcome.run_log_dir.clone(),
        outcome.shutdown_manager_remote_path.clone(),
        outcome.name_prefix.clone(),
        outcome.wrapper_bin_remote_path.clone(),
        mem_manager_reserved_bytes,
    );

    // --- Build the PySlurmSpawner pyclass. ---
    //
    // Rule 3 (#543): a respawned secondary's `--job-name` preserves the
    // initial-cohort consumer prefix (`outcome.name_prefix`) — operators
    // eyeballing `squeue` see consistent naming across initial and
    // respawned jobs, instead of the bare framework id-only fallback.
    let consumer_job_name_prefix = if outcome.name_prefix.is_empty() {
        None
    } else {
        Some(outcome.name_prefix.clone())
    };
    let spawner = crate::slurm::respawn_bridge::PySlurmSpawner::new(
        job_manager_arc,
        preparation_arc,
        info_reader,
        wrapper_gen,
        outcome.run_log_dir.clone(),
        consumer_job_name_prefix,
    );
    let spawner_py = Py::new(py, spawner)?;

    log.call_method1(
        "info",
        (format!(
            "SLURM respawn pipeline wired: policy=on-secondary-death \
             (max_per={max_per_secondary}, max_total={max_total}, cooldown={cooldown_secs:.1}s)",
        ),),
    )?;

    Ok(Some((policy.unbind(), spawner_py.into_any())))
}
