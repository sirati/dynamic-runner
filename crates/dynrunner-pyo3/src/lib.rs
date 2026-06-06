use pyo3::prelude::*;

mod config;
mod discovery;
mod driver;
mod estimator;
mod fulfillability_matcher_bridge;
mod gateway;
mod identifier;
mod logging;
mod managers;
mod network;
mod peer_lifecycle_bridge;
mod protocol_manager_worker;
mod publish;
mod pytypes;
mod slurm;
mod subprocess_factory;
mod system_resources;
mod task_completed_bridge;
mod task_def;
mod transport;

use config::distributed::DistributedConfig;
use config::local_manager::PyLocalManagerConfig;
use config::log_paths::LogPathConfig;
use config::phase::PyPhase;
use config::primary_secondary::{PyPrimaryConfig, PySecondaryConfig};
use config::resources::PyResourceMap;
use config::respawn::PyRespawnPolicy;
use config::scheduler::SchedulerConfig;
use config::slurm::PySlurmConfig;
use config::worker_spec::WorkerSpec;
use gateway::local::PyLocalGateway;
use managers::distributed::PyDistributedManager;
use managers::factory_callback::{PyCallbackResourceMonitor, PyCallbackWorkerFactory};
use managers::local::PyLocalManager;
use managers::multi_process_respawner::PyMultiProcessSpawner;
use managers::observer_late_joiner::{PyObserverLateJoiner, run_observer_late_joiner};
use managers::primary::PyPrimaryCoordinator;
use managers::primary_handle::PyPrimaryHandle;
use managers::run::{compute_task_hash, run_distributed, run_local, run_primary, run_secondary};
use managers::run_config_fetch::fetch_run_config;
use managers::secondary::PySecondaryCoordinator;
use pyo3::wrap_pyfunction;
use pytypes::{PyBinaryIdentifier, PyFailedTask, PyProcessingStats, PyTaskInfo, PyTaskInfoView};
use slurm::PyRustSlurmJobManager;
use system_resources::{parse_cores, parse_memory, pick_free_port};

/// Python module definition.
/// The compiled extension is exposed as `dynamic_runner._native`;
/// the public `dynamic_runner` namespace is the mixed-layout package
/// in `python/dynamic_runner/__init__.py` which re-exports from
/// `_native` and adds the pure-Python `comm` subpackage.
#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Logging is NOT installed at import: the subscriber is chosen by the
    // Python CLI's parsed flags and installed explicitly via `init_logging`
    // (see `crate::logging::py_init_logging`). Installing at import would
    // force the config to be read from the environment before argparse runs
    // — the exact import-time coupling this surface removes.
    m.add_function(wrap_pyfunction!(logging::py_init_logging, m)?)?;

    m.add_class::<PyBinaryIdentifier>()?;
    m.add_class::<PyTaskInfo>()?;
    m.add_class::<PyTaskInfoView>()?;
    m.add_class::<PyProcessingStats>()?;
    m.add_class::<PyFailedTask>()?;
    m.add_class::<LogPathConfig>()?;
    m.add_class::<WorkerSpec>()?;
    m.add_class::<SchedulerConfig>()?;
    m.add_class::<PySlurmConfig>()?;
    m.add_class::<DistributedConfig>()?;
    m.add_class::<PyRespawnPolicy>()?;
    m.add_class::<PyResourceMap>()?;
    m.add_class::<PyPhase>()?;
    m.add_class::<PyLocalManagerConfig>()?;
    m.add_class::<PyPrimaryConfig>()?;
    m.add_class::<PySecondaryConfig>()?;
    m.add_class::<PyLocalGateway>()?;
    m.add_class::<PyLocalManager>()?;
    m.add_class::<PyDistributedManager>()?;
    m.add_class::<PyPrimaryCoordinator>()?;
    m.add_class::<PyPrimaryHandle>()?;
    m.add_class::<PyMultiProcessSpawner>()?;
    m.add_class::<PyRustSlurmJobManager>()?;
    m.add_class::<slurm::respawn_bridge::PySlurmSpawner>()?;
    m.add_class::<PySecondaryCoordinator>()?;
    m.add_class::<PyObserverLateJoiner>()?;
    m.add_class::<gateway::ssh::PySshGateway>()?;
    m.add_class::<PyCallbackWorkerFactory>()?;
    m.add_class::<PyCallbackResourceMonitor>()?;
    m.add_function(wrap_pyfunction!(run_local, m)?)?;
    m.add_function(wrap_pyfunction!(run_primary, m)?)?;
    m.add_function(wrap_pyfunction!(run_secondary, m)?)?;
    m.add_function(wrap_pyfunction!(run_distributed, m)?)?;
    m.add_function(wrap_pyfunction!(run_observer_late_joiner, m)?)?;
    m.add_function(wrap_pyfunction!(fetch_run_config, m)?)?;
    m.add_function(wrap_pyfunction!(compute_task_hash, m)?)?;
    m.add_function(wrap_pyfunction!(parse_cores, m)?)?;
    m.add_function(wrap_pyfunction!(parse_memory, m)?)?;
    m.add_function(wrap_pyfunction!(pick_free_port, m)?)?;
    m.add_class::<discovery::FolderProxy>()?;
    m.add_class::<discovery::FileProxy>()?;
    m.add_function(wrap_pyfunction!(discovery::find_items, m)?)?;
    m.add("PublishError", m.py().get_type::<publish::PublishError>())?;
    m.add_function(wrap_pyfunction!(publish::publish_one, m)?)?;
    m.add_function(wrap_pyfunction!(
        slurm::wrapper_script::generate_wrapper_script,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        slurm::wrapper_script::generate_test_wrapper_script,
        m
    )?)?;
    m.add_class::<slurm::preparation::PySlurmPreparation>()?;
    m.add_function(wrap_pyfunction!(slurm::pipeline::run_slurm_pipeline, m)?)?;
    m.add_function(wrap_pyfunction!(
        slurm::pipeline::run_remote_podman_pipeline,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(slurm::pipeline::run_preparation_py, m)?)?;

    // `dynamic_runner.driver` submodule — public driver primitives
    // extracted into the `dynrunner-driver` crate. Wired as a
    // submodule so the Python import path is
    // `dynamic_runner.driver.SshMaster` (not
    // `dynamic_runner._native.SshMaster`). The submodule is created
    // here at extension-init time; the package-level `__init__.py`
    // imports from `dynamic_runner._native.driver`.
    let driver_mod = PyModule::new(m.py(), "driver")?;
    driver_mod.add_class::<driver::PySshMaster>()?;
    driver_mod.add_function(wrap_pyfunction!(
        driver::py_cluster_is_running,
        &driver_mod
    )?)?;
    driver_mod.add_function(wrap_pyfunction!(
        driver::py_ensure_dispatcher_keypair,
        &driver_mod
    )?)?;
    driver_mod.add_function(wrap_pyfunction!(driver::py_write_ssh_config, &driver_mod)?)?;
    m.add_submodule(&driver_mod)?;

    // `dynamic_runner._native.protocol_manager_worker` submodule —
    // single source of truth for the manager-worker line-delimited
    // text codec. The Python re-export module at
    // `python/dynamic_runner/comm/proto/messages.py` imports from
    // here so existing callers (`from dynamic_runner.comm.proto
    // import Command, ...`) see the same names without changes.
    protocol_manager_worker::register(m)?;

    Ok(())
}
