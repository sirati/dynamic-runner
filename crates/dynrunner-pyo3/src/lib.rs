use pyo3::prelude::*;

mod config;
mod discovery;
mod estimator;
mod identifier;
mod managers;
mod network;
mod publish;
mod pytypes;
mod subprocess_factory;
mod system_resources;
mod task_def;
mod transport;

use config::distributed::DistributedConfig;
use config::local_manager::PyLocalManagerConfig;
use config::log_paths::LogPathConfig;
use config::phase::PyPhase;
use config::primary_secondary::{PyPrimaryConfig, PySecondaryConfig};
use config::resources::PyResourceMap;
use config::scheduler::SchedulerConfig;
use config::slurm::PySlurmConfig;
use config::worker_spec::WorkerSpec;
use managers::distributed::PyDistributedManager;
use managers::factory_callback::{PyCallbackResourceMonitor, PyCallbackWorkerFactory};
use managers::local::PyLocalManager;
use managers::primary::PyPrimaryCoordinator;
use managers::run::{compute_task_hash, run_distributed, run_local, run_primary, run_secondary};
use system_resources::{parse_cores, parse_memory, pick_free_port};
use managers::secondary::PySecondaryCoordinator;
use pyo3::wrap_pyfunction;
use pytypes::{PyBinaryIdentifier, PyTaskInfo, PyFailedTask, PyProcessingStats};

/// Python module definition.
/// The compiled extension is exposed as `dynamic_runner._native`;
/// the public `dynamic_runner` namespace is the mixed-layout package
/// in `python/dynamic_runner/__init__.py` which re-exports from
/// `_native` and adds the pure-Python `comm` subpackage.
#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();

    m.add_class::<PyBinaryIdentifier>()?;
    m.add_class::<PyTaskInfo>()?;
    m.add_class::<PyProcessingStats>()?;
    m.add_class::<PyFailedTask>()?;
    m.add_class::<LogPathConfig>()?;
    m.add_class::<WorkerSpec>()?;
    m.add_class::<SchedulerConfig>()?;
    m.add_class::<PySlurmConfig>()?;
    m.add_class::<DistributedConfig>()?;
    m.add_class::<PyResourceMap>()?;
    m.add_class::<PyPhase>()?;
    m.add_class::<PyLocalManagerConfig>()?;
    m.add_class::<PyPrimaryConfig>()?;
    m.add_class::<PySecondaryConfig>()?;
    m.add_class::<PyLocalManager>()?;
    m.add_class::<PyDistributedManager>()?;
    m.add_class::<PyPrimaryCoordinator>()?;
    m.add_class::<PySecondaryCoordinator>()?;
    m.add_class::<PyCallbackWorkerFactory>()?;
    m.add_class::<PyCallbackResourceMonitor>()?;
    m.add_function(wrap_pyfunction!(run_local, m)?)?;
    m.add_function(wrap_pyfunction!(run_primary, m)?)?;
    m.add_function(wrap_pyfunction!(run_secondary, m)?)?;
    m.add_function(wrap_pyfunction!(run_distributed, m)?)?;
    m.add_function(wrap_pyfunction!(compute_task_hash, m)?)?;
    m.add_function(wrap_pyfunction!(parse_cores, m)?)?;
    m.add_function(wrap_pyfunction!(parse_memory, m)?)?;
    m.add_function(wrap_pyfunction!(pick_free_port, m)?)?;
    m.add_class::<discovery::FolderProxy>()?;
    m.add_class::<discovery::FileProxy>()?;
    m.add_function(wrap_pyfunction!(discovery::find_items, m)?)?;
    m.add("PublishError", m.py().get_type::<publish::PublishError>())?;
    m.add_function(wrap_pyfunction!(publish::publish_one, m)?)?;
    Ok(())
}
