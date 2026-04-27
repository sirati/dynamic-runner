use pyo3::prelude::*;

mod config;
mod estimator;
mod identifier;
mod managers;
mod network;
mod pytypes;
mod subprocess_factory;
mod task_def;
mod transport;

use config::distributed::DistributedConfig;
use config::log_paths::LogPathConfig;
use config::scheduler::SchedulerConfig;
use config::worker_spec::WorkerSpec;
use managers::distributed::PyDistributedManager;
use managers::factory_callback::{PyCallbackResourceMonitor, PyCallbackWorkerFactory};
use managers::local::PyLocalManager;
use managers::primary::PyPrimaryCoordinator;
use managers::secondary::PySecondaryCoordinator;
use pytypes::{PyBinaryIdentifier, PyBinaryInfo, PyFailedTask, PyProcessingStats};

/// Python module definition.
#[pymodule]
fn dynamic_batch_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();

    m.add_class::<PyBinaryIdentifier>()?;
    m.add_class::<PyBinaryInfo>()?;
    m.add_class::<PyProcessingStats>()?;
    m.add_class::<PyFailedTask>()?;
    m.add_class::<LogPathConfig>()?;
    m.add_class::<WorkerSpec>()?;
    m.add_class::<SchedulerConfig>()?;
    m.add_class::<DistributedConfig>()?;
    m.add_class::<PyLocalManager>()?;
    m.add_class::<PyDistributedManager>()?;
    m.add_class::<PyPrimaryCoordinator>()?;
    m.add_class::<PySecondaryCoordinator>()?;
    m.add_class::<PyCallbackWorkerFactory>()?;
    m.add_class::<PyCallbackResourceMonitor>()?;
    Ok(())
}
