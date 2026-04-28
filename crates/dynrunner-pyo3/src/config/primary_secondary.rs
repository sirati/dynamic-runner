use std::path::PathBuf;

use pyo3::prelude::*;

use dynrunner_manager_distributed::{PrimaryConfig as RustPrimaryConfig, SecondaryConfig as RustSecondaryConfig};

use super::distributed::DistributedConfig;
use super::resources::PyResourceMap;

/// Per-primary tuning. Combine with `DistributedConfig` for the shared
/// connect/peer/keepalive knobs.
#[pyclass(name = "PrimaryConfig")]
#[derive(Clone)]
pub(crate) struct PyPrimaryConfig {
    #[pyo3(get, set)]
    pub(crate) node_id: String,
    #[pyo3(get, set)]
    pub(crate) num_secondaries: u32,
    #[pyo3(get, set)]
    pub(crate) distributed_config: DistributedConfig,
}

#[pymethods]
impl PyPrimaryConfig {
    #[new]
    #[pyo3(signature = (
        num_secondaries,
        node_id = "primary".to_string(),
        distributed_config = None,
    ))]
    fn new(
        num_secondaries: u32,
        node_id: String,
        distributed_config: Option<DistributedConfig>,
    ) -> Self {
        Self {
            node_id,
            num_secondaries,
            distributed_config: distributed_config.unwrap_or_default(),
        }
    }
}

impl PyPrimaryConfig {
    pub(crate) fn to_rust(&self) -> RustPrimaryConfig {
        RustPrimaryConfig {
            node_id: self.node_id.clone(),
            num_secondaries: self.num_secondaries,
            connect_timeout: self.distributed_config.connect_timeout(),
            peer_timeout: self.distributed_config.peer_timeout(),
            keepalive_interval: self.distributed_config.keepalive_interval(),
            keepalive_miss_threshold: self.distributed_config.keepalive_miss_threshold(),
        }
    }
}

/// Per-secondary tuning.
#[pyclass(name = "SecondaryConfig")]
#[derive(Clone)]
pub(crate) struct PySecondaryConfig {
    #[pyo3(get, set)]
    pub(crate) secondary_id: String,
    #[pyo3(get, set)]
    pub(crate) num_workers: u32,
    #[pyo3(get, set)]
    pub(crate) max_resources: PyResourceMap,
    #[pyo3(get, set)]
    pub(crate) hostname: String,
    #[pyo3(get, set)]
    pub(crate) src_network: Option<PathBuf>,
    #[pyo3(get, set)]
    pub(crate) src_tmp: Option<PathBuf>,
    #[pyo3(get, set)]
    pub(crate) distributed_config: DistributedConfig,
}

#[pymethods]
impl PySecondaryConfig {
    #[new]
    #[pyo3(signature = (
        secondary_id,
        num_workers,
        max_resources,
        hostname = "localhost".to_string(),
        src_network = None,
        src_tmp = None,
        distributed_config = None,
    ))]
    fn new(
        secondary_id: String,
        num_workers: u32,
        max_resources: PyResourceMap,
        hostname: String,
        src_network: Option<PathBuf>,
        src_tmp: Option<PathBuf>,
        distributed_config: Option<DistributedConfig>,
    ) -> Self {
        Self {
            secondary_id,
            num_workers,
            max_resources,
            hostname,
            src_network,
            src_tmp,
            distributed_config: distributed_config.unwrap_or_default(),
        }
    }
}

impl PySecondaryConfig {
    pub(crate) fn to_rust(&self) -> RustSecondaryConfig {
        RustSecondaryConfig {
            secondary_id: self.secondary_id.clone(),
            num_workers: self.num_workers,
            max_resources: self.max_resources.to_rust(),
            hostname: self.hostname.clone(),
            keepalive_interval: self.distributed_config.keepalive_interval(),
            src_network: self.src_network.clone(),
            src_tmp: self.src_tmp.clone(),
            peer_timeout: self.distributed_config.peer_timeout(),
            keepalive_miss_threshold: self.distributed_config.keepalive_miss_threshold(),
        }
    }
}
