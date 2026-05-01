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
    /// Construct a `SecondaryConfig`. `num_workers` and
    /// `max_resources` default to system-detected values when
    /// omitted: number of logical CPUs visible to the current
    /// process (respects cgroup CPU limits, which is what we want
    /// under SLURM — `--cpus-per-task` is enforced via cgroup) and
    /// total RAM read from `/proc/meminfo`. The detection is in
    /// Rust so the Python side doesn't need a `psutil` dependency
    /// just to read two integers it then hands straight back.
    #[new]
    #[pyo3(signature = (
        secondary_id,
        num_workers = None,
        max_resources = None,
        hostname = "localhost".to_string(),
        src_network = None,
        src_tmp = None,
        distributed_config = None,
    ))]
    fn new(
        secondary_id: String,
        num_workers: Option<u32>,
        max_resources: Option<PyResourceMap>,
        hostname: String,
        src_network: Option<PathBuf>,
        src_tmp: Option<PathBuf>,
        distributed_config: Option<DistributedConfig>,
    ) -> Self {
        let num_workers = num_workers.unwrap_or_else(detect_num_workers);
        let max_resources = max_resources
            .unwrap_or_else(|| PyResourceMap::from_single("memory", detect_total_memory_bytes()));
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

/// Number of logical CPUs visible to the current process. Under
/// cgroup CPU limits (e.g. SLURM `--cpus-per-task`) the kernel
/// reflects the allocated quota here, which is what we want — we
/// would over-spawn workers if we used the host's physical core
/// count instead. Falls back to 4 if the platform can't report it.
fn detect_num_workers() -> u32 {
    std::thread::available_parallelism()
        .map(|n| n.get() as u32)
        .unwrap_or(4)
}

/// Total RAM in bytes, read from `/proc/meminfo` (Linux). The
/// framework targets Linux SLURM clusters; macOS/Windows would
/// need a different probe. Returns 0 if `/proc/meminfo` is
/// unavailable or unparseable, which lets the scheduler treat the
/// node as having no memory budget — surfaces the misdetection
/// as immediate scheduling failures rather than silent over-
/// provisioning.
fn detect_total_memory_bytes() -> u64 {
    let Ok(content) = std::fs::read_to_string("/proc/meminfo") else {
        return 0;
    };
    for line in content.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            // Format: "MemTotal:       16384000 kB"
            if let Some(kb_str) = rest.split_whitespace().next() {
                if let Ok(kb) = kb_str.parse::<u64>() {
                    return kb * 1024;
                }
            }
        }
    }
    0
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
