use pyo3::prelude::*;

use db_scheduler_impl::ResourceStealingScheduler;

/// Tuning knobs for `ResourceStealingScheduler` exposed to Python.
///
/// Defaults match the prior hard-coded values:
/// - `base_overhead`: 150 MiB
/// - `pressure_threshold`: 500 MiB
/// - `temp_factors`: `[1.5, 2.0, 3.0, 4.0]` (slowest opportunistic worker
///   gets `available / 1.5`, the next one `/ 2.0`, etc.; later workers reuse
///   the final value).
#[pyclass(name = "SchedulerConfig", get_all, set_all, from_py_object)]
#[derive(Clone, Debug)]
pub(crate) struct SchedulerConfig {
    base_overhead: u64,
    pressure_threshold: u64,
    temp_factors: Vec<f64>,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            base_overhead: 150 * 1024 * 1024,
            pressure_threshold: 500 * 1024 * 1024,
            temp_factors: vec![1.5, 2.0, 3.0, 4.0],
        }
    }
}

#[pymethods]
impl SchedulerConfig {
    #[new]
    #[pyo3(signature = (
        base_overhead = None,
        pressure_threshold = None,
        temp_factors = None,
    ))]
    fn new(
        base_overhead: Option<u64>,
        pressure_threshold: Option<u64>,
        temp_factors: Option<Vec<f64>>,
    ) -> Self {
        let d = SchedulerConfig::default();
        Self {
            base_overhead: base_overhead.unwrap_or(d.base_overhead),
            pressure_threshold: pressure_threshold.unwrap_or(d.pressure_threshold),
            temp_factors: temp_factors.unwrap_or(d.temp_factors),
        }
    }
}

impl SchedulerConfig {
    pub(crate) fn build_memory_scheduler(&self) -> ResourceStealingScheduler {
        ResourceStealingScheduler {
            resource_kind: db_comm_api_base::ResourceKind::memory(),
            base_overhead: self.base_overhead,
            pressure_threshold: self.pressure_threshold,
            temp_factors: self.temp_factors.clone(),
        }
    }
}

