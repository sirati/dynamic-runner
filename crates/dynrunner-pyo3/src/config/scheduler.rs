use pyo3::prelude::*;

use dynrunner_core::ResourceKind;
use dynrunner_scheduler::ResourceStealingScheduler;

/// Tuning knobs for `ResourceStealingScheduler` exposed to Python.
///
/// Defaults target the canonical `"memory"` resource kind:
/// - `resource_kind`: `"memory"` (any opaque string is accepted —
///   future multi-resource composition would create one
///   `SchedulerConfig` per kind).
/// - `base_overhead`: 150 MiB
/// - `pressure_threshold`: 500 MiB
/// - `cgroup_safety_margin`: 1 GiB — headroom below the cgroup cap
///   at which the framework's userland preempt fires. Without this
///   margin the active-kill threshold sat at exactly the cgroup cap
///   and consistently lost the race against the kernel's
///   `memory.max` enforcement; with the margin, both kill branches
///   shift down so the smallest-active kill lands BEFORE the kernel
///   SIGKILLs the cgroup. Surfaced to operators via
///   `--oom-cgroup-safety-margin`.
/// - `swap_pressure_threshold`: 64 MiB — aggregate per-worker swap
///   above which the main multi-worker phase fires the heaviest-
///   swapper kill. The contract is "a worker's swap counts as RAM
///   demand; kill workers to free RAM so no swap is used"; this knob
///   is the small hysteresis band that ignores cold-page eviction
///   while still tripping on genuine working-set spill. Suppressed
///   when only one worker is active (the OOM-retry phase exception).
/// - `temp_factors`: `[1.5, 2.0, 3.0, 4.0]` (slowest opportunistic worker
///   gets `available / 1.5`, the next one `/ 2.0`, etc.; later workers reuse
///   the final value).
#[pyclass(name = "SchedulerConfig", get_all, set_all, from_py_object)]
#[derive(Clone, Debug)]
pub(crate) struct SchedulerConfig {
    resource_kind: String,
    base_overhead: u64,
    pressure_threshold: u64,
    cgroup_safety_margin: u64,
    swap_pressure_threshold: u64,
    temp_factors: Vec<f64>,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            resource_kind: "memory".into(),
            base_overhead: 150 * 1024 * 1024,
            pressure_threshold: 500 * 1024 * 1024,
            cgroup_safety_margin: 1024 * 1024 * 1024,
            swap_pressure_threshold: 64 * 1024 * 1024,
            temp_factors: vec![1.5, 2.0, 3.0, 4.0],
        }
    }
}

#[pymethods]
impl SchedulerConfig {
    #[new]
    #[pyo3(signature = (
        resource_kind = None,
        base_overhead = None,
        pressure_threshold = None,
        cgroup_safety_margin = None,
        swap_pressure_threshold = None,
        temp_factors = None,
    ))]
    fn new(
        resource_kind: Option<String>,
        base_overhead: Option<u64>,
        pressure_threshold: Option<u64>,
        cgroup_safety_margin: Option<u64>,
        swap_pressure_threshold: Option<u64>,
        temp_factors: Option<Vec<f64>>,
    ) -> Self {
        let d = SchedulerConfig::default();
        Self {
            resource_kind: resource_kind.unwrap_or(d.resource_kind),
            base_overhead: base_overhead.unwrap_or(d.base_overhead),
            pressure_threshold: pressure_threshold.unwrap_or(d.pressure_threshold),
            cgroup_safety_margin: cgroup_safety_margin.unwrap_or(d.cgroup_safety_margin),
            swap_pressure_threshold: swap_pressure_threshold
                .unwrap_or(d.swap_pressure_threshold),
            temp_factors: temp_factors.unwrap_or(d.temp_factors),
        }
    }
}

impl SchedulerConfig {
    /// Backwards-compatible alias for `build_scheduler` — used by
    /// existing call sites that hard-coded a memory-only scheduler.
    pub(crate) fn build_memory_scheduler(&self) -> ResourceStealingScheduler {
        self.build_scheduler()
    }

    pub(crate) fn build_scheduler(&self) -> ResourceStealingScheduler {
        ResourceStealingScheduler {
            resource_kind: ResourceKind::new(self.resource_kind.as_str()),
            base_overhead: self.base_overhead,
            pressure_threshold: self.pressure_threshold,
            cgroup_safety_margin: self.cgroup_safety_margin,
            swap_pressure_threshold: self.swap_pressure_threshold,
            temp_factors: self.temp_factors.clone(),
        }
    }
}
