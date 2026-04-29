//! Common Python `TaskDefinition` adapter.
//!
//! Every Python-facing manager (`RustLocalManager`, `RustDistributedManager`,
//! `RustSecondaryCoordinator`) needs the same set of fields off of the
//! `task_definition` Python object. This module bundles the extraction so a
//! single source of truth governs the `TaskDefinition` ABI seen from Rust.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use pyo3::prelude::*;

use dynrunner_core::TypeId;

use crate::config::log_paths::LogPathConfig;
use crate::estimator::PyMemoryEstimatorBridge;

/// Resolved fields pulled out of a Python `task_definition` instance, plus the
/// per-run paths the runner derives from it.
pub(crate) struct LoadedTaskDefinition {
    pub(crate) estimator: PyMemoryEstimatorBridge,
    pub(crate) worker_module: String,
    pub(crate) worker_cmd_args: Vec<String>,
    pub(crate) source_path: PathBuf,
    pub(crate) output_path: PathBuf,
    pub(crate) log_dir: PathBuf,
    pub(crate) log_paths: LogPathConfig,
    pub(crate) python_executable: PathBuf,
}

impl LoadedTaskDefinition {
    pub(crate) fn from_python(
        py: Python<'_>,
        task_definition: &Bound<'_, PyAny>,
        task_args: &Bound<'_, PyAny>,
        source_dir: &str,
        output_dir: &str,
        skip_existing: bool,
        log_paths: Option<LogPathConfig>,
    ) -> PyResult<Self> {
        // TODO(phases-5a): replace this single ("default", "estimate_memory")
        // tuple with the full set of (TypeId, estimator_attr) pairs extracted
        // from `task_definition.get_phases()`. Phase 3 only wires the new
        // constructor signature; Phase 5A will walk the phase graph.
        let types = vec![(TypeId::from("default"), "estimate_memory".to_string())];
        let estimator = PyMemoryEstimatorBridge::from_python(py, task_definition, &types)?;

        let worker_module: String = task_definition
            .call_method0("get_worker_module")?
            .extract()?;

        let source_path = PathBuf::from(source_dir);
        let output_path = PathBuf::from(output_dir);
        let worker_cmd_args: Vec<String> = task_definition
            .call_method1(
                "build_worker_command_args",
                (
                    task_args,
                    source_path.to_str().unwrap(),
                    output_path.to_str().unwrap(),
                    skip_existing,
                ),
            )?
            .extract()?;

        let log_paths = log_paths.unwrap_or_default();
        let log_dir = log_paths.resolve_log_dir(py, &output_path)?;
        std::fs::create_dir_all(&log_dir).ok();

        let sys = py.import("sys")?;
        let python_executable: String = sys.getattr("executable")?.extract()?;

        Ok(Self {
            estimator,
            worker_module,
            worker_cmd_args,
            source_path,
            output_path,
            log_dir,
            log_paths,
            python_executable: PathBuf::from(python_executable),
        })
    }
}

/// Pull `TaskDefinition.get_stages()` into a phase-name → timeout map.
///
/// Only `LocalManager` consumes per-stage timeouts today; the distributed
/// secondaries don't run a stage timer (their primary delegates assignment
/// timing). This helper lives next to `LoadedTaskDefinition` because it
/// reads the same Python object.
pub(crate) fn extract_stage_timeouts(
    task_definition: &Bound<'_, PyAny>,
) -> PyResult<HashMap<String, Duration>> {
    let mut stage_timeouts = HashMap::new();
    let stages: Vec<Bound<'_, PyAny>> = task_definition.call_method0("get_stages")?.extract()?;
    for stage in &stages {
        let phase = stage.getattr("phase")?;
        let phase_name: String = phase.getattr("value")?.extract()?;
        let timeout_obj = stage.getattr("timeout_seconds")?;
        if !timeout_obj.is_none() {
            let timeout_secs: f64 = timeout_obj.extract()?;
            stage_timeouts.insert(phase_name, Duration::from_secs_f64(timeout_secs));
        }
    }
    Ok(stage_timeouts)
}

