//! `PySecondaryCoordinator` constructor — wires the
//! `LoadedTaskDefinition` and resolves per-run state (log dir,
//! resource map, persisted Python handles) before yielding the
//! pyclass instance to the caller. The actual `run()` loop lives in
//! the sibling [`run`] file.

use std::path::PathBuf;

use pyo3::prelude::*;

use dynrunner_core::{ResourceKind, ResourceMap};

use crate::config::distributed::DistributedConfig;
use crate::config::log_paths::LogPathConfig;
use crate::config::resources::PyResourceMap;
use crate::config::scheduler::SchedulerConfig;
use crate::config::worker_spec::WorkerSpec;
use crate::task_def::LoadedTaskDefinition;

use super::PySecondaryCoordinator;

#[pymethods]
impl PySecondaryCoordinator {
    #[new]
    #[pyo3(signature = (
        primary_url,
        secondary_id,
        num_workers,
        ram_bytes,
        source_dir,
        output_dir,
        task_definition,
        task_args,
        skip_existing = false,
        log_paths = None,
        worker_spec = None,
        distributed_config = None,
        src_network = None,
        src_tmp = None,
        max_resources = None,
        peer_lifecycle_listener = None,
        log_dir = None,
        scheduler_config = None,
    ))]
    // PyO3 kwargs surface — collapsing to a builder is a separate
    // API refactor.
    #[allow(clippy::too_many_arguments)]
    fn new(
        py: Python<'_>,
        primary_url: String,
        secondary_id: String,
        num_workers: u32,
        ram_bytes: u64,
        source_dir: String,
        output_dir: String,
        task_definition: &Bound<'_, PyAny>,
        task_args: &Bound<'_, PyAny>,
        skip_existing: bool,
        log_paths: Option<LogPathConfig>,
        worker_spec: Option<WorkerSpec>,
        distributed_config: Option<DistributedConfig>,
        src_network: Option<PathBuf>,
        src_tmp: Option<PathBuf>,
        max_resources: Option<PyResourceMap>,
        peer_lifecycle_listener: Option<Py<PyAny>>,
        log_dir: Option<String>,
        scheduler_config: Option<SchedulerConfig>,
    ) -> PyResult<Self> {
        let task = LoadedTaskDefinition::from_python(
            py,
            task_definition,
            task_args,
            &source_dir,
            &output_dir,
            log_dir.as_deref(),
            skip_existing,
            log_paths,
        )?;

        // Resolve this secondary's per-run log directory under the
        // log-mount root, using `secondary_id` so two co-located
        // secondaries on the same shared mount get distinct
        // directories. `create_dir_all` errors surface as
        // construction-time failures — silently swallowing this with
        // `.ok()` produced 6h runs with zero worker log output when
        // the mount happened to be read-only or missing.
        let log_dir =
            task.log_paths
                .resolve_log_dir(py, &task.log_path, &secondary_id)?;
        std::fs::create_dir_all(&log_dir).map_err(|e| {
            pyo3::exceptions::PyOSError::new_err(format!(
                "failed to create log directory {log_dir:?}: {e}"
            ))
        })?;

        // Boundary normalization: typed `max_resources` ResourceMap wins
        // when supplied; otherwise fall back to a single-key memory map
        // built from the legacy scalar `ram_bytes`.
        let max_resources = max_resources.map(|m| m.to_rust()).unwrap_or_else(|| {
            ResourceMap::from([(ResourceKind::memory(), ram_bytes)])
        });

        Ok(Self {
            python_executable: task.python_executable,
            primary_url,
            secondary_id,
            num_workers,
            max_resources,
            source_dir: task.source_path,
            output_dir: task.output_path,
            log_dir,
            log_paths: task.log_paths,
            worker_spec,
            distributed_config: distributed_config.unwrap_or_default(),
            src_network,
            src_tmp,
            types: task.types,
            // `from_python` already extracted phase_deps off the
            // TaskDefinition's `get_phases()`; we keep it on the
            // wrapper for the setup-promote yield path. Legacy
            // (non-pre-staged) runs never inspect this field.
            phase_deps: task.phase_deps,
            skip_existing,
            estimator: task.estimator,
            // Bump the refcount and unbind to a `Py<PyAny>` so the
            // handle outlives the constructor's `Bound` lifetime. The
            // setup-promote yield re-binds each iteration under a
            // fresh `Python::attach` scope.
            task_definition_py: task_definition.clone().unbind(),
            task_args_py: task_args.clone().unbind(),
            peer_lifecycle_listener,
            scheduler_config: scheduler_config.unwrap_or_default(),
            completed: 0,
        })
    }
}
