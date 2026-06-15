//! `PyDistributedManager` constructor + `handle()` factory. The
//! load-bearing `run()` loop is in the sibling [`run`] module.

use std::path::PathBuf;

use pyo3::prelude::*;

use dynrunner_core::{ResourceKind, ResourceMap};

use crate::config::distributed::DistributedConfig;
use crate::config::log_paths::LogPathConfig;
use crate::config::resources::PyResourceMap;
use crate::config::scheduler::SchedulerConfig;
use crate::config::worker_spec::WorkerSpec;
use crate::task_def::LoadedTaskDefinition;

use super::PyDistributedManager;

#[pymethods]
impl PyDistributedManager {
    #[new]
    #[pyo3(signature = (
        num_secondaries,
        num_workers_per_secondary,
        ram_per_secondary,
        source_dir,
        output_dir,
        task_definition,
        task_args,
        skip_existing = false,
        log_paths = None,
        worker_spec = None,
        distributed_config = None,
        max_resources_per_secondary = None,
        source_pre_staged_root = None,
        stage_via_setup_tasks = false,
        peer_lifecycle_listener = None,
        task_completed_listener = None,
        import_action = None,
        upload_action = None,
        unfulfillable_reinject_max_per_task = None,
        log_dir = None,
        scheduler_config = None,
        panik_watcher_paths = None,
        panik_watcher_poll_interval_secs = 10.0,
        memprofile_enabled = false,
        forwarded_argv = Vec::new(),
    ))]
    // PyO3 kwargs surface — collapsing to a builder is a separate
    // API refactor.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        py: Python<'_>,
        num_secondaries: u32,
        num_workers_per_secondary: u32,
        ram_per_secondary: u64,
        source_dir: String,
        output_dir: String,
        task_definition: &Bound<'_, PyAny>,
        task_args: &Bound<'_, PyAny>,
        skip_existing: bool,
        log_paths: Option<LogPathConfig>,
        worker_spec: Option<WorkerSpec>,
        distributed_config: Option<DistributedConfig>,
        max_resources_per_secondary: Option<PyResourceMap>,
        source_pre_staged_root: Option<PathBuf>,
        stage_via_setup_tasks: bool,
        peer_lifecycle_listener: Option<Py<PyAny>>,
        task_completed_listener: Option<Py<PyAny>>,
        import_action: Option<Py<PyAny>>,
        upload_action: Option<Py<PyAny>>,
        unfulfillable_reinject_max_per_task: Option<u32>,
        log_dir: Option<String>,
        scheduler_config: Option<SchedulerConfig>,
        panik_watcher_paths: Option<Vec<PathBuf>>,
        panik_watcher_poll_interval_secs: f64,
        memprofile_enabled: bool,
        forwarded_argv: Vec<String>,
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

        // Boundary normalization: typed `max_resources_per_secondary`
        // ResourceMap wins; fall back to a single-key memory map built
        // from the legacy scalar `ram_per_secondary` if no map given.
        let max_resources_per_secondary = max_resources_per_secondary
            .map(|m| m.to_rust())
            .unwrap_or_else(|| ResourceMap::from([(ResourceKind::memory(), ram_per_secondary)]));

        // Build the command-channel + reinject-cap bundle. Same
        // helper as `PyPrimaryCoordinator`; see
        // `crate::managers::control_plane` for the lifecycle.
        let control_plane = crate::managers::control_plane::PrimaryControlPlane::new(
            unfulfillable_reinject_max_per_task,
        );

        Ok(Self {
            python_executable: task.python_executable,
            num_secondaries,
            num_workers_per_secondary,
            max_resources_per_secondary,
            source_dir: task.source_path,
            output_dir: task.output_path,
            log_path: task.log_path,
            log_paths: task.log_paths,
            worker_spec,
            distributed_config: distributed_config.unwrap_or_default(),
            types: task.types,
            phase_deps: task.phase_deps,
            phase_may_be_empty: task.phase_may_be_empty,
            skip_existing,
            uses_file_based_items: task.uses_file_based_items,
            max_concurrent_per_type: task.max_concurrent_per_type,
            estimator: task.estimator,
            completed: 0,
            failed: 0,
            stranded: 0,
            source_pre_staged_root,
            stage_via_setup_tasks,
            task_definition: task_definition.clone().unbind(),
            task_args: task_args.clone().unbind(),
            peer_lifecycle_listener,
            task_completed_listener,
            import_action,
            upload_action,
            control_plane,
            scheduler_config: scheduler_config.unwrap_or_default(),
            panik_watcher_paths: panik_watcher_paths.unwrap_or_default(),
            panik_watcher_poll_interval_secs,
            memprofile_enabled,
            forwarded_argv,
        })
    }

    /// PrimaryHandle factory for the in-process distributed primary.
    /// Symmetric with `PyPrimaryCoordinator::handle` — each call
    /// returns a freshly-built handle (with its own in-handle tokio
    /// runtime); the underlying `command_tx` and reinject-cap cell
    /// are cloned so multiple Python control planes / threads can
    /// share one manager. Callable BEFORE `run()` so the Python
    /// caller can hand the handle off to its `on_run_start` hook
    /// before the blocking `run()` enters the detached runtime.
    fn handle(&self) -> PyResult<crate::managers::primary_handle::PyPrimaryHandle> {
        self.control_plane.to_handle()
    }
}
