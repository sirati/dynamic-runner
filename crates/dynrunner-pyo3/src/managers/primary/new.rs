//! `PyPrimaryCoordinator` constructor + small pymethods: `handle()`,
//! `uses_file_based_items` getter, `queue_initial_staging`. The
//! load-bearing `run()` loop is in the sibling [`run`] module.

use pyo3::prelude::*;

use dynrunner_manager_distributed::{StagingError, compute_initial_staging_entries};

use crate::config::distributed::DistributedConfig;
use crate::config::scheduler::SchedulerConfig;
use crate::task_def::LoadedTopology;

use super::PyPrimaryCoordinator;

#[pymethods]
impl PyPrimaryCoordinator {
    #[new]
    #[pyo3(signature = (
        num_secondaries,
        task_definition,
        spawn_secondary,
        distributed_config = None,
        listen_port = None,
        source_pre_staged_root = None,
        source_dir = None,
        stage_via_setup_tasks = false,
        unfulfillable_reinject_max_per_task = None,
        peer_lifecycle_listener = None,
        fulfillability_matcher = None,
        respawn_policy = None,
        respawn_spawner = None,
        task_completed_listener = None,
        scheduler_config = None,
        panik_watcher_paths = None,
        panik_watcher_poll_interval_secs = 10.0,
        forwarded_argv = Vec::new(),
        peer_credentials_path = None,
    ))]
    // PyO3 kwargs surface — collapsing to a builder is a separate
    // API refactor.
    #[allow(clippy::too_many_arguments)]
    fn new(
        py: Python<'_>,
        num_secondaries: u32,
        task_definition: &Bound<'_, PyAny>,
        spawn_secondary: Py<PyAny>,
        distributed_config: Option<DistributedConfig>,
        listen_port: Option<u16>,
        source_pre_staged_root: Option<std::path::PathBuf>,
        source_dir: Option<std::path::PathBuf>,
        stage_via_setup_tasks: bool,
        unfulfillable_reinject_max_per_task: Option<u32>,
        peer_lifecycle_listener: Option<Py<PyAny>>,
        fulfillability_matcher: Option<Py<PyAny>>,
        respawn_policy: Option<crate::config::respawn::PyRespawnPolicy>,
        respawn_spawner: Option<Py<PyAny>>,
        task_completed_listener: Option<Py<PyAny>>,
        scheduler_config: Option<SchedulerConfig>,
        panik_watcher_paths: Option<Vec<std::path::PathBuf>>,
        panik_watcher_poll_interval_secs: f64,
        forwarded_argv: Vec<String>,
        peer_credentials_path: Option<std::path::PathBuf>,
    ) -> PyResult<Self> {
        let topology = LoadedTopology::from_python(task_definition)?;
        let uses_file_based_items: bool = task_definition
            .getattr("uses_file_based_items")
            .ok()
            .and_then(|v| v.extract().ok())
            .unwrap_or(true);
        // Build the command-channel + reinject-cap bundle. The helper
        // owns the channel pair, seeds the cap cell from the kwarg,
        // and (later) hands back the handle factory + run-start
        // wiring through a single API. See
        // `crate::managers::control_plane` for the lifecycle.
        let control_plane = crate::managers::control_plane::PrimaryControlPlane::new(
            unfulfillable_reinject_max_per_task,
        );
        Ok(Self {
            num_secondaries,
            estimator: topology.estimator,
            phase_deps: topology.phase_deps,
            phase_may_be_empty: topology.phase_may_be_empty,
            spawn_secondary: spawn_secondary.clone_ref(py),
            distributed_config: distributed_config.unwrap_or_default(),
            listen_port,
            completed: 0,
            failed: 0,
            stranded: 0,
            pending_stage_files: Vec::new(),
            source_pre_staged_root,
            source_dir,
            stage_via_setup_tasks,
            uses_file_based_items,
            max_concurrent_per_type: topology.max_concurrent_per_type,
            task_definition: task_definition.clone().unbind(),
            control_plane,
            peer_lifecycle_listener,
            fulfillability_matcher,
            slurm_job_manager: None,
            tunnel_reconnector: None,
            job_ledger_probe: None,
            respawn_policy: respawn_policy
                .unwrap_or_else(crate::config::respawn::PyRespawnPolicy::rust_disabled),
            respawn_spawner,
            task_completed_listener,
            scheduler_config: scheduler_config.unwrap_or_default(),
            panik_watcher_paths: panik_watcher_paths.unwrap_or_default(),
            panik_watcher_poll_interval_secs,
            forwarded_argv,
            peer_credentials_path,
        })
    }

    /// PrimaryHandle factory. Each call returns a freshly-built
    /// handle (with its own in-handle tokio runtime); the underlying
    /// `command_tx` and reinject-cap cell are cloned so multiple
    /// Python control planes / threads can share one coordinator.
    /// Callable BEFORE `run()` so the Python caller can hand the
    /// handle off to its async executor / thread BEFORE the
    /// blocking `run()` starts.
    fn handle(&self) -> PyResult<crate::managers::primary_handle::PyPrimaryHandle> {
        self.control_plane.to_handle()
    }

    /// Whether items are file-backed (read at construction from
    /// `TaskDefinition.uses_file_based_items`; defaults to True).
    /// Pipeline.py reads this to decide whether to call
    /// `queue_initial_staging` — when False, no primary-side staging
    /// happens at all.
    #[getter]
    fn uses_file_based_items(&self) -> bool {
        self.uses_file_based_items
    }

    /// Bulk-queue StageFile notifications for every binary in
    /// `binaries`, broadcast to all `num_secondaries` configured on
    /// this coordinator.
    ///
    /// PyO3 layer is intentionally a thin extract-and-delegate
    /// shell: the staging walk (path resolution, content hashing,
    /// per-secondary fan-out, error classification) lives in
    /// `dynrunner_manager_distributed::compute_initial_staging_entries`
    /// so the in-process distributed pipeline (which constructs its
    /// `PrimaryCoordinator` directly, never crossing this PyO3
    /// boundary) shares the same code. This wrapper does
    /// PyList → `Vec<TaskInfo>`, delegates, and maps the typed
    /// `StagingError` variants to the consumer-facing Python
    /// exceptions.
    fn queue_initial_staging(
        &mut self,
        binaries: &Bound<'_, pyo3::types::PyList>,
        source_root: String,
    ) -> PyResult<()> {
        // Staging-entry computation is independent of the discovery
        // already-done marker (every discovered item still names a source
        // file to stage); strip the bit at this call site.
        let rust_binaries: Vec<_> = crate::pytypes::extract_binaries(binaries)?
            .into_iter()
            .map(|(task, _skipped)| task)
            .collect();
        let source_root = std::path::PathBuf::from(source_root);
        // Secondary IDs the SLURM/network primary spawns under;
        // mirrors the format used in `run` below (line ~225) and in
        // `connect.rs`'s missing-secondary diagnostic.
        let secondary_ids: Vec<String> = (0..self.num_secondaries)
            .map(|i| format!("secondary-{i}"))
            .collect();
        let entries = compute_initial_staging_entries(&rust_binaries, &secondary_ids, &source_root)
            .map_err(|e| match e {
                StagingError::SourceUnreadable { .. } => {
                    pyo3::exceptions::PyFileNotFoundError::new_err(e.to_string())
                }
            })?;
        self.pending_stage_files.extend(entries);
        Ok(())
    }
}
