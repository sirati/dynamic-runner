use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use pyo3::prelude::*;
use pyo3::types::PyList;

use dynrunner_core::{resolve_against_root, PhaseId, ResourceKind, ResourceMap, TaskInfo};
use dynrunner_manager_local::{LocalManager, LocalManagerConfig, ProcessingStats};

use crate::config::connection::ConnectionMode;
use crate::config::log_paths::LogPathConfig;
use crate::config::resources::PyResourceMap;
use crate::config::scheduler::SchedulerConfig;
use crate::config::worker_spec::WorkerSpec;
use crate::estimator::PyMemoryEstimatorBridge;
use crate::identifier::RunnerIdentifier;
use crate::network::gethostname;
use crate::pytypes::{PyTaskInfo, PyFailedTask, PyProcessingStats, extract_binaries};
use crate::subprocess_factory::SubprocessWorkerFactory;
use crate::task_def::{LoadedTaskDefinition, TypeRegistry};
use crate::transport::EitherManagerEnd;

/// The main Python-facing local manager class.
#[pyclass(name = "RustLocalManager")]
pub(crate) struct PyLocalManager {
    python_executable: PathBuf,
    num_workers: u32,
    max_resources: ResourceMap,
    low_resource_thresholds: ResourceMap,
    always_restart_worker: bool,
    restart_predicate: Option<Py<PyAny>>,
    retry_max_attempts: u32,
    print_pid: bool,
    source_dir: PathBuf,
    output_dir: PathBuf,
    log_dir: PathBuf,
    log_paths: LogPathConfig,
    worker_spec: Option<WorkerSpec>,
    scheduler_config: SchedulerConfig,
    phase_status_log_intervals_secs: Vec<f64>,
    /// Per-phase keepalive watchdog. The map key is the phase name as
    /// reported by `Task.set_phase(...)`; the value is the maximum
    /// silence (no keepalive / phase update) tolerated before the
    /// manager kills and restarts the worker. Empty disables the
    /// watchdog entirely (current default). Forwarded verbatim into
    /// `LocalManagerConfig.stage_timeouts`.
    stage_timeouts_secs: HashMap<String, f64>,
    /// Surfaces `LocalManagerConfig.log_oom_watcher` through the
    /// legacy pyclass so callers that bypass the typed
    /// `LocalManagerConfig` path still pick up the flag.
    log_oom_watcher: bool,
    types: TypeRegistry,
    phase_deps: HashMap<PhaseId, Vec<PhaseId>>,
    skip_existing: bool,
    estimator: PyMemoryEstimatorBridge,
    connection_mode: ConnectionMode,
    manual_start_worker: bool,
    stats: Option<ProcessingStats>,
    failed_tasks: Vec<dynrunner_core::FailedTask<RunnerIdentifier>>,
    oom_tasks: Vec<dynrunner_core::FailedTask<RunnerIdentifier>>,
    task_payloads: Vec<(TaskInfo<RunnerIdentifier>, Option<Vec<u8>>)>,
    /// Held for the per-phase lifecycle hooks that re-acquire the GIL
    /// and call back into Python from the manager's LocalSet (Phase 5B).
    /// `Py<PyAny>` is `Send + Sync + 'static` so it satisfies the
    /// `FnMut + Send + 'static` bounds on `process_binaries`'s closure
    /// arguments.
    task_definition: Py<PyAny>,
}

#[pymethods]
impl PyLocalManager {
    #[new]
    #[pyo3(signature = (
        num_workers,
        max_memory,
        source_dir,
        output_dir,
        task_definition,
        task_args,
        skip_existing = false,
        always_restart_worker = false,
        restart_predicate = None,
        retry_max_attempts = 1,
        print_pid = false,
        connection_mode = "socketpair",
        socket_dir = None,
        manual_start_worker = false,
        log_paths = None,
        worker_spec = None,
        low_memory_threshold = None,
        scheduler_config = None,
        phase_status_log_intervals_secs = None,
        stage_timeouts_secs = None,
        max_resources = None,
        low_resource_thresholds = None,
        log_oom_watcher = false,
    ))]
    fn new(
        py: Python<'_>,
        num_workers: u32,
        max_memory: u64,
        source_dir: String,
        output_dir: String,
        task_definition: &Bound<'_, PyAny>,
        task_args: &Bound<'_, PyAny>,
        skip_existing: bool,
        always_restart_worker: bool,
        restart_predicate: Option<Py<PyAny>>,
        retry_max_attempts: u32,
        print_pid: bool,
        connection_mode: &str,
        socket_dir: Option<String>,
        manual_start_worker: bool,
        log_paths: Option<LogPathConfig>,
        worker_spec: Option<WorkerSpec>,
        low_memory_threshold: Option<u64>,
        scheduler_config: Option<SchedulerConfig>,
        phase_status_log_intervals_secs: Option<Vec<f64>>,
        stage_timeouts_secs: Option<HashMap<String, f64>>,
        max_resources: Option<PyResourceMap>,
        low_resource_thresholds: Option<PyResourceMap>,
        log_oom_watcher: bool,
    ) -> PyResult<Self> {
        let task = LoadedTaskDefinition::from_python(
            py,
            task_definition,
            task_args,
            &source_dir,
            &output_dir,
            skip_existing,
            log_paths,
        )?;

        // Single-process mode synthesises a `secondary_id` from the
        // hostname (falling back to the literal `"local"`) and feeds
        // it through the same log-dir template the distributed and
        // SLURM modes use. The resulting directory is unique by
        // construction even on a shared mount with other runners,
        // so there is no special-case "single process" branch in the
        // log-path policy.
        let secondary_id = {
            let h = gethostname();
            if h.is_empty() || h == "unknown" {
                "local".to_string()
            } else {
                h
            }
        };
        let log_dir =
            task.log_paths
                .resolve_log_dir(py, &task.output_path, &secondary_id)?;
        std::fs::create_dir_all(&log_dir).map_err(|e| {
            pyo3::exceptions::PyOSError::new_err(format!(
                "failed to create log directory {log_dir:?}: {e}"
            ))
        })?;

        // Parse connection mode
        let conn_mode = match connection_mode {
            "socketpair" => ConnectionMode::Socketpair,
            "named" => {
                let dir = socket_dir
                    .map(PathBuf::from)
                    .ok_or_else(|| {
                        pyo3::exceptions::PyValueError::new_err(
                            "socket_dir is required when connection_mode is 'named'",
                        )
                    })?;
                std::fs::create_dir_all(&dir).ok();
                ConnectionMode::Named { socket_dir: dir }
            }
            other => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "unknown connection_mode: {other:?}, expected 'socketpair' or 'named'"
                )));
            }
        };

        // Normalize the resource budget at the boundary: callers may pass a
        // typed `max_resources` ResourceMap (preferred) or just the legacy
        // scalar `max_memory: u64` (single-key memory). When both are
        // given, the typed map wins. Same shape for `low_resource_thresholds`.
        let max_resources = max_resources.map(|m| m.to_rust()).unwrap_or_else(|| {
            ResourceMap::from([(ResourceKind::memory(), max_memory)])
        });
        let low_resource_thresholds = low_resource_thresholds
            .map(|m| m.to_rust())
            .unwrap_or_else(|| {
                ResourceMap::from([(
                    ResourceKind::memory(),
                    low_memory_threshold.unwrap_or(300 * 1024 * 1024),
                )])
            });

        Ok(Self {
            python_executable: task.python_executable,
            num_workers,
            max_resources,
            low_resource_thresholds,
            always_restart_worker,
            restart_predicate,
            retry_max_attempts,
            print_pid,
            source_dir: task.source_path,
            output_dir: task.output_path,
            log_dir,
            log_paths: task.log_paths,
            worker_spec,
            scheduler_config: scheduler_config.unwrap_or_default(),
            phase_status_log_intervals_secs: phase_status_log_intervals_secs
                .unwrap_or_else(|| vec![60.0, 300.0, 600.0, 1800.0, 3600.0]),
            stage_timeouts_secs: stage_timeouts_secs.unwrap_or_default(),
            log_oom_watcher,
            types: task.types,
            phase_deps: task.phase_deps,
            skip_existing,
            estimator: task.estimator,
            connection_mode: conn_mode,
            manual_start_worker,
            stats: None,
            failed_tasks: Vec::new(),
            oom_tasks: Vec::new(),
            task_payloads: Vec::new(),
            task_definition: task_definition.clone().unbind(),
        })
    }

    /// Process a list of PyTaskInfo objects.
    fn process_binaries(&mut self, py: Python<'_>, binaries: &Bound<'_, PyList>) -> PyResult<()> {
        let mut rust_binaries = extract_binaries(binaries)?;

        // Normalise each `binary.path` to the worker-facing wire id
        // (relative-to-`source_dir`). Out-of-tree paths are left
        // verbatim — the worker opens them via
        // `Path(source_dir).join(<abs>)`, which discards the source
        // prefix and reaches the absolute target.
        for binary in &mut rust_binaries {
            let resolved = resolve_against_root(&binary.path, &self.source_dir);
            if let Some(rel) = resolved.relative {
                binary.path = rel;
            }
        }

        let estimator = self.estimator.clone();
        let scheduler = self.scheduler_config.build_memory_scheduler();

        let memuse_log_path = Some(self.output_dir.join("memuse.log"));

        let restart_predicate = self.restart_predicate.as_ref().map(|cb| {
            let cb = cb.clone_ref(py);
            let predicate: dynrunner_manager_local::RestartPredicate =
                Box::new(move |ctx: &dynrunner_manager_local::RestartContext<'_>| -> bool {
                    crate::managers::factory_callback::invoke_restart_predicate(&cb, ctx)
                });
            predicate
        });

        let config = LocalManagerConfig {
            num_workers: self.num_workers,
            max_resources: self.max_resources.clone(),
            always_restart_worker: self.always_restart_worker,
            restart_predicate,
            retry_max_attempts: self.retry_max_attempts,
            print_pid: self.print_pid,
            memuse_log_path,
            // Phase keys here are the raw strings the worker reports via
            // `Task.set_phase(...)` — the watchdog matches on those, not
            // on `PhaseId` from `get_phases()`. Per-type
            // `TaskTypeSpec.timeout_seconds` is a separate forward-looking
            // field that requires worker→type tracking to enforce; until
            // that follow-up lands, callers wanting timeout enforcement
            // pass `stage_timeouts_secs` on `LocalManagerConfig`.
            stage_timeouts: self
                .stage_timeouts_secs
                .iter()
                .map(|(k, v)| (k.clone(), Duration::from_secs_f64(*v)))
                .collect(),
            low_resource_thresholds: self.low_resource_thresholds.clone(),
            resource_check_interval: std::time::Duration::from_millis(100),
            phase_status_log_intervals: self
                .phase_status_log_intervals_secs
                .iter()
                .map(|s| std::time::Duration::from_secs_f64(*s))
                .collect(),
            log_oom_watcher: self.log_oom_watcher,
        };

        // TODO(phase-5a-followup): worker subprocesses currently use the
        // first type's worker_module + cmd_args; restart-on-type-shift
        // (so each worker is bound to a single TypeId for its lifetime)
        // is not yet implemented. The subprocess factory should grow a
        // `spawn_worker(worker_id, type_id)` overload that consults the
        // full `TypeRegistry` instead of a single string.
        let first_type = self.types.first().ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(
                "task_definition.get_phases() yielded zero TaskTypeSpec entries",
            )
        })?;
        let mut factory = SubprocessWorkerFactory {
            python_executable: self.python_executable.clone(),
            source_dir: self.source_dir.clone(),
            output_dir: self.output_dir.clone(),
            log_dir: self.log_dir.clone(),
            log_paths: self.log_paths.clone(),
            worker_module: first_type.worker_module.clone(),
            worker_cmd_args: first_type.cmd_args.clone(),
            skip_existing: self.skip_existing,
            connection_mode: self.connection_mode.clone(),
            manual_start_worker: self.manual_start_worker,
            worker_spec: self.worker_spec.clone(),
            child_processes: Vec::new(),
        };

        let phase_deps = self.phase_deps.clone();
        // GIL-reacquiring closures that dispatch to the Python
        // TaskDefinition's on_phase_start / on_phase_end. Each closure
        // owns its own ref-bumped Py<PyAny> so the manager's lifetime
        // is independent of `self`.
        let on_phase_start =
            crate::managers::lifecycle::make_on_phase_start(self.task_definition.clone_ref(py));
        let on_phase_end =
            crate::managers::lifecycle::make_on_phase_end(self.task_definition.clone_ref(py));

        // Run the async manager on a current-thread tokio runtime,
        // releasing the GIL during processing.
        let run_result: Result<(), String> = py.detach(|| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to create tokio runtime");

            let local = tokio::task::LocalSet::new();
            let result = rt.block_on(local.run_until(async {
                let mut manager: LocalManager<EitherManagerEnd, _, _, _> =
                    LocalManager::new(config, scheduler, estimator);
                // phase_deps comes from LoadedTaskDefinition (5A);
                // on_phase_* closures bridge to Python (5B).
                let outcome = manager
                    .process_binaries(
                        rust_binaries,
                        phase_deps,
                        on_phase_start,
                        on_phase_end,
                        &mut factory,
                    )
                    .await;

                self.stats = Some(manager.stats().clone());
                self.failed_tasks = manager.failed_tasks().to_vec();
                self.oom_tasks = manager.resource_pressure_tasks().to_vec();
                self.task_payloads = manager.task_payloads().to_vec();
                outcome
            }));

            // Tear down tracked worker subprocesses via the shared
            // SIGTERM → grace → SIGKILL primitive. SIGKILL with no
            // grace orphans podman-launched workers because podman
            // traps SIGTERM to remove the container; SIGKILL skips
            // that path. Per project rule "no special-casing", the
            // teardown policy is uniform across worker kinds.
            factory.cleanup_all();
            result
        });

        run_result.map_err(pyo3::exceptions::PyRuntimeError::new_err)
    }

    #[getter]
    fn stats(&self) -> PyResult<PyProcessingStats> {
        let s = self.stats.as_ref().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("process_binaries not yet called")
        })?;
        Ok(PyProcessingStats {
            completed: s.completed,
            total: s.total,
            errored: s.errored,
            skipped: s.skipped,
        })
    }

    #[getter]
    fn failed_tasks(&self) -> Vec<PyFailedTask> {
        self.failed_tasks
            .iter()
            .map(|t| PyFailedTask {
                binary: PyTaskInfo::from(&t.binary),
                error_type: format!("{:?}", t.error_type),
                error_message: t.error_message.clone(),
            })
            .collect()
    }

    #[getter]
    fn task_results(&self) -> Vec<(PyTaskInfo, Option<Vec<u8>>)> {
        self.task_payloads
            .iter()
            .map(|(bi, data)| (PyTaskInfo::from(bi), data.clone()))
            .collect()
    }

    #[getter]
    fn oom_tasks(&self) -> Vec<PyFailedTask> {
        self.oom_tasks
            .iter()
            .map(|t| PyFailedTask {
                binary: PyTaskInfo::from(&t.binary),
                error_type: format!("{:?}", t.error_type),
                error_message: t.error_message.clone(),
            })
            .collect()
    }
}

