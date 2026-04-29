use std::path::PathBuf;

use pyo3::prelude::*;
use pyo3::types::PyList;

use dynrunner_core::TaskInfo;
use dynrunner_manager_local::{LocalManager, LocalManagerConfig, ProcessingStats};

use crate::config::connection::ConnectionMode;
use crate::config::log_paths::LogPathConfig;
use crate::config::scheduler::SchedulerConfig;
use crate::config::worker_spec::WorkerSpec;
use crate::estimator::PyMemoryEstimatorBridge;
use crate::identifier::TokenizerIdentifier;
use crate::pytypes::{PyTaskInfo, PyFailedTask, PyProcessingStats, extract_binaries};
use crate::subprocess_factory::SubprocessWorkerFactory;
use crate::task_def::{LoadedTaskDefinition, extract_stage_timeouts};
use crate::transport::EitherManagerEnd;

/// The main Python-facing local manager class.
#[pyclass(name = "RustLocalManager")]
pub(crate) struct PyLocalManager {
    python_executable: PathBuf,
    num_workers: u32,
    max_memory: u64,
    low_memory_threshold: u64,
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
    worker_module: String,
    worker_cmd_args: Vec<String>,
    skip_existing: bool,
    estimator: PyMemoryEstimatorBridge,
    stage_timeouts: std::collections::HashMap<String, std::time::Duration>,
    connection_mode: ConnectionMode,
    manual_start_worker: bool,
    stats: Option<ProcessingStats>,
    failed_tasks: Vec<dynrunner_core::FailedTask<TokenizerIdentifier>>,
    oom_tasks: Vec<dynrunner_core::FailedTask<TokenizerIdentifier>>,
    task_payloads: Vec<(TaskInfo<TokenizerIdentifier>, Option<Vec<u8>>)>,
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
        let stage_timeouts = extract_stage_timeouts(task_definition)?;

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

        Ok(Self {
            python_executable: task.python_executable,
            num_workers,
            max_memory,
            low_memory_threshold: low_memory_threshold.unwrap_or(300 * 1024 * 1024),
            always_restart_worker,
            restart_predicate,
            retry_max_attempts,
            print_pid,
            source_dir: task.source_path,
            output_dir: task.output_path,
            log_dir: task.log_dir,
            log_paths: task.log_paths,
            worker_spec,
            scheduler_config: scheduler_config.unwrap_or_default(),
            phase_status_log_intervals_secs: phase_status_log_intervals_secs
                .unwrap_or_else(|| vec![60.0, 300.0, 600.0, 1800.0, 3600.0]),
            worker_module: task.worker_module,
            worker_cmd_args: task.worker_cmd_args,
            skip_existing,
            estimator: task.estimator,
            stage_timeouts,
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

        // Convert absolute paths to relative (matching Python's relative_to(source_dir))
        for binary in &mut rust_binaries {
            if let Ok(rel) = binary.path.strip_prefix(&self.source_dir) {
                binary.path = rel.to_path_buf();
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
            max_resources: dynrunner_core::ResourceMap::from([(dynrunner_core::ResourceKind::memory(), self.max_memory)]),
            always_restart_worker: self.always_restart_worker,
            restart_predicate,
            retry_max_attempts: self.retry_max_attempts,
            print_pid: self.print_pid,
            memuse_log_path,
            stage_timeouts: self.stage_timeouts.clone(),
            low_resource_thresholds: dynrunner_core::ResourceMap::from([(
                dynrunner_core::ResourceKind::memory(),
                self.low_memory_threshold,
            )]),
            resource_check_interval: std::time::Duration::from_millis(100),
            phase_status_log_intervals: self
                .phase_status_log_intervals_secs
                .iter()
                .map(|s| std::time::Duration::from_secs_f64(*s))
                .collect(),
        };

        let mut factory = SubprocessWorkerFactory {
            python_executable: self.python_executable.clone(),
            source_dir: self.source_dir.clone(),
            output_dir: self.output_dir.clone(),
            log_dir: self.log_dir.clone(),
            log_paths: self.log_paths.clone(),
            worker_module: self.worker_module.clone(),
            worker_cmd_args: self.worker_cmd_args.clone(),
            skip_existing: self.skip_existing,
            connection_mode: self.connection_mode.clone(),
            manual_start_worker: self.manual_start_worker,
            worker_spec: self.worker_spec.clone(),
            child_processes: Vec::new(),
        };

        // Phase 5B: re-acquire the GIL from the manager's LocalSet and
        // dispatch to the Python TaskDefinition's `on_phase_*` methods.
        // Each closure owns its own ref-bumped `Py<PyAny>` so the manager
        // owns the lifetime independent of `self`.
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
                // TODO(phases-5a): replace the empty `phase_deps` with the
                // dependency graph extracted from `task_definition.get_phases()`.
                // Phase 5A owns the topology walk; until it lands, a
                // single-phase run with no deps preserves pre-pool behaviour.
                let outcome = manager
                    .process_binaries(
                        rust_binaries,
                        std::collections::HashMap::new(),
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

            // Clean up child processes
            for child in &mut factory.child_processes {
                if let Some(mut c) = child.take() {
                    let _ = c.kill();
                    let _ = c.wait();
                }
            }
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

