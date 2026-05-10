use std::collections::HashMap;
use std::path::PathBuf;

use pyo3::prelude::*;
use pyo3::types::PyList;

use dynrunner_core::{PhaseId, ResourceKind, ResourceMap, TypeId};
use dynrunner_manager_distributed::{PrimaryConfig, PrimaryCoordinator, SecondaryConfig, SecondaryCoordinator};
use dynrunner_scheduler::ResourceStealingScheduler;
use dynrunner_transport_channel::{ChannelPrimaryTransportEnd, ChannelSecondaryTransportEnd};

use crate::config::connection::ConnectionMode;
use crate::config::distributed::DistributedConfig;
use crate::config::log_paths::LogPathConfig;
use crate::config::resources::PyResourceMap;
use crate::config::worker_spec::WorkerSpec;
use crate::estimator::PyMemoryEstimatorBridge;
use crate::pytypes::extract_binaries;
use crate::subprocess_factory::SubprocessWorkerFactory;
use crate::task_def::{LoadedTaskDefinition, TypeRegistry};

#[pyclass(name = "RustDistributedManager")]
pub(crate) struct PyDistributedManager {
    python_executable: PathBuf,
    num_secondaries: u32,
    num_workers_per_secondary: u32,
    max_resources_per_secondary: ResourceMap,
    source_dir: PathBuf,
    output_dir: PathBuf,
    log_paths: LogPathConfig,
    worker_spec: Option<WorkerSpec>,
    distributed_config: DistributedConfig,
    types: TypeRegistry,
    phase_deps: HashMap<PhaseId, Vec<PhaseId>>,
    skip_existing: bool,
    uses_file_based_items: bool,
    max_concurrent_per_type: HashMap<TypeId, u32>,
    estimator: PyMemoryEstimatorBridge,
    completed: u32,
    failed: u32,
    /// Held for the per-phase lifecycle hooks that re-acquire the GIL
    /// from inside `PrimaryCoordinator::run` (Phase 5B). The
    /// distributed in-process pipeline drives a primary; secondaries
    /// don't fire user-visible phase hooks.
    task_definition: Py<PyAny>,
}

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
    ))]
    fn new(
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

        // Boundary normalization: typed `max_resources_per_secondary`
        // ResourceMap wins; fall back to a single-key memory map built
        // from the legacy scalar `ram_per_secondary` if no map given.
        let max_resources_per_secondary = max_resources_per_secondary
            .map(|m| m.to_rust())
            .unwrap_or_else(|| {
                ResourceMap::from([(ResourceKind::memory(), ram_per_secondary)])
            });

        Ok(Self {
            python_executable: task.python_executable,
            num_secondaries,
            num_workers_per_secondary,
            max_resources_per_secondary,
            source_dir: task.source_path,
            output_dir: task.output_path,
            log_paths: task.log_paths,
            worker_spec,
            distributed_config: distributed_config.unwrap_or_default(),
            types: task.types,
            phase_deps: task.phase_deps,
            skip_existing,
            uses_file_based_items: task.uses_file_based_items,
            max_concurrent_per_type: task.max_concurrent_per_type,
            estimator: task.estimator,
            completed: 0,
            failed: 0,
            task_definition: task_definition.clone().unbind(),
        })
    }

    /// Run the distributed processing pipeline.
    fn run(&mut self, py: Python<'_>, binaries: &Bound<'_, PyList>) -> PyResult<()> {
        let rust_binaries = extract_binaries(binaries)?;

        let num_secondaries = self.num_secondaries;
        let num_workers = self.num_workers_per_secondary;
        let max_resources_per_secondary = self.max_resources_per_secondary.clone();
        let estimator = self.estimator.clone();
        let python_executable = self.python_executable.clone();
        let source_dir = self.source_dir.clone();
        let output_dir = self.output_dir.clone();
        let log_paths = self.log_paths.clone();

        // Pre-compute per-secondary log directories under the GIL —
        // `resolve_log_dir` calls into Python's `datetime` module —
        // before detaching for the tokio runtime. Each secondary gets
        // its own `{timestamp}/{secondary_id}` subdirectory so the
        // default `worker_<id>.log` filename never collides across
        // secondaries on a shared mount, and `create_dir_all` errors
        // surface here at run start rather than as silent log loss.
        let mut sec_log_dirs: Vec<(String, PathBuf)> =
            Vec::with_capacity(num_secondaries as usize);
        for i in 0..num_secondaries {
            let sid = format!("sec-{i}");
            let dir = log_paths.resolve_log_dir(py, &output_dir, &sid)?;
            std::fs::create_dir_all(&dir).map_err(|e| {
                pyo3::exceptions::PyOSError::new_err(format!(
                    "failed to create log directory {dir:?} for {sid}: {e}"
                ))
            })?;
            sec_log_dirs.push((sid, dir));
        }
        let dist_keepalive = self.distributed_config.keepalive_interval();
        let dist_peer_timeout = self.distributed_config.peer_timeout();
        let dist_connect_timeout = self.distributed_config.connect_timeout();
        let dist_keepalive_miss_threshold =
            self.distributed_config.keepalive_miss_threshold();
        let dist_retry_max_passes = self.distributed_config.retry_max_passes();
        let dist_mass_death_grace = self.distributed_config.mass_death_grace();
        let dist_mass_death_min_count = self.distributed_config.mass_death_min_count();
        let worker_spec = self.worker_spec.clone();
        // TODO(phase-5a-followup): worker subprocesses currently use the
        // first type's worker_module + cmd_args; restart-on-type-shift
        // is not yet implemented. The factory will need a per-type
        // dispatch path that consults the full TypeRegistry.
        let first_type = self.types.first().ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(
                "task_definition.get_phases() yielded zero TaskTypeSpec entries",
            )
        })?;
        let worker_module = first_type.worker_module.clone();
        let worker_cmd_args = first_type.cmd_args.clone();
        let skip_existing = self.skip_existing;
        let uses_file_based_items = self.uses_file_based_items;
        let max_concurrent_per_type = self.max_concurrent_per_type.clone();
        let phase_deps = self.phase_deps.clone();

        // Phase 5B: re-acquire the GIL from the coordinator's LocalSet
        // and dispatch to the Python TaskDefinition's `on_phase_*`
        // methods. Built before `py.detach` so the closures can capture
        // ref-bumped `Py<PyAny>` clones.
        let on_phase_start: Box<dyn FnMut(&dynrunner_core::PhaseId) + Send> = Box::new(
            crate::managers::lifecycle::make_on_phase_start(
                self.task_definition.clone_ref(py),
            ),
        );
        let on_phase_end: Box<dyn FnMut(&dynrunner_core::PhaseId, u32, u32) + Send> = Box::new(
            crate::managers::lifecycle::make_on_phase_end(
                self.task_definition.clone_ref(py),
            ),
        );

        let mut completed = 0u32;
        let mut failed = 0u32;

        py.detach(|| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to create tokio runtime");

            let local = tokio::task::LocalSet::new();
            rt.block_on(local.run_until(async {
                use tokio::sync::mpsc as tokio_mpsc;

                let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
                let mut outgoing = HashMap::new();
                let mut sec_handles = Vec::new();
                let mut all_child_processes: Vec<Option<std::process::Child>> = Vec::new();

                for (secondary_id, sec_log) in sec_log_dirs.into_iter() {
                    // primary→secondary channel
                    let (pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();
                    // secondary→primary channel
                    let (sec_to_pri_tx, sec_to_pri_rx) = tokio_mpsc::unbounded_channel();

                    outgoing.insert(secondary_id.clone(), pri_to_sec_tx);

                    // Forward secondary→primary messages
                    let fwd_tx = incoming_tx.clone();
                    tokio::task::spawn_local(async move {
                        let mut rx = sec_to_pri_rx;
                        while let Some(msg) = rx.recv().await {
                            if fwd_tx.send(msg).is_err() {
                                break;
                            }
                        }
                    });

                    let sec_python = python_executable.clone();
                    let sec_worker_spec = worker_spec.clone();
                    let sec_source = source_dir.clone();
                    let sec_output = output_dir.clone();
                    let sec_log_paths = log_paths.clone();
                    let sec_worker_module = worker_module.clone();
                    let sec_worker_args = worker_cmd_args.clone();
                    let sec_estimator = estimator.clone();
                    let sec_max_resources = max_resources_per_secondary.clone();

                    let handle = tokio::task::spawn_local(async move {
                        let transport = ChannelPrimaryTransportEnd {
                            tx: sec_to_pri_tx,
                            rx: pri_to_sec_rx,
                        };
                        let config = SecondaryConfig {
                            secondary_id,
                            num_workers,
                            max_resources: sec_max_resources,
                            hostname: "localhost".into(),
                            keepalive_interval: dist_keepalive,
                            // In-process mode: primary and
                            // secondaries share filesystem
                            // visibility, so the staging walk's
                            // relative `src_path` (e.g.
                            // `input-0.txt`, derived from
                            // `binary.path` post-strip-prefix)
                            // resolves under the primary's
                            // `source_dir`. Without this set,
                            // `stage_and_register`'s `stage_file`
                            // call rejects every relative
                            // src_path with "no src_network is
                            // configured" and the next
                            // TaskAssignment surfaces as the
                            // legacy "expected StageFile
                            // notification first" failure even
                            // though staging WAS queued — pairs
                            // with the staging-walk fix above:
                            // both are needed for the in-process
                            // pipeline to actually process file-
                            // backed items.
                            src_network: Some(sec_source.clone()),
                            src_tmp: None,
                            peer_timeout: dist_peer_timeout,
                            keepalive_miss_threshold: dist_keepalive_miss_threshold,
                            retry_max_passes: dist_retry_max_passes,
                        };

                        let estimator = sec_estimator;

                        let mut factory = SubprocessWorkerFactory {
                            python_executable: sec_python,
                            source_dir: sec_source,
                            output_dir: sec_output,
                            log_dir: sec_log,
                            log_paths: sec_log_paths,
                            worker_module: sec_worker_module,
                            worker_cmd_args: sec_worker_args,
                            skip_existing,
                            connection_mode: ConnectionMode::Socketpair,
                            manual_start_worker: false,
                            worker_spec: sec_worker_spec.clone(),
                            child_processes: Vec::new(),
                        };

                        let mut secondary = SecondaryCoordinator::new(
                            config,
                            transport,
                            dynrunner_transport_quic::NoPeerTransport,
                            ResourceStealingScheduler::memory(),
                            estimator,
                        );
                        let result = secondary.run(&mut factory).await;
                        if let Err(e) = &result {
                            tracing::error!(error = %e, "secondary failed");
                        }

                        // Collect child processes for cleanup
                        let children: Vec<Option<std::process::Child>> =
                            factory.child_processes.drain(..).collect();

                        (secondary.completed_count(), children)
                    });

                    sec_handles.push(handle);
                }
                drop(incoming_tx); // Only forwarding tasks hold senders now

                let transport = ChannelSecondaryTransportEnd { outgoing, incoming_rx };
                let config = PrimaryConfig {
                    node_id: "primary".into(),
                    num_secondaries,
                    connect_timeout: dist_connect_timeout,
                    peer_timeout: dist_peer_timeout,
                    keepalive_interval: dist_keepalive,
                    keepalive_miss_threshold: dist_keepalive_miss_threshold,
                    // In-process distributed manager doesn't run the
                    // SLURM packaging pipeline, so pre-staged mode
                    // doesn't apply here.
                    source_pre_staged_root: None,
                    uses_file_based_items,
                    max_concurrent_per_type: max_concurrent_per_type.clone(),
                    retry_max_passes: dist_retry_max_passes,
                    fleet_dead_timeout: std::time::Duration::from_secs(30),
                    mesh_ready_timeout: std::time::Duration::from_secs(60),
                    mass_death_grace: dist_mass_death_grace,
                    mass_death_min_count: dist_mass_death_min_count,
                    // Threaded into PrimaryConfig so the manager's
                    // run() has the local source root needed for the
                    // initial staging walk's content-hash + per-
                    // secondary fan-out. The explicit
                    // `queue_initial_staging_from_binaries` call
                    // below pre-populates the queue today; threading
                    // the field uniformly keeps the manager's
                    // future-direction (auto-stage when no caller
                    // pre-queues) wired without each caller re-
                    // implementing the orchestration.
                    source_dir: Some(source_dir.clone()),
                };

                let mut primary = PrimaryCoordinator::new(
                    config,
                    transport,
                    ResourceStealingScheduler::memory(),
                    estimator,
                );

                // Initial staging is now driven by
                // `PrimaryCoordinator::run` itself: with
                // `PrimaryConfig.source_dir = Some(source_dir)`
                // threaded above, the manager's auto-stage gate
                // (`pending_stage_files.is_empty()` &&
                // `uses_file_based_items` && pre-staged-mode off
                // && source_dir is Some) walks `binaries ×
                // secondary_ids` once secondaries have welcomed
                // and queues the entries before initial
                // assignment. Removes the previous explicit pre-
                // call here in favour of a single source of truth
                // at the manager boundary; consistent with the
                // network-primary path, which also relies on the
                // auto-stage. The SLURM pipeline retains its
                // explicit `queue_initial_staging` because that
                // caller's source-root resolution depends on
                // `--source-already-staged` and other flags
                // unique to it; the gate detects the non-empty
                // queue and skips.

                // phase_deps + lifecycle closures captured from the
                // outer scope (5A built phase_deps; 5B built the
                // GIL-reacquiring on_phase_* closures).
                let result = primary
                    .run(rust_binaries, phase_deps, on_phase_start, on_phase_end)
                    .await;
                if let Err(e) = &result {
                    tracing::error!(error = %e, "primary failed");
                }

                completed = primary.completed_count() as u32;
                failed = primary.failed_count() as u32;

                // Drop primary to close channels, allowing secondaries to exit
                drop(primary);

                // Wait for secondaries and clean up child processes
                for handle in sec_handles {
                    if let Ok((_, children)) = handle.await {
                        all_child_processes.extend(children);
                    }
                }

                // Clean up all child processes
                for child in &mut all_child_processes {
                    if let Some(mut c) = child.take() {
                        let _ = c.kill();
                        let _ = c.wait();
                    }
                }
            }));
        });

        self.completed = completed;
        self.failed = failed;

        Ok(())
    }

    #[getter]
    fn completed(&self) -> u32 {
        self.completed
    }

    #[getter]
    fn failed(&self) -> u32 {
        self.failed
    }
}

// ── Network-based primary coordinator (spawns real secondary processes) ──
