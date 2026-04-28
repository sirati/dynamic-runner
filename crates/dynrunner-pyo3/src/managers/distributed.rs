use std::collections::HashMap;
use std::path::PathBuf;

use pyo3::prelude::*;
use pyo3::types::PyList;

use dynrunner_manager_distributed::{PrimaryConfig, PrimaryCoordinator, SecondaryConfig, SecondaryCoordinator};
use dynrunner_scheduler::ResourceStealingScheduler;
use dynrunner_transport_channel::{ChannelPrimaryTransportEnd, ChannelSecondaryTransportEnd};

use crate::config::connection::ConnectionMode;
use crate::config::distributed::DistributedConfig;
use crate::config::log_paths::LogPathConfig;
use crate::config::worker_spec::WorkerSpec;
use crate::estimator::PyMemoryEstimatorBridge;
use crate::pytypes::extract_binaries;
use crate::subprocess_factory::SubprocessWorkerFactory;
use crate::task_def::LoadedTaskDefinition;

#[pyclass(name = "RustDistributedManager")]
pub(crate) struct PyDistributedManager {
    python_executable: PathBuf,
    num_secondaries: u32,
    num_workers_per_secondary: u32,
    ram_per_secondary: u64,
    source_dir: PathBuf,
    output_dir: PathBuf,
    log_dir: PathBuf,
    log_paths: LogPathConfig,
    worker_spec: Option<WorkerSpec>,
    distributed_config: DistributedConfig,
    worker_module: String,
    worker_cmd_args: Vec<String>,
    skip_existing: bool,
    estimator_slope: f64,
    estimator_intercept: f64,
    completed: u32,
    failed: u32,
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

        Ok(Self {
            python_executable: task.python_executable,
            num_secondaries,
            num_workers_per_secondary,
            ram_per_secondary,
            source_dir: task.source_path,
            output_dir: task.output_path,
            log_dir: task.log_dir,
            log_paths: task.log_paths,
            worker_spec,
            distributed_config: distributed_config.unwrap_or_default(),
            worker_module: task.worker_module,
            worker_cmd_args: task.worker_cmd_args,
            skip_existing,
            estimator_slope: task.estimator.slope,
            estimator_intercept: task.estimator.intercept,
            completed: 0,
            failed: 0,
        })
    }

    /// Run the distributed processing pipeline.
    fn run(&mut self, py: Python<'_>, binaries: &Bound<'_, PyList>) -> PyResult<()> {
        let rust_binaries = extract_binaries(binaries)?;

        let num_secondaries = self.num_secondaries;
        let num_workers = self.num_workers_per_secondary;
        let ram = self.ram_per_secondary;
        let slope = self.estimator_slope;
        let intercept = self.estimator_intercept;
        let python_executable = self.python_executable.clone();
        let source_dir = self.source_dir.clone();
        let output_dir = self.output_dir.clone();
        let log_dir = self.log_dir.clone();
        let log_paths = self.log_paths.clone();
        let dist_keepalive = self.distributed_config.keepalive_interval();
        let dist_peer_timeout = self.distributed_config.peer_timeout();
        let dist_connect_timeout = self.distributed_config.connect_timeout();
        let dist_keepalive_miss_threshold =
            self.distributed_config.keepalive_miss_threshold();
        let worker_spec = self.worker_spec.clone();
        let worker_module = self.worker_module.clone();
        let worker_cmd_args = self.worker_cmd_args.clone();
        let skip_existing = self.skip_existing;

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

                for i in 0..num_secondaries {
                    let secondary_id = format!("sec-{i}");

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
                    let sec_log = log_dir.clone();
                    let sec_log_paths = log_paths.clone();
                    let sec_worker_module = worker_module.clone();
                    let sec_worker_args = worker_cmd_args.clone();

                    let handle = tokio::task::spawn_local(async move {
                        let transport = ChannelPrimaryTransportEnd {
                            tx: sec_to_pri_tx,
                            rx: pri_to_sec_rx,
                        };
                        let config = SecondaryConfig {
                            secondary_id,
                            num_workers,
                            max_resources: dynrunner_core::ResourceMap::from([(dynrunner_core::ResourceKind::memory(), ram)]),
                            hostname: "localhost".into(),
                            keepalive_interval: dist_keepalive,
                            src_network: None,
                            src_tmp: None,
                            peer_timeout: dist_peer_timeout,
                            keepalive_miss_threshold: dist_keepalive_miss_threshold,
                        };

                        let estimator = PyMemoryEstimatorBridge { slope, intercept };

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
                };

                let estimator = PyMemoryEstimatorBridge { slope, intercept };
                let mut primary = PrimaryCoordinator::new(
                    config,
                    transport,
                    ResourceStealingScheduler::memory(),
                    estimator,
                );

                let result = primary.run(rust_binaries).await;
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
