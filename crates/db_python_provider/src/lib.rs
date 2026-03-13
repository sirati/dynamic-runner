use std::os::fd::FromRawFd;
use std::path::PathBuf;

use pyo3::prelude::*;
use pyo3::types::PyList;

use serde::{Deserialize, Serialize};

use db_comm_api_base::{BinaryInfo, MemoryBytes, WorkerId};

/// The concrete identifier type for the tokenizer task.
///
/// This is the task-specific struct that was previously hardcoded as
/// `BinaryIdentifier` in `db_comm_api_base`. Different task definitions
/// can define their own identifier types implementing the `Identifier`
/// trait (Clone + Debug + Hash + Eq + Serialize + Deserialize + Send + 'static).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TokenizerIdentifier {
    pub binary_name: String,
    pub platform: String,
    pub compiler: String,
    pub version: String,
    pub opt_level: String,
}
use db_local_manager::{LocalManager, LocalManagerConfig, ProcessingStats, WorkerFactory};
use db_scheduler_api::MemoryEstimator;
use db_scheduler_impl::MemoryStealingScheduler;
use db_transport_socket::socketpair::{SocketpairManagerEnd, create_socketpair};

/// Python-visible wrapper for BinaryIdentifier.
#[pyclass(name = "BinaryIdentifier", from_py_object)]
#[derive(Clone)]
struct PyBinaryIdentifier {
    #[pyo3(get)]
    binary_name: String,
    #[pyo3(get)]
    platform: String,
    #[pyo3(get)]
    compiler: String,
    #[pyo3(get)]
    version: String,
    #[pyo3(get)]
    opt_level: String,
}

#[pymethods]
impl PyBinaryIdentifier {
    #[new]
    fn new(
        binary_name: String,
        platform: String,
        compiler: String,
        version: String,
        opt_level: String,
    ) -> Self {
        Self {
            binary_name,
            platform,
            compiler,
            version,
            opt_level,
        }
    }
}

impl From<&PyBinaryIdentifier> for TokenizerIdentifier {
    fn from(py: &PyBinaryIdentifier) -> Self {
        TokenizerIdentifier {
            binary_name: py.binary_name.clone(),
            platform: py.platform.clone(),
            compiler: py.compiler.clone(),
            version: py.version.clone(),
            opt_level: py.opt_level.clone(),
        }
    }
}

/// Python-visible wrapper for BinaryInfo.
#[pyclass(name = "BinaryInfo", from_py_object)]
#[derive(Clone)]
struct PyBinaryInfo {
    #[pyo3(get)]
    path: String,
    #[pyo3(get)]
    size: u64,
    #[pyo3(get)]
    identifier: PyBinaryIdentifier,
}

#[pymethods]
impl PyBinaryInfo {
    #[new]
    fn new(path: String, size: u64, identifier: PyBinaryIdentifier) -> Self {
        Self {
            path,
            size,
            identifier,
        }
    }
}

impl From<&PyBinaryInfo> for BinaryInfo<TokenizerIdentifier> {
    fn from(py: &PyBinaryInfo) -> Self {
        BinaryInfo {
            path: PathBuf::from(&py.path),
            size: py.size,
            identifier: TokenizerIdentifier::from(&py.identifier),
        }
    }
}

impl From<&BinaryInfo<TokenizerIdentifier>> for PyBinaryInfo {
    fn from(bi: &BinaryInfo<TokenizerIdentifier>) -> Self {
        PyBinaryInfo {
            path: bi.path.to_string_lossy().into_owned(),
            size: bi.size,
            identifier: PyBinaryIdentifier {
                binary_name: bi.identifier.binary_name.clone(),
                platform: bi.identifier.platform.clone(),
                compiler: bi.identifier.compiler.clone(),
                version: bi.identifier.version.clone(),
                opt_level: bi.identifier.opt_level.clone(),
            },
        }
    }
}

/// Python-visible processing stats.
#[pyclass(name = "ProcessingStats")]
struct PyProcessingStats {
    #[pyo3(get)]
    completed: u32,
    #[pyo3(get)]
    total: u32,
    #[pyo3(get)]
    errored: u32,
    #[pyo3(get)]
    skipped: u32,
}

/// Python-visible failed task.
#[pyclass(name = "FailedTask")]
struct PyFailedTask {
    #[pyo3(get)]
    binary: PyBinaryInfo,
    #[pyo3(get)]
    error_type: String,
    #[pyo3(get)]
    error_message: String,
}

/// Memory estimator that calls a Python function.
#[derive(Clone)]
struct PyMemoryEstimatorBridge {
    /// Cached linear coefficient: memory = slope * binary_size + intercept.
    /// For the common case where estimate_memory is linear, we precompute.
    /// If not linear, we store a callable.
    slope: f64,
    intercept: f64,
}

impl PyMemoryEstimatorBridge {
    fn from_python(_py: Python<'_>, estimate_fn: &Bound<'_, PyAny>) -> PyResult<Self> {
        // Probe the function with two sizes to determine if it's linear.
        let size_a: u64 = 1_000_000;
        let size_b: u64 = 2_000_000;
        let est_a: u64 = estimate_fn.call1((size_a,))?.extract()?;
        let est_b: u64 = estimate_fn.call1((size_b,))?.extract()?;

        let slope = (est_b as f64 - est_a as f64) / (size_b as f64 - size_a as f64);
        let intercept = est_a as f64 - slope * size_a as f64;

        // Verify with a third point
        let size_c: u64 = 500_000;
        let est_c: u64 = estimate_fn.call1((size_c,))?.extract()?;
        let predicted_c = (slope * size_c as f64 + intercept) as u64;

        if (predicted_c as i64 - est_c as i64).unsigned_abs() > 1024 {
            // Not perfectly linear — fall back to sampling more points,
            // but for now just use the two-point approximation which is
            // good enough for the tokenizer's linear formula.
            tracing::warn!(
                "memory estimator is not perfectly linear, using approximation"
            );
        }

        Ok(Self { slope, intercept })
    }
}

impl MemoryEstimator for PyMemoryEstimatorBridge {
    fn estimate_memory(&self, binary_size: u64) -> MemoryBytes {
        (self.slope * binary_size as f64 + self.intercept).max(0.0) as u64
    }
}

/// Subprocess worker factory: spawns Python workers via socketpair.
struct SubprocessWorkerFactory {
    python_executable: PathBuf,
    source_dir: PathBuf,
    output_dir: PathBuf,
    log_dir: PathBuf,
    worker_module: String,
    worker_cmd_args: Vec<String>,
    skip_existing: bool,
    child_processes: Vec<Option<std::process::Child>>,
}

impl WorkerFactory<SocketpairManagerEnd> for SubprocessWorkerFactory {
    fn spawn_worker(&mut self, worker_id: WorkerId) -> (SocketpairManagerEnd, Option<u32>) {
        let (manager_end, child_fd) = create_socketpair()
            .unwrap_or_else(|e| panic!("failed to create socketpair for worker {worker_id}: {e}"));

        let worker_log = self.log_dir.join(format!("worker_{worker_id}.log"));

        let mut cmd = std::process::Command::new(&self.python_executable);
        cmd.arg("-m")
            .arg(&self.worker_module)
            .arg("--dynamic_queue")
            .arg(child_fd.to_string())
            .arg("--source")
            .arg(&self.source_dir)
            .arg("--output")
            .arg(&self.output_dir)
            .arg("--log-file")
            .arg(&worker_log);

        if self.skip_existing {
            cmd.arg("--skip_existing");
        }

        for arg in &self.worker_cmd_args {
            cmd.arg(arg);
        }

        // Pass the child fd
        use std::os::unix::process::CommandExt;
        unsafe {
            cmd.pre_exec(move || {
                // The child_fd is already open; nothing to do.
                Ok(())
            });
        }

        cmd.stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());

        let child = cmd
            .spawn()
            .unwrap_or_else(|e| panic!("failed to spawn worker {worker_id}: {e}"));

        // Close child fd on parent side (it was duped into child).
        // Wrapping in OwnedFd will close it on drop.
        drop(unsafe { std::os::fd::OwnedFd::from_raw_fd(child_fd) });

        // Store child handle
        let pid = child.id();
        let idx = worker_id as usize;
        if self.child_processes.len() <= idx {
            self.child_processes.resize_with(idx + 1, || None);
        }
        self.child_processes[idx] = Some(child);

        (manager_end, Some(pid))
    }
}

/// The main Python-facing local manager class.
#[pyclass(name = "RustLocalManager")]
struct PyLocalManager {
    python_executable: PathBuf,
    num_workers: u32,
    max_memory: u64,
    always_restart_worker: bool,
    print_pid: bool,
    source_dir: PathBuf,
    output_dir: PathBuf,
    log_dir: PathBuf,
    worker_module: String,
    worker_cmd_args: Vec<String>,
    skip_existing: bool,
    estimator_slope: f64,
    estimator_intercept: f64,
    stats: Option<ProcessingStats>,
    failed_tasks: Vec<db_comm_api_base::FailedTask<TokenizerIdentifier>>,
    oom_tasks: Vec<db_comm_api_base::FailedTask<TokenizerIdentifier>>,
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
        print_pid = false,
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
        print_pid: bool,
    ) -> PyResult<Self> {
        // Extract memory estimator from task_definition
        let estimate_fn = task_definition.getattr("estimate_memory")?;
        let bridge = PyMemoryEstimatorBridge::from_python(py, &estimate_fn)?;

        // Extract worker module
        let worker_module: String = task_definition
            .call_method0("get_worker_module")?
            .extract()?;

        // Build worker command args
        let source_path = PathBuf::from(&source_dir);
        let output_path = PathBuf::from(&output_dir);
        let args_list: Vec<String> = task_definition
            .call_method1(
                "build_worker_command_args",
                (task_args, source_path.to_str().unwrap(), output_path.to_str().unwrap(), skip_existing),
            )?
            .extract()?;

        let log_dir = output_path.join("logs");
        std::fs::create_dir_all(&log_dir).ok();

        // Detect the current Python interpreter so workers use the same one.
        let sys = py.import("sys")?;
        let python_executable: String = sys.getattr("executable")?.extract()?;

        Ok(Self {
            python_executable: PathBuf::from(python_executable),
            num_workers,
            max_memory,
            always_restart_worker,
            print_pid,
            source_dir: source_path,
            output_dir: output_path,
            log_dir,
            worker_module,
            worker_cmd_args: args_list,
            skip_existing,
            estimator_slope: bridge.slope,
            estimator_intercept: bridge.intercept,
            stats: None,
            failed_tasks: Vec::new(),
            oom_tasks: Vec::new(),
        })
    }

    /// Process a list of PyBinaryInfo objects.
    fn process_binaries(&mut self, py: Python<'_>, binaries: &Bound<'_, PyList>) -> PyResult<()> {
        let rust_binaries = extract_binaries(binaries)?;

        let estimator = PyMemoryEstimatorBridge {
            slope: self.estimator_slope,
            intercept: self.estimator_intercept,
        };

        let memuse_log_path = Some(self.output_dir.join("memuse.log"));

        let config = LocalManagerConfig {
            num_workers: self.num_workers,
            max_memory: self.max_memory,
            always_restart_worker: self.always_restart_worker,
            print_pid: self.print_pid,
            memuse_log_path,
        };

        let mut factory = SubprocessWorkerFactory {
            python_executable: self.python_executable.clone(),
            source_dir: self.source_dir.clone(),
            output_dir: self.output_dir.clone(),
            log_dir: self.log_dir.clone(),
            worker_module: self.worker_module.clone(),
            worker_cmd_args: self.worker_cmd_args.clone(),
            skip_existing: self.skip_existing,
            child_processes: Vec::new(),
        };

        // Run the async manager on a current-thread tokio runtime,
        // releasing the GIL during processing.
        py.detach(|| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to create tokio runtime");

            rt.block_on(async {
                let mut manager =
                    LocalManager::new(config, MemoryStealingScheduler, estimator);
                manager.process_binaries(rust_binaries, &mut factory).await;

                self.stats = Some(manager.stats().clone());
                self.failed_tasks = manager.failed_tasks().to_vec();
                self.oom_tasks = manager.oom_tasks().to_vec();
            });

            // Clean up child processes
            for child in &mut factory.child_processes {
                if let Some(mut c) = child.take() {
                    let _ = c.kill();
                    let _ = c.wait();
                }
            }
        });

        Ok(())
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
                binary: PyBinaryInfo::from(&t.binary),
                error_type: format!("{:?}", t.error_type),
                error_message: t.error_message.clone(),
            })
            .collect()
    }

    #[getter]
    fn oom_tasks(&self) -> Vec<PyFailedTask> {
        self.oom_tasks
            .iter()
            .map(|t| PyFailedTask {
                binary: PyBinaryInfo::from(&t.binary),
                error_type: format!("{:?}", t.error_type),
                error_message: t.error_message.clone(),
            })
            .collect()
    }
}

/// Helper: extract a Vec<BinaryInfo<TokenizerIdentifier>> from a Python list of BinaryInfo-like objects.
fn extract_binaries(binaries: &Bound<'_, PyList>) -> PyResult<Vec<BinaryInfo<TokenizerIdentifier>>> {
    binaries
        .iter()
        .map(|item| {
            let path_obj = item.getattr("path")?;
            let path: String = path_obj.str()?.to_string();
            let size: u64 = item.getattr("size")?.extract()?;
            let ident = item.getattr("identifier")?;
            let binary_name: String = ident.getattr("binary_name")?.extract()?;
            let platform: String = ident.getattr("platform")?.extract()?;
            let compiler: String = ident.getattr("compiler")?.extract()?;
            let version: String = ident.getattr("version")?.extract()?;
            let opt_level: String = ident.getattr("opt_level")?.extract()?;

            Ok(BinaryInfo {
                path: PathBuf::from(path),
                size,
                identifier: TokenizerIdentifier {
                    binary_name,
                    platform,
                    compiler,
                    version,
                    opt_level,
                },
            })
        })
        .collect()
}

// ── Distributed coordinator bindings ──

use std::collections::HashMap;
use db_distributed_manager::{
    PrimaryCoordinator, PrimaryConfig, SecondaryTransport,
    SecondaryCoordinator, SecondaryConfig, PrimaryTransport,
};
use db_primary_secondary_comm::DistributedMessage;
use std::time::Duration;

/// Channel-based SecondaryTransport for in-process primary.
struct ChannelSecondaryTransport {
    outgoing: HashMap<String, tokio::sync::mpsc::UnboundedSender<DistributedMessage<TokenizerIdentifier>>>,
    incoming_rx: tokio::sync::mpsc::UnboundedReceiver<DistributedMessage<TokenizerIdentifier>>,
}

impl SecondaryTransport<TokenizerIdentifier> for ChannelSecondaryTransport {
    async fn send_to(&mut self, secondary_id: &str, msg: DistributedMessage<TokenizerIdentifier>) -> Result<(), String> {
        if let Some(tx) = self.outgoing.get(secondary_id) {
            tx.send(msg).map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    async fn recv(&mut self) -> Option<DistributedMessage<TokenizerIdentifier>> {
        self.incoming_rx.recv().await
    }
}

/// Channel-based PrimaryTransport for in-process secondary.
struct ChannelPrimaryTransport {
    tx: tokio::sync::mpsc::UnboundedSender<DistributedMessage<TokenizerIdentifier>>,
    rx: tokio::sync::mpsc::UnboundedReceiver<DistributedMessage<TokenizerIdentifier>>,
}

impl PrimaryTransport<TokenizerIdentifier> for ChannelPrimaryTransport {
    async fn send(&mut self, msg: DistributedMessage<TokenizerIdentifier>) -> Result<(), String> {
        self.tx.send(msg).map_err(|e| e.to_string())
    }

    async fn recv(&mut self) -> Option<DistributedMessage<TokenizerIdentifier>> {
        self.rx.recv().await
    }
}

/// In-process distributed manager: runs primary + N secondaries in the same
/// process using channel transport. Suitable for `--multi-computer single-process`.
#[pyclass(name = "RustDistributedManager")]
struct PyDistributedManager {
    python_executable: PathBuf,
    num_secondaries: u32,
    num_workers_per_secondary: u32,
    ram_per_secondary: u64,
    source_dir: PathBuf,
    output_dir: PathBuf,
    log_dir: PathBuf,
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
    ) -> PyResult<Self> {
        let estimate_fn = task_definition.getattr("estimate_memory")?;
        let bridge = PyMemoryEstimatorBridge::from_python(py, &estimate_fn)?;

        let worker_module: String = task_definition
            .call_method0("get_worker_module")?
            .extract()?;

        let source_path = PathBuf::from(&source_dir);
        let output_path = PathBuf::from(&output_dir);
        let args_list: Vec<String> = task_definition
            .call_method1(
                "build_worker_command_args",
                (task_args, source_path.to_str().unwrap(), output_path.to_str().unwrap(), skip_existing),
            )?
            .extract()?;

        let log_dir = output_path.join("logs");
        std::fs::create_dir_all(&log_dir).ok();

        // Detect the current Python interpreter so workers use the same one.
        let sys = py.import("sys")?;
        let python_executable: String = sys.getattr("executable")?.extract()?;

        Ok(Self {
            python_executable: PathBuf::from(python_executable),
            num_secondaries,
            num_workers_per_secondary,
            ram_per_secondary,
            source_dir: source_path,
            output_dir: output_path,
            log_dir,
            worker_module,
            worker_cmd_args: args_list,
            skip_existing,
            estimator_slope: bridge.slope,
            estimator_intercept: bridge.intercept,
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

            rt.block_on(async {
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
                    tokio::spawn(async move {
                        let mut rx = sec_to_pri_rx;
                        while let Some(msg) = rx.recv().await {
                            if fwd_tx.send(msg).is_err() {
                                break;
                            }
                        }
                    });

                    let sec_python = python_executable.clone();
                    let sec_source = source_dir.clone();
                    let sec_output = output_dir.clone();
                    let sec_log = log_dir.clone();
                    let sec_worker_module = worker_module.clone();
                    let sec_worker_args = worker_cmd_args.clone();

                    let handle = tokio::spawn(async move {
                        let transport = ChannelPrimaryTransport {
                            tx: sec_to_pri_tx,
                            rx: pri_to_sec_rx,
                        };
                        let config = SecondaryConfig {
                            secondary_id,
                            num_workers,
                            ram_bytes: ram,
                            hostname: "localhost".into(),
                            keepalive_interval: Duration::from_secs(60),
                        };

                        let estimator = PyMemoryEstimatorBridge { slope, intercept };

                        let mut factory = SubprocessWorkerFactory {
                            python_executable: sec_python,
                            source_dir: sec_source,
                            output_dir: sec_output,
                            log_dir: sec_log,
                            worker_module: sec_worker_module,
                            worker_cmd_args: sec_worker_args,
                            skip_existing,
                            child_processes: Vec::new(),
                        };

                        let mut secondary = SecondaryCoordinator::new(
                            config,
                            transport,
                            MemoryStealingScheduler,
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

                let transport = ChannelSecondaryTransport { outgoing, incoming_rx };
                let config = PrimaryConfig {
                    node_id: "primary".into(),
                    num_secondaries,
                    connect_timeout: Duration::from_secs(30),
                    peer_timeout: Duration::from_secs(30),
                };

                let estimator = PyMemoryEstimatorBridge { slope, intercept };
                let mut primary = PrimaryCoordinator::new(
                    config,
                    transport,
                    MemoryStealingScheduler,
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
            });
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

/// Python module definition.
#[pymodule]
fn dynamic_batch_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyBinaryIdentifier>()?;
    m.add_class::<PyBinaryInfo>()?;
    m.add_class::<PyProcessingStats>()?;
    m.add_class::<PyFailedTask>()?;
    m.add_class::<PyLocalManager>()?;
    m.add_class::<PyDistributedManager>()?;
    Ok(())
}
