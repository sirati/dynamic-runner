use std::os::fd::FromRawFd;
use std::path::{Path, PathBuf};

use pyo3::prelude::*;
use pyo3::types::PyList;

use serde::{Deserialize, Serialize};

use db_comm_api_base::{
    BinaryInfo, MessageReceiver, MessageSender, WorkerId,
};
use db_manager_runner_comm::{Command, Response};

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
use db_scheduler_api::ResourceEstimator;
use db_scheduler_impl::ResourceStealingScheduler;
use db_transport_socket::named_socket::NamedSocketManagerEnd;
use db_transport_socket::socketpair::{SocketpairManagerEnd, create_socketpair};

// ── EitherManagerEnd: unified transport for socketpair + named socket ──

/// A manager-side transport endpoint that works with either socketpair or named
/// socket connections. Named sockets require an async `accept()` before
/// communication, which is performed lazily on the first `recv_responses` call.
enum EitherManagerEnd {
    Socketpair(SocketpairManagerEnd),
    /// Named socket — `Option` holds it until accept is called; after accept
    /// it stays `Some` (the accept mutates the inner state to have a connection).
    Named {
        inner: NamedSocketManagerEnd,
        accepted: bool,
    },
}

impl MessageSender<Command> for EitherManagerEnd {
    async fn send(&mut self, msg: Command) -> Result<(), String> {
        match self {
            EitherManagerEnd::Socketpair(s) => s.send(msg).await,
            EitherManagerEnd::Named { inner, accepted } => {
                if !*accepted {
                    return Err("Named socket: connection not yet accepted".into());
                }
                inner.send(msg).await
            }
        }
    }
}

impl MessageReceiver<Response> for EitherManagerEnd {
    async fn recv(&mut self) -> Option<Response> {
        match self {
            EitherManagerEnd::Socketpair(s) => s.recv().await,
            EitherManagerEnd::Named { inner, accepted } => {
                // Lazy accept: on first recv, wait for the worker to connect
                if !*accepted {
                    match inner.accept().await {
                        Ok(()) => {
                            *accepted = true;
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "named socket accept failed");
                            return None;
                        }
                    }
                }
                inner.recv().await
            }
        }
    }
}

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

impl ResourceEstimator for PyMemoryEstimatorBridge {
    fn estimate(&self, binary_size: u64) -> db_comm_api_base::ResourceMap {
        let mem = (self.slope * binary_size as f64 + self.intercept).max(0.0) as u64;
        db_comm_api_base::ResourceMap::from([(db_comm_api_base::ResourceKind::memory(), mem)])
    }
}

/// Connection mode for worker communication.
#[derive(Clone, Debug)]
enum ConnectionMode {
    /// Anonymous Unix socketpair — FD is passed to child process.
    Socketpair,
    /// Named Unix domain socket — socket path is passed to child process.
    Named {
        socket_dir: PathBuf,
    },
}

/// Argv + env + cwd template for worker subprocesses.
///
/// Python supplies the executable, flag names, and argument order. Rust
/// substitutes runtime values for the following placeholders inside any
/// argv element, env value, or cwd:
///
/// - `{COMM_FD}` — child socketpair file descriptor (decimal). Empty in
///   named-socket mode.
/// - `{SOCKET_PATH}` — named-socket path. Empty in socketpair mode.
/// - `{WORKER_ID}` — integer worker id (decimal).
/// - `{LOG_FILE}` — per-worker log file path (resolved via `LogPathConfig`).
///
/// `argv[0]` is the executable. If no `WorkerSpec` is provided, the
/// SubprocessWorkerFactory falls back to building the legacy
/// `python -m <module> --dynamic_queue/--socket-path --source --output
/// --log-file [--skip_existing] <task_args...>` shape.
#[pyclass(name = "WorkerSpec", get_all, set_all, from_py_object)]
#[derive(Clone, Debug)]
struct WorkerSpec {
    argv: Vec<String>,
    env: std::collections::HashMap<String, String>,
    cwd: Option<String>,
}

#[pymethods]
impl WorkerSpec {
    #[new]
    #[pyo3(signature = (argv, env = None, cwd = None))]
    fn new(
        argv: Vec<String>,
        env: Option<std::collections::HashMap<String, String>>,
        cwd: Option<String>,
    ) -> PyResult<Self> {
        if argv.is_empty() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "WorkerSpec.argv must contain at least one element (the executable)",
            ));
        }
        Ok(Self {
            argv,
            env: env.unwrap_or_default(),
            cwd,
        })
    }
}

/// Runtime values substituted into a `WorkerSpec` template.
struct WorkerVars<'a> {
    comm_fd: Option<i32>,
    socket_path: Option<&'a Path>,
    worker_id: WorkerId,
    log_file: &'a Path,
}

impl WorkerSpec {
    fn render(&self, vars: &WorkerVars<'_>) -> (Vec<String>, std::collections::HashMap<String, String>, Option<String>) {
        let comm_fd = vars
            .comm_fd
            .map(|fd| fd.to_string())
            .unwrap_or_default();
        let socket_path = vars
            .socket_path
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_default();
        let worker_id = vars.worker_id.to_string();
        let log_file = vars.log_file.to_string_lossy().into_owned();
        let subst = |s: &str| -> String {
            s.replace("{COMM_FD}", &comm_fd)
                .replace("{SOCKET_PATH}", &socket_path)
                .replace("{WORKER_ID}", &worker_id)
                .replace("{LOG_FILE}", &log_file)
        };
        (
            self.argv.iter().map(|a| subst(a)).collect(),
            self.env.iter().map(|(k, v)| (k.clone(), subst(v))).collect(),
            self.cwd.as_deref().map(subst),
        )
    }
}

/// Tuning knobs for `ResourceStealingScheduler` exposed to Python.
///
/// The defaults match the prior hard-coded values:
/// - `base_overhead`: 150 MiB
/// - `pressure_threshold`: 500 MiB
/// - `temp_factors`: `[1.5, 2.0, 3.0, 4.0]` (slowest opportunistic worker
///   gets `available / 1.5`, the next one `/ 2.0`, etc.; later workers reuse
///   the final value).
#[pyclass(name = "SchedulerConfig", get_all, set_all, from_py_object)]
#[derive(Clone, Debug)]
struct SchedulerConfig {
    base_overhead: u64,
    pressure_threshold: u64,
    temp_factors: Vec<f64>,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            base_overhead: 150 * 1024 * 1024,
            pressure_threshold: 500 * 1024 * 1024,
            temp_factors: vec![1.5, 2.0, 3.0, 4.0],
        }
    }
}

#[pymethods]
impl SchedulerConfig {
    #[new]
    #[pyo3(signature = (
        base_overhead = None,
        pressure_threshold = None,
        temp_factors = None,
    ))]
    fn new(
        base_overhead: Option<u64>,
        pressure_threshold: Option<u64>,
        temp_factors: Option<Vec<f64>>,
    ) -> Self {
        let d = SchedulerConfig::default();
        Self {
            base_overhead: base_overhead.unwrap_or(d.base_overhead),
            pressure_threshold: pressure_threshold.unwrap_or(d.pressure_threshold),
            temp_factors: temp_factors.unwrap_or(d.temp_factors),
        }
    }
}

impl SchedulerConfig {
    fn build_memory_scheduler(&self) -> ResourceStealingScheduler {
        ResourceStealingScheduler {
            resource_kind: db_comm_api_base::ResourceKind::memory(),
            base_overhead: self.base_overhead,
            pressure_threshold: self.pressure_threshold,
            temp_factors: self.temp_factors.clone(),
        }
    }
}

/// Path-naming policy for log files, sockets, and the per-run log directory.
///
/// Templates accept simple `{worker_id}` and `{timestamp}` placeholders. Defaults
/// preserve the legacy layout: `<output>/logs/<%Y%m%d_%H%M%S>/worker_<id>.log`
/// and `worker_<id>.sock` inside the named-socket directory.
#[pyclass(name = "LogPathConfig", get_all, set_all, from_py_object)]
#[derive(Clone, Debug)]
struct LogPathConfig {
    log_dir_template: String,
    worker_log_pattern: String,
    socket_path_pattern: String,
    timestamp_fmt: String,
}

impl Default for LogPathConfig {
    fn default() -> Self {
        Self {
            log_dir_template: "logs/{timestamp}".into(),
            worker_log_pattern: "worker_{worker_id}.log".into(),
            socket_path_pattern: "worker_{worker_id}.sock".into(),
            timestamp_fmt: "%Y%m%d_%H%M%S".into(),
        }
    }
}

#[pymethods]
impl LogPathConfig {
    #[new]
    #[pyo3(signature = (
        log_dir_template = None,
        worker_log_pattern = None,
        socket_path_pattern = None,
        timestamp_fmt = None,
    ))]
    fn new(
        log_dir_template: Option<String>,
        worker_log_pattern: Option<String>,
        socket_path_pattern: Option<String>,
        timestamp_fmt: Option<String>,
    ) -> Self {
        let d = LogPathConfig::default();
        Self {
            log_dir_template: log_dir_template.unwrap_or(d.log_dir_template),
            worker_log_pattern: worker_log_pattern.unwrap_or(d.worker_log_pattern),
            socket_path_pattern: socket_path_pattern.unwrap_or(d.socket_path_pattern),
            timestamp_fmt: timestamp_fmt.unwrap_or(d.timestamp_fmt),
        }
    }
}

impl LogPathConfig {
    fn worker_log(&self, log_dir: &Path, worker_id: WorkerId) -> PathBuf {
        log_dir.join(
            self.worker_log_pattern
                .replace("{worker_id}", &worker_id.to_string()),
        )
    }

    fn socket_path(&self, socket_dir: &Path, worker_id: WorkerId) -> PathBuf {
        socket_dir.join(
            self.socket_path_pattern
                .replace("{worker_id}", &worker_id.to_string()),
        )
    }

    /// Build the per-run log directory under `output_dir` from the template
    /// using the current timestamp. The template may include `{timestamp}`.
    fn resolve_log_dir(&self, py: Python<'_>, output_dir: &Path) -> PyResult<PathBuf> {
        let datetime_mod = py.import("datetime")?;
        let now = datetime_mod.getattr("datetime")?.call_method0("now")?;
        let timestamp: String = now
            .call_method1("strftime", (self.timestamp_fmt.as_str(),))?
            .extract()?;
        let rendered = self.log_dir_template.replace("{timestamp}", &timestamp);
        let path = PathBuf::from(rendered);
        Ok(if path.is_absolute() {
            path
        } else {
            output_dir.join(path)
        })
    }
}

/// One of the two transport-specific values that have to flow into the worker
/// argv: a socketpair file descriptor or a named-socket path.
enum FdOrSocket<'a> {
    Fd(i32),
    Socket(&'a Path),
}

/// Subprocess worker factory: spawns Python workers via socketpair or named socket.
struct SubprocessWorkerFactory {
    python_executable: PathBuf,
    source_dir: PathBuf,
    output_dir: PathBuf,
    log_dir: PathBuf,
    log_paths: LogPathConfig,
    worker_module: String,
    worker_cmd_args: Vec<String>,
    skip_existing: bool,
    connection_mode: ConnectionMode,
    manual_start_worker: bool,
    /// If `Some`, Python supplies the full argv/env/cwd template and the
    /// fields above are only used to render placeholders. If `None`, the
    /// factory falls back to the legacy hardcoded argv shape.
    worker_spec: Option<WorkerSpec>,
    child_processes: Vec<Option<std::process::Child>>,
}

impl SubprocessWorkerFactory {
    /// Build the legacy hardcoded argv when no explicit `WorkerSpec` was
    /// provided. The first element is the executable.
    fn legacy_argv(&self, worker_id: WorkerId, fd_or_socket: FdOrSocket<'_>) -> Vec<String> {
        let worker_log = self.log_paths.worker_log(&self.log_dir, worker_id);
        let mut argv: Vec<String> = vec![
            self.python_executable.to_string_lossy().into_owned(),
            "-m".into(),
            self.worker_module.clone(),
        ];
        match fd_or_socket {
            FdOrSocket::Fd(fd) => {
                argv.push("--dynamic_queue".into());
                argv.push(fd.to_string());
            }
            FdOrSocket::Socket(p) => {
                argv.push("--socket-path".into());
                argv.push(p.to_string_lossy().into_owned());
            }
        }
        argv.push("--source".into());
        argv.push(self.source_dir.to_string_lossy().into_owned());
        argv.push("--output".into());
        argv.push(self.output_dir.to_string_lossy().into_owned());
        argv.push("--log-file".into());
        argv.push(worker_log.to_string_lossy().into_owned());
        if self.skip_existing {
            argv.push("--skip_existing".into());
        }
        for arg in &self.worker_cmd_args {
            argv.push(arg.clone());
        }
        argv
    }

    /// Build (argv, env, cwd) for a worker, picking the explicit `WorkerSpec`
    /// template when present and falling back to the legacy argv otherwise.
    fn render_command(
        &self,
        worker_id: WorkerId,
        fd_or_socket: FdOrSocket<'_>,
    ) -> (Vec<String>, std::collections::HashMap<String, String>, Option<String>) {
        let worker_log = self.log_paths.worker_log(&self.log_dir, worker_id);
        if let Some(spec) = &self.worker_spec {
            let (fd, sock) = match fd_or_socket {
                FdOrSocket::Fd(fd) => (Some(fd), None),
                FdOrSocket::Socket(p) => (None, Some(p)),
            };
            spec.render(&WorkerVars {
                comm_fd: fd,
                socket_path: sock,
                worker_id,
                log_file: &worker_log,
            })
        } else {
            (
                self.legacy_argv(worker_id, fd_or_socket),
                std::collections::HashMap::new(),
                None,
            )
        }
    }

    /// Spawn using socketpair mode: create a socketpair, pass child FD.
    fn spawn_socketpair(
        &mut self,
        worker_id: WorkerId,
    ) -> Result<(EitherManagerEnd, Option<u32>), String> {
        let (manager_end, child_fd) =
            create_socketpair().map_err(|e| format!("failed to create socketpair: {e}"))?;

        let (argv, env, cwd) = self.render_command(worker_id, FdOrSocket::Fd(child_fd));
        let mut cmd = std::process::Command::new(&argv[0]);
        cmd.args(&argv[1..]);
        for (k, v) in &env {
            cmd.env(k, v);
        }
        if let Some(cwd) = &cwd {
            cmd.current_dir(cwd);
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
            .map_err(|e| format!("failed to exec worker {worker_id}: {e}"))?;

        // Close child fd on parent side (duped into child).
        drop(unsafe { std::os::fd::OwnedFd::from_raw_fd(child_fd) });

        let pid = child.id();
        let idx = worker_id as usize;
        if self.child_processes.len() <= idx {
            self.child_processes.resize_with(idx + 1, || None);
        }
        self.child_processes[idx] = Some(child);

        Ok((EitherManagerEnd::Socketpair(manager_end), Some(pid)))
    }

    /// Spawn using named socket mode: bind socket, then optionally spawn subprocess.
    fn spawn_named(
        &mut self,
        worker_id: WorkerId,
        socket_dir: &PathBuf,
    ) -> Result<(EitherManagerEnd, Option<u32>), String> {
        let socket_path = self.log_paths.socket_path(socket_dir, worker_id);
        let manager_end = NamedSocketManagerEnd::bind(&socket_path)
            .map_err(|e| format!("failed to bind named socket: {e}"))?;

        let (argv, env, cwd) = self.render_command(worker_id, FdOrSocket::Socket(&socket_path));

        if self.manual_start_worker {
            tracing::info!(
                worker_id,
                "\n[Worker {worker_id}] Please run the following command in another terminal:\n  {}\n[Worker {worker_id}] Manager will detect when worker connects via socket: {}",
                argv.join(" "),
                socket_path.display()
            );

            let endpoint = EitherManagerEnd::Named {
                inner: manager_end,
                accepted: false,
            };
            // No child process — worker started manually
            return Ok((endpoint, None));
        }

        // Auto-spawn subprocess with --socket-path
        let mut cmd = std::process::Command::new(&argv[0]);
        cmd.args(&argv[1..]);
        for (k, v) in &env {
            cmd.env(k, v);
        }
        if let Some(cwd) = &cwd {
            cmd.current_dir(cwd);
        }

        cmd.stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());

        let child = cmd
            .spawn()
            .map_err(|e| format!("failed to exec worker {worker_id}: {e}"))?;

        let pid = child.id();
        let idx = worker_id as usize;
        if self.child_processes.len() <= idx {
            self.child_processes.resize_with(idx + 1, || None);
        }
        self.child_processes[idx] = Some(child);

        let endpoint = EitherManagerEnd::Named {
            inner: manager_end,
            accepted: false,
        };
        Ok((endpoint, Some(pid)))
    }
}

impl WorkerFactory<EitherManagerEnd> for SubprocessWorkerFactory {
    fn spawn_worker(
        &mut self,
        worker_id: WorkerId,
    ) -> Result<(EitherManagerEnd, Option<u32>), String> {
        match &self.connection_mode {
            ConnectionMode::Socketpair => self.spawn_socketpair(worker_id),
            ConnectionMode::Named { socket_dir } => {
                let socket_dir = socket_dir.clone();
                self.spawn_named(worker_id, &socket_dir)
            }
        }
    }
}

/// The main Python-facing local manager class.
#[pyclass(name = "RustLocalManager")]
struct PyLocalManager {
    python_executable: PathBuf,
    num_workers: u32,
    max_memory: u64,
    low_memory_threshold: u64,
    always_restart_worker: bool,
    print_pid: bool,
    source_dir: PathBuf,
    output_dir: PathBuf,
    log_dir: PathBuf,
    log_paths: LogPathConfig,
    worker_spec: Option<WorkerSpec>,
    scheduler_config: SchedulerConfig,
    worker_module: String,
    worker_cmd_args: Vec<String>,
    skip_existing: bool,
    estimator_slope: f64,
    estimator_intercept: f64,
    stage_timeouts: std::collections::HashMap<String, std::time::Duration>,
    connection_mode: ConnectionMode,
    manual_start_worker: bool,
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
        connection_mode = "socketpair",
        socket_dir = None,
        manual_start_worker = false,
        log_paths = None,
        worker_spec = None,
        low_memory_threshold = None,
        scheduler_config = None,
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
        connection_mode: &str,
        socket_dir: Option<String>,
        manual_start_worker: bool,
        log_paths: Option<LogPathConfig>,
        worker_spec: Option<WorkerSpec>,
        low_memory_threshold: Option<u64>,
        scheduler_config: Option<SchedulerConfig>,
    ) -> PyResult<Self> {
        // Extract memory estimator from task_definition
        let estimate_fn = task_definition.getattr("estimate_memory")?;
        let bridge = PyMemoryEstimatorBridge::from_python(py, &estimate_fn)?;

        // Extract stage timeouts from task_definition.get_stages()
        let mut stage_timeouts = std::collections::HashMap::new();
        let stages: Vec<Bound<'_, PyAny>> = task_definition.call_method0("get_stages")?.extract()?;
        for stage in &stages {
            let phase = stage.getattr("phase")?;
            let phase_name: String = phase.getattr("value")?.extract()?;
            let timeout_obj = stage.getattr("timeout_seconds")?;
            if !timeout_obj.is_none() {
                let timeout_secs: f64 = timeout_obj.extract()?;
                stage_timeouts.insert(
                    phase_name,
                    std::time::Duration::from_secs_f64(timeout_secs),
                );
            }
        }

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

        // Create the per-run log directory from the LogPathConfig template.
        let log_paths = log_paths.unwrap_or_default();
        let log_dir = log_paths.resolve_log_dir(py, &output_path)?;
        std::fs::create_dir_all(&log_dir).ok();

        // Detect the current Python interpreter so workers use the same one.
        let sys = py.import("sys")?;
        let python_executable: String = sys.getattr("executable")?.extract()?;

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
            python_executable: PathBuf::from(python_executable),
            num_workers,
            max_memory,
            low_memory_threshold: low_memory_threshold.unwrap_or(300 * 1024 * 1024),
            always_restart_worker,
            print_pid,
            source_dir: source_path,
            output_dir: output_path,
            log_dir,
            log_paths,
            worker_spec,
            scheduler_config: scheduler_config.unwrap_or_default(),
            worker_module,
            worker_cmd_args: args_list,
            skip_existing,
            estimator_slope: bridge.slope,
            estimator_intercept: bridge.intercept,
            stage_timeouts,
            connection_mode: conn_mode,
            manual_start_worker,
            stats: None,
            failed_tasks: Vec::new(),
            oom_tasks: Vec::new(),
        })
    }

    /// Process a list of PyBinaryInfo objects.
    fn process_binaries(&mut self, py: Python<'_>, binaries: &Bound<'_, PyList>) -> PyResult<()> {
        let mut rust_binaries = extract_binaries(binaries)?;

        // Convert absolute paths to relative (matching Python's relative_to(source_dir))
        for binary in &mut rust_binaries {
            if let Ok(rel) = binary.path.strip_prefix(&self.source_dir) {
                binary.path = rel.to_path_buf();
            }
        }

        let estimator = PyMemoryEstimatorBridge {
            slope: self.estimator_slope,
            intercept: self.estimator_intercept,
        };
        let scheduler = self.scheduler_config.build_memory_scheduler();

        let memuse_log_path = Some(self.output_dir.join("memuse.log"));

        let config = LocalManagerConfig {
            num_workers: self.num_workers,
            max_resources: db_comm_api_base::ResourceMap::from([(db_comm_api_base::ResourceKind::memory(), self.max_memory)]),
            always_restart_worker: self.always_restart_worker,
            print_pid: self.print_pid,
            memuse_log_path,
            stage_timeouts: self.stage_timeouts.clone(),
            low_resource_thresholds: db_comm_api_base::ResourceMap::from([(
                db_comm_api_base::ResourceKind::memory(),
                self.low_memory_threshold,
            )]),
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
                let outcome = manager.process_binaries(rust_binaries, &mut factory).await;

                self.stats = Some(manager.stats().clone());
                self.failed_tasks = manager.failed_tasks().to_vec();
                self.oom_tasks = manager.resource_pressure_tasks().to_vec();
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
    PrimaryCoordinator, PrimaryConfig,
    SecondaryCoordinator, SecondaryConfig,
};
use db_transport_channel::{ChannelSecondaryTransportEnd, ChannelPrimaryTransportEnd};
use std::time::Duration;

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
    log_paths: LogPathConfig,
    worker_spec: Option<WorkerSpec>,
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

        // Create the per-run log directory from the LogPathConfig template.
        let log_paths = log_paths.unwrap_or_default();
        let log_dir = log_paths.resolve_log_dir(py, &output_path)?;
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
            log_paths,
            worker_spec,
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
        let log_paths = self.log_paths.clone();
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
                            max_resources: db_comm_api_base::ResourceMap::from([(db_comm_api_base::ResourceKind::memory(), ram)]),
                            hostname: "localhost".into(),
                            keepalive_interval: Duration::from_secs(60),
                            src_network: None,
                            src_tmp: None,
                            peer_timeout: Duration::from_secs(120),
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
                            db_transport_quic::NoPeerTransport,
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
                    connect_timeout: Duration::from_secs(30),
                    peer_timeout: Duration::from_secs(30),
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

use db_transport_quic::{NetworkClient, NetworkServer};

/// Python-facing primary coordinator that listens for real network connections
/// from secondary processes. For `--multi-computer local` mode.
///
/// Spawns secondary subprocesses that connect back via WSS, then runs the
/// Rust `PrimaryCoordinator` with `NetworkServer` as the transport.
#[pyclass(name = "RustPrimaryCoordinator")]
struct PyPrimaryCoordinator {
    num_secondaries: u32,
    estimator_slope: f64,
    estimator_intercept: f64,
    spawn_secondary: Py<PyAny>,
    completed: u32,
    failed: u32,
}

#[pymethods]
impl PyPrimaryCoordinator {
    #[new]
    #[pyo3(signature = (
        num_secondaries,
        task_definition,
        spawn_secondary,
    ))]
    fn new(
        py: Python<'_>,
        num_secondaries: u32,
        task_definition: &Bound<'_, PyAny>,
        spawn_secondary: Py<PyAny>,
    ) -> PyResult<Self> {
        let estimate_fn = task_definition.getattr("estimate_memory")?;
        let bridge = PyMemoryEstimatorBridge::from_python(py, &estimate_fn)?;

        Ok(Self {
            num_secondaries,
            estimator_slope: bridge.slope,
            estimator_intercept: bridge.intercept,
            spawn_secondary: spawn_secondary.clone_ref(py),
            completed: 0,
            failed: 0,
        })
    }

    /// Run the primary coordination pipeline over real network connections.
    fn run(&mut self, py: Python<'_>, binaries: &Bound<'_, PyList>) -> PyResult<()> {
        let rust_binaries = extract_binaries(binaries)?;

        let num_secondaries = self.num_secondaries;
        let slope = self.estimator_slope;
        let intercept = self.estimator_intercept;

        // Pick a free port for the primary server before spawning secondaries.
        let tmp_listener = std::net::TcpListener::bind("127.0.0.1:0")
            .map_err(|e| pyo3::exceptions::PyOSError::new_err(format!("failed to bind: {e}")))?;
        let port = tmp_listener.local_addr().unwrap().port();
        drop(tmp_listener);

        let primary_url = format!("tcp://127.0.0.1:{}", port);

        // Call the Python spawn_secondary callback for each secondary.
        // The callback receives (primary_url, secondary_id, quic_port) and
        // should return a subprocess.Popen (or compatible object with kill/wait).
        let mut child_processes: Vec<Py<PyAny>> = Vec::new();
        for i in 0..num_secondaries {
            let secondary_id = format!("secondary-{i}");
            let process = self.spawn_secondary.call1(
                py,
                (&primary_url, &secondary_id, 0u16),
            )?;
            tracing::info!(
                secondary_id = %secondary_id,
                "spawned secondary process via callback"
            );
            child_processes.push(process);
        }

        let mut completed = 0u32;
        let mut failed = 0u32;

        py.detach(|| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to create tokio runtime");

            let local = tokio::task::LocalSet::new();
            rt.block_on(local.run_until(async {
                // Bind the network server to the port we already picked.
                let bind_addr: std::net::SocketAddr =
                    format!("127.0.0.1:{}", port).parse().unwrap();
                let server: NetworkServer<TokenizerIdentifier> =
                    match NetworkServer::bind(bind_addr).await {
                        Ok(s) => s,
                        Err(e) => {
                            tracing::error!(error = %e, "failed to start network server");
                            return;
                        }
                    };
                tracing::info!(port, "primary network server listening");

                // Give secondaries a moment to start up.
                tokio::time::sleep(Duration::from_secs(2)).await;

                // Run the primary coordinator with the network server transport.
                let config = PrimaryConfig {
                    node_id: "primary".into(),
                    num_secondaries,
                    connect_timeout: Duration::from_secs(600),
                    peer_timeout: Duration::from_secs(300),
                };

                let estimator = PyMemoryEstimatorBridge { slope, intercept };
                let mut primary: PrimaryCoordinator<_, _, _, TokenizerIdentifier> =
                    PrimaryCoordinator::new(
                        config,
                        server,
                        ResourceStealingScheduler::memory(),
                        estimator,
                    );

                let result = primary.run(rust_binaries).await;
                if let Err(e) = &result {
                    tracing::error!(error = %e, "primary coordinator failed");
                }

                completed = primary.completed_count() as u32;
                failed = primary.failed_count() as u32;
            }));
        });

        // Back with the GIL — terminate secondary processes via the Python objects.
        for process in &child_processes {
            let _ = process.call_method0(py, "kill");
            let _ = process.call_method0(py, "wait");
        }

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

// ── Network-based secondary coordinator ──

/// Python-facing secondary coordinator that connects to a remote primary
/// over the network (WSS) and runs local workers. For `--secondary` mode.
#[pyclass(name = "RustSecondaryCoordinator")]
struct PySecondaryCoordinator {
    python_executable: PathBuf,
    primary_url: String,
    secondary_id: String,
    num_workers: u32,
    ram_bytes: u64,
    source_dir: PathBuf,
    output_dir: PathBuf,
    log_dir: PathBuf,
    log_paths: LogPathConfig,
    worker_spec: Option<WorkerSpec>,
    worker_module: String,
    worker_cmd_args: Vec<String>,
    skip_existing: bool,
    estimator_slope: f64,
    estimator_intercept: f64,
    completed: u32,
}

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
    ))]
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

        let log_paths = log_paths.unwrap_or_default();
        let log_dir = log_paths.resolve_log_dir(py, &output_path)?;
        std::fs::create_dir_all(&log_dir).ok();

        let sys = py.import("sys")?;
        let python_executable: String = sys.getattr("executable")?.extract()?;

        Ok(Self {
            python_executable: PathBuf::from(python_executable),
            primary_url,
            secondary_id,
            num_workers,
            ram_bytes,
            source_dir: source_path,
            output_dir: output_path,
            log_dir,
            log_paths,
            worker_spec,
            worker_module,
            worker_cmd_args: args_list,
            skip_existing,
            estimator_slope: bridge.slope,
            estimator_intercept: bridge.intercept,
            completed: 0,
        })
    }

    /// Connect to the primary and run the secondary coordination loop.
    fn run(&mut self, py: Python<'_>) -> PyResult<()> {
        let primary_url = self.primary_url.clone();
        let secondary_id = self.secondary_id.clone();
        let num_workers = self.num_workers;
        let ram_bytes = self.ram_bytes;
        let slope = self.estimator_slope;
        let intercept = self.estimator_intercept;
        let python_executable = self.python_executable.clone();
        let source_dir = self.source_dir.clone();
        let output_dir = self.output_dir.clone();
        let log_dir = self.log_dir.clone();
        let log_paths = self.log_paths.clone();
        let worker_spec = self.worker_spec.clone();
        let worker_module = self.worker_module.clone();
        let worker_cmd_args = self.worker_cmd_args.clone();
        let skip_existing = self.skip_existing;

        let mut completed = 0u32;

        py.detach(|| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to create tokio runtime");

            let local = tokio::task::LocalSet::new();
            rt.block_on(local.run_until(async {
                // Parse the primary URL to get the address.
                // Supports formats like "tcp://host:port", "ws://host:port", or "host:port"
                let addr_str = primary_url
                    .strip_prefix("tcp://")
                    .or_else(|| primary_url.strip_prefix("ws://"))
                    .or_else(|| primary_url.strip_prefix("wss://"))
                    .unwrap_or(&primary_url);

                let addr: std::net::SocketAddr = match addr_str.parse() {
                    Ok(a) => a,
                    Err(e) => {
                        tracing::error!(url = %primary_url, error = %e, "failed to parse primary URL");
                        return;
                    }
                };

                // Connect to primary via WSS with retry logic (up to 60 seconds)
                let connect_timeout = Duration::from_secs(60);
                let retry_delay = Duration::from_secs(1);
                let start = std::time::Instant::now();
                let mut attempt = 0u32;
                let client = loop {
                    attempt += 1;
                    let elapsed = start.elapsed();
                    if elapsed > connect_timeout {
                        tracing::error!(
                            addr = %addr,
                            attempts = attempt,
                            "failed to connect to primary after {:.0}s",
                            connect_timeout.as_secs_f64()
                        );
                        return;
                    }
                    match NetworkClient::connect_wss_only(addr).await {
                        Ok(c) => {
                            tracing::info!(
                                addr = %addr,
                                elapsed_s = elapsed.as_secs_f64(),
                                attempts = attempt,
                                "connected to primary"
                            );
                            break c;
                        }
                        Err(e) => {
                            let remaining = connect_timeout.saturating_sub(elapsed);
                            if remaining > retry_delay {
                                tracing::info!(
                                    attempt,
                                    error = %e,
                                    "connection failed, retrying in {:.0}s...",
                                    retry_delay.as_secs_f64()
                                );
                                tokio::time::sleep(retry_delay).await;
                            } else {
                                tracing::error!(addr = %addr, error = %e, "failed to connect to primary");
                                return;
                            }
                        }
                    }
                };

                // Start peer network for peer-to-peer communication
                let peer_network: db_transport_quic::PeerNetwork<TokenizerIdentifier> =
                    db_transport_quic::PeerNetwork::start(&format!("sec-{}", num_workers))
                        .await
                        .unwrap_or_else(|e| {
                            tracing::error!(error = %e, "failed to start peer network, using no-op");
                            // This won't happen in practice since PeerNetwork::start only fails
                            // on cert generation or bind errors, but we handle it gracefully.
                            panic!("peer network start failed: {e}");
                        });

                let peer_cert_pem = peer_network.cert_pem().to_string();
                let peer_port = peer_network.port();

                let config = SecondaryConfig {
                    secondary_id: secondary_id.clone(),
                    num_workers,
                    max_resources: db_comm_api_base::ResourceMap::from([(db_comm_api_base::ResourceKind::memory(), ram_bytes)]),
                    hostname: gethostname(),
                    keepalive_interval: Duration::from_secs(1),
                    src_network: None,
                    src_tmp: None,
                    peer_timeout: Duration::from_secs(120),
                };

                let estimator = PyMemoryEstimatorBridge { slope, intercept };

                let mut factory = SubprocessWorkerFactory {
                    python_executable,
                    source_dir,
                    output_dir,
                    log_dir,
                    log_paths,
                    worker_module,
                    worker_cmd_args,
                    skip_existing,
                    connection_mode: ConnectionMode::Socketpair,
                    manual_start_worker: false,
                    worker_spec,
                    child_processes: Vec::new(),
                };

                let mut secondary: SecondaryCoordinator<_, _, _, _, _, TokenizerIdentifier> = SecondaryCoordinator::new(
                    config,
                    client,
                    peer_network,
                    ResourceStealingScheduler::memory(),
                    estimator,
                );

                // Set peer cert info so the CertExchange message includes our QUIC details
                secondary.set_peer_cert_info(
                    db_distributed_manager::PeerCertInfo {
                        public_cert_pem: peer_cert_pem,
                        ipv4_address: Some(detect_ipv4(None)),
                        ipv6_address: None,
                        quic_port: peer_port,
                    },
                );

                match secondary.run(&mut factory).await {
                    Ok(()) => {
                        tracing::info!("secondary finished successfully");
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "secondary failed");
                    }
                }

                completed = secondary.completed_count() as u32;

                // Clean up child processes
                for child in &mut factory.child_processes {
                    if let Some(mut c) = child.take() {
                        let _ = c.kill();
                        let _ = c.wait();
                    }
                }
            }));
        });

        self.completed = completed;
        Ok(())
    }

    #[getter]
    fn completed(&self) -> u32 {
        self.completed
    }
}

/// Get the hostname, falling back to "unknown" on error.
fn gethostname() -> String {
    std::process::Command::new("hostname")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".into())
}

/// Detect the local IPv4 address.
///
/// Resolution order:
///   1. If `override_ip` is `Some`, use it verbatim. This is the explicit
///      Python-side `local_ip` config knob (wired in via PrimaryConfig /
///      SecondaryConfig once those typed configs exist).
///   2. Otherwise, parse the first non-loopback IPv4 from `hostname -I`.
///   3. Otherwise, fall back to "127.0.0.1".
///
/// No outbound network connection is made — this works in air-gapped
/// clusters where the previous `8.8.8.8:80` UDP probe failed.
fn detect_ipv4(override_ip: Option<&str>) -> String {
    if let Some(ip) = override_ip {
        return ip.to_string();
    }

    let output = std::process::Command::new("hostname")
        .arg("-I")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok());

    if let Some(line) = output {
        for token in line.split_whitespace() {
            if let Ok(addr) = token.parse::<std::net::Ipv4Addr>() {
                if !addr.is_loopback() && !addr.is_unspecified() {
                    return addr.to_string();
                }
            }
        }
    }

    "127.0.0.1".into()
}

/// Python module definition.
#[pymodule]
fn dynamic_batch_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Initialize tracing subscriber (only once, ignore if already set)
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();

    m.add_class::<PyBinaryIdentifier>()?;
    m.add_class::<PyBinaryInfo>()?;
    m.add_class::<PyProcessingStats>()?;
    m.add_class::<PyFailedTask>()?;
    m.add_class::<LogPathConfig>()?;
    m.add_class::<WorkerSpec>()?;
    m.add_class::<SchedulerConfig>()?;
    m.add_class::<PyLocalManager>()?;
    m.add_class::<PyDistributedManager>()?;
    m.add_class::<PyPrimaryCoordinator>()?;
    m.add_class::<PySecondaryCoordinator>()?;
    Ok(())
}
