use std::path::Path;

use dynrunner_core::WorkerId;
use pyo3::prelude::*;

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
pub(crate) struct WorkerSpec {
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
pub(crate) struct WorkerVars<'a> {
    pub(crate) comm_fd: Option<i32>,
    pub(crate) socket_path: Option<&'a Path>,
    pub(crate) worker_id: WorkerId,
    pub(crate) log_file: &'a Path,
}

/// Result of rendering a [`WorkerSpec`] template — argv, env, and cwd in
/// terms the standard library's `Command` builder consumes directly.
pub(crate) struct RenderedCommand {
    pub(crate) argv: Vec<String>,
    pub(crate) env: std::collections::HashMap<String, String>,
    pub(crate) cwd: Option<String>,
}

impl WorkerSpec {
    pub(crate) fn render(&self, vars: &WorkerVars<'_>) -> RenderedCommand {
        let comm_fd = vars.comm_fd.map(|fd| fd.to_string()).unwrap_or_default();
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
        RenderedCommand {
            argv: self.argv.iter().map(|a| subst(a)).collect(),
            env: self
                .env
                .iter()
                .map(|(k, v)| (k.clone(), subst(v)))
                .collect(),
            cwd: self.cwd.as_deref().map(subst),
        }
    }
}
