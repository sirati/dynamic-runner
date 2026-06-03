use std::path::{Path, PathBuf};

use dynrunner_core::WorkerId;
use pyo3::prelude::*;

/// Path-naming policy for log files, sockets, and the per-run log directory.
///
/// Templates accept `{worker_id}`, `{timestamp}`, and `{secondary_id}`
/// placeholders. The default layout puts every secondary's logs into
/// its own `{timestamp}/{secondary_id}/` subdirectory under the
/// log-mount root, so per-secondary uniqueness lives at the directory
/// level rather than in the filename. The "logs/" prefix that older
/// shapes carried only made sense when logs lived inside the output
/// dir; with the dedicated log-network mount it would just nest one
/// layer too deep.
#[pyclass(name = "LogPathConfig", get_all, set_all, from_py_object)]
#[derive(Clone, Debug)]
pub(crate) struct LogPathConfig {
    log_dir_template: String,
    worker_log_pattern: String,
    socket_path_pattern: String,
    timestamp_fmt: String,
}

impl Default for LogPathConfig {
    fn default() -> Self {
        Self {
            log_dir_template: "{timestamp}/{secondary_id}".into(),
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
    pub(crate) fn worker_log(&self, log_dir: &Path, worker_id: WorkerId) -> PathBuf {
        log_dir.join(
            self.worker_log_pattern
                .replace("{worker_id}", &worker_id.to_string()),
        )
    }

    pub(crate) fn socket_path(&self, socket_dir: &Path, worker_id: WorkerId) -> PathBuf {
        socket_dir.join(
            self.socket_path_pattern
                .replace("{worker_id}", &worker_id.to_string()),
        )
    }

    /// Build the per-run log directory under `output_dir` from the template
    /// using the current timestamp and the caller-supplied `secondary_id`.
    /// The template may include `{timestamp}` and `{secondary_id}`.
    ///
    /// Single-process mode is not a special case: the runner allocates
    /// itself a synthetic `secondary_id` (hostname or `"local"`) at
    /// startup and feeds it through the same template, yielding a real
    /// path that never collides with another node's logs on a shared
    /// mount. There is no empty-placeholder branch.
    pub(crate) fn resolve_log_dir(
        &self,
        py: Python<'_>,
        output_dir: &Path,
        secondary_id: &str,
    ) -> PyResult<PathBuf> {
        let datetime_mod = py.import("datetime")?;
        let now = datetime_mod.getattr("datetime")?.call_method0("now")?;
        let timestamp: String = now
            .call_method1("strftime", (self.timestamp_fmt.as_str(),))?
            .extract()?;
        let rendered = self
            .log_dir_template
            .replace("{timestamp}", &timestamp)
            .replace("{secondary_id}", secondary_id);
        let path = PathBuf::from(rendered);
        Ok(if path.is_absolute() {
            path
        } else {
            output_dir.join(path)
        })
    }
}
