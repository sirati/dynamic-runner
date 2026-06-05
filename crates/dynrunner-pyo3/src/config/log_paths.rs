use std::path::{Path, PathBuf};

use dynrunner_core::WorkerId;
use pyo3::prelude::*;

/// Path-naming policy for log files, sockets, and the per-run log directory.
///
/// Templates accept `{worker_id}`, `{timestamp}`, and `{secondary_id}`
/// placeholders. The default layout puts every secondary's logs into
/// its own `{secondary_id}/` subdirectory under the log-mount root, so
/// per-secondary uniqueness lives at the directory level rather than in
/// the filename — and `worker_*.log` lands in the SAME per-secondary
/// folder the role logs (`secondary.log`/`primary.log`) use, whose
/// directory the spawn paths forward as `--full-log-dir=<root>/{sid}`.
/// Per-RUN uniqueness is the responsibility of the `--log-dir` value
/// itself (the SLURM pipeline anchors it at `<base>/run_<timestamp>`),
/// so the template does NOT re-add a `{timestamp}` level; doing so
/// nested worker logs one layer below the role logs
/// (`<root>/{timestamp}/{sid}/` vs `<root>/{sid}/`). The "logs/" prefix
/// that older shapes carried only made sense when logs lived inside the
/// output dir; with the dedicated log-network mount it would just nest
/// one layer too deep.
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
            log_dir_template: "{secondary_id}".into(),
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

    /// Build the per-secondary log directory under `output_dir` from the
    /// template using the caller-supplied `secondary_id`. The template may
    /// include `{secondary_id}` and (optionally) `{timestamp}`; the default
    /// uses only `{secondary_id}` so the directory matches the per-secondary
    /// folder the role logs use. `{timestamp}` substitution stays available
    /// for callers that override the template and want per-run uniqueness at
    /// the directory level rather than via the `output_dir` value.
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

#[cfg(test)]
mod tests {
    use super::LogPathConfig;
    use pyo3::prelude::*;
    use std::path::{Path, PathBuf};

    /// Default layout: the per-secondary log dir is `<root>/<sid>` with
    /// NO extra inner timestamp level, so a worker log lands at
    /// `<root>/<sid>/worker_<id>.log` — the SAME per-secondary folder the
    /// spawn paths forward as `--full-log-dir=<root>/<sid>` for the role
    /// logs (`secondary.log`). Pins the BUG1 fix: pre-fix the default
    /// template was `{timestamp}/{secondary_id}`, nesting worker logs one
    /// layer below the role logs under an extra `run_dir/<timestamp>/`.
    #[test]
    fn default_worker_log_lands_in_per_secondary_dir_no_timestamp_level() {
        Python::attach(|py| {
            let cfg = LogPathConfig::default();
            let root = Path::new("/app/log-network");
            let dir = cfg
                .resolve_log_dir(py, root, "secondary-3")
                .expect("resolve_log_dir must succeed");
            assert_eq!(
                dir,
                PathBuf::from("/app/log-network/secondary-3"),
                "default per-secondary dir must be `<root>/<sid>` with no \
                 inner timestamp level"
            );

            let worker_log = cfg.worker_log(&dir, 0);
            assert_eq!(
                worker_log,
                PathBuf::from("/app/log-network/secondary-3/worker_0.log"),
                "worker log must land in the per-secondary folder, matching \
                 the `--full-log-dir=<root>/<sid>` that holds secondary.log"
            );
        });
    }

    /// `{timestamp}` substitution remains available for callers that
    /// override the template (per-run uniqueness at the directory level).
    #[test]
    fn timestamp_placeholder_still_substituted_when_template_uses_it() {
        Python::attach(|py| {
            let cfg = LogPathConfig::new(
                Some("{timestamp}/{secondary_id}".to_string()),
                None,
                None,
                Some("FIXED".to_string()),
            );
            let dir = cfg
                .resolve_log_dir(py, Path::new("/logs"), "sec-0")
                .expect("resolve_log_dir must succeed");
            assert_eq!(dir, PathBuf::from("/logs/FIXED/sec-0"));
        });
    }
}
