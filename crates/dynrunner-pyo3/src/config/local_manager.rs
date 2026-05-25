use std::time::Duration;

use pyo3::prelude::*;

use dynrunner_manager_local::{LocalManagerConfig as RustLocalManagerConfig, RestartPredicate};

use super::resources::PyResourceMap;
use super::scheduler::SchedulerConfig;

/// Python-facing configuration for the in-process local manager.
///
/// Mirrors `dynrunner_manager_local::LocalManagerConfig` but uses the Python-typed
/// pyclasses (`ResourceMap`, callables, seconds-as-f64) and provides a
/// `to_rust(...)` builder that consumes the Python `restart_predicate`
/// callable (which is single-use because the resulting RestartPredicate
/// holds the Py<PyAny>).
#[pyclass(name = "LocalManagerConfig")]
pub(crate) struct PyLocalManagerConfig {
    #[pyo3(get, set)]
    pub(crate) num_workers: u32,
    #[pyo3(get, set)]
    pub(crate) max_resources: PyResourceMap,
    #[pyo3(get, set)]
    pub(crate) low_resource_thresholds: PyResourceMap,
    #[pyo3(get, set)]
    pub(crate) always_restart_worker: bool,
    #[pyo3(get, set)]
    pub(crate) restart_predicate: Option<Py<PyAny>>,
    #[pyo3(get, set)]
    pub(crate) retry_max_attempts: u32,
    #[pyo3(get, set)]
    pub(crate) print_pid: bool,
    #[pyo3(get, set)]
    pub(crate) memuse_log_path: Option<std::path::PathBuf>,
    #[pyo3(get, set)]
    pub(crate) stage_timeouts_secs: std::collections::HashMap<String, f64>,
    #[pyo3(get, set)]
    pub(crate) resource_check_interval_secs: f64,
    #[pyo3(get, set)]
    pub(crate) phase_status_log_intervals_secs: Vec<f64>,
    #[pyo3(get, set)]
    pub(crate) scheduler_config: SchedulerConfig,
    /// Surfaces `LocalManagerConfig.log_oom_watcher` through the
    /// Python config so the CLI's `--log-oom-watcher` can flip it.
    /// Default `false`.
    #[pyo3(get, set)]
    pub(crate) log_oom_watcher: bool,
    /// Run-level output directory the operator passed via `--output`
    /// (Python side resolves to an absolute path). `Some(...)` here
    /// is the necessary precondition for memprofile sampling — the
    /// `to_rust` builder composes `{output_dir}/memprofile/` into
    /// `LocalManagerConfig.output_dir`, which in turn drives sampler
    /// construction. The pairing with `memprofile_enabled` controls
    /// whether the composition happens at all; this field on its
    /// own does nothing.
    #[pyo3(get, set)]
    pub(crate) output_dir: Option<std::path::PathBuf>,
    /// Python-side `--memprofile` opt-in. The flag's behaviour lives
    /// entirely in Rust (`to_rust` decides whether to compose the
    /// memprofile path into `LocalManagerConfig.output_dir`); Python
    /// is just the bridge that toggles this bool from the CLI parser.
    #[pyo3(get, set)]
    pub(crate) memprofile_enabled: bool,
}

#[pymethods]
impl PyLocalManagerConfig {
    #[new]
    #[pyo3(signature = (
        num_workers,
        max_resources,
        low_resource_thresholds = None,
        always_restart_worker = false,
        restart_predicate = None,
        retry_max_attempts = 1,
        print_pid = false,
        memuse_log_path = None,
        stage_timeouts_secs = None,
        resource_check_interval_secs = 0.1,
        phase_status_log_intervals_secs = None,
        scheduler_config = None,
        log_oom_watcher = false,
        output_dir = None,
        memprofile_enabled = false,
    ))]
    // PyO3 kwargs surface — collapsing to a builder is a separate
    // API refactor.
    #[allow(clippy::too_many_arguments)]
    fn new(
        num_workers: u32,
        max_resources: PyResourceMap,
        low_resource_thresholds: Option<PyResourceMap>,
        always_restart_worker: bool,
        restart_predicate: Option<Py<PyAny>>,
        retry_max_attempts: u32,
        print_pid: bool,
        memuse_log_path: Option<std::path::PathBuf>,
        stage_timeouts_secs: Option<std::collections::HashMap<String, f64>>,
        resource_check_interval_secs: f64,
        phase_status_log_intervals_secs: Option<Vec<f64>>,
        scheduler_config: Option<SchedulerConfig>,
        log_oom_watcher: bool,
        output_dir: Option<std::path::PathBuf>,
        memprofile_enabled: bool,
    ) -> Self {
        Self {
            num_workers,
            max_resources,
            low_resource_thresholds: low_resource_thresholds
                .unwrap_or_else(|| PyResourceMap::from_single("memory", 300 * 1024 * 1024)),
            always_restart_worker,
            restart_predicate,
            retry_max_attempts,
            print_pid,
            memuse_log_path,
            stage_timeouts_secs: stage_timeouts_secs.unwrap_or_default(),
            resource_check_interval_secs,
            phase_status_log_intervals_secs: phase_status_log_intervals_secs
                .unwrap_or_else(|| vec![60.0, 300.0, 600.0, 1800.0, 3600.0]),
            scheduler_config: scheduler_config.unwrap_or_default(),
            log_oom_watcher,
            output_dir,
            memprofile_enabled,
        }
    }
}

impl PyLocalManagerConfig {
    /// Build the Rust-side config. Consumes `self.restart_predicate` by
    /// cloning the Py reference (callers may keep `self` for inspection;
    /// the predicate closure clones the Py once and holds it for the run).
    ///
    /// Currently unused — the in-process Python-facing manager
    /// constructs `LocalManagerConfig` directly from kwargs rather
    /// than via this wrapper. Kept as documented API surface for
    /// callers that prefer to build a `PyLocalManagerConfig` and
    /// convert in one step.
    #[allow(dead_code)]
    pub(crate) fn to_rust(&self, py: Python<'_>) -> RustLocalManagerConfig {
        let restart_predicate = self.restart_predicate.as_ref().map(|cb| {
            let cb = cb.clone_ref(py);
            let predicate: RestartPredicate =
                Box::new(move |ctx: &dynrunner_manager_local::RestartContext<'_>| {
                    crate::managers::factory_callback::invoke_restart_predicate(&cb, ctx)
                });
            predicate
        });

        let stage_timeouts = self
            .stage_timeouts_secs
            .iter()
            .map(|(k, v)| (k.clone(), Duration::from_secs_f64(*v)))
            .collect();

        RustLocalManagerConfig {
            num_workers: self.num_workers,
            max_resources: self.max_resources.to_rust(),
            always_restart_worker: self.always_restart_worker,
            restart_predicate,
            retry_max_attempts: self.retry_max_attempts,
            print_pid: self.print_pid,
            memuse_log_path: self.memuse_log_path.clone(),
            stage_timeouts,
            low_resource_thresholds: self.low_resource_thresholds.to_rust(),
            resource_check_interval: Duration::from_secs_f64(self.resource_check_interval_secs),
            phase_status_log_intervals: self
                .phase_status_log_intervals_secs
                .iter()
                .map(|s| Duration::from_secs_f64(*s))
                .collect(),
            log_oom_watcher: self.log_oom_watcher,
            // Composes the memprofile sampler's output directory
            // when both opt-in pieces are present: the user passed
            // `--memprofile` (so `memprofile_enabled = true`) AND
            // there's a run-level `output_dir` to anchor the
            // `memprofile/` subdirectory under. Either piece
            // missing collapses to `None`, which disables the
            // sampler at construction time. The Rust core (in
            // `LocalManager`) consumes this single `Option<PathBuf>`
            // and never sees the two-flag form.
            output_dir: resolve_memprofile_dir(
                self.memprofile_enabled,
                self.output_dir.as_deref(),
            ),
        }
    }
}

/// Compose the memprofile output directory from the two Python-side
/// inputs. Single concern: combine `memprofile_enabled` and the
/// run-level `output_dir` into the `Option<PathBuf>` shape the
/// Rust-side `LocalManagerConfig` consumes. Lives at the PyO3
/// boundary because Python is the only producer of these two values
/// and the Rust side intentionally accepts only one.
pub(crate) fn resolve_memprofile_dir(
    memprofile_enabled: bool,
    output_dir: Option<&std::path::Path>,
) -> Option<std::path::PathBuf> {
    if !memprofile_enabled {
        return None;
    }
    output_dir.map(|p| p.join(dynrunner_manager_local::memprofile::config::MEMPROFILE_SUBDIR))
}

#[cfg(test)]
mod tests {
    use super::resolve_memprofile_dir;
    use std::path::PathBuf;

    #[test]
    fn memprofile_off_returns_none_regardless_of_output_dir() {
        assert!(resolve_memprofile_dir(false, None).is_none());
        assert!(
            resolve_memprofile_dir(false, Some(std::path::Path::new("/tmp/run"))).is_none()
        );
    }

    #[test]
    fn memprofile_on_without_output_dir_returns_none() {
        // The LocalManager path treats `None` as "profiling
        // disabled"; no sampler is constructed. The Python caller
        // is responsible for refusing to set memprofile when no
        // output_dir is available (since the operator's `--output`
        // is mandatory in practice, this branch only fires in
        // mis-wired tests).
        assert!(resolve_memprofile_dir(true, None).is_none());
    }

    #[test]
    fn memprofile_on_with_output_dir_appends_subdir() {
        let resolved = resolve_memprofile_dir(true, Some(std::path::Path::new("/tmp/run")))
            .expect("memprofile_enabled + output_dir must compose");
        assert_eq!(resolved, PathBuf::from("/tmp/run/memprofile"));
    }
}
