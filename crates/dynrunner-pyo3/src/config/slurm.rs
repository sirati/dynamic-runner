use dynrunner_slurm::SlurmConfig;
use pyo3::prelude::*;

use crate::pytypes::PyPathStr;

/// Python binding for [`dynrunner_slurm::SlurmConfig`].
///
/// Single concern: expose the SLURM configuration dataclass to Python
/// with parity. Field names, defaults, path-derivation methods, and
/// `validate` semantics all mirror the Python `SlurmConfig` so the
/// thin-shim Python wrapper can re-export this class as `SlurmConfig`
/// with no behavioural drift.
///
/// Path methods (`get_image_dir`, `get_output_dir`, `get_log_dir`,
/// `get_srcbins_dir`, `get_srcbins_mount_source`) keep the Python
/// names rather than the Rust-internal `*_path()` shape — Python is
/// the consumer-facing surface and renaming there would force every
/// downstream caller to migrate.
///
/// `root_folder` and `prestaged_src_bins_path` are typed `PyPathStr`
/// so they accept either `str` or `os.PathLike` (matching the
/// pre-migration Python type hint `str | Path`); both surface to
/// Python as plain `str`.
#[pyclass(name = "SlurmConfig", get_all, set_all, from_py_object)]
#[derive(Clone, Debug)]
pub(crate) struct PySlurmConfig {
    root_folder: PyPathStr,
    image_subfolder: String,
    output_subfolder: String,
    log_subfolder: String,
    partition: String,
    time_limit: String,
    cpus_per_task: u32,
    memory_per_node: Option<String>,
    nodes: u32,
    notify_email: Option<String>,
    prestaged_src_bins_path: Option<PyPathStr>,
    signal_lead_seconds: u32,
}

impl Default for PySlurmConfig {
    fn default() -> Self {
        Self::from(SlurmConfig::default())
    }
}

impl From<SlurmConfig> for PySlurmConfig {
    fn from(c: SlurmConfig) -> Self {
        Self {
            root_folder: PyPathStr::from(c.root_folder),
            image_subfolder: c.image_subfolder,
            output_subfolder: c.output_subfolder,
            log_subfolder: c.log_subfolder,
            partition: c.partition,
            time_limit: c.time_limit,
            cpus_per_task: c.cpus_per_task,
            memory_per_node: c.memory_per_node,
            nodes: c.nodes,
            notify_email: c.notify_email,
            prestaged_src_bins_path: c.prestaged_src_bins_path.map(PyPathStr::from),
            signal_lead_seconds: c.signal_lead_seconds,
        }
    }
}

impl From<&PySlurmConfig> for SlurmConfig {
    fn from(c: &PySlurmConfig) -> Self {
        Self {
            root_folder: c.root_folder.as_str().to_owned(),
            image_subfolder: c.image_subfolder.clone(),
            output_subfolder: c.output_subfolder.clone(),
            log_subfolder: c.log_subfolder.clone(),
            partition: c.partition.clone(),
            time_limit: c.time_limit.clone(),
            cpus_per_task: c.cpus_per_task,
            memory_per_node: c.memory_per_node.clone(),
            nodes: c.nodes,
            notify_email: c.notify_email.clone(),
            prestaged_src_bins_path: c
                .prestaged_src_bins_path
                .as_ref()
                .map(|p| p.as_str().to_owned()),
            signal_lead_seconds: c.signal_lead_seconds,
        }
    }
}

#[pymethods]
impl PySlurmConfig {
    #[new]
    #[pyo3(signature = (
        root_folder,
        image_subfolder = None,
        output_subfolder = None,
        log_subfolder = None,
        partition = None,
        time_limit = None,
        cpus_per_task = None,
        memory_per_node = None,
        nodes = None,
        notify_email = None,
        prestaged_src_bins_path = None,
        signal_lead_seconds = None,
    ))]
    // PyO3 kwargs surface — collapsing to a builder is a separate
    // API refactor.
    #[allow(clippy::too_many_arguments)]
    fn new(
        root_folder: PyPathStr,
        image_subfolder: Option<String>,
        output_subfolder: Option<String>,
        log_subfolder: Option<String>,
        partition: Option<String>,
        time_limit: Option<String>,
        cpus_per_task: Option<u32>,
        memory_per_node: Option<String>,
        nodes: Option<u32>,
        notify_email: Option<String>,
        prestaged_src_bins_path: Option<PyPathStr>,
        signal_lead_seconds: Option<u32>,
    ) -> Self {
        let d = SlurmConfig::default();
        Self {
            root_folder,
            image_subfolder: image_subfolder.unwrap_or(d.image_subfolder),
            output_subfolder: output_subfolder.unwrap_or(d.output_subfolder),
            log_subfolder: log_subfolder.unwrap_or(d.log_subfolder),
            partition: partition.unwrap_or(d.partition),
            time_limit: time_limit.unwrap_or(d.time_limit),
            cpus_per_task: cpus_per_task.unwrap_or(d.cpus_per_task),
            // Python passes `memory_per_node=None` through verbatim
            // (default is None). Operators who want `--mem` set on
            // sbatch pass an explicit string here; everyone else gets
            // `None`, which makes `submit_job` omit `--mem` entirely.
            memory_per_node,
            nodes: nodes.unwrap_or(d.nodes),
            notify_email,
            prestaged_src_bins_path,
            signal_lead_seconds: signal_lead_seconds.unwrap_or(d.signal_lead_seconds),
        }
    }

    /// Full image directory path on the gateway (`<root>/<image_subfolder>`).
    fn get_image_dir(&self) -> String {
        SlurmConfig::from(self).image_path()
    }

    /// Full output directory path (`<root>/<output_subfolder>`).
    fn get_output_dir(&self) -> String {
        SlurmConfig::from(self).output_path()
    }

    /// Full log directory path (`<root>/<log_subfolder>`).
    fn get_log_dir(&self) -> String {
        SlurmConfig::from(self).log_path()
    }

    /// Full source-binaries directory path
    /// (`<root>/<image_subfolder>/srcbins`).
    fn get_srcbins_dir(&self) -> String {
        SlurmConfig::from(self).src_bins_path()
    }

    /// Path to bind-mount into the container at `/app/src-network`.
    /// Returns `prestaged_src_bins_path` (absolute, or resolved
    /// against `root_folder` when relative) if set; otherwise the
    /// per-run staging directory under the image dir.
    fn get_srcbins_mount_source(&self) -> String {
        SlurmConfig::from(self).srcbins_mount_source()
    }

    /// Validate the configuration. Raises `ValueError` on failure.
    ///
    /// `gateway` (when provided) must expose a `file_exists(path)`
    /// callable returning a truthy/falsy value; the suggested-path
    /// list in the error message uses `gateway.remote_home` when set.
    /// Pass `None` to skip the existence check (path-only validation).
    #[pyo3(signature = (gateway = None))]
    fn validate(&self, gateway: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
        let cfg = SlurmConfig::from(self);

        // No gateway, or the caller passed something that doesn't
        // implement `file_exists` → path-only validation. Matches the
        // Python `hasattr(gateway, "file_exists")` branch and keeps
        // the Rust core gateway-agnostic.
        let gw = match gateway {
            Some(gw) if gw.hasattr("file_exists").unwrap_or(false) => gw,
            _ => {
                return cfg
                    .validate(None, None)
                    .map_err(pyo3::exceptions::PyValueError::new_err);
            }
        };

        let remote_home: Option<String> = gw
            .getattr("remote_home")
            .ok()
            .and_then(|h| h.extract().ok());

        // Python errors raised from inside the closure are stashed
        // here so `validate` can short-circuit and surface them
        // verbatim — the Rust core's `root_exists` callback returns
        // `bool`, and propagating `PyErr` through it would force the
        // core to know about Python.
        let py_err: std::cell::RefCell<Option<PyErr>> = std::cell::RefCell::new(None);
        let exists = |path: &str| -> bool {
            match gw.call_method1("file_exists", (path,)) {
                Ok(v) => v.is_truthy().unwrap_or(false),
                Err(e) => {
                    py_err.borrow_mut().get_or_insert(e);
                    false
                }
            }
        };

        let result = cfg.validate(remote_home.as_deref(), Some(&exists));
        if let Some(e) = py_err.into_inner() {
            return Err(e);
        }
        result.map_err(pyo3::exceptions::PyValueError::new_err)
    }

    fn __repr__(&self) -> String {
        // `PyPathStr` derives `Debug` which would render as
        // `PyPathStr("…")`; project the inner `String` (and the
        // `Option<PyPathStr>` -> `Option<&String>`) so the repr
        // matches the historical `str | None` shape.
        format!(
            "SlurmConfig(root_folder={:?}, image_subfolder={:?}, \
             output_subfolder={:?}, log_subfolder={:?}, partition={:?}, \
             time_limit={:?}, cpus_per_task={}, memory_per_node={:?}, \
             nodes={}, notify_email={:?}, prestaged_src_bins_path={:?}, \
             signal_lead_seconds={})",
            self.root_folder.as_str(),
            self.image_subfolder,
            self.output_subfolder,
            self.log_subfolder,
            self.partition,
            self.time_limit,
            self.cpus_per_task,
            self.memory_per_node,
            self.nodes,
            self.notify_email,
            self.prestaged_src_bins_path.as_ref().map(PyPathStr::as_str),
            self.signal_lead_seconds,
        )
    }
}
