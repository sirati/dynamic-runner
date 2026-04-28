use std::path::PathBuf;
use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyList;

use db_comm_api_base::{BinaryInfo, RunnerIdentifier};

/// Canonical identifier-key separator. Matches the Python
/// `TokenizerIdentifier.identifier_key()` join order
/// `"binary_name/platform/compiler/version/opt_level"`. Sibling task
/// packages can compose their own key with the same separator.
const ID_SEP: char = '/';

fn join_identifier(
    binary_name: &str,
    platform: &str,
    compiler: &str,
    version: &str,
    opt_level: &str,
) -> RunnerIdentifier {
    Arc::from(
        format!(
            "{binary_name}{ID_SEP}{platform}{ID_SEP}{compiler}{ID_SEP}{version}{ID_SEP}{opt_level}"
        )
        .as_str(),
    )
}

fn split_identifier(id: &str) -> (String, String, String, String, String) {
    let mut parts = id.splitn(5, ID_SEP);
    (
        parts.next().unwrap_or("").to_owned(),
        parts.next().unwrap_or("").to_owned(),
        parts.next().unwrap_or("").to_owned(),
        parts.next().unwrap_or("").to_owned(),
        parts.next().unwrap_or("").to_owned(),
    )
}

/// Python-visible wrapper for BinaryIdentifier.
#[pyclass(name = "BinaryIdentifier", from_py_object)]
#[derive(Clone)]
pub(crate) struct PyBinaryIdentifier {
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

impl From<&PyBinaryIdentifier> for RunnerIdentifier {
    fn from(py: &PyBinaryIdentifier) -> Self {
        join_identifier(
            &py.binary_name,
            &py.platform,
            &py.compiler,
            &py.version,
            &py.opt_level,
        )
    }
}

/// Python-visible wrapper for BinaryInfo.
#[pyclass(name = "BinaryInfo", from_py_object)]
#[derive(Clone)]
pub(crate) struct PyBinaryInfo {
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

impl From<&PyBinaryInfo> for BinaryInfo<RunnerIdentifier> {
    fn from(py: &PyBinaryInfo) -> Self {
        BinaryInfo {
            path: PathBuf::from(&py.path),
            size: py.size,
            identifier: RunnerIdentifier::from(&py.identifier),
        }
    }
}

impl From<&BinaryInfo<RunnerIdentifier>> for PyBinaryInfo {
    fn from(bi: &BinaryInfo<RunnerIdentifier>) -> Self {
        let (binary_name, platform, compiler, version, opt_level) =
            split_identifier(&bi.identifier);
        PyBinaryInfo {
            path: bi.path.to_string_lossy().into_owned(),
            size: bi.size,
            identifier: PyBinaryIdentifier {
                binary_name,
                platform,
                compiler,
                version,
                opt_level,
            },
        }
    }
}

/// Python-visible processing stats.
#[pyclass(name = "ProcessingStats")]
pub(crate) struct PyProcessingStats {
    #[pyo3(get)]
    pub(crate) completed: u32,
    #[pyo3(get)]
    pub(crate) total: u32,
    #[pyo3(get)]
    pub(crate) errored: u32,
    #[pyo3(get)]
    pub(crate) skipped: u32,
}

/// Python-visible failed task.
#[pyclass(name = "FailedTask")]
pub(crate) struct PyFailedTask {
    #[pyo3(get)]
    pub(crate) binary: PyBinaryInfo,
    #[pyo3(get)]
    pub(crate) error_type: String,
    #[pyo3(get)]
    pub(crate) error_message: String,
}

pub(crate) fn extract_binaries(
    binaries: &Bound<'_, PyList>,
) -> PyResult<Vec<BinaryInfo<RunnerIdentifier>>> {
    binaries
        .iter()
        .map(|item| {
            let path_obj = item.getattr("path")?;
            let path: String = path_obj.str()?.to_string();
            let size: u64 = item.getattr("size")?.extract()?;
            let ident = item.getattr("identifier")?;

            // Prefer the structured-identifier interface (`identifier_key()`)
            // when the Python identifier is a TokenizerIdentifier dataclass
            // or compatible. Fall back to PyBinaryIdentifier's 5 explicit
            // fields for backward compat.
            let identifier: RunnerIdentifier = if let Ok(key_attr) = ident.getattr("identifier_key")
            {
                let key: String = key_attr.call0()?.extract()?;
                Arc::from(key.as_str())
            } else {
                let binary_name: String = ident.getattr("binary_name")?.extract()?;
                let platform: String = ident.getattr("platform")?.extract()?;
                let compiler: String = ident.getattr("compiler")?.extract()?;
                let version: String = ident.getattr("version")?.extract()?;
                let opt_level: String = ident.getattr("opt_level")?.extract()?;
                join_identifier(&binary_name, &platform, &compiler, &version, &opt_level)
            };

            Ok(BinaryInfo {
                path: PathBuf::from(path),
                size,
                identifier,
            })
        })
        .collect()
}

