use std::path::PathBuf;

use pyo3::prelude::*;
use pyo3::types::PyList;

use db_comm_api_base::BinaryInfo;

use crate::identifier::TokenizerIdentifier;

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
) -> PyResult<Vec<BinaryInfo<TokenizerIdentifier>>> {
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
