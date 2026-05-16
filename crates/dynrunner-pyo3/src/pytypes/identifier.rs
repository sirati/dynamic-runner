//! `PyBinaryIdentifier` pyclass + private identifier-key encoding.
//!
//! `RunnerIdentifier` is a 5-field key (`binary_name/platform/compiler/
//! version/opt_level`) joined with `/`. The `join_identifier`/
//! `split_identifier` pair is the encoding boundary — every From impl
//! that crosses `PyBinaryIdentifier <-> RunnerIdentifier` routes through
//! here, so the separator + part order are owned in one place.

use std::sync::Arc;

use pyo3::prelude::*;

use dynrunner_core::RunnerIdentifier;

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

pub(super) fn split_identifier(id: &str) -> (String, String, String, String, String) {
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
    pub(super) binary_name: String,
    #[pyo3(get)]
    pub(super) platform: String,
    #[pyo3(get)]
    pub(super) compiler: String,
    #[pyo3(get)]
    pub(super) version: String,
    #[pyo3(get)]
    pub(super) opt_level: String,
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

/// Resolve a Python identifier object to a `RunnerIdentifier`.
///
/// Prefers the structured-identifier interface (`obj.identifier_key()` —
/// any callable that returns a string) and falls back to the explicit
/// 5-field `BinaryIdentifier` shape (`binary_name`, `platform`, `compiler`,
/// `version`, `opt_level`).
pub(crate) fn identifier_from_pyobj(
    obj: &Bound<'_, PyAny>,
) -> PyResult<RunnerIdentifier> {
    if let Ok(key_attr) = obj.getattr("identifier_key") {
        let key: String = key_attr.call0()?.extract()?;
        return Ok(Arc::from(key.as_str()));
    }
    let binary_name: String = obj.getattr("binary_name")?.extract()?;
    let platform: String = obj.getattr("platform")?.extract()?;
    let compiler: String = obj.getattr("compiler")?.extract()?;
    let version: String = obj.getattr("version")?.extract()?;
    let opt_level: String = obj.getattr("opt_level")?.extract()?;
    Ok(join_identifier(
        &binary_name,
        &platform,
        &compiler,
        &version,
        &opt_level,
    ))
}
