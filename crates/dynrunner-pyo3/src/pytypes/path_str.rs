//! FFI-boundary wrapper for `str | os.PathLike` Python parameters.

use std::convert::Infallible;
use std::path::PathBuf;

use pyo3::conversion::IntoPyObject;
use pyo3::prelude::*;
use pyo3::types::PyString;
use pyo3::{Borrowed, FromPyObject};

/// FFI-boundary wrapper that accepts either a Python `str` or any
/// `os.PathLike` (e.g. `pathlib.Path`) and stores the resolved path
/// as a UTF-8 `String`.
///
/// Pre-Rust-migration the Python `SlurmConfig` dataclass typed
/// `root_folder: str | Path`; downstream consumers relied on the
/// `Path` arm. The PyO3 `String` extractor only accepts Python `str`,
/// so wrapping the field in `PyPathStr` restores the original
/// contract without forcing every config field to know about path
/// coercion. Use it for any pyclass field whose Python type signature
/// is `str | os.PathLike`.
#[derive(Clone, Debug, Default)]
pub(crate) struct PyPathStr(pub(crate) String);

impl PyPathStr {
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for PyPathStr {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<PyPathStr> for String {
    fn from(p: PyPathStr) -> Self {
        p.0
    }
}

impl FromPyObject<'_, '_> for PyPathStr {
    type Error = PyErr;

    fn extract(ob: Borrowed<'_, '_, PyAny>) -> Result<Self, Self::Error> {
        // `str` fast path: the common case is a plain Python string,
        // so try direct extraction before paying for the `os.fspath`
        // round-trip used by `PathBuf::extract`.
        if let Ok(s) = ob.extract::<String>() {
            return Ok(Self(s));
        }
        // Fallback: anything implementing `os.PathLike` (e.g.
        // `pathlib.Path`). `PathBuf::extract` calls `os.fspath()`
        // under the hood; the resulting `OsString` is converted to
        // `String` via `to_string_lossy`, mirroring the existing
        // `pytypes.rs` precedent (line 190) for path-to-string
        // coercion at the Python boundary.
        let path: PathBuf = ob.extract()?;
        Ok(Self(path.to_string_lossy().into_owned()))
    }
}

impl<'py> IntoPyObject<'py> for PyPathStr {
    type Target = PyString;
    type Output = Bound<'py, PyString>;
    type Error = Infallible;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(PyString::new(py, &self.0))
    }
}

impl<'py> IntoPyObject<'py> for &PyPathStr {
    type Target = PyString;
    type Output = Bound<'py, PyString>;
    type Error = Infallible;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(PyString::new(py, &self.0))
    }
}
