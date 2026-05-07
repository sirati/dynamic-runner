//! Python binding for `dynrunner_publish::publish_one`.
//!
//! Single concern: surface the Rust crate's atomic stage→destination
//! publish to Python, mapping every `PublishError` variant onto a
//! single `PublishError` exception class. The Python layer reads the
//! string form of the error to decide what (if anything) to log; it
//! does not branch on variant identity.

use std::path::PathBuf;

use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;

create_exception!(
    _native,
    PublishError,
    PyException,
    "Atomic stage→destination publish failed."
);

#[pyfunction]
pub(crate) fn publish_one(
    src: PathBuf,
    dst: PathBuf,
    src_root: PathBuf,
) -> PyResult<()> {
    dynrunner_publish::publish_one(&src, &dst, &src_root)
        .map_err(|e| PublishError::new_err(e.to_string()))
}
