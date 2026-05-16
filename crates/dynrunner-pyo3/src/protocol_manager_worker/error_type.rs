//! `PyErrorType` — Python-visible 3-value enum mirroring the legacy
//! `messages.ErrorType` shape with `to_core`/`from_core` mappings
//! onto `dynrunner_core::ErrorType`. The Rust core has a richer
//! `ResourceExhausted(ResourceKind)` variant; the Python enum
//! predates that expansion and only knows the three historical wire
//! tags, so conversion uses `wire_value` / `from_wire` to keep the
//! legacy `oom` shorthand round-tripping correctly.

use dynrunner_core::{ErrorType as CoreErrorType, ResourceKind};
use pyo3::prelude::*;

// ---------------------------------------------------------------------------
// ErrorType — Python-visible 3-value enum mirroring the legacy
// `messages.ErrorType` shape. The Rust `dynrunner_core::ErrorType`
// has a richer `ResourceExhausted(ResourceKind)` variant; the
// Python enum predates that expansion and only knows the three
// historical wire tags. Conversion uses `wire_value` / `from_wire`
// so the legacy `oom` shorthand round-trips correctly.

/// Mirror of the historical Python `ErrorType` enum. Three named
/// constants; the Python class exposes them as `ErrorType.OUT_OF_MEMORY`,
/// `ErrorType.NON_RECOVERABLE`, `ErrorType.RECOVERABLE` and accepts a
/// constructor argument matching the wire string for symmetry with the
/// pre-refactor `enum.Enum`-based shape.
#[pyclass(name = "ErrorType", eq, eq_int, from_py_object)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PyErrorType {
    OutOfMemory,
    NonRecoverable,
    Recoverable,
}

#[pymethods]
impl PyErrorType {
    /// Wire-string value. Matches the pre-refactor enum's `.value`
    /// attribute (``"oom"`` / ``"non_recoverable"`` / ``"recoverable"``)
    /// so any caller that reached for `.value` still gets the same
    /// string.
    #[getter]
    fn value(&self) -> &'static str {
        match self {
            PyErrorType::OutOfMemory => "oom",
            PyErrorType::NonRecoverable => "non_recoverable",
            PyErrorType::Recoverable => "recoverable",
        }
    }

    /// `ErrorType._from_value("oom")` — preserves the `enum.Enum`-style
    /// "construct from value" convenience. The Python re-export module
    /// exposes this as a classmethod-style helper so legacy callers
    /// reaching for `ErrorType(wire_value)` still resolve.
    #[staticmethod]
    fn _from_value(value: &str) -> PyResult<Self> {
        match value {
            "oom" => Ok(PyErrorType::OutOfMemory),
            "non_recoverable" => Ok(PyErrorType::NonRecoverable),
            "recoverable" => Ok(PyErrorType::Recoverable),
            other => Err(pyo3::exceptions::PyValueError::new_err(format!(
                "{:?} is not a valid ErrorType",
                other
            ))),
        }
    }
}

impl PyErrorType {
    pub(super) fn to_core(self) -> CoreErrorType {
        match self {
            PyErrorType::OutOfMemory => CoreErrorType::ResourceExhausted(ResourceKind::memory()),
            PyErrorType::NonRecoverable => CoreErrorType::NonRecoverable,
            PyErrorType::Recoverable => CoreErrorType::Recoverable,
        }
    }

    /// Map a `dynrunner_core::ErrorType` back to the 3-variant Python
    /// enum. Non-memory `ResourceExhausted` kinds collapse to `None`
    /// because the Python enum has no representation for them — same
    /// failure mode as `ErrorType(wire_value)` would have produced
    /// pre-refactor when the wire value wasn't one of the three
    /// recognised tags.
    pub(super) fn from_core(et: &CoreErrorType) -> Option<Self> {
        match et {
            CoreErrorType::ResourceExhausted(kind) if kind.as_str() == "memory" => {
                Some(PyErrorType::OutOfMemory)
            }
            CoreErrorType::ResourceExhausted(_) => None,
            CoreErrorType::NonRecoverable => Some(PyErrorType::NonRecoverable),
            CoreErrorType::Recoverable => Some(PyErrorType::Recoverable),
            // `Unfulfillable` has no representation in the legacy
            // 3-variant Python enum. Returning `None` mirrors how
            // non-memory `ResourceExhausted` collapses — callers that
            // unwrap-or to a default (see `unwrap_or(Recoverable)` at
            // line ~516) will get the same conservative fallback. A
            // future Python-side enum extension can plumb this through
            // properly; for the wire-protocol commit it stays out of
            // the legacy bridge.
            CoreErrorType::Unfulfillable { .. } => None,
        }
    }
}
