//! Command pyclasses: `Command`, `StopCommand`, `ProcessBinaryCommand`.
//!
//! Marker base + two concrete subclasses (mapping 1:1 onto Rust
//! `Command::{Stop, ProcessTask}`). Each `serialize()` delegates to
//! `codec::serialize_command`.

use dynrunner_protocol_manager_worker::Command as RustCommand;
use dynrunner_protocol_manager_worker::codec::serialize_command as codec_serialize_command;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use super::rust_bytes_to_py;

// ---------------------------------------------------------------------------
// Command + subclasses.
//
// `Command` is a marker base class with no fields — Python callers
// reach for `isinstance(cmd, Command)` and concrete subclasses carry
// the variant payload. Each subclass holds the Rust enum value as a
// private field and delegates `serialize()` straight to
// `codec::serialize_command`.

/// Base class for manager→worker commands. Concrete shapes are
/// `StopCommand` and `ProcessBinaryCommand`. Holds no state of its
/// own; subclasses store the Rust enum value.
#[pyclass(name = "Command", subclass)]
pub(crate) struct PyCommand;

#[pymethods]
impl PyCommand {
    #[new]
    fn new() -> Self {
        Self
    }

    fn serialize(&self) -> PyResult<Py<PyBytes>> {
        Err(pyo3::exceptions::PyNotImplementedError::new_err(
            "Command.serialize() is abstract; use a subclass",
        ))
    }
}

#[pyclass(name = "StopCommand", extends = PyCommand)]
pub(crate) struct PyStopCommand;

#[pymethods]
impl PyStopCommand {
    #[new]
    fn new() -> (Self, PyCommand) {
        (Self, PyCommand)
    }

    fn serialize<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        rust_bytes_to_py(py, codec_serialize_command(&RustCommand::Stop))
    }
}

/// `relative_path`, `payload`, `resolved_path` mirror the Rust
/// `Command::ProcessTask` variant 1:1. Python keeps the historical
/// class name (`ProcessBinaryCommand`) so existing imports continue
/// to resolve.
#[pyclass(name = "ProcessBinaryCommand", extends = PyCommand)]
pub(crate) struct PyProcessBinaryCommand {
    #[pyo3(get, set)]
    pub(super) relative_path: String,
    #[pyo3(get, set)]
    pub(super) payload: Option<String>,
    #[pyo3(get, set)]
    pub(super) resolved_path: Option<String>,
}

#[pymethods]
impl PyProcessBinaryCommand {
    #[new]
    #[pyo3(signature = (relative_path, payload=None, resolved_path=None))]
    fn new(
        relative_path: String,
        payload: Option<String>,
        resolved_path: Option<String>,
    ) -> (Self, PyCommand) {
        (
            Self {
                relative_path,
                payload,
                resolved_path,
            },
            PyCommand,
        )
    }

    fn serialize<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        let cmd = RustCommand::ProcessTask {
            relative_path: self.relative_path.clone(),
            payload: self.payload.clone(),
            resolved_path: self.resolved_path.clone(),
            // `predecessor_outputs` will become a JSON-string field
            // on this pyclass once the Python-side `Task.predecessor_outputs`
            // wiring lands; until then the bridge always serialises
            // an empty map.
            predecessor_outputs: std::collections::BTreeMap::new(),
        };
        rust_bytes_to_py(py, codec_serialize_command(&cmd))
    }
}
