//! Command pyclasses: `Command`, `StopCommand`, `ProcessBinaryCommand`.
//!
//! Marker base + two concrete subclasses (mapping 1:1 onto Rust
//! `Command::{Stop, ProcessTask}`). Each `serialize()` delegates to
//! `codec::serialize_command`.

use std::collections::BTreeMap;

use dynrunner_core::TaskOutputs;
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

/// `relative_path`, `payload`, `resolved_path`, `predecessor_outputs_json`
/// mirror the Rust `Command::ProcessTask` variant 1:1. Python keeps the
/// historical class name (`ProcessBinaryCommand`) so existing imports
/// continue to resolve.
#[pyclass(name = "ProcessBinaryCommand", extends = PyCommand)]
pub(crate) struct PyProcessBinaryCommand {
    #[pyo3(get, set)]
    pub(super) relative_path: String,
    #[pyo3(get, set)]
    pub(super) payload: Option<String>,
    #[pyo3(get, set)]
    pub(super) resolved_path: Option<String>,
    /// JSON-serialized `BTreeMap<String, TaskOutputs>` carrying the
    /// predecessor task outputs gathered by the dispatcher (the
    /// `Command::ProcessTask.predecessor_outputs` field from Phase 2c).
    ///
    /// Bridged as a string (not pythonized) for the same reason as
    /// `PyTaskInfo::payload_json`: it lets the boundary stay free of
    /// pyclasses for `TaskOutputs` / `ResultValue`, which are pure
    /// serde-derived data types.
    ///
    /// Python parses with `json.loads(...)`. The documented shape is:
    /// `{predecessor_task_id: {output_key: {"kind": "inline"|"file",
    /// "value": str}}}` — the inner adjacent-tagging is fixed by the
    /// `ResultValue` serde attribute (`tag = "kind"`, `content =
    /// "value"`) and must not drift.
    ///
    /// For an empty `predecessor_outputs` map this field carries an
    /// empty JSON object (`"{}"`), NOT an empty string — `json.loads`
    /// on the Python side always returns a `dict`.
    #[pyo3(get, set)]
    pub(super) predecessor_outputs_json: String,
}

#[pymethods]
impl PyProcessBinaryCommand {
    #[new]
    #[pyo3(signature = (
        relative_path,
        payload=None,
        resolved_path=None,
        predecessor_outputs_json="{}".to_string(),
    ))]
    fn new(
        relative_path: String,
        payload: Option<String>,
        resolved_path: Option<String>,
        predecessor_outputs_json: String,
    ) -> (Self, PyCommand) {
        (
            Self {
                relative_path,
                payload,
                resolved_path,
                predecessor_outputs_json,
            },
            PyCommand,
        )
    }

    fn serialize<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        // Reverse path: parse the JSON string back into the Rust map.
        // `TaskOutputs` is a serde-derived type with adjacent tagging
        // (`{"kind","value"}`); on any decode failure we fall back to
        // an empty map and warn — the historical wire frame stays well-
        // formed and we don't crash the codec on a malformed pyclass.
        let predecessor_outputs: BTreeMap<String, TaskOutputs> =
            serde_json::from_str(&self.predecessor_outputs_json).unwrap_or_else(|err| {
                tracing::warn!(
                    error = %err,
                    json = %self.predecessor_outputs_json,
                    "PyProcessBinaryCommand.predecessor_outputs_json failed to deserialise; \
                     falling back to empty map",
                );
                BTreeMap::new()
            });
        let cmd = RustCommand::ProcessTask {
            relative_path: self.relative_path.clone(),
            payload: self.payload.clone(),
            resolved_path: self.resolved_path.clone(),
            predecessor_outputs,
        };
        rust_bytes_to_py(py, codec_serialize_command(&cmd))
    }
}
