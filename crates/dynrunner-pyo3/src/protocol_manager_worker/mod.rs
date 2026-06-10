//! Python adapter for the manager-worker wire codec.
//!
//! Single concern: bridge `dynrunner_protocol_manager_worker::codec`
//! (the canonical wire-format encoder/decoder for line-delimited text
//! frames sent over the worker's stdio / socket pipe) into Python so
//! `dynamic_runner.comm.proto.messages` can re-export it. No
//! protocol-shape logic lives here; every byte-string the Python side
//! produces comes out of the Rust codec, and every parse decision is
//! made by the Rust codec. The classes below are thin pyclass
//! wrappers whose only role is to give Python `isinstance(...)` and
//! attribute-access ergonomics over the Rust enum variants.
//!
//! The classes are wired as a submodule `protocol_manager_worker`
//! under `dynamic_runner._native`; the Python re-export module at
//! `python/dynamic_runner/comm/proto/messages.py` imports from
//! `dynamic_runner._native.protocol_manager_worker` so existing
//! callers (`from dynamic_runner.comm.proto import ...`) see the same
//! names without changes.
//!
//! Submodules carve the pyclass family by concern:
//!   - [`error_type`] — `PyErrorType` enum + core mapping.
//!   - [`commands`] — `Command` + two subclasses.
//!   - [`responses`] — `Response` + seven subclasses.
//!   - [`codec`] — Rust-codec wrappers (`command_into_py`,
//!     `response_into_py`, `decode_legacy_pickle`) + the
//!     Python-facing `parse_command` / `parse_response` pyfunctions.

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyModule};
use pyo3::wrap_pyfunction;

mod codec;
mod commands;
mod error_type;
mod responses;

use codec::{py_parse_command, py_parse_response};
use commands::{PyCommand, PyCustomMessageCommand, PyProcessBinaryCommand, PyStopCommand};
use error_type::PyErrorType;
use responses::{
    PyCustomMessageResponse, PyDoneResponse, PyErrorResponse, PyKeepaliveResponse,
    PyPhaseUpdateResponse, PyPickledErrorResponse, PyReadyResponse, PyResponse,
    PyWorkerExceptionResponse,
};

/// Turn a `Vec<u8>` (the codec's output) into a Python `bytes`
/// object owned by the caller's `'py` lifetime. Shared by the
/// `serialize()` methods on the command and response pyclasses.
pub(super) fn rust_bytes_to_py<'py>(py: Python<'py>, bytes: Vec<u8>) -> Bound<'py, PyBytes> {
    PyBytes::new(py, &bytes)
}

/// Register classes and functions on the
/// `dynamic_runner._native.protocol_manager_worker` submodule.
pub(crate) fn register(parent: &Bound<'_, PyModule>) -> PyResult<()> {
    let m = PyModule::new(parent.py(), "protocol_manager_worker")?;
    m.add_class::<PyErrorType>()?;
    m.add_class::<PyCommand>()?;
    m.add_class::<PyStopCommand>()?;
    m.add_class::<PyProcessBinaryCommand>()?;
    m.add_class::<PyCustomMessageCommand>()?;
    m.add_class::<PyResponse>()?;
    m.add_class::<PyDoneResponse>()?;
    m.add_class::<PyErrorResponse>()?;
    m.add_class::<PyPickledErrorResponse>()?;
    m.add_class::<PyWorkerExceptionResponse>()?;
    m.add_class::<PyPhaseUpdateResponse>()?;
    m.add_class::<PyKeepaliveResponse>()?;
    m.add_class::<PyReadyResponse>()?;
    m.add_class::<PyCustomMessageResponse>()?;
    m.add_function(wrap_pyfunction!(py_parse_command, &m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_response, &m)?)?;
    parent.add_submodule(&m)?;
    Ok(())
}
