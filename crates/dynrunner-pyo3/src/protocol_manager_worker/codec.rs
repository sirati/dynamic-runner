//! Bridge from Rust codec to Python subclass instances.
//!
//! `command_into_py`/`response_into_py` wrap Rust enum values in the
//! matching pyclass so `isinstance(...)` continues to work; the
//! Python-facing `parse_command`/`parse_response` pyfunctions just
//! call into the codec and route through these wrappers.

use dynrunner_protocol_manager_worker::{Command as RustCommand, Response as RustResponse};
use dynrunner_protocol_manager_worker::codec::{
    parse_command as codec_parse_command, parse_response as codec_parse_response,
};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use super::commands::{PyCommand, PyProcessBinaryCommand, PyStopCommand};
use super::error_type::PyErrorType;
use super::responses::{
    PyDoneResponse, PyErrorResponse, PyKeepaliveResponse, PyPhaseUpdateResponse,
    PyPickledErrorResponse, PyReadyResponse, PyResponse, PyWorkerExceptionResponse,
};

// ---------------------------------------------------------------------------
// parse_command / parse_response — call into the Rust codec, then
// wrap the resulting enum value in the matching Python subclass so
// `isinstance(cmd, ProcessBinaryCommand)` continues to work.

/// Build a Python `Command` subclass instance from a Rust `Command`.
fn command_into_py(py: Python<'_>, cmd: RustCommand) -> PyResult<Py<PyAny>> {
    match cmd {
        RustCommand::Stop => Ok(Py::new(py, (PyStopCommand, PyCommand))?.into_any()),
        RustCommand::ProcessTask {
            relative_path,
            payload,
            resolved_path,
        } => Ok(Py::new(
            py,
            (
                PyProcessBinaryCommand {
                    relative_path,
                    payload,
                    resolved_path,
                },
                PyCommand,
            ),
        )?
        .into_any()),
    }
}

/// Build a Python `Response` subclass instance from a Rust `Response`.
/// The `error:pickle:` legacy shape arrives here as a `WorkerException`
/// with `exception_type == "LegacyPickledException"` — we route it to
/// `PickledErrorResponse` so the historical decode contract is
/// preserved at the surface (the field reconstruction itself happens
/// Python-side because pickle is a Python-only format; see the docstring
/// on `PickledErrorResponse.serialize`).
fn response_into_py(py: Python<'_>, resp: RustResponse) -> PyResult<Py<PyAny>> {
    match resp {
        RustResponse::Ready => Ok(Py::new(py, (PyReadyResponse, PyResponse))?.into_any()),
        RustResponse::Keepalive => Ok(Py::new(py, (PyKeepaliveResponse, PyResponse))?.into_any()),
        RustResponse::Done { result_data } => Ok(Py::new(
            py,
            (PyDoneResponse { result_data }, PyResponse),
        )?
        .into_any()),
        RustResponse::Error {
            error_type,
            message,
        } => {
            // `from_core` collapses unrecognised `ResourceExhausted`
            // kinds to `None`; the pre-refactor Python code defaulted
            // unknown wire tags to `Recoverable`, so preserve that.
            let py_et =
                PyErrorType::from_core(&error_type).unwrap_or(PyErrorType::Recoverable);
            Ok(Py::new(
                py,
                (
                    PyErrorResponse {
                        error_type: py_et,
                        error_message: message,
                    },
                    PyResponse,
                ),
            )?
            .into_any())
        }
        RustResponse::WorkerException {
            exception_type,
            message,
            traceback,
            error_type,
        } => {
            if exception_type == "LegacyPickledException" {
                // Rust codec hands the raw pickle bytes through the
                // `message` field. Python-side decode reconstructs the
                // structured type/message/traceback by unpickling — we
                // call into the consumer's pickle here because pickle
                // is a Python-only format. The Rust codec stops at
                // wire-shape recognition; richer Python-only decoding
                // sits above it.
                return decode_legacy_pickle(py, &message);
            }
            Ok(Py::new(
                py,
                (
                    PyWorkerExceptionResponse {
                        exception_type,
                        exception_message: message,
                        traceback_str: traceback,
                        error_type: error_type.as_ref().and_then(PyErrorType::from_core),
                    },
                    PyResponse,
                ),
            )?
            .into_any())
        }
        RustResponse::PhaseUpdate { phase_name } => Ok(Py::new(
            py,
            (PyPhaseUpdateResponse { phase_name }, PyResponse),
        )?
        .into_any()),
    }
}

/// Best-effort `pickle.loads` on the raw legacy bytes. Mirrors the
/// pre-refactor Python `parse_response` behaviour: on any decode
/// failure, fall back to an `ErrorResponse(RECOVERABLE, ...)` so a
/// malformed legacy frame doesn't crash the parser.
fn decode_legacy_pickle(py: Python<'_>, raw_message: &str) -> PyResult<Py<PyAny>> {
    let attempt = || -> PyResult<Py<PyAny>> {
        let pickle = py.import("pickle")?;
        // Python `parse_response` did `data[13:].encode('latin-1')`;
        // we have the post-prefix portion as a Rust `&str`. Re-encode
        // via latin-1 to mirror the historical round-trip; on a non-
        // round-trippable string this falls through to the recoverable-
        // error path below.
        let pickled_bytes_buf: Vec<u8> = raw_message.chars().map(|c| c as u8).collect();
        let pickled_bytes = PyBytes::new(py, &pickled_bytes_buf);
        let error_info = pickle.getattr("loads")?.call1((pickled_bytes,))?;
        let exception_type: String = error_info
            .call_method1("get", ("type", "Unknown"))?
            .extract()?;
        let exception_message: String = error_info
            .call_method1("get", ("message", "No message"))?
            .extract()?;
        let traceback_str: String = error_info
            .call_method1("get", ("traceback", "No traceback"))?
            .extract()?;
        Ok(Py::new(
            py,
            (
                PyPickledErrorResponse {
                    exception_type,
                    exception_message,
                    traceback_str,
                },
                PyResponse,
            ),
        )?
        .into_any())
    };
    match attempt() {
        Ok(obj) => Ok(obj),
        Err(_) => Ok(Py::new(
            py,
            (
                PyErrorResponse {
                    error_type: PyErrorType::Recoverable,
                    error_message: "Failed to unpickle error".to_owned(),
                },
                PyResponse,
            ),
        )?
        .into_any()),
    }
}

/// `parse_command(line: str) -> Command | None` — wrap the Rust
/// codec's parser, mapping the returned enum to the matching Python
/// subclass.
#[pyfunction]
#[pyo3(name = "parse_command")]
pub(crate) fn py_parse_command(py: Python<'_>, line: &str) -> PyResult<Option<Py<PyAny>>> {
    match codec_parse_command(line) {
        None => Ok(None),
        Some(cmd) => command_into_py(py, cmd).map(Some),
    }
}

/// `parse_response(line: str) -> Response | None` — wrap the Rust
/// codec's parser, mapping the returned enum to the matching Python
/// subclass.
#[pyfunction]
#[pyo3(name = "parse_response")]
pub(crate) fn py_parse_response(py: Python<'_>, line: &str) -> PyResult<Option<Py<PyAny>>> {
    match codec_parse_response(line) {
        None => Ok(None),
        Some(resp) => response_into_py(py, resp).map(Some),
    }
}
