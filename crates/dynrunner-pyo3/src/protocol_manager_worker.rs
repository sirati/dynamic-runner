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

use dynrunner_core::{ErrorType as CoreErrorType, ResourceKind};
use dynrunner_protocol_manager_worker::codec::{
    parse_command as codec_parse_command, parse_response as codec_parse_response,
    serialize_command as codec_serialize_command, serialize_response as codec_serialize_response,
};
use dynrunner_protocol_manager_worker::{Command as RustCommand, Response as RustResponse};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyModule};
use pyo3::wrap_pyfunction;

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
    fn to_core(self) -> CoreErrorType {
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
    fn from_core(et: &CoreErrorType) -> Option<Self> {
        match et {
            CoreErrorType::ResourceExhausted(kind) if kind.as_str() == "memory" => {
                Some(PyErrorType::OutOfMemory)
            }
            CoreErrorType::ResourceExhausted(_) => None,
            CoreErrorType::NonRecoverable => Some(PyErrorType::NonRecoverable),
            CoreErrorType::Recoverable => Some(PyErrorType::Recoverable),
        }
    }
}

// ---------------------------------------------------------------------------
// Helper: turn a `Vec<u8>` (the codec's output) into a Python `bytes`
// object owned by the caller's `'py` lifetime.

fn rust_bytes_to_py<'py>(py: Python<'py>, bytes: Vec<u8>) -> Bound<'py, PyBytes> {
    PyBytes::new(py, &bytes)
}

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
    relative_path: String,
    #[pyo3(get, set)]
    payload: Option<String>,
    #[pyo3(get, set)]
    resolved_path: Option<String>,
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
        };
        rust_bytes_to_py(py, codec_serialize_command(&cmd))
    }
}

// ---------------------------------------------------------------------------
// Response + subclasses.

/// Base class for worker→manager responses. Concrete shapes are the
/// seven subclasses below; no own state.
#[pyclass(name = "Response", subclass)]
pub(crate) struct PyResponse;

#[pymethods]
impl PyResponse {
    #[new]
    fn new() -> Self {
        Self
    }

    fn serialize(&self) -> PyResult<Py<PyBytes>> {
        Err(pyo3::exceptions::PyNotImplementedError::new_err(
            "Response.serialize() is abstract; use a subclass",
        ))
    }
}

/// Done-response payload bytes stored as `Vec<u8>` (the
/// codec-native shape) and converted to a Python `bytes` on
/// read. Avoids cloning a `Py<PyBytes>` (which doesn't impl Clone)
/// and keeps `result_data` round-trippable through `parse_response`.
#[pyclass(name = "DoneResponse", extends = PyResponse)]
pub(crate) struct PyDoneResponse {
    result_data: Option<Vec<u8>>,
}

#[pymethods]
impl PyDoneResponse {
    #[new]
    #[pyo3(signature = (result_data=None))]
    fn new(py: Python<'_>, result_data: Option<Bound<'_, PyAny>>) -> PyResult<(Self, PyResponse)> {
        let data = match result_data {
            None => None,
            Some(obj) if obj.is_none() => None,
            Some(obj) => {
                // Accept `bytes` directly; the historical Python
                // class was a dataclass with `Optional[bytes]` so the
                // caller surface allows None or a bytes-like object.
                let bytes: &Bound<'_, PyBytes> = obj.cast()?;
                Some(bytes.as_bytes().to_vec())
            }
        };
        let _ = py;
        Ok((Self { result_data: data }, PyResponse))
    }

    #[getter]
    fn result_data<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyBytes>> {
        self.result_data.as_ref().map(|b| PyBytes::new(py, b))
    }

    #[setter]
    fn set_result_data(&mut self, value: Option<Bound<'_, PyAny>>) -> PyResult<()> {
        self.result_data = match value {
            None => None,
            Some(obj) if obj.is_none() => None,
            Some(obj) => {
                let bytes: &Bound<'_, PyBytes> = obj.cast()?;
                Some(bytes.as_bytes().to_vec())
            }
        };
        Ok(())
    }

    fn serialize<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        let resp = RustResponse::Done {
            result_data: self.result_data.clone(),
        };
        rust_bytes_to_py(py, codec_serialize_response(&resp))
    }
}

#[pyclass(name = "ErrorResponse", extends = PyResponse)]
pub(crate) struct PyErrorResponse {
    #[pyo3(get, set)]
    error_type: PyErrorType,
    #[pyo3(get, set)]
    error_message: String,
}

#[pymethods]
impl PyErrorResponse {
    #[new]
    fn new(error_type: PyErrorType, error_message: String) -> (Self, PyResponse) {
        (
            Self {
                error_type,
                error_message,
            },
            PyResponse,
        )
    }

    fn serialize<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        let resp = RustResponse::Error {
            error_type: self.error_type.to_core(),
            message: self.error_message.clone(),
        };
        rust_bytes_to_py(py, codec_serialize_response(&resp))
    }
}

/// Legacy pickled-error wire form. The Rust codec only *parses* this
/// form (mapping it onto `Response::WorkerException` with the special
/// type tag `LegacyPickledException`); it never emits `error:pickle:`.
/// This class is preserved for callers that may construct it
/// directly — its `serialize()` therefore lives in Python (the only
/// remaining "logic" the bridge cannot avoid, because pickle is a
/// Python-only format). The constructor + `parse_response` paths
/// route the legacy form to this class on the way back from Rust.
#[pyclass(name = "PickledErrorResponse", extends = PyResponse)]
pub(crate) struct PyPickledErrorResponse {
    #[pyo3(get, set)]
    exception_type: String,
    #[pyo3(get, set)]
    exception_message: String,
    #[pyo3(get, set)]
    traceback_str: String,
}

#[pymethods]
impl PyPickledErrorResponse {
    #[new]
    fn new(
        exception_type: String,
        exception_message: String,
        traceback_str: String,
    ) -> (Self, PyResponse) {
        (
            Self {
                exception_type,
                exception_message,
                traceback_str,
            },
            PyResponse,
        )
    }

    /// Re-emit the legacy `error:pickle:<pickle-bytes>\n` wire shape.
    /// Encoded directly here (pickle is Python-only); the Rust codec
    /// never emits this form. Kept for backward compatibility with
    /// callers that construct `PickledErrorResponse` directly.
    fn serialize<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let pickle = py.import("pickle")?;
        let dict = PyDict::new(py);
        dict.set_item("type", &self.exception_type)?;
        dict.set_item("message", &self.exception_message)?;
        dict.set_item("traceback", &self.traceback_str)?;
        let pickled = pickle.getattr("dumps")?.call1((dict,))?;
        let pickled_bytes: &Bound<'_, PyBytes> = pickled.cast()?;
        let body = pickled_bytes.as_bytes();
        let mut out: Vec<u8> = b"error:pickle:".to_vec();
        out.extend_from_slice(body);
        out.push(b'\n');
        Ok(PyBytes::new(py, &out))
    }
}

#[pyclass(name = "WorkerExceptionResponse", extends = PyResponse)]
pub(crate) struct PyWorkerExceptionResponse {
    #[pyo3(get, set)]
    exception_type: String,
    #[pyo3(get, set)]
    exception_message: String,
    #[pyo3(get, set)]
    traceback_str: String,
    #[pyo3(get, set)]
    error_type: Option<PyErrorType>,
}

#[pymethods]
impl PyWorkerExceptionResponse {
    #[new]
    #[pyo3(signature = (exception_type, exception_message, traceback_str, error_type=None))]
    fn new(
        exception_type: String,
        exception_message: String,
        traceback_str: String,
        error_type: Option<PyErrorType>,
    ) -> (Self, PyResponse) {
        (
            Self {
                exception_type,
                exception_message,
                traceback_str,
                error_type,
            },
            PyResponse,
        )
    }

    fn serialize<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        let resp = RustResponse::WorkerException {
            exception_type: self.exception_type.clone(),
            message: self.exception_message.clone(),
            traceback: self.traceback_str.clone(),
            error_type: self.error_type.map(|e| e.to_core()),
        };
        rust_bytes_to_py(py, codec_serialize_response(&resp))
    }
}

#[pyclass(name = "PhaseUpdateResponse", extends = PyResponse)]
pub(crate) struct PyPhaseUpdateResponse {
    #[pyo3(get, set)]
    phase_name: String,
}

#[pymethods]
impl PyPhaseUpdateResponse {
    #[new]
    fn new(phase_name: String) -> (Self, PyResponse) {
        (Self { phase_name }, PyResponse)
    }

    fn serialize<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        let resp = RustResponse::PhaseUpdate {
            phase_name: self.phase_name.clone(),
        };
        rust_bytes_to_py(py, codec_serialize_response(&resp))
    }
}

#[pyclass(name = "KeepaliveResponse", extends = PyResponse)]
pub(crate) struct PyKeepaliveResponse;

#[pymethods]
impl PyKeepaliveResponse {
    #[new]
    fn new() -> (Self, PyResponse) {
        (Self, PyResponse)
    }

    fn serialize<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        rust_bytes_to_py(py, codec_serialize_response(&RustResponse::Keepalive))
    }
}

#[pyclass(name = "ReadyResponse", extends = PyResponse)]
pub(crate) struct PyReadyResponse;

#[pymethods]
impl PyReadyResponse {
    #[new]
    fn new() -> (Self, PyResponse) {
        (Self, PyResponse)
    }

    fn serialize<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        rust_bytes_to_py(py, codec_serialize_response(&RustResponse::Ready))
    }
}

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

// ---------------------------------------------------------------------------
// Submodule registration.

/// Register classes and functions on the
/// `dynamic_runner._native.protocol_manager_worker` submodule.
pub(crate) fn register(parent: &Bound<'_, PyModule>) -> PyResult<()> {
    let m = PyModule::new(parent.py(), "protocol_manager_worker")?;
    m.add_class::<PyErrorType>()?;
    m.add_class::<PyCommand>()?;
    m.add_class::<PyStopCommand>()?;
    m.add_class::<PyProcessBinaryCommand>()?;
    m.add_class::<PyResponse>()?;
    m.add_class::<PyDoneResponse>()?;
    m.add_class::<PyErrorResponse>()?;
    m.add_class::<PyPickledErrorResponse>()?;
    m.add_class::<PyWorkerExceptionResponse>()?;
    m.add_class::<PyPhaseUpdateResponse>()?;
    m.add_class::<PyKeepaliveResponse>()?;
    m.add_class::<PyReadyResponse>()?;
    m.add_function(wrap_pyfunction!(py_parse_command, &m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_response, &m)?)?;
    parent.add_submodule(&m)?;
    Ok(())
}
