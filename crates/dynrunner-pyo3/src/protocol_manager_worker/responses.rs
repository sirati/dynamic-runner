//! Response pyclasses: `Response` + seven concrete subclasses.
//!
//! Marker base + concrete subclasses mapping 1:1 onto Rust
//! `Response::{Ready, Keepalive, Done, Error, WorkerException,
//! PhaseUpdate}`. The eighth class (`PickledErrorResponse`) is a
//! Python-only legacy form whose `serialize()` runs the pickle dump
//! in-Python because pickle is not a Rust-codec concern.

use dynrunner_protocol_manager_worker::Response as RustResponse;
use dynrunner_protocol_manager_worker::codec::serialize_response as codec_serialize_response;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};

use super::error_type::PyErrorType;
use super::rust_bytes_to_py;

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
    pub(super) result_data: Option<Vec<u8>>,
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
    pub(super) error_type: PyErrorType,
    #[pyo3(get, set)]
    pub(super) error_message: String,
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
    pub(super) exception_type: String,
    #[pyo3(get, set)]
    pub(super) exception_message: String,
    #[pyo3(get, set)]
    pub(super) traceback_str: String,
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
    pub(super) exception_type: String,
    #[pyo3(get, set)]
    pub(super) exception_message: String,
    #[pyo3(get, set)]
    pub(super) traceback_str: String,
    #[pyo3(get, set)]
    pub(super) error_type: Option<PyErrorType>,
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
    pub(super) phase_name: String,
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
