//! Bridge from Rust codec to Python subclass instances.
//!
//! `command_into_py`/`response_into_py` wrap Rust enum values in the
//! matching pyclass so `isinstance(...)` continues to work; the
//! Python-facing `parse_command`/`parse_response` pyfunctions just
//! call into the codec and route through these wrappers.

use dynrunner_protocol_manager_worker::codec::{
    parse_command as codec_parse_command, parse_response as codec_parse_response,
};
use dynrunner_protocol_manager_worker::{Command as RustCommand, Response as RustResponse};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use super::commands::{PyCommand, PyCustomMessageCommand, PyProcessBinaryCommand, PyStopCommand};
use super::error_type::PyErrorType;
use super::responses::{
    PyCustomMessageResponse, PyDoneResponse, PyErrorResponse, PyKeepaliveResponse,
    PyPhaseUpdateResponse, PyPickledErrorResponse, PyReadyResponse, PyResponse,
    PyWorkerExceptionResponse,
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
            predecessor_outputs,
        } => {
            // `TaskOutputs` is a serde-derived type and the outer map is
            // a `BTreeMap<String, _>` (string keys). `serde_json::to_string`
            // only fails on i/o errors or non-string map keys, neither of
            // which apply — the `expect` is sound. Empty map serialises
            // to `"{}"` (symmetric with `json.loads("{}") == {}` on the
            // Python side).
            let predecessor_outputs_json = serde_json::to_string(&predecessor_outputs)
                .expect("BTreeMap<String, TaskOutputs> always serialises to JSON");
            Ok(Py::new(
                py,
                (
                    PyProcessBinaryCommand {
                        relative_path,
                        payload,
                        resolved_path,
                        predecessor_outputs_json,
                    },
                    PyCommand,
                ),
            )?
            .into_any())
        }
        RustCommand::Custom { topic, data } => Ok(Py::new(
            py,
            (PyCustomMessageCommand { topic, data }, PyCommand),
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
        RustResponse::Done { result_data } => {
            Ok(Py::new(py, (PyDoneResponse { result_data }, PyResponse))?.into_any())
        }
        RustResponse::Error {
            error_type,
            message,
        } => {
            // `from_core` collapses unrecognised `ResourceExhausted`
            // kinds to `None`; the pre-refactor Python code defaulted
            // unknown wire tags to `Recoverable`, so preserve that.
            let py_et = PyErrorType::from_core(&error_type).unwrap_or(PyErrorType::Recoverable);
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
        RustResponse::PhaseUpdate { phase_name } => {
            Ok(Py::new(py, (PyPhaseUpdateResponse { phase_name }, PyResponse))?.into_any())
        }
        RustResponse::Custom { topic, data } => Ok(Py::new(
            py,
            (PyCustomMessageResponse { topic, data }, PyResponse),
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

#[cfg(test)]
#[cfg(feature = "test-with-python")]
mod tests {
    //! Contract tests for the `predecessor_outputs` PyO3 bridge —
    //! `Command::ProcessTask.predecessor_outputs` (`BTreeMap<String,
    //! TaskOutputs>`) crosses the boundary as a JSON string field on
    //! `PyProcessBinaryCommand`. The tests pin the empty-map shape
    //! (`"{}"`), the populated-map shape (adjacent-tagging via the
    //! `ResultValue` serde attribute), and the Python→Rust reverse
    //! path through `serialize()`.
    //!
    //! Gated on the `test-with-python` feature because the
    //! `command_into_py` and `PyProcessBinaryCommand::serialize`
    //! surfaces both require an embedded CPython interpreter. Invoke
    //! as: `cargo test -p dynrunner-pyo3 --lib --no-default-features
    //!        --features test-with-python protocol_manager_worker`.
    use super::*;
    use dynrunner_core::{ResultValue, TaskOutputs};
    use dynrunner_protocol_manager_worker::codec::parse_command as codec_parse_command;
    use std::collections::BTreeMap;

    /// Pull the `predecessor_outputs_json` attribute off the boxed
    /// `Py<PyAny>` returned by `command_into_py`. Centralised here so
    /// the per-test arrangement stays focused on the input shape.
    fn extract_predecessor_outputs_json(py: Python<'_>, any: &Py<PyAny>) -> String {
        any.bind(py)
            .getattr("predecessor_outputs_json")
            .expect("ProcessBinaryCommand exposes predecessor_outputs_json")
            .extract::<String>()
            .expect("predecessor_outputs_json is a str")
    }

    /// Empty `predecessor_outputs` must surface as `"{}"` so the
    /// Python-side `json.loads(...)` yields an empty dict (not a
    /// `JSONDecodeError` on an empty string).
    #[test]
    fn empty_predecessor_outputs_serialises_to_empty_json_object() {
        Python::attach(|py| {
            let cmd = RustCommand::ProcessTask {
                relative_path: "bin/a".into(),
                payload: None,
                resolved_path: None,
                predecessor_outputs: BTreeMap::new(),
            };
            let py_any = command_into_py(py, cmd).expect("command_into_py succeeds");
            let json = extract_predecessor_outputs_json(py, &py_any);
            assert_eq!(json, "{}");
        });
    }

    /// Populated map must surface as a JSON object keyed by
    /// `predecessor_task_id`, with the inner `ResultValue` rendered
    /// via adjacent tagging (`{"kind","value"}`) — this shape is the
    /// load-bearing contract the Python worker runtime parses.
    #[test]
    fn populated_predecessor_outputs_uses_adjacent_tagging() {
        Python::attach(|py| {
            let mut inner = BTreeMap::new();
            inner.insert("nonce".to_string(), ResultValue::Inline("xyz".to_string()));
            inner.insert(
                "artifact".to_string(),
                ResultValue::File("/shared/out.bin".to_string()),
            );
            let mut outer = BTreeMap::new();
            outer.insert("task_a".to_string(), TaskOutputs(inner));
            let cmd = RustCommand::ProcessTask {
                relative_path: "bin/b".into(),
                payload: None,
                resolved_path: None,
                predecessor_outputs: outer.clone(),
            };
            let py_any = command_into_py(py, cmd).expect("command_into_py succeeds");
            let json = extract_predecessor_outputs_json(py, &py_any);

            // Parse it back into a Value and assert per-key shape so
            // the assertion doesn't depend on serde-json's whitespace.
            let value: serde_json::Value = serde_json::from_str(&json).expect("JSON parses");
            let task_a = value.get("task_a").expect("task_a present");
            let nonce = task_a.get("nonce").expect("nonce present");
            assert_eq!(nonce.get("kind").and_then(|v| v.as_str()), Some("inline"));
            assert_eq!(nonce.get("value").and_then(|v| v.as_str()), Some("xyz"));
            let artifact = task_a.get("artifact").expect("artifact present");
            assert_eq!(artifact.get("kind").and_then(|v| v.as_str()), Some("file"),);
            assert_eq!(
                artifact.get("value").and_then(|v| v.as_str()),
                Some("/shared/out.bin"),
            );

            // Strongly typed round-trip: the JSON parses back into the
            // exact same `BTreeMap<String, TaskOutputs>` (modulo the
            // BTreeMap's stable ordering).
            let parsed: BTreeMap<String, TaskOutputs> =
                serde_json::from_str(&json).expect("round-trip");
            assert_eq!(parsed, outer);
        });
    }

    /// Reverse path: a `PyProcessBinaryCommand` constructed Python-
    /// side with a populated `predecessor_outputs_json` must encode
    /// back through `serialize()` to a wire frame whose decoded
    /// `Command::ProcessTask.predecessor_outputs` matches the source
    /// map. This pins the symmetric Python→Rust JSON parse.
    use super::super::commands::{PyCommand, PyProcessBinaryCommand};

    #[test]
    fn serialize_reverse_path_round_trips_predecessor_outputs() {
        Python::attach(|py| {
            let mut inner = BTreeMap::new();
            inner.insert("nonce".to_string(), ResultValue::Inline("xyz".to_string()));
            let mut outer = BTreeMap::new();
            outer.insert("task_a".to_string(), TaskOutputs(inner));
            let json = serde_json::to_string(&outer).expect("serialise outer");

            let py_cmd = Py::new(
                py,
                (
                    PyProcessBinaryCommand {
                        relative_path: "bin/b".into(),
                        payload: None,
                        resolved_path: None,
                        predecessor_outputs_json: json,
                    },
                    PyCommand,
                ),
            )
            .expect("construct PyProcessBinaryCommand");

            let bytes_any = py_cmd
                .bind(py)
                .call_method0("serialize")
                .expect("serialize() returns bytes");
            let bytes = bytes_any
                .cast::<PyBytes>()
                .expect("serialize() returns PyBytes")
                .as_bytes()
                .to_vec();
            let line = std::str::from_utf8(&bytes)
                .expect("wire frame is UTF-8")
                .trim_end_matches('\n')
                .to_string();
            let decoded = codec_parse_command(&line).expect("codec parses wire frame");
            match decoded {
                RustCommand::ProcessTask {
                    predecessor_outputs,
                    ..
                } => assert_eq!(predecessor_outputs, outer),
                other => panic!("expected ProcessTask, got {other:?}"),
            }
        });
    }

    /// Reverse-path fault tolerance: a malformed
    /// `predecessor_outputs_json` must NOT crash `serialize()`; the
    /// bridge falls back to an empty map (and warns via `tracing`).
    /// Verifies the `unwrap_or_else` arm in
    /// `PyProcessBinaryCommand::serialize`.
    #[test]
    fn serialize_reverse_path_falls_back_to_empty_on_invalid_json() {
        Python::attach(|py| {
            let py_cmd = Py::new(
                py,
                (
                    PyProcessBinaryCommand {
                        relative_path: "bin/b".into(),
                        payload: None,
                        resolved_path: None,
                        predecessor_outputs_json: "{not-json".into(),
                    },
                    PyCommand,
                ),
            )
            .expect("construct PyProcessBinaryCommand");

            let bytes_any = py_cmd
                .bind(py)
                .call_method0("serialize")
                .expect("serialize() returns bytes");
            let bytes = bytes_any
                .cast::<PyBytes>()
                .expect("serialize() returns PyBytes")
                .as_bytes()
                .to_vec();
            let line = std::str::from_utf8(&bytes)
                .expect("wire frame is UTF-8")
                .trim_end_matches('\n')
                .to_string();
            let decoded = codec_parse_command(&line).expect("codec parses wire frame");
            match decoded {
                RustCommand::ProcessTask {
                    predecessor_outputs,
                    ..
                } => assert!(
                    predecessor_outputs.is_empty(),
                    "malformed JSON should fall back to empty map",
                ),
                other => panic!("expected ProcessTask, got {other:?}"),
            }
        });
    }

    /// Codec round-trip pin for the consumer-reported truncation: a
    /// `relative_path` whose LEAF component equals its FIRST component
    /// (`m4/clang21_ppc64_O1_9ac0ed8d/m4`) must survive the
    /// serialize → wire-frame → parse round-trip byte-for-byte, NOT
    /// collapse to its first component (`m4`). Closes the codec layer
    /// empirically alongside the extract-boundary and dispatch pins.
    #[test]
    fn serialize_round_trips_leaf_equals_first_relative_path() {
        Python::attach(|py| {
            const COLLIDE: &str = "m4/clang21_ppc64_O1_9ac0ed8d/m4";
            let py_cmd = Py::new(
                py,
                (
                    PyProcessBinaryCommand {
                        relative_path: COLLIDE.into(),
                        payload: None,
                        resolved_path: None,
                        predecessor_outputs_json: "{}".into(),
                    },
                    PyCommand,
                ),
            )
            .expect("construct PyProcessBinaryCommand");

            let bytes_any = py_cmd
                .bind(py)
                .call_method0("serialize")
                .expect("serialize() returns bytes");
            let bytes = bytes_any
                .cast::<PyBytes>()
                .expect("serialize() returns PyBytes")
                .as_bytes()
                .to_vec();
            let line = std::str::from_utf8(&bytes)
                .expect("wire frame is UTF-8")
                .trim_end_matches('\n')
                .to_string();
            let decoded = codec_parse_command(&line).expect("codec parses wire frame");
            match decoded {
                RustCommand::ProcessTask { relative_path, .. } => assert_eq!(
                    relative_path, COLLIDE,
                    "leaf==first relative_path must survive the codec verbatim, \
                     NOT collapse to its first component"
                ),
                other => panic!("expected ProcessTask, got {other:?}"),
            }
        });
    }
}
