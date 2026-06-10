use std::collections::BTreeMap;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use dynrunner_core::{ErrorType, TaskOutputs};
use serde::{Deserialize, Serialize};

use crate::command::{Command, Response};

/// Wire payload for `Response::WorkerException`. Encoded as
/// base64-JSON to keep the line-delimited text format intact even
/// when tracebacks contain newlines or `:` characters.
///
/// `error_type` is the optional restart-or-not category. Omitted on
/// the wire (the legacy shape — every WorkerException meant
/// "worker is dead, restart it") deserialises to `None`. Newer
/// senders set the wire value to `"recoverable"`, `"non_recoverable"`,
/// or `"oom"` to control how the runner classifies the failure.
#[derive(Debug, Serialize, Deserialize)]
struct WorkerExceptionWire {
    #[serde(rename = "type")]
    exception_type: String,
    message: String,
    traceback: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    error_type: Option<String>,
}

/// Wire payload for `Response::Custom` / `Command::Custom` (the two
/// directions share one frame shape: `"custom:<b64 {topic,
/// data_b64}>\n"`). Base64-JSON for the same reason as
/// [`WorkerExceptionWire`]: consumer topics and payloads may contain
/// newlines or `:` characters, and the b64 envelope keeps the
/// line-delimited text format intact. `data_b64` is the standard-
/// alphabet base64 of the raw payload bytes (the payload is opaque
/// `Vec<u8>`, not necessarily UTF-8, so it cannot ride JSON as a
/// string directly). Old parsers that predate the frame ignore it
/// (`parse_response` returns no match for unknown prefixes only
/// after every known prefix — see the `custom:` arms below).
#[derive(Debug, Serialize, Deserialize)]
struct CustomMessageWire {
    topic: String,
    data_b64: String,
}

/// Encode one custom-message line frame (shared by the command and
/// response serializers — the two directions are byte-identical on
/// the wire).
fn serialize_custom_frame(topic: &str, data: &[u8]) -> Vec<u8> {
    let wire = CustomMessageWire {
        topic: topic.to_owned(),
        data_b64: BASE64.encode(data),
    };
    // Serialising a two-string-field struct cannot fail.
    let json = serde_json::to_string(&wire)
        .unwrap_or_else(|_| "{\"topic\":\"\",\"data_b64\":\"\"}".into());
    let encoded = BASE64.encode(json.as_bytes());
    format!("custom:{encoded}\n").into_bytes()
}

/// Topic stamped on a custom frame whose b64-JSON payload failed to
/// decode. Mirrors the `MalformedException` fallback on the
/// `error:exception:` idiom: the frame is surfaced loudly (raw bytes
/// as `data`) instead of being silently dropped — and crucially
/// instead of returning `None`, which the manager-side transports
/// map onto "connection closed".
pub const MALFORMED_CUSTOM_TOPIC: &str = "__malformed_custom__";

/// Decode the body of a custom frame (the bytes after the `custom:`
/// prefix) into `(topic, data)`. On any decode failure, falls back to
/// `(MALFORMED_CUSTOM_TOPIC, <raw post-prefix bytes>)` — loud beats
/// silent, and a `None` would be misread as a disconnect.
fn parse_custom_frame(rest: &str) -> (String, Vec<u8>) {
    if let Ok(json_bytes) = BASE64.decode(rest.as_bytes())
        && let Ok(wire) = serde_json::from_slice::<CustomMessageWire>(&json_bytes)
        && let Ok(data) = BASE64.decode(wire.data_b64.as_bytes())
    {
        return (wire.topic, data);
    }
    (MALFORMED_CUSTOM_TOPIC.to_owned(), rest.as_bytes().to_vec())
}

/// Serialize a command to bytes (line-delimited text, backward-compatible with Python).
///
/// Format:
///   "stop\n"
///   "<relative_path>\n"                                                          (legacy ProcessTask, no optional fields)
///   "task:<json {path, payload?, resolved_path?, predecessor_outputs?}>\n"  (any optional field present)
///   "custom:<b64 {topic, data_b64}>\n"                                      (secondary→worker custom message)
///
/// The `task:` prefix routes the new form. Legacy paths starting
/// with the literal string `task:` would collide; in practice
/// paths don't, and consumers that need payload-bearing,
/// resolved-path, or predecessor-outputs dispatch opt in
/// knowing they shouldn't emit `task:`-prefixed paths in the
/// same run.
///
/// The bare-path form is preserved when every optional field is
/// absent (`payload`/`resolved_path` are `None` AND
/// `predecessor_outputs` is empty) — pre-feature tasks remain
/// byte-identical on the wire.
pub fn serialize_command(cmd: &Command) -> Vec<u8> {
    match cmd {
        Command::Stop => b"stop\n".to_vec(),
        Command::ProcessTask {
            relative_path,
            payload: None,
            resolved_path: None,
            predecessor_outputs,
        } if predecessor_outputs.is_empty() => format!("{relative_path}\n").into_bytes(),
        Command::ProcessTask {
            relative_path,
            payload,
            resolved_path,
            predecessor_outputs,
        } => {
            // serde_json::json! only includes keys with valid Value
            // shapes; build the map explicitly to omit absent fields
            // so legacy parsers that don't know about
            // `resolved_path` / `predecessor_outputs` don't trip on
            // a `null`.
            let mut wrapper = serde_json::Map::new();
            wrapper.insert(
                "path".into(),
                serde_json::Value::String(relative_path.clone()),
            );
            if let Some(p) = payload {
                wrapper.insert("payload".into(), serde_json::Value::String(p.clone()));
            }
            if let Some(rp) = resolved_path {
                wrapper.insert(
                    "resolved_path".into(),
                    serde_json::Value::String(rp.clone()),
                );
            }
            if !predecessor_outputs.is_empty() {
                // serde_json::to_value never fails for a
                // `BTreeMap<String, TaskOutputs>` (string keys,
                // serde-derived values) — unwrap is sound.
                let outputs_value = serde_json::to_value(predecessor_outputs)
                    .expect("BTreeMap<String, TaskOutputs> always serialises");
                wrapper.insert("predecessor_outputs".into(), outputs_value);
            }
            let value = serde_json::Value::Object(wrapper);
            // serde_json compact never emits newlines, so this is
            // safe to embed in a single line.
            format!("task:{}\n", value).into_bytes()
        }
        Command::Custom { topic, data } => serialize_custom_frame(topic, data),
    }
}

/// Parse a single line into a Command.
///
/// The line may or may not include the trailing newline.
pub fn parse_command(line: &str) -> Option<Command> {
    let line = line.trim();
    if line.is_empty() {
        return None;
    }
    if line == "stop" {
        return Some(Command::Stop);
    }
    if let Some(rest) = line.strip_prefix("custom:") {
        // Secondary→worker custom message: `custom:<b64 {topic,
        // data_b64}>`. Checked BEFORE the bare-path fallback so a
        // custom frame can never be misread as a legacy task path.
        // (Legacy paths starting with the literal `custom:` would
        // collide — same documented non-collision contract as the
        // `task:` prefix above.)
        let (topic, data) = parse_custom_frame(rest);
        return Some(Command::Custom { topic, data });
    }
    if let Some(rest) = line.strip_prefix("task:") {
        // New form: task:<json {path, payload?, resolved_path?, predecessor_outputs?}>.
        // Falls back to legacy interpretation if the JSON is
        // malformed (treat the whole line as a literal path) —
        // defensive, since a legacy emitter that happened to send a
        // `task:`-prefixed path would otherwise hit a parse error
        // here. Missing `payload` / `resolved_path` /
        // `predecessor_outputs` deserialise as `None` / empty,
        // preserving wire compatibility with senders that omit any
        // of them. Unknown keys are silently ignored, so a future
        // sender adding more optionals stays decode-compatible.
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(rest) {
            let path = value
                .get("path")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_owned();
            let payload = value
                .get("payload")
                .and_then(|v| v.as_str())
                .map(|s| s.to_owned());
            let resolved_path = value
                .get("resolved_path")
                .and_then(|v| v.as_str())
                .map(|s| s.to_owned());
            let predecessor_outputs = value
                .get("predecessor_outputs")
                .cloned()
                .map(serde_json::from_value::<BTreeMap<String, TaskOutputs>>)
                .and_then(Result::ok)
                .unwrap_or_default();
            return Some(Command::ProcessTask {
                relative_path: path,
                payload,
                resolved_path,
                predecessor_outputs,
            });
        }
    }
    Some(Command::ProcessTask {
        relative_path: line.to_owned(),
        payload: None,
        resolved_path: None,
        predecessor_outputs: BTreeMap::new(),
    })
}

/// Serialize a response to bytes (line-delimited text, backward-compatible with Python).
///
/// Format:
///   "ready\n"
///   "done\n" or "done:<result_data_utf8>\n"
///   "error:<type>:<message>\n"
///   "phase:<name>\n"
///   "keepalive\n"
///   "custom:<b64 {topic, data_b64}>\n"  (worker→secondary custom message)
///
/// The bytes after `done:` are fully opaque to the framework: the
/// codec round-trips them as a `Vec<u8>` and never inspects them.
/// `done:` vs `error:` is the only structured signal the framework
/// needs from a worker's terminal response. Anything richer
/// (warnings/filtered counts, structured per-task results, etc.)
/// is a consumer concern and must be encoded inside `result_data`
/// by the producing worker and decoded by the consuming primary —
/// the framework just forwards the bytes.
pub fn serialize_response(resp: &Response) -> Vec<u8> {
    match resp {
        Response::Ready => b"ready\n".to_vec(),
        Response::Done { result_data } => match result_data {
            None => b"done\n".to_vec(),
            Some(data) => {
                let text = String::from_utf8_lossy(data);
                format!("done:{text}\n").into_bytes()
            }
        },
        Response::Error {
            error_type,
            message,
        } => format!("error:{}:{message}\n", error_type.wire_value()).into_bytes(),
        Response::WorkerException {
            exception_type,
            message,
            traceback,
            error_type,
        } => {
            let wire = WorkerExceptionWire {
                exception_type: exception_type.clone(),
                message: message.clone(),
                traceback: traceback.clone(),
                error_type: error_type.as_ref().map(|t| t.wire_value().to_string()),
            };
            let json = serde_json::to_string(&wire)
                .unwrap_or_else(|_| "{\"type\":\"\",\"message\":\"\",\"traceback\":\"\"}".into());
            let encoded = BASE64.encode(json.as_bytes());
            format!("error:exception:{encoded}\n").into_bytes()
        }
        Response::PhaseUpdate { phase_name } => format!("phase:{phase_name}\n").into_bytes(),
        Response::Keepalive => b"keepalive\n".to_vec(),
        Response::Custom { topic, data } => serialize_custom_frame(topic, data),
    }
}

/// Parse a single line into a Response.
///
/// The line may or may not include the trailing newline.
pub fn parse_response(line: &str) -> Option<Response> {
    let line = line.trim();
    if line.is_empty() {
        return None;
    }

    if line == "keepalive" {
        return Some(Response::Keepalive);
    }
    if line == "ready" {
        return Some(Response::Ready);
    }
    if line == "done" {
        return Some(Response::Done { result_data: None });
    }
    if let Some(rest) = line.strip_prefix("done:") {
        return Some(Response::Done {
            result_data: Some(rest.as_bytes().to_vec()),
        });
    }
    if let Some(phase_name) = line.strip_prefix("phase:") {
        return Some(Response::PhaseUpdate {
            phase_name: phase_name.to_owned(),
        });
    }
    if let Some(rest) = line.strip_prefix("custom:") {
        // Worker→secondary custom message: `custom:<b64 {topic,
        // data_b64}>`. Same frame shape as the command direction.
        let (topic, data) = parse_custom_frame(rest);
        return Some(Response::Custom { topic, data });
    }
    if let Some(rest) = line.strip_prefix("error:exception:") {
        // Modern shape: base64-JSON {type, message, traceback,
        // error_type?}. Backwards compatible: senders that omit
        // `error_type` parse to `None`, which the runner treats as
        // NonRecoverable (legacy "worker is dead" semantic).
        if let Ok(json_bytes) = BASE64.decode(rest.as_bytes())
            && let Ok(wire) = serde_json::from_slice::<WorkerExceptionWire>(&json_bytes)
        {
            return Some(Response::WorkerException {
                exception_type: wire.exception_type,
                message: wire.message,
                traceback: wire.traceback,
                error_type: wire.error_type.as_deref().and_then(ErrorType::from_wire),
            });
        }
        return Some(Response::WorkerException {
            exception_type: "MalformedException".to_owned(),
            message: rest.to_owned(),
            traceback: String::new(),
            error_type: None,
        });
    }
    if let Some(rest) = line.strip_prefix("error:pickle:") {
        // Legacy shape from older Python workers. Bytes after the prefix are the
        // pickle blob; we don't deserialise Python objects, so surface them as
        // an opaque message and lose type/traceback fidelity.
        return Some(Response::WorkerException {
            exception_type: "LegacyPickledException".to_owned(),
            message: rest.to_owned(),
            traceback: String::new(),
            error_type: None,
        });
    }
    if let Some(rest) = line.strip_prefix("error:") {
        let parts: Vec<&str> = rest.splitn(2, ':').collect();
        if parts.len() >= 2 {
            let error_type = ErrorType::from_wire(parts[0]).unwrap_or(ErrorType::Recoverable);
            let message = parts[1].to_owned();
            return Some(Response::Error {
                error_type,
                message,
            });
        }
    }

    None
}

#[cfg(test)]
#[path = "codec_tests.rs"]
mod tests;
