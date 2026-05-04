use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use dynrunner_core::ErrorType;
use serde::{Deserialize, Serialize};

use crate::command::{Command, Response};

/// Wire payload for `Response::WorkerException` — three plain strings.
/// Encoded as base64-JSON to keep the line-delimited text format intact even
/// when tracebacks contain newlines or `:` characters.
#[derive(Debug, Serialize, Deserialize)]
struct WorkerExceptionWire {
    #[serde(rename = "type")]
    exception_type: String,
    message: String,
    traceback: String,
}

/// Serialize a command to bytes (line-delimited text, backward-compatible with Python).
///
/// Format:
///   "stop\n"
///   "<relative_path>\n"                          (legacy ProcessTask, no payload)
///   "task:<json {path, payload}>\n"              (new ProcessTask with payload)
///
/// The `task:` prefix routes the new form. Legacy paths starting
/// with the literal string `task:` would collide; in practice
/// paths don't, and consumers that need payload-bearing dispatch
/// opt in via `Some(payload)` knowing they shouldn't emit
/// `task:`-prefixed paths in the same run.
pub fn serialize_command(cmd: &Command) -> Vec<u8> {
    match cmd {
        Command::Stop => b"stop\n".to_vec(),
        Command::ProcessTask {
            relative_path,
            payload: None,
        } => format!("{relative_path}\n").into_bytes(),
        Command::ProcessTask {
            relative_path,
            payload: Some(payload),
        } => {
            let wrapper = serde_json::json!({
                "path": relative_path,
                "payload": payload,
            });
            // serde_json compact never emits newlines, so this is
            // safe to embed in a single line.
            format!("task:{}\n", wrapper).into_bytes()
        }
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
    if let Some(rest) = line.strip_prefix("task:") {
        // New form: task:<json {path, payload}>. Falls back to
        // legacy interpretation if the JSON is malformed (treat the
        // whole line as a literal path) — defensive, since a
        // legacy emitter that happened to send a `task:`-prefixed
        // path would otherwise hit a parse error here.
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
            return Some(Command::ProcessTask {
                relative_path: path,
                payload,
            });
        }
    }
    Some(Command::ProcessTask {
        relative_path: line.to_owned(),
        payload: None,
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
        } => {
            let wire = WorkerExceptionWire {
                exception_type: exception_type.clone(),
                message: message.clone(),
                traceback: traceback.clone(),
            };
            let json = serde_json::to_string(&wire)
                .unwrap_or_else(|_| "{\"type\":\"\",\"message\":\"\",\"traceback\":\"\"}".into());
            let encoded = BASE64.encode(json.as_bytes());
            format!("error:exception:{encoded}\n").into_bytes()
        }
        Response::PhaseUpdate { phase_name } => format!("phase:{phase_name}\n").into_bytes(),
        Response::Keepalive => b"keepalive\n".to_vec(),
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
    if let Some(rest) = line.strip_prefix("error:exception:") {
        // New shape: base64-JSON {type, message, traceback}.
        if let Ok(json_bytes) = BASE64.decode(rest.as_bytes()) {
            if let Ok(wire) = serde_json::from_slice::<WorkerExceptionWire>(&json_bytes) {
                return Some(Response::WorkerException {
                    exception_type: wire.exception_type,
                    message: wire.message,
                    traceback: wire.traceback,
                });
            }
        }
        return Some(Response::WorkerException {
            exception_type: "MalformedException".to_owned(),
            message: rest.to_owned(),
            traceback: String::new(),
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
