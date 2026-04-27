use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use db_comm_api_base::ErrorType;
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
///   "<relative_path>\n"
pub fn serialize_command(cmd: &Command) -> Vec<u8> {
    match cmd {
        Command::Stop => b"stop\n".to_vec(),
        Command::ProcessTask { relative_path } => format!("{relative_path}\n").into_bytes(),
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
    Some(Command::ProcessTask {
        relative_path: line.to_owned(),
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
mod tests {
    use super::*;
    use db_comm_api_base::ResourceKind;

    #[test]
    fn command_stop_roundtrip() {
        let bytes = serialize_command(&Command::Stop);
        assert_eq!(bytes, b"stop\n");
        let parsed = parse_command("stop\n").unwrap();
        assert!(matches!(parsed, Command::Stop));
    }

    #[test]
    fn command_process_task_roundtrip() {
        let cmd = Command::ProcessTask {
            relative_path: "path/to/binary".into(),
        };
        let bytes = serialize_command(&cmd);
        assert_eq!(bytes, b"path/to/binary\n");
        let parsed = parse_command("path/to/binary\n").unwrap();
        match parsed {
            Command::ProcessTask { relative_path } => {
                assert_eq!(relative_path, "path/to/binary");
            }
            _ => panic!("expected ProcessTask"),
        }
    }

    #[test]
    fn response_ready_roundtrip() {
        let bytes = serialize_response(&Response::Ready);
        assert_eq!(bytes, b"ready\n");
        let parsed = parse_response("ready\n").unwrap();
        assert!(matches!(parsed, Response::Ready));
    }

    #[test]
    fn response_done_no_data() {
        let resp = Response::Done { result_data: None };
        let bytes = serialize_response(&resp);
        assert_eq!(bytes, b"done\n");
        let parsed = parse_response("done\n").unwrap();
        match parsed {
            Response::Done { result_data } => assert!(result_data.is_none()),
            _ => panic!("expected Done"),
        }
    }

    #[test]
    fn response_done_with_data() {
        let resp = Response::Done {
            result_data: Some(b"3:7".to_vec()),
        };
        let bytes = serialize_response(&resp);
        assert_eq!(bytes, b"done:3:7\n");
        let parsed = parse_response("done:3:7\n").unwrap();
        match parsed {
            Response::Done { result_data } => {
                assert_eq!(result_data.unwrap(), b"3:7");
            }
            _ => panic!("expected Done"),
        }
    }

    #[test]
    fn response_done_legacy_compat() {
        // Python workers send done:3:7 — should parse as result_data bytes
        let parsed = parse_response("done:3:7\n").unwrap();
        match parsed {
            Response::Done { result_data } => {
                assert_eq!(result_data.unwrap(), b"3:7");
            }
            _ => panic!("expected Done"),
        }
    }

    #[test]
    fn response_error_roundtrip() {
        let resp = Response::Error {
            error_type: ErrorType::ResourceExhausted(ResourceKind::Memory),
            message: "worker exceeded budget".into(),
        };
        let bytes = serialize_response(&resp);
        assert_eq!(bytes, b"error:oom:worker exceeded budget\n");
        let parsed = parse_response("error:oom:worker exceeded budget\n").unwrap();
        match parsed {
            Response::Error {
                error_type,
                message,
            } => {
                assert_eq!(error_type, ErrorType::ResourceExhausted(ResourceKind::Memory));
                assert_eq!(message, "worker exceeded budget");
            }
            _ => panic!("expected Error"),
        }
    }

    #[test]
    fn response_error_non_recoverable() {
        let parsed = parse_response("error:non_recoverable:segfault").unwrap();
        match parsed {
            Response::Error {
                error_type,
                message,
            } => {
                assert_eq!(error_type, ErrorType::NonRecoverable);
                assert_eq!(message, "segfault");
            }
            _ => panic!("expected Error"),
        }
    }

    #[test]
    fn response_phase_update_roundtrip() {
        let resp = Response::PhaseUpdate {
            phase_name: "ANGR_1".into(),
        };
        let bytes = serialize_response(&resp);
        assert_eq!(bytes, b"phase:ANGR_1\n");
        let parsed = parse_response("phase:ANGR_1\n").unwrap();
        match parsed {
            Response::PhaseUpdate { phase_name } => {
                assert_eq!(phase_name, "ANGR_1");
            }
            _ => panic!("expected PhaseUpdate"),
        }
    }

    #[test]
    fn response_keepalive_roundtrip() {
        let bytes = serialize_response(&Response::Keepalive);
        assert_eq!(bytes, b"keepalive\n");
        let parsed = parse_response("keepalive\n").unwrap();
        assert!(matches!(parsed, Response::Keepalive));
    }

    #[test]
    fn parse_empty_returns_none() {
        assert!(parse_command("").is_none());
        assert!(parse_command("  \n").is_none());
        assert!(parse_response("").is_none());
        assert!(parse_response("  \n").is_none());
    }

    #[test]
    fn parse_unknown_error_type_defaults_to_recoverable() {
        let parsed = parse_response("error:unknown_type:some message").unwrap();
        match parsed {
            Response::Error {
                error_type,
                message,
            } => {
                assert_eq!(error_type, ErrorType::Recoverable);
                assert_eq!(message, "some message");
            }
            _ => panic!("expected Error"),
        }
    }

    #[test]
    fn parse_legacy_pickled_error() {
        let parsed = parse_response("error:pickle:some_raw_data").unwrap();
        match parsed {
            Response::WorkerException {
                exception_type,
                message,
                ..
            } => {
                assert_eq!(exception_type, "LegacyPickledException");
                assert_eq!(message, "some_raw_data");
            }
            _ => panic!("expected WorkerException"),
        }
    }

    #[test]
    fn worker_exception_roundtrip() {
        let resp = Response::WorkerException {
            exception_type: "ValueError".into(),
            message: "thing went wrong: detail".into(),
            traceback: "Traceback (most recent call last):\n  File ...".into(),
        };
        let bytes = serialize_response(&resp);
        let line = std::str::from_utf8(&bytes).unwrap();
        let parsed = parse_response(line).unwrap();
        match parsed {
            Response::WorkerException {
                exception_type,
                message,
                traceback,
            } => {
                assert_eq!(exception_type, "ValueError");
                assert_eq!(message, "thing went wrong: detail");
                assert_eq!(traceback, "Traceback (most recent call last):\n  File ...");
            }
            _ => panic!("expected WorkerException"),
        }
    }

    #[test]
    fn response_error_with_colons_in_message() {
        let parsed = parse_response("error:oom:path:to:something ran out").unwrap();
        match parsed {
            Response::Error {
                error_type,
                message,
            } => {
                assert_eq!(error_type, ErrorType::ResourceExhausted(ResourceKind::Memory));
                assert_eq!(message, "path:to:something ran out");
            }
            _ => panic!("expected Error"),
        }
    }
}
