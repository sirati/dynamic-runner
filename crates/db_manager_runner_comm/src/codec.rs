use db_comm_api_base::{Command, ErrorType, Response};

/// Serialize a command to bytes (line-delimited text, backward-compatible with Python).
///
/// Format:
///   "stop\n"
///   "<relative_path>\n"
pub fn serialize_command(cmd: &Command) -> Vec<u8> {
    match cmd {
        Command::Stop => b"stop\n".to_vec(),
        Command::ProcessBinary { relative_path } => format!("{relative_path}\n").into_bytes(),
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
    Some(Command::ProcessBinary {
        relative_path: line.to_owned(),
    })
}

/// Serialize a response to bytes (line-delimited text, backward-compatible with Python).
///
/// Format:
///   "ready\n"
///   "done\n" or "done:<warnings>:<filtered>\n"
///   "error:<type>:<message>\n"
///   "phase:<name>\n"
///   "keepalive\n"
///
/// Note: PickledError is serialized as a plain error with the exception details
/// concatenated. The pickle format is Python-specific and not reproduced here.
pub fn serialize_response(resp: &Response) -> Vec<u8> {
    match resp {
        Response::Ready => b"ready\n".to_vec(),
        Response::Done { warnings, filtered } => {
            if *warnings == 0 && *filtered == 0 {
                b"done\n".to_vec()
            } else {
                format!("done:{warnings}:{filtered}\n").into_bytes()
            }
        }
        Response::Error {
            error_type,
            message,
        } => format!("error:{}:{message}\n", error_type.wire_value()).into_bytes(),
        Response::PickledError {
            exception_type,
            message,
            traceback,
        } => {
            // For Rust-side serialization, emit as a structured error line.
            // Python workers use pickle; Rust workers use this plain format.
            format!("error:recoverable:{exception_type}: {message}\n{traceback}\n").into_bytes()
        }
        Response::PhaseUpdate { phase_name } => format!("phase:{phase_name}\n").into_bytes(),
        Response::Keepalive => b"keepalive\n".to_vec(),
    }
}

/// Parse a single line into a Response.
///
/// The line may or may not include the trailing newline.
/// This matches the Python `parse_response` function exactly.
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
        return Some(Response::Done {
            warnings: 0,
            filtered: 0,
        });
    }
    if let Some(rest) = line.strip_prefix("done:") {
        let parts: Vec<&str> = rest.splitn(2, ':').collect();
        let warnings = parts.first().and_then(|s| s.parse().ok()).unwrap_or(0);
        let filtered = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);
        return Some(Response::Done { warnings, filtered });
    }
    if let Some(phase_name) = line.strip_prefix("phase:") {
        return Some(Response::PhaseUpdate {
            phase_name: phase_name.to_owned(),
        });
    }
    // Handle pickled errors from Python workers — we parse the raw bytes
    // but cannot unpickle, so treat as a recoverable error with the raw content
    if let Some(rest) = line.strip_prefix("error:pickle:") {
        return Some(Response::PickledError {
            exception_type: "PythonPickledError".to_owned(),
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

    #[test]
    fn command_stop_roundtrip() {
        let bytes = serialize_command(&Command::Stop);
        assert_eq!(bytes, b"stop\n");
        let parsed = parse_command("stop\n").unwrap();
        assert!(matches!(parsed, Command::Stop));
    }

    #[test]
    fn command_process_binary_roundtrip() {
        let cmd = Command::ProcessBinary {
            relative_path: "path/to/binary".into(),
        };
        let bytes = serialize_command(&cmd);
        assert_eq!(bytes, b"path/to/binary\n");
        let parsed = parse_command("path/to/binary\n").unwrap();
        match parsed {
            Command::ProcessBinary { relative_path } => {
                assert_eq!(relative_path, "path/to/binary");
            }
            _ => panic!("expected ProcessBinary"),
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
    fn response_done_no_counts() {
        let resp = Response::Done {
            warnings: 0,
            filtered: 0,
        };
        let bytes = serialize_response(&resp);
        assert_eq!(bytes, b"done\n");
        let parsed = parse_response("done\n").unwrap();
        match parsed {
            Response::Done { warnings, filtered } => {
                assert_eq!(warnings, 0);
                assert_eq!(filtered, 0);
            }
            _ => panic!("expected Done"),
        }
    }

    #[test]
    fn response_done_with_counts() {
        let resp = Response::Done {
            warnings: 3,
            filtered: 7,
        };
        let bytes = serialize_response(&resp);
        assert_eq!(bytes, b"done:3:7\n");
        let parsed = parse_response("done:3:7\n").unwrap();
        match parsed {
            Response::Done { warnings, filtered } => {
                assert_eq!(warnings, 3);
                assert_eq!(filtered, 7);
            }
            _ => panic!("expected Done"),
        }
    }

    #[test]
    fn response_error_roundtrip() {
        let resp = Response::Error {
            error_type: ErrorType::OutOfMemory,
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
                assert_eq!(error_type, ErrorType::OutOfMemory);
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
    fn parse_pickled_error() {
        let parsed = parse_response("error:pickle:some_raw_data").unwrap();
        match parsed {
            Response::PickledError {
                exception_type,
                message,
                ..
            } => {
                assert_eq!(exception_type, "PythonPickledError");
                assert_eq!(message, "some_raw_data");
            }
            _ => panic!("expected PickledError"),
        }
    }

    #[test]
    fn response_error_with_colons_in_message() {
        // Messages can contain colons — only split on the first two
        let parsed = parse_response("error:oom:path:to:something ran out").unwrap();
        match parsed {
            Response::Error {
                error_type,
                message,
            } => {
                assert_eq!(error_type, ErrorType::OutOfMemory);
                assert_eq!(message, "path:to:something ran out");
            }
            _ => panic!("expected Error"),
        }
    }
}
