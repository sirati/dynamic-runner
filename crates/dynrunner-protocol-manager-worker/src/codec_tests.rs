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
        error_type: ErrorType::ResourceExhausted(ResourceKind::memory()),
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
            assert_eq!(error_type, ErrorType::ResourceExhausted(ResourceKind::memory()));
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
            assert_eq!(error_type, ErrorType::ResourceExhausted(ResourceKind::memory()));
            assert_eq!(message, "path:to:something ran out");
        }
        _ => panic!("expected Error"),
    }
}
