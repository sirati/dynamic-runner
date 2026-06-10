use std::collections::BTreeMap;

use super::*;
use dynrunner_core::{ResourceKind, ResultValue, TaskOutputs};

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
        payload: None,
        resolved_path: None,
        predecessor_outputs: BTreeMap::new(),
    };
    let bytes = serialize_command(&cmd);
    assert_eq!(bytes, b"path/to/binary\n");
    let parsed = parse_command("path/to/binary\n").unwrap();
    match parsed {
        Command::ProcessTask { relative_path, .. } => {
            assert_eq!(relative_path, "path/to/binary");
        }
        _ => panic!("expected ProcessTask"),
    }
}

#[test]
fn command_process_task_predecessor_outputs_roundtrip() {
    // Two predecessors, each carrying a tagged-enum value, round-trip
    // verbatim through the `task:<json>` wrapper.
    let mut outputs_a = BTreeMap::new();
    outputs_a.insert("nonce".to_string(), ResultValue::Inline("xyz".to_string()));
    outputs_a.insert(
        "artifact".to_string(),
        ResultValue::File("/app/out-network/a.tar".to_string()),
    );
    let mut predecessor_outputs = BTreeMap::new();
    predecessor_outputs.insert("task_a".to_string(), TaskOutputs(outputs_a));

    let cmd = Command::ProcessTask {
        relative_path: "task_b/item".into(),
        payload: None,
        resolved_path: None,
        predecessor_outputs: predecessor_outputs.clone(),
    };
    let bytes = serialize_command(&cmd);
    let line = std::str::from_utf8(&bytes).unwrap();
    // Non-empty predecessor_outputs forces the `task:` wrapper form.
    assert!(line.starts_with("task:"));
    let parsed = parse_command(line).unwrap();
    match parsed {
        Command::ProcessTask {
            relative_path,
            payload,
            resolved_path,
            predecessor_outputs: parsed_outputs,
        } => {
            assert_eq!(relative_path, "task_b/item");
            assert!(payload.is_none());
            assert!(resolved_path.is_none());
            assert_eq!(parsed_outputs, predecessor_outputs);
        }
        _ => panic!("expected ProcessTask"),
    }
}

#[test]
fn command_process_task_legacy_wrapper_backcompat() {
    // Wire fixture from before `predecessor_outputs` existed: the
    // `task:<json>` wrapper carries only `path` / `payload` /
    // `resolved_path`. Parser must accept it cleanly and default
    // `predecessor_outputs` to empty.
    let legacy = "task:{\"path\":\"bin/x\",\"payload\":\"{\\\"a\\\":1}\",\"resolved_path\":\"/abs/bin/x\"}\n";
    let parsed = parse_command(legacy).unwrap();
    match parsed {
        Command::ProcessTask {
            relative_path,
            payload,
            resolved_path,
            predecessor_outputs,
        } => {
            assert_eq!(relative_path, "bin/x");
            assert_eq!(payload.as_deref(), Some("{\"a\":1}"));
            assert_eq!(resolved_path.as_deref(), Some("/abs/bin/x"));
            assert!(predecessor_outputs.is_empty());
        }
        _ => panic!("expected ProcessTask"),
    }
}

#[test]
fn command_process_task_bare_path_preserved_when_all_empty() {
    // The bare-path fast-path is preserved when `predecessor_outputs`
    // is empty AND every other optional is absent — pre-feature tasks
    // remain byte-identical on the wire.
    let cmd = Command::ProcessTask {
        relative_path: "path/to/binary".into(),
        payload: None,
        resolved_path: None,
        predecessor_outputs: BTreeMap::new(),
    };
    let bytes = serialize_command(&cmd);
    assert_eq!(bytes, b"path/to/binary\n");
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
            assert_eq!(
                error_type,
                ErrorType::ResourceExhausted(ResourceKind::memory())
            );
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
        error_type: None,
    };
    let bytes = serialize_response(&resp);
    let line = std::str::from_utf8(&bytes).unwrap();
    let parsed = parse_response(line).unwrap();
    match parsed {
        Response::WorkerException {
            exception_type,
            message,
            traceback,
            error_type,
        } => {
            assert_eq!(exception_type, "ValueError");
            assert_eq!(message, "thing went wrong: detail");
            assert_eq!(traceback, "Traceback (most recent call last):\n  File ...");
            assert!(error_type.is_none());
        }
        _ => panic!("expected WorkerException"),
    }
}

#[test]
fn worker_exception_recoverable_roundtrip() {
    // Sender (consumer worker that wants to surface a traceback for a
    // user-task IndexError without forcing a worker restart) sets
    // error_type=Recoverable; runner must echo it back through the
    // wire so state.rs can route to PollResult::Completed instead of
    // Disconnected.
    let resp = Response::WorkerException {
        exception_type: "IndexError".into(),
        message: "list index out of range".into(),
        traceback: "Traceback (most recent call last):\n  File 'w.py', line 42\n    x[10]\nIndexError: list index out of range".into(),
        error_type: Some(ErrorType::Recoverable),
    };
    let bytes = serialize_response(&resp);
    let line = std::str::from_utf8(&bytes).unwrap();
    let parsed = parse_response(line).unwrap();
    match parsed {
        Response::WorkerException {
            exception_type,
            traceback,
            error_type,
            ..
        } => {
            assert_eq!(exception_type, "IndexError");
            assert!(traceback.contains("IndexError: list index out of range"));
            assert_eq!(error_type, Some(ErrorType::Recoverable));
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
            assert_eq!(
                error_type,
                ErrorType::ResourceExhausted(ResourceKind::memory())
            );
            assert_eq!(message, "path:to:something ran out");
        }
        _ => panic!("expected Error"),
    }
}

// ─── custom-message frames (worker↔secondary consumer channel) ───

/// Response::Custom round-trips with a topic and payload that both
/// contain newlines and colons — the b64-JSON envelope keeps the
/// line-delimited text format intact (the error:exception: idiom).
#[test]
fn response_custom_roundtrip_with_newline_and_colon_payload() {
    let resp = Response::Custom {
        topic: "phase4:batch\nline2".into(),
        data: b"raw\nbytes:with\x00nul and : colons\n".to_vec(),
    };
    let bytes = serialize_response(&resp);
    let line = std::str::from_utf8(&bytes).unwrap();
    assert!(line.starts_with("custom:"));
    // Exactly one frame-terminating newline — nothing from the
    // payload leaks into the framing.
    assert_eq!(line.matches('\n').count(), 1);
    assert!(line.ends_with('\n'));
    let parsed = parse_response(line).unwrap();
    match parsed {
        Response::Custom { topic, data } => {
            assert_eq!(topic, "phase4:batch\nline2");
            assert_eq!(data, b"raw\nbytes:with\x00nul and : colons\n");
        }
        other => panic!("expected Custom, got {other:?}"),
    }
}

/// Command::Custom round-trips identically (the two directions share
/// one frame shape).
#[test]
fn command_custom_roundtrip_with_newline_and_colon_payload() {
    let cmd = Command::Custom {
        topic: "reply:topic".into(),
        data: b"data\nwith:everything".to_vec(),
    };
    let bytes = serialize_command(&cmd);
    let line = std::str::from_utf8(&bytes).unwrap();
    assert!(line.starts_with("custom:"));
    let parsed = parse_command(line).unwrap();
    match parsed {
        Command::Custom { topic, data } => {
            assert_eq!(topic, "reply:topic");
            assert_eq!(data, b"data\nwith:everything");
        }
        other => panic!("expected Custom, got {other:?}"),
    }
}

/// Wire-shape PIN: the frame is `custom:<b64 {"topic": ..,
/// "data_b64": ..}>\n` with STANDARD-alphabet base64 — asserted
/// against a verbatim hand-built frame (built from a JSON string
/// literal, NOT through the codec's own structs), so an
/// implementation drift on either side of the cross-language seam
/// fails this test rather than round-tripping invisibly.
#[test]
fn custom_frame_wire_shape_pin() {
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD as B64;
    // topic "t:op\nic", data b"d:a\nta" — adversarial bytes.
    let json = format!(
        "{{\"topic\":\"t:op\\nic\",\"data_b64\":\"{}\"}}",
        B64.encode(b"d:a\nta")
    );
    let frame = format!("custom:{}\n", B64.encode(json.as_bytes()));

    // Decode the hand-built frame on BOTH parser sides.
    match parse_response(&frame).unwrap() {
        Response::Custom { topic, data } => {
            assert_eq!(topic, "t:op\nic");
            assert_eq!(data, b"d:a\nta");
        }
        other => panic!("expected Custom, got {other:?}"),
    }
    match parse_command(&frame).unwrap() {
        Command::Custom { topic, data } => {
            assert_eq!(topic, "t:op\nic");
            assert_eq!(data, b"d:a\nta");
        }
        other => panic!("expected Custom, got {other:?}"),
    }

    // Encode-side: the serializer must emit a frame whose decoded
    // JSON matches the pinned keys verbatim (field order is serde's,
    // so compare the parsed JSON, not the raw string).
    let bytes = serialize_response(&Response::Custom {
        topic: "t:op\nic".into(),
        data: b"d:a\nta".to_vec(),
    });
    let line = std::str::from_utf8(&bytes).unwrap();
    let body = line.strip_prefix("custom:").unwrap().trim_end();
    let decoded_json = B64.decode(body.as_bytes()).unwrap();
    let value: serde_json::Value = serde_json::from_slice(&decoded_json).unwrap();
    assert_eq!(value.get("topic").and_then(|v| v.as_str()), Some("t:op\nic"));
    assert_eq!(
        value.get("data_b64").and_then(|v| v.as_str()),
        Some(B64.encode(b"d:a\nta").as_str())
    );
}

/// Malformed custom payloads surface LOUDLY (the MalformedException
/// idiom) instead of mapping to `None` — a `None` would be misread
/// as a transport disconnect by the manager-side reader.
#[test]
fn custom_frame_malformed_payload_is_loud_not_none() {
    let frame = "custom:!!!not-base64!!!\n";
    match parse_response(frame).unwrap() {
        Response::Custom { topic, data } => {
            assert_eq!(topic, MALFORMED_CUSTOM_TOPIC);
            assert_eq!(data, b"!!!not-base64!!!");
        }
        other => panic!("expected malformed-fallback Custom, got {other:?}"),
    }
    match parse_command(frame).unwrap() {
        Command::Custom { topic, .. } => assert_eq!(topic, MALFORMED_CUSTOM_TOPIC),
        other => panic!("expected malformed-fallback Custom, got {other:?}"),
    }
}

/// A max-size payload (CUSTOM_MESSAGE_MAX_BYTES) frames comfortably
/// under MAX_RESPONSE_FRAME_BYTES — the wire TOLERATES what the API
/// caps (size enforcement lives at the call sites, never the codec).
#[test]
fn custom_frame_max_payload_is_under_response_frame_cap() {
    let resp = Response::Custom {
        topic: "bulk".into(),
        data: vec![0xAB; crate::command::CUSTOM_MESSAGE_MAX_BYTES],
    };
    let bytes = serialize_response(&resp);
    assert!(bytes.len() < crate::framing::MAX_RESPONSE_FRAME_BYTES);
    let line = std::str::from_utf8(&bytes).unwrap();
    let parsed = parse_response(line).unwrap();
    match parsed {
        Response::Custom { data, .. } => {
            assert_eq!(data.len(), crate::command::CUSTOM_MESSAGE_MAX_BYTES);
        }
        other => panic!("expected Custom, got {other:?}"),
    }
}

/// Old-parser tolerance: a custom RESPONSE frame fed to a parser that
/// predates it would hit the unknown-prefix fallthrough (None →
/// ignored line). This test pins the inverse guarantee on the COMMAND
/// side: the `custom:` prefix is recognised BEFORE the bare-path
/// fallback, so a custom command can never be misread as a task path.
#[test]
fn custom_command_prefix_wins_over_barepath_fallback() {
    let cmd = Command::Custom {
        topic: "x".into(),
        data: b"y".to_vec(),
    };
    let bytes = serialize_command(&cmd);
    let line = std::str::from_utf8(&bytes).unwrap();
    match parse_command(line).unwrap() {
        Command::Custom { .. } => {}
        Command::ProcessTask { relative_path, .. } => {
            panic!("custom frame misparsed as task path {relative_path:?}")
        }
        other => panic!("unexpected parse: {other:?}"),
    }
}
