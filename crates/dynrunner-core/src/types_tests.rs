use super::*;

/// Minimal test identifier for unit tests.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct TestId(String);

#[test]
fn error_type_wire_roundtrip() {
    for et in [
        ErrorType::ResourceExhausted(ResourceKind::memory()),
        ErrorType::NonRecoverable,
        ErrorType::Recoverable,
        ErrorType::Unfulfillable {
            reason: "toolchain outpath /nix/store/abc-foo missing"
                .to_string()
                .into(),
        },
        ErrorType::InvalidTask {
            reason: "dependency (phase-a, task-7) does not exist"
                .to_string()
                .into(),
        },
    ] {
        let wire = et.wire_value();
        let parsed = ErrorType::from_wire(&wire).unwrap();
        assert_eq!(et, parsed);
    }
}

#[test]
fn error_type_invalid_task_wire_format() {
    let et = ErrorType::InvalidTask {
        reason: "duplicate task id".to_string().into(),
    };
    assert_eq!(et.wire_value(), "invalid_task:duplicate task id");
}

#[test]
fn error_type_invalid_task_wire_roundtrip_reason_with_colons_and_edges() {
    // `from_wire` strips only the `invalid_task:` prefix and keeps the
    // remainder verbatim, so a reason containing colons (and other
    // edge characters that are NOT newlines, which would break the
    // line-oriented text codec's framing) round-trips losslessly.
    for reason in [
        "missing dep: phase-a:task-7 :: also phase-b:task-9",
        ":leading-colon",
        "trailing-colon:",
        "tabs\tand spaces and unicode → ✓",
        "", // empty reason
    ] {
        let et = ErrorType::InvalidTask {
            reason: reason.to_string().into(),
        };
        let wire = et.wire_value();
        assert_eq!(wire, format!("invalid_task:{reason}"));
        let parsed = ErrorType::from_wire(&wire).unwrap();
        assert_eq!(parsed, et);
        match parsed {
            ErrorType::InvalidTask { reason: r } => assert_eq!(r.as_str(), reason),
            other => panic!("expected InvalidTask, got {other:?}"),
        }
    }
}

#[test]
fn error_type_invalid_task_distinct_from_unfulfillable_on_wire() {
    // The two reason-bearing variants must not collide on the wire:
    // `invalid_task:` and `unfulfillable:` share no prefix, so a tag
    // for one never parses as the other.
    let invalid = ErrorType::InvalidTask {
        reason: "x".to_string().into(),
    };
    let unfulfillable = ErrorType::Unfulfillable {
        reason: "x".to_string().into(),
    };
    assert_eq!(
        ErrorType::from_wire(&invalid.wire_value()).unwrap(),
        invalid
    );
    assert_eq!(
        ErrorType::from_wire(&unfulfillable.wire_value()).unwrap(),
        unfulfillable
    );
    assert_ne!(invalid.wire_value(), unfulfillable.wire_value());
}

#[test]
fn error_type_unfulfillable_wire_format() {
    let et = ErrorType::Unfulfillable {
        reason: "missing dep".to_string().into(),
    };
    assert_eq!(et.wire_value(), "unfulfillable:missing dep");
}

#[test]
fn error_type_unfulfillable_json_roundtrip() {
    // serde-derive of `ErrorType` uses external tagging for variants
    // with payloads (the existing convention, matching how
    // `ResourceExhausted` already serialises). The wire JSON for
    // `Unfulfillable` is the same shape: a single-key object whose
    // key is the variant name and value is the struct body.
    let et = ErrorType::Unfulfillable {
        reason: "fail".to_string().into(),
    };
    let json = serde_json::to_string(&et).unwrap();
    let parsed: ErrorType = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed, et);
}

#[test]
fn error_type_unfulfillable_deserialise_caps_oversize_reason() {
    // A peer that sends an oversized `reason` must not be able to
    // make the receiver hold an unbounded buffer. The cap lives in
    // the `BoundedString<2048>` deserialiser, which trims to a
    // UTF-8 boundary on the way in.
    let body = "x".repeat(4097);
    let json = format!("{{\"Unfulfillable\":{{\"reason\":\"{}\"}}}}", body);
    let parsed: ErrorType = serde_json::from_str(&json).unwrap();
    match parsed {
        ErrorType::Unfulfillable { reason } => {
            assert_eq!(reason.as_str().len(), 2048);
        }
        other => panic!("expected Unfulfillable, got {other:?}"),
    }
}

#[test]
fn error_type_wire_roundtrip_custom_kind() {
    let et = ErrorType::ResourceExhausted(ResourceKind::new("gpu_vram"));
    let wire = et.wire_value();
    assert_eq!(wire, "resource_exhausted:gpu_vram");
    let parsed = ErrorType::from_wire(&wire).unwrap();
    assert_eq!(et, parsed);
}

#[test]
fn error_type_from_wire_unknown_returns_none() {
    assert!(ErrorType::from_wire("garbage").is_none());
}

#[test]
fn error_type_from_wire_forward_compat() {
    let parsed = ErrorType::from_wire("resource_exhausted:memory").unwrap();
    assert_eq!(parsed, ErrorType::ResourceExhausted(ResourceKind::memory()));
}

#[test]
fn task_result_ok() {
    let r: TaskResult = TaskResult::ok();
    assert!(r.success);
    assert!(r.error_type.is_none());
    assert!(r.result.is_none());
}

#[test]
fn task_result_ok_with_payload() {
    #[derive(Clone, Debug, PartialEq)]
    struct Payload {
        warnings: u32,
        filtered: u32,
    }
    let r = TaskResult::ok_with(Payload {
        warnings: 3,
        filtered: 5,
    });
    assert!(r.success);
    assert_eq!(
        r.result,
        Some(Payload {
            warnings: 3,
            filtered: 5
        })
    );
}

#[test]
fn task_result_error() {
    let r: TaskResult = TaskResult::error(
        ErrorType::ResourceExhausted(ResourceKind::memory()),
        "out of memory".into(),
    );
    assert!(!r.success);
    assert_eq!(
        r.error_type,
        Some(ErrorType::ResourceExhausted(ResourceKind::memory()))
    );
}

#[test]
fn task_info_generic() {
    let bi = TaskInfo {
        path: PathBuf::from("/tmp/test"),
        size: 1024,
        identifier: TestId("test-binary".into()),
        phase_id: PhaseId::from("default"),
        type_id: TypeId::from("default"),
        affinity_id: None,
        payload: serde_json::Value::Null,
        task_id: "test-task".into(),
        task_depends_on: vec![],
        preferred_secondaries: SoftPreferredSecondaries::default(),
        preferred_version: Default::default(),
        kind: Default::default(),
        resolved_path: None,
    };
    assert_eq!(bi.size, 1024);
    assert_eq!(bi.identifier.0, "test-binary");
}

#[test]
fn task_info_serde_roundtrip_with_phase_fields() {
    let bi = TaskInfo {
        path: PathBuf::from("/tmp/test"),
        size: 4096,
        identifier: TestId("rt-binary".into()),
        phase_id: PhaseId::from("warmup"),
        type_id: TypeId::from("tokenize"),
        affinity_id: Some(AffinityId::from("shard-7")),
        payload: serde_json::json!({"k": "v", "n": 42}),
        task_id: "test-task".into(),
        task_depends_on: vec![],
        preferred_secondaries: SoftPreferredSecondaries::default(),
        preferred_version: Default::default(),
        kind: Default::default(),
        resolved_path: None,
    };
    let json = serde_json::to_string(&bi).unwrap();
    let parsed: TaskInfo<TestId> = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.path, bi.path);
    assert_eq!(parsed.size, bi.size);
    assert_eq!(parsed.identifier, bi.identifier);
    assert_eq!(parsed.phase_id, bi.phase_id);
    assert_eq!(parsed.type_id, bi.type_id);
    assert_eq!(parsed.affinity_id, bi.affinity_id);
    assert_eq!(parsed.payload, bi.payload);
}

#[test]
fn resource_map_basic() {
    let mut map = ResourceMap::new();
    let mem = ResourceKind::memory();
    assert!(map.is_empty());
    assert_eq!(map.get(&mem), 0);

    map.insert(mem.clone(), 1024);
    assert!(!map.is_empty());
    assert_eq!(map.get(&mem), 1024);
    assert!(map.contains_key(&mem));
}

#[test]
fn resource_map_from_array() {
    let map = ResourceMap::from([(ResourceKind::memory(), 2048)]);
    assert_eq!(map.get(&ResourceKind::memory()), 2048);
}

#[test]
fn resource_map_display() {
    let map = ResourceMap::from([(ResourceKind::memory(), 1024)]);
    assert_eq!(format!("{map}"), "{memory: 1024}");
}

#[test]
fn phase_id_constructors_clone_display_serde() {
    let a = PhaseId::new("foo");
    let b = PhaseId::from("foo");
    let c = PhaseId::from("foo".to_string());
    assert_eq!(a, b);
    assert_eq!(a, c);

    let cloned = a.clone();
    assert!(std::ptr::eq(a.as_str().as_ptr(), cloned.as_str().as_ptr()));

    assert_eq!(format!("{}", PhaseId::from("x")), "x");

    let json = serde_json::to_string(&PhaseId::from("x")).unwrap();
    assert_eq!(json, "\"x\"");
    let parsed: PhaseId = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed, PhaseId::from("x"));
}

#[test]
fn type_id_constructors_clone_display_serde() {
    let a = TypeId::new("foo");
    let b = TypeId::from("foo");
    let c = TypeId::from("foo".to_string());
    assert_eq!(a, b);
    assert_eq!(a, c);

    let cloned = a.clone();
    assert!(std::ptr::eq(a.as_str().as_ptr(), cloned.as_str().as_ptr()));

    assert_eq!(format!("{}", TypeId::from("x")), "x");

    let json = serde_json::to_string(&TypeId::from("x")).unwrap();
    assert_eq!(json, "\"x\"");
    let parsed: TypeId = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed, TypeId::from("x"));
}

#[test]
fn affinity_id_constructors_clone_display_serde() {
    let a = AffinityId::new("foo");
    let b = AffinityId::from("foo");
    let c = AffinityId::from("foo".to_string());
    assert_eq!(a, b);
    assert_eq!(a, c);

    let cloned = a.clone();
    assert!(std::ptr::eq(a.as_str().as_ptr(), cloned.as_str().as_ptr()));

    assert_eq!(format!("{}", AffinityId::from("x")), "x");

    let json = serde_json::to_string(&AffinityId::from("x")).unwrap();
    assert_eq!(json, "\"x\"");
    let parsed: AffinityId = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed, AffinityId::from("x"));
}

#[test]
fn soft_preferred_secondaries_round_trips_through_serde() {
    // Transparent newtype: the wire form must be indistinguishable from
    // a bare `Vec<String>`, otherwise pre-this-change peers that emit the
    // bare-vec shape (or omit the field entirely, see the
    // `task_info_preferred_secondaries_default_empty` test) would fail
    // to decode. Pinning the wire shape as the bare list here means a
    // future "strict" sibling newtype can ship without disturbing this
    // wire contract — the boundary is the type, not the JSON shape.
    let hint = SoftPreferredSecondaries::new(vec!["sec-a".into(), "sec-b".into()]);
    let json = serde_json::to_string(&hint).unwrap();
    assert_eq!(json, "[\"sec-a\",\"sec-b\"]");
    let parsed: SoftPreferredSecondaries = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed, hint);
    assert_eq!(
        parsed.as_slice(),
        &["sec-a".to_string(), "sec-b".to_string()]
    );
    assert!(!parsed.is_empty());

    let empty = SoftPreferredSecondaries::default();
    assert!(empty.is_empty());
    let empty_json = serde_json::to_string(&empty).unwrap();
    assert_eq!(empty_json, "[]");
    let parsed_empty: SoftPreferredSecondaries = serde_json::from_str(&empty_json).unwrap();
    assert_eq!(parsed_empty, empty);
}

#[test]
fn task_info_preferred_secondaries_default_empty() {
    // Regression: pre-this-change peers don't emit
    // `preferred_secondaries` on the wire at all. `#[serde(default)]`
    // on the field has to fill in the empty value during deserialise,
    // otherwise rolling upgrades break in the receive direction. The
    // matching `skip_serializing_if = "…is_empty"` on the host field
    // keeps the wire silent for the common empty case on the send
    // direction, so the wire is symmetrically backward-compatible.
    let json = serde_json::json!({
        "path": "/tmp/legacy",
        "size": 64,
        "identifier": {"old": "shape"},
        "phase_id": "default",
        "type_id": "default",
        "affinity_id": null,
        "payload": null,
        "task_id": "legacy-task",
        "task_depends_on": [],
        // NOTE: no `preferred_secondaries` key — simulates a
        // pre-this-change peer.
    });

    #[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
    struct ShapeId {
        old: String,
    }

    let parsed: TaskInfo<ShapeId> = serde_json::from_value(json).unwrap();
    assert!(parsed.preferred_secondaries.is_empty());

    // And: an empty hint serialises without emitting the key, so the
    // wire round-trip pre→post→pre carries no extra field.
    let bi = TaskInfo {
        path: PathBuf::from("/tmp/x"),
        size: 8,
        identifier: ShapeId {
            old: "shape".into(),
        },
        phase_id: PhaseId::from("default"),
        type_id: TypeId::from("default"),
        affinity_id: None,
        payload: serde_json::Value::Null,
        task_id: "test-task".into(),
        task_depends_on: vec![],
        preferred_secondaries: SoftPreferredSecondaries::default(),
        preferred_version: Default::default(),
        kind: Default::default(),
        resolved_path: None,
    };
    let re_json = serde_json::to_value(&bi).unwrap();
    assert!(
        re_json.get("preferred_secondaries").is_none(),
        "empty preferred_secondaries must be omitted from the wire \
         (skip_serializing_if), got: {re_json}"
    );
}
