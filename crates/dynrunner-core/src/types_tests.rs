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
    ] {
        let wire = et.wire_value();
        let parsed = ErrorType::from_wire(&wire).unwrap();
        assert_eq!(et, parsed);
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
        task_id: None,
        task_depends_on: vec![],
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
        task_id: None,
        task_depends_on: vec![],
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
