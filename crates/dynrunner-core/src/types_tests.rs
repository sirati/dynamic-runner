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
fn binary_info_generic() {
    let bi = BinaryInfo {
        path: PathBuf::from("/tmp/test"),
        size: 1024,
        identifier: TestId("test-binary".into()),
    };
    assert_eq!(bi.size, 1024);
    assert_eq!(bi.identifier.0, "test-binary");
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
