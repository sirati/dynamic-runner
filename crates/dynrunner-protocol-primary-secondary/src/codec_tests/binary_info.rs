use super::*;

#[test]
fn wire_format_flattened_identifier() {
    let msg: DistributedMessage<TestId> = DistributedMessage::TaskAssignment {
        sender_id: "p".into(),
        timestamp: 0.0,
        secondary_id: "s".into(),
        worker_id: 0,
        zip_file: None,
        binary_info: DistributedBinaryInfo {
            path: "/tmp/test".into(),
            size: 1024,
            identifier: test_id("test_binary"),
            phase_id: "default".into(),
            type_id: "default".into(),
            affinity_id: None,
            payload_json: "null".into(),
            task_id: None,
            task_depends_on: vec![],
            preferred_secondaries: Default::default(),
        },
        local_path: "test".into(),
        file_hash: "h".into(),
    };

    let json = serde_json::to_string(&msg).unwrap();
    let v: serde_json::Value = serde_json::from_str(&json).unwrap();

    let bi = &v["binary_info"];
    assert_eq!(bi["path"], "/tmp/test");
    assert_eq!(bi["size"], 1024);
    // Identifier is now nested as a single field; pre-B2 the tokenizer's
    // 5 fields were flattened directly into binary_info.
    assert!(bi.get("identifier").is_some());
    let id = &bi["identifier"];
    assert_eq!(id["binary_name"], "test_binary");
    assert_eq!(id["platform"], "x86_64");
}

/// Phase 4b: `DistributedBinaryInfo` carries phase/type/affinity/payload
/// across the wire so secondaries hydrate `TaskInfo<I>` with the exact
/// tags the primary held in its `PendingPool` — round-trip preserves
/// all four new fields end-to-end.
#[test]
fn roundtrip_distributed_binary_info_phase_tags() {
    let msg: DistributedMessage<TestId> = DistributedMessage::TaskAssignment {
        sender_id: "primary".into(),
        timestamp: 0.0,
        secondary_id: "sec-0".into(),
        worker_id: 3,
        zip_file: None,
        binary_info: DistributedBinaryInfo {
            path: "/tmp/x".into(),
            size: 42,
            identifier: test_id("phased"),
            phase_id: "embed".into(),
            type_id: "tokenize".into(),
            affinity_id: Some("shard_7".into()),
            payload_json: "{\"shard\":7,\"chunk\":\"abc\"}".into(),
            task_id: None,
            task_depends_on: vec![],
            preferred_secondaries: Default::default(),
        },
        local_path: "x".into(),
        file_hash: "h".into(),
    };
    let bytes = serialize_message(&msg).unwrap();
    let (decoded, _) = decode_frame::<TestId>(&bytes).unwrap().unwrap();
    match decoded {
        DistributedMessage::TaskAssignment { binary_info, .. } => {
            assert_eq!(binary_info.phase_id, "embed");
            assert_eq!(binary_info.type_id, "tokenize");
            assert_eq!(binary_info.affinity_id.as_deref(), Some("shard_7"));
            assert_eq!(binary_info.payload_json, "{\"shard\":7,\"chunk\":\"abc\"}");
        }
        _ => panic!("expected TaskAssignment"),
    }
}

/// Backward-compat: a JSON payload from a pre-Phase-4b sender (missing
/// the new four fields) decodes with sensible defaults — `default` for
/// `phase_id`/`type_id`, `None` for `affinity_id`, `"null"` for
/// `payload_json`. Without `#[serde(default)]` this would refuse the
/// frame and break rolling upgrades.
#[test]
fn legacy_distributed_binary_info_decodes_with_defaults() {
    let legacy = serde_json::json!({
        "msg_type": "task_assignment",
        "sender_id": "primary",
        "timestamp": 0.0,
        "secondary_id": "sec-0",
        "worker_id": 0,
        "zip_file": null,
        "binary_info": {
            "path": "/tmp/x",
            "size": 1,
            "identifier": {
                "binary_name": "legacy",
                "platform": "x86_64",
                "compiler": "gcc",
                "version": "12.0",
                "opt_level": "O2"
            }
        },
        "local_path": "x",
        "file_hash": "h"
    });
    let json = serde_json::to_vec(&legacy).unwrap();
    let decoded: DistributedMessage<TestId> = serde_json::from_slice(&json).unwrap();
    match decoded {
        DistributedMessage::TaskAssignment { binary_info, .. } => {
            assert_eq!(binary_info.phase_id, "default");
            assert_eq!(binary_info.type_id, "default");
            assert_eq!(binary_info.affinity_id, None);
            assert_eq!(binary_info.payload_json, "null");
        }
        _ => panic!("expected TaskAssignment"),
    }
}

/// StageFile roundtrip: per-file location notification serializes
/// cleanly and the secondary-side fields are intact after decode.
#[test]
fn distributed_binary_info_omits_empty_field_on_wire() {
    let empty = DistributedBinaryInfo {
        path: "/tmp/x".into(),
        size: 1,
        identifier: test_id("empty"),
        phase_id: "default".into(),
        type_id: "default".into(),
        affinity_id: None,
        payload_json: "null".into(),
        task_id: None,
        task_depends_on: vec![],
        preferred_secondaries: Default::default(),
    };
    let v = serde_json::to_value(&empty).unwrap();
    assert!(
        v.get("preferred_secondaries").is_none(),
        "empty preferred_secondaries must be omitted (skip_serializing_if), got: {v}"
    );

    let populated = DistributedBinaryInfo {
        path: "/tmp/y".into(),
        size: 2,
        identifier: test_id("populated"),
        phase_id: "default".into(),
        type_id: "default".into(),
        affinity_id: None,
        payload_json: "null".into(),
        task_id: None,
        task_depends_on: vec![],
        preferred_secondaries: dynrunner_core::SoftPreferredSecondaries::new(vec![
            "sec-alpha".into(),
            "sec-beta".into(),
        ]),
    };
    let v = serde_json::to_value(&populated).unwrap();
    assert_eq!(
        v.get("preferred_secondaries").unwrap(),
        &serde_json::json!(["sec-alpha", "sec-beta"])
    );

    // Full round-trip preserves the hint.
    let bytes = serialize_message(&DistributedMessage::TaskAssignment {
        sender_id: "p".into(),
        timestamp: 0.0,
        secondary_id: "s".into(),
        worker_id: 0,
        zip_file: None,
        binary_info: populated.clone(),
        local_path: "l".into(),
        file_hash: "h".into(),
    })
    .unwrap();
    let (decoded, _) = decode_frame::<TestId>(&bytes).unwrap().unwrap();
    match decoded {
        DistributedMessage::TaskAssignment { binary_info, .. } => {
            assert_eq!(
                binary_info.preferred_secondaries.as_slice(),
                &["sec-alpha".to_string(), "sec-beta".to_string()],
            );
        }
        _ => panic!("expected TaskAssignment"),
    }
}
