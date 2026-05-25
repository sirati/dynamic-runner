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
            task_id: "test-task".into(),
            task_depends_on: vec![],
            preferred_secondaries: Default::default(),
        },
        local_path: "test".into(),
        file_hash: "h".into(),
        predecessor_outputs: std::collections::BTreeMap::new(),
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
            task_id: "phased-task".into(),
            task_depends_on: vec![],
            preferred_secondaries: Default::default(),
        },
        local_path: "x".into(),
        file_hash: "h".into(),
        predecessor_outputs: std::collections::BTreeMap::new(),
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
/// `phase_id`/`type_id`/`affinity_id`/`payload_json`) decodes with
/// sensible defaults — `default` for `phase_id`/`type_id`, `None` for
/// `affinity_id`, `"null"` for `payload_json`. `task_id` is REQUIRED
/// on the wire post-breaking-change so the JSON includes it
/// explicitly; pre-task_id senders are wire-incompatible (a
/// missing-field error is the intentional loud-fail rather than the
/// prior silent-drop-on-anonymous behaviour).
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
            },
            "task_id": "legacy-task"
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
            assert_eq!(binary_info.task_id, "legacy-task");
        }
        _ => panic!("expected TaskAssignment"),
    }
}

/// Wire contract: a payload that omits `task_id` fails to decode loudly
/// (no silent default). Pins the breaking-change behaviour so a future
/// re-introduction of `#[serde(default)]` on `task_id` would be caught
/// by this test.
#[test]
fn distributed_binary_info_missing_task_id_fails_decode() {
    let missing_task_id = serde_json::json!({
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
                "binary_name": "no-task-id",
                "platform": "x86_64",
                "compiler": "gcc",
                "version": "12.0",
                "opt_level": "O2"
            }
        },
        "local_path": "x",
        "file_hash": "h"
    });
    let json = serde_json::to_vec(&missing_task_id).unwrap();
    let result: Result<DistributedMessage<TestId>, _> = serde_json::from_slice(&json);
    assert!(
        result.is_err(),
        "missing `task_id` must fail decode loudly; got {result:?}"
    );
}

/// Wire backcompat for the `task_depends_on` upgrade from `Vec<String>`
/// to `Vec<TaskDep>`: a `DistributedBinaryInfo` payload emitted by a
/// pre-keyed-outputs sender carries bare-string elements
/// (`["a", "b"]`). `TaskDep`'s `#[serde(untagged)]` deserializer must
/// accept those without an explicit `inherit_outputs` key and decode
/// each as `TaskDep { task_id, inherit_outputs: false }`. Without the
/// nested untagged decoder, rolling upgrades would refuse legacy
/// assignment frames.
#[test]
fn distributed_binary_info_task_depends_on_decodes_bare_strings() {
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
            },
            "task_id": "legacy-task",
            "task_depends_on": ["a", "b"]
        },
        "local_path": "x",
        "file_hash": "h"
    });
    let json = serde_json::to_vec(&legacy).unwrap();
    let decoded: DistributedMessage<TestId> = serde_json::from_slice(&json).unwrap();
    match decoded {
        DistributedMessage::TaskAssignment { binary_info, .. } => {
            assert_eq!(binary_info.task_depends_on.len(), 2);
            assert_eq!(binary_info.task_depends_on[0].task_id, "a");
            assert!(!binary_info.task_depends_on[0].inherit_outputs);
            assert_eq!(binary_info.task_depends_on[1].task_id, "b");
            assert!(!binary_info.task_depends_on[1].inherit_outputs);
        }
        _ => panic!("expected TaskAssignment"),
    }
}

/// Mixed wire shape forwards-compat: a sender that emits a mix of
/// bare-string and full-struct entries within `task_depends_on` decodes
/// element-by-element. Pin alongside the legacy-only case because the
/// `Vec<TaskDep>` derive on `DistributedBinaryInfo` defers to the
/// element's `Deserialize`, and this test guards against accidental
/// `serde(with = ...)` collapses that would force a uniform-shape
/// constraint.
#[test]
fn distributed_binary_info_task_depends_on_decodes_mixed_shapes() {
    let mixed = serde_json::json!({
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
                "binary_name": "mixed",
                "platform": "x86_64",
                "compiler": "gcc",
                "version": "12.0",
                "opt_level": "O2"
            },
            "task_id": "mixed-task",
            "task_depends_on": [
                "legacy_dep",
                { "task_id": "modern_dep", "inherit_outputs": true }
            ]
        },
        "local_path": "x",
        "file_hash": "h"
    });
    let json = serde_json::to_vec(&mixed).unwrap();
    let decoded: DistributedMessage<TestId> = serde_json::from_slice(&json).unwrap();
    match decoded {
        DistributedMessage::TaskAssignment { binary_info, .. } => {
            assert_eq!(binary_info.task_depends_on.len(), 2);
            assert_eq!(binary_info.task_depends_on[0].task_id, "legacy_dep");
            assert!(!binary_info.task_depends_on[0].inherit_outputs);
            assert_eq!(binary_info.task_depends_on[1].task_id, "modern_dep");
            assert!(binary_info.task_depends_on[1].inherit_outputs);
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
        task_id: "wire-task".into(),
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
        task_id: "wire-task".into(),
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
        predecessor_outputs: std::collections::BTreeMap::new(),
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
