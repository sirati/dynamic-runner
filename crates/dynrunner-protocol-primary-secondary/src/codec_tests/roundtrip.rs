use super::*;

#[test]
fn roundtrip_keepalive() {
    let msg: DistributedMessage<TestId> = DistributedMessage::Keepalive {
        sender_id: "sec-1".into(),
        timestamp: 1234.5,
        secondary_id: "sec-1".into(),
        active_workers: 4,
        emitter_role: KeepaliveRole::Secondary,
    };

    let bytes = serialize_message(&msg).unwrap();
    let (decoded, consumed) = decode_frame::<TestId>(&bytes).unwrap().unwrap();
    assert_eq!(consumed, bytes.len());

    match decoded {
        DistributedMessage::Keepalive {
            sender_id,
            secondary_id,
            active_workers,
            ..
        } => {
            assert_eq!(sender_id, "sec-1");
            assert_eq!(secondary_id, "sec-1");
            assert_eq!(active_workers, 4);
        }
        _ => panic!("expected Keepalive"),
    }
}

/// `emitter_role` survives the wire on its NON-default value. `Secondary`
/// is the `#[serde(default)]`, so `roundtrip_keepalive` (which uses
/// `Secondary`) would still pass even if the field were dropped on the
/// wire — a default masks a dropped layer perfectly. A `Primary`
/// keepalive must decode back as `Primary` for primary-liveness tracking
/// to be distinguishable from peer-mesh liveness.
#[test]
fn roundtrip_keepalive_primary_emitter_role() {
    let msg: DistributedMessage<TestId> = DistributedMessage::Keepalive {
        sender_id: "primary".into(),
        timestamp: 1234.5,
        secondary_id: "primary".into(),
        active_workers: 0,
        emitter_role: KeepaliveRole::Primary,
    };

    let bytes = serialize_message(&msg).unwrap();
    let (decoded, consumed) = decode_frame::<TestId>(&bytes).unwrap().unwrap();
    assert_eq!(consumed, bytes.len());

    match decoded {
        DistributedMessage::Keepalive { emitter_role, .. } => {
            assert_eq!(emitter_role, KeepaliveRole::Primary);
        }
        _ => panic!("expected Keepalive"),
    }
}

/// The full `StateDigest` payload survives the wire byte-for-byte. The
/// every-variant sweep only checks `msg_type`/`sender_id`, so this is the
/// guard that a dropped digest field (or a serde-shape regression) is
/// caught — every count, fold, scalar, and latch must decode back equal.
#[test]
fn roundtrip_state_digest_payload() {
    let digest = StateDigest {
        tasks_count: 11,
        tasks_hash: 0x0123_4567_89AB_CDEF,
        secondary_capacities_count: 3,
        secondary_capacities_hash: 0xFEED,
        task_outputs_count: 6,
        task_outputs_hash: 0x9999_8888,
        phase_deps_count: 7,
        primary_epoch: 42,
        run_complete: true,
        run_aborted: true,
    };
    let msg: DistributedMessage<TestId> = DistributedMessage::StateDigest {
        sender_id: "sec-7".into(),
        timestamp: 1234.5,
        digest,
    };

    let bytes = serialize_message(&msg).unwrap();
    let (decoded, consumed) = decode_frame::<TestId>(&bytes).unwrap().unwrap();
    assert_eq!(consumed, bytes.len());

    match decoded {
        DistributedMessage::StateDigest {
            sender_id,
            digest: decoded_digest,
            ..
        } => {
            assert_eq!(sender_id, "sec-7");
            assert_eq!(decoded_digest, digest);
        }
        _ => panic!("expected StateDigest"),
    }
}

#[test]
fn roundtrip_secondary_welcome() {
    use dynrunner_core::{ResourceAmount, ResourceKind};
    let msg: DistributedMessage<TestId> = DistributedMessage::SecondaryWelcome {
        sender_id: "sec-2".into(),
        timestamp: 9999.0,
        secondary_id: "sec-2".into(),
        resources: vec![ResourceAmount {
            kind: ResourceKind::memory(),
            amount: 8 * 1024 * 1024 * 1024,
        }],
        worker_count: 4,
        hostname: "node-01".into(),
        is_observer: false,
        can_be_primary: true,
    };

    let bytes = serialize_message(&msg).unwrap();
    let (decoded, _) = decode_frame::<TestId>(&bytes).unwrap().unwrap();

    match decoded {
        DistributedMessage::SecondaryWelcome {
            resources,
            worker_count,
            hostname,
            ..
        } => {
            assert_eq!(resources[0].amount, 8 * 1024 * 1024 * 1024);
            assert_eq!(worker_count, 4);
            assert_eq!(hostname, "node-01");
        }
        _ => panic!("expected SecondaryWelcome"),
    }
}

#[test]
fn roundtrip_task_assignment() {
    let msg: DistributedMessage<TestId> = DistributedMessage::TaskAssignment {
        sender_id: "primary".into(),
        timestamp: 100.0,
        secondary_id: "sec-1".into(),
        worker_id: 0,
        zip_file: Some("batch_0.zip".into()),
        binary_info: DistributedBinaryInfo {
            path: "/data/bins/test".into(),
            size: 1024,
            identifier: test_id("test"),
            phase_id: "phase_a".into(),
            type_id: "type_x".into(),
            affinity_id: Some("aff_42".into()),
            payload_json: "{\"k\":1}".into(),
            task_id: "roundtrip-task".into(),
            task_depends_on: vec![],
            preferred_secondaries: Default::default(),
        },
        local_path: "test".into(),
        file_hash: "abc123".into(),
        predecessor_outputs: std::collections::BTreeMap::new(),
    };

    let bytes = serialize_message(&msg).unwrap();
    let (decoded, _) = decode_frame::<TestId>(&bytes).unwrap().unwrap();

    match decoded {
        DistributedMessage::TaskAssignment {
            worker_id,
            zip_file,
            binary_info,
            file_hash,
            ..
        } => {
            assert_eq!(worker_id, 0);
            assert_eq!(zip_file.as_deref(), Some("batch_0.zip"));
            assert_eq!(binary_info.identifier.binary_name, "test");
            assert_eq!(file_hash, "abc123");
            // Phase 4b: phase/type/affinity/payload survive the round trip.
            assert_eq!(binary_info.phase_id, "phase_a");
            assert_eq!(binary_info.type_id, "type_x");
            assert_eq!(binary_info.affinity_id.as_deref(), Some("aff_42"));
            assert_eq!(binary_info.payload_json, "{\"k\":1}");
        }
        _ => panic!("expected TaskAssignment"),
    }
}

#[test]
fn roundtrip_task_failed() {
    let msg: DistributedMessage<TestId> = DistributedMessage::TaskFailed {
        sender_id: "sec-1".into(),
        timestamp: 200.0,
        secondary_id: "sec-1".into(),
        worker_id: 2,
        task_hash: "hash123".into(),
        error_type: ErrorType::ResourceExhausted(ResourceKind::memory()),
        error_message: "out of memory".into(),
    };

    let bytes = serialize_message(&msg).unwrap();
    let (decoded, _) = decode_frame::<TestId>(&bytes).unwrap().unwrap();

    match decoded {
        DistributedMessage::TaskFailed {
            error_type,
            error_message,
            ..
        } => {
            assert_eq!(
                error_type,
                ErrorType::ResourceExhausted(ResourceKind::memory())
            );
            assert_eq!(error_message, "out of memory");
        }
        _ => panic!("expected TaskFailed"),
    }
}

#[test]
fn roundtrip_peer_info() {
    let msg: DistributedMessage<TestId> = DistributedMessage::PeerInfo {
        sender_id: "primary".into(),
        timestamp: 300.0,
        peers: vec![
            PeerConnectionInfo {
                secondary_id: "sec-1".into(),
                cert: "PEM1".into(),
                ipv4: Some("10.0.0.1".into()),
                ipv6: None,
                port: 5000,
                is_observer: false,
            },
            PeerConnectionInfo {
                secondary_id: "sec-2".into(),
                cert: "PEM2".into(),
                ipv4: None,
                ipv6: Some("::1".into()),
                port: 5001,
                is_observer: false,
            },
        ],
    };

    let bytes = serialize_message(&msg).unwrap();
    let (decoded, _) = decode_frame::<TestId>(&bytes).unwrap().unwrap();

    match decoded {
        DistributedMessage::PeerInfo { peers, .. } => {
            assert_eq!(peers.len(), 2);
            assert_eq!(peers[0].secondary_id, "sec-1");
            assert_eq!(peers[1].port, 5001);
        }
        _ => panic!("expected PeerInfo"),
    }
}

/// Keyed-outputs feature: a populated `predecessor_outputs` map (one
/// predecessor, one inline value + one file value) round-trips through
/// the codec verbatim.
#[test]
fn roundtrip_task_assignment_predecessor_outputs_populated() {
    use dynrunner_core::{ResultValue, TaskOutputs};
    use std::collections::BTreeMap;

    let mut producer_map: BTreeMap<String, ResultValue> = BTreeMap::new();
    producer_map.insert("nonce".into(), ResultValue::Inline("xyz".into()));
    producer_map.insert(
        "artifact".into(),
        ResultValue::File("/app/out-network/build/foo.tar".into()),
    );

    let mut preds: BTreeMap<String, TaskOutputs> = BTreeMap::new();
    preds.insert("task_a".into(), TaskOutputs(producer_map));

    let msg: DistributedMessage<TestId> = DistributedMessage::TaskAssignment {
        sender_id: "primary".into(),
        timestamp: 0.0,
        secondary_id: "sec-1".into(),
        worker_id: 0,
        zip_file: None,
        binary_info: DistributedBinaryInfo {
            path: "/tmp/b".into(),
            size: 1,
            identifier: test_id("dependent"),
            phase_id: "default".into(),
            type_id: "default".into(),
            affinity_id: None,
            payload_json: "null".into(),
            task_id: "task_b".into(),
            task_depends_on: vec![],
            preferred_secondaries: Default::default(),
        },
        local_path: "b".into(),
        file_hash: "h".into(),
        predecessor_outputs: preds.clone(),
    };

    let bytes = serialize_message(&msg).unwrap();
    let (decoded, _) = decode_frame::<TestId>(&bytes).unwrap().unwrap();
    match decoded {
        DistributedMessage::TaskAssignment {
            predecessor_outputs,
            ..
        } => {
            assert_eq!(predecessor_outputs, preds);
        }
        _ => panic!("expected TaskAssignment"),
    }
}

/// Backcompat: a JSON payload of `TaskAssignment` from a pre-keyed-
/// outputs sender (the `predecessor_outputs` field absent entirely)
/// decodes with the field empty. `#[serde(default)]` is what keeps
/// rolling upgrades working: a primary that has not learned about the
/// field yet can still ship `TaskAssignment` frames to a newer
/// secondary, and vice versa.
#[test]
fn legacy_task_assignment_without_predecessor_outputs_decodes_empty() {
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
        DistributedMessage::TaskAssignment {
            predecessor_outputs,
            ..
        } => {
            assert!(predecessor_outputs.is_empty());
        }
        _ => panic!("expected TaskAssignment"),
    }
}

/// Wire-bytes elision: an empty `predecessor_outputs` map serializes to
/// bytes that do NOT contain the field name. Matches the
/// `preferred_secondaries` / `task_depends_on` "optional fields elide
/// when default" idiom so the no-dep common case keeps the same byte
/// representation it had pre-feature.
#[test]
fn empty_predecessor_outputs_elided_on_wire() {
    let msg: DistributedMessage<TestId> = DistributedMessage::TaskAssignment {
        sender_id: "primary".into(),
        timestamp: 0.0,
        secondary_id: "sec-0".into(),
        worker_id: 0,
        zip_file: None,
        binary_info: DistributedBinaryInfo {
            path: "/tmp/x".into(),
            size: 1,
            identifier: test_id("no_deps"),
            phase_id: "default".into(),
            type_id: "default".into(),
            affinity_id: None,
            payload_json: "null".into(),
            task_id: "no-deps-task".into(),
            task_depends_on: vec![],
            preferred_secondaries: Default::default(),
        },
        local_path: "x".into(),
        file_hash: "h".into(),
        predecessor_outputs: std::collections::BTreeMap::new(),
    };

    let json = serde_json::to_string(&msg).unwrap();
    assert!(
        !json.contains("predecessor_outputs"),
        "empty predecessor_outputs must be elided via skip_serializing_if, got: {json}"
    );
}
