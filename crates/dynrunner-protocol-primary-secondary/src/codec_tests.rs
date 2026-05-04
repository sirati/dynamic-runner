use super::*;
use crate::messages::*;
use serde::{Deserialize, Serialize};

/// Test identifier matching the tokenizer's wire format.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct TestId {
    binary_name: String,
    platform: String,
    compiler: String,
    version: String,
    opt_level: String,
}

fn test_id(name: &str) -> TestId {
    TestId {
        binary_name: name.into(),
        platform: "x86_64".into(),
        compiler: "gcc".into(),
        version: "12.0".into(),
        opt_level: "O2".into(),
    }
}

#[test]
fn roundtrip_keepalive() {
    let msg: DistributedMessage<TestId> = DistributedMessage::Keepalive {
        sender_id: "sec-1".into(),
        timestamp: 1234.5,
        secondary_id: "sec-1".into(),
        active_workers: 4,
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

#[test]
fn roundtrip_secondary_welcome() {
    use dynrunner_core::{ResourceAmount, ResourceKind};
    let msg: DistributedMessage<TestId> = DistributedMessage::SecondaryWelcome {
        sender_id: "sec-2".into(),
        timestamp: 9999.0,
        secondary_id: "sec-2".into(),
        resources: vec![ResourceAmount { kind: ResourceKind::memory(), amount: 8 * 1024 * 1024 * 1024 }],
        worker_count: 4,
        hostname: "node-01".into(),
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
        },
        local_path: "test".into(),
        file_hash: "abc123".into(),
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
        error_type: "oom".into(),
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
            assert_eq!(error_type, "oom");
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
            },
            PeerConnectionInfo {
                secondary_id: "sec-2".into(),
                cert: "PEM2".into(),
                ipv4: None,
                ipv6: Some("::1".into()),
                port: 5001,
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

#[test]
fn decode_frame_incomplete_length() {
    assert!(decode_frame::<TestId>(&[0, 0]).unwrap().is_none());
}

#[test]
fn decode_frame_incomplete_body() {
    assert!(decode_frame::<TestId>(&[0, 0, 0, 10, 1, 2, 3]).unwrap().is_none());
}

#[test]
fn msg_type_and_sender() {
    let msg: DistributedMessage<TestId> = DistributedMessage::Entropy {
        sender_id: "primary".into(),
        timestamp: 1.0,
        entropy_hex: "deadbeef".into(),
    };
    assert_eq!(msg.sender_id(), "primary");
    assert_eq!(msg.msg_type(), MessageType::Entropy);
}

#[test]
fn roundtrip_all_message_types() {
    use dynrunner_core::{ResourceAmount, ResourceKind};
    let messages: Vec<DistributedMessage<TestId>> = vec![
        DistributedMessage::SecondaryWelcome {
            sender_id: "s".into(),
            timestamp: 0.0,
            secondary_id: "s".into(),
            resources: vec![ResourceAmount { kind: ResourceKind::memory(), amount: 1024 }],
            worker_count: 1,
            hostname: "h".into(),
        },
        DistributedMessage::Entropy {
            sender_id: "p".into(),
            timestamp: 0.0,
            entropy_hex: "aa".into(),
        },
        DistributedMessage::CertExchange {
            sender_id: "s".into(),
            timestamp: 0.0,
            secondary_id: "s".into(),
            public_cert_pem: "cert".into(),
            ipv4_address: None,
            ipv6_address: None,
            quic_port: 5000,
        },
        DistributedMessage::PeerInfo {
            sender_id: "p".into(),
            timestamp: 0.0,
            peers: vec![],
        },
        DistributedMessage::InitialAssignment {
            pre_staged_mode: false,
            uses_file_based_items: true,
            sender_id: "p".into(),
            timestamp: 0.0,
            secondary_id: "s".into(),
            zip_files: vec![],
            workers_ready: vec![],
            staged_files: vec![],
        },
        DistributedMessage::TaskRequest {
            sender_id: "s".into(),
            timestamp: 0.0,
            secondary_id: "s".into(),
            worker_id: 0,
            available_resources: vec![ResourceAmount { kind: ResourceKind::memory(), amount: 1024 }],
        },
        DistributedMessage::TaskAssignment {
            sender_id: "p".into(),
            timestamp: 0.0,
            secondary_id: "s".into(),
            worker_id: 0,
            zip_file: None,
            binary_info: DistributedBinaryInfo {
                path: "p".into(),
                size: 1,
                identifier: test_id("b"),
                phase_id: "default".into(),
                type_id: "default".into(),
                affinity_id: None,
                payload_json: "null".into(),
            },
            local_path: "l".into(),
            file_hash: "h".into(),
        },
        DistributedMessage::TransferComplete {
            sender_id: "p".into(),
            timestamp: 0.0,
            total_files: 10,
            total_bytes: 1024,
        },
        DistributedMessage::PromotePrimary {
            sender_id: "p".into(),
            timestamp: 0.0,
            new_primary_id: "s".into(),
        },
        DistributedMessage::FullTaskList {
            sender_id: "p".into(),
            timestamp: 0.0,
            all_tasks: vec![],
            completed_tasks: vec![],
            pending_tasks: vec![],
            phase_deps: std::collections::HashMap::new(),
        },
        DistributedMessage::TaskComplete {
            sender_id: "s".into(),
            timestamp: 0.0,
            secondary_id: "s".into(),
            worker_id: 0,
            task_hash: "h".into(),
            result_data: None,
        },
        DistributedMessage::TaskFailed {
            sender_id: "s".into(),
            timestamp: 0.0,
            secondary_id: "s".into(),
            worker_id: 0,
            task_hash: "h".into(),
            error_type: "oom".into(),
            error_message: "m".into(),
        },
        DistributedMessage::Keepalive {
            sender_id: "s".into(),
            timestamp: 0.0,
            secondary_id: "s".into(),
            active_workers: 1,
        },
        DistributedMessage::TimeoutDetected {
            sender_id: "s".into(),
            timestamp: 0.0,
            timed_out_secondary_id: "s2".into(),
            last_seen: 0.0,
        },
        DistributedMessage::TimeoutQuery {
            sender_id: "s".into(),
            timestamp: 0.0,
            query_node_id: "s2".into(),
        },
        DistributedMessage::TimeoutResponse {
            sender_id: "s".into(),
            timestamp: 0.0,
            query_node_id: "s2".into(),
            last_keepalive: Some(1.0),
        },
        DistributedMessage::PromotionVote {
            sender_id: "s".into(),
            timestamp: 0.0,
            candidate_id: "s".into(),
            vote_round: 1,
        },
        DistributedMessage::PromotionConfirm {
            sender_id: "s".into(),
            timestamp: 0.0,
            new_primary_id: "s".into(),
            vote_round: 1,
        },
    ];

    for msg in &messages {
        let bytes = serialize_message(msg).unwrap();
        let (decoded, consumed) = decode_frame::<TestId>(&bytes).unwrap().unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded.msg_type(), msg.msg_type());
        assert_eq!(decoded.sender_id(), msg.sender_id());
    }
}

/// Wire format post-B2: identifier is a single nested field (no longer
/// `#[serde(flatten)]`). The runner treats every identifier as an opaque
/// key; the structure pre-B2 was a tokenizer-specific leak through the
/// generic protocol.
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
fn roundtrip_stage_file() {
    let msg: DistributedMessage<TestId> = DistributedMessage::StageFile {
        sender_id: "primary".into(),
        timestamp: 4.2,
        secondary_id: "sec-7".into(),
        file_hash: "abcdef0123456789".into(),
        content_hash: "deadbeef".repeat(8),
        src_path: "rel/to/network/foo.bin".into(),
        dest_path: "scratch/foo.bin".into(),
    };
    let frame = serialize_message(&msg).unwrap();
    let (decoded, n) = decode_frame::<TestId>(&frame).unwrap().unwrap();
    assert_eq!(n, frame.len());
    match decoded {
        DistributedMessage::StageFile {
            sender_id,
            secondary_id,
            file_hash,
            src_path,
            dest_path,
            ..
        } => {
            assert_eq!(sender_id, "primary");
            assert_eq!(secondary_id, "sec-7");
            assert_eq!(file_hash, "abcdef0123456789");
            assert_eq!(src_path, "rel/to/network/foo.bin");
            assert_eq!(dest_path, "scratch/foo.bin");
        }
        other => panic!("expected StageFile, got {:?}", other.msg_type()),
    }
}
