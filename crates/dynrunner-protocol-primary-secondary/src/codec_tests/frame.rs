use super::*;
use dynrunner_core::ErrorType;

#[test]
fn decode_frame_incomplete_length() {
    assert!(decode_frame::<TestId>(&[0, 0]).unwrap().is_none());
}

#[test]
fn decode_frame_incomplete_body() {
    assert!(
        decode_frame::<TestId>(&[0, 0, 0, 10, 1, 2, 3])
            .unwrap()
            .is_none()
    );
}

#[test]
fn msg_type_and_sender() {
    let msg: DistributedMessage<TestId> = DistributedMessage::Entropy {
        target: None,
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
            target: None,
            sender_id: "s".into(),
            timestamp: 0.0,
            secondary_id: "s".into(),
            resources: vec![ResourceAmount {
                kind: ResourceKind::memory(),
                amount: 1024,
            }],
            worker_count: 1,
            hostname: "h".into(),
            is_observer: false,
            can_be_primary: true,
        },
        DistributedMessage::Entropy {
            target: None,
            sender_id: "p".into(),
            timestamp: 0.0,
            entropy_hex: "aa".into(),
        },
        DistributedMessage::CertExchange {
            target: None,
            sender_id: "s".into(),
            timestamp: 0.0,
            secondary_id: "s".into(),
            public_cert_pem: "cert".into(),
            ipv4_address: None,
            ipv6_address: None,
            quic_port: 5000,
            liveness_port: None,
        },
        DistributedMessage::PeerInfo {
            target: None,
            sender_id: "p".into(),
            timestamp: 0.0,
            peers: vec![],
        },
        DistributedMessage::InitialAssignment {
            target: None,
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
            target: None,
            sender_id: "s".into(),
            timestamp: 0.0,
            secondary_id: "s".into(),
            worker_id: 0,
            available_resources: vec![ResourceAmount {
                kind: ResourceKind::memory(),
                amount: 1024,
            }],
        },
        DistributedMessage::TaskAssignment {
            target: None,
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
                task_id: "test-frame-task".into(),
                task_depends_on: vec![],
                preferred_secondaries: Default::default(),
            },
            local_path: "l".into(),
            file_hash: "h".into(),
            predecessor_outputs: std::collections::BTreeMap::new(),
        },
        DistributedMessage::TransferComplete {
            target: None,
            sender_id: "p".into(),
            timestamp: 0.0,
            total_files: 10,
            total_bytes: 1024,
        },
        DistributedMessage::RequestSnapshotStream {
            target: None,
            sender_id: "s".into(),
            timestamp: 0.0,
            stream_id: "s/0".into(),
            resume_after: None,
            task_ranges: Vec::new(),
            is_observer: false,
            can_be_primary: true,
        },
        DistributedMessage::SnapshotStreamPackage {
            target: None,
            sender_id: "p".into(),
            timestamp: 0.0,
            stream_id: "s/0".into(),
            seq: 0,
            cursor: Some("crate-000001".into()),
            payload: "oWE=".into(),
            done: true,
        },
        DistributedMessage::RequestRunConfig {
            target: None,
            sender_id: "s".into(),
            timestamp: 0.0,
        },
        DistributedMessage::RunConfig {
            target: None,
            sender_id: "p".into(),
            timestamp: 0.0,
            forwarded_argv: vec!["--epochs".into(), "3".into()],
        },
        DistributedMessage::StateDigest {
            target: None,
            sender_id: "s".into(),
            timestamp: 0.0,
            digest: StateDigest {
                tasks_count: 7,
                tasks_hash: 0xDEAD_BEEF,
                secondary_capacities_count: 2,
                secondary_capacities_hash: 0x42,
                task_outputs_count: 4,
                task_outputs_hash: 0xABC,
                phase_deps_count: 5,
                phase_deps_hash: 0x55,
                current_primary_hash: 0x66,
                capabilities_count: 3,
                capabilities_hash: 0x77,
                primary_epoch: 3,
                run_complete: true,
                run_aborted: false,
                graceful_abort: true,
                discovery_debt: crate::DiscoveryDebt::Owed,
                phase_event_tallies_count: 6,
                phase_event_tallies_hash: 0x88,
                retry_passes_used_count: 2,
                retry_passes_used_hash: 0x99,
                unfulfillable_reinject_used_count: 1,
                unfulfillable_reinject_used_hash: 0xAA,
                respawn_events_count: 4,
                respawn_events_hash: 0xBB,
                phases_ended_count: 1,
                phases_ended_hash: 0xCC,
                custom_messages_count: 2,
                custom_messages_hash: 0xDD,
                custom_terminal_watermarks_count: 1,
                custom_terminal_watermarks_hash: 0xEE,
            },
            sender_is_observer: true,
        },
        DistributedMessage::TaskComplete {
            target: None,
            sender_id: "s".into(),
            timestamp: 0.0,
            secondary_id: "s".into(),
            worker_id: 0,
            task_hash: "h".into(),
            result_data: None,
            delivery_seq: None,
            // Stamped at the send_to_primary chokepoint (ordering gate).
            msgs_posted_through: None,
        },
        DistributedMessage::TaskFailed {
            target: None,
            sender_id: "s".into(),
            timestamp: 0.0,
            secondary_id: "s".into(),
            worker_id: 0,
            task_hash: "h".into(),
            error_type: ErrorType::ResourceExhausted(ResourceKind::memory()),
            error_message: "m".into(),
            delivery_seq: None,
            // Stamped at the send_to_primary chokepoint (ordering gate).
            msgs_posted_through: None,
        },
        DistributedMessage::TerminalAck {
            target: None,
            sender_id: "p".into(),
            timestamp: 0.0,
            seq: 7,
        },
        DistributedMessage::CustomMessage {
            target: None,
            sender_id: "s".into(),
            timestamp: 0.0,
            origin_secondary_id: "s".into(),
            msg_seq: 3,
            topic: "phase4-batch".into(),
            data: vec![1, 2, 3],
            important: true,
            delivery_seq: Some(9),
        },
        DistributedMessage::TaskHoldQuery {
            target: None,
            sender_id: "p".into(),
            timestamp: 0.0,
            task_hash: "h".into(),
        },
        DistributedMessage::TaskHoldResponse {
            target: None,
            sender_id: "s".into(),
            timestamp: 0.0,
            task_hash: "h".into(),
            held: false,
        },
        DistributedMessage::Keepalive {
            target: None,
            sender_id: "s".into(),
            timestamp: 0.0,
            secondary_id: "s".into(),
            active_workers: 1,
            emitter_role: KeepaliveRole::Secondary,
        },
        DistributedMessage::TimeoutDetected {
            target: None,
            sender_id: "s".into(),
            timestamp: 0.0,
            timed_out_secondary_id: "s2".into(),
            last_seen: 0.0,
        },
        DistributedMessage::TimeoutQuery {
            target: None,
            sender_id: "s".into(),
            timestamp: 0.0,
            query_node_id: "s2".into(),
        },
        DistributedMessage::TimeoutResponse {
            target: None,
            sender_id: "s".into(),
            timestamp: 0.0,
            query_node_id: "s2".into(),
            last_keepalive: Some(1.0),
        },
        DistributedMessage::PromotionVote {
            target: None,
            sender_id: "s".into(),
            timestamp: 0.0,
            candidate_id: "s".into(),
            vote_round: 1,
        },
        DistributedMessage::PromotionConfirm {
            target: None,
            sender_id: "s".into(),
            timestamp: 0.0,
            new_primary_id: "s".into(),
            vote_round: 1,
        },
        DistributedMessage::SecondaryFatalError {
            target: None,
            sender_id: "s".into(),
            timestamp: 0.0,
            secondary_id: "s".into(),
            error:
                "peer mesh fully failed to form: 0 of 4 peers reachable; cluster routing impossible"
                    .into(),
        },
        DistributedMessage::SetupAssignment {
            target: None,
            sender_id: "p".into(),
            timestamp: 0.0,
            secondary_id: "s".into(),
            task_hash: "h".into(),
        },
        DistributedMessage::SetupTerminal {
            target: None,
            sender_id: "s".into(),
            timestamp: 0.0,
            secondary_id: "s".into(),
            task_hash: "h".into(),
            success: true,
            error_message: String::new(),
        },
        DistributedMessage::TaskQueuedAfterLocalDependency {
            target: None,
            sender_id: "s".into(),
            timestamp: 0.0,
            secondary_id: "s".into(),
            task_hash: "h".into(),
            affine_hash: "g".into(),
        },
        DistributedMessage::LocalDependencyReleased {
            target: None,
            sender_id: "s".into(),
            timestamp: 0.0,
            secondary_id: "s".into(),
            task_hash: "h".into(),
            worker_id: 0,
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

/// Wire-shape mirror for `RequestSnapshotStream` (NOT
/// symmetric-on-the-wrong-shape): decode the EXACT JSON bytes a sender's
/// framing layer emits, pinning the tag + field names + optional-field
/// encoding the other side must produce — then re-encode and require the
/// identical bytes back.
#[test]
fn request_snapshot_stream_mirrors_literal_sender_bytes() {
    let literal = r#"{"msg_type":"request_snapshot_stream","sender_id":"joiner-1","timestamp":3.25,"stream_id":"joiner-1/0","resume_after":"crate-000100","is_observer":true,"can_be_primary":false}"#;
    let decoded: DistributedMessage<TestId> = serde_json::from_str(literal).unwrap();
    match &decoded {
        DistributedMessage::RequestSnapshotStream {
            target,
            sender_id,
            timestamp,
            stream_id,
            resume_after,
            task_ranges,
            is_observer,
            can_be_primary,
        } => {
            assert!(target.is_none());
            assert_eq!(sender_id, "joiner-1");
            assert_eq!(*timestamp, 3.25);
            assert_eq!(stream_id, "joiner-1/0");
            assert_eq!(resume_after.as_deref(), Some("crate-000100"));
            // The literal carries no `task_ranges` key (empty = all-ranges,
            // the P0 full stream); it decodes as an empty vec via
            // serde(default) and re-encodes WITHOUT the key
            // (skip_serializing_if), so the literal-mirror below holds.
            assert!(task_ranges.is_empty());
            assert!(*is_observer);
            assert!(!*can_be_primary);
        }
        other => panic!("expected RequestSnapshotStream, got {:?}", other.msg_type()),
    }
    let reencoded = serde_json::to_string(&decoded).unwrap();
    assert_eq!(reencoded, literal);
    // A fresh (non-resume) request omits nothing: `resume_after` rides
    // as an explicit null, and a pre-field sender's frame (no
    // `resume_after` key at all) decodes as `None` via serde(default).
    let pre_field = r#"{"msg_type":"request_snapshot_stream","sender_id":"j","timestamp":0.0,"stream_id":"j/0","is_observer":false,"can_be_primary":true}"#;
    let decoded_pre: DistributedMessage<TestId> = serde_json::from_str(pre_field).unwrap();
    match decoded_pre {
        DistributedMessage::RequestSnapshotStream {
            resume_after,
            task_ranges,
            ..
        } => {
            assert!(resume_after.is_none());
            // A pre-`task_ranges` sender decodes as empty = all-ranges =
            // the P0 full stream: a missing delta NEVER silently drops a
            // range, it only forgoes the narrowing (the data-loss fail-safe).
            assert!(task_ranges.is_empty());
        }
        other => panic!("expected RequestSnapshotStream, got {:?}", other.msg_type()),
    }
    // Wire-shape mirror for a POPULATED delta: the EXACT bytes a P1
    // requester's framing layer emits with a non-empty `task_ranges`, so a
    // field rename or a u16-vs-uXX encoding drift is caught against the
    // other side's literal (not a re-encode of our own value).
    let delta = r#"{"msg_type":"request_snapshot_stream","sender_id":"behind","timestamp":1.0,"stream_id":"behind/2","resume_after":null,"task_ranges":[3,17,255],"is_observer":false,"can_be_primary":false}"#;
    let decoded_delta: DistributedMessage<TestId> = serde_json::from_str(delta).unwrap();
    match &decoded_delta {
        DistributedMessage::RequestSnapshotStream { task_ranges, .. } => {
            assert_eq!(*task_ranges, vec![3u16, 17, 255]);
        }
        other => panic!("expected RequestSnapshotStream, got {:?}", other.msg_type()),
    }
    assert_eq!(serde_json::to_string(&decoded_delta).unwrap(), delta);
}

/// Wire-shape mirror for `SnapshotStreamPackage`: the exact bytes the
/// responder's egress emits — tag, field names, the explicit-null
/// cursor on head/tail packages, and the base64 payload carried
/// verbatim as a JSON string.
#[test]
fn snapshot_stream_package_mirrors_literal_sender_bytes() {
    let literal = r#"{"msg_type":"snapshot_stream_package","sender_id":"resp-1","timestamp":4.5,"stream_id":"joiner-1/0","seq":2,"cursor":"crate-000200","payload":"oWE=","done":false}"#;
    let decoded: DistributedMessage<TestId> = serde_json::from_str(literal).unwrap();
    match &decoded {
        DistributedMessage::SnapshotStreamPackage {
            target,
            sender_id,
            timestamp,
            stream_id,
            seq,
            cursor,
            payload,
            done,
        } => {
            assert!(target.is_none());
            assert_eq!(sender_id, "resp-1");
            assert_eq!(*timestamp, 4.5);
            assert_eq!(stream_id, "joiner-1/0");
            assert_eq!(*seq, 2);
            assert_eq!(cursor.as_deref(), Some("crate-000200"));
            assert_eq!(payload, "oWE=");
            assert!(!*done);
        }
        other => panic!("expected SnapshotStreamPackage, got {:?}", other.msg_type()),
    }
    let reencoded = serde_json::to_string(&decoded).unwrap();
    assert_eq!(reencoded, literal);
    // Head/tail packages carry no cursor (explicit null on the wire).
    let final_pkg = r#"{"msg_type":"snapshot_stream_package","sender_id":"resp-1","timestamp":4.5,"stream_id":"joiner-1/0","seq":9,"cursor":null,"payload":"oWE=","done":true}"#;
    let decoded_final: DistributedMessage<TestId> = serde_json::from_str(final_pkg).unwrap();
    match decoded_final {
        DistributedMessage::SnapshotStreamPackage { cursor, done, .. } => {
            assert!(cursor.is_none());
            assert!(done);
        }
        other => panic!("expected SnapshotStreamPackage, got {:?}", other.msg_type()),
    }
}

/// Wire-shape mirror for `SetupAssignment` (primary → executor member):
/// the EXACT bytes the primary's egress emits — tag, field names, field
/// order, with `target` elided while `None`. Decode the literal, pin every
/// field, then re-encode and require identical bytes back (NOT
/// symmetric-on-the-wrong-shape).
#[test]
fn setup_assignment_mirrors_literal_sender_bytes() {
    let literal = r#"{"msg_type":"setup_assignment","sender_id":"setup","timestamp":7.5,"secondary_id":"sec-0","task_hash":"abc123"}"#;
    let decoded: DistributedMessage<TestId> = serde_json::from_str(literal).unwrap();
    match &decoded {
        DistributedMessage::SetupAssignment {
            target,
            sender_id,
            timestamp,
            secondary_id,
            task_hash,
        } => {
            assert!(target.is_none());
            assert_eq!(sender_id, "setup");
            assert_eq!(*timestamp, 7.5);
            assert_eq!(secondary_id, "sec-0");
            assert_eq!(task_hash, "abc123");
        }
        other => panic!("expected SetupAssignment, got {:?}", other.msg_type()),
    }
    let reencoded = serde_json::to_string(&decoded).unwrap();
    assert_eq!(reencoded, literal);
}

/// Wire-shape mirror for `SetupTerminal` (executor member → primary): the
/// EXACT bytes the off-primary executor's report emits, pinning the
/// `success` bool and `error_message` shape on BOTH the success (empty
/// message) and failure (non-empty message) variants.
#[test]
fn setup_terminal_mirrors_literal_sender_bytes() {
    // Success report.
    let ok = r#"{"msg_type":"setup_terminal","sender_id":"sec-0","timestamp":8.0,"secondary_id":"sec-0","task_hash":"abc123","success":true,"error_message":""}"#;
    let decoded: DistributedMessage<TestId> = serde_json::from_str(ok).unwrap();
    match &decoded {
        DistributedMessage::SetupTerminal {
            target,
            sender_id,
            timestamp,
            secondary_id,
            task_hash,
            success,
            error_message,
        } => {
            assert!(target.is_none());
            assert_eq!(sender_id, "sec-0");
            assert_eq!(*timestamp, 8.0);
            assert_eq!(secondary_id, "sec-0");
            assert_eq!(task_hash, "abc123");
            assert!(*success);
            assert_eq!(error_message, "");
        }
        other => panic!("expected SetupTerminal, got {:?}", other.msg_type()),
    }
    assert_eq!(serde_json::to_string(&decoded).unwrap(), ok);

    // Failure report carries the reason.
    let fail = r#"{"msg_type":"setup_terminal","sender_id":"sec-0","timestamp":8.0,"secondary_id":"sec-0","task_hash":"abc123","success":false,"error_message":"build action failed"}"#;
    let decoded_fail: DistributedMessage<TestId> = serde_json::from_str(fail).unwrap();
    match &decoded_fail {
        DistributedMessage::SetupTerminal {
            success,
            error_message,
            ..
        } => {
            assert!(!*success);
            assert_eq!(error_message, "build action failed");
        }
        other => panic!("expected SetupTerminal, got {:?}", other.msg_type()),
    }
    assert_eq!(serde_json::to_string(&decoded_fail).unwrap(), fail);
}

/// Wire-shape mirror for `TaskQueuedAfterLocalDependency` (#497, secondary →
/// primary): the EXACT bytes the reporting secondary emits — the
/// snake_case `msg_type` tag, the `task_hash` (B) + `affine_hash` (the gate
/// I) field names, `target` elided while `None` — decode, pin every field,
/// then re-encode and require identical bytes back (NOT
/// symmetric-on-the-wrong-shape).
#[test]
fn task_queued_after_local_dependency_mirrors_literal_sender_bytes() {
    let literal = r#"{"msg_type":"task_queued_after_local_dependency","sender_id":"sec-0","timestamp":9.0,"secondary_id":"sec-0","task_hash":"build-h","affine_hash":"import-h"}"#;
    let decoded: DistributedMessage<TestId> = serde_json::from_str(literal).unwrap();
    match &decoded {
        DistributedMessage::TaskQueuedAfterLocalDependency {
            target,
            sender_id,
            timestamp,
            secondary_id,
            task_hash,
            affine_hash,
        } => {
            assert!(target.is_none());
            assert_eq!(sender_id, "sec-0");
            assert_eq!(*timestamp, 9.0);
            assert_eq!(secondary_id, "sec-0");
            assert_eq!(task_hash, "build-h");
            assert_eq!(affine_hash, "import-h");
        }
        other => panic!(
            "expected TaskQueuedAfterLocalDependency, got {:?}",
            other.msg_type()
        ),
    }
    assert_eq!(serde_json::to_string(&decoded).unwrap(), literal);
}

/// Wire-shape mirror for `LocalDependencyReleased` (#497, secondary →
/// primary): the EXACT bytes the releasing secondary emits — the snake_case
/// tag, the `task_hash` (B) + integer `worker_id` field names, `target`
/// elided while `None`.
#[test]
fn local_dependency_released_mirrors_literal_sender_bytes() {
    let literal = r#"{"msg_type":"local_dependency_released","sender_id":"sec-0","timestamp":9.5,"secondary_id":"sec-0","task_hash":"build-h","worker_id":4}"#;
    let decoded: DistributedMessage<TestId> = serde_json::from_str(literal).unwrap();
    match &decoded {
        DistributedMessage::LocalDependencyReleased {
            target,
            sender_id,
            timestamp,
            secondary_id,
            task_hash,
            worker_id,
        } => {
            assert!(target.is_none());
            assert_eq!(sender_id, "sec-0");
            assert_eq!(*timestamp, 9.5);
            assert_eq!(secondary_id, "sec-0");
            assert_eq!(task_hash, "build-h");
            assert_eq!(*worker_id, 4);
        }
        other => panic!(
            "expected LocalDependencyReleased, got {:?}",
            other.msg_type()
        ),
    }
    assert_eq!(serde_json::to_string(&decoded).unwrap(), literal);
}
