use super::*;

#[test]
fn roundtrip_keepalive() {
    let msg: DistributedMessage<TestId> = DistributedMessage::Keepalive {
        target: None,
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
        target: None,
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
        phase_deps_hash: 0xAAAA_BBBB,
        current_primary_hash: 0xCCCC_DDDD,
        capabilities_count: 4,
        capabilities_hash: 0xEEEE_FFFF,
        primary_epoch: 42,
        run_complete: true,
        run_aborted: true,
        discovery_debt: crate::DiscoveryDebt::Owed,
        phase_event_tallies_count: 9,
        phase_event_tallies_hash: 0x1357_2468,
        retry_passes_used_count: 5,
        retry_passes_used_hash: 0x2468_1357,
        unfulfillable_reinject_used_count: 2,
        unfulfillable_reinject_used_hash: 0xABCD_1234,
        respawn_events_count: 8,
        respawn_events_hash: 0xDEAD_C0DE,
        phases_ended_count: 2,
        phases_ended_hash: 0xFEED_FACE,
        custom_messages_count: 3,
        custom_messages_hash: 0xC0FF_EE00,
        custom_terminal_watermarks_count: 1,
        custom_terminal_watermarks_hash: 0xBEEF_0001,
    };
    let msg: DistributedMessage<TestId> = DistributedMessage::StateDigest {
        target: None,
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
        target: None,
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
        target: None,
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
        target: None,
        sender_id: "sec-1".into(),
        timestamp: 200.0,
        secondary_id: "sec-1".into(),
        worker_id: 2,
        task_hash: "hash123".into(),
        error_type: ErrorType::ResourceExhausted(ResourceKind::memory()),
        error_message: "out of memory".into(),
        delivery_seq: None,
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
        target: None,
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
                liveness_port: None,
            },
            PeerConnectionInfo {
                secondary_id: "sec-2".into(),
                cert: "PEM2".into(),
                ipv4: None,
                ipv6: Some("::1".into()),
                port: 5001,
                is_observer: false,
                liveness_port: None,
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
        target: None,
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
        target: None,
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

/// `RequestRunConfig` carries no payload beyond the routing/common
/// fields, so the round-trip pins only that the frame encodes and decodes
/// back to the same variant with its `sender_id` intact.
#[test]
fn roundtrip_request_run_config() {
    let msg: DistributedMessage<TestId> = DistributedMessage::RequestRunConfig {
        target: None,
        sender_id: "sec-late".into(),
        timestamp: 77.0,
    };

    let bytes = serialize_message(&msg).unwrap();
    let (decoded, consumed) = decode_frame::<TestId>(&bytes).unwrap().unwrap();
    assert_eq!(consumed, bytes.len());

    match decoded {
        DistributedMessage::RequestRunConfig { sender_id, .. } => {
            assert_eq!(sender_id, "sec-late");
        }
        _ => panic!("expected RequestRunConfig"),
    }
}

/// The full `RunConfig.forwarded_argv` survives the wire token-for-token.
/// An empty vec is the `#[serde(default)]`, so a populated argv is the
/// case that proves the field isn't silently dropped — every token and
/// its order must decode back equal (argv reconstruction is exact).
#[test]
fn roundtrip_run_config_forwarded_argv() {
    let forwarded_argv = vec![
        "--config".to_string(),
        "/app/run.toml".to_string(),
        "--seed".to_string(),
        "42".to_string(),
    ];
    let msg: DistributedMessage<TestId> = DistributedMessage::RunConfig {
        target: None,
        sender_id: "primary".into(),
        timestamp: 88.0,
        forwarded_argv: forwarded_argv.clone(),
    };

    let bytes = serialize_message(&msg).unwrap();
    let (decoded, consumed) = decode_frame::<TestId>(&bytes).unwrap().unwrap();
    assert_eq!(consumed, bytes.len());

    match decoded {
        DistributedMessage::RunConfig {
            sender_id,
            forwarded_argv: decoded_argv,
            ..
        } => {
            assert_eq!(sender_id, "primary");
            assert_eq!(decoded_argv, forwarded_argv);
        }
        _ => panic!("expected RunConfig"),
    }
}

/// Backcompat: a JSON payload of `RunConfig` from a pre-field sender (the
/// `forwarded_argv` field absent entirely) decodes with an empty argv.
/// `#[serde(default)]` is what keeps a rolling upgrade working — a peer
/// that has not learned about the field yet can still ship `RunConfig`
/// frames to a newer requester, and vice versa.
#[test]
fn legacy_run_config_without_forwarded_argv_decodes_empty() {
    let legacy = serde_json::json!({
        "msg_type": "run_config",
        "sender_id": "primary",
        "timestamp": 0.0
    });
    let json = serde_json::to_vec(&legacy).unwrap();
    let decoded: DistributedMessage<TestId> = serde_json::from_slice(&json).unwrap();
    match decoded {
        DistributedMessage::RunConfig { forwarded_argv, .. } => {
            assert!(forwarded_argv.is_empty());
        }
        _ => panic!("expected RunConfig"),
    }
}

/// `TerminalAck` (#352) round-trips through the length-prefixed codec
/// with its `seq` preserved verbatim — the app-level delivery
/// confirmation the primary's ingest echoes back for every
/// `delivery_seq`-stamped terminal landing.
#[test]
fn roundtrip_terminal_ack() {
    let msg: DistributedMessage<TestId> = DistributedMessage::TerminalAck {
        target: None,
        sender_id: "primary".into(),
        timestamp: 42.0,
        seq: 9001,
    };

    let bytes = serialize_message(&msg).unwrap();
    let (decoded, consumed) = decode_frame::<TestId>(&bytes).unwrap().unwrap();
    assert_eq!(consumed, bytes.len());

    match decoded {
        DistributedMessage::TerminalAck { sender_id, seq, .. } => {
            assert_eq!(sender_id, "primary");
            assert_eq!(seq, 9001);
        }
        _ => panic!("expected TerminalAck"),
    }
}

/// Wire-shape mirror (NOT symmetric-on-the-wrong-shape): decode the EXACT
/// JSON bytes a primary emits for a `TerminalAck` —
/// `{"msg_type":"terminal_ack",...,"seq":N}` (internally tagged,
/// snake_case) — rather than re-encoding our own value, so a tag/field
/// rename that still round-trips against itself is caught against the
/// other side's actual bytes.
#[test]
fn terminal_ack_decodes_literal_sender_bytes() {
    let bytes = r#"{"msg_type":"terminal_ack","sender_id":"primary","timestamp":7.5,"seq":42}"#;
    let decoded: DistributedMessage<TestId> = serde_json::from_str(bytes).unwrap();
    match decoded {
        DistributedMessage::TerminalAck { sender_id, seq, .. } => {
            assert_eq!(sender_id, "primary");
            assert_eq!(seq, 42);
        }
        _ => panic!("expected TerminalAck"),
    }
}

/// The reconciliation-probe pair (#308) round-trips through the
/// length-prefixed codec with `task_hash` (and the response's `held`
/// polarity) preserved verbatim.
#[test]
fn roundtrip_task_hold_query_and_response() {
    let query: DistributedMessage<TestId> = DistributedMessage::TaskHoldQuery {
        target: None,
        sender_id: "primary".into(),
        timestamp: 42.0,
        task_hash: "h-probe".into(),
    };
    let bytes = serialize_message(&query).unwrap();
    let (decoded, consumed) = decode_frame::<TestId>(&bytes).unwrap().unwrap();
    assert_eq!(consumed, bytes.len());
    match decoded {
        DistributedMessage::TaskHoldQuery {
            sender_id,
            task_hash,
            ..
        } => {
            assert_eq!(sender_id, "primary");
            assert_eq!(task_hash, "h-probe");
        }
        _ => panic!("expected TaskHoldQuery"),
    }

    // BOTH polarities of `held` round-trip — the `false` (positive
    // denial) is the load-bearing verdict and must never decode as the
    // benign `true`.
    for held in [true, false] {
        let response: DistributedMessage<TestId> = DistributedMessage::TaskHoldResponse {
            target: None,
            sender_id: "sec-1".into(),
            timestamp: 43.0,
            task_hash: "h-probe".into(),
            held,
        };
        let bytes = serialize_message(&response).unwrap();
        let (decoded, consumed) = decode_frame::<TestId>(&bytes).unwrap().unwrap();
        assert_eq!(consumed, bytes.len());
        match decoded {
            DistributedMessage::TaskHoldResponse {
                sender_id,
                task_hash,
                held: decoded_held,
                ..
            } => {
                assert_eq!(sender_id, "sec-1");
                assert_eq!(task_hash, "h-probe");
                assert_eq!(decoded_held, held);
            }
            _ => panic!("expected TaskHoldResponse"),
        }
    }
}

/// Wire-shape mirror (NOT symmetric-on-the-wrong-shape): decode the
/// EXACT JSON bytes a probing primary emits for a `TaskHoldQuery` —
/// `{"msg_type":"task_hold_query",...,"task_hash":"..."}` (internally
/// tagged, snake_case) — rather than re-encoding our own value, so a
/// tag/field rename that still round-trips against itself is caught
/// against the other side's actual bytes.
#[test]
fn task_hold_query_decodes_literal_sender_bytes() {
    let bytes = r#"{"msg_type":"task_hold_query","sender_id":"primary","timestamp":7.5,"task_hash":"h-lit"}"#;
    let decoded: DistributedMessage<TestId> = serde_json::from_str(bytes).unwrap();
    match decoded {
        DistributedMessage::TaskHoldQuery {
            sender_id,
            task_hash,
            ..
        } => {
            assert_eq!(sender_id, "primary");
            assert_eq!(task_hash, "h-lit");
        }
        _ => panic!("expected TaskHoldQuery"),
    }
}

/// Wire-shape mirror for the answer: the EXACT JSON bytes a holder
/// secondary emits for a `TaskHoldResponse` carrying the load-bearing
/// `held:false` denial.
#[test]
fn task_hold_response_decodes_literal_sender_bytes() {
    let bytes = r#"{"msg_type":"task_hold_response","sender_id":"sec-2","timestamp":1.0,"task_hash":"h-lit","held":false}"#;
    let decoded: DistributedMessage<TestId> = serde_json::from_str(bytes).unwrap();
    match decoded {
        DistributedMessage::TaskHoldResponse {
            sender_id,
            task_hash,
            held,
            ..
        } => {
            assert_eq!(sender_id, "sec-2");
            assert_eq!(task_hash, "h-lit");
            assert!(!held, "the positive denial must decode as held=false");
        }
        _ => panic!("expected TaskHoldResponse"),
    }
}

/// A `delivery_seq`-stamped terminal report round-trips with the seq
/// preserved — what lets a replay re-send the SAME seq and the primary
/// echo the matching ack.
#[test]
fn roundtrip_task_complete_with_delivery_seq() {
    let mut msg: DistributedMessage<TestId> = DistributedMessage::TaskComplete {
        target: None,
        sender_id: "sec-1".into(),
        timestamp: 5.0,
        secondary_id: "sec-1".into(),
        worker_id: 3,
        task_hash: "h-seq".into(),
        result_data: None,
        delivery_seq: None,
    };
    assert_eq!(msg.delivery_seq(), None);
    msg.set_delivery_seq(11);
    assert_eq!(msg.delivery_seq(), Some(11));
    assert_eq!(msg.delivery_reporter(), Some("sec-1"));

    let bytes = serialize_message(&msg).unwrap();
    let (decoded, _) = decode_frame::<TestId>(&bytes).unwrap().unwrap();
    assert_eq!(decoded.delivery_seq(), Some(11));
    assert_eq!(decoded.task_hash(), Some("h-seq"));
}

/// Wire-shape mirror for the seq-stamped terminal: decode the EXACT JSON
/// bytes a stamping secondary emits (`"delivery_seq":N` riding the
/// internally-tagged `task_failed` frame) against the other side's
/// actual bytes.
#[test]
fn task_failed_with_delivery_seq_decodes_literal_sender_bytes() {
    let bytes = r#"{"msg_type":"task_failed","sender_id":"sec-2","timestamp":1.0,"secondary_id":"sec-2","worker_id":0,"task_hash":"h-lit","error_type":"Recoverable","error_message":"worker pipe broken; respawning","delivery_seq":3}"#;
    let decoded: DistributedMessage<TestId> = serde_json::from_str(bytes).unwrap();
    match &decoded {
        DistributedMessage::TaskFailed {
            task_hash,
            error_message,
            delivery_seq,
            ..
        } => {
            assert_eq!(task_hash, "h-lit");
            assert_eq!(error_message, "worker pipe broken; respawning");
            assert_eq!(*delivery_seq, Some(3));
        }
        _ => panic!("expected TaskFailed"),
    }
    assert_eq!(decoded.delivery_reporter(), Some("sec-2"));
}

/// Backcompat both ways for the additive `delivery_seq` field:
///   * a pre-field sender's bytes (field absent) decode as `None`, and
///   * a `None` frame serializes WITHOUT the field — byte-identical to
///     the pre-#352 wire (`skip_serializing_if`), so a rolling upgrade
///     never trips an old decoder on an unknown field.
#[test]
fn delivery_seq_is_wire_additive() {
    let legacy = r#"{"msg_type":"task_complete","sender_id":"sec-1","timestamp":0.0,"secondary_id":"sec-1","worker_id":0,"task_hash":"h-old"}"#;
    let decoded: DistributedMessage<TestId> = serde_json::from_str(legacy).unwrap();
    assert_eq!(decoded.delivery_seq(), None);

    let unstamped: DistributedMessage<TestId> = DistributedMessage::TaskComplete {
        target: None,
        sender_id: "sec-1".into(),
        timestamp: 0.0,
        secondary_id: "sec-1".into(),
        worker_id: 0,
        task_hash: "h-old".into(),
        result_data: None,
        delivery_seq: None,
    };
    let json = serde_json::to_string(&unstamped).unwrap();
    assert!(
        !json.contains("delivery_seq"),
        "a None delivery_seq must be elided from the wire bytes; got {json}"
    );
}

/// F5 `CustomMessage` round-trip: every field — the `(origin, msg_seq)`
/// idempotency key, the opaque `(topic, data)` payload, the delivery
/// class, and the #352 `delivery_seq` stamp — survives the wire.
#[test]
fn roundtrip_custom_message_important() {
    let msg: DistributedMessage<TestId> = DistributedMessage::CustomMessage {
        target: None,
        sender_id: "relay-2".into(),
        timestamp: 12.5,
        origin_secondary_id: "sec-1".into(),
        msg_seq: 7,
        topic: "phase4-batch".into(),
        data: b"descriptor batch".to_vec(),
        important: true,
        delivery_seq: Some(42),
    };
    let bytes = serialize_message(&msg).unwrap();
    let (decoded, consumed) = decode_frame::<TestId>(&bytes).unwrap().unwrap();
    assert_eq!(consumed, bytes.len());
    assert_eq!(decoded.msg_type(), MessageType::CustomMessage);
    // The confirmable classifiers see the important custom message.
    assert!(decoded.requires_delivery_ack());
    assert_eq!(decoded.delivery_seq(), Some(42));
    // The ack must address the ORIGINATOR, not the wire sender (relay).
    assert_eq!(decoded.delivery_reporter(), Some("sec-1"));
    assert_eq!(decoded.sender_id(), "relay-2");
    match decoded {
        DistributedMessage::CustomMessage {
            origin_secondary_id,
            msg_seq,
            topic,
            data,
            important,
            ..
        } => {
            assert_eq!(origin_secondary_id, "sec-1");
            assert_eq!(msg_seq, 7);
            assert_eq!(topic, "phase4-batch");
            assert_eq!(data, b"descriptor batch".to_vec());
            assert!(important);
        }
        _ => panic!("expected CustomMessage"),
    }
}

/// A DROPPABLE custom message is NOT confirmable: never
/// `delivery_seq`-stamped (the field is elided from the wire bytes),
/// never retained, never acked.
#[test]
fn droppable_custom_message_is_not_confirmable_and_elides_delivery_seq() {
    let mut msg: DistributedMessage<TestId> = DistributedMessage::CustomMessage {
        target: None,
        sender_id: "sec-1".into(),
        timestamp: 0.0,
        origin_secondary_id: "sec-1".into(),
        msg_seq: 1,
        topic: "progress".into(),
        data: vec![0xFF],
        important: false,
        delivery_seq: None,
    };
    assert!(!msg.requires_delivery_ack());
    let json = serde_json::to_string(&msg).unwrap();
    assert!(
        !json.contains("delivery_seq"),
        "a None delivery_seq must be elided from the wire bytes; got {json}"
    );
    // The stamping accessor itself is variant-shaped (the gate is the
    // chokepoint's `requires_delivery_ack` check, upstream).
    msg.set_delivery_seq(9);
    assert_eq!(msg.delivery_seq(), Some(9));
}

/// Literal-bytes pin for the F5 `CustomMessage` wire shape: decode the
/// exact JSON a current sender emits (internally tagged
/// `msg_type: "custom_message"`, snake_case fields, `data` as a JSON
/// byte array). Pinning the sender bytes catches a silent tag /
/// field-name divergence that a symmetric encode→decode round-trip
/// cannot see (the wire-shape mirror discipline).
#[test]
fn custom_message_decodes_literal_sender_bytes() {
    let literal = r#"{"msg_type":"custom_message","sender_id":"sec-1","timestamp":1.0,"origin_secondary_id":"sec-1","msg_seq":2,"topic":"phase4-batch","data":[104,105],"important":true,"delivery_seq":5}"#;
    let decoded: DistributedMessage<TestId> = serde_json::from_str(literal).unwrap();
    match decoded {
        DistributedMessage::CustomMessage {
            origin_secondary_id,
            msg_seq,
            topic,
            data,
            important,
            delivery_seq,
            ..
        } => {
            assert_eq!(origin_secondary_id, "sec-1");
            assert_eq!(msg_seq, 2);
            assert_eq!(topic, "phase4-batch");
            assert_eq!(data, b"hi".to_vec());
            assert!(important);
            assert_eq!(delivery_seq, Some(5));
        }
        _ => panic!("expected CustomMessage"),
    }
}

/// The wire-only `RedialRequest` (member-leg redial handshake) survives
/// the codec, INCLUDING its non-default `attempts` count — `attempts`
/// carries `#[serde(default)]`, so a roundtrip with 0 would still pass
/// if the field were dropped on the wire (a default masks a dropped
/// layer perfectly); the non-zero value pins the field's presence. The
/// mirror direction (a pre-`attempts` sender) is pinned by
/// `legacy_redial_request_without_attempts_decodes_zero` below.
#[test]
fn roundtrip_redial_request() {
    let msg: DistributedMessage<TestId> = DistributedMessage::RedialRequest {
        target: None,
        sender_id: "sec-7".into(),
        timestamp: 99.25,
        attempts: 3,
    };

    let bytes = serialize_message(&msg).unwrap();
    let (decoded, consumed) = decode_frame::<TestId>(&bytes).unwrap().unwrap();
    assert_eq!(consumed, bytes.len());

    match decoded {
        DistributedMessage::RedialRequest {
            sender_id,
            timestamp,
            attempts,
            ..
        } => {
            assert_eq!(sender_id, "sec-7");
            assert_eq!(timestamp, 99.25);
            assert_eq!(attempts, 3);
        }
        _ => panic!("expected RedialRequest"),
    }
}

/// Mirror-the-other-side's-bytes: a `RedialRequest` emitted WITHOUT the
/// `attempts` field (a sender predating it) decodes with `attempts == 0`
/// — inside the dial owner's grace window, the conservative
/// don't-prune-a-live-wire value.
#[test]
fn legacy_redial_request_without_attempts_decodes_zero() {
    let wire = br#"{"msg_type":"redial_request","sender_id":"sec-9","timestamp":7.5}"#;
    let decoded: DistributedMessage<TestId> = deserialize_message(wire).unwrap();
    match decoded {
        DistributedMessage::RedialRequest {
            sender_id,
            attempts,
            ..
        } => {
            assert_eq!(sender_id, "sec-9");
            assert_eq!(
                attempts, 0,
                "absent attempts must decode as 0 (grace window)"
            );
        }
        _ => panic!("expected RedialRequest"),
    }
}
