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
        graceful_abort: true,
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
        wind_down_requested_count: 1,
        wind_down_requested_hash: 0x0467_0467,
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
        // The sender's declared role must survive the wire so a pull
        // directed at it can be typed off its role (the addressing fix).
        sender_is_observer: true,
    };

    let bytes = serialize_message(&msg).unwrap();
    let (decoded, consumed) = decode_frame::<TestId>(&bytes).unwrap().unwrap();
    assert_eq!(consumed, bytes.len());

    match decoded {
        DistributedMessage::StateDigest {
            sender_id,
            digest: decoded_digest,
            sender_is_observer,
            ..
        } => {
            assert_eq!(sender_id, "sec-7");
            assert_eq!(decoded_digest, digest);
            assert!(
                sender_is_observer,
                "the sender's declared-observer bit must round-trip on the wire"
            );
        }
        _ => panic!("expected StateDigest"),
    }
}

/// Wire-shape mirror + backcompat: a `StateDigest` from a pre-field sender
/// (the `sender_is_observer` key absent entirely, the digest minimal)
/// decodes with `sender_is_observer == false` — the conservative
/// compute-role shape. `#[serde(default)]` keeps a rolling upgrade working:
/// a legacy sender's pull-target typing falls back to `Secondary`, and the
/// receiver-side id==self fan covers the residual mis-type without noise.
/// Decoding the OTHER side's literal bytes (not a re-encode of our own
/// value) catches a tag/field rename that still round-trips against itself.
#[test]
fn legacy_state_digest_without_sender_is_observer_decodes_false() {
    let bytes = r#"{"msg_type":"state_digest","sender_id":"sec-1","timestamp":0.0,"digest":{}}"#;
    let decoded: DistributedMessage<TestId> = serde_json::from_str(bytes).unwrap();
    match decoded {
        DistributedMessage::StateDigest {
            sender_id,
            sender_is_observer,
            ..
        } => {
            assert_eq!(sender_id, "sec-1");
            assert!(
                !sender_is_observer,
                "a pre-field sender must decode as non-observer (the conservative shape)"
            );
        }
        _ => panic!("expected StateDigest"),
    }
}

/// Mirror the OTHER direction: a sender that DOES stamp the field emits
/// `"sender_is_observer":true` and a peer decodes it verbatim — the
/// observer-typed pull path. Pins that the encoder writes the field (it is
/// not `skip_serializing`) so an observer sender's role actually reaches
/// the wire for the puller to mirror.
#[test]
fn state_digest_observer_sender_bit_is_on_the_wire() {
    let msg: DistributedMessage<TestId> = DistributedMessage::StateDigest {
        target: None,
        sender_id: "obs-1".into(),
        timestamp: 0.0,
        digest: StateDigest::default(),
        sender_is_observer: true,
    };
    let json = serde_json::to_string(&msg).unwrap();
    assert!(
        json.contains("\"sender_is_observer\":true"),
        "the observer sender's role bit must be emitted on the wire, got: {json}"
    );
    let decoded: DistributedMessage<TestId> = serde_json::from_str(&json).unwrap();
    match decoded {
        DistributedMessage::StateDigest {
            sender_is_observer, ..
        } => assert!(sender_is_observer),
        _ => panic!("expected StateDigest"),
    }
}

/// Pull-model PROBE round-trips: the requester id + the carried digest
/// survive the wire. The digest is the responder-side `ahead`-filter input,
/// so its fields must arrive intact.
#[test]
fn roundtrip_pull_probe() {
    let msg: DistributedMessage<TestId> = DistributedMessage::PullProbe {
        target: None,
        sender_id: "behind-node".into(),
        timestamp: 99.5,
        digest: StateDigest {
            tasks_count: 7,
            tasks_hash: 0xABCD,
            primary_epoch: 3,
            ..Default::default()
        },
    };
    let bytes = serialize_message(&msg).unwrap();
    let (decoded, consumed) = decode_frame::<TestId>(&bytes).unwrap().unwrap();
    assert_eq!(consumed, bytes.len());
    match decoded {
        DistributedMessage::PullProbe {
            sender_id, digest, ..
        } => {
            assert_eq!(sender_id, "behind-node");
            assert_eq!(digest.tasks_count, 7);
            assert_eq!(digest.tasks_hash, 0xABCD);
            assert_eq!(digest.primary_epoch, 3);
        }
        _ => panic!("expected PullProbe"),
    }
}

/// Pull-model PROBE REPLY round-trips: the requester addressee, the inbox
/// depth, the `ahead` bit, AND the piggybacked P1 `range_digest` all survive
/// on their NON-default values (`ahead = true` is the value that actually
/// selects a target — a default `false` would mask a dropped field; the
/// range digest carries a sentinel bucket so a dropped/zeroed array is
/// caught, the wire-shape mirror discipline for the new field).
#[test]
fn roundtrip_pull_probe_reply() {
    let mut range_digest = crate::RangeDigest::default();
    range_digest.counts[7] = 5;
    range_digest.folds[7] = 0xDEAD_BEEF;
    range_digest.counts[200] = 1;
    range_digest.folds[200] = 0x1234_5678_9ABC_DEF0;
    let msg: DistributedMessage<TestId> = DistributedMessage::PullProbeReply {
        target: None,
        sender_id: "donor".into(),
        timestamp: 1.0,
        requester: "behind-node".into(),
        inbox_size: 42,
        ahead: true,
        range_digest: Box::new(range_digest.clone()),
    };
    let bytes = serialize_message(&msg).unwrap();
    let (decoded, consumed) = decode_frame::<TestId>(&bytes).unwrap().unwrap();
    assert_eq!(consumed, bytes.len());
    match decoded {
        DistributedMessage::PullProbeReply {
            sender_id,
            requester,
            inbox_size,
            ahead,
            range_digest: rd,
            ..
        } => {
            assert_eq!(sender_id, "donor");
            assert_eq!(requester, "behind-node");
            assert_eq!(inbox_size, 42);
            assert!(ahead, "the ahead bit must survive on its non-default value");
            assert_eq!(
                rd.counts, range_digest.counts,
                "the piggybacked range-digest counts must survive the wire"
            );
            assert_eq!(
                rd.folds, range_digest.folds,
                "the piggybacked range-digest folds must survive the wire"
            );
        }
        _ => panic!("expected PullProbeReply"),
    }
}

/// Wire-shape mirror + backcompat: a `PullProbeReply` from a pre-`ahead`
/// sender (the `ahead` key absent entirely) decodes as `ahead == false` —
/// the conservative "cannot help" shape, so a legacy responder is never
/// selected as a pull target. Decodes the OTHER side's literal bytes (not a
/// re-encode of our own value) so a tag/field rename that still round-trips
/// against itself is caught.
#[test]
fn legacy_pull_probe_reply_without_ahead_decodes_false() {
    let bytes = r#"{"msg_type":"pull_probe_reply","sender_id":"donor","timestamp":0.0,"requester":"behind-node","inbox_size":3}"#;
    let decoded: DistributedMessage<TestId> = serde_json::from_str(bytes).unwrap();
    match decoded {
        DistributedMessage::PullProbeReply {
            requester,
            inbox_size,
            ahead,
            range_digest,
            ..
        } => {
            assert_eq!(requester, "behind-node");
            assert_eq!(inbox_size, 3);
            assert!(
                !ahead,
                "a pre-field reply must decode as NOT-ahead (never a pull candidate)"
            );
            // A pre-`range_digest` reply decodes as the all-zero digest; the
            // requester then computes no narrowing and falls back to the
            // all-ranges full stream (the data-loss fail-safe — a legacy
            // responder degrades to a P0 full pull, never a dropped range).
            assert_eq!(range_digest.counts, [0u32; crate::RANGE_COUNT]);
            assert_eq!(range_digest.folds, [0u64; crate::RANGE_COUNT]);
        }
        _ => panic!("expected PullProbeReply"),
    }
}

/// Pull-model FAIL round-trips: the requester addressee + the failed
/// stream id survive, so the requester correlates the fail to its in-flight
/// pull and falls to the next target.
#[test]
fn roundtrip_pull_fail() {
    let msg: DistributedMessage<TestId> = DistributedMessage::PullFail {
        target: None,
        sender_id: "dead-leg-target".into(),
        timestamp: 5.0,
        requester: "behind-node".into(),
        stream_id: "behind-node/4".into(),
    };
    let bytes = serialize_message(&msg).unwrap();
    let (decoded, consumed) = decode_frame::<TestId>(&bytes).unwrap().unwrap();
    assert_eq!(consumed, bytes.len());
    match decoded {
        DistributedMessage::PullFail {
            sender_id,
            requester,
            stream_id,
            ..
        } => {
            assert_eq!(sender_id, "dead-leg-target");
            assert_eq!(requester, "behind-node");
            assert_eq!(stream_id, "behind-node/4");
        }
        _ => panic!("expected PullFail"),
    }
}

/// The three pull frames carry the EXPECTED `msg_type` discriminators on
/// the wire (the receiver demuxes on these), and a peer mirrors the literal
/// bytes back into the right variant — catches a `rename_all` drift that
/// would still round-trip against itself.
#[test]
fn pull_frames_wire_tags_mirror() {
    for (literal, want_tag) in [
        (
            r#"{"msg_type":"pull_probe","sender_id":"a","timestamp":0.0,"digest":{}}"#,
            MessageType::PullProbe,
        ),
        (
            r#"{"msg_type":"pull_probe_reply","sender_id":"b","timestamp":0.0,"requester":"a","inbox_size":1,"ahead":true}"#,
            MessageType::PullProbeReply,
        ),
        (
            r#"{"msg_type":"pull_fail","sender_id":"c","timestamp":0.0,"requester":"a","stream_id":"a/0"}"#,
            MessageType::PullFail,
        ),
    ] {
        let decoded: DistributedMessage<TestId> = serde_json::from_str(literal).unwrap();
        assert_eq!(decoded.msg_type(), want_tag, "literal: {literal}");
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
        supplanted_holder: None,
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
        // Stamped at the send_to_primary chokepoint (ordering gate).
        msgs_posted_through: None,
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
        supplanted_holder: None,
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

/// Pre-start fence A wire field (#530a): a `TaskAssignment` carrying a
/// `Some(supplanted_holder)` hint round-trips verbatim through the
/// codec — the tuple shape `(peer-id, gen)` is preserved, no field is
/// silently dropped. This pins the load-bearing fence-A wire contract:
/// a primary that stamps the hint reaches the addressee secondary with
/// the exact (peer, gen) the receiver's `cluster_state` compares
/// against.
#[test]
fn roundtrip_task_assignment_with_supplanted_holder_fence_a() {
    let msg: DistributedMessage<TestId> = DistributedMessage::TaskAssignment {
        target: None,
        sender_id: "primary".into(),
        timestamp: 0.0,
        secondary_id: "sec-b".into(),
        worker_id: 7,
        zip_file: None,
        binary_info: DistributedBinaryInfo {
            path: "/tmp/x".into(),
            size: 1,
            identifier: test_id("fenced"),
            phase_id: "default".into(),
            type_id: "default".into(),
            affinity_id: None,
            payload_json: "null".into(),
            task_id: "fenced-task".into(),
            task_depends_on: vec![],
            preferred_secondaries: Default::default(),
        },
        local_path: "x".into(),
        file_hash: "h".into(),
        predecessor_outputs: std::collections::BTreeMap::new(),
        supplanted_holder: Some(("sec-a".into(), 1)),
    };

    let bytes = serialize_message(&msg).unwrap();
    let (decoded, _) = decode_frame::<TestId>(&bytes).unwrap().unwrap();
    match decoded {
        DistributedMessage::TaskAssignment {
            supplanted_holder, ..
        } => {
            assert_eq!(supplanted_holder, Some(("sec-a".to_string(), 1)));
        }
        _ => panic!("expected TaskAssignment"),
    }
}

/// Backcompat (#530a): a pre-fence sender emits a `TaskAssignment` JSON
/// payload without the `supplanted_holder` field. `#[serde(default)]`
/// decodes it as `None` — the safe degraded shape (the secondary's
/// fence-A arm falls through, exactly as documented on the variant).
/// Without this, a rolling upgrade would refuse legacy frames and
/// stall mixed-version clusters mid-deploy.
#[test]
fn legacy_task_assignment_without_supplanted_holder_decodes_as_none() {
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
            supplanted_holder, ..
        } => {
            assert_eq!(supplanted_holder, None);
        }
        _ => panic!("expected TaskAssignment"),
    }
}

/// Wire-bytes elision (#530a): when `supplanted_holder` is `None`, the
/// JSON output must NOT contain the field name — matches the
/// `predecessor_outputs` / `preferred_secondaries` "elide when default"
/// idiom so the byte representation of the common (no-fence) case is
/// byte-identical to the pre-#530 sender's frame, and a rolling
/// upgrade introduces zero wire bloat on the steady-state path.
#[test]
fn no_supplanted_holder_elided_on_wire() {
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
            identifier: test_id("no_fence"),
            phase_id: "default".into(),
            type_id: "default".into(),
            affinity_id: None,
            payload_json: "null".into(),
            task_id: "no-fence-task".into(),
            task_depends_on: vec![],
            preferred_secondaries: Default::default(),
        },
        local_path: "x".into(),
        file_hash: "h".into(),
        predecessor_outputs: std::collections::BTreeMap::new(),
        supplanted_holder: None,
    };
    let json = serde_json::to_string(&msg).unwrap();
    assert!(
        !json.contains("supplanted_holder"),
        "None supplanted_holder must elide via skip_serializing_if, got: {json}"
    );
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
        supplanted_holder: None,
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

/// `GracefulAbortRequest` (the observer's ONE management command) carries
/// no payload beyond the routing/common fields; the round-trip pins the
/// length-prefixed frame codec preserving the variant + `sender_id`.
#[test]
fn roundtrip_graceful_abort_request() {
    let msg: DistributedMessage<TestId> = DistributedMessage::GracefulAbortRequest {
        target: None,
        sender_id: "obs-1".into(),
        timestamp: 42.0,
    };

    let bytes = serialize_message(&msg).unwrap();
    let (decoded, consumed) = decode_frame::<TestId>(&bytes).unwrap().unwrap();
    assert_eq!(consumed, bytes.len());

    match decoded {
        DistributedMessage::GracefulAbortRequest { sender_id, .. } => {
            assert_eq!(sender_id, "obs-1");
        }
        _ => panic!("expected GracefulAbortRequest"),
    }
}

/// Wire-shape mirror (NOT symmetric-on-the-wrong-shape): decode the EXACT
/// JSON bytes an observer emits — the internally-tagged
/// `{"msg_type":"graceful_abort_request",...}` envelope with the
/// `target: None` routing header elided via `skip_serializing_if` —
/// rather than re-encoding our own value, so a tag/field divergence that
/// still round-trips against itself is caught against the sender's
/// actual bytes.
#[test]
fn graceful_abort_request_decodes_literal_sender_bytes() {
    let literal =
        r#"{"msg_type":"graceful_abort_request","sender_id":"obs-relocated","timestamp":3.5}"#;
    let decoded: DistributedMessage<TestId> = serde_json::from_str(literal).unwrap();

    match decoded {
        DistributedMessage::GracefulAbortRequest {
            target,
            sender_id,
            timestamp,
        } => {
            assert!(target.is_none(), "elided target must decode as None");
            assert_eq!(sender_id, "obs-relocated");
            assert_eq!(timestamp, 3.5);
        }
        _ => panic!("expected GracefulAbortRequest"),
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
        // Stamped at the send_to_primary chokepoint (ordering gate).
        msgs_posted_through: None,
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
        // Stamped at the send_to_primary chokepoint (ordering gate).
        msgs_posted_through: None,
    };
    let json = serde_json::to_string(&unstamped).unwrap();
    assert!(
        !json.contains("delivery_seq"),
        "a None delivery_seq must be elided from the wire bytes; got {json}"
    );
}

/// A `msgs_posted_through`-stamped terminal (the message-vs-phase-end
/// ordering gate's causal watermark) round-trips through the
/// length-prefixed codec with the stamp preserved — what lets a replay
/// re-land the SAME gate threshold at a promoted primary.
#[test]
fn roundtrip_task_complete_with_msgs_posted_through() {
    let mut msg: DistributedMessage<TestId> = DistributedMessage::TaskComplete {
        target: None,
        sender_id: "sec-1".into(),
        timestamp: 5.0,
        secondary_id: "sec-1".into(),
        worker_id: 3,
        task_hash: "h-gate".into(),
        result_data: None,
        delivery_seq: Some(11),
        msgs_posted_through: None,
    };
    assert_eq!(msg.msgs_posted_through(), None);
    msg.set_msgs_posted_through(4);
    assert_eq!(msg.msgs_posted_through(), Some(4));

    let bytes = serialize_message(&msg).unwrap();
    let (decoded, consumed) = decode_frame::<TestId>(&bytes).unwrap().unwrap();
    assert_eq!(consumed, bytes.len());
    assert_eq!(decoded.msgs_posted_through(), Some(4));
    assert_eq!(decoded.task_hash(), Some("h-gate"));
}

/// Wire-shape mirror (NOT symmetric-on-the-wrong-shape): decode the
/// EXACT JSON bytes a stamping secondary emits for a gated terminal —
/// `"msgs_posted_through":N` riding the internally-tagged
/// `task_failed` frame — against the other side's actual bytes, so a
/// tag/field rename that still round-trips against itself is caught.
#[test]
fn task_failed_with_msgs_posted_through_decodes_literal_sender_bytes() {
    let bytes = r#"{"msg_type":"task_failed","sender_id":"sec-2","timestamp":1.0,"secondary_id":"sec-2","worker_id":0,"task_hash":"h-lit","error_type":"Recoverable","error_message":"boom","delivery_seq":3,"msgs_posted_through":4}"#;
    let decoded: DistributedMessage<TestId> = serde_json::from_str(bytes).unwrap();
    match &decoded {
        DistributedMessage::TaskFailed {
            task_hash,
            msgs_posted_through,
            ..
        } => {
            assert_eq!(task_hash, "h-lit");
            assert_eq!(*msgs_posted_through, Some(4));
        }
        _ => panic!("expected TaskFailed"),
    }
    assert_eq!(decoded.msgs_posted_through(), Some(4));
}

/// Backcompat both ways for the additive `msgs_posted_through` field
/// (the `delivery_seq` precedent):
///   * a pre-field sender's bytes decode as `None` — no causal claim,
///     the gate is open (the pre-fix behaviour for legacy senders), and
///   * a `None` frame serializes WITHOUT the field — byte-identical to
///     the pre-gate wire, so a rolling upgrade never trips an old
///     decoder on an unknown field.
#[test]
fn msgs_posted_through_is_wire_additive() {
    let legacy = r#"{"msg_type":"task_complete","sender_id":"sec-1","timestamp":0.0,"secondary_id":"sec-1","worker_id":0,"task_hash":"h-old","delivery_seq":9}"#;
    let decoded: DistributedMessage<TestId> = serde_json::from_str(legacy).unwrap();
    assert_eq!(decoded.msgs_posted_through(), None);
    assert_eq!(decoded.delivery_seq(), Some(9));

    let unstamped: DistributedMessage<TestId> = DistributedMessage::TaskFailed {
        target: None,
        sender_id: "sec-1".into(),
        timestamp: 0.0,
        secondary_id: "sec-1".into(),
        worker_id: 0,
        task_hash: "h-old".into(),
        error_type: dynrunner_core::ErrorType::Recoverable,
        error_message: "boom".into(),
        delivery_seq: None,
        msgs_posted_through: None,
    };
    let json = serde_json::to_string(&unstamped).unwrap();
    assert!(
        !json.contains("msgs_posted_through"),
        "a None msgs_posted_through must be elided from the wire bytes; got {json}"
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

/// Wire-shape mirror (NOT symmetric-on-the-wrong-shape): decode the
/// EXACT JSON bytes a relocated/promoted primary emits for a
/// `RespawnSpawnRequest` — internally tagged, snake_case, with the
/// primary-minted replacement id that doubles as the correlation +
/// idempotency key — rather than re-encoding our own value, so a
/// tag/field rename that still round-trips against itself is caught
/// against the other side's actual bytes.
#[test]
fn respawn_spawn_request_decodes_literal_sender_bytes() {
    let wire = br#"{"msg_type":"respawn_spawn_request","sender_id":"secondary-2","timestamp":7.5,"new_secondary_id":"secondary-5","primary_endpoint":"10.0.0.7:5555","primary_pubkey_pem":"-----BEGIN PUBLIC KEY-----\nA\n-----END PUBLIC KEY-----\n"}"#;
    let decoded: DistributedMessage<TestId> = deserialize_message(wire).unwrap();
    match decoded {
        DistributedMessage::RespawnSpawnRequest {
            sender_id,
            new_secondary_id,
            primary_endpoint,
            primary_pubkey_pem,
            ..
        } => {
            assert_eq!(sender_id, "secondary-2");
            assert_eq!(new_secondary_id, "secondary-5");
            assert_eq!(primary_endpoint, "10.0.0.7:5555");
            assert!(primary_pubkey_pem.starts_with("-----BEGIN"));
        }
        _ => panic!("expected RespawnSpawnRequest"),
    }
}

/// The sender-side bytes of a `RespawnSpawnRequest` carry the exact
/// snake_case tag + fields the observer-side decode above mirrors —
/// pinning the two directions against EACH OTHER (a serializer-side
/// rename now fails this test, a decoder-side rename fails the mirror).
#[test]
fn respawn_spawn_request_serializes_expected_wire_bytes() {
    let msg: DistributedMessage<TestId> = DistributedMessage::RespawnSpawnRequest {
        target: None,
        sender_id: "secondary-2".into(),
        timestamp: 7.5,
        new_secondary_id: "secondary-5".into(),
        primary_endpoint: "10.0.0.7:5555".into(),
        primary_pubkey_pem: "PEM".into(),
        // None elides the field entirely (`skip_serializing_if`), so the
        // wire bytes are unchanged from before the field existed — an
        // older receiver stays byte-compatible.
        dead_member_id: None,
    };
    let json = serde_json::to_string(&msg).unwrap();
    assert_eq!(
        json,
        r#"{"msg_type":"respawn_spawn_request","sender_id":"secondary-2","timestamp":7.5,"new_secondary_id":"secondary-5","primary_endpoint":"10.0.0.7:5555","primary_pubkey_pem":"PEM"}"#,
    );
}

/// `dead_member_id = Some(id)` puts the field on the wire, and the
/// mirror decode recovers it. Pins BOTH directions for the populated
/// case (the elided-None case is covered by the two tests above): a
/// serializer-side rename fails the byte assertion, a decoder-side
/// rename fails the round-trip read.
#[test]
fn respawn_spawn_request_round_trips_dead_member_id() {
    let msg: DistributedMessage<TestId> = DistributedMessage::RespawnSpawnRequest {
        target: None,
        sender_id: "secondary-2".into(),
        timestamp: 7.5,
        new_secondary_id: "secondary-5".into(),
        primary_endpoint: "10.0.0.7:5555".into(),
        primary_pubkey_pem: "PEM".into(),
        dead_member_id: Some("secondary-0".into()),
    };
    let json = serde_json::to_string(&msg).unwrap();
    assert_eq!(
        json,
        r#"{"msg_type":"respawn_spawn_request","sender_id":"secondary-2","timestamp":7.5,"new_secondary_id":"secondary-5","primary_endpoint":"10.0.0.7:5555","primary_pubkey_pem":"PEM","dead_member_id":"secondary-0"}"#,
    );
    // Mirror: the EXACT bytes a primary emits decode back to Some(id).
    let wire = br#"{"msg_type":"respawn_spawn_request","sender_id":"secondary-2","timestamp":7.5,"new_secondary_id":"secondary-5","primary_endpoint":"10.0.0.7:5555","primary_pubkey_pem":"PEM","dead_member_id":"secondary-0"}"#;
    let decoded: DistributedMessage<TestId> = deserialize_message(wire).unwrap();
    match decoded {
        DistributedMessage::RespawnSpawnRequest { dead_member_id, .. } => {
            assert_eq!(dead_member_id.as_deref(), Some("secondary-0"));
        }
        _ => panic!("expected RespawnSpawnRequest"),
    }
}

/// Wire-shape mirror for the SUCCESS result: the EXACT bytes the
/// provider-host observer emits — `error` elided entirely on success
/// (`skip_serializing_if`), so the decoder must default it to `None`.
#[test]
fn respawn_spawn_result_success_decodes_literal_sender_bytes() {
    let wire = br#"{"msg_type":"respawn_spawn_result","sender_id":"setup","timestamp":9.0,"new_secondary_id":"secondary-5"}"#;
    let decoded: DistributedMessage<TestId> = deserialize_message(wire).unwrap();
    match decoded {
        DistributedMessage::RespawnSpawnResult {
            sender_id,
            new_secondary_id,
            error,
            ..
        } => {
            assert_eq!(sender_id, "setup");
            assert_eq!(new_secondary_id, "secondary-5");
            assert_eq!(error, None, "elided error field must decode as success");
        }
        _ => panic!("expected RespawnSpawnResult"),
    }
}

/// Wire-shape mirror for the FAILURE result: the provider error string
/// rides back verbatim and feeds the primary's budget/logging exactly
/// as a local provider `Err` does.
#[test]
fn respawn_spawn_result_error_decodes_literal_sender_bytes() {
    let wire = br#"{"msg_type":"respawn_spawn_result","sender_id":"setup","timestamp":9.0,"new_secondary_id":"secondary-5","error":"sbatch: gateway unreachable"}"#;
    let decoded: DistributedMessage<TestId> = deserialize_message(wire).unwrap();
    match decoded {
        DistributedMessage::RespawnSpawnResult {
            new_secondary_id,
            error,
            ..
        } => {
            assert_eq!(new_secondary_id, "secondary-5");
            assert_eq!(error.as_deref(), Some("sbatch: gateway unreachable"));
        }
        _ => panic!("expected RespawnSpawnResult"),
    }
}

/// The success result serializes WITHOUT the `error` key (the byte
/// shape the success mirror above decodes) — pinning the elision.
#[test]
fn respawn_spawn_result_serializes_expected_wire_bytes() {
    let msg: DistributedMessage<TestId> = DistributedMessage::RespawnSpawnResult {
        target: None,
        sender_id: "setup".into(),
        timestamp: 9.0,
        new_secondary_id: "secondary-5".into(),
        error: None,
    };
    let json = serde_json::to_string(&msg).unwrap();
    assert_eq!(
        json,
        r#"{"msg_type":"respawn_spawn_result","sender_id":"setup","timestamp":9.0,"new_secondary_id":"secondary-5"}"#,
    );
}

/// Wire-shape mirror: the EXACT bytes a primary emits to revoke a
/// still-pending replacement (its original re-admitted).
#[test]
fn respawn_revoke_request_decodes_literal_sender_bytes() {
    let wire = br#"{"msg_type":"respawn_revoke_request","sender_id":"secondary-2","timestamp":3.25,"new_secondary_id":"secondary-5"}"#;
    let decoded: DistributedMessage<TestId> = deserialize_message(wire).unwrap();
    match decoded {
        DistributedMessage::RespawnRevokeRequest {
            sender_id,
            new_secondary_id,
            ..
        } => {
            assert_eq!(sender_id, "secondary-2");
            assert_eq!(new_secondary_id, "secondary-5");
        }
        _ => panic!("expected RespawnRevokeRequest"),
    }
}

/// Wire-shape mirror for the revoke outcome, error polarity included
/// (an `Err` means the provider could not reach its backend; the
/// primary logs loudly and the teardown sweep reclaims).
#[test]
fn respawn_revoke_result_decodes_literal_sender_bytes() {
    let ok_wire = br#"{"msg_type":"respawn_revoke_result","sender_id":"setup","timestamp":4.0,"new_secondary_id":"secondary-5"}"#;
    let decoded: DistributedMessage<TestId> = deserialize_message(ok_wire).unwrap();
    match decoded {
        DistributedMessage::RespawnRevokeResult {
            new_secondary_id,
            error,
            ..
        } => {
            assert_eq!(new_secondary_id, "secondary-5");
            assert_eq!(error, None);
        }
        _ => panic!("expected RespawnRevokeResult"),
    }
    let err_wire = br#"{"msg_type":"respawn_revoke_result","sender_id":"setup","timestamp":4.0,"new_secondary_id":"secondary-5","error":"scancel: ssh transport failure"}"#;
    let decoded: DistributedMessage<TestId> = deserialize_message(err_wire).unwrap();
    match decoded {
        DistributedMessage::RespawnRevokeResult { error, .. } => {
            assert_eq!(error.as_deref(), Some("scancel: ssh transport failure"));
        }
        _ => panic!("expected RespawnRevokeResult"),
    }
}

/// #518 `RequestInFlightRoster` carries no payload beyond the
/// routing/common fields; the round-trip pins the length-prefixed codec
/// preserving the variant + `sender_id`.
#[test]
fn roundtrip_request_inflight_roster() {
    let msg: DistributedMessage<TestId> = DistributedMessage::RequestInFlightRoster {
        target: None,
        sender_id: "primary".into(),
        timestamp: 55.0,
    };
    let bytes = serialize_message(&msg).unwrap();
    let (decoded, consumed) = decode_frame::<TestId>(&bytes).unwrap().unwrap();
    assert_eq!(consumed, bytes.len());
    match decoded {
        DistributedMessage::RequestInFlightRoster { sender_id, .. } => {
            assert_eq!(sender_id, "primary");
        }
        _ => panic!("expected RequestInFlightRoster"),
    }
}

/// #518 `InFlightRoster` round-trips: the reporting member, its membership
/// generation, AND every entry (hash + worker_id + typed identity) survive
/// the wire. The `member_gen` is carried on a NON-default value so a
/// dropped field would be caught, and the entries vec holds two members so
/// order + count are pinned.
#[test]
fn roundtrip_inflight_roster() {
    let msg: DistributedMessage<TestId> = DistributedMessage::InFlightRoster {
        target: None,
        sender_id: "sec-A".into(),
        timestamp: 7.0,
        secondary_id: "sec-A".into(),
        member_gen: 4,
        entries: vec![
            InFlightRosterEntry {
                hash: "h-1".into(),
                worker_id: 0,
                task_id: test_id("task-one"),
            },
            InFlightRosterEntry {
                hash: "h-2".into(),
                worker_id: 3,
                task_id: test_id("task-two"),
            },
        ],
    };
    let bytes = serialize_message(&msg).unwrap();
    let (decoded, consumed) = decode_frame::<TestId>(&bytes).unwrap().unwrap();
    assert_eq!(consumed, bytes.len());
    match decoded {
        DistributedMessage::InFlightRoster {
            secondary_id,
            member_gen,
            entries,
            ..
        } => {
            assert_eq!(secondary_id, "sec-A");
            assert_eq!(member_gen, 4, "the member generation must survive the wire");
            assert_eq!(entries.len(), 2);
            assert_eq!(entries[0].hash, "h-1");
            assert_eq!(entries[0].worker_id, 0);
            assert_eq!(entries[0].task_id.binary_name, "task-one");
            assert_eq!(entries[1].hash, "h-2");
            assert_eq!(entries[1].worker_id, 3);
            assert_eq!(entries[1].task_id.binary_name, "task-two");
        }
        _ => panic!("expected InFlightRoster"),
    }
}

/// #518 `WithdrawTask` round-trips: the addressed member, the worker id,
/// and the duplicate hash survive the wire — the primary directs exactly
/// one member's worker to stand down.
#[test]
fn roundtrip_withdraw_task() {
    let msg: DistributedMessage<TestId> = DistributedMessage::WithdrawTask {
        target: None,
        sender_id: "primary".into(),
        timestamp: 8.0,
        secondary_id: "sec-B".into(),
        worker_id: 2,
        task_hash: "h-dup".into(),
    };
    let bytes = serialize_message(&msg).unwrap();
    let (decoded, consumed) = decode_frame::<TestId>(&bytes).unwrap().unwrap();
    assert_eq!(consumed, bytes.len());
    match decoded {
        DistributedMessage::WithdrawTask {
            secondary_id,
            worker_id,
            task_hash,
            ..
        } => {
            assert_eq!(secondary_id, "sec-B");
            assert_eq!(worker_id, 2);
            assert_eq!(task_hash, "h-dup");
        }
        _ => panic!("expected WithdrawTask"),
    }
}

/// Wire-shape mirror (NOT symmetric-on-the-wrong-shape): decode the EXACT
/// JSON bytes each #518 sender emits — internally tagged, snake_case, with
/// the `target` routing header elided via `skip_serializing_if` — rather
/// than re-encoding our own value, so a tag/field rename that still
/// round-trips against itself is caught against the other side's bytes.
#[test]
fn inflight_reconcile_frames_decode_literal_sender_bytes() {
    // Primary -> re-admitted member: the pull request.
    let req = r#"{"msg_type":"request_in_flight_roster","sender_id":"primary","timestamp":1.0}"#;
    match serde_json::from_str::<DistributedMessage<TestId>>(req).unwrap() {
        DistributedMessage::RequestInFlightRoster {
            target, sender_id, ..
        } => {
            assert!(target.is_none(), "elided target must decode as None");
            assert_eq!(sender_id, "primary");
        }
        _ => panic!("expected RequestInFlightRoster"),
    }

    // Re-admitted member -> primary: the roster answer (one entry).
    let roster = r#"{"msg_type":"in_flight_roster","sender_id":"sec-A","timestamp":2.0,"secondary_id":"sec-A","member_gen":4,"entries":[{"hash":"h-1","worker_id":0,"task_id":{"binary_name":"task-one","platform":"x86_64","compiler":"gcc","version":"12.0","opt_level":"O2"}}]}"#;
    match serde_json::from_str::<DistributedMessage<TestId>>(roster).unwrap() {
        DistributedMessage::InFlightRoster {
            secondary_id,
            member_gen,
            entries,
            ..
        } => {
            assert_eq!(secondary_id, "sec-A");
            assert_eq!(member_gen, 4);
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].hash, "h-1");
            assert_eq!(entries[0].worker_id, 0);
        }
        _ => panic!("expected InFlightRoster"),
    }

    // Primary -> duplicate-holder: the withdraw command.
    let withdraw = r#"{"msg_type":"withdraw_task","sender_id":"primary","timestamp":3.0,"secondary_id":"sec-B","worker_id":2,"task_hash":"h-dup"}"#;
    match serde_json::from_str::<DistributedMessage<TestId>>(withdraw).unwrap() {
        DistributedMessage::WithdrawTask {
            secondary_id,
            worker_id,
            task_hash,
            ..
        } => {
            assert_eq!(secondary_id, "sec-B");
            assert_eq!(worker_id, 2);
            assert_eq!(task_hash, "h-dup");
        }
        _ => panic!("expected WithdrawTask"),
    }
}

/// The three #518 frames carry the EXPECTED `msg_type` discriminators on
/// the wire (the receiver demuxes on these), and a peer mirrors the
/// literal bytes back into the right variant — catches a `rename_all`
/// drift that would still round-trip against itself.
#[test]
fn inflight_reconcile_frames_wire_tags_mirror() {
    for (literal, want_tag) in [
        (
            r#"{"msg_type":"request_in_flight_roster","sender_id":"p","timestamp":0.0}"#,
            MessageType::RequestInFlightRoster,
        ),
        (
            r#"{"msg_type":"in_flight_roster","sender_id":"a","timestamp":0.0,"secondary_id":"a","member_gen":0,"entries":[]}"#,
            MessageType::InFlightRoster,
        ),
        (
            r#"{"msg_type":"withdraw_task","sender_id":"p","timestamp":0.0,"secondary_id":"b","worker_id":0,"task_hash":"h"}"#,
            MessageType::WithdrawTask,
        ),
    ] {
        let decoded: DistributedMessage<TestId> = serde_json::from_str(literal).unwrap();
        assert_eq!(decoded.msg_type(), want_tag, "literal: {literal}");
    }
}
