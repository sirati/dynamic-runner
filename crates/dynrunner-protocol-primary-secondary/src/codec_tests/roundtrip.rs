use super::*;

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
        is_observer: false,
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
            task_id: None,
            task_depends_on: vec![],
            preferred_secondaries: Default::default(),
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
            assert_eq!(error_type, ErrorType::ResourceExhausted(ResourceKind::memory()));
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

