use super::*;
use dynrunner_core::ErrorType;

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
            is_observer: false,
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
                task_id: None,
                task_depends_on: vec![],
                preferred_secondaries: Default::default(),
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
            epoch: 1,
            required_setup: false,
        },
        DistributedMessage::RequestClusterSnapshot {
            sender_id: "s".into(),
            timestamp: 0.0,
        },
        DistributedMessage::ClusterSnapshot {
            sender_id: "p".into(),
            timestamp: 0.0,
            snapshot_json: "{}".into(),
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
            error_type: ErrorType::ResourceExhausted(ResourceKind::memory()),
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
        DistributedMessage::SecondaryFatalError {
            sender_id: "s".into(),
            timestamp: 0.0,
            secondary_id: "s".into(),
            error: "peer mesh fully failed to form: 0 of 4 peers reachable; cluster routing impossible".into(),
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
