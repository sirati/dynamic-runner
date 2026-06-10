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
        DistributedMessage::RequestClusterSnapshot {
            target: None,
            sender_id: "s".into(),
            timestamp: 0.0,
            is_observer: false,
            can_be_primary: true,
        },
        DistributedMessage::ClusterSnapshot {
            target: None,
            sender_id: "p".into(),
            timestamp: 0.0,
            snapshot_json: "{}".into(),
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
                custom_handled_watermarks_count: 1,
                custom_handled_watermarks_hash: 0xEE,
            },
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
    ];

    for msg in &messages {
        let bytes = serialize_message(msg).unwrap();
        let (decoded, consumed) = decode_frame::<TestId>(&bytes).unwrap().unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded.msg_type(), msg.msg_type());
        assert_eq!(decoded.sender_id(), msg.sender_id());
    }
}
