use db_comm_api_base::Identifier;
use crate::messages::DistributedMessage;

/// Serialize a distributed message to a length-prefixed JSON frame.
///
/// Wire format: 4-byte big-endian length prefix + JSON bytes.
/// This matches the Python protocol which uses length-prefixed JSON.
pub fn serialize_message<I: Identifier>(msg: &DistributedMessage<I>) -> Result<Vec<u8>, String> {
    let json = serde_json::to_string(msg).map_err(|e| e.to_string())?;
    let json_bytes = json.as_bytes();
    let len = json_bytes.len() as u32;
    let mut buf = Vec::with_capacity(4 + json_bytes.len());
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(json_bytes);
    Ok(buf)
}

/// Deserialize a distributed message from JSON bytes (without length prefix).
pub fn deserialize_message<I: Identifier>(json_bytes: &[u8]) -> Result<DistributedMessage<I>, String> {
    serde_json::from_slice(json_bytes).map_err(|e| e.to_string())
}

/// Extract one message from a buffer that may contain length-prefixed frames.
///
/// Returns (message, bytes_consumed) or None if not enough data.
pub fn decode_frame<I: Identifier>(buf: &[u8]) -> Result<Option<(DistributedMessage<I>, usize)>, String> {
    if buf.len() < 4 {
        return Ok(None);
    }
    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if buf.len() < 4 + len {
        return Ok(None);
    }
    let msg = deserialize_message(&buf[4..4 + len])?;
    Ok(Some((msg, 4 + len)))
}

#[cfg(test)]
mod tests {
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
        let msg: DistributedMessage<TestId> = DistributedMessage::SecondaryWelcome {
            sender_id: "sec-2".into(),
            timestamp: 9999.0,
            secondary_id: "sec-2".into(),
            ram_bytes: 8 * 1024 * 1024 * 1024,
            worker_count: 4,
            hostname: "node-01".into(),
        };

        let bytes = serialize_message(&msg).unwrap();
        let (decoded, _) = decode_frame::<TestId>(&bytes).unwrap().unwrap();

        match decoded {
            DistributedMessage::SecondaryWelcome {
                ram_bytes,
                worker_count,
                hostname,
                ..
            } => {
                assert_eq!(ram_bytes, 8 * 1024 * 1024 * 1024);
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
    fn roundtrip_command_result() {
        let msg: DistributedMessage<TestId> = DistributedMessage::CommandResult {
            sender_id: "host".into(),
            timestamp: 400.0,
            command_id: "cmd-1".into(),
            return_code: 0,
            stdout: "hello\n".into(),
            stderr: String::new(),
        };

        let bytes = serialize_message(&msg).unwrap();
        let (decoded, _) = decode_frame::<TestId>(&bytes).unwrap().unwrap();

        match decoded {
            DistributedMessage::CommandResult {
                command_id,
                return_code,
                stdout,
                ..
            } => {
                assert_eq!(command_id, "cmd-1");
                assert_eq!(return_code, 0);
                assert_eq!(stdout, "hello\n");
            }
            _ => panic!("expected CommandResult"),
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
        let messages: Vec<DistributedMessage<TestId>> = vec![
            DistributedMessage::SecondaryWelcome {
                sender_id: "s".into(),
                timestamp: 0.0,
                secondary_id: "s".into(),
                ram_bytes: 1024,
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
                sender_id: "p".into(),
                timestamp: 0.0,
                secondary_id: "s".into(),
                zip_files: vec![],
                workers_ready: vec![],
            },
            DistributedMessage::TaskRequest {
                sender_id: "s".into(),
                timestamp: 0.0,
                secondary_id: "s".into(),
                worker_id: 0,
                available_memory: 1024,
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
            },
            DistributedMessage::TaskComplete {
                sender_id: "s".into(),
                timestamp: 0.0,
                secondary_id: "s".into(),
                worker_id: 0,
                task_hash: "h".into(),
                warnings: 0,
                filtered: 0,
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
                query_secondary_id: "s2".into(),
            },
            DistributedMessage::TimeoutResponse {
                sender_id: "s".into(),
                timestamp: 0.0,
                query_secondary_id: "s2".into(),
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
            DistributedMessage::ExecuteCommand {
                sender_id: "c".into(),
                timestamp: 0.0,
                command: "ls".into(),
                command_id: "cmd-1".into(),
            },
            DistributedMessage::CommandResult {
                sender_id: "h".into(),
                timestamp: 0.0,
                command_id: "cmd-1".into(),
                return_code: 0,
                stdout: "".into(),
                stderr: "".into(),
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

    /// Verify wire format backward compatibility: identifier fields are
    /// flattened into the JSON object, not nested under "identifier".
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
            },
            local_path: "test".into(),
            file_hash: "h".into(),
        };

        let json = serde_json::to_string(&msg).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();

        // binary_info should have flattened fields
        let bi = &v["binary_info"];
        assert_eq!(bi["binary_name"], "test_binary");
        assert_eq!(bi["platform"], "x86_64");
        assert_eq!(bi["path"], "/tmp/test");
        assert_eq!(bi["size"], 1024);
        // Should NOT have a nested "identifier" key
        assert!(bi.get("identifier").is_none());
    }
}
