//! Repro-first regression tests for the production wire-cap defect
//! (asm-dataset run_20260611_115429): a 67k-task `ClusterSnapshot`
//! serialized to ~100–116 MB, exceeded `MAX_WIRE_FRAME_BYTES` (96 MiB)
//! and was serialize-DROPPED by the sender's framing gate on every
//! anti-entropy / rejoin pull — the requester never converged and the
//! fleet starved. The fix: an oversized chunk-eligible frame is split
//! into `FrameChunk`s under the cap and reassembled at the receiving
//! pump.
//!
//! * `oversize_cluster_snapshot_crosses_the_mesh` — RED pre-fix (the
//!   frame was dropped, the receiver never got it), GREEN post-fix
//!   (byte-identical payload delivered through real QUIC/WSS legs).
//! * `oversize_consumer_payload_is_still_dropped` — pins that the cap
//!   is NOT relaxed for consumer payloads (#364/#366): an oversized
//!   `TaskComplete` is still rejected at the sender.

use std::time::Duration;

use super::super::PeerNetwork;
use super::TestId;
use dynrunner_protocol_primary_secondary::{
    DistributedMessage, PeerConnectionInfo, PeerTransport,
};

/// Wire up two real peer networks on localhost and return them
/// connected (same dance as `two_peers_exchange_messages`).
async fn connected_pair() -> (PeerNetwork<TestId>, PeerNetwork<TestId>) {
    let mut peer_a: PeerNetwork<TestId> = PeerNetwork::start("peer-a", None).await.unwrap();
    let mut peer_b: PeerNetwork<TestId> = PeerNetwork::start("peer-b", None).await.unwrap();

    let peers = vec![
        PeerConnectionInfo {
            secondary_id: "peer-a".into(),
            cert: peer_a.cert_pem().to_string(),
            ipv4: Some("127.0.0.1".into()),
            ipv6: None,
            port: peer_a.port(),
            is_observer: false,
            liveness_port: None,
        },
        PeerConnectionInfo {
            secondary_id: "peer-b".into(),
            cert: peer_b.cert_pem().to_string(),
            ipv4: Some("127.0.0.1".into()),
            ipv6: None,
            port: peer_b.port(),
            is_observer: false,
            liveness_port: None,
        },
    ];
    peer_a.connect_to_peers(&peers);
    peer_b.connect_to_peers(&peers);
    tokio::time::sleep(Duration::from_millis(100)).await;
    peer_a.drain_new_connections();
    peer_b.drain_new_connections();
    (peer_a, peer_b)
}

/// A snapshot payload guaranteed to push the serialized frame over
/// `MAX_WIRE_FRAME_BYTES` (the envelope adds a few hundred bytes; one
/// extra MiB of payload makes the violation unambiguous). The content
/// is structured (not one repeated byte) so a reassembly-order bug
/// cannot accidentally pass the equality check.
fn oversize_snapshot_json() -> String {
    let target = crate::framing::MAX_WIRE_FRAME_BYTES + 1024 * 1024;
    let mut s = String::with_capacity(target + 64);
    s.push_str("{\"tasks\":\"");
    let mut i: u64 = 0;
    while s.len() < target {
        s.push_str("task-");
        s.push_str(&i.to_string());
        s.push(' ');
        i += 1;
    }
    s.push_str("\"}");
    s
}

/// RED→GREEN for the production defect: a ClusterSnapshot larger than
/// the wire cap must cross the mesh INTACT (chunked + reassembled),
/// not be serialize-dropped in a loop.
#[tokio::test(flavor = "current_thread")]
async fn oversize_cluster_snapshot_crosses_the_mesh() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut peer_a, mut peer_b) = connected_pair().await;

            let snapshot_json = oversize_snapshot_json();
            let sent_len = snapshot_json.len();
            assert!(sent_len > crate::framing::MAX_WIRE_FRAME_BYTES);
            let sent_checksum =
                dynrunner_protocol_primary_secondary::chunking::fnv1a64(snapshot_json.as_bytes());

            let msg: DistributedMessage<TestId> = DistributedMessage::ClusterSnapshot {
                target: None,
                sender_id: "peer-a".into(),
                timestamp: 1.0,
                snapshot_json,
            };
            peer_a.send_to_peer("peer-b", msg).await.unwrap();

            // Generous budget: ~130 MB of chunk frames over localhost
            // QUIC/WSS plus base64 + reassembly work.
            let received = tokio::time::timeout(Duration::from_secs(60), peer_b.recv_peer())
                .await
                .expect("timeout: the oversize snapshot never arrived (dropped at the cap?)")
                .expect("transport closed before the snapshot arrived");

            match received {
                DistributedMessage::ClusterSnapshot {
                    sender_id,
                    snapshot_json,
                    ..
                } => {
                    assert_eq!(sender_id, "peer-a");
                    assert_eq!(snapshot_json.len(), sent_len, "payload length must survive");
                    assert_eq!(
                        dynrunner_protocol_primary_secondary::chunking::fnv1a64(
                            snapshot_json.as_bytes()
                        ),
                        sent_checksum,
                        "payload bytes must survive verbatim"
                    );
                }
                other => panic!("expected ClusterSnapshot, got {:?}", other.msg_type()),
            }
        })
        .await;
}

/// The wire cap is NOT relaxed for consumer payloads (#364/#366): an
/// oversized `TaskComplete` is still rejected at the sender — nothing
/// arrives, and the connection stays healthy (a normal frame sent
/// afterwards still gets through).
#[tokio::test(flavor = "current_thread")]
async fn oversize_consumer_payload_is_still_dropped() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut peer_a, mut peer_b) = connected_pair().await;

            // result_data rides JSON as a number array (~4 bytes per
            // element), so cap/2 raw bytes serialize comfortably over
            // the cap while keeping the test's memory modest.
            let oversize: DistributedMessage<TestId> = DistributedMessage::TaskComplete {
                target: None,
                sender_id: "peer-a".into(),
                timestamp: 1.0,
                secondary_id: "peer-a".into(),
                worker_id: 0,
                task_hash: "deadbeef".into(),
                result_data: Some(vec![b'x'; crate::framing::MAX_WIRE_FRAME_BYTES / 2]),
                delivery_seq: None,
                msgs_posted_through: None,
            };
            peer_a.send_to_peer("peer-b", oversize).await.unwrap();

            // A small follow-up frame proves (a) the oversize one was
            // dropped (the follow-up arrives FIRST — nothing precedes
            // it) and (b) the connection survived the violation.
            let follow_up: DistributedMessage<TestId> = DistributedMessage::TerminalAck {
                target: None,
                sender_id: "peer-a".into(),
                timestamp: 2.0,
                seq: 7,
            };
            peer_a.send_to_peer("peer-b", follow_up).await.unwrap();

            let received = tokio::time::timeout(Duration::from_secs(30), peer_b.recv_peer())
                .await
                .expect("timeout waiting for the follow-up frame")
                .expect("transport closed");
            match received {
                DistributedMessage::TerminalAck { seq, .. } => assert_eq!(seq, 7),
                other => panic!(
                    "the oversized TaskComplete must be dropped at the sender; \
                     got {:?} instead of the follow-up TerminalAck",
                    other.msg_type()
                ),
            }
        })
        .await;
}
