//! Unit tests for the `FrameChunk` split/reassemble mechanism: the
//! split→ingest round-trip, the idempotency/fault rules of the
//! reassembler (duplicate, gap, supersede, checksum, reassembly cap),
//! and the `FrameChunk` wire shape (round-trip + literal-bytes mirror +
//! the pinned loud-not-panicking legacy decode of an unknown variant).

use super::*;
use crate::codec;

#[derive(Debug, Clone, PartialEq, Eq, std::hash::Hash, serde::Serialize, serde::Deserialize)]
struct TestId(String);

fn snapshot_msg(payload: &str) -> DistributedMessage<TestId> {
    DistributedMessage::SnapshotStreamPackage {
        target: None,
        sender_id: "holder-1".into(),
        timestamp: 12.5,
        stream_id: "joiner/0".into(),
        seq: 0,
        cursor: None,
        payload: payload.into(),
        done: false,
    }
}

/// Destructure one FrameChunk's fields for ingest.
fn fields(msg: &DistributedMessage<TestId>) -> (u64, u32, u32, u64, &str) {
    match msg {
        DistributedMessage::FrameChunk {
            transfer_id,
            index,
            total,
            checksum,
            payload_b64,
            ..
        } => (*transfer_id, *index, *total, *checksum, payload_b64),
        other => panic!("expected FrameChunk, got {:?}", other.msg_type()),
    }
}

fn ingest_all(
    reasm: &mut ChunkReassembler,
    chunks: &[DistributedMessage<TestId>],
) -> Option<Vec<u8>> {
    for c in chunks {
        let (tid, idx, total, sum, b64) = fields(c);
        match reasm.ingest(tid, idx, total, sum, b64) {
            ChunkIngest {
                outcome: ChunkOutcome::Complete(bytes),
                ..
            } => return Some(bytes),
            ChunkIngest {
                outcome: ChunkOutcome::Incomplete,
                ..
            } => {}
            ChunkIngest {
                outcome: ChunkOutcome::Rejected { reason },
                ..
            } => panic!("unexpected rejection: {reason}"),
        }
    }
    None
}

/// Split → reassemble round-trip restores the EXACT original bytes and
/// the decoded message equals the original (full wire fidelity).
#[test]
fn split_and_reassemble_round_trip() {
    let msg = snapshot_msg(&"x".repeat(10_000));
    let frame = codec::serialize_message(&msg).unwrap();
    let json = &frame[4..];
    let chunks = split_frame(&msg, json, 1024);
    assert_eq!(chunks.len(), json.len().div_ceil(1024));
    // Every chunk shares the header; indexes are contiguous from 0.
    let (tid0, _, total0, sum0, _) = fields(&chunks[0]);
    assert_eq!(total0 as usize, chunks.len());
    for (i, c) in chunks.iter().enumerate() {
        let (tid, idx, total, sum, _) = fields(c);
        assert_eq!((tid, total, sum), (tid0, total0, sum0));
        assert_eq!(idx as usize, i);
        assert_eq!(c.sender_id(), "holder-1");
    }
    let mut reasm = ChunkReassembler::new(0);
    let bytes = ingest_all(&mut reasm, &chunks).expect("transfer must complete");
    assert_eq!(bytes, json);
    let decoded: DistributedMessage<TestId> = codec::deserialize_message(&bytes).unwrap();
    match decoded {
        DistributedMessage::SnapshotStreamPackage { payload, .. } => {
            assert_eq!(payload.len(), 10_000);
        }
        other => panic!("expected SnapshotStreamPackage, got {:?}", other.msg_type()),
    }
    assert!(!reasm.in_progress());
}

/// A duplicate of an already-consumed index is an idempotent NoOp; the
/// transfer still completes with the right bytes.
#[test]
fn duplicate_chunk_is_idempotent() {
    let msg = snapshot_msg(&"y".repeat(3_000));
    let frame = codec::serialize_message(&msg).unwrap();
    let json = &frame[4..];
    let chunks = split_frame(&msg, json, 512);
    assert!(chunks.len() >= 3);
    let mut reasm = ChunkReassembler::new(0);
    let (tid, idx, total, sum, b64) = fields(&chunks[0]);
    assert!(matches!(
        reasm.ingest(tid, idx, total, sum, b64).outcome,
        ChunkOutcome::Incomplete
    ));
    // Replay chunk 0: ignored, no abandonment, transfer still live.
    let replay = reasm.ingest(tid, idx, total, sum, b64);
    assert!(replay.abandoned.is_none());
    assert!(matches!(replay.outcome, ChunkOutcome::Incomplete));
    let bytes = ingest_all(&mut reasm, &chunks[1..]).expect("transfer must complete");
    assert_eq!(bytes, json);
}

/// An index gap (lost chunk on what should be an ordered leg) abandons
/// the partial with a notice — the bounded-loud path, never a silent
/// partial.
#[test]
fn index_gap_abandons_loudly() {
    let msg = snapshot_msg(&"z".repeat(3_000));
    let frame = codec::serialize_message(&msg).unwrap();
    let chunks = split_frame(&msg, &frame[4..], 512);
    assert!(chunks.len() >= 4);
    let mut reasm = ChunkReassembler::new(0);
    let (tid, idx, total, sum, b64) = fields(&chunks[0]);
    assert!(matches!(
        reasm.ingest(tid, idx, total, sum, b64).outcome,
        ChunkOutcome::Incomplete
    ));
    // Skip chunk 1, feed chunk 2: gap → abandoned + rejected.
    let (tid, idx, total, sum, b64) = fields(&chunks[2]);
    let res = reasm.ingest(tid, idx, total, sum, b64);
    let abandoned = res.abandoned.expect("partial must be abandoned");
    assert_eq!(abandoned.transfer_id, tid);
    assert_eq!(abandoned.chunks_received, 1);
    assert!(abandoned.buffered_bytes > 0);
    assert!(matches!(res.outcome, ChunkOutcome::Rejected { .. }));
    assert!(!reasm.in_progress());
}

/// A NEW transfer starting at index 0 supersedes an in-progress one:
/// the old partial is abandoned (one notice) and the new transfer
/// completes normally.
#[test]
fn superseding_transfer_abandons_old_and_completes() {
    let old = snapshot_msg(&"a".repeat(3_000));
    let new = snapshot_msg(&"b".repeat(2_000));
    let old_frame = codec::serialize_message(&old).unwrap();
    let new_frame = codec::serialize_message(&new).unwrap();
    let old_chunks = split_frame(&old, &old_frame[4..], 512);
    let new_chunks = split_frame(&new, &new_frame[4..], 512);
    let mut reasm = ChunkReassembler::new(0);
    let (tid, idx, total, sum, b64) = fields(&old_chunks[0]);
    assert!(matches!(
        reasm.ingest(tid, idx, total, sum, b64).outcome,
        ChunkOutcome::Incomplete
    ));
    // First chunk of the NEW transfer lands: old partial abandoned.
    let (tid2, idx2, total2, sum2, b64_2) = fields(&new_chunks[0]);
    let res = reasm.ingest(tid2, idx2, total2, sum2, b64_2);
    let abandoned = res.abandoned.expect("old partial must be abandoned");
    assert_eq!(abandoned.transfer_id, tid);
    assert!(matches!(res.outcome, ChunkOutcome::Incomplete));
    let bytes = ingest_all(&mut reasm, &new_chunks[1..]).expect("new transfer completes");
    assert_eq!(bytes, &new_frame[4..]);
}

/// A mid-transfer chunk of an unknown transfer (receiver joined late /
/// state was discarded) is rejected — never a silent partial.
#[test]
fn mid_transfer_join_is_rejected() {
    let msg = snapshot_msg(&"c".repeat(3_000));
    let frame = codec::serialize_message(&msg).unwrap();
    let chunks = split_frame(&msg, &frame[4..], 512);
    let mut reasm = ChunkReassembler::new(0);
    let (tid, idx, total, sum, b64) = fields(&chunks[2]);
    let res = reasm.ingest(tid, idx, total, sum, b64);
    assert!(res.abandoned.is_none());
    assert!(matches!(res.outcome, ChunkOutcome::Rejected { .. }));
}

/// A corrupted slice fails the final checksum and the transfer is
/// rejected loudly rather than delivering corrupt bytes.
#[test]
fn checksum_mismatch_rejects() {
    let msg = snapshot_msg(&"d".repeat(2_000));
    let frame = codec::serialize_message(&msg).unwrap();
    let chunks = split_frame(&msg, &frame[4..], 512);
    let mut reasm = ChunkReassembler::new(0);
    let last = chunks.len() - 1;
    for c in &chunks[..last] {
        let (tid, idx, total, sum, b64) = fields(c);
        assert!(matches!(
            reasm.ingest(tid, idx, total, sum, b64).outcome,
            ChunkOutcome::Incomplete
        ));
    }
    // Corrupt the final slice (valid base64, wrong bytes).
    use base64::Engine as _;
    let (tid, idx, total, sum, b64) = fields(&chunks[last]);
    let mut raw = base64::engine::general_purpose::STANDARD.decode(b64).unwrap();
    raw[0] ^= 0xff;
    let corrupt = base64::engine::general_purpose::STANDARD.encode(&raw);
    let res = reasm.ingest(tid, idx, total, sum, &corrupt);
    match res.outcome {
        ChunkOutcome::Rejected { reason } => assert!(reason.contains("checksum")),
        other => panic!("expected checksum rejection, got {other:?}"),
    }
    assert!(!reasm.in_progress());
}

/// The reassembly cap rejects a transfer before buffering past the
/// policy limit (memory-bound against corrupt/malicious `total`).
#[test]
fn reassembly_cap_rejects_oversize_transfer() {
    let msg = snapshot_msg(&"e".repeat(5_000));
    let frame = codec::serialize_message(&msg).unwrap();
    let chunks = split_frame(&msg, &frame[4..], 512);
    let mut reasm = ChunkReassembler::new(1024);
    let mut rejected = false;
    for c in &chunks {
        let (tid, idx, total, sum, b64) = fields(c);
        match reasm.ingest(tid, idx, total, sum, b64).outcome {
            ChunkOutcome::Incomplete => {}
            ChunkOutcome::Rejected { reason } => {
                assert!(reason.contains("reassembly cap"));
                rejected = true;
                break;
            }
            ChunkOutcome::Complete(_) => panic!("must not complete past the cap"),
        }
    }
    assert!(rejected, "the cap must reject the transfer");
    assert!(!reasm.in_progress());
}

/// FNV-1a-64 is pinned to its published constants — the checksum is a
/// cross-process wire contract, so the algorithm must never drift.
#[test]
fn fnv1a64_pinned_vectors() {
    // Published FNV-1a 64 test vectors.
    assert_eq!(fnv1a64(b""), 0xcbf2_9ce4_8422_2325);
    assert_eq!(fnv1a64(b"a"), 0xaf63_dc4c_8601_ec8c);
    assert_eq!(fnv1a64(b"foobar"), 0x85944171f73967e8);
}

/// `chunk_eligible` is the closed framework-frame allowlist: the
/// snapshot (and a Relay around it) is eligible; consumer-payload
/// carriers are NOT (the #364/#366 cap is a contract).
#[test]
fn chunk_eligibility_allowlist() {
    let snap = snapshot_msg("{}");
    assert!(snap.chunk_eligible());
    let relayed: DistributedMessage<TestId> = DistributedMessage::Relay {
        target: None,
        sender_id: "s".into(),
        timestamp: 0.0,
        target_id: "t".into(),
        relay_id: 1,
        path: vec!["s".into()],
        inner: Box::new(snapshot_msg("{}")),
    };
    assert!(relayed.chunk_eligible());
    let task_complete: DistributedMessage<TestId> = DistributedMessage::TaskComplete {
        target: None,
        sender_id: "s".into(),
        timestamp: 0.0,
        secondary_id: "s".into(),
        worker_id: 0,
        task_hash: "h".into(),
        result_data: Some(vec![0u8; 64]),
        delivery_seq: None,
        msgs_posted_through: None,
    };
    assert!(!task_complete.chunk_eligible());
    let relayed_task: DistributedMessage<TestId> = DistributedMessage::Relay {
        target: None,
        sender_id: "s".into(),
        timestamp: 0.0,
        target_id: "t".into(),
        relay_id: 2,
        path: vec!["s".into()],
        inner: Box::new(task_complete),
    };
    assert!(!relayed_task.chunk_eligible());
}

/// FrameChunk codec round-trip through the length-prefixed frame.
#[test]
fn frame_chunk_codec_round_trip() {
    let chunk: DistributedMessage<TestId> = DistributedMessage::FrameChunk {
        target: None,
        sender_id: "holder-1".into(),
        timestamp: 3.25,
        transfer_id: 42,
        index: 1,
        total: 3,
        checksum: 0xdead_beef_cafe_f00d,
        payload_b64: "aGVsbG8=".into(),
    };
    assert_eq!(chunk.msg_type(), crate::MessageType::FrameChunk);
    let frame = codec::serialize_message(&chunk).unwrap();
    let (decoded, consumed) = codec::decode_frame::<TestId>(&frame).unwrap().unwrap();
    assert_eq!(consumed, frame.len());
    match decoded {
        DistributedMessage::FrameChunk {
            sender_id,
            transfer_id,
            index,
            total,
            checksum,
            payload_b64,
            ..
        } => {
            assert_eq!(sender_id, "holder-1");
            assert_eq!((transfer_id, index, total), (42, 1, 3));
            assert_eq!(checksum, 0xdead_beef_cafe_f00d);
            assert_eq!(payload_b64, "aGVsbG8=");
        }
        other => panic!("expected FrameChunk, got {:?}", other.msg_type()),
    }
}

/// Wire-shape mirror (NOT symmetric-on-the-wrong-shape): decode the
/// EXACT JSON bytes a sender's framing layer emits, pinning the field
/// names + tag the other side must produce.
#[test]
fn frame_chunk_decodes_literal_sender_bytes() {
    let literal = r#"{"msg_type":"frame_chunk","sender_id":"holder-1","timestamp":3.25,"transfer_id":42,"index":1,"total":3,"checksum":1311768467463790320,"payload_b64":"aGVsbG8="}"#;
    let decoded: DistributedMessage<TestId> = serde_json::from_str(literal).unwrap();
    match &decoded {
        DistributedMessage::FrameChunk {
            target,
            sender_id,
            timestamp,
            transfer_id,
            index,
            total,
            checksum,
            payload_b64,
        } => {
            let (timestamp, transfer_id, index, total, checksum) =
                (*timestamp, *transfer_id, *index, *total, *checksum);
            assert!(target.is_none());
            assert_eq!(sender_id, "holder-1");
            assert_eq!(timestamp, 3.25);
            assert_eq!((transfer_id, index, total), (42, 1, 3));
            assert_eq!(checksum, 0x1234_5678_9abc_def0);
            assert_eq!(payload_b64, "aGVsbG8=");
        }
        other => panic!("expected FrameChunk, got {:?}", other.msg_type()),
    }
    // And the encoder produces the same shape back (tag + fields).
    let reencoded = serde_json::to_string(&decoded).unwrap();
    assert_eq!(reencoded, literal);
}

/// Legacy-decode pin: a receiver WITHOUT this variant (any unknown
/// `msg_type` tag) gets a decode `Err` naming the unknown variant —
/// loud-but-graceful (the pumps log ERROR and run the normal disconnect
/// path), never a panic and never a silent drop.
#[test]
fn unknown_msg_type_is_a_loud_decode_error() {
    let future_frame =
        br#"{"msg_type":"from_the_future","sender_id":"s","timestamp":0.0,"weird":true}"#;
    let err = codec::deserialize_message::<TestId>(future_frame).unwrap_err();
    assert!(err.contains("unknown variant"), "got: {err}");
    assert!(err.contains("from_the_future"), "got: {err}");
}
