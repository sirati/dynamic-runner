use super::*;

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

/// Backward-compat: a pre-fix `PromotePrimary` wire frame that omits
/// the new `required_setup` field decodes with `required_setup=false`,
/// taking the legacy bootstrap/failover path unchanged. Without
/// `#[serde(default)]` this would refuse the frame and break rolling
/// upgrades that mix pre- and post-fix senders. Round-trip with
/// `required_setup=true` also exercises the field surviving encode/
/// decode in the setup-promote direction.
#[test]
fn promote_primary_required_setup_backcompat_and_roundtrip() {
    // Pre-fix wire format: no `required_setup` key in the JSON object.
    // Must decode with the default (`false`) — same path as legacy
    // bootstrap and failover.
    let legacy = serde_json::json!({
        "msg_type": "promote_primary",
        "sender_id": "primary",
        "timestamp": 0.0,
        "new_primary_id": "sec-a",
        "epoch": 1
    });
    let bytes = serde_json::to_vec(&legacy).unwrap();
    let decoded: DistributedMessage<TestId> = serde_json::from_slice(&bytes).unwrap();
    match decoded {
        DistributedMessage::PromotePrimary {
            new_primary_id,
            epoch,
            required_setup,
            ..
        } => {
            assert_eq!(new_primary_id, "sec-a");
            assert_eq!(epoch, 1);
            assert!(
                !required_setup,
                "pre-fix wire frame must decode with required_setup=false"
            );
        }
        other => panic!("expected PromotePrimary, got {:?}", other.msg_type()),
    }

    // Post-fix wire format with the setup-promote flag set: must
    // survive a full length-prefixed frame round-trip.
    let msg: DistributedMessage<TestId> = DistributedMessage::PromotePrimary {
        sender_id: "primary".into(),
        timestamp: 0.0,
        new_primary_id: "sec-a".into(),
        epoch: 7,
        required_setup: true,
    };
    let frame = serialize_message(&msg).unwrap();
    let (decoded, n) = decode_frame::<TestId>(&frame).unwrap().unwrap();
    assert_eq!(n, frame.len());
    match decoded {
        DistributedMessage::PromotePrimary {
            new_primary_id,
            epoch,
            required_setup,
            ..
        } => {
            assert_eq!(new_primary_id, "sec-a");
            assert_eq!(epoch, 7);
            assert!(
                required_setup,
                "required_setup=true must survive encode/decode round-trip"
            );
        }
        other => panic!("expected PromotePrimary, got {:?}", other.msg_type()),
    }
}
