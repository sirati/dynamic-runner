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
