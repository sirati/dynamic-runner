use super::*;

#[test]
fn roundtrip_role_addressed() {
    let inner: DistributedMessage<TestId> = DistributedMessage::TaskRequest {
        sender_id: "sec-a".into(),
        timestamp: 1.0,
        secondary_id: "sec-a".into(),
        worker_id: 3,
        available_resources: vec![],
    };
    let envelope: DistributedMessage<TestId> = DistributedMessage::RoleAddressed {
        sender_id: "sec-a".into(),
        timestamp: 2.0,
        intended_role: Role::Primary,
        payload: Box::new(inner),
        attempts: 0,
    };
    let frame = serialize_message(&envelope).unwrap();
    let (decoded, n) = decode_frame::<TestId>(&frame).unwrap().unwrap();
    assert_eq!(n, frame.len());
    match decoded {
        DistributedMessage::RoleAddressed {
            sender_id,
            intended_role,
            payload,
            attempts,
            ..
        } => {
            assert_eq!(sender_id, "sec-a");
            assert_eq!(intended_role, Role::Primary);
            assert_eq!(attempts, 0);
            match *payload {
                DistributedMessage::TaskRequest {
                    secondary_id,
                    worker_id,
                    ..
                } => {
                    assert_eq!(secondary_id, "sec-a");
                    assert_eq!(worker_id, 3);
                }
                other => panic!("expected wrapped TaskRequest, got {:?}", other.msg_type()),
            }
        }
        other => panic!("expected RoleAddressed, got {:?}", other.msg_type()),
    }
}

/// `RoleMisaddressHint` round-trip: the cache-warming response a
/// receiver in Step 4 will emit when its own role-table disagrees
/// with the sender's. All four wire fields (`sender_id`,
/// `timestamp`, `role`, `holder_id`) must survive the frame.
#[test]
fn roundtrip_role_misaddress_hint() {
    let msg: DistributedMessage<TestId> = DistributedMessage::RoleMisaddressHint {
        sender_id: "sec-b".into(),
        timestamp: 12.5,
        role: Role::Primary,
        holder_id: "sec-c".into(),
    };
    let frame = serialize_message(&msg).unwrap();
    let (decoded, n) = decode_frame::<TestId>(&frame).unwrap().unwrap();
    assert_eq!(n, frame.len());
    match decoded {
        DistributedMessage::RoleMisaddressHint {
            sender_id,
            timestamp,
            role,
            holder_id,
        } => {
            assert_eq!(sender_id, "sec-b");
            assert!((timestamp - 12.5).abs() < f64::EPSILON);
            assert_eq!(role, Role::Primary);
            assert_eq!(holder_id, "sec-c");
        }
        other => panic!("expected RoleMisaddressHint, got {:?}", other.msg_type()),
    }
}

/// Backward-compat: a JSON encoding of `RoleAddressed` that omits
/// the `attempts` field decodes with `attempts == 0` (the
/// `#[serde(default)]` default). Without the default the receiver
/// would refuse pre-Step-3 senders. Same shape as the
/// `is_observer` / `required_setup` backcompat tests above —
/// `serde(default)` is the protocol's standing pattern for adding
/// fields without breaking rolling upgrades.
#[test]
fn role_addressed_backcompat_default_attempts() {
    // Hand-write JSON omitting `attempts`. The inner payload is a
    // minimal `Keepalive` so the wrapper structure stays tiny.
    let legacy = serde_json::json!({
        "msg_type": "role_addressed",
        "sender_id": "sec-a",
        "timestamp": 0.0,
        "intended_role": "primary",
        "payload": {
            "msg_type": "keepalive",
            "sender_id": "sec-a",
            "timestamp": 0.0,
            "secondary_id": "sec-a",
            "active_workers": 0
        }
    });
    let bytes = serde_json::to_vec(&legacy).unwrap();
    let decoded: DistributedMessage<TestId> = serde_json::from_slice(&bytes).unwrap();
    match decoded {
        DistributedMessage::RoleAddressed {
            intended_role,
            attempts,
            payload,
            ..
        } => {
            assert_eq!(intended_role, Role::Primary);
            assert_eq!(attempts, 0, "missing attempts field must decode as 0");
            assert!(matches!(*payload, DistributedMessage::Keepalive { .. }));
        }
        other => panic!("expected RoleAddressed, got {:?}", other.msg_type()),
    }
}
