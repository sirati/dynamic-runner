use super::*;
use crate::cluster_mutation::ClusterMutation;

#[test]
fn roundtrip_task_completed_with_result_data() {
    let mutation: ClusterMutation<TestId> = ClusterMutation::TaskCompleted {
        hash: "h-result".into(),
        result_data: Some(b"foo".to_vec()),
    };

    let json = serde_json::to_string(&mutation).unwrap();
    let decoded: ClusterMutation<TestId> = serde_json::from_str(&json).unwrap();

    match decoded {
        ClusterMutation::TaskCompleted { hash, result_data } => {
            assert_eq!(hash, "h-result");
            assert_eq!(result_data.as_deref(), Some(b"foo".as_ref()));
        }
        _ => panic!("expected TaskCompleted"),
    }
}

/// Backward-compat: a pre-Phase-2a sender's JSON shape — bare `{ "hash": ... }`
/// without the new `result_data` field — must decode with `result_data: None`.
/// Without `#[serde(default)]` the decode would refuse the frame and break
/// rolling upgrades.
#[test]
fn legacy_task_completed_decodes_without_result_data() {
    let legacy = serde_json::json!({
        "TaskCompleted": { "hash": "legacy-hash" }
    });
    let json = legacy.to_string();
    let decoded: ClusterMutation<TestId> = serde_json::from_str(&json).unwrap();

    match decoded {
        ClusterMutation::TaskCompleted { hash, result_data } => {
            assert_eq!(hash, "legacy-hash");
            assert!(result_data.is_none());
        }
        _ => panic!("expected TaskCompleted"),
    }
}

/// `SecondaryCapacity` round-trips through serde with its
/// `worker_count` + advertised `resources` preserved verbatim.
#[test]
fn roundtrip_secondary_capacity() {
    let mutation: ClusterMutation<TestId> = ClusterMutation::SecondaryCapacity {
        secondary: "sec-0".into(),
        worker_count: 6,
        resources: vec![dynrunner_core::ResourceAmount {
            kind: ResourceKind::memory(),
            amount: 8 * 1024 * 1024 * 1024,
        }],
    };

    let json = serde_json::to_string(&mutation).unwrap();
    let decoded: ClusterMutation<TestId> = serde_json::from_str(&json).unwrap();

    match decoded {
        ClusterMutation::SecondaryCapacity {
            secondary,
            worker_count,
            resources,
        } => {
            assert_eq!(secondary, "sec-0");
            assert_eq!(worker_count, 6);
            assert_eq!(
                resources,
                vec![dynrunner_core::ResourceAmount {
                    kind: ResourceKind::memory(),
                    amount: 8 * 1024 * 1024 * 1024,
                }]
            );
        }
        _ => panic!("expected SecondaryCapacity"),
    }
}

/// `TaskRequeued` round-trips through serde with its `hash` preserved
/// (the dead-secondary recovery `InFlight → Pending` mutation carries
/// only the hash; the preserved `TaskInfo` lives on the ledger entry,
/// not the wire).
#[test]
fn roundtrip_task_requeued() {
    let mutation: ClusterMutation<TestId> = ClusterMutation::TaskRequeued {
        hash: "h-requeued".into(),
    };

    let json = serde_json::to_string(&mutation).unwrap();
    let decoded: ClusterMutation<TestId> = serde_json::from_str(&json).unwrap();

    match decoded {
        ClusterMutation::TaskRequeued { hash } => assert_eq!(hash, "h-requeued"),
        _ => panic!("expected TaskRequeued"),
    }
}

/// `RunAborted` round-trips through serde with its `reason` preserved
/// (the failure twin of `RunComplete`; carries the operator-facing abort
/// reason that rides the wire to every connected secondary).
#[test]
fn roundtrip_run_aborted() {
    let mutation: ClusterMutation<TestId> = ClusterMutation::RunAborted {
        reason: "2 duplicate task identities in the initial batch".into(),
    };

    let json = serde_json::to_string(&mutation).unwrap();
    let decoded: ClusterMutation<TestId> = serde_json::from_str(&json).unwrap();

    match decoded {
        ClusterMutation::RunAborted { reason } => {
            assert_eq!(reason, "2 duplicate task identities in the initial batch");
        }
        _ => panic!("expected RunAborted"),
    }
}

/// `skip_serializing_if = "Option::is_none"` means an absent `result_data`
/// elides from the JSON output entirely — the wire bytes are identical to
/// the legacy bare-hash form, so new senders sending `result_data: None`
/// don't bloat the wire.
#[test]
fn task_completed_omits_absent_result_data_on_wire() {
    let mutation: ClusterMutation<TestId> = ClusterMutation::TaskCompleted {
        hash: "h-bare".into(),
        result_data: None,
    };
    let v = serde_json::to_value(&mutation).unwrap();
    let inner = &v["TaskCompleted"];
    assert!(
        inner.get("result_data").is_none(),
        "absent result_data must be omitted on the wire, got: {v}"
    );
    assert_eq!(inner["hash"], "h-bare");
}
