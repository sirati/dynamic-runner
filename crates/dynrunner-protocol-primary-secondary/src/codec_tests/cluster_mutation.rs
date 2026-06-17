use super::*;
use crate::cluster_mutation::{ClusterMutation, PrimaryChangeReason};
use crate::removal_cause::RemovalCause;
use dynrunner_core::TaskVersion;

#[test]
fn roundtrip_task_completed_with_result_data() {
    let mutation: ClusterMutation<TestId> = ClusterMutation::TaskCompleted {
        hash: "h-result".into(),
        result_data: Some(b"foo".to_vec()),
        // Pin a NON-DEFAULT attempt (F2) so the assertion catches a dropped
        // `attempt` on the wire.
        attempt: 3,
    };

    let json = serde_json::to_string(&mutation).unwrap();
    let decoded: ClusterMutation<TestId> = serde_json::from_str(&json).unwrap();

    match decoded {
        ClusterMutation::TaskCompleted {
            hash,
            result_data,
            attempt,
        } => {
            assert_eq!(hash, "h-result");
            assert_eq!(result_data.as_deref(), Some(b"foo".as_ref()));
            assert_eq!(attempt, 3);
        }
        _ => panic!("expected TaskCompleted"),
    }
}

/// Backward-compat: a pre-Phase-2a sender's JSON shape — bare `{ "hash": ... }`
/// without the new `result_data` field (nor the F2 `attempt`) — must decode
/// with `result_data: None` and `attempt: 0`. Without `#[serde(default)]` the
/// decode would refuse the frame and break rolling upgrades.
#[test]
fn legacy_task_completed_decodes_without_result_data() {
    let legacy = serde_json::json!({
        "TaskCompleted": { "hash": "legacy-hash" }
    });
    let json = legacy.to_string();
    let decoded: ClusterMutation<TestId> = serde_json::from_str(&json).unwrap();

    match decoded {
        ClusterMutation::TaskCompleted {
            hash,
            result_data,
            attempt,
        } => {
            assert_eq!(hash, "legacy-hash");
            assert!(result_data.is_none());
            assert_eq!(attempt, 0);
        }
        _ => panic!("expected TaskCompleted"),
    }
}

/// `TaskSkippedAlreadyDone` round-trips through serde with its `hash`
/// preserved (the discovery-time skip carries only the hash; the
/// preserved `TaskInfo` lives on the ledger entry the prior `TaskAdded`
/// seeded, not on the wire).
#[test]
fn roundtrip_task_skipped_already_done() {
    let mutation: ClusterMutation<TestId> = ClusterMutation::TaskSkippedAlreadyDone {
        hash: "h-skip".into(),
    };

    let json = serde_json::to_string(&mutation).unwrap();
    let decoded: ClusterMutation<TestId> = serde_json::from_str(&json).unwrap();

    match decoded {
        ClusterMutation::TaskSkippedAlreadyDone { hash } => {
            assert_eq!(hash, "h-skip");
        }
        _ => panic!("expected TaskSkippedAlreadyDone"),
    }
}

/// Wire-shape mirror (NOT symmetric-on-the-wrong-shape): decode the EXACT
/// JSON bytes a sender emits — `{"TaskSkippedAlreadyDone":{"hash":"..."}}`
/// — rather than re-encoding our own value. A future field reorder/rename
/// that still round-trips against itself would slip past a symmetric test;
/// pinning the literal sender bytes catches the divergence against the
/// other side's actual wire.
#[test]
fn task_skipped_already_done_decodes_literal_sender_bytes() {
    let bytes = r#"{"TaskSkippedAlreadyDone":{"hash":"h-from-sender"}}"#;
    let decoded: ClusterMutation<TestId> = serde_json::from_str(bytes).unwrap();

    match decoded {
        ClusterMutation::TaskSkippedAlreadyDone { hash } => {
            assert_eq!(hash, "h-from-sender");
        }
        _ => panic!("expected TaskSkippedAlreadyDone"),
    }
}

/// The minimal `task` JSON object a `TaskAdded` carries — only the
/// non-serde-default `TaskInfo` fields (the rest decode to their defaults),
/// so the def-id wire tests can decode literal sender bytes without spelling
/// out all sixteen fields.
fn minimal_task_json(task_id: &str) -> serde_json::Value {
    serde_json::json!({
        "path": "/tasks/x",
        "size": 0,
        "identifier": test_id(task_id),
        "phase_id": "p0",
        "type_id": "t0",
        "affinity_id": null,
        "payload": null,
        "task_id": task_id,
    })
}

/// `TaskAdded` round-trips its PRIMARY-allocated `def_id` (L3a): a stamped
/// `Some(7)` survives the wire so every replica interns the def under the
/// same id. Pins a NON-default id so a dropped `def_id` would fail the
/// assertion.
#[test]
fn roundtrip_task_added_carries_def_id() {
    let json = serde_json::json!({
        "TaskAdded": {
            "hash": "h-added",
            "task": minimal_task_json("t-added"),
            "def_id": 7,
        }
    })
    .to_string();
    let decoded: ClusterMutation<TestId> = serde_json::from_str(&json).unwrap();
    match decoded {
        ClusterMutation::TaskAdded { hash, def_id, .. } => {
            assert_eq!(hash, "h-added");
            assert_eq!(def_id, Some(7));
        }
        _ => panic!("expected TaskAdded"),
    }
}

/// Backward-compat: a pre-L3a sender's `TaskAdded` JSON — no `def_id` field —
/// decodes with `def_id: None` (the un-allocated local-apply shape, which the
/// receiver falls back to node-local allocation for). Without `#[serde(default)]`
/// the decode would refuse the frame and break rolling upgrades. Mirrors the
/// `legacy_task_completed_decodes_without_result_data` contract.
#[test]
fn legacy_task_added_decodes_without_def_id() {
    let json = serde_json::json!({
        "TaskAdded": {
            "hash": "h-legacy",
            "task": minimal_task_json("t-legacy"),
        }
    })
    .to_string();
    let decoded: ClusterMutation<TestId> = serde_json::from_str(&json).unwrap();
    match decoded {
        ClusterMutation::TaskAdded { hash, def_id, .. } => {
            assert_eq!(hash, "h-legacy");
            assert_eq!(def_id, None);
        }
        _ => panic!("expected TaskAdded"),
    }
}

/// `SetupCompleted` round-trips with its `hash` preserved (the setup-success
/// terminal carries only the hash — version-LESS / attempt-LESS, like
/// `TaskSkippedAlreadyDone`; the `TaskInfo` + `attempt` live on the ledger
/// entry, not on the wire).
#[test]
fn roundtrip_setup_completed() {
    let mutation: ClusterMutation<TestId> = ClusterMutation::SetupCompleted {
        hash: "h-setup".into(),
    };
    let json = serde_json::to_string(&mutation).unwrap();
    let decoded: ClusterMutation<TestId> = serde_json::from_str(&json).unwrap();
    match decoded {
        ClusterMutation::SetupCompleted { hash } => assert_eq!(hash, "h-setup"),
        _ => panic!("expected SetupCompleted"),
    }
}

/// Wire-shape mirror (NOT symmetric-on-the-wrong-shape): decode the EXACT
/// JSON bytes the executor's success origination emits —
/// `{"SetupCompleted":{"hash":"..."}}` — pinning the externally-tagged
/// shape the other side must produce.
#[test]
fn setup_completed_decodes_literal_sender_bytes() {
    let bytes = r#"{"SetupCompleted":{"hash":"h-from-executor"}}"#;
    let decoded: ClusterMutation<TestId> = serde_json::from_str(bytes).unwrap();
    match decoded {
        ClusterMutation::SetupCompleted { hash } => assert_eq!(hash, "h-from-executor"),
        _ => panic!("expected SetupCompleted"),
    }
}

/// `PhaseEnded` round-trips through serde with its `phase` preserved
/// (the replicated "on_phase_end edge completed" fact carries only the
/// phase id — grow-only set semantics live in the apply rule, not on the
/// wire).
#[test]
fn roundtrip_phase_ended() {
    let mutation: ClusterMutation<TestId> = ClusterMutation::PhaseEnded {
        phase: dynrunner_core::PhaseId::from("build"),
    };

    let json = serde_json::to_string(&mutation).unwrap();
    let decoded: ClusterMutation<TestId> = serde_json::from_str(&json).unwrap();

    match decoded {
        ClusterMutation::PhaseEnded { phase } => {
            assert_eq!(phase.as_str(), "build");
        }
        _ => panic!("expected PhaseEnded"),
    }
}

/// Wire-shape mirror (NOT symmetric-on-the-wrong-shape): decode the EXACT
/// JSON bytes a sender emits — `{"PhaseEnded":{"phase":"..."}}` (`PhaseId`
/// is `#[serde(transparent)]`, a plain string on the wire) — rather than
/// re-encoding our own value, so a field reorder/rename that still
/// round-trips against itself is caught against the other side's actual
/// bytes.
#[test]
fn phase_ended_decodes_literal_sender_bytes() {
    let bytes = r#"{"PhaseEnded":{"phase":"unify_vocab"}}"#;
    let decoded: ClusterMutation<TestId> = serde_json::from_str(bytes).unwrap();

    match decoded {
        ClusterMutation::PhaseEnded { phase } => {
            assert_eq!(phase.as_str(), "unify_vocab");
        }
        _ => panic!("expected PhaseEnded"),
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

/// `SecondaryResourceSample` (#575) round-trips through serde with every
/// aggregate field preserved verbatim — the wire shape every observer
/// projection consumes.
#[test]
fn roundtrip_secondary_resource_sample() {
    use crate::SecondaryResourceSampleRecord;

    let record = SecondaryResourceSampleRecord {
        member_gen: 7,
        emitted_at_ms: 1_700_000_000_000,
        mem_p10_bytes: 100 * 1024 * 1024,
        mem_p30_bytes: 200 * 1024 * 1024,
        mem_p50_bytes: 400 * 1024 * 1024,
        mem_p70_bytes: 800 * 1024 * 1024,
        mem_p90_bytes: 1_600 * 1024 * 1024,
        mem_avg_bytes: 512 * 1024 * 1024,
        total_free_memory_bytes: 4 * 1024 * 1024 * 1024,
        total_swap_used_bytes: 256 * 1024 * 1024,
        total_free_swap_bytes: 2 * 1024 * 1024 * 1024,
        cpu_utilization_milli: 65_500,
        // #589 loop-health: exercise the wire-compat path by carrying
        // the present fields too. The legacy round-trip is covered by
        // the separate `roundtrip_secondary_resource_sample_pre_589`
        // test below (a JSON literal omitting these fields must
        // serde-default to zero / empty).
        oploop_iters_per_sec_milli: 12_500,
        dominant_arm_name: "mem_check".to_string(),
        dominant_arm_pct_milli: 55_000,
        dominant_arm_time_ms_per_sec: 425,
        max_unacked_for_secs: 120,
    };
    let mutation: ClusterMutation<TestId> = ClusterMutation::SecondaryResourceSample {
        secondary: "sec-3".into(),
        record,
    };

    let json = serde_json::to_string(&mutation).unwrap();
    let decoded: ClusterMutation<TestId> = serde_json::from_str(&json).unwrap();

    match decoded {
        ClusterMutation::SecondaryResourceSample { secondary, record } => {
            assert_eq!(secondary, "sec-3");
            assert_eq!(record.member_gen, 7);
            assert_eq!(record.emitted_at_ms, 1_700_000_000_000);
            assert_eq!(record.mem_p10_bytes, 100 * 1024 * 1024);
            assert_eq!(record.mem_p30_bytes, 200 * 1024 * 1024);
            assert_eq!(record.mem_p50_bytes, 400 * 1024 * 1024);
            assert_eq!(record.mem_p70_bytes, 800 * 1024 * 1024);
            assert_eq!(record.mem_p90_bytes, 1_600 * 1024 * 1024);
            assert_eq!(record.mem_avg_bytes, 512 * 1024 * 1024);
            assert_eq!(record.total_free_memory_bytes, 4 * 1024 * 1024 * 1024);
            assert_eq!(record.total_swap_used_bytes, 256 * 1024 * 1024);
            assert_eq!(record.total_free_swap_bytes, 2 * 1024 * 1024 * 1024);
            assert_eq!(record.cpu_utilization_milli, 65_500);
            assert_eq!(record.oploop_iters_per_sec_milli, 12_500);
            assert_eq!(record.dominant_arm_name, "mem_check");
            assert_eq!(record.dominant_arm_pct_milli, 55_000);
            assert_eq!(record.dominant_arm_time_ms_per_sec, 425);
            assert_eq!(record.max_unacked_for_secs, 120);
        }
        _ => panic!("expected SecondaryResourceSample"),
    }
}

/// `SecondaryResourceSample` (#589) wire-compat: a pre-#589 record
/// JSON literal omitting the 3 new loop-health fields decodes cleanly
/// into the new struct shape, with each loop-health field at its
/// serde-default (zero / empty). Proves the rolling-upgrade contract:
/// a #589-adopting receiver consuming a still-pre-#589 originator's
/// broadcast NEVER drops the frame, just treats the loop-health axis
/// as "no signal yet" (which the observer's 25%-against-zero gate
/// then suppresses).
#[test]
fn roundtrip_secondary_resource_sample_pre_589() {
    use crate::SecondaryResourceSampleRecord;

    // Hand-written pre-#589 JSON: every #575 field is present, none of
    // the 3 loop-health fields. A serde-default decode MUST yield a
    // valid record with the new fields all zero/empty.
    let json = r#"{
        "member_gen": 3,
        "emitted_at_ms": 1700000000000,
        "mem_p10_bytes": 0,
        "mem_p30_bytes": 0,
        "mem_p50_bytes": 0,
        "mem_p70_bytes": 0,
        "mem_p90_bytes": 0,
        "mem_avg_bytes": 0,
        "total_free_memory_bytes": 0,
        "total_swap_used_bytes": 0,
        "total_free_swap_bytes": 0,
        "cpu_utilization_milli": 0
    }"#;
    let decoded: SecondaryResourceSampleRecord = serde_json::from_str(json).unwrap();
    assert_eq!(decoded.member_gen, 3);
    assert_eq!(decoded.oploop_iters_per_sec_milli, 0);
    assert!(decoded.dominant_arm_name.is_empty());
    assert_eq!(decoded.dominant_arm_pct_milli, 0);
    assert_eq!(decoded.max_unacked_for_secs, 0);
}

/// `TaskRequeued` round-trips through serde with its `hash` preserved
/// (the dead-secondary recovery `InFlight → Pending` mutation carries
/// only the hash; the preserved `TaskInfo` lives on the ledger entry,
/// not the wire).
#[test]
fn roundtrip_task_requeued() {
    let mutation: ClusterMutation<TestId> = ClusterMutation::TaskRequeued {
        hash: "h-requeued".into(),
        version: TaskVersion {
            primary_epoch: 3,
            seq: 7,
        },
    };

    let json = serde_json::to_string(&mutation).unwrap();
    let decoded: ClusterMutation<TestId> = serde_json::from_str(&json).unwrap();

    match decoded {
        ClusterMutation::TaskRequeued { hash, version } => {
            assert_eq!(hash, "h-requeued");
            assert_eq!(
                version,
                TaskVersion {
                    primary_epoch: 3,
                    seq: 7
                }
            );
        }
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
        counts: dynrunner_core::TerminalOutcomeCounts::default(),
    };

    let json = serde_json::to_string(&mutation).unwrap();
    let decoded: ClusterMutation<TestId> = serde_json::from_str(&json).unwrap();

    match decoded {
        ClusterMutation::RunAborted { reason, .. } => {
            assert_eq!(reason, "2 duplicate task identities in the initial batch");
        }
        _ => panic!("expected RunAborted"),
    }
}

/// `GracefulAbortRequested` (the payload-free dispatch-freeze latch, the
/// graceful sibling of `RunComplete`'s wire shape) round-trips through
/// serde — the variant discriminant survives encode→decode so the
/// primary's freeze broadcast reaches every replica.
#[test]
fn roundtrip_graceful_abort_requested() {
    let mutation: ClusterMutation<TestId> = ClusterMutation::GracefulAbortRequested;

    let json = serde_json::to_string(&mutation).unwrap();
    let decoded: ClusterMutation<TestId> = serde_json::from_str(&json).unwrap();

    assert!(matches!(decoded, ClusterMutation::GracefulAbortRequested));
}

/// Wire-shape mirror (NOT symmetric-on-the-wrong-shape): decode the EXACT
/// JSON bytes a sender emits for the payload-free latch — the externally-
/// tagged unit variant is the BARE string `"GracefulAbortRequested"` on
/// the wire (same shape as `RunComplete`) — rather than re-encoding our
/// own value, so a tagging-shape change that still round-trips against
/// itself is caught against the other side's actual bytes.
#[test]
fn graceful_abort_requested_decodes_literal_sender_bytes() {
    let bytes = r#""GracefulAbortRequested""#;
    let decoded: ClusterMutation<TestId> = serde_json::from_str(bytes).unwrap();

    assert!(matches!(decoded, ClusterMutation::GracefulAbortRequested));
}

/// `DiscoveryDebtDeclared` (the payload-free debt-declare latch, twin of
/// `RunComplete`'s wire shape) round-trips through serde — the variant
/// discriminant survives encode→decode so a relocated submitter's debt
/// declaration reaches every replica.
#[test]
fn roundtrip_discovery_debt_declared() {
    let mutation: ClusterMutation<TestId> = ClusterMutation::DiscoveryDebtDeclared;

    let json = serde_json::to_string(&mutation).unwrap();
    let decoded: ClusterMutation<TestId> = serde_json::from_str(&json).unwrap();

    assert!(matches!(decoded, ClusterMutation::DiscoveryDebtDeclared));
}

/// `DiscoverySettled` (the payload-free debt-settle ratchet) round-trips
/// through serde — the discriminant survives encode→decode so the
/// compute-peer primary's settle reaches every replica.
#[test]
fn roundtrip_discovery_settled() {
    let mutation: ClusterMutation<TestId> = ClusterMutation::DiscoverySettled;

    let json = serde_json::to_string(&mutation).unwrap();
    let decoded: ClusterMutation<TestId> = serde_json::from_str(&json).unwrap();

    assert!(matches!(decoded, ClusterMutation::DiscoverySettled));
}

/// `PrimaryChanged` round-trips through serde carrying a NON-DEFAULT
/// `reason: Transferred` — the bootstrap-transfer marker survives
/// encode→decode rather than collapsing to the `Election` default. A
/// default-valued test (`Election`) would pass even if the field were
/// dropped on the wire, so the assertion is pinned on the non-default.
#[test]
fn roundtrip_primary_changed_transferred_reason() {
    let mutation: ClusterMutation<TestId> = ClusterMutation::PrimaryChanged {
        new: "chosen-peer".into(),
        epoch: 2,
        reason: PrimaryChangeReason::Transferred,
    };

    let json = serde_json::to_string(&mutation).unwrap();
    let decoded: ClusterMutation<TestId> = serde_json::from_str(&json).unwrap();

    match decoded {
        ClusterMutation::PrimaryChanged { new, epoch, reason } => {
            assert_eq!(new, "chosen-peer");
            assert_eq!(epoch, 2);
            assert_eq!(reason, PrimaryChangeReason::Transferred);
        }
        _ => panic!("expected PrimaryChanged"),
    }
}

/// Backward-compat: a sender that predates the `reason` field emits a
/// `PrimaryChanged` JSON shape with only `{ new, epoch }`. `#[serde(default)]`
/// must decode it as `reason: Election` (the only shape that existed before),
/// keeping the wire safe under a coordinated/rolling restart.
#[test]
fn legacy_primary_changed_decodes_reason_as_election() {
    let legacy = serde_json::json!({
        "PrimaryChanged": { "new": "legacy-primary", "epoch": 1 }
    });
    let json = legacy.to_string();
    let decoded: ClusterMutation<TestId> = serde_json::from_str(&json).unwrap();

    match decoded {
        ClusterMutation::PrimaryChanged { new, epoch, reason } => {
            assert_eq!(new, "legacy-primary");
            assert_eq!(epoch, 1);
            assert_eq!(reason, PrimaryChangeReason::Election);
        }
        _ => panic!("expected PrimaryChanged"),
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
        attempt: 0,
    };
    let v = serde_json::to_value(&mutation).unwrap();
    let inner = &v["TaskCompleted"];
    assert!(
        inner.get("result_data").is_none(),
        "absent result_data must be omitted on the wire, got: {v}"
    );
    assert_eq!(inner["hash"], "h-bare");
}

/// `TaskFailed` round-trips carrying a NON-DEFAULT `version` — the
/// primary-stamped terminal-payload version survives encode→decode. A
/// default-valued test would pass even if the field were dropped on the
/// wire, so the assertion is pinned on the non-default.
#[test]
fn roundtrip_task_failed_with_version() {
    let mutation: ClusterMutation<TestId> = ClusterMutation::TaskFailed {
        hash: "h-failed".into(),
        kind: ErrorType::NonRecoverable,
        error: "boom".into(),
        version: TaskVersion {
            primary_epoch: 5,
            seq: 2,
        },
        // Pin a NON-DEFAULT attempt (F2): the failure carries the generation
        // it failed under so the retry originator reads it to mint n+1.
        attempt: 2,
    };

    let json = serde_json::to_string(&mutation).unwrap();
    let decoded: ClusterMutation<TestId> = serde_json::from_str(&json).unwrap();

    match decoded {
        ClusterMutation::TaskFailed {
            hash,
            kind,
            error,
            version,
            attempt,
        } => {
            assert_eq!(hash, "h-failed");
            assert_eq!(kind, ErrorType::NonRecoverable);
            assert_eq!(error, "boom");
            assert_eq!(
                version,
                TaskVersion {
                    primary_epoch: 5,
                    seq: 2
                }
            );
            assert_eq!(attempt, 2);
        }
        _ => panic!("expected TaskFailed"),
    }
}

/// Backward-compat: a sender that predates the `version` field (nor the F2
/// `attempt`) emits a `TaskFailed` JSON shape with only `{ hash, kind, error }`.
/// `#[serde(default)]` must decode `version` as the `(0, 0)` strict minimum and
/// `attempt` as the cold generation `0`, so a legacy record never dominates a
/// versioned/attempt-bearing one.
#[test]
fn legacy_task_failed_decodes_version_as_default() {
    let legacy = serde_json::json!({
        "TaskFailed": { "hash": "legacy", "kind": "NonRecoverable", "error": "e" }
    });
    let json = legacy.to_string();
    let decoded: ClusterMutation<TestId> = serde_json::from_str(&json).unwrap();

    match decoded {
        ClusterMutation::TaskFailed {
            version, attempt, ..
        } => {
            assert_eq!(version, TaskVersion::default());
            assert_eq!(attempt, 0);
        }
        _ => panic!("expected TaskFailed"),
    }
}

/// `TaskAssigned` round-trips carrying a NON-DEFAULT `version` (the
/// assignment-lifecycle version that lets a stale assignment lose to a
/// higher-version reset).
#[test]
fn roundtrip_task_assigned_with_version() {
    let mutation: ClusterMutation<TestId> = ClusterMutation::TaskAssigned {
        hash: "h-assigned".into(),
        secondary: "sec-1".into(),
        worker: 4,
        version: TaskVersion {
            primary_epoch: 1,
            seq: 9,
        },
        // Pin a NON-DEFAULT attempt (F2): the assignment carries the retried
        // generation so a worker outcome out-ranks the reset Pending.
        attempt: 5,
    };

    let json = serde_json::to_string(&mutation).unwrap();
    let decoded: ClusterMutation<TestId> = serde_json::from_str(&json).unwrap();

    match decoded {
        ClusterMutation::TaskAssigned {
            hash,
            secondary,
            worker,
            version,
            attempt,
        } => {
            assert_eq!(hash, "h-assigned");
            assert_eq!(secondary, "sec-1");
            assert_eq!(worker, 4);
            assert_eq!(
                version,
                TaskVersion {
                    primary_epoch: 1,
                    seq: 9
                }
            );
            assert_eq!(attempt, 5);
        }
        _ => panic!("expected TaskAssigned"),
    }
}

/// `TaskPreferredSecondariesUpdated` round-trips carrying a NON-DEFAULT
/// `version` (the preferred-metadata version, mirrored onto
/// `TaskInfo.preferred_version`).
#[test]
fn roundtrip_preferred_updated_with_version() {
    let mutation: ClusterMutation<TestId> = ClusterMutation::TaskPreferredSecondariesUpdated {
        hash: "h-pref".into(),
        secondaries: vec!["a".into(), "b".into()],
        version: TaskVersion {
            primary_epoch: 2,
            seq: 1,
        },
    };

    let json = serde_json::to_string(&mutation).unwrap();
    let decoded: ClusterMutation<TestId> = serde_json::from_str(&json).unwrap();

    match decoded {
        ClusterMutation::TaskPreferredSecondariesUpdated {
            hash,
            secondaries,
            version,
        } => {
            assert_eq!(hash, "h-pref");
            assert_eq!(secondaries, vec!["a".to_string(), "b".to_string()]);
            assert_eq!(
                version,
                TaskVersion {
                    primary_epoch: 2,
                    seq: 1
                }
            );
        }
        _ => panic!("expected TaskPreferredSecondariesUpdated"),
    }
}

/// `TaskReinjected` round-trips carrying a NON-DEFAULT reset `version`.
#[test]
fn roundtrip_task_reinjected_with_version() {
    let mutation: ClusterMutation<TestId> = ClusterMutation::TaskReinjected {
        hash: "h-reinj".into(),
        version: TaskVersion {
            primary_epoch: 4,
            seq: 6,
        },
    };

    let json = serde_json::to_string(&mutation).unwrap();
    let decoded: ClusterMutation<TestId> = serde_json::from_str(&json).unwrap();

    match decoded {
        ClusterMutation::TaskReinjected { hash, version } => {
            assert_eq!(hash, "h-reinj");
            assert_eq!(
                version,
                TaskVersion {
                    primary_epoch: 4,
                    seq: 6
                }
            );
        }
        _ => panic!("expected TaskReinjected"),
    }
}

/// `TaskRetried` round-trips carrying a NON-DEFAULT `attempt` AND `version`
/// (F2 — the per-phase retry reset `Failed { attempt: n } → Pending { attempt:
/// n+1 }`). The originator-computed `attempt` is the TOP of the join key, so it
/// MUST survive the wire; a default-valued test would pass even if the field
/// were dropped, hence both fields are pinned non-default.
#[test]
fn roundtrip_task_retried_with_attempt_and_version() {
    let mutation: ClusterMutation<TestId> = ClusterMutation::TaskRetried {
        hash: "h-retried".into(),
        attempt: 4,
        version: TaskVersion {
            primary_epoch: 6,
            seq: 8,
        },
    };

    let json = serde_json::to_string(&mutation).unwrap();
    let decoded: ClusterMutation<TestId> = serde_json::from_str(&json).unwrap();

    match decoded {
        ClusterMutation::TaskRetried {
            hash,
            attempt,
            version,
        } => {
            assert_eq!(hash, "h-retried");
            assert_eq!(attempt, 4);
            assert_eq!(
                version,
                TaskVersion {
                    primary_epoch: 6,
                    seq: 8
                }
            );
        }
        _ => panic!("expected TaskRetried"),
    }
}

/// Backward-compat: a `TaskRetried` from a sender that predates either field
/// (only `{ hash }`) decodes `attempt` as the cold generation `0` and `version`
/// as the `(0, 0)` strict minimum.
///
/// NOTE: this legacy decode path is in practice UNREACHABLE — `TaskRetried` is
/// a NEW variant (F2), so no sender predating its `attempt`/`version` fields
/// ever existed; an originator always mints `attempt: n+1`. The
/// `#[serde(default)]` and this test pin the field-defaulting purely as a
/// safety net so a malformed/truncated frame degrades to the cold generation
/// rather than refusing to decode.
#[test]
fn legacy_task_retried_decodes_attempt_and_version_as_default() {
    let legacy = serde_json::json!({
        "TaskRetried": { "hash": "h" }
    });
    let decoded: ClusterMutation<TestId> = serde_json::from_str(&legacy.to_string()).unwrap();

    match decoded {
        ClusterMutation::TaskRetried {
            hash,
            attempt,
            version,
        } => {
            assert_eq!(hash, "h");
            assert_eq!(attempt, 0);
            assert_eq!(version, TaskVersion::default());
        }
        _ => panic!("expected TaskRetried"),
    }
}

/// Backward-compat: a `TaskAssigned` from a sender that predates the F2
/// `attempt` (only `{ hash, secondary, worker }`, optionally `version`) decodes
/// `attempt` as the cold generation `0`.
#[test]
fn legacy_task_assigned_decodes_attempt_as_default() {
    let legacy = serde_json::json!({
        "TaskAssigned": { "hash": "h", "secondary": "sec", "worker": 1 }
    });
    let decoded: ClusterMutation<TestId> = serde_json::from_str(&legacy.to_string()).unwrap();

    match decoded {
        ClusterMutation::TaskAssigned { attempt, .. } => {
            assert_eq!(attempt, 0);
        }
        _ => panic!("expected TaskAssigned"),
    }
}

/// `PeerJoined` round-trips carrying a NON-DEFAULT `cap_version` (C6 — the
/// capability version that arbitrates a `can_be_primary` flip-back).
#[test]
fn roundtrip_peer_joined_with_cap_version() {
    let mutation: ClusterMutation<TestId> = ClusterMutation::PeerJoined {
        peer_id: "compute-1".into(),
        is_observer: false,
        can_be_primary: true,
        cap_version: TaskVersion {
            primary_epoch: 7,
            seq: 3,
        },
        // Pin a NON-DEFAULT generation so the assertion catches a
        // dropped `member_gen` on the wire (the re-admission lattice).
        member_gen: 2,
    };

    let json = serde_json::to_string(&mutation).unwrap();
    let decoded: ClusterMutation<TestId> = serde_json::from_str(&json).unwrap();

    match decoded {
        ClusterMutation::PeerJoined {
            peer_id,
            is_observer,
            can_be_primary,
            cap_version,
            member_gen,
        } => {
            assert_eq!(peer_id, "compute-1");
            assert!(!is_observer);
            assert!(can_be_primary);
            assert_eq!(
                cap_version,
                TaskVersion {
                    primary_epoch: 7,
                    seq: 3
                }
            );
            assert_eq!(member_gen, 2);
        }
        _ => panic!("expected PeerJoined"),
    }
}

/// Backward-compat: a sender that predates the `cap_version` field emits a
/// `PeerJoined` with only `{ peer_id, is_observer, can_be_primary }` (or
/// even without `can_be_primary`). `#[serde(default)]` must decode
/// `cap_version` as the `(0, 0)` strict minimum, so a legacy re-emit loses
/// to any stamped version and never regresses a converged capability.
#[test]
fn legacy_peer_joined_decodes_cap_version_as_default() {
    let legacy = serde_json::json!({
        "PeerJoined": { "peer_id": "legacy-peer", "is_observer": true }
    });
    let decoded: ClusterMutation<TestId> = serde_json::from_str(&legacy.to_string()).unwrap();

    match decoded {
        ClusterMutation::PeerJoined {
            peer_id,
            is_observer,
            can_be_primary,
            cap_version,
            member_gen,
        } => {
            assert_eq!(peer_id, "legacy-peer");
            assert!(is_observer);
            // can_be_primary also serde(default) → false.
            assert!(!can_be_primary);
            assert_eq!(cap_version, TaskVersion::default());
            // member_gen serde(default) → 0, the pre-generation cold
            // sticky semantics.
            assert_eq!(member_gen, 0);
        }
        _ => panic!("expected PeerJoined"),
    }
}

/// `SetCanBePrimary` round-trips carrying a NON-DEFAULT `cap_version`.
#[test]
fn roundtrip_set_can_be_primary_with_cap_version() {
    let mutation: ClusterMutation<TestId> = ClusterMutation::SetCanBePrimary {
        peer_id: "p".into(),
        can_be_primary: false,
        cap_version: TaskVersion {
            primary_epoch: 4,
            seq: 9,
        },
    };

    let json = serde_json::to_string(&mutation).unwrap();
    let decoded: ClusterMutation<TestId> = serde_json::from_str(&json).unwrap();

    match decoded {
        ClusterMutation::SetCanBePrimary {
            peer_id,
            can_be_primary,
            cap_version,
        } => {
            assert_eq!(peer_id, "p");
            assert!(!can_be_primary);
            assert_eq!(
                cap_version,
                TaskVersion {
                    primary_epoch: 4,
                    seq: 9
                }
            );
        }
        _ => panic!("expected SetCanBePrimary"),
    }
}

/// Backward-compat: a `SetCanBePrimary` from a pre-`cap_version` sender
/// (only `{ peer_id, can_be_primary }`) decodes `cap_version` as `(0, 0)`.
#[test]
fn legacy_set_can_be_primary_decodes_cap_version_as_default() {
    let legacy = serde_json::json!({
        "SetCanBePrimary": { "peer_id": "p", "can_be_primary": true }
    });
    let decoded: ClusterMutation<TestId> = serde_json::from_str(&legacy.to_string()).unwrap();

    match decoded {
        ClusterMutation::SetCanBePrimary {
            peer_id,
            can_be_primary,
            cap_version,
        } => {
            assert_eq!(peer_id, "p");
            assert!(can_be_primary);
            assert_eq!(cap_version, TaskVersion::default());
        }
        _ => panic!("expected SetCanBePrimary"),
    }
}

/// Backward-compat: a sender that predates the reset `version` field emits
/// `TaskRequeued` / `TaskReinjected` with only `{ hash }`; the field must
/// decode to the `(0, 0)` strict minimum.
#[test]
fn legacy_reset_mutations_decode_version_as_default() {
    for (key, wrap) in [
        ("TaskRequeued", "TaskRequeued"),
        ("TaskReinjected", "TaskReinjected"),
    ] {
        let legacy = serde_json::json!({ wrap: { "hash": "h" } });
        let decoded: ClusterMutation<TestId> = serde_json::from_str(&legacy.to_string()).unwrap();
        let version = match decoded {
            ClusterMutation::TaskRequeued { version, .. } => version,
            ClusterMutation::TaskReinjected { version, .. } => version,
            _ => panic!("expected {key}"),
        };
        assert_eq!(
            version,
            TaskVersion::default(),
            "{key} legacy version default"
        );
    }
}

/// F5 `CustomMessagePosted` round-trips with the full `(origin, seq)`
/// key + the opaque payload preserved verbatim.
#[test]
fn roundtrip_custom_message_posted() {
    let mutation: ClusterMutation<TestId> = ClusterMutation::CustomMessagePosted {
        origin: "sec-1".into(),
        seq: 11,
        topic: "phase4-batch".into(),
        data: b"batch payload".to_vec(),
        // #583/#587: an explicit-true on the originator round-trips.
        is_high_volume: true,
    };
    let json = serde_json::to_string(&mutation).unwrap();
    let decoded: ClusterMutation<TestId> = serde_json::from_str(&json).unwrap();
    match decoded {
        ClusterMutation::CustomMessagePosted {
            origin,
            seq,
            topic,
            data,
            is_high_volume,
        } => {
            assert_eq!(origin, "sec-1");
            assert_eq!(seq, 11);
            assert_eq!(topic, "phase4-batch");
            assert_eq!(data, b"batch payload".to_vec());
            assert!(is_high_volume, "#583/#587: explicit-true survives the wire");
        }
        _ => panic!("expected CustomMessagePosted"),
    }
}

/// F5 `CustomMessageHandled` round-trips with its `(origin, seq)` key.
#[test]
fn roundtrip_custom_message_handled() {
    let mutation: ClusterMutation<TestId> = ClusterMutation::CustomMessageHandled {
        origin: "sec-1".into(),
        seq: 11,
    };
    let json = serde_json::to_string(&mutation).unwrap();
    let decoded: ClusterMutation<TestId> = serde_json::from_str(&json).unwrap();
    match decoded {
        ClusterMutation::CustomMessageHandled { origin, seq } => {
            assert_eq!(origin, "sec-1");
            assert_eq!(seq, 11);
        }
        _ => panic!("expected CustomMessageHandled"),
    }
}

/// F5 `CustomMessageFailed` round-trips with its `(origin, seq, reason)`
/// triple (the terminal twin of `CustomMessageHandled` — a handler raise).
/// The `reason` field (#570 — narration-only plumbing for the
/// CustomMessageOutcomeEvent, never CRDT state) survives the wire
/// verbatim when non-empty.
#[test]
fn roundtrip_custom_message_failed() {
    let mutation: ClusterMutation<TestId> = ClusterMutation::CustomMessageFailed {
        origin: "sec-1".into(),
        seq: 11,
        reason: "handler raised: bad config".into(),
    };
    let json = serde_json::to_string(&mutation).unwrap();
    let decoded: ClusterMutation<TestId> = serde_json::from_str(&json).unwrap();
    match decoded {
        ClusterMutation::CustomMessageFailed {
            origin,
            seq,
            reason,
        } => {
            assert_eq!(origin, "sec-1");
            assert_eq!(seq, 11);
            assert_eq!(reason, "handler raised: bad config");
        }
        _ => panic!("expected CustomMessageFailed"),
    }
}

/// F5 `CustomMessageFailed` with an empty `reason` is wire-LEGACY-shaped
/// — `#[serde(default, skip_serializing_if = "String::is_empty")]` drops
/// the field on encode and defaults it on decode — so a legacy sender's
/// reason-less frame still decodes (back-compat), and a current sender
/// with no reason emits the same legacy bytes (forward-compat).
#[test]
fn roundtrip_custom_message_failed_empty_reason_is_legacy_shape() {
    // Current sender's empty-reason encoding drops the field.
    let mutation: ClusterMutation<TestId> = ClusterMutation::CustomMessageFailed {
        origin: "sec-1".into(),
        seq: 11,
        reason: String::new(),
    };
    let json = serde_json::to_string(&mutation).unwrap();
    assert_eq!(
        json, r#"{"CustomMessageFailed":{"origin":"sec-1","seq":11}}"#,
        "an empty reason is dropped on encode (skip_serializing_if)"
    );
    // Legacy sender's reason-less frame decodes to empty reason
    // (#[serde(default)]).
    let legacy = r#"{"CustomMessageFailed":{"origin":"sec-1","seq":11}}"#;
    let decoded: ClusterMutation<TestId> = serde_json::from_str(legacy).unwrap();
    match decoded {
        ClusterMutation::CustomMessageFailed {
            origin,
            seq,
            reason,
        } => {
            assert_eq!((origin.as_str(), seq), ("sec-1", 11));
            assert_eq!(reason, "", "legacy reason-less frame decodes to empty");
        }
        _ => panic!("expected CustomMessageFailed"),
    }
}

/// Literal-bytes pins for the F5 mutations (the externally-tagged
/// `ClusterMutation` shape every current originator emits). Pinning the
/// sender bytes catches a tag / field-name divergence a symmetric
/// round-trip cannot see.
#[test]
fn custom_message_mutations_decode_literal_sender_bytes() {
    let posted = r#"{"CustomMessagePosted":{"origin":"sec-1","seq":3,"topic":"t","data":[1,2]}}"#;
    let decoded: ClusterMutation<TestId> = serde_json::from_str(posted).unwrap();
    match decoded {
        ClusterMutation::CustomMessagePosted {
            origin,
            seq,
            topic,
            data,
            is_high_volume,
        } => {
            assert_eq!((origin.as_str(), seq, topic.as_str()), ("sec-1", 3, "t"));
            assert_eq!(data, vec![1, 2]);
            assert!(
                !is_high_volume,
                "#583/#587: legacy bytes (no is_high_volume field) decode to false (skip_serializing_if default)"
            );
        }
        _ => panic!("expected CustomMessagePosted"),
    }
    let handled = r#"{"CustomMessageHandled":{"origin":"sec-1","seq":3}}"#;
    let decoded: ClusterMutation<TestId> = serde_json::from_str(handled).unwrap();
    match decoded {
        ClusterMutation::CustomMessageHandled { origin, seq } => {
            assert_eq!((origin.as_str(), seq), ("sec-1", 3));
        }
        _ => panic!("expected CustomMessageHandled"),
    }
    // Legacy reason-less bytes (the pre-#570 wire shape) still decode —
    // the `#[serde(default)]` defaults the reason to empty.
    let failed = r#"{"CustomMessageFailed":{"origin":"sec-1","seq":3}}"#;
    let decoded: ClusterMutation<TestId> = serde_json::from_str(failed).unwrap();
    match decoded {
        ClusterMutation::CustomMessageFailed {
            origin,
            seq,
            reason,
        } => {
            assert_eq!((origin.as_str(), seq), ("sec-1", 3));
            assert_eq!(reason, "", "legacy reason-less frame decodes to empty");
        }
        _ => panic!("expected CustomMessageFailed"),
    }
    // Current-shape bytes with a populated reason decode verbatim — the
    // narration-only plumbing surface for the #570 event channel.
    let failed_with_reason =
        r#"{"CustomMessageFailed":{"origin":"sec-1","seq":4,"reason":"boom"}}"#;
    let decoded: ClusterMutation<TestId> = serde_json::from_str(failed_with_reason).unwrap();
    match decoded {
        ClusterMutation::CustomMessageFailed {
            origin,
            seq,
            reason,
        } => {
            assert_eq!((origin.as_str(), seq, reason.as_str()), ("sec-1", 4, "boom"));
        }
        _ => panic!("expected CustomMessageFailed"),
    }
    // #583/#587 wire-shape pin: a CustomMessagePosted with
    // is_high_volume=true round-trips with the field PRESENT on the
    // wire; the false default is dropped (skip_serializing_if).
    let posted_hv: ClusterMutation<TestId> = ClusterMutation::CustomMessagePosted {
        origin: "sec-1".into(),
        seq: 5,
        topic: "t".into(),
        data: vec![1, 2],
        is_high_volume: true,
    };
    let hv_json = serde_json::to_string(&posted_hv).unwrap();
    assert_eq!(
        hv_json,
        r#"{"CustomMessagePosted":{"origin":"sec-1","seq":5,"topic":"t","data":[1,2],"is_high_volume":true}}"#,
        "is_high_volume=true rides the wire as a present field"
    );
    let posted_lv: ClusterMutation<TestId> = ClusterMutation::CustomMessagePosted {
        origin: "sec-1".into(),
        seq: 6,
        topic: "t".into(),
        data: vec![1, 2],
        is_high_volume: false,
    };
    let lv_json = serde_json::to_string(&posted_lv).unwrap();
    assert_eq!(
        lv_json,
        r#"{"CustomMessagePosted":{"origin":"sec-1","seq":6,"topic":"t","data":[1,2]}}"#,
        "is_high_volume=false is dropped (wire-byte-identical to a legacy sender)"
    );
}

/// `PeerRemoved` round-trips carrying a NON-DEFAULT `member_gen` (the
/// re-admission lattice: the removal kills ONE membership incarnation,
/// so the generation must survive the wire or a stale removal could
/// re-bury a re-admitted live peer).
#[test]
fn roundtrip_peer_removed_with_member_gen() {
    let mutation: ClusterMutation<TestId> = ClusterMutation::PeerRemoved {
        id: "secondary-2".into(),
        cause: RemovalCause::KeepaliveMiss,
        member_gen: 3,
    };

    let json = serde_json::to_string(&mutation).unwrap();
    let decoded: ClusterMutation<TestId> = serde_json::from_str(&json).unwrap();

    match decoded {
        ClusterMutation::PeerRemoved {
            id,
            cause,
            member_gen,
        } => {
            assert_eq!(id, "secondary-2");
            assert_eq!(cause, RemovalCause::KeepaliveMiss);
            assert_eq!(member_gen, 3);
        }
        _ => panic!("expected PeerRemoved"),
    }
}

/// Wire-shape mirror (NOT symmetric-on-the-wrong-shape): decode the EXACT
/// JSON bytes a generation-stamping sender emits —
/// `{"PeerRemoved":{"id":"...","cause":"KeepaliveMiss","member_gen":1}}` —
/// rather than re-encoding our own value, so a field reorder/rename that
/// still round-trips against itself is caught against the other side's
/// actual bytes.
#[test]
fn peer_removed_decodes_literal_sender_bytes() {
    let bytes = r#"{"PeerRemoved":{"id":"secondary-1","cause":"KeepaliveMiss","member_gen":1}}"#;
    let decoded: ClusterMutation<TestId> = serde_json::from_str(bytes).unwrap();

    match decoded {
        ClusterMutation::PeerRemoved {
            id,
            cause,
            member_gen,
        } => {
            assert_eq!(id, "secondary-1");
            assert_eq!(cause, RemovalCause::KeepaliveMiss);
            assert_eq!(member_gen, 1);
        }
        _ => panic!("expected PeerRemoved"),
    }
}

/// Backward-compat: a sender that predates the `member_gen` field emits a
/// `PeerRemoved` with only `{ id, cause }`. `#[serde(default)]` must
/// decode `member_gen` as 0 — the pre-generation sticky semantics.
#[test]
fn legacy_peer_removed_decodes_member_gen_as_default() {
    let bytes = r#"{"PeerRemoved":{"id":"old-peer","cause":"KeepaliveMiss"}}"#;
    let decoded: ClusterMutation<TestId> = serde_json::from_str(bytes).unwrap();

    match decoded {
        ClusterMutation::PeerRemoved {
            id,
            cause,
            member_gen,
        } => {
            assert_eq!(id, "old-peer");
            assert_eq!(cause, RemovalCause::KeepaliveMiss);
            assert_eq!(member_gen, 0);
        }
        _ => panic!("expected PeerRemoved"),
    }
}

/// Wire-shape mirror for the RE-ADMISSION `PeerJoined` (the headline
/// frame of the membership-readmission fix): decode the EXACT JSON
/// bytes the primary's frame-ingest re-admission seam emits — a join at
/// `member_gen: 1` superseding a generation-0 removal — against the
/// other side's actual bytes.
#[test]
fn readmission_peer_joined_decodes_literal_sender_bytes() {
    let bytes = r#"{"PeerJoined":{"peer_id":"secondary-2","is_observer":false,"can_be_primary":true,"cap_version":{"primary_epoch":1,"seq":4},"member_gen":1}}"#;
    let decoded: ClusterMutation<TestId> = serde_json::from_str(bytes).unwrap();

    match decoded {
        ClusterMutation::PeerJoined {
            peer_id,
            is_observer,
            can_be_primary,
            cap_version,
            member_gen,
        } => {
            assert_eq!(peer_id, "secondary-2");
            assert!(!is_observer);
            assert!(can_be_primary);
            assert_eq!(
                cap_version,
                TaskVersion {
                    primary_epoch: 1,
                    seq: 4
                }
            );
            assert_eq!(member_gen, 1);
        }
        _ => panic!("expected PeerJoined"),
    }
}

/// Wire-shape mirror for the replicated respawn caps: decode the EXACT
/// JSON bytes a submitter primary's seed batch carries for a
/// `RespawnPolicySet` (externally tagged enum, integer `cooldown_ms`),
/// and pin the sender-side encoding against the same bytes — the caps
/// are what a promoted primary re-arms its respawn decision from, so a
/// silent shape drift would re-open the inert-after-relocation hole.
#[test]
fn respawn_policy_set_mirrors_wire_bytes() {
    let wire = r#"{"RespawnPolicySet":{"max_per_secondary":3,"max_total":10,"cooldown_ms":30000}}"#;
    let decoded: ClusterMutation<TestId> = serde_json::from_str(wire).unwrap();
    match decoded {
        ClusterMutation::RespawnPolicySet {
            max_per_secondary,
            max_total,
            cooldown_ms,
        } => {
            assert_eq!(max_per_secondary, 3);
            assert_eq!(max_total, 10);
            assert_eq!(cooldown_ms, 30_000);
        }
        _ => panic!("expected RespawnPolicySet"),
    }
    let encoded: ClusterMutation<TestId> = ClusterMutation::RespawnPolicySet {
        max_per_secondary: 3,
        max_total: 10,
        cooldown_ms: 30_000,
    };
    assert_eq!(serde_json::to_string(&encoded).unwrap(), wire);
}

/// `RunComplete` (#513) round-trips with its carried `TerminalOutcomeCounts`
/// preserved — the verdict's finalized per-class partition the primary
/// stamps and the observer narrates from. A non-default `fail_final` is
/// pinned so the assertion catches a dropped count field on the wire.
#[test]
fn roundtrip_run_complete_with_counts() {
    let mutation: ClusterMutation<TestId> = ClusterMutation::RunComplete {
        counts: dynrunner_core::TerminalOutcomeCounts {
            succeeded: 2,
            fail_retry: 0,
            fail_oom: 0,
            fail_final: 538,
            skipped: 0,
            setup_succeeded: 1,
        },
    };
    let json = serde_json::to_string(&mutation).unwrap();
    let decoded: ClusterMutation<TestId> = serde_json::from_str(&json).unwrap();
    match decoded {
        ClusterMutation::RunComplete { counts } => {
            assert_eq!(counts.succeeded, 2);
            assert_eq!(counts.fail_final, 538);
            assert_eq!(counts.setup_succeeded, 1);
        }
        _ => panic!("expected RunComplete"),
    }
}

/// `RunAborted` (#513) round-trips with BOTH its `reason` and its carried
/// `TerminalOutcomeCounts` preserved.
#[test]
fn roundtrip_run_aborted_with_reason_and_counts() {
    let mutation: ClusterMutation<TestId> = ClusterMutation::RunAborted {
        reason: "cluster routing collapsed".into(),
        counts: dynrunner_core::TerminalOutcomeCounts {
            succeeded: 7,
            fail_final: 3,
            ..Default::default()
        },
    };
    let json = serde_json::to_string(&mutation).unwrap();
    let decoded: ClusterMutation<TestId> = serde_json::from_str(&json).unwrap();
    match decoded {
        ClusterMutation::RunAborted { reason, counts } => {
            assert_eq!(reason, "cluster routing collapsed");
            assert_eq!(counts.succeeded, 7);
            assert_eq!(counts.fail_final, 3);
        }
        _ => panic!("expected RunAborted"),
    }
}

/// Wire-shape mirror (NOT symmetric-on-the-wrong-shape): decode the EXACT
/// externally-tagged JSON bytes the primary's terminal-verdict broadcast
/// emits for `RunComplete` — `{"RunComplete":{"counts":{...all 7 buckets...}}}`
/// — pinning the literal shape the OTHER side must produce/consume. A
/// renamed/reordered count field or a changed enum tag fails HERE even if the
/// crate's own encode/decode stay self-consistent.
#[test]
fn run_complete_decodes_literal_sender_bytes() {
    let bytes = r#"{"RunComplete":{"counts":{"succeeded":2,"fail_retry":0,"fail_oom":0,"fail_final":538,"skipped":0,"setup_succeeded":1}}}"#;
    let decoded: ClusterMutation<TestId> = serde_json::from_str(bytes).unwrap();
    match decoded {
        ClusterMutation::RunComplete { counts } => {
            assert_eq!(counts.succeeded, 2);
            assert_eq!(counts.fail_final, 538);
            assert_eq!(counts.setup_succeeded, 1);
        }
        _ => panic!("expected RunComplete"),
    }
    // The OTHER direction of the mirror: the crate's encoder produces EXACTLY
    // these bytes (field order is the struct declaration order), so a sender
    // on this crate and a decoder expecting the literal above agree verbatim.
    let encoded: ClusterMutation<TestId> = ClusterMutation::RunComplete {
        counts: dynrunner_core::TerminalOutcomeCounts {
            succeeded: 2,
            fail_retry: 0,
            fail_oom: 0,
            fail_final: 538,
            skipped: 0,
            setup_succeeded: 1,
        },
    };
    assert_eq!(serde_json::to_string(&encoded).unwrap(), bytes);
}

/// Wire-shape mirror for `RunAborted` — the EXACT bytes the routing-collapse
/// abort emits: `{"RunAborted":{"reason":"...","counts":{...}}}`.
#[test]
fn run_aborted_decodes_literal_sender_bytes() {
    let bytes = r#"{"RunAborted":{"reason":"collapsed","counts":{"succeeded":7,"fail_retry":0,"fail_oom":0,"fail_final":3,"skipped":0,"setup_succeeded":0}}}"#;
    let decoded: ClusterMutation<TestId> = serde_json::from_str(bytes).unwrap();
    match decoded {
        ClusterMutation::RunAborted { reason, counts } => {
            assert_eq!(reason, "collapsed");
            assert_eq!(counts.succeeded, 7);
            assert_eq!(counts.fail_final, 3);
        }
        _ => panic!("expected RunAborted"),
    }
    let encoded: ClusterMutation<TestId> = ClusterMutation::RunAborted {
        reason: "collapsed".into(),
        counts: dynrunner_core::TerminalOutcomeCounts {
            succeeded: 7,
            fail_final: 3,
            ..Default::default()
        },
    };
    assert_eq!(serde_json::to_string(&encoded).unwrap(), bytes);
}

/// Back-compat: a PRE-#513 sender's `RunComplete` / `RunAborted` bytes carry
/// NO `counts` field. `#[serde(default)]` is NOT on the enum-variant struct
/// fields (externally-tagged variants take the variant body verbatim), so a
/// missing `counts` would REFUSE the frame — this test PINS that the carried
/// shape is the agreed wire contract going forward. A rolling upgrade across
/// this field is a coordinated cut (both crates ship together — same wheel),
/// so the strict shape is intentional; this test documents + guards it.
#[test]
fn run_complete_literal_carries_counts_object() {
    // The encoder always emits the `counts` object (no flatten / skip), so a
    // decoder can rely on its presence — the contract the narrator's
    // carried-count read depends on.
    let encoded: ClusterMutation<TestId> = ClusterMutation::RunComplete {
        counts: dynrunner_core::TerminalOutcomeCounts::default(),
    };
    let json = serde_json::to_string(&encoded).unwrap();
    assert!(
        json.contains("\"counts\""),
        "RunComplete must serialize a counts object: {json}"
    );
}

// ── AF-id: affine state-layer mutations ──

/// `SecondaryCellRegistered` round-trips with its `hash` + cell-id (the
/// content→cell-id binding, the per-secondary bitvector cell index).
#[test]
fn roundtrip_secondary_cell_registered() {
    let mutation: ClusterMutation<TestId> = ClusterMutation::SecondaryCellRegistered {
        hash: "h-affine".into(),
        cell_id: 11,
    };
    let json = serde_json::to_string(&mutation).unwrap();
    let decoded: ClusterMutation<TestId> = serde_json::from_str(&json).unwrap();
    match decoded {
        ClusterMutation::SecondaryCellRegistered { hash, cell_id } => {
            assert_eq!(hash, "h-affine");
            assert_eq!(cell_id, 11);
        }
        _ => panic!("expected SecondaryCellRegistered"),
    }
}

/// The four per-secondary bitvector CELL mutations round-trip with their
/// `(secondary, cell_id, generation)` preserved — pinning a dropped
/// `generation` (the per-cell LWW stamp) on the wire.
#[test]
fn roundtrip_secondary_cell_mutations() {
    let cells: Vec<ClusterMutation<TestId>> = vec![
        ClusterMutation::SecondaryCellFinished {
            secondary: "s1".into(),
            cell_id: 3,
            generation: 7,
        },
        ClusterMutation::SecondaryCellQueued {
            secondary: "s2".into(),
            cell_id: 4,
            generation: 8,
        },
        ClusterMutation::SecondaryCellFailed {
            secondary: "s3".into(),
            cell_id: 5,
            generation: 9,
        },
        ClusterMutation::SecondaryCellUnqueued {
            secondary: "s4".into(),
            cell_id: 6,
            generation: 10,
        },
    ];
    for m in cells {
        let json = serde_json::to_string(&m).unwrap();
        let decoded: ClusterMutation<TestId> = serde_json::from_str(&json).unwrap();
        match (m, decoded) {
            (
                ClusterMutation::SecondaryCellFinished { secondary: a, cell_id: b, generation: c },
                ClusterMutation::SecondaryCellFinished { secondary: x, cell_id: y, generation: z },
            )
            | (
                ClusterMutation::SecondaryCellQueued { secondary: a, cell_id: b, generation: c },
                ClusterMutation::SecondaryCellQueued { secondary: x, cell_id: y, generation: z },
            )
            | (
                ClusterMutation::SecondaryCellFailed { secondary: a, cell_id: b, generation: c },
                ClusterMutation::SecondaryCellFailed { secondary: x, cell_id: y, generation: z },
            )
            | (
                ClusterMutation::SecondaryCellUnqueued { secondary: a, cell_id: b, generation: c },
                ClusterMutation::SecondaryCellUnqueued { secondary: x, cell_id: y, generation: z },
            ) => {
                assert_eq!((a, b, c), (x, y, z));
            }
            _ => panic!("cell mutation variant changed across round-trip"),
        }
    }
}

/// Wire-shape mirror: decode the EXACT externally-tagged bytes the originator
/// emits for a queued-cell mutation, pinning the shape the other side produces.
#[test]
fn secondary_affine_queued_decodes_literal_bytes() {
    let bytes = r#"{"SecondaryCellQueued":{"secondary":"node-7","cell_id":2,"generation":5}}"#;
    let decoded: ClusterMutation<TestId> = serde_json::from_str(bytes).unwrap();
    match decoded {
        ClusterMutation::SecondaryCellQueued {
            secondary,
            cell_id,
            generation,
        } => {
            assert_eq!(secondary, "node-7");
            assert_eq!(cell_id, 2);
            assert_eq!(generation, 5);
        }
        _ => panic!("expected SecondaryCellQueued"),
    }
}

// ── Recompose v2: TasksSpawned def-id stamping (the runtime-spawned twin of
// TaskAdded's def_id) ──

/// `TasksSpawned` round-trips its PER-TASK, primary-allocated `def_ids`
/// (recompose v2) — the parallel `Vec<Option<u32>>` aligned by position to
/// `tasks` so every replica interns each spawned def under the SAME id. Pins
/// NON-default ids (and a NON-default per-edge `TaskDep.def_id`) so a dropped
/// id-carrier fails the assertion. Mirrors the `roundtrip_task_added_carries_def_id`
/// contract for the batch shape.
#[test]
fn roundtrip_tasks_spawned_carries_def_ids() {
    let json = serde_json::json!({
        "TasksSpawned": {
            "tasks": [
                {
                    "path": "/tasks/build_common_dep",
                    "size": 0,
                    "identifier": test_id("build_common_dep"),
                    "phase_id": "BUILD",
                    "type_id": "t0",
                    "affinity_id": null,
                    "payload": null,
                    "task_id": "build_common_dep",
                    // The per-edge dep ids already ride TaskDep.def_id (the
                    // same L5 carrier TaskAdded uses) — pin a non-default one.
                    "task_depends_on": [
                        { "task_id": "import_common", "phase_id": "BUILD", "def_id": 0 }
                    ],
                },
                {
                    "path": "/tasks/build_variant__a",
                    "size": 0,
                    "identifier": test_id("build_variant__a"),
                    "phase_id": "BUILD",
                    "type_id": "t0",
                    "affinity_id": null,
                    "payload": null,
                    "task_id": "build_variant__a",
                },
            ],
            "def_ids": [50, 51],
        }
    })
    .to_string();
    let decoded: ClusterMutation<TestId> = serde_json::from_str(&json).unwrap();
    match decoded {
        ClusterMutation::TasksSpawned { tasks, def_ids } => {
            assert_eq!(tasks.len(), 2);
            assert_eq!(def_ids, vec![Some(50), Some(51)]);
            assert_eq!(tasks[0].task_id, "build_common_dep");
            assert_eq!(tasks[0].task_depends_on[0].def_id, Some(0));
        }
        _ => panic!("expected TasksSpawned"),
    }
}

/// Wire-shape mirror (NOT symmetric-on-the-wrong-shape) for the recompose-v2
/// `def_ids` carrier: encode a value and pin the EXACT bytes the originator
/// emits, then decode those literal bytes back — so an encoder/decoder drift
/// in the new parallel-vector shape (a renamed field, a dropped position, a
/// tagging change) is caught against the other side's actual wire, per the
/// wire-shape-mirror discipline. The full `TaskInfo` shape is exercised via
/// the crate's own encode (the OTHER direction of the mirror).
#[test]
fn tasks_spawned_def_ids_mirror_wire_bytes() {
    // Round-trip the crate's own encode → decode of a stamped batch (a
    // single-task batch keeps the TaskInfo bytes bounded; the def_ids carrier
    // is what this pins).
    let task: serde_json::Value = serde_json::json!({
        "path": "/tasks/x",
        "size": 0,
        "identifier": test_id("x"),
        "phase_id": "p0",
        "type_id": "t0",
        "affinity_id": null,
        "payload": null,
        "task_id": "x",
    });
    let with_ids = serde_json::json!({
        "TasksSpawned": { "tasks": [task], "def_ids": [7] }
    })
    .to_string();
    let decoded: ClusterMutation<TestId> = serde_json::from_str(&with_ids).unwrap();
    // Re-encode and confirm the def_ids carrier survives byte-for-byte through
    // the crate's own serializer (the encoder side of the mirror).
    let reencoded = serde_json::to_value(&decoded).unwrap();
    assert_eq!(
        reencoded["TasksSpawned"]["def_ids"],
        serde_json::json!([7]),
        "the stamped def_ids carrier survives encode→decode→encode verbatim"
    );
}

/// `skip_serializing_if = "Vec::is_empty"`: an UN-STAMPED `TasksSpawned`
/// (empty `def_ids` — a local-apply construction, or a pre-recompose-v2
/// sender) elides the field on the wire (byte-identical to the legacy shape),
/// and a legacy frame WITHOUT `def_ids` decodes to an EMPTY vector — the
/// receiver reads it positionally as all-`None` (node-local fallback). Mirrors
/// the `legacy_task_added_decodes_without_def_id` rolling-upgrade contract.
#[test]
fn legacy_tasks_spawned_decodes_without_def_ids() {
    // A current sender with an empty def_ids drops the field on the wire.
    let empty: ClusterMutation<TestId> = ClusterMutation::TasksSpawned {
        tasks: Vec::new(),
        def_ids: Vec::new(),
    };
    let v = serde_json::to_value(&empty).unwrap();
    assert!(
        v["TasksSpawned"].get("def_ids").is_none(),
        "an empty def_ids must be omitted on the wire (skip_serializing_if): {v}"
    );
    // A legacy frame (no def_ids field at all) decodes to an empty vector.
    let legacy = serde_json::json!({
        "TasksSpawned": {
            "tasks": [{
                "path": "/tasks/legacy",
                "size": 0,
                "identifier": test_id("legacy"),
                "phase_id": "p0",
                "type_id": "t0",
                "affinity_id": null,
                "payload": null,
                "task_id": "legacy",
            }]
        }
    })
    .to_string();
    let decoded: ClusterMutation<TestId> = serde_json::from_str(&legacy).unwrap();
    match decoded {
        ClusterMutation::TasksSpawned { tasks, def_ids } => {
            assert_eq!(tasks.len(), 1);
            assert!(
                def_ids.is_empty(),
                "a legacy frame decodes def_ids to empty (read positionally as all-None)"
            );
        }
        _ => panic!("expected TasksSpawned"),
    }
}
