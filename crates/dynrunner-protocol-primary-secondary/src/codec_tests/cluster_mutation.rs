use super::*;
use crate::cluster_mutation::{ClusterMutation, PrimaryChangeReason};
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
    };

    let json = serde_json::to_string(&mutation).unwrap();
    let decoded: ClusterMutation<TestId> = serde_json::from_str(&json).unwrap();

    match decoded {
        ClusterMutation::PeerJoined {
            peer_id,
            is_observer,
            can_be_primary,
            cap_version,
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
        } => {
            assert_eq!(peer_id, "legacy-peer");
            assert!(is_observer);
            // can_be_primary also serde(default) → false.
            assert!(!can_be_primary);
            assert_eq!(cap_version, TaskVersion::default());
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
    };
    let json = serde_json::to_string(&mutation).unwrap();
    let decoded: ClusterMutation<TestId> = serde_json::from_str(&json).unwrap();
    match decoded {
        ClusterMutation::CustomMessagePosted {
            origin,
            seq,
            topic,
            data,
        } => {
            assert_eq!(origin, "sec-1");
            assert_eq!(seq, 11);
            assert_eq!(topic, "phase4-batch");
            assert_eq!(data, b"batch payload".to_vec());
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

/// F5 `CustomMessageFailed` round-trips with its `(origin, seq)` key
/// (the terminal twin of `CustomMessageHandled` — a handler raise).
#[test]
fn roundtrip_custom_message_failed() {
    let mutation: ClusterMutation<TestId> = ClusterMutation::CustomMessageFailed {
        origin: "sec-1".into(),
        seq: 11,
    };
    let json = serde_json::to_string(&mutation).unwrap();
    let decoded: ClusterMutation<TestId> = serde_json::from_str(&json).unwrap();
    match decoded {
        ClusterMutation::CustomMessageFailed { origin, seq } => {
            assert_eq!(origin, "sec-1");
            assert_eq!(seq, 11);
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
        } => {
            assert_eq!((origin.as_str(), seq, topic.as_str()), ("sec-1", 3, "t"));
            assert_eq!(data, vec![1, 2]);
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
    let failed = r#"{"CustomMessageFailed":{"origin":"sec-1","seq":3}}"#;
    let decoded: ClusterMutation<TestId> = serde_json::from_str(failed).unwrap();
    match decoded {
        ClusterMutation::CustomMessageFailed { origin, seq } => {
            assert_eq!((origin.as_str(), seq), ("sec-1", 3));
        }
        _ => panic!("expected CustomMessageFailed"),
    }
}
