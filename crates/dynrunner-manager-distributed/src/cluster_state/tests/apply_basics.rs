//! Tests for the per-mutation `apply` rule transitions.
//!
//! Covers the basic per-hash apply rules — task lifecycle (TaskAdded,
//! TaskAssigned, TaskCompleted, TaskFailed), terminal-states-win,
//! retry-success commutativity, the OutcomeSummary partition, the
//! per-attempts counter, the primary epoch / epoch mirror, the
//! iter_pending filter, convergence under duplicates / out-of-order
//! delivery, and the PhaseDepsSet idempotency.

use super::*;

#[test]
fn task_added_idempotent() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let t = mk_task("a");
    assert_eq!(
        s.apply(ClusterMutation::TaskAdded {
            hash: "h".into(),
            task: t.clone(),
        }),
        ApplyOutcome::Applied
    );
    assert_eq!(
        s.apply(ClusterMutation::TaskAdded {
            hash: "h".into(),
            task: t,
        }),
        ApplyOutcome::NoOp
    );
    assert_eq!(s.task_count(), 1);
    assert_eq!(s.counts().pending, 1);
}

#[test]
fn assigned_late_after_completed_is_noop() {
    // Demonstrates terminal-states-win: TaskCompleted lands first
    // (out-of-order delivery), the late TaskAssigned must NOT
    // resurrect the entry to InFlight.
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_task("a"),
    });
    assert_eq!(
        s.apply(ClusterMutation::TaskCompleted {
            hash: "h".into(),
            result_data: None
        }),
        ApplyOutcome::Applied
    );
    assert_eq!(
        s.apply(ClusterMutation::TaskAssigned {
            hash: "h".into(),
            secondary: "s1".into(),
            worker: 0,

            version: Default::default(),
        }),
        ApplyOutcome::NoOp
    );
    assert!(matches!(
        s.task_state("h"),
        Some(TaskState::Completed { .. })
    ));
}

#[test]
fn duplicate_completed_is_noop() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_task("a"),
    });
    s.apply(ClusterMutation::TaskCompleted {
        hash: "h".into(),
        result_data: None,
    });
    assert_eq!(
        s.apply(ClusterMutation::TaskCompleted {
            hash: "h".into(),
            result_data: None
        }),
        ApplyOutcome::NoOp
    );
}

#[test]
fn failed_then_completed_transitions_to_completed_retry_success() {
    // Retry-success path: the retry pass re-injects a previously
    // Recoverable-failed binary, a worker picks it up, runs to
    // success, and emits TaskCompleted for the same hash. The CRDT
    // must transition Failed → Completed (success is the strongest
    // terminal); pre-fix this branch NoOp'd, leaving the ledger
    // stuck reporting the retry-succeeded task as `fail_retry` and
    // breaking the asm-tokenizer LMU run-done detection (2-of-235
    // retried successes hung the demoted primary in RunComplete-
    // wait). Asymmetric with the `Completed`-locks-out-`Failed`
    // direction below: a late TaskFailed against a Completed entry
    // is still NoOp (success never regresses).
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_task("a"),
    });
    s.apply(ClusterMutation::TaskFailed {
        hash: "h".into(),
        kind: ErrorType::Recoverable,
        error: "x".into(),

        version: Default::default(),
    });
    assert_eq!(
        s.apply(ClusterMutation::TaskCompleted {
            hash: "h".into(),
            result_data: None
        }),
        ApplyOutcome::Applied
    );
    assert!(matches!(
        s.task_state("h"),
        Some(TaskState::Completed { .. })
    ));
}

#[test]
fn completed_then_failed_stays_completed_success_never_regresses() {
    // Symmetric inverse of the retry-success path: once a node has
    // observed `TaskCompleted`, a late `TaskFailed` for the same
    // hash (typically a stale redundant-dispatch path that lost the
    // race) must not regress the ledger. `Completed` is the
    // strongest terminal; the late `TaskFailed` is the NoOp side.
    //
    // Together with `failed_then_completed_transitions_to_completed_retry_success`
    // these two pins prove the lattice converges to `Completed`
    // regardless of (TaskFailed, TaskCompleted) arrival order —
    // commutativity is preserved across the asymmetric transition.
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_task("a"),
    });
    s.apply(ClusterMutation::TaskCompleted {
        hash: "h".into(),
        result_data: None,
    });
    assert_eq!(
        s.apply(ClusterMutation::TaskFailed {
            hash: "h".into(),
            kind: ErrorType::Recoverable,
            error: "late".into(),

            version: Default::default(),
        }),
        ApplyOutcome::NoOp
    );
    assert!(matches!(
        s.task_state("h"),
        Some(TaskState::Completed { .. })
    ));
}

/// Cosmetic #88 regression pin: a demoted-primary terminal log
/// line reads `cluster_state.outcome_counts()` to get the
/// CRDT-authoritative per-class tally. Mixed Completed +
/// Failed-by-kind population must partition into the four
/// `OutcomeSummary` buckets per the documented mapping
/// (`Recoverable → fail_retry`, `ResourceExhausted("memory") →
/// fail_oom`, other `ResourceExhausted` + `NonRecoverable` →
/// `fail_final`); Pending / InFlight entries contribute to neither.
#[test]
fn outcome_counts_partitions_terminal_states_by_error_class() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    // 2 succeeded
    for hash in ["a", "b"] {
        s.apply(ClusterMutation::TaskAdded {
            hash: hash.into(),
            task: mk_task(hash),
        });
        s.apply(ClusterMutation::TaskCompleted {
            hash: hash.into(),
            result_data: None,
        });
    }
    // 1 fail_retry (Recoverable)
    s.apply(ClusterMutation::TaskAdded {
        hash: "c".into(),
        task: mk_task("c"),
    });
    s.apply(ClusterMutation::TaskFailed {
        hash: "c".into(),
        kind: ErrorType::Recoverable,
        error: "x".into(),

        version: Default::default(),
    });
    // 1 fail_oom (ResourceExhausted("memory"))
    s.apply(ClusterMutation::TaskAdded {
        hash: "d".into(),
        task: mk_task("d"),
    });
    s.apply(ClusterMutation::TaskFailed {
        hash: "d".into(),
        kind: ErrorType::ResourceExhausted("memory".into()),
        error: "oom".into(),

        version: Default::default(),
    });
    // 1 fail_final (ResourceExhausted("disk") falls through)
    s.apply(ClusterMutation::TaskAdded {
        hash: "e".into(),
        task: mk_task("e"),
    });
    s.apply(ClusterMutation::TaskFailed {
        hash: "e".into(),
        kind: ErrorType::ResourceExhausted("disk".into()),
        error: "no space".into(),

        version: Default::default(),
    });
    // 1 fail_final (NonRecoverable)
    s.apply(ClusterMutation::TaskAdded {
        hash: "f".into(),
        task: mk_task("f"),
    });
    s.apply(ClusterMutation::TaskFailed {
        hash: "f".into(),
        kind: ErrorType::NonRecoverable,
        error: "panic".into(),

        version: Default::default(),
    });
    // 1 Pending (uncounted)
    s.apply(ClusterMutation::TaskAdded {
        hash: "g".into(),
        task: mk_task("g"),
    });

    let o = s.outcome_counts();
    assert_eq!(o.succeeded, 2, "two TaskCompleted entries");
    assert_eq!(o.fail_retry, 1, "Recoverable → fail_retry");
    assert_eq!(o.fail_oom, 1, "ResourceExhausted(\"memory\") → fail_oom");
    assert_eq!(
        o.fail_final, 2,
        "ResourceExhausted(other) + NonRecoverable → fail_final"
    );
    assert_eq!(o.total_terminal(), 6, "sum across all four buckets");
}

/// The discrete `TaskState::InvalidTask` entry is bucketed as
/// `fail_final` by `outcome_counts` (sibling to `Unfulfillable`),
/// tallied by `counts().invalid_task`, and surfaced by `iter_terminal`
/// (it IS a terminal). Pins all three CRDT read surfaces for the new
/// variant in one population.
#[test]
fn invalid_task_counts_as_fail_final_and_is_terminal() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    // One succeeded, one InvalidTask, one Pending (uncounted terminal).
    s.apply(ClusterMutation::TaskAdded {
        hash: "ok".into(),
        task: mk_task("ok"),
    });
    s.apply(ClusterMutation::TaskCompleted {
        hash: "ok".into(),
        result_data: None,
    });
    s.apply(ClusterMutation::TaskAdded {
        hash: "bad".into(),
        task: mk_task("bad"),
    });
    s.apply(ClusterMutation::TaskFailed {
        hash: "bad".into(),
        kind: ErrorType::InvalidTask {
            reason: "missing dep".to_string().into(),
        },
        error: "invalid_task:missing dep".into(),

        version: Default::default(),
    });
    s.apply(ClusterMutation::TaskAdded {
        hash: "pend".into(),
        task: mk_task("pend"),
    });

    // outcome_counts: the InvalidTask folds into fail_final.
    let o = s.outcome_counts();
    assert_eq!(o.succeeded, 1);
    assert_eq!(o.fail_retry, 0);
    assert_eq!(o.fail_oom, 0);
    assert_eq!(o.fail_final, 1, "InvalidTask → fail_final");
    assert_eq!(o.total_terminal(), 2);

    // counts: dedicated per-discriminant tally.
    let c = s.counts();
    assert_eq!(c.invalid_task, 1, "counts().invalid_task tallies the entry");
    assert_eq!(c.completed, 1);
    assert_eq!(c.pending, 1);
    assert_eq!(
        c.failed, 0,
        "InvalidTask is NOT folded into the generic failed count"
    );

    // iter_terminal includes the InvalidTask entry (it is terminal);
    // the Pending entry is excluded.
    let terminal_ids: std::collections::HashSet<&str> =
        s.iter_terminal().map(|(_, t)| t.task_id.as_str()).collect();
    assert!(terminal_ids.contains("bad"), "InvalidTask is terminal");
    assert!(terminal_ids.contains("ok"));
    assert!(!terminal_ids.contains("pend"), "Pending is not terminal");
}

/// A HIGHER-version re-failure supersedes the prior failure record
/// (`attempts` is dropped — convergence rides the per-task version, D-A/
/// D-V). The newer failure's `(kind, last_error)` wins the join.
#[test]
fn higher_version_refailure_supersedes() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_task("a"),
    });
    s.apply(ClusterMutation::TaskFailed {
        hash: "h".into(),
        kind: ErrorType::Recoverable,
        error: "first".into(),
        version: TaskVersion {
            primary_epoch: 1,
            seq: 0,
        },
    });
    // A strictly-higher-version re-failure wins the join (the re-failure
    // cadence the collector's repeat-count relies on, B1).
    assert_eq!(
        s.apply(ClusterMutation::TaskFailed {
            hash: "h".into(),
            kind: ErrorType::NonRecoverable,
            error: "second".into(),
            version: TaskVersion {
                primary_epoch: 1,
                seq: 1,
            },
        }),
        ApplyOutcome::Applied
    );
    match s.task_state("h") {
        Some(TaskState::Failed {
            kind,
            last_error,
            version,
            ..
        }) => {
            assert_eq!(*kind, ErrorType::NonRecoverable);
            assert_eq!(last_error, "second");
            assert_eq!(
                *version,
                TaskVersion {
                    primary_epoch: 1,
                    seq: 1
                }
            );
        }
        other => panic!("expected Failed, got {other:?}"),
    }
    // A same-version re-delivery is an idempotent NoOp (today's
    // per-delivery double-count is fixed).
    assert_eq!(
        s.apply(ClusterMutation::TaskFailed {
            hash: "h".into(),
            kind: ErrorType::NonRecoverable,
            error: "second".into(),
            version: TaskVersion {
                primary_epoch: 1,
                seq: 1,
            },
        }),
        ApplyOutcome::NoOp
    );
}

#[test]
fn primary_changed_higher_epoch_wins() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::PrimaryChanged {
        new: "a".into(),
        epoch: 1,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
    });
    // Lower epoch is rejected.
    assert_eq!(
        s.apply(ClusterMutation::PrimaryChanged {
            new: "b".into(),
            epoch: 0,
            reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
        }),
        ApplyOutcome::NoOp
    );
    assert_eq!(s.current_primary(), Some("a"));
    // Higher epoch wins.
    s.apply(ClusterMutation::PrimaryChanged {
        new: "c".into(),
        epoch: 5,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
    });
    assert_eq!(s.current_primary(), Some("c"));
    assert_eq!(s.primary_epoch(), 5);
}

/// `primary_epoch_mirror` tracks `primary_epoch` on every applied
/// `PrimaryChanged` mutation. Pins the lock-free reader contract
/// the observer-side announcer task depends on: a `mirror.load()`
/// in response to a role-change hook fire observes the
/// post-mutation value, not the pre-mutation one. Reject paths
/// (lower epoch, same epoch + same id) leave the mirror
/// unchanged because the underlying field is also unchanged.
#[test]
fn primary_epoch_mirror_tracks_apply() {
    use std::sync::atomic::Ordering;
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let mirror = s.primary_epoch_mirror();
    assert_eq!(mirror.load(Ordering::Acquire), 0);

    s.apply(ClusterMutation::PrimaryChanged {
        new: "a".into(),
        epoch: 3,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
    });
    assert_eq!(mirror.load(Ordering::Acquire), 3);

    // NoOp branch (lower epoch) leaves the mirror untouched.
    s.apply(ClusterMutation::PrimaryChanged {
        new: "b".into(),
        epoch: 1,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
    });
    assert_eq!(mirror.load(Ordering::Acquire), 3);

    s.apply(ClusterMutation::PrimaryChanged {
        new: "c".into(),
        epoch: 7,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
    });
    assert_eq!(mirror.load(Ordering::Acquire), 7);
}

/// Snapshot-restore path keeps the mirror in lockstep with the
/// restored epoch. The late-joiner observer's first trigger is the
/// role-change hook firing from inside `restore`'s
/// `primary_epoch > local` branch — that branch writes the mirror
/// BEFORE `fire_role_change_hooks` so the announcer's first
/// awoken read sees the restored epoch, not 0.
#[test]
fn primary_epoch_mirror_tracks_restore() {
    use std::sync::atomic::Ordering;
    let mut origin = ClusterState::<RunnerIdentifier>::new();
    origin.apply(ClusterMutation::PrimaryChanged {
        new: "primary-id".into(),
        epoch: 11,
        reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
    });
    let snap = origin.snapshot();

    let mut joiner = ClusterState::<RunnerIdentifier>::new();
    let mirror = joiner.primary_epoch_mirror();
    assert_eq!(mirror.load(Ordering::Acquire), 0);
    joiner.restore(snap);
    assert_eq!(mirror.load(Ordering::Acquire), 11);
}

#[test]
fn iter_pending_only_returns_pending() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "p".into(),
        task: mk_task("p"),
    });
    s.apply(ClusterMutation::TaskAdded {
        hash: "i".into(),
        task: mk_task("i"),
    });
    s.apply(ClusterMutation::TaskAssigned {
        hash: "i".into(),
        secondary: "s".into(),
        worker: 0,

        version: Default::default(),
    });
    s.apply(ClusterMutation::TaskAdded {
        hash: "c".into(),
        task: mk_task("c"),
    });
    s.apply(ClusterMutation::TaskCompleted {
        hash: "c".into(),
        result_data: None,
    });

    let mut pending: Vec<&str> = s.iter_pending().map(|(h, _)| h.as_str()).collect();
    pending.sort();
    assert_eq!(pending, vec!["p"]);

    let in_flight: Vec<&str> = s.iter_in_flight().map(|(h, _, _)| h.as_str()).collect();
    assert_eq!(in_flight, vec!["i"]);
}

/// Convergence under the realistic reordering: `TaskAdded` always
/// happens-before `TaskAssigned` and `TaskCompleted` (the primary
/// originates `TaskAdded` before issuing the assignment, which is
/// itself a strict prerequisite of the worker completing). Within
/// that constraint, `TaskCompleted` can race ahead of the matching
/// `TaskAssigned` at a third-party receiver — both orderings must
/// converge to `Completed`.
#[test]
fn convergence_completed_can_race_assigned() {
    let added = ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_task("a"),
    };
    let assigned = ClusterMutation::TaskAssigned {
        hash: "h".into(),
        secondary: "s".into(),
        worker: 0,

        version: Default::default(),
    };
    let completed: ClusterMutation<RunnerIdentifier> = ClusterMutation::TaskCompleted {
        hash: "h".into(),
        result_data: None,
    };

    let mut a = ClusterState::<RunnerIdentifier>::new();
    a.apply(added.clone());
    a.apply(assigned.clone());
    a.apply(completed.clone());

    let mut b = ClusterState::<RunnerIdentifier>::new();
    b.apply(added);
    b.apply(completed);
    b.apply(assigned);

    assert!(matches!(
        a.task_state("h"),
        Some(TaskState::Completed { .. })
    ));
    assert!(matches!(
        b.task_state("h"),
        Some(TaskState::Completed { .. })
    ));
}

/// Convergence under duplicates: applying every mutation twice
/// in interleaved order produces the same final state as
/// applying each once in their natural order.
#[test]
fn convergence_under_duplicates() {
    let muts: Vec<ClusterMutation<RunnerIdentifier>> = vec![
        ClusterMutation::TaskAdded {
            hash: "h1".into(),
            task: mk_task("h1"),
        },
        ClusterMutation::TaskAdded {
            hash: "h2".into(),
            task: mk_task("h2"),
        },
        ClusterMutation::TaskAssigned {
            hash: "h1".into(),
            secondary: "s".into(),
            worker: 0,

            version: Default::default(),
        },
        ClusterMutation::TaskCompleted {
            hash: "h1".into(),
            result_data: None,
        },
        ClusterMutation::TaskFailed {
            hash: "h2".into(),
            kind: ErrorType::Recoverable,
            error: "boom".into(),

            version: Default::default(),
        },
    ];
    let mut once = ClusterState::<RunnerIdentifier>::new();
    for m in muts.iter().cloned() {
        once.apply(m);
    }
    let mut twice = ClusterState::<RunnerIdentifier>::new();
    for m in muts.iter().cloned() {
        twice.apply(m.clone());
        twice.apply(m);
    }
    assert_eq!(once.counts(), twice.counts());
    assert!(matches!(
        once.task_state("h1"),
        Some(TaskState::Completed { .. })
    ));
    assert!(matches!(
        twice.task_state("h1"),
        Some(TaskState::Completed { .. })
    ));
    // The same-version `TaskFailed` applied twice is idempotent (the
    // second is a NoOp under the version-keyed join — `attempts` is
    // dropped, so a re-delivery no longer mutates the record). Both
    // states converge to the identical failure record.
    match (once.task_state("h2"), twice.task_state("h2")) {
        (
            Some(TaskState::Failed {
                kind: k1,
                last_error: e1,
                version: v1,
                ..
            }),
            Some(TaskState::Failed {
                kind: k2,
                last_error: e2,
                version: v2,
                ..
            }),
        ) => {
            assert_eq!(k1, k2);
            assert_eq!(e1, e2);
            assert_eq!(v1, v2);
            assert_eq!(*k2, ErrorType::Recoverable);
            assert_eq!(e2, "boom");
        }
        other => panic!("expected Failed on both, got {other:?}"),
    }
}

#[test]
fn phase_deps_set_then_re_set_is_noop() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let deps: HashMap<PhaseId, Vec<PhaseId>> = [(PhaseId::from("p1"), vec![PhaseId::from("p0")])]
        .into_iter()
        .collect();
    assert_eq!(
        s.apply(ClusterMutation::PhaseDepsSet { deps: deps.clone() }),
        ApplyOutcome::Applied
    );
    assert_eq!(s.phase_deps(), &deps);
    // Re-application is silent — the per-run graph is static.
    let other: HashMap<PhaseId, Vec<PhaseId>> = HashMap::new();
    assert_eq!(
        s.apply(ClusterMutation::PhaseDepsSet { deps: other }),
        ApplyOutcome::NoOp
    );
    assert_eq!(s.phase_deps(), &deps);
}
