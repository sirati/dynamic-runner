//! Wave-A CRDT convergence acceptance tests (design §5.3).
//!
//! The non-negotiable invariant: apply == restore == digest across every
//! `TaskState` transition, in both arrival orders, with exactly-once
//! side-effects. These pin the shared `task_join_key` / `merge_task_state`
//! linchpin: the D-T InvalidTask-TOP terminal order, the C3
//! stale-assignment-after-reset closure, the B1 re-failure emit cadence,
//! the C4 equal-version divergent-payload detection, and the
//! apply/restore/digest agreement.

use super::super::merge::{MergeOutcome, task_join_key, task_join_key_dominates};
use super::*;
use dynrunner_core::ErrorType;

// ── helpers ──

/// Build the seven canonical states for a fixed hash/task, each with a
/// distinct version where the variant carries one, so the property tests
/// can enumerate ordered pairs.
fn state_variants() -> Vec<(&'static str, TaskState<RunnerIdentifier>)> {
    let t = || mk_task("x");
    let v = |seq| TaskVersion {
        primary_epoch: 1,
        seq,
    };
    vec![
        (
            "pending",
            TaskState::Pending {
                task: t(),
                version: v(1),
            },
        ),
        (
            "inflight",
            TaskState::InFlight {
                task: t(),
                secondary: "s".into(),
                worker: 0,
                version: v(2),
            },
        ),
        (
            "blocked",
            TaskState::Blocked {
                task: t(),
                on: "p".into(),
            },
        ),
        ("completed", TaskState::Completed { task: t() }),
        (
            "failed",
            TaskState::Failed {
                task: t(),
                kind: ErrorType::Recoverable,
                last_error: "e".into(),
                version: v(1),
            },
        ),
        (
            "unfulfillable",
            TaskState::Unfulfillable {
                task: t(),
                reason: "r".into(),
                last_error: "e".into(),
                version: v(1),
            },
        ),
        (
            "invalid",
            TaskState::InvalidTask {
                task: t(),
                reason: "r".into(),
                last_error: "e".into(),
                version: v(1),
            },
        ),
    ]
}

/// Merge `a` then `b` into a fresh state via the shared join and return
/// the resulting variant discriminant name.
fn merge_pair(a: &TaskState<RunnerIdentifier>, b: &TaskState<RunnerIdentifier>) -> &'static str {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let mut resumed = Vec::new();
    s.merge_task_state("h", a.clone(), None, &mut resumed);
    s.merge_task_state("h", b.clone(), None, &mut resumed);
    variant_name(s.task_state("h").expect("present"))
}

fn variant_name(s: &TaskState<RunnerIdentifier>) -> &'static str {
    match s {
        TaskState::Pending { .. } => "pending",
        TaskState::InFlight { .. } => "inflight",
        TaskState::Blocked { .. } => "blocked",
        TaskState::Completed { .. } => "completed",
        TaskState::Failed { .. } => "failed",
        TaskState::Unfulfillable { .. } => "unfulfillable",
        TaskState::InvalidTask { .. } => "invalid",
    }
}

// ── §5.3 #1: total + commutative join ──

/// The join is COMMUTATIVE: `merge(a, b) == merge(b, a)` for every
/// ordered pair of states. This is the apply==restore order-independence
/// foundation (every replica converges regardless of arrival order).
#[test]
fn merge_is_total_and_commutative() {
    let variants = state_variants();
    for (na, a) in &variants {
        for (nb, b) in &variants {
            let ab = merge_pair(a, b);
            let ba = merge_pair(b, a);
            assert_eq!(
                ab, ba,
                "merge({na}, {nb}) = {ab} but merge({nb}, {na}) = {ba} — non-commutative"
            );
        }
    }
}

/// `task_join_key_dominates` is a strict total order: it is irreflexive
/// (a state never dominates an equal-keyed copy of itself) and
/// antisymmetric (at most one of `a>b`, `b>a` holds).
#[test]
fn dominance_is_strict_and_antisymmetric() {
    let variants = state_variants();
    for (_, a) in &variants {
        let ka = task_join_key(a);
        assert!(
            !task_join_key_dominates(&ka, &ka),
            "a state must not dominate an equal key (idempotent NoOp)"
        );
        for (_, b) in &variants {
            let kb = task_join_key(b);
            let ab = task_join_key_dominates(&ka, &kb);
            let ba = task_join_key_dominates(&kb, &ka);
            assert!(!(ab && ba), "antisymmetry violated");
        }
    }
}

// ── §5.3 #2: D-T — InvalidTask is the unique TOP (both directions) ──

/// Both arrival orders of `(Completed, InvalidTask)` converge to
/// InvalidTask (the D-T reversal from v1's Completed-TOP), AND the
/// apply-level flip is pinned: incoming InvalidTask against a local
/// Completed WINS (the flipped `apply.rs:231-233`), while incoming
/// Completed against a local InvalidTask is a NoOp (the KEPT
/// `apply.rs:124`).
#[test]
fn completed_vs_invalidtask_invalidtask_wins() {
    // Merge-level: both orders → InvalidTask.
    let completed = TaskState::Completed { task: mk_task("x") };
    let invalid = TaskState::InvalidTask {
        task: mk_task("x"),
        reason: "dup".into(),
        last_error: "invalid_task:dup".into(),
        version: Default::default(),
    };
    assert_eq!(merge_pair(&completed, &invalid), "invalid");
    assert_eq!(merge_pair(&invalid, &completed), "invalid");

    // Apply-level: incoming InvalidTask supersedes a local Completed.
    let mut a = ClusterState::<RunnerIdentifier>::new();
    a.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_task("x"),
    });
    a.apply(ClusterMutation::TaskCompleted {
        hash: "h".into(),
        result_data: None,
    });
    assert_eq!(
        a.apply(ClusterMutation::TaskFailed {
            hash: "h".into(),
            kind: ErrorType::InvalidTask {
                reason: "dup".to_string().into(),
            },
            error: "invalid_task:dup".into(),
            version: Default::default(),
        }),
        ApplyOutcome::Applied,
        "incoming InvalidTask must supersede a local Completed (D-T flip)"
    );
    assert!(matches!(
        a.task_state("h"),
        Some(TaskState::InvalidTask { .. })
    ));

    // Apply-level reverse: incoming Completed against a local InvalidTask
    // is a NoOp (the kept apply.rs:124 lockout).
    let mut b = ClusterState::<RunnerIdentifier>::new();
    b.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_task("x"),
    });
    b.apply(ClusterMutation::TaskFailed {
        hash: "h".into(),
        kind: ErrorType::InvalidTask {
            reason: "dup".to_string().into(),
        },
        error: "invalid_task:dup".into(),
        version: Default::default(),
    });
    assert_eq!(
        b.apply(ClusterMutation::TaskCompleted {
            hash: "h".into(),
            result_data: None,
        }),
        ApplyOutcome::NoOp,
        "incoming Completed must NOT regress a local InvalidTask"
    );
    assert!(matches!(
        b.task_state("h"),
        Some(TaskState::InvalidTask { .. })
    ));
}

/// The full terminal order `{Failed, Unfulfillable} < Completed <
/// InvalidTask` holds at the merge level (Completed dominates the
/// failure-likes; InvalidTask dominates Completed).
#[test]
fn terminal_total_order_holds() {
    let completed = TaskState::Completed { task: mk_task("x") };
    let failed = TaskState::Failed {
        task: mk_task("x"),
        kind: ErrorType::Recoverable,
        last_error: "e".into(),
        version: Default::default(),
    };
    let unful = TaskState::Unfulfillable {
        task: mk_task("x"),
        reason: "r".into(),
        last_error: "e".into(),
        version: Default::default(),
    };
    assert_eq!(merge_pair(&failed, &completed), "completed");
    assert_eq!(merge_pair(&completed, &failed), "completed");
    assert_eq!(merge_pair(&unful, &completed), "completed");
    assert_eq!(merge_pair(&completed, &unful), "completed");
}

// ── §2.2: FailedLike-vs-FailedLike — version BEFORE the discriminant ──

/// Within `TerminalRank::FailedLike`, the join key compares `version`
/// BEFORE the `failedlike` discriminant (design §2.2: the tuple is
/// `(band, terminal_rank, version, nonterminal_rank, failedlike,
/// payload_hash)`). So a higher-version generic `Failed` SUPERSEDES a
/// lower-version `Unfulfillable`, and only at EQUAL version does the
/// `Failed < Unfulfillable` discriminant decide.
///
/// This pins the merge-level behavior EXPLICITLY rather than leaning on
/// the upstream primary-side `failed_tasks` dedup gate
/// (`primary/task/failed.rs` + `handler.rs`) that keeps the higher-version
/// generic-`Failed`-over-`Unfulfillable` case unreachable in production —
/// that gate is a DIFFERENT concern, and this test makes the comparator's
/// own ordering a pinned invariant in its own right.
#[test]
fn failedlike_version_arbitrates_before_discriminant() {
    let task = mk_task("x");
    let unful_s1 = TaskState::Unfulfillable {
        task: task.clone(),
        reason: "no-toolchain".into(),
        last_error: "unfulfillable".into(),
        version: TaskVersion {
            primary_epoch: 0,
            seq: 1,
        },
    };
    let failed_s2 = TaskState::Failed {
        task: task.clone(),
        kind: ErrorType::NonRecoverable,
        last_error: "boom".into(),
        version: TaskVersion {
            primary_epoch: 0,
            seq: 2,
        },
    };

    // (a) Higher-version generic `Failed` (s2) WINS over a lower-version
    // `Unfulfillable` (s1): version arbitrates before the discriminant, so
    // the incoming `Failed` strictly dominates → Applied, state → Failed.
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let mut resumed = Vec::new();
    s.merge_task_state("h", unful_s1.clone(), None, &mut resumed);
    let out = s.merge_task_state("h", failed_s2.clone(), None, &mut resumed);
    assert!(
        matches!(
            out,
            MergeOutcome::Applied {
                failure_won: true,
                ..
            }
        ),
        "higher-version Failed must win the FailedLike join (version before discriminant), got {out:?}"
    );
    assert_eq!(
        variant_name(s.task_state("h").expect("present")),
        "failed",
        "the higher-version Failed (s2) supersedes the Unfulfillable (s1)"
    );
    // The reverse delivery order: the now-local higher-version Failed (s2)
    // is NOT regressed by a redelivered lower-version Unfulfillable (s1).
    let out_rev = s.merge_task_state("h", unful_s1.clone(), None, &mut resumed);
    assert_eq!(
        out_rev,
        MergeOutcome::NoOp,
        "a lower-version Unfulfillable must NOT regress the won higher-version Failed"
    );
    // And the converged state is order-independent: s2-Failed wins both ways.
    assert_eq!(merge_pair(&unful_s1, &failed_s2), "failed");
    assert_eq!(merge_pair(&failed_s2, &unful_s1), "failed");

    // (b) EQUAL version (both s1): the `failedlike` discriminant decides.
    // `Failed = 0 < Unfulfillable = 1`, and the join keeps the MAX key, so
    // `Unfulfillable` is the deterministic winner — and an incoming generic
    // `Failed` at equal version is a NoOp against a local `Unfulfillable`.
    let failed_s1 = TaskState::Failed {
        task: task.clone(),
        kind: ErrorType::NonRecoverable,
        last_error: "boom".into(),
        version: TaskVersion {
            primary_epoch: 0,
            seq: 1,
        },
    };
    let mut eq = ClusterState::<RunnerIdentifier>::new();
    let mut eq_resumed = Vec::new();
    eq.merge_task_state("h", unful_s1.clone(), None, &mut eq_resumed);
    let eq_out = eq.merge_task_state("h", failed_s1.clone(), None, &mut eq_resumed);
    assert_eq!(
        eq_out,
        MergeOutcome::NoOp,
        "an equal-version generic Failed must NOT supersede a local Unfulfillable (Failed < Unfulfillable)"
    );
    assert_eq!(
        variant_name(eq.task_state("h").expect("present")),
        "unfulfillable",
        "at equal version the Unfulfillable discriminant wins"
    );
    // Commutative at equal version: Unfulfillable wins regardless of order.
    assert_eq!(merge_pair(&unful_s1, &failed_s1), "unfulfillable");
    assert_eq!(merge_pair(&failed_s1, &unful_s1), "unfulfillable");
}

// ── §5.3 #3: C3 stale-assignment-after-requeue ──

/// `Pending v0 → InFlight v1 (assigned) → Pending v2 (requeue reset,
/// version-bumped)`; then redeliver the stale `TaskAssigned` carrying
/// `InFlight v1`. It must LOSE (state stays the reset Pending), and a
/// genuine higher-version re-assignment WINS. Covers C3 (a)(b)(c).
#[test]
fn stale_assignment_after_requeue_does_not_resurrect() {
    let task = mk_task("x");
    let pending_v0 = TaskState::Pending {
        task: task.clone(),
        version: TaskVersion {
            primary_epoch: 1,
            seq: 0,
        },
    };
    let inflight_v1 = TaskState::InFlight {
        task: task.clone(),
        secondary: "dead-sec".into(),
        worker: 0,
        version: TaskVersion {
            primary_epoch: 1,
            seq: 1,
        },
    };
    let reset_pending_v2 = TaskState::Pending {
        task: task.clone(),
        version: TaskVersion {
            primary_epoch: 1,
            seq: 2,
        },
    };
    let reassign_inflight_v3 = TaskState::InFlight {
        task: task.clone(),
        secondary: "live-sec".into(),
        worker: 1,
        version: TaskVersion {
            primary_epoch: 1,
            seq: 3,
        },
    };

    let mut s = ClusterState::<RunnerIdentifier>::new();
    let mut resumed = Vec::new();
    s.merge_task_state("h", pending_v0, None, &mut resumed);
    s.merge_task_state("h", inflight_v1.clone(), None, &mut resumed);
    // (a)/(b): the reset Pending v2 beats the stale InFlight v1 even
    // though Pending's rank is lower — version arbitrates within the band.
    s.merge_task_state("h", reset_pending_v2, None, &mut resumed);
    assert!(
        matches!(s.task_state("h"), Some(TaskState::Pending { .. })),
        "reset Pending v2 must win over the InFlight v1 it replaces"
    );
    // Redeliver the stale InFlight v1: it must LOSE to the reset.
    let out = s.merge_task_state("h", inflight_v1, None, &mut resumed);
    assert_eq!(
        out,
        MergeOutcome::NoOp,
        "a redelivered stale InFlight v1 must NOT resurrect the dead assignment"
    );
    assert!(matches!(s.task_state("h"), Some(TaskState::Pending { .. })));
    // (c): a genuine post-reset re-assignment (still higher version) wins.
    s.merge_task_state("h", reassign_inflight_v3, None, &mut resumed);
    match s.task_state("h") {
        Some(TaskState::InFlight { secondary, .. }) => assert_eq!(secondary, "live-sec"),
        other => panic!("expected re-assigned InFlight, got {other:?}"),
    }
}

// ── §5.3 #6: B1 re-failure emit cadence ──

/// A higher-version re-failure WINS the join and emits (the re-failure
/// cadence the collector's repeat-count relies on); a same-version
/// re-delivery is a NoOp with no emit (today's per-delivery double-count
/// is fixed).
#[tokio::test]
async fn refailure_higher_version_emits_same_version_noops() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    s.install_task_completed_sender(tx);
    s.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_task("x"),
    });
    // First failure at v1 — emits.
    s.apply(ClusterMutation::TaskFailed {
        hash: "h".into(),
        kind: ErrorType::Recoverable,
        error: "first".into(),
        version: TaskVersion {
            primary_epoch: 1,
            seq: 1,
        },
    });
    assert!(rx.try_recv().is_ok(), "first failure must emit");
    // Higher-version re-failure — WINS, emits again.
    assert_eq!(
        s.apply(ClusterMutation::TaskFailed {
            hash: "h".into(),
            kind: ErrorType::NonRecoverable,
            error: "second".into(),
            version: TaskVersion {
                primary_epoch: 1,
                seq: 2
            },
        }),
        ApplyOutcome::Applied
    );
    match rx.try_recv() {
        Ok(ev) => {
            assert!(!ev.success);
            assert_eq!(ev.last_error.as_deref(), Some("second"));
        }
        other => panic!("higher-version re-failure must emit, got {other:?}"),
    }
    // Same-version re-delivery — NoOp, no emit.
    assert_eq!(
        s.apply(ClusterMutation::TaskFailed {
            hash: "h".into(),
            kind: ErrorType::NonRecoverable,
            error: "second".into(),
            version: TaskVersion {
                primary_epoch: 1,
                seq: 2
            },
        }),
        ApplyOutcome::NoOp
    );
    assert!(
        rx.try_recv().is_err(),
        "a same-version re-delivery must NOT emit (no double-count)"
    );
}

// ── §5.3 #5 / C4: failure-record divergence is digest-detectable ──

/// Two replicas with the same hash but divergent failure records produce
/// DIFFERENT digests so `is_behind` fires — including the EQUAL-version
/// sub-case (C4): same version, different `(kind, error)` still diverge
/// because the payload content hash (not the version) carries it.
#[test]
fn failure_record_divergence_detected() {
    let mk = |kind: ErrorType, err: &str, seq: u32| {
        let mut s = ClusterState::<RunnerIdentifier>::new();
        s.apply(ClusterMutation::TaskAdded {
            hash: "h".into(),
            task: mk_task("x"),
        });
        s.apply(ClusterMutation::TaskFailed {
            hash: "h".into(),
            kind,
            error: err.into(),
            version: TaskVersion {
                primary_epoch: 1,
                seq,
            },
        });
        s
    };
    // Different version → different digest.
    let a = mk(ErrorType::Recoverable, "boom", 1);
    let b = mk(ErrorType::Recoverable, "boom", 2);
    assert_ne!(a.digest().tasks_hash, b.digest().tasks_hash);
    assert!(a.digest().is_behind(&b.digest()) || b.digest().is_behind(&a.digest()));

    // EQUAL version, different (kind, error) → still different digest (C4).
    let c = mk(ErrorType::Recoverable, "boom", 5);
    let d = mk(ErrorType::NonRecoverable, "panic", 5);
    assert_ne!(
        c.digest().tasks_hash,
        d.digest().tasks_hash,
        "equal-version divergent payload must produce different digests (C4)"
    );
    assert!(c.digest().is_behind(&d.digest()));
    assert!(d.digest().is_behind(&c.digest()));
}

/// `last_error` survives a snapshot/restore round-trip identically for
/// every failure-bearing terminal (TS-4: the non-lossy add that kills the
/// apply-emit vs restore-emit divergence).
#[test]
fn last_error_survives_restore_for_all_terminals() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    for (h, kind, msg) in [
        ("f", ErrorType::NonRecoverable, "fail-msg"),
        (
            "u",
            ErrorType::Unfulfillable {
                reason: "res".to_string().into(),
            },
            "unful-msg",
        ),
        (
            "i",
            ErrorType::InvalidTask {
                reason: "dup".to_string().into(),
            },
            "invalid-msg",
        ),
    ] {
        s.apply(ClusterMutation::TaskAdded {
            hash: h.into(),
            task: mk_task(h),
        });
        s.apply(ClusterMutation::TaskFailed {
            hash: h.into(),
            kind,
            error: msg.into(),
            version: Default::default(),
        });
    }
    let mut joiner = ClusterState::<RunnerIdentifier>::new();
    joiner.restore(s.snapshot());
    let last_error = |st: Option<&TaskState<RunnerIdentifier>>| match st {
        Some(TaskState::Failed { last_error, .. })
        | Some(TaskState::Unfulfillable { last_error, .. })
        | Some(TaskState::InvalidTask { last_error, .. }) => last_error.clone(),
        other => panic!("expected failure terminal, got {other:?}"),
    };
    assert_eq!(last_error(joiner.task_state("f")), "fail-msg");
    assert_eq!(last_error(joiner.task_state("u")), "unful-msg");
    assert_eq!(last_error(joiner.task_state("i")), "invalid-msg");
}

// ── §5.3 #8/#11: apply == restore == digest agreement ──

/// An apply-built state and a fresh state restored from its snapshot are
/// byte-equal by digest and neither is `is_behind` the other (TS-1/AE-1).
#[test]
fn apply_restore_digest_agree() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    for h in ["a", "b", "c", "d"] {
        s.apply(ClusterMutation::TaskAdded {
            hash: h.into(),
            task: mk_task(h),
        });
    }
    s.apply(ClusterMutation::TaskAssigned {
        hash: "b".into(),
        secondary: "s".into(),
        worker: 0,
        version: TaskVersion {
            primary_epoch: 1,
            seq: 1,
        },
    });
    s.apply(ClusterMutation::TaskCompleted {
        hash: "c".into(),
        result_data: None,
    });
    s.apply(ClusterMutation::TaskFailed {
        hash: "d".into(),
        kind: ErrorType::NonRecoverable,
        error: "boom".into(),
        version: TaskVersion {
            primary_epoch: 1,
            seq: 1,
        },
    });
    let mut joiner = ClusterState::<RunnerIdentifier>::new();
    joiner.restore(s.snapshot());
    assert_eq!(s.digest().tasks_hash, joiner.digest().tasks_hash);
    assert_eq!(s.digest().tasks_count, joiner.digest().tasks_count);
    assert!(!s.digest().is_behind(&joiner.digest()));
    assert!(!joiner.digest().is_behind(&s.digest()));
}

/// Local `Failed`, restore a snapshot with `Completed` for the same hash
/// → Completed wins, emits a success event, populates the output cache by
/// content hash, and resumes a blocked dependent (the TS-1/TS-2/TS-3/TS-5
/// repro on the restore path).
#[test]
fn restore_supersedes_failed_with_completed() {
    // Source: a Completed prereq with a Blocked dependent on it.
    let mut src = ClusterState::<RunnerIdentifier>::new();
    src.apply(ClusterMutation::TaskAdded {
        hash: "prereq".into(),
        task: mk_task("prereq"),
    });
    src.apply(ClusterMutation::TaskCompleted {
        hash: "prereq".into(),
        result_data: None,
    });

    // Local replica: the same prereq is locally Failed, and a dependent
    // is Blocked on it.
    let mut local = ClusterState::<RunnerIdentifier>::new();
    local.apply(ClusterMutation::TaskAdded {
        hash: "prereq".into(),
        task: mk_task("prereq"),
    });
    local.apply(ClusterMutation::TaskFailed {
        hash: "prereq".into(),
        kind: ErrorType::Recoverable,
        error: "transient".into(),
        version: TaskVersion {
            primary_epoch: 1,
            seq: 1,
        },
    });
    local.apply(ClusterMutation::TaskAdded {
        hash: "dep".into(),
        task: mk_task("dep"),
    });
    local.apply(ClusterMutation::TaskBlocked {
        hash: "dep".into(),
        on: "prereq".into(),
    });

    // Restore the source snapshot, collecting resumed dependents.
    let mut resumed = Vec::new();
    local.restore_collecting_resumed(src.snapshot(), &mut resumed);

    // Completed superseded the local Failed.
    assert!(matches!(
        local.task_state("prereq"),
        Some(TaskState::Completed { .. })
    ));
    // The blocked dependent auto-resumed to Pending (TS-2 on restore).
    assert!(matches!(
        local.task_state("dep"),
        Some(TaskState::Pending { .. })
    ));
    assert_eq!(resumed.len(), 1, "the blocked dependent is surfaced");
    assert_eq!(resumed[0].task_id, "dep");
}

/// Re-restoring the same snapshot is idempotent AND emits each terminal
/// event exactly once (TS-5): the first restore emits; the second is
/// all-NoOp (the key no longer dominates) so no event fires twice.
#[tokio::test]
async fn re_restore_is_idempotent_and_emits_once() {
    let mut src = ClusterState::<RunnerIdentifier>::new();
    src.apply(ClusterMutation::TaskAdded {
        hash: "c".into(),
        task: mk_task("c"),
    });
    src.apply(ClusterMutation::TaskCompleted {
        hash: "c".into(),
        result_data: None,
    });
    src.apply(ClusterMutation::TaskAdded {
        hash: "f".into(),
        task: mk_task("f"),
    });
    src.apply(ClusterMutation::TaskFailed {
        hash: "f".into(),
        kind: ErrorType::NonRecoverable,
        error: "boom".into(),
        version: TaskVersion {
            primary_epoch: 1,
            seq: 1,
        },
    });
    let snap = src.snapshot();

    let mut joiner = ClusterState::<RunnerIdentifier>::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    joiner.install_task_completed_sender(tx);
    joiner.restore(snap.clone());
    // Two terminal events on the first restore (one success, one failure).
    let mut first = 0;
    while rx.try_recv().is_ok() {
        first += 1;
    }
    assert_eq!(first, 2, "first restore emits exactly the two terminals");
    let digest_after_first = joiner.digest();
    // Second restore: all-NoOp, no new events, identical digest.
    joiner.restore(snap);
    assert!(
        rx.try_recv().is_err(),
        "re-restore must not re-emit a terminal event"
    );
    assert_eq!(joiner.digest().tasks_hash, digest_after_first.tasks_hash);
}

/// Three partial snapshots applied in all 6 orders converge to the same
/// state and the same digest (multi-responder union order-independence).
#[test]
fn n_responder_union_order_independent() {
    // Build three partial snapshots, each holding a distinct subset.
    let mk_snap = |h: &str, terminal: bool| {
        let mut s = ClusterState::<RunnerIdentifier>::new();
        s.apply(ClusterMutation::TaskAdded {
            hash: h.into(),
            task: mk_task(h),
        });
        if terminal {
            s.apply(ClusterMutation::TaskCompleted {
                hash: h.into(),
                result_data: None,
            });
        }
        s.snapshot()
    };
    let snaps = [mk_snap("a", true), mk_snap("b", false), mk_snap("c", true)];
    let indices = [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ];
    let mut reference: Option<u64> = None;
    for order in indices {
        let mut s = ClusterState::<RunnerIdentifier>::new();
        for &i in &order {
            s.restore(snaps[i].clone());
        }
        let h = s.digest().tasks_hash;
        match reference {
            None => reference = Some(h),
            Some(r) => assert_eq!(h, r, "union digest must be order-independent"),
        }
        assert_eq!(s.counts().completed, 2);
        assert_eq!(s.counts().pending, 1);
    }
}
