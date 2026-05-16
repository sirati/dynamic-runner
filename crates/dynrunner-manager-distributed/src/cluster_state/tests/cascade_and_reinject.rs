//! Tests for the discrete `Unfulfillable` / `Blocked` state machine,
//! the cascade-on-unfulfillable apply rule, the `TaskCompleted` auto-
//! resume that lifts blocked dependents back to `Pending`, and the
//! `TaskReinjected` apply rule's tightening to `Unfulfillable`-only.
//!
//! Also covers the `TaskPreferredSecondariesUpdated` apply rule:
//! the apply rule writes the new SoftPreferredSecondaries set onto
//! the task entry regardless of which non-terminal state it is in.

use super::*;




#[test]
fn task_preferred_secondaries_updated_apply_writes_to_task() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_task("h"),
    });
    assert_eq!(
        s.apply(ClusterMutation::TaskPreferredSecondariesUpdated {
            hash: "h".into(),
            secondaries: vec!["secondary-2".into(), "secondary-5".into()],
        }),
        ApplyOutcome::Applied
    );
    let Some(TaskState::Pending { task }) = s.task_state("h") else {
        panic!("expected Pending");
    };
    assert_eq!(task.preferred_secondaries.as_slice(), &["secondary-2", "secondary-5"]);
}

#[test]
fn task_preferred_secondaries_updated_apply_unknown_hash_is_noop() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    assert_eq!(
        s.apply(ClusterMutation::TaskPreferredSecondariesUpdated {
            hash: "nope".into(),
            secondaries: vec!["secondary-1".into()],
        }),
        ApplyOutcome::NoOp
    );
}

#[test]
fn task_preferred_secondaries_updated_apply_preserves_state() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_task("h"),
    });
    s.apply(ClusterMutation::TaskFailed {
        hash: "h".into(),
        kind: ErrorType::Unfulfillable {
            reason: "missing".to_string().into(),
        },
        error: "missing".into(),
    });
    assert_eq!(
        s.apply(ClusterMutation::TaskPreferredSecondariesUpdated {
            hash: "h".into(),
            secondaries: vec!["secondary-7".into()],
        }),
        ApplyOutcome::Applied
    );
    let Some(TaskState::Unfulfillable { task, reason }) = s.task_state("h") else {
        panic!("state must stay Unfulfillable across preferred-secondaries update");
    };
    assert_eq!(reason, "missing");
    assert_eq!(task.preferred_secondaries.as_slice(), &["secondary-7"]);
}

// ── Discrete Unfulfillable / Blocked state pins ──

/// `TaskFailed { kind: ErrorType::Unfulfillable, .. }` lands in the
/// discrete `TaskState::Unfulfillable { reason, task }` variant,
/// NOT in `TaskState::Failed { kind: Unfulfillable, .. }`. The
/// `reason` field carries the inner `BoundedString` body verbatim
/// (stored as `String` in the in-memory ledger).
#[test]
fn task_failed_with_unfulfillable_lands_in_unfulfillable_variant() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_task("h"),
    });
    assert_eq!(
        s.apply(ClusterMutation::TaskFailed {
            hash: "h".into(),
            kind: ErrorType::Unfulfillable {
                reason: "missing toolchain xyz".to_string().into(),
            },
            error: "unfulfillable".into(),
        }),
        ApplyOutcome::Applied
    );
    match s.task_state("h") {
        Some(TaskState::Unfulfillable { reason, .. }) => {
            assert_eq!(reason, "missing toolchain xyz");
        }
        other => panic!("expected Unfulfillable, got {other:?}"),
    }
}

/// Regression pin for the dispatcher in the `TaskFailed` apply
/// arm: generic non-recoverable errors must still land in
/// `TaskState::Failed`, NOT in `Unfulfillable`. Pins that the
/// kind-based routing only fires for `Unfulfillable` and every
/// other `ErrorType` keeps the legacy shape.
#[test]
fn task_failed_with_generic_nonrecoverable_lands_in_failed_variant() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_task("h"),
    });
    s.apply(ClusterMutation::TaskFailed {
        hash: "h".into(),
        kind: ErrorType::NonRecoverable,
        error: "panic".into(),
    });
    assert!(matches!(
        s.task_state("h"),
        Some(TaskState::Failed { kind: ErrorType::NonRecoverable, .. })
    ));
    // And Recoverable also stays in Failed (sanity check the
    // dispatcher routes ONLY Unfulfillable to the new variant).
    let mut s2 = ClusterState::<RunnerIdentifier>::new();
    s2.apply(ClusterMutation::TaskAdded {
        hash: "h2".into(),
        task: mk_task("h2"),
    });
    s2.apply(ClusterMutation::TaskFailed {
        hash: "h2".into(),
        kind: ErrorType::Recoverable,
        error: "transient".into(),
    });
    assert!(matches!(
        s2.task_state("h2"),
        Some(TaskState::Failed { kind: ErrorType::Recoverable, .. })
    ));
}

/// `ClusterMutation::TaskBlocked { hash, on }` lands a `Pending`
/// entry in `TaskState::Blocked { on, task }`. Pins the cascade
/// broadcast shape: dependents of an Unfulfillable prereq mirror
/// across every replica as Blocked (not Failed), carrying the
/// prereq's hash so auto-resume can identify them.
#[test]
fn cascade_on_unfulfillable_marks_dependents_blocked() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    // Prereq enters Unfulfillable.
    s.apply(ClusterMutation::TaskAdded {
        hash: "prereq".into(),
        task: mk_task("prereq"),
    });
    s.apply(ClusterMutation::TaskFailed {
        hash: "prereq".into(),
        kind: ErrorType::Unfulfillable {
            reason: "missing".to_string().into(),
        },
        error: "missing".into(),
    });
    // Dependent enters Blocked-on-prereq via cascade broadcast.
    s.apply(ClusterMutation::TaskAdded {
        hash: "dep".into(),
        task: mk_task("dep"),
    });
    assert_eq!(
        s.apply(ClusterMutation::TaskBlocked {
            hash: "dep".into(),
            on: "prereq".into(),
        }),
        ApplyOutcome::Applied
    );
    match s.task_state("dep") {
        Some(TaskState::Blocked { on, .. }) => assert_eq!(on, "prereq"),
        other => panic!("expected Blocked, got {other:?}"),
    }
    // Re-apply against an already-Blocked entry with the same
    // `on` is a silent NoOp (idempotent under at-least-once
    // delivery).
    assert_eq!(
        s.apply(ClusterMutation::TaskBlocked {
            hash: "dep".into(),
            on: "prereq".into(),
        }),
        ApplyOutcome::NoOp
    );
}


/// `TaskCompleted` apply arm auto-resumes every Blocked dependent
/// whose `on` matches the completing hash back to `Pending`.
/// Event-driven: the same broadcast that converges the prereq to
/// Completed converges every blocked dependent to Pending in one
/// apply call across every replica.
#[test]
fn task_completed_auto_resumes_blocked_dependents() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    // Prereq landed Unfulfillable then was reinjected (Unfulfillable→Pending).
    s.apply(ClusterMutation::TaskAdded {
        hash: "prereq".into(),
        task: mk_task("prereq"),
    });
    s.apply(ClusterMutation::TaskFailed {
        hash: "prereq".into(),
        kind: ErrorType::Unfulfillable {
            reason: "missing".to_string().into(),
        },
        error: "missing".into(),
    });
    s.apply(ClusterMutation::TaskReinjected { hash: "prereq".into() });
    // Two dependents Blocked-on-prereq.
    for h in ["d1", "d2"] {
        s.apply(ClusterMutation::TaskAdded {
            hash: h.into(),
            task: mk_task(h),
        });
        s.apply(ClusterMutation::TaskBlocked {
            hash: h.into(),
            on: "prereq".into(),
        });
    }
    // An unrelated Blocked-on-other-prereq dependent must NOT auto-resume.
    s.apply(ClusterMutation::TaskAdded {
        hash: "unrelated".into(),
        task: mk_task("unrelated"),
    });
    s.apply(ClusterMutation::TaskBlocked {
        hash: "unrelated".into(),
        on: "some-other-prereq".into(),
    });
    // Prereq completes — every Blocked-on-prereq entry resumes.
    assert_eq!(
        s.apply(ClusterMutation::TaskCompleted { hash: "prereq".into() }),
        ApplyOutcome::Applied
    );
    assert!(matches!(
        s.task_state("d1"),
        Some(TaskState::Pending { .. })
    ));
    assert!(matches!(
        s.task_state("d2"),
        Some(TaskState::Pending { .. })
    ));
    // Unrelated stays Blocked — the auto-resume keys on the `on`
    // field, not blanket-resumes every Blocked entry.
    assert!(matches!(
        s.task_state("unrelated"),
        Some(TaskState::Blocked { .. })
    ));
}

/// `TaskReinjected` apply rule tightening: post-variant, only
/// `TaskState::Unfulfillable { .. }` transitions to `Pending`.
/// Other states (including the legacy `Failed { NonRecoverable, .. }`
/// the pre-variant matcher accepted) are NoOp.
#[test]
fn reinject_task_command_filters_to_unfulfillable_only() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    // Unfulfillable → Pending: accepted.
    s.apply(ClusterMutation::TaskAdded {
        hash: "u".into(),
        task: mk_task("u"),
    });
    s.apply(ClusterMutation::TaskFailed {
        hash: "u".into(),
        kind: ErrorType::Unfulfillable {
            reason: "missing".to_string().into(),
        },
        error: "missing".into(),
    });
    assert_eq!(
        s.apply(ClusterMutation::TaskReinjected { hash: "u".into() }),
        ApplyOutcome::Applied
    );
    assert!(matches!(
        s.task_state("u"),
        Some(TaskState::Pending { .. })
    ));

    // Failed{NonRecoverable} → reinject: NoOp (pre-variant
    // matcher accepted this; the tightened rule rejects).
    s.apply(ClusterMutation::TaskAdded {
        hash: "f".into(),
        task: mk_task("f"),
    });
    s.apply(ClusterMutation::TaskFailed {
        hash: "f".into(),
        kind: ErrorType::NonRecoverable,
        error: "panic".into(),
    });
    assert_eq!(
        s.apply(ClusterMutation::TaskReinjected { hash: "f".into() }),
        ApplyOutcome::NoOp
    );
    assert!(matches!(
        s.task_state("f"),
        Some(TaskState::Failed { .. })
    ));
}
