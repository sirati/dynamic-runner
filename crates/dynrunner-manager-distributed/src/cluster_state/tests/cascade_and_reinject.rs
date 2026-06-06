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
            // Strictly above the task's initial preferred_version (0,0)
            // so the update wins (the choke point stamps this in prod).
            version: TaskVersion {
                primary_epoch: 1,
                seq: 0,
            },
        }),
        ApplyOutcome::Applied
    );
    let Some(TaskState::Pending { task, .. }) = s.task_state("h") else {
        panic!("expected Pending");
    };
    assert_eq!(
        task.preferred_secondaries.as_slice(),
        &["secondary-2", "secondary-5"]
    );
}

#[test]
fn task_preferred_secondaries_updated_apply_unknown_hash_is_noop() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    assert_eq!(
        s.apply(ClusterMutation::TaskPreferredSecondariesUpdated {
            hash: "nope".into(),
            secondaries: vec!["secondary-1".into()],
            version: TaskVersion {
                primary_epoch: 1,
                seq: 0,
            },
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
        attempt: 0,
        hash: "h".into(),
        kind: ErrorType::Unfulfillable {
            reason: "missing".to_string().into(),
        },
        error: "missing".into(),
        version: Default::default(),
    });
    assert_eq!(
        s.apply(ClusterMutation::TaskPreferredSecondariesUpdated {
            hash: "h".into(),
            secondaries: vec!["secondary-7".into()],
            version: TaskVersion {
                primary_epoch: 1,
                seq: 0,
            },
        }),
        ApplyOutcome::Applied
    );
    let Some(TaskState::Unfulfillable { task, reason, .. }) = s.task_state("h") else {
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
            attempt: 0,
            hash: "h".into(),
            kind: ErrorType::Unfulfillable {
                reason: "missing toolchain xyz".to_string().into(),
            },
            error: "unfulfillable".into(),
            version: Default::default(),
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
        attempt: 0,
        hash: "h".into(),
        kind: ErrorType::NonRecoverable,
        error: "panic".into(),
        version: Default::default(),
    });
    assert!(matches!(
        s.task_state("h"),
        Some(TaskState::Failed {
            kind: ErrorType::NonRecoverable,
            ..
        })
    ));
    // And Recoverable also stays in Failed (sanity check the
    // dispatcher routes ONLY Unfulfillable to the new variant).
    let mut s2 = ClusterState::<RunnerIdentifier>::new();
    s2.apply(ClusterMutation::TaskAdded {
        hash: "h2".into(),
        task: mk_task("h2"),
    });
    s2.apply(ClusterMutation::TaskFailed {
        attempt: 0,
        hash: "h2".into(),
        kind: ErrorType::Recoverable,
        error: "transient".into(),
        version: Default::default(),
    });
    assert!(matches!(
        s2.task_state("h2"),
        Some(TaskState::Failed {
            kind: ErrorType::Recoverable,
            ..
        })
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
        attempt: 0,
        hash: "prereq".into(),
        kind: ErrorType::Unfulfillable {
            reason: "missing".to_string().into(),
        },
        error: "missing".into(),
        version: Default::default(),
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

/// A fresh `TasksSpawned` whose dep is an EXISTING `InvalidTask`
/// cascade-fails as `Failed { NonRecoverable, last_error:
/// "upstream-failed" }` — NOT as a fresh `invalid_task`. This keeps the
/// `invalid_task` reason space accurate: only a literally-absent dep
/// mints a fresh `InvalidTask`; an existing-but-invalid dep cascades
/// through the same `Failed { NonRecoverable }` shape as any other
/// upstream terminal. (The dep classifier in `apply_tasks_spawned`.)
#[test]
fn spawned_dep_on_existing_invalid_task_cascades_as_non_recoverable() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    // Prereq `x` exists in the ledger and is InvalidTask.
    s.apply(ClusterMutation::TaskAdded {
        hash: "x_hash".into(),
        task: mk_task("x"),
    });
    s.apply(ClusterMutation::TaskFailed {
        attempt: 0,
        hash: "x_hash".into(),
        kind: ErrorType::InvalidTask {
            reason: "missing dep".to_string().into(),
        },
        error: "missing dependency".into(),
        version: Default::default(),
    });
    assert_eq!(s.counts().invalid_task, 1);

    // Spawn a dependent V naming (p0, x) — the classifier resolves the
    // dep to x's ledger hash, sees InvalidTask, cascade-fails V.
    let mut v = mk_task("v");
    v.task_depends_on = vec![dynrunner_core::TaskDep {
        task_id: "x".into(),
        phase_id: PhaseId::from("p0"),
        inherit_outputs: false,
    }];
    let v_hash = crate::primary::wire::compute_task_hash(&v);
    s.apply(ClusterMutation::TasksSpawned { tasks: vec![v] });

    match s.task_state(&v_hash) {
        Some(TaskState::Failed {
            kind, last_error, ..
        }) => {
            assert_eq!(
                *kind,
                ErrorType::NonRecoverable,
                "cascades as NonRecoverable"
            );
            assert_eq!(
                last_error, "upstream-failed",
                "upstream-invalid cascade shape"
            );
        }
        other => panic!("expected Failed{{NonRecoverable}} cascade, got {other:?}"),
    }
    // The dependent did NOT become a fresh invalid_task — the count is
    // still just the original prereq.
    assert_eq!(
        s.counts().invalid_task,
        1,
        "the dependent cascaded as NonRecoverable, NOT a fresh invalid_task"
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
        attempt: 0,
        hash: "prereq".into(),
        kind: ErrorType::Unfulfillable {
            reason: "missing".to_string().into(),
        },
        error: "missing".into(),
        version: Default::default(),
    });
    s.apply(ClusterMutation::TaskReinjected {
        hash: "prereq".into(),
        version: Default::default(),
    });
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
        s.apply(ClusterMutation::TaskCompleted {
            attempt: 0,
            hash: "prereq".into(),
            result_data: None
        }),
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
        attempt: 0,
        hash: "u".into(),
        kind: ErrorType::Unfulfillable {
            reason: "missing".to_string().into(),
        },
        error: "missing".into(),
        version: Default::default(),
    });
    assert_eq!(
        s.apply(ClusterMutation::TaskReinjected {
            hash: "u".into(),
            version: Default::default()
        }),
        ApplyOutcome::Applied
    );
    assert!(matches!(s.task_state("u"), Some(TaskState::Pending { .. })));

    // Failed{NonRecoverable} → reinject: NoOp (pre-variant
    // matcher accepted this; the tightened rule rejects).
    s.apply(ClusterMutation::TaskAdded {
        hash: "f".into(),
        task: mk_task("f"),
    });
    s.apply(ClusterMutation::TaskFailed {
        attempt: 0,
        hash: "f".into(),
        kind: ErrorType::NonRecoverable,
        error: "panic".into(),
        version: Default::default(),
    });
    assert_eq!(
        s.apply(ClusterMutation::TaskReinjected {
            hash: "f".into(),
            version: Default::default()
        }),
        ApplyOutcome::NoOp
    );
    assert!(matches!(s.task_state("f"), Some(TaskState::Failed { .. })));
}

// ── Dead-secondary requeue (`TaskRequeued`, InFlight → Pending) ──

/// `ClusterMutation::TaskRequeued { hash }` transitions an `InFlight`
/// entry back to `Pending`, preserving the `TaskInfo` so the requeued
/// task re-dispatches the same binary. This is the CRDT half of
/// dead-secondary recovery: the local pool requeue and this transition
/// move in lockstep so no stale `InFlight` survives.
#[test]
fn task_requeued_transitions_in_flight_back_to_pending() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_task("h"),
    });
    s.apply(ClusterMutation::TaskAssigned {
        attempt: 0,
        hash: "h".into(),
        secondary: "dead-sec".into(),
        worker: 0,
        version: Default::default(),
    });
    assert!(matches!(
        s.task_state("h"),
        Some(TaskState::InFlight { .. })
    ));
    assert_eq!(
        s.apply(ClusterMutation::TaskRequeued {
            hash: "h".into(),
            version: Default::default()
        }),
        ApplyOutcome::Applied
    );
    let Some(TaskState::Pending { task, .. }) = s.task_state("h") else {
        panic!("InFlight must requeue to Pending");
    };
    assert_eq!(
        task.task_id, "h",
        "the preserved TaskInfo re-dispatches the same task"
    );
}

/// `TaskRequeued` is a NoOp against every non-`InFlight` state:
///   * `Pending` — idempotent under at-least-once delivery;
///   * `Blocked` — a cascade-pause, not a dispatched task;
///   * terminals (`Completed` / `Failed` / `Unfulfillable` /
///     `InvalidTask`) — a terminal that raced the death observation
///     wins; the requeue must NOT resurrect it to `Pending`.
#[test]
fn task_requeued_is_noop_against_non_in_flight_states() {
    // Unknown hash.
    let mut s = ClusterState::<RunnerIdentifier>::new();
    assert_eq!(
        s.apply(ClusterMutation::TaskRequeued {
            hash: "nope".into(),
            version: Default::default(),
        }),
        ApplyOutcome::NoOp
    );
    // Pending (idempotent).
    s.apply(ClusterMutation::TaskAdded {
        hash: "p".into(),
        task: mk_task("p"),
    });
    assert_eq!(
        s.apply(ClusterMutation::TaskRequeued {
            hash: "p".into(),
            version: Default::default()
        }),
        ApplyOutcome::NoOp
    );
    assert!(matches!(s.task_state("p"), Some(TaskState::Pending { .. })));
    // Completed terminal wins (the Complete-before-Requeue reorder).
    s.apply(ClusterMutation::TaskAdded {
        hash: "c".into(),
        task: mk_task("c"),
    });
    s.apply(ClusterMutation::TaskAssigned {
        attempt: 0,
        hash: "c".into(),
        secondary: "dead-sec".into(),
        worker: 0,
        version: Default::default(),
    });
    s.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
        hash: "c".into(),
        result_data: None,
    });
    assert_eq!(
        s.apply(ClusterMutation::TaskRequeued {
            hash: "c".into(),
            version: Default::default()
        }),
        ApplyOutcome::NoOp,
        "a completion that raced the death observation must win"
    );
    assert!(matches!(
        s.task_state("c"),
        Some(TaskState::Completed { .. })
    ));
    // InvalidTask terminal also wins.
    s.apply(ClusterMutation::TaskAdded {
        hash: "i".into(),
        task: mk_task("i"),
    });
    s.apply(ClusterMutation::TaskFailed {
        attempt: 0,
        hash: "i".into(),
        kind: ErrorType::InvalidTask {
            reason: "dup".to_string().into(),
        },
        error: "invalid_task:dup".into(),
        version: Default::default(),
    });
    assert_eq!(
        s.apply(ClusterMutation::TaskRequeued {
            hash: "i".into(),
            version: Default::default()
        }),
        ApplyOutcome::NoOp
    );
    assert!(matches!(
        s.task_state("i"),
        Some(TaskState::InvalidTask { .. })
    ));
}

// ── Discrete InvalidTask state pins ──

/// `TaskFailed { kind: ErrorType::InvalidTask, .. }` lands in the
/// discrete `TaskState::InvalidTask { reason, task }` variant, NOT in
/// `TaskState::Failed { kind: InvalidTask, .. }`. Mirrors the
/// `Unfulfillable` routing pin; the `reason` carries the inner
/// `BoundedString` body verbatim.
#[test]
fn task_failed_with_invalid_task_lands_in_invalid_task_variant() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_task("h"),
    });
    assert_eq!(
        s.apply(ClusterMutation::TaskFailed {
            attempt: 0,
            hash: "h".into(),
            kind: ErrorType::InvalidTask {
                reason: "missing dep nope".to_string().into(),
            },
            error: "invalid_task:missing dep nope".into(),
            version: Default::default(),
        }),
        ApplyOutcome::Applied
    );
    match s.task_state("h") {
        Some(TaskState::InvalidTask { reason, .. }) => {
            assert_eq!(reason, "missing dep nope");
        }
        other => panic!("expected InvalidTask, got {other:?}"),
    }
}

/// `InvalidTask` is a TERMINAL, NON-reinjectable lockout. Unlike
/// `Unfulfillable` (which `TaskReinjected` lifts back to `Pending`),
/// a `TaskReinjected` against an `InvalidTask` entry is a NoOp and the
/// state stays `InvalidTask` — there is no external action that makes
/// a structurally-invalid task valid.
#[test]
fn reinject_against_invalid_task_is_noop_non_reinjectable() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_task("h"),
    });
    s.apply(ClusterMutation::TaskFailed {
        attempt: 0,
        hash: "h".into(),
        kind: ErrorType::InvalidTask {
            reason: "dup id".to_string().into(),
        },
        error: "invalid_task:dup id".into(),
        version: Default::default(),
    });
    assert_eq!(
        s.apply(ClusterMutation::TaskReinjected {
            hash: "h".into(),
            version: Default::default()
        }),
        ApplyOutcome::NoOp,
        "InvalidTask is non-reinjectable; ReinjectTask must NoOp"
    );
    assert!(
        matches!(s.task_state("h"), Some(TaskState::InvalidTask { .. })),
        "state must stay InvalidTask after a rejected reinject"
    );
}

/// Terminal lockout: a late generic `TaskFailed` and a spurious
/// `TaskCompleted` must NOT overwrite a terminal `InvalidTask` entry.
/// The discrete reason stays accurate across out-of-order delivery.
#[test]
fn invalid_task_terminal_lockout_blocks_late_failed_and_completed() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_task("h"),
    });
    s.apply(ClusterMutation::TaskFailed {
        attempt: 0,
        hash: "h".into(),
        kind: ErrorType::InvalidTask {
            reason: "missing dep".to_string().into(),
        },
        error: "invalid_task:missing dep".into(),
        version: Default::default(),
    });
    // A late generic worker-originated TaskFailed must NoOp.
    assert_eq!(
        s.apply(ClusterMutation::TaskFailed {
            attempt: 0,
            hash: "h".into(),
            kind: ErrorType::NonRecoverable,
            error: "panic".into(),
            version: Default::default(),
        }),
        ApplyOutcome::NoOp
    );
    // A spurious TaskCompleted must NoOp (an invalid task is never
    // dispatched, so success is impossible by construction).
    assert_eq!(
        s.apply(ClusterMutation::TaskCompleted {
            attempt: 0,
            hash: "h".into(),
            result_data: None,
        }),
        ApplyOutcome::NoOp
    );
    match s.task_state("h") {
        Some(TaskState::InvalidTask { reason, .. }) => {
            assert_eq!(reason, "missing dep");
        }
        other => panic!("expected InvalidTask preserved, got {other:?}"),
    }
}
