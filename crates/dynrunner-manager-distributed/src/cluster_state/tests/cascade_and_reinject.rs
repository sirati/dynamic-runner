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
        def_id: None,
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
    let Some(state @ TaskState::Pending { .. }) = s.task_state("h") else {
        panic!("expected Pending");
    };
    assert_eq!(
        state.routing().preferred_secondaries.as_slice(),
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
        def_id: None,
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
    let Some(state @ TaskState::Unfulfillable { reason, .. }) = s.task_state("h") else {
        panic!("state must stay Unfulfillable across preferred-secondaries update");
    };
    assert_eq!(reason, "missing");
    assert_eq!(state.routing().preferred_secondaries.as_slice(), &["secondary-7"]);
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
        def_id: None,
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
        def_id: None,
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
        def_id: None,
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
        def_id: None,
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
        def_id: None,
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
        def_id: None,
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
        def_id: None,
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
            def_id: None,
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
        def_id: None,
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
        def_id: None,
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
        def_id: None,
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
        def_id: None,
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
    let Some(state @ TaskState::Pending { .. }) = s.task_state("h") else {
        panic!("InFlight must requeue to Pending");
    };
    assert_eq!(
        state.def().task_id, "h",
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
        def_id: None,
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
        def_id: None,
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
        def_id: None,
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
        def_id: None,
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
        def_id: None,
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
        def_id: None,
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

// ── Runtime cascade-FAIL of blocked dependents (#527) ──
//
// The failure-analogue of the `TaskCompleted` auto-resume above: a task
// that transitions to a NEVER-PRODUCED-OUTPUT terminal at RUNTIME
// (NonRecoverable / InvalidTask) must cascade-fail every dependent sitting
// `Blocked { on: <its hash> }`, transitively, rather than strand them
// `Blocked` forever (the consumer's 1,314-task drain-guard fire).

/// A task in a chosen phase (the cross-phase drain scenario needs the
/// dependent in a later phase than its prereq). `mk_task` hardwires "p0".
fn mk_task_in(name: &str, phase: &str) -> TaskInfo<RunnerIdentifier> {
    let mut t = mk_task(name);
    t.phase_id = PhaseId::from(phase);
    t
}

/// HEADLINE + revert-confirm: `b` Blocked-on-`a`; `a` fails NonRecoverable
/// at runtime → `b` is terminally resolved to `Failed { NonRecoverable,
/// "upstream-failed" }`, leaves the `Blocked` count, and is ACCOUNTED in
/// `outcome_counts().fail_final`.
///
/// REVERT-CONFIRM: without the `cascade_fail_blocked_dependents` call in
/// `merge_task_state`, `b` stays `Blocked` (the strand) — `counts().blocked`
/// stays 1 and `fail_final` counts only `a` — so the assertions below FAIL,
/// reproducing the production strand.
#[test]
fn cascade_fail_resolves_direct_blocked_dependent() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "a".into(),
        task: mk_task("a"),
        def_id: None,
    });
    s.apply(ClusterMutation::TaskAdded {
        hash: "b".into(),
        task: mk_task("b"),
        def_id: None,
    });
    s.apply(ClusterMutation::TaskBlocked {
        hash: "b".into(),
        on: "a".into(),
    });
    assert_eq!(s.counts().blocked, 1, "precondition: b is Blocked-on-a");

    // a fails terminally (never-produced output).
    assert_eq!(
        s.apply(ClusterMutation::TaskFailed {
            attempt: 0,
            hash: "a".into(),
            kind: ErrorType::NonRecoverable,
            error: "boom".into(),
            version: Default::default(),
        }),
        ApplyOutcome::Applied
    );

    // b cascade-failed to the canonical upstream-failed shape.
    match s.task_state("b") {
        Some(TaskState::Failed {
            kind, last_error, ..
        }) => {
            assert_eq!(*kind, ErrorType::NonRecoverable);
            assert_eq!(last_error, "upstream-failed");
        }
        other => panic!("expected b cascade-failed, got {other:?}"),
    }
    // b left the Blocked set; both a and b are accounted as terminal failures.
    assert_eq!(s.counts().blocked, 0, "b is no longer stranded Blocked");
    assert_eq!(
        s.outcome_counts().fail_final,
        2,
        "both a and the cascaded b are accounted as fail_final"
    );
}

/// TRANSITIVE a → b → c: a fails → b cascade-fails → c (Blocked-on-b)
/// ALSO cascade-fails. The transitive flood rides the per-dependent
/// `merge_task_state` recursion.
#[test]
fn cascade_fail_is_transitive() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    for h in ["a", "b", "c"] {
        s.apply(ClusterMutation::TaskAdded {
            hash: h.into(),
            task: mk_task(h),
            def_id: None,
        });
    }
    s.apply(ClusterMutation::TaskBlocked {
        hash: "b".into(),
        on: "a".into(),
    });
    s.apply(ClusterMutation::TaskBlocked {
        hash: "c".into(),
        on: "b".into(),
    });

    s.apply(ClusterMutation::TaskFailed {
        attempt: 0,
        hash: "a".into(),
        kind: ErrorType::NonRecoverable,
        error: "boom".into(),
        version: Default::default(),
    });

    for h in ["b", "c"] {
        match s.task_state(h) {
            Some(TaskState::Failed {
                kind, last_error, ..
            }) => {
                assert_eq!(*kind, ErrorType::NonRecoverable);
                assert_eq!(last_error, "upstream-failed");
            }
            other => panic!("expected {h} cascade-failed, got {other:?}"),
        }
    }
    assert_eq!(s.counts().blocked, 0);
    assert_eq!(s.outcome_counts().fail_final, 3, "a, b, c all fail_final");
}

/// DIAMOND: `c` depends on `{a, b}` (Blocked-on-a; b still Pending). `a`
/// fails → `c` cascade-fails even though `b` is fine — ONE failed dep
/// suffices (an all-deps task with any never-produced prereq is itself
/// unfulfillable). `b` is untouched.
///
/// `c` is Blocked on the FIRST-unresolved dep (`a`) per the
/// `apply_tasks_spawned` classifier's `on` rule; this models that shape.
#[test]
fn cascade_fail_one_failed_dep_suffices_diamond() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    for h in ["a", "b", "c"] {
        s.apply(ClusterMutation::TaskAdded {
            hash: h.into(),
            task: mk_task(h),
            def_id: None,
        });
    }
    // c is blocked on a (the first unresolved dep); b stays Pending.
    s.apply(ClusterMutation::TaskBlocked {
        hash: "c".into(),
        on: "a".into(),
    });

    s.apply(ClusterMutation::TaskFailed {
        attempt: 0,
        hash: "a".into(),
        kind: ErrorType::NonRecoverable,
        error: "boom".into(),
        version: Default::default(),
    });

    assert!(
        matches!(
            s.task_state("c"),
            Some(TaskState::Failed {
                kind: ErrorType::NonRecoverable,
                ..
            })
        ),
        "c cascade-fails on its single failed dep a, despite b being live"
    );
    // b — a healthy sibling, never a dependent of a — is untouched.
    assert!(
        matches!(s.task_state("b"), Some(TaskState::Pending { .. })),
        "b is unrelated to a's failure and stays Pending"
    );
}

/// InvalidTask is a never-produced-output terminal too → it cascades a
/// Blocked dependent (matching the spawn-time classifier's InvalidTask arm).
#[test]
fn cascade_fail_fires_on_invalid_task_prereq() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "a".into(),
        task: mk_task("a"),
        def_id: None,
    });
    s.apply(ClusterMutation::TaskAdded {
        hash: "b".into(),
        task: mk_task("b"),
        def_id: None,
    });
    s.apply(ClusterMutation::TaskBlocked {
        hash: "b".into(),
        on: "a".into(),
    });
    s.apply(ClusterMutation::TaskFailed {
        attempt: 0,
        hash: "a".into(),
        kind: ErrorType::InvalidTask {
            reason: "structurally invalid".to_string().into(),
        },
        error: "invalid".into(),
        version: Default::default(),
    });
    assert!(
        matches!(
            s.task_state("b"),
            Some(TaskState::Failed {
                kind: ErrorType::NonRecoverable,
                ..
            })
        ),
        "an InvalidTask prereq cascade-fails its blocked dependent"
    );
}

/// RECOVERABLE failure (retry-eligible) does NOT cascade: the dependent
/// stays Blocked, correctly, because the prereq's retry pass may yet
/// succeed and produce the output. (The drain-edge `finalize_soft_failures`
/// owns the cascade IF the retry buckets later decline.)
#[test]
fn recoverable_fail_does_not_cascade() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "a".into(),
        task: mk_task("a"),
        def_id: None,
    });
    s.apply(ClusterMutation::TaskAdded {
        hash: "b".into(),
        task: mk_task("b"),
        def_id: None,
    });
    s.apply(ClusterMutation::TaskBlocked {
        hash: "b".into(),
        on: "a".into(),
    });
    s.apply(ClusterMutation::TaskFailed {
        attempt: 0,
        hash: "a".into(),
        kind: ErrorType::Recoverable,
        error: "transient".into(),
        version: Default::default(),
    });
    assert!(
        matches!(s.task_state("b"), Some(TaskState::Blocked { .. })),
        "a recoverable (retry-eligible) failure must NOT cascade — b stays Blocked"
    );
}

/// UNFULFILLABLE (operator-reinjectable) does NOT cascade: the dependent
/// stays Blocked, awaiting the reinject + complete path — the deliberate
/// cascade-PAUSE contract (`apply_fail_permanent`'s Unfulfillable split).
/// Pins that the tighter `post_is_cascade_terminal` predicate excludes
/// `Unfulfillable` even though it is a `failure_won` terminal.
#[test]
fn unfulfillable_fail_does_not_cascade() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "a".into(),
        task: mk_task("a"),
        def_id: None,
    });
    s.apply(ClusterMutation::TaskAdded {
        hash: "b".into(),
        task: mk_task("b"),
        def_id: None,
    });
    s.apply(ClusterMutation::TaskBlocked {
        hash: "b".into(),
        on: "a".into(),
    });
    s.apply(ClusterMutation::TaskFailed {
        attempt: 0,
        hash: "a".into(),
        kind: ErrorType::Unfulfillable {
            reason: "missing".to_string().into(),
        },
        error: "missing".into(),
        version: Default::default(),
    });
    assert!(
        matches!(s.task_state("b"), Some(TaskState::Blocked { .. })),
        "an Unfulfillable (reinjectable) prereq must NOT cascade — b stays Blocked"
    );
}

/// THE DRAIN SCENARIO (reproduces the consumer's 1,314-stranded drain-guard
/// fire): the prereq `a` lives in phase `p0`; its dependent `b` lives in a
/// LATER phase `p1`, Blocked-on-`a`. When `a` fails NonRecoverable, `b`
/// cascade-fails — so phase `p1`'s rollup reports `has_live == false` (every
/// task terminal) and the phase drains CLEANLY, no stranded live `Blocked`
/// dependent to trip the per-phase drain-guard into a spurious RunShouldFail.
///
/// REVERT-CONFIRM: without the cascade, `b` stays `Blocked` (non-terminal →
/// `has_live == true`), so `p1` never reaches a terminal outcome and the
/// drain-guard authors the false run failure — the assertion below FAILS.
#[test]
fn cascade_fail_drains_dependent_phase_cleanly() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    // a in p0; b in p1 depends on a (cross-phase, the production shape).
    s.apply(ClusterMutation::TaskAdded {
        hash: "a".into(),
        task: mk_task_in("a", "p0"),
        def_id: None,
    });
    s.apply(ClusterMutation::TaskAdded {
        hash: "b".into(),
        task: mk_task_in("b", "p1"),
        def_id: None,
    });
    s.apply(ClusterMutation::TaskBlocked {
        hash: "b".into(),
        on: "a".into(),
    });

    // Before the failure: p1 holds a live (Blocked) dependent.
    {
        let rollups = s.phase_rollups();
        let p1 = PhaseId::from("p1");
        let r = rollups.get(&p1).expect("p1 present");
        assert!(r.has_any && r.has_live, "p1 is live before a's failure");
    }

    s.apply(ClusterMutation::TaskFailed {
        attempt: 0,
        hash: "a".into(),
        kind: ErrorType::NonRecoverable,
        error: "boom".into(),
        version: Default::default(),
    });

    // After the cascade: p1 drains cleanly — no live work remains.
    let rollups = s.phase_rollups();
    let p1 = PhaseId::from("p1");
    let r = rollups.get(&p1).expect("p1 present");
    assert!(
        r.has_any && !r.has_live,
        "p1 drains cleanly after the cascade — b is terminal, not stranded Blocked"
    );
    // And both phases' failures are accounted (no vanished dependent).
    assert_eq!(s.outcome_counts().fail_final, 2);
}

/// The cascaded dependent surfaces a terminal-completion EVENT (so consumer
/// dedup buckets + the demoted-primary narration see the failure), built
/// from the canonical upstream-failed shape via the shared
/// `to_completed_event` projection.
#[test]
fn cascade_fail_emits_dependent_completion_event() {
    use crate::task_completed::TaskCompletedEvent;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<TaskCompletedEvent>();
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.install_task_completed_sender(tx);
    s.apply(ClusterMutation::TaskAdded {
        hash: "a".into(),
        task: mk_task("a"),
        def_id: None,
    });
    s.apply(ClusterMutation::TaskAdded {
        hash: "b".into(),
        task: mk_task("b"),
        def_id: None,
    });
    s.apply(ClusterMutation::TaskBlocked {
        hash: "b".into(),
        on: "a".into(),
    });
    s.apply(ClusterMutation::TaskFailed {
        attempt: 0,
        hash: "a".into(),
        kind: ErrorType::NonRecoverable,
        error: "boom".into(),
        version: Default::default(),
    });

    let mut events = Vec::new();
    while let Ok(ev) = rx.try_recv() {
        events.push(ev);
    }
    // Both the prereq AND the cascaded dependent emit a failure event.
    assert!(
        events.iter().any(|e| e.task_id == "a" && !e.success),
        "prereq a emits a failure event: {events:?}"
    );
    assert!(
        events.iter().any(|e| e.task_id == "b" && !e.success),
        "cascaded dependent b emits a failure event: {events:?}"
    );
}
