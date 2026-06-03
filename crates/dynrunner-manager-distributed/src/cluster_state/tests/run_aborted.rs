//! `ClusterMutation::RunAborted` apply semantics + the `invalid_task`
//! CRDT transitions the manager-side 3a/3b/#2 paths rely on.
//!
//! Pins:
//!   * `RunAborted` sets the sticky `run_aborted()` accessor (`Some`),
//!     is `Applied` once and `NoOp` on re-application (failure twin of
//!     `RunComplete`);
//!   * a `TaskFailed { kind: InvalidTask }` against a `Pending` entry
//!     transitions it to `InvalidTask` and fans the wire-tagged kind —
//!     the #2 / 3b emission shape;
//!   * the `InvalidTask` terminal locks out a later generic
//!     `TaskFailed` (the reason stays accurate, so an upstream-invalid
//!     dependent that cascades as `NonRecoverable` does NOT overwrite a
//!     genuine `invalid_task`).

use super::*;
use dynrunner_core::BoundedString;

#[test]
fn run_aborted_sets_accessor_and_is_idempotent() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    assert!(s.run_aborted().is_none(), "fresh state is not aborted");
    assert_eq!(
        s.apply(ClusterMutation::RunAborted {
            reason: "dup task identity in initial batch".into(),
        }),
        ApplyOutcome::Applied
    );
    assert_eq!(
        s.run_aborted(),
        Some("dup task identity in initial batch"),
        "run_aborted() carries the abort reason"
    );
    // Sticky monotonic: a second (or differently-worded) RunAborted is
    // a NoOp and never churns the latched reason.
    assert_eq!(
        s.apply(ClusterMutation::RunAborted {
            reason: "a different reason".into(),
        }),
        ApplyOutcome::NoOp
    );
    assert_eq!(s.run_aborted(), Some("dup task identity in initial batch"));
}

#[test]
fn run_aborted_independent_of_run_complete() {
    // The two flags are orthogonal twins — one does not imply the other.
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::RunComplete);
    assert!(s.run_complete());
    assert!(s.run_aborted().is_none());

    let mut s2 = ClusterState::<RunnerIdentifier>::new();
    s2.apply(ClusterMutation::RunAborted { reason: "x".into() });
    assert!(s2.run_aborted().is_some());
    assert!(!s2.run_complete());
}

#[test]
fn task_failed_invalid_task_transitions_pending_to_invalid() {
    // The #2 missing-dep / 3b run-wide emission shape: a Pending entry
    // hit with `TaskFailed { kind: InvalidTask }` becomes InvalidTask.
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_task("a"),
    });
    assert_eq!(s.counts().pending, 1);
    assert_eq!(
        s.apply(ClusterMutation::TaskFailed {
            hash: "h".into(),
            kind: ErrorType::InvalidTask {
                reason: BoundedString::from("missing dep (phase=p0, task_id=ghost)".to_string()),
            },
            error: "missing dependency".into(),
        }),
        ApplyOutcome::Applied
    );
    assert_eq!(s.counts().invalid_task, 1, "task moved to invalid_task");
    assert_eq!(s.counts().pending, 0);
}

#[test]
fn invalid_task_terminal_locks_out_later_generic_failure() {
    // An existing-but-InvalidTask dep cascades as NonRecoverable
    // "upstream-invalid"; that cascade must NOT overwrite the genuine
    // invalid_task entry — the discrete reason stays accurate. The
    // apply-rule lockout is what guarantees this.
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_task("a"),
    });
    s.apply(ClusterMutation::TaskFailed {
        hash: "h".into(),
        kind: ErrorType::InvalidTask {
            reason: BoundedString::from("missing dep".to_string()),
        },
        error: "missing dependency".into(),
    });
    // A later generic NonRecoverable TaskFailed is locked out.
    assert_eq!(
        s.apply(ClusterMutation::TaskFailed {
            hash: "h".into(),
            kind: ErrorType::NonRecoverable,
            error: "upstream-failed".into(),
        }),
        ApplyOutcome::NoOp
    );
    assert_eq!(s.counts().invalid_task, 1, "still invalid_task");
    assert_eq!(s.counts().failed, 0, "not overwritten to a generic failure");
}
