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
            counts: Default::default(),
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
            counts: Default::default(),
        }),
        ApplyOutcome::NoOp
    );
    assert_eq!(s.run_aborted(), Some("dup task identity in initial batch"));
}

#[test]
fn run_aborted_independent_of_run_complete() {
    // The two flags are orthogonal twins — one does not imply the other.
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::RunComplete { counts: Default::default() });
    assert!(s.run_complete());
    assert!(s.run_aborted().is_none());

    let mut s2 = ClusterState::<RunnerIdentifier>::new();
    s2.apply(ClusterMutation::RunAborted {
        reason: "x".into(),
        counts: Default::default(),
    });
    assert!(s2.run_aborted().is_some());
    assert!(!s2.run_complete());
}

/// #513 — the verdict's carried counts latch ATOMICALLY with the run latch
/// (one mutation), set-once, and a `RunComplete` exposes them via
/// `terminal_outcome()`. This is the property the narrator depends on:
/// observing the latch implies the authoritative counts are in hand.
#[test]
fn run_complete_latches_carried_counts_atomically() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    assert!(
        s.terminal_outcome().is_none(),
        "fresh state carries no verdict counts"
    );
    s.apply(ClusterMutation::RunComplete {
        counts: dynrunner_core::TerminalOutcomeCounts {
            succeeded: 2,
            fail_final: 538,
            ..Default::default()
        },
    });
    assert!(s.run_complete(), "the latch is set");
    let counts = s
        .terminal_outcome()
        .expect("the carried counts latch with the run-complete flag");
    assert_eq!(counts.succeeded, 2);
    assert_eq!(counts.fail_final, 538, "the authoritative fail_final is in hand");
}

/// #513 — first-writer-wins on the carried counts, mirroring `run_aborted`'s
/// sticky reason: a re-applied verdict NoOps and never churns the latched
/// counts.
#[test]
fn terminal_outcome_is_first_writer_wins() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::RunComplete {
        counts: dynrunner_core::TerminalOutcomeCounts {
            fail_final: 538,
            ..Default::default()
        },
    });
    // A second (duplicate / re-broadcast) verdict with DIFFERENT counts must
    // NOT overwrite the first.
    assert_eq!(
        s.apply(ClusterMutation::RunComplete {
            counts: dynrunner_core::TerminalOutcomeCounts {
                fail_final: 0,
                ..Default::default()
            },
        }),
        ApplyOutcome::NoOp,
        "a re-applied RunComplete is a NoOp (sticky latch)"
    );
    assert_eq!(
        s.terminal_outcome().unwrap().fail_final,
        538,
        "the FIRST verdict's counts win; a later one never churns them"
    );
}

#[test]
fn task_failed_invalid_task_transitions_pending_to_invalid() {
    // The #2 missing-dep / 3b run-wide emission shape: a Pending entry
    // hit with `TaskFailed { kind: InvalidTask }` becomes InvalidTask.
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_task("a"),
        def_id: None,
    });
    assert_eq!(s.counts().pending, 1);
    assert_eq!(
        s.apply(ClusterMutation::TaskFailed {
            attempt: 0,
            hash: "h".into(),
            kind: ErrorType::InvalidTask {
                reason: BoundedString::from("missing dep (phase=p0, task_id=ghost)".to_string()),
            },
            error: "missing dependency".into(),
            version: Default::default(),
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
        def_id: None,
    });
    s.apply(ClusterMutation::TaskFailed {
        attempt: 0,
        hash: "h".into(),
        kind: ErrorType::InvalidTask {
            reason: BoundedString::from("missing dep".to_string()),
        },
        error: "missing dependency".into(),
        version: Default::default(),
    });
    // A later generic NonRecoverable TaskFailed is locked out.
    assert_eq!(
        s.apply(ClusterMutation::TaskFailed {
            attempt: 0,
            hash: "h".into(),
            kind: ErrorType::NonRecoverable,
            error: "upstream-failed".into(),
            version: Default::default(),
        }),
        ApplyOutcome::NoOp
    );
    assert_eq!(s.counts().invalid_task, 1, "still invalid_task");
    assert_eq!(s.counts().failed, 0, "not overwritten to a generic failure");
}
