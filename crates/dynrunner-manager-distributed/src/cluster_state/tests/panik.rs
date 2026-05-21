//! Tests for the `ClusterMutation::PanikRequested` apply rule and the
//! `TaskState::Cancelled` variant introduced by the operator-initiated
//! emergency-shutdown feature.
//!
//! Single concern: pin the load-bearing properties downstream code
//! relies on:
//!   - non-terminal entries (`Pending`, `InFlight`, `Blocked`) sweep
//!     to `Cancelled`;
//!   - terminal entries (`Completed`, `Failed`, `Unfulfillable`)
//!     preserve;
//!   - sticky-first-wins under repeated `PanikRequested`;
//!   - late `TaskCompleted` against `Cancelled` succeeds (success is
//!     the strongest terminal);
//!   - late `TaskFailed` / `TaskBlocked` against `Cancelled` are
//!     silent NoOps (operator-stopped state is preserved);
//!   - `StateCounts.cancelled` and `OutcomeSummary.cancelled` partition
//!     correctly;
//!   - snapshot/restore round-trips the panik latch + reason.

use super::*;

fn seed_with_one_pending() -> (ClusterState<RunnerIdentifier>, String) {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_task("a"),
    });
    (s, "h".to_string())
}

#[test]
fn panik_sets_sticky_flag_and_records_reason() {
    let (mut s, _) = seed_with_one_pending();
    assert!(!s.panik_active());
    assert_eq!(s.panik_reason(), None);
    assert_eq!(s.panik_source(), None);

    let outcome = s.apply(ClusterMutation::PanikRequested {
        source_peer: "primary".into(),
        reason: "file: /tmp/asm-tokenizer.panik".into(),
    });
    assert_eq!(outcome, ApplyOutcome::Applied);
    assert!(s.panik_active());
    assert_eq!(s.panik_reason(), Some("file: /tmp/asm-tokenizer.panik"));
    assert_eq!(s.panik_source(), Some("primary"));
}

#[test]
fn second_panik_is_noop_first_wins() {
    // Two distinct nodes detecting the file independently and
    // broadcasting in parallel must converge: the second-applying
    // broadcast finds the latch set and exits silently, leaving the
    // first-applying source/reason as canonical.
    let (mut s, _) = seed_with_one_pending();
    s.apply(ClusterMutation::PanikRequested {
        source_peer: "primary".into(),
        reason: "first".into(),
    });
    let outcome = s.apply(ClusterMutation::PanikRequested {
        source_peer: "secondary-3".into(),
        reason: "second".into(),
    });
    assert_eq!(outcome, ApplyOutcome::NoOp);
    assert_eq!(s.panik_source(), Some("primary"));
    assert_eq!(s.panik_reason(), Some("first"));
}

#[test]
fn panik_sweeps_pending_in_flight_and_blocked_to_cancelled() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    // Pending
    s.apply(ClusterMutation::TaskAdded {
        hash: "h-pending".into(),
        task: mk_task("a"),
    });
    // InFlight
    s.apply(ClusterMutation::TaskAdded {
        hash: "h-inflight".into(),
        task: mk_task("b"),
    });
    s.apply(ClusterMutation::TaskAssigned {
        hash: "h-inflight".into(),
        secondary: "s1".into(),
        worker: 0,
    });
    // Blocked: seed a prereq Unfulfillable then a dependent TaskBlocked
    s.apply(ClusterMutation::TaskAdded {
        hash: "h-blocked".into(),
        task: mk_task("c"),
    });
    s.apply(ClusterMutation::TaskBlocked {
        hash: "h-blocked".into(),
        on: "h-prereq".into(),
    });

    s.apply(ClusterMutation::PanikRequested {
        source_peer: "primary".into(),
        reason: "stop".into(),
    });

    for h in ["h-pending", "h-inflight", "h-blocked"] {
        assert!(
            matches!(s.task_state(h), Some(TaskState::Cancelled { reason, .. }) if reason == "stop"),
            "{h} must be Cancelled after panik"
        );
    }
}

#[test]
fn panik_preserves_completed_failed_unfulfillable() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    // Completed
    s.apply(ClusterMutation::TaskAdded {
        hash: "h-comp".into(),
        task: mk_task("a"),
    });
    s.apply(ClusterMutation::TaskCompleted {
        hash: "h-comp".into(),
        result_data: None,
    });
    // Failed
    s.apply(ClusterMutation::TaskAdded {
        hash: "h-fail".into(),
        task: mk_task("b"),
    });
    s.apply(ClusterMutation::TaskFailed {
        hash: "h-fail".into(),
        kind: ErrorType::NonRecoverable,
        error: "boom".into(),
    });
    // Unfulfillable
    s.apply(ClusterMutation::TaskAdded {
        hash: "h-unf".into(),
        task: mk_task("c"),
    });
    s.apply(ClusterMutation::TaskFailed {
        hash: "h-unf".into(),
        kind: ErrorType::Unfulfillable {
            reason: "no holder".into(),
        },
        error: "n/a".into(),
    });

    s.apply(ClusterMutation::PanikRequested {
        source_peer: "primary".into(),
        reason: "stop".into(),
    });

    assert!(matches!(
        s.task_state("h-comp"),
        Some(TaskState::Completed { .. })
    ));
    assert!(matches!(
        s.task_state("h-fail"),
        Some(TaskState::Failed { .. })
    ));
    assert!(matches!(
        s.task_state("h-unf"),
        Some(TaskState::Unfulfillable { .. })
    ));
}

#[test]
fn late_task_completed_against_cancelled_supersedes_to_completed() {
    // Worker finished a task milliseconds before the panik kill
    // reached it; the TaskCompleted broadcast lands after
    // PanikRequested. Success is the strongest terminal regardless
    // of arrival order — the entry transitions Cancelled → Completed.
    let (mut s, hash) = seed_with_one_pending();
    s.apply(ClusterMutation::PanikRequested {
        source_peer: "primary".into(),
        reason: "stop".into(),
    });
    assert!(matches!(
        s.task_state(&hash),
        Some(TaskState::Cancelled { .. })
    ));
    let outcome = s.apply(ClusterMutation::TaskCompleted { hash: hash.clone(), result_data: None });
    assert_eq!(outcome, ApplyOutcome::Applied);
    assert!(matches!(
        s.task_state(&hash),
        Some(TaskState::Completed { .. })
    ));
}

#[test]
fn late_task_failed_against_cancelled_is_noop() {
    // Operator-stopped state is preserved against a late
    // worker-surfaced failure (the panik kill happened, the worker's
    // outgoing TaskFailed message races and arrives after the
    // cancellation). Cancelled wins for non-success terminals.
    let (mut s, hash) = seed_with_one_pending();
    s.apply(ClusterMutation::PanikRequested {
        source_peer: "primary".into(),
        reason: "stop".into(),
    });
    let outcome = s.apply(ClusterMutation::TaskFailed {
        hash: hash.clone(),
        kind: ErrorType::NonRecoverable,
        error: "boom".into(),
    });
    assert_eq!(outcome, ApplyOutcome::NoOp);
    assert!(matches!(
        s.task_state(&hash),
        Some(TaskState::Cancelled { .. })
    ));
}

#[test]
fn late_task_blocked_against_cancelled_is_noop() {
    let (mut s, hash) = seed_with_one_pending();
    s.apply(ClusterMutation::PanikRequested {
        source_peer: "primary".into(),
        reason: "stop".into(),
    });
    let outcome = s.apply(ClusterMutation::TaskBlocked {
        hash: hash.clone(),
        on: "other".into(),
    });
    assert_eq!(outcome, ApplyOutcome::NoOp);
    assert!(matches!(
        s.task_state(&hash),
        Some(TaskState::Cancelled { .. })
    ));
}

#[test]
fn counts_and_outcome_summary_include_cancelled_bucket() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "h-a".into(),
        task: mk_task("a"),
    });
    s.apply(ClusterMutation::TaskAdded {
        hash: "h-b".into(),
        task: mk_task("b"),
    });
    s.apply(ClusterMutation::TaskAdded {
        hash: "h-c".into(),
        task: mk_task("c"),
    });
    s.apply(ClusterMutation::TaskCompleted {
        hash: "h-a".into(),
        result_data: None,
    });
    s.apply(ClusterMutation::PanikRequested {
        source_peer: "primary".into(),
        reason: "stop".into(),
    });

    let c = s.counts();
    assert_eq!(c.completed, 1);
    assert_eq!(c.cancelled, 2);
    assert_eq!(c.pending, 0);

    let o = s.outcome_counts();
    assert_eq!(o.succeeded, 1);
    assert_eq!(o.cancelled, 2);
    assert_eq!(o.fail_retry, 0);
    assert_eq!(o.fail_oom, 0);
    assert_eq!(o.fail_final, 0);
    assert_eq!(o.total_terminal(), 3);
}

#[test]
fn snapshot_round_trips_panik_latch_and_cancelled_state() {
    // Source replica observes the panik; its snapshot must carry
    // the latch + reason so a late-joiner restoring from this
    // snapshot inherits the same operator-stop state.
    let (mut source, _) = seed_with_one_pending();
    source.apply(ClusterMutation::PanikRequested {
        source_peer: "primary".into(),
        reason: "file: /tmp/asm.panik".into(),
    });
    let snap = source.snapshot();

    let mut sink = ClusterState::<RunnerIdentifier>::new();
    sink.restore(snap);
    assert!(sink.panik_active());
    assert_eq!(sink.panik_reason(), Some("file: /tmp/asm.panik"));
    assert_eq!(sink.panik_source(), Some("primary"));
    assert!(matches!(
        sink.task_state("h"),
        Some(TaskState::Cancelled { .. })
    ));
}

#[test]
fn snapshot_with_panik_false_does_not_clear_local_latch() {
    // Monotonic-true: a snapshot from a peer who hasn't yet seen the
    // panik mutation (`panik_active = false`) must NOT regress a
    // local replica that already latched. The restore's lattice-
    // merge contract for sticky flags is "true wins".
    let (mut s, _) = seed_with_one_pending();
    s.apply(ClusterMutation::PanikRequested {
        source_peer: "primary".into(),
        reason: "stop".into(),
    });
    let pre = ClusterState::<RunnerIdentifier>::new();
    s.restore(pre.snapshot());
    assert!(s.panik_active());
    assert_eq!(s.panik_reason(), Some("stop"));
}
