//! End-to-end tests for the #520 per-task state-change channel — the
//! CRDT-layer half of the observer-narration feature.
//!
//! Pins the "merge join builds the event, exactly-once, from BOTH the
//! live apply path AND the snapshot restore path" contract:
//!   - every WINNING transition (assign / complete / fail / non-terminal)
//!     enqueues exactly one [`TaskStateChangeEvent`] with the right
//!     classification + holder;
//!   - the holder for a completion / failure is the PRIOR `InFlight`
//!     holder the terminal superseded (resolved AT the merge);
//!   - the fail-class fold matches `outcome_counts` (terminal / recoverable
//!     / oom) so the observer's ERROR/WARN level is the CRDT's own
//!     bucketing;
//!   - a dedup / dominated re-delivery is a NoOp → NO event (so the
//!     observer never double-narrates);
//!   - a restore-delivered transition fires the SAME event (the
//!     path-independence the bootstrap baseline + WARN-dropped-broadcast
//!     recovery both depend on);
//!   - the channel is O(1) per change — there is NO per-apply ledger sweep
//!     (the event is built once at the single per-task join, never by
//!     scanning `tasks`).
//!
//! `#[tokio::test]` because the channel is a `tokio::sync::mpsc` whose
//! receiver the test reads after the apply (mirrors `dispatchers.rs`).

use super::*;
use crate::task_state_change::{TaskStateChange, TaskStateChangeEvent};

/// Add a task as `Pending`, then assign it to `(secondary, worker)` — the
/// `Pending → InFlight` the live primary's dispatch originates. Returns
/// after both applies.
fn add_and_assign(
    s: &mut ClusterState<RunnerIdentifier>,
    hash: &str,
    secondary: &str,
    worker: u32,
) {
    s.apply(ClusterMutation::TaskAdded {
        hash: hash.into(),
        task: mk_task(hash),
        def_id: None,
    });
    s.apply(ClusterMutation::TaskAssigned {
        hash: hash.into(),
        secondary: secondary.into(),
        worker,
        version: Default::default(),
        attempt: 0,
    });
}

/// Drain every buffered event off the channel (the test reads after a
/// synchronous apply batch, so all events are already enqueued).
fn drain(rx: &mut tokio::sync::mpsc::UnboundedReceiver<TaskStateChangeEvent>) -> Vec<TaskStateChangeEvent> {
    let mut out = Vec::new();
    while let Ok(ev) = rx.try_recv() {
        out.push(ev);
    }
    out
}

/// LIVE path, the assign → complete script: assignment narrates
/// `Assigned` with the new holder; completion narrates `Completed` with
/// the PRIOR `InFlight` holder (the terminal carries none of its own).
#[tokio::test]
async fn assign_then_complete_emits_assigned_then_completed_with_holder() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    s.install_task_state_change_sender(tx);

    add_and_assign(&mut s, "h1", "sec-a", 5);
    s.apply(ClusterMutation::TaskCompleted {
        hash: "h1".into(),
        result_data: None,
        attempt: 0,
    });

    let events = drain(&mut rx);
    // A2: one event per winning transition across BOTH seams —
    // TaskAdded→Pending (the direct-insert A2 arm), TaskAssigned→InFlight +
    // TaskCompleted (the merge seam).
    assert_eq!(events.len(), 3, "one event per winning transition: {events:?}");

    assert!(
        matches!(events[0].change, TaskStateChange::Other { state: "pending" }),
        "the spawn-time Pending narrates live via the A2 helper"
    );
    assert_eq!(events[0].holder, None, "a spawn-time Pending names no holder");

    assert!(matches!(events[1].change, TaskStateChange::Assigned));
    assert_eq!(
        events[1].holder.as_ref().map(|(s, w)| (s.as_str(), *w)),
        Some(("sec-a", 5)),
        "the assignment carries the new InFlight holder"
    );
    // From→to (#520): the assignment's prior slot occupant is the Pending
    // the assign superseded — captured at the apply seam.
    assert_eq!(
        events[1].from,
        Some("pending"),
        "the assign carries the PRE-write Pending as its from-state"
    );

    assert!(matches!(events[2].change, TaskStateChange::Completed));
    assert_eq!(
        events[2].holder.as_ref().map(|(s, w)| (s.as_str(), *w)),
        Some(("sec-a", 5)),
        "the completion carries the PRIOR InFlight holder, resolved at the merge"
    );
    assert_eq!(events[2].task_id, "h1");
    // From→to (#520): the completion's prior slot occupant is the InFlight
    // it superseded.
    assert_eq!(
        events[2].from,
        Some("in-flight"),
        "the completion carries the PRE-write InFlight as its from-state"
    );
    // The spawn-time Pending is a logical CREATE (vacant slot) — no
    // from-state to name.
    assert_eq!(events[0].from, None, "a CREATE names no prior state");
    // CRDT txn id (#520): the version-less Completed reports the
    // attempt-only coordinate (epoch/seq default to 0; attempt is 0 here).
    assert_eq!(
        events[2].txn,
        crate::task_state_change::TaskTxnId { primary_epoch: 0, seq: 0, attempt: 0 },
        "the completion's CRDT txn id is its (version, attempt) coordinate"
    );
}

/// From→to + CRDT txn id PATH-INDEPENDENCE: a retry reset
/// (`Failed → Pending`) at a stamped epoch surfaces the failed→pending
/// transition AND the reset's stamped `TaskVersion` as the txn id, so the
/// operator correlates the re-queue line to the exact CRDT reset.
#[tokio::test]
async fn retry_reset_carries_from_failed_and_stamped_txn_version() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    s.install_task_state_change_sender(tx);

    add_and_assign(&mut s, "h9", "sec-h", 1);
    s.apply(ClusterMutation::TaskFailed {
        hash: "h9".into(),
        kind: ErrorType::Recoverable,
        error: "blip".into(),
        version: TaskVersion { primary_epoch: 1, seq: 0 },
        attempt: 0,
    });
    s.apply(ClusterMutation::TaskRetried {
        hash: "h9".into(),
        attempt: 1,
        version: TaskVersion { primary_epoch: 2, seq: 4 },
    });

    let events = drain(&mut rx);
    // The reset is the LAST event: Failed → Pending.
    let reset = events
        .iter()
        .rev()
        .find(|e| matches!(&e.change, TaskStateChange::Other { state: "pending" }))
        .expect("the TaskRetried reset narrates a Pending transition");
    assert_eq!(
        reset.from,
        Some("failed"),
        "the retry reset names the PRE-write Failed as its from-state"
    );
    // The reset Pending carries the originator-stamped (epoch 2, seq 4)
    // version + the bumped attempt 1 — the exact CRDT transaction
    // coordinates of the reset.
    assert_eq!(
        reset.txn,
        crate::task_state_change::TaskTxnId { primary_epoch: 2, seq: 4, attempt: 1 },
        "the reset's CRDT txn id is the stamped (epoch, seq) + bumped attempt"
    );
}

/// LIVE path, the assign → terminal-fail script: a `NonRecoverable`
/// failure is a `TerminalFailure` carrying reason + the full last_error,
/// on the prior holder.
#[tokio::test]
async fn assign_then_terminal_fail_carries_reason_and_full_last_error() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    s.install_task_state_change_sender(tx);

    add_and_assign(&mut s, "h2", "sec-b", 2);
    s.apply(ClusterMutation::TaskFailed {
        hash: "h2".into(),
        kind: ErrorType::NonRecoverable,
        error: "segfault in stage 3 (rc=139)".into(),
        version: Default::default(),
        attempt: 0,
    });

    let events = drain(&mut rx);
    let last = events.last().expect("a fail event");
    match &last.change {
        TaskStateChange::TerminalFailure { reason, last_error } => {
            assert_eq!(reason, "non_recoverable");
            assert_eq!(
                last_error, "segfault in stage 3 (rc=139)",
                "the FULL last_error rides the terminal event"
            );
        }
        other => panic!("expected TerminalFailure, got {other:?}"),
    }
    assert_eq!(
        last.holder.as_ref().map(|(s, w)| (s.as_str(), *w)),
        Some(("sec-b", 2)),
        "the terminal carries the prior InFlight holder"
    );
}

/// LIVE path, the assign → recoverable-fail script: an `ErrorType::
/// Recoverable` failure is a `RecoverableFailure` (the `fail_retry` fold).
#[tokio::test]
async fn assign_then_recoverable_fail_is_recoverable_class() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    s.install_task_state_change_sender(tx);

    add_and_assign(&mut s, "h3", "sec-c", 0);
    s.apply(ClusterMutation::TaskFailed {
        hash: "h3".into(),
        kind: ErrorType::Recoverable,
        error: "transient network blip".into(),
        version: Default::default(),
        attempt: 0,
    });

    let last = drain(&mut rx).pop().expect("a fail event");
    assert!(
        matches!(last.change, TaskStateChange::RecoverableFailure { .. }),
        "Recoverable folds to the recoverable (WARN) class, got {:?}",
        last.change
    );
}

/// LIVE path, the oom-fail class: `ResourceExhausted("memory")` is an
/// `OomFailure` (the `fail_oom` fold) — matching `outcome_counts`.
#[tokio::test]
async fn oom_fail_is_oom_class() {
    use dynrunner_core::types::identifiers::ResourceKind;
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    s.install_task_state_change_sender(tx);

    add_and_assign(&mut s, "h4", "sec-d", 1);
    s.apply(ClusterMutation::TaskFailed {
        hash: "h4".into(),
        kind: ErrorType::ResourceExhausted(ResourceKind::memory()),
        error: "oom-killed".into(),
        version: Default::default(),
        attempt: 0,
    });

    let last = drain(&mut rx).pop().expect("a fail event");
    assert!(
        matches!(last.change, TaskStateChange::OomFailure { .. }),
        "ResourceExhausted(memory) folds to the oom (WARN) class, got {:?}",
        last.change
    );
}

/// A2: a LIVE non-terminal transition (`TaskAdded → Pending`, then
/// `TaskBlocked`) narrates per-event through the shared `rewrite_task_state`
/// / direct-insert seams — the `Other` "changed state to {pending|blocked}"
/// lines, holder-less. These arms bypass `merge_task_state`, so the A2
/// shared helper (`emit_task_state_change_for`) is what surfaces them live.
#[tokio::test]
async fn live_non_terminal_transitions_narrate_via_a2_helper() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    s.install_task_state_change_sender(tx);

    // TaskAdded → Pending (direct-insert arm).
    s.apply(ClusterMutation::TaskAdded {
        hash: "h5".into(),
        task: mk_task("h5"),
        def_id: None,
    });
    // TaskBlocked → Blocked (rewrite_task_state arm).
    s.apply(ClusterMutation::TaskBlocked {
        hash: "h5".into(),
        on: "prereq".into(),
    });

    let events = drain(&mut rx);
    // Both transitions narrate live, in order: pending then blocked.
    let states: Vec<&str> = events
        .iter()
        .filter(|e| e.task_id == "h5")
        .filter_map(|e| match &e.change {
            TaskStateChange::Other { state } => Some(*state),
            _ => None,
        })
        .collect();
    assert_eq!(
        states,
        vec!["pending", "blocked"],
        "TaskAdded→Pending then TaskBlocked narrate live as Other state changes: {events:?}"
    );
    assert!(events.iter().all(|e| e.holder.is_none()));
}

/// A2: a LIVE retry reset (`TaskFailed` then `TaskRetried`) narrates the
/// terminal failure (WARN/ERROR, via the merge seam) AND the subsequent
/// `Pending` reset (INFO Other, via the `rewrite_task_state` A2 seam) — so
/// the operator sees both the failure and the re-queue live.
#[tokio::test]
async fn live_retry_reset_narrates_failure_then_pending() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    s.install_task_state_change_sender(tx);

    add_and_assign(&mut s, "h8", "sec-g", 1);
    s.apply(ClusterMutation::TaskFailed {
        hash: "h8".into(),
        kind: ErrorType::Recoverable,
        error: "blip".into(),
        version: TaskVersion {
            primary_epoch: 1,
            seq: 0,
        },
        attempt: 0,
    });
    // The retry originator mints attempt n+1 against the Failed { attempt: 0 }.
    s.apply(ClusterMutation::TaskRetried {
        hash: "h8".into(),
        attempt: 1,
        version: TaskVersion {
            primary_epoch: 2,
            seq: 0,
        },
    });

    let events = drain(&mut rx);
    // ...Assigned, RecoverableFailure (merge seam), then Pending reset (A2).
    let recoverable = events
        .iter()
        .any(|e| matches!(e.change, TaskStateChange::RecoverableFailure { .. }));
    let reset_to_pending = events
        .iter()
        .any(|e| matches!(&e.change, TaskStateChange::Other { state: "pending" }));
    assert!(recoverable, "the recoverable failure narrates: {events:?}");
    assert!(
        reset_to_pending,
        "the TaskRetried reset to Pending narrates live via the A2 seam: {events:?}"
    );
}

/// A dominated / duplicate re-delivery is a NoOp on the join → NO event,
/// so the observer never double-narrates the same transition.
#[tokio::test]
async fn duplicate_redelivery_emits_no_event() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    s.install_task_state_change_sender(tx);

    add_and_assign(&mut s, "h6", "sec-e", 4);
    s.apply(ClusterMutation::TaskCompleted {
        hash: "h6".into(),
        result_data: None,
        attempt: 0,
    });
    let first = drain(&mut rx);
    let completes = first
        .iter()
        .filter(|e| matches!(e.change, TaskStateChange::Completed))
        .count();
    assert_eq!(completes, 1);

    // Re-deliver the SAME completion (a late duplicate / re-broadcast).
    s.apply(ClusterMutation::TaskCompleted {
        hash: "h6".into(),
        result_data: None,
        attempt: 0,
    });
    assert!(
        drain(&mut rx).is_empty(),
        "a dominated re-delivery NoOps the join — no second event, no double-narration"
    );
}

/// PATH-INDEPENDENCE: a transition delivered ONLY via snapshot restore
/// (its live broadcast never reached this mirror) fires the SAME event on
/// the SAME channel — the merge join is the single seam, so apply and
/// restore narrate identically. This is what makes the observer's
/// narration cover the snapshot-stream / anti-entropy ingestion path.
#[tokio::test]
async fn restore_delivered_transition_emits_the_same_event() {
    // Source replica: drive an assign → complete so its snapshot carries
    // the Completed task (holder folded away on the terminal, as the
    // ledger keeps it).
    let mut source = ClusterState::<RunnerIdentifier>::new();
    add_and_assign(&mut source, "h7", "sec-f", 9);
    source.apply(ClusterMutation::TaskCompleted {
        hash: "h7".into(),
        result_data: None,
        attempt: 0,
    });
    let snap = source.snapshot();

    // Target replica (the observer's mirror): install the channel, then
    // RESTORE — the per-task merge loop must fire the state-change event
    // for the Completed task even though no live mutation was applied.
    let mut target = ClusterState::<RunnerIdentifier>::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    target.install_task_state_change_sender(tx);
    target.restore(snap);

    let events = drain(&mut rx);
    assert!(
        events
            .iter()
            .any(|e| e.task_id == "h7" && matches!(e.change, TaskStateChange::Completed)),
        "a restore-delivered Completed fires the SAME state-change event \
         (path-independent narration): {events:?}"
    );

    // Re-restore the same snapshot: the merge NoOps every key → no event,
    // so a bootstrap re-pull never re-floods the narrator.
    let snap2 = source.snapshot();
    target.restore(snap2);
    assert!(
        drain(&mut rx).is_empty(),
        "a re-restore NoOps every key — no duplicate narration on a re-pull"
    );
}
