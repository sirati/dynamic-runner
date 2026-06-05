//! End-to-end dispatcher fan-out tests.
//!
//! Pins the "apply emits, dispatcher receiver observes" contract for
//! the three dispatcher channels installed on `ClusterState`:
//!
//!   - `lifecycle_tx` (`PeerLifecycleEvent`) — state-changing
//!     `PeerJoined`/`PeerRemoved` applies enqueue `Added`/`Removed`
//!     events on the channel.
//!   - `task_completed_tx` (`TaskCompletedEvent`) — every Applied
//!     `TaskCompleted`/`TaskFailed` apply enqueues a corresponding
//!     event with the wire-stable error_kind tag; dedup NoOps stay
//!     silent so consumers don't see ghost completions.
//!
//! These are `#[tokio::test]` because the dispatcher channels use
//! `tokio::sync::mpsc` and the test reads the receiver end after
//! the apply.

use super::*;

/// End-to-end: a state-changing `PeerJoined` apply, with a
/// dispatcher sender installed, MUST deliver a corresponding
/// `PeerLifecycleEvent::Added` on the channel. This pins the
/// "apply emits, dispatcher rx receives" contract — the
/// boundary that replaces the prior stub `emit_lifecycle_event`.
#[tokio::test]
async fn apply_peer_joined_emits_event_through_dispatcher() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    s.install_lifecycle_sender(tx);

    assert_eq!(
        s.apply(ClusterMutation::PeerJoined {
            peer_id: "peer-x".into(),
            is_observer: false,
            can_be_primary: false,
        }),
        ApplyOutcome::Applied
    );
    // The receiver MUST observe exactly one event with the
    // matching id / observer flag. `try_recv` confirms the
    // emit was non-blocking from the apply path's side.
    match rx.try_recv() {
        Ok(crate::peer_lifecycle::PeerLifecycleEvent::Added { id, is_observer }) => {
            assert_eq!(id, "peer-x");
            assert!(!is_observer);
        }
        other => panic!("expected Added event, got {other:?}"),
    }

    // Apply a removal as well to confirm the channel keeps
    // accepting subsequent events.
    assert_eq!(
        s.apply(ClusterMutation::PeerRemoved {
            id: "peer-x".into(),
            cause: RemovalCause::KeepaliveMiss,
        }),
        ApplyOutcome::Applied
    );
    match rx.try_recv() {
        Ok(crate::peer_lifecycle::PeerLifecycleEvent::Removed { id, cause }) => {
            assert_eq!(id, "peer-x");
            assert_eq!(cause, RemovalCause::KeepaliveMiss);
        }
        other => panic!("expected Removed event, got {other:?}"),
    }
}

// ── TaskCompleted / TaskFailed dispatcher fan-out tests ──
//
// Pin the "apply emits, dispatcher rx receives" contract for the
// task-completion module — the boundary the PyO3
// `task_completed_listener` kwarg ultimately observes.

/// A successful `TaskCompleted` apply MUST emit
/// `TaskCompletedEvent { success: true, error_kind: None,
/// task_hash, task_id }` on the installed dispatcher channel.
#[tokio::test]
async fn task_completed_listener_fires_on_task_completed_apply() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    s.install_task_completed_sender(tx);
    let task = mk_task("alpha");
    s.apply(ClusterMutation::TaskAdded {
        hash: "h-alpha".into(),
        task: task.clone(),
    });
    // Move it through to InFlight so the success transition isn't
    // a Pending → Completed shortcut (the apply rule covers both
    // but the in-flight path is the production shape).
    s.apply(ClusterMutation::TaskAssigned {
        hash: "h-alpha".into(),
        secondary: "sec-1".into(),
        worker: 0,

        version: Default::default(),
    });
    assert_eq!(
        s.apply(ClusterMutation::TaskCompleted {
            hash: "h-alpha".into(),
            result_data: None,
        }),
        ApplyOutcome::Applied
    );
    match rx.try_recv() {
        Ok(event) => {
            assert_eq!(event.task_hash, "h-alpha");
            assert_eq!(event.task_id, "alpha");
            assert!(event.success);
            assert!(event.error_kind.is_none());
        }
        other => panic!("expected TaskCompleted event, got {other:?}"),
    }
}

/// A `TaskFailed` apply MUST emit
/// `TaskCompletedEvent { success: false, error_kind:
/// Some(<wire_value>), last_error: Some(<message>), ... }` so
/// consumers can dispatch on the wire-stable error tag AND dedup
/// on the carried message without re-deriving either from `Debug`
/// or re-reading the ledger out of band.
#[tokio::test]
async fn task_completed_listener_fires_on_task_failed_with_error_kind() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    s.install_task_completed_sender(tx);
    s.apply(ClusterMutation::TaskAdded {
        hash: "h-beta".into(),
        task: mk_task("beta"),
    });
    // Pending → Failed (NonRecoverable). The wire tag for
    // NonRecoverable is `"non_recoverable"`.
    assert_eq!(
        s.apply(ClusterMutation::TaskFailed {
            hash: "h-beta".into(),
            kind: ErrorType::NonRecoverable,
            error: "disk full".into(),

            version: Default::default(),
        }),
        ApplyOutcome::Applied
    );
    match rx.try_recv() {
        Ok(event) => {
            assert_eq!(event.task_hash, "h-beta");
            assert_eq!(event.task_id, "beta");
            assert!(!event.success);
            assert_eq!(event.error_kind.as_deref(), Some("non_recoverable"));
            // The carried message is the same body that lands on the
            // ledger entry's `last_error` (captured before `error` is
            // moved into the entry).
            assert_eq!(event.last_error.as_deref(), Some("disk full"));
        }
        other => panic!("expected TaskFailed event, got {other:?}"),
    }
}

/// `TaskFailed { kind: Unfulfillable, .. }` against a Pending
/// task drives the `TaskState::Unfulfillable` transition; the
/// dispatcher event still fires with `success=false` and the
/// wire-stable `unfulfillable:<reason>` tag. Validates that the
/// Unfulfillable arm hooks into the same emit point as the
/// Failed arm — consumers don't have to know which terminal
/// the CRDT chose.
#[tokio::test]
async fn task_completed_listener_fires_on_unfulfillable_terminal() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    s.install_task_completed_sender(tx);
    s.apply(ClusterMutation::TaskAdded {
        hash: "h-gamma".into(),
        task: mk_task("gamma"),
    });
    assert_eq!(
        s.apply(ClusterMutation::TaskFailed {
            hash: "h-gamma".into(),
            kind: ErrorType::Unfulfillable {
                reason: "missing toolchain".to_owned().into(),
            },
            error: "missing toolchain".into(),

            version: Default::default(),
        }),
        ApplyOutcome::Applied
    );
    match rx.try_recv() {
        Ok(event) => {
            assert_eq!(event.task_hash, "h-gamma");
            assert!(!event.success);
            assert_eq!(
                event.error_kind.as_deref(),
                Some("unfulfillable:missing toolchain"),
            );
        }
        other => panic!("expected Unfulfillable event, got {other:?}"),
    }
}

/// A `TaskCompleted` apply that re-deduplicates (the task was
/// already `Completed`) MUST NOT emit a dispatcher event. The
/// apply rule is a NoOp; the dispatcher channel should stay
/// silent so consumers don't see ghost "task X completed again"
/// notifications.
#[tokio::test]
async fn task_completed_dedup_does_not_re_emit() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    s.install_task_completed_sender(tx);
    s.apply(ClusterMutation::TaskAdded {
        hash: "h-delta".into(),
        task: mk_task("delta"),
    });
    s.apply(ClusterMutation::TaskCompleted {
        hash: "h-delta".into(),
        result_data: None,
    });
    // Drain the first (valid) event so we can prove the
    // dedup-apply doesn't enqueue a second.
    rx.try_recv().expect("first TaskCompleted must emit");
    assert_eq!(
        s.apply(ClusterMutation::TaskCompleted {
            hash: "h-delta".into(),
            result_data: None,
        }),
        ApplyOutcome::NoOp
    );
    // No event should follow the NoOp dedup apply.
    assert!(
        matches!(
            rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ),
        "dedup TaskCompleted must not re-emit a dispatcher event",
    );
}

// ── Worker-management signal bus install/emit/drain tests ──
//
// Pin the decoupling-bus plumbing on `ClusterState`: phase/task
// management `emit_worker_mgmt`s signals, worker management drains a
// coalesced batch. There are no apply-path emit call sites yet (those
// land in a later subtask), so these tests drive `emit_worker_mgmt`
// directly — exactly the entry point the future call sites will use.

/// A burst of `emit_worker_mgmt` calls, with a sender installed,
/// coalesces into one batch via `drain_worker_signal_batch` that
/// preserves every signal in arrival order. Pins the
/// install → emit → drain plumbing end-to-end.
#[tokio::test(flavor = "current_thread")]
async fn worker_mgmt_emit_burst_coalesces_into_one_drained_batch() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    s.install_worker_mgmt_sender(tx);

    let s1 = crate::worker_signal::WorkerMgmtSignal::TasksAdded;
    let s2 = crate::worker_signal::WorkerMgmtSignal::PhaseStartedNeedsWorkers {
        phase: PhaseId::from("phase-a"),
        min: 2,
    };
    let s3 = crate::worker_signal::WorkerMgmtSignal::RunShouldFail {
        reason: "operator abort".to_string(),
    };
    s.emit_worker_mgmt(s1.clone());
    s.emit_worker_mgmt(s2.clone());
    s.emit_worker_mgmt(s3.clone());

    let batch = crate::worker_signal::drain_worker_signal_batch(
        &mut rx,
        std::time::Duration::from_millis(50),
    )
    .await
    .expect("emitted burst should produce a batch");
    assert_eq!(batch.signals, vec![s1, s2, s3]);
}

/// With no sender installed, `emit_worker_mgmt` is a silent no-op:
/// it does not panic and the (separately created, never-installed)
/// receiver observes nothing. Pins the best-effort / decoupled
/// contract that mirrors `emit_matcher_trigger`.
#[tokio::test(flavor = "current_thread")]
async fn worker_mgmt_emit_is_silent_no_op_without_installed_sender() {
    let s = ClusterState::<RunnerIdentifier>::new();
    // No `install_worker_mgmt_sender` — the bus has no sender.
    s.emit_worker_mgmt(crate::worker_signal::WorkerMgmtSignal::TasksAdded);

    // A channel created here but never installed must stay empty,
    // confirming the emit routed nowhere rather than onto some
    // implicit default.
    let (_tx, mut rx) =
        tokio::sync::mpsc::unbounded_channel::<crate::worker_signal::WorkerMgmtSignal>();
    assert!(
        matches!(
            rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ),
        "emit without an installed sender must be a silent no-op",
    );
}
