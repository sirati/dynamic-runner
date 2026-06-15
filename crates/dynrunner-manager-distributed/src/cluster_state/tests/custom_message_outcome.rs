//! End-to-end tests for the #570 F5 custom-message outcome-narration
//! channel — the CRDT-layer half of the observer-narration feature.
//!
//! Pins the contract:
//!   - every WINNING terminal apply (`CustomMessageHandled` /
//!     `CustomMessageFailed`) enqueues exactly one
//!     [`CustomMessageOutcomeEvent`] with the right outcome label —
//!     INCLUDING through the per-origin watermark compactor, which
//!     erases the label from the CRDT state in the SAME arm: the event
//!     fires BEFORE the compactor and carries the truth;
//!   - `Failed` carries the wire `reason` verbatim — narration-only
//!     plumbing the apply rule emits and drops; no CRDT state holds it;
//!   - a watermark-subsumed redelivery (the post-compaction NoOp path)
//!     emits no event — no double-narration on a re-apply;
//!   - an already-terminal redelivery (`Handled→Handled`,
//!     `Failed→Failed`) NoOps the latch → no event;
//!   - the lattice's theoretical `Failed→Handled` convergence (the
//!     Handled-wins join) IS an Applied transition → it DOES emit a
//!     `Handled` event;
//!   - a vacant `Handled`/`Failed` insert (a terminal that outran its
//!     `Posted` on a different gossip path) IS Applied → it DOES emit;
//!   - a `CustomMessagePosted` apply emits NO outcome event (the
//!     landing edge is the [`crate::run_narrator`]'s concern; the
//!     outcome channel is terminal-only);
//!   - the #568 boundary `custom_message_terminals_are_silent_in_state_narrator`
//!     still passes — the state-derived path stays silent; the
//!     event-driven path is its independent sibling.

use super::*;
use crate::custom_message_outcome::{CustomMessageOutcome, CustomMessageOutcomeEvent};

/// Drain every buffered outcome event off the channel (the test reads
/// after a synchronous apply batch, so all events are already enqueued).
fn drain(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<CustomMessageOutcomeEvent>,
) -> Vec<CustomMessageOutcomeEvent> {
    let mut out = Vec::new();
    while let Ok(ev) = rx.try_recv() {
        out.push(ev);
    }
    out
}

/// LIVE path: Posted → Handled emits ONE Handled outcome event with the
/// `(origin, seq)` key; the post-compaction CRDT state has lost the
/// label (the per-origin watermark advanced over it), but the event
/// fired BEFORE the compactor — proving the design that #568 deferred.
#[tokio::test]
async fn handled_terminal_emits_handled_outcome_before_compaction() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    s.install_custom_message_outcome_sender(tx);

    s.apply(ClusterMutation::CustomMessagePosted {
        origin: "sec-a".into(),
        seq: 1,
        topic: "t".into(),
        data: b"payload".to_vec(),
    });
    // Posted itself fires no outcome event — it is the landing edge,
    // not a terminal.
    assert!(
        drain(&mut rx).is_empty(),
        "Posted is not a terminal — no outcome event fires"
    );

    s.apply(ClusterMutation::CustomMessageHandled {
        origin: "sec-a".into(),
        seq: 1,
    });
    let events = drain(&mut rx);
    assert_eq!(events.len(), 1, "Handled terminal emits exactly one event");
    assert_eq!(events[0].origin, "sec-a");
    assert_eq!(events[0].seq, 1);
    assert!(matches!(events[0].outcome, CustomMessageOutcome::Handled));

    // Post-compaction: the watermark has advanced over (sec-a, 1), so
    // the CRDT state can no longer tell Handled from Failed. The
    // outcome above proves the apply-site emit FIRES BEFORE compaction.
    assert_eq!(
        s.custom_terminal_watermark("sec-a"),
        Some(1),
        "watermark advanced (compaction ran, erasing the label)"
    );
}

/// LIVE path: Posted → Failed{reason} emits ONE Failed outcome event
/// carrying the verbatim reason. The reason rides only the wire
/// mutation + the event; the CRDT state stores a label-less, reason-less
/// `Failed` tombstone that the compactor then sweeps (no #570 footprint
/// in the lattice).
#[tokio::test]
async fn failed_terminal_emits_failed_outcome_with_verbatim_reason() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    s.install_custom_message_outcome_sender(tx);

    s.apply(ClusterMutation::CustomMessagePosted {
        origin: "sec-b".into(),
        seq: 1,
        topic: "u".into(),
        data: b"x".to_vec(),
    });
    let _ = drain(&mut rx);

    s.apply(ClusterMutation::CustomMessageFailed {
        origin: "sec-b".into(),
        seq: 1,
        reason: "boom in stage 3".into(),
    });
    let events = drain(&mut rx);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].origin, "sec-b");
    assert_eq!(events[0].seq, 1);
    match &events[0].outcome {
        CustomMessageOutcome::Failed { reason } => {
            assert_eq!(reason, "boom in stage 3", "verbatim reason rides the event");
        }
        other => panic!("expected Failed, got {other:?}"),
    }
    // Compaction ran, label is gone from state.
    assert_eq!(s.custom_terminal_watermark("sec-b"), Some(1));
}

/// A re-applied terminal (a redelivered `Handled`/`Failed`) NoOps —
/// either by watermark coverage or by the already-terminal latch — and
/// emits NO second event, so the observer never double-narrates.
#[tokio::test]
async fn redelivered_terminal_emits_no_second_event() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    s.install_custom_message_outcome_sender(tx);

    s.apply(ClusterMutation::CustomMessagePosted {
        origin: "sec-c".into(),
        seq: 1,
        topic: "t".into(),
        data: b"z".to_vec(),
    });
    s.apply(ClusterMutation::CustomMessageHandled {
        origin: "sec-c".into(),
        seq: 1,
    });
    let first = drain(&mut rx);
    assert_eq!(first.len(), 1);

    // Re-deliver the SAME Handled (a late duplicate / re-broadcast / AE
    // re-pull). The apply is watermark-subsumed → NoOp → no event.
    let outcome = s.apply(ClusterMutation::CustomMessageHandled {
        origin: "sec-c".into(),
        seq: 1,
    });
    assert_eq!(outcome, ApplyOutcome::NoOp);
    assert!(
        drain(&mut rx).is_empty(),
        "a watermark-subsumed redelivery NoOps → no event"
    );
}

/// A `Failed` redelivery against an already-`Handled` (the Handled-wins
/// join's lockout direction: `Failed` never overwrites `Handled`) NoOps
/// → no event. The dual `Handled` over already-`Failed` IS Applied (the
/// theoretical Handled-wins convergence) → it emits a Handled event.
/// Per the apply arm: arrange the conflict BEFORE compaction so the
/// watermark hasn't yet advanced — use a gap-blocking unposted seq.
#[tokio::test]
async fn handled_wins_lattice_emits_handled_on_failed_to_handled_join() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    s.install_custom_message_outcome_sender(tx);

    // Seq 2 with seq 1 left unposted: the watermark CANNOT advance
    // past 2, so the two-terminal collision happens in the live map
    // (not behind the compaction wall).
    s.apply(ClusterMutation::CustomMessagePosted {
        origin: "sec-d".into(),
        seq: 2,
        topic: "t".into(),
        data: b"data".to_vec(),
    });
    let _ = drain(&mut rx);

    // First terminal: Failed{reason}.
    s.apply(ClusterMutation::CustomMessageFailed {
        origin: "sec-d".into(),
        seq: 2,
        reason: "first".into(),
    });
    let after_fail = drain(&mut rx);
    assert_eq!(after_fail.len(), 1);
    assert!(matches!(
        after_fail[0].outcome,
        CustomMessageOutcome::Failed { .. }
    ));

    // Second terminal: Handled — the deterministic Handled-wins join
    // (Failed→Handled is Applied). The apply rule emits a Handled
    // event.
    s.apply(ClusterMutation::CustomMessageHandled {
        origin: "sec-d".into(),
        seq: 2,
    });
    let after_handled = drain(&mut rx);
    assert_eq!(
        after_handled.len(),
        1,
        "Handled-wins join is Applied → exactly one Handled event"
    );
    assert!(matches!(
        after_handled[0].outcome,
        CustomMessageOutcome::Handled
    ));

    // Reverse direction: Handled → Failed is the Handled-wins LOCKOUT
    // (NoOp) — no event.
    s.apply(ClusterMutation::CustomMessageFailed {
        origin: "sec-d".into(),
        seq: 2,
        reason: "later".into(),
    });
    assert!(
        drain(&mut rx).is_empty(),
        "Handled→Failed is the Handled-wins lockout → NoOp → no event"
    );
}

/// A vacant `Handled` (a terminal that outran its `Posted` on a
/// different gossip path — the absent-key latch arm) IS Applied → it
/// emits a Handled event.
#[tokio::test]
async fn vacant_handled_insert_emits_handled_outcome() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    s.install_custom_message_outcome_sender(tx);

    s.apply(ClusterMutation::CustomMessageHandled {
        origin: "sec-e".into(),
        seq: 1,
    });
    let events = drain(&mut rx);
    assert_eq!(events.len(), 1);
    assert!(matches!(events[0].outcome, CustomMessageOutcome::Handled));
    assert_eq!(events[0].origin, "sec-e");
    assert_eq!(events[0].seq, 1);
}

/// A vacant `Failed{reason}` (the absent-key latch arm — the `Failed`
/// twin of the vacant-Handled case) IS Applied → it emits a Failed
/// event carrying the reason.
#[tokio::test]
async fn vacant_failed_insert_emits_failed_outcome_with_reason() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    s.install_custom_message_outcome_sender(tx);

    s.apply(ClusterMutation::CustomMessageFailed {
        origin: "sec-f".into(),
        seq: 1,
        reason: "vacant-failed-reason".into(),
    });
    let events = drain(&mut rx);
    assert_eq!(events.len(), 1);
    match &events[0].outcome {
        CustomMessageOutcome::Failed { reason } => {
            assert_eq!(reason, "vacant-failed-reason");
        }
        other => panic!("expected Failed, got {other:?}"),
    }
}

/// Cluster state with no installed sender (every role except the
/// observer): the apply path's emit is a SILENT drop. This is the
/// degenerate-receiver contract every dispatcher channel shares; the
/// CRDT itself never depends on whether someone listens.
#[tokio::test]
async fn no_installed_sender_is_silent_drop() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    // NO `install_custom_message_outcome_sender` call.
    let outcome = s.apply(ClusterMutation::CustomMessageHandled {
        origin: "sec-g".into(),
        seq: 1,
    });
    assert_eq!(
        outcome,
        ApplyOutcome::Applied,
        "the apply rule itself is unchanged by whether a sender exists"
    );
}
