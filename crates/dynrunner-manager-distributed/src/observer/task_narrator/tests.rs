//! Unit tests for the #520 observer per-task narrator: the
//! event→line projection + the level mapping + the baseline guard.
//!
//! These drive [`ObserverTaskNarrator`] directly with hand-built
//! [`TaskStateChangeEvent`]s — the PURE projection unit. The
//! end-to-end CRDT→channel→narrator path (proving the merge join builds
//! the right event, exactly-once, from both apply and restore) is
//! exercised by the coordinator integration test in
//! `observer/coordinator/tests.rs`.

use tracing::Level;

use super::*;
use crate::cluster_state::StateCounts;
use crate::task_state_change::{TaskStateChange, TaskStateChangeEvent};
use crate::test_capture::{IMPORTANT_TARGET, LeveledEvent, TargetCapture};

/// Run `body` with a [`TargetCapture`] on the importance marker
/// installed as the thread-local default, returning every captured
/// leveled event (level preserved — this is the crux for the
/// ERROR/WARN/INFO assertions).
fn capture(body: impl FnOnce()) -> Vec<LeveledEvent> {
    use tracing_subscriber::Registry;
    use tracing_subscriber::layer::SubscriberExt;

    let cap = TargetCapture::for_target(IMPORTANT_TARGET);
    let subscriber = Registry::default().with(cap.clone());
    tracing::subscriber::with_default(subscriber, body);
    cap.events()
}

fn evt(task_id: &str, change: TaskStateChange, holder: Option<(&str, u32)>) -> TaskStateChangeEvent {
    TaskStateChangeEvent {
        task_id: task_id.to_string(),
        change,
        holder: holder.map(|(s, w)| (s.to_string(), w)),
    }
}

/// A narrator with live narration already armed (the baseline summary
/// fired) — the precondition for every per-event level test.
fn armed() -> ObserverTaskNarrator {
    let mut n = ObserverTaskNarrator::default();
    // Arm with an empty baseline (no summary line emitted, but live is
    // armed) so the per-event tests start from a clean capture.
    n.narrate_baseline(0, StateCounts::default());
    n
}

/// Assignment narrates INFO "assigned to {secondary}-{worker}".
#[test]
fn assigned_narrates_info_with_holder() {
    let events = capture(|| {
        let n = armed();
        assert!(n.narrate_live(&evt("t1", TaskStateChange::Assigned, Some(("sec-a", 3)))));
    });
    assert_eq!(events.len(), 1, "one line: {events:?}");
    assert_eq!(events[0].level, Level::INFO);
    assert!(
        events[0].event.message.contains("t1") && events[0].event.message.contains("sec-a-3"),
        "assign line names task + holder: {:?}",
        events[0].event.message
    );
    assert!(events[0].event.message.contains("assigned to"));
}

/// Completion narrates INFO "completed on {secondary}-{worker}", where
/// the holder is the PRIOR InFlight holder carried on the event.
#[test]
fn completed_narrates_info_with_prior_holder() {
    let events = capture(|| {
        let n = armed();
        assert!(n.narrate_live(&evt("t2", TaskStateChange::Completed, Some(("sec-b", 7)))));
    });
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].level, Level::INFO);
    assert!(events[0].event.message.contains("t2"));
    assert!(events[0].event.message.contains("completed on"));
    assert!(events[0].event.message.contains("sec-b-7"));
}

/// A terminal failure narrates ERROR and carries BOTH the reason AND the
/// full last_error.
#[test]
fn terminal_failure_narrates_error_with_full_last_error() {
    let events = capture(|| {
        let n = armed();
        assert!(n.narrate_live(&evt(
            "t3",
            TaskStateChange::TerminalFailure {
                reason: "non_recoverable".to_string(),
                last_error: "panic at frobnicator.rs:42: index out of bounds".to_string(),
            },
            Some(("sec-c", 1)),
        )));
    });
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].level, Level::ERROR, "terminal fail is ERROR");
    assert!(events[0].event.message.contains("terminally failed on"));
    assert!(events[0].event.message.contains("sec-c-1"));
    assert!(
        events[0].event.message.contains("index out of bounds"),
        "the FULL last_error rides the ERROR line: {:?}",
        events[0].event.message
    );
}

/// A recoverable failure narrates WARN "(recoverable)".
#[test]
fn recoverable_failure_narrates_warn() {
    let events = capture(|| {
        let n = armed();
        assert!(n.narrate_live(&evt(
            "t4",
            TaskStateChange::RecoverableFailure {
                reason: "recoverable".to_string(),
            },
            Some(("sec-d", 0)),
        )));
    });
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].level, Level::WARN, "recoverable fail is WARN");
    assert!(events[0].event.message.contains("(recoverable)"));
    assert!(events[0].event.message.contains("sec-d-0"));
}

/// An OOM failure narrates WARN "(oom)".
#[test]
fn oom_failure_narrates_warn() {
    let events = capture(|| {
        let n = armed();
        assert!(n.narrate_live(&evt(
            "t5",
            TaskStateChange::OomFailure {
                reason: "oom".to_string(),
            },
            Some(("sec-e", 2)),
        )));
    });
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].level, Level::WARN, "oom fail is WARN");
    assert!(events[0].event.message.contains("(oom)"));
}

/// Any other (non-terminal / non-fail) transition narrates INFO "changed
/// state to {state}".
#[test]
fn other_state_change_narrates_info() {
    let events = capture(|| {
        let n = armed();
        assert!(n.narrate_live(&evt("t6", TaskStateChange::Other { state: "blocked" }, None)));
    });
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].level, Level::INFO);
    assert!(events[0].event.message.contains("t6"));
    assert!(events[0].event.message.contains("changed state to blocked"));
}

/// A completion / failure whose prior InFlight was never observed (no
/// holder on the event) still narrates — only the worker attribution is
/// `unknown-holder`.
#[test]
fn missing_holder_narrates_unknown_holder() {
    let events = capture(|| {
        let n = armed();
        assert!(n.narrate_live(&evt("t7", TaskStateChange::Completed, None)));
    });
    assert_eq!(events.len(), 1);
    assert!(events[0].event.message.contains("unknown-holder"));
}

/// BOOTSTRAP-FLOOD guard: a baseline of N transitions narrates exactly
/// ONE summary line, NOT N per-task lines — and live narration is INERT
/// until the baseline summary has fired.
#[test]
fn baseline_narrates_one_summary_not_n_lines_and_gates_live() {
    let events = capture(|| {
        let mut n = ObserverTaskNarrator::default();
        // BEFORE the baseline: a live event is a no-op (the gate).
        assert!(
            !n.narrate_live(&evt("early", TaskStateChange::Assigned, Some(("s", 1)))),
            "live narration must be inert before the baseline summary"
        );
        // The bootstrap restored 66_000 task transitions; the converged
        // mirror partition is what the one summary reports.
        let counts = StateCounts {
            completed: 60_000,
            in_flight: 4_000,
            pending: 2_000,
            ..Default::default()
        };
        n.narrate_baseline(66_000, counts);
    });
    // EXACTLY one line for the whole baseline — never 66k.
    assert_eq!(
        events.len(),
        1,
        "the 66k-task baseline narrates ONE summary line, not 66k changes: {} lines",
        events.len()
    );
    assert_eq!(events[0].level, Level::INFO);
    assert!(events[0].event.message.contains("mirroring baseline"));
    assert!(
        events[0].event.message.contains("60000") && events[0].event.message.contains("4000"),
        "summary carries the converged partition: {:?}",
        events[0].event.message
    );
}

/// An EMPTY baseline (cold-join before any snapshot) emits NO summary
/// line but still arms live narration.
#[test]
fn empty_baseline_emits_no_line_but_arms_live() {
    let events = capture(|| {
        let mut n = ObserverTaskNarrator::default();
        n.narrate_baseline(0, StateCounts::default());
        // Live is now armed.
        assert!(n.narrate_live(&evt("t", TaskStateChange::Assigned, Some(("s", 1)))));
    });
    // No baseline summary line; exactly the one live line.
    assert_eq!(events.len(), 1);
    assert!(events[0].event.message.contains("assigned to"));
}
