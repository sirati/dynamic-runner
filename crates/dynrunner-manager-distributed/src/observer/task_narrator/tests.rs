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
use dynrunner_core::OBSERVER_TASK_TARGET;

/// One captured event annotated with which target it landed on.
#[derive(Clone, Debug)]
struct TargetedEvent {
    target: &'static str,
    leveled: LeveledEvent,
}

/// Run `body` under captures on BOTH narrator targets — the
/// importance marker (wake-worthy failure arms + baseline summary)
/// AND the per-task observer-task target (non-wake-worthy
/// assign/complete/state-change INFO). Returns each captured event
/// tagged with the target it landed on, so a test asserts BOTH the
/// level and the routing — the crux of the #573 split.
fn capture(body: impl FnOnce()) -> Vec<TargetedEvent> {
    use tracing_subscriber::Registry;
    use tracing_subscriber::layer::SubscriberExt;

    let important = TargetCapture::for_target(IMPORTANT_TARGET);
    let observer_task = TargetCapture::for_target(OBSERVER_TASK_TARGET);
    let subscriber = Registry::default()
        .with(important.clone())
        .with(observer_task.clone());
    tracing::subscriber::with_default(subscriber, body);
    // The two captures are siblings of the same subscriber, so a
    // single emit lands in exactly one of them — never both, never
    // neither. Concatenation order is meaningful only within a single
    // target (each capture preserves emission order for its own
    // target); cross-target ordering is not asserted by these tests
    // (a narrator call produces at most one line).
    important
        .events()
        .into_iter()
        .map(|leveled| TargetedEvent {
            target: IMPORTANT_TARGET,
            leveled,
        })
        .chain(
            observer_task
                .events()
                .into_iter()
                .map(|leveled| TargetedEvent {
                    target: OBSERVER_TASK_TARGET,
                    leveled,
                }),
        )
        .collect()
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

/// Assignment narrates INFO "assigned to {secondary}-{worker}" on the
/// per-task observer-task target (non-wake-worthy: suppressed from
/// stdio under `--important-stdio-only`).
#[test]
fn assigned_narrates_info_with_holder() {
    let events = capture(|| {
        let n = armed();
        assert!(n.narrate_live(&evt("t1", TaskStateChange::Assigned, Some(("sec-a", 3)))));
    });
    assert_eq!(events.len(), 1, "one line: {events:?}");
    assert_eq!(events[0].target, OBSERVER_TASK_TARGET, "non-wake target");
    assert_eq!(events[0].leveled.level, Level::INFO);
    let msg = &events[0].leveled.event.message;
    assert!(
        msg.contains("t1") && msg.contains("sec-a-3"),
        "assign line names task + holder: {msg:?}",
    );
    assert!(msg.contains("assigned to"));
}

/// Completion narrates INFO "completed on {secondary}-{worker}" on the
/// per-task observer-task target (non-wake-worthy), where the holder is
/// the PRIOR InFlight holder carried on the event.
#[test]
fn completed_narrates_info_with_prior_holder() {
    let events = capture(|| {
        let n = armed();
        assert!(n.narrate_live(&evt("t2", TaskStateChange::Completed, Some(("sec-b", 7)))));
    });
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].target, OBSERVER_TASK_TARGET, "non-wake target");
    assert_eq!(events[0].leveled.level, Level::INFO);
    let msg = &events[0].leveled.event.message;
    assert!(msg.contains("t2"));
    assert!(msg.contains("completed on"));
    assert!(msg.contains("sec-b-7"));
}

/// A terminal failure narrates ERROR on the importance marker
/// (wake-worthy: reaches stdio under `--important-stdio-only`) and
/// carries BOTH the reason AND the full last_error.
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
    assert_eq!(events[0].target, IMPORTANT_TARGET, "wake-worthy target");
    assert_eq!(events[0].leveled.level, Level::ERROR, "terminal fail is ERROR");
    let msg = &events[0].leveled.event.message;
    assert!(msg.contains("terminally failed on"));
    assert!(msg.contains("sec-c-1"));
    assert!(
        msg.contains("index out of bounds"),
        "the FULL last_error rides the ERROR line: {msg:?}",
    );
}

/// A recoverable failure narrates WARN "(recoverable)" on the
/// importance marker (wake-worthy).
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
    assert_eq!(events[0].target, IMPORTANT_TARGET, "wake-worthy target");
    assert_eq!(events[0].leveled.level, Level::WARN, "recoverable fail is WARN");
    let msg = &events[0].leveled.event.message;
    assert!(msg.contains("(recoverable)"));
    assert!(msg.contains("sec-d-0"));
}

/// An OOM failure narrates WARN "(oom)" on the importance marker
/// (wake-worthy).
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
    assert_eq!(events[0].target, IMPORTANT_TARGET, "wake-worthy target");
    assert_eq!(events[0].leveled.level, Level::WARN, "oom fail is WARN");
    assert!(events[0].leveled.event.message.contains("(oom)"));
}

/// Any other (non-terminal / non-fail) transition narrates INFO
/// "changed state to {state}" on the per-task observer-task target
/// (non-wake-worthy).
#[test]
fn other_state_change_narrates_info() {
    let events = capture(|| {
        let n = armed();
        assert!(n.narrate_live(&evt("t6", TaskStateChange::Other { state: "blocked" }, None)));
    });
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].target, OBSERVER_TASK_TARGET, "non-wake target");
    assert_eq!(events[0].leveled.level, Level::INFO);
    let msg = &events[0].leveled.event.message;
    assert!(msg.contains("t6"));
    assert!(msg.contains("changed state to blocked"));
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
    assert_eq!(events[0].target, OBSERVER_TASK_TARGET, "non-wake target");
    assert!(events[0].leveled.event.message.contains("unknown-holder"));
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
    // Baseline summary is a once-per-run milestone: wake-worthy.
    assert_eq!(events[0].target, IMPORTANT_TARGET, "baseline summary is wake-worthy");
    assert_eq!(events[0].leveled.level, Level::INFO);
    let msg = &events[0].leveled.event.message;
    assert!(msg.contains("mirroring baseline"));
    assert!(
        msg.contains("60000") && msg.contains("4000"),
        "summary carries the converged partition: {msg:?}",
    );
}

/// An EMPTY baseline (cold-join before any snapshot) emits NO summary
/// line but still arms live narration; the live line lands on the
/// per-task (non-wake-worthy) target.
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
    assert_eq!(events[0].target, OBSERVER_TASK_TARGET, "non-wake target");
    assert!(events[0].leveled.event.message.contains("assigned to"));
}
