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
use crate::task_state_change::{NarrationSource, TaskStateChange, TaskStateChangeEvent, TaskTxnId};
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
    // Default: a CREATE (no prior state) with a cold txn id. The from→to
    // and txn-id tests build their own events with `evt_from` / `evt_txn`.
    TaskStateChangeEvent {
        task_id: task_id.to_string(),
        change,
        holder: holder.map(|(s, w)| (s.to_string(), w)),
        from: None,
        txn: TaskTxnId { primary_epoch: 0, seq: 0, attempt: 0 },
        // The narrate_live unit tests drive the LIVE path (CatchUp routing
        // is exercised end-to-end in the coordinator integration test).
        source: NarrationSource::LiveBroadcast,
    }
}

/// An event carrying a known FROM-state and a non-trivial CRDT txn id —
/// for the from→to + correlator assertions.
fn evt_from(
    task_id: &str,
    change: TaskStateChange,
    holder: Option<(&str, u32)>,
    from: &'static str,
    txn: TaskTxnId,
) -> TaskStateChangeEvent {
    TaskStateChangeEvent {
        task_id: task_id.to_string(),
        change,
        holder: holder.map(|(s, w)| (s.to_string(), w)),
        from: Some(from),
        txn,
        source: NarrationSource::LiveBroadcast,
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

/// T3 (#587): a terminal failure narrates ERROR on the
/// `OBSERVER_TASK_TARGET` — under the #583/#587 per-narration-kind
/// classification the per-task class is uniformly HIGH-VOLUME, so the
/// per-event ERROR is suppressed from stdio under
/// `--important-stdio-only`; the rate-limited
/// `ErrorAggregationPolicy` rollup on `IMPORTANT_TARGET` is the wake
/// signal. The line still carries BOTH the reason AND the full
/// last_error.
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
    assert_eq!(
        events[0].target, OBSERVER_TASK_TARGET,
        "per-task terminal failure is HIGH-VOLUME (#587); wake signal is the aggregator rollup",
    );
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
/// per-task observer-task target (HIGH-VOLUME under #583/#587, like
/// every per-task arm).
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
    assert_eq!(events[0].target, OBSERVER_TASK_TARGET, "per-task HIGH-VOLUME (#587)");
    assert_eq!(events[0].leveled.level, Level::WARN, "recoverable fail is WARN");
    let msg = &events[0].leveled.event.message;
    assert!(msg.contains("(recoverable)"));
    assert!(msg.contains("sec-d-0"));
}

/// An OOM failure narrates WARN "(oom)" on the per-task observer-task
/// target (HIGH-VOLUME under #583/#587, like every per-task arm).
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
    assert_eq!(events[0].target, OBSERVER_TASK_TARGET, "per-task HIGH-VOLUME (#587)");
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

/// KIND SPLIT: the baseline line reports SETUP tasks under `setup-`
/// categories and per-secondary affine GATE tokens as a single flat
/// `secondary-affine` count — and NEITHER is folded into the generic
/// `pending`. The bug this pins: a baseline with 100 work-pending + 30
/// setup-pending + 200 affine tokens previously rendered "330 pending".
#[test]
fn baseline_splits_setup_and_affine_out_of_generic_pending() {
    let events = capture(|| {
        let mut n = ObserverTaskNarrator::default();
        let counts = StateCounts {
            pending: 100,
            setup_pending: 30,
            secondary_affine: 200,
            setup_succeeded: 5,
            ..Default::default()
        };
        n.narrate_baseline(335, counts);
    });
    assert_eq!(events.len(), 1, "one summary line");
    let msg = &events[0].leveled.event.message;
    // The generic pending is the WORK count alone — NOT the lumped 330.
    assert!(
        msg.contains("100 pending"),
        "generic pending is work-only (100), not the lumped total: {msg:?}",
    );
    assert!(!msg.contains("330"), "the old lumped 330 must NOT appear: {msg:?}");
    // Setup tasks under setup- categories.
    assert!(msg.contains("30 setup-pending"), "setup-pending split out: {msg:?}");
    assert!(msg.contains("5 setup-done"), "setup-done present: {msg:?}");
    // Affine tokens as ONE flat count.
    assert!(
        msg.contains("200 secondary-affine"),
        "affine reported flat, no state subdivision: {msg:?}",
    );
}

/// FROM→TO: a non-terminal transition with a known prior state narrates
/// "changed state from {prev} to {new}" and carries the CRDT txn id.
#[test]
fn other_transition_narrates_from_to_with_txn() {
    let events = capture(|| {
        let n = armed();
        assert!(n.narrate_live(&evt_from(
            "t1",
            TaskStateChange::Other { state: "pending" },
            None,
            "failed",
            TaskTxnId { primary_epoch: 3, seq: 7, attempt: 1 },
        )));
    });
    assert_eq!(events.len(), 1, "{events:?}");
    let msg = &events[0].leveled.event.message;
    assert!(
        msg.contains("changed state from failed to pending"),
        "from→to transition: {msg:?}",
    );
    assert!(msg.contains("crdt_txn=e3.v7.a1"), "CRDT txn id: {msg:?}");
}

/// A CREATE (no prior state) into a MEANINGFUL first state narrates the
/// bare "changed state to {new}" (no dangling arrow) and still carries the
/// txn id. `blocked` (a dependency wait) is such a state — distinct from
/// the suppressed `pending` seed (see
/// [`create_into_pending_seed_is_suppressed`]).
#[test]
fn other_transition_create_has_no_from_arrow() {
    let events = capture(|| {
        let n = armed();
        assert!(n.narrate_live(&evt("t1", TaskStateChange::Other { state: "blocked" }, None)));
    });
    let msg = &events[0].leveled.event.message;
    assert!(msg.contains("changed state to blocked"), "{msg:?}");
    assert!(!msg.contains("from"), "no from-arrow on a CREATE: {msg:?}");
    assert!(msg.contains("crdt_txn=e0.v0.a0"), "txn id even on a CREATE: {msg:?}");
}

/// #651 SEED-FLOOD guard (RED→GREEN): a CREATE (no prior state) into the
/// DEFAULT `pending` state — the initial baseline seed — narrates NOTHING
/// per task (returns `false`, emits no line). At 120k tasks a relocated
/// observer mirrors the recompose-seed as 120k LIVE CREATE-into-pending
/// broadcasts; one operator line each made narration O(tasks-at-seed) (a
/// multi-hour, 300MB+ replay that wedged the observer behind the seed). The
/// converged seed is the once-per-run baseline summary's job, not a
/// per-task line. RED before the guard: this asserted a "changed state to
/// pending" line; GREEN now: zero lines.
#[test]
fn create_into_pending_seed_is_suppressed() {
    let events = capture(|| {
        let n = armed();
        // The seed shape: a CREATE (from = None) into pending.
        assert!(
            !n.narrate_live(&evt("seed-1", TaskStateChange::Other { state: "pending" }, None)),
            "a CREATE-into-pending seed must narrate nothing per task",
        );
        // A WHOLE seed batch is O(1) narration (zero lines) — this stands
        // in for the 120k-task seed flood the guard collapses.
        for i in 0..1000 {
            assert!(!n.narrate_live(&evt(
                &format!("seed-{i}"),
                TaskStateChange::Other { state: "pending" },
                None,
            )));
        }
    });
    assert_eq!(
        events.len(),
        0,
        "the seed batch narrates ZERO per-task lines, not one-per-task: {} lines",
        events.len(),
    );
}

/// #651 boundary: a RE-entry into pending (a known prior state — a requeue
/// / cascade resume) is a GENUINE transition and STILL narrates per-task.
/// Only the initial seed (CREATE-into-pending, no prior state) is
/// suppressed; a task that legitimately returns to pending from a known
/// state is the operator's interest.
#[test]
fn reentry_into_pending_still_narrates() {
    let events = capture(|| {
        let n = armed();
        assert!(
            n.narrate_live(&evt_from(
                "requeued",
                TaskStateChange::Other { state: "pending" },
                None,
                "failed",
                TaskTxnId { primary_epoch: 2, seq: 4, attempt: 1 },
            )),
            "a transition BACK to pending from a known state is a real change",
        );
    });
    assert_eq!(events.len(), 1, "the re-entry narrates one line: {events:?}");
    let msg = &events[0].leveled.event.message;
    assert!(
        msg.contains("changed state from failed to pending"),
        "re-entry names the prior state: {msg:?}",
    );
}

/// An assignment with a known prior state renders the symmetric
/// `(pending→in-flight)` transition alongside the holder + txn id.
#[test]
fn assigned_narrates_from_to_transition() {
    let events = capture(|| {
        let n = armed();
        assert!(n.narrate_live(&evt_from(
            "t1",
            TaskStateChange::Assigned,
            Some(("sec-a", 3)),
            "pending",
            TaskTxnId { primary_epoch: 1, seq: 2, attempt: 0 },
        )));
    });
    let msg = &events[0].leveled.event.message;
    assert!(msg.contains("assigned to sec-a-3"), "{msg:?}");
    assert!(msg.contains("(pending→in-flight)"), "from→to transition: {msg:?}");
    assert!(msg.contains("crdt_txn=e1.v2.a0"), "{msg:?}");
}

/// A completion with a known prior `in-flight` renders
/// `(in-flight→completed)` + the holder + the txn id.
#[test]
fn completed_narrates_from_to_transition() {
    let events = capture(|| {
        let n = armed();
        assert!(n.narrate_live(&evt_from(
            "t2",
            TaskStateChange::Completed,
            Some(("sec-b", 7)),
            "in-flight",
            TaskTxnId { primary_epoch: 0, seq: 0, attempt: 2 },
        )));
    });
    let msg = &events[0].leveled.event.message;
    assert!(msg.contains("completed on sec-b-7"), "{msg:?}");
    assert!(msg.contains("(in-flight→completed)"), "from→to transition: {msg:?}");
    // A version-less Completed reports the attempt-only coordinate.
    assert!(msg.contains("crdt_txn=e0.v0.a2"), "version-less completed txn: {msg:?}");
}

/// An `evt` re-tagged as a snapshot-restore CATCH-UP transition (the
/// shape a relocated / late-join observer's in-loop bootstrap restore
/// fires) — the `observe`-routing tests below.
fn catch_up(task_id: &str, change: TaskStateChange, holder: Option<(&str, u32)>) -> TaskStateChangeEvent {
    TaskStateChangeEvent { source: NarrationSource::CatchUp, ..evt(task_id, change, holder) }
}

/// CATCH-UP ROUTING (#636-followup): `observe` folds N CatchUp
/// transitions into ONE summary on `flush_catch_up` (naming the distinct
/// task + transition counts), narrates NOTHING per-task for them, and the
/// counter RESETS per batch — while a LiveBroadcast event in the SAME
/// stream still narrates individually.
#[test]
fn observe_folds_catch_up_into_one_summary_and_narrates_live_individually() {
    let events = capture(|| {
        let mut n = armed();
        // Three restored InFlight transitions over THREE distinct tasks +
        // one re-touch of the first (4 transitions, 3 tasks) — accumulate,
        // emit nothing yet.
        for id in ["a", "b", "c", "a"] {
            assert!(
                !n.observe(&catch_up(id, TaskStateChange::Assigned, Some(("sec-0", 1)))),
                "a catch-up transition narrates nothing individually"
            );
        }
        // A genuine LIVE assignment in the same stream → ONE individual line.
        assert!(
            n.observe(&evt("live", TaskStateChange::Assigned, Some(("sec-0", 2)))),
            "a live-broadcast transition narrates individually"
        );
        // Terminal package: flush the batch → ONE summary line.
        assert!(n.flush_catch_up(), "a non-empty batch flushes one summary");
        // Counter reset: a second flush over an empty batch emits nothing.
        assert!(!n.flush_catch_up(), "an empty batch (post-reset) emits nothing");
    });
    let summaries: Vec<_> = events
        .iter()
        .filter(|e| e.leveled.event.message.contains("observer caught up"))
        .collect();
    assert_eq!(summaries.len(), 1, "exactly one catch-up summary: {events:?}");
    assert_eq!(summaries[0].target, IMPORTANT_TARGET, "summary is wake-worthy");
    assert_eq!(
        summaries[0].leveled.event.fields.get("catch_up_tasks").map(String::as_str),
        Some("3"),
        "3 distinct catch-up tasks (a re-touched): {:?}",
        summaries[0]
    );
    assert_eq!(
        summaries[0].leveled.event.fields.get("catch_up_transitions").map(String::as_str),
        Some("4"),
        "4 catch-up transitions: {:?}",
        summaries[0]
    );
    // The ONLY individual per-task line is the LIVE one (none of a/b/c).
    let per_task: Vec<_> = events
        .iter()
        .filter(|e| e.target == OBSERVER_TASK_TARGET)
        .collect();
    assert_eq!(per_task.len(), 1, "only the live transition narrates per-task: {events:?}");
    assert!(per_task[0].leveled.event.message.contains("task live assigned"));
}

/// An empty catch-up batch (a `done` over a converged re-stream that won
/// no transitions) flushes NOTHING — the quiescent-observer no-op.
#[test]
fn empty_catch_up_batch_flushes_nothing() {
    let events = capture(|| {
        let mut n = armed();
        assert!(!n.flush_catch_up(), "an empty batch emits no summary");
    });
    assert!(events.is_empty(), "no line for an empty catch-up batch: {events:?}");
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
