//! Deterministic tests for the two observer failure policies, exercised
//! THROUGH the real shared collector primitive.
//!
//! Clock: `tokio::time::pause` + `advance` so every window boundary fires
//! at a known virtual instant with zero wall-clock race.
//!
//! Tracing: a thread-local `set_default` subscriber + a tiny capture
//! layer records every `dynrunner_important` event so Policy C's emit
//! cadence + dedup are assertable. The current-thread tokio runtime runs
//! the spawned driver on the same thread, so the thread-scoped subscriber
//! guard sees the driver's events too.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc::unbounded_channel;

use dynrunner_core::IMPORTANT_TARGET;
use dynrunner_manager_distributed::task_completed::{
    TaskCompletedEvent, run_collector, windowed_failure_collector,
};

use super::aggregation::ErrorAggregationPolicy;
use super::invalid_task::InvalidTaskMonitorPolicy;

// ── tracing capture (importance channel) ──

/// Captures the `message` field of every `dynrunner_important` event.
struct ImportantCapture {
    records: Arc<Mutex<Vec<String>>>,
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for ImportantCapture {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if event.metadata().target() != IMPORTANT_TARGET {
            return;
        }
        struct Visitor(String);
        impl tracing::field::Visit for Visitor {
            fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                if field.name() == "message" {
                    self.0 = value.to_string();
                }
            }
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                if field.name() == "message" {
                    self.0 = format!("{value:?}");
                }
            }
        }
        let mut visitor = Visitor(String::new());
        event.record(&mut visitor);
        if let Ok(mut buf) = self.records.lock() {
            buf.push(visitor.0);
        }
    }
}

/// Install a thread-local importance-channel capture for the rest of the
/// current thread's scope. Returns the shared buffer + the guard (drop to
/// restore the prior subscriber).
fn capture_important() -> (Arc<Mutex<Vec<String>>>, tracing::dispatcher::DefaultGuard) {
    use tracing_subscriber::layer::SubscriberExt;
    let records: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let layer = ImportantCapture {
        records: Arc::clone(&records),
    };
    let subscriber = tracing_subscriber::Registry::default().with(layer);
    let guard = tracing::subscriber::set_default(subscriber);
    (records, guard)
}

// ── fixtures ──

fn invalid(task_id: &str, reason: &str) -> TaskCompletedEvent {
    TaskCompletedEvent {
        task_id: task_id.into(),
        task_hash: format!("hash-{task_id}"),
        success: false,
        error_kind: Some(format!("invalid_task:{reason}")),
        last_error: Some(format!("invalid task: {reason}")),
    }
}

fn fail(task_id: &str, kind: &str, message: &str) -> TaskCompletedEvent {
    TaskCompletedEvent {
        task_id: task_id.into(),
        task_hash: format!("hash-{task_id}"),
        success: false,
        error_kind: Some(kind.into()),
        last_error: Some(message.into()),
    }
}

// ── Policy B — invalid_task monitor ──

/// First invalid_task arms a 1-minute window; on elapse the policy fires
/// the fatal-exit signal exactly once with a reason naming the invalid
/// tasks. Asserts the SIGNAL — the test process never exits.
#[tokio::test(start_paused = true)]
async fn policy_b_arms_one_minute_then_signals_fatal_exit() {
    let (exit_tx, mut exit_rx) = unbounded_channel::<String>();
    let policy = InvalidTaskMonitorPolicy::new(exit_tx);
    let (listener, driver) = windowed_failure_collector(policy);
    let cancel = tokio::sync::oneshot::channel::<()>();
    let driver_task = tokio::spawn(run_collector(driver, async move {
        let _ = cancel.1.await;
    }));

    // First invalid_task arms the window; a second distinct + a repeat
    // join the same window.
    listener.on_event(&invalid("t1", "missing dep a"));
    listener.on_event(&invalid("t2", "duplicate id"));
    listener.on_event(&invalid("t3", "missing dep a")); // dup message of t1

    // Before the 1-minute window elapses: no signal yet.
    tokio::time::advance(Duration::from_secs(59)).await;
    tokio::task::yield_now().await;
    assert!(
        exit_rx.try_recv().is_err(),
        "must not signal before the 1-minute window elapses"
    );

    // Cross the window boundary → fatal-exit signal fires once.
    tokio::time::advance(Duration::from_secs(2)).await;
    tokio::task::yield_now().await;
    let reason = exit_rx
        .try_recv()
        .expect("fatal-exit signal must fire after the 1-minute window");
    assert!(
        reason.contains("invalid task"),
        "reason names the cause: {reason}"
    );
    assert!(
        reason.contains("2 invalid task"),
        "two distinct invalid-task messages collected: {reason}"
    );

    // One-shot: a later invalid_task does NOT fire a second signal.
    listener.on_event(&invalid("t4", "another missing dep"));
    tokio::time::advance(Duration::from_secs(120)).await;
    tokio::task::yield_now().await;
    assert!(
        exit_rx.try_recv().is_err(),
        "Policy B is one-shot — no second fatal-exit signal"
    );

    drop(cancel.0);
    let _ = driver_task.await;
}

/// A non-invalid_task failure (e.g. NonRecoverable) never arms Policy B's
/// window, so the observer does NOT exit on ordinary failures.
#[tokio::test(start_paused = true)]
async fn policy_b_ignores_non_invalid_failures() {
    let (exit_tx, mut exit_rx) = unbounded_channel::<String>();
    let policy = InvalidTaskMonitorPolicy::new(exit_tx);
    let (listener, driver) = windowed_failure_collector(policy);
    let cancel = tokio::sync::oneshot::channel::<()>();
    let driver_task = tokio::spawn(run_collector(driver, async move {
        let _ = cancel.1.await;
    }));

    listener.on_event(&fail("nf", "non_recoverable", "boom"));
    listener.on_event(&fail("oom", "oom", "oom-killed"));
    tokio::time::advance(Duration::from_secs(180)).await;
    tokio::task::yield_now().await;
    assert!(
        exit_rx.try_recv().is_err(),
        "ordinary failures must not trip the observer's invalid_task exit"
    );

    drop(cancel.0);
    let _ = driver_task.await;
}

// ── Policy C — error aggregation ──

/// Within one rolling 10-min window the policy emits each distinct
/// message once, with `xN other tasks` for repeats, and does NOT re-emit
/// a message that recurs in a later sub-window of the same rolling window.
#[tokio::test(start_paused = true)]
async fn policy_c_emits_once_per_distinct_message_within_rolling_window() {
    let (records, _guard) = capture_important();
    let policy = ErrorAggregationPolicy::new();
    let (listener, driver) = windowed_failure_collector(policy);
    let cancel = tokio::sync::oneshot::channel::<()>();
    let driver_task = tokio::spawn(run_collector(driver, async move {
        let _ = cancel.1.await;
    }));

    // Sub-window 1: two distinct messages, one repeated.
    listener.on_event(&fail("a", "non_recoverable", "boom"));
    listener.on_event(&fail("b", "non_recoverable", "boom")); // repeat
    listener.on_event(&fail("c", "oom", "oom-killed"));
    tokio::time::advance(Duration::from_secs(61)).await; // > 1 min collect cap
    tokio::task::yield_now().await;

    {
        let r = records.lock().unwrap();
        assert_eq!(r.len(), 1, "one aggregated emit for sub-window 1: {r:?}");
        assert!(r[0].contains("boom"), "boom reported: {}", r[0]);
        assert!(
            r[0].contains("x1 other tasks"),
            "boom's repeat rendered as xN: {}",
            r[0]
        );
        assert!(r[0].contains("oom-killed"), "oom-killed reported: {}", r[0]);
    }

    // Sub-window 2, SAME rolling window (~t=61s): a new message + a recur
    // of "boom". Only the new message should emit; "boom" is suppressed.
    listener.on_event(&fail("d", "non_recoverable", "boom")); // already reported
    listener.on_event(&fail("e", "non_recoverable", "splat")); // new
    tokio::time::advance(Duration::from_secs(61)).await;
    tokio::task::yield_now().await;

    {
        let r = records.lock().unwrap();
        assert_eq!(r.len(), 2, "second aggregated emit: {r:?}");
        assert!(r[1].contains("splat"), "new message reported: {}", r[1]);
        assert!(
            !r[1].contains("boom"),
            "already-reported message suppressed this rolling window: {}",
            r[1]
        );
    }

    drop(cancel.0);
    let _ = driver_task.await;
}

/// After the rolling 10-min window expires, the dedup memory resets: a
/// message reported in the first window is reported AGAIN in the next.
#[tokio::test(start_paused = true)]
async fn policy_c_resets_dedup_each_rolling_window() {
    let (records, _guard) = capture_important();
    let policy = ErrorAggregationPolicy::new();
    let (listener, driver) = windowed_failure_collector(policy);
    let cancel = tokio::sync::oneshot::channel::<()>();
    let driver_task = tokio::spawn(run_collector(driver, async move {
        let _ = cancel.1.await;
    }));

    // Rolling window 1 starts at t=0; report "boom".
    listener.on_event(&fail("a", "non_recoverable", "boom"));
    tokio::time::advance(Duration::from_secs(61)).await;
    tokio::task::yield_now().await;
    assert_eq!(records.lock().unwrap().len(), 1);

    // Advance well past the rolling 10-min boundary (t=0..600). The next
    // failure at ~t=620s starts a FRESH rolling window with cleared dedup
    // memory, so "boom" reports again.
    tokio::time::advance(Duration::from_secs(600)).await;
    tokio::task::yield_now().await;
    listener.on_event(&fail("z", "non_recoverable", "boom"));
    tokio::time::advance(Duration::from_secs(61)).await;
    tokio::task::yield_now().await;

    {
        let r = records.lock().unwrap();
        assert_eq!(
            r.len(),
            2,
            "boom re-reported in the new rolling window: {r:?}"
        );
        assert!(
            r[1].contains("boom"),
            "re-report carries the message: {}",
            r[1]
        );
    }

    drop(cancel.0);
    let _ = driver_task.await;
}

/// Sub-window length is `min(1min, rolling-remainder)`: a failure landing
/// near the end of a rolling window collects only until the rolling
/// boundary, never spilling into the next window.
#[tokio::test(start_paused = true)]
async fn policy_c_caps_collection_at_rolling_window_remainder() {
    let (records, _guard) = capture_important();
    let policy = ErrorAggregationPolicy::new();
    let (listener, driver) = windowed_failure_collector(policy);
    let cancel = tokio::sync::oneshot::channel::<()>();
    let driver_task = tokio::spawn(run_collector(driver, async move {
        let _ = cancel.1.await;
    }));

    // Establish the rolling window at t=0 and let its first sub-window
    // fire so the window is anchored.
    listener.on_event(&fail("a", "non_recoverable", "first"));
    tokio::time::advance(Duration::from_secs(61)).await;
    tokio::task::yield_now().await;
    assert_eq!(records.lock().unwrap().len(), 1);

    // Advance to t=570s — only 30s remain in the rolling window. A failure
    // here arms a sub-window of min(60s, 30s) = 30s.
    tokio::time::advance(Duration::from_secs(570 - 61)).await; // now ~t=570s
    listener.on_event(&fail("b", "non_recoverable", "near-boundary"));
    // After 29s (t=599s) the capped 30s window has NOT elapsed.
    tokio::time::advance(Duration::from_secs(29)).await;
    tokio::task::yield_now().await;
    assert_eq!(
        records.lock().unwrap().len(),
        1,
        "capped sub-window has not yet elapsed at +29s"
    );
    // At +31s (t=601s, past the 30s cap AND the rolling boundary) it fires.
    tokio::time::advance(Duration::from_secs(2)).await;
    tokio::task::yield_now().await;
    {
        let r = records.lock().unwrap();
        assert_eq!(r.len(), 2, "capped sub-window fired at the boundary: {r:?}");
        assert!(r[1].contains("near-boundary"), "got: {}", r[1]);
    }

    drop(cancel.0);
    let _ = driver_task.await;
}
