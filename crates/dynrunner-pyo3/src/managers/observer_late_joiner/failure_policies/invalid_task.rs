//! Policy B — the observer's invalid_task monitor.
//!
//! # Single concern
//!
//! Watch the terminal-failure stream for `invalid_task`-kind failures.
//! On the FIRST one, arm a 1-minute collection window (so a batch of
//! invalid tasks landing together are all logged at once), then signal
//! the observer's fatal-exit with a reason that names the invalid tasks.
//! ONE-SHOT: after the signal there is nothing left to do.
//!
//! Per owner decision B: only the OBSERVER exits on `invalid_task` — the
//! cluster (primary + secondaries) keeps running. This policy is the
//! single place `invalid_task` presence drives an observer exit.
//!
//! # How it reaches the observer's exit (the `fatal_exit` mechanism)
//!
//! The collector's action runs on the collector driver task, which holds
//! no `&mut SecondaryCoordinator`, so it cannot write `fatal_exit`
//! directly. Instead the policy fires a SIGNAL on a channel — exactly
//! mirroring the panik-watcher signal pattern (`register_panik_signal_rx`
//! → a `select!` arm that sets `fatal_exit`). The integration site holds
//! the matching receiver; the observer's operational loop consumes it and
//! latches `fatal_exit`, exiting the run non-zero. This keeps the policy
//! free of any `std::process::exit` and makes the trigger a plain,
//! assertable channel send in tests.

use std::time::Duration;

use tokio::sync::mpsc::UnboundedSender;
use tokio::time::Instant;

use dynrunner_core::IMPORTANT_TARGET;
use dynrunner_manager_distributed::task_completed::{
    CollectedFailure, CollectorPolicy, TaskCompletedEvent,
};

/// The invalid_task collection window: a batch of invalid tasks that
/// land within a minute of the first are all collected before the
/// observer exits, so the operator sees the full set in one log burst
/// rather than racing the exit against the first one.
pub const INVALID_TASK_WINDOW: Duration = Duration::from_secs(60);

/// The `error_kind` tag prefix every `ErrorType::InvalidTask` failure
/// carries (`ErrorType::wire_value` → `"invalid_task:<reason>"`). The
/// monitor filters on this prefix rather than re-deriving the typed
/// `ErrorType`, matching the contract `TaskCompletedEvent::error_kind`
/// documents (the wire-stable tag is the consumer-facing identity).
pub const INVALID_TASK_KIND_PREFIX: &str = "invalid_task:";

/// Fatal-exit signal sink. The policy fires the assembled exit reason
/// once; the observer's operational loop receives it and latches
/// `fatal_exit`. An `UnboundedSender` (not a oneshot) so the send is
/// infallible-at-the-policy and the receiver side decides ordering — and
/// so a test can simply drain the channel to assert the trigger.
pub type ObserverFatalExit = UnboundedSender<String>;

/// Policy B as a [`CollectorPolicy`]. Matches only `invalid_task:*`
/// failures, uses a constant 1-minute window, does NOT re-arm, and on
/// window-elapse fires the fatal-exit signal with a reason listing every
/// invalid task observed.
pub struct InvalidTaskMonitorPolicy {
    fatal_exit: ObserverFatalExit,
}

impl InvalidTaskMonitorPolicy {
    pub fn new(fatal_exit: ObserverFatalExit) -> Self {
        Self { fatal_exit }
    }
}

impl CollectorPolicy for InvalidTaskMonitorPolicy {
    fn matches(&self, event: &TaskCompletedEvent) -> bool {
        event
            .error_kind
            .as_deref()
            .is_some_and(|k| k.starts_with(INVALID_TASK_KIND_PREFIX))
    }

    fn window_for(&mut self, _now: Instant) -> Duration {
        INVALID_TASK_WINDOW
    }

    fn on_window_elapsed(&mut self, collected: Vec<CollectedFailure>, _now: Instant) {
        // Build the operator-facing reason: one line per distinct invalid
        // task, with the repeat count for identical messages. Logged here
        // (so the full set is in the log file even under the importance
        // filter) AND carried in the fatal-exit reason.
        let detail = render_invalid_tasks(&collected);
        tracing::error!(
            target: IMPORTANT_TARGET,
            "observer exiting: {} invalid task(s) observed — these can never run \
             and the cluster will not complete them:\n{detail}",
            distinct_count(&collected),
        );
        // Fire the fatal-exit signal. Best-effort: a dropped receiver
        // (the run loop already exiting for another reason) makes this a
        // no-op, which is correct — we only ever wanted to ensure the
        // observer exits non-zero.
        let _ = self.fatal_exit.send(format!(
            "observer: {} invalid task(s) observed (terminal, non-recoverable): {}",
            distinct_count(&collected),
            summary_line(&collected),
        ));
    }

    fn rearm_after_fire(&self) -> bool {
        // ONE-SHOT: the observer is exiting; no second window.
        false
    }
}

/// Number of distinct invalid-task messages collected.
fn distinct_count(collected: &[CollectedFailure]) -> usize {
    collected.len()
}

/// Multi-line detail: one block per distinct failure with its message,
/// the representative task id, and how many other tasks shared the
/// message.
fn render_invalid_tasks(collected: &[CollectedFailure]) -> String {
    collected
        .iter()
        .map(|f| {
            let msg = f
                .representative
                .last_error
                .as_deref()
                .unwrap_or("<no message>");
            let kind = f
                .representative
                .error_kind
                .as_deref()
                .unwrap_or("invalid_task:");
            let repeat = if f.repeat_count > 0 {
                format!(" (x{} other tasks)", f.repeat_count)
            } else {
                String::new()
            };
            format!(
                "  - task {} [{}]: {}{}",
                f.representative.task_id, kind, msg, repeat
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Single-line summary for the fatal-exit reason (the multi-line detail
/// already went to the log via `tracing::error!`).
fn summary_line(collected: &[CollectedFailure]) -> String {
    collected
        .iter()
        .map(|f| {
            let msg = f
                .representative
                .last_error
                .as_deref()
                .unwrap_or("<no message>");
            if f.repeat_count > 0 {
                format!("{} (x{} others)", msg, f.repeat_count)
            } else {
                msg.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("; ")
}
