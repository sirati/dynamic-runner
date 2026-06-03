//! Policy C — the observer's error-aggregation reporter.
//!
//! # Single concern
//!
//! Collapse a burst of task failures into ONE wake-worthy log on the
//! importance channel instead of N noisy lines. Per owner-decision C-5:
//!
//!   * A rolling 10-minute window is established by the first failure.
//!   * On that first failure (and any later one that finds no collection
//!     sub-window armed) collect for `min(1 minute, remainder of the
//!     current rolling 10-min window)`, then emit every distinct error in
//!     detail — `xN other tasks` appended for identical messages.
//!   * Each distinct error message is emitted at most ONCE per rolling
//!     10-minute window (a message already reported earlier in the same
//!     rolling window is suppressed on a later sub-window).
//!   * The rolling window RESETS every 10 minutes: a failure arriving
//!     after the window expires starts a fresh window with fresh
//!     reported-message memory.
//!   * NEVER exits — this is pure observability.
//!
//! # Two layers of dedup
//!
//! The shared collector already dedups WITHIN one collection sub-window
//! (identical messages collapse to one [`CollectedFailure`] with a
//! repeat count). This policy adds the SECOND layer the spec requires:
//! across sub-windows of the same rolling 10-min window, a message
//! already reported is dropped entirely (not re-emitted, not re-counted).
//! The first layer is the primitive's concern; the second is this
//! policy's, because "once per rolling window" is a policy rule, not a
//! window mechanic.

use std::collections::HashSet;
use std::time::Duration;

use tokio::time::Instant;

use dynrunner_core::IMPORTANT_TARGET;
use dynrunner_manager_distributed::task_completed::{
    CollectedFailure, CollectorPolicy, TaskCompletedEvent,
};

/// The outer rolling window: dedup memory + window-boundary cadence.
pub const ROLLING_WINDOW: Duration = Duration::from_secs(600);
/// The maximum collection sub-window. Capped to `min(this, rolling
/// remainder)` so a sub-window never spills past the rolling boundary.
pub const COLLECT_CAP: Duration = Duration::from_secs(60);

/// Policy C as a [`CollectorPolicy`]. Matches every failure, computes a
/// `min(1min, rolling-remainder)` sub-window, rolls + resets its dedup
/// memory every 10 minutes, and emits a deduped detail report on the
/// importance channel each time a sub-window elapses. Re-arms forever.
pub struct ErrorAggregationPolicy {
    /// Start of the CURRENT rolling 10-minute window, or `None` before
    /// the first failure. Reset to `now` whenever a failure arrives at or
    /// after `rolling_start + ROLLING_WINDOW`.
    rolling_start: Option<Instant>,
    /// Distinct `last_error` messages already EMITTED in the current
    /// rolling window. Cleared on each rollover. Suppresses repeat emits
    /// of the same message across sub-windows of one rolling window.
    reported_this_window: HashSet<String>,
}

impl Default for ErrorAggregationPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl ErrorAggregationPolicy {
    pub fn new() -> Self {
        Self {
            rolling_start: None,
            reported_this_window: HashSet::new(),
        }
    }

    /// Roll the window over if `now` is at/after the current window's end
    /// (or no window exists yet): start a fresh window at `now` and clear
    /// the reported-message memory. Returns the (possibly fresh) window
    /// start. Idempotent within a window.
    fn roll_to(&mut self, now: Instant) -> Instant {
        let needs_roll = match self.rolling_start {
            None => true,
            Some(start) => now.duration_since(start) >= ROLLING_WINDOW,
        };
        if needs_roll {
            self.rolling_start = Some(now);
            self.reported_this_window.clear();
        }
        // Safe: either we just set it, or it was already Some.
        self.rolling_start.expect("rolling_start set by roll_to")
    }
}

impl CollectorPolicy for ErrorAggregationPolicy {
    fn matches(&self, _event: &TaskCompletedEvent) -> bool {
        // Every failure participates (successes are filtered upstream by
        // the collector before the policy is consulted).
        true
    }

    fn window_for(&mut self, now: Instant) -> Duration {
        // Arm-time is the natural point to roll the outer window: a fresh
        // sub-window is starting, so check whether it belongs to a new
        // rolling 10-min window and reset the dedup memory if so.
        let start = self.roll_to(now);
        let elapsed = now.duration_since(start);
        let remainder = ROLLING_WINDOW.saturating_sub(elapsed);
        // min(1min, remainder): never collect past the rolling boundary.
        COLLECT_CAP.min(remainder)
    }

    fn on_window_elapsed(&mut self, collected: Vec<CollectedFailure>, _now: Instant) {
        // Apply the second dedup layer: drop any message already reported
        // earlier in THIS rolling window; record the survivors as reported.
        let mut fresh: Vec<&CollectedFailure> = Vec::new();
        for failure in &collected {
            let key = failure
                .representative
                .last_error
                .clone()
                .unwrap_or_default();
            if self.reported_this_window.insert(key) {
                fresh.push(failure);
            }
        }
        if fresh.is_empty() {
            // Every message in this sub-window was already reported this
            // rolling window — nothing wake-worthy to add.
            return;
        }
        let detail = render_failures(&fresh);
        tracing::info!(
            target: IMPORTANT_TARGET,
            "task failures (aggregated):\n{detail}",
        );
    }

    fn rearm_after_fire(&self) -> bool {
        // Rolling forever — every new failure may arm a fresh sub-window.
        true
    }
}

/// Multi-line detail: one block per distinct failure with its message,
/// the error kind, the representative task id, and `xN other tasks` for
/// identical-message repeats collected in the same sub-window.
fn render_failures(failures: &[&CollectedFailure]) -> String {
    failures
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
                .unwrap_or("<unknown>");
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
