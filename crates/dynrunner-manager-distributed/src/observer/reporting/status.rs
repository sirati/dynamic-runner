//! The observer's SUSTAINED-degradation status, folded into the periodic
//! report (#662).
//!
//! # Single concern
//!
//! Decide, AT periodic-report time, whether the observer is in a SUSTAINED
//! (≥ [`STATUS_SUSTAINED_THRESHOLD`], 5 min) degraded state — either its
//! connection has been lost that long, or its CRDT mirror has been behind
//! the cluster that long — and, if so, produce the ONE folded status line
//! the report includes (and that forces the otherwise-idle-skipping report
//! to emit). A transient (<5 min) loss or catch-up produces NOTHING.
//!
//! This REPLACES the per-snapshot-package "observer caught up: N
//! transitions" line (which fired once per incremental catch-up flush and
//! spammed the importance stream under `--important-stdio-only`): the
//! catch-up state is still TRACKED (here, as a since-stamp), but it surfaces
//! to the operator ONLY as this single gated, report-folded line.
//!
//! # Module boundary
//!
//! Two crossing points, both narrow:
//!   * [`CatchUpTracker`] is the PURE since-stamp state machine the observer
//!     coordinator feeds its current "behind the cluster?" boolean each loop
//!     iteration; it owns no clock and no CRDT — the coordinator passes
//!     `now` and the behind-bit, the tracker only remembers WHEN the current
//!     behind-spell began.
//!   * [`StatusCell`] is the shared publish seam (the same shape as
//!     [`super::reporter::SharedSnapshotSource`] / the wake-note slot): the
//!     coordinator PUBLISHES the two since-instants each iteration (the
//!     catch-up stamp from its tracker, the loss stamp from the
//!     [`crate::observer::lost_visibility::LostVisibilityReporter`] it
//!     already owns); the reporter task READS them at report time through
//!     [`StatusCell::status_line`], applying the threshold against its own
//!     [`super::reporter::Clock`]. The reporter never reaches into the
//!     coordinator, and the coordinator never owns the 5-min decision —
//!     each stays single-concern.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::observer::lost_visibility::WAKE_LOSS_THRESHOLD;

/// How long a degraded condition (connection lost / not caught up) must be
/// CONTINUOUSLY true before it surfaces as a folded status line and forces
/// the periodic report to emit. Reuses the wake-loss policy's 5-minute mark
/// ([`WAKE_LOSS_THRESHOLD`]) so the two operator-facing "sustained problem"
/// thresholds are ONE value, not two that can drift apart.
pub const STATUS_SUSTAINED_THRESHOLD: Duration = WAKE_LOSS_THRESHOLD;

/// The two SINCE-stamps the periodic-report status gate reads. Each is
/// `Some(t)` while the corresponding condition is currently true, carrying
/// the instant the CURRENT spell began; `None` while the condition is
/// clear. The reporter compares each against `now` under its own clock.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StatusStamps {
    /// When the current connection-loss episode began (`None` while
    /// connected or merely degraded — a keepalive-addressing gap is not a
    /// loss). Sourced from
    /// [`crate::observer::lost_visibility::LostVisibilityReporter::loss_since_std`].
    pub connection_lost_since: Option<Instant>,
    /// When the observer's CRDT mirror most recently went BEHIND the cluster
    /// (`None` while caught up). Sourced from the coordinator's
    /// [`CatchUpTracker`].
    pub not_caught_up_since: Option<Instant>,
}

/// PURE since-stamp tracker for "the observer's CRDT mirror is behind the
/// cluster". The observer coordinator feeds it the current behind-bit (any
/// known peer's digest shows the local replica behind / an inbound
/// snapshot-stream catch-up is outstanding) once per loop iteration; the
/// tracker remembers WHEN the current behind-spell began so the report gate
/// can tell a sustained catch-up from a transient one. Owns no clock — the
/// caller passes `now`.
#[derive(Debug, Default)]
pub struct CatchUpTracker {
    since: Option<Instant>,
}

impl CatchUpTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fold this iteration's behind-bit in. Stamps the spell start on the
    /// transition into "behind" (and keeps the ORIGINAL start across a
    /// sustained spell — never refreshes it, so the elapsed time the gate
    /// reads measures the WHOLE spell); clears the stamp the instant the
    /// mirror catches up. Returns the current since-stamp for publication.
    pub fn observe(&mut self, behind: bool, now: Instant) -> Option<Instant> {
        if behind {
            self.since.get_or_insert(now);
        } else {
            self.since = None;
        }
        self.since
    }

    /// The current behind-since stamp (`None` while caught up).
    pub fn since(&self) -> Option<Instant> {
        self.since
    }
}

/// Shared status-stamp cell: the coordinator's publish handle and the
/// reporter's read handle are clones over one `Arc<Mutex<_>>`, mirroring
/// [`super::reporter::SharedSnapshotSource`]. The coordinator publishes the
/// latest stamps each loop iteration; the reporter reads them at report
/// time. `Default` yields all-clear (a reporter wired without a producer —
/// the inline primary-tail path — reports no status, the correct quiet
/// behaviour).
#[derive(Clone, Debug, Default)]
pub struct StatusCell {
    stamps: Arc<Mutex<StatusStamps>>,
}

impl StatusCell {
    pub fn new() -> Self {
        Self::default()
    }

    /// Publish the latest status stamps for the reporter's next report.
    /// Lock-poison-recovering (a panicked prior holder must not wedge the
    /// reporter).
    pub fn publish(&self, stamps: StatusStamps) {
        let mut guard = self.stamps.lock().unwrap_or_else(|p| p.into_inner());
        *guard = stamps;
    }

    /// Read the current stamps.
    fn stamps(&self) -> StatusStamps {
        *self.stamps.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// The ONE folded status line to include in the periodic report at
    /// `now`, or `None` when no condition has been SUSTAINED for
    /// [`STATUS_SUSTAINED_THRESHOLD`]. A `Some` return both forces the
    /// report to emit (overriding the idle-skip gate) AND is appended to the
    /// report body. When both conditions qualify, connection loss leads (it
    /// subsumes "not caught up" — a lost connection cannot catch up).
    pub fn status_line(&self, now: Instant) -> Option<String> {
        let stamps = self.stamps();
        if let Some(elapsed) = sustained_for(stamps.connection_lost_since, now) {
            return Some(format!(
                "observer degraded: connection to the run lost for {}m (no successful \
                 reconnect+sync); see the full log for the per-leg diagnostics",
                elapsed.as_secs() / 60
            ));
        }
        if let Some(elapsed) = sustained_for(stamps.not_caught_up_since, now) {
            return Some(format!(
                "observer degraded: CRDT mirror not caught up with the cluster for {}m \
                 (a peer's digest is ahead / snapshot-stream catch-up outstanding)",
                elapsed.as_secs() / 60
            ));
        }
        None
    }
}

/// The elapsed spell duration IFF `since` is set AND it has been at least
/// [`STATUS_SUSTAINED_THRESHOLD`] in the past relative to `now`; `None`
/// otherwise (clear, or still transient). `saturating_duration_since`
/// tolerates a `now` slightly before the stamp (clock skew across the
/// publish seam) by reading zero elapsed.
fn sustained_for(since: Option<Instant>, now: Instant) -> Option<Duration> {
    let elapsed = now.saturating_duration_since(since?);
    (elapsed >= STATUS_SUSTAINED_THRESHOLD).then_some(elapsed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(t0: Instant, secs: u64) -> Instant {
        t0 + Duration::from_secs(secs)
    }

    #[test]
    fn catch_up_tracker_stamps_start_and_holds_across_spell() {
        let t0 = Instant::now();
        let mut tr = CatchUpTracker::new();
        // Caught up → no stamp.
        assert_eq!(tr.observe(false, t0), None);
        // Goes behind at +10 → stamp the start.
        assert_eq!(tr.observe(true, at(t0, 10)), Some(at(t0, 10)));
        // Still behind at +200 → the ORIGINAL start is preserved (the gate
        // measures the whole spell, not the last tick).
        assert_eq!(tr.observe(true, at(t0, 200)), Some(at(t0, 10)));
        // Catches up at +210 → cleared.
        assert_eq!(tr.observe(false, at(t0, 210)), None);
        // A fresh behind-spell stamps the NEW start.
        assert_eq!(tr.observe(true, at(t0, 300)), Some(at(t0, 300)));
    }

    #[test]
    fn no_status_line_when_all_clear() {
        let cell = StatusCell::new();
        assert_eq!(cell.status_line(Instant::now()), None);
        cell.publish(StatusStamps::default());
        assert_eq!(cell.status_line(Instant::now()), None);
    }

    #[test]
    fn transient_not_caught_up_under_threshold_produces_nothing() {
        let t0 = Instant::now();
        let cell = StatusCell::new();
        cell.publish(StatusStamps {
            not_caught_up_since: Some(t0),
            ..Default::default()
        });
        // 299s < 300s threshold → no line.
        assert_eq!(cell.status_line(at(t0, 299)), None);
    }

    #[test]
    fn sustained_not_caught_up_at_threshold_produces_line() {
        let t0 = Instant::now();
        let cell = StatusCell::new();
        cell.publish(StatusStamps {
            not_caught_up_since: Some(t0),
            ..Default::default()
        });
        let line = cell.status_line(at(t0, 360)).expect("sustained → line");
        assert!(line.contains("not caught up"), "got: {line}");
        assert!(line.contains("6m"), "elapsed minutes rendered: {line}");
    }

    #[test]
    fn sustained_loss_at_threshold_produces_line_and_leads() {
        let t0 = Instant::now();
        let cell = StatusCell::new();
        // Both conditions sustained: loss leads (it subsumes not-caught-up).
        cell.publish(StatusStamps {
            connection_lost_since: Some(t0),
            not_caught_up_since: Some(t0),
        });
        let line = cell.status_line(at(t0, 600)).expect("sustained → line");
        assert!(line.contains("connection to the run lost"), "got: {line}");
        assert!(!line.contains("not caught up"), "loss leads: {line}");
    }

    #[test]
    fn transient_loss_under_threshold_produces_nothing() {
        let t0 = Instant::now();
        let cell = StatusCell::new();
        cell.publish(StatusStamps {
            connection_lost_since: Some(t0),
            ..Default::default()
        });
        assert_eq!(cell.status_line(at(t0, 120)), None);
    }
}
