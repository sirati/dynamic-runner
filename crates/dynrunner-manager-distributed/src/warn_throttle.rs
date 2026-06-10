//! [`WarnThrottle`] — a minimum-interval gate for recurring operator
//! warnings.
//!
//! # Single concern
//!
//! ONE concern: decide whether a periodic fault report is due again,
//! and account for the occurrences suppressed in between. A cadence arm
//! that detects the same fault every tick (e.g. the observer's ~20s
//! anti-entropy tick over an empty peer registry) must surface the fault
//! at WARN — silence killed the run_20260610 diagnosability — but must
//! not spam one WARN per tick for the lifetime of the outage. The caller
//! owns WHAT to log; this type owns only WHEN (the edge + the interval)
//! and HOW MANY were swallowed since the last emit.
//!
//! Uses [`tokio::time::Instant`] so throttled cadences behave correctly
//! under `start_paused` test time.

use std::time::Duration;
use tokio::time::Instant;

/// Minimum-interval emission gate with a suppressed-occurrence counter.
#[derive(Debug)]
pub(crate) struct WarnThrottle {
    /// Minimum spacing between two permitted emissions.
    min_interval: Duration,
    /// When the last permitted emission happened; `None` until the first.
    last_emit: Option<Instant>,
    /// Occurrences suppressed since the last permitted emission.
    suppressed: u64,
}

impl WarnThrottle {
    pub(crate) fn new(min_interval: Duration) -> Self {
        Self {
            min_interval,
            last_emit: None,
            suppressed: 0,
        }
    }

    /// Report one occurrence of the throttled condition. Returns
    /// `Some(suppressed_since_last_emit)` when the caller should emit NOW
    /// (the first occurrence always emits; later ones once per
    /// `min_interval`), or `None` when this occurrence is suppressed.
    pub(crate) fn permit(&mut self) -> Option<u64> {
        let now = Instant::now();
        match self.last_emit {
            Some(last) if now.duration_since(last) < self.min_interval => {
                self.suppressed += 1;
                None
            }
            _ => {
                let suppressed = self.suppressed;
                self.suppressed = 0;
                self.last_emit = Some(now);
                Some(suppressed)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// First occurrence emits immediately; within the interval everything
    /// is suppressed and counted; past the interval the next occurrence
    /// emits carrying the suppressed count.
    #[tokio::test(start_paused = true)]
    async fn first_emits_then_suppresses_until_interval_elapses() {
        let mut t = WarnThrottle::new(Duration::from_secs(60));
        assert_eq!(t.permit(), Some(0), "the first occurrence always emits");
        assert_eq!(t.permit(), None);
        assert_eq!(t.permit(), None);
        tokio::time::advance(Duration::from_secs(59)).await;
        assert_eq!(t.permit(), None, "still inside the interval");
        tokio::time::advance(Duration::from_secs(2)).await;
        assert_eq!(
            t.permit(),
            Some(3),
            "past the interval: emit, naming the 3 suppressed occurrences"
        );
        assert_eq!(t.permit(), None, "the counter restarts after an emit");
    }

    /// A quiet period does not bank credit: an emit is gated only on the
    /// time since the LAST emit, so a long-idle throttle emits immediately
    /// on the next occurrence with a zero suppressed count.
    #[tokio::test(start_paused = true)]
    async fn idle_period_emits_immediately_with_zero_suppressed() {
        let mut t = WarnThrottle::new(Duration::from_secs(60));
        assert_eq!(t.permit(), Some(0));
        tokio::time::advance(Duration::from_secs(600)).await;
        assert_eq!(t.permit(), Some(0));
    }
}
