//! [`DepthWatch`] — threshold + rate-limit policy for queue-depth WARNs.
//!
//! # Single concern
//!
//! Decide — purely, testably — WHEN a queue-depth observation warrants
//! an operator-facing WARN: the depth has reached the watch's threshold
//! AND the rate limit admits another emission. Nothing here reads a
//! queue, names a peer, or formats a log line — the call site (a
//! framed-IO writer pump draining its per-connection egress channel,
//! the mesh-pump draining its dispatch queue) owns the read and the
//! line; this owns only the policy, so every depth WARN in the system
//! fires under ONE rule.
//!
//! # Why this exists (the unbounded-channel honesty rule)
//!
//! The egress channels are deliberately unbounded (a bounded channel
//! would force the sender to choose between silent drop-on-full and
//! wedging an operational loop against a slow consumer). Unbounded is
//! only honest when ACCUMULATION IS VISIBLE: a leg whose consumer
//! drains slower than its producer fills — a blackholed-but-live wire
//! in TCP retransmit limbo, a starved pump — grows its queue silently
//! and surfaces, eventually, as an unexplained multi-GB cold process.
//! The watch makes the growth a log line at queue depths ~6 orders of
//! magnitude before that.

use std::time::{Duration, Instant};

/// Default depth at which a queue is considered to be accumulating
/// rather than buffering: production steady-state egress queues drain
/// to ~zero between wakeups, so thousands of queued frames means the
/// consumer is persistently behind its producer.
pub const DEPTH_WARN_THRESHOLD: usize = 4096;

/// Default minimum spacing between two depth WARNs from one watch: the
/// signal is a trend diagnostic, not a per-frame alarm.
pub const DEPTH_WARN_INTERVAL: Duration = Duration::from_secs(60);

/// Threshold + rate-limit decision state for one watched queue.
#[derive(Debug)]
pub struct DepthWatch {
    threshold: usize,
    min_interval: Duration,
    last_warn: Option<Instant>,
}

impl DepthWatch {
    /// A watch with explicit knobs (tests, special call sites).
    pub fn new(threshold: usize, min_interval: Duration) -> Self {
        Self {
            threshold,
            min_interval,
            last_warn: None,
        }
    }

    /// A watch with the shared production defaults.
    pub fn with_defaults() -> Self {
        Self::new(DEPTH_WARN_THRESHOLD, DEPTH_WARN_INTERVAL)
    }

    /// Feed one depth observation at `now`; returns `Some(depth)`
    /// exactly when the call site should WARN — depth at-or-over the
    /// threshold and the rate limit open. Below-threshold observations
    /// never emit and never touch the rate limit (a queue oscillating
    /// across the threshold re-warns at most once per interval).
    pub fn observe(&mut self, depth: usize, now: Instant) -> Option<usize> {
        if depth < self.threshold {
            return None;
        }
        if self
            .last_warn
            .is_some_and(|last| now.duration_since(last) < self.min_interval)
        {
            return None;
        }
        self.last_warn = Some(now);
        Some(depth)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn below_threshold_never_warns() {
        let mut watch = DepthWatch::new(100, Duration::from_secs(60));
        let now = Instant::now();
        assert_eq!(watch.observe(0, now), None);
        assert_eq!(watch.observe(99, now), None);
    }

    #[test]
    fn at_threshold_warns_once_per_interval() {
        let mut watch = DepthWatch::new(100, Duration::from_secs(60));
        let t0 = Instant::now();
        assert_eq!(watch.observe(100, t0), Some(100));
        // Inside the rate limit: suppressed even though still over.
        assert_eq!(watch.observe(5000, t0 + Duration::from_secs(30)), None);
        // Past the rate limit: re-warns with the CURRENT depth.
        assert_eq!(
            watch.observe(5000, t0 + Duration::from_secs(61)),
            Some(5000)
        );
    }

    #[test]
    fn dipping_below_threshold_does_not_reset_rate_limit() {
        let mut watch = DepthWatch::new(100, Duration::from_secs(60));
        let t0 = Instant::now();
        assert_eq!(watch.observe(200, t0), Some(200));
        // Drain below, then re-grow inside the interval: still suppressed
        // (the threshold crossing is not an edge trigger — the rate limit
        // alone spaces emissions).
        assert_eq!(watch.observe(10, t0 + Duration::from_secs(10)), None);
        assert_eq!(watch.observe(300, t0 + Duration::from_secs(20)), None);
        assert_eq!(
            watch.observe(300, t0 + Duration::from_secs(61)),
            Some(300)
        );
    }
}
