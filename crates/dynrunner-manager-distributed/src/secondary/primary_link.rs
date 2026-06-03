//! Encapsulates this secondary's link to whichever node currently holds
//! primary authority — per-worker request rate limiting and the
//! failover-health sub-state.
//!
//! The single concern owned here: rate-limit this secondary's
//! `TaskRequest`s per worker, and track the link-health window that
//! arms a failover election when the primary's transport goes silent.
//! "Where is the primary / who holds the role" is NOT this module's
//! concern — that is owned by `cluster_state.current_primary()` (the
//! single source of "who is primary") and resolved at the egress edge
//! via `send_to(Destination::Primary, ..)`.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use dynrunner_core::WorkerId;

/// Initial per-worker TaskRequest backoff after the first
/// "no work available" reply. Doubles on each subsequent empty
/// reply, capped at `MAX_BACKOFF`.
pub(super) const INITIAL_BACKOFF: Duration = Duration::from_secs(1);

/// Hard cap on the per-worker request backoff. Picked so an idle
/// worker still polls at least once per minute even after long
/// quiet stretches — required for the periodic-repoll safety net.
pub(super) const MAX_BACKOFF: Duration = Duration::from_secs(60);

/// Default reconnect-failure threshold (number of probes after which
/// the link is declared dead and failover is armed). Five matches the
/// task description (R1): 4 probes leave room for a single packet
/// drop + retransmit cleanly, the fifth confirms a sustained outage.
/// Bound below 3 would arm on a single dropped TCP packet retransmit
/// — too eager.
pub(super) const DEFAULT_FAILURE_THRESHOLD: u32 = 5;

/// Default reconnect-failure window (wall-clock time after which the
/// link is declared dead even if `DEFAULT_FAILURE_THRESHOLD` probes
/// haven't accrued — covers slow-tick configurations where the
/// keepalive interval is long enough that 5 probes would exceed the
/// SLURM time budget). Thirty seconds is the SSH ControlMaster
/// reconnect window plus slack — see `mass_death_grace_secs` in
/// pyo3 config for the parallel choice on the primary side.
pub(super) const DEFAULT_FAILURE_WINDOW: Duration = Duration::from_secs(30);

/// State + behavior for the secondary→primary link.
pub(super) struct PrimaryLink {
    /// Per-worker rate-limit window. Doubles on each empty
    /// "no work available" reply (or absence of a successful
    /// assignment between requests), capped at `MAX_BACKOFF`.
    request_backoff: HashMap<WorkerId, Duration>,

    /// Wall-clock timestamp of the last TaskRequest sent for this
    /// worker. Combined with `request_backoff` to gate the next
    /// request. Removed on successful assignment so a fresh request
    /// can fire on the worker's next idle tick.
    last_request_time: HashMap<WorkerId, Instant>,

    // Health sub-state: tracks consecutive observed-dead probes after
    // the primary's transport returned None. Reset when a primary
    // message arrives (via `record_recv_success`, called from
    // `record_primary_message`). The actual failover-arming decision
    // lives at the call site (`processing.rs`), which consults
    // `should_arm_failover` once `record_recv_failure` returns.
    /// Wall-clock timestamp of the first observed recv-None event.
    /// `None` while the link is healthy. Used as the anchor for the
    /// time-based half of the threshold.
    first_failure_at: Option<Instant>,

    /// Count of probes observed dead since `first_failure_at`. Bumped
    /// once per call to `record_recv_failure`. Used for the
    /// attempts-based half of the threshold so slow-keepalive
    /// configurations can still arm on time alone.
    failure_count: u32,

    /// Failure-count threshold above which `should_arm_failover`
    /// returns true. Configurable so tests can drive the threshold
    /// with a tight value.
    failure_threshold: u32,

    /// Failure-window after which `should_arm_failover` returns true
    /// regardless of `failure_count`. Configurable for the same
    /// reason.
    failure_window: Duration,
}

impl PrimaryLink {
    /// Constructor with explicit failover-threshold knobs. Production
    /// callers pass the values from `SecondaryConfig` (which default
    /// to `DEFAULT_FAILURE_THRESHOLD` / `DEFAULT_FAILURE_WINDOW`);
    /// tests use this variant to drive a tight threshold without
    /// waiting 30s of wall-clock time.
    pub(super) fn with_failover_threshold(
        failure_threshold: u32,
        failure_window: Duration,
    ) -> Self {
        Self {
            request_backoff: HashMap::new(),
            last_request_time: HashMap::new(),
            first_failure_at: None,
            failure_count: 0,
            failure_threshold,
            failure_window,
        }
    }

    /// Returns true iff this worker's per-request rate limit
    /// permits another `TaskRequest` to fire now.
    pub(super) fn should_request_now(&self, worker_id: WorkerId) -> bool {
        let backoff = self
            .request_backoff
            .get(&worker_id)
            .copied()
            .unwrap_or(INITIAL_BACKOFF);
        match self.last_request_time.get(&worker_id) {
            Some(last) => Instant::now().duration_since(*last) >= backoff,
            None => true,
        }
    }

    /// Record that a `TaskRequest` was just sent for this worker
    /// and double its backoff window (capped at `MAX_BACKOFF`).
    pub(super) fn note_request_sent(&mut self, worker_id: WorkerId) {
        let now = Instant::now();
        let prev = self
            .request_backoff
            .get(&worker_id)
            .copied()
            .unwrap_or(INITIAL_BACKOFF);
        let next = (prev * 2).min(MAX_BACKOFF);
        self.last_request_time.insert(worker_id, now);
        self.request_backoff.insert(worker_id, next);
    }

    /// Reset rate limiting for a worker after a successful task
    /// assignment so the next idle tick can fire a fresh request
    /// without sitting through a stale backoff window.
    pub(super) fn reset_backoff(&mut self, worker_id: WorkerId) {
        self.request_backoff.remove(&worker_id);
        self.last_request_time.remove(&worker_id);
    }

    /// Clear EVERY worker's request backoff. Used when the primary
    /// identity changes (an applied `PrimaryChanged`): backoff accrued
    /// against the prior primary is stale the moment the role flips, so
    /// every idle
    /// worker must be free to re-issue its `TaskRequest` immediately at
    /// the new primary. Keyed off the backoff maps themselves (not the
    /// worker pool) so it works regardless of whether the pool has been
    /// initialised yet.
    pub(super) fn reset_all_backoff(&mut self) {
        self.request_backoff.clear();
        self.last_request_time.clear();
    }

    /// Record one observation of "the primary's transport recv()
    /// returned None" (or, equivalently, one failed reconnect probe).
    /// Anchors the failure window on the first call; subsequent calls
    /// just bump the counter. Returns `true` iff the threshold has
    /// been breached and the caller should arm failover.
    ///
    /// Threshold breach is `failure_count >= failure_threshold` OR
    /// `now - first_failure_at >= failure_window`, whichever fires
    /// first. This keeps both the dropped-packet (count) and the
    /// SLURM-time (window) failure modes covered with one method.
    pub(super) fn record_recv_failure(&mut self) -> bool {
        let now = Instant::now();
        if self.first_failure_at.is_none() {
            self.first_failure_at = Some(now);
        }
        self.failure_count = self.failure_count.saturating_add(1);
        self.should_arm_failover()
    }

    /// Reset the health sub-state to the "link is healthy" baseline.
    /// Called from `record_primary_message` (election.rs) on every
    /// primary-side message — that's the canonical "primary is alive"
    /// signal, and any transient failure window we were tracking
    /// should be discarded the moment a real message arrives.
    pub(super) fn record_recv_success(&mut self) {
        self.first_failure_at = None;
        self.failure_count = 0;
    }

    /// Pure read of the threshold breach predicate. Exposed so the
    /// processing-loop tick can consult it on each iteration without
    /// having to call `record_recv_failure` (which has the side
    /// effect of bumping the counter — wrong for "did we exceed the
    /// time window since the last bump?" queries).
    pub(super) fn should_arm_failover(&self) -> bool {
        match self.first_failure_at {
            None => false,
            Some(at) => {
                self.failure_count >= self.failure_threshold || at.elapsed() >= self.failure_window
            }
        }
    }

    /// True iff the health sub-state has observed at least one
    /// recv-None probe since the last reset. Used by the
    /// processing-loop tick to decide whether to consult the
    /// time-based half of the threshold.
    pub(super) fn is_link_failing(&self) -> bool {
        self.first_failure_at.is_some()
    }
}

#[cfg(test)]
mod tests {
    //! Unit-level coverage for the primary-link health sub-state. The
    //! integration-level tests live in `secondary/tests.rs` and drive
    //! these methods via the full processing loop.

    use super::*;

    /// T-R1-count-arms: count-axis half of the threshold. After
    /// `failure_threshold` consecutive `record_recv_failure` calls,
    /// `should_arm_failover` returns true and the next
    /// `record_recv_failure` returns true at the call boundary.
    /// Pinning the threshold separately from the time axis keeps the
    /// test deterministic — wall-clock isn't involved.
    #[test]
    fn reconnect_threshold_arms_election_after_n_failures() {
        let mut link = PrimaryLink::with_failover_threshold(
            5,
            Duration::from_secs(3600), // huge window — count axis only
        );
        assert!(!link.should_arm_failover());
        for i in 1..5 {
            let armed = link.record_recv_failure();
            assert!(!armed, "should not arm before threshold (probe {i})");
            assert!(!link.should_arm_failover());
        }
        // Fifth probe breaches the threshold.
        let armed = link.record_recv_failure();
        assert!(armed, "fifth probe must arm failover");
        assert!(link.should_arm_failover());
    }

    /// T-R1-time-arms: time-axis half of the threshold. Once the
    /// failure window has elapsed since the first probe,
    /// `should_arm_failover` returns true even if `failure_count`
    /// hasn't reached `failure_threshold`.
    #[test]
    fn reconnect_threshold_arms_after_window_elapsed() {
        let mut link = PrimaryLink::with_failover_threshold(
            1000, // huge count threshold — time axis only
            Duration::from_millis(50),
        );
        assert!(!link.record_recv_failure());
        // Sleep beyond the window.
        std::thread::sleep(Duration::from_millis(60));
        assert!(
            link.should_arm_failover(),
            "time-axis must arm once window elapsed"
        );
    }

    /// T-R1-recv-success-resets: a primary message arriving (which
    /// calls `record_recv_success` via `record_primary_message`)
    /// resets the failure window so the next probe starts fresh.
    /// Without this, a brief flap that recovers would still leave
    /// the link half-armed on the next flap.
    #[test]
    fn record_recv_success_resets_failure_window() {
        let mut link = PrimaryLink::with_failover_threshold(3, Duration::from_secs(30));
        link.record_recv_failure();
        link.record_recv_failure();
        assert!(link.is_link_failing());
        link.record_recv_success();
        assert!(!link.is_link_failing());
        assert!(!link.should_arm_failover());
        // After reset, a single new failure shouldn't arm.
        assert!(!link.record_recv_failure());
    }

    /// Boundary: the `should_arm_failover` predicate is a pure read —
    /// calling it repeatedly without `record_recv_failure` doesn't
    /// bump anything. Pins the side-effect contract so callers can
    /// rely on tick-driven re-checks.
    #[test]
    fn should_arm_failover_is_pure() {
        let mut link = PrimaryLink::with_failover_threshold(5, Duration::from_secs(3600));
        assert!(!link.should_arm_failover());
        assert!(!link.should_arm_failover());
        // Still healthy.
        assert!(!link.is_link_failing());
        // First failure registers.
        link.record_recv_failure();
        assert!(link.is_link_failing());
        // Repeated should_arm calls don't change state.
        assert!(!link.should_arm_failover());
        assert!(!link.should_arm_failover());
    }
}
