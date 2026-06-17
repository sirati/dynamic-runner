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

use std::collections::{HashMap, HashSet};
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
/// reconnect window plus slack.
pub(super) const DEFAULT_FAILURE_WINDOW: Duration = Duration::from_secs(30);

/// Default patient backstop for `SecondaryConfig::primary_silence_backstop`
/// — the staleness of `primary_last_seen` past which the secondary
/// elects against a primary whose link never armed a no-route failure
/// (alive at QUIC, wedged at the application layer). 120s ≈ 24× the 5s
/// production `keepalive_interval`, and comfortably past the 60s QUIC
/// `max_idle_timeout`: a quiet-but-live link survives to 60s without
/// closing, so any link the QUIC layer would itself tear down arms the
/// FAST leg (`should_arm_failover`) long before this patient leg fires.
/// This leg is reached ONLY for a primary that stays routable yet
/// app-silent — the one case the fast leg structurally cannot catch.
///
/// `pub` + re-exported from the crate root so the PyO3 manager
/// construction sites (which hand-build a `SecondaryConfig` literal and
/// have no `..Default::default()` spread) reference this single source
/// of truth rather than duplicating the literal.
pub const DEFAULT_PRIMARY_SILENCE_BACKSTOP: Duration = Duration::from_secs(120);

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

    /// Failover slot-reconfirmation window. `Some(confirmed)` while a
    /// just-applied `PrimaryChanged` leaves the (possibly newly-promoted)
    /// primary holding stale `InFlight` occupancy guesses for inherited
    /// slots: every idle worker must re-issue ITS OWN `TaskRequest` so the
    /// primary's `handle_task_request` reconciles each inherited slot
    /// against ground truth (request.rs: `reconcile_inherited_slot`). The
    /// set accumulates the workers that have re-confirmed (issued a request
    /// while the window was open). `None` in STEADY STATE — there is no
    /// inherited-slot debt, so the live primary's event-driven push
    /// (`dispatch_to_idle_workers` on `TasksAdded` / a completion) assigns
    /// every idle worker without any periodic re-poll. The window auto-
    /// closes (→ `None`) once every currently-idle worker is in the set
    /// (every inherited slot this node could reconcile has been
    /// reconfirmed).
    failover_reconfirm: Option<HashSet<WorkerId>>,

    /// When the breach was last REPORTED to a caller (the `true` return
    /// of [`Self::record_recv_failure`]). `None` while no breach has
    /// been reported this failure window. The probe-breach accounting
    /// clock: one suspicion is reported ONCE PER `failure_window`, on
    /// this field's own schedule — never once per send attempt — so a
    /// send-loop flood (a replay storm issuing dozens of no-route sends
    /// per second) cannot turn one breached window into dozens of
    /// arming evaluations per second. The level-read
    /// [`Self::should_arm_failover`] (the election tick's input) is
    /// untouched by this rate: the breach STATE stays continuously
    /// observable; only the edge REPORT is once-per-window.
    last_breach_report: Option<Instant>,
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
            failover_reconfirm: None,
            last_breach_report: None,
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
        // A request issued during an open reconfirmation window IS this
        // worker's ground-truth confirmation to the (possibly newly-
        // promoted) primary: record it so the window can auto-close once
        // every idle worker has reconfirmed.
        if let Some(confirmed) = self.failover_reconfirm.as_mut() {
            confirmed.insert(worker_id);
        }
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

    /// Open a failover slot-reconfirmation window. Called on a genuinely
    /// applied `PrimaryChanged`: the new primary holds stale `InFlight`
    /// guesses for inherited slots, so the periodic re-poll
    /// ([`Self::periodic_repoll_pending`]) is re-enabled until every idle
    /// worker has re-issued its `TaskRequest` (the per-worker ground-truth
    /// reconciliation). Idempotent — a fresh window restarts the
    /// confirmed-set so a SECOND failover before the first settled
    /// re-confirms cleanly.
    pub(super) fn arm_failover_reconfirm(&mut self) {
        self.failover_reconfirm = Some(HashSet::new());
    }

    /// Failover-only gate for the SECONDARY's PERIODIC keepalive re-poll.
    /// Returns `true` iff a reconfirmation window is open AND at least one
    /// of the currently-idle workers (`idle_worker_ids`) has not yet
    /// re-issued its `TaskRequest` since the window opened — i.e. an
    /// inherited slot this node can still reconcile is outstanding. In
    /// STEADY STATE (no window) this is always `false`: the live primary's
    /// event-driven push assigns idle workers, so the keepalive re-poll is
    /// redundant and stays silent. The window AUTO-CLOSES (→ `None`) the
    /// moment every idle worker is confirmed, so a single drained run never
    /// re-enables polling once failover settled.
    pub(super) fn periodic_repoll_pending(
        &mut self,
        idle_worker_ids: impl IntoIterator<Item = WorkerId>,
    ) -> bool {
        let Some(confirmed) = self.failover_reconfirm.as_ref() else {
            return false;
        };
        let any_unconfirmed = idle_worker_ids
            .into_iter()
            .any(|wid| !confirmed.contains(&wid));
        if !any_unconfirmed {
            // Every idle worker has reconfirmed — the inherited-slot debt
            // this node could settle is cleared; close the window.
            self.failover_reconfirm = None;
        }
        any_unconfirmed
    }

    /// Record one observation of "the primary's transport recv()
    /// returned None" (or, equivalently, one failed reconnect probe).
    /// Anchors the failure window on the first call; subsequent calls
    /// just bump the counter. Returns `true` iff the threshold has been
    /// breached AND the breach has not yet been reported this
    /// `failure_window` — the ONCE-PER-WINDOW breach report, on the
    /// link's own clock (`last_breach_report`), NEVER once per call.
    ///
    /// Why once-per-window: this method is called per FAILED SEND, and
    /// the send rate is the caller's business (a buffered-report replay
    /// drain under an outage re-sends dozens of frames per loop tick —
    /// the run_20260610_221140 storm hit ~60 no-route sends/second).
    /// Pre-fix every post-breach call returned `true`, so ONE suspicion
    /// ("the primary is unreachable") became ~60 arming
    /// evaluations/second at the caller (WARN + `primary_last_seen`
    /// backdate each). The breach STATE is unchanged and continuously
    /// observable through the level-read [`Self::should_arm_failover`]
    /// (the election tick's leg-(A) input); only the edge REPORT this
    /// return carries is rate-limited to its own window.
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
        if !self.should_arm_failover() {
            return false;
        }
        // Breached: report once per failure_window on the report clock.
        let due = match self.last_breach_report {
            None => true,
            Some(at) => now.saturating_duration_since(at) >= self.failure_window,
        };
        if due {
            self.last_breach_report = Some(now);
        }
        due
    }

    /// Reset the health sub-state to the "link is healthy" baseline.
    /// Called from `record_primary_message` (election.rs) on every
    /// primary-side message — that's the canonical "primary is alive"
    /// signal, and any transient failure window we were tracking
    /// should be discarded the moment a real message arrives.
    pub(super) fn record_recv_success(&mut self) {
        self.first_failure_at = None;
        self.failure_count = 0;
        // A healthy link closes the failure window entirely; the next
        // breach (a genuinely new suspicion) reports immediately.
        self.last_breach_report = None;
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

    /// Probe-breach accounting is ONCE-PER-WINDOW, not per send attempt
    /// (the run_20260610_221140 replay-storm face: ~60 no-route sends/s
    /// each pre-fix returned `armed=true`, turning ONE suspicion into 60
    /// arming evaluations per second). N failures inside one window
    /// must yield exactly ONE breach report; the level-read
    /// `should_arm_failover` stays continuously true for the election
    /// tick.
    #[test]
    fn breach_report_fires_once_per_window_under_send_flood() {
        let mut link = PrimaryLink::with_failover_threshold(
            3,
            Duration::from_secs(3600), // huge window — one report max
        );
        let mut reports = 0;
        // The flood: many failed sends in one window.
        for _ in 0..200 {
            if link.record_recv_failure() {
                reports += 1;
            }
        }
        assert_eq!(
            reports, 1,
            "N send failures in one window must produce exactly ONE              breach report"
        );
        // The breach STATE stays continuously observable (the election
        // tick's leg-(A) level read is unthrottled).
        assert!(link.should_arm_failover());
    }

    /// The breach report re-arms on the report's OWN clock: after one
    /// `failure_window` elapses, a still-breached link reports once
    /// more (a persistent outage stays operator-visible at window
    /// cadence, never at send cadence).
    #[test]
    fn breach_report_rearms_after_window_elapses() {
        let mut link = PrimaryLink::with_failover_threshold(1, Duration::from_millis(40));
        assert!(
            link.record_recv_failure(),
            "first breach reports immediately"
        );
        assert!(
            !link.record_recv_failure(),
            "inside the window: no second report"
        );
        std::thread::sleep(Duration::from_millis(50));
        assert!(
            link.record_recv_failure(),
            "a full window later the persistent breach reports again"
        );
        assert!(!link.record_recv_failure());
    }

    /// A recovery (`record_recv_success`) closes the window entirely:
    /// the NEXT breach is a genuinely new suspicion and reports
    /// immediately, not after a stale report-clock residue.
    #[test]
    fn breach_report_resets_on_recovery() {
        let mut link = PrimaryLink::with_failover_threshold(1, Duration::from_secs(3600));
        assert!(link.record_recv_failure());
        assert!(!link.record_recv_failure());
        link.record_recv_success();
        assert!(
            link.record_recv_failure(),
            "a fresh breach after recovery reports immediately"
        );
    }

    /// STEADY STATE: with no reconfirmation window armed, the periodic
    /// re-poll gate is ALWAYS closed — the live primary's event-driven
    /// push assigns idle workers, so the keepalive re-poll is redundant
    /// and stays silent regardless of how many workers are idle.
    #[test]
    fn periodic_repoll_silent_without_a_failover_window() {
        let mut link = PrimaryLink::with_failover_threshold(5, Duration::from_secs(30));
        assert!(
            !link.periodic_repoll_pending([0, 1, 2]),
            "no window armed → periodic re-poll never fires in steady state"
        );
        // Idempotent: still closed.
        assert!(!link.periodic_repoll_pending([0, 1, 2]));
    }

    /// FAILOVER: once `arm_failover_reconfirm` opens a window, the periodic
    /// re-poll fires until EVERY idle worker has re-issued its request
    /// (`note_request_sent` records the confirmation), then auto-closes —
    /// and stays closed thereafter (steady state restored).
    #[test]
    fn failover_window_repolls_until_every_idle_worker_reconfirms() {
        let mut link = PrimaryLink::with_failover_threshold(5, Duration::from_secs(30));
        link.arm_failover_reconfirm();
        // Three idle workers, none reconfirmed yet → pending.
        assert!(
            link.periodic_repoll_pending([0, 1, 2]),
            "armed window with unconfirmed idle workers must re-poll"
        );
        // Two of them issue their reconfirming request.
        link.note_request_sent(0);
        link.note_request_sent(1);
        assert!(
            link.periodic_repoll_pending([0, 1, 2]),
            "still pending while worker 2 has not reconfirmed"
        );
        // The last one reconfirms → the gate reports false on the next
        // check AND closes the window.
        link.note_request_sent(2);
        assert!(
            !link.periodic_repoll_pending([0, 1, 2]),
            "every idle worker reconfirmed → window closes"
        );
        // Window closed: steady-state silence even if a worker frees later.
        assert!(
            !link.periodic_repoll_pending([0, 1, 2, 3]),
            "after the window auto-closed the periodic re-poll stays silent"
        );
    }

    /// A SECOND failover before the first settled re-arms cleanly: a fresh
    /// window discards the prior confirmed-set, so the same workers must
    /// reconfirm against the new primary.
    #[test]
    fn re_arming_resets_the_confirmed_set() {
        let mut link = PrimaryLink::with_failover_threshold(5, Duration::from_secs(30));
        link.arm_failover_reconfirm();
        link.note_request_sent(0);
        assert!(link.periodic_repoll_pending([0, 1]));
        // Second PrimaryChanged before the first window settled.
        link.arm_failover_reconfirm();
        assert!(
            link.periodic_repoll_pending([0]),
            "a fresh window makes even a previously-confirmed worker reconfirm"
        );
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
