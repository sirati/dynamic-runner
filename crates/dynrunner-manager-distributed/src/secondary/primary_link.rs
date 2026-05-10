//! Encapsulates this secondary's link to whichever node currently holds
//! primary authority — primary identity, per-worker request rate
//! limiting, and the routing decision for operational sends.
//!
//! Pre-extraction this state lived as three loose fields
//! (`primary_peer_id`, `request_backoff`, `last_request_time`) on
//! `SecondaryCoordinator` and was poked from five files
//! (`mod`, `dispatch`, `processing`, `resource`, `election`,
//! `peer`). Adding a side-effect on primary-change required editing
//! every site, and the trace at `feb1052` showed exactly that bug
//! class: PromotePrimary set the routing target but no single place
//! could "cancel pending requests at the old primary and re-issue
//! at the new one", so the new primary's local workers stayed silent
//! after promotion.
//!
//! The single concern owned here: "this secondary's link to whichever
//! node currently holds primary authority." Anything that crosses
//! that boundary goes through the methods below; the fields are
//! private. Phase P wires `on_primary_changed` into the
//! `PromotePrimary` handler to actually fire the cancel-and-reissue.

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
    /// This node's id. Used by `is_self_primary` to recognise the
    /// "we are the current primary" case so that
    /// `send_to_current_primary` falls through to local handling.
    secondary_id: String,

    /// Identity of the current primary peer, if the original primary
    /// is dead and an election has resolved. `None` while the original
    /// primary is alive (TaskRequest goes to `primary_transport`); `Some`
    /// while we're voting for or have voted for a candidate (TaskRequest
    /// is routed to that peer via `peer_transport`). Cleared whenever a
    /// live primary message arrives.
    primary_peer_id: Option<String>,

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
        secondary_id: String,
        failure_threshold: u32,
        failure_window: Duration,
    ) -> Self {
        Self {
            secondary_id,
            primary_peer_id: None,
            request_backoff: HashMap::new(),
            last_request_time: HashMap::new(),
            first_failure_at: None,
            failure_count: 0,
            failure_threshold,
            failure_window,
        }
    }

    /// Returns the id of the node currently holding primary
    /// authority, or `None` while the original primary is still
    /// alive (in which case operational sends go through the
    /// `primary_transport`).
    pub(super) fn current_primary(&self) -> Option<&str> {
        self.primary_peer_id.as_deref()
    }

    /// Update the routing target. Used by:
    ///  - the failover election state machine when a candidate is
    ///    chosen (transitional) or confirmed,
    ///  - the `record_primary_message` reset path that clears the
    ///    target on receiving a live-primary message during an
    ///    election,
    ///  - the explicit `PromotePrimary` handler in
    ///    `dispatch_message` (Phase P will switch this site to
    ///    `on_primary_changed` so backoff state is reset in
    ///    lockstep with the role flip).
    pub(super) fn set_current_primary(&mut self, id: Option<String>) {
        self.primary_peer_id = id;
    }

    /// True iff the routing target is this node itself (i.e. we
    /// won the election and now hold primary authority). Phase P
    /// uses this in the role-flip handler; provided now so the
    /// boundary contract is complete.
    #[allow(dead_code)]
    pub(super) fn is_self_primary(&self) -> bool {
        self.primary_peer_id
            .as_deref()
            .is_some_and(|id| id == self.secondary_id)
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

    /// React to a primary-identity change: route at the new primary,
    /// drop any per-worker backoff state accrued against the old
    /// one (otherwise idle workers would sit through stale windows
    /// against the now-dead primary before re-requesting), and let
    /// callers cancel any pending requests they tracked.
    ///
    /// Phase P's `PromotePrimary` handler is the canonical caller.
    /// Until then, the dispatch.rs handler still calls
    /// `set_current_primary` directly — the role flip works but
    /// the new primary's local workers may sit through residual
    /// backoff instead of dispatching immediately.
    #[allow(dead_code)]
    pub(super) fn on_primary_changed(&mut self, new_primary: String) {
        self.primary_peer_id = Some(new_primary);
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
                self.failure_count >= self.failure_threshold
                    || at.elapsed() >= self.failure_window
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
