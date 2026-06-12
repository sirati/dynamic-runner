//! [`CollectiveSilenceGate`] — the self-suspect guard for staleness-based
//! removals: when EVERY remote member looks dead at once, suspect THIS
//! node's wire first.
//!
//! # Concern
//!
//! ONE question, answered once per heartbeat sweep: "is the pattern of
//! silences this sweep observed more parsimoniously explained by N
//! independent peer deaths, or by ONE local ingest/wire failure?" N
//! remote members falling silent SIMULTANEOUSLY — with zero live remote
//! frames proving the local ingest path works — is the latter: the
//! run_20260612_043357 face, where a saturated primary's QUIC legs to
//! all three remotes (and its observer egress) collapsed while its
//! co-located in-process member kept completing tasks, and the sweep
//! declared three LIVE peers dead off the primary's own deafness.
//!
//! # Relation to the sibling decider-health guards
//!
//! - the TICK-LAG guard (`crate::own_tick_health`) covers the LOOP's
//!   health — the sweep itself ran late;
//! - the INGEST-EDGE gate ([`super::IngestEdgeGate`]) covers the PUMP's
//!   health — frames arrived at the transport but were not drained;
//! - THIS gate covers the WIRE's health, which no local clock can
//!   observe directly: when frames never reach the transport's read
//!   loops at all (connections dead, node-level network failure), the
//!   arrival clocks hold nothing and both sibling guards stay silent.
//!   The only in-process evidence left is the COLLECTIVE shape of the
//!   silences themselves — one live remote proves the local ingest
//!   works and disables the gate; zero live remotes is self-suspect.
//!
//! # Bounded — the hard backstop stays load-bearing
//!
//! A genuinely all-dead fleet (the cohort-3 face: tunnel blips killed
//! every secondary at once) must STILL be declared dead, or the
//! fleet-dead arm never arms and the run hangs forever. The deferral is
//! therefore bounded the same way the chronic tick-lag escalation is:
//! once the gate has actively suppressed a due HARD declaration for a
//! full escalation window (the caller passes its hard silence window —
//! deferral has then outlived a full death verdict), it escalates,
//! sweeps resume declaring, and the onset is WARNed loudly. Any remote
//! evidence advance (one member dropping below the first WARN stage)
//! ends the episode and resets the gate entirely.

use std::time::{Duration, Instant};

use crate::warn_throttle::WarnThrottle;

/// Minimum spacing between two deferral WARNs. The gate re-detects the
/// same collective silence on every heartbeat sweep (5s cadence in
/// production); a minute-cadence WARN with a suppressed count keeps the
/// outage narrated without one line per tick.
const DEFER_WARN_INTERVAL: Duration = Duration::from_secs(60);

/// Minimum number of REMOTE judged members for the collective-silence
/// inference to hold. With a single remote, "everyone is dead" and "one
/// member is dead" are the same observation — there is no collective
/// shape to read, so the gate stays out of the way and the schedule
/// declares as today. From two remotes up, simultaneous silence of ALL
/// of them is the self-suspect signal.
const MIN_REMOTE_MEMBERS: usize = 2;

/// One member's classification from the sweep, as the gate consumes it:
/// where the member's frames come from (remote wire vs in-process
/// loopback) and how silent the sweep judged it. The gate never sees
/// ids or clocks — the sweep owns the silence arithmetic.
pub(in crate::primary) struct SilenceObservation {
    /// `true` for a member whose frames traverse the wire (its id is
    /// not this node's own); the co-located same-peer member's loopback
    /// frames prove nothing about the wire, so it is excluded from the
    /// collective inference.
    pub(in crate::primary) remote: bool,
    /// The member crossed at least the first WARN stage this sweep.
    pub(in crate::primary) silent: bool,
    /// The member crossed the HARD backstop this sweep (a declaration
    /// is due; the gate's deferral clock runs while one is suppressed).
    pub(in crate::primary) hard: bool,
}

/// Sweep-side tracker of the collective-silence episode, plus the
/// deferral verdict + its operator narration. Owned by the
/// `PrimaryCoordinator`; fed once per heartbeat sweep via
/// [`CollectiveSilenceGate::observe`]; consulted between sweeps (the
/// dispatch-altitude silent-set read) via
/// [`CollectiveSilenceGate::deferring`].
pub(in crate::primary) struct CollectiveSilenceGate {
    /// When the CURRENT collective episode started (the first sweep
    /// that saw every remote judged member silent). `None` while any
    /// remote member is live.
    episode_since: Option<Instant>,
    /// When the gate FIRST suppressed a due HARD declaration this
    /// episode — the escalation clock: the bound measures how long an
    /// otherwise-due death has been actively deferred, not how long the
    /// members have merely been WARN-stage silent.
    hard_suppressed_since: Option<Instant>,
    /// Latched once the suppression has outlived the escalation window:
    /// declarations resume for the remainder of the episode (a
    /// genuinely all-dead fleet is then removed and the fleet-dead arm
    /// can arm). Reset with the episode.
    escalated: bool,
    /// The current verdict: `Some(episode age)` while the gate defers
    /// staleness-based removals, `None` while healthy or escalated.
    /// Refreshed by [`Self::observe`]; at most one sweep stale for the
    /// between-sweeps readers.
    deferral: Option<Duration>,
    /// Throttle for the deferral WARN.
    warn: WarnThrottle,
}

impl CollectiveSilenceGate {
    pub(in crate::primary) fn new() -> Self {
        Self {
            episode_since: None,
            hard_suppressed_since: None,
            escalated: false,
            deferral: None,
            warn: WarnThrottle::new(DEFER_WARN_INTERVAL),
        }
    }

    /// Fold one sweep's per-member classification into the tracker and
    /// return the verdict: `Some(episode age)` iff EVERY remote judged
    /// member (of at least [`MIN_REMOTE_MEMBERS`]) is silent
    /// simultaneously and the deferral has not yet outlived
    /// `escalation_window` — the sweep must then author no
    /// staleness-based removal. Logs the deferral (throttled WARN
    /// naming the shape and the suspicion), the escalation (one loud
    /// WARN per episode), and the recovery (one INFO per episode), so
    /// no branch is silent.
    pub(in crate::primary) fn observe(
        &mut self,
        members: &[SilenceObservation],
        now: Instant,
        escalation_window: Duration,
    ) -> Option<Duration> {
        let remote_total = members.iter().filter(|m| m.remote).count();
        let remote_silent = members.iter().filter(|m| m.remote && m.silent).count();
        let collective = remote_total >= MIN_REMOTE_MEMBERS && remote_silent == remote_total;

        if !collective {
            if let Some(since) = self.episode_since.take() {
                tracing::info!(
                    episode_s = now.saturating_duration_since(since).as_secs_f64(),
                    remote_members = remote_total,
                    remote_silent,
                    "collective-silence episode over (a remote member is \
                     live again, or the judged set changed); staleness-based \
                     dead-peer declarations resume on the normal schedule"
                );
            }
            self.hard_suppressed_since = None;
            self.escalated = false;
            self.deferral = None;
            return None;
        }

        let since = *self.episode_since.get_or_insert(now);
        let any_hard_due = members.iter().any(|m| m.remote && m.hard);
        if any_hard_due && self.hard_suppressed_since.is_none() {
            self.hard_suppressed_since = Some(now);
        }
        if let Some(first) = self.hard_suppressed_since
            && now.saturating_duration_since(first) > escalation_window
        {
            if !self.escalated {
                // Escalation onset — once per episode, NOT throttled:
                // the operator must see exactly when the self-suspect
                // deferral gave way to declarations again.
                tracing::warn!(
                    suppressed_for_s = now.saturating_duration_since(first).as_secs_f64(),
                    escalation_window_s = escalation_window.as_secs_f64(),
                    remote_members = remote_total,
                    "collective silence has outlived a full escalation \
                     window with no remote evidence either way — the fleet \
                     may genuinely be dead, and deferring forever would \
                     leave it un-declared (no requeue, no respawn, no \
                     fleet-dead exit); resuming dead-peer declarations"
                );
            }
            self.escalated = true;
        }
        if self.escalated {
            self.deferral = None;
            return None;
        }

        let age = now.saturating_duration_since(since);
        if let Some(suppressed) = self.warn.permit() {
            tracing::warn!(
                remote_members = remote_total,
                episode_s = age.as_secs_f64(),
                hard_declaration_due = any_hard_due,
                suppressed_since_last_warn = suppressed,
                "EVERY remote member is silent simultaneously — N \
                 independent deaths are less likely than ONE local \
                 ingest/wire failure, so this node suspects its own \
                 deafness first (self-suspect gate): staleness-based \
                 dead-peer declarations are DEFERRED until a remote frame \
                 proves the wire, or the bounded escalation window elapses"
            );
        }
        self.deferral = Some(age);
        self.deferral
    }

    /// The verdict of the most recent sweep, for staleness consumers
    /// running between sweeps (the dispatch-altitude silent-set read):
    /// `Some` while the gate defers. At most one sweep stale — the same
    /// staleness class those consumers already accept from the
    /// keepalive clocks themselves.
    pub(in crate::primary) fn deferring(&self) -> Option<Duration> {
        self.deferral
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const WINDOW: Duration = Duration::from_millis(200);

    fn obs(remote: bool, silent: bool, hard: bool) -> SilenceObservation {
        SilenceObservation {
            remote,
            silent,
            hard,
        }
    }

    /// ALL remotes silent (≥2) defers; one live remote — proof the
    /// local ingest works — never engages the gate, even with every
    /// other member past the hard backstop.
    #[test]
    fn defers_only_when_every_remote_is_silent() {
        let mut gate = CollectiveSilenceGate::new();
        let t0 = Instant::now();

        // One live remote among silent ones: the wire is proven, the
        // schedule declares as today.
        let mixed = [obs(true, true, true), obs(true, false, false)];
        assert_eq!(gate.observe(&mixed, t0, WINDOW), None);
        assert_eq!(gate.deferring(), None);

        // Every remote silent: self-suspect, defer.
        let all = [obs(true, true, false), obs(true, true, true)];
        let age = gate
            .observe(&all, t0 + Duration::from_millis(10), WINDOW)
            .expect("collective silence defers");
        assert_eq!(age, Duration::ZERO, "episode starts at this sweep");
        assert!(gate.deferring().is_some(), "verdict visible between sweeps");
    }

    /// The co-located same-peer member is NOT remote: its loopback
    /// frames prove nothing about the wire, so a fresh local member
    /// plus all-silent remotes still defers (the run_20260612_043357
    /// topology), while a single-remote fleet never engages the gate
    /// (no collective shape to read).
    #[test]
    fn local_member_is_excluded_and_single_remote_never_engages() {
        let mut gate = CollectiveSilenceGate::new();
        let t0 = Instant::now();

        // Fresh co-located member + two silent remotes: defer.
        let colocated = [
            obs(false, false, false),
            obs(true, true, true),
            obs(true, true, false),
        ];
        assert!(gate.observe(&colocated, t0, WINDOW).is_some());

        // Single remote silent past hard (plus the local member):
        // below MIN_REMOTE_MEMBERS — gate stays out of the way.
        let single = [obs(false, false, false), obs(true, true, true)];
        assert_eq!(
            gate.observe(&single, t0 + Duration::from_millis(10), WINDOW),
            None
        );
        assert_eq!(gate.deferring(), None);
    }

    /// BOUNDED: once a due HARD declaration has been suppressed past
    /// the escalation window, the gate escalates (declarations resume)
    /// for the remainder of the episode — a genuinely all-dead fleet is
    /// still removed and the fleet-dead arm can arm.
    #[test]
    fn suppressed_hard_declaration_escalates_after_the_window() {
        let mut gate = CollectiveSilenceGate::new();
        let t0 = Instant::now();
        let all_warn = [obs(true, true, false), obs(true, true, false)];
        let all_hard = [obs(true, true, true), obs(true, true, true)];

        // WARN-stage collective silence: deferring, but the escalation
        // clock has not started (no hard declaration is due yet).
        assert!(gate.observe(&all_warn, t0, WINDOW).is_some());
        // Hard becomes due: still deferred, escalation clock starts NOW.
        assert!(
            gate.observe(&all_hard, t0 + Duration::from_millis(300), WINDOW)
                .is_some(),
            "the WARN-stage episode age must not pre-burn the escalation \
             window: the clock runs from the first SUPPRESSED hard"
        );
        // Inside the window: still deferred.
        assert!(
            gate.observe(&all_hard, t0 + Duration::from_millis(450), WINDOW)
                .is_some()
        );
        // Past the window: escalated — declarations resume.
        assert_eq!(
            gate.observe(&all_hard, t0 + Duration::from_millis(550), WINDOW),
            None,
            "deferral must be bounded; a genuinely all-dead fleet is declared"
        );
        assert_eq!(gate.deferring(), None);
        // Escalation latches for the episode.
        assert_eq!(
            gate.observe(&all_hard, t0 + Duration::from_millis(600), WINDOW),
            None
        );
    }

    /// Recovery resets the WHOLE gate: after a remote frame ends the
    /// episode, a later collective episode starts fresh (new episode
    /// clock, new escalation window, no leftover latch).
    #[test]
    fn remote_evidence_resets_episode_and_escalation() {
        let mut gate = CollectiveSilenceGate::new();
        let t0 = Instant::now();
        let all_hard = [obs(true, true, true), obs(true, true, true)];
        let one_live = [obs(true, true, true), obs(true, false, false)];

        // Drive into escalation.
        assert!(gate.observe(&all_hard, t0, WINDOW).is_some());
        assert_eq!(
            gate.observe(&all_hard, t0 + Duration::from_millis(300), WINDOW),
            None
        );

        // A remote frame proves the wire: episode over.
        assert_eq!(
            gate.observe(&one_live, t0 + Duration::from_millis(350), WINDOW),
            None
        );

        // A NEW collective episode defers again from scratch.
        assert!(
            gate.observe(&all_hard, t0 + Duration::from_millis(400), WINDOW)
                .is_some(),
            "a fresh episode must re-arm the deferral (no leftover \
             escalation latch from the previous one)"
        );
    }
}
