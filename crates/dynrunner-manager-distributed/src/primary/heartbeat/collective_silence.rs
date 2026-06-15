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
/// loopback), how silent the sweep judged it, and which id it has (for
/// the slurm-authoritative tiebreak in [`CollectiveSilenceGate::observe`]).
/// The gate never sees clocks — the sweep owns the silence arithmetic.
pub(in crate::primary) struct SilenceObservation {
    /// The member's secondary id (`secondary-N`). The gate uses it to
    /// consult the slurm-authoritative snapshot on escalation
    /// (#544 tiebreak): a silent remote member whose slurm job is still
    /// `Alive` is local-deafness evidence, not a real death.
    pub(in crate::primary) secondary_id: String,
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
    /// Latched once the suppression has outlived the escalation window
    /// AND the slurm-authoritative tiebreak agrees the fleet is gone:
    /// declarations resume for the remainder of the episode. Reset
    /// with the episode.
    escalated: bool,
    /// Latched once we have logged the "deferring past the escalation
    /// window because authority says ≥1 silent peer is still Alive"
    /// (or "no authoritative evidence either way") WARN for this
    /// episode, so the operator sees a single line per episode rather
    /// than one per sweep. Reset with the episode.
    warned_authoritative_defer: bool,
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
            warned_authoritative_defer: false,
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
        authority: &dyn crate::authority_snapshot::SlurmAuthoritativeSnapshot,
        co_located_secondary_id: Option<&str>,
        co_located_last_frame_age: Option<Duration>,
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
                    co_located_secondary = co_located_secondary_id,
                    co_located_last_frame_age_s = co_located_last_frame_age.map(|d| d.as_secs_f64()),
                    "collective-silence episode over (a remote member is \
                     live again, or the judged set changed); staleness-based \
                     dead-peer declarations resume on the normal schedule"
                );
            }
            self.hard_suppressed_since = None;
            self.escalated = false;
            self.warned_authoritative_defer = false;
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
            // SLURM-AUTHORITATIVE TIEBREAK (#544): the wall-clock window
            // outlived. Wall-clock-alone escalation was the
            // run_20260615_112332 face — the primary's apply_spawn_tasks
            // wedge made every remote member's keepalive look silent for
            // ~6 min, the wall-clock then escalated and declared all 10
            // dead while their SLURM jobs were still RUNNING. The
            // off-loop probe is the second opinion: only escalate when
            // SLURM itself agrees every silent peer's job is GONE.
            use crate::authority_snapshot::PeerLifeState;
            let lives: Vec<PeerLifeState> = members
                .iter()
                .filter(|m| m.remote && m.silent)
                .map(|m| authority.peer_life(&m.secondary_id))
                .collect();
            let any_alive = lives.iter().any(|s| matches!(s, PeerLifeState::Alive));
            let all_gone = !lives.is_empty()
                && lives.iter().all(|s| matches!(s, PeerLifeState::Gone));
            if any_alive {
                if !self.warned_authoritative_defer {
                    tracing::warn!(
                        suppressed_for_s = now.saturating_duration_since(first).as_secs_f64(),
                        escalation_window_s = escalation_window.as_secs_f64(),
                        remote_members = remote_total,
                        alive_count = lives.iter().filter(|s| matches!(s, PeerLifeState::Alive)).count(),
                        co_located_secondary = co_located_secondary_id,
                        co_located_last_frame_age_s = co_located_last_frame_age.map(|d| d.as_secs_f64()),
                        "collective silence outlived escalation window but slurm-authoritative \
                         evidence shows ≥1 silent peer is still RUNNING — the silence is \
                         LOCAL (this node's ingest/wire failed, fleet is alive); CONTINUING \
                         to defer staleness-based declarations until either a remote frame \
                         proves the wire or slurm-authoritative evidence shows the fleet gone",
                    );
                    self.warned_authoritative_defer = true;
                }
                // Continue deferring — DO NOT set self.escalated.
            } else if all_gone {
                if !self.escalated {
                    tracing::warn!(
                        suppressed_for_s = now.saturating_duration_since(first).as_secs_f64(),
                        escalation_window_s = escalation_window.as_secs_f64(),
                        remote_members = remote_total,
                        co_located_secondary = co_located_secondary_id,
                        co_located_last_frame_age_s = co_located_last_frame_age.map(|d| d.as_secs_f64()),
                        "collective silence outlived escalation window AND slurm-authoritative \
                         evidence confirms every silent peer's job is GONE — the fleet is \
                         genuinely dead; resuming dead-peer declarations",
                    );
                }
                self.escalated = true;
            } else {
                // No Alive seen but at least one Unknown → fail-closed.
                if !self.warned_authoritative_defer {
                    tracing::warn!(
                        suppressed_for_s = now.saturating_duration_since(first).as_secs_f64(),
                        escalation_window_s = escalation_window.as_secs_f64(),
                        remote_members = remote_total,
                        unknown_count = lives.iter().filter(|s| matches!(s, PeerLifeState::Unknown)).count(),
                        co_located_secondary = co_located_secondary_id,
                        co_located_last_frame_age_s = co_located_last_frame_age.map(|d| d.as_secs_f64()),
                        "collective silence outlived escalation window with no slurm-authoritative \
                         evidence either way (probe stale or unable to read) — fail-closed: \
                         CONTINUING to defer until evidence lands",
                    );
                    self.warned_authoritative_defer = true;
                }
            }
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
                co_located_secondary = co_located_secondary_id,
                co_located_last_frame_age_s = co_located_last_frame_age.map(|d| d.as_secs_f64()),
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
    use crate::authority_snapshot::test_helpers::StaticSnapshot;
    use crate::authority_snapshot::{PeerLifeState, SlurmAuthoritativeSnapshot};
    use std::collections::HashMap;

    const WINDOW: Duration = Duration::from_millis(200);

    fn obs(id: &str, remote: bool, silent: bool, hard: bool) -> SilenceObservation {
        SilenceObservation {
            secondary_id: id.into(),
            remote,
            silent,
            hard,
        }
    }

    /// Empty static snapshot: every id reads `Unknown`. The escalation
    /// path fail-closes on this — the gate keeps deferring past the
    /// window UNLESS authoritative evidence confirms the fleet is gone.
    fn unknown_snapshot() -> StaticSnapshot {
        StaticSnapshot {
            map: HashMap::new(),
            count: None,
        }
    }

    /// Static snapshot with `state` for every id in `ids`. The
    /// authority tiebreak escalates only when all silent peers report
    /// `Gone`; one `Alive` peer pins the gate in defer.
    fn snapshot_with(ids: &[&str], state: PeerLifeState) -> StaticSnapshot {
        let map = ids.iter().map(|i| ((*i).into(), state)).collect();
        StaticSnapshot { map, count: None }
    }

    fn observe(
        gate: &mut CollectiveSilenceGate,
        members: &[SilenceObservation],
        now: Instant,
        snap: &dyn SlurmAuthoritativeSnapshot,
    ) -> Option<Duration> {
        gate.observe(members, now, WINDOW, snap, None, None)
    }

    /// ALL remotes silent (≥2) defers; one live remote — proof the
    /// local ingest works — never engages the gate, even with every
    /// other member past the hard backstop.
    #[test]
    fn defers_only_when_every_remote_is_silent() {
        let mut gate = CollectiveSilenceGate::new();
        let t0 = Instant::now();
        let snap = unknown_snapshot();

        // One live remote among silent ones: the wire is proven, the
        // schedule declares as today.
        let mixed = [obs("a", true, true, true), obs("b", true, false, false)];
        assert_eq!(observe(&mut gate, &mixed, t0, &snap), None);
        assert_eq!(gate.deferring(), None);

        // Every remote silent: self-suspect, defer.
        let all = [obs("a", true, true, false), obs("b", true, true, true)];
        let age = observe(&mut gate, &all, t0 + Duration::from_millis(10), &snap)
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
        let snap = unknown_snapshot();

        // Fresh co-located member + two silent remotes: defer.
        let colocated = [
            obs("local", false, false, false),
            obs("a", true, true, true),
            obs("b", true, true, false),
        ];
        assert!(observe(&mut gate, &colocated, t0, &snap).is_some());

        // Single remote silent past hard (plus the local member):
        // below MIN_REMOTE_MEMBERS — gate stays out of the way.
        let single = [
            obs("local", false, false, false),
            obs("a", true, true, true),
        ];
        assert_eq!(
            observe(&mut gate, &single, t0 + Duration::from_millis(10), &snap),
            None
        );
        assert_eq!(gate.deferring(), None);
    }

    /// SLURM-AUTHORITATIVE ESCALATION: once a due HARD declaration has
    /// been suppressed past the escalation window AND slurm reports
    /// every silent peer's job GONE, the gate escalates (declarations
    /// resume) for the remainder of the episode.
    #[test]
    fn collective_silence_escalates_when_authoritative_says_all_gone() {
        let mut gate = CollectiveSilenceGate::new();
        let t0 = Instant::now();
        let all_warn = [obs("a", true, true, false), obs("b", true, true, false)];
        let all_hard = [obs("a", true, true, true), obs("b", true, true, true)];
        let gone = snapshot_with(&["a", "b"], PeerLifeState::Gone);

        // WARN-stage collective silence: deferring, but the escalation
        // clock has not started (no hard declaration is due yet).
        assert!(observe(&mut gate, &all_warn, t0, &gone).is_some());
        // Hard becomes due: still deferred, escalation clock starts NOW.
        assert!(
            observe(&mut gate, &all_hard, t0 + Duration::from_millis(300), &gone).is_some(),
            "the WARN-stage episode age must not pre-burn the escalation \
             window: the clock runs from the first SUPPRESSED hard"
        );
        // Inside the window: still deferred.
        assert!(
            observe(&mut gate, &all_hard, t0 + Duration::from_millis(450), &gone).is_some()
        );
        // Past the window AND authority says all Gone: escalated.
        assert_eq!(
            observe(&mut gate, &all_hard, t0 + Duration::from_millis(550), &gone),
            None,
            "deferral must be bounded; a genuinely all-dead fleet (per slurm) is declared"
        );
        assert_eq!(gate.deferring(), None);
        // Escalation latches for the episode.
        assert_eq!(
            observe(&mut gate, &all_hard, t0 + Duration::from_millis(600), &gone),
            None
        );
    }

    /// Past the window, if slurm-authoritative evidence shows ≥1 silent
    /// peer is still `Alive`, the gate KEEPS DEFERRING (the silence is
    /// local-deafness, not a real death — the run_20260615_112332
    /// face). This is the #544 tiebreak that prevents the wall-clock
    /// false-positive escalation.
    #[test]
    fn collective_silence_defers_when_authoritative_says_all_alive() {
        let mut gate = CollectiveSilenceGate::new();
        let t0 = Instant::now();
        let all_hard = [obs("a", true, true, true), obs("b", true, true, true)];
        let alive = snapshot_with(&["a", "b"], PeerLifeState::Alive);

        assert!(observe(&mut gate, &all_hard, t0, &alive).is_some());
        // Past the window: WOULD escalate on wall-clock alone, but
        // authority says Alive → continue deferring.
        assert!(
            observe(&mut gate, &all_hard, t0 + Duration::from_millis(300), &alive).is_some(),
            "authority says Alive → gate must continue deferring past the window"
        );
        assert!(gate.deferring().is_some());
    }

    /// Past the window with NO authoritative evidence either way
    /// (snapshot stale / probe failure / no probe wired), the gate
    /// fail-closes: continue deferring until evidence lands.
    #[test]
    fn collective_silence_defers_on_unknown_authority() {
        let mut gate = CollectiveSilenceGate::new();
        let t0 = Instant::now();
        let all_hard = [obs("a", true, true, true), obs("b", true, true, true)];
        let snap = unknown_snapshot();

        assert!(observe(&mut gate, &all_hard, t0, &snap).is_some());
        // Past the window with no positive evidence: fail-closed →
        // continue deferring.
        assert!(
            observe(&mut gate, &all_hard, t0 + Duration::from_millis(300), &snap).is_some(),
            "no authoritative evidence either way → fail-closed defer"
        );
        assert!(gate.deferring().is_some());
    }

    /// One Alive among Unknowns is still "any_alive" — the gate keeps
    /// deferring (the fleet is not provably all-gone).
    #[test]
    fn collective_silence_defers_when_any_alive_among_unknowns() {
        let mut gate = CollectiveSilenceGate::new();
        let t0 = Instant::now();
        let three_hard = [
            obs("a", true, true, true),
            obs("b", true, true, true),
            obs("c", true, true, true),
        ];
        // a is Alive, b/c are Unknown (no map entries).
        let mut map = HashMap::new();
        map.insert("a".into(), PeerLifeState::Alive);
        let mixed = StaticSnapshot { map, count: None };

        assert!(observe(&mut gate, &three_hard, t0, &mixed).is_some());
        assert!(
            observe(&mut gate, &three_hard, t0 + Duration::from_millis(300), &mixed).is_some(),
            "one Alive (rest Unknown) → defer past window"
        );
    }

    /// Recovery resets the WHOLE gate: after a remote frame ends the
    /// episode, a later collective episode starts fresh (new episode
    /// clock, new escalation window, no leftover latch).
    #[test]
    fn remote_evidence_resets_episode_and_escalation() {
        let mut gate = CollectiveSilenceGate::new();
        let t0 = Instant::now();
        let all_hard = [obs("a", true, true, true), obs("b", true, true, true)];
        let one_live = [obs("a", true, true, true), obs("b", true, false, false)];
        let gone = snapshot_with(&["a", "b"], PeerLifeState::Gone);

        // Drive into escalation (authority confirms Gone past window).
        assert!(observe(&mut gate, &all_hard, t0, &gone).is_some());
        assert_eq!(
            observe(&mut gate, &all_hard, t0 + Duration::from_millis(300), &gone),
            None
        );

        // A remote frame proves the wire: episode over.
        assert_eq!(
            observe(&mut gate, &one_live, t0 + Duration::from_millis(350), &gone),
            None
        );

        // A NEW collective episode defers again from scratch.
        assert!(
            observe(&mut gate, &all_hard, t0 + Duration::from_millis(400), &gone).is_some(),
            "a fresh episode must re-arm the deferral (no leftover \
             escalation latch from the previous one)"
        );
    }
}
