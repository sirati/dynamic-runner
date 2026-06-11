//! [`OwnTickHealth`] — the single owner of "is THIS node's own measurement
//! clock trustworthy right now, and how much of the recent window is
//! invalid?".
//!
//! # Concern
//!
//! A node measures a peer's silence as `now - last_evidence_from_peer`.
//! That arithmetic is only honest while THIS node's own event-loop tick
//! ran on cadence: a CPU-starved / frozen runtime fires its deferred
//! timers in a burst the instant it unfreezes, BEFORE the mesh pump has
//! drained the inbound backlog into the per-peer clocks — so a judgment
//! taken at that instant measures THIS node's OWN stall as every peer's
//! silence (the wake-from-freeze face). EVERY silence-based death /
//! liveness judgment in this crate must therefore consult one shared
//! authority before trusting its silence reads; this module IS that
//! authority.
//!
//! # The signal
//!
//! A periodic loop tick (the primary's heartbeat sweep, the secondary's
//! keepalive arm) feeds its wall-clock instant to [`OwnTickHealth::observe_tick`]
//! once per tick. The inter-tick gap (`now - last_tick`) is the own-tick
//! lag: a gap stretched beyond [`STARVATION_TICK_MULTIPLE`] × the loop's
//! cadence means the runtime was frozen/starved for the interim, so every
//! silence age the node would measure across that gap is inflated by its
//! own stall, not the peer's silence.
//!
//! # The three faces of the verdict
//!
//! - DEFER (the boolean) — [`observe_tick`] returns `true` on a lagged
//!   tick. A caller that judges a whole peer-SET in one sweep (the
//!   primary) reads this and skips the sweep entirely; the judgment is
//!   named-deferred to the next on-cadence tick.
//! - RE-BASE (the floor) — a lagged tick advances `trustworthy_since` to
//!   `now`, and [`trustworthy_anchor`] clamps any silence anchor up to
//!   that floor. A caller that judges per-anchor silence (the secondary's
//!   primary-silence backstop + its peer-keepalive reaper) reads its
//!   silence as `now - trustworthy_anchor(last_evidence)`, so the
//!   starved window contributes ZERO silence: the peer is judged from
//!   fresh, post-lag evidence. A genuinely-dead peer is still detected one
//!   healthy cadence window later (the floor expires only when a real
//!   frame advances the anchor past it). Correctness over speed.
//! - ACCRUE (the judged clock) — every tick advances a monotone
//!   starvation-honest clock ([`judged_elapsed`]) by its inter-tick gap
//!   CAPPED at the starvation threshold, so a frozen window contributes
//!   at most one threshold's worth of judgeable time however long the
//!   wall-clock stall was. Consumers that must keep judging through
//!   CHRONIC starvation (see below) measure peer silence as a difference
//!   of judged-clock readings instead of wall-clock instants: the clock
//!   never runs faster than wall time, so a judged silence can only
//!   UNDERSTATE a wall silence — the false-mass-removal face of a
//!   wake-from-freeze burst is structurally impossible on it.
//!
//! # Chronic starvation must escalate, never defer forever
//!
//! The DEFER face assumes the next on-cadence tick eventually comes. On a
//! node whose runtime is CHRONICALLY starved (a saturated host: every
//! inter-tick gap stretches past the threshold for minutes on end —
//! the run_20260611_200548 face) that tick never arrives, so a
//! defer-only consumer would postpone every death judgment unboundedly:
//! genuinely dead peers are never removed, and everything downstream of
//! removal (task requeue, membership, respawn) is inert exactly when it
//! is needed. A consumer that opts in via
//! [`OwnTickHealth::new_with_chronic_escalation`] therefore gets a
//! bounded deferral: once one continuous starved streak has spanned the
//! caller's escalation window (its hard silence window — judgments were
//! deferred for longer than a full death verdict takes), the DEFER
//! verdict drops away (sweeps resume; the onset is WARNed loudly) and the
//! trustworthy floor FREEZES (per-anchor clamps stop chasing `now`, so
//! silence can accrue again from the last pre-chronic floor). The
//! consumer is expected to judge on the ACCRUE face while
//! [`in_chronic_starvation`] reports `true`. A single acute freeze (one
//! long gap, e.g. suspend/resume) never escalates: the streak is measured
//! from its first STARVED TICK, so the wake tick still defers and
//! re-bases exactly as before.
//!
//! All faces share ONE lag measurement, ONE threshold, and ONE throttled
//! operator WARN, so the primary's sweep guard and the secondary's
//! election/peer-liveness judgments cannot drift on what "my own tick
//! lagged" means.
//!
//! [`judged_elapsed`]: OwnTickHealth::judged_elapsed
//! [`in_chronic_starvation`]: OwnTickHealth::in_chronic_starvation

use std::time::{Duration, Instant};

use crate::warn_throttle::WarnThrottle;

/// How many loop cadences the tick's OWN inter-tick gap may stretch before
/// the node is judged locally starved. 3× sits meaningfully above
/// scheduler/timer jitter on a healthy loop (the tick is an `Interval` on
/// the cadence) and far below any hard death backstop (the primary's
/// `silence_hard_multiple` is 24× by default; the secondary's
/// `primary_silence_backstop` is ≈120s ≫ 3× the keepalive interval), so a
/// single deferred judgment delays a GENUINE death declaration by one
/// cadence window at most.
pub(crate) const STARVATION_TICK_MULTIPLE: u32 = 3;

/// Minimum spacing between two own-tick-starvation WARNs. A pinned runtime
/// can lag many consecutive ticks; a minute-cadence WARN with a suppressed
/// count keeps the starvation narrated without one line per tick.
const STARVATION_WARN_INTERVAL: Duration = Duration::from_secs(60);

/// PURE: did the inter-tick gap (`now - prev_tick`) stretch beyond
/// `threshold`? `prev_tick == None` (the first tick) is never starved —
/// the silence anchors were seeded at welcome/hydrate/promotion, so the
/// first tick's ages are honest. Strictly-greater so a gap exactly at the
/// threshold is healthy.
fn inter_tick_gap_starved(prev_tick: Option<Instant>, now: Instant, threshold: Duration) -> bool {
    match prev_tick {
        Some(prev) => now.saturating_duration_since(prev) > threshold,
        None => false,
    }
}

/// The shared own-tick-health authority. Owned by whichever coordinator
/// drives a silence-judging loop (the `PrimaryCoordinator`'s heartbeat
/// sweep, the `SecondaryCoordinator`'s keepalive arm); fed once per tick
/// via [`Self::observe_tick`] and read either as the DEFER boolean (that
/// call's return) or the RE-BASE floor ([`Self::trustworthy_anchor`]).
#[derive(Debug)]
pub(crate) struct OwnTickHealth {
    /// `STARVATION_TICK_MULTIPLE` × the loop cadence — the inter-tick gap
    /// budget. Stored (not recomputed per tick) so the cadence authority is
    /// captured once at construction.
    starvation_threshold: Duration,
    /// Chronic-starvation escalation window: once ONE continuous starved
    /// streak has spanned this, the DEFER verdict drops away and the floor
    /// freezes (see the module doc). `None` (the plain [`Self::new`]
    /// constructor) keeps the legacy defer-indefinitely behaviour for
    /// per-anchor consumers that never gate a sweep on the boolean.
    chronic_after: Option<Duration>,
    /// When the loop tick LAST ran. `None` until the first `observe_tick`.
    last_tick_at: Option<Instant>,
    /// The floor below which a silence anchor cannot be trusted: every
    /// ACUTE starved tick advances it to that tick's `now`, so silence
    /// measured against `max(anchor, trustworthy_since)` excludes the
    /// frozen window. `None` until the first starvation is observed (the
    /// identity clamp). FROZEN while the chronic escalation is active.
    trustworthy_since: Option<Instant>,
    /// The monotone starvation-honest clock (the ACCRUE face): advances
    /// per tick by `min(inter-tick gap, starvation_threshold)`, so it
    /// tracks wall time on a healthy loop and accrues at most one
    /// threshold per scheduling round under starvation.
    judged_elapsed: Duration,
    /// First starved tick of the CURRENT starved streak. Cleared by any
    /// healthy tick.
    starved_streak_since: Option<Instant>,
    /// Latched while the current streak has spanned `chronic_after`.
    chronic: bool,
    /// Throttle for the starvation WARN.
    warn: WarnThrottle,
}

impl OwnTickHealth {
    /// Build the authority for a loop running on `cadence` (the keepalive
    /// interval). The starvation threshold is [`STARVATION_TICK_MULTIPLE`] ×
    /// `cadence`. No chronic escalation: the DEFER verdict repeats for as
    /// long as the ticks keep lagging (the per-anchor consumers' shape —
    /// they never gate a judgment on the boolean).
    pub(crate) fn new(cadence: Duration) -> Self {
        Self::build(cadence, None)
    }

    /// As [`Self::new`], with the chronic-starvation escalation armed: once
    /// one continuous starved streak has spanned `chronic_after` (the
    /// caller's hard silence window — deferral has then outlived a full
    /// death verdict), [`Self::observe_tick`] stops returning the DEFER
    /// verdict and the trustworthy floor freezes; the caller is expected to
    /// judge on the [`Self::judged_elapsed`] clock while
    /// [`Self::in_chronic_starvation`] reports `true`.
    pub(crate) fn new_with_chronic_escalation(cadence: Duration, chronic_after: Duration) -> Self {
        Self::build(cadence, Some(chronic_after))
    }

    fn build(cadence: Duration, chronic_after: Option<Duration>) -> Self {
        Self {
            starvation_threshold: cadence.saturating_mul(STARVATION_TICK_MULTIPLE),
            chronic_after,
            last_tick_at: None,
            trustworthy_since: None,
            judged_elapsed: Duration::ZERO,
            starved_streak_since: None,
            chronic: false,
            warn: WarnThrottle::new(STARVATION_WARN_INTERVAL),
        }
    }

    /// Record one loop tick at `now` and return whether THIS node's own
    /// tick lagged past the starvation threshold AND the lag is still
    /// acute (the DEFER verdict).
    ///
    /// Every tick advances the starvation-honest judged clock by its gap
    /// capped at the threshold. On an ACUTE lagged tick: advance the
    /// trustworthy floor to `now` (so every subsequent silence read
    /// re-bases off fresh, post-lag evidence), emit a throttled operator
    /// WARN naming the lag and the threshold, and return `true` — a
    /// whole-sweep judge (the primary) skips this sweep; a per-anchor
    /// judge (the secondary) need not branch on it, its
    /// [`Self::trustworthy_anchor`] reads already re-based. Once the
    /// starved streak has spanned the chronic escalation window (only with
    /// [`Self::new_with_chronic_escalation`]), the verdict flips to
    /// `false` — sweeps resume on the judged clock — and the floor stays
    /// frozen so per-anchor silence can accrue again.
    pub(crate) fn observe_tick(&mut self, now: Instant) -> bool {
        let gap = self
            .last_tick_at
            .map(|prev| now.saturating_duration_since(prev));
        let starved = inter_tick_gap_starved(self.last_tick_at, now, self.starvation_threshold);
        self.last_tick_at = Some(now);
        // The ACCRUE face: real time on a healthy loop (gap ≤ threshold by
        // definition), at most one threshold per round under starvation.
        if let Some(gap) = gap {
            self.judged_elapsed += gap.min(self.starvation_threshold);
        }
        if !starved {
            self.starved_streak_since = None;
            self.chronic = false;
            return false;
        }
        let streak_since = *self.starved_streak_since.get_or_insert(now);
        let chronic = self
            .chronic_after
            .is_some_and(|bound| now.saturating_duration_since(streak_since) > bound);
        if chronic && !self.chronic {
            // Escalation onset — once per streak, NOT throttled: the
            // operator must see exactly when deferral gave way to
            // judged-clock judgments.
            tracing::warn!(
                streak_s = now.saturating_duration_since(streak_since).as_secs_f64(),
                escalation_window_s = self
                    .chronic_after
                    .unwrap_or_default()
                    .as_secs_f64(),
                "own-tick starvation is CHRONIC: the starved streak has \
                 spanned the full judgment window, so silence judgments \
                 cannot stay deferred without never judging at all — \
                 resuming death/liveness judgments on starvation-honest \
                 accrued time (each lagged round contributes at most one \
                 starvation threshold of judgeable silence)"
            );
        }
        self.chronic = chronic;
        if !chronic {
            self.trustworthy_since = Some(now);
        }
        if let Some(suppressed) = self.warn.permit() {
            tracing::warn!(
                threshold_s = self.starvation_threshold.as_secs_f64(),
                suppressed_since_last_warn = suppressed,
                chronic,
                "own tick lagged far past the loop cadence (local runtime \
                 starvation/freeze) — every wall-clock silence this node \
                 would measure across the gap reflects OUR stall, not peer \
                 silence; acute: deferring silence-based death/liveness \
                 judgments and re-basing the silence window to fresh \
                 post-lag evidence; chronic: judging on starvation-honest \
                 accrued time instead"
            );
        }
        !chronic
    }

    /// The monotone starvation-honest clock (the ACCRUE face): consumers
    /// judging through chronic starvation measure a peer's silence as
    /// `judged_elapsed() - judged_elapsed-at-last-evidence`. Never runs
    /// faster than wall time, so a judged silence can only UNDERSTATE the
    /// wall silence.
    pub(crate) fn judged_elapsed(&self) -> Duration {
        self.judged_elapsed
    }

    /// `true` while the chronic-starvation escalation is active (the
    /// current starved streak has spanned the escalation window). Only
    /// ever `true` for an authority built with
    /// [`Self::new_with_chronic_escalation`].
    pub(crate) fn in_chronic_starvation(&self) -> bool {
        self.chronic
    }

    /// Clamp a silence `anchor` (a peer's last-evidence-of-life instant) up
    /// to the trustworthy floor: `max(anchor, trustworthy_since)`. The seam
    /// EVERY per-anchor silence judgment consumes — silence becomes
    /// `now - trustworthy_anchor(last_evidence)`, so a freeze that
    /// pre-dates the anchor contributes no silence. With no starvation yet
    /// observed (`trustworthy_since == None`) this is the identity (the
    /// anchor is trusted as-is).
    pub(crate) fn trustworthy_anchor(&self, anchor: Instant) -> Instant {
        match self.trustworthy_since {
            Some(floor) => anchor.max(floor),
            None => anchor,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// PURE `inter_tick_gap_starved`: `None` (first tick) is never starved;
    /// a gap at or under the threshold is healthy; a gap beyond it is
    /// starved (the watchdog fires-under-load law: a stored Instant compared
    /// per tick, not a resettable select-arm sleep).
    #[test]
    fn inter_tick_gap_judges_starvation() {
        let threshold = Duration::from_millis(150); // 3× a 50ms cadence
        let now = Instant::now();
        assert!(!inter_tick_gap_starved(None, now, threshold));
        assert!(!inter_tick_gap_starved(
            Some(now - Duration::from_millis(60)),
            now,
            threshold
        ));
        // Exactly at the threshold: healthy (strictly-greater fires).
        assert!(!inter_tick_gap_starved(
            Some(now - threshold),
            now,
            threshold
        ));
        // A genuine stall: starved.
        assert!(inter_tick_gap_starved(
            Some(now - Duration::from_millis(400)),
            now,
            threshold
        ));
    }

    /// `observe_tick`: the first tick is never starved and arms no floor; an
    /// on-cadence tick stays healthy with an identity clamp; a lagged tick
    /// returns the DEFER verdict and advances the floor so subsequent
    /// clamps re-base.
    #[test]
    fn observe_tick_defers_and_rebases_on_lag() {
        let mut h = OwnTickHealth::new(Duration::from_millis(50)); // threshold 150ms
        let t0 = Instant::now();
        let old_anchor = t0 - Duration::from_secs(10);

        // First tick: never starved, floor unarmed → identity clamp.
        assert!(!h.observe_tick(t0));
        assert_eq!(h.trustworthy_anchor(old_anchor), old_anchor);

        // On-cadence second tick: healthy, still identity.
        let t1 = t0 + Duration::from_millis(60);
        assert!(!h.observe_tick(t1));
        assert_eq!(h.trustworthy_anchor(old_anchor), old_anchor);

        // A lagged tick (400ms gap > 150ms): DEFER + floor armed to t2.
        let t2 = t1 + Duration::from_millis(400);
        assert!(h.observe_tick(t2));
        assert_eq!(
            h.trustworthy_anchor(old_anchor),
            t2,
            "a stale anchor must clamp UP to the post-lag floor"
        );
        // A fresher anchor (past the floor) is trusted as-is.
        let fresh = t2 + Duration::from_millis(10);
        assert_eq!(h.trustworthy_anchor(fresh), fresh);
    }

    /// The ACCRUE face: the judged clock tracks wall time on a healthy
    /// loop and contributes at most one starvation threshold per lagged
    /// round, so an arbitrarily long freeze can never inflate it.
    #[test]
    fn judged_clock_caps_each_gap_at_the_threshold() {
        let mut h = OwnTickHealth::new(Duration::from_millis(50)); // threshold 150ms
        let t0 = Instant::now();
        h.observe_tick(t0);
        assert_eq!(h.judged_elapsed(), Duration::ZERO, "first tick accrues nothing");

        // Healthy gap: full credit.
        let t1 = t0 + Duration::from_millis(60);
        h.observe_tick(t1);
        assert_eq!(h.judged_elapsed(), Duration::from_millis(60));

        // A 10s freeze: capped at the 150ms threshold.
        let t2 = t1 + Duration::from_secs(10);
        h.observe_tick(t2);
        assert_eq!(
            h.judged_elapsed(),
            Duration::from_millis(210),
            "a frozen window contributes at most one threshold of judgeable time"
        );
    }

    /// The chronic escalation: while a starved streak is ACUTE (span ≤
    /// the escalation window) every lagged tick defers and re-bases the
    /// floor; once the streak spans the window, the DEFER verdict drops
    /// away (sweeps resume), `in_chronic_starvation` reports `true`, and
    /// the floor FREEZES at its last pre-chronic value so per-anchor
    /// silence can accrue again. A healthy tick ends the streak and
    /// restores normal behaviour.
    #[test]
    fn chronic_streak_escalates_and_freezes_the_floor() {
        // cadence 50ms → threshold 150ms; escalation window 300ms.
        let mut h = OwnTickHealth::new_with_chronic_escalation(
            Duration::from_millis(50),
            Duration::from_millis(300),
        );
        let t0 = Instant::now();
        assert!(!h.observe_tick(t0));

        // Streak tick 1 (acute: span 0): defer + re-base.
        let t1 = t0 + Duration::from_millis(200);
        assert!(h.observe_tick(t1), "an acute lagged tick defers");
        assert!(!h.in_chronic_starvation());

        // Streak tick 2 (span 200ms ≤ 300ms): still acute.
        let t2 = t1 + Duration::from_millis(200);
        assert!(h.observe_tick(t2), "still inside the escalation window");
        assert!(!h.in_chronic_starvation());

        // Streak tick 3 (span 400ms > 300ms): CHRONIC — no defer, floor
        // frozen at t2 (the last acute re-base).
        let t3 = t2 + Duration::from_millis(200);
        assert!(
            !h.observe_tick(t3),
            "a chronic lagged tick must NOT defer (judgments resume)"
        );
        assert!(h.in_chronic_starvation());
        let stale = t0 - Duration::from_secs(5);
        assert_eq!(
            h.trustworthy_anchor(stale),
            t2,
            "the floor froze at the last ACUTE re-base; chronic ticks no \
             longer chase `now`, so silence can accrue from the floor"
        );

        // A healthy tick ends the streak and clears the escalation.
        let t4 = t3 + Duration::from_millis(60);
        assert!(!h.observe_tick(t4));
        assert!(!h.in_chronic_starvation());

        // A NEW streak starts acute again (defer + re-base resume).
        let t5 = t4 + Duration::from_millis(200);
        assert!(h.observe_tick(t5), "a fresh streak defers from its first tick");
        assert_eq!(h.trustworthy_anchor(stale), t5);
    }

    /// Without the opt-in constructor the boolean NEVER escalates — the
    /// legacy defer-indefinitely shape per-anchor consumers rely on.
    #[test]
    fn plain_constructor_never_escalates() {
        let mut h = OwnTickHealth::new(Duration::from_millis(50));
        let mut t = Instant::now();
        h.observe_tick(t);
        for _ in 0..50 {
            t += Duration::from_millis(200);
            assert!(h.observe_tick(t), "every lagged tick keeps deferring");
            assert!(!h.in_chronic_starvation());
        }
    }

    /// The floor PERSISTS across subsequent healthy ticks (it is not reset
    /// by a healthy tick), so a genuinely-dead peer accumulates fresh
    /// silence only from the recovery instant — detected one cadence window
    /// later, never off the starved window.
    #[test]
    fn floor_persists_until_anchor_advances_past_it() {
        let mut h = OwnTickHealth::new(Duration::from_millis(50));
        let t0 = Instant::now();
        h.observe_tick(t0);
        let lagged = t0 + Duration::from_millis(400);
        assert!(h.observe_tick(lagged));
        // A healthy tick after recovery does NOT clear the floor.
        let recovered = lagged + Duration::from_millis(60);
        assert!(!h.observe_tick(recovered));
        let stale_anchor = t0 - Duration::from_secs(5);
        assert_eq!(
            h.trustworthy_anchor(stale_anchor),
            lagged,
            "the floor stays at the lag instant; a stale anchor still clamps to it"
        );
    }
}
