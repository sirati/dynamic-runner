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
//! # The two faces of the verdict
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
//!
//! Both faces share ONE lag measurement, ONE threshold, and ONE throttled
//! operator WARN, so the primary's sweep guard and the secondary's
//! election/peer-liveness judgments cannot drift on what "my own tick
//! lagged" means.

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
    /// When the loop tick LAST ran. `None` until the first `observe_tick`.
    last_tick_at: Option<Instant>,
    /// The floor below which a silence anchor cannot be trusted: every
    /// starved tick advances it to that tick's `now`, so silence measured
    /// against `max(anchor, trustworthy_since)` excludes the frozen window.
    /// `None` until the first starvation is observed (the identity clamp).
    trustworthy_since: Option<Instant>,
    /// Throttle for the starvation WARN.
    warn: WarnThrottle,
}

impl OwnTickHealth {
    /// Build the authority for a loop running on `cadence` (the keepalive
    /// interval). The starvation threshold is [`STARVATION_TICK_MULTIPLE`] ×
    /// `cadence`.
    pub(crate) fn new(cadence: Duration) -> Self {
        Self {
            starvation_threshold: cadence.saturating_mul(STARVATION_TICK_MULTIPLE),
            last_tick_at: None,
            trustworthy_since: None,
            warn: WarnThrottle::new(STARVATION_WARN_INTERVAL),
        }
    }

    /// Record one loop tick at `now` and return whether THIS node's own
    /// tick lagged past the starvation threshold (the DEFER verdict).
    ///
    /// On a lagged tick: advance the trustworthy floor to `now` (so every
    /// subsequent silence read re-bases off fresh, post-lag evidence) and
    /// emit a throttled operator WARN naming the lag and the threshold.
    /// `true` tells a whole-sweep judge (the primary) to skip this sweep;
    /// a per-anchor judge (the secondary) need not branch on it — its
    /// [`Self::trustworthy_anchor`] reads already re-based.
    pub(crate) fn observe_tick(&mut self, now: Instant) -> bool {
        let starved = inter_tick_gap_starved(self.last_tick_at, now, self.starvation_threshold);
        self.last_tick_at = Some(now);
        if starved {
            self.trustworthy_since = Some(now);
            if let Some(suppressed) = self.warn.permit() {
                tracing::warn!(
                    threshold_s = self.starvation_threshold.as_secs_f64(),
                    suppressed_since_last_warn = suppressed,
                    "own tick lagged far past the loop cadence (local runtime \
                     starvation/freeze) — every silence this node would measure \
                     across the gap reflects OUR stall, not peer silence; \
                     deferring silence-based death/liveness judgments and \
                     re-basing the silence window to fresh post-lag evidence"
                );
            }
        }
        starved
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
