//! Last-resort fleet-death presumption for the zero-authority observer.
//!
//! # Single concern
//!
//! ONE concern: derive, from evidence the observer ACTUALLY OWNS, the
//! verdict "the whole fleet is unreachable and presumed dead" — and make
//! that a BOUNDED terminal instead of an infinite stale-snapshot spin.
//!
//! # Why (the asm-dataset LMU bring-up death)
//!
//! When every SLURM job FAILED (the 15 secondaries' setup deadlines all
//! expired and the quorum-proceed relocated the primary into the dead
//! fleet), the local observer looped forever on "last CRDT snapshot
//! still shows 10 live worker-secondary members … mesh running
//! autonomously": the [`super::lost_visibility::MeshLiveness`] gate reads
//! the REPLICATED membership ledger, and a fleet that dies WITHOUT
//! originating `PeerRemoved` mutations leaves that ledger frozen at its
//! last converged (alive-looking) state. A stale snapshot is evidence of
//! the PAST, not the present — reassurance keyed on it alone can spin
//! forever while the submitter never exits and no verdict is ever
//! rendered.
//!
//! The observer cannot see `sacct`. What it CAN see, and what this
//! detector derives from, is exclusively its OWN present-tense evidence:
//!
//! * its transport shows ZERO live legs (`peer_count() == 0` — no member
//!   is wired by any path), AND
//! * NOTHING has been received from ANY member for far past every
//!   timeout in the system (`last-received-anything age ≥` the
//!   presumption threshold, default
//!   [`super::ObserverConfig::DEFAULT_FLEET_DEATH_PRESUMPTION`] = 20
//!   minutes — 4× the 300s `peer_timeout`, 4× the 5-minute wake-loss
//!   threshold), AND
//! * reconnect recovery has been DRIVEN and failed: at least
//!   [`MIN_RECONNECT_ATTEMPTS`] lost-visibility recovery cycles fired
//!   (the ~60s [`super::lost_visibility`] cadence — each triggers the
//!   tunnel rebuild where one is wired) without restoring a single leg
//!   or frame.
//!
//! # rc-B respected (report-and-retry stays primary)
//!
//! This is a LAST-RESORT bounded terminal, not a new strand-exit: the
//! never-fatal [`super::lost_visibility::LostVisibilityReporter`]
//! machinery is untouched (immediate full-log diagnostics, the 5-minute
//! wake-loss gating, the ~60s report+reconnect cadence all run first and
//! keep running). Only after the LONG threshold of total silence with
//! failed recovery does the observer stop asserting what it cannot
//! verify and render the honest verdict — loudly on the wake stream
//! (distinct wording, via the coordinator's single terminal-reason emit
//! site) and as a non-zero exit
//! ([`crate::primary::RunError::FatalPolicyExit`], the documented home
//! of deliberate policy aborts). Any sign of life (a leg, a frame)
//! before the threshold fully resets the episode.

use std::time::Duration;

use tokio::time::Instant;

/// Minimum lost-visibility recovery cycles (each one a report + a driven
/// reconnect attempt where a reconnector is wired) that must have fired
/// during the CURRENT silence episode before the presumption may trip.
/// Guards against a pathological clock jump declaring death without
/// recovery ever having been driven.
pub(crate) const MIN_RECONNECT_ATTEMPTS: u32 = 3;

/// What [`FleetDeathDetector::observe`] tells the coordinator this
/// iteration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FleetDeathVerdict {
    /// Some member is wired, or a frame arrived since the last check —
    /// the fleet is not silent; the episode (if any) was reset.
    Alive,
    /// Total silence, but the presumption threshold / attempt floor has
    /// not been reached — keep observing (the lost-visibility machinery
    /// owns the reporting + retrying meanwhile).
    Watching,
    /// The last resort: zero legs, nothing received from ANY member for
    /// `threshold`, and ≥ [`MIN_RECONNECT_ATTEMPTS`] recovery cycles
    /// failed. `reason` is the operator-facing verdict line for the
    /// coordinator's terminal-reason emit + the `FatalPolicyExit`.
    PresumedDead { reason: String },
}

/// Episode-tracking state machine. Single writer (the observer run loop,
/// `LocalSet`-bound) — no synchronisation. It owns ONLY the derivation;
/// the coordinator owns the inputs (its transport view, its inbound
/// clock, the recovery cadence) and the exit action.
#[derive(Debug)]
pub(crate) struct FleetDeathDetector {
    /// The presumption threshold (config-derived:
    /// `ObserverConfig::fleet_death_presumption`).
    threshold: Duration,
    /// Recovery cycles fired during the CURRENT silence episode. Reset
    /// whenever the fleet shows any sign of life.
    reconnect_attempts: u32,
    /// The `last_inbound_at` observed on the previous call — a NEWER
    /// value means a frame arrived in between (sign of life), even if
    /// the transport view already collapsed again.
    seen_inbound_at: Option<Instant>,
}

impl FleetDeathDetector {
    pub(crate) fn new(threshold: Duration) -> Self {
        Self {
            threshold,
            reconnect_attempts: 0,
            seen_inbound_at: None,
        }
    }

    /// One lost-visibility recovery cycle fired (the ~60s
    /// report+reconnect cadence — [`super::lost_visibility::RetryDirective::ReconnectDue`]).
    pub(crate) fn note_reconnect_attempt(&mut self) {
        self.reconnect_attempts = self.reconnect_attempts.saturating_add(1);
    }

    /// Feed the present-tense evidence; learn the verdict.
    ///
    /// * `zero_legs` — the transport's live-leg view is empty
    ///   (`peer_count() == 0`).
    /// * `last_inbound_at` — when the observer last received ANYTHING
    ///   from ANY member (every inbound frame, regardless of type or
    ///   sender).
    pub(crate) fn observe(
        &mut self,
        zero_legs: bool,
        last_inbound_at: Instant,
        now: Instant,
    ) -> FleetDeathVerdict {
        let fresh_inbound = self
            .seen_inbound_at
            .is_some_and(|seen| last_inbound_at > seen);
        self.seen_inbound_at = Some(last_inbound_at);
        if !zero_legs || fresh_inbound {
            // Sign of life — full episode reset. The attempts that fired
            // belonged to an episode that is over.
            self.reconnect_attempts = 0;
            return FleetDeathVerdict::Alive;
        }
        let silence = now.saturating_duration_since(last_inbound_at);
        if silence < self.threshold || self.reconnect_attempts < MIN_RECONNECT_ATTEMPTS {
            return FleetDeathVerdict::Watching;
        }
        FleetDeathVerdict::PresumedDead {
            reason: format!(
                "fleet unreachable — presumed dead: nothing received from ANY \
                 member for {}s (presumption threshold {}s, far past every \
                 keepalive/peer timeout), the transport shows zero live legs, \
                 and {} reconnect recovery cycles failed to restore a single \
                 one. The observer cannot verify the mesh survived — its last \
                 CRDT snapshot is STALE evidence of the past — so it stops \
                 spinning on it and exits non-zero (last-resort bounded \
                 terminal; the run rendered no verdict of its own).",
                silence.as_secs(),
                self.threshold.as_secs(),
                self.reconnect_attempts,
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(t0: Instant, secs: u64) -> Instant {
        t0 + Duration::from_secs(secs)
    }

    /// The production replay (asm-dataset LMU): zero legs, zero inbound,
    /// reconnect cycles firing and failing → past the threshold the
    /// verdict is `PresumedDead` with the distinct wording — never an
    /// infinite `Watching` spin.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn total_silence_past_threshold_with_failed_reconnects_is_presumed_dead() {
        let t0 = Instant::now();
        let mut d = FleetDeathDetector::new(Duration::from_secs(600));
        let last_inbound = t0;
        // Drive the episode: every minute one recovery cycle fires.
        for m in 1..=9 {
            d.note_reconnect_attempt();
            assert_eq!(
                d.observe(true, last_inbound, at(t0, m * 60)),
                FleetDeathVerdict::Watching,
                "below the threshold the detector only watches (minute {m})"
            );
        }
        d.note_reconnect_attempt();
        match d.observe(true, last_inbound, at(t0, 600)) {
            FleetDeathVerdict::PresumedDead { reason } => {
                assert!(
                    reason.contains("presumed dead"),
                    "the verdict carries the distinct wording: {reason}"
                );
                assert!(reason.contains("600s"), "names the silence: {reason}");
            }
            other => panic!("at the threshold with failed recovery: {other:?}"),
        }
    }

    /// A wired leg OR a fresh inbound frame resets the episode — the
    /// presumption never builds across signs of life.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn any_sign_of_life_resets_the_episode() {
        let t0 = Instant::now();
        let mut d = FleetDeathDetector::new(Duration::from_secs(100));
        for _ in 0..5 {
            d.note_reconnect_attempt();
        }
        // Legs present: alive, attempts reset.
        assert_eq!(
            d.observe(false, t0, at(t0, 200)),
            FleetDeathVerdict::Alive,
            "a wired leg is life regardless of inbound age"
        );
        // Legs gone again, silence already past the threshold by age —
        // but the attempts were reset, so it only watches.
        assert_eq!(d.observe(true, t0, at(t0, 350)), FleetDeathVerdict::Watching);
        for _ in 0..3 {
            d.note_reconnect_attempt();
        }
        // A FRESH inbound frame (newer last_inbound_at) is life even with
        // zero legs at sampling time.
        assert_eq!(
            d.observe(true, at(t0, 400), at(t0, 401)),
            FleetDeathVerdict::Alive,
            "a frame received since the last check is life"
        );
        // And the silence clock restarts from the new inbound instant.
        for _ in 0..3 {
            d.note_reconnect_attempt();
        }
        assert_eq!(
            d.observe(true, at(t0, 400), at(t0, 450)),
            FleetDeathVerdict::Watching,
            "50s of silence against a 100s threshold"
        );
        assert!(matches!(
            d.observe(true, at(t0, 400), at(t0, 500)),
            FleetDeathVerdict::PresumedDead { .. }
        ));
    }

    /// Silence alone never trips the verdict: without the recovery-cycle
    /// floor (reconnects driven and failed) the detector keeps watching —
    /// a clock jump cannot declare death before recovery was attempted.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn silence_without_driven_recovery_keeps_watching() {
        let t0 = Instant::now();
        let mut d = FleetDeathDetector::new(Duration::from_secs(60));
        assert_eq!(
            d.observe(true, t0, at(t0, 3600)),
            FleetDeathVerdict::Watching,
            "an hour of silence with zero driven recovery cycles must not trip"
        );
        d.note_reconnect_attempt();
        d.note_reconnect_attempt();
        assert_eq!(
            d.observe(true, t0, at(t0, 3660)),
            FleetDeathVerdict::Watching,
            "below the attempt floor"
        );
        d.note_reconnect_attempt();
        assert!(matches!(
            d.observe(true, t0, at(t0, 3720)),
            FleetDeathVerdict::PresumedDead { .. }
        ));
    }
}
