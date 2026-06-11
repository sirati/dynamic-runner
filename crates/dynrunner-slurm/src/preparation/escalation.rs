//! Half-dead-tunnel escalation (#342): the pure per-secondary state
//! machine that decides when the observer-reconnect liveness gate's
//! alive-NO-OP verdict has repeated long enough to prove the tunnel
//! HALF-DEAD and force a rebuild anyway.
//!
//! # The defect this closes
//!
//! The liveness gate in
//! [`SlurmPreparation::reestablish_one_tunnel`](super::pipeline::SlurmPreparation::reestablish_one_tunnel)
//! reads a tunnel ALIVE via `Child::try_wait` — a LOCAL-process probe.
//! A tunnel can be half-dead: the local `ssh -N -R` process and its
//! master TCP session survive (so ssh's own ServerAlive never trips),
//! while the WORKER-side `-R` forward is gone. The gate then NO-OPs
//! the rebuild forever, the secondary's bootstrap-redial dials a dead
//! forward indefinitely, and the observer stays blind until run end.
//!
//! # The escalation (and why it cannot regress the gate)
//!
//! The gate exists so the ~60s lost-visibility reconnect cadence NEVER
//! release+rebinds against its own HEALTHY forward (the rc=255 churn /
//! self-kill class). The escalation keeps that property by demanding
//! PERSISTENCE before it overrides the gate: it counts, per secondary,
//! CONSECUTIVE alive-noop reconnect ticks. Each tick only happens
//! while the observer's visibility is LOST (the cadence stops firing
//! on recovery), so K consecutive ticks ⇒ the gate said "healthy" K
//! times in a row AND visibility never recovered in between. A healthy
//! forward that is actually carrying recovery flips visibility back
//! within ~one tick of the secondary's redial landing — the ticks then
//! STOP, the streak goes stale, and the escalation never fires. Only a
//! forward that looks alive locally yet delivers nothing for K straight
//! cadence periods gets force-rebuilt — and even then exactly once,
//! with the streak reset, so a misjudged force degenerates to one
//! rebuild per K cadence periods, never an every-tick churn loop.
//!
//! # Reset semantics
//!
//! * **Recovery resets.** The seam has no positive "visibility
//!   recovered" call (the cadence simply stops invoking it), so
//!   recovery is detected as a STALE streak: a noop tick arriving more
//!   than [`ReconnectEscalation::fresh_episode_gap`] after the previous
//!   one belongs to a NEW loss episode and restarts the streak at 1.
//!   Lost-visibility ticks arrive every ~60s
//!   (`REPORT_RECURRENCE` in the observer's lost-visibility reporter),
//!   so any gap well above that means visibility recovered in between.
//! * **A rebuild resets.** Whether the gate found a dead child (normal
//!   rebuild) or the escalation forced one, a completed rebuild calls
//!   [`ReconnectEscalation::on_rebuilt`] and the streak restarts from
//!   zero — a fresh child gets the full K-tick benefit of the doubt.
//! * **Firing resets.** [`EscalationVerdict::ForceRebuild`] itself
//!   clears the streak, so a force whose rebuild then FAILS (node
//!   unreachable, …) is retried only after K further ticks, never
//!   tick-after-tick.
//!
//! Pure: no clocks, no I/O, no logging — the caller supplies `now` and
//! renders the verdict. Single writer (the reestablish path), guarded
//! by the owning [`SlurmPreparation`]'s mutex.

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Force a rebuild after this many CONSECUTIVE alive-noop reconnect
/// ticks without recovery. At the observer's ~60s cadence this rebuilds
/// a half-dead tunnel within ~3 minutes, while a healthy forward
/// carrying recovery flips visibility back within ~one tick and never
/// reaches the threshold.
const DEFAULT_FORCE_AFTER: u32 = 3;

/// A noop tick arriving more than this after the previous one starts a
/// NEW loss episode (the streak restarts at 1). Must comfortably exceed
/// the cadence period (~60s) so consecutive ticks of ONE episode chain,
/// while any recovered-in-between gap resets. 5 cadence periods.
const DEFAULT_FRESH_EPISODE_GAP: Duration = Duration::from_secs(300);

/// What the reestablish path should do with an alive-noop tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EscalationVerdict {
    /// Trust the liveness gate — keep the no-op. Carries the current
    /// consecutive-noop streak for the caller's log line.
    Tolerate { streak: u32 },
    /// The streak hit the threshold: the tunnel is presumed half-dead.
    /// Override the gate and force the rebuild (registry replace +
    /// release + respawn). The streak is already reset.
    ForceRebuild,
}

/// Per-secondary consecutive alive-noop streak.
#[derive(Debug, Clone, Copy)]
struct Streak {
    count: u32,
    last_noop: Instant,
}

/// The per-secondary escalation tracker. One instance per
/// [`SlurmPreparation`](super::pipeline::SlurmPreparation), shared by
/// every reestablish call on that manager.
#[derive(Debug)]
pub(crate) struct ReconnectEscalation {
    force_after: u32,
    fresh_episode_gap: Duration,
    streaks: HashMap<String, Streak>,
}

impl Default for ReconnectEscalation {
    fn default() -> Self {
        Self::new(DEFAULT_FORCE_AFTER, DEFAULT_FRESH_EPISODE_GAP)
    }
}

impl ReconnectEscalation {
    /// `force_after` is clamped to ≥1 (0 would force on a state the
    /// machine never observes).
    pub(crate) fn new(force_after: u32, fresh_episode_gap: Duration) -> Self {
        Self {
            force_after: force_after.max(1),
            fresh_episode_gap,
            streaks: HashMap::new(),
        }
    }

    /// Record one alive-noop reconnect tick for `secondary_id` at `now`
    /// and learn whether to keep tolerating or force the rebuild.
    pub(crate) fn on_alive_noop(&mut self, secondary_id: &str, now: Instant) -> EscalationVerdict {
        let streak = self
            .streaks
            .entry(secondary_id.to_owned())
            .and_modify(|s| {
                // A stale streak means the cadence stopped firing in
                // between — visibility recovered — so this tick opens a
                // NEW loss episode.
                if now.duration_since(s.last_noop) > self.fresh_episode_gap {
                    s.count = 0;
                }
                s.count += 1;
                s.last_noop = now;
            })
            .or_insert(Streak {
                count: 1,
                last_noop: now,
            })
            .count;
        if streak >= self.force_after {
            self.streaks.remove(secondary_id);
            EscalationVerdict::ForceRebuild
        } else {
            EscalationVerdict::Tolerate { streak }
        }
    }

    /// A rebuild for `secondary_id` completed (gate-found-dead path or
    /// forced path alike): the fresh child starts with a clean slate.
    pub(crate) fn on_rebuilt(&mut self, secondary_id: &str) {
        self.streaks.remove(secondary_id);
    }
}
