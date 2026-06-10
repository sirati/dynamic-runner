//! Pure tests for the half-dead-tunnel escalation state machine
//! (#342): K consecutive alive-noop reconnect ticks without recovery
//! force a rebuild; recovery (a stale streak), a completed rebuild,
//! and the force itself each reset the streak. No clocks, no I/O —
//! the tests drive `now` explicitly.

use std::time::{Duration, Instant};

use crate::preparation::escalation::{EscalationVerdict, ReconnectEscalation};

const GAP: Duration = Duration::from_secs(300);
/// The observer's lost-visibility cadence period — consecutive ticks
/// of one loss episode arrive about this far apart.
const TICK: Duration = Duration::from_secs(60);

fn machine() -> ReconnectEscalation {
    ReconnectEscalation::new(3, GAP)
}

/// THE escalation: the 3rd consecutive alive-noop tick (one cadence
/// period apart — same loss episode) forces the rebuild; the first two
/// tolerate with an accurate streak count.
#[test]
fn forces_on_kth_consecutive_noop() {
    let mut m = machine();
    let t0 = Instant::now();
    assert_eq!(
        m.on_alive_noop("secondary-0", t0),
        EscalationVerdict::Tolerate { streak: 1 }
    );
    assert_eq!(
        m.on_alive_noop("secondary-0", t0 + TICK),
        EscalationVerdict::Tolerate { streak: 2 }
    );
    assert_eq!(
        m.on_alive_noop("secondary-0", t0 + 2 * TICK),
        EscalationVerdict::ForceRebuild,
        "the 3rd consecutive no-op tick must force the rebuild"
    );
}

/// RECOVERY RESETS: ticks stop while visibility is recovered, so the
/// next noop arrives past the fresh-episode gap and restarts the streak
/// at 1 — a new loss episode never inherits an old episode's count.
/// (This is the guard against force-killing a healthy tunnel on the
/// FIRST tick of a later, unrelated loss episode.)
#[test]
fn stale_streak_resets_on_new_episode() {
    let mut m = machine();
    let t0 = Instant::now();
    m.on_alive_noop("secondary-0", t0);
    m.on_alive_noop("secondary-0", t0 + TICK);
    // Visibility recovers; the cadence goes quiet. The next loss episode
    // begins well past the gap.
    let t1 = t0 + TICK + GAP + Duration::from_secs(1);
    assert_eq!(
        m.on_alive_noop("secondary-0", t1),
        EscalationVerdict::Tolerate { streak: 1 },
        "a noop past the fresh-episode gap must restart the streak"
    );
    // And the new episode still needs the full K ticks to force.
    assert_eq!(
        m.on_alive_noop("secondary-0", t1 + TICK),
        EscalationVerdict::Tolerate { streak: 2 }
    );
    assert_eq!(
        m.on_alive_noop("secondary-0", t1 + 2 * TICK),
        EscalationVerdict::ForceRebuild
    );
}

/// A gap EXACTLY at the boundary still chains (the reset is strictly
/// `> gap`): cadence jitter must not split one episode in two.
#[test]
fn gap_at_boundary_still_chains() {
    let mut m = machine();
    let t0 = Instant::now();
    m.on_alive_noop("secondary-0", t0);
    assert_eq!(
        m.on_alive_noop("secondary-0", t0 + GAP),
        EscalationVerdict::Tolerate { streak: 2 },
        "a gap of exactly fresh_episode_gap belongs to the same episode"
    );
}

/// A COMPLETED REBUILD resets: after `on_rebuilt` the fresh child gets
/// the full K-tick benefit of the doubt.
#[test]
fn rebuilt_resets_streak() {
    let mut m = machine();
    let t0 = Instant::now();
    m.on_alive_noop("secondary-0", t0);
    m.on_alive_noop("secondary-0", t0 + TICK);
    m.on_rebuilt("secondary-0");
    assert_eq!(
        m.on_alive_noop("secondary-0", t0 + 2 * TICK),
        EscalationVerdict::Tolerate { streak: 1 },
        "a rebuild must clear the streak"
    );
}

/// FIRING RESETS: after a ForceRebuild verdict the streak restarts, so
/// a force whose rebuild then fails is retried only after K further
/// ticks — never tick-after-tick churn.
#[test]
fn force_resets_streak() {
    let mut m = machine();
    let t0 = Instant::now();
    m.on_alive_noop("secondary-0", t0);
    m.on_alive_noop("secondary-0", t0 + TICK);
    assert_eq!(
        m.on_alive_noop("secondary-0", t0 + 2 * TICK),
        EscalationVerdict::ForceRebuild
    );
    assert_eq!(
        m.on_alive_noop("secondary-0", t0 + 3 * TICK),
        EscalationVerdict::Tolerate { streak: 1 },
        "the tick after a force must start a fresh streak"
    );
}

/// Streaks are PER-SECONDARY: ticks for one id never advance another's.
#[test]
fn streaks_are_per_secondary() {
    let mut m = machine();
    let t0 = Instant::now();
    m.on_alive_noop("secondary-0", t0);
    m.on_alive_noop("secondary-0", t0 + TICK);
    assert_eq!(
        m.on_alive_noop("secondary-1", t0 + 2 * TICK),
        EscalationVerdict::Tolerate { streak: 1 },
        "another id's ticks must not bleed into this id's streak"
    );
    // secondary-0 still forces on ITS 3rd tick.
    assert_eq!(
        m.on_alive_noop("secondary-0", t0 + 2 * TICK),
        EscalationVerdict::ForceRebuild
    );
}

/// `force_after` is clamped to ≥1 — a zero threshold forces on the
/// very first noop instead of never/immediately-on-no-state.
#[test]
fn force_after_clamped_to_one() {
    let mut m = ReconnectEscalation::new(0, GAP);
    assert_eq!(
        m.on_alive_noop("secondary-0", Instant::now()),
        EscalationVerdict::ForceRebuild
    );
}
