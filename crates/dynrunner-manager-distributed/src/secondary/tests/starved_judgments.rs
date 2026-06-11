//! #423 — silence-based death/liveness judgments must DEFER while THIS
//! node's own event-loop tick lagged (CPU starvation/freeze).
//!
//! Production replay (asm-tokenizer run_20260611_172436, ~14 Ghidra-JVM
//! workers per 14-core node, every node fully loaded): 6 of 11 secondaries
//! fatal-exited on "peer mesh required for failover but not available:
//! primary death suspected (primary_silence_exceeded=true)". The primary
//! was alive-but-unheard; each judging secondary's OWN keepalive arm had
//! lagged (its `MissedTickBehavior::Skip` catch-up tick fired the instant
//! the runtime unfroze, BEFORE the mesh pump drained the inbound backlog
//! into the liveness clocks), so `now - primary_last_seen` measured OUR
//! stall as the primary's silence.
//!
//! The fix routes EVERY silence judgment through the shared
//! `crate::own_tick_health` authority: a lagged tick re-bases a trustworthy
//! floor, and each judgment reads `now - trustworthy_anchor(last_seen)`, so
//! the starved window contributes ZERO silence. These tests pin:
//!   * leg (B) (primary-silence backstop) DEFERS under a lagged own tick,
//!     where the bare staleness predicate WOULD have elected (RED→GREEN);
//!   * the peer-keepalive reaper does NOT prune a live mesh off our stall
//!     (the mesh-view-emptiness face);
//!   * the NEGATIVE: a genuinely-silent primary with a HEALTHY own tick
//!     still elects at the backstop (no liveness regression).

#![cfg(test)]

use std::time::{Duration, Instant};

use super::super::election::ElectionState;
use super::super::test_helpers::{election_config, make_secondary_membership};
use dynrunner_protocol_primary_secondary::address::PeerId;
use dynrunner_protocol_primary_secondary::{ClusterMutation, PrimaryChangeReason};

/// The tiny app-silence backstop `election_config` installs (leg (B)).
const BACKSTOP: Duration = Duration::from_millis(100);

/// Drive the shared own-tick-health authority into the STARVED state: a
/// first tick to seed `last_tick_at`, then a second tick whose inter-tick
/// gap is far past the cadence threshold — arming the trustworthy floor at
/// ~`Instant::now()`. Mirrors the production wake-from-freeze: the keepalive
/// arm's catch-up tick fires long after its scheduled cadence.
fn starve_own_tick<M, S, E, I>(sec: &mut super::super::SecondaryCoordinator<M, S, E, I>)
where
    M: dynrunner_protocol_manager_worker::ManagerEndpoint + 'static,
    S: dynrunner_scheduler_api::Scheduler<I> + Clone,
    E: dynrunner_scheduler_api::ResourceEstimator<I> + Clone,
    I: dynrunner_core::Identifier,
{
    let now = Instant::now();
    // A first observed tick (10s in the past) seeds the previous-tick clock;
    // the next tick at `now` then measures a 10s inter-tick gap ≫ the 150ms
    // (3× 50ms cadence) starvation threshold.
    assert!(!sec.own_tick_health.observe_tick(now - Duration::from_secs(10)));
    assert!(
        sec.own_tick_health.observe_tick(now),
        "a 10s inter-tick gap must be judged starved"
    );
}

/// RED→GREEN: leg (B) (the patient primary-silence backstop) must NOT fire
/// when the primary is alive-but-unheard and THIS node's own tick lagged.
///
/// The bare staleness predicate is unambiguously met: `primary_last_seen`
/// is backdated well past the backstop, so without the own-tick re-base
/// `run_election_tick` would enter Suspecting (and, on a degraded mesh,
/// fatal-exit). With the fix the lagged tick re-bases the trustworthy floor
/// to ~now, the leg-(B) clamp lifts `primary_last_seen` above
/// `now - backstop`, and the node stays Normal — deferring the judgment.
#[tokio::test(flavor = "current_thread")]
async fn lagged_own_tick_defers_primary_silence_election() {
    let (mut sec, _members) = make_secondary_membership(
        election_config("sec-a"),
        // Primary AND a live peer are transport members, so leg (C) does not
        // mask and a (wrongly) firing election reaches Suspecting (not a
        // degraded bail) — isolating "we stayed Normal" as a genuine
        // no-election.
        vec![PeerId::from("primary-orig"), PeerId::from("sec-b")],
    );
    sec.enter_operational_for_test();
    sec.op_mut()
        .peer_keepalives
        .insert("sec-b".into(), Instant::now());
    sec.cluster_state.apply(ClusterMutation::PrimaryChanged {
        new: "primary-orig".into(),
        epoch: 1,
        reason: PrimaryChangeReason::Election,
    });
    sec.record_primary_message();

    // The route never errored (leg A silent) and the primary is a member
    // (leg C silent) — only leg (B) is in play.
    assert!(!sec.op_mut().primary_link.should_arm_failover());

    // The bare leg-(B) condition is MET: `primary_last_seen` is stale past
    // the backstop. This is exactly what fired in production.
    sec.op_mut().primary_last_seen = Some(Instant::now() - (BACKSTOP + Duration::from_millis(50)));

    // ...but THIS node's own tick just lagged (CPU starvation): re-base the
    // trustworthy floor to ~now.
    starve_own_tick(&mut sec);

    let actions = sec.run_election_tick();
    assert!(
        matches!(sec.op_mut().election, ElectionState::Normal),
        "a lagged own tick must DEFER the primary-silence judgment — the \
         backstop staleness reflects OUR stall, not the primary's silence; \
         got {:?}",
        std::mem::discriminant(&sec.op_mut().election),
    );
    assert!(
        actions.broadcast.is_empty(),
        "no TimeoutQuery should be broadcast while own-tick-starved",
    );
    assert!(
        sec.fatal_exit.is_none(),
        "no fatal exit while own-tick-starved",
    );
}

/// RED→GREEN (mesh-view-emptiness face): the peer-keepalive reaper
/// (`check_peer_timeouts`) must NOT prune a LIVE mesh peer off our own
/// stall. The peer's `last_seen` is stale past `peer_timeout` only because
/// our runtime froze; clamping to the trustworthy floor keeps it in
/// `peer_keepalives` (and therefore in `live_peer_ids` → the failover quorum
/// denominator). Pre-fix this empties the quorum denominator, the very
/// `mesh_degraded`/empty-`live_peers` shape that drove the production fatal
/// exit and lone-survivor split-brain.
#[tokio::test(flavor = "current_thread")]
async fn lagged_own_tick_does_not_prune_live_peer() {
    // peer_timeout is 120s in election_config; backdate the peer past it.
    let (mut sec, _members) = make_secondary_membership(
        election_config("sec-a"),
        vec![PeerId::from("primary-orig"), PeerId::from("sec-b")],
    );
    sec.enter_operational_for_test();
    sec.cluster_state.apply(ClusterMutation::PrimaryChanged {
        new: "primary-orig".into(),
        epoch: 1,
        reason: PrimaryChangeReason::Election,
    });
    // sec-b's last keepalive is stale past the 120s peer_timeout — but only
    // because our own tick froze for that long.
    sec.op_mut()
        .peer_keepalives
        .insert("sec-b".into(), Instant::now() - Duration::from_secs(200));

    starve_own_tick(&mut sec);
    sec.check_peer_timeouts();

    assert!(
        sec.op_mut().peer_keepalives.contains_key("sec-b"),
        "a live peer must NOT be pruned off our own stall — the clamp re-bases \
         its silence to post-lag evidence, keeping the failover quorum honest",
    );
}

/// NEGATIVE (no liveness regression): a genuinely-silent primary with a
/// HEALTHY own tick still elects at the backstop within the normal window.
/// The own-tick floor is unarmed (no starvation observed), so
/// `trustworthy_anchor` is the identity and leg (B) fires exactly as before
/// the fix.
#[tokio::test(flavor = "current_thread")]
async fn healthy_own_tick_still_elects_on_genuine_primary_silence() {
    let (mut sec, _members) = make_secondary_membership(
        election_config("sec-a"),
        vec![PeerId::from("primary-orig"), PeerId::from("sec-b")],
    );
    sec.enter_operational_for_test();
    sec.op_mut()
        .peer_keepalives
        .insert("sec-b".into(), Instant::now());
    sec.cluster_state.apply(ClusterMutation::PrimaryChanged {
        new: "primary-orig".into(),
        epoch: 1,
        reason: PrimaryChangeReason::Election,
    });
    sec.record_primary_message();
    assert!(!sec.op_mut().primary_link.should_arm_failover());

    // Healthy own tick: two on-cadence observations leave the floor unarmed.
    let now = Instant::now();
    assert!(!sec.own_tick_health.observe_tick(now - Duration::from_millis(60)));
    assert!(
        !sec.own_tick_health.observe_tick(now),
        "an on-cadence inter-tick gap is NOT starved",
    );

    // The primary genuinely went silent past the backstop.
    sec.op_mut().primary_last_seen = Some(Instant::now() - (BACKSTOP + Duration::from_millis(50)));

    let actions = sec.run_election_tick();
    assert!(
        matches!(sec.op_mut().election, ElectionState::Suspecting { .. }),
        "a genuinely-silent primary with a healthy own tick must still elect \
         at the backstop (no liveness regression); got {:?}",
        std::mem::discriminant(&sec.op_mut().election),
    );
    assert!(
        actions
            .broadcast
            .iter()
            .any(|m| matches!(
                m,
                dynrunner_protocol_primary_secondary::DistributedMessage::TimeoutQuery { .. }
            )),
        "the honest backstop election must broadcast TimeoutQuery",
    );
}
