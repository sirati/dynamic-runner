//! #423-twin — `node_beacon_fresh` must re-base its last-beacon anchor
//! through the shared `own_tick_health` trustworthy floor, EXACTLY as the
//! leg-(B) backstop (`primary_silence_exceeded`) already does.
//!
//! The hole: `node_beacon_fresh` judged a primary's beacon staleness from
//! the RAW `Instant::now() - last_beacon`. The beacon LISTENER is an
//! ordinary tokio task (`liveness::listener`) that cannot drain inbound
//! beacon datagrams while THIS secondary's runtime is CPU-starved — so a
//! self-starved receiver measured its OWN stall as the primary's beacon
//! silence and false-judged an alive primary (whose dedicated-thread,
//! starvation-immune beacon was faithfully emitting) as beacon-stale. With
//! the beacon judged stale, `run_election_tick`'s `primary_beacon_fresh`
//! short-circuit lifts and a mesh-death leg arms a false election — the
//! self-starvation false-election that seeds the failover cascade.
//!
//! These tests pin the clamp (mirroring `starved_judgments` for leg (B)):
//!   * a self-starved secondary judges an alive primary's beacon FRESH
//!     under the clamp (RED→GREEN — the bare predicate WOULD have judged it
//!     stale);
//!   * NEGATIVE: a genuinely-silent beacon with a HEALTHY own tick is still
//!     judged stale (no liveness regression — the clamp is the identity).

#![cfg(test)]

use std::time::{Duration, Instant};

use dynrunner_protocol_primary_secondary::address::PeerId;
use dynrunner_protocol_primary_secondary::{ClusterMutation, PrimaryChangeReason};

use super::super::test_helpers::{election_config, make_secondary_membership};
use crate::liveness::BeaconLiveness;

const PRIMARY_ID: &str = "primary-orig";

/// The staleness yardstick `election_config` installs: `keepalive_interval ×
/// keepalive_miss_threshold` = 50ms × 2 = 100ms.
const BEACON_DEADLINE: Duration = Duration::from_millis(100);

/// Drive the shared own-tick-health authority into the STARVED state: a
/// first seed tick 10s in the past, then a tick at `now` whose 10s
/// inter-tick gap is far past the 150ms (3× 50ms cadence) starvation
/// threshold — arming the trustworthy floor at ~`now`. Identical to
/// `starved_judgments::starve_own_tick` (the leg-(B) twin).
fn starve_own_tick<M, S, E, I>(sec: &mut super::super::SecondaryCoordinator<M, S, E, I>)
where
    M: dynrunner_protocol_manager_worker::ManagerEndpoint + 'static,
    S: dynrunner_scheduler_api::Scheduler<I> + Clone,
    E: dynrunner_scheduler_api::ResourceEstimator<I> + Clone,
    I: dynrunner_core::Identifier,
{
    let now = Instant::now();
    assert!(!sec.own_tick_health.observe_tick(now - Duration::from_secs(10)));
    assert!(
        sec.own_tick_health.observe_tick(now),
        "a 10s inter-tick gap must be judged starved",
    );
}

/// RED→GREEN: a self-starved secondary must NOT false-judge an alive
/// primary's beacon as stale.
///
/// The beacon was last recorded just past the 100ms deadline (the listener
/// could not drain a fresher datagram while the runtime was frozen), so the
/// BARE `now - last_beacon` predicate is unambiguously stale. With the
/// own-tick clamp, the lagged tick re-bases the trustworthy floor to ~now,
/// the last-beacon anchor lifts above `now - deadline`, and the beacon is
/// judged FRESH — the primary is alive (its dedicated-thread beacon is
/// emitting), and the receiver's own stall must not be read as its silence.
#[tokio::test(flavor = "current_thread")]
async fn lagged_own_tick_judges_alive_primary_beacon_fresh() {
    let (mut sec, _members) = make_secondary_membership(
        election_config("sec-a"),
        vec![PeerId::from(PRIMARY_ID), PeerId::from("sec-b")],
    );
    sec.enter_operational_for_test();
    sec.cluster_state.apply(ClusterMutation::PrimaryChanged {
        new: PRIMARY_ID.into(),
        epoch: 1,
        reason: PrimaryChangeReason::Election,
    });

    // The primary's beacon was last recorded just past the deadline — the
    // self-starved listener could not drain a fresher one. Install a beacon
    // view we hold the writer side of and backdate the record.
    let beacon = BeaconLiveness::new();
    beacon.record_at(PRIMARY_ID, Instant::now() - (BEACON_DEADLINE + Duration::from_millis(50)));
    sec.set_beacon_liveness(beacon);

    // Sanity: with a HEALTHY own tick the bare predicate is stale.
    {
        let now = Instant::now();
        assert!(!sec.own_tick_health.observe_tick(now - Duration::from_millis(60)));
        assert!(!sec.own_tick_health.observe_tick(now));
        assert!(
            !sec.node_beacon_fresh(PRIMARY_ID),
            "fixture: with no starvation the backdated beacon is stale (the bare \
             predicate the production hole used)",
        );
    }

    // ...but THIS node's own tick just lagged (CPU starvation): re-base the
    // trustworthy floor to ~now.
    starve_own_tick(&mut sec);

    assert!(
        sec.node_beacon_fresh(PRIMARY_ID),
        "a self-starved secondary must judge an alive primary's beacon FRESH — \
         the staleness reflects OUR stall, not the primary's beacon silence",
    );
}

/// NEGATIVE (no liveness regression): a genuinely-silent beacon with a
/// HEALTHY own tick is still judged stale, so the beacon-union short-circuit
/// lifts and a genuine death still elects. The own-tick floor is unarmed
/// (no starvation), so `trustworthy_anchor` is the identity and the bare
/// staleness predicate is unchanged.
#[tokio::test(flavor = "current_thread")]
async fn healthy_own_tick_still_judges_silent_beacon_stale() {
    let (mut sec, _members) = make_secondary_membership(
        election_config("sec-a"),
        vec![PeerId::from(PRIMARY_ID), PeerId::from("sec-b")],
    );
    sec.enter_operational_for_test();
    sec.cluster_state.apply(ClusterMutation::PrimaryChanged {
        new: PRIMARY_ID.into(),
        epoch: 1,
        reason: PrimaryChangeReason::Election,
    });

    let beacon = BeaconLiveness::new();
    beacon.record_at(PRIMARY_ID, Instant::now() - (BEACON_DEADLINE + Duration::from_millis(50)));
    sec.set_beacon_liveness(beacon);

    // Healthy own tick: two on-cadence observations leave the floor unarmed.
    let now = Instant::now();
    assert!(!sec.own_tick_health.observe_tick(now - Duration::from_millis(60)));
    assert!(
        !sec.own_tick_health.observe_tick(now),
        "an on-cadence inter-tick gap is NOT starved",
    );

    assert!(
        !sec.node_beacon_fresh(PRIMARY_ID),
        "a genuinely-silent beacon with a healthy own tick must still be judged \
         stale (no liveness regression — the clamp is the identity)",
    );
}
