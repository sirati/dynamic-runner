//! Honest secondary primary-liveness: `run_election_tick`'s
//! `need_election` predicate is honest BY SOURCE, so the secondary rides
//! out a transient keepalive blip exactly as the primary side does.
//!
//! `need_election = primary_link.should_arm_failover()         // (A)
//!                  || primary_last_seen older than backstop`   // (B)
//!
//! These tests pin the four behaviours the (A)+(B) split must deliver:
//!   (i)   transient blip — keepalives stop, the route stays up, staleness
//!         below the backstop → NO election (the spurious-15s-election the
//!         old bare-staleness trigger produced is gone);
//!   (ii)  busy genuine-death — the route arms via (A) → fast election,
//!         WITHOUT waiting the patient backstop;
//!   (iii) wedged-but-routable primary — the route never arms but
//!         `primary_last_seen` exceeds the backstop → election at (B);
//!   (iv)  recovery — a primary message arriving cancels an in-`Suspecting`
//!         election (the recovery the blip never let escalate to).
//!
//! Deterministic time: the (B) staleness predicate reads
//! `std::time::Instant` (NOT a tokio timer), so a "wedged primary" is
//! simulated by an explicitly backdated `primary_last_seen`. Leg (A) is
//! driven through `PrimaryLink::record_recv_failure` (the same no-route
//! probe `send_to_primary` feeds in production) so the test does not have
//! to stand up an unrouteable transport.

#![cfg(test)]

use std::time::{Duration, Instant};

use super::super::election::ElectionState;
use super::super::test_helpers::{FakeWorkerFactory, election_config, make_secondary};
use super::super::wire::timestamp_now;
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, DistributedMessage, KeepaliveRole, PrimaryChangeReason,
};

/// The tiny app-silence backstop `election_config` installs (leg (B)).
/// Kept in sync with `test_helpers::election_config`.
const BACKSTOP: Duration = Duration::from_millis(100);

/// (i) Transient blip: the primary's keepalives stop briefly but the QUIC
/// connection stays up, so NO primary-bound send ever errors — leg (A)
/// (`should_arm_failover`) stays silent — and the keepalive staleness has
/// not yet reached the patient backstop (leg (B)). `need_election` must be
/// false: the secondary rides out the blip instead of spuriously electing,
/// exactly as the primary side patiently waits.
///
/// This is the asymmetry the fix closes: the OLD bare
/// `keepalive_interval × keepalive_miss_threshold` (≈15s) trigger would
/// have fired here.
#[tokio::test(flavor = "current_thread")]
async fn transient_blip_no_election() {
    let mut sec = make_secondary(election_config("sec-a"));
    sec.enter_operational_for_test();
    // A live mesh peer so a (wrongly) firing election would have a
    // non-degraded path to Suspecting — isolating "we stayed Normal"
    // as a genuine no-election, not a degraded bail.
    sec.op_mut()
        .peer_keepalives
        .insert("sec-b".into(), Instant::now());
    sec.cluster_state.apply(ClusterMutation::PrimaryChanged {
        new: "primary-orig".into(),
        epoch: 1,
        reason: PrimaryChangeReason::Election,
    });
    // Primary spoke once, then went quiet: `primary_last_seen` is recent.
    sec.record_primary_message();

    // The route NEVER errored (we drive no `record_recv_failure`), so
    // leg (A) is silent — the honest "connection still up" signal.
    assert!(
        !sec.op_mut().primary_link.should_arm_failover(),
        "a quiet-but-live link must not arm the fast leg",
    );

    // Keep the keepalive staleness BELOW the patient backstop so leg (B)
    // is inactive too. A 40ms quiet stretch is well under the 100ms
    // backstop. (election_config sets the backstop to the OLD bare
    // 15s-equivalent death deadline — keepalive_interval ×
    // keepalive_miss_threshold = 50ms × 2 = 100ms — so this 40ms quiet is
    // a window in which the OLD trigger had ALREADY have started counting
    // toward a spurious election; the new predicate stays Normal.)
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert!(BACKSTOP > Duration::from_millis(40));

    let actions = sec.run_election_tick();
    assert!(
        matches!(sec.op_mut().election, ElectionState::Normal),
        "transient blip (route up, staleness < backstop) must NOT elect; got {:?}",
        std::mem::discriminant(&sec.op_mut().election),
    );
    assert!(
        actions.broadcast.is_empty(),
        "no TimeoutQuery should be broadcast during a blip",
    );
    assert!(sec.fatal_exit.is_none());
}

/// (ii) Busy genuine-death: the primary link arms via leg (A) (a
/// primary-bound send returned no-route, the honest dead-link signal) even
/// though the keepalive staleness has NOT reached the patient backstop.
/// `need_election` must fire FAST through (A) — the secondary enters
/// Suspecting and broadcasts a `TimeoutQuery` without waiting the 2-min
/// backstop. This is why the bare deadline is NOT merely lengthened to the
/// backstop: a genuinely dead link must still fail over quickly.
#[tokio::test(flavor = "current_thread")]
async fn busy_genuine_death_arms_fast_via_leg_a() {
    let mut sec = make_secondary(election_config("sec-a"));
    sec.enter_operational_for_test();
    sec.op_mut()
        .peer_keepalives
        .insert("sec-b".into(), Instant::now());
    sec.cluster_state.apply(ClusterMutation::PrimaryChanged {
        new: "primary-orig".into(),
        epoch: 1,
        reason: PrimaryChangeReason::Election,
    });
    // Primary JUST spoke — `primary_last_seen` is fresh, so leg (B) is
    // nowhere near firing.
    sec.record_primary_message();

    // Drive the no-route probe past the count threshold (3 in
    // election_config) — the exact signal `send_to_primary` feeds on a
    // dead link. After the third, leg (A) is armed.
    for _ in 0..3 {
        sec.op_mut().primary_link.record_recv_failure();
    }
    assert!(
        sec.op_mut().primary_link.should_arm_failover(),
        "three no-route probes must arm the fast leg",
    );
    // Leg (B) is NOT what fires here: staleness is fresh, far under the
    // backstop.
    let stale = Instant::now()
        .duration_since(sec.op_mut().primary_last_seen.expect("set by record_primary_message"));
    assert!(
        stale < BACKSTOP,
        "leg (B) must be inactive — staleness {stale:?} should be under backstop {BACKSTOP:?}",
    );

    let actions = sec.run_election_tick();
    assert!(
        matches!(sec.op_mut().election, ElectionState::Suspecting { .. }),
        "a dead link (leg A) must elect FAST without waiting the backstop; got {:?}",
        std::mem::discriminant(&sec.op_mut().election),
    );
    assert!(
        actions
            .broadcast
            .iter()
            .any(|m| matches!(m, DistributedMessage::TimeoutQuery { .. })),
        "fast (leg A) election must broadcast TimeoutQuery",
    );
}

/// (iii) Wedged-but-routable primary: the connection stays up (no
/// primary-bound send ever errors, so leg (A) is silent) but the primary
/// has stopped emitting keepalives at the application layer. `need_election`
/// must fire through the patient backstop (leg (B)) once `primary_last_seen`
/// staleness exceeds it — the ONLY death mode the fast leg structurally
/// cannot catch.
#[tokio::test(flavor = "current_thread")]
async fn wedged_primary_elects_at_backstop() {
    let mut sec = make_secondary(election_config("sec-a"));
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

    // The route never errored — leg (A) stays silent across the whole
    // test (a wedged primary's connection remains routable).
    assert!(
        !sec.op_mut().primary_link.should_arm_failover(),
        "a routable-but-wedged primary must NOT arm the fast leg",
    );

    // App-silence past the backstop: backdate `primary_last_seen` well
    // beyond it (deterministic — no wall-clock racing).
    sec.op_mut().primary_last_seen = Some(Instant::now() - (BACKSTOP + Duration::from_millis(50)));

    // Leg (A) is still silent; only leg (B) carries this.
    assert!(!sec.op_mut().primary_link.should_arm_failover());

    let actions = sec.run_election_tick();
    assert!(
        matches!(sec.op_mut().election, ElectionState::Suspecting { .. }),
        "a wedged-but-routable primary must elect via the backstop (leg B); got {:?}",
        std::mem::discriminant(&sec.op_mut().election),
    );
    assert!(
        actions
            .broadcast
            .iter()
            .any(|m| matches!(m, DistributedMessage::TimeoutQuery { .. })),
        "backstop (leg B) election must broadcast TimeoutQuery",
    );
}

/// (iv) Recovery: a primary message arriving while an election is
/// in-`Suspecting` cancels it back to `Normal` (`record_primary_message`).
/// This is the recovery a transient blip never lets escalate to — but if a
/// brief outage DID arm leg (A) and start an election, a resumed primary
/// message must abort it cleanly, NOT leave the node mid-failover.
#[tokio::test(flavor = "current_thread")]
async fn primary_recovery_cancels_in_flight_election() {
    let mut sec = make_secondary(election_config("sec-a"));
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

    // Arm leg (A) and run a tick → Suspecting.
    for _ in 0..3 {
        sec.op_mut().primary_link.record_recv_failure();
    }
    sec.run_election_tick();
    assert!(
        matches!(sec.op_mut().election, ElectionState::Suspecting { .. }),
        "precondition: an election must be in flight (Suspecting)",
    );

    // The primary speaks again through the real recognition path: a
    // `Primary`-tagged keepalive whose originator IS the current primary
    // routes to `record_primary_message`.
    sec.handle_inbound(
        DistributedMessage::Keepalive {
            sender_id: "primary-orig".into(),
            timestamp: timestamp_now(),
            secondary_id: "primary-orig".into(),
            active_workers: 0,
            emitter_role: KeepaliveRole::Primary,
        },
        &mut FakeWorkerFactory,
    )
    .await;

    assert!(
        matches!(sec.op_mut().election, ElectionState::Normal),
        "a resumed primary message must cancel the in-flight election; got {:?}",
        std::mem::discriminant(&sec.op_mut().election),
    );
    // Recovery also reset the link-health sub-state, so a follow-up tick
    // does not immediately re-arm leg (A).
    assert!(
        !sec.op_mut().primary_link.should_arm_failover(),
        "primary recovery must reset the fast-leg health window",
    );
    let actions = sec.run_election_tick();
    assert!(
        matches!(sec.op_mut().election, ElectionState::Normal),
        "post-recovery the node stays Normal",
    );
    assert!(actions.broadcast.is_empty());
}
