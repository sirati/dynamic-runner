//! THE AMPLIFIER cure — election arming is suppressed during the
//! post-failover `MeshReady` reconfirmation window.
//!
//! Production trace (affine=TRUE run): a transient node disruption (or a
//! transient self-starvation) caused ONE failover, and a pre-existing
//! amplifier turned it into a permanent self-sustaining cascade — the
//! primary epoch flapped 1→2→3→4 every ~20s, zero dispatch, never
//! recovered. The amplifier: every applied `PrimaryChanged` re-arms each
//! member's one-shot `MeshReady` reporter and resets the NEW primary's
//! mesh-confirmation set to empty, so proactive dispatch is VETOED until
//! each member re-reports `MeshReady` and the new primary processes it. If
//! the primary flips again FASTER than that reconfirmation completes,
//! members stay permanently unconfirmed → zero dispatch → no progress →
//! another failover arms → cascade. There was NO settle window after a
//! `PrimaryChanged` before another election could arm.
//!
//! These tests pin the gate added in
//! `secondary/election/coordinator.rs::run_election_tick`:
//!
//!   need_election = mesh_says_dead && !primary_beacon_fresh
//!                   && !run_terminal_latched && !within_settle_window
//!
//! where `within_settle_window` is `now - last_primary_change_at <
//! ELECTION_SETTLE_KEEPALIVE_MULTIPLE × keepalive_interval`, with
//! `last_primary_change_at` stamped on every applied `PrimaryChanged`.
//!
//! The two load-bearing pins:
//!   * a RECENT applied `PrimaryChanged` SUPPRESSES arming on the SAME
//!     mesh-death input that would otherwise arm (reconfirmation gets its
//!     window); and
//!   * the NEGATIVE / "does NOT break failover": a genuinely-dead primary
//!     PAST the settle window STILL arms — the window suppresses RE-arming
//!     during reconfirmation, never legitimate failover.

#![cfg(test)]

use std::time::{Duration, Instant};

use dynrunner_protocol_primary_secondary::address::PeerId;
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, DistributedMessage, PrimaryChangeReason,
};

use super::super::election::ElectionState;
use super::super::test_helpers::{election_config, make_secondary_membership};

const PRIMARY_ID: &str = "primary-orig";
const PEER_ID: &str = "sec-b";

/// `election_config` cadence: keepalive_interval = 50ms,
/// keepalive_miss_threshold = 2. The settle window is
/// ELECTION_SETTLE_KEEPALIVE_MULTIPLE (3) × 50ms = 150ms.
const KEEPALIVE_INTERVAL: Duration = Duration::from_millis(50);
const SETTLE_WINDOW: Duration = Duration::from_millis(150);

/// Bring a membership-controlled secondary into `Operational` with the
/// current primary applied, a live peer (so an armed election reaches
/// `Suspecting`, not a degraded bail — making a suppression assertion
/// honest), and one observed primary message (leg (C)'s
/// `primary_last_seen.is_some()` gate). Identical to
/// `election_terminal_latch_563::operational_with_seen_primary`.
fn operational_with_seen_primary() -> (
    super::super::test_helpers::SecondaryHarness<
        super::super::test_helpers::MembershipControlPeer,
    >,
    std::rc::Rc<std::cell::RefCell<Vec<PeerId>>>,
) {
    let (mut sec, members) = make_secondary_membership(
        election_config("sec-a"),
        vec![PeerId::from(PRIMARY_ID), PeerId::from(PEER_ID)],
    );
    sec.enter_operational_for_test();
    sec.op_mut()
        .peer_keepalives
        .insert(PEER_ID.into(), Instant::now());
    sec.cluster_state.apply(ClusterMutation::PrimaryChanged {
        new: PRIMARY_ID.into(),
        epoch: 1,
        reason: PrimaryChangeReason::Election,
    });
    sec.publish_membership();
    sec.record_primary_message();
    (sec, members)
}

/// Drop the primary out of the transport mesh — the leg (C) mesh-death
/// signal. After this, `mesh_says_dead` is true; whether the election arms
/// depends purely on the settle-window gate.
fn drop_primary_from_membership(
    sec: &mut super::super::test_helpers::SecondaryHarness<
        super::super::test_helpers::MembershipControlPeer,
    >,
    members: &std::rc::Rc<std::cell::RefCell<Vec<PeerId>>>,
) {
    members
        .borrow_mut()
        .retain(|id| id != &PeerId::from(PRIMARY_ID));
    sec.publish_membership();
}

/// SUPPRESSION: a `PrimaryChanged` applied within the settle window
/// suppresses arming on the SAME leg-(C) mesh-death input that would
/// otherwise arm. This is the amplifier cure — reconfirmation gets its
/// window before another election can fire.
#[tokio::test(flavor = "current_thread")]
async fn recent_primary_change_suppresses_arming() {
    let (mut sec, members) = operational_with_seen_primary();

    // A PrimaryChanged just applied: stamp the settle-window anchor at ~now
    // (the apply seam does this in production; here we set it directly to
    // isolate the gate from the apply machinery).
    sec.last_primary_change_at = Some(Instant::now());

    // The new primary's mesh leg drops — leg (C) `mesh_says_dead` is true.
    drop_primary_from_membership(&mut sec, &members);
    assert!(
        !sec.client.has_peer(&PeerId::from(PRIMARY_ID)),
        "fixture: leg (C) input — the primary has left the transport mesh",
    );

    let actions = sec.run_election_tick();

    assert!(
        matches!(sec.op_mut().election, ElectionState::Normal),
        "a PrimaryChanged within the settle window must suppress election \
         arming so MeshReady reconfirmation can complete; got {:?}",
        std::mem::discriminant(&sec.op_mut().election),
    );
    assert!(
        actions.broadcast.is_empty(),
        "no TimeoutQuery within the settle window; got {} broadcasts",
        actions.broadcast.len(),
    );
    assert!(!actions.promoted, "no self-promotion within the settle window");
    assert!(
        sec.fatal_exit.is_none(),
        "the settle window suppresses the election; it does NOT fatal-bail",
    );
}

/// THE LOAD-BEARING "DOES NOT BREAK FAILOVER" PROOF: a genuinely-dead
/// primary PAST the settle window STILL arms. Same mesh-death input as the
/// suppression test, but with `last_primary_change_at` backdated beyond the
/// window — the window suppresses RE-arming during reconfirmation, never a
/// legitimate failover of a dead new primary.
#[tokio::test(flavor = "current_thread")]
async fn dead_primary_past_settle_window_still_elects() {
    let (mut sec, members) = operational_with_seen_primary();

    // The last PrimaryChanged was applied LONGER ago than the settle window:
    // reconfirmation had its chance, and now the (new) primary is genuinely
    // dead. The window has expired.
    sec.last_primary_change_at =
        Some(Instant::now() - (SETTLE_WINDOW + Duration::from_millis(50)));

    drop_primary_from_membership(&mut sec, &members);

    let actions = sec.run_election_tick();
    assert!(
        matches!(sec.op_mut().election, ElectionState::Suspecting { .. }),
        "past the settle window a genuinely-dead primary must STILL arm the \
         election (the window does NOT disable legitimate failover); got {:?}",
        std::mem::discriminant(&sec.op_mut().election),
    );
    assert!(
        actions
            .broadcast
            .iter()
            .any(|m| matches!(m, DistributedMessage::TimeoutQuery { target: _, .. })),
        "the legitimate failover must still broadcast a TimeoutQuery",
    );
}

/// NO ANCHOR (cold start / pre-failover): with `last_primary_change_at`
/// never stamped, the window is closed and arming is governed by the
/// mesh-death legs alone — the first/legitimate failover is never delayed.
#[tokio::test(flavor = "current_thread")]
async fn no_prior_change_does_not_suppress() {
    let (mut sec, members) = operational_with_seen_primary();
    // operational_with_seen_primary applies a PrimaryChanged through
    // cluster_state.apply directly (not the apply seam), so the anchor is
    // unset here; assert that and confirm arming proceeds.
    sec.last_primary_change_at = None;

    drop_primary_from_membership(&mut sec, &members);

    let actions = sec.run_election_tick();
    assert!(
        matches!(sec.op_mut().election, ElectionState::Suspecting { .. }),
        "with no recent PrimaryChanged the settle window is closed and the \
         mesh-death legs arm normally; got {:?}",
        std::mem::discriminant(&sec.op_mut().election),
    );
    assert!(
        actions
            .broadcast
            .iter()
            .any(|m| matches!(m, DistributedMessage::TimeoutQuery { target: _, .. })),
    );
}

/// RAPID FLIP CASCADE: drives back-to-back applied `PrimaryChanged`s
/// through the REAL apply seam and asserts that each refreshes the window
/// so no election arms across the burst — the exact cascade shape the gate
/// dissolves. After the burst, advancing the anchor past the window lets a
/// genuinely-dead primary elect (the failover is only DEFERRED, never
/// disabled).
#[tokio::test(flavor = "current_thread")]
async fn rapid_primary_flips_suppressed_then_elect_after_window() {
    let (mut sec, members) = operational_with_seen_primary();
    // The current primary (`primary-orig`) is gone from the mesh — leg (C)
    // `mesh_says_dead` is TRUE on every tick below, so the ONLY thing that
    // can keep the election from arming is the settle window.
    drop_primary_from_membership(&mut sec, &members);
    assert!(
        !sec.client.has_peer(&PeerId::from(PRIMARY_ID)),
        "fixture: the (dead) current primary has left the mesh",
    );

    // Rapid epoch flips re-electing the SAME (dead) primary, each applied
    // via the real seam (apply_cluster_mutations → on_primary_identity_
    // advanced), which stamps the settle-window anchor at ~now. Epoch-LWW
    // makes each higher-epoch re-name a genuine `Applied` advance (not a
    // stale NoOp). This is the flapping-epoch cascade shape: mesh_says_dead
    // is continuously true, yet each fresh advance re-opens the window.
    for epoch in 2..=4u64 {
        sec.apply_cluster_mutations(vec![ClusterMutation::PrimaryChanged {
            new: PRIMARY_ID.into(),
            epoch,
            reason: PrimaryChangeReason::Election,
        }]);
        assert!(
            sec.last_primary_change_at.is_some(),
            "the apply seam must stamp the settle-window anchor",
        );
        let actions = sec.run_election_tick();
        assert!(
            matches!(sec.op_mut().election, ElectionState::Normal),
            "no election may arm during the rapid-flip settle window (epoch \
             {epoch}); got {:?}",
            std::mem::discriminant(&sec.op_mut().election),
        );
        assert!(
            actions.broadcast.is_empty(),
            "no TimeoutQuery during the rapid-flip settle window (epoch {epoch})",
        );
    }

    // The flips stop and the genuinely-dead primary (still `primary-orig`,
    // still off the mesh) stays silent: once the window expires, the
    // legitimate failover arms.
    sec.last_primary_change_at =
        Some(Instant::now() - (SETTLE_WINDOW + Duration::from_millis(50)));
    let actions = sec.run_election_tick();
    assert!(
        matches!(sec.op_mut().election, ElectionState::Suspecting { .. }),
        "after the flips stop and the window expires, a genuinely-dead primary \
         elects; got {:?}",
        std::mem::discriminant(&sec.op_mut().election),
    );
    assert!(
        actions
            .broadcast
            .iter()
            .any(|m| matches!(m, DistributedMessage::TimeoutQuery { target: _, .. })),
    );
    // Window is a small multiple of the keepalive interval — far below any
    // death backstop, so the deferral it imposes is bounded.
    assert!(
        SETTLE_WINDOW <= KEEPALIVE_INTERVAL * 4,
        "the settle window must stay a small keepalive multiple",
    );
}
