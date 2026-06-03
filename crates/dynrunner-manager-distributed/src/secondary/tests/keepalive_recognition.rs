//! Keepalive recognition: a keepalive whose originator is the current
//! primary refreshes `primary_last_seen` (primary-liveness), while a
//! keepalive from any other peer feeds `peer_keepalives`. This is the
//! recognition half of the primary-liveness invariant — before it,
//! every keepalive was filed as a peer keepalive and primary liveness
//! was parasitic on the workload-dispatch path, so `primary_silent`
//! tripped the instant dispatch quiesced and never cleared.

#![cfg(test)]

use std::time::{Duration, Instant};

use super::super::test_helpers::{FakeWorkerFactory, TestId, election_config, make_secondary};
use dynrunner_protocol_primary_secondary::{DistributedMessage, KeepaliveRole};

/// Build a runtime `Keepalive` originated by `origin`. The wire shape
/// sets `sender_id == secondary_id == origin` for every emitter (the
/// primary's `broadcast_primary_keepalive` and the secondary's
/// `send_keepalive` both stamp their own `node_id` into both fields),
/// so recognition keys on that single originator identity.
fn keepalive(origin: &str) -> DistributedMessage<TestId> {
    DistributedMessage::Keepalive {
        sender_id: origin.into(),
        timestamp: 1.0,
        secondary_id: origin.into(),
        active_workers: 0,
        emitter_role: KeepaliveRole::Secondary,
    }
}

/// `PromotePrimary` naming `primary_id` as the cluster primary. Driving
/// the real apply path is the single source of `current_primary` (and is
/// what every other secondary test uses to install a primary identity).
fn promote(primary_id: &str) -> DistributedMessage<TestId> {
    DistributedMessage::PromotePrimary {
        sender_id: "promoter".into(),
        timestamp: 0.0,
        new_primary_id: primary_id.into(),
        epoch: 1,
        required_setup: false,
    }
}

/// (a) A keepalive from the CURRENT PRIMARY refreshes `primary_last_seen`
/// (advances it) and is NOT filed as a peer keepalive.
#[tokio::test(flavor = "current_thread")]
async fn primary_keepalive_refreshes_primary_last_seen() {
    let mut sec = make_secondary(election_config("sec-b"));
    sec.dispatch_message(promote("sec-a"), &mut FakeWorkerFactory)
        .await
        .expect("PromotePrimary handler succeeds");
    assert_eq!(sec.cluster_state.current_primary(), Some("sec-a"));

    // Stamp a known-stale baseline so "advanced" is observable: the
    // keepalive must move `primary_last_seen` strictly forward of it.
    // (`PromotePrimary` is itself a primary-facing frame whose dispatch
    // refreshes liveness, so we re-baseline rather than assume `None`.)
    let stale = Instant::now() - Duration::from_secs(60);
    sec.primary_last_seen = Some(stale);

    sec.handle_inbound(keepalive("sec-a"), &mut FakeWorkerFactory)
        .await;

    assert!(
        sec.primary_last_seen.expect("primary_last_seen set") > stale,
        "a keepalive from the current primary must advance primary_last_seen"
    );
    assert!(
        !sec.peer_keepalives.contains_key("sec-a"),
        "the primary's keepalive must NOT be filed as a peer keepalive"
    );
}

/// (b) A keepalive from a NON-PRIMARY peer lands in `peer_keepalives`
/// only and leaves `primary_last_seen` unchanged.
#[tokio::test(flavor = "current_thread")]
async fn peer_keepalive_does_not_touch_primary_last_seen() {
    let mut sec = make_secondary(election_config("sec-b"));
    sec.dispatch_message(promote("sec-a"), &mut FakeWorkerFactory)
        .await
        .expect("PromotePrimary handler succeeds");
    assert_eq!(sec.cluster_state.current_primary(), Some("sec-a"));

    let baseline = Instant::now() - Duration::from_secs(60);
    sec.primary_last_seen = Some(baseline);

    // sec-c is a regular peer, not the current primary.
    sec.handle_inbound(keepalive("sec-c"), &mut FakeWorkerFactory)
        .await;

    assert_eq!(
        sec.peer_keepalives.get("sec-c").copied(),
        Some(1.0),
        "a non-primary peer keepalive must be filed in peer_keepalives"
    );
    assert_eq!(
        sec.primary_last_seen,
        Some(baseline),
        "a non-primary peer keepalive must NOT touch primary_last_seen"
    );
}
