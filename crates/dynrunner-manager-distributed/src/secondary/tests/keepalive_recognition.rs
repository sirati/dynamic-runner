//! Keepalive recognition by EMITTER ROLE: a `Primary`-tagged keepalive
//! whose originator is the current primary refreshes `primary_last_seen`
//! (primary-liveness), while a `Secondary`-tagged keepalive ALWAYS feeds
//! `peer_keepalives` (peer-mesh liveness) — even when its originator id is
//! the current primary, because a co-located primary+secondary host is a
//! live mesh peer in its own right and is tracked as BOTH. This is the
//! recognition half of the primary-liveness ≠ peer-liveness invariant:
//! the two signals no longer collide on one entry, so primary liveness is
//! not parasitic on workload dispatch and a co-located host is never
//! dropped from `peer_keepalives` (which would corrupt election quorum).

#![cfg(test)]

use std::time::{Duration, Instant};

use super::super::test_helpers::{FakeWorkerFactory, TestId, election_config, make_secondary};
use dynrunner_protocol_primary_secondary::{DistributedMessage, KeepaliveRole};

/// Build a runtime `Keepalive` originated by `origin`, tagged with the
/// emitter `role`. The wire shape sets `sender_id == secondary_id ==
/// origin` for every emitter (the primary's `broadcast_primary_keepalive`
/// and the secondary's `send_keepalive` both stamp their own `node_id`
/// into both fields), so recognition keys on that single originator
/// identity plus the emitter role tag.
fn keepalive(origin: &str, role: KeepaliveRole) -> DistributedMessage<TestId> {
    DistributedMessage::Keepalive {
        sender_id: origin.into(),
        timestamp: 1.0,
        secondary_id: origin.into(),
        active_workers: 0,
        emitter_role: role,
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

/// (a) A `Primary`-tagged keepalive from the CURRENT PRIMARY refreshes
/// `primary_last_seen` (advances it) and is NOT filed as a peer keepalive.
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

    sec.handle_inbound(keepalive("sec-a", KeepaliveRole::Primary), &mut FakeWorkerFactory)
        .await;

    assert!(
        sec.primary_last_seen.expect("primary_last_seen set") > stale,
        "a Primary keepalive from the current primary must advance primary_last_seen"
    );
    assert!(
        !sec.peer_keepalives.contains_key("sec-a"),
        "a Primary keepalive must NOT be filed as a peer keepalive"
    );
}

/// (b) A `Secondary`-tagged keepalive from a NON-PRIMARY peer lands in
/// `peer_keepalives` only and leaves `primary_last_seen` unchanged.
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
    sec.handle_inbound(keepalive("sec-c", KeepaliveRole::Secondary), &mut FakeWorkerFactory)
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

/// (c) A multi-role (co-located primary+secondary) host is tracked as
/// BOTH: its `Secondary`-tagged keepalive lands in `peer_keepalives` even
/// though its id IS the current primary, while a `Primary`-tagged
/// keepalive from the same id refreshes `primary_last_seen`. The two
/// liveness signals are independent — neither displaces the other.
#[tokio::test(flavor = "current_thread")]
async fn colocated_host_tracked_as_both_primary_and_peer() {
    let mut sec = make_secondary(election_config("sec-b"));
    sec.dispatch_message(promote("sec-a"), &mut FakeWorkerFactory)
        .await
        .expect("PromotePrimary handler succeeds");
    assert_eq!(sec.cluster_state.current_primary(), Some("sec-a"));

    let stale = Instant::now() - Duration::from_secs(60);
    sec.primary_last_seen = Some(stale);

    // The co-located host emits its secondary-capability keepalive: it is
    // a live mesh peer and MUST land in peer_keepalives despite its id
    // being the current primary.
    sec.handle_inbound(keepalive("sec-a", KeepaliveRole::Secondary), &mut FakeWorkerFactory)
        .await;
    assert_eq!(
        sec.peer_keepalives.get("sec-a").copied(),
        Some(1.0),
        "a Secondary keepalive from the primary's host MUST land in peer_keepalives \
         (multi-role host is a live mesh peer)"
    );
    assert_eq!(
        sec.primary_last_seen,
        Some(stale),
        "a Secondary keepalive must NOT refresh primary_last_seen, even from the primary's id"
    );

    // Its primary-capability keepalive refreshes primary_last_seen,
    // independently of the peer entry just recorded.
    sec.handle_inbound(keepalive("sec-a", KeepaliveRole::Primary), &mut FakeWorkerFactory)
        .await;
    assert!(
        sec.primary_last_seen.expect("primary_last_seen set") > stale,
        "a Primary keepalive from the current primary refreshes primary_last_seen"
    );
    assert!(
        sec.peer_keepalives.contains_key("sec-a"),
        "the earlier peer entry survives — the two liveness signals are independent"
    );

    // And the quorum view excludes the co-located primary from the live
    // peer set, so the peer entry never inflates election counts.
    assert!(
        sec.live_peer_ids().all(|id| id != "sec-a"),
        "live_peer_ids must exclude the current primary even though it has a peer_keepalives entry"
    );
}
