//! Keepalive EMISSION: a secondary's `send_keepalive` fans ONE keepalive
//! out to the whole mesh, reaching the primary EXACTLY ONCE (architecture
//! invariant #5). Post-fold the primary is a first-class mesh peer, so the
//! single `Destination::All` broadcast already reaches it — a separate
//! primary-unicast leg would double-deliver. The broadcast also fires when
//! the mesh is degraded (the primary is a member of the broadcast set
//! regardless of the role-aware degraded latch), so a degraded secondary
//! does not starve the primary of keepalives.
//!
//! This is the emission counterpart to [`super::keepalive_recognition`]
//! (which pins the inbound role-demux of a received keepalive).

#![cfg(test)]

use super::super::test_helpers::{election_config, make_secondary_recording};
use dynrunner_protocol_primary_secondary::DistributedMessage;

/// One `send_keepalive` emits EXACTLY ONE transport send. The
/// `RecordingPeer` records both `broadcast` and `send_to_peer` into the
/// same log, so the old double-send (primary-unicast + peer-broadcast)
/// would log TWO entries; the unified single fan-out logs exactly one.
#[tokio::test(flavor = "current_thread")]
async fn keepalive_is_emitted_exactly_once() {
    // peer_count = 1: a healthy (non-degraded) mesh.
    let (mut sec, log) = make_secondary_recording(election_config("sec-0"), 1);
    sec.enter_operational_for_test();
    sec.set_bootstrap_primary_id("primary".to_string());

    sec.send_keepalive().await;
    // Flush the queued keepalive fan-out onto the RecordingPeer log
    // (MeshClient::send is queued, drained by the pump).
    sec.drain_egress().await;

    let recorded = log.borrow();
    assert_eq!(
        recorded.len(),
        1,
        "send_keepalive must emit EXACTLY ONE transport send (one mesh fan-out, \
         not a primary-unicast + peer-broadcast double-send); got {recorded:?}"
    );
    assert!(
        matches!(recorded[0], DistributedMessage::Keepalive {
    target: _, .. }),
        "the single emitted frame must be the Keepalive"
    );
}

/// A DEGRADED mesh still emits the keepalive: the primary is a broadcast
/// member regardless of the role-aware degraded latch, so the old
/// degraded-mesh early-return (which skipped the broadcast and relied on a
/// now-deleted primary-unicast) would have starved the primary and tripped
/// false primary-death.
#[tokio::test(flavor = "current_thread")]
async fn keepalive_still_emitted_when_mesh_degraded() {
    let (mut sec, log) = make_secondary_recording(election_config("sec-0"), 0);
    sec.enter_operational_for_test();
    sec.set_bootstrap_primary_id("primary".to_string());
    sec.mesh.degraded = true;

    sec.send_keepalive().await;
    sec.drain_egress().await;

    let recorded = log.borrow();
    assert_eq!(
        recorded.len(),
        1,
        "a degraded mesh must still emit EXACTLY ONE keepalive fan-out so the \
         primary keeps seeing this secondary alive; got {recorded:?}"
    );
    assert!(matches!(recorded[0], DistributedMessage::Keepalive {
    target: _, .. }));
}
