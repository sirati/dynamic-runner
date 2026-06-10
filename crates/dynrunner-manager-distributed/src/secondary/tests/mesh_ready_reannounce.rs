//! MeshReady re-announce on primary change — the pairwise-confirmation
//! contract (production: asm-tokenizer run_20260610_130116).
//!
//! "Mesh-leg confirmed" is PAIRWISE: a member's `MeshReady` proves its
//! egress leg delivers to the PRIMARY THAT RECEIVED IT. A
//! promoted/relocated primary starts with an EMPTY node-local
//! `mesh_ready_secondaries` set, and pre-fix the secondary's `MeshReady`
//! was one-shot PER PROCESS (`mesh.mesh_ready_sent` latched forever), so
//! already-operational members were structurally unrecoverable into the
//! new primary's confirmed set. The dispatch-readiness gate
//! (`member_mesh_confirmed`) then withheld them from every proactive
//! dispatch — production: 10 of 15 members at ZERO tasks while a 28-task
//! injected batch packed onto the ~5 stragglers whose reports landed
//! around the promotion window.
//!
//! The fix pinned here: the one-shot latch is per-PRIMARY-IDENTITY, not
//! per-process. When a secondary observes a genuinely-applied
//! `PrimaryChanged` (any reason — failover `Election` or relocation
//! `Transferred`, on either operational receive arm), it re-arms the
//! idempotent reporter and re-announces `MeshReady` to the NEW primary
//! (`Destination::Primary` re-resolves at the egress edge). The
//! primary-side `handle_mesh_ready` tolerates duplicates (unconditional
//! `HashSet::insert`), so the re-announce rides the existing
//! late-MeshReady recovery unchanged. A STALE `PrimaryChanged`
//! (lower-epoch NoOp) re-announces nothing — the identity did not move,
//! so the pair did not change.

#![cfg(test)]

use std::collections::HashMap;
use std::time::Instant;

use super::super::test_helpers::{
    FakeWorkerFactory, SecondaryHarness, TestId, election_config, make_secondary_channel,
};
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, DistributedMessage, PrimaryChangeReason,
};
use dynrunner_transport_channel::ChannelPeerTransport;
use tokio::sync::mpsc as tokio_mpsc;

/// Build an OPERATIONAL secondary whose channel mesh routes to the peer
/// that will be named the new primary, returning the harness plus that
/// peer's inbound receiver (so the test observes exactly the frames the
/// secondary's egress delivers to the NEW primary). The secondary's
/// shape models the production fleet at promotion time:
///   - peer mesh formed (`peer_keepalives` has the new-primary peer
///     alive, so the role-aware `alive_secondary_count()` reads 1),
///   - `MeshReady` ALREADY reported once — to the OLD primary, before
///     the change — so the one-shot latch is set (`mesh_ready_sent`).
fn operational_secondary_with_route_to(
    secondary_id: &str,
    new_primary_id: &str,
) -> (
    SecondaryHarness<ChannelPeerTransport<TestId>>,
    tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
) {
    let (to_new_primary_tx, new_primary_rx) = tokio_mpsc::unbounded_channel();
    // Inbound is never fed: these tests deliver frames via the direct
    // handler methods (`dispatch_message` / `handle_inbound`), the same
    // entry points the production pump demuxes onto.
    let (_never_tx, never_rx) = tokio_mpsc::unbounded_channel();
    let mut outgoing: HashMap<String, tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>> =
        HashMap::new();
    outgoing.insert(new_primary_id.to_string(), to_new_primary_tx);
    let transport =
        ChannelPeerTransport::from_raw_channels(secondary_id.into(), outgoing, never_rx);
    let mut secondary = make_secondary_channel(election_config(secondary_id), transport);
    // Cold-cache bootstrap fallback names the OLD primary; the tests
    // install the NEW identity through the real `PrimaryChanged` apply.
    secondary.set_bootstrap_primary_id("old-primary".to_string());
    secondary.enter_operational_for_test();
    // Peer mesh formed: the new-primary peer (promoted FROM a
    // secondary, so it is a live peer-secondary in global state) is
    // keepalive-fresh.
    secondary
        .op_mut()
        .peer_keepalives
        .insert(new_primary_id.to_string(), Instant::now());
    secondary.mesh.peer_dial_count = 1;
    // The one-shot report already fired — at the OLD primary, before
    // the role moved. This latch is exactly what made the confirmed
    // set unrecoverable on the new primary pre-fix.
    secondary.mesh.mesh_ready_sent = true;
    (secondary, new_primary_rx)
}

/// One `PrimaryChanged` mutation frame as broadcast on the wire.
fn primary_changed(
    new: &str,
    epoch: u64,
    reason: PrimaryChangeReason,
) -> DistributedMessage<TestId> {
    DistributedMessage::ClusterMutation {
        target: None,
        sender_id: "promoter".into(),
        timestamp: 0.0,
        mutations: vec![ClusterMutation::PrimaryChanged {
            new: new.into(),
            epoch,
            reason,
        }],
    }
}

/// Drain every `MeshReady` currently delivered to the new primary's
/// receive end, returning their `(secondary_id, peer_count)` pairs.
fn drain_mesh_ready(
    rx: &mut tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
) -> Vec<(String, u32)> {
    let mut out = Vec::new();
    while let Ok(msg) = rx.try_recv() {
        if let DistributedMessage::MeshReady {
            secondary_id,
            peer_count,
            ..
        } = msg
        {
            out.push((secondary_id, peer_count));
        }
    }
    out
}

/// FAILOVER edge (`reason = Election`), operational dispatch arm
/// (`dispatch_message` — the primary-link inbound path): a
/// genuinely-applied `PrimaryChanged` must re-announce `MeshReady` to
/// the NEW primary even though the one-shot latch was already set by
/// the report to the OLD primary. Pre-fix this is RED: the latch is
/// per-process, nothing re-arms it, and the new primary never hears the
/// member's confirmation — the production mechanism behind the ten
/// zero-task members.
#[tokio::test(flavor = "current_thread")]
async fn primary_change_via_dispatch_reannounces_mesh_ready() {
    let _ = tracing_subscriber::fmt::try_init();
    let (mut secondary, mut new_primary_rx) =
        operational_secondary_with_route_to("sec-a", "sec-new");

    secondary
        .dispatch_message(
            primary_changed("sec-new", 1, PrimaryChangeReason::Election),
            &mut FakeWorkerFactory,
        )
        .await
        .expect("PrimaryChanged dispatch succeeds");
    assert_eq!(
        secondary.cluster_state.current_primary(),
        Some("sec-new"),
        "precondition: the identity genuinely advanced"
    );
    secondary.drain_egress().await;

    let reports = drain_mesh_ready(&mut new_primary_rx);
    assert_eq!(
        reports,
        vec![("sec-a".to_string(), 1)],
        "a genuinely-applied PrimaryChanged must re-announce exactly one \
         MeshReady to the NEW primary (pairwise confirmation: the report \
         to the OLD primary proves nothing about this pair); peer_count \
         carries the live peer-secondary count"
    );
}

/// RELOCATION edge (`reason = Transferred`), peer-mesh relay arm
/// (`handle_inbound` — the arm a winner's broadcast or a relayed
/// mutation batch lands on): the SAME re-announce contract. Any primary
/// CHANGE re-announces; the reason discriminant is routing metadata for
/// the promotion build, never a re-announce filter.
#[tokio::test(flavor = "current_thread")]
async fn primary_change_via_peer_relay_reannounces_mesh_ready() {
    let _ = tracing_subscriber::fmt::try_init();
    let (mut secondary, mut new_primary_rx) =
        operational_secondary_with_route_to("sec-b", "sec-dest");

    secondary
        .handle_inbound(
            primary_changed("sec-dest", 1, PrimaryChangeReason::Transferred),
            &mut FakeWorkerFactory,
        )
        .await;
    assert_eq!(
        secondary.cluster_state.current_primary(),
        Some("sec-dest"),
        "precondition: the identity genuinely advanced"
    );
    secondary.drain_egress().await;

    let reports = drain_mesh_ready(&mut new_primary_rx);
    assert_eq!(
        reports,
        vec![("sec-b".to_string(), 1)],
        "a Transferred primary change must re-announce MeshReady to the \
         relocated-to primary exactly like the failover edge"
    );
}

/// Negative control: a STALE `PrimaryChanged` (lower epoch than the
/// installed identity — the apply NoOps) must NOT re-announce. The pair
/// (member ↔ current primary) did not change, so re-arming the one-shot
/// reporter would be evidence-free spam toward the unchanged primary.
#[tokio::test(flavor = "current_thread")]
async fn stale_primary_changed_does_not_reannounce() {
    let _ = tracing_subscriber::fmt::try_init();
    let (mut secondary, mut new_primary_rx) =
        operational_secondary_with_route_to("sec-c", "sec-new");

    // Install the CURRENT identity at epoch 2 (this legitimately
    // re-announces — drain it away to isolate the stale delivery).
    secondary
        .dispatch_message(
            primary_changed("sec-new", 2, PrimaryChangeReason::Election),
            &mut FakeWorkerFactory,
        )
        .await
        .expect("PrimaryChanged dispatch succeeds");
    secondary.drain_egress().await;
    while new_primary_rx.try_recv().is_ok() {}

    // A stale lower-epoch announcement naming somebody else NoOps.
    secondary
        .dispatch_message(
            primary_changed("sec-stale", 1, PrimaryChangeReason::Election),
            &mut FakeWorkerFactory,
        )
        .await
        .expect("stale PrimaryChanged dispatch still returns Ok");
    assert_eq!(
        secondary.cluster_state.current_primary(),
        Some("sec-new"),
        "precondition: the stale announcement must NOT move the identity"
    );
    secondary.drain_egress().await;

    assert!(
        drain_mesh_ready(&mut new_primary_rx).is_empty(),
        "a stale-epoch PrimaryChanged (apply NoOp) must NOT re-announce \
         MeshReady — the member↔primary pair did not change"
    );
}

/// Re-announce is gated on the REPORTABLE mesh state, exactly like the
/// initial report: a secondary whose peer-mesh watchdog is still
/// PENDING (deadline armed, zero alive peers — the bring-up window) has
/// nothing settled to report at primary-change time. The re-arm clears
/// the latch and the WATCHDOG later delivers the report to whoever then
/// holds the role (`Destination::Primary` re-resolves at egress) — the
/// bring-up announcement flow is unchanged, just re-pointed.
#[tokio::test(flavor = "current_thread")]
async fn primary_change_during_unsettled_mesh_defers_to_watchdog() {
    use std::time::Duration;
    let _ = tracing_subscriber::fmt::try_init();
    let (mut secondary, mut new_primary_rx) =
        operational_secondary_with_route_to("sec-d", "sec-new");
    // Unsettle the mesh: nobody alive yet, watchdog deadline still in
    // the future. (The latch stays set from the fixture — the stale
    // report to the old primary.)
    secondary.op_mut().peer_keepalives.clear();
    secondary.mesh.peer_mesh_check_at = Some(Instant::now() + Duration::from_secs(30));

    secondary
        .dispatch_message(
            primary_changed("sec-new", 1, PrimaryChangeReason::Election),
            &mut FakeWorkerFactory,
        )
        .await
        .expect("PrimaryChanged dispatch succeeds");
    secondary.drain_egress().await;
    assert!(
        drain_mesh_ready(&mut new_primary_rx).is_empty(),
        "an UNSETTLED mesh has nothing to report at primary-change time \
         (same reportable-state rules as the initial report)"
    );

    // The watchdog's deadline elapses with zero peers → degraded
    // terminal state → the (re-armed) reporter delivers MeshReady(0)
    // to the CURRENT primary. The re-arm must have cleared the stale
    // latch, otherwise this report is suppressed forever.
    secondary.mesh.peer_mesh_check_at = Some(Instant::now() - Duration::from_secs(1));
    secondary.check_peer_mesh_watchdog().await;
    secondary.drain_egress().await;
    assert_eq!(
        drain_mesh_ready(&mut new_primary_rx),
        vec![("sec-d".to_string(), 0)],
        "the watchdog's terminal report must reach the NEW primary — the \
         primary change re-armed the one-shot reporter"
    );
}
