//! Sender-side role-convergence: a member's `Destination::Primary`
//! resolution must track its OWN mirror's `current_primary` with NO
//! independent cached copy, so the next primary-bound send re-points the
//! instant the mirror applies a `PrimaryChanged` (whether the apply
//! arrives via a live broadcast OR an anti-entropy snapshot heal).
//!
//! The production signature these pin (run_20260612_072807): a
//! secondary's confirmable reports kept landing at the OLD primary's host
//! for 258+ seconds after the primary role had relocated — its
//! `Destination::Primary` resolution stayed stale across the whole replay
//! window. A member whose role view lags minutes behind a `PrimaryChanged`
//! ALREADY IN ITS OWN MIRROR is a bug regardless of how the report
//! eventually reaches the authority. The egress resolver
//! (`resolve_destination`) reads `cluster_state.current_primary()` live on
//! EVERY send and every replay re-send, so once the mirror advances the
//! register the resolution converges within one send — no keepalive-period
//! staleness window, no cache to invalidate.

#![cfg(test)]

use dynrunner_protocol_primary_secondary::address::{PeerId, SendTarget, resolve_destination};
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, Destination, DistributedMessage, PrimaryChangeReason,
};

use super::super::test_helpers::{
    FakeWorkerFactory, TestId, election_config, make_secondary_membership_relay,
    make_secondary_recording,
};
use crate::cluster_state::ClusterState;

/// THIS node — the member whose resolution must converge.
const SELF_ID: &str = "secondary-9";
/// The original primary, reachable only at bootstrap; it relocates away.
const OLD_PRIMARY: &str = "secondary-0";
/// The new primary after relocation — reachable, distinct from self.
const NEW_PRIMARY: &str = "secondary-1";

/// The egress resolver reads recognition-identity LIVE: a `Destination::
/// Primary` resolves to whatever `current_primary` holds at the moment of
/// the call, with the bootstrap link as the only `None` fallback. There is
/// no cached send-target to go stale, so advancing `current_primary` is
/// the WHOLE re-point. Pins the no-cache property at the pure-function
/// boundary, independent of any coordinator wiring.
#[test]
fn resolution_tracks_current_primary_with_no_cache() {
    // Cold mirror: falls back to the bootstrap-dialled primary.
    assert_eq!(
        resolve_destination(Destination::Primary, None, Some(OLD_PRIMARY), SELF_ID),
        Some(SendTarget::Peer(PeerId::from(OLD_PRIMARY))),
        "a cold (None) register resolves to the bootstrap primary",
    );
    // Mirror warmed to the old primary: that is the holder.
    assert_eq!(
        resolve_destination(
            Destination::Primary,
            Some(OLD_PRIMARY),
            Some(OLD_PRIMARY),
            SELF_ID,
        ),
        Some(SendTarget::Peer(PeerId::from(OLD_PRIMARY))),
    );
    // Mirror advances to the NEW primary — the resolution moves WITH it in
    // the same call, no separate invalidation step. The bootstrap fallback
    // is never consulted while `current_primary` is `Some`.
    assert_eq!(
        resolve_destination(
            Destination::Primary,
            Some(NEW_PRIMARY),
            Some(OLD_PRIMARY),
            SELF_ID,
        ),
        Some(SendTarget::Peer(PeerId::from(NEW_PRIMARY))),
        "advancing current_primary re-points the resolution with no cache lag",
    );
}

/// End-to-end through the egress chokepoint: with the OLD primary
/// unroutable and the NEW primary reachable, a confirmable report sent
/// while the mirror still names the old primary is absorbed + retained for
/// replay (the no-route path) — but the INSTANT the mirror applies the
/// relocation `PrimaryChanged`, the next confirmable send AND the replay
/// drain both resolve to the NEW primary and deliver, with the failover-
/// health probe quiescing. A stale send-side resolution would keep landing
/// at the unroutable old host and re-arm the probe — the 258s production
/// symptom.
#[tokio::test(flavor = "current_thread")]
async fn confirmable_send_repoints_when_mirror_applies_primary_changed() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // The mesh holds both candidate primaries plus self as live
            // members; the relay path keeps every present id routable
            // unless explicitly declared unroutable.
            let (mut sec, connected, unroutable) = make_secondary_membership_relay(
                election_config(SELF_ID),
                vec![
                    PeerId::from(OLD_PRIMARY),
                    PeerId::from(NEW_PRIMARY),
                    PeerId::from(SELF_ID),
                ],
            );
            sec.enter_operational_for_test();

            // Phase 1 — the mirror names the OLD primary, which has gone
            // unroutable (it relocated away / died): dropped from the live
            // membership AND bounced off every relay forwarder, so no path
            // by any route. A confirmable report resolves to it, no-routes,
            // and is RETAINED for replay.
            sec.cluster_state.apply(ClusterMutation::PrimaryChanged {
                new: OLD_PRIMARY.into(),
                epoch: 1,
                reason: PrimaryChangeReason::Election,
            });
            connected
                .borrow_mut()
                .retain(|m| m != &PeerId::from(OLD_PRIMARY));
            unroutable.borrow_mut().push(PeerId::from(OLD_PRIMARY));
            sec.publish_membership();

            sec.report_deferred_task_lost(0, "task-a")
                .await
                .expect("no-route is absorbed into Ok (failover-B)");
            assert_eq!(
                sec.pending_report_replays.len(),
                1,
                "a confirmable report to the unroutable old primary is \
                 retained for replay",
            );
            assert!(
                sec.op_mut().primary_link.is_link_failing(),
                "resolving to the unroutable old primary arms the \
                 failover-health probe",
            );

            // The retained report's next replay slot is pushed FAR into the
            // future (the production 60s-capped backoff): without a prompt
            // re-drive on the relocation it would idle here for a full slot,
            // still no-routing the gone primary — the 258s production stall.
            sec.pending_report_replays[0].next_due =
                std::time::Instant::now() + std::time::Duration::from_secs(600);

            // Phase 2 — the relocation `PrimaryChanged` lands in THIS
            // member's mirror (a live broadcast or an AE snapshot heal —
            // the apply seam is identical). The OLD primary is gone; the NEW
            // primary is the routable holder.
            unroutable.borrow_mut().clear();
            sec.publish_membership();
            let advanced = sec.apply_cluster_mutations(vec![ClusterMutation::PrimaryChanged {
                new: NEW_PRIMARY.into(),
                epoch: 2,
                reason: PrimaryChangeReason::Election,
            }]);
            assert!(advanced, "the relocation PrimaryChanged advances the mirror");
            assert_eq!(
                sec.cluster_state.current_primary(),
                Some(NEW_PRIMARY),
                "the member's mirror now names the new primary",
            );

            // The identity-change REACTION (the one owner the receive arms
            // call) must re-drive the retained report NOW — overriding its
            // far-future backoff slot — so it re-resolves to the routable
            // new primary and lands within one reaction instead of waiting
            // out a backoff timed against the gone primary. This is the
            // convergence the lost-broadcast 258s stall lacked.
            sec.react_to_primary_identity_change().await;
            assert_eq!(
                sec.pending_report_replays.len(),
                1,
                "the retained report stays a single in-place entry across \
                 the re-drive",
            );
            assert!(
                sec.pending_report_replays[0].state.is_awaiting_ack(),
                "the identity-change reaction re-drove the retained report to \
                 the NEW primary (now awaiting-ack), overriding its backoff \
                 slot — not left no-routing the relocated-away old primary",
            );
            // The re-drive's send succeeded, so the entry's next slot is a
            // fresh ack-window from NOW — it is no longer pinned to the
            // far-future backoff it carried while no-routing the gone
            // primary (the 258s-stall slot is released).
            assert!(
                sec.pending_report_replays[0].next_due
                    < std::time::Instant::now() + std::time::Duration::from_secs(300),
                "the re-driven report's next slot is a fresh ack window, not \
                 the released far-future backoff slot",
            );
        })
        .await;
}

/// The late-mirror case (the actual production root): the relocation
/// `PrimaryChanged` broadcast never reached THIS member, so its mirror is
/// stuck naming the old primary (epoch 1) while the cluster moved to a new
/// primary (epoch 2). The member is genuinely BEHIND and its egress keeps
/// resolving `Destination::Primary` to the stale old holder — until the
/// anti-entropy digest exchange detects the divergence, pulls a snapshot,
/// and `restore()` advances the mirror's primary register. The resolution
/// re-points the instant that heal lands, because the egress reads
/// `current_primary()` live (no separate cached copy to invalidate). This
/// closes the convergence path the lost broadcast left open: a member's
/// role view never lags its OWN healed mirror.
#[tokio::test(flavor = "current_thread")]
async fn resolution_repoints_as_soon_as_anti_entropy_heals_a_stale_mirror() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // `make_secondary_recording` seeds the bootstrap primary as
            // `"setup"` (the routable folded primary in the recorded mesh).
            let (mut sec, _log) = make_secondary_recording(election_config(SELF_ID), 1);
            sec.set_bootstrap_primary_id("setup".to_string());
            sec.enter_operational_for_test();

            // The member's mirror is stuck at the OLD primary (epoch 1): it
            // missed the relocation broadcast.
            sec.cluster_state.apply(ClusterMutation::PrimaryChanged {
                new: "setup".into(),
                epoch: 1,
                reason: PrimaryChangeReason::Election,
            });
            // The egress resolves to the stale old primary — the symptom.
            assert_eq!(
                resolve_destination(
                    Destination::Primary,
                    sec.cluster_state.current_primary(),
                    Some("setup"),
                    SELF_ID,
                ),
                Some(SendTarget::Peer(PeerId::from("setup"))),
                "precondition: the stale mirror resolves to the OLD primary",
            );

            // A COMPLETE peer (donor) that DID apply the relocation: the
            // primary moved to NEW_PRIMARY at epoch 2.
            let mut donor = ClusterState::<TestId>::new();
            donor.apply(ClusterMutation::PrimaryChanged {
                new: "setup".into(),
                epoch: 1,
                reason: PrimaryChangeReason::Election,
            });
            donor.apply(ClusterMutation::PrimaryChanged {
                new: NEW_PRIMARY.into(),
                epoch: 2,
                reason: PrimaryChangeReason::Election,
            });

            // The replicas genuinely diverge on the primary register, so the
            // anti-entropy detector flags the stale member as behind.
            assert!(
                sec.cluster_state.digest().is_behind(&donor.digest()),
                "precondition: the stale-mirror member is behind on the \
                 primary register (higher epoch on the donor)",
            );

            // ── The AE round: the donor's digest arrives; the member pulls a
            // snapshot; the donor answers with its package stream (the SAME
            // plan + codec a production responder uses). ──
            let digest_frame = DistributedMessage::StateDigest {
                target: None,
                sender_id: "setup".into(),
                timestamp: 0.0,
                digest: donor.digest(),
                sender_is_observer: false,
            };
            sec.dispatch_message(digest_frame, &mut FakeWorkerFactory)
                .await
                .expect("StateDigest dispatch succeeds");
            sec.drain_egress().await;
            for reply in
                crate::snapshot_stream::stream_frames_for_test(&donor, "setup", "secondary-9/0")
            {
                sec.dispatch_message(reply, &mut FakeWorkerFactory)
                    .await
                    .expect("SnapshotStreamPackage dispatch succeeds");
            }

            // Healed: the restore advanced the primary register to the new
            // holder, and the egress resolution re-points to it in the same
            // breath (it reads `current_primary()` live).
            assert_eq!(
                sec.cluster_state.current_primary(),
                Some(NEW_PRIMARY),
                "the AE snapshot heal advanced the mirror's primary register",
            );
            assert_eq!(
                resolve_destination(
                    Destination::Primary,
                    sec.cluster_state.current_primary(),
                    Some("setup"),
                    SELF_ID,
                ),
                Some(SendTarget::Peer(PeerId::from(NEW_PRIMARY))),
                "the resolution re-points to the new primary the instant the \
                 AE heal lands — no separate cache, no staleness window past \
                 the mirror's own convergence",
            );
        })
        .await;
}
