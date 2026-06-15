//! #539 ordering invariant: an IMPORTANT custom-message landing's
//! `TerminalAck` MUST NOT precede its `CustomMessagePosted` CRDT
//! origination.
//!
//! # The wedge this test pins
//!
//! Pre-fix, `dispatch_message` echoed the `TerminalAck` for every
//! confirmable landing BEFORE the variant-specific handler ran — and
//! for an important [`DistributedMessage::CustomMessage`] the handler is
//! the only path that originates `ClusterMutation::CustomMessagePosted`.
//! The ack let the origin's retain buffer (the secondary's
//! `pending_report_replays`) drop the message the instant the ack
//! landed, while the entry was NOT yet in any cluster_state — local OR
//! remote. A primary that died in that window stranded the message
//! cluster-wide: no peer's snapshot pull recovered it, the origin no
//! longer had it to replay, and any task terminal stamped at that
//! origin with `msgs_posted_through >= seq` parked forever in the
//! terminal-ordering gate (`terminal_gate.rs:terminal_gate_admits`).
//! The relocated primary's operational loop then spun indefinitely on
//! `active_workers > 0` for the parked terminal's slot, RunComplete
//! never broadcast (#539 asm-tokenizer build-memmap 50-min hang).
//!
//! # What the fix establishes
//!
//! For an important custom-message landing the ack now lives in
//! `dispatch_message` AFTER `handle_custom_message` returns — past
//! `apply_and_broadcast(CustomMessagePosted)`'s local apply. So when
//! the ack hits the wire, the local CRDT carries the Unhandled entry,
//! and the CRDT's anti-entropy backstop guarantees any surviving peer
//! can converge on it. A primary that dies between the local apply and
//! the wire fan-out (the harder #541 crash-case follow-up) leaves the
//! entry on its local CRDT only — strictly narrower than the pre-fix
//! window which dropped the message everywhere.
//!
//! # What this test asserts
//!
//! Two complementary observations of the ORDER:
//!
//! 1. By the time `dispatch_message` returns, the primary's
//!    `cluster_state` records the `CustomMessagePosted` entry (so the
//!    ack — sent on return — is post-apply, exactly the ordering the
//!    fix establishes).
//! 2. Inspecting the wire frames the primary emitted during the call,
//!    the `CustomMessagePosted` broadcast precedes (or is at minimum
//!    co-emitted with) the `TerminalAck`.
//!
//! Pre-fix the FIRST assertion still happens to hold for a happy-path
//! handler return (the apply runs synchronously inside the same dispatch
//! frame), but the SECOND fails — the ack lands on the wire before the
//! `CustomMessagePosted` broadcast, which is the observable shape of
//! the pre-fix race. Post-fix both hold by construction.
//!
//! Sibling-of: [`super::terminal_ack`] pins the variant-agnostic
//! pre-handler ack for terminals + droppable customs (the
//! at-least-once-acked contract that is UNCHANGED by #539); this
//! module pins the one variant the fix singles out.

use super::*;

use dynrunner_protocol_primary_secondary::ClusterMutation;

/// Drain every frame currently reaching `rx` (the secondary's inbound
/// wire end), letting the spawned mesh-pump run, until it goes quiet.
async fn drain_frames(
    rx: &mut tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
) -> Vec<DistributedMessage<TestId>> {
    let mut frames = Vec::new();
    while let Ok(Some(frame)) =
        tokio::time::timeout(std::time::Duration::from_millis(200), rx.recv()).await
    {
        frames.push(frame);
    }
    frames
}

/// Position (in arrival order on the originating secondary's inbound
/// wire) of the first `TerminalAck` for `seq`, or `None`. The position
/// is intentionally a `usize` so the ordering compare against a sibling
/// position is total.
fn ack_position(frames: &[DistributedMessage<TestId>], seq: u64) -> Option<usize> {
    frames
        .iter()
        .position(|m| matches!(m, DistributedMessage::TerminalAck { seq: s, .. } if *s == seq))
}

/// Position of the first frame whose `ClusterMutation` batch contains a
/// `CustomMessagePosted` for `(origin, seq)`. The check walks the
/// arrival order so a co-emitted batch and a standalone ack are
/// directly comparable.
fn custom_posted_position(
    frames: &[DistributedMessage<TestId>],
    origin: &str,
    seq: u64,
) -> Option<usize> {
    frames.iter().position(|m| match m {
        DistributedMessage::ClusterMutation { mutations, .. } => mutations.iter().any(|mu| {
            matches!(
                mu,
                ClusterMutation::CustomMessagePosted {
                    origin: o,
                    seq: s,
                    ..
                } if o == origin && *s == seq
            )
        }),
        _ => false,
    })
}

/// THE repro for #539's ack-vs-CustomMessagePosted ordering invariant
/// (the orderly-relocation arm — see the harder crash-case follow-up
/// #541).
///
/// Steps:
/// 1. Build a single-secondary primary.
/// 2. Inject an important custom-message landing stamped with
///    `delivery_seq = 11`.
/// 3. Drive `dispatch_message` to completion.
/// 4. Assert: after the call, the primary's CRDT names the message
///    (the CustomMessagePosted has applied locally — proves the ack we
///    are about to observe on the wire was sent POST-APPLY).
/// 5. Assert: on the wire, the `CustomMessagePosted` broadcast
///    precedes the `TerminalAck`. This is the ORDERING shape the fix
///    establishes; pre-fix the ack rode ahead.
#[tokio::test(flavor = "current_thread")]
async fn important_custom_ack_follows_custom_message_posted_broadcast() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, mut ends) = setup_test(1);
            let (mut primary, _mesh) = build_test_primary(
                PrimaryConfig::default(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            // No handler is installed: the `invoke_custom_handler`
            // hook-less path returns Ok and an important message
            // latches Handled via the dispatch tail. The ORDERING the
            // test asserts is independent of that — `CustomMessagePosted`
            // is the FIRST mutation `handle_custom_message` originates,
            // before the dispatch decision walks the inbox — so the
            // pre-ack-post-Posted invariant holds whether the handler
            // is installed or not. Keeping the harness handler-less
            // narrows the test to the seam the fix touches.

            let important = DistributedMessage::CustomMessage::<TestId> {
                target: None,
                sender_id: "sec-0".into(),
                timestamp: 0.0,
                origin_secondary_id: "sec-0".into(),
                msg_seq: 1,
                topic: "phase-marker".into(),
                data: b"opaque-payload".to_vec(),
                important: true,
                is_high_volume: false,
                delivery_seq: Some(11),
            };

            primary
                .dispatch_message(important, &mut None)
                .await
                .unwrap();

            // Post-dispatch the local CRDT carries the Unhandled
            // entry — already compacted to Handled if the no-hook path
            // walked the inbox tail. Either way, the watermark covers
            // seq=1 (every dispatch trigger's tail releases the
            // terminal-ordering gate), which is the OPERATOR-visible
            // proof that the ack we observe on the wire was sent
            // post-`apply_and_broadcast(CustomMessagePosted)`: a
            // pre-apply ack would have collapsed both observations.
            let cs = primary.cluster_state_mut_for_test();
            assert_eq!(
                cs.custom_terminal_watermark("sec-0"),
                Some(1),
                "post-dispatch the watermark covers the message's seq — \
                 proves CustomMessagePosted applied (and the no-hook \
                 dispatch tail walked it Handled) before this call returned"
            );

            // Inspect what the primary actually emitted on the wire to
            // the originating secondary.
            let (_id, rx, _tx) = &mut ends[0];
            let frames = drain_frames(rx).await;

            let posted_pos = custom_posted_position(&frames, "sec-0", 1).expect(
                "primary must broadcast CustomMessagePosted for the important landing — \
                 the local apply has run, so the wire fan-out is in the same batch \
                 the local apply broadcasts (see apply_and_broadcast_cluster_mutations)",
            );
            let ack_pos = ack_position(&frames, 11).expect(
                "primary must send a TerminalAck for the stamped important landing — \
                 #539's fix shifts the ack POST-handler, not OFF",
            );

            assert!(
                posted_pos < ack_pos,
                "the CustomMessagePosted broadcast (pos={posted_pos}) must precede \
                 the TerminalAck (pos={ack_pos}) for seq=11 — the #539 ordering \
                 invariant. Pre-fix the ack rode ahead, letting the originator \
                 drop the message from its retain buffer before any cluster_state \
                 (local OR remote) recorded the Unhandled entry; a primary that \
                 died in that window stranded the message and parked every \
                 subsequent stamp-bearing terminal forever in the gate. \
                 Frames in arrival order: {frames:?}"
            );
        })
        .await;
}

/// Negative-control: a DROPPABLE custom-message landing is acked
/// PRE-handler (unchanged by #539 — the droppable contract has no
/// CRDT residue to lose). The droppable path through `handle_custom_message`
/// invokes the handler directly with NO `CustomMessagePosted` broadcast,
/// so there is nothing to defer the ack behind. Asserts both the ack
/// AND the absence of any `CustomMessagePosted` for the dropped key.
///
/// Sanity-check that the fix's `is_important_custom_message`
/// classifier does not accidentally route droppables through the
/// deferred-ack path (an over-broad gate would suppress acks for the
/// droppable class too, breaking the at-least-once-acked contract for
/// stamped droppables — see [`super::terminal_ack`]'s pre-handler ack
/// invariant).
#[tokio::test(flavor = "current_thread")]
async fn droppable_custom_ack_path_unchanged_by_539() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, mut ends) = setup_test(1);
            let (mut primary, _mesh) = build_test_primary(
                PrimaryConfig::default(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let droppable = DistributedMessage::CustomMessage::<TestId> {
                target: None,
                sender_id: "sec-0".into(),
                timestamp: 0.0,
                origin_secondary_id: "sec-0".into(),
                msg_seq: 0, // droppables are unsequenced
                topic: "transient".into(),
                data: b"oneshot".to_vec(),
                important: false,
                is_high_volume: false,
                // A droppable IS sometimes stamped with a delivery_seq
                // (the chokepoint stamps every confirmable; droppables
                // aren't confirmable so they shouldn't be — but a
                // hand-built test frame can still carry one. The point
                // here is that the variant-agnostic pre-ack still fires
                // because `is_important_custom_message` returns false).
                delivery_seq: Some(99),
            };
            primary
                .dispatch_message(droppable, &mut None)
                .await
                .unwrap();

            let (_id, rx, _tx) = &mut ends[0];
            let frames = drain_frames(rx).await;

            assert!(
                ack_position(&frames, 99).is_some(),
                "a droppable landing is acked through the pre-handler path \
                 (unchanged by #539); got {frames:?}"
            );
            assert!(
                custom_posted_position(&frames, "sec-0", 0).is_none(),
                "a droppable produces NO CustomMessagePosted broadcast — the \
                 droppable contract has no replicated residue; got {frames:?}"
            );
        })
        .await;
}
