//! F5 secondary-side custom-message send seam + retention contract.
//!
//! Pins:
//!   * an IMPORTANT custom message sent through a no-route window is
//!     RETAINED (the #352 machinery, generalized) and re-delivered to a
//!     NEW primary once one is named — with the SAME `delivery_seq` AND
//!     the SAME `(origin, msg_seq)` idempotency key — and an
//!     observation of the OWN `CustomMessagePosted` in the local CRDT
//!     mirror releases it (#541: the retention-drop trigger split — an
//!     `AwaitingCrdtConvergence` important-custom IS NOT released by a
//!     `TerminalAck`, only by observing the broadcast come back; this
//!     forecloses the post-#539 hard-crash window where the primary
//!     dies between local apply of `CustomMessagePosted` and the mesh-
//!     pump's wire fan-out);
//!   * a DROPPABLE custom message is never retained: lost on the
//!     no-route window (the failover-loss-by-design negative), never
//!     `delivery_seq`-stamped on the healthy path;
//!   * the 100 KiB size gate rejects at the seam, naming size + limit,
//!     before any seq is burned or frame built.

#![cfg(test)]

use super::super::test_helpers::{
    FakeWorkerFactory, election_config, make_secondary_recording_with_membership,
};
use dynrunner_protocol_primary_secondary::{
    CUSTOM_MESSAGE_MAX_BYTES, ClusterMutation, DistributedMessage,
};

/// The custom-message frames in the recorded wire log, as
/// `(origin, msg_seq, important, delivery_seq)` tuples.
fn sent_customs(
    log: &std::rc::Rc<
        std::cell::RefCell<Vec<DistributedMessage<super::super::test_helpers::TestId>>>,
    >,
) -> Vec<(String, u64, bool, Option<u64>)> {
    log.borrow()
        .iter()
        .filter_map(|m| match m {
            DistributedMessage::CustomMessage {
                origin_secondary_id,
                msg_seq,
                important,
                delivery_seq,
                ..
            } => Some((
                origin_secondary_id.clone(),
                *msg_seq,
                *important,
                *delivery_seq,
            )),
            _ => None,
        })
        .collect()
}

/// THE F5 failover-safety repro: an important custom message sent while
/// NO primary is routable is retained, then re-delivered — same
/// `delivery_seq`, same `(origin, msg_seq)` — to the NEW primary the
/// role table names after failover; the new primary's `TerminalAck`
/// releases the retention.
#[tokio::test(flavor = "current_thread")]
async fn important_custom_through_no_route_window_redelivers_to_new_primary() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut secondary, log, membership) =
                make_secondary_recording_with_membership(election_config("sec-2"), 1);
            secondary.set_bootstrap_primary_id("setup".to_string());
            let mut factory = FakeWorkerFactory;
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;

            // ROUTE DOWN: the old primary "setup" leaves the mesh before
            // the consumer's send fires.
            membership.borrow_mut().retain(|id| id.as_str() != "setup");
            secondary.publish_membership();

            let send = secondary
                .send_custom_to_primary("phase4-batch".into(), b"batch-1".to_vec(), true)
                .await;
            assert!(
                send.is_ok(),
                "a no-route is absorbed (failover signal, not an error): {send:?}"
            );
            assert_eq!(
                secondary.pending_report_replays.len(),
                1,
                "the important custom must be RETAINED on the no-route absorb"
            );
            let retained_seq = secondary.pending_report_replays[0]
                .frame
                .delivery_seq()
                .expect("the chokepoint stamps delivery_seq on an important custom");
            assert!(
                sent_customs(&log).is_empty(),
                "nothing reached the wire while no primary was routable"
            );

            // FAILOVER: a NEW primary is named in the role table and joins
            // the mesh. The retention drain must re-resolve
            // `Destination::Primary` to it.
            secondary.cluster_state.apply(ClusterMutation::<
                super::super::test_helpers::TestId,
            >::PrimaryChanged {
                new: "new-primary".into(),
                epoch: 1,
                reason: Default::default(),
            });
            membership
                .borrow_mut()
                .push(dynrunner_protocol_primary_secondary::PeerId::from(
                    "new-primary",
                ));
            secondary.publish_membership();
            secondary.drain_report_replays().await;
            secondary.drain_egress().await;

            let sent = sent_customs(&log);
            assert_eq!(
                sent,
                vec![("sec-2".to_string(), 1, true, Some(retained_seq))],
                "re-delivered EXACTLY ONCE with the SAME (origin, msg_seq) \
                 idempotency key and the SAME delivery_seq — the only \
                 routable member is the NEW primary, so the frame landed \
                 there"
            );
            assert!(
                secondary.pending_report_replays[0]
                    .state
                    .is_awaiting_crdt_convergence(),
                "an IMPORTANT custom on a successful re-delivery enters \
                 AwaitingCrdtConvergence (#541), NOT AwaitingAck — its \
                 drop trigger is observing the own CustomMessagePosted in \
                 the local CRDT mirror, not the primary's TerminalAck"
            );

            // The post-#539 TerminalAck for an important-custom seq is a
            // NO-OP for retention release (#541): a TerminalAck-driven
            // drop would re-open the hard-crash window where the primary
            // dies between local apply and wire fan-out, stranding the
            // entry on its dead local CRDT only. The ack still flies on
            // the wire (operator-visible "primary acked" in logs), but
            // it does NOT touch this AwaitingCrdtConvergence entry.
            secondary
                .handle_inbound(
                    DistributedMessage::TerminalAck {
                        target: None,
                        sender_id: "new-primary".into(),
                        timestamp: 0.0,
                        seq: retained_seq,
                    },
                    &mut factory,
                )
                .await;
            assert_eq!(
                secondary.pending_report_replays.len(),
                1,
                "TerminalAck for an important-custom seq is a no-op for \
                 retention release (#541)"
            );
            assert!(
                secondary.pending_report_replays[0]
                    .state
                    .is_awaiting_crdt_convergence(),
                "the retention stays AwaitingCrdtConvergence post-ack — \
                 only the CRDT-convergence observation drops it"
            );

            // The CRDT-convergence drop trigger: a `ClusterMutation`
            // broadcast carrying `CustomMessagePosted` for this
            // secondary's own (origin, seq) arrives on the apply seam —
            // proof the primary's broadcast came back, so the entry is
            // durably in at least this replica's mirror AND the
            // broadcast actually fanned (the primary did not die
            // between local apply and the wire fan-out).
            secondary.apply_cluster_mutations(vec![
                ClusterMutation::<super::super::test_helpers::TestId>::CustomMessagePosted {
                    origin: "sec-2".into(),
                    seq: 1,
                    topic: "phase4-batch".into(),
                    data: b"batch-1".to_vec(),
                },
            ]);
            assert!(
                secondary.pending_report_replays.is_empty(),
                "observing the own CustomMessagePosted in the local CRDT \
                 mirror releases the AwaitingCrdtConvergence retention \
                 (#541 drop trigger)"
            );
        })
        .await;
}

/// THE #541 hard-crash repro: replays the exact sequence the bug
/// describes — a healthy send that gets a TerminalAck back from the
/// primary BEFORE any wire fan-out of `CustomMessagePosted` happens.
///
/// # Why this is the right repro shape
///
/// Post-#539 the primary's ack flies AFTER `apply_and_broadcast_
/// cluster_mutations(CustomMessagePosted)` returns. Inside that method,
/// `broadcast_applied_mutations` calls `MeshClient::send(All, frame)` —
/// a non-blocking mpsc enqueue to the mesh-pump's egress queue. The
/// pump is a separate `spawn_local` task, so the bytes have NOT
/// reached any peer when the function returns; the ack is sent from
/// the dispatch tail and goes out via the SAME pump. If the primary
/// dies in the window between the local apply and the wire fan-out of
/// the broadcast — but AFTER the ack frame was emitted toward this
/// secondary — the originating secondary sees the ack land at it
/// while no peer (including itself) ever observed the
/// `CustomMessagePosted` broadcast.
///
/// On the secondary side, that scenario is observationally identical
/// to: a successful first send produces an `AwaitingCrdtConvergence`
/// retention, then a `TerminalAck` arrives with NO `ClusterMutation`
/// carrying the matching `CustomMessagePosted` ever following. The
/// test replays exactly this on the secondary's seam.
///
/// # The two assertions
///
/// 1. The post-#539 `TerminalAck` for an important-custom seq is a
///    NO-OP for retention release (it does NOT drop the entry).
/// 2. The retention survives, so a subsequent failover + replay
///    drain re-delivers the message to the new primary — exactly the
///    recovery shape the #541 fix establishes (the new primary then
///    re-applies + re-broadcasts, the secondary's CRDT observes it,
///    retention drops).
///
/// Pre-fix (under #539 only — pre-#541) the `TerminalAck` would have
/// dropped the retention and a subsequent failover would have had
/// NOTHING to replay (the entry exists only on the dead primary's
/// CRDT, which is gone with it) — confirming this test fails on the
/// post-#539 / pre-#541 trunk by construction.
#[tokio::test(flavor = "current_thread")]
async fn important_custom_retains_through_ack_until_crdt_convergence_observed() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut secondary, log, _membership) =
                make_secondary_recording_with_membership(election_config("sec-2"), 1);
            secondary.set_bootstrap_primary_id("setup".to_string());
            let mut factory = FakeWorkerFactory;
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;

            // Healthy send: route is up, the chokepoint stamps a
            // delivery_seq and queues the frame; the Ok-side retention
            // enters AwaitingCrdtConvergence for an IMPORTANT custom.
            secondary
                .send_custom_to_primary("phase4-batch".into(), b"batch-1".to_vec(), true)
                .await
                .unwrap();
            secondary.drain_egress().await;

            assert_eq!(
                secondary.pending_report_replays.len(),
                1,
                "the important custom is retained pending CRDT-convergence \
                 observation"
            );
            let retained_seq = secondary.pending_report_replays[0]
                .frame
                .delivery_seq()
                .expect("the chokepoint stamps delivery_seq on important customs");
            assert!(
                secondary.pending_report_replays[0]
                    .state
                    .is_awaiting_crdt_convergence(),
                "an important-custom's Ok-side retention is \
                 AwaitingCrdtConvergence (#541), NOT AwaitingAck"
            );
            assert_eq!(
                sent_customs(&log),
                vec![("sec-2".to_string(), 1, true, Some(retained_seq))],
                "the important custom went out on the wire (the send was Ok)"
            );

            // THE #541 WINDOW: a TerminalAck for the important-custom
            // seq lands — the primary applied locally and sent its
            // post-#539 ack, but the broadcast either never fanned (the
            // primary died in the window) or has not arrived yet on
            // this secondary's mirror. Pre-fix this ack would drop the
            // retention; post-fix it is a no-op.
            secondary
                .handle_inbound(
                    DistributedMessage::TerminalAck {
                        target: None,
                        sender_id: "setup".into(),
                        timestamp: 0.0,
                        seq: retained_seq,
                    },
                    &mut factory,
                )
                .await;
            assert_eq!(
                secondary.pending_report_replays.len(),
                1,
                "TerminalAck for an important-custom seq is a no-op for \
                 retention release (#541) — pre-fix this assertion would \
                 fail (the entry would be gone)"
            );
            assert!(
                secondary.pending_report_replays[0]
                    .state
                    .is_awaiting_crdt_convergence(),
                "retention state unchanged by the ack"
            );

            // RECOVERY: the new primary (after failover) re-applies the
            // replayed message and broadcasts `CustomMessagePosted`. The
            // broadcast lands on this secondary's CRDT-apply seam — that
            // observation is the drop trigger.
            secondary.apply_cluster_mutations(vec![
                ClusterMutation::<super::super::test_helpers::TestId>::CustomMessagePosted {
                    origin: "sec-2".into(),
                    seq: 1,
                    topic: "phase4-batch".into(),
                    data: b"batch-1".to_vec(),
                },
            ]);
            assert!(
                secondary.pending_report_replays.is_empty(),
                "observing the own CustomMessagePosted in the local CRDT \
                 mirror releases the AwaitingCrdtConvergence retention"
            );
        })
        .await;
}

/// Negative-control covering two non-targets of the #541 drop trigger:
///
/// 1. A `CustomMessagePosted` for a DIFFERENT origin (a peer's
///    message) must NOT drop this secondary's own retentions — the
///    origin pre-filter keys precisely on `origin == self.id`.
/// 2. A `CustomMessagePosted` for THIS secondary's id but a DIFFERENT
///    seq must NOT drop the retention either — the seq match is exact
///    (the same precision discipline `ack_delivery` uses against the
///    shared monotonic counter).
///
/// Together these pin the drop trigger's identity discipline: only an
/// (origin, seq) pair this secondary itself stamped, observed coming
/// back via the CRDT broadcast, drops THAT specific retention.
#[tokio::test(flavor = "current_thread")]
async fn important_custom_retention_not_dropped_by_other_origin_or_other_seq() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut secondary, _log, _membership) =
                make_secondary_recording_with_membership(election_config("sec-2"), 1);
            secondary.set_bootstrap_primary_id("setup".to_string());
            let mut factory = FakeWorkerFactory;
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;

            secondary
                .send_custom_to_primary("own".into(), b"body".to_vec(), true)
                .await
                .unwrap();
            secondary.drain_egress().await;
            assert_eq!(secondary.pending_report_replays.len(), 1);
            let own_msg_seq: u64 = 1;

            // (1) A peer's CustomMessagePosted does NOT release this
            // secondary's retention — the origin id is different.
            secondary.apply_cluster_mutations(vec![
                ClusterMutation::<super::super::test_helpers::TestId>::CustomMessagePosted {
                    origin: "some-peer-secondary".into(),
                    seq: own_msg_seq,
                    topic: "peer-topic".into(),
                    data: b"peer-body".to_vec(),
                },
            ]);
            assert_eq!(
                secondary.pending_report_replays.len(),
                1,
                "a peer's CustomMessagePosted (different origin) must not \
                 drop this secondary's own retention"
            );

            // (2) A CustomMessagePosted with the right origin but a
            // DIFFERENT seq does not match (e.g. a future message this
            // secondary hasn't sent yet). The retention stays.
            secondary.apply_cluster_mutations(vec![
                ClusterMutation::<super::super::test_helpers::TestId>::CustomMessagePosted {
                    origin: "sec-2".into(),
                    seq: own_msg_seq + 42,
                    topic: "future".into(),
                    data: b"future-body".to_vec(),
                },
            ]);
            assert_eq!(
                secondary.pending_report_replays.len(),
                1,
                "a CustomMessagePosted for the right origin but the wrong \
                 seq must not drop the retention — the seq match is exact"
            );

            // Sanity: the exact (origin, seq) match DOES drop it.
            secondary.apply_cluster_mutations(vec![
                ClusterMutation::<super::super::test_helpers::TestId>::CustomMessagePosted {
                    origin: "sec-2".into(),
                    seq: own_msg_seq,
                    topic: "own".into(),
                    data: b"body".to_vec(),
                },
            ]);
            assert!(
                secondary.pending_report_replays.is_empty(),
                "the exact (origin, seq) match releases the retention"
            );
        })
        .await;
}

/// The droppable NEGATIVE: a droppable custom is never retained — sent
/// through the same no-route window it is simply LOST (at-most-once by
/// contract; lost on failover by design) — and on the healthy path it
/// rides the wire un-stamped (no `delivery_seq`, no retention entry).
#[tokio::test(flavor = "current_thread")]
async fn droppable_custom_is_lost_on_no_route_and_never_retained() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut secondary, log, membership) =
                make_secondary_recording_with_membership(election_config("sec-2"), 1);
            secondary.set_bootstrap_primary_id("setup".to_string());
            let mut factory = FakeWorkerFactory;
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;

            // No-route window: the droppable send is absorbed AND dropped.
            membership.borrow_mut().retain(|id| id.as_str() != "setup");
            secondary.publish_membership();
            secondary
                .send_custom_to_primary("progress".into(), b"p1".to_vec(), false)
                .await
                .unwrap();
            assert!(
                secondary.pending_report_replays.is_empty(),
                "a droppable custom is NEVER retained"
            );

            // Route recovery + drain: the lost droppable does NOT reappear.
            membership
                .borrow_mut()
                .push(dynrunner_protocol_primary_secondary::PeerId::from("setup"));
            secondary.publish_membership();
            secondary.drain_report_replays().await;
            secondary.drain_egress().await;
            assert!(
                sent_customs(&log).is_empty(),
                "the droppable sent into the no-route window is lost by design"
            );

            // Healthy path: the droppable rides the wire un-stamped AND
            // UNSEQUENCED (`msg_seq = 0` — droppables never occupy a slot
            // in the gate-counted important identity space, so a lost one
            // can never be awaited), and leaves no retention entry.
            secondary
                .send_custom_to_primary("progress".into(), b"p2".to_vec(), false)
                .await
                .unwrap();
            secondary.drain_egress().await;
            assert_eq!(
                sent_customs(&log),
                vec![("sec-2".to_string(), 0, false, None)],
                "droppable: delivered once, unsequenced (msg_seq 0), no \
                 delivery_seq stamp"
            );
            assert!(secondary.pending_report_replays.is_empty());
        })
        .await;
}

/// The size gate rejects an over-limit payload AT the seam, naming size
/// + limit, before any frame is built or seq burned.
#[tokio::test(flavor = "current_thread")]
async fn oversize_custom_is_rejected_naming_size_and_limit() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut secondary, log, _membership) =
                make_secondary_recording_with_membership(election_config("sec-2"), 1);
            secondary.set_bootstrap_primary_id("setup".to_string());
            let mut factory = FakeWorkerFactory;
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;

            let oversize = vec![0u8; CUSTOM_MESSAGE_MAX_BYTES + 1];
            let err = secondary
                .send_custom_to_primary("big".into(), oversize, true)
                .await
                .expect_err("an over-limit payload must be rejected");
            assert!(
                err.contains(&(CUSTOM_MESSAGE_MAX_BYTES + 1).to_string())
                    && err.contains(&CUSTOM_MESSAGE_MAX_BYTES.to_string()),
                "the rejection names size + limit: {err}"
            );
            assert!(secondary.pending_report_replays.is_empty());

            // The rejected send burned NO msg_seq: the next message is 1.
            secondary
                .send_custom_to_primary("ok".into(), b"fits".to_vec(), true)
                .await
                .unwrap();
            secondary.drain_egress().await;
            let sent = sent_customs(&log);
            assert_eq!(sent.len(), 1);
            assert_eq!(
                sent[0].1, 1,
                "the size rejection happens before the seq stamp"
            );
        })
        .await;
}
