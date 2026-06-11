//! F5 secondary-side custom-message send seam + retention contract.
//!
//! Pins:
//!   * an IMPORTANT custom message sent through a no-route window is
//!     RETAINED (the #352 machinery, generalized) and re-delivered to a
//!     NEW primary once one is named — with the SAME `delivery_seq` AND
//!     the SAME `(origin, msg_seq)` idempotency key — and the
//!     `TerminalAck` releases it;
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
                secondary.pending_report_replays[0].state.is_awaiting_ack(),
                "the re-delivered custom stays retained AWAITING ACK \
                 (transport Ok proves nothing on a blackholed leg)"
            );

            // The new primary's per-landing ack is the only drop site.
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
            assert!(
                secondary.pending_report_replays.is_empty(),
                "the TerminalAck releases the retention"
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
