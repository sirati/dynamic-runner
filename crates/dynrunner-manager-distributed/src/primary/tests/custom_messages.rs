//! F5 primary-side handler-dispatch decision: per-origin ordered
//! dispatch, the Handled latch origination, the promotion replay over an
//! inherited ledger, the poison cap, the no-handler consume, and the
//! ingest arm's droppable-vs-important routing.

use super::*;

use std::sync::{Arc, Mutex};

use dynrunner_protocol_primary_secondary::ClusterMutation;
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use crate::cluster_state::CustomMsgState;

type CallLog = Arc<Mutex<Vec<(String, u64, String, bool)>>>;

/// Install a recording handler that returns `outcomes[topic]` (default
/// `Ok`). The log records `(origin, parsed seq-from-data, topic,
/// important)` — the tests encode the seq into the payload so the
/// handler-visible order is directly assertable.
fn install_recording_handler<S, E>(
    primary: &mut PrimaryCoordinator<S, E, TestId>,
    fail_topics: Vec<String>,
) -> CallLog
where
    S: Scheduler<TestId>,
    E: ResourceEstimator<TestId>,
{
    let log: CallLog = Arc::new(Mutex::new(Vec::new()));
    let log2 = log.clone();
    primary.set_custom_message_handler(Box::new(move |origin, topic, data, important| {
        let seq: u64 = String::from_utf8_lossy(data).parse().unwrap_or(0);
        log2.lock()
            .unwrap()
            .push((origin.to_string(), seq, topic.to_string(), important));
        if fail_topics.iter().any(|t| t == topic) {
            Err(format!("handler refused topic {topic}"))
        } else {
            Ok(())
        }
    }));
    log
}

fn post<S, E>(primary: &mut PrimaryCoordinator<S, E, TestId>, origin: &str, seq: u64, topic: &str)
where
    S: Scheduler<TestId>,
    E: ResourceEstimator<TestId>,
{
    primary
        .cluster_state_mut_for_test()
        .apply(ClusterMutation::CustomMessagePosted {
            origin: origin.into(),
            seq,
            topic: topic.into(),
            data: seq.to_string().into_bytes(),
        });
}

/// The dispatch decision walks every `Unhandled` entry in `(origin,
/// seq)` order, invokes the handler once per message, latches each
/// `Handled` (payload dropped), and a re-dispatch invokes NOTHING — the
/// exactly-once consumer contract.
#[tokio::test(flavor = "current_thread")]
async fn dispatch_handles_unhandled_in_origin_seq_order_exactly_once() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
        let (transport, _ends) = setup_test(1);
        let (mut primary, _mesh) = build_test_primary(
            PrimaryConfig::default(),
            transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );
        let log = install_recording_handler(&mut primary, vec![]);
        // Posted out of order across two origins.
        post(&mut primary, "sec-1", 2, "batch");
        post(&mut primary, "sec-2", 1, "batch");
        post(&mut primary, "sec-1", 1, "batch");

        primary.dispatch_unhandled_custom_messages(&mut None).await;

        assert_eq!(
            log.lock().unwrap().clone(),
            vec![
                ("sec-1".to_string(), 1, "batch".to_string(), true),
                ("sec-1".to_string(), 2, "batch".to_string(), true),
                ("sec-2".to_string(), 1, "batch".to_string(), true),
            ],
            "handler order is the sorted (origin, seq) walk"
        );
        let cs = primary.cluster_state_mut_for_test();
        assert!(cs.unhandled_custom_messages().is_empty());
        // sec-1's contiguous 1..=2 prefix compacted; sec-2's seq 1 too.
        assert_eq!(cs.custom_handled_watermark("sec-1"), Some(2));
        assert_eq!(cs.custom_handled_watermark("sec-2"), Some(1));

        // Exactly once: a re-dispatch invokes nothing.
        primary.dispatch_unhandled_custom_messages(&mut None).await;
        assert_eq!(log.lock().unwrap().len(), 3);
        })
        .await;
}

/// PROMOTION REPLAY: a primary that died between the `Posted`
/// origination and the handler leaves `Unhandled` entries in every
/// replica; the promoted primary inherits them via its snapshot seed
/// and its dispatch replays each EXACTLY ONCE to the local handler.
/// Replays the production residue (snapshot-seeded ledger), not a live
/// re-landing.
#[tokio::test(flavor = "current_thread")]
async fn promotion_replay_dispatches_inherited_unhandled_exactly_once() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
        // The DYING primary: posted two messages, handled neither.
        let mut origin_state = crate::cluster_state::ClusterState::<TestId>::new();
        origin_state.apply(ClusterMutation::CustomMessagePosted {
            origin: "sec-1".into(),
            seq: 1,
            topic: "batch".into(),
            data: b"1".to_vec(),
        });
        origin_state.apply(ClusterMutation::CustomMessagePosted {
            origin: "sec-1".into(),
            seq: 2,
            topic: "batch".into(),
            data: b"2".to_vec(),
        });
        let snapshot = origin_state.snapshot();

        // The PROMOTED primary: seeded from the converged snapshot (the
        // production promotion input), handler installed pre-run.
        let (transport, _ends) = setup_test(1);
        let (mut promoted, _mesh) = build_test_primary(
            PrimaryConfig::default(),
            transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );
        let log = install_recording_handler(&mut promoted, vec![]);
        promoted.seed_from_promotion_snapshot(snapshot);

        // The replay trigger `run_pipeline`'s PromotedDestination arm fires
        // after hydrate.
        promoted.dispatch_unhandled_custom_messages(&mut None).await;
        assert_eq!(
            log.lock().unwrap().clone(),
            vec![
                ("sec-1".to_string(), 1, "batch".to_string(), true),
                ("sec-1".to_string(), 2, "batch".to_string(), true),
            ],
            "every inherited Unhandled entry replays to the promoted \
             primary's handler, in per-origin order"
        );
        promoted.dispatch_unhandled_custom_messages(&mut None).await;
        assert_eq!(
            log.lock().unwrap().len(),
            2,
            "the replay is exactly-once — the Handled latch suppresses redo"
        );
        })
        .await;
}

/// POISON CAP (F5-c): an always-raising handler is retried (the test
/// zeroes the backoff base so every dispatch pass is a due retry) and
/// after 5 consecutive raises the message is latched `Handled` anyway;
/// a per-origin successor is BLOCKED while the head is raising and
/// dispatches after the poison latch resolves it.
#[tokio::test(flavor = "current_thread")]
async fn poison_cap_latches_after_five_raises_and_unblocks_successors() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
        let (transport, _ends) = setup_test(1);
        let (mut primary, _mesh) = build_test_primary(
            PrimaryConfig::default(),
            transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );
        primary.custom_handler_backoff_base = std::time::Duration::ZERO;
        let log = install_recording_handler(&mut primary, vec!["poison".into()]);
        post(&mut primary, "sec-1", 1, "poison");
        post(&mut primary, "sec-1", 2, "fine");

        for pass in 1..=4 {
            primary.dispatch_unhandled_custom_messages(&mut None).await;
            let calls = log.lock().unwrap().clone();
            assert_eq!(
                calls.len(),
                pass,
                "pass {pass}: exactly one (head-of-origin) attempt per pass"
            );
            assert!(
                calls.iter().all(|(_, seq, _, _)| *seq == 1),
                "the raising head BLOCKS its per-origin successor (seq 2 \
                 must not overtake): {calls:?}"
            );
            assert!(
                primary
                    .cluster_state_mut_for_test()
                    .unhandled_custom_messages()
                    .iter()
                    .any(|(_, seq, _, _)| *seq == 1),
                "below the cap the message stays Unhandled"
            );
        }

        // The 5th raise trips the cap: the poison message latches Handled
        // UNCONSUMED and the successor dispatches in the same pass.
        primary.dispatch_unhandled_custom_messages(&mut None).await;
        let calls = log.lock().unwrap().clone();
        assert_eq!(
            calls.len(),
            6,
            "pass 5 = the capping attempt on seq 1 + the unblocked seq 2"
        );
        assert_eq!(calls[4], ("sec-1".to_string(), 1, "poison".to_string(), true));
        assert_eq!(calls[5], ("sec-1".to_string(), 2, "fine".to_string(), true));
        let cs = primary.cluster_state_mut_for_test();
        assert!(cs.unhandled_custom_messages().is_empty());
        assert_eq!(
            cs.custom_handled_watermark("sec-1"),
            Some(2),
            "the poison latch + the clean successor compact the prefix"
        );

        // Strikes were dropped at the cap: nothing left to retry.
        primary.dispatch_unhandled_custom_messages(&mut None).await;
        assert_eq!(log.lock().unwrap().len(), 6);
        })
        .await;
}

/// A backoff-deferred entry is NOT retried before its window elapses:
/// with a real (non-zero) backoff base, an immediate second dispatch
/// pass skips the raising head entirely.
#[tokio::test(flavor = "current_thread")]
async fn backoff_window_defers_the_retry() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
        let (transport, _ends) = setup_test(1);
        let (mut primary, _mesh) = build_test_primary(
            PrimaryConfig::default(),
            transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );
        primary.custom_handler_backoff_base = std::time::Duration::from_secs(3600);
        let log = install_recording_handler(&mut primary, vec!["poison".into()]);
        post(&mut primary, "sec-1", 1, "poison");

        primary.dispatch_unhandled_custom_messages(&mut None).await;
        primary.dispatch_unhandled_custom_messages(&mut None).await;
        assert_eq!(
            log.lock().unwrap().len(),
            1,
            "inside the backoff window the entry is not re-attempted"
        );
        })
        .await;
}

/// A consumer with NO handler: important messages are consumed
/// UNHANDLED (WARN + Handled latch) so the replicated inbox never grows
/// unboundedly on a hook-less consumer.
#[tokio::test(flavor = "current_thread")]
async fn no_handler_consumes_important_messages_unhandled() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
        let (transport, _ends) = setup_test(1);
        let (mut primary, _mesh) = build_test_primary(
            PrimaryConfig::default(),
            transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );
        post(&mut primary, "sec-1", 1, "batch");
        primary.dispatch_unhandled_custom_messages(&mut None).await;
        let cs = primary.cluster_state_mut_for_test();
        assert!(cs.unhandled_custom_messages().is_empty());
        assert_eq!(cs.custom_handled_watermark("sec-1"), Some(1));
        })
        .await;
}

/// The ingest arm routes by delivery class: a DROPPABLE landing
/// dispatches the handler directly (no CRDT residency); an IMPORTANT
/// landing posts to the inbox first and then runs the decision — and a
/// duplicate important landing (a transport replay) NoOps on the
/// `(origin, msg_seq)` key, never re-invoking the handler.
#[tokio::test(flavor = "current_thread")]
async fn ingest_routes_droppable_direct_and_important_via_inbox_with_dedup() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
        let (transport, _ends) = setup_test(1);
        let (mut primary, _mesh) = build_test_primary(
            PrimaryConfig::default(),
            transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );
        let log = install_recording_handler(&mut primary, vec![]);

        let frame = |msg_seq: u64, important: bool| DistributedMessage::<TestId>::CustomMessage {
            target: None,
            sender_id: "sec-1".into(),
            timestamp: 0.0,
            origin_secondary_id: "sec-1".into(),
            msg_seq,
            topic: "t".into(),
            data: msg_seq.to_string().into_bytes(),
            important,
            delivery_seq: if important { Some(7) } else { None },
        };

        // Droppable: direct dispatch, nothing CRDT-resident.
        primary.handle_custom_message(frame(1, false), &mut None).await;
        assert_eq!(
            log.lock().unwrap().clone(),
            vec![("sec-1".to_string(), 1, "t".to_string(), false)]
        );
        assert_eq!(
            primary
                .cluster_state_mut_for_test()
                .custom_message_state("sec-1", 1),
            None,
            "a droppable custom never touches the replicated inbox"
        );

        // Important: posted + handled.
        primary.handle_custom_message(frame(2, true), &mut None).await;
        assert_eq!(log.lock().unwrap().len(), 2);
        assert_eq!(
            primary
                .cluster_state_mut_for_test()
                .custom_message_state("sec-1", 2),
            Some(CustomMsgState::Handled)
        );

        // Duplicate important landing (transport replay): the Posted NoOps
        // on the latched key — the handler is NOT re-invoked.
        primary.handle_custom_message(frame(2, true), &mut None).await;
        assert_eq!(
            log.lock().unwrap().len(),
            2,
            "the (origin, msg_seq) idempotency key dedups the replay"
        );
        })
        .await;
}
