//! F5 primary-side handler-dispatch decision: per-origin ordered
//! dispatch, the ATOMIC effect+Handled one-frame batch, the raise →
//! terminal `Failed` + discard-unexecuted contract, the promotion
//! replay over an inherited ledger (Unhandled ONLY — never Failed),
//! the no-handler consume, the ingest arm's droppable-vs-important
//! routing, and the keep-up backlog monitor's pure decision logic.

use super::*;

use std::sync::{Arc, Mutex};

use dynrunner_protocol_primary_secondary::ClusterMutation;
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use crate::cluster_state::CustomMsgState;
use crate::primary::command_channel::PrimaryCommand;

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
        assert_eq!(cs.custom_terminal_watermark("sec-1"), Some(2));
        assert_eq!(cs.custom_terminal_watermark("sec-2"), Some(1));

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

/// RAISE → terminal `Failed` (no retry, EVER — a handler raise is a
/// user error): the raising head resolves terminally in the same pass,
/// so its per-origin successor proceeds immediately (nothing blocks),
/// the payload is dropped, the watermark compacts over the `Failed`
/// tombstone exactly as over `Handled`, and a re-dispatch invokes
/// NOTHING (the handler ran exactly once per message).
#[tokio::test(flavor = "current_thread")]
async fn raise_fails_terminally_without_retry_and_successors_proceed() {
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
        let log = install_recording_handler(&mut primary, vec!["poison".into()]);
        post(&mut primary, "sec-1", 1, "poison");
        post(&mut primary, "sec-1", 2, "fine");

        primary.dispatch_unhandled_custom_messages(&mut None).await;
        let calls = log.lock().unwrap().clone();
        assert_eq!(
            calls,
            vec![
                ("sec-1".to_string(), 1, "poison".to_string(), true),
                ("sec-1".to_string(), 2, "fine".to_string(), true),
            ],
            "the raising head resolves terminally (Failed) in the same \
             pass, so the per-origin successor is handled right behind it"
        );
        let cs = primary.cluster_state_mut_for_test();
        assert!(cs.unhandled_custom_messages().is_empty());
        assert_eq!(
            cs.custom_terminal_watermark("sec-1"),
            Some(2),
            "the Failed tombstone + the clean successor compact the \
             prefix — the watermark GC covers both terminals"
        );

        // NO retry: a handler raise is terminal, every later dispatch
        // pass finds nothing Unhandled.
        primary.dispatch_unhandled_custom_messages(&mut None).await;
        primary.dispatch_unhandled_custom_messages(&mut None).await;
        assert_eq!(
            log.lock().unwrap().len(),
            2,
            "a raise is never retried — the handler ran exactly once per \
             message"
        );
        })
        .await;
}

/// A `Failed` entry is NEVER replayed on promotion: the promoted
/// primary's dispatch walks ONLY `Unhandled` entries. The inherited
/// ledger carries an uncompacted `Failed` (its seq sits behind a
/// not-yet-arrived gap, so the watermark cannot subsume it) — the
/// handler is still never invoked for it.
#[tokio::test(flavor = "current_thread")]
async fn promotion_replay_skips_failed_entries() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
        // The DYING primary's ledger: seq 2 posted and FAILED (seq 1
        // never arrived — the gap keeps the tombstone uncompacted, the
        // hardest shape: physically present, still not replayable).
        let mut origin_state = crate::cluster_state::ClusterState::<TestId>::new();
        origin_state.apply(ClusterMutation::CustomMessagePosted {
            origin: "sec-1".into(),
            seq: 2,
            topic: "batch".into(),
            data: b"2".to_vec(),
        });
        origin_state.apply(ClusterMutation::CustomMessageFailed {
            origin: "sec-1".into(),
            seq: 2,
            // This promotion-replay test inspects only that a Failed
            // tombstone is NOT replayed; the reason is irrelevant.
            reason: String::new(),
        });
        let snapshot = origin_state.snapshot();

        let (transport, _ends) = setup_test(1);
        let (mut promoted, _mesh) = build_test_primary(
            PrimaryConfig::default(),
            transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );
        let log = install_recording_handler(&mut promoted, vec![]);
        promoted.seed_from_promotion_snapshot(snapshot);

        assert_eq!(
            promoted
                .cluster_state_mut_for_test()
                .custom_message_state("sec-1", 2),
            Some(CustomMsgState::Failed),
            "the inherited Failed tombstone is physically present"
        );
        promoted.dispatch_unhandled_custom_messages(&mut None).await;
        assert!(
            log.lock().unwrap().is_empty(),
            "Failed is terminal — a promoted primary never re-dispatches it"
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
        assert_eq!(cs.custom_terminal_watermark("sec-1"), Some(1));
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

/// Shared fixture for the atomicity tests: a primary over a 1-secondary
/// channel transport, its ledger seeded with one task in phase `p1`
/// (so a handler-spawned sibling task validates), a live pool owning
/// `p1`, and a command channel whose sender the handler closure uses
/// for `PrimaryHandle`-style `spawn_tasks`. Returns the primary, the
/// recording peer's inbox, the live `command_rx`, and the binary the
/// handler will spawn.
#[allow(clippy::type_complexity)]
fn setup_spawning_handler_fixture(
    handler_raises: bool,
) -> (
    PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
    PrimaryMeshKeepalive,
    tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
    Option<tokio_mpsc::Receiver<PrimaryCommand<TestId>>>,
    TaskInfo<TestId>,
    tokio::sync::oneshot::Receiver<
        Result<Vec<(usize, crate::primary::command_channel::SpawnError)>, String>,
    >,
) {
    let (transport, mut ends) = setup_test(1);
    let (mut primary, mesh) = build_test_primary(
        PrimaryConfig::default(),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    // Ledger + pool own phase `p1` so the handler's spawn validates and
    // re-injects.
    let seeded = make_phased_typed_binary("seeded", "p1", "t", 100);
    seed_operational_ledger(&mut primary, vec![seeded], HashMap::new());
    let mut phases = std::collections::HashSet::new();
    phases.insert(dynrunner_core::PhaseId::from("p1"));
    primary.pending = Some(
        dynrunner_scheduler_api::PendingPool::new(phases, HashMap::new()).expect("pool init"),
    );

    // The handler queues ONE spawn_tasks through the command channel —
    // the in-runtime PrimaryHandle shape (queue + fire-and-forget) —
    // then returns per `handler_raises`.
    let spawned = make_phased_typed_binary("handler_spawned", "p1", "t", 100);
    let (command_tx, command_rx) = tokio_mpsc::channel(8);
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    let reply_slot = Arc::new(Mutex::new(Some(reply_tx)));
    let spawned_for_handler = spawned.clone();
    primary.set_custom_message_handler(Box::new(move |_origin, _topic, _data, _important| {
        let reply = reply_slot
            .lock()
            .unwrap()
            .take()
            .expect("handler fired once");
        command_tx
            .try_send(PrimaryCommand::SpawnTasks {
                tasks: vec![spawned_for_handler.clone()],
                reply,
            })
            .expect("queue spawn");
        if handler_raises {
            Err("consumer handler exploded".into())
        } else {
            Ok(())
        }
    }));

    let (_id, to_sec_rx, _to_pri_tx) = ends.remove(0);
    (primary, mesh, to_sec_rx, Some(command_rx), spawned, reply_rx)
}

/// Drain every `ClusterMutation` frame the recording peer received.
fn drain_mutation_frames(
    rx: &mut tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
) -> Vec<Vec<ClusterMutation<TestId>>> {
    let mut frames = Vec::new();
    while let Ok(msg) = rx.try_recv() {
        if let DistributedMessage::ClusterMutation { mutations, .. } = msg {
            frames.push(mutations);
        }
    }
    frames
}

/// ATOMICITY, the effect→terminal direction: the handler's effect
/// mutations and `CustomMessageHandled` ride ONE broadcast frame —
/// effects first, the terminal LAST — and no other frame carries the
/// effect. Every replica's batch apply is synchronous, so the cluster
/// can never observe the effect without the terminal (or the terminal
/// without the effect) from this origination.
///
/// This same fixture IS the mid-handler-death replay shape: the
/// pre-state (`Unhandled`, no effect anywhere) is exactly what a
/// primary that died mid-handler leaves in every replica, and this
/// dispatch is the promoted primary's replay producing the
/// effect+terminal batch.
#[tokio::test(flavor = "current_thread")]
async fn handler_effect_and_handled_terminal_ride_one_frame() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut primary, _mesh, mut to_sec_rx, mut command_rx, spawned, _reply_rx) =
                setup_spawning_handler_fixture(false);
            post(&mut primary, "sec-0", 1, "spawn");

            primary
                .dispatch_unhandled_custom_messages(&mut command_rx)
                .await;
            settle_pump().await;

            let frames = drain_mutation_frames(&mut to_sec_rx);
            let with_terminal: Vec<_> = frames
                .iter()
                .filter(|f| {
                    f.iter()
                        .any(|m| matches!(m, ClusterMutation::CustomMessageHandled { .. }))
                })
                .collect();
            assert_eq!(
                with_terminal.len(),
                1,
                "exactly one frame carries the Handled terminal: {frames:?}"
            );
            let frame = with_terminal[0];
            let spawn_idx = frame
                .iter()
                .position(|m| matches!(m, ClusterMutation::TasksSpawned { .. }))
                .expect("the handler's spawn effect rides the SAME frame as the terminal");
            let terminal_idx = frame
                .iter()
                .position(|m| matches!(m, ClusterMutation::CustomMessageHandled { .. }))
                .unwrap();
            assert!(
                spawn_idx < terminal_idx,
                "effect mutations precede the terminal fact in the frame"
            );
            assert_eq!(
                terminal_idx,
                frame.len() - 1,
                "the terminal is the LAST mutation in the batch"
            );
            // The effect never leaks into any OTHER frame.
            assert!(
                frames
                    .iter()
                    .filter(|f| !std::ptr::eq(*f, frame))
                    .all(|f| !f
                        .iter()
                        .any(|m| matches!(m, ClusterMutation::TasksSpawned { .. }))),
                "no other frame carries the spawn effect: {frames:?}"
            );

            // Local side: the spawned task landed Pending (the capture
            // only diverts the wire leg — local semantics unchanged).
            let spawned_hash = crate::primary::wire::compute_task_hash(&spawned);
            assert!(
                matches!(
                    primary.cluster_state_mut_for_test().task_state(&spawned_hash),
                    Some(crate::cluster_state::TaskState::Pending { .. })
                ),
                "the handler's spawn applied locally"
            );
        })
        .await;
}

/// DISCARD ON RAISE, the no-partial-effect direction: a raising
/// handler's queued `spawn_tasks` is discarded UNEXECUTED — nothing in
/// the ledger, nothing in the pool, nothing on the wire (the
/// `CustomMessageFailed` terminal is originated ALONE) — and the
/// discarded command's reply oneshot receives an explicit rejection.
/// No replay either: the raise is terminal.
#[tokio::test(flavor = "current_thread")]
async fn raise_discards_queued_effect_unexecuted_and_fails_alone() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut primary, _mesh, mut to_sec_rx, mut command_rx, spawned, mut reply_rx) =
                setup_spawning_handler_fixture(true);
            post(&mut primary, "sec-0", 1, "spawn");

            primary
                .dispatch_unhandled_custom_messages(&mut command_rx)
                .await;
            settle_pump().await;

            // The discarded effect landed NOWHERE: not in the ledger…
            let spawned_hash = crate::primary::wire::compute_task_hash(&spawned);
            assert!(
                primary
                    .cluster_state_mut_for_test()
                    .task_state(&spawned_hash)
                    .is_none(),
                "a raising handler's spawn must never reach the CRDT"
            );
            // …not on the wire (the terminal travels ALONE; no frame
            // ever carries the spawn)…
            let frames = drain_mutation_frames(&mut to_sec_rx);
            assert!(
                frames
                    .iter()
                    .all(|f| !f
                        .iter()
                        .any(|m| matches!(m, ClusterMutation::TasksSpawned { .. }))),
                "a raising handler's effect must never be broadcast: {frames:?}"
            );
            let failed_frames: Vec<_> = frames
                .iter()
                .filter(|f| {
                    f.iter()
                        .any(|m| matches!(m, ClusterMutation::CustomMessageFailed { .. }))
                })
                .collect();
            assert_eq!(failed_frames.len(), 1, "the Failed terminal broadcast");
            assert_eq!(
                failed_frames[0].len(),
                1,
                "CustomMessageFailed is originated ALONE, never batched \
                 with effect mutations: {failed_frames:?}"
            );
            // …and the blocked replier learned of the discard.
            match reply_rx.try_recv() {
                Ok(Err(reason)) => assert!(
                    reason.contains("discarded"),
                    "the rejection names the discard: {reason}"
                ),
                other => panic!("expected an explicit rejection, got {other:?}"),
            }

            // Terminal — no replay on a later dispatch pass.
            primary
                .dispatch_unhandled_custom_messages(&mut command_rx)
                .await;
            assert!(
                primary
                    .cluster_state_mut_for_test()
                    .unhandled_custom_messages()
                    .is_empty(),
                "the raise is terminal; nothing is owed a replay"
            );
        })
        .await;
}

/// Keep-up monitor decision logic (pure): growth across consecutive
/// observations fires; a stable backlog does not (until the oldest-age
/// threshold); the rate limit suppresses a second WARN inside the
/// window and re-arms after it; an emptied backlog resets cleanly.
#[test]
fn backlog_monitor_warn_decision() {
    use crate::primary::custom_message::{
        CUSTOM_BACKLOG_OLDEST_WARN, CUSTOM_BACKLOG_WARN_INTERVAL, CustomBacklogMonitor,
    };
    use std::time::{Duration, Instant};

    let key = |n: u64| ("sec-1".to_string(), n);
    let mut monitor = CustomBacklogMonitor::default();
    let t0 = Instant::now();

    // Tick 1: backlog grew 0 → 2 ⇒ WARN with both fields.
    let report = monitor.observe(&[key(1), key(2)], t0).expect("growth warns");
    assert_eq!(report.count, 2);
    assert_eq!(report.oldest_age, Duration::ZERO);

    // Tick 2 (1s later): grew again, but inside the rate-limit window
    // ⇒ suppressed.
    assert!(
        monitor
            .observe(&[key(1), key(2), key(3)], t0 + Duration::from_secs(1))
            .is_none(),
        "the once-per-interval rate limit holds"
    );

    // Tick 3 (past the rate window): stable count, oldest entry now
    // past the age threshold ⇒ WARN with the accrued age.
    let t3 = t0 + CUSTOM_BACKLOG_WARN_INTERVAL + Duration::from_secs(5);
    let report = monitor
        .observe(&[key(1), key(2), key(3)], t3)
        .expect("oldest-age breach warns");
    assert_eq!(report.count, 3);
    assert!(report.oldest_age > CUSTOM_BACKLOG_OLDEST_WARN);

    // Tick 4: backlog fully resolved ⇒ silent, state reset.
    let t4 = t3 + CUSTOM_BACKLOG_WARN_INTERVAL + Duration::from_secs(1);
    assert!(monitor.observe(&[], t4).is_none(), "empty backlog is silent");

    // Tick 5: a stable RE-OBSERVED single entry (no growth vs… the
    // reset zero — one fresh entry IS growth 0 → 1) warns again only
    // because it grew; its age restarts from first sight.
    let report = monitor
        .observe(&[key(9)], t4 + Duration::from_secs(1))
        .expect("regrowth after reset warns");
    assert_eq!(report.count, 1);
    assert_eq!(report.oldest_age, Duration::ZERO);

    // Tick 6 (30s later): stable count, age 30s — neither growth nor
    // the age threshold trips ⇒ silent regardless of the rate limit.
    assert!(
        monitor
            .observe(&[key(9)], t4 + Duration::from_secs(31))
            .is_none(),
        "a stable young backlog does not warn"
    );
}
