//! Primary-side half of the #352 app-level delivery confirmation: the
//! ingest echoes a `TerminalAck { seq }` to the report's ORIGINATING
//! secondary for EVERY `delivery_seq`-stamped terminal landing —
//! including landings the hash-keyed dedup gate drops (a duplicate
//! means the original ack was lost or the replay raced it; not
//! re-acking would replay forever) — while the side effects stay
//! exactly-once (the proven hash-keyed terminal idempotence; no
//! per-secondary seq state is kept, so a freshly-promoted primary acks
//! replays with zero handoff). An unstamped (pre-field) landing gets
//! no ack and keeps the pre-#352 behaviour.

use super::*;

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

fn ack_seqs(frames: &[DistributedMessage<TestId>]) -> Vec<u64> {
    frames
        .iter()
        .filter_map(|m| match m {
            DistributedMessage::TerminalAck { seq, .. } => Some(*seq),
            _ => None,
        })
        .collect()
}

fn task_completed_mutations(frames: &[DistributedMessage<TestId>], hash: &str) -> usize {
    frames
        .iter()
        .filter_map(|m| match m {
            DistributedMessage::ClusterMutation { mutations, .. } => Some(mutations),
            _ => None,
        })
        .flatten()
        .filter(|mu| matches!(mu, ClusterMutation::TaskCompleted { hash: h, .. } if h == hash))
        .count()
}

/// Duplicate stamped landings (the original + a blackhole-suspected
/// replay carrying the SAME seq) each get an ack, while the completion
/// side effects fire exactly once — the dedup gate and the ack emit are
/// independent, which is precisely what lets a lost ack be recovered by
/// the replay without double-counting the task.
#[tokio::test(flavor = "current_thread")]
async fn stamped_terminal_landings_are_acked_per_landing_with_exactly_once_side_effects() {
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
            // Seed the replicated ledger with the task so the first
            // landing's `TaskCompleted` origination genuinely APPLIES
            // (and broadcasts); the duplicate's dedup-drop is then
            // observable as "still exactly one broadcast".
            let task = make_binary("ack-task", 100);
            let hash = crate::primary::wire::compute_task_hash(&task);
            seed_operational_ledger(&mut primary, vec![task], HashMap::new());

            let stamped = DistributedMessage::TaskComplete {
                target: None,
                sender_id: "sec-0".into(),
                timestamp: 0.0,
                secondary_id: "sec-0".into(),
                worker_id: 0,
                task_hash: hash.clone(),
                result_data: None,
                delivery_seq: Some(7),
            };
            // Original landing …
            primary
                .dispatch_message(stamped.clone(), &mut None)
                .await
                .unwrap();
            // … and the replayed duplicate (same seq — the reporter's
            // ack-timeout fired because the first ack was lost).
            primary.dispatch_message(stamped, &mut None).await.unwrap();

            let (_id, rx, _tx) = &mut ends[0];
            let frames = drain_frames(rx).await;

            assert_eq!(
                ack_seqs(&frames),
                vec![7, 7],
                "EVERY stamped landing must be acked — including the \
                 dedup-dropped duplicate (a lost ack is only recoverable \
                 if the replay's landing is re-acked); got {frames:?}"
            );
            assert!(
                primary.completed_tasks.contains(&hash),
                "the completion landed"
            );
            assert_eq!(
                task_completed_mutations(&frames, &hash),
                1,
                "the terminal side effects (CRDT origination) fire exactly \
                 once across duplicate landings — the proven hash-keyed \
                 idempotence is the dedup, not seq state"
            );
        })
        .await;
}

/// An UNSTAMPED terminal landing (a pre-field sender) gets no ack —
/// wire-additive backcompat: the old sender keeps its pre-#352
/// no-route-only replay behaviour and is never confused by acks it
/// would not understand.
#[tokio::test(flavor = "current_thread")]
async fn unstamped_terminal_landing_is_not_acked() {
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

            let unstamped = DistributedMessage::TaskComplete {
                target: None,
                sender_id: "sec-0".into(),
                timestamp: 0.0,
                secondary_id: "sec-0".into(),
                worker_id: 0,
                task_hash: "legacy-hash".into(),
                result_data: None,
                delivery_seq: None,
            };
            primary
                .dispatch_message(unstamped, &mut None)
                .await
                .unwrap();

            let (_id, rx, _tx) = &mut ends[0];
            let frames = drain_frames(rx).await;
            assert!(
                ack_seqs(&frames).is_empty(),
                "an unstamped (pre-field) landing must not be acked; got {frames:?}"
            );
            assert!(
                primary.completed_tasks.contains("legacy-hash"),
                "the legacy landing is still processed normally"
            );
        })
        .await;
}

/// A stamped `TaskFailed` landing — including the backpressure shape,
/// which the handler REQUEUES rather than terminally records — is acked
/// too: the reporting secondary retains every terminal-bearing frame
/// (the backpressure-shaped deferred-lost reinject included), so every
/// such landing needs its delivery confirmed or the reporter replays a
/// requeue signal forever.
#[tokio::test(flavor = "current_thread")]
async fn stamped_backpressure_task_failed_landing_is_acked() {
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

            let backpressure = DistributedMessage::TaskFailed {
                target: None,
                sender_id: "sec-0".into(),
                timestamp: 0.0,
                secondary_id: "sec-0".into(),
                worker_id: 0,
                task_hash: "bp-hash".into(),
                error_type: dynrunner_core::ErrorType::Recoverable,
                error_message: "worker pipe broken; respawning".into(),
                delivery_seq: Some(3),
            };
            primary
                .dispatch_message(backpressure, &mut None)
                .await
                .unwrap();

            let (_id, rx, _tx) = &mut ends[0];
            let frames = drain_frames(rx).await;
            assert_eq!(
                ack_seqs(&frames),
                vec![3],
                "a stamped backpressure-shaped TaskFailed landing is acked; \
                 got {frames:?}"
            );
        })
        .await;
}
