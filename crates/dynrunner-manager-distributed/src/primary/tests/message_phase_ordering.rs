//! Production replay: phase-end overtaking the phase's own IMPORTANT
//! custom messages (asm-dataset, run_20260611_005220).
//!
//! The consumer's streamed-spawn pipeline: a dep-graph worker sends 3
//! important spawn-batch messages + 1 important summary via
//! `Task.send_message`; the secondary's `worker_message_listener`
//! forwards all four via `SecondaryHandle.send_to_primary(...,
//! important=True)` BEFORE the worker exits. On the wire the
//! task-terminal overtook seq 2..4 (the secondary's control-queue
//! drain raced the worker-event arm; peer-forwarded terminal redundancy
//! can reorder the same way) — the primary handled seq=1, processed the
//! terminal, derived phase-end, and the consumer's `on_phase_end`
//! barrier raised "handoff incomplete: no summary" while seq 2..4 were
//! still in flight.
//!
//! The invariant these tests pin (the per-origin causal terminal gate):
//! a task terminal stamped `msgs_posted_through = W` from origin X is
//! NOT processed — and phase-end, which derives from terminals, does
//! NOT fire — until X's replicated custom-inbox terminal watermark
//! covers W (every important seq `1..=W` is Handled/Failed-resolved).
//! Resolution of the awaited messages re-checks the gate on the
//! existing dispatch cadence (ingest / heartbeat / promotion replay).
//!
//! Variants: handler-raise (Failed still resolves the gate — the
//! consumer's barrier then fires legitimately), droppable-class
//! (droppables are NOT counted in the `msg_seq` space, so a
//! lost-by-design droppable can never wedge the gate), no-handler
//! (consume-unhandled-with-WARN resolves the gate), dead-origin (an
//! origin removed from membership opens its gates — its retained
//! messages died with it).

use super::*;

use std::sync::{Arc, Mutex};

use dynrunner_core::{PhaseId, ResourceAmount, ResourceKind, TypeId};

use crate::primary::wire::compute_task_hash;

/// Interleaved event log: handler invocations vs phase-end firings,
/// in observed order — the production race is an ORDER violation, so
/// one shared log is the assertable artifact.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Ev {
    /// `custom_message_handler(origin, seq-parsed-from-payload)` ran.
    Handler(u64),
    /// `on_phase_end(phase, completed, failed)` fired.
    PhaseEnd(String, u32, u32),
}

type EvLog = Arc<Mutex<Vec<Ev>>>;

fn mem(bytes: u64) -> Vec<ResourceAmount> {
    vec![ResourceAmount {
        kind: ResourceKind::memory(),
        amount: bytes,
    }]
}

/// Build the dep-graph task in phase `dependency_graph`.
fn dep_graph_task() -> TaskInfo<TestId> {
    let mut t = make_binary("dep_graph", 100);
    t.phase_id = PhaseId::from("dependency_graph");
    t.type_id = TypeId::from("default");
    t
}

/// Seed the production shape: origin `sec-0` is a LIVE member with one
/// worker; the single `dependency_graph` task is replicated `InFlight`
/// on `(sec-0, 0)`; hydrate rebuilds the slot + in-flight ledger so the
/// wire terminal drives the real completion cascade. Returns the task
/// hash.
fn seed_one_inflight_task(
    primary: &mut PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
) -> String {
    let task = dep_graph_task();
    let hash = compute_task_hash(&task);
    {
        let cs = primary.cluster_state_mut_for_test();
        cs.apply(ClusterMutation::PeerJoined {
            peer_id: "sec-0".into(),
            is_observer: false,
            can_be_primary: true,
            cap_version: Default::default(),
            member_gen: 0,
        });
        cs.apply(ClusterMutation::SecondaryCapacity {
            secondary: "sec-0".into(),
            worker_count: 1,
            resources: mem(8 * 1024 * 1024 * 1024),
        });
        cs.apply(ClusterMutation::TaskAdded {
            hash: hash.clone(),
            task: task.clone(),
        });
        cs.apply(ClusterMutation::TaskAssigned {
            attempt: 0,
            hash: hash.clone(),
            secondary: "sec-0".into(),
            worker: 0,
            version: Default::default(),
        });
    }
    primary.hydrate_from_cluster_state()
        .expect("test fixture: composed task graph is valid");
    hash
}

/// Install the interleave-recording hooks: the custom-message handler
/// logs `Handler(seq)` (seq parsed from the payload; raises on
/// `fail_topics`), `on_phase_end` logs `PhaseEnd(..)`.
fn install_recording_hooks(
    primary: &mut PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
    fail_topics: Vec<String>,
) -> EvLog {
    let log: EvLog = Arc::new(Mutex::new(Vec::new()));
    let h_log = log.clone();
    primary.set_custom_message_handler(Box::new(move |_origin, topic, data, _important| {
        let seq: u64 = String::from_utf8_lossy(data).parse().unwrap_or(0);
        h_log.lock().unwrap().push(Ev::Handler(seq));
        if fail_topics.iter().any(|t| t == topic) {
            Err(format!("handler refused topic {topic}"))
        } else {
            Ok(())
        }
    }));
    let e_log = log.clone();
    primary.register_phase_lifecycle_callbacks(
        Box::new(|_p| {}),
        Box::new(move |p, c, f, _outputs| {
            e_log.lock().unwrap().push(Ev::PhaseEnd(p.to_string(), c, f));
        }),
    );
    log
}

/// One important `CustomMessage` wire frame from `sec-0` (payload =
/// seq, so the handler-visible order is directly assertable).
fn important_custom(seq: u64, topic: &str) -> DistributedMessage<TestId> {
    DistributedMessage::CustomMessage {
        target: None,
        sender_id: "sec-0".into(),
        timestamp: 0.0,
        origin_secondary_id: "sec-0".into(),
        msg_seq: seq,
        topic: topic.into(),
        data: seq.to_string().into_bytes(),
        important: true,
        is_high_volume: false,
        delivery_seq: Some(seq),
    }
}

/// The task terminal from `sec-0`, stamped with the causal watermark
/// `msgs_posted_through` (the production terminal would carry 4: three
/// spawn batches + the summary were stamped before it left).
fn stamped_complete(hash: &str, msgs_posted_through: u64) -> DistributedMessage<TestId> {
    DistributedMessage::TaskComplete {
        target: None,
        sender_id: "sec-0".into(),
        timestamp: 0.0,
        secondary_id: "sec-0".into(),
        worker_id: 0,
        task_hash: hash.into(),
        result_data: None,
        delivery_seq: Some(100),
        msgs_posted_through: Some(msgs_posted_through),
    }
}

fn phase_end_count(log: &EvLog) -> usize {
    log.lock()
        .unwrap()
        .iter()
        .filter(|e| matches!(e, Ev::PhaseEnd(..)))
        .count()
}

/// THE production interleave (run_20260611_005220): seq=1 lands and is
/// handled; the terminal (stamped through 4) lands NEXT, while seq 2..4
/// are still in flight; they land after. Phase-end MUST NOT fire on the
/// terminal's landing — it fires only once seq 2..4 (incl. the summary)
/// are resolved, and the full handler sequence precedes it.
#[tokio::test(flavor = "current_thread")]
async fn phase_end_defers_until_terminals_causal_messages_resolve() {
    let _ = tracing_subscriber::fmt::try_init();
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
            let hash = seed_one_inflight_task(&mut primary);
            let log = install_recording_hooks(&mut primary, vec![]);

            // "custom_message_handler: spawn_batch seq=0; spawning 125"
            primary
                .dispatch_message(important_custom(1, "spawn_batch"), &mut None)
                .await
                .unwrap();
            assert_eq!(log.lock().unwrap().clone(), vec![Ev::Handler(1)]);

            // "task complete (dep_graph)" — overtaking seq 2..4.
            primary
                .dispatch_message(stamped_complete(&hash, 4), &mut None)
                .await
                .unwrap();
            assert_eq!(
                phase_end_count(&log),
                0,
                "phase-end fired with the terminal's causally-prior \
                 important messages (seq 2..4) unresolved — the \
                 production race; log: {:?}",
                log.lock().unwrap()
            );
            assert_eq!(
                primary.completed_count(),
                0,
                "the gated terminal must not be accounted before its \
                 causal messages resolve"
            );

            // seq 2..3 (spawn batches) land — still short of the stamp.
            for seq in 2..=3u64 {
                primary
                    .dispatch_message(important_custom(seq, "spawn_batch"), &mut None)
                    .await
                    .unwrap();
            }
            assert_eq!(
                phase_end_count(&log),
                0,
                "watermark 3 < stamp 4: the gate must still hold; log: {:?}",
                log.lock().unwrap()
            );

            // The summary (seq 4) lands — the gate opens on the SAME
            // dispatch cadence and the deferred terminal processes.
            primary
                .dispatch_message(important_custom(4, "summary"), &mut None)
                .await
                .unwrap();

            let events = log.lock().unwrap().clone();
            assert_eq!(
                events,
                vec![
                    Ev::Handler(1),
                    Ev::Handler(2),
                    Ev::Handler(3),
                    Ev::Handler(4),
                    Ev::PhaseEnd("dependency_graph".into(), 1, 0),
                ],
                "every causally-prior handler invocation precedes the \
                 phase-end firing, which fires exactly once"
            );
            assert_eq!(primary.completed_count(), 1);
            assert!(
                primary.slot_is_idle_for_test("sec-0", 0),
                "the released terminal runs the full completion cascade \
                 (slot freed)"
            );
        })
        .await;
}

/// Handler-raise variant: the summary handler raises → the message
/// resolves terminally `Failed` — the gate counts Failed as resolved,
/// the deferred terminal processes, and phase-end still fires (the
/// consumer's barrier-on-missing-summary then fires legitimately in
/// ITS on_phase_end — framework-side nothing wedges).
#[tokio::test(flavor = "current_thread")]
async fn raising_handler_resolves_gate_and_phase_end_fires() {
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
            let hash = seed_one_inflight_task(&mut primary);
            let log = install_recording_hooks(&mut primary, vec!["summary".into()]);

            primary
                .dispatch_message(stamped_complete(&hash, 2), &mut None)
                .await
                .unwrap();
            primary
                .dispatch_message(important_custom(1, "spawn_batch"), &mut None)
                .await
                .unwrap();
            assert_eq!(phase_end_count(&log), 0, "stamp 2 > watermark 1");

            // The raising summary: terminal Failed — resolved.
            primary
                .dispatch_message(important_custom(2, "summary"), &mut None)
                .await
                .unwrap();

            let events = log.lock().unwrap().clone();
            assert_eq!(
                events,
                vec![
                    Ev::Handler(1),
                    Ev::Handler(2),
                    Ev::PhaseEnd("dependency_graph".into(), 1, 0),
                ],
                "a handler raise is a terminal resolution — the gate \
                 opens and phase-end fires (never a wedge)"
            );
            assert_eq!(primary.completed_count(), 1);
        })
        .await;
}

/// Droppable-class invariant: droppables are NOT counted in the
/// `msg_seq` identity space (they stamp `msg_seq = 0` and never post),
/// so the terminal's stamp counts importants ONLY — a droppable lost
/// by design (no-route / failover) can never hold the gate. Here the
/// consumer sent one important + N droppables that were ALL lost; the
/// stamp is 1 and the terminal processes the moment seq 1 resolves.
#[tokio::test(flavor = "current_thread")]
async fn lost_droppables_never_wedge_the_gate() {
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
            let hash = seed_one_inflight_task(&mut primary);
            let log = install_recording_hooks(&mut primary, vec![]);

            // The one important message the origin sent (its droppables
            // — unsequenced, msg_seq 0 — were lost in flight and never
            // arrive).
            primary
                .dispatch_message(important_custom(1, "spawn_batch"), &mut None)
                .await
                .unwrap();
            // Terminal stamped 1: importants only — the lost droppables
            // do not gate.
            primary
                .dispatch_message(stamped_complete(&hash, 1), &mut None)
                .await
                .unwrap();
            assert_eq!(
                phase_end_count(&log),
                1,
                "stamp counts IMPORTANT messages only; lost droppables \
                 must not defer the terminal; log: {:?}",
                log.lock().unwrap()
            );
            assert_eq!(primary.completed_count(), 1);
        })
        .await;
}

/// No-handler variant: a consumer without `custom_message_handler`
/// consumes important messages unhandled-with-WARN (terminal Handled)
/// — the gate resolves and the deferred terminal processes. Also pins
/// the full wire-reorder shape: the terminal lands FIRST (parked), the
/// message lands after (ingest-cadence release).
#[tokio::test(flavor = "current_thread")]
async fn no_handler_consumer_does_not_wedge_the_gate() {
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
            let hash = seed_one_inflight_task(&mut primary);
            // Phase-end recorder only — NO custom handler installed.
            let log: EvLog = Arc::new(Mutex::new(Vec::new()));
            let e_log = log.clone();
            primary.register_phase_lifecycle_callbacks(
                Box::new(|_p| {}),
                Box::new(move |p, c, f, _outputs| {
                    e_log.lock().unwrap().push(Ev::PhaseEnd(p.to_string(), c, f));
                }),
            );

            primary
                .dispatch_message(stamped_complete(&hash, 1), &mut None)
                .await
                .unwrap();
            assert_eq!(phase_end_count(&log), 0, "terminal parked behind seq 1");

            primary
                .dispatch_message(important_custom(1, "spawn_batch"), &mut None)
                .await
                .unwrap();
            assert_eq!(
                phase_end_count(&log),
                1,
                "the hook-less consume-unhandled (WARN + Handled latch) \
                 must resolve the gate; log: {:?}",
                log.lock().unwrap()
            );
            assert_eq!(primary.completed_count(), 1);
        })
        .await;
}

/// Dead-origin variant: the origin dies after its terminal landed but
/// before its retained important messages could re-deliver — the
/// messages died with the origin's retention buffer and can NEVER
/// arrive, so the membership removal opens the origin's gates on the
/// next release pass (the heartbeat-cadence dispatch backstop) instead
/// of wedging the phase forever.
#[tokio::test(flavor = "current_thread")]
async fn dead_origin_releases_its_gated_terminals() {
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
            let hash = seed_one_inflight_task(&mut primary);
            let log = install_recording_hooks(&mut primary, vec![]);

            primary
                .dispatch_message(important_custom(1, "spawn_batch"), &mut None)
                .await
                .unwrap();
            primary
                .dispatch_message(stamped_complete(&hash, 4), &mut None)
                .await
                .unwrap();
            assert_eq!(phase_end_count(&log), 0, "gated on seq 2..4");

            // The origin is removed from the replicated membership —
            // its unsent seq 2..4 died with its retention buffer.
            primary
                .cluster_state_mut_for_test()
                .apply(ClusterMutation::PeerRemoved {
                    id: "sec-0".into(),
                    cause: dynrunner_protocol_primary_secondary::RemovalCause::KeepaliveMiss,
                    member_gen: 0,
                });

            // The next dispatch-cadence pass (the heartbeat backstop
            // calls exactly this) must release the dead origin's gate.
            primary.dispatch_unhandled_custom_messages(&mut None).await;
            assert_eq!(
                phase_end_count(&log),
                1,
                "a dead origin's gated terminals must FORCE-RELEASE (its \
                 messages are lost-with-origin by design); log: {:?}",
                log.lock().unwrap()
            );
            assert_eq!(primary.completed_count(), 1);
        })
        .await;
}

/// Legacy-sender compat: a terminal with NO stamp (pre-field sender —
/// serde-default decode) carries no causal claim and is never gated,
/// even with an unresolved important backlog from the same origin.
#[tokio::test(flavor = "current_thread")]
async fn unstamped_terminal_is_never_gated() {
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
            let hash = seed_one_inflight_task(&mut primary);
            let log = install_recording_hooks(&mut primary, vec![]);

            // An unstamped terminal (legacy sender).
            let mut msg = stamped_complete(&hash, 0);
            if let DistributedMessage::TaskComplete {
                msgs_posted_through,
                ..
            } = &mut msg
            {
                *msgs_posted_through = None;
            }
            primary.dispatch_message(msg, &mut None).await.unwrap();
            assert_eq!(
                phase_end_count(&log),
                1,
                "an unstamped terminal makes no causal claim — processed \
                 immediately (pre-fix behaviour preserved for legacy \
                 senders)"
            );
        })
        .await;
}
