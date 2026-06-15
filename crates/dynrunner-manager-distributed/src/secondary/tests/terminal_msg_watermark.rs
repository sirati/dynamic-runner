//! The terminal-ordering gate's SECONDARY half (asm-dataset
//! run_20260611_005220): the causal `msgs_posted_through` stamp on task
//! terminals + the control-queue pre-drain that makes it a TRUE causal
//! watermark.
//!
//! Pins:
//!   * the worker-event arm's pre-drain applies every queued
//!     `SendToPrimary` control command (the consumer's
//!     `worker_message_listener` enqueues) BEFORE the worker terminal
//!     is built — so the customs precede the terminal on the wire AND
//!     the terminal's stamp covers them;
//!   * the stamp counts IMPORTANT messages only (droppables are
//!     unsequenced, `msg_seq = 0` — the droppable-class invariant: a
//!     lost-by-design droppable can never be awaited by the primary's
//!     gate);
//!   * the stamp is STICKY on the retained replay copy — a replay after
//!     later sends re-delivers the ORIGINAL watermark (the causal claim
//!     is about what preceded the terminal, not the replay).

#![cfg(test)]

use super::super::control::SecondaryControlCommand;
use super::super::test_helpers::{
    FakeWorkerFactory, election_config, make_secondary_recording,
    make_secondary_recording_with_membership,
};
use super::generation_gate::test_oom_watcher;
use super::processing::make_binary;
use dynrunner_core::{ResourceMap, TaskResult};
use dynrunner_manager_local::WorkerEvent;
use dynrunner_protocol_primary_secondary::{ClusterMutation, DistributedMessage};

/// The production shape, at the loop-arm boundary: the consumer's
/// listener queued 3 important spawn batches + 1 droppable progress
/// ping + 1 important summary on the control channel; the worker then
/// exits. The worker-event arm runs the CAUSAL PRE-DRAIN, then the
/// terminal report — so the wire shows customs (dense important seqs
/// 1..=4; the droppable unsequenced at 0) BEFORE the `TaskComplete`,
/// whose `msgs_posted_through` stamp covers all four importants.
#[tokio::test(flavor = "current_thread")]
async fn predrain_orders_queued_customs_before_terminal_and_stamps_watermark() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut secondary, log) = make_secondary_recording(election_config("sec-2"), 1);
            secondary.set_bootstrap_primary_id("setup".to_string());
            let mut factory = FakeWorkerFactory;
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;

            // Bind the dep-graph task to worker 0.
            let binary = make_binary("dep_graph", 50);
            let file_hash = format!("hash_{}", binary.identifier.0);
            secondary
                .op_mut()
                .active_tasks
                .insert(file_hash.clone(), 0);

            // The consumer's worker_message_listener enqueues through the
            // PyO3 SecondaryHandle's control sender — BEFORE the exit.
            let control_tx = secondary.secondary_control_sender();
            for seq in 1..=3u64 {
                control_tx
                    .send(SecondaryControlCommand::SendToPrimary {
                        topic: "spawn_batch".into(),
                        data: seq.to_string().into_bytes(),
                        important: true,
                        is_high_volume: false,
                    })
                    .unwrap();
            }
            control_tx
                .send(SecondaryControlCommand::SendToPrimary {
                    topic: "progress".into(),
                    data: b"ping".to_vec(),
                    important: false,
                    is_high_volume: false,
                })
                .unwrap();
            control_tx
                .send(SecondaryControlCommand::SendToPrimary {
                    topic: "summary".into(),
                    data: b"4".to_vec(),
                    important: true,
                    is_high_volume: false,
                })
                .unwrap();

            // The pool-event seam the loop arm routes through: causal
            // fence (pre-drain), then the event (whose terminal report
            // is stamped at the send_to_primary chokepoint).
            let mut control_rx = secondary.secondary_control_rx.take();
            let oom = test_oom_watcher();
            secondary
                .process_worker_pool_event(
                    WorkerEvent::TaskCompleted {
                        worker_id: 0,
                        generation: 0,
                        result: TaskResult::ok(),
                        result_data: None,
                        binary: Some(binary),
                        estimated_resources: ResourceMap::new(),
                    },
                    &oom,
                    &mut control_rx,
                )
                .await
                .unwrap();
            secondary.drain_egress().await;

            // Wire order + shapes: customs first (dense important seqs;
            // the droppable unsequenced), then the stamped terminal.
            let frames = log.borrow().clone();
            let mut custom_seqs = Vec::new();
            let mut terminal_stamp = None;
            let mut terminal_after_customs = false;
            for frame in &frames {
                match frame {
                    DistributedMessage::CustomMessage {
                        msg_seq, important, ..
                    } => {
                        assert!(
                            terminal_stamp.is_none(),
                            "every queued custom must precede the terminal on \
                             the wire; got a custom after it: {frames:?}"
                        );
                        custom_seqs.push((*msg_seq, *important));
                    }
                    DistributedMessage::TaskComplete {
                        task_hash,
                        msgs_posted_through,
                        ..
                    } if *task_hash == file_hash => {
                        terminal_stamp = Some(*msgs_posted_through);
                        terminal_after_customs = custom_seqs.len() == 5;
                    }
                    _ => {}
                }
            }
            assert_eq!(
                custom_seqs,
                vec![(1, true), (2, true), (3, true), (0, false), (4, true)],
                "importants get the dense per-origin seqs 1..=4 in enqueue \
                 order; the droppable is unsequenced (msg_seq 0)"
            );
            assert!(
                terminal_after_customs,
                "the terminal must leave AFTER all five queued customs; \
                 frames: {frames:?}"
            );
            assert_eq!(
                terminal_stamp,
                Some(Some(4)),
                "the terminal's causal watermark covers every IMPORTANT \
                 message enqueued before the worker exit (and does not \
                 count the droppable)"
            );
        })
        .await;
}

/// A terminal from an origin that never sent any custom message is
/// stamped `Some(0)` — an explicit "no causal claim" the primary's gate
/// admits unconditionally (watermark-absent reads 0).
#[tokio::test(flavor = "current_thread")]
async fn terminal_with_no_prior_customs_stamps_zero() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut secondary, log) = make_secondary_recording(election_config("sec-2"), 1);
            secondary.set_bootstrap_primary_id("setup".to_string());
            let mut factory = FakeWorkerFactory;
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;

            let binary = make_binary("plain", 50);
            let file_hash = format!("hash_{}", binary.identifier.0);
            secondary
                .op_mut()
                .active_tasks
                .insert(file_hash.clone(), 0);
            let oom = test_oom_watcher();
            secondary
                .handle_worker_event(
                    WorkerEvent::TaskCompleted {
                        worker_id: 0,
                        generation: 0,
                        result: TaskResult::ok(),
                        result_data: None,
                        binary: Some(binary),
                        estimated_resources: ResourceMap::new(),
                    },
                    &oom,
                )
                .await
                .unwrap();
            secondary.drain_egress().await;

            let stamps: Vec<Option<u64>> = log
                .borrow()
                .iter()
                .filter_map(|m| match m {
                    DistributedMessage::TaskComplete {
                        msgs_posted_through,
                        ..
                    } => Some(*msgs_posted_through),
                    _ => None,
                })
                .collect();
            assert_eq!(
                stamps,
                vec![Some(0)],
                "no prior importants → watermark 0 (gate trivially open)"
            );
        })
        .await;
}

/// #426 trace replay (asm-dataset run_20260611_182745): the tail of a
/// streamed spawn — the worker sends its FINAL descriptor batch, then
/// the summary, then completes immediately. The batches enter as
/// `WorkerEvent::CustomMessage` POOL events (the production ingress —
/// NOT pre-queued control commands), ride the real worker-message
/// dispatcher pipeline to the consumer's relaying
/// `worker_message_listener`, and only THEN become `SendToPrimary`
/// control commands. The completion pool event arrives while the final
/// sends are still in that pipeline.
///
/// Production failure this pins: on_phase_end fired with
/// spawned = 66675 = 66713 − 38 — the terminal's
/// `msgs_posted_through` stamp covered every batch EXCEPT the final one
/// (and the summary), because #386's control-queue pre-drain cannot see
/// sends that have not yet crossed the dispatcher-task → listener →
/// control-queue hop. The fence must make the terminal wait for the
/// pipeline: every important the task sent before completing leaves the
/// wire BEFORE the terminal, and the stamp covers them ALL.
#[tokio::test(flavor = "current_thread")]
async fn trace_426_completion_covers_customs_still_in_dispatcher_pipeline() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut secondary, log) = make_secondary_recording(election_config("sec-2"), 1);
            secondary.set_bootstrap_primary_id("setup".to_string());

            // The consumer's `worker_message_listener` shape: relay
            // every worker batch/summary to the primary as IMPORTANT,
            // through the same control-channel ingress the PyO3
            // `SecondaryHandle.send_to_primary` uses (a non-blocking
            // queue — the listener never touches the loop's state).
            struct RelayListener {
                control: tokio::sync::mpsc::UnboundedSender<SecondaryControlCommand>,
            }
            impl crate::worker_messages::WorkerMessageListener for RelayListener {
                fn on_message(&self, event: &crate::worker_messages::WorkerCustomMessage) {
                    self.control
                        .send(SecondaryControlCommand::SendToPrimary {
                            topic: event.topic.clone(),
                            data: event.data.clone(),
                            important: true,
                            is_high_volume: false,
                        })
                        .expect("control channel alive");
                }
            }
            let control = secondary.secondary_control_sender();
            secondary.register_worker_message_listener(Box::new(RelayListener { control }));
            // Stand up the REAL dispatcher pipeline (the spawn the
            // coordinator performs at run start).
            secondary.spawn_worker_message_dispatcher();

            let mut factory = FakeWorkerFactory;
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;

            let binary = make_binary("dep_graph", 50);
            let file_hash = format!("hash_{}", binary.identifier.0);
            secondary
                .op_mut()
                .active_tasks
                .insert(file_hash.clone(), 0);

            // Batches 1..618 of the trace, compressed to two: already
            // relayed AND seq-assigned (seqs 1, 2) — the part of the
            // stream the production stamp DID cover.
            for n in [617u64, 618] {
                secondary
                    .send_custom_to_primary("spawn_batch".into(), n.to_string().into_bytes(), true, false)
                    .await
                    .unwrap();
            }

            let mut control_rx = secondary.secondary_control_rx.take();
            let oom = test_oom_watcher();

            // THE TRACE TAIL, each step entering through the loop's
            // pool-event seam exactly as the operational arm runs it:
            // final batch → summary → immediate completion.
            for (topic, data) in [
                ("spawn_batch", b"619".to_vec()),
                ("summary", b"66713".to_vec()),
            ] {
                secondary
                    .process_worker_pool_event(
                        WorkerEvent::CustomMessage {
                            worker_id: 0,
                            generation: 0,
                            topic: topic.into(),
                            data,
                        },
                        &oom,
                        &mut control_rx,
                    )
                    .await
                    .unwrap();
            }
            secondary
                .process_worker_pool_event(
                    WorkerEvent::TaskCompleted {
                        worker_id: 0,
                        generation: 0,
                        result: TaskResult::ok(),
                        result_data: None,
                        binary: Some(binary),
                        estimated_resources: ResourceMap::new(),
                    },
                    &oom,
                    &mut control_rx,
                )
                .await
                .unwrap();
            secondary.drain_egress().await;

            // The wire must show ALL FOUR importants (dense seqs 1..=4)
            // before the terminal, whose stamp covers every one of them.
            let frames = log.borrow().clone();
            let mut custom_seqs = Vec::new();
            let mut terminal_stamp = None;
            for frame in &frames {
                match frame {
                    DistributedMessage::CustomMessage { msg_seq, .. } => {
                        assert!(
                            terminal_stamp.is_none(),
                            "a custom the task sent before completing must \
                             precede its terminal on the wire; got one after: \
                             {frames:?}"
                        );
                        custom_seqs.push(*msg_seq);
                    }
                    DistributedMessage::TaskComplete {
                        task_hash,
                        msgs_posted_through,
                        ..
                    } if *task_hash == file_hash => {
                        terminal_stamp = Some(*msgs_posted_through);
                    }
                    _ => {}
                }
            }
            assert_eq!(
                custom_seqs,
                vec![1, 2, 3, 4],
                "all four importants (incl. the final batch + summary that \
                 rode the dispatcher pipeline) leave before the terminal; \
                 frames: {frames:?}"
            );
            assert_eq!(
                terminal_stamp,
                Some(Some(4)),
                "the terminal's causal watermark covers the task's LAST \
                 sends, not just the ones the pre-drain caught; frames: \
                 {frames:?}"
            );
        })
        .await;
}

/// The common-path negative (#426): a completion with NO outstanding
/// sends in the dispatcher pipeline processes promptly — the fence adds
/// one dispatcher round-trip, never a timeout-class wait.
#[tokio::test(flavor = "current_thread")]
async fn trace_426_completion_with_empty_pipeline_processes_immediately() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut secondary, log) = make_secondary_recording(election_config("sec-2"), 1);
            secondary.set_bootstrap_primary_id("setup".to_string());
            // Real dispatcher running, zero listeners — an idle pipeline.
            secondary.spawn_worker_message_dispatcher();
            let mut factory = FakeWorkerFactory;
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;

            let binary = make_binary("plain", 50);
            let file_hash = format!("hash_{}", binary.identifier.0);
            secondary
                .op_mut()
                .active_tasks
                .insert(file_hash.clone(), 0);
            let mut control_rx = secondary.secondary_control_rx.take();
            let oom = test_oom_watcher();
            tokio::time::timeout(
                std::time::Duration::from_secs(5),
                secondary.process_worker_pool_event(
                    WorkerEvent::TaskCompleted {
                        worker_id: 0,
                        generation: 0,
                        result: TaskResult::ok(),
                        result_data: None,
                        binary: Some(binary),
                        estimated_resources: ResourceMap::new(),
                    },
                    &oom,
                    &mut control_rx,
                ),
            )
            .await
            .expect("an empty pipeline must never make the terminal wait")
            .unwrap();
            secondary.drain_egress().await;

            let stamps: Vec<Option<u64>> = log
                .borrow()
                .iter()
                .filter_map(|m| match m {
                    DistributedMessage::TaskComplete {
                        msgs_posted_through,
                        ..
                    } => Some(*msgs_posted_through),
                    _ => None,
                })
                .collect();
            assert_eq!(stamps, vec![Some(0)], "no prior importants → watermark 0");
        })
        .await;
}

/// Replay stickiness (the promotion/failover leg): a terminal retained
/// on a no-route window keeps its ORIGINAL `msgs_posted_through` across
/// replays, even when later importants advance the counter before the
/// route recovers — the causal claim is about what preceded THIS
/// terminal, and a replayed re-landing at a promoted primary must carry
/// the same gate threshold.
#[tokio::test(flavor = "current_thread")]
async fn retained_terminal_replays_with_original_watermark() {
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

            // One important custom precedes the terminal: watermark 1.
            secondary
                .send_custom_to_primary("spawn_batch".into(), b"1".to_vec(), true, false)
                .await
                .unwrap();

            // ROUTE DOWN, then the worker exits: the terminal (stamped 1)
            // is absorbed + retained.
            membership.borrow_mut().retain(|id| id.as_str() != "setup");
            secondary.publish_membership();
            let binary = make_binary("dep_graph", 50);
            let file_hash = format!("hash_{}", binary.identifier.0);
            secondary
                .op_mut()
                .active_tasks
                .insert(file_hash.clone(), 0);
            let oom = test_oom_watcher();
            secondary
                .handle_worker_event(
                    WorkerEvent::TaskCompleted {
                        worker_id: 0,
                        generation: 0,
                        result: TaskResult::ok(),
                        result_data: None,
                        binary: Some(binary),
                        estimated_resources: ResourceMap::new(),
                    },
                    &oom,
                )
                .await
                .unwrap();
            let retained: Vec<Option<u64>> = secondary
                .pending_report_replays
                .iter()
                .filter(|e| e.frame.task_hash() == Some(file_hash.as_str()))
                .map(|e| e.frame.msgs_posted_through())
                .collect();
            assert_eq!(
                retained,
                vec![Some(1)],
                "the retained terminal carries the watermark stamped at \
                 its FIRST send"
            );

            // Later sends advance the counter while the terminal waits.
            secondary
                .send_custom_to_primary("spawn_batch".into(), b"2".to_vec(), true, false)
                .await
                .unwrap();

            // FAILOVER: a new primary is named; the drain re-delivers.
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

            let replayed: Vec<Option<u64>> = log
                .borrow()
                .iter()
                .filter_map(|m| match m {
                    DistributedMessage::TaskComplete {
                        task_hash,
                        msgs_posted_through,
                        ..
                    } if *task_hash == file_hash => Some(*msgs_posted_through),
                    _ => None,
                })
                .collect();
            assert_eq!(
                replayed,
                vec![Some(1)],
                "the replay re-delivers the ORIGINAL causal watermark — \
                 never re-stamped against the advanced counter"
            );
        })
        .await;
}
