//! Worker↔secondary custom-message tests at the pool/handle layer.
//!
//! Pins the doc'd contracts of the channel:
//!   * mid-task `Response::Custom` frames surface as in-order
//!     `WorkerEvent::CustomMessage`s and do NOT perturb terminal
//!     attribution (the #364 class);
//!   * the reply direction delivers MID-TASK over the full-duplex
//!     transport (`WorkerHandle::send_custom` while Processing) — the
//!     e2e smoke's "reply reaches `poll_messages`" half;
//!   * an Idle-time `send_custom` flushes immediately, and a custom
//!     queued before an assign is flushed BEFORE the ProcessTask
//!     frame (the between-tasks `@message_handler` ordering);
//!   * the chokepoint rejects over-limit payloads.

use std::collections::BTreeMap;

use dynrunner_core::{MessageReceiver, MessageSender, ResourceMap, TaskInfo, WorkerId};
use dynrunner_protocol_manager_worker::{CUSTOM_MESSAGE_MAX_BYTES, Command, Response};
use dynrunner_transport_channel::{ChannelManagerEnd, ChannelRunnerEnd, channel_pair};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use super::event::WorkerEvent;
use super::handle::WorkerHandle;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct TestId(String);

fn make_binary(name: &str) -> TaskInfo<TestId> {
    TaskInfo {
        path: std::path::PathBuf::from(name),
        size: 100,
        identifier: TestId(name.into()),
        phase_id: dynrunner_core::PhaseId::from("default"),
        type_id: dynrunner_core::TypeId::from("default"),
        affinity_id: None,
        payload: serde_json::Value::Null,
        task_id: name.into(),
        task_depends_on: Vec::new(),
        preferred_secondaries: dynrunner_core::SoftPreferredSecondaries::default(),
        preferred_version: Default::default(),
        kind: Default::default(),
        resolved_path: None,
    }
}

/// Drive `poll_ready` until the handle reports Ready.
async fn wait_ready(handle: &mut WorkerHandle<ChannelManagerEnd, TestId>) {
    loop {
        if handle.is_ready() {
            return;
        }
        handle.poll_ready().await;
        tokio::task::yield_now().await;
    }
}

fn new_handle(
    transport: ChannelManagerEnd,
) -> (
    WorkerHandle<ChannelManagerEnd, TestId>,
    mpsc::UnboundedReceiver<WorkerEvent<TestId>>,
) {
    let (event_tx, event_rx) = mpsc::unbounded_channel();
    (WorkerHandle::new(0 as WorkerId, 0, transport, event_tx), event_rx)
}

/// E2E (Rust half of the smoke): the worker streams THREE customs
/// mid-task → the manager observes three in-order, generation-stamped
/// `CustomMessage` events; the manager replies MID-TASK via
/// `send_custom` (the slot is Processing — the poll task's biased
/// select drains the outbox onto the full-duplex transport); the
/// worker receives the reply while still processing and finishes with
/// a `done:` echoing it — and the terminal lands as a normal
/// `TaskCompleted` attributed to the task (customs perturbed
/// nothing).
#[tokio::test(flavor = "current_thread")]
async fn mid_task_customs_stream_in_order_and_reply_reaches_worker() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (manager_end, runner_end) = channel_pair();

            // Fake worker: Ready → on ProcessTask, stream 3 customs,
            // then BLOCK on the manager's reply custom, then echo it
            // through `done:<reply payload>`.
            tokio::task::spawn_local(async move {
                let mut runner: ChannelRunnerEnd = runner_end;
                runner.send(Response::Ready).await.unwrap();
                loop {
                    match MessageReceiver::<Command>::recv(&mut runner).await {
                        Some(Command::ProcessTask { .. }) => {
                            for i in 0..3u8 {
                                runner
                                    .send(Response::Custom {
                                        topic: format!("batch-{i}"),
                                        data: vec![i; 4],
                                    })
                                    .await
                                    .unwrap();
                            }
                            // Mid-task reply wait: the next command
                            // MUST be the manager's custom (a Stop or
                            // a second ProcessTask here would fail
                            // the test through the echo mismatch).
                            let reply = MessageReceiver::<Command>::recv(&mut runner).await;
                            let echoed = match reply {
                                Some(Command::Custom { topic, data }) => {
                                    format!("{topic}:{}", data.len())
                                }
                                other => format!("UNEXPECTED {other:?}"),
                            };
                            runner
                                .send(Response::Done {
                                    result_data: Some(echoed.into_bytes()),
                                })
                                .await
                                .unwrap();
                        }
                        Some(Command::Stop) | None => break,
                        Some(Command::Custom { .. }) => {
                            panic!("unexpected custom outside a task in this fixture")
                        }
                    }
                }
            });

            let (mut handle, mut event_rx) = new_handle(manager_end);
            wait_ready(&mut handle).await;

            handle
                .assign_task(
                    make_binary("streamer"),
                    ResourceMap::new(),
                    false,
                    BTreeMap::new(),
                )
                .await
                .expect("assign");

            // Three in-order CustomMessage events, all stamped with
            // the live generation.
            for i in 0..3u8 {
                match event_rx.recv().await.expect("event") {
                    WorkerEvent::CustomMessage {
                        worker_id,
                        generation,
                        topic,
                        data,
                    } => {
                        assert_eq!(worker_id, 0);
                        assert_eq!(generation, 0, "stamped with the live generation");
                        assert_eq!(topic, format!("batch-{i}"), "in wire order");
                        assert_eq!(data, vec![i; 4]);
                    }
                    other => panic!("expected CustomMessage #{i}, got {other:?}"),
                }
            }

            // Reply MID-TASK: the slot is Processing (the poll task
            // owns the protocol); send_custom queues on the outbox
            // and the poll task's select delivers it.
            assert!(handle.is_processing());
            handle
                .send_custom("reply-topic".into(), vec![0xFF; 7])
                .await
                .expect("mid-task send_custom");

            // Terminal attribution intact: a normal TaskCompleted for
            // the streamed task, echoing the reply the worker saw.
            match event_rx.recv().await.expect("terminal") {
                WorkerEvent::TaskCompleted {
                    worker_id,
                    result,
                    result_data,
                    binary,
                    ..
                } => {
                    assert_eq!(worker_id, 0);
                    assert!(result.success, "customs must not perturb the terminal");
                    assert_eq!(
                        result_data.as_deref(),
                        Some(b"reply-topic:7".as_slice()),
                        "the worker received the mid-task reply before finishing"
                    );
                    assert_eq!(binary.unwrap().task_id, "streamer");
                }
                other => panic!("expected TaskCompleted, got {other:?}"),
            }
        })
        .await;
}

/// Idle-time `send_custom` flushes immediately; a custom queued while
/// the slot is Idle but unflushed (none here — queue-then-assign in
/// one breath) is delivered BEFORE the next ProcessTask frame so the
/// worker's pre-task read (the `@message_handler` point) sees it.
#[tokio::test(flavor = "current_thread")]
async fn idle_send_custom_delivers_before_next_dispatch() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (manager_end, runner_end) = channel_pair();
            let (order_tx, mut order_rx) = mpsc::unbounded_channel::<String>();

            tokio::task::spawn_local(async move {
                let mut runner: ChannelRunnerEnd = runner_end;
                runner.send(Response::Ready).await.unwrap();
                loop {
                    match MessageReceiver::<Command>::recv(&mut runner).await {
                        Some(Command::Custom { topic, .. }) => {
                            order_tx.send(format!("custom:{topic}")).unwrap();
                        }
                        Some(Command::ProcessTask { relative_path, .. }) => {
                            order_tx.send(format!("task:{relative_path}")).unwrap();
                            runner
                                .send(Response::Done { result_data: None })
                                .await
                                .unwrap();
                        }
                        Some(Command::Stop) | None => break,
                    }
                }
            });

            let (mut handle, mut event_rx) = new_handle(manager_end);
            wait_ready(&mut handle).await;

            // Idle-time send: flushed inline through the typed Idle
            // allowance.
            handle
                .send_custom("between-tasks".into(), b"hello".to_vec())
                .await
                .expect("idle send_custom");

            handle
                .assign_task(
                    make_binary("after-custom"),
                    ResourceMap::new(),
                    false,
                    BTreeMap::new(),
                )
                .await
                .expect("assign");

            // Wait for the task terminal so the worker has consumed
            // both frames.
            loop {
                match event_rx.recv().await.expect("event") {
                    WorkerEvent::TaskCompleted { .. } => break,
                    _ => continue,
                }
            }

            assert_eq!(order_rx.recv().await.unwrap(), "custom:between-tasks");
            assert_eq!(order_rx.recv().await.unwrap(), "task:after-custom");
        })
        .await;
}

/// The pool chokepoint defensively rejects over-limit payloads with
/// an error naming the size and the limit (the API call sites raise
/// first; nothing internal may bypass the contract).
#[tokio::test(flavor = "current_thread")]
async fn send_custom_rejects_oversize_payload() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (manager_end, runner_end) = channel_pair();
            // Keep the runner end alive so sends don't fail for the
            // wrong reason.
            let _runner = runner_end;
            let (mut handle, _event_rx) = new_handle(manager_end);

            let oversize = vec![0u8; CUSTOM_MESSAGE_MAX_BYTES + 1];
            let err = handle
                .send_custom("big".into(), oversize)
                .await
                .expect_err("oversize must be rejected");
            assert!(
                err.contains(&(CUSTOM_MESSAGE_MAX_BYTES + 1).to_string()),
                "names actual size: {err}"
            );
            assert!(
                err.contains(&CUSTOM_MESSAGE_MAX_BYTES.to_string()),
                "names the limit: {err}"
            );
        })
        .await;
}
