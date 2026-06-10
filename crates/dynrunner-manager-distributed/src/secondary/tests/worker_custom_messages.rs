//! Worker→secondary custom-message bridge tests (feature 2).
//!
//! Pins the secondary side of the channel:
//!   1. a CURRENT-generation `WorkerEvent::CustomMessage` is enqueued
//!      for the worker-message dispatcher, in order, carrying the
//!      sending task's `type_id`;
//!   2. a STALE-generation custom (buffered by a replaced subprocess)
//!      rides the SAME generation gate as every other worker event
//!      and is dropped before any listener can observe it;
//!   3. customs are node-local: handling one reports NOTHING to the
//!      primary (no CRDT origination, no wire frame — the
//!      no-observer-only-CRDT law);
//!   4. the `SecondaryHandle` reply ingress
//!      (`secondary_control_sender`) reaches the receiver the
//!      operational loop's drain arm owns.

use super::super::test_helpers::{FakeWorkerFactory, make_secondary_recording};
use super::super::*;
use super::generation_gate::{one_worker_config, test_oom_watcher};
use super::processing::make_binary;
use dynrunner_manager_local::WorkerEvent;

/// Current-generation customs dispatch in order with the running
/// task's `type_id`; stale-generation customs are dropped; nothing is
/// ever reported to the primary.
#[tokio::test(flavor = "current_thread")]
async fn customs_dispatch_in_order_and_stale_generation_drops() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut secondary, log) = make_secondary_recording(one_worker_config("sec-1"), 1);
            secondary.set_bootstrap_primary_id("setup".to_string());

            let mut factory = FakeWorkerFactory;
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;

            // Take the dispatcher receiver the run boundary would
            // normally hand to `run_worker_message_dispatcher`; the
            // test observes the enqueued events directly.
            let mut worker_message_rx = secondary
                .worker_message_rx
                .take()
                .expect("dispatcher rx present pre-run");

            // Bind a running task so the bridge can resolve the
            // sending task's type identity.
            let binary = make_binary("streaming-task", 50);
            let expected_type = binary.type_id.to_string();
            secondary.pool_mut().workers[0].current_binary = Some(binary);
            let current_gen = secondary.pool_mut().workers[0].generation;

            let oom = test_oom_watcher();

            // Two in-order customs at the CURRENT generation.
            for i in 0..2u8 {
                secondary
                    .handle_worker_event(
                        WorkerEvent::CustomMessage {
                            worker_id: 0,
                            generation: current_gen,
                            topic: format!("batch-{i}"),
                            data: vec![i; 3],
                        },
                        &oom,
                    )
                    .await
                    .unwrap();
            }
            // One STALE custom (generation bumped past the event's):
            // simulate the replaced-subprocess buffer by bumping the
            // slot's generation through a real replacement edge.
            secondary
                .pool_mut()
                .restart_worker(0, &mut factory, false)
                .await
                .unwrap();
            secondary
                .handle_worker_event(
                    WorkerEvent::CustomMessage {
                        worker_id: 0,
                        generation: current_gen, // now stale (slot is gen+1)
                        topic: "stale".into(),
                        data: b"dead".to_vec(),
                    },
                    &oom,
                )
                .await
                .unwrap();

            // Exactly the two current-generation events, in order,
            // with the running task's type identity.
            let first = worker_message_rx.try_recv().expect("first custom");
            assert_eq!(first.worker_id, 0);
            assert_eq!(first.type_id, expected_type);
            assert_eq!(first.topic, "batch-0");
            assert_eq!(first.data, vec![0u8; 3]);
            let second = worker_message_rx.try_recv().expect("second custom");
            assert_eq!(second.topic, "batch-1");
            assert!(
                worker_message_rx.try_recv().is_err(),
                "the stale-generation custom must be dropped by the gate"
            );

            // Node-local by law: NOTHING was reported to the primary.
            secondary.drain_egress().await;
            assert!(
                log.borrow().is_empty(),
                "customs must not produce primary-bound frames; got {:?}",
                log.borrow()
            );
        })
        .await;
}

/// The `SecondaryHandle` ingress: a command queued through
/// `secondary_control_sender()` lands on the receiver the
/// operational loop's drain arm takes at entry.
#[tokio::test(flavor = "current_thread")]
async fn secondary_control_sender_reaches_loop_receiver() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut secondary, _log) = make_secondary_recording(one_worker_config("sec-1"), 1);
            let tx = secondary.secondary_control_sender();
            tx.send(SecondaryControlCommand::SendToWorker {
                worker_id: 0,
                topic: "reply".into(),
                data: b"pong".to_vec(),
            })
            .expect("queue control command");

            let mut rx = secondary
                .secondary_control_rx
                .take()
                .expect("control rx present pre-run");
            match rx.try_recv().expect("command delivered") {
                SecondaryControlCommand::SendToWorker {
                    worker_id,
                    topic,
                    data,
                } => {
                    assert_eq!(worker_id, 0);
                    assert_eq!(topic, "reply");
                    assert_eq!(data, b"pong");
                }
                other => panic!("expected the queued SendToWorker back, got {other:?}"),
            }
        })
        .await;
}
