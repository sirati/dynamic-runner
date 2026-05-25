//! Plumbing contract: `result_data` bytes attached to a worker's
//! `Response::Done` must ride the secondary's outbound
//! `DistributedMessage::TaskComplete` to the primary. Pre-P3b the
//! `WorkerEvent::TaskCompleted` destructure in
//! `secondary/processing/worker_event.rs` dropped the bytes via `..`
//! and hardcoded `result_data: None` on the forward message.

#![cfg(test)]

use std::time::Duration;

use dynrunner_core::{MessageReceiver, MessageSender, TaskInfo, WorkerId};
use dynrunner_manager_local::WorkerFactory;
use dynrunner_protocol_manager_worker::{Command, Response};
use dynrunner_protocol_primary_secondary::{
    DistributedBinaryInfo, DistributedMessage, MessageType,
};
use dynrunner_scheduler::ResourceStealingScheduler;
use dynrunner_transport_channel::{channel_pair, ChannelManagerEnd, ChannelPrimaryTransportEnd};
use tokio::sync::mpsc as tokio_mpsc;

use super::super::test_helpers::{FixedEstimator, NoPeers, TestId};
use super::super::{SecondaryConfig, SecondaryCoordinator};

/// Worker factory that replies `Response::Done` with a fixed
/// non-None `result_data` payload for every `ProcessTask`. Used by
/// the propagation test to drive the worker→event→wire chain with a
/// detectable byte pattern that the secondary's outbound message must
/// echo verbatim.
struct PayloadWorkerFactory {
    payload: Vec<u8>,
}

impl WorkerFactory<ChannelManagerEnd> for PayloadWorkerFactory {
    fn spawn_worker(
        &mut self,
        _worker_id: WorkerId,
        _subcgroup: Option<&dynrunner_manager_local::cgroup::SubcgroupHandle>,
    ) -> Result<(ChannelManagerEnd, Option<u32>), String> {
        let (manager_end, runner_end) = channel_pair();
        let payload = self.payload.clone();
        tokio::task::spawn_local(async move {
            let mut runner = runner_end;
            let _ = runner.send(Response::Ready).await;
            loop {
                match MessageReceiver::<Command>::recv(&mut runner).await {
                    Some(Command::Stop) => break,
                    Some(Command::ProcessTask { .. }) => {
                        let _ = runner
                            .send(Response::Done {
                                result_data: Some(payload.clone()),
                            })
                            .await;
                    }
                    None => break,
                }
            }
        });
        Ok((manager_end, None))
    }
}

fn make_binary(name: &str, size: u64) -> TaskInfo<TestId> {
    TaskInfo {
        path: std::path::PathBuf::from(format!("/tmp/{name}")),
        size,
        identifier: TestId(name.into()),
        phase_id: dynrunner_core::PhaseId::from("default"),
        type_id: dynrunner_core::TypeId::from("default"),
        affinity_id: None,
        payload: serde_json::Value::Null,
        task_id: None,
        task_depends_on: vec![],
        preferred_secondaries: dynrunner_core::SoftPreferredSecondaries::default(),
        resolved_path: None,
    }
}

fn extract_worker_id(msg: &DistributedMessage<TestId>) -> WorkerId {
    match msg {
        DistributedMessage::TaskRequest { worker_id, .. } => *worker_id,
        _ => 0,
    }
}

fn send_task_assignment(
    tx: &tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
    secondary_id: &str,
    binary: &TaskInfo<TestId>,
    worker_id: WorkerId,
) {
    let hash = format!("hash_{}", binary.identifier.0);
    tx.send(DistributedMessage::TaskAssignment {
        sender_id: "primary".into(),
        timestamp: 0.0,
        secondary_id: secondary_id.into(),
        worker_id,
        zip_file: None,
        binary_info: DistributedBinaryInfo::from_task_info(binary),
        local_path: binary.path.to_string_lossy().into_owned(),
        file_hash: hash,
        predecessor_outputs: std::collections::BTreeMap::new(),
    })
    .unwrap();
}

/// Drive one task through a real secondary backed by a worker that
/// emits a payload-bearing `Response::Done`. Assert the
/// `DistributedMessage::TaskComplete` the secondary sends to the
/// primary carries the same bytes.
#[tokio::test(flavor = "current_thread")]
async fn worker_event_task_completed_forwards_result_data_to_distributed_message() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (sec_to_pri_tx, sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
            let (pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();

            let transport = ChannelPrimaryTransportEnd {
                tx: sec_to_pri_tx,
                rx: pri_to_sec_rx,
            };

            let config = SecondaryConfig {
                secondary_id: "sec-payload".into(),
                num_workers: 1,
                max_resources: dynrunner_core::ResourceMap::from([(
                    dynrunner_core::ResourceKind::memory(),
                    1024 * 1024 * 1024,
                )]),
                hostname: "test-host".into(),
                keepalive_interval: Duration::from_secs(60),
                src_network: None,
                src_tmp: None,
                peer_timeout: Duration::from_secs(120),
                keepalive_miss_threshold: 3,
                retry_max_passes: 1,
                oom_retry_max_passes: 1,
                primary_link_failure_threshold: 5,
                primary_link_failure_window: Duration::from_secs(30),
                setup_deadline: Duration::from_secs(60),
                is_observer: false,
                resource_check_interval: Duration::from_millis(100),
                log_oom_watcher: false,
                promoted_primary_quiesce_grace: Duration::from_millis(100),
                unfulfillable_reinject_max_per_task: None,
                mem_manager_reserved_bytes: None,
                output_dir: None,
                memuse_log_path: None,
            };

            let binary = make_binary("payload-task", 100);
            let payload: Vec<u8> = b"keyed-output-bytes".to_vec();

            // Drive a minimal fake primary inline: complete the
            // welcome+cert handshake, send empty initial assignment,
            // then respond to one TaskRequest with the binary and
            // collect the resulting TaskComplete.
            let secondary_id = config.secondary_id.clone();
            let to_secondary = pri_to_sec_tx;
            let captured: std::rc::Rc<std::cell::RefCell<Option<DistributedMessage<TestId>>>> =
                std::rc::Rc::new(std::cell::RefCell::new(None));
            let captured_for_task = captured.clone();
            let primary_handle = tokio::task::spawn_local(async move {
                let mut from_secondary = sec_to_pri_rx;
                let mut got_welcome = false;
                let mut got_cert = false;
                while !got_welcome || !got_cert {
                    if let Some(msg) = from_secondary.recv().await {
                        match msg.msg_type() {
                            MessageType::SecondaryWelcome => got_welcome = true,
                            MessageType::CertExchange => got_cert = true,
                            _ => {}
                        }
                    }
                }
                to_secondary
                    .send(DistributedMessage::PeerInfo {
                        sender_id: "primary".into(),
                        timestamp: 0.0,
                        peers: vec![],
                    })
                    .unwrap();
                to_secondary
                    .send(DistributedMessage::InitialAssignment {
                        pre_staged_mode: false,
                        uses_file_based_items: true,
                        sender_id: "primary".into(),
                        timestamp: 0.0,
                        secondary_id: secondary_id.clone(),
                        zip_files: vec![],
                        workers_ready: vec![],
                        staged_files: vec![],
                    })
                    .unwrap();
                to_secondary
                    .send(DistributedMessage::TransferComplete {
                        sender_id: "primary".into(),
                        timestamp: 0.0,
                        total_files: 0,
                        total_bytes: 0,
                    })
                    .unwrap();

                let mut sent_one = false;
                while let Some(msg) = from_secondary.recv().await {
                    match msg.msg_type() {
                        MessageType::TaskRequest if !sent_one => {
                            sent_one = true;
                            send_task_assignment(
                                &to_secondary,
                                &secondary_id,
                                &binary,
                                extract_worker_id(&msg),
                            );
                        }
                        MessageType::TaskComplete => {
                            *captured_for_task.borrow_mut() = Some(msg);
                            // Drop sender so the secondary's run-loop exits.
                            drop(to_secondary);
                            return;
                        }
                        _ => {}
                    }
                }
            });

            let mut secondary = SecondaryCoordinator::new(
                config,
                transport,
                NoPeers,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            let mut factory = PayloadWorkerFactory {
                payload: payload.clone(),
            };
            secondary.run(&mut factory).await.unwrap();
            primary_handle.await.unwrap();

            let msg = captured
                .borrow_mut()
                .take()
                .expect("secondary must have sent a TaskComplete after the payload-bearing Done");
            match msg {
                DistributedMessage::TaskComplete { result_data, .. } => {
                    assert_eq!(
                        result_data,
                        Some(payload),
                        "result_data bytes must survive the worker->WorkerEvent->\
                         DistributedMessage::TaskComplete chain in worker_event.rs; \
                         a drop here means P3b plumbing regressed at the \
                         secondary's destructure-and-reconstruct site"
                    );
                }
                other => panic!(
                    "expected TaskComplete, got {:?}",
                    other.msg_type()
                ),
            }
        })
        .await;
}
