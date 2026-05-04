//! Shared test fixtures for primary-coordinator tests. Compiled only
//! under `#[cfg(test)]` so they never enter the production binary.

use std::collections::HashMap;

use dynrunner_core::{TaskInfo, Identifier, MessageReceiver, MessageSender, PhaseId, TypeId};
use dynrunner_manager_local::WorkerFactory;
use dynrunner_protocol_manager_worker::{Command, Response};
use dynrunner_protocol_primary_secondary::{DistributedMessage, PeerConnectionInfo, PeerTransport};
use dynrunner_scheduler_api::ResourceEstimator;
use dynrunner_transport_channel::{
    channel_pair, ChannelManagerEnd, ChannelSecondaryTransportEnd,
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc as tokio_mpsc;

/// Minimal serializable identifier used by every primary test.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(super) struct TestId(pub String);

/// Estimator that returns the same fixed memory amount for every binary.
#[derive(Clone)]
pub(super) struct FixedEstimator(pub u64);

impl ResourceEstimator<TestId> for FixedEstimator {
    fn estimate(&self, _task: &TaskInfo<TestId>) -> dynrunner_core::ResourceMap {
        dynrunner_core::ResourceMap::from([(dynrunner_core::ResourceKind::memory(), self.0)])
    }
}

pub(super) fn make_binary(name: &str, size: u64) -> TaskInfo<TestId> {
    // Absolute path (despite no real file backing it) — the in-process
    // test fixtures don't configure src_network, and dispatch.rs's
    // unresolvable-task guard fail-loud-rejects relative local_paths
    // when the secondary has no staging dir (since they cannot be
    // resolved by the worker without one). Tests that only exercise
    // the dispatch wire flow (fake worker doesn't actually open the
    // file) are happy with any absolute path; using `/tmp/<name>`
    // keeps the fixture trivial and survives that guard.
    TaskInfo {
        path: std::path::PathBuf::from(format!("/tmp/{name}")),
        size,
        identifier: TestId(name.into()),
        phase_id: PhaseId::from("default"),
        type_id: TypeId::from("default"),
        affinity_id: None,
        payload: serde_json::Value::Null,
    }
}

/// PeerTransport that drops every message and never produces input.
pub(super) struct NoPeers;

impl<I: Identifier> PeerTransport<I> for NoPeers {
    async fn broadcast(&mut self, _msg: DistributedMessage<I>) -> Result<(), String> {
        Ok(())
    }
    async fn send_to_peer(
        &mut self,
        _peer_id: &str,
        _msg: DistributedMessage<I>,
    ) -> Result<(), String> {
        Ok(())
    }
    async fn recv_peer(&mut self) -> Option<DistributedMessage<I>> {
        std::future::pending().await
    }
    fn try_recv_peer(&mut self) -> Option<DistributedMessage<I>> {
        None
    }
    fn peer_count(&self) -> usize {
        0
    }
    async fn connect_to_peers(&mut self, _peers: &[PeerConnectionInfo]) {}
}

/// Factory that spawns fake workers via channel transport.
pub(super) struct FakeWorkerFactory;

impl WorkerFactory<ChannelManagerEnd> for FakeWorkerFactory {
    fn spawn_worker(
        &mut self,
        _worker_id: u32,
    ) -> Result<(ChannelManagerEnd, Option<u32>), String> {
        let (manager_end, runner_end) = channel_pair();
        tokio::task::spawn_local(async move {
            let mut runner = runner_end;
            let _ = runner.send(Response::Ready).await;
            loop {
                match MessageReceiver::<Command>::recv(&mut runner).await {
                    Some(Command::Stop) => break,
                    Some(Command::ProcessTask { .. }) => {
                        let _ = runner.send(Response::Done { result_data: None }).await;
                    }
                    None => break,
                }
            }
        });
        Ok((manager_end, None))
    }
}

/// Simulate a secondary that sends welcome + cert, then echoes
/// assignments as completions.
pub(super) async fn fake_secondary(
    secondary_id: String,
    num_workers: u32,
    ram_bytes: u64,
    mut incoming_from_primary: tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
    outgoing_to_primary: tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
) {
    outgoing_to_primary
        .send(DistributedMessage::SecondaryWelcome {
            sender_id: secondary_id.clone(),
            timestamp: 0.0,
            secondary_id: secondary_id.clone(),
            resources: vec![dynrunner_core::ResourceAmount {
                kind: dynrunner_core::ResourceKind::memory(),
                amount: ram_bytes,
            }],
            worker_count: num_workers,
            hostname: "test-host".into(),
        })
        .unwrap();

    outgoing_to_primary
        .send(DistributedMessage::CertExchange {
            sender_id: secondary_id.clone(),
            timestamp: 0.0,
            secondary_id: secondary_id.clone(),
            public_cert_pem: "FAKE_CERT".into(),
            ipv4_address: Some("127.0.0.1".into()),
            ipv6_address: None,
            quic_port: 5000,
        })
        .unwrap();

    while let Some(msg) = incoming_from_primary.recv().await {
        match msg {
            DistributedMessage::PeerInfo { .. } => {}
            DistributedMessage::InitialAssignment { zip_files, .. } => {
                for zip_file in &zip_files {
                    for entry in &zip_file.binaries {
                        outgoing_to_primary
                            .send(DistributedMessage::TaskComplete {
                                sender_id: secondary_id.clone(),
                                timestamp: 0.0,
                                secondary_id: secondary_id.clone(),
                                worker_id: 0,
                                task_hash: entry.hash.clone(),
                                result_data: None,
                            })
                            .unwrap();

                        outgoing_to_primary
                            .send(DistributedMessage::TaskRequest {
                                sender_id: secondary_id.clone(),
                                timestamp: 0.0,
                                secondary_id: secondary_id.clone(),
                                worker_id: 0,
                                available_resources: vec![dynrunner_core::ResourceAmount {
                                    kind: dynrunner_core::ResourceKind::memory(),
                                    amount: ram_bytes,
                                }],
                            })
                            .unwrap();
                    }
                }
            }
            DistributedMessage::TransferComplete { .. } => {}
            DistributedMessage::TaskAssignment { file_hash, .. } => {
                outgoing_to_primary
                    .send(DistributedMessage::TaskComplete {
                        sender_id: secondary_id.clone(),
                        timestamp: 0.0,
                        secondary_id: secondary_id.clone(),
                        worker_id: 0,
                        task_hash: file_hash,
                        result_data: None,
                    })
                    .unwrap();

                outgoing_to_primary
                    .send(DistributedMessage::TaskRequest {
                        sender_id: secondary_id.clone(),
                        timestamp: 0.0,
                        secondary_id: secondary_id.clone(),
                        worker_id: 0,
                        available_resources: vec![dynrunner_core::ResourceAmount {
                            kind: dynrunner_core::ResourceKind::memory(),
                            amount: ram_bytes,
                        }],
                    })
                    .unwrap();
            }
            _ => {}
        }
    }
}

/// Allocate the channel-pairs for `num_secondaries` and return the
/// primary's `ChannelSecondaryTransportEnd` plus per-secondary
/// (id, secondary→primary inbox, secondary→primary outbox) tuples
/// that the test plumbs into `fake_secondary` (or a real
/// SecondaryCoordinator via `spawn_real_secondary` in the
/// `e2e_helpers` companion).
pub(super) fn setup_test(
    num_secondaries: u32,
) -> (
    ChannelSecondaryTransportEnd<TestId>,
    Vec<(
        String,
        tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
        tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
    )>,
) {
    let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
    let mut outgoing = HashMap::new();
    let mut secondary_ends = Vec::new();

    for i in 0..num_secondaries {
        let id = format!("sec-{i}");
        let (to_sec_tx, to_sec_rx) = tokio_mpsc::unbounded_channel();
        outgoing.insert(id.clone(), to_sec_tx);
        secondary_ends.push((id, to_sec_rx, incoming_tx.clone()));
    }

    (
        ChannelSecondaryTransportEnd {
            outgoing,
            incoming_rx,
        },
        secondary_ends,
    )
}
