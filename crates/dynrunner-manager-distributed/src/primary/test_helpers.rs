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

/// Worker factory that fails the first N Recoverable attempts on each
/// task whose `relative_path` is in `failure_quotas`, then succeeds.
/// Tasks not in the map always succeed. Shared state is a single
/// `Rc<RefCell<HashMap<String, u32>>>` for the per-task attempt
/// counter so multiple worker subprocesses (when num_workers > 1)
/// share one ledger.
///
/// Single concern: deterministically translate `(task path, attempt#)`
/// into success-or-Recoverable, regardless of which worker drew the
/// assignment. Set a quota of `u32::MAX` for "always fails" coverage.
///
/// Single-threaded by construction (uses `Rc`/`RefCell`); only safe
/// inside a `tokio::task::LocalSet`. Pairs with the in-process
/// channel-transport tests.
#[derive(Clone)]
pub(super) struct FlakyWorkerFactory {
    pub(super) attempts: std::rc::Rc<std::cell::RefCell<HashMap<String, u32>>>,
    pub(super) failure_quotas: std::rc::Rc<HashMap<String, u32>>,
}

impl FlakyWorkerFactory {
    /// Build a factory whose worker fails the first
    /// `failure_quotas[relative_path]` attempts of each named task,
    /// succeeding from the (quota+1)-th attempt onwards. Tasks not
    /// in the map succeed unconditionally.
    pub(super) fn with_quotas(failure_quotas: HashMap<String, u32>) -> Self {
        Self {
            attempts: std::rc::Rc::new(std::cell::RefCell::new(HashMap::new())),
            failure_quotas: std::rc::Rc::new(failure_quotas),
        }
    }
}

impl WorkerFactory<ChannelManagerEnd> for FlakyWorkerFactory {
    fn spawn_worker(
        &mut self,
        _worker_id: u32,
    ) -> Result<(ChannelManagerEnd, Option<u32>), String> {
        let (manager_end, runner_end) = channel_pair();
        let attempts = self.attempts.clone();
        let quotas = self.failure_quotas.clone();
        tokio::task::spawn_local(async move {
            let mut runner = runner_end;
            let _ = runner.send(Response::Ready).await;
            loop {
                match MessageReceiver::<Command>::recv(&mut runner).await {
                    Some(Command::Stop) => break,
                    Some(Command::ProcessTask { relative_path, .. }) => {
                        // Per-task attempt counter, shared across
                        // workers via Rc<RefCell>. Increment first
                        // so attempt #1 is the first worker
                        // invocation.
                        let attempt = {
                            let mut map = attempts.borrow_mut();
                            let n = map.entry(relative_path.clone()).or_insert(0);
                            *n += 1;
                            *n
                        };
                        let quota = quotas.get(&relative_path).copied().unwrap_or(0);
                        let response = if attempt <= quota {
                            Response::Error {
                                error_type: dynrunner_core::ErrorType::Recoverable,
                                message: format!(
                                    "synthetic recoverable failure on attempt {attempt} (quota: {quota})"
                                ),
                            }
                        } else {
                            Response::Done { result_data: None }
                        };
                        let _ = runner.send(response).await;
                    }
                    None => break,
                }
            }
        });
        Ok((manager_end, None))
    }
}

/// Simulate a secondary that sends welcome + cert, then echoes
/// assignments as completions. Convenience wrapper around
/// [`fake_secondary_with_addrs`] using the historical
/// `(ipv4=127.0.0.1, ipv6=None)` defaults — kept so existing tests
/// don't have to thread address arguments they don't care about.
pub(super) async fn fake_secondary(
    secondary_id: String,
    num_workers: u32,
    ram_bytes: u64,
    incoming_from_primary: tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
    outgoing_to_primary: tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
) {
    fake_secondary_with_addrs(
        secondary_id,
        num_workers,
        ram_bytes,
        Some("127.0.0.1".into()),
        None,
        incoming_from_primary,
        outgoing_to_primary,
    )
    .await
}

/// Like [`fake_secondary`] but parametrised on the `(ipv4, ipv6)` pair
/// the secondary advertises in its `CertExchange`. Used by tests that
/// inspect the primary-side `PeerInfo` broadcast: dropping `ipv6` here
/// at the typestate level was the cause of an empty
/// happy-eyeballs candidate set for every cross-secondary dial, so
/// the round-trip through `handle_cert_exchange` →
/// `SecondaryConnectionState` → `peer_setup` is the load-bearing path
/// to pin.
///
/// Also stands in for the secondary-side SLURM-primary post-handoff:
/// the real local primary now demotes itself the moment it sends
/// `PromotePrimary` and stops dispatching, so the fake — when promoted
/// via `PromotePrimary { new_primary_id == self }` — drains the
/// `FullTaskList.pending_tasks` payload by emitting `TaskComplete` for
/// each entry. Without that drain, tests that rely on more binaries
/// than fit in the initial assignment would hang waiting for
/// completions that no longer come from the local primary.
pub(super) async fn fake_secondary_with_addrs(
    secondary_id: String,
    num_workers: u32,
    ram_bytes: u64,
    ipv4: Option<String>,
    ipv6: Option<String>,
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
            ipv4_address: ipv4,
            ipv6_address: ipv6,
            quic_port: 5000,
        })
        .unwrap();

    // Mirror the real secondary's behaviour: as soon as the
    // peer-mesh is settled (or there are no peers — which is the
    // default for the in-process tests), report MeshReady so the
    // primary's `wait_for_mesh_ready` step doesn't have to time out
    // before promoting SLURM-primary. Fired pre-emptively here
    // because the in-process fake doesn't model peer-dial latency.
    outgoing_to_primary
        .send(DistributedMessage::MeshReady {
            sender_id: secondary_id.clone(),
            timestamp: 0.0,
            secondary_id: secondary_id.clone(),
            peer_count: 0,
        })
        .unwrap();

    // Track whether this fake is the secondary-side SLURM-primary
    // (set by `PromotePrimary` if `new_primary_id` matches our id).
    // Only the SLURM-primary drains the post-handoff `pending_tasks`
    // list from `FullTaskList`; non-promoted secondaries ignore it
    // (in production they only consult their cache for failover).
    let mut is_slurm_primary = false;
    while let Some(msg) = incoming_from_primary.recv().await {
        match msg {
            DistributedMessage::PeerInfo { .. } => {}
            DistributedMessage::PromotePrimary { new_primary_id, .. } => {
                if new_primary_id == secondary_id {
                    is_slurm_primary = true;
                }
            }
            DistributedMessage::InitialAssignment {
                zip_files,
                workers_ready,
                ..
            } => {
                // Pair each binary with the worker that the primary's
                // `assign_initial` placed it on. `workers_ready[i]`
                // and `zip_files[0].binaries[i]` are positionally
                // aligned in `perform_initial_assignment`. Without
                // this pairing every TaskComplete would carry
                // `worker_id=0`, which after demotion is no longer
                // self-healed by the heartbeat-driven requeue and
                // leaves later workers permanently mid-dispatch.
                let entries: Vec<_> = zip_files
                    .iter()
                    .flat_map(|zf| zf.binaries.iter())
                    .collect();
                for (idx, entry) in entries.iter().enumerate() {
                    let worker_id = workers_ready
                        .get(idx)
                        .map(|w| w.worker_id)
                        .unwrap_or(0);
                    outgoing_to_primary
                        .send(DistributedMessage::TaskComplete {
                            sender_id: secondary_id.clone(),
                            timestamp: 0.0,
                            secondary_id: secondary_id.clone(),
                            worker_id,
                            task_hash: entry.hash.clone(),
                            result_data: None,
                        })
                        .unwrap();

                    outgoing_to_primary
                        .send(DistributedMessage::TaskRequest {
                            sender_id: secondary_id.clone(),
                            timestamp: 0.0,
                            secondary_id: secondary_id.clone(),
                            worker_id,
                            available_resources: vec![dynrunner_core::ResourceAmount {
                                kind: dynrunner_core::ResourceKind::memory(),
                                amount: ram_bytes,
                            }],
                        })
                        .unwrap();
                }
            }
            DistributedMessage::TransferComplete { .. } => {}
            // Stand in for the secondary-side SLURM-primary: when
            // the real primary broadcasts `FullTaskList`, every task
            // that wasn't already in-flight (`pending_tasks`) is the
            // SLURM-primary's responsibility. The real
            // SecondaryCoordinator drains those via its
            // `slurm_pending` self-dispatch path; the fake
            // short-circuits and emits TaskComplete for each so the
            // counter-check exit can fire.
            DistributedMessage::FullTaskList { pending_tasks, .. } => {
                if is_slurm_primary {
                    for task_hash in pending_tasks {
                        outgoing_to_primary
                            .send(DistributedMessage::TaskComplete {
                                sender_id: secondary_id.clone(),
                                timestamp: 0.0,
                                secondary_id: secondary_id.clone(),
                                worker_id: 0,
                                task_hash,
                                result_data: None,
                            })
                            .unwrap();
                    }
                }
            }
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
