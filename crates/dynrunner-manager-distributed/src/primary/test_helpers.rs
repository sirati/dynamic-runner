//! Shared test fixtures for primary-coordinator tests. Compiled only
//! under `#[cfg(test)]` so they never enter the production binary.

use std::collections::{HashMap, HashSet};

use dynrunner_core::{
    Identifier, MessageReceiver, MessageSender, PhaseId, SoftPreferredSecondaries, TaskInfo, TypeId,
};
use dynrunner_manager_local::WorkerFactory;
use dynrunner_protocol_manager_worker::{Command, Response};
use dynrunner_protocol_primary_secondary::address::Destination;
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, DistributedMessage, PeerConnectionInfo, PeerId, PeerTransport,
};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};
use dynrunner_transport_channel::{ChannelManagerEnd, ChannelPeerTransport, channel_pair};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::mpsc as tokio_mpsc;

use crate::primary::{PrimaryConfig, PrimaryCoordinator};
use crate::process::{LocalRole, Mesh, RoleSlot};

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

/// Build a `TaskInfo` whose `path` is RELATIVE. Pairs with the
/// staging-coverage tests: dispatch's `report_unresolvable_task`
/// gate fires on `local_path_is_relative=true && src_network=None`,
/// so a relative-path binary is the wire shape that exercises the
/// "primary forgot to queue StageFile" failure mode the in-process
/// distributed pipeline regressed into.
///
/// Placed next to `make_binary` instead of inlined in the test
/// because both regression tests (T1 — failure pin; T2 — fix
/// validation) share the exact same binary shape; centralising
/// keeps them in lockstep.
pub(super) fn make_relative_binary(name: &str, size: u64) -> TaskInfo<TestId> {
    TaskInfo {
        path: std::path::PathBuf::from(name),
        size,
        identifier: TestId(name.into()),
        phase_id: PhaseId::from("default"),
        type_id: TypeId::from("default"),
        affinity_id: None,
        payload: serde_json::Value::Null,
        task_id: name.into(),
        task_depends_on: vec![],
        preferred_secondaries: SoftPreferredSecondaries::default(),
        preferred_version: Default::default(),
        resolved_path: None,
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
        task_id: name.into(),
        task_depends_on: vec![],
        preferred_secondaries: SoftPreferredSecondaries::default(),
        preferred_version: Default::default(),
        resolved_path: None,
    }
}

/// PeerTransport that records every outbound message into a shared log
/// instead of dropping it (the [`NoPeers`] behaviour). Lets a test
/// assert that an emission (e.g. the primary keepalive) was actually
/// issued over the peer transport, without standing up a real mesh.
///
/// Both `broadcast` and `send_to_peer` append to the same log: the
/// keepalive emitter routes through `send(Address::Broadcast(
/// AllSecondaries), ..)`, whose default trait impl delegates to
/// `broadcast`, but recording both keeps the helper honest if a future
/// emission switches to unicast. `recv_peer` parks forever so the
/// recorder never closes the peer arm.
///
/// Single-threaded (`Rc`/`RefCell`); only safe inside a
/// `tokio::task::LocalSet` / `current_thread` runtime, like every other
/// fixture in this module.
pub(super) struct RecordingPeer<I: Identifier> {
    pub(super) broadcasts: std::rc::Rc<std::cell::RefCell<Vec<DistributedMessage<I>>>>,
}

impl<I: Identifier> RecordingPeer<I> {
    pub(super) fn new() -> Self {
        Self {
            broadcasts: std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
        }
    }

    /// Clone of the shared log handle. The recorder is moved into
    /// `PrimaryCoordinator::new`, so the test grabs this before the move.
    pub(super) fn log_handle(&self) -> std::rc::Rc<std::cell::RefCell<Vec<DistributedMessage<I>>>> {
        self.broadcasts.clone()
    }
}

impl<I: Identifier> PeerTransport<I> for RecordingPeer<I> {
    async fn broadcast(&mut self, msg: DistributedMessage<I>) -> Result<(), String> {
        self.broadcasts.borrow_mut().push(msg);
        Ok(())
    }
    async fn send_to_peer(
        &mut self,
        _peer_id: &str,
        msg: DistributedMessage<I>,
    ) -> Result<(), String> {
        self.broadcasts.borrow_mut().push(msg);
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
    fn has_peer(&self, _id: &PeerId) -> bool {
        // Records outbound sends but models no connected peers
        // (`peer_count == 0`); every id is a non-member.
        false
    }
    async fn connect_to_peers(&mut self, _peers: &[PeerConnectionInfo]) {}
}

/// PeerTransport that drops every message and never produces input.
// Unused since `spawn_real_secondary*` moved to the channel-backed mesh
// harness (`channel_mesh_secondary_ends`); kept as a drop-everything
// stub for ad-hoc isolation tests.
#[allow(dead_code)]
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
    fn has_peer(&self, _id: &PeerId) -> bool {
        // Models no peers — every id is a non-member. Consistent with
        // `peer_count == 0`.
        false
    }
    async fn connect_to_peers(&mut self, _peers: &[PeerConnectionInfo]) {}
}

/// Factory that spawns fake workers via channel transport. Drives the
/// real secondaries the channel-backed mesh harness stands up
/// (`spawn_real_secondary`).
#[allow(dead_code)]
pub(super) struct FakeWorkerFactory;

impl WorkerFactory<ChannelManagerEnd> for FakeWorkerFactory {
    fn spawn_worker(
        &mut self,
        _worker_id: u32,
        _subcgroup: Option<&dynrunner_manager_local::cgroup::SubcgroupHandle>,
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

/// Worker factory whose per-task latency is driven by a substring match
/// on `relative_path`. Tasks whose `relative_path` contains any of the
/// `slow_markers` keys sleep for the matched value before responding
/// `Done`; all others respond instantly.
///
/// Single concern: per-task synthetic latency. The marker matching is
/// substring-based on the wire path (`TaskInfo.path.to_string_lossy()`)
/// so callers can drive timing entirely from `make_binary("slow_X", _)`-
/// style fixture names with no extra plumbing into the wire shape.
///
/// Single-threaded (`Rc`); only safe inside a `tokio::task::LocalSet`.
// Drives `spawn_real_secondary_slow` in the channel-backed mesh harness.
#[allow(dead_code)]
#[derive(Clone)]
pub(super) struct SlowFakeWorkerFactory {
    slow_markers: std::rc::Rc<Vec<(String, std::time::Duration)>>,
}

#[allow(dead_code)]
impl SlowFakeWorkerFactory {
    pub(super) fn with_markers(slow_markers: Vec<(String, std::time::Duration)>) -> Self {
        Self {
            slow_markers: std::rc::Rc::new(slow_markers),
        }
    }
}

impl WorkerFactory<ChannelManagerEnd> for SlowFakeWorkerFactory {
    fn spawn_worker(
        &mut self,
        _worker_id: u32,
        _subcgroup: Option<&dynrunner_manager_local::cgroup::SubcgroupHandle>,
    ) -> Result<(ChannelManagerEnd, Option<u32>), String> {
        let (manager_end, runner_end) = channel_pair();
        let markers = self.slow_markers.clone();
        tokio::task::spawn_local(async move {
            let mut runner = runner_end;
            let _ = runner.send(Response::Ready).await;
            loop {
                match MessageReceiver::<Command>::recv(&mut runner).await {
                    Some(Command::Stop) => break,
                    Some(Command::ProcessTask { relative_path, .. }) => {
                        let delay = markers
                            .iter()
                            .find(|(needle, _)| relative_path.contains(needle))
                            .map(|(_, d)| *d)
                            .unwrap_or_else(|| std::time::Duration::from_millis(0));
                        if !delay.is_zero() {
                            tokio::time::sleep(delay).await;
                        }
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
        _subcgroup: Option<&dynrunner_manager_local::cgroup::SubcgroupHandle>,
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
/// Also stands in for the secondary-side primary post-handoff:
/// the real local primary now demotes itself the moment it originates
/// `PrimaryChanged` and stops dispatching, so the fake — when named
/// primary via `ClusterMutation::PrimaryChanged { new == self }` —
/// drains every task hash still tracked as Pending in its replicated
/// `cluster_state` mirror by emitting `TaskComplete` for each. The
/// mirror is fed by
/// `ClusterMutation::TaskAdded` (entry) /
/// `ClusterMutation::TaskCompleted | TaskFailed` (terminal) broadcasts
/// the same way the real `SecondaryCoordinator` ingests them; the
/// fake drains the Pending set on promotion so tests that rely on
/// more binaries than fit in the initial assignment don't hang
/// waiting for completions the local primary no longer issues.
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
            target: Some(Destination::Primary),
            sender_id: secondary_id.clone(),
            timestamp: 0.0,
            secondary_id: secondary_id.clone(),
            resources: vec![dynrunner_core::ResourceAmount {
                kind: dynrunner_core::ResourceKind::memory(),
                amount: ram_bytes,
            }],
            worker_count: num_workers,
            hostname: "test-host".into(),
            is_observer: false,
            can_be_primary: false,
        })
        .unwrap();

    outgoing_to_primary
        .send(DistributedMessage::CertExchange {
            target: Some(Destination::Primary),
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
    // before promoting primary. Fired pre-emptively here
    // because the in-process fake doesn't model peer-dial latency.
    outgoing_to_primary
        .send(DistributedMessage::MeshReady {
            target: Some(Destination::Primary),
            sender_id: secondary_id.clone(),
            timestamp: 0.0,
            secondary_id: secondary_id.clone(),
            peer_count: 0,
        })
        .unwrap();

    // Replicated `cluster_state` mirror, reduced to "hashes still
    // Pending from this fake's view". Fed by `ClusterMutation::TaskAdded`
    // (insertion) and removed on `TaskCompleted` / `TaskFailed` /
    // self-emitted TaskComplete. On promotion the residual set is
    // drained by emitting TaskComplete for each entry — the same
    // post-handoff drain the pre-Phase-B FullTaskList path used to
    // perform, now driven off of the replicated ledger.
    let mut pending_hashes: HashSet<String> = HashSet::new();
    while let Some(msg) = incoming_from_primary.recv().await {
        match msg {
            DistributedMessage::PeerInfo { .. } => {}
            DistributedMessage::ClusterMutation { mutations, .. } => {
                // Mirror the live cluster ledger: TaskAdded enters
                // pending, TaskCompleted / TaskFailed terminates.
                // `PrimaryChanged { new = self }` is the unified
                // primary-activation frame: when it names this fake,
                // drain the residual Pending set — every hash still
                // tracked here is the new primary's responsibility
                // post-handoff — by emitting TaskComplete for each so
                // the primary's counter-check exit can fire. TaskAssigned
                // is a no-op here; the fake's drain only cares about
                // Pending vs terminal.
                for m in mutations {
                    match m {
                        ClusterMutation::TaskAdded { hash, .. } => {
                            pending_hashes.insert(hash);
                        }
                        ClusterMutation::TaskCompleted { hash, .. }
                        | ClusterMutation::TaskFailed { hash, .. } => {
                            pending_hashes.remove(&hash);
                        }
                        ClusterMutation::PrimaryChanged { new, .. } if new == secondary_id => {
                            for task_hash in pending_hashes.drain() {
                                outgoing_to_primary
                                    .send(DistributedMessage::TaskComplete {
                                        target: Some(Destination::Primary),
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
                        _ => {}
                    }
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
                let entries: Vec<_> = zip_files.iter().flat_map(|zf| zf.binaries.iter()).collect();
                for (idx, entry) in entries.iter().enumerate() {
                    let worker_id = workers_ready.get(idx).map(|w| w.worker_id).unwrap_or(0);
                    pending_hashes.remove(&entry.hash);
                    outgoing_to_primary
                        .send(DistributedMessage::TaskComplete {
                            target: Some(Destination::Primary),
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
                            target: Some(Destination::Primary),
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
            DistributedMessage::TaskAssignment { file_hash, .. } => {
                pending_hashes.remove(&file_hash);
                outgoing_to_primary
                    .send(DistributedMessage::TaskComplete {
                        target: Some(Destination::Primary),
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
                        target: Some(Destination::Primary),
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

/// Keeps the primary role-slot `Arc` + the demote sender + the running
/// production mesh-pump alive for as long as the [`PrimaryCoordinator`] built
/// by [`build_test_primary`] lives.
///
/// In production the [`crate::process::Node`] owns the `Mesh`, the
/// `Arc<RoleSlot>`, and the demote channel, and runs the mesh-pump that
/// drains the coordinator's queued egress + feeds its inbox. This harness
/// reproduces exactly that turn for the in-process primary tests: it spawns
/// the PRODUCTION [`crate::process::pump::run_pump`] (the pump OWNS the
/// `Mesh`), so a `primary.run(..)`'s queued sends reach the wire and inbound
/// frames reach the primary slot — TRUE e2e against the production pump, not
/// a test-double. Hold it for the coordinator's lifetime (bind to `_mesh` at
/// the call site); dropping it closes the control channel + drops the slot,
/// tearing the mesh out from under the coordinator.
pub(super) struct PrimaryMeshKeepalive {
    /// The primary's slot `Arc`, held ONLY on the no-pump (sync-test) path —
    /// on the pump path the pump task owns it (so it drops on wire-close).
    _slot: Option<Arc<RoleSlot<TestId>>>,
    _demote_tx: tokio_mpsc::UnboundedSender<()>,
    /// Held so the pump's control arm stays open for the run's lifetime
    /// (only present when a pump was spawned).
    _control: Option<crate::process::MeshControlHandle<TestId>>,
    /// The spawned pump task — aborted on drop so it does not outlive the run.
    /// `None` for a SYNC unit test (no tokio runtime): there the mesh is held
    /// idle below and no pump runs (the coordinator's queued egress simply
    /// accumulates, harmlessly, because such tests never drive a wire round
    /// trip — they inspect the coordinator's in-memory state directly).
    pump: Option<tokio::task::JoinHandle<()>>,
    /// When no pump was spawned (sync test), the mesh is parked here so its
    /// egress-queue receiver stays alive (a queued `client.send` must not
    /// error as "pump dropped"). `None` once the mesh was moved into a pump.
    _mesh: Option<Mesh<TestId, ChannelPeerTransport<TestId>>>,
}

impl Drop for PrimaryMeshKeepalive {
    fn drop(&mut self) {
        if let Some(h) = self.pump.take() {
            h.abort();
        }
    }
}

/// Mint the primary's mesh capability trio from a test `Mesh` (mirroring
/// C0's `process/tests`), build a [`PrimaryCoordinator`] over them, and spawn
/// the PRODUCTION mesh-pump over the mesh.
///
/// Returns the coordinator plus the [`PrimaryMeshKeepalive`] guard the
/// caller MUST keep alive alongside it (`let (mut primary, _mesh) = …`).
/// This is the single construction choke point the tests share so the
/// `register_local_role` → `new(client, inbox, demote_rx, …)` wiring + the
/// pump spawn live in ONE place. Because the pump is the real
/// [`crate::process::pump::run_pump`], a test that drives wire traffic
/// through `primary.run(..)` runs against the genuine production turn — these
/// are the e2e tests C-NODE re-enabled. MUST be called inside a
/// `tokio::task::LocalSet` (the pump is `spawn_local`'d).
pub(super) fn build_test_primary<S, E>(
    config: PrimaryConfig,
    transport: ChannelPeerTransport<TestId>,
    scheduler: S,
    estimator: E,
) -> (PrimaryCoordinator<S, E, TestId>, PrimaryMeshKeepalive)
where
    S: Scheduler<TestId> + 'static,
    E: ResourceEstimator<TestId> + 'static,
{
    let mut mesh = Mesh::new(transport);
    let (slot, client, inbox) =
        mesh.register_local_role(LocalRole::Primary, PeerId::from(config.node_id.as_str()));
    let (demote_tx, demote_rx) = tokio_mpsc::unbounded_channel();
    let primary = PrimaryCoordinator::new(config, client, inbox, demote_rx, scheduler, estimator);
    // Publish live membership before the pump spawns so the primary's
    // failover/strand reads see the connected secondaries from the first tick
    // (the pump republishes every cycle thereafter).
    mesh.publish_membership();
    // Spawn the production pump (sole mesh owner) ONLY when there is a tokio
    // runtime to spawn into — i.e. the e2e/wire-driving tests, which all run
    // inside a `LocalSet::run_until`. SYNC unit tests (`#[test]`) that just
    // build a coordinator and inspect its in-memory state have no runtime; for
    // them we park the mesh idle in the keepalive (so the egress queue's
    // receiver stays alive) and skip the pump — they never drive a wire round
    // trip, so a queued `client.send` simply accumulates harmlessly.
    if tokio::runtime::Handle::try_current().is_ok() {
        let (control, control_rx) = crate::process::pump::control_channel::<TestId>();
        // The pump task OWNS the primary's slot `Arc` for the pump's lifetime:
        // while the pump runs the mesh `Weak` upgrades (inbound reaches the
        // primary); when the pump EXITS on wire-close (`recv_peer → None`) it
        // drops the `Arc`, so the slot's inbound sender drops and the primary's
        // `inbox.recv()` returns `None` — the cluster-collapse signal the
        // operational loop detects. (In production the `Node` owns this
        // wire-close→teardown coupling; this harness reproduces it.)
        let pump = tokio::task::spawn_local(async move {
            let _slot = slot;
            crate::process::pump::run_pump(mesh, control_rx).await;
        });
        (
            primary,
            PrimaryMeshKeepalive {
                _slot: None,
                _demote_tx: demote_tx,
                _control: Some(control),
                pump: Some(pump),
                _mesh: None,
            },
        )
    } else {
        (
            primary,
            PrimaryMeshKeepalive {
                _slot: Some(slot),
                _demote_tx: demote_tx,
                _control: None,
                pump: None,
                _mesh: Some(mesh),
            },
        )
    }
}

/// Allocate the channel-pairs for `num_secondaries` and return the
/// primary's single `ChannelPeerTransport` plus per-secondary
/// (id, secondary→primary inbox, secondary→primary outbox) tuples
/// that the test plumbs into `fake_secondary` (or a real
/// SecondaryCoordinator via `spawn_real_secondary` in the
/// `e2e_helpers` companion).
///
/// Post-collapse the primary holds ONE `Tr: PeerTransport`. The fake
/// secondaries still drive raw `DistributedMessage` channels (they are
/// hand-rolled, not real `ChannelPeerTransport`s); the primary's
/// transport is built from those raw channels via
/// `ChannelPeerTransport::from_raw_channels` so its `send_to_peer(id)`
/// reaches the matching fake's inbox and its `recv_peer()` drains the
/// aggregated inbound the fakes write to. THIS is the migration the
/// send-collapse needs: workload now flows over the peer transport, not
/// the deleted `ChannelSecondaryTransportEnd` handle.
// One-off test-helper return; the tuple shape is documented by the
// per-element doc above and isn't reused elsewhere.
#[allow(clippy::type_complexity)]
pub(super) fn setup_test(
    num_secondaries: u32,
) -> (
    ChannelPeerTransport<TestId>,
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
        ChannelPeerTransport::from_raw_channels("primary".into(), outgoing, incoming_rx),
        secondary_ends,
    )
}
