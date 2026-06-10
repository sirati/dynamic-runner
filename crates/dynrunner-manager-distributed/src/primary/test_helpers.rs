//! Shared test fixtures for primary-coordinator tests. Compiled only
//! under `#[cfg(test)]` so they never enter the production binary.

use std::collections::{HashMap, HashSet};

use dynrunner_core::{
    Identifier, MessageReceiver, MessageSender, PhaseId, SETUP_NODE_ID, SoftPreferredSecondaries,
    TaskInfo, TypeId,
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

/// One scripted per-task behaviour for [`ScriptedWorkerFactory`], selected by
/// substring match on the wire `relative_path` (same selection scheme as
/// [`SlowFakeWorkerFactory`]'s markers).
#[derive(Clone)]
pub(super) enum WorkerScript {
    /// Sleep `delay`, then respond `Response::Done` (a successful task).
    Done { delay: std::time::Duration },
    /// Sleep `delay`, then respond `Response::Error { error_type, message }`.
    /// The worker protocol resolves an `Error` response to
    /// `PollResult::Disconnected` (needs restart), so the pool surfaces it as
    /// `WorkerEvent::Disconnected { result, binary: Some(..) }` — the
    /// disconnect-with-error shape (the nix-build-failure / #341 class) —
    /// and the secondary respawns the slot.
    Error {
        delay: std::time::Duration,
        error_type: dynrunner_core::ErrorType,
        message: String,
    },
}

/// Worker factory driven by per-`relative_path` substring scripts, with a
/// CROSS-THREAD run-count + spawn-count ledger.
///
/// Generalises the timing-only [`SlowFakeWorkerFactory`] and the
/// Recoverable-only [`FlakyWorkerFactory`]: a scenario that needs latency AND
/// worker-protocol errors AND per-task run accounting in ONE fleet-wide
/// factory uses this. The ledgers are `Arc<Mutex<..>>` / `Arc<AtomicU32>`
/// (not `Rc<RefCell<..>>`) deliberately: the producer-backstop failover
/// scenario runs one node on a SEPARATE thread/runtime (the kill target), and
/// its workers must record into the same ledger as the main-thread fleet's.
///
/// `Clone` shares the ledgers + scripts (construct once, clone per node), so
/// `run_counts` is the fleet-wide "how many times did each task actually run
/// on a worker" oracle — the redo/no-redo accounting a failover scenario
/// asserts on — and `spawn_count` counts every `spawn_worker` call (initial
/// pool spawns + first-bind/type-shift/disconnect respawns), the worker-churn
/// observability.
#[derive(Clone)]
pub(super) struct ScriptedWorkerFactory {
    scripts: Arc<Vec<(String, WorkerScript)>>,
    /// Per-task run counter keyed by the wire `relative_path`, bumped on
    /// every `ProcessTask` a worker receives (before the scripted response).
    pub(super) run_counts: Arc<std::sync::Mutex<HashMap<String, u32>>>,
    /// Total `spawn_worker` invocations across the fleet (initial spawns +
    /// every respawn).
    pub(super) spawn_count: Arc<std::sync::atomic::AtomicU32>,
}

impl ScriptedWorkerFactory {
    pub(super) fn new(scripts: Vec<(String, WorkerScript)>) -> Self {
        Self {
            scripts: Arc::new(scripts),
            run_counts: Arc::new(std::sync::Mutex::new(HashMap::new())),
            spawn_count: Arc::new(std::sync::atomic::AtomicU32::new(0)),
        }
    }
}

impl WorkerFactory<ChannelManagerEnd> for ScriptedWorkerFactory {
    fn spawn_worker(
        &mut self,
        _worker_id: u32,
        _subcgroup: Option<&dynrunner_manager_local::cgroup::SubcgroupHandle>,
    ) -> Result<(ChannelManagerEnd, Option<u32>), String> {
        self.spawn_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let (manager_end, runner_end) = channel_pair();
        let scripts = self.scripts.clone();
        let run_counts = self.run_counts.clone();
        tokio::task::spawn_local(async move {
            let mut runner = runner_end;
            let _ = runner.send(Response::Ready).await;
            loop {
                match MessageReceiver::<Command>::recv(&mut runner).await {
                    Some(Command::Stop) => break,
                    Some(Command::ProcessTask { relative_path, .. }) => {
                        *run_counts
                            .lock()
                            .expect("run_counts mutex poisoned")
                            .entry(relative_path.clone())
                            .or_insert(0) += 1;
                        let script = scripts
                            .iter()
                            .find(|(needle, _)| relative_path.contains(needle))
                            .map(|(_, s)| s.clone())
                            .unwrap_or(WorkerScript::Done {
                                delay: std::time::Duration::ZERO,
                            });
                        let response = match script {
                            WorkerScript::Done { delay } => {
                                if !delay.is_zero() {
                                    tokio::time::sleep(delay).await;
                                }
                                Response::Done { result_data: None }
                            }
                            WorkerScript::Error {
                                delay,
                                error_type,
                                message,
                            } => {
                                if !delay.is_zero() {
                                    tokio::time::sleep(delay).await;
                                }
                                Response::Error {
                                    error_type,
                                    message,
                                }
                            }
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
            liveness_port: None,
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

/// A relocation-target fake that is TRANSPORT-CONNECTED but NEVER
/// OPERATIONAL: it sends `SecondaryWelcome` (with `can_be_primary: true`,
/// so it is an eligible `select_relocation_target` candidate) +
/// `CertExchange`, then drains the replicated ledger and answers the
/// `PrimaryChanged { new = self }` promotion handoff exactly like
/// [`fake_secondary_with_addrs`] — but it DELIBERATELY never emits
/// `MeshReady`.
///
/// This models a secondary still inside `wait_for_setup` (it has cert-
/// exchanged with the setup peer but has not yet received an
/// `InitialAssignment`, so it has not reached its operational loop where
/// `report_mesh_ready_if_needed` fires). It is the fixture that proves BUG
/// C is fixed: with the pre-fix code the setup peer's unconditional
/// `wait_for_mesh_ready` blocks on a `MeshReady` this fake never sends
/// (circular: the assignment that would make it operational only comes from
/// the operational primary, which only exists after relocation), so the
/// relocate burns the full `mesh_ready_timeout`. With the fix the setup peer
/// relocates immediately off the transport-connected fleet, ignoring the
/// absent operational signal.
///
/// Trimmed vs [`fake_secondary_with_addrs`]: no `InitialAssignment` /
/// `TaskAssignment` worker-drain arms (the setup peer never assigns — it
/// relocates), only the `PrimaryChanged`-promotion ledger drain needed to
/// let the relocated-target run reach a clean terminal.
pub(super) async fn fake_secondary_transport_only_no_meshready(
    secondary_id: String,
    num_workers: u32,
    ram_bytes: u64,
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
            // Eligible relocation target (a network compute secondary).
            can_be_primary: true,
        })
        .unwrap();

    outgoing_to_primary
        .send(DistributedMessage::CertExchange {
            target: Some(Destination::Primary),
            sender_id: secondary_id.clone(),
            timestamp: 0.0,
            secondary_id: secondary_id.clone(),
            public_cert_pem: "FAKE_CERT".into(),
            ipv4_address: Some("127.0.0.1".into()),
            ipv6_address: None,
            quic_port: 5000,
            liveness_port: None,
        })
        .unwrap();

    // NO MeshReady — this is the whole point: a transport-connected but
    // not-yet-operational secondary. The setup peer must relocate WITHOUT it.

    // Mirror the live ledger so the promotion-handoff drain can fire (same
    // shape as `fake_secondary_with_addrs`, minus the assignment arms).
    let mut pending_hashes: HashSet<String> = HashSet::new();
    while let Some(msg) = incoming_from_primary.recv().await {
        if let DistributedMessage::ClusterMutation { mutations, .. } = msg {
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
    // Mesh-always: there is no construction-time relocation policy. Whether a
    // primary built here relocates or runs operationally is decided ENTIRELY
    // by the `SeedSource` its `run`/`run_consuming` receives (`ColdStart` /
    // `RelocatedSeed` ⇒ setup peer ⇒ relocate; `PromotionSnapshot` ⇒
    // operational). The relocate path is exercised by the dedicated unit tests
    // that call `select_relocation_target` / `relocate_primary_to` directly,
    // and by the seed-keyed bootstrap-role tests.
    let mut mesh = Mesh::new(transport);
    let (slot, client, inbox) =
        mesh.register_local_role(LocalRole::Primary, PeerId::from(config.node_id.as_str()));
    let (demote_tx, demote_rx) = tokio_mpsc::unbounded_channel();
    let mut primary =
        PrimaryCoordinator::new(config, client, inbox, demote_rx, scheduler, estimator);
    // Wire the loss-of-primacy hook exactly as the production `Node` does the
    // moment it builds a primary: the local apply of a `PrimaryChanged` naming
    // ANOTHER peer fires `demote_tx`, so a setup peer's `relocate_primary_to`
    // drives `run_consuming`'s demote arm to a `PrimaryRunOutcome::Relocated`
    // handoff. Without this the relocate's local apply has no observer and the
    // SetupPeer arm parks forever. Harmless on the non-relocating paths (the
    // hook only fires on a self→other flip; the unbounded `demote_rx` sits on
    // `self` undrained otherwise).
    primary.register_demote_on_displaced(demote_tx.clone());
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

/// Pre-seed a test primary's replicated ledger with a cold-task corpus so a
/// subsequent `run(SeedSource::PromotionSnapshot)` resumes it as an
/// OPERATIONAL primary in place — the honest mesh-always seed for "a primary
/// that runs the dispatch/retry/phase loop locally" (≡ the relocated target /
/// failover-promoted primary; a `ColdStart` is now a setup peer that relocates
/// AWAY, so it would never run the loop itself).
///
/// Applies `PhaseDepsSet` + one Pending `TaskAdded` per binary to the LOCAL
/// `cluster_state` (the inherited-ledger shape a promotion carries); the
/// always-run hydrate in `run_pipeline` then rebuilds the pool + `total_tasks`.
/// The operational-loop unit tests that previously seeded via `ColdStart` use
/// this + `run(PromotionSnapshot)` to assert the SAME dispatch behaviour
/// honestly under the uniform model. Single choke point so the cold→operational
/// re-key is one line per test, not an inline mutation loop each.
pub(super) fn seed_operational_ledger<S, E>(
    primary: &mut PrimaryCoordinator<S, E, TestId>,
    binaries: Vec<TaskInfo<TestId>>,
    phase_deps: HashMap<PhaseId, Vec<PhaseId>>,
) where
    S: Scheduler<TestId>,
    E: ResourceEstimator<TestId>,
{
    let cs = primary.cluster_state_mut_for_test();
    cs.apply(ClusterMutation::PhaseDepsSet { deps: phase_deps });
    for task in &binaries {
        cs.apply(ClusterMutation::TaskAdded {
            hash: crate::primary::wire::compute_task_hash(task),
            task: task.clone(),
        });
    }
}

/// Build a [`crate::process::PromotedPrimaryBuilder`] for a relocate/failover
/// TARGET in the test crate — the transport-agnostic recipe `Node::run`
/// invokes on a `PromotionSignal` to construct the snapshot-seeded promoted
/// primary. Mirrors the pyo3 `build_promoted_primary_recipe` minus the
/// Python/config plumbing: given the just-minted mesh ends + the node-owned
/// demote receiver + the promoting host's converged snapshot, it builds the
/// coordinator, optionally registers a discovery policy (for the pre-staged /
/// `RelocatedSeed` path, where the target runs `discover_on_promotion`), seeds
/// from the snapshot, and returns the ready-to-`run` primary with a
/// `PromotionSnapshot` seed (⇒ `BootstrapRole::PromotedDestination`, so it runs
/// the operational loop in place).
///
/// `setup_discovery` is `Some` only on the pre-staged path; `None` on the cold
/// path (the snapshot already carries the seeded tasks). The optional
/// `on_phase_*` default to no-ops; a caller that asserts phase narration passes
/// real closures.
pub(super) fn build_test_promote_recipe(
    config_id: String,
    setup_discovery: Option<crate::discovery::SetupDiscovery<TestId>>,
) -> crate::process::PromotedPrimaryBuilder<
    dynrunner_scheduler::ResourceStealingScheduler,
    FixedEstimator,
    TestId,
> {
    build_test_promote_recipe_with_config(
        PrimaryConfig {
            node_id: config_id,
            mesh_ready_timeout: std::time::Duration::from_secs(5),
            ..PrimaryConfig::default()
        },
        setup_discovery,
    )
}

/// As [`build_test_promote_recipe`] but with an explicit [`PrimaryConfig`] for
/// the promoted primary — for tests that need fast death-detection timeouts
/// (`keepalive_interval` / `peer_timeout` / `fleet_dead_timeout`) so the
/// operational target promptly strands work routed to dead compute peers.
pub(super) fn build_test_promote_recipe_with_config(
    config: PrimaryConfig,
    setup_discovery: Option<crate::discovery::SetupDiscovery<TestId>>,
) -> crate::process::PromotedPrimaryBuilder<
    dynrunner_scheduler::ResourceStealingScheduler,
    FixedEstimator,
    TestId,
> {
    build_test_promote_recipe_with_config_and_hooks(
        config,
        setup_discovery,
        Box::new(|_| (Box::new(|_| {}), Box::new(|_, _, _, _| {}))),
    )
}

/// The phase-hooks factory a scenario hands to
/// [`build_test_promote_recipe_with_config_and_hooks`]: invoked ONCE at the
/// promotion build, AFTER the coordinator exists, with the promoted primary's
/// live command sender — so the built hooks can drive
/// `PrimaryCommand::SpawnTasks` (the consumer's phase-chaining
/// `on_phase_end → primary_handle.spawn_tasks` pattern) against the primary
/// they belong to. Mirrors how the production pyo3 recipe builds its
/// Python-callback hooks around the coordinator it just constructed.
pub(super) type PromoteHooksFactory = Box<
    dyn FnOnce(
        tokio_mpsc::Sender<crate::primary::command_channel::PrimaryCommand<TestId>>,
    ) -> (crate::primary::OnPhaseStart, crate::primary::OnPhaseEnd),
>;

/// As [`build_test_promote_recipe_with_config`] but the caller supplies a
/// [`PromoteHooksFactory`] for the promoted primary's `on_phase_start` /
/// `on_phase_end` — the seam a phase-chaining e2e scenario needs on a
/// PROMOTED primary (relocate target or failover winner), where the hooks
/// must capture the freshly-built coordinator's command sender.
pub(super) fn build_test_promote_recipe_with_config_and_hooks(
    config: PrimaryConfig,
    setup_discovery: Option<crate::discovery::SetupDiscovery<TestId>>,
    hooks: PromoteHooksFactory,
) -> crate::process::PromotedPrimaryBuilder<
    dynrunner_scheduler::ResourceStealingScheduler,
    FixedEstimator,
    TestId,
> {
    let mut setup_discovery = setup_discovery;
    // The recipe fires at most once (a node promotes once); take the
    // single-use `PrimaryConfig` / hooks factory (not `Clone`) on that one
    // invocation.
    let mut config = Some(config);
    let mut hooks = Some(hooks);
    Box::new(move |client, inbox, demote_rx, snapshot| {
        let config = config
            .take()
            .expect("promote recipe invoked more than once");
        let mut primary = PrimaryCoordinator::new(
            config,
            client,
            inbox,
            demote_rx,
            dynrunner_scheduler::ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );
        if let Some(sd) = setup_discovery.take() {
            primary.register_setup_discovery(sd);
        }
        primary.seed_from_promotion_snapshot(snapshot);
        let (on_phase_start, on_phase_end) =
            (hooks.take().expect("promote recipe invoked more than once"))(
                primary.command_sender(),
            );
        crate::process::PromotedPrimary {
            coordinator: primary,
            run_args: crate::process::PrimaryRunArgs {
                seed: crate::process::SeedSource::PromotionSnapshot,
                on_phase_start,
                on_phase_end,
            },
        }
    })
}

/// The two staging-dispatch flags the promote recipe sources from the node's
/// OWN local producer (the relocate-staging fix). A FAITHFUL mirror of the
/// pyo3 recipe inputs (`managers/secondary/run.rs::PromotedPrimaryRecipeInputs`'s
/// `uses_file_based_items` / `pre_staged_mode`), so the manager-distributed
/// relocate test drives the SAME stamping logic the production recipe runs —
/// rather than a pre-built config that bypasses the source entirely.
pub(super) struct ProducerStagingFlags {
    /// From the consumer `task_definition.uses_file_based_items`.
    pub uses_file_based_items: bool,
    /// From `task_args.source_already_staged` non-None (mirrors the submitter's
    /// `source_pre_staged_root.is_some()`).
    pub pre_staged_mode: bool,
    /// The pre-staged corpus root, threaded into `source_pre_staged_root` IFF
    /// `pre_staged_mode` (the recipe's gating discriminant).
    pub source_pre_staged_root: Option<std::path::PathBuf>,
    /// The local source-tree root for `maybe_auto_stage_initial`'s re-walk.
    pub source_dir: Option<std::path::PathBuf>,
}

/// Build a promote recipe that mirrors the PRODUCTION pyo3 recipe's source:
/// it stamps `uses_file_based_items` / `source_pre_staged_root` from the
/// node's own LOCAL PRODUCER (`flags`), NOT from the `InitialAssignment`-fed
/// `StagingDispatchContext` cell. `cell` is the relocate-target secondary's
/// live cell handle — captured to PROVE it stays at `Default` (a relocate-
/// target receives no `InitialAssignment` before promotion) and is irrelevant
/// to the stamped flags. This is what makes the relocate test actually catch
/// the false-green: the cell is on the path but is NOT the source.
///
/// REVERT-CHECK seam: flipping the two `flags.*` reads below to
/// `cell.lock()...` reproduces the bug (the cell is at `Default`, so the
/// promoted primary stamps `uses_file_based_items=true` / no pre-staging) and
/// the dispatch-target assertions fail — confirming the test exercises the
/// real gap.
pub(super) fn build_test_promote_recipe_from_producer(
    config_id: String,
    flags: ProducerStagingFlags,
    cell: std::sync::Arc<std::sync::Mutex<crate::secondary::StagingDispatchContext>>,
    setup_discovery: Option<crate::discovery::SetupDiscovery<TestId>>,
) -> crate::process::PromotedPrimaryBuilder<
    dynrunner_scheduler::ResourceStealingScheduler,
    FixedEstimator,
    TestId,
> {
    let ProducerStagingFlags {
        uses_file_based_items,
        pre_staged_mode,
        source_pre_staged_root,
        source_dir,
    } = flags;
    let mut setup_discovery = setup_discovery;
    Box::new(move |client, inbox, demote_rx, snapshot| {
        // The cell is captured (on the live promotion path) but deliberately
        // NOT read for the stamped flags — assert it is still at `Default` to
        // pin that a relocate-target gets no `InitialAssignment` pre-promotion.
        let cell_at_promotion = *cell
            .lock()
            .expect("staging_dispatch_context mutex poisoned");
        assert_eq!(
            cell_at_promotion,
            crate::secondary::StagingDispatchContext::default(),
            "a relocate-target's wire-fed cell MUST be at Default at promotion \
             (no InitialAssignment yet) — if this fails the test's premise is wrong"
        );
        let config = PrimaryConfig {
            node_id: config_id.clone(),
            mesh_ready_timeout: std::time::Duration::from_secs(5),
            keepalive_interval: std::time::Duration::from_secs(60),
            peer_timeout: std::time::Duration::from_secs(120),
            // Sourced from the LOCAL PRODUCER (`flags`), mirroring the pyo3
            // recipe — NOT the `cell` above.
            uses_file_based_items,
            source_pre_staged_root: if pre_staged_mode {
                source_pre_staged_root.clone()
            } else {
                None
            },
            source_dir: source_dir.clone(),
            ..PrimaryConfig::default()
        };
        let mut primary = PrimaryCoordinator::new(
            config,
            client,
            inbox,
            demote_rx,
            dynrunner_scheduler::ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );
        if let Some(sd) = setup_discovery.take() {
            primary.register_setup_discovery(sd);
        }
        primary.seed_from_promotion_snapshot(snapshot);
        crate::process::PromotedPrimary {
            coordinator: primary,
            run_args: crate::process::PrimaryRunArgs {
                seed: crate::process::SeedSource::PromotionSnapshot,
                on_phase_start: Box::new(|_| {}),
                on_phase_end: Box::new(|_, _, _, _| {}),
            },
        }
    })
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
        ChannelPeerTransport::from_raw_channels(SETUP_NODE_ID.into(), outgoing, incoming_rx),
        secondary_ends,
    )
}
