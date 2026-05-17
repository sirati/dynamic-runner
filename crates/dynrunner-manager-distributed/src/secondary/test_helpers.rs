//! Shared test fixtures for secondary-side tests. Compiled only under
//! `#[cfg(test)]` so it never enters the production binary.

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use dynrunner_core::{Identifier, MessageReceiver, MessageSender};
use dynrunner_protocol_manager_worker::{Command, Response};
use dynrunner_protocol_primary_secondary::{
    DistributedMessage, PeerConnectionInfo, PeerTransport,
};
use dynrunner_manager_local::WorkerFactory;
use dynrunner_scheduler_api::ResourceEstimator;
use dynrunner_scheduler::ResourceStealingScheduler;
use dynrunner_transport_channel::{channel_pair, ChannelManagerEnd, ChannelPrimaryTransportEnd};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc as tokio_mpsc;

use super::{SecondaryConfig, SecondaryCoordinator};

/// Minimal serializable identifier used by every secondary test.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(super) struct TestId(pub String);

/// Estimator that returns the same fixed memory amount for every binary.
#[derive(Clone)]
pub(super) struct FixedEstimator(pub u64);

impl ResourceEstimator<TestId> for FixedEstimator {
    fn estimate(&self, _task: &dynrunner_core::TaskInfo<TestId>) -> dynrunner_core::ResourceMap {
        dynrunner_core::ResourceMap::from([(dynrunner_core::ResourceKind::memory(), self.0)])
    }
}

/// PeerTransport that drops every message and never produces input. Use
/// for tests that exercise the secondary in isolation.
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

/// PeerTransport stub that reports a fixed peer count without
/// actually wiring any messages. Used by the peer-mesh watchdog
/// tests to drive the "mesh formed" branch (peer_count > 0)
/// without spinning up real QUIC endpoints.
pub(super) struct FixedPeerCount(pub usize);

impl<I: Identifier> PeerTransport<I> for FixedPeerCount {
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
        self.0
    }
    async fn connect_to_peers(&mut self, _peers: &[PeerConnectionInfo]) {}
}

/// PeerTransport that captures every `broadcast` and `send_to_peer`
/// call into a shared `Rc<RefCell<Vec<_>>>` so a test that drives the
/// secondary's promoted-primary side (e.g. `ingest_setup_discovery`)
/// can assert on the messages that would have gone over the peer mesh.
///
/// `recv_peer` blocks forever — these tests synthesize their own
/// inbound messages via `dispatch_message` / `handle_peer_message`
/// rather than driving them through the transport.
///
/// `peer_count` is configurable so the same recorder serves both the
/// "healthy mesh, broadcasts go out" and "no peers, broadcasts are
/// best-effort" branches without two near-identical recorders.
pub(super) struct RecordingPeer<I: Identifier> {
    pub(super) broadcasts: Rc<RefCell<Vec<DistributedMessage<I>>>>,
    pub(super) peer_count: usize,
}

impl<I: Identifier> RecordingPeer<I> {
    pub(super) fn new(peer_count: usize) -> Self {
        Self {
            broadcasts: Rc::new(RefCell::new(Vec::new())),
            peer_count,
        }
    }

    /// Clone of the shared broadcast log. The recorder is moved into
    /// `SecondaryCoordinator::new` so callers need the handle they keep
    /// before that move.
    pub(super) fn log_handle(&self) -> Rc<RefCell<Vec<DistributedMessage<I>>>> {
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
        // Unicast goes into the same log as broadcasts for these tests;
        // none of the four setup-promote scenarios distinguish the two
        // (ingest_setup_discovery only uses broadcast). Recording both
        // means a future variant that switches to unicast still gets
        // captured rather than being silently dropped.
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
        self.peer_count
    }
    async fn connect_to_peers(&mut self, _peers: &[PeerConnectionInfo]) {}
}

/// WorkerFactory that fakes a runner: replies Ready, then echoes Done for
/// each ProcessTask without doing real work.
pub(super) struct FakeWorkerFactory;

impl WorkerFactory<ChannelManagerEnd> for FakeWorkerFactory {
    fn spawn_worker(
        &mut self,
        _worker_id: dynrunner_core::WorkerId,
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

/// Default `SecondaryConfig` for election-state tests: short keepalive so
/// real-time tests can finish quickly, threshold 2 (death after 100ms).
pub(super) fn election_config(secondary_id: &str) -> SecondaryConfig {
    SecondaryConfig {
        secondary_id: secondary_id.into(),
        num_workers: 1,
        max_resources: dynrunner_core::ResourceMap::from([(
            dynrunner_core::ResourceKind::memory(),
            1024 * 1024 * 1024,
        )]),
        hostname: "test-host".into(),
        keepalive_interval: Duration::from_millis(50),
        src_network: None,
        src_tmp: None,
        peer_timeout: Duration::from_secs(120),
        keepalive_miss_threshold: 2,
        retry_max_passes: 1,
        // Tight failover threshold so R1 tests don't have to wait
        // 30s of wall-clock. Threshold of 3 is the minimum allowed
        // by the design (single-packet drop margin); the time
        // window is 200ms (4 keepalive intervals at 50ms each)
        // which is the smallest window that still gives the
        // count-axis some headroom in tests that drive only
        // count-axis behaviour.
        primary_link_failure_threshold: 3,
        primary_link_failure_window: Duration::from_millis(200),
        // Tests that drive election state don't exercise setup;
        // 60s is the production default and well outside any test's
        // wall-clock budget, so it never fires accidentally.
        setup_deadline: Duration::from_secs(60),
        is_observer: false,
        resource_check_interval: Duration::from_millis(100),
        log_oom_watcher: false,
        // Short grace so promotion-gated tests can drive the
        // natural-quiesce branch without waiting the production
        // 2-second default. Production code-path semantics are
        // identical; only the wall-clock threshold differs.
        promoted_primary_quiesce_grace: Duration::from_millis(100),
        // Tests that drive ReinjectTask explicitly populate the
        // budget per fixture; the election-state default leaves it
        // unbounded (None) so tests not exercising the budget see
        // the production-default semantics.
        unfulfillable_reinject_max_per_task: None,
        mem_manager_reserved_bytes: None,
    }
}

/// Construct a SecondaryCoordinator with channel transports detached from
/// any real primary or peer; used to drive the election state machine via
/// direct method calls without needing a full multi-process harness.
pub(super) fn make_secondary(
    config: SecondaryConfig,
) -> SecondaryCoordinator<
    ChannelPrimaryTransportEnd<TestId>,
    NoPeers,
    ChannelManagerEnd,
    ResourceStealingScheduler,
    FixedEstimator,
    TestId,
> {
    let (sec_to_pri_tx, _sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
    let (_pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();
    let transport = ChannelPrimaryTransportEnd {
        tx: sec_to_pri_tx,
        rx: pri_to_sec_rx,
    };
    SecondaryCoordinator::new(
        config,
        transport,
        NoPeers,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    )
}
