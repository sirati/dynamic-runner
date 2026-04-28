//! Shared test fixtures for secondary-side tests. Compiled only under
//! `#[cfg(test)]` so it never enters the production binary.

use std::time::Duration;

use db_comm_api_base::{Identifier, MessageReceiver, MessageSender};
use db_manager_runner_comm::{Command, Response};
use db_primary_secondary_comm::{
    DistributedMessage, PeerConnectionInfo, PeerTransport,
};
use db_local_manager::WorkerFactory;
use db_scheduler_api::ResourceEstimator;
use db_scheduler_impl::ResourceStealingScheduler;
use db_transport_channel::{channel_pair, ChannelManagerEnd, ChannelPrimaryTransportEnd};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc as tokio_mpsc;

use super::{SecondaryConfig, SecondaryCoordinator};

/// Minimal serializable identifier used by every secondary test.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(super) struct TestId(pub String);

/// Estimator that returns the same fixed memory amount for every binary.
#[derive(Clone)]
pub(super) struct FixedEstimator(pub u64);

impl ResourceEstimator for FixedEstimator {
    fn estimate(&self, _size: u64) -> db_comm_api_base::ResourceMap {
        db_comm_api_base::ResourceMap::from([(db_comm_api_base::ResourceKind::memory(), self.0)])
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

/// WorkerFactory that fakes a runner: replies Ready, then echoes Done for
/// each ProcessTask without doing real work.
pub(super) struct FakeWorkerFactory;

impl WorkerFactory<ChannelManagerEnd> for FakeWorkerFactory {
    fn spawn_worker(
        &mut self,
        _worker_id: db_comm_api_base::WorkerId,
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
        max_resources: db_comm_api_base::ResourceMap::from([(
            db_comm_api_base::ResourceKind::memory(),
            1024 * 1024 * 1024,
        )]),
        hostname: "test-host".into(),
        keepalive_interval: Duration::from_millis(50),
        src_network: None,
        src_tmp: None,
        peer_timeout: Duration::from_secs(120),
        keepalive_miss_threshold: 2,
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
