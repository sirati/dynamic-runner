//! Shared test fixtures for secondary-side tests. Compiled only under
//! `#[cfg(test)]` so it never enters the production binary.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Duration;

use dynrunner_core::{Identifier, MessageReceiver, MessageSender};
use dynrunner_manager_local::WorkerFactory;
use dynrunner_protocol_manager_worker::{Command, Response};
use dynrunner_protocol_primary_secondary::{
    DistributedMessage, PeerConnectionInfo, PeerId, PeerTransport,
};
use dynrunner_scheduler::ResourceStealingScheduler;
use dynrunner_scheduler_api::ResourceEstimator;
use dynrunner_transport_channel::{ChannelManagerEnd, ChannelPeerTransport, channel_pair};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use super::{SecondaryConfig, SecondaryCoordinator};

/// The single `Tr: PeerTransport` secondary tests construct: the
/// peer-mesh stub itself. `P` lets a test pick the stub (`NoPeers`,
/// `RecordingPeer`, or a real routing-aware `ChannelPeerTransport`
/// built via `channel_mesh_to_primary` / `channel_mesh_no_primary`).
///
/// Post-uplink-deletion the secondary holds its mesh `PeerTransport`
/// DIRECTLY — there is no per-role uplink leg and no wrapper. Tests
/// that drive the coordinator via direct method calls (election state,
/// resource-probe, mesh-watchdog) construct it from a stub and exercise
/// the coordinator without any primary inbound. Tests that previously
/// injected "primary" frames over the channel uplink (full setup +
/// dispatch against a `fake_primary` / `spawn_real_secondary`) need the
/// primary to be a mesh peer they can feed — i.e. a channel-backed mesh
/// stub with the primary registered — which is the secondary-test
/// harness mesh-migration concern, NOT this uplink-deletion leaf; those
/// are `#[ignore]`d at their call sites until that leaf lands.
pub(super) type TestTransport<P> = P;

/// Build a [`TestTransport`] from a peer-mesh stub. The secondary holds
/// the stub directly as its `Tr: PeerTransport` (the primary is a mesh
/// peer reached by id, not a wrapped uplink).
pub(super) fn make_transport<P: PeerTransport<TestId>>(peer: P) -> TestTransport<P> {
    peer
}

/// Build the channel-backed mesh transport a secondary holds when driven
/// against a `fake_primary` in-process: the primary is folded in as an
/// ordinary mesh peer keyed by `"primary"` (via
/// [`ChannelPeerTransport::register_primary_link`]), the channel analog of
/// how the QUIC bootstrap wire folds into `PeerNetwork`.
///
/// `to_primary` carries the secondary's outbound to the folded primary
/// link (the `fake_primary` reads its paired receiver); `from_primary`
/// is the transport's inbound (the `fake_primary` writes its paired
/// sender). Callers pair this with `set_bootstrap_primary_id("primary")`
/// so the egress edge resolves `Destination::Primary` to the same id
/// while the role table is cold.
pub(super) fn channel_mesh_to_primary(
    secondary_id: &str,
    to_primary: mpsc::UnboundedSender<DistributedMessage<TestId>>,
    from_primary: mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
) -> ChannelPeerTransport<TestId> {
    let mut transport =
        ChannelPeerTransport::from_raw_channels(secondary_id.into(), HashMap::new(), from_primary);
    transport.register_primary_link("primary".into(), to_primary);
    transport
}

/// Build a channel-backed mesh transport with the primary folded in AND
/// a single observed peer outbox, returning the transport plus the
/// observed peer's inbound receiver. The primary link (keyed `"primary"`)
/// carries the secondary's primary-bound traffic + the inbound setup
/// frames; the observed peer receives the secondary's mesh `broadcast`s
/// (the primary is excluded from the fan-out), so a test can drain
/// `observer_rx` to assert what the secondary fanned out onto the mesh.
///
/// `peer_count()` is 1 (the observed peer; the primary link is excluded),
/// matching the `RecordingPeer::new(1)` cardinality the broadcast-observer
/// tests previously used. Pair with `set_bootstrap_primary_id("primary")`.
pub(super) fn channel_mesh_with_observed_peer(
    secondary_id: &str,
    to_primary: mpsc::UnboundedSender<DistributedMessage<TestId>>,
    from_primary: mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
) -> (
    ChannelPeerTransport<TestId>,
    mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
) {
    let (observer_tx, observer_rx) = mpsc::unbounded_channel();
    let mut outgoing: HashMap<String, mpsc::UnboundedSender<DistributedMessage<TestId>>> =
        HashMap::new();
    outgoing.insert("peer-observer".to_string(), observer_tx);
    let mut transport =
        ChannelPeerTransport::from_raw_channels(secondary_id.into(), outgoing, from_primary);
    transport.register_primary_link("primary".into(), to_primary);
    (transport, observer_rx)
}

/// Build a routing-aware channel-backed mesh stub with `peer_count` peer
/// outboxes registered but NO primary link, so `peer_count()` reports the
/// configured cardinality (a healthy mesh) while `send_to_peer("primary")`
/// returns a real NoRoute `Err`. Inbound is a never-fed receiver
/// (`recv_peer` blocks forever, like the prior stubs).
///
/// This is what the R1 failover-health-probe tests drive: paired with
/// `set_bootstrap_primary_id("primary")`, `send_to_primary` resolves
/// `Destination::Primary` to `"primary"`, finds no outbox for it, and
/// surfaces the no-route `Err` that arms the count-axis — the real
/// routing-aware no-route signal, replacing the identity-blind
/// `FixedPeerCount` stub that could only no-op (Ok) on every send.
pub(super) fn channel_mesh_no_primary(
    secondary_id: &str,
    peer_count: usize,
) -> ChannelPeerTransport<TestId> {
    // `incoming_rx` is fed by a sender we immediately drop: `recv_peer`
    // never yields (the R1 tests drive the coordinator by direct method
    // calls, never through the transport's inbound).
    let (_never_tx, never_rx) = mpsc::unbounded_channel();
    let mut outgoing: HashMap<String, mpsc::UnboundedSender<DistributedMessage<TestId>>> =
        HashMap::new();
    for i in 0..peer_count {
        // Dummy peer outboxes: their receivers are dropped, but the
        // sender's presence is what `peer_count()` / `has_peer(peer)`
        // measure. Keyed by `peer-{i}` — never `"primary"`, so the
        // primary stays unrouteable.
        let (peer_tx, _peer_rx) = mpsc::unbounded_channel();
        outgoing.insert(format!("peer-{i}"), peer_tx);
    }
    ChannelPeerTransport::from_raw_channels(secondary_id.into(), outgoing, never_rx)
}

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
    fn has_peer(&self, _id: &PeerId) -> bool {
        // Models no peers — every id is a non-member. Consistent with
        // `peer_count == 0`.
        false
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
    fn has_peer(&self, _id: &PeerId) -> bool {
        // Identity-blind recorder: it models a configurable peer
        // CARDINALITY (`self.peer_count`) for the "healthy mesh vs no
        // peers" branches but records sends keyed by nothing. The only
        // internally-consistent boolean it can give is derived from that
        // count: a non-empty mesh has peers, an empty one does not.
        self.peer_count > 0
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
        oom_retry_max_passes: 1,
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
        // Production pre-config default; tests driving election state
        // never sit in the unconfigured states long enough for this to
        // fire, so the 10-min default is well outside any test budget.
        unconfigured_deadline: Duration::from_secs(600),
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
        output_dir: None,
        memuse_log_path: None,
    }
}

/// Construct a SecondaryCoordinator over the unified transport
/// (channel uplink + `NoPeers` mesh stub) detached from any real
/// primary or peer; used to drive the election state machine via direct
/// method calls without a full multi-process harness.
pub(super) fn make_secondary(
    config: SecondaryConfig,
) -> SecondaryCoordinator<
    TestTransport<NoPeers>,
    ChannelManagerEnd,
    ResourceStealingScheduler,
    FixedEstimator,
    TestId,
> {
    SecondaryCoordinator::new(
        config,
        make_transport(NoPeers),
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    )
}

/// Construct a SecondaryCoordinator over the unified transport with a
/// [`RecordingPeer`] mesh stub, returning the coordinator + the shared
/// broadcast log so a test can assert on the messages the failover
/// terminal action (e.g. the `PromotePrimary { new = self }` re-point)
/// fans out onto the mesh. `peer_count` configures the recorder's
/// reported mesh cardinality.
#[allow(clippy::type_complexity)]
pub(super) fn make_secondary_recording(
    config: SecondaryConfig,
    peer_count: usize,
) -> (
    SecondaryCoordinator<
        TestTransport<RecordingPeer<TestId>>,
        ChannelManagerEnd,
        ResourceStealingScheduler,
        FixedEstimator,
        TestId,
    >,
    Rc<RefCell<Vec<DistributedMessage<TestId>>>>,
) {
    let recorder = RecordingPeer::<TestId>::new(peer_count);
    let log = recorder.log_handle();
    let coord = SecondaryCoordinator::new(
        config,
        make_transport(recorder),
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    (coord, log)
}
