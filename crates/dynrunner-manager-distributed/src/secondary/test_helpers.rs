//! Shared test fixtures for secondary-side tests. Compiled only under
//! `#[cfg(test)]` so it never enters the production binary.
//!
//! # The mesh harness
//!
//! Post one-mesh, a `SecondaryCoordinator` no longer holds a transport: it
//! reaches the wire ONLY through a [`crate::process::MeshClient`] (egress) +
//! [`crate::process::RoleInbox`] (ingress), minted together with its
//! `Arc<RoleSlot>` by `Mesh::register_local_role`. So every fixture wraps the
//! test transport in a [`crate::process::Mesh`], registers the secondary
//! role, and hands the coordinator the minted `client + inbox` plus a
//! `promotion_tx`. The fixture returns a [`SecondaryHarness`] that OWNS the
//! `Mesh` (so a test can drain the coordinator's queued egress against the
//! transport — `MeshClient::send` is QUEUED, not synchronous), the
//! `Arc<RoleSlot>` (keeping it alive so loopback delivery works), and the
//! `promotion_rx` (so the C4 promotion signal has a live receiver to assert
//! on). The harness `Deref`s to the coordinator so existing `sec.method()` /
//! `sec.field` call sites are unchanged.

use std::cell::RefCell;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::rc::Rc;
use std::sync::Arc;
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
use crate::process::{LocalRole, Mesh, PromotionSignal, RoleSlot};

/// The single `Tr: PeerTransport` secondary tests construct: the
/// peer-mesh stub itself. `P` lets a test pick the stub (`NoPeers`,
/// `RecordingPeer`, or a real routing-aware `ChannelPeerTransport`
/// built via `channel_mesh_to_primary` / `channel_mesh_no_primary`).
///
/// Post-uplink-deletion the secondary holds its mesh `PeerTransport`
/// DIRECTLY — there is no per-role uplink leg and no wrapper. Tests
/// that drive the coordinator via direct method calls (election state,
/// resource-probe, mesh-watchdog) construct it from a stub and exercise
/// the coordinator without any primary inbound. Tests that inject
/// "primary" frames (full setup + dispatch against a `fake_primary` /
/// `spawn_real_secondary`) feed the primary as an ordinary mesh peer via
/// a channel-backed mesh stub with the primary link folded in.
#[allow(dead_code)] // scaffolding retained for the PENDING-C-NODE secondary e2e tests
pub(super) type TestTransport<P> = P;

/// Build a [`TestTransport`] from a peer-mesh stub. The mesh wraps the
/// stub; the secondary reaches it through the minted `MeshClient` /
/// `RoleInbox` (the primary is a mesh peer reached by id, not a wrapped
/// uplink).
#[allow(dead_code)] // scaffolding retained for the PENDING-C-NODE secondary e2e tests
pub(super) fn make_transport<P: PeerTransport<TestId>>(peer: P) -> TestTransport<P> {
    peer
}

/// The concrete secondary coordinator type a test fixture builds. The
/// coordinator no longer carries a transport generic — the mesh does — so
/// this type is the same regardless of which peer-mesh stub backs the
/// harness's `Mesh`.
pub(super) type TestSecondary = SecondaryCoordinator<
    ChannelManagerEnd,
    ResourceStealingScheduler,
    FixedEstimator,
    TestId,
>;

/// A built secondary plus the mesh plumbing a test must keep alive to
/// drive it.
///
/// `Deref`/`DerefMut` to the [`SecondaryCoordinator`] so existing
/// `sec.method()` / `sec.field` sites are unchanged. The harness OWNS:
/// - `test_mesh`: the [`Mesh`] wrapping the transport. The coordinator's
///   `MeshClient::send` QUEUES onto it; a test drains that queue with
///   [`SecondaryHarness::drain_egress`] (which applies each dispatch
///   against the transport, so a `RecordingPeer` log / channel receiver
///   sees the sends) and routes inbound wire frames with
///   [`SecondaryHarness::pump_inbound`].
/// - `_slot`: the secondary's `Arc<RoleSlot>`. Held so the mesh `Weak`
///   keeps upgrading (loopback delivery + the slot inbound stay live).
/// - `promotion_rx`: the C4 promotion-signal receiver. A promotion test
///   asserts a [`PromotionSignal`] arrives here.
pub(super) struct SecondaryHarness<P: PeerTransport<TestId>> {
    coord: TestSecondary,
    pub(super) test_mesh: Mesh<TestId, P>,
    _slot: Arc<RoleSlot<TestId>>,
    pub(super) promotion_rx: mpsc::UnboundedReceiver<PromotionSignal<TestId>>,
}

impl<P: PeerTransport<TestId>> Deref for SecondaryHarness<P> {
    type Target = TestSecondary;
    fn deref(&self) -> &Self::Target {
        &self.coord
    }
}

impl<P: PeerTransport<TestId>> DerefMut for SecondaryHarness<P> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.coord
    }
}

impl<P: PeerTransport<TestId>> SecondaryHarness<P> {
    /// Drain EVERY currently-queued egress dispatch the coordinator's
    /// `MeshClient` enqueued, applying each against the mesh (and thus the
    /// transport). After this returns, a `RecordingPeer` log / channel
    /// receiver has observed all the sends. Also publishes the live
    /// membership so the coordinator's `has_peer` egress gate reads the
    /// transport truth.
    ///
    /// `MeshClient::send` enqueues synchronously, so by the time a test
    /// awaits this every send it issued is already in the queue. The
    /// `biased` select drains each ready item and breaks the instant the
    /// queue is empty (the `next_local_dispatch` future parks `Pending`, so
    /// the always-`Ready` fallback arm fires) — never blocking on a frame
    /// that hasn't been sent.
    pub(super) async fn drain_egress(&mut self) {
        self.test_mesh.publish_membership();
        loop {
            tokio::select! {
                biased;
                item = self.test_mesh.next_local_dispatch() => match item {
                    Some(i) => {
                        let _ = self.test_mesh.apply_local_dispatch(i).await;
                    }
                    None => break,
                },
                _ = std::future::ready(()) => break,
            }
        }
    }

    /// Publish the live transport membership into the view the
    /// coordinator's `MeshClient` reads (the `has_peer` no-route gate).
    /// Call after seeding peer outboxes / registering the primary link so
    /// a direct-method-call test (no running pump) sees a fresh view.
    #[allow(dead_code)] // scaffolding retained for the PENDING-C-NODE secondary e2e tests
    pub(super) fn publish_membership(&mut self) {
        self.test_mesh.publish_membership();
    }

    /// Deliver one wire frame to this secondary's slot inbox — the test
    /// analogue of the mesh-pump's ingress demux for a single-role harness
    /// (every inbound frame in these fixtures is for the secondary).
    #[allow(dead_code)] // scaffolding retained for the PENDING-C-NODE secondary e2e tests
    pub(super) fn deliver_to_inbox(&mut self, frame: DistributedMessage<TestId>) -> bool {
        self.test_mesh.deliver_local(LocalRole::Secondary, frame)
    }
}

/// Wrap `transport` in a mesh, register the secondary role, and build the
/// coordinator with the minted `client + inbox` + a fresh promotion
/// channel. The single place every fixture mints the trio + signal, so a
/// test never hand-pairs a client with the wrong inbox.
fn build_harness<P: PeerTransport<TestId>>(
    config: SecondaryConfig,
    transport: P,
    scheduler: ResourceStealingScheduler,
    estimator: FixedEstimator,
) -> SecondaryHarness<P> {
    let secondary_id = config.secondary_id.clone();
    let mut mesh = Mesh::new(transport);
    let (slot, client, inbox) =
        mesh.register_local_role(LocalRole::Secondary, PeerId::from(secondary_id.as_str()));
    mesh.publish_membership();
    let (promotion_tx, promotion_rx) = mpsc::unbounded_channel();
    let mut coord = SecondaryCoordinator::new(config, client, inbox, scheduler, estimator);
    coord.register_promotion_signal(promotion_tx);
    SecondaryHarness {
        coord,
        test_mesh: mesh,
        _slot: slot,
        promotion_rx,
    }
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
/// frames; both the observed peer AND the folded primary receive the
/// secondary's mesh `broadcast`s — the role-blind transport fans out to
/// every member — so a test can drain `observer_rx` to assert what the
/// secondary fanned out onto the mesh.
///
/// `peer_count()` is 2 (the observed peer plus the folded primary, an
/// ordinary role-blind member). Pair with `set_bootstrap_primary_id("primary")`.
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
    fn has_peer(&self, id: &PeerId) -> bool {
        // Synthetic membership: the recorder models a healthy mesh with the
        // primary reachable (every recording test drives a node that already
        // recognises a `"primary"`), plus `peer_count` peers. The egress
        // no-route gate (`send_to`'s `has_peer` check on a resolved
        // `Peer(id)`) reads this through the published `MembershipView`, so
        // a `Destination::Primary` send must NOT be no-routed away here — it
        // is the very send these tests want to observe in the log.
        self.connected_ids().iter().any(|c| c == id)
    }
    fn connected_ids(&self) -> Vec<PeerId> {
        // The folded primary is always a member; the configured cardinality
        // is filled with `peer-{i}` ids. This backs the published view the
        // coordinator's egress gate reads — `has_peer("primary")` is true so
        // the recorded primary-bound sends route, and `peer_count()` agrees.
        std::iter::once(PeerId::from("primary"))
            .chain((0..self.peer_count).map(|i| PeerId::from(format!("peer-{i}").as_str())))
            .collect()
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
        // Tiny app-silence backstop (the patient leg (B) of
        // `run_election_tick`'s honest-liveness predicate) so election-
        // state tests drive a wedged-but-routable primary sub-second.
        // 100ms == the OLD bare receive-staleness deadline
        // (keepalive_interval 50ms × keepalive_miss_threshold 2), so the
        // pre-existing `PAST_DEATH = 110ms` backdate keeps tripping the
        // election with the same margin it always had.
        primary_silence_backstop: Duration::from_millis(100),
        // Tests that drive election state don't exercise setup;
        // 60s is the production default and well outside any test's
        // wall-clock budget, so it never fires accidentally.
        // Production pre-config default; tests driving election state
        // never sit in the unconfigured states long enough for this to
        // fire, so the 10-min default is well outside any test budget.
        unconfigured_deadline: Duration::from_secs(600),
        can_be_primary: false,
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

/// Construct a secondary over a `NoPeers` mesh stub, detached from any
/// real primary or peer; used to drive the election state machine via
/// direct method calls without a full multi-process harness. Returns the
/// mesh harness — `Deref`s to the coordinator, so `sec.method()` /
/// `sec.field` sites are unchanged.
pub(super) fn make_secondary(config: SecondaryConfig) -> SecondaryHarness<NoPeers> {
    build_harness(
        config,
        NoPeers,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    )
}

/// Seed a secondary's replicated `cluster_state` mirror with one
/// worker-secondary member through the REAL CRDT apply path the primary's
/// fleet-connect originates (`PeerJoined` + `SecondaryCapacity`, see
/// `primary/connect.rs`). The seeded member is alive, carries the given
/// `can_be_primary` / `is_observer` projection, and advertises one
/// worker (so it appears in `alive_secondary_members`).
///
/// Used by the setup-discovery designation tests to build the same
/// membership view a node's mirror holds after the primary broadcasts the
/// fleet roster — the input `is_designated_discoverer` reads.
pub(super) fn seed_member<P: PeerTransport<TestId>>(
    sec: &mut SecondaryHarness<P>,
    id: &str,
    can_be_primary: bool,
    is_observer: bool,
) {
    use dynrunner_protocol_primary_secondary::ClusterMutation;
    sec.cluster_state.apply(ClusterMutation::PeerJoined {
        peer_id: id.into(),
        is_observer,
        can_be_primary,
        cap_version: Default::default(),
    });
    sec.cluster_state.apply(ClusterMutation::SecondaryCapacity {
        secondary: id.into(),
        worker_count: 1,
        resources: vec![],
    });
}

/// Set the recognized post-promotion authority on a secondary's mirror
/// through the REAL `PrimaryChanged` apply path (epoch 1, advisory
/// `Transferred` reason — the bootstrap-relocate shape). `is_designated_
/// discoverer`'s sibling axis (5) reads `current_primary()`.
pub(super) fn set_current_primary<P: PeerTransport<TestId>>(
    sec: &mut SecondaryHarness<P>,
    id: &str,
) {
    use dynrunner_protocol_primary_secondary::{ClusterMutation, PrimaryChangeReason};
    sec.cluster_state.apply(ClusterMutation::<TestId>::PrimaryChanged {
        new: id.into(),
        epoch: 1,
        reason: PrimaryChangeReason::Transferred,
    });
}

/// Arm a pre-staged secondary as the SINGLE designated discoverer AND the
/// recognized authority: seed it as the sole alive, `can_be_primary`,
/// non-observer worker-secondary member and set `current_primary` to
/// itself. After this the node satisfies axes (4) + (5) of
/// `setup_discovery_pending`, so the legacy single-node yield tests (which
/// predate the designation gate) hold their original intent.
pub(super) fn arm_designated_discoverer<P: PeerTransport<TestId>>(sec: &mut SecondaryHarness<P>) {
    let self_id = sec.config.secondary_id.clone();
    seed_member(sec, &self_id, true, false);
    set_current_primary(sec, &self_id);
}

/// Construct a secondary over a [`RecordingPeer`] mesh stub, returning the
/// harness + the shared broadcast log so a test can assert on the messages
/// the failover terminal action (e.g. the `PrimaryChanged { new = self }`
/// re-point) fans out onto the mesh. Because `MeshClient::send` is QUEUED,
/// a test must call [`SecondaryHarness::drain_egress`] AFTER the
/// send-issuing call and BEFORE reading the log. `peer_count` configures
/// the recorder's reported mesh cardinality.
#[allow(clippy::type_complexity)]
pub(super) fn make_secondary_recording(
    config: SecondaryConfig,
    peer_count: usize,
) -> (
    SecondaryHarness<RecordingPeer<TestId>>,
    Rc<RefCell<Vec<DistributedMessage<TestId>>>>,
) {
    let recorder = RecordingPeer::<TestId>::new(peer_count);
    let log = recorder.log_handle();
    let harness = build_harness(
        config,
        recorder,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    (harness, log)
}

/// Build a pre-staged, operational secondary whose replicated mirror holds
/// the given fleet `roster` (each `(id, can_be_primary, is_observer)`
/// seeded through the real `PeerJoined` + `SecondaryCapacity` apply path)
/// and the given recognized `current_primary`. This is the membership view
/// a node's mirror holds after the primary broadcasts the fleet roster —
/// the exact input `setup_discovery_pending`'s designation axes read.
#[allow(clippy::type_complexity)]
pub(super) fn node_with_roster(
    self_id: &str,
    roster: &[(&str, bool, bool)],
    current_primary: Option<&str>,
) -> (
    SecondaryHarness<RecordingPeer<TestId>>,
    Rc<RefCell<Vec<DistributedMessage<TestId>>>>,
) {
    let (mut sec, log) = make_secondary_recording(election_config(self_id), roster.len());
    sec.enter_operational_for_test();
    sec.set_pre_staged_mode(true);
    for (id, can_be_primary, is_observer) in roster {
        seed_member(&mut sec, id, *can_be_primary, *is_observer);
    }
    if let Some(p) = current_primary {
        set_current_primary(&mut sec, p);
    }
    (sec, log)
}

/// Build a secondary over a `ChannelPeerTransport` mesh stub, returning the
/// harness. The trio is minted from a real `Mesh` over the channel
/// transport, so a test can drive the operational `select!` loop end-to-end
/// by running the coordinator's `run` future alongside the mesh-pump (see
/// [`run_secondary_to_completion`]).
pub(super) fn make_secondary_channel(
    config: SecondaryConfig,
    transport: ChannelPeerTransport<TestId>,
) -> SecondaryHarness<ChannelPeerTransport<TestId>> {
    build_harness(
        config,
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    )
}

/// Keeps the secondary's mesh plumbing alive while a test drives the
/// detached coordinator against the running production pump.
///
/// Holds the role-slot `Arc` (so the mesh `Weak` keeps upgrading — the pump
/// can deliver inbound to the secondary slot; dropping it would sever
/// ingress) and the pump task + control handle (so the pump keeps turning).
/// A test drops it once it is done reading off the coordinator.
pub(super) struct SecondaryPumpGuard {
    _slot: Arc<RoleSlot<TestId>>,
    _pump_task: tokio::task::JoinHandle<()>,
    _control: crate::process::pump::MeshControlHandle<TestId>,
}

/// Spawn the PRODUCTION concurrent mesh-pump
/// ([`crate::process::pump::run_pump`]) for `harness`, returning the detached
/// coordinator + a [`SecondaryPumpGuard`] that keeps the plumbing alive.
///
/// This is the secondary analogue of `run_secondary_node`
/// (`primary/tests/mod.rs`): the harness owns both the coordinator and the
/// `Mesh`; this consumes the harness, hands the `Mesh` to the production pump
/// (an INDEPENDENT `spawn_local` task — exactly as `Node::run` composes it,
/// concurrently draining egress AND routing inbound, so a queued send never
/// starves), and hands the coordinator back so the caller drives it directly
/// (`run`, `run_until_setup_or_done`, …) and inspects it afterward. Replaces
/// the PENDING-C-NODE `run_secondary_to_completion` sequential stub for the
/// full-handshake tests.
///
/// R5 pre-publish ordering: `build_harness` already published the membership
/// at mint, and the pump republishes on its own entry
/// (synchronous-before-await), so the secondary's first
/// `has_peer("primary")`-gated egress reads the folded primary link as
/// connected from the very first send.
pub(super) fn start_secondary_pump(
    harness: SecondaryHarness<ChannelPeerTransport<TestId>>,
) -> (TestSecondary, SecondaryPumpGuard) {
    let SecondaryHarness {
        coord,
        test_mesh,
        _slot,
        ..
    } = harness;

    let (control, control_rx) = crate::process::pump::control_channel::<TestId>();
    let pump_task = tokio::task::spawn_local(crate::process::pump::run_pump(test_mesh, control_rx));

    (
        coord,
        SecondaryPumpGuard {
            _slot,
            _pump_task: pump_task,
            _control: control,
        },
    )
}

/// Drive a channel-backed secondary's `run` to completion against the
/// PRODUCTION concurrent mesh-pump, returning the coordinator so the caller
/// reads its post-run counters.
///
/// Built on [`start_secondary_pump`]: the pump runs as an independent task
/// and `coord.run` is awaited separately, so when the wire closes and the
/// pump exits the secondary's `run` keeps running to its clean `RunComplete`
/// exit (a non-ping-pong `TaskRequest`→`TaskAssignment` handshake no longer
/// starves — the pump drains the queued send while the run awaits the reply,
/// M4 / BUG-2). The guard is held for the whole run, then dropped.
pub(super) async fn run_secondary_node(
    harness: SecondaryHarness<ChannelPeerTransport<TestId>>,
    factory: &mut impl WorkerFactory<ChannelManagerEnd>,
) -> (TestSecondary, Result<(), String>) {
    let (mut coord, guard) = start_secondary_pump(harness);
    let result = coord.run(factory).await;
    drop(guard);
    (coord, result)
}

/// Drive a channel-backed secondary's `run` to completion against a SIMPLE
/// SEQUENTIAL test mesh-pump (drain ALL ready egress, then await ONE
/// inbound).
///
/// This sequential drain is faithful ONLY for a non-ping-pong fixture: a
/// fixture where the secondary enqueues a send AND THEN awaits an inbound
/// that depends on it would starve the send while the pump is parked on a
/// prior `recv_peer`. The ping-pong / full-handshake tests therefore use the
/// PRODUCTION concurrent pump via [`run_secondary_node`] /
/// [`start_secondary_pump`] instead. This stub is retained ONLY for the
/// cold-start one-way-TIMEOUT tests (`r1.rs`): the primary never speaks, so
/// the secondary drains its welcome/cert egress and then blocks on a silent
/// inbound until its own `unconfigured_deadline` returns `Err` — a one-way
/// timeout, not a handshake, which the sequential drain serves correctly.
///
/// - EGRESS: drain `next_local_dispatch` → `apply_local_dispatch`, which
///   routes each frame loopback-or-remote (the secondary's primary-bound
///   sends go over the wire to the folded `"primary"` link).
/// - INGRESS: `recv_peer` → deliver to the secondary slot. These fixtures
///   host exactly ONE local role, so every inbound wire frame is for the
///   secondary; the `fake_primary` does not stamp C3 targets, so the
///   single-role `deliver_local` is the faithful demux here.
pub(super) async fn run_secondary_to_completion(
    harness: &mut SecondaryHarness<ChannelPeerTransport<TestId>>,
    factory: &mut impl WorkerFactory<ChannelManagerEnd>,
) -> Result<(), String> {
    let SecondaryHarness {
        coord, test_mesh, ..
    } = harness;

    // Sequential pump: borrow `test_mesh` for ONE drain/await at a time so
    // the two `&mut self` mesh methods never coexist in a `select!`.
    let pump = async {
        loop {
            // Drain all currently-ready egress (single borrow per select).
            loop {
                tokio::select! {
                    biased;
                    item = test_mesh.next_local_dispatch() => match item {
                        Some(i) => {
                            let _ = test_mesh.apply_local_dispatch(i).await;
                        }
                        None => return,
                    },
                    _ = std::future::ready(()) => break,
                }
            }
            // Then await one inbound and route it to the secondary slot.
            match test_mesh.recv_peer().await {
                Some(frame) => {
                    test_mesh.deliver_local(LocalRole::Secondary, frame);
                }
                None => return,
            }
        }
    };

    let run = coord.run(factory);

    tokio::select! {
        r = run => r,
        _ = pump => Err("mesh-pump exited before the secondary run completed".to_string()),
    }
}
