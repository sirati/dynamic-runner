//! Deterministic unit tests for [`UnifiedSecondaryTransport`].
//!
//! The uplink and mesh handles are hand-built channel stubs so the
//! routing decisions (self-loopback / uplink / peer) and the
//! promotion re-route (a `RoleCache` update simulating a
//! `PrimaryChanged` apply) are exercised with no real network and no
//! wall-clock races — every assertion is a synchronous channel peek
//! after a single `send` await.

use crate::UnifiedSecondaryTransport;
use dynrunner_core::{MessageReceiver, MessageSender};
use dynrunner_protocol_primary_secondary::{
    install_role_change_hook, new_role_cache, Address, DistributedMessage, PeerConnectionInfo,
    PeerTransport, Role, RoleCache, RoleChangeHookRegistrar, RoleTable, Scope,
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct TestId(String);

fn keepalive(sender: &str) -> DistributedMessage<TestId> {
    DistributedMessage::Keepalive {
        sender_id: sender.into(),
        timestamp: 1.0,
        secondary_id: sender.into(),
        active_workers: 0,
    }
}

// ── Uplink stub: a `MessageSender + MessageReceiver` over channels ──

struct UplinkStub {
    /// What this side WROTE (the secondary→primary direction). The
    /// test asserts on this to confirm a `Role::Primary` send routed
    /// to the uplink.
    sent_tx: mpsc::UnboundedSender<DistributedMessage<TestId>>,
    /// Inbound from the "primary" — what `recv()` yields.
    inbound_rx: mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
}

impl MessageSender<DistributedMessage<TestId>> for UplinkStub {
    async fn send(&mut self, msg: DistributedMessage<TestId>) -> Result<(), String> {
        self.sent_tx.send(msg).map_err(|e| e.to_string())
    }
}

impl MessageReceiver<DistributedMessage<TestId>> for UplinkStub {
    async fn recv(&mut self) -> Option<DistributedMessage<TestId>> {
        self.inbound_rx.recv().await
    }
}

// ── Mesh stub: a minimal `PeerTransport` recording per-peer sends ──

struct MeshStub {
    local_id: String,
    /// Per-peer writer table. A test pre-registers the peers it
    /// expects sends to land at.
    peers: std::collections::HashMap<String, mpsc::UnboundedSender<DistributedMessage<TestId>>>,
    /// Broadcast sink — every `broadcast` clones here.
    broadcast_tx: mpsc::UnboundedSender<DistributedMessage<TestId>>,
    /// Inbound peer stream.
    inbound_rx: mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
    role_cache: RoleCache,
}

impl PeerTransport<TestId> for MeshStub {
    async fn broadcast(&mut self, msg: DistributedMessage<TestId>) -> Result<(), String> {
        self.broadcast_tx.send(msg).map_err(|e| e.to_string())
    }

    async fn send_to_peer(
        &mut self,
        peer_id: &str,
        msg: DistributedMessage<TestId>,
    ) -> Result<(), String> {
        match self.peers.get(peer_id) {
            Some(tx) => tx.send(msg).map_err(|e| e.to_string()),
            None => Err(format!("mesh stub: no peer '{peer_id}'")),
        }
    }

    async fn recv_peer(&mut self) -> Option<DistributedMessage<TestId>> {
        self.inbound_rx.recv().await
    }

    fn try_recv_peer(&mut self) -> Option<DistributedMessage<TestId>> {
        self.inbound_rx.try_recv().ok()
    }

    fn peer_count(&self) -> usize {
        self.peers.len()
    }

    async fn connect_to_peers(&mut self, _peers: &[PeerConnectionInfo]) {}

    fn register_with_cluster_state(&self, registrar: &mut dyn RoleChangeHookRegistrar) {
        install_role_change_hook(RoleCache::clone(&self.role_cache), registrar);
    }

    fn peer_for_role(&self, role: &Role) -> Option<String> {
        dynrunner_protocol_primary_secondary::read_role_cache(&self.role_cache, role)
    }

    fn local_id(&self) -> &str {
        &self.local_id
    }
}

/// Minimal in-test [`RoleChangeHookRegistrar`] — accumulates hooks
/// and fires them on demand. `fire(table)` simulates a
/// `ClusterState::apply(PrimaryChanged)` driving the write-through
/// caches.
#[derive(Default)]
struct TestRegistrar {
    #[allow(clippy::type_complexity)]
    hooks: Vec<Box<dyn Fn(&RoleTable) + Send + Sync>>,
}

impl TestRegistrar {
    fn fire(&self, table: &RoleTable) {
        for h in &self.hooks {
            h(table);
        }
    }
}

impl RoleChangeHookRegistrar for TestRegistrar {
    fn register_role_change_hook(
        &mut self,
        hook: Box<dyn Fn(&RoleTable) + Send + Sync + 'static>,
    ) {
        self.hooks.push(hook);
    }
}

/// Fixture: a unified transport for `local` plus the channel ends a
/// test asserts on.
struct Fixture {
    transport: UnifiedSecondaryTransport<UplinkStub, MeshStub, TestId>,
    /// What the secondary WROTE to the uplink.
    uplink_sent_rx: mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
    /// Feed inbound from the "primary" into the uplink.
    uplink_inbound_tx: mpsc::UnboundedSender<DistributedMessage<TestId>>,
    /// What the mesh broadcast.
    mesh_broadcast_rx: mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
    /// Feed inbound peer frames into the mesh.
    mesh_inbound_tx: mpsc::UnboundedSender<DistributedMessage<TestId>>,
    /// Per-peer receivers the test pre-registered (peer_id → rx).
    peer_rx: std::collections::HashMap<String, mpsc::UnboundedReceiver<DistributedMessage<TestId>>>,
    registrar: TestRegistrar,
}

fn fixture(local: &str, peers: &[&str]) -> Fixture {
    let (uplink_sent_tx, uplink_sent_rx) = mpsc::unbounded_channel();
    let (uplink_inbound_tx, uplink_inbound_rx) = mpsc::unbounded_channel();
    let uplink = UplinkStub {
        sent_tx: uplink_sent_tx,
        inbound_rx: uplink_inbound_rx,
    };

    let (mesh_broadcast_tx, mesh_broadcast_rx) = mpsc::unbounded_channel();
    let (mesh_inbound_tx, mesh_inbound_rx) = mpsc::unbounded_channel();
    let mut peer_map = std::collections::HashMap::new();
    let mut peer_rx = std::collections::HashMap::new();
    for p in peers {
        let (tx, rx) = mpsc::unbounded_channel();
        peer_map.insert((*p).to_string(), tx);
        peer_rx.insert((*p).to_string(), rx);
    }
    let mesh = MeshStub {
        local_id: local.to_string(),
        peers: peer_map,
        broadcast_tx: mesh_broadcast_tx,
        inbound_rx: mesh_inbound_rx,
        role_cache: new_role_cache(),
    };

    let transport = UnifiedSecondaryTransport::new(local.to_string(), uplink, mesh);
    let mut registrar = TestRegistrar::default();
    transport.register_with_cluster_state(&mut registrar);

    Fixture {
        transport,
        uplink_sent_rx,
        uplink_inbound_tx,
        mesh_broadcast_rx,
        mesh_inbound_tx,
        peer_rx,
        registrar,
    }
}

/// A `PrimaryChanged`-equivalent: fire the role-change hooks with the
/// new primary id installed in the table. This is exactly what
/// `ClusterState::apply(PrimaryChanged)` does for the write-through
/// caches — the single mechanism behind the promotion re-route.
fn promote(registrar: &TestRegistrar, new_primary: &str) {
    let table = RoleTable {
        primary: Some(new_primary.to_string()),
        observers: Default::default(),
    };
    registrar.fire(&table);
}

/// Cache cold (no PromotePrimary observed): `Role::Primary` routes to
/// the UPLINK — the original-primary sentinel default.
#[tokio::test]
async fn role_primary_cold_cache_routes_to_uplink() {
    let mut fx = fixture("sec-A", &["sec-B"]);
    fx.transport
        .send(Address::Role(Role::Primary), keepalive("sec-A"))
        .await
        .expect("cold-cache Role::Primary must route to the healthy uplink");
    // The uplink received it.
    let got = fx.uplink_sent_rx.try_recv().expect("uplink should have the frame");
    assert!(matches!(got, DistributedMessage::Keepalive { .. }));
    // The mesh did NOT broadcast and no peer received it.
    assert!(fx.mesh_broadcast_rx.try_recv().is_err());
    assert!(fx.peer_rx.get_mut("sec-B").unwrap().try_recv().is_err());
}

/// After a peer is promoted, `Role::Primary` re-points to that peer
/// over the mesh — the SAME transport handle, loop intact (no
/// re-construction). This is the promotion re-route.
#[tokio::test]
async fn promotion_reroutes_role_primary_to_peer_loop_intact() {
    let mut fx = fixture("sec-A", &["sec-B"]);
    // Promote sec-B (a peer).
    promote(&fx.registrar, "sec-B");
    fx.transport
        .send(Address::Role(Role::Primary), keepalive("sec-A"))
        .await
        .expect("post-promotion Role::Primary must route to the promoted peer");
    // sec-B received it over the mesh; the uplink did NOT.
    let got = fx
        .peer_rx
        .get_mut("sec-B")
        .unwrap()
        .try_recv()
        .expect("promoted peer should receive the Role::Primary frame");
    assert!(matches!(got, DistributedMessage::Keepalive { .. }));
    assert!(fx.uplink_sent_rx.try_recv().is_err());
}

/// When THIS node holds the primary role, `Role::Primary` loops back
/// to its own inbound stream (no wire selected).
#[tokio::test]
async fn role_primary_holder_is_self_loops_back() {
    let mut fx = fixture("sec-A", &["sec-B"]);
    // Promote ourselves.
    promote(&fx.registrar, "sec-A");
    fx.transport
        .send(Address::Role(Role::Primary), keepalive("sec-A"))
        .await
        .expect("self-held Role::Primary loops back");
    // No wire was selected.
    assert!(fx.uplink_sent_rx.try_recv().is_err());
    assert!(fx.peer_rx.get_mut("sec-B").unwrap().try_recv().is_err());
    // It surfaces on our own inbound stream.
    let got = fx
        .transport
        .recv_peer()
        .await
        .expect("loopback frame must surface via recv_peer");
    assert!(matches!(got, DistributedMessage::Keepalive { .. }));
}

/// A self-addressed `Address::Peer(local_id)` loops back (subsumes the
/// deleted self-assign-vs-wire branch).
#[tokio::test]
async fn self_addressed_peer_send_loops_back() {
    let mut fx = fixture("sec-A", &["sec-B"]);
    fx.transport
        .send(Address::Peer("sec-A".into()), keepalive("sec-A"))
        .await
        .expect("self-addressed peer send loops back");
    // No mesh peer received it.
    assert!(fx.peer_rx.get_mut("sec-B").unwrap().try_recv().is_err());
    let got = fx
        .transport
        .recv_peer()
        .await
        .expect("self peer frame surfaces via recv_peer");
    assert!(matches!(got, DistributedMessage::Keepalive { .. }));
}

/// `Address::Peer(other)` routes to that peer over the mesh.
#[tokio::test]
async fn peer_addressed_send_routes_to_mesh() {
    let mut fx = fixture("sec-A", &["sec-B"]);
    fx.transport
        .send(Address::Peer("sec-B".into()), keepalive("sec-A"))
        .await
        .expect("peer send routes to mesh");
    let got = fx
        .peer_rx
        .get_mut("sec-B")
        .unwrap()
        .try_recv()
        .expect("sec-B receives the peer frame");
    assert!(matches!(got, DistributedMessage::Keepalive { .. }));
    assert!(fx.uplink_sent_rx.try_recv().is_err());
}

/// `Address::Broadcast(Scope::Mesh)` fans out via the mesh only (the
/// authority-originate path; the uplink leg is gone).
#[tokio::test]
async fn broadcast_fans_out_via_mesh_only() {
    let mut fx = fixture("sec-A", &["sec-B"]);
    fx.transport
        .send(Address::Broadcast(Scope::Mesh), keepalive("sec-A"))
        .await
        .expect("broadcast fans out");
    let got = fx
        .mesh_broadcast_rx
        .try_recv()
        .expect("mesh should have the broadcast");
    assert!(matches!(got, DistributedMessage::Keepalive { .. }));
    // The uplink did NOT get a copy.
    assert!(fx.uplink_sent_rx.try_recv().is_err());
}

/// recv fan-in: a frame arriving on the uplink surfaces via
/// `recv_peer`.
#[tokio::test]
async fn recv_fanin_yields_uplink_frame() {
    let mut fx = fixture("sec-A", &["sec-B"]);
    fx.uplink_inbound_tx.send(keepalive("primary")).unwrap();
    let got = fx
        .transport
        .recv_peer()
        .await
        .expect("uplink frame surfaces via recv_peer");
    assert!(matches!(got, DistributedMessage::Keepalive { .. }));
}

/// recv fan-in: a frame arriving on the mesh surfaces via
/// `recv_peer`.
#[tokio::test]
async fn recv_fanin_yields_mesh_frame() {
    let mut fx = fixture("sec-A", &["sec-B"]);
    fx.mesh_inbound_tx.send(keepalive("sec-B")).unwrap();
    let got = fx
        .transport
        .recv_peer()
        .await
        .expect("mesh frame surfaces via recv_peer");
    assert!(matches!(got, DistributedMessage::Keepalive { .. }));
}

/// Uplink-closed is an internal transport event: after the uplink
/// closes, a cache-cold `Role::Primary` send surfaces a uniform
/// no-route `Err` rather than sending into a dead handle. The manager
/// sees no `is_primary`/`peer_count` cascade — only the `Err`.
#[tokio::test]
async fn uplink_close_makes_cold_role_primary_error() {
    let mut fx = fixture("sec-A", &["sec-B"]);
    // Drop the "primary" side of the uplink inbound so recv() yields
    // None on the next poll.
    drop(fx.uplink_inbound_tx);
    // Drive one recv so the transport observes the close and latches
    // uplink_open=false. Feed a mesh frame so recv_peer returns
    // (otherwise it would park).
    fx.mesh_inbound_tx.send(keepalive("sec-B")).unwrap();
    let got = fx.transport.recv_peer().await;
    assert!(matches!(got, Some(DistributedMessage::Keepalive { .. })));
    // Now a cache-cold Role::Primary send errors uniformly.
    let res = fx
        .transport
        .send(Address::Role(Role::Primary), keepalive("sec-A"))
        .await;
    assert!(
        res.is_err(),
        "cache-cold Role::Primary after uplink close must surface a no-route Err"
    );
}

/// Even after the uplink closes, a promotion re-points `Role::Primary`
/// to the mesh — the loop stays intact and routing recovers without
/// the uplink.
#[tokio::test]
async fn promotion_recovers_routing_after_uplink_close() {
    let mut fx = fixture("sec-A", &["sec-B"]);
    drop(fx.uplink_inbound_tx);
    fx.mesh_inbound_tx.send(keepalive("sec-B")).unwrap();
    let _ = fx.transport.recv_peer().await; // latch uplink closed
    // Promote a peer.
    promote(&fx.registrar, "sec-B");
    fx.transport
        .send(Address::Role(Role::Primary), keepalive("sec-A"))
        .await
        .expect("post-promotion Role::Primary routes to the peer even with uplink closed");
    let got = fx
        .peer_rx
        .get_mut("sec-B")
        .unwrap()
        .try_recv()
        .expect("promoted peer receives the frame");
    assert!(matches!(got, DistributedMessage::Keepalive { .. }));
}

/// `peer_for_role(Primary)` reflects the cache after a promotion —
/// the single source of "who is primary now".
#[tokio::test]
async fn peer_for_role_reflects_promotion() {
    let fx = fixture("sec-A", &["sec-B"]);
    assert_eq!(fx.transport.peer_for_role(&Role::Primary), None);
    promote(&fx.registrar, "sec-B");
    assert_eq!(
        fx.transport.peer_for_role(&Role::Primary),
        Some("sec-B".to_string())
    );
}

/// `peer_count()` reports the MESH cardinality (mesh-health), not
/// counting the uplink.
#[tokio::test]
async fn peer_count_is_mesh_cardinality() {
    let fx = fixture("sec-A", &["sec-B", "sec-C"]);
    assert_eq!(fx.transport.peer_count(), 2);
}
