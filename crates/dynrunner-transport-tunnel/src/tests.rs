//! Unit tests for [`TunneledPeerTransport`]. Driven by hand-
//! constructed channel pairs that stand in for the per-secondary
//! tunnel writers + inbound demux. The pattern mirrors
//! `dynrunner_manager_distributed::primary::test_helpers::setup_test`,
//! which is what the integration test in
//! `crates/dynrunner-manager-distributed/tests/network_integration.rs`
//! actually uses; here we exercise the transport in isolation
//! with no manager coordinator wrapped around it.
use crate::{InboundTap, TunneledPeerTransport};
use dynrunner_protocol_primary_secondary::{
    Address, DistributedMessage, PeerTransport, Role, RoleChangeHookRegistrar, RoleTable, Scope,
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct TestId(String);

/// Minimal in-test `RoleChangeHookRegistrar` — same shape as the
/// channel-transport test stub. Holds onto registered hooks and
/// fires them on demand; no real `ClusterState` dep.
#[derive(Default)]
struct TestRegistrar {
    // `Vec<Box<dyn Fn(_) + Send + Sync>>` is the shape the
    // registrar trait dictates; factoring into a `type` alias
    // for one test fixture is not load-bearing.
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

fn keepalive(sender: &str) -> DistributedMessage<TestId> {
    DistributedMessage::Keepalive {
        sender_id: sender.into(),
        timestamp: 1.0,
        secondary_id: sender.into(),
        active_workers: 0,
    }
}

/// 1 primary + 2 secondaries fixture: pre-register both
/// secondaries' writers in the shared outgoing table BEFORE the
/// transport is asked to send, mirroring what `NetworkServer`'s
/// accept-loop `drain_new_connections` would do as each secondary
/// completes handshake. Returns the transport plus the two
/// per-secondary receivers so the test can assert on what each
/// secondary actually received.
// The 4-tuple shape is locked by the fixture contract and only
// used inside this test module; factoring would split the test
// setup across modules for no maintainability gain.
#[allow(clippy::type_complexity)]
fn fixture() -> (
    TunneledPeerTransport<TestId>,
    mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
    mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
    InboundTap<TestId>,
) {
    // The registration sink is dropped: this fixture registers writers
    // DIRECTLY into the shared `outgoing` table (the in-process / test
    // path), so the `recv_peer` demux's `new_conn_rx` arm parks.
    let (transport, outgoing, inbound_tap, _reg_sink) =
        TunneledPeerTransport::<TestId>::new("primary".into());
    let (sec_a_tx, sec_a_rx) = mpsc::unbounded_channel();
    let (sec_b_tx, sec_b_rx) = mpsc::unbounded_channel();
    outgoing.borrow_mut().insert("sec-A".into(), sec_a_tx);
    outgoing.borrow_mut().insert("sec-B".into(), sec_b_tx);
    (transport, sec_a_rx, sec_b_rx, inbound_tap)
}

/// `send_to_peer(id, msg)` reaches exactly the writer for `id`
/// and nothing else. The submitter primary's `task::handle_task_request`
/// relay arm sits on top of this (via the trait's `send` default
/// impl after role resolution).
#[tokio::test(flavor = "current_thread")]
async fn send_to_peer_reaches_only_target() {
    let (mut transport, mut sec_a_rx, mut sec_b_rx, _tap) = fixture();
    transport
        .send_to_peer("sec-A", keepalive("primary"))
        .await
        .unwrap();
    assert!(sec_a_rx.try_recv().is_ok(), "sec-A must receive");
    assert!(sec_b_rx.try_recv().is_err(), "sec-B must NOT receive");
}

/// `broadcast(msg)` reaches every writer in the table.
#[tokio::test(flavor = "current_thread")]
async fn broadcast_reaches_all() {
    let (mut transport, mut sec_a_rx, mut sec_b_rx, _tap) = fixture();
    transport.broadcast(keepalive("primary")).await.unwrap();
    assert!(sec_a_rx.try_recv().is_ok());
    assert!(sec_b_rx.try_recv().is_ok());
}

/// `recv_peer()` returns whatever the legacy transport's tap
/// pushed through `inbound_tap`. Pre-Step-6 nothing consumes this
/// in production; the unit assertion here pins the wire path so
/// Step 6's `select! { peer_transport.recv_peer() }` arm has a
/// load-bearing channel underneath.
#[tokio::test(flavor = "current_thread")]
async fn recv_peer_yields_tapped_inbound() {
    let (mut transport, _sec_a_rx, _sec_b_rx, tap) = fixture();
    tap.send(keepalive("sec-A")).unwrap();
    let got = transport.recv_peer().await.expect("must receive tapped");
    assert_eq!(got.sender_id(), "sec-A");
}

/// `try_recv_peer()` returns `None` when the tap queue is empty.
#[tokio::test(flavor = "current_thread")]
async fn try_recv_peer_empty_returns_none() {
    let (mut transport, _sec_a_rx, _sec_b_rx, _tap) = fixture();
    assert!(transport.try_recv_peer().is_none());
}

/// The Step-2 role-table cache populates via the registrar hook:
/// after `register_with_cluster_state` runs and the registrar
/// fires with `RoleTable { primary: Some(id), .. }`, the
/// transport's `peer_for_role(Role::Primary)` returns that id.
/// Mirror of the channel-transport test
/// `peer_transport_role_cache_populates_via_hook`.
#[tokio::test(flavor = "current_thread")]
async fn role_cache_populates_via_hook() {
    let (transport, _outgoing, _tap, _reg_sink) =
        TunneledPeerTransport::<TestId>::new("primary".into());
    assert_eq!(
        transport.peer_for_role(&Role::Primary),
        None,
        "cache empty before hook fires"
    );
    let mut registrar = TestRegistrar::default();
    transport.register_with_cluster_state(&mut registrar);
    registrar.fire(&RoleTable {
        primary: Some("sec-A".into()),
        ..Default::default()
    });
    assert_eq!(
        transport.peer_for_role(&Role::Primary),
        Some("sec-A".into()),
    );
}

/// `Role::Self_` is seeded at construction (mirror of
/// `role_self_cache_populated_at_init` in the channel-transport
/// tests). The seed lets the receiver-side Case-A unwrap treat a
/// hypothetical inbound `RoleAddressed { intended_role: Self_ }`
/// envelope as "local" rather than dropping it.
#[tokio::test(flavor = "current_thread")]
async fn role_self_cache_populated_at_init() {
    let (transport, _outgoing, _tap, _reg_sink) =
        TunneledPeerTransport::<TestId>::new("primary".into());
    assert_eq!(
        transport.peer_for_role(&Role::Self_),
        Some("primary".into()),
    );
}

/// `send(Address::Role(Role::Primary), msg)` with a populated
/// cache routes the envelope to the cached holder. The Case-A
/// unwrap happens at the receiver (which this transport's local
/// view never observes); here we assert that the wire-side
/// envelope reaches the expected secondary's writer.
#[tokio::test(flavor = "current_thread")]
async fn send_role_primary_routes_via_cache() {
    let (mut transport, mut sec_a_rx, mut sec_b_rx, _tap) = fixture();
    let mut registrar = TestRegistrar::default();
    transport.register_with_cluster_state(&mut registrar);
    registrar.fire(&RoleTable {
        primary: Some("sec-A".into()),
        ..Default::default()
    });

    transport
        .send(Address::Role(Role::Primary), keepalive("primary"))
        .await
        .expect("Role(Primary) send must succeed with populated cache");

    let received = sec_a_rx.try_recv().expect("sec-A must receive envelope");
    assert!(
        matches!(received, DistributedMessage::RoleAddressed { .. }),
        "wire shape must be RoleAddressed wrapper: {received:?}"
    );
    assert!(sec_b_rx.try_recv().is_err(), "sec-B must NOT receive");
}

/// Cold-cache `Address::Role(_)` send returns the contract Err
/// the trait documents — "Role" and "cache" both appear in the
/// message. Same shape as the channel-transport equivalent.
#[tokio::test(flavor = "current_thread")]
async fn send_role_unresolved_returns_err() {
    let (mut transport, _sec_a_rx, _sec_b_rx, _tap) = fixture();
    let err = transport
        .send(Address::Role(Role::Primary), keepalive("primary"))
        .await
        .expect_err("cold cache must error");
    assert!(err.contains("Role"), "error must reference Role; got: {err}");
    assert!(err.contains("cache"), "error must reference cache; got: {err}");
}

/// `send(Address::Broadcast(Scope::AllSecondaries), msg)` fans
/// out via the trait's `send` default-impl `broadcast` delegation
/// — every peer in the writer table receives. Matches the
/// channel-transport contract; the AllSecondaries scope is what
/// the Step-5 keepalive migration will use.
#[tokio::test(flavor = "current_thread")]
async fn send_broadcast_all_secondaries_fans_out() {
    let (mut transport, mut sec_a_rx, mut sec_b_rx, _tap) = fixture();
    transport
        .send(
            Address::Broadcast(Scope::AllSecondaries),
            keepalive("primary"),
        )
        .await
        .unwrap();
    assert!(sec_a_rx.try_recv().is_ok());
    assert!(sec_b_rx.try_recv().is_ok());
}

/// `peer_count()` reflects the shared outgoing table size — the
/// gate `peer_transport.peer_count() > 0` Step 6 will use to relax
/// the demoted-primary disconnect detection needs this to be
/// accurate against the same writer table the legacy transport
/// populates.
#[tokio::test(flavor = "current_thread")]
async fn peer_count_reflects_outgoing_table() {
    let (transport, outgoing, _tap, _reg_sink) =
        TunneledPeerTransport::<TestId>::new("primary".into());
    assert_eq!(transport.peer_count(), 0);
    let (a_tx, _a_rx) = mpsc::unbounded_channel::<DistributedMessage<TestId>>();
    outgoing.borrow_mut().insert("sec-A".into(), a_tx);
    assert_eq!(transport.peer_count(), 1);
    let (b_tx, _b_rx) = mpsc::unbounded_channel::<DistributedMessage<TestId>>();
    outgoing.borrow_mut().insert("sec-B".into(), b_tx);
    assert_eq!(transport.peer_count(), 2);
}

/// `local_id()` returns the constructor-supplied id. Pinned
/// because the Step-3 `send` default-impl uses this to stamp
/// `RoleAddressed.sender_id`; the wire path's `decide_role_addressed`
/// at the receiver matches against THIS id.
#[test]
fn local_id_reflects_constructor_arg() {
    let (transport, _outgoing, _tap, _reg_sink) =
        TunneledPeerTransport::<TestId>::new("primary".into());
    assert_eq!(transport.local_id(), "primary");
}
