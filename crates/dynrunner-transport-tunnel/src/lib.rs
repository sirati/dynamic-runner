//! Tunneled peer transport: a [`PeerTransport`] over the primary's
//! per-secondary tunnel connections.
//!
//! # What this crate gives the rest of the workspace
//!
//! The submitter-side primary keeps a per-secondary writer + a demuxed
//! inbound channel via the existing SSH-tunneled (or in-process
//! channel) [`SecondaryTransport`]. Those connections are alive,
//! peer-routable, and survive promotion — but until now they sat
//! behind the [`SecondaryTransport`] trait and were therefore invisible
//! to the mesh-level [`PeerTransport`] surface. Step 5 of the
//! transport-unification refactor added a `peer_transport: P` field on
//! `PrimaryCoordinator` and routed primary-bound relays through
//! `peer_transport.send(Address::Role(Role::Primary), msg)`, but the
//! production constructors still passed
//! `dynrunner_transport_quic::NoPeerTransport` — so the role-addressed
//! send always errored "no holder", the `Err` was swallowed, and the
//! relay arm was inert.
//!
//! [`TunneledPeerTransport`] closes that gap. It is a *peer-mesh-only*
//! view over the same writer table + inbound channel the legacy
//! [`SecondaryTransport`] already produces. At the mesh-level
//! abstraction the primary is just another peer; the networking
//! implementation underneath happens to be SSH tunnels (or channels in
//! test fixtures) instead of QUIC. No special-casing at the mesh
//! layer — Step 4's role routing, Step 3's [`Address::Role(_)`]
//! dispatch, and Step 2's [`RoleTable`] write-through cache all work
//! against this transport identically to how they work against
//! `dynrunner_transport_quic::PeerNetwork`.
//!
//! # Module boundary
//!
//! The crate exposes exactly one trait impl ([`PeerTransport`] for
//! [`TunneledPeerTransport`]) and one builder
//! ([`TunneledPeerTransport::new`]) that returns the transport plus
//! two handles the caller hands to the legacy transport: a shared
//! outgoing-writer table and an inbound-tap sender. The legacy
//! transport's job is reduced to (a) populating the shared writer
//! table from its accept loops and (b) cloning each inbound message
//! into the tap so the peer view's `recv_peer()` can observe it.
//! Beyond that, `TunneledPeerTransport` owns its mesh-level state:
//! local-id, role-cache, inbound mpsc.
//!
//! # Single-threaded by construction
//!
//! `Rc<RefCell<_>>` is fine here because the primary coordinator runs
//! on a [`tokio::task::LocalSet`] (PyO3 manager + integration tests
//! both use the `current_thread` flavour and `spawn_local`). The
//! workspace's `clippy::await_holding_refcell_ref = "deny"` lint
//! catches any future regression that holds a borrow across an await.
//!
//! # What stays available on the SECONDARY side
//!
//! `dynrunner_transport_quic::NoPeerTransport` is unaffected by this
//! crate and remains the right choice for the
//! `disable_peer_overlay` path (firewalled inter-compute fabrics like
//! LMU SLURM). The primary's call sites stop using `NoPeerTransport`
//! once their constructors thread [`TunneledPeerTransport`] through;
//! secondary call sites keep it for as long as that disable path
//! exists.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{
    apply_role_misaddress_hint, decide_role_addressed_with_cache, install_role_change_hook,
    new_role_cache, read_role_cache, seed_self_role, DistributedMessage, PeerConnectionInfo,
    PeerTransport, Role, RoleAddressedAction, RoleCache, RoleChangeHookRegistrar,
};
use tokio::sync::mpsc;

/// Shared per-secondary writer table. The submitter primary's accept
/// loops populate this map when a secondary completes its
/// `SecondaryWelcome`; the legacy [`SecondaryTransport`] (NetworkServer
/// / ChannelSecondaryTransportEnd) and the new [`TunneledPeerTransport`]
/// both hold an `Rc<RefCell<_>>` clone, so adds and removes from one
/// side become visible to the other.
///
/// `Rc<RefCell<_>>` instead of `Arc<Mutex<_>>` because the primary
/// coordinator runs on a single-threaded `LocalSet` — every accept
/// loop, the operational loop, and the per-peer write tasks all live
/// on the same thread. The `mpsc::UnboundedSender<_>` values inside
/// the map are themselves `Send + Sync` so the per-peer write tasks
/// are free to keep their own clones without crossing the
/// shared-map's borrow boundary.
pub type SharedOutgoing<I> =
    Rc<RefCell<HashMap<String, mpsc::UnboundedSender<DistributedMessage<I>>>>>;

/// Inbound tap: a sender the legacy [`SecondaryTransport`] writes a
/// clone of every inbound message into, AFTER it has yielded the
/// original to its own `recv()` caller. The peer view's `recv_peer()`
/// pulls from the matching receiver.
///
/// Step 5b leaves the legacy transport as the primary inbound
/// consumer; nothing reads the peer view's queue in production until
/// Step 6 lands the demoted-primary's `select! { peer_transport.recv_peer() }`
/// arm. The peer queue accumulates harmlessly until then; if Step 6 is
/// delayed beyond expected, the queue grows unbounded — but that's
/// the same shape as any unbounded mpsc on the inbound side, and the
/// rate is bounded by per-secondary keepalive cadence (5s) +
/// task-completion rate.
pub type InboundTap<I> = mpsc::UnboundedSender<DistributedMessage<I>>;

/// Mesh-level [`PeerTransport`] over the primary's per-secondary
/// tunnel connections.
///
/// Construct via [`TunneledPeerTransport::new`]; the returned handles
/// (`outgoing` + `inbound_tap`) are what the legacy
/// [`SecondaryTransport`] receives to share writer-table state and
/// fan inbound messages into the peer queue.
pub struct TunneledPeerTransport<I: Identifier> {
    /// Local peer-id. The submitter primary uses a stable id (today
    /// `"primary"` per `PrimaryConfig::default().node_id`); the value
    /// is surfaced via [`PeerTransport::local_id`] so the `send`
    /// default-impl can stamp the `sender_id` field on
    /// `RoleAddressed` envelopes.
    local_id: String,
    /// Shared writer table. See [`SharedOutgoing`].
    outgoing: SharedOutgoing<I>,
    /// Inbound queue — owned exclusively here. Fed by the legacy
    /// transport's `recv()` tap (see [`InboundTap`]).
    incoming_rx: mpsc::UnboundedReceiver<DistributedMessage<I>>,
    /// Write-through cache of `Role → peer_id` populated by the hook
    /// registered via [`PeerTransport::register_with_cluster_state`].
    /// Same shape as `PeerNetwork.role_cache` and
    /// `ChannelPeerTransport.role_cache`.
    role_cache: RoleCache,
}

impl<I: Identifier> TunneledPeerTransport<I> {
    /// Build a new tunneled peer transport and return:
    /// 1. the transport itself (held by `PrimaryCoordinator` as its
    ///    `peer_transport: P` field),
    /// 2. a shared-outgoing handle the legacy [`SecondaryTransport`]
    ///    is configured to use as its writer table,
    /// 3. an inbound-tap sender the legacy transport clones each
    ///    `recv()`-yielded message into so the peer view's
    ///    `recv_peer()` can observe it.
    ///
    /// `local_id` is the primary's stable id — must match
    /// `PrimaryConfig::node_id` so cluster-state mutations the primary
    /// emits are accepted by other peers as originating from itself.
    /// `Role::Self_` is seeded immediately into the role-cache so the
    /// Step-4 receiver-side handling treats a hypothetical inbound
    /// `RoleAddressed { intended_role: Self_ }` envelope as Case A
    /// (unwrap) rather than Case C (drop). The Step-2 write-through
    /// hook covers `Role::Primary` once registered.
    pub fn new(local_id: String) -> (Self, SharedOutgoing<I>, InboundTap<I>) {
        let outgoing: SharedOutgoing<I> = Rc::new(RefCell::new(HashMap::new()));
        let (inbound_tap, incoming_rx) = mpsc::unbounded_channel();
        let role_cache = new_role_cache();
        seed_self_role(&role_cache, &local_id);
        let transport = Self {
            local_id,
            outgoing: Rc::clone(&outgoing),
            incoming_rx,
            role_cache,
        };
        (transport, outgoing, inbound_tap)
    }

    /// Role-layer interceptor — mirrors
    /// `ChannelPeerTransport::handle_role_layer`. The
    /// `decide_role_addressed_with_cache` decision is the single
    /// source of truth for the four cases (A/B/C/D); the relay-send
    /// dispatch path here just uses `send_to_peer` rather than
    /// reaching into a [`Router`] because the tunneled transport has
    /// no router state (no relay-via-peer at the submitter — every
    /// secondary is directly addressable via its own tunnel).
    fn handle_role_layer(
        &mut self,
        msg: DistributedMessage<I>,
    ) -> Option<DistributedMessage<I>> {
        match msg {
            DistributedMessage::RoleAddressed {
                sender_id,
                intended_role,
                payload,
                attempts,
                ..
            } => {
                let decision = decide_role_addressed_with_cache(
                    &self.local_id,
                    &self.role_cache,
                    sender_id,
                    intended_role,
                    payload,
                    attempts,
                );
                match decision {
                    RoleAddressedAction::Unwrap(inner) => Some(inner),
                    RoleAddressedAction::Relay {
                        forward_to,
                        forwarded,
                        hint_to,
                        hint,
                    } => {
                        // Both sends are fire-and-forget. We bypass
                        // the `PeerTransport::send_to_peer` body
                        // (which would re-check the writer table and
                        // surface no-route errors to caller) and go
                        // straight at the table — the role-layer is
                        // transport-internal bookkeeping, not an
                        // application-layer send.
                        if let Err(e) = self.send_direct(&forward_to, forwarded) {
                            tracing::warn!(
                                forward_to = %forward_to,
                                error = %e,
                                "RoleAddressed relay forward failed (tunneled)",
                            );
                        }
                        if let Err(e) = self.send_direct(&hint_to, hint) {
                            tracing::warn!(
                                hint_to = %hint_to,
                                error = %e,
                                "RoleMisaddressHint send failed (tunneled)",
                            );
                        }
                        None
                    }
                    RoleAddressedAction::Drop { reason } => {
                        tracing::warn!(reason, "RoleAddressed dropped (tunneled)");
                        None
                    }
                }
            }
            DistributedMessage::RoleMisaddressHint {
                role, holder_id, ..
            } => {
                // Cache-warming only — never surfaced to the
                // application layer (per Step 4 design rationale:
                // senders that issued an `Address::Role(_)` send
                // are not awaiting a hint reply).
                apply_role_misaddress_hint(&self.role_cache, role, holder_id);
                None
            }
            other => Some(other),
        }
    }

    /// Internal send helper. Clones the sender out of the shared
    /// writer table behind a SHORT borrow window (no `.await` while
    /// the borrow is live, so the workspace's
    /// `await_holding_refcell_ref = "deny"` lint is satisfied), then
    /// dispatches on the cloned sender.
    fn send_direct(
        &self,
        peer_id: &str,
        msg: DistributedMessage<I>,
    ) -> Result<(), String> {
        let tx_opt = self.outgoing.borrow().get(peer_id).cloned();
        match tx_opt {
            Some(tx) => tx.send(msg).map_err(|e| e.to_string()),
            None => Err(format!(
                "no tunneled writer for peer '{peer_id}': either the secondary \
                 hasn't completed handshake yet, or its writer task has exited \
                 (e.g. the per-secondary channel was closed after demotion)."
            )),
        }
    }
}

impl<I: Identifier> PeerTransport<I> for TunneledPeerTransport<I> {
    async fn broadcast(&mut self, msg: DistributedMessage<I>) -> Result<(), String> {
        // Snapshot the senders out of the shared map behind a
        // bounded borrow, then iterate the clones without holding
        // the RefCell across `.await`. `UnboundedSender::send` is
        // itself synchronous (no await), but we keep the explicit
        // clone-and-drop pattern so any future shape change (a
        // bounded channel, an alternative dispatch primitive) stays
        // compatible with the workspace's "no RefCell borrow held
        // across await" lint.
        let senders: Vec<mpsc::UnboundedSender<DistributedMessage<I>>> =
            self.outgoing.borrow().values().cloned().collect();
        for tx in senders {
            // Closed peers are tolerated — the secondary went away.
            // Matches `ChannelPeerTransport::broadcast`'s contract.
            let _ = tx.send(msg.clone());
        }
        Ok(())
    }

    async fn send_to_peer(
        &mut self,
        peer_id: &str,
        msg: DistributedMessage<I>,
    ) -> Result<(), String> {
        // Sync delegation — see `send_direct` comment for the
        // borrow-vs-await rationale.
        self.send_direct(peer_id, msg)
    }

    async fn recv_peer(&mut self) -> Option<DistributedMessage<I>> {
        loop {
            let msg = self.incoming_rx.recv().await?;
            match self.handle_role_layer(msg) {
                Some(payload) => return Some(payload),
                None => continue,
            }
        }
    }

    fn try_recv_peer(&mut self) -> Option<DistributedMessage<I>> {
        loop {
            let msg = self.incoming_rx.try_recv().ok()?;
            match self.handle_role_layer(msg) {
                Some(payload) => return Some(payload),
                None => continue,
            }
        }
    }

    fn peer_count(&self) -> usize {
        self.outgoing.borrow().len()
    }

    async fn connect_to_peers(&mut self, _peers: &[PeerConnectionInfo]) {
        // No-op: the writer table is populated by the legacy
        // transport's accept loops as secondaries connect — that
        // path is the single source of truth for "who is in the
        // mesh today". Calling `connect_to_peers` on the submitter
        // primary's tunneled transport is meaningful only if the
        // primary itself were to dial secondaries (it does not —
        // dial direction is secondary-to-primary in production).
    }

    fn register_with_cluster_state(&self, registrar: &mut dyn RoleChangeHookRegistrar) {
        // Install the Step-2 write-through hook against the cluster
        // state's `RoleTable`. From this point on every
        // `apply(PrimaryChanged)` updates this transport's cache;
        // the cache feeds Step-3's `Address::Role(_)` dispatch on
        // the send hot path.
        install_role_change_hook(RoleCache::clone(&self.role_cache), registrar);
    }

    fn peer_for_role(&self, role: &Role) -> Option<String> {
        read_role_cache(&self.role_cache, role)
    }

    fn local_id(&self) -> &str {
        &self.local_id
    }
}

#[cfg(test)]
mod tests {
    //! Unit tests for [`TunneledPeerTransport`]. Driven by hand-
    //! constructed channel pairs that stand in for the per-secondary
    //! tunnel writers + inbound demux. The pattern mirrors
    //! `dynrunner_manager_distributed::primary::test_helpers::setup_test`,
    //! which is what the integration test in
    //! `crates/dynrunner-manager-distributed/tests/network_integration.rs`
    //! actually uses; here we exercise the transport in isolation
    //! with no manager coordinator wrapped around it.
    use super::*;
    use dynrunner_protocol_primary_secondary::{Address, RoleTable, Scope};
    use serde::{Deserialize, Serialize};

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
        let (transport, outgoing, inbound_tap) =
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
        let (transport, _outgoing, _tap) =
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
        let (transport, _outgoing, _tap) =
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
        let (transport, outgoing, _tap) =
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
        let (transport, _outgoing, _tap) =
            TunneledPeerTransport::<TestId>::new("primary".into());
        assert_eq!(transport.local_id(), "primary");
    }
}
