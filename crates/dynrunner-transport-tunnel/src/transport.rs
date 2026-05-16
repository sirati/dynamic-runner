//! Mesh-level [`PeerTransport`] backed by the primary's per-secondary
//! tunnel connections.
//!
//! Implementation of [`TunneledPeerTransport`] along with the
//! [`SharedOutgoing`] writer table and [`InboundTap`] sender. See the
//! crate root for the design rationale (this module owns only the
//! transport's mesh-level state: local id, role cache, inbound mpsc).

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
                        if let Err(e) = self.send_direct(&hint_to, *hint) {
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
