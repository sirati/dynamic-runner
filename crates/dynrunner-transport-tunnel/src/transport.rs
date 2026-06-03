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
    DistributedMessage, PeerConnectionInfo, PeerTransport, Role, RoleAddressedAction, RoleCache,
    RoleChangeHookRegistrar, apply_role_misaddress_hint, decide_role_addressed_with_cache,
    install_role_change_hook, new_role_cache, read_role_cache, seed_self_role,
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

/// Inbound sink: the sender the accept loops (QUIC/WSS per-connection
/// reader tasks) and the in-process per-secondary forwarder write
/// every inbound frame into. It feeds the transport's OWN
/// `incoming_rx` — the single, canonical inbound stream the unified
/// [`TunneledPeerTransport::recv_peer`] drains and demuxes. This is
/// the inbound half of the `NetworkServer` ownership move: the real
/// inbound channel is owned here, not in a separate legacy transport
/// behind a fan-out tap.
pub type InboundTap<I> = mpsc::UnboundedSender<DistributedMessage<I>>;

/// A newly-accepted peer's writer registration. The accept loops mint
/// one per handshaked secondary (after reading its first frame to
/// learn the id) and push it through the [`RegistrationSink`]; the
/// transport's `recv_peer` demux drains the matching receiver and
/// inserts the writer into the shared [`SharedOutgoing`] table.
///
/// Tunnel-crate-local (carries only `String` + a `DistributedMessage`
/// sender) so the inbound-ownership move keeps the dependency edge
/// (`transport-quic` → `transport-tunnel`) intact: the quic accept
/// loops produce this generic shape rather than the transport owning
/// a quic-specific connection type.
pub struct PeerRegistration<I: Identifier> {
    /// The peer-id read from the connection's first frame
    /// (`SecondaryWelcome.sender_id`).
    pub peer_id: String,
    /// The per-connection writer the accept loop's writer task drains.
    pub outgoing_tx: mpsc::UnboundedSender<DistributedMessage<I>>,
}

/// The registration half of the `NetworkServer` ownership move: the
/// sender the accept loops push each [`PeerRegistration`] through. Its
/// matching receiver is owned by [`TunneledPeerTransport`] and drained
/// inside `recv_peer`'s demux.
pub type RegistrationSink<I> = mpsc::UnboundedSender<PeerRegistration<I>>;

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
    ///
    /// Populated from TWO sources that both insert here: the
    /// `recv_peer` demux drains [`PeerRegistration`]s minted by the
    /// QUIC/WSS accept loops; the in-process / test paths register
    /// writers directly through their [`SharedOutgoing`] handle. Both
    /// converge on the same map, so `send_to_peer` / `broadcast` /
    /// role-resolved dispatch reach every connected peer regardless of
    /// how it registered.
    outgoing: SharedOutgoing<I>,
    /// THE canonical inbound stream — owned exclusively here. Fed by
    /// the accept loops' per-connection reader tasks (QUIC/WSS) and the
    /// in-process per-secondary forwarder via the [`InboundTap`]. This
    /// is the `NetworkServer` inbound ownership move: there is no
    /// separate legacy `recv()` consumer + fan-out tap; this receiver
    /// is the single source, drained by `recv_peer`.
    incoming_rx: mpsc::UnboundedReceiver<DistributedMessage<I>>,
    /// New-connection registrations from the accept loops. `recv_peer`
    /// demuxes this alongside `incoming_rx` (the relocated
    /// `NetworkServer::recv` `select!`), inserting each handshaked
    /// secondary's writer into `outgoing`. The in-process / test paths
    /// leave this idle — they register writers directly into the
    /// shared `outgoing` table — so its sender is simply dropped.
    new_conn_rx: mpsc::UnboundedReceiver<PeerRegistration<I>>,
    /// `false` once `new_conn_rx.recv()` has returned `None` (every
    /// registration sink dropped — the accept loops exited, or the
    /// in-process / test path never held the sink). The `recv_peer`
    /// select! gates the registration arm off after that so its
    /// perpetually-ready closed-channel future can't be selected and
    /// stall the demux (a closed mpsc `recv()` resolves immediately,
    /// so an ungated arm would starve `incoming_rx`). Mirrors the
    /// `uplink_open` latch in `UnifiedSecondaryTransport::recv_peer`.
    registrations_open: bool,
    /// Write-through cache of `Role → peer_id` populated by the hook
    /// registered via [`PeerTransport::register_with_cluster_state`].
    /// Same shape as `PeerNetwork.role_cache` and
    /// `ChannelPeerTransport.role_cache`.
    role_cache: RoleCache,
}

impl<I: Identifier> TunneledPeerTransport<I> {
    /// Build a new tunneled peer transport and return:
    /// 1. the transport itself (held by `PrimaryCoordinator` as its
    ///    sole `Tr: PeerTransport` field),
    /// 2. a shared-outgoing handle for callers that register writers
    ///    directly (the in-process per-secondary path + tests); the
    ///    QUIC accept loops instead register via the registration
    ///    sink below, but both insert into the SAME table,
    /// 3. an inbound sink the accept loops' reader tasks (and the
    ///    in-process forwarder) push every inbound frame into — it
    ///    feeds this transport's owned `incoming_rx`, the single
    ///    canonical inbound stream,
    /// 4. a registration sink the QUIC/WSS accept loops push each
    ///    handshaked secondary's [`PeerRegistration`] through; the
    ///    `recv_peer` demux drains it and populates `outgoing`.
    ///
    /// This is the `NetworkServer` inbound ownership move: the real
    /// `incoming_rx` + `new_conn_rx` that the legacy `NetworkServer`
    /// used to own (and demux inside its `recv()`) now live here, so
    /// `recv_peer` is the SOLE driver of the inbound demux. The naive
    /// "route the recv sites to recv_peer while NetworkServer keeps the
    /// channels" deadlocks (recv_peer would have nothing to drive); by
    /// owning the channels here the unified transport drives its own
    /// inbound.
    ///
    /// `local_id` is the primary's stable id — must match
    /// `PrimaryConfig::node_id` so cluster-state mutations the primary
    /// emits are accepted by other peers as originating from itself.
    /// `Role::Self_` is seeded immediately into the role-cache so the
    /// receiver-side handling treats a hypothetical inbound
    /// `RoleAddressed { intended_role: Self_ }` envelope as Case A
    /// (unwrap) rather than Case C (drop). The write-through hook
    /// covers `Role::Primary` once registered.
    pub fn new(local_id: String) -> (Self, SharedOutgoing<I>, InboundTap<I>, RegistrationSink<I>) {
        let outgoing: SharedOutgoing<I> = Rc::new(RefCell::new(HashMap::new()));
        let (inbound_tap, incoming_rx) = mpsc::unbounded_channel();
        let (new_conn_tx, new_conn_rx) = mpsc::unbounded_channel();
        let role_cache = new_role_cache();
        seed_self_role(&role_cache, &local_id);
        let transport = Self {
            local_id,
            outgoing: Rc::clone(&outgoing),
            incoming_rx,
            new_conn_rx,
            registrations_open: true,
            role_cache,
        };
        (transport, outgoing, inbound_tap, new_conn_tx)
    }

    /// Register a newly-accepted peer's writer into the shared
    /// `outgoing` table. The single place the demux applies an accept-
    /// loop registration; kept as a bounded synchronous helper so the
    /// `recv_peer` hot path stays readable and the `RefCell` borrow
    /// never spans an await.
    fn register_peer(&self, reg: PeerRegistration<I>) {
        tracing::info!(secondary = %reg.peer_id, "secondary registered");
        self.outgoing
            .borrow_mut()
            .insert(reg.peer_id, reg.outgoing_tx);
    }

    /// Role-layer interceptor — mirrors
    /// `ChannelPeerTransport::handle_role_layer`. The
    /// `decide_role_addressed_with_cache` decision is the single
    /// source of truth for the four cases (A/B/C/D); the relay-send
    /// dispatch path here just uses `send_to_peer` rather than
    /// reaching into a [`Router`] because the tunneled transport has
    /// no router state (no relay-via-peer at the submitter — every
    /// secondary is directly addressable via its own tunnel).
    fn handle_role_layer(&mut self, msg: DistributedMessage<I>) -> Option<DistributedMessage<I>> {
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
    fn send_direct(&self, peer_id: &str, msg: DistributedMessage<I>) -> Result<(), String> {
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
        // The relocated `NetworkServer::recv` demux: drive both the
        // canonical inbound stream AND the accept-loop registration
        // stream from this single consumer. A registration carries no
        // application payload, so it is applied (writer inserted into
        // `outgoing`) and the loop continues; an inbound frame goes
        // through the role layer and is yielded (or consumed
        // internally) exactly as before.
        //
        // FIFO: `SecondaryWelcome` and `CertExchange` for one secondary
        // both ride `incoming_rx` (a single mpsc), so their relative
        // order is preserved automatically. The registration on
        // `new_conn_rx` is interleaved by the `select!`, but the accept
        // loop emits it immediately after the welcome and before any
        // further frames, and a registration only mutates the writer
        // table — it never reorders the application stream. Both arms'
        // `recv` are cancel-safe (mpsc), so the loser re-builds cleanly
        // on the next iteration.
        loop {
            let raw = tokio::select! {
                msg = self.incoming_rx.recv() => {
                    // Eagerly apply any registrations already queued
                    // before surfacing this frame. The accept loop emits
                    // a secondary's `SecondaryWelcome` (on `incoming_rx`)
                    // immediately followed by its writer registration (on
                    // `new_conn_rx`); the `select!` can pick the welcome
                    // first, so without this drain the writer might not
                    // be in `outgoing` yet when the welcome surfaces and
                    // a same-tick reply would find no route. Mirrors the
                    // legacy `NetworkServer::recv`, which called
                    // `drain_new_connections()` on every inbound yield.
                    while let Ok(reg) = self.new_conn_rx.try_recv() {
                        self.register_peer(reg);
                    }
                    msg?
                }
                reg = self.new_conn_rx.recv(), if self.registrations_open => {
                    match reg {
                        Some(reg) => {
                            self.register_peer(reg);
                            continue;
                        }
                        // Every registration sink dropped (the accept
                        // loops exited, or the in-process / test path
                        // never held the sink). Latch the arm off so the
                        // closed-channel future — which resolves to
                        // `None` immediately and forever — can't be
                        // re-selected and starve `incoming_rx`. The
                        // `if self.registrations_open` guard then
                        // disables this arm for the rest of the loop;
                        // `incoming_rx` drives inbound alone.
                        None => {
                            self.registrations_open = false;
                            continue;
                        }
                    }
                }
            };
            match self.handle_role_layer(raw) {
                Some(payload) => return Some(payload),
                None => continue,
            }
        }
    }

    fn try_recv_peer(&mut self) -> Option<DistributedMessage<I>> {
        // Non-blocking drain. Apply any pending registrations first
        // (so the writer table is current before a synchronous peek of
        // the inbound stream), then surface the next inbound frame.
        while let Ok(reg) = self.new_conn_rx.try_recv() {
            self.register_peer(reg);
        }
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
