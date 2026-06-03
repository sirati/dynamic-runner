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
use std::time::Instant;

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{
    Clocks, DistributedMessage, InboundOutcome, PeerConnectionInfo, PeerId, PeerTransport, Role,
    RoleAddressedAction, RoleCache, RoleChangeHookRegistrar, Router, SendOutcome,
    apply_role_misaddress_hint, decide_role_addressed_with_cache, install_role_change_hook,
    new_role_cache, read_role_cache, seed_self_role,
};
use tokio::sync::mpsc;

/// Snapshot the [`Clocks`] pair the [`Router`] consumes on every entry
/// point: monotonic `now` for its TTL/cooldown arithmetic, unix-epoch
/// `wire` for the timestamps it stamps on outgoing relay envelopes.
/// Mirrors `PeerNetwork`'s `now_clocks` so the relay state machine sees
/// the same clock shape regardless of which transport drives it.
fn now_clocks() -> Clocks {
    Clocks {
        now: Instant::now(),
        wire: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0),
    }
}

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
    /// Peer-mesh routing dispatcher. Owns ALL routing state (in-flight
    /// relays, blacklist, per-peer route observation, monotonic relay-id
    /// counter) over the SAME `outgoing` connection table — so the
    /// submitter primary can forward a secondary-A→secondary-B frame
    /// through itself and behave as a real mesh peer, instead of being
    /// reachable only as a direct hop. The exact `Router<I>` the
    /// `PeerNetwork` QUIC transport uses; this transport supplies it the
    /// shared writer table on every entry point and never inspects its
    /// state directly.
    ///
    /// Redial signal: the `Router` emits a `redial_target` when an
    /// active relay relationship is first observed. `PeerNetwork` acts
    /// on it via `spawn_redial`; the submitter primary has NO dial path
    /// (dial direction is secondary-to-primary in production, and
    /// `connect_to_peers` is a no-op here), so the signal is
    /// deliberately dropped — there is nothing for the submitter to dial
    /// out to.
    router: Router<I>,
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
        let router = Router::new(local_id.clone());
        let transport = Self {
            local_id,
            outgoing: Rc::clone(&outgoing),
            incoming_rx,
            new_conn_rx,
            registrations_open: true,
            role_cache,
            router,
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

    /// Drain every accept-loop registration already queued on
    /// `new_conn_rx` into the shared `outgoing` table, non-blocking.
    /// The single owner of "apply all pending registrations now";
    /// `recv_peer` (its eager pre-yield drain), `try_recv_peer`, and
    /// `broadcast` all call it so the writer table is current before
    /// each peeks/iterates. Mirrors `PeerNetwork::drain_new_connections`
    /// — the broadcast fan-out must drain first so a secondary whose
    /// handshake completed since the last poll (its registration still
    /// on `new_conn_rx`, not yet in `outgoing`) is not silently skipped.
    fn drain_registrations(&mut self) {
        while let Ok(reg) = self.new_conn_rx.try_recv() {
            self.register_peer(reg);
        }
    }

    /// Role-layer interceptor — mirrors
    /// `PeerNetwork::handle_role_layer`. The
    /// `decide_role_addressed_with_cache` decision is the single
    /// source of truth for the four cases (A/B/C/D); the relay-send
    /// dispatch path goes through the [`Router`] (same as
    /// `PeerNetwork`) so a role-resolved forward that has no DIRECT
    /// writer can still be relayed through another peer rather than
    /// dropped. `clocks` is threaded from the `recv_peer` /
    /// `try_recv_peer` entry point so the Router's send-side state uses
    /// the same monotonic clock as inbound dispatch.
    fn handle_role_layer(
        &mut self,
        msg: DistributedMessage<I>,
        clocks: Clocks,
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
                        // Both sends are fire-and-forget transport-
                        // internal bookkeeping. They go through the
                        // Router's send path (NOT the trait's
                        // `send_to_peer`, whose NoRoute → Err contract
                        // is wrong for an internal hint) so a forward
                        // with no direct writer relays through a peer
                        // instead of erroring — same contract as
                        // `PeerNetwork::handle_role_layer`.
                        if let Err(e) = self.router_send(&forward_to, forwarded, clocks) {
                            tracing::warn!(
                                forward_to = %forward_to,
                                error = %e,
                                "RoleAddressed relay forward failed (tunneled)",
                            );
                        }
                        // Unbox once at the dispatch boundary.
                        if let Err(e) = self.router_send(&hint_to, *hint, clocks) {
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

    /// Originate a send to `peer_id` through the [`Router`] over the
    /// shared `outgoing` table. The single place that bridges the
    /// `Rc<RefCell<_>>` writer table to the Router's `&mut HashMap`
    /// contract: borrow the table mutably for the duration of the
    /// (synchronous) Router call, map [`SendOutcome`] to the
    /// transport's `Result<(), String>` contract, then drop the borrow.
    ///
    /// The `RefCell` borrow never spans an `.await` (the Router call is
    /// synchronous), so the workspace's
    /// `await_holding_refcell_ref = "deny"` lint is satisfied without a
    /// clone-out dance. `SendOutcome::Relayed::redial_target` is dropped
    /// — see the `router` field doc (the submitter has no dial path).
    fn router_send(
        &mut self,
        peer_id: &str,
        msg: DistributedMessage<I>,
        clocks: Clocks,
    ) -> Result<(), String> {
        let mut outgoing = self.outgoing.borrow_mut();
        self.router.prune(clocks.now);
        let outcome = self
            .router
            .send_to_peer(peer_id, msg, &mut outgoing, clocks)
            .map_err(|e| e.to_string())?;
        match outcome {
            SendOutcome::Direct | SendOutcome::Relayed { .. } => Ok(()),
            // Preserve the pre-Router no-writer mapping: a target with
            // neither a direct writer nor any forwarder is a fatal "no
            // route", surfaced to the caller as `Err` (the
            // keepalive/relay arms match on `Err`) rather than a silent
            // success.
            SendOutcome::NoRoute => Err(format!(
                "no route to peer '{peer_id}': no tunneled writer and no \
                 forwarder available (either the secondary hasn't completed \
                 handshake yet, or its writer task has exited)."
            )),
        }
    }

    /// Drive one inbound frame through the [`Router`] over the shared
    /// `outgoing` table, returning the frame to deliver to the role
    /// layer (or `None` when the Router consumed it as a relay /
    /// backoff / stale-drop). Bridges the `Rc<RefCell<_>>` table to the
    /// Router's `&mut HashMap` contract, same borrow discipline as
    /// [`Self::router_send`]. `redial_target` is dropped (no dial path).
    fn router_inbound(
        &mut self,
        msg: DistributedMessage<I>,
        clocks: Clocks,
    ) -> Option<DistributedMessage<I>> {
        let mut outgoing = self.outgoing.borrow_mut();
        self.router.prune(clocks.now);
        match self.router.process_inbound(msg, &mut outgoing, clocks) {
            InboundOutcome::Deliver { msg, .. } => Some(*msg),
            InboundOutcome::Handled { .. } => None,
        }
    }
}

impl<I: Identifier> PeerTransport<I> for TunneledPeerTransport<I> {
    async fn broadcast(&mut self, msg: DistributedMessage<I>) -> Result<(), String> {
        // ONE broadcast topology, exactly-once. This is a direct
        // fan-out over the SAME Router-backed `outgoing` connection
        // table the Router relays through — NOT a per-peer
        // `router.send_to_peer`, and NOT a separate writer set. The
        // exactly-once guarantee for a global-state (CRDT) broadcast
        // is structural: each connected peer gets exactly ONE plain
        // `msg.clone()` send (not a `Relay` envelope), so no peer is
        // reachable both directly AND as a relay forwarder for the
        // same frame — there is no direct+relay double-delivery and no
        // re-broadcast (receivers apply the CRDT idempotently and never
        // re-fan-out). The idempotent epoch-LWW apply is a safety net,
        // not a license to send twice. Mirrors
        // `PeerNetwork::broadcast` step-for-step.
        //
        // Drain pending accept-loop registrations FIRST so a secondary
        // whose handshake completed since the last `recv_peer` poll —
        // its writer still queued on `new_conn_rx`, not yet in
        // `outgoing` — receives this broadcast too (else it would be
        // silently skipped: a missed delivery). Same ordering rationale
        // as `PeerNetwork::broadcast`'s leading `drain_new_connections`.
        self.drain_registrations();
        // Memory hygiene: even when this node only broadcasts (no
        // `send_to_peer` / `recv_peer` traffic), relay routing state
        // accumulated by past forwarding needs sweeping — mirrors
        // `PeerNetwork::broadcast`.
        self.router.prune(Instant::now());
        // Snapshot `(peer_id, sender)` out of the shared map behind a
        // bounded borrow, then iterate the clones without holding the
        // RefCell across `.await`. `UnboundedSender::send` is itself
        // synchronous (no await), but the explicit clone-and-drop keeps
        // any future shape change compatible with the workspace's "no
        // RefCell borrow held across await" lint. We carry the id
        // alongside the sender so a dead writer can be removed below.
        let senders: Vec<(String, mpsc::UnboundedSender<DistributedMessage<I>>)> = self
            .outgoing
            .borrow()
            .iter()
            .map(|(id, tx)| (id.clone(), tx.clone()))
            .collect();
        let mut dead: Vec<String> = Vec::new();
        for (peer_id, tx) in senders {
            // A closed writer means the secondary went away. Collect it
            // for removal so the table stays an accurate membership view
            // (`has_peer`/`peer_count`) and a later broadcast does not
            // re-attempt a dead writer. The submitter has NO dial path
            // (see the `router` field doc + `connect_to_peers` no-op),
            // so — unlike `PeerNetwork::broadcast` — there is no redial
            // to kick on detection; removal is the whole disposition.
            if tx.send(msg.clone()).is_err() {
                dead.push(peer_id);
            }
        }
        if !dead.is_empty() {
            let mut outgoing = self.outgoing.borrow_mut();
            for peer_id in &dead {
                outgoing.remove(peer_id);
            }
        }
        Ok(())
    }

    async fn send_to_peer(
        &mut self,
        peer_id: &str,
        msg: DistributedMessage<I>,
    ) -> Result<(), String> {
        // Route through the Router over the shared `outgoing` table so
        // a target with no DIRECT writer can be relayed through another
        // peer (the relay capability this transport gained). The Router
        // call is synchronous, so the `RefCell` borrow inside
        // `router_send` never spans an await.
        self.router_send(peer_id, msg, now_clocks())
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
                    self.drain_registrations();
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
            // Route the raw frame through the Router first: a `Relay`
            // envelope addressed elsewhere is forwarded (or bounced)
            // through the `outgoing` table and consumed here; a `Relay`
            // for us is unwrapped; everything else passes through. Only
            // a delivered frame reaches the role layer. Mirrors
            // `PeerNetwork::recv_peer`'s `process_inbound` → role-layer
            // ordering.
            let clocks = now_clocks();
            let Some(delivered) = self.router_inbound(raw, clocks) else {
                continue;
            };
            match self.handle_role_layer(delivered, clocks) {
                Some(payload) => return Some(payload),
                None => continue,
            }
        }
    }

    fn try_recv_peer(&mut self) -> Option<DistributedMessage<I>> {
        // Non-blocking drain. Apply any pending registrations first
        // (so the writer table is current before a synchronous peek of
        // the inbound stream), then surface the next inbound frame.
        self.drain_registrations();
        let clocks = now_clocks();
        self.router.prune(clocks.now);
        loop {
            let msg = self.incoming_rx.try_recv().ok()?;
            // Sync Router dispatch: unwraps a `Relay` addressed to us,
            // drops a `Relay` addressed elsewhere (forwarding needs the
            // async path — same constraint as
            // `PeerNetwork::try_recv_peer`), passes everything else
            // through. No `outgoing` borrow needed:
            // `process_inbound_sync` is pure state mutation.
            let delivered = match self.router.process_inbound_sync(msg, clocks) {
                InboundOutcome::Deliver { msg, .. } => *msg,
                InboundOutcome::Handled { .. } => continue,
            };
            match self.handle_role_layer(delivered, clocks) {
                Some(payload) => return Some(payload),
                None => continue,
            }
        }
    }

    fn peer_count(&self) -> usize {
        self.outgoing.borrow().len()
    }

    fn has_peer(&self, id: &PeerId) -> bool {
        // Real per-id membership: a peer is a member iff it has a writer
        // in the shared `outgoing` table (the same table `peer_count`
        // measures). A secondary is inserted once its accept-loop
        // registration is drained; a demoted/disconnected one is
        // removed. Short synchronous borrow — no await in scope, so the
        // `await_holding_refcell_ref` lint is satisfied.
        self.outgoing.borrow().contains_key(id.as_str())
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
