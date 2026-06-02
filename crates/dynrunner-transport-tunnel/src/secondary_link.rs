//! [`UnifiedSecondaryTransport`] — the secondary-side single
//! [`PeerTransport`] that makes the physical location of the primary
//! OPAQUE to the manager.
//!
//! # Concern
//!
//! A secondary used to hold two physical handles: an *uplink*
//! (`MessageSender + MessageReceiver` to whichever node it dialled at
//! bootstrap — a `NetworkClient` over WSS/QUIC, a co-located channel
//! end, or `NoPrimaryTransport`) and a *mesh* handle
//! (`PeerTransport`, e.g. `EitherPeerTransport`). The manager then
//! branched on transport locality at every primary-bound send
//! (`send_to_current_primary`: loopback-vs-wire) and read the dual
//! handles by name across ~12 sites.
//!
//! This type composes those two physical handles plus a
//! [`RoleCache`] — THE single source of "who holds the primary role
//! now" — behind one opaque [`PeerTransport`] surface. The manager
//! addresses purely by [`Address`]; this transport resolves each
//! address to the uplink, the mesh, or a local loopback. The
//! promotion re-route is just a [`RoleCache`] update fired by the
//! `ClusterState`'s `PrimaryChanged` apply hook — the manager's
//! operational loop is never torn down.
//!
//! # Routing (the four cases the manager never sees)
//!
//! - `send(Address::Role(Role::Primary))`:
//!     * cache holder == `local_id` → **loopback** (this node holds
//!       the primary pool; deliver to its own inbound stream).
//!     * cache holder is some other peer id → **mesh.send_to_peer**
//!       (a promoted peer holds the role now).
//!     * cache cold (no holder yet — the *original-primary sentinel*:
//!       no `PromotePrimary` has been observed) → **uplink.send**
//!       (route to the upstream primary over the dedicated link).
//!       Once the uplink has closed this becomes a uniform no-route
//!       `Err` instead.
//! - `send(Address::Role(Role::Self_))` → **loopback**.
//! - `send(Address::Peer(id))`: id == `local_id` → **loopback**;
//!   else **mesh.send_to_peer**. (Subsumes the deleted
//!   `handle_primary_task_request` self-assign-vs-wire branch.)
//! - `send(Address::Broadcast(_))` → **mesh.broadcast**. The
//!   authority's CRDT broadcast reaches the demoted node because
//!   that node is itself a mesh member.
//!
//! # Inbound fan-in
//!
//! [`PeerTransport::recv_peer`] merges THREE inbound sources into one
//! stream: the uplink's `recv()`, the mesh's `recv_peer()`, and the
//! internal loopback queue (fed by self-addressed / `Role::Self_` /
//! holder==self sends). The uplink closing is an INTERNAL transport
//! event: it latches `uplink_open = false` so the cache-cold
//! `Role::Primary` branch stops selecting a dead handle and surfaces a
//! uniform no-route `Err`; it is NOT surfaced to the manager as an
//! `is_primary` / `peer_count` cascade.
//!
//! # Reuse
//!
//! The role-layer interceptor (`RoleAddressed` unwrap / relay-hint,
//! `RoleMisaddressHint` cache-warming) reuses
//! [`decide_role_addressed_with_cache`] + [`apply_role_misaddress_hint`]
//! verbatim — identical to [`crate::TunneledPeerTransport`]. The
//! match-and-delegate shape over the inner handles mirrors
//! `EitherPeerTransport`. `Role::Self_ → loopback` is the documented
//! transport responsibility (see `address.rs`).
//!
//! # Single-threaded by construction
//!
//! Like the rest of this crate, the secondary coordinator runs on a
//! `current_thread` `LocalSet`; the loopback queue is a plain
//! `tokio::sync::mpsc` and the role cache is the workspace-shared
//! `Arc<RwLock<_>>`. The `await_holding_*` lints catch any borrow held
//! across an await.

use dynrunner_core::{Identifier, MessageReceiver, MessageSender};
use dynrunner_protocol_primary_secondary::{
    apply_role_misaddress_hint, decide_role_addressed_with_cache, install_role_change_hook,
    new_role_cache, read_role_cache, seed_self_role, Address, DistributedMessage,
    PeerConnectionInfo, PeerTransport, Role, RoleAddressedAction, RoleCache,
    RoleChangeHookRegistrar, Scope,
};
use tokio::sync::mpsc;

/// Opaque secondary-side transport: composes the uplink handle, the
/// mesh handle, and a [`RoleCache`] behind one [`PeerTransport`].
///
/// Generic over:
/// - `U`: the uplink handle — `MessageSender + MessageReceiver` over
///   `DistributedMessage<I>` (a `NetworkClient`, a co-located channel
///   end, or `NoPrimaryTransport`). Construction picks this by
///   topology; this transport treats it opaquely.
/// - `P`: the mesh handle — a [`PeerTransport`] (e.g.
///   `EitherPeerTransport`).
/// - `I`: the identifier type.
pub struct UnifiedSecondaryTransport<U, P, I>
where
    U: MessageSender<DistributedMessage<I>> + MessageReceiver<DistributedMessage<I>>,
    P: PeerTransport<I>,
    I: Identifier,
{
    /// This node's own peer-id. Stamped onto `RoleAddressed`
    /// envelopes (via [`PeerTransport::local_id`]) and compared
    /// against the role-cache holder to detect "I hold this role"
    /// (→ loopback).
    local_id: String,

    /// Uplink to the node this secondary dialled at bootstrap — the
    /// *original primary*. Carries the pre-mesh setup-phase frames
    /// (exposed via [`Self::uplink_mut`]) and is the routing target
    /// for `Role::Primary` while the role cache is still cold (no
    /// `PromotePrimary` observed). Opaque `MessageSender +
    /// MessageReceiver`.
    uplink: U,

    /// `false` once the uplink's `recv()` has returned `None` (the
    /// bridge reader/writer task exited). After that the cache-cold
    /// `Role::Primary` branch stops selecting the dead uplink and
    /// surfaces a uniform no-route `Err`; the uplink's `recv` arm is
    /// disabled in the fan-in so the persistently-`None` future
    /// doesn't hot-loop. Promotion of a peer re-points `Role::Primary`
    /// at the mesh regardless of this flag.
    uplink_open: bool,

    /// Mesh handle — the peer overlay over which promoted-peer and
    /// inter-secondary traffic flows. `peer_count()` (mesh-health,
    /// NOT routing) and broadcasts delegate here.
    mesh: P,

    /// Loopback inbound queue. Self-addressed sends (`Role::Self_`,
    /// `Address::Peer(local_id)`, and `Role::Primary` when this node
    /// holds the role) are delivered here and surface through
    /// [`Self::recv_peer`] exactly as if they had arrived on the wire
    /// — the manager's dispatch path is identical regardless of
    /// origin. `local_id`-targeted loopback is the transport's
    /// documented responsibility for `Role::Self_`.
    loopback_tx: mpsc::UnboundedSender<DistributedMessage<I>>,
    loopback_rx: mpsc::UnboundedReceiver<DistributedMessage<I>>,

    /// Write-through cache of `Role → peer_id`. Seeded with
    /// `Role::Self_ = local_id`; warmed for `Role::Primary` by the
    /// `ClusterState` role-change hook installed via
    /// [`PeerTransport::register_with_cluster_state`]. THE single
    /// source of "who is primary now"; the promotion re-route is a
    /// hook-driven update of this cache.
    role_cache: RoleCache,
}

impl<U, P, I> UnifiedSecondaryTransport<U, P, I>
where
    U: MessageSender<DistributedMessage<I>> + MessageReceiver<DistributedMessage<I>>,
    P: PeerTransport<I>,
    I: Identifier,
{
    /// Compose the unified transport from the uplink + mesh handles.
    ///
    /// `local_id` is this secondary's own id (matches
    /// `SecondaryConfig::secondary_id`). `Role::Self_` is seeded into
    /// the cache immediately so a self-addressed `RoleAddressed`
    /// envelope unwraps (Case A) rather than dropping; the
    /// `Role::Primary` entry is warmed later by the role-change hook.
    pub fn new(local_id: String, uplink: U, mesh: P) -> Self {
        let role_cache = new_role_cache();
        seed_self_role(&role_cache, &local_id);
        let (loopback_tx, loopback_rx) = mpsc::unbounded_channel();
        Self {
            local_id,
            uplink,
            uplink_open: true,
            mesh,
            loopback_tx,
            loopback_rx,
            role_cache,
        }
    }

    /// Narrow mutable accessor to the uplink handle for the pre-mesh
    /// setup-phase bootstrap (`SecondaryWelcome` / `CertExchange` /
    /// the setup-window recv loop).
    ///
    /// These frames flow on the uplink BEFORE the peer mesh exists —
    /// the cert exchange is what *establishes* the mesh — so they are
    /// a genuine uplink-only concern that predates any role cache.
    /// The setup module wraps this borrow in
    /// `SecondarySetupBootstrap` for the duration of one send/recv and
    /// drops it. Runtime routing never touches this accessor; it goes
    /// through [`PeerTransport::send`].
    pub fn uplink_mut(&mut self) -> &mut U {
        &mut self.uplink
    }

    /// Deliver a message to this node's own inbound stream. Used for
    /// `Role::Self_`, self-addressed `Address::Peer(local_id)`, and
    /// `Role::Primary` when this node holds the role. A closed
    /// loopback receiver is impossible inside an active transport (the
    /// receiver lives on `self`), so a send error here is a logic bug
    /// surfaced to the caller.
    fn loopback(&self, msg: DistributedMessage<I>) -> Result<(), String> {
        self.loopback_tx
            .send(msg)
            .map_err(|_| "loopback inbound queue closed".to_string())
    }

    /// Resolve + dispatch an `Address::Role(Role::Primary)` send. This
    /// is the single chokepoint that replaces the deleted
    /// `send_to_current_primary` locality branch.
    async fn send_to_primary(&mut self, msg: DistributedMessage<I>) -> Result<(), String> {
        match read_role_cache(&self.role_cache, &Role::Primary) {
            // A peer (possibly ourselves) has been promoted.
            Some(holder) if holder == self.local_id => self.loopback(msg),
            Some(holder) => self.mesh.send_to_peer(&holder, msg).await,
            // Cache cold — the original-primary sentinel: no
            // `PromotePrimary` has been observed yet, so the primary
            // is the node we dialled at bootstrap. Route over the
            // uplink while it is healthy; once it has closed, surface
            // a uniform no-route error.
            None if self.uplink_open => self.uplink.send(msg).await,
            None => Err(
                "Address::Role(Primary) unresolvable: role cache cold (no PromotePrimary \
                 observed) and the bootstrap uplink has closed; no route to the primary"
                    .to_string(),
            ),
        }
    }

    /// Role-layer interceptor for inbound frames — identical decision
    /// surface to [`crate::TunneledPeerTransport::handle_role_layer`].
    /// `RoleAddressed` envelopes unwrap (Case A), relay-and-hint
    /// (Case B), or drop (Cases C/D); `RoleMisaddressHint` warms the
    /// cache and is never surfaced. Returns `Some(payload)` for the
    /// application layer, `None` for transport-internal frames.
    async fn handle_role_layer(
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
                        // Re-dispatch via the regular peer send so the
                        // forward follows the same routing as any other
                        // peer-addressed frame. Fire-and-forget; the
                        // role layer is transport-internal bookkeeping.
                        if let Err(e) =
                            PeerTransport::<I>::send_to_peer(self, &forward_to, forwarded).await
                        {
                            tracing::warn!(
                                forward_to = %forward_to,
                                error = %e,
                                "RoleAddressed relay forward failed (unified secondary)",
                            );
                        }
                        if let Err(e) =
                            PeerTransport::<I>::send_to_peer(self, &hint_to, *hint).await
                        {
                            tracing::warn!(
                                hint_to = %hint_to,
                                error = %e,
                                "RoleMisaddressHint send failed (unified secondary)",
                            );
                        }
                        None
                    }
                    RoleAddressedAction::Drop { reason } => {
                        tracing::warn!(reason, "RoleAddressed dropped (unified secondary)");
                        None
                    }
                }
            }
            DistributedMessage::RoleMisaddressHint {
                role, holder_id, ..
            } => {
                apply_role_misaddress_hint(&self.role_cache, role, holder_id);
                None
            }
            other => Some(other),
        }
    }
}

impl<U, P, I> PeerTransport<I> for UnifiedSecondaryTransport<U, P, I>
where
    U: MessageSender<DistributedMessage<I>> + MessageReceiver<DistributedMessage<I>>,
    P: PeerTransport<I>,
    I: Identifier,
{
    /// Override the default `send` dispatch: the role-resolution for
    /// `Role::Primary` is the three-way uplink/loopback/mesh decision
    /// owned here, not the trait-default "wrap in RoleAddressed + ship
    /// to the cached holder via send_to_peer". `Role::Self_` loops
    /// back. Everything else delegates to the per-primitive methods.
    async fn send(
        &mut self,
        addr: Address,
        msg: DistributedMessage<I>,
    ) -> Result<(), String> {
        match addr {
            Address::Role(Role::Primary) => self.send_to_primary(msg).await,
            Address::Role(Role::Self_) => self.loopback(msg),
            Address::Peer(id) => self.send_to_peer(&id, msg).await,
            Address::Broadcast(Scope::Mesh) | Address::Broadcast(Scope::AllSecondaries) => {
                self.broadcast(msg).await
            }
        }
    }

    async fn broadcast(&mut self, msg: DistributedMessage<I>) -> Result<(), String> {
        // Mesh fan-out only. The authority's CRDT broadcast reaches
        // the demoted/co-located node because that node is itself a
        // mesh member — no separate uplink leg.
        self.mesh.broadcast(msg).await
    }

    async fn send_to_peer(
        &mut self,
        peer_id: &str,
        msg: DistributedMessage<I>,
    ) -> Result<(), String> {
        if peer_id == self.local_id {
            // Self-addressed unicast loops back — subsumes the deleted
            // self-assign-vs-wire branch in the old
            // `handle_primary_task_request`.
            self.loopback(msg)
        } else {
            self.mesh.send_to_peer(peer_id, msg).await
        }
    }

    async fn recv_peer(&mut self) -> Option<DistributedMessage<I>> {
        loop {
            // Merge the three inbound sources. `mpsc::Receiver::recv`
            // and the inner transports' `recv`/`recv_peer` are all
            // cancel-safe (bridge-task mpsc underneath), so the loser
            // arms of this select! re-build cleanly on the next
            // iteration. The uplink arm is gated off once the uplink
            // has closed so its persistently-`None` future can't
            // hot-loop the select.
            //
            // `biased`: poll the uplink arm first. This guarantees a
            // uplink-close (recv → `None`, which a closed channel
            // makes perpetually ready) is OBSERVED and latched on the
            // very poll it happens, even when a mesh/loopback frame is
            // simultaneously ready — otherwise the close detection
            // could be starved by a steady inbound stream, leaving
            // cache-cold `Role::Primary` sends routing into a dead
            // uplink. There is no starvation risk the other direction:
            // once `None` is observed the arm latches `uplink_open =
            // false` and is gated off forever, so from that point the
            // mesh + loopback arms get fair (top-of-`biased`) ordering.
            // While the uplink is healthy, its `recv` only resolves
            // when an actual frame arrives, so biasing it first does
            // not starve the other arms either.
            let raw = tokio::select! {
                biased;
                msg = self.uplink.recv(), if self.uplink_open => {
                    match msg {
                        Some(m) => m,
                        None => {
                            // Uplink closed: internal transport event.
                            // Latch the flag so the cache-cold
                            // `Role::Primary` branch re-routes (or
                            // errors uniformly) and this arm parks.
                            // NOT surfaced to the manager.
                            self.uplink_open = false;
                            tracing::debug!(
                                "unified secondary transport: uplink closed; \
                                 Role::Primary cache-cold sends now error until a \
                                 PromotePrimary re-points the role"
                            );
                            continue;
                        }
                    }
                }
                msg = self.mesh.recv_peer() => {
                    match msg {
                        Some(m) => m,
                        // Mesh inbound closed. Nothing left on the mesh
                        // side; fall through so the next iteration is
                        // driven by the remaining arms (uplink /
                        // loopback). If everything is closed the
                        // select! parks on the gated arms forever,
                        // which is the same "no more inbound" shape the
                        // legacy two-arm loop produced.
                        None => continue,
                    }
                }
                msg = self.loopback_rx.recv() => {
                    match msg {
                        Some(m) => m,
                        // The loopback sender lives on `self`, so this
                        // only fires after `self` is being torn down.
                        None => continue,
                    }
                }
            };
            match self.handle_role_layer(raw).await {
                Some(payload) => return Some(payload),
                None => continue,
            }
        }
    }

    fn try_recv_peer(&mut self) -> Option<DistributedMessage<I>> {
        // Non-blocking drain across loopback then mesh. The uplink
        // handle exposes no non-blocking primitive (`MessageReceiver`
        // is `recv`-only), so `try_recv_peer` covers the mesh + the
        // internal loopback — the two sources that have a synchronous
        // peek. Role-layer interception still applies; we loop until a
        // non-internal frame surfaces or both sources are empty.
        loop {
            let raw = match self.loopback_rx.try_recv() {
                Ok(m) => m,
                Err(_) => match self.mesh.try_recv_peer() {
                    Some(m) => m,
                    None => return None,
                },
            };
            // `try_recv_peer` is sync; the role layer's relay arm
            // awaits sends, so we cannot run the full interceptor
            // here. Surface non-`RoleAddressed`/`RoleMisaddressHint`
            // frames directly; the rare role-layer frame is left for
            // the async `recv_peer` path (the production inbound
            // consumer). This matches the underlying transports'
            // `try_recv_peer`, which also don't run the async relay.
            match raw {
                DistributedMessage::RoleMisaddressHint {
                    role, holder_id, ..
                } => {
                    apply_role_misaddress_hint(&self.role_cache, role, holder_id);
                    continue;
                }
                other => return Some(other),
            }
        }
    }

    fn peer_count(&self) -> usize {
        // Mesh-health cardinality (NOT a routing input). The uplink is
        // not a "peer"; failover/watchdog readers want the mesh count.
        self.mesh.peer_count()
    }

    async fn connect_to_peers(&mut self, peers: &[PeerConnectionInfo]) {
        // Mesh dialing only. The uplink is already connected (it was
        // the bootstrap dial); the peer list drives the mesh overlay.
        self.mesh.connect_to_peers(peers).await;
    }

    fn register_with_cluster_state(&self, registrar: &mut dyn RoleChangeHookRegistrar) {
        // Install the write-through hook on OUR role cache (the single
        // source of "who is primary"). Every `apply(PrimaryChanged)`
        // now re-points `Role::Primary` — the promotion re-route,
        // entirely inside this transport. The mesh handle keeps its
        // own cache for its own role-layer relays; registering both is
        // harmless (the registrar accumulates hooks) and keeps the
        // mesh's relay decisions coherent with ours.
        install_role_change_hook(RoleCache::clone(&self.role_cache), registrar);
        self.mesh.register_with_cluster_state(registrar);
    }

    fn peer_for_role(&self, role: &Role) -> Option<String> {
        read_role_cache(&self.role_cache, role)
    }

    fn local_id(&self) -> &str {
        &self.local_id
    }
}
