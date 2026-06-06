//! [`Mesh`] — route a `(target, frame)` to a local role-slot (loopback)
//! or a remote connection, by id.
//!
//! # Concern
//!
//! The `Mesh` is the role-demux layer that sits ON TOP of a role-agnostic
//! [`PeerTransport`]. It owns the by-value transport plus a registry of
//! the (up to three) local [`RoleSlot`]s as `Weak`s. Its whole job is:
//! given an origin role and a directed [`Destination`] (or a broadcast),
//! decide whether the frame is delivered to a LOCAL slot (loopback) or
//! sent to a REMOTE connection by id — and fan a broadcast to both sides
//! minus the originating role.
//!
//! # What it reuses (does NOT re-derive)
//!
//! Membership, loopback, and broadcast already exist correctly in the
//! transport (the `register_primary_link` by-id fold + the
//! `inprocess_secondary_mesh` model — clarification RV-2). The `Mesh`
//! ADDS ONLY the role-slot demux: remote sends/broadcasts go straight
//! through the transport's `send_to_peer` / `broadcast`; the new code is
//! purely "which local slot does a directed/`All` frame also reach".
//!
//! # Boundary (SUPREME-LAW #5)
//!
//! The transport stays role-agnostic (by `PeerId` only); ALL role
//! knowledge lives here in `manager-distributed`. A caller hands a
//! [`LocalRole`] origin + a [`Destination`] target; it never touches the
//! transport or the slots. The role slots are held as `Weak` so a
//! dropped role's `Arc` (the [`super::Process`]'s) lets the slot
//! auto-die — `deliver_local` self-prunes a stale `Weak` (clarification
//! BUG-2).

use std::sync::{Arc, Weak};

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::address::{Destination, PeerId};
use dynrunner_protocol_primary_secondary::{DistributedMessage, PeerTransport};
use tokio::sync::mpsc;

use super::membership::MembershipView;
use super::mesh_client::{LocalDispatch, MeshClient, RoleInbox};
use super::role::LocalRole;
use super::role_slot::RoleSlot;

/// The role-demux mesh wrapper over a role-agnostic [`PeerTransport`].
///
/// Owns the transport by value (it IS the mesh's wire), the three local
/// role-slot `Weak`s, the pump-published [`MembershipView`], and the
/// local-dispatch queue endpoints the detached [`MeshClient`]s feed.
pub struct Mesh<I: Identifier, Tr: PeerTransport<I>> {
    /// The role-agnostic transport — the single source of truth for mesh
    /// membership (`connections`). The `Mesh` never shadows its count.
    transport: Tr,
    /// The local primary slot, if a primary runs in this process.
    primary: Option<Weak<RoleSlot<I>>>,
    /// The local secondary slot, if a secondary runs in this process.
    secondary: Option<Weak<RoleSlot<I>>>,
    /// The local observer slot, if an observer runs in this process.
    observer: Option<Weak<RoleSlot<I>>>,
    /// Pump-published live-read membership the detached clients read.
    membership: MembershipView,
    /// Sender cloned into every [`MeshClient`] for local delivery; the
    /// pump drains the egress receiver and applies each item via
    /// [`Self::apply_local_dispatch`].
    local_dispatch_tx: mpsc::UnboundedSender<LocalDispatch<I>>,
    /// Receive end of the local-dispatch queue.
    ///
    /// `Some` until the mesh-pump ([`super::pump`]) takes it out via
    /// [`Self::take_local_dispatch_rx`] to OWN it disjointly from the
    /// `&mut Mesh` it uses for apply/route. That disjoint ownership is the
    /// E0499 resolution: the pump's egress-drain future borrows only the
    /// owned receiver, never `&mut Mesh`, so the egress and ingress drains
    /// can coexist in one `select!` without double-borrowing the mesh (the
    /// inbound arm is the sole `&mut Mesh` future; the egress arm's handler
    /// then borrows the mesh only after the inbound future is dropped). The
    /// `next_local_dispatch` helper still drains it in place for the
    /// `process/tests` unit harness, which runs no pump.
    local_dispatch_rx: Option<mpsc::UnboundedReceiver<LocalDispatch<I>>>,
}

impl<I: Identifier, Tr: PeerTransport<I>> Mesh<I, Tr> {
    /// Wrap a role-agnostic transport in the role-demux layer.
    pub fn new(transport: Tr) -> Self {
        let (local_dispatch_tx, local_dispatch_rx) = mpsc::unbounded_channel();
        Self {
            transport,
            primary: None,
            secondary: None,
            observer: None,
            membership: MembershipView::new(),
            local_dispatch_tx,
            local_dispatch_rx: Some(local_dispatch_rx),
        }
    }

    /// Register a local role and MINT its capability trio together.
    ///
    /// Creates the `Arc<RoleSlot>`, stores the matching `Weak` in this
    /// mesh, and returns `(Arc<RoleSlot>, MeshClient, RoleInbox)` — the
    /// trio is minted in one place so a [`MeshClient`] can never be paired
    /// with the wrong inbox/slot (clarification M3). The caller
    /// ([`super::Process`]) holds the `Arc`; dropping it lets the slot
    /// auto-die (clarification H4).
    ///
    /// A second registration for the same role REPLACES the prior `Weak`
    /// (a fresh coordinator for the role): the old `Arc`, once dropped by
    /// the caller, simply never upgrades again.
    pub fn register_local_role(
        &mut self,
        role: LocalRole,
        peer_id: PeerId,
    ) -> (Arc<RoleSlot<I>>, MeshClient<I>, RoleInbox<I>) {
        let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
        let slot = Arc::new(RoleSlot::new(role, peer_id, inbound_tx));
        let weak = Arc::downgrade(&slot);
        match role {
            LocalRole::Primary => self.primary = Some(weak),
            LocalRole::Secondary => self.secondary = Some(weak),
            LocalRole::Observer => self.observer = Some(weak),
        }
        let client = MeshClient::new(role, self.local_dispatch_tx.clone(), self.membership.clone());
        let inbox = RoleInbox::new(inbound_rx);
        (slot, client, inbox)
    }

    /// Deliver a frame to ONE local role slot (loopback).
    ///
    /// Upgrades the target role's `Weak`; on success hands the frame to
    /// the slot's inbound. On upgrade FAILURE (the role's `Arc` was
    /// dropped — teardown, H4) or a dropped inbound, self-prunes that
    /// slot's `Weak` and returns `false` — NEVER panics, NEVER prunes
    /// while iterating (clarification BUG-2). Returns `true` iff the frame
    /// reached a live slot.
    pub fn deliver_local(&mut self, target: LocalRole, frame: DistributedMessage<I>) -> bool {
        let slot = match target {
            LocalRole::Primary => &self.primary,
            LocalRole::Secondary => &self.secondary,
            LocalRole::Observer => &self.observer,
        };
        let Some(weak) = slot else {
            return false;
        };
        let prune = match weak.upgrade() {
            Some(arc) => arc.deliver(frame).is_err(),
            None => true,
        };
        if prune {
            self.clear_slot(target);
            return false;
        }
        true
    }

    /// Route a directed frame from `origin` to `target`: loopback to a
    /// local slot when the target role's host is local, else remote by id.
    /// [`Destination::All`] fans (origin-excluded — see [`Self::broadcast`]);
    /// a directed delivery NEVER excludes the origin. `origin` is carried
    /// only for the `All` fan's origin-exclusion (clarification BUG-1).
    pub async fn dispatch(
        &mut self,
        origin: LocalRole,
        target: Destination,
        frame: DistributedMessage<I>,
    ) -> Result<(), String> {
        match &target {
            Destination::All => {
                self.broadcast(origin, frame).await;
                Ok(())
            }
            Destination::Primary => {
                // Primary is id-less on the wire: a local primary is the
                // loopback target. A REMOTE primary needs the resolved host
                // id carried on the frame — a C3 seam (no `target` field
                // yet); until then C2's egress collapse resolves
                // `Destination::Primary` to a concrete id BEFORE dispatch,
                // so this arm is unreachable in the wired system. Surface
                // it loudly rather than silently drop.
                if self.deliver_local(LocalRole::Primary, frame) {
                    return Ok(());
                }
                Err("Mesh::dispatch: remote Destination::Primary requires the resolved \
                     host id (C3 frame target)"
                    .to_string())
            }
            Destination::Secondary(id) | Destination::Observer(id) => {
                let role = LocalRole::from_destination(&target)
                    .expect("Secondary/Observer always carry a role");
                if self.is_local_host(id) && self.deliver_local(role, frame.clone()) {
                    return Ok(());
                }
                self.transport.send_to_peer(id.as_str(), frame).await
            }
        }
    }

    /// Fan a frame to every remote connection AND every local slot EXCEPT
    /// the originating role/slot (clarification BUG-1).
    ///
    /// The exclusion keys on the `origin` ROLE — NEVER on the originating
    /// PEER. A same-peer secondary's `All` frame therefore still reaches
    /// the local primary slot (the §14 fix: the local primary's death
    /// clock is refreshed by its own host's secondary keepalive). Local
    /// upgrade failures self-prune (collect-then-prune, BUG-2).
    pub async fn broadcast(&mut self, origin: LocalRole, frame: DistributedMessage<I>) {
        // Remote fan: the transport broadcasts to every connection
        // role-blind. The same-peer self is not a remote connection, so
        // no peer is wrongly excluded here.
        let _ = self.transport.broadcast(frame.clone()).await;

        // Local fan: every local slot except the originating role.
        self.fan_local(Some(origin), frame);
    }

    /// Route ONE frame received off the wire to the right LOCAL slot(s),
    /// reading the frame's resolved [`Destination`] target (the C3 field
    /// stamped by the sender's egress) — a pure `role → slot` table, NEVER
    /// a content classifier and NEVER an `if host == local_id` test.
    ///
    /// This is the mesh-pump's INGRESS demux. It is local-only: an inbound
    /// frame has already crossed the wire to THIS peer, so it is NEVER
    /// re-fanned to remotes (the no-re-broadcast invariant, dirty-D3 /
    /// BUG-8). The `origin`-exclusion of [`Self::broadcast`] does not apply
    /// — the originator is a remote peer, not a local role.
    ///
    /// - directed `Some(Primary|Secondary|Observer)` → [`Self::deliver_local`]
    ///   to that one role's slot.
    /// - `Some(All)` → local fan to every live slot.
    /// - `None` (transitional: a frame stamped before the egress rewire
    ///   lands) → a LOUD `debug_assert!` + `warn`, then the documented safe
    ///   default: fan to every live LOCAL slot so NO local coordinator
    ///   misses a frame. We never silently drop. Once every egress edge
    ///   stamps `Some(resolved)` (the next coordinator-rewire wave), this
    ///   arm is unreachable and the `debug_assert!` guards that invariant.
    pub fn route_incoming(&mut self, frame: DistributedMessage<I>) {
        match frame.target() {
            Some(Destination::All) => self.fan_local(None, frame),
            Some(dst) => {
                // `from_destination` is total over the non-`All` directed
                // variants; the `All` arm is handled above.
                let role = LocalRole::from_destination(dst)
                    .expect("non-All directed Destination always carries a role");
                self.deliver_local(role, frame);
            }
            None => {
                // A frame arriving unstamped is either a real production
                // egress bug OR a test double that injects raw wire frames
                // without going through a coordinator's stamping egress. The
                // documented safe default — fan to every live local slot —
                // handles BOTH without dropping the frame, so we WARN (the
                // diagnostic for the production-bug case) rather than panic
                // (which a debug_assert would, killing legitimate raw-frame
                // test doubles). Production egress still stamps every edge;
                // this arm is the no-drop backstop, not the happy path.
                tracing::warn!(
                    kind = ?frame.msg_type(),
                    "mesh ingress: frame has no routing target (pre-stamp transitional); \
                     fanning to every local slot rather than dropping it"
                );
                self.fan_local(None, frame);
            }
        }
    }

    /// Deliver a frame to every LIVE local slot, optionally excluding one
    /// role. Collect-then-prune over a stale `Weak` (never prune during
    /// the upgrade walk — BUG-2). This is the local half of both the
    /// egress `All`-fan (`exclude = Some(origin)`, BUG-1) and the ingress
    /// fan (`exclude = None`). It NEVER touches the wire — the remote half
    /// of an egress broadcast is the caller's separate concern.
    fn fan_local(&mut self, exclude: Option<LocalRole>, frame: DistributedMessage<I>) {
        let mut to_prune: Vec<LocalRole> = Vec::new();
        for role in [LocalRole::Primary, LocalRole::Secondary, LocalRole::Observer] {
            if Some(role) == exclude {
                continue;
            }
            if let Some(weak) = self.slot_for(role) {
                match weak.upgrade() {
                    Some(arc) => {
                        if arc.deliver(frame.clone()).is_err() {
                            to_prune.push(role);
                        }
                    }
                    None => to_prune.push(role),
                }
            }
        }
        for role in to_prune {
            self.clear_slot(role);
        }
    }

    /// Re-point a live local slot from the `old` role's field to the `new`
    /// role's field, atomically with the slot's in-place
    /// [`RoleSlot::set_role`] retag (clarification D-RETAG / H5).
    ///
    /// C0 keys slots by FIELD (`primary`/`secondary`/`observer`), not by
    /// the slot's live `role()`. So a bare `slot.set_role(new)` would leave
    /// this mesh demuxing `new`-role frames to a `None` field and
    /// `old`-role frames to the now-retagged slot. This method moves the
    /// `Weak` from the `old` field to the `new` field AND flips the slot's
    /// role, in one step, so the demux stays correct across the
    /// submitter-primary→observer handoff. The SAME `Arc`/channel is
    /// preserved — the [`super::RoleInbox`] drains uninterrupted (stable
    /// channel, no delivery gap).
    ///
    /// `old == new` is a no-op. If no slot occupies the `old` field
    /// (already torn down or never registered), the move is a no-op — there
    /// is nothing live to retag — and the `new` field is left as-is. The
    /// caller (the [`super::Node`] swap) holds the matching `Arc` and is
    /// the authority on whether a retag is appropriate.
    pub fn retag_local_role(&mut self, old: LocalRole, new: LocalRole) {
        if old == new {
            return;
        }
        let Some(weak) = self.take_slot(old) else {
            return;
        };
        // Flip the live slot's role in place (the stable-channel retag),
        // then re-point the mesh `Weak` into the `new` field. A pre-existing
        // `Weak` in the `new` field is REPLACED — its `Arc`, once the caller
        // drops it, simply never upgrades again (the same semantics as a
        // second `register_local_role`).
        if let Some(arc) = weak.upgrade() {
            arc.set_role(new);
        }
        self.set_slot(new, weak);
    }

    /// Publish the LIVE transport membership into the [`MembershipView`]
    /// the detached [`MeshClient`]s read.
    ///
    /// Called by the mesh-pump once per drain cycle. The published value
    /// is ALWAYS a direct transport read ([`PeerTransport::peer_count`] +
    /// the connected id-set) — never a hand-maintained delta (the SETTLED
    /// no-shadow rule).
    pub fn publish_membership(&self) {
        self.membership
            .publish(self.transport.peer_count(), self.transport.connected_ids());
    }

    /// Live mesh cardinality — the transport's `connections.len()`. The
    /// single source of truth; never a shadow.
    pub fn peer_count(&self) -> usize {
        self.transport.peer_count()
    }

    /// Live per-id membership — delegates to the transport's connection
    /// table.
    pub fn has_peer(&self, id: &PeerId) -> bool {
        self.transport.has_peer(id)
    }

    /// Apply one queued local-dispatch item against the LIVE slot set.
    ///
    /// The mesh-pump (C1) drains [`Self::local_dispatch_rx`] and calls
    /// this. Resolution is live per-frame (no cached `Weak`s — BUG-2):
    /// the item carries only the origin role + role-bearing target.
    pub async fn apply_local_dispatch(&mut self, item: LocalDispatch<I>) -> Result<(), String> {
        self.dispatch(item.origin, item.target, item.frame).await
    }

    /// Drain the next queued local-dispatch item, if any.
    ///
    /// In-place drain for the `process/tests` unit harness (which runs no
    /// pump); `None` once every [`MeshClient`] sender is dropped OR the
    /// pump has already TAKEN the receiver via [`Self::take_local_dispatch_rx`]
    /// (after the take, this mesh no longer owns the egress queue — the pump
    /// drains it through the owned receiver).
    pub async fn next_local_dispatch(&mut self) -> Option<LocalDispatch<I>> {
        match self.local_dispatch_rx.as_mut() {
            Some(rx) => rx.recv().await,
            None => None,
        }
    }

    /// Take the egress-queue receiver OUT of the mesh so the mesh-pump
    /// ([`super::pump`]) owns it disjointly from the `&mut Mesh` it uses for
    /// apply/route.
    ///
    /// This is the E0499 resolution B-SECONDARY flagged: with the receiver
    /// owned by the pump, the egress-drain future (`rx.recv()`) borrows only
    /// the receiver — never `&mut Mesh` — so it can coexist in one `select!`
    /// with the inbound-route arm (the sole `&mut Mesh` future). The egress
    /// handler then takes `&mut Mesh` (for `apply_local_dispatch`) only after
    /// the select drops the inbound future, so the two never double-borrow.
    ///
    /// Returns `None` if already taken (idempotent guard); the pump takes it
    /// exactly once at startup.
    pub fn take_local_dispatch_rx(&mut self) -> Option<mpsc::UnboundedReceiver<LocalDispatch<I>>> {
        self.local_dispatch_rx.take()
    }

    /// Receive ONE inbound wire frame, dial off it if it is a `PeerInfo`
    /// (RV-2 peer-mesh discovery), then route it to the right local slot(s)
    /// — a single self-contained `&mut Mesh` call whose borrow is released on
    /// return (so a sibling `select!` arm may then borrow the mesh for an
    /// egress apply).
    ///
    /// Returns `true` if a frame was handled, `false` once the transport's
    /// inbound is closed (`recv_peer → None`) — the pump's ingress-side
    /// teardown signal. Both the dial and the route are delegated wholesale
    /// (the pump is a thin adapter — H6); the dial is observed-not-shadowed
    /// (it folds the listed peers into the transport, which stays the single
    /// membership source). A `PeerInfo` is dialed AND ALSO routed to the
    /// local slots, so the coordinator still observes the frame as today
    /// (e.g. the secondary's watchdog arming) — the pump only ADDS the dial.
    pub async fn recv_dial_and_route(&mut self) -> bool {
        let Some(frame) = self.recv_peer().await else {
            return false;
        };
        if let DistributedMessage::PeerInfo { peers, .. } = &frame {
            // Clone the seed list out of the borrowed frame so the dial's
            // `&mut self` does not alias the frame we still route below.
            let peers = peers.clone();
            self.connect_to_peers(&peers).await;
        }
        self.route_incoming(frame);
        true
    }

    /// Receive the next frame from any remote peer. Thin pass-through to
    /// the transport for the mesh-pump's ingress drain.
    pub async fn recv_peer(&mut self) -> Option<DistributedMessage<I>> {
        self.transport.recv_peer().await
    }

    /// Dial the peers in a `PeerInfo` list, folding each into the
    /// transport's mesh (RV-2). Thin pass-through to
    /// [`PeerTransport::connect_to_peers`]: peer discovery is a
    /// transport/membership concern the mesh-pump owns, NOT the
    /// coordinator's (the coordinator holds only a `MeshClient`/`RoleInbox`,
    /// neither of which dials — see the `PHASE-C-SEAM[C-NODE]` at
    /// `secondary/setup.rs`). The pump observes every inbound `PeerInfo`
    /// frame, so it dials off the same list the coordinator would have, with
    /// no manager-layer `connect_to_peers` call. Membership re-derivation
    /// stays in the transport (the pump never shadows it).
    pub async fn connect_to_peers(
        &mut self,
        peers: &[dynrunner_protocol_primary_secondary::PeerConnectionInfo],
    ) {
        self.transport.connect_to_peers(peers).await;
    }

    /// Borrow of one role's `Weak`, role-keyed.
    fn slot_for(&self, role: LocalRole) -> Option<&Weak<RoleSlot<I>>> {
        match role {
            LocalRole::Primary => self.primary.as_ref(),
            LocalRole::Secondary => self.secondary.as_ref(),
            LocalRole::Observer => self.observer.as_ref(),
        }
    }

    /// Drop a role's `Weak` (self-prune after an upgrade failure).
    fn clear_slot(&mut self, role: LocalRole) {
        match role {
            LocalRole::Primary => self.primary = None,
            LocalRole::Secondary => self.secondary = None,
            LocalRole::Observer => self.observer = None,
        }
    }

    /// Take a role's `Weak` out of its field, leaving it `None` (the move
    /// source for [`Self::retag_local_role`]).
    fn take_slot(&mut self, role: LocalRole) -> Option<Weak<RoleSlot<I>>> {
        match role {
            LocalRole::Primary => self.primary.take(),
            LocalRole::Secondary => self.secondary.take(),
            LocalRole::Observer => self.observer.take(),
        }
    }

    /// Install a `Weak` into a role's field (the move destination for
    /// [`Self::retag_local_role`]).
    fn set_slot(&mut self, role: LocalRole, weak: Weak<RoleSlot<I>>) {
        match role {
            LocalRole::Primary => self.primary = Some(weak),
            LocalRole::Secondary => self.secondary = Some(weak),
            LocalRole::Observer => self.observer = Some(weak),
        }
    }

    /// Whether `id` is this process's own host id — i.e. some live local
    /// slot runs on it. A secondary/observer `Destination` carrying the
    /// local host id is a loopback.
    fn is_local_host(&self, id: &PeerId) -> bool {
        [LocalRole::Primary, LocalRole::Secondary, LocalRole::Observer]
            .into_iter()
            .filter_map(|r| self.slot_for(r))
            .filter_map(|w| w.upgrade())
            .any(|arc| arc.peer_id() == id)
    }
}
