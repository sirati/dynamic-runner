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


mod egress;
mod ingress;
mod membership;
mod routing;

use std::sync::{Arc, Weak};

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::address::PeerId;
use dynrunner_protocol_primary_secondary::PeerTransport;
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
