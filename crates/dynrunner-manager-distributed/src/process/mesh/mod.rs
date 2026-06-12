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
pub mod role_holder;
mod routing;

use std::collections::VecDeque;
use std::sync::{Arc, Weak};

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::DistributedMessage;
use dynrunner_protocol_primary_secondary::PeerTransport;
use dynrunner_protocol_primary_secondary::address::PeerId;
use tokio::sync::mpsc;

use super::membership::MembershipView;
use super::mesh_client::{LocalDispatch, MeshClient, RoleInbox};
use super::role::LocalRole;
use super::role_slot::RoleSlot;
use role_holder::RoleHolderView;

/// Bound on the slotless-ingress hold buffer (see [`Mesh::slotless_hold`]).
///
/// During a promotion / role swap this process can transiently have ZERO
/// live local slots (the coordinator slot is torn down and recreated through
/// the pump's serialized control arm — a sub-millisecond window in the wired
/// system). An ingress frame fanned in that window would reach nobody and
/// vanish silently; instead it is HELD here and replayed the instant the next
/// slot registers. The window is short and the at-risk frames are sparse
/// control frames (e.g. `RequestClusterSnapshot`), so a small ring suffices;
/// overflow drops the OLDEST with a WARN naming kind/target, never silently.
pub(crate) const SLOTLESS_HOLD_CAPACITY: usize = 64;

/// Bound on the ingress relay-ring (the role-relay loop guard — see
/// `routing.rs`).
///
/// The ring remembers the fingerprints of frames THIS process has
/// already ingress-relayed toward a recognized role holder. A frame
/// seen AGAIN at this ingress (a stale holder view bounced it back, or
/// a longer relay cycle revisited us) is never relayed twice — it comes
/// to rest in the documented fan/hold default instead, so divergent
/// holder views can bound-cycle a frame at most once per process. The
/// relay path is cold (mis-addressed directed frames only), so a small
/// ring suffices; eviction is oldest-first and merely re-permits a
/// (re-)relay, never drops a frame.
pub(crate) const ROLE_RELAY_RING_CAPACITY: usize = 64;

/// Minimum spacing between two ingress diagnostic WARNs (slotless-hold,
/// overflow, and the role-miss / unstamped fan fallbacks). A frame storm into
/// a slotless or role-mismatched process (a long swap) would otherwise emit
/// one WARN per frame at wire rate; this gate surfaces the condition (silence
/// is the failure mode this fix exists to kill) without spamming, carrying
/// the suppressed count on each permitted emit.
const INGRESS_WARN_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

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
    /// Ingress frames received while NO live local slot existed (a transient
    /// promotion / role-swap window), held in arrival order and replayed when
    /// the next slot registers ([`Self::drain_slotless_hold`]). A bounded
    /// ring; overflow drops the OLDEST with a WARN. Only the ingress fan
    /// paths ([`Self::route_incoming`]) push here — an EGRESS broadcast never
    /// holds (a same-host fan with no other local slot legitimately reaches
    /// only the wire). See [`SLOTLESS_HOLD_CAPACITY`].
    slotless_hold: VecDeque<DistributedMessage<I>>,
    /// Min-interval gate for the per-frame slotless-hold WARN so a long swap
    /// (or a storm of frames into a slotless process) cannot spam one WARN
    /// per frame at wire rate.
    slotless_hold_warn: crate::warn_throttle::WarnThrottle,
    /// Min-interval gate for the ingress fan-fallback WARNs (a directed frame
    /// naming an absent local role, or an unstamped frame) so a long
    /// role-swap with a sustained inbound stream cannot spam one WARN per
    /// frame at wire rate.
    ingress_fallback_warn: crate::warn_throttle::WarnThrottle,
    /// Coordinator-published view of which PEER hosts the id-less
    /// Primary role (the ROUTING-holder fact). Cloned into every
    /// [`MeshClient`] at mint so each coordinator's `ClusterState`
    /// role-change hook can publish into it
    /// ([`role_holder::attach_primary_recognition`]); read by the
    /// ingress demux ([`Self::route_incoming`]) to RELAY a directed
    /// `Primary` frame toward the holder when no live local Primary
    /// slot exists, instead of black-holing it in the local fan/hold.
    pub(super) role_holder: RoleHolderView,
    /// This process's own host peer-id, captured at the first
    /// [`Self::register_local_role`] (every local slot shares it).
    /// The ingress relay reads it so a holder that resolves to THIS
    /// host (a local-pending role mid-swap) is never "relayed" onto
    /// the wire — the slot-less hold is the correct resting place.
    pub(super) local_peer_id: Option<PeerId>,
    /// Fingerprints of frames this process already ingress-relayed (the
    /// role-relay loop guard — see [`ROLE_RELAY_RING_CAPACITY`]).
    pub(super) relayed_ring: VecDeque<u64>,
}

impl<I: Identifier, Tr: PeerTransport<I>> Mesh<I, Tr> {
    /// Test-only access to the owned transport, for fixtures that must
    /// drive transport-internal routing state (e.g. seeding the
    /// Router's post-bounce blacklist to represent a genuinely
    /// unroutable node). Production code NEVER reaches through the
    /// mesh to the transport — membership reads go through the
    /// published view / the mesh's own delegating methods.
    #[cfg(test)]
    pub(crate) fn transport_mut(&mut self) -> &mut Tr {
        &mut self.transport
    }

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
            slotless_hold: VecDeque::new(),
            slotless_hold_warn: crate::warn_throttle::WarnThrottle::new(INGRESS_WARN_INTERVAL),
            ingress_fallback_warn: crate::warn_throttle::WarnThrottle::new(INGRESS_WARN_INTERVAL),
            role_holder: RoleHolderView::new(),
            local_peer_id: None,
            relayed_ring: VecDeque::new(),
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
        // Capture this process's host peer-id on the first registration
        // (every local slot shares it) — the ingress relay's
        // "holder-is-local" test reads it even while the process is
        // momentarily slotless.
        if self.local_peer_id.is_none() {
            self.local_peer_id = Some(peer_id.clone());
        }
        // One ingest-freshness cell per trio: the slot records at its
        // delivery choke point; the inbox reads. Minted here so the pair
        // cannot mismatch (the M3 trio rule).
        let ingest_liveness = super::ingest_liveness::IngestLiveness::new();
        let slot = Arc::new(RoleSlot::with_ingest_liveness(
            role,
            peer_id,
            inbound_tx,
            ingest_liveness.clone(),
        ));
        // The transport's ingest-edge clocks (arrival at the connection
        // read loops / drained at the pump's `recv_peer` pull) ride the
        // SAME inbox handle as the slot-ingest cell, so a coordinator's
        // liveness reads reach the earliest attributable measuring point
        // on this node without ever touching the transport. `None` for
        // transports that cannot observe arrival pre-`recv_peer`.
        let transport_edges = self.transport.ingest_edges();
        self.set_slot(role, Arc::downgrade(&slot));
        let client = MeshClient::new(
            role,
            self.local_dispatch_tx.clone(),
            self.membership.clone(),
            self.role_holder.clone(),
        );
        let inbox = RoleInbox::new(inbound_rx, ingest_liveness, transport_edges);
        // A freshly-registered slot is the first live local delivery target if
        // the process was momentarily slotless: replay any ingress frames held
        // through that window so the new coordinator sees them (the
        // promotion / role-swap no-drop guarantee).
        self.drain_slotless_hold();
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
        [
            LocalRole::Primary,
            LocalRole::Secondary,
            LocalRole::Observer,
        ]
        .into_iter()
        .filter_map(|r| self.slot_for(r))
        .filter_map(|w| w.upgrade())
        .any(|arc| arc.peer_id() == id)
    }
}
