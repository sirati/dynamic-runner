//! [`MeshClient`] + [`RoleInbox`] — a role's locality-oblivious send
//! capability and its inbound stream.
//!
//! # Concern
//!
//! A coordinator must send to ANY destination — a remote peer OR a
//! same-process sibling role — without knowing which it is, and must
//! receive frames addressed to its own role. These two ends are the
//! coordinator's entire view of the mesh:
//!
//! - [`MeshClient`] is the send capability. It is locality-oblivious: the
//!   coordinator hands it a role-bearing [`Destination`] and the frame;
//!   the client QUEUES `(origin_role, target, frame)` onto the mesh's
//!   local-dispatch queue, and the mesh-pump resolves loopback-vs-remote
//!   against the LIVE slot set. This is the "MeshSendHandle + local
//!   deliver" capability the clarification (M3) requires unified into ONE
//!   path: the coordinator never owns local delivery, and a remote send
//!   and a loopback send are the same call. `peer_count`/`has_peer` read
//!   the pump-published [`MembershipView`] — honest by type (no fake-0).
//! - [`RoleInbox`] is the receive end: the coordinator drains it for the
//!   frames the mesh delivered to its slot.
//!
//! # Queued, not synchronous (clarification M4)
//!
//! A [`MeshClient::send`] does NOT deliver synchronously. It enqueues onto
//! the local-dispatch queue, drained later by the mesh-pump (the same
//! contract the existing `MeshSendHandle` has — its sends are drained by
//! the transport's `recv_peer`). No caller may assume delivery-on-send.
//!
//! # Minted together (clarification M3)
//!
//! The trio `(Arc<RoleSlot>, MeshClient, RoleInbox)` is created in ONE
//! place — [`super::Mesh::register_local_role`] — so a client can never be
//! paired with the wrong inbox or slot. There is no public standalone
//! constructor for a client/inbox that bypasses the mint.
//!
//! # Boundary
//!
//! Lives in `manager-distributed`. The coordinator holds these; it never
//! sees the [`super::Mesh`], the transport, or the other roles' slots. The
//! frame carried is the existing wire [`DistributedMessage<I>`] — no new
//! envelope.

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::DistributedMessage;
use dynrunner_protocol_primary_secondary::address::{Destination, PeerId};
use tokio::sync::mpsc;

use super::ingest_liveness::IngestLiveness;
use super::membership::MembershipView;
use super::mesh::role_holder::RoleHolderView;
use super::role::LocalRole;

/// One queued egress item from a [`MeshClient`], drained by the mesh-pump
/// and applied against the LIVE slot set via [`super::Mesh::dispatch`].
///
/// It carries the origin role (for the `All`-fan origin-exclusion —
/// clarification BUG-1) and the role-bearing [`Destination`] target (NOT
/// the role-erased `SendTarget` — clarification dirty-D2), so the mesh
/// resolves loopback-vs-remote per-frame with no cached `Weak`s.
pub struct LocalDispatch<I: Identifier> {
    /// The role that originated this send (the mesh excludes it from an
    /// `All` fan).
    pub origin: LocalRole,
    /// The role-bearing destination the mesh demuxes against its live
    /// slots.
    pub target: Destination,
    /// The frame to route.
    pub frame: DistributedMessage<I>,
}

/// A role's locality-oblivious send capability over the mesh.
///
/// Cloneable: every clone shares the one local-dispatch queue and the one
/// [`MembershipView`]. Held by a coordinator; minted by
/// [`super::Mesh::register_local_role`] alongside the matching
/// [`RoleInbox`] + `Arc<RoleSlot>` so the trio cannot mismatch.
pub struct MeshClient<I: Identifier> {
    /// The originating role stamped onto every send (BUG-1 origin key).
    origin: LocalRole,
    /// The queue feeding the mesh-pump. Sends are QUEUED here, never
    /// delivered synchronously (M4).
    egress: mpsc::UnboundedSender<LocalDispatch<I>>,
    /// Pump-published live-read membership (no shadow counter).
    membership: MembershipView,
    /// The mesh's coordinator-published Primary-holder view (the
    /// recognition→routing bridge). The coordinator's constructor
    /// hands a clone to
    /// [`super::mesh::role_holder::attach_primary_recognition`] so its
    /// `ClusterState` role-change hook publishes the routing-holder
    /// fact the mesh's ingress relay reads.
    role_holder: RoleHolderView,
}

impl<I: Identifier> Clone for MeshClient<I> {
    fn clone(&self) -> Self {
        Self {
            origin: self.origin,
            egress: self.egress.clone(),
            membership: self.membership.clone(),
            role_holder: self.role_holder.clone(),
        }
    }
}

impl<I: Identifier> MeshClient<I> {
    /// Internal mint — only [`super::Mesh::register_local_role`] calls
    /// this, so a client is always paired with its slot + inbox.
    pub(super) fn new(
        origin: LocalRole,
        egress: mpsc::UnboundedSender<LocalDispatch<I>>,
        membership: MembershipView,
        role_holder: RoleHolderView,
    ) -> Self {
        Self {
            origin,
            egress,
            membership,
            role_holder,
        }
    }

    /// The role this client sends as. Stamped onto every send as the
    /// `All`-fan origin (BUG-1).
    pub fn origin(&self) -> LocalRole {
        self.origin
    }

    /// Queue a frame for `target`.
    ///
    /// Locality-oblivious: `target` is a role-bearing [`Destination`]; the
    /// mesh-pump resolves loopback-vs-remote against the live slots. This
    /// is QUEUED, not synchronous (M4) — `Err` only if the mesh-pump (the
    /// queue's receiver) has been dropped, i.e. the process is winding
    /// down. The frame is unrecoverable then (no pump to drain it), so the
    /// error is a small reason string — matching the existing
    /// `MeshSendHandle` send shape.
    pub fn send(&self, target: Destination, frame: DistributedMessage<I>) -> Result<(), String> {
        self.egress
            .send(LocalDispatch {
                origin: self.origin,
                target,
                frame,
            })
            .map_err(|_| "mesh-pump (local-dispatch receiver) dropped".to_string())
    }

    /// Live mesh cardinality as of the last pump publish (see
    /// [`MembershipView`]'s staleness contract). Honest by type: a
    /// detached client reading a published live count can never report the
    /// old fake 0-peer count a same-peer detached send-handle used to
    /// return.
    pub fn peer_count(&self) -> usize {
        self.membership.peer_count()
    }

    /// Whether `id` was a connected member as of the last pump publish.
    pub fn has_peer(&self, id: &PeerId) -> bool {
        self.membership.has_peer(id)
    }

    /// Whether the transport could DELIVER to `id` (direct OR relay) as
    /// of the last pump publish. The deliverability companion to
    /// [`Self::has_peer`] — see `MembershipView::has_route` for the
    /// formula and the has_route-vs-has_peer consumer split.
    pub fn has_route(&self, id: &PeerId) -> bool {
        self.membership.has_route(id)
    }

    /// Clone of the mesh's Primary-holder view (the recognition→routing
    /// bridge). The coordinator's constructor hands this to
    /// [`super::mesh::role_holder::attach_primary_recognition`] so its
    /// `ClusterState` publishes the routing-holder fact the mesh's
    /// ingress relay reads — the coordinator itself never reads or
    /// writes the view directly.
    pub fn role_holder_view(&self) -> RoleHolderView {
        self.role_holder.clone()
    }
}

/// The receive end of a role's mesh inbound.
///
/// The coordinator drains this for frames the mesh delivered to its slot
/// (loopback siblings + remote peers, demuxed by the ingress role-target
/// table). Minted alongside the [`MeshClient`] + `Arc<RoleSlot>`; the
/// slot's inbound `Sender` is the matching write end.
pub struct RoleInbox<I: Identifier> {
    rx: mpsc::UnboundedReceiver<DistributedMessage<I>>,
    /// Read clone of the slot's frame-INGEST freshness view (the write
    /// side records in `RoleSlot::deliver`, the moment a frame enters
    /// this inbox's channel). The coordinator's flood-immune liveness
    /// reads ("when did a frame from X last ARRIVE, processed or not")
    /// go through [`Self::last_ingest_from`].
    ingest_liveness: IngestLiveness,
    /// Read clones of the TRANSPORT's ingest-edge clocks (arrival at
    /// the connection read loops, drained at the pump's `recv_peer`
    /// pull) — one queue upstream of the slot cell above. `None` when
    /// the transport cannot observe arrival earlier than its own
    /// `recv_peer` (see `PeerTransport::ingest_edges`). The
    /// coordinator's earliest-attributable liveness read is
    /// [`Self::last_transport_arrival_from`]; the removal gate samples
    /// the pair via [`Self::transport_ingest_edges`].
    transport_edges: Option<dynrunner_protocol_primary_secondary::IngestEdges>,
}

impl<I: Identifier> RoleInbox<I> {
    /// Internal mint — only [`super::Mesh::register_local_role`] calls
    /// this so the inbox is always paired with its slot's inbound sender
    /// AND the slot's ingest-freshness cell AND the owning transport's
    /// ingest-edge clocks.
    pub(super) fn new(
        rx: mpsc::UnboundedReceiver<DistributedMessage<I>>,
        ingest_liveness: IngestLiveness,
        transport_edges: Option<dynrunner_protocol_primary_secondary::IngestEdges>,
    ) -> Self {
        Self {
            rx,
            ingest_liveness,
            transport_edges,
        }
    }

    /// When did a frame from `node_id` last ENTER this inbox — recorded
    /// at the slot's delivery choke point, BEFORE the frame waits in the
    /// channel. `None` if no frame from it ever arrived. The flood-
    /// immunity read: a death clock that unions this with its
    /// processing-time view cannot declare a peer silent while that
    /// peer's frames sit in a backed-up inbox.
    pub fn last_ingest_from(&self, node_id: &str) -> Option<std::time::Instant> {
        self.ingest_liveness.last_seen(node_id)
    }

    /// When did a frame from `node_id` last arrive AT THE TRANSPORT —
    /// recorded by the connection read loops the moment the frame
    /// decodes, BEFORE it waits in the transport's inbound queue (one
    /// queue upstream of the slot cell behind
    /// [`Self::last_ingest_from`]). `None` if the transport publishes
    /// no arrival clock, or no frame from `node_id` ever decoded. The
    /// ingest-edge read: a death clock that unions this stays honest
    /// even when the MESH PUMP (not just the coordinator loop) is
    /// starved and arrived frames never reach the slot's delivery choke
    /// point — the run_20260611_115429 false-removal face.
    pub fn last_transport_arrival_from(&self, node_id: &str) -> Option<std::time::Instant> {
        self.transport_edges
            .as_ref()
            .and_then(|edges| edges.arrival.last_seen(node_id))
    }

    /// Cheap clones of the transport's ingest-edge clock pair, for the
    /// removal gate's arrival-vs-drained backlog sampling. `None` when
    /// the transport publishes none (the gate then stays inactive).
    pub fn transport_ingest_edges(
        &self,
    ) -> Option<dynrunner_protocol_primary_secondary::IngestEdges> {
        self.transport_edges.clone()
    }

    /// Await the next frame addressed to this role. `None` once every
    /// write end (the slot's inbound `Sender`) is dropped — the role's
    /// teardown signal.
    pub async fn recv(&mut self) -> Option<DistributedMessage<I>> {
        self.rx.recv().await
    }

    /// Non-blocking drain of one ready frame, if any.
    pub fn try_recv(&mut self) -> Option<DistributedMessage<I>> {
        self.rx.try_recv().ok()
    }

    /// Frames currently queued in this inbox — the accumulation-
    /// visibility read for the periodic collection-stats line. A
    /// drained loop holds ~zero; a persistently-growing depth means
    /// the owning coordinator is starved against its ingress rate
    /// (every queued frame is retained, cold, until processed).
    pub fn depth(&self) -> usize {
        self.rx.len()
    }
}
