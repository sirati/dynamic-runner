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

use super::membership::MembershipView;
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
}

impl<I: Identifier> Clone for MeshClient<I> {
    fn clone(&self) -> Self {
        Self {
            origin: self.origin,
            egress: self.egress.clone(),
            membership: self.membership.clone(),
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
    ) -> Self {
        Self {
            origin,
            egress,
            membership,
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
}

/// The receive end of a role's mesh inbound.
///
/// The coordinator drains this for frames the mesh delivered to its slot
/// (loopback siblings + remote peers, demuxed by the ingress role-target
/// table). Minted alongside the [`MeshClient`] + `Arc<RoleSlot>`; the
/// slot's inbound `Sender` is the matching write end.
pub struct RoleInbox<I: Identifier> {
    rx: mpsc::UnboundedReceiver<DistributedMessage<I>>,
}

impl<I: Identifier> RoleInbox<I> {
    /// Internal mint — only [`super::Mesh::register_local_role`] calls
    /// this so the inbox is always paired with its slot's inbound sender.
    pub(super) fn new(rx: mpsc::UnboundedReceiver<DistributedMessage<I>>) -> Self {
        Self { rx }
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
}
