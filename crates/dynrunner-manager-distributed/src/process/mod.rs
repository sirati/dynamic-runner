//! Process-owned mesh primitives — the role-demux layer between a peer's
//! coordinators and the role-agnostic transport.
//!
//! # Why this module exists (SUPREME-LAW #5: transport ⊥ roles)
//!
//! The transport (`PeerNetwork` / `ChannelPeerTransport`) knows ONLY
//! `PeerId`s — it never resolves or mentions a role. Yet a peer that runs
//! several coordinators (a primary + its own secondary after a promotion)
//! must demux an incoming directed frame to the RIGHT local coordinator
//! and must let a coordinator address a same-process sibling without a
//! "co-located" concept. That role-aware demux lives HERE, in
//! `manager-distributed`, wrapped strictly on top of the role-agnostic
//! transport. The transport stays untouched for roles.
//!
//! # The five types and their single concern + boundary
//!
//! Each type owns exactly ONE concern; no two share state through
//! anything but the typed APIs below (a caller of one never reaches into
//! another's internals):
//!
//! - [`role::LocalRole`] — *which* of the three local roles a slot/frame
//!   is for. Derives from the protocol crate's `Destination` so there is
//!   ONE role vocabulary. Boundary: pure value over `Destination`/`PeerId`.
//! - [`role_slot::RoleSlot`] — a local role's *inbound endpoint +
//!   identity*. The `Process` holds the `Arc`, the `Mesh` holds the
//!   `Weak`: dropping the `Arc` is role DEATH (the `Weak` stops
//!   upgrading); an atomic `set_role` is the in-place RETAG. Boundary:
//!   holds a `DistributedMessage` inbound `Sender` + a `PeerId` + an
//!   atomic role; names no transport.
//! - [`membership::MembershipView`] — a *pump-published live-read* of
//!   transport membership, so a detached send-handle answers
//!   `peer_count`/`has_peer` honestly WITHOUT a shadow counter (the
//!   SETTLED no-shadow rule). Boundary: the `Mesh` writes a live
//!   transport read; the `MeshClient` reads.
//! - [`mesh::Mesh`] — *route a `(target, frame)` to a local role-slot
//!   (loopback) or a remote connection, by id*. Owns the transport + the
//!   three `Weak` slots + the membership view; adds ONLY the role-slot
//!   demux on top of the transport's existing membership/loopback/
//!   broadcast (clarification RV-2). Boundary: callers hand a `LocalRole`
//!   origin + a `Destination` target; never touch the transport or slots.
//! - [`mesh_client::MeshClient`] + [`mesh_client::RoleInbox`] — a role's
//!   *locality-oblivious send capability + inbound stream*. The trio
//!   `(Arc<RoleSlot>, MeshClient, RoleInbox)` is minted together by
//!   [`mesh::Mesh::register_local_role`] so it cannot mismatch (M3).
//!   Boundary: the coordinator holds these; sees neither the mesh nor the
//!   transport.
//!
//! # C0 scope (this module) vs later phases
//!
//! These are the linchpin types only. The `Process` object (composition +
//! mesh-pump + spawn/drive) is C1; the egress collapse at the coordinator
//! `send_to` edges is C2; the explicit per-frame `target: Destination`
//! field on the wire type is C3. C0 designs `dispatch`/`deliver_local` to
//! ACCEPT a role-bearing target so those phases plug in without reshaping
//! the API.

pub mod membership;
pub mod mesh;
pub mod mesh_client;
pub mod role;
pub mod role_slot;

#[cfg(test)]
mod tests;

pub use membership::MembershipView;
pub use mesh::Mesh;
pub use mesh_client::{LocalDispatch, MeshClient, RoleInbox};
pub use role::LocalRole;
pub use role_slot::RoleSlot;
