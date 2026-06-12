//! Process-owned mesh primitives — the role-demux layer between a peer's
//! coordinators and the role-agnostic transport.
//!
//! # Why this module exists (SUPREME-LAW #5: transport ⊥ roles)
//!
//! The transport (`PeerNetwork` / `ChannelPeerTransport`) knows ONLY
//! `PeerId`s — it never resolves or mentions a role. Yet a peer that runs
//! several coordinators (a primary + its own secondary after a promotion)
//! must demux an incoming directed frame to the RIGHT local coordinator
//! and must let a coordinator address a same-process sibling on the SAME
//! peer through a normal `Destination`, never a special same-peer code
//! path. That role-aware demux lives HERE, in `manager-distributed`,
//! wrapped strictly on top of the role-agnostic transport. The transport
//! stays untouched for roles.
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
//! - [`node::Node`] — the *OS-process role composition shell*: the
//!   [`mesh::Mesh`] + one nullable [`node::RoleEntry`] per role + the
//!   promotion/demote lifecycle channels. SKELETON only here (the struct +
//!   `RoleEntry` + [`node::PromotionSignal`] + channel plumbing); the
//!   `run` composition, the promotion build, and the BUG-6 teardown are
//!   the node-wiring wave. Boundary: names the coordinators by generic
//!   parameter, never reaching into one.
//!
//! # Scope (this module) vs the coordinator-rewire waves
//!
//! The wire frame's per-variant routing `target: Option<Destination>` (the
//! C3 field), the `Mesh` ingress demux ([`mesh::Mesh::route_incoming`]),
//! the in-place mesh retag ([`mesh::Mesh::retag_local_role`]), and the
//! [`node::Node`] SKELETON all live now. Still to come (the
//! coordinator-per-agent waves): the coordinators dropping their transport
//! generic to take a [`mesh_client::MeshClient`] + [`mesh_client::RoleInbox`],
//! stamping the resolved `target` at their egress edges, and the
//! `Node::run` composition that registers roles, spawns the coordinators +
//! mesh-pump, and drives promotion/demotion. `dispatch`/`deliver_local`/
//! `route_incoming` already ACCEPT a role-bearing target so those waves
//! plug in without reshaping the API.

pub mod ingest_liveness;
pub mod membership;
pub mod mesh;
pub mod mesh_client;
pub mod mesh_host;
pub mod node;
pub mod pump;
pub mod role;
pub mod role_slot;
pub mod run;
pub mod run_inputs;

#[cfg(test)]
mod tests;

pub use ingest_liveness::IngestLiveness;
pub use membership::MembershipView;
pub use mesh::Mesh;
pub use mesh::role_holder::{RoleHolderView, attach_primary_recognition};
pub use mesh_client::{LocalDispatch, MeshClient, RoleInbox};
pub use mesh_host::MeshHost;
pub use node::{Node, PromotionSignal, RoleEntry};
pub use pump::{MeshControl, MeshControlHandle};
pub use role::LocalRole;
pub use role_slot::RoleSlot;
pub use run::{NodeRunOutcome, RunTerminal};
pub use run_inputs::{
    NodeRunInputs, PrimaryRunArgs, PromotedPrimary, PromotedPrimaryBuilder, SeedSource,
};
