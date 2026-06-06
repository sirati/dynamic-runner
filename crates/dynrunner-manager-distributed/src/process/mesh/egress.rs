//! The local-dispatch (egress) queue endpoints of [`Mesh`].
//!
//! # Concern
//!
//! ONE concern: own the queue endpoints the detached
//! [`super::super::mesh_client::MeshClient`]s feed and the mesh-pump
//! ([`super::super::pump`]) drains — apply one queued item against the LIVE
//! slot set, the in-place drain for the pump-less `process/tests` harness,
//! and the take-out that hands the receiver to the pump disjointly from
//! `&mut Mesh` (the E0499 resolution).

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::PeerTransport;
use tokio::sync::mpsc;

use super::super::mesh_client::LocalDispatch;
use super::Mesh;

impl<I: Identifier, Tr: PeerTransport<I>> Mesh<I, Tr> {
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
}
