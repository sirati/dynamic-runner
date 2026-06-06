//! [`MembershipView`] — a pump-published, live-read snapshot of the
//! transport's mesh membership.
//!
//! # Concern
//!
//! A detached [`super::MeshClient`] (held by a coordinator, off the
//! pump's `LocalSet`) must answer `peer_count()` / `has_peer()` honestly,
//! but it cannot borrow the by-value transport the pump owns. This type
//! is the ONLY bridge: a shared cell the [`super::Mesh`] writes from a
//! LIVE transport read and the client reads.
//!
//! # Why this is not a "shadow counter" (the SETTLED no-shadow rule)
//!
//! The convergent 3-lens finding (dirty-D8 / apis-RV-1 / maint-B1) bans a
//! parallel `AtomicUsize` that the MANAGER layer increments/decrements:
//! such a counter cannot textually reach the transport/Router crate's 5×
//! `connections.remove` sites without breaking SUPREME-LAW #5, so it
//! silently drifts and re-creates the §14/§15 keepalive-self-death class.
//!
//! This view is the explicitly-blessed alternative: a PUMP-PUBLISHED
//! cardinality whose value is ALWAYS a direct
//! [`dynrunner_protocol_primary_secondary::PeerTransport::peer_count`]
//! read (and the live `connected` id-set), never a hand-maintained delta.
//! [`MembershipView::publish`] takes the snapshot the mesh just read off
//! the live transport; nothing here ever increments.
//!
//! # Staleness contract (explicit, per the SETTLED finding)
//!
//! A read reflects membership AS OF the last [`MembershipView::publish`]
//! call (the mesh-pump publishes once per drain cycle). It is therefore
//! at most one pump-cycle stale — bounded, monotone-toward-truth, and
//! NEVER wrong in a way a hand-counter is (it cannot miss a remove). A
//! caller that needs the instantaneous count reads the transport directly
//! through the mesh; the view is for the detached send-handle path.
//!
//! # Boundary
//!
//! Lives in `manager-distributed`. The mesh writes; the client reads;
//! neither sees the other's internals. It carries only the role-agnostic
//! cardinality + the connected `PeerId` set — no role, no slot, no
//! transport type.

use std::sync::{Arc, Mutex};

use dynrunner_protocol_primary_secondary::address::PeerId;

/// The published membership snapshot. Cardinality + the connected id-set,
/// both taken from a live transport read.
#[derive(Debug, Clone, Default)]
struct MembershipSnapshot {
    /// `transport.peer_count()` at publish time (= `connections.len()`).
    count: usize,
    /// The connected peer-ids at publish time. `has_peer` reads this so a
    /// per-id answer is as honest as the cardinality.
    connected: Vec<PeerId>,
}

/// A cloneable, pump-published live-read view of mesh membership.
///
/// Every clone shares one cell. The mesh holds the write side
/// ([`MembershipView::publish`]); each [`super::MeshClient`] holds a clone
/// for [`MembershipView::peer_count`] / [`MembershipView::has_peer`].
#[derive(Clone)]
pub struct MembershipView {
    inner: Arc<Mutex<MembershipSnapshot>>,
}

impl MembershipView {
    /// A fresh view reporting an empty mesh until the first
    /// [`MembershipView::publish`].
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(MembershipSnapshot::default())),
        }
    }

    /// Publish the membership the mesh just read LIVE off the transport.
    ///
    /// Called by the mesh once per pump cycle with
    /// `transport.peer_count()` + the connected id-set. This is the ONLY
    /// writer; the value is never derived from a delta. `connected` is
    /// taken by value to keep the snapshot self-owned (no borrow of the
    /// transport's `connections` table escapes the mesh).
    pub fn publish(&self, count: usize, connected: Vec<PeerId>) {
        let mut guard = self.inner.lock().expect("membership view poisoned");
        guard.count = count;
        guard.connected = connected;
    }

    /// The last-published peer count (live read as of the last pump
    /// cycle — see the staleness contract in the module docs).
    pub fn peer_count(&self) -> usize {
        self.inner.lock().expect("membership view poisoned").count
    }

    /// Whether `id` was a connected member as of the last publish.
    pub fn has_peer(&self, id: &PeerId) -> bool {
        self.inner
            .lock()
            .expect("membership view poisoned")
            .connected
            .iter()
            .any(|p| p == id)
    }
}

impl Default for MembershipView {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fresh view reports an empty mesh; a publish moves it to the
    /// published value; clones share the same cell.
    #[test]
    fn publish_is_observed_by_clones() {
        let view = MembershipView::new();
        assert_eq!(view.peer_count(), 0);
        assert!(!view.has_peer(&PeerId::from("a")));

        let reader = view.clone();
        view.publish(2, vec![PeerId::from("a"), PeerId::from("b")]);

        assert_eq!(reader.peer_count(), 2);
        assert!(reader.has_peer(&PeerId::from("a")));
        assert!(reader.has_peer(&PeerId::from("b")));
        assert!(!reader.has_peer(&PeerId::from("c")));
    }

    /// A later publish wholly REPLACES the prior snapshot (it is a live
    /// read, not a delta) — a peer that left is gone, not decremented.
    #[test]
    fn publish_replaces_not_accumulates() {
        let view = MembershipView::new();
        view.publish(
            3,
            vec![PeerId::from("a"), PeerId::from("b"), PeerId::from("c")],
        );
        assert_eq!(view.peer_count(), 3);

        // Peer "b" left; the mesh re-reads the live transport (count 2,
        // set {a,c}) and republishes the whole snapshot.
        view.publish(2, vec![PeerId::from("a"), PeerId::from("c")]);
        assert_eq!(view.peer_count(), 2);
        assert!(view.has_peer(&PeerId::from("a")));
        assert!(
            !view.has_peer(&PeerId::from("b")),
            "departed peer is gone, not decremented"
        );
        assert!(view.has_peer(&PeerId::from("c")));
    }
}
