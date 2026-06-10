//! [`IngestLiveness`] — the slot-published per-peer FRAME-INGEST
//! freshness view.
//!
//! # Concern
//!
//! The INGEST-time counterpart of the dispatch-time death clocks, for
//! flood-immune liveness decisions. [`super::RoleSlot::deliver`] — the
//! single choke point every loopback + wire frame flows through on its
//! way into a role's inbox — records `sender_id -> Instant` here at the
//! moment the frame ENTERS the inbox. A detached reader — the primary's
//! heartbeat sweep — polls "when did a frame from this peer last ARRIVE
//! at my inbox" and unions it with its processed-frame view of the same
//! peer.
//!
//! # Why ingest-time, not processing-time (the flood-immunity law)
//!
//! The processing-time clocks (`secondary_keepalives`, refreshed when a
//! frame is DISPATCHED) measure "when did I last get around to handling
//! this peer's frame" — under inbox starvation (a flooded operational
//! loop, the run_20260610_221140 face: depth 52654 with the keepalive
//! arm starved) that clock inflates while the peer's keepalives sit
//! QUEUED, and the death declaration becomes "we are busy", not "the
//! peer is silent". This view is the honest substrate: a frame recorded
//! here proves the peer was alive at receipt regardless of when (or
//! whether) the loop processes it, so a starved node cannot author a
//! removal for a peer whose frames are in its backed-up inbox.
//!
//! # Why a published cell, not a channel
//!
//! Mirrors [`crate::liveness::BeaconLiveness`] / [`super::MembershipView`]:
//! a shared `Arc<Mutex<_>>` the delivery choke point publishes and a
//! detached reader samples on its own cadence (the heartbeat tick reads
//! it once per sweep). Minted per role-slot trio by
//! [`super::Mesh::register_local_role`] so the inbox handle a
//! coordinator holds carries its own ingest view — no extra composition
//! plumbing, and the trio cannot mismatch.
//!
//! # Freshness, not membership
//!
//! This view records ONLY positive liveness (a frame arrived at time
//! `t`); it never removes entries. Staleness is the reader's concern —
//! it compares `now - last_seen` against its own threshold. A peer that
//! stops sending simply stops refreshing its entry. (Eviction belongs
//! to the role-specific death-clock, not to this raw freshness view.)

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// A cloneable handle to the per-peer frame-ingest freshness view.
///
/// Every clone shares one cell. The role slot holds the write side
/// ([`IngestLiveness::record`], called from `RoleSlot::deliver`); the
/// matching [`super::RoleInbox`] holds a clone for
/// [`IngestLiveness::last_seen`].
#[derive(Clone, Default)]
pub struct IngestLiveness {
    inner: Arc<Mutex<HashMap<String, Instant>>>,
}

impl IngestLiveness {
    /// A fresh view with no frames ingested yet.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Record that a frame from `node_id` just ENTERED the inbox (local
    /// receipt `Instant`). Called by the slot's delivery choke point per
    /// frame. Keying on a monotonic receipt `Instant` (mirroring the
    /// secondary's `primary_last_seen` and the primary's
    /// `secondary_keepalives`) makes the freshness immune to a
    /// coordinated host suspend/resume.
    pub fn record(&self, node_id: &str) {
        self.inner
            .lock()
            .expect("ingest liveness poisoned")
            .insert(node_id.to_string(), Instant::now());
    }

    /// The most recent frame-ingest `Instant` for `node_id`, or `None`
    /// if no frame from it has ever entered the inbox. The reader
    /// compares `now - last_seen` against its own staleness threshold.
    pub fn last_seen(&self, node_id: &str) -> Option<Instant> {
        self.inner
            .lock()
            .expect("ingest liveness poisoned")
            .get(node_id)
            .copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn record_observed_by_clones() {
        let v = IngestLiveness::new();
        let reader = v.clone();
        assert_eq!(reader.last_seen("secondary-0"), None);
        v.record("secondary-0");
        let first = reader.last_seen("secondary-0").expect("recorded");
        // A later record refreshes the same node's entry to a newer instant.
        std::thread::sleep(Duration::from_millis(2));
        v.record("secondary-0");
        let second = reader.last_seen("secondary-0").expect("refreshed");
        assert!(second >= first, "the entry advances on a fresh frame");
        // A different node is tracked independently.
        assert_eq!(reader.last_seen("secondary-1"), None);
        v.record("secondary-1");
        assert!(reader.last_seen("secondary-1").is_some());
    }
}
