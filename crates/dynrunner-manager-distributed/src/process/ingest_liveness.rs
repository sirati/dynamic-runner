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

/// The per-peer frame-ingest freshness view: the slot-mounted instance
/// of the shared [`FreshnessClock`] mechanism (defined once in the
/// protocol crate; the transports mount the same cell type on their own
/// ingest edges — see `dynrunner_protocol_primary_secondary::freshness`).
///
/// Every clone shares one cell. The role slot holds the write side
/// (`record`, called from `RoleSlot::deliver` with a monotonic receipt
/// `Instant` — suspend/resume-immune like the secondary's
/// `primary_last_seen` and the primary's `secondary_keepalives`); the
/// matching [`super::RoleInbox`] holds a clone for `last_seen`.
pub type IngestLiveness = dynrunner_protocol_primary_secondary::FreshnessClock;

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
