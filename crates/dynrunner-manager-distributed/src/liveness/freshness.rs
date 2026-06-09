//! [`BeaconLiveness`] — the cloneable, listener-published per-node beacon
//! freshness view.
//!
//! # Concern
//!
//! The transport-independent counterpart of the mesh-frame liveness clock,
//! for the failover-detector's UNION. A [`super::LivenessListener`] decodes
//! every inbound beacon datagram and records `node_id -> Instant` here (the
//! local receipt time). A detached reader — the secondary's election tick —
//! polls "when did I last hear this node's BEACON" and unions it with its
//! mesh-frame view of the same node: a node is declared silent only when
//! BOTH its beacon AND its frames are stale, exactly mirroring the
//! primary-side death-clock where the beacon and the inbound frame both
//! refresh one freshness timestamp.
//!
//! # Why a published cell, not a channel
//!
//! Mirrors [`crate::process::MembershipView`] / [`super::BeaconTarget`]: a
//! shared `Arc<Mutex<_>>` the listener publishes and a detached reader
//! samples on its own cadence — the POLL interface (the election tick reads
//! it once per keepalive tick). The listener's per-datagram PUSH interface
//! (an `mpsc` of node-ids) serves a different subscriber (the primary's
//! reaper, which reacts per-datagram); the two subscription styles are
//! independent outputs of the one decode, so a listener can drive both
//! without either consumer knowing about the other.
//!
//! # Freshness, not membership
//!
//! This view records ONLY positive liveness (a beacon was heard at time
//! `t`); it never removes entries. Staleness is the reader's concern — it
//! compares `now - last_seen` against its own threshold. A node that stops
//! beaconing simply stops refreshing its entry, so the reader sees the age
//! grow without any explicit eviction. (`record_keepalive`-style eviction
//! belongs to the role-specific death-clock, not to this raw freshness
//! view.)

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// A cloneable handle to the per-node beacon-receipt freshness view.
///
/// Every clone shares one cell. The listener holds the write side
/// ([`BeaconLiveness::record`]); a role's liveness-tracker holds a clone
/// for [`BeaconLiveness::last_seen`].
#[derive(Clone, Default)]
pub struct BeaconLiveness {
    inner: Arc<Mutex<HashMap<String, Instant>>>,
}

impl BeaconLiveness {
    /// A fresh view with no beacons heard yet.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Record that `node_id`'s beacon was just received (local receipt
    /// `Instant`). Called by the listener per decoded datagram. Keying on a
    /// monotonic receipt `Instant` (mirroring the secondary's
    /// `primary_last_seen` and the primary's `secondary_keepalives`) makes
    /// the freshness immune to a coordinated host suspend/resume.
    pub fn record(&self, node_id: &str) {
        self.inner
            .lock()
            .expect("beacon liveness poisoned")
            .insert(node_id.to_string(), Instant::now());
    }

    /// The most recent beacon-receipt `Instant` for `node_id`, or `None`
    /// if no beacon from it has ever been heard. The reader compares
    /// `now - last_seen` against its own staleness threshold.
    pub fn last_seen(&self, node_id: &str) -> Option<Instant> {
        self.inner
            .lock()
            .expect("beacon liveness poisoned")
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
        let v = BeaconLiveness::new();
        let reader = v.clone();
        assert_eq!(reader.last_seen("primary-0"), None);
        v.record("primary-0");
        let first = reader.last_seen("primary-0").expect("recorded");
        // A later record refreshes the same node's entry to a newer instant.
        std::thread::sleep(Duration::from_millis(2));
        v.record("primary-0");
        let second = reader.last_seen("primary-0").expect("refreshed");
        assert!(second >= first, "the entry advances on a fresh beacon");
        // A different node is tracked independently.
        assert_eq!(reader.last_seen("secondary-1"), None);
        v.record("secondary-1");
        assert!(reader.last_seen("secondary-1").is_some());
    }
}
