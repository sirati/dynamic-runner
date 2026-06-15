//! [`PeerLivenessAddrs`] — the cloneable, node-scoped peer→liveness-address
//! book.
//!
//! # Concern
//!
//! ONE concern: hold `node_id -> liveness SocketAddr` for every peer that
//! advertised a `liveness_port` + `ipv4` in its `PeerInfo`, so a beacon
//! emitter can resolve a peer-id to the raw UDP address its
//! [`super::LivenessListener`] is bound on (the beacon is a raw socket, so
//! it cannot route by peer-id through the QUIC mesh — it needs the concrete
//! `ipv4:port`).
//!
//! # Why a SHARED node-scoped cell, not a per-coordinator map
//!
//! A compute node runs its `SecondaryCoordinator` and, after a
//! promotion/relocation, its `PrimaryCoordinator` CONCURRENTLY in one
//! process. Only the secondary observes the setup-phase `PeerInfo`
//! broadcast that carries every peer's liveness address; the promoted
//! primary is built from the replicated CRDT (which carries no liveness
//! addresses) and never re-runs the cert-exchange handshake — so it has NO
//! address for its secondaries on its own. This cell is the bridge: the
//! SECONDARY writes the book from `PeerInfo` ([`PeerLivenessAddrs::ingest`])
//! and the promoted PRIMARY reads it ([`PeerLivenessAddrs::get`]) to build
//! its beacon target set. Mirrors [`super::BeaconTarget`] /
//! [`crate::process::MembershipView`]: a shared `Arc<Mutex<_>>` one role
//! publishes and a detached reader consumes.
//!
//! Addresses are stable for a run (a node's `ipv4:port` does not change
//! mid-run), so the LAST `PeerInfo`-derived book stays valid across the
//! promotion even though no new `PeerInfo` flows post-setup.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use dynrunner_protocol_primary_secondary::PeerConnectionInfo;

/// A cloneable handle to the node's peer→liveness-address book.
///
/// Every clone shares one cell. The secondary holds the write side
/// ([`PeerLivenessAddrs::ingest`]); the secondary's beacon-target resolver
/// and the promoted primary's beacon-target builder hold clones for
/// [`PeerLivenessAddrs::get`].
#[derive(Clone, Default)]
pub struct PeerLivenessAddrs {
    inner: Arc<Mutex<HashMap<String, SocketAddr>>>,
}

impl PeerLivenessAddrs {
    /// A fresh, empty book.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Rebuild the book from a `PeerInfo` roster. For each peer that
    /// advertised a `liveness_port` AND a parseable `ipv4`, record
    /// `(ipv4:port)`; a peer missing either is simply absent (no beacon
    /// address known — strictly better than a bogus one). IPv4 is the
    /// beacon transport (the QUIC mesh's primary LAN family); ipv6-only
    /// peers are not beaconed. Wholly replaces the prior book.
    pub fn ingest(&self, peers: &[PeerConnectionInfo]) {
        let book: HashMap<String, SocketAddr> = peers
            .iter()
            .filter_map(|p| {
                let port = p.liveness_port?;
                let ipv4 = p.ipv4.as_deref()?;
                let addr: SocketAddr = format!("{ipv4}:{port}").parse().ok()?;
                Some((p.secondary_id.clone(), addr))
            })
            .collect();
        *self.inner.lock().expect("peer liveness addrs poisoned") = book;
    }

    /// The liveness `SocketAddr` advertised by `node_id`, or `None` if it
    /// advertised no beacon address (or is unknown to this node).
    pub fn get(&self, node_id: &str) -> Option<SocketAddr> {
        self.inner
            .lock()
            .expect("peer liveness addrs poisoned")
            .get(node_id)
            .copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(id: &str, ipv4: Option<&str>, port: Option<u16>) -> PeerConnectionInfo {
        PeerConnectionInfo {
            secondary_id: id.to_string(),
            cert: String::new(),
            ipv4: ipv4.map(str::to_owned),
            ipv6: None,
            port: 0,
            is_observer: false,
            liveness_port: port,
            slurm_job_id: None,
        }
    }

    #[test]
    fn ingest_then_get_is_shared_across_clones() {
        let book = PeerLivenessAddrs::new();
        let reader = book.clone();
        assert_eq!(reader.get("secondary-1"), None);
        book.ingest(&[
            peer("secondary-1", Some("10.0.0.1"), Some(5001)),
            peer("secondary-2", Some("10.0.0.2"), Some(5002)),
            // No liveness_port → absent.
            peer("secondary-3", Some("10.0.0.3"), None),
            // No ipv4 → absent.
            peer("secondary-4", None, Some(5004)),
        ]);
        assert_eq!(reader.get("secondary-1"), "10.0.0.1:5001".parse().ok());
        assert_eq!(reader.get("secondary-2"), "10.0.0.2:5002".parse().ok());
        assert_eq!(reader.get("secondary-3"), None);
        assert_eq!(reader.get("secondary-4"), None);
        // A later ingest wholly replaces the prior book.
        book.ingest(&[peer("secondary-9", Some("10.0.0.9"), Some(5009))]);
        assert_eq!(reader.get("secondary-1"), None);
        assert_eq!(reader.get("secondary-9"), "10.0.0.9:5009".parse().ok());
    }
}
