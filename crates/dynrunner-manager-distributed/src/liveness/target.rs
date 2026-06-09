//! [`BeaconTarget`] — the cloneable, runtime-published SET of addresses the
//! liveness beacon thread sends to each tick.
//!
//! # Concern
//!
//! The beacon runs on a dedicated OS thread that a co-resident CPU-bound
//! build can NOT starve (that is the whole point). It therefore cannot
//! borrow the coordinator's `cluster_state` / mesh to learn WHO it must
//! reassure or WHERE their liveness sockets live — those live on the tokio
//! runtime the beacon exists to be independent of. This cell is the ONE
//! bridge: the runtime WRITES the set of liveness `SocketAddr`s the emitter
//! must reach, and the beacon thread READS the set each tick and sends one
//! datagram to every address.
//!
//! # One target type, two emitter roles
//!
//! The set generalizes the original single-primary target so the SAME
//! beacon mechanism serves both liveness directions without forking:
//!   - A SECONDARY reassures the ONE current primary, so it publishes a
//!     0-or-1-element set ([`BeaconTarget::publish_one`], the degenerate
//!     case — `None` when no primary is resolved yet). Re-pointed on
//!     `PrimaryChanged` from the primary's advertised
//!     [`dynrunner_protocol_primary_secondary::PeerConnectionInfo`]
//!     `liveness_port` + `ipv4`.
//!   - A PRIMARY reassures ALL of its live secondaries, so it publishes the
//!     N-element set ([`BeaconTarget::publish_set`]), rebuilt on each
//!     roster change. This is the half a CPU-starved primary needs: its
//!     mesh keepalive freezes with its runtime, but this dedicated-thread
//!     beacon keeps asserting the primary's liveness so its secondaries do
//!     not false-elect a successor.
//!
//! # Why a published cell, not a query
//!
//! Mirrors [`crate::process::MembershipView`]: a shared `Arc<Mutex<_>>`
//! the runtime publishes and a detached reader consumes, never a delta.
//! The beacon re-reads every tick, so a failover (`PrimaryChanged`) or a
//! roster change that republishes a new set is picked up on the next
//! beacon without the beacon knowing anything about elections or
//! membership — it just sends to whatever set is currently published.
//!
//! # Staleness during starvation
//!
//! When the runtime is CPU-starved it cannot republish — but the LAST
//! published set stays valid: a starved node's role/roster has not changed
//! (a role change would require this node's runtime to participate, which a
//! starved node cannot), so the beacon keeps sending to the last-known-good
//! set, which is exactly the peers it should be reassuring. An empty set
//! (no peers resolved yet) makes the beacon a no-op for that tick.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

/// A cloneable handle to the SET of liveness addresses the beacon thread
/// sends to.
///
/// Every clone shares one cell. The runtime holds the write side
/// ([`BeaconTarget::publish_one`] / [`BeaconTarget::publish_set`]); the
/// beacon thread holds a clone for [`BeaconTarget::current`].
#[derive(Clone, Default)]
pub struct BeaconTarget {
    inner: Arc<Mutex<Vec<SocketAddr>>>,
}

impl BeaconTarget {
    /// A fresh target with no addresses resolved yet (the beacon no-ops
    /// until the first publish).
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Publish the single address this emitter must reach (the SECONDARY's
    /// current primary). `None` clears the set (e.g. the primary departed
    /// and no successor is resolved yet). The degenerate 0-or-1 case of
    /// [`BeaconTarget::publish_set`].
    pub fn publish_one(&self, addr: Option<SocketAddr>) {
        self.publish_set(addr.into_iter().collect());
    }

    /// Publish the full set of addresses this emitter must reach (the
    /// PRIMARY's live secondaries). Wholly replaces the prior set — the
    /// runtime owns "who must I reach now", the beacon just transmits to
    /// whatever is published. An empty `Vec` no-ops the beacon.
    pub fn publish_set(&self, addrs: Vec<SocketAddr>) {
        *self.inner.lock().expect("beacon target poisoned") = addrs;
    }

    /// The last-published address set, read by the beacon thread each tick.
    pub fn current(&self) -> Vec<SocketAddr> {
        self.inner.lock().expect("beacon target poisoned").clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_one_observed_by_clones() {
        let t = BeaconTarget::new();
        assert!(t.current().is_empty());
        let reader = t.clone();
        let addr: SocketAddr = "10.0.0.1:9999".parse().unwrap();
        t.publish_one(Some(addr));
        assert_eq!(reader.current(), vec![addr]);
        // A failover republish wholly replaces the prior target.
        let addr2: SocketAddr = "10.0.0.2:8888".parse().unwrap();
        t.publish_one(Some(addr2));
        assert_eq!(reader.current(), vec![addr2]);
        // Clearing it (primary departed, no successor) no-ops the beacon.
        t.publish_one(None);
        assert!(reader.current().is_empty());
    }

    #[test]
    fn publish_set_holds_all_targets() {
        let t = BeaconTarget::new();
        let reader = t.clone();
        let a: SocketAddr = "10.0.0.1:1".parse().unwrap();
        let b: SocketAddr = "10.0.0.2:2".parse().unwrap();
        let c: SocketAddr = "10.0.0.3:3".parse().unwrap();
        t.publish_set(vec![a, b, c]);
        assert_eq!(reader.current(), vec![a, b, c]);
        // A roster change wholly replaces the prior set (b departed, d joined).
        let d: SocketAddr = "10.0.0.4:4".parse().unwrap();
        t.publish_set(vec![a, c, d]);
        assert_eq!(reader.current(), vec![a, c, d]);
        // Empty set no-ops the beacon (no live secondaries to reach).
        t.publish_set(vec![]);
        assert!(reader.current().is_empty());
    }
}
