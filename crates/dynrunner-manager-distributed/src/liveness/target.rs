//! [`BeaconTarget`] — the cloneable, runtime-published address the
//! secondary's liveness beacon thread sends to.
//!
//! # Concern
//!
//! The beacon runs on a dedicated OS thread that the worker's CPU-bound
//! build can NOT starve (that is the whole point). It therefore cannot
//! borrow the coordinator's `cluster_state` / mesh to learn WHO the
//! current primary is or WHERE its liveness socket lives — those live on
//! the tokio runtime the beacon exists to be independent of. This cell is
//! the ONE bridge: the secondary's runtime WRITES the current primary's
//! liveness `SocketAddr` (on `PrimaryChanged`, from the primary's
//! advertised [`dynrunner_protocol_primary_secondary::PeerConnectionInfo`]
//! `liveness_port` + `ipv4`), and the beacon thread READS it each tick.
//!
//! # Why a published cell, not a query
//!
//! Mirrors [`crate::process::MembershipView`]: a shared `Arc<Mutex<_>>`
//! the runtime publishes and a detached reader consumes, never a delta.
//! The beacon re-reads every tick, so a failover (`PrimaryChanged`) that
//! republishes a new target is picked up on the next beacon without the
//! beacon knowing anything about elections — it just sends to whatever
//! address is currently published.
//!
//! # Staleness during starvation
//!
//! When the runtime is CPU-starved it cannot republish — but the LAST
//! published target stays valid: a starved secondary's primary has not
//! changed (a failover would require this node's runtime to participate,
//! which a starved node cannot, so it is not electing a new primary while
//! starved). The beacon keeps sending to the last-known-good target,
//! which is exactly the node it should be reassuring. `None` (no primary
//! resolved yet) makes the beacon a no-op for that tick.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

/// A cloneable handle to the current primary's liveness address.
///
/// Every clone shares one cell. The secondary's runtime holds the write
/// side ([`BeaconTarget::publish`]); the beacon thread holds a clone for
/// [`BeaconTarget::current`].
#[derive(Clone, Default)]
pub struct BeaconTarget {
    inner: Arc<Mutex<Option<SocketAddr>>>,
}

impl BeaconTarget {
    /// A fresh target with no primary resolved yet (the beacon no-ops
    /// until the first [`BeaconTarget::publish`]).
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(None)),
        }
    }

    /// Publish the current primary's liveness `SocketAddr`. Called by the
    /// secondary's runtime whenever the resolved primary (or its
    /// advertised liveness address) changes. `None` clears the target
    /// (e.g. the primary departed and no successor is resolved yet).
    pub fn publish(&self, addr: Option<SocketAddr>) {
        *self.inner.lock().expect("beacon target poisoned") = addr;
    }

    /// The last-published target, read by the beacon thread each tick.
    pub fn current(&self) -> Option<SocketAddr> {
        *self.inner.lock().expect("beacon target poisoned")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_observed_by_clones() {
        let t = BeaconTarget::new();
        assert_eq!(t.current(), None);
        let reader = t.clone();
        let addr: SocketAddr = "10.0.0.1:9999".parse().unwrap();
        t.publish(Some(addr));
        assert_eq!(reader.current(), Some(addr));
        // A failover republish wholly replaces the prior target.
        let addr2: SocketAddr = "10.0.0.2:8888".parse().unwrap();
        t.publish(Some(addr2));
        assert_eq!(reader.current(), Some(addr2));
        // Clearing it (primary departed, no successor) no-ops the beacon.
        t.publish(None);
        assert_eq!(reader.current(), None);
    }
}
