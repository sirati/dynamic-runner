//! Wire-level reason carried by `ClusterMutation::PeerRemoved`.
//!
//! Why this lives in the protocol crate (not in the higher-level
//! `dynrunner-manager-distributed::peer_lifecycle` module where the
//! sibling `PeerLifecycleEvent` lives): `RemovalCause` is a field of
//! the wire-level `ClusterMutation::PeerRemoved` variant, so the
//! mutation type and its payload must live in the same crate as the
//! rest of the CRDT vocabulary. Defining a parallel `RemovalCause` in
//! each crate would duplicate the source of truth; lifting
//! `PeerLifecycleEvent` here would drag the dispatcher event-type
//! into the protocol surface where it does not belong. The chosen
//! split keeps the wire payload here and lets `peer_lifecycle`
//! re-export it, preserving the single source of truth.
//!
//! The single concern of this module is the *shape* of the removal
//! reason; emission, telemetry mapping, and dispatcher consumption
//! all live in `dynrunner-manager-distributed`.

use dynrunner_core::BoundedString;

/// Why a peer was removed from the cluster.
///
/// Authored by the primary at the point the corresponding
/// `ClusterMutation::PeerRemoved` is built; receivers treat the cause
/// as opaque metadata for logging / telemetry.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RemovalCause {
    /// Authoritative-detection paths: primary's keepalive watchdog
    /// observed N missed heartbeats from a secondary.
    KeepaliveMiss,
    /// A peer reported a fatal error to the primary; the primary
    /// authored a `PeerRemoved` with this cause.
    ///
    /// The diagnostic string is byte-capped at construction and on
    /// deserialise (see `BoundedString`), so a malicious or buggy
    /// reporter cannot force unbounded allocation on receivers.
    FatalError(BoundedString<1024>),
    /// A node is announcing its own departure from the mesh (it observed
    /// a panik file or per-host SIGTERM and is tearing down its own
    /// workers and exiting locally). Self-authored: the leaving node
    /// authors the `PeerRemoved` for its OWN id so peers LOG the
    /// departure and mark the peer Dead — it is observability-only and
    /// MUST NOT cancel cluster work or terminate the run on peers.
    ///
    /// The payload carries the human-readable reason (e.g.
    /// `"panik file: <path>"` / `"panik SIGTERM (per-host)"`),
    /// byte-capped identically to `FatalError`.
    SelfDeparture(BoundedString<1024>),
    /// Post-mesh roster RE-EMIT of an already-departed id (C6/B5): the
    /// primary's `rebroadcast_full_roster` re-emits a `PeerRemoved` for
    /// each `Departed`-tombstoned id in the `capabilities` 2P-set so a
    /// reconnecting node's LIVENESS view catches up. The original
    /// detection cause is not retained on the tombstone, so this cause
    /// names the re-emit itself — observability-only and idempotent at the
    /// sticky receiver (a node that already buried the id NoOps).
    RosterReemit,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fatal_error_round_trips_via_json() {
        let original = RemovalCause::FatalError(BoundedString::from("disk full"));
        let json = serde_json::to_string(&original).unwrap();
        let parsed: RemovalCause = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, original);
    }

    #[test]
    fn fatal_error_truncates_oversized_input() {
        // BoundedString<1024> caps at 1024 bytes; an input >1024 must be
        // truncated at construction so the variant never carries an
        // oversized payload.
        let long = "a".repeat(4096);
        let cause = RemovalCause::FatalError(BoundedString::from(long));
        match cause {
            RemovalCause::FatalError(s) => {
                assert_eq!(s.as_str().len(), 1024);
                assert!(s.as_str().chars().all(|c| c == 'a'));
            }
            _ => panic!("expected FatalError variant"),
        }
    }
}
