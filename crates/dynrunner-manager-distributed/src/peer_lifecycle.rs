//! Peer-lifecycle event types.
//!
//! Why this exists: cluster membership changes (peer joined, peer
//! removed) are an authoritative observation made by the primary and
//! propagated to all peers via the cluster-state CRDT. Downstream
//! consumers (scheduler, telemetry, supervisor) need to react to those
//! transitions without polling the CRDT. The dispatcher task that owns
//! that fan-out is introduced in a later subtask; this module just
//! defines the value types that will flow across its mpsc boundary.
//!
//! The single concern of this module is the *shape* of those events.
//! No emission logic, no consumer logic, no CRDT wiring lives here —
//! those land in subsequent subtasks against this stable type surface.

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
    /// Mass-death finalize after the grace window: the primary's
    /// detector observed simultaneous death of >= N peers and the
    /// grace window elapsed without recovery.
    MassDeathEscalation,
    /// A peer reported a fatal error to the primary; the primary
    /// authored a `PeerRemoved` with this cause.
    ///
    /// The diagnostic string is byte-capped at construction and on
    /// deserialise (see `BoundedString`), so a malicious or buggy
    /// reporter cannot force unbounded allocation on receivers.
    FatalError(BoundedString<1024>),
}

/// Lifecycle event surfaced on the dispatcher mpsc when a
/// `ClusterMutation::PeerRemoved` / `PeerJoined` apply lands.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PeerLifecycleEvent {
    Removed { id: String, cause: RemovalCause },
    Added { id: String, is_observer: bool },
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
