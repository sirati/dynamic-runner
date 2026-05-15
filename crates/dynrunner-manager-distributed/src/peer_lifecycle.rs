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
//!
//! [`RemovalCause`] is wire-level payload carried by
//! `ClusterMutation::PeerRemoved` and therefore lives in the protocol
//! crate (`dynrunner_protocol_primary_secondary::removal_cause`); it
//! is re-exported here so consumers that already reach into
//! `peer_lifecycle` for [`PeerLifecycleEvent`] keep a single import
//! path.

pub use dynrunner_protocol_primary_secondary::RemovalCause;

/// Lifecycle event surfaced on the dispatcher mpsc when a
/// `ClusterMutation::PeerRemoved` / `PeerJoined` apply lands.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PeerLifecycleEvent {
    Removed { id: String, cause: RemovalCause },
    Added { id: String, is_observer: bool },
}
