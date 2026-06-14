//! Replicated cluster ledger.
//!
//! Single concern: every node holds a continuously-coherent view of the
//! cluster's task ledger and the current primary identity, maintained by
//! applying CRDT-style mutations broadcast across the mesh.
//!
//! The dispatcher (primary) is the only node that *originates* `TaskAdded`
//! and `TaskAssigned` mutations; every node — primary included — applies
//! every mutation that flows through. `TaskCompleted` / `TaskFailed` are
//! originated by whichever node observes the worker outcome (typically
//! the secondary that owns the worker), and `PrimaryChanged` by the
//! election protocol.
//!
//! Idempotency-by-precondition: each mutation describes the state it
//! applies against, and re-application against the post-state is a
//! `NoOp`. This makes out-of-order delivery and at-least-once delivery
//! safe: terminal states (`Completed` / `Failed`) lock out non-terminal
//! transitions, so a `TaskCompleted` that lands before the matching
//! `TaskAssigned` correctly leaves the entry terminal even when the
//! late `TaskAssigned` arrives next.
//!
//! Asymmetry between the two terminal states: `Completed` is the
//! strongest terminal (success). A `TaskCompleted` superseding a prior
//! `Failed { Recoverable }` is the retry-pass mechanism's normal
//! shape — the same binary is re-injected, re-dispatched, and runs
//! to success. The CRDT must propagate that supersession or the
//! `outcome_counts()` partition stays stuck reporting the retry-
//! succeeded task as `fail_retry`. `Completed` never regresses: a
//! `TaskFailed` against a `Completed` entry is a NoOp (the late
//! failure from a redundant dispatch path can't undo a recorded
//! success). Commutativity is preserved — see `apply`'s TaskCompleted
//! arm doc.

mod accessors;
mod affine;
mod apply;
mod apply_custom;
mod apply_peer;
mod apply_tasks;
mod broadcast;
mod digest;
mod events;
mod grow_max;
mod keyspace;
mod merge;
mod range_digest;
mod settled;
mod snapshot;
mod state;
mod stream;
mod types;

// Re-export the public-facing value types and the `ClusterState`
// struct so external callers continue to see `cluster_state::TaskState`,
// `cluster_state::ClusterState`, etc., at the original paths. Sub-
// module-private items (`PeerState`, `PeerEntry`) stay `pub(super)`
// inside `types`; struct fields are `pub(super)` so sibling sub-modules
// (`apply`, `apply_peer`, `apply_tasks`, `accessors`, `events`,
// `snapshot`, `broadcast`) can read/write them while external callers
// see only the public method surface.
pub(crate) use apply_custom::CustomInboxStats;
pub(crate) use broadcast::{AppliedBatch, apply_locally_for_broadcast};
// Settled-spill surface: the slim index entry + class projection the
// fat-body-free readers consume, the store handed across the promotion
// seam, and the batch/receipt + blocking-write primitives the
// `settled_spill` driver schedules.
pub(crate) use accessors::TaskView;
// `SettledStore` is `pub` because it crosses the wire-less promotion
// seam inside the `pub` `process::PromotionSignal` and the
// `PromotedPrimaryBuilder` signature (the pyo3 recipe threads it
// opaquely). The other settled types stay in-crate (the
// fat-body-free readers + the spill driver are all this crate).
pub use settled::SettledStore;
pub(crate) use settled::{SettledClass, SpillReceipt, write_spill_batch};
pub use snapshot::ClusterStateSnapshot;
pub use state::ClusterState;
// Snapshot-stream partition policy + payload codec: the plan iterates a
// ledger as bounded partial-snapshot packages; the codec is the ONE
// encode/decode pair every responder, receiver, and test uses (pyo3's
// late-joiner bootstrap decodes through it too).
pub use stream::{SnapshotStreamPlan, StreamPackage, decode_stream_payload, encode_stream_payload};
pub use types::{
    ApplyOutcome, CapabilityEntry, CustomMsgState, OutcomeSummary, PeerMembership,
    PeerReadmission, PhaseRollup, PhaseTally, PhaseTaskPartition, ReplicatedRespawnPolicy,
    RespawnEventRecord, RoleChangeHook, StateCounts, TaskState,
};
// `DiscoveryDebt` is the wire-format value type for the discovery-debt
// field; it lives in the protocol crate (it crosses the wire inside
// `StateDigest`, sibling to `SecondaryCapacityRecord`). Re-exported here so
// `cluster_state::DiscoveryDebt` resolves at the original path for callers
// that read it off `ClusterState`.
pub use dynrunner_protocol_primary_secondary::DiscoveryDebt;

// Test seam: the keyspace bucket function, so the range-digest tests assert
// a changed key's bucket without re-deriving the hash-prefix rule.
#[cfg(test)]
pub(crate) use keyspace::range_index_for_test;

#[cfg(test)]
mod tests;
