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
mod apply;
mod apply_peer;
mod apply_tasks;
mod broadcast;
mod digest;
mod events;
mod grow_max;
mod merge;
mod snapshot;
mod state;
mod types;

// Re-export the public-facing value types and the `ClusterState`
// struct so external callers continue to see `cluster_state::TaskState`,
// `cluster_state::ClusterState`, etc., at the original paths. Sub-
// module-private items (`PeerState`, `PeerEntry`) stay `pub(super)`
// inside `types`; struct fields are `pub(super)` so sibling sub-modules
// (`apply`, `apply_peer`, `apply_tasks`, `accessors`, `events`,
// `snapshot`, `broadcast`) can read/write them while external callers
// see only the public method surface.
pub(crate) use broadcast::{AppliedBatch, apply_locally_for_broadcast};
pub use snapshot::ClusterStateSnapshot;
pub use state::ClusterState;
pub use types::{
    ApplyOutcome, CapabilityEntry, OutcomeSummary, PhaseRollup, PhaseTally, RoleChangeHook,
    StateCounts, TaskState,
};

#[cfg(test)]
mod tests;
