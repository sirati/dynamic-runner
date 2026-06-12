//! Wire-message handlers for the primary coordinator. Each sub-module
//! owns one `DistributedMessage` family:
//!
//! - [`request`] — `TaskRequest` (worker pull, role-relay path).
//! - [`complete`] — `TaskComplete` and the per-secondary
//!   completion-forwarding helper.
//! - [`failed`] — `TaskFailed` (Recoverable/Unfulfillable/NonRecoverable).
//! - [`already_held`] — the duplicate-assignment coherence report
//!   (`TASK_ALREADY_HELD_WIRE_MESSAGE`, recognised at the top of the
//!   `TaskFailed` handler): the holder is ALREADY RUNNING the assigned
//!   hash, so the task stays in flight on it (no requeue, no terminal).
//! - [`mutation`] — `ClusterMutation` apply + the CRDT-mirroring
//!   helpers (`mirror_mutation_to_accounting`,
//!   `mirror_tasks_spawned_post_apply`).
//! - [`predecessor_outputs`] — dispatch-time gathering of
//!   `TaskAssignment.predecessor_outputs` from the replicated
//!   `task_outputs` cache. Pure read over `ClusterState`; called by
//!   both `DistributedMessage::TaskAssignment` construction sites
//!   (`primary/lifecycle/dispatch.rs` and `primary/task/request.rs`)
//!   so the assembled shape is identical regardless of which
//!   dispatch path fires.
//!
//! Every handler is an inherent method on `PrimaryCoordinator`; the
//! sub-files only re-open the impl block with the matching generics.

mod already_held;
mod complete;
mod failed;
mod mutation;
pub(crate) mod predecessor_outputs;
mod request;
