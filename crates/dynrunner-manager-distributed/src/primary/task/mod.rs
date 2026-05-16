//! Wire-message handlers for the primary coordinator. Each sub-module
//! owns one `DistributedMessage` family:
//!
//! - [`request`] — `TaskRequest` (worker pull, role-relay path).
//! - [`complete`] — `TaskComplete` and the per-secondary
//!   completion-forwarding helper.
//! - [`failed`] — `TaskFailed` (Recoverable/Unfulfillable/NonRecoverable).
//! - [`mutation`] — `ClusterMutation` apply + the CRDT-mirroring
//!   helpers (`mirror_mutation_to_accounting`,
//!   `mirror_tasks_spawned_post_apply`).
//!
//! Every handler is an inherent method on `PrimaryCoordinator`; the
//! sub-files only re-open the impl block with the matching generics.

mod complete;
mod failed;
mod mutation;
mod request;
