//! Wire-message handlers for the primary coordinator. Each sub-module
//! owns one `DistributedMessage` family:
//!
//! - [`request`] — `TaskRequest` (worker pull, role-relay path).
//! - [`complete`] — `TaskComplete` and the per-secondary
//!   completion-forwarding helper.
//! - [`failed`] — `TaskFailed` (Recoverable/Unfulfillable/NonRecoverable).
//! - [`affine_deferral`] — the SecondaryAffine local-import gate reports
//!   (#497): `TaskQueuedAfterLocalDependency` (a secondary parked a work
//!   task behind its local import — originate `QueuedAfterLocalDependencySet`
//!   and DROP the parked dependent from `self.in_flight` so the
//!   reconciliation probe stops looping on it) and `LocalDependencyReleased`
//!   (the import finished and the secondary self-dispatched the dependent —
//!   re-originate the EXISTING `TaskAssigned` and re-enter the ledger; NO
//!   re-pushed `TaskAssignment`, the secondary already dispatched it).
//! - [`already_held`] — the duplicate-assignment coherence report
//!   (`TASK_ALREADY_HELD_WIRE_MESSAGE`, recognised at the top of the
//!   `TaskFailed` handler): the holder is ALREADY RUNNING the assigned
//!   hash, so the task stays in flight on it (no requeue, no terminal).
//! - [`illegal_assignment`] — the `IllegallyAssignedToNonidleWorker`
//!   bounce (#517): the secondary refused to run a task on a non-idle
//!   slot and never re-picked. NOT a `TaskFailed` — reconcile the
//!   diverged `(secondary, worker_id)` occupancy + requeue the task.
//! - [`inflight_reconcile`] — the cross-member duplicate dedup (#518):
//!   a falsely-removed-but-alive member's re-admission `InFlightRoster`
//!   names the tasks it is AUTHORITATIVELY running; the primary re-seats
//!   each onto that member and WITHDRAWS the requeued duplicate copy
//!   from whichever OTHER member it was re-dispatched to. The shared
//!   "this member is the authoritative holder → withdraw the duplicate"
//!   primitive is also the route the `already_held` cross-member arm
//!   takes.
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

mod affine_deferral;
mod already_held;
mod complete;
mod failed;
mod illegal_assignment;
mod inflight_reconcile;
mod mutation;
pub(crate) mod predecessor_outputs;
mod request;

pub(in crate::primary) use illegal_assignment::ILLEGAL_ASSIGNMENT_WARN_INTERVAL;
