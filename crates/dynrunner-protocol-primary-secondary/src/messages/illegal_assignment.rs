//! Wire descriptor for the `IllegallyAssignedToNonidleWorker` report
//! (#517): the (task-hash, task-id) pair naming one task involved in an
//! illegal-assignment bounce.
//!
//! The secondary bounces this report (NOT a `TaskFailed`) when the
//! primary directs a task at a worker slot that is NOT idle — the
//! secondary must honor the assigned `worker_id` and never silently
//! re-pick another worker (the dispatch-decoupling law: a secondary
//! holds no scheduling authority). The report names BOTH the task the
//! primary illegally assigned AND the task the worker is currently
//! running (the incumbent), so the primary can reconcile its diverged
//! per-(secondary, worker_id) occupancy model and requeue the bounced
//! task without accounting it as a failure.

use serde::{Deserialize, Serialize};

/// One task named in an [`crate::DistributedMessage::IllegallyAssignedToNonidleWorker`]
/// report: its wire hash (the in-flight ledger key the primary tracks
/// it by) plus its consumer-facing `task_id` (the generic identifier
/// `I`, for the operator-facing ERROR log).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssignedTaskRef<I> {
    /// The task's wire hash — equal to the `TaskAssignment.file_hash`
    /// the primary committed into its in-flight ledger.
    pub hash: String,
    /// The task's structured identity — the generic `TaskInfo.identifier`
    /// (`I`), the same identity the rest of the wire carries (e.g.
    /// `DistributedBinaryInfo.identifier`). Named `task_id` for the
    /// operator-facing report; carries the canonical identifier, not the
    /// human-facing `TaskInfo.task_id` string.
    pub task_id: I,
}
