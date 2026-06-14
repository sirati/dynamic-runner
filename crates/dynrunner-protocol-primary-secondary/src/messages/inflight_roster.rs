//! Wire descriptor for the `InFlightRoster` report (#518): one task a
//! just-re-admitted member is ACTUALLY running, named by its wire hash
//! plus the secondary-local worker id holding it.
//!
//! A member falsely declared dead keeps running its in-flight tasks while
//! the primary requeues them onto OTHER members (cross-member
//! double-execution). On re-admission the member is the source of truth
//! for what its workers run, so it answers the primary's
//! `RequestInFlightRoster` with these entries, read off its own
//! `active_tasks` bookkeeping. The primary reconciles each entry: a hash
//! it had requeued onto a different member is authoritatively the
//! reporter's, and the duplicate copy is withdrawn (`WithdrawTask`).

use serde::{Deserialize, Serialize};

/// One task named in an [`crate::DistributedMessage::InFlightRoster`]
/// report: its wire hash (the in-flight ledger key the primary tracks it
/// by) plus the secondary-local worker id holding it (for the
/// per-`(secondary, worker_id)` slot re-seat, mirroring the #517 incumbent
/// re-seat). The structured identity (`I`) rides along so the primary can
/// re-seat the ledger entry without re-hashing — the same vocabulary as
/// the #517 [`crate::AssignedTaskRef`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InFlightRosterEntry<I> {
    /// The task's wire hash — equal to the `TaskAssignment.file_hash` the
    /// primary keys its in-flight ledger on.
    pub hash: String,
    /// The secondary-local worker id running the task — the same id
    /// namespace `TaskAssignment.worker_id` uses, so the primary resolves
    /// the stable `(secondary, worker_id)` to a live slot via
    /// `worker_idx_for`.
    pub worker_id: u32,
    /// The task's structured identity — the generic `TaskInfo.identifier`
    /// (`I`), carried so the primary's ledger re-seat preserves the exact
    /// identity without re-hashing the binary.
    pub task_id: I,
}
