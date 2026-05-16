//! Promoted-secondary primary-role concerns.
//!
//! # Sub-module layout
//!
//! - [`hydrate`] — `populate_primary_from_cluster_state`: rebuild the
//!   `PendingPool` and `primary_in_flight` ledger from the replicated
//!   CRDT state at promotion time.
//! - [`ledger_ops`] — `primary_pending_len`,
//!   `note_primary_item_completed`, `note_primary_item_failed`,
//!   `primary_drain_check_and_retry`, `primary_pending_is_empty`:
//!   maintain the in-flight ledger as items complete or fail and
//!   drive Recoverable-retry re-injection.
//! - [`task_request`] — `handle_primary_task_request`: respond to a
//!   peer's `TaskRequest` by picking from the pool and sending a
//!   `TaskAssignment`.
//! - [`recovery`] — `recover_in_flight_to_pool`,
//!   `handle_primary_peer_rejection`,
//!   `clear_primary_peer_backpressure`: undo a dispatch that didn't
//!   reach a worker and apply / clear per-peer backpressure.
//! - [`broadcast`] — `apply_and_broadcast_mutations`,
//!   `ingest_setup_discovery`: originator-side
//!   `apply_locally_for_broadcast` + dual fan-out for batches this
//!   node produces, plus the wrapper entry-point for setup-time
//!   discovery results.
//!
//! Free helpers `task_file_hash` and `cascade_drain_done` live in
//! this `mod.rs` because every submodule references them — pulling
//! them into one of the submodules would create back-edges between
//! siblings.

use dynrunner_core::{Identifier, TaskInfo};
use dynrunner_scheduler_api::PendingPool;

mod broadcast;
mod hydrate;
mod ledger_ops;
mod recovery;
mod task_request;

/// Stable hash of a `TaskInfo`'s path+identifier, matching the wire
/// `file_hash` shape used elsewhere in the secondary. Pulled out as a
/// free function so primary's "drop completed-elsewhere" filter
/// and the assignment path agree on the key space without duplicating
/// the hashing recipe.
pub(super) fn task_file_hash<I: Identifier>(item: &TaskInfo<I>) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    item.path.hash(&mut h);
    item.identifier.hash(&mut h);
    format!("{:016x}", h.finish())
}

/// Run the phase-lifecycle drain cascade on a pool until quiescent.
/// Shared between `populate_primary_from_cluster_state` (catches phases
/// whose only items pre-completed elsewhere) and `note_primary_item_completed`
/// (catches phases whose only items just finished). Each iteration:
///   1. `drain_empty_active_phases` — moves any Active phase whose
///      `(queued, in_flight) == (0, 0)` to Drained, queues it for
///      `poll_drain_transitions`.
///   2. `poll_drain_transitions` — returns and clears the
///      drained-pending list.
///   3. `mark_phase_done` — flips Drained → Done, may unblock
///      dependent phases (Blocked → Active).
///
/// The loop terminates when no new drains surface (the next
/// `drain_empty_active_phases` finds nothing to transition AND
/// `poll_drain_transitions` returns empty).
///
/// Free function (rather than impl method) so the lifecycle test
/// in `tests.rs` can drive it on a hand-built pool without
/// instantiating a full `SecondaryCoordinator` — single concern,
/// single dependency on `&mut PendingPool`.
pub(in crate::secondary) fn cascade_drain_done<I: Identifier>(pool: &mut PendingPool<I>) {
    loop {
        pool.drain_empty_active_phases();
        let drained = pool.poll_drain_transitions();
        if drained.is_empty() {
            break;
        }
        for p in &drained {
            pool.mark_phase_done(p);
        }
    }
}
