//! Promoted-secondary primary-role concerns.
//!
//! # Sub-module layout
//!
//! - [`hydrate`] — `populate_primary_from_cluster_state`: rebuild the
//!   `PendingPool` and `primary_in_flight` ledger from the replicated
//!   CRDT state at promotion time.
//! - [`ledger_ops`] — `primary_pending_len`,
//!   `note_primary_item_completed`, `note_primary_item_failed`,
//!   `primary_pending_is_empty`: maintain the in-flight ledger as
//!   items complete or fail. Retry-bucket re-injection is owned by
//!   `lifecycle`'s phase-drain cascade, not by this module.
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
//! - [`lifecycle`] — `process_primary_phase_lifecycle`,
//!   `fire_primary_phase_starts`: phase-lifecycle cascade and
//!   `on_phase_start`/`on_phase_end` fire-sites for the
//!   promoted-secondary path. Mirrors
//!   `PrimaryCoordinator::process_phase_lifecycle` so a setup-promoted
//!   secondary that owns the live pool fires the same hooks the
//!   demoted primary would have.
//! - [`spawn_tasks`] — `apply_spawn_tasks`: promoted-secondary's
//!   analog of `PrimaryCoordinator::apply_spawn_tasks`. Shares the
//!   pre-apply validator with the primary so the duplicate-hash +
//!   unknown-dep rules cannot drift; routes the post-apply state into
//!   `primary_pending` / `primary_failed` so a runtime-injected batch
//!   reaches the live pool on the promoted-secondary path too.
//! - [`fail_permanent`] — `apply_fail_permanent`: promoted-secondary's
//!   analog of `PrimaryCoordinator::apply_fail_permanent`. Drives the
//!   pool cascade primitive against `primary_pending`, records into
//!   `primary_failed`, fires `process_primary_phase_lifecycle`, and
//!   broadcasts `TaskFailed` + cascade-paused `TaskBlocked`s through
//!   the same `apply_and_broadcast_mutations` helper.
//! - [`reinject_task`] — `apply_reinject_task`: promoted-secondary's
//!   analog of `PrimaryCoordinator::apply_reinject_task`. Owns the
//!   secondary-side `unfulfillable_reinject_remaining` counter +
//!   `SecondaryConfig::unfulfillable_reinject_max_per_task` budget;
//!   transitions Unfulfillable → Pending via `primary_pending::reinject`
//!   and broadcasts `TaskReinjected`.
//! - [`update_preferred_secondaries`] —
//!   `apply_update_preferred_secondaries`: promoted-secondary's analog
//!   of the same primary method. Mirrors the new preference list onto
//!   the live `primary_pending` entry via `update_first_match_in_place`
//!   and broadcasts `TaskPreferredSecondariesUpdated`.
//!
//! Free helpers `task_file_hash` and `cascade_drain_done` live in
//! this `mod.rs` because every submodule references them — pulling
//! them into one of the submodules would create back-edges between
//! siblings.

use dynrunner_core::{Identifier, TaskInfo};
use dynrunner_scheduler_api::PendingPool;

mod broadcast;
mod fail_permanent;
mod hydrate;
mod ledger_ops;
mod lifecycle;
mod recovery;
mod reinject_task;
mod spawn_tasks;
mod task_request;
mod update_preferred_secondaries;

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
///
/// `pub(crate)` so the symmetric primary-side hydration
/// (`crate::primary::hydrate`) reuses the identical drain cascade
/// rather than re-deriving the loop — single source of truth for
/// "drain a freshly-seeded pool to quiescence".
pub(crate) fn cascade_drain_done<I: Identifier>(pool: &mut PendingPool<I>) {
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
