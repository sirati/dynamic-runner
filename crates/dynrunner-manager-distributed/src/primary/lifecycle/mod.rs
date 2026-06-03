//! Primary-coordinator run-loop concerns. Each sub-module owns one
//! piece of the operational pipeline:
//!
//! - [`mutations`] — `apply_and_broadcast_cluster_mutations`,
//!   `seed_cluster_state`, `send_transfer_complete`. The wire-side
//!   broadcast helpers + Phase-4.5 cluster ledger seed.
//! - [`operational_loop`] — the main `operational_loop`,
//!   `run_retry_passes`, and `drain_pending_messages`. The
//!   long-running `select!` and retry-pass driver are one cohesive
//!   concern; ~800 lines because the loop's per-arm logic can't be
//!   split without leaking borrow-checker state across modules.
//! - [`dispatch`] — `dispatch_to_idle_workers` (the per-tick
//!   dispatch fan-out / worker-management recheck).
//! - [`worker_mgmt`] — the worker-management reaction to the
//!   `worker_signal` bus (`react_to_worker_signal_batch`): the
//!   parked-recheck `TasksAdded` handler plus the
//!   `PhaseStartedNeedsWorkers` liveness check and the `RunShouldFail`
//!   break-outcome. Decoupled from the phase/task code that emits the
//!   signals (the dispatch-decoupling law).
//! - [`promotion`] — `wait_for_mesh_ready` + `activate_local_primary`
//!   (the mesh-settle gate + the single composition mechanism that
//!   activates THIS node's co-located primary as the authority; no
//!   remote role hand-off — see `activate_local_primary`).
//!
//! `dispatch_order` (free fn) lives here because every sub-module
//! consumes it; it has no `&self` so it can't sit on the inherent
//! impl.

use std::collections::HashMap;

use dynrunner_core::Identifier;

use crate::primary::RemoteWorkerState;

mod dispatch;
mod mutations;
mod operational_loop;
mod promotion;
mod worker_mgmt;

/// The bootstrap hand-off outcome the `run_pipeline` fork branches on.
/// Re-exported so the fork (in `primary/coordinator.rs`) can name it
/// without reaching into the private `promotion` sub-module.
pub(crate) use promotion::RelocationOutcome;

#[cfg(test)]
mod tests;

/// Order FREE worker indices for a single dispatch tick, biasing
/// toward secondaries with fewer currently-running tasks. Stable
/// tie-break by `worker_id` so equal-loaded secondaries fall through
/// to the existing iteration order.
///
/// Selection authority vs. advisory load: a worker is a dispatch
/// CANDIDATE iff `held_task().is_none()` — the authoritative free
/// predicate (a slot holds a task or it doesn't). The
/// `(busy-workers-on-secondary)` sort key is ADVISORY load info that
/// reads the `is_idle()` slot-state view; per the dispatch-decoupling
/// law `is_idle` ("we tried to assign and could not") must never be
/// the dispatch gate. (In R1's `SlotState` typestate the two coincide
/// by construction — `is_idle()` IS `held_task().is_none()` — but the
/// predicates are kept distinct so selection authority can never drift
/// onto the advisory name.)
///
/// Pre-fix the flat `0..workers.len()` scan iterated workers grouped
/// by secondary (the order initial-assignment populates them), giving
/// the first-iterated secondary's free workers systematic priority
/// when both sides had capacity. Tail-of-phase dispatches — where the
/// pool has fewer items than there are free workers — then
/// concentrated remaining work on the already-loaded secondary
/// instead of spreading across the fleet.
pub(crate) fn dispatch_order<I: Identifier>(workers: &[RemoteWorkerState<I>]) -> Vec<usize> {
    let mut load_per_secondary: HashMap<&str, usize> = HashMap::new();
    for w in workers {
        // Advisory load count: how busy does this secondary look?
        if !w.is_idle() {
            *load_per_secondary
                .entry(w.secondary_id.as_str())
                .or_default() += 1;
        }
    }
    // Selection authority: dispatch candidates are workers that hold
    // NO task, read off the authoritative free predicate — never the
    // advisory `is_idle` name (dispatch-decoupling law).
    let mut idle: Vec<usize> = workers
        .iter()
        .enumerate()
        .filter(|(_, w)| w.held_task().is_none())
        .map(|(i, _)| i)
        .collect();
    idle.sort_by_key(|&i| {
        (
            load_per_secondary
                .get(workers[i].secondary_id.as_str())
                .copied()
                .unwrap_or(0),
            workers[i].worker_id,
        )
    });
    idle
}
