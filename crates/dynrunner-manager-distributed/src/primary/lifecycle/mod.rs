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
//!   (the mesh-settle gate + the single mechanism that activates THIS
//!   node as the primary authority — see `activate_local_primary`).
//! - [`graceful_abort`] — the primary side of the observer-requested
//!   graceful abort: the request→latch origination, the per-iteration
//!   drain/relocate/terminal decision, and the respawn-admission gate
//!   predicate. The dispatch FREEZE itself lives at the one dispatch-view
//!   seam (`PrimaryCoordinator::dispatch_view_for_worker`).
//!
//! `dispatch_order` (free fn) lives here because every sub-module
//! consumes it; it has no `&self` so it can't sit on the inherent
//! impl.

use std::collections::HashMap;

use dynrunner_core::Identifier;

use crate::primary::RemoteWorkerState;

mod dispatch;
mod graceful_abort;
mod mutations;
mod operational_loop;
mod promotion;
mod worker_mgmt;

pub(crate) use mutations::TerminalVerdict;
pub(crate) use promotion::RelocationPolicy;

#[cfg(test)]
mod tests;

/// Order FREE worker indices for a single dispatch tick so that
/// consecutive grants INTERLEAVE across secondaries instead of
/// exhausting one secondary's workers before touching the next.
///
/// This is the SOLE owner of the dispatch-target ordering policy: the
/// batched initial assignment (`perform_initial_assignment`) and the
/// idle-worker recheck (`dispatch_to_idle_workers`) both consume it,
/// so spread-across-secondaries can never diverge between the two
/// batch paths. (The demand path, `handle_task_request`, performs no
/// selection — the requesting worker IS the target.)
///
/// Sort key: `(projected_load, worker_id)` where `projected_load` =
/// the secondary's busy-worker count at tick start PLUS this worker's
/// rank within its own secondary's free list. A whole-batch consumer
/// that grants down the returned order therefore behaves exactly like
/// a greedy "always pick the secondary with the lowest projected
/// in-flight count" loop — least-loaded-secondary-first ROUND-ROBIN —
/// without recomputing loads per grant. Stable tie-break by
/// `worker_id` keeps equal-wave ordering deterministic.
///
/// Selection authority vs. advisory load: a worker is a dispatch
/// CANDIDATE iff `held_task().is_none()` — the authoritative free
/// predicate (a slot holds a task or it doesn't). The
/// `(busy-workers-on-secondary)` load component is ADVISORY info that
/// reads the `is_idle()` slot-state view; per the dispatch-decoupling
/// law `is_idle` ("we tried to assign and could not") must never be
/// the dispatch gate. (In R1's `SlotState` typestate the two coincide
/// by construction — `is_idle()` IS `held_task().is_none()` — but the
/// predicates are kept distinct so selection authority can never drift
/// onto the advisory name.)
///
/// Two pre-fix generations of this ordering both packed:
/// * the flat `0..workers.len()` scan gave the first-iterated
///   secondary's free workers systematic priority (tail-of-phase
///   concentration);
/// * the `(static_load, worker_id)` sort that replaced it still
///   grouped WHOLE secondaries — the load key never advanced as
///   grants committed within the tick, so the least-loaded
///   secondary's entire free set drained before the next secondary
///   saw a single task (production capture: one secondary's 14
///   workers filled to capacity, eleven idle secondaries at zero).
///   It also left the equal-load case's spread an accident of the
///   roster's interleaved `worker_id` layout rather than a property
///   of the policy.
///
/// The per-worker rank component fixes both: ordering interleaves by
/// construction, independent of roster layout and of load skew.
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
    // Rank free workers within their own secondary in `worker_id`
    // order, so the projected-load key below is deterministic even if
    // the roster Vec is not id-sorted.
    idle.sort_by_key(|&i| workers[i].worker_id);
    let mut rank_within_secondary: HashMap<&str, usize> = HashMap::new();
    let mut keyed: Vec<(usize, u32, usize)> = idle
        .into_iter()
        .map(|i| {
            let secondary = workers[i].secondary_id.as_str();
            let rank = rank_within_secondary.entry(secondary).or_default();
            let projected_load = load_per_secondary.get(secondary).copied().unwrap_or(0) + *rank;
            *rank += 1;
            (projected_load, workers[i].worker_id, i)
        })
        .collect();
    keyed.sort_unstable();
    keyed.into_iter().map(|(_, _, i)| i).collect()
}
