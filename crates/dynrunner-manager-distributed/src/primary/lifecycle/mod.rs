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
//!   dispatch fan-out).
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

#[cfg(test)]
mod tests;

/// Order idle worker indices for a single dispatch tick, biasing
/// toward secondaries with fewer currently-running tasks. Stable
/// tie-break by `worker_id` so equal-loaded secondaries fall through
/// to the existing iteration order.
///
/// Pre-fix the flat `0..workers.len()` scan iterated workers grouped
/// by secondary (the order initial-assignment populates them), giving
/// the first-iterated secondary's idle workers systematic priority
/// when both sides had idle capacity. Tail-of-phase dispatches —
/// where the pool has fewer items than there are idle workers — then
/// concentrated remaining work on the already-loaded secondary
/// instead of spreading across the fleet.
pub(crate) fn dispatch_order<I: Identifier>(workers: &[RemoteWorkerState<I>]) -> Vec<usize> {
    let mut load_per_secondary: HashMap<&str, usize> = HashMap::new();
    for w in workers {
        if !w.is_idle() {
            *load_per_secondary
                .entry(w.secondary_id.as_str())
                .or_default() += 1;
        }
    }
    let mut idle: Vec<usize> = workers
        .iter()
        .enumerate()
        .filter(|(_, w)| w.is_idle())
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
