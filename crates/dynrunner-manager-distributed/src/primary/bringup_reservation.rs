//! Bring-up task RESERVATION wiring — the coordinator half of the #494
//! preliminary-allocation fix.
//!
//! ## Single concern
//!
//! Drive the pending pool's formation-window reservation overlay (owned
//! in `dynrunner-scheduler-api`, see `pending_pool::reservation`) from
//! the coordinator's roster + interleave knowledge. The POOL owns the
//! per-member reservation state and the view-scoping; THIS file owns only
//! the policy inputs the pool can't see:
//!   * the partition itself — which member each initial-pool task is
//!     reserved for, computed via the EXISTING projected-load interleave
//!     (`dispatch_order`), so reservation spread is the same policy as
//!     dispatch spread (one interleave owner);
//!   * the load-ordered survivor list a `redistribute_member` folds a
//!     dead member's share onto.
//!
//! ## Why a member's reserved share fixes the #494 pack
//!
//! At cold/staggered bring-up the #382 mesh-veto admits only the
//! first-confirmed members, and the greedy idle-worker recheck pops from
//! the GLOBAL pool with no per-member cap — so sec-0/sec-7's idle slots
//! drain all 28 tasks before the other 13 confirm 70-90s later (the
//! 14/14/0×13 pack). With a reservation, `dispatch_view_for_worker`
//! scopes each member's view to its ~2 reserved tasks; a confirmed member
//! drains only its slice and the late confirmers' slices stay held until
//! THEY confirm (their own confirmation-edge `TasksAdded` then dispatches
//! their share). Nothing is ever sent to an unconfirmed member — the #382
//! veto in `should_skip_worker_for_dispatch` STAYS; the reservation only
//! caps what a CONFIRMED member may pull.

use dynrunner_core::Identifier;
use dynrunner_scheduler_api::{ReservationKey, ResourceEstimator, Scheduler};

use crate::state::SecondaryConnectionState;

use super::PrimaryCoordinator;
use super::lifecycle::dispatch_order;

impl<S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<S, E, I> {
    /// OPEN the bring-up reservation: partition the initial pending pool
    /// across the FULL EXPECTED member set (every member in the
    /// reconstructed `self.workers` roster — `live_known_secondaries`,
    /// confirmed or not) via the projected-load interleave, then hand the
    /// per-task → member plan to the pool.
    ///
    /// Runs ONCE, in the bring-up pre-loop chain AFTER
    /// `reconstruct_workers_from_cluster_state` (needs the roster) and
    /// BEFORE `perform_initial_assignment` (so even the initial batch is
    /// scoped to each member's reserved share — the batch's
    /// `view_for_worker` rides the same `dispatch_view_for_worker` seam).
    ///
    /// The interleave reuse: `dispatch_order(&self.workers)` is the SOLE
    /// dispatch-target ordering policy (least-projected-load round-robin
    /// across members). Walking it yields a member sequence already
    /// spread across the fleet; reserving the i-th queued task to the
    /// member of the i-th worker in that order therefore partitions the
    /// pool by the SAME interleave that governs dispatch — so a member's
    /// reserved share matches the share it would have won in an
    /// all-confirmed greedy batch (~ceil(tasks/members) each). No-op when
    /// the roster is empty (single-node / no expected members) — the pool
    /// then never opens a window and dispatch is unscoped.
    pub(super) fn seed_bringup_reservation(&mut self) {
        // COLD-BRING-UP ONLY. The reservation is the FORMATION-window
        // mechanism: it caps each member to its share while members
        // transition from `InitialAssigning` (parked awaiting the initial
        // batch) to operational and confirm `MeshReady` for the first
        // time. A FAILOVER-PROMOTED primary inherits a pool whose members
        // are already `Operational` survivors (reconstructed metadata-only
        // by `reconstruct_secondaries_from_cluster_state`, NONE in
        // `InitialAssigning`); its inherited tasks dispatch through the
        // re-announce flow (`injected_spread`), which a reservation would
        // WEDGE — the survivors' `MeshReady` re-announces drive
        // confirmation, but holding each member to a pre-partitioned share
        // of the inherited pool strands work whose holder never re-confirms
        // in lockstep. So gate on the SAME "provably parked awaiting the
        // batch" fact `perform_initial_assignment`'s `batch_eligible` uses:
        // a member in `InitialAssigning`. No such member ⇒ this is not a
        // cold bring-up ⇒ no reservation.
        let is_cold_bringup = self
            .secondaries
            .values()
            .any(|s| matches!(s, SecondaryConnectionState::InitialAssigning(_)));
        if !is_cold_bringup {
            return;
        }

        // The interleaved member sequence: each idle worker mapped to its
        // owning member, in projected-load round-robin order. On the cold
        // all-idle roster this visits every member's worker-0 first, then
        // worker-1, etc. — so the first `member_count` entries are the
        // distinct members (one each), and the share grows round-robin
        // from there.
        let order = dispatch_order(&self.workers);
        if order.is_empty() {
            return;
        }
        let member_sequence: Vec<String> = order
            .into_iter()
            .map(|idx| self.workers[idx].secondary_id.clone())
            .collect();

        // The initial pool's task identities, in the pool's deterministic
        // queued-iteration order. Collected up front so the immutable pool
        // borrow is released before `open_reservation`'s mutable borrow.
        let task_keys: Vec<ReservationKey> = self
            .pool()
            .iter()
            .map(|t| (t.phase_id.clone(), t.task_id.clone()))
            .collect();
        if task_keys.is_empty() {
            return;
        }

        // Pair the i-th queued task with the member of the i-th worker in
        // the interleave (cycling the member sequence if there are more
        // tasks than idle workers — the share then wraps round-robin,
        // still spread across the same members).
        let plan: Vec<(ReservationKey, String)> = task_keys
            .into_iter()
            .enumerate()
            .map(|(i, key)| (key, member_sequence[i % member_sequence.len()].clone()))
            .collect();

        let members = self.distinct_reservation_members(&plan);
        tracing::info!(
            reserved_tasks = plan.len(),
            members,
            "bring-up reservation opened: partitioned the initial pool across the \
             expected member set so each member drains only its share while the \
             fleet forms"
        );
        self.pool_mut().open_reservation(plan);
    }

    /// REDISTRIBUTE a dead member's reserved share onto the surviving
    /// fleet. Called from the genuine member-removal path
    /// (`requeue_dead_secondary`) AFTER the dead member's workers are
    /// retained out of `self.workers`, so the survivor order below
    /// excludes it. NOT called at the mesh-ready proceed-deadline: a
    /// slow-to-form member keeps its share HELD to claim late, and
    /// redistributing at the deadline would re-clump it onto the
    /// already-confirmed members.
    ///
    /// The fallback order is the load-aware survivor list (the interleave
    /// applied to the post-removal roster), so the dead member's tasks
    /// fold round-robin onto the least-loaded survivors first — the same
    /// spread policy as initial reservation. No-op when no reservation
    /// window is open (steady-state death needs no redistribute).
    pub(super) fn redistribute_reservation_for_dead_member(&mut self, dead: &str) {
        if !self.pool().reservation_active() {
            return;
        }
        let fallbacks = self.reservation_fallback_members();
        self.pool_mut().redistribute_member(dead, &fallbacks);
    }

    /// The load-ordered list of surviving members for a redistribute
    /// fold: walk the interleave over the current roster and dedup
    /// member ids in first-seen (least-projected-load-first) order. The
    /// dead member is already gone from `self.workers` by the time this
    /// runs, so it cannot appear.
    fn reservation_fallback_members(&self) -> Vec<String> {
        let mut seen = std::collections::HashSet::new();
        let mut ordered = Vec::new();
        for idx in dispatch_order(&self.workers) {
            let id = &self.workers[idx].secondary_id;
            if seen.insert(id.clone()) {
                ordered.push(id.clone());
            }
        }
        ordered
    }

    /// Count of distinct members a reservation plan spreads across — for
    /// the bring-up INFO emit only.
    fn distinct_reservation_members(&self, plan: &[(ReservationKey, String)]) -> usize {
        plan.iter()
            .map(|(_, m)| m.as_str())
            .collect::<std::collections::HashSet<_>>()
            .len()
    }
}
