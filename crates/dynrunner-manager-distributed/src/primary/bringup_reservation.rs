//! Bring-up task RESERVATION wiring — the coordinator half of the #494
//! preliminary-allocation fix, opened on the right path by #507.
//!
//! ## Single concern
//!
//! Drive the pending pool's formation-window reservation overlay (owned
//! in `dynrunner-scheduler-api`, see `pending_pool::reservation`) from
//! the coordinator's roster + interleave knowledge. The POOL owns the
//! per-member reservation state and the view-scoping; THIS file owns only
//! the policy inputs the pool can't see:
//!   * WHEN to open it — the cold-bring-up-vs-failover discriminator
//!     (`cluster_state.run_is_unstarted()`);
//!   * the partition itself — which member each initial-pool task is
//!     reserved for, computed via the EXISTING projected-load interleave
//!     (`dispatch_order`), so reservation spread is the same policy as
//!     dispatch spread (one interleave owner), CAPACITY-BOUNDED to one
//!     task per idle worker;
//!   * the load-ordered survivor list a `redistribute_member` folds a
//!     dead member's share onto.
//!
//! ## Why #507: the reservation was dead-on-arrival under mesh-always
//!
//! Under mesh-always the operational primary is ALWAYS a relocate target
//! (`BootstrapRole::PromotedDestination`) whose inherited members hydrate
//! to `Operational` — never `InitialAssigning`. The original #494 gate
//! ("any member `InitialAssigning`") was therefore FALSE on every real
//! run, so the reservation never opened and the #494 pack recurred
//! unmitigated. The fix gates on the inherited LEDGER instead
//! (`run_is_unstarted()` — no post-dispatch task), which is true on a
//! bootstrap-relocation cold target (setup peer relocated before any
//! dispatch) and false on a failover survivor-inherit (mid-run, ≥1
//! dispatched task) — preserving the failover exclusion correctly.
//!
//! ## Why a member's reserved share fixes the pack
//!
//! At cold/staggered bring-up the #382 mesh-veto admits only the
//! first-confirmed members, and the greedy idle-worker recheck pops from
//! the GLOBAL pool with no per-member cap — so the co-located, high-worker
//! node drains the whole pool before the late members confirm (the
//! 14/2/0×N pack). With a reservation, `dispatch_view_for_worker` scopes
//! each member's view to its reserved share; a confirmed member drains
//! only its slice and the still-forming members' slices stay HELD until
//! THEY confirm (their own confirmation-edge `TasksAdded` then dispatches
//! their share) OR their holder is redistributed on death. A reserved task
//! admits ONLY its holder while the window is open — there is NO
//! freed-on-confirm widening, so a confirmed high-worker node can never
//! steal a still-forming member's share. The capacity bound (one reserved
//! task per idle worker) guarantees a holder can always drain its own
//! share, so holder-only admits never strands. Nothing is ever sent to an
//! unconfirmed member — the #382 veto in `should_skip_worker_for_dispatch`
//! STAYS; the reservation only caps what a CONFIRMED member may pull.

use dynrunner_core::Identifier;
use dynrunner_scheduler_api::{ReservationKey, ResourceEstimator, Scheduler};

use super::PrimaryCoordinator;
use super::lifecycle::dispatch_order;

impl<S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<S, E, I> {
    /// OPEN the bring-up reservation: partition the initial pending pool
    /// across the connected fleet (every member in the reconstructed
    /// `self.workers` roster) via the projected-load interleave, ONE
    /// reserved task per idle worker (capacity-bounded), then hand the
    /// per-task → member plan to the pool. Tasks beyond total idle capacity
    /// stay UNRESERVED (free for any member — the steady-state pool).
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
    /// all-confirmed greedy batch. CAPACITY-BOUNDED: the member sequence
    /// holds exactly one entry per idle worker, and each task pairs with a
    /// DISTINCT worker's member (no wrap), so no member is ever reserved
    /// MORE tasks than it has idle workers — a confirmed holder can always
    /// drain its whole share itself, leaving NO over-subscription overflow
    /// for another member to steal (the #507 co-located-high-worker pack)
    /// and NO undrainable share to strand. No-op when the roster is empty
    /// (single-node / no expected members) — the pool then never opens a
    /// window and dispatch is unscoped.
    pub(super) fn seed_bringup_reservation(&mut self, kind: crate::process::BootstrapKind) {
        // COLD-BRING-UP ONLY: a BOOTSTRAP-RELOCATION target with an UNSTARTED
        // ledger.
        //
        // Under mesh-always the operational primary is ALWAYS a
        // `BootstrapRole::PromotedDestination` (the setup peer relocates the
        // role to a compute peer BEFORE `perform_initial_assignment` — this
        // is the SOLE call site of `seed_bringup_reservation`, inside that
        // arm). Its inherited members are hydrated to `Operational` by
        // `reconstruct_secondaries_from_cluster_state` (metadata-only seed),
        // NONE in `InitialAssigning` — so the old "any member
        // `InitialAssigning`" gate was FALSE on every real run and #494's
        // reservation never opened (the #507 dead-on-arrival pack).
        //
        // TWO promotion paths both reach this arm and the CRDT cannot tell
        // them apart:
        //   * a BOOTSTRAP-relocation cold target — the setup peer handed off
        //     a freshly-formed run that has NEVER had an operational primary;
        //     the fleet is forming and will confirm `MeshReady` for the FIRST
        //     time. The reservation MUST open here;
        //   * a FAILOVER survivor-inherit — a survivor inherited a MID-RUN
        //     primary; its inherited pool dispatches through the re-announce
        //     flow, and a pre-partitioned share would WEDGE work whose holder
        //     never re-confirms in lockstep. It must NOT reserve.
        // `run_is_unstarted()` (the CRDT-pure 4-post-dispatch-bucket read)
        // is NOT sufficient ALONE: an EARLY failover whose in-flight tasks all
        // re-queued to `Pending` is ledger-identical to a never-dispatched
        // relocation (both read unstarted). So the AUTHORITATIVE discriminator
        // is the construction-stamped `BootstrapKind` (which the promoting
        // node knows from the `PrimaryChangeReason` — `Transferred` relocate
        // vs `Election` failover — that the CRDT discards); `run_is_unstarted`
        // stays as a corroborating AND-guard, so that if a relocation ever
        // carried a started pool (a bug) the reservation safely declines to
        // open rather than wedging the inherited work.
        if kind != crate::process::BootstrapKind::BootstrapRelocation {
            return;
        }
        if !self.cluster_state.run_is_unstarted() {
            return;
        }

        // The interleaved member sequence: ONE entry per idle worker,
        // mapped to its owning member, in projected-load round-robin order.
        // On the cold all-idle roster this visits every member's worker-0
        // first, then worker-1, etc. — so the i-th entry is a distinct
        // worker, and a member appears exactly as many times as it has idle
        // workers (its capacity bound).
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

        // CAPACITY-BOUNDED partition: pair the i-th queued task with the
        // member of the i-th idle worker — but ONLY for i < idle-worker
        // count. No `% len` wrap: tasks beyond total idle capacity zip out
        // (the `Iterator::zip` stops at the shorter side) and stay
        // UNRESERVED, free for any member via the steady-state pool. This is
        // what makes a member's share <= its idle-worker count, so the
        // reserved-task-admits-ONLY-its-holder rule (no overflow-on-confirm
        // widening) can never strand a genuinely-undrainable overflow — the
        // member always has a worker for every task reserved to it.
        let plan: Vec<(ReservationKey, String)> = task_keys
            .into_iter()
            .zip(member_sequence)
            .collect();

        let members = self.distinct_reservation_members(&plan);
        tracing::info!(
            reserved_tasks = plan.len(),
            members,
            "bring-up reservation opened: partitioned the initial pool across the \
             connected fleet (one task per idle worker) so each member drains only \
             its share while the fleet forms"
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
