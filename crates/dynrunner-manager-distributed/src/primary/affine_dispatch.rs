//! The dispatch CONSUMER of the per-secondary affine scheduler
//! ([`super::affine_scheduler::AffineScheduler`]) — the operational leaf that
//! makes `TaskKind::SecondaryAffine` work tasks RUN again on the new
//! primary-modeled bitvector.
//!
//! ## The one concern
//! Feed the affine scheduler's per-secondary queues as a DISPATCH SOURCE, on
//! top of the unchanged global-pool dispatch. Three operational seams + one
//! failover seam, each delegating to the pure policy
//! ([`super::affine_scheduler::AffineScheduler`]) and routing the emitted cell
//! mutations through the originate/broadcast choke
//! ([`PrimaryCoordinator::apply_and_broadcast_cluster_mutations`], where the
//! generation is stamped):
//!
//!   * **placement trigger** ([`Self::place_dependency_satisfied_affine_tasks`])
//!     — when an affine-dep WORK task is otherwise ready-to-schedule (its
//!     non-affine deps satisfied AND all its affine prereqs are themselves
//!     ready), select a secondary by rank ([`PrimaryCoordinator::affine_placement_for`]
//!     + [`super::affine_scheduler::AffineScheduler::select_secondary`]) and
//!     [`super::affine_scheduler::AffineScheduler::place`] the work task there
//!     after its still-not-done affine prereqs, emitting the resulting
//!     `SecondaryAffineQueued` mutations. The affine prereq is then taken OUT
//!     of the global pool (it is now owned by the per-secondary queue, exactly
//!     as the removed gate-resolver took a ready gate by hash). Detection
//!     REUSES the pool's already-computed readiness (a `SecondaryAffine` task
//!     in a bucket = its deps are met) — it never re-decides dep resolution.
//!   * **per-secondary-first pop** ([`Self::try_affine_dispatch_for_worker`])
//!     — when assigning to a worker on secondary `S`, pop `S`'s per-secondary
//!     queue FIRST and dispatch the popped unit through the shared
//!     [`PrimaryCoordinator::dispatch_one_assignment`] seam, BEFORE the global
//!     pool view. The popped unit's `TaskInfo` is reconstructed by hash
//!     ([`crate::cluster_state::ClusterState::task_info_for_hash`]) — here it
//!     is explicitly dispatched from the per-secondary queue (the design's
//!     "secondary treats it like any other task"), so the global-queue
//!     `is_worker_assignable` gate that hid the affine def does not apply.
//!   * **idle-steal** ([`Self::try_affine_dispatch_for_worker`], the empty-pool
//!     branch) — when BOTH the global pool view AND `S`'s queue are empty at
//!     assignment time, [`super::affine_scheduler::AffineScheduler::steal_for`]
//!     a whole schedulable unit from the longest-queue donor, emit the
//!     `SecondaryAffineUnqueued`/re-`Queued` mutations, and dispatch the
//!     stolen unit.
//!   * **failover rebuild** is the hydrate seam
//!     ([`PrimaryCoordinator::hydrate_from_cluster_state`] calls
//!     [`super::affine_scheduler::AffineScheduler::clear`] +
//!     `rebuild`), not here.
//!
//! ## Module boundary (CLAUDE.md design-first)
//!   * Owner: the primary. The seams it crosses are (1) the worker-management
//!     reaction's `TasksAdded` branch (placement, the same seam the removed
//!     gate-resolver used) and (2) the per-worker assignment sites
//!     (`dispatch_to_idle_workers` + `handle_task_request`), which call the
//!     ADDITIVE per-secondary-first pop BEFORE their unchanged global-pool
//!     decision.
//!   * The pure policy is consumed, never reimplemented: ranking / append /
//!     pop / steal / rebuild all live in `affine_scheduler`; the cell state +
//!     merge live in `cluster_state::affine_state`; this module only wires the
//!     two together through the typed seams.

use std::collections::HashSet;

use dynrunner_core::{Identifier, TaskInfo};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use super::affine_scheduler::QueuedUnit;
use super::lifecycle::dispatch::DispatchOutcome;
use super::wire::compute_task_hash;
use super::PrimaryCoordinator;
use crate::cluster_state::AffineId;

impl<S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<S, E, I> {
    /// Placement trigger: queue every WORK task whose non-affine deps are
    /// satisfied AND whose affine prereqs are now all ready onto a
    /// rank-selected secondary, dragging in its still-not-done affine prereqs.
    ///
    /// Called from the worker-management `TasksAdded` reaction — the SAME
    /// recheck seam the removed `resolve_dependency_satisfied_affine_gates`
    /// used. A `SecondaryAffine` prereq whose deps just completed was unblocked
    /// `blocked → bucket` by the pool's dep walk (which emits `TasksAdded`), so
    /// it appears in `pool().iter()` (queued, never worker-assignable). That is
    /// the readiness signal — the pool already did the dep resolution; this
    /// reuses it rather than re-scanning the ledger.
    ///
    /// A work task is placed only when ALL its affine prereqs are
    /// ready-in-bucket (the whole schedulable unit is runnable), so a placed
    /// affine `QueuedUnit` is always dispatchable when popped. The work task
    /// itself stays in the global pool (blocked on its affine deps): when the
    /// placed affine prereq RUNS and completes on its secondary, the normal
    /// terminal cascade (`note_item_completed`) unblocks it there, exactly as a
    /// completed work prereq would — the per-secondary queue is a locality
    /// HINT, not a second dependency authority. The affine prereq IS removed
    /// from the pool bucket (it is now owned by the per-secondary queue,
    /// mirroring the old gate-resolver's take-by-hash), so it is never
    /// double-counted nor left as an inert never-dispatchable queued item.
    ///
    /// Idempotent + coalesced: a prereq already `Queued`/`Done` for the chosen
    /// secondary is not re-appended (the `place` policy skips it) and is the
    /// only `SecondaryAffine` pool item this pass takes; re-running with
    /// nothing newly ready is a cheap no-op.
    pub(crate) async fn place_dependency_satisfied_affine_tasks(&mut self) {
        // The candidate secondaries for rank selection: the live roster. An
        // empty roster (no secondary yet) means nothing can be placed.
        let secondaries: Vec<String> = self.affine_placement_secondaries();
        if secondaries.is_empty() {
            return;
        }

        // The set of affine-prereq hashes the pool has marked READY (a
        // `SecondaryAffine` task sitting in a bucket = its own deps are met).
        // `pool().iter()` yields only queued (bucket) items, never blocked
        // ones, so a still-blocked affine prereq is correctly NOT ready.
        let ready_affine: HashSet<String> = self
            .pool()
            .iter()
            .filter(|t| t.kind.is_secondary_affine())
            .map(compute_task_hash)
            .collect();
        if ready_affine.is_empty() {
            return;
        }

        // Find every WORK task whose affine prereqs are ALL in `ready_affine`
        // (the whole schedulable unit is runnable). A work task with no affine
        // dep is never a candidate (it dispatches from the global pool as
        // today). Resolve the candidate's `WorkPlacement` while iterating, so
        // the placement input is built once per candidate.
        let placements: Vec<(String, super::affine_scheduler::WorkPlacement)> = self
            .cluster_state
            .tasks_iter()
            .filter_map(|(_, state)| {
                let def = state.def();
                if def.kind.is_secondary_affine() {
                    return None;
                }
                let task = self.cluster_state.task_to_info(state);
                let placement = self.affine_placement_for(&task);
                if placement.affine_deps.is_empty() {
                    return None;
                }
                // Every affine prereq must be ready-in-bucket: otherwise the
                // unit is not yet wholly runnable, so defer (a later
                // `TasksAdded`, fired when the lagging prereq's deps complete,
                // re-evaluates it).
                let all_ready = placement
                    .affine_deps
                    .iter()
                    .all(|(_, hash)| ready_affine.contains(hash));
                all_ready.then(|| (placement.hash.clone(), placement))
            })
            .collect();

        // Apply each placement once: select a secondary by rank, append the
        // [not-done affine prereqs…, work task] unit to that secondary's queue,
        // and emit the resulting Queued cell mutations. The affine prereq is
        // NOT removed from the global pool — under Model B it stays the pool's
        // ledger TOKEN (uncounted for phase drain; non-worker-assignable so the
        // global view never grabs it) until its FIRST per-secondary run gives
        // it a global terminal; subsequent per-secondary runs are
        // bitvector-only. The work task likewise stays a pool item (ready on
        // its non-affine deps, withheld from the global view by `has_affine_dep`
        // so it dispatches ONLY through the per-secondary queue).
        //
        // Idempotency: a work task is placed AT MOST ONCE (the `place` policy
        // always appends, so a second placement would double-queue). The
        // scheduler's `placed_work` guard records placed work hashes (co-located
        // with the queues it guards; reset together with them by the failover
        // rebuild). A work task placed once runs its whole unit on its chosen
        // secondary; another dependent of the same affine prereq routed to a
        // DIFFERENT secondary re-appends the prereq to THAT secondary's queue
        // (the per-secondary re-run), which `place` does because that
        // secondary's cell is still NotDone.
        for (work_hash, placement) in placements {
            if !self.affine_scheduler.record_placed_work(&work_hash) {
                continue;
            }
            let Some(sec) = self.affine_scheduler.select_secondary(
                &secondaries,
                &placement,
                |s: &str, a: AffineId| self.cluster_state.affine_state(s, a),
            ) else {
                self.affine_scheduler.unrecord_placed_work(&work_hash);
                continue;
            };
            let mutations = self.affine_scheduler.place::<I, _>(
                &sec,
                &placement,
                |s: &str, a: AffineId| self.cluster_state.affine_state(s, a),
            );
            if !mutations.is_empty() {
                self.apply_and_broadcast_cluster_mutations(mutations).await;
            }
        }
    }

    /// Per-secondary-FIRST pop for the idle worker at `worker_idx`: pop the
    /// worker's secondary's affine queue and dispatch the popped unit, BEFORE
    /// the caller's global-pool decision. Returns `true` iff a unit was
    /// committed (the caller skips its global path for this worker this tick);
    /// `false` leaves the worker for the unchanged global dispatch (empty
    /// queue, or a stale/uncommittable unit — harmless under the HINT
    /// property). The caller invokes this with NO live pool borrow (it precedes
    /// view construction), so the design's "per-secondary queue FIRST, then the
    /// global queue" ordering holds.
    pub(crate) async fn try_affine_pop_for_worker(&mut self, worker_idx: usize) -> bool {
        let secondary = self.workers[worker_idx].secondary_id.clone();
        let Some(unit) = self.affine_scheduler.pop_next(&secondary) else {
            return false;
        };
        self.dispatch_affine_unit(worker_idx, &secondary, unit).await
    }

    /// Idle-steal for the idle worker at `worker_idx`: the caller's
    /// precondition is that BOTH the global pool view AND this worker's
    /// secondary's queue are empty (the design's idle-steal trigger). Steal a
    /// whole schedulable unit from the longest-queue donor — emitting the
    /// `SecondaryAffineUnqueued`/re-`Queued` cell mutations — then pop +
    /// dispatch it. Returns `true` iff a stolen unit was committed; `false`
    /// when no donor had work to steal (the worker stays idle this tick).
    pub(crate) async fn try_affine_steal_for_worker(&mut self, worker_idx: usize) -> bool {
        let secondary = self.workers[worker_idx].secondary_id.clone();
        let mutations = self.affine_scheduler.steal_for::<I, _>(
            &secondary,
            |s: &str, a: AffineId| self.cluster_state.affine_state(s, a),
        );
        if !mutations.is_empty() {
            self.apply_and_broadcast_cluster_mutations(mutations).await;
        }
        let Some(unit) = self.affine_scheduler.pop_next(&secondary) else {
            return false;
        };
        self.dispatch_affine_unit(worker_idx, &secondary, unit).await
    }

    /// Reconstruct a popped [`QueuedUnit`]'s worker-dispatchable `TaskInfo` by
    /// hash and dispatch it through the shared
    /// [`PrimaryCoordinator::dispatch_one_assignment`] seam. On a non-committed
    /// outcome the unit's binary came from the per-secondary queue (NOT the
    /// pool), so it is re-pushed to the FRONT of `secondary`'s queue
    /// ([`super::affine_scheduler::AffineScheduler::requeue_front`]) to be
    /// retried before any other queued unit. Returns `true` iff committed.
    async fn dispatch_affine_unit(
        &mut self,
        worker_idx: usize,
        secondary: &str,
        unit: QueuedUnit,
    ) -> bool {
        let (hash, is_work) = match &unit {
            QueuedUnit::Affine { hash, .. } => (hash.clone(), false),
            QueuedUnit::Work { hash } => (hash.clone(), true),
        };
        let Some(task) = self.cluster_state.task_info_for_hash(&hash) else {
            // Not in the fat ledger (settled / gone): the unit is stale. Drop
            // it (HINT property — a fresh placement re-derives a still-needed
            // prereq) and fall through to the global path.
            return false;
        };
        let phase = task.phase_id.clone();

        // POOL ACCOUNTING (Model B). A WORK unit is a pool item (the
        // phase-drain token): take it OUT of its bucket + `mark_in_flight` so
        // the phase accounting goes queued→in_flight exactly as a global
        // dispatch's `take_selected` would (its terminal then runs
        // `note_item_completed`, draining the phase). Without this the work task
        // lingers in its bucket forever (queued_count > 0 ⇒ the phase never
        // drains ⇒ the run hangs). An AFFINE unit is UNcounted (the import is
        // not phase-completion work): no pool take, no in_flight bump — it is
        // dispatched purely by-hash and its terminal is phase-neutral
        // (`handle_affine_task_complete`).
        if is_work {
            let target = hash.clone();
            if self
                .pool_mut()
                .take_first_match(|t| compute_task_hash(t) == target)
                .is_none()
            {
                // The work item is not in a dispatchable bucket (already taken /
                // its phase not Active) — the per-secondary queue claim is
                // stale. Drop the unit and fall through to the global path
                // (HINT property — a fresh placement re-derives it).
                return false;
            }
            self.pool_mut().mark_in_flight(&phase);
        }

        let estimated = self.estimator.estimate(&task);
        match self
            .dispatch_one_assignment(worker_idx, std::sync::Arc::new(task), estimated)
            .await
        {
            DispatchOutcome::Committed => true,
            DispatchOutcome::CommitRefused(binary) | DispatchOutcome::SendFailed(binary) => {
                // Re-push the unit to the FRONT of S's queue so it is retried.
                // For a WORK unit, also undo the pool take + in_flight bump
                // (requeue restores the bucket item AND decrements in_flight),
                // keeping the phase accounting balanced.
                if is_work {
                    self.pool_mut().requeue(binary);
                }
                self.affine_scheduler.requeue_front(secondary, unit);
                false
            }
        }
    }

    /// The candidate secondaries for affine rank selection: the live roster
    /// (every secondary with ≥1 worker slot). Name-deduplicated + sorted for
    /// the deterministic tie-break `select_secondary` / `rebuild` rely on.
    fn affine_placement_secondaries(&self) -> Vec<String> {
        let mut secs: Vec<String> = self
            .workers
            .iter()
            .map(|w| w.secondary_id.clone())
            .collect();
        secs.sort();
        secs.dedup();
        secs
    }

    /// Build the rank-selection placement list for the failover rebuild: the
    /// [`super::affine_scheduler::WorkPlacement`] of every WORK task with ≥1
    /// affine dep, in a DETERMINISTIC (hash-sorted) order so the rebuild
    /// reproduces the same per-secondary layout on every promoted primary
    /// (the bitvector — incl. `Queued` — is replicated, so re-placing against
    /// it lands each unit on the same secondary up to the deterministic
    /// tie-break). Consumed by [`Self::rebuild_affine_schedule`].
    pub(crate) fn affine_rebuild_placements(&self) -> Vec<super::affine_scheduler::WorkPlacement> {
        let mut placements: Vec<super::affine_scheduler::WorkPlacement> = self
            .cluster_state
            .tasks_iter()
            .filter_map(|(_, state)| {
                let def = state.def();
                if def.kind.is_secondary_affine() {
                    return None;
                }
                let task: TaskInfo<I> = self.cluster_state.task_to_info(state);
                let placement = self.affine_placement_for(&task);
                (!placement.affine_deps.is_empty()).then_some(placement)
            })
            .collect();
        placements.sort_by(|a, b| a.hash.cmp(&b.hash));
        placements
    }

    /// Failover rebuild: discard the local per-secondary queues and re-run
    /// rank placement over the pending work pool against the inherited
    /// (replicated) bitvector. The promoted-primary seam — the queues are
    /// LOCAL (not replicated), so a promotion re-derives them from the
    /// replicated bitvector + the work pool, reproducing the per-secondary
    /// layout up to the deterministic tie-break (the bitvector, incl. `Queued`,
    /// IS replicated, so re-placing lands each unit on the same secondary).
    ///
    /// Called by [`PrimaryCoordinator::hydrate_from_cluster_state`] AFTER the
    /// pool + roster are reconstructed. Hydrate is a SYNC constructor-time
    /// derived-cache rebuild (it runs in `seed_from_promotion_snapshot` BEFORE
    /// the run loop + dispatchers start, so no async broadcast transport
    /// exists), so the rebuild's emitted cell mutations are applied LOCALLY
    /// through the same stamping choke the async originator uses
    /// ([`crate::cluster_state::apply_locally_for_broadcast`], which stamps the
    /// per-cell LWW generation), NOT broadcast. The bitvector is replicated, so
    /// a re-claim against an already-`Queued` inherited cell emits NOTHING
    /// (`place` skips it); a NotDone→Queued re-claim is applied locally and the
    /// promoted primary's subsequent anti-entropy + live origination propagate
    /// it — the LWW generation keeps convergence intact across the epoch.
    pub(crate) fn rebuild_affine_schedule(&mut self) {
        // `clear` drops both the queues AND the placement guard, so the rebuild
        // re-derives placements from scratch against the inherited bitvector (a
        // placement whose `Queued` claim survived in the replicated bitvector
        // re-appends nothing — `place` skips an already-queued cell — but the
        // work-task append must be allowed to re-run).
        self.affine_scheduler.clear();
        let secondaries = self.affine_placement_secondaries();
        if secondaries.is_empty() {
            return;
        }
        let placements = self.affine_rebuild_placements();
        // Seed the idempotency guard with every work hash the rebuild places,
        // so the next LIVE placement trigger does not re-queue an already-
        // rebuilt work task (the rebuild placed the whole pending affine-dep
        // pool; `rebuild` calls `place` for each, bypassing the per-call
        // guard).
        for placement in &placements {
            self.affine_scheduler.record_placed_work(&placement.hash);
        }
        let mutations = self.affine_scheduler.rebuild::<I, _>(
            &secondaries,
            &placements,
            |s: &str, a: AffineId| self.cluster_state.affine_state(s, a),
        );
        if !mutations.is_empty() {
            crate::cluster_state::apply_locally_for_broadcast(&mut self.cluster_state, mutations);
        }
    }
}
