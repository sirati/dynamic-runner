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
//! - PLACEMENT trigger ([`Self::place_dependency_satisfied_affine_tasks`]):
//!   when an affine-dep WORK task is otherwise ready-to-schedule (its non-affine
//!   deps satisfied AND all its affine prereqs themselves ready), select a
//!   secondary by rank ([`PrimaryCoordinator::affine_placement_for`] +
//!   `AffineScheduler::select_secondary`) and `AffineScheduler::place` the work
//!   task there after its still-not-done affine prereqs, emitting the resulting
//!   `SecondaryCellQueued` mutations. Detection REUSES the pool's
//!   already-computed readiness (a `SecondaryAffine` task in a bucket = its deps
//!   are met) — it never re-decides dep resolution.
//! - PER-SECONDARY-FIRST pop ([`Self::try_affine_pop_for_worker`]): when
//!   assigning to a worker on secondary `S`, pop `S`'s per-secondary queue FIRST
//!   and dispatch the popped unit through the shared
//!   [`PrimaryCoordinator::dispatch_one_assignment`] seam, BEFORE the global
//!   pool view. The popped unit's `TaskInfo` is reconstructed by hash
//!   ([`crate::cluster_state::ClusterState::task_info_for_hash`]); a popped WORK
//!   unit is gated on EVERY affine dep being `Done` on `S` (the bitvector is the
//!   readiness authority) and re-derived if stranded without its import.
//! - IDLE-STEAL ([`Self::try_affine_steal_for_worker`]): when BOTH the global
//!   pool view AND `S`'s queue are empty at assignment time,
//!   `AffineScheduler::steal_for` a whole schedulable unit from the
//!   longest-queue donor, emit the `SecondaryCellUnqueued`/re-`Queued`
//!   mutations, and dispatch the stolen unit.
//! - FAILOVER rebuild ([`Self::rebuild_affine_schedule`], called from
//!   [`PrimaryCoordinator::hydrate_from_cluster_state`]): discard the local
//!   queues and re-derive them from the inherited bitvector + work pool.
//!
//! ## Module boundary (CLAUDE.md design-first)
//!
//! Owner: the primary. The seams it crosses are (1) the worker-management
//! reaction's `TasksAdded` branch (placement, the same seam the removed
//! gate-resolver used) and (2) the per-worker assignment sites
//! (`dispatch_to_idle_workers` + `handle_task_request`), which call the ADDITIVE
//! per-secondary-first pop BEFORE their unchanged global-pool decision. The pure
//! policy is consumed, never reimplemented: ranking / append / pop / steal /
//! rebuild all live in `affine_scheduler`; the cell state + merge live in
//! `cluster_state::affine_state`; this module only wires the two together.

use std::collections::HashSet;

use dynrunner_core::{Identifier, TaskInfo};
use dynrunner_protocol_primary_secondary::{SecondaryCell, ClusterMutation};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use super::affine_scheduler::{QueuedUnit, WorkPlacement};
use super::lifecycle::dispatch::DispatchOutcome;
use super::wire::compute_task_hash;
use super::PrimaryCoordinator;
use crate::cluster_state::SecondaryCellId;

/// The per-secondary affine-readiness verdict for a popped WORK unit, derived
/// from its affine deps' bitvector cells on the dispatching secondary (the
/// readiness authority — never a global terminal). Discriminates the cell
/// states so the gate acts correctly per shape instead of blindly re-placing
/// on any non-`Done` (which storms on an in-flight import and livelocks on a
/// failed one). Computed by [`PrimaryCoordinator::affine_readiness_gate`].
enum AffineGateOutcome {
    /// Every affine dep is `Done` on this secondary — dispatch now.
    Ready,
    /// A dep is `Queued` (the import is IN FLIGHT here), none `NotDone`/`Failed`
    /// — leave the work at the queue front and let the import's terminal
    /// re-nudge (no re-place, no re-emit: re-placing here would storm).
    InFlightHere,
    /// A dep is `NotDone` here (and none `Failed`) — the import was never
    /// placed/run here: re-derive the prereq prefix + work onto THIS secondary.
    StrandedHere,
    /// A dep is `Failed` here, but the named OTHER secondary can still satisfy
    /// every dep (none `Failed` there) — re-route the unit onto it. A `Failed`
    /// cell is non-sticky, so the re-route recovers.
    Reroute(String),
    /// A dep is `Failed` on EVERY eligible secondary — the import cannot run
    /// anywhere: terminal-fail the dependent (cascade) rather than spin.
    Unsatisfiable,
}

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
    /// itself stays in the global pool, ready on its non-affine deps but
    /// withheld from the global worker view by `has_affine_dep` (the affine
    /// deps are excluded from its pool-blocking set under Model B): it
    /// dispatches ONLY through the per-secondary queue, whose per-secondary
    /// bitvector readiness gate is the authority — the per-secondary queue is
    /// a locality HINT, not a second dependency authority.
    ///
    /// The affine prereq STAYS in the pool bucket (it is NOT taken out): under
    /// Model B it is the pool's placement-readiness SIGNAL + ledger token. It
    /// is non-worker-assignable (the global worker view never grabs it) and
    /// EXCLUDED from the phase-drain count (`counts_for_phase_drain` —
    /// `pool::queued_count`), so it neither double-counts nor wedges the
    /// drain; its per-secondary runs are driven off-queue by the affine
    /// scheduler + bitvector. The token is dropped at the phase's
    /// `Drained → Done` edge (`pool::mark_phase_done → drop_affine_items`), so
    /// a drained phase's token can never bleed into a later phase's placement
    /// scan.
    ///
    /// Idempotent + coalesced: a prereq already `Queued`/`Done` for the chosen
    /// secondary is not re-appended (the `place` policy skips it) and is the
    /// only `SecondaryAffine` pool item this pass takes; re-running with
    /// nothing newly ready is a cheap no-op.
    ///
    /// ## Dead-upstream-aware placement (#650)
    /// A work unit can become ready-in-bucket AFTER its affine import has already
    /// reached a GLOBAL terminal-failure (recorded in `failed_tasks` by a
    /// pool-cascade — e.g. the import's own non-affine upstream failed
    /// non-recoverably). The #648 fast-fail bridge fires ONCE at the import's
    /// terminal seam, so a dependent that was not-yet-ready-in-bucket at that
    /// instant is MISSED by the bridge; when it later becomes ready, this
    /// placement source would re-admit it as if the import were live. To close
    /// that residual, each ready candidate is PARTITIONED via the EXACT #648
    /// predicate ([`Self::affine_unit_satisfiable_secondaries`], which keys on
    /// `failed_tasks`): a SATISFIABLE candidate is placed unchanged; a DOOMED one
    /// (no roster secondary can satisfy its deps) is NEVER recorded placed and is
    /// instead collected into the returned `(hash, phase)` doomed set. The caller
    /// (which holds `command_rx`) routes that set through the SAME claim+batch
    /// terminalization the bridge uses ([`Self::terminalize_doomed_affine_work`]).
    /// This is the CONTINUOUS complement on the placement source to #648's
    /// one-shot event-driven bridge — two triggers, ONE predicate, ONE batch — so
    /// a doomed dependent is terminalized whether its import failed before or
    /// after the dependent became ready.
    ///
    /// Returns the doomed `(work_hash, phase)` set for the caller to terminalize;
    /// empty when every ready candidate was satisfiable (the common path).
    pub(crate) async fn place_dependency_satisfied_affine_tasks(
        &mut self,
    ) -> Vec<(String, dynrunner_core::PhaseId)> {
        // The candidate secondaries for rank selection: the live roster. An
        // empty roster (no secondary yet) means nothing can be placed.
        let secondaries: Vec<String> = self.affine_placement_secondaries();
        if secondaries.is_empty() {
            return Vec::new();
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

        // Find every WORK candidate, partitioning each into PLACEABLE vs DOOMED.
        // A candidate qualifies on EITHER of two independent conditions:
        //
        //   * PLACEABLE: every affine prereq is ready-in-bucket (`ready_affine`)
        //     — the whole schedulable unit is runnable; place it as today. A unit
        //     not yet wholly ready (a prereq still blocked) is deferred (a later
        //     `TasksAdded` re-evaluates it).
        //   * DOOMED (#650): the unit is `Unsatisfiable` — its affine import is
        //     globally-failed (the EXACT #648 predicate, which reads
        //     `failed_tasks`). This is checked INDEPENDENTLY of `ready_affine`,
        //     because a globally-failed import will NEVER be a ready-in-bucket
        //     prereq, so the `all_ready` placeable gate alone can never see it —
        //     the doomed unit must be terminalized at its own work-readiness, not
        //     gated behind a prereq that can never become ready.
        //
        // A SATISFIABLE-but-not-yet-ready candidate (a live/in-flight import not
        // yet in a bucket) is neither: it is simply deferred. Only the
        // `failed_tasks`-globally-failed case diverts to terminalization — a live
        // import still places normally once ready.
        //
        // `affine_unit_satisfiable_secondaries` is computed once per candidate and
        // reused for both the doomed decision and (for a placeable one) is not
        // needed again — so the predicate runs at most once per candidate.
        // COST (MED-3b): the discriminators run on `state.def()` / the resolved
        // dep-refs IN PLACE, never on a full `task_to_info` clone. The placement
        // construction reads ONLY the task's content hash (the iteration's `hash`
        // key) + the affine subset of its deps, so the whole-`TaskInfo` clone the
        // old path paid for EVERY non-affine task (before the `affine_deps`
        // filter dropped the vast majority) is never materialized. The placed-set
        // is identical: `affine_placement_for_state` produces the same
        // `WorkPlacement` as the prior `affine_placement_for(&task_to_info(state))`
        // for the same logical task, and every downstream gate (the doomed
        // predicate, the `all_ready` check) reads only that placement plus the
        // entry's `phase_id` (read from `def()` for the doomed set, no clone).
        let mut doomed: Vec<(String, dynrunner_core::PhaseId)> = Vec::new();
        let placements: Vec<(String, super::affine_scheduler::WorkPlacement)> = self
            .cluster_state
            .tasks_iter()
            .filter_map(|(hash, state)| {
                let def = state.def();
                if def.kind.is_secondary_affine() {
                    return None;
                }
                let placement = self.affine_placement_for_state(hash, state);
                if placement.affine_deps.is_empty() {
                    return None;
                }
                // DOOMED takes precedence: a unit whose import is globally-failed
                // is collected for terminalization regardless of prereq-readiness
                // (its prereq can never become ready-in-bucket).
                if self
                    .affine_unit_satisfiable_secondaries(&placement)
                    .is_empty()
                {
                    doomed.push((placement.hash.clone(), def.phase_id.clone()));
                    return None;
                }
                // PLACEABLE: every affine prereq is ready-in-bucket.
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
                |s: &str, a: SecondaryCellId| self.cluster_state.affine_state(s, a),
            ) else {
                self.affine_scheduler.unrecord_placed_work(&work_hash);
                continue;
            };
            let mutations = self.affine_scheduler.place::<I, _>(
                &sec,
                &placement,
                |s: &str, a: SecondaryCellId| self.cluster_state.affine_state(s, a),
            );
            if !mutations.is_empty() {
                self.apply_and_broadcast_cluster_mutations(mutations).await;
            }
        }
        doomed
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
    /// `SecondaryCellUnqueued`/re-`Queued` cell mutations — then pop +
    /// dispatch it. Returns `true` iff a stolen unit was committed; `false`
    /// when no donor had work to steal (the worker stays idle this tick).
    pub(crate) async fn try_affine_steal_for_worker(&mut self, worker_idx: usize) -> bool {
        let secondary = self.workers[worker_idx].secondary_id.clone();
        // Disjoint-field borrows: the steal-eligibility closure reads
        // `cluster_state` + `workers` while `affine_scheduler` is mutably borrowed
        // (distinct fields). A work is steal-INELIGIBLE when its affine import is
        // in flight on the donor — under lazy import (c) that means the work
        // committed there (it triggered the on-demand import), so it belongs there
        // and must not be stolen (which would strand the running import).
        let cluster_state = &self.cluster_state;
        let workers = &self.workers;
        let mutations = self.affine_scheduler.steal_for::<I, _, _>(
            &secondary,
            |s: &str, a: SecondaryCellId| cluster_state.affine_state(s, a),
            |donor: &str, work_hash: &str| {
                let Some(task) = cluster_state.task_info_for_hash(work_hash) else {
                    return true;
                };
                let import_in_flight_on_donor = task.task_depends_on.iter().any(|dep| {
                    let Some(import_hash) =
                        cluster_state.task_hash_for_dep(&dep.phase_id, &dep.task_id)
                    else {
                        return false;
                    };
                    if cluster_state.affine_id_for_hash(import_hash).is_none() {
                        return false; // not an affine dep
                    }
                    workers
                        .iter()
                        .any(|w| w.secondary_id == donor && w.held_task_hash() == Some(import_hash))
                });
                !import_in_flight_on_donor
            },
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
        let (hash, is_work, affine_unit_id) = match &unit {
            QueuedUnit::Affine { hash, affine_id } => (hash.clone(), false, Some(*affine_id)),
            QueuedUnit::Work { hash } => (hash.clone(), true, None),
        };
        let Some(task) = self.cluster_state.task_info_for_hash(&hash) else {
            // Not in the fat ledger (settled / gone): the unit is stale. Drop
            // it (HINT property — a fresh placement re-derives a still-needed
            // prereq) and fall through to the global path.
            return false;
        };
        let phase = task.phase_id.clone();

        // PER-SECONDARY AFFINE-READINESS GATE (the design's HINT-property safety
        // net, Model B). Before dispatching a WORK unit, DISCRIMINATE its affine
        // deps' per-secondary bitvector cells on THIS secondary (the bitvector is
        // the readiness authority — never a global terminal) and act per the cell
        // state, rather than blindly re-placing on any non-`Done`. The four shapes
        // and their owners are computed once in [`Self::affine_readiness_gate`];
        // here we only carry out the chosen action:
        //
        //   * `Ready` (every dep `Done` here) — fall through to dispatch.
        //   * `InFlightHere` (a dep `Queued` here, none `NotDone`/`Failed`) — the
        //     import is RUNNING on this secondary. Do NOT re-place or re-emit
        //     (that would storm: an idle worker re-pops W → re-place → re-emit →
        //     loop until the import lands). Leave W at the FRONT of this queue and
        //     return; the import's own terminal (`handle_affine_task_complete` /
        //     `handle_affine_task_failed`) is the single owner of the re-pop nudge
        //     (it emits `TasksAdded`), re-popping W once the cell goes
        //     `Done`/`Failed`.
        //   * `StrandedHere` (a dep `NotDone` here, none `Failed`) — genuinely
        //     stranded without its import. RE-DERIVE: re-place the not-done
        //     prereqs + the work onto THIS secondary's queue; discard the popped
        //     orphan; re-nudge so the freshly-queued import is popped first.
        //   * `Reroute(sec)` (a dep `Failed` here, but some OTHER eligible
        //     secondary can still satisfy every dep) — re-place onto that
        //     secondary; discard the orphan; re-nudge. A `Failed` cell is
        //     non-sticky, so a re-route to a still-satisfiable secondary recovers.
        //   * `Unsatisfiable` (a dep `Failed` on EVERY eligible secondary — the
        //     import cannot run anywhere) — TERMINAL-FAIL the dependent (cascade),
        //     do NOT spin. This both fixes the all-Failed LIVELOCK and realizes
        //     the owner Q1 default (affine failure terminal-by-default).
        if is_work {
            let placement = self.affine_placement_for(&task);
            match self.affine_readiness_gate(secondary, &placement) {
                AffineGateOutcome::Ready => {}
                AffineGateOutcome::InFlightHere => {
                    // Import in flight here (#652 concern B): the work's import
                    // is `Queued` on this secondary (running, perhaps for a
                    // sibling). Do NOT requeue W onto the queue — that is the
                    // spin the redesign removes (every `TasksAdded` re-pops +
                    // re-requeues W until the import lands). Instead BLOCK W in
                    // the per-secondary blocked map on its not-yet-`Done` import
                    // cells; the import's terminal flips the cell `Done` and
                    // `on_cell_finished` (in the affine-complete handler)
                    // re-enqueues W onto this secondary's queue. The popped
                    // orphan copy is discarded (W left the queue). No re-dispatch
                    // of the import (it is already in flight).
                    let pending = self.not_done_cells_on(secondary, &placement);
                    self.affine_scheduler
                        .block_until_import(secondary, &hash, pending);
                    return false;
                }
                AffineGateOutcome::StrandedHere => {
                    // Lazy-import model (c) under #652 concern B: the work was
                    // popped for a worker on THIS secondary but an affine dep is
                    // `NotDone` here — dispatch the import ON-DEMAND now (worker
                    // in scope), claiming the cell `Queued`. Then BLOCK W in the
                    // per-secondary blocked map on its not-yet-`Done` import
                    // cells INSTEAD of requeuing it onto the queue (the spin the
                    // redesign removes); `on_cell_finished` re-enqueues W once
                    // its imports complete. The import runs ONLY because this
                    // dependent committed here — never speculatively ahead of one
                    // — so it can never be stranded by a steal. The popped orphan
                    // is discarded (W left the queue).
                    let pending = self.not_done_cells_on(secondary, &placement);
                    self.affine_scheduler
                        .block_until_import(secondary, &hash, pending);
                    self.dispatch_affine_import_on_demand(worker_idx, secondary, &placement)
                        .await;
                    return false;
                }
                AffineGateOutcome::Reroute(target) => {
                    self.affine_replace_and_nudge(&target, &placement).await;
                    return false;
                }
                AffineGateOutcome::Unsatisfiable => {
                    // The import failed on every eligible secondary — the work
                    // task can never run. Take it OUT of its bucket and account
                    // it in-flight (the symmetric accounting the dispatch path
                    // below does), then enqueue a decoupled `FailPermanent` so
                    // the operational loop cascades the terminal to dependents
                    // with the proper `command_rx` (the dispatch path holds none
                    // — the dispatch-decoupling law: emit a command, never drive
                    // the cascade inline). The popped orphan is discarded.
                    self.fail_unsatisfiable_affine_work(&hash, &phase);
                    return false;
                }
            }
        }

        // PER-SECONDARY RUN-ONCE DISPATCH GUARD (affine import). The import runs
        // ONCE per secondary, but the placement drags it in once PER dependent
        // work task, so a burst of K ready dependents on one secondary enqueues K
        // redundant import units. Two redundant-run shapes follow, both gated
        // here against the per-secondary authorities (slot occupancy + the
        // replicated bitvector cell):
        //
        //   * CONCURRENT (the `already_held` storm): K idle workers pop all K
        //     import units and dispatch the SAME hash at once, before the first
        //     dispatch's terminal lands. Only one actually runs; the secondary
        //     answers `already_held` for the rest, and those already-held slots
        //     NEVER receive a terminal — they strand `Assigned`, so
        //     `active_workers` never returns to 0 and `RunComplete` never fires.
        //     Guard: a slot on THIS secondary already holds the hash (in flight
        //     here now).
        //   * SEQUENTIAL (a SECOND run-once violation): a leftover import unit
        //     popped AFTER the first run already completed re-dispatches the
        //     import — running the per-secondary run-once body twice. Guard: the
        //     bitvector cell is already `Done` on THIS secondary (the run-once
        //     authority — it ran here). NOT gated on `Queued`: `place` sets the
        //     cell `Queued` at placement time, BEFORE the legitimate first
        //     dispatch, so a `Queued` skip would suppress the real first run.
        //
        // Both are scoped to THIS secondary, so a legitimate per-secondary run on
        // a different secondary is never suppressed. WORK units are not run-once
        // and are never gated here.
        if !is_work {
            let cell_done = affine_unit_id.is_some_and(|aid| {
                self.cluster_state.affine_state(secondary, aid) == SecondaryCell::Done
            });
            if cell_done || self.secondary_has_slot_holding_hash(secondary, &hash) {
                return false;
            }
        }

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

    /// Discriminate a popped WORK unit's affine-readiness on `secondary` by
    /// reading its deps' bitvector cells (the readiness authority). Returns the
    /// action the gate must take — see [`AffineGateOutcome`]. Pure reads — no
    /// mutation, no emit.
    ///
    /// ## Plain dependency-not-met semantics (walked in LIST ORDER)
    /// A dep is MET only when its cell is `Done`; a `Queued` (in-flight here) or
    /// `NotDone` dep is simply NOT MET, so nothing that depends on it can
    /// dispatch yet. The gate walks the deps in LIST ORDER and decides on the
    /// FIRST not-`Done` dep — it must NOT advance past an unmet earlier dep to a
    /// later one.
    ///
    /// A `Failed` dep ANYWHERE is order-independent: it means this secondary
    /// cannot run the unit no matter what the other deps are, so it forces the
    /// re-route / terminal-fail decision regardless of position.
    ///
    /// Among the NON-failed deps, the FIRST not-`Done` dep is the decision point:
    /// if it is `Queued` (in flight here — its import was claimed, possibly by a
    /// SIBLING worker on this same secondary) it is UNMET, so the unit
    /// `InFlightHere`-WAITS for it to reach `Done`; if it is `NotDone` the unit is
    /// genuinely stranded without that import (`StrandedHere`). This is the
    /// correction to the previously order-BLIND `Failed` > `NotDone` > `Queued`
    /// precedence, which wrongly SKIPPED a `Queued` (unmet) earlier dep to act on
    /// a later `NotDone` one — dispatching e.g. a delta import before its
    /// still-in-flight base had landed on the node (the multi-worker-same-node
    /// race: the delta's imported path is invalid until the base is present).
    /// The wait reuses the EXISTING `InFlightHere` mechanics (`requeue_front` +
    /// the import's own terminal re-nudge) — no new abstraction; the in-flight
    /// dep WILL reach `Done` on the worker running it and re-nudge the waiter.
    fn affine_readiness_gate(
        &self,
        secondary: &str,
        placement: &WorkPlacement,
    ) -> AffineGateOutcome {
        // A GLOBALLY-failed affine import (in the pool's `failed_tasks` terminal
        // ledger — its non-affine upstream pool-cascade-failed it #648) is
        // order-independent UNsatisfiable: unlike a per-secondary `Failed` cell
        // (recoverable by a re-route to a still-eligible secondary), a globally-
        // failed import cannot run ANYWHERE, so there is nothing to re-route to.
        // Checked FIRST so the verdict is `Unsatisfiable` even when every cell is
        // still `NotDone` (a pool-cascade import never ran, so it flips no cell).
        // The `select_secondary` over `affine_unit_satisfiable_secondaries`
        // (empty for a globally-failed dep) would also yield `Unsatisfiable`, but
        // the direct check makes the dead-import case explicit + skips the rank
        // walk.
        if placement
            .affine_deps
            .iter()
            .any(|(_, import_hash)| self.failed_tasks.contains_key(import_hash))
        {
            return AffineGateOutcome::Unsatisfiable;
        }

        // A `Failed` dep anywhere is order-independent terminal: this secondary
        // cannot satisfy the unit regardless of any other dep's position. Re-route
        // to an eligible secondary that can still satisfy EVERY dep (none `Failed`
        // there), by the same locality rank a fresh placement uses; if none exists
        // (the import failed everywhere), the unit is doomed.
        let any_failed = placement.affine_deps.iter().any(|(aid, _)| {
            self.cluster_state.affine_state(secondary, *aid) == SecondaryCell::Failed
        });
        if any_failed {
            let satisfiable = self.affine_unit_satisfiable_secondaries(placement);
            return match self.affine_scheduler.select_secondary(
                &satisfiable,
                placement,
                |s: &str, a: SecondaryCellId| self.cluster_state.affine_state(s, a),
            ) {
                Some(target) => AffineGateOutcome::Reroute(target),
                None => AffineGateOutcome::Unsatisfiable,
            };
        }

        // No dep failed: decide on the FIRST dep in LIST ORDER that is not yet
        // `Done` (the deps list is the dispatch order — a base precedes the delta
        // layered on it). A dep is MET only when `Done`; a `Queued` earlier dep
        // (import in flight here, perhaps claimed by a sibling worker) is UNMET,
        // so the unit `InFlightHere`-WAITS for it — the later deps are NOT
        // considered, and the unit is never ASSIGNED ahead of the unmet dep. Only
        // when the first not-`Done` dep is itself `NotDone` is the unit genuinely
        // stranded without that import here. This stops the previously order-blind
        // gate from skipping a `Queued` (unmet) earlier dep to a later `NotDone`.
        for (aid, _) in &placement.affine_deps {
            match self.cluster_state.affine_state(secondary, *aid) {
                SecondaryCell::Done => {}
                SecondaryCell::Queued => return AffineGateOutcome::InFlightHere,
                SecondaryCell::NotDone => return AffineGateOutcome::StrandedHere,
                // `Failed` was handled order-independently above.
                SecondaryCell::Failed => unreachable!("Failed handled above"),
            }
        }
        AffineGateOutcome::Ready
    }

    /// The eligible (roster) secondaries on which the unit is still SATISFIABLE
    /// — every affine dep's cell is NOT `Failed` (a `Done`/`Queued`/`NotDone`
    /// cell is satisfied, in flight, or placeable). The "any eligible secondary
    /// still satisfiable?" check behind the re-route-vs-terminal-fail decision:
    /// an empty result means the import `Failed` on every eligible secondary, so
    /// the dependent is doomed. Reads the roster (not just the bitvector's
    /// written secondaries) so a fresh all-`NotDone` secondary counts as
    /// placeable.
    ///
    /// ## GLOBAL import-failure is order-independent unsatisfiable (#648)
    /// A per-secondary `Failed` cell is the LOCAL signal a single secondary
    /// could not run the import (recoverable elsewhere). But an affine import
    /// can also reach a GLOBAL terminal-failure that no secondary can recover
    /// from: when its OWN non-affine upstream fails non-recoverably, the
    /// permanent-fail cascade (`fail_permanent_cascade_mutations` →
    /// `on_item_failed_permanent`) records the import in `failed_tasks` WITHOUT
    /// flipping any bitvector cell (the import never ran anywhere — its cells
    /// stay `NotDone`). A unit with such a dep is doomed on EVERY secondary
    /// regardless of cell state, so a globally-failed dep collapses the
    /// satisfiable set to empty — the missing edge the per-secondary `Failed`
    /// gate alone never sees (its cells are all `NotDone`). This is the single
    /// predicate that bridges a pool-cascade import failure into the affine
    /// `Unsatisfiable` verdict; `fast_fail_affine_dependents_if_unsatisfiable`
    /// then terminalizes the doomed dependents.
    fn affine_unit_satisfiable_secondaries(&self, placement: &WorkPlacement) -> Vec<String> {
        // A GLOBALLY-failed affine import (recorded in the pool's `failed_tasks`
        // terminal ledger) cannot run on ANY secondary — the unit is doomed
        // everywhere, so the satisfiable set is empty.
        if placement
            .affine_deps
            .iter()
            .any(|(_, import_hash)| self.failed_tasks.contains_key(import_hash))
        {
            return Vec::new();
        }
        self.affine_placement_secondaries()
            .into_iter()
            .filter(|sec| {
                placement.affine_deps.iter().all(|(aid, _)| {
                    self.cluster_state.affine_state(sec, *aid) != SecondaryCell::Failed
                })
            })
            .collect()
    }

    /// Whether one affine IMPORT (`affine_id`) can no longer run anywhere — its
    /// cell is `Failed` on EVERY eligible roster secondary, with none left where
    /// it is placeable (`Done`/`Queued`/`NotDone`). The import-level twin of
    /// [`Self::affine_unit_satisfiable_secondaries`] (which asks the same
    /// roster question for a WORK unit's full dep set): an all-`Failed` import
    /// has reached its GLOBAL terminal-failure and must be recorded in the
    /// pool's terminal set so its phase's affine guard clears (the
    /// strand-forever otherwise). Reads the roster, not just the bitvector's
    /// written cells, so a fresh all-`NotDone` secondary still counts as
    /// placeable (the import is NOT yet globally failed).
    pub(super) fn affine_import_globally_failed(&self, affine_id: SecondaryCellId) -> bool {
        let secondaries = self.affine_placement_secondaries();
        // An empty roster is not a global FAILURE — the import simply has no
        // secondary to run on yet (a transient bring-up window); treat it as
        // not-globally-failed so a momentarily-empty roster never records a
        // spurious terminal.
        !secondaries.is_empty()
            && secondaries
                .iter()
                .all(|sec| self.cluster_state.affine_state(sec, affine_id) == SecondaryCell::Failed)
    }

    /// Re-place `placement`'s still-not-done prereqs + the work onto `target`'s
    /// per-secondary queue (the design's "re-derive and re-queue the missing
    /// affine deps") and re-nudge the recheck so the freshly-queued import is
    /// popped first. The popped orphan copy is DISCARDED by the caller (it
    /// returns `false`), so the queue now carries the properly-ordered
    /// import→work prefix. Shared by the `StrandedHere` (re-place on THIS
    /// secondary) and `Reroute` (re-place on another satisfiable secondary)
    /// arms — one re-derive owner.
    async fn affine_replace_and_nudge(&mut self, target: &str, placement: &WorkPlacement) {
        let mutations = self.affine_scheduler.place::<I, _>(
            target,
            placement,
            |s: &str, a: SecondaryCellId| self.cluster_state.affine_state(s, a),
        );
        if !mutations.is_empty() {
            self.apply_and_broadcast_cluster_mutations(mutations).await;
        }
        self.cluster_state
            .emit_worker_mgmt(crate::worker_signal::WorkerMgmtSignal::TasksAdded);
    }

    /// Cell-`Finished` unblock (#652 concern B): an affine import's cell just
    /// flipped `Done` on `secondary`. Re-enqueue every work that was blocked on
    /// that import here onto `secondary`'s queue so it re-pops + re-gates (the
    /// gate then dispatches it `Ready`, kicks its next list-order import
    /// `StrandedHere`, or re-blocks `InFlightHere`). Called from the
    /// affine-complete handler right after the cell flips `Done`. A no-op when
    /// nothing was blocked on this `(secondary, affine_id)`.
    ///
    /// Re-enqueue is via `place` (re-appends the Work unit to the queue — the
    /// work was previously popped OUT of the queue into the blocked map, so it is
    /// absent from the queue and `place` does not double-queue it; `placed_work`
    /// stays set, which is correct: the work IS placed, just on the blocked
    /// overlay until now).
    ///
    /// `recheck_signal` is the worker-management recheck the caller wants for the
    /// re-popped work (#656 M2). A genuine import COMPLETION (the affine-complete
    /// caller) frees its worker — a real capacity-restoring event — so it passes
    /// the bypass=TRUE `TasksAdded` (and the #652 `TaskComplete` clear has
    /// already removed any backpressure flag for that secondary). A
    /// CAPACITY-BOUNCE (the failed handler's "No idle worker available" arm)
    /// instead passes the bypass=FALSE `TasksReadyBackpressureAware` so the
    /// at-capacity secondary the caller just flagged is SKIPPED on the recheck —
    /// braking the import-bounce micro-loop. The seam takes the signal rather
    /// than deciding it so the bounce-shape policy stays wholly in the caller.
    pub(crate) async fn reenqueue_affine_unblocked_on_cell(
        &mut self,
        secondary: &str,
        affine_id: SecondaryCellId,
        recheck_signal: crate::worker_signal::WorkerMgmtSignal,
    ) {
        let freed = self.affine_scheduler.on_cell_finished(secondary, affine_id);
        if freed.is_empty() {
            return;
        }
        for work_hash in &freed {
            let Some(task) = self.cluster_state.task_info_for_hash(work_hash) else {
                // Settled / gone (the work terminalized while blocked): drop it,
                // nothing to re-enqueue.
                continue;
            };
            let placement = self.affine_placement_for(&task);
            let mutations = self.affine_scheduler.place::<I, _>(
                secondary,
                &placement,
                |s: &str, a: SecondaryCellId| self.cluster_state.affine_state(s, a),
            );
            if !mutations.is_empty() {
                self.apply_and_broadcast_cluster_mutations(mutations).await;
            }
        }
        self.cluster_state.emit_worker_mgmt(recheck_signal);
    }

    /// Cell-`Failed` / dead-secondary re-route (#652 concern B's import-FAIL +
    /// dead-secondary edges): drain every work blocked on `secondary` (filtered
    /// to `affine_id` when `Some`, the whole secondary when `None`) and make a
    /// FRESH route/terminalize decision for each RIGHT NOW — never leaving it
    /// stranded until the 5-min reconcile. For each drained work:
    ///   * SATISFIABLE elsewhere → clear its placement-dedup so the next
    ///     placement pass re-derives it onto a still-eligible secondary (the
    ///     same `unrecord_placed_work` seam `requeue_affine_aware` uses); the
    ///     emitted `TasksAdded` re-runs placement.
    ///   * UNSATISFIABLE (import failed on every eligible secondary) → collect
    ///     for the shared #648/#650 terminalize batch.
    ///
    /// The work STAYS a pool item throughout (its phase-drain token); this only
    /// re-decides the per-secondary scheduling overlay.
    pub(crate) async fn reroute_affine_blocked_on(
        &mut self,
        secondary: &str,
        affine_id: Option<SecondaryCellId>,
        command_rx: &mut Option<tokio::sync::mpsc::Receiver<super::command_channel::PrimaryCommand<I>>>,
    ) {
        let drained = self.affine_scheduler.drain_blocked_on(secondary, affine_id);
        if drained.is_empty() {
            return;
        }
        let mut doomed: Vec<(String, dynrunner_core::PhaseId)> = Vec::new();
        for work_hash in &drained {
            let Some(task) = self.cluster_state.task_info_for_hash(work_hash) else {
                continue;
            };
            let placement = self.affine_placement_for(&task);
            if self.affine_unit_satisfiable_secondaries(&placement).is_empty() {
                // Doomed everywhere — terminalize via the shared dead-upstream
                // batch path (claim out of bucket + one broadcast + one cascade).
                doomed.push((work_hash.clone(), task.phase_id.clone()));
            } else {
                // Re-routable: clear the placement guard so placement re-derives
                // the work onto a still-eligible secondary on the next pass.
                self.affine_scheduler.unrecord_placed_work(work_hash);
            }
        }
        if !doomed.is_empty() {
            self.terminalize_doomed_affine_work(doomed, command_rx).await;
        }
        // Re-run placement (re-route the cleared works) + dispatch.
        self.cluster_state
            .emit_worker_mgmt(crate::worker_signal::WorkerMgmtSignal::TasksAdded);
    }

    /// 5-min reconcile sweep (#652 concern C) — the ONE level-net that replaces
    /// the deleted per-task DispatchBackoff, uniform across normal + affine
    /// blocked work. Fired by the operational loop's `ARM_RECONCILE` arm on a
    /// FIXED 5-min cadence. Recovers a blocked dependent whose unblock event will
    /// never come (its dep's holder died, or a completion/`Finished` was lost) by
    /// moving it to the GENERAL-QUEUE HEAD, where the idempotent pop-time
    /// re-check (D.1) / fresh affine placement re-routes it. The PRIMARY handlers
    /// for KNOWN events (import-fail, dead-secondary) already drain their blocks
    /// directly; this is the orphan-only backstop for the truly-stranded.
    ///
    /// Two owners, ONE orchestrator: the pool owns the normal-blocked validity
    /// predicate ([`PendingPool::reconcile_blocked`]); the affine scheduler owns
    /// the per-secondary-blocked one
    /// ([`AffineScheduler::reconcile_per_secondary_blocked`]). This method only
    /// supplies the live-roster + in-flight + bitvector reads and routes the
    /// returned orphans — it re-implements no dep logic.
    pub(crate) async fn reconcile_orphaned_blocked_work(&mut self) {
        // The live roster (reachable secondaries) + the set of dep task_ids
        // currently in-flight on a LIVE worker (dead workers are reaped out of
        // `self.workers`, so a held hash is in-flight on a reachable secondary by
        // construction). Built once for both sweeps.
        let reachable: std::collections::HashSet<String> =
            self.affine_placement_secondaries().into_iter().collect();
        let in_flight_dep_ids: HashSet<String> = self
            .workers
            .iter()
            .filter_map(|w| w.held_task_hash())
            .filter_map(|hash| self.cluster_state.task_info_for_hash(hash))
            .map(|t| t.task_id.clone())
            .collect();

        // NORMAL-blocked orphans → general-queue head (D.1 re-routes on pop).
        let normal_orphans = self
            .pool_mut()
            .reconcile_blocked(|dep_id| in_flight_dep_ids.contains(dep_id));
        let normal_count = normal_orphans.len();
        for item in normal_orphans {
            self.pool_mut().push_to_queue_head(item);
        }

        // AFFINE-blocked orphans → clear the placement guard so the next
        // placement pass re-derives each onto a live-route secondary (the
        // requeue_affine_aware unrecord seam). The work stays a pool item, so
        // no general-queue push is needed — affine-dep work dispatches only
        // through the per-secondary path, never the general worker view.
        //
        // M1 (#656): a `Queued` import cell counts as "unblock coming" ONLY
        // when its import is GENUINELY running on a live worker slot of that
        // secondary. A `Queued` cell with no holding slot is a silently-lost
        // holder (no `TaskFailed` bounce emitted, so the backpressure-arm reset
        // never fired) — the dependent would otherwise stay blocked forever.
        // The `import_in_flight` predicate mirrors `secondary_has_slot_holding_hash`
        // (the in-tree "is this import running on this secondary's slots?"
        // precedent `reset_stale_eager_prep_cells` uses), composed here from
        // disjoint field reads so the scheduler's `&mut` borrow stays clear of
        // the `&self` reads (`hash_for_cell_id` + the live-slot scan). The
        // affine scheduler owns no slot/replicated state, so it learns no hash.
        let workers = &self.workers;
        let cluster_state = &self.cluster_state;
        let affine_orphans = self.affine_scheduler.reconcile_per_secondary_blocked(
            |sec: &str| reachable.contains(sec),
            |s: &str, a: SecondaryCellId| cluster_state.affine_state(s, a),
            |s: &str, a: SecondaryCellId| {
                cluster_state.hash_for_cell_id(a).is_some_and(|hash| {
                    workers.iter().any(|w| {
                        w.secondary_id == s && w.held_task_hash() == Some(hash)
                    })
                })
            },
        );
        let affine_count = affine_orphans.len();
        for work_hash in &affine_orphans {
            self.affine_scheduler.unrecord_placed_work(work_hash);
        }

        if normal_count > 0 || affine_count > 0 {
            tracing::info!(
                normal_orphans = normal_count,
                affine_orphans = affine_count,
                "reconcile: re-routed orphaned blocked work (the 5-min level-net)"
            );
            // A pool-entry / placement edge: nudge the recheck so the re-routed
            // work is re-evaluated (D.1 pop-time re-check for normal, fresh
            // affine placement for affine).
            self.cluster_state
                .emit_worker_mgmt(crate::worker_signal::WorkerMgmtSignal::TasksAdded);
        }
    }

    /// The affine import cells of `placement` that are NOT yet `Done` on
    /// `secondary` (#652 concern B) — exactly the imports a work blocked on this
    /// secondary is WAITING for. `block_until_import` records this set; each cell
    /// flipping `Done` ([`AffineScheduler::on_cell_finished`]) drops one, and the
    /// work unblocks when the set empties. A `Failed` cell is included (it is not
    /// `Done`); the import-FAIL edge drains such a block via
    /// [`AffineScheduler::drain_blocked_on`] before the cell could matter here.
    fn not_done_cells_on(&self, secondary: &str, placement: &WorkPlacement) -> Vec<SecondaryCellId> {
        placement
            .affine_deps
            .iter()
            .filter(|(aid, _)| {
                self.cluster_state.affine_state(secondary, *aid) != SecondaryCell::Done
            })
            .map(|(aid, _)| *aid)
            .collect()
    }

    /// Lazy-import ON-DEMAND dispatch (model c) under #652 concern B: a WORK
    /// unit committed on `secondary` (popped for `worker_idx`) found an affine
    /// dep `NotDone` here (the `StrandedHere` gate). The caller has ALREADY
    /// blocked the work in the per-secondary blocked map; this helper only
    /// dispatches the missing import on `secondary` NOW — claiming the cell
    /// `Queued` and sending the import to this worker. When the import terminals
    /// its cell flips `Done` and the affine-complete handler's
    /// [`AffineScheduler::on_cell_finished`] re-enqueues the work onto this
    /// secondary's queue (NO requeue/spin here — the work LEFT the queue into the
    /// blocked map). The import runs ONLY because this dependent committed here,
    /// so it is never speculatively dispatched ahead of a work and never
    /// strandable by a steal.
    ///
    /// Dispatches the FIRST `NotDone` dep. This is consistent with the
    /// list-order dependency-not-met gate: `StrandedHere` fires ONLY when the
    /// first not-`Done` dep is itself `NotDone` (an earlier `Queued`/unmet dep
    /// would have gated `InFlightHere` and never reached here), so the first
    /// `NotDone` dep IS the first not-`Done` dep — the right import to run.
    /// Multi-dep units resolve one import per unblock in list order (each earlier
    /// dep, once in flight, is unmet so the work stays blocked on it via the
    /// `InFlightHere` re-block on the next pop), so a delta import is never
    /// dispatched ahead of its still-not-met base. The run-once + concurrent
    /// dispatch-once guards in [`Self::dispatch_affine_unit`] still apply (a
    /// `Done`/in-flight import here is skipped), so a re-pop racing the import's
    /// terminal cannot double-run it.
    async fn dispatch_affine_import_on_demand(
        &mut self,
        worker_idx: usize,
        secondary: &str,
        placement: &WorkPlacement,
    ) {
        // First NotDone affine dep — the import to run on-demand here. (Queued =
        // already in flight here; Done = already ran here; Failed = the gate took
        // a different arm — none reach `StrandedHere`.)
        let next_import = placement.affine_deps.iter().find(|(aid, _)| {
            self.cluster_state.affine_state(secondary, *aid) == SecondaryCell::NotDone
        });
        let Some((aid, import_hash)) = next_import.cloned() else {
            // No NotDone dep (raced to Done/Queued/Failed since the gate read) —
            // just nudge so the work re-pops and re-gates.
            self.cluster_state
                .emit_worker_mgmt(crate::worker_signal::WorkerMgmtSignal::TasksAdded);
            return;
        };

        // Claim the cell `Queued` (the per-secondary in-flight claim), then
        // dispatch the import as its own affine unit through the shared seam —
        // which re-applies the run-once + dispatch-once guards.
        self.apply_and_broadcast_cluster_mutations(vec![ClusterMutation::SecondaryCellQueued {
            secondary: secondary.to_string(),
            cell_id: aid.0,
            generation: 0,
        }])
        .await;
        // `Box::pin` the recursive dispatch: `dispatch_affine_unit` (work arm) →
        // here → `dispatch_affine_unit` (import arm) is a cycle the async state
        // machine must box. The import arm never re-enters this helper (it is not
        // a work unit), so the recursion is depth-1.
        Box::pin(self.dispatch_affine_unit(
            worker_idx,
            secondary,
            QueuedUnit::Affine {
                affine_id: aid,
                hash: import_hash,
            },
        ))
        .await;

        // Re-nudge so a sibling idle worker re-pops the work (held `InFlightHere`
        // until the import's terminal flips the cell `Done`).
        self.cluster_state
            .emit_worker_mgmt(crate::worker_signal::WorkerMgmtSignal::TasksAdded);
    }

    /// Claim a queued affine-dep WORK item for terminal failure: take it OUT of
    /// its bucket + `mark_in_flight`. Returns `true` iff the item was in a
    /// dispatchable bucket (so a failure must now follow), `false` if the claim
    /// is stale (already taken / phase not Active — nothing to fail).
    ///
    /// The bucket-take + in-flight bump is the SAME symmetric accounting a real
    /// dispatch does, so the subsequent `on_item_failed_permanent` in-flight
    /// DECREMENT (run by `apply_fail_permanent`, whichever way the failure is
    /// executed) balances — the item was never globally dispatched but is queued,
    /// so without this the decrement would corrupt a sibling's slot. The single
    /// owner of this accounting, shared by the per-dispatch single-item path
    /// ([`Self::fail_unsatisfiable_affine_work`]) and the event-driven batch path
    /// ([`Self::fast_fail_affine_dependents_if_unsatisfiable`]).
    #[must_use]
    fn claim_affine_work_for_fail(&mut self, hash: &str, phase: &dynrunner_core::PhaseId) -> bool {
        let target = hash.to_string();
        if self
            .pool_mut()
            .take_first_match(|t| compute_task_hash(t) == target)
            .is_none()
        {
            return false;
        }
        self.pool_mut().mark_in_flight(phase);
        true
    }

    /// The canonical reason string for an affine dependent whose import failed
    /// on every eligible secondary — shared by the single-item + batch paths so
    /// the operator-visible terminal message is identical.
    fn affine_unsatisfiable_reason(hash: &str) -> String {
        format!(
            "affine import for work task {hash} FAILED on every eligible \
             secondary — the per-secondary import cannot be satisfied anywhere, \
             so the dependent is permanently unfulfillable"
        )
    }

    /// Terminal-fail an affine-dep WORK task whose import `Failed` on every
    /// eligible secondary (the `Unsatisfiable` gate verdict) — the cascade that
    /// breaks the all-`Failed` livelock and realizes the owner Q1 default
    /// (affine failure terminal-by-default).
    ///
    /// THE DISPATCH-PATH (single-item) caller: invoked from
    /// [`Self::dispatch_affine_unit`]'s `Unsatisfiable` arm, which holds NO
    /// `command_rx` — the dispatch-decoupling law: the dispatch path EMITS a
    /// command, it never drives the cascade inline. This is ALWAYS a SINGLE
    /// dependent per call (one popped unit), never a burst, so the bounded self
    /// command channel ([`COMMAND_CHANNEL_CAPACITY`]) is never overflowed by it.
    /// The event-driven BURST (an all-`Failed` gate's whole dependent set) takes
    /// the direct path instead ([`Self::fast_fail_affine_dependents_if_unsatisfiable`]),
    /// precisely BECAUSE a bounded channel cannot carry thousands of dependents
    /// in one sweep without dropping the overflow.
    ///
    /// A `Full`/`Closed` channel is a degenerate teardown state; the drop is
    /// benign (the run is winding down). The reply oneshot is fire-and-forget
    /// (the dropped receiver is the documented in-runtime shape).
    fn fail_unsatisfiable_affine_work(&mut self, hash: &str, phase: &dynrunner_core::PhaseId) {
        // Claim the queued work item (take out of bucket + mark in-flight); a
        // stale claim has nothing to fail — drop quietly.
        if !self.claim_affine_work_for_fail(hash, phase) {
            return;
        }
        // Clear the placement-dedup record (#650): a gate-failed work task must
        // not stay `placed_work`-marked, else it sits placed-but-unqueued forever
        // (the strand diagnostic grows monotonically — the 3570-churn's secondary
        // contributor when a doomed work is requeued + re-popped + gate-failed
        // repeatedly without ever clearing its placed record). It is now terminal,
        // so the placement scan will not re-derive it.
        self.affine_scheduler.unrecord_placed_work(hash);
        let (reply, _rx) = tokio::sync::oneshot::channel();
        let cmd = super::command_channel::PrimaryCommand::FailPermanent {
            hash: hash.to_string(),
            error: dynrunner_core::ErrorType::NonRecoverable,
            reason: Self::affine_unsatisfiable_reason(hash),
            reply,
        };
        if let Err(err) = self.command_tx.try_send(cmd) {
            // A full/closed self-channel is a degenerate teardown state; the
            // cascade is dropped (the work item is already out of the bucket +
            // accounted in-flight, so it no longer dispatches — the run is
            // winding down). Undo the in-flight bump so a surviving phase
            // machine is not left with a phantom slot.
            tracing::debug!(
                task_hash = %hash,
                error = %err,
                "affine terminal-fail: command channel unavailable; dropping cascade"
            );
            self.pool_mut().on_item_finished(phase, None);
        }
    }

    /// EVENT-DRIVEN BATCH fast-fail for an affine import that just FAILED on a
    /// secondary (the live trigger from
    /// [`PrimaryCoordinator::handle_affine_task_failed`], called right after the
    /// terminal flips the cell `→ Failed`). When that failure makes the affine
    /// gate transition to all-eligible-`Failed` — the import can no longer run on
    /// any roster secondary — enumerate EVERY WORK unit depending on that
    /// `affine_id` and terminal-fail the ones that are now `Unsatisfiable` in ONE
    /// sweep, instead of waiting for each dependent to be popped for a worker and
    /// lazily gated `Unsatisfiable` at dispatch time (the per-dispatch-tick drain
    /// that starved the phase: ~0.2 fails/sec across 12.5k dependents).
    ///
    /// ## Why this is NOT a new policy
    /// The terminal-fail decision is the EXACT same predicate the per-dispatch
    /// gate ([`Self::affine_readiness_gate`] → `Unsatisfiable`) already uses:
    /// [`Self::affine_unit_satisfiable_secondaries`] is empty (no roster
    /// secondary can satisfy EVERY one of the dependent's affine deps). A
    /// dependent with another satisfiable secondary (a `Done`/`Queued`/`NotDone`
    /// cell elsewhere — incl. a fresh all-`NotDone` secondary) is left untouched,
    /// so this never over-fast-fails: it is the same roster-aware `Unsatisfiable`
    /// semantics, applied EAGERLY + BATCHED on the failure transition rather than
    /// LAZILY per dispatch tick. A dependent with a SECOND affine dep that is
    /// still placeable somewhere is NOT failed here — only the genuinely-doomed
    /// ones are.
    ///
    /// Each doomed dependent is claimed out of its bucket
    /// ([`Self::claim_affine_work_for_fail`]) and the whole set is terminal-failed
    /// by ONE [`PrimaryCoordinator::apply_fail_permanent_batch`] — NOT by
    /// enqueuing `FailPermanent`s onto the bounded self command channel, and NOT
    /// by calling `apply_fail_permanent` per item. Two scale flaws this closes:
    ///
    ///  1. The channel is bounded ([`COMMAND_CHANNEL_CAPACITY`]), so a
    ///     synchronous burst of N≫capacity `try_send`s would `Err(Full)` past the
    ///     cap and DROP the overflow dependents — each already out of its bucket
    ///     and accounted in-flight, hence permanently LOST (never terminal → the
    ///     run hangs). The trigger
    ///     ([`PrimaryCoordinator::handle_affine_task_failed`]) is the operational
    ///     loop's terminal handler, which HOLDS `command_rx` (unlike the dispatch
    ///     path), so the burst is failed DIRECTLY — no channel between it and the
    ///     cascade, nothing dropped no matter how many dependents fail.
    ///  2. Failing per item would do N broadcasts (each pushed onto the mesh send
    ///     queue) + N phase-lifecycle passes — an op-loop stall + a mesh-send
    ///     flood. `apply_fail_permanent_batch` accumulates ALL terminal mutations
    ///     and broadcasts them ONCE, then runs the lifecycle cascade ONCE.
    ///
    /// (The single-item dispatch-path arm keeps the EMIT — it is never a burst.)
    ///
    /// The batch is idempotent against an already-terminal dependent (it dedups
    /// via `failed_tasks` / the CRDT terminal), and a non-bucket / stale
    /// dependent is dropped by the `claim` returning `false`, so a later stale
    /// per-secondary re-pop of the same unit is a harmless no-op and the gate is
    /// effectively not re-evaluated per dependent. Resolving the failed hash to
    /// no `affine_id` (an ordinary work terminal) is a no-op.
    pub(crate) async fn fast_fail_affine_dependents_if_unsatisfiable(
        &mut self,
        failed_task_hash: &str,
        command_rx: &mut Option<tokio::sync::mpsc::Receiver<super::command_channel::PrimaryCommand<I>>>,
    ) {
        // The terminal must be an AFFINE import for a fast-fail to apply; an
        // ordinary work terminal binds to no affine-id.
        let Some(affine_id) = self.cluster_state.affine_id_for_hash(failed_task_hash) else {
            return;
        };

        // Enumerate every WORK unit that depends on this affine_id and is NOW
        // unsatisfiable on the whole roster (the gate's `Unsatisfiable` shape).
        // Build the `(hash, phase)` failure list first, holding no live ledger
        // borrow into the mutating fail sweep.
        let doomed: Vec<(String, dynrunner_core::PhaseId)> = self
            .cluster_state
            .tasks_iter()
            .filter_map(|(_, state)| {
                let def = state.def();
                if def.kind.is_secondary_affine() {
                    return None;
                }
                let task = self.cluster_state.task_to_info(state);
                let placement = self.affine_placement_for(&task);
                // Only units that actually depend on the just-failed affine_id —
                // a failure on one import cannot doom a unit that never needed it.
                if !placement.affine_deps.iter().any(|(aid, _)| *aid == affine_id) {
                    return None;
                }
                // The EXACT `Unsatisfiable` predicate the per-dispatch gate uses:
                // no roster secondary can satisfy EVERY affine dep of this unit.
                // A unit with another still-satisfiable secondary (incl. a fresh
                // all-`NotDone` one, or a second dep placeable elsewhere) is left
                // for the normal dispatch/reroute path — never over-fast-failed.
                if !self
                    .affine_unit_satisfiable_secondaries(&placement)
                    .is_empty()
                {
                    return None;
                }
                Some((placement.hash.clone(), task.phase_id.clone()))
            })
            .collect();

        // Claim + batch-terminalize the doomed set — the SAME tail the placement
        // source (`place_dependency_satisfied_affine_tasks`) routes its declined
        // doomed candidates through (#650), so the claim ↔ decrement accounting
        // and the one-broadcast batch live in ONE owner.
        self.terminalize_doomed_affine_work(doomed, command_rx).await;
    }

    /// Claim a set of doomed affine-dep WORK units out of their buckets and
    /// terminal-fail the whole set in ONE batch — the shared claim+batch tail of
    /// every dead-upstream-aware affine terminalization path. Two triggers feed
    /// it ONE predicate-already-applied `(hash, phase)` set:
    ///
    ///   * the EVENT-DRIVEN bridge ([`Self::fast_fail_affine_dependents_if_unsatisfiable`],
    ///     #648) — an affine import just reached its global terminal-failure;
    ///   * the PLACEMENT SOURCE ([`Self::place_dependency_satisfied_affine_tasks`],
    ///     #650) — a work unit became ready-in-bucket AFTER its import was already
    ///     globally-failed, so placement declines it instead of re-admitting it.
    ///
    /// Both callers have ALREADY applied the `Unsatisfiable` predicate
    /// ([`Self::affine_unit_satisfiable_secondaries`] empty); this owner only does
    /// the claim + batch, so the two sources share one accounting + one broadcast.
    ///
    /// CLAIM each doomed dependent out of its bucket (+ mark in-flight) so the
    /// batch's `on_item_failed_permanent` decrement balances. A stale claim
    /// (already taken / phase not Active) is skipped — nothing to fail there.
    /// Only the successfully-claimed items go to the batch, keeping the
    /// claim ↔ decrement accounting exactly paired.
    pub(crate) async fn terminalize_doomed_affine_work(
        &mut self,
        doomed: Vec<(String, dynrunner_core::PhaseId)>,
        command_rx: &mut Option<tokio::sync::mpsc::Receiver<super::command_channel::PrimaryCommand<I>>>,
    ) {
        let claimed: Vec<(String, dynrunner_core::ErrorType, String)> = doomed
            .into_iter()
            .filter_map(|(hash, phase)| {
                self.claim_affine_work_for_fail(&hash, &phase).then(|| {
                    let reason = Self::affine_unsatisfiable_reason(&hash);
                    (hash, dynrunner_core::ErrorType::NonRecoverable, reason)
                })
            })
            .collect();

        // ONE broadcast + ONE lifecycle pass for the whole burst — no bounded
        // channel (⇒ no overflow ⇒ none lost) and no N-broadcast mesh flood — so
        // the all-Failed gate's dependents fail promptly in a single batch
        // instead of draining one per dispatch tick.
        self.apply_fail_permanent_batch(claimed, command_rx).await;
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
        // Lazy import (c): the rebuild re-derives ONLY the work units against the
        // inherited bitvector. There is no enqueued import unit to reconstruct, so
        // the stranded-vs-in-flight `import_held` discriminator the eager model
        // needed is gone: a `Queued` cell whose import was in flight will terminal
        // (its cell flips and the work gates normally); a `Queued` cell whose
        // holder also died re-derives a fresh import on-demand when the work next
        // commits (the `StrandedHere` arm reads the live cell). Either way no
        // double-run, no stranded dependent.
        let mutations = self.affine_scheduler.rebuild::<I, _>(
            &secondaries,
            &placements,
            |s: &str, a: SecondaryCellId| self.cluster_state.affine_state(s, a),
        );
        if !mutations.is_empty() {
            crate::cluster_state::apply_locally_for_broadcast(&mut self.cluster_state, mutations);
        }
    }

    /// Test-only inspector: the [`AffineGateOutcome`] discriminant the readiness
    /// gate produces for `task` on `secondary`, as a stable label. Lets the
    /// gate tests assert the list-order classification (a `Queued`/unmet earlier
    /// dep ⇒ `InFlightHere`, not skipped to a later `NotDone`) without exposing
    /// the private outcome enum across the module boundary.
    #[cfg(test)]
    pub(crate) fn affine_gate_label_for_test(&self, secondary: &str, task: &TaskInfo<I>) -> String {
        let placement = self.affine_placement_for(task);
        match self.affine_readiness_gate(secondary, &placement) {
            AffineGateOutcome::Ready => "Ready".into(),
            AffineGateOutcome::InFlightHere => "InFlightHere".into(),
            AffineGateOutcome::StrandedHere => "StrandedHere".into(),
            AffineGateOutcome::Reroute(target) => format!("Reroute({target})"),
            AffineGateOutcome::Unsatisfiable => "Unsatisfiable".into(),
        }
    }
}
