//! Phase-state machine plus task-completion / failure / retry inputs:
//! every method that mutates `in_flight_per_phase`, `phase_state`,
//! `blocked_per_phase`, or the dependents walk lives here.
//!
//! Entry points:
//! * [`PendingPool::on_item_finished`] — terminal success.
//! * [`PendingPool::on_item_failed_permanent`] — terminal failure
//!   (cascades through `dependents_of`).
//! * [`PendingPool::on_item_failed_pending_retry`] — terminal failure
//!   whose permanence awaits the phase's drain-edge retry decision
//!   (soft marker; no cascade yet).
//! * [`PendingPool::finalize_soft_failures`] — drain-edge promotion of
//!   a phase's soft failures to permanent (+ the dependents cascade).
//! * [`PendingPool::mark_in_flight`] — out-of-band dispatch from
//!   the promoted-secondary path.
//! * [`PendingPool::requeue`] — transient retry, item back to FRONT.
//! * [`PendingPool::reinject`] — manager-side retry of already-finished
//!   task; item to BACK; phase unwinds to `Active` if needed.
//! * [`PendingPool::drain_queued`] — bulk move of queued items.
//! * [`PendingPool::release_worker`] — worker death / departure.
//! * [`PendingPool::poll_drain_transitions`] — one-shot drained list.
//! * [`PendingPool::mark_phase_done`] — caller-side acknowledgment of
//!   the `Drained → Done` transition; cascades activation.
//! * [`PendingPool::drain_empty_active_phases`] — startup helper for
//!   phases that began `Active` but never received items.
//! * `maybe_transition_drain` (private to the submodule) — the
//!   `Active → Draining → Drained` state machine; called by every
//!   path that may zero out queued + in-flight + blocked counts.
//! * `queued_count` (private to the submodule) — count of a phase's
//!   queued items that hold it open against the drain (the
//!   `counts_for_phase_drain` kinds; the affine ledger token is excluded).

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

use dynrunner_core::{Identifier, PhaseId, TaskInfo, WorkerId};

use super::pool::PendingPool;
use super::types::{Bucket, PhaseState, affinity_key};

impl<I: Identifier> PendingPool<I> {
    /// Notify the pool that an item completed successfully (or that
    /// the caller wants the in-flight count decremented without
    /// recording a per-task completion — pass `task_id = None`).
    ///
    /// * Decrements `in_flight_per_phase` and may transition the phase
    ///   `Draining → Drained`.
    /// * If `task_id` is `Some(id)`: marks that task as completed and
    ///   walks `dependents_of[id]`. Any dependent whose final
    ///   unresolved prereq this resolves moves from `blocked` to the
    ///   FRONT of its bucket (matching `requeue` semantics so freshly
    ///   unblocked tasks dispatch ahead of newly-extended items in the
    ///   same bucket). Dependent phases that had been `Draining` due
    ///   to all queued items being blocked elsewhere flip back to
    ///   `Active`.
    ///
    /// Pass `None` for transient failures (Recoverable retry pending):
    /// the in-flight count drops so the phase machine progresses, but
    /// no per-task completion is recorded — dependents stay blocked
    /// until either a successful retry calls this method with
    /// `Some(id)` or a permanent-fail cascade is invoked via
    /// `on_item_failed_permanent`.
    pub fn on_item_finished(&mut self, phase_id: &PhaseId, task_id: Option<&str>) {
        if let Some(c) = self.in_flight_per_phase.get_mut(phase_id) {
            let was = *c;
            *c = c.saturating_sub(1);
            tracing::debug!(
                phase = %phase_id,
                new_in_flight = *c,
                saturated = was == 0,
                "pool: in_flight -1 (on_item_finished)"
            );
        }
        if let Some(id) = task_id {
            self.in_flight_tasks.remove(id);
            self.resolve_completed_dependents(id);
        }
        self.maybe_transition_drain(phase_id);
    }

    /// Record an AFFINE prereq's FIRST per-secondary terminal — the
    /// phase-neutral pool notification fired by the manager's
    /// `handle_affine_task_complete` once the import's first run originates its
    /// global `TaskCompleted`.
    ///
    /// DISTINCT from [`Self::on_item_finished`]: an affine import is uncounted
    /// for phase drain (never `mark_in_flight`'d, excluded from `queued_count`),
    /// so there is NO `in_flight_per_phase` decrement to do; and its dependents'
    /// readiness is the per-secondary bitvector, NOT this global terminal, so it
    /// must NOT run `resolve_completed_dependents` (which would unblock a
    /// dependent against a single global terminal — the head-of-line-blocking
    /// the per-secondary model removes). The ONLY two effects are:
    ///   1. record the affine `task_id` terminal in `completed_tasks` so
    ///      [`Self::phase_has_live_affine_prereq`] flips `false` for its phase
    ///      (the import is no longer live); and
    ///   2. re-run [`Self::maybe_transition_drain`] for its phase so the
    ///      now-genuinely-drained affine-only phase is pushed onto
    ///      `drained_pending` for the manager's drain-edge — the re-trigger the
    ///      affine terminal otherwise lacks (it is phase-neutral and emits only
    ///      `TasksAdded`, which never re-polls the lifecycle).
    ///
    /// Idempotent: a re-delivered same-/other-secondary terminal re-inserts an
    /// already-present id (a no-op `HashSet` insert) and re-runs an idempotent
    /// transition. Only the FIRST run reaches here (the manager gates on the
    /// global `TaskState` not-yet-terminal), but the method is safe regardless.
    pub fn note_affine_terminal(&mut self, phase_id: &PhaseId, task_id: &str) {
        self.completed_tasks.insert(task_id.to_string());
        self.maybe_transition_drain(phase_id);
    }

    /// Record an AFFINE prereq's GENUINE GLOBAL terminal FAILURE — the
    /// permanent-failure twin of [`Self::note_affine_terminal`], fired by the
    /// manager's `handle_affine_task_failed` when the import can no longer run
    /// on any roster secondary (the gate is all-eligible-`Failed`).
    ///
    /// Like the success twin, this is phase-NEUTRAL and runs NO dependent
    /// cascade (the manager already terminal-fails the now-`Unsatisfiable`
    /// dependents through `fast_fail_affine_dependents_if_unsatisfiable`; the
    /// per-secondary bitvector — not this global terminal — is what dependent
    /// dispatch reads). The ONLY two effects:
    ///   1. record the affine `task_id` in `failed_tasks` so
    ///      [`Self::phase_has_live_affine_prereq`] flips `false` for its phase
    ///      (a genuinely-failed import is no longer LIVE — its terminal is
    ///      reached); and
    ///   2. re-run [`Self::maybe_transition_drain`] for its phase so the
    ///      now-drained affine-only phase is pushed onto `drained_pending`.
    ///
    /// Without this, a genuinely-failed affine would hold its phase's Gate B
    /// (`phase_has_live_affine_prereq` reads `!failed_tasks.contains`) forever —
    /// the failed-terminal twin the complete path already had. Idempotent: a
    /// `HashSet` insert + an idempotent transition.
    pub fn note_affine_failed(&mut self, phase_id: &PhaseId, task_id: &str) {
        self.failed_tasks.insert(task_id.to_string());
        self.maybe_transition_drain(phase_id);
    }

    /// Record `id` completed and unblock every dependent whose final
    /// unresolved prereq this resolves: move it `blocked → FRONT of its
    /// bucket` (matching `requeue` so freshly-unblocked tasks dispatch ahead
    /// of newly-extended items), flip its phase `Draining → Active` if the
    /// phase was draining only because everything was blocked, and stale out
    /// any pending-drained record for it.
    ///
    /// The SINGLE dependent-unblock walk, owned by the in-flight terminal
    /// path ([`Self::on_item_finished`], which additionally owns the
    /// in-flight decrement). Collects the dependent ids first to avoid
    /// borrowing `self.dependents_of` while mutating `self.blocked` /
    /// `self.task_deps`.
    fn resolve_completed_dependents(&mut self, id: &str) {
        self.completed_tasks.insert(id.to_string());
        let dependents = self.dependents_of.remove(id).unwrap_or_default();
        for dep_id in dependents {
            let still_blocked = if let Some(remaining) = self.task_deps.get_mut(&dep_id) {
                remaining.remove(id);
                !remaining.is_empty()
            } else {
                // Already unblocked / not present — defensive no-op.
                continue;
            };
            if still_blocked {
                continue;
            }
            self.task_deps.remove(&dep_id);
            let item = match self.blocked.remove(&dep_id) {
                Some(it) => it,
                None => continue,
            };
            let dep_phase = item.phase_id.clone();
            if let Some(c) = self.blocked_per_phase.get_mut(&dep_phase) {
                *c = c.saturating_sub(1);
            }
            let key = (
                item.phase_id.clone(),
                item.type_id.clone(),
                affinity_key(&item),
            );
            self.buckets
                .entry(key)
                .or_insert_with(Bucket::new)
                .items
                .push_front(item);
            // Unblocking grew this phase's queue: if it was
            // `Draining` only because everything was blocked, flip
            // it back to `Active`. Mirrors `requeue` behaviour.
            if self.phase_state.get(&dep_phase) == Some(&PhaseState::Draining) {
                self.phase_state
                    .insert(dep_phase.clone(), PhaseState::Active);
            }
            // A drained-pending entry for this phase is now stale —
            // the phase is no longer drained.
            self.drained_pending.retain(|p| p != &dep_phase);
        }
    }

    /// Inverse of [`Self::resolve_completed_dependents`]: un-complete `id`
    /// and re-route every LIVE direct dependent so its dep on `id` becomes
    /// unmet again.
    ///
    /// Called by [`Self::reinject`] when an already-FINISHED task is
    /// re-run: its predecessor OUTPUT is being regenerated, so a dependent
    /// must not dispatch (nor unblock toward dispatch) against the
    /// stale/torn input. A no-op when `id` was never completed (the
    /// soft-failed / dormant revival cases had no unblock to invert).
    ///
    /// ## Two live-dependent shapes, ONE re-route
    /// `id`'s prior completion ran `resolve_completed_dependents`, which
    /// for EVERY direct dependent `b` removed `id` from `b`'s unmet set
    /// (`task_deps[b]`) and dropped `b` from the now-removed
    /// `dependents_of[id]`. Where `b` ended up split into two shapes that
    /// BOTH still carry a (now-stale) resolution of the dep on `id`:
    ///   * `b` had NO other unmet dep → it moved `blocked → a bucket`
    ///     (READY); its `task_deps` entry was deleted entirely.
    ///   * `b` had OTHER unmet deps → it STAYED in `blocked` with `id`
    ///     silently dropped from its `task_deps[b]` set (the hole a
    ///     queued-only scan would miss: when those other deps later
    ///     resolve, `b` would unblock against the regenerating `id`).
    ///
    /// Either way `b`'s declared `task_depends_on` (carried on its stored
    /// `TaskInfo`) still names `id`, so a single scan over BOTH the queued
    /// buckets and the `blocked` map finds every `b`. Each is extracted
    /// (tearing down any stale blocked-edges) and re-routed through the
    /// single edge-builder [`Self::commit_item`], which recomputes `b`'s
    /// FULL unmet set against the current `completed_tasks` — now that
    /// `id` is no longer completed, `id` re-enters that set and `b`'s
    /// edges (`dependents_of` / `task_deps` / `blocked` /
    /// `blocked_per_phase`) are rebuilt identically to ingest. Never a
    /// hand-rolled parallel edge builder.
    ///
    /// ## Scope + boundaries
    /// * DIRECT dependents only — matching the unblock walk's own scope.
    ///   A transitive `a → b → c` does not touch `c` here (it names `b`,
    ///   not `id`); the manager reinjects each regenerated task it wants
    ///   re-run, and re-blocking `b` does not disturb `c`.
    /// * A dependent ALREADY DISPATCHED (in flight, gone from both the
    ///   buckets and `blocked`) is not re-pulled — a running task cannot
    ///   be un-run.
    /// * A dependent with OTHER unmet deps is re-routed too (the blocked
    ///   shape above); `commit_item` re-blocks it on its FULL unmet set
    ///   (its old deps PLUS the freshly-unmet `id`) without double-counting.
    /// * A dependent that has itself gone terminal is in neither the
    ///   buckets nor `blocked`, so it is a no-op.
    /// * Diamonds (`id → b`, `id → c`) re-route both — both name `id`.
    ///
    /// Collects the matching ids first (an immutable scan over both
    /// sources), then extracts + recommits them, to avoid mutating the
    /// collections while iterating. Each re-route may empty its phase's
    /// queue, so a drain transition is re-evaluated per affected phase.
    fn reblock_dependents_on_uncompleted(&mut self, id: &str) {
        // OPT-2 — an affine token's GLOBAL terminal is STICKY: a
        // `SecondaryAffine` import has no regenerable global output (its
        // dependents' readiness is the per-secondary bitvector, NOT this
        // global terminal), so re-running it must NOT un-complete it and must
        // NOT re-block its dependents. Leave its `completed_tasks` entry intact
        // and return — keeping the affine terminal recorded so the pool's
        // affine guard (`phase_has_live_affine_prereq`) stays `false`. This is
        // the SAME established pattern the auto-resume reinject funnel already
        // applies (`primary/lifecycle/mutations.rs:77` skips `is_secondary_affine`
        // on reinject); restoring it here closes the lost-mirror window at the
        // un-complete source. Non-affine ids fall through to the unchanged
        // dependent-reblocking path below.
        if self.affine_prereq_ids.contains(id) {
            return;
        }
        // Only a previously-completed id has dependents to re-block.
        if !self.completed_tasks.remove(id) {
            return;
        }
        // Every LIVE direct dependent — queued OR blocked — whose declared
        // `task_depends_on` names `id` (bare task_id, the keying
        // `commit_item` resolves on). The blocked side closes the hole a
        // queued-only scan leaves: a dependent kept blocked by ANOTHER
        // unmet dep had `id` silently dropped from its `task_deps` at
        // unblock time and would otherwise resolve against the
        // regenerating `id` once that other dep completes.
        let names_id = |item: &TaskInfo<I>| item.task_depends_on.iter().any(|d| d.task_id == id);
        let reroute_ids: Vec<String> = self
            .buckets
            .values()
            .flat_map(|b| b.items.iter())
            .chain(self.blocked.values())
            .filter(|item| names_id(item.as_ref()))
            .map(|item| item.task_id.clone())
            .collect();
        if reroute_ids.is_empty() {
            return;
        }
        let mut affected_phases: HashSet<PhaseId> = HashSet::new();
        for dep_id in reroute_ids {
            // Extract the dependent from wherever it currently lives
            // (queued bucket or the blocked map, tearing down its stale
            // blocked-edges in the latter case), then re-route it through
            // the single edge-builder now that `id` is unresolved again —
            // it lands back in `blocked` against its full unmet set.
            if let Some(item) = self.extract_live_dependent(&dep_id) {
                affected_phases.insert(item.phase_id.clone());
                self.commit_item(item);
            }
        }
        // Re-routing dependents may have emptied a phase's queue while
        // leaving live blocked work behind: re-run the drain transition
        // so each touched phase's state reflects its new counts.
        for ph in &affected_phases {
            self.maybe_transition_drain(ph);
        }
    }

    /// Remove a live dependent `dep_id` from wherever it currently sits —
    /// a queued bucket OR the `blocked` map — returning its owned
    /// `TaskInfo` for an immediate re-route through [`Self::commit_item`].
    /// `None` if the id is in neither (already dispatched / terminal).
    ///
    /// The blocked-map branch tears down the item's STALE blocked-edges so
    /// the subsequent `commit_item` rebuilds them without duplication:
    /// `task_deps[dep_id]` is dropped, `dep_id` is removed from each of its
    /// recorded prereqs' `dependents_of` reverse lists, and
    /// `blocked_per_phase` is decremented. A queued item carries no
    /// blocked-edges (its `task_deps` entry was consumed when it unblocked
    /// into the bucket), so the bucket branch is a plain
    /// [`Self::take_first_match`] removal.
    ///
    /// Sole caller is [`Self::reblock_dependents_on_uncompleted`]; kept a
    /// named helper so the extract-then-rebuild symmetry with
    /// `commit_item` is explicit and the borrow of `self.blocked` is
    /// scoped away from the recommit.
    fn extract_live_dependent(&mut self, dep_id: &str) -> Option<Arc<TaskInfo<I>>> {
        // Queued: a plain removal — queued items hold no blocked-edges.
        if let Some(item) = self.take_first_match(|it| it.task_id == dep_id) {
            return Some(item);
        }
        // Blocked: remove the item and tear down its stale blocked-edges.
        let item = self.blocked.remove(dep_id)?;
        if let Some(c) = self.blocked_per_phase.get_mut(&item.phase_id) {
            *c = c.saturating_sub(1);
        }
        if let Some(deps) = self.task_deps.remove(dep_id) {
            for dep in deps {
                if let Some(rev) = self.dependents_of.get_mut(&dep) {
                    rev.retain(|d| d != dep_id);
                    if rev.is_empty() {
                        self.dependents_of.remove(&dep);
                    }
                }
            }
        }
        Some(item)
    }

    /// Notify the pool that a task has terminated PERMANENTLY (e.g.
    /// retry budget exhausted or a NonRecoverable error). Cascades
    /// the failure to every transitive dependent so dependents that
    /// can never succeed do not sit in `blocked` forever.
    ///
    /// Returns the `TaskInfo` of every cascaded dependent so the
    /// caller can update its own per-task ledgers (failed-tasks set,
    /// metrics, observability hooks). The caller's own task whose
    /// failure triggered this is NOT in the returned vec — it has
    /// already been removed from in-flight via the normal task-event
    /// path; this method just records its id and walks the cascade.
    ///
    /// Side effects:
    /// * `task_id` and every cascaded dependent id are added to
    ///   `failed_tasks`.
    /// * `in_flight_per_phase[phase_id]` is decremented by one (the
    ///   originating task was in-flight).
    /// * Cascaded dependents are removed from `blocked` and their
    ///   `blocked_per_phase` entries decremented.
    /// * Drain transitions fire for every phase whose blocked-set
    ///   was reduced (the originating phase plus every distinct
    ///   cascaded phase).
    pub fn on_item_failed_permanent(
        &mut self,
        phase_id: &PhaseId,
        task_id: &str,
    ) -> Vec<Arc<TaskInfo<I>>> {
        if let Some(c) = self.in_flight_per_phase.get_mut(phase_id) {
            *c = c.saturating_sub(1);
        }
        self.in_flight_tasks.remove(task_id);
        // The failure is permanent NOW — a still-pending soft marker for
        // this id (the retry-decision-pending state) is superseded.
        self.soft_failed.remove(task_id);
        self.failed_tasks.insert(task_id.to_string());

        let mut affected_phases: HashSet<PhaseId> = HashSet::new();
        affected_phases.insert(phase_id.clone());
        let cascaded = self.cascade_fail_dependents_of(task_id, &mut affected_phases);

        for ph in &affected_phases {
            self.maybe_transition_drain(ph);
        }
        cascaded
    }

    /// The permanent-failure dependents walk shared by
    /// [`Self::on_item_failed_permanent`] (immediate permanence — the
    /// consumer-command / extend-time class) and
    /// [`Self::finalize_soft_failures`] (drain-edge permanence — the
    /// wire-terminal class whose retry decision just declined).
    ///
    /// BFS over `dependents_of`. Every dependent reached is unreachable
    /// for any successful path — it cannot satisfy its dep on a
    /// permanently-failed prereq. Cascade-fail it: record in
    /// `failed_tasks`, drop its dep bookkeeping, remove it from
    /// `blocked` (collecting the `TaskInfo` for the caller's own
    /// ledgers), and accumulate every touched phase into
    /// `affected_phases` so the CALLER runs the drain transitions once
    /// after all roots are walked.
    fn cascade_fail_dependents_of(
        &mut self,
        task_id: &str,
        affected_phases: &mut HashSet<PhaseId>,
    ) -> Vec<Arc<TaskInfo<I>>> {
        let mut cascaded: Vec<Arc<TaskInfo<I>>> = Vec::new();
        let mut frontier: VecDeque<String> = VecDeque::new();
        frontier.push_back(task_id.to_string());
        while let Some(failed_id) = frontier.pop_front() {
            let dependents = self.dependents_of.remove(&failed_id).unwrap_or_default();
            for dep_id in dependents {
                if !self.failed_tasks.insert(dep_id.clone()) {
                    // Already cascaded via a different path — its
                    // blocked entry is gone too; skip.
                    continue;
                }
                // A cascaded dependent's own pending-retry marker (it
                // could itself have soft-failed earlier) is superseded
                // by the cascade's permanence.
                self.soft_failed.remove(&dep_id);
                self.task_deps.remove(&dep_id);
                if let Some(item) = self.blocked.remove(&dep_id) {
                    let dep_phase = item.phase_id.clone();
                    if let Some(c) = self.blocked_per_phase.get_mut(&dep_phase) {
                        *c = c.saturating_sub(1);
                    }
                    affected_phases.insert(dep_phase);
                    cascaded.push(item);
                }
                frontier.push_back(dep_id);
            }
        }
        cascaded
    }

    /// Notify the pool that a task's latest attempt terminally FAILED at
    /// the manager level, with the failure's PERMANENCE still pending the
    /// manager's per-phase retry decision (the retry buckets run at the
    /// phase's drain edge; a reinject there revives the task).
    ///
    /// The retry-pending twin of `on_item_finished(phase, None)`:
    /// decrements the phase's in-flight count and drops the in-flight
    /// id, but ADDITIONALLY records the id in the `soft_failed` marker
    /// set so the drain gate can discount blocked dependents doomed by
    /// this failure. Without the marker the dependents hold the phase in
    /// `Draining` forever, the drain edge (where the retry-or-cascade
    /// decision lives) is unreachable, and the run wedges — the
    /// blocked-dependent hang.
    ///
    /// Dependents are NOT cascaded here (the failure may yet be revived
    /// by a retry-bucket reinject); permanence is decided at the drain
    /// edge by [`Self::finalize_soft_failures`]. An id that is already
    /// permanently failed keeps its `failed_tasks` membership and gets
    /// no soft marker (idempotence against the immediate-permanence
    /// path having run first).
    pub fn on_item_failed_pending_retry(&mut self, phase_id: &PhaseId, task_id: &str) {
        if let Some(c) = self.in_flight_per_phase.get_mut(phase_id) {
            let was = *c;
            *c = c.saturating_sub(1);
            tracing::debug!(
                phase = %phase_id,
                new_in_flight = *c,
                saturated = was == 0,
                "pool: in_flight -1 (on_item_failed_pending_retry)"
            );
        }
        self.in_flight_tasks.remove(task_id);
        if !self.failed_tasks.contains(task_id) {
            self.soft_failed
                .insert(task_id.to_string(), phase_id.clone());
        }
        self.maybe_transition_drain(phase_id);
    }

    /// Promote `phase_id`'s soft (retry-pending) failures to PERMANENT
    /// and cascade-fail their transitive dependents.
    ///
    /// The drain-edge counterpart of [`Self::on_item_failed_pending_retry`]:
    /// the manager calls this once the phase's retry buckets have
    /// DECLINED at the drain edge (no candidates, or budget exhausted) —
    /// at that point no future pass can revive the phase's failures, so
    /// every dependent blocked on them is permanently unfulfillable and
    /// must stop holding the run open.
    ///
    /// Per finalized root: the id moves `soft_failed → failed_tasks` and
    /// the shared cascade walk collects its dependents. Returns
    /// `(root_task_id, cascaded_dependents)` pairs so the caller can
    /// attribute + account each dependent in its own ledgers (and
    /// broadcast the terminal state). Drain transitions run once for
    /// every phase the cascades touched. Empty when the phase holds no
    /// soft failures — the common no-failure drain edge stays free.
    pub fn finalize_soft_failures(
        &mut self,
        phase_id: &PhaseId,
    ) -> Vec<(String, Vec<Arc<TaskInfo<I>>>)> {
        let roots: Vec<String> = self
            .soft_failed
            .iter()
            .filter(|(_, p)| *p == phase_id)
            .map(|(id, _)| id.clone())
            .collect();
        if roots.is_empty() {
            return Vec::new();
        }
        let mut affected_phases: HashSet<PhaseId> = HashSet::new();
        let mut out: Vec<(String, Vec<Arc<TaskInfo<I>>>)> = Vec::with_capacity(roots.len());
        for root in roots {
            self.soft_failed.remove(&root);
            self.failed_tasks.insert(root.clone());
            let cascaded = self.cascade_fail_dependents_of(&root, &mut affected_phases);
            out.push((root, cascaded));
        }
        for ph in &affected_phases {
            self.maybe_transition_drain(ph);
        }
        out
    }

    /// Notify the pool that a task has been dispatched outside the
    /// `pop_for_worker` / `take_selected` path (which already do the
    /// in-flight bookkeeping). Pair with [`on_item_finished`] when the
    /// task completes. Used by the promoted secondary, which
    /// extracts items via [`super::PendingPool::take_first_match`] (a removal primitive
    /// that does not touch in-flight counters) but needs the phase
    /// machine to observe the dispatch so a `Draining` transition
    /// fires only after the cluster reports the item finished.
    pub fn mark_in_flight(&mut self, phase_id: &PhaseId) {
        let count = self
            .in_flight_per_phase
            .entry(phase_id.clone())
            .or_insert(0);
        *count += 1;
        tracing::debug!(
            phase = %phase_id,
            new_in_flight = *count,
            "pool: in_flight +1 (mark_in_flight)"
        );
    }

    /// Re-queue an item that needs retry (worker death, transient
    /// failure). Inserts at the FRONT of its `(phase, type, affinity)`
    /// bucket. Decrements the phase's in-flight count (the item was
    /// in-flight and is now back in the queue) and flips the phase
    /// `Draining → Active` if needed.
    ///
    /// A requeued item is immediately dispatch-eligible again. Readiness
    /// changes only on events, and re-dispatching a not-yet-placeable item
    /// is harmless — it is simply re-checked. Every dispatch is driven by a
    /// `TasksAdded` worker-management signal, never a per-task timer, so a
    /// bounce loop cannot hot-spin.
    pub fn requeue(&mut self, item: Arc<TaskInfo<I>>) {
        let phase_id = item.phase_id.clone();
        if let Some(c) = self.in_flight_per_phase.get_mut(&phase_id) {
            let was = *c;
            *c = c.saturating_sub(1);
            tracing::debug!(
                phase = %phase_id,
                new_in_flight = *c,
                saturated = was == 0,
                "pool: in_flight -1 (requeue)"
            );
        }
        let key = (
            item.phase_id.clone(),
            item.type_id.clone(),
            affinity_key(&item),
        );
        self.buckets
            .entry(key)
            .or_insert_with(Bucket::new)
            .items
            .push_front(item);
        if self.phase_state.get(&phase_id) == Some(&PhaseState::Draining) {
            self.phase_state.insert(phase_id, PhaseState::Active);
        }
    }

    /// Push `item` to the FRONT of its `(phase, type, affinity)` bucket
    /// WITHOUT touching the in-flight counter — the move primitive for the
    /// 5-min reconcile arm (#652 concern C).
    ///
    /// Distinct from [`Self::requeue`]: requeue is the inverse of a DISPATCH
    /// (the item WAS in flight, so it decrements the in-flight count). A
    /// reconcile-moved item was NEVER in flight (it sat blocked / queued
    /// waiting on an orphaned dep), so there is no in-flight count to undo —
    /// decrementing here would corrupt a sibling's slot. The item is pushed to
    /// the HEAD so the reconcile-surfaced work is re-evaluated before the rest of
    /// its bucket; the [`Self::take_at_if_ready`] pop-time guard (#652 D.1) then
    /// re-routes it (dispatch if its deps are in fact met, else re-block).
    ///
    /// Like `requeue`, a `Draining` phase is flipped back to `Active` so the
    /// re-surfaced item is dispatchable.
    pub fn push_to_queue_head(&mut self, item: Arc<TaskInfo<I>>) {
        let phase_id = item.phase_id.clone();
        let key = (
            item.phase_id.clone(),
            item.type_id.clone(),
            affinity_key(&item),
        );
        self.buckets
            .entry(key)
            .or_insert_with(Bucket::new)
            .items
            .push_front(item);
        if self.phase_state.get(&phase_id) == Some(&PhaseState::Draining) {
            self.phase_state.insert(phase_id, PhaseState::Active);
        }
    }

    /// Re-inject an item whose previous attempt has already been
    /// finalised via `on_item_finished` (so it is no longer counted as
    /// in-flight). Pushes to the BACK of its bucket and, if the phase
    /// has progressed past `Active` (`Draining`, `Drained`, or `Done`),
    /// flips it back to `Active` so the newly-injected item is
    /// dispatchable. Any pending drained notification for the phase
    /// is cancelled (the phase is no longer drained).
    ///
    /// This is the right hook for manager-side retry queues that
    /// re-introduce already-finished tasks: the in-flight count is
    /// untouched, only the queue contents and phase state move.
    /// Reinjecting after `Done` unwinds the phase into `Active`
    /// without re-firing `on_phase_start` — the manager owns
    /// lifecycle bookkeeping (phase_started_emitted) and decides
    /// whether the second-pass dispatch is observable to consumers.
    ///
    /// ## Re-block of dependents (the inverse of completion's unblock)
    /// If the reinjected `item` was already recorded COMPLETED (a
    /// finished task being re-run, the dominant reinject case), its
    /// completion had previously resolved its dep on each dependent via
    /// [`Self::resolve_completed_dependents`]. Re-running the task means
    /// its predecessor OUTPUT is being regenerated, so every still-LIVE
    /// direct dependent must have that dep made unmet again — otherwise
    /// it would dispatch (or later unblock toward dispatch) against the
    /// stale/torn output. [`Self::reblock_dependents_on_uncompleted`]
    /// (the symmetric inverse of the unblock) re-routes both shapes the
    /// dependent could be in: a READY one (queued) and a still-BLOCKED
    /// one whose other deps kept it blocked. A dependent already
    /// dispatched (gone from both the buckets and `blocked`) cannot be
    /// un-run and is left alone — the documented boundary. A reinject of
    /// a NEVER-completed task (the soft-failed / dormant revival cases)
    /// had no unblock to invert, so the inverse is a no-op for it.
    pub fn reinject(&mut self, item: Arc<TaskInfo<I>>) {
        let phase_id = item.phase_id.clone();
        // Revival: a reinjected task's pending-retry failure marker is
        // void — the retry bucket granted it another pass, so blocked
        // dependents are once again legitimately waiting on it and the
        // drain gate must stop discounting them. The dormant
        // registration (an operator-reinjectable Unfulfillable root
        // seeded at hydration) is likewise void: the id is known again
        // through its bucket entry from here on.
        self.soft_failed.remove(&item.task_id);
        self.dormant_tasks.remove(&item.task_id);
        // Un-complete: if this id was a finished task being re-run, its
        // prior completion had unblocked its dependents — invert that
        // now (re-block the still-ready ones) before it re-enters the
        // queue. No-op when the id was never completed.
        self.reblock_dependents_on_uncompleted(&item.task_id);
        let key = (
            item.phase_id.clone(),
            item.type_id.clone(),
            affinity_key(&item),
        );
        self.buckets
            .entry(key)
            .or_insert_with(Bucket::new)
            .items
            .push_back(item);
        let current = self.phase_state.get(&phase_id).copied();
        if matches!(
            current,
            Some(PhaseState::Draining | PhaseState::Drained | PhaseState::Done)
        ) {
            self.phase_state
                .insert(phase_id.clone(), PhaseState::Active);
            // If it was queued for drain notification, drop that entry —
            // the phase is no longer drained.
            self.drained_pending.retain(|p| p != &phase_id);
        }
    }

    /// Drain all currently queued items from the pool (without touching
    /// in-flight counts or phase state). Used by managers that need to
    /// move leftover queued items into a side queue between manager-
    /// internal phase transitions (e.g. moving NoFit items from the
    /// main phase queue into an "unassigned" bucket).
    pub fn drain_queued(&mut self) -> Vec<Arc<TaskInfo<I>>> {
        let mut out = Vec::new();
        for bucket in self.buckets.values_mut() {
            while let Some(item) = bucket.items.pop_front() {
                out.push(item);
            }
        }
        out
    }

    /// Worker died / left — clear its affinity record and remove it
    /// from any bucket's `pinned_workers`.
    ///
    /// Items the worker was processing are re-queued via separate
    /// `requeue` calls from the manager — that concern is not the
    /// pool's.
    pub fn release_worker(&mut self, worker_id: WorkerId) {
        if let Some(Some(key)) = self.worker_affinity.remove(&worker_id) {
            if let Some(bucket) = self.buckets.get_mut(&key) {
                bucket.pinned_workers.retain(|w| *w != worker_id);
            }
        } else {
            // Worker had no recorded affinity; ensure no bucket holds
            // a stale reference to it (defensive, cheap given the
            // soft-pin invariant only writes via take_from_bucket).
            for bucket in self.buckets.values_mut() {
                bucket.pinned_workers.retain(|w| *w != worker_id);
            }
        }
    }

    /// 5-min reconcile sweep for the `blocked` map (#652 concern C) — the ONE
    /// level-net that replaces the deleted per-task DispatchBackoff. A blocked
    /// dependent is ORPHANED when one of its unmet deps will never resolve: the
    /// dep is neither `completed` (the pool's own ledger) NOR alive per the
    /// caller's `dep_alive` predicate (in-flight on a reachable secondary). Such
    /// an entry's unblock event will never come — its dep's holder died, or a
    /// completion was lost — so it would sit blocked forever (the dropped-task
    /// net the deleted backoff used to recover).
    ///
    /// Each orphaned entry is REMOVED (its `blocked` / `dependents_of` /
    /// `task_deps` / `blocked_per_phase` edges torn down identically to a normal
    /// unblock) and its `TaskInfo` RETURNED so the caller pushes it to the
    /// general-queue HEAD ([`Self::push_to_queue_head`]), where the idempotent
    /// pop-time re-check (#652 D.1) re-routes it: dispatch if its deps are in
    /// fact met now, else re-block. A blocked entry whose every unmet dep is
    /// `completed`-or-alive is HEALTHY (its unblock is still coming) and is left
    /// untouched.
    ///
    /// `dep_alive(dep_task_id)` is the caller-supplied "this dep is in-flight on
    /// a reachable secondary" predicate (the manager owns the in-flight ledger +
    /// roster). The pool ORs it with its own `completed_tasks` membership, so the
    /// caller need only answer the in-flight half. Pure read of the caller's
    /// predicate per unmet dep; no dep-resolution logic is re-implemented here
    /// (the dep set is the same `task_deps` the ingest router built).
    pub fn reconcile_blocked<P>(&mut self, dep_alive: P) -> Vec<Arc<TaskInfo<I>>>
    where
        P: Fn(&str) -> bool,
    {
        // Identify orphaned blocked ids: any whose `task_deps` set holds a dep
        // that is neither completed nor alive. Collected first (the removal
        // borrows `self` mutably and tears down edges).
        let orphaned_ids: Vec<String> = self
            .blocked
            .keys()
            .filter(|id| {
                self.task_deps
                    .get(*id)
                    .is_some_and(|deps| {
                        deps.iter().any(|dep| {
                            !self.completed_tasks.contains(dep.as_str()) && !dep_alive(dep)
                        })
                    })
            })
            .cloned()
            .collect();

        let mut orphaned = Vec::new();
        for id in orphaned_ids {
            // Tear down the orphan's blocked-edges identically to a normal
            // unblock (the `extract_live_dependent` blocked branch), then return
            // its TaskInfo for general-queue-head re-routing.
            let Some(item) = self.blocked.remove(&id) else {
                continue;
            };
            if let Some(c) = self.blocked_per_phase.get_mut(&item.phase_id) {
                *c = c.saturating_sub(1);
            }
            if let Some(deps) = self.task_deps.remove(&id) {
                for dep in deps {
                    if let Some(rev) = self.dependents_of.get_mut(&dep) {
                        rev.retain(|d| d != &id);
                        if rev.is_empty() {
                            self.dependents_of.remove(&dep);
                        }
                    }
                }
            }
            orphaned.push(item);
        }
        orphaned
    }

    /// Return the set of phases that just transitioned to `Drained`
    /// since the last call. One-shot per phase: once a phase is
    /// returned here, it is not re-emitted on subsequent polls
    /// (the phase stays in `Drained` until `mark_phase_done`).
    pub fn poll_drain_transitions(&mut self) -> Vec<PhaseId> {
        std::mem::take(&mut self.drained_pending)
    }

    /// Whether any phase is currently queued on `drained_pending` awaiting the
    /// manager's drain-edge cascade — a NON-destructive peek (unlike
    /// [`Self::poll_drain_transitions`], which takes the queue). The manager's
    /// bounded re-surface drive uses this to decide whether
    /// [`Self::drain_empty_active_phases`] /
    /// [`Self::resurface_drained_pending`] produced a phase to re-enter the
    /// cascade for, WITHOUT consuming the queue the cascade's own
    /// `poll_drain_transitions` must then read.
    pub fn has_drained_pending(&self) -> bool {
        !self.drained_pending.is_empty()
    }

    /// Phases that own a retry-pending (`soft_failed`) root AND have no
    /// LIVE WORK OF THEIR OWN — `queued_count == 0` and `in_flight == 0` —
    /// REGARDLESS of any foreign-phase-blocked dependents that
    /// [`Self::live_blocked_count`] would otherwise count as live work.
    ///
    /// # Why this exists — the cross-phase drain-edge deadlock
    ///
    /// `maybe_transition_drain` keeps a phase out of `Drained` while it has
    /// LIVE blocked work, and `live_blocked_count` treats a dependent
    /// blocked on a soft root in a DIFFERENT phase as live (that root's
    /// retry decision belongs to the OTHER phase's drain edge, and a later
    /// revival there must not be stranded). That is correct in isolation,
    /// but it can form a CYCLE: phase A owns soft root `a` and a dependent
    /// blocked on phase B's soft root `b`; phase B owns soft root `b` and a
    /// dependent blocked on `a`. Each phase's blocked dependent keeps it
    /// out of `Drained`, so NEITHER reaches the drain edge where its own
    /// soft root would be finalized — neither promotes, neither cascades,
    /// the run hangs forever.
    ///
    /// This predicate breaks the cycle by surfacing such a phase to the
    /// MANAGER, which owns the retry-EXHAUSTION decision the pool cannot
    /// see. The manager runs this phase's retry buckets exactly as it does
    /// at a normal drain edge: if a bucket still has budget the soft root
    /// is REVIVABLE — the manager reinjects it and does NOT finalize (so a
    /// later success is never stranded). Only if every bucket DECLINES
    /// (exhausted) does the manager call `finalize_soft_failures`, moving
    /// the soft root to `failed_tasks` (permanent) and cascading its
    /// dependents — at which point the OTHER phase's dependent sees a
    /// genuinely `is_dead_ended` prereq via the EXISTING `live_blocked_count`
    /// path and drains naturally. Only the FIRST phase in the cycle needs
    /// this surfacing; the rest fall out of the normal machinery.
    ///
    /// The pool stays the sole owner of the COUNTERS and drain state and
    /// learns NOTHING about retry budgets — it only reports the structural
    /// fact "this phase has no live work of its own yet still owns a soft
    /// root". A phase that has NO soft roots, or still owns queued /
    /// in-flight work, is never returned (it reaches its drain edge through
    /// the ordinary `maybe_transition_drain` path).
    pub fn phases_pending_soft_finalize(&self) -> Vec<PhaseId> {
        let mut phases: Vec<PhaseId> = self
            .soft_failed
            .values()
            .filter(|phase_id| {
                matches!(
                    self.phase_state.get(*phase_id),
                    Some(PhaseState::Active | PhaseState::Draining)
                ) && self.queued_count(phase_id) == 0
                    && self.in_flight(phase_id) == 0
            })
            .cloned()
            .collect();
        phases.sort();
        phases.dedup();
        phases
    }

    /// Phases that SHOULD have reached the manager's drain edge but are
    /// stranded short of it — the structural fact a level-triggered
    /// re-evaluation needs to re-service. Two strands:
    ///
    ///   * `Active` / `Draining` AND all-clear: `queued_count == 0`,
    ///     `in_flight == 0`, no LIVE blocked work (`live_blocked_count == 0`),
    ///     every predecessor `Done`, and no live affine import
    ///     (`!phase_has_live_affine_prereq`). This is EXACTLY
    ///     `maybe_transition_drain`'s `Drained` condition: a phase in this
    ///     shape ought to be `Drained` and on `drained_pending`, but its last
    ///     mutating event may have evaluated `maybe_transition_drain` at an
    ///     instant when one of those counters was momentarily unsettled, and
    ///     `maybe_transition_drain` only re-runs on a pool mutation — once the
    ///     event stream stops nothing re-checks it.
    ///   * `Drained` but not yet `Done`: the transition fired and the phase
    ///     was consumed off `drained_pending` by `poll_drain_transitions`, but
    ///     the manager's drained-phase cascade did not complete its
    ///     `mark_phase_done` (a flipped-then-consumed race). The
    ///     `!matches!(Active | Draining)` early-return in
    ///     `maybe_transition_drain` means `drain_empty_active_phases` will
    ///     never re-surface it, so it is reported here for the manager to
    ///     re-push.
    ///
    /// Reports the STRUCTURAL fact only — RE-EVALUATING the SAME gates the
    /// ordinary drain path uses, RELAXING none (an affine-only phase whose
    /// import has not run still reads `phase_has_live_affine_prereq == true`
    /// and is NOT returned; a phase with live blocked work or unfinished
    /// predecessors is NOT returned). The drain-edge twin of
    /// [`Self::phases_pending_soft_finalize`]: that surfaces a soft-root
    /// deadlock the ordinary poll cannot break, this surfaces a drain edge the
    /// ordinary poll lost track of. Drives the manager's bounded re-surface
    /// (and its level-trigger arm): empty ⇒ the arm disarms (no hot-spin).
    pub fn phases_stuck_drainable(&self) -> Vec<PhaseId> {
        let mut phases: Vec<PhaseId> = self
            .phase_state
            .iter()
            .filter(|(phase_id, state)| match state {
                PhaseState::Active | PhaseState::Draining => {
                    self.queued_count(phase_id) == 0
                        && self.in_flight(phase_id) == 0
                        && self.live_blocked_count(phase_id) == 0
                        && self.predecessors_done(phase_id)
                        && !self.phase_has_live_affine_prereq(phase_id)
                }
                // Flipped-then-consumed: drained but the manager's cascade
                // did not finish marking it done.
                PhaseState::Drained => true,
                // `Blocked` still awaits its predecessor edges; `Done` is
                // finished. Neither is stranded short of a drain edge.
                PhaseState::Blocked | PhaseState::Done => false,
            })
            .map(|(phase_id, _)| phase_id.clone())
            .collect();
        phases.sort();
        phases.dedup();
        phases
    }

    /// Re-push every phase currently in state `Drained` (the transition
    /// fired) that is not yet `Done` and is not already queued on
    /// `drained_pending` — the flipped-then-consumed defence the manager's
    /// bounded re-surface drive invokes. A `Drained` phase consumed off
    /// `drained_pending` by [`Self::poll_drain_transitions`] whose
    /// `mark_phase_done` never completed cannot be re-surfaced by
    /// [`Self::drain_empty_active_phases`] (`maybe_transition_drain`
    /// early-returns for a non-`Active`/`Draining` phase), so the pool — the
    /// sole owner of `drained_pending` — re-queues it here. Idempotent: a
    /// phase still on `drained_pending` is not duplicated, an `Active` /
    /// `Draining` / `Done` phase is untouched (the former two re-surface via
    /// `drain_empty_active_phases`, the latter is finished). Returns whether
    /// any phase was re-pushed.
    pub fn resurface_drained_pending(&mut self) -> bool {
        let to_repush: Vec<PhaseId> = self
            .phase_state
            .iter()
            .filter(|(phase_id, state)| {
                matches!(state, PhaseState::Drained) && !self.drained_pending.contains(phase_id)
            })
            .map(|(phase_id, _)| phase_id.clone())
            .collect();
        let surfaced = !to_repush.is_empty();
        for phase_id in to_repush {
            self.drained_pending.push(phase_id);
        }
        surfaced
    }

    /// Drop every `SecondaryAffine` ledger-token item still sitting in
    /// `phase_id`'s buckets — the phase-end cleanup of Model B's
    /// placement-readiness signal.
    ///
    /// The affine prereq stays in the pool as a non-worker-assignable ledger
    /// token (the placement-readiness signal `place_dependency_satisfied_affine_tasks`
    /// reads, and the per-secondary ledger), EXCLUDED from
    /// [`Self::queued_count`] so it never wedges the drain. It is the ONE
    /// queued item a drained phase legitimately still holds. But the
    /// placement-readiness scan reads ready-affine across ALL buckets (not
    /// phase-filtered), so a drained phase's stale token would bleed into a
    /// later phase's placement and re-place a prereq whose dependents are all
    /// terminal. Dropping it at the phase's `Drained → Done` edge (this
    /// phase's work is wholly done — every affine-dep work task dispatched +
    /// completed, so no remaining dependent needs the signal) keeps the scan
    /// phase-honest without phase-filtering it. Non-affine items are left
    /// untouched (a `Done` phase holds none of them anyway).
    fn drop_affine_items(&mut self, phase_id: &PhaseId) {
        for ((p, _, _), bucket) in self.buckets.iter_mut() {
            if p == phase_id {
                bucket
                    .items
                    .retain(|item| !item.kind.is_secondary_affine());
            }
        }
    }

    /// Mark a phase `Done` after the manager has fired
    /// `on_phase_end` for it. Activates any `Blocked` phase whose
    /// `depends_on` set is now fully `Done`.
    pub fn mark_phase_done(&mut self, phase_id: &PhaseId) {
        self.phase_state.insert(phase_id.clone(), PhaseState::Done);
        // Phase-end cleanup: the affine ledger token (kept in the bucket as
        // the placement-readiness signal, uncounted for drain) has served its
        // purpose now this phase's work is wholly done — drop it so it cannot
        // bleed into a later phase's (un-phase-filtered) placement scan.
        self.drop_affine_items(phase_id);
        // Activation pass: any Blocked phase whose deps are all Done
        // becomes Active. We do not recurse — a phase can only be
        // Done by an explicit `mark_phase_done` call, which the
        // manager will issue per phase.
        self.activate_phases_with_all_deps_done();
    }

    /// Seed a set of phases as already-`Done` at HYDRATION time, with NO
    /// `on_phase_end` re-fire. Sibling of [`Self::mark_phase_done`] for the
    /// failover-promotion resume path: a phase whose tasks are all terminal
    /// in the inherited CRDT was already drained AND had its `on_phase_end`
    /// fired by the run's original primary. The promoted primary's freshly
    /// built pool ([`Self::new`]) initialises every phase `Active`/`Blocked`
    /// from the dep graph alone — it cannot tell "completed-and-ended" from
    /// "never started" — so without this seed those completed phases
    /// re-`(0,0,0)`-drain through `maybe_transition_drain → poll_drain_transitions`
    /// and the manager RE-fires `on_phase_end` (re-spawning a consumer hook's
    /// children with the same deterministic identities → run-wide
    /// invalidation). Seeding them straight to `Done` keeps them out of
    /// `poll_drain_transitions` entirely (the `Drained → Done` edge the manager
    /// observes never occurs for a phase that starts `Done`).
    ///
    /// Distinct from `mark_phase_done` in PURPOSE and shape, not just timing:
    /// `mark_phase_done` is the RUNTIME acknowledgment "the manager just fired
    /// `on_phase_end` for this one drained phase"; this is the CONSTRUCTION
    /// seed "these phases already ended on a prior primary — do NOT fire it
    /// again." It takes the whole completed set at once and runs ONE
    /// convergent activation pass after marking them all `Done`, so a
    /// multi-level chain (A→B→C all complete, plus a live D depending on C)
    /// resolves regardless of iteration order — calling `mark_phase_done` per
    /// phase would leave a dependent un-activated if its other (also-done) dep
    /// hadn't been marked yet.
    ///
    /// Idempotent: a phase already `Done` stays `Done`; a phase not in the
    /// pool's `phase_state` is ignored.
    pub fn seed_completed_phases(&mut self, phases: impl IntoIterator<Item = PhaseId>) {
        for phase_id in phases {
            // Only seed phases the pool actually tracks; an inherited CRDT
            // phase absent from the dep-graph-derived phase set is not a pool
            // concern (defensive — the hydrate caller derives both from the
            // same CRDT, so this is a no-op in practice).
            if self.phase_state.contains_key(&phase_id) {
                self.phase_state.insert(phase_id, PhaseState::Done);
            }
        }
        // Single convergent activation pass: re-run until no further phase
        // flips, so a `Blocked` phase whose deps span SEVERAL just-seeded
        // `Done` phases activates even if the seed set was iterated in an
        // order that marked one of its deps last.
        loop {
            let flipped = self.activate_phases_with_all_deps_done();
            if !flipped {
                break;
            }
        }
    }

    /// Flip every `Blocked` phase whose `depends_on` set is now fully `Done`
    /// to `Active`. Returns `true` iff at least one phase flipped, so a
    /// convergence loop can re-run until the fixpoint. Shared by
    /// [`Self::mark_phase_done`] (one pass — a single phase just went `Done`)
    /// and [`Self::seed_completed_phases`] (looped — a whole set went `Done`
    /// at once). A phase with no recorded `depends_on` (`unwrap_or(true)`)
    /// activates immediately, matching the original `mark_phase_done`
    /// semantics.
    fn activate_phases_with_all_deps_done(&mut self) -> bool {
        let candidates: Vec<PhaseId> = self
            .phase_state
            .iter()
            .filter(|(_, s)| **s == PhaseState::Blocked)
            .map(|(p, _)| p.clone())
            .collect();
        let mut flipped = false;
        for p in candidates {
            let all_done = self
                .phase_deps
                .get(&p)
                .map(|deps| {
                    deps.iter()
                        .all(|d| self.phase_state.get(d) == Some(&PhaseState::Done))
                })
                .unwrap_or(true);
            if all_done {
                self.phase_state.insert(p, PhaseState::Active);
                flipped = true;
            }
        }
        flipped
    }

    /// Mark every currently-`Active` or `Draining` phase that has no
    /// queued AND no in-flight items as `Drained`, pushing each onto
    /// `drained_pending` so the manager's `process_phase_lifecycle`
    /// pass observes them and cascades into `mark_phase_done` plus
    /// dependent-phase activation. Idempotent.
    ///
    /// Why this exists: `maybe_transition_drain` only runs when an
    /// item is removed from the pool (`take_at`) or finished
    /// (`on_item_finished`). A phase that started `Active` (because
    /// it had no upstream deps) but never received any items would
    /// otherwise stay `Active` forever, holding `Blocked` dependents
    /// that own the actual work. Multi-phase task definitions where
    /// every item lives in a non-zero-indexed phase trip this on
    /// startup; so does any run where `--skip-existing` (or
    /// equivalent task-side filtering) leaves an early phase
    /// completely empty.
    ///
    /// Callers should invoke this after the initial `extend()` and
    /// inside the lifecycle cascade in the manager — newly-`Active`
    /// dependents may themselves be empty and require the same
    /// transition before the cascade can continue.
    pub fn drain_empty_active_phases(&mut self) {
        let candidates: Vec<PhaseId> = self
            .phase_state
            .iter()
            .filter(|(_, s)| matches!(**s, PhaseState::Active | PhaseState::Draining))
            .map(|(p, _)| p.clone())
            .collect();
        for p in &candidates {
            self.maybe_transition_drain(p);
        }
    }

    /// Predicate twin of `cluster_state.phase_boundary_open` (#584) on
    /// the pool's `PhaseState::Done` set: returns `true` iff every
    /// `phase_deps[phase]` entry has reached `Done`, or `phase` has no
    /// recorded deps. The pool's `Done` set is the per-phase
    /// counterpart of `cluster_state.phases_ended` — both are flipped
    /// at the same site (`mark_phase_done` runs immediately after the
    /// manager originates the `PhaseEnded` mutation, see
    /// `process_phase_lifecycle`), so this predicate observes the same
    /// edge the manager's gate consults. Used by `maybe_transition_drain`
    /// to block the spurious `(0,0,0) → Drained` transition on a
    /// barrier=False phase whose predecessor hasn't finished injecting
    /// (#588). A phase with no recorded deps is unconditionally open
    /// (matches `phase_boundary_open` and `activate_phases_with_all_deps_done`).
    fn predecessors_done(&self, phase_id: &PhaseId) -> bool {
        match self.phase_deps.get(phase_id) {
            None => true,
            Some(deps) => deps
                .iter()
                .all(|d| self.phase_state.get(d) == Some(&PhaseState::Done)),
        }
    }

    /// Inspect a phase to decide if it should transition between
    /// `Active`, `Draining`, and `Drained`. Idempotent — safe to call
    /// from anywhere a relevant counter changed.
    ///
    /// A phase is `Drained` only when ALL three of `queued`,
    /// `in_flight`, AND `blocked_per_phase` are zero — a non-zero
    /// blocked count means the phase still has items waiting on
    /// unresolved task-level prereqs (typically in another phase) and
    /// must not be considered done. `Draining` covers the case where
    /// the queue is empty but in-flight or blocked items remain.
    ///
    /// # Phase-boundary gate (#588)
    ///
    /// A phase whose predecessor phases have not yet reached `Done`
    /// MUST NOT transition `Active → Drained` even when its counters
    /// read `(0,0,0)`. The barrier=False pattern (`set_no_barrier_phases`)
    /// flips dependent phases `Blocked → Active` BEFORE their
    /// predecessors complete so streamed-injected tasks can begin
    /// dispatching ahead of the predecessor's `PhaseEnded` — the pool
    /// is legitimately empty BY DESIGN while the predecessor is still
    /// producing work for it (the dep_graph + `on_phase_end` streaming
    /// pattern). Transitioning that phase `Drained` here lets the
    /// manager's drain-edge handler observe a `(0 completed, 0 failed)`
    /// phase, fire `on_phase_end` against an empty ledger, and emit
    /// `RunShouldFail` ("phase reached drain with no terminal outcome")
    /// — the consumer's BUILD phase aborting at init within ~5s before
    /// dep_graph injected anything. This is the structural twin of the
    /// `phase_boundary_open` gate at the manager's `phase_can_proceed`
    /// (#584): the manager's gate stops `PhaseEnded` origination on the
    /// LIVE / drain path, this gate stops the spurious `Drained`
    /// transition that would feed that path with a zero-tally phase
    /// before its predecessor finished injecting.
    ///
    /// The gate is keyed on the pool's `PhaseState::Done` — the
    /// per-phase counterpart of `cluster_state.phases_ended` set by
    /// `mark_phase_done` AT the manager's `PhaseEnded` origination
    /// site, so the two views are paired. Once the predecessor reaches
    /// `Done`, the outer cascade in `process_phase_lifecycle` re-runs
    /// `drain_empty_active_phases`, which re-invokes this and lets the
    /// held-back phase transition `Drained` legitimately (empty-and-
    /// deps-done is the genuine `may_be_empty` / structural-leaf case
    /// the manager surfaces).
    pub(super) fn maybe_transition_drain(&mut self, phase_id: &PhaseId) {
        let current = match self.phase_state.get(phase_id).copied() {
            Some(s) => s,
            None => return,
        };
        // Only meaningful transitions are out of Active or Draining.
        if !matches!(current, PhaseState::Active | PhaseState::Draining) {
            return;
        }
        let queued = self.queued_count(phase_id);
        let in_flight = self.in_flight(phase_id);
        let blocked = self.blocked_per_phase.get(phase_id).copied().unwrap_or(0);

        // #588 — phase-boundary gate. Compute the predecessor-done
        // predicate ONCE here and consult it on every arm that would
        // flip `Drained`: an empty phase whose predecessor hasn't
        // reached `Done` is empty-by-design-pending-injection, NOT
        // genuinely drained. Holding it at its current state (Active or
        // Draining) is safe — the manager's lifecycle cascade re-runs
        // `drain_empty_active_phases` after every `mark_phase_done`, so
        // the held-back phase re-evaluates as soon as the predecessor
        // edge closes. The `Active` and `Draining` arms are unaffected
        // because they carry live work (queued / in_flight / live
        // blocked); the gate only suppresses the spurious `Drained`
        // edge that feeds the manager's drain-guard with a zero-tally
        // phase before its predecessor finished injecting.
        let predecessors_done = self.predecessors_done(phase_id);

        // Model-B affine guard. A phase holding a LIVE (non-terminal)
        // `SecondaryAffine` import is NOT drained — the import is real work
        // that has not reached a terminal. Its bucket token is uncounted in
        // `queued` (and it is never `in_flight`/`blocked`), so the counters
        // alone read `(0,0,0)` for an affine-only phase from SEED time; this
        // guard suppresses the `Drained`-producing arms until the import's
        // first per-secondary run records its terminal via
        // `note_affine_terminal`, which re-runs this transition. The pool-side
        // counterpart of the CRDT rollup's `has_live` (the manager's
        // `phase_can_proceed` arm), keeping the pool the single owner of the
        // drain decision rather than the manager re-deciding it. The non-
        // `Drained` arms (`Active` / `Draining` / hold-`current`) are
        // unaffected — they already keep a phase with live work open.
        let drained_eligible = !self.phase_has_live_affine_prereq(phase_id);

        let next = match (queued, in_flight, blocked) {
            (0, 0, 0) if predecessors_done && drained_eligible => PhaseState::Drained,
            (0, 0, 0) => current,
            // Blocked items remain, but every one of them is DOOMED by a
            // dead prereq (final-failed anywhere, or soft-failed in THIS
            // phase — see `live_blocked_count`): they are not live work,
            // they are dependents awaiting the drain edge's
            // retry-or-cascade decision. Counting them as live would make
            // the drain edge unreachable — the retry buckets and the
            // permanent-failure finalization both run AT that edge, so
            // the phase would hold itself open waiting for a decision
            // that only the drain edge can take (the blocked-dependent
            // run-wedge). The manager's drain-edge handler either
            // reinjects the root (flipping the phase back to `Active`,
            // dependents stay blocked) or finalizes
            // (`finalize_soft_failures` cascade-fails the dependents
            // before `on_phase_end` fires).
            //
            // A live affine import suppresses this drain arm too (`Draining`
            // is the honest state: the queue is empty of counted work but the
            // import is still live), so a phase whose blocked dependents are
            // all doomed yet still holds a live import waits for the import.
            (0, 0, _) if drained_eligible && self.live_blocked_count(phase_id) == 0 => {
                PhaseState::Drained
            }
            (0, _, _) => PhaseState::Draining,
            (_, _, _) => PhaseState::Active,
        };
        if next != current {
            self.phase_state.insert(phase_id.clone(), next);
            if next == PhaseState::Drained {
                // One-shot record. Avoid duplicates if this method
                // somehow runs twice in a row (it shouldn't, but
                // be defensive).
                if !self.drained_pending.contains(phase_id) {
                    self.drained_pending.push(phase_id.clone());
                }
            }
        }
    }

    /// Count of `phase_id`'s blocked items that are LIVE — i.e. NOT
    /// doomed by a dead prerequisite. A prereq is dead when it is
    /// final-failed (`failed_tasks`, any phase) or soft-failed in
    /// `phase_id` ITSELF (`soft_failed` — its retry decision happens at
    /// THIS phase's drain edge, which is exactly the edge this count
    /// gates). A soft failure in ANOTHER phase does NOT doom for this
    /// gate: that phase's own drain edge owns the retry-or-cascade
    /// decision, and its finalization cascade will reach our dependents
    /// (re-running our drain transition) if it declines.
    ///
    /// Doom propagates transitively through the blocked graph — a
    /// dependent of a doomed blocked item is itself doomed (ANY dead or
    /// doomed dep suffices: the item can never satisfy ALL deps, the
    /// same any-dep rule the permanent cascade walks) — computed as a
    /// fixpoint over the WHOLE blocked map because chains may cross
    /// phases. Cost is bounded by `blocked.len()` per iteration and the
    /// caller only evaluates it on the candidate-wedge shape
    /// (`queued == 0 && in_flight == 0 && blocked > 0`).
    fn live_blocked_count(&self, phase_id: &PhaseId) -> usize {
        // Permanent failure (`failed_tasks`, any phase) is the canonical
        // [`Self::is_dead_ended`] leaf (one owner of "never runnable
        // again"); ADD the drain-edge-local clause — a `soft_failed`
        // prereq IN `phase_id` ITSELF, dead for THIS gate because this
        // phase's drain edge is its retry-decision point. A `soft_failed`
        // in ANOTHER phase is NOT dead here (its own drain edge decides).
        let is_dead = |id: &str| {
            self.is_dead_ended(id) || self.soft_failed.get(id).is_some_and(|p| p == phase_id)
        };
        let mut doomed: HashSet<&str> = HashSet::new();
        loop {
            let mut changed = false;
            for id in self.blocked.keys() {
                if doomed.contains(id.as_str()) {
                    continue;
                }
                // No dep entry (defensive — a blocked item always has
                // one): treat as live.
                let Some(deps) = self.task_deps.get(id) else {
                    continue;
                };
                if deps
                    .iter()
                    .any(|d| is_dead(d) || doomed.contains(d.as_str()))
                {
                    doomed.insert(id.as_str());
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }
        self.blocked
            .values()
            .filter(|it| it.phase_id == *phase_id && !doomed.contains(it.task_id.as_str()))
            .count()
    }

    /// Count of `phase_id`'s queued items that hold the phase open against
    /// the drain transition — the items the phase must still consume from its
    /// buckets before it can drain.
    ///
    /// Counts only items for which [`dynrunner_core::TaskKind::counts_for_phase_drain`]
    /// holds: `Work` (dispatched to a worker) and `Setup` (consumed from the
    /// bucket by its in-process executor) both count — while either sits
    /// queued, the phase has outstanding bucket work. A `SecondaryAffine`
    /// ledger token does NOT: under Model B it is a non-worker-assignable
    /// placement-readiness signal whose per-secondary runs are driven
    /// off-queue by the affine scheduler + bitvector and is never consumed
    /// from the bucket on a drain path, so counting it would pin
    /// `queued_count > 0` forever and the phase would never drain (the
    /// affine-dep producer's `on_phase_end` would never fire). The drain-side
    /// counterpart of the dispatch view's `is_worker_assignable` gate: an item
    /// the phase will never consume from its bucket must not be counted as
    /// queued work the drain waits on.
    pub(super) fn queued_count(&self, phase_id: &PhaseId) -> usize {
        self.buckets
            .iter()
            .filter(|((p, _, _), _)| p == phase_id)
            .map(|(_, b)| {
                b.items
                    .iter()
                    .filter(|item| item.kind.counts_for_phase_drain())
                    .count()
            })
            .sum()
    }

    /// Whether `phase_id` still holds a LIVE (non-terminal) `SecondaryAffine`
    /// prereq token — the drain-detection's affine guard.
    ///
    /// Under Model B the affine prereq stays in its bucket as a
    /// non-worker-assignable LEDGER TOKEN, EXCLUDED from [`Self::queued_count`]
    /// (it is never consumed from the bucket on a drain path) and never
    /// `mark_in_flight`'d (its per-secondary runs are driven off-queue). So the
    /// `(queued, in_flight, blocked)` counters `maybe_transition_drain` keys on
    /// are all ZERO for a phase whose ONLY content is a no-dep affine import —
    /// at SEED time, BEFORE that import has been placed/dispatched/run. Without
    /// this guard the phase would flip `Drained` immediately and the manager's
    /// drain-edge `phase_can_proceed` would evaluate against a still-live import
    /// (its global `TaskState` is `Pending` until its FIRST per-secondary run
    /// originates a `TaskCompleted`), false-failing the run. A phase with a live
    /// import is NOT drained — it owns work that has not yet reached a terminal,
    /// exactly the invariant the CRDT rollup's `has_live` expresses; this is its
    /// pool-side counterpart, keyed on the pool's own terminal sets so the pool
    /// stays the single owner of the drain decision.
    ///
    /// "Live" = the token's `task_id` is in `affine_prereq_ids` but NOT yet in
    /// `completed_tasks` / `failed_tasks` — the affine first-run terminal is
    /// recorded into `completed_tasks` by [`Self::note_affine_terminal`] (the
    /// phase-neutral pool notification the manager fires from
    /// `handle_affine_task_complete`), which flips this predicate and re-runs
    /// the drain transition so the now-genuinely-drained phase is observed. The
    /// token stays in the bucket until `mark_phase_done → drop_affine_items`, so
    /// bucket-membership alone cannot distinguish "never run" from "first run
    /// done" — the terminal set is the discriminator.
    fn phase_has_live_affine_prereq(&self, phase_id: &PhaseId) -> bool {
        self.buckets
            .iter()
            .filter(|((p, _, _), _)| p == phase_id)
            .flat_map(|(_, b)| b.items.iter())
            .any(|item| {
                item.kind.is_secondary_affine()
                    && self.affine_prereq_ids.contains(item.task_id.as_str())
                    && !self.completed_tasks.contains(item.task_id.as_str())
                    && !self.failed_tasks.contains(item.task_id.as_str())
            })
    }

    /// Test-only observability seam for the otherwise-private affine drain
    /// guard [`Self::phase_has_live_affine_prereq`] — the regression tests for
    /// the affine-terminal mirror (OPT-1/OPT-2 + the failed-path twin) assert
    /// directly on Gate B for one phase. Production code reads the guard
    /// in-module; this widens NOTHING outside `cfg(test)`.
    #[cfg(test)]
    pub(crate) fn phase_has_live_affine_prereq_for_test(&self, phase_id: &PhaseId) -> bool {
        self.phase_has_live_affine_prereq(phase_id)
    }
}
