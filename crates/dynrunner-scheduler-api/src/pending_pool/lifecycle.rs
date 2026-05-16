//! Phase-state machine plus task-completion / failure / retry inputs:
//! every method that mutates `in_flight_per_phase`, `phase_state`,
//! `blocked_per_phase`, or the dependents walk lives here.
//!
//! Entry points:
//! * [`PendingPool::on_item_finished`] — terminal success.
//! * [`PendingPool::on_item_failed_permanent`] — terminal failure
//!   (cascades through `dependents_of`).
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
//! * `queued_count` (private to the submodule) — sum of queued items
//!   across all buckets of a phase.

use std::collections::{HashSet, VecDeque};

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
            self.completed_tasks.insert(id.to_string());
            // Walk dependents and possibly unblock them. Collect ids
            // first to avoid borrowing `self.dependents_of` while we
            // mutate `self.blocked` / `self.task_deps`.
            let dependents = self
                .dependents_of
                .remove(id)
                .unwrap_or_default();
            for dep_id in dependents {
                let still_blocked = if let Some(remaining) =
                    self.task_deps.get_mut(&dep_id)
                {
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
                    self.phase_state.insert(dep_phase.clone(), PhaseState::Active);
                }
                // A drained-pending entry for this phase is now stale —
                // the phase is no longer drained.
                self.drained_pending.retain(|p| p != &dep_phase);
            }
        }
        self.maybe_transition_drain(phase_id);
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
    ) -> Vec<TaskInfo<I>> {
        if let Some(c) = self.in_flight_per_phase.get_mut(phase_id) {
            *c = c.saturating_sub(1);
        }
        self.in_flight_tasks.remove(task_id);
        self.failed_tasks.insert(task_id.to_string());

        let mut cascaded: Vec<TaskInfo<I>> = Vec::new();
        let mut affected_phases: HashSet<PhaseId> = HashSet::new();
        affected_phases.insert(phase_id.clone());

        // BFS over `dependents_of`. Every dependent we reach is
        // unreachable for any successful path — it cannot satisfy its
        // dep on a permanently-failed prereq. Cascade-fail it.
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

        for ph in &affected_phases {
            self.maybe_transition_drain(ph);
        }
        cascaded
    }

    /// Notify the pool that a task has been dispatched outside the
    /// `pop_for_worker` / `take_from_view` path (which already do the
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
    pub fn requeue(&mut self, item: TaskInfo<I>) {
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
    pub fn reinject(&mut self, item: TaskInfo<I>) {
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
            .push_back(item);
        let current = self.phase_state.get(&phase_id).copied();
        if matches!(
            current,
            Some(PhaseState::Draining | PhaseState::Drained | PhaseState::Done)
        ) {
            self.phase_state.insert(phase_id.clone(), PhaseState::Active);
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
    pub fn drain_queued(&mut self) -> Vec<TaskInfo<I>> {
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

    /// Return the set of phases that just transitioned to `Drained`
    /// since the last call. One-shot per phase: once a phase is
    /// returned here, it is not re-emitted on subsequent polls
    /// (the phase stays in `Drained` until `mark_phase_done`).
    pub fn poll_drain_transitions(&mut self) -> Vec<PhaseId> {
        std::mem::take(&mut self.drained_pending)
    }

    /// Mark a phase `Done` after the manager has fired
    /// `on_phase_end` for it. Activates any `Blocked` phase whose
    /// `depends_on` set is now fully `Done`.
    pub fn mark_phase_done(&mut self, phase_id: &PhaseId) {
        self.phase_state.insert(phase_id.clone(), PhaseState::Done);
        // Activation pass: any Blocked phase whose deps are all Done
        // becomes Active. We do not recurse — a phase can only be
        // Done by an explicit `mark_phase_done` call, which the
        // manager will issue per phase.
        let candidates: Vec<PhaseId> = self
            .phase_state
            .iter()
            .filter(|(_, s)| **s == PhaseState::Blocked)
            .map(|(p, _)| p.clone())
            .collect();
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
            }
        }
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
        let blocked = self
            .blocked_per_phase
            .get(phase_id)
            .copied()
            .unwrap_or(0);

        let next = match (queued, in_flight, blocked) {
            (0, 0, 0) => PhaseState::Drained,
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

    /// Sum of queued items across all buckets of `phase_id`.
    pub(super) fn queued_count(&self, phase_id: &PhaseId) -> usize {
        self.buckets
            .iter()
            .filter(|((p, _, _), _)| p == phase_id)
            .map(|(_, b)| b.items.len())
            .sum()
    }
}
