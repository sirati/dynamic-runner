//! Read-only accessors plus the two non-dispatch *queued-side*
//! mutation primitives (`retain`, `update_first_match_in_place`,
//! `take_first_match`) that read or rewrite the queued portion of
//! the pool without touching the in-flight counters.
//!
//! Entry points:
//! * [`PendingPool::is_run_complete`] — terminal-state predicate.
//! * [`PendingPool::len`] / [`PendingPool::is_empty`] — outstanding-work
//!   counters.
//! * [`PendingPool::iter`] — iteration over queued items (diagnostic).
//! * [`PendingPool::retain`] — drop queued items by predicate.
//! * [`PendingPool::update_first_match_in_place`] — first-match in-place
//!   edit (queued ∪ blocked).
//! * [`PendingPool::take_first_match`] — first-match removal from a
//!   dispatchable bucket (does NOT increment in-flight; for callers
//!   that pair with `mark_in_flight`).
//! * [`PendingPool::active_phases`] / [`PendingPool::phase_state`] /
//!   [`PendingPool::in_flight`] — phase-state accessors.

use dynrunner_core::{Identifier, PhaseId, TaskInfo};

use super::pool::PendingPool;
use super::types::{BucketKey, DispatchRank, PhaseState, affinity_key, no_affinity};

impl<I: Identifier> PendingPool<I> {
    /// True iff the entire pool is empty AND no phase is `Active` or
    /// `Draining`. Manager loop predicate.
    pub fn is_run_complete(&self) -> bool {
        if !self.is_empty() {
            return false;
        }
        !self
            .phase_state
            .values()
            .any(|s| matches!(s, PhaseState::Active | PhaseState::Draining))
    }

    /// Total items remaining: queued + in-flight + blocked, all phases.
    /// Blocked items are part of the run's outstanding work even though
    /// they're not in any bucket — they will become queued once their
    /// task-level prereqs resolve.
    pub fn len(&self) -> usize {
        let queued: usize = self.buckets.values().map(|b| b.items.len()).sum();
        let in_flight: usize = self.in_flight_per_phase.values().map(|c| *c as usize).sum();
        let blocked: usize = self.blocked.len();
        queued + in_flight + blocked
    }

    /// True iff `len() == 0`.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Iterate over all queued items (does not include in-flight).
    /// Used for diagnostic logging in the managers.
    pub fn iter(&self) -> impl Iterator<Item = &TaskInfo<I>> {
        self.buckets.values().flat_map(|b| b.items.iter())
    }

    /// Drop every queued item for which `pred` returns `false`. Iterates
    /// every bucket in deterministic key order; empty buckets are left
    /// in place (cheap, and matches the lazy-creation pattern in
    /// `extend`). Does NOT touch in-flight items: removal of an item
    /// already handed to a worker is the manager's concern, surfaced
    /// via `release_worker` / `on_item_finished`.
    ///
    /// Used by callers (e.g. primary) to drop items completed
    /// elsewhere in the cluster without disturbing the phase machine.
    pub fn retain<F>(&mut self, mut pred: F)
    where
        F: FnMut(&TaskInfo<I>) -> bool,
    {
        for bucket in self.buckets.values_mut() {
            bucket.items.retain(|item| pred(item));
        }
    }

    /// Apply `update` in-place to the FIRST queued or blocked
    /// `TaskInfo<I>` for which `pred` returns `true`. Returns `true`
    /// iff a match was found and updated.
    ///
    /// Scan order: queued buckets in `BucketKey` order, FIFO within
    /// each bucket; then the `blocked` map in its `HashMap` iteration
    /// order (deterministic enough for "first match wins" semantics
    /// when at most one task can match — the intended use case).
    /// In-flight items are NOT visited: they've already been
    /// dispatched, and the per-task metadata snapshot they carry on
    /// the wire was taken at dispatch time. The pool's
    /// post-dispatch mutation cannot retroactively reshape an
    /// in-flight task.
    ///
    /// Used by callers that own out-of-band per-task metadata
    /// updates (e.g. preferred-secondaries change applied via the
    /// CRDT command channel) and need the live pool's clone to
    /// reflect the change before the next scheduler tick reads it.
    /// The pool stays generic over what "match" means: the predicate
    /// closes over whatever identity key the caller cares about
    /// (task hash, task_id, identifier) so the pool doesn't have to
    /// learn about wire-canonical hashing.
    pub fn update_first_match_in_place<F, U>(&mut self, pred: F, mut update: U) -> bool
    where
        F: Fn(&TaskInfo<I>) -> bool,
        U: FnMut(&mut TaskInfo<I>),
    {
        for bucket in self.buckets.values_mut() {
            if let Some(item) = bucket.items.iter_mut().find(|t| pred(t)) {
                update(item);
                return true;
            }
        }
        for item in self.blocked.values_mut() {
            if pred(item) {
                update(item);
                return true;
            }
        }
        false
    }

    /// Find the first queued item (in bucket-key order, FIFO within a
    /// bucket) for which `pred` returns `true`, remove it from its
    /// bucket and return it. Returns `None` if no item matches.
    ///
    /// Buckets whose phase is not `Active` or `Draining` are skipped
    /// — a `Blocked` phase's items must not be dispatched out of order,
    /// and `Drained`/`Done` phases hold no live work. `Draining` is
    /// included so items pushed back into a draining phase via
    /// `requeue` (which flips it back to `Active`) and `reinject` paths
    /// remain reachable through this primitive.
    ///
    /// Does NOT update in-flight counts or worker affinity — this is
    /// a *removal* primitive, not a *dispatch* primitive. Intended for
    /// callers (promoted primary) that need to extract a task
    /// matching a runtime predicate (e.g. memory-fit) without going
    /// through the soft-pin algorithm `pop_for_worker` implements.
    ///
    /// If a matched item leaves its bucket empty, the bucket's pinned
    /// workers are unpinned (mirroring `take_from_bucket`'s behaviour),
    /// so soft-pin invariants stay correct for any later dispatch.
    pub fn take_first_match<F>(&mut self, mut pred: F) -> Option<TaskInfo<I>>
    where
        F: FnMut(&TaskInfo<I>) -> bool,
    {
        let mut hit_key: Option<BucketKey> = None;
        let mut hit_idx: usize = 0;
        for (key, bucket) in &self.buckets {
            // Skip buckets whose phase is not currently dispatchable.
            // Active = items may dispatch; Draining = items requeued/
            // reinjected after a drain transition flipped back to
            // Active are dispatchable too. Blocked / Drained / Done
            // phases must not have items pulled out of order.
            if !matches!(
                self.phase_state.get(&key.0),
                Some(PhaseState::Active | PhaseState::Draining)
            ) {
                continue;
            }
            if let Some(idx) = bucket.items.iter().position(&mut pred) {
                hit_key = Some(key.clone());
                hit_idx = idx;
                break;
            }
        }
        let key = hit_key?;
        let bucket = self.buckets.get_mut(&key)?;
        let item = bucket.items.remove(hit_idx)?;

        // Clear soft-pin slots if this drained the bucket, mirroring
        // the bookkeeping in `take_from_bucket` so a later dispatch
        // doesn't see stale pin state.
        if bucket.items.is_empty() {
            let drained_pinners = std::mem::take(&mut bucket.pinned_workers);
            for w in drained_pinners {
                if let Some(slot) = self.worker_affinity.get_mut(&w)
                    && slot.as_ref() == Some(&key)
                {
                    *slot = None;
                }
            }
        }
        Some(item)
    }

    /// Phases currently in `Active` state (callers may need this to
    /// filter scheduling decisions).
    pub fn active_phases(&self) -> Vec<PhaseId> {
        self.phase_state
            .iter()
            .filter(|(_, s)| **s == PhaseState::Active)
            .map(|(p, _)| p.clone())
            .collect()
    }

    /// True iff at least one QUEUED item sits in a bucket whose phase is
    /// currently `Active` — i.e. there is dispatchable work an idle worker
    /// could be assigned right now.
    ///
    /// Distinct from `is_empty()` / `len()`, which fold in-flight and
    /// blocked items (so they stay `false`/`>0` whenever any task is still
    /// outstanding, even if every remaining task is in-flight on a silent
    /// secondary or blocked on an unresolved prereq). This predicate reads
    /// ONLY the queued, dispatchable portion, reusing `active_phases()` for
    /// the dispatchable-phase set — the same `Active` gate the dispatch
    /// view (`view_for_worker`) applies for its non-pin classes.
    ///
    /// Single concern: a queued-side dispatchability read. Callers that
    /// must distinguish "nothing left to hand out" from "everything left is
    /// in-flight/blocked" (the starvation oracle) compose this with their
    /// own in-flight/blocked reads.
    pub fn has_queued_dispatchable(&self) -> bool {
        let active: std::collections::HashSet<PhaseId> = self.active_phases().into_iter().collect();
        self.buckets
            .iter()
            .any(|(key, bucket)| !bucket.items.is_empty() && active.contains(&key.0))
    }

    /// True iff the number of READY (deps-met, worker-dispatchable) queued
    /// items is strictly below `threshold` — answered by a SHORT-CIRCUITING
    /// scan that stops the moment it has counted `threshold` eligible items.
    /// Worst case `O(threshold)` eligible items observed (plus the skipped
    /// non-eligible prefix), NEVER `O(queued)`: at 66k queued items the gate
    /// caller (#519) must not pay a range-digest-fold (#504-class) sweep per
    /// recheck, so this returns `false` as soon as the `threshold`-th
    /// eligible item is seen and abandons the rest of the queue.
    ///
    /// "Ready" is the SAME gate the dispatch view emits: an item in an
    /// `Active` phase that is [`Self::dispatch_eligible_now`] (worker-
    /// assignable kind AND not parked under an unexpired re-dispatch
    /// backoff). A `Setup`/`SecondaryAffine` task, a backed-off task, or an
    /// item in a non-`Active` phase is NOT ready — exactly the items a
    /// worker view would skip. Composes `active_phases()` (the dispatchable-
    /// phase set, as `has_queued_dispatchable`) with the single eligibility
    /// seam, so it can never diverge from what a dispatch actually sees.
    ///
    /// Single concern: a bounded "is the ready pool shallower than N" read.
    /// `threshold == 0` is vacuously `false` (zero items is not "below 0").
    pub fn ready_dispatchable_below(&self, threshold: usize) -> bool {
        if threshold == 0 {
            return false;
        }
        let active: std::collections::HashSet<PhaseId> = self.active_phases().into_iter().collect();
        let mut counted = 0usize;
        for (key, bucket) in &self.buckets {
            if !active.contains(&key.0) {
                continue;
            }
            for item in &bucket.items {
                if !self.dispatch_eligible_now(item) {
                    continue;
                }
                counted += 1;
                if counted >= threshold {
                    // Reached the threshold — the ready pool is NOT below it.
                    // Abandon the rest of the queue (the short-circuit that
                    // keeps this `O(threshold)`, never `O(queued)`).
                    return false;
                }
            }
        }
        // Exhausted every bucket without reaching `threshold` eligible items.
        counted < threshold
    }

    /// True iff at least one currently-`blocked` task is LIVE — none of its
    /// unmet prereqs is [`Self::is_dead_ended`] (definitely never-runnable).
    /// Short-circuits at the FIRST live blocked task found.
    ///
    /// This is the #519 gate's clause-2 ("∃ a blocked task whose unmet deps
    /// are all NON-FAILED — it can still complete, so deepening the pipeline
    /// toward it refills the ready pool"). A task blocked on a dead-ended
    /// prereq is excluded: it can never run, so a ready prerequisite of it
    /// is not worth preferring. Per the owner guardrail, "dead-ended" is the
    /// DEFINITELY-doomed set only (`failed_tasks` via [`Self::is_dead_ended`]);
    /// a retry-eligible `soft_failed` prereq may yet recover, so its
    /// dependents stay LIVE (their prerequisites still matter).
    ///
    /// Cost: `O(blocked · deps_per_blocked)` worst case, but bounded by the
    /// FIRST live hit (typically immediate). The #519 gate evaluates this
    /// ONLY when [`Self::ready_dispatchable_below`] already holds, so the
    /// combined gate stays cheap.
    pub fn has_live_blocked(&self) -> bool {
        self.blocked
            .keys()
            .any(|id| !self.is_blocked_task_dead_ended(id))
    }

    /// True iff the ready (deps-met, worker-dispatchable) queued task
    /// `task_id` is a DIRECT prerequisite of at least one LIVE blocked task
    /// — i.e. completing it would shrink some live blocked task's unmet-dep
    /// set, refilling the ready pool. The #519 selection bias's per-candidate
    /// test.
    ///
    /// Reuses the pool's ONE dependency reverse index `dependents_of` (the
    /// same edges [`Self::dependent_dispatch_rank`] and the cascade walk
    /// traverse): `dependents_of[task_id]` lists the tasks that blocked ON
    /// `task_id` at commit time and have not since gone terminal. A
    /// dependent counts only if it is still `blocked` AND is LIVE (not
    /// dead-ended, the same [`Self::is_dead_ended`]-based liveness clause-2
    /// uses) — preferring a prerequisite of an already-doomed dependent
    /// would deepen toward never-runnable work.
    ///
    /// DIRECT prerequisites only (no transitive walk): the owner spec is "a
    /// ready task that is a direct prerequisite of a blocked-non-failed
    /// task". Cost: `O(direct dependents of task_id)`, bounded by the view
    /// size at the call site, so it is safe to evaluate per dispatch
    /// candidate.
    pub fn is_ready_prerequisite_of_live_blocked(&self, task_id: &str) -> bool {
        let Some(dependents) = self.dependents_of.get(task_id) else {
            return false;
        };
        dependents.iter().any(|dep_id| {
            self.blocked.contains_key(dep_id) && !self.is_blocked_task_dead_ended(dep_id)
        })
    }

    /// True iff the blocked task `blocked_id` is dead-ended — at least one of
    /// its unmet prereqs is [`Self::is_dead_ended`] (the DEFINITELY-doomed
    /// set). The per-blocked-task liveness leaf shared by
    /// [`Self::has_live_blocked`] and
    /// [`Self::is_ready_prerequisite_of_live_blocked`] so the two agree on
    /// what "live" means.
    ///
    /// A blocked task with no recorded `task_deps` entry (defensive — a
    /// blocked item always has one) is treated as LIVE, matching
    /// `live_blocked_count`'s same defensive default.
    fn is_blocked_task_dead_ended(&self, blocked_id: &str) -> bool {
        match self.task_deps.get(blocked_id) {
            Some(deps) => deps.iter().any(|d| self.is_dead_ended(d)),
            None => false,
        }
    }

    /// True iff task `id` is DEFINITELY dead-ended — its latest terminal is
    /// permanent and no future pass can revive it. The canonical "never
    /// runnable again" leaf: `failed_tasks` membership ONLY.
    ///
    /// Deliberately excludes the OTHER terminal-but-revivable classes:
    ///   * `soft_failed` — retry-decision-pending; a drain-edge reinject may
    ///     revive it, so its dependents are still LIVE work (the #519 owner
    ///     guardrail: do not deepen AWAY from a prerequisite of a
    ///     soft-failed-dep dependent, it may yet recover).
    ///   * `dormant_tasks` — operator-revivable (`Unfulfillable`); its
    ///     dependents legitimately hold the run open.
    ///
    /// The phase-SCOPED doom in [`Self::live_blocked_count`] (which ALSO
    /// treats a `soft_failed`-in-the-gating-phase prereq as dead, because
    /// that phase's drain edge is the retry-decision point) is a DIFFERENT,
    /// drain-edge-local concern; it composes this predicate as its
    /// permanent-failure disjunct rather than re-reading `failed_tasks`, so
    /// the "permanent failure" concept has ONE owner here.
    pub(super) fn is_dead_ended(&self, id: &str) -> bool {
        self.failed_tasks.contains(id)
    }

    /// State of one phase. Useful for tests and diagnostic logging.
    pub fn phase_state(&self, phase_id: &PhaseId) -> Option<PhaseState> {
        self.phase_state.get(phase_id).copied()
    }

    /// Number of in-flight items for a phase. Useful for tests.
    pub fn in_flight(&self, phase_id: &PhaseId) -> u32 {
        self.in_flight_per_phase.get(phase_id).copied().unwrap_or(0)
    }

    /// Total items currently `blocked` (waiting on an unresolved
    /// task-level prereq), across all phases. These are neither queued
    /// nor in-flight; a caller deciding whether the only outstanding work
    /// is in-flight on silent secondaries must confirm none are blocked
    /// (a blocked item will become dispatchable once its prereq resolves,
    /// so evicting a holder would be premature).
    pub fn blocked_len(&self) -> usize {
        self.blocked.len()
    }

    /// The would-be dispatch standing of the WORK tasks gated (transitively)
    /// on `setup_task_id` — the ordering key the primary uses to route the
    /// upload whose dependents are most dispatch-imminent FIRST.
    ///
    /// `None` ⇒ no Work dependent is reachable yet (the dependent work task
    /// has not spawned, or only non-Work pass-through nodes are wired). The
    /// caller treats `None` as [`DispatchRank::WORST`] (route last), so a
    /// discovered-dependent upload always wins and an as-yet-dependent-less
    /// one is deferred, never starved (it re-ranks the moment a dependent
    /// spawns).
    ///
    /// ## Transitive walk (single dep-resolution owner)
    /// The pool's dependency reverse-index `dependents_of` is the ONE graph
    /// the dependent walks (the same edges [`Self::resolve_completed_dependents`]
    /// and the permanent-failure cascade traverse). A dependent that is not a
    /// `Work` task — a `Setup` upload feeding another upload, or a #497
    /// `SecondaryAffine` import gate between an upload and its builds — is a
    /// PASS-THROUGH node: it never dispatches to a worker itself, so it
    /// contributes no rank of its own; the walk recurses through it to the
    /// `Work` leaves whose dispatch standing the upload truly serves
    /// (upload → import → build ⇒ the build's standing). Only `Work` leaves
    /// score.
    ///
    /// ## Per-leaf rank + aggregation
    /// Each `Work` leaf's [`DispatchRank`] is derived from the SAME reads the
    /// dispatch view uses — its phase's [`PhaseState`] (→ `phase_tier`) and
    /// its `affinity_key` vs the `no_affinity` sentinel (→ `class_tier`) — so
    /// no soft-pin logic is duplicated. The aggregate is the MIN (best) leaf
    /// rank: the hottest dependent pulls the upload forward. The
    /// `neg_dependent_count` of the returned rank is set to `-(number of Work
    /// leaves)` so that two uploads whose best leaf is the same (phase, class)
    /// tier are tie-broken toward the one feeding MORE builds (the asm-dataset
    /// `group_common` file shared across a GROUP outranks a single-dependent
    /// `delta`).
    pub fn dependent_dispatch_rank(&self, setup_task_id: &str) -> Option<DispatchRank> {
        // Collect the transitive WORK-leaf dependents reachable from the
        // setup task, recursing through non-Work pass-through nodes. A
        // `visited` set guards against a diamond (a leaf reached via two
        // pass-through paths is counted once) and any defensive cycle.
        let mut work_leaves: Vec<&TaskInfo<I>> = Vec::new();
        let mut visited: std::collections::HashSet<&str> = std::collections::HashSet::new();
        let mut frontier: Vec<&str> = vec![setup_task_id];
        while let Some(dep_owner_id) = frontier.pop() {
            let Some(dependents) = self.dependents_of.get(dep_owner_id) else {
                continue;
            };
            for dependent_id in dependents {
                if !visited.insert(dependent_id.as_str()) {
                    continue;
                }
                match self.task_by_id(dependent_id) {
                    // A Work leaf: it scores. (A dependent referenced by
                    // `dependents_of` is normally still `blocked`, but a
                    // bucket lookup covers the case where a sibling dep
                    // already unblocked it.)
                    Some(item) if item.kind.is_worker_assignable() => work_leaves.push(item),
                    // A non-Work pass-through (Setup / SecondaryAffine): it
                    // never dispatches itself; recurse to ITS dependents.
                    Some(_) => frontier.push(dependent_id.as_str()),
                    // Referenced by the index but no longer resolvable
                    // (terminal): nothing to score, nothing to recurse.
                    None => {}
                }
            }
        }

        if work_leaves.is_empty() {
            return None;
        }
        // MIN over leaves on (phase_tier, class_tier); the dependent count
        // sets the tiebreak field on the aggregate.
        let count = work_leaves.len() as i32;
        let best = work_leaves
            .into_iter()
            .map(|item| self.work_task_rank(item))
            .min()
            .expect("non-empty (checked above)");
        Some(DispatchRank {
            phase_tier: best.phase_tier,
            class_tier: best.class_tier,
            neg_dependent_count: -count,
        })
    }

    /// Per-leaf dispatch rank of one `Work` task, derived from the SAME
    /// phase-state + affinity reads the dispatch view uses. The
    /// `neg_dependent_count` is left at the single-leaf default (`-1`); the
    /// aggregate count is stamped by [`Self::dependent_dispatch_rank`].
    fn work_task_rank(&self, item: &TaskInfo<I>) -> DispatchRank {
        let phase_tier = match self.phase_state.get(&item.phase_id) {
            // Mirrors the dispatch view's class gates: Active dispatches now,
            // Draining still drains requeued/reinjected items, everything
            // else (Blocked-will-activate, or an absent/terminal phase) is
            // not dispatchable now but the dependent will still reach it.
            Some(PhaseState::Active) => 0,
            Some(PhaseState::Draining) => 1,
            _ => 2,
        };
        // Typed (pinned) vs free-pool, via the same `no_affinity` sentinel
        // `view_for_worker` keys its typed-vs-free-pool classes on.
        let class_tier = if affinity_key(item) == no_affinity() { 1 } else { 0 };
        DispatchRank {
            phase_tier,
            class_tier,
            neg_dependent_count: -1,
        }
    }

    /// Resolve a task by its `task_id` to the live `TaskInfo` the pool holds
    /// — checking the task-level `blocked` map first (where a dependent
    /// referenced by `dependents_of` normally sits), then the queued buckets
    /// (a dependent already unblocked by a sibling dep). Returns `None` for a
    /// task the pool no longer holds (terminal / never seen). Read-only;
    /// shared by the dependent-rank walk.
    fn task_by_id(&self, task_id: &str) -> Option<&TaskInfo<I>> {
        if let Some(item) = self.blocked.get(task_id) {
            return Some(item);
        }
        self.iter().find(|t| t.task_id == task_id)
    }
}
