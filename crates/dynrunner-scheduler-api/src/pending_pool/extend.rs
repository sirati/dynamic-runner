//! `PendingPool::extend` — the ingest path that validates one batch of
//! items (`task_id` uniqueness, `task_depends_on` resolvability and
//! acyclicity) and commits each survivor to either its bucket or the
//! blocked map.
//!
//! Owns the private helpers `commit_item` (per-item routing) and
//! `collect_known_task_ids` (the union of queued/blocked/completed/
//! failed/in-flight ids used by duplicate detection).

use std::collections::{HashMap, HashSet, VecDeque};

use dynrunner_core::{Identifier, TaskInfo};

use super::pool::PendingPool;
use super::types::{Bucket, PendingPoolError, affinity_key};

impl<I: Identifier> PendingPool<I> {
    /// Insert items into the pool. Each item is bucketed by
    /// `(phase_id, type_id, affinity_id-or-sentinel)`. Items are
    /// pushed FIFO — caller is responsible for the order it wants
    /// dispatched (typically size-DESC).
    ///
    /// Validates `task_id` uniqueness and `task_depends_on`
    /// well-formedness:
    /// * `DuplicateTaskId` — a new item's `task_id` collides with
    ///   another in the same batch, or with an existing
    ///   queued / blocked / completed / failed task. Hard error
    ///   because the contract is "every task_id is unique within a
    ///   run" — a collision is a producer-side bug that would
    ///   otherwise mask one of the colliding tasks.
    /// * `UnknownTaskDep` — a `task_depends_on` entry references an id
    ///   that is not present in the union of (existing pool tasks,
    ///   batch tasks, completed tasks, failed tasks).
    /// * `TaskDepCycle` — the union dep graph (existing blocked entries
    ///   + new batch) contains a cycle.
    ///
    /// On error the pool is unchanged (atomic validate-then-commit).
    /// Items whose every `task_depends_on` entry is already in
    /// `completed_tasks` are pre-resolved and pushed straight into
    /// their bucket. Items whose deps include a `failed_tasks` entry
    /// cascade-fail at extend time: their id is recorded in
    /// `failed_tasks` and the `TaskInfo` is dropped — same semantics
    /// as `on_item_failed_permanent`'s cascade.
    pub fn extend(
        &mut self,
        items: impl IntoIterator<Item = TaskInfo<I>>,
    ) -> Result<(), PendingPoolError> {
        let new_items: Vec<TaskInfo<I>> = items.into_iter().collect();

        // ---------- 1. Validate duplicate task_ids ----------
        // Duplicate within batch. `task_id` is non-optional per the
        // boundary contract (see `dynrunner_core::types::task::TaskInfo`)
        // so the dedup loop reads the field directly.
        let mut seen_in_batch: HashSet<&str> = HashSet::new();
        for item in &new_items {
            if !seen_in_batch.insert(item.task_id.as_str()) {
                return Err(PendingPoolError::DuplicateTaskId(item.task_id.clone()));
            }
        }
        // Duplicate against existing state.
        let existing_ids = self.collect_known_task_ids();
        for item in &new_items {
            if existing_ids.contains(item.task_id.as_str()) {
                return Err(PendingPoolError::DuplicateTaskId(item.task_id.clone()));
            }
        }

        // ---------- 2. Validate every dep references a known id ----------
        // Known = existing pool tasks ∪ batch tasks ∪ completed ∪ failed.
        let mut known: HashSet<String> = existing_ids;
        for item in &new_items {
            known.insert(item.task_id.clone());
        }
        for item in &new_items {
            for dep in &item.task_depends_on {
                if !known.contains(&dep.task_id) {
                    return Err(PendingPoolError::UnknownTaskDep {
                        task: dep.task_id.clone(),
                        referenced_by: item.task_id.clone(),
                    });
                }
            }
        }

        // ---------- 3. Cycle check (Kahn's on the union graph) ----------
        // Nodes: union of (existing blocked task_ids, batch task_ids).
        // Edges: dep → dependent. Already-completed deps are pre-resolved
        // and excluded; already-failed deps will cascade-fail (no edge).
        // Within-batch items contribute their full task_depends_on; existing
        // blocked items contribute their current `task_deps[id]` set
        // (which already excludes resolved/completed entries by construction).
        let mut indegree: HashMap<String, usize> = HashMap::new();
        let mut children_of: HashMap<String, Vec<String>> = HashMap::new();
        let pre_resolved = |dep: &str| {
            self.completed_tasks.contains(dep) || self.failed_tasks.contains(dep)
        };
        // Existing blocked nodes.
        for (id, deps) in &self.task_deps {
            indegree.entry(id.clone()).or_insert(0);
            for dep in deps {
                if pre_resolved(dep) {
                    continue;
                }
                *indegree.entry(id.clone()).or_insert(0) += 1;
                children_of
                    .entry(dep.clone())
                    .or_default()
                    .push(id.clone());
                indegree.entry(dep.clone()).or_insert(0);
            }
        }
        // New batch nodes. Every task carries a `task_id` (boundary
        // contract); the cycle-check graph nodes are exactly the
        // batch's task_ids unioned with the existing-blocked set.
        for item in &new_items {
            let id = item.task_id.clone();
            indegree.entry(id.clone()).or_insert(0);
            for dep in &item.task_depends_on {
                if pre_resolved(&dep.task_id) {
                    continue;
                }
                *indegree.entry(id.clone()).or_insert(0) += 1;
                children_of
                    .entry(dep.task_id.clone())
                    .or_default()
                    .push(id.clone());
                indegree.entry(dep.task_id.clone()).or_insert(0);
            }
        }
        // Kahn's: drain zero-indegree, decrement children, count.
        let mut queue: VecDeque<String> = indegree
            .iter()
            .filter_map(|(id, &d)| if d == 0 { Some(id.clone()) } else { None })
            .collect();
        // Deterministic order: lowest id first.
        let mut queue_vec: Vec<String> = queue.drain(..).collect();
        queue_vec.sort();
        queue.extend(queue_vec);
        let mut visited = 0usize;
        let mut residual = indegree.clone();
        while let Some(p) = queue.pop_front() {
            visited += 1;
            if let Some(children) = children_of.get(&p) {
                let mut newly_zero = Vec::new();
                for child in children {
                    let entry = residual.get_mut(child).expect("child in indegree map");
                    *entry -= 1;
                    if *entry == 0 {
                        newly_zero.push(child.clone());
                    }
                }
                newly_zero.sort();
                queue.extend(newly_zero);
            }
        }
        if visited != residual.len() {
            // Pick the lowest-id node with non-zero residual indegree as
            // the cycle start; report the SCC walk reachable from it.
            let mut start: Vec<String> = residual
                .iter()
                .filter_map(|(id, &d)| if d != 0 { Some(id.clone()) } else { None })
                .collect();
            start.sort();
            let mut cycle_walk: Vec<String> = Vec::new();
            let mut visited_walk: HashSet<String> = HashSet::new();
            if let Some(first) = start.first() {
                let mut cur = first.clone();
                while visited_walk.insert(cur.clone()) {
                    cycle_walk.push(cur.clone());
                    let next = children_of
                        .get(&cur)
                        .and_then(|cs| {
                            // Pick the smallest still-unresolved child to
                            // make the walk deterministic.
                            cs.iter()
                                .filter(|c| residual.get(*c).copied().unwrap_or(0) != 0)
                                .min()
                                .cloned()
                        });
                    match next {
                        Some(n) => cur = n,
                        None => break,
                    }
                }
            }
            return Err(PendingPoolError::TaskDepCycle(cycle_walk));
        }

        // ---------- 4. Commit: insert each item into bucket OR blocked ----------
        for item in new_items {
            self.commit_item(item);
        }
        Ok(())
    }

    /// Commit one validated item: pre-resolve `task_depends_on` against
    /// `completed_tasks` / `failed_tasks`; route to bucket, blocked,
    /// or cascaded-fail accordingly.
    fn commit_item(&mut self, item: TaskInfo<I>) {
        // Cascade-fail at extend time: if any prereq is already in
        // `failed_tasks`, this item is itself a cascaded failure.
        let any_failed_dep = item
            .task_depends_on
            .iter()
            .any(|d| self.failed_tasks.contains(&d.task_id));
        if any_failed_dep {
            self.failed_tasks.insert(item.task_id.clone());
            // Drop the TaskInfo — extend-time cascade does not surface
            // it (the consumer hasn't given us a place to land it
            // because it specified a hard prereq that's already failed).
            return;
        }

        // Compute unresolved prereqs (ones not yet in `completed_tasks`).
        let unresolved: HashSet<String> = item
            .task_depends_on
            .iter()
            .map(|d| d.task_id.clone())
            .filter(|id| !self.completed_tasks.contains(id.as_str()))
            .collect();

        let task_id = item.task_id.clone();
        let phase_id = item.phase_id.clone();
        if unresolved.is_empty() {
            // Ready: straight into the bucket.
            let key = (phase_id, item.type_id.clone(), affinity_key(&item));
            self.buckets
                .entry(key)
                .or_insert_with(Bucket::new)
                .items
                .push_back(item);
            return;
        }
        // Blocked: register in the dep maps and counters, NOT in any bucket.
        for dep in &unresolved {
            self.dependents_of
                .entry(dep.clone())
                .or_default()
                .push(task_id.clone());
        }
        self.task_deps.insert(task_id.clone(), unresolved);
        *self.blocked_per_phase.entry(phase_id).or_insert(0) += 1;
        self.blocked.insert(task_id, item);
    }

    /// Return the union of every task_id the pool currently knows
    /// about (queued in any bucket, blocked waiting on prereqs,
    /// completed, or failed). Used by `extend`'s duplicate-id check
    /// and by the sibling `partition_ingest` (`partition.rs`) for its
    /// phase-less terminal/in-flight fallback — `pub(super)` so the two
    /// well-formedness policies share one collector.
    pub(super) fn collect_known_task_ids(&self) -> HashSet<String> {
        let mut out: HashSet<String> = HashSet::new();
        for bucket in self.buckets.values() {
            for item in &bucket.items {
                out.insert(item.task_id.clone());
            }
        }
        for id in self.blocked.keys() {
            out.insert(id.clone());
        }
        for id in &self.completed_tasks {
            out.insert(id.clone());
        }
        for id in &self.failed_tasks {
            out.insert(id.clone());
        }
        for id in &self.in_flight_tasks {
            out.insert(id.clone());
        }
        out
    }

    /// The pool's phase-RESOLVABLE entries as full `(phase_id, task_id)`
    /// identities — every queued bucket item and every blocked item.
    /// Sibling of [`Self::collect_known_task_ids`] for the callers that
    /// need the phase (the `(phase_id, task_id)`-keyed duplicate +
    /// dep-resolution rules in `partition_ingest`).
    ///
    /// The terminal (completed / failed) and in-flight sets are NOT
    /// included: the pool retains only their `task_id`, not their phase,
    /// so they cannot be expressed as a full identity. Callers that need
    /// those fall back to the phase-less `collect_known_task_ids`.
    pub(super) fn collect_known_phase_task_ids(
        &self,
    ) -> Vec<(dynrunner_core::PhaseId, String)> {
        let mut out: Vec<(dynrunner_core::PhaseId, String)> = Vec::new();
        for bucket in self.buckets.values() {
            for item in &bucket.items {
                out.push((item.phase_id.clone(), item.task_id.clone()));
            }
        }
        for item in self.blocked.values() {
            out.push((item.phase_id.clone(), item.task_id.clone()));
        }
        out
    }
}
