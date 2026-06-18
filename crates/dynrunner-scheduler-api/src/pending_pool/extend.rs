//! `PendingPool::extend` — the ingest path that validates one batch of
//! items (`task_id` uniqueness, `task_depends_on` resolvability and
//! acyclicity) and commits each survivor to either its bucket or the
//! blocked map.
//!
//! Owns the private helpers `commit_item` (per-item routing) and
//! `collect_known_task_ids` (the union of queued/blocked/completed/
//! failed/in-flight ids used by duplicate detection).

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use dynrunner_core::{Identifier, PhaseId, TaskInfo};

use super::pool::PendingPool;
use super::types::{Bucket, PendingPoolError, affinity_key};

impl<I: Identifier> PendingPool<I> {
    /// Insert items into the pool. Each item is bucketed by
    /// `(phase_id, type_id, affinity_id-or-sentinel)`. Items are
    /// pushed FIFO — caller is responsible for the order it wants
    /// dispatched (typically size-DESC).
    ///
    /// Validates `(phase_id, task_id)` uniqueness and `task_depends_on`
    /// well-formedness, keyed on the FULL `(phase_id, task_id)` identity
    /// so it AGREES with the non-mutating sibling
    /// [`PendingPool::partition_ingest`] (the single duplicate +
    /// dep-resolution authority): the SAME `task_id` in two DIFFERENT
    /// phases is a DISTINCT task, and a dep names a full
    /// `(phase_id, task_id)`.
    ///
    /// * `DuplicateTaskId` — a new item's `(phase_id, task_id)` collides
    ///   with another in the same batch, or with an existing
    ///   queued / blocked entry of the same identity, or with a
    ///   phase-less terminal / in-flight `task_id` (the pool does not
    ///   retain the phase for those, so a collision against a
    ///   finished/in-flight id is a producer-side reuse bug regardless
    ///   of phase). Hard error because the contract is "every
    ///   `(phase_id, task_id)` is unique within a run".
    /// * `UnknownTaskDep` — a `task_depends_on` entry names a
    ///   `(phase_id, task_id)` absent from the union of (existing pool
    ///   tasks, batch tasks — both keyed on full identity) AND the
    ///   phase-less terminal / in-flight `task_id` fallback.
    /// * `TaskDepCycle` — the union dep graph (existing blocked entries
    ///   unioned with the new batch), with `(phase_id, task_id)` node
    ///   identity, contains a cycle.
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

        // ---------- 1. Validate duplicate (phase_id, task_id) ----------
        // Identity is the full `(phase_id, task_id)`, mirroring
        // `partition_ingest`. Both fields are non-optional per the
        // boundary contract (see `dynrunner_core::types::task::TaskInfo`)
        // so the dedup loop reads them directly.
        //
        // Pool-resident full identities (queued + blocked) plus the
        // phase-less terminal / in-flight `task_id` fallback (the pool
        // does not retain the phase for those). Computed ONCE and reused
        // for the dep-resolution known set below.
        let pool_full: HashSet<(PhaseId, String)> =
            self.collect_known_phase_task_ids().into_iter().collect();
        let known_ids_phaseless: HashSet<String> = self.collect_known_task_ids();
        // Duplicate within batch on full identity.
        let mut seen_in_batch: HashSet<(PhaseId, String)> = HashSet::new();
        for item in &new_items {
            let key = (item.phase_id.clone(), item.task_id.clone());
            if !seen_in_batch.insert(key) {
                return Err(PendingPoolError::DuplicateTaskId(item.task_id.clone()));
            }
        }
        // Duplicate against existing state: same full identity in the
        // pool, OR a phase-less reuse of a finished / in-flight id.
        for item in &new_items {
            let key = (item.phase_id.clone(), item.task_id.clone());
            if pool_full.contains(&key) || known_ids_phaseless.contains(item.task_id.as_str()) {
                return Err(PendingPoolError::DuplicateTaskId(item.task_id.clone()));
            }
        }

        // ---------- 2. Validate every dep references a known id ----------
        // Known (full identity) = existing pool tasks ∪ batch tasks. A
        // dep resolves against its FULL `(phase_id, task_id)`, or — for a
        // finished/in-flight prereq the pool only remembers by id — the
        // phase-less `known_ids_phaseless` fallback.
        let mut known_full: HashSet<(PhaseId, String)> = pool_full;
        for item in &new_items {
            known_full.insert((item.phase_id.clone(), item.task_id.clone()));
        }
        for item in &new_items {
            for dep in &item.task_depends_on {
                let dep_key = (dep.phase_id.clone(), dep.task_id.clone());
                let resolves = known_full.contains(&dep_key)
                    || known_ids_phaseless.contains(dep.task_id.as_str());
                if !resolves {
                    return Err(PendingPoolError::UnknownTaskDep {
                        task: dep.task_id.clone(),
                        referenced_by: item.task_id.clone(),
                    });
                }
            }
        }

        // ---------- 3. Cycle check (Kahn's on the union graph) ----------
        // Nodes: union of (existing blocked, batch), keyed on the FULL
        // `(phase_id, task_id)` identity so the SAME `task_id` in two
        // phases is two DISTINCT nodes (a phase-blind collapse would
        // both fabricate self-cycles and hide genuine cross-phase
        // cycles). Edges: dep → dependent. Already-completed deps are
        // pre-resolved and excluded; already-failed deps cascade-fail
        // (no edge). Existing blocked items contribute their original
        // `task_depends_on` (carried on the stored `TaskInfo`, with each
        // dep's full identity) filtered by `pre_resolved` — the same
        // treatment the batch gets. Pre-resolution is matched on the
        // dep's bare `task_id` because the terminal sets are phase-less.
        type Node = (PhaseId, String);
        let mut indegree: HashMap<Node, usize> = HashMap::new();
        let mut children_of: HashMap<Node, Vec<Node>> = HashMap::new();
        let pre_resolved =
            |dep: &str| self.completed_tasks.contains(dep) || self.failed_tasks.contains(dep);
        let mut add_edges = |node: Node, deps: &[dynrunner_core::TaskDep]| {
            indegree.entry(node.clone()).or_insert(0);
            for dep in deps {
                if pre_resolved(dep.task_id.as_str()) {
                    continue;
                }
                let dep_node = (dep.phase_id.clone(), dep.task_id.clone());
                *indegree.entry(node.clone()).or_insert(0) += 1;
                children_of
                    .entry(dep_node.clone())
                    .or_default()
                    .push(node.clone());
                indegree.entry(dep_node).or_insert(0);
            }
        };
        // Existing blocked nodes: full identity + original deps.
        for item in self.blocked.values() {
            add_edges(
                (item.phase_id.clone(), item.task_id.clone()),
                &item.task_depends_on,
            );
        }
        // New batch nodes.
        for item in &new_items {
            add_edges(
                (item.phase_id.clone(), item.task_id.clone()),
                &item.task_depends_on,
            );
        }
        // Kahn's: drain zero-indegree, decrement children, count.
        let mut queue: VecDeque<Node> = indegree
            .iter()
            .filter_map(|(id, &d)| if d == 0 { Some(id.clone()) } else { None })
            .collect();
        // Deterministic order: lowest `(phase_id, task_id)` first.
        let mut queue_vec: Vec<Node> = queue.drain(..).collect();
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
            // Pick the lowest-identity node with non-zero residual
            // indegree as the cycle start; report the SCC walk reachable
            // from it. The `TaskDepCycle` payload stays a `Vec<String>`
            // of `task_id`s (the public error shape) — the phase lives in
            // the node identity that drove the walk.
            let mut start: Vec<Node> = residual
                .iter()
                .filter_map(|(id, &d)| if d != 0 { Some(id.clone()) } else { None })
                .collect();
            start.sort();
            let mut cycle_walk: Vec<String> = Vec::new();
            let mut visited_walk: HashSet<Node> = HashSet::new();
            if let Some(first) = start.first() {
                let mut cur = first.clone();
                while visited_walk.insert(cur.clone()) {
                    cycle_walk.push(cur.1.clone());
                    let next = children_of.get(&cur).and_then(|cs| {
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
        // Wrap each validated item in an `Arc` ONCE here, at the ingest
        // boundary. From this point the pool holds only `Arc<TaskInfo>`,
        // so dispatch / requeue / cascade share the SAME allocation
        // (clone the Arc, never deep-clone the TaskInfo).
        for item in new_items {
            self.commit_item(Arc::new(item));
        }
        Ok(())
    }

    /// Commit one validated item: pre-resolve `task_depends_on` against
    /// `completed_tasks` / `failed_tasks`; route to bucket, blocked,
    /// or cascaded-fail accordingly.
    ///
    /// `pub(super)` — the SINGLE edge-builder. Besides `extend`'s ingest
    /// loop, the lifecycle re-block path ([`PendingPool::reinject`] when
    /// it un-completes an already-finished dep) re-routes a freshly
    /// re-pulled queued dependent through this same routine so the
    /// blocked-map edges (`dependents_of`, `task_deps`, `blocked`,
    /// `blocked_per_phase`) are rebuilt identically to ingest — never a
    /// hand-rolled parallel builder.
    pub(super) fn commit_item(&mut self, item: Arc<TaskInfo<I>>) {
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

        // Compute unresolved prereqs (the SINGLE dep-resolution authority —
        // see [`Self::unresolved_deps`]). AFFINE deps are excluded; the set is
        // empty on a run with no affine task, so a non-affine work task's
        // blocking set is unchanged (baseline-preserved).
        let unresolved: HashSet<String> = self.unresolved_deps(&item);

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

    /// The SINGLE dep-resolution primitive: `item`'s `task_depends_on` entries
    /// that are NOT yet satisfied — i.e. not in `completed_tasks` and not an
    /// affine prereq. An empty result means the item is READY (every non-affine
    /// dep completed).
    ///
    /// AFFINE deps are EXCLUDED: a `TaskKind::SecondaryAffine` prereq's
    /// readiness is per-secondary (the bitvector + the per-secondary queue
    /// order), NOT a global terminal — so it must never block its dependent
    /// work task in this global pool (the work task is ready on its NON-affine
    /// deps, and is routed per-secondary by the affine scheduler). The
    /// `affine_prereq_ids` set is empty on a run with no affine task, so a
    /// non-affine work task's blocking set is unchanged (baseline-preserved).
    ///
    /// `pub(super)` so BOTH the ingest router ([`Self::commit_item`]) AND the
    /// pop-time idempotent re-check ([`Self::take_at_if_ready`]
    /// in `dispatch.rs`) read readiness through ONE owner — never a
    /// re-implemented dep walk. The pop-time guard exists because the 5-min
    /// reconcile arm (#652 concern C) pushes a possibly-not-ready item to a
    /// bucket head, violating the old "bucketed ⇒ ready" invariant; the guard
    /// restores it by re-blocking a not-ready item the moment dispatch selects
    /// it.
    pub(super) fn unresolved_deps(&self, item: &TaskInfo<I>) -> HashSet<String> {
        item.task_depends_on
            .iter()
            .map(|d| d.task_id.clone())
            .filter(|id| {
                !self.completed_tasks.contains(id.as_str())
                    && !self.affine_prereq_ids.contains(id.as_str())
            })
            .collect()
    }

    /// Return the union of every task_id the pool currently knows
    /// about (queued in any bucket, blocked waiting on prereqs,
    /// completed, failed, soft-failed, dormant, or in flight). Used by
    /// `extend`'s duplicate-id check and by the sibling
    /// `partition_ingest` (`partition.rs`) for its phase-less
    /// terminal/in-flight fallback — `pub(super)` so the two
    /// well-formedness policies share one collector.
    ///
    /// `soft_failed` and `dormant_tasks` membership counts as KNOWN: a
    /// retry-pending or dormant root is a real task identity the pool
    /// tracks — its id left the bucket/in-flight sets on its terminal,
    /// but a dependent referencing it must land in `blocked` (awaiting
    /// the drain-edge / operator revival decision), not fail
    /// `UnknownTaskDep`. Neither set enters `commit_item`'s
    /// pre-resolution (the dep stays unresolved) nor its extend-time
    /// cascade (the failure is not permanent), so "known" is their ONLY
    /// extend-side effect.
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
        for id in self.soft_failed.keys() {
            out.insert(id.clone());
        }
        for id in &self.dormant_tasks {
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
    pub(super) fn collect_known_phase_task_ids(&self) -> Vec<(dynrunner_core::PhaseId, String)> {
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
