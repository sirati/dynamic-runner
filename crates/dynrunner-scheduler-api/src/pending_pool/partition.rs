//! `PendingPool::partition_ingest` — the NON-mutating sibling of
//! [`super::extend`]'s validation pass.
//!
//! Single concern: classify one incoming batch of `TaskInfo<I>` items
//! into three disjoint sets — *valid* (every `task_depends_on` resolves
//! and no `(phase_id, task_id)` duplicate), *invalid_deps* (at least one
//! `task_depends_on` entry names a literally-absent `(phase_id, task_id)`)
//! and *duplicates* (the item's `(phase_id, task_id)` collides with an
//! earlier batch item or an existing pool entry). The pool is NOT
//! mutated — this returns DATA only. The manager owns the policy that
//! turns the returned partition into `ClusterMutation`s / `RunError`s
//! (the distributed 3a/3b split, the local `FailedTask` parity).
//!
//! Why a sibling of `extend` rather than a flag on `extend`: `extend`'s
//! contract is "validate-then-commit atomically; ANY validation failure
//! leaves the pool untouched and surfaces a hard `PendingPoolError`".
//! `invalid_task` flips the missing-dep + duplicate cases from a hard
//! error into a per-task soft classification the manager broadcasts as a
//! terminal `InvalidTask` (the cluster keeps running). The two are
//! different policies over the same well-formedness rules, so the rules
//! live in one place ([`PendingPool::collect_known_task_ids`] + the
//! `(phase_id, task_id)` key reused here) and each policy owns its own
//! entry point. `extend` then runs on the returned `valid` subset and
//! keeps its atomic contract there (a CYCLE among valid tasks is still a
//! hard `PendingPoolError::TaskDepCycle` — cycles are not an
//! `invalid_task` class).
//!
//! Identity is the FULL `(phase_id, task_id)`: the same `task_id` in two
//! different phases is a DISTINCT task, so a cross-phase same-`task_id`
//! is NOT a duplicate, and a dep that names a different phase than the
//! batch item carrying it resolves against that other phase's entry.

use std::collections::HashSet;

use dynrunner_core::{Identifier, PhaseId, TaskInfo};

use super::pool::PendingPool;

/// The non-mutating classification of one ingest batch, keyed on the
/// full `(phase_id, task_id)` identity.
///
/// Each input item lands in exactly one of the three vectors. The
/// `valid` vector preserves the input order (so the caller can hand it
/// straight to [`PendingPool::extend`], which itself preserves order).
/// `invalid_deps` and `duplicates` carry the offending item plus a
/// human-readable reason the manager surfaces verbatim in the
/// `InvalidTask { reason }` it broadcasts.
#[derive(Debug)]
pub struct IngestPartition<I> {
    /// Items whose `task_depends_on` all resolve and whose
    /// `(phase_id, task_id)` is unique within the batch and against the
    /// existing pool. Hand to [`PendingPool::extend`].
    pub valid: Vec<TaskInfo<I>>,
    /// Items with at least one `task_depends_on` entry naming a
    /// literally-absent `(phase_id, task_id)`. The `String` is the
    /// reason (names the absent ids), suitable for an
    /// `ErrorType::InvalidTask { reason }`.
    pub invalid_deps: Vec<(TaskInfo<I>, String)>,
    /// Items whose `(phase_id, task_id)` collides with an earlier batch
    /// item or an existing pool entry. The `String` is the reason.
    pub duplicates: Vec<(TaskInfo<I>, String)>,
}

impl<I: Identifier> PendingPool<I> {
    /// Classify a batch into `{ valid, invalid_deps, duplicates }`
    /// WITHOUT mutating the pool. Keyed on `(phase_id, task_id)`.
    ///
    /// Classification per item (first matching rule wins, so an item is
    /// never double-counted):
    /// 1. **duplicate** — the item's `(phase_id, task_id)` was already
    ///    seen earlier in this batch, OR it collides with an existing
    ///    pool entry (queued / blocked) of the same `(phase_id, task_id)`,
    ///    OR its `task_id` is in the pool's phase-less terminal /
    ///    in-flight sets ([`PendingPool::collect_known_task_ids`]). The
    ///    terminal / in-flight sets are matched on `task_id` alone
    ///    because the pool does not retain the phase for those entries
    ///    (an unphased collision against a finished/in-flight id is a
    ///    producer-side reuse bug regardless of phase).
    /// 2. **invalid_deps** — at least one `task_depends_on` entry names a
    ///    `(phase_id, task_id)` that is absent from BOTH the batch's own
    ///    `(phase_id, task_id)` set AND the pool's known set (the
    ///    phase-resolvable queued/blocked entries plus the phase-less
    ///    terminal/in-flight `task_id`s). "Literally-absent" only — a dep
    ///    that EXISTS but is itself an `invalid_task` is NOT detected
    ///    here (it cascades through the manager's dep classifier as an
    ///    upstream-invalid `NonRecoverable`, keeping the `invalid_task`
    ///    reason space accurate).
    /// 3. **valid** — everything else.
    ///
    /// Within-batch deps resolve against the batch itself (a batch item
    /// may depend on another batch item, INCLUDING one that this same
    /// pass classifies as `invalid_deps` — presence, not validity, is
    /// what "literally-absent" tests). The pool stays untouched, so the
    /// caller is free to call this as many times as it likes before
    /// committing the `valid` subset via `extend`.
    pub fn partition_ingest(
        &self,
        items: impl IntoIterator<Item = TaskInfo<I>>,
    ) -> IngestPartition<I> {
        let new_items: Vec<TaskInfo<I>> = items.into_iter().collect();

        // ---------- Known-set construction (read-only) ----------
        // Pool-resident full identities `(phase_id, task_id)` from the
        // phase-resolvable entries (queued buckets + blocked). Computed
        // ONCE. Used both for the duplicate-against-pool check (a batch
        // id colliding with an existing pool entry) and as the base of
        // the dep-resolution known set.
        let pool_full: HashSet<(PhaseId, String)> =
            self.collect_known_phase_task_ids().into_iter().collect();
        // Dep-resolution known set: the pool entries PLUS the batch's
        // own identities (so within-batch deps resolve, matching
        // `extend`). Presence — not validity — is the test: an item that
        // depends on a batch sibling resolves even if that sibling is
        // itself classified invalid by this same pass (the sibling
        // exists; the cascade is the manager's concern, not a fresh
        // missing-dep here).
        let mut known_full: HashSet<(PhaseId, String)> = pool_full.clone();
        for item in &new_items {
            known_full.insert((item.phase_id.clone(), item.task_id.clone()));
        }
        // Phase-less fallback: the pool's terminal (completed / failed)
        // and in-flight `task_id`s, for which the phase was not
        // retained. A dep resolves if its `task_id` is in here even when
        // its phase can't be matched (a finished prereq from an earlier
        // phase the pool only remembers by id); a batch id colliding
        // with one of these is a reuse-of-a-finished-id duplicate.
        let known_ids_phaseless: HashSet<String> = self.collect_known_task_ids();

        // ---------- Per-item classification ----------
        // `seen_in_batch` accumulates the `(phase_id, task_id)` of every
        // item we have already classified so a later item colliding with
        // an earlier one is a duplicate. Built incrementally so the
        // FIRST occurrence of a `(phase, task_id)` is classified by its
        // own merits and every subsequent occurrence is a duplicate.
        let mut seen_in_batch: HashSet<(PhaseId, String)> = HashSet::new();
        let mut result = IngestPartition {
            valid: Vec::with_capacity(new_items.len()),
            invalid_deps: Vec::new(),
            duplicates: Vec::new(),
        };

        for item in new_items {
            let key = (item.phase_id.clone(), item.task_id.clone());

            // Rule 1: duplicate against an earlier batch item, an
            // existing pool entry of the same identity, or a phase-less
            // finished/in-flight id. `known_full` includes the batch's
            // own identities, so we additionally check `seen_in_batch`
            // (the already-classified prefix) to distinguish the first
            // occurrence from a later collision.
            let dup_in_batch = seen_in_batch.contains(&key);
            let dup_in_pool_full = pool_full.contains(&key);
            let dup_in_pool_phaseless = known_ids_phaseless.contains(item.task_id.as_str());
            if dup_in_batch || dup_in_pool_full || dup_in_pool_phaseless {
                let reason = format!(
                    "duplicate task identity (phase={}, task_id={})",
                    item.phase_id, item.task_id
                );
                result.duplicates.push((item, reason));
                continue;
            }
            // First occurrence of this identity — record it so later
            // collisions are caught even if THIS item is itself invalid.
            seen_in_batch.insert(key);

            // Rule 2: any dep names a literally-absent (phase, task_id).
            let missing: Vec<String> = item
                .task_depends_on
                .iter()
                .filter(|dep| {
                    let dep_key = (dep.phase_id.clone(), dep.task_id.clone());
                    let resolves_full = known_full.contains(&dep_key);
                    let resolves_phaseless = known_ids_phaseless.contains(dep.task_id.as_str());
                    !(resolves_full || resolves_phaseless)
                })
                .map(|dep| format!("(phase={}, task_id={})", dep.phase_id, dep.task_id))
                .collect();
            if !missing.is_empty() {
                let reason = format!("missing dep {}", missing.join(", "));
                result.invalid_deps.push((item, reason));
                continue;
            }

            // Rule 3: valid.
            result.valid.push(item);
        }

        result
    }
}
