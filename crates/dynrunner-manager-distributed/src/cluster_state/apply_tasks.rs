//! Task-batch apply rule and the cascade-resume helper.
//!
//! Single concern: the `TasksSpawned` batch apply (which classifies
//! each newly-spawned entry as `Pending`, `Blocked`, or cascade-failed
//! based on its `task_depends_on` resolutions) and the
//! `resume_blocked_on` helper that the `TaskCompleted` apply arm in
//! sibling `apply.rs` invokes to auto-transition every dependent
//! `Blocked { on, .. }` back to `Pending` when its prerequisite
//! completes.

use dynrunner_core::{ErrorType, Identifier, TaskInfo};

use super::{ApplyOutcome, ClusterState, TaskState};

impl<I: Identifier> ClusterState<I> {
    /// Auto-resume helper: scan every entry in `tasks`, transition any
    /// `TaskState::Blocked { on, task }` whose `on` matches `prereq_hash`
    /// back to `Pending { task }`, and return a clone of every just-
    /// resumed `TaskInfo<I>` so originator-side callers can mirror the
    /// transition into their live `PendingPool` (whose cascade-paused
    /// items were dropped by the earlier `on_item_failed_permanent`
    /// call and have to be re-introduced for dispatch to see them).
    ///
    /// Invoked from the `TaskCompleted` apply arm — completion of a
    /// prerequisite is the event that unblocks every cascade-paused
    /// dependent. Linear scan over `tasks` because the CRDT does not
    /// maintain a hash-keyed reverse index (the PendingPool's
    /// `dependents_of` is task-id-keyed and lives only on the primary;
    /// every replica must run this auto-resume locally to converge,
    /// and the scan keeps the dependency-tracking concern self-
    /// contained inside cluster_state).
    ///
    /// Single-pass and self-contained: the resumed entries land in
    /// `Pending` immediately, so a chain of blocked dependents waiting
    /// on the same prereq all resume in one call. Further chained
    /// resumes (a now-resumed task itself completing later) fire on
    /// their own `TaskCompleted` apply arm; no recursion here.
    ///
    /// Implementation note: two-pass (collect hashes, then mutate) so
    /// the inner `TaskInfo<I>` can be moved out by value without
    /// requiring `I: Default` or unsafe placeholder construction. The
    /// hashmap-key clone is the only allocation; the `TaskInfo` move
    /// itself is in-place. We clone the task once before re-insertion
    /// so the returned `Vec<TaskInfo<I>>` is independent of further
    /// CRDT mutations — callers may hold the clones across additional
    /// apply calls.
    pub(super) fn resume_blocked_on(&mut self, prereq_hash: &str) -> Vec<TaskInfo<I>> {
        let to_resume: Vec<String> = self
            .tasks
            .iter()
            .filter_map(|(h, s)| match s {
                TaskState::Blocked { on, .. } if on == prereq_hash => Some(h.clone()),
                _ => None,
            })
            .collect();
        let mut resumed: Vec<TaskInfo<I>> = Vec::with_capacity(to_resume.len());
        for h in to_resume {
            if let Some(TaskState::Blocked { task, .. }) = self.tasks.remove(&h) {
                resumed.push(task.clone());
                self.tasks.insert(h, TaskState::Pending { task });
            }
        }
        resumed
    }







    /// Apply a `ClusterMutation::TasksSpawned` batch.
    ///
    /// For each `TaskInfo<I>` in `tasks` (iteration order = caller's
    /// insertion order; the wire format preserves it):
    ///
    /// 1. Compute the wire-canonical content hash.
    /// 2. If `self.tasks` already contains an entry under that hash,
    ///    NoOp this entry (idempotent re-injection of an already-
    ///    spawned task is silent; the originator's pre-validation
    ///    surfaces the duplicate as a per-index `SpawnError` so
    ///    callers see it even when the wire apply is silent).
    /// 3. Otherwise resolve each `task_depends_on` task_id to a hash
    ///    via [`Self::task_hash_for_task_id`] and decide the initial
    ///    state per the cascade rules:
    ///      * Any dep in `Failed { kind: NonRecoverable, .. }` →
    ///        insert as `Failed { kind: NonRecoverable, task,
    ///        last_error: "upstream-failed", attempts: 1 }`
    ///        (cascade-fail; matches the legacy worker-originated
    ///        cascade shape).
    ///      * Else any dep in `Unfulfillable { .. }` → insert as
    ///        `Blocked { task, on: dep_hash }` so the auto-resume
    ///        mechanism in `resume_blocked_on` re-activates this
    ///        entry when the prereq's TaskCompleted fires.
    ///      * Else any dep in `Pending { .. } / InFlight { .. } /
    ///        Blocked { .. }` → insert as `Blocked { task, on:
    ///        first-unresolved-dep-hash }`. The first non-terminal
    ///        dep wins; later deps don't widen the `on` field
    ///        because auto-resume fires whenever `on` matches the
    ///        completing prereq's hash and a dependent that
    ///        immediately re-blocks on a still-pending sibling is
    ///        already covered by the next `TaskCompleted` apply.
    ///      * Else (no deps OR all deps `Completed { .. }`) →
    ///        insert as `Pending { task }`.
    ///
    /// Pre-apply validation in the originator's `apply_spawn_tasks`
    /// rejects entries whose deps reference an unknown id (those
    /// surface as `SpawnError::UnknownDependency` on the reply
    /// oneshot, not as wire state); this apply rule trusts every
    /// referenced id resolves and `panic!`s in debug-mode if it
    /// doesn't (a contract violation by the originator).
    ///
    /// Returns `Applied` if AT LEAST ONE entry actually mutated the
    /// ledger; `NoOp` if every entry was a duplicate (the whole batch
    /// was already-applied — e.g. retransmission of a CRDT snapshot).
    ///
    /// `newly_pending_from_spawn` accumulates a clone of every input
    /// task whose post-classify state is `Pending` (i.e. no deps, or
    /// all deps already `Completed`). This is the receiver-side
    /// surface for derived-view pool growth: a coordinator that
    /// applies a TasksSpawned observed on the wire AND locally owns a
    /// dispatch pool (live primary's `pending`, promoted-secondary's
    /// `primary_pending`) uses the surfaced clones to extend the pool
    /// so the CRDT ledger and the pool stay coherent. The originator
    /// path (live primary's / promoted-secondary's own
    /// `apply_spawn_tasks`) already performs an equivalent post-apply
    /// walk via `task_state` lookup and chooses to ignore this surface
    /// — keeping the originator's behaviour byte-identical avoids
    /// double-inject.
    ///
    /// Duplicate-hash entries NoOp on the ledger AND are NOT surfaced
    /// (re-applying a TasksSpawned snapshot does not re-grow the
    /// pool). Cascade-failed and Blocked entries are not surfaced
    /// because they should not enter a dispatch pool.
    pub(super) fn apply_tasks_spawned(
        &mut self,
        tasks: Vec<TaskInfo<I>>,
        newly_pending_from_spawn: &mut Vec<TaskInfo<I>>,
    ) -> ApplyOutcome {
        let mut applied_any = false;
        for task in tasks {
            let hash = crate::primary::wire::compute_task_hash(&task);
            if self.tasks.contains_key(&hash) {
                // Idempotent: already present in the ledger.
                continue;
            }
            // Resolve deps + classify in one pass. We scan deps in
            // order; the first dep we hit that's in
            // `Failed{NonRecoverable}` short-circuits to cascade-fail
            // (the strongest blocker — no Blocked-then-cascade
            // ordering anomaly is possible). The first
            // `Unfulfillable` dep produces the Blocked-on-that-hash
            // entry; any remaining non-terminal dep is recorded as a
            // fallback `Blocked` target. Each replica reaches the
            // same classification deterministically because the
            // ledger they read is the same.
            let mut cascade_fail = false;
            let mut blocked_on_unfulfillable: Option<String> = None;
            let mut blocked_on_pending: Option<String> = None;
            for dep_id in &task.task_depends_on {
                let dep_hash = match self.task_hash_for_task_id(dep_id) {
                    Some(h) => h.to_string(),
                    None => {
                        // Originator-side pre-validation should have
                        // caught this; log + treat as unblocked so
                        // we don't lose the task. Defensive.
                        tracing::warn!(
                            target: "dynrunner_cluster_state",
                            dep_id,
                            "TasksSpawned: dep id not present in ledger; \
                             treating as resolved (originator-side \
                             pre-validation contract violated)"
                        );
                        continue;
                    }
                };
                match self.tasks.get(&dep_hash) {
                    Some(TaskState::Failed {
                        kind: ErrorType::NonRecoverable,
                        ..
                    }) => {
                        cascade_fail = true;
                        break;
                    }
                    Some(TaskState::Failed { .. }) => {
                        // Other ErrorType classes (Recoverable, OOM,
                        // ResourceExhausted) are not cascade-fail
                        // terminals — they're retry-eligible. A
                        // dependent on a Recoverable failure is
                        // effectively blocked until the retry passes
                        // succeed or budget exhausts. Treat as
                        // pending-blocked.
                        if blocked_on_pending.is_none() {
                            blocked_on_pending = Some(dep_hash);
                        }
                    }
                    Some(TaskState::Unfulfillable { .. }) => {
                        if blocked_on_unfulfillable.is_none() {
                            blocked_on_unfulfillable = Some(dep_hash);
                        }
                    }
                    Some(TaskState::Completed { .. }) => {
                        // Resolved dep — contributes nothing to the
                        // blocking decision.
                    }
                    Some(TaskState::Pending { .. })
                    | Some(TaskState::InFlight { .. })
                    | Some(TaskState::Blocked { .. }) => {
                        if blocked_on_pending.is_none() {
                            blocked_on_pending = Some(dep_hash);
                        }
                    }
                    Some(TaskState::Cancelled { .. }) => {
                        // Dependent of a panik-cancelled prereq.
                        // The prereq won't reach Completed, so the
                        // dependent can't ever succeed; treat as
                        // cascade-fail (parallel to a NonRecoverable
                        // prereq above). The new spawn lands in
                        // `Failed { NonRecoverable, "upstream-failed" }`
                        // — distinct from a fresh `Cancelled`
                        // discriminant because the panik latch is the
                        // single source of "this run was operator-
                        // stopped"; per-task `Cancelled` is reserved
                        // for the sweep originated by the panik
                        // broadcast, not for new derived tasks
                        // spawned afterwards.
                        cascade_fail = true;
                        break;
                    }
                    None => {
                        tracing::warn!(
                            target: "dynrunner_cluster_state",
                            dep_id,
                            dep_hash = %dep_hash,
                            "TasksSpawned: dep id resolved to hash but \
                             hash not in ledger (concurrent removal?)"
                        );
                    }
                }
            }
            let initial = if cascade_fail {
                TaskState::Failed {
                    task,
                    kind: ErrorType::NonRecoverable,
                    last_error: "upstream-failed".to_string(),
                    attempts: 1,
                }
            } else if let Some(on) = blocked_on_unfulfillable {
                TaskState::Blocked { task, on }
            } else if let Some(on) = blocked_on_pending {
                TaskState::Blocked { task, on }
            } else {
                // Surface a clone of the freshly-Pending task so a
                // receive-side caller can grow its local dispatch
                // pool. The clone is independent of the CRDT entry —
                // callers may move it into a pool via `reinject`
                // without disturbing the ledger.
                newly_pending_from_spawn.push(task.clone());
                TaskState::Pending { task }
            };
            tracing::debug!(
                target: "dynrunner_cluster_state",
                hash = %hash,
                event = "task_spawned",
                state = ?std::mem::discriminant(&initial),
                "TasksSpawned: inserted entry"
            );
            self.tasks.insert(hash, initial);
            applied_any = true;
        }
        if applied_any {
            ApplyOutcome::Applied
        } else {
            ApplyOutcome::NoOp
        }
    }
}
