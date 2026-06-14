//! Task-batch apply rule and the cascade-resume helper.
//!
//! Single concern: the `TasksSpawned` batch apply (which classifies
//! each newly-spawned entry as `Pending`, `Blocked`, or cascade-failed
//! based on its `task_depends_on` resolutions) and the
//! `resume_blocked_on` helper that the `TaskCompleted` apply arm in
//! sibling `apply.rs` invokes to auto-transition every dependent
//! `Blocked { on, .. }` back to `Pending` when its prerequisite
//! completes.

use dynrunner_core::{DonePayload, ErrorType, Identifier, TaskInfo, TaskOutputs};

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
    /// Cache a completing task's `TaskOutputs` under its content hash.
    ///
    /// Invoked from the `TaskCompleted` apply arm with the completing
    /// task's hash and the wire mutation's `result_data` payload. The
    /// cache is keyed by the wire-canonical content hash — which folds
    /// in `phase_id`, so the same `task_id` in two different phases
    /// keys to two distinct cache entries (no cross-phase output
    /// collision). The dispatch-time predecessor assembler resolves a
    /// dep's full `(phase_id, task_id)` identity to its hash, then
    /// reads this cache by that hash.
    ///
    /// Three branches, in priority order:
    ///
    /// 1. `result_data` is `None` (no outputs committed) — nothing to
    ///    record. Callers are responsible for emitting `Some(_)` only
    ///    when the worker actually published outputs; an empty
    ///    `TaskOutputs` round-trips as `Some(b"{}"...)` so the
    ///    `None` arm is a true "did not publish" signal.
    /// 2. `result_data` decodes as [`DonePayload`] — extract the
    ///    inner `outputs` (a [`TaskOutputs`]) and insert it under the
    ///    completing task's content hash. The wrapper's counter fields
    ///    (`warnings`/`filtered`) are not consumed cluster-side; serde
    ///    drops them silently because the struct does NOT use
    ///    `deny_unknown_fields`. A hash with no ledger entry (late-
    ///    arriving mutation for a task this replica never saw) is
    ///    silently skipped (no entry to anchor the cache against).
    /// 3. `result_data` is malformed JSON — emit a `tracing::warn!`
    ///    and insert an empty `TaskOutputs`. Storing the empty entry
    ///    rather than skipping keeps dependents that hard-require a
    ///    key from racing the cache between "populated" and "absent";
    ///    the warn surfaces the wire-format mismatch to the operator.
    ///
    /// First-write-wins (AE-5 / C7): the cache entry is set exactly once
    /// per hash. The guard is LOAD-BEARING for the merge-driven RESTORE
    /// path — a co-present snapshot output landing on a slot a live
    /// broadcast already populated must NOT clobber the local entry
    /// (matching `restore`'s own `or_insert`). On the apply path the
    /// second-`TaskCompleted` already NoOps in `merge_task_state` BEFORE
    /// this helper fires, so the guard is a no-op there — but it is not
    /// "redundant": the restore path has no such upstream NoOp.
    ///
    /// `outputs: None` (the worker did not publish outputs) records
    /// nothing. A hash with no ledger entry (late-arriving mutation for a
    /// task this replica never saw) is silently skipped (no anchor).
    pub(super) fn record_task_outputs_value(&mut self, hash: &str, outputs: Option<TaskOutputs>) {
        let Some(outputs) = outputs else {
            return;
        };
        if !self.tasks.contains_key(hash) {
            return;
        }
        self.task_outputs.entry(hash.to_string()).or_insert(outputs);
    }

    /// Decode a `TaskCompleted` mutation's wire `result_data` payload into
    /// the [`TaskOutputs`] the cache stores. Single owner of the
    /// DonePayload decode concern (kept separate from the cache insert so
    /// the apply path decodes once and hands the value to the shared
    /// `merge_task_state`, while restore reads its already-decoded
    /// co-present snapshot value directly).
    ///
    /// `None` (no outputs committed) → `None`. A malformed payload emits a
    /// `tracing::warn!` and yields an EMPTY `TaskOutputs` (rather than
    /// `None`) so dependents that hard-require a key see a controlled-empty
    /// view rather than racing the cache between "populated" and "absent".
    pub(super) fn decode_done_payload_outputs(result_data: Option<Vec<u8>>) -> Option<TaskOutputs> {
        let bytes = result_data?;
        match serde_json::from_slice::<DonePayload>(&bytes) {
            Ok(body) => Some(body.outputs),
            Err(e) => {
                tracing::warn!(
                    target: "dynrunner_cluster_state",
                    error = %e,
                    "TaskCompleted result_data failed to decode as DonePayload; \
                     storing empty entry"
                );
                Some(TaskOutputs::default())
            }
        }
    }

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
            if let Some(blocked @ TaskState::Blocked { .. }) = self.tasks.remove(&h) {
                // The OLD term (the just-removed Blocked state) — captured
                // before we build the new Pending so the memo swap is exact.
                let old_term = super::keyspace::task_digest_term(&h, &blocked);
                let TaskState::Blocked { task, attempt, .. } = blocked else {
                    unreachable!("matched Blocked above");
                };
                resumed.push(task.clone());
                // Auto-resume is an authoritative cross-task transition
                // (Blocked → Pending), not an assignment; the fresh
                // `Pending` starts at the default version and a later
                // genuine assignment mints a higher one. The retry
                // generation (F2) is PRESERVED from the Blocked entry — a
                // cascade-resume is not a new retry attempt.
                let resumed_state = TaskState::Pending {
                    task,
                    version: Default::default(),
                    attempt,
                };
                // Range-fold memo: Blocked → Pending under a FIXED key — a
                // state CHANGE (count conserved). XOR old out, new in. The
                // `remove`+`insert` above is one logical entry staying in the
                // ledger, NOT a logical remove, so we swap (never remove).
                let new_term = super::keyspace::task_digest_term(&h, &resumed_state);
                self.range_fold_memo.swap(&h, old_term, new_term);
                self.tasks.insert(h.clone(), resumed_state);
                // #520: a cascade-resume Blocked → Pending is a
                // narration-worthy transition. This path does remove+insert
                // (not the `rewrite_task_state` seam), so emit through the
                // shared helper here. The `to_resume` filter guarantees each
                // `h` was genuinely Blocked-on the prereq, so this fires only
                // on a real resume.
                self.emit_task_state_change_for(&h);
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
    /// 3. Otherwise resolve each `task_depends_on` dep's full
    ///    `(phase_id, task_id)` identity to a hash via
    ///    [`Self::task_hash_for_dep`] and decide the initial
    ///    state per the cascade rules:
    ///      * Any dep in `Failed { kind: NonRecoverable, .. }` →
    ///        insert as `Failed { kind: NonRecoverable, task,
    ///        last_error: "upstream-failed", version: default }`
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
            if self.contains_task(&hash) {
                // Idempotent: already present in the LOGICAL ledger —
                // fat in-memory entry OR a settled (spilled) one; a
                // re-spawn must never resurrect a settled terminal.
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
            for dep in &task.task_depends_on {
                let dep_id = dep.task_id.as_str();
                // Resolve against the dep's FULL `(phase_id, task_id)`
                // identity — the same `task_id` in two phases is a
                // distinct prerequisite with a distinct hash.
                let dep_hash = match self.task_hash_for_dep(&dep.phase_id, dep_id) {
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
                // A SETTLED dep (fat body spilled) classifies off its slim
                // index class — the same per-state rules as the fat match
                // below, projected: Completed/Skipped resolve, InvalidTask
                // and Failed{NonRecoverable} cascade-fail, any other final
                // Failed kind is pending-blocked (faithful to the fat arm,
                // which only cascade-fails on NonRecoverable).
                if let Some(entry) = self.settled_entry(&dep_hash) {
                    use super::settled::SettledClass;
                    match &entry.class {
                        SettledClass::Completed
                        | SettledClass::SkippedAlreadyDone
                        | SettledClass::SetupCompleted
                        // A resolved SecondaryAffine gate satisfies the dep
                        // exactly like a succeeded setup task: the gate's
                        // dependents are schedulable the moment it is
                        // AffineReady (the READY-not-EXECUTED resolution).
                        | SettledClass::AffineReady => {}
                        SettledClass::InvalidTask => {
                            cascade_fail = true;
                            break;
                        }
                        SettledClass::FailedFinal(ErrorType::NonRecoverable) => {
                            cascade_fail = true;
                            break;
                        }
                        SettledClass::FailedFinal(_) => {
                            if blocked_on_pending.is_none() {
                                blocked_on_pending = Some(dep_hash);
                            }
                        }
                    }
                    continue;
                }
                match self.tasks.get(&dep_hash) {
                    Some(TaskState::Failed {
                        kind: ErrorType::NonRecoverable,
                        ..
                    }) => {
                        cascade_fail = true;
                        break;
                    }
                    // A dep that is itself structurally invalid is a
                    // non-recoverable upstream terminal: the dependent
                    // can never run, so it cascade-fails through the
                    // same `Failed { NonRecoverable }` shape (the
                    // "upstream-invalid" case). Keeping it out of the
                    // `invalid_task` reason space — only literally-
                    // absent deps mint a fresh `InvalidTask`; an
                    // existing-but-invalid dep cascades.
                    Some(TaskState::InvalidTask { .. }) => {
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
                    Some(TaskState::Completed { .. })
                    | Some(TaskState::SkippedAlreadyDone { .. })
                    | Some(TaskState::SetupCompleted { .. })
                    | Some(TaskState::AffineReady { .. }) => {
                        // Resolved dep — contributes nothing to the
                        // blocking decision. A skipped prereq's outputs
                        // already exist on the shared fs, so a dependent of
                        // it is unblocked exactly like a dependent of a
                        // completed task. A SUCCEEDED setup task likewise
                        // satisfies the dep (the setup-task primitive's
                        // whole point: build tasks gate on it overlapping).
                        // A resolved SecondaryAffine gate (AffineReady)
                        // satisfies the dep too — its dependents are
                        // schedulable the moment it becomes ready (the
                        // READY-not-EXECUTED resolution).
                    }
                    Some(TaskState::Pending { .. })
                    | Some(TaskState::InFlight { .. })
                    // A dep queued behind a secondary's local import is not
                    // yet terminal — the dependent stays blocked until it
                    // runs and completes, exactly like a dep on `InFlight`.
                    | Some(TaskState::QueuedAfterLocalDependency { .. })
                    | Some(TaskState::Blocked { .. }) => {
                        if blocked_on_pending.is_none() {
                            blocked_on_pending = Some(dep_hash);
                        }
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
            // Every TasksSpawned entry is a BRAND-NEW task, so it enters at
            // the cold retry generation (F2 attempt 0) regardless of which
            // initial state it classifies into.
            let initial = if cascade_fail {
                TaskState::Failed {
                    task,
                    kind: ErrorType::NonRecoverable,
                    last_error: "upstream-failed".to_string(),
                    version: Default::default(),
                    attempt: 0,
                }
            } else if let Some(on) = blocked_on_unfulfillable {
                TaskState::Blocked {
                    task,
                    on,
                    attempt: 0,
                }
            } else if let Some(on) = blocked_on_pending {
                TaskState::Blocked {
                    task,
                    on,
                    attempt: 0,
                }
            } else {
                // Surface a clone of the freshly-Pending task so a
                // receive-side caller can grow its local dispatch
                // pool. The clone is independent of the CRDT entry —
                // callers may move it into a pool via `reinject`
                // without disturbing the ledger.
                newly_pending_from_spawn.push(task.clone());
                TaskState::Pending {
                    task,
                    version: Default::default(),
                    attempt: 0,
                }
            };
            tracing::debug!(
                target: "dynrunner_cluster_state",
                hash = %hash,
                event = "task_spawned",
                state = ?std::mem::discriminant(&initial),
                "TasksSpawned: inserted entry"
            );
            // Range-fold memo: a logical CREATE — XOR the new term in + bump
            // the bucket count, off the SAME term the fold would see for this
            // fresh entry. Computed before the move into the map.
            let term = super::keyspace::task_digest_term(&hash, &initial);
            self.range_fold_memo.add(&hash, term);
            self.tasks.insert(hash.clone(), initial);
            // #520: a freshly-spawned entry is a narration-worthy transition
            // (Pending / Blocked / cascade-Failed at spawn). This batch arm
            // inserts directly (not via the `rewrite_task_state` seam), so
            // emit through the shared helper here. A re-spawn of an
            // already-present hash `continue`d above, so this only fires on a
            // genuine new entry.
            self.emit_task_state_change_for(&hash);
            applied_any = true;
        }
        if applied_any {
            ApplyOutcome::Applied
        } else {
            ApplyOutcome::NoOp
        }
    }
}
