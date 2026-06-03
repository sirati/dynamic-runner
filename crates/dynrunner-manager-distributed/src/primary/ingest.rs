//! Initial-batch ingest classification + the run-wide invalidation op.
//!
//! Single concern: turn the `PendingPool::partition_ingest` DATA into
//! the manager-side POLICY for the `invalid_task` feature —
//!   * **#2 dependency-existence** — tasks whose `task_depends_on` names
//!     a literally-absent `(phase_id, task_id)` become terminal
//!     `InvalidTask` (the cluster keeps running);
//!   * **#3a pre-phase duplicate** — a `(phase_id, task_id)` collision in
//!     the INITIAL batch aborts the whole run (`RunAborted` + a
//!     structured `RunError`);
//!   * **#3b post-phase duplicate** — handled by `invalidate_all_pending`
//!     (invoked from the runtime `SpawnTasks` path), which fails every
//!     not-yet-terminal task run-wide while the cluster CONTINUES.
//!
//! The pool returns DATA; this module (the manager) decides what
//! `ClusterMutation`s / `RunError`s that data becomes and broadcasts
//! them through the canonical `apply_and_broadcast_cluster_mutations`
//! pipeline. The 3a/3b discriminator is `phase_started_emitted.is_empty()`:
//! the initial-batch ingest runs BEFORE `fire_initial_phase_starts`, so
//! it is unconditionally 3a; the runtime `SpawnTasks` path runs after a
//! phase has started, so it is 3b.

use dynrunner_core::{BoundedString, ErrorType, Identifier, TaskInfo};
use dynrunner_protocol_primary_secondary::{ClusterMutation, PeerTransport};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use crate::cluster_state::TaskState;
use crate::primary::wire::compute_task_hash;
use crate::primary::{PrimaryCoordinator, RunError};

impl<Tr: PeerTransport<I>, S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier>
    PrimaryCoordinator<Tr, S, E, I>
{
    /// Classify + commit the INITIAL task batch, the `extend`
    /// replacement on the bootstrap path.
    ///
    /// Runs `PendingPool::partition_ingest` (keyed on the full
    /// `(phase_id, task_id)`), then routes each partition:
    ///   * **duplicates** (non-empty) → this is the pre-phase case (#3a)
    ///     by construction (the caller invokes this BEFORE
    ///     `fire_initial_phase_starts`, so `phase_started_emitted` is
    ///     empty). Record the abort directive in `self.pending_run_abort`
    ///     so `run_pipeline` can broadcast `RunAborted` + return the
    ///     structured `RunError` once secondaries have connected. The
    ///     batch is NOT committed — the run is doomed.
    ///   * **invalid_deps** (#2 missing-dep) → the survivors' pool
    ///     `extend` must still see these ids as "known" so a valid
    ///     dependent neither fails `extend` nor strands: pre-seed the
    ///     pool's `failed_tasks` with each invalid-dep `task_id` (which
    ///     also cascade-drops any pool survivor that depends on one,
    ///     matching the runtime cascade). Keep them in `all_binaries` so
    ///     `seed_cluster_state` adds them to the CRDT as `Pending`, and
    ///     stash them in `self.pending_invalid_dep_tasks` so
    ///     `run_pipeline` emits `TaskFailed { InvalidTask }` for each
    ///     after the seed.
    ///   * **valid** → handed to `extend`, preserving its atomic
    ///     contract (a CYCLE among valid tasks is still a hard
    ///     `PendingPoolError` surfaced as `RunError::Other`).
    ///
    /// `self.all_binaries` is set to `valid ∪ invalid_deps` (NOT the
    /// duplicates — on the abort path the run never seeds). On the
    /// abort path the pool is left untouched and the caller short-
    /// circuits at the abort gate.
    pub(crate) fn ingest_initial_batch(&mut self, batch: Vec<TaskInfo<I>>) -> Result<(), RunError> {
        let partition = self.pool().partition_ingest(batch);

        // #3a: a duplicate in the INITIAL batch aborts the whole run.
        // Discriminator is structural: this runs before
        // `fire_initial_phase_starts`, so `phase_started_emitted` is
        // empty — unconditionally pre-phase. Record the directive and
        // return cleanly; the bootstrap proceeds to connect secondaries
        // and `run_pipeline` fires the abort at the gate so the
        // `RunAborted` broadcast actually reaches them.
        if !partition.duplicates.is_empty() {
            debug_assert!(
                self.phase_started_emitted.is_empty(),
                "ingest_initial_batch must run before fire_initial_phase_starts \
                 (the 3a/3b discriminator); a non-empty phase_started_emitted \
                 here means the ingest order regressed"
            );
            let reasons: Vec<String> = partition
                .duplicates
                .iter()
                .map(|(_, reason)| reason.clone())
                .collect();
            let reason = format!(
                "{} duplicate task identity/identities in the initial batch: {}",
                partition.duplicates.len(),
                reasons.join("; ")
            );
            tracing::error!(reason = %reason, "initial-batch duplicate detected; aborting run");
            self.pending_run_abort = Some(reason);
            // Do not seed / dispatch a doomed run.
            self.all_binaries = Vec::new();
            self.total_tasks = 0;
            return Ok(());
        }

        // #2: pre-seed the pool's failed set with the missing-dep ids so
        // the survivors' dep-existence + extend-time cascade stay
        // correct, then extend ONLY the valid subset.
        let invalid_ids: Vec<String> = partition
            .invalid_deps
            .iter()
            .map(|(task, _)| task.task_id.clone())
            .collect();
        self.pool_mut().mark_tasks_failed(invalid_ids);

        let valid = partition.valid;
        // `all_binaries` keeps BOTH the valid survivors and the
        // invalid-dep tasks: the latter must be seeded into the CRDT as
        // `Pending` (so the `TaskFailed { InvalidTask }` emit has a
        // target) and counted in `total_tasks` (so the operational
        // loop's exit denominator accounts for them — they terminate as
        // InvalidTask, not as stranded).
        let mut all: Vec<TaskInfo<I>> =
            Vec::with_capacity(valid.len() + partition.invalid_deps.len());
        all.extend(valid.iter().cloned());
        for (task, reason) in &partition.invalid_deps {
            all.push(task.clone());
            self.pending_invalid_dep_tasks
                .push((task.clone(), reason.clone()));
        }
        self.all_binaries = all;
        self.total_tasks = self.all_binaries.len();

        self.pool_mut().extend(valid).map_err(|e| {
            RunError::Other(format!("PendingPool::extend rejected task graph: {e}"))
        })?;
        Ok(())
    }

    /// Broadcast the pending #3a abort, if one was recorded at ingest.
    ///
    /// Called from `run_pipeline` AFTER `wait_for_connections` so the
    /// `RunAborted` broadcast reaches the connected secondaries (at
    /// ingest time none were connected yet). Returns `Err(RunError::
    /// DuplicateTaskIdPrePhase)` so the primary's own PyO3 boundary
    /// surfaces a non-zero exit; `Ok(())` when no abort is pending (the
    /// clean path). Single read of `pending_run_abort`.
    pub(crate) async fn fire_pending_run_abort(&mut self) -> Result<(), RunError> {
        let Some(reason) = self.pending_run_abort.take() else {
            return Ok(());
        };
        // Same broadcast/apply/settle path as `RunComplete`, so the
        // abort inherits the identical delivery semantics — the CRDT
        // `run_aborted` flag lands on every connected secondary and its
        // `process_tasks` loop returns `RunOutcome::Aborted`.
        self.apply_and_broadcast_cluster_mutations(vec![ClusterMutation::RunAborted {
            reason: reason.clone(),
        }])
        .await;
        // Brief settle window so the broadcast lands before the
        // dispatcher tears down its transport — the same
        // `PRIMARY_BROADCAST_SETTLE` window the `RunComplete` path uses.
        tokio::time::sleep(crate::primary::PRIMARY_BROADCAST_SETTLE).await;
        Err(RunError::DuplicateTaskIdPrePhase { reason })
    }

    /// Emit `TaskFailed { kind: InvalidTask }` for every missing-dep
    /// task recorded at ingest (#2). Called from `run_pipeline` AFTER
    /// `seed_cluster_state` — the tasks are then `Pending` in the CRDT,
    /// so the `TaskFailed` apply rule transitions each `Pending →
    /// InvalidTask` and fans a `TaskCompletedEvent` (carrying
    /// `error_kind = "invalid_task:<reason>"`) to the dispatcher, which
    /// is the framework's emission for the observer's invalid_task
    /// monitor — no extra wiring. The cluster keeps running.
    ///
    /// One mutation per task; routed through the canonical
    /// `apply_and_broadcast_cluster_mutations` pipeline. Drains
    /// `self.pending_invalid_dep_tasks`.
    pub(crate) async fn emit_invalid_dep_tasks(&mut self) {
        if self.pending_invalid_dep_tasks.is_empty() {
            return;
        }
        let pending = std::mem::take(&mut self.pending_invalid_dep_tasks);
        let mut mutations: Vec<ClusterMutation<I>> = Vec::with_capacity(pending.len());
        for (task, reason) in pending {
            let hash = compute_task_hash(&task);
            tracing::warn!(
                task_id = %task.task_id,
                phase = %task.phase_id,
                reason = %reason,
                "task has a missing dependency; marking invalid_task"
            );
            mutations.push(ClusterMutation::TaskFailed {
                hash,
                kind: ErrorType::InvalidTask {
                    reason: BoundedString::from(reason),
                },
                error: "missing dependency".to_string(),
            });
        }
        self.apply_and_broadcast_cluster_mutations(mutations).await;
    }

    /// Fail every not-yet-terminal task across the WHOLE run as
    /// `InvalidTask` — the #3b op (a duplicate detected AFTER a phase
    /// started). The cluster CONTINUES (no `RunAborted`); the run simply
    /// produces a fully-invalid outcome because the duplicate made the
    /// task set ambiguous.
    ///
    /// Scans `cluster_state.tasks_iter()` and emits a `TaskFailed
    /// { kind: InvalidTask }` for every entry in a non-terminal state
    /// (`Pending` / `InFlight` / `Blocked`). Already-terminal entries
    /// (`Completed` / `Failed` / `InvalidTask`) and the settled
    /// `Unfulfillable` failure-class entries are skipped — the
    /// `TaskFailed` apply rule's terminal lockout would NoOp them
    /// anyway, so the skip keeps the broadcast minimal and the reasons
    /// accurate. Each emitted entry's `Pending|InFlight|Blocked →
    /// InvalidTask` transition fans a `TaskCompletedEvent` to the
    /// dispatcher exactly like the #2 path.
    ///
    /// Routed through the canonical broadcast/apply pipeline. The pool's
    /// own copies of the now-invalid tasks drain via the normal
    /// terminal-observation paths.
    pub(crate) async fn invalidate_all_pending(&mut self, reason: String) {
        // Collect targets first (immutable borrow), then build the
        // mutation batch — `apply_and_broadcast` takes `&mut self`.
        let targets: Vec<(String, TaskInfo<I>)> = self
            .cluster_state
            .tasks_iter()
            .filter_map(|(hash, state)| match state {
                TaskState::Pending { task }
                | TaskState::InFlight { task, .. }
                | TaskState::Blocked { task, .. } => Some((hash.clone(), task.clone())),
                TaskState::Completed { .. }
                | TaskState::Failed { .. }
                | TaskState::Unfulfillable { .. }
                | TaskState::InvalidTask { .. } => None,
            })
            .collect();
        if targets.is_empty() {
            return;
        }
        tracing::error!(
            count = targets.len(),
            reason = %reason,
            "duplicate task identity after a phase started; invalidating all \
             not-yet-terminal tasks run-wide (cluster continues)"
        );
        let mut mutations: Vec<ClusterMutation<I>> = Vec::with_capacity(targets.len());
        for (hash, _task) in targets {
            mutations.push(ClusterMutation::TaskFailed {
                hash,
                kind: ErrorType::InvalidTask {
                    reason: BoundedString::from(reason.clone()),
                },
                error: "run-wide invalidation (duplicate task identity)".to_string(),
            });
        }
        self.apply_and_broadcast_cluster_mutations(mutations).await;
    }
}
