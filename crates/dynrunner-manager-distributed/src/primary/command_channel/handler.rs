//! Per-variant command handlers. The `handle_primary_command` entry
//! is the single match called from the operational loop's `select!`;
//! each arm forwards to an `apply_*` method on `PrimaryCoordinator`
//! defined below so the mutation's state-machine semantics stay
//! co-located with the rest of the coordinator's state.

use dynrunner_core::{ErrorType, Identifier, TaskInfo};
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, PeerTransport, SecondaryTransport,
};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use crate::cluster_state::TaskState;
use crate::primary::PrimaryCoordinator;

use super::types::{PrimaryCommand, SpawnError};


/// Dispatch one received command to its handler. Single line at the
/// `select!` call site keeps the operational-loop's match arm
/// transport-shape-pure.
pub async fn handle_primary_command<T, P, S, E, I>(
    coordinator: &mut PrimaryCoordinator<T, P, S, E, I>,
    command: PrimaryCommand<I>,
) where
    T: SecondaryTransport<I>,
    P: PeerTransport<I>,
    S: Scheduler<I>,
    E: ResourceEstimator<I>,
    I: Identifier,
{
    match command {
        PrimaryCommand::FailPermanent {
            hash,
            error,
            reason,
            reply,
        } => {
            let result = coordinator
                .apply_fail_permanent(hash, error, reason)
                .await;
            let _ = reply.send(result);
        }
        PrimaryCommand::ReinjectTask { hash, reply } => {
            let result = coordinator.apply_reinject_task(hash).await;
            let _ = reply.send(result);
        }
        PrimaryCommand::UpdatePreferredSecondaries {
            hash,
            secondaries,
            reply,
        } => {
            let result = coordinator
                .apply_update_preferred_secondaries(hash, secondaries)
                .await;
            let _ = reply.send(result);
        }
        PrimaryCommand::SpawnTasks { tasks, reply } => {
            let result = coordinator.apply_spawn_tasks(tasks).await;
            let _ = reply.send(result);
        }
    }
}

impl<T, P, S, E, I> PrimaryCoordinator<T, P, S, E, I>
where
    T: SecondaryTransport<I>,
    P: PeerTransport<I>,
    S: Scheduler<I>,
    E: ResourceEstimator<I>,
    I: Identifier,
{
    /// Resolve a task hash through the CRDT ledger and return
    /// `(phase_id, task_id)` for the pool's bookkeeping. The CRDT is
    /// the single authoritative source for the post-failure metadata
    /// the pool needs; the local `pending_pool` doesn't itself index
    /// by hash.
    pub(super) fn task_meta_for_hash(
        &self,
        hash: &str,
    ) -> Option<(dynrunner_core::PhaseId, Option<String>)> {
        let state = self.cluster_state.task_state(hash)?;
        let task = match state {
            TaskState::Pending { task }
            | TaskState::InFlight { task, .. }
            | TaskState::Completed { task }
            | TaskState::Failed { task, .. }
            | TaskState::Unfulfillable { task, .. }
            | TaskState::Blocked { task, .. }
            | TaskState::Cancelled { task, .. } => task,
        };
        Some((task.phase_id.clone(), task.task_id.clone()))
    }

    /// Handler for `PrimaryCommand::FailPermanent`. Wraps the existing
    /// `pending_pool::on_item_failed_permanent` primitive so the
    /// cascade-to-dependents semantics that primitive owns also apply
    /// to externally-requested failures, then broadcasts the
    /// `TaskFailed` mutation so every node mirrors the terminal state.
    ///
    /// Cascade routing splits on `error`:
    /// * `ErrorType::Unfulfillable { .. }` — dependents are broadcast
    ///   as `ClusterMutation::TaskBlocked { hash, on: <root> }`, so
    ///   the CRDT mirrors land in `TaskState::Blocked { on, task }`
    ///   on every replica. The matching `TaskCompleted` apply arm
    ///   auto-resumes them to `Pending` when the prereq later
    ///   completes via the reinject + re-run path. Dependents are
    ///   NOT recorded in the local per-pass `failed_tasks` ledger —
    ///   they're cascade-paused, not failed.
    /// * Any other `ErrorType` — dependents are recorded in the local
    ///   `failed_tasks` ledger with the same error (the legacy shape
    ///   a worker-driven cascade-fail produces).
    ///
    /// Pool-side auto-resume of cascade-paused dependents is wired
    /// through `apply_and_broadcast_cluster_mutations`: when the
    /// prereq's `TaskCompleted` later flows through the apply path,
    /// `cluster_state::apply_locally_for_broadcast` surfaces every
    /// just-resumed `TaskInfo<I>` and the caller re-injects each
    /// into the live `PendingPool` so the next dispatch tick picks
    /// them up. The CRDT and pool stay coherent without a per-task
    /// re-cascade walk here.
    pub(super) async fn apply_fail_permanent(
        &mut self,
        hash: String,
        error: ErrorType,
        reason: String,
    ) -> Result<(), String> {
        let Some((phase_id, task_id)) = self.task_meta_for_hash(&hash) else {
            return Err(format!(
                "fail_permanent: unknown task hash {hash}"
            ));
        };
        // Record the failure in the local per-pass ledger so the
        // operational loop's accounting + the per-phase counters match
        // the wire-side state. Mirrors `handle_task_failed`'s
        // `failed_tasks.insert(...)` step (the same in-memory side-
        // effect a worker-originated failure would have).
        self.failed_tasks.insert(hash.clone(), error.clone());

        // Cascade-to-dependents via the pool primitive. The returned
        // list is the dependents that the pool just gave up on; how
        // the caller observes them depends on the error class
        // (cascade-pause for Unfulfillable, cascade-fail otherwise).
        let cascaded_blocks: Vec<(String, String)> = if let Some(id) = task_id.as_deref() {
            let cascaded = self
                .pool_mut()
                .on_item_failed_permanent(&phase_id, id);
            let is_unfulfillable = matches!(error, ErrorType::Unfulfillable { .. });
            let mut blocks = Vec::new();
            for cascaded_binary in &cascaded {
                let cascaded_hash =
                    crate::primary::wire::compute_task_hash(cascaded_binary);
                if is_unfulfillable {
                    blocks.push((cascaded_hash, hash.clone()));
                } else {
                    self.failed_tasks
                        .insert(cascaded_hash, error.clone());
                }
            }
            blocks
        } else {
            Vec::new()
        };

        // Phase + lifecycle bookkeeping. Must run AFTER the pool
        // mutation so `process_phase_lifecycle` observes the post-
        // cascade pool state.
        self.note_item_failed(&phase_id, task_id.as_deref());

        // Broadcast the terminal state for the originating task plus
        // any cascade-paused dependents (Unfulfillable case only).
        // The CRDT-applied broadcast is the single source of truth
        // for every observer; ordering the originating TaskFailed
        // first means receivers see the prereq's Unfulfillable state
        // before the dependents' Blocked state — the cascade root is
        // visible whenever a dependent's `on` field is consulted.
        let mut mutations: Vec<ClusterMutation<I>> = Vec::with_capacity(
            1 + cascaded_blocks.len(),
        );
        mutations.push(ClusterMutation::TaskFailed {
            hash,
            kind: error,
            error: reason,
        });
        for (dep_hash, on_hash) in cascaded_blocks {
            mutations.push(ClusterMutation::TaskBlocked {
                hash: dep_hash,
                on: on_hash,
            });
        }
        self.apply_and_broadcast_cluster_mutations(mutations).await;
        Ok(())
    }

    /// Handler for `PrimaryCommand::ReinjectTask`. Accepts only entries
    /// whose CRDT state is the discrete `TaskState::Unfulfillable { .. }`
    /// — the operator-resolvable-failure class. Decrements the per-task
    /// budget; on exhaustion the local state stays `Unfulfillable` and
    /// the caller receives `Err`.
    pub(super) async fn apply_reinject_task(
        &mut self,
        hash: String,
    ) -> Result<(), String> {
        // Inspect CRDT state first — the local pool isn't indexed by
        // hash, and the discrete-variant gate has to read the
        // authoritative ledger.
        let binary = match self.cluster_state.task_state(&hash) {
            Some(TaskState::Unfulfillable { task, .. }) => task.clone(),
            Some(_) => {
                return Err(format!(
                    "reinject_task: hash {hash} not in Unfulfillable state"
                ));
            }
            None => {
                return Err(format!(
                    "reinject_task: unknown task hash {hash}"
                ));
            }
        };

        // Budget check. None == unbounded (the bypass branch);
        // `Some(0)` means "exhausted, refuse"; `Some(n>0)` decrements
        // and proceeds. The map is initialised lazily — first reinject
        // for a hash seeds the counter from the configured cap.
        let max = self.config.unfulfillable_reinject_max_per_task;
        if let Some(cap) = max {
            let remaining = self
                .unfulfillable_reinject_remaining
                .entry(hash.clone())
                .or_insert(cap);
            if *remaining == 0 {
                tracing::warn!(
                    task_hash = %hash,
                    cap,
                    event = "unfulfillable_reinject_budget_exhausted",
                    "reinject budget exhausted for task; staying Failed"
                );
                return Err(format!(
                    "reinject_task: budget exhausted for hash {hash} \
                     (cap={cap})"
                ));
            }
            *remaining -= 1;
        }

        // Local pool reinject: same primitive the retry-pass code path
        // uses. Re-injecting flips Drained/Done phase state back to
        // Active for this binary's phase, putting the item back into
        // the bucket head so the next dispatch tick picks it up.
        self.failed_tasks.remove(&hash);
        self.pool_mut().reinject(binary);

        // Broadcast so every node's CRDT mirror moves the entry off
        // `Failed` synchronously.
        self.apply_and_broadcast_cluster_mutations(vec![
            ClusterMutation::TaskReinjected { hash },
        ])
        .await;
        Ok(())
    }

    /// Handler for `PrimaryCommand::UpdatePreferredSecondaries`.
    /// Broadcasts the per-task preferred-secondaries update so every
    /// node's CRDT mirror sees the new preference list AND mirrors
    /// the new list onto the live primary's `PendingPool` entry so
    /// the next scheduler tick reads the updated preference. The
    /// pool stores `TaskInfo<I>` clones (taken at injection time);
    /// without this mirror the CRDT write would only become visible
    /// to the scheduler on a snapshot-restore cycle — every
    /// dispatch between the two would see the stale preference list.
    ///
    /// The pool match is keyed on the wire-canonical task hash via
    /// the generic `pool::update_first_match_in_place` primitive,
    /// so the pool itself stays oblivious to hashing.
    pub(super) async fn apply_update_preferred_secondaries(
        &mut self,
        hash: String,
        secondaries: Vec<String>,
    ) -> Result<(), String> {
        if self.cluster_state.task_state(&hash).is_none() {
            return Err(format!(
                "update_preferred_secondaries: unknown task hash {hash}"
            ));
        }
        // Mirror onto the live pool's TaskInfo clone. Done BEFORE the
        // broadcast so a hypothetical synchronous reader of the pool
        // (post-apply, pre-broadcast) sees the new preferences and
        // the CRDT-side mirror simultaneously. The hash-keyed
        // predicate closes over `compute_task_hash`; the pool API
        // takes any predicate so it doesn't have to learn about
        // wire-canonical hashing.
        let target_hash = hash.clone();
        let new_preferences = dynrunner_core::SoftPreferredSecondaries::new(
            secondaries.clone(),
        );
        let matched = self.pool_mut().update_first_match_in_place(
            |t| crate::primary::wire::compute_task_hash(t) == target_hash,
            |t| t.preferred_secondaries = new_preferences.clone(),
        );
        if !matched {
            // The pool may legitimately not hold the binary (in-flight
            // / completed / not yet seeded), and that's fine — only
            // queued/blocked items need the live mirror. CRDT side
            // still broadcasts so every replica's `TaskInfo` clone
            // converges on the new preference list.
            tracing::debug!(
                task_hash = %hash,
                "update_preferred_secondaries: hash not present in pool; \
                 CRDT mirror only"
            );
        }
        self.apply_and_broadcast_cluster_mutations(vec![
            ClusterMutation::TaskPreferredSecondariesUpdated {
                hash,
                secondaries,
            },
        ])
        .await;
        Ok(())
    }

    /// Handler for `PrimaryCommand::SpawnTasks`. Pre-validates every
    /// input task (duplicate-hash + unknown-dependency check) against
    /// the current cluster ledger, builds a single
    /// `ClusterMutation::TasksSpawned` carrying the valid subset, and
    /// applies+broadcasts it. Returns per-index errors for the
    /// rejected entries; the rest of the batch proceeds regardless.
    ///
    /// Post-apply, every freshly-Pending task is re-injected into the
    /// live primary's `PendingPool` so the next dispatch tick picks
    /// it up. Tasks that landed in `Blocked` are not pool-resident
    /// (they wait for the auto-resume mechanism in
    /// `resume_blocked_on` to fire on a later `TaskCompleted`). Tasks
    /// that landed in `Failed { NonRecoverable, .. }` (cascade-fail
    /// against an upstream `Failed { NonRecoverable, .. }` dep) are
    /// recorded in the per-pass `failed_tasks` ledger so the
    /// operational loop's accounting matches the wire-side state —
    /// same shape `apply_fail_permanent` produces for worker-
    /// originated permanent failures.
    pub(super) async fn apply_spawn_tasks(
        &mut self,
        tasks: Vec<TaskInfo<I>>,
    ) -> Result<Vec<(usize, SpawnError)>, String> {
        let mut errors: Vec<(usize, SpawnError)> = Vec::new();
        let mut valid_tasks: Vec<TaskInfo<I>> = Vec::with_capacity(tasks.len());
        // Build a set of task_ids the pre-validation pass treats as
        // known: every task_id in the existing ledger PLUS every
        // task_id contributed by the input batch (so within-batch
        // dependencies validate). The wire-side apply rule does its
        // own dep resolution per-task; this pre-pass surfaces failures
        // for the caller before the broadcast happens.
        let mut known_task_ids: std::collections::HashSet<String> = self
            .cluster_state
            .tasks_iter()
            .filter_map(|(_, s)| {
                let task = match s {
                    crate::cluster_state::TaskState::Pending { task }
                    | crate::cluster_state::TaskState::InFlight { task, .. }
                    | crate::cluster_state::TaskState::Completed { task }
                    | crate::cluster_state::TaskState::Failed { task, .. }
                    | crate::cluster_state::TaskState::Unfulfillable { task, .. }
                    | crate::cluster_state::TaskState::Blocked { task, .. }
                    | crate::cluster_state::TaskState::Cancelled { task, .. } => task,
                };
                task.task_id.clone()
            })
            .collect();
        for task in &tasks {
            if let Some(id) = task.task_id.as_deref() {
                known_task_ids.insert(id.to_string());
            }
        }
        // Per-task validation pass. A task can fail multiple checks
        // (duplicate hash AND unknown dep); we surface the FIRST
        // failure per index so the caller sees one error per rejected
        // task. Duplicate-hash is checked first because it short-
        // circuits the rest of the task's checks: a hash collision
        // means the task is already in the ledger and re-validating
        // its deps against the existing entry would be redundant.
        for (idx, task) in tasks.into_iter().enumerate() {
            let hash = crate::primary::wire::compute_task_hash(&task);
            if self.cluster_state.task_state(&hash).is_some() {
                errors.push((idx, SpawnError::DuplicateTaskHash(hash)));
                continue;
            }
            let mut bad_dep: Option<String> = None;
            for dep_id in &task.task_depends_on {
                if !known_task_ids.contains(dep_id) {
                    bad_dep = Some(dep_id.clone());
                    break;
                }
            }
            if let Some(dep_task_id) = bad_dep {
                errors.push((
                    idx,
                    SpawnError::UnknownDependency {
                        task_hash: hash,
                        dep_task_id,
                    },
                ));
                continue;
            }
            valid_tasks.push(task);
        }

        if valid_tasks.is_empty() {
            // No mutation to broadcast; the per-index errors are the
            // entire result. Skip the apply+broadcast pass so we
            // don't emit an empty-batch wire event.
            return Ok(errors);
        }

        // Compute hashes of the valid subset so we can post-apply
        // inspect each entry's CRDT state to decide pool-side
        // bookkeeping. The hash function is deterministic; the
        // apply rule recomputes the same value internally, so the
        // hashes here line up with cluster_state's HashMap keys.
        let valid_hashes: Vec<String> = valid_tasks
            .iter()
            .map(crate::primary::wire::compute_task_hash)
            .collect();

        self.apply_and_broadcast_cluster_mutations(vec![
            ClusterMutation::TasksSpawned {
                tasks: valid_tasks,
            },
        ])
        .await;

        // Pool-side bookkeeping for the live primary. Read every
        // valid entry's post-apply state and route by classification:
        //   * Pending → reinject into the pool so the next dispatch
        //     tick picks it up. `reinject` is the right primitive
        //     here (vs `extend`): the pool's dep-tracking is the
        //     CRDT's concern post-Phase-B, the pool just dispatches
        //     what's in it.
        //   * Blocked → CRDT auto-resume on a later `TaskCompleted`
        //     fires `resume_blocked_on`; the existing
        //     `apply_and_broadcast_cluster_mutations` plumb
        //     re-injects via `resumed_for_dispatch`. No pool action
        //     here.
        //   * Failed → record in the in-pass `failed_tasks` ledger so
        //     accounting matches the wire-side state. Same shape
        //     `apply_fail_permanent` produces for the legacy
        //     cascade-fail path.
        for hash in valid_hashes {
            match self.cluster_state.task_state(&hash) {
                Some(TaskState::Pending { task }) => {
                    let task = task.clone();
                    self.pool_mut().reinject(task);
                }
                Some(TaskState::Failed { kind, .. }) => {
                    self.failed_tasks.insert(hash, kind.clone());
                }
                _ => {
                    // Blocked / other states: no pool-side action.
                }
            }
        }

        Ok(errors)
    }
}

