//! Cross-thread / cross-async-runtime ingress for "from outside the
//! operational loop, please apply this mutation to the running primary".
//!
//! Single concern: a typed, reply-bearing command channel whose receiver
//! is read inside the primary's operational-loop `select!` and whose
//! sender is cloned out to consumers (PyO3 `PrimaryHandle`, future
//! Rust-side control-plane callers). Each command carries a
//! `oneshot::Sender<Result<...>>` so the caller can block / await the
//! handler's outcome and surface success / failure synchronously.
//!
//! Module boundary:
//!   * Owns the `PrimaryCommand<I>` enum and the handler entry
//!     `handle_primary_command`. The handlers themselves dispatch back
//!     into `PrimaryCoordinator` methods (`apply_fail_permanent`,
//!     `apply_reinject_task`, `apply_update_preferred_secondaries`) so
//!     each mutation's *implementation* stays co-located with the rest
//!     of the coordinator's state-machine semantics.
//!   * The operational loop's `select!` arm (`lifecycle.rs`) calls
//!     `handle_primary_command(self, cmd).await` — single line, no
//!     per-variant logic at the call site.
//!
//! What callers see (Python and Rust):
//!   * `mpsc::Sender<PrimaryCommand<I>>` — clone, build a command +
//!     `oneshot::channel()`, `send().await`, then `await` the reply.
//!   * `oneshot::Sender::send`-side error paths on the handler side
//!     are non-fatal: a dropped `reply` receiver just means the caller
//!     stopped caring (e.g. timed out, panicked). No coordinator state
//!     change rolls back on `reply.send(...)` failing.
//!
//! Capacity: the inbound channel is bounded (`COMMAND_CHANNEL_CAPACITY`)
//! so a runaway caller can't OOM the primary. Backpressure surfaces to
//! the sender side as a slow `send().await`; the handler-side reply
//! oneshot is the per-command flow-control signal.
//!
//! # Wire / CRDT effects
//!
//! Each handler routes through the same
//! `apply_and_broadcast_cluster_mutations` primitive the rest of the
//! coordinator uses, so the live primary's local apply and the cluster-
//! wide CRDT broadcast happen together. Variants:
//!   * `FailPermanent` — drives `pending_pool::on_item_failed_permanent`
//!     (with the cascade-to-dependents semantics that primitive owns)
//!     and broadcasts `TaskFailed { kind: NonRecoverable, .. }`.
//!   * `ReinjectTask` — accepts only entries whose CRDT state is the
//!     discrete `TaskState::Unfulfillable { .. }` variant (the
//!     operator-resolvable-failure class); flips the local pool's
//!     Unfulfillable → re-injected, broadcasts `TaskReinjected{hash}`,
//!     and decrements the per-task budget
//!     `unfulfillable_reinject_remaining[hash]` (initialised from
//!     `PrimaryConfig::unfulfillable_reinject_max_per_task`; `None`
//!     means unbounded). Budget exhaustion is a structured-log event,
//!     never a panic.
//!   * `UpdatePreferredSecondaries` — broadcasts
//!     `TaskPreferredSecondariesUpdated{hash, secondaries}` so every
//!     node's mirror sees the same update. Local `TaskInfo`-side
//!     storage of the field lands in Phase 4; the command-variant +
//!     reply path are in place today so the PyO3 surface can ship.

use dynrunner_core::{ErrorType, Identifier, TaskInfo};
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, PeerTransport, SecondaryTransport,
};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};
use tokio::sync::oneshot;

use crate::cluster_state::TaskState;
use super::PrimaryCoordinator;

/// Bounded capacity for the command channel. Sized so a noisy caller
/// can't OOM the primary while still giving multi-command batches
/// (e.g. a control-plane that emits N `UpdatePreferredSecondaries`
/// commands in a tight loop) some slack before backpressure kicks in.
pub const COMMAND_CHANNEL_CAPACITY: usize = 256;

/// Per-task error returned by `PrimaryCommand::SpawnTasks` from the
/// pre-apply validation pass.
///
/// Vec-wide failure modes (channel closed, oneshot dropped) surface
/// as the outer `Result<_, String>` on the reply oneshot; per-task
/// failure modes (one entry in the input vec) ride inside the
/// returned `Vec<(usize, SpawnError)>`. The rest of the input vec
/// proceeds regardless — `DuplicateTaskHash` and `UnknownDependency`
/// are per-task failures, not vec-aborts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpawnError {
    /// The task's wire-canonical content hash collides with an entry
    /// already present in the cluster ledger. The originator-side
    /// pre-validation catches this; the apply rule itself is also
    /// idempotent (a duplicate is silently NoOp'd) so the wire layer
    /// never regresses prior state.
    DuplicateTaskHash(String),
    /// The task's `task_depends_on` references a task_id that does
    /// not resolve to any entry in the current cluster ledger.
    /// `task_hash` is the wire-canonical hash of the task that
    /// carried the bad reference; `dep_task_id` is the missing
    /// dependency's task_id.
    UnknownDependency {
        task_hash: String,
        dep_task_id: String,
    },
}

/// One in-flight command on the `PrimaryHandle` → coordinator channel.
///
/// Generic over `I: Identifier` because [`PrimaryCommand::SpawnTasks`]
/// carries `Vec<TaskInfo<I>>` for runtime task injection. The other
/// variants still address tasks by their content hash (`String`) and
/// would be `I`-free in isolation; carrying the generic on the enum
/// keeps every variant on the same channel and lets the receiver be
/// `tokio_mpsc::Receiver<PrimaryCommand<I>>` matching the
/// coordinator's own `I` parameter. The PyO3 layer specialises
/// `I = RunnerIdentifier` (the only concrete identifier type Python
/// can construct).
pub enum PrimaryCommand<I: Identifier> {
    /// Apply `pending_pool::on_item_failed_permanent` + cascade for the
    /// named hash and broadcast `ClusterMutation::TaskFailed{
    /// NonRecoverable, error }`.
    ///
    /// Error path: unknown hash → `Err(...)`. The handler's `reply`
    /// carries the result; the coordinator's own state is unchanged on
    /// the error arm.
    FailPermanent {
        hash: String,
        error: ErrorType,
        reason: String,
        reply: oneshot::Sender<Result<(), String>>,
    },

    /// External-control reinjection: accept iff the named hash is in
    /// `TaskState::Unfulfillable { .. }` (the discrete state for the
    /// operator-resolvable-failure class) and there's at least one
    /// reinjection ticket left in `unfulfillable_reinject_remaining[hash]`.
    /// On accept, transition Unfulfillable→Pending and broadcast
    /// `ClusterMutation::TaskReinjected{ hash }`. On budget exhaustion,
    /// emit the `unfulfillable_reinject_budget_exhausted` structured
    /// log event and return `Err` to the caller — the local state
    /// stays `Unfulfillable` (no regression).
    ReinjectTask {
        hash: String,
        reply: oneshot::Sender<Result<(), String>>,
    },

    /// External-control update of the named task's preferred-secondaries
    /// list. Broadcasts `ClusterMutation::TaskPreferredSecondariesUpdated`
    /// so every node's mirror picks up the new preference list; the
    /// Phase-4 `TaskInfo.preferred_secondaries` storage owns the
    /// in-memory side once it lands.
    UpdatePreferredSecondaries {
        hash: String,
        secondaries: Vec<String>,
        reply: oneshot::Sender<Result<(), String>>,
    },

    /// Runtime task injection: a batch of brand-new `TaskInfo<I>`
    /// entries to add to the cluster ledger so the live primary
    /// dispatches them and every replica's CRDT mirror converges.
    ///
    /// The handler [`PrimaryCoordinator::apply_spawn_tasks`]
    /// performs per-task pre-validation (duplicate-hash, unknown
    /// dependency) and emits a single
    /// `ClusterMutation::TasksSpawned { tasks: <valid subset> }`
    /// mutation; the reply oneshot returns a per-index `SpawnError`
    /// for every input task that failed validation (empty `Vec` =
    /// full success). The outer `Result<_, String>` carries
    /// channel-wide failure modes (unknown internal state, broadcast
    /// retry exhaustion). One wire-broadcast event per call,
    /// regardless of batch size.
    SpawnTasks {
        tasks: Vec<TaskInfo<I>>,
        reply: oneshot::Sender<Result<Vec<(usize, SpawnError)>, String>>,
    },
}

/// Dispatch one received command to its handler. Single line at the
/// `select!` call site keeps the operational-loop's match arm
/// transport-shape-pure.
pub(super) async fn handle_primary_command<T, P, S, E, I>(
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
            | TaskState::Blocked { task, .. } => task,
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
                    super::wire::compute_task_hash(cascaded_binary);
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
            |t| super::wire::compute_task_hash(t) == target_hash,
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
    ) -> Result<Vec<(usize, super::command_channel::SpawnError)>, String> {
        use super::command_channel::SpawnError;
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
                    | crate::cluster_state::TaskState::Blocked { task, .. } => task,
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
            let hash = super::wire::compute_task_hash(&task);
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
            .map(super::wire::compute_task_hash)
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::time::Duration;

    use dynrunner_core::ErrorType;
    use dynrunner_scheduler::ResourceStealingScheduler;
    use tokio::sync::oneshot;

    use crate::primary::test_helpers::{
        make_binary, setup_test, FixedEstimator, NoPeers, TestId,
    };
    use crate::primary::wire::compute_task_hash;
    use crate::primary::PrimaryConfig;
    use crate::primary::PrimaryCoordinator;
    use crate::cluster_state::TaskState;
    use dynrunner_transport_channel::ChannelSecondaryTransportEnd;

    /// Build a `PrimaryCoordinator` against the in-process channel
    /// transport stub used by the rest of the primary tests. We
    /// don't drive a full run; the tests below call the command-
    /// channel handlers directly to assert per-command semantics
    /// without coupling to the operational loop's exit conditions.
    fn make_coordinator() -> PrimaryCoordinator<
        ChannelSecondaryTransportEnd<TestId>,
        NoPeers,
        ResourceStealingScheduler,
        FixedEstimator,
        TestId,
    > {
        let (transport, _secondary_ends) = setup_test(0);
        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: 0,
            connect_timeout: Duration::from_secs(1),
            peer_timeout: Duration::from_secs(1),
            keepalive_interval: Duration::from_millis(100),
            keepalive_miss_threshold: 3,
            source_pre_staged_root: None,
            uses_file_based_items: false,
            required_setup_on_promote: false,
            max_concurrent_per_type: HashMap::new(),
            retry_max_passes: 0,
            fleet_dead_timeout: Duration::from_secs(1),
            mesh_ready_timeout: Duration::from_secs(1),
            mass_death_grace: Duration::from_secs(1),
            mass_death_min_count: 2,
            source_dir: None,
            unfulfillable_reinject_max_per_task: None,
        };
        PrimaryCoordinator::new(
            config,
            transport,
            NoPeers,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        )
    }

    /// End-to-end: send `FailPermanent`, observe the local
    /// `failed_tasks` ledger updates and the reply oneshot fires.
    #[tokio::test(flavor = "current_thread")]
    async fn fail_permanent_via_channel() {
        let local = tokio::task::LocalSet::new();
        local.run_until(async {
            let mut coordinator = make_coordinator();
            // Seed a single Pending task into cluster_state so the
            // hash-to-meta lookup succeeds. Also pre-initialise the
            // pool so `pool.on_item_failed_permanent` has a phase
            // to discount in_flight against.
            let binary = make_binary("a", 100);
            let hash = compute_task_hash(&binary);
            coordinator.cluster_state.apply(
                ClusterMutation::TaskAdded {
                    hash: hash.clone(),
                    task: binary.clone(),
                },
            );
            let mut phase_set = std::collections::HashSet::new();
            phase_set.insert(binary.phase_id.clone());
            coordinator.pending = Some(
                dynrunner_scheduler_api::PendingPool::new(phase_set, HashMap::new())
                    .expect("pool init"),
            );
            for p in coordinator.pool().active_phases() {
                coordinator.phase_completed.insert(p.clone(), 0);
                coordinator.phase_failed.insert(p.clone(), 0);
            }

            let (reply_tx, reply_rx) = oneshot::channel();
            super::handle_primary_command(
                &mut coordinator,
                PrimaryCommand::FailPermanent {
                    hash: hash.clone(),
                    error: ErrorType::NonRecoverable,
                    reason: "test".into(),
                    reply: reply_tx,
                },
            )
            .await;
            let reply = reply_rx.await.expect("reply oneshot closed");
            assert!(reply.is_ok(), "fail_permanent should accept: {reply:?}");
            assert!(
                coordinator.failed_tasks.contains_key(&hash),
                "failed_tasks should include the hash"
            );
            // CRDT mirror reflects the Failed terminal state.
            match coordinator.cluster_state.task_state(&hash) {
                Some(TaskState::Failed { kind, .. }) => {
                    assert_eq!(*kind, ErrorType::NonRecoverable);
                }
                other => panic!("expected Failed, got {other:?}"),
            }
        }).await;
    }

    /// `FailPermanent` on an unknown hash returns Err and leaves
    /// coordinator state untouched.
    #[tokio::test(flavor = "current_thread")]
    async fn fail_permanent_unknown_hash() {
        let local = tokio::task::LocalSet::new();
        local.run_until(async {
            let mut coordinator = make_coordinator();
            let (reply_tx, reply_rx) = oneshot::channel();
            super::handle_primary_command(
                &mut coordinator,
                PrimaryCommand::FailPermanent {
                    hash: "nonexistent".into(),
                    error: ErrorType::NonRecoverable,
                    reason: "test".into(),
                    reply: reply_tx,
                },
            )
            .await;
            let reply = reply_rx.await.expect("reply oneshot closed");
            assert!(reply.is_err(), "unknown hash should error");
            assert!(coordinator.failed_tasks.is_empty());
        }).await;
    }

    /// `ReinjectTask` accepts on `TaskState::Unfulfillable { .. }` and
    /// budget exhaustion locks further reinjects out without
    /// regressing the ledger.
    #[tokio::test(flavor = "current_thread")]
    async fn reinject_task_budget_exhaustion() {
        let local = tokio::task::LocalSet::new();
        local.run_until(async {
            let mut coordinator = make_coordinator();
            coordinator
                .set_unfulfillable_reinject_max_per_task(Some(1));
            let binary = make_binary("a", 100);
            let hash = compute_task_hash(&binary);
            coordinator.cluster_state.apply(ClusterMutation::TaskAdded {
                hash: hash.clone(),
                task: binary.clone(),
            });
            coordinator.cluster_state.apply(ClusterMutation::TaskFailed {
                hash: hash.clone(),
                kind: ErrorType::Unfulfillable {
                    reason: "missing toolchain".to_string().into(),
                },
                error: "unfulfillable".into(),
            });
            // Pool init: reinject requires the phase to exist.
            let mut phase_set = std::collections::HashSet::new();
            phase_set.insert(binary.phase_id.clone());
            coordinator.pending = Some(
                dynrunner_scheduler_api::PendingPool::new(phase_set, HashMap::new())
                    .expect("pool init"),
            );

            // First reinject — accepts.
            let (reply_tx, reply_rx) = oneshot::channel();
            super::handle_primary_command(
                &mut coordinator,
                PrimaryCommand::ReinjectTask {
                    hash: hash.clone(),
                    reply: reply_tx,
                },
            )
            .await;
            assert!(reply_rx.await.unwrap().is_ok(), "first reinject accepts");
            // CRDT mirror moved off Unfulfillable.
            assert!(matches!(
                coordinator.cluster_state.task_state(&hash),
                Some(TaskState::Pending { .. })
            ));

            // Re-set to Unfulfillable and try again — budget should be exhausted.
            coordinator.cluster_state.apply(ClusterMutation::TaskFailed {
                hash: hash.clone(),
                kind: ErrorType::Unfulfillable {
                    reason: "still missing".to_string().into(),
                },
                error: "unfulfillable again".into(),
            });
            let (reply_tx2, reply_rx2) = oneshot::channel();
            super::handle_primary_command(
                &mut coordinator,
                PrimaryCommand::ReinjectTask {
                    hash: hash.clone(),
                    reply: reply_tx2,
                },
            )
            .await;
            let r2 = reply_rx2.await.unwrap();
            assert!(r2.is_err(), "second reinject should hit budget cap");
            // Ledger stays Unfulfillable.
            assert!(matches!(
                coordinator.cluster_state.task_state(&hash),
                Some(TaskState::Unfulfillable { .. })
            ));
        }).await;
    }

    /// `UpdatePreferredSecondaries` happy-path smoke: CRDT applies
    /// and the live pool mirror moves in lockstep. The deeper
    /// pool-mirror assertion lives in
    /// `update_preferred_secondaries_propagates_to_live_pool`; this
    /// test just pins that the handler accepts under a fully-seeded
    /// operational fixture (pool initialised, task present in both
    /// CRDT and pool) the way the production operational loop calls
    /// in.
    #[tokio::test(flavor = "current_thread")]
    async fn update_preferred_secondaries_smoke() {
        let local = tokio::task::LocalSet::new();
        local.run_until(async {
            let mut coordinator = make_coordinator();
            let binary = make_binary("a", 100);
            let hash = compute_task_hash(&binary);
            coordinator.cluster_state.apply(ClusterMutation::TaskAdded {
                hash: hash.clone(),
                task: binary.clone(),
            });
            let mut phase_set = std::collections::HashSet::new();
            phase_set.insert(binary.phase_id.clone());
            coordinator.pending = Some(
                dynrunner_scheduler_api::PendingPool::new(phase_set, HashMap::new())
                    .expect("pool init"),
            );
            let (reply_tx, reply_rx) = oneshot::channel();
            super::handle_primary_command(
                &mut coordinator,
                PrimaryCommand::UpdatePreferredSecondaries {
                    hash: hash.clone(),
                    secondaries: vec!["sec-1".into(), "sec-2".into()],
                    reply: reply_tx,
                },
            )
            .await;
            assert!(reply_rx.await.unwrap().is_ok());
        }).await;
    }

    /// End-to-end through the cross-thread channel: send commands via
    /// the public `command_sender()` and consume them through the
    /// `PrimaryCommand` arm. Exercises the same code path the PyO3
    /// `PrimaryHandle` uses.
    #[tokio::test(flavor = "current_thread")]
    async fn command_channel_end_to_end() {
        let local = tokio::task::LocalSet::new();
        local.run_until(async {
            let mut coordinator = make_coordinator();
            let binary = make_binary("a", 100);
            let hash = compute_task_hash(&binary);
            coordinator.cluster_state.apply(
                ClusterMutation::TaskAdded {
                    hash: hash.clone(),
                    task: binary.clone(),
                },
            );
            let mut phase_set = std::collections::HashSet::new();
            phase_set.insert(binary.phase_id.clone());
            coordinator.pending = Some(
                dynrunner_scheduler_api::PendingPool::new(phase_set, HashMap::new())
                    .expect("pool init"),
            );
            for p in coordinator.pool().active_phases() {
                coordinator.phase_completed.insert(p.clone(), 0);
                coordinator.phase_failed.insert(p.clone(), 0);
            }
            let sender = coordinator.command_sender();
            let (reply_tx, reply_rx) = oneshot::channel();
            // Send through the channel (the same `tokio::sync::mpsc`
            // the PyO3 handle uses), then drain the receiver once.
            sender
                .send(PrimaryCommand::FailPermanent {
                    hash: hash.clone(),
                    error: ErrorType::NonRecoverable,
                    reason: "via channel".into(),
                    reply: reply_tx,
                })
                .await
                .expect("send into command channel");
            // Mimic the operational loop: take the receiver, recv one
            // command, dispatch it.
            let mut rx = coordinator.command_rx.take().expect("rx present");
            let command = rx.recv().await.expect("first command");
            super::handle_primary_command(&mut coordinator, command).await;
            coordinator.command_rx = Some(rx);
            let reply = reply_rx.await.expect("reply oneshot closed");
            assert!(reply.is_ok(), "{reply:?}");
            assert!(coordinator.failed_tasks.contains_key(&hash));
        }).await;
    }

    /// `FailPermanent` with `ErrorType::Unfulfillable` routes the
    /// cascade to a `TaskBlocked` broadcast for each dependent — the
    /// dependent's CRDT entry lands in `TaskState::Blocked { on, .. }`
    /// rather than `TaskState::Failed`, so the auto-resume path can
    /// recover when the prereq is reinjected. Pins the cascade
    /// dispatch in `apply_fail_permanent`.
    #[tokio::test(flavor = "current_thread")]
    async fn fail_permanent_unfulfillable_blocks_dependents() {
        let local = tokio::task::LocalSet::new();
        local.run_until(async {
            let mut coordinator = make_coordinator();

            // Prereq carries an explicit task_id so the pool can wire
            // the dep-cascade reverse-index.
            let mut prereq = make_binary("prereq", 100);
            prereq.task_id = Some("prereq_id".into());
            let prereq_hash = compute_task_hash(&prereq);

            // Dependent declares task_depends_on for the cascade walk.
            let mut dep = make_binary("dep", 100);
            dep.task_id = Some("dep_id".into());
            dep.task_depends_on = vec!["prereq_id".into()];
            let dep_hash = compute_task_hash(&dep);

            // Seed CRDT for both.
            coordinator.cluster_state.apply(ClusterMutation::TaskAdded {
                hash: prereq_hash.clone(),
                task: prereq.clone(),
            });
            coordinator.cluster_state.apply(ClusterMutation::TaskAdded {
                hash: dep_hash.clone(),
                task: dep.clone(),
            });

            // Pool seeded with both phases + the items so the cascade
            // primitive has dependents to walk.
            let mut phase_set = std::collections::HashSet::new();
            phase_set.insert(prereq.phase_id.clone());
            let mut pool = dynrunner_scheduler_api::PendingPool::new(
                phase_set,
                HashMap::new(),
            )
            .expect("pool init");
            pool.extend(vec![prereq.clone(), dep.clone()])
                .expect("pool extend");
            coordinator.pending = Some(pool);
            for p in coordinator.pool().active_phases() {
                coordinator.phase_completed.insert(p.clone(), 0);
                coordinator.phase_failed.insert(p.clone(), 0);
            }
            // Mark the prereq in flight so on_item_failed_permanent's
            // in_flight bookkeeping doesn't saturate.
            coordinator.pool_mut().mark_in_flight(&prereq.phase_id);

            let (reply_tx, reply_rx) = oneshot::channel();
            super::handle_primary_command(
                &mut coordinator,
                PrimaryCommand::FailPermanent {
                    hash: prereq_hash.clone(),
                    error: ErrorType::Unfulfillable {
                        reason: "missing toolchain".to_string().into(),
                    },
                    reason: "no peer holds the resource".into(),
                    reply: reply_tx,
                },
            )
            .await;
            let reply = reply_rx.await.expect("reply oneshot closed");
            assert!(reply.is_ok(), "fail_permanent should accept: {reply:?}");

            // Prereq lands in the discrete Unfulfillable variant.
            assert!(matches!(
                coordinator.cluster_state.task_state(&prereq_hash),
                Some(TaskState::Unfulfillable { .. })
            ));
            // Dependent lands in Blocked-on-prereq via the cascade
            // broadcast (NOT in Failed).
            match coordinator.cluster_state.task_state(&dep_hash) {
                Some(TaskState::Blocked { on, .. }) => {
                    assert_eq!(on, &prereq_hash);
                }
                other => panic!("expected Blocked, got {other:?}"),
            }
            // Dependent is NOT in the local failed_tasks ledger — it's
            // cascade-paused, not failed.
            assert!(
                !coordinator.failed_tasks.contains_key(&dep_hash),
                "blocked dependent must not be in failed_tasks"
            );
        }).await;
    }

    /// Full Unfulfillable-cascade chain: root Unfulfillable →
    /// dependents Blocked → reinject root → root completes →
    /// auto-resume on the CRDT side mirrors into the live pool via
    /// `apply_and_broadcast_cluster_mutations`'s
    /// `resumed_for_dispatch` plumb. After completion the dependent
    /// must be back in the live pool and dispatchable through the
    /// normal pool primitives.
    ///
    /// Pins Phase-6 Finding 6: pool-side cascade-resume of Blocked
    /// dependents when the prereq's `TaskCompleted` lands. The CRDT
    /// side was already correct; this test enforces the pool stays
    /// coherent.
    #[tokio::test(flavor = "current_thread")]
    async fn unfulfillable_reinject_root_complete_resumes_blocked_dependents_in_pool() {
        let local = tokio::task::LocalSet::new();
        local.run_until(async {
            let mut coordinator = make_coordinator();

            let mut prereq = make_binary("prereq", 100);
            prereq.task_id = Some("prereq_id".into());
            let prereq_hash = compute_task_hash(&prereq);

            let mut dep = make_binary("dep", 100);
            dep.task_id = Some("dep_id".into());
            dep.task_depends_on = vec!["prereq_id".into()];
            let dep_hash = compute_task_hash(&dep);

            // Seed CRDT and pool with both items.
            coordinator.cluster_state.apply(ClusterMutation::TaskAdded {
                hash: prereq_hash.clone(),
                task: prereq.clone(),
            });
            coordinator.cluster_state.apply(ClusterMutation::TaskAdded {
                hash: dep_hash.clone(),
                task: dep.clone(),
            });
            let mut phase_set = std::collections::HashSet::new();
            phase_set.insert(prereq.phase_id.clone());
            let mut pool = dynrunner_scheduler_api::PendingPool::new(
                phase_set,
                HashMap::new(),
            )
            .expect("pool init");
            pool.extend(vec![prereq.clone(), dep.clone()])
                .expect("pool extend");
            coordinator.pending = Some(pool);
            for p in coordinator.pool().active_phases() {
                coordinator.phase_completed.insert(p.clone(), 0);
                coordinator.phase_failed.insert(p.clone(), 0);
            }
            coordinator.pool_mut().mark_in_flight(&prereq.phase_id);

            // Step 1: prereq fails Unfulfillable → dep moves to
            // Blocked in CRDT, pool drops the dep entry.
            let (rx1, rxr1) = oneshot::channel();
            super::handle_primary_command(
                &mut coordinator,
                PrimaryCommand::FailPermanent {
                    hash: prereq_hash.clone(),
                    error: ErrorType::Unfulfillable {
                        reason: "missing toolchain".to_string().into(),
                    },
                    reason: "no peer".into(),
                    reply: rx1,
                },
            )
            .await;
            assert!(rxr1.await.unwrap().is_ok());
            // Sanity: pool no longer contains the dep binary.
            assert!(
                !coordinator
                    .pool()
                    .iter()
                    .any(|t| t.task_id.as_deref() == Some("dep_id")),
                "dep should be dropped from pool after Unfulfillable cascade"
            );

            // Step 2: reinject the root.
            let (rx2, rxr2) = oneshot::channel();
            super::handle_primary_command(
                &mut coordinator,
                PrimaryCommand::ReinjectTask {
                    hash: prereq_hash.clone(),
                    reply: rx2,
                },
            )
            .await;
            assert!(rxr2.await.unwrap().is_ok());
            // Dep still Blocked in CRDT (only root flipped to
            // Pending; auto-resume fires on TaskCompleted, not on
            // TaskReinjected).
            assert!(matches!(
                coordinator.cluster_state.task_state(&dep_hash),
                Some(TaskState::Blocked { .. })
            ));
            // Dep still absent from the pool.
            assert!(
                !coordinator
                    .pool()
                    .iter()
                    .any(|t| t.task_id.as_deref() == Some("dep_id")),
                "dep must still be absent from pool until root completes"
            );

            // Step 3: simulate the root completing. The mark_in_flight
            // above already incremented in_flight by 1; we route
            // `TaskCompleted` through `apply_and_broadcast_cluster_mutations`
            // so the resumed-dispatch plumbing fires.
            coordinator
                .apply_and_broadcast_cluster_mutations(vec![
                    ClusterMutation::TaskCompleted {
                        hash: prereq_hash.clone(),
                    },
                ])
                .await;

            // CRDT side: dep auto-resumed to Pending.
            assert!(matches!(
                coordinator.cluster_state.task_state(&dep_hash),
                Some(TaskState::Pending { .. })
            ));
            // Pool side: dep is back in a bucket and dispatchable.
            assert!(
                coordinator
                    .pool()
                    .iter()
                    .any(|t| t.task_id.as_deref() == Some("dep_id")),
                "dep must be back in the pool after auto-resume"
            );
            // The dep's phase must be dispatchable (not Blocked).
            // `reinject` flips Draining/Drained/Done back to Active;
            // for an originally-Active phase it stays Active.
            let phase_state = coordinator.pool().phase_state(&dep.phase_id);
            assert!(
                matches!(
                    phase_state,
                    Some(dynrunner_scheduler_api::PhaseState::Active)
                ),
                "dep's phase must be Active for dispatch; got {phase_state:?}"
            );
        }).await;
    }

    /// Re-inject path is independent of dependent unblock: after the
    /// root flips Unfulfillable→Pending via `ReinjectTask`, the
    /// dependents must STAY Blocked in CRDT (auto-resume only fires
    /// on `TaskCompleted`) and remain absent from the pool. Guards
    /// against an accidental "reinject also unblocks dependents"
    /// regression that would let dependents dispatch ahead of a
    /// still-pending root.
    #[tokio::test(flavor = "current_thread")]
    async fn reinject_resets_blocked_dependents_pool_state() {
        let local = tokio::task::LocalSet::new();
        local.run_until(async {
            let mut coordinator = make_coordinator();

            let mut prereq = make_binary("prereq", 100);
            prereq.task_id = Some("prereq_id".into());
            let prereq_hash = compute_task_hash(&prereq);

            let mut dep = make_binary("dep", 100);
            dep.task_id = Some("dep_id".into());
            dep.task_depends_on = vec!["prereq_id".into()];
            let dep_hash = compute_task_hash(&dep);

            coordinator.cluster_state.apply(ClusterMutation::TaskAdded {
                hash: prereq_hash.clone(),
                task: prereq.clone(),
            });
            coordinator.cluster_state.apply(ClusterMutation::TaskAdded {
                hash: dep_hash.clone(),
                task: dep.clone(),
            });
            let mut phase_set = std::collections::HashSet::new();
            phase_set.insert(prereq.phase_id.clone());
            let mut pool = dynrunner_scheduler_api::PendingPool::new(
                phase_set,
                HashMap::new(),
            )
            .expect("pool init");
            pool.extend(vec![prereq.clone(), dep.clone()])
                .expect("pool extend");
            coordinator.pending = Some(pool);
            for p in coordinator.pool().active_phases() {
                coordinator.phase_completed.insert(p.clone(), 0);
                coordinator.phase_failed.insert(p.clone(), 0);
            }
            coordinator.pool_mut().mark_in_flight(&prereq.phase_id);

            // Cascade: prereq Unfulfillable → dep Blocked.
            let (tx1, rx1) = oneshot::channel();
            super::handle_primary_command(
                &mut coordinator,
                PrimaryCommand::FailPermanent {
                    hash: prereq_hash.clone(),
                    error: ErrorType::Unfulfillable {
                        reason: "missing toolchain".to_string().into(),
                    },
                    reason: "no peer".into(),
                    reply: tx1,
                },
            )
            .await;
            assert!(rx1.await.unwrap().is_ok());

            // Reinject root only.
            let (tx2, rx2) = oneshot::channel();
            super::handle_primary_command(
                &mut coordinator,
                PrimaryCommand::ReinjectTask {
                    hash: prereq_hash.clone(),
                    reply: tx2,
                },
            )
            .await;
            assert!(rx2.await.unwrap().is_ok());

            // CRDT: root flipped to Pending; dep stays Blocked.
            assert!(matches!(
                coordinator.cluster_state.task_state(&prereq_hash),
                Some(TaskState::Pending { .. })
            ));
            match coordinator.cluster_state.task_state(&dep_hash) {
                Some(TaskState::Blocked { on, .. }) => {
                    assert_eq!(on, &prereq_hash);
                }
                other => panic!("dep should still be Blocked, got {other:?}"),
            }
            // Pool: dep must NOT be present (the cascade dropped it
            // and reinject of root does not re-introduce it).
            assert!(
                !coordinator
                    .pool()
                    .iter()
                    .any(|t| t.task_id.as_deref() == Some("dep_id")),
                "dep must stay out of pool until root completes"
            );
            // Root IS back in the pool.
            assert!(
                coordinator
                    .pool()
                    .iter()
                    .any(|t| t.task_id.as_deref() == Some("prereq_id")),
                "root must be back in pool after reinject"
            );
        }).await;
    }

    /// `UpdatePreferredSecondaries` runtime mutation must propagate to
    /// the live pool's `TaskInfo` clone, not only to the CRDT mirror.
    /// The scheduler's hot path reads `preferred_secondaries` from the
    /// pool entry it dispatches; a CRDT-only update would only become
    /// visible on snapshot-restore.
    ///
    /// Pins Phase-6 Finding 7: pool-side mirror of preferred-secondaries.
    #[tokio::test(flavor = "current_thread")]
    async fn update_preferred_secondaries_propagates_to_live_pool() {
        let local = tokio::task::LocalSet::new();
        local.run_until(async {
            let mut coordinator = make_coordinator();
            let mut binary = make_binary("a", 100);
            binary.task_id = Some("a_id".into());
            let hash = compute_task_hash(&binary);
            coordinator.cluster_state.apply(ClusterMutation::TaskAdded {
                hash: hash.clone(),
                task: binary.clone(),
            });
            let mut phase_set = std::collections::HashSet::new();
            phase_set.insert(binary.phase_id.clone());
            let mut pool = dynrunner_scheduler_api::PendingPool::new(
                phase_set,
                HashMap::new(),
            )
            .expect("pool init");
            pool.extend(vec![binary.clone()]).expect("pool extend");
            coordinator.pending = Some(pool);
            for p in coordinator.pool().active_phases() {
                coordinator.phase_completed.insert(p.clone(), 0);
                coordinator.phase_failed.insert(p.clone(), 0);
            }

            // Pre-condition: pool's clone has empty preferred_secondaries.
            let pre = coordinator
                .pool()
                .iter()
                .find(|t| t.task_id.as_deref() == Some("a_id"))
                .expect("task in pool")
                .preferred_secondaries
                .clone();
            assert!(pre.as_slice().is_empty(), "fixture starts empty");

            let (tx, rx) = oneshot::channel();
            super::handle_primary_command(
                &mut coordinator,
                PrimaryCommand::UpdatePreferredSecondaries {
                    hash: hash.clone(),
                    secondaries: vec!["sec-a".into(), "sec-b".into()],
                    reply: tx,
                },
            )
            .await;
            assert!(rx.await.unwrap().is_ok());

            // CRDT mirror updated.
            let crdt_task = match coordinator.cluster_state.task_state(&hash) {
                Some(TaskState::Pending { task }) => task.clone(),
                other => panic!("expected Pending, got {other:?}"),
            };
            let expected: Vec<&str> = vec!["sec-a", "sec-b"];
            assert_eq!(
                crdt_task
                    .preferred_secondaries
                    .as_slice()
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>(),
                expected,
                "CRDT mirror"
            );

            // Live pool's clone reflects the update on the next read.
            let post = coordinator
                .pool()
                .iter()
                .find(|t| t.task_id.as_deref() == Some("a_id"))
                .expect("task still in pool")
                .preferred_secondaries
                .clone();
            assert_eq!(
                post.as_slice().iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                expected,
                "live pool's TaskInfo.preferred_secondaries must mirror the CRDT update"
            );
        }).await;
    }

    // ── PrimaryCommand::SpawnTasks ──

    /// Seed `coordinator.pending` with a `PendingPool` whose active
    /// phases include `phase_id` (plus any extras supplied) and
    /// initialise the per-phase completed/failed counters. Centralised
    /// so every spawn-tasks test starts from the same shape the
    /// production operational loop uses.
    fn seed_pool(
        coordinator: &mut PrimaryCoordinator<
            ChannelSecondaryTransportEnd<TestId>,
            NoPeers,
            ResourceStealingScheduler,
            FixedEstimator,
            TestId,
        >,
        phases: &[&dynrunner_core::PhaseId],
    ) {
        let mut phase_set = std::collections::HashSet::new();
        for p in phases {
            phase_set.insert((*p).clone());
        }
        coordinator.pending = Some(
            dynrunner_scheduler_api::PendingPool::new(phase_set, HashMap::new())
                .expect("pool init"),
        );
        for p in coordinator.pool().active_phases() {
            coordinator.phase_completed.insert(p.clone(), 0);
            coordinator.phase_failed.insert(p.clone(), 0);
        }
    }

    /// Drive a `SpawnTasks` command through the dispatch path and
    /// return the per-index error list. Centralised so every test
    /// uses the same call sequence.
    async fn spawn_via_handler(
        coordinator: &mut PrimaryCoordinator<
            ChannelSecondaryTransportEnd<TestId>,
            NoPeers,
            ResourceStealingScheduler,
            FixedEstimator,
            TestId,
        >,
        tasks: Vec<dynrunner_core::TaskInfo<TestId>>,
    ) -> Result<Vec<(usize, super::SpawnError)>, String> {
        let (reply_tx, reply_rx) = oneshot::channel();
        super::handle_primary_command(
            coordinator,
            PrimaryCommand::SpawnTasks {
                tasks,
                reply: reply_tx,
            },
        )
        .await;
        reply_rx.await.expect("reply oneshot closed")
    }

    /// 3 fresh tasks with no deps: all 3 land in Pending in the CRDT
    /// and are reinjected into the live pool.
    #[tokio::test(flavor = "current_thread")]
    async fn spawn_tasks_all_pending_dispatched() {
        let local = tokio::task::LocalSet::new();
        local.run_until(async {
            let mut coordinator = make_coordinator();
            let mut a = make_binary("a", 100);
            a.task_id = Some("a_id".into());
            let mut b = make_binary("b", 100);
            b.task_id = Some("b_id".into());
            let mut c = make_binary("c", 100);
            c.task_id = Some("c_id".into());
            seed_pool(&mut coordinator, &[&a.phase_id]);

            let errors = spawn_via_handler(
                &mut coordinator,
                vec![a.clone(), b.clone(), c.clone()],
            )
            .await
            .expect("spawn_tasks succeeds");
            assert!(errors.is_empty(), "no per-index errors expected: {errors:?}");

            for binary in &[&a, &b, &c] {
                let hash = compute_task_hash(binary);
                assert!(
                    matches!(
                        coordinator.cluster_state.task_state(&hash),
                        Some(TaskState::Pending { .. })
                    ),
                    "task {:?} must land in Pending",
                    binary.task_id
                );
                assert!(
                    coordinator
                        .pool()
                        .iter()
                        .any(|t| t.task_id == binary.task_id),
                    "task {:?} must be re-injected into the live pool",
                    binary.task_id
                );
            }
        })
        .await;
    }

    /// Task A depends on Pending task B: A lands in `Blocked{on=B}`,
    /// NOT in the pool. B was seeded as Pending in the ledger.
    #[tokio::test(flavor = "current_thread")]
    async fn spawn_tasks_with_pending_dep_lands_blocked() {
        let local = tokio::task::LocalSet::new();
        local.run_until(async {
            let mut coordinator = make_coordinator();
            let mut b = make_binary("b", 100);
            b.task_id = Some("b_id".into());
            let b_hash = compute_task_hash(&b);
            coordinator.cluster_state.apply(ClusterMutation::TaskAdded {
                hash: b_hash.clone(),
                task: b.clone(),
            });
            seed_pool(&mut coordinator, &[&b.phase_id]);

            let mut a = make_binary("a", 100);
            a.task_id = Some("a_id".into());
            a.task_depends_on = vec!["b_id".into()];

            let errors = spawn_via_handler(&mut coordinator, vec![a.clone()])
                .await
                .expect("spawn_tasks succeeds");
            assert!(errors.is_empty(), "no per-index errors expected: {errors:?}");

            let a_hash = compute_task_hash(&a);
            match coordinator.cluster_state.task_state(&a_hash) {
                Some(TaskState::Blocked { on, .. }) => {
                    assert_eq!(on, &b_hash, "Blocked.on must point to dep's hash");
                }
                other => panic!("expected Blocked, got {other:?}"),
            }
            assert!(
                !coordinator.pool().iter().any(|t| t.task_id == a.task_id),
                "Blocked task must not be in the pool"
            );
        })
        .await;
    }

    /// Task A depends on Completed task B: A lands in Pending (deps
    /// resolved) and is reinjected into the pool.
    #[tokio::test(flavor = "current_thread")]
    async fn spawn_tasks_with_completed_dep_lands_pending() {
        let local = tokio::task::LocalSet::new();
        local.run_until(async {
            let mut coordinator = make_coordinator();
            let mut b = make_binary("b", 100);
            b.task_id = Some("b_id".into());
            let b_hash = compute_task_hash(&b);
            coordinator.cluster_state.apply(ClusterMutation::TaskAdded {
                hash: b_hash.clone(),
                task: b.clone(),
            });
            coordinator.cluster_state.apply(ClusterMutation::TaskCompleted {
                hash: b_hash.clone(),
            });
            seed_pool(&mut coordinator, &[&b.phase_id]);

            let mut a = make_binary("a", 100);
            a.task_id = Some("a_id".into());
            a.task_depends_on = vec!["b_id".into()];

            let errors = spawn_via_handler(&mut coordinator, vec![a.clone()])
                .await
                .expect("spawn_tasks succeeds");
            assert!(errors.is_empty(), "no per-index errors expected: {errors:?}");

            let a_hash = compute_task_hash(&a);
            assert!(
                matches!(
                    coordinator.cluster_state.task_state(&a_hash),
                    Some(TaskState::Pending { .. })
                ),
                "task with all-Completed deps must land in Pending"
            );
            assert!(
                coordinator.pool().iter().any(|t| t.task_id == a.task_id),
                "Pending task must be in the pool"
            );
        })
        .await;
    }

    /// Task A depends on Unfulfillable task B: A lands in
    /// `Blocked{on=B}`. The CRDT's auto-resume on a later
    /// `TaskCompleted` will move A back to Pending — same shape as
    /// the legacy cascade-pause path.
    #[tokio::test(flavor = "current_thread")]
    async fn spawn_tasks_with_unfulfillable_dep_lands_blocked() {
        let local = tokio::task::LocalSet::new();
        local.run_until(async {
            let mut coordinator = make_coordinator();
            let mut b = make_binary("b", 100);
            b.task_id = Some("b_id".into());
            let b_hash = compute_task_hash(&b);
            coordinator.cluster_state.apply(ClusterMutation::TaskAdded {
                hash: b_hash.clone(),
                task: b.clone(),
            });
            coordinator.cluster_state.apply(ClusterMutation::TaskFailed {
                hash: b_hash.clone(),
                kind: ErrorType::Unfulfillable {
                    reason: "missing toolchain".to_string().into(),
                },
                error: "unfulfillable".into(),
            });
            seed_pool(&mut coordinator, &[&b.phase_id]);

            let mut a = make_binary("a", 100);
            a.task_id = Some("a_id".into());
            a.task_depends_on = vec!["b_id".into()];

            let errors = spawn_via_handler(&mut coordinator, vec![a.clone()])
                .await
                .expect("spawn_tasks succeeds");
            assert!(errors.is_empty(), "no per-index errors expected: {errors:?}");

            let a_hash = compute_task_hash(&a);
            match coordinator.cluster_state.task_state(&a_hash) {
                Some(TaskState::Blocked { on, .. }) => {
                    assert_eq!(on, &b_hash);
                }
                other => panic!("expected Blocked-on-unfulfillable, got {other:?}"),
            }
            assert!(
                !coordinator.pool().iter().any(|t| t.task_id == a.task_id),
                "Blocked-on-Unfulfillable task must not be in the pool"
            );
        })
        .await;
    }

    /// Vec with 3 tasks, 1 with a hash that already exists in the
    /// ledger: the duplicate surfaces as a per-index error
    /// `SpawnError::DuplicateTaskHash`; the other 2 land normally.
    #[tokio::test(flavor = "current_thread")]
    async fn spawn_tasks_duplicate_hash_returns_per_index_error() {
        let local = tokio::task::LocalSet::new();
        local.run_until(async {
            let mut coordinator = make_coordinator();
            // Pre-seed `dup` so the second input clashes.
            let mut dup = make_binary("dup", 100);
            dup.task_id = Some("dup_id".into());
            let dup_hash = compute_task_hash(&dup);
            coordinator.cluster_state.apply(ClusterMutation::TaskAdded {
                hash: dup_hash.clone(),
                task: dup.clone(),
            });
            seed_pool(&mut coordinator, &[&dup.phase_id]);

            let mut a = make_binary("a", 100);
            a.task_id = Some("a_id".into());
            let mut c = make_binary("c", 100);
            c.task_id = Some("c_id".into());

            let errors = spawn_via_handler(
                &mut coordinator,
                vec![a.clone(), dup.clone(), c.clone()],
            )
            .await
            .expect("spawn_tasks succeeds (per-task failures are NOT vec-level)");
            assert_eq!(errors.len(), 1, "exactly one per-index error: {errors:?}");
            let (idx, err) = &errors[0];
            assert_eq!(*idx, 1, "duplicate at vec position 1");
            match err {
                super::SpawnError::DuplicateTaskHash(h) => assert_eq!(h, &dup_hash),
                other => panic!("expected DuplicateTaskHash, got {other:?}"),
            }
            // The other two land normally.
            assert!(
                matches!(
                    coordinator.cluster_state.task_state(&compute_task_hash(&a)),
                    Some(TaskState::Pending { .. })
                ),
                "task a must land in Pending"
            );
            assert!(
                matches!(
                    coordinator.cluster_state.task_state(&compute_task_hash(&c)),
                    Some(TaskState::Pending { .. })
                ),
                "task c must land in Pending"
            );
        })
        .await;
    }

    /// Vec with 3 tasks, 1 carrying `task_depends_on=["nope"]`: the
    /// bad-dep entry surfaces as `SpawnError::UnknownDependency`;
    /// the other 2 land normally.
    #[tokio::test(flavor = "current_thread")]
    async fn spawn_tasks_unknown_dependency_returns_per_index_error() {
        let local = tokio::task::LocalSet::new();
        local.run_until(async {
            let mut coordinator = make_coordinator();
            let mut a = make_binary("a", 100);
            a.task_id = Some("a_id".into());
            seed_pool(&mut coordinator, &[&a.phase_id]);
            let mut bad = make_binary("bad", 100);
            bad.task_id = Some("bad_id".into());
            bad.task_depends_on = vec!["nope".into()];
            let mut c = make_binary("c", 100);
            c.task_id = Some("c_id".into());

            let errors = spawn_via_handler(
                &mut coordinator,
                vec![a.clone(), bad.clone(), c.clone()],
            )
            .await
            .expect("spawn_tasks succeeds");
            assert_eq!(errors.len(), 1, "exactly one per-index error: {errors:?}");
            let (idx, err) = &errors[0];
            assert_eq!(*idx, 1);
            match err {
                super::SpawnError::UnknownDependency {
                    task_hash,
                    dep_task_id,
                } => {
                    assert_eq!(task_hash, &compute_task_hash(&bad));
                    assert_eq!(dep_task_id, "nope");
                }
                other => panic!("expected UnknownDependency, got {other:?}"),
            }
            // Other two land normally.
            assert!(matches!(
                coordinator.cluster_state.task_state(&compute_task_hash(&a)),
                Some(TaskState::Pending { .. })
            ));
            assert!(matches!(
                coordinator.cluster_state.task_state(&compute_task_hash(&c)),
                Some(TaskState::Pending { .. })
            ));
            // And the bad entry is NOT in the ledger.
            assert!(
                coordinator.cluster_state.task_state(&compute_task_hash(&bad)).is_none(),
                "bad-dep task must not be inserted in the ledger"
            );
        })
        .await;
    }

    /// `ClusterMutation::TasksSpawned` round-trips through serde.
    /// Pins wire-codec compatibility for the new variant.
    #[test]
    fn tasks_spawned_mutation_round_trips_through_serde() {
        let mut a = make_binary("a", 100);
        a.task_id = Some("a_id".into());
        let mut b = make_binary("b", 100);
        b.task_id = Some("b_id".into());
        b.task_depends_on = vec!["a_id".into()];
        let m: ClusterMutation<TestId> = ClusterMutation::TasksSpawned {
            tasks: vec![a.clone(), b.clone()],
        };
        let json = serde_json::to_string(&m).expect("serialize");
        let round: ClusterMutation<TestId> =
            serde_json::from_str(&json).expect("deserialize");
        match round {
            ClusterMutation::TasksSpawned { tasks } => {
                assert_eq!(tasks.len(), 2);
                assert_eq!(tasks[0].task_id.as_deref(), Some("a_id"));
                assert_eq!(tasks[1].task_id.as_deref(), Some("b_id"));
                assert_eq!(tasks[1].task_depends_on, vec!["a_id".to_string()]);
            }
            other => panic!("variant lost in round-trip: {other:?}"),
        }
    }
}
