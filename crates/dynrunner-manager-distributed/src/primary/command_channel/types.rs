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
use tokio::sync::oneshot;

use crate::cluster_state::ClusterState;

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

/// Pre-apply validation for a `SpawnTasks` batch against the current
/// cluster ledger. Single concern: walk the input vec, partition into
/// `(valid, errors)` by the same rules `apply_spawn_tasks` enforces —
/// duplicate-hash and unknown-dependency — without touching either
/// coordinator's pool / failed-task ledger. Shared between
/// `PrimaryCoordinator::apply_spawn_tasks` (live primary path) and
/// `SecondaryCoordinator::apply_spawn_tasks` (promoted-secondary path)
/// so the rules cannot drift.
///
/// Module boundary:
///   * Owns: the per-task validation rules and the wire-canonical
///     hashing recipe (delegated to `primary::wire::compute_task_hash`).
///   * Does NOT own: the broadcast, the post-apply pool routing, the
///     `failed_tasks` ledger bookkeeping. Each coordinator owns those
///     steps because each holds the live pool / per-pass ledger.
///
/// `task_depends_on` references resolve against (a) every task_id in
/// the existing ledger AND (b) every task_id contributed by the input
/// batch itself, so within-batch dependencies validate.
pub fn validate_spawn_tasks<I: Identifier>(
    cluster_state: &ClusterState<I>,
    tasks: Vec<TaskInfo<I>>,
) -> (Vec<TaskInfo<I>>, Vec<(usize, SpawnError)>) {
    let mut errors: Vec<(usize, SpawnError)> = Vec::new();
    let mut valid_tasks: Vec<TaskInfo<I>> = Vec::with_capacity(tasks.len());
    // Build a set of task_ids the pre-validation pass treats as
    // known: every task_id in the existing ledger PLUS every task_id
    // contributed by the input batch (so within-batch dependencies
    // validate). The wire-side apply rule does its own dep resolution
    // per-task; this pre-pass surfaces failures for the caller before
    // the broadcast happens.
    let mut known_task_ids: std::collections::HashSet<String> = cluster_state
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
    // (duplicate hash AND unknown dep); we surface the FIRST failure
    // per index so the caller sees one error per rejected task.
    // Duplicate-hash is checked first because it short-circuits the
    // rest of the task's checks: a hash collision means the task is
    // already in the ledger and re-validating its deps against the
    // existing entry would be redundant.
    for (idx, task) in tasks.into_iter().enumerate() {
        let hash = crate::primary::wire::compute_task_hash(&task);
        if cluster_state.task_state(&hash).is_some() {
            errors.push((idx, SpawnError::DuplicateTaskHash(hash)));
            continue;
        }
        let mut bad_dep: Option<String> = None;
        for dep in &task.task_depends_on {
            if !known_task_ids.contains(&dep.task_id) {
                bad_dep = Some(dep.task_id.clone());
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
    (valid_tasks, errors)
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
