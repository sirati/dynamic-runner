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
