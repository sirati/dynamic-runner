//! Cross-thread / cross-async-runtime ingress for "from outside the
//! operational loop, please apply this mutation to the running primary".
//!
//! Single concern: a typed, reply-bearing command channel whose receiver
//! is read inside the primary's operational-loop `select!` (distributed
//! backend) or the local manager's worker-loop `select!` (local backend)
//! and whose sender is cloned out to consumers (PyO3 `PrimaryHandle`,
//! future Rust-side control-plane callers). Each command carries a
//! `oneshot::Sender<Result<...>>` so the caller can block / await the
//! handler's outcome and surface success / failure synchronously.
//!
//! Module boundary:
//!   * Owns: the `PrimaryCommand<I>` enum (the over-the-wire shape of
//!     every PyPrimaryHandle command), the `SpawnError` per-task error
//!     enum, the `COMMAND_CHANNEL_CAPACITY` bound, and the
//!     `validate_spawn_tasks` closure-based pre-apply validator.
//!   * Does NOT own: the per-backend application of each command. Each
//!     backend (distributed primary, distributed promoted-secondary,
//!     local manager) owns a `handle_*_command` dispatcher whose arms
//!     mutate that backend's state. The validator is the single piece
//!     of read-only logic shared across all three so the duplicate-hash
//!     and unknown-dep rules cannot drift.
//!
//! Lives in `dynrunner-core` (not in either manager crate) because:
//!   * Both `dynrunner-manager-distributed` and
//!     `dynrunner-manager-local` need the same `PrimaryCommand` wire
//!     type for their command-channel receivers to share the single
//!     `PyPrimaryHandle` pyclass on the Python side.
//!   * `dynrunner-manager-local` does not depend on
//!     `dynrunner-manager-distributed` (the latter is the
//!     full-network-stack manager; the former is single-host). Adding
//!     a manager → manager dependency edge would introduce an upward
//!     coupling for a piece of shared wire-shape data.
//!   * `dynrunner-core` already owns the wire-canonical
//!     [`crate::compute_task_hash`] and every type the enum is generic
//!     over (`Identifier`, `TaskInfo`, `ErrorType`).
//!
//! Capacity: the inbound channel is bounded (`COMMAND_CHANNEL_CAPACITY`)
//! so a runaway caller can't OOM the receiver. Backpressure surfaces to
//! the sender side as a slow `send().await`; the handler-side reply
//! oneshot is the per-command flow-control signal.

use crate::{ErrorType, Identifier, TaskInfo, compute_task_hash};
use tokio::sync::oneshot;

/// Bounded capacity for the command channel. Sized so a noisy caller
/// can't OOM the receiver while still giving multi-command batches
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
    /// already present in the receiver's ledger. The originator-side
    /// pre-validation catches this; the apply rule itself is also
    /// idempotent (a duplicate is silently NoOp'd) so the wire layer
    /// never regresses prior state.
    DuplicateTaskHash(String),
    /// The task's `task_depends_on` references a task_id that does
    /// not resolve to any entry in the receiver's ledger.
    /// `task_hash` is the wire-canonical hash of the task that
    /// carried the bad reference; `dep_task_id` is the missing
    /// dependency's task_id.
    UnknownDependency {
        task_hash: String,
        dep_task_id: String,
    },
}

/// Pre-apply validation for a `SpawnTasks` batch. Single concern: walk
/// the input vec, partition into `(valid, errors)` by the same rules
/// every `apply_spawn_tasks` handler enforces — duplicate-hash and
/// unknown-dependency — without touching any ledger / pool state.
/// Shared between every backend (primary `apply_spawn_tasks`,
/// promoted-secondary `apply_spawn_tasks`, local manager
/// `handle_local_command::SpawnTasks`) so the rules cannot drift.
///
/// Module boundary:
///   * Owns: the per-task validation rules and the wire-canonical
///     hashing recipe (delegated to [`crate::compute_task_hash`]).
///   * Does NOT own: the broadcast, the post-apply pool routing, the
///     `failed_tasks` ledger bookkeeping. Each backend owns those
///     steps because each holds its own pool / per-pass ledger.
///
/// The receiver-side state is exposed through two closures so this
/// helper has no compile-time dependency on either backend's concrete
/// ledger type:
/// * `is_task_present_by_hash(&str) -> bool` — is a task with the
///   given content hash already in the receiver's ledger? `true`
///   triggers `SpawnError::DuplicateTaskHash`.
/// * `is_known_task_id(&PhaseId, &str) -> bool` — does the receiver's
///   ledger know the FULL `(phase_id, task_id)` identity? Used to
///   validate `task_depends_on` entries that reference identities
///   outside the input batch itself. (Within-batch dependencies
///   validate automatically — the helper unions every
///   `(phase_id, task_id)` from the input batch onto the known-set
///   before walking the dep references.)
///
/// Dep resolution is keyed on the FULL `(phase_id, task_id)` identity
/// (the same rule the scheduler-api `PendingPool::partition_ingest`
/// ingest enforces, mirrored here because this helper lives in
/// `dynrunner-core` and cannot depend on the scheduler-api crate that
/// owns `partition_ingest`). The SAME `task_id` in two DIFFERENT phases
/// is a DISTINCT task: a dep naming a phase where its `task_id` is
/// absent is `UnknownDependency`, even if a same-named `task_id` exists
/// in another phase — without this the task would pass pre-validation
/// only to land silently never-runnable (the receiver's phase-aware
/// `task_hash_for_dep` returns `None` for the mismatched phase and the
/// apply rule treats the dep as resolved).
///
/// `task_depends_on` references resolve against (a) every
/// `(phase_id, task_id)` the `is_known_task_id` closure accepts AND
/// (b) every `(phase_id, task_id)` contributed by the input batch
/// itself, so within-batch dependencies validate.
pub fn validate_spawn_tasks<I, F, G>(
    is_task_present_by_hash: F,
    is_known_task_id: G,
    tasks: Vec<TaskInfo<I>>,
) -> (Vec<TaskInfo<I>>, Vec<(usize, SpawnError)>)
where
    I: Identifier,
    F: Fn(&str) -> bool,
    G: Fn(&crate::PhaseId, &str) -> bool,
{
    let mut errors: Vec<(usize, SpawnError)> = Vec::new();
    let mut valid_tasks: Vec<TaskInfo<I>> = Vec::with_capacity(tasks.len());
    // Build a set of full `(phase_id, task_id)` identities the
    // pre-validation pass treats as known: every identity the receiver
    // knows PLUS every identity contributed by the input batch (so
    // within-batch dependencies validate). The wire-side apply rule
    // does its own phase-aware dep resolution per-task
    // (`task_hash_for_dep`); this pre-pass surfaces failures for the
    // caller before the broadcast happens.
    // Both fields are non-optional per the framework's boundary
    // contract; the batch-side known-set is built directly.
    let batch_identities: std::collections::HashSet<(crate::PhaseId, String)> = tasks
        .iter()
        .map(|t| (t.phase_id.clone(), t.task_id.clone()))
        .collect();
    // Per-task validation pass. A task can fail multiple checks
    // (duplicate hash AND unknown dep); we surface the FIRST failure
    // per index so the caller sees one error per rejected task.
    // Duplicate-hash is checked first because it short-circuits the
    // rest of the task's checks: a hash collision means the task is
    // already in the ledger and re-validating its deps against the
    // existing entry would be redundant.
    for (idx, task) in tasks.into_iter().enumerate() {
        let hash = compute_task_hash(&task);
        if is_task_present_by_hash(&hash) {
            errors.push((idx, SpawnError::DuplicateTaskHash(hash)));
            continue;
        }
        let mut bad_dep: Option<String> = None;
        for dep in &task.task_depends_on {
            let dep_key = (dep.phase_id.clone(), dep.task_id.clone());
            if !batch_identities.contains(&dep_key)
                && !is_known_task_id(&dep.phase_id, &dep.task_id)
            {
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

/// One in-flight command on the `PrimaryHandle` → backend channel.
///
/// Generic over `I: Identifier` because [`PrimaryCommand::SpawnTasks`]
/// carries `Vec<TaskInfo<I>>` for runtime task injection. The other
/// variants still address tasks by their content hash (`String`) and
/// would be `I`-free in isolation; carrying the generic on the enum
/// keeps every variant on the same channel and lets the receiver be
/// `tokio_mpsc::Receiver<PrimaryCommand<I>>` matching the
/// backend's own `I` parameter. The PyO3 layer specialises
/// `I = RunnerIdentifier` (the only concrete identifier type Python
/// can construct).
///
/// Per-variant semantics live in each backend's handler module:
///   * Distributed primary —
///     `dynrunner-manager-distributed::primary::command_channel::handler`.
///   * Distributed promoted-secondary —
///     `dynrunner-manager-distributed::secondary::primary::spawn_tasks`
///     (and sibling files).
///   * Local manager —
///     `dynrunner-manager-local::manager::command_channel`.
pub enum PrimaryCommand<I: Identifier> {
    /// Apply `pending_pool::on_item_failed_permanent` + cascade for the
    /// named hash. On the distributed backend the apply also broadcasts
    /// `ClusterMutation::TaskFailed{...}`; on the local backend the
    /// command is local-only (no peer concept).
    ///
    /// Error path: unknown hash → `Err(...)`. The handler's `reply`
    /// carries the result; the backend's own state is unchanged on
    /// the error arm.
    FailPermanent {
        hash: String,
        error: ErrorType,
        reason: String,
        reply: oneshot::Sender<Result<(), String>>,
    },

    /// External-control reinjection. The distributed backend gates on
    /// the CRDT's `TaskState::Unfulfillable { .. }` (the operator-
    /// resolvable-failure class) and applies a budgeted state flip; the
    /// local backend looks the task up in its side queues
    /// (`failed_tasks`, `resource_pressure_tasks`, `unassigned_tasks`)
    /// and reinjects via `pool.reinject`. Both backends apply the same
    /// per-task budget cap when configured.
    ReinjectTask {
        hash: String,
        reply: oneshot::Sender<Result<(), String>>,
    },

    /// External-control update of the named task's preferred-secondaries
    /// list. Distributed backend broadcasts
    /// `ClusterMutation::TaskPreferredSecondariesUpdated` so every
    /// node's CRDT mirror picks up the new preference list; local
    /// backend mirrors onto the pool's `TaskInfo.preferred_secondaries`
    /// field (no peer concept; the field is still meaningful for
    /// future-cluster-promotion replay) and logs a debug line.
    UpdatePreferredSecondaries {
        hash: String,
        secondaries: Vec<String>,
        reply: oneshot::Sender<Result<(), String>>,
    },

    /// External-control update of a peer's primary-capability — whether
    /// the named peer may ever host the primary role. The distributed
    /// backend broadcasts `ClusterMutation::SetCanBePrimary` so every
    /// node's `RoleTable.can_be_primary` set converges; the local
    /// backend has no peer concept, so it is a no-op that replies `Ok`
    /// (the operator's intent has no error path in local mode).
    SetCanBePrimary {
        peer_id: String,
        can_be_primary: bool,
        reply: oneshot::Sender<Result<(), String>>,
    },

    /// Runtime task injection: a batch of brand-new `TaskInfo<I>`
    /// entries to add to the receiver's task set so the live backend
    /// dispatches them.
    ///
    /// Each backend's handler performs per-task pre-validation
    /// (duplicate-hash, unknown dependency) via
    /// [`validate_spawn_tasks`]; the reply oneshot returns a per-index
    /// `SpawnError` for every input task that failed validation (empty
    /// `Vec` = full success). The outer `Result<_, String>` carries
    /// channel-wide failure modes (unknown internal state, pool extend
    /// rejection). One apply pass per call, regardless of batch size.
    SpawnTasks {
        tasks: Vec<TaskInfo<I>>,
        reply: oneshot::Sender<Result<Vec<(usize, SpawnError)>, String>>,
    },
}
