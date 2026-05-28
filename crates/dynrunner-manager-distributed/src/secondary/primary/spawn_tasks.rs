//! Promoted-secondary side of `PrimaryCommand::SpawnTasks`.
//!
//! Single concern: route the runtime-task-injection flow through the
//! promoted secondary's live pool. Mirrors
//! `PrimaryCoordinator::apply_spawn_tasks` 1:1 — the validation step
//! is the shared
//! [`crate::primary::command_channel::validate_spawn_tasks`] helper
//! (so duplicate-hash + unknown-dep rules can't drift between the two
//! apply sites), and the rest of the body is the secondary's analog
//! of the primary's apply+broadcast + post-apply pool-routing dance.
//!
//! Module boundary:
//!   * Owns: the pool-side post-apply routing for the secondary's
//!     `primary_pending` pool + the `primary_failed` ledger
//!     bookkeeping for cascade-fail entries.
//!   * Does NOT own: the validation rules (shared with the primary
//!     via `validate_spawn_tasks`), the broadcast helper
//!     (`apply_and_broadcast_mutations` in `broadcast.rs`), or the
//!     CRDT apply rule itself (`apply_locally_for_broadcast`).
//!
//! Why mirror rather than promote to a single coordinator: the primary
//! pool (`PrimaryCoordinator::pending_pool`) and the secondary's
//! `primary_pending` are two distinct fields, each owned by the
//! coordinator that drives dispatch for its run-phase. The shared
//! validator collapses the duplicated logic to the parts that
//! genuinely differ (the destination pool + the per-pass
//! `failed_tasks` mirror).

use dynrunner_core::{Identifier, MessageReceiver, MessageSender, TaskInfo};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, DistributedMessage, PeerTransport,
};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use super::super::SecondaryCoordinator;
use crate::cluster_state::TaskState;
use crate::primary::{validate_spawn_tasks, SpawnError};

impl<PT, P, M, S, E, I> SecondaryCoordinator<PT, P, M, S, E, I>
where
    PT: MessageSender<DistributedMessage<I>> + MessageReceiver<DistributedMessage<I>>,
    P: PeerTransport<I>,
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    /// Apply a runtime-task-injection batch on the promoted-secondary
    /// path. Pre-validates the input vec against the current ledger,
    /// builds a single `ClusterMutation::TasksSpawned` carrying the
    /// valid subset, applies+broadcasts it, then walks the post-apply
    /// CRDT state to route each valid entry to the right local
    /// destination (pool re-inject for `Pending`, `primary_failed`
    /// ledger insert for cascade-`Failed`).
    ///
    /// Returns per-index errors for the rejected entries. The rest of
    /// the batch proceeds regardless — same shape the primary's
    /// `apply_spawn_tasks` produces.
    ///
    /// Single-broadcast guarantee: a 100-task graph computed at
    /// runtime emits ONE `TasksSpawned` mutation, not 100. Idempotent
    /// under repetition by the CRDT's apply-time dedupe.
    ///
    /// Production caller: the secondary-side
    /// `PrimaryCommand::SpawnTasks` arm in
    /// `secondary/command_channel/handler.rs`, dispatched from the
    /// `command_rx` `select!` arm in `process_tasks` when a
    /// `PySecondaryCoordinator`-minted `PyPrimaryHandle` issues
    /// `spawn_tasks(...)` from inside a Python `on_phase_end`
    /// callback. The `phase_lifecycle_callback` regression tests
    /// pin the Rust-side contract independently of the PyO3 bridge.
    pub(in crate::secondary) async fn apply_spawn_tasks(
        &mut self,
        tasks: Vec<TaskInfo<I>>,
    ) -> Result<Vec<(usize, SpawnError)>, String> {
        // Closure-based shared validator: same shape the primary's
        // `apply_spawn_tasks` uses, same `cluster_state` probe — the
        // helper itself lives in `dynrunner-core` so the local-manager
        // command-channel handler can call it too without a manager →
        // manager dependency edge. The two closures expose the
        // `task_state(hash)` / `tasks_iter()` reads the validator
        // needs; the rule set (duplicate-hash, unknown-dep) stays
        // single-source in the core helper.
        let (valid_tasks, errors) = validate_spawn_tasks(
            |hash| self.cluster_state.task_state(hash).is_some(),
            |task_id| {
                self.cluster_state.tasks_iter().any(|(_, s)| {
                    let task = match s {
                        crate::cluster_state::TaskState::Pending { task }
                        | crate::cluster_state::TaskState::InFlight { task, .. }
                        | crate::cluster_state::TaskState::Completed { task }
                        | crate::cluster_state::TaskState::Failed { task, .. }
                        | crate::cluster_state::TaskState::Unfulfillable { task, .. }
                        | crate::cluster_state::TaskState::Blocked { task, .. }
                        | crate::cluster_state::TaskState::Cancelled { task, .. } => task,
                    };
                    task.task_id == task_id
                })
            },
            tasks,
        );

        if valid_tasks.is_empty() {
            // No mutation to broadcast; the per-index errors are the
            // entire result. Skip the apply+broadcast pass so we
            // don't emit an empty-batch wire event. Mirrors the
            // primary's identical short-circuit.
            return Ok(errors);
        }

        // Compute hashes of the valid subset so we can post-apply
        // inspect each entry's CRDT state to decide pool-side
        // bookkeeping. The hash function is deterministic; the apply
        // rule recomputes the same value internally, so the hashes
        // here line up with cluster_state's HashMap keys.
        let valid_hashes: Vec<String> = valid_tasks
            .iter()
            .map(crate::primary::wire::compute_task_hash)
            .collect();

        // Originator-side apply + dual fan-out (peer broadcast +
        // demoted-submitter loopback). The submitter relies on the
        // loopback to update its mirror; without it the submitter's
        // exit-counter check trips early. Errors are logged inside
        // the helper — the broadcast best-effort, the local apply
        // already happened, so the return value is silently
        // discarded here.
        let _ = self
            .apply_and_broadcast_mutations(vec![ClusterMutation::TasksSpawned {
                tasks: valid_tasks,
            }])
            .await;

        // Pool-side bookkeeping mirrors the primary path:
        //   * Pending → reinject into `primary_pending` so the next
        //     dispatch tick picks it up. `reinject` is the right
        //     primitive (vs `extend`): the pool's dep-tracking is the
        //     CRDT's concern, the pool just dispatches what's in it.
        //   * Blocked → no pool-side action; the CRDT auto-resume
        //     mechanism fires on a later `TaskCompleted` and
        //     re-injects via `apply_and_broadcast_mutations`'
        //     `resumed_for_dispatch` plumb.
        //   * Failed → record in the per-pass `primary_failed` ledger
        //     so the secondary's drain-check + retry-budget paths see
        //     the cascade-fail entry. Mirrors the primary's
        //     `failed_tasks.insert(...)` step on the same arm.
        for hash in valid_hashes {
            match self.cluster_state.task_state(&hash) {
                Some(TaskState::Pending { task }) => {
                    let task = task.clone();
                    if let Some(pool) = self.primary_pending.as_mut() {
                        pool.reinject(task);
                    }
                }
                Some(TaskState::Failed { task, kind, .. }) => {
                    // Mirror the primary's per-pass failed_tasks
                    // accounting onto the secondary's `primary_failed`
                    // ledger. The ledger carries the binary AND the
                    // ErrorType (the secondary's wire shape requires
                    // both for the outcome-class partition).
                    self.primary_failed.insert(
                        hash,
                        crate::secondary::FailedTaskEntry {
                            binary: task.clone(),
                            error_type: kind.clone(),
                        },
                    );
                }
                _ => {
                    // Blocked / other states: no pool-side action.
                }
            }
        }

        Ok(errors)
    }
}
