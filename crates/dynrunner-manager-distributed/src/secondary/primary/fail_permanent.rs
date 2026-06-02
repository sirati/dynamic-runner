//! Promoted-secondary side of `PrimaryCommand::FailPermanent`.
//!
//! Single concern: mirror `PrimaryCoordinator::apply_fail_permanent` for
//! the promoted-secondary path so external callers (Python `PrimaryHandle`
//! held by a `PySecondaryCoordinator`) can drive permanent-failure +
//! cascade via the secondary's `primary_pending` pool / `primary_failed`
//! ledger / `apply_and_broadcast_mutations` broadcast helper.
//!
//! Module boundary:
//!   * Owns: the post-resolution wiring (primary_failed ledger insert,
//!     pool cascade walk, per-phase counter bump, phase-lifecycle
//!     fire, mutation build, broadcast).
//!   * Shares with the primary: the `pending_pool::on_item_failed_permanent`
//!     cascade primitive (in `dynrunner-scheduler-api`) — both
//!     coordinators reuse the same pool method, so cascade-routing
//!     semantics (which dependents are dropped, which stay) cannot
//!     drift.
//!
//! Wire / CRDT effects: same shape the primary produces. One
//! `TaskFailed` for the originating hash, plus one `TaskBlocked` per
//! cascade-paused dependent (only on `ErrorType::Unfulfillable`).
//! Non-Unfulfillable cascades record dependents in the local
//! `primary_failed` ledger directly (no extra broadcast) — same
//! convention `note_primary_item_failed` uses on the secondary's
//! worker-event path.
//!
//! Acting-as-primary precondition: the handler assumes
//! `primary_pending` is populated. Pre-promotion the field is `None`
//! and the cascade walk is a silent no-op (no dependents discovered);
//! the originator's `TaskFailed` is still broadcast so the CRDT
//! mirror converges, but the local ledger / counter steps fire
//! against a non-existent pool. The PyO3 caller surface holds
//! `PrimaryHandle` for the `PySecondaryCoordinator`'s `on_run_start`,
//! which only issues commands once the secondary is acting as
//! primary, so the pre-promotion path is structurally cold.

use dynrunner_core::{ErrorType, Identifier, MessageReceiver, MessageSender};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, DistributedMessage, PeerTransport,
};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};
use tokio::sync::mpsc as tokio_mpsc;

use super::super::SecondaryCoordinator;
use crate::cluster_state::TaskState;
use crate::primary::PrimaryCommand;

impl<PT, P, M, S, E, I> SecondaryCoordinator<PT, P, M, S, E, I>
where
    PT: MessageSender<DistributedMessage<I>> + MessageReceiver<DistributedMessage<I>>,
    P: PeerTransport<I>,
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    /// Resolve a task hash through the CRDT ledger and return
    /// `(phase_id, task_id, binary)` for the pool's bookkeeping.
    /// Mirrors `PrimaryCoordinator::task_meta_for_hash` but also
    /// returns the binary so cascade-fail dependents can record
    /// it into `primary_failed` without a second ledger lookup.
    fn task_meta_for_hash_with_binary(
        &self,
        hash: &str,
    ) -> Option<(dynrunner_core::PhaseId, String, dynrunner_core::TaskInfo<I>)> {
        let state = self.cluster_state.task_state(hash)?;
        let task = match state {
            TaskState::Pending { task }
            | TaskState::InFlight { task, .. }
            | TaskState::Completed { task }
            | TaskState::Failed { task, .. }
            | TaskState::Unfulfillable { task, .. }
            | TaskState::Blocked { task, .. } => task,
        };
        Some((task.phase_id.clone(), task.task_id.clone(), task.clone()))
    }

    /// Handler for `PrimaryCommand::FailPermanent` on the promoted-
    /// secondary path. Wraps the existing
    /// `pending_pool::on_item_failed_permanent` primitive so the
    /// cascade-to-dependents semantics that primitive owns also apply
    /// to externally-requested failures, then broadcasts the
    /// `TaskFailed` mutation so every node mirrors the terminal state.
    ///
    /// Cascade routing splits on `error`:
    /// * `ErrorType::Unfulfillable { .. }` — dependents are broadcast
    ///   as `ClusterMutation::TaskBlocked { hash, on: <root> }`, so
    ///   the CRDT mirrors land in `TaskState::Blocked { on, task }`
    ///   on every replica. Dependents are NOT recorded in the local
    ///   per-pass `primary_failed` ledger — they're cascade-paused,
    ///   not failed.
    /// * Any other `ErrorType` — dependents are recorded in the local
    ///   `primary_failed` ledger with the same error (same shape a
    ///   worker-driven cascade-fail produces via
    ///   `note_primary_item_failed`).
    ///
    /// Pool-side auto-resume of cascade-paused dependents rides on
    /// `apply_and_broadcast_mutations`' shared
    /// `apply_locally_for_broadcast` plumb — when the prereq's
    /// `TaskCompleted` later flows through the apply path it surfaces
    /// resumed items for the pool to re-inject. Same CRDT/pool
    /// coherence guarantee the primary's path provides.
    pub(in crate::secondary) async fn apply_fail_permanent(
        &mut self,
        hash: String,
        error: ErrorType,
        reason: String,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
    ) -> Result<(), String> {
        let Some((phase_id, task_id, _binary)) =
            self.task_meta_for_hash_with_binary(&hash)
        else {
            return Err(format!("fail_permanent: unknown task hash {hash}"));
        };

        // Record the failure in the local per-pass ledger so the
        // promoted-secondary's accounting + per-phase counters match
        // the wire-side state. Same shape `apply_spawn_tasks` produces
        // for cascade-Failed entries (see spawn_tasks.rs).
        //
        // The `_binary` above is the originating task's binary as held
        // in cluster_state; the failed-ledger entry mirrors the
        // primary's `failed_tasks` shape but carries the binary too so
        // a future retry pass has the TaskInfo to re-inject (the
        // primary's separate `all_binaries` list serves this role on
        // the primary side; the secondary has the binaries only via
        // cluster_state, which is what we're reading here).
        self.primary_failed.insert(
            hash.clone(),
            crate::secondary::FailedTaskEntry {
                binary: _binary,
                error_type: error.clone(),
            },
        );

        // Cascade-to-dependents via the pool primitive. The returned
        // list is the dependents the pool just gave up on; how the
        // caller observes them depends on the error class
        // (cascade-pause for Unfulfillable, cascade-fail otherwise).
        // `primary_pending` may be `None` pre-promotion — silent skip;
        // the originator's broadcast still goes out. `task_id` is
        // non-optional per the framework's boundary contract.
        let cascaded_blocks: Vec<(String, String)> = {
            let cascaded = if let Some(pool) = self.primary_pending.as_mut() {
                pool.on_item_failed_permanent(&phase_id, task_id.as_str())
            } else {
                Vec::new()
            };
            let is_unfulfillable = matches!(error, ErrorType::Unfulfillable { .. });
            let mut blocks = Vec::new();
            for cascaded_binary in cascaded.into_iter() {
                let cascaded_hash =
                    crate::primary::wire::compute_task_hash(&cascaded_binary);
                if is_unfulfillable {
                    blocks.push((cascaded_hash, hash.clone()));
                } else {
                    self.primary_failed.insert(
                        cascaded_hash,
                        crate::secondary::FailedTaskEntry {
                            binary: cascaded_binary,
                            error_type: error.clone(),
                        },
                    );
                }
            }
            blocks
        };

        // Per-phase counter bump + lifecycle cascade. Mirrors the
        // `note_item_failed` step on the primary's `apply_fail_permanent`
        // (and the secondary's own `note_primary_item_failed` worker-
        // event handler). Counter bump fires BEFORE the cascade-drain
        // poll so `on_phase_end(phase, completed, failed)` observes the
        // failure when this failure is the one taking the phase to
        // `Drained`.
        *self
            .primary_phase_failed
            .entry(phase_id.clone())
            .or_insert(0) += 1;
        if let Some(pool) = self.primary_pending.as_mut() {
            pool.on_item_finished(&phase_id, Some(task_id.as_str()));
        }
        self.process_primary_phase_lifecycle(command_rx).await;

        // Broadcast the terminal state for the originating task plus
        // any cascade-paused dependents (Unfulfillable case only).
        // Ordering: originating `TaskFailed` first so receivers see
        // the prereq's terminal state before any dependent's Blocked
        // state — the cascade root is visible whenever a dependent's
        // `on` field is consulted. Same ordering invariant the
        // primary's path enforces.
        let mut mutations: Vec<ClusterMutation<I>> =
            Vec::with_capacity(1 + cascaded_blocks.len());
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
        // Best-effort broadcast: `apply_and_broadcast_mutations`
        // already swallows transport-level errors (logged, not
        // propagated). Forward its return through so the reply oneshot
        // surfaces the same error-class shape the primary's path does.
        self.apply_and_broadcast_mutations(mutations).await
    }
}
