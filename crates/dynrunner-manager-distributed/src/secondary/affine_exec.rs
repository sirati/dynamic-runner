//! Secondary-side RUN-ONCE-PER-SECONDARY executor for a SecondaryAffine
//! IMPORT a work task gates on (#497 P4 — the headline correctness phase).
//!
//! ## The one concern
//! Run a SecondaryAffine task `I`'s per-secondary IMPORT AT MOST ONCE on this
//! node, gating EVERY work task on this node that depends on `I` behind that
//! single run. The secondary holds NO CRDT authority for this: the import +
//! the run-once latch are NODE-LOCAL (`OperationalState::affine_done` /
//! `affine_running`, never replicated); the only CRDT-visible effects are the
//! frames this executor REPORTS to the primary
//! ([`DistributedMessage::TaskQueuedAfterLocalDependency`] when a work task is
//! queued behind the import, [`DistributedMessage::LocalDependencyReleased`]
//! when it releases, [`DistributedMessage::TaskFailed`] when the import
//! fails). The primary ORIGINATES the authoritative CRDT mutation off each
//! frame (the work-split law) — this executor never writes the CRDT.
//!
//! ## The run-once invariant (the headline — owner-emphasized)
//! Multiple work tasks on the SAME secondary depending on the SAME
//! not-yet-imported `I` must run `I` EXACTLY ONCE; ALL of them enter
//! `QueuedAfterLocalDependency`. The latch is the PRESENCE of `I`'s hash in
//! `affine_running`:
//!   * 1st dependent → inserts `affine_running[I] = vec![dep]`, reports the
//!     queued state, spawns the ONE import.
//!   * 2nd..Nth dependent → APPENDS to the existing vec, reports the queued
//!     state, starts NO second import.
//!   * import `Ok` → drains the vec, inserts `I` into `affine_done`, sends one
//!     `LocalDependencyReleased` per queued dependent (→ each `B` `InFlight`).
//!   * import `Err` → sends one `TaskFailed{class}` per queued dependent
//!     (re-routable per #495), does NOT set `affine_done` (a later assignment
//!     / another secondary retries its OWN import — the done set is never
//!     poisoned).
//!
//! ## Module boundary (CLAUDE.md design-first)
//!   * Owner: the secondary. The seam it crosses is the EXISTING dispatch
//!     router (Phase 5), which calls [`Self::ensure_affine_import`] for a work
//!     task whose SecondaryAffine dependency is locally unmet — a one-line
//!     delegate; no run-once / queue logic in the router.
//!   * Execution body: the role-agnostic [`execute_affine_with_action`] core
//!     (read the task, run the action with a bounded transient retry, classify
//!     the outcome). The wrapper adds ONLY the secondary-specific parts:
//!     resolving `I`'s `TaskInfo` from the local replicated `cluster_state`,
//!     and routing each queued dependent's release / failure to the primary.
//!   * What callers see: `ensure_affine_import(affine_hash, dep)` →
//!     [`AffineGateOutcome`]. The router never learns the run-once latch, the
//!     queue, or the import.

use dynrunner_core::{Identifier, TaskInfo};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::DistributedMessage;
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use super::wire::timestamp_now;
use super::{AffineGateOutcome, PendingAffineDependent, SecondaryCoordinator};
use crate::affine_action::{IMPORT_OUTER_RETRIES, ImportActionHandle, ImportError};

/// The classified result of running a SecondaryAffine import in-process, after
/// the bounded transient retry. Mirrors
/// [`crate::setup_exec::SetupOutcome`]: the role wrapper maps `Success` onto
/// the release path (one `LocalDependencyReleased` per queued dependent) and
/// `Failure` onto the per-dependent `TaskFailed` path, carrying the #495
/// failure class so the authority can re-route (Recoverable) or cascade
/// (NonRecoverable).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::secondary) enum AffineOutcome {
    /// The import completed cleanly — the affine hash is now locally done.
    Success,
    /// The import failed; carries the #495 class to stamp on each queued
    /// dependent's `TaskFailed` terminal and the operator-facing reason.
    Failure {
        error_type: dynrunner_core::ErrorType,
        reason: String,
    },
}

/// The ASYNC execution path the secondary's run-once executor calls for the
/// single per-secondary import of a SecondaryAffine task — #497 P4's role-
/// agnostic core, mirroring [`crate::setup_exec::execute_setup_with_upload`].
///
/// Dispatch by the registered action:
///   * An `ImportAction` IS registered ⇒ run the import (with a bounded OUTER
///     retry on a `Transient` the provider could not absorb — see
///     [`IMPORT_OUTER_RETRIES`]; any per-step retry lives in the provider).
///     Provider `Ok` ⇒ `Success`; `Recoverable`/`NonRecoverable` ⇒ `Failure`
///     with that class; `Transient` exhausted ⇒ `Failure { Recoverable }`
///     (the import could not complete on THIS node, so re-route per #495).
///   * NO action is registered ⇒ `Failure { NonRecoverable }` (the executor
///     was asked to import but has no importer — a wiring error, surfaced
///     loudly rather than silently succeeded; a work task WITHOUT an affine
///     dependency never reaches here).
pub(in crate::secondary) async fn execute_affine_with_action<I>(
    task: &TaskInfo<I>,
    action: &ImportActionHandle<I>,
) -> AffineOutcome
where
    I: Identifier,
{
    let Some(importer) = action.as_ref() else {
        return AffineOutcome::Failure {
            error_type: dynrunner_core::ErrorType::NonRecoverable,
            reason: format!(
                "work task gates on SecondaryAffine import '{}' but no import \
                 action is registered on this secondary — wiring error",
                task.task_id,
            ),
        };
    };
    // Bounded OUTER retry: the provider owns any per-step transient retry, so
    // this only re-attempts a WHOLE-action transient the provider could not
    // absorb. A Recoverable / NonRecoverable failure short-circuits (no
    // retry) — its class is preserved.
    let mut last_transient: Option<String> = None;
    for attempt in 0..=IMPORT_OUTER_RETRIES {
        match importer.import(task).await {
            Ok(()) => return AffineOutcome::Success,
            Err(e) if e.is_transient() => {
                tracing::warn!(
                    target: "dynrunner_affine",
                    task_id = %task.task_id,
                    attempt,
                    reason = %e.reason(),
                    "secondary-affine import hit a transient failure; re-attempting"
                );
                last_transient = Some(e.reason().to_string());
            }
            Err(e) => {
                return AffineOutcome::Failure {
                    error_type: e.error_type(),
                    reason: format!(
                        "secondary-affine import of '{}' failed: {}",
                        task.task_id,
                        e.reason(),
                    ),
                };
            }
        }
    }
    // Transient exhaustion folds into a Recoverable work-task failure: the
    // import could not complete on THIS node, but another may import cleanly.
    AffineOutcome::Failure {
        error_type: ImportError::Transient(String::new()).error_type(),
        reason: format!(
            "secondary-affine import of '{}' failed after {} transient attempts: {}",
            task.task_id,
            IMPORT_OUTER_RETRIES + 1,
            last_transient.unwrap_or_else(|| "unknown transient fault".to_string()),
        ),
    }
}

impl<M, S, E, I> SecondaryCoordinator<M, S, E, I>
where
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    /// Gate a work task `B` on its locally-unmet SecondaryAffine dependency
    /// `I` (#497 P4 — the run-once entry point the dispatch router calls).
    ///
    /// Partitions on the node-local sets:
    ///   * `affine_hash ∈ affine_done` → [`AffineGateOutcome::AlreadyDone`]:
    ///     the import already ran on this node; the caller releases `B`
    ///     straight to `InFlight` (no queue, no report, no import).
    ///   * `affine_hash ∈ affine_running` → APPEND `dependent` to the existing
    ///     queue, report `TaskQueuedAfterLocalDependency` for `B`, start NO
    ///     second import → [`AffineGateOutcome::QueuedBehindRun`].
    ///   * otherwise (FIRST dependent) → insert `affine_running[hash] =
    ///     vec![dependent]`, report `TaskQueuedAfterLocalDependency` for `B`,
    ///     run EXACTLY ONE import → [`AffineGateOutcome::StartedRun`].
    ///
    /// The PRESENCE of the hash key in `affine_running` is the run-once latch:
    /// only the first call (which finds neither set populated) returns
    /// [`AffineGateOutcome::StartedRun`], TELLING the caller it is the one
    /// that must now drive [`Self::run_affine_import_once`]; every concurrent
    /// / later dependent on the SAME hash finds the key present and only
    /// queues ([`AffineGateOutcome::QueuedBehindRun`]).
    ///
    /// ## Why the gate is SYNCHRONOUS (does not await the import)
    /// This method performs ONLY the synchronous run-once decision (check the
    /// node-local sets, append the dependent, report the queued state) and
    /// returns WITHOUT running the import. Driving the import here would hold
    /// `&mut self` across the (blocking) import await, so the 2nd..Nth
    /// dependent assignments could not be processed and queued WHILE the
    /// single import is in flight — exactly the run-once-under-concurrency
    /// invariant. The caller drives the import via `run_affine_import_once`
    /// on a `StartedRun`, decoupled from the queue-and-report gate, so ALL N
    /// dependents enter `QueuedAfterLocalDependency` behind the ONE run.
    // Reached via the dispatch router's TaskAssignment arm in Phase 5 (#497
    // P5 wires the `unmet_local_affine_dep` intercept); Phase 4 builds + tests
    // this executor at the executor level with a stub ImportAction.
    #[allow(dead_code)]
    pub(in crate::secondary) async fn ensure_affine_import(
        &mut self,
        affine_hash: String,
        dependent: PendingAffineDependent<I>,
    ) -> Result<AffineGateOutcome, String> {
        // Already locally imported: release straight to InFlight. No queue, no
        // report — the caller (router) re-emits the standard assignment.
        if self.op_mut().affine_done.contains(&affine_hash) {
            return Ok(AffineGateOutcome::AlreadyDone);
        }

        // Whether THIS call is the first dependent (it must drive the single
        // import) or a 2nd..Nth (it only queues). The vacancy of the
        // `affine_running` key is the discriminator — checked + claimed under
        // one borrow so two near-simultaneous calls cannot both claim "first".
        let is_first = !self.op_mut().affine_running.contains_key(&affine_hash);
        let work_hash = dependent.work_hash.clone();
        self.op_mut()
            .affine_running
            .entry(affine_hash.clone())
            .or_default()
            .push(dependent);

        // Every queued dependent (first or not) reports the CRDT-visible
        // queued state, so the primary/observer SEE `B` waiting on the local
        // import (never silently stuck mid-InFlight). The primary originates
        // `QueuedAfterLocalDependencySet` off this frame.
        self.report_queued_after_local_dependency(work_hash, affine_hash)
            .await?;

        // The import itself is driven by the caller off a `StartedRun` (the
        // synchronous-gate rationale above): exactly one dependent per (node,
        // affine hash) sees the key vacant.
        Ok(if is_first {
            AffineGateOutcome::StartedRun
        } else {
            AffineGateOutcome::QueuedBehindRun
        })
    }

    /// Run the SINGLE per-secondary import for `affine_hash` and drain every
    /// dependent queued behind it (#497 P4). Called EXACTLY ONCE per import,
    /// from the first-dependent branch of [`Self::ensure_affine_import`].
    ///
    /// On `Success`: mark the hash locally-done, take the queued dependents,
    /// and send one `LocalDependencyReleased` per dependent (→ the primary
    /// originates `TaskAssigned` → `B` `InFlight`). On `Failure`: send one
    /// `TaskFailed{class}` per dependent (re-routable per #495) and do NOT set
    /// `affine_done` — a later assignment / another secondary retries its OWN
    /// import (the done set is never poisoned).
    // Driven by the dispatch router on a `StartedRun` in Phase 5 (#497 P5);
    // Phase 4 drives it directly from the executor-level tests.
    #[allow(dead_code)]
    pub(in crate::secondary) async fn run_affine_import_once(
        &mut self,
        affine_hash: String,
    ) -> Result<(), String> {
        // Resolve `I`'s TaskInfo from this node's replicated CRDT mirror (a
        // prior `TaskAdded` broadcast seeded it; the Phase-2 originator put it
        // in `AffineReady`). CLONE it out so the `cluster_state` borrow ends
        // before the async import runs against `&self.import_action`. A hash
        // this node does not know is a structural wiring fault — fail every
        // queued dependent non-recoverably rather than leave them stranded.
        let task = self
            .cluster_state
            .task_state(&affine_hash)
            .map(|state| state.task().clone());

        let outcome = match task {
            Some(task) => execute_affine_with_action(&task, &self.import_action).await,
            None => AffineOutcome::Failure {
                error_type: dynrunner_core::ErrorType::NonRecoverable,
                reason: format!(
                    "SecondaryAffine gate '{affine_hash}' absent from the local \
                     ledger (assignment outran TaskAdded, or a concurrent \
                     removal) — failing its queued dependents non-recoverably",
                ),
            },
        };

        // Drain the queued dependents for THIS hash, clearing the run-once
        // latch. On success the hash also enters `affine_done` so any FUTURE
        // dependent releases immediately; on failure the done set is left
        // untouched (no poison) so a fresh `ensure_affine_import` starts a new
        // single run.
        let dependents = self
            .op_mut()
            .affine_running
            .remove(&affine_hash)
            .unwrap_or_default();
        if matches!(outcome, AffineOutcome::Success) {
            self.op_mut().affine_done.insert(affine_hash.clone());
        }

        for dependent in dependents {
            match &outcome {
                AffineOutcome::Success => {
                    // Release `B`: the primary originates the EXISTING
                    // `TaskAssigned` (→ `B` `InFlight`) off this frame — NOT a
                    // second InFlight originator.
                    self.report_local_dependency_released(
                        dependent.work_hash,
                        dependent.worker_id,
                    )
                    .await?;
                }
                AffineOutcome::Failure { error_type, reason } => {
                    // Fail `B` with the import's #495 class, reusing the
                    // worker-terminal `TaskFailed` report path. Recoverable ⇒
                    // re-routable to a secondary that runs its OWN import.
                    self.report_deferred_task_failed(
                        dependent.worker_id,
                        &dependent.work_hash,
                        error_type.clone(),
                        reason.clone(),
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }

    /// Report a work task `B` as QUEUED behind this node's local SecondaryAffine
    /// import (#497 P4) — the secondary half of the queued state (the primary
    /// originates `QueuedAfterLocalDependencySet`). Reuses the same
    /// `send_to_primary` chokepoint every primary-bound report uses.
    async fn report_queued_after_local_dependency(
        &mut self,
        task_hash: String,
        affine_hash: String,
    ) -> Result<(), String> {
        let report = DistributedMessage::TaskQueuedAfterLocalDependency {
            target: None,
            sender_id: self.config.secondary_id.clone(),
            timestamp: timestamp_now(),
            secondary_id: self.config.secondary_id.clone(),
            task_hash,
            affine_hash,
        };
        self.send_to_primary(report).await
    }

    /// Report that this node's local SecondaryAffine import for the task queued
    /// as `task_hash` is DONE — release it (#497 P4). The primary originates
    /// the EXISTING `TaskAssigned` off this frame, pinning the same
    /// `(secondary, worker)` pair this node chose.
    async fn report_local_dependency_released(
        &mut self,
        task_hash: String,
        worker_id: dynrunner_core::WorkerId,
    ) -> Result<(), String> {
        let report = DistributedMessage::LocalDependencyReleased {
            target: None,
            sender_id: self.config.secondary_id.clone(),
            timestamp: timestamp_now(),
            secondary_id: self.config.secondary_id.clone(),
            task_hash,
            worker_id,
        };
        self.send_to_primary(report).await
    }
}
