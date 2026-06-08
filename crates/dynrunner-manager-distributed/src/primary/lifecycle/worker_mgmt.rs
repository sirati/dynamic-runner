//! Worker management's reaction to the decoupled signal bus.
//!
//! Single concern: the worker-management side of the
//! `crate::worker_signal` bus. The operational loop's `select!` arm
//! drains a coalesced [`crate::worker_signal::WorkerSignalBatch`] and
//! hands it here; this module owns the policy for what to do with each
//! signal WITHOUT the phase/task code that emitted it ever calling
//! worker management directly (the dispatch-decoupling law). The three
//! reactions:
//!
//!   - [`WorkerMgmtSignal::TasksAdded`] → re-run the dispatch recheck
//!     over EVERY free worker (`held_task().is_none()`), bypassing the
//!     per-secondary backpressure backoff (a real `TasksAdded` means
//!     circumstances changed).
//!   - [`WorkerMgmtSignal::PhaseStartedNeedsWorkers`] → a liveness
//!     check: if the started phase needs workers and the cluster has
//!     none AND no fleet recovery is in progress or possible, the phase
//!     can never make progress → escalate to a clean run failure.
//!   - [`WorkerMgmtSignal::RunShouldFail`] → record the break outcome
//!     (typed `RunError::Other`, the swallow-eligible generic failure)
//!     so the operational loop exits and `run_pipeline` surfaces the
//!     failure (the worker arm OWNS the clean-shutdown drive).
//!   - [`WorkerMgmtSignal::PolicyFatalExit`] → identical break-outcome
//!     mechanism, but the typed outcome is `RunError::FatalPolicyExit`
//!     (the PyO3 boundary RAISES it, never swallows) — the consumer
//!     `on_phase_end`-raise path.
//!
//! Each reaction runs against `&mut self` (worker-management state) from
//! inside the operational `select!`, never on a spawned task, so the
//! `await_holding_lock` / `await_holding_refcell_ref` lints stay clean.

use dynrunner_core::Identifier;
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use crate::primary::{PrimaryCoordinator, RunError};
use crate::worker_signal::{WorkerMgmtSignal, WorkerSignalBatch};

impl<S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<S, E, I> {
    /// Worker management's reaction to one coalesced signal batch
    /// drained from the bus. Acts on every signal in arrival order —
    /// the batch preserves them all (unlike the matcher's latest-only
    /// collapse) because a `RunShouldFail` must not be lost behind a
    /// later `TasksAdded`.
    ///
    /// A burst of N `TasksAdded` collapses into ONE dispatch recheck:
    /// the recheck is idempotent over the pool/worker view, so we run it
    /// at most once per batch even if the batch carried several
    /// `TasksAdded`. The other two signals act per-occurrence.
    pub(crate) async fn react_to_worker_signal_batch(&mut self, batch: WorkerSignalBatch) {
        let mut tasks_added = false;
        for signal in batch.signals {
            match signal {
                // Coalesce: a batch may carry several `TasksAdded`
                // (e.g. a phase queues a wave of items). One recheck
                // covers them all — defer it to after the batch walk so
                // every just-spawned task is in the pool first.
                WorkerMgmtSignal::TasksAdded => {
                    tasks_added = true;
                }
                WorkerMgmtSignal::PhaseStartedNeedsWorkers { phase, min } => {
                    self.handle_phase_started_needs_workers(&phase, min);
                }
                WorkerMgmtSignal::RunShouldFail { reason } => {
                    // Generic run-should-fail wedge → the swallow-eligible
                    // `Other` (the pre-existing stay-local-primary exit-0
                    // behaviour the PyO3 boundary keeps for unexpected
                    // generic failures).
                    self.record_run_fail_outcome(RunError::Other(reason));
                }
                WorkerMgmtSignal::PolicyFatalExit { reason } => {
                    // Consumer-/policy-driven fatal abort (e.g. an
                    // `on_phase_end` hook raised) → the structured
                    // `FatalPolicyExit` the PyO3 boundary RAISES on. Same
                    // break-outcome latch + clean-shutdown drive as the
                    // generic case; only the typed outcome differs.
                    self.record_run_fail_outcome(RunError::FatalPolicyExit { reason });
                }
            }
        }
        if tasks_added {
            // A genuine `TasksAdded` recheck BYPASSES the per-secondary
            // backpressure backoff: circumstances changed (new work
            // entered the pool, or a worker freed elsewhere), so a freed
            // slot on a recently-backpressured secondary is a valid
            // dispatch target again. The OOM single-worker mask is NOT
            // bypassed (that one is correctness, not a rate-limit).
            // Send failures are logged + rolled back inside the recheck;
            // `.ok()` swallows the transient so the reaction can't abort
            // the loop.
            self.dispatch_to_idle_workers(true).await.ok();
            // Lazy on-demand dead-secondary requeue. AFTER the dispatch
            // pass returns (NEVER inside the per-worker loop:
            // `requeue_dead_secondary` runs `self.workers.retain(..)`,
            // which would invalidate the `dispatch_order` indices the loop
            // iterates — a use-after-free hazard). If the pass left an idle
            // worker with nothing to dispatch and the only remaining work
            // is in-flight on silent secondaries, declare those holders
            // dead so their tasks return to the pool. The declaration
            // re-emits `TasksAdded` (inside `requeue_dead_secondary`),
            // which the NEXT loop iteration drains and re-dispatches — bus,
            // not synchronous recursion. Consulted as the two boundary
            // methods only; dispatch never learns the silence policy.
            self.maybe_requeue_silent_held_work().await;
        }
    }

    /// The dispatch-altitude consult of the starvation oracle + command.
    /// Single concern: translate "only silent-held work remains" into a
    /// dead-secondary declaration. Pure consumer of the liveness module's
    /// two boundary methods — it neither computes the silent set nor
    /// touches the worker-management bus (the wrapped
    /// `requeue_dead_secondary` re-nudges `TasksAdded`).
    async fn maybe_requeue_silent_held_work(&mut self) {
        if !self.only_silent_held_work_remains() {
            return;
        }
        let dead = self.silent_held_dead_declarations();
        // Reuse the keepalive-miss cause: a silent secondary whose only
        // remaining role is sitting on un-dispatchable in-flight work is a
        // missed-keepalive outage by the consumer-facing semantic.
        self.declare_silent_secondaries_dead(
            dead,
            dynrunner_protocol_primary_secondary::RemovalCause::KeepaliveMiss,
        )
        .await
        .ok();
    }

    /// Liveness check for a started phase that needs workers. The phase
    /// layer emitted [`WorkerMgmtSignal::PhaseStartedNeedsWorkers`]
    /// stating demand; this is worker management deciding whether that
    /// demand can be met.
    ///
    /// - `min == 0`: the phase makes no worker demand — nothing to do.
    /// - At least one alive worker: the phase will dispatch its work
    ///   through the next `TasksAdded` recheck (a single worker drains a
    ///   phase sequentially). Throughput scale-up beyond the floor is
    ///   not a correctness concern, so we do not force-spawn here.
    /// - Zero alive workers: the phase can only make progress if the
    ///   fleet recovers. If a respawn is in flight or still possible we
    ///   let the death-driven respawn pipeline produce a worker. If
    ///   recovery is neither in progress nor possible, the phase is
    ///   wedged forever — escalate to a clean run failure rather than
    ///   idle until an unrelated timeout.
    fn handle_phase_started_needs_workers(&mut self, phase: &dynrunner_core::PhaseId, min: usize) {
        if min == 0 {
            return;
        }
        if self.alive_worker_count() > 0 {
            return;
        }
        if self.fleet_recovery_in_progress_or_possible() {
            tracing::info!(
                phase = %phase,
                min,
                "phase started needing workers with none alive; fleet recovery \
                 in progress or possible — deferring to the respawn pipeline"
            );
            return;
        }
        let reason = format!(
            "phase {phase} started needing {min} worker(s) but the cluster has \
             none alive and no fleet recovery is in progress or possible"
        );
        tracing::error!(phase = %phase, min, "{reason}");
        // The phase-floor liveness wedge is the generic run-should-fail
        // class (swallow-eligible `Other`), same as the phase
        // proceed-or-fail decision — NOT a consumer-policy fatal.
        self.record_run_fail_outcome(RunError::Other(reason));
    }

    /// Record the run-fail break outcome as the TYPED `RunError` the run
    /// should surface. Idempotent: the first outcome wins (a later
    /// signal in the same run does not overwrite the originating cause).
    /// The operational loop reads `worker_mgmt_fail_outcome.is_some()` at
    /// the top of its next iteration and breaks; `run_pipeline` then
    /// returns the recorded outcome verbatim. The signal-to-`RunError`
    /// classification happens at the single call site in
    /// `react_to_worker_signal_batch` (generic wedge → `Other`,
    /// consumer-policy abort → `FatalPolicyExit`); this method is the one
    /// latch-write both classes funnel through.
    fn record_run_fail_outcome(&mut self, outcome: RunError) {
        if self.worker_mgmt_fail_outcome.is_some() {
            return;
        }
        tracing::warn!(error = %outcome, "worker management: run should fail");
        self.worker_mgmt_fail_outcome = Some(outcome);
    }

    /// Count of alive workers across the fleet. A worker is alive iff it
    /// is a registered slot — `self.workers` only holds slots for
    /// secondaries the primary believes are operational (the dead-
    /// secondary path removes them via `self.workers.retain(..)`). Both
    /// free and busy slots count as alive (a busy worker is still making
    /// progress). Single concern: fleet-liveness arithmetic for the
    /// worker-management phase-floor check.
    fn alive_worker_count(&self) -> usize {
        self.workers.len()
    }

    /// True iff a fleet recovery is in progress (a respawn task is in
    /// flight) OR still possible (the respawn pipeline is enabled and
    /// the total budget is not yet exhausted). Used by the phase-floor
    /// liveness check to distinguish "transiently zero workers, recovery
    /// underway" from "permanently wedged, escalate".
    ///
    /// Single concern: the recovery-feasibility predicate. The
    /// per-secondary cap and cooldown are deliberately NOT consulted
    /// here — they gate an individual respawn DECISION (the
    /// `RespawnBudget::should_respawn(original_id, ..)` family-chain
    /// check), which is keyed on a SPECIFIC dead family. This predicate's
    /// caller, the phase-floor liveness check
    /// ([`Self::handle_phase_started_needs_workers`]), is family-AGNOSTIC:
    /// it fires on "a phase started but zero workers are alive anywhere"
    /// and has no dead-secondary id in scope — there is no single family
    /// to consult `should_respawn` against. Failover surfaces a dead-id
    /// at the death-detection / requeue site (`process_heartbeat_tick`),
    /// NOT here, so the 3B "tighten to the full `should_respawn`
    /// predicate" refinement does not apply at this site; the coarse
    /// total-budget question ("could the fleet come back at all") is the
    /// correct shape here. It is conservative-by-design — it never
    /// spuriously escalates (it errs toward "recovery possible", so a
    /// per-family-exhausted-but-total-budget-remaining cluster defers to
    /// the respawn pipeline rather than failing the run), which is the
    /// safe direction for a liveness floor.
    fn fleet_recovery_in_progress_or_possible(&self) -> bool {
        if !self.respawn_tasks.is_empty() {
            return true;
        }
        match (self.respawn_spawner.as_ref(), self.respawn_budget.as_ref()) {
            (Some(_), Some(budget)) => {
                (self.cluster_state.respawn_events().len() as u32) < budget.max_total
            }
            _ => false,
        }
    }
}
