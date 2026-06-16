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
use tokio::sync::mpsc as tokio_mpsc;

use crate::primary::command_channel::PrimaryCommand;
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
    pub(crate) async fn react_to_worker_signal_batch(
        &mut self,
        batch: WorkerSignalBatch,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
    ) {
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
            // Setup-task dispatch: the symmetric SELECTION pass for
            // `TaskKind::Setup` tasks (a setup task entering the pool emits
            // the same `TasksAdded`). Routes each setup task whose affinity
            // member is connected to its in-process executor (off-primary
            // member) or runs it locally (primary affinity). Self-contained
            // in `primary::setup_dispatch`; the worker recheck never learns
            // the setup concern. Runs alongside — not inside — the worker
            // recheck because the two select disjoint task sets (worker work
            // vs setup) over the SAME pool.
            self.dispatch_setup_tasks(command_rx).await;
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
            // Best-effort estimate-escalation rescue (#499). LAST in the
            // post-dispatch chain: every normal path (worker recheck,
            // setup/affine passes, silent-held requeue) has already had its
            // chance, so this fires ONLY on work that is genuinely
            // estimate-stalled — no queued task fits any per-worker budget
            // while an assignable worker idles. It re-attempts the stuck
            // tasks against the largest secondary's full capacity (the
            // distributed analog of local's unassigned-phase budget boost)
            // and fails the genuinely-unfittable ones individually as
            // ResourceExhausted, converting a whole-pool strand into
            // best-effort dispatch + actionable per-task failure.
            // Self-contained in `primary::estimate_escalation`; the worker
            // recheck never learns the escalation concern.
            self.escalate_estimate_stalled_dispatch(command_rx).await;
        }
    }

    /// React to a worker-roster GROWTH: a previously-unknown secondary's
    /// `SecondaryCapacity` was just applied (a worker became ready), so a
    /// new idle slot now exists in the replicated ledger but NOT yet in
    /// the primary-local `self.workers` cache.
    ///
    /// Single concern: keep the worker-roster derived cache + the dispatch
    /// recheck coherent with a capacity-record growth, exactly as the
    /// task-ledger growth surfaces (`TasksAdded`) keep the pool + recheck
    /// coherent. The two coupled steps:
    ///   1. REBUILD `self.workers` via the SOLE roster builder
    ///      [`Self::reconstruct_workers_from_cluster_state`] — idempotent,
    ///      name-sorted round-robin, and re-crosses every replicated
    ///      `TaskState::InFlight` back onto its slot, so a rebuild
    ///      mid-bringup never zeroes a committed-and-originated slot (the
    ///      live dispatch sites originate `TaskAssigned` immediately after
    ///      a successful send, so every committed slot is `InFlight` in the
    ///      CRDT by the time any capacity record can interleave).
    ///   2. EMIT [`WorkerMgmtSignal::TasksAdded`] so the existing dispatch
    ///      recheck (`dispatch_to_idle_workers`) re-evaluates EVERY free
    ///      worker — now including the freshly-rostered idle slot — against
    ///      the ready pool. Decoupled emit, never a direct dispatch call
    ///      (the dispatch-decoupling law): the operational loop's
    ///      worker-management arm (or a pre-loop wait's inline drain) runs
    ///      the recheck off the bus.
    ///
    /// This is the worker-ready half of "dispatch is a pure function of
    /// (ready-tasks ∩ idle-worker-capacity), re-evaluated on every event
    /// that can create a match": `TasksAdded` covers a new task arriving at
    /// an idle worker; this covers a new idle worker arriving at a ready
    /// task. Both startup (a secondary whose capacity lands AFTER
    /// `perform_initial_assignment`) and mid-run (a type-shift-respawned
    /// worker becoming ready after its phase's assignment) converge here.
    pub(crate) fn react_to_capacity_growth(&mut self) {
        self.reconstruct_workers_from_cluster_state();
        self.cluster_state
            .emit_worker_mgmt(WorkerMgmtSignal::TasksAdded);
    }

    /// Drain whatever worker-management signals are CURRENTLY queued on
    /// the bus (non-blocking) and run the same reaction the operational
    /// loop's parked worker-management arm runs. Returns `true` iff a
    /// batch was drained (at least one signal was queued).
    ///
    /// Single concern: let a caller OUTSIDE the operational loop service
    /// the worker-management bus synchronously at a point where leaving a
    /// queued `TasksAdded` unserviced would stall. The two callers are the
    /// pre-loop waits (`wait_for_connections` / `wait_for_mesh_ready`) and
    /// the operational-loop entry sweep:
    ///
    ///   - PRE-LOOP IN-WAIT SERVICING. `TasksAdded` signals emitted
    ///     DURING a wait — a late `SecondaryCapacity` growing the roster
    ///     (`react_to_capacity_growth`) or a `MeshReady` confirming a
    ///     member into the assignable set (`handle_mesh_ready`'s
    ///     confirmation-edge wakeup) — land on the bus, but the
    ///     operational loop (the usual drain) has not started yet, so the
    ///     dispatch recheck would be deferred past the wait and ready
    ///     work would pool while admitted members idle. Draining +
    ///     reacting inline dispatches NOW, per confirmation edge. (The
    ///     `MeshReady` that `wait_for_mesh_ready` blocks on is not
    ///     dispatch-driven: secondaries reach their operational loop via
    ///     the ungated setup-trio fan-out and report from there.)
    ///
    ///   - ENTRY SWEEP. Any `TasksAdded` emitted across the pre-loop chain
    ///     (initial empty-phase cascade, a late capacity, an `on_phase_end`
    ///     spawn) is serviced once at loop entry so the steady state is
    ///     reached with dispatch already a pure function of
    ///     (ready-tasks ∩ idle-worker-capacity), never waiting for the
    ///     next bus event to first act on a backlog.
    ///
    /// Take-drain-react-putback the receiver, mirroring the operational
    /// loop's own borrow discipline (`react_to_worker_signal_batch` needs
    /// `&mut self`; the drain needs `&mut rx`). A `None` receiver (already
    /// consumed by a prior loop entry) is a no-op.
    pub(crate) async fn drain_and_react_to_pending_worker_signals(&mut self) -> bool {
        let Some(mut rx) = self.worker_mgmt_rx.take() else {
            return false;
        };
        let drained = crate::worker_signal::try_collect_worker_signal_batch(&mut rx);
        self.worker_mgmt_rx = Some(rx);
        match drained {
            Some(batch) => {
                // Pre-loop in-wait servicing: no operational command channel
                // is taken yet, so a setup task that self-execs here drives
                // its phase cascade with no inline callback receiver (matching
                // every other pre-loop `note_item_*` caller's `&mut None`).
                self.react_to_worker_signal_batch(batch, &mut None).await;
                true
            }
            None => false,
        }
    }

    /// The dispatch-altitude consult of the starvation oracle + command.
    /// Single concern: translate "only silent-held work remains" into a
    /// LOCAL scheduling-suspect — recover the in-flight tasks back into
    /// the pool so idle workers don't starve waiting on stalled holders,
    /// but DO NOT mesh-declare the holders dead (no `PeerRemoved`, no
    /// `TimeoutDetected`, no respawn). The owner-approved #556 split: a
    /// primary may suspect a secondary locally for scheduling purposes
    /// (work-redistribution), but mesh-declaring requires consensus and
    /// flows through the FSM path on the heartbeat hard backstop.
    ///
    /// The silent peer stays alive in the roster and may re-prove itself
    /// with a fresh keepalive; its workers stay registered for future
    /// dispatch (a brief network blip that resolves before the hard
    /// backstop does not cost the cluster a respawn). The FSM is seeded
    /// with the local scheduling-suspect set so the operator-visible FSM
    /// state mirrors reality — even though [`Self::consensus_escalate`]
    /// is NOT called here (escalation is the hard-backstop's job).
    async fn maybe_requeue_silent_held_work(&mut self) {
        if !self.only_silent_held_work_remains() {
            return;
        }
        let dead = self.silent_held_dead_declarations();
        let suspect_set: std::collections::BTreeSet<String> =
            dead.iter().map(|d| d.secondary_id.clone()).collect();
        // Local-only requeue: TaskRequeued mutations + supplanted_holders
        // fence + TasksAdded re-nudge, NO PeerRemoved / TimeoutDetected /
        // worker drop / roster clear (the silent peer stays in
        // `self.secondaries` and `self.workers`).
        self.requeue_silent_held_work_locally(&suspect_set).await.ok();
        // Reflect the local scheduling-suspect in the FSM (no escalate).
        // A subsequent hard-backstop sweep on the same peers will be the
        // one to call `consensus_escalate` and run the round.
        self.set_consensus_scheduling_suspect(suspect_set);
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
        // Belt to the emit chokepoint's suspenders: every recorded
        // outcome implies the dispatch freeze, including paths that
        // record directly (the phase-floor liveness check) without an
        // emit. Idempotent.
        self.run_fail_dispatch_freeze = true;
        if self.worker_mgmt_fail_outcome.is_some() {
            return;
        }
        tracing::warn!(error = %outcome, "worker management: run should fail");
        self.worker_mgmt_fail_outcome = Some(outcome);
    }

    /// THE run-fail emit chokepoint: SYNCHRONOUSLY latch the dispatch
    /// freeze, then put the signal on the decoupled worker-management
    /// bus. Every `RunShouldFail` / `PolicyFatalExit` emission routes
    /// through here — the bus drain (which records the typed break
    /// outcome and drives the clean shutdown) stays asynchronous per
    /// the decoupling law, but the freeze is effective the moment this
    /// returns: the dispatch-view pipeline's step-0 seam
    /// (`dispatch_view_for_worker`) reads the latch exactly like the
    /// graceful-abort freeze, so no dispatch path can assign work in
    /// the emit→break window. Production smell this closes
    /// (run_20260611_005220): an `on_phase_end` raise emitted
    /// `PolicyFatalExit`, the cascade marked the phase done, and 6
    /// next-phase tasks were assigned before the parked
    /// worker-management arm consumed the signal.
    pub(crate) fn emit_run_fail_signal(&mut self, signal: WorkerMgmtSignal) {
        debug_assert!(
            matches!(
                signal,
                WorkerMgmtSignal::RunShouldFail { .. } | WorkerMgmtSignal::PolicyFatalExit { .. }
            ),
            "emit_run_fail_signal is the FAIL-class chokepoint; other \
             signals go through emit_worker_mgmt directly"
        );
        self.run_fail_dispatch_freeze = true;
        self.cluster_state.emit_worker_mgmt(signal);
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
