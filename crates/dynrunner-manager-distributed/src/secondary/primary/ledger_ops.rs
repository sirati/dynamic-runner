//! Primary-side in-flight ledger queries and retry orchestration.
//!
//! Single concern: maintain `primary_in_flight` / `primary_pending`
//! invariants as items complete or fail, and drive the
//! drain-check-and-retry sweep that re-injects Recoverable failures
//! into the pool when no live work remains. Pool retries are gated
//! by the ledger here (callers say "an item finished" / "an item
//! failed"; this module owns the bookkeeping side effects).

use dynrunner_core::{Identifier, MessageReceiver, MessageSender, TaskInfo};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{DistributedMessage, PeerTransport};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use super::super::SecondaryCoordinator;
use super::cascade_drain_done;

impl<PT, P, M, S, E, I> SecondaryCoordinator<PT, P, M, S, E, I>
where
    PT: MessageSender<DistributedMessage<I>> + MessageReceiver<DistributedMessage<I>>,
    P: PeerTransport<I>,
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    /// Test/inspection helper: number of queued items in the pool.
    /// Returns 0 if the pool isn't initialised yet.
    pub(in crate::secondary) fn primary_pending_len(&self) -> usize {
        self.primary_pending.as_ref().map(|p| p.len()).unwrap_or(0)
    }

    /// Record completion of an item the primary previously
    /// dispatched (via `handle_primary_task_request`). Decrements the
    /// pool's in-flight counter for that item's phase, then promotes
    /// any newly-`Drained` phase to `Done` so dependents can become
    /// `Active`. No-op if the hash wasn't dispatched by this node — a
    /// peer-completion the primary never issued belongs to a
    /// different in-flight ledger and is silently ignored.
    ///
    /// Mirrors `process_phase_lifecycle` on the local primary side: a
    /// single `mark_phase_done` may flip a `Blocked` dependent phase
    /// to `Active`, and that newly-active phase may itself be empty
    /// (dependency chain `0 → 1 → 2 → 3` with all items in phase 3,
    /// or any phase whose only item just completed with no follow-up
    /// items). Loop until no phase is `Drained` and call
    /// `drain_empty_active_phases` each iteration so the cascade
    /// continues all the way to the next populated phase. Without
    /// this loop the primary would stop one phase short and
    /// the next phase's items would sit in the pool with the phase
    /// still `Blocked`.
    pub(in crate::secondary) fn note_primary_item_completed(&mut self, file_hash: &str) {
        let (phase_id, task_id) = match self.primary_in_flight.remove(file_hash) {
            Some(item) => (item.phase_id, item.binary.task_id),
            None => return,
        };
        // Symmetric with retry: a successful completion supersedes any
        // earlier Recoverable failure recorded against the same hash.
        // Without this, a task that fails Recoverably (lands in
        // `primary_failed`) and then succeeds on a subsequent
        // attempt mid-pass — possible when the operational loop re-
        // dispatches before our drain-check fires — would still
        // trigger a pointless retry pass. Mirrors the live primary's
        // `failed_tasks.remove` in `handle_task_complete`.
        self.primary_failed.remove(file_hash);
        if let Some(pool) = self.primary_pending.as_mut() {
            pool.on_item_finished(&phase_id, task_id.as_deref());
            cascade_drain_done(pool);
        }
    }

    /// Sibling to `note_primary_item_completed` for the failure path.
    /// Decrements the pool's in-flight counter for the item's phase
    /// (same as completion — phase machine doesn't distinguish
    /// success vs failure for in-flight bookkeeping). For Recoverable
    /// failures of tasks THIS secondary dispatched as primary
    /// via `handle_primary_task_request`, also stash the binary in
    /// `primary_failed` so `primary_drain_check_and_retry`
    /// can re-inject it after the main pass drains. Non-Recoverable
    /// / OutOfMemory / Unknown failures bypass the ledger — they're
    /// terminal at the worker level and retry would likely fail
    /// again the same way.
    ///
    /// Tasks not in `primary_in_flight` (i.e. not dispatched by this
    /// secondary as primary — e.g. peer-completion forwards
    /// for tasks dispatched elsewhere, or initial-assignment failures
    /// from the live-primary's pre-promotion authority) bypass both
    /// the ledger and the pool decrement: those tasks were never on
    /// this pool's books to begin with. Mirrors `note_primary_item_completed`'s
    /// silent-skip behaviour for unknown hashes.
    ///
    /// Called from every wire-arrival site that observes a TaskFailed
    /// for a primary-dispatched task: peer.rs (peer transport),
    /// processing.rs (own worker event), dispatch.rs (live-primary
    /// forward, no-op for Recoverable). The Recoverable filter is
    /// inside this function so the callers don't have to special-case
    /// the retry path.
    pub(in crate::secondary) fn note_primary_item_failed(
        &mut self,
        file_hash: &str,
        error_type: &dynrunner_core::ErrorType,
    ) {
        let item = match self.primary_in_flight.remove(file_hash) {
            Some(item) => item,
            None => return,
        };
        let phase_id = item.phase_id.clone();
        let task_id = item.binary.task_id.clone();
        if matches!(error_type, dynrunner_core::ErrorType::Recoverable) {
            // Stash for the retry pass. Idempotent — the same hash
            // appearing twice (e.g. after re-injection fails again)
            // overwrites with the same binary and the latest
            // ErrorType, which is harmless. The entry carries
            // `error_type` so the outcome-summary breakdown can
            // partition the ledger by class; today only Recoverable
            // lands here (retry-pass scope), but the structure is
            // ready when non-Recoverable accounting joins.
            self.primary_failed.insert(
                file_hash.to_string(),
                crate::secondary::FailedTaskEntry {
                    binary: item.binary,
                    error_type: error_type.clone(),
                },
            );
        }
        if let Some(pool) = self.primary_pending.as_mut() {
            pool.on_item_finished(&phase_id, task_id.as_deref());
            cascade_drain_done(pool);
        }
    }

    /// Primary-side equivalent of the local primary's
    /// `run_retry_passes`. Called once per keepalive tick from
    /// `process_tasks`. When the main pass has drained for THIS
    /// primary's view (pool empty, no items in flight, no local
    /// active tasks) AND there are Recoverable failures pending in
    /// `primary_failed` AND the retry budget hasn't been
    /// exhausted, take a snapshot of the failed binaries, clear the
    /// ledger, re-inject each into `primary_pending` via
    /// `pool.reinject`, bump the pass counter, and kick our own idle
    /// workers via `repoll_idle_workers` so the operational loop
    /// re-engages with the just-injected items.
    ///
    /// Why "drain-check" rather than a phase-explicit "main pass" /
    /// "retry pass" boundary: the primary's `process_tasks` loop
    /// has no notion of pass boundaries — it's a single select! that
    /// runs until shutdown. The drain-check fires whenever the loop
    /// is observably idle and there's leftover retry work, which is
    /// the same trigger condition the local primary's two-phase
    /// `operational_loop` → `run_retry_passes` design used (drain →
    /// re-inject → re-run). Repeated firing is gated by the budget
    /// counter: once `retry_passes_used == retry_max_passes`, the
    /// next drain-check leaves `primary_failed` populated and
    /// the run wraps up via the normal exit conditions.
    ///
    /// Peer secondaries' workers don't need a kickstart from here:
    /// they re-poll on their own keepalive tick via
    /// `repoll_idle_workers` (with backoff) so any peer worker that
    /// got "no work" before the re-injection will see new work on its
    /// next request. Only the primary's own workers need an
    /// immediate kick — they're the ones whose `request_task_for_worker`
    /// short-circuits through `handle_primary_task_request` directly.
    pub(in crate::secondary) async fn primary_drain_check_and_retry(&mut self) {
        if !self.is_primary {
            return;
        }
        if self.primary_failed.is_empty() {
            return;
        }
        if !self.primary_pending_is_empty()
            || !self.primary_in_flight.is_empty()
            || !self.active_tasks.is_empty()
        {
            return;
        }
        if !self.primary_retry_budget.should_retry() {
            // Budget exhausted on either axis (attempt-count cap OR
            // SLURM-wallclock deadline minus safety margin): the
            // residual entries are permanent failures. Keep them in
            // the ledger so test fixtures (and future operator-
            // visible probes) can count permanent failures from the
            // primary's perspective; the log-spam guard is
            // `exhaustion_warning_emitted` so we emit the warning
            // once per run rather than every drain check.
            if !self.exhaustion_warning_emitted {
                // Tasks in `primary_failed` at retry-exhaustion time
                // are now terminal (no further passes). Surface them
                // as `fail_final` so the operator's log-side
                // breakdown matches the actual disposition; the
                // class-of-error these tasks hit was Recoverable
                // (only Recoverable lands in primary_failed today),
                // but the run-level outcome class is "final" because
                // the retry policy gave up.
                //
                // `passes` reflects the legacy attempt-count cap; if
                // the cliff was the SLURM-wallclock side, the actual
                // attempts-used will be < retry_max_passes — operators
                // chasing "why didn't I get all my retries?" should
                // cross-reference `$SLURM_JOB_END_TIME` and the run
                // duration. Single log shape kept stable for
                // back-compat with the existing operator dashboards.
                tracing::warn!(
                    fail_final = self.primary_failed.len(),
                    passes = self.config.retry_max_passes,
                    "primary retry budget exhausted; failed tasks are permanent"
                );
                self.exhaustion_warning_emitted = true;
            }
            return;
        }

        // Drain the failed-ledger: each entry yields its `binary`
        // for re-injection into `primary_pending`. The `error_type`
        // recorded on the entry is intentionally discarded here —
        // the next pass will overwrite with whatever outcome the
        // retry produces.
        let to_retry: Vec<TaskInfo<I>> =
            std::mem::take(&mut self.primary_failed)
                .into_values()
                .map(|entry| entry.binary)
                .collect();
        let pass = self.primary_retry_budget.attempts_used() + 1;
        tracing::info!(
            pass,
            count = to_retry.len(),
            "primary retry pass: re-injecting failed tasks"
        );
        if let Some(pool) = self.primary_pending.as_mut() {
            for binary in to_retry {
                pool.reinject(binary);
            }
        }
        self.primary_retry_budget.record_attempt();

        // Kick our own idle workers — see method-level doc. Peer
        // workers self-recover on their next keepalive-driven repoll.
        self.repoll_idle_workers().await;
    }

    /// Test/inspection helper: whether the pool has zero queued items.
    /// Treats "no pool yet" as empty so resource-loop predicates don't
    /// have to special-case the pre-snapshot state.
    pub(in crate::secondary) fn primary_pending_is_empty(&self) -> bool {
        self.primary_pending
            .as_ref()
            .map(|p| p.is_empty())
            .unwrap_or(true)
    }
}
