//! Per-phase retry-bucket primitive.
//!
//! Single concern: at each phase's drain edge, decide whether any of
//! that phase's failed tasks should be re-injected for one more
//! attempt, and which bucket they belong to.
//!
//! Module boundary:
//!   * Owns: the [`BucketKind`] enum (which `ErrorType`s belong to
//!     which retry channel), the per-(phase, bucket) pass counter
//!     stored on [`PrimaryCoordinator::retry_passes_used`], the
//!     reinjection driver [`PrimaryCoordinator::try_run_phase_retry_bucket`].
//!   * Does NOT own: the cascade itself (lives in
//!     `coordinator::process_phase_lifecycle`), the per-task
//!     dispatch decisions (live in `lifecycle::dispatch_to_idle_workers`
//!     and `task::request::handle_task_request`), or the `failed_tasks`
//!     ledger insertion (lives in `task::failed::handle_task_failed`).
//!
//! Callers see a single primitive: `try_run_phase_retry_bucket(phase,
//! kind, command_rx) -> bool`. The Boolean answers "did we reinject
//! anything?"; on `true` the caller skips `on_phase_end` +
//! `mark_phase_done` because the phase is now Active again (reinject
//! flips Drained → Active per `PendingPool::reinject`). On `false` —
//! either no failures of this kind for this phase OR the per-phase
//! budget is exhausted — the caller falls through to the next bucket
//! or to the phase-end fire-site.
//!
//! Why per-(phase, kind) instead of per-phase: the user spec
//! (2026-05-17) wants Recoverable and OOM retries to consume
//! independent budgets so a workload that wants fail-fast OOM
//! response (`oom_retry_max_passes = 0`) keeps its transient-error
//! retries, or vice versa. Per-phase keying is the load-bearing
//! invariant: phase B's retries don't run until phase A is fully
//! done (every retry-bucket exhausted), matching the user's "next
//! phase depends on previous phase being done" framing.

use std::collections::HashMap;

use dynrunner_core::{ErrorType, Identifier, PhaseId, ResourceKind, TaskInfo};
use dynrunner_protocol_primary_secondary::{PeerTransport, SecondaryTransport};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};
use tokio::sync::mpsc as tokio_mpsc;

use crate::primary::command_channel::PrimaryCommand;
use crate::primary::wire::compute_task_hash;
use crate::primary::{PrimaryConfig, PrimaryCoordinator};

/// Which retry channel a `failed_tasks` entry belongs to.
///
/// `Recoverable` covers `ErrorType::Recoverable` only — every
/// transient failure (worker pipe wedged, no-fault preempt that
/// somehow surfaced through the regular failed path, etc.) gets the
/// historical `retry_max_passes` budget.
///
/// `Oom` covers `ErrorType::ResourceExhausted(memory)` only — actual
/// over-budget kills + kernel-OOM upgrades. Non-memory
/// `ResourceExhausted` (e.g. gpu_vram) and `NonRecoverable` /
/// `Unfulfillable` stay in `failed_tasks` permanently; they are NOT
/// the retry-bucket primitive's concern.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum BucketKind {
    Recoverable,
    Oom,
}

impl BucketKind {
    /// Does this bucket's predicate accept `et`?
    pub(crate) fn matches(self, et: &ErrorType) -> bool {
        match self {
            BucketKind::Recoverable => matches!(et, ErrorType::Recoverable),
            BucketKind::Oom => match et {
                ErrorType::ResourceExhausted(kind) => *kind == ResourceKind::memory(),
                _ => false,
            },
        }
    }

    /// Per-bucket budget from the coordinator config.
    pub(crate) fn max_passes(self, config: &PrimaryConfig) -> u32 {
        match self {
            BucketKind::Recoverable => config.retry_max_passes,
            BucketKind::Oom => config.oom_retry_max_passes,
        }
    }
}

/// Per-(phase, bucket) pass counter. Initialised empty; entries are
/// inserted at the moment a bucket runs for the first time on a
/// given phase. Lookups fall back to 0.
///
/// Lifetime: tied to the coordinator. Survives across multiple
/// `process_phase_lifecycle` cascades within a single `run()`; reset
/// implicitly when a fresh `PrimaryCoordinator` is constructed for a
/// new run. No explicit clear is required between phases — the key
/// includes `PhaseId`, so phase A's counter is structurally
/// independent of phase B's.
pub(crate) type RetryPassesUsed = HashMap<(PhaseId, BucketKind), u32>;

impl<T, P, S, E, I> PrimaryCoordinator<T, P, S, E, I>
where
    T: SecondaryTransport<I>,
    P: PeerTransport<I>,
    S: Scheduler<I>,
    E: ResourceEstimator<I>,
    I: Identifier,
{
    /// Try to reinject this phase's failures of the requested kind
    /// for one more pass.
    ///
    /// Returns `Ok(true)` iff at least one task was reinjected. The
    /// caller — `process_phase_lifecycle` — uses the Boolean to
    /// decide whether to fire `on_phase_end` + `mark_phase_done`
    /// (false) or to defer them until the freshly-Active phase
    /// drains again (true). Per [`super::PendingPool::reinject`],
    /// a reinjected item flips the phase from `Drained → Active`
    /// and cancels the pending drained notification, so the next
    /// `poll_drain_transitions` will only return this phase again
    /// after the new items terminate.
    ///
    /// Three return paths:
    /// 1. No failures of `kind` for `phase` → `Ok(false)`. Caller
    ///    falls through to the next bucket or `on_phase_end`.
    /// 2. Budget exhausted (`retry_passes_used[(phase, kind)] >=
    ///    kind.max_passes(config)`) → `Ok(false)`. Surviving
    ///    failures stay in `failed_tasks` permanently;
    ///    `on_phase_end` fires next, and the run's final accounting
    ///    counts them under the relevant `fail_*` class.
    /// 3. Budget available AND failures present → reinject every
    ///    matching binary, bump the counter, kickstart dispatch,
    ///    return `Ok(true)`.
    ///
    /// `command_rx` is threaded down so the `dispatch_to_idle_workers`
    /// kickstart's call sites that recursively process commands
    /// (e.g. `apply_fail_permanent` re-entering through the cascade)
    /// keep their drain-pending-commands chokepoint. Parking the
    /// argument matches the `9427d0b` pattern the consumer's
    /// lazy-spawn relies on.
    pub(crate) async fn try_run_phase_retry_bucket(
        &mut self,
        phase: &PhaseId,
        kind: BucketKind,
        _command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
    ) -> Result<bool, String> {
        // 1. Filter binaries by (phase, kind). Walk `all_binaries`
        //    (the run-start snapshot) and consult `failed_tasks` for
        //    each hash. `all_binaries` is the only source of truth
        //    for the `TaskInfo<I>` payload — `failed_tasks` is keyed
        //    by hash and carries the latest `ErrorType` only.
        let candidates: Vec<TaskInfo<I>> = self
            .all_binaries
            .iter()
            .filter(|b| b.phase_id == *phase)
            .filter(|b| {
                let h = compute_task_hash(*b);
                self.failed_tasks
                    .get(&h)
                    .is_some_and(|et| kind.matches(et))
            })
            .cloned()
            .collect();
        if candidates.is_empty() {
            // No failures of this kind for this phase. Caller moves
            // on. We intentionally do NOT touch the counter here:
            // an empty bucket pass is not a "used" pass — a future
            // re-arrival of a failure (e.g. the cascade triggered
            // by an `apply_fail_permanent` cross-cut) should still
            // get a fresh budget if the counter was at 0.
            return Ok(false);
        }

        // 2. Per-(phase, kind) counter.
        let key = (phase.clone(), kind);
        let used = self.retry_passes_used.get(&key).copied().unwrap_or(0);
        let cap = kind.max_passes(&self.config);
        if used >= cap {
            // Budget exhausted. Surviving failures stay in
            // `failed_tasks`; caller fires `on_phase_end` and the
            // phase advances. The fail_* count in the run's outcome
            // summary surfaces these to the operator.
            tracing::debug!(
                phase = %phase,
                bucket = ?kind,
                used,
                cap,
                pending_failures = candidates.len(),
                "per-phase retry bucket: budget exhausted; failures permanent"
            );
            return Ok(false);
        }

        // 3. Reinject. `pool.reinject` flips Drained → Active for
        //    this phase and drops any pending drained-notification,
        //    so `process_phase_lifecycle`'s next
        //    `poll_drain_transitions` returns an empty list and the
        //    cascade exits. Control returns to the operational loop
        //    which dispatches the freshly-reinjected items.
        let count = candidates.len();
        for binary in candidates {
            let h = compute_task_hash(&binary);
            self.failed_tasks.remove(&h);
            self.pool_mut().reinject(binary);
        }

        // 4. Bump counter BEFORE the kickstart so a kickstart-side
        //    error path leaving us in an inconsistent state doesn't
        //    burn a second pass on the same set of failures.
        self.retry_passes_used.insert(key, used + 1);

        tracing::info!(
            phase = %phase,
            bucket = ?kind,
            pass = used + 1,
            cap,
            count,
            "per-phase retry bucket: re-injecting failed tasks"
        );

        // 5. Kickstart dispatch: the workers won't request a new
        //    task on their own (they already sent their last
        //    `TaskRequest` which got `nothing-to-do` because the
        //    failure hadn't been reinjected yet). Same rationale as
        //    the legacy `run_retry_passes` body — without the
        //    kickstart, reinjected binaries sit in the pool forever.
        self.dispatch_to_idle_workers().await?;

        Ok(true)
    }
}
