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

use dynrunner_core::{ErrorType, Identifier, PhaseId, ResourceKind, SoftPreferredSecondaries, TaskInfo};
use dynrunner_protocol_primary_secondary::PeerTransport;
use dynrunner_scheduler_api::{PendingPool, ResourceEstimator, Scheduler};
use tokio::sync::mpsc as tokio_mpsc;

use crate::primary::command_channel::PrimaryCommand;
use crate::primary::wire::compute_task_hash;
use crate::primary::{PrimaryConfig, PrimaryCoordinator};
use crate::worker_signal::WorkerMgmtSignal;

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

    /// Per-bucket budget from the live-primary's config.
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

/// Pure retry-bucket primitive shared between the live-primary and
/// the promoted-secondary's primary path. Owns ONLY the three
/// load-bearing semantics:
///   1. Empty `candidates` returns `false` without touching the
///      counter (an empty bucket pass is not a "used" pass).
///   2. Exhausted budget returns `false` and leaves the failed
///      entries intact (the caller will fire `on_phase_end` and
///      let the run's outcome summary surface them).
///   3. Available budget + at least one candidate: remove each
///      candidate from the caller's failed-store via
///      `on_remove_from_failed`, `pool.reinject(binary)`, bump the
///      counter BEFORE the caller's kickstart so a kickstart-side
///      error does not burn a second pass.
///
/// Caller responsibilities:
///   * Build `candidates` from its own failed-store (the primary
///     walks `all_binaries` + `failed_tasks`; the secondary walks
///     `primary_failed` directly because each entry carries the
///     binary).
///   * Drive the post-reinject kickstart of idle workers (the two
///     paths have different worker-fan-out helpers).
///
/// Returns `true` iff at least one binary was reinjected — the
/// caller uses this to skip `on_phase_end` + `mark_phase_done` for
/// this phase (the pool just flipped `Drained → Active` via
/// `PendingPool::reinject` and the next `poll_drain_transitions`
/// will be empty for this phase until the freshly-active items
/// terminate).
pub(crate) fn try_phase_retry_bucket_core<I: Identifier>(
    phase: &PhaseId,
    kind: BucketKind,
    candidates: Vec<TaskInfo<I>>,
    pool: &mut PendingPool<I>,
    retry_passes_used: &mut RetryPassesUsed,
    max_passes: u32,
    mut on_remove_from_failed: impl FnMut(&str),
) -> bool {
    if candidates.is_empty() {
        // No failures of this kind for this phase. Caller moves
        // on. We intentionally do NOT touch the counter here:
        // an empty bucket pass is not a "used" pass — a future
        // re-arrival of a failure (e.g. the cascade triggered
        // by an `apply_fail_permanent` cross-cut) should still
        // get a fresh budget if the counter was at 0.
        return false;
    }

    let key = (phase.clone(), kind);
    let used = retry_passes_used.get(&key).copied().unwrap_or(0);
    if used >= max_passes {
        // Budget exhausted. Surviving failures stay in the
        // caller's failed-store; caller fires `on_phase_end` and
        // the phase advances. The fail_* count in the run's
        // outcome summary surfaces these to the operator.
        tracing::debug!(
            phase = %phase,
            bucket = ?kind,
            used,
            cap = max_passes,
            pending_failures = candidates.len(),
            "per-phase retry bucket: budget exhausted; failures permanent"
        );
        return false;
    }

    let count = candidates.len();
    for binary in candidates {
        let h = compute_task_hash(&binary);
        on_remove_from_failed(&h);
        pool.reinject(binary);
    }

    // Bump counter BEFORE the caller's kickstart so a kickstart-
    // side error path leaving the system in an inconsistent state
    // does not burn a second pass on the same set of failures.
    retry_passes_used.insert(key, used + 1);

    // Phase-transition important event: the start of a retry pass.
    // ONE emit site shared by both retry channels — `bucket = ?kind`
    // discriminates error-retry (`Recoverable`) from OOM-retry (`Oom`),
    // never a per-kind branch. Emitted at the importance target so the
    // dual-sink surfaces it on stdio under `--important-stdio-only`;
    // only the budget-available reinject path (this point) is the
    // "start of a retry" — the empty / budget-exhausted returns above
    // are not.
    tracing::info!(
        target: crate::primary::important_events::IMPORTANT_TARGET,
        phase = %phase,
        bucket = ?kind,
        pass = used + 1,
        cap = max_passes,
        count,
        "per-phase retry bucket: re-injecting failed tasks"
    );

    true
}

impl<Tr, S, E, I> PrimaryCoordinator<Tr, S, E, I>
where
    Tr: PeerTransport<I>,
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
        // OOM-bucket dispatch shape. `single_worker_mode` lives on
        // `PrimaryCoordinator`, so this is the only machine that drives
        // it — there is no parallel secondary-side retry mirror in the
        // unified model (a promoted node runs its co-located primary,
        // which is THIS machine). Entry-side: flip the coordinator into
        // single-worker mode for the duration of the bucket so the
        // dispatch pipeline masks workers != local-id-0 and promotes
        // `preferred_secondaries` to a strict filter. Exit-side: every
        // `Ok(false)` return below clears the flag. `Ok(true)` keeps it
        // set — the operational loop will re-enter and the next drain
        // edge re-runs this bucket. See `single_worker_mode` field doc
        // on `PrimaryCoordinator` for the user-spec rationale
        // (2026-05-17). No-op for the Recoverable bucket.
        if matches!(kind, BucketKind::Oom) {
            self.single_worker_mode = true;
        }

        // Build candidates from `all_binaries` (the run-start snapshot)
        // cross-referenced against `failed_tasks` (the hash-keyed
        // ErrorType ledger). On a parked primary that activated via the
        // seeded resume, `all_binaries` is empty (the pool was hydrated
        // from the CRDT, not a run-start binary list); `failed_tasks` is
        // seeded from the restored ledger, and the candidate set is
        // built from the hydrated pool's view. Both paths share the core
        // via `try_phase_retry_bucket_core`.
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

        // OOM bucket: bind each retry to a specific secondary BEFORE
        // handing to the core so the dispatch pipeline's strict
        // `preferred_secondaries` gate routes each task to its pinned
        // node. Pairing: tasks sorted by estimated memory DESC,
        // secondaries sorted by advertised memory DESC, zipped
        // cyclically (biggest task → biggest secondary). Snapshotted at
        // entry — a secondary dying mid-bucket fails dispatch
        // naturally; the next bucket entry re-samples. No-op on
        // Recoverable / empty.
        let candidates = self.assign_oom_preferred_secondaries(kind, candidates);

        let cap = kind.max_passes(&self.config);
        let reinjected = {
            let failed_tasks = &mut self.failed_tasks;
            try_phase_retry_bucket_core(
                phase,
                kind,
                candidates,
                self.pending.as_mut().expect("pool must be initialised"),
                &mut self.retry_passes_used,
                cap,
                |h| {
                    failed_tasks.remove(h);
                },
            )
        };
        if reinjected {
            // Reinjection is a pool-entry edge: the workers won't
            // request a new task on their own (they already sent their
            // last `TaskRequest` which got `nothing-to-do` because the
            // failure hadn't been reinjected yet). EMIT a `TasksAdded`
            // onto the decoupled worker-management bus rather than
            // calling dispatch directly (the dispatch-decoupling law);
            // the operational loop's worker-management arm coalesces it
            // into one batched recheck. Without this emit the reinjected
            // binaries sit in the pool forever (the negative control
            // test pins it load-bearing).
            self.cluster_state
                .emit_worker_mgmt(WorkerMgmtSignal::TasksAdded);
        } else if matches!(kind, BucketKind::Oom) {
            // No reinjection happened — either empty candidates or
            // budget exhausted. Lift the single-worker dispatch-shape
            // gate so subsequent phases' normal-pass dispatch (and
            // this phase's permanent failures' downstream accounting)
            // run unmasked.
            self.single_worker_mode = false;
        }
        Ok(reinjected)
    }

    /// OOM-bucket dispatch-shape preprocessor: sort retries by
    /// estimated memory DESC and bind each to a secondary cycling
    /// through the cluster's memory-DESC order. Pure transformation
    /// — modifies `preferred_secondaries` on each `TaskInfo<I>` and
    /// returns the rebound vector in the new dispatch order.
    ///
    /// No-op for non-OOM kinds (returns the input unchanged) and
    /// for an empty cluster (no secondaries means no rebinding;
    /// dispatch will fail naturally at the next worker iteration,
    /// matching the snapshot-at-entry semantics).
    ///
    /// Single concern: per-task target binding for the OOM bucket.
    /// The strict `preferred_secondaries` gate in the dispatch
    /// pipeline reads what this method writes; neither side learns
    /// the other's internals.
    fn assign_oom_preferred_secondaries(
        &self,
        kind: BucketKind,
        candidates: Vec<TaskInfo<I>>,
    ) -> Vec<TaskInfo<I>> {
        if !matches!(kind, BucketKind::Oom) {
            return candidates;
        }
        let mem_kind = ResourceKind::memory();
        let secondaries = self.secondaries_sorted_by_memory_desc();
        if secondaries.is_empty() {
            return candidates;
        }
        // Sort tasks by estimated memory DESC so the biggest task
        // pairs with the biggest secondary. The estimator is the
        // only authority on per-task resource cost; reusing it
        // keeps the OOM dispatch shape consistent with the
        // scheduler's normal-pass assignment math.
        let mut tasks = candidates;
        tasks.sort_by(|a, b| {
            let mem_a = self.estimator.estimate(a).get(&mem_kind);
            let mem_b = self.estimator.estimate(b).get(&mem_kind);
            mem_b.cmp(&mem_a)
        });
        for (i, task) in tasks.iter_mut().enumerate() {
            let target = secondaries[i % secondaries.len()].clone();
            task.preferred_secondaries = SoftPreferredSecondaries::new(vec![target]);
        }
        tasks
    }
}

#[cfg(test)]
mod important_event_tests {
    //! Pins the phase-transition "start of retry" important event on
    //! the shared retry-bucket emit site: it fires exactly once on the
    //! budget-available reinject, never on the empty-candidate or
    //! budget-exhausted paths, and the SINGLE site discriminates
    //! error-retry (`Recoverable`) from OOM-retry (`Oom`) purely via
    //! the `bucket` field — no per-kind branch.

    use std::collections::HashMap;

    use dynrunner_core::{PhaseId, RunnerIdentifier, SoftPreferredSecondaries, TaskInfo, TypeId};
    use dynrunner_scheduler_api::PendingPool;
    use tracing::subscriber::with_default;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::{Layer, Registry};

    use super::{try_phase_retry_bucket_core, BucketKind, RetryPassesUsed};
    use crate::test_capture::{important_only, ImportantCapture};

    fn task(name: &str, phase: &PhaseId) -> TaskInfo<RunnerIdentifier> {
        TaskInfo {
            path: std::path::PathBuf::from(format!("/tmp/{name}")),
            size: 1,
            identifier: RunnerIdentifier::from(name),
            phase_id: phase.clone(),
            type_id: TypeId::from("default"),
            affinity_id: None,
            payload: serde_json::Value::Null,
            task_id: name.into(),
            task_depends_on: vec![],
            preferred_secondaries: SoftPreferredSecondaries::default(),
            resolved_path: None,
        }
    }

    fn pool(phase: &PhaseId) -> PendingPool<RunnerIdentifier> {
        PendingPool::new([phase.clone()], HashMap::new()).expect("single-phase pool")
    }

    /// Drive the core with the given bucket over a capture and return
    /// (reinjected?, captured events). The phase has budget for one pass.
    fn run_with_capture(
        kind: BucketKind,
        candidates: Vec<TaskInfo<RunnerIdentifier>>,
        max_passes: u32,
        used_seed: u32,
    ) -> (bool, Vec<crate::test_capture::CapturedEvent>) {
        let phase = PhaseId::from("phase-a");
        let mut pool = pool(&phase);
        let mut used: RetryPassesUsed = HashMap::new();
        if used_seed > 0 {
            used.insert((phase.clone(), kind), used_seed);
        }
        let capture = ImportantCapture::default();
        let subscriber =
            Registry::default().with(capture.clone().with_filter(important_only()));
        let reinjected = with_default(subscriber, || {
            try_phase_retry_bucket_core(
                &phase,
                kind,
                candidates,
                &mut pool,
                &mut used,
                max_passes,
                |_h| {},
            )
        });
        (reinjected, capture.events())
    }

    #[test]
    fn error_retry_emits_one_event_tagged_recoverable() {
        let phase = PhaseId::from("phase-a");
        let (reinjected, events) =
            run_with_capture(BucketKind::Recoverable, vec![task("t0", &phase)], 1, 0);
        assert!(reinjected);
        assert_eq!(events.len(), 1, "exactly one important event: {events:?}");
        assert!(events[0].message.contains("re-injecting failed tasks"));
        assert_eq!(
            events[0].fields.get("bucket").map(String::as_str),
            Some("Recoverable"),
            "error-retry must be tagged Recoverable: {events:?}"
        );
    }

    #[test]
    fn oom_retry_emits_one_event_tagged_oom() {
        let phase = PhaseId::from("phase-a");
        let (reinjected, events) =
            run_with_capture(BucketKind::Oom, vec![task("t0", &phase)], 1, 0);
        assert!(reinjected);
        assert_eq!(events.len(), 1, "exactly one important event: {events:?}");
        assert_eq!(
            events[0].fields.get("bucket").map(String::as_str),
            Some("Oom"),
            "OOM-retry must be tagged Oom: {events:?}"
        );
    }

    #[test]
    fn empty_candidates_emit_no_event() {
        let (reinjected, events) = run_with_capture(BucketKind::Recoverable, vec![], 1, 0);
        assert!(!reinjected);
        assert!(events.is_empty(), "no important event on empty bucket: {events:?}");
    }

    #[test]
    fn exhausted_budget_emits_no_event() {
        let phase = PhaseId::from("phase-a");
        // Seed the counter at the cap so this pass is over budget.
        let (reinjected, events) =
            run_with_capture(BucketKind::Recoverable, vec![task("t0", &phase)], 1, 1);
        assert!(!reinjected);
        assert!(
            events.is_empty(),
            "no important event when the budget is exhausted: {events:?}"
        );
    }
}
