//! Incremental tally of the per-outcome terminal partition (#…) — the
//! O(1)-read maintained twin of the O(ledger) [`super::accessors`]
//! `outcome_counts()` double-walk.
//!
//! # Single concern
//!
//! "Keep a running [`OutcomeSummary`] of the LOGICAL terminal ledger so
//! [`ClusterState::outcome_counts`](super::ClusterState::outcome_counts)
//! reads in O(1), not O(ledger)." Nothing else: this module holds the SINGLE
//! state→bucket classification (so the maintained tally and the `#[cfg(test)]`
//! full-walk oracle cannot drift) and the running [`OutcomeSummary`] the
//! classification folds into.
//!
//! # Why incremental, not a per-call walk
//!
//! `outcome_counts()` is called once per worker-task completion (the
//! primary's per-completion log line, plus the secondary / observer /
//! narrator readers). Each call walked the ENTIRE fat `tasks` map AND the
//! settled index to re-partition every terminal — O(ledger). At the 46k-
//! affine scale that is 46k completions × O(46k) = O(N²), the build-phase
//! wall whose iter-rate decays with N and freezes. So the partition is
//! maintained INCREMENTALLY: each task-state mutation DECREMENTS the old
//! state's bucket (if terminal) and INCREMENTS the new state's bucket (if
//! terminal), and the read is a cheap struct copy.
//!
//! # The classification — ONE source of truth
//!
//! [`outcome_bucket_of`] maps a live [`TaskState`] to its
//! [`OutcomeBucket`], or `None` for a NON-terminal state (Pending / InFlight
//! / Blocked — uncounted). It is the SOLE
//! mapping: the incremental tally folds through it at the write seam, and the
//! `#[cfg(test)]` oracle ([`ClusterState::outcome_counts_by_scan`](super::ClusterState::outcome_counts_by_scan))
//! folds through it (plus [`settled_bucket_of`] for the settled half) so the
//! two CANNOT drift. The `Failed { kind }` kind→bucket split routes through
//! the one shared [`bucket_for_failed_kind`], the same split
//! `accessors::fold_failed_kind` uses.
//!
//! # Logical-ledger scope (fat ∪ settled) — spill invariance
//!
//! The tally tracks the LOGICAL terminal ledger (fat in-memory `tasks` ∪
//! spilled `settled`), the same universe `outcome_counts()` partitioned. A
//! terminal task is counted ONCE, at the [`set_task_state`](super::ClusterState::set_task_state)
//! transition that made it terminal. Spilling (fat→settled, `commit_spill`)
//! and rehydrating (settled→fat, `unsettle_if_dominated`) move the fat body
//! WITHOUT routing through `set_task_state` — they touch `self.tasks`
//! directly — so the tally is NOT touched on a spill, and the already-counted
//! terminal STAYS counted while settled (its outcome class is unchanged by
//! the move). The tally is therefore spill-NEUTRAL by construction, exactly
//! as the spill is `tasks_hash`-neutral (the term moves between halves) and
//! the `range_fold_memo` / `blocked_by` indices are spill-neutral. A
//! subsequent dominating merge that REHYDRATES then routes the change through
//! `set_task_state`, which reads the now-fat old state and swaps buckets
//! correctly.

use dynrunner_core::ErrorType;

use super::TaskState;
use super::settled::SettledClass;
use super::types::OutcomeSummary;

/// One terminal outcome partition — the single classification target the
/// maintained tally and the oracle both fold through. Mirrors the
/// [`OutcomeSummary`] fields one-for-one; the [`OutcomeTally`] folds it into
/// the matching scalar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OutcomeBucket {
    Succeeded,
    FailRetry,
    FailOom,
    FailFinal,
    Skipped,
    SetupSucceeded,
}

/// The SINGLE `Failed { kind }` → bucket split. `Recoverable` → `FailRetry`,
/// `ResourceExhausted("memory")` → `FailOom`, everything else (incl. the
/// defensively-unreachable `Unfulfillable`/`InvalidTask` kinds a legacy wire
/// path could land inside a `Failed`) → `FailFinal`. The same split
/// `accessors::fold_failed_kind` folds, kept here so the tally and the oracle
/// share one source.
pub(super) fn bucket_for_failed_kind(kind: &ErrorType) -> OutcomeBucket {
    match kind {
        ErrorType::Recoverable => OutcomeBucket::FailRetry,
        ErrorType::ResourceExhausted(k) if k.as_str() == "memory" => OutcomeBucket::FailOom,
        ErrorType::ResourceExhausted(_)
        | ErrorType::NonRecoverable
        | ErrorType::Unfulfillable { .. }
        | ErrorType::InvalidTask { .. } => OutcomeBucket::FailFinal,
    }
}

/// Classify a LIVE [`TaskState`] onto its outcome bucket, or `None` for a
/// NON-terminal state. This is the SOLE fat-state classification both the
/// incremental tally (at the write seam) and the `#[cfg(test)]` oracle fold
/// through. The bucket assignment mirrors the pre-incremental
/// `outcome_counts()` fat arm exactly:
///
/// * `Completed` → `Succeeded`
/// * `Failed { kind }` → [`bucket_for_failed_kind`]
/// * `Unfulfillable` / `InvalidTask` → `FailFinal`
/// * `SkippedAlreadyDone` → `Skipped`
/// * `SetupCompleted` → `SetupSucceeded`
/// * `Pending` / `InFlight` / `Blocked` → `None` (non-terminal, uncounted)
pub(super) fn outcome_bucket_of<I>(state: &TaskState<I>) -> Option<OutcomeBucket> {
    match state {
        TaskState::Completed { .. } => Some(OutcomeBucket::Succeeded),
        TaskState::Failed { kind, .. } => Some(bucket_for_failed_kind(kind)),
        // `Unfulfillable` (reinjectable resource-availability failure) and
        // `InvalidTask` (terminal structural failure) both tally as
        // `FailFinal` — the same mapping the pre-incremental fat arm used.
        TaskState::Unfulfillable { .. } | TaskState::InvalidTask { .. } => {
            Some(OutcomeBucket::FailFinal)
        }
        TaskState::SkippedAlreadyDone { .. } => Some(OutcomeBucket::Skipped),
        TaskState::SetupCompleted { .. } => Some(OutcomeBucket::SetupSucceeded),
        // Non-terminal: contribute to no bucket. A transition INTO one of
        // these from a terminal (a Recoverable Failed reinjected to Pending,
        // a reset) DECREMENTS the prior bucket.
        TaskState::Pending { .. }
        | TaskState::InFlight { .. }
        | TaskState::Blocked { .. } => None,
    }
}

/// Classify a SETTLED entry's [`SettledClass`] onto its outcome bucket. A
/// settled entry is ALWAYS terminal (only terminals settle), so this is total
/// (no `Option`). Folded by the `#[cfg(test)]` full-walk oracle over the
/// settled half AND by the production [`ClusterState::seed_outcome_tally_from_settled`](super::ClusterState::seed_outcome_tally_from_settled)
/// promotion seed (a settled base installed via `install_settled_base` enters
/// the ledger WITHOUT routing through the `set_task_state` write seam, so its
/// terminals must be folded into the tally explicitly there). A live spill
/// (fat→settled) stays tally-neutral — the term was already counted fat — so
/// this is NOT folded at the `commit_spill` seam. `FailedFinal` routes through
/// the SAME [`bucket_for_failed_kind`] split as the fat arm so the kind
/// partition cannot drift across the fat/settled split.
pub(super) fn settled_bucket_of(class: &SettledClass) -> OutcomeBucket {
    match class {
        SettledClass::Completed => OutcomeBucket::Succeeded,
        SettledClass::FailedFinal(kind) => bucket_for_failed_kind(kind),
        SettledClass::InvalidTask => OutcomeBucket::FailFinal,
        SettledClass::SkippedAlreadyDone => OutcomeBucket::Skipped,
        SettledClass::SetupCompleted => OutcomeBucket::SetupSucceeded,
    }
}

/// The maintained running partition. Wraps the public [`OutcomeSummary`] (the
/// returned shape) and folds [`OutcomeBucket`] increments/decrements into its
/// scalars. Kept a distinct node-local type so the read site is the one place
/// the maintained tally and the returned summary cross.
#[derive(Debug, Clone, Default)]
pub(super) struct OutcomeTally {
    summary: OutcomeSummary,
}

impl OutcomeTally {
    /// Borrow the maintained partition as the public [`OutcomeSummary`] — the
    /// O(1) read `outcome_counts()` returns (a cheap `Copy`).
    pub(super) fn summary(&self) -> OutcomeSummary {
        self.summary
    }

    /// A state CHANGED at a fixed key (the [`set_task_state`](super::ClusterState::set_task_state)
    /// write seam): DECREMENT the old state's bucket (if terminal) and
    /// INCREMENT the new state's bucket (if terminal). A logical CREATE
    /// passes `old = None`; a terminal→non-terminal transition (reinject /
    /// reset) passes `new = None` (decrement only); a terminal→different-
    /// terminal adjusts both. One call so a transition can never half-update.
    pub(super) fn swap(&mut self, old: Option<OutcomeBucket>, new: Option<OutcomeBucket>) {
        if let Some(b) = old {
            self.dec(b);
        }
        if let Some(b) = new {
            self.inc(b);
        }
    }

    fn inc(&mut self, bucket: OutcomeBucket) {
        *self.field_mut(bucket) += 1;
    }

    fn dec(&mut self, bucket: OutcomeBucket) {
        let slot = self.field_mut(bucket);
        // A decrement always pairs a prior increment of the SAME bucket (the
        // task was counted into it when it became that terminal), so this
        // never underflows on a coherent ledger; `saturating_sub` keeps a
        // bug from panicking the oploop (the `#[cfg(test)]` oracle catches the
        // drift instead).
        *slot = slot.saturating_sub(1);
    }

    fn field_mut(&mut self, bucket: OutcomeBucket) -> &mut usize {
        match bucket {
            OutcomeBucket::Succeeded => &mut self.summary.succeeded,
            OutcomeBucket::FailRetry => &mut self.summary.fail_retry,
            OutcomeBucket::FailOom => &mut self.summary.fail_oom,
            OutcomeBucket::FailFinal => &mut self.summary.fail_final,
            OutcomeBucket::Skipped => &mut self.summary.skipped,
            OutcomeBucket::SetupSucceeded => &mut self.summary.setup_succeeded,
        }
    }
}
