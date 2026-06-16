//! [`TerminalOutcomeCounts`] — the authoritative per-class outcome
//! partition the primary FINALIZES at run end and carries ON its terminal
//! verdict mutation.
//!
//! Single concern: the data shape of the finalized outcome counts. The
//! authoritative primary stamps it on the run-terminal verdict
//! (`RunComplete` / `RunAborted`) at the instant it DECIDES the verdict, so
//! the latch and the counts converge to every replica ATOMICALLY (one
//! mutation). The narrator — on the primary AND on a zero-authority
//! observer — reads the carried counts back rather than re-folding its own
//! (possibly unconverged) ledger mirror: a verdict observed implies its
//! counts are in hand, with no separate per-task convergence to wait on.
//!
//! Lives in `dynrunner-core` so BOTH the protocol crate (which owns the
//! `ClusterMutation` carrying it on the wire) and the manager crate (which
//! folds its live ledger into it via `From<OutcomeSummary>` and reads it
//! back for narration) share the one definition. The bucket SEMANTICS — what
//! each class means and how the live ledger maps onto it — stay with the
//! manager crate's `OutcomeSummary` / `outcome_counts`; this is only the
//! wire-carried shape.

use serde::{Deserialize, Serialize};

/// The primary's FINALIZED per-class outcome partition, carried on the
/// terminal-verdict mutation so every replica narrates the SAME
/// authoritative counts the primary decided the verdict from.
///
/// Bucket meanings mirror the manager crate's `OutcomeSummary` one-for-one
/// (`succeeded` = worker work that completed; `fail_retry` / `fail_oom` /
/// `fail_final` = the failure-class partition; `skipped` = discovery-time
/// already-done terminals; `setup_succeeded` = succeeded setup-kind tasks).
/// `u64` for wire stability (the manager-side counts are `usize`; widening to
/// `u64` on the wire avoids a platform-dependent width).
///
/// `Default` is all-zero — the honest partition for a PRE-DISPATCH abort
/// (e.g. a bring-up / pre-phase-duplicate `RunAborted` broadcast before any
/// task ran), so a verdict that fires before work exists carries the
/// truthful zero rather than a placeholder.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize,
)]
pub struct TerminalOutcomeCounts {
    pub succeeded: u64,
    pub fail_retry: u64,
    pub fail_oom: u64,
    pub fail_final: u64,
    pub skipped: u64,
    pub setup_succeeded: u64,
}
