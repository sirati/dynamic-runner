//! Cluster-empty terminal verdict for an observer that hosts the job
//! ledger.
//!
//! # Single concern
//!
//! ONE concern: from a sequence of job-ledger consults
//! ([`super::job_ledger::JobLedgerStatus`]), derive the verdict "every one
//! of the run's jobs has left the queue — the cluster is GONE" — with a
//! defensive double-check so a single flaky/empty consult can never kill a
//! live run.
//!
//! # Why (the run_20260612_043357 aftermath)
//!
//! When an entire cluster run died (all SLURM jobs exited, squeue empty),
//! the relocated submitter→observer logged "no reachable peer" every
//! ~10 minutes for 3.5+ hours with no verdict and no teardown — even
//! though the SAME process hosts the job ledger that PROVES the run is
//! over. The never-terminal report-and-retry machinery
//! ([`super::lost_visibility`]) is correct for a transport blip, and the
//! [`super::fleet_death`] presumption is the right LAST resort when the
//! observer has only indirect evidence (silence + zero legs). But when the
//! observer can read the job ledger directly, it should not have to
//! PRESUME — it can KNOW. This detector turns "every job left the queue"
//! into a bounded terminal verdict.
//!
//! # The defensive double-check (one WARN-interval apart)
//!
//! A TRANSIENT squeue failure, or an empty-but-jobs-pending-resubmission
//! window, must not be mistaken for a dead run. So the verdict requires
//! [`JobLedgerStatus::Empty`] on TWO CONSECUTIVE consults. The coordinator
//! drives one consult per wake-loss cadence emit (the
//! [`super::lost_visibility::WAKE_LOSS_RECURRENCE`] 10-minute repeat, first
//! at the 5-minute [`super::lost_visibility::WAKE_LOSS_THRESHOLD`] mark),
//! so two consecutive empties are ~one WARN-interval apart. Any
//! [`JobLedgerStatus::Present`] (a job is back / still there) OR
//! [`JobLedgerStatus::ProbeFailed`] (no information) RESETS the streak —
//! the detector only ever accumulates a verdict from positive, repeated
//! "the queue is empty" evidence.
//!
//! # Boundary
//!
//! This module owns ONLY the streak-counting derivation. The coordinator
//! owns the inputs (it drives the consult through the
//! [`super::job_ledger::JobLedgerProbe`] port and feeds the result here)
//! and the action (on [`ClusterGoneVerdict::Gone`] it runs the existing
//! cleanup/teardown path + exits non-zero — the SAME `FatalPolicyExit`
//! the fleet-death presumption uses). No squeue, no exit, no clock here.

use super::job_ledger::JobLedgerStatus;

/// How many CONSECUTIVE empty-queue consults must land before the cluster
/// is declared gone. Two: the defensive double-check (one WARN-interval
/// apart) against a transient squeue failure or a brief
/// empty-but-resubmitting window.
pub(crate) const REQUIRED_CONSECUTIVE_EMPTY: u32 = 2;

/// What [`ClusterGoneDetector::observe`] tells the coordinator after one
/// job-ledger consult.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ClusterGoneVerdict {
    /// Not enough evidence yet — keep observing. Either a job is still
    /// queued, the probe failed (no information), or only one empty
    /// consult has landed so far (the double-check is not satisfied).
    KeepWatching,
    /// Every one of the run's jobs has left the queue on
    /// [`REQUIRED_CONSECUTIVE_EMPTY`] consecutive consults — the cluster
    /// is gone. `reason` is the operator-facing verdict line for the
    /// coordinator's terminal-reason emit + the non-zero exit.
    Gone { reason: String },
}

/// Streak-tracking state machine. Single writer (the observer run loop,
/// `LocalSet`-bound) — no synchronisation. It owns ONLY the
/// consecutive-empty derivation; the coordinator owns the consult + the
/// exit action.
#[derive(Debug, Default)]
pub(crate) struct ClusterGoneDetector {
    /// Consecutive [`JobLedgerStatus::Empty`] consults observed so far.
    /// Reset to 0 by any `Present` or `ProbeFailed` consult.
    consecutive_empty: u32,
}

impl ClusterGoneDetector {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Feed one job-ledger consult result; learn the verdict.
    ///
    /// `Empty` advances the streak; reaching [`REQUIRED_CONSECUTIVE_EMPTY`]
    /// renders [`ClusterGoneVerdict::Gone`]. `Present` / `ProbeFailed`
    /// reset the streak to 0 (a live or unverifiable consult is not
    /// evidence the run is over). The `last_known_run_state` describes what
    /// the observer last saw of the run (e.g. the last phase narrated, or
    /// that no terminal converged) for the verdict line — the coordinator
    /// supplies it from its converged CRDT so the operator learns what the
    /// run was doing when its cluster vanished.
    pub(crate) fn observe(
        &mut self,
        status: JobLedgerStatus,
        last_known_run_state: &str,
    ) -> ClusterGoneVerdict {
        match status {
            JobLedgerStatus::Present | JobLedgerStatus::ProbeFailed => {
                self.consecutive_empty = 0;
                ClusterGoneVerdict::KeepWatching
            }
            JobLedgerStatus::Empty => {
                self.consecutive_empty = self.consecutive_empty.saturating_add(1);
                if self.consecutive_empty < REQUIRED_CONSECUTIVE_EMPTY {
                    return ClusterGoneVerdict::KeepWatching;
                }
                ClusterGoneVerdict::Gone {
                    reason: format!(
                        "all of the run's jobs have left the cluster queue on {} \
                         consecutive consults (one wake-loss interval apart) — the \
                         cluster is GONE. Last known run state: {last_known_run_state}. \
                         The run did not report a completion verdict of its own, so the \
                         observer treats it as FAILED and tears down (the submitter \
                         process that hosts the job ledger cannot keep spinning on a \
                         cluster it can prove is over).",
                        self.consecutive_empty,
                    ),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The production replay (run_20260612_043357 shape at test timescale):
    /// the cluster is empty on two consecutive consults → the cluster-gone
    /// verdict with the distinct wording, never an endless `KeepWatching`.
    #[test]
    fn two_consecutive_empty_consults_render_cluster_gone() {
        let mut d = ClusterGoneDetector::new();
        assert_eq!(
            d.observe(JobLedgerStatus::Empty, "phase 'tokenize' in progress"),
            ClusterGoneVerdict::KeepWatching,
            "one empty consult alone is not a verdict (the double-check)"
        );
        match d.observe(JobLedgerStatus::Empty, "phase 'tokenize' in progress") {
            ClusterGoneVerdict::Gone { reason } => {
                assert!(reason.contains("GONE"), "distinct wording: {reason}");
                assert!(
                    reason.contains("phase 'tokenize' in progress"),
                    "carries the last known run state: {reason}"
                );
                assert!(
                    reason.contains("FAILED"),
                    "names the failed treatment: {reason}"
                );
            }
            other => panic!("two consecutive empties must render Gone; got {other:?}"),
        }
    }

    /// The defensive double-check: one empty consult, then a job reappears
    /// (`Present`) — the streak resets, so no verdict. This is the
    /// empty-but-jobs-pending-resubmission window the brief guards against.
    #[test]
    fn empty_then_present_does_not_render_a_verdict() {
        let mut d = ClusterGoneDetector::new();
        assert_eq!(
            d.observe(JobLedgerStatus::Empty, "no terminal observed"),
            ClusterGoneVerdict::KeepWatching
        );
        assert_eq!(
            d.observe(JobLedgerStatus::Present, "no terminal observed"),
            ClusterGoneVerdict::KeepWatching,
            "a job reappearing resets the empty streak"
        );
        // A subsequent single empty is back to streak 1 — still no verdict.
        assert_eq!(
            d.observe(JobLedgerStatus::Empty, "no terminal observed"),
            ClusterGoneVerdict::KeepWatching,
            "the streak restarted; one empty after a reset is not a verdict"
        );
    }

    /// A TRANSIENT probe failure between two empties resets the streak — a
    /// flaky squeue must never accumulate toward a verdict.
    #[test]
    fn probe_failure_resets_the_empty_streak() {
        let mut d = ClusterGoneDetector::new();
        assert_eq!(
            d.observe(JobLedgerStatus::Empty, "no terminal observed"),
            ClusterGoneVerdict::KeepWatching
        );
        assert_eq!(
            d.observe(JobLedgerStatus::ProbeFailed, "no terminal observed"),
            ClusterGoneVerdict::KeepWatching,
            "a probe failure is no information — it resets the streak"
        );
        assert_eq!(
            d.observe(JobLedgerStatus::Empty, "no terminal observed"),
            ClusterGoneVerdict::KeepWatching,
            "back to streak 1 after the reset — not a verdict"
        );
        // Only two genuinely-consecutive empties now render Gone.
        assert!(matches!(
            d.observe(JobLedgerStatus::Empty, "no terminal observed"),
            ClusterGoneVerdict::Gone { .. }
        ));
    }
}
