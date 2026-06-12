//! The observer's job-ledger consult port.
//!
//! # Single concern
//!
//! ONE concern: the seam the zero-authority observer crosses to ask
//! "are any of THIS run's jobs still in the cluster's queue?" WITHOUT
//! owning the job-manager / squeue / SLURM machinery. The relocated
//! submitter→observer process physically HOSTS the job ledger (the
//! `SlurmJobManager` it submitted the cohort from, with the run's job
//! ids + the squeue/sacct gateway seam). When that observer loses
//! transport visibility for a long time, the lost-visibility
//! report-and-retry machinery alone cannot tell an ssh blip from a run
//! that is OVER — but the job ledger CAN: a run whose every job has left
//! the queue is done, full stop. This port lets the observer consult
//! that ground truth.
//!
//! # Why a port (and not the `SecondarySpawner` / `TunnelReconnector`)
//!
//! The respawn [`crate::primary::respawn::SecondarySpawner`] EXECUTES
//! spawn/revoke decisions; the [`crate::observer::reconnect::TunnelReconnector`]
//! REBUILDS a dropped `-R` tunnel. Neither answers "is the run's cluster
//! still queued" — a DISTINCT concern (a read-only ledger consult, no
//! authority, no mutation). Folding it onto either would conflate two
//! domains in one trait. So this is its own single-concern port, wired
//! exactly the way the other two are: the trait lives here in
//! `manager-distributed`, the SLURM binding lives in `dynrunner-slurm`
//! (it wraps the SAME `Arc<Mutex<SlurmJobManager>>` the respawn spawner
//! shares and queries `get_job_status` over the run's `job_ids`), and the
//! pyo3 layer wires the production binding onto the submitter primary so
//! it rides the [`crate::observer::ObserverHandoff`] across the
//! primary→observer demotion. The role layer never names squeue.
//!
//! # Boundary
//!
//! `dynrunner-manager-distributed` owns the trait + the "when/who" (the
//! observer consults on the lost-visibility wake-loss cadence, only when
//! it hosts a ledger); `dynrunner-slurm` owns the "how" (the squeue
//! query). A cold-join desktop observer (a late-joiner console) submitted
//! NO jobs and hosts no ledger — it holds [`None`] here and keeps the
//! never-terminal report-and-retry behaviour, because it cannot teardown
//! a cluster it did not submit.

use std::sync::Arc;

/// The ground-truth verdict of one job-ledger consult.
///
/// Distinguishes the THREE outcomes the observer must treat differently:
/// jobs still present (the run is live), every job gone (the run is over),
/// and a transient probe failure (the gateway round-trip itself failed —
/// NOT evidence of anything). The defensive double-check in
/// [`super::cluster_gone::ClusterGoneDetector`] consumes this: only
/// [`Self::Empty`] on TWO consecutive consults renders the terminal
/// verdict; a [`Self::ProbeFailed`] resets the streak (a flaky squeue
/// must never kill a live run).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobLedgerStatus {
    /// At least one of the run's jobs is still PENDING/RUNNING in the
    /// queue (or in an unrecognised transient state the cluster still
    /// tracks). The run is live — keep observing.
    Present,
    /// Zero of the run's jobs are still in the queue — every one has left
    /// (COMPLETED / CANCELLED / FAILED / purged). Positive evidence the
    /// cluster is GONE. (One consult of this is not yet a verdict — the
    /// detector requires two consecutive.)
    Empty,
    /// The consult could not be completed (the gateway transport failed,
    /// or the ledger was momentarily unreadable). NOT evidence either way
    /// — treated as "no new information" by the detector, which resets the
    /// empty streak so a flaky probe cannot accumulate toward a verdict.
    ProbeFailed,
}

/// Port the observer crosses to consult the job ledger it hosts.
///
/// `#[async_trait(?Send)]` because the production binding drives a squeue
/// query over a gateway whose future is not `Send` (the same provider
/// physics that forces the `?Send` bound on
/// [`crate::observer::reconnect::TunnelReconnector`] and
/// [`crate::primary::respawn::SecondarySpawner`]). The observer run loop is
/// `LocalSet`-bound for exactly this reason. The trait object stays
/// `Send + Sync` so an `Arc<dyn JobLedgerProbe>` is moveable across the
/// handoff.
///
/// Single concern: "are any of the run's jobs still queued?". The
/// implementation owns HOW (query squeue over the run's ids); the observer
/// owns only WHEN (a long lost-visibility episode) and WHAT-NEXT (the
/// double-checked terminal verdict). Read-only by contract — a consult
/// never cancels, submits, or mutates anything.
#[async_trait::async_trait(?Send)]
pub trait JobLedgerProbe: Send + Sync {
    /// Consult the hosted ledger: are any of the run's jobs still in the
    /// queue? A best-effort read; a transport failure surfaces as
    /// [`JobLedgerStatus::ProbeFailed`] rather than a panic/error, so the
    /// observer's defensive double-check treats it as "no information".
    async fn jobs_still_queued(&self) -> JobLedgerStatus;
}

/// A job-ledger probe handle the observer holds. [`None`] on a path that
/// hosts no ledger (the cold-join desktop observer — it submitted no jobs
/// and cannot teardown a cluster it did not launch); [`Some`] on the
/// relocated submitter→observer path, which physically holds the
/// `SlurmJobManager` it submitted the cohort from.
pub type JobLedgerProbeHandle = Option<Arc<dyn JobLedgerProbe>>;
