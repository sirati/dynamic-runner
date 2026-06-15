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

/// The AUTHORITATIVE terminal disposition of a run whose jobs have all
/// left the queue — the disambiguation `JobLedgerStatus::Empty` cannot
/// carry.
///
/// # Why this is needed (the run_20260613 66k false-FAIL)
///
/// `jobs_still_queued` rides `squeue`, which only ever reports
/// PENDING/RUNNING; once every job leaves the queue it returns nothing
/// REGARDLESS of WHY they left — a clean COMPLETED framework shutdown
/// (every job exit-0) and a crash/scancel/OOM both collapse to
/// [`JobLedgerStatus::Empty`]. So "the cluster is gone" alone is
/// AMBIGUOUS. A relocated submitter→observer whose verdict leg dropped at
/// the instant of a clean completion (it never received `RunComplete`)
/// would then conservatively treat the gone-cluster as FAILED and exit 1
/// — a clean 66k run surfacing as a non-zero failure, breaking exit-code
/// automation. This outcome — read from `sacct`'s retained terminal
/// State, which survives the job leaving the queue — distinguishes the
/// two so the observer reports the run's ACTUAL disposition.
///
/// Consulted ONLY after the [`super::cluster_gone::ClusterGoneDetector`]'s
/// two-consecutive-empty double-check has already concluded the cluster
/// is gone — the authoritative classification is the more expensive probe
/// (`sacct` over the whole cohort), so it is reserved for the moment a
/// verdict is actually about to be authored, never run on the cheap
/// per-cadence streak probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterTerminalOutcome {
    /// EVERY one of the run's jobs reached a clean terminal (`sacct`
    /// State `COMPLETED`, exit 0) — a clean framework shutdown. The run
    /// reached its terminal; the missing `RunComplete` verdict was lost to
    /// the dropped leg, not absent because the run failed. Report the run
    /// as COMPLETED (exit 0), NOT failed.
    Completed,
    /// At least one job reached a FAILURE terminal (`FAILED` / `CANCELLED`
    /// / `TIMEOUT` / `NODE_FAIL` / `OUT_OF_MEMORY` / a non-zero exit) — a
    /// real failure (crash / scancel / OOM). Keep the FAILED verdict.
    Failed,
    /// The authoritative state could not be read (the `sacct` consult
    /// failed at the gateway, or accounting returned nothing for the
    /// cohort — e.g. accounting purged or disabled). NOT positive
    /// evidence of a clean completion, so the caller keeps the
    /// conservative FAILED verdict (a clean completion is asserted ONLY on
    /// a positive [`Self::Completed`] reading — the genuine-failure path
    /// must never be turned optimistic by an unreadable probe).
    Indeterminate,
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

    /// Read the AUTHORITATIVE terminal disposition of the run's jobs from
    /// SLURM accounting (`sacct`), which retains each job's final State
    /// after it leaves the queue. Consulted ONLY once the
    /// two-consecutive-empty double-check has concluded the cluster is
    /// gone, to disambiguate gone-because-COMPLETED (clean framework
    /// shutdown) from gone-because-FAILED (crash / scancel / OOM) — the
    /// distinction `jobs_still_queued` collapses (see
    /// [`ClusterTerminalOutcome`]).
    ///
    /// A best-effort read: an unreadable consult (gateway failure, or
    /// accounting that returned nothing) surfaces as
    /// [`ClusterTerminalOutcome::Indeterminate`] rather than a
    /// panic/error, so the caller keeps the conservative FAILED verdict —
    /// the optimistic COMPLETED disposition is asserted ONLY on a positive
    /// reading that EVERY job completed cleanly.
    async fn run_terminal_outcome(&self) -> ClusterTerminalOutcome;
}

/// A job-ledger probe handle the observer holds. [`None`] on a path that
/// hosts no ledger (the cold-join desktop observer — it submitted no jobs
/// and cannot teardown a cluster it did not launch); [`Some`] on the
/// relocated submitter→observer path, which physically holds the
/// `SlurmJobManager` it submitted the cohort from.
pub type JobLedgerProbeHandle = Option<Arc<dyn JobLedgerProbe>>;
