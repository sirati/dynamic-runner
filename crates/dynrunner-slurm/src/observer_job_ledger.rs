//! SLURM binding of the observer's job-ledger consult port.
//!
//! Single concern: implement
//! [`dynrunner_manager_distributed::observer::JobLedgerProbe`] over the
//! SAME [`SlurmJobManager`] the run submitted its cohort from, so a
//! relocated submitter→observer can consult squeue for the run's job ids
//! and learn whether the whole cluster has left the queue. The consult
//! maps onto the manager's [`SlurmJobManager::any_job_still_queued`] — the
//! read-only `squeue`/`get_job_status` probe over the manager's own
//! tracked `job_ids`.
//!
//! # Why the observer needs this
//!
//! When an entire cluster run dies (all jobs exit, squeue empty), the
//! relocated submitter→observer would otherwise spin on "no reachable
//! peer" forever — even though the SAME process hosts the job ledger that
//! proves the run is over. This binding lets the observer ask that ledger
//! directly instead of presuming from indirect silence evidence.
//!
//! # Boundary
//!
//! `dynrunner-manager-distributed` owns the trait + the "when/who" (the
//! observer consults on a long lost-visibility episode, only when it hosts
//! a ledger); `dynrunner-slurm` owns the "how" (the squeue query over the
//! run's ids). The pyo3 layer wires the production binding onto the
//! submitter primary via `set_job_ledger_probe`, exactly as it wires the
//! tunnel reconnector via `set_tunnel_reconnector`. The role layer never
//! names squeue. The job manager is shared (`Arc<Mutex<…>>`) with the
//! respawn spawner — the SAME ledger that holds the run's `job_ids`.

use std::sync::Arc;

use async_trait::async_trait;
use dynrunner_gateway::traits::Gateway;
use dynrunner_manager_distributed::observer::{
    ClusterTerminalOutcome, JobLedgerProbe, JobLedgerStatus,
};
use tokio::sync::Mutex;

use crate::job_manager::{RunTerminalDisposition, SlurmJobManager};

/// Production binding of [`JobLedgerProbe`] to
/// [`SlurmJobManager::any_job_still_queued`]. Holds the SAME
/// `Arc<Mutex<SlurmJobManager>>` the cohort-setup + respawn paths share,
/// so the consult reads the run's actual `job_ids`.
pub struct SlurmJobLedgerProbe<G: Gateway> {
    job_manager: Arc<Mutex<SlurmJobManager<G>>>,
}

impl<G: Gateway> SlurmJobLedgerProbe<G> {
    /// Construct a job-ledger consult binding over the shared
    /// `SlurmJobManager` — the same handle the respawn spawner is built
    /// from (it tracks every submitted job id).
    pub fn new(job_manager: Arc<Mutex<SlurmJobManager<G>>>) -> Self {
        Self { job_manager }
    }
}

#[async_trait(?Send)]
impl<G> JobLedgerProbe for SlurmJobLedgerProbe<G>
where
    G: Gateway + Send + Sync + 'static,
{
    async fn jobs_still_queued(&self) -> JobLedgerStatus {
        // Read-only consult: the lock is held only across the squeue
        // round-trips. A transport failure surfaces as `Err` →
        // `ProbeFailed` (no information — the observer's double-check
        // resets its empty streak, never declaring a cluster dead on a
        // flaky gateway).
        let guard = self.job_manager.lock().await;
        match guard.any_job_still_queued().await {
            Ok(true) => JobLedgerStatus::Present,
            Ok(false) => JobLedgerStatus::Empty,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "observer job-ledger consult (squeue) failed; treating as a \
                     transient probe failure (no evidence the cluster is gone)"
                );
                JobLedgerStatus::ProbeFailed
            }
        }
    }

    async fn run_terminal_outcome(&self) -> ClusterTerminalOutcome {
        // The AUTHORITATIVE post-departure classification: `sacct` retains
        // each job's final State after it leaves the queue, so it can tell a
        // clean COMPLETED framework shutdown from a crash/scancel/OOM — the
        // distinction `jobs_still_queued`'s `squeue` probe collapses. The
        // manager owns the SLURM-vocabulary fold; this binding maps its
        // 3-way disposition onto the role-layer's `ClusterTerminalOutcome`.
        // The same lock the queue consult uses, held only across the `sacct`
        // round-trips.
        let guard = self.job_manager.lock().await;
        match guard.run_terminal_disposition().await {
            RunTerminalDisposition::AllCompleted => ClusterTerminalOutcome::Completed,
            RunTerminalDisposition::AnyFailed => ClusterTerminalOutcome::Failed,
            RunTerminalDisposition::Indeterminate => ClusterTerminalOutcome::Indeterminate,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SlurmConfig;
    use dynrunner_gateway::traits::{CommandResult, GatewayError};
    use std::path::Path;

    /// A gateway that answers every `squeue -j <id>` with one canned
    /// squeue body (so the whole cohort reads the same state), or fails the
    /// transport entirely (the probe-failure path). `squeue_body = None`
    /// fails `execute_command`; `Some(String::new())` is the empty-queue
    /// (no row) shape; `Some("RUNNING|…")` is a still-queued job.
    struct CannedSqueueGateway {
        squeue_body: Option<String>,
    }

    impl Gateway for CannedSqueueGateway {
        async fn connect(&mut self) -> Result<(), GatewayError> {
            Ok(())
        }
        async fn disconnect(&mut self) -> Result<(), GatewayError> {
            Ok(())
        }
        async fn execute_command(
            &self,
            _cmd: &str,
            _cwd: Option<&str>,
        ) -> Result<CommandResult, GatewayError> {
            match &self.squeue_body {
                Some(body) => Ok(CommandResult {
                    return_code: if body.is_empty() { 1 } else { 0 },
                    stdout: body.clone(),
                    stderr: String::new(),
                }),
                None => Err(GatewayError::CommandFailed("transport down".to_string())),
            }
        }
        async fn transfer_file(&self, _local: &Path, _remote: &str) -> Result<(), GatewayError> {
            Ok(())
        }
        async fn download_file(&self, _remote: &str, _local: &Path) -> Result<(), GatewayError> {
            Ok(())
        }
        async fn create_directory(&self, _remote: &str) -> Result<(), GatewayError> {
            Ok(())
        }
        async fn file_exists(&self, _remote: &str) -> Result<bool, GatewayError> {
            Ok(false)
        }
        fn setup_port_forwarding(&mut self, _l: u16, _r: u16) -> Result<(), GatewayError> {
            Ok(())
        }
    }

    fn manager_with(
        ids: &[&str],
        squeue_body: Option<String>,
    ) -> Arc<Mutex<SlurmJobManager<CannedSqueueGateway>>> {
        let mut jm = SlurmJobManager::new(SlurmConfig::default(), CannedSqueueGateway { squeue_body });
        jm.seed_job_ids_for_test(ids);
        Arc::new(Mutex::new(jm))
    }

    /// Every job gone (no squeue row) → `Empty` — the cluster-empty ground
    /// truth the verdict needs.
    #[tokio::test(flavor = "current_thread")]
    async fn empty_queue_maps_to_empty() {
        let probe = SlurmJobLedgerProbe::new(manager_with(&["111", "222"], Some(String::new())));
        assert_eq!(probe.jobs_still_queued().await, JobLedgerStatus::Empty);
    }

    /// A still-RUNNING job → `Present` (the run is live).
    #[tokio::test(flavor = "current_thread")]
    async fn running_job_maps_to_present() {
        let probe = SlurmJobLedgerProbe::new(manager_with(
            &["111", "222"],
            Some("RUNNING|node01|None".to_string()),
        ));
        assert_eq!(probe.jobs_still_queued().await, JobLedgerStatus::Present);
    }

    /// A gateway transport failure → `ProbeFailed` (no information — never
    /// mistaken for a gone cluster).
    #[tokio::test(flavor = "current_thread")]
    async fn transport_failure_maps_to_probe_failed() {
        let probe = SlurmJobLedgerProbe::new(manager_with(&["111"], None));
        assert_eq!(probe.jobs_still_queued().await, JobLedgerStatus::ProbeFailed);
    }

    // ── Authoritative terminal-outcome consult (`sacct`) — the #532
    //    cluster-gone disambiguation that keeps a clean completion from
    //    false-reporting FAILED ──

    /// A gateway that answers `sacct -j <id> … -o State` with a
    /// PER-JOB-ID State (keyed by the id in the command), so a cohort can
    /// mix clean + failed jobs, or fails the transport entirely. Any
    /// non-`sacct` command (the squeue path) answers benign-empty — these
    /// tests drive only the terminal-outcome consult.
    struct CannedSacctGateway {
        /// `job_id → State token` (e.g. "COMPLETED", "FAILED",
        /// "CANCELLED by 7"). An id absent from the map answers an empty
        /// `sacct` body (no accounting row → unreadable for that id).
        states: std::collections::HashMap<String, String>,
        /// When true, EVERY `sacct` command fails the transport (the
        /// accounting-unreadable path).
        fail_sacct: bool,
    }

    impl Gateway for CannedSacctGateway {
        async fn connect(&mut self) -> Result<(), GatewayError> {
            Ok(())
        }
        async fn disconnect(&mut self) -> Result<(), GatewayError> {
            Ok(())
        }
        async fn execute_command(
            &self,
            cmd: &str,
            _cwd: Option<&str>,
        ) -> Result<CommandResult, GatewayError> {
            if cmd.contains("sacct") {
                if self.fail_sacct {
                    return Err(GatewayError::CommandFailed("sacct transport down".into()));
                }
                // Find which job id this `sacct -j <id> …` is for.
                let state = self
                    .states
                    .iter()
                    .find(|(id, _)| cmd.contains(format!("-j {id} ").as_str()))
                    .map(|(_, s)| s.clone());
                return Ok(CommandResult {
                    // `sacct` exits 0 with an empty body for an unknown id.
                    return_code: 0,
                    stdout: state.unwrap_or_default(),
                    stderr: String::new(),
                });
            }
            // Non-sacct (squeue) path: benign empty (no row).
            Ok(CommandResult {
                return_code: 1,
                stdout: String::new(),
                stderr: String::new(),
            })
        }
        async fn transfer_file(&self, _local: &Path, _remote: &str) -> Result<(), GatewayError> {
            Ok(())
        }
        async fn download_file(&self, _remote: &str, _local: &Path) -> Result<(), GatewayError> {
            Ok(())
        }
        async fn create_directory(&self, _remote: &str) -> Result<(), GatewayError> {
            Ok(())
        }
        async fn file_exists(&self, _remote: &str) -> Result<bool, GatewayError> {
            Ok(false)
        }
        fn setup_port_forwarding(&mut self, _l: u16, _r: u16) -> Result<(), GatewayError> {
            Ok(())
        }
    }

    fn manager_with_sacct(
        ids: &[&str],
        states: &[(&str, &str)],
        fail_sacct: bool,
    ) -> Arc<Mutex<SlurmJobManager<CannedSacctGateway>>> {
        let states = states
            .iter()
            .map(|(id, s)| ((*id).to_string(), (*s).to_string()))
            .collect();
        let mut jm = SlurmJobManager::new(
            SlurmConfig::default(),
            CannedSacctGateway { states, fail_sacct },
        );
        jm.seed_job_ids_for_test(ids);
        Arc::new(Mutex::new(jm))
    }

    /// EVERY job COMPLETED (exit 0) → `Completed` — the clean-framework-
    /// shutdown disposition that keeps the observer from false-FAILing a
    /// cleanly-finished run (#532).
    #[tokio::test(flavor = "current_thread")]
    async fn all_completed_maps_to_completed() {
        let probe = SlurmJobLedgerProbe::new(manager_with_sacct(
            &["111", "222"],
            &[("111", "COMPLETED"), ("222", "COMPLETED")],
            false,
        ));
        assert_eq!(
            probe.run_terminal_outcome().await,
            ClusterTerminalOutcome::Completed
        );
    }

    /// One job FAILED (the rest COMPLETED) → `Failed` — a real failure is
    /// failure-dominant; the genuine-failure path is preserved.
    #[tokio::test(flavor = "current_thread")]
    async fn one_failed_maps_to_failed() {
        let probe = SlurmJobLedgerProbe::new(manager_with_sacct(
            &["111", "222"],
            &[("111", "COMPLETED"), ("222", "FAILED")],
            false,
        ));
        assert_eq!(
            probe.run_terminal_outcome().await,
            ClusterTerminalOutcome::Failed
        );
    }

    /// A CANCELLED job (scancel) → `Failed`. SLURM prints `CANCELLED by
    /// <uid>`; only the leading word is matched.
    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_maps_to_failed() {
        let probe = SlurmJobLedgerProbe::new(manager_with_sacct(
            &["111"],
            &[("111", "CANCELLED by 1000")],
            false,
        ));
        assert_eq!(
            probe.run_terminal_outcome().await,
            ClusterTerminalOutcome::Failed
        );
    }

    /// An OOM job → `Failed` (`OUT_OF_MEMORY`).
    #[tokio::test(flavor = "current_thread")]
    async fn oom_maps_to_failed() {
        let probe = SlurmJobLedgerProbe::new(manager_with_sacct(
            &["111"],
            &[("111", "OUT_OF_MEMORY")],
            false,
        ));
        assert_eq!(
            probe.run_terminal_outcome().await,
            ClusterTerminalOutcome::Failed
        );
    }

    /// `sacct` transport failure → `Indeterminate` (not positive evidence of
    /// a clean completion; the caller keeps its conservative FAILED verdict).
    #[tokio::test(flavor = "current_thread")]
    async fn sacct_unreadable_maps_to_indeterminate() {
        let probe = SlurmJobLedgerProbe::new(manager_with_sacct(&["111"], &[], true));
        assert_eq!(
            probe.run_terminal_outcome().await,
            ClusterTerminalOutcome::Indeterminate
        );
    }

    /// A COMPLETED job alongside one with NO accounting row (empty `sacct`
    /// body — purged/disabled) → `Indeterminate`, NOT Completed: the
    /// optimistic disposition requires EVERY job positively read clean.
    #[tokio::test(flavor = "current_thread")]
    async fn partial_completed_with_missing_row_is_indeterminate() {
        let probe = SlurmJobLedgerProbe::new(manager_with_sacct(
            &["111", "222"],
            // 222 has no row → unreadable for that id.
            &[("111", "COMPLETED")],
            false,
        ));
        assert_eq!(
            probe.run_terminal_outcome().await,
            ClusterTerminalOutcome::Indeterminate
        );
    }
}
