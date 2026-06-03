//! Types exposed by the `job_manager` module: the public `JobStatus` /
//! `JobStatusInfo` snapshot returned by `get_job_status`, the
//! `SlurmJobManager` struct definition (fields only — its impl block
//! lives in [`manager`](super::manager)), and the `SlurmError` enum
//! that all manager methods return.

use dynrunner_gateway::traits::{Gateway, GatewayError};

use crate::config::SlurmConfig;
use crate::packaging::PackagingError;

/// Status of a SLURM job (parsed from the raw squeue state string).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
    Unknown(String),
}

/// Full snapshot returned by `get_job_status`.
///
/// `state`/`state_kind` are `None` when squeue had no record for the
/// job (transient query failure or post-purge). The Python wrapper
/// exposes that as `state="UNKNOWN"` to mirror the historical
/// `SlurmJobManager.get_job_status` shape; Rust callers that need the
/// "no longer in queue → presumed completed" interpretation should
/// layer it themselves rather than have it baked in here, because
/// the squeue purge horizon and "actually completed" are not the
/// same thing on every cluster.
#[derive(Debug, Clone)]
pub struct JobStatusInfo {
    /// Raw squeue state string (e.g. "RUNNING", "PENDING"). `None` if
    /// the job had no row in squeue's output.
    pub state: Option<String>,
    /// Parsed `JobStatus` for Rust callers. `None` mirrors `state`.
    pub state_kind: Option<JobStatus>,
    /// Node assignment from squeue (`%N`); empty when unknown.
    pub node: String,
    /// Reason field from squeue (`%r`); empty when unknown.
    pub reason: String,
}

/// Manages SLURM job submission and lifecycle via a `Gateway`.
///
/// The `gateway` and `job_ids` fields are `pub(super)` so the impl
/// block in [`manager`](super::manager) can mutate them while still
/// being invisible to consumers of this module.
pub struct SlurmJobManager<G: Gateway> {
    pub config: SlurmConfig,
    pub(super) gateway: G,
    pub(super) job_ids: Vec<String>,
    /// Remote (gateway-side) absolute path of the uploaded
    /// `dynrunner-slurm-shutdown` binary, or `None` until
    /// [`SlurmJobManager::upload_shutdown_manager_binary_from`] runs
    /// successfully. Populated once during preparation; subsequent
    /// wrapper-script renders (initial cohort + respawn) read it via
    /// [`SlurmJobManager::shutdown_manager_remote_path`] so the
    /// uploaded binary is referenced by the same path every secondary
    /// the run produces uses.
    pub(super) shutdown_manager_remote_path: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum SlurmError {
    #[error("gateway error: {0}")]
    Gateway(#[from] GatewayError),
    #[error("command error: {0}")]
    Command(String),
    #[error("packaging error: {0}")]
    Packaging(#[from] PackagingError),
    /// Local-source path supplied to
    /// [`SlurmJobManager::upload_shutdown_manager_binary_from`] did
    /// not exist on the dispatcher filesystem. The upload step
    /// surfaces this as a hard error rather than silently skipping:
    /// the caller already decided this is the binary to deploy, and
    /// the only failure modes are misconfiguration (wrong path) or a
    /// build that did not produce the expected output (broken
    /// framework wheel). Both deserve loud surfacing at dispatch
    /// time, not a silent "orphan cleanup disabled" warning that
    /// surfaces only after the first stuck container.
    #[error("shutdown-manager source binary not found: {0}")]
    ShutdownBinaryNotFound(std::path::PathBuf),
}
