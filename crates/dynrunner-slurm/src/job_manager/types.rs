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

/// What `cancel_job`'s `scancel` actually did, for callers that need
/// to pick a log severity (e.g. the respawn revocation path treats a
/// gone job as a quiet no-op). Distinct from the `Err` arm, which is
/// reserved for the gateway transport failing — scancel never ran at
/// all there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelOutcome {
    /// scancel exited 0 — the job was cancelled (or was already in a
    /// cancelling state scancel accepts silently).
    Cancelled,
    /// scancel ran but reported an error — on a reachable gateway this
    /// means the job id is no longer known to the controller (already
    /// finished, already cancelled, or purged).
    AlreadyGone,
}

/// Bounded poll budget for [`SlurmJobManager::cancel_all_jobs`]'s
/// post-`scancel` verification sweep.
///
/// A bare `scancel` is fire-and-forget: it exits 0 even when the job
/// then stays RUNNING because the cancel raced a PENDING→RUNNING
/// transition or the gateway round-trip partially failed (asm-dataset
/// run_20260611_182745: 3 of 4 jobs cancelled, secondary-2/155629 was
/// still RUNNING 4+ minutes later and had to be scancelled by hand). So
/// after issuing the scancel set, `cancel_all_jobs` re-queries squeue
/// for survivors and re-issues scancel on them, up to `attempts` times
/// with `poll_delay` between rounds.
///
/// FAIL-SAFE by construction: the budget is bounded, so verification
/// can never turn a clean abort into a hang. Any job still present after
/// the budget is exhausted is surfaced with a loud WARN carrying the job
/// id (the operator needs the id to scancel by hand) — the sweep then
/// returns, it does not block.
#[derive(Debug, Clone, Copy)]
pub struct CancelVerifyPolicy {
    /// Total squeue re-query rounds AFTER the initial scancel pass. Each
    /// round re-scancels any survivor before the next poll. `0` disables
    /// verification entirely (legacy fire-and-forget shape).
    pub attempts: u32,
    /// Delay between verification rounds. Tests pass a near-zero value
    /// to keep the bounded loop off the wall clock.
    pub poll_delay: std::time::Duration,
}

impl Default for CancelVerifyPolicy {
    /// 3 verification rounds, 10s apart — a ~30s budget over which a
    /// genuinely-stuck scancel is re-issued and any final survivor is
    /// WARN-flagged. Comfortably covers a PENDING→RUNNING race (which
    /// settles in seconds) without stalling a clean teardown: every
    /// already-gone job clears on the FIRST squeue poll, so the typical
    /// path costs one squeue round-trip and returns immediately.
    fn default() -> Self {
        Self {
            attempts: 3,
            poll_delay: std::time::Duration::from_secs(10),
        }
    }
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
    /// Remote (gateway-side) absolute path of the uploaded
    /// `dynrunner-slurm-wrapper` binary, or `None` until
    /// [`SlurmJobManager::upload_wrapper_binary_from`] runs
    /// successfully. Populated once during preparation; subsequent
    /// wrapper-script renders (initial cohort + respawn) read it via
    /// [`SlurmJobManager::wrapper_bin_remote_path`] so every per-job
    /// stub `exec`s the binary at the same path. Mirrors
    /// `shutdown_manager_remote_path`.
    pub(super) wrapper_bin_remote_path: Option<String>,
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
    /// Local-source path supplied to
    /// [`SlurmJobManager::upload_wrapper_binary_from`] did not exist
    /// on the dispatcher filesystem. Hard error for the same reasons
    /// as [`SlurmError::ShutdownBinaryNotFound`]: the SLURM dispatch
    /// path always renders the wrapper stub against this binary, so a
    /// missing source is misconfiguration or a broken framework wheel,
    /// not a benign skip.
    #[error("wrapper source binary not found: {0}")]
    WrapperBinaryNotFound(std::path::PathBuf),
    /// The post-upload freshness verification in
    /// [`SlurmJobManager::upload_binary_hash_conditional`] found the
    /// gateway copy's SHA-256 diverging from the local source's right
    /// after a transfer (truncated/corrupted transfer, or an
    /// out-of-band clobber racing the upload). Hard error: every job
    /// in the run would `exec` the staged binary, so wrong bytes at
    /// the staging path must fail dispatch loudly — a stale wrapper
    /// fleet is far costlier than an aborted submit.
    #[error(
        "staged binary failed post-upload hash verification at {remote}: \
         local sha256 {local_hash}, remote sha256 {remote_hash:?}"
    )]
    StagedBinaryHashMismatch {
        remote: String,
        local_hash: String,
        remote_hash: Option<String>,
    },
}
