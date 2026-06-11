//! SLURM job manager: lifecycle of `sbatch`-submitted jobs over a
//! [`Gateway`](dynrunner_gateway::traits::Gateway).
//!
//! Single concern: orchestrate the gateway-side steps that surround a
//! SLURM job submission — image build / transfer (delegated to
//! [`PodmanPackaging`](crate::packaging::PodmanPackaging)), source-
//! binary upload, working-directory preparation, `sbatch` of a wrapper
//! script, cancellation, and `squeue` status snapshots.
//!
//! Layout:
//! - [`types`] — struct definition, status snapshot enum, error enum.
//! - [`manager`] — constructors + accessors on the struct.
//! - [`images`] — image-staging methods (build/transfer images,
//!   upload source binaries) that delegate to [`PodmanPackaging`].
//! - [`lifecycle`] — SLURM-specific methods (prepare directories,
//!   submit / cancel / status).
//! - [`binary_upload`] — shared hash-conditional staging mechanics
//!   for the two musl-static binaries below: skip the transfer when
//!   the gateway already holds a byte-identical copy.
//! - [`shutdown_binary`] — staging primitive for the
//!   `dynrunner-slurm-shutdown` musl-static binary (uploaded to the
//!   gateway alongside the per-job wrapper scripts so out-of-cgroup
//!   orphan-container cleanup survives `/nix/store` not being shared
//!   between dispatcher and compute node).
//! - [`wrapper_binary`] — staging primitive for the
//!   `dynrunner-slurm-wrapper` musl-static binary (uploaded the same
//!   way so the per-job wrapper-script stub can `exec` it to run the
//!   full secondary lifecycle in place of the legacy inline bash).
//! - [`tests`] — module-internal tests.

mod binary_upload;
mod images;
mod lifecycle;
mod manager;
mod shutdown_binary;
#[cfg(test)]
mod tests;
mod types;
mod wrapper_binary;

pub use shutdown_binary::SHUTDOWN_BIN_REMOTE_BASENAME;
pub use types::{
    CancelOutcome, CancelVerifyPolicy, JobStatus, JobStatusInfo, SlurmError, SlurmJobManager,
};
pub use wrapper_binary::WRAPPER_BIN_REMOTE_BASENAME;
