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
//! - [`shutdown_binary`] — staging primitive for the
//!   `dynrunner-slurm-shutdown` musl-static binary (uploaded to the
//!   gateway alongside the per-job wrapper scripts so out-of-cgroup
//!   orphan-container cleanup survives `/nix/store` not being shared
//!   between dispatcher and compute node).
//! - [`tests`] — module-internal tests.

mod images;
mod lifecycle;
mod manager;
mod shutdown_binary;
#[cfg(test)]
mod tests;
mod types;

pub use shutdown_binary::SHUTDOWN_BIN_REMOTE_BASENAME;
pub use types::{JobStatus, JobStatusInfo, SlurmError, SlurmJobManager};
