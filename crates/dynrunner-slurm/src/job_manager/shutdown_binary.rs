//! Upload primitive for the `dynrunner-slurm-shutdown` binary.
//!
//! Single concern: stage the musl-static shutdown-manager binary on
//! the SLURM gateway so per-job wrapper scripts can spawn it via
//! `systemd-run --user --scope` and have it survive cgroup teardown.
//! Same deployment pattern as the per-job wrapper script
//! (`job_<name>.sh`) and the source-binary upload — write to
//! `root_folder`, `chmod` the remote, remember the resolved path.
//!
//! Why upload instead of letting the wrapper reference a
//! `/nix/store/...` path verbatim: not every SLURM cluster shares
//! `/nix/store` between the head node (dispatcher) and the compute
//! nodes (where the wrapper runs). LMU Krater does not. Uploading to
//! the gateway's shared NFS-mounted `root_folder` (the same folder
//! that already holds `job_<name>.sh` and the image tarball)
//! guarantees the wrapper finds the binary regardless of nix-store
//! sharing.
//!
//! Why an env var (`DYNRUNNER_SLURM_SHUTDOWN_BIN_SOURCE`) for the
//! source path: the consumer's `flake.nix` builds the
//! `shutdown-manager` subproject and pipes the
//! `result/bin/dynrunner-slurm-shutdown` path into the dispatcher's
//! env. The framework reads it here. No build-time coupling between
//! the framework crate and the shutdown-manager crate (they live in
//! the same repo but are intentionally NOT in the same Cargo
//! workspace — see workspace `exclude = ["shutdown-manager"]`).

use std::path::PathBuf;

use dynrunner_gateway::traits::Gateway;
use tracing;

use super::types::{SlurmError, SlurmJobManager};

/// Environment variable the dispatcher consults for the local
/// (head-node-side) path to the built `dynrunner-slurm-shutdown`
/// binary. Set by the consumer's nix flake (`result/bin/dynrunner-
/// slurm-shutdown`); unset means "skip the upload — orphan-container
/// cleanup is disabled for this run".
pub const SHUTDOWN_BIN_SOURCE_ENV: &str = "DYNRUNNER_SLURM_SHUTDOWN_BIN_SOURCE";

/// Basename of the binary on the gateway. Lives alongside
/// `job_<name>.sh` under `root_folder`.
pub const SHUTDOWN_BIN_REMOTE_BASENAME: &str = "dynrunner-slurm-shutdown";

impl<G: Gateway> SlurmJobManager<G> {
    /// Stage the `dynrunner-slurm-shutdown` binary on the gateway.
    ///
    /// Reads the local source path from
    /// [`SHUTDOWN_BIN_SOURCE_ENV`]. On the unset path: logs a warning
    /// and returns `Ok(None)` so callers continue without orphan
    /// cleanup (the warning surfaces the missing integration to
    /// operators; the run still proceeds because the cleanup feature
    /// is an opt-in production integration, not a correctness
    /// prerequisite).
    ///
    /// On the set path:
    ///
    /// * Verifies the local file exists. A misconfigured env var or a
    ///   broken consumer flake surfaces as
    ///   [`SlurmError::ShutdownBinaryNotFound`] rather than a silent
    ///   skip — the operator opted into the feature, so a failure
    ///   deserves loud surfacing at dispatch time.
    /// * Transfers the binary to
    ///   `<root_folder>/dynrunner-slurm-shutdown` via the gateway's
    ///   `transfer_file` primitive (same path the per-job wrapper
    ///   scripts land under).
    /// * `chmod 755`s the remote file. Mirrors `submit_job`'s
    ///   `chmod +x` step for `job_<name>.sh`; uses an explicit `755`
    ///   so the bit pattern is operator-visible in the rendered
    ///   command (operator-readable artifacts on a shared NFS folder
    ///   benefit from the explicit mode).
    /// * Records the resolved remote path on the manager so wrapper
    ///   renders (both initial cohort and respawn) read it back via
    ///   [`SlurmJobManager::shutdown_manager_remote_path`].
    ///
    /// Returns the resolved remote path on success, or `None` when
    /// the env var was unset.
    ///
    /// Idempotency: this is a write-once per `SlurmJobManager`
    /// lifetime. Calling twice re-uploads the same artifact — same
    /// shape as the image-transfer and source-binary upload paths
    /// (neither has a "skip if exists" short-circuit; that's a
    /// gateway-layer concern, not a manager-layer concern).
    pub async fn upload_shutdown_manager_binary(
        &mut self,
    ) -> Result<Option<String>, SlurmError> {
        let local = match std::env::var(SHUTDOWN_BIN_SOURCE_ENV) {
            Ok(s) if !s.is_empty() => PathBuf::from(s),
            _ => {
                // Env var unset OR explicitly empty. Treat both as
                // "operator did not opt into the feature" and continue
                // without orphan cleanup. The warning surfaces the
                // missing integration so an operator who DID intend
                // to enable it sees the gap.
                tracing::warn!(
                    env_var = SHUTDOWN_BIN_SOURCE_ENV,
                    "{SHUTDOWN_BIN_SOURCE_ENV} not set; orphan-container cleanup \
                     disabled for this run"
                );
                self.shutdown_manager_remote_path = None;
                return Ok(None);
            }
        };

        if !local.exists() {
            return Err(SlurmError::ShutdownBinaryNotFound(local));
        }

        // Remote layout mirrors `submit_job`'s `job_<name>.sh`
        // placement: directly under `root_folder`, no nested
        // subfolder. Keeps the shared-NFS layout flat and predictable
        // (one operator-visible directory holds every per-run
        // gateway-side artifact: wrappers, image tarball, the
        // shutdown binary).
        let remote_path = format!(
            "{}/{}",
            self.config.root_folder, SHUTDOWN_BIN_REMOTE_BASENAME
        );

        self.gateway
            .transfer_file(local.as_path(), &remote_path)
            .await?;

        // Explicit `chmod 755` rather than `chmod +x`: the binary
        // lives on a shared NFS folder, and the explicit mode makes
        // the rendered command operator-readable when audited. Mirror
        // of `submit_job`'s chmod step, modulo the literal mode bits.
        let chmod_cmd = format!("chmod 755 {remote_path}");
        let result = self.gateway.execute_command(&chmod_cmd, None).await?;
        if !result.success() {
            return Err(SlurmError::Command(format!(
                "chmod 755 on uploaded shutdown-manager binary failed: {}",
                result.stderr
            )));
        }

        tracing::info!(
            local = %local.display(),
            remote = %remote_path,
            "uploaded shutdown-manager binary",
        );

        self.shutdown_manager_remote_path = Some(remote_path.clone());
        Ok(Some(remote_path))
    }
}
