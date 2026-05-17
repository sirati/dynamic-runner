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
//! Source-path resolution lives one layer up, in the Python bridge
//! (``dynamic_runner._shutdown_manager.bundled_binary_path``):
//! env-var override (``DYNRUNNER_SLURM_SHUTDOWN_BIN_SOURCE``) >
//! wheel-bundled artifact under ``dynamic_runner/_shutdown_manager/``.
//! This Rust primitive takes the already-resolved local path and
//! performs only the upload + chmod + path-record mechanics. Keeping
//! the resolution policy on the Python side lets the framework wheel
//! ship the binary as bundled data (the nix-wheel postInstall copies
//! it into the site-packages tree) without coupling the Rust crate
//! to ``importlib.resources``.

use std::path::PathBuf;

use dynrunner_gateway::traits::Gateway;
use tracing;

use super::types::{SlurmError, SlurmJobManager};

/// Basename of the binary on the gateway. Lives alongside
/// `job_<name>.sh` under `root_folder`.
pub const SHUTDOWN_BIN_REMOTE_BASENAME: &str = "dynrunner-slurm-shutdown";

impl<G: Gateway> SlurmJobManager<G> {
    /// Stage the `dynrunner-slurm-shutdown` binary on the gateway,
    /// reading it from the caller-supplied local source path.
    ///
    /// Behaviour:
    ///
    /// * Verifies the local file exists. A path that does not point
    ///   at a real file surfaces as
    ///   [`SlurmError::ShutdownBinaryNotFound`] — the caller already
    ///   decided this is the binary to deploy, so a missing source
    ///   deserves a hard failure rather than a silent skip.
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
    /// Returns the resolved remote path on success. There is no
    /// "skip" branch: the previous opt-in env-var model silently
    /// disabled orphan-container cleanup whenever the var was unset,
    /// which is exactly the failure mode this binary was built to
    /// prevent. Source resolution (env-var override vs wheel-bundled
    /// artifact) is the Python bridge's concern; this primitive only
    /// uploads what it was given.
    ///
    /// Idempotency: this is a write-once per `SlurmJobManager`
    /// lifetime. Calling twice re-uploads the same artifact — same
    /// shape as the image-transfer and source-binary upload paths
    /// (neither has a "skip if exists" short-circuit; that's a
    /// gateway-layer concern, not a manager-layer concern).
    pub async fn upload_shutdown_manager_binary_from(
        &mut self,
        local: PathBuf,
    ) -> Result<String, SlurmError> {
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
        Ok(remote_path)
    }
}
