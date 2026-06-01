//! Upload primitive for the `dynrunner-slurm-wrapper` binary.
//!
//! Single concern: stage the musl-static secondary-wrapper binary on
//! the SLURM gateway so each per-job wrapper-script stub can `exec` it
//! to run the full secondary lifecycle (the stub is rendered by
//! `wrapper_script::generate` when
//! `WrapperScriptConfig::wrapper_bin_path` is `Some`).
//! Same deployment pattern as the per-job wrapper script
//! (`job_<name>.sh`) and the source-binary upload — write to
//! `root_folder`, `chmod` the remote, remember the resolved path.
//!
//! Why upload instead of letting the stub reference a
//! `/nix/store/...` path verbatim: not every SLURM cluster shares
//! `/nix/store` between the head node (dispatcher) and the compute
//! nodes (where the wrapper runs). LMU Krater does not. Uploading to
//! the gateway's shared NFS-mounted `root_folder` (the same folder
//! that already holds `job_<name>.sh` and the image tarball)
//! guarantees the stub finds the binary regardless of nix-store
//! sharing.
//!
//! Source-path resolution lives one layer up, in the Python bridge
//! (``dynamic_runner._wrapper_manager.bundled_binary_path``):
//! env-var override (``DYNRUNNER_SLURM_WRAPPER_BIN_SOURCE``) >
//! wheel-bundled artifact under ``dynamic_runner/_wrapper_manager/``.
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
pub const WRAPPER_BIN_REMOTE_BASENAME: &str = "dynrunner-slurm-wrapper";

impl<G: Gateway> SlurmJobManager<G> {
    /// Stage the `dynrunner-slurm-wrapper` binary on the gateway,
    /// reading it from the caller-supplied local source path.
    ///
    /// Behaviour:
    ///
    /// * Verifies the local file exists. A path that does not point
    ///   at a real file surfaces as
    ///   [`SlurmError::WrapperBinaryNotFound`] — the caller already
    ///   decided this is the binary to deploy, so a missing source
    ///   deserves a hard failure rather than a silent skip.
    /// * Transfers the binary to
    ///   `<root_folder>/dynrunner-slurm-wrapper` via the gateway's
    ///   `transfer_file` primitive (same path the per-job wrapper
    ///   scripts land under).
    /// * `chmod 755`s the remote file. Mirrors `submit_job`'s
    ///   `chmod +x` step for `job_<name>.sh`; uses an explicit `755`
    ///   so the bit pattern is operator-visible in the rendered
    ///   command (operator-readable artifacts on a shared NFS folder
    ///   benefit from the explicit mode).
    /// * Records the resolved remote path on the manager so wrapper
    ///   renders (both initial cohort and respawn) read it back via
    ///   [`SlurmJobManager::wrapper_bin_remote_path`].
    ///
    /// Returns the resolved remote path on success. There is no
    /// "skip" branch: the SLURM dispatch path always renders the stub
    /// against this binary, so a missing source is a hard failure, not
    /// a silent fall-back to the legacy bash body. Source resolution
    /// (env-var override vs wheel-bundled artifact) is the Python
    /// bridge's concern; this primitive only uploads what it was given.
    ///
    /// Idempotency: this is a write-once per `SlurmJobManager`
    /// lifetime. Calling twice re-uploads the same artifact — same
    /// shape as the image-transfer and source-binary upload paths
    /// (neither has a "skip if exists" short-circuit; that's a
    /// gateway-layer concern, not a manager-layer concern).
    pub async fn upload_wrapper_binary_from(
        &mut self,
        local: PathBuf,
    ) -> Result<String, SlurmError> {
        if !local.exists() {
            return Err(SlurmError::WrapperBinaryNotFound(local));
        }

        // Remote layout mirrors `submit_job`'s `job_<name>.sh`
        // placement: directly under `root_folder`, no nested
        // subfolder. Keeps the shared-NFS layout flat and predictable
        // (one operator-visible directory holds every per-run
        // gateway-side artifact: wrappers, image tarball, the
        // wrapper binary).
        let remote_path = format!(
            "{}/{}",
            self.config.root_folder, WRAPPER_BIN_REMOTE_BASENAME
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
                "chmod 755 on uploaded wrapper binary failed: {}",
                result.stderr
            )));
        }

        tracing::info!(
            local = %local.display(),
            remote = %remote_path,
            "uploaded wrapper binary",
        );

        self.wrapper_bin_remote_path = Some(remote_path.clone());
        Ok(remote_path)
    }
}
