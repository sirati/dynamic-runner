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
    /// * Stages the binary at `<root_folder>/dynrunner-slurm-wrapper`
    ///   hash-conditionally (see
    ///   [`SlurmJobManager::upload_binary_hash_conditional`]): the
    ///   transfer is skipped when the gateway already holds a
    ///   byte-identical copy, and `chmod 755`d on either branch.
    /// * Records the resolved remote path on the manager so wrapper
    ///   renders (both initial cohort and respawn) read it back via
    ///   [`SlurmJobManager::wrapper_bin_remote_path`].
    ///
    /// Returns the resolved remote path on success. There is no
    /// "missing source is fine" branch: the SLURM dispatch path always
    /// renders the stub against this binary, so a missing source is a
    /// hard failure, not a silent fall-back to the legacy bash body.
    /// Source resolution (env-var override vs wheel-bundled artifact)
    /// is the Python bridge's concern; this primitive only uploads what
    /// it was given.
    ///
    /// Idempotency: calling twice re-stages the same artifact. The
    /// hash-conditional gate makes the second call a no-op transfer
    /// (cache hit) rather than re-pushing the bytes — and a changed or
    /// corrupted/partial remote forces a re-upload.
    pub async fn upload_wrapper_binary_from(
        &mut self,
        local: PathBuf,
    ) -> Result<String, SlurmError> {
        let remote_path = self
            .upload_binary_hash_conditional(
                local.as_path(),
                WRAPPER_BIN_REMOTE_BASENAME,
                SlurmError::WrapperBinaryNotFound,
            )
            .await?;
        self.wrapper_bin_remote_path = Some(remote_path.clone());
        Ok(remote_path)
    }
}
