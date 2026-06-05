//! Shared mechanics for staging a musl-static binary on the SLURM
//! gateway, used by both [`upload_shutdown_manager_binary_from`] and
//! [`upload_wrapper_binary_from`].
//!
//! [`upload_shutdown_manager_binary_from`]: super::shutdown_binary
//! [`upload_wrapper_binary_from`]: super::wrapper_binary
//!
//! Single concern: take an already-resolved local source path plus a
//! gateway-side basename and place the file under `root_folder`,
//! hash-conditionally — skipping the transfer when the gateway already
//! holds a byte-identical copy. The two public upload primitives keep
//! their distinct concerns (which basename, which `NotFound` error
//! variant, which remote-path field to record); the existence check,
//! the skip-if-up-to-date gate, the `chmod`, and the logging live here
//! once so neither primitive carries a copy of the mechanics.
//!
//! ## Why hash-conditional
//!
//! These binaries land on a shared NFS `root_folder` that persists
//! across runs. Re-uploading a multi-MB musl-static binary on every
//! dispatch — when the gateway already holds the same bytes from a
//! prior run — is wasted transfer. This mirrors the container-image
//! upload's cache-hit gate (`PodmanPackaging._maybe_upload`): compute
//! the local SHA-256, compare against the gateway copy, and only push
//! the bytes on a mismatch (or when the remote is absent).
//!
//! ## Where the gateway hash comes from
//!
//! `sha256sum <remote-path>` on the gateway, hashing the *actual*
//! remote bytes. Deliberately ground-truth rather than a sidecar
//! marker file: a marker can drift from the bytes it claims to
//! describe (a half-written or out-of-band-clobbered binary with an
//! intact marker would be falsely treated as up-to-date), whereas
//! re-hashing the real file catches a corrupted/partial remote and
//! forces a re-upload. A missing remote makes `sha256sum` exit
//! non-zero, which the gate reads as "absent" → upload.

use std::path::Path;

use dynrunner_gateway::traits::Gateway;
use dynrunner_manager_distributed::compute_file_hash;
use tracing;

use super::types::{SlurmError, SlurmJobManager};

impl<G: Gateway> SlurmJobManager<G> {
    /// Stage `local` on the gateway at `<root_folder>/<remote_basename>`,
    /// skipping the transfer when the gateway already holds a
    /// byte-identical copy.
    ///
    /// `not_found` builds the hard error a missing local source
    /// surfaces as — the caller already decided this is the binary to
    /// deploy, so a missing source deserves a loud failure rather than
    /// a silent skip.
    ///
    /// Behaviour:
    ///
    /// * Verifies the local file exists; otherwise returns
    ///   `not_found(local)`.
    /// * Computes the local SHA-256 and compares it against
    ///   `sha256sum <remote-path>` on the gateway. On a match the
    ///   transfer is skipped (the gateway already holds the same
    ///   bytes); the `chmod` still runs so a remote whose mode bits
    ///   drifted is corrected idempotently and cheaply.
    /// * On a mismatch or an absent remote, transfers the binary via
    ///   `transfer_file` and `chmod 755`s it.
    /// * Returns the resolved remote path.
    ///
    /// Remote layout mirrors `submit_job`'s `job_<name>.sh` placement:
    /// directly under `root_folder`, no nested subfolder, keeping the
    /// shared-NFS layout flat and operator-readable.
    pub(super) async fn upload_binary_hash_conditional(
        &mut self,
        local: &Path,
        remote_basename: &str,
        not_found: impl FnOnce(std::path::PathBuf) -> SlurmError,
    ) -> Result<String, SlurmError> {
        if !local.exists() {
            return Err(not_found(local.to_path_buf()));
        }

        let remote_path = format!("{}/{}", self.config.root_folder, remote_basename);

        // Compute the local digest and the gateway copy's digest, then
        // gate the transfer on equality. `compute_file_hash` is the
        // shared SHA-256 helper (`dynrunner-manager-distributed`); it
        // returns `None` only on a local read error, which — given the
        // `exists()` check above — means the file vanished or is
        // unreadable between the two calls. Treat that as "cannot vouch
        // for the local bytes" and fall through to an unconditional
        // upload (`transfer_file` then surfaces the real I/O error if
        // the source is genuinely gone).
        let local_hash = compute_file_hash(local);
        let remote_hash = self.gateway_file_sha256(&remote_path).await?;

        if let Some(local_hash) = &local_hash
            && remote_hash.as_deref() == Some(local_hash.as_str())
        {
            tracing::info!(
                local = %local.display(),
                remote = %remote_path,
                "binary up-to-date on gateway, skipping upload",
            );
        } else {
            self.gateway.transfer_file(local, &remote_path).await?;
            tracing::info!(
                local = %local.display(),
                remote = %remote_path,
                "uploaded binary to gateway",
            );
        }

        // Explicit `chmod 755` rather than `chmod +x`: the binary lives
        // on a shared NFS folder, and the explicit mode makes the
        // rendered command operator-readable when audited. Runs on both
        // the upload and the skip branch so a reused remote whose mode
        // bits drifted is corrected — the `chmod` is a cheap one-shot
        // command, not a byte transfer.
        let chmod_cmd = format!("chmod 755 {remote_path}");
        let result = self.gateway.execute_command(&chmod_cmd, None).await?;
        if !result.success() {
            return Err(SlurmError::Command(format!(
                "chmod 755 on staged binary {remote_path} failed: {}",
                result.stderr
            )));
        }

        Ok(remote_path)
    }

    /// SHA-256 (hex) of `remote_path` on the gateway via `sha256sum`,
    /// or `None` when the file is absent or `sha256sum` could not hash
    /// it.
    ///
    /// `sha256sum` prints `<hex>␣␣<path>`; we take the leading
    /// whitespace-delimited token. A non-zero exit (missing file, no
    /// `sha256sum`) maps to `None` so the caller treats it as "no
    /// usable remote copy" and uploads.
    async fn gateway_file_sha256(&self, remote_path: &str) -> Result<Option<String>, SlurmError> {
        let cmd = format!("sha256sum {remote_path}");
        let result = self.gateway.execute_command(&cmd, None).await?;
        if !result.success() {
            return Ok(None);
        }
        Ok(result
            .stdout
            .split_whitespace()
            .next()
            .map(str::to_owned)
            .filter(|h| !h.is_empty()))
    }
}
