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
//!
//! ## Provably fresh: verify-after-upload
//!
//! Both branches of the gate end with a PROVEN hash match between the
//! local source and the remote bytes:
//!
//! * the **skip** branch is taken only when the freshly computed local
//!   SHA-256 equals the freshly computed remote SHA-256 (no recorded
//!   state is ever consulted, so there is nothing that can go stale);
//! * the **upload** branch re-hashes the remote AFTER the transfer and
//!   hard-errors on a mismatch (a truncated/corrupted transfer must
//!   never let a run dispatch against wrong bytes).
//!
//! Every decision line logs BOTH hashes, so an operator can compare
//! the deployed binary against a locally built artifact with one
//! `sha256sum`. This keeps the #241 bandwidth win (no transfer on a
//! genuine cache hit) with zero staleness window: a stale REMOTE is
//! impossible by construction. A stale LOCAL source (e.g. an outdated
//! `DYNRUNNER_SLURM_WRAPPER_BIN_SOURCE` override) is outside this
//! primitive's reach — the logged local hash is the audit hook for
//! that case.

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
    ///   `transfer_file`, then RE-HASHES the remote and requires it to
    ///   equal the local hash —
    ///   [`SlurmError::StagedBinaryHashMismatch`] on any divergence —
    ///   and `chmod 755`s it.
    /// * Returns the resolved remote path. On `Ok`, the remote bytes
    ///   provably equal the local bytes (hash-verified on BOTH
    ///   branches; see the module doc's "Provably fresh" section).
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
        // the source is genuinely gone, and the post-upload
        // verification below re-attempts the local hash).
        let local_hash = compute_file_hash(local);
        let remote_hash = self.gateway_file_sha256(&remote_path).await?;

        let up_to_date = match (&local_hash, &remote_hash) {
            (Some(l), Some(r)) => l == r,
            _ => false,
        };
        if up_to_date {
            tracing::info!(
                local = %local.display(),
                remote = %remote_path,
                hash = local_hash.as_deref().unwrap_or("<unhashable>"),
                "binary up-to-date on gateway (local and remote SHA-256 match), skipping upload",
            );
        } else {
            tracing::info!(
                local = %local.display(),
                remote = %remote_path,
                local_hash = local_hash.as_deref().unwrap_or("<unhashable>"),
                remote_hash = remote_hash.as_deref().unwrap_or("<absent>"),
                "no byte-identical copy on gateway, uploading binary",
            );
            self.gateway.transfer_file(local, &remote_path).await?;

            // Freshness proof: re-hash the REMOTE after the transfer and
            // require it to equal the local hash. A truncated/corrupted
            // transfer (or a gateway-side clobber racing the upload) must
            // surface as a hard error at dispatch time, never as a fleet
            // exec'ing wrong bytes. The local hash is re-attempted here
            // for the (`exists()`-raced) case where the pre-gate hash
            // failed: a transfer that just succeeded read the source, so
            // a still-unhashable local is itself a verification failure.
            let local_hash = match local_hash.or_else(|| compute_file_hash(local)) {
                Some(h) => h,
                None => {
                    return Err(SlurmError::StagedBinaryHashMismatch {
                        remote: remote_path,
                        local_hash: "<unhashable local source>".into(),
                        remote_hash: None,
                    });
                }
            };
            let uploaded_hash = self.gateway_file_sha256(&remote_path).await?;
            if uploaded_hash.as_deref() != Some(local_hash.as_str()) {
                return Err(SlurmError::StagedBinaryHashMismatch {
                    remote: remote_path,
                    local_hash,
                    remote_hash: uploaded_hash,
                });
            }
            tracing::info!(
                local = %local.display(),
                remote = %remote_path,
                hash = %local_hash,
                "uploaded binary to gateway (remote SHA-256 verified against local)",
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
