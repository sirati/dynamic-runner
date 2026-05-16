//! StageFile handler.
//!
//! The runner does not transfer file payloads itself: files live on a
//! shared drive (`src_network`) or are out-of-band-staged via SSH. The
//! `StageFile` wire message tells a secondary "the file is now
//! available — copy it to your local scratch and register the location
//! so the next TaskAssignment for this hash resolves cleanly."
//!
//! Resolution semantics:
//! - If `src_path` is absolute, it's treated as already at the source
//!   path (out-of-band SSH staging fallback).
//! - If `src_path` is relative AND `src_network` is configured, the
//!   absolute source is `src_network/<src_path>`.
//! - The destination is always `src_tmp/<dest_path>`.
//!
//! Failures (missing source, copy I/O, hash mismatch) are logged and
//! swallowed: the next TaskAssignment for the un-staged hash will
//! still surface a clean TaskFailed via dispatch.rs.

use std::path::{Path, PathBuf};

use crate::zip_extract::compute_file_hash;

/// Result of a single stage attempt.
pub(super) struct StageOutcome {
    pub(super) dest: PathBuf,
}

/// Copy `src` → `dest` (creating parent dirs), then verify the
/// destination's SHA256 matches `expected_content_hash`. On any
/// failure returns Err with a human-readable reason; the caller
/// logs and skips registration.
///
/// `expected_content_hash` is the SHA256 of the file contents (a
/// 64-char hex string) — the primary computes it once via
/// `compute_file_hash` at staging-notification time and ships it as
/// `StageFile.content_hash` / `StagedFileRecord.content_hash`. It's
/// distinct from the task identifier hash (`file_hash`), which is
/// path/identifier-derived and used as the `ExtractionCache` lookup
/// key — those used to be conflated under one field, which always
/// failed verification (16-char DefaultHasher hex vs 64-char SHA256
/// hex). The two are now plumbed independently.
pub(super) fn stage_file(
    src_network: Option<&Path>,
    src_tmp: &Path,
    src_path: &str,
    dest_path: &str,
    expected_content_hash: &str,
) -> Result<StageOutcome, String> {
    let src_p = PathBuf::from(src_path);
    let effective_src = if src_p.is_absolute() {
        src_p
    } else {
        let net = src_network.ok_or_else(|| {
            format!(
                "stage_file: src_path {src_path:?} is relative but no src_network is configured"
            )
        })?;
        net.join(src_path)
    };

    if !effective_src.exists() {
        return Err(format!(
            "stage_file: source not found at {}",
            effective_src.display()
        ));
    }

    let dest = src_tmp.join(dest_path);
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("stage_file: mkdir {} failed: {e}", parent.display()))?;
    }

    // If the destination already matches the expected content hash,
    // skip the copy (idempotent — repeated StageFile notifications
    // are cheap).
    if dest.exists()
        && let Some(existing_hash) = compute_file_hash(&dest)
        && existing_hash == expected_content_hash
    {
        return Ok(StageOutcome { dest });
    }

    std::fs::copy(&effective_src, &dest).map_err(|e| {
        format!(
            "stage_file: copy {} -> {} failed: {e}",
            effective_src.display(),
            dest.display()
        )
    })?;

    let actual = compute_file_hash(&dest).ok_or_else(|| {
        format!(
            "stage_file: failed to compute hash of staged file {}",
            dest.display()
        )
    })?;
    if actual != expected_content_hash {
        return Err(format!(
            "stage_file: content-hash mismatch at {}: expected {expected_content_hash}, got {actual}",
            dest.display()
        ));
    }

    Ok(StageOutcome { dest })
}
