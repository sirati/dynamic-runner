//! Primary-side helper for emitting StageFile notifications.
//!
//! The primary does NOT transfer file payloads itself: a separate
//! pipeline (packaging.preparation, SSH copy, etc.) places the file
//! on the shared drive (`src_network`) — or out-of-band on the
//! secondary's host — and then asks us to tell the secondary "this
//! file is now available at `<src_path>`; please stage it to
//! `<dest_path>` so the next TaskAssignment for it resolves cleanly."

use std::path::{Path, PathBuf};

use dynrunner_core::{Identifier, TaskInfo, TypeId};
use dynrunner_protocol_primary_secondary::{
    Destination, DistributedMessage, PeerId, PeerTransport,
};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use super::PrimaryCoordinator;
use super::wire::{compute_task_hash, timestamp_now};
use crate::zip_extract::compute_file_hash;

/// Errors raised by the primary's initial-staging walk.
///
/// Distinct variants so PyO3 (or any other front-end) can map each
/// cause to its own exception class without parsing free-form
/// strings.
#[derive(Debug)]
pub enum StagingError {
    /// A binary's resolved on-disk source path could not be read
    /// (missing file, permission denied, wrong `--source` directory,
    /// etc.). The diagnostic preserves the same surface that the
    /// previous PyO3-side wrapper produced so existing consumer
    /// breadcrumbs (e.g. error-grep predicates in
    /// `asm-tokenizer`) keep matching.
    SourceUnreadable {
        /// The original `binary.path` as discovered by the consumer.
        path: PathBuf,
        /// The path actually opened on the primary's filesystem
        /// (`source_root.join(path)` for relative paths, `path`
        /// verbatim when absolute).
        resolved: PathBuf,
        /// `TaskInfo.type_id` of the offending binary; aids the
        /// operator pinning down which task list contains the
        /// broken entry.
        type_id: TypeId,
    },
}

impl std::fmt::Display for StagingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StagingError::SourceUnreadable {
                path,
                resolved,
                type_id,
            } => write!(
                f,
                "queue_initial_staging: cannot read {} (resolved={}, type_id={}). \
                 Typical causes: --source points at the wrong tree; the file is \
                 missing or permission-denied. Aborting before dispatch so the \
                 misconfiguration surfaces here rather than as a downstream secondary \
                 'not pre-staged at <path>' error.",
                path.display(),
                resolved.display(),
                type_id,
            ),
        }
    }
}

impl std::error::Error for StagingError {}

/// One per-secondary staging entry emitted by
/// [`compute_initial_staging_entries`]: the 5-tuple
/// `(secondary_id, file_hash, content_hash, src_path, dest_path)`.
/// `file_hash` is the task identifier (cache lookup key);
/// `content_hash` is the SHA256 of the file contents the staging
/// integrity check verifies against; `src_path` / `dest_path` are
/// the primary-side read location and the secondary-side stage
/// destination respectively.
pub type StagingEntry = (String, String, String, String, String);

/// Compute the per-secondary StageFile entries for `binaries`, fanned
/// out across each id in `secondary_ids`.
///
/// Pure function (no `&mut self`) so the same walk can populate both
/// the in-process `PrimaryCoordinator::pending_stage_files` (via the
/// method below) AND a free-standing PyO3-wrapper buffer that holds
/// staging entries before the coordinator is constructed (the SLURM
/// pipeline's `coord.queue_initial_staging(...)` pre-call shape).
///
/// `secondary_ids` is supplied by the caller so this function holds
/// no embedded naming convention: the SLURM/network pipeline uses
/// `secondary-{i}`, the in-process `PyDistributedManager` uses
/// `sec-{i}`, and tests can use any string. Every entry in
/// `binaries` is fanned out across every id; ordering of `entries`
/// is `(binary_0 × ids_0..n) ++ (binary_1 × ids_0..n) ++ …`.
///
/// `source_root` interprets `binary.path` shapes uniformly:
///
/// * absolute under `source_root` — `<rel>` is the strip-prefixed
///   tail (the legacy shape, e.g. when discovery emits
///   `source_root.join(rel)` directly);
/// * absolute out-of-tree — `<rel>` keeps the full path and the
///   secondary's `stage_file` handler treats it as out-of-band
///   staged (must already exist by some other means);
/// * relative — resolved against `source_root` for the on-disk
///   read; `<rel>` is the original relative path verbatim.
///
/// Reads each binary file once on the primary side to compute the
/// content SHA256. Errors out on the first unreadable file rather
/// than silently skipping — a broken local `--source` is a
/// configuration bug the consumer wants to surface immediately, not
/// a partial dispatch that later fails on the secondary as a
/// confusing "not pre-staged at <path>" error with no breadcrumb
/// pointing back to the primary's drop.
///
/// Tuple shape: `(secondary_id, file_hash, content_hash, src_path, dest_path)`.
/// `file_hash` is the task identifier (cache lookup key);
/// `content_hash` is the SHA256 of the file contents the staging
/// integrity check verifies against.
pub fn compute_initial_staging_entries<I: Identifier>(
    binaries: &[TaskInfo<I>],
    secondary_ids: &[String],
    source_root: &Path,
) -> Result<Vec<StagingEntry>, StagingError> {
    let mut entries = Vec::with_capacity(binaries.len() * secondary_ids.len());
    for binary in binaries {
        // Resolve the on-disk read location: relative paths join
        // against `source_root` (post-Bug-B wire-id shape);
        // absolute paths are used verbatim. `rel` (the wire form
        // shipped to secondaries) is then derived from the
        // resolved path so the strip-prefix branch covers both
        // legacy `source_root.join(rel)` shapes and new relative
        // emissions uniformly.
        let resolved = if binary.path.is_absolute() {
            binary.path.clone()
        } else {
            source_root.join(&binary.path)
        };
        let rel = match resolved.strip_prefix(source_root) {
            Ok(p) => p.to_string_lossy().into_owned(),
            Err(_) => binary.path.to_string_lossy().into_owned(),
        };
        let file_hash = compute_task_hash(binary);
        let Some(content_hash) = compute_file_hash(&resolved) else {
            return Err(StagingError::SourceUnreadable {
                path: binary.path.clone(),
                resolved,
                type_id: binary.type_id.clone(),
            });
        };
        for sid in secondary_ids {
            entries.push((
                sid.clone(),
                file_hash.clone(),
                content_hash.clone(),
                rel.clone(),
                rel.clone(),
            ));
        }
    }
    Ok(entries)
}

impl<Tr, S, E, I> PrimaryCoordinator<Tr, S, E, I>
where
    Tr: PeerTransport<I>,
    S: Scheduler<I>,
    E: ResourceEstimator<I>,
    I: Identifier,
{
    /// Walk `binaries`, compute task + content hashes, resolve source
    /// paths against `source_root`, and queue per-secondary StageFile
    /// entries (one per `(binary, secondary_id)` pair) onto
    /// `self.pending_stage_files`. Subsequent
    /// `perform_initial_assignment` drains them into each recipient's
    /// `InitialAssignment.staged_files`.
    ///
    /// Single source of truth for the pre-`run()` staging walk;
    /// shared between the in-process distributed pipeline and the
    /// PyO3 SLURM-pipeline pre-call (which delegates here via
    /// [`compute_initial_staging_entries`]).
    ///
    /// `secondary_ids` is supplied by the caller — different
    /// pipelines name their secondaries differently (`secondary-{i}`
    /// for the SLURM/network primary, `sec-{i}` for the in-process
    /// distributed manager) and the pure walk shouldn't bake either
    /// convention in.
    ///
    /// Errors (e.g. `SourceUnreadable`) abort before any wire I/O
    /// so a misconfigured `--source` surfaces here instead of as a
    /// downstream secondary "not pre-staged at <path>" rejection.
    pub fn queue_initial_staging_from_binaries(
        &mut self,
        binaries: &[TaskInfo<I>],
        secondary_ids: &[String],
        source_root: &Path,
    ) -> Result<(), StagingError> {
        let entries = compute_initial_staging_entries(binaries, secondary_ids, source_root)?;
        self.pending_stage_files.extend(entries);
        Ok(())
    }

    /// Auto-stage entry point invoked from `run()` after
    /// `wait_for_connections`. Walks `self.all_binaries` against
    /// `self.config.source_dir` and queues per-secondary StageFile
    /// entries for every connected secondary, but only when:
    ///
    ///   * `pending_stage_files.is_empty()` — no caller pre-queued
    ///     (the SLURM pipeline's explicit `queue_initial_staging`
    ///     pre-call wins; here we'd skip).
    ///   * `config.uses_file_based_items` — items are file-backed
    ///     (when False the framework passes `local_path` through to
    ///     the worker as an opaque identifier, no staging).
    ///   * `config.source_pre_staged_root.is_none()` — pre-staged
    ///     mode bind-mounts the source; staging would be redundant.
    ///   * `config.source_dir.is_some()` — we have a root to read
    ///     file contents from for the content-hash. Callers without
    ///     `source_dir` (e.g. tests with absolute on-disk paths and
    ///     fake workers) must pre-queue or accept the failure.
    ///
    /// Errors propagate as the `String` shape `run()` already returns
    /// (the `StagingError` formatting carries the full diagnostic).
    /// Each gate is logged at debug level so a regression where a
    /// caller forgot to thread `source_dir` is visible without
    /// silently losing staging.
    pub(super) fn maybe_auto_stage_initial(&mut self) -> Result<(), String> {
        if !self.pending_stage_files.is_empty() {
            tracing::debug!(
                pre_queued = self.pending_stage_files.len(),
                "auto-stage skipped: caller pre-queued staging entries"
            );
            return Ok(());
        }
        if !self.config.uses_file_based_items {
            tracing::debug!("auto-stage skipped: uses_file_based_items=false (opaque local_path)");
            return Ok(());
        }
        if self.config.source_pre_staged_root.is_some() {
            tracing::debug!("auto-stage skipped: pre-staged-source mode (bind-mount)");
            return Ok(());
        }
        let Some(source_dir) = self.config.source_dir.clone() else {
            tracing::debug!(
                "auto-stage skipped: source_dir not configured; \
                 caller must pre-queue or rely on out-of-band staging"
            );
            return Ok(());
        };
        let secondary_ids: Vec<String> = self.secondaries.keys().cloned().collect();
        if secondary_ids.is_empty() {
            // Reachable only as a contract violation:
            // maybe_auto_stage_initial runs after wait_for_connections,
            // which only returns Ok once num_secondaries are
            // registered. Defending against a future refactor that
            // moves the call site.
            tracing::warn!(
                "auto-stage skipped: zero connected secondaries at staging time \
                 (called before wait_for_connections?)"
            );
            return Ok(());
        }
        let binaries = self.all_binaries.clone();
        tracing::info!(
            binaries = binaries.len(),
            secondaries = secondary_ids.len(),
            source_dir = %source_dir.display(),
            "auto-staging initial entries (no caller pre-queue)"
        );
        self.queue_initial_staging_from_binaries(&binaries, &secondary_ids, &source_dir)
            .map_err(|e| e.to_string())
    }

    /// Send a `StageFile` notification to a specific secondary.
    ///
    /// `src_path` is interpreted by the secondary relative to its
    /// configured `src_network` (when relative) or as an absolute
    /// path (out-of-band SSH-staged source). `dest_path` is always
    /// relative to the secondary's `src_tmp`.
    pub async fn notify_stage_file(
        &mut self,
        secondary_id: &str,
        file_hash: String,
        content_hash: String,
        src_path: String,
        dest_path: String,
    ) -> Result<(), String> {
        let msg = DistributedMessage::StageFile {
            target: None,
            sender_id: self.config.node_id.clone(),
            timestamp: timestamp_now(),
            secondary_id: secondary_id.to_string(),
            file_hash,
            content_hash,
            src_path,
            dest_path,
        };
        self.send_to(Destination::Secondary(PeerId::from(secondary_id)), msg)
            .await
    }
}
