//! Primary-side helper for emitting StageFile notifications.
//!
//! The primary does NOT transfer file payloads itself: a separate
//! pipeline (packaging.preparation, SSH copy, etc.) places the file
//! on the shared drive (`src_network`) — or out-of-band on the
//! secondary's host — and then asks us to tell the secondary "this
//! file is now available at `<src_path>`; please stage it to
//! `<dest_path>` so the next TaskAssignment for it resolves cleanly."

use std::path::{Path, PathBuf};

use dynrunner_core::{Identifier, TaskInfo, TypeId, resolve_against_root};
use dynrunner_protocol_primary_secondary::{Destination, DistributedMessage, PeerId};
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
/// `source_root` interprets `binary.path` shapes uniformly via the
/// shared [`resolve_against_root`] predicate — the SAME
/// "is this binary stageable into `<srcbins>/<rel>`?" question the
/// SLURM upload (`SlurmJobManager::upload_source_binaries`) asks:
///
/// * absolute under `source_root` — `<rel>` is the strip-prefixed
///   tail (the legacy shape, e.g. when discovery emits
///   `source_root.join(rel)` directly); stageable;
/// * relative — resolved against `source_root` for the on-disk
///   read; `<rel>` is the original relative path verbatim; stageable;
/// * absolute out-of-tree — NOT stageable: there is no `<rel>` tail,
///   so the srcbins layout `<srcbins>/<rel>` cannot place it. The
///   upload SKIPS such a binary (`images.rs` strip-prefix-`Err`
///   branch → `continue` + warn), so emitting a staging entry here
///   would tell the secondary about a file the upload never staged
///   — a silent `stage_file` "source not found" / empty-srcbins
///   strand. We therefore MIRROR the upload: skip + warn, never
///   emit an entry. The two functions agree on EXACTLY which
///   binaries are stageable (the [`resolve_against_root`] predicate
///   == strip-prefix succeeds under `source_root`).
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
        // Shared stageability predicate (mirrors the SLURM upload):
        // `resolve_against_root` joins relative paths against
        // `source_root` for the on-disk read and derives the
        // wire-relative `<rel>` tail. `relative == Some(rel)` means
        // the binary is stageable into `<srcbins>/<rel>`; `None` is
        // the absolute-out-of-tree shape that has no `<rel>` tail.
        let resolved_path = resolve_against_root(&binary.path, source_root);
        let resolved = resolved_path.absolute;
        let Some(rel) = resolved_path.relative else {
            // Out-of-tree: NOT stageable. The upload skips this
            // binary (it cannot place it under `<srcbins>/<rel>`),
            // so we MUST NOT emit a staging entry — doing so would
            // promise the secondary a file the upload never staged,
            // surfacing as a swallowed `stage_file` "source not
            // found" / empty-srcbins strand. Skip + warn so an
            // absolute-path consumer SEES why their file wasn't
            // staged rather than hitting a silent File-not-found.
            // Kept in lock-step with
            // `SlurmJobManager::upload_source_binaries` (images.rs).
            tracing::warn!(
                raw = %binary.path.display(),
                resolved = %resolved.display(),
                source_root = %source_root.display(),
                "binary is not under --source root; skipping staging \
                 entry (the upload also skips it; the secondary will \
                 not be told about a file that was never staged).",
            );
            continue;
        };
        let rel = rel.to_string_lossy().into_owned();
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

impl<S, E, I> PrimaryCoordinator<S, E, I>
where
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
        // Framework flagged staging (#489 P3): when `--stage-via-setup-tasks`
        // is on, file-staging is the setup-task model (per-file pre-succeeded
        // setup tasks seeded at run-seed time), so the OLD StageFile fan-out
        // MUST NOT also run — the two staging systems never both execute (no
        // double-staging). The selector is the SOLE branch on the strategy in
        // this module; the setup-task path's seeding lives entirely in the
        // seed originators (`originate_cold_seed` / `discover_on_promotion`),
        // not here.
        if self.config.staging_strategy != crate::primary::StagingStrategy::Disabled {
            tracing::debug!(
                "auto-stage skipped: staging-via-setup-tasks is on (the \
                 setup-task model seeds per-file pre-succeeded setup tasks at \
                 run-seed time; the old StageFile fan-out does not run)"
            );
            return Ok(());
        }
        // Resume detection (failover-promotion). A promoted primary that
        // INHERITED a populated CRDT is RESUMING an already-running run —
        // the corpus was staged once by the run's original primary and
        // replicated as `InFlight`/terminal task state. Re-running the
        // full staging walk on resume re-copies every binary needlessly
        // (the "auto-staging initial entries binaries=320" smell on a
        // hydrated resume). A run is RESUMING iff ≥1 task carries
        // DISPATCH-DERIVED state — `InFlight`, or a terminal a worker
        // produced (`Completed`/`Failed`/`Unfulfillable`): each of those
        // proves a primary previously reached its dispatch loop, which
        // implies the corpus was staged. SEED-TIME terminals prove no such
        // thing and are excluded: `InvalidTask` (the ingest's #2
        // missing-dep classification, minted terminal BEFORE any dispatch)
        // and `SkippedAlreadyDone` (the discovery-time skip) both exist on
        // a first-ever cold/relocated seed. Counting `invalid_task` here
        // mis-read any fresh relocate whose corpus carried an invalid
        // task as a failover-resume, skipped the staging walk wholesale,
        // and every dispatched task died NonRecoverable "not pre-staged
        // at <path>" (the distributed-local-subprocess e2e repro,
        // 2026-06-10). A genuinely FRESH promoted destination (a
        // setup-peer relocate, or a `--source-already-staged` local
        // primary) hydrates a CRDT with no dispatch-derived entry, so this
        // gate is open and the first-ever staging proceeds.
        let counts = self.cluster_state.counts();
        let progressed =
            counts.in_flight + counts.completed + counts.failed + counts.unfulfillable;
        if progressed > 0 {
            tracing::debug!(
                in_flight = counts.in_flight,
                completed = counts.completed,
                failed = counts.failed,
                "auto-stage skipped: populated CRDT indicates a resume \
                 (failover-promotion) — the corpus was staged by the \
                 original primary; not re-staging on resume"
            );
            return Ok(());
        }
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

#[cfg(test)]
mod tests {
    //! Tests for [`compute_initial_staging_entries`]'s stageability
    //! predicate — specifically that it agrees with the SLURM upload
    //! (`SlurmJobManager::upload_source_binaries`) on which binaries
    //! are stageable into `<srcbins>/<rel>`:
    //!
    //! * in-tree (relative / absolute-under-root) → entry emitted;
    //! * out-of-tree (absolute, not under `--source`) → NO entry +
    //!   a warn (the upload also skips it, so promising the secondary
    //!   such a file would be a silent strand).
    //!
    //! The pre-fix behaviour fell back to keeping the entry with the
    //! full absolute path as `rel`; the revert-check (`revert_check_*`)
    //! pins the corrected skip so a regression to fallback-keep fails
    //! loudly here.

    use std::sync::{Arc, Mutex};

    use dynrunner_core::{PhaseId, SoftPreferredSecondaries, TaskInfo, TypeId, resolve_against_root};
    use serde::{Deserialize, Serialize};
    use tracing::field::{Field, Visit};
    use tracing_subscriber::Registry;
    use tracing_subscriber::layer::{Context, Layer, SubscriberExt};

    use super::{StagingEntry, compute_initial_staging_entries};

    #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
    struct TestId(String);

    /// Build a `TaskInfo` with an arbitrary `path` shape.
    fn make_binary(path: impl Into<std::path::PathBuf>) -> TaskInfo<TestId> {
        let path = path.into();
        let id = path.display().to_string();
        TaskInfo {
            path,
            size: 0,
            identifier: TestId(id.clone()),
            phase_id: PhaseId::from("default"),
            type_id: TypeId::from("default"),
            affinity_id: None,
            payload: serde_json::Value::Null,
            task_id: id,
            task_depends_on: vec![],
            preferred_secondaries: SoftPreferredSecondaries::default(),
            preferred_version: Default::default(),
            kind: Default::default(),
            setup_affinity: None,
            upload_file: None,
            resolved_path: None,
        }
    }

    /// Minimal WARN-level capture layer: records every WARN event's
    /// message so a test can assert the skip-warn actually fired
    /// (no false-green). The level discrimination happens INSIDE
    /// `on_event` rather than via a global `LevelFilter` layer: a
    /// global filter would cache `Interest` in the PROCESS-GLOBAL
    /// per-callsite table and poison the info-level
    /// `IMPORTANT_TARGET` callsites that a concurrently-running
    /// importance-capture test (`primary::important_events`) depends
    /// on (see `test_capture.rs`). A bare-`Registry` + unfiltered
    /// layer caches `Interest::always`, which never suppresses a
    /// sibling test's emission.
    #[derive(Clone, Default)]
    struct WarnCapture(Arc<Mutex<Vec<String>>>);

    impl WarnCapture {
        fn messages(&self) -> Vec<String> {
            self.0.lock().unwrap_or_else(|e| e.into_inner()).clone()
        }
    }

    impl<S: tracing::Subscriber> Layer<S> for WarnCapture {
        fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
            if *event.metadata().level() != tracing::Level::WARN {
                return;
            }
            struct V<'a>(&'a mut String);
            impl Visit for V<'_> {
                fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
                    if field.name() == "message" {
                        *self.0 = format!("{value:?}");
                    }
                }
            }
            let mut msg = String::new();
            event.record(&mut V(&mut msg));
            self.0.lock().unwrap_or_else(|e| e.into_inner()).push(msg);
        }
    }

    /// Run `body` with a WARN capture installed as the thread-local
    /// default subscriber. `body` must emit synchronously (no
    /// `.await`) for the per-callsite interest cache to be reliable —
    /// `compute_initial_staging_entries` is a pure sync function, so
    /// this holds. No global filter layer (the `WarnCapture` layer
    /// self-filters in `on_event`) so the install caches
    /// `Interest::always` and never poisons a sibling test's
    /// callsites.
    fn with_warn_capture<R>(body: impl FnOnce() -> R) -> (R, Vec<String>) {
        let cap = WarnCapture::default();
        let subscriber = Registry::default().with(cap.clone());
        let out = tracing::subscriber::with_default(subscriber, body);
        (out, cap.messages())
    }

    /// Write a real file at `<root>/<rel>` so the in-tree content-hash
    /// read in `compute_initial_staging_entries` succeeds.
    fn write_in_tree(root: &std::path::Path, rel: &str, bytes: &[u8]) -> std::path::PathBuf {
        let p = root.join(rel);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(&p, bytes).unwrap();
        p
    }

    /// Out-of-tree (absolute, NOT under `--source`): no staging entry
    /// is emitted (matches the upload skip) and a warn fires.
    ///
    /// The out-of-tree file is a REAL readable file in a second
    /// tempdir outside `--source` — the faithful silent-strand shape:
    /// the file exists on the primary (so the pre-fix fallback-keep
    /// would happily read it and emit an entry) but is absent on the
    /// secondary, since the upload never staged it. The fix must skip
    /// it on the read-succeeds path, not merely error on a missing
    /// file.
    #[test]
    fn out_of_tree_emits_no_entry_and_warns() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        // Readable file OUTSIDE the --source root.
        let outside = tempfile::tempdir().unwrap();
        let out_file = outside.path().join("out_of_tree.bin");
        std::fs::write(&out_file, b"x").unwrap();

        let binaries = vec![make_binary(&out_file)];
        let ids = vec!["secondary-0".to_string()];

        let (entries, warns) =
            with_warn_capture(|| compute_initial_staging_entries(&binaries, &ids, root).unwrap());

        assert!(
            entries.is_empty(),
            "out-of-tree binary must NOT yield a staging entry (got {entries:?})"
        );
        assert!(
            warns.iter().any(|m| m.contains("not under --source root")),
            "a skip warn must fire for the out-of-tree binary (got {warns:?})"
        );
    }

    /// In-tree (relative + absolute-under-root): both still yield one
    /// entry per (binary × secondary), proving no regression to the
    /// working path. The wire `src_path`/`dest_path` is the
    /// `<rel>` tail (verbatim relative, or stripped tail for absolute).
    #[test]
    fn in_tree_still_emits_entries() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        write_in_tree(root, "a/rel.bin", b"r");
        let abs = write_in_tree(root, "abs.bin", b"a");

        let binaries = vec![make_binary("a/rel.bin"), make_binary(&abs)];
        let ids = vec!["secondary-0".to_string()];

        let entries = compute_initial_staging_entries(&binaries, &ids, root).unwrap();

        assert_eq!(entries.len(), 2, "both in-tree binaries must stage");
        let rels: Vec<&str> = entries.iter().map(|(_, _, _, src, _)| src.as_str()).collect();
        assert!(rels.contains(&"a/rel.bin"), "relative tail preserved (got {rels:?})");
        assert!(rels.contains(&"abs.bin"), "absolute-under-root strips to tail (got {rels:?})");
    }

    /// Mixed input: staging emits an entry for EXACTLY the binaries
    /// the stageability predicate (`resolve_against_root(...).relative
    /// .is_some()`) accepts — i.e. exactly the set the upload uploads.
    /// This locks the two sibling functions' agreement at the shared
    /// predicate so they can't silently diverge again.
    #[test]
    fn staging_stageable_set_matches_predicate() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        write_in_tree(root, "in/rel.bin", b"r");
        let in_abs = write_in_tree(root, "in_abs.bin", b"a");
        // Two REAL readable out-of-tree files in a second dir, so the
        // only reason they're excluded is the stageability predicate,
        // not an unreadable-file error.
        let outside = tempfile::tempdir().unwrap();
        let out_x = outside.path().join("x.bin");
        let out_y = outside.path().join("y.bin");
        std::fs::write(&out_x, b"x").unwrap();
        std::fs::write(&out_y, b"y").unwrap();

        let binaries = vec![
            make_binary("in/rel.bin"), // relative, in-tree
            make_binary(&in_abs),      // absolute, in-tree
            make_binary(&out_x),       // absolute, out-of-tree (readable)
            make_binary(&out_y),       // absolute, out-of-tree (readable)
        ];
        let ids = vec!["secondary-0".to_string()];

        // The shared predicate's verdict for each binary.
        let stageable_by_predicate: Vec<bool> = binaries
            .iter()
            .map(|b| resolve_against_root(&b.path, root).relative.is_some())
            .collect();
        let expected_stageable = stageable_by_predicate.iter().filter(|s| **s).count();

        let entries = compute_initial_staging_entries(&binaries, &ids, root).unwrap();
        // One id, so #entries == #stageable binaries.
        assert_eq!(
            entries.len(),
            expected_stageable,
            "staging must emit an entry for exactly the predicate-stageable binaries \
             (predicate={stageable_by_predicate:?}, entries={entries:?})"
        );
        // And specifically: the two in-tree binaries staged, neither
        // out-of-tree one did.
        let out_prefix = outside.path().to_string_lossy().into_owned();
        let staged_srcs: Vec<&str> = entries.iter().map(|(_, _, _, s, _)| s.as_str()).collect();
        assert!(staged_srcs.contains(&"in/rel.bin"));
        assert!(staged_srcs.contains(&"in_abs.bin"));
        assert!(
            !staged_srcs.iter().any(|s| s.starts_with(&out_prefix)),
            "no out-of-tree binary may appear as a staging entry (got {staged_srcs:?})"
        );
    }

    /// Revert-check: pin the corrected skip so a regression to the
    /// pre-fix fallback-keep (which DID emit an entry carrying the
    /// full absolute path as `rel`) fails here. The out-of-tree file
    /// is REAL+readable, so under fallback-keep the content-hash read
    /// succeeds and an entry IS emitted with the absolute path as
    /// src/dest — exactly the silent-strand shape this test forbids.
    #[test]
    fn revert_check_no_fallback_keep_of_absolute_path() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let outside = tempfile::tempdir().unwrap();
        let out_file = outside.path().join("foo.bin");
        std::fs::write(&out_file, b"z").unwrap();
        let out_str = out_file.to_string_lossy().into_owned();

        let binaries = vec![make_binary(&out_file)];
        let ids = vec!["secondary-0".to_string()];

        let entries: Vec<StagingEntry> =
            compute_initial_staging_entries(&binaries, &ids, root).unwrap();

        assert!(
            !entries
                .iter()
                .any(|(_, _, _, src, dest)| src == &out_str || dest == &out_str),
            "pre-fix fallback-keep would have emitted an entry with the absolute \
             path as src/dest; the fix must skip it (got {entries:?})"
        );
        assert!(
            entries.is_empty(),
            "out-of-tree binary yields zero entries post-fix (got {entries:?})"
        );
    }
}
