//! Framework file-staging via setup tasks — the NEW staging path selected by
//! the `--stage-via-setup-tasks` flag (P3/P4 of #489) PLUS the per-work-task
//! required-files attach (#336 P2).
//!
//! ## The ONE concern
//! Turn file-staging INTENT into setup-task structure: derive per-file SETUP
//! tasks and make the file-backed work tasks depend on them (`TaskDep`). This
//! module produces ONLY the structure — the extra `TaskInfo`s, the dep edges,
//! and the set of setup-task hashes that are ALREADY satisfied (pre-staged
//! files). It does NOT seed the ledger, does NOT execute anything, and does
//! NOT resolve dependencies — those are the existing originator
//! (`originate_cold_seed` / `discover_on_promotion`), the existing
//! `ClusterMutation::SetupCompleted` apply arm, and the existing `PendingPool`
//! dep machine respectively.
//!
//! ## Two composed transforms behind one entry point
//! There are TWO distinct staging intents, composed (not conflated) inside the
//! single [`augment_batch_for_staging`] entry the originators call:
//!   1. **Required-files attach (#336 P2)** — `attach_required_files`. A WORK
//!      task declares `required_files` (a set of [`UploadFileRef`]s it needs
//!      UPLOADED before it runs); the transform collects those across the
//!      WHOLE batch, DEDUPS by `(source, dest)` so each unique file becomes
//!      EXACTLY ONE upload setup task (carrying `upload_file` so P1's
//!      upload-action executor transfers it), and wires each work task's
//!      `task_depends_on` to the setup tasks for ITS OWN files. A file shared
//!      by an arbitrary SUBSET of work tasks → ONE upload + a dep from exactly
//!      that subset; there is NO universal-common slot (the shared subset is
//!      whatever happens to list the file). This transform is DATA-driven
//!      (the presence of `required_files`), NOT flag-gated — a consumer's
//!      `files=` works regardless of `--stage-via-setup-tasks`. The upload
//!      setup tasks are NOT pre-succeeded (they EXECUTE the upload).
//!   2. **Flagged mode-2 pre-staged staging (#489 P3)** — the
//!      [`StagingStrategy::SetupTasks`] pass. Each remaining file-backed work
//!      task gets a per-file PRE-SUCCEEDED setup task (the file is already on
//!      the cluster; the task never executes). Flag-gated; identity transform
//!      when off.
//!
//! Both passes emit the SAME [`StagingAugmentation`] shape, so the originators
//! see one call. The required-files attach runs FIRST (its deps + setup tasks
//! land in the batch), then the flag pass runs on the result.
//!
//! ## Module boundary (CLAUDE.md design-first)
//!   * Owner: this module owns "what setup-task augmentation does the
//!     framework's flagged staging produce for a given batch + strategy".
//!   * API the callers see: ONE pure function
//!     [`augment_batch_for_staging`] taking the discovery `marked_batch`
//!     (`Vec<(TaskInfo, skipped)>`) + a [`StagingStrategy`] and returning a
//!     [`StagingAugmentation`] — the augmented batch (work tasks with deps
//!     wired + the injected setup tasks) plus the `pre_succeeded` hashes the
//!     originator must transition `Pending → SetupCompleted`. The caller fans
//!     `TaskAdded` over the augmented batch through its EXISTING fan-out and
//!     emits `SetupCompleted` for `pre_succeeded` exactly as it already emits
//!     `TaskSkippedAlreadyDone` via `skip_transitions` — no `if flag` inside
//!     the originator beyond the one strategy-dispatch call.
//!   * What crosses NO boundary: dependency resolution. The work task's new
//!     `TaskDep` references the setup task's `(phase_id, task_id)`; the
//!     setup task is present in the SAME batch (so `partition_ingest` /
//!     `extend` see the dep resolve by presence) and is pre-succeeded in the
//!     ledger (so hydrate routes its `task_id` into `completed_task_ids` and
//!     the dependent pre-resolves to `Pending` — never `Blocked`). This is
//!     EXACTLY the existing `SetupCompleted`/`TaskDep` machinery; this module
//!     re-implements none of it.
//!
//! ## Why this is the #488-free path
//! A pre-staged file becomes a pre-SUCCEEDED setup task in the REPLICATED
//! ledger. Any primary — original, relocated, or promoted — reads the ledger,
//! sees the setup task `SetupCompleted`, and resolves the dependent's
//! `TaskDep`. There is no `pre_staged_mode` flag for a relocated primary to
//! mis-stamp (the #488 defect of the OLD path): the ledger IS the source of
//! truth. The OLD default path (flag off) is untouched and retains its #488
//! limitation by design (owner correction #4).

use std::collections::HashMap;
use std::path::PathBuf;

use dynrunner_core::{Identifier, TaskDep, TaskInfo, TaskKind, UploadFileRef};

/// Which framework file-staging strategy the run uses. The SELECTOR (flag
/// off → old path; flag on → this new path) lives at run construction
/// (`PrimaryConfig`), driven by the `--stage-via-setup-tasks` CLI flag.
///
/// `Disabled` is the default: the framework's OLD staging
/// (`maybe_auto_stage_initial` / `StageFile`) runs and this module produces
/// NO augmentation — the cold seed is byte-for-byte what it was before.
///
/// `SetupTasks` is the new flagged path. It is currently ONLY supported with a
/// pre-staged corpus (`--source-already-staged`, mode-2): the file is already
/// on the cluster filesystem at run start, so each file-backed work task gets
/// a per-file SETUP task seeded PRE-SUCCEEDED, gated by `TaskDep`. The
/// pre-succeeded stamp is honest because the file is already present — the
/// setup task never executes.
///
/// Mode-1 (framework-upload / files-on-submitter) staging via setup tasks is
/// NOT yet wired: the legacy `StageFile` physical-resolution path (which moves
/// files to each secondary) is suppressed on this flag path but its replacement
/// is not implemented. The CLI guard (`validate_parsed_args`) rejects the
/// `--stage-via-setup-tasks` + no-`--source-already-staged` combination at
/// startup with a clear error, so this code path is never reached for mode-1.
///
/// In mode-2 the setup task is pre-succeeded in the replicated ledger, so any
/// primary (original / relocated / promoted) resolves the dependent's dep from
/// the ledger — the #488-free path. When the flag is on, the OLD StageFile
/// fan-out (`maybe_auto_stage_initial`) does NOT run (no double-staging).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StagingStrategy {
    /// The flag is off. The framework uses the OLD staging path; this module
    /// is inert (the augmentation is the identity transform — the batch is
    /// returned unchanged, no setup tasks, no deps). The default.
    #[default]
    Disabled,
    /// The flag is on. Each file-backed work task gets a per-file SETUP task
    /// seeded PRE-SUCCEEDED (no in-process execution — the upload, if any,
    /// already ran pre-`run()`), and the work task is gated on it via
    /// `TaskDep`. The single ledger model for both upload modes.
    SetupTasks,
}

/// The setup-task augmentation [`augment_batch_for_staging`] produces for one
/// batch under one [`StagingStrategy`].
///
/// `batch` is the FULL augmented batch the originator fans `TaskAdded` over:
/// the original work tasks (now carrying their new staging `TaskDep`) PLUS the
/// injected per-file setup tasks. `pre_succeeded` is the set of injected
/// setup-task content hashes the originator must additionally transition
/// `Pending → SetupCompleted` in the SAME seed pass (the parallel of
/// `skip_transitions`' `SkippedAlreadyDone` set).
pub struct StagingAugmentation<I: Identifier> {
    /// The augmented `marked_batch`: every original entry (work tasks, with
    /// their staging dep wired) plus one `(setup_task, false)` per injected
    /// per-file setup task. The `skipped_already_done` bit is `false` for an
    /// injected setup task — a setup task is never a discovery skip; its
    /// pre-succeeded terminal is carried by `pre_succeeded`, not the skip set.
    pub batch: Vec<(TaskInfo<I>, bool)>,
    /// Content hashes of the injected setup tasks that are ALREADY satisfied
    /// (pre-staged). The originator emits `ClusterMutation::SetupCompleted`
    /// for each AFTER the `TaskAdded` fan-out seeds them `Pending` — the same
    /// seed-then-terminal ordering `skip_transitions` uses.
    pub pre_succeeded: Vec<String>,
}

/// Prefix for a staging setup task's synthetic `task_id`, namespaced so it
/// can never collide with a consumer-supplied work-task id. The dependent
/// work task references this id in its `task_depends_on`.
const STAGE_TASK_ID_PREFIX: &str = "__framework_stage__";

/// Prefix for a staging setup task's synthetic `path`. `compute_task_hash`'s
/// recipe is `{phase_id, path, identifier}`; the stage task reuses the work
/// task's `phase_id` + `identifier` (the `Identifier` trait offers no
/// constructor, so a synthetic identifier cannot be minted generically — it
/// is cloned from the work task), and uses THIS distinct synthetic path so
/// the stage task's hash never collides with the work task's.
const STAGE_PATH_PREFIX: &str = "__framework_stage__";

/// Derive the per-file STAGE setup task for one file-backed work task.
///
/// Reuses the work task's `phase_id` (same-phase dep — cross-phase-valid, but
/// the file belongs to the work task's phase) and its `identifier` (the only
/// way to obtain an `I` value without a constructor the trait does not
/// provide). The distinct hash comes from the synthetic `path`; the distinct
/// `(phase_id, task_id)` identity comes from the synthetic `task_id`.
fn stage_task_for<I: Identifier>(work: &TaskInfo<I>) -> TaskInfo<I> {
    let stage_task_id = format!("{STAGE_TASK_ID_PREFIX}/{}", work.task_id);
    let stage_path = PathBuf::from(format!(
        "{STAGE_PATH_PREFIX}/{}",
        work.path.to_string_lossy()
    ));
    TaskInfo {
        path: stage_path,
        size: 0,
        identifier: work.identifier.clone(),
        phase_id: work.phase_id.clone(),
        type_id: work.type_id.clone(),
        // The framework's auto-staging executor is the submitter — the
        // source-owning member for files staged from the submitter's tree.
        // For the pre-staged (mode-2) case the affinity is moot (the task is seeded
        // pre-succeeded and never executes), but it carries the same affinity
        // the upload path will use so the two strategies agree structurally.
        kind: TaskKind::Setup,
        setup_affinity: Some(dynrunner_core::SETUP_NODE_ID.to_string()),
        // No upload-file ref: this auto-staging stage task is the PRE-STAGED
        // (mode-2) gate — the file is already on the cluster, so the task is
        // seeded pre-succeeded and its action never runs (the #489 no-op
        // gate). #336 P2 owns the upload variant (attach an `UploadFileRef`
        // here when the file is NOT yet on the cluster); P1 only delivers the
        // executor's upload-action path such a ref would drive.
        upload_file: None,
        required_files: None,
        affinity_id: None,
        payload: serde_json::Value::Null,
        task_id: stage_task_id,
        task_depends_on: Vec::new(),
        preferred_secondaries: Default::default(),
        preferred_version: Default::default(),
        resolved_path: None,
    }
}

/// The dep edge a work task gains on its stage setup task.
fn stage_dep<I: Identifier>(stage: &TaskInfo<I>) -> TaskDep {
    TaskDep {
        task_id: stage.task_id.clone(),
        phase_id: stage.phase_id.clone(),
        // The work task does not read the stage task's published outputs — it
        // reads the staged FILE (on disk / in the bind-mount). The dep is a
        // pure ordering/readiness gate, so no output inheritance.
        inherit_outputs: false,
        // Resolved at TaskAdded origination (the broadcast stamp); the
        // augmentation builds the un-resolved dep here.
        def_id: None,
    }
}

/// Augment a discovery `marked_batch` with the framework's setup-task staging
/// structure — composing the two staging transforms this module owns.
///
/// The composition (in order):
///   1. **Required-files attach (#336 P2)** — [`attach_required_files`] turns
///      each work task's declared `required_files` into DEDUPED per-file
///      upload setup tasks + the work task's deps. DATA-driven (runs whenever
///      a work task declares files), NOT gated on `strategy`.
///   2. **Flagged mode-2 staging (#489 P3)** — driven by `strategy`:
///      * [`StagingStrategy::Disabled`] — the IDENTITY transform for THIS
///        pass: the (already required-files-augmented) batch passes through
///        unchanged and no additional `pre_succeeded` hashes are produced.
///      * [`StagingStrategy::SetupTasks`] — each remaining file-backed work
///        task gains a per-file PRE-SUCCEEDED stage setup task + a `TaskDep`;
///        every injected stage task is reported `pre_succeeded`. A non-worker
///        task or a discovery-skipped work task is left untouched.
///
/// The two passes are SEPARATE transforms (one upload-driven, one
/// pre-staged-gate-driven) sharing the [`StagingAugmentation`] shape, so the
/// originators (`originate_cold_seed` / `discover_on_promotion`) see ONE call.
///
/// Pure (`&`-only inputs, owned output): callable from both originators
/// exactly as `skip_transitions` is, with no coordinator state touched.
pub fn augment_batch_for_staging<I: Identifier>(
    marked_batch: Vec<(TaskInfo<I>, bool)>,
    strategy: StagingStrategy,
) -> StagingAugmentation<I> {
    // Pass 1: required-files attach (#336 P2). Deduped upload setup tasks +
    // per-work-task deps. A batch with no declared `required_files` passes
    // through unchanged (the common case), so this is a no-op for every
    // pre-#336 consumer.
    let marked_batch = attach_required_files(marked_batch);

    // Pass 2: flagged mode-2 pre-staged staging (#489 P3).
    match strategy {
        StagingStrategy::Disabled => StagingAugmentation {
            batch: marked_batch,
            pre_succeeded: Vec::new(),
        },
        StagingStrategy::SetupTasks => {
            let mut batch: Vec<(TaskInfo<I>, bool)> =
                Vec::with_capacity(marked_batch.len() * 2);
            let mut pre_succeeded: Vec<String> = Vec::new();
            for (mut work, skipped) in marked_batch {
                // Only ordinary work that will actually dispatch needs a
                // staging gate. A setup task (the consumer's own, the P2
                // upload tasks injected in pass 1, or one we inject here) is
                // never gated on a stage task, and a discovery-skipped task
                // never dispatches.
                if !work.kind.is_worker_assignable() || skipped {
                    batch.push((work, skipped));
                    continue;
                }
                let stage = stage_task_for(&work);
                let stage_hash = dynrunner_core::compute_task_hash(&stage);
                work.task_depends_on.push(stage_dep(&stage));
                pre_succeeded.push(stage_hash);
                // Seed the stage task FIRST so the dependent's dep resolves by
                // presence in the same batch (partition_ingest / extend), then
                // the work task carrying the new dep.
                batch.push((stage, false));
                batch.push((work, false));
            }
            StagingAugmentation {
                batch,
                pre_succeeded,
            }
        }
    }
}

// ── #336 P2: per-work-task required-files attach ────────────────────────────

/// Synthetic-id / path prefix for a #336 P2 per-FILE UPLOAD setup task. The
/// suffix is the file's `(source, dest)` dedup key (see [`upload_file_key`]),
/// so two work tasks that list the same file derive the SAME setup-task id and
/// hence the SAME `TaskDep` — that identity-by-construction is exactly how the
/// dedup makes N dependents converge on ONE upload setup task (the pool's dep
/// machine resolves both deps to the single seeded task).
const UPLOAD_TASK_ID_PREFIX: &str = "__framework_upload__";

/// The `(source, dest)` dedup key for an [`UploadFileRef`], rendered as a
/// single stable string. TWO files are "the same upload" iff this key matches
/// — a file at the same `source` with the same explicit `dest` (or both
/// derived, `dest = None`) under the same `root` is uploaded ONCE no matter how
/// many work tasks list it. The key embeds ALL THREE coordinates because the
/// same `source` placed at two different explicit `dest`s — or under two
/// different mount roots (#644) — is two distinct uploads.
fn upload_file_key(file: &UploadFileRef) -> String {
    let root = match file.root {
        dynrunner_core::UploadRoot::Source => "src",
        dynrunner_core::UploadRoot::Output => "out",
    };
    match &file.dest {
        Some(dest) => format!("{root}\u{1f}{}\u{1f}{}", file.source.display(), dest.display()),
        None => format!("{root}\u{1f}{}\u{1f}", file.source.display()),
    }
}

/// Derive the per-file UPLOAD setup task for one unique attached file.
///
/// Mirrors [`stage_task_for`]'s identifier/phase borrowing (the `Identifier`
/// trait offers no constructor, so the synthetic identifier is CLONED from a
/// work task that lists the file; the distinct content hash comes from the
/// synthetic `path`). UNLIKE the mode-2 stage task, this one carries the
/// `upload_file` ref so P1's upload-action executor actually transfers the
/// file — it is NOT pre-succeeded.
///
/// `phase` is the phase of the FIRST work task (in batch order) that lists the
/// file. Every dependent's `TaskDep` names this exact phase, so a cross-phase
/// dependent resolves correctly (deps are cross-phase-valid; the phase barrier
/// guarantees an earlier-phase upload completes before a later-phase
/// dependent). In the realistic consumer case all dependents share the file's
/// phase, so this is unambiguous.
fn upload_task_for<I: Identifier>(
    file: &UploadFileRef,
    identifier: &I,
    phase: &dynrunner_core::PhaseId,
    type_id: &dynrunner_core::TypeId,
) -> TaskInfo<I> {
    let key = upload_file_key(file);
    let upload_task_id = format!("{UPLOAD_TASK_ID_PREFIX}/{key}");
    let upload_path = PathBuf::from(format!("{UPLOAD_TASK_ID_PREFIX}/{key}"));
    TaskInfo {
        path: upload_path,
        size: 0,
        identifier: identifier.clone(),
        phase_id: phase.clone(),
        type_id: type_id.clone(),
        kind: TaskKind::Setup,
        // Source-owner affinity: the submitter / observer physically holds the
        // file. Same affinity the #489 mode-2 stage task carries, so the two
        // staging transforms agree structurally.
        setup_affinity: Some(dynrunner_core::SETUP_NODE_ID.to_string()),
        // The action payload: P1's upload-action executor transfers THIS file.
        // Distinct from the mode-2 stage task (which carries `None` and is
        // pre-succeeded) — this task EXECUTES the upload.
        upload_file: Some(Box::new(file.clone())),
        affinity_id: None,
        payload: serde_json::Value::Null,
        task_id: upload_task_id,
        task_depends_on: Vec::new(),
        // An upload setup task itself declares no required files (its file is
        // the `upload_file` action payload, not a prerequisite).
        required_files: None,
        preferred_secondaries: Default::default(),
        preferred_version: Default::default(),
        resolved_path: None,
    }
}

/// The dep edge a work task gains on the upload setup task for one of its
/// required files. Pure ordering/readiness gate (the work task reads the
/// uploaded FILE on disk, not the setup task's published outputs), so no
/// output inheritance — mirrors [`stage_dep`].
fn upload_dep<I: Identifier>(upload: &TaskInfo<I>) -> TaskDep {
    TaskDep {
        task_id: upload.task_id.clone(),
        phase_id: upload.phase_id.clone(),
        inherit_outputs: false,
        // Resolved at TaskAdded origination (the broadcast stamp).
        def_id: None,
    }
}

/// Attach each work task's declared `required_files` as DEDUPED per-file
/// upload setup tasks + per-work-task deps (#336 P2).
///
/// THE DEDUP IS THE KEY: `required_files` are collected across the WHOLE batch
/// and deduped by `(source, dest)` ([`upload_file_key`]), so each unique file
/// becomes EXACTLY ONE upload setup task. Each work task's `task_depends_on`
/// gains one dep per its OWN files (pointing at that file's single upload
/// task). A file shared by an arbitrary SUBSET of work tasks → ONE upload +
/// a dep from exactly that subset; there is NO universal-common slot — the
/// shared subset is whatever happens to list the file.
///
/// A work task with empty `required_files` passes through UNCHANGED (the
/// common case — every pre-#336 consumer): no upload tasks, no deps, the
/// bulk-walk / mode-2 path intact. A non-worker task (a consumer's own setup
/// task, or a setup task injected by another pass) and a discovery-skipped
/// work task contribute no uploads — neither dispatches as ordinary work, so a
/// gate would only add inert ledger entries; we still preserve them verbatim.
///
/// Pure (`marked_batch` in, augmented `marked_batch` out): a sibling transform
/// to the flagged staging pass, composed ahead of it in
/// [`augment_batch_for_staging`].
fn attach_required_files<I: Identifier>(
    marked_batch: Vec<(TaskInfo<I>, bool)>,
) -> Vec<(TaskInfo<I>, bool)> {
    // First pass: collect the unique upload setup tasks, keyed by the file
    // dedup key, in first-seen order (so the batch is deterministic). Only a
    // dispatchable work task contributes — a non-worker / skipped task's files
    // (if any) are not gated. The map value is the derived setup task; the
    // ordered key list preserves first-seen order for the injected batch.
    let mut upload_tasks: HashMap<String, TaskInfo<I>> = HashMap::new();
    let mut upload_order: Vec<String> = Vec::new();
    for (work, skipped) in &marked_batch {
        if !work.kind.is_worker_assignable() || *skipped {
            continue;
        }
        for file in work.required_files() {
            let key = upload_file_key(file);
            if !upload_tasks.contains_key(&key) {
                // The FIRST work task (in batch order) that lists this file
                // donates its identifier / phase / type to the upload task.
                let upload = upload_task_for(file, &work.identifier, &work.phase_id, &work.type_id);
                upload_tasks.insert(key.clone(), upload);
                upload_order.push(key);
            }
        }
    }

    if upload_order.is_empty() {
        // No work task declared any files: identity transform (the common
        // pre-#336 case). The batch is returned byte-for-byte.
        return marked_batch;
    }

    // Second pass: build the augmented batch. Seed the deduped upload setup
    // tasks FIRST so each dependent's `TaskDep` resolves by presence in the
    // same batch (partition_ingest / extend), then the work tasks carrying
    // their new deps. A work task's `required_files` map deterministically to
    // its deps via the SAME `upload_file_key`, so a file shared across N tasks
    // wires N deps onto the ONE seeded upload task.
    let mut batch: Vec<(TaskInfo<I>, bool)> =
        Vec::with_capacity(marked_batch.len() + upload_order.len());
    for key in &upload_order {
        // `false`: an injected setup task is never a discovery skip.
        batch.push((upload_tasks[key].clone(), false));
    }
    for (mut work, skipped) in marked_batch {
        if work.kind.is_worker_assignable() && !skipped && !work.required_files().is_empty() {
            // Dedup the work task's OWN files too (a task that lists the same
            // file twice gets one dep), preserving first-seen order. Collect
            // the deduped keys into an owned list FIRST so the immutable
            // `required_files()` borrow ends before the mutable
            // `task_depends_on` push.
            let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
            let keys: Vec<String> = work
                .required_files()
                .iter()
                .map(upload_file_key)
                .filter(|key| seen.insert(key.clone()))
                .collect();
            for key in keys {
                work.task_depends_on.push(upload_dep(&upload_tasks[&key]));
            }
        }
        batch.push((work, skipped));
    }
    batch
}

#[cfg(test)]
mod tests {
    use super::*;
    use dynrunner_core::{PhaseId, RunnerIdentifier, TaskKind, TypeId};

    fn work_task(task_id: &str, path: &str) -> TaskInfo<RunnerIdentifier> {
        TaskInfo {
            path: PathBuf::from(path),
            size: 1,
            identifier: RunnerIdentifier::from(task_id),
            phase_id: PhaseId::from("p0"),
            type_id: TypeId::from("t0"),
            kind: TaskKind::Work,
            setup_affinity: None,
            affinity_id: None,
            payload: serde_json::Value::Null,
            task_id: task_id.into(),
            task_depends_on: Vec::new(),
            preferred_secondaries: Default::default(),
            preferred_version: Default::default(),
            upload_file: None,
            required_files: None,
            resolved_path: None,
        }
    }

    #[test]
    fn disabled_is_identity_transform() {
        // Flag off: the batch is returned byte-for-byte, no setup tasks, no
        // pre-succeeded hashes. This is the regression guard that the new
        // module contributes NOTHING to the old cold seed.
        let batch = vec![
            (work_task("a", "/src/a"), false),
            (work_task("b", "/src/b"), true),
        ];
        let aug = augment_batch_for_staging(batch.clone(), StagingStrategy::Disabled);
        assert!(aug.pre_succeeded.is_empty());
        assert_eq!(aug.batch.len(), batch.len());
        for ((got, gs), (want, ws)) in aug.batch.iter().zip(batch.iter()) {
            assert_eq!(got.task_id, want.task_id);
            assert_eq!(got.task_depends_on.len(), 0, "no dep wired when disabled");
            assert_eq!(gs, ws);
            assert_eq!(got.kind, want.kind);
        }
    }

    #[test]
    fn setuptasks_injects_per_file_setup_task_and_wires_dep() {
        // Flag on, pre-staged: one work task -> one injected pre-succeeded
        // setup task + a dep edge on it.
        let batch = vec![(work_task("a", "/src/a"), false)];
        let aug = augment_batch_for_staging(batch, StagingStrategy::SetupTasks);

        // Two entries: the injected setup task and the work task.
        assert_eq!(aug.batch.len(), 2);
        let setup = aug
            .batch
            .iter()
            .find(|(t, _)| t.kind.is_setup())
            .expect("an injected setup task");
        let work = aug
            .batch
            .iter()
            .find(|(t, _)| t.kind.is_worker_assignable())
            .expect("the original work task");

        // Setup task: Setup kind, submitter affinity, namespaced id, no deps.
        assert_eq!(setup.0.kind, TaskKind::Setup);
        assert_eq!(
            setup.0.setup_affinity.as_deref(),
            Some(dynrunner_core::SETUP_NODE_ID)
        );
        assert!(setup.0.task_id.starts_with(STAGE_TASK_ID_PREFIX));
        assert!(setup.0.task_depends_on.is_empty());

        // Its hash is reported pre-succeeded.
        let setup_hash = dynrunner_core::compute_task_hash(&setup.0);
        assert_eq!(aug.pre_succeeded, vec![setup_hash]);

        // Work task: now depends on the setup task by full identity.
        assert_eq!(work.0.task_depends_on.len(), 1);
        let dep = &work.0.task_depends_on[0];
        assert_eq!(dep.task_id, setup.0.task_id);
        assert_eq!(dep.phase_id, setup.0.phase_id);
        assert!(!dep.inherit_outputs);
    }

    #[test]
    fn stage_task_hash_distinct_from_work_task_hash() {
        // The synthetic path makes the stage task's content hash distinct from
        // its work task's, so the two never collide in the ledger even though
        // they share phase + identifier (the identifier cannot be minted
        // generically, so it is cloned).
        let w = work_task("a", "/src/a");
        let s = stage_task_for(&w);
        assert_ne!(
            dynrunner_core::compute_task_hash(&w),
            dynrunner_core::compute_task_hash(&s),
            "stage task hash must not collide with the work task hash"
        );
    }

    #[test]
    fn setuptasks_skips_already_done_and_non_worker_tasks() {
        // A discovery-skipped work task and a non-worker (setup/producer) task
        // get NO staging gate: neither dispatches as ordinary work, so a stage
        // dep would only add inert ledger entries.
        let mut producer = work_task("p", "/src/p");
        producer.kind = TaskKind::Setup; // a non-worker task in the batch
        let batch = vec![
            (work_task("skipped", "/src/s"), true),
            (producer, false),
            (work_task("live", "/src/l"), false),
        ];
        let aug = augment_batch_for_staging(batch, StagingStrategy::SetupTasks);

        // Only the one live work task is augmented: it + its stage task.
        assert_eq!(aug.pre_succeeded.len(), 1);
        let staged_for: Vec<&str> = aug
            .batch
            .iter()
            .filter(|(t, _)| t.kind.is_setup() && t.task_id.starts_with(STAGE_TASK_ID_PREFIX))
            .map(|(t, _)| t.task_id.as_str())
            .collect();
        assert_eq!(staged_for.len(), 1);
        assert!(staged_for[0].contains("live"));

        // The skipped task keeps its skip bit and gains no dep.
        let skipped = aug
            .batch
            .iter()
            .find(|(t, _)| t.task_id == "skipped")
            .unwrap();
        assert!(skipped.1, "skip bit preserved");
        assert!(skipped.0.task_depends_on.is_empty());
    }

    // ── #336 P2: per-work-task required-files attach (deduped) ──────────────

    /// A work task declaring `required_files` from a list of `(source, dest)`
    /// pairs (`dest = None` ⇒ derived destination).
    fn work_with_files(
        task_id: &str,
        files: &[(&str, Option<&str>)],
    ) -> TaskInfo<RunnerIdentifier> {
        let mut t = work_task(task_id, &format!("/src/{task_id}"));
        t.required_files = dynrunner_core::required_files_storage(
            files
                .iter()
                .map(|(source, dest)| UploadFileRef {
                    source: PathBuf::from(source),
                    dest: dest.map(PathBuf::from),
                    root: dynrunner_core::UploadRoot::Source,
                })
                .collect(),
        );
        t
    }

    /// All the injected UPLOAD setup tasks in an augmented batch (the #336 P2
    /// ones — `Setup` kind carrying an `upload_file` ref).
    fn upload_tasks_in(
        aug: &StagingAugmentation<RunnerIdentifier>,
    ) -> Vec<&TaskInfo<RunnerIdentifier>> {
        aug.batch
            .iter()
            .filter(|(t, _)| {
                t.kind.is_setup()
                    && t.upload_file.is_some()
                    && t.task_id.starts_with(UPLOAD_TASK_ID_PREFIX)
            })
            .map(|(t, _)| t)
            .collect()
    }

    /// The work task with `task_id` in an augmented batch.
    fn work_in<'a>(
        aug: &'a StagingAugmentation<RunnerIdentifier>,
        task_id: &str,
    ) -> &'a TaskInfo<RunnerIdentifier> {
        aug.batch
            .iter()
            .find(|(t, _)| t.kind.is_worker_assignable() && t.task_id == task_id)
            .map(|(t, _)| t)
            .unwrap_or_else(|| panic!("work task '{task_id}' not in augmented batch"))
    }

    #[test]
    fn no_required_files_is_unchanged() {
        // A batch with NO declared files is byte-for-byte unchanged: no upload
        // setup tasks, no deps, regardless of the staging strategy. The
        // bulk-walk / no-op path is intact — this is the regression guard that
        // the P2 attach contributes NOTHING when files aren't declared.
        let batch = vec![
            (work_task("a", "/src/a"), false),
            (work_task("b", "/src/b"), false),
        ];
        let aug = augment_batch_for_staging(batch.clone(), StagingStrategy::Disabled);
        assert!(upload_tasks_in(&aug).is_empty(), "no upload setup tasks");
        assert_eq!(aug.batch.len(), batch.len());
        for ((got, _), (want, _)) in aug.batch.iter().zip(batch.iter()) {
            assert_eq!(got.task_id, want.task_id);
            assert!(
                got.task_depends_on.is_empty(),
                "no dep wired when no files declared"
            );
        }
    }

    #[test]
    fn two_files_produce_two_upload_tasks_and_two_deps() {
        // A work task with required_files=[a, b] -> 2 upload setup tasks + 2
        // deps; the work task depends on BOTH (it dispatches only after both
        // uploads complete).
        let batch = vec![(
            work_with_files("build", &[("/src/a", None), ("/src/b", None)]),
            false,
        )];
        let aug = augment_batch_for_staging(batch, StagingStrategy::Disabled);

        let uploads = upload_tasks_in(&aug);
        assert_eq!(uploads.len(), 2, "one upload setup task per unique file");
        // Each upload task carries its file on `upload_file`, source-owner
        // affinity, Setup kind, and is NOT pre-succeeded (it executes).
        for u in &uploads {
            assert_eq!(u.kind, TaskKind::Setup);
            assert_eq!(u.setup_affinity.as_deref(), Some(dynrunner_core::SETUP_NODE_ID));
            assert!(u.upload_file.is_some(), "upload task carries its file");
        }
        assert!(
            aug.pre_succeeded.is_empty(),
            "upload setup tasks EXECUTE — never pre-succeeded"
        );

        // The work task depends on both upload tasks (by their ids).
        let work = work_in(&aug, "build");
        let dep_ids: std::collections::HashSet<&str> =
            work.task_depends_on.iter().map(|d| d.task_id.as_str()).collect();
        assert_eq!(dep_ids.len(), 2, "two deps, one per file");
        for u in &uploads {
            assert!(dep_ids.contains(u.task_id.as_str()), "dep on each upload task");
        }
    }

    #[test]
    fn dedup_subset_sharing_one_upload_per_unique_file() {
        // HEADLINE dedup test (owner-refined): the dedup must be FULLY GENERAL
        // — a file shared by an arbitrary SUBSET of work tasks → ONE upload
        // setup task that EXACTLY that subset depends on. NO universal-common
        // slot. The shape:
        //   X = /tc/X shared by builds {1, 2, 3}
        //   Y = /tc/Y shared by builds {4, 5, 6}
        //   Z = /tc/Z only in build {1}
        // The intersection of all task closures is EMPTY (no file shared by
        // every build), exactly the multi-era toolchain matrix. Assert each
        // unique file -> one upload + only-its-subset deps.
        let x = ("/tc/X", None);
        let y = ("/tc/Y", None);
        let z = ("/tc/Z", None);
        let batch = vec![
            (work_with_files("b1", &[x, z]), false), // X, Z
            (work_with_files("b2", &[x]), false),    // X
            (work_with_files("b3", &[x]), false),    // X
            (work_with_files("b4", &[y]), false),    // Y
            (work_with_files("b5", &[y]), false),    // Y
            (work_with_files("b6", &[y]), false),    // Y
        ];
        let aug = augment_batch_for_staging(batch, StagingStrategy::Disabled);

        // EXACTLY 3 upload setup tasks — one per unique file (X, Y, Z), NOT
        // one per (task, file) pair (which would be 8).
        let uploads = upload_tasks_in(&aug);
        assert_eq!(
            uploads.len(),
            3,
            "exactly one upload setup task per UNIQUE file (X, Y, Z) — deduped"
        );

        // Map each unique file's source to its single upload-task id.
        let id_for = |source: &str| -> String {
            uploads
                .iter()
                .find(|u| {
                    u.upload_file.as_ref().unwrap().source.as_path()
                        == std::path::Path::new(source)
                })
                .unwrap_or_else(|| panic!("no upload task for {source}"))
                .task_id
                .clone()
        };
        let (xid, yid, zid) = (id_for("/tc/X"), id_for("/tc/Y"), id_for("/tc/Z"));
        // The three ids are distinct (no collision across files).
        assert_ne!(xid, yid);
        assert_ne!(xid, zid);
        assert_ne!(yid, zid);

        let deps_of = |task_id: &str| -> std::collections::HashSet<String> {
            work_in(&aug, task_id)
                .task_depends_on
                .iter()
                .map(|d| d.task_id.clone())
                .collect()
        };

        // X's subset {b1, b2, b3} all depend on the ONE X upload; nobody else.
        for b in ["b1", "b2", "b3"] {
            assert!(deps_of(b).contains(&xid), "{b} deps on the single X upload");
        }
        // Y's subset {b4, b5, b6} all depend on the ONE Y upload; nobody else.
        for b in ["b4", "b5", "b6"] {
            assert!(deps_of(b).contains(&yid), "{b} deps on the single Y upload");
        }
        // Z's subset {b1} only.
        assert!(deps_of("b1").contains(&zid), "b1 deps on Z");
        for b in ["b2", "b3", "b4", "b5", "b6"] {
            assert!(!deps_of(b).contains(&zid), "{b} must NOT dep on Z (subset is {{b1}})");
        }
        // Cross-subset non-membership: nobody in Y's subset deps on X, etc.
        for b in ["b4", "b5", "b6"] {
            assert!(!deps_of(b).contains(&xid), "{b} (Y-subset) must NOT dep on X");
        }
        for b in ["b2", "b3"] {
            assert!(!deps_of(b).contains(&yid), "{b} (X-subset) must NOT dep on Y");
        }

        // Exact dep counts: b1 -> {X, Z} = 2; b2/b3/b4/b5/b6 -> 1 each.
        assert_eq!(deps_of("b1").len(), 2, "b1 deps on exactly X and Z");
        for b in ["b2", "b3", "b4", "b5", "b6"] {
            assert_eq!(deps_of(b).len(), 1, "{b} deps on exactly one file");
        }
    }

    #[test]
    fn dest_distinguishes_uploads_with_same_source() {
        // The dedup key is (source, dest): the same source placed at two
        // DIFFERENT explicit dests is two distinct uploads; the same
        // (source, dest) across tasks is one.
        let batch = vec![
            (work_with_files("a", &[("/src/lib", Some("/dst/one"))]), false),
            (work_with_files("b", &[("/src/lib", Some("/dst/two"))]), false),
            (work_with_files("c", &[("/src/lib", Some("/dst/one"))]), false),
        ];
        let aug = augment_batch_for_staging(batch, StagingStrategy::Disabled);
        let uploads = upload_tasks_in(&aug);
        assert_eq!(
            uploads.len(),
            2,
            "same source at two distinct dests => two uploads; (a,c) share one"
        );
        // a and c (same source+dest) share their upload task id.
        let dep_a = &work_in(&aug, "a").task_depends_on[0].task_id;
        let dep_c = &work_in(&aug, "c").task_depends_on[0].task_id;
        let dep_b = &work_in(&aug, "b").task_depends_on[0].task_id;
        assert_eq!(dep_a, dep_c, "(a,c) share the same (source,dest) upload");
        assert_ne!(dep_a, dep_b, "b's distinct dest is a distinct upload");
    }

    #[test]
    fn upload_setup_task_seeded_before_dependent_work() {
        // The dependent's dep must resolve by presence IN THE SAME batch:
        // the upload setup task is seeded BEFORE the work task that depends on
        // it (partition_ingest / extend see the dep resolve in-batch).
        let batch = vec![(work_with_files("w", &[("/src/f", None)]), false)];
        let aug = augment_batch_for_staging(batch, StagingStrategy::Disabled);
        let upload_idx = aug
            .batch
            .iter()
            .position(|(t, _)| t.kind.is_setup() && t.upload_file.is_some())
            .expect("an upload setup task");
        let work_idx = aug
            .batch
            .iter()
            .position(|(t, _)| t.task_id == "w")
            .expect("the work task");
        assert!(
            upload_idx < work_idx,
            "the upload setup task is seeded before its dependent work task"
        );
    }

    #[test]
    fn attach_composes_with_flagged_mode2_staging() {
        // The P2 files-attach and the #489 mode-2 flag pass COMPOSE: a work
        // task with required_files gets BOTH an upload setup task (its file)
        // and a pre-succeeded mode-2 stage task (its own path) when the flag
        // is on. The upload task executes (not pre-succeeded); the stage task
        // is pre-succeeded.
        let batch = vec![(work_with_files("w", &[("/src/f", None)]), false)];
        let aug = augment_batch_for_staging(batch, StagingStrategy::SetupTasks);

        // One upload setup task (P2, carries its file, NOT pre-succeeded).
        let uploads = upload_tasks_in(&aug);
        assert_eq!(uploads.len(), 1);
        assert!(uploads[0].upload_file.is_some());
        // One pre-succeeded mode-2 stage task (no upload_file).
        let stages: Vec<_> = aug
            .batch
            .iter()
            .filter(|(t, _)| t.kind.is_setup() && t.task_id.starts_with(STAGE_TASK_ID_PREFIX))
            .collect();
        assert_eq!(stages.len(), 1, "one mode-2 stage task");
        assert!(stages[0].0.upload_file.is_none(), "stage task carries no upload ref");
        assert_eq!(
            aug.pre_succeeded.len(),
            1,
            "only the mode-2 stage task is pre-succeeded; the upload task executes"
        );

        // The work task depends on BOTH the upload task and the stage task.
        let work = work_in(&aug, "w");
        let dep_ids: std::collections::HashSet<&str> =
            work.task_depends_on.iter().map(|d| d.task_id.as_str()).collect();
        assert!(dep_ids.contains(uploads[0].task_id.as_str()), "deps on the upload task");
        assert!(dep_ids.contains(stages[0].0.task_id.as_str()), "deps on the stage task");
    }

    #[test]
    fn non_worker_and_skipped_tasks_contribute_no_uploads() {
        // A consumer's own setup task and a discovery-skipped work task do NOT
        // produce upload setup tasks even if they carry required_files —
        // neither dispatches as ordinary work, so gating them would only add
        // inert ledger entries. A live work task sharing a file with the
        // skipped one still gets its single upload.
        let mut producer = work_with_files("p", &[("/src/x", None)]);
        producer.kind = TaskKind::Setup;
        let batch = vec![
            (work_with_files("skipped", &[("/src/x", None)]), true),
            (producer, false),
            (work_with_files("live", &[("/src/x", None)]), false),
        ];
        let aug = augment_batch_for_staging(batch, StagingStrategy::Disabled);

        // Only the live work task's file produces an upload (one, for /src/x).
        let uploads = upload_tasks_in(&aug);
        assert_eq!(uploads.len(), 1, "only the live work task contributes its file");
        // The skipped + producer tasks gain no deps.
        let skipped = aug.batch.iter().find(|(t, _)| t.task_id == "skipped").unwrap();
        assert!(skipped.1, "skip bit preserved");
        assert!(skipped.0.task_depends_on.is_empty(), "skipped task gains no dep");
        let producer = aug.batch.iter().find(|(t, _)| t.task_id == "p").unwrap();
        assert!(producer.0.task_depends_on.is_empty(), "producer task gains no dep");
        // The live task deps on the single upload.
        assert_eq!(work_in(&aug, "live").task_depends_on.len(), 1);
    }

    #[test]
    fn duplicate_file_in_one_task_dedups_to_one_dep() {
        // A single work task that lists the SAME file twice gets ONE upload
        // task and ONE dep (not two) — the per-task dedup mirrors the
        // cross-task dedup.
        let batch = vec![(
            work_with_files("w", &[("/src/f", None), ("/src/f", None)]),
            false,
        )];
        let aug = augment_batch_for_staging(batch, StagingStrategy::Disabled);
        assert_eq!(upload_tasks_in(&aug).len(), 1, "one upload for the repeated file");
        assert_eq!(
            work_in(&aug, "w").task_depends_on.len(),
            1,
            "one dep even though the file was listed twice"
        );
    }

    #[test]
    fn upload_task_hash_distinct_from_work_task_hash() {
        // The synthetic path makes each upload setup task's content hash
        // distinct from any work task's, so they never collide in the ledger.
        let w = work_with_files("w", &[("/src/f", None)]);
        let aug = augment_batch_for_staging(vec![(w.clone(), false)], StagingStrategy::Disabled);
        let upload = upload_tasks_in(&aug)[0];
        assert_ne!(
            dynrunner_core::compute_task_hash(&w),
            dynrunner_core::compute_task_hash(upload),
            "upload setup task hash must not collide with the work task hash"
        );
    }
}
