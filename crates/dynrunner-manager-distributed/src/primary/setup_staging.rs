//! Framework file-staging via setup tasks — the NEW staging path selected by
//! the `--stage-via-setup-tasks` flag (P3/P4 of #489).
//!
//! ## The ONE concern
//! Turn the framework's file-staging INTENT into setup-task structure: for
//! each file-backed work task, derive a per-file SETUP task and make the work
//! task depend on it (`TaskDep`). This module produces ONLY the structure —
//! the extra `TaskInfo`s + the dep edges + the set of setup-task hashes that
//! are ALREADY satisfied (pre-staged files). It does NOT seed the ledger,
//! does NOT execute anything, and does NOT resolve dependencies — those are
//! the existing originator (`originate_cold_seed` / `discover_on_promotion`),
//! the existing `ClusterMutation::SetupCompleted` apply arm, and the existing
//! `PendingPool` dep machine respectively.
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

use std::path::PathBuf;

use dynrunner_core::{Identifier, TaskDep, TaskInfo, TaskKind};

/// Which framework file-staging strategy the run uses. The SELECTOR (flag
/// off → old path; flag on → this new path) lives at run construction
/// (`PrimaryConfig`), driven by the `--stage-via-setup-tasks` CLI flag.
///
/// `Disabled` is the default: the framework's OLD staging
/// (`maybe_auto_stage_initial` / `StageFile`) runs and this module produces
/// NO augmentation — the cold seed is byte-for-byte what it was before.
///
/// `SetupTasks` is the new flagged path and covers BOTH upload modes with ONE
/// ledger model (owner-adjudicated option C): each file-backed work task gets
/// a per-file SETUP task seeded PRE-SUCCEEDED, gated by `TaskDep`. The modes
/// differ ONLY in whether a pre-run upload of the file happened, NOT in the
/// ledger shape — mode-2 (`--source-already-staged`) has the file already on
/// the cluster (nothing uploads), and mode-1 (files-on-submitter) runs the
/// EXISTING Python `upload_source_binaries` BEFORE `run()` (unchanged), so by
/// the time the setup task is seeded the file IS present and pre-succeeded is
/// honest.
///
/// In both modes the setup task is pre-succeeded in the replicated ledger, so
/// any primary (original / relocated / promoted) resolves the dependent's dep
/// from the ledger — the #488-free path. When the flag is on, the OLD
/// StageFile fan-out (`maybe_auto_stage_initial`) does NOT run (no
/// double-staging).
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
    }
}

/// Augment a discovery `marked_batch` with the framework's flagged
/// setup-task staging structure.
///
/// * [`StagingStrategy::Disabled`] — the IDENTITY transform: the batch is
///   returned unchanged and `pre_succeeded` is empty. The framework's OLD
///   staging path runs; this module contributes nothing. (Byte-for-byte the
///   pre-feature cold seed.)
/// * [`StagingStrategy::SetupTasks`] — each file-backed work task gains a
///   per-file stage setup task (injected into the batch) and a `TaskDep` on
///   it; every injected setup task is reported `pre_succeeded`. A work task
///   that is NOT file-backed (a producer/computed item — `kind != Work` or a
///   sentinel a consumer marked) is left untouched: there is no file to
///   stage. A discovery-skipped work task (`skipped == true`) is ALSO left
///   un-augmented — it never dispatches, so gating it on a stage task would
///   only add inert ledger entries.
///
/// Pure (`&`-only inputs, owned output): callable from both originators
/// (`originate_cold_seed` and `discover_on_promotion`) exactly as
/// `skip_transitions` is, with no coordinator state touched.
pub fn augment_batch_for_staging<I: Identifier>(
    marked_batch: Vec<(TaskInfo<I>, bool)>,
    strategy: StagingStrategy,
) -> StagingAugmentation<I> {
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
                // staging gate. A setup task (the consumer's own, or one we
                // inject in another pass) is never gated on a stage task, and
                // a discovery-skipped task never dispatches.
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
}
