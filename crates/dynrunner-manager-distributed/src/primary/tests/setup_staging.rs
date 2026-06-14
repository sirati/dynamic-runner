//! Framework file-staging via setup tasks (#489 P3) — the cold-seed +
//! relocate/promote ledger behaviour of the flagged staging path, and the
//! flag-off regression that the OLD path is byte-for-byte unchanged.
//!
//! These are the SEED-LEVEL tests (what the ledger holds + what the pool
//! resolves); the pure transform itself is unit-tested in
//! `primary::setup_staging`. The PyO3/Python flag round-trip is covered by the
//! pyo3 tests.

use super::*;

use crate::primary::wire::compute_task_hash;
use crate::primary::StagingStrategy;

/// The synthetic stage-task id the framework staging derives for a work task
/// — mirrors `setup_staging::STAGE_TASK_ID_PREFIX`. The hash is recomputed
/// from the derived TaskInfo, but the id is asserted to confirm the dep wiring
/// names the framework stage task.
const STAGE_PREFIX: &str = "__framework_stage__";

/// The synthetic id-prefix for a #336 P2 per-file UPLOAD setup task — mirrors
/// `setup_staging::UPLOAD_TASK_ID_PREFIX`.
const UPLOAD_PREFIX: &str = "__framework_upload__";

/// A `make_binary`-shaped work task declaring `required_files` (#336 P2). The
/// files attach is DATA-driven (no flag), so the config can stay the default.
/// The return type carries the test-harness `TestId` (both re-exported via the
/// module glob).
fn work_with_required_files(name: &str, sources: &[&str]) -> TaskInfo<TestId> {
    let mut t = make_binary(name, 100);
    t.required_files = dynrunner_core::required_files_storage(
        sources
            .iter()
            .map(|s| dynrunner_core::UploadFileRef {
                source: std::path::PathBuf::from(s),
                dest: None,
            })
            .collect(),
    );
    t
}

/// A `PrimaryConfig` with the framework staging-via-setup-tasks flag ON.
fn staging_config() -> PrimaryConfig {
    PrimaryConfig {
        staging_strategy: StagingStrategy::SetupTasks,
        ..test_primary_config()
    }
}

/// Headline (the #488 scenario at seed time): flag ON, a file-backed work task
/// is cold-seeded. The framework injects a PRE-SUCCEEDED per-file setup task,
/// the work task gains a `TaskDep` on it, and after hydrate the work task is
/// DISPATCHABLE (queued in the pool, not blocked) — its dep was satisfied by
/// the ledger's `SetupCompleted` entry, with NO setup execution and NO
/// `pre_staged_mode` involved.
#[test]
fn flag_on_seeds_setup_completed_and_dependent_is_dispatchable() {
    let (transport, _ends) = setup_test(1);
    let (mut primary, _mesh) = build_test_primary(
        staging_config(),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );

    let work = make_binary("staged-work", 100);
    let work_hash = compute_task_hash(&work);

    primary
        .originate_cold_seed(vec![(work, false)], HashMap::new())
        .expect("flagged staging cold seed");
    primary
        .hydrate_from_cluster_state()
        .expect("composed task graph is valid");

    let cs = primary.cluster_state_for_test();

    // Two ledger entries: the work task + its injected stage setup task.
    assert_eq!(
        cs.task_count(),
        2,
        "the work task + one injected per-file stage setup task"
    );

    // The injected setup task is SetupCompleted (pre-succeeded — never
    // executed) and carries the framework stage id.
    let (setup_hash, setup_task_id) = cs
        .tasks_iter()
        .find(|(_, s)| s.task().kind.is_setup())
        .map(|(h, s)| (h.clone(), s.task().task_id.clone()))
        .expect("an injected setup task in the ledger");
    assert!(
        setup_task_id.starts_with(STAGE_PREFIX),
        "the injected setup task carries the framework stage id; got {setup_task_id:?}"
    );
    assert!(
        matches!(
            cs.task_state(&setup_hash),
            Some(crate::cluster_state::TaskState::SetupCompleted { .. })
        ),
        "the per-file stage setup task must be seeded PRE-SUCCEEDED; got {:?}",
        cs.task_state(&setup_hash)
    );

    // The work task is Pending (dep resolved at seed) — NOT Blocked.
    assert!(
        matches!(
            cs.task_state(&work_hash),
            Some(crate::cluster_state::TaskState::Pending { .. })
        ),
        "the dependent work task must land Pending (its dep is satisfied by \
         the pre-succeeded setup task); got {:?}",
        cs.task_state(&work_hash)
    );

    // And it is DISPATCHABLE: the pool's queued view holds it, and nothing is
    // blocked. A setup task is never worker-dispatchable, so the pool's queued
    // view holds EXACTLY the one work task.
    let queued: Vec<String> = primary
        .pool()
        .iter()
        .map(|t| t.task_id.clone())
        .collect();
    assert_eq!(
        queued,
        vec!["staged-work".to_string()],
        "exactly the work task is queued+dispatchable (the setup task is never \
         worker-dispatchable); got {queued:?}"
    );
    assert_eq!(
        primary.pool().blocked_len(),
        0,
        "no task is blocked — the staging dep was satisfied at seed time"
    );
}

/// The #488-free guarantee: a RELOCATED / PROMOTED primary reads the SAME
/// replicated ledger and the dependent's dep is satisfied — no
/// `pre_staged_mode` flag to mis-stamp. Seed on one primary, snapshot, restore
/// onto a FRESH primary (the `seed_from_promotion_snapshot` path a relocate
/// target / promoted node takes), hydrate, and assert the work task is
/// dispatchable purely from the ledger.
#[test]
fn relocated_primary_reads_ledger_and_dep_is_satisfied() {
    // ORIGINAL primary seeds the flagged staging ledger.
    let (transport, _ends) = setup_test(1);
    let (mut original, _mesh) = build_test_primary(
        staging_config(),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    let work = make_binary("relocated-work", 100);
    let work_hash = compute_task_hash(&work);
    original
        .originate_cold_seed(vec![(work, false)], HashMap::new())
        .expect("flagged staging cold seed");
    original
        .hydrate_from_cluster_state()
        .expect("composed task graph is valid");
    let snapshot = original.cluster_state_for_test().snapshot();

    // FRESH primary (the relocate target / promoted node) inherits the ledger
    // via the promotion snapshot path. Note its OWN config has staging
    // DISABLED — the #488-free property must hold from the LEDGER, not from
    // any local staging-mode flag on the promoted primary.
    let (transport2, _ends2) = setup_test(1);
    let (mut promoted, _mesh2) = build_test_primary(
        test_primary_config(), // staging Disabled here on purpose
        transport2,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    promoted.seed_from_promotion_snapshot(snapshot);
    promoted
        .hydrate_from_cluster_state()
        .expect("promoted hydrate over inherited ledger");

    // The promoted primary sees the setup task SetupCompleted and the work
    // task dispatchable — entirely from the inherited ledger.
    let cs = promoted.cluster_state_for_test();
    assert!(
        cs.tasks_iter().any(|(_, s)| s.task().kind.is_setup()
            && matches!(s, crate::cluster_state::TaskState::SetupCompleted { .. })),
        "the inherited ledger carries the pre-succeeded stage setup task"
    );
    assert!(
        matches!(
            cs.task_state(&work_hash),
            Some(crate::cluster_state::TaskState::Pending { .. })
        ),
        "the promoted primary resolves the work task's dep from the ledger \
         (Pending, dispatchable); got {:?}",
        cs.task_state(&work_hash)
    );
    let queued: Vec<String> = promoted.pool().iter().map(|t| t.task_id.clone()).collect();
    assert_eq!(
        queued,
        vec!["relocated-work".to_string()],
        "the promoted primary's pool holds the dispatchable work task; got {queued:?}"
    );
    assert_eq!(promoted.pool().blocked_len(), 0, "nothing blocked post-relocate");
}

/// Flag ON via the mode-2 discovery originator (`discover_on_promotion`): the
/// SAME augmentation runs on the discovered batch (the corpus is discovered
/// post-relocate in the `--source-already-staged` path), seeding a
/// pre-succeeded setup task + a dispatchable dependent.
#[tokio::test(flavor = "current_thread")]
async fn flag_on_discovery_path_seeds_setup_completed_and_dependent_dispatchable() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(1);
            let (mut primary, _mesh) = build_test_primary(
                staging_config(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // Mode-2: declare debt + register a discovery policy that yields one
            // file-backed work task, then run the discovery originator.
            let work = make_binary("discovered-work", 100);
            let work_hash = compute_task_hash(&work);
            let fire = std::rc::Rc::new(std::cell::Cell::new(0u32));
            primary.register_setup_discovery(fixed_discovery(
                vec![work],
                HashMap::new(),
                fire.clone(),
            ));
            primary
                .cluster_state_mut_for_test()
                .apply(ClusterMutation::DiscoveryDebtDeclared);

            primary
                .discover_on_promotion()
                .await
                .expect("discovery seam with flagged staging");
            assert_eq!(fire.get(), 1, "discovery policy fired once");

            let cs = primary.cluster_state_for_test();
            assert_eq!(
                cs.task_count(),
                2,
                "discovered work task + its injected stage setup task"
            );
            assert!(
                cs.tasks_iter().any(|(_, s)| s.task().kind.is_setup()
                    && matches!(s, crate::cluster_state::TaskState::SetupCompleted { .. })),
                "the discovery path seeds the stage setup task pre-succeeded too"
            );
            assert!(
                matches!(
                    cs.task_state(&work_hash),
                    Some(crate::cluster_state::TaskState::Pending { .. })
                ),
                "the discovered work task is dispatchable (dep satisfied); got {:?}",
                cs.task_state(&work_hash)
            );
            let queued: Vec<String> =
                primary.pool().iter().map(|t| t.task_id.clone()).collect();
            assert_eq!(queued, vec!["discovered-work".to_string()]);
            assert_eq!(primary.pool().blocked_len(), 0);
        })
        .await;
}

/// Regression: flag OFF (the default) → the cold seed is byte-for-byte the OLD
/// path. NO setup task is injected, NO `SetupCompleted` entry is seeded, the
/// work task is the sole ledger entry and lands `Pending` with no dep — exactly
/// as before the feature. This pins that the new module contributes NOTHING
/// when the flag is off.
#[test]
fn flag_off_old_path_is_unchanged() {
    let (transport, _ends) = setup_test(1);
    let (mut primary, _mesh) = build_test_primary(
        test_primary_config(), // StagingStrategy::Disabled (the default)
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );

    let work = make_binary("plain-work", 100);
    let work_hash = compute_task_hash(&work);

    primary
        .originate_cold_seed(vec![(work, false)], HashMap::new())
        .expect("default cold seed");
    primary
        .hydrate_from_cluster_state()
        .expect("composed task graph is valid");

    let cs = primary.cluster_state_for_test();
    // Exactly ONE ledger entry — no injected setup task.
    assert_eq!(
        cs.task_count(),
        1,
        "flag off: no setup task is injected — only the work task is seeded"
    );
    assert!(
        cs.tasks_iter().all(|(_, s)| s.task().kind.is_worker_assignable()),
        "flag off: no Setup-kind task exists in the ledger"
    );
    // No SetupCompleted state anywhere.
    assert!(
        !cs.tasks_iter()
            .any(|(_, s)| matches!(s, crate::cluster_state::TaskState::SetupCompleted { .. })),
        "flag off: nothing is seeded SetupCompleted"
    );
    // The work task is plain Pending, no dep wired.
    assert!(
        matches!(
            cs.task_state(&work_hash),
            Some(crate::cluster_state::TaskState::Pending { .. })
        ),
        "flag off: the work task lands Pending; got {:?}",
        cs.task_state(&work_hash)
    );
    let work_deps = cs
        .task_state(&work_hash)
        .map(|s| s.task().task_depends_on.clone())
        .expect("the work task entry");
    assert!(
        work_deps.is_empty(),
        "flag off: the work task gains NO staging dep"
    );
}

// ── #336 P2: per-work-task required-files attach (seed-level) ───────────────

/// Headline P2 seed behaviour: a work task declaring `required_files=[a, b]`
/// cold-seeds into TWO upload setup tasks (seeded `Pending` — they EXECUTE the
/// upload, NOT pre-succeeded) + a work task that is BLOCKED at seed (its deps
/// are not satisfied until the uploads run). Drive both uploads to
/// `SetupCompleted` and the work task becomes DISPATCHABLE. This proves the
/// gate is real: the work task dispatches ONLY after both upload terminals.
/// DATA-driven — the staging flag is OFF (default), so this is orthogonal to
/// the #489 mode-2 path.
#[test]
fn required_files_seed_uploads_pending_and_work_blocked_until_completed() {
    let (transport, _ends) = setup_test(1);
    let (mut primary, _mesh) = build_test_primary(
        test_primary_config(), // staging Disabled — P2 attach is DATA-driven
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );

    let work = work_with_required_files("build-task", &["/src/a", "/src/b"]);
    let work_hash = compute_task_hash(&work);

    primary
        .originate_cold_seed(vec![(work, false)], HashMap::new())
        .expect("P2 files-attach cold seed");
    primary
        .hydrate_from_cluster_state()
        .expect("composed task graph is valid");

    let cs = primary.cluster_state_for_test();

    // THREE ledger entries: the work task + TWO upload setup tasks (one per
    // unique file).
    assert_eq!(
        cs.task_count(),
        3,
        "the work task + two per-file upload setup tasks"
    );

    // Both upload setup tasks are seeded PENDING (NOT pre-succeeded — they
    // execute the upload), carry an `upload_file` ref + source-owner affinity,
    // and the framework upload id.
    let uploads: Vec<_> = cs
        .tasks_iter()
        .filter(|(_, s)| s.task().kind.is_setup())
        .map(|(h, s)| (h.clone(), s.task().clone()))
        .collect();
    assert_eq!(uploads.len(), 2, "two upload setup tasks");
    for (hash, task) in &uploads {
        assert!(
            task.task_id.starts_with(UPLOAD_PREFIX),
            "upload task carries the framework upload id; got {:?}",
            task.task_id
        );
        assert!(task.upload_file.is_some(), "upload task carries its file");
        assert_eq!(
            task.setup_affinity.as_deref(),
            Some(dynrunner_core::SETUP_NODE_ID),
            "source-owner affinity"
        );
        assert!(
            matches!(
                cs.task_state(hash),
                Some(crate::cluster_state::TaskState::Pending { .. })
            ),
            "the upload setup task must be seeded PENDING (it executes); got {:?}",
            cs.task_state(hash)
        );
    }

    // The work task is BLOCKED at seed — its upload deps are NOT yet satisfied
    // (the uploads have not run).
    assert_eq!(
        primary.pool().blocked_len(),
        1,
        "the work task is BLOCKED until its uploads complete"
    );
    let queued_before: Vec<String> = primary
        .pool()
        .iter()
        .map(|t| t.task_id.clone())
        .collect();
    assert!(
        !queued_before.contains(&"build-task".to_string()),
        "the work task is NOT dispatchable before its uploads complete; got {queued_before:?}"
    );

    // Drive BOTH uploads to SetupCompleted (the terminal the upload-action
    // executor originates on a successful upload), then re-hydrate. The work
    // task's deps are now satisfied from the ledger.
    {
        let cs = primary.cluster_state_mut_for_test();
        for (hash, _) in &uploads {
            cs.apply(ClusterMutation::SetupCompleted { hash: hash.clone() });
        }
    }
    primary
        .hydrate_from_cluster_state()
        .expect("re-hydrate after the uploads completed");

    // Now the work task is DISPATCHABLE: Pending, queued, nothing blocked.
    let cs = primary.cluster_state_for_test();
    assert!(
        matches!(
            cs.task_state(&work_hash),
            Some(crate::cluster_state::TaskState::Pending { .. })
        ),
        "after both uploads complete, the work task is Pending/dispatchable; got {:?}",
        cs.task_state(&work_hash)
    );
    let queued_after: Vec<String> = primary
        .pool()
        .iter()
        .map(|t| t.task_id.clone())
        .collect();
    assert_eq!(
        queued_after,
        vec!["build-task".to_string()],
        "exactly the work task is queued+dispatchable after both uploads; got {queued_after:?}"
    );
    assert_eq!(
        primary.pool().blocked_len(),
        0,
        "nothing blocked once both upload deps are satisfied"
    );
}

/// DEDUP at the seed level (the multi-era subset-sharing shape): N work tasks
/// declaring the SAME shared file produce EXACTLY ONE upload setup task that
/// all N depend on — NOT N uploads. Plus a per-task delta file. Asserts the
/// ledger holds one upload per UNIQUE file and every sharer is gated on the
/// single shared upload.
#[test]
fn required_files_dedup_one_upload_shared_by_all_sharers() {
    let (transport, _ends) = setup_test(1);
    let (mut primary, _mesh) = build_test_primary(
        test_primary_config(),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );

    // Three builds share /tc/common; b1 also needs its own /tc/delta1.
    let b1 = work_with_required_files("b1", &["/tc/common", "/tc/delta1"]);
    let b2 = work_with_required_files("b2", &["/tc/common"]);
    let b3 = work_with_required_files("b3", &["/tc/common"]);

    primary
        .originate_cold_seed(
            vec![(b1, false), (b2, false), (b3, false)],
            HashMap::new(),
        )
        .expect("P2 dedup cold seed");
    primary
        .hydrate_from_cluster_state()
        .expect("composed task graph is valid");

    let cs = primary.cluster_state_for_test();

    // 3 work tasks + 2 unique-file uploads (common, delta1) = 5 — NOT 4
    // uploads (which is what one-upload-per-(task,file) would give).
    assert_eq!(
        cs.task_count(),
        5,
        "3 work tasks + EXACTLY 2 deduped upload setup tasks (common, delta1)"
    );
    let uploads: Vec<_> = cs
        .tasks_iter()
        .filter(|(_, s)| s.task().kind.is_setup())
        .map(|(_, s)| s.task().clone())
        .collect();
    assert_eq!(uploads.len(), 2, "exactly one upload per unique file");

    // The /tc/common upload's id — the single task all three builds gate on.
    let common_upload_id = uploads
        .iter()
        .find(|t| {
            t.upload_file.as_ref().unwrap().source.as_path()
                == std::path::Path::new("/tc/common")
        })
        .expect("a /tc/common upload task")
        .task_id
        .clone();

    // Each of b1/b2/b3 depends on the SAME common upload id.
    for name in ["b1", "b2", "b3"] {
        let deps = cs
            .tasks_iter()
            .find(|(_, s)| s.task().task_id == name)
            .map(|(_, s)| s.task().task_depends_on.clone())
            .unwrap_or_else(|| panic!("{name} in ledger"));
        assert!(
            deps.iter().any(|d| d.task_id == common_upload_id),
            "{name} must gate on the SINGLE shared /tc/common upload"
        );
    }
    // b1 additionally gates on its delta upload; b2/b3 do not.
    let b1_dep_count = cs
        .tasks_iter()
        .find(|(_, s)| s.task().task_id == "b1")
        .map(|(_, s)| s.task().task_depends_on.len())
        .unwrap();
    assert_eq!(b1_dep_count, 2, "b1 gates on common + its own delta1");
    for name in ["b2", "b3"] {
        let n = cs
            .tasks_iter()
            .find(|(_, s)| s.task().task_id == name)
            .map(|(_, s)| s.task().task_depends_on.len())
            .unwrap();
        assert_eq!(n, 1, "{name} gates on common only");
    }
}

/// Manual-spawn: a consumer directly spawns a file-setup-task (a `Setup` task
/// carrying an `upload_file`, NOT tied to any task's `required_files`), and a
/// separate work task depends on it by id. The upload executes (seeded
/// Pending), the dependent is Blocked until the upload's SetupCompleted, then
/// dispatchable — the SAME gate the auto-attach uses, confirming a
/// consumer-spawned file-setup-task composes with `task_depends_on`.
#[test]
fn manual_spawned_file_setup_task_gates_a_dependent_work_task() {
    let (transport, _ends) = setup_test(1);
    let (mut primary, _mesh) = build_test_primary(
        test_primary_config(),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );

    // A directly-spawned file-setup-task: a Setup task carrying an upload_file.
    let mut shared_upload = make_binary("shared-closure", 0);
    shared_upload.kind = dynrunner_core::TaskKind::Setup;
    shared_upload.setup_affinity = Some(dynrunner_core::SETUP_NODE_ID.to_string());
    shared_upload.upload_file = Some(Box::new(dynrunner_core::UploadFileRef {
        source: std::path::PathBuf::from("/shared/closure.tar"),
        dest: None,
    }));
    let upload_hash = compute_task_hash(&shared_upload);

    // A work task depending on the manually-spawned file-setup-task by id.
    let mut dependent = make_binary("consumer-build", 100);
    let dep_hash = compute_task_hash(&dependent);
    dependent.task_depends_on = vec![dynrunner_core::TaskDep {
        task_id: "shared-closure".into(),
        phase_id: dynrunner_core::PhaseId::from("default"),
        inherit_outputs: false,
    }];

    primary
        .originate_cold_seed(
            vec![(shared_upload, false), (dependent, false)],
            HashMap::new(),
        )
        .expect("manual-spawn cold seed");
    primary
        .hydrate_from_cluster_state()
        .expect("composed task graph is valid");

    // The upload setup task is seeded Pending (it executes); the dependent is
    // Blocked until it completes.
    let cs = primary.cluster_state_for_test();
    assert!(
        matches!(
            cs.task_state(&upload_hash),
            Some(crate::cluster_state::TaskState::Pending { .. })
        ),
        "the manually-spawned file-setup-task is seeded Pending (it uploads)"
    );
    assert_eq!(
        primary.pool().blocked_len(),
        1,
        "the dependent work task is Blocked until the upload completes"
    );

    // Drive the upload to SetupCompleted, re-hydrate: the dependent unblocks.
    primary
        .cluster_state_mut_for_test()
        .apply(ClusterMutation::SetupCompleted {
            hash: upload_hash.clone(),
        });
    primary
        .hydrate_from_cluster_state()
        .expect("re-hydrate after the manual upload completed");

    let cs = primary.cluster_state_for_test();
    assert!(
        matches!(
            cs.task_state(&dep_hash),
            Some(crate::cluster_state::TaskState::Pending { .. })
        ),
        "the dependent work task unblocks once the manual upload completes"
    );
    let queued: Vec<String> = primary.pool().iter().map(|t| t.task_id.clone()).collect();
    assert_eq!(
        queued,
        vec!["consumer-build".to_string()],
        "the dependent is dispatchable after the manual file-setup-task completes"
    );
    assert_eq!(primary.pool().blocked_len(), 0);
}
