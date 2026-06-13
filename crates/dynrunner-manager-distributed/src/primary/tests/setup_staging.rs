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
