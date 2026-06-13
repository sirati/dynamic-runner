//! Death-seam test for the setup-task primitive (P1 seam (b)): an
//! in-flight `TaskKind::Setup` task is NON-reassignable — when its
//! holder dies it is driven to a terminal-unrecoverable `Failed`
//! (NonRecoverable), NOT requeued. An in-flight `Work` task on the same
//! dead holder is requeued as usual.
//!
//! P1 has no setup-task executor, so a setup task never reaches the
//! in-flight ledger through normal dispatch (the scheduling seam keeps
//! it out of every worker view). The test therefore seeds the in-flight
//! ledger DIRECTLY — simulating what the P2 in-process executor will do
//! — and exercises the reassignment rule that lands in P1.

use super::*;

use dynrunner_core::{PhaseId, ResourceMap, ResourceKind, TaskKind, TypeId};

use crate::primary::coordinator::InFlightEntry;
use crate::primary::wire::compute_task_hash;

/// A `Work` (default) task on phase `work`.
fn work_task(name: &str) -> TaskInfo<TestId> {
    let mut t = make_binary(name, 100);
    t.phase_id = PhaseId::from("work");
    t.type_id = TypeId::from("default");
    t
}

/// A `Setup`-kind task on phase `work`.
fn setup_task(name: &str) -> TaskInfo<TestId> {
    let mut t = work_task(name);
    t.kind = TaskKind::Setup;
    t
}

/// Build a one-phase primary with an empty `work` pool and one idle
/// worker on `sec-0`, ready to have its in-flight ledger seeded directly.
fn primary_with_work_pool()
-> PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId> {
    let (transport, _ends) = setup_test(1);
    let (mut primary, _mesh) = build_test_primary(
        test_primary_config(),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    let phase = PhaseId::from("work");
    let pool = dynrunner_scheduler_api::PendingPool::<TestId>::new([phase], HashMap::new())
        .expect("work-phase pool");
    primary.pending = Some(pool);
    let budget = ResourceMap::from([(ResourceKind::memory(), 1024 * 1024 * 1024u64)]);
    primary.register_idle_worker_for_test("sec-0".into(), 0, budget);
    primary
}

/// Seed one in-flight ledger entry (+ the CRDT `InFlight`) directly,
/// simulating a holder running the task — for a setup task this is the
/// P2 in-process executor; for a work task a dispatched worker.
fn seed_in_flight(
    primary: &mut PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
    secondary: &str,
    task: TaskInfo<TestId>,
) -> String {
    let hash = compute_task_hash(&task);
    primary.cluster_state.apply(ClusterMutation::TaskAdded {
        hash: hash.clone(),
        task: task.clone(),
    });
    primary.in_flight.insert(
        hash.clone(),
        InFlightEntry {
            phase: task.phase_id.clone(),
            secondary_id: secondary.to_string(),
            local_worker_id: Some(0),
            task,
        },
    );
    hash
}

/// The death seam: an in-flight setup task on a dead secondary is NOT
/// requeued — it is failed terminally (NonRecoverable) — while a sibling
/// work task on the same dead secondary IS requeued.
///
/// RED before the seam: the recovery loop requeued EVERY in-flight task
/// (one `TaskRequeued` per hash, both pushed into the pool), so the
/// setup task would have been re-dispatched.
#[test]
fn dead_secondary_fails_setup_task_terminally_but_requeues_work() {
    let mut primary = primary_with_work_pool();

    let work_hash = seed_in_flight(&mut primary, "sec-0", work_task("the-work"));
    let setup_hash = seed_in_flight(&mut primary, "sec-0", setup_task("the-setup"));

    let mutations = primary.recover_inflight_for_dead_secondary("sec-0");
    assert_eq!(mutations.len(), 2, "both in-flight tasks are accounted for");

    // The WORK task is requeued (InFlight -> Pending).
    let work_requeued = mutations.iter().any(|m| {
        matches!(m, ClusterMutation::TaskRequeued { hash, .. } if hash == &work_hash)
    });
    assert!(work_requeued, "the work task is requeued on holder death");

    // The SETUP task is NOT requeued — it is failed terminally
    // (NonRecoverable), the non-reassignable rule.
    let setup_requeued = mutations.iter().any(|m| {
        matches!(m, ClusterMutation::TaskRequeued { hash, .. } if hash == &setup_hash)
    });
    assert!(
        !setup_requeued,
        "a setup task must NEVER be requeued on holder death (non-reassignable)"
    );
    let setup_failed = mutations.iter().any(|m| {
        matches!(
            m,
            ClusterMutation::TaskFailed { hash, kind, .. }
                if hash == &setup_hash && *kind == dynrunner_core::ErrorType::NonRecoverable
        )
    });
    assert!(
        setup_failed,
        "the setup task must be driven to a terminal Failed(NonRecoverable) — \
         executor death is unrecoverable; got {mutations:?}"
    );

    // Pool side: only the work task re-entered the pool; the setup task
    // did not (it can never be re-dispatched).
    let pool_ids: Vec<String> = primary.pool().iter().map(|t| t.task_id.clone()).collect();
    assert_eq!(
        pool_ids,
        vec!["the-work".to_string()],
        "only the work task is requeued into the pool; the setup task is not"
    );
    // Both in-flight ledger entries were drained.
    assert!(
        primary.in_flight.is_empty(),
        "both in-flight entries are removed by the recovery"
    );
}
