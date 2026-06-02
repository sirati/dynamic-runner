//! Tests for `PrimaryCoordinator::hydrate_from_cluster_state` — the
//! authoritative-primary pool/ledger seed from the replicated CRDT.
//!
//! Faithful-port companion to the secondary's hydration tests; the two
//! drive the symmetric `cascade_drain_done` primitive. Both fixtures
//! seed `cluster_state` directly via `ClusterState::apply` (the
//! mutable test accessor) so the pre-pool state is built without the
//! broadcast path's pool-dependent auto-resume step.

use super::*;

use dynrunner_core::{PhaseId, TaskDep, TypeId};

/// Build a `TaskInfo` with an explicit phase + dependency list so the
/// hydration tests can exercise the dep-resolution seed and the
/// phase-counter bookkeeping. `task_id == hash == name` keeps the
/// CRDT key and the dep-graph key aligned (the cluster ledger is keyed
/// by hash; `task_depends_on` references the prereq's `task_id`).
fn dep_binary(name: &str, phase: &str, depends_on: &[&str]) -> TaskInfo<TestId> {
    let mut t = make_binary(name, 100);
    t.phase_id = PhaseId::from(phase);
    t.type_id = TypeId::from("default");
    t.task_depends_on = depends_on
        .iter()
        .map(|d| TaskDep {
            task_id: (*d).to_string(),
            inherit_outputs: false,
        })
        .collect();
    t
}

/// (1) A terminal toolchain task plus `Pending` dependents whose
/// `task_depends_on` references it. After hydration the pool must
/// include the dependents (no `UnknownTaskDep` rejection): the
/// terminal entry seeded `mark_tasks_completed`, so `extend()` accepts
/// every dependent whose prereq finished pre-composition.
#[test]
fn hydrate_seeds_completed_deps_so_dependents_enter_pool() {
    let (transport, _ends) = setup_test(1);
    let mut primary: PrimaryCoordinator<_, _, _, _, TestId> = PrimaryCoordinator::new(
        PrimaryConfig::default(),
        transport,
        NoPeers,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );

    // Toolchain prereq: a single phase-A task that already completed
    // pre-composition. Its dependents declare `task_depends_on:
    // ["toolchain"]`.
    let toolchain = dep_binary("toolchain", "build", &[]);
    let dep_a = dep_binary("dep-a", "compile", &["toolchain"]);
    let dep_b = dep_binary("dep-b", "compile", &["toolchain"]);

    {
        let cs = primary.cluster_state_mut_for_test();
        // Phase deps: `compile` depends on `build`.
        cs.apply(ClusterMutation::PhaseDepsSet {
            deps: HashMap::from([(
                PhaseId::from("compile"),
                vec![PhaseId::from("build")],
            )]),
        });
        cs.apply(ClusterMutation::TaskAdded {
            hash: "toolchain".into(),
            task: toolchain.clone(),
        });
        // Drive `toolchain` to terminal Completed.
        cs.apply(ClusterMutation::TaskCompleted {
            hash: "toolchain".into(),
            result_data: None,
        });
        cs.apply(ClusterMutation::TaskAdded {
            hash: "dep-a".into(),
            task: dep_a.clone(),
        });
        cs.apply(ClusterMutation::TaskAdded {
            hash: "dep-b".into(),
            task: dep_b.clone(),
        });
    }

    primary.hydrate_from_cluster_state();

    // The pool exists and carries exactly the two dependents — the
    // terminal toolchain seeded `completed_task_ids`, so `extend()`
    // accepted both without an `UnknownTaskDep` error (which would have
    // left the pool `None`).
    let pool = primary.pool();
    assert_eq!(pool.len(), 2, "both dependents must enter the pool");
    let ids: std::collections::HashSet<&str> =
        pool.iter().map(|t| t.task_id.as_str()).collect();
    assert!(ids.contains("dep-a"));
    assert!(ids.contains("dep-b"));
    // The terminal toolchain hash is recorded in the primary-side
    // completed ledger and is NOT re-queued.
    assert!(!ids.contains("toolchain"));
    assert_eq!(primary.cluster_state_for_test().task_count(), 3);
    // total_tasks tracks the cluster ledger's task count (single
    // source of truth).
    assert_eq!(primary.total_tasks, 3);
}

/// (2) A single `InFlight` task. After hydration the dispatch/recheck
/// view must NOT re-offer that hash (it was never queued — it is
/// in-flight), the phase in-flight counter must read 1, and the
/// pre-owned in-flight ledger must hold the entry so a later broadcast
/// completion decrements the correct phase.
#[test]
fn hydrate_inflight_task_not_reoffered_and_counter_one() {
    let (transport, _ends) = setup_test(1);
    let mut primary: PrimaryCoordinator<_, _, _, _, TestId> = PrimaryCoordinator::new(
        PrimaryConfig::default(),
        transport,
        NoPeers,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );

    let task = dep_binary("inflight-1", "work", &[]);
    {
        let cs = primary.cluster_state_mut_for_test();
        cs.apply(ClusterMutation::TaskAdded {
            hash: "inflight-1".into(),
            task: task.clone(),
        });
        // Drive to InFlight on a remote secondary's worker.
        cs.apply(ClusterMutation::TaskAssigned {
            hash: "inflight-1".into(),
            secondary: "secondary-0".into(),
            worker: 0,
        });
    }

    primary.hydrate_from_cluster_state();

    let phase = PhaseId::from("work");
    let pool = primary.pool();
    // The in-flight task is NOT a queued pool item (it entered via the
    // InFlight arm, not `items`), so no dispatch view can re-offer it.
    let queued_ids: std::collections::HashSet<&str> =
        pool.iter().map(|t| t.task_id.as_str()).collect();
    assert!(
        !queued_ids.contains("inflight-1"),
        "in-flight task must not be re-offered as a pending item"
    );
    // No QUEUED items — the only task is in-flight. (`pool.len()`
    // counts queued + in-flight + blocked, so it reads 1 here; the
    // queued-iterator is the dispatch-offer surface and must be empty.)
    assert_eq!(pool.iter().count(), 0, "no queued/dispatchable items");
    // The dispatch/recheck path (`view_for_worker`) offers nothing for
    // a fresh worker — the in-flight hash is not re-offered, so no
    // `TaskAssignment` would be emitted for it.
    let view = pool.view_for_worker(0, None);
    assert!(
        view.is_empty(),
        "dispatch view must not re-offer the in-flight task"
    );
    // Phase in-flight counter seeded to exactly 1 via
    // `mark_tasks_in_flight`.
    assert_eq!(
        pool.in_flight(&phase),
        1,
        "phase in-flight counter must read 1 for the single InFlight task"
    );
    // The unified in-flight ledger holds the inherited entry so a later
    // broadcast TaskComplete/TaskFailed finds it BY HASH and decrements
    // the right phase.
    assert_eq!(primary.in_flight_len_for_test(), 1);
}

/// (C4) A broadcast `TaskComplete` for an INHERITED in-flight task —
/// one this coordinator inherited via hydration, NOT dispatched to a
/// local worker — must decrement the CORRECT phase's in-flight counter
/// (N+1 → N) by resolving the unified `in_flight` ledger entry BY HASH
/// in `free_slot_on_terminal` (no holding slot needed), and drain its
/// ledger entry. Without the by-hash resolution, no `note_item_completed`
/// fires and the phase counter stays stuck at 1 forever.
#[tokio::test(flavor = "current_thread")]
async fn inherited_in_flight_completion_decrements_phase_counter() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(1);
            let mut primary: PrimaryCoordinator<_, _, _, _, TestId> =
                PrimaryCoordinator::new(
                    PrimaryConfig::default(),
                    transport,
                    NoPeers,
                    ResourceStealingScheduler::memory(),
                    FixedEstimator(100),
                );

            let task = dep_binary("inflight-1", "work", &[]);
            {
                let cs = primary.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::TaskAdded {
                    hash: "inflight-1".into(),
                    task: task.clone(),
                });
                cs.apply(ClusterMutation::TaskAssigned {
                    hash: "inflight-1".into(),
                    secondary: "secondary-0".into(),
                    worker: 0,
                });
            }
            primary.hydrate_from_cluster_state();

            let phase = PhaseId::from("work");
            assert_eq!(primary.pool().in_flight(&phase), 1);
            assert_eq!(primary.in_flight_len_for_test(), 1);

            // A broadcast TaskComplete lands for the inherited hash. No
            // local `RemoteWorkerState` holds it (none were registered),
            // so `free_slot_on_terminal` resolves the ledger entry BY
            // HASH (worker_idx = None) and carries the phase decrement.
            let msg = DistributedMessage::TaskComplete {
                sender_id: "secondary-0".into(),
                timestamp: 0.0,
                secondary_id: "secondary-0".into(),
                worker_id: 0,
                task_hash: "inflight-1".into(),
                result_data: None,
            };
            primary.handle_task_complete(msg, &mut None).await;

            assert_eq!(
                primary.pool().in_flight(&phase),
                0,
                "pre-owned completion must drop the phase in-flight counter to 0"
            );
            assert_eq!(
                primary.in_flight_len_for_test(),
                0,
                "inherited ledger entry must be drained on terminal observation"
            );
            assert!(primary.completed_tasks.contains("inflight-1"));
        })
        .await;
}
