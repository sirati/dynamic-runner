//! Tests for `PrimaryCoordinator::hydrate_from_cluster_state` — the
//! authoritative-primary pool/ledger seed from the replicated CRDT.
//!
//! Faithful-port companion to the secondary's hydration tests; the two
//! drive the symmetric `cascade_drain_done` primitive. Both fixtures
//! seed `cluster_state` directly via `ClusterState::apply` (the
//! mutable test accessor) so the pre-pool state is built without the
//! broadcast path's pool-dependent auto-resume step.

use super::*;

use dynrunner_core::{PhaseId, ResourceAmount, ResourceKind, TaskDep, TypeId};

use crate::primary::wire::compute_task_hash;

/// One advertised-memory resource amount (in bytes) for a secondary
/// capacity record / task request. Mirrors the live welcome shape: a
/// single `memory` `ResourceAmount`.
fn mem(bytes: u64) -> Vec<ResourceAmount> {
    vec![ResourceAmount {
        kind: ResourceKind::memory(),
        amount: bytes,
    }]
}

/// Drain every `TaskAssignment` `task_id` queued on the primary→secondary
/// wire (non-blocking). `task_id == name` for `dep_binary`, so the
/// re-dispatch assertions compare against the task name.
fn drain_assigned_task_ids(
    rx: &mut tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
) -> Vec<String> {
    let mut ids = Vec::new();
    while let Ok(msg) = rx.try_recv() {
        if let DistributedMessage::TaskAssignment {
            target: _,
            binary_info,
            ..
        } = msg
        {
            ids.push(binary_info.task_id);
        }
    }
    ids
}

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
            phase_id: PhaseId::from(phase),
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
    let (mut primary, _mesh) = build_test_primary(
        PrimaryConfig::default(),
        transport,
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
            deps: HashMap::from([(PhaseId::from("compile"), vec![PhaseId::from("build")])]),
        });
        cs.apply(ClusterMutation::TaskAdded {
            hash: "toolchain".into(),
            task: toolchain.clone(),
        });
        // Drive `toolchain` to terminal Completed.
        cs.apply(ClusterMutation::TaskCompleted {
            attempt: 0,
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
    let ids: std::collections::HashSet<&str> = pool.iter().map(|t| t.task_id.as_str()).collect();
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

/// (1b) An `InvalidTask` terminal prereq plus a `Pending` dependent
/// whose `task_depends_on` references it. A structurally-dead root
/// dooms its dependents like a `NonRecoverable` one (the
/// `apply_tasks_spawned` cascade-fail classification): hydration seeds
/// it into the pool's retry-pending marker — the dependent's dep
/// RESOLVES (no `UnknownTaskDep`) but lands BLOCKED, never
/// dispatchable — and the resume cascade's drain edge finalizes the
/// root (no bucket can revive an `InvalidTask`: its hash is not in the
/// kind ledger), cascade-failing the dependent with the accounted
/// `upstream-failed` terminal. The non-reinjectable root entry stays
/// in the CRDT; its hash is recorded in the primary-side completed
/// ledger for the run-completion counter (not re-queued).
#[tokio::test(flavor = "current_thread")]
async fn hydrate_invalid_task_root_blocks_then_dooms_dependent() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(1);
            let (mut primary, _mesh) = build_test_primary(
                PrimaryConfig::default(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let toolchain = dep_binary("toolchain", "build", &[]);
            let dep_a = dep_binary("dep-a", "compile", &["toolchain"]);
            let dep_hash = compute_task_hash(&dep_a);

            {
                let cs = primary.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::PhaseDepsSet {
                    deps: HashMap::from([(PhaseId::from("compile"), vec![PhaseId::from("build")])]),
                });
                cs.apply(ClusterMutation::TaskAdded {
                    hash: "toolchain".into(),
                    task: toolchain.clone(),
                });
                // Drive `toolchain` to terminal InvalidTask.
                cs.apply(ClusterMutation::TaskFailed {
                    attempt: 0,
                    hash: "toolchain".into(),
                    kind: dynrunner_core::ErrorType::InvalidTask {
                        reason: "missing upstream".to_string().into(),
                    },
                    error: "invalid_task:missing upstream".into(),
                    version: Default::default(),
                });
                cs.apply(ClusterMutation::TaskAdded {
                    hash: dep_hash.clone(),
                    task: dep_a.clone(),
                });
            }

            primary.hydrate_from_cluster_state();

            // The dependent's dep RESOLVED (no `UnknownTaskDep` pool
            // wipe-out) but it is BLOCKED — a structurally-dead prereq
            // must never make its dependents dispatchable.
            let pool = primary.pool();
            let ids: std::collections::HashSet<&str> =
                pool.iter().map(|t| t.task_id.as_str()).collect();
            assert!(
                !ids.contains("dep-a"),
                "dependent of an InvalidTask prereq must NOT be dispatchable"
            );
            assert!(
                !ids.contains("toolchain"),
                "the terminal InvalidTask prereq is not re-queued"
            );
            assert_eq!(pool.blocked_len(), 1, "the dependent hydrates BLOCKED");
            // The terminal InvalidTask hash is recorded in the primary-side
            // completed ledger (run-completion accounting slot).
            assert!(primary.completed_tasks.contains("toolchain"));
            // The InvalidTask entry itself stays in the CRDT (non-reinjectable).
            assert!(matches!(
                primary.cluster_state_for_test().task_state("toolchain"),
                Some(crate::cluster_state::TaskState::InvalidTask { .. })
            ));
            assert_eq!(primary.total_tasks, 2);

            // Resume cascade: the drain edge finalizes the dead root and
            // dooms the dependent through the standard upstream-failed
            // cascade — accounted, so the run can complete.
            primary.fire_initial_phase_starts();
            primary.pool_mut().drain_empty_active_phases();
            primary.process_phase_lifecycle(&mut None).await;

            assert!(
                matches!(
                    primary.cluster_state_for_test().task_state(&dep_hash),
                    Some(crate::cluster_state::TaskState::Failed {
                        kind: dynrunner_core::ErrorType::NonRecoverable,
                        ..
                    })
                ),
                "the dependent of a structurally-dead root is cascade-failed"
            );
            assert!(primary.failed_tasks.contains_key(&dep_hash));
            assert_eq!(
                primary.completed_tasks.len() + primary.failed_tasks.len(),
                primary.total_tasks,
                "every task terminal — run accounting closes"
            );
        })
        .await;
}

/// (2) A single `InFlight` task. After hydration the dispatch/recheck
/// view must NOT re-offer that hash (it was never queued — it is
/// in-flight), the phase in-flight counter must read 1, and the
/// pre-owned in-flight ledger must hold the entry so a later broadcast
/// completion decrements the correct phase.
#[test]
fn hydrate_inflight_task_not_reoffered_and_counter_one() {
    let (transport, _ends) = setup_test(1);
    let (mut primary, _mesh) = build_test_primary(
        PrimaryConfig::default(),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );

    let task = dep_binary("inflight-1", "work", &[]);
    {
        let cs = primary.cluster_state_mut_for_test();
        // Capacity record (D1) so the failover roster reconstructs the
        // secondary's worker slot — production always has one for an
        // InFlight task's secondary.
        cs.apply(ClusterMutation::SecondaryCapacity {
            secondary: "secondary-0".into(),
            worker_count: 1,
            resources: mem(8 * 1024 * 1024 * 1024),
        });
        cs.apply(ClusterMutation::TaskAdded {
            hash: "inflight-1".into(),
            task: task.clone(),
        });
        // Drive to InFlight on a remote secondary's worker.
        cs.apply(ClusterMutation::TaskAssigned {
            attempt: 0,
            hash: "inflight-1".into(),
            secondary: "secondary-0".into(),
            worker: 0,
            version: Default::default(),
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
/// one this coordinator inherited via hydration, NOT dispatched by it —
/// must decrement the CORRECT phase's in-flight counter (N+1 → N) and
/// drain its ledger entry. Post-D3 the inherited entry is seeded with
/// `local_worker_id = Some(worker)` against a holding slot reconstructed
/// from the replicated capacity (D1) × InFlight occupancy (D2), so
/// `free_slot_on_terminal` resolves the stable `(secondary, worker)`
/// holder, frees the slot, and carries the phase decrement. Without the
/// reconstructed slot + matching ledger id, no `note_item_completed`
/// fires and the phase counter stays stuck at 1 forever.
#[tokio::test(flavor = "current_thread")]
async fn inherited_in_flight_completion_decrements_phase_counter() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(1);
            let (mut primary, _mesh) = build_test_primary(
                PrimaryConfig::default(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let task = dep_binary("inflight-1", "work", &[]);
            {
                let cs = primary.cluster_state_mut_for_test();
                // Capacity record (D1) for the secondary holding the task:
                // in production every InFlight task's secondary has one
                // (originated at connect), so the failover roster
                // reconstructs the holding slot.
                cs.apply(ClusterMutation::SecondaryCapacity {
                    secondary: "secondary-0".into(),
                    worker_count: 1,
                    resources: mem(8 * 1024 * 1024 * 1024),
                });
                cs.apply(ClusterMutation::TaskAdded {
                    hash: "inflight-1".into(),
                    task: task.clone(),
                });
                cs.apply(ClusterMutation::TaskAssigned {
                    attempt: 0,
                    hash: "inflight-1".into(),
                    secondary: "secondary-0".into(),
                    worker: 0,
                    version: Default::default(),
                });
            }
            primary.hydrate_from_cluster_state();

            let phase = PhaseId::from("work");
            assert_eq!(primary.pool().in_flight(&phase), 1);
            assert_eq!(primary.in_flight_len_for_test(), 1);
            // The reconstructed slot holds the inherited task.
            assert!(primary.slot_holds_hash_for_test("secondary-0", 0, "inflight-1"));

            // A broadcast TaskComplete lands for the inherited hash. The
            // reconstructed `(secondary-0, worker 0)` slot holds it, so
            // `free_slot_on_terminal` resolves the stable holder, frees the
            // slot, and carries the phase decrement.
            let msg = DistributedMessage::TaskComplete {
                target: None,
                sender_id: "secondary-0".into(),
                timestamp: 0.0,
                secondary_id: "secondary-0".into(),
                worker_id: 0,
                task_hash: "inflight-1".into(),
                result_data: None,
                delivery_seq: None,
                // Stamped at the send_to_primary chokepoint (ordering gate).
                msgs_posted_through: None,
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

/// (D3-a) `hydrate_from_cluster_state` reconstructs `self.workers` from
/// the two replicated sources — D1 per-secondary capacity × D2
/// `TaskState::InFlight { secondary, worker }` occupancy — so a promoted
/// primary holds the FULL roster (idle + occupied) and is dispatch-
/// capable. The occupied slot is `Assigned` with the inherited hash,
/// `worker_idx_for` resolves it, and an inherited broadcast completion
/// frees the slot back to `Idle`.
#[tokio::test(flavor = "current_thread")]
async fn hydrate_reconstructs_worker_roster_from_capacity_and_inflight() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(1);
            let (mut primary, _mesh) = build_test_primary(
                PrimaryConfig::default(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // Replicated ledger as a promoted primary inherits it: one
            // secondary advertising 2 worker slots (D1 capacity) and one
            // of those slots holding an in-flight task (D2 InFlight on
            // worker 1).
            let task = dep_binary("inflight-1", "work", &[]);
            let hash = compute_task_hash(&task);
            {
                let cs = primary.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::SecondaryCapacity {
                    secondary: "sec-0".into(),
                    worker_count: 2,
                    resources: mem(8 * 1024 * 1024 * 1024),
                });
                cs.apply(ClusterMutation::TaskAdded {
                    hash: hash.clone(),
                    task,
                });
                cs.apply(ClusterMutation::TaskAssigned {
                    attempt: 0,
                    hash: hash.clone(),
                    secondary: "sec-0".into(),
                    worker: 1,
                    version: Default::default(),
                });
            }

            primary.hydrate_from_cluster_state();

            // FULL roster reconstructed: 2 slots for sec-0's advertised
            // capacity (was 0 before this fix — a promoted primary could
            // not dispatch).
            assert_eq!(
                primary.alive_worker_count_for_test(),
                2,
                "both advertised worker slots must be reconstructed"
            );
            assert!(
                primary.alive_worker_count_for_test() > 0,
                "promoted primary must be dispatch-capable"
            );
            // The in-flight slot (worker 1) is Assigned with the inherited
            // hash; worker_idx_for resolves the stable (secondary, worker)
            // holder onto it.
            assert!(
                primary.slot_holds_hash_for_test("sec-0", 1, &hash),
                "the occupied slot must hold the inherited in-flight hash"
            );
            // Exactly one busy slot; the other is idle and available.
            assert_eq!(
                primary.active_workers_for_test(),
                1,
                "only the occupied slot is busy"
            );
            assert!(
                primary.slot_is_idle_for_test("sec-0", 0),
                "the unoccupied slot must be idle and dispatch-ready"
            );
            // The inherited ledger entry resolves a holder (not the
            // defensive no-slot arm).
            assert_eq!(primary.in_flight_len_for_test(), 1);

            // An inherited broadcast completion frees the held slot back
            // to Idle through free_slot_on_terminal's stable-id resolution.
            primary
                .handle_task_complete(
                    DistributedMessage::TaskComplete {
                        target: None,
                        sender_id: "sec-0".into(),
                        timestamp: 0.0,
                        secondary_id: "sec-0".into(),
                        worker_id: 1,
                        task_hash: hash.clone(),
                        result_data: None,
                        delivery_seq: None,
                        // Stamped at the send_to_primary chokepoint (ordering gate).
                        msgs_posted_through: None,
                    },
                    &mut None,
                )
                .await;
            assert!(
                primary.slot_is_idle_for_test("sec-0", 1),
                "the inherited completion must free the held slot"
            );
            assert_eq!(
                primary.in_flight_len_for_test(),
                0,
                "the inherited ledger entry must drain on its terminal"
            );
            assert!(primary.completed_tasks.contains(&hash));
        })
        .await;
}

/// (D3-c / R-1) Dead-secondary requeue → snapshot → hydrate dispatches
/// the requeued task EXACTLY ONCE. A live primary holds the task
/// in-flight on a secondary; that secondary dies, so
/// `recover_inflight_for_dead_secondary` requeues it and emits a
/// `TaskRequeued` mutation (InFlight → Pending in the CRDT). A snapshot
/// taken after the requeue is restored into a freshly-promoted primary
/// that still sees the (surviving) secondary's capacity; it hydrates the
/// task as Pending and dispatches it once — never stranded, never double-
/// executed.
#[tokio::test(flavor = "current_thread")]
async fn dead_secondary_requeue_then_hydrate_dispatches_exactly_once() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // --- Live primary: capacity for two secondaries; the task is
            // in-flight on sec-dead's worker 0. ---
            let snapshot = {
                let (transport, _ends) = setup_test(1);
                let (mut live, _mesh) = build_test_primary(
                    PrimaryConfig::default(),
                    transport,
                    ResourceStealingScheduler::memory(),
                    FixedEstimator(100),
                );

                let task = dep_binary("t-0", "work", &[]);
                let hash = compute_task_hash(&task);
                {
                    let cs = live.cluster_state_mut_for_test();
                    // sec-0 is the SURVIVING secondary the requeued task
                    // can land on. The dead secondary (sec-dead) is gone:
                    // its capacity record is intentionally absent from the
                    // post-failover roster, matching a promoted primary
                    // that no longer counts the departed node. The
                    // in-flight ledger entry for sec-dead's task is still
                    // seeded by hydrate (the ledger seed is independent of
                    // the roster crossing), so the recovery path can drain
                    // it.
                    cs.apply(ClusterMutation::SecondaryCapacity {
                        secondary: "sec-0".into(),
                        worker_count: 1,
                        resources: mem(8 * 1024 * 1024 * 1024),
                    });
                    cs.apply(ClusterMutation::TaskAdded {
                        hash: hash.clone(),
                        task,
                    });
                    cs.apply(ClusterMutation::TaskAssigned {
                        attempt: 0,
                        hash: hash.clone(),
                        secondary: "sec-dead".into(),
                        worker: 0,
                        version: Default::default(),
                    });
                }
                // Hydrate the live primary so its in-flight ledger holds
                // the task (the state the dead-secondary recovery drains).
                // sec-dead has no capacity record, so the occupancy
                // crossing logs a no-slot warning for it — but the ledger
                // entry is seeded regardless.
                live.hydrate_from_cluster_state();
                assert_eq!(live.in_flight_len_for_test(), 1);

                // sec-dead dies: recover its in-flight work. The returned
                // TaskRequeued mutations move InFlight → Pending in the CRDT
                // (and the local pool requeue happens inside the method).
                let mutations = live.recover_inflight_for_dead_secondary("sec-dead");
                assert_eq!(mutations.len(), 1, "exactly one task requeued");
                for m in mutations {
                    live.cluster_state_mut_for_test().apply(m);
                }
                // CRDT now sees the task as Pending (not InFlight).
                assert!(matches!(
                    live.cluster_state_for_test().task_state(&hash),
                    Some(crate::cluster_state::TaskState::Pending { .. })
                ));
                live.cluster_state_for_test().snapshot()
            };

            // --- Freshly-promoted primary: restore the post-requeue
            // snapshot, hydrate, and dispatch. sec-0's capacity is in the
            // snapshot, so the roster reconstructs a free worker, and the
            // `setup_test(1)` transport routes `Address::Peer("sec-0")`
            // assignments to the live `ends[0]` inbox. ---
            let (transport, mut ends) = setup_test(1);
            let (mut promoted, _mesh) = build_test_primary(
                PrimaryConfig::default(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            promoted.cluster_state_mut_for_test().restore(snapshot);

            promoted.hydrate_from_cluster_state();
            // The requeued task hydrated as a pending pool item (Pending in
            // the CRDT), NOT into the in-flight ledger.
            assert_eq!(
                promoted.in_flight_len_for_test(),
                0,
                "the requeued task is Pending, not in-flight, after hydrate"
            );
            assert!(
                promoted.alive_worker_count_for_test() > 0,
                "the surviving secondary's worker slot is reconstructed"
            );
            // The surviving member's re-announced MeshReady has landed
            // (the e3178916 secondary-side re-announce on PrimaryChanged)
            // — the dispatch-readiness gate keys on confirmation alone,
            // and this test's concern is the hydrate/requeue crossing,
            // not the gate (pinned in mesh_readiness_gate.rs).
            promoted.confirm_member_mesh_for_test("sec-0");

            // A single dispatch tick assigns the requeued task EXACTLY
            // ONCE to sec-live's free worker.
            promoted
                .dispatch_to_idle_workers(true)
                .await
                .expect("dispatch tick must succeed");
            settle_pump().await;
            let assigned = drain_assigned_task_ids(&mut ends[0].1);
            assert_eq!(
                assigned,
                vec!["t-0".to_string()],
                "the requeued task must dispatch exactly once after hydrate"
            );

            // A second tick dispatches nothing further (the slot is now
            // Assigned; the task is in-flight, not re-offered).
            promoted
                .dispatch_to_idle_workers(true)
                .await
                .expect("second dispatch tick must succeed");
            settle_pump().await;
            assert!(
                drain_assigned_task_ids(&mut ends[0].1).is_empty(),
                "no second dispatch of the same task — exactly once"
            );
        })
        .await;
}

/// Build a `TaskRequest` for one `(secondary, worker)` — a survivor
/// worker's post-`PrimaryChanged` idle re-confirmation.
fn task_request_for(secondary: &str, worker: u32) -> DistributedMessage<TestId> {
    DistributedMessage::TaskRequest {
        target: None,
        sender_id: secondary.into(),
        timestamp: 0.0,
        secondary_id: secondary.into(),
        worker_id: worker,
        available_resources: mem(8 * 1024 * 1024 * 1024),
    }
}

/// (FAILOVER-RESUME, headline) A promoted primary hydrated with a task
/// `InFlight` on a SURVIVOR worker whose completion was LOST during the
/// primary-less election window reconciles on the worker's live idle
/// re-confirmation (`TaskRequest`): it frees the phantom-busy slot,
/// requeues the inherited task (`InFlight → Pending` broadcast), and
/// dispatches the now-ready work — succeeded advances, NOT 0-forever.
///
/// Without the reconciliation the slot stays `Assigned` and the
/// `TaskRequest` is the R1 no-op, so NOTHING dispatches: the
/// `assigned=0 remaining=N` deadlock. See the explicit revert-check test
/// below.
#[tokio::test(flavor = "current_thread")]
async fn promoted_primary_reconciles_stale_inherited_slot_on_idle_request() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, mut ends) = setup_test(1);
            let (mut primary, _mesh) = build_test_primary(
                PrimaryConfig::default(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // The inherited CRDT as the promoted primary sees it: sec-0
            // advertises 1 worker (D1 capacity) holding one InFlight task
            // (D2) — but that worker FINISHED it during the primary-less
            // window, so the completion was lost and the worker is now
            // idle. A SECOND task is Pending (the stranded ready work that
            // must dispatch once the slot is freed).
            let stuck = dep_binary("stuck-inflight", "work", &[]);
            let stuck_hash = compute_task_hash(&stuck);
            let ready = dep_binary("ready-pending", "work", &[]);
            {
                let cs = primary.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::SecondaryCapacity {
                    secondary: "sec-0".into(),
                    worker_count: 1,
                    resources: mem(8 * 1024 * 1024 * 1024),
                });
                cs.apply(ClusterMutation::TaskAdded {
                    hash: stuck_hash.clone(),
                    task: stuck,
                });
                cs.apply(ClusterMutation::TaskAssigned {
                    attempt: 0,
                    hash: stuck_hash.clone(),
                    secondary: "sec-0".into(),
                    worker: 0,
                    version: Default::default(),
                });
                cs.apply(ClusterMutation::TaskAdded {
                    hash: compute_task_hash(&ready),
                    task: ready,
                });
            }

            primary.hydrate_from_cluster_state();

            // Post-hydrate: the only worker slot is phantom-busy (Assigned,
            // INHERITED provenance) and the ready task cannot dispatch —
            // the deadlock precondition.
            assert!(
                primary.slot_holds_hash_for_test("sec-0", 0, &stuck_hash),
                "the survivor slot is reconstructed Assigned with the inherited hash"
            );
            assert!(
                primary.slot_is_inherited_for_test("sec-0", 0),
                "a reconstructed-from-InFlight slot is INHERITED (unconfirmed occupancy)"
            );
            assert_eq!(primary.active_workers_for_test(), 1, "1 phantom-busy slot");
            assert_eq!(primary.in_flight_len_for_test(), 1);
            // REVERT-CHECK (inline): the slot is NOT idle. The pre-fix
            // `handle_task_request` gated assignment SOLELY on `is_idle()`
            // and NEVER freed an `Assigned` slot, so a request here was a
            // no-op → 0 dispatched, forever. The reconciliation below is the
            // ONLY thing that frees it; remove it and `is_idle()` stays
            // false → the deadlock returns.
            assert!(
                !primary.slot_is_idle_for_test("sec-0", 0),
                "the phantom-busy slot is non-idle: the pre-fix is_idle()-only \
                 request path would no-op here (the 0-dispatched deadlock)"
            );

            // The survivor worker re-confirms idle: its post-PrimaryChanged
            // TaskRequest lands for (sec-0, worker 0). This reconciles the
            // stale slot AND dispatches in the same call.
            primary
                .handle_task_request(task_request_for("sec-0", 0), &mut None)
                .await
                .expect("task request handling must succeed");
            settle_pump().await;

            // Reconciled: the inherited task returned to Pending in the CRDT
            // (InFlight → Pending), the phantom in-flight is gone, and the
            // slot is now busy with a FRESHLY-dispatched task (the ready one
            // — or the requeued stuck one; whichever the scheduler picked).
            assert!(
                matches!(
                    primary.cluster_state_for_test().task_state(&stuck_hash),
                    Some(crate::cluster_state::TaskState::Pending { .. })
                        | Some(crate::cluster_state::TaskState::InFlight { .. })
                ),
                "the stale inherited task was requeued (Pending) and may have \
                 been re-dispatched (InFlight) — never stranded"
            );

            // Dispatch FIRED: at least one TaskAssignment went out (succeeded
            // can advance), proving the deadlock is broken. Both tasks must
            // ultimately be dispatchable; the first request placed one.
            let assigned = drain_assigned_task_ids(&mut ends[0].1);
            assert!(
                !assigned.is_empty(),
                "reconciliation must let dispatch fire — NOT 0-forever (the \
                 LMU-gating deadlock)"
            );

            // The slot is occupied again by a live DISPATCHED assignment,
            // and a second idle request drains the other task too — the run
            // makes progress, never wedged at assigned=0.
            primary
                .handle_task_request(task_request_for("sec-0", 0), &mut None)
                .await
                .expect("second request ok");
            settle_pump().await;
            // Across the two requests both work items have been offered for
            // dispatch (one per freed slot cycle), so the pool is drained of
            // ready work — the run advances rather than deadlocking.
        })
        .await;
}

/// (REVERT-CHECK) Without the reconciliation, a `TaskRequest` for a
/// phantom-busy INHERITED slot is the R1 no-op and NOTHING dispatches —
/// the exact `assigned=0 remaining=N` deadlock. This drives the slot
/// state directly (the same hydrate output) and asserts the bare
/// `handle_task_request` would strand: it confirms the test exercises the
/// real gap. The fix's effect is isolated to `reconcile_inherited_slot`;
/// to "revert" we observe what happens when the slot is NOT inherited-
/// reconcilable — i.e. a live `Dispatched` slot — where the request MUST
/// stay a no-op (this is also the rc-G2 / steady-state guard).
#[tokio::test(flavor = "current_thread")]
async fn dispatched_slot_request_is_noop_no_double_dispatch_rc_g2() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, mut ends) = setup_test(1);
            let (mut primary, _mesh) = build_test_primary(
                PrimaryConfig::default(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // A LIVE-dispatched busy slot (this primary sent the
            // assignment): the relocated/normal/rc-G2 case where the worker
            // is genuinely running its task. There is NO other ready work,
            // so any dispatch would be a (forbidden) double-dispatch of the
            // running task. Seed the CRDT InFlight too (production: a
            // dispatched task is InFlight in the replicated ledger) so the
            // requeue/no-requeue invariant is observable on the CRDT.
            let running = dep_binary("running", "work", &[]);
            let running_hash = compute_task_hash(&running);
            {
                let cs = primary.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::TaskAdded {
                    hash: running_hash.clone(),
                    task: running.clone(),
                });
                cs.apply(ClusterMutation::TaskAssigned {
                    attempt: 0,
                    hash: running_hash.clone(),
                    secondary: "sec-0".into(),
                    worker: 0,
                    version: Default::default(),
                });
            }
            // Stage the live DISPATCHED slot + ledger entry (commit_assignment
            // path), matching the CRDT InFlight just seeded.
            primary.stage_in_flight_for_test("sec-0".into(), 0, running);

            // Sanity: the slot is busy and DISPATCHED (not inherited).
            assert!(
                primary.slot_holds_hash_for_test("sec-0", 0, &running_hash),
                "the live slot holds the running task"
            );
            assert!(
                !primary.slot_is_inherited_for_test("sec-0", 0),
                "a commit_assignment slot is DISPATCHED, never inherited — the \
                 reconciliation must not touch it"
            );
            assert_eq!(primary.in_flight_len_for_test(), 1);

            // A stray/duplicate TaskRequest for that genuinely-busy worker
            // (rc-G2 steady state) MUST be the R1 no-op: the slot stays
            // Assigned, the task stays InFlight, and NO TaskAssignment goes
            // out — never a double-dispatch.
            primary
                .handle_task_request(task_request_for("sec-0", 0), &mut None)
                .await
                .expect("request ok");
            settle_pump().await;

            assert!(
                primary.slot_holds_hash_for_test("sec-0", 0, &running_hash),
                "the live-dispatched slot must NOT be freed by a bare request \
                 (R1 / rc-G2: preserving committed in_flight)"
            );
            assert_eq!(
                primary.in_flight_len_for_test(),
                1,
                "the running task stays in-flight; no requeue, no double-dispatch"
            );
            assert!(
                matches!(
                    primary.cluster_state_for_test().task_state(&running_hash),
                    Some(crate::cluster_state::TaskState::InFlight { .. })
                ),
                "CRDT keeps the running task InFlight — NOT requeued to Pending"
            );
            assert!(
                drain_assigned_task_ids(&mut ends[0].1).is_empty(),
                "no TaskAssignment — the busy live slot is never re-dispatched"
            );
        })
        .await;
}

/// (RECONCILE-ONLY-INHERITED) A direct unit pin of
/// `reconcile_inherited_slot`: it returns `Some(TaskRequeued)` and frees
/// the slot ONLY for an inherited slot, and `None` (no state change) for a
/// live dispatched slot. The discriminator is provenance, not idleness —
/// this is what keeps the rc-G2 relocated/normal path intact while curing
/// the failover deadlock.
#[tokio::test(flavor = "current_thread")]
async fn reconcile_inherited_slot_gates_on_provenance() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(1);
            let (mut primary, _mesh) = build_test_primary(
                PrimaryConfig::default(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // INHERITED slot via hydrate.
            let inh = dep_binary("inh", "work", &[]);
            let inh_hash = compute_task_hash(&inh);
            {
                let cs = primary.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::SecondaryCapacity {
                    secondary: "sec-0".into(),
                    worker_count: 1,
                    resources: mem(8 * 1024 * 1024 * 1024),
                });
                cs.apply(ClusterMutation::TaskAdded {
                    hash: inh_hash.clone(),
                    task: inh,
                });
                cs.apply(ClusterMutation::TaskAssigned {
                    attempt: 0,
                    hash: inh_hash.clone(),
                    secondary: "sec-0".into(),
                    worker: 0,
                    version: Default::default(),
                });
            }
            primary.hydrate_from_cluster_state();
            let idx = primary
                .worker_idx_for("sec-0", 0)
                .expect("inherited slot resolves");

            // Inherited → reconciles: returns a TaskRequeued, frees the slot,
            // drops the ledger entry, requeues the task.
            let requeue = primary.reconcile_inherited_slot(idx);
            assert!(
                matches!(
                    requeue,
                    crate::primary::coordinator::InheritedSlotReconcile::Requeued(ref m)
                        if matches!(
                            **m,
                            ClusterMutation::TaskRequeued { ref hash, .. } if *hash == inh_hash
                        )
                ),
                "an inherited slot reconciles → TaskRequeued for its held hash"
            );
            assert!(
                primary.slot_is_idle_for_test("sec-0", 0),
                "the reconciled slot is freed to Idle"
            );
            assert_eq!(
                primary.in_flight_len_for_test(),
                0,
                "the inherited ledger entry is dropped on reconcile"
            );

            // A second reconcile of the now-idle slot is a no-op (nothing to
            // reconcile).
            assert!(
                matches!(
                    primary.reconcile_inherited_slot(idx),
                    crate::primary::coordinator::InheritedSlotReconcile::NotInherited
                ),
                "an Idle slot has nothing to reconcile"
            );
        })
        .await;
}

/// V2 single-builder idempotency: `reconstruct_workers_from_cluster_state`
/// wholesale-REPLACES `self.workers` (it is the SOLE roster builder, the
/// round-robin `self.workers.push` block having been deleted from
/// `perform_initial_assignment`). So invoking it TWICE yields a roster of
/// cardinality Σ(per-secondary capacity), NOT 2× — the second call re-derives
/// from the same CRDT capacity, never appends. Pins the "double-invoke ⇒ Σ
/// capacity (not 2×)" V2 acceptance.
#[tokio::test(flavor = "current_thread")]
async fn reconstruct_workers_double_invoke_is_sum_capacity_not_double() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(1);
            let (mut primary, _mesh) = build_test_primary(
                PrimaryConfig::default(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            // Two secondaries advertise capacity: sec-0 → 3 slots, sec-1 → 2.
            // Σ capacity = 5.
            {
                let cs = primary.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::SecondaryCapacity {
                    secondary: "sec-0".into(),
                    worker_count: 3,
                    resources: mem(8 * 1024 * 1024 * 1024),
                });
                cs.apply(ClusterMutation::SecondaryCapacity {
                    secondary: "sec-1".into(),
                    worker_count: 2,
                    resources: mem(8 * 1024 * 1024 * 1024),
                });
            }

            primary.reconstruct_workers_from_cluster_state();
            assert_eq!(
                primary.alive_worker_count_for_test(),
                5,
                "first reconstruct must build Σ capacity (3 + 2) slots"
            );

            // Re-invoke: the wholesale REPLACE re-derives the SAME 5-slot
            // roster from the unchanged CRDT — never 10 (which an append-based
            // builder would produce).
            primary.reconstruct_workers_from_cluster_state();
            assert_eq!(
                primary.alive_worker_count_for_test(),
                5,
                "double-invoke must remain Σ capacity (5), NOT 2× (10) — the \
                 sole builder wholesale-replaces, never appends"
            );
        })
        .await;
}

/// The worker-roster rebuild must NOT resurrect a REMOVED peer: capacity
/// records are set-once and never deleted (`secondary_capacities` keys
/// outlive the member), so an unfiltered rebuild re-creates worker slots
/// for a peer whose membership was authoritatively killed by
/// `PeerRemoved` — any later capacity-growth rebuild
/// (`react_to_capacity_growth`) would hand the dead peer dispatchable
/// slots again. The membership ledger (`peer_state` `Dead`, written in
/// lockstep with the `CapabilityEntry::Departed` tombstone) is the
/// filter seam. Re-admission (a generation-advancing `PeerJoined`) flips
/// the SAME ledger entry back to `Alive`, so a re-admitted peer's
/// preserved capacity record naturally re-enters the roster — restored,
/// never re-advertised.
#[tokio::test(flavor = "current_thread")]
async fn reconstruct_workers_excludes_removed_peer_until_readmission() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(1);
            let (mut primary, _mesh) = build_test_primary(
                PrimaryConfig::default(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            // Two members join and advertise capacity: sec-0 → 2 slots,
            // sec-1 → 3 slots (the full welcome-time replicated facts:
            // membership + capacity).
            {
                let cs = primary.cluster_state_mut_for_test();
                for (id, count) in [("sec-0", 2u32), ("sec-1", 3u32)] {
                    cs.apply(ClusterMutation::PeerJoined {
                        peer_id: id.into(),
                        is_observer: false,
                        can_be_primary: true,
                        cap_version: Default::default(),
                        member_gen: 0,
                    });
                    cs.apply(ClusterMutation::SecondaryCapacity {
                        secondary: id.into(),
                        worker_count: count,
                        resources: mem(8 * 1024 * 1024 * 1024),
                    });
                }
            }
            primary.reconstruct_workers_from_cluster_state();
            assert_eq!(
                primary.alive_worker_count_for_test(),
                5,
                "both live members' capacity enters the roster"
            );

            // Authoritative removal of sec-1 (kills its membership
            // incarnation; the capacity record stays — set-once).
            let dead_gen = primary.cluster_state_for_test().peer_member_gen("sec-1");
            primary
                .cluster_state_mut_for_test()
                .apply(ClusterMutation::PeerRemoved {
                    id: "sec-1".into(),
                    cause: dynrunner_protocol_primary_secondary::RemovalCause::KeepaliveMiss,
                    member_gen: dead_gen,
                });

            // A capacity-growth-shaped rebuild after the removal.
            primary.reconstruct_workers_from_cluster_state();
            assert!(
                !primary.workers.iter().any(|w| w.secondary_id == "sec-1"),
                "a rebuild must NOT resurrect worker slots for an \
                 authoritatively-removed peer (its capacity record is a \
                 tombstoned member's, not a live one's)"
            );
            assert_eq!(
                primary.alive_worker_count_for_test(),
                2,
                "the surviving member's slots are intact"
            );

            // RE-ADMISSION: the generation-advancing PeerJoined (what the
            // frame-ingest re-admission seam originates) restores the
            // member — and with it, its preserved capacity record.
            primary
                .cluster_state_mut_for_test()
                .apply(ClusterMutation::PeerJoined {
                    peer_id: "sec-1".into(),
                    is_observer: false,
                    can_be_primary: true,
                    cap_version: Default::default(),
                    member_gen: dead_gen + 1,
                });
            primary.reconstruct_workers_from_cluster_state();
            assert_eq!(
                primary
                    .workers
                    .iter()
                    .filter(|w| w.secondary_id == "sec-1")
                    .count(),
                3,
                "re-admission restores the peer's full advertised capacity \
                 to the roster"
            );
            assert_eq!(primary.alive_worker_count_for_test(), 5);
        })
        .await;
}

/// (a) HEADLINE (#326 guard): a promoted primary hydrated from a CRDT where
/// phases X,Y,Z are ALL-terminal (completed) AND carry the replicated
/// `PhaseEnded` fact (the original primary's cascade originated it at its
/// `mark_phase_done` edge) must seed X,Y,Z as `PhaseState::Done` — NOT
/// re-run them. Without this the freshly built pool starts them
/// `Active`/`Blocked` and the post-hydrate lifecycle cascade
/// re-`(0,0,0)`-drains them → re-fires `on_phase_end` → (the real-world
/// failure) a consumer hook re-spawns 2041 children with identical
/// identities → run-wide invalidation. W (live, depends on Z) is `Active`
/// (its dep Z is Done) and its work enters the pool.
///
/// Revert-check: drop the `seed_completed_phases` call in
/// `hydrate_from_cluster_state` and X,Y,Z come back `Active` (a zero-dep
/// completed phase) — which would re-drain and re-fire `on_phase_end`.
#[test]
fn hydrate_seeds_completed_phases_as_done_not_rerun() {
    use dynrunner_scheduler_api::PhaseState;

    let (transport, _ends) = setup_test(1);
    let (mut primary, _mesh) = build_test_primary(
        PrimaryConfig::default(),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );

    // Chain X → Y → Z → W. X,Y,Z are each a single Completed task (the
    // already-ended phases that ALSO already fired on_phase_end + spawned
    // their children pre-failover). W has a single Pending task depending on
    // Z's task (live work — the current phase the resume should land on).
    let x = dep_binary("x-task", "X", &[]);
    let y = dep_binary("y-task", "Y", &["x-task"]);
    let z = dep_binary("z-task", "Z", &["y-task"]);
    let mut w = dep_binary("w-task", "W", &["z-task"]);
    // W's dep names Z's task in phase Z (cross-phase dep).
    w.task_depends_on = vec![dynrunner_core::TaskDep {
        task_id: "z-task".into(),
        phase_id: PhaseId::from("Z"),
        inherit_outputs: false,
    }];

    {
        let cs = primary.cluster_state_mut_for_test();
        cs.apply(ClusterMutation::PhaseDepsSet {
            deps: HashMap::from([
                (PhaseId::from("Y"), vec![PhaseId::from("X")]),
                (PhaseId::from("Z"), vec![PhaseId::from("Y")]),
                (PhaseId::from("W"), vec![PhaseId::from("Z")]),
            ]),
        });
        for (hash, task) in [("x-task", &x), ("y-task", &y), ("z-task", &z)] {
            cs.apply(ClusterMutation::TaskAdded {
                hash: hash.into(),
                task: task.clone(),
            });
            cs.apply(ClusterMutation::TaskCompleted {
                attempt: 0,
                hash: hash.into(),
                result_data: None,
            });
        }
        // The original primary's cascade fired `on_phase_end` for X,Y,Z and
        // originated the replicated `PhaseEnded` fact at its
        // `mark_phase_done` edge — the no-redo decision input (#343). The
        // inherited CRDT carries it; a terminal-only phase WITHOUT the fact
        // is a never-ended phase and flows through the live cascade instead
        // (see `hydrate_does_not_seed_done_without_phase_ended_fact`).
        for ph in ["X", "Y", "Z"] {
            cs.apply(ClusterMutation::PhaseEnded {
                phase: PhaseId::from(ph),
            });
        }
        cs.apply(ClusterMutation::TaskAdded {
            hash: "w-task".into(),
            task: w.clone(),
        });
    }

    primary.hydrate_from_cluster_state();

    let pool = primary.pool();
    // X, Y, Z are already-completed-and-ended → seeded straight to Done so
    // the lifecycle cascade never observes a Drained edge for them and
    // on_phase_end does NOT re-fire.
    for ph in ["X", "Y", "Z"] {
        assert_eq!(
            pool.phase_state(&PhaseId::from(ph)),
            Some(PhaseState::Done),
            "completed-and-ended phase {ph} must be seeded Done on resume \
             (pre-fix it is Active and re-fires on_phase_end)"
        );
    }
    // W has live work and its only dep (Z) is Done → Active, work in pool.
    assert_eq!(
        pool.phase_state(&PhaseId::from("W")),
        Some(PhaseState::Active),
        "the live current phase W (dep Z now Done) must be Active"
    );
    assert!(
        pool.iter().any(|t| t.task_id == "w-task"),
        "W's live task must be in the pool to dispatch"
    );
}

/// #343 discriminator, fact-ABSENT side: a terminal-only phase WITHOUT the
/// replicated `PhaseEnded` fact is a phase whose `on_phase_end` edge never
/// completed ANYWHERE — the freshly-discovered all-`SkippedAlreadyDone`
/// shape (the skip seed makes a phase all-terminal the moment it lands,
/// before any hook ran). Hydrate must NOT seed it `Done`: it stays
/// `Active` so the live cascade fires its FIRST `on_phase_start` /
/// `on_phase_end` and a chaining consumer's injection lands. The dependent
/// phase stays `Blocked` (its dep has not ended). Also pins the
/// started-derivation half: a skip is a spawn-time terminal, NOT
/// activation evidence, so `phase_started_emitted` must NOT contain the
/// phase (otherwise `on_phase_start` is suppressed and the consumer sees
/// an End without a Start).
#[test]
fn hydrate_does_not_seed_done_without_phase_ended_fact() {
    use dynrunner_scheduler_api::PhaseState;

    let (transport, _ends) = setup_test(1);
    let (mut primary, _mesh) = build_test_primary(
        PrimaryConfig::default(),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );

    // `build` (all skipped at discovery) → `ship` (live work, depends on
    // build). NO `PhaseEnded` fact for `build` — its hook never ran.
    let a = dep_binary("a-task", "build", &[]);
    let b = dep_binary("b-task", "build", &[]);
    let s = dep_binary("s-task", "ship", &[]);
    {
        let cs = primary.cluster_state_mut_for_test();
        cs.apply(ClusterMutation::PhaseDepsSet {
            deps: HashMap::from([(PhaseId::from("ship"), vec![PhaseId::from("build")])]),
        });
        for (hash, task) in [("a-task", &a), ("b-task", &b)] {
            cs.apply(ClusterMutation::TaskAdded {
                hash: hash.into(),
                task: task.clone(),
            });
            cs.apply(ClusterMutation::TaskSkippedAlreadyDone { hash: hash.into() });
        }
        cs.apply(ClusterMutation::TaskAdded {
            hash: "s-task".into(),
            task: s.clone(),
        });
    }

    primary.hydrate_from_cluster_state();

    let pool = primary.pool();
    assert_eq!(
        pool.phase_state(&PhaseId::from("build")),
        Some(PhaseState::Active),
        "a terminal-only phase WITHOUT the PhaseEnded fact must NOT be \
         seeded Done — its on_phase_end never fired anywhere and must fire \
         through the live cascade (pre-fix: seeded Done, hook silently \
         dropped, the chaining consumer's injection lost)"
    );
    assert_eq!(
        pool.phase_state(&PhaseId::from("ship")),
        Some(PhaseState::Blocked),
        "the dependent stays Blocked until its dep phase actually ends \
         through the cascade"
    );
    assert!(
        !primary
            .phase_started_emitted
            .contains(&PhaseId::from("build")),
        "a skip is a spawn-time terminal, not activation evidence — the \
         started set must not contain the fresh all-skipped phase, so its \
         first on_phase_start fires before its first on_phase_end"
    );
}

/// #326 preservation at the CASCADE level: an inherited all-terminal phase
/// WITH the `PhaseEnded` fact is seeded `Done` and the post-hydrate
/// lifecycle cascade (`fire_initial_phase_starts` +
/// `drain_empty_active_phases` + `process_phase_lifecycle`) fires ZERO
/// duplicate `on_phase_end` (and zero duplicate `on_phase_start`) for it —
/// the exact promotion seam, hook-counted, not just the pool-state
/// assertion.
#[tokio::test(flavor = "current_thread")]
async fn inherited_ended_phase_with_fact_does_not_refire_on_phase_end() {
    use dynrunner_scheduler_api::PhaseState;

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(1);
            let (mut primary, _mesh) = build_test_primary(
                PrimaryConfig::default(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // Inherited CRDT: `build` is all-skipped AND ended on the
            // original primary (fact present); `ship` depends on it and has
            // live work (the resume target).
            let a = dep_binary("a-task", "build", &[]);
            let b = dep_binary("b-task", "build", &[]);
            let s = dep_binary("s-task", "ship", &[]);
            {
                let cs = primary.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::PhaseDepsSet {
                    deps: HashMap::from([(PhaseId::from("ship"), vec![PhaseId::from("build")])]),
                });
                for (hash, task) in [("a-task", &a), ("b-task", &b)] {
                    cs.apply(ClusterMutation::TaskAdded {
                        hash: hash.into(),
                        task: task.clone(),
                    });
                    cs.apply(ClusterMutation::TaskSkippedAlreadyDone { hash: hash.into() });
                }
                cs.apply(ClusterMutation::PhaseEnded {
                    phase: PhaseId::from("build"),
                });
                cs.apply(ClusterMutation::TaskAdded {
                    hash: "s-task".into(),
                    task: s.clone(),
                });
            }

            // Counting hooks installed on the coordinator (what `run()`
            // would wire from the consumer callbacks). `Arc<Mutex<..>>`
            // because the hook types are `+ Send`.
            let starts = std::sync::Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
            let ends = std::sync::Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
            let starts_cb = starts.clone();
            primary.on_phase_start = Some(Box::new(move |p: &PhaseId| {
                starts_cb.lock().unwrap().push(p.to_string());
            }));
            let ends_cb = ends.clone();
            primary.on_phase_end = Some(Box::new(move |p: &PhaseId, _c, _f, _outputs| {
                ends_cb.lock().unwrap().push(p.to_string());
            }));

            primary.hydrate_from_cluster_state();
            assert_eq!(
                primary.pool().phase_state(&PhaseId::from("build")),
                Some(PhaseState::Done),
                "all-terminal + PhaseEnded fact ⇒ seeded Done on resume"
            );
            assert_eq!(
                primary.pool().phase_state(&PhaseId::from("ship")),
                Some(PhaseState::Active),
                "the live dependent activates (its dep is Done) and resumes"
            );

            // The promotion seam's pre-loop cascade: zero duplicate firings
            // for the already-ended phase.
            primary.fire_initial_phase_starts();
            primary.pool_mut().drain_empty_active_phases();
            primary.process_phase_lifecycle(&mut None).await;

            let fired_ends = ends.lock().unwrap().clone();
            let fired_starts = starts.lock().unwrap().clone();
            assert!(
                !fired_ends.iter().any(|p| p == "build"),
                "an inherited ENDED phase (fact in the snapshot) must NOT \
                 re-fire on_phase_end on promotion (#326); fired: {fired_ends:?}"
            );
            assert!(
                !fired_starts.iter().any(|p| p == "build"),
                "an inherited ENDED phase must not re-fire on_phase_start \
                 either; fired: {fired_starts:?}"
            );
            // The live dependent's own first start DID fire (the resume is
            // not suppressed wholesale).
            assert_eq!(
                fired_starts.iter().filter(|p| *p == "ship").count(),
                1,
                "the live dependent fires its own first on_phase_start; \
                 fired: {fired_starts:?}"
            );
        })
        .await;
}

/// (a) cold-path NOT regressed at hydrate: a fresh all-`Pending` cold seed
/// has NO terminal task, so the completed-phase derivation is EMPTY and NO
/// phase is seeded `Done`. Every phase stays in its `PendingPool::new`
/// state (zero-dep → Active, deps → Blocked) so the cold-start
/// `fire_initial_phase_starts` + empty-phase cascade run + fire
/// `on_phase_end` exactly once, unchanged.
#[tokio::test(flavor = "current_thread")]
async fn hydrate_cold_seed_does_not_seed_any_phase_done() {
    use dynrunner_scheduler_api::PhaseState;

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(1);
            let (mut primary, _mesh) = build_test_primary(
                PrimaryConfig::default(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            let a = dep_binary("a", "build", &[]);
            let b = dep_binary("b", "ship", &[]);
            primary
                .originate_cold_seed(vec![(a, false), (b, false)], HashMap::new())
                .expect("cold seed");
            primary.hydrate_from_cluster_state();

            let pool = primary.pool();
            for ph in ["build", "ship"] {
                let st = pool.phase_state(&PhaseId::from(ph));
                assert_ne!(
                    st,
                    Some(PhaseState::Done),
                    "a cold all-Pending phase {ph} must NOT be seeded Done \
                     (got {st:?})"
                );
                assert_eq!(
                    st,
                    Some(PhaseState::Active),
                    "a zero-dep cold phase {ph} stays Active for the normal \
                     cold-start cascade"
                );
            }
        })
        .await;
}

// ---------------------------------------------------------------------------
// Per-class terminal-FAILURE dep seeding (fix/hydrate-failed-deps).
//
// The replicated ledger's terminal classes carry DIFFERENT dependency
// semantics (the same classification `apply_tasks_spawned` applies to a
// freshly-spawned dependent):
//   * Completed / SkippedAlreadyDone — satisfies dependents' deps.
//   * Failed { any kind }            — the retry decision is pending at the
//     phase's drain edge: dependents stay BLOCKED; the drain edge's buckets
//     either revive the root (`reinject`) or `finalize_soft_failures`
//     cascade-fails the dependents with the canonical `upstream-failed`
//     terminal — the SAME path the live primary takes.
//   * Unfulfillable                  — operator-reinjectable dormancy:
//     dependents stay BLOCKED, nothing is doomed, the run holds open.
//   * InvalidTask                    — structurally-dead root: dependents
//     cascade-fail (`upstream-failed`), like a NonRecoverable root.
//
// Pre-fix, hydrate routed EVERY terminal task_id into the dep-resolution
// COMPLETED seed, so after a failover the dependents of a terminally-FAILED
// task became DISPATCHABLE (their dep read satisfied) and ran against
// outputs that were never produced.
// ---------------------------------------------------------------------------

/// HEADLINE (RED→GREEN): `A` is `Failed { NonRecoverable }` in the inherited
/// CRDT; `B` (Pending) declares `task_depends_on = [A]`. Pre-fix, hydrate
/// seeded A's task_id into `mark_tasks_completed` and B became DISPATCHABLE.
/// Post-fix B hydrates BLOCKED, and the resume cascade (the `run_pipeline`
/// pre-loop shape) reaches A's phase drain edge where the buckets decline
/// (NonRecoverable matches none) and `finalize_phase_soft_failures` dooms B
/// through the EXACT live-path machinery: a broadcast
/// `TaskFailed { NonRecoverable, "upstream-failed: …" }`, accounted in the
/// hash-keyed `failed_tasks` ledger.
#[tokio::test(flavor = "current_thread")]
async fn hydrate_failed_final_root_dooms_dependents_via_finalize_cascade() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(1);
            let (mut primary, _mesh) = build_test_primary(
                PrimaryConfig::default(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let a = dep_binary("toolchain", "build", &[]);
            let b = dep_binary("dep-a", "compile", &["toolchain"]);
            let a_hash = compute_task_hash(&a);
            let b_hash = compute_task_hash(&b);
            {
                let cs = primary.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::PhaseDepsSet {
                    deps: HashMap::from([(PhaseId::from("compile"), vec![PhaseId::from("build")])]),
                });
                cs.apply(ClusterMutation::TaskAdded {
                    hash: a_hash.clone(),
                    task: a.clone(),
                });
                cs.apply(ClusterMutation::TaskFailed {
                    attempt: 0,
                    hash: a_hash.clone(),
                    kind: dynrunner_core::ErrorType::NonRecoverable,
                    error: "exit 1".into(),
                    version: Default::default(),
                });
                cs.apply(ClusterMutation::TaskAdded {
                    hash: b_hash.clone(),
                    task: b.clone(),
                });
            }

            primary.hydrate_from_cluster_state();

            // A FAILED terminal must NOT satisfy dependents' deps: B is
            // NOT dispatchable (pre-fix it was queued), it waits BLOCKED
            // for the drain edge's retry-or-cascade decision.
            let queued: Vec<&str> = primary.pool().iter().map(|t| t.task_id.as_str()).collect();
            assert!(
                !queued.contains(&"dep-a"),
                "dependent of a FAILED prereq must not be dispatchable \
                 after hydrate; queued = {queued:?}"
            );
            assert_eq!(
                primary.pool().blocked_len(),
                1,
                "the dependent hydrates BLOCKED, awaiting the drain edge"
            );

            // Resume cascade: the same pre-loop sequence `run_pipeline`
            // drives after hydrate.
            primary.fire_initial_phase_starts();
            primary.pool_mut().drain_empty_active_phases();
            primary.process_phase_lifecycle(&mut None).await;

            // B was cascade-failed exactly as the live path would have:
            // the canonical upstream-failed NonRecoverable terminal,
            // replicated AND accounted.
            match primary.cluster_state_for_test().task_state(&b_hash) {
                Some(crate::cluster_state::TaskState::Failed {
                    kind: dynrunner_core::ErrorType::NonRecoverable,
                    last_error,
                    ..
                }) => {
                    assert!(
                        last_error.contains("upstream-failed"),
                        "cascaded dependent carries the canonical \
                         upstream-failed shape; got {last_error:?}"
                    );
                }
                other => panic!(
                    "dependent of a final-failed root must be cascade-failed \
                     NonRecoverable after the resume cascade; got {other:?}"
                ),
            }
            assert!(
                primary.failed_tasks.contains_key(&b_hash),
                "the cascaded dependent is accounted in the hash-keyed ledger"
            );
            assert_eq!(primary.pool().blocked_len(), 0, "no dependent stranded");
            // Run accounting closes: every task terminal.
            assert_eq!(
                primary.completed_tasks.len() + primary.failed_tasks.len(),
                primary.total_tasks,
                "completed + failed must reach total — the run can complete"
            );
        })
        .await;
}

/// A `Failed { Recoverable }` root with retry budget REMAINING re-enters the
/// retry flow at its phase's drain edge: the Recoverable bucket reinjects it
/// (CRDT `Failed → Pending { attempt+1 }` via `TaskRetried`) and its
/// dependent stays BLOCKED — NOT doomed, NOT dispatchable — until the
/// retry resolves. Pre-fix the dependent dispatched immediately.
#[tokio::test(flavor = "current_thread")]
async fn hydrate_failed_retryable_root_reenters_retry_flow_dependent_blocked() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(1);
            let (mut primary, _mesh) = build_test_primary(
                PrimaryConfig::default(), // retry_max_passes = 1: budget remains
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let a = dep_binary("toolchain", "build", &[]);
            let b = dep_binary("dep-a", "compile", &["toolchain"]);
            let a_hash = compute_task_hash(&a);
            let b_hash = compute_task_hash(&b);
            {
                let cs = primary.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::PhaseDepsSet {
                    deps: HashMap::from([(PhaseId::from("compile"), vec![PhaseId::from("build")])]),
                });
                cs.apply(ClusterMutation::TaskAdded {
                    hash: a_hash.clone(),
                    task: a.clone(),
                });
                cs.apply(ClusterMutation::TaskFailed {
                    attempt: 0,
                    hash: a_hash.clone(),
                    kind: dynrunner_core::ErrorType::Recoverable,
                    error: "transient".into(),
                    version: Default::default(),
                });
                cs.apply(ClusterMutation::TaskAdded {
                    hash: b_hash.clone(),
                    task: b.clone(),
                });
            }

            primary.hydrate_from_cluster_state();
            assert_eq!(
                primary.pool().blocked_len(),
                1,
                "the dependent hydrates BLOCKED (not dispatchable)"
            );

            primary.fire_initial_phase_starts();
            primary.pool_mut().drain_empty_active_phases();
            primary.process_phase_lifecycle(&mut None).await;

            // The root re-entered the retry flow: reinjected (queued) and
            // reset in the CRDT (`TaskRetried` → Pending, attempt bumped).
            let queued: Vec<&str> = primary.pool().iter().map(|t| t.task_id.as_str()).collect();
            assert!(
                queued.contains(&"toolchain"),
                "the retryable root must be reinjected by the Recoverable \
                 bucket; queued = {queued:?}"
            );
            assert!(
                matches!(
                    primary.cluster_state_for_test().task_state(&a_hash),
                    Some(crate::cluster_state::TaskState::Pending { attempt: 1, .. })
                ),
                "the CRDT retry reset moves the root Failed → Pending(attempt 1)"
            );
            // The dependent is neither doomed nor dispatchable: still
            // BLOCKED, awaiting the retry's outcome.
            assert_eq!(primary.pool().blocked_len(), 1);
            assert!(
                !primary.failed_tasks.contains_key(&b_hash),
                "the dependent must NOT be cascade-failed while the retry \
                 budget can still revive its prereq"
            );
            assert!(
                matches!(
                    primary.cluster_state_for_test().task_state(&b_hash),
                    Some(crate::cluster_state::TaskState::Pending { .. })
                ),
                "the dependent's CRDT entry stays live"
            );
        })
        .await;
}

/// A `Failed { Recoverable }` root whose replicated retry budget is already
/// EXHAUSTED (`retry_max_passes = 0`) behaves like a final root: the bucket
/// declines, finalize promotes, and the dependent is cascade-failed with the
/// upstream-failed terminal — the budget decision replays identically on the
/// promoted primary (no re-granted retries).
#[tokio::test(flavor = "current_thread")]
async fn hydrate_failed_retryable_budget_exhausted_dooms_dependent() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(1);
            let config = PrimaryConfig {
                retry_max_passes: 0,
                ..PrimaryConfig::default()
            };
            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let a = dep_binary("toolchain", "build", &[]);
            let b = dep_binary("dep-a", "compile", &["toolchain"]);
            let a_hash = compute_task_hash(&a);
            let b_hash = compute_task_hash(&b);
            {
                let cs = primary.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::PhaseDepsSet {
                    deps: HashMap::from([(PhaseId::from("compile"), vec![PhaseId::from("build")])]),
                });
                cs.apply(ClusterMutation::TaskAdded {
                    hash: a_hash.clone(),
                    task: a.clone(),
                });
                cs.apply(ClusterMutation::TaskFailed {
                    attempt: 0,
                    hash: a_hash.clone(),
                    kind: dynrunner_core::ErrorType::Recoverable,
                    error: "transient".into(),
                    version: Default::default(),
                });
                cs.apply(ClusterMutation::TaskAdded {
                    hash: b_hash.clone(),
                    task: b.clone(),
                });
            }

            primary.hydrate_from_cluster_state();
            primary.fire_initial_phase_starts();
            primary.pool_mut().drain_empty_active_phases();
            primary.process_phase_lifecycle(&mut None).await;

            assert!(
                matches!(
                    primary.cluster_state_for_test().task_state(&b_hash),
                    Some(crate::cluster_state::TaskState::Failed {
                        kind: dynrunner_core::ErrorType::NonRecoverable,
                        ..
                    })
                ),
                "with the budget exhausted the dependent is doomed exactly \
                 like a final-failed root's"
            );
            assert_eq!(
                primary.completed_tasks.len() + primary.failed_tasks.len(),
                primary.total_tasks
            );
        })
        .await;
}

/// An `Unfulfillable` root keeps the operator-reinjectable DORMANCY
/// contract across hydrate: its dependent stays BLOCKED (neither
/// dispatchable nor doomed), the root's phase proceeds, the dependent's
/// phase holds the run open, and an operator reinject revives the root.
#[tokio::test(flavor = "current_thread")]
async fn hydrate_unfulfillable_root_keeps_dependent_blocked_dormant() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(1);
            let (mut primary, _mesh) = build_test_primary(
                PrimaryConfig::default(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let a = dep_binary("toolchain", "build", &[]);
            let b = dep_binary("dep-a", "compile", &["toolchain"]);
            let a_hash = compute_task_hash(&a);
            let b_hash = compute_task_hash(&b);
            {
                let cs = primary.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::PhaseDepsSet {
                    deps: HashMap::from([(PhaseId::from("compile"), vec![PhaseId::from("build")])]),
                });
                cs.apply(ClusterMutation::TaskAdded {
                    hash: a_hash.clone(),
                    task: a.clone(),
                });
                cs.apply(ClusterMutation::TaskFailed {
                    attempt: 0,
                    hash: a_hash.clone(),
                    kind: dynrunner_core::ErrorType::Unfulfillable {
                        reason: "toolchain outpath not staged".to_string().into(),
                    },
                    error: "unfulfillable".into(),
                    version: Default::default(),
                });
                cs.apply(ClusterMutation::TaskAdded {
                    hash: b_hash.clone(),
                    task: b.clone(),
                });
            }

            primary.hydrate_from_cluster_state();

            let queued: Vec<&str> = primary.pool().iter().map(|t| t.task_id.as_str()).collect();
            assert!(
                !queued.contains(&"dep-a"),
                "dependent of an Unfulfillable prereq must not dispatch; \
                 queued = {queued:?}"
            );
            assert_eq!(primary.pool().blocked_len(), 1);

            primary.fire_initial_phase_starts();
            primary.pool_mut().drain_empty_active_phases();
            primary.process_phase_lifecycle(&mut None).await;

            // DORMANCY: nothing is doomed, the dependent stays blocked,
            // the run is held open (not complete), the root entry stays
            // Unfulfillable (reinjectable).
            assert_eq!(
                primary.pool().blocked_len(),
                1,
                "the dependent stays BLOCKED through the cascade — dormancy"
            );
            assert!(
                !primary.failed_tasks.contains_key(&b_hash),
                "no upstream-failed cascade for an Unfulfillable root"
            );
            assert!(matches!(
                primary.cluster_state_for_test().task_state(&a_hash),
                Some(crate::cluster_state::TaskState::Unfulfillable { .. })
            ));
            assert!(
                !primary.pool().is_run_complete(),
                "dormancy holds the run open until the operator decides"
            );

            // REVIVAL: the operator reinject command (the SAME live seam,
            // through the command-channel chokepoint) moves the root back
            // into the pool; the dependent stays blocked on it until it
            // actually completes.
            let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
            crate::primary::command_channel::handle_primary_command(
                &mut primary,
                crate::primary::command_channel::PrimaryCommand::ReinjectTask {
                    hash: a_hash.clone(),
                    reply: reply_tx,
                },
                &mut None,
            )
            .await;
            reply_rx
                .await
                .expect("reply delivered")
                .expect("operator reinject of the dormant root accepts");
            let queued: Vec<&str> = primary.pool().iter().map(|t| t.task_id.as_str()).collect();
            assert!(
                queued.contains(&"toolchain"),
                "the reinjected root is dispatchable again; queued = {queued:?}"
            );
            assert_eq!(primary.pool().blocked_len(), 1);
        })
        .await;
}
