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
/// whose `task_depends_on` references it. `InvalidTask` is treated as
/// terminal by hydration: its `task_id` seeds `mark_tasks_completed`
/// (so the dependent's dep resolves and it enters the pool) and its
/// hash is recorded in the primary-side completed ledger (so it is not
/// re-queued). The non-reinjectable terminal entry stays in the CRDT.
#[test]
fn hydrate_treats_invalid_task_as_terminal_dep_seed() {
    let (transport, _ends) = setup_test(1);
    let (mut primary, _mesh) = build_test_primary(
        PrimaryConfig::default(),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );

    let toolchain = dep_binary("toolchain", "build", &[]);
    let dep_a = dep_binary("dep-a", "compile", &["toolchain"]);

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
            hash: "dep-a".into(),
            task: dep_a.clone(),
        });
    }

    primary.hydrate_from_cluster_state();

    // The dependent entered the pool — the InvalidTask prereq seeded
    // `completed_task_ids`, so `extend()` accepted it without an
    // `UnknownTaskDep` rejection.
    let pool = primary.pool();
    let ids: std::collections::HashSet<&str> = pool.iter().map(|t| t.task_id.as_str()).collect();
    assert!(
        ids.contains("dep-a"),
        "dependent of an InvalidTask prereq must enter the pool"
    );
    assert!(
        !ids.contains("toolchain"),
        "the terminal InvalidTask prereq is not re-queued"
    );
    // The terminal InvalidTask hash is recorded in the primary-side
    // completed ledger.
    assert!(primary.completed_tasks.contains("toolchain"));
    // The InvalidTask entry itself stays in the CRDT (non-reinjectable).
    assert!(matches!(
        primary.cluster_state_for_test().task_state("toolchain"),
        Some(crate::cluster_state::TaskState::InvalidTask { .. })
    ));
    assert_eq!(primary.total_tasks, 2);
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
                .handle_task_request(task_request_for("sec-0", 0))
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
                .handle_task_request(task_request_for("sec-0", 0))
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
                .handle_task_request(task_request_for("sec-0", 0))
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
                    Some(ClusterMutation::TaskRequeued { ref hash, .. }) if *hash == inh_hash
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
                primary.reconcile_inherited_slot(idx).is_none(),
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
