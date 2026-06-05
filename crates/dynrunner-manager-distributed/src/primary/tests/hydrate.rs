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
        if let DistributedMessage::TaskAssignment { binary_info, .. } = msg {
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
    let mut primary: PrimaryCoordinator<_, _, _, TestId> = PrimaryCoordinator::new(
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
    let mut primary: PrimaryCoordinator<_, _, _, TestId> = PrimaryCoordinator::new(
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
    let mut primary: PrimaryCoordinator<_, _, _, TestId> = PrimaryCoordinator::new(
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
            let mut primary: PrimaryCoordinator<_, _, _, TestId> = PrimaryCoordinator::new(
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

/// `activate_local_primary` is the single composition mechanism both
/// handoff sides converge on. On the FAILOVER-resume shape — a parked
/// co-located primary that never ran `run_pipeline`'s pool-build
/// (`total_tasks == 0`) activating into an ALREADY-replicated ledger
/// (`cluster_state.task_count() > 0`) — it must hydrate the pool +
/// in-flight ledger + completed set from the CRDT, BYPASSING the connect
/// / mesh-ready handshake. This pins that the production entry point
/// (not just `hydrate_from_cluster_state` in isolation) seeds the resume.
#[tokio::test(flavor = "current_thread")]
async fn activate_local_primary_hydrates_on_seeded_resume() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(1);
            let mut primary: PrimaryCoordinator<_, _, _, TestId> = PrimaryCoordinator::new(
                PrimaryConfig::default(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // Replicated ledger as a failover-resuming node would have
            // it: one completed prereq + two pending dependents, with the
            // pool still UNSEEDED (`total_tasks == 0`, `pending == None`)
            // — the parked-primary signature.
            let toolchain = dep_binary("toolchain", "build", &[]);
            let dep_a = dep_binary("dep-a", "compile", &["toolchain"]);
            let dep_b = dep_binary("dep-b", "compile", &["toolchain"]);
            {
                let cs = primary.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::PhaseDepsSet {
                    deps: HashMap::from([(PhaseId::from("compile"), vec![PhaseId::from("build")])]),
                });
                cs.apply(ClusterMutation::TaskAdded {
                    hash: "toolchain".into(),
                    task: toolchain,
                });
                cs.apply(ClusterMutation::TaskCompleted {
                    hash: "toolchain".into(),
                    result_data: None,
                });
                cs.apply(ClusterMutation::TaskAdded {
                    hash: "dep-a".into(),
                    task: dep_a,
                });
                cs.apply(ClusterMutation::TaskAdded {
                    hash: "dep-b".into(),
                    task: dep_b,
                });
            }

            // Precondition: parked — no pool, no counted tasks.
            assert!(
                primary.pending.is_none(),
                "parked primary starts with no pool"
            );
            assert_eq!(primary.total_tasks, 0, "parked primary counts no tasks yet");

            // THE production entry point. The seeded-resume discriminator
            // (`total_tasks == 0 && cluster_state.task_count() > 0`) is
            // true, so this hydrates.
            primary
                .activate_local_primary()
                .await
                .expect("activation must succeed");

            // Hydration ran: the pool holds the two dependents, the
            // completed prereq is recorded, and total_tasks tracks the
            // full ledger.
            let pool = primary.pool();
            assert_eq!(pool.len(), 2, "both dependents hydrated into the pool");
            let ids: std::collections::HashSet<&str> =
                pool.iter().map(|t| t.task_id.as_str()).collect();
            assert!(ids.contains("dep-a") && ids.contains("dep-b"));
            assert!(
                primary.completed_tasks.contains("toolchain"),
                "completed prereq recorded on the primary-side ledger"
            );
            assert_eq!(
                primary.total_tasks, 3,
                "total_tasks refreshed from the replicated ledger"
            );
        })
        .await;
}

/// Negative control for the seeded-resume discriminator: the BOOTSTRAP
/// shape — `run_pipeline` already set `total_tasks` from `binaries`
/// before calling `activate_local_primary` — must NOT re-hydrate (it
/// would clobber the freshly-built pool with a CRDT rebuild). Activation
/// is a no-op on the pool when `total_tasks > 0`.
#[tokio::test(flavor = "current_thread")]
async fn activate_local_primary_does_not_hydrate_on_bootstrap() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(1);
            let mut primary: PrimaryCoordinator<_, _, _, TestId> = PrimaryCoordinator::new(
                PrimaryConfig::default(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // Bootstrap shape: a pool already built and `total_tasks`
            // set, with the CRDT also holding a (mirrored) ledger. The
            // discriminator must read `total_tasks > 0` and skip
            // hydration so the bootstrap pool is left intact.
            let phase = PhaseId::from("work");
            let pool = dynrunner_scheduler_api::PendingPool::<TestId>::new(
                [phase.clone()],
                HashMap::new(),
            )
            .expect("work-phase pool");
            primary.pending = Some(pool);
            primary.total_tasks = 5;
            {
                let cs = primary.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::TaskAdded {
                    hash: "crdt-task".into(),
                    task: dep_binary("crdt-task", "work", &[]),
                });
            }

            primary
                .activate_local_primary()
                .await
                .expect("activation must succeed");

            // No re-hydration: total_tasks unchanged (a hydrate would
            // have refreshed it to cluster_state.task_count() == 1), and
            // the bootstrap pool is still the empty one we installed.
            assert_eq!(
                primary.total_tasks, 5,
                "bootstrap activation must NOT refresh total_tasks from the CRDT"
            );
            assert_eq!(
                primary.pool().iter().count(),
                0,
                "bootstrap pool left intact (no CRDT rebuild)"
            );
        })
        .await;
}

/// (5) ON-DEMAND activation through the full `run_activated` path.
///
/// A co-located primary built ON DEMAND (the moment a peer is named
/// primary) starts with NO local pool AND an empty cluster_state. It is
/// driven directly via `run_activated(snapshot)` — there is no pre-parked
/// object and no promotion gate. `run_activated` restores the snapshot,
/// enters the seeded resume (`activate_local_primary` hydrates from the
/// restored CRDT), then the shared operational-loop-and-finalize tail
/// runs. With every CRDT task already terminal, `run_complete_check` trips
/// on the loop's first iteration so the run finalizes cleanly — no
/// transport traffic needed. This pins the restore→activate→hydrate→
/// finalize wiring end-to-end at the manager layer (the two-coordinator
/// RUNTIME composition is e2e-only).
#[tokio::test(flavor = "current_thread")]
async fn run_activated_finalizes_from_crdt() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Drop the secondary ends so the legacy transport closes;
            // combined with all-terminal CRDT the operational loop's
            // top-of-iteration `run_complete_check` exits at once.
            let (transport, _ends) = setup_test(1);
            let mut primary: PrimaryCoordinator<_, _, _, TestId> = PrimaryCoordinator::new(
                PrimaryConfig::default(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // An on-demand-built primary starts with NO local pool AND an
            // empty cluster_state. The activator hands it a SNAPSHOT of the
            // secondary's continuously-mirrored ledger; build that snapshot
            // from a separate ClusterState holding two tasks, both already
            // Completed (the rest of the cluster finished them before this
            // node was named primary).
            assert_eq!(primary.total_tasks, 0);
            assert_eq!(
                primary.cluster_state_for_test().task_count(),
                0,
                "on-demand primary's own ledger is empty until the restore"
            );
            let snapshot = {
                let mut seed = crate::cluster_state::ClusterState::<TestId>::new();
                for name in ["t-0", "t-1"] {
                    seed.apply(ClusterMutation::TaskAdded {
                        hash: name.into(),
                        task: dep_binary(name, "work", &[]),
                    });
                    seed.apply(ClusterMutation::TaskCompleted {
                        hash: name.into(),
                        result_data: None,
                    });
                }
                seed.snapshot()
            };

            // Drive run_activated with the snapshot directly (restore →
            // activate → hydrate → finalize). No gate.
            primary
                .run_activated(snapshot)
                .await
                .expect("on-demand activation run must finalize cleanly");

            // The seeded resume restored the snapshot into the primary's
            // own ledger, then hydrated total_tasks from it (the
            // discriminator total_tasks==0 && task_count>0 fired) and
            // both tasks are accounted as completed.
            assert_eq!(
                primary.cluster_state_for_test().task_count(),
                2,
                "the activation snapshot must be restored into the primary's ledger"
            );
            assert_eq!(
                primary.total_tasks, 2,
                "activation must hydrate total_tasks from the restored CRDT"
            );
            assert_eq!(
                primary.completed_count(),
                2,
                "both pre-completed CRDT tasks credited on the seeded resume"
            );
            assert_eq!(
                primary.stranded_count(),
                0,
                "no stranded tasks — the run finalized on a fully-terminal ledger"
            );
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
            let mut primary: PrimaryCoordinator<_, _, _, TestId> = PrimaryCoordinator::new(
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

/// (D3-b) A promotion-shape resume (`activate_local_primary`, the
/// production seeded-resume entry) seeded with capacity + an InFlight
/// task asserts the promoted primary is dispatch-capable
/// (`alive_worker_count > 0`) AND does NOT re-dispatch the inherited
/// in-flight task: a dispatch tick over the reconstructed roster emits
/// NO `TaskAssignment` for it (the InFlight entry is not a pool item, and
/// its holding slot is `Assigned`, not idle).
#[tokio::test(flavor = "current_thread")]
async fn promotion_resume_reconstructs_roster_without_redispatch() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, mut ends) = setup_test(1);
            let mut primary: PrimaryCoordinator<_, _, _, TestId> = PrimaryCoordinator::new(
                PrimaryConfig::default(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // Parked-primary signature: pool unseeded (`total_tasks == 0`),
            // replicated ledger holds capacity (1 worker on sec-0) + that
            // worker's in-flight task.
            let task = dep_binary("inflight-1", "work", &[]);
            let hash = compute_task_hash(&task);
            {
                let cs = primary.cluster_state_mut_for_test();
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
                    hash: hash.clone(),
                    secondary: "sec-0".into(),
                    worker: 0,

                    version: Default::default(),
                });
            }
            assert_eq!(primary.total_tasks, 0, "parked primary counts no tasks yet");

            // THE production entry: the seeded-resume discriminator fires.
            primary
                .activate_local_primary()
                .await
                .expect("activation must succeed");

            // Roster reconstructed → dispatch-capable.
            assert!(
                primary.alive_worker_count_for_test() > 0,
                "promoted primary must hold a non-empty roster"
            );
            assert!(
                primary.slot_holds_hash_for_test("sec-0", 0, &hash),
                "the inherited in-flight task occupies its worker slot"
            );

            // Drain the activation keepalive(s) off the wire so the
            // dispatch assertion sees only (the absence of) a TaskAssignment.
            let _ = drain_assigned_task_ids(&mut ends[0].1);

            // A dispatch tick over the reconstructed roster must NOT
            // re-dispatch the inherited task: it is not a pool item, and
            // its only worker slot is Assigned (not idle).
            primary
                .dispatch_to_idle_workers(true)
                .await
                .expect("dispatch tick must succeed");
            assert!(
                drain_assigned_task_ids(&mut ends[0].1).is_empty(),
                "the inherited in-flight task must NOT be re-dispatched"
            );
            // The slot is still Assigned to the inherited task (untouched).
            assert!(
                primary.slot_holds_hash_for_test("sec-0", 0, &hash),
                "the inherited slot stays Assigned after the dispatch tick"
            );
        })
        .await;
}

/// Drain a secondary inbox and return every `hash` carried by a
/// `ClusterMutation::TaskRequeued` across all envelopes. The dead-
/// secondary recovery originates these (InFlight → Pending) through the
/// canonical broadcast pipeline, so a promoted primary's roster must hold
/// the dead secondary for them to be emitted at all.
fn drain_requeued(
    rx: &mut tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
) -> Vec<String> {
    let mut out = Vec::new();
    while let Ok(msg) = rx.try_recv() {
        if let DistributedMessage::ClusterMutation { mutations, .. } = msg {
            for m in mutations {
                if let ClusterMutation::TaskRequeued { hash, .. } = m {
                    out.push(hash);
                }
            }
        }
    }
    out
}

/// (D R-1, post-promotion) A promoted primary must reconstruct its
/// SECONDARY roster (`self.secondaries` + `self.secondary_keepalives`)
/// from the replicated capacity ledger — not just `self.workers` — so a
/// secondary dying AFTER promotion is detected and its inherited in-flight
/// task is requeued (a `TaskRequeued` broadcast), NOT stranded forever.
///
/// Pre-fix the on-demand promotion path (`activate_local_primary` → hydrate)
/// rebuilt `self.workers` but left `self.secondaries` empty, so
/// `collect_heartbeat_report` (which keys off `self.secondaries`) could
/// mark NO secondary dead → `process_heartbeat_tick` ran the recovery for
/// nobody → the inherited InFlight task stayed in-flight forever (no
/// `TaskRequeued`). This pins the roster reconstruction end-to-end through
/// the heartbeat path.
///
/// Determinism note: the keepalive deadline is measured via
/// `std::time::Instant` (`tokio::time::advance` does not move it — see the
/// in-tree note at `heartbeat/tests.rs`), so this models "advance past the
/// deadline" with a short real-time interval (50ms × 2 = 100ms deadline,
/// a 200ms sleep crosses it), the same approach every heartbeat-deadline
/// test in this crate uses.
#[tokio::test(flavor = "current_thread")]
async fn promoted_primary_detects_dead_secondary_and_requeues_inherited_inflight() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, mut ends) = setup_test(1);
            // Short keepalive deadline so the test crosses it with a brief
            // real sleep; everything else is the production default. The
            // staged silence schedule is shrunk to keepalive-interval-
            // relative tiny multiples (HARD backstop at 2x = 100ms) so the
            // 200ms sleep below trips the hard declaration.
            let config = PrimaryConfig {
                keepalive_interval: Duration::from_millis(50),
                keepalive_miss_threshold: 2,
                silence_warn_multiples: vec![1],
                silence_hard_multiple: 2,
                ..PrimaryConfig::default()
            };
            let mut primary: PrimaryCoordinator<_, _, _, TestId> = PrimaryCoordinator::new(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // Parked-primary signature: pool unseeded (`total_tasks == 0`),
            // replicated ledger holds sec-0's capacity (1 worker) + that
            // worker's in-flight task — the state a promoted primary
            // inherits via the CRDT snapshot.
            let task = dep_binary("inflight-1", "work", &[]);
            let hash = compute_task_hash(&task);
            {
                let cs = primary.cluster_state_mut_for_test();
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
                    hash: hash.clone(),
                    secondary: "sec-0".into(),
                    worker: 0,

                    version: Default::default(),
                });
            }
            assert_eq!(primary.total_tasks, 0, "parked primary counts no tasks yet");

            // THE production entry: hydrate (reconstructs workers AND the
            // secondary roster from the CRDT) + assert authority.
            primary
                .activate_local_primary()
                .await
                .expect("activation must succeed");

            // The inherited task is in-flight on the reconstructed slot.
            assert_eq!(primary.in_flight_len_for_test(), 1);
            assert!(primary.slot_holds_hash_for_test("sec-0", 0, &hash));

            // Drain setup noise (the bootstrap PrimaryChanged broadcast).
            let _ = ends[0].1.try_recv();
            while ends[0].1.try_recv().is_ok() {}

            // sec-0 goes silent past the keepalive deadline.
            tokio::time::sleep(Duration::from_millis(200)).await;

            // The promoted primary's heartbeat tick must detect sec-0 dead
            // (its roster + keepalive entry were reconstructed) and requeue
            // the inherited in-flight task.
            primary
                .process_heartbeat_tick()
                .await
                .expect("heartbeat tick must succeed");

            // A TaskRequeued for the inherited hash was broadcast — the
            // task is recovered, not stranded.
            let requeued = drain_requeued(&mut ends[0].1);
            assert_eq!(
                requeued,
                vec![hash.clone()],
                "the promoted primary must requeue the dead secondary's \
                 inherited in-flight task (pre-fix: empty roster → stranded)"
            );
            // CRDT reflects the recovery: the task is Pending again, and
            // the ledger entry drained.
            assert!(matches!(
                primary.cluster_state_for_test().task_state(&hash),
                Some(crate::cluster_state::TaskState::Pending { .. })
            ));
            assert_eq!(
                primary.in_flight_len_for_test(),
                0,
                "the inherited in-flight ledger entry must drain on recovery"
            );
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
                let mut live: PrimaryCoordinator<_, _, _, TestId> = PrimaryCoordinator::new(
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
            let mut promoted: PrimaryCoordinator<_, _, _, TestId> = PrimaryCoordinator::new(
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
            assert!(
                drain_assigned_task_ids(&mut ends[0].1).is_empty(),
                "no second dispatch of the same task — exactly once"
            );
        })
        .await;
}
