use super::*;
use std::collections::HashMap;
use std::time::Duration;

use dynrunner_core::{ErrorType, PhaseId, TaskDep};
use dynrunner_protocol_primary_secondary::ClusterMutation;
use dynrunner_scheduler::ResourceStealingScheduler;
use tokio::sync::oneshot;

use crate::cluster_state::TaskState;
use crate::primary::PrimaryConfig;
use crate::primary::PrimaryCoordinator;
use crate::primary::test_helpers::{
    FixedEstimator, PrimaryMeshKeepalive, TestId, build_test_primary, make_binary, setup_test,
};
use crate::primary::wire::compute_task_hash;

/// Build a `PrimaryCoordinator` against the in-process channel
/// transport stub used by the rest of the primary tests. We
/// don't drive a full run; the tests below call the command-
/// channel handlers directly to assert per-command semantics
/// without coupling to the operational loop's exit conditions.
fn make_coordinator() -> (
    PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
    PrimaryMeshKeepalive,
) {
    let (transport, _secondary_ends) = setup_test(0);
    let config = PrimaryConfig {
        num_secondaries: 0,
        connect_timeout: Duration::from_secs(1),
        peer_timeout: Duration::from_secs(1),
        keepalive_interval: Duration::from_millis(100),
        uses_file_based_items: false,
        retry_max_passes: 0,
        fleet_dead_timeout: Duration::from_secs(1),
        mesh_ready_timeout: Duration::from_secs(1),
        ..PrimaryConfig::default()
    };
    build_test_primary(
        config,
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    )
}

/// End-to-end: send `FailPermanent`, observe the local
/// `failed_tasks` ledger updates and the reply oneshot fires.
#[tokio::test(flavor = "current_thread")]
async fn fail_permanent_via_channel() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut coordinator, _mesh) = make_coordinator();
            // Seed a single Pending task into cluster_state so the
            // hash-to-meta lookup succeeds. Also pre-initialise the
            // pool so `pool.on_item_failed_permanent` has a phase
            // to discount in_flight against.
            let binary = make_binary("a", 100);
            let hash = compute_task_hash(&binary);
            coordinator.cluster_state.apply(ClusterMutation::TaskAdded {
                hash: hash.clone(),
                task: binary.clone(),
            });
            let mut phase_set = std::collections::HashSet::new();
            phase_set.insert(binary.phase_id.clone());
            coordinator.pending = Some(
                dynrunner_scheduler_api::PendingPool::new(phase_set, HashMap::new())
                    .expect("pool init"),
            );

            let (reply_tx, reply_rx) = oneshot::channel();
            super::handle_primary_command(
                &mut coordinator,
                PrimaryCommand::FailPermanent {
                    hash: hash.clone(),
                    error: ErrorType::NonRecoverable,
                    reason: "test".into(),
                    reply: reply_tx,
                },
                &mut None,
            )
            .await;
            let reply = reply_rx.await.expect("reply oneshot closed");
            assert!(reply.is_ok(), "fail_permanent should accept: {reply:?}");
            assert!(
                coordinator.failed_tasks.contains_key(&hash),
                "failed_tasks should include the hash"
            );
            // CRDT mirror reflects the Failed terminal state.
            match coordinator.cluster_state.task_state(&hash) {
                Some(TaskState::Failed { kind, .. }) => {
                    assert_eq!(*kind, ErrorType::NonRecoverable);
                }
                other => panic!("expected Failed, got {other:?}"),
            }
        })
        .await;
}

/// Operator-set `ResourceExhausted(memory)` via `FailPermanent`
/// funnels into the per-phase OOM retry bucket — the same channel
/// a worker-originated OOM failure flows through. Pins the
/// cross-cut: the bucket-partition predicate reads from
/// `failed_tasks` regardless of who wrote the entry, so the
/// operator-driven path and the worker-driven path converge on
/// identical retry semantics.
///
/// To pin the partition WITHOUT having the in-cascade OOM bucket
/// auto-drain the entry (which it WOULD do with a non-zero budget,
/// because `apply_fail_permanent` calls `note_item_failed` →
/// `process_phase_lifecycle` → `try_run_phase_retry_bucket`), this
/// fixture uses `oom_retry_max_passes = 0`. With the cap at zero
/// the cascade observes the entry, finds the bucket exhausted, and
/// falls through to `on_phase_end`; the entry stays in
/// `failed_tasks` and we directly verify the partition.
#[tokio::test(flavor = "current_thread")]
async fn fail_permanent_oom_routes_into_per_phase_oom_bucket() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut coordinator, _mesh) = make_coordinator();
            // Disable the OOM bucket so the cascade can't auto-drain the
            // entry — we want to inspect `failed_tasks` post-apply.
            coordinator.config.oom_retry_max_passes = 0;
            let binary = make_binary("doomed-by-operator", 100);
            let hash = compute_task_hash(&binary);
            coordinator.cluster_state.apply(ClusterMutation::TaskAdded {
                hash: hash.clone(),
                task: binary.clone(),
            });
            let mut phase_set = std::collections::HashSet::new();
            phase_set.insert(binary.phase_id.clone());
            coordinator.pending = Some(
                dynrunner_scheduler_api::PendingPool::new(phase_set, HashMap::new())
                    .expect("pool init"),
            );
            // `all_binaries` is the binary-lookup table the retry-bucket
            // primitive reads to map `failed_tasks` hashes back to
            // dispatchable `TaskInfo`s. In production this is populated
            // by `run()`'s seed step; here we set it explicitly.
            coordinator.all_binaries = vec![binary.clone()];

            let (reply_tx, reply_rx) = oneshot::channel();
            super::handle_primary_command(
                &mut coordinator,
                PrimaryCommand::FailPermanent {
                    hash: hash.clone(),
                    error: ErrorType::ResourceExhausted(dynrunner_core::ResourceKind::memory()),
                    reason: "over budget".into(),
                    reply: reply_tx,
                },
                &mut None,
            )
            .await;
            assert!(
                reply_rx.await.unwrap().is_ok(),
                "fail_permanent should accept ResourceExhausted(memory)"
            );

            // Bucket exhausted (cap=0), entry stays in `failed_tasks`
            // with the OOM kind — the partition predicate the OOM
            // bucket would have used.
            match coordinator.failed_tasks.get(&hash) {
                Some(ErrorType::ResourceExhausted(kind)) => {
                    assert_eq!(kind.as_str(), "memory");
                }
                other => panic!(
                    "operator-set OOM should land in failed_tasks as \
                 ResourceExhausted(memory); got {other:?}"
                ),
            }

            // Now flip cap to 1 and drive the OOM bucket directly. It
            // must pick the entry up because the partition predicate
            // matches `ResourceExhausted(memory)`, regardless of
            // whether the failure came from a worker or an operator
            // command.
            coordinator.config.oom_retry_max_passes = 1;
            // No counter reset needed: the OOM bucket's (phase, Oom) key was
            // never bumped (the prior cap=0 pass hit budget-exhausted before
            // any reinject), and the replicated grow-only-MAX counter has no
            // clear() — it reads 0 for the never-bumped key.
            let phase = binary.phase_id.clone();
            let mut no_cmd_rx: Option<tokio::sync::mpsc::Receiver<PrimaryCommand<TestId>>> = None;
            let reinjected = coordinator
                .try_run_phase_retry_bucket(
                    &phase,
                    crate::primary::retry_bucket::BucketKind::Oom,
                    &mut no_cmd_rx,
                )
                .await
                .expect("OOM bucket runs cleanly");
            assert!(
                reinjected,
                "operator-set OOM must funnel into the per-phase OOM bucket"
            );
            assert!(
                !coordinator.failed_tasks.contains_key(&hash),
                "OOM bucket should drain the entry from failed_tasks"
            );
        })
        .await;
}

/// `FailPermanent` on an unknown hash returns Err and leaves
/// coordinator state untouched.
#[tokio::test(flavor = "current_thread")]
async fn fail_permanent_unknown_hash() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut coordinator, _mesh) = make_coordinator();
            let (reply_tx, reply_rx) = oneshot::channel();
            super::handle_primary_command(
                &mut coordinator,
                PrimaryCommand::FailPermanent {
                    hash: "nonexistent".into(),
                    error: ErrorType::NonRecoverable,
                    reason: "test".into(),
                    reply: reply_tx,
                },
                &mut None,
            )
            .await;
            let reply = reply_rx.await.expect("reply oneshot closed");
            assert!(reply.is_err(), "unknown hash should error");
            assert!(coordinator.failed_tasks.is_empty());
        })
        .await;
}

/// `ReinjectTask` accepts on `TaskState::Unfulfillable { .. }` and
/// budget exhaustion locks further reinjects out without
/// regressing the ledger.
#[tokio::test(flavor = "current_thread")]
async fn reinject_task_budget_exhaustion() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut coordinator, _mesh) = make_coordinator();
            coordinator.set_unfulfillable_reinject_max_per_task(Some(1));
            let binary = make_binary("a", 100);
            let hash = compute_task_hash(&binary);
            coordinator.cluster_state.apply(ClusterMutation::TaskAdded {
                hash: hash.clone(),
                task: binary.clone(),
            });
            coordinator
                .cluster_state
                .apply(ClusterMutation::TaskFailed {
                    attempt: 0,
                    hash: hash.clone(),
                    kind: ErrorType::Unfulfillable {
                        reason: "missing toolchain".to_string().into(),
                    },
                    error: "unfulfillable".into(),
                    version: Default::default(),
                });
            // Pool init: reinject requires the phase to exist.
            let mut phase_set = std::collections::HashSet::new();
            phase_set.insert(binary.phase_id.clone());
            coordinator.pending = Some(
                dynrunner_scheduler_api::PendingPool::new(phase_set, HashMap::new())
                    .expect("pool init"),
            );

            // First reinject — accepts.
            let (reply_tx, reply_rx) = oneshot::channel();
            super::handle_primary_command(
                &mut coordinator,
                PrimaryCommand::ReinjectTask {
                    hash: hash.clone(),
                    reply: reply_tx,
                },
                &mut None,
            )
            .await;
            assert!(reply_rx.await.unwrap().is_ok(), "first reinject accepts");
            // CRDT mirror moved off Unfulfillable.
            assert!(matches!(
                coordinator.cluster_state.task_state(&hash),
                Some(TaskState::Pending { .. })
            ));

            // Re-set to Unfulfillable and try again — budget should be exhausted.
            coordinator
                .cluster_state
                .apply(ClusterMutation::TaskFailed {
                    attempt: 0,
                    hash: hash.clone(),
                    kind: ErrorType::Unfulfillable {
                        reason: "still missing".to_string().into(),
                    },
                    error: "unfulfillable again".into(),
                    version: Default::default(),
                });
            let (reply_tx2, reply_rx2) = oneshot::channel();
            super::handle_primary_command(
                &mut coordinator,
                PrimaryCommand::ReinjectTask {
                    hash: hash.clone(),
                    reply: reply_tx2,
                },
                &mut None,
            )
            .await;
            let r2 = reply_rx2.await.unwrap();
            assert!(r2.is_err(), "second reinject should hit budget cap");
            // Ledger stays Unfulfillable.
            assert!(matches!(
                coordinator.cluster_state.task_state(&hash),
                Some(TaskState::Unfulfillable { .. })
            ));
        })
        .await;
}

/// `UpdatePreferredSecondaries` happy-path smoke: CRDT applies
/// and the live pool mirror moves in lockstep. The deeper
/// pool-mirror assertion lives in
/// `update_preferred_secondaries_propagates_to_live_pool`; this
/// test just pins that the handler accepts under a fully-seeded
/// operational fixture (pool initialised, task present in both
/// CRDT and pool) the way the production operational loop calls
/// in.
#[tokio::test(flavor = "current_thread")]
async fn update_preferred_secondaries_smoke() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut coordinator, _mesh) = make_coordinator();
            let binary = make_binary("a", 100);
            let hash = compute_task_hash(&binary);
            coordinator.cluster_state.apply(ClusterMutation::TaskAdded {
                hash: hash.clone(),
                task: binary.clone(),
            });
            let mut phase_set = std::collections::HashSet::new();
            phase_set.insert(binary.phase_id.clone());
            coordinator.pending = Some(
                dynrunner_scheduler_api::PendingPool::new(phase_set, HashMap::new())
                    .expect("pool init"),
            );
            let (reply_tx, reply_rx) = oneshot::channel();
            super::handle_primary_command(
                &mut coordinator,
                PrimaryCommand::UpdatePreferredSecondaries {
                    hash: hash.clone(),
                    secondaries: vec!["sec-1".into(), "sec-2".into()],
                    reply: reply_tx,
                },
                &mut None,
            )
            .await;
            assert!(reply_rx.await.unwrap().is_ok());
        })
        .await;
}

/// End-to-end through the cross-thread channel: send commands via
/// the public `command_sender()` and consume them through the
/// `PrimaryCommand` arm. Exercises the same code path the PyO3
/// `PrimaryHandle` uses.
#[tokio::test(flavor = "current_thread")]
async fn command_channel_end_to_end() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut coordinator, _mesh) = make_coordinator();
            let binary = make_binary("a", 100);
            let hash = compute_task_hash(&binary);
            coordinator.cluster_state.apply(ClusterMutation::TaskAdded {
                hash: hash.clone(),
                task: binary.clone(),
            });
            let mut phase_set = std::collections::HashSet::new();
            phase_set.insert(binary.phase_id.clone());
            coordinator.pending = Some(
                dynrunner_scheduler_api::PendingPool::new(phase_set, HashMap::new())
                    .expect("pool init"),
            );
            let sender = coordinator.command_sender();
            let (reply_tx, reply_rx) = oneshot::channel();
            // Send through the channel (the same `tokio::sync::mpsc`
            // the PyO3 handle uses), then drain the receiver once.
            sender
                .send(PrimaryCommand::FailPermanent {
                    hash: hash.clone(),
                    error: ErrorType::NonRecoverable,
                    reason: "via channel".into(),
                    reply: reply_tx,
                })
                .await
                .expect("send into command channel");
            // Mimic the operational loop: take the receiver, recv one
            // command, dispatch it.
            let mut rx = coordinator.command_rx.take().expect("rx present");
            let command = rx.recv().await.expect("first command");
            super::handle_primary_command(&mut coordinator, command, &mut None).await;
            coordinator.command_rx = Some(rx);
            let reply = reply_rx.await.expect("reply oneshot closed");
            assert!(reply.is_ok(), "{reply:?}");
            assert!(coordinator.failed_tasks.contains_key(&hash));
        })
        .await;
}

/// `FailPermanent` with `ErrorType::Unfulfillable` routes the
/// cascade to a `TaskBlocked` broadcast for each dependent — the
/// dependent's CRDT entry lands in `TaskState::Blocked { on, .. }`
/// rather than `TaskState::Failed`, so the auto-resume path can
/// recover when the prereq is reinjected. Pins the cascade
/// dispatch in `apply_fail_permanent`.
#[tokio::test(flavor = "current_thread")]
async fn fail_permanent_unfulfillable_blocks_dependents() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut coordinator, _mesh) = make_coordinator();

            // Prereq carries an explicit task_id so the pool can wire
            // the dep-cascade reverse-index.
            let mut prereq = make_binary("prereq", 100);
            prereq.task_id = "prereq_id".into();
            let prereq_hash = compute_task_hash(&prereq);

            // Dependent declares task_depends_on for the cascade walk.
            let mut dep = make_binary("dep", 100);
            dep.task_id = "dep_id".into();
            dep.task_depends_on = vec![TaskDep {
                task_id: "prereq_id".into(),
                phase_id: PhaseId::from("default"),
                inherit_outputs: false,
            }];
            let dep_hash = compute_task_hash(&dep);

            // Seed CRDT for both.
            coordinator.cluster_state.apply(ClusterMutation::TaskAdded {
                hash: prereq_hash.clone(),
                task: prereq.clone(),
            });
            coordinator.cluster_state.apply(ClusterMutation::TaskAdded {
                hash: dep_hash.clone(),
                task: dep.clone(),
            });

            // Pool seeded with both phases + the items so the cascade
            // primitive has dependents to walk.
            let mut phase_set = std::collections::HashSet::new();
            phase_set.insert(prereq.phase_id.clone());
            let mut pool = dynrunner_scheduler_api::PendingPool::new(phase_set, HashMap::new())
                .expect("pool init");
            pool.extend(vec![prereq.clone(), dep.clone()])
                .expect("pool extend");
            coordinator.pending = Some(pool);
            // Mark the prereq in flight so on_item_failed_permanent's
            // in_flight bookkeeping doesn't saturate.
            coordinator.pool_mut().mark_in_flight(&prereq.phase_id);

            let (reply_tx, reply_rx) = oneshot::channel();
            super::handle_primary_command(
                &mut coordinator,
                PrimaryCommand::FailPermanent {
                    hash: prereq_hash.clone(),
                    error: ErrorType::Unfulfillable {
                        reason: "missing toolchain".to_string().into(),
                    },
                    reason: "no peer holds the resource".into(),
                    reply: reply_tx,
                },
                &mut None,
            )
            .await;
            let reply = reply_rx.await.expect("reply oneshot closed");
            assert!(reply.is_ok(), "fail_permanent should accept: {reply:?}");

            // Prereq lands in the discrete Unfulfillable variant.
            assert!(matches!(
                coordinator.cluster_state.task_state(&prereq_hash),
                Some(TaskState::Unfulfillable { .. })
            ));
            // Dependent lands in Blocked-on-prereq via the cascade
            // broadcast (NOT in Failed).
            match coordinator.cluster_state.task_state(&dep_hash) {
                Some(TaskState::Blocked { on, .. }) => {
                    assert_eq!(on, &prereq_hash);
                }
                other => panic!("expected Blocked, got {other:?}"),
            }
            // Dependent is NOT in the local failed_tasks ledger — it's
            // cascade-paused, not failed.
            assert!(
                !coordinator.failed_tasks.contains_key(&dep_hash),
                "blocked dependent must not be in failed_tasks"
            );
        })
        .await;
}

/// Full Unfulfillable-cascade chain: root Unfulfillable →
/// dependents Blocked → reinject root → root completes →
/// auto-resume on the CRDT side mirrors into the live pool via
/// `apply_and_broadcast_cluster_mutations`'s
/// `resumed_for_dispatch` plumb. After completion the dependent
/// must be back in the live pool and dispatchable through the
/// normal pool primitives.
///
/// Pins Phase-6 Finding 6: pool-side cascade-resume of Blocked
/// dependents when the prereq's `TaskCompleted` lands. The CRDT
/// side was already correct; this test enforces the pool stays
/// coherent.
#[tokio::test(flavor = "current_thread")]
async fn unfulfillable_reinject_root_complete_resumes_blocked_dependents_in_pool() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut coordinator, _mesh) = make_coordinator();

            let mut prereq = make_binary("prereq", 100);
            prereq.task_id = "prereq_id".into();
            let prereq_hash = compute_task_hash(&prereq);

            let mut dep = make_binary("dep", 100);
            dep.task_id = "dep_id".into();
            dep.task_depends_on = vec![TaskDep {
                task_id: "prereq_id".into(),
                phase_id: PhaseId::from("default"),
                inherit_outputs: false,
            }];
            let dep_hash = compute_task_hash(&dep);

            // Seed CRDT and pool with both items.
            coordinator.cluster_state.apply(ClusterMutation::TaskAdded {
                hash: prereq_hash.clone(),
                task: prereq.clone(),
            });
            coordinator.cluster_state.apply(ClusterMutation::TaskAdded {
                hash: dep_hash.clone(),
                task: dep.clone(),
            });
            let mut phase_set = std::collections::HashSet::new();
            phase_set.insert(prereq.phase_id.clone());
            let mut pool = dynrunner_scheduler_api::PendingPool::new(phase_set, HashMap::new())
                .expect("pool init");
            pool.extend(vec![prereq.clone(), dep.clone()])
                .expect("pool extend");
            coordinator.pending = Some(pool);
            coordinator.pool_mut().mark_in_flight(&prereq.phase_id);

            // Step 1: prereq fails Unfulfillable → dep moves to
            // Blocked in CRDT, pool drops the dep entry.
            let (rx1, rxr1) = oneshot::channel();
            super::handle_primary_command(
                &mut coordinator,
                PrimaryCommand::FailPermanent {
                    hash: prereq_hash.clone(),
                    error: ErrorType::Unfulfillable {
                        reason: "missing toolchain".to_string().into(),
                    },
                    reason: "no peer".into(),
                    reply: rx1,
                },
                &mut None,
            )
            .await;
            assert!(rxr1.await.unwrap().is_ok());
            // Sanity: pool no longer contains the dep binary.
            assert!(
                !coordinator.pool().iter().any(|t| t.task_id == "dep_id"),
                "dep should be dropped from pool after Unfulfillable cascade"
            );

            // Step 2: reinject the root.
            let (rx2, rxr2) = oneshot::channel();
            super::handle_primary_command(
                &mut coordinator,
                PrimaryCommand::ReinjectTask {
                    hash: prereq_hash.clone(),
                    reply: rx2,
                },
                &mut None,
            )
            .await;
            assert!(rxr2.await.unwrap().is_ok());
            // Dep still Blocked in CRDT (only root flipped to
            // Pending; auto-resume fires on TaskCompleted, not on
            // TaskReinjected).
            assert!(matches!(
                coordinator.cluster_state.task_state(&dep_hash),
                Some(TaskState::Blocked { .. })
            ));
            // Dep still absent from the pool.
            assert!(
                !coordinator.pool().iter().any(|t| t.task_id == "dep_id"),
                "dep must still be absent from pool until root completes"
            );

            // Step 3: simulate the root completing. The mark_in_flight
            // above already incremented in_flight by 1; we route
            // `TaskCompleted` through `apply_and_broadcast_cluster_mutations`
            // so the resumed-dispatch plumbing fires.
            coordinator
                .apply_and_broadcast_cluster_mutations(vec![ClusterMutation::TaskCompleted {
                    attempt: 0,
                    hash: prereq_hash.clone(),
                    result_data: None,
                }])
                .await;

            // CRDT side: dep auto-resumed to Pending.
            assert!(matches!(
                coordinator.cluster_state.task_state(&dep_hash),
                Some(TaskState::Pending { .. })
            ));
            // Pool side: dep is back in a bucket and dispatchable.
            assert!(
                coordinator.pool().iter().any(|t| t.task_id == "dep_id"),
                "dep must be back in the pool after auto-resume"
            );
            // The dep's phase must be dispatchable (not Blocked).
            // `reinject` flips Draining/Drained/Done back to Active;
            // for an originally-Active phase it stays Active.
            let phase_state = coordinator.pool().phase_state(&dep.phase_id);
            assert!(
                matches!(
                    phase_state,
                    Some(dynrunner_scheduler_api::PhaseState::Active)
                ),
                "dep's phase must be Active for dispatch; got {phase_state:?}"
            );
        })
        .await;
}

/// Re-inject path is independent of dependent unblock: after the
/// root flips Unfulfillable→Pending via `ReinjectTask`, the
/// dependents must STAY Blocked in CRDT (auto-resume only fires
/// on `TaskCompleted`) and remain absent from the pool. Guards
/// against an accidental "reinject also unblocks dependents"
/// regression that would let dependents dispatch ahead of a
/// still-pending root.
#[tokio::test(flavor = "current_thread")]
async fn reinject_resets_blocked_dependents_pool_state() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut coordinator, _mesh) = make_coordinator();

            let mut prereq = make_binary("prereq", 100);
            prereq.task_id = "prereq_id".into();
            let prereq_hash = compute_task_hash(&prereq);

            let mut dep = make_binary("dep", 100);
            dep.task_id = "dep_id".into();
            dep.task_depends_on = vec![TaskDep {
                task_id: "prereq_id".into(),
                phase_id: PhaseId::from("default"),
                inherit_outputs: false,
            }];
            let dep_hash = compute_task_hash(&dep);

            coordinator.cluster_state.apply(ClusterMutation::TaskAdded {
                hash: prereq_hash.clone(),
                task: prereq.clone(),
            });
            coordinator.cluster_state.apply(ClusterMutation::TaskAdded {
                hash: dep_hash.clone(),
                task: dep.clone(),
            });
            let mut phase_set = std::collections::HashSet::new();
            phase_set.insert(prereq.phase_id.clone());
            let mut pool = dynrunner_scheduler_api::PendingPool::new(phase_set, HashMap::new())
                .expect("pool init");
            pool.extend(vec![prereq.clone(), dep.clone()])
                .expect("pool extend");
            coordinator.pending = Some(pool);
            coordinator.pool_mut().mark_in_flight(&prereq.phase_id);

            // Cascade: prereq Unfulfillable → dep Blocked.
            let (tx1, rx1) = oneshot::channel();
            super::handle_primary_command(
                &mut coordinator,
                PrimaryCommand::FailPermanent {
                    hash: prereq_hash.clone(),
                    error: ErrorType::Unfulfillable {
                        reason: "missing toolchain".to_string().into(),
                    },
                    reason: "no peer".into(),
                    reply: tx1,
                },
                &mut None,
            )
            .await;
            assert!(rx1.await.unwrap().is_ok());

            // Reinject root only.
            let (tx2, rx2) = oneshot::channel();
            super::handle_primary_command(
                &mut coordinator,
                PrimaryCommand::ReinjectTask {
                    hash: prereq_hash.clone(),
                    reply: tx2,
                },
                &mut None,
            )
            .await;
            assert!(rx2.await.unwrap().is_ok());

            // CRDT: root flipped to Pending; dep stays Blocked.
            assert!(matches!(
                coordinator.cluster_state.task_state(&prereq_hash),
                Some(TaskState::Pending { .. })
            ));
            match coordinator.cluster_state.task_state(&dep_hash) {
                Some(TaskState::Blocked { on, .. }) => {
                    assert_eq!(on, &prereq_hash);
                }
                other => panic!("dep should still be Blocked, got {other:?}"),
            }
            // Pool: dep must NOT be present (the cascade dropped it
            // and reinject of root does not re-introduce it).
            assert!(
                !coordinator.pool().iter().any(|t| t.task_id == "dep_id"),
                "dep must stay out of pool until root completes"
            );
            // Root IS back in the pool.
            assert!(
                coordinator.pool().iter().any(|t| t.task_id == "prereq_id"),
                "root must be back in pool after reinject"
            );
        })
        .await;
}

/// `UpdatePreferredSecondaries` runtime mutation must propagate to
/// the live pool's `TaskInfo` clone, not only to the CRDT mirror.
/// The scheduler's hot path reads `preferred_secondaries` from the
/// pool entry it dispatches; a CRDT-only update would only become
/// visible on snapshot-restore.
///
/// Pins Phase-6 Finding 7: pool-side mirror of preferred-secondaries.
#[tokio::test(flavor = "current_thread")]
async fn update_preferred_secondaries_propagates_to_live_pool() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut coordinator, _mesh) = make_coordinator();
            let mut binary = make_binary("a", 100);
            binary.task_id = "a_id".into();
            let hash = compute_task_hash(&binary);
            coordinator.cluster_state.apply(ClusterMutation::TaskAdded {
                hash: hash.clone(),
                task: binary.clone(),
            });
            let mut phase_set = std::collections::HashSet::new();
            phase_set.insert(binary.phase_id.clone());
            let mut pool = dynrunner_scheduler_api::PendingPool::new(phase_set, HashMap::new())
                .expect("pool init");
            pool.extend(vec![binary.clone()]).expect("pool extend");
            coordinator.pending = Some(pool);

            // Pre-condition: pool's clone has empty preferred_secondaries.
            let pre = coordinator
                .pool()
                .iter()
                .find(|t| t.task_id == "a_id")
                .expect("task in pool")
                .preferred_secondaries
                .clone();
            assert!(pre.as_slice().is_empty(), "fixture starts empty");

            let (tx, rx) = oneshot::channel();
            super::handle_primary_command(
                &mut coordinator,
                PrimaryCommand::UpdatePreferredSecondaries {
                    hash: hash.clone(),
                    secondaries: vec!["sec-a".into(), "sec-b".into()],
                    reply: tx,
                },
                &mut None,
            )
            .await;
            assert!(rx.await.unwrap().is_ok());

            // CRDT mirror updated.
            let crdt_task = match coordinator.cluster_state.task_state(&hash) {
                Some(TaskState::Pending { task, .. }) => task.clone(),
                other => panic!("expected Pending, got {other:?}"),
            };
            let expected: Vec<&str> = vec!["sec-a", "sec-b"];
            assert_eq!(
                crdt_task
                    .preferred_secondaries
                    .as_slice()
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>(),
                expected,
                "CRDT mirror"
            );

            // Live pool's clone reflects the update on the next read.
            let post = coordinator
                .pool()
                .iter()
                .find(|t| t.task_id == "a_id")
                .expect("task still in pool")
                .preferred_secondaries
                .clone();
            assert_eq!(
                post.as_slice()
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>(),
                expected,
                "live pool's TaskInfo.preferred_secondaries must mirror the CRDT update"
            );
        })
        .await;
}

// ── PrimaryCommand::SpawnTasks ──

/// Seed `coordinator.pending` with a `PendingPool` whose active
/// phases include `phase_id` (plus any extras supplied) and
/// initialise the per-phase completed/failed counters. Centralised
/// so every spawn-tasks test starts from the same shape the
/// production operational loop uses.
fn seed_pool(
    coordinator: &mut PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
    phases: &[&dynrunner_core::PhaseId],
) {
    let mut phase_set = std::collections::HashSet::new();
    for p in phases {
        phase_set.insert((*p).clone());
    }
    coordinator.pending = Some(
        dynrunner_scheduler_api::PendingPool::new(phase_set, HashMap::new()).expect("pool init"),
    );
}

/// Drive a `SpawnTasks` command through the dispatch path and
/// return the per-index error list. Centralised so every test
/// uses the same call sequence.
async fn spawn_via_handler(
    coordinator: &mut PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
    tasks: Vec<dynrunner_core::TaskInfo<TestId>>,
) -> Result<Vec<(usize, super::SpawnError)>, String> {
    let (reply_tx, reply_rx) = oneshot::channel();
    super::handle_primary_command(
        coordinator,
        PrimaryCommand::SpawnTasks {
            tasks,
            reply: reply_tx,
        },
        &mut None,
    )
    .await;
    reply_rx.await.expect("reply oneshot closed")
}

/// 3 fresh tasks with no deps: all 3 land in Pending in the CRDT
/// and are reinjected into the live pool.
#[tokio::test(flavor = "current_thread")]
async fn spawn_tasks_all_pending_dispatched() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut coordinator, _mesh) = make_coordinator();
            let mut a = make_binary("a", 100);
            a.task_id = "a_id".into();
            let mut b = make_binary("b", 100);
            b.task_id = "b_id".into();
            let mut c = make_binary("c", 100);
            c.task_id = "c_id".into();
            seed_pool(&mut coordinator, &[&a.phase_id]);

            let errors = spawn_via_handler(&mut coordinator, vec![a.clone(), b.clone(), c.clone()])
                .await
                .expect("spawn_tasks succeeds");
            assert!(
                errors.is_empty(),
                "no per-index errors expected: {errors:?}"
            );

            for binary in &[&a, &b, &c] {
                let hash = compute_task_hash(binary);
                assert!(
                    matches!(
                        coordinator.cluster_state.task_state(&hash),
                        Some(TaskState::Pending { .. })
                    ),
                    "task {:?} must land in Pending",
                    binary.task_id
                );
                assert!(
                    coordinator
                        .pool()
                        .iter()
                        .any(|t| t.task_id == binary.task_id),
                    "task {:?} must be re-injected into the live pool",
                    binary.task_id
                );
            }
        })
        .await;
}

/// Task A depends on Pending task B: A lands in `Blocked{on=B}`,
/// NOT in the pool. B was seeded as Pending in the ledger.
#[tokio::test(flavor = "current_thread")]
async fn spawn_tasks_with_pending_dep_lands_blocked() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut coordinator, _mesh) = make_coordinator();
            let mut b = make_binary("b", 100);
            b.task_id = "b_id".into();
            let b_hash = compute_task_hash(&b);
            coordinator.cluster_state.apply(ClusterMutation::TaskAdded {
                hash: b_hash.clone(),
                task: b.clone(),
            });
            seed_pool(&mut coordinator, &[&b.phase_id]);

            let mut a = make_binary("a", 100);
            a.task_id = "a_id".into();
            a.task_depends_on = vec![TaskDep {
                task_id: "b_id".into(),
                phase_id: PhaseId::from("default"),
                inherit_outputs: false,
            }];

            let errors = spawn_via_handler(&mut coordinator, vec![a.clone()])
                .await
                .expect("spawn_tasks succeeds");
            assert!(
                errors.is_empty(),
                "no per-index errors expected: {errors:?}"
            );

            let a_hash = compute_task_hash(&a);
            match coordinator.cluster_state.task_state(&a_hash) {
                Some(TaskState::Blocked { on, .. }) => {
                    assert_eq!(on, &b_hash, "Blocked.on must point to dep's hash");
                }
                other => panic!("expected Blocked, got {other:?}"),
            }
            assert!(
                !coordinator.pool().iter().any(|t| t.task_id == a.task_id),
                "Blocked task must not be in the pool"
            );
        })
        .await;
}

/// Task A depends on Completed task B: A lands in Pending (deps
/// resolved) and is reinjected into the pool.
#[tokio::test(flavor = "current_thread")]
async fn spawn_tasks_with_completed_dep_lands_pending() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut coordinator, _mesh) = make_coordinator();
            let mut b = make_binary("b", 100);
            b.task_id = "b_id".into();
            let b_hash = compute_task_hash(&b);
            coordinator.cluster_state.apply(ClusterMutation::TaskAdded {
                hash: b_hash.clone(),
                task: b.clone(),
            });
            coordinator
                .cluster_state
                .apply(ClusterMutation::TaskCompleted {
                    attempt: 0,
                    hash: b_hash.clone(),
                    result_data: None,
                });
            seed_pool(&mut coordinator, &[&b.phase_id]);

            let mut a = make_binary("a", 100);
            a.task_id = "a_id".into();
            a.task_depends_on = vec![TaskDep {
                task_id: "b_id".into(),
                phase_id: PhaseId::from("default"),
                inherit_outputs: false,
            }];

            let errors = spawn_via_handler(&mut coordinator, vec![a.clone()])
                .await
                .expect("spawn_tasks succeeds");
            assert!(
                errors.is_empty(),
                "no per-index errors expected: {errors:?}"
            );

            let a_hash = compute_task_hash(&a);
            assert!(
                matches!(
                    coordinator.cluster_state.task_state(&a_hash),
                    Some(TaskState::Pending { .. })
                ),
                "task with all-Completed deps must land in Pending"
            );
            assert!(
                coordinator.pool().iter().any(|t| t.task_id == a.task_id),
                "Pending task must be in the pool"
            );
        })
        .await;
}

/// Task A depends on Unfulfillable task B: A lands in
/// `Blocked{on=B}`. The CRDT's auto-resume on a later
/// `TaskCompleted` will move A back to Pending — same shape as
/// the legacy cascade-pause path.
#[tokio::test(flavor = "current_thread")]
async fn spawn_tasks_with_unfulfillable_dep_lands_blocked() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut coordinator, _mesh) = make_coordinator();
            let mut b = make_binary("b", 100);
            b.task_id = "b_id".into();
            let b_hash = compute_task_hash(&b);
            coordinator.cluster_state.apply(ClusterMutation::TaskAdded {
                hash: b_hash.clone(),
                task: b.clone(),
            });
            coordinator
                .cluster_state
                .apply(ClusterMutation::TaskFailed {
                    attempt: 0,
                    hash: b_hash.clone(),
                    kind: ErrorType::Unfulfillable {
                        reason: "missing toolchain".to_string().into(),
                    },
                    error: "unfulfillable".into(),
                    version: Default::default(),
                });
            seed_pool(&mut coordinator, &[&b.phase_id]);

            let mut a = make_binary("a", 100);
            a.task_id = "a_id".into();
            a.task_depends_on = vec![TaskDep {
                task_id: "b_id".into(),
                phase_id: PhaseId::from("default"),
                inherit_outputs: false,
            }];

            let errors = spawn_via_handler(&mut coordinator, vec![a.clone()])
                .await
                .expect("spawn_tasks succeeds");
            assert!(
                errors.is_empty(),
                "no per-index errors expected: {errors:?}"
            );

            let a_hash = compute_task_hash(&a);
            match coordinator.cluster_state.task_state(&a_hash) {
                Some(TaskState::Blocked { on, .. }) => {
                    assert_eq!(on, &b_hash);
                }
                other => panic!("expected Blocked-on-unfulfillable, got {other:?}"),
            }
            assert!(
                !coordinator.pool().iter().any(|t| t.task_id == a.task_id),
                "Blocked-on-Unfulfillable task must not be in the pool"
            );
        })
        .await;
}

/// Vec with 3 tasks, 1 with a hash that ALREADY EXISTS in the ledger
/// (an idempotent re-spawn — the failover-replay class): the duplicate
/// surfaces as a per-index `SpawnError::DuplicateTaskHash` and is
/// DROPPED (no-op dedup); the run is NOT invalidated and the other 2
/// fresh tasks DO land. Revert-check companion:
/// `spawn_tasks_within_batch_duplicate_invalidates_run_wide` covers the
/// genuine-bug case that DOES still invalidate.
#[tokio::test(flavor = "current_thread")]
async fn spawn_tasks_already_in_ledger_dedups_no_run_wide_invalidation() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut coordinator, _mesh) = make_coordinator();
            // Pre-seed `dup` so the second input is an already-in-ledger
            // re-spawn (the failover replay class).
            let mut dup = make_binary("dup", 100);
            dup.task_id = "dup_id".into();
            let dup_hash = compute_task_hash(&dup);
            coordinator.cluster_state.apply(ClusterMutation::TaskAdded {
                hash: dup_hash.clone(),
                task: dup.clone(),
            });
            seed_pool(&mut coordinator, &[&dup.phase_id]);

            let mut a = make_binary("a", 100);
            a.task_id = "a_id".into();
            let mut c = make_binary("c", 100);
            c.task_id = "c_id".into();

            let errors =
                spawn_via_handler(&mut coordinator, vec![a.clone(), dup.clone(), c.clone()])
                    .await
                    .expect("spawn_tasks succeeds (per-task failures are NOT vec-level)");
            assert_eq!(errors.len(), 1, "exactly one per-index error: {errors:?}");
            let (idx, err) = &errors[0];
            assert_eq!(*idx, 1, "duplicate at vec position 1");
            match err {
                super::SpawnError::DuplicateTaskHash(h) => assert_eq!(h, &dup_hash),
                other => panic!("expected DuplicateTaskHash, got {other:?}"),
            }
            // An already-in-ledger duplicate is an IDEMPOTENT re-spawn, NOT a
            // run-killer: the cluster continues, the pre-existing `dup` stays
            // exactly as it was (Pending — the prior entry is authoritative),
            // and the fresh a / c DO dispatch (the batch is no longer
            // dropped). This is the failover-replay dedup.
            assert!(
                coordinator.cluster_state.run_aborted().is_none(),
                "an idempotent re-spawn does not abort the run"
            );
            assert!(
                matches!(
                    coordinator.cluster_state.task_state(&dup_hash),
                    Some(TaskState::Pending { .. })
                ),
                "the pre-existing entry is the authoritative copy and is untouched"
            );
            assert!(
                matches!(
                    coordinator.cluster_state.task_state(&compute_task_hash(&a)),
                    Some(TaskState::Pending { .. })
                ),
                "fresh task a IS applied (the batch survivors still dispatch)"
            );
            assert!(
                matches!(
                    coordinator.cluster_state.task_state(&compute_task_hash(&c)),
                    Some(TaskState::Pending { .. })
                ),
                "fresh task c IS applied (the batch survivors still dispatch)"
            );
        })
        .await;
}

/// (b) HEADLINE: a runtime spawn of identities that ALL already exist in
/// the CRDT (the failover-replay re-fire — a promoted primary's
/// `on_phase_end` hook re-spawning every child it spawned pre-failover)
/// is a NO-OP DEDUP: no new duplicate tasks, NO run-wide invalidation, NO
/// `RunError::SpawnRejected`. Seed three tasks (one `Completed`, two
/// `Pending`) then re-spawn ALL THREE identities. Pre-fix this nets
/// `valid_tasks.is_empty()` and either invalidates run-wide or records a
/// loud `spawn_rejected` over not-actually-lost work.
#[tokio::test(flavor = "current_thread")]
async fn spawn_tasks_all_already_in_ledger_is_noop_dedup() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut coordinator, _mesh) = make_coordinator();

            let mut done = make_binary("done", 100);
            done.task_id = "done_id".into();
            let done_hash = compute_task_hash(&done);
            coordinator.cluster_state.apply(ClusterMutation::TaskAdded {
                hash: done_hash.clone(),
                task: done.clone(),
            });
            coordinator
                .cluster_state
                .apply(ClusterMutation::TaskCompleted {
                    attempt: 0,
                    hash: done_hash.clone(),
                    result_data: None,
                });

            let mut p1 = make_binary("p1", 100);
            p1.task_id = "p1_id".into();
            let p1_hash = compute_task_hash(&p1);
            coordinator.cluster_state.apply(ClusterMutation::TaskAdded {
                hash: p1_hash.clone(),
                task: p1.clone(),
            });

            let mut p2 = make_binary("p2", 100);
            p2.task_id = "p2_id".into();
            let p2_hash = compute_task_hash(&p2);
            coordinator.cluster_state.apply(ClusterMutation::TaskAdded {
                hash: p2_hash.clone(),
                task: p2.clone(),
            });

            seed_pool(&mut coordinator, &[&p1.phase_id]);

            // Re-spawn ALL three already-present identities (failover replay).
            let errors =
                spawn_via_handler(&mut coordinator, vec![done.clone(), p1.clone(), p2.clone()])
                    .await
                    .expect("spawn_tasks succeeds (per-task failures are not vec-level)");
            assert_eq!(errors.len(), 3, "all three surface per-index DuplicateTaskHash");
            for (_, err) in &errors {
                assert!(
                    matches!(err, super::SpawnError::DuplicateTaskHash(_)),
                    "every re-spawn is an idempotent already-in-ledger dup, got {err:?}"
                );
            }
            // No run-wide invalidation: every entry keeps its prior state.
            assert!(
                coordinator.cluster_state.run_aborted().is_none(),
                "the failover-replay re-spawn does not abort the run"
            );
            assert!(
                matches!(
                    coordinator.cluster_state.task_state(&done_hash),
                    Some(TaskState::Completed { .. })
                ),
                "completed stays completed"
            );
            assert!(
                matches!(
                    coordinator.cluster_state.task_state(&p1_hash),
                    Some(TaskState::Pending { .. })
                ),
                "p1 NOT invalidated run-wide (idempotent dedup)"
            );
            assert!(
                matches!(
                    coordinator.cluster_state.task_state(&p2_hash),
                    Some(TaskState::Pending { .. })
                ),
                "p2 NOT invalidated run-wide (idempotent dedup)"
            );
            // The loud-fail backstop must NOT have recorded these as lost
            // work: an all-already-in-ledger batch drops no work (the prior
            // copies are authoritative + counted in total_tasks).
            assert!(
                coordinator.spawn_rejected_task_ids.is_empty(),
                "an all-idempotent-re-spawn nets ZERO spawn_rejected (no work lost)"
            );
        })
        .await;
}

/// (b) WITHIN-BATCH still caught: two tasks in ONE fresh spawn batch
/// share an identity (a genuine ambiguous producer batch — no
/// authoritative prior copy). This DOES escalate to a run-wide
/// invalidation (every not-yet-terminal task → InvalidTask), the cluster
/// continues, and the within-batch dup surfaces as
/// `SpawnError::DuplicateInBatch`. Guards against fix (b) weakening the
/// genuine-bug detection.
#[tokio::test(flavor = "current_thread")]
async fn spawn_tasks_within_batch_duplicate_invalidates_run_wide() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut coordinator, _mesh) = make_coordinator();

            // A pre-existing Pending task that must flip to InvalidTask when
            // the within-batch dup escalates run-wide.
            let mut p1 = make_binary("p1", 100);
            p1.task_id = "p1_id".into();
            let p1_hash = compute_task_hash(&p1);
            coordinator.cluster_state.apply(ClusterMutation::TaskAdded {
                hash: p1_hash.clone(),
                task: p1.clone(),
            });
            seed_pool(&mut coordinator, &[&p1.phase_id]);

            // A brand-new identity that is NOT in the ledger, sent TWICE in
            // the same batch — the within-batch duplicate.
            let mut fresh = make_binary("fresh", 100);
            fresh.task_id = "fresh_id".into();
            let fresh_hash = compute_task_hash(&fresh);

            let errors =
                spawn_via_handler(&mut coordinator, vec![fresh.clone(), fresh.clone()])
                    .await
                    .expect("spawn_tasks succeeds (per-task failures are not vec-level)");
            // The SECOND occurrence is the within-batch dup; the first was
            // valid-then-dropped because the batch escalates run-wide.
            assert_eq!(errors.len(), 1, "the within-batch dup surfaces once: {errors:?}");
            let (idx, err) = &errors[0];
            assert_eq!(*idx, 1, "the duplicate is the 2nd batch item");
            match err {
                super::SpawnError::DuplicateInBatch(h) => assert_eq!(h, &fresh_hash),
                other => panic!("expected DuplicateInBatch, got {other:?}"),
            }
            // Run-wide invalidation (cluster continues, no RunAborted).
            assert!(
                coordinator.cluster_state.run_aborted().is_none(),
                "3b does not abort the run"
            );
            assert!(
                matches!(
                    coordinator.cluster_state.task_state(&p1_hash),
                    Some(TaskState::InvalidTask { .. })
                ),
                "the pre-existing Pending task is invalidated run-wide"
            );
            // The fresh duplicated identity is NOT applied (batch dropped).
            assert!(
                coordinator.cluster_state.task_state(&fresh_hash).is_none(),
                "the within-batch-dup batch is dropped, not applied"
            );
        })
        .await;
}

/// Vec with 3 tasks, 1 carrying `task_depends_on=["nope"]`: the
/// bad-dep entry surfaces as `SpawnError::UnknownDependency`;
/// the other 2 land normally.
#[tokio::test(flavor = "current_thread")]
async fn spawn_tasks_unknown_dependency_returns_per_index_error() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut coordinator, _mesh) = make_coordinator();
            let mut a = make_binary("a", 100);
            a.task_id = "a_id".into();
            seed_pool(&mut coordinator, &[&a.phase_id]);
            let mut bad = make_binary("bad", 100);
            bad.task_id = "bad_id".into();
            bad.task_depends_on = vec![TaskDep {
                task_id: "nope".into(),
                phase_id: PhaseId::from("default"),
                inherit_outputs: false,
            }];
            let mut c = make_binary("c", 100);
            c.task_id = "c_id".into();

            let errors =
                spawn_via_handler(&mut coordinator, vec![a.clone(), bad.clone(), c.clone()])
                    .await
                    .expect("spawn_tasks succeeds");
            assert_eq!(errors.len(), 1, "exactly one per-index error: {errors:?}");
            let (idx, err) = &errors[0];
            assert_eq!(*idx, 1);
            match err {
                super::SpawnError::UnknownDependency {
                    task_hash,
                    dep_task_id,
                } => {
                    assert_eq!(task_hash, &compute_task_hash(&bad));
                    assert_eq!(dep_task_id, "nope");
                }
                other => panic!("expected UnknownDependency, got {other:?}"),
            }
            // Other two land normally.
            assert!(matches!(
                coordinator.cluster_state.task_state(&compute_task_hash(&a)),
                Some(TaskState::Pending { .. })
            ));
            assert!(matches!(
                coordinator.cluster_state.task_state(&compute_task_hash(&c)),
                Some(TaskState::Pending { .. })
            ));
            // And the bad entry is NOT in the ledger.
            assert!(
                coordinator
                    .cluster_state
                    .task_state(&compute_task_hash(&bad))
                    .is_none(),
                "bad-dep task must not be inserted in the ledger"
            );
        })
        .await;
}

/// Live-primary regression: a `SpawnTasks` command must refresh
/// `total_tasks` from the post-apply CRDT `task_count()`, symmetric
/// with the receive-side mirror in `handle_cluster_mutation`. Pins
/// the asm-tokenizer phase-3 memmap race: pre-seeded total reflects
/// the run-start binary count N; a callback fires
/// `spawn_tasks(memmap_items)` with M brand-new tasks; without the
/// refresh `total_tasks` stays at N and the operational-loop exit
/// check (`completed + failed >= total_tasks`) trips the moment all
/// N pre-spawn tasks finish — the post-spawn task sits dispatchable
/// in the pool but the loop has already signalled RunComplete.
///
/// Asserts both faces of the contract:
///   (1) `total_tasks` grew to `N + M` after the spawn applied.
///   (2) The exit-check predicate that the operational loop reads
///       (`completed + failed >= total_tasks`) returns `false` when
///       `completed == N` and `failed == 0` — proving the loop
///       stays alive long enough to see the post-spawn task through.
///
/// The pre-spawn fixture seeds the same shape `run()` would: N
/// tasks land in `cluster_state` via `TaskAdded` and the field
/// `total_tasks = N` is set explicitly (the seed_cluster_state path
/// writes the same value at `coordinator.rs:1238`). The pool is
/// seeded with the spawned task's phase so the post-apply reinject
/// has a destination — same shape every other spawn_tasks test in
/// this file uses.
#[tokio::test(flavor = "current_thread")]
async fn spawn_tasks_refreshes_total_tasks_from_cluster_state() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut coordinator, _mesh) = make_coordinator();

            // Pre-spawn: N=4 binaries seeded into the CRDT, exactly the
            // shape `seed_cluster_state` produces at run start.
            let mut pre_spawn: Vec<dynrunner_core::TaskInfo<TestId>> = Vec::new();
            for i in 0..4 {
                let mut b = make_binary(&format!("pre_{i}"), 100);
                b.task_id = format!("pre_{i}_id");
                let h = compute_task_hash(&b);
                coordinator.cluster_state.apply(ClusterMutation::TaskAdded {
                    hash: h,
                    task: b.clone(),
                });
                pre_spawn.push(b);
            }
            // Mirror `run()`s explicit set at coordinator.rs:1238 — the
            // field starts as a derived view written from `binaries.len()`.
            coordinator.total_tasks = pre_spawn.len();
            let n = coordinator.total_tasks;
            assert_eq!(
                n,
                coordinator.cluster_state.task_count(),
                "fixture invariant: pre-spawn total_tasks matches CRDT view"
            );

            // Seed the pool covering the spawned task's phase so the
            // post-apply reinject has a destination. Every other
            // spawn_tasks test uses the same `seed_pool` helper.
            let mut spawned = make_binary("memmap_0", 100);
            spawned.task_id = "memmap_0_id".into();
            seed_pool(&mut coordinator, &[&spawned.phase_id]);

            // M=1 brand-new task with no deps: the asm-tokenizer phase-3
            // memmap discovery surfaced exactly 1 item.
            let m = 1usize;
            let spawned_hash = compute_task_hash(&spawned);

            let errors = spawn_via_handler(&mut coordinator, vec![spawned.clone()])
                .await
                .expect("spawn_tasks succeeds on well-formed input");
            assert!(
                errors.is_empty(),
                "no per-index errors expected on a no-deps freshly-Pending task: {errors:?}"
            );

            // (1) The CRDT grew by M; total_tasks mirrors that growth.
            assert_eq!(
                coordinator.cluster_state.task_count(),
                n + m,
                "CRDT task_count grew by the spawn batch size"
            );
            assert_eq!(
                coordinator.total_tasks,
                n + m,
                "total_tasks must refresh from cluster_state.task_count() after \
             apply_spawn_tasks; without this refresh the operational loop's \
             exit check (`completed + failed >= total_tasks`) trips at the \
             pre-spawn total the moment all N initial tasks finish"
            );

            // Pin the surface of the contract: the spawned task is in
            // the CRDT as Pending AND reinjected into the pool — the
            // existing spawn_tasks_all_pending_dispatched test already
            // pins this for the no-prior-state case; we re-assert here
            // because the asm-tokenizer bug specifically required BOTH
            // the pool reinject AND the total_tasks refresh to land:
            // the pool reinject alone would still see the loop exit
            // before the dispatch tick if total_tasks lagged.
            assert!(
                matches!(
                    coordinator.cluster_state.task_state(&spawned_hash),
                    Some(TaskState::Pending { .. })
                ),
                "spawned task lands as Pending in the CRDT"
            );
            assert!(
                coordinator
                    .pool()
                    .iter()
                    .any(|t| t.task_id == spawned.task_id),
                "spawned task is re-injected into the live pool"
            );

            // (2) Exit-check predicate at the pre-spawn-success
            // boundary: `completed == N, failed == 0`. The operational
            // loop's check is `completed_tasks.len() + failed_tasks.len()
            // >= total_tasks` (see lifecycle/operational_loop.rs:166).
            // With the refresh in place this evaluates to `4 + 0 >= 5`
            // = false; without it the read was `4 + 0 >= 4` = true, and
            // RunComplete fired before memmap dispatched.
            let succeeded = n; // simulate the post-pre-spawn-completion state
            let failed = 0usize;
            let would_exit = succeeded + failed >= coordinator.total_tasks;
            assert!(
                !would_exit,
                "exit predicate must be false when only the N pre-spawn tasks \
             have completed; got succeeded={succeeded} failed={failed} \
             total_tasks={} — if this fires, the refresh did NOT happen and \
             the loop would exit before the spawned task dispatches",
                coordinator.total_tasks
            );
        })
        .await;
}

/// SITE B (runtime SpawnTasks): a spawned task depending on
/// `(phase=B, foo)` while `foo` exists only in a DIFFERENT phase (A)
/// is minted `UnknownDependency` and NOT applied — it must NOT land
/// silently Pending with an unsatisfiable dep.
///
/// Pre-fix the validator's dep resolution was phase-blind, so the
/// phase-B dep passed (any phase carrying `foo` satisfied it). The
/// task then reached `apply_tasks_spawned`, whose phase-aware
/// `task_hash_for_dep(B, foo)` returned `None` → "treat as resolved" →
/// the task landed Pending and never-runnable.
#[tokio::test(flavor = "current_thread")]
async fn spawn_tasks_cross_phase_missing_dep_is_invalid_not_silent_pending() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut coordinator, _mesh) = make_coordinator();
            // `foo` exists in phase A only (seeded Pending in the ledger).
            let mut foo = make_binary("foo", 100);
            foo.phase_id = PhaseId::from("A");
            foo.task_id = "foo".into();
            let foo_hash = compute_task_hash(&foo);
            coordinator.cluster_state.apply(ClusterMutation::TaskAdded {
                hash: foo_hash.clone(),
                task: foo.clone(),
            });
            seed_pool(
                &mut coordinator,
                &[&PhaseId::from("A"), &PhaseId::from("B")],
            );

            // `child` in phase B depends on (phase=B, foo) — absent in B.
            let mut child = make_binary("child", 100);
            child.phase_id = PhaseId::from("B");
            child.task_id = "child".into();
            child.task_depends_on = vec![TaskDep {
                task_id: "foo".into(),
                phase_id: PhaseId::from("B"),
                inherit_outputs: false,
            }];

            let errors = spawn_via_handler(&mut coordinator, vec![child.clone()])
                .await
                .expect("spawn_tasks call itself succeeds");
            assert_eq!(errors.len(), 1, "the phase-B dep is unsatisfiable");
            match &errors[0].1 {
                super::SpawnError::UnknownDependency { dep_task_id, .. } => {
                    assert_eq!(dep_task_id, "foo");
                }
                other => panic!("expected UnknownDependency, got {other:?}"),
            }

            // The task must NOT have been applied (no silent Pending).
            let child_hash = compute_task_hash(&child);
            assert!(
                coordinator.cluster_state.task_state(&child_hash).is_none(),
                "the invalid-dep task must NOT land in the ledger"
            );
            assert!(
                !coordinator.pool().iter().any(|t| t.task_id == "child"),
                "the invalid-dep task must NOT be in the pool"
            );
        })
        .await;
}

/// SITE B companion: a cross-phase dep naming the RIGHT phase resolves.
/// `child` (phase B) depends on (phase=A, foo) where `foo` lives in A —
/// it applies and lands Blocked-on-foo (foo is Pending).
#[tokio::test(flavor = "current_thread")]
async fn spawn_tasks_cross_phase_dep_naming_right_phase_resolves() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut coordinator, _mesh) = make_coordinator();
            let mut foo = make_binary("foo", 100);
            foo.phase_id = PhaseId::from("A");
            foo.task_id = "foo".into();
            let foo_hash = compute_task_hash(&foo);
            coordinator.cluster_state.apply(ClusterMutation::TaskAdded {
                hash: foo_hash.clone(),
                task: foo.clone(),
            });
            seed_pool(
                &mut coordinator,
                &[&PhaseId::from("A"), &PhaseId::from("B")],
            );

            let mut child = make_binary("child", 100);
            child.phase_id = PhaseId::from("B");
            child.task_id = "child".into();
            child.task_depends_on = vec![TaskDep {
                task_id: "foo".into(),
                phase_id: PhaseId::from("A"),
                inherit_outputs: false,
            }];

            let errors = spawn_via_handler(&mut coordinator, vec![child.clone()])
                .await
                .expect("spawn_tasks succeeds");
            assert!(errors.is_empty(), "cross-phase dep resolves: {errors:?}");

            let child_hash = compute_task_hash(&child);
            match coordinator.cluster_state.task_state(&child_hash) {
                Some(TaskState::Blocked { on, .. }) => {
                    assert_eq!(on, &foo_hash, "Blocked.on points to foo's phase-A hash");
                }
                other => panic!("expected Blocked-on-foo, got {other:?}"),
            }
        })
        .await;
}

/// `ClusterMutation::TasksSpawned` round-trips through serde.
/// Pins wire-codec compatibility for the new variant.
#[test]
fn tasks_spawned_mutation_round_trips_through_serde() {
    let mut a = make_binary("a", 100);
    a.task_id = "a_id".into();
    let mut b = make_binary("b", 100);
    b.task_id = "b_id".into();
    b.task_depends_on = vec![TaskDep {
        task_id: "a_id".into(),
        phase_id: PhaseId::from("default"),
        inherit_outputs: false,
    }];
    let m: ClusterMutation<TestId> = ClusterMutation::TasksSpawned {
        tasks: vec![a.clone(), b.clone()],
    };
    let json = serde_json::to_string(&m).expect("serialize");
    let round: ClusterMutation<TestId> = serde_json::from_str(&json).expect("deserialize");
    match round {
        ClusterMutation::TasksSpawned { tasks } => {
            assert_eq!(tasks.len(), 2);
            assert_eq!(tasks[0].task_id, "a_id");
            assert_eq!(tasks[1].task_id, "b_id");
            assert_eq!(
                tasks[1].task_depends_on,
                vec![TaskDep {
                    task_id: "a_id".to_string(),
                    phase_id: PhaseId::from("default"),
                    inherit_outputs: false
                }]
            );
        }
        other => panic!("variant lost in round-trip: {other:?}"),
    }
}

/// F4 event-shape: a fail → (retry) succeed sequence for the SAME phase
/// increments BOTH the Failed and Completed per-phase EVENT tallies (each
/// terminal OBSERVATION is one event), and the tallies survive a promotion
/// (snapshot → restore) reporting the SAME event-shaped numbers — NOT a
/// terminal-state projection (the CRDT holds one terminal state, but the
/// event count of 1 failed + 1 completed is what `on_phase_end` reports).
#[tokio::test(flavor = "current_thread")]
async fn phase_event_tallies_are_event_shaped_and_survive_promotion() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut coordinator, _mesh) = make_coordinator();
            let phase = PhaseId::from("default");
            let mut phase_set = std::collections::HashSet::new();
            phase_set.insert(phase.clone());
            coordinator.pending = Some(
                dynrunner_scheduler_api::PendingPool::new(phase_set, HashMap::new())
                    .expect("pool init"),
            );

            // The task fails once, then (after a retry reinject) completes —
            // the live primary observes BOTH terminal events.
            coordinator
                .note_item_failed(&phase, Some("a_id"), &mut None)
                .await;
            coordinator
                .note_item_completed(&phase, Some("a_id"), &mut None)
                .await;

            // BOTH event tallies are 1 — event-shaped, not a terminal
            // projection (a terminal projection of the single converged
            // state would show failed=0).
            assert_eq!(
                coordinator.phase_failed_for_test(&phase),
                1,
                "the fail event is counted even after the later success"
            );
            assert_eq!(coordinator.phase_completed_for_test(&phase), 1);

            // Promotion: a fresh primary restores the live snapshot and
            // reports the SAME event numbers (the events were replicated).
            let snap = coordinator.cluster_state_for_test().snapshot();
            let (mut promoted, _mesh2) = make_coordinator();
            promoted.cluster_state_mut_for_test().restore(snap);
            assert_eq!(
                promoted.phase_failed_for_test(&phase),
                1,
                "the failed EVENT tally survives promotion"
            );
            assert_eq!(
                promoted.phase_completed_for_test(&phase),
                1,
                "the completed EVENT tally survives promotion"
            );
        })
        .await;
}

/// P3-reinject: with an UNBOUNDED cap (`None`), the reinject handler accepts
/// repeatedly and NEVER originates the used counter (no cap to enforce), so
/// the replicated `unfulfillable_reinject_used` stays empty.
#[tokio::test(flavor = "current_thread")]
async fn unbounded_reinject_cap_skips_used_origination() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut coordinator, _mesh) = make_coordinator();
            // `None` == unbounded.
            coordinator.set_unfulfillable_reinject_max_per_task(None);
            let binary = make_binary("a", 100);
            let hash = compute_task_hash(&binary);
            coordinator.cluster_state.apply(ClusterMutation::TaskAdded {
                hash: hash.clone(),
                task: binary.clone(),
            });
            let mut phase_set = std::collections::HashSet::new();
            phase_set.insert(binary.phase_id.clone());
            coordinator.pending = Some(
                dynrunner_scheduler_api::PendingPool::new(phase_set, HashMap::new())
                    .expect("pool init"),
            );

            // Reinject twice; each time re-set to Unfulfillable first.
            for round in 0..2 {
                coordinator
                    .cluster_state
                    .apply(ClusterMutation::TaskFailed {
                        attempt: 0,
                        hash: hash.clone(),
                        kind: ErrorType::Unfulfillable {
                            reason: format!("missing {round}").into(),
                        },
                        error: "unfulfillable".into(),
                        version: Default::default(),
                    });
                let (reply_tx, reply_rx) = oneshot::channel();
                super::handle_primary_command(
                    &mut coordinator,
                    PrimaryCommand::ReinjectTask {
                        hash: hash.clone(),
                        reply: reply_tx,
                    },
                    &mut None,
                )
                .await;
                assert!(
                    reply_rx.await.unwrap().is_ok(),
                    "unbounded reinject always accepts (round {round})"
                );
            }

            // No used counter was ever originated for an unbounded cap.
            assert_eq!(
                coordinator
                    .cluster_state_for_test()
                    .unfulfillable_reinject_used_for(&hash),
                0,
                "unbounded cap must not originate the used counter"
            );
        })
        .await;
}

/// P3-reinject: with a BOUNDED cap the used counter IS originated and
/// survives a promotion, so a promoted primary does NOT re-grant the
/// reinject budget (the budget is clear-gated by grow-only-MAX inheritance).
#[tokio::test(flavor = "current_thread")]
async fn bounded_reinject_used_survives_promotion() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut coordinator, _mesh) = make_coordinator();
            coordinator.set_unfulfillable_reinject_max_per_task(Some(2));
            let binary = make_binary("a", 100);
            let hash = compute_task_hash(&binary);
            coordinator.cluster_state.apply(ClusterMutation::TaskAdded {
                hash: hash.clone(),
                task: binary.clone(),
            });
            coordinator
                .cluster_state
                .apply(ClusterMutation::TaskFailed {
                    attempt: 0,
                    hash: hash.clone(),
                    kind: ErrorType::Unfulfillable {
                        reason: "missing".into(),
                    },
                    error: "unfulfillable".into(),
                    version: Default::default(),
                });
            let mut phase_set = std::collections::HashSet::new();
            phase_set.insert(binary.phase_id.clone());
            coordinator.pending = Some(
                dynrunner_scheduler_api::PendingPool::new(phase_set, HashMap::new())
                    .expect("pool init"),
            );

            // One successful reinject — used bumps to 1.
            let (reply_tx, reply_rx) = oneshot::channel();
            super::handle_primary_command(
                &mut coordinator,
                PrimaryCommand::ReinjectTask {
                    hash: hash.clone(),
                    reply: reply_tx,
                },
                &mut None,
            )
            .await;
            assert!(reply_rx.await.unwrap().is_ok());
            assert_eq!(
                coordinator
                    .cluster_state_for_test()
                    .unfulfillable_reinject_used_for(&hash),
                1
            );

            // Promotion inherits the used count via max-merge: the promoted
            // primary derives remaining = 2 − 1 = 1 (NOT a fresh cap of 2).
            let snap = coordinator.cluster_state_for_test().snapshot();
            let (mut promoted, _mesh2) = make_coordinator();
            promoted.cluster_state_mut_for_test().restore(snap);
            assert_eq!(
                promoted
                    .cluster_state_for_test()
                    .unfulfillable_reinject_used_for(&hash),
                1,
                "the consumed reinject budget survives promotion"
            );
        })
        .await;
}
