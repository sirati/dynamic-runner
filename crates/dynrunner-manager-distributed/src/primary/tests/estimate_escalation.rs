//! Best-effort estimate-escalation dispatch coverage (#499).
//!
//! The distributed primary's per-worker reserved budget is the
//! `max / num_workers` parallel-scheduling fraction. A task whose estimate
//! exceeds every IDLE worker's per-worker budget `NoFit`s every worker, is
//! never dispatched, never completes, never fails — so the operational
//! loop's `completed + failed >= total` counter exit can never trip and the
//! whole pool eventually strands (`ClusterCollapsed`). The escalation pass
//! converts that into best-effort dispatch (boost ONE idle worker to the
//! node's full capacity and re-attempt) plus, for a genuinely-unfittable
//! task (estimate > the largest node's full capacity), an INDIVIDUAL
//! `ResourceExhausted` failure that lets the counter exit trip — matching
//! the local manager's unassigned-phase contract.
//!
//! Deterministic + synchronous, mirroring `capacity_dispatch.rs`: a 2-worker
//! secondary is rostered from the CRDT capacity record (worker 0 budget =
//! full node, worker 1 = ~half), worker 0 is made BUSY so only the
//! smaller-budget worker 1 is idle, and the worker-management bus is driven
//! by hand (`react_to_worker_signal_batch`). Each scenario pairs the fix
//! behaviour with a revert-check that pins the pre-fix strand.

use super::*;

use dynrunner_core::{ErrorType, PhaseId, ResourceAmount, ResourceKind, ResourceMap, TypeId};
use dynrunner_scheduler_api::ResourceEstimator;
use std::collections::HashMap as StdHashMap;

use crate::primary::wire::compute_task_hash;
use crate::worker_signal::{WorkerMgmtSignal, WorkerSignalBatch};

/// 8 GiB node so worker 0's reserved budget (full node) is 8 GiB and
/// worker 1's (`max/2 + base_overhead`) is ~4.15 GiB. A task estimated
/// between those two bounds fits the node but not worker 1.
const NODE_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const GIB: u64 = 1024 * 1024 * 1024;

/// Estimator keyed by `task_id`, so a small "busy" task and a large
/// "stuck" task can coexist (the shared `FixedEstimator` returns one value
/// for all tasks). Unknown ids fall back to a trivial estimate.
#[derive(Clone)]
struct PerTaskEstimator(StdHashMap<String, u64>);

impl ResourceEstimator<TestId> for PerTaskEstimator {
    fn estimate(&self, task: &TaskInfo<TestId>) -> ResourceMap {
        let bytes = self.0.get(&task.task_id).copied().unwrap_or(1);
        ResourceMap::from([(ResourceKind::memory(), bytes)])
    }
}

fn mem(bytes: u64) -> Vec<ResourceAmount> {
    vec![ResourceAmount {
        kind: ResourceKind::memory(),
        amount: bytes,
    }]
}

/// One zero-dep task `name` in phase "p", type "default".
fn one_task(name: &str) -> TaskInfo<TestId> {
    let mut t = make_binary(name, 100);
    t.phase_id = PhaseId::from("p");
    t.type_id = TypeId::from("default");
    t
}

/// Apply a `PeerJoined` + `SecondaryCapacity` (2 workers, 8 GiB) for
/// `sec`, the wire shape that grows the roster from the CRDT.
fn capacity_batch(sec: &str) -> DistributedMessage<TestId> {
    DistributedMessage::ClusterMutation {
        target: None,
        sender_id: "setup".into(),
        timestamp: 0.0,
        mutations: vec![
            ClusterMutation::PeerJoined {
                peer_id: sec.into(),
                is_observer: false,
                can_be_primary: true,
                cap_version: Default::default(),
                member_gen: 0,
            },
            ClusterMutation::SecondaryCapacity {
                secondary: sec.into(),
                worker_count: 2,
                resources: mem(NODE_BYTES),
            },
        ],
    }
}

fn mesh_ready_from(sec: &str) -> DistributedMessage<TestId> {
    DistributedMessage::MeshReady {
        target: None,
        sender_id: sec.into(),
        timestamp: 0.0,
        secondary_id: sec.into(),
        peer_count: 1,
    }
}

/// Drain every `TaskAssignment` `task_id` queued on a secondary's wire.
fn assigned_ids(rx: &mut tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>) -> Vec<String> {
    let mut ids = Vec::new();
    while let Ok(msg) = rx.try_recv() {
        if let DistributedMessage::TaskAssignment { binary_info, .. } = msg {
            ids.push(binary_info.task_id);
        }
    }
    ids
}

/// A bus batch carrying a single `TasksAdded` — what the worker-management
/// arm drains and reacts to.
fn tasks_added_batch() -> WorkerSignalBatch {
    WorkerSignalBatch {
        signals: vec![WorkerMgmtSignal::TasksAdded],
    }
}

type TestPrimary =
    PrimaryCoordinator<ResourceStealingScheduler, PerTaskEstimator, TestId>;

/// Build a single-secondary (2 workers, 8 GiB) primary whose pool holds
/// `tasks`, with the roster grown + mesh-confirmed from the CRDT. Returns
/// the primary, the secondaries' wire ends, and the secondary id.
#[allow(clippy::type_complexity)]
async fn primary_with_tasks(
    estimates: &[(&str, u64)],
) -> (
    TestPrimary,
    Vec<(
        String,
        tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
        tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
    )>,
    String,
    PrimaryMeshKeepalive,
) {
    let (transport, ends) = setup_test(1);
    let est_map: StdHashMap<String, u64> = estimates
        .iter()
        .map(|(id, b)| (id.to_string(), *b))
        .collect();
    let (mut primary, mesh) = build_test_primary(
        test_primary_config(),
        transport,
        ResourceStealingScheduler::memory(),
        PerTaskEstimator(est_map),
    );
    {
        let cs = primary.cluster_state_mut_for_test();
        cs.apply(ClusterMutation::PhaseDepsSet {
            deps: HashMap::from([(PhaseId::from("p"), vec![])]),
        });
        for (id, _) in estimates {
            let task = one_task(id);
            cs.apply(ClusterMutation::TaskAdded {
                hash: compute_task_hash(&task),
                task,
            });
        }
    }
    primary
        .hydrate_from_cluster_state()
        .expect("test fixture: composed task graph is valid");

    // Grow the roster from the CRDT capacity + confirm the mesh leg so the
    // workers are dispatch-eligible (the capacity_dispatch harness flow).
    primary
        .handle_cluster_mutation(capacity_batch("sec-0"), &mut None)
        .await;
    primary.handle_mesh_ready(mesh_ready_from("sec-0"));
    settle_pump().await;
    (primary, ends, "sec-0".to_string(), mesh)
}

/// Make `sec`/`local_worker_id` BUSY with a SYNTHETIC occupier task that
/// is NOT in the pool/CRDT — so the only queued work is the stuck task(s)
/// under test, and the busy worker is genuinely off the idle set. Commits
/// the occupier through the same `commit_assignment` triple a live
/// dispatch uses (it does not require the task to have been pooled).
fn make_worker_busy(primary: &mut TestPrimary, sec: &str, local_worker_id: u32) {
    let idx = primary
        .worker_idx_for(sec, local_worker_id)
        .expect("worker exists in roster");
    let occupier = one_task("occupier");
    let hash = compute_task_hash(&occupier);
    // The slot is idle here, so the commit always takes (#517 guard).
    assert!(
        primary.commit_assignment(
            idx,
            std::sync::Arc::new(occupier),
            hash,
            ResourceMap::from([(ResourceKind::memory(), GIB)]),
        ),
        "make_worker_busy must commit onto the idle slot"
    );
}

/// FIX (fittable-on-node, not on the idle worker). Worker 0 (full-node
/// budget) is busy; worker 1 (~4.15 GiB) is the only idle worker. A task
/// estimated 6 GiB fits the node but NOT worker 1's per-worker budget, so
/// the normal recheck strands it. The escalation boosts worker 1 to the
/// node's full 8 GiB and dispatches it — best-effort rescue.
#[tokio::test(flavor = "current_thread")]
async fn estimate_stalled_task_dispatches_under_node_boost() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Only "big" (6 GiB) is queued: it fits the node (8 GiB) but
            // not worker 1's ~4.15 GiB per-worker budget.
            let (mut primary, mut ends, sec, _mesh) =
                primary_with_tasks(&[("big", 6 * GIB)]).await;

            // Worker 0 (full-node budget) is occupied by a synthetic task,
            // so worker 1 is the only idle slot. Drain any capacity
            // broadcast that rode the wire so later reads see only the
            // escalation dispatch.
            make_worker_busy(&mut primary, &sec, 0);
            let _ = assigned_ids(&mut ends[0].1);

            // A normal recheck cannot place "big": worker 1's 4.15 GiB
            // budget `NoFit`s the 6 GiB estimate, worker 0 is busy. The
            // worker-management arm runs the normal passes AND then the
            // escalation — which boosts worker 1 to 8 GiB and dispatches.
            primary
                .react_to_worker_signal_batch(tasks_added_batch(), &mut None)
                .await;
            settle_pump().await;

            assert_eq!(
                assigned_ids(&mut ends[0].1),
                vec!["big".to_string()],
                "the estimate-stalled task must dispatch under the node-cap boost"
            );
            // The boost is scoped: worker 1's budget is restored after the
            // pass (it is now busy with "big", so re-idling it would expose
            // the restored value — assert the slot took the task).
            assert!(
                primary.slot_holds_hash_for_test(&sec, 1, &compute_task_hash(&one_task("big"))),
                "worker 1 must hold the rescued task"
            );
        })
        .await;
}

/// REVERT-CHECK for the rescue. WITHOUT the escalation (only the normal
/// recheck), the 6 GiB task strands at the idle 4.15 GiB worker — no
/// dispatch — which is the pre-fix whole-pool-strand seed.
#[tokio::test(flavor = "current_thread")]
async fn without_escalation_node_fittable_task_strands() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut primary, mut ends, sec, _mesh) =
                primary_with_tasks(&[("big", 6 * GIB)]).await;
            make_worker_busy(&mut primary, &sec, 0);
            let _ = assigned_ids(&mut ends[0].1);

            // Drive ONLY the normal dispatch recheck (the pre-fix behaviour),
            // never the escalation. The 6 GiB task fits no idle worker's
            // budget, so nothing dispatches and it stays queued.
            primary.dispatch_to_idle_workers(true).await.ok();
            settle_pump().await;

            assert!(
                assigned_ids(&mut ends[0].1).is_empty(),
                "without escalation the node-fittable-but-oversized task strands at \
                 the smaller idle worker (the pre-fix whole-pool-strand seed)"
            );
        })
        .await;
}

/// FIX (genuinely unfittable). A task estimated 16 GiB exceeds even the
/// largest node's full 8 GiB capacity: no boost can help. The escalation
/// fails it INDIVIDUALLY as `ResourceExhausted`, so it is accounted under
/// the failed ledger and the run can reach its counter exit — instead of
/// hanging until a whole-pool strand.
#[tokio::test(flavor = "current_thread")]
async fn unfittable_task_fails_individually_as_resource_exhausted() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut primary, mut ends, sec, _mesh) =
                primary_with_tasks(&[("huge", 16 * GIB)]).await;
            make_worker_busy(&mut primary, &sec, 0);
            let _ = assigned_ids(&mut ends[0].1);

            let huge_hash = compute_task_hash(&one_task("huge"));

            primary
                .react_to_worker_signal_batch(tasks_added_batch(), &mut None)
                .await;
            settle_pump().await;

            // No dispatch — 16 GiB fits nothing, even boosted.
            assert!(
                assigned_ids(&mut ends[0].1).is_empty(),
                "an over-node-cap task must not dispatch even under the boost"
            );
            // It is failed individually as ResourceExhausted in the CRDT
            // ledger (the terminal that lets the counter exit trip).
            assert!(
                matches!(
                    primary.cluster_state_for_test().task_state(&huge_hash),
                    Some(crate::cluster_state::TaskState::Failed {
                        kind: ErrorType::ResourceExhausted(k),
                        ..
                    }) if *k == ResourceKind::memory()
                ),
                "the unfittable task must be failed as ResourceExhausted; got {:?}",
                primary.cluster_state_for_test().task_state(&huge_hash),
            );
            assert_eq!(
                primary.phase_failed_for_test(&PhaseId::from("p")),
                1,
                "the unfittable task counts as one phase failure"
            );
        })
        .await;
}

/// REVERT-CHECK for the individual fail. WITHOUT the escalation, the 16 GiB
/// task is NEVER failed — it sits queued, neither completed nor failed, so
/// the `completed + failed >= total` counter exit can never trip (the
/// whole-pool hang the fix eliminates).
#[tokio::test(flavor = "current_thread")]
async fn without_escalation_unfittable_task_is_never_accounted() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut primary, mut ends, sec, _mesh) =
                primary_with_tasks(&[("huge", 16 * GIB)]).await;
            make_worker_busy(&mut primary, &sec, 0);
            let _ = assigned_ids(&mut ends[0].1);

            let huge_hash = compute_task_hash(&one_task("huge"));

            // Normal recheck only (pre-fix): nothing fits, nothing fails.
            primary.dispatch_to_idle_workers(true).await.ok();
            settle_pump().await;

            assert!(
                assigned_ids(&mut ends[0].1).is_empty(),
                "the unfittable task cannot dispatch"
            );
            assert!(
                !matches!(
                    primary.cluster_state_for_test().task_state(&huge_hash),
                    Some(crate::cluster_state::TaskState::Failed { .. })
                ),
                "without escalation the unfittable task is never failed — the \
                 counter exit can never trip (the pre-fix whole-pool hang); got {:?}",
                primary.cluster_state_for_test().task_state(&huge_hash),
            );
            assert_eq!(
                primary.phase_failed_for_test(&PhaseId::from("p")),
                0,
                "without escalation no failure is accounted"
            );
        })
        .await;
}

/// GUARD: escalation must NOT fire when a queued task already fits a
/// per-worker budget. Worker 0 is idle (full-node budget); a 6 GiB task
/// fits it, so the NORMAL recheck dispatches it and the escalation
/// precondition (`is_estimate_stalled`) is false — no boost, no spurious
/// individual-fail. Pins that escalation never pre-empts a normal dispatch.
#[tokio::test(flavor = "current_thread")]
async fn no_escalation_when_a_worker_budget_already_fits() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Only "big" (6 GiB); worker 0 (8 GiB) is idle and fits it.
            let (mut primary, mut ends, _sec, _mesh) =
                primary_with_tasks(&[("big", 6 * GIB)]).await;

            primary
                .react_to_worker_signal_batch(tasks_added_batch(), &mut None)
                .await;
            settle_pump().await;

            // Dispatched by the NORMAL recheck to worker 0 — not a boost.
            assert_eq!(
                assigned_ids(&mut ends[0].1),
                vec!["big".to_string()],
                "a task that fits an idle worker's budget dispatches normally"
            );
            // Nothing was spuriously failed.
            assert_eq!(
                primary.phase_failed_for_test(&PhaseId::from("p")),
                0,
                "no spurious individual-fail when a normal dispatch is possible"
            );
        })
        .await;
}
