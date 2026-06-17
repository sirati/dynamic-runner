//! Eager-prep idle-filler dispatch integration tests (#638), synchronous +
//! deterministic — the same harness shape the affine dispatch tests use
//! (`handle_cluster_mutation` + `handle_mesh_ready` to confirm a worker without
//! the in-process mesh watchdog, `react_to_worker_signal_batch` to run the
//! recheck, `handle_task_complete`/`handle_task_failed` to feed a terminal).
//!
//! They validate the model-A behaviour: the filler fires ONLY as the LAST
//! dispatch resort (after the global pool view + affine pop + affine steal all
//! decline), claims the per-secondary cell `Queued`, dispatches the prep, and
//! the worker terminal writes the cell `Done`/`Failed` through the shared
//! kind-blind per-secondary cell terminal path. A phase transition does NOT
//! wait on a live eager-prep cell (phase-agnostic, uncounted).

use super::*;

use dynrunner_core::{PhaseId, ResourceAmount, ResourceKind, TaskKind, TypeId};
use dynrunner_protocol_primary_secondary::SecondaryCell;

use crate::primary::wire::compute_task_hash;
use crate::worker_signal::WorkerMgmtSignal;

type TestPrimary = PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>;

fn mem(bytes: u64) -> Vec<ResourceAmount> {
    vec![ResourceAmount {
        kind: ResourceKind::memory(),
        amount: bytes,
    }]
}

/// A `SecondaryEagerPrep` task (no deps), phase "work".
fn eager_prep(name: &str) -> TaskInfo<TestId> {
    let mut t = make_binary(name, 10);
    t.phase_id = PhaseId::from("work");
    t.type_id = TypeId::from("default");
    t.kind = TaskKind::SecondaryEagerPrep;
    t
}

/// An ordinary `Work` task, phase "work".
fn work(name: &str) -> TaskInfo<TestId> {
    let mut t = make_binary(name, 20);
    t.phase_id = PhaseId::from("work");
    t.type_id = TypeId::from("default");
    t
}

fn capacity_batch(secondary: &str, n: u32) -> DistributedMessage<TestId> {
    DistributedMessage::ClusterMutation {
        target: None,
        sender_id: "setup".into(),
        timestamp: 0.0,
        mutations: vec![
            ClusterMutation::PeerJoined {
                peer_id: secondary.into(),
                is_observer: false,
                can_be_primary: true,
                cap_version: Default::default(),
                member_gen: 0,
            },
            ClusterMutation::SecondaryCapacity {
                secondary: secondary.into(),
                worker_count: n,
                resources: mem(8 * 1024 * 1024 * 1024),
            },
        ],
    }
}

fn mesh_ready_from(secondary_id: &str) -> DistributedMessage<TestId> {
    DistributedMessage::MeshReady {
        target: None,
        sender_id: secondary_id.into(),
        timestamp: 0.0,
        secondary_id: secondary_id.into(),
        peer_count: 1,
    }
}

fn task_complete(secondary: &str, worker: u32, task_hash: &str) -> DistributedMessage<TestId> {
    DistributedMessage::TaskComplete {
        target: None,
        sender_id: secondary.into(),
        timestamp: 0.0,
        secondary_id: secondary.into(),
        worker_id: worker,
        task_hash: task_hash.into(),
        result_data: None,
        delivery_seq: None,
        msgs_posted_through: None,
    }
}

fn task_failed(secondary: &str, worker: u32, task_hash: &str) -> DistributedMessage<TestId> {
    DistributedMessage::TaskFailed {
        target: None,
        sender_id: secondary.into(),
        timestamp: 0.0,
        secondary_id: secondary.into(),
        worker_id: worker,
        task_hash: task_hash.into(),
        error_type: dynrunner_core::ErrorType::NonRecoverable,
        error_message: "eager prep failed".into(),
        delivery_seq: None,
        msgs_posted_through: None,
    }
}

fn assignments(
    rx: &mut tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
) -> Vec<(String, String, u32, String)> {
    let mut out = Vec::new();
    while let Ok(msg) = rx.try_recv() {
        if let DistributedMessage::TaskAssignment {
            binary_info,
            secondary_id,
            worker_id,
            file_hash,
            ..
        } = msg
        {
            out.push((binary_info.task_id, secondary_id, worker_id, file_hash));
        }
    }
    out
}

/// Build a 1-secondary primary (1 worker, mesh-confirmed) whose CRDT holds
/// `binaries`, registering a cell-id for EVERY cell-bearing def (affine +
/// eager-prep) — exactly what `inject_cell_registrations` does on the live
/// origination path, applied directly here.
#[allow(clippy::type_complexity)]
fn primary_one_secondary_with(
    binaries: Vec<TaskInfo<TestId>>,
) -> (
    TestPrimary,
    Vec<(
        String,
        tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
        tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
    )>,
    tokio_mpsc::UnboundedReceiver<WorkerMgmtSignal>,
    PrimaryMeshKeepalive,
) {
    let (transport, ends) = setup_test(1);
    let config = PrimaryConfig {
        num_secondaries: 1,
        ..test_primary_config()
    };
    let (mut primary, mesh) = build_test_primary(
        config,
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    {
        let cs = primary.cluster_state_mut_for_test();
        cs.apply(ClusterMutation::PhaseDepsSet {
            deps: HashMap::from([(PhaseId::from("work"), vec![])]),
        });
        for task in &binaries {
            let hash = compute_task_hash(task);
            cs.apply(ClusterMutation::TaskAdded {
                hash: hash.clone(),
                task: task.clone(),
                def_id: None,
            });
            if task.kind.has_secondary_cell() {
                let cell_id = cs.allocate_cell_id(&hash).0;
                cs.apply(ClusterMutation::SecondaryCellRegistered { hash, cell_id });
            }
        }
    }
    primary
        .hydrate_from_cluster_state()
        .expect("composed graph is valid");
    let (wm_tx, wm_rx) = tokio_mpsc::unbounded_channel::<WorkerMgmtSignal>();
    primary
        .cluster_state_mut_for_test()
        .install_worker_mgmt_sender(wm_tx);
    (primary, ends, wm_rx, mesh)
}

async fn confirm_one(primary: &mut TestPrimary) {
    primary
        .handle_cluster_mutation(capacity_batch("sec-0", 1), &mut None)
        .await;
    primary.handle_mesh_ready(mesh_ready_from("sec-0"));
}

async fn drain_rechecks(
    primary: &mut TestPrimary,
    wm_rx: &mut tokio_mpsc::UnboundedReceiver<WorkerMgmtSignal>,
) {
    while let Some(batch) = crate::worker_signal::try_collect_worker_signal_batch(wm_rx) {
        primary.react_to_worker_signal_batch(batch, &mut None).await;
        settle_pump().await;
    }
}

/// The eager-prep cell-id of the named prep task.
fn cell_of(primary: &TestPrimary, prep_hash: &str) -> crate::cluster_state::SecondaryCellId {
    primary
        .cluster_state_for_test()
        .affine_id_for_hash(prep_hash)
        .expect("registered eager-prep cell-id")
}

/// (a) The filler fires ONLY when the worker has nothing else: with an
/// eager-prep task and NO pending work, an idle worker speculatively runs the
/// prep, claims the cell `Queued`, and dispatches it.
#[tokio::test(flavor = "current_thread")]
async fn eager_prep_fills_idle_worker_when_no_other_work() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let prep = eager_prep("prep");
            let prep_hash = compute_task_hash(&prep);
            let (mut primary, mut ends, mut wm_rx, _mesh) =
                primary_one_secondary_with(vec![prep]);
            confirm_one(&mut primary).await;
            let cid = cell_of(&primary, &prep_hash);

            drain_rechecks(&mut primary, &mut wm_rx).await;

            let dispatched: Vec<_> = assignments(&mut ends[0].1);
            assert!(
                dispatched.iter().any(|(_, sec, _, h)| sec == "sec-0" && *h == prep_hash),
                "the idle worker should speculatively dispatch the eager-prep task; got {dispatched:?}"
            );
            // The cell was claimed Queued on the dispatching secondary.
            assert_eq!(
                primary.cluster_state_for_test().affine_state("sec-0", cid),
                SecondaryCell::Queued,
                "the eager-prep cell must be claimed Queued at dispatch"
            );
        })
        .await;
}

/// (a, negative) With a normal `Work` task ALSO present, the global pool view is
/// non-empty, so the filler is NEVER reached — the worker dispatches the real
/// work, and the eager-prep cell stays NotDone (untouched) that tick.
#[tokio::test(flavor = "current_thread")]
async fn eager_prep_does_not_fire_while_real_work_is_dispatchable() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let prep = eager_prep("prep");
            let prep_hash = compute_task_hash(&prep);
            let job = work("job");
            let job_hash = compute_task_hash(&job);
            let (mut primary, mut ends, mut wm_rx, _mesh) =
                primary_one_secondary_with(vec![prep, job]);
            confirm_one(&mut primary).await;
            let cid = cell_of(&primary, &prep_hash);

            drain_rechecks(&mut primary, &mut wm_rx).await;

            let dispatched: Vec<_> = assignments(&mut ends[0].1);
            // The single worker took the REAL work, not the prep.
            assert!(
                dispatched.iter().any(|(_, _, _, h)| *h == job_hash),
                "the real work must dispatch first; got {dispatched:?}"
            );
            assert!(
                !dispatched.iter().any(|(_, _, _, h)| *h == prep_hash),
                "the eager-prep filler must NOT fire while real work is dispatchable; got {dispatched:?}"
            );
            assert_eq!(
                primary.cluster_state_for_test().affine_state("sec-0", cid),
                SecondaryCell::NotDone,
                "the eager-prep cell must be untouched while the worker has real work"
            );
        })
        .await;
}

/// (c) A worker COMPLETE for the prep writes the cell `Done` (the run-once
/// authority) through the shared kind-blind per-secondary cell terminal path; a
/// FAIL writes the cell `Failed`.
#[tokio::test(flavor = "current_thread")]
async fn eager_prep_terminal_writes_the_cell() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Two distinct prep tasks so we can drive one to Done and one to
            // Failed independently on the same secondary.
            let prep_ok = eager_prep("prep_ok");
            let prep_fail = eager_prep("prep_fail");
            let ok_hash = compute_task_hash(&prep_ok);
            let fail_hash = compute_task_hash(&prep_fail);
            let (mut primary, mut ends, mut wm_rx, _mesh) =
                primary_one_secondary_with(vec![prep_ok, prep_fail]);
            confirm_one(&mut primary).await;
            let ok_cid = cell_of(&primary, &ok_hash);
            let fail_cid = cell_of(&primary, &fail_hash);

            // Drive dispatch + terminals until both cells are terminal. Each
            // tick the single worker fills with one prep; complete/fail it by
            // hash, then re-drain so the freed worker fills the other.
            for _ in 0..8 {
                drain_rechecks(&mut primary, &mut wm_rx).await;
                let round = assignments(&mut ends[0].1);
                if round.is_empty() {
                    break;
                }
                for (_, sec, worker, h) in &round {
                    if *h == ok_hash {
                        primary
                            .handle_task_complete(task_complete(sec, *worker, h), &mut None)
                            .await;
                    } else if *h == fail_hash {
                        primary
                            .handle_task_failed(task_failed(sec, *worker, h), &mut None)
                            .await;
                    }
                    settle_pump().await;
                }
            }

            assert_eq!(
                primary.cluster_state_for_test().affine_state("sec-0", ok_cid),
                SecondaryCell::Done,
                "a COMPLETE terminal must write the eager-prep cell Done"
            );
            assert_eq!(
                primary.cluster_state_for_test().affine_state("sec-0", fail_cid),
                SecondaryCell::Failed,
                "a FAIL terminal must write the eager-prep cell Failed"
            );
        })
        .await;
}

/// (d) The run-once / dispatch-once guard: once the prep cell is `Done` on a
/// secondary, the filler NEVER re-dispatches it there (it is excluded from the
/// non-terminal candidate set), so the prep runs at most once-per-secondary.
#[tokio::test(flavor = "current_thread")]
async fn eager_prep_runs_at_most_once_per_secondary() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let prep = eager_prep("prep");
            let prep_hash = compute_task_hash(&prep);
            let (mut primary, mut ends, mut wm_rx, _mesh) =
                primary_one_secondary_with(vec![prep]);
            confirm_one(&mut primary).await;
            let cid = cell_of(&primary, &prep_hash);

            // First tick: filler dispatches the prep, complete it → cell Done.
            drain_rechecks(&mut primary, &mut wm_rx).await;
            let first = assignments(&mut ends[0].1);
            assert_eq!(first.len(), 1, "exactly one prep dispatched; got {first:?}");
            let (_, sec, worker, h) = &first[0];
            assert_eq!(*h, prep_hash);
            primary
                .handle_task_complete(task_complete(sec, *worker, h), &mut None)
                .await;
            settle_pump().await;
            assert_eq!(
                primary.cluster_state_for_test().affine_state("sec-0", cid),
                SecondaryCell::Done,
            );

            // Subsequent ticks: the cell is Done here, so the filler finds NO
            // non-terminal candidate and dispatches NOTHING more.
            for _ in 0..3 {
                drain_rechecks(&mut primary, &mut wm_rx).await;
            }
            let later = assignments(&mut ends[0].1);
            assert!(
                later.is_empty(),
                "the prep must NOT re-dispatch once its cell is Done (run-once); got {later:?}"
            );
        })
        .await;
}
