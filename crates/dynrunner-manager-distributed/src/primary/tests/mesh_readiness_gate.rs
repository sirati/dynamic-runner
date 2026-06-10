//! Half-A strand prevention: the PROACTIVE dispatch path
//! (`dispatch_to_idle_workers`) must NOT push work to a half-joined
//! member — one that already received a task but never confirmed its
//! peer-mesh leg (`MeshReady`). In run_20260610_105906 the primary
//! proactively assigned two post-Ready first-binds to `secondary-2`
//! while it was `missing` from the mesh-ready set; that member's
//! terminals then swallowed on its half-formed mesh egress leg, so the
//! tasks stranded and wedged the phase barrier.
//!
//! The gate (`should_skip_worker_for_dispatch` → `member_mesh_confirmed`)
//! withholds proactive work from such a member until a `MeshReady` lands
//! (late-join recovery), while:
//!   - a member's FIRST/bootstrap dispatch is never gated (otherwise the
//!     bring-up recovery deadlocks — the assigned=0 hang), and
//!   - the REACTIVE path (`handle_task_request`) is never gated: a request
//!     that arrived is its own proof the member's uplink delivers (the
//!     strand member could not send requests — they swallowed too).
//!
//! These are deterministic direct-handler tests: drive the exact
//! dispatch-shape at the member and assert what reaches its wire.

use super::*;

use dynrunner_core::{PhaseId, ResourceAmount, ResourceKind, ResourceMap, TypeId};

use crate::primary::wire::compute_task_hash;
use crate::worker_signal::{WorkerMgmtSignal, drain_worker_signal_batch};

type TestPrimary = PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>;

/// A single zero-dep task in phase "p", type "default".
fn one_task(name: &str) -> TaskInfo<TestId> {
    let mut t = make_binary(name, 50);
    t.phase_id = PhaseId::from("p");
    t.type_id = TypeId::from("default");
    t
}

/// A `TaskRequest` from `(secondary, worker)`.
fn task_request(secondary: &str, worker: u32) -> DistributedMessage<TestId> {
    DistributedMessage::TaskRequest {
        target: None,
        sender_id: secondary.into(),
        timestamp: 0.0,
        secondary_id: secondary.into(),
        worker_id: worker,
        available_resources: vec![ResourceAmount {
            kind: ResourceKind::memory(),
            amount: 1024 * 1024 * 1024,
        }],
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

/// Run one `TasksAdded` recheck (the operational-loop worker-management
/// arm's body) so `dispatch_to_idle_workers` re-evaluates every free
/// worker against the live pool.
async fn run_dispatch_recheck(primary: &mut TestPrimary) {
    let (wm_tx, mut wm_rx) = tokio_mpsc::unbounded_channel::<WorkerMgmtSignal>();
    primary
        .cluster_state_mut_for_test()
        .install_worker_mgmt_sender(wm_tx);
    primary
        .cluster_state_mut_for_test()
        .emit_worker_mgmt(WorkerMgmtSignal::TasksAdded);
    let batch = drain_worker_signal_batch(&mut wm_rx, Duration::from_millis(50))
        .await
        .expect("emit must produce a batch");
    primary.react_to_worker_signal_batch(batch).await;
}

/// THE strand-prevention pin. Two members each with one idle worker:
/// `sec-good` is mesh-confirmed, `sec-half` is the half-joined strand
/// member (it already got work but never sent `MeshReady`). One ready
/// task is in the pool.
///
/// A proactive `TasksAdded` recheck (`dispatch_to_idle_workers`) must
/// route the task to `sec-good` and NEVER to `sec-half` — the production
/// bypass that stranded `secondary-2`. Then a LATE `MeshReady` for
/// `sec-half` recovers it: a second task in the pool now flows to it.
#[tokio::test(flavor = "current_thread")]
async fn proactive_dispatch_skips_half_joined_member_until_mesh_ready_lands() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Two secondaries, one wire end each (ends[0]=sec-good, ends[1]=sec-half).
            let (transport, mut ends) = setup_test(2);
            let (mut primary, _mesh) = build_test_primary(
                test_primary_config(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            // The wire ids `setup_test` minted, in registration order.
            let good_id = ends[0].0.clone();
            let half_id = ends[1].0.clone();

            // Two ready tasks seeded up front: `t0` is dispatched in the
            // first recheck (to `sec-good` only, since `sec-half` is gated);
            // `t1` stays queued until `sec-half` recovers. (Seeded together
            // so the roster — registered below via the test seam, NOT via a
            // `SecondaryCapacity` record — survives: a second
            // `hydrate_from_cluster_state` would rebuild the roster off the
            // CRDT and drop these seam-registered workers.)
            let t0 = one_task("t0");
            let t1 = one_task("t1");
            let hash_t0 = compute_task_hash(&t0);
            let hash_t1 = compute_task_hash(&t1);
            {
                let cs = primary.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::PhaseDepsSet {
                    deps: HashMap::from([(PhaseId::from("p"), vec![])]),
                });
                cs.apply(ClusterMutation::TaskAdded {
                    hash: hash_t0.clone(),
                    task: t0,
                });
                cs.apply(ClusterMutation::TaskAdded {
                    hash: hash_t1.clone(),
                    task: t1,
                });
            }
            primary.hydrate_from_cluster_state();

            let budget = ResourceMap::from([(ResourceKind::memory(), 1024 * 1024 * 1024u64)]);
            // `register_idle_worker_for_test` marks BOTH members mesh-confirmed
            // (it models a fully-operational member).
            primary.register_idle_worker_for_test(good_id.clone(), 0, budget.clone());
            primary.register_idle_worker_for_test(half_id.clone(), 0, budget);

            // Make `sec-half` the half-joined strand member: it already
            // received work (mark dispatched-to so the first-dispatch
            // exemption does NOT apply) but its `MeshReady` never landed
            // (drop it from the confirmation set).
            primary.confirm_member_mesh_for_test(&good_id);
            primary.mark_member_mesh_unconfirmed_for_test(&half_id);
            primary.mark_member_dispatched_for_test(&half_id);

            // PROACTIVE recheck: exactly ONE task goes to `sec-good`
            // (one idle worker), and NOTHING to the half-joined `sec-half`.
            // The scheduler may pick either of the two same-phase tasks, so
            // the per-member assertions are which-task-agnostic.
            run_dispatch_recheck(&mut primary).await;
            settle_pump().await;

            let good_got = assigned_ids(&mut ends[0].1);
            assert_eq!(
                good_got.len(),
                1,
                "exactly one task must dispatch to the mesh-confirmed member's \
                 single idle worker; got {good_got:?}"
            );
            let first_task = good_got[0].clone();
            assert!(
                first_task == "t0" || first_task == "t1",
                "the dispatched task is one of the two seeded; got {first_task}"
            );
            assert!(
                assigned_ids(&mut ends[1].1).is_empty(),
                "NO task may be pushed to the half-joined member — its terminals \
                 would swallow on its half-formed egress leg (the production strand)"
            );
            assert!(
                primary.slot_is_idle_for_test(&half_id, 0),
                "the half-joined member's worker stays idle: dispatch withheld it"
            );
            // The OTHER task is still queued — the gate held it back from the
            // only remaining idle worker (`sec-half`).
            let queued_task = if first_task == "t0" { "t1" } else { "t0" };

            // LATE-JOIN RECOVERY. Deliver the member's `MeshReady` — it must
            // now become assignable and the proactive recheck flows the
            // queued task to it.
            primary.handle_mesh_ready(DistributedMessage::MeshReady {
                target: None,
                sender_id: half_id.clone(),
                timestamp: 0.0,
                secondary_id: half_id.clone(),
                peer_count: 1,
            });

            run_dispatch_recheck(&mut primary).await;
            settle_pump().await;

            assert_eq!(
                assigned_ids(&mut ends[1].1),
                vec![queued_task.to_string()],
                "after its MeshReady lands, the recovered member must receive the \
                 queued task (late join must recover)"
            );
        })
        .await;
}

/// First/bootstrap dispatch is NEVER gated: a member that has NOT yet
/// received any task is assignable even without `MeshReady` — that very
/// dispatch is what drives it operational so it CAN emit `MeshReady`.
/// Gating it would re-create the assigned=0 bring-up deadlock.
#[tokio::test(flavor = "current_thread")]
async fn first_dispatch_to_unconfirmed_member_is_not_gated() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, mut ends) = setup_test(1);
            let (mut primary, _mesh) = build_test_primary(
                test_primary_config(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            let id = ends[0].0.clone();

            let t0 = one_task("boot");
            let hash = compute_task_hash(&t0);
            {
                let cs = primary.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::PhaseDepsSet {
                    deps: HashMap::from([(PhaseId::from("p"), vec![])]),
                });
                cs.apply(ClusterMutation::TaskAdded {
                    hash: hash.clone(),
                    task: t0,
                });
            }
            primary.hydrate_from_cluster_state();
            let budget = ResourceMap::from([(ResourceKind::memory(), 1024 * 1024 * 1024u64)]);
            primary.register_idle_worker_for_test(id.clone(), 0, budget);

            // Unconfirmed AND never-dispatched: a true late-joiner awaiting
            // its bring-up push.
            primary.mark_member_mesh_unconfirmed_for_test(&id);

            run_dispatch_recheck(&mut primary).await;
            settle_pump().await;

            assert_eq!(
                assigned_ids(&mut ends[0].1),
                vec!["boot".to_string()],
                "a member's first/bootstrap dispatch must NOT be gated on MeshReady — \
                 it is what drives the member operational so it can emit MeshReady"
            );
        })
        .await;
}

/// The REACTIVE path is never gated: a `TaskRequest` that ARRIVED is its
/// own proof the member's uplink delivers, so honouring it can never
/// strand on an unreachable member — even if the member never sent
/// `MeshReady` and already had work.
#[tokio::test(flavor = "current_thread")]
async fn reactive_task_request_is_honored_even_when_member_unconfirmed() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, mut ends) = setup_test(1);
            let (mut primary, _mesh) = build_test_primary(
                test_primary_config(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            let id = ends[0].0.clone();

            let t0 = one_task("reactive");
            let hash = compute_task_hash(&t0);
            {
                let cs = primary.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::PhaseDepsSet {
                    deps: HashMap::from([(PhaseId::from("p"), vec![])]),
                });
                cs.apply(ClusterMutation::TaskAdded {
                    hash: hash.clone(),
                    task: t0,
                });
            }
            primary.hydrate_from_cluster_state();
            let budget = ResourceMap::from([(ResourceKind::memory(), 1024 * 1024 * 1024u64)]);
            primary.register_idle_worker_for_test(id.clone(), 0, budget);

            // Half-joined shape: already dispatched-to + unconfirmed. The
            // proactive gate WOULD withhold work, but a request it sends
            // (which demonstrably reached us) is honoured anyway.
            primary.mark_member_mesh_unconfirmed_for_test(&id);
            primary.mark_member_dispatched_for_test(&id);

            primary
                .handle_task_request(task_request(&id, 0))
                .await
                .unwrap();
            settle_pump().await;

            assert_eq!(
                assigned_ids(&mut ends[0].1),
                vec!["reactive".to_string()],
                "a TaskRequest that arrived is proof of a working uplink — the reactive \
                 path is never withheld by the mesh-confirmation gate"
            );
        })
        .await;
}
