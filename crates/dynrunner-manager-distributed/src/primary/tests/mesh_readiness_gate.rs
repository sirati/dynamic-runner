//! Dispatch ⊥ peer mesh: PROACTIVE dispatch
//! (`dispatch_to_idle_workers`) flows to an idle worker regardless of
//! whether its member ever reported `MeshReady`. The peer mesh
//! (secondary↔secondary, counted by `MeshReady`'s `peer_count`) is the
//! FAILOVER substrate only; dispatch + terminals ride the independent
//! primary↔secondary leg, and a terminal that ever rode a half-formed
//! leg self-heals via the secondary's confirmable-replay
//! (`send_to_primary`: retained `AwaitingAck`, replayed with the same
//! `delivery_seq` until a `TerminalAck` lands). So the historic #360
//! per-dispatch mesh-confirmation VETO is gone — it was pure
//! over-conservatism, and removing it cannot re-strand. Mesh formation is
//! now enforced ONCE as a background run-abort deadline
//! (`mesh_formation_missing` in the operational loop), never a
//! per-dispatch gate.
//!
//! These tests pin the decoupling: a member that never reported
//! `MeshReady` still receives proactive AND reactive work. They are
//! deterministic direct-handler tests: drive the exact dispatch-shape at
//! the member and assert what reaches its wire.

use super::*;

use dynrunner_core::{PhaseId, ResourceAmount, ResourceKind, ResourceMap, TypeId};

use crate::primary::wire::compute_task_hash;
use crate::worker_signal::{WorkerMgmtSignal, recv_worker_signal_batch};

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

/// Drain every `TaskAssignment` `task_id` from a CO-LOCATED secondary's
/// loopback inbox (the [`crate::process::RoleInbox`] minted by registering
/// `LocalRole::Secondary` on the primary's mesh). Production-faithful
/// counterpart to [`assigned_ids`]: in production a primary that runs on
/// the same OS process as one of its members delivers that member's
/// assignments via the mesh loopback (`deliver_local(LocalRole::Secondary)`),
/// not the wire — so the test fixture reads from the local inbox here,
/// not the channel transport's `outgoing` slot. Underpins the #551
/// at-least-once contract: a queued frame whose loopback target's slot
/// raced the apply is retained, not silently dropped.
fn assigned_ids_inbox(
    inbox: &mut crate::process::RoleInbox<TestId>,
) -> Vec<String> {
    let mut ids = Vec::new();
    while let Some(msg) = inbox.try_recv() {
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
    let batch = recv_worker_signal_batch(&mut wm_rx)
        .await
        .expect("emit must produce a batch");
    primary.react_to_worker_signal_batch(batch, &mut None).await;
}

/// THE decoupling pin. Two members each with one idle worker:
/// `sec-confirmed` reported `MeshReady`, `sec-unconfirmed` never did.
/// Two ready tasks are in the pool. A proactive `TasksAdded` recheck
/// (`dispatch_to_idle_workers`) must fill BOTH idle workers — one task to
/// each — including the member that never reported `MeshReady`. Dispatch
/// does not gate on the peer mesh; a terminal that rides a half-formed
/// leg self-heals via confirmable-replay, so there is nothing to withhold.
#[tokio::test(flavor = "current_thread")]
async fn proactive_dispatch_flows_to_member_without_mesh_ready() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Two secondaries, one wire end each.
            let (transport, mut ends) = setup_test(2);
            let (mut primary, _mesh) = build_test_primary(
                test_primary_config(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            let confirmed_id = ends[0].0.clone();
            let unconfirmed_id = ends[1].0.clone();

            // Two ready tasks seeded up front: one for each idle worker.
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
                    def_id: None,
                });
                cs.apply(ClusterMutation::TaskAdded {
                    hash: hash_t1.clone(),
                    task: t1,
                    def_id: None,
                });
            }
            primary.hydrate_from_cluster_state()
        .expect("test fixture: composed task graph is valid");

            let budget = ResourceMap::from([(ResourceKind::memory(), 1024 * 1024 * 1024u64)]);
            primary.register_idle_worker_for_test(confirmed_id.clone(), 0, budget.clone());
            primary.register_idle_worker_for_test(unconfirmed_id.clone(), 0, budget);

            // One member reported a formed mesh, the other never did — the
            // peer-mesh signal must NOT change dispatch either way.
            primary.confirm_member_mesh_for_test(&confirmed_id);
            primary.mark_member_mesh_unconfirmed_for_test(&unconfirmed_id);

            // PROACTIVE recheck: BOTH idle workers take a task — the
            // unconfirmed member is dispatched to just like the confirmed one.
            run_dispatch_recheck(&mut primary).await;
            settle_pump().await;

            let confirmed_got = assigned_ids(&mut ends[0].1);
            let unconfirmed_got = assigned_ids(&mut ends[1].1);
            assert_eq!(
                confirmed_got.len(),
                1,
                "the confirmed member's idle worker takes one task; got {confirmed_got:?}"
            );
            assert_eq!(
                unconfirmed_got.len(),
                1,
                "the member that never reported MeshReady ALSO takes one task — \
                 dispatch is decoupled from the peer mesh; got {unconfirmed_got:?}"
            );
            // Both seeded tasks dispatched; nothing withheld.
            let mut all: Vec<String> = confirmed_got.into_iter().chain(unconfirmed_got).collect();
            all.sort();
            assert_eq!(
                all,
                vec!["t0".to_string(), "t1".to_string()],
                "both ready tasks dispatched across the two idle workers"
            );
            assert_eq!(
                primary.pool().iter().count(),
                0,
                "no task may sit queued while an idle worker exists — even an \
                 unconfirmed member's"
            );
        })
        .await;
}

/// The REACTIVE path is never gated: a `TaskRequest` that ARRIVED is its
/// own proof the member's uplink delivers, so honouring it can never
/// strand on an unreachable member — even if the member never sent
/// `MeshReady`.
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
                    def_id: None,
                });
            }
            primary.hydrate_from_cluster_state()
        .expect("test fixture: composed task graph is valid");
            let budget = ResourceMap::from([(ResourceKind::memory(), 1024 * 1024 * 1024u64)]);
            primary.register_idle_worker_for_test(id.clone(), 0, budget);

            // Never reported MeshReady — irrelevant to dispatch now, but
            // pin that the reactive (pull) path serves it regardless.
            primary.mark_member_mesh_unconfirmed_for_test(&id);

            primary
                .handle_task_request(task_request(&id, 0), &mut None)
                .await
                .unwrap();
            settle_pump().await;

            assert_eq!(
                assigned_ids(&mut ends[0].1),
                vec!["reactive".to_string()],
                "the reactive path serves a TaskRequest regardless of MeshReady \
                 (dispatch ⊥ peer mesh)"
            );
        })
        .await;
}

/// First dispatch to a member that never reported `MeshReady` FLOWS — the
/// historic #360 first-dispatch withholding is gone. The peer mesh is the
/// failover substrate only; a task's terminal rides the independent
/// primary↔secondary leg and self-heals via confirmable-replay even if
/// that leg was momentarily half-formed, so there is nothing to withhold.
/// A proactive recheck must push the ready task to the member's idle
/// worker with no `MeshReady` round-trip.
#[tokio::test(flavor = "current_thread")]
async fn first_dispatch_to_member_without_mesh_ready_flows() {
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
                    def_id: None,
                });
            }
            primary.hydrate_from_cluster_state()
        .expect("test fixture: composed task graph is valid");
            let budget = ResourceMap::from([(ResourceKind::memory(), 1024 * 1024 * 1024u64)]);
            primary.register_idle_worker_for_test(id.clone(), 0, budget);

            // Never dispatched to, never reported MeshReady — the member
            // the deleted #360 veto would have withheld.
            primary.mark_member_mesh_unconfirmed_for_test(&id);

            run_dispatch_recheck(&mut primary).await;
            settle_pump().await;

            assert_eq!(
                assigned_ids(&mut ends[0].1),
                vec!["boot".to_string()],
                "the first dispatch to a member without MeshReady FLOWS — \
                 dispatch is decoupled from the peer mesh, and a terminal that \
                 rides a half-formed leg self-heals via confirmable-replay"
            );
            assert!(
                !primary.slot_is_idle_for_test(&id, 0),
                "the member's worker is now busy with the dispatched task"
            );
        })
        .await;
}

/// Lone-survivor / co-located member (run_20260612_035452): the member
/// whose peer-id IS this primary's host dispatches without any `MeshReady`
/// — trivially true now that dispatch is decoupled from the peer mesh, but
/// pinned here on the lone-survivor self-quorum shape that the old gate
/// once vetoed until its co-located secondary's loopbacked report landed.
///
/// Production shape: a bootstrap handoff promoted `secondary-1` into a
/// fleet whose ONLY live member was its own host. The promoted
/// primary's confirmation set starts empty, the self-MeshReady only
/// lands once the co-located secondary finishes consuming the setup
/// trio, and until then every proactive recheck vetoed the ONLY
/// dispatchable workers ("member remains unassignable until its mesh
/// leg confirms").
#[tokio::test(flavor = "current_thread")]
async fn co_located_member_is_assignable_without_mesh_ready() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, ends) = setup_test(1);
            // The promoted-primary shape: this primary RUNS ON the
            // member's host — node_id == the member's peer-id.
            let (mut primary, keepalive) = build_test_primary(
                PrimaryConfig {
                    node_id: ends[0].0.clone(),
                    ..test_primary_config()
                },
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            let own_id = ends[0].0.clone();
            // Production-faithful: a primary co-located with its
            // secondary shares ONE mesh; the secondary slot must be
            // registered so the primary's `Destination::Secondary(own_id)`
            // dispatch finds it via `deliver_local(LocalRole::Secondary)`.
            // Pre-#551 the fixture relied on the channel transport's
            // wire-fallthrough (the dispatch's `is_local_host && !deliver_local`
            // arm bounced to `transport.send_to_peer`); post-#551 the
            // dispatch retains-for-resolution and the loopback inbox is
            // the production-faithful capture point. Drop the secondary
            // role's `Arc` immediately — the inbox + the pump's `Weak`
            // keep the channel alive for delivery.
            let (_sec_slot, _sec_client, mut sec_inbox) = keepalive
                .control()
                .expect("co-located fixture runs on the async/pump path")
                .register(
                    crate::process::LocalRole::Secondary,
                    dynrunner_protocol_primary_secondary::address::PeerId::from(own_id.as_str()),
                )
                .await
                .expect("Secondary registration via mesh-pump control");

            let t0 = one_task("lone0");
            let t1 = one_task("lone1");
            {
                let cs = primary.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::PhaseDepsSet {
                    deps: HashMap::from([(PhaseId::from("p"), vec![])]),
                });
                cs.apply(ClusterMutation::TaskAdded {
                    hash: compute_task_hash(&t0),
                    task: t0,
                    def_id: None,
                });
                cs.apply(ClusterMutation::TaskAdded {
                    hash: compute_task_hash(&t1),
                    task: t1,
                    def_id: None,
                });
            }
            primary
                .hydrate_from_cluster_state()
                .expect("test fixture: composed task graph is valid");
            let budget = ResourceMap::from([(ResourceKind::memory(), 1024 * 1024 * 1024u64)]);
            primary.register_idle_worker_for_test(own_id.clone(), 0, budget.clone());
            primary.register_idle_worker_for_test(own_id.clone(), 1, budget);

            // The production promoted-primary state: NO MeshReady has
            // landed from anyone — the confirmation set is empty.
            primary.mark_member_mesh_unconfirmed_for_test(&own_id);

            run_dispatch_recheck(&mut primary).await;
            settle_pump().await;

            // Drain the co-located secondary's LOOPBACK inbox (production-
            // faithful) rather than the wire end of `ends[0]` — see
            // `assigned_ids_inbox`.
            let mut got = assigned_ids_inbox(&mut sec_inbox);
            got.sort();
            assert_eq!(
                got,
                vec!["lone0".to_string(), "lone1".to_string()],
                "the co-located member's workers must be assignable WITHOUT a \
                 MeshReady round-trip — its mesh leg to the primary is the \
                 in-process loopback (the lone-survivor self-quorum path must \
                 actually dispatch)"
            );
            assert_eq!(
                primary.pool().iter().count(),
                0,
                "no task may sit queued while the co-located member's workers idle"
            );
            // Silence unused-variable warning for the wire-end of the
            // co-located member (we now capture via the loopback inbox).
            let _ = ends;
            let _ = keepalive;
        })
        .await;
}

/// In a MULTI-member fleet, BOTH the co-located AND the REMOTE member
/// receive proactive work without any `MeshReady` — dispatch is uniform
/// and decoupled from the peer mesh (there is no remote-only gate to
/// "leak" or "not leak"). The remote's terminal self-heals via
/// confirmable-replay even if its leg is momentarily half-formed.
#[tokio::test(flavor = "current_thread")]
async fn both_co_located_and_remote_members_dispatch_without_mesh_ready() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, mut ends) = setup_test(2);
            let (mut primary, keepalive) = build_test_primary(
                PrimaryConfig {
                    node_id: ends[0].0.clone(),
                    ..test_primary_config()
                },
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            let own_id = ends[0].0.clone();
            let remote_id = ends[1].0.clone();
            // Production-faithful co-located Secondary registration: see
            // the sibling test `co_located_member_is_assignable_without_mesh_ready`
            // for the #551 reasoning. The remote member's leg stays on
            // the wire (it is a true remote — `is_local_host` is false
            // there — and continues to land at `ends[1].1`).
            let (_sec_slot, _sec_client, mut sec_inbox) = keepalive
                .control()
                .expect("co-located fixture runs on the async/pump path")
                .register(
                    crate::process::LocalRole::Secondary,
                    dynrunner_protocol_primary_secondary::address::PeerId::from(own_id.as_str()),
                )
                .await
                .expect("Secondary registration via mesh-pump control");

            let t0 = one_task("m0");
            let t1 = one_task("m1");
            {
                let cs = primary.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::PhaseDepsSet {
                    deps: HashMap::from([(PhaseId::from("p"), vec![])]),
                });
                cs.apply(ClusterMutation::TaskAdded {
                    hash: compute_task_hash(&t0),
                    task: t0,
                    def_id: None,
                });
                cs.apply(ClusterMutation::TaskAdded {
                    hash: compute_task_hash(&t1),
                    task: t1,
                    def_id: None,
                });
            }
            primary
                .hydrate_from_cluster_state()
                .expect("test fixture: composed task graph is valid");
            let budget = ResourceMap::from([(ResourceKind::memory(), 1024 * 1024 * 1024u64)]);
            primary.register_idle_worker_for_test(own_id.clone(), 0, budget.clone());
            primary.register_idle_worker_for_test(remote_id.clone(), 0, budget);

            // Neither member has a landed MeshReady.
            primary.mark_member_mesh_unconfirmed_for_test(&own_id);
            primary.mark_member_mesh_unconfirmed_for_test(&remote_id);

            run_dispatch_recheck(&mut primary).await;
            settle_pump().await;

            // CO-LOCATED secondary reads via its LOOPBACK inbox (the
            // production-faithful path post-#551); the REMOTE member's
            // leg stays on the wire at `ends[1].1`.
            let own_got = assigned_ids_inbox(&mut sec_inbox);
            assert_eq!(
                own_got.len(),
                1,
                "the co-located member's single idle worker takes one task; \
                 got {own_got:?}"
            );
            let remote_got = assigned_ids(&mut ends[1].1);
            assert_eq!(
                remote_got.len(),
                1,
                "the REMOTE member — which never reported MeshReady — ALSO \
                 takes a task: dispatch is decoupled from the peer mesh, no \
                 remote-only gate; got {remote_got:?}"
            );
            assert!(
                !primary.slot_is_idle_for_test(&remote_id, 0),
                "the remote member's worker is busy with its dispatched task"
            );
            assert_eq!(
                primary.pool().iter().count(),
                0,
                "both ready tasks dispatched across the two idle workers"
            );
            let _ = keepalive;
        })
        .await;
}
