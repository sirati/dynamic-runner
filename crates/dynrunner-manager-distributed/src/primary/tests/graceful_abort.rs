//! Primary-side graceful-abort protocol tests.
//!
//! Pins, per the feature's contract:
//!   * the dispatch FREEZE: with the replicated latch set, the one
//!     dispatch-view seam empties every worker's view, so a dispatch tick
//!     assigns NOTHING and the ready pool is preserved (the positive
//!     control proves the same fixture dispatches without the latch);
//!   * the no-redo law: a promoted primary restoring a FROZEN snapshot
//!     inherits the latch and also refuses to schedule;
//!   * the request→latch origination: an observer's typed
//!     `GracefulAbortRequest` frame latches the CRDT exactly once
//!     (idempotent re-request);
//!   * the `MostActiveWorkers` relocation policy: busiest eligible
//!     secondary wins, ineligible/idle candidates excluded;
//!   * the per-iteration drain decision (`graceful_abort_tick`): break on
//!     full fleet drain; relocate to the busiest secondary when the
//!     primary's own node drained while remote work continues; stay put
//!     while own work runs.

use super::*;

use dynrunner_core::{PhaseId, ResourceAmount, ResourceKind, TypeId};

use crate::primary::lifecycle::RelocationPolicy;

/// One advertised-memory resource amount (the live welcome shape).
fn mem(bytes: u64) -> Vec<ResourceAmount> {
    vec![ResourceAmount {
        kind: ResourceKind::memory(),
        amount: bytes,
    }]
}

/// Drain every `TaskAssignment` `task_id` queued on the primary→secondary
/// wire (non-blocking).
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

/// A single-phase `TaskInfo` whose `task_id == hash == name` (the
/// CRDT-key / dep-key alignment the sibling hydrate tests use).
fn plain_task(name: &str) -> TaskInfo<TestId> {
    let mut t = make_binary(name, 100);
    t.phase_id = PhaseId::from("default");
    t.type_id = TypeId::from("default");
    t
}

/// Seed `primary`'s CRDT with the `sec-0` capacity record + two Pending
/// tasks, then hydrate (pool + worker roster derived from the ledger) and
/// confirm `sec-0`'s mesh leg so the dispatch-readiness gate admits it.
fn seed_two_pending_on_sec0(
    primary: &mut PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
) {
    {
        let cs = primary.cluster_state_mut_for_test();
        cs.apply(ClusterMutation::SecondaryCapacity {
            secondary: "sec-0".into(),
            worker_count: 2,
            resources: mem(8 * 1024 * 1024 * 1024),
        });
        for name in ["t-0", "t-1"] {
            cs.apply(ClusterMutation::TaskAdded {
                hash: name.into(),
                task: plain_task(name),
            });
        }
    }
    primary.hydrate_from_cluster_state()
        .expect("test fixture: composed task graph is valid");
    primary.confirm_member_mesh_for_test("sec-0");
}

/// Register `id` as an alive cluster member with `can_be_primary` /
/// observer capability bits plus a worker-capacity record, and assign
/// `inflight` replicated `InFlight` tasks to it — the CRDT facts the
/// relocation policy + drain decision read.
fn seed_member_with_inflight(
    primary: &mut PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
    id: &str,
    can_be_primary: bool,
    inflight: usize,
) {
    let cs = primary.cluster_state_mut_for_test();
    cs.apply(ClusterMutation::PeerJoined {
        peer_id: id.into(),
        is_observer: false,
        can_be_primary,
        cap_version: Default::default(),
        member_gen: 0,
    });
    cs.apply(ClusterMutation::SecondaryCapacity {
        secondary: id.into(),
        worker_count: 4,
        resources: mem(8 * 1024 * 1024 * 1024),
    });
    for i in 0..inflight {
        let hash = format!("{id}-task-{i}");
        cs.apply(ClusterMutation::TaskAdded {
            hash: hash.clone(),
            task: plain_task(&hash),
        });
        cs.apply(ClusterMutation::TaskAssigned {
            hash,
            secondary: id.into(),
            worker: i as u32,
            version: Default::default(),
            attempt: 0,
        });
    }
}

/// (a) The dispatch freeze, with its positive control: the SAME fixture
/// dispatches normally without the latch, and assigns NOTHING with it —
/// the ready tasks stay in the pool, every view is emptied at the one
/// dispatch-view seam.
#[tokio::test(flavor = "current_thread")]
async fn graceful_abort_freezes_dispatch_at_the_view_seam() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Positive control — no latch: the fixture dispatches.
            let (transport, mut ends) = setup_test(1);
            let (mut control, _mesh_a) = build_test_primary(
                PrimaryConfig::default(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            seed_two_pending_on_sec0(&mut control);
            control
                .dispatch_to_idle_workers(true)
                .await
                .expect("control dispatch tick");
            settle_pump().await;
            assert_eq!(
                drain_assigned_task_ids(&mut ends[0].1).len(),
                2,
                "positive control: without the latch the fixture must dispatch"
            );

            // Frozen: identical fixture + the latch → nothing dispatches.
            let (transport, mut ends) = setup_test(1);
            let (mut frozen, _mesh_b) = build_test_primary(
                PrimaryConfig::default(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            seed_two_pending_on_sec0(&mut frozen);
            frozen
                .cluster_state_mut_for_test()
                .apply(ClusterMutation::GracefulAbortRequested);

            // The ONE seam: every worker's dispatch view is emptied.
            for worker_idx in 0..frozen.workers.len() {
                assert!(
                    frozen.dispatch_view_for_worker(worker_idx).is_empty(),
                    "worker {worker_idx} must see an EMPTY dispatch view under \
                     the graceful-abort freeze"
                );
            }
            frozen
                .dispatch_to_idle_workers(true)
                .await
                .expect("frozen dispatch tick");
            settle_pump().await;
            assert!(
                drain_assigned_task_ids(&mut ends[0].1).is_empty(),
                "no assignment may leave the pool under the freeze"
            );
            assert_eq!(
                frozen.pool().len(),
                2,
                "the ready tasks stay in the pool (preserved, not dropped)"
            );
        })
        .await;
}

/// (c, no-redo half) A promoted primary restoring a FROZEN snapshot
/// inherits the latch through the CRDT and also refuses to schedule —
/// including the post-promotion `perform_initial_assignment` path, which
/// routes through the same view seam.
#[tokio::test(flavor = "current_thread")]
async fn promoted_primary_inherits_freeze_via_snapshot() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // The pre-failover primary latches the freeze; its snapshot is
            // what the promotion seed carries.
            let snapshot = {
                let (transport, _ends) = setup_test(0);
                let (mut old, _mesh) = build_test_primary(
                    PrimaryConfig::default(),
                    transport,
                    ResourceStealingScheduler::memory(),
                    FixedEstimator(100),
                );
                let cs = old.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::GracefulAbortRequested);
                cs.apply(ClusterMutation::SecondaryCapacity {
                    secondary: "sec-0".into(),
                    worker_count: 2,
                    resources: mem(8 * 1024 * 1024 * 1024),
                });
                for name in ["t-0", "t-1"] {
                    cs.apply(ClusterMutation::TaskAdded {
                        hash: name.into(),
                        task: plain_task(name),
                    });
                }
                cs.snapshot()
            };

            let (transport, mut ends) = setup_test(1);
            let (mut promoted, _mesh) = build_test_primary(
                PrimaryConfig::default(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            promoted.cluster_state_mut_for_test().restore(snapshot);
            assert!(
                promoted.cluster_state_for_test().graceful_abort_requested(),
                "the latch must ride the promotion snapshot"
            );
            promoted.hydrate_from_cluster_state()
        .expect("test fixture: composed task graph is valid");
            promoted.confirm_member_mesh_for_test("sec-0");

            promoted
                .dispatch_to_idle_workers(true)
                .await
                .expect("dispatch tick");
            settle_pump().await;
            assert!(
                drain_assigned_task_ids(&mut ends[0].1).is_empty(),
                "a promoted primary must inherit the freeze and refuse to schedule"
            );
        })
        .await;
}

/// The observer's typed request latches the CRDT exactly once: the first
/// `GracefulAbortRequest` frame applies the latch, a re-sent request is a
/// NoOp (no churn, no re-announce).
#[tokio::test(flavor = "current_thread")]
async fn graceful_abort_request_latches_once() {
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
            assert!(!primary.cluster_state_for_test().graceful_abort_requested());

            let request = || DistributedMessage::<TestId>::GracefulAbortRequest {
                target: None,
                sender_id: "obs-1".into(),
                timestamp: 0.0,
            };
            primary.handle_graceful_abort_request(request()).await;
            assert!(
                primary.cluster_state_for_test().graceful_abort_requested(),
                "the observer's request must latch the replicated freeze"
            );
            // Idempotent re-request (operator re-trigger): still latched,
            // no panic, no state churn.
            primary.handle_graceful_abort_request(request()).await;
            assert!(primary.cluster_state_for_test().graceful_abort_requested());
        })
        .await;
}

/// The `MostActiveWorkers` policy picks the busiest ELIGIBLE secondary:
/// a busier-but-`can_be_primary=false` candidate and idle candidates are
/// excluded; with no busy eligible candidate the selection is `None`
/// (the graceful tick then stays put instead of relocating).
#[tokio::test(flavor = "current_thread")]
async fn most_active_workers_policy_picks_busiest_eligible() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(0);
            let (mut primary, _mesh) = build_test_primary(
                PrimaryConfig::default(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            // sec-loaded: busiest ELIGIBLE (3 in flight).
            seed_member_with_inflight(&mut primary, "sec-loaded", true, 3);
            // sec-light: eligible but lighter (1 in flight).
            seed_member_with_inflight(&mut primary, "sec-light", true, 1);
            // sec-banned: busiest overall but NOT can_be_primary.
            seed_member_with_inflight(&mut primary, "sec-banned", false, 5);
            // sec-idle: eligible but drained — excluded (about to exit).
            seed_member_with_inflight(&mut primary, "sec-idle", true, 0);

            assert_eq!(
                primary
                    .select_relocation_target(RelocationPolicy::MostActiveWorkers)
                    .as_deref(),
                Some("sec-loaded"),
                "busiest ELIGIBLE secondary wins — sec-banned (busier, not \
                 can_be_primary) and sec-idle (drained) are excluded"
            );
            // The LowestId policy over the same set stays the bootstrap
            // behaviour (one selector, two policies).
            assert_eq!(
                primary
                    .select_relocation_target(RelocationPolicy::LowestId)
                    .as_deref(),
                Some("sec-idle"),
                "LowestId ignores occupancy (sec-idle < sec-light < sec-loaded)"
            );
        })
        .await;
}

/// (c + d, the decision ladder) `graceful_abort_tick`:
///   * un-latched → no-op `false`;
///   * latched + own node still busy → `false` (keep draining in place);
///   * latched + own node drained + remote work running → RELOCATES to
///     the busiest eligible secondary (the `PrimaryChanged{Transferred}`
///     local apply re-points `current_primary`) and returns `false`;
///   * latched + full fleet drain → `true` (break into the finalize
///     tail's graceful verdict).
#[tokio::test(flavor = "current_thread")]
async fn graceful_tick_relocates_then_breaks_on_full_drain() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(0);
            let (mut primary, _mesh) = build_test_primary(
                PrimaryConfig::default(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            let own_id = primary.config.node_id.clone();

            // Un-latched: a pure no-op regardless of state.
            assert!(!primary.graceful_abort_tick().await);

            // Latch + name THIS node the primary; give the own node one
            // in-flight task and a busier remote secondary.
            seed_member_with_inflight(&mut primary, &own_id, true, 1);
            seed_member_with_inflight(&mut primary, "sec-busy", true, 2);
            {
                let cs = primary.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::GracefulAbortRequested);
                cs.apply(ClusterMutation::PrimaryChanged {
                    new: own_id.clone(),
                    epoch: 1,
                    reason: Default::default(),
                });
            }

            // Own node still busy → keep draining in place, no relocate.
            assert!(!primary.graceful_abort_tick().await);
            assert_eq!(
                primary.cluster_state_for_test().current_primary(),
                Some(own_id.as_str()),
                "no relocation while this node's own work still runs"
            );

            // Own node drains (its task completes) while sec-busy still
            // runs → the tick relocates to the busiest secondary.
            primary
                .cluster_state_mut_for_test()
                .apply(ClusterMutation::TaskCompleted {
                    hash: format!("{own_id}-task-0"),
                    result_data: None,
                    attempt: 0,
                });
            assert!(!primary.graceful_abort_tick().await);
            assert_eq!(
                primary.cluster_state_for_test().current_primary(),
                Some("sec-busy"),
                "own node drained + remote work running ⇒ relocate to the \
                 busiest eligible secondary"
            );
            // Post-relocate ticks are inert (this node is no longer the
            // recognized primary; the demote arm owns the rest).
            assert!(!primary.graceful_abort_tick().await);

            // Full drain on a node that IS the recognized primary → break.
            let (transport, _ends) = setup_test(0);
            let (mut last_holder, _mesh2) = build_test_primary(
                PrimaryConfig::default(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            let own_id = last_holder.config.node_id.clone();
            {
                let cs = last_holder.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::GracefulAbortRequested);
                cs.apply(ClusterMutation::PrimaryChanged {
                    new: own_id,
                    epoch: 1,
                    reason: Default::default(),
                });
            }
            assert!(
                last_holder.graceful_abort_tick().await,
                "no in-flight work anywhere ⇒ break into the graceful finalize"
            );
        })
        .await;
}
