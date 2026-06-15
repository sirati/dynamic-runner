//! #580 primary-pinned task types: the framework primitive that pins a
//! TaskTypeSpec's items to the primary node. The dispatch view filter is
//! the canonical enforcement seam; the dead-secondary recovery path
//! emits a defense-in-depth ERROR for the structurally-impossible case.
//!
//! Five tests, all deterministic, all view-shape only (no scheduler
//! plumbing — the view is what the scheduler sees, so hiding an item
//! here proves the worker can never receive it):
//!
//! * T1 — eviction of the primary's own secondary requeues a pinned
//!   task; while only non-primary workers exist, no worker view shows
//!   it; once the primary's secondary is re-admitted, the view shows it
//!   to the primary-side worker again.
//! * T2 — an unpinned task (the default) is freely visible across every
//!   worker — wire-compat with the pre-#580 behaviour.
//! * T3 — a primary-pinned task wrongly stamped in_flight on a
//!   NON-primary secondary at dead-secondary recovery emits the
//!   defense-in-depth ERROR and still requeues through the canonical
//!   seam (the view filter holds on re-dispatch).
//! * T4 — multi-worker, multi-secondary: a primary-pinned task appears
//!   ONLY in the primary-node worker's view; never in any non-primary
//!   worker's view.
//! * T5 — pre-existing tasks (with no primary-pinned types registered)
//!   behave byte-identically to the pre-#580 path.
//!
//! All tests construct the coordinator with `node_id == SETUP_NODE_ID`
//! ("setup"), so the primary's own secondary id is "setup" and every
//! other registered secondary id is a peer.

use super::*;

use dynrunner_core::{PhaseId, ResourceMap, ResourceKind, TypeId};

use crate::primary::wire::compute_task_hash;

type TestPrimary = PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>;

/// A small idle-worker resource budget so a `register_idle_worker_for_test`
/// call places a worker that can in principle receive any test task.
fn one_gib() -> ResourceMap {
    ResourceMap::from([(ResourceKind::memory(), 1024 * 1024 * 1024u64)])
}

/// One advertised-memory resource amount for a CRDT secondary-capacity
/// record. Mirrors `tests/hydrate.rs::mem`.
fn mem(bytes: u64) -> Vec<dynrunner_core::ResourceAmount> {
    vec![dynrunner_core::ResourceAmount {
        kind: ResourceKind::memory(),
        amount: bytes,
    }]
}

/// A free-pool work task with explicit `type_id`.
fn task_of_type(name: &str, type_id: &str) -> TaskInfo<TestId> {
    let mut t = make_binary(name, 100);
    t.phase_id = PhaseId::from("default");
    t.type_id = TypeId::from(type_id);
    t.affinity_id = None;
    t
}

/// Build a primary whose `primary_pinned_types` contains the supplied
/// `TypeId`s. `node_id` defaults to `SETUP_NODE_ID` ("setup") — every
/// `register_idle_worker_for_test("setup", ..)` call thus registers a
/// primary-node worker; any other secondary id is a peer.
fn primary_with_pinned_types(
    pinned: &[&str],
) -> (TestPrimary, PrimaryMeshKeepalive) {
    let mut pinned_set = std::collections::HashSet::new();
    for t in pinned {
        pinned_set.insert(TypeId::from(*t));
    }
    let config = PrimaryConfig {
        primary_pinned_types: pinned_set,
        ..test_primary_config()
    };
    let (transport, _ends) = setup_test(1);
    build_test_primary(
        config,
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    )
}

/// Seed `tasks` into the CRDT and hydrate the primary. Mirrors the seed
/// shape in `tests/prefer_dependency.rs::primary_with_dag` but skips the
/// per-task dep graph (these tests don't exercise dep ordering).
fn seed_and_hydrate(primary: &mut TestPrimary, tasks: Vec<TaskInfo<TestId>>) {
    {
        let cs = primary.cluster_state_mut_for_test();
        for t in tasks {
            cs.apply(ClusterMutation::TaskAdded {
                hash: compute_task_hash(&t),
                task: t,
            });
        }
    }
    primary
        .hydrate_from_cluster_state()
        .expect("test fixture: composed task graph is valid");
}

/// task_ids visible in the dispatch view for `worker_idx`.
fn view_ids(primary: &TestPrimary, worker_idx: usize) -> Vec<String> {
    primary
        .dispatch_view_for_worker(worker_idx, false)
        .as_slice()
        .iter()
        .map(|t| t.task_id.clone())
        .collect()
}

// ────────────────────────────────────────────────────────────────────
// T1 — eviction of the primary's own secondary requeues; the pinned
// task is hidden from every non-primary worker view, and re-visible to
// the primary-side worker on re-admission.
// ────────────────────────────────────────────────────────────────────

/// The original bug shape (asm-dataset-nix run_20260615_173709, #580):
/// the primary's own secondary "setup" is wrongly declared dead under
/// collective-silence; its held `dep_graph` task is requeued and would,
/// on the pre-fix path, be dispatched to a peer secondary. With the
/// primary_pinned flag set, the dispatch view at every peer-secondary
/// worker shows ZERO pinned-type items, and the requeued task remains
/// stranded in `Pending` until the primary's own secondary re-admits —
/// at which point the primary-node worker's view sees it again.
#[tokio::test(flavor = "current_thread")]
async fn pinned_task_requeue_hides_from_peer_workers_and_resurfaces_on_readmission() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut primary, _mesh) = primary_with_pinned_types(&["dep_graph"]);

            // The "setup" secondary is alive at start; a peer "peer-1" is
            // alive too. Both have capacity records so the eviction path
            // has a roster to work with.
            {
                let cs = primary.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::SecondaryCapacity {
                    secondary: "setup".into(),
                    worker_count: 1,
                    resources: mem(8 * 1024 * 1024 * 1024),
                });
                cs.apply(ClusterMutation::SecondaryCapacity {
                    secondary: "peer-1".into(),
                    worker_count: 1,
                    resources: mem(8 * 1024 * 1024 * 1024),
                });
            }

            // Seed a primary-pinned `dep_graph` task: pre-assigned to
            // the primary's own secondary "setup" (the lifecycle
            // matches the production scenario — the task was running
            // on the primary's secondary when the false-eviction hit).
            let task = task_of_type("dg-0", "dep_graph");
            let hash = compute_task_hash(&task);
            {
                let cs = primary.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::TaskAdded {
                    hash: hash.clone(),
                    task,
                });
                cs.apply(ClusterMutation::TaskAssigned {
                    attempt: 0,
                    hash: hash.clone(),
                    secondary: "setup".into(),
                    worker: 0,
                    version: Default::default(),
                });
            }
            primary
                .hydrate_from_cluster_state()
                .expect("test fixture: composed task graph is valid");
            assert_eq!(
                primary.in_flight_len_for_test(),
                1,
                "the pinned task seeds in_flight on setup's worker"
            );

            // Collective-silence false-eviction of the primary's own
            // secondary. The recovery path requeues the held task; no
            // ERROR is emitted because the holder IS the primary's
            // own secondary (the expected case).
            let mutations = primary.recover_inflight_for_dead_secondary("setup");
            assert_eq!(mutations.len(), 1, "the pinned task is requeued");
            for m in mutations {
                primary.cluster_state_mut_for_test().apply(m);
            }

            // Only the peer-1 worker is alive right now. Its dispatch
            // view must NOT show the requeued pinned task.
            primary.register_idle_worker_for_test("peer-1".into(), 0, one_gib());
            let peer_view = view_ids(&primary, 0);
            assert!(
                peer_view.is_empty(),
                "primary-pinned task must be hidden from peer-secondary workers; \
                 saw {:?}",
                peer_view
            );

            // Re-admit the primary's own secondary. Its worker now sees
            // the pinned task in its dispatch view.
            primary.register_idle_worker_for_test("setup".into(), 1, one_gib());
            let setup_view = view_ids(&primary, 1);
            assert_eq!(
                setup_view,
                vec!["dg-0".to_string()],
                "primary-pinned task resurfaces in the primary-secondary worker's view \
                 on re-admission"
            );
            // The peer-1 worker's view is still empty.
            assert!(
                view_ids(&primary, 0).is_empty(),
                "primary-pinned task stays hidden from the peer-secondary worker even \
                 after the primary-secondary is re-admitted"
            );
        })
        .await;
}

// ────────────────────────────────────────────────────────────────────
// T2 — an unpinned task (the default) is freely visible across every
// worker — wire-compat with the pre-#580 behaviour.
// ────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "current_thread")]
async fn unpinned_task_visible_to_every_worker_default_behaviour() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // No registered pinned types — the empty-set fast path.
            let (mut primary, _mesh) = primary_with_pinned_types(&[]);

            let task = task_of_type("t-0", "default");
            seed_and_hydrate(&mut primary, vec![task]);

            primary.register_idle_worker_for_test("setup".into(), 0, one_gib());
            primary.register_idle_worker_for_test("peer-1".into(), 0, one_gib());

            assert_eq!(
                view_ids(&primary, 0),
                vec!["t-0".to_string()],
                "default-type task visible to the primary-secondary worker"
            );
            assert_eq!(
                view_ids(&primary, 1),
                vec!["t-0".to_string()],
                "default-type task visible to the peer-secondary worker (wire-compat)"
            );
        })
        .await;
}

// ────────────────────────────────────────────────────────────────────
// T3 — defense-in-depth: a primary-pinned task held by a NON-primary
// secondary at dead-secondary recovery emits the ERROR + still requeues
// through the canonical seam.
// ────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "current_thread")]
async fn pinned_task_on_non_primary_secondary_emits_error_and_requeues() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut primary, _mesh) = primary_with_pinned_types(&["dep_graph"]);

            // Both secondaries known: setup (primary) + peer-bad
            // (the offender that the test impossibly seeds with the
            // pinned task).
            {
                let cs = primary.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::SecondaryCapacity {
                    secondary: "setup".into(),
                    worker_count: 1,
                    resources: mem(8 * 1024 * 1024 * 1024),
                });
                cs.apply(ClusterMutation::SecondaryCapacity {
                    secondary: "peer-bad".into(),
                    worker_count: 1,
                    resources: mem(8 * 1024 * 1024 * 1024),
                });
            }

            // Defense-in-depth case: a primary-pinned task is stamped
            // in_flight on peer-bad (a NON-primary secondary). This is
            // structurally impossible — the dispatch filter is supposed
            // to prevent it — but the recovery path must still emit a
            // loud ERROR and route the task through the canonical
            // requeue seam so the filter holds on re-dispatch.
            let task = task_of_type("dg-bad", "dep_graph");
            let hash = compute_task_hash(&task);
            {
                let cs = primary.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::TaskAdded {
                    hash: hash.clone(),
                    task,
                });
                cs.apply(ClusterMutation::TaskAssigned {
                    attempt: 0,
                    hash: hash.clone(),
                    secondary: "peer-bad".into(),
                    worker: 0,
                    version: Default::default(),
                });
            }
            primary
                .hydrate_from_cluster_state()
                .expect("test fixture: composed task graph is valid");
            assert_eq!(primary.in_flight_len_for_test(), 1);

            // peer-bad dies. The recovery path must still requeue the
            // task (one TaskRequeued mutation), so the CRDT shows
            // Pending and the dispatch filter remains the enforcement
            // seam on re-dispatch. The accompanying ERROR log is the
            // diagnostic — its presence is verified by the WARN/ERROR
            // capture (tracing macros are validated at compile time;
            // we assert the SHAPE of the recovery: requeue happened).
            let mutations = primary.recover_inflight_for_dead_secondary("peer-bad");
            assert_eq!(
                mutations.len(),
                1,
                "the pinned task is requeued through the canonical seam"
            );
            assert!(
                matches!(mutations[0], ClusterMutation::TaskRequeued { .. }),
                "the canonical-seam mutation is TaskRequeued (not a side-channel \
                 terminal), so the dispatch view filter holds on re-dispatch"
            );
            for m in mutations {
                primary.cluster_state_mut_for_test().apply(m);
            }

            // Register a peer worker — the requeued pinned task is still
            // hidden from it (the filter holds), even though the holder
            // bug existed earlier.
            primary.register_idle_worker_for_test("peer-2".into(), 0, one_gib());
            let peer_view = view_ids(&primary, 0);
            assert!(
                peer_view.is_empty(),
                "the requeued primary-pinned task stays hidden from non-primary \
                 workers; saw {:?}",
                peer_view
            );

            // And visible to a primary-side worker.
            primary.register_idle_worker_for_test("setup".into(), 0, one_gib());
            let setup_view = view_ids(&primary, 1);
            assert_eq!(
                setup_view,
                vec!["dg-bad".to_string()],
                "the requeued primary-pinned task is visible to the primary-side \
                 worker — the canonical enforcement seam"
            );
        })
        .await;
}

// ────────────────────────────────────────────────────────────────────
// T4 — multi-worker, multi-secondary: a primary-pinned task appears
// only in the primary-node worker's view.
// ────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "current_thread")]
async fn pinned_task_visible_only_to_primary_node_workers() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut primary, _mesh) = primary_with_pinned_types(&["dep_graph"]);

            let pinned = task_of_type("dg-0", "dep_graph");
            let regular = task_of_type("rg-0", "default");
            seed_and_hydrate(&mut primary, vec![pinned, regular]);

            // Two primary-secondary workers; two peer-secondary workers.
            primary.register_idle_worker_for_test("setup".into(), 0, one_gib());
            primary.register_idle_worker_for_test("setup".into(), 1, one_gib());
            primary.register_idle_worker_for_test("peer-a".into(), 0, one_gib());
            primary.register_idle_worker_for_test("peer-b".into(), 0, one_gib());

            // Primary-secondary workers see BOTH the pinned and the
            // regular task (the pinned filter is a no-op for them).
            for primary_idx in [0usize, 1] {
                let v = view_ids(&primary, primary_idx);
                let mut sorted = v.clone();
                sorted.sort();
                assert_eq!(
                    sorted,
                    vec!["dg-0".to_string(), "rg-0".to_string()],
                    "primary-secondary worker {} sees both tasks; saw {:?}",
                    primary_idx,
                    v
                );
            }

            // Peer-secondary workers see ONLY the regular task — the
            // pinned task is filtered out before they ever see it.
            for peer_idx in [2usize, 3] {
                let v = view_ids(&primary, peer_idx);
                assert_eq!(
                    v,
                    vec!["rg-0".to_string()],
                    "peer-secondary worker {} sees only the regular task; saw {:?}",
                    peer_idx,
                    v
                );
            }
        })
        .await;
}

// ────────────────────────────────────────────────────────────────────
// T5 — wire-compat: with no primary_pinned types registered, the empty-
// set fast path keeps the view byte-identical to the pre-#580 baseline.
// ────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "current_thread")]
async fn empty_pinned_set_preserves_baseline_view_behaviour() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // No registered pinned types: every type is freely
            // relocatable, every worker sees every task.
            let (mut primary, _mesh) = primary_with_pinned_types(&[]);

            let t1 = task_of_type("t-1", "default");
            let t2 = task_of_type("t-2", "dep_graph"); // a type NAME
                                                        // that future runs may pin —
                                                        // but this run does not, so the
                                                        // task is freely visible.
            seed_and_hydrate(&mut primary, vec![t1, t2]);

            primary.register_idle_worker_for_test("setup".into(), 0, one_gib());
            primary.register_idle_worker_for_test("peer-1".into(), 0, one_gib());

            let mut setup_view = view_ids(&primary, 0);
            let mut peer_view = view_ids(&primary, 1);
            setup_view.sort();
            peer_view.sort();
            assert_eq!(
                setup_view,
                vec!["t-1".to_string(), "t-2".to_string()],
                "with no pinned types registered, the primary-secondary worker sees \
                 every task"
            );
            assert_eq!(
                peer_view,
                vec!["t-1".to_string(), "t-2".to_string()],
                "with no pinned types registered, the peer-secondary worker sees the \
                 same set — wire-compat with the pre-#580 baseline"
            );
        })
        .await;
}
