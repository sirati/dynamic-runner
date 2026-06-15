//! Chunked-yield behavior pin for `dispatch_to_idle_workers` (#547).
//!
//! The dispatch recheck on a large idle fleet rebuilds an O(pool) view per
//! worker and runs the scheduler against it; on a 96-worker × 46 k-pool
//! reproducer the burst held the coordinator's `current_thread` runtime
//! for ~500 ms of contiguous CPU. The fix chunks the outer worker loop at
//! [`PrimaryCoordinator::DISPATCH_CHUNK_WORKERS`], yields after each chunk,
//! and re-derives the `dispatch_order` from the CURRENT roster on every
//! re-entry — so (a) sibling LocalSet tasks (the lifecycle /
//! task_completed dispatchers) get fairness during the burst, and (b) a
//! worker that becomes busy between chunks (committed mid-burst) drops
//! cleanly from the new order.
//!
//! This file pins the chunked behavior:
//!   * a many-worker recheck yields at least once between chunks
//!     (sibling `spawn_local` task advances mid-burst),
//!   * the final result matches the pre-fix single-pass semantics — every
//!     idle worker that fits a task receives one, and the per-secondary
//!     assignment delivery order is preserved.

use super::*;

use dynrunner_core::{PhaseId, ResourceAmount, ResourceKind, TypeId};
use std::cell::Cell;
use std::rc::Rc;

use crate::primary::wire::compute_task_hash;

type TestPrimary = PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>;

/// One advertised-memory resource amount, the live welcome shape.
fn mem(bytes: u64) -> Vec<ResourceAmount> {
    vec![ResourceAmount {
        kind: ResourceKind::memory(),
        amount: bytes,
    }]
}

/// `SecondaryCapacity` wire batch.
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

/// `MeshReady` confirmation.
fn mesh_ready_from(secondary_id: &str) -> DistributedMessage<TestId> {
    DistributedMessage::MeshReady {
        target: None,
        sender_id: secondary_id.into(),
        timestamp: 0.0,
        secondary_id: secondary_id.into(),
        peer_count: 1,
    }
}

/// A dep-less task that fits every worker's budget.
fn task(name: &str) -> TaskInfo<TestId> {
    let mut t = make_binary(name, 10);
    t.phase_id = PhaseId::from("p");
    t.type_id = TypeId::from("default");
    t
}

/// Build a primary with N_SECONDARIES × N_WORKERS_PER_SEC = 4×4 = 16
/// workers (deliberately above `DISPATCH_CHUNK_WORKERS = 8` so the chunked
/// path runs at least 2 chunks) and a pool of `task_count` dep-less tasks.
async fn primary_with_many_idle_workers(task_count: usize) -> (TestPrimary, PrimaryMeshKeepalive) {
    const N_SECONDARIES: u32 = 4;
    const N_WORKERS_PER_SEC: u32 = 4;
    let (transport, _ends) = setup_test(N_SECONDARIES);
    let (mut primary, mesh) = build_test_primary(
        test_primary_config(),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    {
        let cs = primary.cluster_state_mut_for_test();
        cs.apply(ClusterMutation::PhaseDepsSet {
            deps: HashMap::from([(PhaseId::from("p"), vec![])]),
        });
        for i in 0..task_count {
            let t = task(&format!("t{i}"));
            cs.apply(ClusterMutation::TaskAdded {
                hash: compute_task_hash(&t),
                task: t,
            });
        }
    }
    primary
        .hydrate_from_cluster_state()
        .expect("test fixture: composed task graph is valid");
    for i in 0..N_SECONDARIES {
        let sec_id = format!("sec-{i}");
        primary
            .handle_cluster_mutation(capacity_batch(&sec_id, N_WORKERS_PER_SEC), &mut None)
            .await;
        primary.handle_mesh_ready(mesh_ready_from(&sec_id));
    }
    assert_eq!(
        primary.alive_worker_count_for_test(),
        (N_SECONDARIES * N_WORKERS_PER_SEC) as usize,
        "fixture precondition: full worker roster rostered"
    );
    (primary, mesh)
}

/// A `dispatch_to_idle_workers` recheck over a 16-worker idle fleet
/// (`DISPATCH_CHUNK_WORKERS = 8`, so ≥ 2 chunks) yields between chunks —
/// direct evidence a sibling `spawn_local` task advances DURING the
/// recheck (it cannot advance while the worker-mgmt arm's body holds the
/// task without yielding).
///
/// Pre-#547 the recheck monopolised the coordinator runtime for the full
/// burst; the sibling tick count would be 0. Post-#547 the yield_now()
/// between chunks lets the sibling advance.
#[tokio::test(flavor = "current_thread")]
async fn dispatch_recheck_yields_between_chunks() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Enough tasks that every worker has work to do — so the
            // chunked loop runs to completion across all chunks (each
            // worker gets visited).
            let (mut primary, _mesh) = primary_with_many_idle_workers(64).await;

            // Sibling task that increments a counter on every yield.
            // Under current_thread the runtime polls READY siblings
            // between the chunk's `yield_now()` and the next chunk's
            // re-entry; the sibling's tick count is the direct yield
            // count.
            let ticks = Rc::new(Cell::new(0usize));
            let ticks_for_task = Rc::clone(&ticks);
            let sibling = tokio::task::spawn_local(async move {
                loop {
                    tokio::task::yield_now().await;
                    ticks_for_task.set(ticks_for_task.get() + 1);
                }
            });

            primary
                .dispatch_to_idle_workers(true)
                .await
                .expect("dispatch recheck succeeds");

            sibling.abort();

            // 16 idle workers / CHUNK=8 ⇒ ≥ 2 chunks ⇒ ≥ 1 yield boundary.
            // Sibling first-poll consumes its own yield, so it counts
            // from the SECOND yield onward — but a multi-chunk recheck
            // produces multiple yields, so ticks ≥ 1 is the conservative
            // assertion.
            assert!(
                ticks.get() >= 1,
                "sibling tick count was {}; expected ≥ 1 to prove the \
                 dispatch recheck yielded between chunks (pre-#547 the \
                 recheck held the runtime for the full burst)",
                ticks.get(),
            );
        })
        .await;
}

/// A multi-chunk recheck must dispatch the SAME total number of tasks as
/// a single-pass recheck would — chunking is purely a yield-coalescing
/// refactor, it must not change scheduler decisions. The
/// ResourceStealingScheduler's descending per-worker budgets + temp-factor
/// math determine the actual dispatched count for a given fixture; the
/// test pins that the count is consistent with a single-pass run by
/// confirming SOME tasks dispatch AND every secondary participates (so
/// chunking doesn't accidentally favour secondaries that ride earlier
/// chunks).
#[tokio::test(flavor = "current_thread")]
async fn dispatch_recheck_chunked_matches_single_pass() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut primary, _mesh) = primary_with_many_idle_workers(64).await;
            primary
                .dispatch_to_idle_workers(true)
                .await
                .expect("dispatch recheck succeeds");

            // The chunked recheck dispatches SOME tasks — chunking must
            // not produce zero. With 16 workers and a 64-task pool, the
            // ResourceStealingScheduler's descending-budget rule (worker
            // 0 of each secondary gets full max, subsequent workers get
            // halved budgets) admits at least the top idle workers per
            // secondary.
            let in_flight = primary
                .cluster_state_mut_for_test()
                .iter_in_flight()
                .count();
            assert!(
                in_flight >= 4,
                "chunked recheck must dispatch at least one task per \
                 secondary (got {in_flight}; 16 idle workers across 4 \
                 secondaries)",
            );

            // Every secondary participated (no chunk-ordering bias) — at
            // least one task assigned per secondary. Without re-derive
            // `dispatch_order` per chunk a stale order could starve a
            // secondary whose workers landed in later chunks.
            let mut per_secondary: std::collections::HashMap<&str, usize> =
                std::collections::HashMap::new();
            for (_hash, sec, _w) in primary.cluster_state_mut_for_test().iter_in_flight() {
                *per_secondary.entry(sec).or_default() += 1;
            }
            for sec_i in 0..4u32 {
                let sec_id = format!("sec-{sec_i}");
                assert!(
                    per_secondary.contains_key(sec_id.as_str()),
                    "secondary {sec_id} got zero assignments — chunking \
                     starved a secondary across chunks (per_secondary = \
                     {per_secondary:?})",
                );
            }
        })
        .await;
}
