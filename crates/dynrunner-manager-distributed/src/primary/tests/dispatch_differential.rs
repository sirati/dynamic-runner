//! Differential pin for `dispatch_to_idle_workers`: a fixed
//! (workers × pool) scenario must always produce the exact same
//! per-worker assignments. The fixture exercises every input the
//! dispatch view pipeline composes — typed-affinity buckets, the free
//! pool, the soft `preferred_secondaries` tie-break, and the
//! pin-evolution interplay between successive workers within ONE
//! recheck (worker N's view must observe worker N-1's take) — so any
//! reshaping of the recheck internals (budget-info construction, view
//! representation) that changes scheduler-visible ordering or content
//! fails loudly here.
//!
//! Expected assignments derive from the pinned semantics:
//!   * roster: round-robin name-sorted → [sec-0/w0, sec-1/w0,
//!     sec-0/w1, sec-1/w1]; `dispatch_order` visits all-idle slots in
//!     that (stable) order;
//!   * hydrate sorts the pool size-DESC, so intra-bucket FIFO is
//!     alpha [t20, t10], beta [t40, t30], free [t60, t50];
//!   * sec-0/w0 leads with the typed classes (alpha < beta in
//!     BTreeMap order) → takes t20, pinning alpha;
//!   * sec-1/w0 sees beta as the only unpinned typed bucket → takes
//!     t40, pinning beta;
//!   * sec-0/w1 has no unpinned typed bucket left; its free-pool
//!     class keeps FIFO order (t60's preference names sec-1, which is
//!     Equal-vs-Equal from sec-0's predicate — a soft preference only
//!     LIFTS items for the preferred secondary, it never sinks them
//!     for others) → takes t60;
//!   * sec-1/w1 gets the remaining free-pool item → t50.

use super::*;

use dynrunner_core::{
    AffinityId, PhaseId, ResourceAmount, ResourceKind, SoftPreferredSecondaries, TypeId,
};

use crate::primary::wire::compute_task_hash;

type TestPrimary = PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>;

/// One advertised-memory resource amount (bytes), the live welcome shape.
fn mem(bytes: u64) -> Vec<ResourceAmount> {
    vec![ResourceAmount {
        kind: ResourceKind::memory(),
        amount: bytes,
    }]
}

/// A task in phase "p" / type "default" with the given affinity
/// (empty → free pool) and soft preferred secondaries.
fn task(name: &str, size: u64, affinity: &str, preferred: &[&str]) -> TaskInfo<TestId> {
    let mut t = make_binary(name, size);
    t.phase_id = PhaseId::from("p");
    t.type_id = TypeId::from("default");
    t.affinity_id = if affinity.is_empty() {
        None
    } else {
        Some(AffinityId::from(affinity))
    };
    t.preferred_secondaries =
        SoftPreferredSecondaries(preferred.iter().map(|s| s.to_string()).collect());
    t
}

/// A `SecondaryCapacity` wire batch naming `secondary` with `n` workers,
/// paired with its `PeerJoined` (same shape as the capacity-dispatch
/// tests use).
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

/// The member's `MeshReady` confirmation (assignability gate).
fn mesh_ready_from(secondary_id: &str) -> DistributedMessage<TestId> {
    DistributedMessage::MeshReady {
        target: None,
        sender_id: secondary_id.into(),
        timestamp: 0.0,
        secondary_id: secondary_id.into(),
        peer_count: 1,
    }
}

/// Drain every `TaskAssignment` queued on a secondary's wire end into
/// ordered `(worker_id, task_id)` pairs.
fn assignments(
    rx: &mut tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
) -> Vec<(u32, String)> {
    let mut out = Vec::new();
    while let Ok(msg) = rx.try_recv() {
        if let DistributedMessage::TaskAssignment {
            worker_id,
            binary_info,
            ..
        } = msg
        {
            out.push((worker_id, binary_info.task_id));
        }
    }
    out
}

/// Build a 2-secondary × 2-worker primary whose pool holds the fixed
/// six-task mix (two typed buckets, two free-pool items, one soft
/// preference) — the differential scenario.
#[allow(clippy::type_complexity)]
async fn primary_with_mixed_pool() -> (
    TestPrimary,
    Vec<(
        String,
        tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
        tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
    )>,
    PrimaryMeshKeepalive,
) {
    let (transport, ends) = setup_test(2);
    let (mut primary, mesh) = build_test_primary(
        test_primary_config(),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    let tasks = [
        task("t10", 10, "alpha", &[]),
        task("t20", 20, "alpha", &[]),
        task("t30", 30, "beta", &[]),
        task("t40", 40, "beta", &[]),
        task("t50", 50, "", &[]),
        task("t60", 60, "", &["sec-1"]),
    ];
    {
        let cs = primary.cluster_state_mut_for_test();
        cs.apply(ClusterMutation::PhaseDepsSet {
            deps: HashMap::from([(PhaseId::from("p"), vec![])]),
        });
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
    for sec in ["sec-0", "sec-1"] {
        primary
            .handle_cluster_mutation(capacity_batch(sec, 2), &mut None)
            .await;
        primary.handle_mesh_ready(mesh_ready_from(sec));
    }
    assert_eq!(
        primary.alive_worker_count_for_test(),
        4,
        "fixture precondition: 2 secondaries × 2 workers rostered"
    );
    (primary, ends, mesh)
}

/// Same workers + same pool → same assignments, down to the per-worker
/// task identity and the per-secondary delivery order.
#[tokio::test(flavor = "current_thread")]
async fn fixed_scenario_produces_identical_assignments() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut primary, mut ends, _mesh) = primary_with_mixed_pool().await;

            primary
                .dispatch_to_idle_workers(true)
                .await
                .expect("dispatch recheck succeeds");
            settle_pump().await;

            assert_eq!(
                assignments(&mut ends[0].1),
                vec![(0, "t20".to_string()), (1, "t60".to_string())],
                "sec-0: w0 leads the typed classes (alpha first), w1 falls \
                 back to the free pool in FIFO order"
            );
            assert_eq!(
                assignments(&mut ends[1].1),
                vec![(0, "t40".to_string()), (1, "t50".to_string())],
                "sec-1: w0 takes the unpinned typed bucket (beta), w1 gets \
                 the remaining free-pool item"
            );

            // The two co-pin-class leftovers stay queued.
            let mut left: Vec<u64> = primary.pool().iter().map(|t| t.size).collect();
            left.sort();
            assert_eq!(left, vec![10, 30], "alpha#2 + beta#2 remain queued");
        })
        .await;
}
