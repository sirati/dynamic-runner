//! #502 — the DISTRIBUTED primary-ORIGINATE-path AffineReady regression.
//!
//! The #497 SecondaryAffine gate resolves-as-dependency the moment its own
//! deps are done (the READY-not-EXECUTED `AffineReady` transition), so its
//! dependent build tasks unblock without the primary ever executing the gate.
//! The cluster_state unit tests (`cluster_state::tests::affine`) pinned the
//! DETECTION rule by calling `affine_ready_mutations_for` directly — the
//! function, NOT the origination wiring. This file pins the wiring on the
//! path PRODUCTION uses: the primary ORIGINATES a no-dep gate + a dependent
//! build through its SEED originators (`originate_cold_seed` /
//! `discover_on_promotion`) and the promotion-snapshot inherit, then hydrates
//! its pool — and the gate MUST come out `AffineReady` with the dependent
//! build dispatchable.
//!
//! Why these tests catch what the units missed: the seed originators grow the
//! ledger via `ClusterMutation::TaskAdded`, whose apply arm deliberately does
//! NOT feed the `newly_pending_from_spawn` delta surface the live AffineReady
//! originator fires on (the receive side rebuilds the whole pool for a
//! `TaskAdded` batch instead). So a no-dep gate seeded this way is born
//! `Pending`-all-resolved yet — pre-fix — NEVER transitions to `AffineReady`:
//! its dependent build stays Blocked, the build-phase initial-assignment finds
//! ZERO worker-assignable tasks (matcher=0), and the run deadlocks. Each test
//! below FAILS on trunk-without-the-fix (the gate stays `Pending`, the build
//! lands Blocked) and passes once the seed originators drive the AffineReady
//! resolution before hydrate.

use super::*;

use dynrunner_core::{TaskDep, TaskKind};

use crate::cluster_state::TaskState;
use crate::primary::wire::compute_task_hash;

/// A no-dep `TaskKind::SecondaryAffine` gate (the per-secondary import gate
/// `I` between an upload and a build) on `make_binary`'s default phase.
fn affine_gate(name: &str) -> TaskInfo<TestId> {
    let mut t = make_binary(name, 100);
    t.kind = TaskKind::SecondaryAffine;
    t
}

/// A `Work` (default) build task that depends on `dep` (same default phase
/// `make_binary` uses, so the cross-task `task_depends_on` resolves by id).
fn build_depending_on(name: &str, dep: &str) -> TaskInfo<TestId> {
    let mut t = make_binary(name, 100);
    t.task_depends_on = vec![TaskDep {
        task_id: dep.into(),
        phase_id: t.phase_id.clone(),
        inherit_outputs: false,
    }];
    t
}

/// Assert the post-hydrate invariant the fix establishes: the gate is
/// `AffineReady` (resolved at seed, never to be executed), the build is
/// `Pending` and DISPATCHABLE (its dep on the gate resolved), the gate is NOT
/// in the dispatch pool (a resolved gate is a terminal, never pooled), and
/// nothing is Blocked. Each clause is exactly what trunk-without-the-fix
/// violates (gate stays `Pending`, build lands Blocked).
fn assert_gate_resolved_and_build_dispatchable(
    primary: &PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
    gate_hash: &str,
    build_hash: &str,
) {
    let cs = primary.cluster_state_for_test();
    assert!(
        matches!(cs.task_state(gate_hash), Some(TaskState::AffineReady { .. })),
        "the seeded no-dep SecondaryAffine gate must resolve to AffineReady on \
         the primary's own originate path (pre-fix it stays Pending forever); \
         got {:?}",
        cs.task_state(gate_hash)
    );
    assert!(
        matches!(cs.task_state(build_hash), Some(TaskState::Pending { .. })),
        "the dependent build must land Pending (its dep on the gate resolved \
         the moment the gate became ready); pre-fix it is Blocked on the \
         never-ready gate; got {:?}",
        cs.task_state(build_hash)
    );

    // The pool's queued view holds EXACTLY the build: a resolved gate is a
    // terminal (not pooled), and the build is dispatchable. Pre-fix the gate
    // sits Pending in the pool (inert — a gate is not worker-assignable) and
    // the build is Blocked, so this view would be `[<gate>]` with a non-zero
    // blocked count.
    let queued: Vec<String> = primary.pool().iter().map(|t| t.task_id.clone()).collect();
    assert_eq!(
        queued,
        vec!["build".to_string()],
        "exactly the build is queued+dispatchable; the resolved gate is a \
         terminal and never enters the pool; got {queued:?}"
    );
    assert_eq!(
        primary.pool().blocked_len(),
        0,
        "nothing is Blocked — the gate's seed-time resolution unblocked the \
         build (pre-fix the build is Blocked on the never-ready gate)"
    );
}

/// (a) PRIMARY ORIGINATE via cold-seed (mode-1). The primary cold-seeds a
/// no-dep SecondaryAffine gate + a dependent build (the build-phase
/// streamed-spawn analog), then hydrates its pool. The gate must resolve to
/// AffineReady on the seed path and the build must be dispatchable.
///
/// FAILS on trunk-without-the-fix: `TaskAdded` never feeds the AffineReady
/// originator's delta surface, so the gate stays `Pending`, the build hydrates
/// Blocked, and the pool's queued view is `[gate]` with `blocked_len() == 1`.
#[test]
fn cold_seed_originate_resolves_gate_and_build_is_dispatchable() {
    let (transport, _ends) = setup_test(1);
    let (mut primary, _mesh) = build_test_primary(
        test_primary_config(),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );

    let gate = affine_gate("import");
    let build = build_depending_on("build", "import");
    let gate_hash = compute_task_hash(&gate);
    let build_hash = compute_task_hash(&build);

    primary
        .originate_cold_seed(vec![(gate, false), (build, false)], HashMap::new())
        .expect("cold seed of a no-dep affine gate + dependent build");
    primary
        .hydrate_from_cluster_state()
        .expect("composed task graph is valid");

    assert_gate_resolved_and_build_dispatchable(&primary, &gate_hash, &build_hash);
}

/// (a') PROMOTION-SNAPSHOT inherit. A FRESH primary (the relocate target / the
/// failover-election winner) inherits the cold-seeded ledger via
/// `seed_from_promotion_snapshot` and hydrates. The gate's seed-time
/// resolution must already be in the snapshot — the build is dispatchable
/// purely from the inherited ledger, no local re-origination.
///
/// This is the production ColdStart shape: the setup peer cold-seeds + staged-
/// broadcasts the resolved gate, the relocate target captures its snapshot
/// AFTER that broadcast, and runs the build phase off the inherited ledger.
/// FAILS on trunk-without-the-fix: the original primary never resolved the
/// gate, so the snapshot carries a `Pending` gate and the promoted primary
/// hydrates the build Blocked.
#[test]
fn promotion_snapshot_inherits_resolved_gate_and_build_is_dispatchable() {
    // ORIGINAL primary cold-seeds the gate + build (and resolves the gate).
    let (transport, _ends) = setup_test(1);
    let (mut original, _mesh) = build_test_primary(
        test_primary_config(),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    let gate = affine_gate("import");
    let build = build_depending_on("build", "import");
    let gate_hash = compute_task_hash(&gate);
    let build_hash = compute_task_hash(&build);
    original
        .originate_cold_seed(vec![(gate, false), (build, false)], HashMap::new())
        .expect("cold seed on the original primary");
    original
        .hydrate_from_cluster_state()
        .expect("original hydrate");
    let snapshot = original.cluster_state_for_test().snapshot();

    // FRESH primary inherits the ledger via the promotion snapshot path and
    // hydrates — the resolved gate must ride the snapshot.
    let (transport2, _ends2) = setup_test(1);
    let (mut promoted, _mesh2) = build_test_primary(
        test_primary_config(),
        transport2,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    promoted.seed_from_promotion_snapshot(snapshot);
    promoted
        .hydrate_from_cluster_state()
        .expect("promoted hydrate over the inherited ledger");

    assert_gate_resolved_and_build_dispatchable(&promoted, &gate_hash, &build_hash);
}

/// (a'') PRIMARY ORIGINATE via discover-on-promotion (mode-2). The relocated /
/// pre-staged primary discovers the corpus post-promotion and originates it
/// (PhaseDepsSet + TaskAdded* + DiscoverySettled) through the SAME
/// `apply_and_broadcast_cluster_mutations` pipeline, then re-hydrates. The
/// discovered no-dep gate must resolve to AffineReady on this originate path
/// and the discovered build must be dispatchable.
///
/// FAILS on trunk-without-the-fix for the SAME reason as the cold-seed path:
/// the discovered `TaskAdded` gate never reaches the AffineReady originator.
#[tokio::test(flavor = "current_thread")]
async fn discover_on_promotion_originate_resolves_gate_and_build_is_dispatchable() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(1);
            let (mut primary, _mesh) = build_test_primary(
                test_primary_config(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let gate = affine_gate("import");
            let build = build_depending_on("build", "import");
            let gate_hash = compute_task_hash(&gate);
            let build_hash = compute_task_hash(&build);

            // Mode-2: declare debt + register a discovery policy that yields
            // the gate + dependent build, then run the discovery originator
            // (the production `discover_on_promotion` seam).
            let fire = std::rc::Rc::new(std::cell::Cell::new(0u32));
            primary.register_setup_discovery(fixed_discovery(
                vec![gate, build],
                HashMap::new(),
                fire.clone(),
            ));
            primary
                .cluster_state_mut_for_test()
                .apply(ClusterMutation::DiscoveryDebtDeclared);

            primary
                .discover_on_promotion()
                .await
                .expect("discovery originate seam");
            assert_eq!(fire.get(), 1, "discovery policy fired once");

            assert_gate_resolved_and_build_dispatchable(&primary, &gate_hash, &build_hash);
        })
        .await;
}
