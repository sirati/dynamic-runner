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
use crate::worker_signal::{WorkerMgmtSignal, WorkerSignalBatch};

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
        def_id: None,
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
        matches!(
            cs.task_state(gate_hash),
            Some(TaskState::AffineReady { .. })
        ),
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
            let fire = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
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
            assert_eq!(
                fire.load(std::sync::atomic::Ordering::Relaxed),
                1,
                "discovery policy fired once"
            );

            assert_gate_resolved_and_build_dispatchable(&primary, &gate_hash, &build_hash);
        })
        .await;
}

// ─────────────────────── #591: with-dep gate runtime-spawn ───────────────────────

/// (c) #591 — a RUNTIME-SPAWNED `TaskKind::SecondaryAffine` gate WITH a
/// pending predecessor dep that COMPLETES after the gate (and its
/// dependents) are runtime-spawned via the `TasksSpawned` classifier.
///
/// THE consumer asm-dataset-nix DEADLOCK scenario (the #591 brief): build
/// tasks gate on a SecondaryAffine `I` which itself depends on a normal
/// WORK task `W`. `I` + dependents land via the runtime `TasksSpawned`
/// classifier while `W` is still InFlight, so `I` is born CRDT-`Blocked`
/// on `W` and every dependent is born CRDT-`Blocked` on `I`. When `W`
/// completes, the primary's `apply_and_broadcast_cluster_mutations` flow
/// auto-resumes `I` `Blocked → Pending` (the `resume_blocked_on(W)`
/// cascade in the `TaskCompleted` arm), the `became_pending` surface
/// drives the `AffineReady` resolution, applying it transitions `I`
/// `Pending → AffineReady` AND resume_blocked_on(I) cascade-resumes every
/// dependent. Both AffineReady + dependent resumption must complete BEFORE
/// the worker-management recheck dispatches anything — otherwise the
/// dependents never enter the pool and no worker picks them up, so no
/// secondary's affine executor ever triggers the gate body, and the gate
/// stays READY-not-EXECUTED forever (the consumer 272-reinject + 0-body-
/// execution deadlock).
///
/// Pre-fix (the bug the asm-dataset run pinned):
///   (a) the gate itself rode `mutations.rs`'s `resumed_for_dispatch`
///       reinject loop unconditionally — but a SecondaryAffine gate is
///       `is_worker_assignable=false`, so reinjecting it leaves an inert
///       non-dispatchable item in the pool with no removal path
///       (resolve_dependency_satisfied_affine_gates only fires on
///       Pending+resolved gates, NOT on the AffineReady gate this
///       resolution produces); and
///   (b) the AffineReady recursion's BROADCAST went out BEFORE the outer
///       TaskCompleted broadcast, so every secondary received AffineReady
///       while its local gate was still CRDT-`Blocked` (the TaskCompleted
///       hadn't applied yet to resume Blocked → Pending). The
///       `Pending`-precondition arm of the AffineReady apply then took the
///       NoOp branch on every secondary, leaving the gate non-`AffineReady`
///       there. `unmet_local_affine_dep` returns `None` for every dependent
///       (it only fires on AffineReady gates), so the secondary's affine
///       executor never dispatches the gate body and the dependents run
///       without their import (broken outputs / retry-loop).
///
/// THIS test pins the PRIMARY-side invariants on the runtime-spawn HAS-DEP
/// path: (i) the gate reaches CRDT `AffineReady`, NOT stuck `Pending`;
/// (ii) the gate is NOT in the dispatch pool (it never was — a gate is
/// inert, the AffineReady resolution + the cascade-resume run in the same
/// `apply_and_broadcast` call, no inert pool residue); (iii) every
/// dependent is CRDT-`Pending` and DISPATCHABLE in the pool (its
/// `Blocked-on-gate` was cleared by the same `resume_blocked_on(gate)`
/// cascade the AffineReady apply emits).
///
/// The broadcast-order half (the half that pins the SECONDARY-side gate-
/// AffineReady transition) lives in `cluster_state::tests::affine`
/// because it reads the `apply_and_broadcast`-emitted frame sequence; the
/// frame-ordering invariant the iterative-drain fix establishes is checked
/// by the `with_dep_runtime_spawn_chain_emits_outer_before_affine_ready`
/// test there.
#[tokio::test(flavor = "current_thread")]
async fn runtime_spawn_with_dep_gate_resolves_when_dependency_completes() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // The PRE-EXISTING normal WORK task `W` (the asm-dataset
            // `build_common_dep`): seeded via `TaskAdded` so it is already
            // CRDT-`Pending` (it is the gate's predecessor), then we will
            // originate its `TaskCompleted` post-spawn to drive the
            // resume cascade onto the runtime-spawned gate.
            let w = make_binary("build_common_dep", 100);
            let w_hash = compute_task_hash(&w);

            let (transport, _ends) = setup_test(1);
            let (mut primary, _mesh) = build_test_primary(
                test_primary_config(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // SEED `W` via the production `TaskAdded` path + hydrate so the
            // pool tracks it as queued-Pending (no dispatch yet — the test
            // never runs a recheck pre-completion).
            {
                let cs = primary.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::TaskAdded {
                    hash: w_hash.clone(),
                    task: w.clone(),
                    def_id: None,
                });
            }
            primary
                .hydrate_from_cluster_state()
                .expect("the W graph is valid");

            // Mark `W` `InFlight` on the CRDT (the production state when its
            // worker is mid-execution and its `TaskCompleted` is about to
            // land), then runtime-spawn the gate + dependents. The dep
            // resolution in `apply_tasks_spawned` sees `W` as `InFlight`
            // (non-terminal), so the gate lands `Blocked { on: w_hash }`
            // and the dependents land `Blocked { on: gate_hash }`.
            {
                let cs = primary.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::TaskAssigned {
                    hash: w_hash.clone(),
                    secondary: "secondary-0".into(),
                    worker: 0,
                    version: Default::default(),
                    attempt: Default::default(),
                });
            }

            // RUNTIME-SPAWN the gate + two dependents through the production
            // `TasksSpawned` apply rule (the exact path `apply_spawn_tasks`
            // calls). They land `Blocked` per the dep classifier; the bug
            // pre-fix surfaces on the LATER TaskCompleted-driven cascade.
            let gate = affine_gate_depending_on("import_common_dep", "build_common_dep");
            let build_a = build_depending_on("build-a", "import_common_dep");
            let build_b = build_depending_on("build-b", "import_common_dep");
            let gate_hash = compute_task_hash(&gate);
            let build_a_hash = compute_task_hash(&build_a);
            let build_b_hash = compute_task_hash(&build_b);
            primary
                .apply_and_broadcast_cluster_mutations(vec![ClusterMutation::TasksSpawned {
                    tasks: vec![gate.clone(), build_a.clone(), build_b.clone()],
                }])
                .await;

            // Pre-completion invariants: `W` InFlight, gate + dependents
            // CRDT-Blocked, NOTHING in the pool (the gate is non-
            // dispatchable; the dependents are CRDT-Blocked; nothing has
            // been resumed yet).
            assert!(
                matches!(
                    primary.cluster_state.task_state(&gate_hash),
                    Some(TaskState::Blocked { .. })
                ),
                "the runtime-spawned gate is CRDT-Blocked on its pending predecessor"
            );
            for (name, hash) in [("build-a", &build_a_hash), ("build-b", &build_b_hash)] {
                assert!(
                    matches!(
                        primary.cluster_state.task_state(hash),
                        Some(TaskState::Blocked { .. })
                    ),
                    "{name} is CRDT-Blocked on the gate (gate is itself Blocked)"
                );
            }
            // The gate + dependents are CRDT-Blocked, so they are NOT in
            // the dispatch pool. `W` (the predecessor) IS in the pool from
            // the hydrate (Pending), but the test never runs a dispatch
            // recheck — its assignment was originated via direct CRDT
            // apply, so the pool entry remains queued; it is consumed only
            // by the post-completion assertions below.
            let queued_pre: Vec<String> =
                primary.pool().iter().map(|t| t.task_id.clone()).collect();
            assert!(
                !queued_pre.contains(&"import_common_dep".to_string()),
                "the gate is non-worker-assignable + CRDT-Blocked; it must \
                 not enter the dispatch pool pre-completion; got {queued_pre:?}"
            );
            assert!(
                !queued_pre.contains(&"build-a".to_string())
                    && !queued_pre.contains(&"build-b".to_string()),
                "the dependents are CRDT-Blocked behind the gate; they must \
                 not enter the dispatch pool pre-completion; got {queued_pre:?}"
            );

            // Originate `W`'s `TaskCompleted` — the seam where the bug fires.
            // This is the EXACT origination path `handle_task_complete` uses
            // (the consumer's run does it the same way: secondary reports
            // `TaskComplete` → primary's `handle_task_complete` →
            // `apply_and_broadcast_cluster_mutations([TaskCompleted])`).
            // The bug: pre-fix the gate's auto-resume Blocked → Pending fed
            // `resumed_for_dispatch`, the reinject loop unconditionally
            // dropped the gate into the pool as an inert item (gate is
            // `is_worker_assignable=false`), THEN the AffineReady recursion
            // applied the gate Pending → AffineReady — but its
            // `broadcast_applied_mutations` for AffineReady WENT OUT FIRST
            // (before the outer's TaskCompleted broadcast), so every
            // secondary's gate stayed CRDT-Blocked when AffineReady arrived
            // and the NoOp branch on the secondary left the gate non-
            // AffineReady there forever. Locally on the primary the cascade
            // also has to resume the dependents Blocked → Pending (the
            // `resume_blocked_on(gate)` chain inside the recursion); this
            // test pins those local invariants.
            primary
                .apply_and_broadcast_cluster_mutations(vec![ClusterMutation::TaskCompleted {
                    hash: w_hash.clone(),
                    result_data: None,
                    attempt: Default::default(),
                }])
                .await;

            // (a) the gate is originated `AffineReady` — NOT stuck Pending,
            // NOT stuck Blocked, NOT in any other live state.
            assert!(
                matches!(
                    primary.cluster_state.task_state(&gate_hash),
                    Some(TaskState::AffineReady { .. })
                ),
                "the runtime-spawned with-dep gate resolves to AffineReady once \
                 its predecessor dep completes (pre-fix it sat Pending in the pool \
                 because the gate was reinjected during the resume cascade); got {:?}",
                primary.cluster_state.task_state(&gate_hash)
            );

            // (b) BOTH builds are CRDT-Pending — their Blocked-on-gate was
            // cleared by the same `resume_blocked_on(gate)` cascade the
            // AffineReady apply emits, and the dependents rode the
            // recursion's `resumed_for_dispatch` into a `pool.reinject` (they
            // ARE worker-assignable and DO belong in the pool).
            for (name, hash) in [("build-a", &build_a_hash), ("build-b", &build_b_hash)] {
                assert!(
                    matches!(
                        primary.cluster_state.task_state(hash),
                        Some(TaskState::Pending { .. })
                    ),
                    "{name} must be Pending once the gate resolved (pre-fix it stayed \
                     Blocked on the never-AffineReady gate); got {:?}",
                    primary.cluster_state.task_state(hash)
                );
            }

            // (c) the dependents are queued + dispatchable in the pool;
            // the gate is NOT in the pool (a resolved gate is a terminal,
            // it never belonged to a dispatch surface). `W` may still be
            // queued (its TaskCompleted apply does not reach the pool
            // through this direct test path), which is irrelevant to the
            // bug — assert only on the gate's absence + the dependents'
            // presence.
            let queued: std::collections::HashSet<String> =
                primary.pool().iter().map(|t| t.task_id.clone()).collect();
            assert!(
                !queued.contains("import_common_dep"),
                "the resolved gate must NOT be in the pool (pre-fix the gate \
                 sat in the pool as an inert non-dispatchable item, reinjected \
                 by `mutations.rs:78` from the gate-resume cascade); got {queued:?}"
            );
            assert!(
                queued.contains("build-a"),
                "build-a must be queued + dispatchable; got {queued:?}"
            );
            assert!(
                queued.contains("build-b"),
                "build-b must be queued + dispatchable; got {queued:?}"
            );
        })
        .await;
}

// ─────────────────────── #506: with-dep gate ───────────────────────

/// A `TaskKind::Setup` task (the upload `U`) with the PRIMARY's own affinity
/// (`None` defaults to the primary) on `make_binary`'s default phase, so it
/// self-execs in-process to `SetupCompleted` when the setup pass runs.
fn upload_setup(name: &str) -> TaskInfo<TestId> {
    let mut t = make_binary(name, 100);
    t.kind = TaskKind::Setup;
    t
}

/// A `SecondaryAffine` gate depending on `dep` (same default phase, resolved
/// by id) — the WITH-dep gate the #506 deadlock hinges on.
fn affine_gate_depending_on(name: &str, dep: &str) -> TaskInfo<TestId> {
    let mut t = affine_gate(name);
    t.task_depends_on = vec![TaskDep {
        task_id: dep.into(),
        phase_id: t.phase_id.clone(),
        inherit_outputs: false,
        def_id: None,
    }];
    t
}

fn one_tasks_added_batch() -> WorkerSignalBatch {
    WorkerSignalBatch {
        signals: vec![WorkerMgmtSignal::TasksAdded],
    }
}

/// (b) #506 — a WITH-dep gate whose dependency completes AFTER seed.
///
/// REPRO (the minimal #506 deadlock): `U`(Setup) → `I_dep`(SecondaryAffine,
/// TaskDep `U`) → two builds(TaskDep `I_dep`), seeded via the REAL
/// `TaskAdded` + hydrate path. Drive ONE worker-management reaction (exactly
/// the operational loop's arm): its setup-dispatch pass self-execs `U` to
/// `SetupCompleted` (which unblocks `I_dep` `blocked → bucket` in the pool and
/// emits `TasksAdded`), and the SAME reaction's affine-resolve pass then
/// drains the now-queued, dependency-satisfied `I_dep` to `AffineReady`,
/// unblocking the builds.
///
/// FAILS on trunk-without-the-fix (revert-confirmed): with the affine-resolve
/// pass removed, `U` completes but `I_dep` rides NEITHER existing firing
/// surface — the seed scan skipped it (its dep `U` was not terminal at seed),
/// and `resume_blocked_on(U)` finds nothing (`I_dep` was seeded CRDT-`Pending`,
/// never CRDT-`Blocked`), so `became_pending` is empty and the originator never
/// re-fires. `I_dep` sits `Pending` forever (a gate is not worker-assignable,
/// so the pool never dispatches it) and the builds stay Blocked behind it →
/// deadlock. Each clause below is exactly what trunk-without-the-fix violates.
#[tokio::test(flavor = "current_thread")]
async fn with_dep_gate_resolves_when_dependency_completes_post_seed() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let upload = upload_setup("upload");
            let gate = affine_gate_depending_on("import", "upload");
            let build_a = build_depending_on("build-a", "import");
            let build_b = build_depending_on("build-b", "import");
            let gate_hash = compute_task_hash(&gate);
            let build_a_hash = compute_task_hash(&build_a);
            let build_b_hash = compute_task_hash(&build_b);

            let (transport, _ends) = setup_test(1);
            let (mut primary, _mesh) = build_test_primary(
                test_primary_config(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // SEED via the production `TaskAdded` path, then hydrate — the gate
            // is born CRDT-`Pending` and pool-`blocked` on `upload`; the builds
            // are pool-`blocked` on `import`.
            {
                let cs = primary.cluster_state_mut_for_test();
                for task in [&upload, &gate, &build_a, &build_b] {
                    cs.apply(ClusterMutation::TaskAdded {
                        hash: compute_task_hash(task),
                        task: task.clone(),
                        def_id: None,
                    });
                }
            }
            primary
                .hydrate_from_cluster_state()
                .expect("the U → I_dep → builds graph is valid");

            // Pre-completion: the gate is NOT yet ready (its dep is live), so
            // it stays Pending and the builds stay Blocked — no premature fire.
            assert!(
                matches!(
                    primary.cluster_state.task_state(&gate_hash),
                    Some(TaskState::Pending { .. })
                ),
                "before its dep completes, the with-dep gate is Pending (the \
                 seed scan correctly skips it); got {:?}",
                primary.cluster_state.task_state(&gate_hash)
            );

            // Drive ONE worker-management reaction: the setup pass self-execs
            // `upload` → `SetupCompleted` (unblocking the gate in the pool +
            // emitting `TasksAdded`), and the affine pass resolves the gate.
            primary
                .react_to_worker_signal_batch(one_tasks_added_batch(), &mut None)
                .await;
            settle_pump().await;

            // (a) the gate is originated `AffineReady` — NOT stuck Pending.
            assert!(
                matches!(
                    primary.cluster_state.task_state(&gate_hash),
                    Some(TaskState::AffineReady { .. })
                ),
                "the with-dep gate resolves to AffineReady once its dependency \
                 completes post-seed (pre-fix it stays Pending forever → \
                 deadlock); got {:?}",
                primary.cluster_state.task_state(&gate_hash)
            );
            assert_eq!(
                primary.cluster_state.outcome_counts().affine_ready,
                1,
                "the resolved gate counts in the affine_ready terminal bucket"
            );

            // (b) BOTH builds are unblocked (CRDT Pending) and dispatchable in
            // the pool — the chain actually completes, not just the gate flip.
            for (name, hash) in [("build-a", &build_a_hash), ("build-b", &build_b_hash)] {
                assert!(
                    matches!(
                        primary.cluster_state.task_state(hash),
                        Some(TaskState::Pending { .. })
                    ),
                    "{name} must be Pending once the gate resolved (pre-fix it \
                     is Blocked on the never-ready gate); got {:?}",
                    primary.cluster_state.task_state(hash)
                );
            }
            let mut queued: Vec<String> =
                primary.pool().iter().map(|t| t.task_id.clone()).collect();
            queued.sort();
            assert_eq!(
                queued,
                vec!["build-a".to_string(), "build-b".to_string()],
                "exactly the two builds are queued + dispatchable; the resolved \
                 gate is a terminal and left the pool, the upload self-executed; \
                 got {queued:?}"
            );
            assert_eq!(
                primary.pool().blocked_len(),
                0,
                "nothing is Blocked — the gate's resolution unblocked both \
                 builds (pre-fix they are Blocked on the never-ready gate)"
            );
        })
        .await;
}
