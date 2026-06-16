//! Tests for the phase-layer proceed-or-fail decision and the
//! `PhaseStartedNeedsWorkers` emission onto the decoupled
//! worker-management bus.
//!
//! Two concerns, both synchronous and deterministic (no operational
//! loop, no wall-clock waits):
//! - [`phase_can_proceed`] decides advance-vs-fail from the replicated
//!   ledger (`phase_rollups`: a phase that drained with tasks reads
//!   `has_any && !has_live`), the per-phase residual-work probe, the
//!   replicated `may_be_empty` opt-out, AND the outstanding-work probe
//!   (`pool.is_empty()`) for the genuinely-empty case — exercised directly
//!   across every policy branch (proceed on a phase that drained with
//!   completed / failed / all-skipped terminal tasks; proceed on
//!   may_be_empty; proceed on a genuinely-empty undeclared phase that still
//!   leaves real work in the pool — leaf or upstream; fail on residual-work
//!   and on a genuinely-empty undeclared phase that empties the run).
//! - `fire_initial_phase_starts` EMITs `PhaseStartedNeedsWorkers` for
//!   each newly-started phase that carries work; the emit is asserted by
//!   installing a worker-management sender and draining the channel
//!   non-blockingly.

use super::*;

use dynrunner_core::{PhaseId, TaskDep, TypeId};

use crate::worker_signal::WorkerMgmtSignal;

/// Build a `TaskInfo` with an explicit phase + dependency list. Mirrors
/// the hydration-test fixture so the pool seed exercises the same
/// dep-resolution path.
fn dep_binary(name: &str, phase: &str, depends_on: &[&str]) -> TaskInfo<TestId> {
    let mut t = make_binary(name, 100);
    t.phase_id = PhaseId::from(phase);
    t.type_id = TypeId::from("default");
    t.task_depends_on = depends_on
        .iter()
        .map(|d| TaskDep {
            task_id: (*d).to_string(),
            phase_id: PhaseId::from(phase),
            inherit_outputs: false,
            def_id: None,
        })
        .collect();
    t
}

fn make_primary() -> (
    PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
    PrimaryMeshKeepalive,
) {
    // The channel ends are unused by these synchronous tests (no
    // transport I/O is driven); dropping them is harmless.
    let (transport, _ends) = setup_test(1);
    build_test_primary(
        PrimaryConfig::default(),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    )
}

/// Build a task with an explicit phase + caller-chosen `task_id` and a
/// list of fully-qualified `(dep_phase, dep_task_id)` deps so
/// cross-phase identity can be expressed.
fn cross_binary(phase: &str, id: &str, deps: &[(&str, &str)]) -> TaskInfo<TestId> {
    let mut t = make_binary(id, 100);
    t.phase_id = PhaseId::from(phase);
    t.task_id = id.to_string();
    t.task_depends_on = deps
        .iter()
        .map(|(dp, dt)| TaskDep {
            task_id: (*dt).to_string(),
            phase_id: PhaseId::from(*dp),
            inherit_outputs: false,
            def_id: None,
        })
        .collect();
    t
}

/// SITE A (distributed seed): the initial batch carrying the SAME
/// `task_id` in two DIFFERENT phases is valid per `partition_ingest`
/// (full `(phase_id, task_id)` identity) and `originate_cold_seed` must
/// NOT false-abort the run. Pre-fix `extend`'s bare-`task_id` dedup
/// rejected the batch, surfacing a false `RunError` from the otherwise-
/// successful seed. Post-F1, the cold seed lands the batch in the CRDT
/// and `hydrate_from_cluster_state` builds the pool / `total_tasks`.
#[test]
fn cold_seed_cross_phase_same_task_id_is_not_a_duplicate() {
    let (mut primary, _mesh) = make_primary();

    let batch = vec![
        (cross_binary("phaseA", "shared", &[]), false),
        (cross_binary("phaseB", "shared", &[]), false),
    ];
    primary
        .originate_cold_seed(batch, HashMap::new())
        .expect("cross-phase same task_id must NOT abort the cold seed");
    // The seed lands in the CRDT; hydrate is the sole pool / total_tasks
    // builder.
    primary.hydrate_from_cluster_state()
        .expect("test fixture: composed task graph is valid");

    assert!(
        primary.pending_run_abort.is_none(),
        "no #3a duplicate abort should be recorded for a cross-phase same task_id"
    );
    assert_eq!(
        primary.cluster_state_for_test().task_count(),
        2,
        "both cross-phase tasks seeded into the CRDT"
    );
    assert_eq!(
        primary.total_tasks, 2,
        "both cross-phase tasks counted as valid (hydrate-derived)"
    );
    assert_eq!(primary.pool().len(), 2, "both tasks landed in the pool");
}

/// PROCEED when the phase drained with tasks that all reached a terminal
/// state, at least one of them a completion (with a failed sibling). The
/// decision is now ledger-derived (`has_any && !has_live`), so the test
/// seeds the terminal ledger entries rather than passing counter args.
#[test]
fn phase_can_proceed_when_some_completed() {
    let (mut primary, _mesh) = make_primary();

    // One Completed + one Failed item in `compile`: the phase has tasks and
    // every one is terminal ⇒ has_any && !has_live ⇒ proceed.
    let ok = dep_binary("ok", "compile", &[]);
    let bad = dep_binary("bad", "compile", &[]);
    {
        let cs = primary.cluster_state_mut_for_test();
        cs.apply(ClusterMutation::TaskAdded {
            hash: "ok".into(),
            task: ok,
            def_id: None,
        });
        cs.apply(ClusterMutation::TaskCompleted {
            attempt: 0,
            hash: "ok".into(),
            result_data: None,
        });
        cs.apply(ClusterMutation::TaskAdded {
            hash: "bad".into(),
            task: bad,
            def_id: None,
        });
        cs.apply(ClusterMutation::TaskFailed {
            attempt: 0,
            hash: "bad".into(),
            kind: dynrunner_core::ErrorType::NonRecoverable,
            error: "x".into(),
            version: Default::default(),
        });
    }
    primary.hydrate_from_cluster_state()
        .expect("test fixture: composed task graph is valid");

    let p = PhaseId::from("compile");
    assert!(primary.phase_can_proceed(&p));
}

/// PROCEED when the phase's items reached a terminal FAILED outcome with
/// no completion. By this point the retry buckets have exhausted, so the
/// failures are PERMANENT and recorded; the canonical contract advances
/// the phase and surfaces the failures in the outcome summary rather than
/// aborting the run (see `retry_bucket` budget-exhausted branch). The
/// phase has tasks and every one is terminal ⇒ has_any && !has_live ⇒
/// proceed, derived from the seeded ledger.
#[test]
fn phase_can_proceed_when_all_items_failed_terminally() {
    let (mut primary, _mesh) = make_primary();

    for name in ["f1", "f2"] {
        let item = dep_binary(name, "compile", &[]);
        let cs = primary.cluster_state_mut_for_test();
        cs.apply(ClusterMutation::TaskAdded {
            hash: name.into(),
            task: item,
            def_id: None,
        });
        cs.apply(ClusterMutation::TaskFailed {
            attempt: 0,
            hash: name.into(),
            kind: dynrunner_core::ErrorType::NonRecoverable,
            error: "boom".into(),
            version: Default::default(),
        });
    }
    primary.hydrate_from_cluster_state()
        .expect("test fixture: composed task graph is valid");

    let p = PhaseId::from("compile");
    assert!(primary.phase_can_proceed(&p));
}

/// FAIL the genuine wedge: a phase that still owns a LIVE (non-terminal)
/// task yet has no terminal-drained tasks — advancing would strand
/// dependents on never-resolved inputs. We seed a single Pending item for
/// the phase so the rollup reads `has_live` and `phase_min_workers` reports
/// residual work, then assert the veto.
#[test]
fn phase_cannot_proceed_with_residual_unresolved_work() {
    let (mut primary, _mesh) = make_primary();

    // Seed one Pending item in a zero-dep phase so it hydrates Active
    // with residual work and no terminal accounting.
    let only = dep_binary("only", "compile", &[]);
    {
        let cs = primary.cluster_state_mut_for_test();
        cs.apply(ClusterMutation::TaskAdded {
            hash: "only".into(),
            task: only,
            def_id: None,
        });
    }
    primary.hydrate_from_cluster_state()
        .expect("test fixture: composed task graph is valid");

    let p = PhaseId::from("compile");
    // Residual pending item ⇒ the phase has a LIVE task (has_live) ⇒ it is
    // NOT the all-terminal first arm ⇒ the residual `phase_min_workers > 0`
    // guard vetoes.
    assert!(!primary.phase_can_proceed(&p));
}

/// F-honesty PROCEED — empty UPSTREAM phase with a blocked dependent (the
/// asm-dataset `build_compilers` shape): an activated phase that drained
/// genuinely empty — zero completed, zero failed, zero skipped, zero
/// residual — and was NOT declared `may_be_empty`, BUT the pool still owns
/// real work blocked on this phase reaching `Done`. Here `build_compilers`
/// is empty (the producer omitted `--build-compilers`; toolchains are
/// pre-staged) while `matrix_eval` carries a real item BLOCKED on it. The
/// empty drain stranded NOTHING — marking `build_compilers` done is the very
/// gate that unblocks `matrix_eval` — so the guard must PROCEED.
///
/// REVERT-CHECK: under the prior unconditional `false` for a genuinely-empty
/// undeclared phase, this asserted VETO and aborted the run at phase 1 (the
/// consumer-confirmed over-fire, e1721c5c / run_20260609_055517). The pool
/// being non-empty (a blocked dependent) is the discriminator that flips it
/// to proceed.
#[test]
fn empty_upstream_phase_with_blocked_dependent_proceeds() {
    let (mut primary, _mesh) = make_primary();

    // Phase graph: matrix_eval depends on build_compilers. Seed ONE pending
    // `matrix_eval` item so the pool carries real work BLOCKED on the empty
    // `build_compilers` phase — but seed NO `build_compilers` item (the
    // legitimate pre-staged-toolchain case, not a suppressed-injection bug).
    let dep = dep_binary("eval-item", "matrix_eval", &[]);
    {
        let cs = primary.cluster_state_mut_for_test();
        cs.apply(ClusterMutation::PhaseDepsSet {
            deps: HashMap::from([(
                PhaseId::from("matrix_eval"),
                vec![PhaseId::from("build_compilers")],
            )]),
        });
        cs.apply(ClusterMutation::TaskAdded {
            hash: "eval-item".into(),
            task: dep,
            def_id: None,
        });
    }
    primary.hydrate_from_cluster_state()
        .expect("test fixture: composed task graph is valid");

    let build_compilers = PhaseId::from("build_compilers");
    // No `build_compilers` ledger entry ⇒ no rollup ⇒ the `_` fallback; zero
    // residual (`phase_min_workers == 0`), not declared may_be_empty ⇒
    // genuinely-empty. BUT the pool is non-empty (the blocked `matrix_eval`
    // item) ⇒ the run still owns real work this phase's `Done` will unblock
    // ⇒ PROCEED.
    assert_eq!(primary.pool().in_flight(&build_compilers), 0);
    assert!(
        !primary.pool().is_empty(),
        "the blocked matrix_eval dependent is real outstanding work"
    );
    assert!(primary.phase_can_proceed(&build_compilers));
}

/// F-honesty PROCEED — empty LEAF phase while real work remains elsewhere:
/// the same outstanding-work discriminator with the empty phase a LEAF
/// (nothing depends on it). `tail` drains empty and undeclared, but another
/// phase still owns a pending item, so the run is not completing-having-done-
/// nothing → PROCEED. Pins that the discriminator is outstanding work, not
/// topology: a leaf empty phase is no more a wedge than an upstream one when
/// real work remains.
///
/// I2 INTERLEAVE: tail's formal-complete predicate (`phase_boundary_open`)
/// also requires its predecessor `work` to have its `PhaseEnded` applied,
/// regardless of the F-honesty branch — the empty drain proceeding through
/// the may-be-empty / pool-non-empty discriminator no longer skips the
/// strict boundary check (the V-A5 fix). The test seeds `PhaseEnded(work)`
/// before asserting proceed; the assertion before the seed pins the
/// invariant — an empty-leaf phase whose predecessor has not formally ended
/// is held, not proceeded.
#[test]
fn empty_leaf_phase_proceeds_when_work_remains_elsewhere() {
    let (mut primary, _mesh) = make_primary();

    // `tail` is a LEAF (nothing depends on it) and drains empty. An
    // independent `work` phase owns a pending item, so the pool is non-empty.
    let work_item = dep_binary("work_item", "work", &[]);
    {
        let cs = primary.cluster_state_mut_for_test();
        cs.apply(ClusterMutation::PhaseDepsSet {
            deps: HashMap::from([(PhaseId::from("tail"), vec![PhaseId::from("work")])]),
        });
        cs.apply(ClusterMutation::TaskAdded {
            hash: "work_item".into(),
            task: work_item,
            def_id: None,
        });
    }
    primary.hydrate_from_cluster_state()
        .expect("test fixture: composed task graph is valid");

    let tail = PhaseId::from("tail");
    assert!(
        !primary.pool().is_empty(),
        "the pending `work` item is real outstanding work"
    );
    // Boundary closed: predecessor `work` has not yet formally ended,
    // so the strict I2 invariant holds tail back regardless of the
    // F-honesty branch underneath. Closes V-A5.
    assert!(
        !primary.phase_can_proceed(&tail),
        "I2: empty-leaf must not formally complete before predecessor's PhaseEnded"
    );
    // Land the authoritative end edge for `work` → tail's boundary
    // opens → F-honesty proceed re-evaluates → PROCEED.
    primary
        .cluster_state_mut_for_test()
        .apply(ClusterMutation::PhaseEnded {
            phase: PhaseId::from("work"),
        });
    assert!(primary.phase_can_proceed(&tail));
}

/// F-honesty FAIL — the genuine SILENT PARTIAL SUCCESS: an activated phase
/// drains genuinely empty (zero completed/failed/skipped/residual), is NOT
/// declared `may_be_empty`, AND leaves the pool with NO outstanding real work
/// anywhere. The run would complete clean rc=0 having produced nothing — the
/// `on_phase_end`-driven injection (or discovery) that should have populated
/// this phase was suppressed, so its planned tasks never entered the pool.
/// MUST veto. This is the original catch the discriminator preserves;
/// REVERT-CHECK against the proceed tests above is the non-empty pool.
#[test]
fn empty_phase_with_no_work_remaining_fails_loud() {
    let (mut primary, _mesh) = make_primary();

    // Declare a single zero-dep phase and seed NO items: the suppressed-
    // injection bug where the phase's planned work never entered the pool and
    // nothing else is outstanding. Hydrate builds an empty pool with `build`
    // Active-and-empty.
    {
        let cs = primary.cluster_state_mut_for_test();
        cs.apply(ClusterMutation::PhaseDepsSet {
            deps: HashMap::from([(PhaseId::from("build"), vec![])]),
        });
    }
    primary.hydrate_from_cluster_state()
        .expect("test fixture: composed task graph is valid");

    let build = PhaseId::from("build");
    // No `build` ledger entry ⇒ no rollup ⇒ `_` fallback; empty pool, not
    // may_be_empty ⇒ the run would finish having done nothing ⇒ veto, fail
    // loud.
    assert!(
        primary.pool().is_empty(),
        "no real work anywhere — the genuine silent-partial-success"
    );
    assert!(!primary.phase_can_proceed(&build));
}

/// F-honesty PROCEED — declared `may_be_empty`: an activated phase that
/// drained genuinely empty (zero completed/failed/skipped/residual) but the
/// consumer DECLARED it `PhaseSpec.may_be_empty` (a pure sequencing gate /
/// terminal-empty phase). The explicit opt-out the owner's "fail loud BY
/// DEFAULT" implies — proceed, NOT fail. Asserted for BOTH a non-leaf and a
/// leaf empty phase to pin that the opt-out is topology-independent.
#[test]
fn declared_may_be_empty_phase_proceeds_when_empty() {
    let (mut primary, _mesh) = make_primary();

    // `gate` is non-leaf (`work` depends on it); `tail` is a leaf. Both are
    // declared may_be_empty and seeded with no items. Seed one `work` item
    // so the pool hydrates.
    let work_item = dep_binary("work_item", "work", &[]);
    {
        let cs = primary.cluster_state_mut_for_test();
        cs.apply(ClusterMutation::PhaseDepsSet {
            deps: HashMap::from([
                (PhaseId::from("work"), vec![PhaseId::from("gate")]),
                (PhaseId::from("tail"), vec![PhaseId::from("work")]),
            ]),
        });
        cs.apply(ClusterMutation::PhaseMayBeEmptySet {
            phases: vec![PhaseId::from("gate"), PhaseId::from("tail")],
        });
        cs.apply(ClusterMutation::TaskAdded {
            hash: "work_item".into(),
            task: work_item,
            def_id: None,
        });
    }
    primary.hydrate_from_cluster_state()
        .expect("test fixture: composed task graph is valid");

    let gate = PhaseId::from("gate");
    let tail = PhaseId::from("tail");
    assert!(primary.cluster_state_for_test().phase_may_be_empty(&gate));
    assert!(primary.cluster_state_for_test().phase_may_be_empty(&tail));
    // `gate` has no declared deps in the dep graph ⇒ boundary open
    // vacuously ⇒ proceeds on the may_be_empty branch.
    assert!(primary.phase_can_proceed(&gate));
    // `tail` depends on `work`, whose item is still pending ⇒ work's
    // PhaseEnded has not fired ⇒ `tail`'s boundary is closed regardless
    // of the may_be_empty opt-out. I2 holds strictly: the
    // may_be_empty branch is for the empty-DRAIN policy, not a
    // relaxation of the formal-complete boundary. Closes V-A5.
    assert!(
        !primary.phase_can_proceed(&tail),
        "I2: may_be_empty does NOT relax the strict boundary; tail must wait for PhaseEnded(work)"
    );
    // Land work's authoritative end edge → tail's boundary opens →
    // may_be_empty + boundary-open ⇒ proceed.
    primary
        .cluster_state_mut_for_test()
        .apply(ClusterMutation::PhaseEnded {
            phase: PhaseId::from("work"),
        });
    assert!(primary.phase_can_proceed(&tail));
}

/// PROCEED — all items SKIPPED-AS-EXISTING: a phase whose items were ALL
/// skipped because their outputs already exist (the `--skip-existing`
/// "nothing left to do" case) is SUCCESS, not a failure — distinct from the
/// never-injected wedge. The skipped items are now REAL terminal ledger
/// entries (`TaskState::SkippedAlreadyDone`), so the phase reads `has_any &&
/// !has_live` and proceeds STRUCTURALLY (no special skip-count branch),
/// even though it is undeclared and non-leaf.
#[test]
fn phase_all_skipped_as_existing_proceeds() {
    let (mut primary, _mesh) = make_primary();

    // Non-leaf topology (compile depends on build); `build` seeded with two
    // items that are BOTH skipped-already-done, and NOT declared
    // may_be_empty — so success here is owed purely to the all-terminal
    // ledger state, not a may_be_empty opt-out.
    let dep = dep_binary("dep", "compile", &[]);
    let s1 = dep_binary("s1", "build", &[]);
    let s2 = dep_binary("s2", "build", &[]);
    {
        let cs = primary.cluster_state_mut_for_test();
        cs.apply(ClusterMutation::PhaseDepsSet {
            deps: HashMap::from([(PhaseId::from("compile"), vec![PhaseId::from("build")])]),
        });
        cs.apply(ClusterMutation::TaskAdded {
            hash: "dep".into(),
            task: dep,
            def_id: None,
        });
        // Seed each build item Pending FIRST, then transition to
        // SkippedAlreadyDone — the same originate-Pending-then-skip pattern
        // the discovery seed seam uses.
        for (h, task) in [("s1", s1), ("s2", s2)] {
            cs.apply(ClusterMutation::TaskAdded {
                hash: h.into(),
                task,
                def_id: None,
            });
            assert_eq!(
                cs.apply(ClusterMutation::TaskSkippedAlreadyDone { hash: h.into() }),
                crate::cluster_state::ApplyOutcome::Applied,
                "Pending → SkippedAlreadyDone applies"
            );
        }
    }
    primary.hydrate_from_cluster_state()
        .expect("test fixture: composed task graph is valid");

    let build = PhaseId::from("build");
    assert!(
        !primary.cluster_state_for_test().phase_may_be_empty(&build),
        "success owed to the all-terminal skip ledger, NOT a may_be_empty declaration"
    );
    assert_eq!(
        primary
            .cluster_state_for_test()
            .phase_task_partition(&build),
        crate::cluster_state::PhaseTaskPartition {
            to_run: 0,
            done: 0,
            failed: 0,
            skipped: 2,
        },
        "both build items are SkippedAlreadyDone (0 to-run, 2 skipped)"
    );
    // The phase has tasks and every one is terminal (skipped) ⇒ has_any &&
    // !has_live ⇒ proceed, structurally.
    assert!(primary.phase_can_proceed(&build));
}

/// `fire_initial_phase_starts` emits `PhaseStartedNeedsWorkers { min: 1 }`
/// for a newly-started phase that carries pending work, and emits nothing
/// further when re-run (idempotent: only newly-inserted phases emit).
#[test]
fn fire_initial_phase_starts_emits_needs_workers_for_phase_with_work() {
    let (mut primary, _mesh) = make_primary();

    // Seed a completed `build`-phase prereq plus two `compile`-phase
    // dependents so `compile` hydrates as Active-with-items.
    let toolchain = dep_binary("toolchain", "build", &[]);
    let dep_a = dep_binary("dep-a", "compile", &["toolchain"]);
    let dep_b = dep_binary("dep-b", "compile", &["toolchain"]);
    {
        let cs = primary.cluster_state_mut_for_test();
        cs.apply(ClusterMutation::PhaseDepsSet {
            deps: HashMap::from([(PhaseId::from("compile"), vec![PhaseId::from("build")])]),
        });
        cs.apply(ClusterMutation::TaskAdded {
            hash: "toolchain".into(),
            task: toolchain,
            def_id: None,
        });
        cs.apply(ClusterMutation::TaskCompleted {
            attempt: 0,
            hash: "toolchain".into(),
            result_data: None,
        });
        cs.apply(ClusterMutation::TaskAdded {
            hash: "dep-a".into(),
            task: dep_a,
            def_id: None,
        });
        cs.apply(ClusterMutation::TaskAdded {
            hash: "dep-b".into(),
            task: dep_b,
            def_id: None,
        });
    }
    primary.hydrate_from_cluster_state()
        .expect("test fixture: composed task graph is valid");
    // `hydrate_from_cluster_state` no longer self-drains empty phases (the
    // coordinator owns the narrated cascade at run-entry). Drain the
    // completed-only `build` phase here so its `compile` dependent unblocks
    // to Active — the same cascade `run_pipeline`'s pre-loop performs before
    // `fire_initial_phase_starts`.
    crate::secondary::origination::cascade_drain_done(primary.pool_mut());
    // Seed the authoritative end-edge fact for `build` so `compile`'s
    // formal-start boundary is open (strict I1). The live pre-loop cascade
    // (`process_phase_lifecycle`) originates `PhaseEnded` at the same
    // decision point as `mark_phase_done`; the silent `cascade_drain_done`
    // path here skips that origination, so the test seeds it directly to
    // mirror the wire fact `fire_initial_phase_starts` now consults.
    primary
        .cluster_state_mut_for_test()
        .apply(ClusterMutation::PhaseEnded {
            phase: PhaseId::from("build"),
        });

    // Install the worker-management bus sender, then fire phase starts.
    let (tx, mut rx) = tokio_mpsc::unbounded_channel::<WorkerMgmtSignal>();
    primary
        .cluster_state_mut_for_test()
        .install_worker_mgmt_sender(tx);

    primary.fire_initial_phase_starts();

    // Exactly one signal: `compile` started with min == 1 (it has work);
    // `build` is already terminal/done so it is not a newly-active phase.
    let first = rx.try_recv().expect("a PhaseStartedNeedsWorkers signal");
    assert_eq!(
        first,
        WorkerMgmtSignal::PhaseStartedNeedsWorkers {
            phase: PhaseId::from("compile"),
            min: 1,
        }
    );
    assert!(
        rx.try_recv().is_err(),
        "only the work-carrying started phase emits"
    );

    // Re-firing is idempotent: `compile` is already in
    // `phase_started_emitted`, so no further signal.
    primary.fire_initial_phase_starts();
    assert!(
        rx.try_recv().is_err(),
        "re-running fire_initial_phase_starts emits nothing for already-started phases"
    );
}

/// `fire_initial_phase_starts` emits the "starting job phase" important
/// event exactly once per newly-started phase (the `phase_started_emitted`
/// insert edge), and re-firing emits nothing — pinning that the
/// phase-start/phase-transition important event tracks the same
/// once-per-phase transition as the worker-management signal.
#[test]
fn fire_initial_phase_starts_emits_one_starting_job_phase_important_event() {
    use crate::test_capture::{ImportantCapture, important_only};
    use tracing::subscriber::with_default;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::{Layer, Registry};

    let (mut primary, _mesh) = make_primary();

    // "build" carries a PENDING task — a genuinely cold/just-active phase
    // that has never started. V3 seeds `phase_started_emitted` from
    // PROGRESSED (InFlight/terminal) tasks only, so an all-`Pending` ledger
    // leaves it EMPTY; `fire_initial_phase_starts` then fires exactly once for
    // the one newly-active work-carrying phase, and the idempotent re-fire
    // emits nothing. (Pre-V3 this seeded a COMPLETED toolchain and pinned a
    // re-fire of the already-completed phase — exactly the resume re-fire V3
    // now correctly suppresses; the once-per-phase emit guard this test pins
    // is unchanged.)
    let toolchain = dep_binary("toolchain", "build", &[]);
    {
        let cs = primary.cluster_state_mut_for_test();
        cs.apply(ClusterMutation::PhaseDepsSet {
            deps: HashMap::new(),
        });
        cs.apply(ClusterMutation::TaskAdded {
            hash: "toolchain".into(),
            task: toolchain,
            def_id: None,
        });
    }
    primary.hydrate_from_cluster_state()
        .expect("test fixture: composed task graph is valid");

    // Worker-management sender so the emit path doesn't drop on a
    // missing sender (orthogonal to the important-event assertion).
    let (tx, _rx) = tokio_mpsc::unbounded_channel::<WorkerMgmtSignal>();
    primary
        .cluster_state_mut_for_test()
        .install_worker_mgmt_sender(tx);

    let capture = ImportantCapture::default();
    let subscriber = Registry::default().with(capture.clone().with_filter(important_only()));
    with_default(subscriber, || {
        primary.fire_initial_phase_starts();
        // Idempotent re-fire emits no further important event.
        primary.fire_initial_phase_starts();
    });

    let msgs = capture.messages();
    assert_eq!(
        msgs.len(),
        1,
        "exactly one starting-job-phase important event for the one newly-active \
         work-carrying phase: {msgs:?}"
    );
    assert!(msgs[0].contains("starting job phase"), "{msgs:?}");
}

// ── #584 phase-boundary tests ──
//
// The single phase-boundary predicate (`phase_boundary_open`) is now
// consulted by `phase_can_proceed` (formal-complete I2) and
// `fire_initial_phase_starts` (formal-start I1). The narrator's gates
// consult the same predicate (covered in run_narrator tests). The four
// tests below pin the LATENT-violation races the audit identified.

/// V-A2 race: `matrix_eval` (depends on `build`) drains its tasks FAST
/// while `build` is still draining its own. Pre-fix `phase_can_proceed`
/// returned true on the `has_any && !has_live` arm with NO predecessor
/// `PhaseEnded` check, so `matrix_eval`'s PhaseEnded could land BEFORE
/// `build`'s, racing `mark_phase_done`. With the predicate at the
/// function-entry gate, `phase_can_proceed(matrix_eval)` returns FALSE
/// until `PhaseEnded(build)` is applied, then flips to TRUE.
#[test]
fn phase_can_proceed_holds_until_predecessor_ended() {
    let (mut primary, _mesh) = make_primary();

    let build = dep_binary("b1", "build", &[]);
    let eval = dep_binary("e1", "matrix_eval", &[]);
    {
        let cs = primary.cluster_state_mut_for_test();
        cs.apply(ClusterMutation::PhaseDepsSet {
            deps: HashMap::from([(
                PhaseId::from("matrix_eval"),
                vec![PhaseId::from("build")],
            )]),
        });
        cs.apply(ClusterMutation::TaskAdded {
            hash: "b1".into(),
            task: build,
            def_id: None,
        });
        cs.apply(ClusterMutation::TaskAdded {
            hash: "e1".into(),
            task: eval,
            def_id: None,
        });
        // matrix_eval's task completes FAST; build's task is still live.
        cs.apply(ClusterMutation::TaskCompleted {
            attempt: 0,
            hash: "e1".into(),
            result_data: None,
        });
    }
    primary.hydrate_from_cluster_state()
        .expect("test fixture: composed task graph is valid");

    let matrix_eval = PhaseId::from("matrix_eval");
    let build_phase = PhaseId::from("build");
    // matrix_eval rolls up has_any && !has_live (its one task completed),
    // but its boundary is still closed — build's PhaseEnded has not fired.
    assert!(
        !primary.phase_can_proceed(&matrix_eval),
        "I2: matrix_eval must wait for PhaseEnded(build); V-A2 race"
    );

    // Apply the authoritative end edge for build → matrix_eval's boundary
    // opens → proceed-or-fail re-evaluates → PROCEED.
    primary
        .cluster_state_mut_for_test()
        .apply(ClusterMutation::PhaseEnded {
            phase: build_phase,
        });
    assert!(primary.phase_can_proceed(&matrix_eval));
}

/// V-A1 init scenario: a `barrier=False` phase `P` (depends on `Q`) is
/// pre-flipped Active by `set_no_barrier_phases` at pool construction
/// (the I3 dispatch-authorization). Pre-fix `fire_initial_phase_starts`
/// iterated `pool.active_phases()` and emitted "starting job phase" +
/// the `on_phase_start` callback for `P` immediately — BEFORE `Q`'s
/// `PhaseEnded` could fire. With the predicate gate, the call skips `P`
/// while the boundary is closed and the post-`mark_phase_done` cascade
/// re-fires it once `PhaseEnded(Q)` lands.
#[test]
fn fire_initial_phase_starts_skips_barrier_false_phase_until_predecessor_ended() {
    use crate::test_capture::{ImportantCapture, important_only};
    use tracing::subscriber::with_default;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::{Layer, Registry};

    let (mut primary, _mesh) = make_primary();

    // Phase graph: matrix_eval depends on build; matrix_eval is barrier=False
    // (the consumer opt-in for early task dispatch). Both have one task.
    let build = dep_binary("b1", "build", &[]);
    let eval = dep_binary("e1", "matrix_eval", &[]);
    {
        let cs = primary.cluster_state_mut_for_test();
        cs.apply(ClusterMutation::PhaseDepsSet {
            deps: HashMap::from([(
                PhaseId::from("matrix_eval"),
                vec![PhaseId::from("build")],
            )]),
        });
        cs.apply(ClusterMutation::PhaseNoBarrierSet {
            phases: vec![PhaseId::from("matrix_eval")],
        });
        cs.apply(ClusterMutation::TaskAdded {
            hash: "b1".into(),
            task: build,
            def_id: None,
        });
        cs.apply(ClusterMutation::TaskAdded {
            hash: "e1".into(),
            task: eval,
            def_id: None,
        });
    }
    primary.hydrate_from_cluster_state()
        .expect("test fixture: composed task graph is valid");

    // Worker-mgmt sender so the emit path does not drop.
    let (tx, _rx) = tokio_mpsc::unbounded_channel::<WorkerMgmtSignal>();
    primary
        .cluster_state_mut_for_test()
        .install_worker_mgmt_sender(tx);

    // First fire: build's boundary is open (no deps); matrix_eval's is
    // closed (depends on build, no PhaseEnded yet). Only build narrates.
    let capture = ImportantCapture::default();
    let subscriber = Registry::default().with(capture.clone().with_filter(important_only()));
    with_default(subscriber, || {
        primary.fire_initial_phase_starts();
    });
    let phases: Vec<String> = capture
        .events()
        .into_iter()
        .filter(|e| e.message.contains("starting job phase"))
        .filter_map(|e| e.fields.get("phase").cloned())
        .collect();
    assert_eq!(
        phases,
        vec!["build".to_string()],
        "V-A1: matrix_eval (barrier=False) must NOT formally start before \
         PhaseEnded(build); only build narrates here, even though the pool \
         pre-flipped matrix_eval Active for I3 task dispatch"
    );

    // Land PhaseEnded(build) → matrix_eval's boundary opens → the next
    // fire (the post-`mark_phase_done` cascade) emits its start line.
    primary
        .cluster_state_mut_for_test()
        .apply(ClusterMutation::PhaseEnded {
            phase: PhaseId::from("build"),
        });
    let capture2 = ImportantCapture::default();
    let subscriber2 = Registry::default().with(capture2.clone().with_filter(important_only()));
    with_default(subscriber2, || {
        primary.fire_initial_phase_starts();
    });
    let phases2: Vec<String> = capture2
        .events()
        .into_iter()
        .filter(|e| e.message.contains("starting job phase"))
        .filter_map(|e| e.fields.get("phase").cloned())
        .collect();
    assert_eq!(
        phases2,
        vec!["matrix_eval".to_string()],
        "matrix_eval narrates AFTER its predecessor's PhaseEnded"
    );
}

/// V-A5 empty-phase: a phase `P` (no tasks, depends on `Q`) cannot
/// `phase_can_proceed` on the `may_be_empty` / outstanding-work branch
/// before `Q`'s `PhaseEnded`. Pre-fix the boundary check was missing on
/// this branch, so an empty `P` whose dep was still draining could
/// false-complete. The function-entry boundary gate now closes that path:
/// the may_be_empty branch is reached only when the boundary is open.
#[test]
fn empty_phase_holds_proceed_until_predecessor_ended_v_a5() {
    let (mut primary, _mesh) = make_primary();

    // P depends on Q; P has zero tasks; Q has one PENDING task (still
    // live). Real work outstanding (`!pool.is_empty()`), so the pre-fix
    // F-honesty branch would have proceeded.
    let q_item = dep_binary("q1", "q", &[]);
    {
        let cs = primary.cluster_state_mut_for_test();
        cs.apply(ClusterMutation::PhaseDepsSet {
            deps: HashMap::from([(PhaseId::from("p"), vec![PhaseId::from("q")])]),
        });
        cs.apply(ClusterMutation::TaskAdded {
            hash: "q1".into(),
            task: q_item,
            def_id: None,
        });
    }
    primary.hydrate_from_cluster_state()
        .expect("test fixture: composed task graph is valid");

    let p = PhaseId::from("p");
    assert!(
        !primary.pool().is_empty(),
        "Q's pending item keeps the pool non-empty (V-A5 setup)"
    );
    // Pre-fix this returned true via the outstanding-work branch even
    // though Q's PhaseEnded had not fired. With V-A5 closed: HOLD.
    assert!(
        !primary.phase_can_proceed(&p),
        "V-A5: an empty phase must not formally complete before predecessor's PhaseEnded"
    );

    // Land PhaseEnded(Q) → P's boundary opens → outstanding-work branch
    // is reached and proceeds (Q's task is still in the pool — non-empty).
    primary
        .cluster_state_mut_for_test()
        .apply(ClusterMutation::PhaseEnded {
            phase: PhaseId::from("q"),
        });
    assert!(primary.phase_can_proceed(&p));
}

/// I3 preserved: a barrier=False phase's TASKS are still authorized for
/// early dispatch by the runtime-spawn interlock (the pool reports the
/// phase Active and the per-task `task_depends_on` graph gates per-task
/// readiness). The phase-boundary predicate gates only the FORMAL
/// boundary; it must not affect the pool's `Active` state nor the
/// per-task readiness path.
#[test]
fn barrier_false_phase_tasks_dispatchable_before_predecessor_ended() {
    let (mut primary, _mesh) = make_primary();

    // matrix_eval (barrier=False) depends on build. matrix_eval has one
    // task with NO task-level deps (so per-task readiness is satisfied
    // immediately). build has one pending task (predecessor still live).
    let build = dep_binary("b1", "build", &[]);
    let eval = dep_binary("e1", "matrix_eval", &[]);
    {
        let cs = primary.cluster_state_mut_for_test();
        cs.apply(ClusterMutation::PhaseDepsSet {
            deps: HashMap::from([(
                PhaseId::from("matrix_eval"),
                vec![PhaseId::from("build")],
            )]),
        });
        cs.apply(ClusterMutation::PhaseNoBarrierSet {
            phases: vec![PhaseId::from("matrix_eval")],
        });
        cs.apply(ClusterMutation::TaskAdded {
            hash: "b1".into(),
            task: build,
            def_id: None,
        });
        cs.apply(ClusterMutation::TaskAdded {
            hash: "e1".into(),
            task: eval,
            def_id: None,
        });
    }
    primary.hydrate_from_cluster_state()
        .expect("test fixture: composed task graph is valid");

    let matrix_eval = PhaseId::from("matrix_eval");
    // The pool reports matrix_eval Active despite build still being live —
    // the I3 set_no_barrier_phases pre-flip, the path the runtime-spawn
    // interlock authorizes early dispatch on. Pinning the pool state here
    // is the I3 invariant the brief requires the boundary fix to
    // PRESERVE — `phase_boundary_open` is the formal-boundary gate, not a
    // dispatch gate.
    let active: std::collections::HashSet<PhaseId> =
        primary.pool().active_phases().into_iter().collect();
    assert!(
        active.contains(&matrix_eval),
        "I3: pool's active_phases still carries the barrier=False phase \
         for early task dispatch, even with its boundary closed"
    );
    // And the cluster_state still records the barrier-set membership —
    // the data the runtime-spawn interlock consults.
    assert!(primary.cluster_state_for_test().phase_no_barrier(&matrix_eval));
}
