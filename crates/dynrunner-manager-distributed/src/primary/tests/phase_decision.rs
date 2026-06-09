//! Tests for the phase-layer proceed-or-fail decision and the
//! `PhaseStartedNeedsWorkers` emission onto the decoupled
//! worker-management bus.
//!
//! Two concerns, both synchronous and deterministic (no operational
//! loop, no wall-clock waits):
//! - [`phase_can_proceed`] decides advance-vs-fail from the phase's
//!   terminal counters (completed / failed / skipped-as-existing), its
//!   residual-work probe, and the replicated `may_be_empty` opt-out —
//!   exercised directly across every policy branch (proceed on
//!   completion/failure/skip/may_be_empty; fail on residual-work and on a
//!   genuinely-empty undeclared phase, leaf or non-leaf).
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
        cross_binary("phaseA", "shared", &[]),
        cross_binary("phaseB", "shared", &[]),
    ];
    primary
        .originate_cold_seed(batch, HashMap::new())
        .expect("cross-phase same task_id must NOT abort the cold seed");
    // The seed lands in the CRDT; hydrate is the sole pool / total_tasks
    // builder.
    primary.hydrate_from_cluster_state();

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

/// PROCEED when the phase produced at least one completed item, even if
/// some siblings failed.
#[test]
fn phase_can_proceed_when_some_completed() {
    let (primary, _mesh) = make_primary();
    let p = PhaseId::from("compile");
    assert!(primary.phase_can_proceed(&p, 3, 0, 0));
    assert!(primary.phase_can_proceed(&p, 1, 2, 0));
}

/// PROCEED when the phase's items reached a terminal FAILED outcome with
/// no completion. By this point the retry buckets have exhausted, so the
/// failures are PERMANENT and recorded; the canonical contract advances
/// the phase and surfaces the failures in the outcome summary rather than
/// aborting the run (see `retry_bucket` budget-exhausted branch).
#[test]
fn phase_can_proceed_when_all_items_failed_terminally() {
    let (primary, _mesh) = make_primary();
    let p = PhaseId::from("compile");
    assert!(primary.phase_can_proceed(&p, 0, 1, 0));
    assert!(primary.phase_can_proceed(&p, 0, 5, 0));
}

/// FAIL the genuine wedge: a phase that produced NO terminal accounting
/// (zero completed, zero failed, zero skipped) yet still owns residual
/// pending work — advancing would strand dependents on never-resolved
/// inputs. We seed a single Pending item for the phase so
/// `phase_min_workers` reports residual work, then assert the veto.
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
        });
    }
    primary.hydrate_from_cluster_state();

    let p = PhaseId::from("compile");
    // Residual pending item ⇒ phase_min_workers > 0 ⇒ with zero terminal
    // accounting the phase cannot proceed.
    assert!(!primary.phase_can_proceed(&p, 0, 0, 0));
}

/// F-honesty FAIL — NON-LEAF empty (asm-tokenizer class): an activated
/// phase that drained genuinely empty — zero completed, zero failed, zero
/// skipped, zero residual — and was NOT declared `may_be_empty`. Its
/// planned work was never injected / discovered (the silent
/// partial-success the consumers hit when `on_phase_end`-driven lazy
/// injection was suppressed). MUST veto, regardless of topology. Here
/// `build` is NON-LEAF (`compile` depends on it).
#[test]
fn non_leaf_phase_drained_genuinely_empty_fails_loud() {
    let (mut primary, _mesh) = make_primary();

    // Phase graph: compile depends on build. Seed ONE pending `compile`
    // item so the pool exists and carries the dep edge — but seed NO
    // `build` item, so `build` is a non-leaf phase that was never injected.
    let dep = dep_binary("dep", "compile", &[]);
    {
        let cs = primary.cluster_state_mut_for_test();
        cs.apply(ClusterMutation::PhaseDepsSet {
            deps: HashMap::from([(PhaseId::from("compile"), vec![PhaseId::from("build")])]),
        });
        cs.apply(ClusterMutation::TaskAdded {
            hash: "dep".into(),
            task: dep,
        });
    }
    primary.hydrate_from_cluster_state();

    let build = PhaseId::from("build");
    // No `build` item ⇒ zero residual (`phase_min_workers == 0`). Zero
    // accounting, zero skip, not declared may_be_empty ⇒ genuinely-empty
    // wedge ⇒ veto.
    assert_eq!(primary.pool().in_flight(&build), 0);
    assert!(!primary.phase_can_proceed(&build, 0, 0, 0));
}

/// F-honesty FAIL — LEAF empty (asm-dataset class): the SAME genuinely-empty
/// wedge where the suppressed phase is a LEAF (`…→dependency_graph→build`,
/// nothing depends on `build`). This is the case the prior "leaf empties
/// always proceed" rule MISSED — the discriminator is the absence of a
/// `may_be_empty` declaration, NOT topology, so a leaf empty undeclared
/// phase MUST veto too.
#[test]
fn leaf_phase_drained_genuinely_empty_fails_loud() {
    let (mut primary, _mesh) = make_primary();

    // Phase graph: build depends on dependency_graph. `build` is a LEAF
    // (nothing depends on it). Seed ONE pending `dependency_graph` item so
    // the pool exists — but seed NO `build` item (the suppressed-injection
    // bug: on_phase_end was supposed to spawn `build`'s items and didn't).
    let dg = dep_binary("dg", "dependency_graph", &[]);
    {
        let cs = primary.cluster_state_mut_for_test();
        cs.apply(ClusterMutation::PhaseDepsSet {
            deps: HashMap::from([(
                PhaseId::from("build"),
                vec![PhaseId::from("dependency_graph")],
            )]),
        });
        cs.apply(ClusterMutation::TaskAdded {
            hash: "dg".into(),
            task: dg,
        });
    }
    primary.hydrate_from_cluster_state();

    let build = PhaseId::from("build");
    // LEAF, zero residual, zero accounting, zero skip, not may_be_empty ⇒
    // genuinely-empty wedge ⇒ veto (the asm-dataset class the old leaf-proceed
    // rule false-greened).
    assert_eq!(primary.pool().in_flight(&build), 0);
    assert!(!primary.phase_can_proceed(&build, 0, 0, 0));
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
        });
    }
    primary.hydrate_from_cluster_state();

    let gate = PhaseId::from("gate");
    let tail = PhaseId::from("tail");
    assert!(primary.cluster_state_for_test().phase_may_be_empty(&gate));
    assert!(primary.cluster_state_for_test().phase_may_be_empty(&tail));
    // Both empty + declared may_be_empty ⇒ proceed (non-leaf gate AND leaf
    // tail), even though an UNDECLARED empty phase in the same position fails.
    assert!(primary.phase_can_proceed(&gate, 0, 0, 0));
    assert!(primary.phase_can_proceed(&tail, 0, 0, 0));
}

/// F-honesty PROCEED — all items SKIPPED-AS-EXISTING: a phase that drained
/// empty because ALL its items were skipped because their outputs already
/// exist (the `--skip-existing` "nothing left to do" case) is SUCCESS, not a
/// failure — distinct from the never-injected wedge. The recorded
/// `PhaseTally::SkippedExisting` count is the discriminator; with
/// `skipped > 0` the phase proceeds even though it is empty, undeclared, and
/// non-leaf.
#[test]
fn phase_all_skipped_as_existing_proceeds() {
    let (mut primary, _mesh) = make_primary();

    // Non-leaf topology (compile depends on build), build seeded with no
    // items but a recorded skipped-as-existing count — and NOT declared
    // may_be_empty, so success here is owed purely to the skip count.
    let dep = dep_binary("dep", "compile", &[]);
    {
        let cs = primary.cluster_state_mut_for_test();
        cs.apply(ClusterMutation::PhaseDepsSet {
            deps: HashMap::from([(PhaseId::from("compile"), vec![PhaseId::from("build")])]),
        });
        cs.apply(ClusterMutation::TaskAdded {
            hash: "dep".into(),
            task: dep,
        });
    }
    primary.hydrate_from_cluster_state();

    let build = PhaseId::from("build");
    // Record the skipped-as-existing count via the same grow-only-MAX
    // accessor the discovery originator uses.
    primary.cluster_state_mut_for_test().record_phase_event_tally(
        (build.clone(), crate::cluster_state::PhaseTally::SkippedExisting),
        3,
    );

    let skipped = primary
        .cluster_state_for_test()
        .phase_event_tally_for(&(build.clone(), crate::cluster_state::PhaseTally::SkippedExisting));
    assert_eq!(skipped, 3, "skipped-as-existing count recorded");
    assert!(
        !primary.cluster_state_for_test().phase_may_be_empty(&build),
        "success owed to the skip count, NOT a may_be_empty declaration"
    );
    // Zero accounting, zero residual, undeclared — but skipped > 0 ⇒ proceed.
    assert!(primary.phase_can_proceed(&build, 0, 0, skipped));
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
        });
        cs.apply(ClusterMutation::TaskCompleted {
            attempt: 0,
            hash: "toolchain".into(),
            result_data: None,
        });
        cs.apply(ClusterMutation::TaskAdded {
            hash: "dep-a".into(),
            task: dep_a,
        });
        cs.apply(ClusterMutation::TaskAdded {
            hash: "dep-b".into(),
            task: dep_b,
        });
    }
    primary.hydrate_from_cluster_state();
    // `hydrate_from_cluster_state` no longer self-drains empty phases (the
    // coordinator owns the narrated cascade at run-entry). Drain the
    // completed-only `build` phase here so its `compile` dependent unblocks
    // to Active — the same cascade `run_pipeline`'s pre-loop performs before
    // `fire_initial_phase_starts`.
    crate::secondary::origination::cascade_drain_done(primary.pool_mut());

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
        });
    }
    primary.hydrate_from_cluster_state();

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
