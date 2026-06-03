//! Tests for the phase-layer proceed-or-fail decision and the
//! `PhaseStartedNeedsWorkers` emission onto the decoupled
//! worker-management bus.
//!
//! Two concerns, both synchronous and deterministic (no operational
//! loop, no wall-clock waits):
//! - [`phase_can_proceed`] is a pure predicate on the phase's terminal
//!   counters — exercised directly for the three policy branches.
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

fn make_primary() -> PrimaryCoordinator<
    ChannelPeerTransport<TestId>,
    ResourceStealingScheduler,
    FixedEstimator,
    TestId,
> {
    // The channel ends are unused by these synchronous tests (no
    // transport I/O is driven); dropping them is harmless.
    let (transport, _ends) = setup_test(1);
    PrimaryCoordinator::new(
        PrimaryConfig::default(),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    )
}

/// PROCEED when the phase produced at least one completed item, even if
/// some siblings failed.
#[test]
fn phase_can_proceed_when_some_completed() {
    let primary = make_primary();
    let p = PhaseId::from("compile");
    assert!(primary.phase_can_proceed(&p, 3, 0));
    assert!(primary.phase_can_proceed(&p, 1, 2));
}

/// PROCEED when the phase had zero items (empty / cascade-through phase):
/// `completed == 0 && failed == 0` and no residual work. It makes no
/// worker demand and blocks nothing.
#[test]
fn phase_can_proceed_when_zero_items() {
    let primary = make_primary();
    let p = PhaseId::from("empty");
    // No pool item carries phase "empty" and no in-flight counter ⇒
    // phase_min_workers == 0 ⇒ proceed.
    assert!(primary.phase_can_proceed(&p, 0, 0));
}

/// PROCEED when the phase's items reached a terminal FAILED outcome with
/// no completion. By this point the retry buckets have exhausted, so the
/// failures are PERMANENT and recorded; the canonical contract advances
/// the phase and surfaces the failures in the outcome summary rather than
/// aborting the run (see `retry_bucket` budget-exhausted branch).
#[test]
fn phase_can_proceed_when_all_items_failed_terminally() {
    let primary = make_primary();
    let p = PhaseId::from("compile");
    assert!(primary.phase_can_proceed(&p, 0, 1));
    assert!(primary.phase_can_proceed(&p, 0, 5));
}

/// FAIL only the genuine wedge: a phase that produced NO terminal
/// accounting (zero completed, zero failed) yet still owns residual
/// pending work — advancing would strand dependents on never-resolved
/// inputs. We seed a single Pending item for the phase so
/// `phase_min_workers` reports residual work, then assert the veto.
#[test]
fn phase_cannot_proceed_with_residual_unresolved_work() {
    let mut primary = make_primary();

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
    assert!(!primary.phase_can_proceed(&p, 0, 0));
}

/// `fire_initial_phase_starts` emits `PhaseStartedNeedsWorkers { min: 1 }`
/// for a newly-started phase that carries pending work, and emits nothing
/// further when re-run (idempotent: only newly-inserted phases emit).
#[test]
fn fire_initial_phase_starts_emits_needs_workers_for_phase_with_work() {
    let mut primary = make_primary();

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

    // Install the worker-management bus sender, then fire phase starts.
    let (tx, mut rx) = tokio_mpsc::unbounded_channel::<WorkerMgmtSignal>();
    primary.cluster_state_mut_for_test().install_worker_mgmt_sender(tx);

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
    use crate::test_capture::{important_only, ImportantCapture};
    use tracing::subscriber::with_default;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::{Layer, Registry};

    let mut primary = make_primary();

    let toolchain = dep_binary("toolchain", "build", &[]);
    let dep_a = dep_binary("dep-a", "compile", &["toolchain"]);
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
            hash: "toolchain".into(),
            result_data: None,
        });
        cs.apply(ClusterMutation::TaskAdded {
            hash: "dep-a".into(),
            task: dep_a,
        });
    }
    primary.hydrate_from_cluster_state();

    // Worker-management sender so the emit path doesn't drop on a
    // missing sender (orthogonal to the important-event assertion).
    let (tx, _rx) = tokio_mpsc::unbounded_channel::<WorkerMgmtSignal>();
    primary.cluster_state_mut_for_test().install_worker_mgmt_sender(tx);

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
