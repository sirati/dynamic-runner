//! Setup-task primitive tests for the replicated ledger (P1 seams (c)
//! dependency-resolution and (d) counters).
//!
//! P1 has NO setup-task executor, so these tests mutate a setup task to
//! its succeeded terminal (`TaskState::SetupCompleted`) DIRECTLY — never
//! by executing it — and pin the model/dep/counter behavior the
//! primitive guarantees:
//!
//!   * a succeeded `Setup` task is terminal for dependency-resolution
//!     and phase-completion (`is_terminal()`), so a dependent's `TaskDep`
//!     resolves against it and the dependent is dispatchable (overlapping);
//!   * a `Blocked`-on-setup dependent auto-resumes to `Pending` through
//!     the same cascade-resume mechanism a completed prereq drives;
//!   * the SEPARATE `setup_succeeded` counter increments while the
//!     worker-work `succeeded` count does NOT.

use super::*;
use dynrunner_core::TaskKind;

/// Build a `TaskKind::Setup` task — `mk_task`'s twin with the kind flipped.
fn mk_setup_task(name: &str) -> TaskInfo<RunnerIdentifier> {
    let mut task = mk_task(name);
    task.kind = TaskKind::Setup;
    task
}

/// A succeeded setup task (`SetupCompleted`) IS terminal for
/// dependency-resolution / phase-completion purposes — the predicate
/// every dep walk and phase rollup shares.
#[test]
fn setup_completed_is_terminal() {
    let state = TaskState::<RunnerIdentifier>::SetupCompleted {
        task: mk_setup_task("s"),
        attempt: 0,
    };
    assert!(
        state.is_terminal(),
        "a succeeded setup task must be terminal so its dependents unblock"
    );
}

/// A dependent spawned AFTER its setup prereq has SUCCEEDED resolves the
/// dep and lands `Pending` (dispatchable) — overlapping, exactly as a
/// dependent of a `Completed` prereq would. (Seam (c), the spawn-time
/// dep classifier in `apply_tasks_spawned`.)
#[test]
fn dependent_of_succeeded_setup_task_is_dispatchable() {
    let mut s = ClusterState::<RunnerIdentifier>::new();

    // The setup task succeeded (set DIRECTLY — P1 has no executor).
    let setup = mk_setup_task("setup");
    let setup_hash = crate::primary::wire::compute_task_hash(&setup);
    s.tasks.insert(
        setup_hash.clone(),
        TaskState::SetupCompleted {
            task: setup,
            attempt: 0,
        },
    );

    // A build task depends on it (same phase p0; `mk_task` uses p0).
    let mut build = mk_task("build");
    build.task_depends_on = vec![dynrunner_core::TaskDep {
        task_id: "setup".into(),
        phase_id: PhaseId::from("p0"),
        inherit_outputs: false,
    }];
    let build_hash = crate::primary::wire::compute_task_hash(&build);
    s.apply(ClusterMutation::TasksSpawned { tasks: vec![build] });

    // The dependent resolved its dep against the succeeded setup task and
    // is dispatchable (Pending), NOT Blocked.
    match s.task_state(&build_hash) {
        Some(TaskState::Pending { .. }) => {}
        other => panic!(
            "dependent of a succeeded setup task must be Pending (dispatchable), got {other:?}"
        ),
    }
}

/// A dependent that was already `Blocked` on a setup task auto-resumes
/// to `Pending` when the setup task's hash drives the cascade-resume —
/// the same mechanism a completed prereq uses. (Seam (c), the
/// `resume_blocked_on` cascade the terminal-success transition fires.)
#[test]
fn blocked_dependent_resumes_when_setup_succeeds() {
    let mut s = ClusterState::<RunnerIdentifier>::new();

    // Setup task pending; a dependent Blocked on it.
    let setup = mk_setup_task("setup");
    let setup_hash = crate::primary::wire::compute_task_hash(&setup);
    s.tasks.insert(
        setup_hash.clone(),
        TaskState::Pending {
            task: setup,
            version: Default::default(),
            attempt: 0,
        },
    );
    let dependent = mk_task("dependent");
    let dep_hash = crate::primary::wire::compute_task_hash(&dependent);
    s.tasks.insert(
        dep_hash.clone(),
        TaskState::Blocked {
            task: dependent,
            on: setup_hash.clone(),
            attempt: 0,
        },
    );

    // The setup task succeeds (set directly), then the cascade-resume
    // runs for its hash — exactly what the executor (P2) will drive.
    s.tasks.insert(
        setup_hash.clone(),
        TaskState::SetupCompleted {
            task: mk_setup_task("setup"),
            attempt: 0,
        },
    );
    let resumed = s.resume_blocked_on(&setup_hash);

    assert_eq!(resumed.len(), 1, "the blocked dependent is resumed");
    assert_eq!(resumed[0].task_id, "dependent");
    match s.task_state(&dep_hash) {
        Some(TaskState::Pending { .. }) => {}
        other => panic!("the dependent must auto-resume to Pending, got {other:?}"),
    }
}

/// A succeeded setup task increments the SEPARATE `setup_succeeded`
/// bucket and does NOT inflate the worker-work `succeeded` count — the
/// counter contract of the primitive (seam (d)). It IS a terminal
/// outcome, so it is included in `total_terminal()`.
#[test]
fn setup_succeeded_counter_disjoint_from_succeeded() {
    let mut s = ClusterState::<RunnerIdentifier>::new();

    // One ordinary completed WORK task ...
    let work = mk_task("work");
    let work_hash = crate::primary::wire::compute_task_hash(&work);
    s.tasks.insert(
        work_hash,
        TaskState::Completed {
            task: work,
            attempt: 0,
        },
    );
    // ... and one succeeded SETUP task.
    let setup = mk_setup_task("setup");
    let setup_hash = crate::primary::wire::compute_task_hash(&setup);
    s.tasks.insert(
        setup_hash,
        TaskState::SetupCompleted {
            task: setup,
            attempt: 0,
        },
    );

    let counts = s.counts();
    assert_eq!(counts.completed, 1, "the work task is `completed`");
    assert_eq!(
        counts.setup_succeeded, 1,
        "the succeeded setup task is in its own `setup_succeeded` bucket"
    );

    let outcome = s.outcome_counts();
    assert_eq!(
        outcome.succeeded, 1,
        "ONLY the work task counts toward `succeeded` — the setup task must NOT"
    );
    assert_eq!(
        outcome.setup_succeeded, 1,
        "the setup task is in the disjoint `setup_succeeded` bucket"
    );
    assert_eq!(
        outcome.total_terminal(),
        2,
        "both terminals are fully accounted (no stranded mis-classification)"
    );
}
