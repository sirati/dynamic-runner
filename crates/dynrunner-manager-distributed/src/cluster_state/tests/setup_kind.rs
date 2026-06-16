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
    super::seed_blocked(&mut s, &dep_hash, dependent, setup_hash.clone(), 0);

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
    s.seed_task_state_for_test(
        &work_hash,
        TaskState::Completed {
            task: work,
            attempt: 0,
        },
    );
    // ... and one succeeded SETUP task.
    let setup = mk_setup_task("setup");
    let setup_hash = crate::primary::wire::compute_task_hash(&setup);
    s.seed_task_state_for_test(
        &setup_hash,
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

// ── P2: the `ClusterMutation::SetupCompleted` WRITE arm ──

/// The `SetupCompleted` mutation transitions an `InFlight` setup task (the
/// state after the executor was assigned) to the terminal
/// `TaskState::SetupCompleted`, preserving the source `attempt`. This is the
/// terminal-WRITE the executor originates on success.
#[test]
fn setup_completed_mutation_writes_terminal_from_inflight() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let setup = mk_setup_task("setup");
    let setup_hash = crate::primary::wire::compute_task_hash(&setup);
    // The executor was assigned: the task is InFlight (attempt 3 to prove
    // the attempt is preserved verbatim).
    s.tasks.insert(
        setup_hash.clone(),
        TaskState::InFlight {
            task: setup,
            secondary: "member-1".into(),
            worker: 0,
            version: Default::default(),
            attempt: 3,
        },
    );

    let outcome = s.apply(ClusterMutation::SetupCompleted {
        hash: setup_hash.clone(),
    });
    assert!(matches!(outcome, ApplyOutcome::Applied));
    match s.task_state(&setup_hash) {
        Some(TaskState::SetupCompleted { attempt, .. }) => {
            assert_eq!(*attempt, 3, "the source attempt is preserved");
        }
        other => panic!("SetupCompleted must write the terminal, got {other:?}"),
    }
}

/// The `SetupCompleted` apply arm AUTO-RESUMES a dependent that was
/// `Blocked` on the setup task — driven END-TO-END through the mutation
/// (not by a direct `resume_blocked_on` call), so a build task gated on the
/// setup task becomes dispatchable the moment the executor's success
/// mutation applies (seam (c) via the WRITE arm).
#[test]
fn setup_completed_mutation_resumes_blocked_dependent() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let setup = mk_setup_task("setup");
    let setup_hash = crate::primary::wire::compute_task_hash(&setup);
    s.tasks.insert(
        setup_hash.clone(),
        TaskState::InFlight {
            task: setup,
            secondary: "member-1".into(),
            worker: 0,
            version: Default::default(),
            attempt: 0,
        },
    );
    let dependent = mk_task("dependent");
    let dep_hash = crate::primary::wire::compute_task_hash(&dependent);
    super::seed_blocked(&mut s, &dep_hash, dependent, setup_hash.clone(), 0);

    // Apply the success terminal through the mutation; the arm's
    // resume_blocked_on unblocks the dependent.
    let outcome = s.apply(ClusterMutation::SetupCompleted {
        hash: setup_hash.clone(),
    });
    assert!(matches!(outcome, ApplyOutcome::Applied));
    match s.task_state(&dep_hash) {
        Some(TaskState::Pending { .. }) => {}
        other => panic!(
            "the SetupCompleted mutation must auto-resume the Blocked dependent to \
             Pending, got {other:?}"
        ),
    }
    // And the setup task itself is terminal.
    assert!(matches!(
        s.task_state(&setup_hash),
        Some(TaskState::SetupCompleted { .. })
    ));
}

/// The `SetupCompleted` arm is gated: it NoOps against a state that is not
/// `InFlight`/`Pending` (a real terminal locks it out), and is idempotent
/// against an already-`SetupCompleted` entry — a late/duplicate executor
/// success can never overwrite real progress.
#[test]
fn setup_completed_mutation_noops_against_real_terminal_and_is_idempotent() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let setup = mk_setup_task("setup");
    let setup_hash = crate::primary::wire::compute_task_hash(&setup);
    // A non-recoverable FAILURE already settled it (e.g. the executor died).
    s.tasks.insert(
        setup_hash.clone(),
        TaskState::Failed {
            task: setup,
            kind: ErrorType::NonRecoverable,
            last_error: "executor died".into(),
            version: Default::default(),
            attempt: 0,
        },
    );
    let outcome = s.apply(ClusterMutation::SetupCompleted {
        hash: setup_hash.clone(),
    });
    assert!(
        matches!(outcome, ApplyOutcome::NoOp),
        "a real terminal locks out a late setup success"
    );
    assert!(
        matches!(s.task_state(&setup_hash), Some(TaskState::Failed { .. })),
        "the failure terminal survives the late success"
    );

    // Idempotent against an already-succeeded entry.
    s.tasks.insert(
        setup_hash.clone(),
        TaskState::SetupCompleted {
            task: mk_setup_task("setup"),
            attempt: 0,
        },
    );
    let again = s.apply(ClusterMutation::SetupCompleted { hash: setup_hash });
    assert!(matches!(again, ApplyOutcome::NoOp));
}

/// The `SetupCompleted` mutation against an unknown hash is a safe NoOp
/// (no panic, no spurious insert) — a duplicate report after the entry was
/// compacted, or a frame for a task this replica never saw.
#[test]
fn setup_completed_mutation_unknown_hash_is_noop() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let outcome = s.apply(ClusterMutation::SetupCompleted {
        hash: "no-such-hash".into(),
    });
    assert!(matches!(outcome, ApplyOutcome::NoOp));
    assert!(s.task_state("no-such-hash").is_none());
}
