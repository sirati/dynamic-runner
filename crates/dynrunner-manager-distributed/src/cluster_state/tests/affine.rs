//! #497 SecondaryAffine CRDT-state tests: Phase 2 (the `AffineReady`
//! ready-not-executed gate resolution) and Phase 3 (the
//! `QueuedAfterLocalDependency` work-task state + its report frames + the
//! primary's apply of them, with the report SEND path stubbed — that is
//! Phase 4).
//!
//! The two states share the `cluster_state` lattice, so they are tested
//! together. The originator detection (the WHEN) is exercised through
//! `ClusterState::affine_ready_mutations_for` (the read-only owner of the
//! READY-not-EXECUTED rule) so the gate resolution is pinned at the
//! cluster_state level without the async primary broadcast wrapper.

use super::*;
use dynrunner_core::TaskKind;

/// Build a `TaskKind::SecondaryAffine` gate task — `mk_task`'s twin with
/// the kind flipped (the gate `I` between an upload and a build).
fn mk_affine_task(name: &str) -> TaskInfo<RunnerIdentifier> {
    let mut task = mk_task(name);
    task.kind = TaskKind::SecondaryAffine;
    task
}

/// Add one dep edge (same phase p0, which `mk_task` uses) onto a task.
fn with_dep(mut task: TaskInfo<RunnerIdentifier>, dep_task_id: &str) -> TaskInfo<RunnerIdentifier> {
    task.task_depends_on.push(dynrunner_core::TaskDep {
        task_id: dep_task_id.into(),
        phase_id: PhaseId::from("p0"),
        inherit_outputs: false,
    });
    task
}

// ─────────────────────────── Phase 2 ───────────────────────────

/// upload (Setup) → I (SecondaryAffine) → B (Work). Completing the upload
/// makes `I` Pending-all-resolved; the originator detects it and emits
/// `AffineReady`; applying it resolves `B` (Pending, dispatchable) — all
/// WITHOUT the primary ever executing `I`.
#[test]
fn affine_ready_resolves_dependents_without_execution() {
    let mut s = ClusterState::<RunnerIdentifier>::new();

    // The upload setup task, in flight (about to succeed).
    let mut upload = mk_task("upload");
    upload.kind = TaskKind::Setup;
    let upload_hash = crate::primary::wire::compute_task_hash(&upload);
    let (def, routing) = crate::cluster_state::split_task_def(upload);
    s.tasks.insert(
        upload_hash.clone(),
        TaskState::InFlight {
            def,
            routing,
            secondary: "member-1".into(),
            worker: 0,
            version: Default::default(),
            attempt: 0,
        },
    );

    // The gate I depends on the upload; it is Blocked on it.
    let gate = with_dep(mk_affine_task("import"), "upload");
    let gate_hash = crate::primary::wire::compute_task_hash(&gate);
    super::seed_blocked(&mut s, &gate_hash, gate, upload_hash.clone(), 0);

    // The build B depends on the gate; it is Blocked on it.
    let build = with_dep(mk_task("build"), "import");
    let build_hash = crate::primary::wire::compute_task_hash(&build);
    super::seed_blocked(&mut s, &build_hash, build, gate_hash.clone(), 0);

    // The upload succeeds → its SetupCompleted apply auto-resumes the gate
    // Blocked → Pending (the resume surface the originator reads).
    let mut resumed: Vec<TaskInfo<RunnerIdentifier>> = Vec::new();
    let mut scratch: Vec<TaskInfo<RunnerIdentifier>> = Vec::new();
    s.apply_with_resumed_blocked(
        ClusterMutation::SetupCompleted {
            hash: upload_hash.clone(),
        },
        &mut resumed,
        &mut scratch,
    );
    // The gate is now Pending (its only dep, the upload, is terminal).
    assert!(
        matches!(s.task_state(&gate_hash), Some(TaskState::Pending { .. })),
        "the gate resumes to Pending when its upload dep resolves"
    );

    // The originator detects the freshly-Pending gate and emits AffineReady.
    let became_pending: Vec<String> = resumed
        .iter()
        .map(crate::primary::wire::compute_task_hash)
        .collect();
    let mutations = s.affine_ready_mutations_for(became_pending);
    assert_eq!(
        mutations.len(),
        1,
        "exactly one AffineReady for the freshly-ready gate"
    );
    assert!(matches!(
        &mutations[0],
        ClusterMutation::AffineReady { hash } if hash == &gate_hash
    ));

    // Apply it: the gate goes AffineReady (NEVER executed) and B unblocks.
    let outcome = s.apply(mutations.into_iter().next().unwrap());
    assert!(matches!(outcome, ApplyOutcome::Applied));
    assert!(
        matches!(
            s.task_state(&gate_hash),
            Some(TaskState::AffineReady { .. })
        ),
        "the gate is AffineReady (the READY-not-EXECUTED terminal), never InFlight/Completed"
    );
    assert!(
        matches!(s.task_state(&build_hash), Some(TaskState::Pending { .. })),
        "B unblocks to Pending the moment the gate is ready"
    );
}

/// OWNER-EMPHASISED: a SecondaryAffine gate with ZERO deps is born Pending
/// all-resolved at SPAWN, so the originator emits AffineReady immediately —
/// its dependents are unblocked from t=0 with no upload needed.
#[test]
fn affine_ready_at_spawn_for_no_dep_secondary_affine() {
    let mut s = ClusterState::<RunnerIdentifier>::new();

    // A no-dep gate AND a build that depends on it, spawned in one batch.
    let gate = mk_affine_task("import");
    let gate_hash = crate::primary::wire::compute_task_hash(&gate);
    let build = with_dep(mk_task("build"), "import");
    let build_hash = crate::primary::wire::compute_task_hash(&build);

    let mut resumed: Vec<TaskInfo<RunnerIdentifier>> = Vec::new();
    let mut newly_pending: Vec<TaskInfo<RunnerIdentifier>> = Vec::new();
    s.apply_with_resumed_blocked(
        ClusterMutation::TasksSpawned {
            tasks: vec![gate, build],
        },
        &mut resumed,
        &mut newly_pending,
    );

    // The no-dep gate is born Pending and surfaces on the spawn surface.
    assert!(
        matches!(s.task_state(&gate_hash), Some(TaskState::Pending { .. })),
        "a no-dep gate is born Pending all-resolved at spawn"
    );
    let became_pending: Vec<String> = newly_pending
        .iter()
        .map(crate::primary::wire::compute_task_hash)
        .collect();
    assert!(
        became_pending.contains(&gate_hash),
        "the spawn surface carries the freshly-Pending gate"
    );

    // The originator emits AffineReady for the gate (NOT for the build —
    // the build is a Work task, filtered out).
    let mutations = s.affine_ready_mutations_for(became_pending);
    assert_eq!(mutations.len(), 1, "only the gate, not the Work build");
    assert!(matches!(
        &mutations[0],
        ClusterMutation::AffineReady { hash } if hash == &gate_hash
    ));

    s.apply(mutations.into_iter().next().unwrap());
    assert!(matches!(
        s.task_state(&gate_hash),
        Some(TaskState::AffineReady { .. })
    ));
    // The build's dependents are born/unblocked Pending from t=0.
    assert!(
        matches!(s.task_state(&build_hash), Some(TaskState::Pending { .. })),
        "the gate's dependents are unblocked from t=0, no upload needed"
    );
}

/// A blocked dependent spawned AFTER the gate is already AffineReady lands
/// Pending (dispatchable), exactly like a dependent of a Completed prereq —
/// the spawn-time dep classifier treats AffineReady as a resolved dep.
#[test]
fn blocked_dependent_born_pending_when_i_already_affine_ready() {
    let mut s = ClusterState::<RunnerIdentifier>::new();

    // The gate is already AffineReady.
    let gate = mk_affine_task("import");
    let gate_hash = crate::primary::wire::compute_task_hash(&gate);
    let (def, routing) = crate::cluster_state::split_task_def(gate);
    s.tasks.insert(
        gate_hash,
        TaskState::AffineReady {
            def,
            routing,
            attempt: 0,
        },
    );

    // A build spawned now, depending on the gate.
    let build = with_dep(mk_task("build"), "import");
    let build_hash = crate::primary::wire::compute_task_hash(&build);
    s.apply(ClusterMutation::TasksSpawned { tasks: vec![build] });

    assert!(
        matches!(s.task_state(&build_hash), Some(TaskState::Pending { .. })),
        "a dependent of an already-AffineReady gate is born Pending (dispatchable), not Blocked"
    );
}

/// LOAD-BEARING accounting: an AffineReady gate is counted in NEITHER
/// `succeeded`/`fail_*`/`setup_succeeded`, BUT it IS in `total_terminal()`
/// (its own inert `affine_ready` bucket) so a clean gate-bearing run is not
/// mis-classified STRANDED at finalize.
#[test]
fn affine_ready_never_counted_in_success_fail_total() {
    let mut s = ClusterState::<RunnerIdentifier>::new();

    // One completed WORK task, one succeeded SETUP task, one AffineReady gate.
    let work = mk_task("work");
    let work_hash = crate::primary::wire::compute_task_hash(&work);
    let (def, routing) = crate::cluster_state::split_task_def(work);
    s.seed_task_state_for_test(
        &work_hash,
        TaskState::Completed {
            def,
            routing,
            attempt: 0,
        },
    );
    let mut setup = mk_task("setup");
    setup.kind = TaskKind::Setup;
    let setup_hash = crate::primary::wire::compute_task_hash(&setup);
    let (def, routing) = crate::cluster_state::split_task_def(setup);
    s.seed_task_state_for_test(
        &setup_hash,
        TaskState::SetupCompleted {
            def,
            routing,
            attempt: 0,
        },
    );
    let gate = mk_affine_task("import");
    let gate_hash = crate::primary::wire::compute_task_hash(&gate);
    let (def, routing) = crate::cluster_state::split_task_def(gate);
    s.seed_task_state_for_test(
        &gate_hash,
        TaskState::AffineReady {
            def,
            routing,
            attempt: 0,
        },
    );

    let counts = s.counts();
    assert_eq!(counts.completed, 1);
    assert_eq!(counts.setup_succeeded, 1);
    assert_eq!(
        counts.affine_ready, 1,
        "the gate is in its OWN inert `affine_ready` bucket"
    );

    let o = s.outcome_counts();
    assert_eq!(o.succeeded, 1, "ONLY the work task — the gate must NOT count");
    assert_eq!(o.setup_succeeded, 1, "the gate is NOT folded into setup_succeeded");
    assert_eq!(o.fail_retry, 0);
    assert_eq!(o.fail_oom, 0);
    assert_eq!(o.fail_final, 0);
    assert_eq!(
        o.affine_ready, 1,
        "the gate is in the disjoint `affine_ready` outcome bucket"
    );
    // The load-bearing line: all three terminals are accounted, so the
    // finalize `stranded = total - total_terminal()` reads zero (no
    // false-abort).
    assert_eq!(
        o.total_terminal(),
        3,
        "the gate IS counted in total_terminal so a gate-bearing run is not STRANDED"
    );
}

/// The `AffineReady` apply arm is gated on `Pending` ONLY and is idempotent:
/// it NoOps against a real terminal (lockout) AND against an already-
/// AffineReady entry, and the `Pending → AffineReady` transition preserves
/// the source `attempt`.
#[test]
fn affine_ready_apply_idempotent_and_gated() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let gate = mk_affine_task("import");
    let gate_hash = crate::primary::wire::compute_task_hash(&gate);

    // Pending (attempt 7 to prove it is preserved) → AffineReady Applied.
    let (def, routing) = crate::cluster_state::split_task_def(gate);
    s.tasks.insert(
        gate_hash.clone(),
        TaskState::Pending {
            def,
            routing,
            version: Default::default(),
            attempt: 7,
        },
    );
    let outcome = s.apply(ClusterMutation::AffineReady {
        hash: gate_hash.clone(),
    });
    assert!(matches!(outcome, ApplyOutcome::Applied));
    match s.task_state(&gate_hash) {
        Some(TaskState::AffineReady { attempt, .. }) => assert_eq!(*attempt, 7),
        other => panic!("expected AffineReady, got {other:?}"),
    }

    // Idempotent: a re-application against the already-AffineReady entry NoOps.
    let again = s.apply(ClusterMutation::AffineReady {
        hash: gate_hash.clone(),
    });
    assert!(matches!(again, ApplyOutcome::NoOp));

    // Gated: a real terminal locks out a late AffineReady.
    let other = mk_affine_task("other");
    let other_hash = crate::primary::wire::compute_task_hash(&other);
    let (def, routing) = crate::cluster_state::split_task_def(other);
    s.tasks.insert(
        other_hash.clone(),
        TaskState::Failed {
            def,
            routing,
            kind: ErrorType::NonRecoverable,
            last_error: "x".into(),
            version: Default::default(),
            attempt: 0,
        },
    );
    let gated = s.apply(ClusterMutation::AffineReady {
        hash: other_hash.clone(),
    });
    assert!(
        matches!(gated, ApplyOutcome::NoOp),
        "a real terminal locks out a late AffineReady"
    );
    assert!(matches!(
        s.task_state(&other_hash),
        Some(TaskState::Failed { .. })
    ));

    // Gated: an InFlight (which a gate never legitimately reaches) is NOT a
    // valid AffineReady source — only Pending is.
    let inflight = mk_affine_task("inflight");
    let inflight_hash = crate::primary::wire::compute_task_hash(&inflight);
    let (def, routing) = crate::cluster_state::split_task_def(inflight);
    s.tasks.insert(
        inflight_hash.clone(),
        TaskState::InFlight {
            def,
            routing,
            secondary: "m".into(),
            worker: 0,
            version: Default::default(),
            attempt: 0,
        },
    );
    let from_inflight = s.apply(ClusterMutation::AffineReady {
        hash: inflight_hash.clone(),
    });
    assert!(
        matches!(from_inflight, ApplyOutcome::NoOp),
        "AffineReady is Pending-only; an InFlight source NoOps"
    );

    // Unknown hash is a safe NoOp.
    let unknown = s.apply(ClusterMutation::AffineReady {
        hash: "no-such".into(),
    });
    assert!(matches!(unknown, ApplyOutcome::NoOp));
}

/// The originator NEVER emits AffineReady for a gate that still has an
/// UNRESOLVED dep (the load-bearing all-deps-resolved re-check — a
/// `resume_blocked_on` transitions a Blocked entry to Pending on its FIRST
/// matching prereq, but a gate with a SECOND still-live dep is not ready).
#[test]
fn affine_ready_originator_requires_all_deps_resolved() {
    let mut s = ClusterState::<RunnerIdentifier>::new();

    // Two upload deps: one Completed, one still InFlight.
    let mut up_done = mk_task("up_done");
    up_done.kind = TaskKind::Setup;
    let up_done_hash = crate::primary::wire::compute_task_hash(&up_done);
    let (def, routing) = crate::cluster_state::split_task_def(up_done);
    s.tasks.insert(
        up_done_hash,
        TaskState::SetupCompleted {
            def,
            routing,
            attempt: 0,
        },
    );
    let mut up_live = mk_task("up_live");
    up_live.kind = TaskKind::Setup;
    let up_live_hash = crate::primary::wire::compute_task_hash(&up_live);
    let (def, routing) = crate::cluster_state::split_task_def(up_live);
    s.tasks.insert(
        up_live_hash,
        TaskState::InFlight {
            def,
            routing,
            secondary: "m".into(),
            worker: 0,
            version: Default::default(),
            attempt: 0,
        },
    );

    // The gate depends on BOTH; it is Pending (e.g. a resume on `up_done`
    // already flipped it Blocked→Pending) but `up_live` is NOT terminal.
    let gate = with_dep(with_dep(mk_affine_task("import"), "up_done"), "up_live");
    let gate_hash = crate::primary::wire::compute_task_hash(&gate);
    let (def, routing) = crate::cluster_state::split_task_def(gate);
    s.tasks.insert(
        gate_hash.clone(),
        TaskState::Pending {
            def,
            routing,
            version: Default::default(),
            attempt: 0,
        },
    );

    let mutations = s.affine_ready_mutations_for(vec![gate_hash.clone()]);
    assert!(
        mutations.is_empty(),
        "a gate with a still-unresolved dep must NOT be declared ready"
    );
    assert!(
        matches!(s.task_state(&gate_hash), Some(TaskState::Pending { .. })),
        "the gate stays Pending until all its deps resolve"
    );
}

/// `AffineReady` survives snapshot/restore (its `to_completed_event → None`
/// so restore fires no spurious completion); a stale Pending snapshot must
/// NOT overwrite the terminal gate.
#[test]
fn affine_ready_round_trips_snapshot() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let gate = mk_affine_task("import");
    let gate_hash = crate::primary::wire::compute_task_hash(&gate);
    let (def, routing) = crate::cluster_state::split_task_def(gate);
    s.tasks.insert(
        gate_hash.clone(),
        TaskState::AffineReady {
            def,
            routing,
            attempt: 0,
        },
    );

    let snap = s.snapshot();
    let mut joiner = ClusterState::<RunnerIdentifier>::new();
    joiner.restore(snap);
    assert!(
        matches!(
            joiner.task_state(&gate_hash),
            Some(TaskState::AffineReady { .. })
        ),
        "the gate survives snapshot/restore"
    );
    assert_eq!(joiner.counts().affine_ready, 1);

    // A stale Pending snapshot must NOT overwrite the terminal gate.
    let mut stale = ClusterState::<RunnerIdentifier>::new();
    let (def, routing) = crate::cluster_state::split_task_def(mk_affine_task("import"));
    stale.tasks.insert(
        gate_hash.clone(),
        TaskState::Pending {
            def,
            routing,
            version: Default::default(),
            attempt: 0,
        },
    );
    joiner.restore(stale.snapshot());
    assert!(
        matches!(
            joiner.task_state(&gate_hash),
            Some(TaskState::AffineReady { .. })
        ),
        "terminal AffineReady must win over a stale Pending snapshot"
    );
}

// ─────────────────────────── Phase 3 ───────────────────────────

/// The `QueuedAfterLocalDependency` state + its mutation round-trip the wire
/// AND a snapshot carrying it restores it. (The report-frame wire mirror is
/// pinned in the protocol crate's codec tests; here the snapshot/restore is
/// the cluster_state half.)
#[test]
fn queued_after_local_dependency_round_trips_the_wire() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let build = mk_task("build");
    let build_hash = crate::primary::wire::compute_task_hash(&build);
    let (def, routing) = crate::cluster_state::split_task_def(build);
    s.tasks.insert(
        build_hash.clone(),
        TaskState::QueuedAfterLocalDependency {
            def,
            routing,
            secondary: "sec-3".into(),
            version: TaskVersion {
                primary_epoch: 2,
                seq: 5,
            },
            attempt: 1,
        },
    );

    // Snapshot/restore preserves the state + its `secondary`.
    let snap = s.snapshot();
    let mut joiner = ClusterState::<RunnerIdentifier>::new();
    joiner.restore(snap);
    match joiner.task_state(&build_hash) {
        Some(TaskState::QueuedAfterLocalDependency {
            secondary,
            version,
            attempt,
            ..
        }) => {
            assert_eq!(secondary, "sec-3");
            assert_eq!(
                *version,
                TaskVersion {
                    primary_epoch: 2,
                    seq: 5
                }
            );
            assert_eq!(*attempt, 1);
        }
        other => panic!("expected QueuedAfterLocalDependency, got {other:?}"),
    }
    assert_eq!(joiner.counts().queued_after_local_dependency, 1);

    // The `QueuedAfterLocalDependencySet` mutation serde round-trips.
    let mutation: ClusterMutation<RunnerIdentifier> =
        ClusterMutation::QueuedAfterLocalDependencySet {
            hash: build_hash.clone(),
            secondary: "sec-3".into(),
        };
    let json = serde_json::to_string(&mutation).unwrap();
    let decoded: ClusterMutation<RunnerIdentifier> = serde_json::from_str(&json).unwrap();
    match decoded {
        ClusterMutation::QueuedAfterLocalDependencySet { hash, secondary } => {
            assert_eq!(hash, build_hash);
            assert_eq!(secondary, "sec-3");
        }
        _ => panic!("expected QueuedAfterLocalDependencySet"),
    }
}

/// The queued state is OBSERVABLE: the observer stats projection NAMES the
/// secondary `S` that parked work behind its local import.
#[test]
fn queued_after_local_dependency_is_observable() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let build = mk_task("build");
    let build_hash = crate::primary::wire::compute_task_hash(&build);
    let (def, routing) = crate::cluster_state::split_task_def(build);
    s.tasks.insert(
        build_hash,
        TaskState::QueuedAfterLocalDependency {
            def,
            routing,
            secondary: "sec-7".into(),
            version: Default::default(),
            attempt: 0,
        },
    );

    let snapshot = crate::observer::reporting::StatsSnapshot::from_cluster_state(&s);
    assert_eq!(
        snapshot.queued_after_local_dependency, 1,
        "the queued task is reported on its own observable line"
    );
    assert_eq!(
        snapshot.per_secondary_queued_after_local_dep.get("sec-7"),
        Some(&1),
        "the observable projection NAMES the secondary parking the work"
    );
    // It is NOT folded into the running `in_flight` count.
    assert_eq!(snapshot.in_flight, 0);
}

/// Pending → QueuedAfterLocalDependency (the secondary's report applied)
/// → InFlight (the release applied via the EXISTING TaskAssigned
/// originator). Pins the full deferred-assignment transition through the
/// apply layer with the report SEND path stubbed.
#[test]
fn queued_then_released_transitions_to_inflight() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let build = mk_task("build");
    let build_hash = crate::primary::wire::compute_task_hash(&build);

    // B starts Pending (e.g. just assigned but the report outran the local
    // TaskAssigned apply — the deferred-assignment race the arm accepts).
    let (def, routing) = crate::cluster_state::split_task_def(build);
    s.tasks.insert(
        build_hash.clone(),
        TaskState::Pending {
            def,
            routing,
            version: Default::default(),
            attempt: 0,
        },
    );

    // The secondary's `TaskQueuedAfterLocalDependency` report → the primary
    // applies QueuedAfterLocalDependencySet.
    let set = s.apply(ClusterMutation::QueuedAfterLocalDependencySet {
        hash: build_hash.clone(),
        secondary: "sec-1".into(),
    });
    assert!(matches!(set, ApplyOutcome::Applied));
    match s.task_state(&build_hash) {
        Some(TaskState::QueuedAfterLocalDependency { secondary, .. }) => {
            assert_eq!(secondary, "sec-1")
        }
        other => panic!("expected QueuedAfterLocalDependency, got {other:?}"),
    }

    // The secondary's `LocalDependencyReleased` report → the primary
    // originates the EXISTING TaskAssigned (a freshly-minted higher version
    // so it dominates the queued entry in the join).
    let assign = s.apply(ClusterMutation::TaskAssigned {
        hash: build_hash.clone(),
        secondary: "sec-1".into(),
        worker: 4,
        version: TaskVersion {
            primary_epoch: 1,
            seq: 1,
        },
        attempt: 0,
    });
    assert!(matches!(assign, ApplyOutcome::Applied));
    match s.task_state(&build_hash) {
        Some(TaskState::InFlight {
            secondary, worker, ..
        }) => {
            assert_eq!(secondary, "sec-1");
            assert_eq!(*worker, 4);
        }
        other => panic!("expected InFlight after release, got {other:?}"),
    }
}

/// The `QueuedAfterLocalDependencySet` arm accepts an `InFlight` source too
/// (the standard just-assigned path) and is gated/idempotent: it NoOps
/// against a terminal and against an already-queued entry; the source
/// `version`+`attempt` are preserved.
#[test]
fn queued_after_local_dependency_set_gated_and_preserves_version() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let build = mk_task("build");
    let build_hash = crate::primary::wire::compute_task_hash(&build);

    // InFlight source (attempt 9, version (3,4)) → Queued, preserving both.
    let (def, routing) = crate::cluster_state::split_task_def(build);
    s.tasks.insert(
        build_hash.clone(),
        TaskState::InFlight {
            def,
            routing,
            secondary: "sec-1".into(),
            worker: 2,
            version: TaskVersion {
                primary_epoch: 3,
                seq: 4,
            },
            attempt: 9,
        },
    );
    let set = s.apply(ClusterMutation::QueuedAfterLocalDependencySet {
        hash: build_hash.clone(),
        secondary: "sec-1".into(),
    });
    assert!(matches!(set, ApplyOutcome::Applied));
    match s.task_state(&build_hash) {
        Some(TaskState::QueuedAfterLocalDependency {
            version, attempt, ..
        }) => {
            assert_eq!(
                *version,
                TaskVersion {
                    primary_epoch: 3,
                    seq: 4
                },
                "the source version is preserved"
            );
            assert_eq!(*attempt, 9, "the source attempt is preserved");
        }
        other => panic!("expected QueuedAfterLocalDependency, got {other:?}"),
    }

    // Idempotent: re-application against the already-queued entry NoOps.
    let again = s.apply(ClusterMutation::QueuedAfterLocalDependencySet {
        hash: build_hash.clone(),
        secondary: "sec-1".into(),
    });
    assert!(matches!(again, ApplyOutcome::NoOp));

    // Gated: a terminal locks it out.
    let done = mk_task("done");
    let done_hash = crate::primary::wire::compute_task_hash(&done);
    let (def, routing) = crate::cluster_state::split_task_def(done);
    s.tasks.insert(
        done_hash.clone(),
        TaskState::Completed {
            def,
            routing,
            attempt: 0,
        },
    );
    let gated = s.apply(ClusterMutation::QueuedAfterLocalDependencySet {
        hash: done_hash.clone(),
        secondary: "sec-1".into(),
    });
    assert!(matches!(gated, ApplyOutcome::NoOp));
    assert!(matches!(
        s.task_state(&done_hash),
        Some(TaskState::Completed { .. })
    ));

    // Unknown hash is a safe NoOp.
    let unknown = s.apply(ClusterMutation::QueuedAfterLocalDependencySet {
        hash: "no-such".into(),
        secondary: "sec-1".into(),
    });
    assert!(matches!(unknown, ApplyOutcome::NoOp));
}

/// On the holding secondary's death the queued task requeues to `Pending`
/// (re-routable per #495) — the death seam's `TaskRequeued` apply arm
/// accepts a `QueuedAfterLocalDependency` source exactly as it does an
/// `InFlight`, so the same single requeue originator covers it.
#[test]
fn queued_after_local_dependency_requeues_on_secondary_death() {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let build = mk_task("build");
    let build_hash = crate::primary::wire::compute_task_hash(&build);
    let (def, routing) = crate::cluster_state::split_task_def(build);
    s.tasks.insert(
        build_hash.clone(),
        TaskState::QueuedAfterLocalDependency {
            def,
            routing,
            secondary: "dead-sec".into(),
            version: TaskVersion {
                primary_epoch: 1,
                seq: 2,
            },
            attempt: 3,
        },
    );

    // The death seam emits TaskRequeued (one originator for both InFlight
    // and Queued sources); applying it moves the queued task → Pending.
    let outcome = s.apply(ClusterMutation::TaskRequeued {
        hash: build_hash.clone(),
        version: TaskVersion {
            primary_epoch: 1,
            seq: 9,
        },
    });
    assert!(matches!(outcome, ApplyOutcome::Applied));
    match s.task_state(&build_hash) {
        Some(TaskState::Pending {
            version, attempt, ..
        }) => {
            // The reset stamps the higher version; the attempt is preserved
            // (a requeue is a within-generation rank-drop).
            assert_eq!(
                *version,
                TaskVersion {
                    primary_epoch: 1,
                    seq: 9
                }
            );
            assert_eq!(*attempt, 3, "the requeue preserves the generation");
        }
        other => panic!("expected Pending after requeue, got {other:?}"),
    }
}
