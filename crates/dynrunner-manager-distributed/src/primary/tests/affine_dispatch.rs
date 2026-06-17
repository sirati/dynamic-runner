//! AF-sched dispatch-consumer integration tests (synchronous, deterministic).
//!
//! These drive the affine per-secondary dispatch through the SAME synchronous
//! seams the dispatch-decoupling / capacity-dispatch tests use
//! (`handle_mesh_ready` to confirm a member without the in-process mesh's
//! ~60s watchdog wait, `react_to_worker_signal_batch` to run the recheck,
//! `handle_task_complete` to feed a terminal back) — so they validate the
//! Model-B affine behaviour (placement → per-secondary import→work ordering →
//! per-secondary bitvector → re-dispatch per secondary) in milliseconds,
//! without the real-secondary mesh harness's multi-round latency.

use super::*;

use dynrunner_core::{PhaseId, ResourceAmount, ResourceKind, TaskDep, TaskKind, TypeId};
use dynrunner_protocol_primary_secondary::AffineCell;

use crate::primary::wire::compute_task_hash;
use crate::worker_signal::WorkerMgmtSignal;

type TestPrimary = PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>;

fn mem(bytes: u64) -> Vec<ResourceAmount> {
    vec![ResourceAmount {
        kind: ResourceKind::memory(),
        amount: bytes,
    }]
}

/// A `SecondaryAffine` import task (no deps), phase "work".
fn affine_import(name: &str) -> TaskInfo<TestId> {
    let mut t = make_binary(name, 10);
    t.phase_id = PhaseId::from("work");
    t.type_id = TypeId::from("default");
    t.kind = TaskKind::SecondaryAffine;
    t
}

/// A `Work` task depending on the (affine) prereq `dep` in phase "work".
fn work_dep(name: &str, dep: &str) -> TaskInfo<TestId> {
    let mut t = make_binary(name, 20);
    t.phase_id = PhaseId::from("work");
    t.type_id = TypeId::from("default");
    t.task_depends_on = vec![TaskDep {
        task_id: dep.into(),
        phase_id: PhaseId::from("work"),
        inherit_outputs: false,
        def_id: None,
    }];
    t
}

/// `PeerJoined` + `SecondaryCapacity` for `secondary` with `n` workers.
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

fn mesh_ready_from(secondary_id: &str) -> DistributedMessage<TestId> {
    DistributedMessage::MeshReady {
        target: None,
        sender_id: secondary_id.into(),
        timestamp: 0.0,
        secondary_id: secondary_id.into(),
        peer_count: 1,
    }
}

/// A `TaskComplete` from `secondary`/`worker` for `task_hash`.
fn task_complete(secondary: &str, worker: u32, task_hash: &str) -> DistributedMessage<TestId> {
    DistributedMessage::TaskComplete {
        target: None,
        sender_id: secondary.into(),
        timestamp: 0.0,
        secondary_id: secondary.into(),
        worker_id: worker,
        task_hash: task_hash.into(),
        result_data: None,
        delivery_seq: None,
        msgs_posted_through: None,
    }
}

/// A `TaskFailed` from `secondary`/`worker` for `task_hash` (NonRecoverable —
/// a genuine terminal, not a backpressure bounce).
fn task_failed(secondary: &str, worker: u32, task_hash: &str) -> DistributedMessage<TestId> {
    DistributedMessage::TaskFailed {
        target: None,
        sender_id: secondary.into(),
        timestamp: 0.0,
        secondary_id: secondary.into(),
        worker_id: worker,
        task_hash: task_hash.into(),
        error_type: dynrunner_core::ErrorType::NonRecoverable,
        error_message: "affine import failed".into(),
        delivery_seq: None,
        msgs_posted_through: None,
    }
}

/// Drain the primary's command channel (the affine terminal-fail path enqueues
/// a `FailPermanent` onto it via `command_tx`), running each command through
/// the real handler so its cascade + broadcast fire exactly as the operational
/// loop would. Takes the receiver out for the duration so the handler can hold
/// `&mut primary`, then restores it.
async fn drain_commands(primary: &mut TestPrimary) {
    let mut rx = primary.command_rx.take().expect("command_rx present");
    while let Ok(cmd) = rx.try_recv() {
        let mut rx_slot: Option<tokio_mpsc::Receiver<_>> = None;
        crate::primary::command_channel::handle_primary_command(primary, cmd, &mut rx_slot).await;
    }
    primary.command_rx = Some(rx);
}

/// Drain the `(task_id, secondary_id, worker_id, file_hash)` of every
/// `TaskAssignment` queued on a secondary's wire (non-blocking).
fn assignments(
    rx: &mut tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
) -> Vec<(String, String, u32, String)> {
    let mut out = Vec::new();
    while let Ok(msg) = rx.try_recv() {
        if let DistributedMessage::TaskAssignment {
            binary_info,
            secondary_id,
            worker_id,
            file_hash,
            ..
        } = msg
        {
            out.push((binary_info.task_id, secondary_id, worker_id, file_hash));
        }
    }
    out
}

/// Build a 2-secondary primary (1 worker each, mesh-confirmed) whose CRDT
/// holds `binaries` (with affine-ids registered for `SecondaryAffine` defs).
/// The internal worker-mgmt bus is replaced with an observable one whose
/// receiver is returned so the test drives the recheck itself.
#[allow(clippy::type_complexity)]
fn primary_two_secondaries_with(
    binaries: Vec<TaskInfo<TestId>>,
) -> (
    TestPrimary,
    Vec<(
        String,
        tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
        tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
    )>,
    tokio_mpsc::UnboundedReceiver<WorkerMgmtSignal>,
    PrimaryMeshKeepalive,
) {
    primary_two_secondaries_with_phase_deps(
        binaries,
        HashMap::from([(PhaseId::from("work"), vec![])]),
    )
}

/// Sibling of [`primary_two_secondaries_with`] taking an explicit `phase_deps`
/// graph, for multi-phase shapes (e.g. a phase-2 `final` depending on phase
/// "work"). `PhaseDepsSet` is set-once-immutable, so the graph must be the
/// FULL declared set, applied once here.
#[allow(clippy::type_complexity)]
fn primary_two_secondaries_with_phase_deps(
    binaries: Vec<TaskInfo<TestId>>,
    phase_deps: HashMap<PhaseId, Vec<PhaseId>>,
) -> (
    TestPrimary,
    Vec<(
        String,
        tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
        tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
    )>,
    tokio_mpsc::UnboundedReceiver<WorkerMgmtSignal>,
    PrimaryMeshKeepalive,
) {
    let (transport, ends) = setup_test(2);
    let config = PrimaryConfig {
        num_secondaries: 2,
        ..test_primary_config()
    };
    let (mut primary, mesh) = build_test_primary(
        config,
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );

    {
        let cs = primary.cluster_state_mut_for_test();
        cs.apply(ClusterMutation::PhaseDepsSet { deps: phase_deps });
        for task in &binaries {
            let hash = compute_task_hash(task);
            cs.apply(ClusterMutation::TaskAdded {
                hash: hash.clone(),
                task: task.clone(),
                def_id: None,
            });
            if task.kind.is_secondary_affine() {
                let affine_id = cs.allocate_affine_id(&hash).0;
                cs.apply(ClusterMutation::SecondaryAffineRegistered { hash, affine_id });
            }
        }
    }
    primary
        .hydrate_from_cluster_state()
        .expect("composed affine graph is valid");

    // Grow the roster (1 worker each) + confirm both mesh legs, so proactive
    // dispatch is unblocked without the in-process mesh's 60s watchdog.
    let (wm_tx, wm_rx) = tokio_mpsc::unbounded_channel::<WorkerMgmtSignal>();
    primary
        .cluster_state_mut_for_test()
        .install_worker_mgmt_sender(wm_tx);
    (primary, ends, wm_rx, mesh)
}

/// Synchronously confirm both secondaries (capacity + MeshReady) and drain the
/// resulting bus signals, leaving the primary ready for the recheck.
async fn confirm_two(primary: &mut TestPrimary) {
    for sec in ["sec-0", "sec-1"] {
        primary
            .handle_cluster_mutation(capacity_batch(sec, 1), &mut None)
            .await;
        primary.handle_mesh_ready(mesh_ready_from(sec));
    }
}

/// Drive the worker-mgmt bus to quiescence (run every queued recheck batch).
async fn drain_rechecks(
    primary: &mut TestPrimary,
    wm_rx: &mut tokio_mpsc::UnboundedReceiver<WorkerMgmtSignal>,
) {
    while let Some(batch) = crate::worker_signal::try_collect_worker_signal_batch(wm_rx) {
        primary.react_to_worker_signal_batch(batch, &mut None).await;
        settle_pump().await;
    }
}

/// The dependent build NEVER dispatches to a secondary before that secondary's
/// import has run there (the per-secondary affine-readiness gate). Two builds
/// on two secondaries → the import runs on BOTH (each node imports locally);
/// each build runs only AFTER its own secondary's import-cell is `Done`.
///
/// This is the affine end-to-end AF-remove deleted, rewritten for the new
/// per-secondary model and driven synchronously (no mesh-watchdog latency): the
/// import RUNS (not auto-fulfilled), it runs PER-SECONDARY (not one global
/// terminal), and the dependent's readiness is the per-secondary bitvector
/// cell, never a global completion — so a build NEVER lands on a non-imported
/// secondary.
#[tokio::test(flavor = "current_thread")]
async fn affine_import_runs_per_secondary_and_gates_each_dependent() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let import = affine_import("import");
            let import_hash = compute_task_hash(&import);
            let build_0 = work_dep("build_0", "import");
            let build_1 = work_dep("build_1", "import");
            let build_0_hash = compute_task_hash(&build_0);
            let build_1_hash = compute_task_hash(&build_1);

            let (mut primary, mut ends, mut wm_rx, _mesh) =
                primary_two_secondaries_with(vec![import, build_0, build_1]);
            confirm_two(&mut primary).await;

            let affine_id = primary
                .cluster_state_for_test()
                .affine_id_for_hash(&import_hash)
                .expect("registered affine-id");

            // INVARIANT, re-checked after every drain round: NO build is ever
            // dispatched to a secondary whose import cell is not yet `Done`.
            // This is the core per-secondary-correctness property — a build on a
            // non-imported secondary would defeat the redesign.
            let assert_no_premature_build =
                |primary: &TestPrimary, round: &[(String, String, u32, String)]| {
                    for (_, sec, _, h) in round {
                        if *h == build_0_hash || *h == build_1_hash {
                            assert_eq!(
                                primary.cluster_state_for_test().affine_state(sec, affine_id),
                                AffineCell::Done,
                                "build dispatched to {sec} whose import cell is NOT Done \
                                 — a dependent on a non-imported secondary; got {round:?}"
                            );
                        }
                    }
                };

            // Round 1: placement + per-secondary pops. The imports dispatch
            // (one per secondary that owns a dependent); no build dispatches
            // before its secondary's import is Done.
            drain_rechecks(&mut primary, &mut wm_rx).await;
            let mut imports_dispatched: Vec<(String, u32)> = Vec::new();
            loop {
                let mut round: Vec<(String, String, u32, String)> = Vec::new();
                for (_id, rx, _tx) in ends.iter_mut() {
                    round.extend(assignments(rx));
                }
                if round.is_empty() {
                    break;
                }
                assert_no_premature_build(&primary, &round);
                // Complete every dispatched IMPORT on its secondary (→ that
                // secondary's cell goes Done), which re-nudges the recheck so
                // the gated builds can then dispatch there.
                for (_, sec, worker, h) in &round {
                    if *h == import_hash {
                        imports_dispatched.push((sec.clone(), *worker));
                        primary
                            .handle_task_complete(task_complete(sec, *worker, h), &mut None)
                            .await;
                        settle_pump().await;
                    }
                }
                drain_rechecks(&mut primary, &mut wm_rx).await;
            }

            // The import RAN on BOTH secondaries (each build's secondary
            // imported locally) — its per-secondary bitvector cell is `Done` on
            // both.
            for sec in ["sec-0", "sec-1"] {
                assert_eq!(
                    primary.cluster_state_for_test().affine_state(sec, affine_id),
                    AffineCell::Done,
                    "the affine import must have RUN (cell Done) on {sec}"
                );
            }
            let mut import_secs: Vec<String> =
                imports_dispatched.iter().map(|(s, _)| s.clone()).collect();
            import_secs.sort();
            import_secs.dedup();
            assert_eq!(
                import_secs,
                vec!["sec-0".to_string(), "sec-1".to_string()],
                "the import dispatched on BOTH secondaries (per-secondary re-run)"
            );

            // Both builds dispatched (each on a secondary whose import is Done,
            // already asserted by the invariant during the loop).
            let mut all: Vec<(String, String, u32, String)> = Vec::new();
            for (_id, rx, _tx) in ends.iter_mut() {
                all.extend(assignments(rx));
            }
            // (any trailing build dispatch is also gated)
            assert_no_premature_build(&primary, &all);
        })
        .await;
}

/// After the affine import completes on both secondaries AND both dependent
/// builds complete, the phase DRAINS, `on_phase_end` fires, and the run
/// reaches completion.
///
/// This pins the HIGH the original e2e missed: the affine prereq stays in the
/// pool as a non-worker-assignable ledger TOKEN (the placement-readiness
/// signal). If `queued_count` counted it, the phase would never drain
/// (`maybe_transition_drain` would never flip `Drained`), `on_phase_end` would
/// never fire, and a multi-phase lazy-spawn producer would hang. The drain +
/// `on_phase_end` + `is_run_complete` here is the proof the token is excluded
/// from the drain count and dropped at the phase-end edge. A 2-phase shape
/// proves `on_phase_end` actually fires: a phase-2 `final` task, BLOCKED on a
/// phase-1 build, must dispatch + complete only AFTER the phase-1 drain
/// activates phase 2.
#[tokio::test(flavor = "current_thread")]
async fn affine_phase_drains_on_phase_end_fires_and_run_completes() {
    use dynrunner_scheduler_api::pending_pool::PhaseState;
    use std::sync::{Arc, Mutex};

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let import = affine_import("import");
            let import_hash = compute_task_hash(&import);
            let build_0 = work_dep("build_0", "import");
            let build_1 = work_dep("build_1", "import");
            // Phase-2 task gated on a phase-1 build (a cross-phase dep). It can
            // only dispatch once phase "work" drains and "final" activates —
            // the proof `on_phase_end("work")` fired and the cascade advanced.
            let mut final_task = make_binary("final", 20);
            final_task.phase_id = PhaseId::from("final");
            final_task.type_id = TypeId::from("default");
            final_task.task_depends_on = vec![TaskDep {
                task_id: "build_0".into(),
                phase_id: PhaseId::from("work"),
                inherit_outputs: false,
                def_id: None,
            }];
            let final_hash = compute_task_hash(&final_task);

            // Phase "final" depends on "work" so it starts Blocked and is
            // activated only by the "work" drain cascade.
            let (mut primary, mut ends, mut wm_rx, _mesh) =
                primary_two_secondaries_with_phase_deps(
                    vec![import, build_0, build_1, final_task],
                    HashMap::from([
                        (PhaseId::from("work"), vec![]),
                        (PhaseId::from("final"), vec![PhaseId::from("work")]),
                    ]),
                );

            // Record every `on_phase_end` firing so we can prove the "work"
            // edge fired.
            let ended: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
            let ended_cb = Arc::clone(&ended);
            let on_start: OnPhaseStart = Box::new(|_p: &PhaseId| {});
            let on_end: OnPhaseEnd =
                Box::new(move |p: &PhaseId, _c: u32, _f: u32, _outputs| {
                    ended_cb.lock().unwrap().push(p.to_string());
                });
            primary.register_phase_lifecycle_callbacks(on_start, on_end);

            confirm_two(&mut primary).await;

            // Drive to quiescence: dispatch imports + builds, completing every
            // dispatched task on its secondary. The lifecycle cascade runs
            // inside `handle_task_complete`, so completing the last build drains
            // phase "work", fires `on_phase_end("work")`, activates "final",
            // and dispatches the `final` task.
            drain_rechecks(&mut primary, &mut wm_rx).await;
            loop {
                let mut round: Vec<(String, String, u32, String)> = Vec::new();
                for (_id, rx, _tx) in ends.iter_mut() {
                    round.extend(assignments(rx));
                }
                if round.is_empty() {
                    break;
                }
                for (_, sec, worker, h) in &round {
                    primary
                        .handle_task_complete(task_complete(sec, *worker, h), &mut None)
                        .await;
                    settle_pump().await;
                }
                drain_rechecks(&mut primary, &mut wm_rx).await;
            }

            // The import ran on both secondaries.
            let affine_id = primary
                .cluster_state_for_test()
                .affine_id_for_hash(&import_hash)
                .expect("registered affine-id");
            for sec in ["sec-0", "sec-1"] {
                assert_eq!(
                    primary.cluster_state_for_test().affine_state(sec, affine_id),
                    AffineCell::Done,
                    "the affine import must have RUN on {sec}"
                );
            }

            // PHASE DRAIN (the HIGH): phase "work" reached `Done` — the affine
            // ledger token did NOT pin `queued_count`, so `maybe_transition_drain`
            // flipped it `Drained` and the lifecycle cascade marked it `Done`.
            assert_eq!(
                primary.pool().phase_state(&PhaseId::from("work")),
                Some(PhaseState::Done),
                "phase 'work' must drain to Done — the affine ledger token must \
                 NOT hold it open (queued_count excludes it)"
            );

            // `on_phase_end("work")` FIRED (it never fires if the phase doesn't
            // drain) — the multi-phase lazy-spawn producer's hang is closed.
            assert!(
                ended.lock().unwrap().iter().any(|p| p == "work"),
                "on_phase_end('work') must fire at the drain edge; fired: {:?}",
                ended.lock().unwrap()
            );

            // FIX 2: the affine ledger token was DROPPED at the phase-end edge —
            // no `SecondaryAffine` item lingers in any bucket to bleed into a
            // later phase's placement scan.
            assert_eq!(
                primary
                    .pool()
                    .iter()
                    .filter(|t| t.kind.is_secondary_affine())
                    .count(),
                0,
                "the affine ledger token must be dropped at phase-end (FIX 2)"
            );

            // The phase-2 `final` task dispatched + completed inside the loop
            // above (proof the drain cascade activated "final" and the run
            // advanced past phase 1): its global TaskState is terminal.
            assert!(
                primary
                    .cluster_state_for_test()
                    .task_state(&final_hash)
                    .is_some_and(|s| s.is_terminal()),
                "the phase-2 'final' task must complete after the phase-1 drain"
            );

            // RUN COMPLETE: every phase drained, nothing Active/Draining, the
            // pool is empty (the affine token gone, every work task terminal).
            assert!(
                primary.pool().is_run_complete(),
                "the run must reach completion once all work + the import drain"
            );
        })
        .await;
}

/// AFFINE-ONLY PHASE (#affine-only-phase-drain): a phase whose ONLY content is
/// the no-dep affine import, with the dependent builds in a SEPARATE downstream
/// phase. The import phase must NOT proceed-or-fail at SEED time (when its
/// uncounted token leaves `queued/in_flight/blocked` all zero) — it must wait
/// for the import's first per-secondary terminal, THEN drain and activate the
/// build phase. Pre-fix this false-failed at iter=0 ("phase reached drain with
/// no terminal outcome") because the pool's drain transition fired before the
/// import ran while the rollup still showed it live.
///
/// Drives through the same synchronous `handle_*` seams as the same-phase test
/// above (which it must NOT regress). The proof: the import phase reaches
/// `Done` only after the import terminals, the build phase's builds dispatch
/// (each gated on its own secondary's cell), the run completes, and `on_phase_end`
/// fires for the import phase WITHOUT any premature drain edge.
#[tokio::test(flavor = "current_thread")]
async fn affine_only_phase_waits_for_import_then_drains_and_activates_dependents() {
    use dynrunner_scheduler_api::pending_pool::PhaseState;
    use std::sync::{Arc, Mutex};

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // The import ALONE in phase "import"; the builds in a SEPARATE
            // phase "build" depending (per-task) on the import.
            let import = affine_import("import");
            let import_hash = compute_task_hash(&import);

            let make_build = |name: &str| {
                let mut t = make_binary(name, 20);
                t.phase_id = PhaseId::from("build");
                t.type_id = TypeId::from("default");
                t.task_depends_on = vec![TaskDep {
                    task_id: "import".into(),
                    phase_id: PhaseId::from("import"),
                    inherit_outputs: false,
                    def_id: None,
                }];
                t
            };
            let build_0 = make_build("build_0");
            let build_1 = make_build("build_1");

            // Phase "build" depends on "import" so it starts Blocked and is
            // activated only by the "import" drain cascade.
            let (mut primary, mut ends, mut wm_rx, _mesh) =
                primary_two_secondaries_with_phase_deps(
                    vec![import, build_0, build_1],
                    HashMap::from([
                        (PhaseId::from("import"), vec![]),
                        (PhaseId::from("build"), vec![PhaseId::from("import")]),
                    ]),
                );

            let ended: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
            let ended_cb = Arc::clone(&ended);
            let on_start: OnPhaseStart = Box::new(|_p: &PhaseId| {});
            let on_end: OnPhaseEnd =
                Box::new(move |p: &PhaseId, _c: u32, _f: u32, _outputs| {
                    ended_cb.lock().unwrap().push(p.to_string());
                });
            primary.register_phase_lifecycle_callbacks(on_start, on_end);

            confirm_two(&mut primary).await;

            // The pre-loop / seed-time cascade ran inside `hydrate` + the
            // confirm path. The import phase must NOT have drained yet (its
            // import has not run) and the run must NOT have failed.
            assert_eq!(
                primary.pool().phase_state(&PhaseId::from("import")),
                Some(PhaseState::Active),
                "the affine-only 'import' phase must stay Active until its \
                 import terminals — NOT prematurely drained at seed time"
            );
            assert!(
                !primary.has_run_fail_outcome_for_test(),
                "the run must NOT false-fail at seed time on the affine-only phase"
            );
            assert!(
                ended.lock().unwrap().is_empty(),
                "on_phase_end must NOT fire for the import phase before its \
                 import terminals; fired: {:?}",
                ended.lock().unwrap()
            );

            // Drive to quiescence: imports dispatch + complete per secondary,
            // which drains the import phase, activates "build", and dispatches
            // the gated builds (each only on a secondary whose cell is Done).
            drain_rechecks(&mut primary, &mut wm_rx).await;
            loop {
                let mut round: Vec<(String, String, u32, String)> = Vec::new();
                for (_id, rx, _tx) in ends.iter_mut() {
                    round.extend(assignments(rx));
                }
                if round.is_empty() {
                    break;
                }
                for (_, sec, worker, h) in &round {
                    primary
                        .handle_task_complete(task_complete(sec, *worker, h), &mut None)
                        .await;
                    settle_pump().await;
                }
                drain_rechecks(&mut primary, &mut wm_rx).await;
            }

            // The import ran on both secondaries.
            let affine_id = primary
                .cluster_state_for_test()
                .affine_id_for_hash(&import_hash)
                .expect("registered affine-id");
            for sec in ["sec-0", "sec-1"] {
                assert_eq!(
                    primary.cluster_state_for_test().affine_state(sec, affine_id),
                    AffineCell::Done,
                    "the affine import must have RUN on {sec}"
                );
            }

            // The import phase drained to Done AFTER its import terminaled, and
            // its on_phase_end fired — the affine-only-phase wedge is closed.
            assert_eq!(
                primary.pool().phase_state(&PhaseId::from("import")),
                Some(PhaseState::Done),
                "the 'import' phase must drain to Done once its import terminals"
            );
            assert!(
                ended.lock().unwrap().iter().any(|p| p == "import"),
                "on_phase_end('import') must fire at the (post-terminal) drain \
                 edge; fired: {:?}",
                ended.lock().unwrap()
            );
            assert!(
                !primary.has_run_fail_outcome_for_test(),
                "the run must not fail across the whole affine-only-phase run"
            );

            // The build phase activated and both builds completed (proof the
            // drain cascade advanced past the import-only phase).
            assert!(
                primary.pool().is_run_complete(),
                "the run must complete once the import + both builds drain"
            );
        })
        .await;
}

/// FIX 4(c): when the affine import FAILS on a secondary, the gated dependent
/// RE-ROUTES to a still-satisfiable secondary; when it has failed on EVERY
/// eligible secondary, the dependent is TERMINAL-FAILED (cascade) rather than
/// spinning forever. This pins both the all-`Failed` LIVELOCK fix and the
/// owner Q1 terminal-by-default, AND that a `Reroute` re-route happens first
/// (the import does dispatch on the second secondary after failing on the
/// first).
#[tokio::test(flavor = "current_thread")]
async fn affine_import_failed_everywhere_terminal_fails_dependent() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let import = affine_import("import");
            let import_hash = compute_task_hash(&import);
            let build = work_dep("build", "import");
            let build_hash = compute_task_hash(&build);

            let (mut primary, mut ends, mut wm_rx, _mesh) =
                primary_two_secondaries_with(vec![import, build]);
            confirm_two(&mut primary).await;

            let affine_id = primary
                .cluster_state_for_test()
                .affine_id_for_hash(&import_hash)
                .expect("registered affine-id");

            // Drive the run, FAILING every dispatched import (never completing
            // it). The gate re-routes the dependent off each failed secondary
            // onto the next still-satisfiable one; once the import has failed on
            // BOTH, the gate terminal-fails the dependent. Bound the loop so an
            // un-fixed livelock (infinite re-place / re-pop) is caught as a
            // test timeout rather than a silent spin — but assert progress.
            let mut import_failed_on: Vec<String> = Vec::new();
            let mut build_dispatched = false;
            for _round in 0..12 {
                drain_rechecks(&mut primary, &mut wm_rx).await;
                drain_commands(&mut primary).await;
                let mut round: Vec<(String, String, u32, String)> = Vec::new();
                for (_id, rx, _tx) in ends.iter_mut() {
                    round.extend(assignments(rx));
                }
                if round.is_empty() {
                    break;
                }
                for (_, sec, worker, h) in &round {
                    if *h == import_hash {
                        import_failed_on.push(sec.clone());
                        primary
                            .handle_task_failed(task_failed(sec, *worker, h), &mut None)
                            .await;
                        settle_pump().await;
                    } else if *h == build_hash {
                        // A build must NEVER dispatch (every secondary's import
                        // cell is Failed/NotDone, never Done) — the per-secondary
                        // readiness gate forbids it.
                        build_dispatched = true;
                    }
                }
            }

            assert!(
                !build_dispatched,
                "the build must never dispatch — its import never reached Done on \
                 any secondary"
            );
            // The import was attempted (and failed) on BOTH secondaries — proof
            // the gate RE-ROUTED off the first failed secondary before giving up.
            import_failed_on.sort();
            import_failed_on.dedup();
            assert_eq!(
                import_failed_on,
                vec!["sec-0".to_string(), "sec-1".to_string()],
                "the import must be re-routed to (and fail on) BOTH secondaries \
                 before the dependent is terminal-failed"
            );
            for sec in ["sec-0", "sec-1"] {
                assert_eq!(
                    primary.cluster_state_for_test().affine_state(sec, affine_id),
                    AffineCell::Failed,
                    "the import cell must be Failed on {sec}"
                );
            }

            // Drain any final terminal-fail command the last gate pass enqueued.
            drain_commands(&mut primary).await;

            // TERMINAL-FAIL (the livelock fix): the dependent build reached a
            // permanent Failed terminal — it did not spin forever.
            assert!(
                primary
                    .cluster_state_for_test()
                    .task_state(&build_hash)
                    .is_some_and(|s| s.is_terminal()),
                "the dependent build must be terminal-failed once its import \
                 cannot be satisfied on any secondary (no livelock)"
            );
            // No affine-dep work task lingers queued (it was taken out of its
            // bucket by the terminal-fail path).
            assert_eq!(
                primary
                    .pool()
                    .iter()
                    .filter(|t| compute_task_hash(t) == build_hash)
                    .count(),
                0,
                "the terminal-failed build must be removed from its bucket"
            );
        })
        .await;
}

/// RELOCATION-REBUILD STRANDED IMPORT (bug 3): the prior primary PLACED the
/// affine import (cell → `Queued` on a secondary) but RELOCATED before
/// DISPATCHING it, so the import unit lived ONLY in that primary's now-lost
/// node-local queue. After the promoted primary's rebuild, a `Queued`-cell
/// import that NO live worker holds must be RECONSTRUCTED on its secondary's
/// queue (ahead of its dependent work) — without this the dependent pops and
/// spins forever on the `InFlightHere` gate (the hot re-pop worker storm). The
/// cell STAYS `Queued` (no CRDT mutation); only the lost node-local unit is
/// restored.
#[tokio::test(flavor = "current_thread")]
async fn rebuild_reconstructs_stranded_queued_import_not_worker_held() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let import = affine_import("import");
            let import_hash = compute_task_hash(&import);
            let build = work_dep("build", "import");
            let build_hash = compute_task_hash(&build);

            let (mut primary, _ends, _wm_rx, _mesh) =
                primary_two_secondaries_with(vec![import, build]);
            // Roster + capacity present (the rebuild ranks over the live roster
            // and reconstruct_workers reads the per-secondary capacity).
            confirm_two(&mut primary).await;

            let affine_id = primary
                .cluster_state_for_test()
                .affine_id_for_hash(&import_hash)
                .expect("registered affine-id");

            // Simulate the inherited replicated state: the prior primary CLAIMED
            // the import on sec-0 (cell → Queued) but never dispatched it — so
            // there is NO TaskAssigned (no live worker holds it). This is the
            // bitvector the promoted primary inherits.
            primary
                .cluster_state_mut_for_test()
                .apply(ClusterMutation::SecondaryAffineQueued {
                    secondary: "sec-0".into(),
                    affine_id: affine_id.0,
                    generation: 1,
                });
            assert_eq!(
                primary.cluster_state_for_test().affine_state("sec-0", affine_id),
                AffineCell::Queued,
                "fixture: the inherited cell is Queued on sec-0"
            );

            // Promotion rebuild: reconstruct the roster (crosses any InFlight
            // occupancy — here NONE for the import) then re-derive the queues.
            primary.reconstruct_workers_from_cluster_state();
            primary.rebuild_affine_schedule();

            // The stranded import unit must be reconstructed on sec-0's queue,
            // AHEAD of its dependent work (so the import dispatches, runs,
            // terminals the cell Done, and the build's gate then dispatches).
            assert_eq!(
                primary.affine_queue_hashes_for_test("sec-0"),
                vec![import_hash.clone(), build_hash.clone()],
                "the placed-but-undispatched import must be restored ahead of \
                 its work on sec-0's queue"
            );
            // The cell is UNCHANGED (still Queued — the restore emits no
            // mutation; the claim was already on sec-0).
            assert_eq!(
                primary.cluster_state_for_test().affine_state("sec-0", affine_id),
                AffineCell::Queued,
                "restoring the lost unit must NOT mutate the cell"
            );
        })
        .await;
}

/// RELOCATION-REBUILD IN-FLIGHT IMPORT (the discriminator's OTHER arm): a
/// `Queued`-cell import that IS held by a live (reconstructed) worker on its
/// secondary is a mid-run failover — the running import will terminal. The
/// rebuild must NOT reconstruct its unit (doing so would DOUBLE-RUN the import,
/// violating affine run-once-per-secondary + the failover no-reassign-in-flight
/// law). Only the dependent work is queued, gated until the running import's
/// terminal sets the cell Done.
#[tokio::test(flavor = "current_thread")]
async fn rebuild_leaves_inflight_queued_import_worker_held() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let import = affine_import("import");
            let import_hash = compute_task_hash(&import);
            let build = work_dep("build", "import");
            let build_hash = compute_task_hash(&build);

            let (mut primary, _ends, _wm_rx, _mesh) =
                primary_two_secondaries_with(vec![import, build]);
            confirm_two(&mut primary).await;

            let affine_id = primary
                .cluster_state_for_test()
                .affine_id_for_hash(&import_hash)
                .expect("registered affine-id");

            // Inherited state: the prior primary CLAIMED (cell → Queued) AND
            // DISPATCHED the import — the replicated InFlight fact names sec-0's
            // worker 0 as the live holder.
            {
                let cs = primary.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::SecondaryAffineQueued {
                    secondary: "sec-0".into(),
                    affine_id: affine_id.0,
                    generation: 1,
                });
                cs.apply(ClusterMutation::TaskAssigned {
                    attempt: 0,
                    hash: import_hash.clone(),
                    secondary: "sec-0".into(),
                    worker: 0,
                    version: Default::default(),
                });
            }

            // Promotion rebuild: reconstruct_workers crosses the InFlight import
            // onto sec-0's slot, so the rebuild's held-discriminator sees the
            // import as worker-held on sec-0.
            primary.reconstruct_workers_from_cluster_state();
            assert!(
                primary.slot_holds_hash_for_test("sec-0", 0, &import_hash),
                "fixture: the reconstructed sec-0/worker-0 slot holds the import"
            );
            primary.rebuild_affine_schedule();

            // The worker-held import must NOT be re-queued: only the dependent
            // work sits on the queue (no import unit ahead of it).
            assert_eq!(
                primary.affine_queue_hashes_for_test("sec-0"),
                vec![build_hash.clone()],
                "a worker-held (in-flight) import must NOT be reconstructed — \
                 only the dependent work is queued (no double-run)"
            );
        })
        .await;
}

/// REGRESSION (affine run-completion): a per-secondary affine import that RUNS
/// ON BOTH SECONDARIES (same hash, N concurrent terminals) plus its dependent
/// builds must, once everything terminals, satisfy the operational loop's
/// run-completion gate (`run_complete_check`) — RunComplete-eligible — with the
/// import counted EXACTLY ONCE toward the run tally.
///
/// The wedge this pins: the import dispatches the SAME hash on every secondary,
/// but the primary's `in_flight` ledger is hash-keyed (one holder per hash), so
/// each per-secondary `commit_assignment` overwrites the prior secondary's
/// entry. The pre-fix terminal-free routed through that colliding ledger, freed
/// the WRONG secondary's slot, and left the reporting worker's slot `Assigned`
/// forever — `active_workers >= 1` — so BOTH arms of `run_complete_check`
/// (counter AND pool-drain, each gated on `active_workers == 0`) never tripped
/// and `RunComplete` never fired despite every phase draining to `Done` and the
/// CRDT showing all tasks complete. The slot-direct affine terminal-free
/// (`free_affine_slot_on_terminal`) frees the slot by the terminal's OWN
/// `(secondary, worker)`, so every per-secondary run releases its own slot.
///
/// Includes the EMPTY upstream phases (produce/consume/setup) the live consumer
/// declares so the topology matches the on-cluster `secondary-affine` scenario —
/// the same shape the unit tests above omitted, which is why their
/// `pool().is_run_complete()` assertion passed while the live run hung on the
/// `active_workers` half of the gate.
#[tokio::test(flavor = "current_thread")]
async fn affine_import_on_n_secondaries_satisfies_run_completion_once() {
    use dynrunner_scheduler_api::pending_pool::PhaseState;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut import = affine_import("import");
            import.phase_id = PhaseId::from("import");
            let import_hash = compute_task_hash(&import);
            let make_build = |name: &str| {
                let mut t = make_binary(name, 20);
                t.phase_id = PhaseId::from("build");
                t.type_id = TypeId::from("default");
                t.task_depends_on = vec![TaskDep {
                    task_id: "import".into(),
                    phase_id: PhaseId::from("import"),
                    inherit_outputs: false,
                    def_id: None,
                }];
                t
            };
            let mut binaries = vec![import];
            for i in 0..8 {
                binaries.push(make_build(&format!("build_{i}")));
            }
            // The live `secondary-affine` topology: empty produce/consume/setup
            // phases ALONGSIDE the import + build phases, no inter-phase
            // depends_on edges except build→import.
            let (mut primary, mut ends, mut wm_rx, _mesh) =
                primary_two_secondaries_with_phase_deps(
                    binaries,
                    HashMap::from([
                        (PhaseId::from("produce"), vec![]),
                        (PhaseId::from("consume"), vec![]),
                        (PhaseId::from("setup"), vec![]),
                        (PhaseId::from("import"), vec![]),
                        (PhaseId::from("build"), vec![PhaseId::from("import")]),
                    ]),
                );
            let on_start: OnPhaseStart = Box::new(|_p: &PhaseId| {});
            let on_end: OnPhaseEnd = Box::new(move |_p, _c, _f, _o| {});
            primary.register_phase_lifecycle_callbacks(on_start, on_end);
            confirm_two(&mut primary).await;

            // Drive to quiescence: import dispatches + completes per-secondary,
            // builds dispatch (each gated on its own secondary's import cell),
            // every task terminals.
            drain_rechecks(&mut primary, &mut wm_rx).await;
            loop {
                let mut round: Vec<(String, String, u32, String)> = Vec::new();
                for (_id, rx, _tx) in ends.iter_mut() {
                    round.extend(assignments(rx));
                }
                if round.is_empty() {
                    break;
                }
                for (_, sec, worker, h) in &round {
                    primary
                        .handle_task_complete(task_complete(sec, *worker, h), &mut None)
                        .await;
                    settle_pump().await;
                }
                drain_rechecks(&mut primary, &mut wm_rx).await;
            }

            // The import ran on BOTH secondaries (N concurrent terminals of the
            // SAME hash) — the precondition that triggers the ledger collision.
            let affine_id = primary
                .cluster_state_for_test()
                .affine_id_for_hash(&import_hash)
                .expect("registered affine-id");
            for sec in ["sec-0", "sec-1"] {
                assert_eq!(
                    primary.cluster_state_for_test().affine_state(sec, affine_id),
                    AffineCell::Done,
                    "the affine import must have RUN on {sec} (N concurrent runs)"
                );
            }

            // Every phase drained to Done (incl. the empty upstream phases).
            for p in ["produce", "consume", "setup", "import", "build"] {
                assert_eq!(
                    primary.pool().phase_state(&PhaseId::from(p)),
                    Some(PhaseState::Done),
                    "phase {p} must drain to Done"
                );
            }

            // EVERY worker slot freed — no orphaned `Assigned` slot from a
            // per-secondary import terminal that the colliding ledger mis-routed.
            assert_eq!(
                primary.active_workers_for_test(),
                0,
                "no worker slot may stay Assigned after every task terminals — \
                 the per-secondary import terminal must free ITS OWN slot, not \
                 whichever holder the single hash-keyed ledger recorded"
            );

            // The import counted ONCE toward the run tally despite running on N
            // secondaries: 1 import + 8 builds = 9 distinct CRDT terminals.
            assert_eq!(
                primary.cluster_state_counts_for_test().completed,
                9,
                "the import counts exactly once (CRDT TaskCompleted is first-run \
                 only); 1 import + 8 builds = 9"
            );

            // THE GATE: the operational loop's run-completion check is satisfied
            // — RunComplete-eligible. Pre-fix this stayed false forever because
            // active_workers never reached 0.
            assert!(
                primary.run_complete_check(),
                "run_complete_check must be satisfied once the import + all builds \
                 terminal and every slot is freed — the affine run-completion arc"
            );
        })
        .await;
}

/// REGRESSION (per-secondary run-once dispatch): the affine import must dispatch
/// EXACTLY ONCE per secondary even under CONCURRENCY — many idle workers on one
/// secondary plus many dependents that each drag the import into the queue.
///
/// The placement appends the import once PER dependent work task, so K ready
/// dependents on one secondary enqueue K redundant import units. Pre-fix all K
/// were popped by K idle workers and dispatched concurrently (the live
/// `already_held` storm: only one ran, the rest stranded `Assigned` slots that
/// never terminal — wedging run completion), AND a leftover unit popped after
/// the first run completed re-dispatched the import a SECOND time (a run-once
/// violation: the per-secondary import body ran twice). The dispatch guard
/// (`dispatch_affine_unit` → `secondary_has_slot_holding_hash` + the bitvector
/// `Done` cell) drops every redundant unit, so the import dispatches once.
///
/// Single secondary, MANY workers, MANY dependents → forces the concurrent pop.
#[tokio::test(flavor = "current_thread")]
async fn affine_import_dispatches_once_per_secondary_under_concurrency() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let import = affine_import("import");
            let import_hash = compute_task_hash(&import);
            let mut binaries = vec![import];
            for i in 0..6 {
                binaries.push(work_dep(&format!("build_{i}"), "import"));
            }
            // ONE secondary with MANY workers (so K dependents' import units are
            // all poppable concurrently in a single recheck pass).
            let (mut primary, mut ends, mut wm_rx, _mesh) =
                primary_two_secondaries_with(binaries);
            // sec-0: 8 workers; sec-1: 1 (kept minimal). Mesh-confirm both.
            primary
                .handle_cluster_mutation(capacity_batch("sec-0", 8), &mut None)
                .await;
            primary.handle_mesh_ready(mesh_ready_from("sec-0"));
            primary
                .handle_cluster_mutation(capacity_batch("sec-1", 1), &mut None)
                .await;
            primary.handle_mesh_ready(mesh_ready_from("sec-1"));

            // Drive to quiescence, counting every IMPORT dispatch per secondary.
            drain_rechecks(&mut primary, &mut wm_rx).await;
            let mut import_dispatches_by_sec: std::collections::HashMap<String, u32> =
                std::collections::HashMap::new();
            loop {
                let mut round: Vec<(String, String, u32, String)> = Vec::new();
                for (_id, rx, _tx) in ends.iter_mut() {
                    round.extend(assignments(rx));
                }
                if round.is_empty() {
                    break;
                }
                for (_, sec, _, h) in &round {
                    if *h == import_hash {
                        *import_dispatches_by_sec.entry(sec.clone()).or_insert(0) += 1;
                    }
                }
                for (_, sec, worker, h) in &round {
                    primary
                        .handle_task_complete(task_complete(sec, *worker, h), &mut None)
                        .await;
                    settle_pump().await;
                }
                drain_rechecks(&mut primary, &mut wm_rx).await;
            }

            // The import dispatched AT MOST ONCE per secondary — no concurrent
            // storm, no sequential re-run. (Builds on these e2e-local-like runs
            // cluster on one secondary, so only the building secondary's import
            // dispatches; whichever secondaries ran it ran it exactly once.)
            for (sec, count) in &import_dispatches_by_sec {
                assert_eq!(
                    *count, 1,
                    "the affine import must dispatch EXACTLY ONCE on {sec}; got \
                     {count} (a concurrent storm or a sequential re-run)"
                );
            }
            assert!(
                !import_dispatches_by_sec.is_empty(),
                "the import must have dispatched on at least one secondary"
            );

            // Run completes cleanly: every slot freed, gate satisfied.
            assert_eq!(
                primary.active_workers_for_test(),
                0,
                "no worker slot may stay Assigned — no stranded already-held import slot"
            );
            assert!(
                primary.run_complete_check(),
                "run_complete_check must be satisfied once the run drains"
            );
        })
        .await;
}
