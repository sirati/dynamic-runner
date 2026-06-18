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
use dynrunner_protocol_primary_secondary::SecondaryCell;

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

/// A `Work` task depending IN ORDER on two affine prereqs `[base, delta]` — the
/// consumer's `build_variant` shape (a shared base import precedes the
/// per-variant delta layered on top of it). The dep ORDER (base first) is the
/// dispatch order the list-order gate must respect.
fn work_two_deps(name: &str, base: &str, delta: &str) -> TaskInfo<TestId> {
    let mut t = make_binary(name, 20);
    t.phase_id = PhaseId::from("work");
    t.type_id = TypeId::from("default");
    t.task_depends_on = vec![
        TaskDep {
            task_id: base.into(),
            phase_id: PhaseId::from("work"),
            inherit_outputs: false,
            def_id: None,
        },
        TaskDep {
            task_id: delta.into(),
            phase_id: PhaseId::from("work"),
            inherit_outputs: false,
            def_id: None,
        },
    ];
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
                let cell_id = cs.allocate_cell_id(&hash).0;
                cs.apply(ClusterMutation::SecondaryCellRegistered { hash, cell_id });
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
                                SecondaryCell::Done,
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
                    SecondaryCell::Done,
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
                    SecondaryCell::Done,
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
                    SecondaryCell::Done,
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
                    SecondaryCell::Failed,
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

/// AFFINE FAILED-PATH MIRROR (commit-1 fix): an affine-only IMPORT phase whose
/// import FAILS on every eligible secondary must DRAIN its own phase to `Done` —
/// the failed twin of the complete-path's `note_affine_terminal`. Pre-fix the
/// failed path recorded NOTHING in the pool, so a globally-failed import held
/// its phase's Gate B (`phase_has_live_affine_prereq`) forever and the import
/// phase stranded `Active`. Post-fix `note_affine_failed` (fired once the import
/// is `Failed` on every roster secondary) clears Gate B + re-runs the drain, and
/// the phase reaches `Done`.
///
/// Separate-phase topology (import alone in "import", build in "build") so the
/// assertion isolates the IMPORT phase's own drain — distinct from the
/// dependent-cascade the same-phase `affine_import_failed_everywhere…` covers.
#[tokio::test(flavor = "current_thread")]
async fn affine_only_phase_import_failed_everywhere_drains_import_phase() {
    use dynrunner_scheduler_api::pending_pool::PhaseState;

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let import = affine_import("import");
            let import_hash = compute_task_hash(&import);

            let mut build = make_binary("build", 20);
            build.phase_id = PhaseId::from("build");
            build.type_id = TypeId::from("default");
            build.task_depends_on = vec![TaskDep {
                task_id: "import".into(),
                phase_id: PhaseId::from("import"),
                inherit_outputs: false,
                def_id: None,
            }];

            let (mut primary, mut ends, mut wm_rx, _mesh) =
                primary_two_secondaries_with_phase_deps(
                    vec![import, build],
                    HashMap::from([
                        (PhaseId::from("import"), vec![]),
                        (PhaseId::from("build"), vec![PhaseId::from("import")]),
                    ]),
                );
            confirm_two(&mut primary).await;

            // While the import is live (not yet failed everywhere) the import
            // phase stays Active — Gate B holds it (the #617 invariant).
            assert_eq!(
                primary.pool().phase_state(&PhaseId::from("import")),
                Some(PhaseState::Active),
                "the import phase holds Active while its import is still live"
            );

            // Drive the run, FAILING every dispatched import on every secondary.
            // The gate re-routes off each failed secondary; once the import is
            // Failed on BOTH, it is globally failed.
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
                        primary
                            .handle_task_failed(task_failed(sec, *worker, h), &mut None)
                            .await;
                        settle_pump().await;
                    }
                }
            }
            drain_commands(&mut primary).await;

            let affine_id = primary
                .cluster_state_for_test()
                .affine_id_for_hash(&import_hash)
                .expect("registered affine-id");
            for sec in ["sec-0", "sec-1"] {
                assert_eq!(
                    primary.cluster_state_for_test().affine_state(sec, affine_id),
                    SecondaryCell::Failed,
                    "the import cell must be Failed on {sec}"
                );
            }

            // THE FIX: the import phase drained to Done once its import reached a
            // global terminal failure (Gate B cleared by `note_affine_failed`).
            assert_eq!(
                primary.pool().phase_state(&PhaseId::from("import")),
                Some(PhaseState::Done),
                "the import phase must drain to Done once its import is failed on \
                 every secondary — not strand Active forever (the failed-path \
                 lost-mirror fix)"
            );
        })
        .await;
}

/// A BACKPRESSURE-shaped `TaskFailed` from `secondary`/`worker` for `task_hash`
/// — the exact wire shape a type-shift worker respawn's first-bind reinject
/// sends ("worker pipe broken; respawning"). NOT a terminal: the task never ran
/// to completion anywhere; it is a re-queue signal.
fn task_failed_backpressure(
    secondary: &str,
    worker: u32,
    task_hash: &str,
) -> DistributedMessage<TestId> {
    DistributedMessage::TaskFailed {
        target: None,
        sender_id: secondary.into(),
        timestamp: 0.0,
        secondary_id: secondary.into(),
        worker_id: worker,
        task_hash: task_hash.into(),
        error_type: dynrunner_core::ErrorType::Recoverable,
        error_message: "worker pipe broken; respawning".into(),
        delivery_seq: None,
        msgs_posted_through: None,
    }
}

/// A CAPACITY-shaped backpressure `TaskFailed` ("No idle worker available") —
/// the GENUINE-capacity bounce the secondary's dispatch sends when every worker
/// is busy with OTHER work. The #656 M2 brake gates this shape (sets the
/// secondary's backpressure flag); the pipe-broken shape above does NOT.
fn task_failed_capacity_bounce(
    secondary: &str,
    worker: u32,
    task_hash: &str,
) -> DistributedMessage<TestId> {
    DistributedMessage::TaskFailed {
        target: None,
        sender_id: secondary.into(),
        timestamp: 0.0,
        secondary_id: secondary.into(),
        worker_id: worker,
        task_hash: task_hash.into(),
        error_type: dynrunner_core::ErrorType::Recoverable,
        error_message: "No idle worker available".into(),
        delivery_seq: None,
        msgs_posted_through: None,
    }
}

/// REGRESSION (affine-import backpressure-bounce wedge): an on-demand affine
/// IMPORT that its secondary BACKPRESSURE-bounces (a type-shift worker respawn
/// → first-bind reinject `TaskFailed{error_message="worker pipe broken;
/// respawning"}`) must be recovered BY the affine subsystem — its per-secondary
/// cell reset `Queued → NotDone` and its slot freed slot-direct — NEVER routed
/// into the WORK pool.
///
/// Pre-fix: the bounce SKIPPED the affine handler (the gate required
/// `!is_backpressure_shaped`) and fell into the generic work-pool requeue arm,
/// which `pool.requeue`'d the import (a `SecondaryAffine` task —
/// `is_worker_assignable() == false`, so the work pool can NEVER re-surface it)
/// and left the cell `Queued`. The dependent work unit at the front of the
/// secondary's affine queue then re-popped forever onto `InFlightHere`
/// (cell == `Queued`) — a permanent stall, the import terminal having already
/// left as the swallowed bounce.
///
/// Post-fix: the cell goes back to `NotDone`; the next pop reads `StrandedHere`
/// and re-dispatches the import on-demand on a Ready worker; the import is NEVER
/// in the work pool.
#[tokio::test(flavor = "current_thread")]
async fn affine_import_backpressure_bounce_resets_cell_and_redispatches_not_pool() {
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

            // Round 1 — the dependent commits to a secondary and pops; its gate
            // reads `NotDone` → `StrandedHere` → the import is dispatched
            // on-demand there (cell `Queued`, the slot Assigned), the work
            // requeued at the queue front. Capture which secondary it landed on.
            drain_rechecks(&mut primary, &mut wm_rx).await;
            let mut import_dispatch: Option<(String, u32)> = None;
            for (_id, rx, _tx) in ends.iter_mut() {
                for (_, sec, worker, h) in assignments(rx) {
                    assert_ne!(
                        h, build_hash,
                        "the build must NOT dispatch before its import is Done"
                    );
                    if h == import_hash {
                        assert!(
                            import_dispatch.is_none(),
                            "the import dispatches on exactly ONE secondary (one dependent)"
                        );
                        import_dispatch = Some((sec.clone(), worker));
                    }
                }
            }
            let (sec, worker) =
                import_dispatch.expect("the on-demand import dispatched (StrandedHere)");

            // The import is in flight on this secondary: cell `Queued`, slot held.
            assert_eq!(
                primary.cluster_state_for_test().affine_state(&sec, affine_id),
                SecondaryCell::Queued,
                "on-demand dispatch claims the cell Queued"
            );
            assert!(
                primary.secondary_has_slot_holding_hash(&sec, &import_hash),
                "the import slot is Assigned on {sec}"
            );
            // The import sits in the pool as its ONE non-worker-assignable
            // ledger TOKEN (the placement-readiness signal — never taken out;
            // affine units are UNcounted). Capture the count so the bounce can be
            // proven not to push a SECOND, work-pool copy. The pre-fix mis-route
            // `pool.requeue`'d the import binary → a duplicate bucket entry.
            let import_pool_count_before = primary
                .pool()
                .iter()
                .filter(|t| compute_task_hash(t) == import_hash)
                .count();
            assert_eq!(
                import_pool_count_before, 1,
                "the affine import is its one ledger token in the pool"
            );

            // ── THE BOUNCE: a backpressure-shaped TaskFailed for the import. ──
            primary
                .handle_task_failed(task_failed_backpressure(&sec, worker, &import_hash), &mut None)
                .await;
            settle_pump().await;

            // GREEN: the cell is reset to `NotDone` (NOT `Failed`, NOT left
            // `Queued`), the slot freed, and — critically — the import is NOT in
            // the work pool (the pre-fix mis-route would have dropped it there).
            assert_eq!(
                primary.cluster_state_for_test().affine_state(&sec, affine_id),
                SecondaryCell::NotDone,
                "a backpressure bounce resets the import cell Queued → NotDone \
                 (not Failed, not left Queued)"
            );
            assert!(
                !primary.secondary_has_slot_holding_hash(&sec, &import_hash),
                "the import slot is freed by the bounce"
            );
            assert_eq!(
                primary
                    .pool()
                    .iter()
                    .filter(|t| compute_task_hash(t) == import_hash)
                    .count(),
                import_pool_count_before,
                "the affine import must NEVER be requeued into the work pool \
                 (the pre-fix mis-route `pool.requeue`'d a SECOND copy of a \
                 SecondaryAffine binary that is_worker_assignable == false could \
                 never re-surface); the lone ledger token is unchanged"
            );

            // The dependent build is still pending, not terminal-failed (the
            // bounce is recoverable, not a genuine import failure).
            assert!(
                primary
                    .cluster_state_for_test()
                    .task_state(&build_hash)
                    .is_some_and(|s| !s.is_terminal()),
                "a backpressure bounce must NOT terminal-fail the dependent"
            );

            // Round 2 — the freed worker re-evaluates; the dependent re-pops
            // `StrandedHere` (cell == `NotDone`) and re-dispatches the import
            // on-demand. RED pre-fix: the cell stayed `Queued`, so the re-pop hit
            // `InFlightHere` forever and no fresh import dispatched.
            drain_rechecks(&mut primary, &mut wm_rx).await;
            let mut import_redispatched = false;
            for (_id, rx, _tx) in ends.iter_mut() {
                for (_, _sec, _worker, h) in assignments(rx) {
                    assert_ne!(
                        h, build_hash,
                        "the build still must NOT dispatch before its import is Done"
                    );
                    if h == import_hash {
                        import_redispatched = true;
                    }
                }
            }
            assert!(
                import_redispatched,
                "the import must be RE-DISPATCHED on-demand after the bounce reset \
                 its cell to NotDone (StrandedHere) — not wedged InFlightHere forever"
            );
            assert_eq!(
                primary.cluster_state_for_test().affine_state(&sec, affine_id),
                SecondaryCell::Queued,
                "the re-dispatch re-claims the cell Queued"
            );
        })
        .await;
}

/// #665 (the WIRE twin of the backpressure-bounce wedge above): an ON-DEMAND
/// affine IMPORT that COMMITS against this primary's model slot
/// (`dispatch_affine_import_on_demand` → `commit_assignment`) but is then bounced
/// over the wire as `IllegallyAssignedToNonidleWorker` — the secondary's PHYSICAL
/// pool had shrunk/respawned (stale roster: out-of-range id / 0-worker pool /
/// mid-respawn) so the slot was not idle — must be recovered BY the affine
/// subsystem, identically to the backpressure-`TaskFailed` import bounce: its
/// per-secondary cell reset `Queued → NotDone`, its slot freed slot-direct, and
/// the blocked dependent re-derived. NEVER routed into the WORK pool.
///
/// Pre-fix: `handle_illegally_assigned` took the generic work-path requeue
/// (`free_slot_on_terminal` + `requeue_affine_aware`, which `pool.requeue`'d the
/// import — a `SecondaryAffine` task, `is_worker_assignable() == false`, the work
/// pool can NEVER re-surface it) and left the cell `Queued`. The dependent work
/// at the front of the secondary's affine queue then re-popped forever onto
/// `InFlightHere` (cell == `Queued`) — the strand, the import's terminal having
/// already left as the swallowed bounce.
///
/// Post-fix: the cell goes back to `NotDone`; the next pop reads `StrandedHere`
/// and re-dispatches the import on-demand on a fresh slot; the import is NEVER in
/// the work pool. Byte-for-byte the recoverable twin of the backpressure bounce
/// (`affine_import_backpressure_bounce_resets_cell_and_redispatches_not_pool`).
#[tokio::test(flavor = "current_thread")]
async fn affine_import_illegal_bounce_resets_cell_and_redispatches_not_pool() {
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

            // Round 1 — the dependent commits + pops `StrandedHere`, dispatching
            // the import on-demand on exactly one secondary (cell `Queued`, slot
            // Assigned). Capture where it landed.
            drain_rechecks(&mut primary, &mut wm_rx).await;
            let mut import_dispatch: Option<(String, u32)> = None;
            for (_id, rx, _tx) in ends.iter_mut() {
                for (_, sec, worker, h) in assignments(rx) {
                    assert_ne!(
                        h, build_hash,
                        "the build must NOT dispatch before its import is Done"
                    );
                    if h == import_hash {
                        assert!(
                            import_dispatch.is_none(),
                            "the import dispatches on exactly ONE secondary (one dependent)"
                        );
                        import_dispatch = Some((sec.clone(), worker));
                    }
                }
            }
            let (sec, worker) =
                import_dispatch.expect("the on-demand import dispatched (StrandedHere)");

            // The import is in flight on this secondary: cell `Queued`, slot held.
            assert_eq!(
                primary.cluster_state_for_test().affine_state(&sec, affine_id),
                SecondaryCell::Queued,
                "on-demand dispatch claims the cell Queued"
            );
            assert!(
                primary.secondary_has_slot_holding_hash(&sec, &import_hash),
                "the import slot is Assigned on {sec}"
            );
            let import_pool_count_before = primary
                .pool()
                .iter()
                .filter(|t| compute_task_hash(t) == import_hash)
                .count();
            assert_eq!(
                import_pool_count_before, 1,
                "the affine import is its one ledger token in the pool"
            );

            // ── THE BOUNCE: an `IllegallyAssignedToNonidleWorker` divergence
            // report for the on-demand import — the secondary's physical slot was
            // not idle (stale roster). No incumbent (the degenerate stale-roster
            // shape: out-of-range id / 0-worker pool / mid-respawn) — the affine
            // recovery half is independent of the slot-reconcile half. ──
            primary
                .handle_illegally_assigned(DistributedMessage::IllegallyAssignedToNonidleWorker {
                    target: None,
                    sender_id: sec.clone(),
                    timestamp: 0.0,
                    secondary_id: sec.clone(),
                    worker_id: worker,
                    assigned: dynrunner_protocol_primary_secondary::AssignedTaskRef {
                        hash: import_hash.clone(),
                        task_id: TestId("import".into()),
                    },
                    incumbent: None,
                })
                .await;
            settle_pump().await;

            // GREEN: the cell is reset `Queued → NotDone` (NOT Failed, NOT left
            // Queued), the slot freed slot-direct, and the import is NOT pushed
            // into the work pool (the pre-fix mis-route's `pool.requeue` copy).
            assert_eq!(
                primary.cluster_state_for_test().affine_state(&sec, affine_id),
                SecondaryCell::NotDone,
                "an illegal-assignment bounce of an on-demand import resets its \
                 cell Queued → NotDone (RED pre-fix: stayed Queued → the strand)"
            );
            assert!(
                !primary.secondary_has_slot_holding_hash(&sec, &import_hash),
                "the import slot is freed by the bounce"
            );
            assert_eq!(
                primary
                    .pool()
                    .iter()
                    .filter(|t| compute_task_hash(t) == import_hash)
                    .count(),
                import_pool_count_before,
                "the affine import must NEVER be requeued into the work pool \
                 (the pre-fix work-path `pool.requeue`'d a SecondaryAffine binary \
                 that is_worker_assignable == false could never re-surface)"
            );

            // The dependent build is still pending, not terminal-failed (a bounce
            // is recoverable, not a genuine import failure).
            assert!(
                primary
                    .cluster_state_for_test()
                    .task_state(&build_hash)
                    .is_some_and(|s| !s.is_terminal()),
                "an illegal-assignment bounce must NOT terminal-fail the dependent"
            );

            // Round 2 — the freed worker re-evaluates; the dependent re-pops
            // `StrandedHere` (cell == `NotDone`) and re-dispatches the import.
            // RED pre-fix: the cell stayed `Queued`, so the re-pop hit
            // `InFlightHere` forever and no fresh import dispatched.
            drain_rechecks(&mut primary, &mut wm_rx).await;
            let mut import_redispatched = false;
            for (_id, rx, _tx) in ends.iter_mut() {
                for (_, _sec, _worker, h) in assignments(rx) {
                    assert_ne!(
                        h, build_hash,
                        "the build still must NOT dispatch before its import is Done"
                    );
                    if h == import_hash {
                        import_redispatched = true;
                    }
                }
            }
            assert!(
                import_redispatched,
                "the import must be RE-DISPATCHED on-demand after the bounce reset \
                 its cell to NotDone (StrandedHere) — not wedged InFlightHere forever"
            );
            assert_eq!(
                primary.cluster_state_for_test().affine_state(&sec, affine_id),
                SecondaryCell::Queued,
                "the re-dispatch re-claims the cell Queued"
            );
        })
        .await;
}

/// #660 (the #659 affine-strand twin): an ON-DEMAND import dispatch whose inner
/// dispatch does NOT COMMIT (`dispatch_affine_unit` returns `false` — a
/// `CommitRefused`, the target worker slot is not idle, or a `SendFailed`) must
/// HONOR that outcome: reset the import cell `Queued → NotDone` and re-derive
/// the dependent blocked work, exactly as the backpressure-bounce arm treats a
/// wire bounce. Without the fix the helper SWALLOWS the inner outcome — the cell
/// stays phantom-`Queued` with no holding slot, the dependent waits
/// `InFlightHere` on a cell no terminal will ever flip `Done`, and recovery
/// depends on the 5-min reconcile (reconcile-paced drain) instead of being
/// continuous.
///
/// The non-commit is forced as the `CommitRefused` shape: the worker the build
/// pops on is pre-occupied (busy), so the on-demand import — dispatched to that
/// SAME worker — hits the #517 idle-guard and `commit_assignment` refuses, the
/// `false` outcome. (The mesh egress always accepts a queued send in the test
/// harness, so `SendFailed` is not separately inducible here; `CommitRefused`
/// and `SendFailed` are the same `false` branch the fix keys on.)
///
/// REVERT-CHECK: with the swallow restored (the inner bool discarded), the cell
/// stays `Queued` and the build stays blocked-on-import — stranded until the
/// 5-min reconcile orphans the phantom-Queued cell.
#[tokio::test(flavor = "current_thread")]
async fn affine_on_demand_import_noncommit_resets_cell_and_redrains_dependent() {
    use dynrunner_core::ResourceMap;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let import = affine_import("import");
            let import_hash = compute_task_hash(&import);
            let build = work_dep("build", "import");
            let build_hash = compute_task_hash(&build);

            let (mut primary, _ends, _wm_rx, _mesh) =
                primary_two_secondaries_with(vec![import, build.clone()]);
            confirm_two(&mut primary).await;

            let affine_id = primary
                .cluster_state_for_test()
                .affine_id_for_hash(&import_hash)
                .expect("registered affine-id");

            // STAGE the strand on sec-0: enqueue the build on sec-0's per-secondary
            // affine queue (the lazy-import placement state), cell NotDone. Then
            // OCCUPY sec-0's worker so the on-demand import — which dispatches to
            // that SAME worker — is refused (#517 idle-guard).
            let placement = primary.affine_placement_for(&build);
            let _: Vec<ClusterMutation<TestId>> = primary.affine_scheduler.place(
                "sec-0",
                &placement,
                |_s: &str, _a: crate::cluster_state::SecondaryCellId| SecondaryCell::NotDone,
            );
            let worker_idx = primary
                .worker_idx_for("sec-0", 0)
                .expect("sec-0 has a worker in the roster");
            let occupier = make_binary("occupier", 10);
            assert!(
                primary.commit_assignment(
                    worker_idx,
                    std::sync::Arc::new(occupier.clone()),
                    compute_task_hash(&occupier),
                    ResourceMap::new(),
                ),
                "occupier must commit onto the idle slot (the #517 guard takes here)"
            );

            // Pre-state: cell NotDone, no holding slot for the import.
            assert_eq!(
                primary.cluster_state_for_test().affine_state("sec-0", affine_id),
                SecondaryCell::NotDone,
            );

            // DRIVE the per-secondary pop for the (now-busy) worker. The build pops
            // → `StrandedHere` (cell NotDone) → the build is BLOCKED + the import
            // is dispatched on-demand to this SAME (busy) worker → the inner
            // `commit_assignment` is REFUSED (slot not idle) → `dispatch_affine_unit`
            // returns `false` (the non-committed branch).
            let committed = primary.try_affine_pop_for_worker(worker_idx).await;
            settle_pump().await;
            assert!(
                !committed,
                "the work pop's gate took the StrandedHere arm (it does not commit \
                 the work; it blocks it and kicks the import)"
            );

            // GREEN: the swallowed non-commit reset the import cell back to
            // `NotDone` (no phantom `Queued`-no-holder), no holding slot landed for
            // the import, and the build was re-derived (drained from the blocked
            // map). RED pre-fix: the cell stays `Queued` and the build stays
            // blocked-on-import until the 5-min reconcile.
            assert_eq!(
                primary.cluster_state_for_test().affine_state("sec-0", affine_id),
                SecondaryCell::NotDone,
                "a non-committed on-demand import must reset its cell Queued → \
                 NotDone (RED pre-fix: left phantom-Queued with no holding slot)"
            );
            assert!(
                !primary.secondary_has_slot_holding_hash("sec-0", &import_hash),
                "the refused import never landed a holding slot on sec-0"
            );
            assert!(
                !primary.affine_is_blocked_on_import_for_test("sec-0", &build_hash),
                "the dependent build must be re-derived (drained from sec-0's \
                 per-secondary blocked map), NOT stranded until reconcile (RED \
                 pre-fix: still blocked on the phantom-Queued cell)"
            );

            // The build is still pending, NOT terminal-failed: a non-commit is a
            // RECOVERABLE refusal (the import can re-run), never a genuine import
            // failure — the cell reset to NotDone (not Failed) and the dependent
            // stays alive.
            assert!(
                primary
                    .cluster_state_for_test()
                    .task_state(&build_hash)
                    .is_some_and(|s| !s.is_terminal()),
                "a recoverable on-demand non-commit must NOT terminal-fail the dependent"
            );
        })
        .await;
}

/// #656 M2 (affine import-bounce capacity brake): an on-demand affine import
/// refused with the GENUINE CAPACITY shape ("No idle worker available") must set
/// the secondary's backpressure flag so `should_skip_worker_for_dispatch`
/// (already invoked before `try_affine_pop_for_worker`) skips it for affine pops
/// too — braking the import-bounce micro-loop (W re-pops → StrandedHere → import
/// refused → bounce → reset → re-enqueue → re-pop). The flag event-clears on the
/// next real capacity event (the #652 TaskComplete clear). A NON-capacity bounce
/// (pipe-broken/preempt/…) is NOT capacity-exhausted and must recover promptly,
/// so it must NOT set the flag.
#[tokio::test(flavor = "current_thread")]
async fn affine_import_capacity_bounce_sets_flag_noncapacity_does_not() {
    // Stage an on-demand import dispatch (round-1 StrandedHere, the SAME staging
    // the existing bounce test uses) and return the secondary it landed on + its
    // worker + the import hash. Macro (not a fn) to keep the staging inline and
    // sidestep returning the borrow-entangled (primary, ends, wm_rx) bundle.
    macro_rules! stage_import_in_flight {
        ($primary:ident) => {{
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

            drain_rechecks(&mut primary, &mut wm_rx).await;
            let mut import_dispatch: Option<(String, u32)> = None;
            for (_id, rx, _tx) in ends.iter_mut() {
                for (_, sec, worker, h) in assignments(rx) {
                    assert_ne!(h, build_hash, "build must not dispatch before import Done");
                    if h == import_hash {
                        import_dispatch = Some((sec.clone(), worker));
                    }
                }
            }
            let (sec, worker) =
                import_dispatch.expect("the on-demand import dispatched (StrandedHere)");
            assert_eq!(
                primary.cluster_state_for_test().affine_state(&sec, affine_id),
                SecondaryCell::Queued,
                "on-demand dispatch claims the cell Queued"
            );
            $primary = primary;
            (sec, worker, import_hash)
        }};
    }

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // ── CAPACITY bounce → flag SET. ──
            let mut primary;
            let (sec, worker, import_hash) = stage_import_in_flight!(primary);
            assert!(
                !primary.is_backpressured(&sec),
                "no backpressure flag before the bounce"
            );
            primary
                .handle_task_failed(
                    task_failed_capacity_bounce(&sec, worker, &import_hash),
                    &mut None,
                )
                .await;
            settle_pump().await;
            assert!(
                primary.is_backpressured(&sec),
                "a CAPACITY bounce (No idle worker available) sets the secondary's \
                 backpressure flag — the affine import-bounce micro-loop brake"
            );

            // ── NON-capacity bounce → flag NOT set. ──
            let mut primary2;
            let (sec2, worker2, import_hash2) = stage_import_in_flight!(primary2);
            primary2
                .handle_task_failed(
                    task_failed_backpressure(&sec2, worker2, &import_hash2),
                    &mut None,
                )
                .await;
            settle_pump().await;
            assert!(
                !primary2.is_backpressured(&sec2),
                "a NON-capacity bounce (pipe-broken) must NOT set the backpressure \
                 flag — it is not capacity-exhausted and must recover promptly"
            );
        })
        .await;
}

/// PROTOCOL: the affine subsystem's backpressure-recovery mutation builder
/// emits `SecondaryCellUnqueued` (the Queued → NotDone cell reset) for an
/// affine hash — and NEVER a `TaskRequeued` (the work-pool requeue that the
/// pre-fix mis-route wrongly used, which does not touch the bitvector cell at
/// all). A non-affine hash builds `None` (symmetric with
/// `affine_terminal_mutation`), so an ordinary work bounce keeps using the
/// generic work-pool requeue.
#[tokio::test(flavor = "current_thread")]
async fn affine_backpressure_recovery_emits_unqueued_never_task_requeued() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let import = affine_import("import");
            let import_hash = compute_task_hash(&import);
            let work = work_dep("build", "import");
            let work_hash = compute_task_hash(&work);

            let (primary, _ends, _wm_rx, _mesh) =
                primary_two_secondaries_with(vec![import, work.clone()]);
            let affine_id = primary
                .cluster_state_for_test()
                .affine_id_for_hash(&import_hash)
                .expect("registered affine-id");

            // The affine import's bounce recovery is a `SecondaryCellUnqueued`
            // (Queued → NotDone) — NOT a `TaskRequeued`.
            match primary.affine_unqueue_mutation("sec-0", &import_hash) {
                Some(ClusterMutation::SecondaryCellUnqueued {
                    secondary,
                    cell_id: aid,
                    generation,
                }) => {
                    assert_eq!(secondary, "sec-0");
                    assert_eq!(aid, affine_id.0);
                    assert_eq!(generation, 0);
                }
                other => panic!(
                    "affine backpressure recovery must emit SecondaryCellUnqueued, \
                     got {other:?}"
                ),
            }

            // A non-affine (ordinary work) hash builds `None` — its bounce still
            // routes through the generic work-pool requeue, unchanged.
            assert!(
                primary.affine_unqueue_mutation("sec-0", &work_hash).is_none(),
                "a non-affine hash has no affine cell to reset — None"
            );
        })
        .await;
}

/// Drive the on-demand import to COMPLETION on every secondary that dispatches
/// it, then return the `(secondary, worker, build_hash)` of the dependent WORK
/// task once IT dispatches (its cell now `Done`). Used by the affine-dep-WORK
/// requeue-recovery tests below, which must bounce the WORK task (not the
/// import) after it has committed to a worker. Bounded so a regression that
/// never dispatches the build surfaces as a failed expect, not a hang.
#[allow(clippy::type_complexity)]
async fn drive_until_work_dispatches(
    primary: &mut TestPrimary,
    ends: &mut [(
        String,
        tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
        tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
    )],
    wm_rx: &mut tokio_mpsc::UnboundedReceiver<WorkerMgmtSignal>,
    import_hash: &str,
    build_hash: &str,
) -> (String, u32) {
    for _round in 0..12 {
        drain_rechecks(primary, wm_rx).await;
        let mut round: Vec<(String, String, u32, String)> = Vec::new();
        for (_id, rx, _tx) in ends.iter_mut() {
            round.extend(assignments(rx));
        }
        if round.is_empty() {
            continue;
        }
        for (_, sec, worker, h) in &round {
            if h == import_hash {
                // Complete the import on its secondary → cell Done, so the
                // dependent build can then dispatch there.
                primary
                    .handle_task_complete(task_complete(sec, *worker, h), &mut None)
                    .await;
                settle_pump().await;
            } else if h == build_hash {
                // The build committed to a worker — this is what the recovery
                // tests bounce.
                return (sec.clone(), *worker);
            }
        }
    }
    panic!("the dependent build never dispatched within the round budget");
}

/// AFFINE-DEP-WORK REQUEUE RECOVERY (the #646 twin for affine-DEPENDENT WORK):
/// a backpressure-bounced affine-dep WORK task must be RE-DERIVED onto a
/// secondary's affine queue and re-dispatched — NOT stranded.
///
/// RED at 751b7377: the backpressure arm `pool.requeue`'d the work binary
/// (correct — the pool item is its ready-state) but left the affine scheduler's
/// `placed_work` guard SET. The work is withheld from the global worker view by
/// `has_affine_dep`, so it can never dispatch globally; and `placed_work` blocks
/// `place_dependency_satisfied_affine_tasks` from re-deriving its per-secondary
/// queue unit. Result: hidden in the global pool, absent from every affine
/// queue, blocked from re-placement — permanently unassignable.
///
/// GREEN: `requeue_affine_aware` clears `placed_work` on the bounce, so the
/// SAME same-tick `TasksAdded` recheck re-runs the placement pass (re-derives +
/// re-queues the unit onto a rank-selected secondary) and `try_affine_pop`
/// re-dispatches it. The fix is JUST the guard-clear — no pool-routing change.
#[tokio::test(flavor = "current_thread")]
async fn affine_dep_work_backpressure_bounce_recovers_to_affine_queue_not_stranded() {
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

            // Drive the import to Done on its secondary, then let the dependent
            // build dispatch (cell Done). Capture where the build committed.
            let (sec, worker) = drive_until_work_dispatches(
                &mut primary,
                &mut ends,
                &mut wm_rx,
                &import_hash,
                &build_hash,
            )
            .await;

            // Pre-bounce invariants: the build is committed (slot Assigned), and
            // its placement-dedup guard is SET (it was placed). It is the lone
            // pool item too (its ready-state).
            assert!(
                primary.secondary_has_slot_holding_hash(&sec, &build_hash),
                "the build slot must be Assigned on {sec} before the bounce"
            );
            assert!(
                primary.affine_work_is_placed_for_test(&build_hash),
                "the build's placement-dedup guard must be SET (it was placed)"
            );

            // ── THE BOUNCE: a backpressure-shaped TaskFailed for the WORK task. ──
            primary
                .handle_task_failed(
                    task_failed_backpressure(&sec, worker, &build_hash),
                    &mut None,
                )
                .await;
            settle_pump().await;

            // GREEN (the fix): the placement-dedup guard was CLEARED by the
            // affine-aware requeue. RED at 751b7377: it stayed SET — the strand.
            assert!(
                !primary.affine_work_is_placed_for_test(&build_hash),
                "the bounce must UNRECORD the affine-dep work's placement guard \
                 so the placement pass re-derives its queue unit (RED at \
                 751b7377: the guard stayed set → permanently unassignable)"
            );
            // The strand-diagnostic count (placed-but-in-no-queue) the
            // unassignable-park line reports is 0 right after the bounce: the
            // guard was cleared, so the work is no longer placed-but-unqueued.
            // RED at 751b7377: the guard stayed set while the queue unit was gone
            // → count 1 (the strand signature this diagnostic names).
            assert_eq!(
                primary.affine_scheduler_placed_but_unqueued_for_test(),
                0,
                "post-bounce the affine-dep work is not placed-but-unqueued \
                 (guard cleared); RED at 751b7377 it was the strand (count 1)"
            );

            // The build slot was freed by the terminal-free; the work is back in
            // the pool (its ready-state), hidden from the global view by
            // has_affine_dep — exactly the steady state the affine channel feeds
            // off. It is NOT terminal-failed (a bounce is recoverable).
            assert!(
                primary
                    .cluster_state_for_test()
                    .task_state(&build_hash)
                    .is_some_and(|s| !s.is_terminal()),
                "a backpressure bounce must NOT terminal-fail the affine-dep work"
            );

            // The SAME same-tick TasksAdded the bounce emitted re-derives the
            // unit + re-dispatches it. RED at 751b7377: no re-derivation (guard
            // set) → no affine-queue entry → no dispatch → stranded forever.
            drain_rechecks(&mut primary, &mut wm_rx).await;
            let mut build_redispatched = false;
            for (_id, rx, _tx) in ends.iter_mut() {
                for (_, _sec, _worker, h) in assignments(rx) {
                    if h == build_hash {
                        build_redispatched = true;
                    }
                }
            }
            assert!(
                build_redispatched,
                "the affine-dep work must be RE-DISPATCHED after the bounce \
                 cleared its placement guard (StrandedHere re-derivation) — not \
                 wedged hidden-in-pool / absent-from-every-affine-queue forever"
            );
        })
        .await;
}

/// AFFINE-DEP-WORK TERMINAL CLEARS THE PLACEMENT GUARD + the strand diagnostic
/// NEVER names a terminal task (#663). Two assertions:
///   (a1, ROOT) a COMPLETED affine-dep work task is `unrecord_placed_work`'d out
///       of the placement-dedup guard on its terminal — `placed_work` must not
///       grow with completed tasks (a scale leak) and a completed task is NOT
///       in flight, so a lingering guard entry shows as a FALSE strand.
///   (a2, BELT-AND-SUSPENDERS) even under a clear-RACE (the guard re-recorded
///       AFTER the terminal, simulating a path that missed the clear), the
///       coordinator strand diagnostic STILL excludes the hash because it filters
///       out `completed_tasks ∪ failed_tasks` — so a terminal task can never be
///       named as a strand regardless of the guard's state.
///
/// RED before #663: the completion path flipped only the affine cell (an import
/// def's bitvector) and never cleared the affine-DEP WORK task's `placed_work`
/// guard, so a completed work lingered placed-but-unqueued-and-not-in-flight =
/// the false strand the consumer chased (asm-dataset 549509fa, a matrix_eval
/// named 4–8 min after completion).
#[tokio::test(flavor = "current_thread")]
async fn affine_dep_work_terminal_clears_guard_and_strand_diagnostic_excludes_terminal() {
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

            // Drive the import Done, let the dependent build commit; capture its
            // holder. The build is now placed (guard SET) and in flight.
            let (sec, worker) = drive_until_work_dispatches(
                &mut primary,
                &mut ends,
                &mut wm_rx,
                &import_hash,
                &build_hash,
            )
            .await;
            assert!(
                primary.affine_work_is_placed_for_test(&build_hash),
                "the build's placement guard must be SET before its terminal"
            );

            // ── THE TERMINAL: a genuine TaskComplete for the WORK task. ──
            primary
                .handle_task_complete(task_complete(&sec, worker, &build_hash), &mut None)
                .await;
            settle_pump().await;

            // (a1) ROOT: the terminal cleared the placement-dedup guard. RED
            // before #663: only the affine cell flipped; the guard stayed set.
            assert!(
                !primary.affine_work_is_placed_for_test(&build_hash),
                "a COMPLETED affine-dep work must be UNRECORDED from the \
                 placement guard on its terminal (RED before #663: it lingered \
                 → false strand + unbounded placed_work growth at scale)"
            );
            // The strand diagnostic is 0 — nothing placed-but-unqueued survives.
            assert_eq!(
                primary.affine_scheduler_placed_but_unqueued_for_test(),
                0,
                "no false strand after a clean terminal clear"
            );

            // (a2) BELT-AND-SUSPENDERS: simulate a clear-RACE by RE-recording the
            // guard for the (now-completed) hash. The root clear is bypassed, so
            // the guard is set AND the hash sits in no queue AND is not in flight
            // (it terminated). The ONLY thing keeping it out of the strand list
            // is the diagnostic's terminal filter.
            assert!(
                primary.affine_record_placed_work_for_test(&build_hash),
                "re-recording the completed hash reinstates the racy guard entry"
            );
            assert_eq!(
                primary.affine_scheduler_placed_but_unqueued_for_test(),
                0,
                "the strand diagnostic must EXCLUDE a terminal hash even when its \
                 placement guard lingers (clear-race) — the completed/failed \
                 terminal filter is the belt-and-suspenders guard (#663)"
            );
        })
        .await;
}

/// AFFINE-DEP-WORK DEAD-SECONDARY RECOVERY (the mid-run-leg-drop variant of the
/// same recovery): when the holder of a committed affine-dep WORK task DIES,
/// `recover_inflight_for_dead_secondary` requeues it — and must clear the
/// `placed_work` guard, or the requeued work is stranded exactly as in the
/// backpressure case (hidden from the global view, blocked from re-placement).
///
/// RED at 751b7377: the dead-secondary requeue called the bare `pool.requeue`,
/// leaving the guard set → strand. GREEN: it now routes through
/// `requeue_affine_aware`, clearing the guard so the work re-derives.
#[tokio::test(flavor = "current_thread")]
async fn affine_dep_work_dead_secondary_recovery_clears_placement_guard() {
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

            let (sec, _worker) = drive_until_work_dispatches(
                &mut primary,
                &mut ends,
                &mut wm_rx,
                &import_hash,
                &build_hash,
            )
            .await;

            // The build is in flight on `sec` and its placement guard is set.
            assert!(
                primary.affine_work_is_placed_for_test(&build_hash),
                "the build's placement-dedup guard must be SET before the holder dies"
            );

            // ── THE HOLDER DIES: recover its in-flight work (the dead-secondary
            // requeue path). This is the unit directly under test. ──
            let muts = primary.recover_inflight_for_dead_secondary(&sec);
            // The work was requeued (InFlight → Pending), proving it took the
            // reassignable WORK arm (not the veto / setup-fail arms).
            assert!(
                muts.iter().any(|m| matches!(
                    m,
                    ClusterMutation::TaskRequeued { hash, .. } if *hash == build_hash
                )),
                "the dead holder's affine-dep work must be requeued (TaskRequeued)"
            );

            // GREEN (the fix): the placement-dedup guard was CLEARED by the
            // affine-aware requeue. RED at 751b7377: it stayed SET — the strand.
            assert!(
                !primary.affine_work_is_placed_for_test(&build_hash),
                "the dead-secondary requeue must UNRECORD the affine-dep work's \
                 placement guard (RED at 751b7377: stayed set → stranded)"
            );
        })
        .await;
}

/// AFFINE-DEP-WORK ILLEGAL-ASSIGNMENT-BOUNCE RECOVERY (#659): the THIRD
/// requeue-of-recovered-work site — the illegal-assignment bounce handler. When
/// a secondary bounces a committed affine-dep WORK task with an
/// `IllegallyAssignedToNonidleWorker` divergence report, the handler requeues
/// it; that requeue MUST be affine-aware (clear `placed_work`), exactly as the
/// backpressure-failed and dead-secondary siblings already are.
///
/// RED with the bare `pool.requeue` (pre-#659): the requeue left `placed_work`
/// SET, so the affine-dep work was hidden from the global worker view by
/// `has_affine_dep` AND blocked from re-placement by the placement-dedup guard
/// — permanently unassignable, and NOT recovered by the 5-min reconcile (a
/// bounced work is not in `blocked_per_secondary`). GREEN: routing through
/// `requeue_affine_aware` clears the guard so the same-tick `TasksAdded` recheck
/// re-derives its per-secondary unit and re-dispatches it.
#[tokio::test(flavor = "current_thread")]
async fn affine_dep_work_illegal_assignment_bounce_recovers_not_stranded() {
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

            // Drive the import Done + let the dependent build commit; capture
            // the (secondary, worker) the build committed onto.
            let (sec, worker) = drive_until_work_dispatches(
                &mut primary,
                &mut ends,
                &mut wm_rx,
                &import_hash,
                &build_hash,
            )
            .await;

            // Pre-bounce: the build is committed and its placement-dedup guard
            // is SET (it was placed).
            assert!(
                primary.secondary_has_slot_holding_hash(&sec, &build_hash),
                "the build slot must be Assigned on {sec} before the bounce"
            );
            assert!(
                primary.affine_work_is_placed_for_test(&build_hash),
                "the build's placement-dedup guard must be SET (it was placed)"
            );

            // ── THE BOUNCE: an illegal-assignment divergence report for the
            // committed WORK task. (No incumbent: the requeue half — the path
            // under test — runs regardless; the slot-reconcile half is
            // independently covered by the #517/#531 illegal-assignment tests.) ──
            primary
                .handle_illegally_assigned(DistributedMessage::IllegallyAssignedToNonidleWorker {
                    target: None,
                    sender_id: sec.clone(),
                    timestamp: 0.0,
                    secondary_id: sec.clone(),
                    worker_id: worker,
                    assigned: dynrunner_protocol_primary_secondary::AssignedTaskRef {
                        hash: build_hash.clone(),
                        task_id: TestId("build".into()),
                    },
                    incumbent: None,
                })
                .await;
            settle_pump().await;

            // GREEN (the #659 fix): the placement-dedup guard was CLEARED by the
            // affine-aware requeue, so the work re-admits to placement. RED with
            // the bare `pool.requeue`: the guard stayed SET → permanently
            // unassignable (the strand).
            assert!(
                !primary.affine_work_is_placed_for_test(&build_hash),
                "the illegal-assignment bounce must UNRECORD the affine-dep \
                 work's placement guard so the placement pass re-derives its \
                 queue unit (RED with the bare pool.requeue: the guard stayed \
                 set → stranded)"
            );
            assert_eq!(
                primary.affine_scheduler_placed_but_unqueued_for_test(),
                0,
                "post-bounce the affine-dep work is not placed-but-unqueued \
                 (guard cleared); RED with the bare requeue it was the strand"
            );

            // Not terminal-failed — a bounce is recoverable.
            assert!(
                primary
                    .cluster_state_for_test()
                    .task_state(&build_hash)
                    .is_some_and(|s| !s.is_terminal()),
                "an illegal-assignment bounce must NOT terminal-fail the work"
            );

            // The SAME same-tick TasksAdded the bounce emitted re-derives the
            // unit + re-dispatches it. RED with the bare requeue: no
            // re-derivation (guard set) → no affine-queue entry → stranded.
            drain_rechecks(&mut primary, &mut wm_rx).await;
            let mut build_redispatched = false;
            for (_id, rx, _tx) in ends.iter_mut() {
                for (_, _sec, _worker, h) in assignments(rx) {
                    if h == build_hash {
                        build_redispatched = true;
                    }
                }
            }
            assert!(
                build_redispatched,
                "the affine-dep work must be RE-DISPATCHED after the bounce \
                 cleared its placement guard — not wedged hidden-in-pool / \
                 absent-from-every-affine-queue forever"
            );
        })
        .await;
}

/// RELOCATION-REBUILD under lazy import (c): the rebuild re-derives ONLY the
/// dependent WORK units — it never reconstructs an import unit on the queue,
/// regardless of the inherited `Queued` cell. The import is a DEPENDENCY derived
/// on-demand when the work commits (the `StrandedHere` arm reads the live cell),
/// so there is nothing to reconstruct and nothing to double-run: an in-flight
/// inherited import terminals normally (cell flips, the rebuilt work gates), and
/// a stranded one (its holder also died) re-derives a fresh import on-demand.
/// This supersedes the eager model's reconstruct-vs-leave `import_held`
/// discriminator (both arms — stranded-restore and in-flight-leave — collapse to
/// "queue only the work").
#[tokio::test(flavor = "current_thread")]
async fn rebuild_queues_only_work_import_derived_on_demand() {
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

            // Inherited bitvector: the prior primary CLAIMED the import on sec-0
            // (cell → Queued). Whether or not it dispatched it, the rebuild does
            // NOT reconstruct an import unit.
            primary
                .cluster_state_mut_for_test()
                .apply(ClusterMutation::SecondaryCellQueued {
                    secondary: "sec-0".into(),
                    cell_id: affine_id.0,
                    generation: 1,
                });

            primary.reconstruct_workers_from_cluster_state();
            primary.rebuild_affine_schedule();

            // ONLY the work is queued (on whichever secondary rank chose) — no
            // import unit reconstructed anywhere. The import re-derives on-demand
            // when the work commits.
            let sec0_q = primary.affine_queue_hashes_for_test("sec-0");
            let sec1_q = primary.affine_queue_hashes_for_test("sec-1");
            let mut all: Vec<String> = sec0_q.into_iter().chain(sec1_q).collect();
            all.sort();
            assert_eq!(
                all,
                vec![build_hash.clone()],
                "rebuild queues ONLY the work; no import unit is reconstructed"
            );
            assert!(
                !all.contains(&import_hash),
                "the import is never an enqueued unit under lazy import"
            );
        })
        .await;
}

/// FAILOVER REBUILD vs IMPORT-READINESS (#669): the rebuild must NOT place an
/// affine-dep WORK whose import is NOT yet ready-in-bucket — i.e. the import's
/// OWN non-affine upstream is still incomplete. The live placement trigger gates
/// each placement on `all_ready` (every affine import sitting in a pool bucket =
/// its own deps met); the failover rebuild placed EVERY affine-dep work with no
/// such gate, so a promoted primary queued a work whose pop dispatched the
/// not-ready import — the import ran with a PARTIAL `gather_predecessor_outputs`
/// (its upstream's output missing) → failed → spurious cascade. The fix routes
/// BOTH paths through the one shared placeability predicate
/// (`affine_work_placeability` over `affine_ready_in_bucket_imports`), so the
/// rebuild defers a not-ready work exactly as the live trigger does.
///
/// Topology: ordinary Work upstream U → affine import I (non-affine edge on U) →
/// affine-dep Work W (affine edge on I). At rebuild time U is INCOMPLETE, so I
/// is blocked on U (NOT a ready bucket item). Assert the rebuild does NOT place
/// W (so the not-ready import is never dispatched off a rebuilt queue). THEN
/// complete U so I becomes ready-in-bucket, run the LIVE placement, and assert W
/// IS now placed + its import dispatches — the deferred work is correctly picked
/// up once its import is ready.
///
/// RED without the gate: the rebuild places W → W is in an affine queue right
/// after the rebuild (the premature placement this gate removes). GREEN: the
/// queues are empty post-rebuild and W places only after U completes.
#[tokio::test(flavor = "current_thread")]
async fn rebuild_defers_work_whose_import_upstream_incomplete() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let upstream = work_upstream("import_upstream");
            let upstream_hash = compute_task_hash(&upstream);
            let import = affine_import_dep("import", "import_upstream");
            let import_hash = compute_task_hash(&import);
            let build = work_dep("build", "import");
            let build_hash = compute_task_hash(&build);

            let (mut primary, mut ends, mut wm_rx, _mesh) =
                primary_two_secondaries_with(vec![upstream, import, build]);
            confirm_two(&mut primary).await;

            // Precondition: the import is BLOCKED on its incomplete non-affine
            // upstream — it is NOT a ready bucket item, so it is NOT in the
            // ready-in-bucket set the placeability gate reads. The affine-dep
            // work W, by contrast, IS a ready bucket item (its only dep is the
            // affine import, excluded from its global blocking set).
            assert!(
                !primary
                    .pool()
                    .iter()
                    .any(|t| compute_task_hash(t) == import_hash),
                "the affine import must be BLOCKED on its incomplete upstream \
                 (not a ready bucket item)"
            );
            assert!(
                primary
                    .pool()
                    .iter()
                    .any(|t| compute_task_hash(t) == build_hash),
                "the affine-dep work must be a READY bucket item (its affine \
                 import dep is excluded from its global blocking set)"
            );

            // ── THE REBUILD (promoted-primary seam) while the import is not yet
            // ready-in-bucket. ──
            primary.reconstruct_workers_from_cluster_state();
            primary.rebuild_affine_schedule();

            // GREEN (the #669 gate): the rebuild placed NOTHING — W is deferred
            // because its import is not ready-in-bucket, so no per-secondary
            // queue holds a work whose pop would dispatch the not-ready import.
            // RED without the gate: the rebuild placed EVERY affine-dep work, so
            // W would already be in a queue here (the premature placement).
            let post_rebuild: Vec<String> = primary
                .affine_queue_hashes_for_test("sec-0")
                .into_iter()
                .chain(primary.affine_queue_hashes_for_test("sec-1"))
                .collect();
            assert!(
                post_rebuild.is_empty(),
                "the rebuild must NOT place a work whose import upstream is \
                 incomplete (the not-ready import must not be dispatchable off a \
                 rebuilt queue); got {post_rebuild:?}"
            );
            assert!(
                !primary.affine_work_is_placed_for_test(&build_hash),
                "the deferred work must NOT have its placement-dedup guard set, \
                 so the live placement trigger can place it once its import is \
                 ready"
            );

            // ── COMPLETE THE UPSTREAM so the import becomes ready-in-bucket,
            // then drive the whole arc to quiescence. U dispatches as an ordinary
            // work task; the import derives + runs ONLY after U completes; W is
            // placed + dispatched off the per-secondary queue (not stranded). We
            // record that the import NEVER dispatched before its upstream
            // completed (the premature-import shape the rebuild gate prevents).
            let affine_id = primary
                .cluster_state_for_test()
                .affine_id_for_hash(&import_hash)
                .expect("registered affine-id for the import");
            let mut upstream_done = false;
            let mut import_dispatched = false;
            let mut import_before_upstream = false;
            let mut build_dispatched = false;
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
                    if *h == upstream_hash {
                        upstream_done = true;
                    }
                    if *h == import_hash {
                        import_dispatched = true;
                        if !upstream_done {
                            import_before_upstream = true;
                        }
                    }
                    if *h == build_hash {
                        build_dispatched = true;
                    }
                    primary
                        .handle_task_complete(task_complete(sec, *worker, h), &mut None)
                        .await;
                    settle_pump().await;
                }
                drain_rechecks(&mut primary, &mut wm_rx).await;
            }
            assert!(
                upstream_done,
                "the non-affine upstream must dispatch + complete so the import \
                 becomes ready-in-bucket"
            );

            // The import dispatched ONLY AFTER its upstream completed — never off
            // a prematurely-rebuilt queue. (The rebuild deferred W, so no pop
            // dispatched the not-ready import; the live trigger placed W and
            // dragged the import in only once it was ready-in-bucket.)
            assert!(
                import_dispatched,
                "the import must derive + run once its upstream completed (the \
                 deferred work is picked up + dispatched, not stranded)"
            );
            assert!(
                !import_before_upstream,
                "the import must NOT dispatch before its non-affine upstream \
                 completes — a premature import would gather partial predecessor \
                 outputs and fail (the #669 failover asymmetry)"
            );

            // W ran (it was placed by the LIVE trigger once its import became
            // ready) and the whole affine arc drained — the deferred work was
            // not stranded.
            assert!(
                build_dispatched,
                "the deferred affine-dep work must dispatch once its import is \
                 ready — placed by the LIVE trigger, not stranded"
            );
            let import_ran = ["sec-0", "sec-1"].iter().any(|sec| {
                primary.cluster_state_for_test().affine_state(sec, affine_id)
                    == SecondaryCell::Done
            });
            assert!(
                import_ran,
                "the import's per-secondary cell must be Done — it derived + ran"
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
                    SecondaryCell::Done,
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

            // The 8 BUILD (work) tasks counted toward the generic `completed`
            // tally; the affine IMPORT is EXCLUDED from the work buckets and
            // reported in its OWN flat `secondary_affine` count (the kind
            // split — a per-secondary affine GATE token is phase-uncounted,
            // never folded into generic work `completed`). The import's CRDT
            // terminal is still recorded ONCE (first-run-only) despite running
            // on N secondaries — surfaced as `secondary_affine == 1`.
            let counts = primary.cluster_state_counts_for_test();
            assert_eq!(
                counts.completed, 8,
                "only the 8 work BUILDS count toward generic `completed` — the \
                 affine import is excluded from the work buckets"
            );
            assert_eq!(
                counts.secondary_affine, 1,
                "the affine import is reported in its own flat secondary_affine \
                 count (one CRDT entry despite N per-secondary runs)"
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

/// EVENT-DRIVEN BATCH FAST-FAIL (the slow-drain fix): when the affine import
/// fails on the LAST eligible secondary — the gate transitions to
/// all-eligible-`Failed` — EVERY dependent WORK unit is terminal-failed in ONE
/// sweep, driven by the failure EVENT, NOT lazily one-per-dispatch-tick.
///
/// The slow-drain bug: `Unsatisfiable → FailPermanent` was only evaluated
/// per-WORK-unit at dispatch time, so N dependents drained at the dispatch
/// loop's per-tick rate (the live ~0.2 fails/sec across 12.5k dependents while
/// workers sat idle). This pins the fix: N dependents on a single failed
/// affine_id all terminal-fail PROMPTLY off the LAST import-failure terminal,
/// WITHOUT N separate dispatch rounds. The proof of BATCH (not per-tick): we
/// fail the import on both secondaries (NO build dispatch round in between) and
/// then assert ALL N builds are terminal after a single command drain.
#[tokio::test(flavor = "current_thread")]
async fn affine_all_failed_batch_fast_fails_every_dependent_promptly() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let import = affine_import("import");
            let import_hash = compute_task_hash(&import);
            // MANY dependents on the one affine_id — the batch must fail them all
            // off the single failure transition, not one per dispatch tick.
            let mut binaries = vec![import];
            let mut build_hashes = Vec::new();
            for i in 0..10 {
                let b = work_dep(&format!("build_{i}"), "import");
                build_hashes.push(compute_task_hash(&b));
                binaries.push(b);
            }

            let (mut primary, mut ends, mut wm_rx, _mesh) =
                primary_two_secondaries_with(binaries);
            confirm_two(&mut primary).await;

            let affine_id = primary
                .cluster_state_for_test()
                .affine_id_for_hash(&import_hash)
                .expect("registered affine-id");

            // Dispatch the placement + per-secondary import pops, then FAIL every
            // dispatched IMPORT (never a build — builds gate `NotDone`/`Failed`).
            // Crucially we keep draining/failing imports until BOTH secondaries'
            // cells are `Failed`, never feeding a build terminal — so any builds
            // that fail must fail via the BATCH event-driven sweep, not a
            // per-build dispatch gate.
            for _round in 0..6 {
                drain_rechecks(&mut primary, &mut wm_rx).await;
                let mut round: Vec<(String, String, u32, String)> = Vec::new();
                for (_id, rx, _tx) in ends.iter_mut() {
                    round.extend(assignments(rx));
                }
                for (_, sec, worker, h) in &round {
                    if *h == import_hash {
                        primary
                            .handle_task_failed(task_failed(sec, *worker, h), &mut None)
                            .await;
                        settle_pump().await;
                    } else {
                        // A build dispatching would mean the fast-fail did NOT
                        // pre-empt the per-dispatch drain — fail loudly.
                        assert!(
                            !build_hashes.contains(h),
                            "a build dispatched ({h}) — the batch fast-fail should \
                             have terminal-failed every dependent off the import \
                             failure, before any build was popped for a worker"
                        );
                    }
                }
                let both_failed = ["sec-0", "sec-1"].iter().all(|sec| {
                    primary.cluster_state_for_test().affine_state(sec, affine_id)
                        == SecondaryCell::Failed
                });
                if both_failed {
                    break;
                }
            }

            // The import failed on BOTH secondaries — the all-eligible-`Failed`
            // transition that arms the batch fast-fail.
            for sec in ["sec-0", "sec-1"] {
                assert_eq!(
                    primary.cluster_state_for_test().affine_state(sec, affine_id),
                    SecondaryCell::Failed,
                    "the import cell must be Failed on {sec} (the arming transition)"
                );
            }

            // ONE command drain (the batch enqueued N decoupled `FailPermanent`s
            // off the LAST failure event) — and then EVERY dependent is terminal.
            // No per-build dispatch rounds were needed.
            drain_commands(&mut primary).await;

            for (i, h) in build_hashes.iter().enumerate() {
                assert!(
                    primary
                        .cluster_state_for_test()
                        .task_state(h)
                        .is_some_and(|s| s.is_terminal()),
                    "build_{i} must be terminal-failed by the BATCH sweep off the \
                     all-Failed transition — not drained one per dispatch tick"
                );
                // And removed from its pool bucket (the symmetric accounting the
                // fast-fail path does).
                assert_eq!(
                    primary.pool().iter().filter(|t| compute_task_hash(t) == *h).count(),
                    0,
                    "build_{i} must be taken out of its bucket by the fast-fail sweep"
                );
            }
        })
        .await;
}

/// ROSTER-AWARE / NO OVER-FAST-FAIL (partial): when the import has `Failed` on
/// one secondary but a DIFFERENT secondary can still satisfy it, the dependents
/// are NOT batch-failed — they dispatch to the still-satisfiable secondary once
/// its import is `Done`. The batch fast-fail only fires when the gate is
/// all-eligible-`Failed`, so a partial failure (one Done elsewhere) preserves
/// the existing reroute/dispatch semantics exactly.
#[tokio::test(flavor = "current_thread")]
async fn affine_partial_failed_does_not_fast_fail_dependents() {
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

            // Drive the run: FAIL the import on the FIRST secondary it dispatches
            // to, but COMPLETE it on the second. The dependent must NOT be
            // batch-failed (a satisfiable secondary still exists), and must
            // ultimately dispatch + complete on the Done secondary.
            let mut build_completed = false;
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
                        // Fail on sec-0, complete on sec-1 (one stays satisfiable).
                        if sec == "sec-0" {
                            primary
                                .handle_task_failed(task_failed(sec, *worker, h), &mut None)
                                .await;
                        } else {
                            primary
                                .handle_task_complete(task_complete(sec, *worker, h), &mut None)
                                .await;
                        }
                        settle_pump().await;
                    } else if *h == build_hash {
                        // The build dispatched on a Done secondary — complete it.
                        assert_eq!(
                            primary.cluster_state_for_test().affine_state(sec, affine_id),
                            SecondaryCell::Done,
                            "the build must only dispatch where the import is Done"
                        );
                        primary
                            .handle_task_complete(task_complete(sec, *worker, h), &mut None)
                            .await;
                        build_completed = true;
                        settle_pump().await;
                    }
                }
            }
            drain_commands(&mut primary).await;

            // The build was NOT fast-failed — it RAN to completion on the Done
            // secondary. (A terminal that is a SUCCESS, not the permanent fail
            // the all-Failed batch would produce.)
            assert!(
                build_completed,
                "the build must dispatch + complete on the still-satisfiable \
                 secondary — a partial failure must NOT batch-fast-fail it"
            );
            assert!(
                matches!(
                    primary.cluster_state_for_test().task_state(&build_hash),
                    Some(crate::cluster_state::TaskState::Completed { .. })
                ),
                "the build's terminal must be a COMPLETION (not the permanent \
                 fail the over-fast-fail bug would produce)"
            );
        })
        .await;
}

/// ROSTER-AWARE / FRESH SECONDARY (no premature fail): a dependent whose import
/// has `Failed` on the only WRITTEN secondary is NOT batch-failed when a FRESH
/// secondary (all cells `NotDone`) is still on the roster — the gate reads the
/// ROSTER, not just the written cells, so a placeable secondary keeps the unit
/// satisfiable. The build must re-route to (and run on) the fresh secondary.
#[tokio::test(flavor = "current_thread")]
async fn affine_fresh_secondary_keeps_gate_satisfiable_no_premature_fail() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let import = affine_import("import");
            let import_hash = compute_task_hash(&import);
            let build = work_dep("build", "import");
            let build_hash = compute_task_hash(&build);

            let (mut primary, mut ends, mut wm_rx, _mesh) =
                primary_two_secondaries_with(vec![import, build]);
            // Confirm ONLY sec-0 first — sec-1 stays off-roster (fresh) while we
            // fail the import on sec-0. With sec-1 on the roster (all cells
            // NotDone), the gate must NOT fast-fail despite sec-0's Failed cell.
            primary
                .handle_cluster_mutation(capacity_batch("sec-0", 1), &mut None)
                .await;
            primary.handle_mesh_ready(mesh_ready_from("sec-0"));
            primary
                .handle_cluster_mutation(capacity_batch("sec-1", 1), &mut None)
                .await;
            primary.handle_mesh_ready(mesh_ready_from("sec-1"));

            let affine_id = primary
                .cluster_state_for_test()
                .affine_id_for_hash(&import_hash)
                .expect("registered affine-id");

            let mut build_completed = false;
            let mut import_failed_on_sec0 = false;
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
                        // Fail the FIRST sec-0 import; complete every other import
                        // (e.g. the reroute onto sec-1, the fresh secondary).
                        if sec == "sec-0" && !import_failed_on_sec0 {
                            import_failed_on_sec0 = true;
                            primary
                                .handle_task_failed(task_failed(sec, *worker, h), &mut None)
                                .await;
                        } else {
                            primary
                                .handle_task_complete(task_complete(sec, *worker, h), &mut None)
                                .await;
                        }
                        settle_pump().await;
                    } else if *h == build_hash {
                        assert_eq!(
                            primary.cluster_state_for_test().affine_state(sec, affine_id),
                            SecondaryCell::Done,
                            "the build must only dispatch where the import is Done"
                        );
                        primary
                            .handle_task_complete(task_complete(sec, *worker, h), &mut None)
                            .await;
                        build_completed = true;
                        settle_pump().await;
                    }
                }
            }
            drain_commands(&mut primary).await;

            assert!(
                import_failed_on_sec0,
                "the import must have been attempted + failed on sec-0 (the setup)"
            );
            // The build was NOT prematurely failed — the fresh sec-1 (all-NotDone,
            // on the roster) kept the gate satisfiable, so the build re-routed and
            // ran there.
            assert!(
                build_completed,
                "the build must re-route to and run on the fresh secondary — a \
                 roster secondary with all-NotDone cells keeps the gate \
                 satisfiable, so the build must NOT be prematurely batch-failed"
            );
            assert!(
                matches!(
                    primary.cluster_state_for_test().task_state(&build_hash),
                    Some(crate::cluster_state::TaskState::Completed { .. })
                ),
                "the build's terminal must be a COMPLETION (no premature fail)"
            );
        })
        .await;
}

/// SCALE (the two burst flaws): an all-`Failed` affine gate with N dependents
/// FAR ABOVE the bounded self command channel capacity
/// (`COMMAND_CHANNEL_CAPACITY` = 256) must fast-fail EVERY dependent — NONE
/// lost — AND in exactly ONE broadcast.
///
/// Flaw 1 (overflow → lost → hang): enqueuing N `FailPermanent`s onto the
/// bounded channel would `Err(Full)` past 256 and DROP the overflow dependents,
/// each already taken out of its bucket ⇒ permanently lost ⇒ the run never
/// completes. The fix drives the batch DIRECTLY (no channel), so the test
/// asserts ALL N reach terminal and the command channel stays EMPTY.
///
/// Flaw 2 (N broadcasts → op-loop stall + mesh flood): failing per item would
/// ship N `ClusterMutation` broadcast frames. The fix accumulates all N
/// terminals into ONE broadcast, so the test asserts exactly ONE frame carried
/// the burst's `TaskFailed`s and it carried ALL N.
#[tokio::test(flavor = "current_thread")]
async fn affine_all_failed_batch_scales_past_command_channel_capacity() {
    use crate::primary::command_channel::COMMAND_CHANNEL_CAPACITY;

    /// Drain a secondary end ONCE, separating the two frame shapes the test
    /// cares about: the `(…, file_hash)` of every `TaskAssignment` (so imports
    /// can be failed) AND the per-frame `TaskFailed` count of every
    /// `ClusterMutation` broadcast (so the test can assert the burst arrived in a
    /// SINGLE frame). One drain so the two reads never race on the same receiver.
    #[allow(clippy::type_complexity)]
    fn drain_assignments_and_taskfailed_frames(
        rx: &mut tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
    ) -> (Vec<(String, String, u32, String)>, Vec<usize>) {
        let mut assigns = Vec::new();
        let mut frame_sizes = Vec::new();
        while let Ok(msg) = rx.try_recv() {
            match msg {
                DistributedMessage::TaskAssignment {
                    binary_info,
                    secondary_id,
                    worker_id,
                    file_hash,
                    ..
                } => assigns.push((binary_info.task_id, secondary_id, worker_id, file_hash)),
                DistributedMessage::ClusterMutation { mutations, .. } => {
                    let n = mutations
                        .iter()
                        .filter(|m| matches!(m, ClusterMutation::TaskFailed { .. }))
                        .count();
                    if n > 0 {
                        frame_sizes.push(n);
                    }
                }
                _ => {}
            }
        }
        (assigns, frame_sizes)
    }

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // N comfortably above the channel capacity so the pre-fix overflow
            // would drop ~(N - 256) dependents.
            let n_deps: usize = COMMAND_CHANNEL_CAPACITY * 3 + 17; // 785
            let import = affine_import("import");
            let import_hash = compute_task_hash(&import);
            let mut binaries = vec![import];
            let mut build_hashes = Vec::with_capacity(n_deps);
            for i in 0..n_deps {
                let b = work_dep(&format!("build_{i}"), "import");
                build_hashes.push(compute_task_hash(&b));
                binaries.push(b);
            }

            let (mut primary, mut ends, mut wm_rx, _mesh) =
                primary_two_secondaries_with(binaries);
            confirm_two(&mut primary).await;

            let affine_id = primary
                .cluster_state_for_test()
                .affine_id_for_hash(&import_hash)
                .expect("registered affine-id");

            // Dispatch placement + per-secondary import pops, then FAIL every
            // dispatched import until BOTH cells are Failed (never feeding a build
            // terminal). One drain per round separates the import assignments from
            // the TaskFailed broadcast frames the batch ships.
            let mut taskfailed_frame_sizes_seen: Vec<usize> = Vec::new();
            for _round in 0..8 {
                drain_rechecks(&mut primary, &mut wm_rx).await;
                let mut round_assignments: Vec<(String, String, u32, String)> = Vec::new();
                for (_id, rx, _tx) in ends.iter_mut() {
                    let (assigns, frames) = drain_assignments_and_taskfailed_frames(rx);
                    round_assignments.extend(assigns);
                    taskfailed_frame_sizes_seen.extend(frames);
                }
                for (_, sec, worker, h) in &round_assignments {
                    if *h == import_hash {
                        primary
                            .handle_task_failed(task_failed(sec, *worker, h), &mut None)
                            .await;
                        settle_pump().await;
                    } else {
                        assert!(
                            !build_hashes.contains(h),
                            "a build dispatched before the batch fast-fail; the \
                             burst must terminal-fail every dependent off the \
                             import failure"
                        );
                    }
                }
                let both_failed = ["sec-0", "sec-1"].iter().all(|sec| {
                    primary.cluster_state_for_test().affine_state(sec, affine_id)
                        == SecondaryCell::Failed
                });
                if both_failed {
                    break;
                }
            }

            for sec in ["sec-0", "sec-1"] {
                assert_eq!(
                    primary.cluster_state_for_test().affine_state(sec, affine_id),
                    SecondaryCell::Failed,
                    "the import cell must be Failed on {sec} (arming transition)"
                );
            }

            // Collect any final TaskFailed broadcast frames the last failure's
            // batch shipped.
            for (_id, rx, _tx) in ends.iter_mut() {
                let (_assigns, frames) = drain_assignments_and_taskfailed_frames(rx);
                taskfailed_frame_sizes_seen.extend(frames);
            }

            // FLAW 1 — NONE LOST: every one of the N dependents is terminal.
            let mut terminal = 0usize;
            for h in &build_hashes {
                if primary
                    .cluster_state_for_test()
                    .task_state(h)
                    .is_some_and(|s| s.is_terminal())
                {
                    terminal += 1;
                }
            }
            assert_eq!(
                terminal, n_deps,
                "ALL {n_deps} dependents (>> channel capacity \
                 {COMMAND_CHANNEL_CAPACITY}) must terminal-fail — none dropped by \
                 a bounded-channel overflow"
            );

            // FLAW 1 — the command channel was NEVER used for the burst (the
            // batch is driven directly): no FailPermanent queued.
            {
                let mut rx = primary.command_rx.take().expect("command_rx present");
                assert!(
                    rx.try_recv().is_err(),
                    "the burst must NOT enqueue any FailPermanent onto the bounded \
                     command channel — it is failed directly via the batch"
                );
                primary.command_rx = Some(rx);
            }

            // FLAW 2 — ONE broadcast: the burst's TaskFaileds were shipped to
            // each secondary in EXACTLY ONE ClusterMutation frame (not N frames).
            // Each of the 2 secondaries receives the single broadcast once, so we
            // expect every recorded frame to carry the whole burst and the
            // per-secondary frame count to be 1.
            assert!(
                !taskfailed_frame_sizes_seen.is_empty(),
                "the burst must have produced at least one TaskFailed broadcast"
            );
            // The single batch broadcast carries ALL N dependents' TaskFaileds in
            // one frame; replicated to 2 secondaries ⇒ at most 2 frames, each of
            // size N. (If the per-item path regressed, we'd see N*2 frames of
            // size 1.)
            assert!(
                taskfailed_frame_sizes_seen.len() <= ends.len(),
                "the burst must be ONE broadcast per secondary (≤ {} frames), got \
                 {} frames {:?} — a per-item regression would show ~{} frames",
                ends.len(),
                taskfailed_frame_sizes_seen.len(),
                taskfailed_frame_sizes_seen,
                n_deps * ends.len()
            );
            for sz in &taskfailed_frame_sizes_seen {
                assert_eq!(
                    *sz, n_deps,
                    "each broadcast frame must carry ALL {n_deps} TaskFaileds in \
                     one batch (got a frame of {sz}) — proof the broadcast is \
                     batched, not per-item"
                );
            }
        })
        .await;
}

/// IN-FLIGHT DEP IS UNMET (the multi-worker-same-node race): a WORK unit whose
/// affine deps are `[base, delta]` IN ORDER must treat an in-flight (`Queued`)
/// `base` as NOT MET — the readiness gate must classify it `InFlightHere` and
/// WAIT (withhold assignment), NEVER skip the unmet base to `StrandedHere`-
/// dispatch the later `NotDone` delta.
///
/// The bug: with ≥2 workers on the same secondary, worker A claims the shared
/// `base` import (its cell → `Queued`, in flight). Worker B then pops a
/// `build_variant` whose deps are `[base, delta]`. The old ORDER-BLIND gate
/// (`Failed` > `NotDone` > `Queued`) saw `base=Queued` + `delta=NotDone`, and
/// since `NotDone` outranked `Queued` it returned `StrandedHere` — ASSIGNING the
/// delta import to a worker BEFORE the base had landed on the node (the delta's
/// imported path is invalid until the base is present ⇒ "path … is not valid"
/// NonRecoverable). The list-order dependency-not-met gate classifies
/// `base=Queued` as `InFlightHere` (unmet ⇒ wait) instead, so the delta is never
/// considered — let alone assigned — until the base is `Done`.
///
/// Pins all the shapes the gate must produce, so a regression to the order-blind
/// skip is caught directly at the classification:
///   * `[base=Queued, delta=NotDone]` → `InFlightHere` (the BUG: was StrandedHere).
///   * `[base=NotDone, delta=NotDone]` → `StrandedHere` (first not-Done is NotDone).
///   * `[base=Done,    delta=NotDone]` → `StrandedHere` (advance past Done to delta).
///   * `[base=Done,    delta=Queued ]` → `InFlightHere` (delta now the unmet one).
///   * `[base=Done,    delta=Done   ]` → `Ready`.
///   * `[base=Failed,  delta=NotDone]` → terminal (Failed is order-independent):
///     Reroute to the still-satisfiable sibling secondary.
///   * `base Failed on EVERY secondary`  → `Unsatisfiable` (unchanged).
#[tokio::test(flavor = "current_thread")]
async fn affine_gate_inflight_dep_is_unmet_not_skipped_to_delta() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let base = affine_import("base");
            let delta = affine_import("delta");
            let base_hash = compute_task_hash(&base);
            let delta_hash = compute_task_hash(&delta);
            let build = work_two_deps("build", "base", "delta");

            let (mut primary, _ends, _wm_rx, _mesh) =
                primary_two_secondaries_with(vec![base, delta, build.clone()]);
            confirm_two(&mut primary).await;

            let base_aid = primary
                .cluster_state_for_test()
                .affine_id_for_hash(&base_hash)
                .expect("registered base affine-id");
            let delta_aid = primary
                .cluster_state_for_test()
                .affine_id_for_hash(&delta_hash)
                .expect("registered delta affine-id");

            // Helper: set a cell on a secondary directly. Cells are LWW by an
            // ever-increasing generation, so each write (incl. a NotDone reset via
            // SecondaryCellUnqueued) out-stamps the previous — the gate test can
            // walk a cell through any sequence of states. `gen` is a shared
            // monotone counter so every write wins over the prior cell value.
            let mut cell_gen: u64 = 1;
            let mut set = |primary: &mut TestPrimary,
                           sec: &str,
                           aid: crate::cluster_state::SecondaryCellId,
                           cell: SecondaryCell| {
                let g = cell_gen;
                cell_gen += 1;
                let mutation = match cell {
                    SecondaryCell::Queued => ClusterMutation::SecondaryCellQueued {
                        secondary: sec.into(),
                        cell_id: aid.0,
                        generation: g,
                    },
                    SecondaryCell::Done => ClusterMutation::SecondaryCellFinished {
                        secondary: sec.into(),
                        cell_id: aid.0,
                        generation: g,
                    },
                    SecondaryCell::Failed => ClusterMutation::SecondaryCellFailed {
                        secondary: sec.into(),
                        cell_id: aid.0,
                        generation: g,
                    },
                    SecondaryCell::NotDone => ClusterMutation::SecondaryCellUnqueued {
                        secondary: sec.into(),
                        cell_id: aid.0,
                        generation: g,
                    },
                };
                primary.cluster_state_mut_for_test().apply(mutation);
            };
            let label =
                |primary: &TestPrimary| primary.affine_gate_label_for_test("sec-0", &build);

            // THE BUG SHAPE: base in flight (Queued), delta NotDone. Must WAIT on
            // the unmet in-flight base — NOT skip to (and assign) the delta.
            set(&mut primary, "sec-0", base_aid, SecondaryCell::Queued);
            assert_eq!(
                label(&primary),
                "InFlightHere",
                "an in-flight (Queued) base is UNMET — the gate must WAIT (withhold \
                 assignment), never skip it to StrandedHere-dispatch the later \
                 NotDone delta (the multi-worker-same-node 'path is not valid' race)"
            );

            // base NotDone, delta NotDone → first not-Done is the base (NotDone),
            // so StrandedHere dispatches the base import (correct: base first).
            set(&mut primary, "sec-0", base_aid, SecondaryCell::NotDone);
            assert_eq!(
                label(&primary),
                "StrandedHere",
                "both NotDone → stranded on the FIRST (base) import"
            );

            // base Done, delta NotDone → advance past the met base to the delta
            // (NotDone) → StrandedHere on the delta (the original single-worker
            // order: base lands, THEN the delta dispatches).
            set(&mut primary, "sec-0", base_aid, SecondaryCell::Done);
            assert_eq!(
                label(&primary),
                "StrandedHere",
                "base Done (met) → advance to the delta (NotDone) and dispatch it"
            );

            // base Done, delta Queued (in flight) → the delta is now the unmet one.
            set(&mut primary, "sec-0", delta_aid, SecondaryCell::Queued);
            assert_eq!(
                label(&primary),
                "InFlightHere",
                "base met + delta in flight → wait on the unmet delta"
            );

            // base Done, delta Done → Ready.
            set(&mut primary, "sec-0", delta_aid, SecondaryCell::Done);
            assert_eq!(label(&primary), "Ready", "all deps Done → Ready");

            // FAILED IS ORDER-INDEPENDENT (unchanged): base Failed on sec-0 but
            // sec-1 still satisfiable → Reroute(sec-1). Reset sec-0's delta cell so
            // only the (order-independent) Failed base drives the decision.
            set(&mut primary, "sec-0", delta_aid, SecondaryCell::NotDone);
            set(&mut primary, "sec-0", base_aid, SecondaryCell::Failed);
            assert_eq!(
                label(&primary),
                "Reroute(sec-1)",
                "a Failed base is order-independent terminal → reroute to the \
                 still-satisfiable sibling secondary"
            );

            // base Failed on EVERY secondary → Unsatisfiable (unchanged).
            set(&mut primary, "sec-1", base_aid, SecondaryCell::Failed);
            assert_eq!(
                primary.affine_gate_label_for_test("sec-0", &build),
                "Unsatisfiable",
                "a base Failed on EVERY eligible secondary is Unsatisfiable \
                 (the all-Failed terminal — unchanged)"
            );
        })
        .await;
}

/// END-TO-END under ≥2 workers on the SAME secondary (the live race): two
/// `build_variant`s share a base import and each layer a distinct delta on top
/// (`build_a` deps `[base, delta_a]`, `build_b` deps `[base, delta_b]`). With 2
/// idle workers on one secondary, the first build to commit claims the base
/// import (cell → Queued, in flight); the SECOND build, popped for the sibling
/// worker while the base is still `Queued` (UNMET), must NOT be assigned its
/// delta import ahead of the base — it `InFlightHere`-waits at the queue front.
/// Only after the base reaches `Done` (met) does each build's delta dispatch.
/// The invariant re-checked every round: NO delta import is ever ASSIGNED on a
/// secondary whose base cell is not yet `Done`.
#[tokio::test(flavor = "current_thread")]
async fn affine_multiworker_same_node_delta_never_assigned_before_base_done() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let base = affine_import("base");
            let delta_a = affine_import("delta_a");
            let delta_b = affine_import("delta_b");
            let base_hash = compute_task_hash(&base);
            let delta_a_hash = compute_task_hash(&delta_a);
            let delta_b_hash = compute_task_hash(&delta_b);
            let build_a = work_two_deps("build_a", "base", "delta_a");
            let build_b = work_two_deps("build_b", "base", "delta_b");

            // 2 workers on sec-0 (forces the same-node concurrent pop); sec-1 has 1.
            let (mut primary, mut ends, mut wm_rx, _mesh) =
                primary_two_secondaries_with(vec![base, delta_a, delta_b, build_a, build_b]);
            primary
                .handle_cluster_mutation(capacity_batch("sec-0", 2), &mut None)
                .await;
            primary.handle_mesh_ready(mesh_ready_from("sec-0"));
            primary
                .handle_cluster_mutation(capacity_batch("sec-1", 1), &mut None)
                .await;
            primary.handle_mesh_ready(mesh_ready_from("sec-1"));

            let base_aid = primary
                .cluster_state_for_test()
                .affine_id_for_hash(&base_hash)
                .expect("registered base affine-id");

            // INVARIANT: a DELTA import is never ASSIGNED to a secondary whose BASE
            // cell is not yet `Done`. A delta assigned before the base is the exact
            // "path is not valid" race the dependency-not-met gate closes.
            let assert_delta_only_after_base_done =
                |primary: &TestPrimary, round: &[(String, String, u32, String)]| {
                    for (_, sec, _, h) in round {
                        if *h == delta_a_hash || *h == delta_b_hash {
                            assert_eq!(
                                primary.cluster_state_for_test().affine_state(sec, base_aid),
                                SecondaryCell::Done,
                                "a DELTA import was assigned on {sec} whose BASE cell \
                                 is NOT Done — an unmet in-flight base was skipped \
                                 (the multi-worker race); got {round:?}"
                            );
                        }
                    }
                };

            // Drive to quiescence, completing every dispatched import on its
            // secondary. The base must dispatch (and complete) before any delta on
            // that node; the invariant catches any premature delta assignment.
            drain_rechecks(&mut primary, &mut wm_rx).await;
            let mut base_dispatches_by_sec: std::collections::HashMap<String, u32> =
                std::collections::HashMap::new();
            loop {
                let mut round: Vec<(String, String, u32, String)> = Vec::new();
                for (_id, rx, _tx) in ends.iter_mut() {
                    round.extend(assignments(rx));
                }
                if round.is_empty() {
                    break;
                }
                assert_delta_only_after_base_done(&primary, &round);
                for (_, sec, _, h) in &round {
                    if *h == base_hash {
                        *base_dispatches_by_sec.entry(sec.clone()).or_insert(0) += 1;
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

            // The base dispatched EXACTLY ONCE on each secondary that ran it (the
            // dependency-not-met wait serializes it, the run-once guard dedups it
            // — not once-per-build). At least one node ran it.
            assert!(
                !base_dispatches_by_sec.is_empty(),
                "the shared base import must have dispatched on at least one secondary"
            );
            for (sec, count) in &base_dispatches_by_sec {
                assert_eq!(
                    *count, 1,
                    "the shared base import must dispatch EXACTLY ONCE on {sec}; got {count}"
                );
            }

            // Both builds completed (each on a node whose base + its own delta
            // reached Done — enforced by the per-round invariant), and the run
            // drains cleanly with every slot freed.
            assert_eq!(
                primary.active_workers_for_test(),
                0,
                "no worker slot may stay Assigned after the run drains"
            );
            assert!(
                primary.run_complete_check(),
                "the run must complete once the base + both deltas + both builds drain"
            );
        })
        .await;
}

/// A `Work` task depending on a NON-affine gate `gate` AND an affine import
/// `import` — the #650 shape: the gate holds the work BLOCKED (not a dispatchable
/// bucket item) so the #648 fast-fail bridge MISSES it when the import is
/// globally-failed; once the gate completes the work becomes ready-in-bucket and
/// the placement source must terminalize it (instead of re-admitting it).
fn work_gated_on_affine(name: &str, gate: &str, import: &str) -> TaskInfo<TestId> {
    let mut t = make_binary(name, 20);
    t.phase_id = PhaseId::from("work");
    t.type_id = TypeId::from("default");
    t.task_depends_on = vec![
        TaskDep {
            task_id: gate.into(),
            phase_id: PhaseId::from("work"),
            inherit_outputs: false,
            def_id: None,
        },
        TaskDep {
            task_id: import.into(),
            phase_id: PhaseId::from("work"),
            inherit_outputs: false,
            def_id: None,
        },
    ];
    t
}

/// An ordinary `Work` upstream (no deps), phase "work" — the non-affine prereq
/// an affine import is built ON TOP of (the consumer's `build_common_dep`).
fn work_upstream(name: &str) -> TaskInfo<TestId> {
    let mut t = make_binary(name, 10);
    t.phase_id = PhaseId::from("work");
    t.type_id = TypeId::from("default");
    t
}

/// A `SecondaryAffine` import depending on the ordinary `Work` `upstream` via a
/// NON-affine edge (the consumer's `import_common_dep`: an affine import whose
/// own prereq is an ordinary build). The edge is non-affine because `upstream`
/// is an ordinary work def — so the import is BLOCKED in the global pool on
/// `upstream` (and is in `dependents_of[upstream]`), unlike an affine prereq
/// (which is excluded from a dependent's blocking set).
fn affine_import_dep(name: &str, upstream: &str) -> TaskInfo<TestId> {
    let mut t = make_binary(name, 10);
    t.phase_id = PhaseId::from("work");
    t.type_id = TypeId::from("default");
    t.kind = TaskKind::SecondaryAffine;
    t.task_depends_on = vec![TaskDep {
        task_id: upstream.into(),
        phase_id: PhaseId::from("work"),
        inherit_outputs: false,
        def_id: None,
    }];
    t
}

/// DEAD-UPSTREAM AFFINE-DEP TERMINALIZATION (#648): an affine import's OWN
/// non-affine upstream fails non-recoverably → the permanent-fail cascade
/// pool-fails the import (it has a non-affine edge on the upstream, so it is in
/// `dependents_of[upstream]`) — BUT the import flips NO bitvector cell (it never
/// ran anywhere; its cells stay `NotDone`). The affine-DEP work tasks downstream
/// of the import ESCAPE the cascade via two stacked gaps: (1) the Model-B edge
/// filter keeps them OUT of `dependents_of[import]` so the cascade BFS dead-ends
/// at the import, and (2) the per-secondary fast-fail
/// (`fast_fail_affine_dependents_if_unsatisfiable`) only fires from a per-secondary
/// worker terminal that flips a cell `→ Failed`, which a pool-cascade import
/// never does. Pre-fix the dependent sits placed-but-never-terminal forever and
/// the build phase never drains.
///
/// RED at fc1b0ee9: the dependent W is placed (the global view never grabs it,
/// `has_affine_dep`), absent from every affine queue, and NON-terminal after the
/// upstream fails — and `affine_unit_satisfiable_secondaries` reads the
/// all-`NotDone` import as STILL satisfiable, so no fast-fail fires.
///
/// GREEN: the generalized satisfiability predicate reads the import (now in
/// `failed_tasks`) as globally-UNsatisfiable, and the pool-terminal → affine
/// fast-fail bridge (`bridge_affine_imports_to_fast_fail`, run from
/// `apply_fail_permanent`) terminalizes W in one batch — W is terminal, out of
/// its bucket, and no longer placed-but-unqueued, so the phase can drain.
#[tokio::test(flavor = "current_thread")]
async fn affine_dep_work_terminalized_when_import_pool_cascade_failed() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Topology: ordinary Work upstream U → affine import I (non-affine
            // edge on U) → affine-dep Work W (affine edge on I).
            let upstream = work_upstream("build_common_dep");
            let upstream_hash = compute_task_hash(&upstream);
            let import = affine_import_dep("import_common_dep", "build_common_dep");
            let import_hash = compute_task_hash(&import);
            let build = work_dep("cross_arch_build", "import_common_dep");
            let build_hash = compute_task_hash(&build);

            let (mut primary, _ends, _wm_rx, _mesh) =
                primary_two_secondaries_with(vec![upstream, import, build]);
            confirm_two(&mut primary).await;

            let affine_id = primary
                .cluster_state_for_test()
                .affine_id_for_hash(&import_hash)
                .expect("registered affine-id for the import");

            // The import is BLOCKED on its non-affine upstream (it is NOT a ready
            // bucket item) — so it is in `dependents_of[upstream]` and the
            // cascade will reach it. The affine-dep work W, by contrast, IS a
            // ready bucket item (its only dep is the affine import, excluded from
            // its global blocking set by `has_affine_dep`).
            let import_ready = primary
                .pool()
                .iter()
                .any(|t| compute_task_hash(t) == import_hash);
            assert!(
                !import_ready,
                "the affine import must be BLOCKED on its non-affine upstream \
                 (in dependents_of[upstream], not a ready bucket item)"
            );
            let build_ready = primary
                .pool()
                .iter()
                .any(|t| compute_task_hash(t) == build_hash);
            assert!(
                build_ready,
                "the affine-dep work must be a READY bucket item (its affine \
                 import dep is excluded from its global blocking set)"
            );

            // STAGE the strand: the placement trigger recorded W placed (the
            // race-window state) — W dispatches ONLY through the per-secondary
            // affine queue (withheld from the global view by has_affine_dep). It
            // is NOT in any affine queue (the import never became ready to drag
            // it in), so it is exactly placed-but-unqueued.
            assert!(
                primary.affine_record_placed_work_for_test(&build_hash),
                "staging records the work placed for the first time"
            );
            assert_eq!(
                primary.affine_scheduler_placed_but_unqueued_for_test(),
                1,
                "W must be placed-but-unqueued (the strand signature) before the \
                 upstream fails"
            );

            // PRE-FIX (satisfiability): with all cells NotDone, the import looks
            // SATISFIABLE on the roster — this is the gap the predicate
            // generalization closes once the import is in `failed_tasks`.
            for sec in ["sec-0", "sec-1"] {
                assert_eq!(
                    primary.cluster_state_for_test().affine_state(sec, affine_id),
                    SecondaryCell::NotDone,
                    "the pool-cascade import flips NO bitvector cell on {sec}"
                );
            }

            // ── THE TRIGGER: the non-affine upstream fails non-recoverably. ──
            // Drive it through the real permanent-fail seam (the same path a
            // worker `TaskFailed` / setup-failure routes through), holding the
            // command_rx so the bridge's fast-fail runs DIRECTLY.
            let mut command_rx = primary.command_rx.take();
            primary
                .apply_fail_permanent(
                    upstream_hash.clone(),
                    dynrunner_core::ErrorType::NonRecoverable,
                    "build_common_dep failed non-recoverably".into(),
                    &mut command_rx,
                )
                .await
                .expect("the upstream hash is known");
            primary.command_rx = command_rx;
            settle_pump().await;

            // The cascade reached the import: it is recorded in the pool's
            // `failed_tasks` terminal ledger (the global-failure signal the
            // generalized satisfiability predicate reads), WITHOUT flipping any
            // bitvector cell. (The cascaded import gets the local `failed_tasks`
            // terminal, not its own CRDT `TaskFailed` — the existing #358
            // cascade shape; the predicate keys on `failed_tasks`, so this is the
            // exact edge the bridge needs.)
            assert!(
                primary.failed_tasks.contains_key(&import_hash),
                "the cascade must pool-fail the affine import into failed_tasks \
                 (non-affine edge on the dead upstream)"
            );
            for sec in ["sec-0", "sec-1"] {
                assert_eq!(
                    primary.cluster_state_for_test().affine_state(sec, affine_id),
                    SecondaryCell::NotDone,
                    "the pool-cascade import must NOT flip a bitvector cell on \
                     {sec} (it never ran anywhere)"
                );
            }

            // GENERALIZED PREDICATE: the gate now reads W as `Unsatisfiable`
            // even though every cell is `NotDone` — purely because its import is
            // in `failed_tasks`. This is the missing edge: pre-fix the all-
            // `NotDone` import read as satisfiable and the gate never reached
            // `Unsatisfiable`. (W's def is still in the CRDT, so the gate's pure
            // placement read is valid post-terminalization.)
            if let Some(build_state) = primary.cluster_state_for_test().task_state(&build_hash) {
                let build_info = primary.cluster_state_for_test().task_to_info(build_state);
                assert_eq!(
                    primary.affine_gate_label_for_test("sec-0", &build_info),
                    "Unsatisfiable",
                    "the generalized satisfiability predicate must read the \
                     globally-failed import (in failed_tasks, cells all NotDone) \
                     as Unsatisfiable"
                );
            }

            // GREEN (the bridge): the affine-dep work W is terminalized — it is
            // terminal, removed from its bucket, and no longer placed-but-unqueued
            // (claimed out of the bucket by the fast-fail). RED at fc1b0ee9: W was
            // non-terminal, still placed-but-unqueued (count 1), and the phase
            // could not drain.
            assert!(
                primary
                    .cluster_state_for_test()
                    .task_state(&build_hash)
                    .is_some_and(|s| s.is_terminal()),
                "the affine-dep work must be TERMINALIZED once its import is \
                 pool-cascade-failed (RED at fc1b0ee9: stranded non-terminal \
                 forever)"
            );
            assert_eq!(
                primary
                    .pool()
                    .iter()
                    .filter(|t| compute_task_hash(t) == build_hash)
                    .count(),
                0,
                "the terminalized work must be removed from its bucket"
            );

            // The phase can now drain past the affine-dep work: the placed
            // affine-dep work W (the #642 drain-blocker — a live non-terminal
            // task hidden from the global view) is terminal and out of every
            // bucket, so no affine-dep work remains to hold the drain open. (Any
            // other residual queued item, e.g. the upstream U which this test
            // fails while still queued rather than dispatching it in-flight
            // first, is orthogonal to the affine-dep strand the bridge fixes.)
            assert_eq!(
                primary
                    .pool()
                    .iter()
                    .filter(|t| primary.pool().has_affine_dep(t))
                    .count(),
                0,
                "no affine-dep work remains queued — the #642 drain-blocker is \
                 cleared (RED at fc1b0ee9: W stayed queued+non-terminal forever)"
            );
        })
        .await;
}

/// Run one placement wave through the REAL recheck seam
/// ([`PrimaryCoordinator::react_to_worker_signal_batch`]) with the live
/// `command_rx`, so the dead-upstream-aware placement path (#650) — placement
/// returns its doomed set, the recheck caller terminalizes it — runs end-to-end
/// exactly as the operational loop drives it. A bare `TasksAdded` batch is the
/// placement trigger.
async fn run_placement_wave(primary: &mut TestPrimary) {
    let mut command_rx = primary.command_rx.take();
    primary
        .react_to_worker_signal_batch(
            crate::worker_signal::WorkerSignalBatch {
                signals: vec![WorkerMgmtSignal::TasksAdded],
            },
            &mut command_rx,
        )
        .await;
    primary.command_rx = command_rx;
    settle_pump().await;
}

/// DEAD-UPSTREAM-AWARE PLACEMENT (#650) — the REVERSE-ORDER complement to
/// [`affine_dep_work_terminalized_when_import_pool_cascade_failed`] (#648). The
/// #648 bridge fires ONCE at the import's terminal seam; a dependent that is not
/// yet ready-in-bucket at that instant is MISSED by the bridge (its
/// `claim_affine_work_for_fail` → `take_first_match` returns `None` for a blocked
/// item — "the 28 residue"). When the dependent LATER becomes ready, the
/// placement source ran on EVERY `TasksAdded` and — pre-fix — re-ADMITTED it as
/// if the import were live (it never consulted `failed_tasks`), stranding a
/// non-terminal affine-dep work that holds the phase open forever.
///
/// Topology: ordinary Work upstream U → affine import I (non-affine edge on U) →
/// affine-dep Work W (affine edge on I) PLUS a non-affine gate G that holds W
/// BLOCKED. ORDER (vs the #648 test): FIRST fail U (the cascade pool-fails I into
/// `failed_tasks`, cells all NotDone; the bridge runs but W is blocked-on-G so it
/// is MISSED). THEN complete G so W becomes ready-in-bucket and trigger a
/// placement wave.
///
/// RED at 37c19235 (W re-admitted, stranded non-terminal + placed-but-unqueued);
/// GREEN after the placement source partitions the doomed candidate out via the
/// EXACT #648 predicate and terminalizes it through the shared claim+batch path.
#[tokio::test(flavor = "current_thread")]
async fn affine_dep_work_placed_after_import_globally_failed_is_terminalized_not_stranded() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let upstream = work_upstream("build_common_dep");
            let upstream_hash = compute_task_hash(&upstream);
            let import = affine_import_dep("import_common_dep", "build_common_dep");
            let import_hash = compute_task_hash(&import);
            // G: a plain Work task (no deps) that gates W until it completes.
            let gate = work_upstream("gate_dep");
            let gate_hash = compute_task_hash(&gate);
            // W: affine-dep work, BLOCKED on the non-affine gate G until G is Done.
            let build = work_gated_on_affine("cross_arch_build", "gate_dep", "import_common_dep");
            let build_hash = compute_task_hash(&build);

            let (mut primary, mut ends, mut wm_rx, _mesh) = primary_two_secondaries_with(vec![
                upstream.clone(),
                import,
                gate.clone(),
                build,
            ]);
            confirm_two(&mut primary).await;

            let affine_id = primary
                .cluster_state_for_test()
                .affine_id_for_hash(&import_hash)
                .expect("registered affine-id for the import");

            // W is BLOCKED on the non-affine gate G → NOT a ready bucket item yet
            // (so the bridge will miss it). G IS ready.
            assert!(
                !primary
                    .pool()
                    .iter()
                    .any(|t| compute_task_hash(t) == build_hash),
                "W must be BLOCKED on its non-affine gate (not a ready bucket item) \
                 at the time the import is globally-failed — so the #648 bridge \
                 misses it"
            );

            // ── FAIL the upstream FIRST: the cascade pool-fails the import into
            // `failed_tasks` (cells all NotDone); the #648 bridge runs but W is
            // blocked-on-G, so its claim returns None and W is MISSED. ──
            let mut command_rx = primary.command_rx.take();
            primary
                .apply_fail_permanent(
                    upstream_hash.clone(),
                    dynrunner_core::ErrorType::NonRecoverable,
                    "build_common_dep failed non-recoverably".into(),
                    &mut command_rx,
                )
                .await
                .expect("the upstream hash is known");
            primary.command_rx = command_rx;
            settle_pump().await;

            assert!(
                primary.failed_tasks.contains_key(&import_hash),
                "the cascade must pool-fail the affine import into failed_tasks"
            );
            for sec in ["sec-0", "sec-1"] {
                assert_eq!(
                    primary.cluster_state_for_test().affine_state(sec, affine_id),
                    SecondaryCell::NotDone,
                    "the pool-cascade import flips NO bitvector cell on {sec}"
                );
            }
            // The bridge MISSED W (it was blocked): W is non-terminal and was
            // never placed (the strand the placement source must now catch).
            assert!(
                primary
                    .cluster_state_for_test()
                    .task_state(&build_hash)
                    .is_some_and(|s| !s.is_terminal()),
                "W must still be NON-terminal after the upstream fails (the bridge \
                 missed it — W was blocked on its gate)"
            );
            assert!(
                !primary.affine_work_is_placed_for_test(&build_hash),
                "W was never placed yet (still blocked) — the bridge could not \
                 claim it"
            );

            // ── Now complete the gate G so W becomes ready-in-bucket, then run a
            // placement wave (the real recheck seam, with the live command_rx). ──
            drain_rechecks(&mut primary, &mut wm_rx).await;
            // Find + complete G on whichever secondary it dispatched to.
            let mut gate_slot: Option<(String, u32)> = None;
            for (_id, rx, _tx) in ends.iter_mut() {
                for (_task_id, sec, worker, h) in assignments(rx) {
                    if h == gate_hash {
                        gate_slot = Some((sec, worker));
                    }
                }
            }
            let (gate_sec, gate_worker) =
                gate_slot.expect("the non-affine gate G dispatched to a worker");
            primary
                .handle_task_complete(
                    task_complete(&gate_sec, gate_worker, &gate_hash),
                    &mut None,
                )
                .await;
            settle_pump().await;

            // W is now ready-in-bucket (its gate is Done; its affine import is
            // excluded from its global blocking set).
            assert!(
                primary
                    .pool()
                    .iter()
                    .any(|t| compute_task_hash(t) == build_hash),
                "W must be a READY bucket item once its non-affine gate completes"
            );

            // ── THE PLACEMENT WAVE: pre-fix this re-admits W blindly. ──
            run_placement_wave(&mut primary).await;

            // GREEN: W is TERMINALIZED by the placement source (it partitioned W
            // as doomed — its import is in `failed_tasks` — and routed it to the
            // shared batch terminalization instead of placing it). It is terminal,
            // out of its bucket, and NEVER recorded-or-left placed-but-unqueued.
            // RED at 37c19235: W was re-admitted (placed), non-terminal, and
            // placed-but-unqueued == 1.
            assert!(
                primary
                    .cluster_state_for_test()
                    .task_state(&build_hash)
                    .is_some_and(|s| s.is_terminal()),
                "W must be TERMINALIZED by the dead-upstream-aware placement \
                 source (RED at 37c19235: re-admitted + stranded non-terminal)"
            );
            assert_eq!(
                primary
                    .pool()
                    .iter()
                    .filter(|t| compute_task_hash(t) == build_hash)
                    .count(),
                0,
                "the terminalized work must be removed from its bucket"
            );
            assert!(
                !primary.affine_work_is_placed_for_test(&build_hash),
                "a doomed work must NEVER be recorded placed (RED at 37c19235: \
                 placement re-admitted it)"
            );
            assert_eq!(
                primary.affine_scheduler_placed_but_unqueued_for_test(),
                0,
                "no placed-but-unqueued strand may remain (RED at 37c19235: \
                 count == 1, the residue that holds the phase open)"
            );
            // No affine-dep work remains queued — the #642 drain-blocker is clear.
            assert_eq!(
                primary
                    .pool()
                    .iter()
                    .filter(|t| primary.pool().has_affine_dep(t))
                    .count(),
                0,
                "no affine-dep work remains queued — the phase can drain"
            );
        })
        .await;
}

/// CHURN-BOUND regression (#650 — the 3570-churn guard): with an affine import
/// globally-failed AND a dependent the #648 bridge MISSED (blocked-on-its-gate at
/// fail-time, then made ready), running MANY placement waves must NOT grow the
/// placed-but-unqueued count. Pre-fix each wave re-admitted the doomed work
/// (re-recording it placed — and, with no per-secondary pop, it sat in NO queue,
/// so the strand count grew once per wave: 2 → 3570). Post-fix the placement
/// source partitions the doomed work out + terminalizes it ONCE, so the count
/// stays pinned at 0 across every wave (the first wave terminalizes it; the rest
/// are no-ops — W is terminal, never re-derived).
///
/// Topology mirrors [`affine_dep_work_placed_after_import_globally_failed_is_terminalized_not_stranded`]
/// (a gated W the bridge misses), then drives the wave loop instead of asserting
/// a single wave — so it directly bounds the re-admit-per-wave growth.
#[tokio::test(flavor = "current_thread")]
async fn placement_waves_with_dead_import_do_not_grow_placed_but_unqueued() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let upstream = work_upstream("build_common_dep");
            let upstream_hash = compute_task_hash(&upstream);
            let import = affine_import_dep("import_common_dep", "build_common_dep");
            let import_hash = compute_task_hash(&import);
            let gate = work_upstream("gate_dep");
            let gate_hash = compute_task_hash(&gate);
            // W: affine-dep work BLOCKED on the non-affine gate G — so the #648
            // bridge MISSES it when the import is globally-failed.
            let build = work_gated_on_affine("cross_arch_build", "gate_dep", "import_common_dep");
            let build_hash = compute_task_hash(&build);

            let (mut primary, mut ends, mut wm_rx, _mesh) = primary_two_secondaries_with(vec![
                upstream.clone(),
                import,
                gate.clone(),
                build,
            ]);
            confirm_two(&mut primary).await;

            // Fail the upstream → the import lands in `failed_tasks`; the bridge
            // runs but W is blocked-on-G, so W is MISSED (not terminalized).
            let mut command_rx = primary.command_rx.take();
            primary
                .apply_fail_permanent(
                    upstream_hash.clone(),
                    dynrunner_core::ErrorType::NonRecoverable,
                    "build_common_dep failed non-recoverably".into(),
                    &mut command_rx,
                )
                .await
                .expect("the upstream hash is known");
            primary.command_rx = command_rx;
            settle_pump().await;
            assert!(
                primary.failed_tasks.contains_key(&import_hash),
                "the cascade must pool-fail the affine import into failed_tasks"
            );
            assert!(
                primary
                    .cluster_state_for_test()
                    .task_state(&build_hash)
                    .is_some_and(|s| !s.is_terminal()),
                "the bridge must MISS the blocked W (it is still non-terminal)"
            );

            // Complete the gate G so W becomes a ready-in-bucket WORK task — now a
            // doomed candidate the placement source must NOT re-admit.
            drain_rechecks(&mut primary, &mut wm_rx).await;
            let mut gate_slot: Option<(String, u32)> = None;
            for (_id, rx, _tx) in ends.iter_mut() {
                for (_task_id, sec, worker, h) in assignments(rx) {
                    if h == gate_hash {
                        gate_slot = Some((sec, worker));
                    }
                }
            }
            let (gate_sec, gate_worker) =
                gate_slot.expect("the non-affine gate G dispatched to a worker");
            primary
                .handle_task_complete(
                    task_complete(&gate_sec, gate_worker, &gate_hash),
                    &mut None,
                )
                .await;
            settle_pump().await;
            assert!(
                primary
                    .pool()
                    .iter()
                    .any(|t| compute_task_hash(t) == build_hash),
                "W must be a READY bucket item (doomed candidate) before the waves"
            );

            // Drive MANY placement waves. The FIRST partitions W out + terminalizes
            // it; EVERY wave must keep the strand count at 0. Pre-fix each wave
            // re-admitted W (placed_work grows; no pop ⇒ placed-but-unqueued grows
            // monotonically — the 3570-churn).
            for wave in 0..16 {
                run_placement_wave(&mut primary).await;
                assert_eq!(
                    primary.affine_scheduler_placed_but_unqueued_for_test(),
                    0,
                    "placed-but-unqueued must stay 0 on wave {wave} (RED at \
                     37c19235: grows monotonically — the 3570-churn)"
                );
                assert!(
                    !primary.affine_work_is_placed_for_test(&build_hash),
                    "the doomed work must never be (re-)recorded placed on wave \
                     {wave}"
                );
            }

            // W ends terminal and gone from every bucket.
            assert!(
                primary
                    .cluster_state_for_test()
                    .task_state(&build_hash)
                    .is_some_and(|s| s.is_terminal()),
                "W must be terminal after the placement-source terminalization"
            );
        })
        .await;
}

// ── #652 concern B: affine-dep-as-blocked (per-secondary) ──

/// #652 B core: an affine-dep WORK task whose per-secondary import is NOT yet
/// `Done` WAITS in the per-secondary blocked map (it is NOT enqueued + spun on
/// the per-secondary queue, the old `InFlightHere => requeue_front` churn). It is
/// enqueued + dispatched ONLY once its import cell flips `Done`. While it waits,
/// its phase stays open (the blocked work is still its pool phase-drain token —
/// owner requirement #3: a blocked-in-affine-map work must NOT let the phase
/// drain prematurely).
#[tokio::test(flavor = "current_thread")]
async fn affine_dep_work_waits_blocked_until_import_finished() {
    use dynrunner_scheduler_api::pending_pool::PhaseState;
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

            // First recheck wave: placement routes the build onto a secondary,
            // its pop gates `StrandedHere` (import NotDone there), dispatches the
            // IMPORT on-demand, and BLOCKS the build (does NOT enqueue/spin it).
            drain_rechecks(&mut primary, &mut wm_rx).await;

            // Exactly the import dispatched in this wave — NOT the build (it is
            // blocked, waiting on its per-secondary import cell).
            let mut wave: Vec<(String, String, u32, String)> = Vec::new();
            for (_id, rx, _tx) in ends.iter_mut() {
                wave.extend(assignments(rx));
            }
            assert!(
                wave.iter().all(|(_, _, _, h)| *h != build_hash),
                "the build must NOT dispatch before its import is Done; got {wave:?}"
            );
            let (import_sec, import_worker) = wave
                .iter()
                .find(|(_, _, _, h)| *h == import_hash)
                .map(|(_, sec, w, _)| (sec.clone(), *w))
                .expect("the import dispatched on-demand for the blocked build");

            // The build is BLOCKED on its import on that secondary (not queued).
            assert!(
                primary.affine_is_blocked_on_import_for_test(&import_sec, &build_hash),
                "the build must WAIT in the per-secondary blocked map on {import_sec}"
            );
            // Owner requirement #3: phase "work" must NOT drain while the build
            // waits blocked — it is still a live pool item holding the phase open.
            assert_ne!(
                primary.pool().phase_state(&PhaseId::from("work")),
                Some(PhaseState::Done),
                "phase 'work' must stay open while a build is blocked-on-import"
            );

            // Complete the import on its secondary → cell `Done` → on_cell_finished
            // unblocks + re-enqueues the build → it dispatches there.
            primary
                .handle_task_complete(
                    task_complete(&import_sec, import_worker, &import_hash),
                    &mut None,
                )
                .await;
            settle_pump().await;
            drain_rechecks(&mut primary, &mut wm_rx).await;

            // The build is no longer blocked (it was unblocked on cell-Finished).
            assert!(
                !primary.affine_is_blocked_on_import_for_test(&import_sec, &build_hash),
                "the build must be unblocked once its import is Done"
            );
            // And it dispatched (on the secondary whose import is now Done).
            let mut after: Vec<(String, String, u32, String)> = Vec::new();
            for (_id, rx, _tx) in ends.iter_mut() {
                after.extend(assignments(rx));
            }
            let build_dispatch = after.iter().find(|(_, _, _, h)| *h == build_hash);
            assert!(
                build_dispatch.is_some(),
                "the build must dispatch once its import is Done; got {after:?}"
            );
            let affine_id = primary
                .cluster_state_for_test()
                .affine_id_for_hash(&import_hash)
                .unwrap();
            let (_, build_sec, _, _) = build_dispatch.unwrap();
            assert_eq!(
                primary.cluster_state_for_test().affine_state(build_sec, affine_id),
                SecondaryCell::Done,
                "the build dispatched only on a secondary whose import cell is Done"
            );
        })
        .await;
}

/// #652 B acceptance signature: an IDLE secondary actually attempts affine work
/// and triggers its OWN on-demand import — it does not sit idle while all builds
/// concentrate on one secondary (the asm-dataset-nix symptom). With one build
/// per secondary, BOTH secondaries' imports run (each node imports locally),
/// proving the idle secondary received a share, popped it, and kicked its own
/// import via the per-secondary blocked → on-demand path.
#[tokio::test(flavor = "current_thread")]
async fn affine_idle_secondary_triggers_own_import() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let import = affine_import("import");
            let import_hash = compute_task_hash(&import);
            let build_0 = work_dep("build_0", "import");
            let build_1 = work_dep("build_1", "import");

            let (mut primary, mut ends, mut wm_rx, _mesh) =
                primary_two_secondaries_with(vec![import, build_0, build_1]);
            confirm_two(&mut primary).await;
            let affine_id = primary
                .cluster_state_for_test()
                .affine_id_for_hash(&import_hash)
                .unwrap();

            // Drive to quiescence, completing every dispatched IMPORT (so each
            // secondary's cell can reach Done and its build can run).
            drain_rechecks(&mut primary, &mut wm_rx).await;
            let mut import_secs: std::collections::HashSet<String> = Default::default();
            loop {
                let mut round: Vec<(String, String, u32, String)> = Vec::new();
                for (_id, rx, _tx) in ends.iter_mut() {
                    round.extend(assignments(rx));
                }
                if round.is_empty() {
                    break;
                }
                for (_, sec, worker, h) in &round {
                    if *h == import_hash {
                        import_secs.insert(sec.clone());
                    }
                    primary
                        .handle_task_complete(task_complete(sec, *worker, h), &mut None)
                        .await;
                    settle_pump().await;
                }
                drain_rechecks(&mut primary, &mut wm_rx).await;
            }

            // THE ACCEPTANCE SIGNATURE: BOTH secondaries ran the import — the
            // formerly-idle secondary triggered its OWN on-demand import rather
            // than staying idle while sec-0 took every build.
            assert_eq!(
                import_secs.len(),
                2,
                "BOTH secondaries must trigger their own import (idle-secondary \
                 under-utilization fix); import ran on: {import_secs:?}"
            );
            for sec in ["sec-0", "sec-1"] {
                assert_eq!(
                    primary.cluster_state_for_test().affine_state(sec, affine_id),
                    SecondaryCell::Done,
                    "the idle secondary {sec} imported locally (cell Done)"
                );
            }
        })
        .await;
}

/// #652 B import-FAIL edge (owner requirement): when a blocked build's import
/// FAILS on its secondary but is still SATISFIABLE on another, the failure event
/// drains the build from the per-secondary blocked map IMMEDIATELY and re-routes
/// it — it does not strand until the 5-min reconcile. The build ultimately runs
/// on the surviving secondary and the run completes.
#[tokio::test(flavor = "current_thread")]
async fn affine_dep_work_import_failed_reroutes_blocked_build() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let import = affine_import("import");
            let import_hash = compute_task_hash(&import);
            // ONE build → it is blocked on exactly one secondary at a time.
            let build = work_dep("build", "import");
            let build_hash = compute_task_hash(&build);

            let (mut primary, mut ends, mut wm_rx, _mesh) =
                primary_two_secondaries_with(vec![import, build]);
            confirm_two(&mut primary).await;

            drain_rechecks(&mut primary, &mut wm_rx).await;
            // Find where the import dispatched (the build is blocked there).
            let mut wave: Vec<(String, String, u32, String)> = Vec::new();
            for (_id, rx, _tx) in ends.iter_mut() {
                wave.extend(assignments(rx));
            }
            let (fail_sec, fail_worker) = wave
                .iter()
                .find(|(_, _, _, h)| *h == import_hash)
                .map(|(_, sec, w, _)| (sec.clone(), *w))
                .expect("import dispatched for the blocked build");
            assert!(
                primary.affine_is_blocked_on_import_for_test(&fail_sec, &build_hash),
                "the build is blocked on {fail_sec} before the import fails"
            );

            // FAIL the import on that secondary (genuine terminal). The cell
            // flips Failed, the import is still satisfiable on the OTHER
            // secondary, so the blocked build must be drained + re-routed NOW.
            primary
                .handle_task_failed(task_failed(&fail_sec, fail_worker, &import_hash), &mut None)
                .await;
            settle_pump().await;
            // The build is no longer blocked on the failed secondary.
            assert!(
                !primary.affine_is_blocked_on_import_for_test(&fail_sec, &build_hash),
                "the failed import must drain the blocked build from {fail_sec} immediately"
            );

            // Drive to quiescence, completing imports/builds. The build re-routes
            // to the surviving secondary, its import runs there, and it completes.
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
            assert!(
                primary
                    .cluster_state_for_test()
                    .task_state(&build_hash)
                    .is_some_and(|s| s.is_terminal()),
                "the re-routed build must complete on the surviving secondary"
            );
            assert!(
                primary.run_complete_check(),
                "the run completes after the import fails on one secondary + re-routes"
            );
        })
        .await;
}

/// #652 concern C: the 5-min reconcile arm is the ORPHAN net for affine-blocked
/// work. A build BLOCKED on a per-secondary import whose unblock event was LOST
/// (its cell is neither Done nor Queued — e.g. the import's holder died without a
/// terminal) is drained from the blocked map by `reconcile_orphaned_blocked_work`
/// and its placement-dedup guard cleared, so the next placement pass re-routes
/// it (instead of stranding forever). A still-healthy block (cell Queued —
/// import in flight) is left untouched by the same sweep.
#[tokio::test(flavor = "current_thread")]
async fn reconcile_reroutes_orphaned_affine_blocked_work() {
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

            drain_rechecks(&mut primary, &mut wm_rx).await;
            // Find the secondary the build is blocked on (where its import ran).
            let mut wave: Vec<(String, String, u32, String)> = Vec::new();
            for (_id, rx, _tx) in ends.iter_mut() {
                wave.extend(assignments(rx));
            }
            let blocked_sec = wave
                .iter()
                .find(|(_, _, _, h)| *h == import_hash)
                .map(|(_, sec, _, _)| sec.clone())
                .expect("import dispatched for the blocked build");
            assert!(primary.affine_is_blocked_on_import_for_test(&blocked_sec, &build_hash));
            assert!(primary.affine_work_is_placed_for_test(&build_hash));

            // HEALTHY case: the import cell is Queued (in flight) on a reachable
            // secondary → reconcile keeps the block untouched.
            primary.reconcile_orphaned_blocked_work().await;
            assert!(
                primary.affine_is_blocked_on_import_for_test(&blocked_sec, &build_hash),
                "a healthy block (cell Queued, reachable) survives reconcile"
            );

            // LOST-EVENT: reset the import cell Queued → NotDone (the import's
            // holder died without a terminal — no Finished/bounce will arrive).
            let affine_id = primary
                .cluster_state_for_test()
                .affine_id_for_hash(&import_hash)
                .unwrap();
            primary
                .cluster_state_mut_for_test()
                .apply(ClusterMutation::SecondaryCellUnqueued {
                    secondary: blocked_sec.clone(),
                    cell_id: affine_id.0,
                    generation: 1,
                });

            // Reconcile now sees an orphan (cell NotDone, no terminal coming):
            // drains the block + clears the placement guard so it re-routes.
            primary.reconcile_orphaned_blocked_work().await;
            assert!(
                !primary.affine_is_blocked_on_import_for_test(&blocked_sec, &build_hash),
                "the orphaned block must be drained by reconcile"
            );
            assert!(
                !primary.affine_work_is_placed_for_test(&build_hash),
                "reconcile clears the orphan's placement guard so it re-routes"
            );
        })
        .await;
}

// ── #652 backpressure-bounce hot-loop (general-dispatch sibling of the affine spin) ──

/// A plain (non-affine) WORK task in phase "work" — the general-dispatch fixture
/// for the backpressure-bounce tests.
fn plain_work(name: &str) -> TaskInfo<TestId> {
    let mut t = make_binary(name, 20);
    t.phase_id = PhaseId::from("work");
    t.type_id = TypeId::from("default");
    t
}

/// A CAPACITY-shaped backpressure `TaskFailed` ("No idle worker available") —
/// the genuine at-capacity bounce the #652 hot-loop fix gates (distinct from the
/// worker-respawn "worker pipe broken; respawning" shape, which recovers
/// promptly). The secondary's workers are busy with OTHER work; it could not
/// place this inbound assignment.
fn task_failed_capacity(secondary: &str, worker: u32, task_hash: &str) -> DistributedMessage<TestId> {
    DistributedMessage::TaskFailed {
        target: None,
        sender_id: secondary.into(),
        timestamp: 0.0,
        secondary_id: secondary.into(),
        worker_id: worker,
        task_hash: task_hash.into(),
        error_type: dynrunner_core::ErrorType::Recoverable,
        error_message: "No idle worker available".into(),
        delivery_seq: None,
        msgs_posted_through: None,
    }
}

/// #652 backpressure hot-loop fix: a backpressure-bounced task must NOT be
/// re-dispatched to the secondary that just bounced it (it is at capacity) —
/// the bounce no longer drives a bypass-backpressure recheck that re-targets the
/// full secondary (the 24k-redispatch hot-loop). With one secondary + one task,
/// the bounce requeues the task and the recheck leaves it queued (the bounced
/// secondary is gated), instead of bouncing it forever.
#[tokio::test(flavor = "current_thread")]
async fn backpressure_bounce_does_not_redispatch_to_same_full_secondary() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let task = plain_work("t0");
            let task_hash = compute_task_hash(&task);

            // One secondary, one worker.
            let (mut primary, mut ends, mut wm_rx, _mesh) =
                primary_two_secondaries_with(vec![task]);
            primary
                .handle_cluster_mutation(capacity_batch("sec-0", 1), &mut None)
                .await;
            primary.handle_mesh_ready(mesh_ready_from("sec-0"));
            // (sec-1 is NOT confirmed — the task can only go to sec-0.)

            // Initial dispatch: the task goes to sec-0's worker.
            drain_rechecks(&mut primary, &mut wm_rx).await;
            let mut first: Vec<(String, String, u32, String)> = Vec::new();
            for (_id, rx, _tx) in ends.iter_mut() {
                first.extend(assignments(rx));
            }
            let (_, sec, worker, _) = first
                .iter()
                .find(|(_, _, _, h)| *h == task_hash)
                .cloned()
                .expect("the task dispatched to sec-0");
            assert_eq!(sec, "sec-0");

            // sec-0 BACKPRESSURE-bounces it (no idle worker). The task requeues;
            // sec-0 is flagged backpressured.
            primary
                .handle_task_failed(task_failed_capacity(&sec, worker, &task_hash), &mut None)
                .await;
            settle_pump().await;
            // The bounce-emitted recheck must NOT re-target the full sec-0.
            drain_rechecks(&mut primary, &mut wm_rx).await;
            let mut after: Vec<(String, String, u32, String)> = Vec::new();
            for (_id, rx, _tx) in ends.iter_mut() {
                after.extend(assignments(rx));
            }
            assert!(
                after.iter().all(|(_, s, _, _)| s != "sec-0"),
                "the bounced task must NOT be re-dispatched to the at-capacity \
                 sec-0 (the hot-loop fix); got {after:?}"
            );
        })
        .await;
}

/// #652 backpressure fix point 3: a bounced task is re-dispatched to a DIFFERENT
/// idle secondary IMMEDIATELY (the per-secondary gate skips only the bounced
/// one, not the whole pool). sec-0 bounces; sec-1 is idle → the task lands there.
#[tokio::test(flavor = "current_thread")]
async fn backpressure_bounce_redispatches_to_other_idle_secondary() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let task = plain_work("t0");
            let task_hash = compute_task_hash(&task);

            let (mut primary, mut ends, mut wm_rx, _mesh) =
                primary_two_secondaries_with(vec![task]);
            confirm_two(&mut primary).await;

            // Initial dispatch lands the single task on one secondary.
            drain_rechecks(&mut primary, &mut wm_rx).await;
            let mut first: Vec<(String, String, u32, String)> = Vec::new();
            for (_id, rx, _tx) in ends.iter_mut() {
                first.extend(assignments(rx));
            }
            let (_, first_sec, worker, _) = first
                .iter()
                .find(|(_, _, _, h)| *h == task_hash)
                .cloned()
                .expect("the task dispatched");

            // That secondary bounces it. The OTHER secondary is idle, so the
            // requeued task must re-dispatch THERE immediately.
            primary
                .handle_task_failed(
                    task_failed_capacity(&first_sec, worker, &task_hash),
                    &mut None,
                )
                .await;
            settle_pump().await;
            drain_rechecks(&mut primary, &mut wm_rx).await;
            let mut after: Vec<(String, String, u32, String)> = Vec::new();
            for (_id, rx, _tx) in ends.iter_mut() {
                after.extend(assignments(rx));
            }
            let redispatch = after.iter().find(|(_, _, _, h)| *h == task_hash);
            assert!(
                redispatch.is_some(),
                "the bounced task must re-dispatch to the OTHER idle secondary \
                 immediately; got {after:?}"
            );
            let (_, new_sec, _, _) = redispatch.unwrap();
            assert_ne!(
                *new_sec, first_sec,
                "the re-dispatch must go to a DIFFERENT secondary than the bounced one"
            );
        })
        .await;
}

/// #661 LOAD-BALANCE PULL: an idle secondary's worker pulls a parked
/// waiting-on-dependents work off a BUSY secondary, instead of sitting idle while
/// the busy secondary grinds its toolchain-bound backlog.
///
/// THE BUG SHAPE (asm-dataset 2-node): post-#652 a work WAITING on its
/// per-secondary import is parked in the per-secondary BLOCKED map (keyed to the
/// busy secondary), NOT in any queue. The queue-stealing idle-steal only looks at
/// queues, so it can NEVER pull a parked work — the idle node's freed workers
/// can't drain the busy node's backlog. Here: `build` is parked blocked on sec-0
/// (busy, its worker occupied), sec-1 is idle (empty queue, empty pool view). The
/// pull must re-key `build`'s block onto sec-1, dispatch its NotDone import on
/// sec-1's idle worker, and (once the import completes) run `build` on sec-1 —
/// the idle worker is NOT left idle while sec-0 has parked backlog.
#[tokio::test(flavor = "current_thread")]
async fn affine_idle_worker_pulls_parked_waiting_work_off_busy_secondary() {
    use dynrunner_core::ResourceMap;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let import = affine_import("import");
            let import_hash = compute_task_hash(&import);
            let build = work_dep("build", "import");
            let build_hash = compute_task_hash(&build);

            let (mut primary, mut ends, mut wm_rx, _mesh) =
                primary_two_secondaries_with(vec![import, build.clone()]);
            confirm_two(&mut primary).await;

            let affine_id = primary
                .cluster_state_for_test()
                .affine_id_for_hash(&import_hash)
                .expect("registered affine-id");

            // STAGE the strand on sec-0 (the BUSY secondary): record `build` placed
            // and PARK it in sec-0's per-secondary blocked map waiting on the import
            // cell (the exact post-#652 state — work withheld from the global view,
            // absent from every queue, blocked-on-import on the busy secondary).
            // OCCUPY sec-0's worker so the idle-steal sees no donor queue and sec-0
            // is genuinely busy.
            assert!(primary.affine_record_placed_work_for_test(&build_hash));
            primary
                .affine_scheduler
                .block_until_import("sec-0", &build_hash, vec![affine_id]);
            let busy_idx = primary
                .worker_idx_for("sec-0", 0)
                .expect("sec-0 has a worker");
            let occupier = make_binary("occupier", 10);
            assert!(primary.commit_assignment(
                busy_idx,
                std::sync::Arc::new(occupier.clone()),
                compute_task_hash(&occupier),
                ResourceMap::new(),
            ));

            // Pre-state: build is parked on sec-0, NOT sec-1; both cells NotDone.
            assert!(primary.affine_is_blocked_on_import_for_test("sec-0", &build_hash));
            assert!(!primary.affine_is_blocked_on_import_for_test("sec-1", &build_hash));
            for sec in ["sec-0", "sec-1"] {
                assert_eq!(
                    primary.cluster_state_for_test().affine_state(sec, affine_id),
                    SecondaryCell::NotDone,
                );
            }

            // THE PULL: sec-1's idle worker has nothing else (empty pool view, empty
            // own queue, no steal donor — sec-0's worker is occupied and sec-0's
            // QUEUE is empty since `build` is parked in the BLOCKED map). The new
            // load-balance source must pull `build` onto sec-1.
            let idle_idx = primary
                .worker_idx_for("sec-1", 0)
                .expect("sec-1 has a worker");
            let pulled = primary.try_affine_pull_waiting_for_worker(idle_idx).await;
            settle_pump().await;
            assert!(
                pulled,
                "the idle sec-1 worker must PULL the parked build off busy sec-0 \
                 (RED pre-fix: idle-steal only steals from QUEUES, so a parked \
                 blocked work is never pulled and the worker sits idle)"
            );

            // RE-KEYED: the block moved from sec-0 to sec-1 (the dependent now runs
            // on sec-1), and the import is now in flight on sec-1 (cell Queued + a
            // holding slot), NEVER on sec-0 (its worker is busy with the occupier).
            assert!(
                !primary.affine_is_blocked_on_import_for_test("sec-0", &build_hash),
                "the parked block must be RE-KEYED off the busy secondary"
            );
            assert!(
                primary.affine_is_blocked_on_import_for_test("sec-1", &build_hash),
                "the block must be re-keyed ONTO the idle secondary (build runs here \
                 once its import lands here)"
            );
            assert_eq!(
                primary.cluster_state_for_test().affine_state("sec-1", affine_id),
                SecondaryCell::Queued,
                "the pull dispatched the import on the idle secondary (cell Queued)"
            );
            assert!(
                primary.secondary_has_slot_holding_hash("sec-1", &import_hash),
                "the import landed a holding slot on the idle worker"
            );
            assert_eq!(
                primary.cluster_state_for_test().affine_state("sec-0", affine_id),
                SecondaryCell::NotDone,
                "the import was NOT dispatched on the busy secondary"
            );

            // The import dispatched on sec-1's worker (the wire confirms it).
            let mut import_on_sec1 = false;
            for (id, rx, _tx) in ends.iter_mut() {
                for (_, sec, _, h) in assignments(rx) {
                    if h == import_hash && sec == "sec-1" {
                        import_on_sec1 = true;
                    }
                    assert_ne!(
                        (id.as_str(), h.as_str()),
                        ("sec-0", import_hash.as_str()),
                        "the import must not dispatch on the busy secondary"
                    );
                }
            }
            assert!(import_on_sec1, "the import dispatched on the idle sec-1 worker");

            // LOAD-BALANCED END-TO-END: complete the import on sec-1 → its cell goes
            // Done → `build` unblocks + re-enqueues onto sec-1 → it dispatches +
            // runs THERE (on the formerly-idle secondary), proving the pull actually
            // moved the dependent's execution off the busy node.
            primary
                .handle_task_complete(task_complete("sec-1", 0, &import_hash), &mut None)
                .await;
            settle_pump().await;
            drain_rechecks(&mut primary, &mut wm_rx).await;

            let mut build_ran_on_sec1 = false;
            for (_id, rx, _tx) in ends.iter_mut() {
                for (_, sec, _, h) in assignments(rx) {
                    if h == build_hash {
                        assert_eq!(
                            sec, "sec-1",
                            "the build must run on the secondary it was pulled onto"
                        );
                        build_ran_on_sec1 = true;
                    }
                }
            }
            assert!(
                build_ran_on_sec1,
                "the pulled build must eventually RUN on the idle secondary \
                 (load-balanced off the busy one)"
            );
        })
        .await;
}

/// #661 GUARD (not-Done-on-S2): the pull never re-imports a toolchain already
/// present on the idle secondary. A work parked on busy sec-0 whose import is
/// ALREADY `Done` on idle sec-1 gates `Ready` there (no NotDone import to run),
/// so the pull SKIPS it — no duplicate import dispatch, the cell stays `Done`, and
/// the (queue-empty) idle worker is left for the eager-prep filler.
#[tokio::test(flavor = "current_thread")]
async fn affine_pull_skips_work_whose_import_already_done_on_idle_secondary() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let import = affine_import("import");
            let import_hash = compute_task_hash(&import);
            let build = work_dep("build", "import");
            let build_hash = compute_task_hash(&build);

            let (mut primary, mut ends, _wm_rx, _mesh) =
                primary_two_secondaries_with(vec![import, build.clone()]);
            confirm_two(&mut primary).await;

            let affine_id = primary
                .cluster_state_for_test()
                .affine_id_for_hash(&import_hash)
                .expect("registered affine-id");

            // Park `build` on busy sec-0, but mark the import ALREADY `Done` on idle
            // sec-1 (the toolchain is already present there).
            assert!(primary.affine_record_placed_work_for_test(&build_hash));
            primary
                .affine_scheduler
                .block_until_import("sec-0", &build_hash, vec![affine_id]);
            primary
                .cluster_state_mut_for_test()
                .apply(ClusterMutation::SecondaryCellFinished {
                    secondary: "sec-1".into(),
                    cell_id: affine_id.0,
                    generation: 1,
                });
            // drop any wire traffic from setup
            for (_id, rx, _tx) in ends.iter_mut() {
                let _ = assignments(rx);
            }

            let idle_idx = primary
                .worker_idx_for("sec-1", 0)
                .expect("sec-1 has a worker");
            let pulled = primary.try_affine_pull_waiting_for_worker(idle_idx).await;
            settle_pump().await;

            assert!(
                !pulled,
                "the pull must SKIP a work whose import is already Done on the idle \
                 secondary (it gates Ready, not StrandedHere — no toolchain to pull)"
            );
            assert_eq!(
                primary.cluster_state_for_test().affine_state("sec-1", affine_id),
                SecondaryCell::Done,
                "the not-Done guard prevents re-importing a toolchain already \
                 present on the idle secondary (cell stays Done, not re-Queued)"
            );
            assert!(
                !primary.secondary_has_slot_holding_hash("sec-1", &import_hash),
                "no duplicate import dispatch on the idle secondary"
            );
            // The original block is untouched (the pull declined it).
            assert!(
                primary.affine_is_blocked_on_import_for_test("sec-0", &build_hash),
                "a skipped candidate's block is NOT re-keyed"
            );
            for (_id, rx, _tx) in ends.iter_mut() {
                for (_, _, _, h) in assignments(rx) {
                    assert_ne!(h, import_hash, "no import dispatched by a skipped pull");
                }
            }
        })
        .await;
}

/// #661 GUARD (base-before-delta): pulling a multi-dep work onto the idle
/// secondary dispatches its imports in LIST ORDER — the BASE before the delta —
/// the same ordering the on-demand `StrandedHere` path enforces. The pull routes
/// through `dispatch_affine_import_on_demand`, which kicks the FIRST not-`Done`
/// dep, so a delta is never imported ahead of its base on the pulled-onto node.
#[tokio::test(flavor = "current_thread")]
async fn affine_pull_multidep_dispatches_base_before_delta_on_idle_secondary() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let base = affine_import("base");
            let delta = affine_import("delta");
            let base_hash = compute_task_hash(&base);
            let delta_hash = compute_task_hash(&delta);
            let build = work_two_deps("build", "base", "delta");
            let build_hash = compute_task_hash(&build);

            let (mut primary, mut ends, _wm_rx, _mesh) =
                primary_two_secondaries_with(vec![base, delta, build.clone()]);
            confirm_two(&mut primary).await;

            let base_aid = primary
                .cluster_state_for_test()
                .affine_id_for_hash(&base_hash)
                .expect("registered base affine-id");

            // Park the multi-dep build on busy sec-0; both imports NotDone on idle
            // sec-1. The pull must kick the BASE first (list order), never the delta.
            assert!(primary.affine_record_placed_work_for_test(&build_hash));
            primary.affine_scheduler.block_until_import(
                "sec-0",
                &build_hash,
                vec![base_aid],
            );
            for (_id, rx, _tx) in ends.iter_mut() {
                let _ = assignments(rx);
            }

            let idle_idx = primary
                .worker_idx_for("sec-1", 0)
                .expect("sec-1 has a worker");
            assert!(primary.try_affine_pull_waiting_for_worker(idle_idx).await);
            settle_pump().await;

            // ONLY the base imported on sec-1; the delta did NOT (its base is not yet
            // Done, so the list-order gate withholds it).
            assert!(
                primary.secondary_has_slot_holding_hash("sec-1", &base_hash),
                "the BASE import dispatched first on the idle secondary"
            );
            assert!(
                !primary.secondary_has_slot_holding_hash("sec-1", &delta_hash),
                "the DELTA must NOT dispatch before its base is Done on the \
                 pulled-onto secondary (base-before-delta order preserved)"
            );
            let mut dispatched: Vec<String> = Vec::new();
            for (_id, rx, _tx) in ends.iter_mut() {
                for (_, sec, _, h) in assignments(rx) {
                    assert_eq!(sec, "sec-1", "the pull dispatches on the idle secondary");
                    dispatched.push(h);
                }
            }
            assert!(dispatched.contains(&base_hash), "base dispatched");
            assert!(
                !dispatched.contains(&delta_hash),
                "delta NOT dispatched ahead of base"
            );
        })
        .await;
}

// ── #668: dead-secondary affine-import discrimination ──────────────────────
//
// An affine IMPORT in-flight on a secondary that DIES must be treated
// PER-SECONDARY (its terminal is the bitvector cell), NEVER as a global task
// terminal. The dead-secondary recovery's `is_reassignable()` dichotomy had no
// affine arm, so a `SecondaryAffine` import (is_reassignable == false) fell into
// the non-reassignable `else` and emitted a GLOBAL `ClusterMutation::TaskFailed`
// — a spurious doom that lies dormant until a failover hydrate loads the CRDT
// `Failed` into `failed_tasks` and the affine readiness gate dooms the import's
// whole dependent subtree (`Unsatisfiable`).

/// Stage a GENUINELY in-flight affine import on one secondary, with its
/// dependent build blocked `InFlightHere` on that secondary's claimed cell —
/// the exact pre-death state the dead-secondary recovery must discriminate.
/// Returns `(primary, dead_secondary, worker, import_hash, build_hash, affine_id)`.
/// The dependent never dispatches (its import is not yet `Done`).
async fn stage_inflight_import_with_blocked_dependent() -> (
    TestPrimary,
    String,
    u32,
    String,
    String,
    crate::cluster_state::SecondaryCellId,
) {
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

    drain_rechecks(&mut primary, &mut wm_rx).await;
    let mut import_dispatch: Option<(String, u32)> = None;
    for (_id, rx, _tx) in ends.iter_mut() {
        for (_, sec, worker, h) in assignments(rx) {
            assert_ne!(h, build_hash, "build must not dispatch before import Done");
            if h == import_hash {
                import_dispatch = Some((sec.clone(), worker));
            }
        }
    }
    let (sec, worker) =
        import_dispatch.expect("the on-demand import dispatched (StrandedHere)");
    // The on-demand dispatch claimed the cell `Queued` and seeded the in-flight
    // ledger + holding slot for the import on this secondary.
    assert_eq!(
        primary.cluster_state_for_test().affine_state(&sec, affine_id),
        SecondaryCell::Queued,
        "on-demand dispatch claims the cell Queued"
    );
    assert!(
        primary.in_flight.contains_key(&import_hash),
        "the in-flight ledger holds the import after dispatch"
    );
    assert_eq!(
        primary.in_flight[&import_hash].secondary_id, sec,
        "the ledger entry points at the dispatching secondary"
    );
    assert!(
        primary.affine_is_blocked_on_import_for_test(&sec, &build_hash),
        "the dependent build is blocked InFlightHere on the import's cell"
    );
    (primary, sec, worker, import_hash, build_hash, affine_id)
}

/// PART A (root): the dead-secondary recovery returns NO global `TaskFailed` for
/// an in-flight affine import — its death is per-secondary, not a global
/// terminal.
///
/// RED pre-fix: `recover_inflight_for_dead_secondary` had no `is_secondary_affine`
/// arm, so the import (is_reassignable == false) fell into the non-reassignable
/// `else` and the returned mutation set carried a
/// `ClusterMutation::TaskFailed { hash: import }`.
#[tokio::test(flavor = "current_thread")]
async fn dead_secondary_affine_import_emits_no_global_task_failed() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut primary, sec, _worker, import_hash, _build_hash, _affine_id) =
                stage_inflight_import_with_blocked_dependent().await;

            let mutations = primary.recover_inflight_for_dead_secondary(&sec);

            // No global terminal for the affine import — its terminal is the
            // per-secondary cell, never `failed_tasks`/CRDT `Failed`.
            assert!(
                !mutations.iter().any(|m| matches!(
                    m,
                    ClusterMutation::TaskFailed { hash, .. } if *hash == import_hash
                )),
                "an in-flight affine import on a dead secondary must NOT emit a \
                 global TaskFailed (its terminal is per-secondary); got {mutations:?}"
            );
            // The in-flight ledger entry is dropped consistently (its per-dispatch
            // type slot was released at the loop head), so no phantom import lingers.
            assert!(
                !primary.in_flight.contains_key(&import_hash),
                "the dead secondary's in-flight import entry is dropped"
            );
        })
        .await;
}

/// PART A (full chain): the dead-secondary recovery routed through the genuine
/// member-removal primitive (`requeue_dead_secondary`) leaves the affine
/// import's CRDT state NON-`Failed` and re-routes the dependent to a LIVE
/// secondary (re-derived for re-placement), never doomed.
///
/// RED pre-fix: the spurious global `TaskFailed` applied CRDT `TaskState =
/// Failed` for the import, which (latently) dooms every dependent on the next
/// failover hydrate.
#[tokio::test(flavor = "current_thread")]
async fn dead_secondary_affine_import_full_chain_reroutes_dependent() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut primary, sec, _worker, import_hash, build_hash, _affine_id) =
                stage_inflight_import_with_blocked_dependent().await;

            // Drive the FULL dead-secondary path (recover + affine reroute +
            // PeerRemoved broadcast), exactly as the heartbeat monitor would on a
            // keepalive miss.
            primary
                .requeue_dead_secondary_for_test(&sec)
                .await
                .expect("dead-secondary recovery");
            settle_pump().await;

            // The import's CRDT state was NOT flipped to `Failed` — no global
            // terminal was authored for it.
            assert!(
                primary
                    .cluster_state_for_test()
                    .task_state(&import_hash)
                    .is_none_or(|s| !matches!(
                        s,
                        crate::cluster_state::TaskState::Failed { .. }
                    )),
                "the affine import must NOT be flipped to CRDT Failed on a \
                 dead-secondary death; got {:?}",
                primary.cluster_state_for_test().task_state(&import_hash)
            );
            // The import is NOT in the global doom-gate.
            assert!(
                !primary.failed_tasks.contains_key(&import_hash),
                "the affine import must NOT enter the global failed_tasks gate"
            );
            // The dependent is alive (not terminal-failed) and was drained from
            // the dead secondary's per-secondary blocked map so it can re-route to
            // a live secondary (its placement-dedup was cleared for re-derivation).
            assert!(
                !primary.affine_is_blocked_on_import_for_test(&sec, &build_hash),
                "the dependent must be drained from the dead secondary's blocked map"
            );
            assert!(
                primary
                    .cluster_state_for_test()
                    .task_state(&build_hash)
                    .is_some_and(|s| !s.is_terminal()),
                "the dependent must NOT be terminal-failed — it re-routes to a \
                 live secondary; got {:?}",
                primary.cluster_state_for_test().task_state(&build_hash)
            );
        })
        .await;
}

/// PART A (silent-peer twin, #671): the #556 lazy local requeue
/// (`requeue_silent_held_work_locally`) for a SILENT-BUT-ROSTERED secondary
/// holding an in-flight cell-bearing affine import must BOUNCE-recover it —
/// reset the now-holderless `Queued` cell `→ NotDone` so the import re-derives
/// on-demand on a live secondary, NOT leave the dependent blocked on a phantom
/// `Queued`. The dead-secondary `reroute_affine_blocked_on` mirror does NOT work
/// here: the silent peer STAYS rostered, so its stale `Queued` cell remains a
/// placement candidate and the next placement pass re-derives the dependent
/// right back onto it (drain-then-re-block). Resetting the cell clears the
/// strand at its source.
///
/// REVERT-CHECK RED: replace the bounce-recovery in
/// `requeue_silent_held_work_locally` with `reroute_affine_blocked_on` (or remove
/// it) and the cell-reset assertion fails — the cell stays `Queued` and the
/// dependent re-blocks on the silent secondary's phantom cell. (Also RED on
/// Finding-1: the `is_empty` early-continue, before the bounce ran, skipped the
/// whole recovery for a cell-only holder.)
#[tokio::test(flavor = "current_thread")]
async fn silent_peer_affine_import_bounce_recovers_holderless_cell() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut primary, sec, worker, import_hash, build_hash, affine_id) =
                stage_inflight_import_with_blocked_dependent().await;
            let import_type = primary.in_flight[&import_hash].task.type_id.clone();
            let type_inflight_before = primary.in_flight_per_type_for_test(&import_type);

            // The affine harness confirms secondaries via capacity + mesh-ready,
            // which does NOT write the `self.secondaries` roster record the real
            // welcome handshake would; the #556 lazy path's roster guard reads it.
            // Stamp the Operational connection record the handshake would have.
            primary.mark_secondary_operational_for_test(&sec);
            assert!(
                primary.secondaries.contains_key(&sec),
                "the silent secondary must stay rostered for the lazy requeue path"
            );

            // Drive the #556 lazy local requeue for the silent secondary (the
            // dispatch-altitude scheduling-suspect path), exactly as the
            // local-suspect sweep would on a silent-but-alive peer.
            let mut suspects = std::collections::BTreeSet::new();
            suspects.insert(sec.clone());
            primary
                .requeue_silent_held_work_locally(&suspects)
                .await
                .expect("silent-peer local requeue");
            settle_pump().await;

            // The in-flight import leg was dropped exactly ONCE (no phantom
            // ledger entry, no double-handling between recover_inflight + bounce).
            assert!(
                !primary.in_flight.contains_key(&import_hash),
                "the silent secondary's in-flight import entry is dropped"
            );
            // The import is NOT globally doomed (its terminal is per-secondary).
            assert!(
                !primary.failed_tasks.contains_key(&import_hash),
                "the affine import must NOT enter the global failed_tasks gate"
            );
            // The holding slot was freed ONCE → Idle.
            assert!(
                primary.slot_is_idle_for_test(&sec, worker),
                "the holding slot must be freed to Idle by the bounce recovery"
            );
            // The per-type concurrency slot was released ONCE (not double-released):
            // the in-flight-per-type counter returns to its pre-dispatch value.
            assert_eq!(
                primary.in_flight_per_type_for_test(&import_type),
                type_inflight_before.saturating_sub(1),
                "the per-type concurrency slot must be released exactly once"
            );
            // THE BUG THIS FIXES (cell-reset is the source-level cure): the
            // holderless `Queued` cell on the silent secondary is reset to
            // `NotDone`, so the import re-derives StrandedHere on-demand instead
            // of the dependent waiting on a phantom Queued.
            assert_eq!(
                primary.cluster_state_for_test().affine_state(&sec, affine_id),
                SecondaryCell::NotDone,
                "the holderless Queued cell must be reset to NotDone by the bounce \
                 recovery (NOT left Queued, which would re-attract the dependent)"
            );
            // And the dependent is NOT left blocked on the silent secondary's
            // (now-reset) cell — it re-derives (prompt recovery), the proof the
            // bounce-recovery achieves what reroute did not.
            assert!(
                !primary.affine_is_blocked_on_import_for_test(&sec, &build_hash),
                "the dependent must NOT remain blocked on the silent secondary's \
                 reset cell (it re-derives on a live secondary)"
            );
            // The dependent stays alive (it re-derives; it is not terminal-failed).
            assert!(
                primary
                    .cluster_state_for_test()
                    .task_state(&build_hash)
                    .is_some_and(|s| !s.is_terminal()),
                "the dependent must NOT be terminal-failed — it re-derives; got {:?}",
                primary.cluster_state_for_test().task_state(&build_hash)
            );
        })
        .await;
}

/// PART B (defense-in-depth): a failover hydrate of a CRDT that records an
/// affine import as `TaskState::Failed` must NOT load the import hash into
/// `failed_tasks` (the global doom-gate the affine readiness check reads), and
/// the dependent must NOT hydrate terminal-failed.
///
/// This pins the LATENT failover blast directly: the affine readiness gate's
/// FIRST check is `failed_tasks.contains_key(import_hash)` → `Unsatisfiable` for
/// every dependent. Excluding affine hashes at the hydrate `failed_tasks`
/// population site closes that cascade independently of the root fix.
///
/// RED pre-fix: the fat `Failed` arm inserted EVERY `Failed` hash (affine
/// included) into `failed_tasks`.
#[tokio::test(flavor = "current_thread")]
async fn hydrate_excludes_failed_affine_import_from_failed_tasks() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let import = affine_import("import");
            let import_hash = compute_task_hash(&import);
            let build = work_dep("build", "import");
            let build_hash = compute_task_hash(&build);

            // Build a CRDT in the exact post-latent-bug shape: the affine import
            // recorded `TaskState::Failed` (as the spurious dead-secondary global
            // terminal would have left it), its dependent still Pending/Blocked.
            let (mut primary, _ends, _wm_rx, _mesh) =
                primary_two_secondaries_with(vec![import, build]);
            {
                let cs = primary.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::TaskFailed {
                    hash: import_hash.clone(),
                    kind: dynrunner_core::ErrorType::NonRecoverable,
                    error: "spurious dead-secondary global terminal".into(),
                    version: Default::default(),
                    attempt: Default::default(),
                });
            }
            assert!(
                matches!(
                    primary.cluster_state_for_test().task_state(&import_hash),
                    Some(crate::cluster_state::TaskState::Failed { .. })
                ),
                "fixture: the affine import is CRDT Failed before hydrate"
            );

            // Re-hydrate (the promoted-primary failover projection rebuild).
            primary
                .hydrate_from_cluster_state()
                .expect("hydrate the failed-affine-import graph");

            // The affine import hash must NOT be in the global doom-gate — its
            // terminal is per-secondary, never `failed_tasks` (which the affine
            // readiness check reads to fail every dependent `Unsatisfiable`).
            assert!(
                !primary.failed_tasks.contains_key(&import_hash),
                "hydrate must EXCLUDE a failed affine import from failed_tasks \
                 (RED pre-fix: the fat Failed arm inserted it, dooming dependents)"
            );
            // The dependent must NOT hydrate terminal-failed (no spurious
            // Unsatisfiable cascade off the affine doom-gate).
            assert!(
                primary
                    .cluster_state_for_test()
                    .task_state(&build_hash)
                    .is_some_and(|s| !s.is_terminal()),
                "the dependent must NOT be terminal-failed on hydrate; got {:?}",
                primary.cluster_state_for_test().task_state(&build_hash)
            );
        })
        .await;
}
