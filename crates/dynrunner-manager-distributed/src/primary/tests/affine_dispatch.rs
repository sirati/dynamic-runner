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
                AffineCell::Queued,
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
                AffineCell::NotDone,
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
                AffineCell::Queued,
                "the re-dispatch re-claims the cell Queued"
            );
        })
        .await;
}

/// PROTOCOL: the affine subsystem's backpressure-recovery mutation builder
/// emits `SecondaryAffineUnqueued` (the Queued → NotDone cell reset) for an
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

            // The affine import's bounce recovery is a `SecondaryAffineUnqueued`
            // (Queued → NotDone) — NOT a `TaskRequeued`.
            match primary.affine_unqueue_mutation("sec-0", &import_hash) {
                Some(ClusterMutation::SecondaryAffineUnqueued {
                    secondary,
                    affine_id: aid,
                    generation,
                }) => {
                    assert_eq!(secondary, "sec-0");
                    assert_eq!(aid, affine_id.0);
                    assert_eq!(generation, 0);
                }
                other => panic!(
                    "affine backpressure recovery must emit SecondaryAffineUnqueued, \
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
                .apply(ClusterMutation::SecondaryAffineQueued {
                    secondary: "sec-0".into(),
                    affine_id: affine_id.0,
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
                        == AffineCell::Failed
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
                    AffineCell::Failed,
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
                            AffineCell::Done,
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
                            AffineCell::Done,
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
                        == AffineCell::Failed
                });
                if both_failed {
                    break;
                }
            }

            for sec in ["sec-0", "sec-1"] {
                assert_eq!(
                    primary.cluster_state_for_test().affine_state(sec, affine_id),
                    AffineCell::Failed,
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
            // SecondaryAffineUnqueued) out-stamps the previous — the gate test can
            // walk a cell through any sequence of states. `gen` is a shared
            // monotone counter so every write wins over the prior cell value.
            let mut cell_gen: u64 = 1;
            let mut set = |primary: &mut TestPrimary,
                           sec: &str,
                           aid: crate::cluster_state::AffineId,
                           cell: AffineCell| {
                let g = cell_gen;
                cell_gen += 1;
                let mutation = match cell {
                    AffineCell::Queued => ClusterMutation::SecondaryAffineQueued {
                        secondary: sec.into(),
                        affine_id: aid.0,
                        generation: g,
                    },
                    AffineCell::Done => ClusterMutation::SecondaryAffineFinished {
                        secondary: sec.into(),
                        affine_id: aid.0,
                        generation: g,
                    },
                    AffineCell::Failed => ClusterMutation::SecondaryAffineFailed {
                        secondary: sec.into(),
                        affine_id: aid.0,
                        generation: g,
                    },
                    AffineCell::NotDone => ClusterMutation::SecondaryAffineUnqueued {
                        secondary: sec.into(),
                        affine_id: aid.0,
                        generation: g,
                    },
                };
                primary.cluster_state_mut_for_test().apply(mutation);
            };
            let label =
                |primary: &TestPrimary| primary.affine_gate_label_for_test("sec-0", &build);

            // THE BUG SHAPE: base in flight (Queued), delta NotDone. Must WAIT on
            // the unmet in-flight base — NOT skip to (and assign) the delta.
            set(&mut primary, "sec-0", base_aid, AffineCell::Queued);
            assert_eq!(
                label(&primary),
                "InFlightHere",
                "an in-flight (Queued) base is UNMET — the gate must WAIT (withhold \
                 assignment), never skip it to StrandedHere-dispatch the later \
                 NotDone delta (the multi-worker-same-node 'path is not valid' race)"
            );

            // base NotDone, delta NotDone → first not-Done is the base (NotDone),
            // so StrandedHere dispatches the base import (correct: base first).
            set(&mut primary, "sec-0", base_aid, AffineCell::NotDone);
            assert_eq!(
                label(&primary),
                "StrandedHere",
                "both NotDone → stranded on the FIRST (base) import"
            );

            // base Done, delta NotDone → advance past the met base to the delta
            // (NotDone) → StrandedHere on the delta (the original single-worker
            // order: base lands, THEN the delta dispatches).
            set(&mut primary, "sec-0", base_aid, AffineCell::Done);
            assert_eq!(
                label(&primary),
                "StrandedHere",
                "base Done (met) → advance to the delta (NotDone) and dispatch it"
            );

            // base Done, delta Queued (in flight) → the delta is now the unmet one.
            set(&mut primary, "sec-0", delta_aid, AffineCell::Queued);
            assert_eq!(
                label(&primary),
                "InFlightHere",
                "base met + delta in flight → wait on the unmet delta"
            );

            // base Done, delta Done → Ready.
            set(&mut primary, "sec-0", delta_aid, AffineCell::Done);
            assert_eq!(label(&primary), "Ready", "all deps Done → Ready");

            // FAILED IS ORDER-INDEPENDENT (unchanged): base Failed on sec-0 but
            // sec-1 still satisfiable → Reroute(sec-1). Reset sec-0's delta cell so
            // only the (order-independent) Failed base drives the decision.
            set(&mut primary, "sec-0", delta_aid, AffineCell::NotDone);
            set(&mut primary, "sec-0", base_aid, AffineCell::Failed);
            assert_eq!(
                label(&primary),
                "Reroute(sec-1)",
                "a Failed base is order-independent terminal → reroute to the \
                 still-satisfiable sibling secondary"
            );

            // base Failed on EVERY secondary → Unsatisfiable (unchanged).
            set(&mut primary, "sec-1", base_aid, AffineCell::Failed);
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
                                AffineCell::Done,
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
