//! Phase-lifecycle ordering regression. Pins two related invariants:
//!
//! 1. `on_phase_end(phase)` MUST NOT fire before every task that the
//!    pool ever marked in-flight for `phase` has had its TaskComplete
//!    (or TaskFailed) observed by the primary.
//!
//! 2. A `PrimaryCommand::SpawnTasks` queued from inside `on_phase_end`
//!    while the primary is still in its pre-operational-loop wait
//!    phase (`wait_for_connections`, `wait_for_mesh_ready`) must be
//!    applied inline by the cascade's per-iteration drain step
//!    BEFORE `operational_loop` runs its entry-time exit check.
//!    Asm-tokenizer's `FullPipelineTask.on_phase_end` is the live
//!    consumer of this contract: it discovers phase-(N+1) items only
//!    after `on_phase_end(N)` fires and injects them via
//!    `primary_handle.spawn_tasks`. Pre-fix the pre-loop dispatch
//!    sites passed `&mut None` for `command_rx`, so the cascade's
//!    drain step was a no-op — the spawn command sat on the channel
//!    until `operational_loop`'s entry-time
//!    `completed + failed >= total_tasks` check (with the un-refreshed
//!    pre-spawn `total_tasks`) tripped and exited the loop without
//!    ever polling it.
//!
//! The cascade fires `on_phase_end` synchronously from
//! `note_item_completed` AFTER the per-phase Completed EVENT tally is
//! max-bumped for the just-completed task (the `merge_task_state` join
//! bumps it when `handle_task_complete` applies the `TaskCompleted`
//! mutation, BEFORE `note_item_completed` runs the cascade — #358). So
//! when `on_phase_end` fires for a phase, the `completed` argument (read
//! from the replicated tally) is the cumulative count INCLUDING the task
//! whose `handle_task_complete` tick triggered the cascade. A premature
//! firing surfaces as `completed < phase_total`.

use super::*;

use std::sync::{Arc, Mutex};

use dynrunner_core::{PhaseId, SoftPreferredSecondaries, TypeId};

use crate::primary::command_channel::PrimaryCommand;

/// Build a `TaskInfo` placed in the named phase. Path uses `/tmp/`
/// (matching `make_binary`) so the dispatch unresolvable-path guard
/// is happy in the no-`src_network` fixture.
fn phased_binary(name: &str, phase: &str, size: u64) -> TaskInfo<TestId> {
    TaskInfo {
        path: std::path::PathBuf::from(format!("/tmp/{name}")),
        size,
        identifier: TestId(name.into()),
        phase_id: PhaseId::from(phase),
        type_id: TypeId::from("default"),
        affinity_id: None,
        payload: serde_json::Value::Null,
        task_id: name.into(),
        task_depends_on: vec![],
        preferred_secondaries: SoftPreferredSecondaries::default(),
        preferred_version: Default::default(),
        kind: Default::default(),
        setup_affinity: None,
        upload_file: None,
        resolved_path: None,
    }
}

/// Single interleaved event log so the test can assert ordering
/// across `on_phase_start` and `on_phase_end` firings without
/// reconstructing a wall-clock interleaving across two vectors.
#[derive(Debug, Clone, PartialEq, Eq)]
enum PhaseEvent {
    Start(String),
    End {
        phase: String,
        completed: u32,
        failed: u32,
    },
}

/// Smoking-gun reproducer (phase pre-populated). Phase `slow` has TWO
/// items: one instant (`slow_fast`), one that sleeps 500ms
/// (`slow_slow`). With 2 secondaries × 1 worker each, both items
/// dispatch in parallel — `slow_fast` completes ~immediately,
/// `slow_slow` completes ~500ms later. Phase `next` depends on `slow`
/// and is also pre-populated. Verifies `on_phase_end(slow)` fires
/// AFTER both slow items terminate.
#[tokio::test(flavor = "current_thread")]
async fn on_phase_end_fires_after_last_in_flight_completes() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut phase_deps: HashMap<PhaseId, Vec<PhaseId>> = HashMap::new();
            phase_deps.insert(PhaseId::from("next"), vec![PhaseId::from("slow")]);

            let binaries: Vec<TaskInfo<TestId>> = vec![
                phased_binary("slow_fast", "slow", 100),
                phased_binary("slow_slow", "slow", 50),
                phased_binary("next_one", "next", 50),
            ];

            run_phase_ordering_scenario(
                binaries,
                phase_deps,
                vec![("/tmp/slow_slow".to_string(), Duration::from_millis(500))],
                /* lazy_spawn_next: */ None,
                /* expected_total_completed: */ 3,
            )
            .await;
        })
        .await;
}

/// Smoking-gun reproducer (mixed-timing, multi-item phase). Phase
/// `fast` has FOUR items, one of which sleeps 200ms (`fast_3`); phase
/// `slow` has TWO items. With 2 secondaries × 1 worker each, the
/// first three `fast` items burn through ~instantly while `fast_3`
/// is still mid-sleep. Without the post-fix ordering guarantee a
/// premature `on_phase_end(fast)` would fire on the third fast
/// completion (in_flight=1 left for `fast_3`) and immediately
/// activate `slow` — a phase-slow task would dispatch while `fast_3`
/// is still in-flight. Mirrors the prior subagent's local-mode shape
/// applied to the distributed-mode in-process fixture.
#[tokio::test(flavor = "current_thread")]
async fn on_phase_end_fires_after_every_in_flight_item_terminates() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut phase_deps: HashMap<PhaseId, Vec<PhaseId>> = HashMap::new();
            phase_deps.insert(PhaseId::from("slow"), vec![PhaseId::from("fast")]);

            let binaries: Vec<TaskInfo<TestId>> = vec![
                phased_binary("fast_0", "fast", 100),
                phased_binary("fast_1", "fast", 90),
                phased_binary("fast_2", "fast", 80),
                phased_binary("fast_3", "fast", 70),
                phased_binary("slow_a", "slow", 60),
                phased_binary("slow_b", "slow", 50),
            ];

            run_phase_ordering_scenario(
                binaries,
                phase_deps,
                vec![("/tmp/fast_3".to_string(), Duration::from_millis(200))],
                /* lazy_spawn_next: */ None,
                /* expected_total_completed: */ 6,
            )
            .await;
        })
        .await;
}

/// Same as `on_phase_end_fires_after_last_in_flight_completes` but
/// `phase next` is NOT pre-populated; its items are injected lazily
/// via `PrimaryCommand::SpawnTasks` from inside the on_phase_end
/// callback. Mirrors the asm-tokenizer consumer pattern
/// (`FullPipelineTask.on_phase_end → primary_handle.spawn_tasks`)
/// where the next phase's items only enter the pool after
/// `on_phase_end` fires.
#[tokio::test(flavor = "current_thread")]
async fn on_phase_end_fires_after_last_in_flight_completes_with_lazy_spawn() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut phase_deps: HashMap<PhaseId, Vec<PhaseId>> = HashMap::new();
            phase_deps.insert(PhaseId::from("next"), vec![PhaseId::from("slow")]);

            // Phase `slow` is the focus: pre-populated, with one
            // slow item. Phase `next` is empty pre-run — its single
            // item lands via `spawn_tasks` from inside on_phase_end.
            let binaries: Vec<TaskInfo<TestId>> = vec![
                phased_binary("slow_fast", "slow", 100),
                phased_binary("slow_slow", "slow", 50),
            ];
            let next_phase_items = vec![phased_binary("next_one", "next", 50)];

            run_phase_ordering_scenario(
                binaries,
                phase_deps,
                vec![("/tmp/slow_slow".to_string(), Duration::from_millis(500))],
                Some(("slow".into(), next_phase_items)),
                /* expected_total_completed: */ 3,
            )
            .await;
        })
        .await;
}

/// Build a `TaskInfo` in `phase` carrying an intra-phase `task_depends_on`.
/// Used by the producer-path build-spawn reproducer so the lazily-spawned
/// `build_variant` items name their `build_common_dep` sibling.
fn phased_binary_dep(name: &str, phase: &str, depends_on: &[&str]) -> TaskInfo<TestId> {
    TaskInfo {
        task_depends_on: depends_on
            .iter()
            .map(|d| dynrunner_core::TaskDep {
                task_id: (*d).into(),
                phase_id: PhaseId::from(phase),
                inherit_outputs: false,
            })
            .collect(),
        ..phased_binary(name, phase, 50)
    }
}

/// Build a `TaskInfo` carrying CROSS-phase deps: each `(dep_phase, dep_id)`
/// names a prerequisite in a DIFFERENT phase. Mirrors the consumer's
/// `build_variant` tasks whose `build_compilers_depends_on` point at
/// `build_compilers`-phase toolchain tasks.
fn phased_binary_xdep(name: &str, phase: &str, cross_deps: &[(&str, &str)]) -> TaskInfo<TestId> {
    TaskInfo {
        task_depends_on: cross_deps
            .iter()
            .map(|(dep_phase, dep_id)| dynrunner_core::TaskDep {
                task_id: (*dep_id).into(),
                phase_id: PhaseId::from(*dep_phase),
                inherit_outputs: false,
            })
            .collect(),
        ..phased_binary(name, phase, 50)
    }
}

/// Producer-path regression (asm-dataset-nix, c39034f2). Mirrors the
/// consumer's full phase chain: `matrix_eval` (1 task) → `dependency_graph`
/// (1 task) → `build` (declared, EMPTY at run start). The `build` items
/// are injected from inside `on_phase_end("dependency_graph")` — one
/// `build_common_dep` (no deps, lands `Pending`) plus four `build_variant`
/// items that depend on it (land `Blocked`, auto-resume on the common-dep's
/// completion). This is the EXACT shape that silently dispatched ZERO build
/// tasks on the producer path.
///
/// The crux the reproducer pins: `total_tasks` is seeded at exactly 2
/// (eval + dep_graph). When BOTH seeded tasks terminate, the operational
/// loop's `run_complete_check` counter exit (`completed >= total_tasks`)
/// is satisfied UNLESS the `on_phase_end`-spawned build batch refreshed
/// `total_tasks` AND re-armed the pool before the next loop iteration. A
/// premature RunComplete broadcast finishes every secondary at the
/// dependency_graph drain edge and the 5 build tasks never dispatch — a
/// silent `completed=2, stranded≈0, rc=0` total=0.
#[tokio::test(flavor = "current_thread")]
async fn producer_path_build_spawn_dispatches_after_dependency_graph() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Full consumer chain. `build` depends on `dependency_graph`
            // which depends on `matrix_eval`.
            let mut phase_deps: HashMap<PhaseId, Vec<PhaseId>> = HashMap::new();
            phase_deps.insert(
                PhaseId::from("dependency_graph"),
                vec![PhaseId::from("matrix_eval")],
            );
            phase_deps.insert(
                PhaseId::from("build"),
                vec![PhaseId::from("dependency_graph")],
            );

            // Exactly the two seeded tasks the producer path starts with;
            // `total_tasks` = 2. `build` is declared (in phase_deps) but
            // has NO seeded items.
            let binaries: Vec<TaskInfo<TestId>> = vec![
                phased_binary("eval", "matrix_eval", 100),
                phased_binary("dep_graph", "dependency_graph", 100),
            ];

            // The build batch the consumer's on_phase_end spawns: one
            // common-dep with no prereqs (Pending head) + four variants
            // depending on it (Blocked, auto-resume on its completion).
            let build_items: Vec<TaskInfo<TestId>> = vec![
                phased_binary_dep("common_dep", "build", &[]),
                phased_binary_dep("variant_a", "build", &["common_dep"]),
                phased_binary_dep("variant_b", "build", &["common_dep"]),
                phased_binary_dep("variant_c", "build", &["common_dep"]),
                phased_binary_dep("variant_d", "build", &["common_dep"]),
            ];

            run_phase_ordering_scenario(
                binaries,
                phase_deps,
                // No slow markers — the reproducer is about the drain-edge
                // race, not in-flight overlap. With instant workers the
                // eval+dep_graph completions land back-to-back and the
                // counter exit is armed the instant dep_graph drains.
                vec![],
                Some(("dependency_graph".into(), build_items)),
                // 2 seeded + 5 spawned build tasks must ALL complete.
                /* expected_total_completed: */
                7,
            )
            .await;
        })
        .await;
}

/// Producer-path regression — LEAD 1 (cross-phase unresolvable dep).
/// Mirrors the asm-dataset-nix consumer's `build_variant` items, which
/// carry CROSS-phase `build_compilers_depends_on` toolchain edges. When
/// the toolchain (`build_compilers` phase) produced no seeded task for the
/// named id — e.g. a single-binary openssl run whose toolchain manifests
/// resolved to nothing — every variant's cross-phase dep is `UnknownDependency`
/// and is rejected at `validate_spawn_tasks`. Only the dep-free
/// `build_common_dep` survives. The dangerous consequence the consumer hit:
/// the build phase ends up with a degraded (partial OR empty) dispatch that
/// silently produces 0 outputs while the run exits rc=0.
///
/// This test pins the FIX's contract: a `spawn_tasks` batch that the
/// validator rejects WHOLESALE (every task carries an unresolvable
/// cross-phase dep) must NOT silently drain to a clean total=0 — the
/// loud-fail guard surfaces it. The clean (all-resolvable) sibling test
/// above proves the happy path still dispatches.
#[tokio::test(flavor = "current_thread")]
async fn producer_path_build_spawn_all_rejected_does_not_silently_complete() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut phase_deps: HashMap<PhaseId, Vec<PhaseId>> = HashMap::new();
            phase_deps.insert(
                PhaseId::from("dependency_graph"),
                vec![PhaseId::from("matrix_eval")],
            );
            phase_deps.insert(
                PhaseId::from("build"),
                vec![PhaseId::from("dependency_graph")],
            );

            let binaries: Vec<TaskInfo<TestId>> = vec![
                phased_binary("eval", "matrix_eval", 100),
                phased_binary("dep_graph", "dependency_graph", 100),
            ];

            // Every spawned build item names a CROSS-phase prereq in the
            // `build_compilers` phase that was NEVER seeded (no toolchain
            // tasks for this run). validate_spawn_tasks rejects all 5 as
            // UnknownDependency — the build phase plan is non-empty but
            // dispatches ZERO tasks.
            let build_items: Vec<TaskInfo<TestId>> = vec![
                phased_binary_xdep("common_dep", "build", &[("build_compilers", "missing_tc")]),
                phased_binary_xdep("variant_a", "build", &[("build_compilers", "missing_tc")]),
                phased_binary_xdep("variant_b", "build", &[("build_compilers", "missing_tc")]),
                phased_binary_xdep("variant_c", "build", &[("build_compilers", "missing_tc")]),
                phased_binary_xdep("variant_d", "build", &[("build_compilers", "missing_tc")]),
            ];

            run_producer_zero_dispatch_scenario(binaries, phase_deps, build_items).await;
        })
        .await;
}

/// Driver for the loud-fail contract: a build batch whose every task is
/// rejected by the validator (non-empty plan → zero dispatch). The run
/// MUST NOT exit `Ok` with a silent total=0 — `primary.run()` surfaces a
/// structured error. Built as a standalone driver (not
/// `run_phase_ordering_scenario`) because the expected terminal is a
/// loud failure, not a clean `completed == N`.
async fn run_producer_zero_dispatch_scenario(
    binaries: Vec<TaskInfo<TestId>>,
    phase_deps: HashMap<PhaseId, Vec<PhaseId>>,
    rejected_build_items: Vec<TaskInfo<TestId>>,
) {
    let max_res = dynrunner_core::ResourceMap::from([(
        dynrunner_core::ResourceKind::memory(),
        1024 * 1024 * 1024u64,
    )]);
    let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
    let mut outgoing = HashMap::new();
    let mut sec_handles = Vec::new();

    for i in 0..2u32 {
        let sec_id = format!("sec-{i}");
        let (pri_to_sec_tx, sec_to_pri_rx, handle) =
            spawn_real_secondary(sec_id.clone(), 1, max_res.clone());
        outgoing.insert(sec_id, pri_to_sec_tx);
        sec_handles.push(handle);

        let tx = incoming_tx.clone();
        tokio::task::spawn_local(async move {
            let mut rx = sec_to_pri_rx;
            while let Some(msg) = rx.recv().await {
                if tx.send(msg).is_err() {
                    break;
                }
            }
        });
    }
    drop(incoming_tx);

    let transport =
        ChannelPeerTransport::from_raw_channels("setup".into(), outgoing, incoming_rx);
    let config = PrimaryConfig {
        num_secondaries: 2,
        connect_timeout: Duration::from_secs(10),
        peer_timeout: Duration::from_secs(10),
        ..test_primary_config()
    };
    let (mut primary, _mesh) = build_test_primary(
        config,
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );

    let command_sender = primary.command_sender();

    let on_start: OnPhaseStart = Box::new(|_p: &PhaseId| {});
    let items = rejected_build_items.clone();
    let mut already_spawned = false;
    let on_end: OnPhaseEnd = Box::new(move |p: &PhaseId, _c: u32, _f: u32, _outputs| {
        if p.as_str() == "dependency_graph" && !already_spawned {
            already_spawned = true;
            let (reply_tx, _reply_rx) = tokio::sync::oneshot::channel();
            let _ = command_sender.try_send(PrimaryCommand::SpawnTasks {
                tasks: items.clone(),
                reply: reply_tx,
            });
        }
    });

    // Operational primary (mesh-always): seed the inherited ledger + run as
    // `PromotionSnapshot` (a `ColdStart` would relocate away, never running the
    // producer-path dispatch this test asserts).
    seed_operational_ledger(&mut primary, binaries, phase_deps);
    let result = primary
        .run(SeedSource::PromotionSnapshot, on_start, on_end)
        .await;

    drop(primary);
    for h in sec_handles {
        let _ = h.await;
    }

    // The loud-fail contract: a phase declared with a non-empty plan that
    // dispatches ZERO tasks must NOT exit clean. `run()` surfaces a
    // structured `Err` so the PyO3 boundary raises instead of returning a
    // silent rc=0.
    assert!(
        result.is_err(),
        "a build phase whose entire spawned plan was rejected (zero \
         dispatch) must surface a loud error, not a silent clean total=0; \
         got Ok"
    );
}

/// MODE-2 end-to-end: the consumer's EXACT premature-complete shape. A
/// `--source-already-staged` / relocated primary owes discovery (`Owed` +
/// phase graph, NO seeded tasks), runs `discover_on_promotion` itself, and
/// resolves a phase-1 (`probe`) corpus with ZERO to-run items (an empty
/// discovery for that phase). A dependent phase-2 (`build`) is declared but its
/// real work is injected only via `on_phase_end("probe")` — the lazy-spawn
/// consumer pattern (`FullPipelineTask.on_phase_end → primary_handle.spawn_tasks`).
///
/// The deleted `to_run == 0 → RunComplete` shortcut would have made
/// `discover_on_promotion` originate a STICKY `RunComplete` from this
/// single-phase view, finishing the run BEFORE the empty `probe` phase drained
/// through the cascade and injected `build` — the consumer-reproduced
/// "run complete: 0 succeeded" bug (the observer exits while the cascade injects
/// + runs the next phase in an already-"complete" state). POST-FIX the seam
/// originates no run-terminal: the empty `probe` phase stays Active, the pre-loop
/// cascade `drain_empty_active_phases` drains it and fires `on_phase_end("probe")`,
/// the consumer injects the `build` batch inline, and the run completes ONLY
/// once that injected work terminates.
///
/// Asserts: the run exits `Ok`, the injected `build` work all completes
/// (`completed_count == build_count`), and the
/// `on_phase_end("probe") → on_phase_start("build")` ordering holds (proof the
/// injection landed through the cascade, not a premature drain).
#[tokio::test(flavor = "current_thread")]
async fn mode2_zero_to_run_phase1_with_lazy_spawn_phase2_does_not_prematurely_complete() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Phase graph: `build` depends on `probe`. `probe` resolves to ZERO
            // to-run items via discovery; `build`'s real work is injected via
            // on_phase_end("probe").
            let mut phase_deps: HashMap<PhaseId, Vec<PhaseId>> = HashMap::new();
            phase_deps.insert(PhaseId::from("build"), vec![PhaseId::from("probe")]);

            // Discovery resolves the phase-1 `probe` corpus to ZERO items — the
            // empty-phase shape a phase-chaining consumer hits when phase-1 has
            // nothing of its own to run and exists only to gate phase-2 (whose
            // work is injected from on_phase_end). This is the corpus shape that
            // tripped the deleted `to_run == 0 → RunComplete` shortcut.
            let probe_items: Vec<(TaskInfo<TestId>, bool)> = Vec::new();
            // The phase-2 `build` batch the consumer injects from
            // on_phase_end("probe") — real to-run work that must dispatch +
            // complete before the run is allowed to finish.
            let build_items: Vec<TaskInfo<TestId>> = vec![
                phased_binary("build_x", "build", 50),
                phased_binary("build_y", "build", 50),
                phased_binary("build_z", "build", 50),
            ];
            let build_count = build_items.len();

            let max_res = dynrunner_core::ResourceMap::from([(
                dynrunner_core::ResourceKind::memory(),
                1024 * 1024 * 1024u64,
            )]);
            let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
            let mut outgoing = HashMap::new();
            let mut sec_handles = Vec::new();
            for i in 0..2u32 {
                let sec_id = format!("sec-{i}");
                let (pri_to_sec_tx, sec_to_pri_rx, handle) =
                    spawn_real_secondary(sec_id.clone(), 1, max_res.clone());
                outgoing.insert(sec_id, pri_to_sec_tx);
                sec_handles.push(handle);
                let tx = incoming_tx.clone();
                tokio::task::spawn_local(async move {
                    let mut rx = sec_to_pri_rx;
                    while let Some(msg) = rx.recv().await {
                        if tx.send(msg).is_err() {
                            break;
                        }
                    }
                });
            }
            drop(incoming_tx);

            let transport =
                ChannelPeerTransport::from_raw_channels("setup".into(), outgoing, incoming_rx);
            let config = PrimaryConfig {
                num_secondaries: 2,
                connect_timeout: Duration::from_secs(10),
                peer_timeout: Duration::from_secs(10),
                ..test_primary_config()
            };
            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // MODE-2 seed (the relocated/pre-staged inherited shape): declare
            // discovery debt + the phase graph, NO tasks — exactly what
            // `originate_relocated_seed` stages and a promotion snapshot carries.
            // The discovery policy resolves the all-skipped `probe` corpus when
            // `discover_on_promotion` fires (debt is `Owed`).
            {
                let cs = primary.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::PhaseDepsSet {
                    deps: phase_deps.clone(),
                });
                cs.apply(ClusterMutation::DiscoveryDebtDeclared);
            }
            let fires = std::rc::Rc::new(std::cell::Cell::new(0u32));
            primary.register_setup_discovery(fixed_discovery_marked(
                probe_items,
                phase_deps,
                fires.clone(),
            ));

            // Event log + the consumer's lazy-spawn on_phase_end (inject `build`
            // when `probe` ends), reusing the established command-channel pattern.
            let command_sender = primary.command_sender();
            let events: Arc<Mutex<Vec<PhaseEvent>>> = Arc::new(Mutex::new(Vec::new()));
            let starts_cb = events.clone();
            let on_start: OnPhaseStart = Box::new(move |p: &PhaseId| {
                starts_cb
                    .lock()
                    .unwrap()
                    .push(PhaseEvent::Start(p.to_string()));
            });
            let ends_cb = events.clone();
            let mut already_spawned = false;
            let on_end: OnPhaseEnd = Box::new(move |p: &PhaseId, c: u32, f: u32, _outputs| {
                ends_cb.lock().unwrap().push(PhaseEvent::End {
                    phase: p.to_string(),
                    completed: c,
                    failed: f,
                });
                if p.as_str() == "probe" && !already_spawned {
                    already_spawned = true;
                    let (reply_tx, _reply_rx) = tokio::sync::oneshot::channel();
                    let _ = command_sender.try_send(PrimaryCommand::SpawnTasks {
                        tasks: build_items.clone(),
                        reply: reply_tx,
                    });
                }
            });

            // Drive the FULL run as a `PromotionSnapshot` (⇒ PromotedDestination,
            // which runs `discover_on_promotion` because debt is `Owed`).
            let exit = tokio::time::timeout(
                Duration::from_secs(15),
                primary.run(SeedSource::PromotionSnapshot, on_start, on_end),
            )
            .await
            .expect(
                "the mode-2 run must complete promptly — a hang here means the \
                 zero-to-run phase-1 stranded the counter (the anti-hang failure \
                 mode) instead of finalizing through it",
            );
            exit.expect("the mode-2 run must exit Ok, not error");

            assert_eq!(fires.get(), 1, "discovery runs exactly once");
            let completed = primary.completed_count();
            let failed = primary.failed_count();
            let log = events.lock().unwrap().clone();
            drop(primary);
            for h in sec_handles {
                let _ = h.await;
            }

            // The injected phase-2 work all completed — the run did NOT
            // prematurely complete at the zero-to-run phase-1 discovery edge.
            assert_eq!(
                completed, build_count,
                "every injected `build` task must complete; the run must not have \
                 prematurely completed at the zero-to-run `probe` discovery edge. \
                 event log: {log:?}"
            );
            assert_eq!(failed, 0, "no failures expected; event log: {log:?}");
            // The injection landed through the cascade: on_phase_end("probe")
            // fired and on_phase_start("build") followed it.
            let probe_end = log
                .iter()
                .position(|e| matches!(e, PhaseEvent::End { phase, .. } if phase == "probe"));
            let build_start = log
                .iter()
                .position(|e| matches!(e, PhaseEvent::Start(p) if p == "build"));
            assert!(
                probe_end.is_some(),
                "on_phase_end(\"probe\") must fire (the empty phase-1 drains \
                 through the cascade); event log: {log:?}"
            );
            assert!(
                build_start.is_some(),
                "on_phase_start(\"build\") must fire (the injected phase-2 work \
                 activated); event log: {log:?}"
            );
            assert!(
                probe_end < build_start,
                "on_phase_start(\"build\") must follow on_phase_end(\"probe\") — the \
                 injected work activates only after the dependency phase ends; \
                 event log: {log:?}"
            );
        })
        .await;
}

/// #343 reproducer — MODE-2 end-to-end, the FRESH-SKIP twin of the
/// zero-to-run test above. Discovery resolves the phase-1 `probe` corpus to
/// items that are ALL marked `skipped_already_done` (the `--skip-existing`
/// "everything already on disk" shape a consumer hits on a re-run), and the
/// dependent phase-2 `build` work is injected only via
/// `on_phase_end("probe")` — the same lazy-spawn consumer pattern.
///
/// A freshly-discovered all-skipped phase has NEVER fired `on_phase_end`
/// anywhere (discover_on_promotion just seeded it terminal), so the phase
/// MUST flow through the live cascade exactly like the zero-to-run shape:
/// `on_phase_start("probe")` → `on_phase_end("probe", 0, 0)` (skips are
/// terminal LEDGER states, not completion EVENTS, so the event tallies are
/// zero) → the consumer hook injects `build` → the injected work runs.
///
/// PRE-FIX (the bug): `hydrate_from_cluster_state` saw `probe` as
/// "all-terminal with no live work" and seeded it `PhaseState::Done`
/// WITHOUT firing — the failover-INHERITED treatment (where the hook
/// already fired on the original primary, #326) misapplied to a phase
/// whose hook never ran. The injection was silently dropped, the empty
/// dependent `build` phase false-fired `on_phase_end("build", 0, 0)`, and
/// the run tripped the empty-drain fail-loud — event log
/// `[Start("build"), End("build", 0, 0)]` with NO probe events.
#[tokio::test(flavor = "current_thread")]
async fn mode2_all_skipped_phase1_fires_on_phase_end_once_and_lazy_spawn_lands() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Phase graph: `build` depends on `probe`. `probe`'s items are
            // ALL already done at discovery time; `build`'s real work is
            // injected via on_phase_end("probe").
            let mut phase_deps: HashMap<PhaseId, Vec<PhaseId>> = HashMap::new();
            phase_deps.insert(PhaseId::from("build"), vec![PhaseId::from("probe")]);

            // Discovery resolves phase-1 to TWO items, BOTH marked
            // `skipped_already_done` — the fresh all-skipped phase.
            let probe_items: Vec<(TaskInfo<TestId>, bool)> = vec![
                (phased_binary("probe_a", "probe", 50), true),
                (phased_binary("probe_b", "probe", 50), true),
            ];
            let build_items: Vec<TaskInfo<TestId>> = vec![
                phased_binary("build_x", "build", 50),
                phased_binary("build_y", "build", 50),
                phased_binary("build_z", "build", 50),
            ];
            let build_count = build_items.len();

            let max_res = dynrunner_core::ResourceMap::from([(
                dynrunner_core::ResourceKind::memory(),
                1024 * 1024 * 1024u64,
            )]);
            let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
            let mut outgoing = HashMap::new();
            let mut sec_handles = Vec::new();
            for i in 0..2u32 {
                let sec_id = format!("sec-{i}");
                let (pri_to_sec_tx, sec_to_pri_rx, handle) =
                    spawn_real_secondary(sec_id.clone(), 1, max_res.clone());
                outgoing.insert(sec_id, pri_to_sec_tx);
                sec_handles.push(handle);
                let tx = incoming_tx.clone();
                tokio::task::spawn_local(async move {
                    let mut rx = sec_to_pri_rx;
                    while let Some(msg) = rx.recv().await {
                        if tx.send(msg).is_err() {
                            break;
                        }
                    }
                });
            }
            drop(incoming_tx);

            let transport =
                ChannelPeerTransport::from_raw_channels("setup".into(), outgoing, incoming_rx);
            let config = PrimaryConfig {
                num_secondaries: 2,
                connect_timeout: Duration::from_secs(10),
                peer_timeout: Duration::from_secs(10),
                ..test_primary_config()
            };
            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // MODE-2 seed: declare discovery debt + the phase graph, NO tasks.
            {
                let cs = primary.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::PhaseDepsSet {
                    deps: phase_deps.clone(),
                });
                cs.apply(ClusterMutation::DiscoveryDebtDeclared);
            }
            let fires = std::rc::Rc::new(std::cell::Cell::new(0u32));
            primary.register_setup_discovery(fixed_discovery_marked(
                probe_items,
                phase_deps,
                fires.clone(),
            ));

            // Event log + the consumer's lazy-spawn on_phase_end.
            let command_sender = primary.command_sender();
            let events: Arc<Mutex<Vec<PhaseEvent>>> = Arc::new(Mutex::new(Vec::new()));
            let starts_cb = events.clone();
            let on_start: OnPhaseStart = Box::new(move |p: &PhaseId| {
                starts_cb
                    .lock()
                    .unwrap()
                    .push(PhaseEvent::Start(p.to_string()));
            });
            let ends_cb = events.clone();
            let mut already_spawned = false;
            let on_end: OnPhaseEnd = Box::new(move |p: &PhaseId, c: u32, f: u32, _outputs| {
                ends_cb.lock().unwrap().push(PhaseEvent::End {
                    phase: p.to_string(),
                    completed: c,
                    failed: f,
                });
                if p.as_str() == "probe" && !already_spawned {
                    already_spawned = true;
                    let (reply_tx, _reply_rx) = tokio::sync::oneshot::channel();
                    let _ = command_sender.try_send(PrimaryCommand::SpawnTasks {
                        tasks: build_items.clone(),
                        reply: reply_tx,
                    });
                }
            });

            let exit = tokio::time::timeout(
                Duration::from_secs(15),
                primary.run(SeedSource::PromotionSnapshot, on_start, on_end),
            )
            .await
            .expect("the all-skipped mode-2 run must complete promptly, not hang");
            exit.expect(
                "the all-skipped mode-2 run must exit Ok — an Err here is the \
                 #343 shape: on_phase_end(\"probe\") never fired, the injection \
                 was dropped, and the empty dependent phase tripped the \
                 empty-drain fail-loud",
            );

            assert_eq!(fires.get(), 1, "discovery runs exactly once");
            let completed = primary.completed_count();
            let failed = primary.failed_count();
            let log = events.lock().unwrap().clone();
            drop(primary);
            for h in sec_handles {
                let _ = h.await;
            }

            // The injected phase-2 work all completed (skips are accounted in
            // their own outcome class, not `succeeded`).
            assert_eq!(
                completed, build_count,
                "every injected `build` task must complete; event log: {log:?}"
            );
            assert_eq!(failed, 0, "no failures expected; event log: {log:?}");
            // The hook fired EXACTLY ONCE for the fresh all-skipped phase —
            // never (the #343 drop) and twice (a #326-shaped re-fire) are both
            // failures.
            let probe_ends = log
                .iter()
                .filter(|e| matches!(e, PhaseEvent::End { phase, .. } if phase == "probe"))
                .count();
            assert_eq!(
                probe_ends, 1,
                "on_phase_end(\"probe\") must fire exactly once for a freshly \
                 discovered all-skipped phase; event log: {log:?}"
            );
            // The consumer lifecycle contract holds: the phase STARTED before
            // it ended (a fresh skip phase never fired either hook anywhere,
            // so both edges belong to this primary's cascade).
            let probe_start = log
                .iter()
                .position(|e| matches!(e, PhaseEvent::Start(p) if p == "probe"));
            let probe_end = log
                .iter()
                .position(|e| matches!(e, PhaseEvent::End { phase, .. } if phase == "probe"));
            assert!(
                probe_start.is_some() && probe_start < probe_end,
                "on_phase_start(\"probe\") must precede on_phase_end(\"probe\"); \
                 event log: {log:?}"
            );
            // The injection landed through the cascade.
            let build_start = log
                .iter()
                .position(|e| matches!(e, PhaseEvent::Start(p) if p == "build"));
            assert!(
                build_start.is_some() && probe_end < build_start,
                "on_phase_start(\"build\") must follow on_phase_end(\"probe\") — \
                 the injected work activates only after the dependency phase \
                 ends; event log: {log:?}"
            );
        })
        .await;
}

/// Shared scenario driver. Builds the secondary cluster + primary,
/// runs the workload to completion, and asserts every
/// `on_phase_end(phase_id)` firing for a multi-item phase reports a
/// `completed` value equal to the total number of items that phase
/// ever had — i.e. the cascade waited for every in-flight item to
/// terminate before firing the boundary.
///
/// `lazy_spawn_next` carries `(trigger_phase, items)`: when set, the
/// scenario installs an on_phase_end hook that — the first time
/// `trigger_phase` ends — injects `items` via
/// `PrimaryCommand::SpawnTasks` (modelling the asm-tokenizer
/// consumer's lazy phase chaining).
async fn run_phase_ordering_scenario(
    binaries: Vec<TaskInfo<TestId>>,
    phase_deps: HashMap<PhaseId, Vec<PhaseId>>,
    slow_markers: Vec<(String, Duration)>,
    lazy_spawn_next: Option<(String, Vec<TaskInfo<TestId>>)>,
    expected_total_completed: usize,
) {
    // Stable string-keyed deps so the post-run ordering assertion
    // can look up "what does <starting_phase> depend on" without
    // re-introducing the PhaseId/&str dance.
    let deps_lookup: HashMap<String, Vec<String>> = phase_deps
        .iter()
        .map(|(k, v)| (k.to_string(), v.iter().map(|p| p.to_string()).collect()))
        .collect();
    // Compute per-phase totals BEFORE moving `binaries`. Used to
    // assert that every on_phase_end firing observes the full
    // terminal count for its phase.
    let mut phase_totals: HashMap<String, u32> = HashMap::new();
    for b in &binaries {
        *phase_totals.entry(b.phase_id.to_string()).or_insert(0) += 1;
    }
    if let Some((_, ref items)) = lazy_spawn_next {
        for b in items {
            *phase_totals.entry(b.phase_id.to_string()).or_insert(0) += 1;
        }
    }

    let max_res = dynrunner_core::ResourceMap::from([(
        dynrunner_core::ResourceKind::memory(),
        1024 * 1024 * 1024u64,
    )]);
    let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
    let mut outgoing = HashMap::new();
    let mut sec_handles = Vec::new();

    for i in 0..2u32 {
        let sec_id = format!("sec-{i}");
        let (pri_to_sec_tx, sec_to_pri_rx, handle) =
            spawn_real_secondary_slow(sec_id.clone(), 1, max_res.clone(), slow_markers.clone());
        outgoing.insert(sec_id, pri_to_sec_tx);
        sec_handles.push(handle);

        let tx = incoming_tx.clone();
        tokio::task::spawn_local(async move {
            let mut rx = sec_to_pri_rx;
            while let Some(msg) = rx.recv().await {
                if tx.send(msg).is_err() {
                    break;
                }
            }
        });
    }
    drop(incoming_tx);

    let transport =
        ChannelPeerTransport::from_raw_channels("setup".into(), outgoing, incoming_rx);
    let config = PrimaryConfig {
        num_secondaries: 2,
        connect_timeout: Duration::from_secs(10),
        peer_timeout: Duration::from_secs(10),
        ..test_primary_config()
    };
    let (mut primary, _mesh) = build_test_primary(
        config,
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );

    let command_sender = primary.command_sender();

    let events: Arc<Mutex<Vec<PhaseEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let starts_cb = events.clone();
    let on_start: OnPhaseStart = Box::new(move |p: &PhaseId| {
        starts_cb
            .lock()
            .unwrap()
            .push(PhaseEvent::Start(p.to_string()));
    });
    let ends_cb = events.clone();
    let lazy_spawn = lazy_spawn_next.clone();
    let mut already_spawned = false;
    let on_end: OnPhaseEnd = Box::new(move |p: &PhaseId, c: u32, f: u32, _outputs| {
        ends_cb.lock().unwrap().push(PhaseEvent::End {
            phase: p.to_string(),
            completed: c,
            failed: f,
        });
        // Lazy spawn (consumer pattern): if this phase end matches
        // the trigger, fire SpawnTasks via the in-runtime command
        // channel. `try_send` is non-blocking and the cascade's
        // post-callback drain (`process_phase_lifecycle`'s try_recv
        // loop) picks it up inline BEFORE the next
        // `drain_empty_active_phases` call.
        if let Some((ref trigger, ref items)) = lazy_spawn
            && p.as_str() == trigger
            && !already_spawned
        {
            already_spawned = true;
            let (reply_tx, _reply_rx) = tokio::sync::oneshot::channel();
            // `try_send` is non-blocking — required because the
            // callback runs synchronously inside the cascade and
            // the cascade's per-iteration drain step
            // (`process_phase_lifecycle`'s `command_rx.try_recv`
            // loop, see `coordinator.rs`) is what eventually picks
            // the command up. Bounded by `COMMAND_CHANNEL_CAPACITY`
            // (256); the test's single-shot use is well under it.
            let _ = command_sender.try_send(PrimaryCommand::SpawnTasks {
                tasks: items.clone(),
                reply: reply_tx,
            });
        }
    });

    // Operational primary (mesh-always): seed the inherited ledger + run as
    // `PromotionSnapshot` (a `ColdStart` would relocate away).
    seed_operational_ledger(&mut primary, binaries, phase_deps);
    primary
        .run(SeedSource::PromotionSnapshot, on_start, on_end)
        .await
        .unwrap();

    let completed = primary.completed_count();
    let failed = primary.failed_count();

    drop(primary);
    for h in sec_handles {
        let _ = h.await;
    }

    assert_eq!(
        completed, expected_total_completed,
        "all expected items must complete"
    );
    assert_eq!(failed, 0, "no failures expected");

    let log = events.lock().unwrap().clone();

    // Assertion 1: every on_phase_end(phase) firing for a phase
    // that had >=1 item must observe `completed == phase_total`.
    // A premature firing surfaces as `completed < phase_total`
    // (the cascade fired while in-flight items remained). Phases
    // that had no items legitimately fire with `completed=0`
    // (the `drain_empty_active_phases` startup helper).
    for event in &log {
        if let PhaseEvent::End {
            phase,
            completed,
            failed,
        } = event
            && let Some(total) = phase_totals.get(phase)
            && *total > 0
        {
            assert_eq!(
                *completed, *total,
                "on_phase_end({phase}) observed completed={completed} \
                 but the phase had {total} item(s). A `completed<total` \
                 firing indicates a premature cascade fired while \
                 in-flight items had not yet terminated. event log: \
                 {log:?}"
            );
            assert_eq!(
                *failed, 0,
                "on_phase_end({phase}).failed={failed}; event log: \
                 {log:?}"
            );
        }
    }

    // Assertion 2: for every phase with a strict dependency,
    // on_phase_start(dependent) MUST NOT appear before
    // on_phase_end(dep) in the cascade event log. The cascade
    // activates `dependent` only via `mark_phase_done(dep)` inside
    // the same `process_phase_lifecycle` tick that fires
    // `on_phase_end(dep)`; any ordering inversion proves `dep` was
    // wrongly drained before its in-flight items terminated.
    //
    // Derived from `phase_totals` keys with the
    // `lazy_spawn_next.0` trigger: for the lazy-spawn test the
    // dependent phase `next` only has items because the lazy
    // spawn injected them, and the assertion holds for both pre-
    // populated and lazy-populated dependents.
    for event in &log {
        if let PhaseEvent::Start(starting_phase) = event {
            let Some(deps) = deps_lookup.get(starting_phase) else {
                continue;
            };
            for dep_phase in deps {
                let start_idx = log
                    .iter()
                    .position(|e| matches!(e, PhaseEvent::Start(p) if p == starting_phase));
                let dep_end_idx = log
                    .iter()
                    .position(|e| matches!(e, PhaseEvent::End { phase, .. } if phase == dep_phase));
                assert!(
                    dep_end_idx.is_some(),
                    "on_phase_end({dep_phase}) must fire before \
                     on_phase_start({starting_phase}); event log: \
                     {log:?}"
                );
                assert!(
                    dep_end_idx < start_idx,
                    "on_phase_start({starting_phase}) appeared before \
                     on_phase_end({dep_phase}) in the cascade event \
                     log. event log: {log:?}"
                );
            }
        }
    }
}

/// Connect-before-first-phase-start ordering + empty-initial-phase
/// cascade + consumer lazy-spawn, end-to-end. Proves the operator-facing
/// important-event setup narration is correctly ordered AFTER moving
/// the initial `fire_initial_phase_starts` (+ its dependent empty-phase
/// cascade) to run AFTER `wait_for_connections`:
///
///   * The "all secondaries connected" milestone (emitted from
///     `wait_for_connections`) MUST precede the FIRST "starting job
///     phase" milestone (emitted from `fire_initial_phase_starts`).
///     Pre-reorder, the initial phase-start fired BEFORE connect, so the
///     operator saw "starting job phase" before "all secondaries
///     connected" — the inversion this reorder fixes.
///   * "initial assignment complete" (the phase-preparation /
///     task-spawning important event) and "initial setup done" (the
///     steady-state milestone) both appear, "initial setup done" last,
///     and "initial setup done" appears EXACTLY ONCE on the submitter's
///     process.
///
/// The workload deliberately leads with an EMPTY phase (`pre`, zero
/// items) whose `Blocked` dependent (`work`) holds every real task —
/// this exercises `drain_empty_active_phases` + the lifecycle cascade
/// (the block relocated past `wait_for_connections`). Phase `work` has
/// a slow item so it stays in-flight across the assignment window, and
/// phase `post` is populated lazily via `PrimaryCommand::SpawnTasks`
/// from inside `on_phase_end(work)` — the asm-tokenizer consumer
/// pattern. The phase-event ordering invariant
/// (on_phase_start(dependent) never precedes on_phase_end(dep)) is
/// re-asserted to prove the cascade behaviour is identical with the
/// call moved.
#[tokio::test(flavor = "current_thread")]
async fn connected_event_precedes_first_phase_start_with_empty_phase_and_lazy_spawn() {
    use crate::test_capture::{ImportantCapture, important_only};
    use tracing_subscriber::Layer;
    use tracing_subscriber::Registry;
    use tracing_subscriber::layer::SubscriberExt;

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Capture only the importance-target events, scoped to this
            // test's thread for the lifetime of the run. `set_default`
            // (not `with_default`) so the subscriber is held across the
            // `.await` — and `current_thread` + `LocalSet` keep every
            // spawned secondary task on this thread, so the primary's
            // important emits (fired from inside `primary.run().await`)
            // are all reached.
            let capture = ImportantCapture::default();
            let subscriber =
                Registry::default().with(capture.clone().with_filter(important_only()));
            let _guard = tracing::subscriber::set_default(subscriber);

            // Phase chain: pre (EMPTY) → work (all real items, one slow)
            //   plus a lazily-spawned `post` injected from on_phase_end(work).
            let mut phase_deps: HashMap<PhaseId, Vec<PhaseId>> = HashMap::new();
            phase_deps.insert(PhaseId::from("work"), vec![PhaseId::from("pre")]);
            phase_deps.insert(PhaseId::from("post"), vec![PhaseId::from("work")]);

            // `pre` has NO binaries — it is the empty initial phase whose
            // cascade must drain it Done so `work` (Blocked on `pre`)
            // becomes visible to `view_for_worker`.
            let binaries: Vec<TaskInfo<TestId>> = vec![
                phased_binary("work_fast", "work", 100),
                phased_binary("work_slow", "work", 50),
            ];
            let post_items = vec![phased_binary("post_one", "post", 50)];

            let max_res = dynrunner_core::ResourceMap::from([(
                dynrunner_core::ResourceKind::memory(),
                1024 * 1024 * 1024u64,
            )]);
            let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
            let mut outgoing = HashMap::new();
            let mut sec_handles = Vec::new();

            for i in 0..2u32 {
                let sec_id = format!("sec-{i}");
                let (pri_to_sec_tx, sec_to_pri_rx, handle) = spawn_real_secondary_slow(
                    sec_id.clone(),
                    1,
                    max_res.clone(),
                    vec![("/tmp/work_slow".to_string(), Duration::from_millis(300))],
                );
                outgoing.insert(sec_id, pri_to_sec_tx);
                sec_handles.push(handle);

                let tx = incoming_tx.clone();
                tokio::task::spawn_local(async move {
                    let mut rx = sec_to_pri_rx;
                    while let Some(msg) = rx.recv().await {
                        if tx.send(msg).is_err() {
                            break;
                        }
                    }
                });
            }
            drop(incoming_tx);

            let transport =
                ChannelPeerTransport::from_raw_channels("setup".into(), outgoing, incoming_rx);
            let config = PrimaryConfig {
                num_secondaries: 2,
                connect_timeout: Duration::from_secs(10),
                peer_timeout: Duration::from_secs(10),
                ..test_primary_config()
            };
            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let command_sender = primary.command_sender();

            // Interleaved phase-event log (start/end) so the cascade
            // ordering invariant is checked the same way the sibling
            // scenarios do.
            let events: Arc<Mutex<Vec<PhaseEvent>>> = Arc::new(Mutex::new(Vec::new()));
            let starts_cb = events.clone();
            let on_start: OnPhaseStart = Box::new(move |p: &PhaseId| {
                starts_cb
                    .lock()
                    .unwrap()
                    .push(PhaseEvent::Start(p.to_string()));
            });
            let ends_cb = events.clone();
            let mut already_spawned = false;
            let on_end: OnPhaseEnd = Box::new(move |p: &PhaseId, c: u32, f: u32, _outputs| {
                ends_cb.lock().unwrap().push(PhaseEvent::End {
                    phase: p.to_string(),
                    completed: c,
                    failed: f,
                });
                // Lazy spawn the `post` phase the first time `work` ends
                // (the asm-tokenizer consumer pattern). The cascade's
                // post-callback drain applies it inline.
                if p.as_str() == "work" && !already_spawned {
                    already_spawned = true;
                    let (reply_tx, _reply_rx) = tokio::sync::oneshot::channel();
                    let _ = command_sender.try_send(PrimaryCommand::SpawnTasks {
                        tasks: post_items.clone(),
                        reply: reply_tx,
                    });
                }
            });

            // Operational primary (mesh-always): seed the inherited ledger +
            // run as `PromotionSnapshot` (a `ColdStart` would relocate away).
            seed_operational_ledger(&mut primary, binaries, phase_deps);
            // `pre` is an INTENTIONALLY-EMPTY phase (a pure sequencing gate:
            // `work` depends on it but `pre` owns no work of its own). Under
            // the F-honesty contract a phase that drains genuinely empty
            // fails loud BY DEFAULT — the consumer declares the intentional
            // gate via `PhaseSpec.may_be_empty`. Seed that opt-out
            // (`PhaseMayBeEmptySet`) so `phase_can_proceed` advances `pre`
            // through the cascade instead of vetoing it. This pins the
            // empty-phase cascade-drain ordering AND the may_be_empty opt-out
            // end-to-end.
            primary
                .cluster_state_mut_for_test()
                .apply(ClusterMutation::PhaseMayBeEmptySet {
                    phases: vec![PhaseId::from("pre")],
                });
            primary
                .run(SeedSource::PromotionSnapshot, on_start, on_end)
                .await
                .unwrap();

            let completed = primary.completed_count();
            let failed = primary.failed_count();

            drop(primary);
            for h in sec_handles {
                let _ = h.await;
            }

            // All three real items (work_fast, work_slow, post_one) ran;
            // the empty `pre` phase contributed nothing.
            assert_eq!(completed, 3, "all real items must complete");
            assert_eq!(failed, 0, "no failures expected");

            // -------- Phase-event (consumer-callback) ordering --------
            // on_phase_start(work) must precede on_phase_end(pre)'s
            // dependent activation, and on_phase_start(post) must follow
            // on_phase_end(work). This is the cascade behaviour the
            // reorder must preserve.
            let log = events.lock().unwrap().clone();
            let pos = |pred: &dyn Fn(&PhaseEvent) -> bool| log.iter().position(pred);
            let pre_end = pos(&|e| matches!(e, PhaseEvent::End { phase, .. } if phase == "pre"));
            let work_start = pos(&|e| matches!(e, PhaseEvent::Start(p) if p == "work"));
            let work_end = pos(&|e| matches!(e, PhaseEvent::End { phase, .. } if phase == "work"));
            let post_start = pos(&|e| matches!(e, PhaseEvent::Start(p) if p == "post"));
            assert!(
                pre_end.is_some() && work_start.is_some(),
                "empty `pre` must fire on_phase_end and `work` must start; log={log:?}"
            );
            assert!(
                pre_end < work_start,
                "on_phase_start(work) must follow on_phase_end(pre); log={log:?}"
            );
            assert!(
                work_end.is_some() && post_start.is_some() && work_end < post_start,
                "on_phase_start(post) must follow on_phase_end(work) (lazy spawn); log={log:?}"
            );

            // -------- Important-event (operator narration) ordering --------
            let msgs = capture.messages();
            let first_idx = |needle: &str| msgs.iter().position(|m| m.contains(needle));
            let connected = first_idx("all secondaries connected");
            let starting_phase = first_idx("starting job phase");
            let assignment = first_idx("initial assignment complete");
            let setup_done = first_idx("initial setup done");

            assert!(
                connected.is_some(),
                "expected an 'all secondaries connected' important event; got {msgs:?}"
            );
            assert!(
                starting_phase.is_some(),
                "expected a 'starting job phase' important event; got {msgs:?}"
            );
            // Connect before the first phase-start — the regression this
            // reorder fixes.
            assert!(
                connected < starting_phase,
                "'all secondaries connected' must precede the first \
                 'starting job phase'; got {msgs:?}"
            );
            // The count-bearing initial-assignment event is present, before
            // the steady-state milestone.
            assert!(
                assignment.is_some(),
                "expected an 'initial assignment complete' important event \
                 (phase-preparation / task-spawning); got {msgs:?}"
            );
            // "initial setup done" present, EXACTLY ONCE, and last (the
            // steady-state milestone).
            assert!(
                setup_done.is_some(),
                "expected an 'initial setup done' important event; got {msgs:?}"
            );
            assert_eq!(
                msgs.iter()
                    .filter(|m| m.contains("initial setup done"))
                    .count(),
                1,
                "'initial setup done' must be emitted exactly once on the \
                 submitter's process; got {msgs:?}"
            );
            assert!(
                assignment < setup_done && connected < setup_done && starting_phase < setup_done,
                "'initial setup done' must come after connect, assignment, and \
                 the first phase-start; got {msgs:?}"
            );
        })
        .await;
}
