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
//! `note_item_completed` AFTER `phase_completed.entry(phase) += 1` runs
//! for the just-completed task (see `coordinator.rs` —
//! `note_item_completed` increments then calls
//! `process_phase_lifecycle`). So when `on_phase_end` fires for a
//! phase, the `completed` argument is the cumulative count INCLUDING
//! the task whose `handle_task_complete` tick triggered the cascade.
//! A premature firing surfaces as `completed < phase_total`.

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
#[ignore = "drives a real secondary against the primary over a channel uplink (spawn_real_secondary_slow); \
            post-uplink deletion needs the channel-backed mesh harness — channel-mesh-fold leaf"]
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
#[ignore = "drives a real secondary against the primary over a channel uplink (spawn_real_secondary_slow); \
            post-uplink deletion needs the channel-backed mesh harness — channel-mesh-fold leaf"]
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
#[ignore = "drives a real secondary against the primary over a channel uplink (spawn_real_secondary_slow); \
            post-uplink deletion needs the channel-backed mesh harness — channel-mesh-fold leaf"]
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
        ChannelPeerTransport::from_raw_channels("primary".into(), outgoing, incoming_rx);
    let config = PrimaryConfig {
        node_id: "primary".into(),
        num_secondaries: 2,
        connect_timeout: Duration::from_secs(10),
        peer_timeout: Duration::from_secs(10),
        keepalive_interval: Duration::from_secs(5),
        keepalive_miss_threshold: 3,
        source_pre_staged_root: None,
        uses_file_based_items: true,
        required_setup_on_promote: false,
        max_concurrent_per_type: std::collections::HashMap::new(),
        retry_max_passes: 1,
        oom_retry_max_passes: 1,
        fleet_dead_timeout: Duration::from_secs(30),
        mesh_ready_timeout: Duration::from_secs(5),
        mass_death_grace: Duration::ZERO,
        mass_death_min_count: 2,
        source_dir: None,
        unfulfillable_reinject_max_per_task: None,
        setup_promote_deadline: Duration::from_secs(600),
    };
    let mut primary = PrimaryCoordinator::new(
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
    let on_end: OnPhaseEnd = Box::new(move |p: &PhaseId, c: u32, f: u32| {
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

    primary
        .run(binaries, phase_deps, on_start, on_end)
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
