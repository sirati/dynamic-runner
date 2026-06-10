//! Post-Ready first-bind orphan repro (round-2).
//!
//! A worker slot takes its FIRST task of a new type: the dispatch arm
//! hits `EnsureWorkerOutcome::RespawnInProgress` and stashes the binary
//! in `pending_first_bind`. The fresh subprocess reports `Ready`; the
//! Ready arm pops the stash, `assign_task` succeeds, and records the
//! task in `active_tasks` ("pending first-bind assigned post-Ready").
//! THEN the SAME (current-generation) worker emits a real `Disconnected`
//! carrying the nix-build-failure shape (`Response::Error{NonRecoverable,
//! "nix build returned non-zero"}` → `PollResult::Disconnected` →
//! `WorkerEvent::Disconnected{ result: TaskResult::error(NonRecoverable,
//! ..), binary: Some(..) }`).
//!
//! Production wedge (asm-dataset run_20260610_031245, on the generation-
//! fix build): the post-Ready-bound task is NEVER reported terminal —
//! the phase barrier never releases. This module replays that exact
//! sequence and asserts the task IS reported terminal to the primary.

#![cfg(test)]

use super::super::test_helpers::{
    FakeWorkerFactory, channel_mesh_no_primary, make_secondary_channel, make_secondary_recording,
};
use super::super::*;
use super::processing::make_binary;
use dynrunner_core::{ErrorType, TaskResult};
use dynrunner_manager_local::WorkerEvent;
use dynrunner_manager_local::oom::{OomWatcher, OomWatcherConfig};
use dynrunner_protocol_primary_secondary::{DistributedBinaryInfo, DistributedMessage, PeerTransport};
use std::time::Duration;

/// Disabled OOM watcher (flat layout, no workers cgroup) — the repro
/// never exercises the kernel-OOM reclassifier, so `kernel_oom_recent`
/// always reads false and the NonRecoverable disconnect classification
/// is untouched.
fn test_oom_watcher() -> OomWatcher {
    OomWatcher::new_with_workers_cgroup(
        OomWatcherConfig {
            sample_interval: Duration::from_millis(50),
            decision_interval: Duration::from_millis(100),
            heartbeat_interval: Duration::from_secs(60),
            log_enabled: false,
        },
        None,
    )
}

/// Single-worker production-shaped config (mirrors generation_gate.rs).
fn one_worker_config(secondary_id: &str) -> SecondaryConfig {
    SecondaryConfig {
        secondary_id: secondary_id.into(),
        num_workers: 1,
        max_resources: dynrunner_core::ResourceMap::from([(
            dynrunner_core::ResourceKind::memory(),
            1024 * 1024 * 1024,
        )]),
        hostname: "test-host".into(),
        keepalive_interval: Duration::from_secs(60),
        src_network: None,
        src_tmp: None,
        peer_timeout: Duration::from_secs(120),
        keepalive_miss_threshold: 3,
        retry_max_passes: 1,
        oom_retry_max_passes: 1,
        primary_link_failure_threshold: 5,
        primary_link_failure_window: Duration::from_secs(30),
        primary_silence_backstop: Duration::from_secs(120),
        unconfigured_deadline: Duration::from_secs(600),
        can_be_primary: false,
        resource_check_interval: Duration::from_millis(100),
        log_oom_watcher: false,
        promoted_primary_quiesce_grace: Duration::from_millis(100),
        unfulfillable_reinject_max_per_task: None,
        mem_manager_reserved_bytes: None,
        output_dir: None,
        memuse_log_path: None,
        forwarded_argv: Vec::new(),
    }
}

/// Build a wire `TaskAssignment` for `binary`, keyed by `file_hash`,
/// targeting `worker_id` on `sec_id`. The `sender_id` is the (bootstrap)
/// primary so the inbound-message liveness pre-amble is satisfied.
fn task_assignment(
    sender_id: &str,
    sec_id: &str,
    worker_id: u32,
    binary: &dynrunner_core::TaskInfo<test_helpers::TestId>,
    file_hash: &str,
) -> DistributedMessage<test_helpers::TestId> {
    DistributedMessage::TaskAssignment {
        target: None,
        sender_id: sender_id.into(),
        timestamp: 0.0,
        secondary_id: sec_id.into(),
        worker_id,
        zip_file: None,
        binary_info: DistributedBinaryInfo::from_task_info(binary),
        local_path: binary.path.to_string_lossy().into_owned(),
        file_hash: file_hash.into(),
        predecessor_outputs: std::collections::BTreeMap::new(),
    }
}

/// THE repro. First-bind via the REAL dispatch path (`handle_inbound`
/// → `dispatch_message` → `RespawnInProgress` → `pending_first_bind`),
/// then pump the fresh subprocess's `Ready` off the pool channel so the
/// Ready arm assigns the deferred task, then inject the same-generation
/// nix-build-failure `Disconnected`. The task MUST be reported terminal.
#[tokio::test(flavor = "current_thread")]
async fn post_ready_first_bind_disconnect_is_reported_terminal() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // The recorder's synthetic membership names the primary
            // "setup" (see RecordingPeer::connected_ids); the bootstrap
            // primary id must match so `Destination::Primary` routes and
            // the CLASS-1 reports land in the recorded log rather than
            // being absorbed by the no-route failover-health probe.
            let (mut secondary, log) = make_secondary_recording(one_worker_config("sec-2"), 1);
            secondary.set_bootstrap_primary_id("setup".to_string());

            let mut factory = FakeWorkerFactory;
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;

            // The slot is fresh: loaded_type_id == None. The first
            // TaskAssignment is therefore a FIRST-BIND — the dispatch
            // arm hits RespawnInProgress and stashes the binary in
            // pending_first_bind (NOT active_tasks).
            let binary = make_binary("nix-build-task", 50);
            let file_hash = "c48ccbf6".to_string();
            let assignment = task_assignment("setup", "sec-2", 0, &binary, &file_hash);

            secondary
                .handle_inbound(assignment, &mut FakeWorkerFactory)
                .await;

            // Stash landed; nothing bound to active_tasks yet.
            assert!(
                secondary.op_mut().pending_first_bind.contains_key(&0),
                "first-bind must stash the binary in pending_first_bind"
            );
            assert!(
                !secondary.op_mut().active_tasks.contains_key(&file_hash),
                "first-bind must NOT bind active_tasks before Ready"
            );

            // Pump the fresh subprocess's Ready off the pool channel.
            // The RespawnInProgress path spawned a wait_ready background
            // task that wrote a Ready{worker_id:0, generation:1}.
            let oom = test_oom_watcher();
            let ready = secondary
                .op_mut()
                .pool
                .recv_event()
                .await
                .expect("fresh subprocess must emit a Ready event");
            assert!(
                matches!(ready, WorkerEvent::Ready { worker_id: 0, .. }),
                "expected Ready for worker 0; got {ready:?}"
            );
            secondary.handle_worker_event(ready, &oom).await.unwrap();

            // Ready arm popped the stash and assigned: active_tasks now
            // holds the task ("pending first-bind assigned post-Ready").
            assert!(
                secondary.op_mut().active_tasks.contains_key(&file_hash),
                "Ready arm must bind the deferred task into active_tasks"
            );

            // Now the SAME (current-generation) worker emits the real
            // nix-build-failure Disconnected. Production shape: a worker
            // Response::Error{NonRecoverable, "nix build returned
            // non-zero"} resolves to PollResult::Disconnected (needs
            // restart), surfacing as WorkerEvent::Disconnected with
            // result.error_type = NonRecoverable and binary = Some(..).
            let current_gen = secondary.op_mut().pool.workers[0].generation;
            secondary
                .handle_worker_event(
                    WorkerEvent::Disconnected {
                        worker_id: 0,
                        generation: current_gen,
                        result: TaskResult::error(
                            ErrorType::NonRecoverable,
                            "nix build returned non-zero".to_string(),
                        ),
                        binary: Some(binary.clone()),
                    },
                    &oom,
                )
                .await
                .unwrap();
            secondary.drain_egress().await;

            // The task MUST be reported terminal to the primary AND the
            // secondary's own bookkeeping must show it resolved (not
            // orphaned in active_tasks).
            assert!(
                !secondary.op_mut().active_tasks.contains_key(&file_hash),
                "the disconnect must clear the post-Ready-bound task from active_tasks"
            );
            let reported = log.borrow();
            assert!(
                reported.iter().any(|m| matches!(
                    m,
                    DistributedMessage::TaskFailed { task_hash, .. } if *task_hash == file_hash
                )),
                "the post-Ready first-bind task's disconnect MUST be reported \
                 terminal (TaskFailed) for the hash; got {reported:?}"
            );
        })
        .await;
}

/// THE w1-no-disconnect orphan (the KEY round-2 fact: w1 had NO
/// disconnect and was equally orphaned). A task is stashed in
/// `pending_first_bind` by a first-bind `RespawnInProgress`. Before the
/// fresh subprocess's `Ready` is consumed, the SAME slot is replaced
/// AGAIN (a second respawn — the restart loop, an OOM-restart, an
/// assignment-failure respawn, or a second first-bind dispatch all go
/// through `replace_worker_slot`, which bumps the slot generation). The
/// generation gate now DROPS the first watcher's `Ready` (it carries the
/// stale generation). The Ready arm never runs, so `pending_first_bind`
/// is never popped, the task is never assigned, AND it is never reported
/// terminal — it sits in the stash forever with no event ever touching
/// it. Nothing resolves it: the phase barrier wedges with no disconnect.
///
/// `pending_first_bind` is keyed by `WorkerId` and was touched on
/// exactly two ARM edges: the `Ready` arm (pop+assign) and the
/// `Disconnected` arm (pop+report). The round-2 fix adds the THIRD,
/// generic edge: every slot-REPLACEMENT funnels through
/// `sweep_replaced_worker_task` → `reinject_pending_first_bind`, so a
/// replacement that bumps the generation drains the stash into the
/// backpressure reinject path BEFORE the stale-dropped `Ready` can
/// strand it. This test drives the secondary's restart-loop replacement
/// edge (`kill + sweep_replaced_worker_task`, the production
/// `process_tasks` sequence — the raw `pool.restart_worker` only models
/// the POOL half, the generation bump) and asserts the stash is
/// recovered, not orphaned.
#[tokio::test(flavor = "current_thread")]
async fn pending_first_bind_stranded_when_ready_is_stale_dropped() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut secondary, log) = make_secondary_recording(one_worker_config("sec-2"), 1);
            secondary.set_bootstrap_primary_id("setup".to_string());

            let mut factory = FakeWorkerFactory;
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;

            // First-bind dispatch: stash in pending_first_bind (gen 0 → 1).
            let binary = make_binary("w1-task", 50);
            let file_hash = "9ec1e342".to_string();
            let assignment = task_assignment("setup", "sec-2", 0, &binary, &file_hash);
            secondary
                .handle_inbound(assignment, &mut FakeWorkerFactory)
                .await;
            assert!(
                secondary.op_mut().pending_first_bind.contains_key(&0),
                "first-bind must stash the binary in pending_first_bind"
            );
            let gen_after_stash = secondary.op_mut().pool.workers[0].generation;

            // Drain the gen-1 Ready the first wait-ready watcher wrote,
            // but DO NOT feed it to the handler yet — model the race where
            // a second slot-replacement bumps the generation before the
            // Ready is consumed.
            let stale_ready = secondary
                .op_mut()
                .pool
                .recv_event()
                .await
                .expect("first wait-ready watcher must emit a Ready");
            assert_eq!(stale_ready.generation(), gen_after_stash);

            // Second slot replacement via the secondary's restart-loop
            // edge. Production `process_tasks` does, in order:
            // `kill_subprocess` → `sweep_replaced_worker_task(wid)` →
            // `restart_worker_async(wid)` (process_tasks.rs:601-616). Here
            // the raw `pool.restart_worker` stands in for the kill+respawn
            // (the POOL half — it bumps the generation), and the
            // `sweep_replaced_worker_task` call is the SECONDARY half that
            // owns recovery: it drains the deferred stash into the
            // backpressure reinject path before the stale Ready can strand
            // it. ORDER matches production: the sweep runs against the
            // bumped slot.
            secondary
                .pool_mut()
                .restart_worker(0, &mut factory, false)
                .await
                .unwrap();
            let bumped_gen = secondary.op_mut().pool.workers[0].generation;
            assert!(
                bumped_gen > gen_after_stash,
                "the second replacement must bump the slot generation"
            );
            secondary.sweep_replaced_worker_task(0).await.unwrap();

            // The stale (gen-1) Ready now lands. The generation gate drops
            // it — so the Ready arm never pops the stash.
            let oom = test_oom_watcher();
            secondary
                .handle_worker_event(stale_ready, &oom)
                .await
                .unwrap();
            secondary.drain_egress().await;

            // RECOVERED: the replacement edge's sweep must have resolved
            // the deferred stash. SOMETHING must resolve it — either the
            // stash is cleared (swept into the reinject path) OR a terminal
            // was reported for the hash. Pre-fix NEITHER happened (the
            // stranding wedge); the round-2 fix makes the sweep drain it.
            let still_stashed = secondary.op_mut().pending_first_bind.contains_key(&0);
            let reported = log.borrow();
            let any_terminal_for_hash = reported.iter().any(|m| {
                matches!(
                    m,
                    DistributedMessage::TaskFailed { task_hash, .. }
                        | DistributedMessage::TaskComplete { task_hash, .. }
                        if *task_hash == file_hash
                )
            });
            assert!(
                !still_stashed || any_terminal_for_hash,
                "a stale-dropped Ready must not strand the deferred first-bind \
                 task forever: it must be either swept out of pending_first_bind \
                 or reported terminal. still_stashed={still_stashed}, \
                 reported={reported:?}"
            );
        })
        .await;
}

/// Router replacement-edge drain + no-self-cannibalization ordering.
///
/// The type-shift router edge sweeps the slot's PRIOR `pending_first_bind`
/// stash (router.rs: `sweep_replaced_worker_task` at the
/// `RespawnInProgress` arm) BEFORE installing the FRESH stash one line
/// later. This pins both halves: the prior stash is popped + reported as
/// backpressure, and the fresh stash survives (the sweep must not
/// cannibalize the just-installed entry — order is sweep-then-insert).
#[tokio::test(flavor = "current_thread")]
async fn router_first_bind_sweeps_prior_stash_and_keeps_fresh() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut secondary, log) = make_secondary_recording(one_worker_config("sec-2"), 1);
            secondary.set_bootstrap_primary_id("setup".to_string());

            let mut factory = FakeWorkerFactory;
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;

            // Seed a PRIOR first-bind stash for worker 0 — the entry a
            // previous dispatch left when it hit RespawnInProgress and the
            // slot has since been replaced (a stash the next replacement
            // must not strand).
            let prior_binary = make_binary("prior-task", 50);
            let prior_hash = "prior01".to_string();
            secondary.op_mut().pending_first_bind.insert(
                0,
                super::super::PendingFirstBind {
                    binary: prior_binary,
                    file_hash: prior_hash.clone(),
                    estimated: dynrunner_core::ResourceMap::new(),
                    predecessor_outputs: std::collections::BTreeMap::new(),
                },
            );

            // A first-bind dispatch on the fresh slot hits RespawnInProgress
            // (loaded_type_id == None), so the router edge runs:
            // `sweep_replaced_worker_task(0)` THEN `pending_first_bind.insert`
            // for the fresh task.
            let fresh_binary = make_binary("fresh-task", 50);
            let fresh_hash = "fresh01".to_string();
            let assignment = task_assignment("setup", "sec-2", 0, &fresh_binary, &fresh_hash);
            secondary
                .handle_inbound(assignment, &mut FakeWorkerFactory)
                .await;
            secondary.drain_egress().await;

            // The FRESH stash survives (sweep ran before the insert — no
            // self-cannibalization).
            let stash = secondary.op_mut().pending_first_bind.get(&0).cloned();
            assert!(
                matches!(&stash, Some(p) if p.file_hash == fresh_hash),
                "the fresh first-bind stash must survive the replacement-edge \
                 sweep; got {:?}",
                stash.as_ref().map(|p| &p.file_hash)
            );

            // The PRIOR stash was popped AND reported as backpressure-shaped
            // TaskFailed (the reinject contract).
            let reported = log.borrow();
            assert!(
                reported.iter().any(|m| matches!(
                    m,
                    DistributedMessage::TaskFailed {
                        task_hash,
                        error_type: dynrunner_core::ErrorType::Recoverable,
                        ..
                    } if *task_hash == prior_hash
                )),
                "the prior first-bind stash must be reported terminal \
                 (backpressure TaskFailed) by the replacement-edge sweep; \
                 got {reported:?}"
            );
        })
        .await;
}

// ──────────────────────────────────────────────────────────────────────
// ROUND-3: post-Ready-ASSIGNED + tail-churn replacement interleavings.
//
// The round-2 wedge stranded a task that was STILL IN `pending_first_bind`
// (stash never drained). Round 3 targets a DIFFERENT shape the production
// recurrence (consumer nano run_20260610_064528 on 405f21b9) exhibited:
// the first-bind WAS assigned post-Ready (the "pending first-bind assigned
// post-Ready" INFO logged — so `active_tasks[H]=wid` was set and the slot
// was BUSY), and THEN "tail churn replaced their workers". This is no
// longer a stash strand: it is a task in `active_tasks` whose worker is
// replaced. Every replacement edge that can hit a BUSY worker must sweep
// `active_tasks[H]` into the reinject path so the replaced generation
// cannot strand it. These tests drive the post-Ready-assigned state and
// then each replacement edge, asserting the assigned task IS resolved
// (cleared + reported terminal), not orphaned.
// ──────────────────────────────────────────────────────────────────────

/// Drive a single-worker secondary to the post-Ready-ASSIGNED state: a
/// first-bind dispatch stashes the binary, the fresh subprocess's `Ready`
/// is consumed (the Ready arm pops the stash and `assign_task` succeeds),
/// so `active_tasks[file_hash] = 0` and the slot is BUSY (Transitioning).
/// Returns the live slot generation at the post-assign point so the caller
/// can stamp a same-generation terminal. This is the EXACT state the
/// production INFO "pending first-bind assigned post-Ready" marks.
async fn drive_to_post_ready_assigned<P: PeerTransport<test_helpers::TestId>>(
    secondary: &mut super::super::test_helpers::SecondaryHarness<P>,
    oom: &OomWatcher,
    binary: &dynrunner_core::TaskInfo<test_helpers::TestId>,
    file_hash: &str,
) -> u64 {
    let assignment = task_assignment("setup", "sec-2", 0, binary, file_hash);
    secondary
        .handle_inbound(assignment, &mut FakeWorkerFactory)
        .await;
    assert!(
        secondary.op_mut().pending_first_bind.contains_key(&0),
        "first-bind must stash the binary in pending_first_bind"
    );

    let ready = secondary
        .op_mut()
        .pool
        .recv_event()
        .await
        .expect("fresh subprocess must emit a Ready event");
    assert!(
        matches!(ready, WorkerEvent::Ready { worker_id: 0, .. }),
        "expected Ready for worker 0; got {ready:?}"
    );
    secondary.handle_worker_event(ready, oom).await.unwrap();

    assert!(
        secondary.op_mut().active_tasks.contains_key(file_hash),
        "Ready arm must bind the deferred task into active_tasks \
         (the 'pending first-bind assigned post-Ready' state)"
    );
    assert!(
        !secondary.op_mut().pool.workers[0].is_idle_state(),
        "a post-Ready-assigned slot is BUSY (Transitioning), not idle"
    );
    secondary.op_mut().pool.workers[0].generation
}

/// Assert the hash was reported terminal (TaskFailed of any error_type) to
/// the primary at least once. Backpressure (Recoverable) and at-fault
/// (NonRecoverable / ResourceExhausted) both count — the wedge is a
/// MISSING terminal, so any terminal resolves the phase barrier.
fn assert_reported_terminal(
    log: &std::rc::Rc<std::cell::RefCell<Vec<DistributedMessage<test_helpers::TestId>>>>,
    file_hash: &str,
) {
    let reported = log.borrow();
    assert!(
        reported.iter().any(|m| matches!(
            m,
            DistributedMessage::TaskFailed { task_hash, .. }
                | DistributedMessage::TaskComplete { task_hash, .. }
            if *task_hash == file_hash
        )),
        "the post-Ready-assigned task MUST be reported terminal for the \
         hash so the phase barrier releases; got {reported:?}"
    );
}

/// (i) RESTART-LOOP replacement of a post-Ready-ASSIGNED worker.
///
/// The post-Ready-assigned task lives in `active_tasks`; the slot is then
/// replaced by the restart-loop edge — the production `process_tasks`
/// sequence `kill_subprocess` → `sweep_replaced_worker_task(wid)` →
/// `restart_worker_async(wid)` (process_tasks.rs:601-616). The middle call
/// is the SECONDARY-half recovery: it must pop `active_tasks[H]` and report
/// it as backpressure before the restart bumps the generation. This is the
/// "tail churn replaced their workers" edge for a flagged
/// `pending_worker_restarts` slot.
#[tokio::test(flavor = "current_thread")]
async fn post_ready_assigned_restart_loop_replacement_is_reported_terminal() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut secondary, log) = make_secondary_recording(one_worker_config("sec-2"), 1);
            secondary.set_bootstrap_primary_id("setup".to_string());

            let mut factory = FakeWorkerFactory;
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;

            let oom = test_oom_watcher();
            let binary = make_binary("clang20-O0-ltothin", 50);
            let file_hash = "c48ccbf6".to_string();
            let assigned_gen =
                drive_to_post_ready_assigned(&mut secondary, &oom, &binary, &file_hash).await;

            // Restart-loop edge, production sequence. kill → SWEEP → respawn.
            secondary.op_mut().pool.workers[0].kill_subprocess();
            secondary.sweep_replaced_worker_task(0).await.unwrap();
            secondary
                .pool_mut()
                .restart_worker_async(0, &mut factory, false)
                .await
                .unwrap();
            secondary.drain_egress().await;

            let bumped_gen = secondary.op_mut().pool.workers[0].generation;
            assert!(
                bumped_gen > assigned_gen,
                "the restart must bump the slot generation"
            );
            assert!(
                !secondary.op_mut().active_tasks.contains_key(&file_hash),
                "the restart-loop sweep must clear the post-Ready-assigned task \
                 from active_tasks (else it is stranded by the new generation)"
            );
            assert_reported_terminal(&log, &file_hash);
        })
        .await;
}

/// (ii) BINARY-LESS pipe-EOF Disconnect of a post-Ready-ASSIGNED worker.
///
/// The companion round-2 test covers a NonRecoverable-WITH-binary
/// disconnect (the nix-build-failure shape). This covers the OTHER
/// disconnect shape hypothesis B calls out: the just-Ready worker dies
/// BEFORE it can run the assigned task — a pure transport EOF synthesised
/// as `Recoverable + "transport disconnected"` with `binary: None` (the
/// protocol layer's pre-pickup synthesis at state.rs / worker.rs). The
/// Disconnected arm's `active_tasks` scan must still find + report the
/// assigned task regardless of the absent `binary`.
#[tokio::test(flavor = "current_thread")]
async fn post_ready_assigned_binaryless_pipe_eof_disconnect_is_reported_terminal() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut secondary, log) = make_secondary_recording(one_worker_config("sec-2"), 1);
            secondary.set_bootstrap_primary_id("setup".to_string());

            let mut factory = FakeWorkerFactory;
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;

            let oom = test_oom_watcher();
            let binary = make_binary("clang21-O0-ltothin", 50);
            let file_hash = "9ec1e342".to_string();
            let assigned_gen =
                drive_to_post_ready_assigned(&mut secondary, &oom, &binary, &file_hash).await;

            // Pure transport EOF before the worker picked up the task:
            // Recoverable + no binary. Same (current) generation, so the
            // gen-gate passes it through to the Disconnected arm.
            secondary
                .handle_worker_event(
                    WorkerEvent::Disconnected {
                        worker_id: 0,
                        generation: assigned_gen,
                        result: TaskResult::error(
                            ErrorType::Recoverable,
                            "transport disconnected".to_string(),
                        ),
                        binary: None,
                    },
                    &oom,
                )
                .await
                .unwrap();
            secondary.drain_egress().await;

            assert!(
                !secondary.op_mut().active_tasks.contains_key(&file_hash),
                "the binary-less disconnect must clear the post-Ready-assigned \
                 task from active_tasks"
            );
            assert_reported_terminal(&log, &file_hash);
        })
        .await;
}

/// (iii) A SECOND TaskAssignment arrives while the worker is BUSY with a
/// post-Ready-assigned task.
///
/// "The tail churn then hits that worker with ANOTHER type-shift dispatch."
/// The router selects ONLY idle workers (router.rs:162-173 `is_idle_state`
/// filter). A post-Ready-assigned slot is Transitioning (BUSY), and this
/// fixture has a single worker, so the new dispatch finds NO idle target
/// and reports the NEW task as backpressure — it must NOT touch / cannibalize
/// the ORIGINAL active task. This pins that a router dispatch can never
/// strand a busy worker's in-flight task: the original stays tracked (and a
/// later real terminal resolves it), and only the new task is bounced.
#[tokio::test(flavor = "current_thread")]
async fn second_assignment_while_busy_does_not_strand_original_task() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut secondary, log) = make_secondary_recording(one_worker_config("sec-2"), 1);
            secondary.set_bootstrap_primary_id("setup".to_string());

            let mut factory = FakeWorkerFactory;
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;

            let oom = test_oom_watcher();
            let orig_binary = make_binary("clang20-O0-ltothin", 50);
            let orig_hash = "c48ccbf6".to_string();
            let assigned_gen =
                drive_to_post_ready_assigned(&mut secondary, &oom, &orig_binary, &orig_hash).await;

            // A SECOND TaskAssignment for the same worker_id 0 arrives while
            // the slot is BUSY. No idle worker exists, so the router bounces
            // the NEW task and leaves the ORIGINAL active task untouched.
            let new_binary = make_binary("clang21-O0-ltothin", 50);
            let new_hash = "deadbeef".to_string();
            let assignment2 = task_assignment("setup", "sec-2", 0, &new_binary, &new_hash);
            secondary
                .handle_inbound(assignment2, &mut FakeWorkerFactory)
                .await;
            secondary.drain_egress().await;

            // The ORIGINAL task is NOT cannibalized: still tracked, slot
            // generation unchanged (no replacement happened).
            assert_eq!(
                secondary.op_mut().active_tasks.get(&orig_hash),
                Some(&0u32),
                "the busy worker's original active task must survive a second \
                 assignment that found no idle slot"
            );
            assert_eq!(
                secondary.op_mut().pool.workers[0].generation,
                assigned_gen,
                "a no-idle-target dispatch must not replace the busy slot"
            );

            // The NEW task is reported back as backpressure (Recoverable),
            // keyed by the NEW hash — never the original's.
            let reported = log.borrow();
            assert!(
                reported.iter().any(|m| matches!(
                    m,
                    DistributedMessage::TaskFailed {
                        task_hash,
                        error_type: dynrunner_core::ErrorType::Recoverable,
                        ..
                    } if *task_hash == new_hash
                )),
                "the second (un-takeable) assignment must be reported as \
                 backpressure for its OWN hash; got {reported:?}"
            );
            assert!(
                !reported.iter().any(|m| matches!(
                    m,
                    DistributedMessage::TaskFailed { task_hash, .. }
                    if *task_hash == orig_hash
                )),
                "the original task must NOT be reported terminal by the second \
                 assignment (it is still running); got {reported:?}"
            );
        })
        .await;
}

/// (iv) DOUBLE-REPLACEMENT: the sweep resolves the post-Ready-assigned
/// task, THEN the OLD subprocess's buffered terminal arrives stale.
///
/// The dangerous "tail churn" shape: a post-Ready-assigned task lives in
/// `active_tasks` (gen G). The restart-loop sweep resolves it (clears
/// active_tasks + reports backpressure, bumps to gen G+1). THEN the
/// REPLACED generation's poll task — which `abort_poll_task` could not
/// retract — delivers a buffered terminal stamped gen G. The generation
/// gate (pool.rs `is_stale_event`) MUST drop it: processing it would
/// re-report the (already-swept) hash a SECOND time (double-terminal at
/// the primary → over-count / wrong outcome class) OR, worse, mis-attribute
/// it to whatever task the fresh gen-G+1 subprocess was since bound to.
/// This is the post-Ready-ASSIGNED analogue of the gen-gate's stale-Ready
/// case; it pins exactly-one terminal across the replacement.
#[tokio::test(flavor = "current_thread")]
async fn post_ready_assigned_swept_then_stale_terminal_is_dropped() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut secondary, log) = make_secondary_recording(one_worker_config("sec-2"), 1);
            secondary.set_bootstrap_primary_id("setup".to_string());

            let mut factory = FakeWorkerFactory;
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;

            let oom = test_oom_watcher();
            let binary = make_binary("clang20-O0-ltothin", 50);
            let file_hash = "c48ccbf6".to_string();
            let assigned_gen =
                drive_to_post_ready_assigned(&mut secondary, &oom, &binary, &file_hash).await;

            // Restart-loop replacement resolves the assigned task and bumps
            // the generation (G → G+1).
            secondary.op_mut().pool.workers[0].kill_subprocess();
            secondary.sweep_replaced_worker_task(0).await.unwrap();
            secondary
                .pool_mut()
                .restart_worker_async(0, &mut factory, false)
                .await
                .unwrap();
            secondary.drain_egress().await;
            let bumped_gen = secondary.op_mut().pool.workers[0].generation;
            assert!(bumped_gen > assigned_gen);

            // Exactly ONE terminal for the hash after the sweep.
            let terminals_after_sweep = log
                .borrow()
                .iter()
                .filter(|m| matches!(
                    m,
                    DistributedMessage::TaskFailed { task_hash, .. }
                        | DistributedMessage::TaskComplete { task_hash, .. }
                    if *task_hash == file_hash
                ))
                .count();
            assert_eq!(
                terminals_after_sweep, 1,
                "the sweep must report the assigned task terminal EXACTLY once"
            );

            // The REPLACED generation's buffered terminal arrives stale
            // (stamped the OLD generation). It must be gen-gated out: no
            // bookkeeping change, no second report, no panic.
            secondary
                .handle_worker_event(
                    WorkerEvent::TaskCompleted {
                        worker_id: 0,
                        generation: assigned_gen,
                        result: TaskResult::ok(),
                        result_data: None,
                        binary: Some(binary.clone()),
                        estimated_resources: dynrunner_core::ResourceMap::new(),
                    },
                    &oom,
                )
                .await
                .unwrap();
            secondary.drain_egress().await;

            let terminals_after_stale = log
                .borrow()
                .iter()
                .filter(|m| matches!(
                    m,
                    DistributedMessage::TaskFailed { task_hash, .. }
                        | DistributedMessage::TaskComplete { task_hash, .. }
                    if *task_hash == file_hash
                ))
                .count();
            assert_eq!(
                terminals_after_stale, 1,
                "the stale-generation buffered terminal must be DROPPED — the \
                 hash must still have exactly one terminal, not two"
            );
        })
        .await;
}

/// (v) NO-ROUTE ABSORB: the swept terminal is GENUINELY LOST when the
/// primary is unreachable at report time.
///
/// `send_to_primary` ABSORBS a no-route `Err` into `Ok(())` (resource.rs
/// ~197-235): a no-route is a failover SIGNAL, not a run-fatal error, so the
/// secondary must not abort the run — but the comment is explicit that "the
/// absorbed terminal is genuinely LOST, NOT recovered", and a SURVIVING
/// holder's dropped terminal strands the task at the (new/old) primary's
/// in-flight ledger ("the buffered-terminal-replay ... is the proper fix
/// (owner-deferred)").
///
/// This test pins that documented gap as it applies to the round-2 SWEEP:
/// the sweep clears `active_tasks[H]` FIRST (resource.rs:318) then calls
/// `report_deferred_task_lost` → `send_to_primary`. With the primary
/// unrouteable the report is swallowed (returns Ok), so the task is gone
/// LOCALLY with NO terminal on the wire — the secondary believes it
/// reported, the primary keeps the slot in-flight forever. The phase
/// barrier wedges exactly as the production "in-flight froze" symptom
/// describes.
///
/// HONEST TAG: this reproduces a real, code-acknowledged strand, but it is
/// CONDITIONAL on a no-route (primary failover / a mesh-membership blip at
/// report time). Whether the production nano run hit a no-route window is
/// what the consumer's secondary-2 log slice must adjudicate (a
/// "no route to primary" WARN, or a primary changeover near 04:49:17).
#[tokio::test(flavor = "current_thread")]
async fn swept_terminal_is_absorbed_and_lost_when_primary_unreachable() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // `channel_mesh_no_primary` registers `peer-0` but NO "primary"
            // route; with bootstrap_primary_id "primary" the egress edge
            // resolves Destination::Primary → Peer("primary"), finds no
            // member, and surfaces the no-route Err that send_to_primary
            // absorbs.
            let transport = channel_mesh_no_primary("sec-2", 1);
            let mut secondary = make_secondary_channel(one_worker_config("sec-2"), transport);
            secondary.set_bootstrap_primary_id("primary".to_string());

            let mut factory = FakeWorkerFactory;
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;

            let oom = test_oom_watcher();
            let binary = make_binary("clang20-O0-ltothin", 50);
            let file_hash = "c48ccbf6".to_string();
            drive_to_post_ready_assigned(&mut secondary, &oom, &binary, &file_hash).await;

            // Restart-loop sweep while the primary is unrouteable. The sweep
            // clears active_tasks[H] then tries to report — the report is
            // ABSORBED (no-route → Ok), so the call SUCCEEDS despite nothing
            // reaching the wire.
            secondary.op_mut().pool.workers[0].kill_subprocess();
            let sweep_result = secondary.sweep_replaced_worker_task(0).await;

            assert!(
                sweep_result.is_ok(),
                "the sweep absorbs the no-route into Ok (a no-route is a \
                 failover signal, not a run-fatal error); got {sweep_result:?}"
            );
            assert!(
                !secondary.op_mut().active_tasks.contains_key(&file_hash),
                "the sweep cleared active_tasks LOCALLY — yet the terminal was \
                 absorbed (lost). The task is now untracked here AND unreported \
                 to the primary: the documented surviving-holder strand."
            );
        })
        .await;
}
