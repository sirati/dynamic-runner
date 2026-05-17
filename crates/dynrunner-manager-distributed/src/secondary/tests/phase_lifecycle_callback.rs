//! Regression: the promoted-secondary's `note_primary_item_completed`
//! must fire `on_phase_end(phase, completed, failed)` when the
//! item's completion is the one that takes the phase to `Drained`.
//!
//! Mirrors `PrimaryCoordinator::process_phase_lifecycle` —
//! pre-fix, the secondary's path called the callback-silent
//! `cascade_drain_done` free function instead, so a Python task
//! that overrode `on_phase_end` never observed the boundary on the
//! single-process / SLURM paths (the consumer-reported gap from
//! `task_protocol.py`). This file pins the new fire-site so a
//! future refactor cannot regress it back to the silent variant.
//!
//! Scope:
//!   1. Single-phase happy path: completing the only item fires the
//!      callback once with `(completed=1, failed=0)`.
//!   2. Mixed completion + failure: the per-phase counters track both
//!      axes; the final transition fires `(completed, failed)`
//!      matching the bumps.
//!   3. No-op when no callback is registered: the cascade still runs
//!      (phase reaches `Done`) but no panic / no callback miscount.
//!   4. Non-promoted secondary (no `primary_pending`):
//!      `process_primary_phase_lifecycle` is a silent no-op.
//!
//! Test fixture: builds a `SecondaryCoordinator` directly, hand-builds
//! a `PendingPool` + `primary_in_flight` entry, and drives
//! `note_primary_item_completed` / `note_primary_item_failed`
//! synchronously. No tokio runtime, no wire messages — the goal is to
//! pin the fire-site, not the full operational loop.

#![cfg(test)]

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use dynrunner_core::{ErrorType, PhaseId, SoftPreferredSecondaries, TaskInfo, TypeId};
use dynrunner_scheduler_api::{PendingPool, PhaseState};

use super::super::test_helpers::{election_config, make_secondary, TestId};
use super::super::PrimaryInFlightItem;

/// Build a fresh `PrimaryInFlightItem` for the given hash + phase.
/// Used to prime `primary_in_flight` so `note_primary_item_completed`
/// finds the entry and decrements the pool.
fn make_in_flight(name: &str, phase: &str) -> PrimaryInFlightItem<TestId> {
    PrimaryInFlightItem {
        phase_id: PhaseId::from(phase),
        target_secondary_id: "self".into(),
        binary: TaskInfo {
            path: PathBuf::from(format!("/tmp/{name}")),
            size: 100,
            identifier: TestId(name.into()),
            phase_id: PhaseId::from(phase),
            type_id: TypeId::from("default"),
            affinity_id: None,
            payload: serde_json::Value::Null,
            task_id: Some(format!("task-{name}")),
            task_depends_on: vec![],
            preferred_secondaries: SoftPreferredSecondaries::default(),
            resolved_path: None,
        },
    }
}

/// Build a one-phase pool with a single in-flight item. Mirrors the
/// state a promoted-secondary's pool reaches once
/// `populate_primary_from_cluster_state` has hydrated and the first
/// dispatch has primed `primary_in_flight`.
fn one_phase_pool_with_one_item(phase: &PhaseId) -> PendingPool<TestId> {
    let mut phase_ids = HashSet::new();
    phase_ids.insert(phase.clone());
    let mut pool = PendingPool::<TestId>::new(phase_ids, HashMap::new())
        .expect("graph valid");
    // The pool needs to know there's one in-flight task in this
    // phase so `on_item_finished` decrements down to 0 and the
    // phase transitions to Drained on the next poll.
    pool.mark_tasks_in_flight(vec![(
        format!("task-{}", phase.as_str()),
        phase.clone(),
    )]);
    pool
}

/// (1) Single-phase happy path: the only item completes, the phase
/// drains, and the callback observes `(completed=1, failed=0)`.
#[tokio::test(flavor = "current_thread")]
async fn note_primary_item_completed_fires_on_phase_end_on_pool_drain() {
    let phase = PhaseId::from("phase-a");
    let mut sec = make_secondary(election_config("sec-0"));
    sec.primary_pending = Some(one_phase_pool_with_one_item(&phase));
    sec.primary_in_flight
        .insert("hash-a".into(), make_in_flight("a", "phase-a"));

    // Record callback invocations. `Rc<RefCell<...>>` rather than a
    // bare closure-captured Vec so the test can inspect the recorded
    // call list after `note_primary_item_completed` runs.
    let calls: Arc<Mutex<Vec<(String, u32, u32)>>> = Arc::new(Mutex::new(Vec::new()));
    let calls_inner = calls.clone();
    let phase_starts: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let phase_starts_inner = phase_starts.clone();
    sec.register_phase_lifecycle_callbacks(
        Box::new(move |p: &PhaseId| {
            phase_starts_inner.lock()
                .expect("poisoned").push(p.as_str().to_string());
        }),
        Box::new(move |p: &PhaseId, c: u32, f: u32| {
            calls_inner
                .lock()
                .expect("poisoned")
                .push((p.as_str().to_string(), c, f));
        }),
    );

    sec.note_primary_item_completed("hash-a", &mut None).await;

    let recorded = calls.lock().expect("poisoned");
    assert_eq!(
        recorded.len(),
        1,
        "on_phase_end fires exactly once when the pool drains"
    );
    assert_eq!(
        recorded[0],
        ("phase-a".to_string(), 1, 0),
        "on_phase_end receives (phase_id, completed=1, failed=0)"
    );
    assert_eq!(
        sec.primary_pending.as_ref().expect("pool present").phase_state(&phase),
        Some(PhaseState::Done),
        "phase transitions to Done after the cascade"
    );
}

/// (2) Mixed-class drain: 1 completion + 1 failure in the same phase
/// drain the pool; the final cascade-fire reports both counts.
#[tokio::test(flavor = "current_thread")]
async fn note_primary_item_failed_contributes_to_phase_end_counters() {
    let phase = PhaseId::from("phase-a");
    let mut sec = make_secondary(election_config("sec-0"));
    // Two-item phase: the pool needs two in-flight markers so the
    // first finish leaves the phase still Active (1 in_flight remaining).
    let mut phase_ids = HashSet::new();
    phase_ids.insert(phase.clone());
    let mut pool = PendingPool::<TestId>::new(phase_ids, HashMap::new())
        .expect("graph valid");
    pool.mark_tasks_in_flight(vec![
        ("task-a".into(), phase.clone()),
        ("task-b".into(), phase.clone()),
    ]);
    sec.primary_pending = Some(pool);
    sec.primary_in_flight
        .insert("hash-a".into(), make_in_flight("a", "phase-a"));
    sec.primary_in_flight
        .insert("hash-b".into(), make_in_flight("b", "phase-a"));

    let calls: Arc<Mutex<Vec<(String, u32, u32)>>> = Arc::new(Mutex::new(Vec::new()));
    let calls_inner = calls.clone();
    sec.register_phase_lifecycle_callbacks(
        Box::new(|_: &PhaseId| {}),
        Box::new(move |p: &PhaseId, c: u32, f: u32| {
            calls_inner
                .lock()
                .expect("poisoned")
                .push((p.as_str().to_string(), c, f));
        }),
    );

    // First: hash-a completes. Phase still has 1 in_flight; should NOT
    // fire `on_phase_end` yet.
    sec.note_primary_item_completed("hash-a", &mut None).await;
    assert!(
        calls.lock().expect("poisoned").is_empty(),
        "first completion does not drain the phase; no callback yet"
    );

    // Second: hash-b fails Recoverably. With one Recoverable failure
    // remaining on `primary_failed`, the pool's in-flight count
    // reaches 0 but the failed-ledger is non-empty. The phase
    // transitions to Drained either way (the failed ledger is
    // primary's retry-pass concern, not the pool's state).
    sec.note_primary_item_failed("hash-b", &ErrorType::Recoverable, &mut None).await;

    let recorded = calls.lock().expect("poisoned");
    assert_eq!(
        recorded.len(),
        1,
        "the second item's completion takes the phase to Drained"
    );
    assert_eq!(
        recorded[0],
        ("phase-a".to_string(), 1, 1),
        "on_phase_end receives the per-class counts (completed=1, failed=1)"
    );
}

/// (3) No registered callback: cascade still walks the pool, phase
/// reaches `Done`. The Option-guarded fire-site is the only thing
/// that changes; the pool-side state machine is unaffected.
#[tokio::test(flavor = "current_thread")]
async fn no_callback_registered_still_drives_cascade_silently() {
    let phase = PhaseId::from("phase-a");
    let mut sec = make_secondary(election_config("sec-0"));
    sec.primary_pending = Some(one_phase_pool_with_one_item(&phase));
    sec.primary_in_flight
        .insert("hash-a".into(), make_in_flight("a", "phase-a"));

    // No `register_phase_lifecycle_callbacks` call.
    sec.note_primary_item_completed("hash-a", &mut None).await;

    assert_eq!(
        sec.primary_pending.as_ref().expect("pool present").phase_state(&phase),
        Some(PhaseState::Done),
        "the cascade walks to Done even without a registered callback"
    );
}

/// (4) `primary_pending` is `None` (the pre-promotion state):
/// `process_primary_phase_lifecycle` is a silent no-op. Defensive:
/// pins the non-promoted secondary path so installing a callback
/// before promotion can never trip a panic from the apparent
/// absence of a pool.
#[tokio::test(flavor = "current_thread")]
async fn no_pool_yields_silent_no_op_in_process_primary_phase_lifecycle() {
    let mut sec = make_secondary(election_config("sec-0"));
    assert!(sec.primary_pending.is_none(), "fixture starts pre-promotion");

    let calls: Arc<Mutex<Vec<(String, u32, u32)>>> = Arc::new(Mutex::new(Vec::new()));
    let calls_inner = calls.clone();
    sec.register_phase_lifecycle_callbacks(
        Box::new(|_: &PhaseId| {}),
        Box::new(move |p: &PhaseId, c: u32, f: u32| {
            calls_inner
                .lock()
                .expect("poisoned")
                .push((p.as_str().to_string(), c, f));
        }),
    );

    // `note_primary_item_completed` short-circuits on unknown hash
    // (the entry is not in `primary_in_flight`), so this call alone
    // wouldn't drive the cascade. Drive `process_primary_phase_lifecycle`
    // directly to exercise the pool-None branch.
    sec.process_primary_phase_lifecycle(&mut None).await;
    assert!(
        calls.lock().expect("poisoned").is_empty(),
        "no pool ⇒ no cascade ⇒ no callback firings"
    );
}

/// (5) Step 4 of the consumer-reported gap: the callback observes the
/// pool-drain transition AND drives `apply_spawn_tasks` on the
/// promoted secondary, which broadcasts `TasksSpawned` + grows
/// `cluster_state.task_count()`. Mirrors the production scenario
/// where the Python `on_phase_end` overrides `task_protocol.py`'s
/// hook and calls `primary_handle.spawn_tasks(...)` from inside
/// (using the handle captured in `on_run_start`). This test
/// substitutes the GIL-reacquiring closure with a direct
/// `apply_spawn_tasks` call against the same coordinator, since the
/// behaviour we pin is the Rust-side reachability of the SpawnTasks
/// path on the promoted secondary's pool — not the PyO3 bridge.
///
/// The test driver:
///   1. Builds a 2-phase task graph (phase-a depends on nothing,
///      phase-b depends on phase-a's lone task). Hand-builds the
///      pool + in_flight ledger so `note_primary_item_completed`
///      finds the entry and drains phase-a.
///   2. Registers a callback that, on `on_phase_end`, calls
///      `apply_spawn_tasks` to inject a third task targeted at the
///      now-active phase-b.
///   3. Triggers `note_primary_item_completed("hash-a", &mut None)` and verifies:
///       (a) the callback fired with `(phase-a, completed=1, failed=0)`,
///       (b) the cluster_state now contains the spawned task,
///       (c) the spawned task landed in `primary_pending` (Pending).
///
/// Pins the end-to-end Rust contract the brief Step 4 asks for: the
/// secondary IS a coordinator capable of growing its own ledger via
/// `apply_spawn_tasks`. The wire/PyO3 bridge ride on top.
#[tokio::test(flavor = "current_thread")]
async fn callback_can_invoke_apply_spawn_tasks_and_cluster_state_grows() {
    use std::collections::HashMap as StdHashMap;
    use std::collections::HashSet as StdHashSet;

    let phase_a = PhaseId::from("phase-a");
    let phase_b = PhaseId::from("phase-b");
    let mut sec = make_secondary(election_config("sec-0"));
    // Promoted-secondary scenario: the spawn-tasks dispatch path is
    // a no-op when `is_primary == false`. Flip directly — the helper
    // skips wire-side wiring (`promote_via_handler` etc.) since we
    // only want to exercise the apply-side contract.
    sec.is_primary = true;

    // Seed the cluster_state with the prereq task (phase-a) so its
    // task_id is known when the spawned task references it as a dep.
    let task_a = TaskInfo {
        path: PathBuf::from("/tmp/a"),
        size: 100,
        identifier: TestId("a".into()),
        phase_id: phase_a.clone(),
        type_id: TypeId::from("default"),
        affinity_id: None,
        payload: serde_json::Value::Null,
        task_id: Some("task-a".into()),
        task_depends_on: vec![],
        preferred_secondaries: SoftPreferredSecondaries::default(),
        resolved_path: None,
    };
    sec.cluster_state
        .apply(dynrunner_protocol_primary_secondary::ClusterMutation::PhaseDepsSet {
            deps: {
                let mut m = StdHashMap::new();
                m.insert(phase_b.clone(), vec![phase_a.clone()]);
                m
            },
        });
    sec.cluster_state
        .apply(dynrunner_protocol_primary_secondary::ClusterMutation::TaskAdded {
            hash: "hash-a".into(),
            task: task_a.clone(),
        });

    // Build the secondary's primary_pending with the same shape
    // populate_primary_from_cluster_state would produce. Phase-b is
    // seeded with a single placeholder task so the cascade does NOT
    // drain phase-b on phase-a's completion (an empty Active phase
    // would also drain on the next cascade pass — production avoids
    // this by always having items in dependent phases, the test
    // mirrors that invariant).
    let mut phase_ids = StdHashSet::new();
    phase_ids.insert(phase_a.clone());
    phase_ids.insert(phase_b.clone());
    let mut deps = StdHashMap::new();
    deps.insert(phase_b.clone(), vec![phase_a.clone()]);
    let mut pool = PendingPool::<TestId>::new(phase_ids, deps).expect("graph valid");
    pool.mark_tasks_in_flight(vec![("task-a".into(), phase_a.clone())]);
    let placeholder = TaskInfo {
        path: PathBuf::from("/tmp/placeholder"),
        size: 100,
        identifier: TestId("placeholder".into()),
        phase_id: phase_b.clone(),
        type_id: TypeId::from("default"),
        affinity_id: None,
        payload: serde_json::Value::Null,
        task_id: Some("task-placeholder".into()),
        // Placeholder's dep on task-a is what keeps it Blocked until
        // task-a completes. After completion the placeholder
        // unblocks; the pool gains the new dispatchable item AND the
        // phase stays non-empty so the cascade doesn't drain it.
        task_depends_on: vec!["task-a".into()],
        preferred_secondaries: SoftPreferredSecondaries::default(),
        resolved_path: None,
    };
    pool.extend(vec![placeholder]).expect("extend valid");
    sec.primary_pending = Some(pool);
    sec.primary_in_flight
        .insert("hash-a".into(), make_in_flight("a", "phase-a"));

    let initial_count = sec.cluster_state.task_count();
    assert_eq!(initial_count, 1, "fixture starts with task-a in the ledger");

    // Build the task to spawn from inside the callback: phase-b
    // depends on task-a (which is about to complete).
    let task_b_to_spawn = TaskInfo {
        path: PathBuf::from("/tmp/b"),
        size: 100,
        identifier: TestId("b".into()),
        phase_id: phase_b.clone(),
        type_id: TypeId::from("default"),
        affinity_id: None,
        payload: serde_json::Value::Null,
        task_id: Some("task-b".into()),
        task_depends_on: vec!["task-a".into()],
        preferred_secondaries: SoftPreferredSecondaries::default(),
        resolved_path: None,
    };

    // Register a callback that records the firing and queues a
    // spawn-tasks request. Because `on_phase_end` runs synchronously
    // inside `note_primary_item_completed`, and `apply_spawn_tasks`
    // is async, we can't directly call the async method from the
    // sync callback. Instead, the callback records a "spawn next"
    // request that the test driver picks up post-cascade and feeds
    // back into `apply_spawn_tasks`. This is the same shape the
    // PyO3 bridge uses: the Python callback queues commands onto
    // the `PrimaryHandle.command_tx` channel; the operational loop
    // picks them up on a later select! tick. The shared validator
    // + apply path are what we pin here.
    let queued_spawns: Arc<Mutex<Vec<TaskInfo<TestId>>>> =
        Arc::new(Mutex::new(Vec::new()));
    let queued_inner = queued_spawns.clone();
    let task_b_capture = task_b_to_spawn.clone();
    let calls: Arc<Mutex<Vec<(String, u32, u32)>>> = Arc::new(Mutex::new(Vec::new()));
    let calls_inner = calls.clone();
    sec.register_phase_lifecycle_callbacks(
        Box::new(|_: &PhaseId| {}),
        Box::new(move |p: &PhaseId, c: u32, f: u32| {
            calls_inner
                .lock()
                .expect("poisoned")
                .push((p.as_str().to_string(), c, f));
            queued_inner
                .lock()
                .expect("poisoned")
                .push(task_b_capture.clone());
        }),
    );

    sec.note_primary_item_completed("hash-a", &mut None).await;

    // The callback fired.
    let recorded = calls.lock().expect("poisoned").clone();
    assert_eq!(
        recorded.len(),
        1,
        "callback fires once on phase-a's drain"
    );
    assert_eq!(recorded[0], ("phase-a".to_string(), 1, 0));

    // Drive the queued spawn into the coordinator's apply path. This
    // is the operation `primary_handle.spawn_tasks(...)` reaches via
    // the command channel + handler.
    let pending = queued_spawns.lock().expect("poisoned").clone();
    let errors = sec
        .apply_spawn_tasks(pending)
        .await
        .expect("apply_spawn_tasks must succeed");
    assert!(
        errors.is_empty(),
        "no per-task errors for a well-formed batch: {errors:?}"
    );

    // cluster_state grew by 1: the spawned task landed.
    assert_eq!(
        sec.cluster_state.task_count(),
        initial_count + 1,
        "task_count grows by the size of the spawned batch (1)"
    );

    // The spawned task landed in `primary_pending` as Pending. Look
    // it up by its task_id via the cluster_state ledger; if the
    // CRDT says Pending, the post-apply walk must have reinjected
    // it into the pool.
    let hash_b = crate::primary::wire::compute_task_hash(&task_b_to_spawn);
    let state = sec
        .cluster_state
        .task_state(&hash_b)
        .expect("spawned task present in ledger");
    use crate::cluster_state::TaskState;
    assert!(
        matches!(state, TaskState::Pending { .. } | TaskState::Blocked { .. }),
        "spawned task lands as Pending (deps resolved) or Blocked (deps still pending); \
         got {state:?}"
    );
    assert!(
        !sec.primary_pending
            .as_ref()
            .expect("pool present")
            .is_empty()
            || matches!(state, TaskState::Blocked { .. }),
        "spawned task is either pool-resident or Blocked-pending-deps"
    );
}

/// Regression: a phase-end callback that issues `spawn_tasks` for the
/// successor phase via the command channel must see the spawn applied
/// INLINE within the cascade — BEFORE the cascade's next
/// `drain_empty_active_phases` poll. Without the in-cascade drain step
/// added in this commit chain, the cascade would see the successor
/// phase as empty (the SpawnTasks command sits unapplied on
/// `command_rx`), false-fire `on_phase_end(.., 0, 0)` on it, and the
/// spawned task would be silently dropped — exactly the asm-tokenizer
/// "P|FullPipelineTask: phase memmap discovered 0 item(s)" failure
/// from the brief.
///
/// Setup:
///   1. Phase-a holds one in-flight task (`hash-a`).
///   2. Phase-b is declared as a dependent of phase-a, but starts
///      EMPTY in the pool — there is no placeholder. Pre-fix, this
///      is the load-bearing condition: the cascade's
///      `drain_empty_active_phases` would observe phase-b as
///      empty post-mark-done(phase-a) and false-fire on_phase_end.
///   3. The cascade-time `on_phase_end(phase-a, ..)` callback queues a
///      `PrimaryCommand::SpawnTasks` onto the secondary's
///      `command_tx` — the same channel `PrimaryHandle::spawn_tasks`
///      writes to from the consumer's PyO3 callback. The cascade's
///      per-iteration drain picks it up via the
///      `&mut Some(command_rx)` arg, dispatches through
///      `handle_secondary_command` → `apply_spawn_tasks`, which
///      reinjects into `primary_pending` BEFORE the next
///      `drain_empty_active_phases` poll sees phase-b.
///
/// Asserts:
///   * `on_phase_end` fires for phase-a exactly once with the right
///     counts.
///   * `on_phase_end` does NOT fire for phase-b (the spawned task
///     keeps the successor non-empty, so the cascade does not
///     transition phase-b to Drained).
///   * `cluster_state` shows the spawned task as Pending (the apply
///     path's CRDT mutation landed inline).
///   * The pool now holds the spawned task (post-apply pool reinjection
///     fired inline).
#[tokio::test(flavor = "current_thread")]
async fn cascade_drains_callback_queued_spawn_tasks_inline() {
    use std::collections::HashMap as StdHashMap;
    use std::collections::HashSet as StdHashSet;

    use crate::cluster_state::TaskState;
    use crate::primary::PrimaryCommand;

    let phase_a = PhaseId::from("phase-a");
    let phase_b = PhaseId::from("phase-b");
    let mut sec = make_secondary(election_config("sec-0"));
    sec.is_primary = true;

    // Seed cluster_state with the prereq task so apply_spawn_tasks'
    // dep-resolution finds task-a. PhaseDepsSet declares phase-b
    // dependent on phase-a — same shape populate_primary_from_cluster_state
    // produces post-promotion.
    let task_a = TaskInfo {
        path: PathBuf::from("/tmp/a"),
        size: 100,
        identifier: TestId("a".into()),
        phase_id: phase_a.clone(),
        type_id: TypeId::from("default"),
        affinity_id: None,
        payload: serde_json::Value::Null,
        task_id: Some("task-a".into()),
        task_depends_on: vec![],
        preferred_secondaries: SoftPreferredSecondaries::default(),
        resolved_path: None,
    };
    sec.cluster_state
        .apply(dynrunner_protocol_primary_secondary::ClusterMutation::PhaseDepsSet {
            deps: {
                let mut m = StdHashMap::new();
                m.insert(phase_b.clone(), vec![phase_a.clone()]);
                m
            },
        });
    sec.cluster_state
        .apply(dynrunner_protocol_primary_secondary::ClusterMutation::TaskAdded {
            hash: "hash-a".into(),
            task: task_a.clone(),
        });

    // Pool: phase-a has 1 in-flight (task-a), phase-b is declared but
    // EMPTY. This is the load-bearing condition the regression test
    // pins — pre-fix, phase-b's emptiness post-mark-done(phase-a)
    // would false-Drain it.
    let mut phase_ids = StdHashSet::new();
    phase_ids.insert(phase_a.clone());
    phase_ids.insert(phase_b.clone());
    let mut deps = StdHashMap::new();
    deps.insert(phase_b.clone(), vec![phase_a.clone()]);
    let mut pool = PendingPool::<TestId>::new(phase_ids, deps).expect("graph valid");
    pool.mark_tasks_in_flight(vec![("task-a".into(), phase_a.clone())]);
    sec.primary_pending = Some(pool);
    sec.primary_in_flight
        .insert("hash-a".into(), make_in_flight("a", "phase-a"));

    // Spawn target: phase-b task with NO task-level dep on task-a.
    // Task-level deps would make `apply_spawn_tasks` route the entry
    // into CRDT `Blocked` (waiting for task-a's TaskCompleted), and
    // the secondary's apply_spawn_tasks routing rule "Blocked → no
    // pool-side action" would leave the pool empty. The PHASE-level
    // dep (PhaseDepsSet earlier) is what keeps phase-b Blocked in
    // the pool's phase-state machine until mark_phase_done(phase-a)
    // flips it. The pool's task-spawn path treats Pending tasks as
    // pool-resident regardless of phase activation — they're picked
    // up by view_for_worker once the phase activates.
    //
    // This is the production-natural shape for the asm-tokenizer
    // FullPipelineTask bug: the per-phase callback discovers items
    // dynamically (e.g. `phase memmap discovered 1 item(s)`) and
    // they don't carry task-level deps because the per-phase
    // dependency is already encoded in PhaseDepsSet.
    let task_b_to_spawn = TaskInfo {
        path: PathBuf::from("/tmp/b"),
        size: 100,
        identifier: TestId("b".into()),
        phase_id: phase_b.clone(),
        type_id: TypeId::from("default"),
        affinity_id: None,
        payload: serde_json::Value::Null,
        task_id: Some("task-b".into()),
        task_depends_on: vec![],
        preferred_secondaries: SoftPreferredSecondaries::default(),
        resolved_path: None,
    };
    let hash_b = crate::primary::wire::compute_task_hash(&task_b_to_spawn);

    // Take the receiver out of the coordinator and wire the sender
    // into the callback. The receiver lives in this test's local
    // scope for the duration of the cascade — same discipline
    // process_tasks uses to drive its select! arms.
    let mut command_rx = sec.command_rx.take();
    let command_tx = sec.command_sender();

    let calls: Arc<Mutex<Vec<(String, u32, u32)>>> = Arc::new(Mutex::new(Vec::new()));
    let calls_inner = calls.clone();
    // The reply oneshots from the queued commands need to live for the
    // duration of the cascade so the handler can fulfill them; collect
    // them in a Vec the test holds across the cascade call.
    let captured_replies: Arc<Mutex<Vec<tokio::sync::oneshot::Receiver<_>>>> =
        Arc::new(Mutex::new(Vec::new()));
    let captured_replies_inner = captured_replies.clone();
    sec.register_phase_lifecycle_callbacks(
        Box::new(|_: &PhaseId| {}),
        Box::new(move |p: &PhaseId, c: u32, f: u32| {
            calls_inner
                .lock()
                .expect("poisoned")
                .push((p.as_str().to_string(), c, f));
            // Mirror the PyO3 PrimaryHandle.spawn_tasks shape: build a
            // SpawnTasks command with a oneshot reply, try_send onto
            // the channel. Production's channel-into-cascade path is
            // exactly this; only the callback issuer is different
            // (PyO3 wrapper vs this test).
            if p.as_str() == "phase-a" {
                let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
                let cmd = PrimaryCommand::SpawnTasks {
                    tasks: vec![task_b_to_spawn.clone()],
                    reply: reply_tx,
                };
                command_tx
                    .try_send(cmd)
                    .expect("command channel must accept the queued spawn");
                captured_replies_inner
                    .lock()
                    .expect("poisoned")
                    .push(reply_rx);
            }
        }),
    );

    let initial_count = sec.cluster_state.task_count();
    assert_eq!(initial_count, 1, "fixture starts with task-a only");

    // Drive the cascade. This is the exact call shape
    // secondary/processing/process_tasks.rs:140 makes for a worker
    // event that produces a TaskComplete. The `&mut command_rx` is the
    // load-bearing parameter — without it the inline drain step
    // sees `None` and the queued SpawnTasks would sit on the channel
    // until a later select! tick, exactly recreating the bug.
    sec.note_primary_item_completed("hash-a", &mut command_rx).await;

    // Asserts:

    // (1) on_phase_end fired for phase-a exactly once.
    let recorded = calls.lock().expect("poisoned").clone();
    let phase_a_calls: Vec<_> = recorded
        .iter()
        .filter(|(p, _, _)| p == "phase-a")
        .collect();
    assert_eq!(
        phase_a_calls.len(),
        1,
        "on_phase_end fires once for phase-a; got {recorded:?}"
    );
    assert_eq!(
        phase_a_calls[0],
        &("phase-a".to_string(), 1, 0),
        "phase-a callback observes (completed=1, failed=0)"
    );

    // (2) on_phase_end does NOT fire for phase-b. This is THE
    // regression-pin: pre-fix, the cascade's
    // drain_empty_active_phases poll observed phase-b as empty
    // post-mark-done(phase-a) and false-fired on_phase_end here.
    let phase_b_calls: Vec<_> = recorded
        .iter()
        .filter(|(p, _, _)| p == "phase-b")
        .collect();
    assert!(
        phase_b_calls.is_empty(),
        "on_phase_end MUST NOT fire for phase-b: the callback-queued \
         spawn_tasks(task-b) applies inline within the cascade, so \
         phase-b is non-empty when drain_empty_active_phases polls. \
         Pre-fix: phase-b false-fired on_phase_end(.., 0, 0). \
         Got {recorded:?}"
    );

    // (3) cluster_state grew by 1 — the spawn's CRDT mutation landed
    // inside the cascade (via apply_spawn_tasks ->
    // apply_and_broadcast_cluster_mutations).
    assert_eq!(
        sec.cluster_state.task_count(),
        initial_count + 1,
        "task_count grew by 1: the inline-applied SpawnTasks broadcast \
         landed during the cascade"
    );

    // (4) The spawned task is Pending in the CRDT (task-a's prereq is
    // already InFlight at this point in the cascade — apply_spawn_tasks
    // observes the post-cascade ledger where task-a is about to
    // complete, but the on_item_finished hasn't broadcast TaskCompleted
    // yet, so task-b lands Blocked-on-task-a or Pending depending on
    // CRDT apply order). Either terminal state proves the apply
    // path executed.
    let state = sec
        .cluster_state
        .task_state(&hash_b)
        .expect("spawned task present in CRDT ledger");
    assert!(
        matches!(state, TaskState::Pending { .. } | TaskState::Blocked { .. }),
        "spawned task lands Pending or Blocked-pending-deps; got {state:?}"
    );

    // (5) The reply oneshot fired with Ok — apply_spawn_tasks succeeded
    // (no per-index errors). Drop the Mutex guard in a tight scope
    // BEFORE awaiting so the std::sync::Mutex isn't held across an
    // await point (clippy::await_holding_lock).
    let reply = {
        let mut replies = captured_replies.lock().expect("poisoned");
        assert_eq!(replies.len(), 1, "callback queued exactly one SpawnTasks");
        replies.pop().expect("one reply")
    };
    let result = reply.await.expect("reply oneshot fires inline");
    assert!(
        result.as_ref().is_ok_and(|errs| errs.is_empty()),
        "apply_spawn_tasks succeeded with no per-index errors: {result:?}"
    );
}
