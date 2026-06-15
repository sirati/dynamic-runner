#![cfg(test)]

use super::super::test_helpers::{
    NoPeers, SecondaryHarness, TestId, election_config, make_secondary,
};
use super::super::*;
use dynrunner_core::TaskInfo;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

/// Construct a 0-worker late-joiner: a single `SecondaryCoordinator`
/// with `num_workers=0` (the substrate the standalone observer is built
/// on — see `observer/lifecycle.rs`). The "rest of the cluster" shows up
/// purely as the snapshot the test hands it. The `NoPeers` mesh stub
/// (peer_count=0) is what `make_secondary` uses elsewhere; the
/// late-joiner code path the test cares about (restore + skip-setup) runs
/// to completion regardless of peer reachability — peer membership is
/// asserted on the role-table side, not the transport side. There is no
/// observer MODE on the coordinator: observer-ness is the standalone
/// `ObserverCoordinator` role plus the peer-side `RoleTable.observers`
/// filter, never a `config` flag.
fn make_zero_worker_late_joiner(node_id: &str) -> SecondaryHarness<NoPeers> {
    // The node's mesh wraps the `NoPeers` stub; it restores from a snapshot,
    // skips setup, and reads its terminal cue from `cluster_state`
    // (RunComplete applied directly) — it never needs a primary inbound, so
    // the `NoPeers` mesh does not change what the late-joiner test exercises.
    let mut config = election_config(node_id);
    config.num_workers = 0;
    make_secondary(config)
}

/// Build a synthetic `ClusterStateSnapshot<TestId>` carrying two
/// pending tasks, a designated `current_primary`, primary_epoch=7,
/// and one observer id. The same shape the wire frame's
/// `snapshot_json` decodes to.
fn make_synthetic_snapshot() -> crate::cluster_state::ClusterStateSnapshot<TestId> {
    use crate::cluster_state::TaskState;
    let mut tasks = HashMap::new();
    let mk_pending = |path: &str, ident: &str| TaskState::Pending {
        attempt: 0,
        task: TaskInfo {
            path: PathBuf::from(path),
            size: 100,
            identifier: TestId(ident.into()),
            phase_id: dynrunner_core::PhaseId::from("default"),
            type_id: dynrunner_core::TypeId::from("default"),
            affinity_id: None,
            payload: serde_json::Value::Null,
            task_id: ident.into(),
            task_depends_on: vec![],
            preferred_secondaries: dynrunner_core::SoftPreferredSecondaries::default(),
            preferred_version: Default::default(),
            kind: Default::default(),
            setup_affinity: None,
            upload_file: None,
            required_files: None,
            resolved_path: None,
        },
        version: Default::default(),
    };
    tasks.insert("task-1".to_string(), mk_pending("/tmp/task-1", "task-1"));
    tasks.insert("task-2".to_string(), mk_pending("/tmp/task-2", "task-2"));
    // C6: the observer rides the `capabilities` 2P-set as an
    // `Advertised { is_observer: true }`, AND must be ALIVE for the
    // `role_table().observers` projection (capability × local-alive) to
    // include it — so seed `alive_members` with the same id.
    let mut capabilities = HashMap::new();
    capabilities.insert(
        "observer-peer".to_string(),
        crate::cluster_state::CapabilityEntry::Advertised {
            is_observer: true,
            can_be_primary: false,
            cap_version: Default::default(),
            member_gen: 0,
        },
    );
    let mut alive_members = HashSet::new();
    alive_members.insert("observer-peer".to_string());
    crate::cluster_state::ClusterStateSnapshot {
        tasks,
        current_primary: Some("primary-peer".to_string()),
        primary_epoch: 7,
        phase_deps: HashMap::new(),
        phase_may_be_empty: std::collections::HashSet::new(),
        phase_no_barrier: std::collections::HashSet::new(),
        capabilities,
        peer_holdings: HashMap::new(),
        task_outputs: HashMap::new(),
        secondary_capacities: HashMap::new(),
        alive_members,
        run_complete: false,
        run_aborted: None,
        terminal_outcome: None,
        graceful_abort_requested: false,
        wind_down_requested: HashSet::new(),
        discovery_debt: crate::cluster_state::DiscoveryDebt::Undeclared,
        phase_event_tallies: HashMap::new(),
        retry_passes_used: HashMap::new(),
        unfulfillable_reinject_used: HashMap::new(),
        respawn_events: HashMap::new(),
        respawn_policy: None,
        phases_ended: HashSet::new(),
        custom_messages: HashMap::new(),
        custom_terminal_watermarks: HashMap::new(),
        member_generations: HashMap::new(),
    }
}

/// `restore_from_snapshot_and_skip_setup` is the load-bearing
/// API: a single call must (a) install the snapshot's task
/// ledger, observers, and current_primary into the coordinator's
/// `cluster_state`, AND (b) latch `setup_phase_completed=true`
/// so the next `run_until_setup_or_done` call skips the
/// welcome / cert-exchange / wait-for-setup phases.
#[test]
fn restore_installs_snapshot_and_latches_setup_completed() {
    let mut sec = make_zero_worker_late_joiner("late-joiner-1");

    // Pre-condition: every field this test asserts is at its
    // freshly-constructed default. Pinning the pre-conditions
    // catches "the field was already true / non-empty before
    // restore" regressions that would otherwise silently make
    // the post-condition asserts pass for the wrong reason.
    assert!(!sec.lifecycle.setup_phase_completed());
    assert_eq!(sec.cluster_state.task_count(), 0);
    assert!(sec.cluster_state.current_primary().is_none());
    assert!(sec.cluster_state.role_table().observers.is_empty());

    sec.restore_from_snapshot_and_skip_setup(make_synthetic_snapshot());

    // Latch is set — `run_until_setup_or_done` will skip the
    // entire `!setup_phase_completed` setup block on its next
    // call.
    assert!(sec.lifecycle.setup_phase_completed());

    // Task ledger merged in: two pending tasks survive.
    assert_eq!(sec.cluster_state.task_count(), 2);

    // current_primary and primary_epoch reflect the snapshot's
    // authority — the egress edge (`send_to` → `resolve_destination`)
    // reads `cluster_state.current_primary()`, so a
    // `Destination::Primary` send resolves immediately rather than
    // falling back to the bootstrap peer-id.
    assert_eq!(sec.cluster_state.current_primary(), Some("primary-peer"),);
    assert_eq!(sec.cluster_state.primary_epoch(), 7);

    // Observer set merged — the election filter will skip
    // `observer-peer` from `lowest_alive` candidate selection
    // even before the next live PeerInfo broadcast lands.
    let observers = &sec.cluster_state.role_table().observers;
    assert!(observers.contains("observer-peer"));
    assert_eq!(observers.len(), 1);
}

/// The read-only `cluster_state()` accessor returns a borrow of the
/// replicated ledger that reflects the restored snapshot's REAL
/// state — this is the exact view the late-joiner observer's run
/// loop projects (`StatsSnapshot::from_cluster_state`) and publishes
/// to its periodic reporter after `restore_from_snapshot_and_skip_setup`.
/// Pins that the accessor is a faithful, non-mutating window onto the
/// CRDT (the same `counts()` the loop would project), so the reporter
/// receives real data and not a placeholder.
#[test]
fn cluster_state_accessor_reflects_restored_snapshot() {
    let mut sec = make_zero_worker_late_joiner("late-joiner-1");
    // Pre-restore: a fresh coordinator's CRDT is empty, so a
    // projection here is the all-zero default (reporter stays silent).
    assert_eq!(sec.cluster_state().task_count(), 0);

    sec.restore_from_snapshot_and_skip_setup(make_synthetic_snapshot());

    // Post-restore the accessor surfaces the snapshot's two pending
    // tasks — the same window the run loop hands `from_cluster_state`.
    let view = sec.cluster_state();
    assert_eq!(
        view.task_count(),
        2,
        "two tasks visible through the accessor"
    );
    assert_eq!(
        view.counts().pending,
        2,
        "both restored tasks are Pending in the projected view"
    );
    // The accessor is a faithful read-only borrow: it agrees with the
    // crate-internal field it exposes (no lossy copy / divergence).
    assert_eq!(view.task_count(), sec.cluster_state.task_count());
}

/// The same `restore` call applied twice is a no-op the second
/// time — `ClusterState::restore` is documented as idempotent /
/// CRDT-merge. Pins that the wrapper preserves the underlying
/// idempotency (i.e. the wrapper doesn't toggle the latch back
/// or re-broadcast).
#[test]
fn restore_is_idempotent_on_second_call() {
    let mut sec = make_zero_worker_late_joiner("late-joiner-1");
    sec.restore_from_snapshot_and_skip_setup(make_synthetic_snapshot());
    let tasks_after_first = sec.cluster_state.task_count();
    let epoch_after_first = sec.cluster_state.primary_epoch();

    // Second call with the SAME snapshot — the merge rules
    // (`primary_epoch > self.primary_epoch` gate, observer-set
    // "only when local empty" gate) make this a no-op.
    sec.restore_from_snapshot_and_skip_setup(make_synthetic_snapshot());

    assert!(sec.lifecycle.setup_phase_completed(), "latch stays true");
    assert_eq!(sec.cluster_state.task_count(), tasks_after_first);
    assert_eq!(sec.cluster_state.primary_epoch(), epoch_after_first);
}

/// After restore, `run_until_setup_or_done` skips the entire setup
/// handshake (the setup-skip latch took effect) and the observe
/// loop exits ONLY when the cluster's `run_complete()` flag is set
/// — deterministically, no wall-clock race.
///
/// Construction: restore the snapshot AND apply
/// `ClusterMutation::RunComplete` BEFORE driving the loop. With
/// `run_complete()` true and `active_tasks` empty (num_workers=0),
/// `process_tasks`' top-of-loop exit fires on the first iteration
/// and returns `Done`. If the setup-skip latch had NOT taken
/// effect, the welcome handshake would error on the disconnected
/// uplink and return `Err` instead — so an `Ok(Done)` proves BOTH
/// the setup-skip AND the run-complete exit cue.
#[tokio::test(flavor = "current_thread")]
async fn observer_skips_setup_and_exits_on_run_complete() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut sec = make_zero_worker_late_joiner("late-joiner-1");
            sec.restore_from_snapshot_and_skip_setup(make_synthetic_snapshot());
            // Deterministic exit cue: the cluster has declared the
            // run finished. The observer's SOLE exit cue is
            // `run_complete()`.
            sec.cluster_state
                .apply(dynrunner_protocol_primary_secondary::ClusterMutation::RunComplete { counts: Default::default() });
            assert!(
                sec.cluster_state.run_complete(),
                "precondition: RunComplete applied",
            );

            let mut factory = super::super::test_helpers::FakeWorkerFactory;
            let outcome = sec.run_until_setup_or_done(&mut factory).await.expect(
                "run_until_setup_or_done must NOT Err — an Err means the \
                         setup-skip latch failed and the welcome handshake ran \
                         against the dead uplink",
            );
            assert!(
                matches!(outcome, RunOutcome::Terminal),
                "observer must reach a terminal on run_complete(); got {outcome:?}",
            );
            assert!(
                matches!(sec.terminal(), Some(SecondaryTerminal::Done)),
                "observer's per-secondary terminal must be Done on run_complete(); \
                 got {:?}",
                sec.terminal(),
            );
        })
        .await;
}

/// Regression: a `TaskAssignment` reaching a 0-worker `Operational`
/// node MUST (1) NOT panic / `u32`-underflow, AND (2) BOUNCE the task
/// back to the primary as a typed `IllegallyAssignedToNonidleWorker`
/// (#517 honor-or-bounce) — never the old generic backpressure
/// `TaskFailed`.
///
/// A late-joiner / observer (and any node that ends a phase as a
/// 0-worker observer) constructs `Operational` with an EMPTY pool —
/// `operational_observer` installs `WorkerPool::new()` — and that
/// state IS on the inbound dispatch path. The honor-or-bounce seam
/// (`select_honored_target_or_bounce`) selects the dispatch target as
/// an `Option` (`.get()`), so a 0-worker pool yields `None` with no
/// `pool.workers.len() as u32 - 1` underflow and no index into the
/// empty slice; the secondary NEVER re-picks another worker — it
/// bounces the typed report so the primary reconciles + requeues
/// rather than silently dropping or fail-accounting the task. With no
/// worker there is no incumbent, so the bounce carries `incumbent:
/// None`.
///
/// The recording transport + `set_bootstrap_primary_id("setup")`
/// make `Destination::Primary` resolve to a captured peer send, so the
/// test asserts the bounce report actually went out (not merely "did
/// not panic").
///
/// `is_observer=false` deliberately: this is the GENERAL 0-worker
/// `Operational` case (a phase-end observer or a worker that spawned
/// no slots), not specifically the observer ROLE — the underflow was a
/// pool-cardinality bug, independent of role.
#[tokio::test(flavor = "current_thread")]
async fn task_assignment_to_zero_worker_operational_node_reports_backpressure_not_underflow() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // 0-worker node: landed in `Operational` with the empty pool
            // `operational_observer` installs (the same empty-pool shape
            // the production late-joiner / phase-end-observer flow
            // produces). No pool is installed after
            // `enter_operational_for_test`, so `pool.workers` is empty.
            let (mut sec, log) = super::super::test_helpers::make_secondary_recording(
                election_config("zero-worker-op"),
                1,
            );
            // Cold role table: resolve `Destination::Primary` to the
            // captured `"primary"` peer so the backpressure report is
            // recorded (the secondary's own id differs, so it resolves to
            // a `Peer` send, not loopback).
            sec.set_bootstrap_primary_id("setup".to_string());
            sec.enter_operational_for_test();
            assert_eq!(
                sec.op_mut().pool.workers.len(),
                0,
                "precondition: the operational node has a 0-worker pool",
            );

            // Fabricate the wire shape a primary would send. `worker_id`
            // is intentionally a normal index (0) — pre-fix the underflow
            // was in the pool-length clamp, not in the requested id, so
            // even a valid-looking `worker_id` triggered it on an empty
            // pool.
            let binary = super::processing::make_binary("orphan-task", 50);
            let file_hash = format!("hash_{}", binary.identifier.0);
            let assignment = DistributedMessage::TaskAssignment {
                target: None,
                sender_id: "setup".into(),
                timestamp: 0.0,
                secondary_id: "zero-worker-op".into(),
                worker_id: 0,
                zip_file: None,
                binary_info:
                    dynrunner_protocol_primary_secondary::DistributedBinaryInfo::from_task_info(
                        &binary,
                    ),
                local_path: binary.path.to_string_lossy().into_owned(),
                file_hash: file_hash.clone(),
                predecessor_outputs: std::collections::BTreeMap::new(),
                supplanted_holder: None,
            };

            // The critical call: pre-fix this panicked (debug) / indexed
            // out of bounds (release) on the empty pool. Post-#517 it
            // returns Ok and BOUNCES a typed `IllegallyAssignedToNonidleWorker`
            // (the slot does not exist — no incumbent), NOT the old generic
            // backpressure TaskFailed.
            let mut factory = super::super::test_helpers::FakeWorkerFactory;
            let result = sec.dispatch_message(assignment, &mut factory).await;
            // Flush the queued bounce report onto the RecordingPeer log
            // (MeshClient::send is queued, drained by the pump).
            sec.drain_egress().await;
            assert!(
                result.is_ok(),
                "TaskAssignment to a 0-worker Operational node must not \
                 panic / underflow, got {result:?}",
            );

            // (1) It must NOT have spuriously recorded an active task —
            // there was no worker to assign to.
            assert!(
                sec.op_mut().active_tasks.is_empty(),
                "no worker exists, so nothing should be tracked as active; \
                 active_tasks={:?}",
                sec.op_mut().active_tasks,
            );

            // (2) It MUST bounce the task back to the primary as a typed
            // `IllegallyAssignedToNonidleWorker` (the #517 honor-or-bounce),
            // keyed by the original wire `worker_id`, with NO incumbent (a
            // 0-worker pool has no running task). It is NOT a TaskFailed, so
            // the primary reconciles + requeues rather than fail-accounting.
            let bounced = log.borrow().iter().any(|m| {
                matches!(
                    m,
                    DistributedMessage::IllegallyAssignedToNonidleWorker {
                        worker_id: 0,
                        assigned,
                        incumbent: None,
                        ..
                    } if assigned.hash == file_hash
                )
            });
            assert!(
                bounced,
                "a 0-worker node must bounce IllegallyAssignedToNonidleWorker \
                 (no incumbent), not the old backpressure TaskFailed; captured \
                 sends: {:?}",
                log.borrow(),
            );
            assert!(
                !log.borrow().iter().any(|m| matches!(
                    m,
                    DistributedMessage::TaskFailed { task_hash, .. } if *task_hash == file_hash
                )),
                "the bounce must NOT be a TaskFailed (no failure accounting)",
            );
        })
        .await;
}

/// Regression (#517): an out-of-range `worker_id` on a NON-empty pool
/// is BOUNCED — the secondary never re-picks another idle worker, and
/// never clamps onto the last slot.
///
/// Pre-fix #1 (cabd34ab and earlier), selection did
/// `worker_id.min(pool.workers.len() as u32 - 1)`, so a bogus id (e.g.
/// `999`) was clamped onto the last worker. Pre-fix #2 (the cabd34ab
/// `.or_else` fallback) re-picked the FIRST idle worker — still running
/// the task on a slot the primary never assigned (the occupancy-drift
/// root). #517 honors the assigned slot: an out-of-range id has no idle
/// slot to honor, so it bounces `IllegallyAssignedToNonidleWorker` (no
/// incumbent) and the task lands on NO worker here — the primary
/// reconciles + requeues onto a genuinely-idle slot it tracks.
#[tokio::test(flavor = "current_thread")]
async fn out_of_range_worker_id_bounces_never_repicks_or_clamps() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Two-worker operational node. `enter_operational_for_test`
            // installs an empty pool; spawn the workers into it via the
            // real `initialize` (which waits for all-ready), so both
            // slots are genuinely Idle.
            let mut config = election_config("oob-worker");
            config.num_workers = 2;
            let (mut sec, log) = super::super::test_helpers::make_secondary_recording(config, 1);
            sec.set_bootstrap_primary_id("setup".to_string());
            sec.enter_operational_for_test();

            let max = sec.max_resources();
            let scheduler = dynrunner_scheduler::ResourceStealingScheduler::memory();
            let mut factory = super::super::test_helpers::FakeWorkerFactory;
            sec.op_mut()
                .pool
                .initialize(2, &max, &scheduler, &mut factory, false, None)
                .await
                .expect("pool.initialize must spawn both workers");
            // Pre-bind both slots to the task's type so the dispatch hits
            // the `AlreadyLoaded` fast path and records the assignment in
            // `active_tasks` (rather than the respawn-in-progress defer),
            // making the chosen slot directly assertable.
            for w in &mut sec.op_mut().pool.workers {
                w.loaded_type_id = Some(dynrunner_core::TypeId::from("default"));
            }
            assert_eq!(sec.op_mut().pool.workers.len(), 2);
            assert!(
                sec.op_mut().pool.workers.iter().all(|w| w.is_idle_state()),
                "precondition: both workers idle",
            );

            // Assign with a wildly out-of-range `worker_id`. Pre-fix this
            // clamped to slot 1 (the last) / re-picked slot 0; post-#517 it
            // bounces (no idle slot to honor at id 999).
            let binary = super::processing::make_binary("oob-task", 50);
            let file_hash = format!("hash_{}", binary.identifier.0);
            let assignment = DistributedMessage::TaskAssignment {
                target: None,
                sender_id: "setup".into(),
                timestamp: 0.0,
                secondary_id: "oob-worker".into(),
                worker_id: 999,
                zip_file: None,
                binary_info:
                    dynrunner_protocol_primary_secondary::DistributedBinaryInfo::from_task_info(
                        &binary,
                    ),
                local_path: binary.path.to_string_lossy().into_owned(),
                file_hash: file_hash.clone(),
                predecessor_outputs: std::collections::BTreeMap::new(),
                supplanted_holder: None,
            };

            let result = sec.dispatch_message(assignment, &mut factory).await;
            // Flush the queued bounce report onto the RecordingPeer log
            // (MeshClient::send is queued, drained by the pump).
            sec.drain_egress().await;
            assert!(result.is_ok(), "dispatch must succeed, got {result:?}");

            // The task landed on NO worker: the secondary never re-picked an
            // idle slot (slot 0 stays untouched) and never clamped to the
            // last (slot 1). active_tasks is empty for this hash.
            assert!(
                !sec.op_mut().active_tasks.contains_key(&file_hash),
                "out-of-range worker_id must NOT run the task on any slot \
                 (no re-pick, no clamp); active_tasks={:?}",
                sec.op_mut().active_tasks,
            );
            assert!(
                sec.op_mut().pool.workers.iter().all(|w| w.is_idle_state()),
                "both worker slots must remain idle (no re-pick onto slot 0)",
            );

            // It bounced the typed report (worker_id echoed verbatim = 999),
            // with no incumbent (the requested slot does not exist).
            let bounced = log.borrow().iter().any(|m| {
                matches!(
                    m,
                    DistributedMessage::IllegallyAssignedToNonidleWorker {
                        worker_id: 999,
                        assigned,
                        incumbent: None,
                        ..
                    } if assigned.hash == file_hash
                )
            });
            assert!(
                bounced,
                "an out-of-range worker_id must bounce \
                 IllegallyAssignedToNonidleWorker (worker_id=999, no \
                 incumbent); captured sends: {:?}",
                log.borrow(),
            );
        })
        .await;
}

/// A `TaskInfo` carrying a RELATIVE `path` — the wire `local_path` that
/// `report_unresolvable_task` cannot resolve when the secondary has no
/// staging dir (`src_network=None`, the test default). Reaching the
/// fail-loud guard requires `resolved_path.is_none() && local_path is
/// relative`, which a relative path produces (the extraction cache has no
/// entry for it).
fn make_relative_path_binary(name: &str) -> TaskInfo<TestId> {
    TaskInfo {
        path: PathBuf::from(format!("relative/{name}")),
        size: 50,
        identifier: TestId(name.into()),
        phase_id: dynrunner_core::PhaseId::from("default"),
        type_id: dynrunner_core::TypeId::from("default"),
        affinity_id: None,
        payload: serde_json::Value::Null,
        task_id: name.into(),
        task_depends_on: vec![],
        preferred_secondaries: dynrunner_core::SoftPreferredSecondaries::default(),
        preferred_version: Default::default(),
        kind: Default::default(),
        setup_affinity: None,
        upload_file: None,
        required_files: None,
        resolved_path: None,
    }
}

/// Regression (helpers.rs `report_unresolvable_task`): an UNRESOLVABLE
/// relative-path `TaskAssignment` reaching a 0-worker `Operational` node
/// MUST (1) NOT panic / `u32`-underflow, AND (2) report the task back to
/// the primary as the fail-loud `TaskFailed { NonRecoverable }` keyed by
/// the ORIGINAL wire `worker_id`.
///
/// This exercises the SECOND of the two sibling underflow sites missed by
/// the original router fix. `report_unresolvable_task` echoed the worker
/// id back through `worker_id.min(pool.workers.len() as u32 - 1)` purely
/// to "correct" the reported id; on a 0-worker pool that was `0u32 - 1`
/// (panic in debug / wrap in release) before the guard ever produced a
/// frame. The root-cause fix reports the un-clamped wire `worker_id`
/// directly (the value never indexes the pool), so the 0-worker case is a
/// clean fail-loud report with no pool arithmetic at all.
///
/// Distinct from the absolute-path 0-worker test above: an ABSOLUTE
/// `local_path` makes `report_unresolvable_task` return `Ok(false)`
/// (plausibly resolvable at the worker), so it reaches the router's
/// "no idle worker" backpressure. Only a RELATIVE path drives the
/// fail-loud guard whose clamp this test pins.
#[tokio::test(flavor = "current_thread")]
async fn unresolvable_task_to_zero_worker_node_reports_failure_not_underflow() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut sec, log) = super::super::test_helpers::make_secondary_recording(
                election_config("zero-worker-unresolvable"),
                1,
            );
            sec.set_bootstrap_primary_id("setup".to_string());
            sec.enter_operational_for_test();
            assert_eq!(
                sec.op_mut().pool.workers.len(),
                0,
                "precondition: the operational node has a 0-worker pool",
            );

            // Relative `local_path` + no `src_network` (election_config
            // default) ⇒ `report_unresolvable_task` fires.
            let binary = make_relative_path_binary("unresolvable-task");
            let file_hash = format!("hash_{}", binary.identifier.0);
            let assignment = DistributedMessage::TaskAssignment {
                target: None,
                sender_id: "setup".into(),
                timestamp: 0.0,
                secondary_id: "zero-worker-unresolvable".into(),
                // A non-zero wire id to ALSO prove the reported id is the
                // wire value, never a pool-clamped one.
                worker_id: 3,
                zip_file: None,
                binary_info:
                    dynrunner_protocol_primary_secondary::DistributedBinaryInfo::from_task_info(
                        &binary,
                    ),
                local_path: binary.path.to_string_lossy().into_owned(),
                file_hash: file_hash.clone(),
                predecessor_outputs: std::collections::BTreeMap::new(),
                supplanted_holder: None,
            };

            let mut factory = super::super::test_helpers::FakeWorkerFactory;
            let result = sec.dispatch_message(assignment, &mut factory).await;
            // Flush the queued backpressure report onto the RecordingPeer log
            // (MeshClient::send is queued, drained by the pump).
            sec.drain_egress().await;
            assert!(
                result.is_ok(),
                "unresolvable TaskAssignment to a 0-worker node must not \
                 panic / underflow, got {result:?}",
            );

            assert!(
                sec.op_mut().active_tasks.is_empty(),
                "an unresolvable task must not be tracked as active; \
                 active_tasks={:?}",
                sec.op_mut().active_tasks,
            );

            // The fail-loud guard sends `NonRecoverable` keyed by the
            // ORIGINAL wire `worker_id` (3), not a clamped value.
            let reported = log.borrow().iter().any(|m| {
                matches!(
                    m,
                    DistributedMessage::TaskFailed {
                        target: _,
                        error_type: dynrunner_core::ErrorType::NonRecoverable,
                        task_hash,
                        worker_id,
                        ..
                    } if *task_hash == file_hash && *worker_id == 3
                )
            });
            assert!(
                reported,
                "a 0-worker node must fail-loud-report the unresolvable task \
                 (NonRecoverable, wire worker_id=3); captured sends: {:?}",
                log.borrow(),
            );
        })
        .await;
}

/// Regression (setup.rs `handle_initial_assignment`): an
/// `InitialAssignment` naming a worker on a 0-worker pool MUST NOT
/// underflow, and an unresolvable relative-path entry in it MUST be
/// fail-loud-reported (NOT silently assigned to a non-existent slot).
///
/// `handle_initial_assignment` indexed the pool through the identical
/// `worker_id.min(pool.workers.len() as u32 - 1)` clamp — the THIRD
/// sibling of the router site. On a 0-worker pool that underflowed before
/// any task was even examined. The root-cause fix selects the dispatch
/// target as an `Option` (`.get()` / `position()`), so a 0-worker pool
/// yields `None` (no slot, the task is left for the authority) and the
/// unresolvable guard is keyed by the un-clamped wire `worker_id`.
///
/// Driven from an `Operational` 0-worker node: `handle_initial_assignment`
/// reaches the pool / `active_tasks` / `report_unresolvable_task` through
/// the same accessors that resolve in both `Configuring` and
/// `Operational`, so `enter_operational_for_test`'s empty pool is a
/// faithful stand-in for the production "spawned 0 workers" pool.
#[tokio::test(flavor = "current_thread")]
async fn initial_assignment_to_zero_worker_pool_does_not_underflow() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut sec, log) = super::super::test_helpers::make_secondary_recording(
                election_config("zero-worker-initial"),
                1,
            );
            sec.set_bootstrap_primary_id("setup".to_string());
            sec.enter_operational_for_test();
            assert_eq!(
                sec.op_mut().pool.workers.len(),
                0,
                "precondition: the node has a 0-worker pool",
            );

            // One zip entry with a relative `local_path` ⇒ unresolvable,
            // so it reaches `report_unresolvable_task`. `worker_id: 5`
            // names a slot that does not exist on the empty pool — pre-fix
            // the clamp `5.min(0u32 - 1)` underflowed here.
            let binary = make_relative_path_binary("initial-orphan");
            let file_hash = format!("hash_{}", binary.identifier.0);
            let zip_files = vec![dynrunner_protocol_primary_secondary::ZipFileAssignment {
                zip_name: String::new(),
                binaries: vec![dynrunner_protocol_primary_secondary::ZipBinaryEntry {
                    local_path: binary.path.to_string_lossy().into_owned(),
                    binary_info:
                        dynrunner_protocol_primary_secondary::DistributedBinaryInfo::from_task_info(
                            &binary,
                        ),
                    hash: file_hash.clone(),
                }],
            }];
            let workers_ready = vec![dynrunner_protocol_primary_secondary::WorkerReadyInfo {
                worker_id: 5,
                resource_budgets: vec![],
            }];

            let mut factory = super::super::test_helpers::FakeWorkerFactory;
            // The critical call: pre-fix this underflowed on the empty pool
            // before examining any task. Post-fix it completes.
            sec.handle_initial_assignment(zip_files, workers_ready, vec![], &mut factory)
                .await;
            // Flush the queued fail-loud report onto the RecordingPeer log.
            sec.drain_egress().await;

            // No worker exists, so nothing is tracked as active.
            assert!(
                sec.op_mut().active_tasks.is_empty(),
                "a 0-worker pool must not track any initial task as active; \
                 active_tasks={:?}",
                sec.op_mut().active_tasks,
            );

            // The unresolvable entry was fail-loud-reported keyed by the
            // wire `worker_id` (5), proving the guard ran with the
            // un-clamped id rather than panicking.
            let reported = log.borrow().iter().any(|m| {
                matches!(
                    m,
                    DistributedMessage::TaskFailed {
                        target: _,
                        error_type: dynrunner_core::ErrorType::NonRecoverable,
                        task_hash,
                        worker_id,
                        ..
                    } if *task_hash == file_hash && *worker_id == 5
                )
            });
            assert!(
                reported,
                "the unresolvable initial-assignment task must be fail-loud \
                 reported (NonRecoverable, wire worker_id=5); captured sends: {:?}",
                log.borrow(),
            );
        })
        .await;
}

/// Defense-in-depth (#488): a STAGING-mode node (`src_network` set — a
/// pre-staged bind-mount or a StageFile destination) that cannot resolve
/// an assigned file LOCALLY must report the failure RECOVERABLE, not
/// NonRecoverable.
///
/// Why: with `src_network` set the file is reinjectable ELSEWHERE — a
/// DIFFERENT secondary may hold it staged (the bind-mount is per-node;
/// in a respawn/partial-stage topology not every node has every file).
/// A `NonRecoverable` report is terminal AND not re-routed by the
/// primary, so the task is permanently lost (the #488 consumer symptom:
/// 274/660 tasks silently lost, run falsely "complete"). `Recoverable`
/// re-enters the per-phase retry bucket: the primary re-injects it into
/// the pool (where a staged peer can pick it up) and the
/// `retry_max_passes` budget BOUNDS it — a task placeable nowhere
/// fails-final after the budget, never silently lost and never looped.
///
/// The genuine-misconfig case (`src_network` unset + a relative path the
/// worker can never open) stays NonRecoverable — pinned by the two
/// `unresolvable_task_*` / `initial_assignment_*` tests above; rerouting
/// it would only bounce it to identically-unconfigured peers.
#[tokio::test(flavor = "current_thread")]
async fn unresolvable_task_in_staging_mode_is_recoverable_not_lost() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // A staging-mode node: `src_network` points at a real (empty)
            // directory — staging is CONFIGURED, but THIS node never staged
            // the assigned file. Production shape: a member's bind-mount
            // lacks a file that a sibling's bind-mount holds.
            let staging_root = std::env::temp_dir().join(format!(
                "staging_recoverable_{}_{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            std::fs::create_dir_all(&staging_root).unwrap();

            let mut config = election_config("staging-unresolvable");
            config.src_network = Some(staging_root.clone());
            let (mut sec, log) =
                super::super::test_helpers::make_secondary_recording(config, 1);
            sec.set_bootstrap_primary_id("setup".to_string());
            sec.enter_operational_for_test();

            // An ABSOLUTE local_path that does NOT exist + a 16-char
            // identifier hash (NOT a content SHA) + an empty staging dir ⇒
            // `resolve_for_dispatch` returns None and the staging guard
            // (`src_network.is_some()`) fires. The wire `worker_id` is
            // echoed back verbatim.
            let binary = super::processing::make_binary("staging-missing", 50);
            let file_hash = format!("hash_{}", binary.identifier.0);
            let assignment = DistributedMessage::TaskAssignment {
                target: None,
                sender_id: "setup".into(),
                timestamp: 0.0,
                secondary_id: "staging-unresolvable".into(),
                worker_id: 2,
                zip_file: None,
                binary_info:
                    dynrunner_protocol_primary_secondary::DistributedBinaryInfo::from_task_info(
                        &binary,
                    ),
                local_path: binary.path.to_string_lossy().into_owned(),
                file_hash: file_hash.clone(),
                predecessor_outputs: std::collections::BTreeMap::new(),
                supplanted_holder: None,
            };

            let mut factory = super::super::test_helpers::FakeWorkerFactory;
            let result = sec.dispatch_message(assignment, &mut factory).await;
            sec.drain_egress().await;
            assert!(result.is_ok(), "dispatch must not error, got {result:?}");

            // The decisive assertion: the report MUST be Recoverable so the
            // primary re-routes the task to a staged member instead of
            // losing it. Pre-fix this was NonRecoverable and the task was
            // permanently lost.
            let recoverable = log.borrow().iter().any(|m| {
                matches!(
                    m,
                    DistributedMessage::TaskFailed {
                        error_type: dynrunner_core::ErrorType::Recoverable,
                        task_hash,
                        worker_id,
                        ..
                    } if *task_hash == file_hash && *worker_id == 2
                )
            });
            assert!(
                recoverable,
                "a staging-mode node that cannot resolve a file locally must \
                 report it RECOVERABLE (reinjectable to a staged peer), not \
                 lose it as NonRecoverable; captured sends: {:?}",
                log.borrow(),
            );
            // And it must NOT also have emitted a NonRecoverable for the same
            // hash (the classification is exactly one).
            let nonrecoverable = log.borrow().iter().any(|m| {
                matches!(
                    m,
                    DistributedMessage::TaskFailed {
                        error_type: dynrunner_core::ErrorType::NonRecoverable,
                        task_hash,
                        ..
                    } if *task_hash == file_hash
                )
            });
            assert!(
                !nonrecoverable,
                "the staging-mode unresolvable report must not be \
                 NonRecoverable; captured sends: {:?}",
                log.borrow(),
            );

            let _ = std::fs::remove_dir_all(&staging_root);
        })
        .await;
}
