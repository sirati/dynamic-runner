#![cfg(test)]

use super::super::test_helpers::{
    FixedEstimator, NoPeers, TestId, TestTransport, election_config, make_secondary, make_transport,
};
use super::super::*;
use dynrunner_core::TaskInfo;
use dynrunner_scheduler::ResourceStealingScheduler;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

/// Construct a 3-node-mesh-analogous joiner: a single
/// `SecondaryCoordinator` configured as observer
/// (`is_observer=true`, `num_workers=0`). The "rest of the cluster"
/// shows up purely as the snapshot the test hands it. The
/// `NoPeers` mesh stub (peer_count=0) is what `make_secondary`
/// uses elsewhere; the late-joiner code path the test cares about
/// (restore + skip-setup) runs to completion regardless of peer
/// reachability — peer membership is asserted on the role-table
/// side, not the transport side.
fn make_observer_secondary(
    observer_id: &str,
) -> SecondaryCoordinator<
    TestTransport<NoPeers>,
    dynrunner_transport_channel::ChannelManagerEnd,
    ResourceStealingScheduler,
    FixedEstimator,
    TestId,
> {
    // The observer holds the `NoPeers` mesh stub directly; it restores
    // from a snapshot, skips setup, and reads its terminal cue from
    // `cluster_state` (RunComplete applied directly) — it never needs a
    // primary uplink inbound, so dropping the uplink does not change what
    // the late-joiner-observer test exercises.
    let mut config = election_config(observer_id);
    config.is_observer = true;
    config.num_workers = 0;
    SecondaryCoordinator::new(
        config,
        make_transport(NoPeers),
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    )
}

/// Build a synthetic `ClusterStateSnapshot<TestId>` carrying two
/// pending tasks, a designated `current_primary`, primary_epoch=7,
/// and one observer id. The same shape the wire frame's
/// `snapshot_json` decodes to.
fn make_synthetic_snapshot() -> crate::cluster_state::ClusterStateSnapshot<TestId> {
    use crate::cluster_state::TaskState;
    let mut tasks = HashMap::new();
    let mk_pending = |path: &str, ident: &str| TaskState::Pending {
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
            resolved_path: None,
        },
    };
    tasks.insert("task-1".to_string(), mk_pending("/tmp/task-1", "task-1"));
    tasks.insert("task-2".to_string(), mk_pending("/tmp/task-2", "task-2"));
    let mut observers = HashSet::new();
    observers.insert("observer-peer".to_string());
    crate::cluster_state::ClusterStateSnapshot {
        tasks,
        current_primary: Some("primary-peer".to_string()),
        primary_epoch: 7,
        phase_deps: HashMap::new(),
        observers,
        peer_holdings: HashMap::new(),
        task_outputs: HashMap::new(),
        secondary_capacities: HashMap::new(),
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
    let mut sec = make_observer_secondary("observer-1");

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
    // authority — the joiner's role cache (read via the
    // PeerTransport hook registered in `new()`) now knows
    // who's primary, so Address::Role(Role::Primary) dispatches
    // resolve immediately rather than failing with
    // "role-table cache empty".
    assert_eq!(sec.cluster_state.current_primary(), Some("primary-peer"),);
    assert_eq!(sec.cluster_state.primary_epoch(), 7);

    // Observer set merged — Step 7's election filter will skip
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
    let mut sec = make_observer_secondary("observer-1");
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
    let mut sec = make_observer_secondary("observer-1");
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

/// Observer config sanity: an observer's `is_observer=true` flag
/// reaches the coordinator's `config` so downstream consumers
/// (the election filter at `election.rs::run_election_tick`'s
/// `we_lead` branch, the dispatch defensive reject at
/// `dispatch.rs::handle_promote_primary`) see the flag.
#[test]
fn observer_config_propagated_to_coordinator() {
    let sec = make_observer_secondary("observer-1");
    assert!(
        sec.config.is_observer,
        "observer flag must be readable on the coordinator's config — \
             election + dispatch defensive paths both consult it"
    );
    assert_eq!(
        sec.config.num_workers, 0,
        "observer's num_workers must be 0 (no work to take on)"
    );
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
            let mut sec = make_observer_secondary("observer-1");
            sec.restore_from_snapshot_and_skip_setup(make_synthetic_snapshot());
            // Deterministic exit cue: the cluster has declared the
            // run finished. The observer's SOLE exit cue is
            // `run_complete()`.
            sec.cluster_state
                .apply(dynrunner_protocol_primary_secondary::ClusterMutation::RunComplete);
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
                matches!(outcome, RunOutcome::Done),
                "observer must reach Done on run_complete(); got {outcome:?}",
            );
        })
        .await;
}

/// Regression: a `TaskAssignment` reaching a 0-worker `Operational`
/// node MUST be a graceful no-op, NOT a panic / `u32` underflow.
///
/// A late-joiner / observer (and any node that ends a phase as a
/// 0-worker observer) constructs `Operational` with an EMPTY pool —
/// `operational_observer` installs `WorkerPool::new()` — and that
/// state IS on the inbound dispatch path. Pre-fix the
/// `TaskAssignment` arm computed
/// `worker_id.min(pool.workers.len() as u32 - 1)`; with `len() == 0`
/// the `0u32 - 1` underflowed (debug-panic / release-`u32::MAX` →
/// out-of-bounds index into the empty slice). The fix guards the
/// empty pool BEFORE any indexing and returns `Ok(())`.
///
/// `is_observer=false` deliberately: this is the GENERAL 0-worker
/// `Operational` case (a phase-end observer or a worker that spawned
/// no slots), not specifically the observer ROLE — the underflow is a
/// pool-cardinality bug, independent of role.
#[tokio::test(flavor = "current_thread")]
async fn task_assignment_to_zero_worker_operational_node_is_noop_not_underflow() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // 0-worker node: `num_workers=0`, landed in `Operational`
            // with the empty pool `operational_observer` installs (the
            // same empty-pool shape the production late-joiner /
            // phase-end-observer flow produces). No pool is installed
            // after `enter_operational_for_test`, so `pool.workers` is
            // empty.
            let mut sec = make_secondary(election_config("zero-worker-op"));
            sec.enter_operational_for_test();
            assert_eq!(
                sec.op_mut().pool.workers.len(),
                0,
                "precondition: the operational node has a 0-worker pool",
            );

            // Fabricate the wire shape a primary would send. `worker_id`
            // is intentionally a normal index (0) — the underflow was in
            // the pool-length clamp, not in the requested id, so even a
            // valid-looking `worker_id` triggered it on an empty pool.
            let binary = super::processing::make_binary("orphan-task", 50);
            let file_hash = format!("hash_{}", binary.identifier.0);
            let assignment = DistributedMessage::TaskAssignment {
                sender_id: "primary".into(),
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
            };

            // The critical call: pre-fix this panicked (debug) / indexed
            // out of bounds (release) on the empty pool. Post-fix it
            // returns Ok as a no-op.
            let mut factory = super::super::test_helpers::FakeWorkerFactory;
            let result = sec.dispatch_message(assignment, &mut factory).await;
            assert!(
                result.is_ok(),
                "TaskAssignment to a 0-worker Operational node must be a \
                 graceful no-op, got {result:?}",
            );

            // And it must NOT have spuriously recorded an active task —
            // there was no worker to assign to.
            assert!(
                sec.op_mut().active_tasks.is_empty(),
                "no worker exists, so nothing should be tracked as active; \
                 active_tasks={:?}",
                sec.op_mut().active_tasks,
            );
        })
        .await;
}
