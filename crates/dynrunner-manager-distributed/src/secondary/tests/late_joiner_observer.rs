#![cfg(test)]

use super::super::test_helpers::{
    FixedEstimator, NoPeers, TestId, TestTransport, election_config, make_transport,
};
use super::super::*;
use dynrunner_core::TaskInfo;
use dynrunner_protocol_primary_secondary::DistributedMessage;
use dynrunner_scheduler::ResourceStealingScheduler;
use dynrunner_transport_channel::ChannelPrimaryTransportEnd;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use tokio::sync::mpsc as tokio_mpsc;

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
    let (sec_to_pri_tx, _sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
    let (_pri_to_sec_tx, pri_to_sec_rx) =
        tokio_mpsc::unbounded_channel::<DistributedMessage<TestId>>();
    let uplink = ChannelPrimaryTransportEnd {
        tx: sec_to_pri_tx,
        rx: pri_to_sec_rx,
    };
    let mut config = election_config(observer_id);
    config.is_observer = true;
    config.num_workers = 0;
    SecondaryCoordinator::new(
        config,
        make_transport(observer_id, uplink, NoPeers),
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
    assert!(!sec.setup_phase_completed);
    assert_eq!(sec.cluster_state.task_count(), 0);
    assert!(sec.cluster_state.current_primary().is_none());
    assert!(sec.cluster_state.role_table().observers.is_empty());

    sec.restore_from_snapshot_and_skip_setup(make_synthetic_snapshot());

    // Latch is set — `run_until_setup_or_done` will skip the
    // entire `!setup_phase_completed` setup block on its next
    // call.
    assert!(sec.setup_phase_completed);

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

    assert!(sec.setup_phase_completed, "latch stays true");
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
