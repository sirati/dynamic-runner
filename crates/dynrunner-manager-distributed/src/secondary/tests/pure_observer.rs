//! Fresh pure-observer-role tests.
//!
//! The pure observer is the single zero-authority role the demoted node
//! and the late-joiner observer collapse into. This suite pins the
//! P3 invariants directly, with no authority-mirror state in sight:
//!
//!   1. An observer ORIGINATES NOTHING: no keepalive, no
//!      TaskCompleted/TaskFailed, no command-channel, no CRDT mutation.
//!      (The resource-holdings announcer is a SEPARATE opt-in
//!      capability, never attached on the strict-observer path.)
//!   2. An observer HOLDS THE FULL CRDT: terminal state is read ONLY
//!      from `cluster_state.outcome_counts()` / `run_complete()`, never
//!      a per-node counter.
//!   3. An observer EXITS ONLY on `run_complete()`.
//!   4. A late-joining OBSERVER and a late-joining WORKER each get the
//!      full snapshot with the CORRECT role.
//!   5. N concurrent observers all converge on the same full CRDT.
//!
//! Determinism: every test asserts a synchronous predicate or drives
//! the run loop to a deterministic `run_complete()` exit. No unbounded
//! loop is raced against a wall-clock timeout.

#![cfg(test)]

use super::super::test_helpers::{
    election_config, make_transport, FakeWorkerFactory, FixedEstimator, RecordingPeer, TestId,
    TestTransport,
};
use super::super::*;
use dynrunner_core::TaskInfo;
use dynrunner_protocol_primary_secondary::{ClusterMutation, DistributedMessage};
use dynrunner_scheduler::ResourceStealingScheduler;
use dynrunner_transport_channel::ChannelPrimaryTransportEnd;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::rc::Rc;
use std::cell::RefCell;
use tokio::sync::mpsc as tokio_mpsc;

/// Build an observer-mode SecondaryCoordinator (`is_observer=true`,
/// `num_workers=0`) over the unified transport with a `RecordingPeer`
/// mesh stub, so a test can assert on EVERYTHING the observer would
/// have put on the wire. Returns the coordinator + the shared
/// broadcast log + the secondary→primary uplink receiver (so the test
/// can also assert nothing went to the primary role).
#[allow(clippy::type_complexity)]
fn make_observer_with_recorder(
    observer_id: &str,
) -> (
    SecondaryCoordinator<
        TestTransport<RecordingPeer<TestId>>,
        dynrunner_transport_channel::ChannelManagerEnd,
        ResourceStealingScheduler,
        FixedEstimator,
        TestId,
    >,
    Rc<RefCell<Vec<DistributedMessage<TestId>>>>,
    tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
) {
    let (sec_to_pri_tx, sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
    let (_pri_to_sec_tx, pri_to_sec_rx) =
        tokio_mpsc::unbounded_channel::<DistributedMessage<TestId>>();
    let uplink = ChannelPrimaryTransportEnd {
        tx: sec_to_pri_tx,
        rx: pri_to_sec_rx,
    };
    let recorder = RecordingPeer::<TestId>::new(1);
    let peer_log = recorder.log_handle();
    let mut config = election_config(observer_id);
    config.is_observer = true;
    config.num_workers = 0;
    let sec = SecondaryCoordinator::new(
        config,
        make_transport(observer_id, uplink, recorder),
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    (sec, peer_log, sec_to_pri_rx)
}

/// A snapshot with `n_pending` pending tasks + `n_completed` completed
/// tasks, a designated primary, and an observer set. The "rest of the
/// cluster" the late-joiner restores from.
fn snapshot_with(
    n_pending: usize,
    n_completed: usize,
    observers: &[&str],
) -> crate::cluster_state::ClusterStateSnapshot<TestId> {
    use crate::cluster_state::TaskState;
    let mk_task = |ident: &str| TaskInfo {
        path: PathBuf::from(format!("/tmp/{ident}")),
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
    };
    let mut tasks = HashMap::new();
    for i in 0..n_pending {
        let id = format!("pending-{i}");
        tasks.insert(id.clone(), TaskState::Pending { task: mk_task(&id) });
    }
    for i in 0..n_completed {
        let id = format!("done-{i}");
        tasks.insert(
            id.clone(),
            TaskState::Completed {
                task: mk_task(&id),
            },
        );
    }
    crate::cluster_state::ClusterStateSnapshot {
        tasks,
        current_primary: Some("primary-peer".to_string()),
        primary_epoch: 3,
        phase_deps: HashMap::new(),
        observers: observers.iter().map(|s| s.to_string()).collect(),
        peer_holdings: HashMap::new(),
        task_outputs: HashMap::new(),
        secondary_capacities: HashMap::new(),
    }
}

/// (1)+(2): a pure observer, after restoring the full CRDT and ticking
/// every periodic path, ORIGINATES NOTHING on the mesh OR to the
/// primary role, and reports terminal state ONLY from the CRDT.
#[tokio::test(flavor = "current_thread")]
async fn observer_originates_nothing_and_reads_terminal_state_from_crdt() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut sec, peer_log, mut uplink_rx) =
                make_observer_with_recorder("observer-1");
            // Full CRDT: 2 pending + 3 completed.
            sec.restore_from_snapshot_and_skip_setup(snapshot_with(2, 3, &["observer-1"]));

            // Drive the keepalive-tick origination paths directly. An
            // observer suppresses its keepalive (no liveness assertion),
            // and the election self-excludes (no candidate broadcast).
            sec.send_keepalive().await;
            sec.check_peer_timeouts();
            let actions = sec.run_election_tick();

            // (1) NOTHING on the mesh.
            assert!(
                peer_log.borrow().is_empty(),
                "observer must originate NOTHING on the mesh; got {:?}",
                peer_log.borrow()
            );
            // (1) NOTHING to the primary role (the uplink stays silent).
            assert!(
                uplink_rx.try_recv().is_err(),
                "observer must send NOTHING to the primary role"
            );
            // (1) The election produced no broadcast (observer never
            //     self-promotes).
            assert!(
                actions.broadcast.is_empty(),
                "observer election tick must originate no broadcast"
            );

            // (2) Terminal state is the CRDT's, read via the
            //     CRDT-backed accessor — 3 completed.
            assert_eq!(
                sec.completed_count(),
                3,
                "observer reads terminal state from the replicated CRDT"
            );
        })
        .await;
}

/// (3): the observer's SOLE exit cue is `run_complete()`. With the flag
/// set and num_workers=0 (active_tasks always empty), the observe loop
/// exits `Done` on its first iteration — deterministically.
#[tokio::test(flavor = "current_thread")]
async fn observer_exits_only_on_run_complete() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut sec, peer_log, _uplink_rx) =
                make_observer_with_recorder("observer-1");
            sec.restore_from_snapshot_and_skip_setup(snapshot_with(0, 1, &["observer-1"]));
            sec.cluster_state.apply(ClusterMutation::RunComplete);
            assert!(sec.cluster_state.run_complete());

            let mut factory = FakeWorkerFactory;
            let outcome = sec
                .run_until_setup_or_done(&mut factory)
                .await
                .expect("observer run loop must not Err");
            assert!(
                matches!(outcome, RunOutcome::Done),
                "observer exits Done on run_complete(); got {outcome:?}"
            );
            // Even across the full run, the observer originated nothing.
            assert!(
                peer_log.borrow().is_empty(),
                "observer originated mesh traffic during its run; got {:?}",
                peer_log.borrow()
            );
        })
        .await;
}

/// (4a): a late-joining OBSERVER restores the full snapshot with the
/// observer role projected into `role_table.observers`.
#[test]
fn late_joining_observer_gets_full_snapshot_with_observer_role() {
    let (mut sec, _peer_log, _uplink_rx) = make_observer_with_recorder("late-obs");
    assert_eq!(sec.cluster_state.task_count(), 0);

    sec.restore_from_snapshot_and_skip_setup(snapshot_with(2, 3, &["peer-observer"]));

    // Full CRDT: all 5 tasks present.
    assert_eq!(
        sec.cluster_state.task_count(),
        5,
        "late-joining observer must restore the FULL task ledger"
    );
    // Role correct: the snapshot's observer projection is present.
    assert!(
        sec.cluster_state
            .role_table()
            .observers
            .contains("peer-observer"),
        "observer role must survive the snapshot restore"
    );
    // Setup-skip latched.
    assert!(sec.setup_phase_completed);
}

/// (4b): a late-joining WORKER restores the same full snapshot but is
/// NOT mis-projected as an observer. The worker fixture restores with
/// an empty observer set in the snapshot (it is not in it), proving the
/// restore carries the worker's actual (non-observer) role.
#[test]
fn late_joining_worker_gets_full_snapshot_without_observer_role() {
    // A WORKER late-joiner: a regular (non-observer) secondary.
    let (sec_to_pri_tx, _sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
    let (_pri_to_sec_tx, pri_to_sec_rx) =
        tokio_mpsc::unbounded_channel::<DistributedMessage<TestId>>();
    let uplink = ChannelPrimaryTransportEnd {
        tx: sec_to_pri_tx,
        rx: pri_to_sec_rx,
    };
    let config = election_config("late-worker"); // is_observer defaults false
    let mut sec: SecondaryCoordinator<
        TestTransport<super::super::test_helpers::NoPeers>,
        dynrunner_transport_channel::ChannelManagerEnd,
        ResourceStealingScheduler,
        FixedEstimator,
        TestId,
    > = SecondaryCoordinator::new(
        config,
        make_transport("late-worker", uplink, super::super::test_helpers::NoPeers),
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );

    // Snapshot carries an observer set that does NOT include this
    // worker — proving its non-observer role is preserved.
    sec.restore_from_snapshot_and_skip_setup(snapshot_with(2, 3, &["some-other-observer"]));

    assert_eq!(
        sec.cluster_state.task_count(),
        5,
        "late-joining worker must restore the FULL task ledger"
    );
    assert!(
        !sec.cluster_state
            .role_table()
            .observers
            .contains("late-worker"),
        "worker must NOT be mis-projected into the observer set"
    );
    assert!(!sec.config.is_observer, "worker config carries is_observer=false");
}

/// (5): N concurrent observers each restore the SAME full CRDT
/// independently — set semantics, no cross-talk. Pins that any number
/// of observers is supported (the CRDT is replicated per-node).
#[test]
fn n_concurrent_observers_all_hold_the_full_crdt() {
    let snapshot = snapshot_with(4, 6, &["obs-a", "obs-b", "obs-c"]);
    let ids = ["obs-a", "obs-b", "obs-c", "obs-d"];
    let mut observers: Vec<_> = ids
        .iter()
        .map(|id| make_observer_with_recorder(id))
        .collect();

    for (sec, _log, _rx) in observers.iter_mut() {
        sec.restore_from_snapshot_and_skip_setup(snapshot.clone());
    }

    // Every observer converged on the identical full CRDT.
    for (sec, _log, _rx) in &observers {
        assert_eq!(
            sec.cluster_state.task_count(),
            10,
            "each concurrent observer holds the full 10-task ledger"
        );
        assert_eq!(
            sec.completed_count(),
            6,
            "each concurrent observer reads 6 completed from the CRDT"
        );
        let projected: HashSet<&str> = sec
            .cluster_state
            .role_table()
            .observers
            .iter()
            .map(|s| s.as_str())
            .collect();
        assert!(
            projected.contains("obs-a")
                && projected.contains("obs-b")
                && projected.contains("obs-c"),
            "each observer holds the full observer set; got {projected:?}"
        );
    }
}

/// P3 replication-invariant fatality (R3-audit gap): a `ClusterSnapshot`
/// whose JSON payload fails to deserialize is a HARD error, not a
/// swallow. A bootstrapping observer requested the snapshot precisely to
/// populate its CRDT; continuing to "observe" an un-restored (empty)
/// CRDT would report a lie (premature run-complete, wrong counts). The
/// `ClusterSnapshot` arm must latch `fatal_exit` so the operational loop
/// aborts the run instead. Pins: (a) the malformed payload does NOT
/// restore (the CRDT stays empty), and (b) `fatal_exit` is set with the
/// malformed-payload reason.
#[tokio::test(flavor = "current_thread")]
async fn malformed_cluster_snapshot_latches_fatal_exit() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut observer, _peer_log, _uplink_rx) =
                make_observer_with_recorder("obs-malformed");

            // Precondition: nothing restored yet.
            assert_eq!(
                observer.cluster_state.task_count(),
                0,
                "fresh observer starts with an empty CRDT"
            );
            assert!(
                observer.fatal_exit.is_none(),
                "fresh observer has no fatal_exit latched"
            );

            // A ClusterSnapshot whose payload is not a valid
            // `ClusterStateSnapshot<TestId>` JSON. `serde_json::from_str`
            // rejects it; the arm must latch `fatal_exit` rather than
            // silently leave the CRDT empty.
            let mut factory = FakeWorkerFactory;
            observer
                .handle_inbound(
                    DistributedMessage::ClusterSnapshot {
                        sender_id: "primary-peer".into(),
                        timestamp: 0.0,
                        snapshot_json: "{ this is not valid snapshot json ]".into(),
                    },
                    &mut factory,
                )
                .await;

            // (a) The malformed payload did NOT restore anything.
            assert_eq!(
                observer.cluster_state.task_count(),
                0,
                "a malformed snapshot must NOT partially restore the CRDT"
            );
            // (b) The fatality latched.
            let reason = observer
                .fatal_exit
                .as_ref()
                .expect("malformed ClusterSnapshot must latch fatal_exit");
            assert!(
                reason.contains("malformed snapshot"),
                "fatal_exit reason should name the malformed snapshot; got: {reason}"
            );
        })
        .await;
}
