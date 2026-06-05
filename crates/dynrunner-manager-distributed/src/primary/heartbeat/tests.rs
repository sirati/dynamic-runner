use std::collections::HashMap;
use std::time::Duration;

use dynrunner_core::{
    BoundedString, PhaseId, ResourceMap, SoftPreferredSecondaries, TaskInfo, TypeId,
};
use dynrunner_protocol_primary_secondary::{ClusterMutation, DistributedMessage, RemovalCause};
use dynrunner_scheduler::ResourceStealingScheduler;
use dynrunner_transport_channel::ChannelPeerTransport;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc as tokio_mpsc;

use crate::primary::{PrimaryConfig, PrimaryCoordinator};
use crate::state::{SecondaryConnection, SecondaryConnectionState};
use dynrunner_scheduler_api::{PendingPool, ResourceEstimator};

/// Test fixture: install an empty pool with a single "default" phase
/// onto a freshly-constructed primary. Mirrors what `run()` does in
/// production; tests that exercise post-initialisation paths
/// (heartbeat re-queue, etc.) need this so `pool_mut()` doesn't
/// panic.
fn install_default_pool<Tr, S, E>(primary: &mut PrimaryCoordinator<Tr, S, E, TestId>)
where
    Tr: dynrunner_protocol_primary_secondary::PeerTransport<TestId>,
    S: dynrunner_scheduler_api::Scheduler<TestId>,
    E: ResourceEstimator<TestId>,
{
    let phase = PhaseId::from("default");
    let pool = PendingPool::<TestId>::new([phase.clone()], std::collections::HashMap::new())
        .expect("default-phase pool");
    primary.pending = Some(pool);
    primary.phase_completed.insert(phase.clone(), 0);
    primary.phase_failed.insert(phase, 0);
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct TestId(String);

/// Build a `TaskInfo` in the "default" phase/type with the given label as
/// both identifier and `task_id`, plus optional `(prereq_phase,
/// prereq_task_id)` task-level deps. Mirrors the verbose literal the other
/// tests in this file inline, factored out so the policy tests stay short.
fn task(label: &str, depends_on: &[(&str, &str)]) -> TaskInfo<TestId> {
    TaskInfo {
        path: std::path::PathBuf::from(format!("{label}.bin")),
        size: 100,
        identifier: TestId(label.into()),
        phase_id: PhaseId::from("default"),
        type_id: TypeId::from("default"),
        affinity_id: None,
        payload: serde_json::Value::Null,
        task_id: label.into(),
        task_depends_on: depends_on
            .iter()
            .map(|(p, id)| dynrunner_core::TaskDep {
                task_id: (*id).to_string(),
                phase_id: PhaseId::from(*p),
                inherit_outputs: false,
            })
            .collect(),
        preferred_secondaries: SoftPreferredSecondaries::default(),
        preferred_version: Default::default(),
        resolved_path: None,
    }
}

#[derive(Clone)]
struct FixedEstimator;
impl ResourceEstimator<TestId> for FixedEstimator {
    fn estimate(&self, _task: &dynrunner_core::TaskInfo<TestId>) -> ResourceMap {
        ResourceMap::from([(dynrunner_core::ResourceKind::memory(), 1)])
    }
}

fn config(keepalive_interval: Duration, miss_threshold: u32) -> PrimaryConfig {
    PrimaryConfig {
        connect_timeout: Duration::from_secs(5),
        peer_timeout: Duration::from_secs(5),
        keepalive_interval,
        keepalive_miss_threshold: miss_threshold,
        mesh_ready_timeout: std::time::Duration::from_secs(5),
        // Tiny keepalive-interval-relative silence schedule so a brief
        // real-time sleep crosses the stages: WARN at 1x, HARD backstop
        // at 2x the interval. At `keepalive_interval = 50ms` the 200ms
        // sleeps these tests use cross the 100ms hard backstop.
        silence_warn_multiples: vec![1],
        silence_hard_multiple: 2,
        ..PrimaryConfig::default()
    }
}

fn empty_transport() -> (
    ChannelPeerTransport<TestId>,
    tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
    tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
) {
    let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
    let (sec_tx, sec_rx) = tokio_mpsc::unbounded_channel();
    let mut outgoing = HashMap::new();
    outgoing.insert("dead-sec".into(), sec_tx);
    (
        ChannelPeerTransport::from_raw_channels("primary".into(), outgoing, incoming_rx),
        sec_rx,
        incoming_tx,
    )
}

/// Build a primary with one registered secondary that owns one in-flight
/// task; advance time past the death threshold; verify the heartbeat
/// report flags the secondary as dead and `requeue_dead_secondary`
/// requeues the task and drops the worker.
#[tokio::test(flavor = "current_thread")]
async fn dead_secondary_requeues_in_flight_task() {
    let (transport, _sec_rx, _kept_alive_for_outgoing_clone) = empty_transport();
    let mut primary: PrimaryCoordinator<_, _, _, TestId> = PrimaryCoordinator::new(
        config(Duration::from_millis(50), 2),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator,
    );
    install_default_pool(&mut primary);

    // Register the secondary at the connection level. Drive
    // through the full handshake → operational state machine
    // because the heartbeat-monitor only applies the deadline
    // to Operational secondaries (pre-Operational means setup
    // is still in progress; the secondary's own keepalive
    // sender hasn't started yet, so falsely declaring dead
    // would drop a healthy node mid-setup).
    let conn = SecondaryConnection::new("dead-sec".into())
        .receive_welcome(1, vec![], "host".into(), 0, None, false, false)
        .receive_cert_exchange(String::new(), None, None, 0)
        .begin_peer_discovery()
        .peers_ready()
        .assignments_sent();
    primary.secondaries.insert(
        "dead-sec".into(),
        SecondaryConnectionState::Operational(conn),
    );
    primary.seed_keepalive("dead-sec");

    // Stage one in-flight task on a single virtual worker.
    let in_flight = TaskInfo {
        path: std::path::PathBuf::from("victim.bin"),
        size: 100,
        identifier: TestId("victim".into()),
        phase_id: PhaseId::from("default"),
        type_id: TypeId::from("default"),
        affinity_id: None,
        payload: serde_json::Value::Null,
        task_id: "victim".into(),
        task_depends_on: vec![],
        preferred_secondaries: SoftPreferredSecondaries::default(),
        preferred_version: Default::default(),
        resolved_path: None,
    };
    primary.stage_in_flight_for_test("dead-sec".into(), 0, in_flight.clone());

    // Sleep past the HARD backstop (2x the 50ms interval = 100ms) so the
    // staged tick declares the secondary dead, then drive one tick.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let report = primary.collect_heartbeat_report();
    assert_eq!(
        report.silences.len(),
        1,
        "one Operational secondary tracked"
    );
    assert_eq!(report.silences[0].secondary_id, "dead-sec");
    primary.process_heartbeat_tick().await.unwrap();

    assert_eq!(primary.workers.len(), 0, "dead worker should be evicted");
    // After requeue, the in-flight item is back in the pool (queued),
    // not in_flight.
    assert_eq!(primary.pool().len(), 1, "in-flight task requeued");
    let requeued: Vec<_> = primary.pool().iter().collect();
    assert_eq!(requeued[0].identifier.0, "victim");
    assert!(!primary.secondaries.contains_key("dead-sec"));
}

/// Multi-secondary transport variant that pre-registers two
/// secondaries on the outgoing map. Used by the mass-death tests
/// because the singleton `empty_transport` only knows about
/// `dead-sec`, and `requeue_dead_secondary` walks the outgoing
/// table to fan `TimeoutDetected` to survivors.
// One-off test-helper return; the tuple shape is documented
// structurally by the field types and isn't reused elsewhere.
#[allow(clippy::type_complexity)]
fn two_secondary_transport() -> (
    ChannelPeerTransport<TestId>,
    Vec<tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>>,
    tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
) {
    let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
    let (a_tx, a_rx) = tokio_mpsc::unbounded_channel();
    let (b_tx, b_rx) = tokio_mpsc::unbounded_channel();
    let mut outgoing = HashMap::new();
    outgoing.insert("sec-a".into(), a_tx);
    outgoing.insert("sec-b".into(), b_tx);
    (
        ChannelPeerTransport::from_raw_channels("primary".into(), outgoing, incoming_rx),
        vec![a_rx, b_rx],
        incoming_tx,
    )
}

/// Helper: register a secondary in Operational state with a single
/// in-flight task. Mirrors the setup pattern of
/// `dead_secondary_requeues_in_flight_task` but parametrised by id
/// so the mass-death tests can stage two of them.
fn register_operational_secondary<Tr, S, E>(
    primary: &mut PrimaryCoordinator<Tr, S, E, TestId>,
    secondary_id: &str,
    worker_id: u32,
    in_flight_label: &str,
) where
    Tr: dynrunner_protocol_primary_secondary::PeerTransport<TestId>,
    S: dynrunner_scheduler_api::Scheduler<TestId>,
    E: ResourceEstimator<TestId>,
{
    let conn = SecondaryConnection::new(secondary_id.into())
        .receive_welcome(1, vec![], "host".into(), 0, None, false, false)
        .receive_cert_exchange(String::new(), None, None, 0)
        .begin_peer_discovery()
        .peers_ready()
        .assignments_sent();
    primary.secondaries.insert(
        secondary_id.into(),
        SecondaryConnectionState::Operational(conn),
    );
    primary.seed_keepalive(secondary_id);
    primary.stage_in_flight_for_test(
        secondary_id.into(),
        worker_id,
        TaskInfo {
            path: std::path::PathBuf::from(format!("{in_flight_label}.bin")),
            size: 100,
            identifier: TestId(in_flight_label.into()),
            phase_id: PhaseId::from("default"),
            type_id: TypeId::from("default"),
            affinity_id: None,
            payload: serde_json::Value::Null,
            task_id: in_flight_label.into(),
            task_depends_on: vec![],
            preferred_secondaries: SoftPreferredSecondaries::default(),
            preferred_version: Default::default(),
            resolved_path: None,
        },
    );
}

/// Drain `rx` non-blockingly and return every `PeerRemoved` mutation
/// observed in any `DistributedMessage::ClusterMutation` batch. The
/// primary's `apply_and_broadcast_cluster_mutations` helper fans the
/// broadcast across the transport's outgoing channel map, so any
/// receiver wired to that map sees the same payload. Used by the
/// PeerRemoved-origination tests to inspect the mutation primary
/// authored on death.
fn collect_peer_removed(
    rx: &mut tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
) -> Vec<(String, RemovalCause)> {
    let mut out = Vec::new();
    while let Ok(msg) = rx.try_recv() {
        if let DistributedMessage::ClusterMutation { mutations, .. } = msg {
            for m in mutations {
                if let ClusterMutation::PeerRemoved { id, cause } = m {
                    out.push((id, cause));
                }
            }
        }
    }
    out
}

/// Fatal-error path: a secondary explicitly reports a fatal error.
/// The primary originates `PeerRemoved { cause: FatalError(<msg>) }`
/// using `BoundedString::from(error)`. Oversized error strings are
/// truncated at the 1 KiB cap that `RemovalCause::FatalError`
/// carries, so a misbehaving secondary can't force unbounded
/// allocation on receivers.
#[tokio::test(flavor = "current_thread")]
async fn requeue_dead_secondary_emits_peer_removed_with_fatal_error_cause() {
    let (transport, mut sec_rxs, _incoming_tx) = two_secondary_transport();
    let mut primary: PrimaryCoordinator<_, _, _, TestId> = PrimaryCoordinator::new(
        config(Duration::from_millis(50), 2),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator,
    );
    install_default_pool(&mut primary);
    register_operational_secondary(&mut primary, "sec-a", 0, "victim-a");
    register_operational_secondary(&mut primary, "sec-b", 1, "victim-b");

    // Build an oversized error payload so the truncation guarantee
    // is exercised end-to-end (not just in the BoundedString unit
    // test).
    let huge = "x".repeat(4096);
    let fatal = DistributedMessage::<TestId>::SecondaryFatalError {
        sender_id: "sec-a".into(),
        timestamp: 0.0,
        secondary_id: "sec-a".into(),
        error: huge,
    };
    primary.handle_secondary_fatal_error(fatal).await.unwrap();

    let mut removed = collect_peer_removed(&mut sec_rxs[0]);
    removed.extend(collect_peer_removed(&mut sec_rxs[1]));
    removed.sort_by(|a, b| a.0.cmp(&b.0));
    removed.dedup();
    assert_eq!(removed.len(), 1, "exactly one PeerRemoved authored");
    assert_eq!(removed[0].0, "sec-a");
    match &removed[0].1 {
        RemovalCause::FatalError(s) => {
            // BoundedString<1024> truncates at construction; the
            // oversized input must be capped on the wire payload.
            assert_eq!(
                s.as_ref().len(),
                1024,
                "FatalError diagnostic must be truncated to 1024 bytes; \
                 got {} bytes",
                s.as_ref().len()
            );
            let expected: String = "x".repeat(1024);
            assert_eq!(s.as_ref(), expected);
        }
        other => panic!("expected FatalError cause; got {other:?}"),
    }
    // Silence unused-import warning for BoundedString — the
    // truncation invariant is checked via length above, but the
    // type itself is the load-bearing piece for that invariant.
    let _: BoundedString<1024> = BoundedString::from("anchor");
}

/// A secondary that's still sending keepalives stays in the routable
/// set even when other secondaries die.
#[tokio::test(flavor = "current_thread")]
async fn live_secondary_is_not_falsely_declared_dead() {
    let (transport, _sec_rx, _kept_alive_for_outgoing_clone) = empty_transport();
    let mut primary: PrimaryCoordinator<_, _, _, TestId> = PrimaryCoordinator::new(
        config(Duration::from_millis(50), 2),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator,
    );

    let conn = SecondaryConnection::new("dead-sec".into()).receive_welcome(
        1,
        vec![],
        "host".into(),
        0,
        None,
        false,
        false,
    );
    primary.secondaries.insert(
        "dead-sec".into(),
        SecondaryConnectionState::Handshaking(conn),
    );
    primary.seed_keepalive("dead-sec");

    // Bump the keepalive within the deadline window so the heartbeat
    // report excludes it: the secondary is Handshaking (pre-Operational),
    // so the Operational gate keeps it out of the silence sweep entirely.
    tokio::time::sleep(Duration::from_millis(60)).await;
    primary.record_keepalive("dead-sec");
    tokio::time::sleep(Duration::from_millis(60)).await;
    let report = primary.collect_heartbeat_report();
    assert_eq!(report.silences.len(), 0);
}

/// Drain `rx` non-blockingly and return the first `TaskAssignment`
/// observed, if any. The dispatch kickstart fans `TaskAssignment` to
/// the survivor's outgoing channel after the dead-secondary requeue;
/// the test that pins the kickstart contract uses this to assert the
/// recovered task actually re-targets a survivor (i.e. didn't sit in
/// the pool until the next external event).
fn first_task_assignment(
    rx: &mut tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
) -> Option<DistributedMessage<TestId>> {
    while let Ok(msg) = rx.try_recv() {
        if matches!(msg, DistributedMessage::TaskAssignment { .. }) {
            return Some(msg);
        }
    }
    None
}

/// Regression for the dispatch-stall after keepalive-driven recovery:
/// when the primary requeues an in-flight task from a dead secondary,
/// surviving idle workers do NOT auto-poll. Without re-dispatch at the
/// end of the requeue path the recovered task sits in the pool forever
/// — observed in the 2026-05-17 cohort run where the primary logged
/// `recovered_in_flight=1` after a 300 s keepalive timeout but never
/// re-emitted `task_request` to any idle peer, so the entire dispatch
/// chain stalled until the SLURM time-limit killed the wrapper.
///
/// Post dispatch-decoupling the requeue path no longer calls dispatch
/// directly: it EMITS a `WorkerMgmtSignal::TasksAdded` onto the
/// worker-management bus, and the operational loop's worker-management
/// `select!` arm runs the recheck that re-dispatches. This test drives
/// that recheck synchronously (drain the batch + call the reaction) —
/// the dispatch still happens, just via the batched recheck.
#[tokio::test(flavor = "current_thread")]
async fn requeue_dead_secondary_kickstarts_dispatch_to_idle_survivor() {
    let (transport, mut sec_rxs, _incoming_tx) = two_secondary_transport();
    let mut primary: PrimaryCoordinator<_, _, _, TestId> = PrimaryCoordinator::new(
        config(Duration::from_millis(50), 2),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator,
    );
    install_default_pool(&mut primary);

    // sec-a is the wedged secondary; it owns one in-flight task that
    // must be recovered into the pool and re-dispatched to sec-b.
    register_operational_secondary(&mut primary, "sec-a", 0, "victim-a");

    // sec-b is the survivor with an IDLE worker that has a non-zero
    // memory budget (FixedEstimator requires memory=1, so the budget
    // must exceed that). Without a budget the scheduler returns NoFit
    // and the test would falsely pass against a buggy primary.
    let sec_b_conn = SecondaryConnection::new("sec-b".into())
        .receive_welcome(1, vec![], "host".into(), 0, None, false, false)
        .receive_cert_exchange(String::new(), None, None, 0)
        .begin_peer_discovery()
        .peers_ready()
        .assignments_sent();
    primary.secondaries.insert(
        "sec-b".into(),
        SecondaryConnectionState::Operational(sec_b_conn),
    );
    primary.seed_keepalive("sec-b");
    primary.register_idle_worker_for_test(
        "sec-b".into(),
        1,
        ResourceMap::from([(
            dynrunner_core::ResourceKind::memory(),
            1024 * 1024 * 1024u64,
        )]),
    );

    // Install the worker-management bus so the requeue path's
    // `TasksAdded` emit lands on a receiver we drive the recheck from.
    let (wm_tx, mut wm_rx) =
        tokio_mpsc::unbounded_channel::<crate::worker_signal::WorkerMgmtSignal>();
    primary
        .cluster_state_mut_for_test()
        .install_worker_mgmt_sender(wm_tx);

    // Sleep past the keepalive deadline so sec-a is dead. Refresh
    // sec-b's keepalive immediately before the tick so only sec-a
    // ends up in the dead list — the surviving-peer shape the
    // single-death requeue takes in production.
    tokio::time::sleep(Duration::from_millis(200)).await;
    primary.record_keepalive("sec-b");
    primary.process_heartbeat_tick().await.unwrap();

    // sec-a is gone, sec-b survives, the recovered task is in the
    // pool. These three are independent of the kickstart contract —
    // they assert the requeue itself happened, so a regression in
    // the requeue path can't masquerade as a kickstart failure.
    assert!(
        !primary.secondaries.contains_key("sec-a"),
        "dead secondary must be removed"
    );
    assert!(
        primary.secondaries.contains_key("sec-b"),
        "survivor must remain"
    );

    // Deferred-recheck contract: the requeue path emitted a
    // `TasksAdded` rather than dispatching inline. Drain the coalesced
    // batch and run the worker-management reaction synchronously —
    // exactly what the operational loop's worker-management arm does.
    let batch =
        crate::worker_signal::drain_worker_signal_batch(&mut wm_rx, Duration::from_millis(50))
            .await
            .expect("dead-secondary requeue must emit a TasksAdded batch");
    assert!(
        batch
            .signals
            .contains(&crate::worker_signal::WorkerMgmtSignal::TasksAdded),
        "requeue path must emit TasksAdded; got {:?}",
        batch.signals
    );
    // Keep the survivor genuinely live across the reaction: in production
    // sec-b keeps sending keepalives, so it never looks silent. Without
    // the refresh the test's long pre-tick sleep would leave sec-b past
    // the first silence stage, and the dispatch-altitude lazy oracle would
    // (correctly) treat the freshly-assigned-to survivor as a silent
    // holder and evict it — a test artifact, not the kickstart contract.
    primary.record_keepalive("sec-b");
    primary.react_to_worker_signal_batch(batch).await;

    // The load-bearing assertion: sec-b's outgoing channel saw a
    // `TaskAssignment` — i.e. the recheck re-dispatched to the
    // surviving idle worker, the very signal the production run was
    // missing.
    let assignment = first_task_assignment(&mut sec_rxs[1]);
    assert!(
        assignment.is_some(),
        "survivor must receive TaskAssignment after dead-secondary requeue; \
         without the kickstart the recovered task hangs in the pool until \
         the next external event (which never came in the cohort run)"
    );
    if let Some(DistributedMessage::TaskAssignment { secondary_id, .. }) = assignment {
        assert_eq!(secondary_id, "sec-b");
    }
    // Post-dispatch the survivor's worker is no longer idle and the
    // recovered task is no longer in the queued bucket — symmetric
    // to the dispatch-success path elsewhere. `pool().len()` counts
    // queued + in-flight + blocked, so checking `iter()` (queued-
    // only) is the right shape: the task moved from queued to
    // in-flight on the kickstart's dispatch call.
    assert!(
        primary
            .workers
            .iter()
            .any(|w| w.secondary_id == "sec-b" && !w.is_idle()),
        "survivor's worker must flip to busy after the kickstart"
    );
    assert_eq!(
        primary.pool().iter().count(),
        0,
        "recovered task must leave the queued bucket via dispatch kickstart"
    );
}

/// R-1: a dead-secondary requeue transitions the CRDT entry
/// `InFlight → Pending` (via the `TaskRequeued` mutation
/// `recover_inflight_for_dead_secondary` produces and
/// `requeue_dead_secondary` broadcasts), so a snapshot taken after the
/// recovery — restored into a freshly-promoted primary — hydrates the
/// task into the pool and re-dispatches it EXACTLY once.
///
/// Without the `TaskRequeued` transition the local pool requeue would
/// have no CRDT counterpart: a stale `InFlight` would survive the
/// snapshot, `hydrate_from_cluster_state` would route it to the
/// in-flight ledger (NOT the pool), and the promoted primary would
/// never re-dispatch it — a lost task. The "exactly once" assertion
/// pins both failure modes: zero (the lost-task regression) and twice
/// (a stale-InFlight + pool double-count).
#[tokio::test(flavor = "current_thread")]
async fn r1_dead_secondary_requeue_then_hydrate_redispatches_exactly_once() {
    let (transport, _sec_rx, _kept_alive) = empty_transport();
    let mut primary: PrimaryCoordinator<_, _, _, TestId> = PrimaryCoordinator::new(
        config(Duration::from_millis(50), 2),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator,
    );
    install_default_pool(&mut primary);

    // Register the dead-to-be secondary, operational.
    let conn = SecondaryConnection::new("dead-sec".into())
        .receive_welcome(1, vec![], "host".into(), 0, None, false, false)
        .receive_cert_exchange(String::new(), None, None, 0)
        .begin_peer_discovery()
        .peers_ready()
        .assignments_sent();
    primary.secondaries.insert(
        "dead-sec".into(),
        SecondaryConnectionState::Operational(conn),
    );
    primary.seed_keepalive("dead-sec");

    // The victim task: dispatched (CRDT InFlight on dead-sec/w0) AND
    // present in the local in-flight ledger via the real
    // `commit_assignment` lifecycle. The hash is the content hash so
    // the CRDT key and the ledger key align (production dispatch always
    // keys both on `compute_task_hash`).
    let victim = TaskInfo {
        path: std::path::PathBuf::from("victim.bin"),
        size: 100,
        identifier: TestId("victim".into()),
        phase_id: PhaseId::from("default"),
        type_id: TypeId::from("default"),
        affinity_id: None,
        payload: serde_json::Value::Null,
        task_id: "victim".into(),
        task_depends_on: vec![],
        preferred_secondaries: SoftPreferredSecondaries::default(),
        preferred_version: Default::default(),
        resolved_path: None,
    };
    let victim_hash = primary.stage_in_flight_for_test("dead-sec".into(), 0, victim.clone());
    // Mirror the CRDT to InFlight, the state the live `TaskAssigned`
    // origination would have written at dispatch.
    {
        let cs = primary.cluster_state_mut_for_test();
        cs.apply(ClusterMutation::TaskAdded {
            hash: victim_hash.clone(),
            task: victim.clone(),
        });
        cs.apply(ClusterMutation::TaskAssigned {
            hash: victim_hash.clone(),
            secondary: "dead-sec".into(),
            worker: 0,

            version: Default::default(),
        });
    }
    assert!(
        matches!(
            primary.cluster_state_for_test().task_state(&victim_hash),
            Some(crate::cluster_state::TaskState::InFlight { .. })
        ),
        "victim starts InFlight in the CRDT"
    );

    // dead-sec dies → the recovery path requeues locally AND broadcasts
    // the `TaskRequeued` transition, applying it to the local CRDT.
    let dead = super::DeadSecondary {
        secondary_id: "dead-sec".into(),
        last_keepalive: std::time::Instant::now(),
    };
    primary
        .requeue_dead_secondary(dead, RemovalCause::KeepaliveMiss)
        .await
        .unwrap();

    // The CRDT entry is now Pending (InFlight → Pending), in lockstep
    // with the local pool requeue.
    assert!(
        matches!(
            primary.cluster_state_for_test().task_state(&victim_hash),
            Some(crate::cluster_state::TaskState::Pending { .. })
        ),
        "dead-secondary recovery must transition the CRDT InFlight → Pending"
    );

    // Snapshot the post-recovery ledger and restore it into a freshly-
    // promoted primary (the failover hydration path).
    let snapshot = primary.cluster_state_for_test().snapshot();

    let (transport2, _sec_rx2, _kept_alive2) = empty_transport();
    let mut promoted: PrimaryCoordinator<_, _, _, TestId> = PrimaryCoordinator::new(
        config(Duration::from_millis(50), 2),
        transport2,
        ResourceStealingScheduler::memory(),
        FixedEstimator,
    );
    promoted.cluster_state_mut_for_test().restore(snapshot);
    promoted.hydrate_from_cluster_state();

    // EXACTLY ONCE: the requeued task hydrates into the pool as a
    // dispatchable (queued) item — not stranded in the in-flight ledger
    // (zero), not double-counted (twice).
    let queued: Vec<_> = promoted.pool().iter().collect();
    assert_eq!(
        queued.len(),
        1,
        "the requeued task must hydrate as exactly one dispatchable pool item"
    );
    assert_eq!(queued[0].task_id, "victim");
    assert_eq!(
        promoted.in_flight_len_for_test(),
        0,
        "no stale in-flight ledger entry — the task is genuinely pending"
    );
    assert_eq!(
        promoted.pool().in_flight(&PhaseId::from("default")),
        0,
        "no phase in-flight counter held for the requeued task"
    );
}

// ======================================================================
// Honest staged silence-declaration policy
// ======================================================================

use super::{Stage, silence_stage};
use std::time::Instant;

/// PURE `silence_stage`: classifies a continuous silence into the highest
/// schedule stage it crossed — `None` below the first WARN, ascending WARN
/// stages, then `Hard` at the backstop. Schedule: WARN at 1x/2x, HARD at
/// 4x of a 10ms interval.
#[test]
fn silence_stage_classifies_into_highest_crossed_stage() {
    let interval = Duration::from_millis(10);
    let warn = [1u32, 2u32];
    let hard = 4u32;
    let now = Instant::now();
    let at = |ms: u64| now - Duration::from_millis(ms);

    // Below the first WARN multiple (1x = 10ms): no stage.
    assert_eq!(silence_stage(at(5), now, interval, &warn, hard), None);
    // Past 1x but below 2x: WARN(0).
    assert_eq!(
        silence_stage(at(15), now, interval, &warn, hard),
        Some(Stage::Warn(0))
    );
    // Past 2x but below the hard 4x: WARN(1) (highest crossed WARN).
    assert_eq!(
        silence_stage(at(25), now, interval, &warn, hard),
        Some(Stage::Warn(1))
    );
    // Past the hard 4x: Hard wins regardless of WARN crossings.
    assert_eq!(
        silence_stage(at(45), now, interval, &warn, hard),
        Some(Stage::Hard)
    );
}

/// Each WARN stage logs AT MOST ONCE per silence streak. The per-secondary
/// `silence_warn_stage` counter advances as stages fire; re-ticking at the
/// same stage does not re-arm it, and crossing a higher stage fires only
/// the not-yet-logged stages. A keepalive recovery resets the streak so the
/// stages re-arm from zero.
#[tokio::test(flavor = "current_thread")]
async fn warn_stages_fire_once_and_reset_on_recovery() {
    let (transport, _sec_rx, _kept) = empty_transport();
    // Two WARN stages (1x, 2x = 50ms, 100ms) below the hard backstop (10x
    // = 500ms) so a sub-500ms silence stays in WARN territory.
    let mut cfg = config(Duration::from_millis(50), 2);
    cfg.silence_warn_multiples = vec![1, 2];
    cfg.silence_hard_multiple = 10;
    let mut primary: PrimaryCoordinator<_, _, _, TestId> = PrimaryCoordinator::new(
        cfg,
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator,
    );
    install_default_pool(&mut primary);
    register_operational_secondary(&mut primary, "dead-sec", 0, "victim");

    // Cross WARN(0) only (>50ms, <100ms): the tick arms stage 0.
    tokio::time::sleep(Duration::from_millis(70)).await;
    primary.process_heartbeat_tick().await.unwrap();
    assert_eq!(
        primary.silence_warn_stage_for_test("dead-sec"),
        Some(1),
        "WARN(0) fired; counter advanced to 1 (next un-fired stage)"
    );
    // Tick again still inside the WARN(0)..WARN(1) band — no re-warn, no
    // counter change.
    primary.process_heartbeat_tick().await.unwrap();
    assert_eq!(
        primary.silence_warn_stage_for_test("dead-sec"),
        Some(1),
        "re-tick at the same stage must not re-warn"
    );

    // Cross WARN(1) (>100ms, <500ms): the tick arms stage 1 too, never
    // the hard backstop.
    tokio::time::sleep(Duration::from_millis(60)).await;
    primary.process_heartbeat_tick().await.unwrap();
    assert_eq!(
        primary.silence_warn_stage_for_test("dead-sec"),
        Some(2),
        "WARN(1) fired; counter advanced to 2"
    );
    assert!(
        primary.secondaries.contains_key("dead-sec"),
        "WARN stages are LOG-ONLY; the secondary is NOT declared dead"
    );

    // Recovery resets the streak: a fresh keepalive clears the staged
    // counter so the stages re-arm from zero.
    primary.record_keepalive("dead-sec");
    assert_eq!(
        primary.silence_warn_stage_for_test("dead-sec"),
        None,
        "keepalive recovery must reset the staged-WARN counter"
    );
}

/// The HARD backstop declares a secondary dead and requeues its in-flight
/// tasks REGARDLESS of dispatch state — there is no idle survivor to
/// kickstart here, yet the silent holder past the backstop is still
/// evicted. This is the forward-progress guarantee a purely starvation-
/// driven policy would lack.
#[tokio::test(flavor = "current_thread")]
async fn hard_backstop_declares_dead_regardless_of_dispatch_state() {
    let (transport, _sec_rx, _kept) = empty_transport();
    // Hard backstop at 2x the 50ms interval = 100ms.
    let mut primary: PrimaryCoordinator<_, _, _, TestId> = PrimaryCoordinator::new(
        config(Duration::from_millis(50), 2),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator,
    );
    install_default_pool(&mut primary);
    register_operational_secondary(&mut primary, "dead-sec", 0, "victim");

    // No idle survivor exists — the only worker is the dead-sec one. The
    // lazy oracle could not act (no idle worker to starve), so only the
    // hard backstop can recover. Cross it.
    tokio::time::sleep(Duration::from_millis(200)).await;
    primary.process_heartbeat_tick().await.unwrap();

    assert!(
        !primary.secondaries.contains_key("dead-sec"),
        "hard backstop must declare the silent secondary dead"
    );
    assert_eq!(primary.workers.len(), 0, "the dead worker is evicted");
    assert_eq!(
        primary.pool().iter().count(),
        1,
        "the in-flight task is requeued into the pool"
    );
}

/// The `Operational` gate spares a setup-phase (Handshaking) secondary:
/// even silent past the hard backstop, a pre-Operational secondary is
/// excluded from the silence sweep, so the staged tick never declares it
/// dead — a slow-handshaking SLURM secondary is not dropped mid-setup.
#[tokio::test(flavor = "current_thread")]
async fn operational_gate_spares_setup_phase_secondary() {
    let (transport, _sec_rx, _kept) = empty_transport();
    let mut primary: PrimaryCoordinator<_, _, _, TestId> = PrimaryCoordinator::new(
        config(Duration::from_millis(50), 2),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator,
    );
    install_default_pool(&mut primary);

    // Handshaking (pre-Operational), seeded keepalive far in the past.
    let conn = SecondaryConnection::new("slow-sec".into()).receive_welcome(
        1,
        vec![],
        "host".into(),
        0,
        None,
        false,
        false,
    );
    primary.secondaries.insert(
        "slow-sec".into(),
        SecondaryConnectionState::Handshaking(conn),
    );
    primary.seed_keepalive("slow-sec");

    // Way past the hard backstop — but it's not Operational.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let report = primary.collect_heartbeat_report();
    assert_eq!(
        report.silences.len(),
        0,
        "pre-Operational secondaries are excluded from the silence sweep"
    );
    primary.process_heartbeat_tick().await.unwrap();
    assert!(
        primary.secondaries.contains_key("slow-sec"),
        "a setup-phase secondary must NOT be declared dead by the schedule"
    );
}

/// Oracle TRUE: the only outstanding work is in-flight on a silent
/// secondary. No queued dispatchable work, nothing blocked, in-flight
/// non-empty, every in-flight entry held by a silent secondary.
#[tokio::test(flavor = "current_thread")]
async fn oracle_true_when_only_silent_held_work_remains() {
    let (transport, _sec_rx, _kept) = empty_transport();
    let mut primary: PrimaryCoordinator<_, _, _, TestId> = PrimaryCoordinator::new(
        config(Duration::from_millis(50), 2),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator,
    );
    install_default_pool(&mut primary);
    register_operational_secondary(&mut primary, "dead-sec", 0, "victim");

    // Silence past the first WARN stage (50ms) so dead-sec is "silent".
    tokio::time::sleep(Duration::from_millis(120)).await;
    assert!(
        primary.only_silent_held_work_remains(),
        "only-silent-held-work: in-flight held by a silent secondary, \
         nothing queued/blocked"
    );
}

/// Oracle FALSE corners — each one alone flips the predicate off, proving
/// the predicate is the conjunction the brief specifies (no corner is
/// load-bearing-by-accident).
#[tokio::test(flavor = "current_thread")]
async fn oracle_false_corners() {
    // (a) queued dispatchable work exists → false (don't evict; there is
    //     work an idle worker could still take).
    {
        let (transport, _r, _k) = empty_transport();
        let mut p: PrimaryCoordinator<_, _, _, TestId> = PrimaryCoordinator::new(
            config(Duration::from_millis(50), 2),
            transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator,
        );
        install_default_pool(&mut p);
        register_operational_secondary(&mut p, "dead-sec", 0, "victim");
        p.pool_mut().requeue(task("queued", &[]));
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert!(
            !p.only_silent_held_work_remains(),
            "queued dispatchable work present → oracle must be false"
        );
    }
    // (b) blocked > 0 → false (a blocked item will become dispatchable on
    //     prereq resolution; evicting now is premature).
    {
        let (transport, _r, _k) = empty_transport();
        let mut p: PrimaryCoordinator<_, _, _, TestId> = PrimaryCoordinator::new(
            config(Duration::from_millis(50), 2),
            transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator,
        );
        install_default_pool(&mut p);
        register_operational_secondary(&mut p, "dead-sec", 0, "victim");
        // Seed an in-flight prereq id (known but unresolved), then extend
        // a dependent — it lands in `blocked`, not a queued bucket.
        p.pool_mut()
            .mark_tasks_in_flight([("victim".to_string(), PhaseId::from("default"))]);
        p.pool_mut()
            .extend([task("child", &[("default", "victim")])])
            .expect("extend a dependent into blocked");
        assert_eq!(p.pool().blocked_len(), 1, "child sits blocked");
        assert!(
            !p.pool().has_queued_dispatchable(),
            "nothing queued — only the blocked dependent"
        );
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert!(
            !p.only_silent_held_work_remains(),
            "blocked > 0 → oracle must be false"
        );
    }
    // (c) in-flight empty → false (nothing to recover).
    {
        let (transport, _r, _k) = empty_transport();
        let mut p: PrimaryCoordinator<_, _, _, TestId> = PrimaryCoordinator::new(
            config(Duration::from_millis(50), 2),
            transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator,
        );
        install_default_pool(&mut p);
        // Operational but holds NO in-flight task.
        let conn = SecondaryConnection::new("dead-sec".into())
            .receive_welcome(1, vec![], "host".into(), 0, None, false, false)
            .receive_cert_exchange(String::new(), None, None, 0)
            .begin_peer_discovery()
            .peers_ready()
            .assignments_sent();
        p.secondaries.insert(
            "dead-sec".into(),
            SecondaryConnectionState::Operational(conn),
        );
        p.seed_keepalive("dead-sec");
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert!(
            !p.only_silent_held_work_remains(),
            "in-flight empty → oracle must be false"
        );
    }
    // (d) a NON-silent secondary holds in-flight → false (a live secondary
    //     is still making progress; never evict it).
    {
        let (transport, _r, _k) = two_secondary_transport();
        let mut p: PrimaryCoordinator<_, _, _, TestId> = PrimaryCoordinator::new(
            config(Duration::from_millis(50), 2),
            transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator,
        );
        install_default_pool(&mut p);
        register_operational_secondary(&mut p, "sec-a", 0, "victim-a");
        register_operational_secondary(&mut p, "sec-b", 1, "victim-b");
        tokio::time::sleep(Duration::from_millis(120)).await;
        // sec-b refreshes → not silent; it still holds victim-b in-flight.
        p.record_keepalive("sec-b");
        assert!(
            !p.only_silent_held_work_remains(),
            "a non-silent secondary holds in-flight work → oracle must be false"
        );
    }
}

/// Lazy on-demand requeue at the dispatch altitude: when an idle survivor
/// has nothing to dispatch and the only remaining work is in-flight on a
/// silent secondary, the worker-management reaction declares the silent
/// holder dead and the recovered task re-dispatches to the survivor — all
/// BEFORE the hard backstop elapses (this fires at the first WARN stage,
/// well under the 100ms hard bound, driven by the dispatch reaction not the
/// heartbeat tick).
#[tokio::test(flavor = "current_thread")]
async fn lazy_requeue_fires_at_dispatch_altitude_when_only_silent_held_work_remains() {
    let (transport, mut sec_rxs, _incoming_tx) = two_secondary_transport();
    // WARN at 1x (50ms), hard backstop far away (20x = 1s) so the recovery
    // CANNOT be the backstop — it must be the lazy oracle.
    let mut cfg = config(Duration::from_millis(50), 2);
    cfg.silence_warn_multiples = vec![1];
    cfg.silence_hard_multiple = 20;
    let mut primary: PrimaryCoordinator<_, _, _, TestId> = PrimaryCoordinator::new(
        cfg,
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator,
    );
    install_default_pool(&mut primary);

    // sec-a is the silent holder of the only in-flight task.
    register_operational_secondary(&mut primary, "sec-a", 0, "victim-a");

    // sec-b is the idle survivor with a real memory budget.
    let sec_b_conn = SecondaryConnection::new("sec-b".into())
        .receive_welcome(1, vec![], "host".into(), 0, None, false, false)
        .receive_cert_exchange(String::new(), None, None, 0)
        .begin_peer_discovery()
        .peers_ready()
        .assignments_sent();
    primary.secondaries.insert(
        "sec-b".into(),
        SecondaryConnectionState::Operational(sec_b_conn),
    );
    primary.seed_keepalive("sec-b");
    primary.register_idle_worker_for_test(
        "sec-b".into(),
        1,
        ResourceMap::from([(
            dynrunner_core::ResourceKind::memory(),
            1024 * 1024 * 1024u64,
        )]),
    );

    // Install the worker-management bus so the requeue path's re-emitted
    // `TasksAdded` lands on a receiver (drained the NEXT iteration in
    // production; here we just need a live sender).
    let (wm_tx, mut wm_rx) =
        tokio_mpsc::unbounded_channel::<crate::worker_signal::WorkerMgmtSignal>();
    primary
        .cluster_state_mut_for_test()
        .install_worker_mgmt_sender(wm_tx);

    // sec-a goes silent past the FIRST WARN stage (50ms) but NOT past the
    // hard backstop (1s). Refresh sec-b so it stays a live survivor.
    tokio::time::sleep(Duration::from_millis(120)).await;
    primary.record_keepalive("sec-b");
    assert!(
        primary.only_silent_held_work_remains(),
        "precondition: only sec-a's silent-held in-flight work remains"
    );

    // Drive the worker-management reaction with a `TasksAdded` batch — the
    // dispatch pass finds sec-b idle with nothing to dispatch, then the
    // post-pass consult declares sec-a dead and requeues victim-a.
    let batch = crate::worker_signal::WorkerSignalBatch {
        signals: vec![crate::worker_signal::WorkerMgmtSignal::TasksAdded],
    };
    primary.react_to_worker_signal_batch(batch).await;

    assert!(
        !primary.secondaries.contains_key("sec-a"),
        "lazy oracle declared the silent holder dead"
    );
    assert!(
        primary.secondaries.contains_key("sec-b"),
        "the live survivor is untouched"
    );

    // The requeue re-emitted a `TasksAdded` (production drains it next
    // iteration). Drive that recheck synchronously to re-dispatch.
    let followup =
        crate::worker_signal::drain_worker_signal_batch(&mut wm_rx, Duration::from_millis(50))
            .await
            .expect("the lazy requeue must re-emit a TasksAdded batch");
    // Keep the survivor live across the re-dispatch reaction (production
    // invariant: a live secondary keeps sending keepalives).
    primary.record_keepalive("sec-b");
    primary.react_to_worker_signal_batch(followup).await;

    let assignment = first_task_assignment(&mut sec_rxs[1]);
    assert!(
        assignment.is_some(),
        "the recovered task must re-dispatch to the idle survivor"
    );
    if let Some(DistributedMessage::TaskAssignment { secondary_id, .. }) = assignment {
        assert_eq!(secondary_id, "sec-b");
    }
}
