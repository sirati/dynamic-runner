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
use crate::process::{LocalRole, Mesh, RoleSlot};
use crate::state::{SecondaryConnection, SecondaryConnectionState};
use dynrunner_protocol_primary_secondary::address::PeerId;
use dynrunner_protocol_primary_secondary::{
    InboundTap, IngestEdges, PeerConnectionInfo, PeerTransport,
};
use dynrunner_scheduler_api::{PendingPool, ResourceEstimator, Scheduler};
use std::sync::Arc;

/// Keeps the test mesh + primary slot `Arc` + demote sender alive for the
/// life of a [`PrimaryCoordinator`] built by [`build_primary`]. Local twin
/// of `test_helpers::PrimaryMeshKeepalive` (this file uses its own `TestId`,
/// so it cannot reuse that helper).
///
/// Two shapes, exactly mirroring `test_helpers::PrimaryMeshKeepalive`:
/// - NO-PUMP (the default `build_primary`): the mesh is parked in `_mesh` so
///   its egress-queue receiver stays alive; tests inspect the coordinator's
///   in-memory state directly and never drive a wire round trip.
/// - PUMPED (`build_primary_pumped`, used by the queued-egress tests inside a
///   `LocalSet`): the production [`crate::process::pump::run_pump`] OWNS the
///   mesh + slot, so a queued `client.send` reaches the transport's outgoing
///   channels. The guard holds the control handle (keeps the pump's control
///   arm open) and aborts the pump task on drop.
struct MeshKeepalive<Tr: PeerTransport<TestId> = ChannelPeerTransport<TestId>> {
    _mesh: Option<Mesh<TestId, Tr>>,
    _slot: Option<Arc<RoleSlot<TestId>>>,
    _demote_tx: tokio_mpsc::UnboundedSender<()>,
    _control: Option<crate::process::MeshControlHandle<TestId>>,
    pump: Option<tokio::task::JoinHandle<()>>,
}

impl<Tr: PeerTransport<TestId>> Drop for MeshKeepalive<Tr> {
    fn drop(&mut self) {
        if let Some(h) = self.pump.take() {
            h.abort();
        }
    }
}

/// Mint the primary's mesh trio + build the [`PrimaryCoordinator`], returning
/// the coordinator alongside the still-owned `mesh`, `slot`, and demote
/// sender. The single construction choke point both `build_primary` (no pump)
/// and `build_primary_pumped` (production pump) share — the mint +
/// `new(client, inbox, demote_rx, …)` wiring lives here ONCE, and each entry
/// point decides only what to do with the returned mesh/slot.
#[allow(clippy::type_complexity)]
fn mint_primary<S, E, Tr>(
    config: PrimaryConfig,
    transport: Tr,
    scheduler: S,
    estimator: E,
) -> (
    PrimaryCoordinator<S, E, TestId>,
    Mesh<TestId, Tr>,
    Arc<RoleSlot<TestId>>,
    tokio_mpsc::UnboundedSender<()>,
)
where
    S: Scheduler<TestId>,
    E: ResourceEstimator<TestId>,
    Tr: PeerTransport<TestId>,
{
    let mut mesh = Mesh::new(transport);
    let (slot, client, inbox) =
        mesh.register_local_role(LocalRole::Primary, PeerId::from(config.node_id.as_str()));
    let (demote_tx, demote_rx) = tokio_mpsc::unbounded_channel();
    let primary = PrimaryCoordinator::new(config, client, inbox, demote_rx, scheduler, estimator);
    (primary, mesh, slot, demote_tx)
}

/// Mint a [`PrimaryCoordinator`] over a test `Mesh` and spawn NO pump.
/// These tests drive the coordinator's heartbeat methods directly and inspect
/// in-memory state, not wire round-trips; the mesh is parked idle in the
/// keepalive so its egress-queue receiver stays alive (a queued `client.send`
/// must not error as "pump dropped").
fn build_primary<S, E, Tr>(
    config: PrimaryConfig,
    transport: Tr,
    scheduler: S,
    estimator: E,
) -> (PrimaryCoordinator<S, E, TestId>, MeshKeepalive<Tr>)
where
    S: Scheduler<TestId>,
    E: ResourceEstimator<TestId>,
    Tr: PeerTransport<TestId>,
{
    let (primary, mesh, slot, demote_tx) = mint_primary(config, transport, scheduler, estimator);
    (
        primary,
        MeshKeepalive {
            _mesh: Some(mesh),
            _slot: Some(slot),
            _demote_tx: demote_tx,
            _control: None,
            pump: None,
        },
    )
}

/// As [`build_primary`] but spawns the PRODUCTION mesh-pump over the mesh, so
/// the coordinator's QUEUED egress (M4) drains onto the transport's outgoing
/// channels and the per-secondary receivers observe the broadcast. MUST be
/// called inside a `tokio::task::LocalSet` (the pump is `spawn_local`'d); the
/// queued-egress heartbeat tests wrap their body in `LocalSet::run_until`.
/// The pump task OWNS the slot `Arc` for its lifetime, mirroring the node.
fn build_primary_pumped<S, E, Tr>(
    config: PrimaryConfig,
    transport: Tr,
    scheduler: S,
    estimator: E,
) -> (PrimaryCoordinator<S, E, TestId>, MeshKeepalive<Tr>)
where
    S: Scheduler<TestId> + 'static,
    E: ResourceEstimator<TestId> + 'static,
    Tr: PeerTransport<TestId> + 'static,
{
    let (primary, mesh, slot, demote_tx) = mint_primary(config, transport, scheduler, estimator);
    // Publish live membership before the pump spawns (the pump republishes
    // every cycle thereafter).
    mesh.publish_membership();
    let (control, control_rx) = crate::process::pump::control_channel::<TestId>();
    let pump = tokio::task::spawn_local(async move {
        let _slot = slot;
        crate::process::pump::run_pump(mesh, control_rx).await;
    });
    (
        primary,
        MeshKeepalive {
            _mesh: None,
            _slot: None,
            _demote_tx: demote_tx,
            _control: Some(control),
            pump: Some(pump),
        },
    )
}

/// Test fixture: install an empty pool with a single "default" phase
/// onto a freshly-constructed primary. Mirrors what `run()` does in
/// production; tests that exercise post-initialisation paths
/// (heartbeat re-queue, etc.) need this so `pool_mut()` doesn't
/// panic.
fn install_default_pool<S, E>(primary: &mut PrimaryCoordinator<S, E, TestId>)
where
    S: Scheduler<TestId>,
    E: ResourceEstimator<TestId>,
{
    let phase = PhaseId::from("default");
    let pool = PendingPool::<TestId>::new([phase.clone()], std::collections::HashMap::new())
        .expect("default-phase pool");
    primary.pending = Some(pool);
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
        kind: Default::default(),
        setup_affinity: None,
        upload_file: None,
        required_files: None,
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
        ChannelPeerTransport::from_raw_channels("setup".into(), outgoing, incoming_rx),
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
    let (mut primary, _mesh) = build_primary(
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
        .receive_cert_exchange(String::new(), None, None, 0, None)
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
        kind: Default::default(),
        setup_affinity: None,
        upload_file: None,
        required_files: None,
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
    // The infra-distinction contract: a host dying mid-task is NOT the
    // task's fault — the requeue must not charge the task's retry
    // budget (no failed-ledger entry, no failure-class outcome).
    assert_eq!(
        primary.failed_count(),
        0,
        "a dead-host requeue must not consume the task's retry budget"
    );
}

/// A member that DEPARTED GRACEFULLY (its self-authored
/// `PeerRemoved { SelfDeparture }` applied to the replicated membership
/// ledger) must NEVER be silence-judged: its keepalives stopped BY DESIGN,
/// so its silence age inflates legitimately, but it is GONE — not silent.
///
/// REPRO (run_20260612_094056 face): a member departed cleanly, a promotion
/// followed, and the promoted primary still carried the gone member in its
/// roster cache (`self.secondaries` / `secondary_keepalives` are not reaped
/// by the membership apply path — only by the bootstrap handshake + the
/// hydrate rebuild). Two minutes later the silence schedule crossed the hard
/// backstop and the promoted primary silence-removed-with-requeue the
/// departed member, charging a spurious keepalive-miss death + task requeue
/// against a member that left deliberately.
///
/// Post-fix the sweep pre-filters the departed-but-still-cached member off
/// the SAME authoritative ledger the hydrate rebuild reads, so it never
/// enters `collect_heartbeat_report` and the requeue path is never reached.
#[tokio::test(flavor = "current_thread")]
async fn gracefully_departed_member_is_not_silence_removed() {
    let (transport, _sec_rx, _kept_alive_for_outgoing_clone) = empty_transport();
    let (mut primary, _mesh) = build_primary(
        config(Duration::from_millis(50), 2),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator,
    );
    install_default_pool(&mut primary);

    // Register the member at the connection level (Operational) with one
    // in-flight task — the SAME roster-cache shape a promoted primary
    // reconstructs from the replicated capacity ledger.
    let conn = SecondaryConnection::new("departed-sec".into())
        .receive_welcome(1, vec![], "host".into(), 0, None, false, false)
        .receive_cert_exchange(String::new(), None, None, 0, None)
        .begin_peer_discovery()
        .peers_ready()
        .assignments_sent();
    primary.secondaries.insert(
        "departed-sec".into(),
        SecondaryConnectionState::Operational(conn),
    );
    primary.seed_keepalive("departed-sec");
    let in_flight = task("victim", &[]);
    primary.stage_in_flight_for_test("departed-sec".into(), 0, in_flight);

    // The replicated membership facts: the member joined, advertised
    // capacity, then SELF-DEPARTED gracefully (the leaving node's
    // `PeerRemoved { SelfDeparture }`). After the apply the membership
    // ledger reads `RemovedMember` — but the roster cache still holds the
    // departed member (the apply path does not reap it).
    {
        let cs = primary.cluster_state_mut_for_test();
        cs.apply(ClusterMutation::PeerJoined {
            peer_id: "departed-sec".into(),
            is_observer: false,
            can_be_primary: true,
            cap_version: Default::default(),
            member_gen: 0,
        });
        cs.apply(ClusterMutation::SecondaryCapacity {
            secondary: "departed-sec".into(),
            worker_count: 1,
            resources: vec![],
        });
        cs.apply(ClusterMutation::PeerRemoved {
            id: "departed-sec".into(),
            cause: RemovalCause::SelfDeparture(BoundedString::from(
                "graceful abort: local work drained".to_string(),
            )),
            member_gen: 0,
        });
    }
    assert!(
        !primary.cluster_state_for_test().is_peer_alive("departed-sec"),
        "the graceful departure flipped the membership ledger to removed"
    );

    // Sleep WELL past the hard backstop (2x the 50ms interval) — a member
    // judged by the silence schedule would be declared dead here.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // The sweep must NOT include the departed member: it is GONE, not silent.
    let report = primary.collect_heartbeat_report();
    assert!(
        report.silences.is_empty(),
        "a gracefully-departed member must not enter the silence sweep"
    );

    // Drive the tick: no removal, no requeue. The in-flight task stays
    // attributed to the departed member (its terminal/recovery is the
    // failover-hydrate concern, NOT the silence machinery's).
    primary.process_heartbeat_tick().await.unwrap();
    assert_eq!(
        primary.pool().len(),
        0,
        "the silence schedule must NOT requeue the departed member's task"
    );
    assert_eq!(
        primary.failed_count(),
        0,
        "no spurious keepalive-miss death is charged against a departed member"
    );
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
        ChannelPeerTransport::from_raw_channels("setup".into(), outgoing, incoming_rx),
        vec![a_rx, b_rx],
        incoming_tx,
    )
}

/// Helper: register a secondary in Operational state with a single
/// in-flight task. Mirrors the setup pattern of
/// `dead_secondary_requeues_in_flight_task` but parametrised by id
/// so the mass-death tests can stage two of them.
fn register_operational_secondary<S, E>(
    primary: &mut PrimaryCoordinator<S, E, TestId>,
    secondary_id: &str,
    worker_id: u32,
    in_flight_label: &str,
) where
    S: Scheduler<TestId>,
    E: ResourceEstimator<TestId>,
{
    let conn = SecondaryConnection::new(secondary_id.into())
        .receive_welcome(1, vec![], "host".into(), 0, None, false, false)
        .receive_cert_exchange(String::new(), None, None, 0, None)
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
            kind: Default::default(),
            setup_affinity: None,
            upload_file: None,
            required_files: None,
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
        if let DistributedMessage::ClusterMutation {
            target: _,
            mutations,
            ..
        } = msg
        {
            for m in mutations {
                if let ClusterMutation::PeerRemoved { id, cause, .. } = m {
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
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, mut sec_rxs, _incoming_tx) = two_secondary_transport();
            let (mut primary, _mesh) = build_primary_pumped(
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
                target: None,
                sender_id: "sec-a".into(),
                timestamp: 0.0,
                secondary_id: "sec-a".into(),
                error: huge,
            };
            primary.handle_secondary_fatal_error(fatal).await.unwrap();

            // The PeerRemoved is a QUEUED mesh send; settle the production
            // pump so it drains onto the survivors' outgoing channels before
            // the receivers are drained.
            crate::primary::tests::settle_pump().await;
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
        })
        .await;
}

/// A secondary that's still sending keepalives stays in the routable
/// set even when other secondaries die.
#[tokio::test(flavor = "current_thread")]
async fn live_secondary_is_not_falsely_declared_dead() {
    let (transport, _sec_rx, _kept_alive_for_outgoing_clone) = empty_transport();
    let (mut primary, _mesh) = build_primary(
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
        if matches!(msg, DistributedMessage::TaskAssignment { target: _, .. }) {
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
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, mut sec_rxs, _incoming_tx) = two_secondary_transport();
            let (mut primary, _mesh) = build_primary_pumped(
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
                .receive_cert_exchange(String::new(), None, None, 0, None)
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
            let batch = crate::worker_signal::recv_worker_signal_batch(&mut wm_rx)
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
            primary.react_to_worker_signal_batch(batch, &mut None).await;

            // The load-bearing assertion: sec-b's outgoing channel saw a
            // `TaskAssignment` — i.e. the recheck re-dispatched to the
            // surviving idle worker, the very signal the production run was
            // missing. The assignment is a QUEUED mesh send, so settle the
            // production pump before draining the wire.
            crate::primary::tests::settle_pump().await;
            let assignment = first_task_assignment(&mut sec_rxs[1]);
            assert!(
                assignment.is_some(),
                "survivor must receive TaskAssignment after dead-secondary requeue; \
                 without the kickstart the recovered task hangs in the pool until \
                 the next external event (which never came in the cohort run)"
            );
            if let Some(DistributedMessage::TaskAssignment {
                target: _,
                secondary_id,
                ..
            }) = assignment
            {
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
        })
        .await;
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
    let (mut primary, _mesh) = build_primary(
        config(Duration::from_millis(50), 2),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator,
    );
    install_default_pool(&mut primary);

    // Register the dead-to-be secondary, operational.
    let conn = SecondaryConnection::new("dead-sec".into())
        .receive_welcome(1, vec![], "host".into(), 0, None, false, false)
        .receive_cert_exchange(String::new(), None, None, 0, None)
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
        kind: Default::default(),
        setup_affinity: None,
        upload_file: None,
        required_files: None,
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
            attempt: 0,
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
    let (mut promoted, _mesh) = build_primary(
        config(Duration::from_millis(50), 2),
        transport2,
        ResourceStealingScheduler::memory(),
        FixedEstimator,
    );
    promoted.cluster_state_mut_for_test().restore(snapshot);
    promoted.hydrate_from_cluster_state()
        .expect("test fixture: composed task graph is valid");

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
/// 4x of a 10ms interval. The silence is a plain `Duration` — whichever
/// clock the caller judges by (wall-clock evidence age, or the judged
/// clock under chronic starvation).
#[test]
fn silence_stage_classifies_into_highest_crossed_stage() {
    let interval = Duration::from_millis(10);
    let warn = [1u32, 2u32];
    let hard = 4u32;
    let silent_for = Duration::from_millis;

    // Below the first WARN multiple (1x = 10ms): no stage.
    assert_eq!(silence_stage(silent_for(5), interval, &warn, hard), None);
    // Past 1x but below 2x: WARN(0).
    assert_eq!(
        silence_stage(silent_for(15), interval, &warn, hard),
        Some(Stage::Warn(0))
    );
    // Past 2x but below the hard 4x: WARN(1) (highest crossed WARN).
    assert_eq!(
        silence_stage(silent_for(25), interval, &warn, hard),
        Some(Stage::Warn(1))
    );
    // Past the hard 4x: Hard wins regardless of WARN crossings.
    assert_eq!(
        silence_stage(silent_for(45), interval, &warn, hard),
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
    let (mut primary, _mesh) = build_primary(
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
    let (mut primary, _mesh) = build_primary(
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
    let (mut primary, _mesh) = build_primary(
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
    let (mut primary, _mesh) = build_primary(
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

/// Self-cut guard: the recognized primary's OWN same-peer secondary, when
/// transiently silent past the first WARN stage but strictly BEFORE the hard
/// backstop, must NOT appear in `silent_secondary_ids` and must NOT flip
/// `only_silent_held_work_remains` on. The early dispatch-altitude lazy
/// requeue acts on first-stage silence; during a momentary self-keepalive gap
/// (the host's own secondary is still processing but briefly silent) reporting
/// self here would yank the self's LIVE in-flight task before the next
/// keepalive refreshes the clock and before the hard backstop. The identity
/// filter (`id != current_primary`, owned by `silent_secondary_ids` for the
/// early-requeue concern) excludes that single same-peer entry by IDENTITY. The hard backstop
/// is deliberately left unfiltered — this guard is the EARLY (WARN-only) path.
///
/// The schedule here puts the hard backstop far above the sleep (HARD at 10x =
/// 500ms vs WARN at 1x = 50ms), so the ~120ms silence lands comfortably in the
/// WARN-only window (70ms clear of WARN(0), 380ms clear of HARD); the assertion
/// below pins the self entry to `Warn(0)` to prove it. A companion case past
/// the hard backstop confirms the SAME filter (stage-agnostic `.is_some()`)
/// also excludes the self at the HARD stage.
#[tokio::test(flavor = "current_thread")]
async fn self_secondary_excluded_from_silent_set_and_oracle() {
    let (transport, _sec_rx, _kept) = empty_transport();
    // Widen the hard backstop to 10x (500ms) so the sleep below lands in the
    // WARN-only window with a comfortable, non-flaky margin on both sides —
    // the same robust config shape `warn_stages_fire_once_and_reset_on_recovery`
    // uses. WARN stays at 1x (50ms).
    let mut cfg = config(Duration::from_millis(50), 2);
    cfg.silence_hard_multiple = 10;
    let (mut primary, _mesh) = build_primary(
        cfg,
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator,
    );
    install_default_pool(&mut primary);

    // The recognized primary is the local host ("primary", the default
    // `node_id`); its OWN same-peer secondary advertises under the same
    // peer-id and holds the only in-flight task.
    primary
        .cluster_state_mut_for_test()
        .apply(ClusterMutation::PrimaryChanged {
            new: "setup".into(),
            epoch: 1,
            reason: Default::default(),
        });
    register_operational_secondary(&mut primary, "setup", 0, "self-victim");

    // The self-secondary goes silent past the FIRST WARN stage (50ms) but
    // well under the hard backstop (500ms) — exactly the transient
    // self-keepalive gap §15 describes (the WARN-only window).
    tokio::time::sleep(Duration::from_millis(120)).await;

    // Without the filter the staged classifier would flag the self entry;
    // the identity cut excludes it. First pin the silence to the WARN-only
    // window so the test genuinely demonstrates the early path: the self
    // entry is at WARN(0), strictly before the hard backstop.
    let report = primary.collect_heartbeat_report();
    assert_eq!(
        report.silences.len(),
        1,
        "the self-secondary is tracked in the raw silence sweep"
    );
    assert_eq!(
        silence_stage(
            Instant::now().saturating_duration_since(report.silences[0].last_keepalive),
            Duration::from_millis(50),
            &[1],
            10,
        ),
        Some(Stage::Warn(0)),
        "the silence sits in the WARN-only window (past WARN(0), before HARD)"
    );
    assert!(
        primary.silent_secondary_ids().is_empty(),
        "the recognized primary's own same-peer secondary must be excluded \
         from the silent set by the identity filter"
    );

    // And therefore the early dispatch-altitude oracle cannot fire on it:
    // the self's LIVE in-flight task is not early-requeue-eligible.
    assert!(
        !primary.only_silent_held_work_remains(),
        "self-held silent in-flight work must NOT make the lazy-requeue \
         oracle true — yanking the self's live task is the §15 self-cut"
    );
}

/// Companion to the WARN-only self-cut guard: the identity filter is
/// stage-agnostic (`silence_stage(..).is_some()`), so the recognized primary's
/// own same-peer secondary is excluded from `silent_secondary_ids` and the
/// early oracle even when the silence is PAST the hard backstop. (The hard
/// backstop itself, `decide_dead_secondaries`, stays unfiltered and is what
/// recovers the self entry — but the EARLY dispatch-altitude path never does.)
#[tokio::test(flavor = "current_thread")]
async fn self_secondary_excluded_from_early_path_past_hard_backstop() {
    let (transport, _sec_rx, _kept) = empty_transport();
    // Default schedule: WARN at 1x (50ms), HARD at 2x (100ms).
    let (mut primary, _mesh) = build_primary(
        config(Duration::from_millis(50), 2),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator,
    );
    install_default_pool(&mut primary);
    primary
        .cluster_state_mut_for_test()
        .apply(ClusterMutation::PrimaryChanged {
            new: "setup".into(),
            epoch: 1,
            reason: Default::default(),
        });
    register_operational_secondary(&mut primary, "setup", 0, "self-victim");

    // Silence past the hard backstop (100ms): the HARD stage, not WARN.
    tokio::time::sleep(Duration::from_millis(150)).await;

    let report = primary.collect_heartbeat_report();
    assert_eq!(
        silence_stage(
            Instant::now().saturating_duration_since(report.silences[0].last_keepalive),
            Duration::from_millis(50),
            &[1],
            2,
        ),
        Some(Stage::Hard),
        "the silence sits at the HARD stage (past the backstop)"
    );
    assert!(
        primary.silent_secondary_ids().is_empty(),
        "the identity filter excludes the self at the HARD stage too"
    );
    assert!(
        !primary.only_silent_held_work_remains(),
        "the early dispatch-altitude oracle never fires on the self entry, \
         regardless of stage"
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
        let (mut p, _mesh) = build_primary(
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
        let (mut p, _mesh) = build_primary(
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
        let (mut p, _mesh) = build_primary(
            config(Duration::from_millis(50), 2),
            transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator,
        );
        install_default_pool(&mut p);
        // Operational but holds NO in-flight task.
        let conn = SecondaryConnection::new("dead-sec".into())
            .receive_welcome(1, vec![], "host".into(), 0, None, false, false)
            .receive_cert_exchange(String::new(), None, None, 0, None)
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
        let (mut p, _mesh) = build_primary(
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
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, mut sec_rxs, _incoming_tx) = two_secondary_transport();
            // WARN at 1x (50ms), hard backstop far away (20x = 1s) so the recovery
            // CANNOT be the backstop — it must be the lazy oracle.
            let mut cfg = config(Duration::from_millis(50), 2);
            cfg.silence_warn_multiples = vec![1];
            cfg.silence_hard_multiple = 20;
            let (mut primary, _mesh) = build_primary_pumped(
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
                .receive_cert_exchange(String::new(), None, None, 0, None)
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
            primary.react_to_worker_signal_batch(batch, &mut None).await;

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
            let followup = crate::worker_signal::recv_worker_signal_batch(&mut wm_rx)
                .await
                .expect("the lazy requeue must re-emit a TasksAdded batch");
            // Keep the survivor live across the re-dispatch reaction (production
            // invariant: a live secondary keeps sending keepalives).
            primary.record_keepalive("sec-b");
            primary.react_to_worker_signal_batch(followup, &mut None).await;

            // The re-dispatch TaskAssignment is a QUEUED mesh send; settle the
            // production pump so it drains onto sec-b's outgoing channel before
            // the wire is drained.
            crate::primary::tests::settle_pump().await;
            let assignment = first_task_assignment(&mut sec_rxs[1]);
            assert!(
                assignment.is_some(),
                "the recovered task must re-dispatch to the idle survivor"
            );
            if let Some(DistributedMessage::TaskAssignment {
                target: _,
                secondary_id,
                ..
            }) = assignment
            {
                assert_eq!(secondary_id, "sec-b");
            }
        })
        .await;
}

/// HEADLINE (busy-secondary-not-reaped): a secondary whose ONLY liveness
/// signal is its liveness BEACON — i.e. it sends NO mesh frame and fires
/// NO task-completion event for longer than the keepalive-miss threshold,
/// the exact shape of a node pegged on one long build — must NOT be
/// declared dead. The beacon datagram arrives on the primary's
/// `LivenessListener` and is folded into the death-clock through
/// `record_keepalive` (the SAME refresh the operational loop's
/// liveness-ping arm calls). This test drives `record_keepalive` directly
/// (the listener→loop seam) with NO `dispatch_message` (no mesh frame) and
/// asserts the secondary survives the HARD backstop.
///
/// Revert-check: with the beacon coupled to / blocked by the busy runtime
/// (the pre-fix world), NO `record_keepalive` lands during the build, the
/// silence crosses the backstop, and `process_heartbeat_tick` reaps the
/// node — which is precisely the genuine-death test below. So the two
/// tests together are the before/after of the fix.
#[tokio::test]
async fn busy_secondary_beaconing_is_not_reaped() {
    let (transport, _rx, _tx) = empty_transport();
    let (mut primary, _mesh) = build_primary(
        config(Duration::from_millis(50), 2),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator,
    );
    install_default_pool(&mut primary);

    let conn = SecondaryConnection::new("busy-sec".into())
        .receive_welcome(1, vec![], "host".into(), 0, None, false, false)
        .receive_cert_exchange(String::new(), None, None, 0, None)
        .begin_peer_discovery()
        .peers_ready()
        .assignments_sent();
    primary.secondaries.insert(
        "busy-sec".into(),
        SecondaryConnectionState::Operational(conn),
    );
    primary.seed_keepalive("busy-sec");

    // Simulate a long build: NO task events, NO mesh frames — only the
    // dedicated-thread beacon keeps asserting liveness. Real-time sleeps
    // (the death-clock reads `std::time::Instant`, which `start_paused`
    // does NOT control — mirrors the sibling reap tests' real sleeps). Beat
    // the keepalive cadence (50ms) repeatedly so total elapsed (~300ms) is
    // 3x the HARD backstop (2x 50ms = 100ms), refreshing via the beacon
    // path (`record_keepalive`) each step exactly as the listener→loop arm
    // does per datagram.
    for _ in 0..6 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        // The ONLY liveness input: a beacon ping (NOT a mesh frame, NOT a
        // task event). This is what the operational loop's liveness-ping
        // arm does for each datagram the listener forwards.
        primary.record_keepalive("busy-sec");
    }

    // The death-clock must show the node as recently-seen (well under the
    // backstop), NOT silent — the beacon kept it alive with zero task
    // activity.
    let report = primary.collect_heartbeat_report();
    assert_eq!(report.silences.len(), 1, "the busy secondary is still tracked");
    assert!(
        report.silences[0].silence < Duration::from_millis(100),
        "beacon keeps the death-clock fresh ({:?}) — well under the 100ms HARD \
         backstop — despite NO task events for 300ms",
        report.silences[0].silence
    );

    // And the reaper tick does NOT remove it.
    primary.process_heartbeat_tick().await.unwrap();
    assert!(
        primary.secondaries.contains_key("busy-sec"),
        "a busy-but-beaconing secondary must NOT be false-declared dead"
    );
}

/// GENUINE-DEATH (still detected): a secondary that ACTUALLY stops — NO
/// beacon AND NO mesh frame for past the threshold — is still reaped. This
/// guards that the beacon's union refresh did not break real
/// failure-detection: absent EVERY liveness source, the death-clock
/// crosses the HARD backstop and the node is removed.
#[tokio::test]
async fn genuinely_dead_secondary_without_beacon_is_still_reaped() {
    let (transport, _rx, _tx) = empty_transport();
    let (mut primary, _mesh) = build_primary(
        config(Duration::from_millis(50), 2),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator,
    );
    install_default_pool(&mut primary);

    let conn = SecondaryConnection::new("dead-sec".into())
        .receive_welcome(1, vec![], "host".into(), 0, None, false, false)
        .receive_cert_exchange(String::new(), None, None, 0, None)
        .begin_peer_discovery()
        .peers_ready()
        .assignments_sent();
    primary.secondaries.insert(
        "dead-sec".into(),
        SecondaryConnectionState::Operational(conn),
    );
    primary.seed_keepalive("dead-sec");

    // NO beacon, NO frame, NO task event — total silence past the HARD
    // backstop (2x 50ms = 100ms); 300ms is 3x. Real-time sleep (the
    // death-clock reads `std::time::Instant`, like the sibling reap tests).
    tokio::time::sleep(Duration::from_millis(300)).await;

    let report = primary.collect_heartbeat_report();
    assert_eq!(report.silences.len(), 1);
    assert!(
        report.silences[0].silence >= Duration::from_millis(100),
        "a genuinely silent secondary crosses the HARD backstop"
    );

    primary.process_heartbeat_tick().await.unwrap();
    assert!(
        !primary.secondaries.contains_key("dead-sec"),
        "a genuinely dead secondary (no beacon AND no frames) is still reaped — \
         the beacon's union refresh must not break real failure detection"
    );
}

/// PRIMARY-EMIT (#325): `publish_beacon_targets` rebuilds the PRIMARY→
/// secondaries beacon set from the live roster, resolving each secondary's
/// raw beacon `SocketAddr` through the shared `peer_liveness_addrs` book —
/// the source a PROMOTED primary's address-less hydrated roster relies on. It
/// EXCLUDES this primary's own node id (a primary never beacons itself) and
/// skips a secondary whose address the book lacks.
#[tokio::test]
async fn publish_beacon_targets_resolves_live_secondaries_via_book() {
    let (transport, _rx, _tx) = empty_transport();
    let mut cfg = config(Duration::from_millis(50), 2);
    // The co-located node id: it is BOTH the primary and a roster secondary
    // (the promoted-primary case), so it must be excluded from the beacon set.
    cfg.node_id = "secondary-0".into();
    let (mut primary, _mesh) = build_primary(
        cfg,
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator,
    );
    install_default_pool(&mut primary);

    // Roster: self (`secondary-0`, excluded), two remote secondaries with
    // known addresses, and one whose address the book lacks (skipped).
    for id in ["secondary-0", "secondary-1", "secondary-2", "secondary-3"] {
        let conn = SecondaryConnection::new(id.into())
            .receive_welcome(1, vec![], "host".into(), 0, None, false, false)
            .receive_cert_exchange(String::new(), None, None, 0, None)
            .begin_peer_discovery()
            .peers_ready()
            .assignments_sent();
        primary
            .secondaries
            .insert(id.into(), SecondaryConnectionState::Operational(conn));
    }

    // The shared address book (populated by the co-located secondary from
    // PeerInfo): self + two remotes have liveness addresses; secondary-3 does
    // not (e.g. it advertised no liveness_port).
    let book = crate::liveness::PeerLivenessAddrs::new();
    book.ingest(&[
        peer_info("secondary-0", "10.0.0.0", 5000),
        peer_info("secondary-1", "10.0.0.1", 5001),
        peer_info("secondary-2", "10.0.0.2", 5002),
    ]);
    primary.set_peer_liveness_addrs(book);
    let target = primary.beacon_target();

    primary.publish_beacon_targets();

    let mut published = target.current();
    published.sort();
    let a1: std::net::SocketAddr = "10.0.0.1:5001".parse().unwrap();
    let a2: std::net::SocketAddr = "10.0.0.2:5002".parse().unwrap();
    assert_eq!(
        published,
        vec![a1, a2],
        "the beacon set is the live REMOTE secondaries with known addresses — \
         self (secondary-0) excluded, secondary-3 (no address) skipped",
    );
}

/// A `PeerConnectionInfo` with a liveness address, for the address-book seed.
fn peer_info(id: &str, ipv4: &str, port: u16) -> dynrunner_protocol_primary_secondary::PeerConnectionInfo {
    dynrunner_protocol_primary_secondary::PeerConnectionInfo {
        secondary_id: id.to_string(),
        cert: String::new(),
        ipv4: Some(ipv4.to_string()),
        ipv6: None,
        port: 0,
        is_observer: false,
        liveness_port: Some(port),
        slurm_job_id: None,
    }
}

// ======================================================================
// Flood-immune removal + re-admission (run_20260610_221140 repro)
// ======================================================================

/// A `Secondary`-role keepalive frame from `id`, as the peer's keepalive
/// tick emits it — the proof-of-life frame the flood-immunity and
/// re-admission tests replay.
fn keepalive_from(id: &str) -> DistributedMessage<TestId> {
    DistributedMessage::Keepalive {
        target: None,
        sender_id: id.to_string(),
        timestamp: 0.0,
        secondary_id: id.to_string(),
        active_workers: 0,
        emitter_role: dynrunner_protocol_primary_secondary::KeepaliveRole::Secondary,
    }
}

/// REPRO (face a of run_20260610_221140): a starved node must NOT author
/// a removal for a peer whose frames are sitting in its BACKED-UP inbox.
///
/// The production sequence, replayed: the peer's keepalives kept
/// ARRIVING (they entered the primary's inbox) but the flooded loop
/// never processed them, so the processing-time death clock
/// (`secondary_keepalives`) inflated past the hard backstop and the
/// primary declared a LIVE peer dead ("inbox depth 52654, keepalive arm
/// running but starved"). Pre-fix this test removed `live-sec`; the
/// ingest-clock union in `collect_heartbeat_report` keeps it alive
/// because the queued frame was recorded at the slot's delivery choke
/// point. The genuine-death path is then proven intact: with NO frame
/// arriving (an empty inbox — a truly silent peer), the very same
/// schedule still removes it.
#[tokio::test(flavor = "current_thread")]
async fn starved_primary_does_not_remove_peer_whose_frames_are_queued() {
    let (transport, _sec_rx, _kept) = empty_transport();
    // Hard backstop at 2x the 50ms interval = 100ms.
    let (mut primary, keep) = build_primary(
        config(Duration::from_millis(50), 2),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator,
    );
    install_default_pool(&mut primary);
    register_operational_secondary(&mut primary, "live-sec", 0, "victim");

    // The "starved loop" window: the processing clock goes stale far
    // past the hard backstop while the loop is busy elsewhere...
    tokio::time::sleep(Duration::from_millis(200)).await;
    // ...but the peer's keepalive ARRIVES at the inbox (the slot's
    // delivery choke point records it at INGEST) and is never processed
    // — exactly the backed-up-inbox face.
    keep._slot
        .as_ref()
        .expect("no-pump build parks the slot")
        .deliver(keepalive_from("live-sec"))
        .expect("inbox live");

    // First tick after process start: the lag gate's None arm admits it.
    primary.process_heartbeat_tick().await.unwrap();
    assert!(
        primary.secondaries.contains_key("live-sec"),
        "a peer whose frames are QUEUED in the inbox is provably alive — \
         the starved node must not author its removal (the false-removal \
         face of run_20260610_221140)"
    );

    // GENUINE death is intact: the peer now sends NOTHING. The next
    // sweep after a full silence window removes it. (The first tick
    // after the sleep is itself deferred by the local-starvation gate —
    // the test genuinely stalled the runtime — and the follow-up
    // on-cadence tick performs the honest removal.)
    tokio::time::sleep(Duration::from_millis(200)).await;
    primary.process_heartbeat_tick().await.unwrap();
    assert!(
        primary.secondaries.contains_key("live-sec"),
        "the lagged sweep right after a local stall is deferred (its ages \
         reflect OUR stall)"
    );
    primary.process_heartbeat_tick().await.unwrap();
    assert!(
        !primary.secondaries.contains_key("live-sec"),
        "a truly silent peer (empty inbox, stale clocks) is still removed \
         — the genuine-death backstop is intact"
    );
}

// The PURE own-tick-lag classifier (formerly `local_sweep_starved`) now
// lives in the shared `crate::own_tick_health` primitive, with its
// `None`-first / at-threshold-healthy / beyond-threshold-starved coverage in
// that module's own unit tests. The primary-altitude behavioural deferral
// (a stalled runtime defers the sweep, the on-cadence follow-up removes a
// genuinely silent peer) is pinned by
// `starved_primary_does_not_remove_peer_whose_frames_are_queued` above.

/// REPRO (face b — the headline): a member REMOVED from the replicated
/// membership whose authenticated frames keep arriving is RE-ADMITTED
/// automatically, within ONE frame, without the member acting (it never
/// knows it was removed).
///
/// Replays the production sequence: the primary removes `sec-x`
/// (keepalive-timeout removal → sticky `PeerRemoved`, roster dropped),
/// then one of `sec-x`'s keepalives lands. Pre-fix the frame hit the
/// stale-ignore path (`record_keepalive` no-ops for an unknown id) and
/// the member stayed buried forever — "a 'removed' peer whose
/// authenticated frames kept ARRIVING was never re-admitted". Post-fix
/// the dispatch preamble re-admits: the replicated entry returns Alive
/// at generation 1, the roster + keepalive clock + worker slots are
/// restored, and a LATER genuine death still removes it (no permanent
/// immunity).
#[tokio::test(flavor = "current_thread")]
async fn removed_but_sending_peer_is_readmitted_on_next_frame() {
    let (transport, _sec_rx, _kept) = empty_transport();
    let (mut primary, _mesh) = build_primary(
        config(Duration::from_millis(50), 2),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator,
    );
    install_default_pool(&mut primary);
    register_operational_secondary(&mut primary, "sec-x", 0, "victim");
    // The welcome-time replicated facts: membership + the static
    // capacity record (what the re-admission rebuilds the roster from).
    {
        let cs = primary.cluster_state_mut_for_test();
        cs.apply(ClusterMutation::PeerJoined {
            peer_id: "sec-x".into(),
            is_observer: false,
            can_be_primary: true,
            cap_version: Default::default(),
            member_gen: 0,
        });
        cs.apply(ClusterMutation::SecondaryCapacity {
            secondary: "sec-x".into(),
            worker_count: 1,
            resources: vec![],
        });
    }

    // The (false) removal: the keepalive-timeout declaration path.
    primary
        .requeue_dead_secondary(
            super::DeadSecondary {
                secondary_id: "sec-x".into(),
                last_keepalive: Instant::now(),
            },
            RemovalCause::KeepaliveMiss,
        )
        .await
        .unwrap();
    assert!(!primary.secondaries.contains_key("sec-x"));
    assert!(!primary.cluster_state_for_test().is_peer_alive("sec-x"));

    // ONE authenticated frame from the removed-but-alive member arrives.
    primary
        .dispatch_message(keepalive_from("sec-x"), &mut None)
        .await
        .unwrap();

    // RE-ADMITTED, without the member acting:
    assert!(
        primary.cluster_state_for_test().is_peer_alive("sec-x"),
        "the replicated membership re-admits the provably-alive member"
    );
    assert_eq!(
        primary.cluster_state_for_test().peer_member_gen("sec-x"),
        1,
        "re-admission advances the membership incarnation (removal at \
         gen 0, rejoin at gen 1)"
    );
    assert!(
        primary
            .cluster_state_for_test()
            .role_table()
            .can_be_primary
            .contains("sec-x"),
        "the capability preserved on the tombstone is restored"
    );
    assert!(
        primary.secondaries.contains_key("sec-x"),
        "the primary-local roster entry is restored (metadata-only \
         Operational seed from the replicated capacity)"
    );
    assert!(
        primary.secondary_keepalives.contains_key("sec-x"),
        "the death clock is re-seeded at re-admission"
    );
    assert!(
        primary
            .workers
            .iter()
            .any(|w| w.secondary_id == "sec-x"),
        "the worker roster is rebuilt from the replicated capacity"
    );

    // No permanent immunity: the re-admitted member then goes GENUINELY
    // silent — the same schedule removes it again, at the advanced
    // generation. (The first post-stall tick is deferred by the local-
    // starvation gate; the follow-up tick removes.)
    tokio::time::sleep(Duration::from_millis(200)).await;
    primary.process_heartbeat_tick().await.unwrap();
    primary.process_heartbeat_tick().await.unwrap();
    assert!(
        !primary.secondaries.contains_key("sec-x"),
        "a genuinely-dead re-admitted member is removed again"
    );
    assert!(
        !primary.cluster_state_for_test().is_peer_alive("sec-x"),
        "the gen-1 incarnation is killed by a gen-1 removal"
    );
    assert_eq!(
        primary.cluster_state_for_test().peer_member_gen("sec-x"),
        1,
        "the second removal kills the CURRENT incarnation"
    );

    // And a SecondaryFatalError from a removed id must NOT re-admit
    // (its own meaning is "I am dying").
    let fatal = DistributedMessage::<TestId>::SecondaryFatalError {
        target: None,
        sender_id: "sec-x".into(),
        timestamp: 0.0,
        secondary_id: "sec-x".into(),
        error: "dying".into(),
    };
    primary.dispatch_message(fatal, &mut None).await.unwrap();
    assert!(
        !primary.cluster_state_for_test().is_peer_alive("sec-x"),
        "a fatal-error frame is not proof of ongoing life"
    );
}

/// Bring-up Primary-keepalive silent window: a primary whose
/// `self.secondaries` roster is EMPTY (the promoted-primary bring-up
/// window, before any welcome / hydrate has registered a secondary)
/// must STILL broadcast Primary-tagged keepalives to its connected
/// MESH MEMBERS. The keepalive's audience is the liveness peers the
/// transport knows, not the worker-bearing secondary roster: the
/// Primary-role keepalive is the ONLY frame that refreshes a peer's
/// `primary_last_seen` clock (`record_primary_message_if_from_primary`)
/// and cancels elections, so an empty-roster early-return turns a slow
/// bring-up into spurious primary-silence suspicion at every connected
/// member.
///
/// The fixture is exactly the bug shape: one connected transport member
/// (the `empty_transport` outgoing channel), zero registered
/// secondaries. N keepalive periods are driven; the member must observe
/// N Primary-role keepalives.
#[tokio::test(flavor = "current_thread")]
async fn empty_roster_primary_still_keepalives_connected_mesh_members() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // `empty_transport` wires ONE connected member ("dead-sec")
            // into the transport's outgoing map — a mesh member with no
            // roster entry.
            let (transport, mut member_rx, _kept) = empty_transport();
            let (mut primary, _mesh) = build_primary_pumped(
                config(Duration::from_millis(50), 2),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator,
            );
            assert!(
                primary.secondaries.is_empty(),
                "bring-up window precondition: no registered secondaries"
            );

            // Three keepalive periods' worth of emitter ticks.
            for _ in 0..3 {
                primary.broadcast_primary_keepalive().await;
            }
            // The keepalive is a QUEUED mesh send; settle the production
            // pump so it drains onto the member's channel.
            crate::primary::tests::settle_pump().await;

            let mut primary_keepalives = 0usize;
            while let Ok(msg) = member_rx.try_recv() {
                if matches!(
                    &msg,
                    DistributedMessage::Keepalive {
                        emitter_role:
                            dynrunner_protocol_primary_secondary::KeepaliveRole::Primary,
                        ..
                    }
                ) {
                    primary_keepalives += 1;
                }
            }
            assert_eq!(
                primary_keepalives, 3,
                "a promoted primary with an empty secondary roster must \
                 still heartbeat to its connected mesh members (the \
                 keepalive audience is the transport's members, not the \
                 worker roster)"
            );
        })
        .await;
}

// ---------------------------------------------------------------------
// Transport ingest-edge liveness (run_20260611_115429): the removal
// decision must consume the TRANSPORT-arrival clock and must be gated
// on the decider's own ingest health.
// ---------------------------------------------------------------------

/// Minimal [`PeerTransport`] publishing real [`IngestEdges`]: frames the
/// test pushes through the returned [`InboundTap`] are stamped on the
/// ARRIVAL clock (the production read-loop edge) and sit in the inbound
/// queue until something drives `recv_peer` (the pump), which stamps the
/// DRAINED clock — exactly the two measuring edges `PeerNetwork` /
/// `TunneledPeerTransport` mount. The no-pump `build_primary` harness
/// then reproduces the production starvation verbatim: arrivals without
/// drains.
struct EdgeTrackedTransport {
    edges: IngestEdges,
    incoming_rx: tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
}

fn edge_tracked_transport() -> (EdgeTrackedTransport, InboundTap<TestId>) {
    let edges = IngestEdges::new();
    let (tx, incoming_rx) = tokio_mpsc::unbounded_channel();
    let tap = InboundTap::new(tx, edges.arrival.clone());
    (EdgeTrackedTransport { edges, incoming_rx }, tap)
}

impl PeerTransport<TestId> for EdgeTrackedTransport {
    async fn broadcast(&mut self, _msg: DistributedMessage<TestId>) -> Result<(), String> {
        Ok(())
    }

    async fn send_to_peer(
        &mut self,
        _peer_id: &str,
        _msg: DistributedMessage<TestId>,
    ) -> Result<(), String> {
        Ok(())
    }

    async fn recv_peer(&mut self) -> Option<DistributedMessage<TestId>> {
        let msg = self.incoming_rx.recv().await?;
        self.edges.drained.record(msg.sender_id());
        Some(msg)
    }

    fn try_recv_peer(&mut self) -> Option<DistributedMessage<TestId>> {
        let msg = self.incoming_rx.try_recv().ok()?;
        self.edges.drained.record(msg.sender_id());
        Some(msg)
    }

    fn peer_count(&self) -> usize {
        0
    }

    fn has_peer(&self, _id: &PeerId) -> bool {
        false
    }

    fn ingest_edges(&self) -> Option<IngestEdges> {
        Some(self.edges.clone())
    }

    async fn connect_to_peers(&mut self, _peers: &[PeerConnectionInfo]) {}
}

/// REPRO (run_20260611_115429, the false-removal face): a node whose
/// MESH PUMP is starved — peers' keepalives keep ARRIVING at the
/// transport's read loops but never reach the slot's delivery choke
/// point — must NOT author a removal off its stale downstream clocks.
///
/// The production sequence, replayed: the processed clock
/// (`secondary_keepalives`) and the slot-ingest clock
/// (`RoleSlot::deliver` — the flood-immunity edge) both go stale past
/// the hard backstop, the sweep's own tick cadence is HEALTHY (the
/// operational loop ran fine; only the pump lagged — primary.log's
/// missed-keepalive requeues at last_seen 125-199s with all SLURM jobs
/// RUNNING), and the peer's keepalive arrives at the TRANSPORT just
/// before the sweep. Pre-fix the sweep removed the live peer; post-fix
/// the transport-arrival clock keeps its silence age honest. Evidence
/// then ENDS (the peer truly goes silent, the queue drains): the very
/// same schedule still removes it — no permanent immunity.
#[tokio::test(flavor = "current_thread")]
async fn transport_arrival_keeps_starved_pump_node_from_removing_live_peer() {
    let (transport, tap) = edge_tracked_transport();
    // Hard backstop at 2x the 50ms interval = 100ms; the sweep's own
    // tick-lag gate trips past 3x = 150ms, so 120ms gaps below keep
    // every tick locally healthy while silences cross the backstop.
    let (mut primary, mut keep) = build_primary(
        config(Duration::from_millis(50), 2),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator,
    );
    install_default_pool(&mut primary);
    register_operational_secondary(&mut primary, "live-sec", 0, "victim");

    // Tick #1 right away: seeds the tick-lag clock; silences are ~0.
    primary.process_heartbeat_tick().await.unwrap();
    assert!(primary.secondaries.contains_key("live-sec"));

    // The starved-pump window: every downstream clock ages past the
    // hard backstop...
    tokio::time::sleep(Duration::from_millis(120)).await;
    // ...but the peer's keepalive ARRIVES at the transport (the read
    // loop stamps the arrival clock) and is never drained to the slot —
    // the pump is "busy elsewhere".
    tap.send(keepalive_from("live-sec")).expect("transport live");

    // Tick #2: gap 120ms < the 150ms tick-lag gate, so the sweep RUNS —
    // and must see the transport-arrival evidence.
    primary.process_heartbeat_tick().await.unwrap();
    assert!(
        primary.secondaries.contains_key("live-sec"),
        "a peer whose frames ARRIVED at the transport is provably alive — \
         a node whose pump is starved must not author its removal (the \
         run_20260611_115429 false-removal face)"
    );

    // GENUINE death is intact: the pump catches up (drains the queued
    // keepalive into the slot — refreshing the slot clock and advancing
    // the drained edge), then the peer sends NOTHING further. The next
    // on-cadence sweep past the backstop removes it.
    assert!(
        keep._mesh
            .as_mut()
            .expect("no-pump build parks the mesh")
            .recv_dial_and_route()
            .await,
        "queued frame drains"
    );
    tokio::time::sleep(Duration::from_millis(120)).await;
    primary.process_heartbeat_tick().await.unwrap();
    assert!(
        !primary.secondaries.contains_key("live-sec"),
        "once the evidence ends (queue drained, peer silent at EVERY \
         edge) the genuine-death backstop still removes on cadence"
    );
}

/// REPRO (defense-in-depth — the staleness INPUTS guard): a sweep that
/// runs ON CADENCE while the node's ingest path is provably backlogged
/// (a frame arrived at the transport and has sat undrained across
/// sweeps) must not author ANY staleness-based removal — the buried
/// peer's keepalives may be sitting in the same backed-up queue,
/// unattributed (mid-decode, behind the pending frame's FIFO position).
/// The deferral is NAMED (throttled WARN saying why). When the backlog
/// drains, the gate lifts and the genuinely-dead peer is removed on the
/// next cadence — deferred, never amnestied.
#[tokio::test(flavor = "current_thread")]
async fn ingest_backlog_defers_staleness_removals_until_drained() {
    let log = crate::test_capture::TargetCapture::for_target(
        "dynrunner_manager_distributed::primary::heartbeat::ingest_gate",
    );
    let _guard = {
        use tracing_subscriber::layer::SubscriberExt;
        tracing::subscriber::set_default(tracing_subscriber::Registry::default().with(log.clone()))
    };

    let (transport, tap) = edge_tracked_transport();
    // Hard backstop at 4x the 50ms interval = 200ms — ABOVE the 3x =
    // 150ms ingest-pending threshold, mirroring production (backstop
    // 24x >> gate 3x): the gate matures before the backstop fires.
    let (mut primary, mut keep) = build_primary(
        PrimaryConfig {
            silence_hard_multiple: 4,
            ..config(Duration::from_millis(50), 2)
        },
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator,
    );
    install_default_pool(&mut primary);
    register_operational_secondary(&mut primary, "buried-sec", 0, "victim-a");
    register_operational_secondary(&mut primary, "noisy-sec", 1, "victim-b");

    // One frame from noisy-sec arrives at the transport and is never
    // drained — the persistent-backlog witness. buried-sec's keepalives
    // are imagined stuck BEHIND it (unattributed), so the test feeds
    // nothing for it: every clock the sweep can read goes stale.
    tap.send(keepalive_from("noisy-sec")).expect("transport live");
    primary.process_heartbeat_tick().await.unwrap();

    // Five on-cadence sweeps (60ms gaps — under the 150ms tick-lag
    // gate). buried-sec's silence crosses the 200ms backstop at the
    // last one; by then the undrained arrival has persisted ~240ms
    // (> 150ms), so the gate must defer the removal.
    for _ in 0..4 {
        tokio::time::sleep(Duration::from_millis(60)).await;
        primary.process_heartbeat_tick().await.unwrap();
    }
    assert!(
        primary.secondaries.contains_key("buried-sec"),
        "an ingest-backlogged decider must not author staleness-based \
         removals: the buried peer's frames may sit unattributed in the \
         same backed-up queue"
    );
    assert!(primary.secondaries.contains_key("noisy-sec"));
    assert!(
        log.events().iter().any(|e| e.level == tracing::Level::WARN),
        "the deferral is named, not silent: a WARN on the ingest-gate \
         target says why removals are deferred"
    );

    // The pump recovers: the backlog drains (drained edge advances,
    // the queued keepalive reaches the slot). The gate lifts; the next
    // on-cadence sweep removes the genuinely-silent buried-sec while
    // the drained evidence keeps noisy-sec alive... and once noisy-sec
    // goes silent past the backstop too, it is also removed.
    assert!(
        keep._mesh
            .as_mut()
            .expect("no-pump build parks the mesh")
            .recv_dial_and_route()
            .await
    );
    tokio::time::sleep(Duration::from_millis(60)).await;
    // Keep the survivor genuinely live across the sweep (the same
    // refresh-before-tick shape the kickstart test uses): in production
    // noisy-sec keeps sending keepalives, so it never reads as silent.
    // Without it, this test's compressed schedule (WARN(0) at 1× = 50ms)
    // would classify the 60ms-old drained evidence as stage-0 silence,
    // and with BOTH remotes silent the collective-silence self-suspect
    // gate would (correctly, per its contract) defer the removal — a
    // fixture artifact, not the defer-not-amnesty contract under test.
    primary.record_keepalive("noisy-sec");
    primary.process_heartbeat_tick().await.unwrap();
    assert!(
        !primary.secondaries.contains_key("buried-sec"),
        "the gate DEFERS, it does not amnesty: once the ingest path is \
         healthy again the genuinely-silent peer is removed on cadence"
    );
    assert!(
        primary.secondaries.contains_key("noisy-sec"),
        "the freshly-drained keepalive is honest evidence of life"
    );
}

/// Genuine-death control: with the ingest-edge clocks PRESENT and
/// totally healthy (no arrivals at either edge — a truly dead peer),
/// the gate must not interfere and the removal fires on the normal
/// cadence.
#[tokio::test(flavor = "current_thread")]
async fn truly_dead_peer_removed_on_cadence_when_ingest_healthy() {
    let (transport, _tap) = edge_tracked_transport();
    let (mut primary, _keep) = build_primary(
        config(Duration::from_millis(50), 2),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator,
    );
    install_default_pool(&mut primary);
    register_operational_secondary(&mut primary, "dead-sec", 0, "victim");

    primary.process_heartbeat_tick().await.unwrap();
    tokio::time::sleep(Duration::from_millis(120)).await;
    primary.process_heartbeat_tick().await.unwrap();
    assert!(
        !primary.secondaries.contains_key("dead-sec"),
        "no arrivals at EITHER edge: the dead peer is removed on the \
         normal cadence — the ingest gate never blocks a healthy decider"
    );
}

// ======================================================================
// Chronic-starvation replay (run_20260611_200548)
// ======================================================================

/// REPLAY (run_20260611_200548): a CHRONICALLY starved primary — EVERY
/// heartbeat tick's own inter-tick gap stretched past the starvation
/// threshold, for a streak spanning many hard silence windows — must
/// STILL remove a member that went permanently silent, and the removal
/// must reach the respawn pipeline (a replacement is requested under the
/// on-secondary-death policy).
///
/// The production sequence: six secondaries fatal-exited while the
/// promoted primary's node was saturated by uncapped workers; the
/// primary's own tick lagged on every sweep ("own tick lagged ...
/// deferring silence-based judgments", repeatedly for >22 minutes), so
/// the per-tick whole-sweep deferral repeated unboundedly — the dead
/// members were never judged, never removed from the membership, and the
/// respawn pipeline never received a lifecycle event (respawn_request=0
/// for 30 minutes with the respawn policy active).
///
/// The pinned contract: chronic deferral must ESCALATE. Once the starved
/// streak has spanned the hard silence window, sweeps resume on
/// starvation-honest accrued time (each lagged gap contributes at most
/// the starvation threshold), so a permanently-silent member is removed
/// within a bounded number of lagged sweeps while a member with fresh
/// evidence each round is never falsely declared (judged silence never
/// exceeds wall silence).
#[tokio::test(flavor = "current_thread")]
async fn chronically_starved_primary_removes_dead_member_and_requests_respawn() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _sec_rx, _kept) = empty_transport();
            // interval 50ms → starvation threshold 150ms (3×); hard
            // backstop 100ms (2×).
            let (mut primary, keep) = build_primary(
                config(Duration::from_millis(50), 2),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator,
            );
            install_default_pool(&mut primary);
            // The incident's respawn policy defaults: 3 per family,
            // 10 total, 30s cooldown.
            let spawner = std::sync::Arc::new(crate::primary::test_helpers::MockSpawner::new());
            let calls = std::sync::Arc::clone(&spawner.calls);
            primary.enable_respawn(
                spawner,
                crate::primary::respawn::RespawnBudget {
                    max_per_secondary: 3,
                    max_total: 10,
                    cooldown: Duration::from_secs(30),
                },
                "tcp://127.0.0.1:5555".into(),
                "-----BEGIN PUBLIC KEY-----\nFAKE\n".into(),
            );
            // The slurm-authoritative quantity gate (#543) refuses respawn
            // unless slurm reports the fleet below initial count. Wire a
            // static snapshot saying `current=0 < initial=2` so the
            // gate passes and this test exercises the chronic-starvation
            // removal → respawn dispatch path it was built for.
            primary.set_authority_snapshot(std::sync::Arc::new(
                crate::authority_snapshot::test_helpers::StaticSnapshot {
                    map: std::collections::HashMap::new(),
                    count: Some(0),
                },
            ));
            register_operational_secondary(&mut primary, "doomed", 0, "victim");
            register_operational_secondary(&mut primary, "survivor", 1, "victim-2");

            // Baseline on-cadence tick (sets the own-tick clock; both
            // members fresh).
            primary.process_heartbeat_tick().await.unwrap();

            // `doomed` dies (its process exits — no frame ever arrives
            // again). The primary's own runtime is CHRONICALLY starved:
            // every inter-tick gap (200ms) exceeds the 150ms starvation
            // threshold and the streak spans many 100ms hard windows.
            // `survivor` keeps delivering frames each round (its
            // keepalives land in the inbox at the slot's ingest choke
            // point), exactly like the five surviving members did.
            let mut removed_at_tick = None;
            for tick in 0..8 {
                tokio::time::sleep(Duration::from_millis(200)).await;
                keep._slot
                    .as_ref()
                    .expect("no-pump build parks the slot")
                    .deliver(keepalive_from("survivor"))
                    .expect("inbox live");
                primary.process_heartbeat_tick().await.unwrap();
                if !primary.secondaries.contains_key("doomed") {
                    removed_at_tick = Some(tick);
                    break;
                }
            }
            assert!(
                removed_at_tick.is_some(),
                "a permanently-silent member must be removed within a \
                 bounded number of chronically-lagged sweeps — perpetual \
                 whole-sweep deferral must escalate once the starved \
                 streak spans the hard silence window (the \
                 run_20260611_200548 face: 30 minutes of deferral, six \
                 dead members never removed)"
            );
            assert!(
                primary.secondaries.contains_key("survivor"),
                "a member with fresh evidence every round must NOT be \
                 swept up by the chronic-starvation escalation (judged \
                 silence resets on every evidence advance)"
            );
            assert!(
                !primary.cluster_state.is_peer_alive("doomed"),
                "the replicated membership must mark the dead member removed"
            );

            // The removal must reach the respawn pipeline through the
            // SAME listener `enable_respawn` registered — replicate the
            // lifecycle dispatcher fan-out the run loop drives
            // (`run_peer_lifecycle_dispatcher` + the respawn select arm).
            let mut lifecycle_rx = primary
                .lifecycle_rx
                .take()
                .expect("lifecycle dispatcher channel installed at construction");
            while let Ok(event) = lifecycle_rx.try_recv() {
                for listener in &primary.peer_lifecycle_listeners {
                    listener.on_event(&event);
                }
            }
            let mut respawn_rx = primary
                .respawn_lifecycle_rx
                .take()
                .expect("enable_respawn installed the respawn lifecycle channel");
            let mut removed_events = 0;
            while let Ok(event) = respawn_rx.try_recv() {
                if matches!(
                    &event,
                    crate::peer_lifecycle::PeerLifecycleEvent::Removed { id, .. } if id == "doomed"
                ) {
                    removed_events += 1;
                }
                primary.dispatch_respawn_lifecycle(event);
            }
            assert_eq!(
                removed_events, 1,
                "exactly one Removed lifecycle event for the dead member \
                 reaches the respawn pipeline"
            );

            // The request was ACCEPTED: replicated ledger entry written
            // and spawner invoked with a fresh id.
            assert_eq!(
                primary.cluster_state.respawn_events().len(),
                1,
                "the accepted respawn is recorded on the replicated ledger"
            );
            let outcome = primary
                .respawn_tasks
                .join_next()
                .await
                .expect("respawn spawn future present after dispatch")
                .expect("respawn task must not panic");
            assert!(outcome.result.is_ok());
            assert_eq!(outcome.original_id, "doomed");
            assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        })
        .await;
}

// ======================================================================
// Pre-Operational wedge replay (run_20260611_214327)
// ======================================================================

/// A `SecondaryWelcome` frame as the secondary's setup loop emits it —
/// including the handshake-RETRY duplicate that lands after the
/// bring-up walk already advanced the member past `Handshaking`.
fn welcome_from(id: &str) -> DistributedMessage<TestId> {
    DistributedMessage::SecondaryWelcome {
        target: None,
        sender_id: id.to_string(),
        timestamp: 0.0,
        secondary_id: id.to_string(),
        resources: vec![],
        worker_count: 1,
        hostname: "host".into(),
        is_observer: false,
        can_be_primary: true,
    }
}

/// REPLAY (run_20260611_214327): an UNSTARVED primary must remove a
/// wire-dead member within the hard silence window even when the
/// member's connection-state was REGRESSED out of `Operational` by a
/// duplicate `SecondaryWelcome`.
///
/// The production sequence, replayed: the secondary's setup loop
/// retries its welcome on a capped backoff (`wait_for_setup` — the
/// retry is load-bearing per run_20260611_005927), so a duplicate
/// welcome routinely lands AFTER the one-shot bring-up walk advanced
/// the member to `Operational`. Pre-fix, `handle_welcome` re-inserted a
/// fresh `Handshaking` state — and the batch walk phases never re-run,
/// so the member sat pre-Operational FOREVER while still working tasks
/// and keepaliving. When it was then kill -9'd (frames stop, wire dead,
/// SLURM job gone), the silence sweep's Operational gate skipped it on
/// every tick: no WARN stage, no hard backstop, no `PeerRemoved` for
/// 11+ minutes — respawn unreachable, its in-flight task stranded
/// ("left to the silence machinery", which was blind to the holder).
#[tokio::test(flavor = "current_thread")]
async fn rewelcomed_dead_member_is_removed_within_hard_window() {
    let (transport, _sec_rx, _kept) = empty_transport();
    // interval 50ms → hard backstop 100ms (2x), own-tick starvation gate
    // 150ms (3x): the 60ms tick gaps below keep the decider provably
    // healthy (unstarved) while a dead member's silence crosses the
    // backstop.
    let (mut primary, _mesh) = build_primary(
        config(Duration::from_millis(50), 2),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator,
    );
    install_default_pool(&mut primary);
    register_operational_secondary(&mut primary, "dead-sec", 0, "victim");
    register_operational_secondary(&mut primary, "survivor", 1, "victim-2");

    // The member's main loop is provably running: its mesh keepalive
    // arrives through the real ingest path (dispatch preamble +
    // Keepalive arm).
    primary
        .dispatch_message(keepalive_from("dead-sec"), &mut None)
        .await
        .unwrap();

    // The handshake-retry duplicate welcome lands after the bring-up
    // walk. Pre-fix this silently regressed the member to `Handshaking`.
    primary
        .dispatch_message(welcome_from("dead-sec"), &mut None)
        .await
        .unwrap();

    // Baseline on-cadence tick; the member keepalives once more (alive
    // and working — the light-load production shape).
    primary.process_heartbeat_tick().await.unwrap();
    primary
        .dispatch_message(keepalive_from("dead-sec"), &mut None)
        .await
        .unwrap();

    // kill -9: dead-sec's frames STOP (zero ingest, wire dead). The
    // survivor keeps keepaliving every round; the decider ticks on
    // cadence.
    let mut removed_at_tick = None;
    for tick in 0..6 {
        tokio::time::sleep(Duration::from_millis(60)).await;
        primary
            .dispatch_message(keepalive_from("survivor"), &mut None)
            .await
            .unwrap();
        primary.process_heartbeat_tick().await.unwrap();
        if !primary.secondaries.contains_key("dead-sec") {
            removed_at_tick = Some(tick);
            break;
        }
    }
    assert!(
        removed_at_tick.is_some(),
        "a wire-dead member must be PeerRemoved within the hard silence \
         window on an unstarved primary, REGARDLESS of a duplicate-welcome \
         state regression (the run_20260611_214327 wedge: never removed, \
         respawn unreachable, task stranded)"
    );
    assert!(
        !primary.cluster_state.is_peer_alive("dead-sec"),
        "the authoritative PeerRemoved must land in the replicated membership"
    );
    assert!(
        primary.secondaries.contains_key("survivor"),
        "the keepaliving survivor must not be swept up"
    );
    assert_eq!(
        primary.pool().iter().count(),
        1,
        "the dead member's stranded in-flight task is requeued into the pool"
    );
}

/// The bounded-gate law, entry-path-agnostic: a member stuck in ANY
/// pre-Operational connection state whose mesh keepalives have PROVEN
/// its main loop is running must be judged by the silence schedule.
/// The Operational gate's setup exemption exists for members whose
/// keepalive EMITTER has not started yet (the secondary's keepalive arm
/// spins up post-`wait_for_setup`); the moment the member demonstrably
/// emits, the exemption must lift — a removal deferral that can never
/// lift is a bug by construction.
#[tokio::test(flavor = "current_thread")]
async fn keepalive_proven_pre_operational_member_is_silence_judged() {
    let (transport, _sec_rx, _kept) = empty_transport();
    let (mut primary, _mesh) = build_primary(
        config(Duration::from_millis(50), 2),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator,
    );
    install_default_pool(&mut primary);
    register_operational_secondary(&mut primary, "wedged", 0, "victim");

    // Force the wedge directly (entry-path-agnostic — whatever regressed
    // the state, the silence machinery must stay bounded): the member's
    // primary-side state sits at `Handshaking` while its own node keeps
    // running.
    let conn = SecondaryConnection::new("wedged".into()).receive_welcome(
        1,
        vec![],
        "host".into(),
        0,
        None,
        false,
        false,
    );
    primary
        .secondaries
        .insert("wedged".into(), SecondaryConnectionState::Handshaking(conn));

    // Its main loop provably runs: a mesh keepalive frame arrives.
    primary
        .dispatch_message(keepalive_from("wedged"), &mut None)
        .await
        .unwrap();
    primary.process_heartbeat_tick().await.unwrap();

    // Then it dies: total silence past the hard window, decider healthy.
    let mut removed = false;
    for _ in 0..6 {
        tokio::time::sleep(Duration::from_millis(60)).await;
        primary.process_heartbeat_tick().await.unwrap();
        if !primary.secondaries.contains_key("wedged") {
            removed = true;
            break;
        }
    }
    assert!(
        removed,
        "a keepalive-proven member must be silence-judged (and removed on \
         genuine death) regardless of its pre-Operational connection state"
    );
    assert!(!primary.cluster_state.is_peer_alive("wedged"));
}

/// Root-cause guard: a duplicate `SecondaryWelcome` (the setup loop's
/// handshake retry — routine, load-bearing) must NOT regress a walked
/// member's connection typestate. The bring-up walk's batch transitions
/// run once; a member re-buried into `Handshaking` mid-run would never
/// be walked back.
#[tokio::test(flavor = "current_thread")]
async fn duplicate_welcome_does_not_regress_walked_state() {
    let (transport, _sec_rx, _kept) = empty_transport();
    let (mut primary, _mesh) = build_primary(
        config(Duration::from_millis(50), 2),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator,
    );
    install_default_pool(&mut primary);
    register_operational_secondary(&mut primary, "sec-a", 0, "victim");

    primary
        .dispatch_message(welcome_from("sec-a"), &mut None)
        .await
        .unwrap();

    assert!(
        matches!(
            primary.secondaries.get("sec-a"),
            Some(SecondaryConnectionState::Operational(_))
        ),
        "a duplicate welcome (handshake retry) must not regress a walked \
         member's typestate — the batch walk never re-runs, so a regressed \
         member would sit pre-Operational (and silence-invisible) forever"
    );
}

// ======================================================================
// Collective-silence self-suspect gate (run_20260612_043357 replay)
// ======================================================================

/// Register the production topology of run_20260612_043357: the
/// primary's own co-located same-peer member (`"setup"` — the default
/// `node_id`, whose frames ride the in-process loopback) plus three
/// remote members, each holding one in-flight task.
fn register_colocated_plus_three_remotes<S, E>(primary: &mut PrimaryCoordinator<S, E, TestId>)
where
    S: Scheduler<TestId>,
    E: ResourceEstimator<TestId>,
{
    register_operational_secondary(primary, "setup", 0, "victim-self");
    register_operational_secondary(primary, "krater13", 1, "victim-13");
    register_operational_secondary(primary, "krater14", 2, "victim-14");
    register_operational_secondary(primary, "krater15", 3, "victim-15");
    // Seed the replicated membership (PeerJoined → Alive) so the
    // assertions on `is_peer_alive` exercise the real ledger flip on
    // removal, mirroring the lifecycle fixtures.
    for id in ["setup", "krater13", "krater14", "krater15"] {
        let _ = primary
            .cluster_state_mut_for_test()
            .apply(ClusterMutation::PeerJoined {
                peer_id: id.to_string(),
                is_observer: false,
                can_be_primary: false,
                cap_version: Default::default(),
                member_gen: 0,
            });
    }
}

/// One heartbeat round of the replay: sleep `gap`, refresh ONLY the
/// co-located member's death clock (its workers kept completing tasks
/// in the production run — in-process evidence, not wire evidence),
/// then drive the sweep.
async fn sweep_with_fresh_local<S, E>(
    primary: &mut PrimaryCoordinator<S, E, TestId>,
    gap: Duration,
) where
    S: Scheduler<TestId>,
    E: ResourceEstimator<TestId>,
{
    tokio::time::sleep(gap).await;
    primary.record_keepalive("setup");
    primary.process_heartbeat_tick().await.unwrap();
}

/// REPLAY (run_20260612_043357, the false mass-removal face): a primary
/// whose runtime is BURSTY-starved — isolated lagged ticks (each
/// deferred ACUTE by the own-tick gate, never two in a row, so the
/// chronic escalation never engages) interleaved with on-cadence ticks
/// — while its wire to ALL THREE remotes is deaf (no frame arrives at
/// any edge) and its co-located member keeps producing in-process
/// evidence. The on-cadence sweeps between the bursts judge wall-clock
/// silences that cross the hard backstop for every remote
/// simultaneously; pre-fix the first such sweep declared all three LIVE
/// remotes dead (then fleet-dead aborted the run). The self-suspect
/// gate must defer: all-remotes-silent is ONE local wire failure, not
/// three independent deaths.
#[tokio::test(flavor = "current_thread")]
async fn bursty_starved_deaf_primary_defers_mass_removal() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _sec_rx, _kept) = empty_transport();
            // interval 50ms → own-tick starvation threshold 150ms (3×);
            // hard backstop 8× = 400ms (and thus a 400ms gate
            // escalation window), so every assertion below sits ≥140ms
            // clear of a boundary.
            let mut cfg = config(Duration::from_millis(50), 2);
            cfg.silence_hard_multiple = 8;
            let (mut primary, _mesh) = build_primary(
                cfg,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator,
            );
            install_default_pool(&mut primary);
            register_colocated_plus_three_remotes(&mut primary);

            // Baseline on-cadence tick (seeds the own-tick clock; every
            // member fresh). From here NO remote frame ever arrives —
            // the wire is deaf in both directions.
            primary.process_heartbeat_tick().await.unwrap();

            // The production burst pattern: healthy ticks with isolated
            // >threshold gaps in between (the two ACUTE WARNs, ~100s
            // apart, chronic=false). Lagged ticks defer themselves; the
            // healthy ticks in between are the ones that judged — and
            // pre-fix killed — the fleet.
            for gap_ms in [60u64, 200, 60, 60, 60, 200, 60] {
                sweep_with_fresh_local(&mut primary, Duration::from_millis(gap_ms)).await;
            }

            // The wall silences have crossed the hard backstop for every
            // remote (total elapsed ≈ 700ms ≫ 400ms)...
            let report = primary.collect_heartbeat_report();
            let hard = Duration::from_millis(400);
            assert!(
                report
                    .silences
                    .iter()
                    .filter(|s| s.secondary_id != "setup")
                    .all(|s| s.silence > hard),
                "precondition: every remote's raw silence is past the hard backstop"
            );
            // ...yet NO remote was declared dead: all-silent-at-once is
            // self-suspect, and the deferral is visible to the
            // dispatch-altitude consumers too.
            for id in ["krater13", "krater14", "krater15"] {
                assert!(
                    primary.secondaries.contains_key(id),
                    "{id} must NOT be declared dead while EVERY remote is \
                     silent simultaneously (the run_20260612_043357 false \
                     mass-removal: the primary was deaf, the remotes were \
                     alive)"
                );
                assert!(
                    primary.cluster_state.is_peer_alive(id),
                    "{id}'s replicated membership must stay Alive under the deferral"
                );
            }
            assert!(
                primary.silent_secondary_ids().is_empty(),
                "the dispatch-altitude silent set must be empty while the \
                 self-suspect gate defers (no early lazy-requeue either)"
            );
            assert!(
                !primary.only_silent_held_work_remains(),
                "the lazy-requeue oracle must not fire off a self-suspect sweep"
            );
        })
        .await;
}

/// Recovery half of the replay: ONE remote frame proves the local wire
/// works, the collective episode ends, and the schedule resumes — the
/// still-silent remotes are then declared on the very next sweep (no
/// permanent amnesty), while the revived member and the co-located
/// member survive.
#[tokio::test(flavor = "current_thread")]
async fn remote_evidence_ends_deferral_and_remaining_silent_are_declared() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _sec_rx, _kept) = empty_transport();
            // hard backstop 2× = 100ms: short enough that the
            // post-recovery declarations land within a few sweeps.
            let (mut primary, _mesh) = build_primary(
                config(Duration::from_millis(50), 2),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator,
            );
            install_default_pool(&mut primary);
            register_colocated_plus_three_remotes(&mut primary);
            // After #544 the collective-silence gate escalates only when
            // slurm-authoritative evidence agrees the silent fleet is GONE.
            // This test's silent remotes are simulating exited processes,
            // so install a snapshot that reports them all `Gone` —
            // wall-clock plus authority confirmation is what the gate
            // needs to escalate.
            {
                use crate::authority_snapshot::{PeerLifeState, test_helpers::StaticSnapshot};
                let map = ["krater13", "krater14", "krater15"]
                    .iter()
                    .map(|id| ((*id).to_string(), PeerLifeState::Gone))
                    .collect();
                primary.set_authority_snapshot(std::sync::Arc::new(StaticSnapshot {
                    map,
                    count: None,
                }));
            }

            primary.process_heartbeat_tick().await.unwrap();

            // Two on-cadence sweeps with every remote silent past the
            // 100ms hard backstop: deferred (collective).
            sweep_with_fresh_local(&mut primary, Duration::from_millis(60)).await;
            sweep_with_fresh_local(&mut primary, Duration::from_millis(60)).await;
            assert!(
                ["krater13", "krater14", "krater15"]
                    .iter()
                    .all(|id| primary.secondaries.contains_key(*id)),
                "all remotes deferred while the collective episode holds"
            );

            // krater13's frame lands (the wire heals / was never the
            // remotes' fault): the episode is over. The OTHER two are
            // still genuinely silent past the backstop, and with live
            // remote evidence in hand the schedule must declare them —
            // bounded sweeps, no permanent amnesty.
            let mut declared = false;
            for _ in 0..8 {
                primary.record_keepalive("krater13");
                sweep_with_fresh_local(&mut primary, Duration::from_millis(60)).await;
                if !primary.secondaries.contains_key("krater14")
                    && !primary.secondaries.contains_key("krater15")
                {
                    declared = true;
                    break;
                }
            }
            assert!(
                declared,
                "once a remote frame proves the wire, the still-silent \
                 remotes must be declared dead on the normal schedule"
            );
            assert!(
                primary.secondaries.contains_key("krater13"),
                "the revived remote survives"
            );
            assert!(
                primary.secondaries.contains_key("setup"),
                "the co-located member survives"
            );
        })
        .await;
}

/// BOUNDED deferral (the hard backstop stays load-bearing): a fleet
/// whose every remote is GENUINELY dead (the cohort-3 face — tunnel
/// blips killed all secondaries at once) is still fully declared after
/// the gate's escalation window, so requeue/respawn/fleet-dead all
/// remain reachable. The sweep defers first (the gate engaged), then
/// escalates and declares.
#[tokio::test(flavor = "current_thread")]
async fn collective_silence_escalates_and_declares_after_bounded_window() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _sec_rx, _kept) = empty_transport();
            // hard 2× = 100ms; escalation window = the same 100ms.
            let (mut primary, _mesh) = build_primary(
                config(Duration::from_millis(50), 2),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator,
            );
            install_default_pool(&mut primary);
            register_colocated_plus_three_remotes(&mut primary);
            // #544: install an authoritative snapshot reporting all
            // remotes Gone so the escalation can fire — the gate now
            // requires slurm-authoritative confirmation in addition to
            // the wall-clock window having elapsed.
            {
                use crate::authority_snapshot::{PeerLifeState, test_helpers::StaticSnapshot};
                let map = ["krater13", "krater14", "krater15"]
                    .iter()
                    .map(|id| ((*id).to_string(), PeerLifeState::Gone))
                    .collect();
                primary.set_authority_snapshot(std::sync::Arc::new(StaticSnapshot {
                    map,
                    count: None,
                }));
            }

            primary.process_heartbeat_tick().await.unwrap();

            // Drive on-cadence sweeps with the co-located member fresh
            // and every remote permanently silent. The gate must defer
            // at least one hard-due sweep (proof it engaged), then
            // escalate within a bounded number of sweeps and let the
            // schedule declare the whole remote fleet.
            let mut deferred_a_hard_due_sweep = false;
            let mut all_declared_at = None;
            for tick in 0..20 {
                sweep_with_fresh_local(&mut primary, Duration::from_millis(60)).await;
                let remotes_present = ["krater13", "krater14", "krater15"]
                    .iter()
                    .filter(|id| primary.secondaries.contains_key(**id))
                    .count();
                let report = primary.collect_heartbeat_report();
                let past_hard = report
                    .silences
                    .iter()
                    .filter(|s| s.secondary_id != "setup")
                    .filter(|s| s.silence > Duration::from_millis(100))
                    .count();
                if remotes_present == 3 && past_hard == 3 {
                    deferred_a_hard_due_sweep = true;
                }
                if remotes_present == 0 {
                    all_declared_at = Some(tick);
                    break;
                }
            }
            assert!(
                deferred_a_hard_due_sweep,
                "the gate must first DEFER a sweep where every remote is \
                 past the hard backstop (otherwise this test is not \
                 exercising the escalation at all)"
            );
            assert!(
                all_declared_at.is_some(),
                "a genuinely all-dead remote fleet must still be fully \
                 declared once the bounded escalation window elapses — \
                 the hard backstop is the load-bearing forward-progress \
                 guarantee (fleet-dead must stay reachable)"
            );
            for id in ["krater13", "krater14", "krater15"] {
                assert!(
                    !primary.cluster_state.is_peer_alive(id),
                    "{id}'s replicated membership must be Dead after escalation"
                );
            }
            assert!(
                primary.secondaries.contains_key("setup"),
                "the co-located member (fresh evidence every round) survives"
            );
            assert_eq!(
                primary.pool().iter().count(),
                3,
                "the three remote-held in-flight tasks are requeued on declaration"
            );
        })
        .await;
}

// ======================================================================
// Observer-directed keepalive + re-point fan (the late-joined-observer
// keepalive blackout, owner logs 2026-06-11): a relay-only observer
// member receives NO `Destination::All` frame — the transport's
// broadcast is a fire-once fan over its DIRECT connections only — so
// the PRIMARY-class frames the observer's liveness judgment keys on
// (the keepalive and the `PrimaryChanged` re-point) must ALSO ride the
// DIRECTED `Destination::Observer(id)` edge, which the transport router
// relays through a connected sibling toward a not-directly-connected
// target.
// ======================================================================

/// Recorded DIRECTED egress: `(peer_id, frame)` pairs.
type DirectedLog = Arc<std::sync::Mutex<Vec<(String, DistributedMessage<TestId>)>>>;
/// Recorded broadcast egress frames.
type BroadcastLog = Arc<std::sync::Mutex<Vec<DistributedMessage<TestId>>>>;

/// Transport replaying the production relay-only-observer topology:
/// `broadcast` fans over the DIRECT connection table (here: none — the
/// observer has no direct leg to this host), while a DIRECTED
/// `send_to_peer` is relayable toward any peer (the Router-forwarder
/// path). Both egress classes are recorded so a test can assert exactly
/// which class reached whom.
struct RelayOnlyRecordingTransport {
    directed: DirectedLog,
    broadcasts: BroadcastLog,
}

impl PeerTransport<TestId> for RelayOnlyRecordingTransport {
    async fn broadcast(&mut self, msg: DistributedMessage<TestId>) -> Result<(), String> {
        self.broadcasts
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(msg);
        Ok(())
    }

    async fn send_to_peer(
        &mut self,
        peer_id: &str,
        msg: DistributedMessage<TestId>,
    ) -> Result<(), String> {
        self.directed
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push((peer_id.to_string(), msg));
        Ok(())
    }

    async fn recv_peer(&mut self) -> Option<DistributedMessage<TestId>> {
        std::future::pending().await
    }

    fn try_recv_peer(&mut self) -> Option<DistributedMessage<TestId>> {
        None
    }

    fn peer_count(&self) -> usize {
        0
    }

    fn has_peer(&self, _id: &PeerId) -> bool {
        false
    }

    async fn connect_to_peers(&mut self, _peers: &[PeerConnectionInfo]) {}
}

/// Count the recorded DIRECTED frames to `peer_id` matching `pred`.
fn directed_count(
    directed: &DirectedLog,
    peer_id: &str,
    pred: impl Fn(&DistributedMessage<TestId>) -> bool,
) -> usize {
    directed
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .iter()
        .filter(|(id, msg)| id == peer_id && pred(msg))
        .count()
}

/// True iff `msg` is a Primary-role keepalive.
fn is_primary_keepalive(msg: &DistributedMessage<TestId>) -> bool {
    matches!(
        msg,
        DistributedMessage::Keepalive {
            emitter_role: dynrunner_protocol_primary_secondary::KeepaliveRole::Primary,
            ..
        }
    )
}

/// True iff `msg` is a `ClusterMutation` frame carrying a
/// `PrimaryChanged` re-point naming `new`.
fn is_repoint_to(msg: &DistributedMessage<TestId>, new: &str) -> bool {
    matches!(
        msg,
        DistributedMessage::ClusterMutation { mutations, .. }
            if mutations.iter().any(
                |m| matches!(m, ClusterMutation::PrimaryChanged { new: n, .. } if n == new)
            )
    )
}

/// ROOT-CAUSE pin (the production blackout): the primary's keepalive
/// must reach every OBSERVER-role member of the replicated roster as a
/// DIRECTED `Destination::Observer(id)` send — the broadcast alone never
/// reaches a relay-only observer, which then ingests live CRDT gossip
/// from its own direct peers while declaring the named primary silent.
///
/// Covers BOTH observer kinds: the LATE-JOINED observer (seated through
/// the primary's `handle_request_snapshot_stream` responder, which
/// originates its `PeerJoined { is_observer: true }`) and the RELOCATED
/// submitter-observer (its observer role recorded in the replicated
/// capability roster by the responders' `PeerJoined { is_observer:
/// true }` for the demoted id).
#[tokio::test(flavor = "current_thread")]
async fn primary_keepalive_fan_reaches_observer_members_directed() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let directed = Arc::new(std::sync::Mutex::new(Vec::new()));
            let broadcasts = Arc::new(std::sync::Mutex::new(Vec::new()));
            let transport = RelayOnlyRecordingTransport {
                directed: directed.clone(),
                broadcasts: broadcasts.clone(),
            };
            let mut cfg = config(Duration::from_millis(50), 2);
            cfg.node_id = "the-primary".into();
            let (mut primary, _mesh) = build_primary_pumped(
                cfg,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator,
            );

            // Observer kind 1 — LATE-JOINED: seated through the snapshot
            // responder (the production seat path), which originates
            // `PeerJoined { is_observer: true }` for the requester.
            primary
                .handle_request_snapshot_stream(DistributedMessage::RequestSnapshotStream {
                    target: None,
                    sender_id: "obs-late".into(),
                    timestamp: 0.0,
                    stream_id: "obs-late/0".into(),
                    resume_after: None,
                    task_ranges: Vec::new(),
                    is_observer: true,
                    can_be_primary: false,
                })
                .await;
            // Observer kind 2 — RELOCATED submitter-observer: its observer
            // role lands in the replicated capability roster via the same
            // `PeerJoined { is_observer: true }` mutation the responders
            // originate for the demoted id.
            primary
                .apply_and_broadcast_cluster_mutations(vec![ClusterMutation::PeerJoined {
                    peer_id: "obs-reloc".into(),
                    is_observer: true,
                    can_be_primary: false,
                    cap_version: Default::default(),
                    member_gen: 0,
                }])
                .await;
            for obs in ["obs-late", "obs-reloc"] {
                assert!(
                    primary
                        .cluster_state
                        .role_table()
                        .observers
                        .contains(obs),
                    "precondition: {obs} is an observer-role member"
                );
            }

            primary.broadcast_primary_keepalive().await;
            crate::primary::tests::settle_pump().await;

            for obs in ["obs-late", "obs-reloc"] {
                assert_eq!(
                    directed_count(&directed, obs, is_primary_keepalive),
                    1,
                    "the keepalive fan must DIRECT one Primary-role keepalive \
                     at observer member {obs} (the broadcast never reaches a \
                     relay-only observer)"
                );
            }
            assert_eq!(
                broadcasts
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .iter()
                    .filter(|m| is_primary_keepalive(m))
                    .count(),
                1,
                "the mesh broadcast (the direct-leg fan) still fires exactly once"
            );
        })
        .await;
}

/// RE-POINT pin: the `PrimaryChanged` announcement — the only other
/// frame class the observer's `primary_last_seen` clock accepts — must
/// also reach observer members DIRECTED, from BOTH origination sites:
/// `activate_local_primary` (bootstrap + every promotion converge here)
/// and `relocate_primary_to` (the submitter handoff). The re-assert at
/// an already-held epoch NoOps off the broadcast wire entirely, so
/// without the directed fan a relay-only observer can NEVER learn the
/// holder from the authority itself.
#[tokio::test(flavor = "current_thread")]
async fn primary_repoint_fan_reaches_observer_members_directed() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let directed = Arc::new(std::sync::Mutex::new(Vec::new()));
            let broadcasts = Arc::new(std::sync::Mutex::new(Vec::new()));
            let transport = RelayOnlyRecordingTransport {
                directed: directed.clone(),
                broadcasts: broadcasts.clone(),
            };
            let mut cfg = config(Duration::from_millis(50), 2);
            cfg.node_id = "the-primary".into();
            let (mut primary, _mesh) = build_primary_pumped(
                cfg,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator,
            );
            primary
                .apply_and_broadcast_cluster_mutations(vec![ClusterMutation::PeerJoined {
                    peer_id: "obs-1".into(),
                    is_observer: true,
                    can_be_primary: false,
                    cap_version: Default::default(),
                    member_gen: 0,
                }])
                .await;

            // Bootstrap/promotion convergence point: the uniform announce.
            primary
                .activate_local_primary()
                .await
                .expect("activate_local_primary");
            crate::primary::tests::settle_pump().await;
            assert_eq!(
                directed_count(&directed, "obs-1", |m| is_repoint_to(m, "the-primary")),
                1,
                "activate_local_primary must DIRECT its PrimaryChanged \
                 re-point at the observer member"
            );

            // The re-assert path (current_primary already == self) NoOps
            // off the broadcast wire — the directed fan must still carry
            // the authoritative identity to the observer.
            primary
                .activate_local_primary()
                .await
                .expect("re-assert activate_local_primary");
            crate::primary::tests::settle_pump().await;
            assert_eq!(
                directed_count(&directed, "obs-1", |m| is_repoint_to(m, "the-primary")),
                2,
                "the already-self re-assert (a broadcast NoOp) must still \
                 DIRECT the re-point at the observer member"
            );

            // The submitter-relocation handoff re-point.
            primary.relocate_primary_to("sec-9".into()).await;
            crate::primary::tests::settle_pump().await;
            assert_eq!(
                directed_count(&directed, "obs-1", |m| is_repoint_to(m, "sec-9")),
                1,
                "relocate_primary_to must DIRECT its PrimaryChanged re-point \
                 at the observer member"
            );
        })
        .await;
}

/// Pre-start fence A (#530a) — the requeue-stamp invariant: when
/// `requeue_dead_secondary` turns a peer-removed member's in-flight
/// tasks back to `Pending`, every `TaskRequeued` mutation must seed an
/// entry in `supplanted_holders` keyed by hash, recording the dead
/// member's identity AND its `peer_member_gen` AS IT STOOD BEFORE the
/// `PeerRemoved` killed the incarnation. The next dispatch reads this
/// to stamp the wire frame's `supplanted_holder` field; a primary
/// failover before re-dispatch loses the hint by design.
#[tokio::test(flavor = "current_thread")]
async fn requeue_dead_secondary_records_supplanted_holders() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, mut _sec_rxs, _incoming_tx) = two_secondary_transport();
            let (mut primary, _mesh) = build_primary_pumped(
                config(Duration::from_millis(50), 2),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator,
            );
            install_default_pool(&mut primary);
            register_operational_secondary(&mut primary, "sec-a", 0, "victim-a");
            register_operational_secondary(&mut primary, "sec-b", 1, "victim-b");

            // Capture the supplanted member_gen BEFORE the death — this is
            // exactly the read the stamp performs in production: at requeue
            // time, BEFORE `PeerRemoved` is applied.
            let gen_before = primary.cluster_state_for_test().peer_member_gen("sec-a");
            // sec-a's hash is the deterministic hash of its in-flight TaskInfo.
            // Snapshot the ledger entry's key (one entry per holder in this
            // fixture) so we can assert against it after the death.
            let victim_hash: String = primary
                .in_flight_for_test()
                .iter()
                .find(|(_, e)| e.secondary_id == "sec-a")
                .map(|(h, _)| h.clone())
                .expect("fixture invariant: sec-a holds one in-flight entry");

            // Pre-death: no hint yet.
            assert_eq!(
                primary.supplanted_holders_len_for_test(),
                0,
                "no fence hint before any death"
            );

            // Drive sec-a through the full death path — handle_secondary_fatal_error
            // → requeue_dead_secondary → recover_inflight_for_dead_secondary +
            // PeerRemoved apply.
            let fatal = DistributedMessage::<TestId>::SecondaryFatalError {
                target: None,
                sender_id: "sec-a".into(),
                timestamp: 0.0,
                secondary_id: "sec-a".into(),
                error: "test-driven fatal death".into(),
            };
            primary.handle_secondary_fatal_error(fatal).await.unwrap();
            crate::primary::tests::settle_pump().await;

            // Post-death: the fence hint exists for the requeued hash, naming
            // sec-a at the pre-death incarnation.
            assert_eq!(
                primary.supplanted_holders_len_for_test(),
                1,
                "one fence hint per requeued task"
            );
            assert_eq!(
                primary.supplanted_holder_for_test(&victim_hash),
                Some(("sec-a".into(), gen_before)),
                "fence hint must name the supplanted holder and its pre-death gen"
            );
            // Sanity: post-PeerRemoved sec-a is no longer alive.
            assert!(
                !primary.cluster_state_for_test().is_peer_alive("sec-a"),
                "sec-a is removed in the CRDT after the death"
            );
        })
        .await;
}

/// Pre-start fence A (#530a) — the terminal-drop invariant: every
/// terminal-settlement path that drops the in-flight ledger entry must
/// also drop the side-map entry symmetrically, so a hint can never
/// outlive the task it fences. Drives the `TaskComplete` path; the
/// failure path is exercised symmetrically inside the same wire handler.
#[tokio::test(flavor = "current_thread")]
async fn supplanted_holder_drops_on_task_complete_terminal() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _sec_rxs, _incoming_tx) = two_secondary_transport();
            let (mut primary, _mesh) = build_primary_pumped(
                config(Duration::from_millis(50), 2),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator,
            );
            install_default_pool(&mut primary);
            register_operational_secondary(&mut primary, "sec-b", 0, "redirected");

            // The redirected dispatch is on sec-b, but the side-map records
            // that it ORIGINATED as a requeue of sec-a's prior in-flight
            // entry. Production stamps this in `requeue_dead_secondary` (the
            // sibling test above); here we install it directly to keep the
            // test focused on terminal drop semantics.
            let redirected_hash: String = primary
                .in_flight_for_test()
                .iter()
                .find(|(_, e)| e.secondary_id == "sec-b")
                .map(|(h, _)| h.clone())
                .expect("fixture invariant: sec-b holds one in-flight entry");
            primary
                .install_supplanted_holder_for_test(&redirected_hash, "sec-a", 1);
            assert_eq!(
                primary.supplanted_holders_len_for_test(),
                1,
                "fixture precondition: the side-map carries the redirect hint"
            );

            // The redirect completes successfully. The terminal path must
            // drop the in-flight ledger entry AND the supplanted-holder hint
            // symmetrically.
            let complete = DistributedMessage::<TestId>::TaskComplete {
                target: None,
                sender_id: "sec-b".into(),
                timestamp: 0.0,
                secondary_id: "sec-b".into(),
                worker_id: 0,
                task_hash: redirected_hash.clone(),
                result_data: None,
                delivery_seq: None,
                msgs_posted_through: None,
            };
            primary.handle_task_complete(complete, &mut None).await;

            assert_eq!(
                primary.supplanted_holders_len_for_test(),
                0,
                "the side-map entry must be dropped on terminal completion \
                 (symmetric with the in-flight ledger drop)"
            );
            assert_eq!(
                primary.supplanted_holder_for_test(&redirected_hash),
                None
            );
        })
        .await;
}
