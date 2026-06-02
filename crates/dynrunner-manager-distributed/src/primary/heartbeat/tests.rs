use std::collections::HashMap;
use std::time::Duration;

use dynrunner_core::{
    BoundedString, PhaseId, ResourceMap, SoftPreferredSecondaries, TaskInfo, TypeId,
};
use dynrunner_protocol_primary_secondary::{ClusterMutation, DistributedMessage, RemovalCause};
use dynrunner_scheduler::ResourceStealingScheduler;
use dynrunner_transport_channel::ChannelSecondaryTransportEnd;
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
fn install_default_pool<T, P, S, E>(
    primary: &mut PrimaryCoordinator<T, P, S, E, TestId>,
) where
    T: dynrunner_protocol_primary_secondary::SecondaryTransport<TestId>,
    P: dynrunner_protocol_primary_secondary::PeerTransport<TestId>,
    S: dynrunner_scheduler_api::Scheduler<TestId>,
    E: ResourceEstimator<TestId>,
{
    let phase = PhaseId::from("default");
    let pool = PendingPool::<TestId>::new(
        [phase.clone()],
        std::collections::HashMap::new(),
    )
    .expect("default-phase pool");
    primary.pending = Some(pool);
    primary.phase_completed.insert(phase.clone(), 0);
    primary.phase_failed.insert(phase, 0);
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct TestId(String);

#[derive(Clone)]
struct FixedEstimator;
impl ResourceEstimator<TestId> for FixedEstimator {
    fn estimate(&self, _task: &dynrunner_core::TaskInfo<TestId>) -> ResourceMap {
        ResourceMap::from([(dynrunner_core::ResourceKind::memory(), 1)])
    }
}

fn config(keepalive_interval: Duration, miss_threshold: u32) -> PrimaryConfig {
    PrimaryConfig {
        node_id: "primary".into(),
        num_secondaries: 1,
        connect_timeout: Duration::from_secs(5),
        peer_timeout: Duration::from_secs(5),
        keepalive_interval,
        keepalive_miss_threshold: miss_threshold,
        source_pre_staged_root: None,
                uses_file_based_items: true,
                required_setup_on_promote: false,
        max_concurrent_per_type: std::collections::HashMap::new(),
        retry_max_passes: 1,
        oom_retry_max_passes: 1,
        fleet_dead_timeout: std::time::Duration::from_secs(30),
        mesh_ready_timeout: std::time::Duration::from_secs(5),
        // Default OFF in legacy heartbeat tests — they assert the
        // `requeue_dead_secondary` immediate path. Tests that
        // exercise the mass-death path build their own config.
        mass_death_grace: Duration::ZERO,
        mass_death_min_count: 2,
        source_dir: None,
        unfulfillable_reinject_max_per_task: None,
        setup_promote_deadline: std::time::Duration::from_secs(600),
    }
}

fn empty_transport() -> (
    ChannelSecondaryTransportEnd<TestId>,
    tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
    tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
) {
    let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
    let (sec_tx, sec_rx) = tokio_mpsc::unbounded_channel();
    let mut outgoing = HashMap::new();
    outgoing.insert("dead-sec".into(), sec_tx);
    (
        ChannelSecondaryTransportEnd {
            outgoing,
            incoming_rx,
        },
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
    let mut primary: PrimaryCoordinator<_, _, _, _, TestId> = PrimaryCoordinator::new(
        config(Duration::from_millis(50), 2),
        transport,
        dynrunner_transport_quic::NoPeerTransport,
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
        .receive_welcome(1, vec![], "host".into(), 0, None, false)
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
        resolved_path: None,
    };
    primary.stage_in_flight_for_test("dead-sec".into(), 0, in_flight.clone());

    // Sleep past `keepalive_interval * miss_threshold` so the deadline
    // expires, then collect the report.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let report = primary.collect_heartbeat_report();
    assert_eq!(report.dead.len(), 1);
    assert_eq!(report.dead[0].secondary_id, "dead-sec");

    for dead in report.dead {
        primary
            .requeue_dead_secondary(dead, RemovalCause::KeepaliveMiss)
            .await
            .unwrap();
    }

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
    ChannelSecondaryTransportEnd<TestId>,
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
        ChannelSecondaryTransportEnd {
            outgoing,
            incoming_rx,
        },
        vec![a_rx, b_rx],
        incoming_tx,
    )
}

/// Helper: register a secondary in Operational state with a single
/// in-flight task. Mirrors the setup pattern of
/// `dead_secondary_requeues_in_flight_task` but parametrised by id
/// so the mass-death tests can stage two of them.
fn register_operational_secondary<T, P, S, E>(
    primary: &mut PrimaryCoordinator<T, P, S, E, TestId>,
    secondary_id: &str,
    worker_id: u32,
    in_flight_label: &str,
) where
    T: dynrunner_protocol_primary_secondary::SecondaryTransport<TestId>,
    P: dynrunner_protocol_primary_secondary::PeerTransport<TestId>,
    S: dynrunner_scheduler_api::Scheduler<TestId>,
    E: ResourceEstimator<TestId>,
{
    let conn = SecondaryConnection::new(secondary_id.into())
        .receive_welcome(1, vec![], "host".into(), 0, None, false)
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
            resolved_path: None,
        },
    );
}

fn config_with_mass_death(
    keepalive_interval: Duration,
    miss_threshold: u32,
    grace: Duration,
    min_count: u32,
) -> PrimaryConfig {
    let mut cfg = config(keepalive_interval, miss_threshold);
    cfg.mass_death_grace = grace;
    cfg.mass_death_min_count = min_count;
    cfg
}

/// When EVERY connected secondary appears dead at the same
/// heartbeat tick (and there are at least `mass_death_min_count`
/// of them), the framework infers a correlated cause and DEFERS
/// the requeue. Tasks remain in-flight; `pending_mass_death`
/// tracks the deferred set. Pre-fix the primary requeued every
/// secondary immediately, evicted the entire fleet, and burned
/// the retry budget on what was actually a transient gateway-side
/// blip — observed in tokenizer's cohort-5 dispatch where 197
/// in-flight tasks were lost to a 15-second tunnel hiccup.
#[tokio::test(flavor = "current_thread")]
async fn mass_death_defers_requeue_when_all_secondaries_silent() {
    let (transport, _sec_rxs, _incoming_tx) = two_secondary_transport();
    let mut primary: PrimaryCoordinator<_, _, _, _, TestId> =
        PrimaryCoordinator::new(
            config_with_mass_death(
                Duration::from_millis(50),
                2,
                Duration::from_secs(60),
                2,
            ),
            transport,
            dynrunner_transport_quic::NoPeerTransport,
            ResourceStealingScheduler::memory(),
            FixedEstimator,
        );
    install_default_pool(&mut primary);
    register_operational_secondary(&mut primary, "sec-a", 0, "victim-a");
    register_operational_secondary(&mut primary, "sec-b", 1, "victim-b");

    // Sleep past the deadline so both appear in the dead list.
    tokio::time::sleep(Duration::from_millis(200)).await;
    primary.process_heartbeat_tick().await.unwrap();

    // BOTH secondaries deferred — pending_mass_death population
    // matches the connected fleet, no requeue happened, no
    // workers evicted, pool still empty (tasks remain in-flight
    // on `workers[].current_task`).
    assert_eq!(primary.pending_mass_death.len(), 2);
    assert!(primary.pending_mass_death.contains_key("sec-a"));
    assert!(primary.pending_mass_death.contains_key("sec-b"));
    assert_eq!(primary.workers.len(), 2, "no workers evicted");
    assert_eq!(primary.pool().len(), 0, "no tasks requeued");
    assert_eq!(primary.secondaries.len(), 2, "secondaries still registered");
}

/// During mass-death grace, a secondary whose keepalive resumes
/// is silently un-deferred — no requeue, no logged death. The
/// other deferred peer stays pending until it either recovers or
/// the grace expires.
#[tokio::test(flavor = "current_thread")]
async fn mass_death_recovery_during_grace_undefers_secondary() {
    let (transport, _sec_rxs, _incoming_tx) = two_secondary_transport();
    let mut primary: PrimaryCoordinator<_, _, _, _, TestId> =
        PrimaryCoordinator::new(
            config_with_mass_death(
                Duration::from_millis(50),
                2,
                Duration::from_secs(60),
                2,
            ),
            transport,
            dynrunner_transport_quic::NoPeerTransport,
            ResourceStealingScheduler::memory(),
            FixedEstimator,
        );
    install_default_pool(&mut primary);
    register_operational_secondary(&mut primary, "sec-a", 0, "victim-a");
    register_operational_secondary(&mut primary, "sec-b", 1, "victim-b");

    tokio::time::sleep(Duration::from_millis(200)).await;
    primary.process_heartbeat_tick().await.unwrap();
    assert_eq!(primary.pending_mass_death.len(), 2, "both deferred");

    // sec-a's keepalive resumes — simulate by recording a fresh one.
    primary.record_keepalive("sec-a");
    primary.process_heartbeat_tick().await.unwrap();

    // sec-a un-deferred (back in the live fleet), sec-b still
    // deferred. No requeue happened for either.
    assert!(!primary.pending_mass_death.contains_key("sec-a"));
    assert!(primary.pending_mass_death.contains_key("sec-b"));
    assert_eq!(primary.workers.len(), 2, "no workers evicted");
    assert_eq!(primary.pool().len(), 0, "no tasks requeued");
}

/// A single-secondary death is NOT mass-death; the legacy
/// per-secondary requeue path runs unchanged. Guards against
/// over-eager mass detection swallowing every death.
#[tokio::test(flavor = "current_thread")]
async fn solo_death_with_live_peers_takes_legacy_requeue_path() {
    let (transport, _sec_rxs, _incoming_tx) = two_secondary_transport();
    let mut primary: PrimaryCoordinator<_, _, _, _, TestId> =
        PrimaryCoordinator::new(
            config_with_mass_death(
                Duration::from_millis(50),
                2,
                Duration::from_secs(60),
                2,
            ),
            transport,
            dynrunner_transport_quic::NoPeerTransport,
            ResourceStealingScheduler::memory(),
            FixedEstimator,
        );
    install_default_pool(&mut primary);
    register_operational_secondary(&mut primary, "sec-a", 0, "victim-a");
    register_operational_secondary(&mut primary, "sec-b", 1, "victim-b");

    // Only sec-a is past the deadline; sec-b is still fresh.
    tokio::time::sleep(Duration::from_millis(200)).await;
    primary.record_keepalive("sec-b");
    primary.process_heartbeat_tick().await.unwrap();

    // sec-a went through the legacy path (requeue + evict + drop
    // from secondaries); sec-b is unaffected. Mass-death pending
    // stays empty — the rule didn't trip.
    assert_eq!(primary.pending_mass_death.len(), 0);
    assert!(!primary.secondaries.contains_key("sec-a"));
    assert!(primary.secondaries.contains_key("sec-b"));
    assert_eq!(primary.pool().len(), 1, "sec-a's task requeued");
    assert_eq!(primary.workers.len(), 1, "only sec-b's worker remains");
}

/// `mass_death_grace = ZERO` reverts to legacy "requeue every
/// dead secondary immediately" behaviour even when every connected
/// peer dies at the same tick — the disable knob.
#[tokio::test(flavor = "current_thread")]
async fn mass_death_disabled_when_grace_is_zero() {
    let (transport, _sec_rxs, _incoming_tx) = two_secondary_transport();
    let mut primary: PrimaryCoordinator<_, _, _, _, TestId> =
        PrimaryCoordinator::new(
            config_with_mass_death(
                Duration::from_millis(50),
                2,
                Duration::ZERO,
                2,
            ),
            transport,
            dynrunner_transport_quic::NoPeerTransport,
            ResourceStealingScheduler::memory(),
            FixedEstimator,
        );
    install_default_pool(&mut primary);
    register_operational_secondary(&mut primary, "sec-a", 0, "victim-a");
    register_operational_secondary(&mut primary, "sec-b", 1, "victim-b");

    tokio::time::sleep(Duration::from_millis(200)).await;
    primary.process_heartbeat_tick().await.unwrap();

    // Both requeued immediately — no deferral.
    assert_eq!(primary.pending_mass_death.len(), 0);
    assert_eq!(primary.workers.len(), 0, "all workers evicted");
    assert_eq!(primary.pool().len(), 2, "both tasks requeued");
    assert!(primary.secondaries.is_empty());
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

/// Independent / partial-death path: a single secondary misses the
/// keepalive threshold while peers stay alive. The primary
/// originates one `PeerRemoved { cause: KeepaliveMiss }` per dead
/// secondary. Pins the call-site cause wiring (`process_heartbeat_tick`
/// else-branch).
#[tokio::test(flavor = "current_thread")]
async fn requeue_dead_secondary_emits_peer_removed_with_keepalive_miss_cause() {
    let (transport, mut sec_rxs, _incoming_tx) = two_secondary_transport();
    let mut primary: PrimaryCoordinator<_, _, _, _, TestId> =
        PrimaryCoordinator::new(
            config_with_mass_death(
                Duration::from_millis(50),
                2,
                Duration::from_secs(60),
                2,
            ),
            transport,
            dynrunner_transport_quic::NoPeerTransport,
            ResourceStealingScheduler::memory(),
            FixedEstimator,
        );
    install_default_pool(&mut primary);
    register_operational_secondary(&mut primary, "sec-a", 0, "victim-a");
    register_operational_secondary(&mut primary, "sec-b", 1, "victim-b");

    // Only sec-a misses the deadline (sec-b is refreshed below), so
    // the mass-death rule does NOT trip and the else-branch runs.
    tokio::time::sleep(Duration::from_millis(200)).await;
    primary.record_keepalive("sec-b");
    primary.process_heartbeat_tick().await.unwrap();

    // Drain BOTH receivers — broadcast goes to every entry in the
    // outgoing map. Either drain sees the same PeerRemoved payload;
    // we read sec-b's because the dead one's channel may still be
    // sending its TimeoutDetected first.
    let removed_a = collect_peer_removed(&mut sec_rxs[0]);
    let removed_b = collect_peer_removed(&mut sec_rxs[1]);
    let merged = if !removed_b.is_empty() { removed_b } else { removed_a };
    assert_eq!(
        merged.len(),
        1,
        "exactly one PeerRemoved must originate per single death; got {merged:?}",
    );
    assert_eq!(merged[0].0, "sec-a");
    assert_eq!(merged[0].1, RemovalCause::KeepaliveMiss);
}

/// Mass-death finalize path: every connected secondary goes silent
/// at the same tick → defer; after the grace window elapses without
/// recovery, the primary escalates each deferred entry to actual
/// death and originates `PeerRemoved { cause: MassDeathEscalation }`.
/// Pins the call-site cause wiring (mass-death finalize loop).
///
/// Real-time sleeps (not paused tokio time) because the heartbeat
/// path measures via `std::time::Instant::now`, which
/// `tokio::time::advance` doesn't move.
#[tokio::test(flavor = "current_thread")]
async fn requeue_dead_secondary_emits_peer_removed_with_mass_death_escalation_cause() {
    let (transport, mut sec_rxs, _incoming_tx) = two_secondary_transport();
    let mut primary: PrimaryCoordinator<_, _, _, _, TestId> =
        PrimaryCoordinator::new(
            config_with_mass_death(
                Duration::from_millis(50),
                2,
                Duration::from_millis(200),
                2,
            ),
            transport,
            dynrunner_transport_quic::NoPeerTransport,
            ResourceStealingScheduler::memory(),
            FixedEstimator,
        );
    install_default_pool(&mut primary);
    register_operational_secondary(&mut primary, "sec-a", 0, "victim-a");
    register_operational_secondary(&mut primary, "sec-b", 1, "victim-b");

    // First tick: both silent past the deadline → deferred, no
    // PeerRemoved authored yet (the entry-deferral path is silent
    // per the operative rule).
    tokio::time::sleep(Duration::from_millis(200)).await;
    primary.process_heartbeat_tick().await.unwrap();
    assert_eq!(primary.pending_mass_death.len(), 2, "both deferred");
    assert!(
        collect_peer_removed(&mut sec_rxs[0]).is_empty(),
        "entry-deferral must not author PeerRemoved (operative rule)"
    );
    assert!(
        collect_peer_removed(&mut sec_rxs[1]).is_empty(),
        "entry-deferral must not author PeerRemoved (operative rule)"
    );

    // Sleep past the grace window without recovery → finalize.
    tokio::time::sleep(Duration::from_millis(250)).await;
    primary.process_heartbeat_tick().await.unwrap();

    // One PeerRemoved per finalized secondary, all carrying
    // MassDeathEscalation. Both receivers receive each broadcast
    // (broadcast iterates the outgoing map), so reading either is
    // sufficient — drain both and merge.
    let mut removed = collect_peer_removed(&mut sec_rxs[0]);
    removed.extend(collect_peer_removed(&mut sec_rxs[1]));
    // De-dup by id (each finalize broadcasts once; both channels
    // see the same broadcast).
    removed.sort_by(|a, b| a.0.cmp(&b.0));
    removed.dedup();
    assert_eq!(
        removed.len(),
        2,
        "one PeerRemoved per finalized secondary; got {removed:?}"
    );
    for (_, cause) in &removed {
        assert_eq!(*cause, RemovalCause::MassDeathEscalation);
    }
    let ids: std::collections::HashSet<&str> =
        removed.iter().map(|(id, _)| id.as_str()).collect();
    assert!(ids.contains("sec-a"));
    assert!(ids.contains("sec-b"));
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
    let mut primary: PrimaryCoordinator<_, _, _, _, TestId> =
        PrimaryCoordinator::new(
            config(Duration::from_millis(50), 2),
            transport,
            dynrunner_transport_quic::NoPeerTransport,
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

/// Negative pin (operative rule: "PeerRemoved fires only post-
/// mass-death-grace"): while a secondary is deferred during the
/// mass-death grace window, NO `PeerRemoved` mutation is authored.
/// The hook fires only on the finalize path (covered by the
/// `MassDeathEscalation` test above); a recovery during the grace
/// window drops the deferred entry silently.
#[tokio::test(flavor = "current_thread")]
async fn mass_death_grace_entry_deferral_does_not_fire_peer_removed() {
    let (transport, mut sec_rxs, _incoming_tx) = two_secondary_transport();
    let mut primary: PrimaryCoordinator<_, _, _, _, TestId> =
        PrimaryCoordinator::new(
            config_with_mass_death(
                Duration::from_millis(50),
                2,
                Duration::from_secs(60),
                2,
            ),
            transport,
            dynrunner_transport_quic::NoPeerTransport,
            ResourceStealingScheduler::memory(),
            FixedEstimator,
        );
    install_default_pool(&mut primary);
    register_operational_secondary(&mut primary, "sec-a", 0, "victim-a");
    register_operational_secondary(&mut primary, "sec-b", 1, "victim-b");

    tokio::time::sleep(Duration::from_millis(200)).await;
    primary.process_heartbeat_tick().await.unwrap();
    assert_eq!(
        primary.pending_mass_death.len(),
        2,
        "both deferred — neither requeued nor evicted"
    );

    // The entry-deferral path is silent: no PeerRemoved on EITHER
    // receiver. If one ever fires here we'd duplicate-author with
    // the finalize path AND break the recovery contract (a peer
    // that recovers during grace must look as if it never died).
    let from_a = collect_peer_removed(&mut sec_rxs[0]);
    let from_b = collect_peer_removed(&mut sec_rxs[1]);
    assert!(
        from_a.is_empty() && from_b.is_empty(),
        "entry-deferral must not author PeerRemoved; a={from_a:?} b={from_b:?}"
    );

    // Recovery during grace also stays silent: drop the pending
    // entry, no PeerRemoved on either channel.
    primary.record_keepalive("sec-a");
    primary.process_heartbeat_tick().await.unwrap();
    assert!(!primary.pending_mass_death.contains_key("sec-a"));
    let from_a = collect_peer_removed(&mut sec_rxs[0]);
    let from_b = collect_peer_removed(&mut sec_rxs[1]);
    assert!(
        from_a.is_empty() && from_b.is_empty(),
        "grace-window recovery must not author PeerRemoved; \
         a={from_a:?} b={from_b:?}"
    );
}

/// A secondary that's still sending keepalives stays in the routable
/// set even when other secondaries die.
#[tokio::test(flavor = "current_thread")]
async fn live_secondary_is_not_falsely_declared_dead() {
    let (transport, _sec_rx, _kept_alive_for_outgoing_clone) = empty_transport();
    let mut primary: PrimaryCoordinator<_, _, _, _, TestId> = PrimaryCoordinator::new(
        config(Duration::from_millis(50), 2),
        transport,
        dynrunner_transport_quic::NoPeerTransport,
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
    );
    primary.secondaries.insert(
        "dead-sec".into(),
        SecondaryConnectionState::Handshaking(conn),
    );
    primary.seed_keepalive("dead-sec");

    // Bump the keepalive within the deadline window so the heartbeat
    // report should leave it alone.
    tokio::time::sleep(Duration::from_millis(60)).await;
    primary.record_keepalive("dead-sec");
    tokio::time::sleep(Duration::from_millis(60)).await;
    let report = primary.collect_heartbeat_report();
    assert_eq!(report.dead.len(), 0);
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
    let mut primary: PrimaryCoordinator<_, _, _, _, TestId> =
        PrimaryCoordinator::new(
            config(Duration::from_millis(50), 2),
            transport,
            dynrunner_transport_quic::NoPeerTransport,
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
        .receive_welcome(1, vec![], "host".into(), 0, None, false)
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
    // ends up in the dead list (we want the legacy single-death
    // requeue path, not mass-death — mass_death_grace is ZERO via the
    // default `config` builder, but bumping sec-b is the same shape
    // the surviving-peer scenario takes in production).
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
    let batch = crate::worker_signal::drain_worker_signal_batch(
        &mut wm_rx,
        Duration::from_millis(50),
    )
    .await
    .expect("dead-secondary requeue must emit a TasksAdded batch");
    assert!(
        batch
            .signals
            .contains(&crate::worker_signal::WorkerMgmtSignal::TasksAdded),
        "requeue path must emit TasksAdded; got {:?}",
        batch.signals
    );
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
        primary.workers.iter().any(|w| w.secondary_id == "sec-b" && !w.is_idle()),
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
    let mut primary: PrimaryCoordinator<_, _, _, _, TestId> = PrimaryCoordinator::new(
        config(Duration::from_millis(50), 2),
        transport,
        dynrunner_transport_quic::NoPeerTransport,
        ResourceStealingScheduler::memory(),
        FixedEstimator,
    );
    install_default_pool(&mut primary);

    // Register the dead-to-be secondary, operational.
    let conn = SecondaryConnection::new("dead-sec".into())
        .receive_welcome(1, vec![], "host".into(), 0, None, false)
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
        resolved_path: None,
    };
    let victim_hash =
        primary.stage_in_flight_for_test("dead-sec".into(), 0, victim.clone());
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
    let mut promoted: PrimaryCoordinator<_, _, _, _, TestId> = PrimaryCoordinator::new(
        config(Duration::from_millis(50), 2),
        transport2,
        dynrunner_transport_quic::NoPeerTransport,
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
