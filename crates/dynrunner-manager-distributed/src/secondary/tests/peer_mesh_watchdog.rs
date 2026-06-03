//! Peer-mesh-formation watchdog tests: the 30s deadline after a
//! non-empty peer-dial set firing observably (degraded mode, MeshReady
//! with peer_count=0, no fatal exit), plus the healthy-mesh
//! non-regression path.

#![cfg(test)]

use super::super::test_helpers::{
    FakeWorkerFactory, FixedEstimator, TestId, channel_mesh_to_primary,
};
use super::super::*;
use super::processing::{fake_primary, make_binary};
use dynrunner_protocol_primary_secondary::DistributedMessage;
use dynrunner_scheduler::ResourceStealingScheduler;
use std::time::Duration;
use tokio::sync::mpsc as tokio_mpsc;

// Helper: build a no-peer secondary with the watchdog already armed
// past the deadline so the next `check_peer_mesh_watchdog()` call
// fires the degraded path. Returns the secondary plus the primary's
// receive end so callers can drain MeshReady / TaskFailed / etc.
//
// `SecondaryCoordinator` carries six type parameters by design; the
// concrete monomorphisation here is one-off and a `type` alias would
// just push the same shape one layer away.
#[allow(clippy::type_complexity)]
fn arm_watchdog_no_peers(
    secondary_id: &str,
    dial_count: u32,
) -> (
    SecondaryCoordinator<
        dynrunner_transport_channel::ChannelPeerTransport<TestId>,
        dynrunner_transport_channel::ChannelManagerEnd,
        ResourceStealingScheduler,
        FixedEstimator,
        TestId,
    >,
    tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
) {
    use std::time::Instant;
    // Channel-backed mesh with the primary folded in (so `send_to_primary`
    // — the watchdog's `MeshReady` emit — delivers onto `sec_to_pri_rx`,
    // which the direct-method tests drain to observe what the secondary
    // sent) and NO alive secondaries in global state (no `PeerJoined`/
    // `SecondaryCapacity` applied, no peer keepalives), so the role-aware
    // `alive_secondary_count()` reads 0 and arms the degraded path.
    // Inbound is never fed: these tests drive the watchdog/election via
    // direct method calls and never inject primary inbound.
    let (sec_to_pri_tx, sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
    let (_from_primary_tx, from_primary_rx) = tokio_mpsc::unbounded_channel();
    let unified = channel_mesh_to_primary(secondary_id, sec_to_pri_tx, from_primary_rx);
    let config = SecondaryConfig {
        secondary_id: secondary_id.into(),
        num_workers: 1,
        max_resources: dynrunner_core::ResourceMap::from([(
            dynrunner_core::ResourceKind::memory(),
            1024 * 1024 * 1024,
        )]),
        hostname: "test-host".into(),
        keepalive_interval: Duration::from_millis(50),
        src_network: None,
        src_tmp: None,
        peer_timeout: Duration::from_secs(120),
        keepalive_miss_threshold: 2,
        retry_max_passes: 1,
        oom_retry_max_passes: 1,
        primary_link_failure_threshold: 5,
        primary_link_failure_window: Duration::from_secs(30),
        unconfigured_deadline: Duration::from_secs(600),
        is_observer: false,
        resource_check_interval: Duration::from_millis(100),
        log_oom_watcher: false,
        promoted_primary_quiesce_grace: Duration::from_millis(100),
        unfulfillable_reinject_max_per_task: None,
        mem_manager_reserved_bytes: None,
        output_dir: None,
        memuse_log_path: None,
    };
    let mut secondary: SecondaryCoordinator<
        dynrunner_transport_channel::ChannelPeerTransport<TestId>,
        dynrunner_transport_channel::ChannelManagerEnd,
        ResourceStealingScheduler,
        FixedEstimator,
        TestId,
    > = SecondaryCoordinator::new(
        config,
        unified,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    secondary.set_bootstrap_primary_id("primary".to_string());
    secondary.mesh.peer_dial_count = dial_count;
    secondary.mesh.peer_mesh_check_at = Some(Instant::now() - Duration::from_secs(1));
    (secondary, sec_to_pri_rx)
}

/// T-B1-graceful: 30s after a non-empty peer dial with zero peers
/// connected, the watchdog enters DEGRADED mode rather than fatal.
/// Asserts the new contract:
///   1. `fatal_exit` is NOT set,
///   2. `peer_mesh_degraded` is true,
///   3. `MeshReady` is sent with `peer_count=0` so the primary's
///      `wait_for_mesh_ready` releases `PromotePrimary`,
///   4. NO `SecondaryFatalError` lands on the primary channel,
///   5. `peer_mesh_check_at` is cleared so the watchdog never
///      re-fires.
///
/// Pre-fix the watchdog declared `SecondaryFatalError` + set
/// `fatal_exit`, killing the secondary process — operationally
/// fatal because primary→secondary task dispatch over WSS was
/// healthy; the QUIC peer mesh is only required for failover and
/// inter-secondary keepalive. Stranded 474 of 484 tasks in
/// asm-tokenizer's `--jobs 2` regression.
#[tokio::test(flavor = "current_thread")]
async fn peer_mesh_watchdog_enters_degraded_mode_when_no_peers() {
    let _ = tracing_subscriber::fmt::try_init();
    let (mut secondary, mut sec_to_pri_rx) = arm_watchdog_no_peers("sec-x", 4);

    // Pre-fault: nothing on the wire, no exit flag, not degraded.
    assert!(secondary.fatal_exit.is_none());
    assert!(!secondary.is_mesh_degraded());
    assert!(sec_to_pri_rx.try_recv().is_err());

    secondary.check_peer_mesh_watchdog().await;

    // Post-fault: degraded latched true, watchdog disarmed, NO
    // fatal_exit (the run continues over WSS).
    assert!(
        secondary.is_mesh_degraded(),
        "peer_mesh_degraded must latch true after deadline-elapsed-zero-peers"
    );
    assert!(
        secondary.fatal_exit.is_none(),
        "watchdog must NOT set fatal_exit in graceful-degrade mode"
    );
    assert!(
        secondary.mesh.peer_mesh_check_at.is_none(),
        "watchdog must disarm after firing"
    );

    // MeshReady(peer_count=0) was sent; SecondaryFatalError was NOT.
    let mut saw_mesh_ready = false;
    while let Ok(msg) = sec_to_pri_rx.try_recv() {
        match msg {
            DistributedMessage::MeshReady {
                secondary_id,
                peer_count,
                ..
            } => {
                assert_eq!(secondary_id, "sec-x");
                assert_eq!(
                    peer_count, 0,
                    "degraded path reports zero peers so primary releases PromotePrimary"
                );
                saw_mesh_ready = true;
            }
            DistributedMessage::SecondaryFatalError { .. } => {
                panic!("watchdog must NOT send SecondaryFatalError in graceful-degrade mode");
            }
            other => panic!(
                "unexpected message on primary channel: {:?}",
                other.msg_type()
            ),
        }
    }
    assert!(
        saw_mesh_ready,
        "MeshReady (peer_count=0) must be sent so primary releases PromotePrimary"
    );

    // Re-firing the watchdog is a no-op (single-shot contract).
    secondary.check_peer_mesh_watchdog().await;
    assert!(
        sec_to_pri_rx.try_recv().is_err(),
        "watchdog must not re-fire after deadline elapses"
    );
}

/// Watchdog-silent-after-RunComplete: in an in-process distributed
/// run the secondaries observe `ClusterMutation::RunComplete` from
/// the primary's broadcast right before teardown, ~30s before their
/// own peer-mesh deadline would elapse on the next keepalive tick.
/// Pre-fix the watchdog still fired during clean shutdown, emitting
/// a misleading "peer mesh did not form" warn and latching
/// `peer_mesh_degraded`. Post-fix the watchdog short-circuits on
/// `cluster_state.run_complete()`, disarming itself silently.
///
/// The single-source-of-truth read lives inside the watchdog
/// (`peer.rs::check_peer_mesh_watchdog`) rather than at each
/// `cluster_state.apply(RunComplete)` site, so the dispatch /
/// processing call sites don't need to know about peer-mesh policy.
#[tokio::test(flavor = "current_thread")]
async fn watchdog_silent_after_run_complete() {
    use dynrunner_protocol_primary_secondary::ClusterMutation;
    let _ = tracing_subscriber::fmt::try_init();

    let (mut secondary, mut sec_to_pri_rx) = arm_watchdog_no_peers("sec-rc", 4);

    // Pre-condition: degraded latch off, no fatal exit, deadline
    // armed past the elapsed point so the watchdog WOULD fire
    // without the run-complete short-circuit.
    assert!(!secondary.is_mesh_degraded());
    assert!(secondary.fatal_exit.is_none());
    assert!(secondary.mesh.peer_mesh_check_at.is_some());

    // Simulate the primary's run-complete broadcast landing on the
    // local cluster mirror — same code path as the production
    // `dispatch.rs::apply_cluster_mutations` arm.
    secondary
        .cluster_state
        .apply(ClusterMutation::<TestId>::RunComplete);

    secondary.check_peer_mesh_watchdog().await;

    // Post-fire: degraded NOT latched, watchdog disarmed silently,
    // no `MeshReady` and no `SecondaryFatalError` on the wire.
    assert!(
        !secondary.is_mesh_degraded(),
        "run-complete short-circuit must NOT enter degraded mode"
    );
    assert!(secondary.fatal_exit.is_none());
    assert!(
        secondary.mesh.peer_mesh_check_at.is_none(),
        "run-complete short-circuit must disarm the watchdog"
    );
    assert!(
        sec_to_pri_rx.try_recv().is_err(),
        "watchdog must NOT emit messages after run-complete"
    );

    // Re-tick is also a no-op.
    secondary.check_peer_mesh_watchdog().await;
    assert!(sec_to_pri_rx.try_recv().is_err());
}

/// Counterpart to `watchdog_silent_after_run_complete`: with the
/// same setup but WITHOUT the `RunComplete` mutation, the watchdog
/// still fires the #15 graceful-degrade path. Pins that the
/// run-complete short-circuit doesn't leak past its precondition
/// (i.e. `cluster_state.run_complete()` flipping is genuinely
/// required to suppress the fault).
#[tokio::test(flavor = "current_thread")]
async fn watchdog_still_fires_pre_run_complete() {
    let _ = tracing_subscriber::fmt::try_init();

    let (mut secondary, mut sec_to_pri_rx) = arm_watchdog_no_peers("sec-pre-rc", 4);

    // Sanity: cluster_state has not seen RunComplete.
    assert!(
        !secondary.cluster_state.run_complete(),
        "pre-condition: run is not yet complete"
    );

    secondary.check_peer_mesh_watchdog().await;

    // #15 contract is preserved: degraded latched, watchdog
    // disarmed, MeshReady(peer_count=0) emitted to the primary.
    assert!(
        secondary.is_mesh_degraded(),
        "pre-RunComplete watchdog must still enter degraded mode"
    );
    assert!(secondary.mesh.peer_mesh_check_at.is_none());
    assert!(secondary.fatal_exit.is_none());

    let mut saw_mesh_ready = false;
    while let Ok(msg) = sec_to_pri_rx.try_recv() {
        if let DistributedMessage::MeshReady { peer_count, .. } = msg {
            assert_eq!(peer_count, 0);
            saw_mesh_ready = true;
        }
    }
    assert!(
        saw_mesh_ready,
        "pre-RunComplete watchdog must still send MeshReady(0)"
    );
}

/// T-B1-graceful continued: with `peer_mesh_degraded` already
/// latched, an operational `TaskAssignment` arriving over the
/// (WSS-equivalent) primary_transport must still dispatch
/// successfully. Validates the load-bearing claim that peer-mesh
/// failure does NOT block primary→secondary task flow.
///
/// The "watchdog flips degraded mid-run" path is covered by the
/// previous test; here the goal is the dispatch contract, so we
/// pre-set the flag and assert the run completes without
/// regressions. Pre-setting also makes the test deterministic
/// regardless of how fast the FakeWorker churns through 3 tasks
/// vs the 50ms keepalive tick.
#[tokio::test(flavor = "current_thread")]
async fn degraded_secondary_continues_dispatching_over_wss() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (sec_to_pri_tx, sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
            let (pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();
            // Channel-backed mesh: the `fake_primary` is folded in as a
            // mesh peer (no per-role uplink). Inbound is the
            // primary→secondary channel; outbound the secondary→primary.
            let unified = channel_mesh_to_primary("sec-deg", sec_to_pri_tx, pri_to_sec_rx);
            let config = SecondaryConfig {
                secondary_id: "sec-deg".into(),
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
                unconfigured_deadline: Duration::from_secs(600),
                is_observer: false,
                resource_check_interval: Duration::from_millis(100),
                log_oom_watcher: false,
                promoted_primary_quiesce_grace: Duration::from_millis(100),
                unfulfillable_reinject_max_per_task: None,
                mem_manager_reserved_bytes: None,
                output_dir: None,
                memuse_log_path: None,
            };
            let binaries = vec![
                make_binary("a", 50),
                make_binary("b", 60),
                make_binary("c", 70),
            ];
            let secondary_id = config.secondary_id.clone();
            let primary_handle = tokio::task::spawn_local(fake_primary(
                binaries,
                secondary_id.clone(),
                sec_to_pri_rx,
                pri_to_sec_tx,
            ));
            let mut secondary = SecondaryCoordinator::new(
                config,
                unified,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            secondary.set_bootstrap_primary_id("primary".to_string());
            // Pre-latch degraded mode so the run starts in the
            // post-watchdog-fire state. The watchdog's actual fire
            // path is covered by `peer_mesh_watchdog_enters_degraded_mode_when_no_peers`.
            secondary.mesh.degraded = true;
            secondary.mesh.peer_dial_count = 2;

            let mut factory = FakeWorkerFactory;
            secondary
                .run(&mut factory)
                .await
                .expect("degraded run must complete cleanly over WSS");

            assert_eq!(
                secondary.local_tasks_run_for_test(),
                3,
                "WSS dispatch must keep flowing after peer-mesh degraded mode"
            );
            assert!(
                secondary.is_mesh_degraded(),
                "degraded latch must persist for the duration of the run"
            );
            primary_handle.await.unwrap();
        })
        .await;
}

/// T-B1-degraded-failover-fails-loud: a degraded secondary
/// reaching the failover trigger (primary silent) must set
/// `fatal_exit` with a clear reason instead of self-promoting on
/// quorum=1. The election protocol requires peer responses to
/// reach a meaningful quorum; degraded mode means there's nobody
/// to vote with.
#[tokio::test(flavor = "current_thread")]
async fn degraded_failover_fails_loud_instead_of_self_promoting() {
    use super::super::election::ElectionState;
    use super::super::test_helpers::{election_config, make_secondary};
    let _ = tracing_subscriber::fmt::try_init();

    let mut sec = make_secondary(election_config("sec-a"));
    sec.enter_operational_for_test();
    // Latch degraded mode (skip running the watchdog — the prior
    // test covers that path; this one only exercises the consumer).
    sec.mesh.degraded = true;
    sec.mesh.peer_dial_count = 4;
    // Mark the primary as silent past the death deadline. With
    // peer_keepalives empty (no mesh), `run_election_tick` would
    // otherwise enter Suspecting and then self-promote on
    // quorum=1.
    sec.record_primary_message();
    tokio::time::sleep(Duration::from_millis(110)).await;

    let actions = sec.run_election_tick();

    let reason = sec
        .fatal_exit
        .as_ref()
        .expect("degraded + primary-silent must set fatal_exit");
    assert!(
        reason.contains("peer mesh required for failover"),
        "fatal reason should explain the degraded-failover bail, got: {reason}"
    );
    assert!(
        matches!(sec.op_mut().election, ElectionState::Normal),
        "degraded failover bail must NOT transition the election state \
         (no Suspecting, no Candidate, no Promoted)"
    );
    assert!(
        actions.broadcast.is_empty(),
        "degraded failover bail must NOT broadcast TimeoutQuery"
    );
}

/// T-B1-quorum-survives: sanity check that the watchdog's
/// healthy-mesh path is unaffected by the degrade refactor. With
/// three alive peer-secondaries in GLOBAL STATE (their `Secondary`
/// keepalives recorded — the operational signal the watchdog reads)
/// before the deadline, the watchdog clears `peer_mesh_check_at`,
/// sends `MeshReady(peer_count=3)`, and leaves `peer_mesh_degraded`
/// false. The role-aware count is over global state, NOT the
/// transport's role-blind `peer_count()`.
#[tokio::test(flavor = "current_thread")]
async fn watchdog_healthy_mesh_path_unaffected_by_degrade_refactor() {
    use super::super::wire::timestamp_now;
    use std::time::Instant;
    let _ = tracing_subscriber::fmt::try_init();

    let (sec_to_pri_tx, mut sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
    let (_pri_to_sec_tx, pri_to_sec_rx) =
        tokio_mpsc::unbounded_channel::<DistributedMessage<TestId>>();
    // Channel-backed mesh with ONLY the primary folded in (so the
    // watchdog's `MeshReady` emit lands on `sec_to_pri_rx`). The three
    // healthy peer-secondaries are set up as GLOBAL STATE below (recorded
    // `Secondary` keepalives), since the watchdog's role-aware
    // `alive_secondary_count()` reads global state, never the transport.
    let unified = channel_mesh_to_primary("sec-quo", sec_to_pri_tx, pri_to_sec_rx);
    let config = SecondaryConfig {
        secondary_id: "sec-quo".into(),
        num_workers: 1,
        max_resources: dynrunner_core::ResourceMap::from([(
            dynrunner_core::ResourceKind::memory(),
            1024 * 1024 * 1024,
        )]),
        hostname: "test-host".into(),
        keepalive_interval: Duration::from_millis(50),
        src_network: None,
        src_tmp: None,
        peer_timeout: Duration::from_secs(120),
        keepalive_miss_threshold: 2,
        retry_max_passes: 1,
        oom_retry_max_passes: 1,
        primary_link_failure_threshold: 5,
        primary_link_failure_window: Duration::from_secs(30),
        unconfigured_deadline: Duration::from_secs(600),
        is_observer: false,
        resource_check_interval: Duration::from_millis(100),
        log_oom_watcher: false,
        promoted_primary_quiesce_grace: Duration::from_millis(100),
        unfulfillable_reinject_max_per_task: None,
        mem_manager_reserved_bytes: None,
        output_dir: None,
        memuse_log_path: None,
    };
    let mut secondary: SecondaryCoordinator<
        dynrunner_transport_channel::ChannelPeerTransport<TestId>,
        dynrunner_transport_channel::ChannelManagerEnd,
        ResourceStealingScheduler,
        FixedEstimator,
        TestId,
    > = SecondaryCoordinator::new(
        config,
        unified,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    secondary.set_bootstrap_primary_id("primary".to_string());
    // Drive the coordinator Operational (the regime in which the watchdog
    // ticks in production, where `peer_keepalives` is the alive-secondary
    // signal) and record 3 alive peer-secondaries — a fully-formed healthy
    // mesh in GLOBAL STATE. Deadline still in the future; the
    // typed-lifecycle watchdog's pre-deadline early-clear fires when EVERY
    // expected secondary is alive (`alive_secondary_count() ==
    // peer_dial_count`). With 3 == 3 the full-formed branch disarms early.
    secondary.enter_operational_for_test();
    for i in 0..3u32 {
        secondary
            .op_mut()
            .peer_keepalives
            .insert(format!("peer-{i}"), timestamp_now());
    }
    secondary.mesh.peer_dial_count = 3;
    secondary.mesh.peer_mesh_check_at = Some(Instant::now() + Duration::from_secs(30));

    secondary.check_peer_mesh_watchdog().await;

    assert!(
        !secondary.is_mesh_degraded(),
        "healthy mesh path must NOT touch peer_mesh_degraded"
    );
    assert!(secondary.fatal_exit.is_none());
    assert!(
        secondary.mesh.peer_mesh_check_at.is_none(),
        "watchdog disarms once the mesh is observed healthy"
    );

    let mut saw_mesh_ready = false;
    while let Ok(msg) = sec_to_pri_rx.try_recv() {
        match msg {
            DistributedMessage::MeshReady {
                secondary_id,
                peer_count,
                ..
            } => {
                assert_eq!(secondary_id, "sec-quo");
                assert_eq!(peer_count, 3, "healthy mesh reports the live peer count");
                saw_mesh_ready = true;
            }
            other => panic!(
                "unexpected message on primary channel: {:?}",
                other.msg_type()
            ),
        }
    }
    assert!(saw_mesh_ready, "MeshReady must be sent on the healthy path");
}
