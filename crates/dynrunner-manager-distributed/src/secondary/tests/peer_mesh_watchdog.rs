//! Peer-mesh-formation watchdog tests: the 30s deadline after a
//! non-empty peer-dial set firing observably (degraded mode, MeshReady
//! with peer_count=0, no fatal exit), plus the healthy-mesh
//! non-regression path.

#![cfg(test)]

use super::super::test_helpers::{
    FakeWorkerFactory, SecondaryHarness, TestId, channel_mesh_to_primary, make_secondary_channel,
    run_secondary_node,
};
use super::super::*;
use super::processing::{fake_primary, make_binary};
use dynrunner_protocol_primary_secondary::DistributedMessage;
use dynrunner_transport_channel::ChannelPeerTransport;
use std::time::Duration;
use tokio::sync::mpsc as tokio_mpsc;

// Helper: build a no-peer secondary with the watchdog already armed
// past the deadline so the next `check_peer_mesh_watchdog()` call
// fires the degraded path. Returns the harness plus the primary's
// receive end so callers can drain MeshReady / TaskFailed / etc. (after
// `drain_egress`, since `MeshReady` is a queued `MeshClient::send`).
fn arm_watchdog_no_peers(
    secondary_id: &str,
    dial_count: u32,
) -> (
    SecondaryHarness<ChannelPeerTransport<TestId>>,
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
        primary_silence_backstop: Duration::from_secs(120),
        unconfigured_deadline: Duration::from_secs(600),
        can_be_primary: false,
        resource_check_interval: Duration::from_millis(100),
        log_oom_watcher: false,
        phase_status_log_intervals: vec![Duration::from_secs(60)],
        promoted_primary_quiesce_grace: Duration::from_millis(100),
        unfulfillable_reinject_max_per_task: None,
        mem_manager_reserved_bytes: None,
        output_dir: None,
        memuse_log_path: None,
        forwarded_argv: Vec::new(),
    };
    let mut secondary = make_secondary_channel(config, unified);
    secondary.set_bootstrap_primary_id("setup".to_string());
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
///      `wait_for_mesh_ready` releases the `PrimaryChanged` announcement,
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
    // Flush the queued MeshReady/etc. egress onto the folded primary
    // channel so `sec_to_pri_rx` observes it (MeshClient::send is queued).
    secondary.drain_egress().await;

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
                    "degraded path reports zero peers so primary releases its PrimaryChanged announcement"
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
        "MeshReady (peer_count=0) must be sent so primary releases its PrimaryChanged announcement"
    );

    // Re-firing the watchdog is a no-op (single-shot contract).
    secondary.check_peer_mesh_watchdog().await;
    // Flush the queued MeshReady/etc. egress onto the folded primary
    // channel so `sec_to_pri_rx` observes it (MeshClient::send is queued).
    secondary.drain_egress().await;
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
        .apply(ClusterMutation::<TestId>::RunComplete { counts: Default::default() });

    secondary.check_peer_mesh_watchdog().await;
    // Flush the queued MeshReady/etc. egress onto the folded primary
    // channel so `sec_to_pri_rx` observes it (MeshClient::send is queued).
    secondary.drain_egress().await;

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
    // Flush the queued MeshReady/etc. egress onto the folded primary
    // channel so `sec_to_pri_rx` observes it (MeshClient::send is queued).
    secondary.drain_egress().await;
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
    // Flush the queued MeshReady/etc. egress onto the folded primary
    // channel so `sec_to_pri_rx` observes it (MeshClient::send is queued).
    secondary.drain_egress().await;

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
        if let DistributedMessage::MeshReady {
            target: _,
            peer_count,
            ..
        } = msg
        {
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
///
/// Driven over the PRODUCTION concurrent mesh-pump (`run_secondary_node`):
/// the degraded secondary's full request/assign handshake against
/// `fake_primary` runs to a clean `RunComplete` exit.
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
                primary_silence_backstop: Duration::from_secs(120),
                unconfigured_deadline: Duration::from_secs(600),
                can_be_primary: false,
                resource_check_interval: Duration::from_millis(100),
                log_oom_watcher: false,
                phase_status_log_intervals: vec![Duration::from_secs(60)],
                promoted_primary_quiesce_grace: Duration::from_millis(100),
                unfulfillable_reinject_max_per_task: None,
                mem_manager_reserved_bytes: None,
                output_dir: None,
                memuse_log_path: None,
                forwarded_argv: Vec::new(),
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
            let mut secondary = make_secondary_channel(config, unified);
            secondary.set_bootstrap_primary_id("setup".to_string());
            // Pre-latch degraded mode so the run starts in the
            // post-watchdog-fire state. The watchdog's actual fire
            // path is covered by `peer_mesh_watchdog_enters_degraded_mode_when_no_peers`.
            secondary.mesh.degraded = true;
            secondary.mesh.peer_dial_count = 2;

            let mut factory = FakeWorkerFactory;
            let (secondary, result) = run_secondary_node(secondary, &mut factory).await;
            result.expect("degraded run must complete cleanly over WSS");

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

/// The run_20260611_200548 replay (#432, REPRO-FIRST): under severe
/// startup load EVERY initial mesh dial fails, the formation watchdog
/// elapses (`MeshReady` peer_count=0, trigger=watchdog-elapsed,
/// degraded latched), and the transport's reconnect ticker keeps
/// re-dialing in the background. The peers then BECOME reachable — a
/// peer's `Secondary` keepalive lands (the same `peer_keepalives`
/// write the message_handler recognition arm performs once the
/// re-dialed leg carries frames). Formation must be a CONTINUOUS
/// concern: the next supervision tick revises the degraded verdict,
/// and when real primary silence trips afterwards the secondary has
/// its election path back (Suspecting + TimeoutQuery), instead of the
/// production fatal "peer mesh required for failover but not
/// available" that killed 6 of 11 secondaries.
///
/// Pre-fix this is RED at step 3: `mesh.degraded` was a PERMANENT
/// latch — set once at watchdog-elapse, re-read forever by
/// `run_election_tick`'s degraded-bail — so a mesh that healed at the
/// transport level (legs established, keepalives flowing) still
/// fatal-exited at the first primary silence. No membership event, no
/// promotion, no re-`PeerInfo` is involved in the recovery.
#[tokio::test(flavor = "current_thread")]
async fn mesh_formed_after_watchdog_elapse_restores_failover_path() {
    use super::super::election::ElectionState;
    use dynrunner_protocol_primary_secondary::{ClusterMutation, PrimaryChangeReason};
    let _ = tracing_subscriber::fmt::try_init();

    let (mut secondary, mut sec_to_pri_rx) = arm_watchdog_no_peers("sec-late", 10);
    // Election-grade timing: the patient leg-(B) backstop trips
    // sub-second so the post-recovery silence step stays in test
    // budget. Read live by `run_election_tick` on every tick.
    secondary.config.primary_silence_backstop = Duration::from_millis(100);
    secondary.enter_operational_for_test();
    // A real primary identity (the production run had one): the
    // Suspecting entry's `TimeoutQuery` names the silent primary, so
    // `current_primary()` must resolve. "setup" is the id the harness
    // folds the primary channel under, so `Destination::Primary`
    // egress (the MeshReady report) keeps landing on `sec_to_pri_rx`.
    secondary.cluster_state.apply(ClusterMutation::PrimaryChanged {
        new: "setup".into(),
        epoch: 1,
        reason: PrimaryChangeReason::Election,
    });

    // 1. The verdict: deadline elapsed with zero alive secondaries →
    //    degraded latched + MeshReady(peer_count=0) (the
    //    watchdog-elapsed report the production log shows).
    secondary.check_peer_mesh_watchdog().await;
    secondary.drain_egress().await;
    assert!(
        secondary.is_mesh_degraded(),
        "pre-condition: watchdog-elapse with zero alive secondaries latches degraded"
    );
    let mut saw_mesh_ready_zero = false;
    while let Ok(msg) = sec_to_pri_rx.try_recv() {
        if let DistributedMessage::MeshReady { peer_count, .. } = msg {
            assert_eq!(peer_count, 0, "watchdog-elapsed report carries zero peers");
            saw_mesh_ready_zero = true;
        }
    }
    assert!(
        saw_mesh_ready_zero,
        "pre-condition: MeshReady(0) is sent so the run proceeds degraded"
    );

    // 2. Peers BECOME reachable: the transport's reconnect ticker
    //    (formation_retry.rs pins that it never stops re-dialing
    //    never-formed legs) establishes a leg and the peer's Secondary
    //    keepalive lands.
    secondary
        .op_mut()
        .peer_keepalives
        .insert("peer-1".into(), std::time::Instant::now());

    // 3. The next keepalive tick re-runs the formation supervision:
    //    the degraded verdict must be revised — formation is a
    //    continuous concern, not a one-shot give-up.
    secondary.check_peer_mesh_watchdog().await;
    assert!(
        !secondary.is_mesh_degraded(),
        "a mesh that formed AFTER the watchdog elapsed must clear the \
         degraded latch (formation supervision is continuous, not \
         abandoned at the verdict — the #432 shape)"
    );

    // The settled-report latch is untouched by the recovery: MeshReady
    // was already sent for this primary identity, no duplicate.
    secondary.drain_egress().await;
    assert!(
        sec_to_pri_rx.try_recv().is_err(),
        "recovery must not re-send MeshReady (one settled report per \
         primary identity)"
    );

    // 4. REAL primary silence now trips (the 18:21:16 moment): with a
    //    live peer the election must PROCEED — Suspecting entered,
    //    TimeoutQuery broadcast — not fatal-exit.
    secondary.record_primary_message();
    tokio::time::sleep(Duration::from_millis(110)).await;
    let actions = secondary.run_election_tick();
    assert!(
        secondary.fatal_exit.is_none(),
        "primary silence with a since-formed mesh must NOT fatal-exit \
         (the fatal is only correct while the mesh has never formed); \
         got: {:?}",
        secondary.fatal_exit
    );
    assert!(
        matches!(
            secondary.op_mut().election,
            ElectionState::Suspecting { .. }
        ),
        "the election path must be available again (Suspecting)"
    );
    assert!(
        !actions.broadcast.is_empty(),
        "Suspecting entry broadcasts the TimeoutQuery over the healed mesh"
    );
}

/// Degraded-supervision observability: while the mesh stays empty
/// after the watchdog verdict, the supervision tick emits a
/// rate-limited WARN (once per throttle interval, never per keepalive
/// tick), and the late-formation recovery is narrated at INFO once the
/// first alive secondary appears — after which the supervision goes
/// silent. Also pins that supervision never re-sends MeshReady.
#[tokio::test(start_paused = true)]
async fn degraded_supervision_warns_throttled_while_mesh_stays_empty() {
    use tracing_subscriber::layer::SubscriberExt;
    let capture = crate::test_capture::TargetCapture::for_target(
        "dynrunner_manager_distributed::secondary::peer::mesh_watchdog",
    );
    let subscriber = tracing_subscriber::Registry::default().with(capture.clone());
    let _guard = tracing::subscriber::set_default(subscriber);

    let (mut secondary, mut sec_to_pri_rx) = arm_watchdog_no_peers("sec-degsup", 10);
    secondary.enter_operational_for_test();

    // The verdict: degraded latched, MeshReady(0) sent.
    secondary.check_peer_mesh_watchdog().await;
    secondary.drain_egress().await;
    assert!(secondary.is_mesh_degraded());
    while sec_to_pri_rx.try_recv().is_ok() {}

    let degraded_warns = |capture: &crate::test_capture::TargetCapture| {
        capture
            .events()
            .iter()
            .filter(|e| {
                e.level == tracing::Level::WARN && e.event.message.contains("peer mesh still empty")
            })
            .count()
    };

    // Supervision ticks inside the throttle window: exactly ONE WARN
    // (the first occurrence emits; the rest are suppressed) — loud but
    // never per-tick.
    for _ in 0..5 {
        secondary.check_peer_mesh_watchdog().await;
    }
    assert_eq!(
        degraded_warns(&capture),
        1,
        "degraded supervision must warn exactly once inside the throttle window"
    );

    // Past the throttle interval the WARN recurs.
    tokio::time::advance(Duration::from_secs(61)).await;
    secondary.check_peer_mesh_watchdog().await;
    assert_eq!(
        degraded_warns(&capture),
        2,
        "degraded supervision must keep warning once per throttle interval"
    );

    // Supervision never re-sends MeshReady while degraded.
    secondary.drain_egress().await;
    assert!(
        sec_to_pri_rx.try_recv().is_err(),
        "degraded supervision must not emit MeshReady"
    );

    // Recovery: the first alive secondary clears the latch with an
    // INFO naming the transition...
    secondary
        .op_mut()
        .peer_keepalives
        .insert("peer-1".into(), std::time::Instant::now());
    secondary.check_peer_mesh_watchdog().await;
    assert!(!secondary.is_mesh_degraded());
    assert!(
        capture.events().iter().any(|e| {
            e.level == tracing::Level::INFO && e.event.message.contains("degraded latch cleared")
        }),
        "late formation must be narrated at INFO; captured: {:#?}",
        capture.events()
    );

    // ...and the supervision goes silent afterwards.
    tokio::time::advance(Duration::from_secs(61)).await;
    secondary.check_peer_mesh_watchdog().await;
    assert_eq!(
        degraded_warns(&capture),
        2,
        "a recovered mesh must not keep warning"
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
        primary_silence_backstop: Duration::from_secs(120),
        unconfigured_deadline: Duration::from_secs(600),
        can_be_primary: false,
        resource_check_interval: Duration::from_millis(100),
        log_oom_watcher: false,
        phase_status_log_intervals: vec![Duration::from_secs(60)],
        promoted_primary_quiesce_grace: Duration::from_millis(100),
        unfulfillable_reinject_max_per_task: None,
        mem_manager_reserved_bytes: None,
        output_dir: None,
        memuse_log_path: None,
        forwarded_argv: Vec::new(),
    };
    let mut secondary = make_secondary_channel(config, unified);
    secondary.set_bootstrap_primary_id("setup".to_string());
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
            .insert(format!("peer-{i}"), Instant::now());
    }
    secondary.mesh.peer_dial_count = 3;
    secondary.mesh.peer_mesh_check_at = Some(Instant::now() + Duration::from_secs(30));

    secondary.check_peer_mesh_watchdog().await;
    // Flush the queued MeshReady/etc. egress onto the folded primary
    // channel so `sec_to_pri_rx` observes it (MeshClient::send is queued).
    secondary.drain_egress().await;

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
