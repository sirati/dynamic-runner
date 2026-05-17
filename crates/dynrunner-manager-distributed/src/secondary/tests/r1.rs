//! R1 promotion-threshold tests + cold-start no-primary tests +
//! the post-promotion peer-message dispatch test. All share the
//! `r1_helpers::make_with_peers` builder that wires a
//! `FixedPeerCount(N)` peer-transport stub so the processing-loop
//! sees a healthy mesh.

#![cfg(test)]

use super::processing::make_binary;
use super::super::test_helpers::{FakeWorkerFactory, FixedEstimator, NoPeers, TestId};
use super::super::*;
use std::time::Duration;
use dynrunner_protocol_primary_secondary::DistributedBinaryInfo;
use dynrunner_scheduler::ResourceStealingScheduler;
use dynrunner_transport_channel::ChannelPrimaryTransportEnd;
use tokio::sync::mpsc as tokio_mpsc;

// (helpers in `r1_helpers` keep the test bodies focused on the
// state-machine assertions rather than wiring boilerplate)

mod r1_helpers {
    //! Shared setup for R1 tests. Uses `FixedPeerCount(N)` so the
    //! processing-loop's peer-count check observes a healthy mesh
    //! (which is what makes promotion via election possible). The
    //! `make_secondary` helper in `test_helpers.rs` uses `NoPeers`,
    //! which reports peer_count=0 — fine for election-state tests
    //! that don't go through the operational threshold path, but
    //! wrong for R1 tests that do.

    use crate::secondary::test_helpers::{election_config, FixedEstimator, FixedPeerCount, TestId};
    use super::*;
    use dynrunner_scheduler::ResourceStealingScheduler;

    pub(super) type R1Secondary = SecondaryCoordinator<
        ChannelPrimaryTransportEnd<TestId>,
        FixedPeerCount,
        dynrunner_transport_channel::ChannelManagerEnd,
        ResourceStealingScheduler,
        FixedEstimator,
        TestId,
    >;

    /// Construct a SecondaryCoordinator with `FixedPeerCount(peers)`
    /// for the peer transport so the processing-loop helper's
    /// peer-count check observes the configured mesh size.
    pub(super) fn make_with_peers(secondary_id: &str, peers: usize) -> R1Secondary {
        let (sec_to_pri_tx, _sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
        let (_pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();
        let transport = ChannelPrimaryTransportEnd {
            tx: sec_to_pri_tx,
            rx: pri_to_sec_rx,
        };
        SecondaryCoordinator::new(
            election_config(secondary_id),
            transport,
            FixedPeerCount(peers),
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        )
    }

    /// Inline `wire::timestamp_now()` (path is `pub(super)` to wire,
    /// not visible from this test module).
    pub(super) fn timestamp_now() -> f64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0)
    }
}
//
//   1. A SUSTAINED primary-link outage (count or time threshold
//      breached) arms `primary_disconnected = true` and backdates
//      `primary_last_seen` so the next election tick promotes.
//   2. A TRANSIENT outage (one probe, brief flap) does NOT arm
//      failover — `record_primary_message` resets the health
//      sub-state cleanly when the primary message arrives.
//   3. The #15 degraded-mesh guard still holds: a primary-link
//      threshold breach with `peer_mesh_degraded = true` results in
//      a fatal exit, NOT a unilateral self-promotion.
//   4. Promotion preserves the peer mesh: no transport-close events
//      on inter-peer connections during the promotion window.
//
// The tests use the `make_secondary` helper (channel transports +
// NoPeers / FixedPeerCount stubs) and drive the threshold via
// direct `check_primary_link_threshold` / `run_election_tick`
// calls. The full `process_tasks` loop is not exercised here —
// existing integration tests already cover the loop's structural
// behaviour, and these tests would be flaky against the loop's
// internal `tokio::select!` ordering.

/// T-R1-promotion-on-disconnect (count axis): a non-promoted
/// secondary with a healthy peer mesh observes the primary-link
/// threshold breach via N consecutive recv-None probes, arms
/// `primary_disconnected`, and the next election tick enters
/// Suspecting (the count-axis half of the threshold). Pinning
/// the count path here keeps the test deterministic — no
/// wall-clock reliance.
#[tokio::test(flavor = "current_thread")]
async fn r1_promotion_on_disconnect_count_axis() {
    use super::super::election::ElectionState;
    let _ = tracing_subscriber::fmt::try_init();

    // Healthy peer mesh: 2 peers visible at the transport layer
    // so the threshold path takes the elect-via-mesh branch
    // (not the no-peer break-out).
    let mut sec = r1_helpers::make_with_peers("sec-a", 2);
    sec.peer_keepalives
        .insert("sec-b".into(), r1_helpers::timestamp_now());
    sec.peer_keepalives
        .insert("sec-c".into(), r1_helpers::timestamp_now());
    sec.record_primary_message();

    // Drive the count-axis by feeding 3 probes (test_helpers sets
    // failure_threshold=3). Each probe records a recv-None event;
    // the third returns true and arms the link.
    assert!(!sec.primary_link.record_recv_failure());
    assert!(!sec.primary_link.record_recv_failure());
    assert!(
        sec.primary_link.record_recv_failure(),
        "third probe must arm the link (threshold=3 in election_config)"
    );
    assert!(sec.primary_link.should_arm_failover());

    // The processing-loop helper translates "should_arm" into the
    // operational arming flags. Pre-arming, primary_disconnected
    // should still be false (the count probes were direct
    // record_recv_failure calls — they don't touch the operational
    // flag; that's the processing-loop's job).
    assert!(!sec.primary_disconnected);
    sec.check_primary_link_threshold();
    assert!(
        sec.primary_disconnected,
        "tick re-check must propagate the threshold breach to the operational flag"
    );

    // Election tick now sees the primary as silent (backdated
    // past the keepalive miss threshold) and enters Suspecting.
    // With healthy peers, the degraded-mesh guard does NOT fire.
    let actions = sec.run_election_tick();
    assert!(
        matches!(sec.election, ElectionState::Suspecting { .. }),
        "election must enter Suspecting on threshold-armed failover; \
         got {:?}",
        std::mem::discriminant(&sec.election)
    );
    assert!(
        actions
            .broadcast
            .iter()
            .any(|m| matches!(m, DistributedMessage::TimeoutQuery { .. })),
        "Suspecting transition must broadcast TimeoutQuery"
    );
    assert!(
        sec.fatal_exit.is_none(),
        "healthy mesh must not fatal-exit"
    );
}

/// T-R1-recover-on-primary-back: a transient flap (one probe, then
/// a primary message arrives via `record_primary_message`) resets
/// the health sub-state cleanly. No election fires. The test
/// drives the API contract directly — the message-arrival path
/// itself runs through `dispatch_message` in production but that
/// path is already covered by `primary_recovery_clears_routing_target`
/// elsewhere in the file.
#[tokio::test(flavor = "current_thread")]
async fn r1_recover_on_primary_back() {
    use super::super::election::ElectionState;
    let _ = tracing_subscriber::fmt::try_init();

    let mut sec = r1_helpers::make_with_peers("sec-a", 1);
    sec.peer_keepalives
        .insert("sec-b".into(), r1_helpers::timestamp_now());
    sec.record_primary_message();

    // One probe, then primary comes back — short flap.
    sec.primary_link.record_recv_failure();
    assert!(sec.primary_link.is_link_failing());

    sec.record_primary_message();
    assert!(
        !sec.primary_link.is_link_failing(),
        "primary-back must reset the health sub-state"
    );
    assert!(!sec.primary_link.should_arm_failover());

    // Tick re-check is a no-op now that the link is healthy.
    sec.check_primary_link_threshold();
    assert!(!sec.primary_disconnected, "no arming on healthy link");

    // Election stays in Normal — no Suspecting.
    let actions = sec.run_election_tick();
    assert!(matches!(sec.election, ElectionState::Normal));
    assert!(actions.broadcast.is_empty());
}

/// T-R1-respects-degraded-guard: when the peer mesh is degraded
/// (#15 contract), a primary-link threshold breach must NOT
/// self-promote. The election tick fatal-exits with the
/// degraded-failover reason. Pre-fix the degraded-mesh guard
/// could have been bypassed if the threshold path armed via a
/// different code path; this test pins that the threshold and the
/// guard compose correctly.
#[tokio::test(flavor = "current_thread")]
async fn r1_respects_degraded_guard() {
    use super::super::election::ElectionState;
    let _ = tracing_subscriber::fmt::try_init();

    // Degraded mode is the no-peers case; FixedPeerCount(0) so the
    // processing-loop helper's peer_count check matches reality.
    // The watchdog has already latched the degraded flag (#15
    // contract: peer mesh failed to form). Threshold arming must
    // still flow through `check_primary_link_threshold`, then the
    // election tick should fatal-exit.
    let mut sec = r1_helpers::make_with_peers("sec-a", 0);
    sec.peer_mesh_degraded = true;
    sec.peer_dial_count = 4;
    sec.record_primary_message();

    // Drive count-axis past threshold.
    sec.primary_link.record_recv_failure();
    sec.primary_link.record_recv_failure();
    sec.primary_link.record_recv_failure();
    assert!(sec.primary_link.should_arm_failover());

    // Tick re-check observes peer_count == 0 and takes the
    // no-peer-mesh exit (sets primary_disconnected without
    // backdating). The election tick then needs to fire on the
    // primary_silent axis. We need to backdate primary_last_seen
    // to past the deadline manually for the election tick to
    // observe the silence — in production the keepalive-tick
    // pathway would have backdated already if the primary was
    // actually silent, but in this state-machine-isolated test
    // we set it explicitly. This mirrors how
    // `degraded_failover_fails_loud_instead_of_self_promoting`
    // sets up its precondition.
    sec.check_primary_link_threshold();
    assert!(sec.primary_disconnected);

    // Drive the elapsed-time precondition for run_election_tick by
    // pre-aging the primary_last_seen by past the deadline.
    sec.primary_last_seen = Some(
        std::time::Instant::now()
            .checked_sub(std::time::Duration::from_millis(150))
            .unwrap_or_else(std::time::Instant::now),
    );

    // Election tick observes degraded mesh + primary-silent and
    // sets fatal_exit per the #15 contract.
    let _actions = sec.run_election_tick();
    let reason = sec
        .fatal_exit
        .as_ref()
        .expect("degraded + threshold-armed must set fatal_exit");
    assert!(
        reason.contains("peer mesh required for failover"),
        "fatal reason should explain the degraded-failover bail, got: {reason}"
    );
    assert!(
        matches!(sec.election, ElectionState::Normal),
        "degraded failover bail must NOT transition the election state"
    );
}

/// T-R1-no-mesh-rebuild: the threshold path is purely state-machine
/// internal and does not touch the peer transport in any way. This
/// test pins that contract: drive the threshold, observe arming,
/// and assert the peer-mesh view (`peer_keepalives`) and routing
/// target (`primary_link.current_primary`) are unchanged across
/// the arming window.
///
/// The architectural invariant is that the threshold path produces
/// ZERO peer-transport side effects during arming — only the
/// election-tick path emits `TimeoutQuery` (which is a NORMAL
/// message, not a mesh rebuild).
#[tokio::test(flavor = "current_thread")]
async fn r1_no_mesh_rebuild_during_arming() {
    let _ = tracing_subscriber::fmt::try_init();

    let mut sec = r1_helpers::make_with_peers("sec-a", 2);
    sec.peer_keepalives
        .insert("sec-b".into(), r1_helpers::timestamp_now());
    sec.peer_keepalives
        .insert("sec-c".into(), r1_helpers::timestamp_now());
    sec.record_primary_message();

    // Snapshot the peer-mesh view before arming so we can assert
    // it's preserved across the threshold path.
    let peers_before: std::collections::HashSet<String> =
        sec.peer_keepalives.keys().cloned().collect();
    assert_eq!(peers_before.len(), 2);

    // Drive count-axis past threshold and arm.
    sec.primary_link.record_recv_failure();
    sec.primary_link.record_recv_failure();
    sec.primary_link.record_recv_failure();
    sec.check_primary_link_threshold();
    assert!(sec.primary_disconnected);

    // Peer-mesh view unchanged.
    let peers_after: std::collections::HashSet<String> =
        sec.peer_keepalives.keys().cloned().collect();
    assert_eq!(peers_before, peers_after, "arming must not mutate peer keepalives");

    // The primary_link's current_primary routing target stays
    // unchanged at None (primary not yet promoted to a peer) —
    // arming alone doesn't pick a candidate; that's the election's
    // job.
    assert!(
        sec.primary_link.current_primary().is_none(),
        "arming alone must not set a routing target"
    );
}

/// T-cold-start (#25 asm-dataset-nix T7 attempt 2):
/// A late-arriving secondary boots AFTER the run has logically
/// completed; the primary URL is unreachable and no peer has dialled
/// in. Pre-fix, the secondary hung in `wait_for_setup`'s blocking
/// recv for ~6min (transport retries) before SLURM container
/// teardown reaped it. Post-fix, the orchestration-level
/// `setup_deadline` cancels the setup future and the secondary
/// exits cold with a clear error.
///
/// Test shape: drop the primary tx end immediately and use
/// `NoPeers` for the peer transport (`peer_count() == 0`). Set a
/// tight deadline (200ms) so the test finishes in milliseconds
/// rather than the production 60s. Verify `run()` returns Err and
/// that the error message identifies the cold-start cause so
/// operators can distinguish it from mid-run failure modes.
///
/// Why this lives at the orchestration level: `wait_for_setup`'s
/// own doc-comment explicitly forbids a `tokio::select!` race
/// against `recv()` (cancellation hazard around partially-decoded
/// messages). The deadline wraps the entire setup phase from
/// outside, so a cancellation simply abandons the partial state
/// — no subsequent iteration touches it.
#[tokio::test(flavor = "current_thread")]
async fn cold_start_exits_when_primary_unreachable_and_no_peers() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (sec_to_pri_tx, _sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
            // KEEP `_pri_to_sec_tx` bound (the `_` prefix is just an
            // unused-name lint suppressor — Rust drops bindings at
            // end of scope, not immediately). This makes
            // `primary_transport.recv()` BLOCK forever rather than
            // returning None — simulating the asm-dataset-nix T7
            // scenario where the primary URL is unreachable and the
            // transport's internal retries never give up. Returning
            // None hits `wait_for_setup`'s existing `primary
            // disconnected during setup` arm in milliseconds, well
            // before setup_deadline fires; we want to exercise the
            // deadline path.
            let (_pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel::<DistributedMessage<TestId>>();
            let transport = ChannelPrimaryTransportEnd {
                tx: sec_to_pri_tx,
                rx: pri_to_sec_rx,
            };

            let config = SecondaryConfig {
                secondary_id: "sec-cold".into(),
                num_workers: 1,
                max_resources: dynrunner_core::ResourceMap::from([(dynrunner_core::ResourceKind::memory(), 1024 * 1024 * 1024)]),
                hostname: "test-host".into(),
                keepalive_interval: Duration::from_millis(50),
                src_network: None,
                src_tmp: None,
                peer_timeout: Duration::from_secs(120),
                keepalive_miss_threshold: 3,
                retry_max_passes: 1,
                primary_link_failure_threshold: 5,
                primary_link_failure_window: Duration::from_secs(30),
                // Tight deadline so the test reaps in ~200ms.
                setup_deadline: Duration::from_millis(200),
                is_observer: false,
                resource_check_interval: Duration::from_millis(100),
                log_oom_watcher: false,
                promoted_primary_quiesce_grace: Duration::from_millis(100),
                unfulfillable_reinject_max_per_task: None,
            };

            let mut secondary = SecondaryCoordinator::new(
                config,
                transport,
                NoPeers,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let mut factory = FakeWorkerFactory;
            let start = std::time::Instant::now();
            let result = secondary.run(&mut factory).await;
            let elapsed = start.elapsed();

            // Should be Err — the primary is unreachable AND no peers.
            assert!(
                result.is_err(),
                "expected cold-start failure, got Ok: {result:?}"
            );

            // Error should identify the cold-start case so operators
            // can distinguish it from mid-run failures. The exact
            // wording is "no primary, no peers" per the doc-comment.
            let err = result.unwrap_err();
            assert!(
                err.contains("no primary") && err.contains("no peers"),
                "expected cold-start identifier in error, got: {err}"
            );

            // Should reap promptly — at most setup_deadline + slack
            // (worker init, log emission, future cancellation cost).
            // 2s is generous; the actual elapsed is typically <250ms.
            assert!(
                elapsed < Duration::from_secs(2),
                "cold-start reap took too long: {elapsed:?} (expected < 2s)"
            );
        })
        .await;
}

/// T-cold-start-with-peers (#25 negative branch):
/// When the primary URL is unreachable BUT peers HAVE dialled in,
/// the secondary still exits on setup_deadline — but with a
/// different error class than the no-peers branch. This is the
/// "primary unresponsive but mesh formed" scenario, which is
/// distinct from "everyone is gone" and should be operator-
/// distinguishable. Pinning the branch divergence to prevent
/// future code from silently merging them.
#[tokio::test(flavor = "current_thread")]
async fn cold_start_with_peers_emits_distinct_error() {
    use crate::secondary::test_helpers::FixedPeerCount;

    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (sec_to_pri_tx, _sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
            // Same blocking-recv trick as the no-peers test above —
            // keep the sender bound so the secondary blocks waiting
            // for the primary that never speaks.
            let (_pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel::<DistributedMessage<TestId>>();
            let transport = ChannelPrimaryTransportEnd {
                tx: sec_to_pri_tx,
                rx: pri_to_sec_rx,
            };

            let config = SecondaryConfig {
                secondary_id: "sec-cold-with-peers".into(),
                num_workers: 1,
                max_resources: dynrunner_core::ResourceMap::from([(dynrunner_core::ResourceKind::memory(), 1024 * 1024 * 1024)]),
                hostname: "test-host".into(),
                keepalive_interval: Duration::from_millis(50),
                src_network: None,
                src_tmp: None,
                peer_timeout: Duration::from_secs(120),
                keepalive_miss_threshold: 3,
                retry_max_passes: 1,
                primary_link_failure_threshold: 5,
                primary_link_failure_window: Duration::from_secs(30),
                setup_deadline: Duration::from_millis(200),
                is_observer: false,
                resource_check_interval: Duration::from_millis(100),
                log_oom_watcher: false,
                promoted_primary_quiesce_grace: Duration::from_millis(100),
                unfulfillable_reinject_max_per_task: None,
            };

            // FixedPeerCount(2) reports peer_count() == 2 without
            // actually wiring messages; that's enough for the
            // `peer_count() == 0` check to fail and route to the
            // "peers reachable" branch.
            let mut secondary = SecondaryCoordinator::new(
                config,
                transport,
                FixedPeerCount(2),
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let mut factory = FakeWorkerFactory;
            let result = secondary.run(&mut factory).await;
            assert!(result.is_err(), "expected setup-deadline failure");

            let err = result.unwrap_err();
            // Distinct from the no-peers branch: error mentions
            // peers reachable, NOT "no primary, no peers".
            assert!(
                err.contains("peer") && !err.contains("no peers"),
                "expected peers-reachable identifier, got: {err}"
            );
        })
        .await;
}

/// T-#28 (post-promotion task distribution):
/// When a peer-routed TaskAssignment arrives at `handle_peer_message`,
/// it MUST be dispatched to a worker — not silently dropped via the
/// `_` catch-all. Pre-fix, `handle_peer_message` had no
/// `TaskAssignment` arm; the promoted peer-primary's assignments to
/// other secondaries fell through to `tracing::debug!("unhandled peer
/// message")` and never reached `pool.workers[i].assign_task`.
/// Symptom (asm-tokenizer 9ca9124): the promoted node's own workers
/// ran 445/446 tasks each while peer secondaries' workers stopped at
/// 1 task each (their pre-promotion initial assignment), parking
/// half the cluster's compute.
///
/// This test drives `handle_peer_message` directly with a fabricated
/// TaskAssignment and asserts that `active_tasks` contains the
/// expected hash, proving the worker received the assignment.
#[tokio::test(flavor = "current_thread")]
async fn handle_peer_message_dispatches_task_assignment_to_worker() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (sec_to_pri_tx, _sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
            let (_pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel::<DistributedMessage<TestId>>();
            let transport = ChannelPrimaryTransportEnd {
                tx: sec_to_pri_tx,
                rx: pri_to_sec_rx,
            };

            let config = SecondaryConfig {
                secondary_id: "sec-1".into(),
                num_workers: 1,
                max_resources: dynrunner_core::ResourceMap::from([(dynrunner_core::ResourceKind::memory(), 1024 * 1024 * 1024)]),
                hostname: "test-host".into(),
                keepalive_interval: Duration::from_secs(60),
                src_network: None,
                src_tmp: None,
                peer_timeout: Duration::from_secs(120),
                keepalive_miss_threshold: 3,
                retry_max_passes: 1,
                primary_link_failure_threshold: 5,
                primary_link_failure_window: Duration::from_secs(30),
                setup_deadline: Duration::from_secs(60),
                is_observer: false,
                resource_check_interval: Duration::from_millis(100),
                log_oom_watcher: false,
                promoted_primary_quiesce_grace: Duration::from_millis(100),
                unfulfillable_reinject_max_per_task: None,
            };

            let mut secondary = SecondaryCoordinator::new(
                config,
                transport,
                NoPeers,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // Initialise workers so `assign_task` has a target.
            let mut factory = FakeWorkerFactory;
            secondary.initialize_workers(&mut factory).await.unwrap();

            // Fabricate the wire shape the promoted-peer-primary would
            // send. file_hash is the key we'll later assert against in
            // `active_tasks` to prove the dispatch actually happened.
            let binary = make_binary("post-promotion-task", 50);
            let file_hash = format!("hash_{}", binary.identifier.0);
            let assignment = DistributedMessage::TaskAssignment {
                sender_id: "sec-0".into(),       // promoted peer-primary
                timestamp: 0.0,
                secondary_id: "sec-1".into(),
                worker_id: 0,
                zip_file: None,
                binary_info: DistributedBinaryInfo::from_task_info(&binary),
                local_path: binary.path.to_string_lossy().into_owned(),
                file_hash: file_hash.clone(),
            };

            // The critical call: route via peer_transport handler.
            // Pre-fix this fell into the catch-all and was lost.
            secondary.handle_peer_message(assignment, &mut None).await;

            // Worker received the assignment → `active_tasks` records it.
            // (The `dispatch_message` body inserts on the assign_task
            // success path; the FakeWorkerFactory's runner always
            // accepts assignments.)
            assert!(
                secondary.active_tasks.contains_key(&file_hash),
                "TaskAssignment via peer_transport must reach the worker; \
                 active_tasks={:?}",
                secondary.active_tasks
            );
        })
        .await;
}
