//! #542 cause-B — the OBSERVER-only persistent-dial-failure removal path.
//!
//! When the QUIC transport's reconnect tick gives up dialing a peer (the
//! `DIAL_SUMMARY_THRESHOLD` boundary on `dynrunner-transport-quic`'s
//! reconnect tracker), it forwards the peer id over the
//! `persistent_dial_failure` channel; the primary's operational loop's
//! arm calls [`PrimaryCoordinator::handle_persistent_dial_failure`].
//!
//! The handler is the SOLE author of
//! `ClusterMutation::PeerRemoved { cause: PersistentDialFailure }`. Three
//! pinned invariants here:
//!  1. An id in `role_table.observers` IS removed (the cause-B fix —
//!     #542's recurring 60s "peer unreachable" WARN ends).
//!  2. A non-observer id (a known secondary; secondaries already have
//!     the heartbeat-miss authoritative removal path through
//!     `requeue_dead_secondary`) is NOT removed by this path — adding a
//!     second source would race that path.
//!  3. An unknown id (stale dial info, never advertised here) is NOT
//!     removed by this path — no role table entry to prune, nothing to
//!     do.
//!
//! REVERT-CHECK: drop the `role_table().observers.contains(...)` gate
//! and invariant (2) fails — the secondary is wrongly removed by this
//! path while the heartbeat-miss path was still the authoritative
//! source.

use super::*;
use dynrunner_protocol_primary_secondary::ClusterMutation;

/// (1) and (3): the OBSERVER case fires + the UNKNOWN-id case no-ops.
/// Pinned as one test so the gate's both directions are exercised back to
/// back over the same coordinator (no setup duplication).
#[tokio::test(flavor = "current_thread")]
async fn handle_persistent_dial_failure_removes_observer_only() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(0);
            let config = test_primary_config();
            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // Seed an observer in the role table by applying the same
            // `PeerJoined { is_observer: true }` mutation the wire path
            // would produce on an observer's `CapabilityAdvertised`. This
            // is what `terminal_verdict.rs` does to seed the gate; we
            // mirror the seam so the test exercises the exact CRDT
            // shape production would carry.
            {
                let cs = primary.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::PeerJoined {
                    peer_id: "obs".to_string(),
                    is_observer: true,
                    can_be_primary: false,
                    cap_version: Default::default(),
                    member_gen: 0,
                });
            }
            assert!(
                primary
                    .cluster_state_for_test()
                    .role_table()
                    .observers
                    .contains("obs"),
                "test setup: observer must be in the role table before the dial-failure fires"
            );

            // Drive the handler with the SAME shape the operational-loop
            // arm would (one peer id off the persistent_dial_failure
            // channel). Observer → removed.
            primary
                .handle_persistent_dial_failure("obs".to_string())
                .await;

            assert!(
                !primary
                    .cluster_state_for_test()
                    .role_table()
                    .observers
                    .contains("obs"),
                "#542 cause-B: a persistent-dial-failure for an observer id MUST prune \
                 role_table.observers (the dial loop has no other way to learn the \
                 observer is gone — observers run no tasks → no heartbeat-miss path)"
            );

            // Unknown id (never advertised here): no-op. Asserts the
            // handler does not panic or pollute state on a stale dial
            // info trigger.
            let observers_snapshot: std::collections::HashSet<String> = primary
                .cluster_state_for_test()
                .role_table()
                .observers
                .iter()
                .cloned()
                .collect();
            primary
                .handle_persistent_dial_failure("never-advertised".to_string())
                .await;
            assert_eq!(
                primary
                    .cluster_state_for_test()
                    .role_table()
                    .observers
                    .iter()
                    .cloned()
                    .collect::<std::collections::HashSet<_>>(),
                observers_snapshot,
                "an unknown id MUST be a no-op (no role-table entry to prune)"
            );
        })
        .await;
}

/// (2): a SECONDARY id is NOT removed by this path. The heartbeat-miss
/// dead-declaration is the authoritative source for secondaries; the
/// dial-failure signal is observer-specific. The gate is `is in
/// role_table.observers`, so a secondary id MUST NOT trip it.
///
/// REVERT-CHECK: drop the observers-contains gate and this test fails —
/// the secondary is wrongly authored a `PeerRemoved` from the
/// dial-failure path while it was still alive on the heartbeat path.
#[tokio::test(flavor = "current_thread")]
async fn handle_persistent_dial_failure_does_not_remove_secondary() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(0);
            let config = test_primary_config();
            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // Seed a normal secondary via PeerJoined { is_observer: false }.
            {
                let cs = primary.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::PeerJoined {
                    peer_id: "sec-0".to_string(),
                    is_observer: false,
                    can_be_primary: true,
                    cap_version: Default::default(),
                    member_gen: 0,
                });
            }
            // Confirm setup: sec-0 is in can_be_primary AND NOT in observers.
            assert!(
                primary
                    .cluster_state_for_test()
                    .role_table()
                    .can_be_primary
                    .contains("sec-0"),
                "test setup: sec-0 must be in can_be_primary"
            );
            assert!(
                !primary
                    .cluster_state_for_test()
                    .role_table()
                    .observers
                    .contains("sec-0"),
                "test setup: sec-0 must NOT be in observers"
            );

            // Dial-failure for the secondary: handler must NO-OP. The
            // heartbeat-miss path (`requeue_dead_secondary`) is the
            // authoritative removal source for secondaries; double-sourcing
            // here would race that path.
            primary
                .handle_persistent_dial_failure("sec-0".to_string())
                .await;

            assert!(
                primary
                    .cluster_state_for_test()
                    .role_table()
                    .can_be_primary
                    .contains("sec-0"),
                "#542 cause-B: a persistent-dial-failure for a SECONDARY id must NOT prune \
                 it — secondaries are removed by the heartbeat-miss path, and a second \
                 source would race it"
            );
        })
        .await;
}

/// END-TO-END: the operational loop's arm consumes the
/// `persistent_dial_failure_rx` and drives the handler all the way to
/// the role-table prune through the LIVE `select!` pipeline.
///
/// Calls `primary.operational_loop()` directly under paused tokio time
/// with a bounded wait (mirroring `lifecycle/tests.rs`'s healthy-fleet
/// pattern). The pool has NO tasks seeded and NO RunComplete, so the
/// loop stays parked in its `select!` indefinitely — every arm has time
/// to fire. A secondary is seeded into the role table (so the
/// fleet-dead arm doesn't fire on a 0-secondary fleet and exit the
/// loop). The test pre-queues a dial-failure signal on the tx; the arm
/// consumes it on its first `select!` poll, the handler authors the
/// `PeerRemoved`, `reproject_roles` drops the observer from
/// `role_table.observers`, and the timeout-bounded wait completes Err
/// (the loop is still parked, never exited).
///
/// This is the contract test owner Q3 asked for: it proves the arm is
/// WIRED. REVERT-CHECK: comment out the new arm body in
/// `lifecycle/operational_loop.rs` and the assertion below fails — the
/// observer stays in the role table because the message stays queued
/// in the rx never drained by the loop.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn persistent_dial_failure_arm_drives_observer_prune_through_oploop() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Keep an inbound sender alive so `recv_peer` parks (the
            // loop genuinely blocks rather than exiting on a closed
            // transport). Mirrors `lifecycle/tests.rs`'s
            // healthy-fleet-does-not-arm-fleet-dead pattern.
            let (transport, secondary_ends) = setup_test(1);
            let _inbound_keepalive = secondary_ends;

            let config = PrimaryConfig {
                num_secondaries: 1,
                connect_timeout: Duration::from_secs(5),
                peer_timeout: Duration::from_secs(5),
                ..test_primary_config()
            };
            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // Wire the dial-failure channel.
            let (dial_tx, dial_rx) = tokio::sync::mpsc::unbounded_channel();
            primary.set_persistent_dial_failure_rx(dial_rx);

            // Seed: recognized primary, an alive worker secondary (so
            // the fleet-dead arm doesn't trip and exit the loop), an
            // observer to prune, and a non-empty pool with
            // `total_tasks > 0` (so the counter exit doesn't fire on
            // `completed_tasks + failed_tasks >= total_tasks` with zero
            // total). Mirrors the priming `lifecycle/tests.rs::
            // healthy_fleet_does_not_arm_fleet_dead` uses.
            {
                let cs = primary.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::PrimaryChanged {
                    new: "setup".to_string(),
                    epoch: 1,
                    reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
                });
                cs.apply(ClusterMutation::PeerJoined {
                    peer_id: "sec-0".to_string(),
                    is_observer: false,
                    can_be_primary: false,
                    cap_version: Default::default(),
                    member_gen: 0,
                });
                cs.apply(ClusterMutation::SecondaryCapacity {
                    secondary: "sec-0".to_string(),
                    worker_count: 4,
                    resources: vec![],
                });
                cs.apply(ClusterMutation::PeerJoined {
                    peer_id: "obs".to_string(),
                    is_observer: true,
                    can_be_primary: false,
                    cap_version: Default::default(),
                    member_gen: 0,
                });
            }
            // Prime the pool with queued tasks so total_tasks > 0 and
            // the counter-based exit can't trip on an empty cohort.
            let binaries = vec![make_binary("bin_0", 50)];
            let phase = dynrunner_core::PhaseId::from("default");
            let mut pool =
                dynrunner_scheduler_api::PendingPool::<TestId>::new([phase], HashMap::new())
                    .expect("default-phase pool");
            pool.extend(binaries.clone()).expect("valid extend");
            primary.pending = Some(pool);
            primary.all_binaries = binaries.clone();
            primary.total_tasks = binaries.len();
            assert!(
                primary
                    .cluster_state_for_test()
                    .role_table()
                    .observers
                    .contains("obs"),
                "test setup: observer must be in the role table before the loop spins"
            );

            // Queue the dial-failure for the observer.
            dial_tx
                .send("obs".to_string())
                .expect("dial_failure_rx is installed and live");

            // Drive the operational loop directly under paused time
            // with a bounded wait. The loop has NO exit cue seeded
            // (no tasks, no RunComplete, secondary alive in
            // role_table); the timeout is EXPECTED to expire. While
            // the loop spins through its select! arms, the queued
            // dial-failure is consumed and the role_table is pruned.
            // We assert ON role_table AFTER the timeout fires (the
            // loop future is dropped, releasing `&mut primary` so the
            // post-timeout inspection compiles).
            let outcome = tokio::time::timeout(
                Duration::from_secs(120),
                primary.operational_loop(),
            )
            .await;
            assert!(
                outcome.is_err(),
                "the operational loop should be parked (no exit cue \
                 seeded); a clean Ok(..) would mean the loop exited \
                 unexpectedly and this test cannot conclude on prune"
            );

            assert!(
                !primary
                    .cluster_state_for_test()
                    .role_table()
                    .observers
                    .contains("obs"),
                "#542 cause-B end-to-end: the operational-loop's \
                 persistent-dial-failure arm must consume the queued \
                 signal + drive the handler + prune role_table.observers \
                 over the live select! pipeline. A revert that removes \
                 the arm body leaves the observer in the role table — \
                 the production recurring 60s 'peer unreachable' WARN \
                 re-fires forever."
            );
        })
        .await;
}
