//! Tests for the primary coordinator. Fixtures live in
//! `super::test_helpers`; this file holds the test scenarios.

use super::test_helpers::{
    fake_secondary, fake_secondary_with_addrs, make_binary, make_relative_binary, setup_test,
    FakeWorkerFactory, FixedEstimator, NoPeers, TestId,
};
use super::*;
use dynrunner_protocol_primary_secondary::DistributedMessage;
use dynrunner_scheduler::ResourceStealingScheduler;
use dynrunner_transport_channel::{
    ChannelPrimaryTransportEnd, ChannelSecondaryTransportEnd,
};
use crate::secondary::{SecondaryConfig, SecondaryCoordinator};
use std::collections::HashMap;
use tokio::sync::mpsc as tokio_mpsc;

/// Phase 4b: tests that don't care about phase lifecycle pass an empty
/// dep map and no-op closures. Centralised here so individual tests
/// stay focused on the wire-flow they actually exercise.
fn noop_phase_args() -> (
    HashMap<dynrunner_core::PhaseId, Vec<dynrunner_core::PhaseId>>,
    OnPhaseStart,
    OnPhaseEnd,
) {
    (HashMap::new(), Box::new(|_| {}), Box::new(|_, _, _| {}))
}


#[tokio::test(flavor = "current_thread")]
async fn single_secondary_processes_all_tasks() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let (transport, secondary_ends) = setup_test(1);

        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: 1,
            connect_timeout: Duration::from_secs(5),
            peer_timeout: Duration::from_secs(5),
                    keepalive_interval: Duration::from_secs(5),
                    keepalive_miss_threshold: 3,
                    source_pre_staged_root: None,
                    uses_file_based_items: true,
            max_concurrent_per_type: std::collections::HashMap::new(),
            retry_max_passes: 1,
            fleet_dead_timeout: std::time::Duration::from_secs(30),
            mesh_ready_timeout: std::time::Duration::from_secs(5),
            mass_death_grace: std::time::Duration::ZERO,
            mass_death_min_count: 2,
            source_dir: None,
        };

        let mut primary = PrimaryCoordinator::new(
            config,
            transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        let binaries = vec![
            make_binary("a", 50),
            make_binary("b", 60),
            make_binary("c", 70),
        ];

        for (id, rx, tx) in secondary_ends {
            tokio::task::spawn_local(fake_secondary(
                id,
                2,
                1024 * 1024 * 1024,
                rx,
                tx,
            ));
        }

        { let (deps, ops, ope) = noop_phase_args(); primary.run(binaries, deps, ops, ope).await.unwrap() };

        assert_eq!(primary.completed_count(), 3);
        assert_eq!(primary.failed_count(), 0);
    }).await;
}

#[tokio::test(flavor = "current_thread")]
async fn two_secondaries_distribute_work() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let (transport, secondary_ends) = setup_test(2);

        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: 2,
            connect_timeout: Duration::from_secs(5),
            peer_timeout: Duration::from_secs(5),
                    keepalive_interval: Duration::from_secs(5),
                    keepalive_miss_threshold: 3,
                    source_pre_staged_root: None,
                    uses_file_based_items: true,
            max_concurrent_per_type: std::collections::HashMap::new(),
            retry_max_passes: 1,
            fleet_dead_timeout: std::time::Duration::from_secs(30),
            mesh_ready_timeout: std::time::Duration::from_secs(5),
            mass_death_grace: std::time::Duration::ZERO,
            mass_death_min_count: 2,
            source_dir: None,
        };

        let mut primary = PrimaryCoordinator::new(
            config,
            transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        let binaries: Vec<TaskInfo<TestId>> = (0..6)
            .map(|i| make_binary(&format!("bin_{i}"), 100))
            .collect();

        for (id, rx, tx) in secondary_ends {
            tokio::task::spawn_local(fake_secondary(
                id,
                2,
                1024 * 1024 * 1024,
                rx,
                tx,
            ));
        }

        { let (deps, ops, ope) = noop_phase_args(); primary.run(binaries, deps, ops, ope).await.unwrap() };

        assert_eq!(primary.completed_count(), 6);
        assert_eq!(primary.failed_count(), 0);
    }).await;
}

/// Regression: when there are more secondaries than initial-assignable
/// items, the secondaries that DON'T get any work must still receive an
/// InitialAssignment message (with empty zip_files / workers_ready /
/// staged_files). Otherwise their `wait_for_setup` waits forever for
/// the third gating message and the run stalls until heartbeat declares
/// them dead. Caught in the field on a 4-secondary run with a single
/// phase-3 item — three secondaries hung in setup, primary killed them
/// 15s later, work proceeded only on the lucky 4th.
///
/// Setup: 2 real secondaries with workers, 1 binary. Pre-fix only
/// secondary 0 receives InitialAssignment; secondary 1 hangs in
/// `wait_for_setup`. Post-fix both reach `process_tasks` and the
/// run completes.
#[tokio::test(flavor = "current_thread")]
async fn empty_batch_secondary_still_reaches_process_tasks() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let max_res = dynrunner_core::ResourceMap::from(
            [(dynrunner_core::ResourceKind::memory(), 2 * 1024 * 1024 * 1024u64)]
        );
        let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
        let mut outgoing = HashMap::new();
        let mut sec_handles = Vec::new();

        for i in 0..2u32 {
            let secondary_id = format!("sec-{i}");
            let (pri_to_sec_tx, sec_to_pri_rx, handle) =
                spawn_real_secondary(secondary_id.clone(), 2, max_res.clone());
            outgoing.insert(secondary_id, pri_to_sec_tx);
            sec_handles.push(handle);

            let tx = incoming_tx.clone();
            tokio::task::spawn_local(async move {
                let mut rx = sec_to_pri_rx;
                while let Some(msg) = rx.recv().await {
                    if tx.send(msg).is_err() {
                        break;
                    }
                }
            });
        }
        drop(incoming_tx);

        let transport = ChannelSecondaryTransportEnd { outgoing, incoming_rx };
        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: 2,
            connect_timeout: Duration::from_secs(10),
            peer_timeout: Duration::from_secs(10),
            keepalive_interval: Duration::from_secs(5),
            keepalive_miss_threshold: 3,
            source_pre_staged_root: None,
            uses_file_based_items: true,
            max_concurrent_per_type: std::collections::HashMap::new(),
            retry_max_passes: 1,
            fleet_dead_timeout: std::time::Duration::from_secs(30),
            mesh_ready_timeout: std::time::Duration::from_secs(5),
            mass_death_grace: std::time::Duration::ZERO,
            mass_death_min_count: 2,
            source_dir: None,
        };

        let mut primary = PrimaryCoordinator::new(
            config,
            transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        // ONE binary for TWO secondaries — initial assignment will
        // dispatch it to whichever secondary's worker the scheduler
        // picks first; the other gets `assigned=0` and must still
        // receive a (possibly empty) InitialAssignment to escape
        // wait_for_setup.
        let binaries = vec![make_binary("only", 50)];

        // The pre-fix bug doesn't prevent primary.run() from
        // returning — secondary 0 completes the binary, pool drains,
        // primary exits. But secondary 1 is wedged in
        // wait_for_setup and never reaches process_tasks, so its
        // `completed_count()` would never observe the cluster-wide
        // forward (the value stays at 0 instead of 1). That
        // discrepancy is the test signal.
        let (deps, ops, ope) = noop_phase_args();
        primary.run(binaries, deps, ops, ope).await.unwrap();

        let completed = primary.completed_count();
        let failed = primary.failed_count();
        drop(primary);

        let mut per_sec_completed = Vec::new();
        for handle in sec_handles {
            per_sec_completed.push(handle.await.unwrap());
        }

        assert_eq!(completed, 1);
        assert_eq!(failed, 0);
        // Both secondaries must have reached process_tasks; the
        // cluster-wide TaskComplete forward registers in each
        // secondary's `completed_tasks` set. Pre-fix the
        // empty-batch secondary is stuck in wait_for_setup and its
        // count stays at 0.
        for (i, count) in per_sec_completed.iter().enumerate() {
            assert!(
                *count >= 1,
                "secondary {i} should have observed the cluster's 1 \
                 completion (entered process_tasks); saw {count}"
            );
        }
    }).await;
}


/// Regression: post-demotion the local primary's `run_retry_passes`
/// is a no-op (the primary owns retry). This test pins the
/// primary side equivalent: a Recoverable failure observed by
/// the primary's own worker should land in
/// `primary_failed`, the synchronous drain-check should
/// re-inject into `primary_pending` once the in-flight ledger empties,
/// and the next dispatch cycle should rerun the task. A task that
/// succeeds on the retry leaves `primary_failed` empty.
///
/// Why the assertions probe the primary (not the local primary):
/// the local primary's `operational_loop` exits the moment its
/// counter check satisfies `completed + failed >= total`, which fires
/// on the FIRST failure observed via the wire forward — well before
/// the primary's keepalive-tick / synchronous drain-check
/// delivers the retry success. Once the local primary returns from
/// `run()`, its `completed_tasks` / `failed_tasks` snapshots are
/// frozen and don't reflect any later retry outcome. The
/// primary's `completed_tasks` is the post-demotion source of
/// truth for cluster-wide completion accounting; the local primary's
/// counters are a forwarding cache that's deliberately stale at this
/// point.
///
/// Setup: 1 binary "ok" (50 bytes) + 1 binary "flaky" (40 bytes), 1
/// real secondary, 1 worker. The pool sorts size-DESC so "ok" is
/// initial-assigned first; "flaky" stays queued and falls into the
/// primary's `primary_pending` post-promotion. The
/// `FlakyWorkerFactory` is parameterised to fail "flaky" exactly once
/// on its first attempt (Recoverable), and to succeed every other
/// task / subsequent attempt. After the first failure the
/// primary re-injects "flaky" into its own pool and the worker
/// picks it up again via the steady-state `request_task_for_worker`
/// path; the second attempt succeeds. End state on the primary
/// side: 2 completions, 0 residual failures, 1 retry pass consumed.
#[tokio::test(flavor = "current_thread")]
async fn recoverable_failure_succeeds_on_retry_pass() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let max_res = dynrunner_core::ResourceMap::from(
            [(dynrunner_core::ResourceKind::memory(), 1024 * 1024 * 1024u64)]
        );
        // Quota=1 for "flaky" (relative_path = "/tmp/flaky"): fail
        // attempt 1 with Recoverable, succeed from attempt 2 onwards.
        // "ok" is unlisted → quota=0 → succeeds on attempt 1.
        let mut quotas = HashMap::new();
        quotas.insert("/tmp/flaky".to_string(), 1u32);
        let flaky = super::test_helpers::FlakyWorkerFactory::with_quotas(quotas);

        let (pri_to_sec_tx, sec_to_pri_rx, sec_handle) =
            spawn_real_secondary_flaky(
                "sec-0".into(),
                /* num_workers = */ 1,
                max_res,
                flaky,
                /* retry_max_passes = */ 1,
            );

        // Wire the channel pair into the primary's transport.
        let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
        let mut outgoing = HashMap::new();
        outgoing.insert("sec-0".to_string(), pri_to_sec_tx);
        tokio::task::spawn_local(async move {
            let mut rx = sec_to_pri_rx;
            while let Some(msg) = rx.recv().await {
                if incoming_tx.send(msg).is_err() {
                    break;
                }
            }
        });
        let transport = ChannelSecondaryTransportEnd { outgoing, incoming_rx };

        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: 1,
            connect_timeout: Duration::from_secs(10),
            peer_timeout: Duration::from_secs(10),
            keepalive_interval: Duration::from_millis(50),
            keepalive_miss_threshold: 3,
            source_pre_staged_root: None,
            uses_file_based_items: true,
            max_concurrent_per_type: std::collections::HashMap::new(),
            retry_max_passes: 1,
            fleet_dead_timeout: std::time::Duration::from_secs(30),
            mesh_ready_timeout: std::time::Duration::from_secs(5),
            mass_death_grace: std::time::Duration::ZERO,
            mass_death_min_count: 2,
            source_dir: None,
        };

        let mut primary = PrimaryCoordinator::new(
            config,
            transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        // Two binaries: "ok" (50 bytes, sorts first under size-DESC
        // → initial-assigned to worker 0) and "flaky" (40 bytes,
        // stays queued → on promotion the new primary's
        // `populate_primary_from_cluster_state` rebuilds
        // `primary_pending` from the replicated cluster ledger).
        let binaries = vec![
            make_binary("ok", 50),
            make_binary("flaky", 40),
        ];

        let (deps, ops, ope) = noop_phase_args();
        primary.run(binaries, deps, ops, ope).await.unwrap();

        // Drop primary to close the secondary's primary_transport;
        // the primary's `process_tasks` exits on transport
        // close + zero peers (single-secondary case). By the time
        // `primary.run()` returns the primary has already
        // observed the retry-success TaskComplete on its own
        // worker-event channel and incremented its
        // `completed_tasks` count to 2 — the local primary's exit
        // happens AFTER the primary's bookkeeping is final.
        drop(primary);

        let (completed, failed_residual, passes_used) =
            sec_handle.await.unwrap();

        // Both binaries reached terminal success on the
        // primary's view: "ok" succeeded first attempt, "flaky"
        // succeeded on retry. No residual permanent failures.
        // Exactly one retry pass was consumed.
        assert_eq!(completed, 2, "primary should report 2 completions");
        assert_eq!(
            failed_residual, 0,
            "primary's failed ledger should be empty after retry success"
        );
        assert_eq!(
            passes_used, 1,
            "exactly one retry pass should have been consumed"
        );
    }).await;
}

/// Companion to `recoverable_failure_succeeds_on_retry_pass`: a task
/// that fails Recoverably on EVERY attempt (main pass + every retry
/// pass) ends up permanently in `primary_failed`, and the
/// retry budget reaches `config.retry_max_passes`. Pins the
/// budget-exhaustion side of the primary retry pass — without
/// this guard the drain-check could re-inject in an unbounded loop.
///
/// Setup: same shape as the success test (1 binary "ok" + 1 binary
/// "doomed", 1 worker, 1 secondary). `FlakyWorkerFactory` is told to
/// fail "doomed" `u32::MAX` times — i.e. always — so both the main
/// dispatch and the single retry attempt return Recoverable. End
/// state on the primary side: 1 completion ("ok"), 1
/// permanent failure ("doomed"), 1 retry pass consumed (=
/// `retry_max_passes`).
#[tokio::test(flavor = "current_thread")]
async fn recoverable_failure_exhausts_retry_budget_and_becomes_permanent() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let max_res = dynrunner_core::ResourceMap::from(
            [(dynrunner_core::ResourceKind::memory(), 1024 * 1024 * 1024u64)]
        );
        // Quota = u32::MAX so "doomed" never succeeds across any
        // number of attempts. With `retry_max_passes = 1`, the
        // primary tries: main pass (fail #1) → retry pass
        // (fail #2) → budget exhausted, "doomed" stays in
        // `primary_failed`.
        let mut quotas = HashMap::new();
        quotas.insert("/tmp/doomed".to_string(), u32::MAX);
        let flaky = super::test_helpers::FlakyWorkerFactory::with_quotas(quotas);

        let (pri_to_sec_tx, sec_to_pri_rx, sec_handle) =
            spawn_real_secondary_flaky(
                "sec-0".into(),
                1,
                max_res,
                flaky,
                /* retry_max_passes = */ 1,
            );

        let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
        let mut outgoing = HashMap::new();
        outgoing.insert("sec-0".to_string(), pri_to_sec_tx);
        tokio::task::spawn_local(async move {
            let mut rx = sec_to_pri_rx;
            while let Some(msg) = rx.recv().await {
                if incoming_tx.send(msg).is_err() {
                    break;
                }
            }
        });
        let transport = ChannelSecondaryTransportEnd { outgoing, incoming_rx };

        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: 1,
            connect_timeout: Duration::from_secs(10),
            peer_timeout: Duration::from_secs(10),
            keepalive_interval: Duration::from_millis(50),
            keepalive_miss_threshold: 3,
            source_pre_staged_root: None,
            uses_file_based_items: true,
            max_concurrent_per_type: std::collections::HashMap::new(),
            retry_max_passes: 1,
            fleet_dead_timeout: std::time::Duration::from_secs(30),
            mesh_ready_timeout: std::time::Duration::from_secs(5),
            mass_death_grace: std::time::Duration::ZERO,
            mass_death_min_count: 2,
            source_dir: None,
        };

        let mut primary = PrimaryCoordinator::new(
            config,
            transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        // "ok" sorts first (size 50 > 40) → initial-assigned →
        // succeeds. "doomed" stays in pool → reaches primary's
        // `primary_pending` post-promotion → dispatched via
        // `handle_primary_task_request` → fails Recoverably → drain-
        // check re-injects → fails again → budget exhausted →
        // permanent.
        let binaries = vec![
            make_binary("ok", 50),
            make_binary("doomed", 40),
        ];

        let (deps, ops, ope) = noop_phase_args();
        primary.run(binaries, deps, ops, ope).await.unwrap();

        // Drop primary so the primary's transport closes and
        // its `process_tasks` exits. By that point the
        // primary has fully consumed its retry budget on
        // "doomed".
        drop(primary);

        let (succeeded, failed_residual, passes_used) =
            sec_handle.await.unwrap();

        // `secondary.completed_count()` is the size of the
        // `completed_tasks` set, which after the latest fix only
        // tracks tasks that reached non-Recoverable termination
        // (success or terminal failure). Recoverable failures —
        // whether retried-to-success, retried-to-Recoverable-again,
        // or budget-exhausted-still-Recoverable — stay out of the
        // set so the primary's dispatch retain doesn't filter
        // them out from a future re-injection. Here "ok" succeeded
        // and is in the set; "doomed" was Recoverable on every
        // attempt and isn't.
        assert_eq!(
            succeeded, 1,
            "only the unconditionally-succeeding binary should land in completed_tasks"
        );
        // The retry-specific bookkeeping is the assertion that
        // matters for this regression: "doomed" still sits in the
        // permanent-failure ledger after the budget was consumed.
        assert_eq!(
            failed_residual, 1,
            "exhausted retry budget should leave 1 entry in primary_failed"
        );
        assert_eq!(
            passes_used, 1,
            "retry budget should be fully consumed"
        );
    }).await;
}

/// Companion: a task that fails BOTH the main pass and the retry
/// pass stays permanently in `failed_tasks` — `retry_max_passes=1`
/// means one retry, no third chance.
#[tokio::test(flavor = "current_thread")]
async fn recoverable_failure_twice_becomes_permanent() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let (transport, mut secondary_ends) = setup_test(1);

        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: 1,
            connect_timeout: Duration::from_secs(5),
            peer_timeout: Duration::from_secs(5),
            keepalive_interval: Duration::from_secs(5),
            keepalive_miss_threshold: 3,
            source_pre_staged_root: None,
            uses_file_based_items: true,
            max_concurrent_per_type: std::collections::HashMap::new(),
            retry_max_passes: 1,
            fleet_dead_timeout: std::time::Duration::from_secs(30),
            mesh_ready_timeout: std::time::Duration::from_secs(5),
            mass_death_grace: std::time::Duration::ZERO,
            mass_death_min_count: 2,
            source_dir: None,
        };

        let mut primary = PrimaryCoordinator::new(
            config,
            transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        let binaries = vec![make_binary("doomed", 50)];

        let (id, rx, tx) = secondary_ends.remove(0);
        tokio::task::spawn_local(async move {
            let mut rx = rx;
            tx.send(DistributedMessage::SecondaryWelcome {
                sender_id: id.clone(), timestamp: 0.0,
                secondary_id: id.clone(),
                resources: vec![dynrunner_core::ResourceAmount {
                    kind: dynrunner_core::ResourceKind::memory(),
                    amount: 1024 * 1024 * 1024,
                }],
                worker_count: 1,
                hostname: "test".into(),
            }).unwrap();
            tx.send(DistributedMessage::CertExchange {
                sender_id: id.clone(), timestamp: 0.0,
                secondary_id: id.clone(),
                public_cert_pem: "FAKE".into(),
                ipv4_address: Some("127.0.0.1".into()),
                ipv6_address: None,
                quic_port: 5000,
            }).unwrap();
            // Fail every attempt — both main and retry pass. Issue a
            // TaskRequest after each failure so primary's operational
            // loop has a chance to dispatch the re-injected retry
            // task; otherwise the pool would sit with the task and
            // never drain.
            while let Some(msg) = rx.recv().await {
                let hash_opt = match &msg {
                    DistributedMessage::InitialAssignment { zip_files, .. } => zip_files
                        .first()
                        .and_then(|z| z.binaries.first())
                        .map(|e| e.hash.clone()),
                    DistributedMessage::TaskAssignment { file_hash, .. } => {
                        Some(file_hash.clone())
                    }
                    _ => None,
                };
                if let Some(h) = hash_opt {
                    tx.send(DistributedMessage::TaskFailed {
                        sender_id: id.clone(), timestamp: 0.0,
                        secondary_id: id.clone(),
                        worker_id: 0,
                        task_hash: h,
                        error_type: dynrunner_core::ErrorType::Recoverable,
                        error_message: "always fails".into(),
                    }).unwrap();
                    tx.send(DistributedMessage::TaskRequest {
                        sender_id: id.clone(), timestamp: 0.0,
                        secondary_id: id.clone(),
                        worker_id: 0,
                        available_resources: vec![dynrunner_core::ResourceAmount {
                            kind: dynrunner_core::ResourceKind::memory(),
                            amount: 1024 * 1024 * 1024,
                        }],
                    }).unwrap();
                }
            }
        });

        let (deps, ops, ope) = noop_phase_args();
        primary.run(binaries, deps, ops, ope).await.unwrap();

        // Main pass fails, retry pass fails again → permanent.
        assert_eq!(primary.completed_count(), 0);
        assert_eq!(primary.failed_count(), 1);
    }).await;
}

/// `retry_max_passes = 0` disables the retry loop entirely: a task
/// that fails Recoverable in the main pass becomes permanently failed
/// without a second attempt. Pins the budget knob's lower bound so
/// consumers that opt into "fail-fast" behaviour (e.g. CI smoke runs
/// where a single Recoverable signals a real bug rather than a flake)
/// get the contract they ask for.
#[tokio::test(flavor = "current_thread")]
async fn retry_max_passes_zero_disables_retry() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let (transport, mut secondary_ends) = setup_test(1);

        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: 1,
            connect_timeout: Duration::from_secs(5),
            peer_timeout: Duration::from_secs(5),
            keepalive_interval: Duration::from_secs(5),
            keepalive_miss_threshold: 3,
            source_pre_staged_root: None,
            uses_file_based_items: true,
            max_concurrent_per_type: std::collections::HashMap::new(),
            retry_max_passes: 0,
            fleet_dead_timeout: std::time::Duration::from_secs(30),
            mesh_ready_timeout: std::time::Duration::from_secs(5),
            mass_death_grace: std::time::Duration::ZERO,
            mass_death_min_count: 2,
            source_dir: None,
        };

        let mut primary = PrimaryCoordinator::new(
            config,
            transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        let binaries = vec![make_binary("doomed", 50)];

        let (id, rx, tx) = secondary_ends.remove(0);
        // Mirror the structure of `recoverable_failure_twice_becomes_permanent`:
        // fail every attempt with Recoverable. With retry_max_passes=0
        // the for-loop in run_retry_passes never iterates, so the
        // single main-pass failure is final.
        tokio::task::spawn_local(async move {
            let mut rx = rx;
            tx.send(DistributedMessage::SecondaryWelcome {
                sender_id: id.clone(), timestamp: 0.0,
                secondary_id: id.clone(),
                resources: vec![dynrunner_core::ResourceAmount {
                    kind: dynrunner_core::ResourceKind::memory(),
                    amount: 1024 * 1024 * 1024,
                }],
                worker_count: 1,
                hostname: "test".into(),
            }).unwrap();
            tx.send(DistributedMessage::CertExchange {
                sender_id: id.clone(), timestamp: 0.0,
                secondary_id: id.clone(),
                public_cert_pem: "FAKE".into(),
                ipv4_address: Some("127.0.0.1".into()),
                ipv6_address: None,
                quic_port: 5000,
            }).unwrap();
            while let Some(msg) = rx.recv().await {
                let hash_opt = match &msg {
                    DistributedMessage::InitialAssignment { zip_files, .. } => zip_files
                        .first()
                        .and_then(|z| z.binaries.first())
                        .map(|e| e.hash.clone()),
                    DistributedMessage::TaskAssignment { file_hash, .. } => {
                        Some(file_hash.clone())
                    }
                    _ => None,
                };
                if let Some(h) = hash_opt {
                    tx.send(DistributedMessage::TaskFailed {
                        sender_id: id.clone(), timestamp: 0.0,
                        secondary_id: id.clone(),
                        worker_id: 0,
                        task_hash: h,
                        error_type: dynrunner_core::ErrorType::Recoverable,
                        error_message: "always fails".into(),
                    }).unwrap();
                    tx.send(DistributedMessage::TaskRequest {
                        sender_id: id.clone(), timestamp: 0.0,
                        secondary_id: id.clone(),
                        worker_id: 0,
                        available_resources: vec![dynrunner_core::ResourceAmount {
                            kind: dynrunner_core::ResourceKind::memory(),
                            amount: 1024 * 1024 * 1024,
                        }],
                    }).unwrap();
                }
            }
        });

        let (deps, ops, ope) = noop_phase_args();
        primary.run(binaries, deps, ops, ope).await.unwrap();

        // Main pass fails once; retry loop is skipped entirely
        // because budget is 0 → permanent failure with no retry.
        assert_eq!(primary.completed_count(), 0);
        assert_eq!(primary.failed_count(), 1);
    }).await;
}


// ── End-to-end tests: real Primary + real Secondary with workers ──


/// Wire up a real SecondaryCoordinator as a tokio task, connected to the
/// primary via channels. Returns the secondary's channel ends that should
/// be plugged into the primary's ChannelTransport.
fn spawn_real_secondary(
    secondary_id: String,
    num_workers: u32,
    max_resources: dynrunner_core::ResourceMap,
) -> (
    tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,  // primary→secondary
    tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>, // secondary→primary
    tokio::task::JoinHandle<usize>,                    // returns completed count
) {
    spawn_real_secondary_with_src_network(secondary_id, num_workers, max_resources, None)
}

fn spawn_real_secondary_with_src_network(
    secondary_id: String,
    num_workers: u32,
    max_resources: dynrunner_core::ResourceMap,
    src_network: Option<std::path::PathBuf>,
) -> (
    tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
    tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
    tokio::task::JoinHandle<usize>,
) {
    // primary→secondary channel
    let (pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();
    // secondary→primary channel
    let (sec_to_pri_tx, sec_to_pri_rx) = tokio_mpsc::unbounded_channel();

    let handle = tokio::task::spawn_local(async move {
        let transport = ChannelPrimaryTransportEnd {
            tx: sec_to_pri_tx,
            rx: pri_to_sec_rx,
        };
        let config = SecondaryConfig {
            secondary_id,
            num_workers,
            max_resources,
            hostname: "test-host".into(),
            keepalive_interval: Duration::from_secs(60),
            src_network,
            src_tmp: None,
            peer_timeout: Duration::from_secs(120),
                keepalive_miss_threshold: 3,
            retry_max_passes: 1,
            primary_link_failure_threshold: 5,
            primary_link_failure_window: Duration::from_secs(30),
            setup_deadline: Duration::from_secs(60),
            is_observer: false,
        };
        let mut secondary = SecondaryCoordinator::new(
            config,
            transport,
            NoPeers,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );
        let mut factory = FakeWorkerFactory;
        secondary.run(&mut factory).await.unwrap();
        secondary.completed_count()
    });

    (pri_to_sec_tx, sec_to_pri_rx, handle)
}

/// Variant of `spawn_real_secondary` that drives a `FlakyWorkerFactory`
/// and threads `retry_max_passes` into `SecondaryConfig` so the
/// primary's retry pass is governed by the same knob the live
/// primary uses. Returns the
/// `(completed_count, primary_failed_count, retry_passes_used)`
/// triple the primary side ended up with — that's the assertion
/// surface for the post-demotion retry tests, since the local primary's
/// `failed_count()` is a stale forwarding cache once the operational
/// loop's exit condition fires (see `recoverable_failure_succeeds_on_retry_pass`).
///
/// `flaky` is cloned (its `Rc<RefCell<HashMap>>` is shared) so the test
/// caller can also inspect the per-task attempt counts after the run.
fn spawn_real_secondary_flaky(
    secondary_id: String,
    num_workers: u32,
    max_resources: dynrunner_core::ResourceMap,
    flaky: super::test_helpers::FlakyWorkerFactory,
    retry_max_passes: u32,
) -> (
    tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
    tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
    tokio::task::JoinHandle<(usize, usize, u32)>,
) {
    let (pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();
    let (sec_to_pri_tx, sec_to_pri_rx) = tokio_mpsc::unbounded_channel();

    let handle = tokio::task::spawn_local(async move {
        let transport = ChannelPrimaryTransportEnd {
            tx: sec_to_pri_tx,
            rx: pri_to_sec_rx,
        };
        let config = SecondaryConfig {
            secondary_id,
            num_workers,
            max_resources,
            hostname: "test-host".into(),
            // Tight keepalive so the keepalive-tick backstop fires
            // quickly enough that tests don't hit the default 60s
            // wait if any code path needs the periodic drain-check
            // (the synchronous one in `note_primary_item_failed` is
            // the primary trigger — this is just defensive).
            keepalive_interval: Duration::from_millis(50),
            src_network: None,
            src_tmp: None,
            peer_timeout: Duration::from_secs(120),
            keepalive_miss_threshold: 3,
            retry_max_passes,
            primary_link_failure_threshold: 5,
            primary_link_failure_window: Duration::from_secs(30),
            setup_deadline: Duration::from_secs(60),
            is_observer: false,
        };
        let mut secondary = SecondaryCoordinator::new(
            config,
            transport,
            NoPeers,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );
        let mut factory = flaky;
        secondary.run(&mut factory).await.unwrap();
        (
            secondary.completed_count(),
            secondary.primary_failed_count_for_test(),
            secondary.primary_retry_passes_used_for_test(),
        )
    });

    (pri_to_sec_tx, sec_to_pri_rx, handle)
}

/// End-to-end: 1 real primary + 1 real secondary (2 workers), 5 tasks.
#[tokio::test(flavor = "current_thread")]
async fn e2e_primary_and_secondary_single_node() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let secondary_id = "sec-0".to_string();
        let max_res = dynrunner_core::ResourceMap::from([(dynrunner_core::ResourceKind::memory(), 1024 * 1024 * 1024u64)]);

        let (pri_to_sec_tx, sec_to_pri_rx, sec_handle) =
            spawn_real_secondary(secondary_id.clone(), 2, max_res);

        // Build primary transport wired to the real secondary
        let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
        let mut outgoing = HashMap::new();
        outgoing.insert(secondary_id.clone(), pri_to_sec_tx);

        // Forward secondary→primary messages into the primary's incoming channel
        tokio::task::spawn_local(async move {
            let mut rx = sec_to_pri_rx;
            while let Some(msg) = rx.recv().await {
                if incoming_tx.send(msg).is_err() {
                    break;
                }
            }
        });

        let transport = ChannelSecondaryTransportEnd { outgoing, incoming_rx };
        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: 1,
            connect_timeout: Duration::from_secs(10),
            peer_timeout: Duration::from_secs(10),
                    keepalive_interval: Duration::from_secs(5),
                    keepalive_miss_threshold: 3,
                    source_pre_staged_root: None,
                    uses_file_based_items: true,
            max_concurrent_per_type: std::collections::HashMap::new(),
            retry_max_passes: 1,
            fleet_dead_timeout: std::time::Duration::from_secs(30),
            mesh_ready_timeout: std::time::Duration::from_secs(5),
            mass_death_grace: std::time::Duration::ZERO,
            mass_death_min_count: 2,
            source_dir: None,
        };

        let mut primary = PrimaryCoordinator::new(
            config,
            transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        let binaries: Vec<TaskInfo<TestId>> = (0..5)
            .map(|i| make_binary(&format!("bin_{i}"), 50 + i * 10))
            .collect();

        { let (deps, ops, ope) = noop_phase_args(); primary.run(binaries, deps, ops, ope).await.unwrap() };

        let completed = primary.completed_count();
        let failed = primary.failed_count();

        // Drop primary to close transport channels, allowing secondaries to exit
        drop(primary);

        let sec_completed = sec_handle.await.unwrap();

        assert_eq!(completed, 5);
        assert_eq!(failed, 0);
        assert_eq!(sec_completed, 5);
    }).await;
}

/// End-to-end: 1 real primary + 2 real secondaries (2 workers each), 10 tasks.
#[tokio::test(flavor = "current_thread")]
async fn e2e_primary_and_two_secondaries() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let max_res = dynrunner_core::ResourceMap::from([(dynrunner_core::ResourceKind::memory(), 2 * 1024 * 1024 * 1024u64)]);
        let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
        let mut outgoing = HashMap::new();
        let mut sec_handles = Vec::new();

        for i in 0..2u32 {
            let secondary_id = format!("sec-{i}");
            let (pri_to_sec_tx, sec_to_pri_rx, handle) =
                spawn_real_secondary(secondary_id.clone(), 2, max_res.clone());

            outgoing.insert(secondary_id, pri_to_sec_tx);
            sec_handles.push(handle);

            // Forward secondary→primary
            let tx = incoming_tx.clone();
            tokio::task::spawn_local(async move {
                let mut rx = sec_to_pri_rx;
                while let Some(msg) = rx.recv().await {
                    if tx.send(msg).is_err() {
                        break;
                    }
                }
            });
        }
        drop(incoming_tx); // Only forwarding tasks hold senders now

        let transport = ChannelSecondaryTransportEnd { outgoing, incoming_rx };
        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: 2,
            connect_timeout: Duration::from_secs(10),
            peer_timeout: Duration::from_secs(10),
                    keepalive_interval: Duration::from_secs(5),
                    keepalive_miss_threshold: 3,
                    source_pre_staged_root: None,
                    uses_file_based_items: true,
            max_concurrent_per_type: std::collections::HashMap::new(),
            retry_max_passes: 1,
            fleet_dead_timeout: std::time::Duration::from_secs(30),
            mesh_ready_timeout: std::time::Duration::from_secs(5),
            mass_death_grace: std::time::Duration::ZERO,
            mass_death_min_count: 2,
            source_dir: None,
        };

        let mut primary = PrimaryCoordinator::new(
            config,
            transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        let binaries: Vec<TaskInfo<TestId>> = (0..10)
            .map(|i| make_binary(&format!("bin_{i}"), 50 + i * 10))
            .collect();

        { let (deps, ops, ope) = noop_phase_args(); primary.run(binaries, deps, ops, ope).await.unwrap() };

        let completed = primary.completed_count();
        let failed = primary.failed_count();

        // Drop primary to close transport channels, allowing secondaries to exit
        drop(primary);

        let mut per_sec_completed = Vec::new();
        for handle in sec_handles {
            per_sec_completed.push(handle.await.unwrap());
        }

        assert_eq!(completed, 10);
        assert_eq!(failed, 0);
        // After the failover-survivability fix, every secondary's
        // `completed_tasks` reflects the CLUSTER view (own work +
        // peer broadcasts + primary-side forwards) so it can serve
        // as a promoted-primary on local-death without
        // re-dispatching done items. Each secondary therefore sees
        // all 10 completions, not just its own ~5. Asserting the
        // cluster-wide invariant directly: every secondary's set
        // has at least the total — anything less is a missed
        // forward that would cause a re-dispatch on failover.
        for (i, count) in per_sec_completed.iter().enumerate() {
            assert!(
                *count >= 10,
                "secondary {i} should have observed all 10 completions \
                 (cluster-wide view for failover survivability), got {count}"
            );
        }
    }).await;
}

/// Live distribution past the initial assignment, primary side: 1 secondary
/// with 2 workers, 20 binaries. The initial assignment can cover at most
/// 2 binaries (one per worker); the operational loop is responsible for
/// the remaining 18+. Pins the live-flow path that the legacy Python
/// never managed to get right.
#[tokio::test(flavor = "current_thread")]
async fn live_distribution_continues_past_initial_batch() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let (transport, secondary_ends) = setup_test(1);

        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: 1,
            connect_timeout: Duration::from_secs(5),
            peer_timeout: Duration::from_secs(5),
            keepalive_interval: Duration::from_secs(5),
            keepalive_miss_threshold: 3,
            source_pre_staged_root: None,
                    uses_file_based_items: true,
            max_concurrent_per_type: std::collections::HashMap::new(),
            retry_max_passes: 1,
            fleet_dead_timeout: std::time::Duration::from_secs(30),
            mesh_ready_timeout: std::time::Duration::from_secs(5),
            mass_death_grace: std::time::Duration::ZERO,
            mass_death_min_count: 2,
            source_dir: None,
        };

        let mut primary = PrimaryCoordinator::new(
            config,
            transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        let binaries: Vec<TaskInfo<TestId>> = (0..20)
            .map(|i| make_binary(&format!("bin_{i}"), 50 + i * 10))
            .collect();

        for (id, rx, tx) in secondary_ends {
            tokio::task::spawn_local(fake_secondary(
                id,
                2,
                1024 * 1024 * 1024,
                rx,
                tx,
            ));
        }

        { let (deps, ops, ope) = noop_phase_args(); primary.run(binaries, deps, ops, ope).await.unwrap() };

        // All 20 must complete; ≥ 18 went via the operational TaskRequest
        // → TaskAssignment loop (one secondary × 2 workers = 2 initial).
        assert_eq!(primary.completed_count(), 20);
        assert_eq!(primary.failed_count(), 0);
    }).await;
}

/// Pin that `notify_stage_file` actually emits a `StageFile` wire
/// message into the targeted secondary's incoming channel with the
/// exact fields supplied. This is what the packaging pipeline
/// depends on — without correct routing, the ExtractionCache on the
/// receiving secondary never gets primed.
#[tokio::test(flavor = "current_thread")]
async fn notify_stage_file_emits_wire_message() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let (transport, mut secondary_ends) = setup_test(1);

        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: 1,
            connect_timeout: Duration::from_secs(5),
            peer_timeout: Duration::from_secs(5),
            keepalive_interval: Duration::from_secs(5),
            keepalive_miss_threshold: 3,
            source_pre_staged_root: None,
                    uses_file_based_items: true,
            max_concurrent_per_type: std::collections::HashMap::new(),
            retry_max_passes: 1,
            fleet_dead_timeout: std::time::Duration::from_secs(30),
            mesh_ready_timeout: std::time::Duration::from_secs(5),
            mass_death_grace: std::time::Duration::ZERO,
            mass_death_min_count: 2,
            source_dir: None,
        };

        let mut primary: PrimaryCoordinator<_, _, _, TestId> =
            PrimaryCoordinator::new(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

        // Send directly via inherent method (skips the queue + run loop).
        primary
            .notify_stage_file(
                "sec-0",
                "deadbeefcafebabe".to_string(),
                "deadbeef".repeat(8),
                "rel/binary".to_string(),
                "scratch/binary".to_string(),
            )
            .await
            .expect("notify_stage_file should succeed");

        // Pull the message out of the targeted secondary's channel.
        let (id, mut to_sec_rx, _outgoing) = secondary_ends.remove(0);
        assert_eq!(id, "sec-0");
        let msg = to_sec_rx
            .recv()
            .await
            .expect("StageFile should be delivered to sec-0");
        match msg {
            DistributedMessage::StageFile {
                secondary_id,
                file_hash,
                src_path,
                dest_path,
                ..
            } => {
                assert_eq!(secondary_id, "sec-0");
                assert_eq!(file_hash, "deadbeefcafebabe");
                assert_eq!(src_path, "rel/binary");
                assert_eq!(dest_path, "scratch/binary");
            }
            other => panic!("expected StageFile, got {:?}", other.msg_type()),
        }
    }).await;
}

/// Phase S — replicated cluster ledger convergence: after a real
/// primary + secondary run completes, the secondary's mirror
/// `ClusterState` must reflect the same `Completed` count the primary
/// observed. Pins that:
///   - the post-`wait_for_peer_connections` `TaskAdded` batch reached
///     the secondary,
///   - per-completion `ClusterMutation::TaskCompleted` broadcasts were
///     applied to the secondary's mirror,
///   - the originator-side `apply_and_broadcast_cluster_mutations`
///     applied locally so the primary's own ledger converges.
#[tokio::test(flavor = "current_thread")]
async fn cluster_state_converges_on_primary_and_secondary() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let secondary_id = "sec-0".to_string();
            let max_res = dynrunner_core::ResourceMap::from([(
                dynrunner_core::ResourceKind::memory(),
                1024 * 1024 * 1024u64,
            )]);

            let (pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();
            let (sec_to_pri_tx, sec_to_pri_rx) = tokio_mpsc::unbounded_channel();

            let sec_handle = tokio::task::spawn_local(async move {
                let transport = ChannelPrimaryTransportEnd {
                    tx: sec_to_pri_tx,
                    rx: pri_to_sec_rx,
                };
                let config = SecondaryConfig {
                    secondary_id: "sec-0".into(),
                    num_workers: 2,
                    max_resources: max_res,
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
                };
                let mut secondary = SecondaryCoordinator::new(
                    config,
                    transport,
                    NoPeers,
                    ResourceStealingScheduler::memory(),
                    FixedEstimator(100),
                );
                let mut factory = FakeWorkerFactory;
                secondary.run(&mut factory).await.unwrap();
                (
                    secondary.completed_count(),
                    secondary.cluster_state_counts_for_test(),
                )
            });

            let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
            let mut outgoing = HashMap::new();
            outgoing.insert(secondary_id.clone(), pri_to_sec_tx);

            tokio::task::spawn_local(async move {
                let mut rx = sec_to_pri_rx;
                while let Some(msg) = rx.recv().await {
                    if incoming_tx.send(msg).is_err() {
                        break;
                    }
                }
            });

            let transport = ChannelSecondaryTransportEnd { outgoing, incoming_rx };
            let config = PrimaryConfig {
                node_id: "primary".into(),
                num_secondaries: 1,
                connect_timeout: Duration::from_secs(10),
                peer_timeout: Duration::from_secs(10),
                keepalive_interval: Duration::from_secs(5),
                keepalive_miss_threshold: 3,
                source_pre_staged_root: None,
                uses_file_based_items: true,
                max_concurrent_per_type: std::collections::HashMap::new(),
                retry_max_passes: 1,
                fleet_dead_timeout: std::time::Duration::from_secs(30),
                mesh_ready_timeout: std::time::Duration::from_secs(5),
                mass_death_grace: std::time::Duration::ZERO,
                mass_death_min_count: 2,
                source_dir: None,
            };
            let mut primary = PrimaryCoordinator::new(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let binaries: Vec<TaskInfo<TestId>> = (0..5)
                .map(|i| make_binary(&format!("bin_{i}"), 50 + i * 10))
                .collect();

            let (deps, ops, ope) = noop_phase_args();
            primary.run(binaries, deps, ops, ope).await.unwrap();

            let primary_counts = primary.cluster_state_counts_for_test();
            assert_eq!(
                primary_counts.completed, 5,
                "primary's own cluster_state should reflect all 5 completions"
            );
            assert_eq!(primary_counts.pending, 0);

            drop(primary);

            let (sec_completed, sec_counts) = sec_handle.await.unwrap();
            assert_eq!(sec_completed, 5);
            assert_eq!(
                sec_counts.completed, 5,
                "secondary's mirror should converge to 5 Completed via \
                 TaskAdded + TaskCompleted broadcasts"
            );
            assert_eq!(sec_counts.pending, 0);
        })
        .await;
}

/// End-to-end: pre-staged source mode locks in the path-mapping
/// contract added in a344b0e (PrimaryConfig.source_pre_staged_root).
///
/// Setup mocks the gateway-bind-mount: a tmpdir holds N fake binary
/// files; the primary's TaskInfo.path is the tmpdir-absolute path
/// (matching what a consumer's discover_items would emit). The
/// primary's `source_pre_staged_root` and the secondary's
/// `src_network` both point at the same tmpdir — in production the
/// gateway-host path and the in-container path are different (the
/// wrapper bind-mounts one to the other) but the test collapses the
/// two views since there's no container.
///
/// Asserts:
///   - All 5 binaries complete.
///   - The wire's local_path on each TaskAssignment was the
///     tmpdir-relative form (the strip happened); without that
///     strip, the secondary's `src_network.join(local_path)` would
///     return the absolute path as-is and `.exists()` would still
///     be true here, so the strip behaviour wouldn't be asserted.
///     We pin it by inspecting the wire on the secondary side.
///
/// This test was the missing pre-stage end-to-end coverage that
/// let bf1ce02 + a344b0e ship with a contract gap each.
#[tokio::test(flavor = "current_thread")]
async fn e2e_pre_staged_source_mode() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Two distinct tmpdirs — `gateway_path` is the host path
            // the primary's TaskInfo.path's are relative to (the
            // wrapper's bind-mount source); `container_path` is what
            // the secondary's `src_network` resolves to (the wrapper's
            // bind-mount destination). Production has these as
            // different paths the wrapper bind-mounts together; the
            // test models them as different tmpdirs with the SAME
            // file basenames present under each. This setup is the
            // load-bearing one: if the primary's `wire_local_path`
            // strip doesn't fire, the wire's local_path is the
            // gateway-absolute `<gateway>/bin_X`, secondary's
            // `src_network.join(<absolute>)` returns the
            // gateway-absolute path verbatim (Path::join rules), and
            // `<gateway>/bin_X.exists()` is true ONLY if the secondary
            // can see the gateway-side files — which it can't here.
            let gateway = tempfile::TempDir::new().expect("gateway tmpdir");
            let gateway_path = gateway.path().to_path_buf();
            let container = tempfile::TempDir::new().expect("container tmpdir");
            let container_path = container.path().to_path_buf();

            let names: Vec<String> = (0..5).map(|i| format!("bin_{i}")).collect();
            for name in &names {
                // Files exist only under the container view (the
                // gateway path is just a string the primary treats as
                // an authoritative root for prefix-stripping).
                std::fs::write(container_path.join(name), b"x")
                    .expect("write fake binary in container view");
            }

            // TaskInfos with paths under the gateway view.
            let binaries: Vec<TaskInfo<TestId>> = names
                .iter()
                .map(|n| {
                    let mut b = make_binary(n, 1);
                    b.path = gateway_path.join(n);
                    b
                })
                .collect();

            let secondary_id = "sec-0".to_string();
            let max_res = dynrunner_core::ResourceMap::from([(
                dynrunner_core::ResourceKind::memory(),
                1024 * 1024 * 1024u64,
            )]);

            let (pri_to_sec_tx, sec_to_pri_rx, sec_handle) =
                spawn_real_secondary_with_src_network(
                    secondary_id.clone(),
                    2,
                    max_res,
                    Some(container_path.clone()),
                );

            let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
            let mut outgoing = HashMap::new();
            outgoing.insert(secondary_id.clone(), pri_to_sec_tx);

            tokio::task::spawn_local(async move {
                let mut rx = sec_to_pri_rx;
                while let Some(msg) = rx.recv().await {
                    if incoming_tx.send(msg).is_err() {
                        break;
                    }
                }
            });

            let transport = ChannelSecondaryTransportEnd { outgoing, incoming_rx };
            let config = PrimaryConfig {
                node_id: "primary".into(),
                num_secondaries: 1,
                connect_timeout: Duration::from_secs(10),
                peer_timeout: Duration::from_secs(10),
                keepalive_interval: Duration::from_secs(5),
                keepalive_miss_threshold: 3,
                source_pre_staged_root: Some(gateway_path.clone()),
                uses_file_based_items: true,
            max_concurrent_per_type: std::collections::HashMap::new(),
            retry_max_passes: 1,
            fleet_dead_timeout: std::time::Duration::from_secs(30),
            mesh_ready_timeout: std::time::Duration::from_secs(5),
            mass_death_grace: std::time::Duration::ZERO,
            mass_death_min_count: 2,
            source_dir: None,
            };
            let mut primary = PrimaryCoordinator::new(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            {
                let (deps, ops, ope) = noop_phase_args();
                primary.run(binaries, deps, ops, ope).await.unwrap()
            };

            let completed = primary.completed_count();
            let failed = primary.failed_count();
            drop(primary);

            let sec_completed = sec_handle.await.unwrap();

            assert_eq!(completed, 5, "primary should see 5 completed in pre-staged mode");
            assert_eq!(failed, 0, "no failures expected");
            assert_eq!(sec_completed, 5, "secondary should resolve all 5 via src_network");
        })
        .await;
}

/// End-to-end: `uses_file_based_items=false` (FR-2). The TaskInfo
/// `path` is an opaque identifier — no real file at that location.
/// The framework MUST NOT stat/hash/resolve it; the secondary
/// passes `local_path` through to the worker verbatim. Asserts all
/// 5 dispatch successfully despite the paths pointing at nowhere.
///
/// Without the flag, the same setup (no src_network, no
/// queue_initial_staging) would hit the unresolvable-task guard
/// and fail every item NonRecoverable.
#[tokio::test(flavor = "current_thread")]
async fn e2e_uses_file_based_items_false() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let secondary_id = "sec-0".to_string();
            let max_res = dynrunner_core::ResourceMap::from([(
                dynrunner_core::ResourceKind::memory(),
                1024 * 1024 * 1024u64,
            )]);

            let (pri_to_sec_tx, sec_to_pri_rx, sec_handle) =
                spawn_real_secondary(secondary_id.clone(), 2, max_res);

            let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
            let mut outgoing = HashMap::new();
            outgoing.insert(secondary_id.clone(), pri_to_sec_tx);

            tokio::task::spawn_local(async move {
                let mut rx = sec_to_pri_rx;
                while let Some(msg) = rx.recv().await {
                    if incoming_tx.send(msg).is_err() {
                        break;
                    }
                }
            });

            let transport = ChannelSecondaryTransportEnd { outgoing, incoming_rx };
            let config = PrimaryConfig {
                node_id: "primary".into(),
                num_secondaries: 1,
                connect_timeout: Duration::from_secs(10),
                peer_timeout: Duration::from_secs(10),
                keepalive_interval: Duration::from_secs(5),
                keepalive_miss_threshold: 3,
                source_pre_staged_root: None,
                uses_file_based_items: false,
                max_concurrent_per_type: std::collections::HashMap::new(),
                retry_max_passes: 1,
                fleet_dead_timeout: std::time::Duration::from_secs(30),
                mesh_ready_timeout: std::time::Duration::from_secs(5),
            mass_death_grace: std::time::Duration::ZERO,
            mass_death_min_count: 2,
            source_dir: None,
            };
            let mut primary = PrimaryCoordinator::new(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // Items with paths that don't back to anything on disk.
            // In file-based mode this would fail the dispatch guard;
            // with uses_file_based_items=false the framework treats
            // these as opaque identifiers.
            let binaries: Vec<TaskInfo<TestId>> = (0..5)
                .map(|i| {
                    let mut b = make_binary(&format!("opaque_{i}"), 1);
                    b.path = std::path::PathBuf::from(format!("opaque://manifest-{i}"));
                    b
                })
                .collect();

            {
                let (deps, ops, ope) = noop_phase_args();
                primary.run(binaries, deps, ops, ope).await.unwrap()
            };

            let completed = primary.completed_count();
            let failed = primary.failed_count();
            drop(primary);

            let sec_completed = sec_handle.await.unwrap();

            assert_eq!(completed, 5, "primary should see 5 completed");
            assert_eq!(failed, 0, "no failures expected");
            assert_eq!(sec_completed, 5, "secondary should pass paths through");
        })
        .await;
}

/// FR-1 (scoped): per-type `max_concurrent` cap. With a 4-worker
/// secondary and a cap of 2 on type "compile", the scheduler should
/// never have more than 2 "compile" items in flight at once even
/// though the worker pool could absorb 4. Other types
/// (uncapped) run at the full pool width.
///
/// This isn't a strict-mid-flight assertion (the test fakes complete
/// every assigned task instantly so the in-flight overlap window is
/// tiny); it asserts the run COMPLETES correctly with the cap on
/// (no deadlock, all items dispatched). The real-world value of
/// the cap shows up under slow workers; here we just pin the wire
/// flow + bookkeeping.
#[tokio::test(flavor = "current_thread")]
async fn e2e_per_type_max_concurrent() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let secondary_id = "sec-0".to_string();
            let max_res = dynrunner_core::ResourceMap::from([(
                dynrunner_core::ResourceKind::memory(),
                1024 * 1024 * 1024u64,
            )]);

            let (pri_to_sec_tx, sec_to_pri_rx, sec_handle) =
                spawn_real_secondary(secondary_id.clone(), 4, max_res);

            let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
            let mut outgoing = HashMap::new();
            outgoing.insert(secondary_id.clone(), pri_to_sec_tx);

            tokio::task::spawn_local(async move {
                let mut rx = sec_to_pri_rx;
                while let Some(msg) = rx.recv().await {
                    if incoming_tx.send(msg).is_err() {
                        break;
                    }
                }
            });

            let mut caps = std::collections::HashMap::new();
            caps.insert(dynrunner_core::TypeId::from("compile"), 2);

            let transport = ChannelSecondaryTransportEnd { outgoing, incoming_rx };
            let config = PrimaryConfig {
                node_id: "primary".into(),
                num_secondaries: 1,
                connect_timeout: Duration::from_secs(10),
                peer_timeout: Duration::from_secs(10),
                keepalive_interval: Duration::from_secs(5),
                keepalive_miss_threshold: 3,
                source_pre_staged_root: None,
                uses_file_based_items: true,
                max_concurrent_per_type: caps,
                retry_max_passes: 1,
                fleet_dead_timeout: std::time::Duration::from_secs(30),
                mesh_ready_timeout: std::time::Duration::from_secs(5),
            mass_death_grace: std::time::Duration::ZERO,
            mass_death_min_count: 2,
            source_dir: None,
            };
            let mut primary = PrimaryCoordinator::new(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // 8 items of type "compile" (capped at 2 concurrent) +
            // 8 items of type "merge" (uncapped) = 16 total.
            let binaries: Vec<TaskInfo<TestId>> = (0..16)
                .map(|i| {
                    let mut b = make_binary(&format!("bin_{i}"), 1);
                    b.type_id = if i < 8 {
                        dynrunner_core::TypeId::from("compile")
                    } else {
                        dynrunner_core::TypeId::from("merge")
                    };
                    b
                })
                .collect();

            {
                let (deps, ops, ope) = noop_phase_args();
                primary.run(binaries, deps, ops, ope).await.unwrap()
            };

            let completed = primary.completed_count();
            let failed = primary.failed_count();
            drop(primary);

            let sec_completed = sec_handle.await.unwrap();

            assert_eq!(completed, 16, "all 16 should complete");
            assert_eq!(failed, 0, "no failures expected");
            assert_eq!(sec_completed, 16, "secondary saw all 16");
        })
        .await;
}

/// Pin the wire-strip behaviour directly: PrimaryConfig::wire_local_path
/// returns the absolute path verbatim outside pre-staged mode and the
/// relative-to-root form inside it. Paths that don't sit under the
/// root pass through unchanged (the secondary then surfaces the
/// mismatch as NonRecoverable).
#[test]
fn wire_local_path_strips_pre_staged_prefix() {
    let mut cfg = PrimaryConfig::default();

    let mut bin = make_binary("x", 0);
    bin.path = std::path::PathBuf::from("/srv/data/bin_0");

    // Off → verbatim.
    assert_eq!(cfg.wire_local_path(&bin), "/srv/data/bin_0");

    // On with matching prefix (abs-under-src) → relative tail.
    cfg.source_pre_staged_root = Some(std::path::PathBuf::from("/srv/data"));
    assert_eq!(cfg.wire_local_path(&bin), "bin_0");

    // On with mismatching prefix (abs-out-of-tree) → verbatim
    // (consumer misconfig is surfaced downstream by
    // `resolve_pre_staged` returning None, not silently re-routed).
    cfg.source_pre_staged_root = Some(std::path::PathBuf::from("/other/prefix"));
    assert_eq!(cfg.wire_local_path(&bin), "/srv/data/bin_0");

    // On with a relative `binary.path` (rel-under-src — the post-
    // Bug-B wire-id shape consumers emit). Resolving the relative
    // path against the prestaged root and re-stripping yields the
    // original relative form verbatim, which is exactly what
    // `secondary.src_network.join(<wire>)` expects. Pre-fix the
    // relative path silently fell through the strip-prefix Err arm
    // and shipped as-is — the value happened to be correct, but
    // for the wrong reason; this test pins the explicit round-trip.
    cfg.source_pre_staged_root = Some(std::path::PathBuf::from("/srv/data"));
    bin.path = std::path::PathBuf::from("bin_0");
    assert_eq!(cfg.wire_local_path(&bin), "bin_0");

    bin.path = std::path::PathBuf::from("nested/bin_1");
    assert_eq!(cfg.wire_local_path(&bin), "nested/bin_1");
}

/// Multi-secondary mesh-ready gate: the primary must NOT issue
/// `PromotePrimary` until every connected secondary has reported
/// `MeshReady`. Pre-fix the promotion fired ~750µs after cert-
/// exchange completed; the promoted secondary then became
/// authoritative against a still-forming peer mesh, and every
/// pre-mesh-formation peer-broadcast routed into the void for up
/// to 30s. This test pins the new ordering: wire `PromotePrimary`
/// arrives at every fake secondary AFTER all of them have sent
/// their own `MeshReady`. Implementation uses a per-secondary
/// `tokio::sync::oneshot` to gate the MeshReady send so the test
/// can drive the order deterministically.
#[tokio::test(flavor = "current_thread")]
async fn promote_primary_held_until_every_secondary_reports_mesh_ready() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            const N_SECONDARIES: u32 = 3;
            let (transport, secondary_ends) = setup_test(N_SECONDARIES);

            // Per-secondary oneshot triggers. Test drives them in
            // order to enforce: the primary doesn't fire
            // PromotePrimary until ALL three have flipped.
            let mut mesh_triggers: Vec<tokio::sync::oneshot::Sender<()>> = Vec::new();
            // Per-secondary observation: did this secondary see
            // PromotePrimary BEFORE it was allowed to send
            // MeshReady? (true = bug present)
            let mut promote_seen_pre_mesh_observers: Vec<
                tokio::sync::oneshot::Receiver<bool>,
            > = Vec::new();

            for (id, rx, tx) in secondary_ends {
                let (mesh_tx, mesh_rx) = tokio::sync::oneshot::channel::<()>();
                let (obs_tx, obs_rx) = tokio::sync::oneshot::channel::<bool>();
                mesh_triggers.push(mesh_tx);
                promote_seen_pre_mesh_observers.push(obs_rx);
                tokio::task::spawn_local(gated_mesh_secondary(
                    id,
                    2,
                    1024 * 1024 * 1024,
                    rx,
                    tx,
                    mesh_rx,
                    obs_tx,
                ));
            }

            let config = PrimaryConfig {
                node_id: "primary".into(),
                num_secondaries: N_SECONDARIES,
                connect_timeout: Duration::from_secs(5),
                peer_timeout: Duration::from_secs(5),
                keepalive_interval: Duration::from_secs(5),
                keepalive_miss_threshold: 3,
                source_pre_staged_root: None,
                uses_file_based_items: true,
                max_concurrent_per_type: std::collections::HashMap::new(),
                retry_max_passes: 1,
                fleet_dead_timeout: std::time::Duration::from_secs(30),
                // Generous timeout so the test can fire triggers
                // sequentially without racing the deadline.
                mesh_ready_timeout: std::time::Duration::from_secs(10),
                mass_death_grace: std::time::Duration::ZERO,
                mass_death_min_count: 2,
                source_dir: None,
            };

            let mut primary = PrimaryCoordinator::new(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let binaries: Vec<TaskInfo<TestId>> = (0..6)
                .map(|i| make_binary(&format!("bin_{i}"), 100))
                .collect();

            // Drive the primary's coordination pipeline on a child
            // task so the test body can release MeshReady triggers
            // in sequence and observe the gate.
            let primary_handle = tokio::task::spawn_local(async move {
                let (deps, ops, ope) = noop_phase_args();
                primary.run(binaries, deps, ops, ope).await.unwrap();
                primary.completed_count()
            });

            // Release MeshReady triggers one at a time. Between
            // each release, yield enough times for the primary's
            // wait loop to observe the freshly-arrived
            // MeshReady. The primary must NOT have advanced past
            // `wait_for_mesh_ready` until all three triggers have
            // fired — otherwise the per-secondary "did I see
            // PromotePrimary before being allowed to MeshReady?"
            // observer would have reported true for some of them.
            for trigger in mesh_triggers {
                trigger.send(()).expect("trigger send");
                // Yield repeatedly so the primary task gets a
                // chance to dequeue & process the MeshReady. A
                // single `yield_now` isn't enough on a
                // current_thread runtime when the primary is
                // mid-message, so spam it.
                for _ in 0..16 {
                    tokio::task::yield_now().await;
                }
            }

            // Collect the per-secondary observations. None of
            // them should have seen PromotePrimary before being
            // allowed to send MeshReady.
            for (i, obs) in promote_seen_pre_mesh_observers.into_iter().enumerate() {
                let saw = obs.await.expect("observer recv");
                assert!(
                    !saw,
                    "secondary {i} observed PromotePrimary BEFORE its own \
                     MeshReady was allowed to send — primary's \
                     wait_for_mesh_ready step is not gating PromotePrimary"
                );
            }

            let completed = primary_handle.await.unwrap();
            assert_eq!(completed, 6, "all 6 tasks should complete");
        })
        .await;
}

/// Fake secondary that defers `MeshReady` until the test fires
/// `mesh_trigger`. Reports via `observer` whether it saw
/// `PromotePrimary` arrive before its `MeshReady` was permitted to
/// send (true = bug). Otherwise behaves like `fake_secondary`.
async fn gated_mesh_secondary(
    secondary_id: String,
    num_workers: u32,
    ram_bytes: u64,
    mut incoming_from_primary: tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
    outgoing_to_primary: tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
    mesh_trigger: tokio::sync::oneshot::Receiver<()>,
    observer: tokio::sync::oneshot::Sender<bool>,
) {
    use dynrunner_protocol_primary_secondary::MessageType;

    outgoing_to_primary
        .send(DistributedMessage::SecondaryWelcome {
            sender_id: secondary_id.clone(),
            timestamp: 0.0,
            secondary_id: secondary_id.clone(),
            resources: vec![dynrunner_core::ResourceAmount {
                kind: dynrunner_core::ResourceKind::memory(),
                amount: ram_bytes,
            }],
            worker_count: num_workers,
            hostname: "test-host".into(),
        })
        .unwrap();

    outgoing_to_primary
        .send(DistributedMessage::CertExchange {
            sender_id: secondary_id.clone(),
            timestamp: 0.0,
            secondary_id: secondary_id.clone(),
            public_cert_pem: "FAKE_CERT".into(),
            ipv4_address: Some("127.0.0.1".into()),
            ipv6_address: None,
            quic_port: 5000,
        })
        .unwrap();

    // Race: receive the trigger to send MeshReady against
    // observing PromotePrimary on the inbound path. If
    // PromotePrimary arrives first, the gate failed.
    let mut mesh_trigger_opt = Some(mesh_trigger);
    let mut observer_opt = Some(observer);
    let mut mesh_sent = false;
    let mut promote_seen_pre_mesh = false;

    loop {
        // While we're still pre-MeshReady, race the trigger
        // against an inbound PromotePrimary. After MeshReady has
        // been sent, the trigger arm is removed and we fall back
        // to a normal recv loop.
        if !mesh_sent {
            let trigger = mesh_trigger_opt.as_mut().unwrap();
            tokio::select! {
                _ = trigger => {
                    outgoing_to_primary
                        .send(DistributedMessage::MeshReady {
                            sender_id: secondary_id.clone(),
                            timestamp: 0.0,
                            secondary_id: secondary_id.clone(),
                            peer_count: 0,
                        })
                        .unwrap();
                    mesh_sent = true;
                    mesh_trigger_opt = None;
                    if let Some(obs) = observer_opt.take() {
                        let _ = obs.send(promote_seen_pre_mesh);
                    }
                }
                msg = incoming_from_primary.recv() => match msg {
                    Some(m) => {
                        if matches!(m.msg_type(), MessageType::PromotePrimary) {
                            promote_seen_pre_mesh = true;
                        }
                        handle_inbound_for_gated_secondary(
                            &secondary_id,
                            &outgoing_to_primary,
                            ram_bytes,
                            m,
                        );
                    }
                    None => break,
                },
            }
        } else {
            match incoming_from_primary.recv().await {
                Some(m) => handle_inbound_for_gated_secondary(
                    &secondary_id,
                    &outgoing_to_primary,
                    ram_bytes,
                    m,
                ),
                None => break,
            }
        }
    }
}

fn handle_inbound_for_gated_secondary(
    secondary_id: &str,
    outgoing: &tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
    ram_bytes: u64,
    msg: DistributedMessage<TestId>,
) {
    match msg {
        DistributedMessage::PeerInfo { .. } => {}
        DistributedMessage::InitialAssignment {
            zip_files,
            workers_ready,
            ..
        } => {
            // Pair each binary with the worker the primary's
            // `assign_initial` placed it on (positional alignment of
            // `workers_ready[i]` and `zip_files[0].binaries[i]` is
            // `perform_initial_assignment`'s contract). Always
            // emitting `worker_id=0` worked pre-demotion because the
            // primary's kickstart re-dispatch eventually cleared
            // every worker's `current_task` regardless of which one
            // a TaskComplete was attributed to. Post-demotion the
            // primary stops dispatching after `PromotePrimary`, so a
            // mis-attributed TaskComplete leaves the OTHER worker
            // permanently mid-dispatch and `active_workers > 0`
            // forever — operational_loop never terminates.
            let entries: Vec<_> = zip_files
                .iter()
                .flat_map(|zf| zf.binaries.iter())
                .collect();
            for (idx, entry) in entries.iter().enumerate() {
                let worker_id = workers_ready
                    .get(idx)
                    .map(|w| w.worker_id)
                    .unwrap_or(0);
                let _ = outgoing.send(DistributedMessage::TaskComplete {
                    sender_id: secondary_id.into(),
                    timestamp: 0.0,
                    secondary_id: secondary_id.into(),
                    worker_id,
                    task_hash: entry.hash.clone(),
                    result_data: None,
                });
                let _ = outgoing.send(DistributedMessage::TaskRequest {
                    sender_id: secondary_id.into(),
                    timestamp: 0.0,
                    secondary_id: secondary_id.into(),
                    worker_id,
                    available_resources: vec![dynrunner_core::ResourceAmount {
                        kind: dynrunner_core::ResourceKind::memory(),
                        amount: ram_bytes,
                    }],
                });
            }
        }
        DistributedMessage::TransferComplete { .. } => {}
        DistributedMessage::TaskAssignment { file_hash, .. } => {
            let _ = outgoing.send(DistributedMessage::TaskComplete {
                sender_id: secondary_id.into(),
                timestamp: 0.0,
                secondary_id: secondary_id.into(),
                worker_id: 0,
                task_hash: file_hash,
                result_data: None,
            });
            let _ = outgoing.send(DistributedMessage::TaskRequest {
                sender_id: secondary_id.into(),
                timestamp: 0.0,
                secondary_id: secondary_id.into(),
                worker_id: 0,
                available_resources: vec![dynrunner_core::ResourceAmount {
                    kind: dynrunner_core::ResourceKind::memory(),
                    amount: ram_bytes,
                }],
            });
        }
        _ => {}
    }
}

/// End-to-end pin for the "peer ipv4/ipv6 addresses reach the dialer"
/// plumbing: spin up a primary against two channel-transport
/// secondaries, have each advertise BOTH families in CertExchange, and
/// inspect the `PeerInfo` broadcast that lands at one of them. The
/// peers vector must carry the OTHER secondary's ipv4 AND ipv6 — pre-
/// fix `peer_setup::send_peer_lists` hardcoded `ipv6: None`, which
/// produced empty happy-eyeballs candidate sets on dual-stack hosts
/// where ipv4 was administratively blocked between compute nodes.
///
/// The test snoops `PeerInfo` by intercepting the second secondary's
/// inbound channel: a forwarder task drains the channel, copies any
/// `PeerInfo` into a `oneshot` for assertion, then forwards every
/// message to the real fake-secondary task so the lifecycle
/// (PeerInfo → InitialAssignment → TaskAssignment → TaskComplete)
/// completes and `primary.run` returns.
#[tokio::test(flavor = "current_thread")]
async fn peer_info_broadcast_carries_both_ipv4_and_ipv6() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let (transport, mut secondary_ends) = setup_test(2);

        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: 2,
            connect_timeout: Duration::from_secs(5),
            peer_timeout: Duration::from_secs(5),
            keepalive_interval: Duration::from_secs(5),
            keepalive_miss_threshold: 3,
            source_pre_staged_root: None,
            uses_file_based_items: true,
            max_concurrent_per_type: std::collections::HashMap::new(),
            retry_max_passes: 1,
            fleet_dead_timeout: std::time::Duration::from_secs(30),
            mesh_ready_timeout: std::time::Duration::from_secs(5),
            mass_death_grace: std::time::Duration::ZERO,
            mass_death_min_count: 2,
            source_dir: None,
        };

        let mut primary = PrimaryCoordinator::new(
            config,
            transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        let binaries = vec![make_binary("a", 50)];

        // Two secondaries, each advertising a distinct ipv4 + ipv6.
        // sec-0 → (10.0.0.1, 2001:db8::1)
        // sec-1 → (10.0.0.2, 2001:db8::2)
        // The assertion below pulls the PeerInfo sec-1 receives and
        // looks up sec-0's entry — that's the entry whose addresses
        // were in flight through `handle_cert_exchange` →
        // `SecondaryConnectionState` → `send_peer_lists`.
        let addrs: Vec<(String, String)> = vec![
            ("10.0.0.1".into(), "2001:db8::1".into()),
            ("10.0.0.2".into(), "2001:db8::2".into()),
        ];

        // Snoop the second secondary's primary→secondary channel: a
        // forwarder task copies any `PeerInfo` into a oneshot before
        // re-forwarding every message to the actual fake-secondary
        // task. Without the forward step, the fake never sees
        // InitialAssignment / TransferComplete and `primary.run`
        // hangs on `wait_for_peer_connections` budgeting → timeout.
        let (peer_info_tx, peer_info_rx) = tokio::sync::oneshot::channel();
        let mut peer_info_tx = Some(peer_info_tx);

        // Pull sec-1 out first so we can wrap its inbound channel.
        // `secondary_ends` is ordered sec-0, sec-1.
        let (sec1_id, sec1_inbound, sec1_outbound) = secondary_ends.remove(1);
        let (sec0_id, sec0_inbound, sec0_outbound) = secondary_ends.remove(0);

        // sec-0: vanilla fake_secondary_with_addrs.
        let (sec0_ipv4, sec0_ipv6) = addrs[0].clone();
        tokio::task::spawn_local(fake_secondary_with_addrs(
            sec0_id.clone(),
            1,
            1024 * 1024 * 1024,
            Some(sec0_ipv4),
            Some(sec0_ipv6),
            sec0_inbound,
            sec0_outbound,
        ));

        // sec-1: forwarder + fake.
        let (sec1_inner_tx, sec1_inner_rx) = tokio_mpsc::unbounded_channel();
        let (sec1_ipv4, sec1_ipv6) = addrs[1].clone();
        tokio::task::spawn_local(fake_secondary_with_addrs(
            sec1_id.clone(),
            1,
            1024 * 1024 * 1024,
            Some(sec1_ipv4),
            Some(sec1_ipv6),
            sec1_inner_rx,
            sec1_outbound,
        ));

        tokio::task::spawn_local(async move {
            let mut rx = sec1_inbound;
            while let Some(msg) = rx.recv().await {
                if let DistributedMessage::PeerInfo { peers, .. } = &msg {
                    if let Some(tx) = peer_info_tx.take() {
                        let _ = tx.send(peers.clone());
                    }
                }
                if sec1_inner_tx.send(msg).is_err() {
                    break;
                }
            }
        });

        let (deps, ops, ope) = noop_phase_args();
        primary.run(binaries, deps, ops, ope).await.unwrap();

        let peers = peer_info_rx.await.expect("PeerInfo never delivered");

        let sec0_peer = peers
            .iter()
            .find(|p| p.secondary_id == "sec-0")
            .expect("sec-0 missing from PeerInfo");
        assert_eq!(
            sec0_peer.ipv4.as_deref(),
            Some("10.0.0.1"),
            "primary dropped ipv4 from peer broadcast"
        );
        assert_eq!(
            sec0_peer.ipv6.as_deref(),
            Some("2001:db8::1"),
            "primary dropped ipv6 from peer broadcast — happy-eyeballs \
             dialer would race only ipv4 candidates and fail on \
             clusters where ipv4 is administratively blocked between \
             compute nodes"
        );
    })
    .await;
}

/// Regression: `promote_primary` flips `self.demoted` to true
/// and from that point `dispatch_to_idle_workers` is a no-op on the
/// scheduler — i.e. the local primary stops handing out work as
/// soon as it has handed authority off to the primary.
///
/// Without this contract the local primary and the promoted secondary
/// would both run dispatch in parallel against the same pool, racing
/// for workers and creating duplicate assignments / inconsistent
/// ledger state. See `demoted` doc on `PrimaryCoordinator` for the
/// full rationale.
#[tokio::test(flavor = "current_thread")]
async fn promote_primary_demotes_local_and_disables_dispatch() {
    use crate::state::{SecondaryConnection, SecondaryConnectionState};
    use dynrunner_scheduler_api::PendingPool;

    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let (transport, _ends) = setup_test(1);
        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: 1,
            connect_timeout: Duration::from_secs(5),
            peer_timeout: Duration::from_secs(5),
            keepalive_interval: Duration::from_secs(5),
            keepalive_miss_threshold: 3,
            source_pre_staged_root: None,
            uses_file_based_items: true,
            max_concurrent_per_type: std::collections::HashMap::new(),
            retry_max_passes: 1,
            fleet_dead_timeout: std::time::Duration::from_secs(30),
            mesh_ready_timeout: std::time::Duration::from_secs(5),
            mass_death_grace: std::time::Duration::ZERO,
            mass_death_min_count: 2,
            source_dir: None,
        };
        let mut primary: PrimaryCoordinator<_, _, _, TestId> = PrimaryCoordinator::new(
            config,
            transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        // Pre-conditions: a registered secondary, a single idle
        // virtual worker bound to it, and a pool with one queued
        // binary that `dispatch_to_idle_workers` would otherwise
        // pick up. We bypass `run()` because we want to drive
        // `promote_primary` and `dispatch_to_idle_workers`
        // in isolation.
        let phase = dynrunner_core::PhaseId::from("default");
        let mut pool = PendingPool::<TestId>::new(
            [phase.clone()],
            std::collections::HashMap::new(),
        )
        .expect("default-phase pool");
        let bin = make_binary("solo", 50);
        pool.extend([bin.clone()]).expect("valid extend");
        primary.pending = Some(pool);
        primary.phase_completed.insert(phase.clone(), 0);
        primary.phase_failed.insert(phase, 0);
        primary.all_binaries = vec![bin];
        primary.total_tasks = 1;

        let conn = SecondaryConnection::new("sec-0".into())
            .receive_welcome(1, vec![], "host".into(), 0, None)
            .receive_cert_exchange(String::new(), None, None, 0)
            .begin_peer_discovery()
            .peers_ready()
            .assignments_sent();
        primary.secondaries.insert(
            "sec-0".into(),
            SecondaryConnectionState::Operational(conn),
        );
        primary.workers.push(RemoteWorkerState {
            worker_id: 0,
            secondary_id: "sec-0".into(),
            resource_budgets: dynrunner_core::ResourceMap::from([(
                dynrunner_core::ResourceKind::memory(),
                1024 * 1024 * 1024u64,
            )]),
            current_task: None,
            estimated_resources: dynrunner_core::ResourceMap::new(),
            is_idle: true,
        });

        assert!(!primary.demoted, "fresh primary is not demoted");

        // Promote: should set `demoted = true` and emit a
        // `PromotePrimary` to the secondary (we don't observe the
        // wire here; the demotion flag is the contract under test).
        primary.promote_primary().await.unwrap();
        assert!(primary.demoted, "promote_primary must demote local");
        assert_eq!(
            primary.primary_id.as_deref(),
            Some("sec-0"),
            "promote_primary records the routing target"
        );

        // The pool still has its queued binary; the worker is
        // still idle. Pre-fix `dispatch_to_idle_workers` would
        // happily take the binary from the pool and assign it.
        // Post-fix it must early-return without touching pool
        // state — since the primary now owns dispatch.
        let pool_len_before = primary.pool().len();
        let view_before = primary.pool().view_for_worker(0).len();
        assert_eq!(pool_len_before, 1);
        assert_eq!(view_before, 1);
        assert!(primary.workers[0].is_idle);
        assert!(primary.workers[0].current_task.is_none());

        primary.dispatch_to_idle_workers().await.unwrap();

        assert_eq!(
            primary.pool().len(),
            pool_len_before,
            "dispatch_to_idle_workers must not take from pool when demoted"
        );
        assert!(
            primary.workers[0].is_idle,
            "worker must remain idle when local primary is demoted"
        );
        assert!(
            primary.workers[0].current_task.is_none(),
            "worker must not be assigned a task when local primary is demoted"
        );
    }).await;
}

// ── Backlog L2: load-aware dispatch ordering ──

fn make_remote_worker(
    worker_id: u32,
    secondary_id: &str,
    busy: bool,
) -> RemoteWorkerState<TestId> {
    RemoteWorkerState {
        worker_id,
        secondary_id: secondary_id.into(),
        resource_budgets: dynrunner_core::ResourceMap::new(),
        current_task: if busy { Some(make_binary("placeholder", 0)) } else { None },
        estimated_resources: dynrunner_core::ResourceMap::new(),
        is_idle: !busy,
    }
}

#[test]
fn dispatch_order_equal_load_preserves_worker_id_order() {
    let workers = vec![
        make_remote_worker(0, "A", false),
        make_remote_worker(1, "A", false),
        make_remote_worker(2, "B", false),
        make_remote_worker(3, "B", false),
    ];
    let order = super::lifecycle::dispatch_order(&workers);
    assert_eq!(order, vec![0, 1, 2, 3]);
}

#[test]
fn dispatch_order_prefers_less_loaded_secondary() {
    // A has 2 busy + 2 idle (load 2). B has 0 busy + 2 idle (load 0).
    // B's idle workers must come before A's even though A's worker_ids
    // are lower — the pre-fix iteration order would have given A first
    // dibs on tail-of-phase items.
    let workers = vec![
        make_remote_worker(0, "A", true),
        make_remote_worker(1, "A", true),
        make_remote_worker(2, "A", false),
        make_remote_worker(3, "A", false),
        make_remote_worker(4, "B", false),
        make_remote_worker(5, "B", false),
    ];
    let order = super::lifecycle::dispatch_order(&workers);
    assert_eq!(order, vec![4, 5, 2, 3]);
}

#[test]
fn dispatch_order_excludes_busy_workers() {
    let workers = vec![
        make_remote_worker(0, "A", true),
        make_remote_worker(1, "A", false),
        make_remote_worker(2, "B", true),
        make_remote_worker(3, "B", false),
    ];
    let order = super::lifecycle::dispatch_order(&workers);
    assert_eq!(order, vec![1, 3]);
}

#[test]
fn dispatch_order_empty_workers() {
    let workers: Vec<RemoteWorkerState<TestId>> = vec![];
    let order = super::lifecycle::dispatch_order(&workers);
    assert!(order.is_empty());
}

#[test]
fn dispatch_order_no_idle_workers() {
    let workers = vec![
        make_remote_worker(0, "A", true),
        make_remote_worker(1, "B", true),
    ];
    let order = super::lifecycle::dispatch_order(&workers);
    assert!(order.is_empty());
}

// ── Regression gate: in-process distributed pipeline must queue
// initial staging entries before `run()`. Without them, every task's
// `local_path` arrives at the secondary unstaged and dispatch's
// `report_unresolvable_task` rejects it with "expected StageFile
// notification first". The pair below pins:
//   T1: the failure mode is reachable when staging is omitted.
//   T2: calling `queue_initial_staging_from_binaries` clears it.
//
// Setup is deliberately minimal: 1 binary with a relative `path`
// (so `local_path_is_relative=true` triggers the unresolvable-task
// guard), 1 real secondary with `src_network=None` (so the guard's
// `src_network.is_some()` clause stays false too — the relative-path
// branch does the work), 1 worker (sufficient to dispatch the single
// binary).
//
// We use a real `SecondaryCoordinator` (not a `fake_secondary`) so
// the wire path that produces the regression error string is
// exercised end-to-end on the secondary side, not just simulated.

/// T1 — regression pin. Asserts that without `queue_stage_file` /
/// `queue_initial_staging_from_binaries`, the in-process distributed
/// pipeline's failure mode is reachable: the task lands as `Failed`
/// with the canonical `expected StageFile notification first` error
/// substring.
///
/// Pairs with T2 (same setup, plus the staging call) — together they
/// form the regression gate against re-introducing the gap that
/// caused asm-tokenizer's `--multi-computer single-process` runs to
/// 100%-fail at HEAD `2f30920`.
#[tokio::test(flavor = "current_thread")]
async fn run_without_stage_file_queue_fails_all_tasks() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let secondary_id = "secondary-0".to_string();
        let max_res = dynrunner_core::ResourceMap::from(
            [(dynrunner_core::ResourceKind::memory(), 1024 * 1024 * 1024u64)]
        );

        let (pri_to_sec_tx, sec_to_pri_rx, sec_handle) =
            spawn_real_secondary(secondary_id.clone(), 1, max_res);

        let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
        let mut outgoing = HashMap::new();
        outgoing.insert(secondary_id.clone(), pri_to_sec_tx);
        tokio::task::spawn_local(async move {
            let mut rx = sec_to_pri_rx;
            while let Some(msg) = rx.recv().await {
                if incoming_tx.send(msg).is_err() {
                    break;
                }
            }
        });
        let transport = ChannelSecondaryTransportEnd { outgoing, incoming_rx };

        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: 1,
            connect_timeout: Duration::from_secs(10),
            peer_timeout: Duration::from_secs(10),
            keepalive_interval: Duration::from_millis(50),
            keepalive_miss_threshold: 3,
            source_pre_staged_root: None,
            uses_file_based_items: true,
            max_concurrent_per_type: std::collections::HashMap::new(),
            // `retry_max_passes = 0` so a Recoverable failure becomes
            // permanent on the first pass — the regression we're
            // pinning produces NonRecoverable failures (the unresolvable
            // task guard sends `ErrorType::NonRecoverable`), so the
            // budget is moot, but keeping it at 0 avoids any chance of
            // a retry pass masking the assertion.
            retry_max_passes: 0,
            fleet_dead_timeout: std::time::Duration::from_secs(30),
            mesh_ready_timeout: std::time::Duration::from_secs(5),
            mass_death_grace: std::time::Duration::ZERO,
            mass_death_min_count: 2,
            source_dir: None,
        };

        let mut primary = PrimaryCoordinator::new(
            config,
            transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        // Relative path → wire `local_path` is relative → secondary's
        // `report_unresolvable_task` sees `src_network=None` AND
        // `local_path_is_relative=true` → fires the StageFile-error
        // failure path under test.
        let binaries = vec![make_relative_binary("missing/binary", 50)];

        let (deps, ops, ope) = noop_phase_args();
        primary.run(binaries, deps, ops, ope).await.unwrap();

        // Failure mode reached: 0 completed, 1 permanent failure.
        assert_eq!(
            primary.completed_count(),
            0,
            "no task should complete when staging is omitted"
        );
        assert_eq!(
            primary.failed_count(),
            1,
            "the single task must land in failed_tasks"
        );

        // Pin the canonical error substring so a future refactor that
        // changes the wording surfaces here (a deliberate breakage,
        // not a silent drift). Consumers (asm-tokenizer's e2e check)
        // grep for this string.
        let cs = primary.cluster_state_for_test();
        let mut saw_expected = false;
        for (_hash, state) in cs.tasks_iter() {
            if let crate::cluster_state::TaskState::Failed { last_error, .. } = state {
                assert!(
                    last_error.contains("expected StageFile notification first"),
                    "failed task's last_error must carry the canonical \
                     regression substring; got: {last_error}"
                );
                saw_expected = true;
            }
        }
        assert!(
            saw_expected,
            "cluster_state must record at least one Failed task"
        );

        drop(primary);
        let _ = sec_handle.await;
    }).await;
}

/// T2 — fix validation. Same setup as T1, but `queue_initial_staging_from_binaries`
/// is invoked before `run()` so the secondary receives a StageFile
/// record in its `InitialAssignment.staged_files`. Asserts the task
/// completes (i.e. the lift-to-Rust method is wired correctly and
/// the per-secondary fan-out targets the supplied id).
///
/// Pairs with T1 — together the two pin the regression at `2f30920`.
#[tokio::test(flavor = "current_thread")]
async fn run_with_initial_staging_succeeds() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        // Materialise a real source tree so `compute_file_hash` can
        // succeed: the staging walk reads the file from disk to hash
        // the contents. Single binary keeps the test fast and the
        // assertion surface tight.
        let source_root = std::env::temp_dir().join(format!(
            "stage_init_t2_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let bin_rel = std::path::PathBuf::from("missing/binary");
        let on_disk = source_root.join(&bin_rel);
        std::fs::create_dir_all(on_disk.parent().unwrap()).unwrap();
        std::fs::write(&on_disk, b"t2-staging-payload").unwrap();

        let secondary_id = "secondary-0".to_string();
        let max_res = dynrunner_core::ResourceMap::from(
            [(dynrunner_core::ResourceKind::memory(), 1024 * 1024 * 1024u64)]
        );

        // Secondary needs `src_network` pointing at the source tree
        // so its `stage_file` step can copy the file into the cache —
        // mirrors the real in-process pipeline, where the secondary
        // shares filesystem visibility with the primary. Without
        // `src_network` set the staging copy fails (no source root)
        // and the task still falls through to the unresolvable
        // guard, which would mask the fix.
        let (pri_to_sec_tx, sec_to_pri_rx, sec_handle) =
            spawn_real_secondary_with_src_network(
                secondary_id.clone(),
                1,
                max_res,
                Some(source_root.clone()),
            );

        let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
        let mut outgoing = HashMap::new();
        outgoing.insert(secondary_id.clone(), pri_to_sec_tx);
        tokio::task::spawn_local(async move {
            let mut rx = sec_to_pri_rx;
            while let Some(msg) = rx.recv().await {
                if incoming_tx.send(msg).is_err() {
                    break;
                }
            }
        });
        let transport = ChannelSecondaryTransportEnd { outgoing, incoming_rx };

        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: 1,
            connect_timeout: Duration::from_secs(10),
            peer_timeout: Duration::from_secs(10),
            keepalive_interval: Duration::from_millis(50),
            keepalive_miss_threshold: 3,
            source_pre_staged_root: None,
            uses_file_based_items: true,
            max_concurrent_per_type: std::collections::HashMap::new(),
            retry_max_passes: 0,
            fleet_dead_timeout: std::time::Duration::from_secs(30),
            mesh_ready_timeout: std::time::Duration::from_secs(5),
            mass_death_grace: std::time::Duration::ZERO,
            mass_death_min_count: 2,
            source_dir: None,
        };

        let mut primary = PrimaryCoordinator::new(
            config,
            transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        let binaries = vec![make_relative_binary(
            bin_rel.to_str().unwrap(),
            18, // matches payload length above; size is informational
        )];

        // The fix under test: lift-to-Rust staging walk. The single
        // secondary's id matches `spawn_real_secondary_with_src_network`'s
        // welcome message, so its `pending_stage_files` entry routes
        // correctly through `staged_per_secondary`.
        let secondary_ids = vec![secondary_id.clone()];
        primary
            .queue_initial_staging_from_binaries(
                &binaries,
                &secondary_ids,
                &source_root,
            )
            .expect("staging walk should succeed for a present, readable file");

        let (deps, ops, ope) = noop_phase_args();
        primary.run(binaries, deps, ops, ope).await.unwrap();

        assert_eq!(
            primary.completed_count(),
            1,
            "task should complete when staging is queued"
        );
        assert_eq!(
            primary.failed_count(),
            0,
            "no task should fail when staging is queued"
        );

        drop(primary);
        let _ = sec_handle.await;

        // Best-effort cleanup; `tempdir`-style teardown.
        let _ = std::fs::remove_dir_all(&source_root);
    }).await;
}

/// Stranded-task accounting: a "happy path" run where every binary
/// reaches a terminal completion must report `stranded_count() == 0`.
///
/// Pin: the new counter must not leak a stale residue from a previous
/// run, must be reset at `run()` start, and must agree with
/// `total - completed - failed` on the success arm. Without this
/// guard a refactor that forgot to reset `stranded_count` between
/// runs would silently turn every clean run into a `RunError::ClusterCollapsed`.
#[tokio::test(flavor = "current_thread")]
async fn stranded_count_is_zero_on_clean_run() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let (transport, secondary_ends) = setup_test(1);

        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: 1,
            connect_timeout: Duration::from_secs(5),
            peer_timeout: Duration::from_secs(5),
            keepalive_interval: Duration::from_secs(5),
            keepalive_miss_threshold: 3,
            source_pre_staged_root: None,
            uses_file_based_items: true,
            max_concurrent_per_type: std::collections::HashMap::new(),
            retry_max_passes: 1,
            fleet_dead_timeout: std::time::Duration::from_secs(30),
            mesh_ready_timeout: std::time::Duration::from_secs(5),
            mass_death_grace: std::time::Duration::ZERO,
            mass_death_min_count: 2,
            source_dir: None,
        };

        let mut primary = PrimaryCoordinator::new(
            config,
            transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        let binaries = vec![
            make_binary("a", 50),
            make_binary("b", 60),
            make_binary("c", 70),
        ];

        for (id, rx, tx) in secondary_ends {
            tokio::task::spawn_local(fake_secondary(
                id,
                2,
                1024 * 1024 * 1024,
                rx,
                tx,
            ));
        }

        let (deps, ops, ope) = noop_phase_args();
        primary.run(binaries, deps, ops, ope).await.unwrap();

        assert_eq!(primary.completed_count(), 3);
        assert_eq!(primary.failed_count(), 0);
        assert_eq!(
            primary.stranded_count(),
            0,
            "clean-run stranded must be zero (total - completed - failed)"
        );
    }).await;
}

/// Mid-handshake disconnect helper: the fake sends Welcome + Cert +
/// MeshReady, then immediately drops the channel that lets it talk
/// back to the primary the moment it sees its first inbound message
/// (which in practice will be `PeerInfo`, the next message after
/// the primary completes its half of the handshake). The drop closes
/// the primary's mpsc receiver, surfacing as `recv() -> None` inside
/// the operational loop — i.e. the cluster-collapse failure mode the
/// stranded-tracking patch is designed to detect.
///
/// Co-located with the test that uses it because the shape ("die
/// after handshake, before the run loop can dispatch a single task")
/// is specific to the cluster-collapse regression and not general
/// enough to merit promotion to `test_helpers.rs`.
async fn fake_secondary_dies_post_mesh_ready(
    secondary_id: String,
    num_workers: u32,
    ram_bytes: u64,
    mut incoming_from_primary: tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
    outgoing_to_primary: tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
) {
    outgoing_to_primary
        .send(DistributedMessage::SecondaryWelcome {
            sender_id: secondary_id.clone(),
            timestamp: 0.0,
            secondary_id: secondary_id.clone(),
            resources: vec![dynrunner_core::ResourceAmount {
                kind: dynrunner_core::ResourceKind::memory(),
                amount: ram_bytes,
            }],
            worker_count: num_workers,
            hostname: "test-host".into(),
        })
        .unwrap();
    outgoing_to_primary
        .send(DistributedMessage::CertExchange {
            sender_id: secondary_id.clone(),
            timestamp: 0.0,
            secondary_id: secondary_id.clone(),
            public_cert_pem: "FAKE_CERT".into(),
            ipv4_address: Some("127.0.0.1".into()),
            ipv6_address: None,
            quic_port: 5000,
        })
        .unwrap();
    outgoing_to_primary
        .send(DistributedMessage::MeshReady {
            sender_id: secondary_id.clone(),
            timestamp: 0.0,
            secondary_id: secondary_id.clone(),
            peer_count: 0,
        })
        .unwrap();

    // Wait for any one inbound message — at this point the
    // handshake on the primary side is past the wait_for_connections
    // phase and we're either in peer_setup or initial_assignment.
    // Drop the outbound channel by letting it go out of scope so the
    // primary's `recv()` returns `None` once every fake has dropped
    // its clone. Any further inbound messages are simply discarded
    // by closing the receiver.
    let _ = incoming_from_primary.recv().await;
    drop(outgoing_to_primary);
}

/// Thread-local tracing buffer: captures every event emitted on the
/// current thread for the lifetime of the returned guard. Used by
/// the cluster-collapse test to pin the diagnostic log line without
/// touching the process-global subscriber that other tests in this
/// binary set via `tracing_subscriber::fmt::try_init`.
///
/// `current_thread` tokio flavour + `LocalSet` keep every spawned
/// fake-secondary on the same thread as the test future, so a
/// `set_default()` thread-local subscriber is reached by every
/// `tracing::error!` site that the `run()` flow hits.
fn capture_logs_thread_local() -> (
    std::sync::Arc<std::sync::Mutex<Vec<u8>>>,
    tracing::dispatcher::DefaultGuard,
) {
    use std::sync::{Arc, Mutex};
    #[derive(Clone)]
    struct SharedWriter(Arc<Mutex<Vec<u8>>>);
    impl std::io::Write for SharedWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for SharedWriter {
        type Writer = SharedWriter;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    let buf = Arc::new(Mutex::new(Vec::new()));
    let writer = SharedWriter(buf.clone());
    let subscriber = tracing_subscriber::fmt()
        .with_writer(writer)
        .with_max_level(tracing::Level::ERROR)
        .with_ansi(false)
        .finish();
    let guard = tracing::subscriber::set_default(subscriber);
    (buf, guard)
}

/// T-stranded-on-cluster-collapse: when the secondaries die fatally
/// after handshake but before any task is dispatched, `run()` must
/// return `RunError::ClusterCollapsed` carrying the per-category
/// counts; the post-call accounting must satisfy
/// `completed + failed + stranded == total`; and the diagnostic log
/// line must fire so consumers grepping for "tasks left unassigned
/// because cluster routing collapsed" see it on every collapse.
///
/// Pre-fix: `run()` returned `Ok(())` with completed=0 / failed=0 /
/// total=N, hiding the `total - 0 - 0 = N` un-dispatched tasks; CI
/// scripts checking exit code saw green when the run had collapsed.
#[tokio::test(flavor = "current_thread")]
async fn stranded_on_cluster_collapse_returns_err_with_counts() {
    // Install the thread-local log capture before any awaits so the
    // diagnostic emitted from inside `primary.run().await` is recorded.
    // The guard scopes the subscriber to the current thread for as
    // long as it lives — dropped at the end of the test, leaving the
    // process-global subscriber (if any) untouched.
    let (log_buf, _log_guard) = capture_logs_thread_local();

    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let (transport, secondary_ends) = setup_test(2);

        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: 2,
            connect_timeout: Duration::from_secs(5),
            peer_timeout: Duration::from_secs(5),
            keepalive_interval: Duration::from_secs(5),
            keepalive_miss_threshold: 3,
            source_pre_staged_root: None,
            uses_file_based_items: true,
            max_concurrent_per_type: std::collections::HashMap::new(),
            retry_max_passes: 1,
            // Long fleet_dead so the operational loop's exit happens
            // via "transport closed" (recv → None), not via the
            // fleet-dead timer push-to-failed path. Keeps this test
            // focused on the stranded-on-recv-None arm; a separate
            // future test could pin the fleet-dead arm independently.
            fleet_dead_timeout: std::time::Duration::from_secs(600),
            mesh_ready_timeout: std::time::Duration::from_secs(5),
            mass_death_grace: std::time::Duration::ZERO,
            mass_death_min_count: 2,
            source_dir: None,
        };

        let mut primary = PrimaryCoordinator::new(
            config,
            transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        let binaries: Vec<TaskInfo<TestId>> = (0..6)
            .map(|i| make_binary(&format!("bin_{i}"), 50 + i * 10))
            .collect();
        let total = binaries.len();

        for (id, rx, tx) in secondary_ends {
            tokio::task::spawn_local(fake_secondary_dies_post_mesh_ready(
                id,
                /* num_workers = */ 1,
                1024 * 1024 * 1024,
                rx,
                tx,
            ));
        }

        let (deps, ops, ope) = noop_phase_args();
        let outcome = primary.run(binaries, deps, ops, ope).await;

        match outcome {
            Err(RunError::ClusterCollapsed { stranded, completed, failed }) => {
                assert!(stranded > 0, "stranded must be positive on cluster collapse");
                assert_eq!(
                    completed + failed + stranded,
                    total,
                    "completed + failed + stranded must equal total"
                );
                assert_eq!(completed, primary.completed_count());
                assert_eq!(failed, primary.failed_count());
                assert_eq!(stranded, primary.stranded_count());
            }
            other => panic!(
                "expected RunError::ClusterCollapsed, got {other:?} (counters: \
                 completed={} failed={} stranded={} total={})",
                primary.completed_count(),
                primary.failed_count(),
                primary.stranded_count(),
                total,
            ),
        }

        // Diagnostic log line must have fired so consumers grepping
        // for the substring see it. Log emission happens inside
        // `PrimaryCoordinator::run`, which is awaited directly in
        // the test scope — the thread-local subscriber installed by
        // `capture_logs_thread_local` records every error-level event
        // from the same thread.
        let captured = String::from_utf8_lossy(&log_buf.lock().unwrap()).into_owned();
        assert!(
            captured.contains("tasks left unassigned because cluster routing collapsed"),
            "diagnostic 'tasks left unassigned because cluster routing collapsed' must \
             fire on the cluster-collapse arm so ops scripts can detect it; captured \
             error-level logs:\n{captured}"
        );
    }).await;
}

/// Pin Fix-#21 contract: when the operational loop's
/// `fleet_dead_timeout` arm fires with queued tasks in the pool, the
/// drained binaries must be classified as `stranded` — not `failed`.
/// They were never dispatched, no secondary attempted them, no worker
/// reported a failure, so the only honest category is "couldn't be
/// tried". Pre-fix the arm pushed each pending task's hash into
/// `failed_tasks`, conflating worker-reported failure with
/// never-dispatched and (worse) burning the retry budget on tasks
/// that hadn't actually failed.
///
/// We drive the operational loop directly (bypassing `run()`'s setup
/// phases) so the fleet-dead arm fires from a primed state: empty
/// secondaries map + non-empty pool + tight timeout. This isolates
/// the pre/post fix semantic delta to a single observable assertion
/// (`failed_tasks.is_empty() && pool drained`) — pre-fix the arm
/// would land every drained binary in `failed_tasks`; post-fix the
/// arm leaves `failed_tasks` empty and the binaries flow into the
/// run-level `stranded` category by way of the `total - completed -
/// failed` accounting in `run()`.
#[tokio::test(flavor = "current_thread")]
async fn fleet_dead_timeout_pending_become_stranded_not_failed() {
    use dynrunner_scheduler_api::PendingPool;

    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let (transport, _ends) = setup_test(0);
        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: 0,
            connect_timeout: Duration::from_secs(5),
            peer_timeout: Duration::from_secs(5),
            keepalive_interval: Duration::from_secs(60),
            keepalive_miss_threshold: 3,
            source_pre_staged_root: None,
            uses_file_based_items: true,
            max_concurrent_per_type: std::collections::HashMap::new(),
            retry_max_passes: 0,
            // Zero timeout so the very first loop iteration's
            // `elapsed >= fleet_dead_timeout` predicate trips, no
            // wall-clock wait needed in the test.
            fleet_dead_timeout: std::time::Duration::ZERO,
            mesh_ready_timeout: std::time::Duration::from_secs(5),
            mass_death_grace: std::time::Duration::ZERO,
            mass_death_min_count: 2,
            source_dir: None,
        };
        let mut primary: PrimaryCoordinator<_, _, _, TestId> = PrimaryCoordinator::new(
            config,
            transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        // Prime: pool with three queued binaries, empty secondaries
        // map (the fleet-dead predicate is `secondaries.is_empty() &&
        // !pool.is_empty()`), `total_tasks` set so the run-level
        // accounting can later compute `stranded = total -
        // completed - failed`.
        let phase = dynrunner_core::PhaseId::from("default");
        let mut pool = PendingPool::<TestId>::new(
            [phase.clone()],
            std::collections::HashMap::new(),
        )
        .expect("default-phase pool");
        let binaries: Vec<TaskInfo<TestId>> = (0..3)
            .map(|i| make_binary(&format!("bin_{i}"), 50 + i * 10))
            .collect();
        pool.extend(binaries.clone()).expect("valid extend");
        primary.pending = Some(pool);
        primary.phase_completed.insert(phase.clone(), 0);
        primary.phase_failed.insert(phase, 0);
        primary.all_binaries = binaries.clone();
        primary.total_tasks = binaries.len();

        // No workers, no secondaries — fleet-dead arm fires
        // immediately on entry to the operational loop.
        primary
            .operational_loop()
            .await
            .expect("operational_loop must return Ok on the fleet-dead exit path");

        // Pool must be drained so the loop terminates (both pre and
        // post fix).
        assert!(
            primary.pool().is_empty(),
            "fleet-dead arm must drain the queued pool"
        );
        // Fix-#21 contract: pre-fix this set was populated with the
        // drained binaries' hashes; post-fix it stays empty so the
        // `total - completed - failed` accounting downstream
        // classifies them as stranded.
        assert!(
            primary.failed_tasks.is_empty(),
            "fleet-dead pending must NOT be classified as failed; pre-fix \
             arm pushed pending hashes into failed_tasks, conflating \
             never-dispatched with worker-reported failure (got {:?})",
            primary.failed_tasks
        );
        assert!(
            primary.completed_tasks.is_empty(),
            "fleet-dead with un-dispatched tasks must report no completions"
        );

        // Drive the run-level accounting that `run()` would do post-
        // operational-loop, end-to-end-equivalent. With failed and
        // completed both empty, every binary lands in the stranded
        // bucket — exactly the category Fix-#21 surfaces.
        let total = primary.total_tasks;
        let completed = primary.completed_tasks.len();
        let failed = primary.failed_tasks.len();
        let stranded = total.saturating_sub(completed + failed);
        assert_eq!(
            stranded, total,
            "every un-dispatched binary must surface as stranded \
             (completed={completed} failed={failed} total={total})"
        );
    })
    .await;
}

/// Pin: `drain_pending_messages` processes any `TaskComplete` /
/// `TaskFailed` messages still queued in the inbound transport when
/// it's invoked, updating `completed_tasks` / `failed_tasks` exactly
/// as the operational loop's `recv → dispatch_message` pipeline does.
/// This is the helper the post-loop drain step in `run()` calls before
/// computing the stranded count, closing the window where the
/// pre-fix accounting saw pre-drain counters and false-positived clean
/// runs into `RunError::ClusterCollapsed` (counted successful
/// completions as `stranded`).
///
/// Construction: we drive the helper directly (no `run()` lifecycle)
/// by pre-loading TaskComplete messages into the shared incoming
/// channel and asserting the helper drains them. The drain helper
/// reuses `dispatch_message`, which calls `handle_task_complete`,
/// which inserts into `completed_tasks` regardless of whether a
/// matching worker exists — so we don't have to plumb a fake worker
/// to exercise the counter update.
#[tokio::test(flavor = "current_thread")]
async fn drain_pending_messages_updates_completed_set() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let (transport, secondary_ends) = setup_test(1);
        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: 1,
            connect_timeout: Duration::from_secs(5),
            peer_timeout: Duration::from_secs(5),
            keepalive_interval: Duration::from_secs(5),
            keepalive_miss_threshold: 3,
            source_pre_staged_root: None,
            uses_file_based_items: true,
            max_concurrent_per_type: std::collections::HashMap::new(),
            retry_max_passes: 1,
            fleet_dead_timeout: std::time::Duration::from_secs(30),
            mesh_ready_timeout: std::time::Duration::from_secs(5),
            mass_death_grace: std::time::Duration::ZERO,
            mass_death_min_count: 2,
            source_dir: None,
        };
        let mut primary: PrimaryCoordinator<_, _, _, TestId> = PrimaryCoordinator::new(
            config,
            transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        // Inject three TaskComplete messages from the fake secondary's
        // outbound clone (which is what `secondary_ends[i].2` is — the
        // shared inbound side from the primary's perspective). Closing
        // the sender at the end ensures `transport.recv()` will yield
        // `Some` for each queued message and then `None`, exercising
        // the drain helper's "process until empty" path through both
        // arms of the recv result.
        let (sec_id, _to_sec_rx, incoming_tx) = secondary_ends.into_iter().next().unwrap();
        for hash in ["hash-a", "hash-b", "hash-c"] {
            incoming_tx
                .send(DistributedMessage::TaskComplete {
                    sender_id: sec_id.clone(),
                    timestamp: 0.0,
                    secondary_id: sec_id.clone(),
                    worker_id: 0,
                    task_hash: hash.into(),
                    result_data: None,
                })
                .unwrap();
        }
        // Drop the sender so the recv channel will eventually yield
        // `None`. The drain helper should treat that as "transport
        // closed → drain complete" and break.
        drop(incoming_tx);

        primary
            .drain_pending_messages(Duration::from_millis(500))
            .await
            .expect("drain must succeed on healthy transport");

        assert_eq!(
            primary.completed_count(),
            3,
            "drain must have processed all three queued TaskComplete messages"
        );
        for hash in ["hash-a", "hash-b", "hash-c"] {
            assert!(
                primary.completed_tasks.contains(hash),
                "completed_tasks must contain {hash} after drain"
            );
        }
    })
    .await;
}

/// Pin Fix-#23 contract end-to-end: a happy-path run with multiple
/// secondaries where every dispatched task succeeds must not surface
/// any task as `stranded`. Pre-fix the accounting in `run()` ran
/// before any post-loop drain of in-flight TaskComplete messages,
/// flipping clean runs into `RunError::ClusterCollapsed`. Post-fix
/// the drain step processes whatever was still in transit before the
/// stranded computation, so completed runs report
/// `completed=N / failed=0 / stranded=0` and `run()` returns `Ok(())`.
///
/// Two secondaries here (vs the N=1 fixture in
/// `stranded_count_is_zero_on_clean_run`) widens the surface to the
/// multi-secondary code paths — completion-forwarding, per-secondary
/// worker bookkeeping — that the e2e scenario in #19 surfaced.
#[tokio::test(flavor = "current_thread")]
async fn clean_run_does_not_false_positive_stranded() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let (transport, secondary_ends) = setup_test(2);

        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: 2,
            connect_timeout: Duration::from_secs(5),
            peer_timeout: Duration::from_secs(5),
            keepalive_interval: Duration::from_secs(5),
            keepalive_miss_threshold: 3,
            source_pre_staged_root: None,
            uses_file_based_items: true,
            max_concurrent_per_type: std::collections::HashMap::new(),
            retry_max_passes: 1,
            fleet_dead_timeout: std::time::Duration::from_secs(30),
            mesh_ready_timeout: std::time::Duration::from_secs(5),
            mass_death_grace: std::time::Duration::ZERO,
            mass_death_min_count: 2,
            source_dir: None,
        };

        let mut primary = PrimaryCoordinator::new(
            config,
            transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        let binaries: Vec<TaskInfo<TestId>> = (0..4)
            .map(|i| make_binary(&format!("bin_{i}"), 50 + i * 10))
            .collect();
        let total = binaries.len();

        for (id, rx, tx) in secondary_ends {
            tokio::task::spawn_local(fake_secondary(
                id,
                /* num_workers = */ 2,
                1024 * 1024 * 1024,
                rx,
                tx,
            ));
        }

        let (deps, ops, ope) = noop_phase_args();
        primary
            .run(binaries, deps, ops, ope)
            .await
            .expect("clean multi-secondary run must return Ok");

        assert_eq!(
            primary.completed_count(),
            total,
            "every binary must report completed on a clean run"
        );
        assert_eq!(
            primary.failed_count(),
            0,
            "no binary should land in failed on a clean run"
        );
        assert_eq!(
            primary.stranded_count(),
            0,
            "no binary should be stranded on a clean run — \
             pre-fix the accounting ran before pending TaskCompletes \
             drained, false-positiving successful tasks as stranded"
        );
    })
    .await;
}

// ── Demoted-primary ClusterMutation arm: regression gate against the
// asm-dataset-nix R2 / T3 1200s hang.
//
// Pre-fix the primary-side `dispatch_message` had no arm for
// `MessageType::ClusterMutation` — every ClusterMutation broadcast
// addressed at the demoted local primary fell through the catch-all,
// leaving its replicated `cluster_state` mirror frozen at the
// pre-promotion view and the per-task accounting (`completed_tasks` /
// `failed_tasks`, the two sets the operational loop's exit-counter
// check reads) blind to cross-secondary completions on the new primary's
// pool. The loop sat forever; the local-primary process never exited;
// the asm-dataset-nix e2e harness killed it at the 1200s deadline.
//
// The three tests below pin:
//   T-A: a synthetic `ClusterMutation::TaskCompleted` arriving via
//        `dispatch_message` lands in `completed_tasks` (the unit
//        contract — without this `completed + failed >= total` cannot
//        trip on a demoted primary).
//   T-B: an end-to-end run where the local primary is demoted and the
//        promoted secondary's RunComplete signal must land on the
//        demoted primary's `cluster_state.run_complete()` and break
//        the operational loop within bounded wait — same window as the
//        existing 500ms RunComplete settle in `run()`.
//   T-C: an explicit ClusterMutation::RunComplete delivered via the
//        demoted primary's transport must drive the same exit cleanly
//        even when no task accounting is in play (the
//        `cluster_state.run_complete()` exit fires standalone).

/// T-A — unit contract. Drive `dispatch_message` directly with a
/// synthesized `DistributedMessage::ClusterMutation` carrying a
/// `TaskCompleted` mutation; assert `completed_tasks` grows. Failed
/// pre-fix because the dispatch-message catch-all silently dropped
/// every ClusterMutation arrival on the primary side; succeeds post-fix
/// because the new arm threads the mutation through both the local
/// `cluster_state` mirror and the accounting sets the operational
/// loop's exit-counter check reads.
#[tokio::test(flavor = "current_thread")]
async fn demoted_primary_applies_cluster_mutation_taskcompleted() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let (transport, _ends) = setup_test(1);
        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: 1,
            connect_timeout: Duration::from_secs(5),
            peer_timeout: Duration::from_secs(5),
            keepalive_interval: Duration::from_secs(5),
            keepalive_miss_threshold: 3,
            source_pre_staged_root: None,
            uses_file_based_items: true,
            max_concurrent_per_type: std::collections::HashMap::new(),
            retry_max_passes: 1,
            fleet_dead_timeout: std::time::Duration::from_secs(30),
            mesh_ready_timeout: std::time::Duration::from_secs(5),
            mass_death_grace: std::time::Duration::ZERO,
            mass_death_min_count: 2,
            source_dir: None,
        };
        let mut primary: PrimaryCoordinator<_, _, _, TestId> = PrimaryCoordinator::new(
            config,
            transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        // Pre-state: empty completed_tasks. Post-fix the
        // ClusterMutation arm grows it from any TaskCompleted /
        // TaskFailed mutation, regardless of whether the hash also
        // appears in cluster_state's CRDT (which has its own
        // happens-before constraint requiring TaskAdded first — that
        // path is exercised by the e2e tests, not this unit one).
        // The accounting sets are the load-bearing surface for the
        // operational loop's exit-counter check, so they're what we
        // pin here.
        assert!(primary.completed_tasks.is_empty());
        assert!(primary.failed_tasks.is_empty());

        // Seed cluster_state with TaskAdded so the subsequent
        // TaskCompleted apply isn't a NoOp (the CRDT requires the
        // entry to exist before transitioning state). Without the
        // seed the cluster_state assertion below would be unreachable
        // even on a correct fix.
        let bin = make_binary("demoted-arm-task", 100);
        let hash = super::wire::compute_task_hash(&bin);
        let seed_msg = DistributedMessage::ClusterMutation {
            sender_id: "sec-promoted".into(),
            timestamp: 0.0,
            mutations: vec![
                dynrunner_protocol_primary_secondary::ClusterMutation::<TestId>::TaskAdded {
                    hash: hash.clone(),
                    task: bin,
                },
            ],
        };
        primary
            .dispatch_message(seed_msg)
            .await
            .expect("seed TaskAdded must dispatch");

        let msg = DistributedMessage::ClusterMutation {
            sender_id: "sec-promoted".into(),
            timestamp: 0.0,
            mutations: vec![dynrunner_protocol_primary_secondary::ClusterMutation::<
                TestId,
            >::TaskCompleted {
                hash: hash.clone(),
            }],
        };
        primary
            .dispatch_message(msg)
            .await
            .expect("dispatch_message must accept a ClusterMutation");

        assert!(
            primary.completed_tasks.contains(&hash),
            "ClusterMutation::TaskCompleted must mirror into completed_tasks; \
             without this the demoted primary's `completed + failed >= total` \
             exit-counter check never trips on cross-secondary completions"
        );

        // The cluster_state mirror also reflects the mutation — the
        // CRDT lattice is the source of truth for the primary's view
        // of the run, even post-demotion. Verifies the apply is on
        // the same code path the secondary's
        // `apply_cluster_mutations` uses.
        let cs_counts = primary.cluster_state_for_test().counts();
        assert_eq!(
            cs_counts.completed, 1,
            "cluster_state must record 1 Completed entry after the mutation"
        );
    }).await;
}

/// T-B — end-to-end. A demoted primary plus a real secondary (acting
/// as the promoted primary) drive the run; the secondary fires
/// `ClusterMutation::RunComplete` once its primary view drains, and
/// the demoted primary's operational loop must observe the signal and
/// exit. The wait is bounded by the timeout below — pre-fix the run
/// never returns and the test would hang until killed by the harness;
/// post-fix the wait closes well within 1s in-process.
///
/// We don't drive a full failover sequence (PromotePrimary handshake,
/// election, etc.) — that surface is covered by the existing failover
/// tests. Here the contract under test is narrower: assuming a
/// promoted secondary has emitted the RunComplete signal AND the
/// signal lands on the demoted primary's transport, does the demoted
/// primary's loop break? We construct that exact wire shape via the
/// single-secondary primary fixture and the secondary's existing
/// "promoted primary done; broadcasting RunComplete" path
/// (processing.rs).
///
/// `demoted=true` is forced via `promote_primary` before `run()` so
/// the operational loop runs in observer mode — exactly what
/// asm-dataset-nix's R2 trace reports for the local primary.
#[tokio::test(flavor = "current_thread")]
async fn demoted_primary_exits_on_run_complete_broadcast() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        // setup_test(1) yields a primary-side transport plus one
        // secondary "end" (id, primary→sec rx, sec→primary tx).
        // The sec→primary tx is the channel we use to deliver
        // synthetic wire messages — exactly the shape a promoted
        // secondary's loopback would produce on the demoted
        // primary's transport.
        let (transport, secondary_ends) = setup_test(1);
        let (_sec_id, _to_sec_rx, incoming_tx) =
            secondary_ends.into_iter().next().unwrap();

        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: 1,
            connect_timeout: Duration::from_secs(5),
            peer_timeout: Duration::from_secs(5),
            keepalive_interval: Duration::from_millis(100),
            keepalive_miss_threshold: 3,
            source_pre_staged_root: None,
            uses_file_based_items: true,
            max_concurrent_per_type: std::collections::HashMap::new(),
            retry_max_passes: 1,
            fleet_dead_timeout: std::time::Duration::from_secs(30),
            mesh_ready_timeout: std::time::Duration::from_secs(5),
            mass_death_grace: std::time::Duration::ZERO,
            mass_death_min_count: 2,
            source_dir: None,
        };
        let mut primary: PrimaryCoordinator<_, _, _, TestId> = PrimaryCoordinator::new(
            config,
            transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        // Bypass `run()`: drive `operational_loop` in isolation with a
        // pre-loaded ClusterMutation::RunComplete arriving on the
        // transport. Same wire shape the promoted secondary's
        // `processing.rs` produces when its primary view drains.
        let phase = dynrunner_core::PhaseId::from("default");
        let pool = dynrunner_scheduler_api::PendingPool::<TestId>::new(
            [phase.clone()],
            std::collections::HashMap::new(),
        )
        .expect("default-phase pool");
        primary.pending = Some(pool);
        primary.phase_completed.insert(phase.clone(), 0);
        primary.phase_failed.insert(phase, 0);
        // total_tasks = 1 with no completion mirrors the asm-dataset-nix
        // R2 starvation: the counter check `completed + failed >=
        // total` is unreachable from this state, so only the
        // RunComplete-driven exit can break the loop. Pre-fix this
        // test would hang inside `operational_loop` indefinitely.
        primary.total_tasks = 1;
        primary.demoted = true;

        // Inject the RunComplete signal on the transport. The recv
        // tick inside operational_loop must dispatch it, the new
        // ClusterMutation arm must apply it, and the new run_complete
        // exit must break the loop.
        incoming_tx
            .send(DistributedMessage::ClusterMutation {
                sender_id: "sec-promoted".into(),
                timestamp: 0.0,
                mutations: vec![
                    dynrunner_protocol_primary_secondary::ClusterMutation::<TestId>::RunComplete,
                ],
            })
            .unwrap();
        // Drop the sender so the loop's recv yields `None` after the
        // queued message, exercising the transport-closed branch as a
        // hard backstop. The post-fix exit MUST come from the
        // run_complete check (cluster_state.run_complete() == true),
        // not from the transport-closed break — assert below
        // distinguishes the two paths.
        drop(incoming_tx);

        // Bounded wait: pre-fix the loop was unbounded. Post-fix the
        // mutation arrives in <1ms, the apply is synchronous, and the
        // next loop iteration's run_complete check breaks. 5s ceiling
        // for CI flake tolerance.
        let exit = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            primary.operational_loop(),
        )
        .await;

        match exit {
            Ok(Ok(())) => {
                assert!(
                    primary.cluster_state_for_test().run_complete(),
                    "cluster_state must record run_complete after the mutation; \
                     if this fails the loop exited via the transport-closed \
                     fallback, not the run_complete check under test"
                );
            }
            Ok(Err(e)) => panic!("operational_loop returned Err on RunComplete: {e}"),
            Err(_) => panic!(
                "operational_loop did not exit within 5s — the demoted \
                 primary's RunComplete-driven exit is broken (pre-fix \
                 hang regression)"
            ),
        }
    }).await;
}

/// T-C — end-to-end happy path. A demoted primary + 2 fake secondaries,
/// where one is the promoted primary draining its replicated pool. Pre-
/// fix the local primary's operational loop sat forever waiting for a
/// counter tick that never came; post-fix the RunComplete signal
/// (delivered via the new primary_transport.send loopback in
/// secondary/processing.rs) lands on the demoted primary's transport,
/// the new ClusterMutation arm applies it, and the run_complete exit
/// closes the loop within bounded wait.
///
/// This wires the same delivery path asm-dataset-nix R2 / T3 exercises
/// in production: the new primary's `processing.rs` RunComplete site
/// fanning out to peers AND back to the demoted primary's transport.
/// Without the primary_transport.send addition this test would still
/// hang post-fix.
#[tokio::test(flavor = "current_thread")]
async fn demoted_primary_exits_on_clean_completion() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let (transport, secondary_ends) = setup_test(1);
        let (_sec_id, _to_sec_rx, incoming_tx) =
            secondary_ends.into_iter().next().unwrap();

        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: 1,
            connect_timeout: Duration::from_secs(5),
            peer_timeout: Duration::from_secs(5),
            keepalive_interval: Duration::from_millis(50),
            keepalive_miss_threshold: 3,
            source_pre_staged_root: None,
            uses_file_based_items: true,
            max_concurrent_per_type: std::collections::HashMap::new(),
            retry_max_passes: 1,
            fleet_dead_timeout: std::time::Duration::from_secs(30),
            mesh_ready_timeout: std::time::Duration::from_secs(5),
            mass_death_grace: std::time::Duration::ZERO,
            mass_death_min_count: 2,
            source_dir: None,
        };
        let mut primary: PrimaryCoordinator<_, _, _, TestId> = PrimaryCoordinator::new(
            config,
            transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        // Pre-state: pool with no items, two pre-mirrored completions,
        // total_tasks set to a value the counter check cannot reach
        // from the existing completions alone — so only the
        // run_complete-driven exit can break the loop. demoted=true
        // puts the loop in observer mode (matches asm-dataset-nix R2:
        // local primary already handed off authority to the promoted
        // secondary).
        let phase = dynrunner_core::PhaseId::from("default");
        let pool = dynrunner_scheduler_api::PendingPool::<TestId>::new(
            [phase.clone()],
            std::collections::HashMap::new(),
        )
        .expect("default-phase pool");
        primary.pending = Some(pool);
        primary.phase_completed.insert(phase.clone(), 0);
        primary.phase_failed.insert(phase, 0);
        primary.total_tasks = 3; // counter-check unreachable
        primary.completed_tasks.insert("h-already-done-1".into());
        primary.completed_tasks.insert("h-already-done-2".into());
        primary.demoted = true;

        // Inject the ClusterMutation::RunComplete on the transport
        // exactly the way the new primary's
        // `processing.rs::primary_transport.send` loopback delivers it
        // post-fix. Pre-fix this delivery path doesn't exist (the
        // RunComplete only went out via peer_transport, which the
        // demoted primary isn't on); even with delivery, pre-fix
        // there's no `MessageType::ClusterMutation` arm to consume it.
        incoming_tx
            .send(DistributedMessage::ClusterMutation {
                sender_id: "sec-promoted".into(),
                timestamp: 0.0,
                mutations: vec![
                    dynrunner_protocol_primary_secondary::ClusterMutation::<TestId>::RunComplete,
                ],
            })
            .unwrap();
        // Hold the sender open: the loop's run_complete exit must fire
        // on its OWN, not via the transport-closed fallback. Asserting
        // on `cluster_state.run_complete()` after the loop returns
        // distinguishes the two paths.
        let _hold = incoming_tx;

        // Bounded wait. Pre-fix the loop was unbounded — the
        // asm-dataset-nix harness killed the local primary at 1200s.
        // Post-fix the run_complete check fires within one heartbeat
        // tick of the mutation arriving (50ms keepalive_interval here
        // means at most ~100ms before the next select! cycle picks up
        // the message). 5s ceiling for CI flake tolerance.
        let exit = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            primary.operational_loop(),
        )
        .await;

        match exit {
            Ok(Ok(())) => {
                assert!(
                    primary.cluster_state_for_test().run_complete(),
                    "cluster_state.run_complete() must be set after the \
                     RunComplete-driven exit fired (distinguishes from a \
                     stale transport-closed break)"
                );
            }
            Ok(Err(e)) => panic!("operational_loop returned Err: {e}"),
            Err(_) => panic!(
                "operational_loop did not exit within 5s on a clean \
                 RunComplete signal — the demoted primary's exit path \
                 is broken (asm-dataset-nix R2 / T3 1200s hang \
                 regression)"
            ),
        }
    }).await;
}

/// T-#33: initial assignment is round-robin across secondaries AND
/// secondary iteration order is deterministic (sorted by name).
///
/// Setup: 3 secondaries × 1 worker × 3 binaries. With contiguous-
/// per-secondary order (pre-fix) the assignment was still
/// one-per-secondary in this exact-fit case, but the SECONDARY-ID
/// ORDER of which-secondary-got-which-binary was HashMap-random.
/// Post-fix the binaries land in sec-0, sec-1, sec-2 order.
///
/// More important regression case: tasks ≪ total_workers. With
/// pre-fix (contiguous), 3 secondaries × 2 workers × 3 tasks would
/// have given the first secondary 2 tasks and one other secondary
/// 1 task — the third got nothing. Post-fix all three each receive
/// exactly 1. We exercise that exact case here to pin the actual
/// behaviour change, not just the determinism gain.
#[tokio::test(flavor = "current_thread")]
async fn initial_assignment_is_round_robin_and_name_sorted() {
    use std::sync::Arc;
    use std::sync::Mutex;

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, secondary_ends) = setup_test(3);

            let config = PrimaryConfig {
                node_id: "primary".into(),
                num_secondaries: 3,
                connect_timeout: Duration::from_secs(5),
                peer_timeout: Duration::from_secs(5),
                keepalive_interval: Duration::from_secs(5),
                keepalive_miss_threshold: 3,
                source_pre_staged_root: None,
                uses_file_based_items: true,
                max_concurrent_per_type: std::collections::HashMap::new(),
                retry_max_passes: 1,
                fleet_dead_timeout: std::time::Duration::from_secs(30),
                mesh_ready_timeout: std::time::Duration::from_secs(5),
                mass_death_grace: std::time::Duration::ZERO,
                mass_death_min_count: 2,
                source_dir: None,
            };

            let mut primary = PrimaryCoordinator::new(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // 3 tasks, 3 secondaries × 2 workers = 6 worker slots.
            // The pre-fix contiguous-per-secondary order would have
            // given two secondaries all 3 tasks and one secondary 0.
            // Post-fix every secondary gets exactly 1.
            let binaries = vec![
                make_binary("a", 50),
                make_binary("b", 50),
                make_binary("c", 50),
            ];

            // Per-secondary initial-assignment count, captured by
            // intercepting each secondary's primary→secondary channel.
            // Forwarder counts InitialAssignment binaries before
            // re-forwarding every message to the real fake-secondary,
            // so the lifecycle still completes via TaskComplete +
            // TaskRequest cycles.
            let counts: Arc<Mutex<std::collections::BTreeMap<String, usize>>> =
                Arc::new(Mutex::new(std::collections::BTreeMap::new()));

            for (id, sec_inbound, sec_outbound) in secondary_ends {
                let (inner_tx, inner_rx) = tokio_mpsc::unbounded_channel();
                let counts_for_secondary = Arc::clone(&counts);
                let id_for_forwarder = id.clone();
                tokio::task::spawn_local(async move {
                    let mut rx = sec_inbound;
                    while let Some(msg) = rx.recv().await {
                        if let DistributedMessage::InitialAssignment {
                            zip_files, ..
                        } = &msg
                        {
                            let n: usize =
                                zip_files.iter().map(|zf| zf.binaries.len()).sum();
                            counts_for_secondary
                                .lock()
                                .unwrap()
                                .insert(id_for_forwarder.clone(), n);
                        }
                        if inner_tx.send(msg).is_err() {
                            break;
                        }
                    }
                });

                tokio::task::spawn_local(fake_secondary(
                    id,
                    2,
                    1024 * 1024 * 1024,
                    inner_rx,
                    sec_outbound,
                ));
            }

            let (deps, ops, ope) = noop_phase_args();
            primary.run(binaries, deps, ops, ope).await.unwrap();

            assert_eq!(primary.completed_count(), 3);
            assert_eq!(primary.failed_count(), 0);

            // Each of the 3 secondaries must have received exactly 1
            // binary in its InitialAssignment. Pre-fix the
            // contiguous-per-secondary layout produced something like
            // {sec-X: 2, sec-Y: 1, sec-Z: 0} where X/Y/Z were
            // HashMap-random; the secondary that got 0 then had to
            // wait for the operational TaskRequest cycle to receive
            // any work at all.
            let final_counts = counts.lock().unwrap().clone();
            assert_eq!(
                final_counts.len(),
                3,
                "every secondary must receive an InitialAssignment \
                 (even an empty one) so wait_for_setup unblocks; \
                 captured: {:?}",
                final_counts
            );
            for sid in &["sec-0", "sec-1", "sec-2"] {
                let n = final_counts
                    .get(*sid)
                    .copied()
                    .expect("expected secondary missing from captured InitialAssignment");
                assert_eq!(
                    n, 1,
                    "{sid} expected exactly 1 initial-assignment binary, \
                     got {n}. Pre-fix this would fail because contiguous-\
                     per-secondary ordering plus HashMap-random iteration \
                     order gave 2 tasks to one secondary and 0 to another. \
                     Captured: {:?}",
                    final_counts
                );
            }
        })
        .await;
}
