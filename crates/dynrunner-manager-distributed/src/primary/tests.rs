//! Tests for the primary coordinator. Fixtures live in
//! `super::test_helpers`; this file holds the test scenarios.

use super::test_helpers::{
    fake_secondary, fake_secondary_with_addrs, make_binary, setup_test, FakeWorkerFactory,
    FixedEstimator, NoPeers, TestId,
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


/// Regression: a Recoverable failure in the main pass should NOT
/// requeue immediately (the legacy busy-loop bug). Instead the task
/// lands in `failed_tasks`; after the main operational loop drains,
/// `run_retry_passes` re-injects every failed task and runs the loop
/// again. A task that succeeds on the retry pass leaves
/// `failed_tasks` empty at the end of the run.
///
/// Setup: 1 secondary, 1 binary. Custom in-line fake fails the
/// first attempt with `Recoverable` and succeeds the second. Pre-fix
/// the test would either succeed by busy-loop (Recoverable retried
/// inline at ~10 retries/sec) or hang. Post-fix the binary is
/// failed once, retried in pass 1, completes — final state has 1
/// completion and 0 permanent failures.
///
/// **Demoted-primary regression** (gated under `#[ignore]`): with the
/// `demote on PromotePrimary` change, the local primary no longer runs
/// `run_retry_passes` — the SLURM-primary owns retry. The current
/// SLURM-primary code path (`secondary/peer.rs::TaskFailed`,
/// `secondary/slurm.rs::note_slurm_item_completed`) does NOT yet
/// implement retry for worker-reported Recoverable failures; it
/// simply marks the item gone. So in production right now a
/// Recoverable failure becomes terminal at the SLURM-primary level
/// just as `recoverable_failure_twice_becomes_permanent` already
/// asserts. Re-enable this test once the SLURM-primary grows a
/// retry-on-Recoverable path; the assertions here describe the
/// desired end-state (completed=1, failed=0).
#[ignore = "Recoverable retry currently has no implementation post-demotion; SLURM-primary needs a retry-pass equivalent. See test doc."]
#[tokio::test(flavor = "current_thread")]
async fn recoverable_failure_succeeds_on_retry_pass() {
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
        };

        let mut primary = PrimaryCoordinator::new(
            config,
            transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        let binaries = vec![make_binary("only", 50)];

        let (id, rx, tx) = secondary_ends.remove(0);
        // Custom fake: fail-then-succeed the first task we see.
        // Tracks per-task attempt count locally; first attempt
        // → Recoverable TaskFailed; second attempt → TaskComplete.
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

            let send_request = |tx: &tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
                                id: &str| {
                tx.send(DistributedMessage::TaskRequest {
                    sender_id: id.to_string(), timestamp: 0.0,
                    secondary_id: id.to_string(),
                    worker_id: 0,
                    available_resources: vec![dynrunner_core::ResourceAmount {
                        kind: dynrunner_core::ResourceKind::memory(),
                        amount: 1024 * 1024 * 1024,
                    }],
                }).unwrap();
            };

            let mut attempts: HashMap<String, u32> = HashMap::new();
            while let Some(msg) = rx.recv().await {
                let task_hash_opt = match &msg {
                    DistributedMessage::PeerInfo { .. }
                    | DistributedMessage::TransferComplete { .. } => continue,
                    DistributedMessage::InitialAssignment { zip_files, .. } => zip_files
                        .first()
                        .and_then(|z| z.binaries.first())
                        .map(|e| e.hash.clone()),
                    DistributedMessage::TaskAssignment { file_hash, .. } => {
                        Some(file_hash.clone())
                    }
                    _ => None,
                };
                let Some(task_hash) = task_hash_opt else { continue };

                let n = attempts.entry(task_hash.clone()).or_insert(0);
                *n += 1;
                if *n == 1 {
                    tx.send(DistributedMessage::TaskFailed {
                        sender_id: id.clone(), timestamp: 0.0,
                        secondary_id: id.clone(),
                        worker_id: 0,
                        task_hash,
                        error_type: "Recoverable".into(),
                        error_message: "synthetic transient error".into(),
                    }).unwrap();
                } else {
                    tx.send(DistributedMessage::TaskComplete {
                        sender_id: id.clone(), timestamp: 0.0,
                        secondary_id: id.clone(),
                        worker_id: 0,
                        task_hash,
                        result_data: None,
                    }).unwrap();
                }
                // Worker is idle again — request next task. Without
                // this, primary's pool would hold the re-injected
                // retry task forever and the operational loop hangs.
                send_request(&tx, &id);
            }
        });

        let (deps, ops, ope) = noop_phase_args();
        primary.run(binaries, deps, ops, ope).await.unwrap();

        // Main pass fails (1 failure), retry pass succeeds → final
        // state: 1 completed, 0 permanent failures.
        assert_eq!(primary.completed_count(), 1);
        assert_eq!(primary.failed_count(), 0);
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
                        error_type: "Recoverable".into(),
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
                        error_type: "Recoverable".into(),
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
        // as a SLURM-promoted-primary on local-death without
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

    // On with matching prefix → relative.
    cfg.source_pre_staged_root = Some(std::path::PathBuf::from("/srv/data"));
    assert_eq!(cfg.wire_local_path(&bin), "bin_0");

    // On with mismatching prefix → verbatim (consumer misconfig is
    // surfaced downstream by resolve_pre_staged returning None, not
    // silently re-routed).
    cfg.source_pre_staged_root = Some(std::path::PathBuf::from("/other/prefix"));
    assert_eq!(cfg.wire_local_path(&bin), "/srv/data/bin_0");
}

/// Multi-secondary mesh-ready gate: the primary must NOT issue
/// `PromotePrimary` until every connected secondary has reported
/// `MeshReady`. Pre-fix the promotion fired ~750µs after cert-
/// exchange completed; the SLURM-promoted secondary then became
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

/// Regression: `promote_slurm_primary` flips `self.demoted` to true
/// and from that point `dispatch_to_idle_workers` is a no-op on the
/// scheduler — i.e. the local primary stops handing out work as
/// soon as it has handed authority off to the SLURM-primary.
///
/// Without this contract the local primary and the promoted secondary
/// would both run dispatch in parallel against the same pool, racing
/// for workers and creating duplicate assignments / inconsistent
/// ledger state. See `demoted` doc on `PrimaryCoordinator` for the
/// full rationale.
#[tokio::test(flavor = "current_thread")]
async fn promote_slurm_primary_demotes_local_and_disables_dispatch() {
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
        // `promote_slurm_primary` and `dispatch_to_idle_workers`
        // in isolation.
        let phase = dynrunner_core::PhaseId::from("default");
        let mut pool = PendingPool::<TestId>::new(
            [phase.clone()],
            std::collections::HashMap::new(),
        )
        .expect("default-phase pool");
        let bin = make_binary("solo", 50);
        pool.extend([bin.clone()]);
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
        primary.promote_slurm_primary().await.unwrap();
        assert!(primary.demoted, "promote_slurm_primary must demote local");
        assert_eq!(
            primary.slurm_primary_id.as_deref(),
            Some("sec-0"),
            "promote_slurm_primary records the routing target"
        );

        // The pool still has its queued binary; the worker is
        // still idle. Pre-fix `dispatch_to_idle_workers` would
        // happily take the binary from the pool and assign it.
        // Post-fix it must early-return without touching pool
        // state — since the SLURM-primary now owns dispatch.
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
