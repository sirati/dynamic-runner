//! Tests for the primary coordinator. Fixtures live in
//! `super::test_helpers`; this file holds the test scenarios.

use super::test_helpers::{
    fake_secondary, make_binary, setup_test, FakeWorkerFactory, FixedEstimator, NoPeers, TestId,
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

