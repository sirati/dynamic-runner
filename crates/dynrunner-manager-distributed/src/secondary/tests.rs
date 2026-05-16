//! Tests for the secondary coordinator. Kept in a sibling file so the
//! production code stays at a manageable size.

use super::test_helpers::{FakeWorkerFactory, FixedEstimator, NoPeers, TestId};
use super::*;
use dynrunner_core::TaskInfo;
use dynrunner_protocol_primary_secondary::{DistributedBinaryInfo, MessageType};
use dynrunner_scheduler::ResourceStealingScheduler;
use dynrunner_transport_channel::ChannelPrimaryTransportEnd;
use tokio::sync::mpsc as tokio_mpsc;

/// Simulate a primary that coordinates with the secondary.
async fn fake_primary(
    binaries: Vec<TaskInfo<TestId>>,
    secondary_id: String,
    mut from_secondary: tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
    to_secondary: tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
) {
    let mut pending = binaries;
    let total = pending.len();
    let mut completed = 0usize;

    // Wait for welcome + cert exchange
    let mut got_welcome = false;
    let mut got_cert = false;
    while !got_welcome || !got_cert {
        if let Some(msg) = from_secondary.recv().await {
            match msg.msg_type() {
                MessageType::SecondaryWelcome => got_welcome = true,
                MessageType::CertExchange => got_cert = true,
                _ => {}
            }
        }
    }

    // Send peer list (empty — no peers in test)
    to_secondary
        .send(DistributedMessage::PeerInfo {
            sender_id: "primary".into(),
            timestamp: 0.0,
            peers: vec![],
        })
        .unwrap();

    // Send initial assignment (empty — tasks come via TaskAssignment)
    to_secondary
        .send(DistributedMessage::InitialAssignment {
            pre_staged_mode: false,
                    uses_file_based_items: true,
            sender_id: "primary".into(),
            timestamp: 0.0,
            secondary_id: secondary_id.clone(),
            zip_files: vec![],
            workers_ready: vec![],
            staged_files: vec![],
        })
        .unwrap();

    // Send transfer complete
    to_secondary
        .send(DistributedMessage::TransferComplete {
            sender_id: "primary".into(),
            timestamp: 0.0,
            total_files: 0,
            total_bytes: 0,
        })
        .unwrap();

    // Process messages from secondary (task requests, completions)
    while completed < total {
        if let Some(msg) = from_secondary.recv().await {
            match msg.msg_type() {
                MessageType::TaskComplete => {
                    completed += 1;
                }
                MessageType::TaskRequest => {
                    if let Some(binary) = pending.pop() {
                        send_task_assignment(
                            &to_secondary,
                            &secondary_id,
                            &binary,
                            extract_worker_id(&msg),
                        );
                    }
                }
                MessageType::Keepalive => {}
                _ => {}
            }
        }
    }

    // Drop channel to signal secondary to stop
    drop(to_secondary);
}

fn extract_worker_id(msg: &DistributedMessage<TestId>) -> WorkerId {
    match msg {
        DistributedMessage::TaskRequest { worker_id, .. } => *worker_id,
        _ => 0,
    }
}

fn send_task_assignment(
    tx: &tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
    secondary_id: &str,
    binary: &TaskInfo<TestId>,
    worker_id: WorkerId,
) {
    let hash = format!("hash_{}", binary.identifier.0);
    tx.send(DistributedMessage::TaskAssignment {
        sender_id: "primary".into(),
        timestamp: 0.0,
        secondary_id: secondary_id.into(),
        worker_id,
        zip_file: None,
        binary_info: DistributedBinaryInfo::from_task_info(binary),
        local_path: binary.path.to_string_lossy().into_owned(),
        file_hash: hash,
    })
    .unwrap();
}

fn make_binary(name: &str, size: u64) -> TaskInfo<TestId> {
    // Absolute path (despite no real file backing it) — the in-process
    // test fixtures don't configure src_network, and dispatch.rs's
    // unresolvable-task guard fail-loud-rejects relative local_paths
    // when the secondary has no staging dir (since they cannot be
    // resolved by the worker without one). Tests that only exercise
    // the dispatch wire flow (fake worker doesn't actually open the
    // file) are happy with any absolute path; using `/tmp/<name>`
    // keeps the fixture trivial and survives that guard.
    TaskInfo {
        path: std::path::PathBuf::from(format!("/tmp/{name}")),
        size,
        identifier: TestId(name.into()),
        phase_id: dynrunner_core::PhaseId::from("default"),
        type_id: dynrunner_core::TypeId::from("default"),
        affinity_id: None,
        payload: serde_json::Value::Null,
        task_id: None,
        task_depends_on: vec![],
        preferred_secondaries: dynrunner_core::SoftPreferredSecondaries::default(),
        resolved_path: None,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn secondary_with_real_workers_processes_tasks() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (sec_to_pri_tx, sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
            let (pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();

            let transport = ChannelPrimaryTransportEnd {
                tx: sec_to_pri_tx,
                rx: pri_to_sec_rx,
            };

            let config = SecondaryConfig {
                secondary_id: "sec-0".into(),
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
            };

            let binaries = vec![
                make_binary("a", 50),
                make_binary("b", 60),
                make_binary("c", 70),
            ];

            let secondary_id = config.secondary_id.clone();
            let primary_handle = tokio::task::spawn_local(fake_primary(
                binaries,
                secondary_id,
                sec_to_pri_rx,
                pri_to_sec_tx,
            ));

            let mut secondary = SecondaryCoordinator::new(
                config,
                transport,
                NoPeers,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let mut factory = FakeWorkerFactory;
            secondary.run(&mut factory).await.unwrap();

            assert_eq!(secondary.completed_count(), 3);

            primary_handle.await.unwrap();
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn secondary_multi_worker_processes_tasks() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (sec_to_pri_tx, sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
            let (pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();

            let transport = ChannelPrimaryTransportEnd {
                tx: sec_to_pri_tx,
                rx: pri_to_sec_rx,
            };

            let config = SecondaryConfig {
                secondary_id: "sec-0".into(),
                num_workers: 2,
                max_resources: dynrunner_core::ResourceMap::from([(dynrunner_core::ResourceKind::memory(), 2 * 1024 * 1024 * 1024)]),
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
            };

            let binaries: Vec<TaskInfo<TestId>> = (0..6)
                .map(|i| make_binary(&format!("bin_{i}"), 50 + i * 10))
                .collect();

            let secondary_id = config.secondary_id.clone();
            let primary_handle = tokio::task::spawn_local(fake_primary(
                binaries,
                secondary_id,
                sec_to_pri_rx,
                pri_to_sec_tx,
            ));

            let mut secondary = SecondaryCoordinator::new(
                config,
                transport,
                NoPeers,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let mut factory = FakeWorkerFactory;
            secondary.run(&mut factory).await.unwrap();

            assert_eq!(secondary.completed_count(), 6);

            primary_handle.await.unwrap();
        })
        .await;
}

/// Live distribution past the initial assignment: 15 binaries, 1 worker.
/// The initial assignment can cover at most 1 binary (one per worker), so
/// the remaining 14+ must come via the operational TaskRequest →
/// TaskAssignment loop. The legacy Python had a known gap here; this test
/// pins the Rust behaviour so it can't silently regress.
#[tokio::test(flavor = "current_thread")]
async fn live_distribution_continues_past_initial_batch_15_binaries_1_worker() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (sec_to_pri_tx, sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
            let (pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();

            let transport = ChannelPrimaryTransportEnd {
                tx: sec_to_pri_tx,
                rx: pri_to_sec_rx,
            };

            let config = SecondaryConfig {
                secondary_id: "sec-0".into(),
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
                primary_link_failure_threshold: 5,
                primary_link_failure_window: Duration::from_secs(30),
                setup_deadline: Duration::from_secs(60),
                is_observer: false,
                resource_check_interval: Duration::from_millis(100),
                log_oom_watcher: false,
            };

            let binaries: Vec<TaskInfo<TestId>> = (0..15)
                .map(|i| make_binary(&format!("bin_{i}"), 50 + i * 10))
                .collect();

            let secondary_id = config.secondary_id.clone();
            let primary_handle = tokio::task::spawn_local(fake_primary(
                binaries,
                secondary_id,
                sec_to_pri_rx,
                pri_to_sec_tx,
            ));

            let mut secondary = SecondaryCoordinator::new(
                config,
                transport,
                NoPeers,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let mut factory = FakeWorkerFactory;
            secondary.run(&mut factory).await.unwrap();

            // All 15 must complete; the operational loop is responsible
            // for >= 14 of them since one worker can hold at most one
            // initial assignment.
            assert_eq!(secondary.completed_count(), 15);

            primary_handle.await.unwrap();
        })
        .await;
}

/// StageFile + TaskAssignment integration: the secondary receives a
/// StageFile telling it "the file is now at src_network/<src_path>;
/// stage it to src_tmp/<dest_path>". Then it gets a TaskAssignment
/// whose `local_path` does NOT exist anywhere — the only way the
/// task can resolve is via the freshly-staged path that StageFile
/// just registered in the ExtractionCache.
///
/// Pinning this end-to-end behaviour is what makes the wire feature
/// safe to commit: the secondary handler, the cache registration,
/// and the ExtractionCache lookup all interact correctly.
#[tokio::test(flavor = "current_thread")]
async fn stage_file_then_assign_task_succeeds() {
    use crate::zip_extract::compute_file_hash;
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let root = std::env::temp_dir().join(format!(
                "stage_file_test_{}_{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            let src_network = root.join("src_network");
            let src_tmp = root.join("src_tmp");
            std::fs::create_dir_all(&src_network).unwrap();
            std::fs::create_dir_all(&src_tmp).unwrap();

            let payload = b"staged-binary-payload";
            let staged_src = src_network.join("staged_bin");
            std::fs::write(&staged_src, payload).unwrap();
            let real_hash = compute_file_hash(&staged_src).unwrap();

            let (sec_to_pri_tx, mut sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
            let (pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();
            let transport = ChannelPrimaryTransportEnd {
                tx: sec_to_pri_tx,
                rx: pri_to_sec_rx,
            };

            let config = SecondaryConfig {
                secondary_id: "sec-stage".into(),
                num_workers: 1,
                max_resources: dynrunner_core::ResourceMap::from([(
                    dynrunner_core::ResourceKind::memory(),
                    1024 * 1024 * 1024,
                )]),
                hostname: "test-host".into(),
                keepalive_interval: Duration::from_secs(60),
                src_network: Some(src_network.clone()),
                src_tmp: Some(src_tmp.clone()),
                peer_timeout: Duration::from_secs(120),
                keepalive_miss_threshold: 3,
                retry_max_passes: 1,
                primary_link_failure_threshold: 5,
                primary_link_failure_window: Duration::from_secs(30),
                setup_deadline: Duration::from_secs(60),
                is_observer: false,
                resource_check_interval: Duration::from_millis(100),
                log_oom_watcher: false,
            };

            let secondary_id_clone = config.secondary_id.clone();
            let real_hash_clone = real_hash.clone();
            let payload_len = payload.len() as u64;
            let primary_handle = tokio::task::spawn_local(async move {
                let mut got_welcome = false;
                let mut got_cert = false;
                while !got_welcome || !got_cert {
                    if let Some(msg) = sec_to_pri_rx.recv().await {
                        match msg.msg_type() {
                            MessageType::SecondaryWelcome => got_welcome = true,
                            MessageType::CertExchange => got_cert = true,
                            _ => {}
                        }
                    }
                }
                pri_to_sec_tx
                    .send(DistributedMessage::PeerInfo {
                        sender_id: "primary".into(),
                        timestamp: 0.0,
                        peers: vec![],
                    })
                    .unwrap();
                pri_to_sec_tx
                    .send(DistributedMessage::InitialAssignment {
                        pre_staged_mode: false,
                    uses_file_based_items: true,
                        sender_id: "primary".into(),
                        timestamp: 0.0,
                        secondary_id: secondary_id_clone.clone(),
                        zip_files: vec![],
                        workers_ready: vec![],
                        staged_files: vec![],
                    })
                    .unwrap();
                pri_to_sec_tx
                    .send(DistributedMessage::TransferComplete {
                        sender_id: "primary".into(),
                        timestamp: 0.0,
                        total_files: 0,
                        total_bytes: 0,
                    })
                    .unwrap();

                pri_to_sec_tx
                    .send(DistributedMessage::StageFile {
                        sender_id: "primary".into(),
                        timestamp: 0.0,
                        secondary_id: secondary_id_clone.clone(),
                        file_hash: real_hash_clone.clone(),
                        content_hash: real_hash_clone.clone(),
                        src_path: "staged_bin".into(),
                        dest_path: "staged_bin".into(),
                    })
                    .unwrap();

                let mut sent_assignment = false;
                let mut completed = 0usize;
                while completed < 1 {
                    if let Some(msg) = sec_to_pri_rx.recv().await {
                        match msg.msg_type() {
                            MessageType::TaskRequest if !sent_assignment => {
                                let worker_id = extract_worker_id(&msg);
                                pri_to_sec_tx
                                    .send(DistributedMessage::TaskAssignment {
                                        sender_id: "primary".into(),
                                        timestamp: 0.0,
                                        secondary_id: secondary_id_clone.clone(),
                                        worker_id,
                                        zip_file: None,
                                        binary_info: DistributedBinaryInfo {
                                            path: "/nowhere/staged_bin".into(),
                                            size: payload_len,
                                            identifier: TestId("staged_bin".into()),
                                            phase_id: "default".into(),
                                            type_id: "default".into(),
                                            affinity_id: None,
                                            payload_json: "null".into(),
                                            task_id: None,
                                            task_depends_on: vec![],
                                            preferred_secondaries: Default::default(),
                                        },
                                        local_path: "/nowhere/staged_bin".into(),
                                        file_hash: real_hash_clone.clone(),
                                    })
                                    .unwrap();
                                sent_assignment = true;
                            }
                            MessageType::TaskComplete => completed += 1,
                            MessageType::TaskFailed => {
                                panic!("task failed even though file was staged");
                            }
                            _ => {}
                        }
                    } else {
                        break;
                    }
                }
                drop(pri_to_sec_tx);
            });

            let mut secondary = SecondaryCoordinator::new(
                config,
                transport,
                NoPeers,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let mut factory = FakeWorkerFactory;
            secondary.run(&mut factory).await.unwrap();

            assert_eq!(
                secondary.completed_count(),
                1,
                "expected the staged-then-assigned task to complete"
            );

            primary_handle.await.unwrap();

            let _ = std::fs::remove_dir_all(&root);
        })
        .await;
}

/// Regression: a promoted secondary's `populate_primary_from_cluster_state`
/// must transition phases whose ONLY items are pre-completed
/// elsewhere from Active to Done at construction time. Without the
/// cascade in `primary.rs`, dependent phases stay Blocked forever and
/// the primary hands out "no tasks" to every request — even
/// though the dependents have queued items.
///
/// Scenario mirrors dataset-peer's stuck-dispatch bug from
/// 2026-05-04 (b7fjzaqcg): two-phase graph, phase-A has 1 item that
/// already completed elsewhere (so the kept-set filters it out and
/// the pool's phase-A has 0 items + 0 in-flight), phase-B depends
/// on phase-A and has 1 queued item. Expected: phase-B is Active
/// after `cascade_drain_done`, so a downstream `take_first_match`
/// call would find the queued item dispatchable.
#[test]
fn cascade_drain_done_unblocks_dependent_when_parent_phase_is_empty() {
    use dynrunner_core::{PhaseId, SoftPreferredSecondaries, TaskInfo, TypeId};
    use dynrunner_scheduler_api::{PendingPool, PhaseState};
    use std::collections::{HashMap, HashSet};
    use std::path::PathBuf;

    let phase_a = PhaseId::from("phase-a");
    let phase_b = PhaseId::from("phase-b");
    let mut phase_ids = HashSet::new();
    phase_ids.insert(phase_a.clone());
    phase_ids.insert(phase_b.clone());
    let mut deps = HashMap::new();
    deps.insert(phase_b.clone(), vec![phase_a.clone()]);

    let mut pool = PendingPool::<TestId>::new(phase_ids, deps).expect("graph valid");

    // Phase-A's only item completed elsewhere → not in `items` (the
    // post-filter set passed to `extend`). Phase-B's queued item
    // mirrors the variant the dataset peer expected to dispatch.
    let item = TaskInfo {
        path: PathBuf::from("/some/binary"),
        size: 0,
        identifier: TestId("variant".into()),
        phase_id: phase_b.clone(),
        type_id: TypeId::from("default"),
        affinity_id: None,
        payload: serde_json::Value::Null,
        task_id: None,
        task_depends_on: vec![],
        preferred_secondaries: SoftPreferredSecondaries::default(),
        resolved_path: None,
    };
    pool.extend(vec![item]).expect("valid extend");

    // Pre-cascade: phase-A is Active (no deps, default), phase-B is
    // Blocked (waits for phase-A).
    assert_eq!(pool.phase_state(&phase_a), Some(PhaseState::Active));
    assert_eq!(pool.phase_state(&phase_b), Some(PhaseState::Blocked));

    super::primary::cascade_drain_done(&mut pool);

    // Post-cascade: phase-A is Done (0 queued, 0 in_flight ⇒ Drained
    // ⇒ Done) and phase-B is Active (parent is Done).
    assert_eq!(pool.phase_state(&phase_a), Some(PhaseState::Done));
    assert_eq!(pool.phase_state(&phase_b), Some(PhaseState::Active));
    assert_eq!(pool.len(), 1, "phase-B's variant must remain queued");
}

// Helper: build a no-peer secondary with the watchdog already armed
// past the deadline so the next `check_peer_mesh_watchdog()` call
// fires the degraded path. Returns the secondary plus the primary's
// receive end so callers can drain MeshReady / TaskFailed / etc.
fn arm_watchdog_no_peers(
    secondary_id: &str,
    dial_count: u32,
) -> (
    SecondaryCoordinator<
        ChannelPrimaryTransportEnd<TestId>,
        NoPeers,
        dynrunner_transport_channel::ChannelManagerEnd,
        ResourceStealingScheduler,
        FixedEstimator,
        TestId,
    >,
    tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
) {
    use std::time::Instant;
    let (sec_to_pri_tx, sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
    let (_pri_to_sec_tx, pri_to_sec_rx) =
        tokio_mpsc::unbounded_channel::<DistributedMessage<TestId>>();
    let transport = ChannelPrimaryTransportEnd {
        tx: sec_to_pri_tx,
        rx: pri_to_sec_rx,
    };
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
        primary_link_failure_threshold: 5,
        primary_link_failure_window: Duration::from_secs(30),
        setup_deadline: Duration::from_secs(60),
        is_observer: false,
        resource_check_interval: Duration::from_millis(100),
        log_oom_watcher: false,
    };
    let mut secondary: SecondaryCoordinator<
        ChannelPrimaryTransportEnd<TestId>,
        NoPeers,
        dynrunner_transport_channel::ChannelManagerEnd,
        ResourceStealingScheduler,
        FixedEstimator,
        TestId,
    > = SecondaryCoordinator::new(
        config,
        transport,
        NoPeers,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    secondary.peer_dial_count = dial_count;
    secondary.peer_mesh_check_at = Some(Instant::now() - Duration::from_secs(1));
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
    assert!(!secondary.peer_mesh_degraded);
    assert!(sec_to_pri_rx.try_recv().is_err());

    secondary.check_peer_mesh_watchdog().await;

    // Post-fault: degraded latched true, watchdog disarmed, NO
    // fatal_exit (the run continues over WSS).
    assert!(
        secondary.peer_mesh_degraded,
        "peer_mesh_degraded must latch true after deadline-elapsed-zero-peers"
    );
    assert!(
        secondary.fatal_exit.is_none(),
        "watchdog must NOT set fatal_exit in graceful-degrade mode"
    );
    assert!(
        secondary.peer_mesh_check_at.is_none(),
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
            other => panic!("unexpected message on primary channel: {:?}", other.msg_type()),
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
    assert!(!secondary.peer_mesh_degraded);
    assert!(secondary.fatal_exit.is_none());
    assert!(secondary.peer_mesh_check_at.is_some());

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
        !secondary.peer_mesh_degraded,
        "run-complete short-circuit must NOT enter degraded mode"
    );
    assert!(secondary.fatal_exit.is_none());
    assert!(
        secondary.peer_mesh_check_at.is_none(),
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
        secondary.peer_mesh_degraded,
        "pre-RunComplete watchdog must still enter degraded mode"
    );
    assert!(secondary.peer_mesh_check_at.is_none());
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
            let transport = ChannelPrimaryTransportEnd {
                tx: sec_to_pri_tx,
                rx: pri_to_sec_rx,
            };
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
                primary_link_failure_threshold: 5,
                primary_link_failure_window: Duration::from_secs(30),
                setup_deadline: Duration::from_secs(60),
                is_observer: false,
                resource_check_interval: Duration::from_millis(100),
                log_oom_watcher: false,
            };
            let binaries = vec![
                make_binary("a", 50),
                make_binary("b", 60),
                make_binary("c", 70),
            ];
            let secondary_id = config.secondary_id.clone();
            let primary_handle = tokio::task::spawn_local(fake_primary(
                binaries,
                secondary_id,
                sec_to_pri_rx,
                pri_to_sec_tx,
            ));
            let mut secondary = SecondaryCoordinator::new(
                config,
                transport,
                NoPeers,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            // Pre-latch degraded mode so the run starts in the
            // post-watchdog-fire state. The watchdog's actual fire
            // path is covered by `peer_mesh_watchdog_enters_degraded_mode_when_no_peers`.
            secondary.peer_mesh_degraded = true;
            secondary.peer_dial_count = 2;

            let mut factory = FakeWorkerFactory;
            secondary
                .run(&mut factory)
                .await
                .expect("degraded run must complete cleanly over WSS");

            assert_eq!(
                secondary.completed_count(),
                3,
                "WSS dispatch must keep flowing after peer-mesh degraded mode"
            );
            assert!(
                secondary.peer_mesh_degraded,
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
    use super::test_helpers::{election_config, make_secondary};
    use super::election::ElectionState;
    let _ = tracing_subscriber::fmt::try_init();

    let mut sec = make_secondary(election_config("sec-a"));
    // Latch degraded mode (skip running the watchdog — the prior
    // test covers that path; this one only exercises the consumer).
    sec.peer_mesh_degraded = true;
    sec.peer_dial_count = 4;
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
        matches!(sec.election, ElectionState::Normal),
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
/// `FixedPeerCount(3)` reporting a non-empty peer set before the
/// deadline, the watchdog clears `peer_mesh_check_at`, sends
/// `MeshReady(peer_count=3)`, and leaves `peer_mesh_degraded`
/// false.
#[tokio::test(flavor = "current_thread")]
async fn watchdog_healthy_mesh_path_unaffected_by_degrade_refactor() {
    use super::test_helpers::FixedPeerCount;
    use std::time::Instant;
    let _ = tracing_subscriber::fmt::try_init();

    let (sec_to_pri_tx, mut sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
    let (_pri_to_sec_tx, pri_to_sec_rx) =
        tokio_mpsc::unbounded_channel::<DistributedMessage<TestId>>();
    let transport = ChannelPrimaryTransportEnd {
        tx: sec_to_pri_tx,
        rx: pri_to_sec_rx,
    };
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
        primary_link_failure_threshold: 5,
        primary_link_failure_window: Duration::from_secs(30),
        setup_deadline: Duration::from_secs(60),
        is_observer: false,
        resource_check_interval: Duration::from_millis(100),
        log_oom_watcher: false,
    };
    let mut secondary: SecondaryCoordinator<
        ChannelPrimaryTransportEnd<TestId>,
        FixedPeerCount,
        dynrunner_transport_channel::ChannelManagerEnd,
        ResourceStealingScheduler,
        FixedEstimator,
        TestId,
    > = SecondaryCoordinator::new(
        config,
        transport,
        FixedPeerCount(3),
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    // 4 peers attempted, 3 reported by the transport — healthy
    // multi-peer config. Deadline still in the future; the
    // pre-deadline `peer_count > 0` branch is the one that fires.
    secondary.peer_dial_count = 4;
    secondary.peer_mesh_check_at = Some(Instant::now() + Duration::from_secs(30));

    secondary.check_peer_mesh_watchdog().await;

    assert!(
        !secondary.peer_mesh_degraded,
        "healthy mesh path must NOT touch peer_mesh_degraded"
    );
    assert!(secondary.fatal_exit.is_none());
    assert!(
        secondary.peer_mesh_check_at.is_none(),
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
            other => panic!("unexpected message on primary channel: {:?}", other.msg_type()),
        }
    }
    assert!(saw_mesh_ready, "MeshReady must be sent on the healthy path");
}

// ============================================================
// R1: secondary failover on primary-link disconnect.
// Tests below pin the contract introduced by the R1 fix:
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

    use super::super::test_helpers::{election_config, FixedEstimator, FixedPeerCount, TestId};
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
    use super::election::ElectionState;
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
    use super::election::ElectionState;
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
    use super::election::ElectionState;
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
    use super::test_helpers::FixedPeerCount;

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
            secondary.handle_peer_message(assignment).await;

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

// ============================================================
// Step 10 — setup-promote discriminator (`required_setup`) coverage.
//
// Four tests pin the wire-level discriminator between the THREE
// reasons a secondary may become primary (see plan
// `rosy-weaving-cascade.md` section "Discriminator + three promotion
// reasons"):
//
//   1. Setup-promote: submitter deferred discovery to us.
//      `PromotePrimary { required_setup: true }` → setup_pending
//      latches → caller runs Python `discover_items` → calls
//      `ingest_setup_discovery` → pool hydrates.
//   2. Legacy bootstrap: submitter pre-seeded the ledger.
//      `PromotePrimary { required_setup: false }` → no setup_pending,
//      pool hydrates immediately from `cluster_state`.
//   3. Failover after primary loss: a peer election elects us.
//      `record_promotion_confirm` flips us to Promoted, pool hydrates
//      from `cluster_state` (already CRDT-replicated from the live
//      primary's pre-failure broadcasts).
//
// Test 4 is the load-bearing case the wire flag exists FOR: a
// `PromotePrimary { required_setup: false }` arriving on a node whose
// `cluster_state` happens to be empty (failover-at-startup against a
// graveyard cluster). The pre-fix "if ledger empty, run discovery"
// heuristic would have wrongly classified this as setup-promote;
// post-fix the wire flag is the only signal.

mod setup_promote_discriminator {
    //! Tests pinning Step 10's setup-promote behaviour on the
    //! `SecondaryCoordinator`. Each test drives `dispatch_message` or
    //! `record_promotion_confirm` directly so the assertions are on
    //! the state-machine outcome, not on a select! loop's timing.

    use super::super::test_helpers::{election_config, FixedEstimator, RecordingPeer, TestId};
    use super::*;
    use dynrunner_core::{PhaseId, SoftPreferredSecondaries, TaskInfo, TypeId};
    use dynrunner_protocol_primary_secondary::ClusterMutation;
    use dynrunner_scheduler::ResourceStealingScheduler;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::time::Duration;

    /// Build a SecondaryCoordinator backed by a `RecordingPeer`. The
    /// `(secondary, primary_rx, peer_log)` triple is the same shape
    /// the existing watchdog / R1 helpers use: primary-side mpsc on
    /// the LHS, peer-side recorder on the RHS. Each test inspects
    /// whichever it cares about.
    ///
    /// Uses `election_config` so the keepalive / death-threshold
    /// constants match the surrounding test ecosystem; the four
    /// setup-promote tests don't drive election timing but reusing
    /// the helper keeps the construction site identical to the R1
    /// tests for grep-affinity.
    fn make_secondary_with_recording_peer(
        secondary_id: &str,
        peer_count: usize,
    ) -> (
        SecondaryCoordinator<
            ChannelPrimaryTransportEnd<TestId>,
            RecordingPeer<TestId>,
            dynrunner_transport_channel::ChannelManagerEnd,
            ResourceStealingScheduler,
            FixedEstimator,
            TestId,
        >,
        tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
        std::rc::Rc<std::cell::RefCell<Vec<DistributedMessage<TestId>>>>,
    ) {
        let (sec_to_pri_tx, sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
        let (_pri_to_sec_tx, pri_to_sec_rx) =
            tokio_mpsc::unbounded_channel::<DistributedMessage<TestId>>();
        let transport = ChannelPrimaryTransportEnd {
            tx: sec_to_pri_tx,
            rx: pri_to_sec_rx,
        };
        let recorder = RecordingPeer::<TestId>::new(peer_count);
        let peer_log = recorder.log_handle();
        let sec = SecondaryCoordinator::new(
            election_config(secondary_id),
            transport,
            recorder,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );
        (sec, sec_to_pri_rx, peer_log)
    }

    /// Same shape as `make_binary` at the top of `tests.rs` but kept
    /// local so the module compiles even if the top-level helper's
    /// signature drifts. `/tmp/...` paths so the dispatch.rs
    /// resolvable-task guard doesn't reject relative paths under
    /// `src_network=None`.
    fn make_binary(name: &str, phase: &str) -> TaskInfo<TestId> {
        TaskInfo {
            path: PathBuf::from(format!("/tmp/{name}")),
            size: 100,
            identifier: TestId(name.into()),
            phase_id: PhaseId::from(phase),
            type_id: TypeId::from("default"),
            affinity_id: None,
            payload: serde_json::Value::Null,
            task_id: None,
            task_depends_on: vec![],
            preferred_secondaries: SoftPreferredSecondaries::default(),
            resolved_path: None,
        }
    }

    /// Count `ClusterMutation` envelopes carrying a `TaskAdded`. A
    /// single broadcast may batch many `TaskAdded` mutations into one
    /// envelope; the assertions in test 1 want the count of ADDED
    /// items across all envelopes, so flatten before counting.
    fn count_task_added_mutations(
        log: &std::rc::Rc<std::cell::RefCell<Vec<DistributedMessage<TestId>>>>,
    ) -> usize {
        log.borrow()
            .iter()
            .flat_map(|msg| match msg {
                DistributedMessage::ClusterMutation { mutations, .. } => mutations.iter(),
                _ => [].iter(),
            })
            .filter(|m| matches!(m, ClusterMutation::TaskAdded { .. }))
            .count()
    }

    /// Reason 2: setup-promote. `PromotePrimary { required_setup: true }`
    /// arrives → `setup_pending` latches true and the pool is NOT
    /// hydrated yet (cluster_state is empty by contract — submitter
    /// deferred everything to us). After `ingest_setup_discovery` runs:
    ///   - the cluster ledger holds the discovered tasks,
    ///   - the primary pool is hydrated (size == discovered count),
    ///   - `setup_pending` clears,
    ///   - two `TaskAdded` mutations + one `PhaseDepsSet` were
    ///     broadcast to peers.
    #[tokio::test(flavor = "current_thread")]
    async fn test_setup_promote_runs_discovery_then_seeds_then_populates() {
        let _ = tracing_subscriber::fmt::try_init();
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (mut sec, _pri_rx, peer_log) =
                    make_secondary_with_recording_peer("sec-a", 0);

                // Pre-condition: no cluster_state content, setup_pending
                // false. This is the wire-state contract for the
                // setup-promote path.
                assert_eq!(sec.cluster_state.counts().pending, 0);
                assert!(!sec.setup_pending);

                let promote = DistributedMessage::PromotePrimary {
                    sender_id: "primary".into(),
                    timestamp: 0.0,
                    new_primary_id: "sec-a".into(),
                    epoch: 1,
                    required_setup: true,
                };
                sec.dispatch_message(promote)
                    .await
                    .expect("PromotePrimary handler succeeds");

                // Post-handler: latched setup_pending; we ARE primary
                // but NO hydration yet — the pool stays unbuilt because
                // there's nothing to populate from.
                assert!(sec.is_primary, "we are now primary");
                assert!(
                    sec.setup_pending,
                    "required_setup=true must latch setup_pending"
                );
                assert_eq!(
                    sec.primary_pending_len(),
                    0,
                    "pool stays unbuilt until ingest_setup_discovery feeds the ledger"
                );
                assert_eq!(
                    count_task_added_mutations(&peer_log),
                    0,
                    "promotion alone broadcasts no TaskAdded — discovery hasn't run yet"
                );

                // Now drive the wrapper's contract: call
                // ingest_setup_discovery with two mock binaries and an
                // empty phase_deps map (single default phase, no edges).
                let binaries = vec![
                    make_binary("bin-a", "default"),
                    make_binary("bin-b", "default"),
                ];
                let phase_deps: HashMap<PhaseId, Vec<PhaseId>> = HashMap::new();
                sec.ingest_setup_discovery(binaries, phase_deps)
                    .await
                    .expect("ingest_setup_discovery succeeds");

                // Post-ingest: ledger has 2 items, pool hydrated to 2,
                // setup_pending cleared, 2 TaskAdded broadcasts went to
                // peers (plus one PhaseDepsSet envelope which the
                // count helper skips).
                assert_eq!(
                    sec.cluster_state.task_count(),
                    2,
                    "cluster ledger holds the two discovered binaries"
                );
                assert_eq!(
                    sec.primary_pending_len(),
                    2,
                    "primary pool hydrated to the discovered count"
                );
                assert!(
                    !sec.setup_pending,
                    "setup_pending clears after successful ingest"
                );
                assert_eq!(
                    count_task_added_mutations(&peer_log),
                    2,
                    "two TaskAdded mutations broadcast to peers"
                );
            })
            .await;
    }

    /// Reason 1: pre-seeded bootstrap (`required_setup_on_promote =
    /// false` — submitter did discovery + pre-seeded the local
    /// ledger before sending PromotePrimary). `required_setup` is
    /// false (the new field defaults to false on missing-wire-field
    /// shapes too via `#[serde(default)]`, so this is byte-
    /// compatible with pre-Step-10 senders). The handler hydrates
    /// the pool from cluster_state at promotion time; no discovery,
    /// no broadcasts.
    #[tokio::test(flavor = "current_thread")]
    async fn test_pre_seeded_promote_does_not_run_discovery() {
        let _ = tracing_subscriber::fmt::try_init();
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (mut sec, _pri_rx, peer_log) =
                    make_secondary_with_recording_peer("sec-a", 0);

                // Pre-seed cluster_state as if the live submitter's
                // `seed_cluster_state` broadcast had landed: 1 task +
                // empty phase_deps. Mirrors
                // `promotion_hydrates_primary_tasks_from_cluster_state`
                // in election.rs but here we drive the DISPATCH path
                // (PromotePrimary wire arrival), not the
                // record_promotion_confirm path.
                sec.cluster_state.apply(ClusterMutation::<TestId>::PhaseDepsSet {
                    deps: HashMap::new(),
                });
                sec.cluster_state.apply(ClusterMutation::<TestId>::TaskAdded {
                    hash: "hash_bin1".into(),
                    task: make_binary("bin1", "default"),
                });
                assert_eq!(sec.cluster_state.task_count(), 1);
                let broadcasts_before = peer_log.borrow().len();

                let promote = DistributedMessage::PromotePrimary {
                    sender_id: "primary".into(),
                    timestamp: 0.0,
                    new_primary_id: "sec-a".into(),
                    epoch: 1,
                    required_setup: false,
                };
                sec.dispatch_message(promote)
                    .await
                    .expect("PromotePrimary handler succeeds");

                assert!(sec.is_primary);
                assert!(
                    !sec.setup_pending,
                    "required_setup=false must NOT latch setup_pending"
                );
                assert_eq!(
                    sec.primary_pending_len(),
                    1,
                    "pool hydrates from the pre-seeded ledger at promotion time"
                );
                let broadcasts_after = peer_log.borrow().len();
                let new_task_added = count_task_added_mutations(&peer_log);
                assert_eq!(
                    new_task_added, 0,
                    "pre-seeded promotion path must NOT originate new TaskAdded broadcasts"
                );
                assert_eq!(
                    broadcasts_before, broadcasts_after,
                    "no peer-side traffic from a pre-seeded promotion"
                );
            })
            .await;
    }

    /// Reason 3: failover after primary loss. The election state
    /// machine flips us to Promoted via `record_promotion_confirm`
    /// (the existing scenario covered by election.rs's
    /// `promotion_hydrates_primary_tasks_from_cluster_state`). Pin
    /// the setup-pending discriminator: same shape as the legacy
    /// bootstrap test — pool hydrates from cluster_state, no
    /// discovery, no new broadcasts.
    #[tokio::test(flavor = "current_thread")]
    async fn test_failover_election_does_not_run_discovery() {
        use super::super::election::ElectionState;
        let _ = tracing_subscriber::fmt::try_init();
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (mut sec, _pri_rx, peer_log) =
                    make_secondary_with_recording_peer("sec-a", 1);
                // One peer present so the election quorum math
                // (peer_count=1 → quorum=2) works.
                sec.peer_keepalives.insert("sec-b".into(), 0.0);

                // Pre-seed cluster_state as if the live primary's
                // pre-failure broadcasts had landed via CRDT
                // replication: 1 task + empty phase_deps.
                sec.cluster_state.apply(ClusterMutation::<TestId>::PhaseDepsSet {
                    deps: HashMap::new(),
                });
                sec.cluster_state.apply(ClusterMutation::<TestId>::TaskAdded {
                    hash: "hash_bin1".into(),
                    task: make_binary("bin1", "default"),
                });
                let broadcasts_before = peer_log.borrow().len();

                // Drive the candidate-to-promoted path: pretend we're
                // already Candidate (e.g. our self-promotion vote went
                // out a tick earlier), then a peer confirms. Quorum =
                // 2; with ourselves + sec-b that promotes us.
                sec.election = ElectionState::Candidate {
                    round: 1,
                    confirms: std::collections::HashSet::from(["sec-a".to_string()]),
                    started: std::time::Instant::now(),
                };
                let promoted =
                    sec.record_promotion_confirm("sec-b".into(), "sec-a".into(), 1);
                assert!(promoted, "self + one peer confirm = quorum");

                assert!(sec.is_primary);
                assert!(matches!(sec.election, ElectionState::Promoted));
                assert!(
                    !sec.setup_pending,
                    "failover election must NOT latch setup_pending — the wire \
                     flag is the only discriminator and election bypasses it \
                     entirely (record_promotion_confirm goes straight to \
                     populate_primary_from_cluster_state)"
                );
                assert_eq!(
                    sec.primary_pending_len(),
                    1,
                    "failover hydrates the pool from the CRDT-replicated ledger"
                );
                assert_eq!(
                    count_task_added_mutations(&peer_log),
                    0,
                    "failover election originates no new TaskAdded broadcasts"
                );
                assert_eq!(
                    peer_log.borrow().len(),
                    broadcasts_before,
                    "election machinery does not piggyback peer broadcasts on \
                     record_promotion_confirm (PromotionVote went out one tick \
                     earlier in the Suspecting transition, which this test \
                     bypassed by jumping straight to Candidate)"
                );
            })
            .await;
    }

    /// Reason-3-edge-case the wire flag EXISTS to disambiguate:
    /// failover-at-startup against an empty ledger.
    ///
    /// A late-arriving secondary boots; the live primary just elected
    /// some peer as new primary and that peer's `PromotePrimary`
    /// broadcast lands on us before any `ClusterMutation` snapshot
    /// has replicated. Cluster_state is empty for the same reason as
    /// the setup-promote path — but this is NOT setup-promote, it's
    /// failover. The discriminator is the wire flag (`required_setup:
    /// false`), NOT "ledger empty". If the handler used the empty-
    /// ledger heuristic instead, it would wrongly latch setup_pending
    /// and the wrapper would call `discover_items` from cold-start —
    /// duplicating work AND racing the actual snapshot fetch.
    ///
    /// This test pins that `required_setup: false` with an empty
    /// ledger leaves setup_pending false. The pool stays unbuilt at
    /// this exact tick (nothing to hydrate from) but that's the
    /// correct behaviour: the subsequent snapshot RPC + CRDT replay
    /// will populate the ledger, and the next
    /// `populate_primary_from_cluster_state` call (e.g. on the next
    /// drain check) will hydrate the pool.
    #[tokio::test(flavor = "current_thread")]
    async fn test_failover_at_startup_does_not_redo_discovery() {
        let _ = tracing_subscriber::fmt::try_init();
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (mut sec, _pri_rx, peer_log) =
                    make_secondary_with_recording_peer("sec-a", 0);

                // Pre-condition: empty cluster_state. Same as the
                // setup-promote test's pre-condition; the wire flag is
                // what differs.
                assert_eq!(sec.cluster_state.task_count(), 0);
                assert!(!sec.setup_pending);
                let broadcasts_before = peer_log.borrow().len();

                // Sender_id deliberately names a peer (not "primary")
                // to make the failover origin explicit in the test
                // shape. The handler doesn't read sender_id for the
                // setup_pending decision — only `required_setup` —
                // but a future maintainer reading this test sees
                // "peer-originated PromotePrimary against empty
                // ledger" and the assertion makes sense.
                let promote = DistributedMessage::PromotePrimary {
                    sender_id: "sec-b".into(),
                    timestamp: 0.0,
                    new_primary_id: "sec-a".into(),
                    epoch: 1,
                    required_setup: false,
                };
                sec.dispatch_message(promote)
                    .await
                    .expect("PromotePrimary handler succeeds");

                assert!(sec.is_primary);
                assert!(
                    !sec.setup_pending,
                    "EMPTY-ledger PromotePrimary with required_setup=false must \
                     NOT latch setup_pending — the wire flag, not ledger \
                     emptiness, is the discriminator"
                );
                assert_eq!(
                    count_task_added_mutations(&peer_log),
                    0,
                    "failover-at-startup originates no TaskAdded broadcasts (no \
                     local discovery)"
                );
                assert_eq!(
                    peer_log.borrow().len(),
                    broadcasts_before,
                    "no peer-side traffic emitted by the failover-at-startup \
                     promotion handler"
                );
                // Sanity: a brief delay then re-check that setup_pending
                // hasn't been flipped by some deferred path. The
                // existing dispatch contract is synchronous, so this
                // is belt-and-suspenders against a future change that
                // moved the latch into a deferred task.
                tokio::time::sleep(Duration::from_millis(10)).await;
                assert!(
                    !sec.setup_pending,
                    "setup_pending stays false across a brief wait — no \
                     deferred flip"
                );
            })
            .await;
    }
}

/// Late-joiner observer scenario (transport-unification Step 9).
///
/// Pins the `restore_from_snapshot_and_skip_setup` foundation API +
/// the observer's run-loop entry: a freshly-constructed
/// `SecondaryCoordinator` with `is_observer=true` and `num_workers=0`
/// that calls the foundation API installs the snapshot's task ledger,
/// observers, and primary epoch, AND latches `setup_phase_completed`
/// so the next `run_until_setup_or_done` call skips the welcome /
/// cert-exchange / wait-for-setup phases entirely (the load-bearing
/// behaviour the late-joiner CLI dispatcher exploits).
///
/// The 3-node analogue runs as three secondaries built on the
/// channel-transport mesh: one "primary-peer" (current_primary
/// holder), one regular peer, and the joining observer. The
/// observer's snapshot restore is driven directly (not via the
/// `RequestClusterSnapshot` RPC — that's covered by the
/// `dynrunner-transport-channel` snapshot_bootstrap integration test)
/// so the assertions focus on the post-restore coordinator state.
mod late_joiner_observer {
    use super::super::test_helpers::{election_config, FixedEstimator, NoPeers, TestId};
    use super::*;
    use dynrunner_core::TaskInfo;
    use dynrunner_scheduler::ResourceStealingScheduler;
    use std::collections::{HashMap, HashSet};
    use std::path::PathBuf;
    use tokio::sync::mpsc as tokio_mpsc;

    /// Construct a 3-node-mesh-analogous joiner: a single
    /// `SecondaryCoordinator` configured as observer
    /// (`is_observer=true`, `num_workers=0`). The "rest of the cluster"
    /// shows up purely as the snapshot the test hands it. The
    /// `NoPeers` peer transport (peer_count=0) is what
    /// `make_secondary` uses elsewhere; the late-joiner code path
    /// the test cares about (restore + skip-setup) runs to
    /// completion regardless of peer reachability — peer membership
    /// is asserted on the role-table side, not the transport side.
    fn make_observer_secondary(
        observer_id: &str,
    ) -> SecondaryCoordinator<
        ChannelPrimaryTransportEnd<TestId>,
        NoPeers,
        dynrunner_transport_channel::ChannelManagerEnd,
        ResourceStealingScheduler,
        FixedEstimator,
        TestId,
    > {
        let (sec_to_pri_tx, _sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
        let (_pri_to_sec_tx, pri_to_sec_rx) =
            tokio_mpsc::unbounded_channel::<DistributedMessage<TestId>>();
        let transport = ChannelPrimaryTransportEnd {
            tx: sec_to_pri_tx,
            rx: pri_to_sec_rx,
        };
        let mut config = election_config(observer_id);
        config.is_observer = true;
        config.num_workers = 0;
        SecondaryCoordinator::new(
            config,
            transport,
            NoPeers,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        )
    }

    /// Build a synthetic `ClusterStateSnapshot<TestId>` carrying two
    /// pending tasks, a designated `current_primary`, primary_epoch=7,
    /// and one observer id. The same shape the wire frame's
    /// `snapshot_json` decodes to.
    fn make_synthetic_snapshot()
    -> crate::cluster_state::ClusterStateSnapshot<TestId> {
        use crate::cluster_state::TaskState;
        let mut tasks = HashMap::new();
        let mk_pending = |path: &str, ident: &str| TaskState::Pending {
            task: TaskInfo {
                path: PathBuf::from(path),
                size: 100,
                identifier: TestId(ident.into()),
                phase_id: dynrunner_core::PhaseId::from("default"),
                type_id: dynrunner_core::TypeId::from("default"),
                affinity_id: None,
                payload: serde_json::Value::Null,
                task_id: None,
                task_depends_on: vec![],
                preferred_secondaries: dynrunner_core::SoftPreferredSecondaries::default(),
                resolved_path: None,
            },
        };
        tasks.insert(
            "task-1".to_string(),
            mk_pending("/tmp/task-1", "task-1"),
        );
        tasks.insert(
            "task-2".to_string(),
            mk_pending("/tmp/task-2", "task-2"),
        );
        let mut observers = HashSet::new();
        observers.insert("observer-peer".to_string());
        crate::cluster_state::ClusterStateSnapshot {
            tasks,
            current_primary: Some("primary-peer".to_string()),
            primary_epoch: 7,
            phase_deps: HashMap::new(),
            observers,
            peer_holdings: HashMap::new(),
        }
    }

    /// `restore_from_snapshot_and_skip_setup` is the load-bearing
    /// API: a single call must (a) install the snapshot's task
    /// ledger, observers, and current_primary into the coordinator's
    /// `cluster_state`, AND (b) latch `setup_phase_completed=true`
    /// so the next `run_until_setup_or_done` call skips the
    /// welcome / cert-exchange / wait-for-setup phases.
    #[test]
    fn restore_installs_snapshot_and_latches_setup_completed() {
        let mut sec = make_observer_secondary("observer-1");

        // Pre-condition: every field this test asserts is at its
        // freshly-constructed default. Pinning the pre-conditions
        // catches "the field was already true / non-empty before
        // restore" regressions that would otherwise silently make
        // the post-condition asserts pass for the wrong reason.
        assert!(!sec.setup_phase_completed);
        assert_eq!(sec.cluster_state.task_count(), 0);
        assert!(sec.cluster_state.current_primary().is_none());
        assert!(sec.cluster_state.role_table().observers.is_empty());

        sec.restore_from_snapshot_and_skip_setup(make_synthetic_snapshot());

        // Latch is set — `run_until_setup_or_done` will skip the
        // entire `!setup_phase_completed` setup block on its next
        // call.
        assert!(sec.setup_phase_completed);

        // Task ledger merged in: two pending tasks survive.
        assert_eq!(sec.cluster_state.task_count(), 2);

        // current_primary and primary_epoch reflect the snapshot's
        // authority — the joiner's role cache (read via the
        // PeerTransport hook registered in `new()`) now knows
        // who's primary, so Address::Role(Role::Primary) dispatches
        // resolve immediately rather than failing with
        // "role-table cache empty".
        assert_eq!(
            sec.cluster_state.current_primary(),
            Some("primary-peer"),
        );
        assert_eq!(sec.cluster_state.primary_epoch(), 7);

        // Observer set merged — Step 7's election filter will skip
        // `observer-peer` from `lowest_alive` candidate selection
        // even before the next live PeerInfo broadcast lands.
        let observers = &sec.cluster_state.role_table().observers;
        assert!(observers.contains("observer-peer"));
        assert_eq!(observers.len(), 1);
    }

    /// The same `restore` call applied twice is a no-op the second
    /// time — `ClusterState::restore` is documented as idempotent /
    /// CRDT-merge. Pins that the wrapper preserves the underlying
    /// idempotency (i.e. the wrapper doesn't toggle the latch back
    /// or re-broadcast).
    #[test]
    fn restore_is_idempotent_on_second_call() {
        let mut sec = make_observer_secondary("observer-1");
        sec.restore_from_snapshot_and_skip_setup(make_synthetic_snapshot());
        let tasks_after_first = sec.cluster_state.task_count();
        let epoch_after_first = sec.cluster_state.primary_epoch();

        // Second call with the SAME snapshot — the merge rules
        // (`primary_epoch > self.primary_epoch` gate, observer-set
        // "only when local empty" gate) make this a no-op.
        sec.restore_from_snapshot_and_skip_setup(make_synthetic_snapshot());

        assert!(sec.setup_phase_completed, "latch stays true");
        assert_eq!(sec.cluster_state.task_count(), tasks_after_first);
        assert_eq!(sec.cluster_state.primary_epoch(), epoch_after_first);
    }

    /// Observer config sanity: an observer's `is_observer=true` flag
    /// reaches the coordinator's `config` so downstream consumers
    /// (the election filter at `election.rs::run_election_tick`'s
    /// `we_lead` branch, the dispatch defensive reject at
    /// `dispatch.rs::handle_promote_primary`) see the flag.
    #[test]
    fn observer_config_propagated_to_coordinator() {
        let sec = make_observer_secondary("observer-1");
        assert!(
            sec.config.is_observer,
            "observer flag must be readable on the coordinator's config — \
             election + dispatch defensive paths both consult it"
        );
        assert_eq!(
            sec.config.num_workers, 0,
            "observer's num_workers must be 0 (no work to take on)"
        );
    }

    /// After restore, the coordinator's `run_until_setup_or_done`
    /// MUST NOT touch `primary_transport` (no welcome /
    /// cert-exchange / wait-for-setup), and MUST NOT spawn workers
    /// via the factory. The cleanest pin is: drive the run loop
    /// briefly under a short tokio::time::timeout — the call must
    /// return `Ok(Done)` only when the cluster reports RunComplete;
    /// for this test we just need to assert it ENTERS the
    /// processing branch (the welcome handshake would have errored
    /// out on the disconnected primary channel within milliseconds).
    ///
    /// We assert the easier shape: with `setup_phase_completed=true`
    /// pre-set, a `run_until_setup_or_done` future advances past
    /// the setup block. The `FakeWorkerFactory` is wired in case
    /// some future code path under processing_loop pulls on it; with
    /// num_workers=0 nothing pulls on the factory today, but the
    /// wiring keeps the test resilient.
    #[tokio::test(flavor = "current_thread")]
    async fn run_after_restore_skips_setup_handshake() {
        let _ = tracing_subscriber::fmt::try_init();
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let mut sec = make_observer_secondary("observer-1");
                sec.restore_from_snapshot_and_skip_setup(make_synthetic_snapshot());

                let mut factory = super::test_helpers::FakeWorkerFactory;
                // The run loop polls peer/primary/timers; without
                // any RunComplete broadcast it would block forever
                // in `process_tasks`. Wrap in a short timeout: a
                // timeout (not an Err) is the success signal here —
                // it means we entered `process_tasks` (the setup
                // block would have errored on the disconnected
                // primary channel and returned an Err well within
                // 100ms). If `Err` comes back, the setup-skip
                // latch failed and the welcome attempted to send /
                // recv on the dead primary transport.
                let outcome = tokio::time::timeout(
                    std::time::Duration::from_millis(100),
                    sec.run_until_setup_or_done(&mut factory),
                )
                .await;
                match outcome {
                    Err(_elapsed) => {
                        // Expected: we're in the processing loop,
                        // waiting on peer / primary / timer events.
                    }
                    Ok(Err(e)) => panic!(
                        "run_until_setup_or_done returned Err — setup-skip \
                         latch did not take effect (welcome attempted on \
                         a dead primary): {e}"
                    ),
                    Ok(Ok(RunOutcome::Done)) => {
                        // Acceptable but unusual: processing-loop's
                        // run-complete exit may fire if the snapshot
                        // carried RunComplete mutation, which it
                        // doesn't in `make_synthetic_snapshot`. If
                        // a future fixture toggles it, this branch
                        // still asserts the right behaviour.
                    }
                    Ok(Ok(RunOutcome::SetupPending)) => panic!(
                        "observer must never see SetupPending — only \
                         PromotePrimary{{required_setup=true}} causes it, \
                         which an observer rejects"
                    ),
                }
            })
            .await;
    }
}

/// Late-joiner accept: the existing-cluster responder to
/// `RequestClusterSnapshot` originates a `PeerJoined { is_observer:
/// true }` mutation for the joiner. By current design every
/// late-joiner is an observer, so the wire flag is fixed; the joiner
/// id comes from the request's `sender_id`.
///
/// Why this lives on the responder rather than on the primary: the
/// joiner reaches the cluster over the peer mesh (see
/// `PeerTransport::join_running_cluster`) and does NOT have a direct
/// primary link until its restore latches `current_primary`. The
/// snapshot-serving peer is the first existing cluster member that
/// observes the joiner, so it is the natural originator. The
/// originator-side fan-out (`apply_and_broadcast_mutations`) carries
/// the mutation to peers AND to the live primary via the secondary's
/// primary_transport, so the primary's `peer_state` ledger converges
/// without a separate route.
mod late_joiner_accept_emits_peer_joined {
    use super::super::test_helpers::{election_config, FixedEstimator, RecordingPeer, TestId};
    use super::*;
    use dynrunner_protocol_primary_secondary::ClusterMutation;
    use dynrunner_scheduler::ResourceStealingScheduler;

    /// Mirror of the `setup_promote_discriminator` fixture: build a
    /// SecondaryCoordinator backed by a `RecordingPeer` so the test can
    /// inspect what got broadcast over the peer mesh, plus a hand-held
    /// primary-side receiver so the test can confirm the unicast
    /// snapshot reply does NOT mis-fire on `primary_transport`.
    fn make_secondary_with_recording_peer(
        secondary_id: &str,
    ) -> (
        SecondaryCoordinator<
            ChannelPrimaryTransportEnd<TestId>,
            RecordingPeer<TestId>,
            dynrunner_transport_channel::ChannelManagerEnd,
            ResourceStealingScheduler,
            FixedEstimator,
            TestId,
        >,
        tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
        std::rc::Rc<std::cell::RefCell<Vec<DistributedMessage<TestId>>>>,
    ) {
        let (sec_to_pri_tx, sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
        let (_pri_to_sec_tx, pri_to_sec_rx) =
            tokio_mpsc::unbounded_channel::<DistributedMessage<TestId>>();
        let transport = ChannelPrimaryTransportEnd {
            tx: sec_to_pri_tx,
            rx: pri_to_sec_rx,
        };
        let recorder = RecordingPeer::<TestId>::new(1);
        let peer_log = recorder.log_handle();
        let sec = SecondaryCoordinator::new(
            election_config(secondary_id),
            transport,
            recorder,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );
        (sec, sec_to_pri_rx, peer_log)
    }

    /// Find the first `ClusterMutation::PeerJoined` mutation in the
    /// recorded peer-bus traffic. The `RecordingPeer` collates both
    /// `broadcast` and `send_to_peer` envelopes into one log; the
    /// snapshot reply uses `send_to_peer` and the originator's
    /// `apply_and_broadcast_mutations` uses `broadcast` — both land in
    /// the same vector, so the filter walks any-arm-any-frame.
    fn find_peer_joined(
        log: &std::rc::Rc<std::cell::RefCell<Vec<DistributedMessage<TestId>>>>,
    ) -> Option<(String, bool)> {
        log.borrow().iter().find_map(|msg| match msg {
            DistributedMessage::ClusterMutation { mutations, .. } => {
                mutations.iter().find_map(|m| match m {
                    ClusterMutation::PeerJoined {
                        peer_id,
                        is_observer,
                    } => Some((peer_id.clone(), *is_observer)),
                    _ => None,
                })
            }
            _ => None,
        })
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handle_late_joiner_accept_emits_peer_joined_observer_true() {
        let _ = tracing_subscriber::fmt::try_init();
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (mut sec, _pri_rx, peer_log) =
                    make_secondary_with_recording_peer("responder");

                // Pre-condition pins: the observer set is empty before
                // the snapshot RPC arrives. Without this the post-state
                // assert could be tautologically satisfied by an
                // unrelated population of `observers`.
                assert!(
                    sec.cluster_state.role_table().observers.is_empty(),
                    "observer set must be empty pre-accept; got {:?}",
                    sec.cluster_state.role_table().observers
                );

                // Drive the late-joiner accept path: the joiner's
                // `RequestClusterSnapshot` arriving on the responder's
                // dispatch loop.
                let req = DistributedMessage::RequestClusterSnapshot {
                    sender_id: "late-observer-1".into(),
                    timestamp: 0.0,
                };
                sec.dispatch_message(req)
                    .await
                    .expect("RequestClusterSnapshot handler succeeds");

                // (1) The originator-side apply landed locally: the
                // joiner shows up in the observer projection. This
                // exercises the widened `apply_peer_joined` rule's
                // `is_observer = true` branch via the canonical
                // `apply_and_broadcast_mutations` path.
                assert!(
                    sec.cluster_state
                        .role_table()
                        .observers
                        .contains("late-observer-1"),
                    "late-joiner must be projected into role_table.observers \
                     via the originator-side apply; observers={:?}",
                    sec.cluster_state.role_table().observers
                );

                // (2) The mutation was broadcast over the peer mesh so
                // every other cluster member converges. `find_peer_joined`
                // scans the recorder for the exact envelope shape.
                let observed = find_peer_joined(&peer_log).expect(
                    "RequestClusterSnapshot accept must originate one \
                     PeerJoined mutation on the peer bus",
                );
                assert_eq!(
                    observed,
                    ("late-observer-1".to_string(), true),
                    "late-joiner PeerJoined must carry the joiner's id \
                     and is_observer=true (current design treats every \
                     late-joiner as an observer); got {:?}",
                    observed
                );
            })
            .await;
    }
}

/// Integration tests for the production announcer wire-out. The
/// `PeerMeshAnnouncerSender` itself is tested in
/// `observer/announcer.rs::tests`; this module pins the bundle the
/// `SecondaryCoordinator::attach_observer_announcer` helper returns
/// (the outbox channel allocation, the production sender's
/// construction, and the coordinator-side receiver install) so a
/// future refactor of that helper cannot silently regress the
/// wire-out without firing a test.
#[cfg(test)]
mod observer_announcer_wireup {
    use super::test_helpers::{election_config, make_secondary};
    use crate::observer::announcer::{
        AnnouncerSender, PeerResourceHoldingsUpdatedPayload,
    };
    use dynrunner_protocol_primary_secondary::{ClusterMutation, DistributedMessage};

    /// Pins the production wiring contract: a `SecondaryCoordinator`
    /// returns both an `AnnouncerHandle` AND a production
    /// `PeerMeshAnnouncerSender` from `attach_observer_announcer`,
    /// and the announcer's `send_holdings` ends up posting a
    /// `DistributedMessage::ClusterMutation { mutations: vec![
    /// PeerResourceHoldingsUpdated { … } ] }` onto the coordinator-
    /// side outbox (which the operational `select!` will drain onto
    /// `peer_transport.send(Role::Primary, …)`).
    ///
    /// Without this integration, a refactor that breaks the bundle
    /// shape (e.g. drops the outbox allocation or returns the wrong
    /// sender variant) would compile but silently leave the
    /// announcer talking to a dead channel — observable only as a
    /// missing wire frame on the production peer mesh.
    #[tokio::test(flavor = "current_thread")]
    async fn observer_run_attaches_production_announcer_sender() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let mut sec = make_secondary(election_config("observer-int"));

                let holdings: std::collections::HashSet<String> = [
                    "/nix/store/alpha".to_string(),
                    "/nix/store/beta".to_string(),
                ]
                .into_iter()
                .collect();
                let (handle, mut sender) = sec.attach_observer_announcer(holdings);

                // The bundle exposes the four `run_observer_announcer`
                // inputs verbatim.
                assert_eq!(handle.peer_id, "observer-int");
                assert_eq!(
                    handle.holdings,
                    [
                        "/nix/store/alpha".to_string(),
                        "/nix/store/beta".to_string()
                    ]
                    .into_iter()
                    .collect::<std::collections::HashSet<_>>()
                );

                // The coordinator now owns the matching outbox receiver
                // (the operational `select!` would take it on entry).
                // Without this, the production drain arm would never
                // see the announcer's posts.
                assert!(
                    sec.announcer_outbox_rx.is_some(),
                    "attach_observer_announcer must install the outbox \
                     receiver on the coordinator so process_tasks' drain \
                     arm can dequeue announce posts"
                );
                assert!(
                    sec.announcer_outbox_tx.is_some(),
                    "the coordinator-side outbox sender clone must be \
                     installed so the channel stays alive across announcer-\
                     task shutdown"
                );

                // Take the outbox receiver out so this test can act as
                // the drain side; the production select! does the same
                // shape.
                let mut outbox_rx = sec
                    .announcer_outbox_rx
                    .take()
                    .expect("just asserted Some");

                // Drive one `send_holdings` and assert the wire shape
                // arrives on the outbox. Concurrent join because
                // `send_holdings` awaits the reply oneshot.
                let body = PeerResourceHoldingsUpdatedPayload {
                    peer_id: "observer-int".into(),
                    holdings: vec![
                        "/nix/store/alpha".into(),
                        "/nix/store/beta".into(),
                    ],
                    epoch: 11,
                };
                let send_fut = sender.send_holdings(&body);
                let drain_fut = async {
                    let item = outbox_rx.recv().await.expect("outbox carries one item");
                    item.reply.send(Ok(())).expect("send-side awaiting");
                    item.msg
                };
                let (send_result, captured_msg) = tokio::join!(send_fut, drain_fut);
                send_result.expect("send_holdings resolves Ok when drain replies Ok");

                match captured_msg {
                    DistributedMessage::ClusterMutation {
                        sender_id,
                        mutations,
                        ..
                    } => {
                        assert_eq!(
                            sender_id, "observer-int",
                            "sender_id must equal the observer's secondary_id"
                        );
                        assert_eq!(mutations.len(), 1, "one mutation per announce");
                        match &mutations[0] {
                            ClusterMutation::PeerResourceHoldingsUpdated {
                                peer_id,
                                holdings,
                                epoch,
                            } => {
                                assert_eq!(peer_id, "observer-int");
                                assert_eq!(
                                    holdings,
                                    &vec![
                                        "/nix/store/alpha".to_string(),
                                        "/nix/store/beta".to_string()
                                    ]
                                );
                                assert_eq!(*epoch, 11);
                            }
                            other => panic!(
                                "expected PeerResourceHoldingsUpdated; got {other:?}"
                            ),
                        }
                    }
                    other => panic!(
                        "expected DistributedMessage::ClusterMutation; got {other:?}"
                    ),
                }
            })
            .await;
    }
}
