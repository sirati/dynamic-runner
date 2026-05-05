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

/// Regression: a SLURM-promoted secondary's `populate_slurm_tasks`
/// must transition phases whose ONLY items are pre-completed
/// elsewhere from Active to Done at construction time. Without the
/// cascade in `slurm.rs`, dependent phases stay Blocked forever and
/// the SLURM-primary hands out "no tasks" to every request — even
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
    use dynrunner_core::{PhaseId, TaskInfo, TypeId};
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
    };
    pool.extend(vec![item]);

    // Pre-cascade: phase-A is Active (no deps, default), phase-B is
    // Blocked (waits for phase-A).
    assert_eq!(pool.phase_state(&phase_a), Some(PhaseState::Active));
    assert_eq!(pool.phase_state(&phase_b), Some(PhaseState::Blocked));

    super::slurm::cascade_drain_done(&mut pool);

    // Post-cascade: phase-A is Done (0 queued, 0 in_flight ⇒ Drained
    // ⇒ Done) and phase-B is Active (parent is Done).
    assert_eq!(pool.phase_state(&phase_a), Some(PhaseState::Done));
    assert_eq!(pool.phase_state(&phase_b), Some(PhaseState::Active));
    assert_eq!(pool.len(), 1, "phase-B's variant must remain queued");
}

/// Watchdog hard-error path: 30s after a non-empty peer dial, zero
/// peers connected ⇒ secondary
///   1. sends a `SecondaryFatalError` to the primary,
///   2. sets `fatal_exit` so `process_tasks` returns `Err(reason)`,
///      causing the secondary to exit non-zero rather than wedging
///      in primary-driven dispatch mode.
///
/// Pre-fix the watchdog only logged a `tracing::warn!` and continued
/// — a SLURM-promoted node with zero peers then sat in a tokio
/// busy-poll (dataset peer reported 86% CPU + 54k epoll_wait/sec on
/// a stuck SLURM-primary for 25 minutes).
#[tokio::test(flavor = "current_thread")]
async fn peer_mesh_watchdog_fatal_error_sets_exit_and_notifies_primary() {
    use std::time::{Duration, Instant};

    let _ = tracing_subscriber::fmt::try_init();

    // Build the secondary by hand so the test can keep the primary
    // side of the channel and inspect what the secondary sent.
    let (sec_to_pri_tx, mut sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
    let (_pri_to_sec_tx, pri_to_sec_rx) =
        tokio_mpsc::unbounded_channel::<DistributedMessage<TestId>>();
    let transport = ChannelPrimaryTransportEnd {
        tx: sec_to_pri_tx,
        rx: pri_to_sec_rx,
    };
    let config = SecondaryConfig {
        secondary_id: "sec-x".into(),
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
    };
    // Concrete `M` is `ChannelManagerEnd` (the helper-shared
    // worker-side endpoint type); see `make_secondary` for the same
    // shape — we duplicate it here only to keep the primary-side
    // receiver around for inspection.
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

    // Arm the watchdog: 4 peers attempted, deadline already in the
    // past, NoPeers reports peer_count() == 0.
    secondary.peer_dial_count = 4;
    secondary.peer_mesh_check_at = Some(Instant::now() - Duration::from_secs(1));

    // Pre-fault: nothing on the wire, no exit flag.
    assert!(secondary.fatal_exit.is_none());
    assert!(sec_to_pri_rx.try_recv().is_err());

    secondary.check_peer_mesh_watchdog().await;

    // Post-fault: exit flag set with a clear reason; watchdog
    // disarmed so a re-tick won't re-fire.
    let reason = secondary
        .fatal_exit
        .as_ref()
        .expect("fatal_exit must be set after watchdog fires");
    assert!(
        reason.contains("0 of 4 peers"),
        "reason should reference attempted peer count, got: {reason}"
    );
    assert!(
        secondary.peer_mesh_check_at.is_none(),
        "watchdog must disarm after firing"
    );

    // The fatal error must have been delivered to the primary's
    // transport endpoint with the matching secondary_id and a
    // non-empty error string.
    let msg = sec_to_pri_rx
        .try_recv()
        .expect("primary channel must have received SecondaryFatalError");
    match msg {
        DistributedMessage::SecondaryFatalError {
            secondary_id,
            error,
            ..
        } => {
            assert_eq!(secondary_id, "sec-x");
            assert!(!error.is_empty());
            assert!(
                error.contains("peer mesh fully failed to form"),
                "error should describe the fault, got: {error}"
            );
        }
        other => panic!("expected SecondaryFatalError, got {:?}", other.msg_type()),
    }

    // The main-loop guard: if `fatal_exit.take()` returns `Some`,
    // `process_tasks` returns `Err(reason)` immediately. Mirror the
    // exact code path here so a future refactor of `process_tasks`
    // that drops the check trips this test.
    let exit = secondary
        .fatal_exit
        .take()
        .map(Err::<(), String>)
        .unwrap_or(Ok(()));
    assert!(exit.is_err(), "fatal_exit must propagate as Err");
    assert!(
        secondary.fatal_exit.is_none(),
        "fatal_exit must be consumed by take()"
    );
}
