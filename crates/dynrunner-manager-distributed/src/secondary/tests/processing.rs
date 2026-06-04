//! Basic worker-processing integration tests: end-to-end
//! initial-assignment + per-task dispatch loop + StageFile pre-flight
//! resolution against the extraction cache.

#![cfg(test)]

use super::super::test_helpers::{
    FakeWorkerFactory, FixedEstimator, TestId, channel_mesh_to_primary,
};
use super::super::*;
use dynrunner_core::{TaskInfo, WorkerId};
use dynrunner_protocol_primary_secondary::{DistributedBinaryInfo, MessageType};
use dynrunner_scheduler::ResourceStealingScheduler;
use std::time::Duration;
use tokio::sync::mpsc as tokio_mpsc;

/// Simulate a primary that coordinates with the secondary.
pub(super) async fn fake_primary(
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

    // Production-faithful shutdown: the primary broadcasts
    // `ClusterMutation::RunComplete` as its last act before exiting, so
    // every secondary's `process_tasks` exits on the
    // `cluster_state.run_complete()` cue. `RunComplete` is the real exit
    // cue (matching production, where the primary's last broadcast — not
    // a transport close — is the run-over signal); the harness drops
    // `to_secondary` only afterwards.
    to_secondary
        .send(DistributedMessage::ClusterMutation {
            sender_id: "primary".into(),
            timestamp: 0.0,
            mutations: vec![dynrunner_protocol_primary_secondary::ClusterMutation::RunComplete],
        })
        .unwrap();

    // Then drop the channel.
    drop(to_secondary);
}

pub(super) fn extract_worker_id(msg: &DistributedMessage<TestId>) -> WorkerId {
    match msg {
        DistributedMessage::TaskRequest { worker_id, .. } => *worker_id,
        _ => 0,
    }
}

pub(super) fn send_task_assignment(
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
        predecessor_outputs: std::collections::BTreeMap::new(),
    })
    .unwrap();
}

pub(super) fn make_binary(name: &str, size: u64) -> TaskInfo<TestId> {
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
        task_id: name.into(),
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
                oom_retry_max_passes: 1,
                primary_link_failure_threshold: 5,
                primary_link_failure_window: Duration::from_secs(30),
                primary_silence_backstop: Duration::from_secs(120),
                unconfigured_deadline: Duration::from_secs(600),
                is_observer: false,
                can_be_primary: false,
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
                secondary_id,
                sec_to_pri_rx,
                pri_to_sec_tx,
            ));

            // Channel-backed mesh: the `fake_primary` is folded in as an
            // ordinary mesh peer keyed by `"primary"` — no per-role uplink.
            let unified = channel_mesh_to_primary(&config.secondary_id, sec_to_pri_tx, pri_to_sec_rx);
            let mut secondary = SecondaryCoordinator::new(
                config,
                unified,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            // Cold-cache resolution of `Destination::Primary` to the folded
            // primary mesh-link's id.
            secondary.set_bootstrap_primary_id("primary".to_string());

            let mut factory = FakeWorkerFactory;
            secondary.run(&mut factory).await.unwrap();

            // The secondary keeps no per-node completed counter; assert
            // the OWN-worker run count (the CRDT-backed `completed_count`
            // is 0 here because the fake primary reports terminal state
            // back but does not broadcast `ClusterMutation::TaskCompleted`
            // into this node's mirror — that's the authority's job).
            assert_eq!(secondary.local_tasks_run_for_test(), 3);

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

            let config = SecondaryConfig {
                secondary_id: "sec-0".into(),
                num_workers: 2,
                max_resources: dynrunner_core::ResourceMap::from([(
                    dynrunner_core::ResourceKind::memory(),
                    2 * 1024 * 1024 * 1024,
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
                is_observer: false,
                can_be_primary: false,
                resource_check_interval: Duration::from_millis(100),
                log_oom_watcher: false,
                promoted_primary_quiesce_grace: Duration::from_millis(100),
                unfulfillable_reinject_max_per_task: None,
                mem_manager_reserved_bytes: None,
                output_dir: None,
                memuse_log_path: None,
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

            // Channel-backed mesh: the `fake_primary` is folded in as an
            // ordinary mesh peer keyed by `"primary"` — no per-role uplink.
            let unified = channel_mesh_to_primary(&config.secondary_id, sec_to_pri_tx, pri_to_sec_rx);
            let mut secondary = SecondaryCoordinator::new(
                config,
                unified,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            // Cold-cache resolution of `Destination::Primary` to the folded
            // primary mesh-link's id.
            secondary.set_bootstrap_primary_id("primary".to_string());

            let mut factory = FakeWorkerFactory;
            secondary.run(&mut factory).await.unwrap();

            assert_eq!(secondary.local_tasks_run_for_test(), 6);

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
                oom_retry_max_passes: 1,
                primary_link_failure_threshold: 5,
                primary_link_failure_window: Duration::from_secs(30),
                primary_silence_backstop: Duration::from_secs(120),
                unconfigured_deadline: Duration::from_secs(600),
                is_observer: false,
                can_be_primary: false,
                resource_check_interval: Duration::from_millis(100),
                log_oom_watcher: false,
                promoted_primary_quiesce_grace: Duration::from_millis(100),
                unfulfillable_reinject_max_per_task: None,
                mem_manager_reserved_bytes: None,
                output_dir: None,
                memuse_log_path: None,
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

            // Channel-backed mesh: the `fake_primary` is folded in as an
            // ordinary mesh peer keyed by `"primary"` — no per-role uplink.
            let unified = channel_mesh_to_primary(&config.secondary_id, sec_to_pri_tx, pri_to_sec_rx);
            let mut secondary = SecondaryCoordinator::new(
                config,
                unified,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            // Cold-cache resolution of `Destination::Primary` to the folded
            // primary mesh-link's id.
            secondary.set_bootstrap_primary_id("primary".to_string());

            let mut factory = FakeWorkerFactory;
            secondary.run(&mut factory).await.unwrap();

            // All 15 must complete; the operational loop is responsible
            // for >= 14 of them since one worker can hold at most one
            // initial assignment.
            assert_eq!(secondary.local_tasks_run_for_test(), 15);

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

            let (sec_to_pri_tx, mut sec_to_pri_rx) =
                tokio_mpsc::unbounded_channel::<DistributedMessage<TestId>>();
            let (pri_to_sec_tx, pri_to_sec_rx) =
                tokio_mpsc::unbounded_channel::<DistributedMessage<TestId>>();
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
                oom_retry_max_passes: 1,
                primary_link_failure_threshold: 5,
                primary_link_failure_window: Duration::from_secs(30),
                primary_silence_backstop: Duration::from_secs(120),
                unconfigured_deadline: Duration::from_secs(600),
                is_observer: false,
                can_be_primary: false,
                resource_check_interval: Duration::from_millis(100),
                log_oom_watcher: false,
                promoted_primary_quiesce_grace: Duration::from_millis(100),
                unfulfillable_reinject_max_per_task: None,
                mem_manager_reserved_bytes: None,
                output_dir: None,
                memuse_log_path: None,
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
                                            task_id: "staged_bin".into(),
                                            task_depends_on: vec![],
                                            preferred_secondaries: Default::default(),
                                        },
                                        local_path: "/nowhere/staged_bin".into(),
                                        file_hash: real_hash_clone.clone(),
                                        predecessor_outputs: std::collections::BTreeMap::new(),
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
                // Production-faithful exit cue (see `fake_primary`):
                // broadcast RunComplete before dropping the primary link so
                // the secondary's `process_tasks` exits on
                // `cluster_state.run_complete()`.
                let _ = pri_to_sec_tx.send(DistributedMessage::ClusterMutation {
                    sender_id: "primary".into(),
                    timestamp: 0.0,
                    mutations: vec![
                        dynrunner_protocol_primary_secondary::ClusterMutation::RunComplete,
                    ],
                });
                drop(pri_to_sec_tx);
            });

            // Channel-backed mesh: the `fake_primary` is folded in as an
            // ordinary mesh peer keyed by `"primary"` — no per-role uplink.
            let unified = channel_mesh_to_primary(&config.secondary_id, sec_to_pri_tx, pri_to_sec_rx);
            let mut secondary = SecondaryCoordinator::new(
                config,
                unified,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            // Cold-cache resolution of `Destination::Primary` to the folded
            // primary mesh-link's id.
            secondary.set_bootstrap_primary_id("primary".to_string());

            let mut factory = FakeWorkerFactory;
            secondary.run(&mut factory).await.unwrap();

            assert_eq!(
                secondary.local_tasks_run_for_test(),
                1,
                "expected the staged-then-assigned task to complete"
            );

            primary_handle.await.unwrap();

            let _ = std::fs::remove_dir_all(&root);
        })
        .await;
}

/// A fake primary that completes the setup handshake then immediately
/// broadcasts `ClusterMutation::RunAborted` — the #3a hard-shutdown cue.
/// No tasks are ever assigned: the abort is the run-over signal.
async fn fake_primary_abort(
    secondary_id: String,
    mut from_secondary: tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
    to_secondary: tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
) {
    // Welcome + cert exchange.
    let (mut got_welcome, mut got_cert) = (false, false);
    while !got_welcome || !got_cert {
        if let Some(msg) = from_secondary.recv().await {
            match msg.msg_type() {
                MessageType::SecondaryWelcome => got_welcome = true,
                MessageType::CertExchange => got_cert = true,
                _ => {}
            }
        } else {
            return;
        }
    }
    to_secondary
        .send(DistributedMessage::PeerInfo {
            sender_id: "primary".into(),
            timestamp: 0.0,
            peers: vec![],
        })
        .unwrap();
    to_secondary
        .send(DistributedMessage::InitialAssignment {
            pre_staged_mode: false,
            uses_file_based_items: true,
            sender_id: "primary".into(),
            timestamp: 0.0,
            secondary_id,
            zip_files: vec![],
            workers_ready: vec![],
            staged_files: vec![],
        })
        .unwrap();
    to_secondary
        .send(DistributedMessage::TransferComplete {
            sender_id: "primary".into(),
            timestamp: 0.0,
            total_files: 0,
            total_bytes: 0,
        })
        .unwrap();
    // The abort cue: broadcast RunAborted. Keep the uplink ALIVE
    // afterwards (drain the secondary's outbound) so the secondary's
    // `process_tasks` exits on the `run_aborted()` check rather than on
    // a channel-closed recv — production-faithful (dropping the uplink
    // alone is NOT the run-over signal; the CRDT flag is). The task
    // returns (dropping `to_secondary`) only once the secondary has
    // gone quiet, which happens after it returns `RunOutcome::Terminal`
    // (projecting to `SecondaryTerminal::Aborted`).
    to_secondary
        .send(DistributedMessage::ClusterMutation {
            sender_id: "primary".into(),
            timestamp: 0.0,
            mutations: vec![
                dynrunner_protocol_primary_secondary::ClusterMutation::RunAborted {
                    reason: "duplicate task identity in the initial batch".into(),
                },
            ],
        })
        .unwrap();
    // Drain until the secondary drops its end (it has exited on the
    // abort), holding `to_secondary` open in the meantime.
    while from_secondary.recv().await.is_some() {}
}

/// `RunAborted` apply → `run_aborted()` set → `process_tasks` returns
/// `RunOutcome::Terminal` (projecting to `SecondaryTerminal::Aborted`),
/// checked BEFORE the `run_complete()` break, and without waiting for any
/// task drain — a hard shutdown.
#[tokio::test(flavor = "current_thread")]
async fn run_aborted_yields_terminal_aborted() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (sec_to_pri_tx, sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
            let (pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();

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
                oom_retry_max_passes: 1,
                primary_link_failure_threshold: 5,
                primary_link_failure_window: Duration::from_secs(30),
                primary_silence_backstop: Duration::from_secs(120),
                unconfigured_deadline: Duration::from_secs(600),
                is_observer: false,
                can_be_primary: false,
                resource_check_interval: Duration::from_millis(100),
                log_oom_watcher: false,
                promoted_primary_quiesce_grace: Duration::from_millis(100),
                unfulfillable_reinject_max_per_task: None,
                mem_manager_reserved_bytes: None,
                output_dir: None,
                memuse_log_path: None,
            };

            let secondary_id = config.secondary_id.clone();
            let primary_handle = tokio::task::spawn_local(fake_primary_abort(
                secondary_id,
                sec_to_pri_rx,
                pri_to_sec_tx,
            ));

            // Channel-backed mesh: the `fake_primary` is folded in as an
            // ordinary mesh peer keyed by `"primary"` — no per-role uplink.
            let unified = channel_mesh_to_primary(&config.secondary_id, sec_to_pri_tx, pri_to_sec_rx);
            let mut secondary = SecondaryCoordinator::new(
                config,
                unified,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            // Cold-cache resolution of `Destination::Primary` to the folded
            // primary mesh-link's id.
            secondary.set_bootstrap_primary_id("primary".to_string());

            let mut factory = FakeWorkerFactory;
            let outcome = secondary
                .run_until_setup_or_done(&mut factory)
                .await
                .expect("run_until_setup_or_done returns Ok(RunOutcome::Terminal)");
            assert!(
                matches!(outcome, RunOutcome::Terminal),
                "expected RunOutcome::Terminal, got {outcome:?}"
            );
            match secondary.terminal() {
                Some(SecondaryTerminal::Aborted { reason }) => {
                    assert!(
                        reason.contains("duplicate task identity"),
                        "Aborted carries the broadcast reason: {reason}"
                    );
                }
                other => panic!("expected SecondaryTerminal::Aborted, got {other:?}"),
            }
            assert!(
                secondary.cluster_state().run_aborted().is_some(),
                "run_aborted() is latched after the RunAborted apply"
            );

            // The fake primary holds the uplink open (draining the
            // secondary's outbound) so the secondary exits on the
            // `run_aborted()` cue rather than a channel-closed recv; it
            // only returns once the secondary drops its end, which won't
            // happen while `secondary` is still owned here. Abort it now
            // that the outcome is asserted.
            primary_handle.abort();
        })
        .await;
}
