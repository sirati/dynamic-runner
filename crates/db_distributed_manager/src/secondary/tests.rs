//! Tests for the secondary coordinator. Kept in a sibling file so the
//! production code stays at a manageable size.

use super::test_helpers::{FakeWorkerFactory, FixedEstimator, NoPeers, TestId};
use super::*;
use db_primary_secondary_comm::{DistributedBinaryInfo, MessageType};
use db_scheduler_impl::ResourceStealingScheduler;
use db_transport_channel::ChannelPrimaryTransportEnd;
use tokio::sync::mpsc as tokio_mpsc;

/// Simulate a primary that coordinates with the secondary.
async fn fake_primary(
    binaries: Vec<BinaryInfo<TestId>>,
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
            sender_id: "primary".into(),
            timestamp: 0.0,
            secondary_id: secondary_id.clone(),
            zip_files: vec![],
            workers_ready: vec![],
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
    binary: &BinaryInfo<TestId>,
    worker_id: WorkerId,
) {
    let hash = format!("hash_{}", binary.identifier.0);
    tx.send(DistributedMessage::TaskAssignment {
        sender_id: "primary".into(),
        timestamp: 0.0,
        secondary_id: secondary_id.into(),
        worker_id,
        zip_file: None,
        binary_info: DistributedBinaryInfo {
            path: binary.path.to_string_lossy().into_owned(),
            size: binary.size,
            identifier: binary.identifier.clone(),
        },
        local_path: binary.path.to_string_lossy().into_owned(),
        file_hash: hash,
    })
    .unwrap();
}

fn make_binary(name: &str, size: u64) -> BinaryInfo<TestId> {
    BinaryInfo {
        path: std::path::PathBuf::from(name),
        size,
        identifier: TestId(name.into()),
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
                max_resources: db_comm_api_base::ResourceMap::from([(db_comm_api_base::ResourceKind::memory(), 1024 * 1024 * 1024)]),
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
                max_resources: db_comm_api_base::ResourceMap::from([(db_comm_api_base::ResourceKind::memory(), 2 * 1024 * 1024 * 1024)]),
                hostname: "test-host".into(),
                keepalive_interval: Duration::from_secs(60),
                src_network: None,
                src_tmp: None,
                peer_timeout: Duration::from_secs(120),
                keepalive_miss_threshold: 3,
            };

            let binaries: Vec<BinaryInfo<TestId>> = (0..6)
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
                max_resources: db_comm_api_base::ResourceMap::from([(
                    db_comm_api_base::ResourceKind::memory(),
                    1024 * 1024 * 1024,
                )]),
                hostname: "test-host".into(),
                keepalive_interval: Duration::from_secs(60),
                src_network: None,
                src_tmp: None,
                peer_timeout: Duration::from_secs(120),
                keepalive_miss_threshold: 3,
            };

            let binaries: Vec<BinaryInfo<TestId>> = (0..15)
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
