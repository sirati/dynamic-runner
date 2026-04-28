//! Tests for the primary coordinator. Fixtures live in
//! `super::test_helpers`; this file holds the test scenarios.

use super::test_helpers::{
    fake_secondary, make_binary, setup_test, FakeWorkerFactory, FixedEstimator, NoPeers, TestId,
};
use super::*;
use db_primary_secondary_comm::{DistributedMessage, MessageType, PeerTransport};
use db_local_manager::WorkerFactory;
use db_comm_api_base::{MessageReceiver, MessageSender};
use db_manager_runner_comm::{Command, Response};
use db_scheduler_impl::ResourceStealingScheduler;
use db_transport_channel::{
    channel_pair, ChannelManagerEnd, ChannelPrimaryTransportEnd, ChannelSecondaryTransportEnd,
};
use crate::secondary::{SecondaryConfig, SecondaryCoordinator};
use std::collections::HashMap;
use tokio::sync::mpsc as tokio_mpsc;


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

        primary.run(binaries).await.unwrap();

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
        };

        let mut primary = PrimaryCoordinator::new(
            config,
            transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        let binaries: Vec<BinaryInfo<TestId>> = (0..6)
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

        primary.run(binaries).await.unwrap();

        assert_eq!(primary.completed_count(), 6);
        assert_eq!(primary.failed_count(), 0);
    }).await;
}

// ── End-to-end tests: real Primary + real Secondary with workers ──


/// Wire up a real SecondaryCoordinator as a tokio task, connected to the
/// primary via channels. Returns the secondary's channel ends that should
/// be plugged into the primary's ChannelTransport.
fn spawn_real_secondary(
    secondary_id: String,
    num_workers: u32,
    max_resources: db_comm_api_base::ResourceMap,
) -> (
    tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,  // primary→secondary
    tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>, // secondary→primary
    tokio::task::JoinHandle<usize>,                    // returns completed count
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
            src_network: None,
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
        let max_res = db_comm_api_base::ResourceMap::from([(db_comm_api_base::ResourceKind::memory(), 1024 * 1024 * 1024u64)]);

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
        };

        let mut primary = PrimaryCoordinator::new(
            config,
            transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        let binaries: Vec<BinaryInfo<TestId>> = (0..5)
            .map(|i| make_binary(&format!("bin_{i}"), 50 + i * 10))
            .collect();

        primary.run(binaries).await.unwrap();

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
        let max_res = db_comm_api_base::ResourceMap::from([(db_comm_api_base::ResourceKind::memory(), 2 * 1024 * 1024 * 1024u64)]);
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
        };

        let mut primary = PrimaryCoordinator::new(
            config,
            transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        let binaries: Vec<BinaryInfo<TestId>> = (0..10)
            .map(|i| make_binary(&format!("bin_{i}"), 50 + i * 10))
            .collect();

        primary.run(binaries).await.unwrap();

        let completed = primary.completed_count();
        let failed = primary.failed_count();

        // Drop primary to close transport channels, allowing secondaries to exit
        drop(primary);

        let mut total_sec_completed = 0;
        for handle in sec_handles {
            total_sec_completed += handle.await.unwrap();
        }

        assert_eq!(completed, 10);
        assert_eq!(failed, 0);
        assert_eq!(total_sec_completed, 10);
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
        };

        let mut primary = PrimaryCoordinator::new(
            config,
            transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        let binaries: Vec<BinaryInfo<TestId>> = (0..20)
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

        primary.run(binaries).await.unwrap();

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

