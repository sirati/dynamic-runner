//! Tests for the primary coordinator. Kept in a sibling file so the
//! production code stays at a manageable size.

use super::*;
use db_comm_api_base::BinaryInfo;
use db_local_manager::WorkerFactory;
use db_manager_runner_comm::{Command, Response};
use db_primary_secondary_comm::{
    DistributedMessage, MessageType, PeerTransport,
};
use db_scheduler_api::ResourceEstimator;
use db_scheduler_impl::ResourceStealingScheduler;
use db_transport_channel::{channel_pair, ChannelManagerEnd};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc as tokio_mpsc;

/// Minimal test identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct TestId(String);

#[derive(Clone)]
struct FixedEstimator(u64);
impl ResourceEstimator for FixedEstimator {
    fn estimate(&self, _size: u64) -> db_comm_api_base::ResourceMap {
        db_comm_api_base::ResourceMap::from([(db_comm_api_base::ResourceKind::memory(), self.0)])
    }
}

fn make_binary(name: &str, size: u64) -> BinaryInfo<TestId> {
    BinaryInfo {
        path: std::path::PathBuf::from(name),
        size,
        identifier: TestId(name.into()),
    }
}

use db_transport_channel::ChannelSecondaryTransportEnd;

/// Simulate a secondary that sends welcome + cert, then echoes assignments as completions.
async fn fake_secondary(
    secondary_id: String,
    num_workers: u32,
    ram_bytes: u64,
    mut incoming_from_primary: tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
    outgoing_to_primary: tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
) {
    // Send welcome
    outgoing_to_primary
        .send(DistributedMessage::SecondaryWelcome {
            sender_id: secondary_id.clone(),
            timestamp: 0.0,
            secondary_id: secondary_id.clone(),
            resources: vec![db_comm_api_base::ResourceAmount {
                kind: db_comm_api_base::ResourceKind::memory(),
                amount: ram_bytes,
            }],
            worker_count: num_workers,
            hostname: "test-host".into(),
        })
        .unwrap();

    // Send cert exchange
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

    // Process messages from primary
    while let Some(msg) = incoming_from_primary.recv().await {
        match msg {
            DistributedMessage::PeerInfo { .. } => {
                // No peer connections needed in test
            }
            DistributedMessage::InitialAssignment { zip_files, .. } => {
                // Complete all initially assigned tasks
                for zip_file in &zip_files {
                    for entry in &zip_file.binaries {
                        outgoing_to_primary
                            .send(DistributedMessage::TaskComplete {
                                sender_id: secondary_id.clone(),
                                timestamp: 0.0,
                                secondary_id: secondary_id.clone(),
                                worker_id: 0,
                                task_hash: entry.hash.clone(),
                                result_data: None,
                            })
                            .unwrap();

                        // Request next task
                        outgoing_to_primary
                            .send(DistributedMessage::TaskRequest {
                                sender_id: secondary_id.clone(),
                                timestamp: 0.0,
                                secondary_id: secondary_id.clone(),
                                worker_id: 0,
                                available_resources: vec![db_comm_api_base::ResourceAmount {
                                    kind: db_comm_api_base::ResourceKind::memory(),
                                    amount: ram_bytes,
                                }],
                            })
                            .unwrap();
                    }
                }
            }
            DistributedMessage::TransferComplete { .. } => {}
            DistributedMessage::TaskAssignment { file_hash, .. } => {
                // Complete the assigned task
                outgoing_to_primary
                    .send(DistributedMessage::TaskComplete {
                        sender_id: secondary_id.clone(),
                        timestamp: 0.0,
                        secondary_id: secondary_id.clone(),
                        worker_id: 0,
                        task_hash: file_hash,
                        result_data: None,
                    })
                    .unwrap();

                // Request next task
                outgoing_to_primary
                    .send(DistributedMessage::TaskRequest {
                        sender_id: secondary_id.clone(),
                        timestamp: 0.0,
                        secondary_id: secondary_id.clone(),
                        worker_id: 0,
                        available_resources: vec![db_comm_api_base::ResourceAmount {
                            kind: db_comm_api_base::ResourceKind::memory(),
                            amount: ram_bytes,
                        }],
                    })
                    .unwrap();
            }
            _ => {}
        }
    }
}

fn setup_test(
    num_secondaries: u32,
) -> (
    ChannelSecondaryTransportEnd<TestId>,
    Vec<(
        String,
        tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
        tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
    )>,
) {
    let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
    let mut outgoing = HashMap::new();
    let mut secondary_ends = Vec::new();

    for i in 0..num_secondaries {
        let id = format!("sec-{i}");
        let (to_sec_tx, to_sec_rx) = tokio_mpsc::unbounded_channel();
        outgoing.insert(id.clone(), to_sec_tx);
        secondary_ends.push((id, to_sec_rx, incoming_tx.clone()));
    }

    (ChannelSecondaryTransportEnd { outgoing, incoming_rx }, secondary_ends)
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

use db_comm_api_base::{MessageReceiver, MessageSender};
use db_transport_channel::ChannelPrimaryTransportEnd;
use crate::secondary::{SecondaryConfig, SecondaryCoordinator};

/// No-op peer transport for tests.
struct NoPeers;
impl<I: Identifier> PeerTransport<I> for NoPeers {
    async fn broadcast(&mut self, _msg: DistributedMessage<I>) -> Result<(), String> { Ok(()) }
    async fn send_to_peer(&mut self, _peer_id: &str, _msg: DistributedMessage<I>) -> Result<(), String> { Ok(()) }
    async fn recv_peer(&mut self) -> Option<DistributedMessage<I>> { std::future::pending().await }
    fn try_recv_peer(&mut self) -> Option<DistributedMessage<I>> { None }
    fn peer_count(&self) -> usize { 0 }
    async fn connect_to_peers(&mut self, _peers: &[db_primary_secondary_comm::PeerConnectionInfo]) {}
}

/// Factory that spawns fake workers via channel transport.
struct FakeWorkerFactory;
impl WorkerFactory<ChannelManagerEnd> for FakeWorkerFactory {
    fn spawn_worker(
        &mut self,
        _worker_id: u32,
    ) -> Result<(ChannelManagerEnd, Option<u32>), String> {
        let (manager_end, runner_end) = channel_pair();
        tokio::task::spawn_local(async move {
            let mut runner = runner_end;
            let _ = runner.send(Response::Ready).await;
            loop {
                match MessageReceiver::<Command>::recv(&mut runner).await {
                    Some(Command::Stop) => break,
                    Some(Command::ProcessTask { .. }) => {
                        let _ = runner
                            .send(Response::Done {
                                result_data: None,
                            })
                            .await;
                    }
                    None => break,
                }
            }
        });
        Ok((manager_end, None))
    }
}

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
