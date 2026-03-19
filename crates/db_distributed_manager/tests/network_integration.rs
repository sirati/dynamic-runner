//! Integration tests: real PrimaryCoordinator + SecondaryCoordinator over
//! actual QUIC/WSS network transport (not channels).

use std::net::SocketAddr;
use std::time::Duration;

use db_comm_api_base::{BinaryInfo, MessageReceiver, MessageSender};
use db_distributed_manager::{PrimaryConfig, PrimaryCoordinator, SecondaryConfig, SecondaryCoordinator};
use db_local_manager::WorkerFactory;
use db_manager_runner_comm::{Command, Response};
use db_scheduler_api::ResourceEstimator;
use db_scheduler_impl::ResourceStealingScheduler;
use db_transport_channel::{channel_pair, ChannelManagerEnd};
use db_transport_quic::{NetworkClient, NetworkServer, NoPeerTransport};
use serde::{Deserialize, Serialize};

/// Test identifier that can be flattened by serde (must be a struct with named
/// fields, not a newtype, because `DistributedBinaryInfo` uses `#[serde(flatten)]`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct TestId {
    name: String,
}

#[derive(Clone)]
struct FixedEstimator(u64);
impl ResourceEstimator for FixedEstimator {
    fn estimate(&self, _size: u64) -> db_comm_api_base::ResourceMap {
        db_comm_api_base::ResourceMap::from([(db_comm_api_base::ResourceKind::Memory, self.0)])
    }
}

fn make_binary(name: &str, size: u64) -> BinaryInfo<TestId> {
    BinaryInfo {
        path: std::path::PathBuf::from(name),
        size,
        identifier: TestId { name: name.into() },
    }
}

/// Factory that spawns fake workers via channel transport.
struct FakeWorkerFactory;
impl WorkerFactory<ChannelManagerEnd> for FakeWorkerFactory {
    fn spawn_worker(&mut self, _worker_id: u32) -> (ChannelManagerEnd, Option<u32>) {
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
        (manager_end, None)
    }
}

/// End-to-end: 1 primary + 1 secondary over real WSS networking.
#[tokio::test(flavor = "current_thread")]
async fn e2e_primary_secondary_over_wss() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server: NetworkServer<TestId> = NetworkServer::bind(addr).await.unwrap();
        let port = server.port();
        let server_addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

        let secondary_id = "sec-0".to_string();
        let ram = 1024 * 1024 * 1024u64;

        // Spawn secondary on a separate task, connecting via WSS
        let sec_id = secondary_id.clone();
        let sec_handle = tokio::task::spawn_local(async move {
            let client = NetworkClient::connect_wss_only(server_addr)
                .await
                .expect("WSS connect failed");

            let config = SecondaryConfig {
                secondary_id: sec_id,
                num_workers: 2,
                max_resources: db_comm_api_base::ResourceMap::from([(db_comm_api_base::ResourceKind::Memory, ram)]),
                hostname: "test-host".into(),
                keepalive_interval: Duration::from_secs(60),
                src_network: None,
                src_tmp: None,
                peer_timeout: Duration::from_secs(120),
            };

            let mut secondary: SecondaryCoordinator<_, _, ChannelManagerEnd, _, _, TestId> =
                SecondaryCoordinator::new(
                    config,
                    client,
                    NoPeerTransport,
                    ResourceStealingScheduler::memory(),
                    FixedEstimator(100),
                );
            let mut factory = FakeWorkerFactory;
            secondary.run(&mut factory).await.unwrap();
            secondary.completed_count()
        });

        // Primary coordinator
        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: 1,
            connect_timeout: Duration::from_secs(10),
            peer_timeout: Duration::from_secs(10),
        };

        let mut primary = PrimaryCoordinator::new(
            config,
            server,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        let binaries: Vec<BinaryInfo<TestId>> = (0..5)
            .map(|i| make_binary(&format!("bin_{i}"), 50 + i * 10))
            .collect();

        primary.run(binaries).await.unwrap();

        let completed = primary.completed_count();
        let failed = primary.failed_count();

        // Drop primary to close transport, allowing secondary to exit
        drop(primary);

        let sec_completed = sec_handle.await.unwrap();

        assert_eq!(completed, 5, "primary should see 5 completed");
        assert_eq!(failed, 0, "no failures expected");
        assert_eq!(sec_completed, 5, "secondary should see 5 completed");
    }).await;
}

/// End-to-end: 1 primary + 1 secondary over real QUIC networking.
#[tokio::test(flavor = "current_thread")]
async fn e2e_primary_secondary_over_quic() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server: NetworkServer<TestId> = NetworkServer::bind(addr).await.unwrap();
        let port = server.port();
        let cert_der = server.cert_der().clone();
        let server_addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

        let secondary_id = "sec-0".to_string();
        let ram = 1024 * 1024 * 1024u64;

        // Spawn secondary connecting via QUIC
        let sec_id = secondary_id.clone();
        let sec_handle = tokio::task::spawn_local(async move {
            let client = NetworkClient::connect(
                server_addr,
                "primary",
                &cert_der,
                Duration::from_secs(5),
            )
            .await
            .expect("QUIC connect failed");

            // Should have used QUIC
            assert!(matches!(client, NetworkClient::Quic(_)));

            let config = SecondaryConfig {
                secondary_id: sec_id,
                num_workers: 2,
                max_resources: db_comm_api_base::ResourceMap::from([(db_comm_api_base::ResourceKind::Memory, ram)]),
                hostname: "test-host".into(),
                keepalive_interval: Duration::from_secs(60),
                src_network: None,
                src_tmp: None,
                peer_timeout: Duration::from_secs(120),
            };

            let mut secondary: SecondaryCoordinator<_, _, ChannelManagerEnd, _, _, TestId> =
                SecondaryCoordinator::new(
                    config,
                    client,
                    NoPeerTransport,
                    ResourceStealingScheduler::memory(),
                    FixedEstimator(100),
                );
            let mut factory = FakeWorkerFactory;
            secondary.run(&mut factory).await.unwrap();
            secondary.completed_count()
        });

        // Primary coordinator
        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: 1,
            connect_timeout: Duration::from_secs(10),
            peer_timeout: Duration::from_secs(10),
        };

        let mut primary = PrimaryCoordinator::new(
            config,
            server,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        let binaries: Vec<BinaryInfo<TestId>> = (0..5)
            .map(|i| make_binary(&format!("bin_{i}"), 50 + i * 10))
            .collect();

        primary.run(binaries).await.unwrap();

        let completed = primary.completed_count();
        let failed = primary.failed_count();

        drop(primary);

        let sec_completed = sec_handle.await.unwrap();

        assert_eq!(completed, 5, "primary should see 5 completed");
        assert_eq!(failed, 0, "no failures expected");
        assert_eq!(sec_completed, 5, "secondary should see 5 completed");
    }).await;
}
