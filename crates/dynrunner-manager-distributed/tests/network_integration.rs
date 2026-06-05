//! Integration tests: real PrimaryCoordinator + SecondaryCoordinator over
//! actual QUIC/WSS network transport (not channels).

use std::net::SocketAddr;
use std::time::Duration;

use dynrunner_core::{
    MessageReceiver, MessageSender, PhaseId, SoftPreferredSecondaries, TaskInfo, TypeId,
};
use dynrunner_manager_distributed::{
    PrimaryConfig, PrimaryCoordinator, SecondaryConfig, SecondaryCoordinator,
};
use dynrunner_manager_local::WorkerFactory;
use dynrunner_protocol_manager_worker::{Command, Response};
use dynrunner_scheduler::ResourceStealingScheduler;
use dynrunner_scheduler_api::ResourceEstimator;
use dynrunner_transport_channel::{ChannelManagerEnd, channel_pair};
use dynrunner_transport_quic::{NetworkClient, NetworkServer, PeerNetwork};
use dynrunner_transport_tunnel::TunneledPeerTransport;
use serde::{Deserialize, Serialize};

/// Test identifier that can be flattened by serde (must be a struct with named
/// fields, not a newtype, because `DistributedBinaryInfo` uses `#[serde(flatten)]`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct TestId {
    name: String,
}

#[derive(Clone)]
struct FixedEstimator(u64);
impl ResourceEstimator<TestId> for FixedEstimator {
    fn estimate(&self, _task: &TaskInfo<TestId>) -> dynrunner_core::ResourceMap {
        dynrunner_core::ResourceMap::from([(dynrunner_core::ResourceKind::memory(), self.0)])
    }
}

fn make_binary(name: &str, size: u64) -> TaskInfo<TestId> {
    // Absolute path (despite no real file backing it) — the integration
    // test fixtures don't configure src_network, and dispatch.rs's
    // unresolvable-task guard fail-loud-rejects relative local_paths
    // when the secondary has no staging dir. Tests that only exercise
    // the dispatch wire flow (fake worker doesn't actually open the
    // file) are happy with any absolute path; using `/tmp/<name>`
    // keeps the fixture trivial and survives that guard.
    TaskInfo {
        path: std::path::PathBuf::from(format!("/tmp/{name}")),
        size,
        identifier: TestId { name: name.into() },
        phase_id: PhaseId::from("default"),
        type_id: TypeId::from("default"),
        affinity_id: None,
        payload: serde_json::Value::Null,
        task_id: name.into(),
        task_depends_on: vec![],
        preferred_secondaries: SoftPreferredSecondaries::default(),
        preferred_version: Default::default(),
        resolved_path: None,
    }
}

/// Factory that spawns fake workers via channel transport.
struct FakeWorkerFactory;
impl WorkerFactory<ChannelManagerEnd> for FakeWorkerFactory {
    fn spawn_worker(
        &mut self,
        _worker_id: u32,
        _subcgroup: Option<&dynrunner_manager_local::cgroup::SubcgroupHandle>,
    ) -> Result<(ChannelManagerEnd, Option<u32>), String> {
        let (manager_end, runner_end) = channel_pair();
        tokio::task::spawn_local(async move {
            let mut runner = runner_end;
            let _ = runner.send(Response::Ready).await;
            loop {
                match MessageReceiver::<Command>::recv(&mut runner).await {
                    Some(Command::Stop) => break,
                    Some(Command::ProcessTask { .. }) => {
                        let _ = runner.send(Response::Done { result_data: None }).await;
                    }
                    None => break,
                }
            }
        });
        Ok((manager_end, None))
    }
}

/// End-to-end: 1 primary + 1 secondary over real WSS networking.
///
/// Step 5b: the primary is constructed with a `TunneledPeerTransport`
/// paired against the legacy `NetworkServer` (instead of the prior
/// `NoPeerTransport`). The happy-path counters must still settle to
/// 5/0 — proves the tunnel wiring did not regress the wire flow
/// (per-secondary writes go via the same `connections` map, inbound
/// flows through the same `incoming_rx`, and the role-cache stays
/// cold throughout because no `PrimaryChanged` re-point is exercised in
/// this 1-secondary path).
#[tokio::test(flavor = "current_thread")]
async fn e2e_primary_secondary_over_wss() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            // Post-collapse: build the unified `TunneledPeerTransport`
            // first so the network server's accept loops feed its inbound +
            // registration sinks directly. The transport OWNS the inbound
            // demux; the server shrinks to bind + accept-loops. The primary
            // holds this transport as its single `Tr`.
            let (peer_transport, _shared_outgoing, inbound, registration) =
                TunneledPeerTransport::<TestId>::new("primary".into());
            let server: NetworkServer = NetworkServer::bind::<TestId>(addr, inbound, registration)
                .await
                .unwrap();
            let port = server.port();
            let _server = server;
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
                    max_resources: dynrunner_core::ResourceMap::from([(
                        dynrunner_core::ResourceKind::memory(),
                        ram,
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
                    can_be_primary: false,
                    resource_check_interval: Duration::from_millis(100),
                    log_oom_watcher: false,
                    promoted_primary_quiesce_grace: Duration::from_millis(100),
                    unfulfillable_reinject_max_per_task: None,
                    mem_manager_reserved_bytes: None,
                    output_dir: None,
                    memuse_log_path: None,
                };

                // Fold the bootstrap wire into a real mesh: the primary
                // becomes a mesh peer reached by id over the SAME dialed
                // connection (both directions), with no separate uplink
                // leg. The secondary holds the `PeerNetwork` directly —
                // exactly the production secondary path.
                let mut peer_network = PeerNetwork::<TestId>::start(&config.secondary_id)
                    .await
                    .expect("peer network start");
                peer_network.register_primary_link("primary".to_string(), client);
                let mut secondary: SecondaryCoordinator<_, ChannelManagerEnd, _, _, TestId> =
                    SecondaryCoordinator::new(
                        config,
                        peer_network,
                        ResourceStealingScheduler::memory(),
                        FixedEstimator(100),
                    );
                // Tell the egress edge which peer-id the dialled bootstrap
                // wire reaches, so `Destination::Primary` resolves to it
                // while the role table is cold (the setup window before
                // any `PrimaryChanged`). This is what makes the primary
                // reachable cold AND warm — the cold-primary resolution
                // these tests pin.
                secondary.set_bootstrap_primary_id("primary".to_string());
                let mut factory = FakeWorkerFactory;
                secondary.run(&mut factory).await.unwrap();
                secondary.completed_count()
            });

            // Primary coordinator
            let config = PrimaryConfig {
                connect_timeout: Duration::from_secs(10),
                peer_timeout: Duration::from_secs(10),
                ..PrimaryConfig::default()
            };

            let mut primary = PrimaryCoordinator::new(
                config,
                peer_transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let binaries: Vec<TaskInfo<TestId>> = (0..5)
                .map(|i| make_binary(&format!("bin_{i}"), 50 + i * 10))
                .collect();

            primary
                .run(
                    binaries,
                    std::collections::HashMap::new(),
                    Box::new(|_| {}),
                    Box::new(|_, _, _| {}),
                )
                .await
                .unwrap();

            let completed = primary.completed_count();
            let failed = primary.failed_count();

            // Drop primary to close transport, allowing secondary to exit
            drop(primary);

            let sec_completed = sec_handle.await.unwrap();

            assert_eq!(completed, 5, "primary should see 5 completed");
            assert_eq!(failed, 0, "no failures expected");
            assert_eq!(sec_completed, 5, "secondary should see 5 completed");
        })
        .await;
}

/// End-to-end: 1 primary + 1 secondary over real QUIC networking.
///
/// Same Step-5b shape as the WSS sibling: primary's `peer_transport`
/// is a `TunneledPeerTransport` paired against the `NetworkServer`,
/// not `NoPeerTransport`. Pins the QUIC path's tunneled-peer wiring
/// matches the WSS path (the accept loops both register through the
/// same `new_conn_tx` channel; `drain_new_connections` mirrors into
/// the shared writer table for both).
#[tokio::test(flavor = "current_thread")]
async fn e2e_primary_secondary_over_quic() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            // Same Shape-A construction as the WSS sibling: transport first,
            // then bind the server with its inbound + registration sinks.
            let (peer_transport, _shared_outgoing, inbound, registration) =
                TunneledPeerTransport::<TestId>::new("primary".into());
            let server: NetworkServer = NetworkServer::bind::<TestId>(addr, inbound, registration)
                .await
                .unwrap();
            let port = server.port();
            let cert_der = server.cert_der().clone();
            let _server = server;
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
                    max_resources: dynrunner_core::ResourceMap::from([(
                        dynrunner_core::ResourceKind::memory(),
                        ram,
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
                    can_be_primary: false,
                    resource_check_interval: Duration::from_millis(100),
                    log_oom_watcher: false,
                    promoted_primary_quiesce_grace: Duration::from_millis(100),
                    unfulfillable_reinject_max_per_task: None,
                    mem_manager_reserved_bytes: None,
                    output_dir: None,
                    memuse_log_path: None,
                };

                // Fold the bootstrap wire into a real mesh: the primary
                // becomes a mesh peer reached by id over the SAME dialed
                // connection (both directions), with no separate uplink
                // leg. The secondary holds the `PeerNetwork` directly —
                // exactly the production secondary path.
                let mut peer_network = PeerNetwork::<TestId>::start(&config.secondary_id)
                    .await
                    .expect("peer network start");
                peer_network.register_primary_link("primary".to_string(), client);
                let mut secondary: SecondaryCoordinator<_, ChannelManagerEnd, _, _, TestId> =
                    SecondaryCoordinator::new(
                        config,
                        peer_network,
                        ResourceStealingScheduler::memory(),
                        FixedEstimator(100),
                    );
                // Tell the egress edge which peer-id the dialled bootstrap
                // wire reaches, so `Destination::Primary` resolves to it
                // while the role table is cold (the setup window before
                // any `PrimaryChanged`). This is what makes the primary
                // reachable cold AND warm — the cold-primary resolution
                // these tests pin.
                secondary.set_bootstrap_primary_id("primary".to_string());
                let mut factory = FakeWorkerFactory;
                secondary.run(&mut factory).await.unwrap();
                secondary.completed_count()
            });

            // Primary coordinator
            let config = PrimaryConfig {
                connect_timeout: Duration::from_secs(10),
                peer_timeout: Duration::from_secs(10),
                ..PrimaryConfig::default()
            };

            let mut primary = PrimaryCoordinator::new(
                config,
                peer_transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let binaries: Vec<TaskInfo<TestId>> = (0..5)
                .map(|i| make_binary(&format!("bin_{i}"), 50 + i * 10))
                .collect();

            primary
                .run(
                    binaries,
                    std::collections::HashMap::new(),
                    Box::new(|_| {}),
                    Box::new(|_, _, _| {}),
                )
                .await
                .unwrap();

            let completed = primary.completed_count();
            let failed = primary.failed_count();

            drop(primary);

            let sec_completed = sec_handle.await.unwrap();

            assert_eq!(completed, 5, "primary should see 5 completed");
            assert_eq!(failed, 0, "no failures expected");
            assert_eq!(sec_completed, 5, "secondary should see 5 completed");
        })
        .await;
}

/// Post-collapse unified-inbound contract: a `ClusterMutation` frame
/// fed into the unified `TunneledPeerTransport`'s SINGLE inbound sink
/// surfaces via `recv_peer()` — the one inbound path the operational
/// loop now reads. This pins the wire-level guarantee the
/// recv-arm unification depends on: there is no separate legacy
/// `transport.recv()` consumer and no fan-out tap; the inbound sink
/// (fed by the accept loops / in-process forwarder) IS the stream
/// `recv_peer` demuxes. `peer_count()` reflects the shared writer
/// table the registration path populates.
#[tokio::test(flavor = "current_thread")]
async fn unified_inbound_surfaces_cluster_mutation_via_recv_peer() {
    use dynrunner_protocol_primary_secondary::{
        ClusterMutation, DistributedMessage, PeerTransport,
    };
    use tokio::sync::mpsc;

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // The unified transport owns the real inbound stream; the
            // `inbound` sink is what an accept-loop reader task (or the
            // in-process per-secondary forwarder) pushes frames into.
            // The registration sink is dropped here — we register the
            // one writer directly into the shared `outgoing` table (the
            // in-process / test path).
            let (mut peer_transport, shared_outgoing, inbound, _registration) =
                TunneledPeerTransport::<TestId>::new("primary".into());

            let (pri_to_sec_tx, _pri_to_sec_rx) =
                mpsc::unbounded_channel::<DistributedMessage<TestId>>();
            shared_outgoing
                .borrow_mut()
                .insert("sec-0".into(), pri_to_sec_tx);

            // Feed a `ClusterMutation::RunComplete` frame "from" sec-0
            // (the post-promotion new primary scenario) into the single
            // inbound sink.
            let mutation_frame = DistributedMessage::ClusterMutation {
                sender_id: "sec-0".into(),
                timestamp: 0.0,
                mutations: vec![ClusterMutation::<TestId>::RunComplete],
            };
            inbound.send(mutation_frame).expect("inbound accepts");

            // `recv_peer()` — the operational loop's SOLE inbound arm —
            // surfaces the mutation. This is the load-bearing path the
            // recv-arm unification keeps (the deleted legacy arm + tap
            // are gone).
            let via_peer = peer_transport
                .recv_peer()
                .await
                .expect("unified inbound must deliver");
            assert!(
                matches!(via_peer, DistributedMessage::ClusterMutation { .. }),
                "recv_peer must surface the mutation: {via_peer:?}"
            );

            // `peer_count()` reflects the shared writer table — the
            // mesh-health read the operational loop / watchdog use.
            assert_eq!(
                peer_transport.peer_count(),
                1,
                "shared writer table reflects the one registered tunnel",
            );
        })
        .await;
}
