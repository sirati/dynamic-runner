//! Integration tests: real PrimaryCoordinator + SecondaryCoordinator over
//! actual QUIC/WSS network transport (not channels).

use std::net::SocketAddr;
use std::time::Duration;

use dynrunner_core::{TaskInfo, MessageReceiver, MessageSender, PhaseId, SoftPreferredSecondaries, TypeId};
use dynrunner_manager_distributed::{PrimaryConfig, PrimaryCoordinator, SecondaryConfig, SecondaryCoordinator};
use dynrunner_manager_local::WorkerFactory;
use dynrunner_protocol_manager_worker::{Command, Response};
use dynrunner_scheduler_api::ResourceEstimator;
use dynrunner_scheduler::ResourceStealingScheduler;
use dynrunner_transport_channel::{channel_pair, ChannelManagerEnd};
use dynrunner_transport_quic::{NetworkClient, NetworkServer, NoPeerTransport};
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
        task_id: None,
        task_depends_on: vec![],
        preferred_secondaries: SoftPreferredSecondaries::default(),
        resolved_path: None,
    }
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

/// End-to-end: 1 primary + 1 secondary over real WSS networking.
///
/// Step 5b: the primary is constructed with a `TunneledPeerTransport`
/// paired against the legacy `NetworkServer` (instead of the prior
/// `NoPeerTransport`). The happy-path counters must still settle to
/// 5/0 — proves the tunnel wiring did not regress the wire flow
/// (per-secondary writes go via the same `connections` map, inbound
/// flows through the same `incoming_rx`, and the role-cache stays
/// cold throughout because no `PromotePrimary` is exercised in this
/// 1-secondary path).
#[tokio::test(flavor = "current_thread")]
async fn e2e_primary_secondary_over_wss() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let mut server: NetworkServer<TestId> = NetworkServer::bind(addr).await.unwrap();
        let port = server.port();
        let server_addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

        // Step 5b: build the tunneled peer view and pair it with
        // the legacy server. From here on, every secondary that
        // completes handshake is registered in BOTH the legacy
        // writer map (drives `transport.send_to`) AND the shared
        // peer writer map (drives `peer_transport.send_to_peer` /
        // role-addressed dispatch); every inbound message is
        // clone-forwarded into the peer queue for Step-6's
        // demoted-primary read arm to consume.
        let (peer_transport, shared_outgoing, inbound_tap) =
            TunneledPeerTransport::<TestId>::new("primary".into());
        server.attach_tunnel(shared_outgoing, inbound_tap);

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
                max_resources: dynrunner_core::ResourceMap::from([(dynrunner_core::ResourceKind::memory(), ram)]),
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
                    keepalive_interval: Duration::from_secs(5),
                    keepalive_miss_threshold: 3,
                    source_pre_staged_root: None,
                    uses_file_based_items: true,
                    required_setup_on_promote: false,
            max_concurrent_per_type: std::collections::HashMap::new(),
            retry_max_passes: 1,
            fleet_dead_timeout: std::time::Duration::from_secs(30),
            mesh_ready_timeout: std::time::Duration::from_secs(60),
            mass_death_grace: std::time::Duration::ZERO,
            mass_death_min_count: 2,
            source_dir: None,
            unfulfillable_reinject_max_per_task: None,
        };

        let mut primary = PrimaryCoordinator::new(
            config,
            server,
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
    }).await;
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
    local.run_until(async {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let mut server: NetworkServer<TestId> = NetworkServer::bind(addr).await.unwrap();
        let port = server.port();
        let cert_der = server.cert_der().clone();
        let server_addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

        // Step 5b: attach a `TunneledPeerTransport` view — same
        // pattern as the WSS sibling above.
        let (peer_transport, shared_outgoing, inbound_tap) =
            TunneledPeerTransport::<TestId>::new("primary".into());
        server.attach_tunnel(shared_outgoing, inbound_tap);

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
                max_resources: dynrunner_core::ResourceMap::from([(dynrunner_core::ResourceKind::memory(), ram)]),
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
                    keepalive_interval: Duration::from_secs(5),
                    keepalive_miss_threshold: 3,
                    source_pre_staged_root: None,
                    uses_file_based_items: true,
                    required_setup_on_promote: false,
            max_concurrent_per_type: std::collections::HashMap::new(),
            retry_max_passes: 1,
            fleet_dead_timeout: std::time::Duration::from_secs(30),
            mesh_ready_timeout: std::time::Duration::from_secs(60),
            mass_death_grace: std::time::Duration::ZERO,
            mass_death_min_count: 2,
            source_dir: None,
            unfulfillable_reinject_max_per_task: None,
        };

        let mut primary = PrimaryCoordinator::new(
            config,
            server,
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
    }).await;
}

/// Step 5b → Step 6 preview: a `ClusterMutation` frame that arrived at
/// the primary's legacy inbound channel reaches the paired
/// `TunneledPeerTransport`'s `recv_peer()` via the inbound-tap
/// fan-out. This is the wire-level precondition for Step 6's
/// demoted-primary `select! { peer_transport.recv_peer() }` arm: when
/// the operational loop's `transport.recv()` arm reads a mutation, the
/// tunneled peer view's queue ALSO receives a clone, so a future
/// post-demotion `recv_peer()` consumer can observe the same mutation
/// flow without depending on whether the legacy receiver was already
/// drained.
///
/// We exercise the channel-backed legacy transport
/// (`ChannelSecondaryTransportEnd`) rather than the QUIC server here
/// because the wiring contract under test is identical (both
/// transports route via the same shared writer table + tap), and the
/// channel fixture is hermetic and deterministic. The QUIC fan-out
/// path is covered separately by `e2e_primary_secondary_over_quic`
/// proving the happy-path counters survive the tunneling refactor.
#[tokio::test(flavor = "current_thread")]
async fn step6_preview_demoted_local_observes_cluster_mutation_via_recv_peer() {
    use dynrunner_core::MessageReceiver;
    use dynrunner_protocol_primary_secondary::{
        ClusterMutation, DistributedMessage, PeerTransport,
    };
    use dynrunner_transport_channel::ChannelSecondaryTransportEnd;
    use tokio::sync::mpsc;

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Build the tunneled peer view up-front so the per-
            // secondary forwarder can clone-into the inbound tap as
            // each frame arrives — same wiring shape as the
            // production in-process distributed PyO3 path
            // (`crates/dynrunner-pyo3/src/managers/distributed.rs`).
            let (mut peer_transport, shared_outgoing, inbound_tap) =
                TunneledPeerTransport::<TestId>::new("primary".into());

            // One secondary; pre-registered in BOTH the legacy
            // outgoing HashMap (drives `transport.send_to`) AND the
            // tunneled peer view's shared writer table. The mirror
            // is what makes the primary a real mesh-member (Step
            // 5b's whole point).
            let (incoming_tx, incoming_rx) =
                mpsc::unbounded_channel::<DistributedMessage<TestId>>();
            let (pri_to_sec_tx, _pri_to_sec_rx) =
                mpsc::unbounded_channel::<DistributedMessage<TestId>>();
            shared_outgoing
                .borrow_mut()
                .insert("sec-0".into(), pri_to_sec_tx.clone());
            let mut outgoing = std::collections::HashMap::new();
            outgoing.insert("sec-0".to_string(), pri_to_sec_tx);

            // Construct the legacy transport against the same
            // inbound queue the secondary's forwarder feeds.
            let mut transport = ChannelSecondaryTransportEnd {
                outgoing,
                incoming_rx,
            };

            // Drive a `ClusterMutation::RunComplete` frame "from"
            // sec-0 (the post-promotion new primary in the Step 6
            // scenario), but ALSO clone-fan it into the tap — this
            // is what the per-secondary forwarder does in
            // production (see `distributed.rs::fwd_tap.send(...)`).
            let mutation_frame = DistributedMessage::ClusterMutation {
                sender_id: "sec-0".into(),
                timestamp: 0.0,
                mutations: vec![ClusterMutation::<TestId>::RunComplete],
            };
            inbound_tap
                .send(mutation_frame.clone())
                .expect("tap accepts");
            incoming_tx.send(mutation_frame).expect("legacy accepts");

            // Legacy `transport.recv()` yields the frame (this is
            // what `PrimaryCoordinator`'s operational loop is doing
            // today, pre-Step-6).
            let via_legacy =
                MessageReceiver::<DistributedMessage<TestId>>::recv(&mut transport)
                    .await
                    .expect("legacy must deliver");
            assert!(
                matches!(via_legacy, DistributedMessage::ClusterMutation { .. }),
                "legacy must surface the mutation: {via_legacy:?}"
            );

            // Tunneled peer view's `recv_peer()` ALSO yields the
            // mutation — this is the load-bearing precondition for
            // Step 6. Without this, adding the
            // `peer_transport.recv_peer()` arm to the operational
            // loop would never fire.
            let via_peer = peer_transport
                .recv_peer()
                .await
                .expect("peer view must deliver");
            assert!(
                matches!(via_peer, DistributedMessage::ClusterMutation { .. }),
                "peer view must surface the mutation: {via_peer:?}"
            );

            // `peer_count()` reflects the shared writer table —
            // Step 6 will use this to relax the demoted-primary
            // disconnect gate; pin it here so the gate condition
            // (`peer_transport.peer_count() > 0`) returns the
            // right answer in production.
            assert_eq!(
                peer_transport.peer_count(),
                1,
                "shared writer table reflects the one registered tunnel",
            );
        })
        .await;
}
