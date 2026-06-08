//! Integration tests: real PrimaryCoordinator + SecondaryCoordinator over
//! actual QUIC/WSS network transport (not channels).

use std::net::SocketAddr;
use std::time::Duration;

use dynrunner_core::{
    MessageReceiver, MessageSender, PhaseId, SoftPreferredSecondaries, TaskInfo, TypeId,
};
use dynrunner_manager_distributed::cluster_state::ClusterState;
use dynrunner_manager_distributed::{
    PrimaryConfig, PrimaryCoordinator, SecondaryConfig, SecondaryCoordinator, compute_task_hash,
};
use dynrunner_protocol_primary_secondary::ClusterMutation;
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

/// Drive a real `SecondaryCoordinator` over ANY `Tr: PeerTransport` against the
/// PRODUCTION mesh-pump to its clean exit.
///
/// Returns nothing: under the operational `PromotionSnapshot` primary (the
/// transport-flow shape these tests now use — a `ColdStart` primary would
/// relocate away, never running the dispatch loop), the fresh-connect secondary
/// never receives the cold-seed `TaskAdded` broadcast (it rides the snapshot /
/// anti-entropy in production), so its CRDT-mirror `completed_count` is
/// legitimately 0 and is NOT a meaningful signal here. The transport-flow proof
/// is the PRIMARY-side `completed == 5`: the primary can only observe 5
/// completions if the secondary's workers processed the dispatched tasks AND
/// the `TaskComplete` reports traversed the real QUIC/WSS transport back — a
/// dropped-message / transport bug fails it. The secondary mirror's
/// CRDT-convergence-via-broadcast is a transport-AGNOSTIC property covered by
/// the mpsc relocate convergence test + the consumer live SLURM gate.
///
/// The real-`Node` e2e harness for the network tests: the coordinator holds
/// only a `MeshClient`/`RoleInbox`; the pump owns the `Mesh` over the real
/// network transport and concurrently drains egress + routes inbound.
async fn run_secondary_over<Tr>(config: SecondaryConfig, transport: Tr)
where
    Tr: dynrunner_protocol_primary_secondary::PeerTransport<TestId> + 'static,
{
    use dynrunner_manager_distributed::process::{LocalRole, Mesh, pump};
    use dynrunner_protocol_primary_secondary::address::PeerId;

    let mut mesh = Mesh::new(transport);
    let id = config.secondary_id.clone();
    let (_slot, client, inbox) =
        mesh.register_local_role(LocalRole::Secondary, PeerId::from(id.as_str()));
    let mut secondary = SecondaryCoordinator::new(
        config,
        client,
        inbox,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    secondary.set_bootstrap_primary_id("setup".to_string());

    // Publish the live membership BEFORE the coordinator's first egress —
    // mirroring production `Node::run`, where the pump's entry
    // `publish_membership()` (synchronous-before-await) precedes any
    // coordinator's first `has_peer`-gated send. The secondary's first egress
    // is `send_welcome` → `send_to(Destination::Primary)`, gated on
    // `client.has_peer("primary")` (which reads the pump-published
    // `MembershipView`, EMPTY until the first publish). The `run`/`run_pump`
    // arms below are ONE unbiased `select!`; without this pre-publish, a
    // `run`-first poll order reads the empty view and no-routes the Welcome.
    mesh.publish_membership();

    let (_control, control_rx) = pump::control_channel::<TestId>();
    let pump_fut = pump::run_pump(mesh, control_rx);
    tokio::pin!(pump_fut);

    {
        let mut factory = FakeWorkerFactory;
        let run = secondary.run(&mut factory);
        tokio::pin!(run);
        tokio::select! {
            r = &mut run => { r.unwrap(); }
            _ = &mut pump_fut => {}
        }
    }
}

/// Run a real OPERATIONAL `PrimaryCoordinator` over ANY `Tr: PeerTransport`
/// against the PRODUCTION mesh-pump, returning `(completed, failed)`. Mirrors
/// `run_secondary_over` for the primary side of the network e2e.
///
/// Under mesh-always a `ColdStart` primary is a SETUP PEER that RELOCATES (it
/// would never run the dispatch loop itself — the whole point of these tests).
/// These are TRANSPORT-FLOW proofs: their value is that the real QUIC/WSS
/// transport carries the dispatch/assign/complete/keepalive wire-flow. The
/// honest seed for "an operational primary dispatching over real transport" is
/// `PromotionSnapshot` (≡ the relocated target / failover-promoted primary —
/// `BootstrapRole::PromotedDestination`, runs the operational loop in place,
/// `.run()` works since it never relocates). So we pre-seed a populated
/// `ClusterState` snapshot from `binaries` (the corpus the target would have
/// inherited) and seed the coordinator from it before the run. Real-transport
/// RELOCATION is validated by the consumer live SLURM gate, not here; the
/// relocate→demote→promote machinery itself is proven over the mpsc peer_mesh
/// (`node_gates`), since relocation speaks `Mesh`/`PeerTransport`, never QUIC.
async fn run_primary_over<Tr>(
    config: PrimaryConfig,
    transport: Tr,
    binaries: Vec<TaskInfo<TestId>>,
) -> (usize, usize)
where
    Tr: dynrunner_protocol_primary_secondary::PeerTransport<TestId> + 'static,
{
    use dynrunner_manager_distributed::process::{LocalRole, Mesh, SeedSource, pump};
    use dynrunner_protocol_primary_secondary::address::PeerId;

    // Build the inherited-ledger snapshot the operational (promotion-snapshot)
    // primary resumes from: the phase graph + one Pending `TaskAdded` per
    // binary, exactly what a relocate target would have inherited.
    let snapshot = {
        let mut cs: ClusterState<TestId> = ClusterState::new();
        cs.apply(ClusterMutation::PhaseDepsSet {
            deps: std::collections::HashMap::new(),
        });
        for task in &binaries {
            cs.apply(ClusterMutation::TaskAdded {
                hash: compute_task_hash(task),
                task: task.clone(),
            });
        }
        cs.snapshot()
    };

    let mut mesh = Mesh::new(transport);
    let (_slot, client, inbox) =
        mesh.register_local_role(LocalRole::Primary, PeerId::from(config.node_id.as_str()));
    let (_demote_tx, demote_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut primary = PrimaryCoordinator::new(
        config,
        client,
        inbox,
        demote_rx,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    // Seed from the inherited snapshot so the `PromotionSnapshot` run resumes
    // on the populated ledger (hydrate rebuilds the pool + total_tasks).
    primary.seed_from_promotion_snapshot(snapshot);

    // Publish the live membership BEFORE the coordinator's first egress —
    // mirroring production `Node::run`, where the pump's entry
    // `publish_membership()` precedes any coordinator's first `has_peer`-gated
    // send. The `run`/`run_pump` arms below are ONE unbiased `select!`; without
    // this pre-publish, a `run`-first poll order reads an empty `MembershipView`
    // before the pump's own entry publish runs.
    mesh.publish_membership();

    let (_control, control_rx) = pump::control_channel::<TestId>();
    let pump_fut = pump::run_pump(mesh, control_rx);
    tokio::pin!(pump_fut);

    {
        let run = primary.run(
            SeedSource::PromotionSnapshot,
            Box::new(|_| {}),
            Box::new(|_, _, _, _| {}),
        );
        tokio::pin!(run);
        tokio::select! {
            r = &mut run => { r.unwrap(); }
            _ = &mut pump_fut => {}
        }
    }
    (primary.completed_count(), primary.failed_count())
}

/// End-to-end: 1 primary + 1 secondary over real WSS networking.
///
/// The primary is constructed with a `TunneledPeerTransport`
/// paired against the legacy `NetworkServer`. The happy-path counters
/// must settle to
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
                TunneledPeerTransport::<TestId>::new("setup".into());
            let server: NetworkServer = NetworkServer::bind::<TestId>(addr, "setup", inbound, registration)
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
                    forwarded_argv: Vec::new(),
                };

                // Fold the bootstrap wire into a real mesh: the primary
                // becomes a mesh peer reached by id over the SAME dialed
                // connection (both directions), with no separate uplink
                // leg. The secondary holds the `PeerNetwork` directly —
                // exactly the production secondary path.
                let mut peer_network = PeerNetwork::<TestId>::start(&config.secondary_id)
                    .await
                    .expect("peer network start");
                peer_network.register_primary_link("setup".to_string(), client);
                // Drive the real secondary over the real network transport
                // against the production mesh-pump (the cold-primary
                // resolution + bootstrap-link fold these tests pin happen
                // inside `run_secondary_over` → the coordinator's egress).
                run_secondary_over(config, peer_network).await
            });

            // Primary coordinator
            let config = PrimaryConfig {
                connect_timeout: Duration::from_secs(10),
                peer_timeout: Duration::from_secs(10),
                ..PrimaryConfig::default()
            };

            let binaries: Vec<TaskInfo<TestId>> = (0..5)
                .map(|i| make_binary(&format!("bin_{i}"), 50 + i * 10))
                .collect();

            // Drive the real primary over the real network transport against
            // the production mesh-pump. Closing the transport (the pump's
            // teardown) lets the secondary exit.
            let (completed, failed) = run_primary_over(config, peer_transport, binaries).await;

            // Drive the secondary to its clean exit.
            sec_handle.await.unwrap();

            // Transport-flow proof: the primary observed all 5 completions over
            // the real QUIC/WSS transport. This REQUIRES the full round-trip —
            // the primary dispatched TaskAssignments, the secondary's workers
            // processed them, and the `TaskComplete` reports traversed the real
            // transport back — so a dropped-message / transport regression fails
            // it. The secondary's CRDT-mirror count is legitimately 0 under the
            // operational `PromotionSnapshot` primary (no cold-seed re-broadcast
            // to a fresh secondary); mirror-convergence-via-broadcast is the
            // mpsc relocate convergence test's job + the consumer live gate.
            assert_eq!(completed, 5, "primary should see 5 completed over the real transport");
            assert_eq!(failed, 0, "no failures expected");
        })
        .await;
}

/// End-to-end: 1 primary + 1 secondary over real QUIC networking.
///
/// Same shape as the WSS sibling: primary's `peer_transport`
/// is a `TunneledPeerTransport` paired against the `NetworkServer`.
/// Pins the QUIC path's tunneled-peer wiring
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
                TunneledPeerTransport::<TestId>::new("setup".into());
            let server: NetworkServer = NetworkServer::bind::<TestId>(addr, "setup", inbound, registration)
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
                    "setup",
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
                    forwarded_argv: Vec::new(),
                };

                // Fold the bootstrap wire into a real mesh: the primary
                // becomes a mesh peer reached by id over the SAME dialed
                // connection (both directions), with no separate uplink
                // leg. The secondary holds the `PeerNetwork` directly —
                // exactly the production secondary path.
                let mut peer_network = PeerNetwork::<TestId>::start(&config.secondary_id)
                    .await
                    .expect("peer network start");
                peer_network.register_primary_link("setup".to_string(), client);
                // Drive the real secondary over the real network transport
                // against the production mesh-pump (the cold-primary
                // resolution + bootstrap-link fold these tests pin happen
                // inside `run_secondary_over` → the coordinator's egress).
                run_secondary_over(config, peer_network).await
            });

            // Primary coordinator
            let config = PrimaryConfig {
                connect_timeout: Duration::from_secs(10),
                peer_timeout: Duration::from_secs(10),
                ..PrimaryConfig::default()
            };

            let binaries: Vec<TaskInfo<TestId>> = (0..5)
                .map(|i| make_binary(&format!("bin_{i}"), 50 + i * 10))
                .collect();

            // Drive the real primary over the real network transport against
            // the production mesh-pump. Closing the transport (the pump's
            // teardown) lets the secondary exit.
            let (completed, failed) = run_primary_over(config, peer_transport, binaries).await;

            // Drive the secondary to its clean exit.
            sec_handle.await.unwrap();

            // Transport-flow proof: the primary observed all 5 completions over
            // the real QUIC/WSS transport. This REQUIRES the full round-trip —
            // the primary dispatched TaskAssignments, the secondary's workers
            // processed them, and the `TaskComplete` reports traversed the real
            // transport back — so a dropped-message / transport regression fails
            // it. The secondary's CRDT-mirror count is legitimately 0 under the
            // operational `PromotionSnapshot` primary (no cold-seed re-broadcast
            // to a fresh secondary); mirror-convergence-via-broadcast is the
            // mpsc relocate convergence test's job + the consumer live gate.
            assert_eq!(completed, 5, "primary should see 5 completed over the real transport");
            assert_eq!(failed, 0, "no failures expected");
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
                TunneledPeerTransport::<TestId>::new("setup".into());

            let (pri_to_sec_tx, _pri_to_sec_rx) =
                mpsc::unbounded_channel::<DistributedMessage<TestId>>();
            shared_outgoing
                .borrow_mut()
                .insert("sec-0".into(), pri_to_sec_tx);

            // Feed a `ClusterMutation::RunComplete` frame "from" sec-0
            // (the post-promotion new primary scenario) into the single
            // inbound sink.
            let mutation_frame = DistributedMessage::ClusterMutation {
                target: None,
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
                matches!(
                    via_peer,
                    DistributedMessage::ClusterMutation { target: None, .. }
                ),
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
