//! Desktop-shaped late-joiner bootstrap through a TCP local-forward
//! endpoint (the `--gateway` fix's transport-level contract).
//!
//! On a SLURM run dispatched from a local desktop via `--gateway`, the
//! peer-info records carry compute-node-internal addresses. The two
//! tests here pin the two halves of the fix's transport story:
//!
//! - RED / repro (`unreachable_cluster_address_fails_loud_and_bounded`):
//!   a seed whose dial target is unreachable from this host — the
//!   desktop's view of a compute-internal address — makes
//!   `join_running_cluster` fail LOUDLY (`JoinError::NoReachablePeer`)
//!   within its connect budget, never hang. This is the pre-fix
//!   failure shape of `--observer-join-from-peer-info-dir` driven from
//!   a desktop.
//!
//! - GREEN (`join_succeeds_through_tcp_forward_endpoint`): the same
//!   cluster peer joined through a LOCAL TCP forward endpoint (the
//!   in-test stand-in for the `ssh -L <local>:<compute>:<port>
//!   <gateway>` tunnel the local-forward registry spawns in
//!   production) with the seed entry rewritten to `127.0.0.1:<local>`
//!   and the cert CLEARED — a TCP forward cannot carry QUIC/UDP, so
//!   the rewrite seam declares the endpoint WSS-only — completes the
//!   bootstrap snapshot RPC end-to-end. This is the linchpin
//!   assumption of the gateway-mode late-joiner: WSS rides a plain
//!   TCP byte-pipe unchanged.
//!
//! - Production frame shape
//!   (`join_accepts_role_stamped_snapshot_reply`): the Test-1a replay.
//!   A REAL responder replies through its coordinator egress, which
//!   stamps the Phase-C routing target on every frame
//!   (`msg.with_target(reply_destination(..))` →
//!   `Some(Destination::Observer(joiner-id))`) — unlike the canned
//!   `target: None` reply above, which models the pre-one-mesh wire.
//!   The joiner must ACCEPT the stamped reply (and keep skipping the
//!   stamped broadcast gossip interleaved with it). Before the fix the
//!   bootstrap window's accept arm pattern-matched `target: None` and
//!   dropped every production reply as "non-ClusterSnapshot … kind=
//!   ClusterSnapshot" until the budget expired — the asm-tokenizer
//!   Test-1a `JoinError::Timeout`.

use std::time::Duration;

use super::super::PeerNetwork;
use super::TestId;
use dynrunner_protocol_primary_secondary::{
    Destination, DistributedMessage, JoinError, KeepaliveRole, PeerConnectionInfo, PeerId,
    PeerTransport,
};

/// Seed entry shaped like a production v2 `.info` record after
/// `records_to_seed`: cert-less (the production wrapper omits
/// `cert_pem_b64`), single ipv4 candidate.
fn seed_entry(secondary_id: &str, ipv4: &str, port: u16) -> PeerConnectionInfo {
    PeerConnectionInfo {
        secondary_id: secondary_id.into(),
        cert: String::new(),
        ipv4: Some(ipv4.into()),
        ipv6: None,
        port,
        is_observer: false,
        liveness_port: None,
    }
}

/// Bidirectional byte-pipe between two TCP streams — the body of the
/// in-test local-forward. `tokio::io::copy_bidirectional` is exactly
/// what sshd's `direct-tcpip` channel does for an `-L` forward.
async fn proxy_one(inbound: tokio::net::TcpStream, target: std::net::SocketAddr) {
    let Ok(mut outbound) = tokio::net::TcpStream::connect(target).await else {
        return;
    };
    let mut inbound = inbound;
    let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
}

/// RED / repro: the desktop-shaped failure. The seed's address is not
/// reachable from this host (a freshly-bound-then-dropped port — the
/// moral equivalent of a compute-internal IP from the desktop), so the
/// joiner must surface `JoinError::NoReachablePeer` within the connect
/// budget instead of hanging or succeeding.
#[tokio::test(flavor = "current_thread")]
async fn unreachable_cluster_address_fails_loud_and_bounded() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // A port nothing listens on: bind, read the port, drop.
            let dead_port = {
                let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let port = l.local_addr().unwrap().port();
                drop(l);
                port
            };

            let mut joiner: PeerNetwork<TestId> =
                PeerNetwork::start("observer-red", None).await.unwrap();
            let seed = vec![seed_entry("secondary-0", "127.0.0.1", dead_port)];

            let budget = Duration::from_secs(4);
            let started = std::time::Instant::now();
            let result = joiner
                .join_running_cluster(&seed, budget, true, false)
                .await;
            // Loud: the typed no-reachable-peer error, not a hang.
            assert!(
                matches!(result, Err(JoinError::NoReachablePeer)),
                "expected NoReachablePeer for an unreachable cluster address, got {result:?}"
            );
            // Bounded: the connect window is budget/4; allow generous
            // scheduling slack but pin that it didn't soak the full
            // budget (or beyond).
            assert!(
                started.elapsed() < budget,
                "join must fail within the connect budget, took {:?}",
                started.elapsed()
            );
        })
        .await;
}

/// GREEN: the same join through a local TCP forward endpoint with the
/// seed entry rewritten the way the gateway-mode seed builder rewrites
/// it (127.0.0.1 + local port, cert cleared → WSS-only dial). The
/// cluster peer answers the snapshot RPC over the proxied connection,
/// proving the reply path routes back through the forward too.
#[tokio::test(flavor = "current_thread")]
async fn join_succeeds_through_tcp_forward_endpoint() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // The "cluster" peer (a compute secondary in production).
            let mut cluster: PeerNetwork<TestId> =
                PeerNetwork::start("secondary-0", None).await.unwrap();
            let cluster_port = cluster.port();
            let cluster_addr: std::net::SocketAddr =
                format!("127.0.0.1:{cluster_port}").parse().unwrap();

            // The local forward endpoint (production: the ssh -L child's
            // 127.0.0.1:<local_port> listener).
            let forward = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let forward_port = forward.local_addr().unwrap().port();
            tokio::task::spawn_local(async move {
                while let Ok((inbound, _)) = forward.accept().await {
                    tokio::task::spawn_local(proxy_one(inbound, cluster_addr));
                }
            });

            // The cluster peer's responder: answer the first
            // RequestSnapshotStream with a canned single-package stream.
            let responder = tokio::task::spawn_local(async move {
                loop {
                    let Some(msg) = cluster.recv_peer().await else {
                        panic!("cluster peer transport closed before the snapshot request");
                    };
                    if let DistributedMessage::RequestSnapshotStream {
                        sender_id,
                        stream_id,
                        is_observer,
                        ..
                    } = msg
                    {
                        assert!(is_observer, "late-joiner observer must declare its role");
                        let reply: DistributedMessage<TestId> =
                            DistributedMessage::SnapshotStreamPackage {
                                target: None,
                                sender_id: "secondary-0".into(),
                                timestamp: 0.0,
                                stream_id,
                                seq: 0,
                                cursor: None,
                                payload: "{\"canned\":true}".into(),
                                done: true,
                            };
                        cluster
                            .send_to_peer(&sender_id, reply)
                            .await
                            .expect("snapshot reply over the proxied connection");
                        return;
                    }
                }
            });

            // The desktop-shaped joiner: seed rewritten to the forward
            // endpoint, cert cleared (WSS-only through the TCP pipe).
            let mut joiner: PeerNetwork<TestId> =
                PeerNetwork::start("observer-green", None).await.unwrap();
            let seed = vec![seed_entry("secondary-0", "127.0.0.1", forward_port)];

            let bootstrap = joiner
                .join_running_cluster(&seed, Duration::from_secs(10), true, false)
                .await
                .expect("join through the TCP forward endpoint must succeed");
            assert_eq!(bootstrap.payloads, vec!["{\"canned\":true}".to_string()]);
            responder.await.expect("responder completed");
        })
        .await;
}

/// Test-1a replay (production frame shape): the responder's reply
/// carries the Phase-C routing target every coordinator egress stamps —
/// `Some(Destination::Observer(<joiner-id>))`, what
/// `anti_entropy::reply_destination` resolves for an observer requester
/// — and broadcast gossip stamped `Some(Destination::All)` interleaves
/// with it on the joiner's wire, exactly the frame mix the production
/// joiner logged. `join_running_cluster` must skip the gossip and
/// ACCEPT the stamped snapshot reply: the frame's arrival on this leg
/// already satisfies the host-addressing, and the target stamp is only
/// the mesh-pump's slot-demux hint (the bootstrap window has no slots).
#[tokio::test(flavor = "current_thread")]
async fn join_accepts_role_stamped_snapshot_reply() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // The "cluster" peer (a compute secondary in production).
            let mut cluster: PeerNetwork<TestId> =
                PeerNetwork::start("secondary-0", None).await.unwrap();
            let cluster_port = cluster.port();
            let cluster_addr: std::net::SocketAddr =
                format!("127.0.0.1:{cluster_port}").parse().unwrap();

            // The local forward endpoint (production: the ssh -L child's
            // 127.0.0.1:<local_port> listener).
            let forward = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let forward_port = forward.local_addr().unwrap().port();
            tokio::task::spawn_local(async move {
                while let Ok((inbound, _)) = forward.accept().await {
                    tokio::task::spawn_local(proxy_one(inbound, cluster_addr));
                }
            });

            // The cluster peer's responder: gossip first (stamped `All`,
            // as the coordinator broadcast egress stamps it), then the
            // snapshot reply stamped with the requester's role-typed
            // return address — the production egress shape.
            let responder = tokio::task::spawn_local(async move {
                loop {
                    let Some(msg) = cluster.recv_peer().await else {
                        panic!("cluster peer transport closed before the snapshot request");
                    };
                    if let DistributedMessage::RequestSnapshotStream {
                        sender_id,
                        stream_id,
                        is_observer,
                        ..
                    } = msg
                    {
                        assert!(is_observer, "late-joiner observer must declare its role");
                        let gossip: DistributedMessage<TestId> = DistributedMessage::Keepalive {
                            target: None,
                            sender_id: "secondary-0".into(),
                            timestamp: 0.0,
                            secondary_id: "secondary-0".into(),
                            active_workers: 0,
                            emitter_role: KeepaliveRole::Secondary,
                        }
                        .with_target(Destination::All);
                        cluster
                            .broadcast(gossip)
                            .await
                            .expect("gossip broadcast reaches the joiner's wire");
                        let reply: DistributedMessage<TestId> =
                            DistributedMessage::SnapshotStreamPackage {
                                target: None,
                                sender_id: "secondary-0".into(),
                                timestamp: 0.0,
                                stream_id,
                                seq: 0,
                                cursor: None,
                                payload: "{\"canned\":true}".into(),
                                done: true,
                            }
                            .with_target(Destination::Observer(PeerId::from(sender_id.clone())));
                        cluster
                            .send_to_peer(&sender_id, reply)
                            .await
                            .expect("stamped snapshot reply over the proxied connection");
                        return;
                    }
                }
            });

            // The desktop-shaped joiner: seed rewritten to the forward
            // endpoint, cert cleared (WSS-only through the TCP pipe).
            let mut joiner: PeerNetwork<TestId> =
                PeerNetwork::start("observer-stamped", None).await.unwrap();
            let seed = vec![seed_entry("secondary-0", "127.0.0.1", forward_port)];

            let bootstrap = joiner
                .join_running_cluster(&seed, Duration::from_secs(10), true, false)
                .await
                .expect("the role-stamped snapshot reply must be accepted by the bootstrap window");
            assert_eq!(bootstrap.payloads, vec!["{\"canned\":true}".to_string()]);
            responder.await.expect("responder completed");
        })
        .await;
}

/// The anonymous-joiner replay: the bootstrap request must carry the
/// JOINER'S REAL IDENTITY — `sender_id` is the address every responder
/// replies to AND the key the receiving accept loop registers the
/// joiner's mesh leg under (first-frame identification).
///
/// REVERT-CHECK: a de-role refactor deleted `PeerNetwork`'s `local_id`
/// override, so production joiners sent `RequestSnapshotStream {
/// sender_id: "" }`: every responder replied to peer `""`, the joiner's
/// legs registered under the anonymous key, and the responder-originated
/// `PeerJoined` recorded a phantom `""` member — the cluster could never
/// address the joiner's real id (its directed pulls fell to the
/// role-miss fan WARN, replies relayed to nowhere). The earlier tests in
/// this file MASKED it: their canned responder echoes to the request's
/// `sender_id` over the single accepted leg, and a `""`-keyed leg
/// happily routes a reply addressed to `""`. This test pins the
/// identity END-TO-END: the responder asserts the carried id, asserts
/// the leg is registered under it, and replies BY that id.
#[tokio::test(flavor = "current_thread")]
async fn join_request_carries_the_joiner_identity() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut cluster: PeerNetwork<TestId> =
                PeerNetwork::start("secondary-0", None).await.unwrap();
            let cluster_port = cluster.port();

            let responder = tokio::task::spawn_local(async move {
                loop {
                    let Some(msg) = cluster.recv_peer().await else {
                        panic!("cluster peer transport closed before the snapshot request");
                    };
                    if let DistributedMessage::RequestSnapshotStream {
                        sender_id,
                        stream_id,
                        ..
                    } = msg
                    {
                        // THE identity pin: the request's return address
                        // is the joiner's real peer-id, never empty.
                        assert_eq!(
                            sender_id, "observer-mirror",
                            "the bootstrap request must carry the joiner's real id \
                             as its return address"
                        );
                        // First-frame identification keyed the joiner's
                        // leg under that SAME id — a directed reply (and
                        // every later directed frame) routes to it.
                        assert!(
                            cluster.has_peer(&PeerId::from(sender_id.as_str())),
                            "the joiner's leg must be registered under its real id"
                        );
                        // Reply BY the carried id, stamped the way a real
                        // responder's egress stamps it (the requester's
                        // declared role).
                        let reply: DistributedMessage<TestId> =
                            DistributedMessage::SnapshotStreamPackage {
                                target: None,
                                sender_id: "secondary-0".into(),
                                timestamp: 0.0,
                                stream_id,
                                seq: 0,
                                cursor: None,
                                payload: "{\"canned\":true}".into(),
                                done: true,
                            }
                            .with_target(Destination::Observer(PeerId::from(sender_id.clone())));
                        cluster
                            .send_to_peer(&sender_id, reply)
                            .await
                            .expect("reply addressed by the joiner's real id must route");
                        return;
                    }
                }
            });

            let mut joiner: PeerNetwork<TestId> =
                PeerNetwork::start("observer-mirror", None).await.unwrap();
            let seed = vec![seed_entry("secondary-0", "127.0.0.1", cluster_port)];

            let bootstrap = joiner
                .join_running_cluster(&seed, Duration::from_secs(10), true, false)
                .await
                .expect("the identity-carrying bootstrap must complete");
            assert_eq!(bootstrap.payloads, vec!["{\"canned\":true}".to_string()]);
            responder.await.expect("responder completed");
        })
        .await;
}

/// The bootstrap deadline is PERSISTENT under constant churn (the
/// watchdog law): a reachable peer that floods the joiner with gossip
/// but never answers the snapshot request must NOT push the join
/// deadline back — `join_running_cluster` returns `Timeout` at
/// (roughly) its budget despite a continuously-active wire. A
/// reset-on-activity deadline would spin here forever, which is the
/// probe-observer "never hit its bootstrap deadline" suspicion shape.
#[tokio::test(flavor = "current_thread")]
async fn bootstrap_deadline_fires_under_constant_gossip_churn() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // The cluster peer: answers NOTHING, gossips constantly.
            let mut cluster: PeerNetwork<TestId> =
                PeerNetwork::start("secondary-0", None).await.unwrap();
            let cluster_port = cluster.port();
            tokio::task::spawn_local(async move {
                let mut ts = 0.0f64;
                loop {
                    ts += 1.0;
                    let gossip: DistributedMessage<TestId> = DistributedMessage::Keepalive {
                        target: None,
                        sender_id: "secondary-0".into(),
                        timestamp: ts,
                        secondary_id: "secondary-0".into(),
                        active_workers: 0,
                        emitter_role: KeepaliveRole::Secondary,
                    }
                    .with_target(Destination::All);
                    let _ = cluster.broadcast(gossip).await;
                    // recv_peer drives the accept loop's registrations so
                    // the joiner's wire stays live (and its request frames
                    // are consumed, never answered).
                    let _ =
                        tokio::time::timeout(Duration::from_millis(10), cluster.recv_peer()).await;
                }
            });

            let mut joiner: PeerNetwork<TestId> =
                PeerNetwork::start("observer-churn", None).await.unwrap();
            let seed = vec![seed_entry("secondary-0", "127.0.0.1", cluster_port)];

            let budget = Duration::from_secs(3);
            let started = std::time::Instant::now();
            let result = joiner
                .join_running_cluster(&seed, budget, true, false)
                .await;
            assert!(
                matches!(result, Err(JoinError::Timeout { .. })),
                "an unanswered bootstrap must surface Timeout, got {result:?}"
            );
            assert!(
                started.elapsed() < budget + Duration::from_secs(2),
                "the bootstrap deadline must fire at ~its budget despite \
                 constant gossip churn (persistent deadline, never \
                 activity-reset); took {:?}",
                started.elapsed()
            );
        })
        .await;
}
