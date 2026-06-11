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

use std::time::Duration;

use super::super::PeerNetwork;
use super::TestId;
use dynrunner_protocol_primary_secondary::{
    DistributedMessage, JoinError, PeerConnectionInfo, PeerTransport,
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
            // RequestClusterSnapshot with a canned ClusterSnapshot.
            let responder = tokio::task::spawn_local(async move {
                loop {
                    let Some(msg) = cluster.recv_peer().await else {
                        panic!("cluster peer transport closed before the snapshot request");
                    };
                    if let DistributedMessage::RequestClusterSnapshot {
                        sender_id,
                        is_observer,
                        ..
                    } = msg
                    {
                        assert!(is_observer, "late-joiner observer must declare its role");
                        let reply: DistributedMessage<TestId> =
                            DistributedMessage::ClusterSnapshot {
                                target: None,
                                sender_id: "secondary-0".into(),
                                timestamp: 0.0,
                                snapshot_json: "{\"canned\":true}".into(),
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

            let snapshots = joiner
                .join_running_cluster(&seed, Duration::from_secs(10), true, false)
                .await
                .expect("join through the TCP forward endpoint must succeed");
            assert_eq!(snapshots, vec!["{\"canned\":true}".to_string()]);
            responder.await.expect("responder completed");
        })
        .await;
}
