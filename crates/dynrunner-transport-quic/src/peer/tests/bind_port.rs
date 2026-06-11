//! `PeerNetwork::start` bind-port contract (#355).
//!
//! The SLURM wrapper pre-allocates a free port host-side, records it
//! in the late-joiner's `connection_info/<id>.info` file, and hands it
//! to the in-container secondary — which must then bind EXACTLY that
//! port on BOTH mesh listeners (QUIC/UDP + WSS/TCP) or the recorded
//! port is dead for every dialing peer. `explicit_bind_port_binds_both_
//! listeners` pins that contract (including the production-relevant
//! cert-less WSS dial-in); `none_bind_port_stays_ephemeral` pins that
//! the `None` path keeps the historical OS-picked behaviour.

use std::net::SocketAddr;

use super::super::PeerNetwork;
use super::TestId;
use crate::wss::connect_wss;

/// Allocate a port that is currently free on BOTH protocols (TCP and
/// UDP) — the same shape the SLURM wrapper's host-side pre-allocation
/// produces. Retries a handful of OS-picked candidates so a UDP
/// squatter on a TCP-free port can't flake the test.
fn alloc_dual_free_port() -> u16 {
    for _ in 0..16 {
        let tcp = std::net::TcpListener::bind("0.0.0.0:0").expect("probe tcp bind");
        let port = tcp.local_addr().expect("probe tcp addr").port();
        if std::net::UdpSocket::bind(("0.0.0.0", port)).is_ok() {
            // Both binds succeeded; release them for the network to claim.
            return port;
        }
    }
    panic!("could not find a port free on both TCP and UDP in 16 attempts");
}

/// An explicit `bind_port` pins BOTH listeners to the requested port:
/// the network reports it, the UDP side holds it (QUIC), and a plain
/// cert-less WSS dial to it connects (the late-joiner's fallback leg).
#[tokio::test(flavor = "current_thread")]
async fn explicit_bind_port_binds_both_listeners() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let port = alloc_dual_free_port();
            let net: PeerNetwork<TestId> = PeerNetwork::start("peer-fixed", Some(port))
                .await
                .expect("start on pre-allocated port");
            assert_eq!(
                net.port(),
                port,
                "the network must report the requested port, not an ephemeral one"
            );

            // QUIC holds the UDP side: a second UDP bind on the same
            // wildcard address must fail while the network lives.
            assert!(
                std::net::UdpSocket::bind(("0.0.0.0", port)).is_err(),
                "the QUIC listener must hold UDP port {port}"
            );

            // WSS holds the TCP side AND accepts a dial — the exact leg
            // a cert-less late-joiner record is dialed over.
            let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
            connect_wss(addr)
                .await
                .expect("a WSS dial to the pinned port must connect");
        })
        .await;
}

/// #422 fail-fast contract for the CONCRETE-port path: when the caller
/// asks for a specific port whose TCP twin is already held (the QUIC/UDP
/// side is free, so the bug's `WssListener::bind` is what fails), `start`
/// returns the `AddrInUse` error promptly — it must NOT silently retry
/// onto a different OS-picked port, because the caller advertised THIS
/// one and any other is a dead address for every dialing peer. (The
/// confirmed production error string for this race is "Address already in
/// use (os error 98)".)
#[tokio::test(flavor = "current_thread")]
async fn explicit_bind_port_with_occupied_tcp_twin_fails_fast() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // A port free on UDP (so QUIC binds) but held on TCP.
            let (tcp_squatter, port) = loop {
                let tcp = std::net::TcpListener::bind("0.0.0.0:0").expect("probe tcp bind");
                let port = tcp.local_addr().expect("probe tcp addr").port();
                if std::net::UdpSocket::bind(("0.0.0.0", port)).is_ok() {
                    break (tcp, port);
                }
            };
            let _keep = tcp_squatter; // hold the TCP port for the duration

            let result: Result<PeerNetwork<TestId>, String> =
                PeerNetwork::start("peer-twin", Some(port)).await;
            let err = result.err().expect(
                "an explicit port whose TCP twin is occupied must fail fast, \
                 not retry onto a different (dead-address) port",
            );
            assert!(
                err.contains("os error 98") || err.contains("in use"),
                "fail-fast error must be the address-in-use class, got: {err}"
            );
        })
        .await;
}

/// #422 stress / regression: many ephemeral (`None`) networks come up
/// concurrently. The OS-picked-UDP / same-numeric-TCP race used to flake
/// a random victim with `AddrInUse`; the pairing-helper retry must make
/// every start succeed.
#[tokio::test(flavor = "current_thread")]
async fn many_ephemeral_starts_all_succeed() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            const N: usize = 64;
            let mut nets: Vec<PeerNetwork<TestId>> = Vec::with_capacity(N);
            for i in 0..N {
                match PeerNetwork::<TestId>::start(&format!("peer-{i}"), None).await {
                    Ok(net) => nets.push(net),
                    Err(e) => panic!("stress start #{i} failed: {e}"),
                }
            }
            assert_eq!(nets.len(), N);
        })
        .await;
}

/// `None` keeps the historical ephemeral behaviour: the OS picks a
/// port, and two concurrent `None` networks coexist (no fixed-port
/// collision).
#[tokio::test(flavor = "current_thread")]
async fn none_bind_port_stays_ephemeral() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let a: PeerNetwork<TestId> = PeerNetwork::start("peer-a", None).await.unwrap();
            let b: PeerNetwork<TestId> = PeerNetwork::start("peer-b", None).await.unwrap();
            assert_ne!(a.port(), 0, "ephemeral bind must yield a concrete port");
            assert_ne!(b.port(), 0, "ephemeral bind must yield a concrete port");
            assert_ne!(
                a.port(),
                b.port(),
                "two ephemeral networks must not collide on one port"
            );
        })
        .await;
}
