//! Real-sshd integration tests for `LocalForwardTunnels` establishment
//! over a live gateway ControlMaster — the late-joiner's `-L` path.
//!
//! Regression pin for the LMU production failure (11/11 forwards
//! "failed"): an `ssh -N -L` spawned as a MUX CLIENT over a live
//! master does NOT behave like a direct `ssh -N` — it opens a real
//! sshd session (the login shell), which EOFs on the null stdin and
//! exits within milliseconds with the shell's exit status (rc=0 on
//! the gateway's bash), while the `-L` forward itself is registered
//! on the MASTER and stays alive. A child-longevity gate therefore
//! declares every working forward dead. These tests assert the
//! contract that gate must honor: `establish` over a live master
//! yields endpoints that actually carry bytes end-to-end.
//!
//! Like `dynrunner-gateway/tests/master_lifetime.rs`, the tests need
//! an sshd at `localhost:22` and skip (with a message) when absent.
//!
//! Run on a host with sshd:
//!   `cargo test -p dynrunner-slurm --test local_forward_mux`

mod local_forward_mux {
    pub(crate) mod helpers;
}

use std::io::{Read, Write};
use std::net::{Ipv4Addr, TcpListener, TcpStream};
use std::time::{Duration, Instant};

use dynrunner_gateway::SshGateway;
use dynrunner_gateway::traits::Gateway;
use dynrunner_slurm::{ForwardTarget, LocalForwardTunnels, ReconnectOutcome};

use local_forward_mux::helpers::{AuthorizedKey, make_config, serialise, sshd_reachable};

/// A destination service stand-in: accepts connections forever and
/// writes a greeting on each. (The forward target a peer's record
/// would name — here on localhost, which is "compute-reachable" from
/// the test gateway's point of view since gateway == localhost.)
struct DestService {
    port: u16,
    _thread: std::thread::JoinHandle<()>,
}

impl DestService {
    fn start() -> Self {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind dest service");
        let port = listener.local_addr().expect("local_addr").port();
        let thread = std::thread::spawn(move || {
            // Exits when the test process does; accept errors mean
            // the listener is being torn down.
            while let Ok((mut conn, _)) = listener.accept() {
                let _ = conn.write_all(b"ok");
            }
        });
        Self {
            port,
            _thread: thread,
        }
    }
}

/// Read the dest greeting through `127.0.0.1:<local_port>` with a
/// deadline — proves the forward carries bytes end-to-end (a bare
/// connect() succeeding only proves a local accept, not the channel).
fn read_through_forward(local_port: u16) -> Result<(), String> {
    let mut conn = TcpStream::connect_timeout(
        &std::net::SocketAddr::from((Ipv4Addr::LOCALHOST, local_port)),
        Duration::from_secs(3),
    )
    .map_err(|e| format!("connect 127.0.0.1:{local_port}: {e}"))?;
    conn.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    let mut buf = [0u8; 2];
    conn.read_exact(&mut buf)
        .map_err(|e| format!("read through 127.0.0.1:{local_port}: {e}"))?;
    if &buf != b"ok" {
        return Err(format!("unexpected greeting {buf:?}"));
    }
    Ok(())
}

/// `true` while something LISTENs on `127.0.0.1:<port>` (bind-probe).
fn port_listening(port: u16) -> bool {
    TcpListener::bind((Ipv4Addr::LOCALHOST, port)).is_err()
}

fn poll_until(deadline: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let end = Instant::now() + deadline;
    while Instant::now() < end {
        if cond() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    cond()
}

/// THE production-regression pin: a cohort of forwards established
/// over a LIVE gateway master must all come up and carry bytes —
/// even though every per-forward ssh involved may exit immediately
/// (mux registration is master-side). Under the shipped
/// child-longevity gate this fails exactly like LMU did: every leg
/// "SSH tunnel exited within 3s rc=Some(0/1)" and `establish`
/// hard-errors with zero dialable endpoints.
#[tokio::test(flavor = "current_thread")]
async fn establish_over_live_master_yields_working_forwards() {
    let _guard = serialise().await;
    if !sshd_reachable() {
        eprintln!("skipping: no sshd on localhost:22");
        return;
    }
    let authorized = match AuthorizedKey::provision() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("skipping: cannot provision authorized key: {e}");
            return;
        }
    };
    let config = make_config(&authorized);
    let mut gateway = SshGateway::new(config.clone());
    gateway.connect().await.expect("gateway connect");
    let control_path = gateway
        .control_path()
        .expect("connected gateway has a control path")
        .to_owned();

    let dests: Vec<DestService> = (0..3).map(|_| DestService::start()).collect();
    let targets: Vec<ForwardTarget> = dests
        .iter()
        .enumerate()
        .map(|(i, d)| ForwardTarget {
            peer_id: format!("secondary-{i}"),
            host: "127.0.0.1".to_owned(),
            port: d.port,
        })
        .collect();

    let local_set = tokio::task::LocalSet::new();
    local_set
        .run_until(async {
            let tunnels = LocalForwardTunnels::new(config.clone(), Some(control_path));
            let endpoints = tunnels
                .establish(&targets)
                .await
                .expect("all forwards must establish over the live master");
            assert_eq!(endpoints.len(), targets.len(), "{endpoints:?}");
            for t in &targets {
                let port = endpoints[&t.peer_id];
                read_through_forward(port)
                    .unwrap_or_else(|e| panic!("{}: forward not live end-to-end: {e}", t.peer_id));
            }

            // Teardown releases every local port (mux legs via
            // `ssh -O cancel`, direct legs via child reap).
            let ports: Vec<u16> = endpoints.values().copied().collect();
            tunnels.teardown().await;
            for port in ports {
                assert!(
                    poll_until(Duration::from_secs(5), || !port_listening(port)),
                    "port {port} still listening after teardown"
                );
            }
        })
        .await;

    gateway.disconnect().await.expect("gateway disconnect");
}

/// Master dies mid-run: the rebuild (reconnect seam, #419 same-port
/// contract) must engage the DIRECT fallback on the SAME local port
/// and the forward must carry bytes again.
#[tokio::test(flavor = "current_thread")]
async fn rebuild_after_master_death_falls_back_to_direct_same_port() {
    let _guard = serialise().await;
    if !sshd_reachable() {
        eprintln!("skipping: no sshd on localhost:22");
        return;
    }
    let authorized = match AuthorizedKey::provision() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("skipping: cannot provision authorized key: {e}");
            return;
        }
    };
    let config = make_config(&authorized);
    let mut gateway = SshGateway::new(config.clone());
    gateway.connect().await.expect("gateway connect");
    let control_path = gateway
        .control_path()
        .expect("connected gateway has a control path")
        .to_owned();

    let dest = DestService::start();
    let targets = vec![ForwardTarget {
        peer_id: "secondary-0".to_owned(),
        host: "127.0.0.1".to_owned(),
        port: dest.port,
    }];

    let local_set = tokio::task::LocalSet::new();
    local_set
        .run_until(async {
            let tunnels = LocalForwardTunnels::new(config.clone(), Some(control_path));
            let endpoints = tunnels.establish(&targets).await.expect("establish");
            let port = endpoints["secondary-0"];
            read_through_forward(port).expect("initial forward live");

            // Kill the master: the mux-registered forward dies with it.
            gateway.disconnect().await.expect("gateway disconnect");
            assert!(
                poll_until(Duration::from_secs(5), || !port_listening(port)),
                "forward should die with the master"
            );

            // The reconnect seam must rebuild on the SAME port via a
            // direct dial (the probe now reports the master dead).
            let outcome = tunnels
                .reconnect_one("secondary-0")
                .await
                .expect("rebuild after master death");
            assert_eq!(outcome, ReconnectOutcome::Rebuilt { local_port: port });
            read_through_forward(port).expect("rebuilt forward live");
            tunnels.teardown().await;
        })
        .await;
}

/// A dead/absent master at establish time must engage the direct
/// fallback for the whole cohort — the joiner must never be
/// hard-broken by a missing master.
#[tokio::test(flavor = "current_thread")]
async fn establish_with_dead_master_direct_dials() {
    let _guard = serialise().await;
    if !sshd_reachable() {
        eprintln!("skipping: no sshd on localhost:22");
        return;
    }
    let authorized = match AuthorizedKey::provision() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("skipping: cannot provision authorized key: {e}");
            return;
        }
    };
    let config = make_config(&authorized);

    let dests: Vec<DestService> = (0..2).map(|_| DestService::start()).collect();
    let targets: Vec<ForwardTarget> = dests
        .iter()
        .enumerate()
        .map(|(i, d)| ForwardTarget {
            peer_id: format!("secondary-{i}"),
            host: "127.0.0.1".to_owned(),
            port: d.port,
        })
        .collect();

    let local_set = tokio::task::LocalSet::new();
    local_set
        .run_until(async {
            let tunnels = LocalForwardTunnels::new(
                config.clone(),
                Some("/tmp/dynrunner-m-0-never-existed.sock".to_owned()),
            );
            let endpoints = tunnels
                .establish(&targets)
                .await
                .expect("direct fallback must establish");
            assert_eq!(endpoints.len(), targets.len());
            for t in &targets {
                read_through_forward(endpoints[&t.peer_id])
                    .unwrap_or_else(|e| panic!("{}: {e}", t.peer_id));
            }
            tunnels.teardown().await;
        })
        .await;
}
