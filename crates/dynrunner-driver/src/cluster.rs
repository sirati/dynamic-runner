//! Cluster-reachability probe. Single function, no state.
//!
//! Per locked design point (l): the framework owns `is_running` —
//! a TCP probe of the gateway sshd port, sufficient to tell "the
//! cluster's listener is up" from "we need bring-up". The framework
//! does NOT own `bring_up` / `bring_down` / `provision_user` — those
//! belong to the cluster harness (slurm-test-env's flake apps), and
//! consumers compose them with `subprocess.run` directly.

use std::net::TcpStream;
use std::time::Duration;

/// Returns `true` iff a TCP connection to `localhost:<ssh_port>` can
/// be established within ~2 seconds.
///
/// Sufficient as an "is the cluster up" gate: if SSH refuses, the
/// cluster is either down or the port is wrong; either way the
/// caller wants to invoke its own bring-up. Doesn't shell out to
/// podman, doesn't ssh, doesn't run nix — just a 2s TCP connect.
///
/// Uses the synchronous `TcpStream::connect_timeout` so callers
/// without a tokio runtime (e.g. the e2e Python harness via the
/// PyO3 binding) don't pay the runtime-build cost.
pub fn is_running(ssh_port: u16) -> bool {
    let addr = match format!("127.0.0.1:{ssh_port}").parse() {
        Ok(a) => a,
        // Unreachable in practice (the format produces a valid
        // socket address for any u16), but we don't want to panic on
        // a probe path.
        Err(_) => return false,
    };
    TcpStream::connect_timeout(&addr, Duration::from_secs(2)).is_ok()
}
