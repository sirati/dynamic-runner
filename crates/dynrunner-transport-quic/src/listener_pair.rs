//! Atomic acquisition of the QUIC(UDP)+WSS(TCP) same-port listener pair.
//!
//! The mesh advertises ONE port number and serves both protocols on it:
//! QUIC on UDP and WSS on TCP. Acquiring that pair has a structural race
//! on the ephemeral (`port == 0`) path: binding QUIC port 0 lets the OS
//! pick a free UDP port, but nothing guarantees the SAME numeric port is
//! free on TCP — another process (or another mesh listener under
//! parallel load) may already hold the TCP twin. The WSS bind then fails
//! with `AddrInUse` (errno 98) and the whole `start` aborts (#422).
//!
//! [`bind_listener_pair`] owns the fix in one place: on the ephemeral
//! path it retries the WHOLE pair — drop the UDP socket, let the OS pick
//! a different port, try the TCP twin again — until both land on a port
//! free for both protocols (or the bounded attempt budget is spent). A
//! caller-requested concrete port keeps fail-fast semantics: the caller
//! asked for THAT exact port (e.g. a pre-allocated, pre-advertised SLURM
//! port), so a different one would be a dead address for every dialing
//! peer — retrying onto an OS-picked port would silently betray the
//! contract.
//!
//! Retry fires ONLY on the address-in-use error class. A cert /
//! permission / config error is fatal and identical across attempts, so
//! it returns immediately rather than burning the whole budget.

use std::net::SocketAddr;

use crate::certs::CertPair;
use crate::transport::QuicListener;
use crate::wss::WssListener;

/// Bounded retry budget for the ephemeral-port pairing race. Each
/// attempt is a full UDP+TCP bind cycle; collisions on a fresh OS-picked
/// port are independent and rare, so a handful of attempts makes a
/// genuine exhaustion astronomically unlikely while still terminating.
const PAIR_BIND_ATTEMPTS: usize = 16;

/// Outcome of one full UDP+TCP pairing attempt that fell short of a
/// bound pair. `Contended` is the address-in-use class the ephemeral
/// retry recovers from; `Fatal` is anything else (cert / permission /
/// fd exhaustion) and must abort immediately, not burn the budget.
enum PairAttemptError {
    Contended(std::io::Error),
    Fatal(std::io::Error),
}

/// Bind the QUIC(UDP)+WSS(TCP) pair on a single port number for ONE
/// attempt: QUIC first (on the ephemeral path the OS picks the port),
/// then WSS on whatever port QUIC resolved. The QUIC socket is dropped
/// before returning a `Contended` TCP error so the next attempt starts
/// from a clean slate (its UDP port may itself be the one contended on
/// TCP). Errors are classified by [`PairAttemptError`] so the caller's
/// retry loop carries no error-string sniffing.
async fn try_bind_pair_once(
    server_config: &quinn::ServerConfig,
    addr: SocketAddr,
) -> Result<(QuicListener, WssListener), PairAttemptError> {
    let quic_listener =
        QuicListener::try_bind(server_config.clone(), addr).map_err(classify)?;
    let port = quic_listener.port();

    // Bind WSS (TCP) on the QUIC-resolved port. This is the leg that
    // loses the #422 race: the OS only vouched for the UDP side.
    let wss_addr = SocketAddr::new(addr.ip(), port);
    match WssListener::try_bind(wss_addr).await {
        Ok(wss_listener) => Ok((quic_listener, wss_listener)),
        Err(e) => {
            // Release the QUIC socket before the next attempt picks a
            // fresh port for the whole pair.
            drop(quic_listener);
            Err(classify(e))
        }
    }
}

/// Acquire the QUIC(UDP)+WSS(TCP) listener pair on a single port number.
///
/// `addr` carries the requested bind address; its port selects the mode:
/// - port `0` (ephemeral): retry the pair atomically until both
///   protocols bind the same OS-picked port, up to [`PAIR_BIND_ATTEMPTS`]
///   times. On exhaustion the last `AddrInUse` error is returned.
/// - concrete port: bind that exact port on both, fail-fast (no retry) —
///   the caller advertised this port and any other is a dead address.
///
/// The cert→`ServerConfig` build is done once up front (a fatal,
/// retry-invariant failure if it errors); only the io-fallible binds are
/// retried.
pub(crate) async fn bind_listener_pair(
    cert: &CertPair,
    addr: SocketAddr,
) -> Result<(QuicListener, WssListener), String> {
    let server_config = cert.server_config()?;
    let retry = addr.port() == 0;

    let mut attempt = 0usize;
    loop {
        attempt += 1;
        match try_bind_pair_once(&server_config, addr).await {
            Ok(pair) => return Ok(pair),
            Err(PairAttemptError::Contended(e))
                if retry && attempt < PAIR_BIND_ATTEMPTS =>
            {
                tracing::debug!(
                    attempt,
                    error = %e,
                    "QUIC/WSS port pair contended on the TCP twin; retrying \
                     with a fresh OS-picked port"
                );
                continue;
            }
            Err(PairAttemptError::Contended(e)) | Err(PairAttemptError::Fatal(e)) => {
                return Err(e.to_string());
            }
        }
    }
}

/// Classify a bind io error: the address-in-use class is the contended
/// race the ephemeral retry recovers from; anything else is fatal.
fn classify(e: std::io::Error) -> PairAttemptError {
    if e.kind() == std::io::ErrorKind::AddrInUse {
        PairAttemptError::Contended(e)
    } else {
        PairAttemptError::Fatal(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A concrete (non-zero) port whose TCP twin is already held is the
    /// production #422 collision. The per-attempt binder must (a)
    /// classify it `Contended` (the address-in-use class), proving the
    /// retry path would engage on the ephemeral variant, and (b) leave
    /// the held TCP listener intact (the attempt released its own QUIC
    /// socket and did not somehow steal the squatter's port).
    #[tokio::test]
    async fn tcp_twin_collision_classifies_as_contended() {
        // Find a port free on UDP but occupied on TCP — the exact shape
        // QUIC-took-UDP / WSS-loses-the-TCP-twin produces in the wild.
        let (tcp_squatter, port) = loop {
            let tcp = std::net::TcpListener::bind("127.0.0.1:0").expect("probe tcp bind");
            let p = tcp.local_addr().expect("probe tcp addr").port();
            if std::net::UdpSocket::bind(("127.0.0.1", p)).is_ok() {
                break (tcp, p);
            }
        };

        let cert = CertPair::generate("pair-test").unwrap();
        let server_config = cert.server_config().unwrap();
        let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

        let outcome = try_bind_pair_once(&server_config, addr).await;
        match outcome {
            Err(PairAttemptError::Contended(e)) => {
                assert_eq!(e.kind(), std::io::ErrorKind::AddrInUse);
            }
            Err(PairAttemptError::Fatal(e)) => {
                panic!("TCP-twin collision misclassified as fatal: {e}");
            }
            Ok(_) => panic!("bind unexpectedly succeeded over a held TCP twin"),
        }

        // The squatter still owns the TCP port (the attempt did not free
        // it), and the released QUIC socket left the UDP port reusable.
        assert!(
            std::net::TcpListener::bind(("127.0.0.1", port)).is_err(),
            "the held TCP listener must still own the port"
        );
        drop(tcp_squatter);
    }

    /// A bind failure that is NOT address-in-use (here: a privileged
    /// port the test process cannot bind) must classify `Fatal`, so the
    /// ephemeral retry loop aborts immediately instead of burning all 16
    /// attempts on an unrecoverable error.
    #[test]
    fn non_addr_in_use_classifies_as_fatal() {
        let permission = std::io::Error::from(std::io::ErrorKind::PermissionDenied);
        assert!(matches!(classify(permission), PairAttemptError::Fatal(_)));
        let in_use = std::io::Error::from(std::io::ErrorKind::AddrInUse);
        assert!(matches!(classify(in_use), PairAttemptError::Contended(_)));
    }
}
