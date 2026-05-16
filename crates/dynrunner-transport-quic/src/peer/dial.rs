//! Happy-eyeballs peer dialer (RFC 8305 — parallel-only variant).
//!
//! For a single peer, builds the candidate socket-address list from
//! [`PeerConnectionInfo`]'s `ipv4`/`ipv6` strings and races a QUIC
//! attempt across every candidate. If every QUIC attempt fails or
//! times out, races a WSS attempt across the same candidates. Whichever
//! socket connects first wins; the rest are dropped (which cancels the
//! in-flight quinn / tcp connect future).
//!
//! Why parallel-only and not the full RFC 8305 staggered start: on a
//! cluster-internal fabric the latency between picking v4 vs v6 first
//! is sub-ms and we don't need the staggering to avoid wasting upstream
//! bandwidth. The reason this module exists is purely to defeat the
//! dual-stack failure mode where one family is administratively
//! reachable and the other is not (e.g. compute nodes that advertise
//! both A and AAAA records but firewall the AAAA path) — picking just
//! one family up front means the wrong choice is fatal.
//!
//! Per-attempt timeout matches the original sequential dialer: 10s for
//! QUIC, 10s for WSS. Total wall-clock for a single peer is therefore
//! bounded by `max(quic_attempts) + max(wss_attempts) ≈ 20s`, the
//! same budget as the pre-happy-eyeballs path.
//!
//! `connect_to_peers` is the only caller; this module is `pub(super)`.

use std::net::SocketAddr;
use std::time::Duration;

use dynrunner_protocol_primary_secondary::PeerConnectionInfo;
use rustls::pki_types::CertificateDer;

use crate::wss::connect_wss;

use super::util::{PeerConnection, parse_cert_pem};

/// Per-attempt timeout for QUIC and WSS dials. Matches the pre-happy-
/// eyeballs sequential dialer so the total per-peer budget is unchanged.
const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(10);

/// Resolve a peer's `(ipv4, ipv6, port)` triple into a list of candidate
/// `SocketAddr`s. Skips fields that don't parse as a literal IP — the
/// addresses are produced upstream by `detect_ipv4` / equivalent and are
/// expected to be numeric. Falls back to `127.0.0.1` when neither family
/// is set, matching the pre-happy-eyeballs dialer's localhost fallback
/// (used by single-host integration tests).
pub(super) fn candidate_addrs(peer_info: &PeerConnectionInfo) -> Vec<SocketAddr> {
    let mut addrs = Vec::new();
    if let Some(s) = peer_info.ipv4.as_deref()
        && let Ok(ip) = s.parse::<std::net::Ipv4Addr>()
    {
        addrs.push(SocketAddr::new(ip.into(), peer_info.port));
    }
    if let Some(s) = peer_info.ipv6.as_deref()
        && let Ok(ip) = s.parse::<std::net::Ipv6Addr>()
    {
        addrs.push(SocketAddr::new(ip.into(), peer_info.port));
    }
    if addrs.is_empty() {
        // Localhost fallback — preserves the pre-happy-eyeballs
        // behavior that single-host tests rely on. Real deployments
        // populate ipv4 (and optionally ipv6) upstream.
        addrs.push(SocketAddr::new(
            std::net::Ipv4Addr::LOCALHOST.into(),
            peer_info.port,
        ));
    }
    addrs
}

/// Race a connection attempt across every `addr` in parallel using
/// `attempt`. Returns the first successful connection along with the
/// address it was made to; returns `None` if every attempt fails or the
/// per-attempt timeout fires for all of them.
///
/// The `attempt` closure produces a future per address. On success,
/// the runner-up futures are dropped (Tokio cancellation), which
/// cancels the underlying connect.
async fn race_first_success<C, F, Fut>(
    addrs: &[SocketAddr],
    timeout: Duration,
    mut attempt: F,
) -> Option<(SocketAddr, C)>
where
    F: FnMut(SocketAddr) -> Fut,
    Fut: std::future::Future<Output = Result<C, String>>,
{
    use futures_util::FutureExt;
    use futures_util::stream::{FuturesUnordered, StreamExt};

    if addrs.is_empty() {
        return None;
    }

    let mut pending: FuturesUnordered<_> = addrs
        .iter()
        .copied()
        .map(|addr| {
            let fut = attempt(addr);
            tokio::time::timeout(timeout, fut).map(move |r| (addr, r))
        })
        .collect();

    while let Some((addr, result)) = pending.next().await {
        match result {
            Ok(Ok(conn)) => return Some((addr, conn)),
            Ok(Err(e)) => {
                tracing::debug!(%addr, error = %e, "peer dial attempt failed");
            }
            Err(_) => {
                tracing::debug!(%addr, "peer dial attempt timed out");
            }
        }
    }
    None
}

/// Dial a single peer, racing QUIC then WSS across every candidate
/// `SocketAddr` derived from `peer_info`. Returns the first connection
/// to succeed, or `None` if every attempt failed.
///
/// QUIC is tried first (it's the preferred transport — UDP, no
/// head-of-line blocking, lower handshake cost on this codepath). When
/// no valid certificate can be parsed from `peer_info.cert`, the QUIC
/// race is skipped and we go straight to WSS — same as the pre-happy-
/// eyeballs dialer.
pub(super) async fn dial_peer(
    peer_id: &str,
    peer_info: &PeerConnectionInfo,
) -> Option<PeerConnection> {
    let addrs = candidate_addrs(peer_info);
    let peer_cert_der = parse_cert_pem(&peer_info.cert);

    if let Some(cert_der) = peer_cert_der.as_ref() {
        if let Some((addr, conn)) =
            race_quic(&addrs, peer_id, cert_der, ATTEMPT_TIMEOUT).await
        {
            tracing::info!(peer = peer_id, %addr, "connected to peer via QUIC");
            return Some(PeerConnection::Quic(conn));
        }
        tracing::warn!(peer = peer_id, "QUIC race to peer failed across all addresses, trying WSS");
    } else {
        tracing::warn!(peer = peer_id, "no valid cert for peer, trying WSS");
    }

    if let Some((addr, conn)) = race_wss(&addrs, ATTEMPT_TIMEOUT).await {
        tracing::info!(peer = peer_id, %addr, "connected to peer via WSS");
        return Some(PeerConnection::Wss(Box::new(conn)));
    }

    tracing::error!(peer = peer_id, "WSS race to peer failed across all addresses");
    None
}

async fn race_quic(
    addrs: &[SocketAddr],
    server_name: &str,
    cert_der: &CertificateDer<'_>,
    timeout: Duration,
) -> Option<(SocketAddr, crate::transport::QuicConnection)> {
    race_first_success(addrs, timeout, |addr| {
        crate::transport::connect(addr, server_name, cert_der)
    })
    .await
}

async fn race_wss(
    addrs: &[SocketAddr],
    timeout: Duration,
) -> Option<(SocketAddr, crate::wss::WssConnection)> {
    race_first_success(addrs, timeout, connect_wss).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pinfo(ipv4: Option<&str>, ipv6: Option<&str>, port: u16) -> PeerConnectionInfo {
        PeerConnectionInfo {
            secondary_id: "p".into(),
            cert: String::new(),
            ipv4: ipv4.map(|s| s.into()),
            ipv6: ipv6.map(|s| s.into()),
            port,
            is_observer: false,
        }
    }

    #[test]
    fn candidate_addrs_v4_only() {
        let addrs = candidate_addrs(&pinfo(Some("10.0.0.1"), None, 1234));
        assert_eq!(addrs, vec!["10.0.0.1:1234".parse().unwrap()]);
    }

    #[test]
    fn candidate_addrs_v6_only() {
        let addrs = candidate_addrs(&pinfo(None, Some("::1"), 1234));
        assert_eq!(addrs, vec!["[::1]:1234".parse().unwrap()]);
    }

    #[test]
    fn candidate_addrs_dual_stack() {
        let addrs = candidate_addrs(&pinfo(Some("10.0.0.1"), Some("fe80::1"), 1234));
        assert_eq!(
            addrs,
            vec![
                "10.0.0.1:1234".parse().unwrap(),
                "[fe80::1]:1234".parse().unwrap(),
            ]
        );
    }

    #[test]
    fn candidate_addrs_neither_falls_back_to_localhost() {
        let addrs = candidate_addrs(&pinfo(None, None, 4242));
        assert_eq!(addrs, vec!["127.0.0.1:4242".parse().unwrap()]);
    }

    #[test]
    fn candidate_addrs_unparseable_skipped() {
        // An ipv4 string that doesn't parse (e.g. a hostname slipped
        // in upstream) is silently skipped; the v6 candidate is still
        // tried. If both are unparseable we end up with the empty-set
        // localhost fallback.
        let addrs = candidate_addrs(&pinfo(Some("not-an-ip"), Some("::1"), 1234));
        assert_eq!(addrs, vec!["[::1]:1234".parse().unwrap()]);

        let addrs = candidate_addrs(&pinfo(Some("not-an-ip"), Some("also-bad"), 1234));
        assert_eq!(addrs, vec!["127.0.0.1:1234".parse().unwrap()]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn race_first_success_returns_first_ok() {
        // Two attempts: one fails fast, one returns Ok after a short
        // sleep. The race should pick the Ok and ignore the failure.
        let addrs = vec![
            "127.0.0.1:1".parse().unwrap(),
            "127.0.0.1:2".parse().unwrap(),
        ];
        let result: Option<(SocketAddr, &'static str)> =
            race_first_success(&addrs, Duration::from_secs(1), |addr| async move {
                if addr.port() == 1 {
                    Err::<&'static str, _>("nope".to_string())
                } else {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    Ok::<&'static str, String>("hello")
                }
            })
            .await;
        let (addr, payload) = result.expect("expected Ok");
        assert_eq!(addr.port(), 2);
        assert_eq!(payload, "hello");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn race_first_success_all_fail_returns_none() {
        let addrs = vec![
            "127.0.0.1:1".parse().unwrap(),
            "127.0.0.1:2".parse().unwrap(),
        ];
        let result: Option<(SocketAddr, ())> =
            race_first_success(&addrs, Duration::from_secs(1), |_addr| async {
                Err::<(), String>("fail".into())
            })
            .await;
        assert!(result.is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn race_first_success_per_attempt_timeout_is_parallel() {
        // All attempts hang past the timeout — race should give up and
        // return None within roughly one timeout window, not
        // sum-of-attempts. Uses a short real-time timeout (50ms) so
        // the test runs fast without depending on tokio's test-util
        // pause feature.
        let addrs = vec![
            "127.0.0.1:1".parse().unwrap(),
            "127.0.0.1:2".parse().unwrap(),
            "127.0.0.1:3".parse().unwrap(),
        ];
        let attempt_timeout = Duration::from_millis(50);
        let start = std::time::Instant::now();
        let result: Option<(SocketAddr, ())> =
            race_first_success(&addrs, attempt_timeout, |_addr| async {
                std::future::pending::<()>().await;
                Ok(())
            })
            .await;
        assert!(result.is_none());
        // Parallel race: all 3 attempts time out together at ~50ms,
        // not 150ms. Allow generous slack (5×) for scheduling so the
        // test isn't flaky on loaded CI but still catches a regression
        // to sequential awaits.
        assert!(
            start.elapsed() < Duration::from_millis(250),
            "race_first_success took {:?} for 3×50ms attempts — expected near-parallel",
            start.elapsed()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn race_first_success_empty_returns_none() {
        let result: Option<(SocketAddr, ())> =
            race_first_success(&[], Duration::from_secs(1), |_addr| async {
                Err::<(), String>("unused".into())
            })
            .await;
        assert!(result.is_none());
    }
}
