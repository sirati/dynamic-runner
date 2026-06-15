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

/// Narration context for one `dial_peer` call: WHICH dial this is, so
/// every attempt/outcome line carries the attempt provenance the
/// operator needs to distinguish "first sweep failed" from "the 5s
/// reconnect ticker is on its Nth retry". Carried alongside the dial —
/// it has no behavioral effect; the dial itself is identical for both.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DialAttempt {
    /// First dial for this peer off a received `PeerInfo` sweep
    /// (`connect_to_peers`).
    Initial,
    /// Reconnect-ticker / router-pulse redial; `attempt` is the
    /// tracker's consecutive-failed-dial count at spawn time (0 when
    /// the redial fired before any tick bumped the counter — e.g. the
    /// immediate redial on first disconnect observation).
    Redial { attempt: u32 },
}

impl std::fmt::Display for DialAttempt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DialAttempt::Initial => write!(f, "initial"),
            DialAttempt::Redial { attempt } => write!(f, "redial#{attempt}"),
        }
    }
}

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

/// Render a peer's candidate `SocketAddr` list into a compact, stable
/// operator string (e.g. `"10.0.0.1:7000, [fe80::1]:7000"`). Used only
/// by the dial-failure summary WARN so an operator can eyeball whether
/// the node is dialing a peer-routable address or a container-internal
/// one. Pure formatting — no behavior, no dialing.
pub(super) fn format_dial_targets(addrs: &[SocketAddr]) -> String {
    addrs
        .iter()
        .map(|a| a.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

/// Race a connection attempt across every `addr` in parallel using
/// `attempt`. Returns the first successful connection along with the
/// address it was made to; returns `Err` with EVERY per-address failure
/// reason (error string, or `"timed out after …"` when the per-attempt
/// timeout fired) if no attempt succeeds — so the caller's outcome log
/// can name WHY each candidate failed instead of just that the race
/// lost (the dial-path silent-branch rule: a failure must carry its
/// reasons to the operator, not just its fact).
///
/// The `attempt` closure produces a future per address. On success,
/// the runner-up futures are dropped (Tokio cancellation), which
/// cancels the underlying connect.
async fn race_first_success<C, F, Fut>(
    addrs: &[SocketAddr],
    timeout: Duration,
    mut attempt: F,
) -> Result<(SocketAddr, C), Vec<(SocketAddr, String)>>
where
    F: FnMut(SocketAddr) -> Fut,
    Fut: std::future::Future<Output = Result<C, String>>,
{
    use futures_util::FutureExt;
    use futures_util::stream::{FuturesUnordered, StreamExt};

    let mut failures: Vec<(SocketAddr, String)> = Vec::new();
    if addrs.is_empty() {
        return Err(failures);
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
            Ok(Ok(conn)) => return Ok((addr, conn)),
            Ok(Err(e)) => {
                tracing::debug!(%addr, error = %e, "peer dial attempt failed");
                failures.push((addr, e));
            }
            Err(_) => {
                tracing::debug!(%addr, "peer dial attempt timed out");
                failures.push((addr, format!("timed out after {timeout:?}")));
            }
        }
    }
    Err(failures)
}

/// Render a failed race's per-address reasons into one compact operator
/// string (`"10.0.0.1:7000: connection refused; [fe80::1]:7000: timed
/// out after 10s"`). Pure formatting for the outcome WARN/ERROR lines.
fn format_failures(failures: &[(SocketAddr, String)]) -> String {
    failures
        .iter()
        .map(|(addr, reason)| format!("{addr}: {reason}"))
        .collect::<Vec<_>>()
        .join("; ")
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
///
/// Narration contract (the silent-branch rule, #362): the dial START
/// logs the peer + every candidate address + the attempt provenance,
/// and EVERY terminal outcome logs — success names the transport +
/// address; failure names every candidate address WITH the reason its
/// attempt ended. No exit from this function is silent.
///
/// Levels are attempt-keyed (the redial-spam rate-limit, #419): the
/// INITIAL sweep is loud — INFO start, WARN/ERROR failures (first
/// contact is operator-significant and one-shot); a 5s-ticker REDIAL is
/// quiet — DEBUG throughout (start AND failures), because the same dead
/// leg would otherwise emit per-tick lines into the full log forever.
/// The redial path's operator-level visibility is owned instead by the
/// throttled `peer unreachable` summary in `process_reconnect_tick`
/// (which fires once per outage + once per recurrence window).
pub(super) async fn dial_peer(
    peer_id: &str,
    peer_info: &PeerConnectionInfo,
    attempt: DialAttempt,
) -> Option<PeerConnection> {
    let addrs = candidate_addrs(peer_info);
    let targets = format_dial_targets(&addrs);
    let peer_cert_der = parse_cert_pem(&peer_info.cert);
    match attempt {
        DialAttempt::Initial => tracing::info!(
            peer = peer_id,
            targets = %targets,
            %attempt,
            "dialing peer (QUIC, then WSS fallback)"
        ),
        DialAttempt::Redial { .. } => tracing::debug!(
            peer = peer_id,
            targets = %targets,
            %attempt,
            "dialing peer (QUIC, then WSS fallback)"
        ),
    }

    match peer_cert_der.as_ref() {
        Ok(cert_der) => match race_quic(&addrs, peer_id, cert_der, ATTEMPT_TIMEOUT).await {
            Ok((addr, conn)) => {
                tracing::info!(peer = peer_id, %addr, %attempt, "connected to peer via QUIC");
                return Some(PeerConnection::Quic(conn));
            }
            Err(failures) => emit_dial_step_failure(
                peer_id,
                attempt,
                "QUIC race to peer failed across all addresses, trying WSS",
                &format_failures(&failures),
            ),
        },
        // No usable cert ⇒ the QUIC race is structurally impossible
        // (nothing to pin the server against). The parser's `Err`
        // carries the SPECIFIC failure (absent vs corrupt cert) so the
        // `reasons=` field is never empty — pre-fix this branch logged
        // `reasons=` blank and the operator could not tell WHY QUIC was
        // skipped (production shape: a late-joiner seeded from cert-less
        // `.info` records).
        Err(reason) => {
            emit_dial_step_failure(peer_id, attempt, "no valid cert for peer, trying WSS", reason)
        }
    }

    match race_wss(&addrs, ATTEMPT_TIMEOUT).await {
        Ok((addr, conn)) => {
            tracing::info!(peer = peer_id, %addr, %attempt, "connected to peer via WSS");
            Some(PeerConnection::Wss(Box::new(conn)))
        }
        Err(failures) => {
            emit_dial_gave_up(peer_id, attempt, &format_failures(&failures));
            None
        }
    }
}

/// Emit a non-terminal dial-step failure (QUIC race lost → WSS next, or
/// no-cert → WSS-only). The LEVEL is attempt-keyed (the redial-spam
/// rate-limit, #419): a first-contact `Initial` dial's step failures are
/// operator-significant and one-shot, so they stay at WARN; a redial's
/// step failures fire on EVERY 5s ticker pulse for a persistently-dead
/// leg, so they drop to DEBUG — the throttled `peer unreachable` summary
/// in `process_reconnect_tick` (count-boundary cadence) owns the redial
/// path's operator-level visibility, so per-tick WARNs here would be
/// pure noise (the bug: 2 lines/5s/dead-leg forever in the full log).
fn emit_dial_step_failure(peer_id: &str, attempt: DialAttempt, msg: &str, reasons: &str) {
    match attempt {
        DialAttempt::Initial => {
            tracing::warn!(peer = peer_id, %attempt, reasons = %reasons, "{msg}")
        }
        DialAttempt::Redial { .. } => {
            tracing::debug!(peer = peer_id, %attempt, reasons = %reasons, "{msg}")
        }
    }
}

/// Emit the terminal "dial gave up" outcome. Same attempt-keyed level as
/// [`emit_dial_step_failure`]: `Initial` is ERROR (a first contact that
/// could reach the peer on no transport is loud), a redial is DEBUG (the
/// throttled summary owns the operator narration; the per-tick failure is
/// expected while the leg stays in the authoritative dial list).
fn emit_dial_gave_up(peer_id: &str, attempt: DialAttempt, reasons: &str) {
    let msg = "WSS race to peer failed across all addresses; dial gave up \
               (the 5s reconnect ticker keeps retrying while the peer stays \
               in the authoritative dial list)";
    match attempt {
        DialAttempt::Initial => {
            tracing::error!(peer = peer_id, %attempt, reasons = %reasons, "{msg}")
        }
        DialAttempt::Redial { .. } => {
            tracing::debug!(peer = peer_id, %attempt, reasons = %reasons, "{msg}")
        }
    }
}

async fn race_quic(
    addrs: &[SocketAddr],
    server_name: &str,
    cert_der: &CertificateDer<'_>,
    timeout: Duration,
) -> Result<(SocketAddr, crate::transport::QuicConnection), Vec<(SocketAddr, String)>> {
    race_first_success(addrs, timeout, |addr| {
        crate::transport::connect(addr, server_name, cert_der)
    })
    .await
}

async fn race_wss(
    addrs: &[SocketAddr],
    timeout: Duration,
) -> Result<(SocketAddr, crate::wss::WssConnection), Vec<(SocketAddr, String)>> {
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
            liveness_port: None,
            slurm_job_id: None,
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
        let result: Result<(SocketAddr, &'static str), _> =
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
    async fn race_first_success_all_fail_returns_every_reason() {
        // The failed race must hand back EVERY per-address reason so
        // the outcome log can name why each candidate failed — the
        // dial-path silent-branch contract (#362). A regression to a
        // reasonless `None` would make the outcome ERROR vague again.
        let addrs: Vec<SocketAddr> = vec![
            "127.0.0.1:1".parse().unwrap(),
            "127.0.0.1:2".parse().unwrap(),
        ];
        let result: Result<(SocketAddr, ()), _> =
            race_first_success(&addrs, Duration::from_secs(1), |addr| async move {
                Err::<(), String>(format!("fail-{}", addr.port()))
            })
            .await;
        let failures = result.expect_err("expected Err with reasons");
        assert_eq!(failures.len(), 2);
        let mut reasons: Vec<String> = failures.iter().map(|(_, r)| r.clone()).collect();
        reasons.sort();
        assert_eq!(reasons, vec!["fail-1".to_string(), "fail-2".to_string()]);
        // And the formatter renders addr+reason pairs for the log line.
        let rendered = format_failures(&failures);
        assert!(rendered.contains("fail-1") && rendered.contains("fail-2"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn race_first_success_timeout_reason_named() {
        // A hung attempt's failure reason must say it TIMED OUT (with
        // the window), not be dropped silently.
        let addrs: Vec<SocketAddr> = vec!["127.0.0.1:1".parse().unwrap()];
        let result: Result<(SocketAddr, ()), _> =
            race_first_success(&addrs, Duration::from_millis(20), |_addr| async {
                std::future::pending::<()>().await;
                Ok(())
            })
            .await;
        let failures = result.expect_err("expected timeout Err");
        assert_eq!(failures.len(), 1);
        assert!(
            failures[0].1.contains("timed out"),
            "timeout reason must be named, got: {}",
            failures[0].1
        );
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
        let result: Result<(SocketAddr, ()), _> =
            race_first_success(&addrs, attempt_timeout, |_addr| async {
                std::future::pending::<()>().await;
                Ok(())
            })
            .await;
        assert!(result.is_err());
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
    async fn race_first_success_empty_returns_err() {
        let result: Result<(SocketAddr, ()), _> =
            race_first_success(&[], Duration::from_secs(1), |_addr| async {
                Err::<(), String>("unused".into())
            })
            .await;
        assert!(
            result
                .expect_err("empty candidate set is a failure")
                .is_empty()
        );
    }
}
