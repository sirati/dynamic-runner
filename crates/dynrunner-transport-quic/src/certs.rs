use std::sync::Arc;
use std::time::Duration;

use rcgen::KeyPair;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

/// QUIC keep-alive PING cadence. The endpoint emits a PING whenever
/// the connection has been idle this long, so even an
/// application-silent-but-live link keeps fresh acknowledged packets
/// flowing. 5s is fast enough that several PINGs accrue inside
/// `IDLE_TIMEOUT`, slow enough to be negligible wire overhead.
const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(5);

/// QUIC idle timeout. The connection is closed when no packets —
/// including the acknowledgements for the keep-alive PINGs above —
/// arrive within this window, which is the QUIC-layer detector for a
/// blackholed (half-open) tunnel: the socket may still exist, but if
/// data is silently dropped the unacked PINGs trip this timeout and
/// the connection errors, which surfaces to the reader/writer tasks.
///
/// MUST exceed the secondary's primary-link failure window
/// (`DEFAULT_FAILURE_WINDOW = 30s` in
/// `dynrunner_manager_distributed::secondary::primary_link`) plus
/// several `KEEP_ALIVE_INTERVAL`s, so a healthy-but-application-quiet
/// bootstrap primary↔secondary wire is NOT closed at the QUIC layer
/// (which would tear down `connections` and trip a spurious failover).
/// 60s = 30s window + 6 PING intervals of headroom satisfies this.
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// A self-signed certificate + private key pair for QUIC.
pub struct CertPair {
    pub cert_pem: String,
    pub key_pem: String,
    pub cert_der: CertificateDer<'static>,
    pub key_der: PrivateKeyDer<'static>,
}

/// Build the shared QUIC [`quinn::TransportConfig`] applied to BOTH
/// the server and client side. Centralised here so the keep-alive /
/// idle-timeout pair is defined exactly once and both builders are
/// guaranteed to agree (an asymmetric timeout would let one side
/// declare the link dead while the other still believed it live).
///
/// Honest-liveness rationale: a stock quinn connection ships with
/// `keep_alive_interval = None` and `max_idle_timeout` unset, so a
/// half-open tunnel (socket present, data blackholed) is never
/// detected below the application layer — the link looks "connected"
/// forever. Emitting PINGs and timing out on their unacked silence
/// makes "connected" honest at the QUIC layer; the reader/writer tasks
/// then observe the connection error and drive prune+redial.
fn transport_config() -> Arc<quinn::TransportConfig> {
    transport_config_with(Some(KEEP_ALIVE_INTERVAL), IDLE_TIMEOUT)
}

/// Parameterised core of [`transport_config`]. Production always passes
/// the shipped `(KEEP_ALIVE_INTERVAL, IDLE_TIMEOUT)` pair; the
/// parameters exist so the idle-death tests can replay the
/// welcomed-but-unserved bring-up window at test timescales (a
/// shortened idle timeout) with and without the keep-alive half — the
/// only way to pin that the PINGs, not luck, are what keep an
/// app-silent wire alive past the idle window.
fn transport_config_with(
    keep_alive: Option<Duration>,
    idle_timeout: Duration,
) -> Arc<quinn::TransportConfig> {
    let mut transport = quinn::TransportConfig::default();
    transport.keep_alive_interval(keep_alive);
    // `IdleTimeout::try_from(Duration)` only errors if the duration
    // exceeds the QUIC VarInt-millisecond bound (~2^62 ms); a 60s
    // constant is far inside it, so the conversion is infallible here.
    if let Ok(idle) = quinn::IdleTimeout::try_from(idle_timeout) {
        transport.max_idle_timeout(Some(idle));
    }
    Arc::new(transport)
}

impl CertPair {
    /// Generate a new self-signed certificate with the given subject name.
    pub fn generate(subject_name: &str) -> Result<Self, String> {
        let mut params =
            rcgen::CertificateParams::new(vec![subject_name.into()]).map_err(|e| e.to_string())?;
        params.distinguished_name = rcgen::DistinguishedName::new();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, subject_name);

        let key_pair = KeyPair::generate().map_err(|e| e.to_string())?;
        let cert = params.self_signed(&key_pair).map_err(|e| e.to_string())?;

        let cert_pem = cert.pem();
        let key_pem = key_pair.serialize_pem();
        let cert_der = cert.der().clone();
        let key_der = PrivateKeyDer::from(PrivatePkcs8KeyDer::from(key_pair.serialize_der()));

        Ok(Self {
            cert_pem,
            key_pem,
            cert_der,
            key_der,
        })
    }

    /// Build a quinn ServerConfig using this cert.
    pub fn server_config(&self) -> Result<quinn::ServerConfig, String> {
        let mut config = quinn::ServerConfig::with_single_cert(
            vec![self.cert_der.clone()],
            self.key_der.clone_key(),
        )
        .map_err(|e| e.to_string())?;
        config.transport_config(transport_config());
        Ok(config)
    }

    /// Build a quinn ClientConfig that trusts this specific cert (for peer connections).
    pub fn client_config_trusting(
        peer_cert_der: &CertificateDer<'_>,
    ) -> Result<quinn::ClientConfig, String> {
        let mut root_store = rustls::RootCertStore::empty();
        root_store
            .add(peer_cert_der.clone())
            .map_err(|e| e.to_string())?;
        let mut config = quinn::ClientConfig::with_root_certificates(Arc::new(root_store))
            .map_err(|e| e.to_string())?;
        config.transport_config(transport_config());
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_and_build_server_config() {
        let pair = CertPair::generate("test-node").unwrap();
        assert!(!pair.cert_pem.is_empty());
        assert!(!pair.key_pem.is_empty());

        let _config = pair.server_config().unwrap();
    }

    #[test]
    fn client_config_trusts_self_signed() {
        let pair = CertPair::generate("test-node").unwrap();
        let _client = CertPair::client_config_trusting(&pair.cert_der).unwrap();
    }

    /// The shipped pair's contract: several keep-alive PINGs must accrue
    /// inside the idle window, or a transient PING/ack loss closes a
    /// healthy wire. Pins the constants' relation so a future edit to
    /// either cannot silently break the liveness story.
    #[test]
    fn shipped_keepalive_accrues_pings_inside_idle_window() {
        assert!(
            KEEP_ALIVE_INTERVAL * 3 <= IDLE_TIMEOUT,
            "keep-alive must fire several times inside the idle timeout"
        );
    }

    /// Drive one real QUIC connection with the given transport-config
    /// parameters, hold it APPLICATION-SILENT for `silent_for`, and
    /// return its `close_reason()` (`None` = still alive). Both sides
    /// get the same config, exactly as [`transport_config`] guarantees
    /// in production.
    async fn idle_wire_close_reason(
        keep_alive: Option<Duration>,
        idle_timeout: Duration,
        silent_for: Duration,
    ) -> Option<quinn::ConnectionError> {
        let cert = CertPair::generate("idle-test").unwrap();

        let mut server_cfg = quinn::ServerConfig::with_single_cert(
            vec![cert.cert_der.clone()],
            cert.key_der.clone_key(),
        )
        .unwrap();
        server_cfg.transport_config(transport_config_with(keep_alive, idle_timeout));
        let server =
            quinn::Endpoint::server(server_cfg, "127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = server.local_addr().unwrap();

        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert.cert_der.clone()).unwrap();
        let mut client_cfg =
            quinn::ClientConfig::with_root_certificates(Arc::new(roots)).unwrap();
        client_cfg.transport_config(transport_config_with(keep_alive, idle_timeout));
        let mut client = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
        client.set_default_client_config(client_cfg);

        let (client_conn, server_conn) = tokio::join!(
            async { client.connect(addr, "idle-test").unwrap().await.unwrap() },
            async { server.accept().await.unwrap().await.unwrap() }
        );

        // The bring-up hostage window: NOTHING application-level flows
        // on the wire while the member waits to be served.
        tokio::time::sleep(silent_for).await;

        // Keep the server side alive for the whole window so the only
        // possible death is the idle timeout, never a peer teardown.
        drop(server_conn);
        client_conn.close_reason()
    }

    /// The GAP replay (the welcomed-but-unserved bring-up shape): an
    /// application-silent wire on a keep-alive-LESS config dies at the
    /// QUIC idle timeout — the failure mode the production RCA observed
    /// on a welcomed member whose setup was delayed past the idle
    /// window.
    #[tokio::test]
    async fn app_silent_wire_dies_at_idle_timeout_without_keepalive() {
        let reason = idle_wire_close_reason(
            None,
            Duration::from_secs(1),
            Duration::from_secs(3),
        )
        .await;
        assert!(
            matches!(reason, Some(quinn::ConnectionError::TimedOut)),
            "an app-silent wire without keep-alive must idle-die \
             (the pre-fix bring-up hostage failure mode); got {reason:?}"
        );
    }

    /// The fix half: the SAME app-silent window with the keep-alive arm
    /// (the shipped config shape — PING cadence well inside the idle
    /// timeout) keeps the wire alive past several idle windows.
    #[tokio::test]
    async fn app_silent_wire_survives_idle_window_with_keepalive() {
        let reason = idle_wire_close_reason(
            Some(Duration::from_millis(250)),
            Duration::from_secs(1),
            Duration::from_secs(3),
        )
        .await;
        assert!(
            reason.is_none(),
            "keep-alive PINGs must keep an app-silent wire alive past \
             the idle window (the shipped transport-config contract); \
             wire closed with {reason:?}"
        );
    }
}
