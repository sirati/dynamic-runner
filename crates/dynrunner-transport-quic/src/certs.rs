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
    let mut transport = quinn::TransportConfig::default();
    transport.keep_alive_interval(Some(KEEP_ALIVE_INTERVAL));
    // `IdleTimeout::try_from(Duration)` only errors if the duration
    // exceeds the QUIC VarInt-millisecond bound (~2^62 ms); a 60s
    // constant is far inside it, so the conversion is infallible here.
    if let Ok(idle) = quinn::IdleTimeout::try_from(IDLE_TIMEOUT) {
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
}
