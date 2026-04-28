use std::sync::Arc;

use rcgen::KeyPair;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

/// A self-signed certificate + private key pair for QUIC.
pub struct CertPair {
    pub cert_pem: String,
    pub key_pem: String,
    pub cert_der: CertificateDer<'static>,
    pub key_der: PrivateKeyDer<'static>,
}

impl CertPair {
    /// Generate a new self-signed certificate with the given subject name.
    pub fn generate(subject_name: &str) -> Result<Self, String> {
        let mut params = rcgen::CertificateParams::new(vec![subject_name.into()])
            .map_err(|e| e.to_string())?;
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
        quinn::ServerConfig::with_single_cert(
            vec![self.cert_der.clone()],
            self.key_der.clone_key(),
        )
        .map_err(|e| e.to_string())
    }

    /// Build a quinn ClientConfig that trusts this specific cert (for peer connections).
    pub fn client_config_trusting(peer_cert_der: &CertificateDer<'_>) -> Result<quinn::ClientConfig, String> {
        let mut root_store = rustls::RootCertStore::empty();
        root_store.add(peer_cert_der.clone()).map_err(|e| e.to_string())?;
        quinn::ClientConfig::with_root_certificates(Arc::new(root_store))
            .map_err(|e| e.to_string())
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
