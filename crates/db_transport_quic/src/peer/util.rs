//! Peer-transport internal utilities: per-connection variant tag and a
//! tiny PEM certificate parser used when ingesting `PeerConnectionInfo`.

use crate::transport::QuicConnection;
use crate::wss::WssConnection;

/// Internal enum for either QUIC or WSS peer connection.
pub(super) enum PeerConnection {
    Quic(QuicConnection),
    Wss(WssConnection),
}

/// Parse a PEM certificate string to get the DER-encoded certificate.
pub(super) fn parse_cert_pem(pem: &str) -> Option<rustls::pki_types::CertificateDer<'static>> {
    if pem.is_empty() {
        return None;
    }
    // Simple PEM parser: extract base64 between BEGIN/END markers
    let mut in_cert = false;
    let mut b64 = String::new();
    for line in pem.lines() {
        if line.contains("BEGIN CERTIFICATE") {
            in_cert = true;
            continue;
        }
        if line.contains("END CERTIFICATE") {
            break;
        }
        if in_cert {
            b64.push_str(line.trim());
        }
    }
    if b64.is_empty() {
        return None;
    }
    use base64::Engine;
    let der = base64::engine::general_purpose::STANDARD.decode(&b64).ok()?;
    Some(rustls::pki_types::CertificateDer::from(der))
}
