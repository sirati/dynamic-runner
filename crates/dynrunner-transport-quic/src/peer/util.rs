//! Peer-transport internal utilities: per-connection variant tag and a
//! tiny PEM certificate parser used when ingesting `PeerConnectionInfo`.

use crate::transport::QuicConnection;
use crate::wss::WssConnection;

/// Internal enum for either QUIC or WSS peer connection.
///
/// `Wss` is boxed because `WssConnection` is ~3× the size of the
/// QUIC variant; leaving it inline blew the enum to ~304 bytes
/// (clippy::large_enum_variant). The Box is consumed at handshake
/// time when we destructure into the per-connection reader/writer
/// halves.
pub(super) enum PeerConnection {
    Quic(QuicConnection),
    Wss(Box<WssConnection>),
}

/// Parse a PEM certificate string to get the DER-encoded certificate.
///
/// Returns `Err` with the SPECIFIC validation failure (the
/// dial-path silent-branch rule: the no-valid-cert WARN in
/// `dial::dial_peer` surfaces this string as its `reasons=` field, so
/// the operator can tell "the seed record simply carries no cert —
/// e.g. a late-joiner reading the wrapper's cert-less `.info` files"
/// apart from "a cert travelled but is corrupt").
pub(super) fn parse_cert_pem(
    pem: &str,
) -> Result<rustls::pki_types::CertificateDer<'static>, String> {
    if pem.is_empty() {
        return Err(
            "peer record carries no certificate (empty cert field — e.g. a seed built \
             from cert-less peer-info records); QUIC needs the peer's pinned cert"
                .to_string(),
        );
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
        return Err("no CERTIFICATE PEM block found in the peer record's cert field".to_string());
    }
    use base64::Engine;
    let der = base64::engine::general_purpose::STANDARD
        .decode(&b64)
        .map_err(|e| format!("cert PEM base64 failed to decode: {e}"))?;
    Ok(rustls::pki_types::CertificateDer::from(der))
}
