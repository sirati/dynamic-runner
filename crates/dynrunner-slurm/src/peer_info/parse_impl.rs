//! Parsers for the connection-info file format: [`parse_v1_uri`] for
//! the legacy line-1 URI, and [`parse`] for the full v1-or-v2 record
//! (URI line plus optional `key=value` envelope). All v2 envelope
//! validation lives here; the `Builder` in [`builder`](super::builder)
//! is the inverse direction.

use std::collections::HashMap;

use super::b64::decode_b64;
use super::types::{LegacyUri, PeerInfoError, PeerInfoRecord, PeerInfoVersion};

/// Parse line 1 of a connection-info file as `<scheme>://<host>:<port>`.
///
/// The scheme is intentionally not validated — the framework's wire
/// convention is `tcp://` for SSH-reverse-tunnel mode and `quic://` for
/// direct mode (see `Primary URL: …` logging in
/// `dynrunner-manager-distributed`), and future schemes may show up
/// without re-spinning the parser. The `url::Url` crate is the
/// authoritative grammar.
pub fn parse_v1_uri(line: &str) -> Result<LegacyUri, PeerInfoError> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return Err(PeerInfoError::Empty);
    }
    let url = url::Url::parse(trimmed)
        .map_err(|e| PeerInfoError::InvalidUri(format!("{trimmed}: {e}")))?;
    let host = url
        .host_str()
        .ok_or_else(|| PeerInfoError::InvalidUri(format!("URL missing host: {trimmed}")))?
        .to_owned();
    let port = url
        .port()
        .ok_or_else(|| PeerInfoError::InvalidUri(format!("URL missing port: {trimmed}")))?;
    Ok(LegacyUri { host, port })
}

/// Parse the full file contents (v1 OR v2) into a [`PeerInfoRecord`].
///
/// Pure (no I/O); reads-from-disk are the caller's concern so the
/// parser can be exercised against in-memory strings in unit tests.
pub fn parse(contents: &str) -> Result<PeerInfoRecord, PeerInfoError> {
    let mut lines = contents.split('\n');
    let line1 = lines.next().ok_or(PeerInfoError::Empty)?;
    let legacy_uri = parse_v1_uri(line1)?;

    // Walk remaining lines collecting key=value pairs. Lines that
    // don't contain `=` and aren't blank are surfaced as a structured
    // error so a typo'd envelope doesn't get silently dropped.
    let mut kvs: HashMap<String, String> = HashMap::new();
    for raw in lines {
        let line = raw.trim();
        if line.is_empty() {
            continue;
        }
        let (k, v) = line
            .split_once('=')
            .ok_or_else(|| PeerInfoError::MalformedEnvelopeLine(line.to_owned()))?;
        kvs.insert(k.trim().to_owned(), v.trim().to_owned());
    }

    let version = match kvs.get("version").map(String::as_str) {
        Some("2") => PeerInfoVersion::V2,
        Some(_) | None if kvs.is_empty() => PeerInfoVersion::V1,
        // Envelope present but `version` absent or unrecognised: treat
        // as v2 (the envelope MUST have come from a v2-aware writer)
        // so unknown future-version files still surface their known
        // keys instead of being treated as a v1 with-noise.
        _ => PeerInfoVersion::V2,
    };

    let quic_port = match kvs.get("quic_port") {
        Some(s) if !s.is_empty() => {
            Some(
                s.parse::<u16>()
                    .map_err(|source| PeerInfoError::InvalidQuicPort {
                        value: s.clone(),
                        source,
                    })?,
            )
        }
        _ => None,
    };
    let is_observer = match kvs.get("is_observer").map(String::as_str) {
        Some("true") => Some(true),
        Some("false") => Some(false),
        Some(other) => return Err(PeerInfoError::InvalidIsObserver(other.to_owned())),
        None => None,
    };
    let cert_pem = match kvs.get("cert_pem_b64") {
        Some(s) if !s.is_empty() => Some(decode_b64(s)?),
        _ => None,
    };

    let ipv4 = kvs.get("ipv4").filter(|s| !s.is_empty()).cloned();
    let ipv6 = kvs.get("ipv6").filter(|s| !s.is_empty()).cloned();
    let secondary_id = kvs.get("secondary_id").filter(|s| !s.is_empty()).cloned();

    Ok(PeerInfoRecord {
        version,
        legacy_uri,
        secondary_id,
        cert_pem,
        ipv4,
        ipv6,
        quic_port,
        is_observer,
    })
}
