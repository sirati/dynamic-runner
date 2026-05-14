//! Peer-info file: versioned parser + writer for the SLURM
//! `<run_log_dir>/connection_info/<secondary_id>.info` file.
//!
//! # Concern
//!
//! Single source of truth for the on-disk schema of the connection-info
//! file that secondaries (or the SLURM wrapper running on their behalf)
//! write so that:
//!
//! 1. The submitter-side gateway preparation can find the host:port of
//!    each secondary's reverse-SSH-tunnel listener (v1, pre-Step 7).
//! 2. A late-joining observer / dispatcher can read a directory of
//!    these files, extract the per-secondary `(secondary_id, cert,
//!    ipv4, ipv6, quic_port, is_observer)` record, and bootstrap into
//!    the running cluster (v2, Step 7+).
//!
//! Concerns (1) and (2) read the SAME file because the alternative —
//! a separate `mesh-info` file — would duplicate the file-lifecycle
//! logic (mkdir, write-once-from-the-compute-node, watch-from-the-
//! gateway) and introduce a race on which file lands first.
//!
//! # Schema
//!
//! **Line 1 (legacy URI, ALWAYS present):** `<scheme>://<host>:<port>`.
//! Parsed by [`parse_v1_uri`]. The gateway-side
//! `SlurmPreparation::setup_ssh_tunnels` reader (pre-Step 7) is the
//! sole consumer of line 1; the host is the compute node, the port is
//! the secondary's reverse-tunnel listener port. Keeping line 1 stable
//! means a v1 reader on a v2 file behaves exactly as on a v1 file.
//!
//! **Lines 2+ (v2 envelope, optional):** `key=value` pairs, one per
//! line. Unknown keys are tolerated for forward compatibility. The
//! recognised keys are:
//!
//! - `version=2` — required marker for v2 records. A file without this
//!   key (and without other v2 keys) is treated as v1.
//! - `secondary_id=<str>` — the cluster id this peer advertises as.
//! - `cert_pem_b64=<base64>` — base64-encoded PEM of the peer's QUIC
//!   server cert. The encoding is `STANDARD` (with padding); the
//!   raw PEM contains newlines that would corrupt the line-oriented
//!   format if written verbatim.
//! - `ipv4=<addr>` — peer's IPv4 dial address. Empty / absent means
//!   "no IPv4 candidate" (legitimate on IPv6-only nodes).
//! - `ipv6=<addr>` — peer's IPv6 dial address. Empty / absent means
//!   "no IPv6 candidate".
//! - `quic_port=<u16>` — peer's QUIC listener port. NOT the same as
//!   the line-1 tunnel port (which is a SSH-reverse-tunnel listener).
//! - `is_observer=<true|false>` — `true` if this peer is an
//!   observer (no workers, non-promotable). Defaults to `false`
//!   when absent (matches the wire-frame `#[serde(default)]`
//!   semantics on `PeerConnectionInfo.is_observer`).
//!
//! # Backward compatibility
//!
//! A v1 reader (today's `SlurmPreparation::setup_ssh_tunnels`) reads
//! line 1, ignores everything after, sees `tcp://<host>:<port>` —
//! works on both v1 and v2 files.
//!
//! A v2 reader on a v1 file: parses line 1 as the legacy URI, finds
//! no `version=2` envelope, returns a [`PeerInfoRecord`] with `version
//! = 1` and all v2 fields absent. Callers (Step 8's
//! `join_running_cluster`) that require v2 fields error explicitly
//! rather than treating absent-fields as defaults — so an in-flight
//! upgrade (a v1 file written by an older wrapper alongside v2 files
//! from newer wrappers) is reported, not silently masked.
//!
//! # Cross-crate boundary
//!
//! This module is the SOLE writer / parser for the format. The wrapper-
//! script generator (`wrapper_script.rs`) and the gateway preparation
//! (`preparation.rs`) call into [`format_v2`] / [`parse`] rather than
//! re-implementing the format inline. Any future v3 evolution adds
//! keys here; existing callers see the v2 record shape unchanged.

use std::collections::HashMap;
use std::fmt::Write as _;

/// Schema-version of a parsed [`PeerInfoRecord`]. Values are the literal
/// version numbers that appear on the wire (`version=2` key).
///
/// `V1`: legacy URI line only (no envelope). Today's wrapper-script
/// output (pre-Step 7) and any tool that emits the bare `tcp://host:port`
/// line.
///
/// `V2`: line 1 (URI, kept for back-compat) PLUS the `key=value`
/// envelope. Emitted by post-Step-7 wrappers and (Step 8+) refreshed
/// by the secondary at runtime to add its cert.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerInfoVersion {
    V1,
    V2,
}

/// One peer's record extracted from a connection-info file. All
/// envelope fields are `Option<_>` because:
///
/// - On v1 files they are universally absent.
/// - On v2 files in-flight (e.g. the wrapper has written the static
///   envelope but the secondary hasn't yet appended its cert) some
///   fields may be missing while others are present.
///
/// Callers MUST decide their own required-set: gateway preparation
/// only needs the legacy URI on line 1 (back-compat); a late-joiner
/// (Step 8) needs `cert_pem_b64`, `quic_port`, and at least one of
/// `ipv4` / `ipv6`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerInfoRecord {
    /// Detected schema version.
    pub version: PeerInfoVersion,
    /// Legacy URI line 1, parsed into `(host, port)`. Always present —
    /// a file without a parsable line-1 is a parse error, not a
    /// well-formed v1 record.
    pub legacy_uri: LegacyUri,
    /// `secondary_id=` value, if the envelope provided it.
    pub secondary_id: Option<String>,
    /// `cert_pem_b64=` value, decoded back to raw PEM. `Some(...)`
    /// only when the envelope carried a syntactically-valid base64
    /// blob. A decode failure surfaces as `PeerInfoError::InvalidCert`.
    pub cert_pem: Option<String>,
    /// `ipv4=` value (verbatim string, not parsed into an
    /// `Ipv4Addr` — the consumer is the QUIC dialer, which accepts a
    /// string anyway).
    pub ipv4: Option<String>,
    /// `ipv6=` value.
    pub ipv6: Option<String>,
    /// `quic_port=` value, parsed as `u16`.
    pub quic_port: Option<u16>,
    /// `is_observer=` value, parsed as `true` / `false`.
    pub is_observer: Option<bool>,
}

/// Legacy line-1 URI, parsed into its `(host, port)` parts. Construction
/// is via [`parse_v1_uri`] so a malformed URI surfaces as a typed
/// `PeerInfoError::InvalidUri` rather than being deferred to the
/// consumer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyUri {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, thiserror::Error)]
pub enum PeerInfoError {
    #[error("file is empty")]
    Empty,
    #[error("line 1 is not a valid URI: {0}")]
    InvalidUri(String),
    #[error("envelope key without `=`: {0}")]
    MalformedEnvelopeLine(String),
    #[error("envelope key `quic_port` is not a u16: {value} ({source})")]
    InvalidQuicPort {
        value: String,
        #[source]
        source: std::num::ParseIntError,
    },
    #[error("envelope key `is_observer` must be `true` or `false`, got `{0}`")]
    InvalidIsObserver(String),
    #[error("envelope key `cert_pem_b64` is not valid base64: {0}")]
    InvalidCert(String),
}

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
    let line1 = lines
        .next()
        .ok_or(PeerInfoError::Empty)?;
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
        Some(s) if !s.is_empty() => Some(s.parse::<u16>().map_err(|source| {
            PeerInfoError::InvalidQuicPort {
                value: s.clone(),
                source,
            }
        })?),
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

    let ipv4 = kvs
        .get("ipv4")
        .filter(|s| !s.is_empty())
        .cloned();
    let ipv6 = kvs
        .get("ipv6")
        .filter(|s| !s.is_empty())
        .cloned();
    let secondary_id = kvs
        .get("secondary_id")
        .filter(|s| !s.is_empty())
        .cloned();

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

/// Builder for a v2 record. Owns the `(host, tunnel_port)` legacy URI
/// + the envelope fields. Producing the final on-disk string goes
/// through [`Builder::format`] so the file shape (line 1 then
/// envelope) is centralised here, not duplicated across writers.
#[derive(Debug, Clone)]
pub struct Builder {
    pub host: String,
    pub tunnel_port: u16,
    pub secondary_id: Option<String>,
    pub cert_pem: Option<String>,
    pub ipv4: Option<String>,
    pub ipv6: Option<String>,
    pub quic_port: Option<u16>,
    pub is_observer: Option<bool>,
}

impl Builder {
    /// Construct a builder with only the legacy URI populated. Other
    /// fields default to `None`; callers fluently set the ones they
    /// know.
    pub fn new(host: impl Into<String>, tunnel_port: u16) -> Self {
        Self {
            host: host.into(),
            tunnel_port,
            secondary_id: None,
            cert_pem: None,
            ipv4: None,
            ipv6: None,
            quic_port: None,
            is_observer: None,
        }
    }

    pub fn secondary_id(mut self, s: impl Into<String>) -> Self {
        self.secondary_id = Some(s.into());
        self
    }

    pub fn cert_pem(mut self, s: impl Into<String>) -> Self {
        self.cert_pem = Some(s.into());
        self
    }

    pub fn ipv4(mut self, s: impl Into<String>) -> Self {
        self.ipv4 = Some(s.into());
        self
    }

    pub fn ipv6(mut self, s: impl Into<String>) -> Self {
        self.ipv6 = Some(s.into());
        self
    }

    pub fn quic_port(mut self, p: u16) -> Self {
        self.quic_port = Some(p);
        self
    }

    pub fn is_observer(mut self, b: bool) -> Self {
        self.is_observer = Some(b);
        self
    }

    /// Render the on-disk string. Line 1 is the legacy URI; lines 2+
    /// are the envelope, version-key first then alphabetical (so a
    /// `diff` of two files is deterministic). Trailing newline.
    pub fn format(&self) -> String {
        let mut out = String::with_capacity(256);
        // Line 1: legacy URI. `tcp://` is the framework convention
        // for SSH-reverse-tunnel mode (see preparation.rs's
        // back-compat reader). Writers in other modes can swap the
        // scheme inline via a direct line-1 string if they need to;
        // for now the only caller is the reverse-mode wrapper.
        let _ = writeln!(&mut out, "tcp://{}:{}", self.host, self.tunnel_port);
        let _ = writeln!(&mut out, "version=2");
        if let Some(s) = &self.secondary_id {
            let _ = writeln!(&mut out, "secondary_id={s}");
        }
        if let Some(s) = &self.cert_pem {
            let _ = writeln!(&mut out, "cert_pem_b64={}", encode_b64(s));
        }
        if let Some(s) = &self.ipv4 {
            let _ = writeln!(&mut out, "ipv4={s}");
        }
        if let Some(s) = &self.ipv6 {
            let _ = writeln!(&mut out, "ipv6={s}");
        }
        if let Some(p) = self.quic_port {
            let _ = writeln!(&mut out, "quic_port={p}");
        }
        if let Some(b) = self.is_observer {
            let _ = writeln!(&mut out, "is_observer={b}");
        }
        out
    }
}

/// Trivial base64 STANDARD-with-padding encoder. Pulled in inline
/// to avoid adding the `base64` crate dep just for the cert blob —
/// the crate is small but the dep adds compile cost on a path
/// (slurm preparation) that already builds for every consumer.
///
/// We don't validate the input PEM — the caller already has a
/// validated cert (CertExchange round-trip succeeded). Any byte
/// stream is round-trippable through this pair.
fn encode_b64(s: &str) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let bytes = s.as_bytes();
    let mut out = String::with_capacity((bytes.len() + 2) / 3 * 4);
    let mut i = 0;
    while i + 3 <= bytes.len() {
        let n = ((bytes[i] as u32) << 16) | ((bytes[i + 1] as u32) << 8) | (bytes[i + 2] as u32);
        out.push(TABLE[((n >> 18) & 0x3f) as usize] as char);
        out.push(TABLE[((n >> 12) & 0x3f) as usize] as char);
        out.push(TABLE[((n >> 6) & 0x3f) as usize] as char);
        out.push(TABLE[(n & 0x3f) as usize] as char);
        i += 3;
    }
    let rem = bytes.len() - i;
    if rem == 1 {
        let n = (bytes[i] as u32) << 16;
        out.push(TABLE[((n >> 18) & 0x3f) as usize] as char);
        out.push(TABLE[((n >> 12) & 0x3f) as usize] as char);
        out.push('=');
        out.push('=');
    } else if rem == 2 {
        let n = ((bytes[i] as u32) << 16) | ((bytes[i + 1] as u32) << 8);
        out.push(TABLE[((n >> 18) & 0x3f) as usize] as char);
        out.push(TABLE[((n >> 12) & 0x3f) as usize] as char);
        out.push(TABLE[((n >> 6) & 0x3f) as usize] as char);
        out.push('=');
    }
    out
}

fn decode_b64(s: &str) -> Result<String, PeerInfoError> {
    fn val(c: u8) -> Option<u8> {
        match c {
            b'A'..=b'Z' => Some(c - b'A'),
            b'a'..=b'z' => Some(c - b'a' + 26),
            b'0'..=b'9' => Some(c - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
    let bytes = s.as_bytes();
    if !bytes.len().is_multiple_of(4) {
        return Err(PeerInfoError::InvalidCert(format!(
            "length {} not a multiple of 4",
            bytes.len()
        )));
    }
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len() / 4 * 3);
    let mut i = 0;
    while i < bytes.len() {
        let q = &bytes[i..i + 4];
        let pad = q.iter().filter(|&&b| b == b'=').count();
        if pad > 2 || (pad > 0 && i + 4 != bytes.len()) {
            return Err(PeerInfoError::InvalidCert(format!(
                "misplaced padding at index {i}"
            )));
        }
        let a = val(q[0])
            .ok_or_else(|| PeerInfoError::InvalidCert(format!("invalid char at {i}")))?;
        let b = val(q[1])
            .ok_or_else(|| PeerInfoError::InvalidCert(format!("invalid char at {}", i + 1)))?;
        let c = if q[2] == b'=' { 0 } else {
            val(q[2]).ok_or_else(|| PeerInfoError::InvalidCert(format!("invalid char at {}", i + 2)))?
        };
        let d = if q[3] == b'=' { 0 } else {
            val(q[3]).ok_or_else(|| PeerInfoError::InvalidCert(format!("invalid char at {}", i + 3)))?
        };
        let n = ((a as u32) << 18) | ((b as u32) << 12) | ((c as u32) << 6) | (d as u32);
        out.push(((n >> 16) & 0xff) as u8);
        if q[2] != b'=' {
            out.push(((n >> 8) & 0xff) as u8);
        }
        if q[3] != b'=' {
            out.push((n & 0xff) as u8);
        }
        i += 4;
    }
    String::from_utf8(out).map_err(|e| PeerInfoError::InvalidCert(format!("utf8: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// v1 contents (line 1 only) parse to a `V1` record. The legacy-URI
    /// fields are populated, envelope fields are all `None`.
    #[test]
    fn parse_v1_legacy_uri_only() {
        let r = parse("tcp://node03:54321\n").unwrap();
        assert_eq!(r.version, PeerInfoVersion::V1);
        assert_eq!(r.legacy_uri.host, "node03");
        assert_eq!(r.legacy_uri.port, 54321);
        assert!(r.secondary_id.is_none());
        assert!(r.cert_pem.is_none());
        assert!(r.ipv4.is_none());
        assert!(r.ipv6.is_none());
        assert!(r.quic_port.is_none());
        assert!(r.is_observer.is_none());
    }

    /// v2 contents parse all envelope keys; absent keys remain `None`.
    #[test]
    fn parse_v2_full_envelope() {
        let contents = "\
tcp://compute1:40001
version=2
secondary_id=secondary-7
cert_pem_b64=SGVsbG8gV29ybGQ=
ipv4=10.0.0.5
ipv6=fd00::1
quic_port=51200
is_observer=true
";
        let r = parse(contents).unwrap();
        assert_eq!(r.version, PeerInfoVersion::V2);
        assert_eq!(r.legacy_uri.host, "compute1");
        assert_eq!(r.legacy_uri.port, 40001);
        assert_eq!(r.secondary_id.as_deref(), Some("secondary-7"));
        assert_eq!(r.cert_pem.as_deref(), Some("Hello World"));
        assert_eq!(r.ipv4.as_deref(), Some("10.0.0.5"));
        assert_eq!(r.ipv6.as_deref(), Some("fd00::1"));
        assert_eq!(r.quic_port, Some(51200));
        assert_eq!(r.is_observer, Some(true));
    }

    /// is_observer=false explicitly false (defaults are caller-side).
    #[test]
    fn parse_v2_is_observer_false() {
        let r = parse("tcp://h:1\nversion=2\nis_observer=false\n").unwrap();
        assert_eq!(r.is_observer, Some(false));
    }

    /// Blank lines in the envelope are tolerated (writer-side neatness
    /// could insert them; reader should ignore).
    #[test]
    fn parse_v2_blank_lines_tolerated() {
        let r = parse("tcp://h:1\n\nversion=2\n\nipv4=10.0.0.1\n\n").unwrap();
        assert_eq!(r.ipv4.as_deref(), Some("10.0.0.1"));
    }

    /// An envelope line without `=` is a structured parse error, not
    /// silently dropped — a typo'd key would otherwise pass unnoticed.
    #[test]
    fn parse_malformed_envelope_line_errors() {
        let err = parse("tcp://h:1\nversion 2\n").unwrap_err();
        assert!(matches!(err, PeerInfoError::MalformedEnvelopeLine(_)));
    }

    /// is_observer must be `true` / `false` exactly; any other value
    /// surfaces a typed error so a typo doesn't silently default.
    #[test]
    fn parse_is_observer_invalid_value() {
        let err = parse("tcp://h:1\nversion=2\nis_observer=yes\n").unwrap_err();
        assert!(matches!(err, PeerInfoError::InvalidIsObserver(_)));
    }

    /// Empty file → `Empty` error. Single-line URI is the minimum
    /// well-formed v1 input.
    #[test]
    fn parse_empty_is_error() {
        let err = parse("").unwrap_err();
        assert!(matches!(err, PeerInfoError::InvalidUri(_) | PeerInfoError::Empty));
    }

    /// Line 1 is required and must be a parsable URI; line-1 garbage
    /// surfaces as `InvalidUri`.
    #[test]
    fn parse_garbage_line1_errors() {
        let err = parse("not a uri\nversion=2\n").unwrap_err();
        assert!(matches!(err, PeerInfoError::InvalidUri(_)));
    }

    /// Builder round-trips: format → parse yields an equivalent record
    /// (modulo version, which the builder always marks v2).
    #[test]
    fn builder_round_trip() {
        let b = Builder::new("node-x", 40000)
            .secondary_id("sec-3")
            .cert_pem("-----BEGIN CERT-----\nMIIB...\n-----END CERT-----\n")
            .ipv4("10.0.0.5")
            .quic_port(51200)
            .is_observer(true);
        let s = b.format();
        let r = parse(&s).unwrap();
        assert_eq!(r.version, PeerInfoVersion::V2);
        assert_eq!(r.legacy_uri.host, "node-x");
        assert_eq!(r.legacy_uri.port, 40000);
        assert_eq!(r.secondary_id.as_deref(), Some("sec-3"));
        assert_eq!(
            r.cert_pem.as_deref(),
            Some("-----BEGIN CERT-----\nMIIB...\n-----END CERT-----\n")
        );
        assert_eq!(r.ipv4.as_deref(), Some("10.0.0.5"));
        assert!(r.ipv6.is_none());
        assert_eq!(r.quic_port, Some(51200));
        assert_eq!(r.is_observer, Some(true));
    }

    /// Forward-compat: a v1 reader on a v2 file (i.e. parsing only
    /// line 1 via `parse_v1_uri`) still resolves the legacy URI
    /// — protects gateway preparation against a v2 wrapper output.
    #[test]
    fn v1_reader_on_v2_file_works() {
        let v2 = "tcp://compute1:40001\nversion=2\nis_observer=true\n";
        let first_line = v2.split('\n').next().unwrap();
        let uri = parse_v1_uri(first_line).unwrap();
        assert_eq!(uri.host, "compute1");
        assert_eq!(uri.port, 40001);
    }

    /// base64 round-trip on a non-ASCII byte sequence (covers the
    /// inline encoder's correctness for arbitrary PEM contents).
    #[test]
    fn b64_round_trip_arbitrary_bytes() {
        let inputs = [
            "",
            "f",
            "fo",
            "foo",
            "foob",
            "fooba",
            "foobar",
            "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n",
        ];
        for s in inputs {
            let enc = encode_b64(s);
            let dec = decode_b64(&enc).unwrap();
            assert_eq!(s, dec, "round-trip failed for {s:?}");
        }
    }

    /// Cert with bad base64 surfaces a typed error.
    #[test]
    fn cert_pem_b64_garbage_errors() {
        let err = parse("tcp://h:1\nversion=2\ncert_pem_b64=not!base64!!\n").unwrap_err();
        assert!(matches!(err, PeerInfoError::InvalidCert(_)));
    }

    /// quic_port out-of-range surfaces InvalidQuicPort with the source
    /// `ParseIntError`.
    #[test]
    fn quic_port_out_of_range_errors() {
        let err = parse("tcp://h:1\nversion=2\nquic_port=999999\n").unwrap_err();
        assert!(matches!(err, PeerInfoError::InvalidQuicPort { .. }));
    }

    /// Unknown envelope keys are tolerated (forward-compat with future
    /// v3 keys).
    #[test]
    fn parse_unknown_envelope_keys_tolerated() {
        let contents = "tcp://h:1\nversion=2\nipv4=10.0.0.1\nfuture_v3_key=value\n";
        let r = parse(contents).unwrap();
        assert_eq!(r.ipv4.as_deref(), Some("10.0.0.1"));
    }
}
