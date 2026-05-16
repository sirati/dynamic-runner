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
