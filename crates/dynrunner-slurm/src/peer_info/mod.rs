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
//! (`preparation.rs`) call into [`Builder::format`] / [`parse`] rather
//! than re-implementing the format inline. Any future v3 evolution adds
//! keys here; existing callers see the v2 record shape unchanged.
//!
//! # Layout
//!
//! - [`types`] — `PeerInfoVersion`, `PeerInfoRecord`, `LegacyUri`,
//!   `PeerInfoError`.
//! - [`parse`] — `parse_v1_uri` and `parse`.
//! - [`builder`] — `Builder` + `Builder::format`.
//! - [`b64`] — inline base64 codec (cert envelope field).
//! - [`read_dir`] — `read_dir_v2` + `ReadDirError`.
//! - [`fetch`] — `fetch_dir_v2` + `PeerInfoFetchError` (gateway-side
//!   mirror for the `--gateway` late-joiner path).
//! - [`tests`] — module-internal tests.

mod b64;
mod builder;
mod fetch;
mod parse_impl;
mod read_dir;
#[cfg(test)]
mod tests;
mod types;

pub use builder::Builder;
pub use fetch::{PeerInfoFetchError, fetch_dir_v2};
pub use parse_impl::{parse, parse_v1_uri};
pub use read_dir::{ReadDirError, read_dir_v2};
pub use types::{LegacyUri, PeerInfoError, PeerInfoRecord, PeerInfoVersion};
