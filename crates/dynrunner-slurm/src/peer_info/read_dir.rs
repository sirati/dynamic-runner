//! Directory-scan reader for the late-joiner bootstrap path:
//! [`read_dir_v2`] walks a `connection_info/` directory, parses every
//! `*.info` file via [`parse`](super::parse_impl::parse), and returns
//! the v2 records (failing loud rather than masking absent v2
//! envelopes). Errors are carried by [`ReadDirError`].

use super::parse_impl::parse;
use super::types::{PeerInfoError, PeerInfoRecord, PeerInfoVersion};

/// Error returned by [`read_dir_v2`] when the late-joiner bootstrap
/// cannot produce a usable seed list from a peer-info directory.
///
/// Each variant carries the operator-visible context an observer
/// dispatcher needs to fail loudly instead of silently hanging on
/// `join_running_cluster`'s connect-budget. Per the late-joiner
/// design rule "If the seed is empty or all peer-info files are v1
/// (no observer-bootstrap data), fail loud with a clear error message".
#[derive(Debug, thiserror::Error)]
pub enum ReadDirError {
    /// `std::fs::read_dir` failed — typically the directory does not
    /// exist, or the process lacks read permission. Wraps the
    /// underlying `io::Error` so the operator sees the OS-level cause.
    #[error("failed to read peer-info directory `{dir}`: {source}")]
    Io {
        dir: String,
        #[source]
        source: std::io::Error,
    },
    /// A specific file under the directory failed to parse. The file
    /// path is included so the operator can inspect / repair it
    /// without re-running discovery.
    #[error("failed to parse peer-info file `{path}`: {source}")]
    Parse {
        path: String,
        #[source]
        source: PeerInfoError,
    },
    /// The directory was readable but produced zero v2 records. Either
    /// the dir is empty, contains only non-`*.info` files, or every
    /// `*.info` file is v1 (legacy-URI-only) — none of which carry the
    /// `(secondary_id, ipv4/ipv6, quic_port)` triple a late-joiner
    /// needs to dial back into the mesh. `cert_pem_b64` is OPTIONAL in
    /// the v2 envelope: the production wrapper intentionally omits it
    /// (`slurm-wrapper/wrapper/src/network.rs`), and a cert-less record
    /// is still dialable over the WSS/TCP fallback — only the QUIC leg
    /// of the dial needs the cert (the dialer pins it at handshake).
    #[error(
        "peer-info directory `{dir}` produced no v2 records — \
         either the dir is empty / has no `*.info` files, or every \
         file is legacy v1 (pre-Step-7 wrapper). Late-joiner bootstrap \
         requires the v2 envelope (`secondary_id`, `quic_port`, and at \
         least one of `ipv4`/`ipv6`; `cert_pem_b64` is optional — \
         without it the joiner dials over WSS, not QUIC); re-run the \
         cluster with a Step-7-or-newer SLURM wrapper, or supply a \
         directory containing v2 records."
    )]
    NoV2Records { dir: String },
}

/// Scan a directory of peer-info files, parse each, and return only
/// the v2 records — the shape a late-joiner bootstrap needs.
///
/// # Concern
///
/// Single-source helper for "harvest a directory of `<secondary_id>.info`
/// files written by the SLURM wrapper into the bootstrap seed list a
/// late-joining observer feeds into
/// [`crate::PeerTransport::join_running_cluster`]". The filename
/// convention (`*.info`) is defined by `wrapper_script.rs:450`; this
/// helper consults the same constant indirectly via the file-extension
/// filter so a future filename change (e.g. `.peer-info`) only needs
/// to be threaded here once. Non-matching files (logs, temp files,
/// editor backups) are silently skipped — the dir is shared with the
/// SLURM `connection_info` namespace and the parser must not error on
/// neighbours it does not own.
///
/// # Filter logic
///
/// - Only regular files whose name ends with `.info` are considered.
///   Directories, symlinks-to-dirs, and other extensions are skipped.
/// - Each candidate is read into memory and passed through [`parse`].
///   A parse failure on any single candidate surfaces a `Parse` error
///   — the operator's seed dir is malformed and silently dropping
///   would let a typo hide.
/// - Records with `version == V1` are discarded (no quic_port, so the
///   joiner has no mesh port to dial at all). The set of
///   surviving records is returned in directory-enumeration order
///   (whatever the OS reports). Ordering is irrelevant to the joiner
///   — `join_running_cluster` fans the request to all non-self seeds
///   and merges every reply via the idempotent lattice.
/// - If the surviving set is empty, return `NoV2Records` rather than
///   an empty `Vec`. An empty vec would cause `join_running_cluster`
///   to wait its entire connect-budget on `peer_count() > 0` (channel
///   transports pre-wire but real PeerNetwork has nothing to dial) —
///   the diagnostic value of "your seed produced 0 records" outranks
///   the consistency of "Ok(empty vec) is the same as no-op".
///
/// # Module boundary
///
/// Pure file-system I/O + parse delegation. The caller (Step 9
/// dispatcher) is responsible for converting `PeerInfoRecord` →
/// `PeerConnectionInfo` (the wire-frame shape `join_running_cluster`
/// accepts); that translation lives in the caller because it depends
/// on the `Identifier`-generic wire type, which this crate does not
/// see.
pub fn read_dir_v2<P: AsRef<std::path::Path>>(dir: P) -> Result<Vec<PeerInfoRecord>, ReadDirError> {
    let dir_ref = dir.as_ref();
    let dir_display = dir_ref.display().to_string();
    let entries = std::fs::read_dir(dir_ref).map_err(|source| ReadDirError::Io {
        dir: dir_display.clone(),
        source,
    })?;

    let mut out: Vec<PeerInfoRecord> = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|source| ReadDirError::Io {
            dir: dir_display.clone(),
            source,
        })?;
        let path = entry.path();
        // File-extension gate: the SLURM wrapper writes
        // `<secondary_id>.info`. Skipping non-matching neighbours
        // keeps the helper tolerant of a shared dir (logs, tunnel
        // PID files, etc.).
        let is_info_file = path
            .extension()
            .and_then(|e| e.to_str())
            .is_some_and(|e| e == "info");
        if !is_info_file {
            continue;
        }
        // file_type() avoids a separate stat call vs. is_file() and
        // is robust against dangling symlinks the SLURM wrapper would
        // never produce but a manual operator might.
        let file_type = entry.file_type().map_err(|source| ReadDirError::Io {
            dir: dir_display.clone(),
            source,
        })?;
        if !file_type.is_file() {
            continue;
        }

        let contents = std::fs::read_to_string(&path).map_err(|source| ReadDirError::Io {
            dir: dir_display.clone(),
            source,
        })?;
        let record = parse(&contents).map_err(|source| ReadDirError::Parse {
            path: path.display().to_string(),
            source,
        })?;
        // v1 files predate the late-joiner envelope and lack the
        // `(cert, quic_port)` pair the QUIC dial needs. They are
        // legitimate for the gateway's reverse-SSH-tunnel path
        // (line-1-only consumers) but useless to a snapshot joiner;
        // silently drop without erroring so a mid-upgrade dir
        // (some v1, some v2) still yields the usable subset.
        if record.version == PeerInfoVersion::V1 {
            continue;
        }
        out.push(record);
    }

    if out.is_empty() {
        return Err(ReadDirError::NoV2Records { dir: dir_display });
    }
    Ok(out)
}
