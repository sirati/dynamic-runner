//! Local persistence of the cluster's peer connection credentials.
//!
//! # Concern
//!
//! ONE concern: the on-disk round-trip of the roster's
//! [`PeerConnectionInfo`] list — the per-peer pinned QUIC cert PEMs
//! plus the dial info they belong to — at the SUBMITTER's local state
//! dir, and the seed overlay that hands those certs to a late-joiner.
//!
//! # Why this exists
//!
//! The mesh's cert model is self-signed-per-node: each node mints an
//! ephemeral cert (CN = its logical id) at transport start, ships the
//! PEM to the primary in `CertExchange`, and the primary fans the full
//! roster out in the `PeerInfo` broadcast — which is how a SECONDARY
//! can QUIC-dial its peers with valid pinned certs. The setup/submitter
//! is the aggregation point of that material, and it used to hold it
//! ONLY in memory: nothing on disk carried the certs, so a late-joiner
//! observer (seeded from the wrapper's deliberately cert-less `.info`
//! records) could never pin a peer cert and every leg degraded to WSS.
//!
//! The primary persists the roster here at the same moment it fans it
//! out (`peer_setup::send_peer_lists`, gated on
//! [`crate::primary::PrimaryConfig::peer_credentials_path`]); the
//! late-joiner loads it and overlays the certs onto its `.info`-derived
//! seed. A joiner WITHOUT the file keeps today's WSS-fallback behaviour
//! exactly.
//!
//! # Security posture
//!
//! The file is written `0600` (owner-only) into a `0700` parent dir,
//! atomically (tmp file + rename). The payload is the run's mesh
//! credential material — it stays in LOCAL state, never in the shared
//! cluster-visible `connection_info/` dir. (The cert PEMs themselves
//! are technically public — any QUIC client receives them during the
//! handshake, and no private key ever leaves the node that minted it —
//! but the file is handled as a credential so a future payload that
//! does carry secret material inherits safe handling by default.)

use std::io::Write as _;
use std::path::Path;

use dynrunner_protocol_primary_secondary::PeerConnectionInfo;

/// Store the roster's connection credentials at `path`, atomically
/// (same-dir tmp file + rename) with owner-only permissions (`0600`
/// file, `0700` for a parent dir this call creates).
pub fn store_peer_credentials(path: &Path, peers: &[PeerConnectionInfo]) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| format!("peer-credentials path has no parent dir: {}", path.display()))?;
    if !parent.exists() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("creating peer-credentials dir {}: {e}", parent.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))
                .map_err(|e| format!("restricting peer-credentials dir permissions: {e}"))?;
        }
    }
    let json = serde_json::to_string_pretty(peers)
        .map_err(|e| format!("serializing peer credentials: {e}"))?;

    // Atomic replace: write a same-dir tmp file (0600 from the first
    // byte) then rename over the target, so a reader never observes a
    // torn file and a crash mid-write never corrupts an existing one.
    let tmp_path = path.with_extension("tmp");
    {
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        let mut f = opts
            .open(&tmp_path)
            .map_err(|e| format!("creating {}: {e}", tmp_path.display()))?;
        f.write_all(json.as_bytes())
            .map_err(|e| format!("writing {}: {e}", tmp_path.display()))?;
        f.sync_all()
            .map_err(|e| format!("syncing {}: {e}", tmp_path.display()))?;
    }
    std::fs::rename(&tmp_path, path)
        .map_err(|e| format!("renaming {} into place: {e}", tmp_path.display()))?;
    Ok(())
}

/// Load a credentials file previously written by
/// [`store_peer_credentials`]. Missing/unreadable/undecodable files are
/// `Err` with the cause — the CALLER decides whether that is fatal (an
/// explicitly-given path) or a skip (a probed default path).
pub fn load_peer_credentials(path: &Path) -> Result<Vec<PeerConnectionInfo>, String> {
    let bytes = std::fs::read(path)
        .map_err(|e| format!("reading peer credentials {}: {e}", path.display()))?;
    serde_json::from_slice(&bytes)
        .map_err(|e| format!("decoding peer credentials {}: {e}", path.display()))
}

/// Overlay the persisted credentials onto a late-joiner's seed: fill
/// the `cert` field of every seed entry that has NONE (the cert-less
/// `.info`-derived shape) from the credentials entry with the same
/// `secondary_id`. Entries that already carry a cert are left alone
/// (the in-band cert is at least as fresh), and credential entries for
/// peers absent from the seed are ignored (the seed's ADDRESSING stays
/// authoritative — the overlay only supplies the pin).
///
/// Returns how many seed entries received a cert (the caller's
/// operator log names the count).
pub fn overlay_seed_certs(
    seed: &mut [PeerConnectionInfo],
    credentials: &[PeerConnectionInfo],
) -> usize {
    let mut filled = 0;
    for entry in seed.iter_mut() {
        if !entry.cert.is_empty() {
            continue;
        }
        if let Some(cred) = credentials
            .iter()
            .find(|c| c.secondary_id == entry.secondary_id && !c.cert.is_empty())
        {
            entry.cert = cred.cert.clone();
            filled += 1;
        }
    }
    filled
}

/// Persist-at-the-fan-out helper used by the primary: stores when a
/// path is configured, narrates the outcome, and NEVER fails the
/// caller (credential persistence is an observability/bootstrap aid;
/// a full disk must not abort cluster setup).
pub(crate) fn store_if_configured(path: Option<&Path>, peers: &[PeerConnectionInfo]) {
    let Some(path) = path else {
        return;
    };
    match store_peer_credentials(path, peers) {
        Ok(()) => tracing::info!(
            path = %path.display(),
            peers = peers.len(),
            "persisted peer connection credentials (cert pins) to local state"
        ),
        Err(error) => tracing::warn!(
            path = %path.display(),
            error = %error,
            "failed to persist peer connection credentials; a late-joiner \
             on this host will fall back to WSS-only dials"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn entry(id: &str, cert: &str, port: u16) -> PeerConnectionInfo {
        PeerConnectionInfo {
            secondary_id: id.into(),
            cert: cert.into(),
            ipv4: Some("10.0.0.1".into()),
            ipv6: None,
            port,
            is_observer: false,
            liveness_port: Some(7000),
        }
    }

    /// Round-trip: store → load yields the same roster, field by field.
    #[test]
    fn store_load_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("creds").join("peer_credentials.json");
        let peers = vec![entry("sec-0", "CERT-0", 5000), entry("sec-1", "CERT-1", 5001)];
        store_peer_credentials(&path, &peers).unwrap();
        let loaded = load_peer_credentials(&path).unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].secondary_id, "sec-0");
        assert_eq!(loaded[0].cert, "CERT-0");
        assert_eq!(loaded[0].ipv4.as_deref(), Some("10.0.0.1"));
        assert_eq!(loaded[0].port, 5000);
        assert_eq!(loaded[0].liveness_port, Some(7000));
        assert_eq!(loaded[1].secondary_id, "sec-1");
        assert_eq!(loaded[1].cert, "CERT-1");
    }

    /// The credential file is owner-only (0600) and a parent dir the
    /// store created is 0700 — the owner's "it's a private key after
    /// all" handling contract.
    #[cfg(unix)]
    #[test]
    fn store_writes_owner_only_permissions() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("creds").join("peer_credentials.json");
        store_peer_credentials(&path, &[entry("sec-0", "CERT", 5000)]).unwrap();
        let file_mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(file_mode, 0o600, "credential file must be 0600");
        let dir_mode = std::fs::metadata(path.parent().unwrap())
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(dir_mode, 0o700, "created credentials dir must be 0700");
    }

    /// A second store REPLACES the file (the freshness contract: the
    /// fan-out site re-persists the current roster wholesale).
    #[test]
    fn store_replaces_previous_contents() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("peer_credentials.json");
        store_peer_credentials(&path, &[entry("sec-0", "OLD", 5000)]).unwrap();
        store_peer_credentials(&path, &[entry("sec-0", "NEW", 5000)]).unwrap();
        let loaded = load_peer_credentials(&path).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].cert, "NEW");
        // No tmp residue.
        assert!(!path.with_extension("tmp").exists());
    }

    /// Missing file → Err naming the path (the caller decides skip vs
    /// fatal).
    #[test]
    fn load_missing_file_errors_with_path() {
        let err = load_peer_credentials(&PathBuf::from("/nonexistent/peer_credentials.json"))
            .expect_err("missing file must error");
        assert!(err.contains("/nonexistent/peer_credentials.json"), "{err}");
    }

    /// Overlay semantics: fills ONLY empty certs, by id; non-empty seed
    /// certs and unknown ids are untouched; credential entries with an
    /// empty cert never "fill" anything.
    #[test]
    fn overlay_fills_only_empty_certs_by_id() {
        let mut seed = vec![
            entry("sec-0", "", 5000),
            entry("sec-1", "ALREADY", 5001),
            entry("sec-2", "", 5002),
        ];
        let creds = vec![
            entry("sec-0", "CRED-0", 9999), // port differs: addressing must NOT be overlaid
            entry("sec-1", "CRED-1", 5001),
            entry("sec-3", "CRED-3", 5003), // not in seed: ignored
            entry("sec-2", "", 5002),       // empty credential cert: cannot fill
        ];
        let filled = overlay_seed_certs(&mut seed, &creds);
        assert_eq!(filled, 1, "only sec-0 is fillable");
        assert_eq!(seed[0].cert, "CRED-0");
        assert_eq!(seed[0].port, 5000, "overlay must not touch addressing");
        assert_eq!(seed[1].cert, "ALREADY", "non-empty seed cert wins");
        assert_eq!(seed[2].cert, "", "an empty credential cert fills nothing");
    }
}
