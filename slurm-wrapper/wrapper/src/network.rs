//! Single concern: peer-routable IP resolution, free-port allocation,
//! and the v2 peer-info file (generate.rs:583-587, :600-630). The
//! peer-info byte format is a HARD contract with
//! `crates/dynrunner-slurm/src/peer_info/`. Phase 1 (1E) fills bodies.

use std::path::Path;

/// Peer-routable IPv4/IPv6 resolved via the node FQDN (generate.rs:583-585).
#[derive(Debug, Clone, Default)]
pub struct PeerIps {
    pub ipv4: Option<String>,
    pub ipv6: Option<String>,
}

/// Resolve the node's FQDN through NSS and pick the canonical cluster
/// IPv4/IPv6 (the bash `getent ahostsv4/ahostsv6` equivalent).
pub fn detect_peer_ips() -> PeerIps {
    todo!("1E: resolve peer-routable IPv4/IPv6 via FQDN")
}

/// Bind a socket to port 0 and read back the assigned port
/// (generate.rs:600-601 free-port trick).
pub fn alloc_free_port() -> std::io::Result<u16> {
    todo!("1E: bind :0, read assigned port")
}

/// Write `<connection_info_dir>/<secondary_id>.info` in the v2 peer-info
/// format (generate.rs:621-629). Byte-for-byte contract with the
/// peer_info reader — cert_pem_b64 is intentionally omitted.
pub fn write_connection_info(
    _connection_info_dir: &Path,
    _secondary_id: &str,
    _hostname: &str,
    _tunnel_port: u16,
    _quic_port: u16,
    _ips: &PeerIps,
    _is_observer: bool,
) -> std::io::Result<()> {
    todo!("1E: write v2 peer-info file")
}
