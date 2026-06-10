//! Single concern: peer-routable IP resolution, free-port allocation,
//! and the v2 peer-info file (generate.rs:583-587, :600-630). The
//! peer-info byte format is a HARD contract with
//! `crates/dynrunner-slurm/src/peer_info/`. Phase 1 (1E) fills bodies.

use std::fs;
use std::io::Write as _;
use std::net::{TcpListener, ToSocketAddrs};
use std::path::Path;
use std::process::Command;

/// Peer-routable IPv4/IPv6 resolved via the node FQDN (generate.rs:583-585).
#[derive(Debug, Clone, Default)]
pub struct PeerIps {
    pub ipv4: Option<String>,
    pub ipv6: Option<String>,
}

/// Resolve the node's FQDN through NSS and pick the canonical cluster
/// IPv4/IPv6 (the bash `getent ahostsv4/ahostsv6` equivalent).
///
/// Node name is `SLURMD_NODENAME` if set, else `hostname -f`. The name is
/// resolved through `getaddrinfo` (via [`ToSocketAddrs`]) — the same NSS
/// path the bash `getent ahostsv4/ahostsv6` walks — and the first IPv4 /
/// first IPv6 candidate is picked. Any failure leaves the corresponding
/// field `None`, mirroring the bash `${VAR:-<unresolved...>}` fallback.
pub fn detect_peer_ips() -> PeerIps {
    let node = std::env::var("SLURMD_NODENAME")
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| {
            Command::new("hostname")
                .arg("-f")
                .output()
                .ok()
                .filter(|o| o.status.success())
                .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
                .filter(|s| !s.is_empty())
        });

    let mut ips = PeerIps::default();
    if let Some(node) = node {
        // `getaddrinfo` needs a port; `:0` matches the bash NSS path
        // without binding anything.
        if let Ok(addrs) = format!("{node}:0").to_socket_addrs() {
            let addrs: Vec<_> = addrs.collect();
            ips.ipv4 = addrs
                .iter()
                .find(|a| a.is_ipv4())
                .map(|a| a.ip().to_string());
            ips.ipv6 = addrs
                .iter()
                .find(|a| a.is_ipv6())
                .map(|a| a.ip().to_string());
        }
    }

    // Mirror the bash echo lines, including the `<unresolved...>` text.
    println!(
        "Peer-routable IPv4: {}",
        ips.ipv4
            .as_deref()
            .unwrap_or("<unresolved, will fall back to hostname -I>")
    );
    println!(
        "Peer-routable IPv6: {}",
        ips.ipv6
            .as_deref()
            .unwrap_or("<unresolved, will fall back to hostname -I or skip>")
    );
    println!();

    ips
}

/// Bind a socket to port 0 and read back the assigned port
/// (generate.rs:600-601 free-port trick).
///
/// Binds `0.0.0.0:0`, reads the kernel-assigned port, then drops the
/// listener so the port is free for the real listener to claim — the same
/// get-free-port-then-release dance the bash `socket().bind(('',0))`
/// one-liner performs.
pub fn alloc_free_port() -> std::io::Result<u16> {
    let listener = TcpListener::bind("0.0.0.0:0")?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

/// Write `<connection_info_dir>/<secondary_id>.info` in the v2 peer-info
/// format (generate.rs:621-629). Byte-for-byte contract with the
/// peer_info reader — cert_pem_b64 is intentionally omitted.
///
/// Lines, each `\n`-terminated, in this exact order:
///   1. `tcp://<hostname>:<tunnel_port>`
///   2. `version=2`
///   3. `secondary_id=<secondary_id>`
///   4. `ipv4=<v4>`        (only if `ips.ipv4` is `Some`)
///   5. `ipv6=<v6>`        (only if `ips.ipv6` is `Some`)
///   6. `quic_port=<quic_port>`
///   7. `is_observer=<true|false>`
///
/// Returns the exact record content written, so callers can surface it
/// (e.g. in the wrapper log) without re-reading the file. The record
/// holds only addresses/ports/ids — nothing sensitive (cert_pem_b64 is
/// omitted by design).
pub fn write_connection_info(
    connection_info_dir: &Path,
    secondary_id: &str,
    hostname: &str,
    tunnel_port: u16,
    quic_port: u16,
    ips: &PeerIps,
    is_observer: bool,
) -> std::io::Result<String> {
    fs::create_dir_all(connection_info_dir)?;

    let mut out = String::with_capacity(256);
    out.push_str(&format!("tcp://{hostname}:{tunnel_port}\n"));
    out.push_str("version=2\n");
    out.push_str(&format!("secondary_id={secondary_id}\n"));
    if let Some(v4) = &ips.ipv4 {
        out.push_str(&format!("ipv4={v4}\n"));
    }
    if let Some(v6) = &ips.ipv6 {
        out.push_str(&format!("ipv6={v6}\n"));
    }
    out.push_str(&format!("quic_port={quic_port}\n"));
    out.push_str(&format!("is_observer={is_observer}\n"));

    let path = connection_info_dir.join(format!("{secondary_id}.info"));
    let mut f = fs::File::create(&path)?;
    f.write_all(out.as_bytes())?;
    Ok(out)
}

/// Render a peer-info record (as returned by [`write_connection_info`])
/// as a single ` | `-separated line for log output.
pub fn record_log_line(record: &str) -> String {
    record.trim_end_matches('\n').replace('\n', " | ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_connection_info_both_ips_exact_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let ips = PeerIps {
            ipv4: Some("10.0.0.5".to_owned()),
            ipv6: Some("fe80::1".to_owned()),
        };
        let returned = write_connection_info(
            dir.path(),
            "sec-1",
            "node01.cluster",
            40001,
            50001,
            &ips,
            false,
        )
        .unwrap();
        let got = fs::read_to_string(dir.path().join("sec-1.info")).unwrap();
        let expected = "tcp://node01.cluster:40001\n\
            version=2\n\
            secondary_id=sec-1\n\
            ipv4=10.0.0.5\n\
            ipv6=fe80::1\n\
            quic_port=50001\n\
            is_observer=false\n";
        assert_eq!(got, expected);
        assert_eq!(returned, expected);
    }

    #[test]
    fn record_log_line_renders_all_fields_on_one_line() {
        let dir = tempfile::tempdir().unwrap();
        let ips = PeerIps {
            ipv4: Some("10.0.0.5".to_owned()),
            ipv6: Some("fe80::1".to_owned()),
        };
        let record = write_connection_info(
            dir.path(),
            "sec-1",
            "node01.cluster",
            40001,
            50001,
            &ips,
            false,
        )
        .unwrap();
        let line = record_log_line(&record);
        assert_eq!(
            line,
            "tcp://node01.cluster:40001 | version=2 | secondary_id=sec-1 | \
             ipv4=10.0.0.5 | ipv6=fe80::1 | quic_port=50001 | is_observer=false"
        );
        assert!(!line.contains('\n'));
    }

    #[test]
    fn write_connection_info_ipv4_only_exact_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let ips = PeerIps {
            ipv4: Some("10.0.0.5".to_owned()),
            ipv6: None,
        };
        let returned = write_connection_info(
            dir.path(),
            "sec-2",
            "node02.cluster",
            40002,
            50002,
            &ips,
            true,
        )
        .unwrap();
        let got = fs::read_to_string(dir.path().join("sec-2.info")).unwrap();
        assert_eq!(returned, got);
        let expected = "tcp://node02.cluster:40002\n\
            version=2\n\
            secondary_id=sec-2\n\
            ipv4=10.0.0.5\n\
            quic_port=50002\n\
            is_observer=true\n";
        assert_eq!(got, expected);
    }

    #[test]
    fn write_connection_info_no_ips_exact_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let ips = PeerIps {
            ipv4: None,
            ipv6: None,
        };
        let returned = write_connection_info(
            dir.path(),
            "sec-3",
            "node03.cluster",
            40003,
            50003,
            &ips,
            false,
        )
        .unwrap();
        let got = fs::read_to_string(dir.path().join("sec-3.info")).unwrap();
        assert_eq!(returned, got);
        let expected = "tcp://node03.cluster:40003\n\
            version=2\n\
            secondary_id=sec-3\n\
            quic_port=50003\n\
            is_observer=false\n";
        assert_eq!(got, expected);
    }

    #[test]
    fn alloc_free_port_returns_nonzero() {
        let port = alloc_free_port().unwrap();
        assert!(port > 0);
    }

    #[test]
    fn detect_peer_ips_does_not_panic() {
        let _ips: PeerIps = detect_peer_ips();
    }
}
