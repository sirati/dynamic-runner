/// Local-host network helpers (hostname, IPv4 detection).

pub(crate) fn gethostname() -> String {
    std::process::Command::new("hostname")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".into())
}

/// Detect the local IPv4 address.
///
/// Resolution order:
///   1. If `override_ip` is `Some`, use it verbatim. This is the explicit
///      Python-side `local_ip` config knob (wired in via PrimaryConfig /
///      SecondaryConfig once those typed configs exist).
///   2. Otherwise, parse the first non-loopback IPv4 from `hostname -I`.
///   3. Otherwise, fall back to "127.0.0.1".
///
/// No outbound network connection is made — this works in air-gapped
/// clusters where the previous `8.8.8.8:80` UDP probe failed.
pub(crate) fn detect_ipv4(override_ip: Option<&str>) -> String {
    if let Some(ip) = override_ip {
        return ip.to_string();
    }

    let output = std::process::Command::new("hostname")
        .arg("-I")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok());

    if let Some(line) = output {
        for token in line.split_whitespace() {
            if let Ok(addr) = token.parse::<std::net::Ipv4Addr>() {
                if !addr.is_loopback() && !addr.is_unspecified() {
                    return addr.to_string();
                }
            }
        }
    }

    "127.0.0.1".into()
}
