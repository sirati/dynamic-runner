/// Local-host network helpers (hostname, IPv4/IPv6 detection).
///
/// Both `detect_ipv4` and `detect_ipv6` share the same primitive: parsing
/// the space-separated address list from `hostname -I`. On Linux that
/// command emits every non-loopback address configured on every
/// non-loopback interface, both families intermixed — so the only
/// difference between IPv4 and IPv6 detection is which `parse::<…Addr>`
/// the per-token filter applies. Factored into [`first_hostname_addr`]
/// to keep the two callers free of duplicated parsing logic.

use std::net::{Ipv4Addr, Ipv6Addr};
use std::str::FromStr;

pub(crate) fn gethostname() -> String {
    std::process::Command::new("hostname")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".into())
}

/// Run `hostname -I` and return the first token that parses as `T` and
/// passes `acceptable`. `T` is typically [`Ipv4Addr`] or [`Ipv6Addr`].
///
/// `acceptable` is the family-specific "is this an externally-reachable
/// address" filter: rejects loopback / unspecified / link-local etc. so
/// the caller doesn't have to re-walk the address list.
///
/// No outbound network connection is made — this works in air-gapped
/// clusters where the older `8.8.8.8:80` UDP probe failed.
fn first_hostname_addr<T, F>(acceptable: F) -> Option<T>
where
    T: FromStr,
    F: Fn(&T) -> bool,
{
    let output = std::process::Command::new("hostname")
        .arg("-I")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())?;

    output
        .split_whitespace()
        .filter_map(|tok| tok.parse::<T>().ok())
        .find(acceptable)
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
/// Returns `String` (never `Option`): the localhost fallback preserves
/// single-host integration-test behaviour where the secondary's peer
/// dialer must still produce a candidate `SocketAddr` even on a host
/// that has no external IPv4 configured.
pub(crate) fn detect_ipv4(override_ip: Option<&str>) -> String {
    if let Some(ip) = override_ip {
        return ip.to_string();
    }

    first_hostname_addr::<Ipv4Addr, _>(|addr| !addr.is_loopback() && !addr.is_unspecified())
        .map(|a| a.to_string())
        .unwrap_or_else(|| "127.0.0.1".into())
}

/// Detect the local IPv6 address, if any externally-reachable one is
/// configured.
///
/// Resolution order:
///   1. If `override_ip` is `Some`, use it verbatim.
///   2. Otherwise, parse the first non-loopback / non-unspecified /
///      non-link-local IPv6 from `hostname -I`.
///   3. Otherwise, return `None`.
///
/// Unlike [`detect_ipv4`] this returns `Option<String>` rather than a
/// localhost fallback: a real cluster host without any IPv6 NIC should
/// advertise no IPv6, so the peer dialer doesn't waste an attempt
/// racing `[::1]:port` (which always fails to reach a different node).
/// Single-host tests already get an IPv4 candidate from `detect_ipv4`'s
/// fallback, so they don't need an IPv6 loopback.
///
/// Link-local (`fe80::/10`) is excluded because it's only routable
/// when paired with a `%scope_id` interface qualifier — we don't carry
/// that on the wire and a peer on a different host can't dial a bare
/// link-local.
pub(crate) fn detect_ipv6(override_ip: Option<&str>) -> Option<String> {
    if let Some(ip) = override_ip {
        return Some(ip.to_string());
    }

    first_hostname_addr::<Ipv6Addr, _>(|addr| {
        !addr.is_loopback() && !addr.is_unspecified() && !is_unicast_link_local(addr)
    })
    .map(|a| a.to_string())
}

/// `Ipv6Addr::is_unicast_link_local` is unstable on stable Rust; mirror
/// the RFC 4291 §2.5.6 prefix check inline. `fe80::/10` covers
/// `fe80::` through `febf:ffff:…` — the top 10 bits must be
/// `1111 1110 10` = `0xfe80 >> 6 == 0x3fa`.
fn is_unicast_link_local(addr: &Ipv6Addr) -> bool {
    (addr.segments()[0] & 0xffc0) == 0xfe80
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn link_local_classification() {
        assert!(is_unicast_link_local(&"fe80::1".parse().unwrap()));
        assert!(is_unicast_link_local(
            &"febf:ffff:ffff:ffff::1".parse().unwrap()
        ));
        // Just above fe80::/10 — not link-local.
        assert!(!is_unicast_link_local(&"fec0::1".parse().unwrap()));
        // Loopback isn't link-local; the caller filters it separately.
        assert!(!is_unicast_link_local(&"::1".parse().unwrap()));
        // Global unicast.
        assert!(!is_unicast_link_local(&"2001:db8::1".parse().unwrap()));
    }

    #[test]
    fn override_short_circuits() {
        assert_eq!(detect_ipv4(Some("10.0.0.99")), "10.0.0.99");
        assert_eq!(detect_ipv6(Some("2001:db8::1")), Some("2001:db8::1".into()));
    }
}
