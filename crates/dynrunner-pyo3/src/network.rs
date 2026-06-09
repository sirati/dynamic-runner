//! Local-host network helpers (hostname, IPv4/IPv6 detection).
//!
//! Both `detect_ipv4_with_source` and `detect_ipv6_with_source` share the
//! same primitive: parsing
//! the space-separated address list from `hostname -I`. On Linux that
//! command emits every non-loopback address configured on every
//! non-loopback interface, both families intermixed — so the only
//! difference between IPv4 and IPv6 detection is which `parse::<…Addr>`
//! the per-token filter applies. Factored into [`first_hostname_addr`]
//! to keep the two callers free of duplicated parsing logic.
//!
//! Resolution honours an env-var hint (`PRIMARY_NODE_IPV4`,
//! `PRIMARY_NODE_IPV6`) before falling back to `hostname -I`. The hint
//! is for deployments where `hostname -I` doesn't yield a peer-routable
//! address — e.g. a SLURM compute node whose first non-loopback IPv4
//! is a podman/CNI bridge or an unrouted secondary NIC, while the
//! cluster's actual LAN address is reachable via DNS on the host's
//! FQDN. The SLURM wrapper computes the routable IP on the host and
//! exports it; everywhere else the env var is unset and detection
//! behaves exactly as before.

use std::net::{Ipv4Addr, Ipv6Addr};
use std::str::FromStr;

/// Env var the SLURM wrapper sets to the host's peer-routable IPv4.
/// Consumed by [`detect_ipv4_with_source`] when no explicit `override_ip`
/// argument is passed.
const ENV_HOST_IPV4: &str = "PRIMARY_NODE_IPV4";

/// Env var the SLURM wrapper sets to the host's peer-routable IPv6.
/// Consumed by [`detect_ipv6_with_source`].
const ENV_HOST_IPV6: &str = "PRIMARY_NODE_IPV6";

/// Which resolution rung produced a detected address. Surfaced
/// alongside the address by [`detect_ipv4_with_source`] /
/// [`detect_ipv6_with_source`] so a node can log, at startup, not just
/// the address it will advertise to peers but WHERE that address came
/// from — the operator-facing datum that distinguishes "the SLURM
/// wrapper handed me a routable LAN IP" from "I fell back to whatever
/// `hostname -I` printed first (possibly a podman/CNI bridge addr no
/// peer can reach)" from "I have no external address at all".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AddrSource {
    /// The explicit Python-side `local_ip` override argument.
    Override,
    /// The `PRIMARY_NODE_IPV4` / `PRIMARY_NODE_IPV6` env-var hint
    /// (set, non-empty) — the SLURM wrapper's host-IP probe.
    EnvHint,
    /// First acceptable address parsed from `hostname -I`.
    HostnameProbe,
    /// IPv4-only: neither hint nor probe yielded an address, so the
    /// localhost fallback (`127.0.0.1`) was used. A node advertising
    /// this to peers is unreachable from any other host.
    LocalhostFallback,
}

impl AddrSource {
    /// Stable lower-case token for the structured `source=` log field.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            AddrSource::Override => "override",
            AddrSource::EnvHint => "env-hint",
            AddrSource::HostnameProbe => "hostname-probe",
            AddrSource::LocalhostFallback => "localhost-fallback",
        }
    }
}

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

/// Look up an env-var-supplied address hint. Empty values are treated
/// the same as unset so the SLURM wrapper can always export the var
/// unconditionally and pass `""` when its host-IP probe yielded
/// nothing — keeping the wrapper free of conditional `-e` plumbing.
fn env_addr_hint(var: &str) -> Option<String> {
    std::env::var(var)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Detect the local IPv4 address AND the resolution rung it came from
/// (see [`AddrSource`]) so the caller can log both at startup.
///
/// Resolution order:
///   1. If `override_ip` is `Some`, use it verbatim ([`AddrSource::Override`]).
///      This is the explicit Python-side `local_ip` config knob (wired in
///      via PrimaryConfig / SecondaryConfig once those typed configs exist).
///   2. The `PRIMARY_NODE_IPV4` env-var hint, if set and non-empty
///      ([`AddrSource::EnvHint`]).
///   3. Otherwise, parse the first non-loopback IPv4 from `hostname -I`
///      ([`AddrSource::HostnameProbe`]).
///   4. Otherwise, fall back to "127.0.0.1" ([`AddrSource::LocalhostFallback`]).
///
/// The address is `String` (never `Option`): the localhost fallback
/// preserves single-host integration-test behaviour where the secondary's
/// peer dialer must still produce a candidate `SocketAddr` even on a host
/// that has no external IPv4 configured.
pub(crate) fn detect_ipv4_with_source(override_ip: Option<&str>) -> (String, AddrSource) {
    if let Some(ip) = override_ip {
        return (ip.to_string(), AddrSource::Override);
    }

    if let Some(hint) = env_addr_hint(ENV_HOST_IPV4) {
        return (hint, AddrSource::EnvHint);
    }

    if let Some(addr) =
        first_hostname_addr::<Ipv4Addr, _>(|addr| !addr.is_loopback() && !addr.is_unspecified())
    {
        return (addr.to_string(), AddrSource::HostnameProbe);
    }

    ("127.0.0.1".into(), AddrSource::LocalhostFallback)
}

/// Detect the local IPv6 address, if any externally-reachable one is
/// configured, AND the resolution rung it came from (see [`AddrSource`]).
///
/// Resolution order:
///   1. If `override_ip` is `Some`, use it verbatim ([`AddrSource::Override`]).
///   2. The `PRIMARY_NODE_IPV6` env-var hint, if set and non-empty
///      ([`AddrSource::EnvHint`]).
///   3. Otherwise, parse the first non-loopback / non-unspecified /
///      non-link-local IPv6 from `hostname -I` ([`AddrSource::HostnameProbe`]).
///   4. Otherwise, return `None`.
///
/// Returns `Option` rather than a localhost fallback: a real cluster host
/// without any IPv6 NIC should advertise no IPv6, so the peer dialer
/// doesn't waste an attempt racing `[::1]:port` (which always fails to
/// reach a different node). There is therefore no `LocalhostFallback`
/// source for v6. Single-host tests already get an IPv4 candidate from
/// [`detect_ipv4_with_source`]'s fallback, so they don't need an IPv6
/// loopback.
///
/// Link-local (`fe80::/10`) is excluded because it's only routable
/// when paired with a `%scope_id` interface qualifier — we don't carry
/// that on the wire and a peer on a different host can't dial a bare
/// link-local.
pub(crate) fn detect_ipv6_with_source(override_ip: Option<&str>) -> Option<(String, AddrSource)> {
    if let Some(ip) = override_ip {
        return Some((ip.to_string(), AddrSource::Override));
    }

    if let Some(hint) = env_addr_hint(ENV_HOST_IPV6) {
        return Some((hint, AddrSource::EnvHint));
    }

    first_hostname_addr::<Ipv6Addr, _>(|addr| {
        !addr.is_loopback() && !addr.is_unspecified() && !is_unicast_link_local(addr)
    })
    .map(|a| (a.to_string(), AddrSource::HostnameProbe))
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
    use std::sync::Mutex;

    /// Cargo's default test runner runs tests in parallel within one
    /// process; mutating `PRIMARY_NODE_IPV{4,6}` from multiple threads
    /// would race. Tests that touch those env vars take this lock.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    /// RAII helper: set an env var, restore the prior state on drop.
    /// Keeps each env-mutating test self-contained and independent of
    /// the order Cargo schedules them in.
    struct EnvVarGuard {
        key: &'static str,
        prev: Option<String>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let prev = std::env::var(key).ok();
            // SAFETY: tests holding ENV_LOCK serialize all access to
            // these vars; no other thread observes the mutation mid-flight.
            unsafe {
                std::env::set_var(key, value);
            }
            Self { key, prev }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            // SAFETY: see EnvVarGuard::set.
            unsafe {
                match &self.prev {
                    Some(v) => std::env::set_var(self.key, v),
                    None => std::env::remove_var(self.key),
                }
            }
        }
    }

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
        assert_eq!(detect_ipv4_with_source(Some("10.0.0.99")).0, "10.0.0.99");
        assert_eq!(
            detect_ipv6_with_source(Some("2001:db8::1")).map(|(a, _)| a),
            Some("2001:db8::1".into())
        );
    }

    #[test]
    fn env_hint_overrides_hostname_probe_ipv4() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _guard = EnvVarGuard::set(ENV_HOST_IPV4, "10.0.0.5");
        // No `override_ip` argument: env hint must beat `hostname -I`,
        // which is the whole point of the hint.
        assert_eq!(detect_ipv4_with_source(None).0, "10.0.0.5");
    }

    #[test]
    fn env_hint_overrides_hostname_probe_ipv6() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _guard = EnvVarGuard::set(ENV_HOST_IPV6, "2001:db8::42");
        assert_eq!(
            detect_ipv6_with_source(None).map(|(a, _)| a),
            Some("2001:db8::42".into())
        );
    }

    #[test]
    fn empty_env_hint_is_ignored() {
        let _lock = ENV_LOCK.lock().unwrap();
        // Wrapper unconditionally sets the var even when its host-IP
        // probe yielded nothing; an empty value must not poison the
        // fallback chain.
        let _guard4 = EnvVarGuard::set(ENV_HOST_IPV4, "");
        let _guard6 = EnvVarGuard::set(ENV_HOST_IPV6, "   ");
        // Can't assert the exact returned value (depends on the test
        // host's NICs) but it must not be the empty string.
        assert!(!detect_ipv4_with_source(None).0.is_empty());
        // IPv6 may legitimately be None on hosts without IPv6, but if
        // Some it must not be empty/whitespace.
        if let Some((v6, _)) = detect_ipv6_with_source(None) {
            assert!(!v6.trim().is_empty());
        }
    }

    #[test]
    fn explicit_override_beats_env_hint() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _guard = EnvVarGuard::set(ENV_HOST_IPV4, "10.0.0.5");
        // `override_ip` is the explicit Python-side knob; it must
        // out-rank any env hint so a user can force a value.
        assert_eq!(detect_ipv4_with_source(Some("192.168.1.1")).0, "192.168.1.1");
    }

    #[test]
    fn source_override_classified() {
        // Override short-circuits before any env / probe and is
        // reported as `Override`.
        let (addr, src) = detect_ipv4_with_source(Some("10.0.0.99"));
        assert_eq!(addr, "10.0.0.99");
        assert_eq!(src, AddrSource::Override);

        let (addr6, src6) = detect_ipv6_with_source(Some("2001:db8::1")).unwrap();
        assert_eq!(addr6, "2001:db8::1");
        assert_eq!(src6, AddrSource::Override);
    }

    #[test]
    fn source_env_hint_classified() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _g4 = EnvVarGuard::set(ENV_HOST_IPV4, "10.0.0.5");
        let _g6 = EnvVarGuard::set(ENV_HOST_IPV6, "2001:db8::42");
        // No override arg: the env hint wins and is reported as
        // `EnvHint` — the SLURM-wrapper-supplied routable address.
        let (addr, src) = detect_ipv4_with_source(None);
        assert_eq!(addr, "10.0.0.5");
        assert_eq!(src, AddrSource::EnvHint);

        let (addr6, src6) = detect_ipv6_with_source(None).unwrap();
        assert_eq!(addr6, "2001:db8::42");
        assert_eq!(src6, AddrSource::EnvHint);
    }

    #[test]
    fn source_falls_past_empty_env_hint() {
        let _lock = ENV_LOCK.lock().unwrap();
        // Wrapper sets the var to empty when its probe found nothing;
        // the empty hint must NOT be reported as `EnvHint` — resolution
        // falls through to the probe (or the localhost fallback for v4).
        let _g4 = EnvVarGuard::set(ENV_HOST_IPV4, "");
        let _g6 = EnvVarGuard::set(ENV_HOST_IPV6, "   ");
        let (_addr, src) = detect_ipv4_with_source(None);
        assert_ne!(
            src,
            AddrSource::EnvHint,
            "empty env hint must not be classified as an env-hint source"
        );
        assert!(matches!(
            src,
            AddrSource::HostnameProbe | AddrSource::LocalhostFallback
        ));
        // v6 either resolves via the probe or is None — never EnvHint.
        if let Some((_a6, src6)) = detect_ipv6_with_source(None) {
            assert_eq!(src6, AddrSource::HostnameProbe);
        }
    }

    #[test]
    fn source_token_strings_stable() {
        // The `source=` log field tokens are an operator-facing
        // contract; pin them.
        assert_eq!(AddrSource::Override.as_str(), "override");
        assert_eq!(AddrSource::EnvHint.as_str(), "env-hint");
        assert_eq!(AddrSource::HostnameProbe.as_str(), "hostname-probe");
        assert_eq!(
            AddrSource::LocalhostFallback.as_str(),
            "localhost-fallback"
        );
    }
}
