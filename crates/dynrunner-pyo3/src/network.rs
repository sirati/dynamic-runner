//! Local-host network helpers (hostname, IPv4/IPv6 detection).
//!
//! Both `detect_ipv4` and `detect_ipv6` share the same primitive: parsing
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
/// Consumed by [`detect_ipv4`] when no explicit `override_ip` argument
/// is passed.
const ENV_HOST_IPV4: &str = "PRIMARY_NODE_IPV4";

/// Env var the SLURM wrapper sets to the host's peer-routable IPv6.
/// Consumed by [`detect_ipv6`].
const ENV_HOST_IPV6: &str = "PRIMARY_NODE_IPV6";

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

/// Detect the local IPv4 address.
///
/// Resolution order:
///   1. If `override_ip` is `Some`, use it verbatim. This is the explicit
///      Python-side `local_ip` config knob (wired in via PrimaryConfig /
///      SecondaryConfig once those typed configs exist).
///   2. The `PRIMARY_NODE_IPV4` env-var hint, if set and non-empty.
///   3. Otherwise, parse the first non-loopback IPv4 from `hostname -I`.
///   4. Otherwise, fall back to "127.0.0.1".
///
/// Returns `String` (never `Option`): the localhost fallback preserves
/// single-host integration-test behaviour where the secondary's peer
/// dialer must still produce a candidate `SocketAddr` even on a host
/// that has no external IPv4 configured.
pub(crate) fn detect_ipv4(override_ip: Option<&str>) -> String {
    if let Some(ip) = override_ip {
        return ip.to_string();
    }

    env_addr_hint(ENV_HOST_IPV4)
        .or_else(|| {
            first_hostname_addr::<Ipv4Addr, _>(|addr| !addr.is_loopback() && !addr.is_unspecified())
                .map(|a| a.to_string())
        })
        .unwrap_or_else(|| "127.0.0.1".into())
}

/// Detect the local IPv6 address, if any externally-reachable one is
/// configured.
///
/// Resolution order:
///   1. If `override_ip` is `Some`, use it verbatim.
///   2. The `PRIMARY_NODE_IPV6` env-var hint, if set and non-empty.
///   3. Otherwise, parse the first non-loopback / non-unspecified /
///      non-link-local IPv6 from `hostname -I`.
///   4. Otherwise, return `None`.
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

    env_addr_hint(ENV_HOST_IPV6).or_else(|| {
        first_hostname_addr::<Ipv6Addr, _>(|addr| {
            !addr.is_loopback() && !addr.is_unspecified() && !is_unicast_link_local(addr)
        })
        .map(|a| a.to_string())
    })
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
            unsafe { std::env::set_var(key, value); }
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
        assert_eq!(detect_ipv4(Some("10.0.0.99")), "10.0.0.99");
        assert_eq!(detect_ipv6(Some("2001:db8::1")), Some("2001:db8::1".into()));
    }

    #[test]
    fn env_hint_overrides_hostname_probe_ipv4() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _guard = EnvVarGuard::set(ENV_HOST_IPV4, "10.0.0.5");
        // No `override_ip` argument: env hint must beat `hostname -I`,
        // which is the whole point of the hint.
        assert_eq!(detect_ipv4(None), "10.0.0.5");
    }

    #[test]
    fn env_hint_overrides_hostname_probe_ipv6() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _guard = EnvVarGuard::set(ENV_HOST_IPV6, "2001:db8::42");
        assert_eq!(detect_ipv6(None), Some("2001:db8::42".into()));
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
        assert!(!detect_ipv4(None).is_empty());
        // IPv6 may legitimately be None on hosts without IPv6, but if
        // Some it must not be empty/whitespace.
        if let Some(v6) = detect_ipv6(None) {
            assert!(!v6.trim().is_empty());
        }
    }

    #[test]
    fn explicit_override_beats_env_hint() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _guard = EnvVarGuard::set(ENV_HOST_IPV4, "10.0.0.5");
        // `override_ip` is the explicit Python-side knob; it must
        // out-rank any env hint so a user can force a value.
        assert_eq!(detect_ipv4(Some("192.168.1.1")), "192.168.1.1");
    }
}
