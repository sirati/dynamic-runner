//! Single concern: container memory-cap probe (generate.rs:540-569).
//! MIN(host MemTotal - 2GiB, cgroup memory.max). Faithful port of the
//! bash that computed `MEM_BYTES` / `MEM_SOURCE` / `MEM_FLAGS`.

use std::fs;

/// Log target shared with the rest of the wrapper binary.
const LOG_TARGET: &str = "slurm-wrapper";

/// Two GiB headroom subtracted from host `MemTotal`, mirroring the bash
/// `2*1024*1024*1024`.
const HEADROOM_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// Host RAM probe. Reads the `MemTotal:` line of a `/proc/meminfo`-shaped
/// string (value in kB), computes `kB*1024 - 2GiB`, and yields the result
/// only when it is strictly positive — mirroring the awk `if (val > 0)`
/// guard. Missing `MemTotal:` or an unparseable value yields `None`.
fn node_cap_from_meminfo(meminfo: &str) -> Option<u64> {
    let kb: u64 = meminfo.lines().find_map(|line| {
        let mut fields = line.split_whitespace();
        match fields.next() {
            Some("MemTotal:") => fields.next().and_then(|v| v.parse::<u64>().ok()),
            _ => None,
        }
    })?;
    // kB -> bytes, then subtract headroom. awk arithmetic is signed; a
    // result <= 0 maps to "" (None). Use checked subtraction so the
    // not-enough-RAM case collapses to None rather than wrapping.
    let bytes = kb.checked_mul(1024)?;
    match bytes.checked_sub(HEADROOM_BYTES) {
        Some(0) | None => None, // `val > 0` only
        Some(v) => Some(v),
    }
}

/// Cgroup probe. Mirrors the bash `case` guards over the trimmed contents
/// of `/sys/fs/cgroup/memory.max`: `""` or `"max"` => `None`; any value
/// containing a non-digit (`*[!0-9]*`) => `None`; otherwise parse u64.
fn cgroup_cap(memory_max: &str) -> Option<u64> {
    let trimmed = memory_max.trim();
    match trimmed {
        "" | "max" => None,
        s if s.bytes().all(|b| b.is_ascii_digit()) => s.parse::<u64>().ok(),
        _ => None,
    }
}

/// Combine the two probes the way the bash `if/elif` ladder did, returning
/// the chosen cap plus the `MEM_SOURCE` string. `None` cap => disabled.
fn choose(node: Option<u64>, cgroup: Option<u64>) -> (Option<u64>, String) {
    match (node, cgroup) {
        (Some(n), Some(c)) => {
            if n < c {
                (
                    Some(n),
                    format!("host MemTotal - 2GiB (tighter than cgroup {c})"),
                )
            } else {
                (
                    Some(c),
                    format!("wrapper cgroup memory.max (tighter than host-MemTotal-2GiB {n})"),
                )
            }
        }
        (Some(n), None) => (
            Some(n),
            "host MemTotal - 2GiB (no cgroup cap detected)".to_string(),
        ),
        (None, Some(c)) => (
            Some(c),
            "wrapper cgroup memory.max (host-MemTotal probe failed)".to_string(),
        ),
        (None, None) => (None, String::new()),
    }
}

/// RAM cap in bytes, or `None` when both probes are empty (=> caller
/// omits the `--memory` flag). The caller renders
/// `--memory=<n> --memory-swap=-1` when `Some`.
pub fn detect_memory_cap() -> Option<u64> {
    // Tolerate read errors as empty, mirroring the bash `2>/dev/null ||
    // echo ""` and the fact that /proc/meminfo is absent off-Linux.
    let meminfo = fs::read_to_string("/proc/meminfo").unwrap_or_default();
    let memory_max = fs::read_to_string("/sys/fs/cgroup/memory.max").unwrap_or_default();

    let node = node_cap_from_meminfo(&meminfo);
    let cgroup = cgroup_cap(&memory_max);
    let (cap, source) = choose(node, cgroup);

    match cap {
        Some(bytes) => {
            tracing::info!(
                target: LOG_TARGET,
                "Container memory cap: {bytes} bytes RAM + unlimited swap ({source})"
            );
        }
        None => {
            tracing::info!(
                target: LOG_TARGET,
                "Container memory cap: disabled (host-MemTotal and cgroup probes both empty)"
            );
        }
    }
    cap
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_cap_realistic() {
        // 98304000 kB == 96 GiB. *1024 = 100_663_296_000 bytes, minus 2 GiB.
        let meminfo = "MemFree:       12345 kB\nMemTotal:   98304000 kB\nBuffers:        678 kB\n";
        let expected = 98_304_000u64 * 1024 - HEADROOM_BYTES;
        assert_eq!(node_cap_from_meminfo(meminfo), Some(expected));
        assert_eq!(expected, 98_515_812_352);
    }

    #[test]
    fn node_cap_tiny_ram_goes_negative() {
        // 1 MiB total: 1024 kB -> bytes far below the 2 GiB headroom.
        let meminfo = "MemTotal:       1024 kB\n";
        assert_eq!(node_cap_from_meminfo(meminfo), None);
    }

    #[test]
    fn node_cap_exactly_headroom_is_none() {
        // val == 0 must map to None (`val > 0` is strict).
        let kb = HEADROOM_BYTES / 1024;
        let meminfo = format!("MemTotal: {kb} kB\n");
        assert_eq!(node_cap_from_meminfo(&meminfo), None);
    }

    #[test]
    fn node_cap_missing_memtotal() {
        let meminfo = "MemFree:       12345 kB\nBuffers:        678 kB\n";
        assert_eq!(node_cap_from_meminfo(meminfo), None);
    }

    #[test]
    fn cgroup_cap_max() {
        assert_eq!(cgroup_cap("max\n"), None);
    }

    #[test]
    fn cgroup_cap_numeric() {
        assert_eq!(cgroup_cap("4294967296\n"), Some(4_294_967_296));
    }

    #[test]
    fn cgroup_cap_garbage() {
        assert_eq!(cgroup_cap("garbage"), None);
    }

    #[test]
    fn cgroup_cap_empty() {
        assert_eq!(cgroup_cap(""), None);
    }

    #[test]
    fn choose_both_node_tighter() {
        let (cap, source) = choose(Some(100), Some(200));
        assert_eq!(cap, Some(100));
        assert_eq!(source, "host MemTotal - 2GiB (tighter than cgroup 200)");
    }

    #[test]
    fn choose_both_cgroup_tighter() {
        // Reversed ordering: cgroup smaller wins, with its source label.
        let (cap, source) = choose(Some(300), Some(150));
        assert_eq!(cap, Some(150));
        assert_eq!(
            source,
            "wrapper cgroup memory.max (tighter than host-MemTotal-2GiB 300)"
        );
    }

    #[test]
    fn choose_only_node() {
        let (cap, source) = choose(Some(42), None);
        assert_eq!(cap, Some(42));
        assert_eq!(source, "host MemTotal - 2GiB (no cgroup cap detected)");
    }

    #[test]
    fn choose_only_cgroup() {
        let (cap, source) = choose(None, Some(42));
        assert_eq!(cap, Some(42));
        assert_eq!(
            source,
            "wrapper cgroup memory.max (host-MemTotal probe failed)"
        );
    }

    #[test]
    fn choose_neither() {
        let (cap, source) = choose(None, None);
        assert_eq!(cap, None);
        assert!(source.is_empty());
    }
}
