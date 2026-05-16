//! Host / cgroup memory probe trait + production `/proc` + `/sys/fs/cgroup`
//! implementation.
//!
//! Single concern: read raw memory values from the running kernel and
//! return them as a plain numeric record. The watcher consumes this
//! record; it never opens `/proc` or `/sys` itself.
//!
//! Tests inject a mock probe (see `mod tests` in `super`) so unit tests
//! don't depend on the host's cgroup-v2 layout.

/// Host RAM / swap / cgroup-v2 memory readout.
///
/// Each field is `None` when the kernel surface that would populate it
/// is unavailable (cgroup-v1 host, non-Linux, missing file, parse
/// failure). The watcher converts `None` to `0` in the structured log
/// line so downstream tooling can grep numeric fields uniformly.
#[derive(Debug, Clone, Copy, Default)]
pub struct HostMemoryReading {
    /// `MemTotal - MemAvailable` from `/proc/meminfo`.
    pub host_ram_used_bytes: Option<u64>,
    /// `MemTotal` from `/proc/meminfo`.
    pub host_ram_total_bytes: Option<u64>,
    /// `SwapTotal - SwapFree` from `/proc/meminfo`.
    pub host_swap_used_bytes: Option<u64>,
    /// `SwapTotal` from `/proc/meminfo`.
    pub host_swap_total_bytes: Option<u64>,
    /// `/sys/fs/cgroup/memory.current` (cgroup v2).
    pub container_memory_current: Option<u64>,
    /// `/sys/fs/cgroup/memory.max` (cgroup v2). The literal string
    /// "max" decodes to `None` (no limit configured).
    pub container_memory_max: Option<u64>,
    /// `/sys/fs/cgroup/memory.swap.current` (cgroup v2).
    pub container_swap_current: Option<u64>,
    /// `/sys/fs/cgroup/memory.swap.max` (cgroup v2). "max" → `None`.
    pub container_swap_max: Option<u64>,
}

/// Trait the OOM watcher uses to read host + cgroup memory state.
///
/// One concern: hide the filesystem behind a trait so unit tests can
/// inject a deterministic mock. Production wires
/// [`ProcSysProbe`] (default) which reads the real `/proc/meminfo` and
/// `/sys/fs/cgroup/memory.*` paths.
pub trait SystemProbe: Send {
    /// Read a fresh snapshot. Cheap (a handful of small file reads on
    /// Linux); called at sample cadence (20Hz by default).
    fn read(&self) -> HostMemoryReading;
}

/// Production probe: reads `/proc/meminfo` + cgroup-v2 files under
/// `/sys/fs/cgroup/`. Each path is independent — a missing cgroup
/// file leaves only that field `None`; the host-RAM fields keep
/// working.
///
/// One-time warning emission on `new()` if cgroup-v2 paths are
/// unavailable; the per-tick reads stay silent so a non-cgroup host
/// doesn't spam the log.
pub struct ProcSysProbe {
    cgroup_memory_current: Option<&'static str>,
    cgroup_memory_max: Option<&'static str>,
    cgroup_swap_current: Option<&'static str>,
    cgroup_swap_max: Option<&'static str>,
}

impl ProcSysProbe {
    /// Build the production probe. Emits one tracing::warn at startup
    /// when the cgroup-v2 memory.current file is missing (non-Linux,
    /// cgroup-v1, or unmounted hierarchy) so the operator sees the
    /// degraded mode once rather than per-tick.
    pub fn new() -> Self {
        let cgroup_memory_current = if std::path::Path::new("/sys/fs/cgroup/memory.current").exists() {
            Some("/sys/fs/cgroup/memory.current")
        } else {
            None
        };
        let cgroup_memory_max = if std::path::Path::new("/sys/fs/cgroup/memory.max").exists() {
            Some("/sys/fs/cgroup/memory.max")
        } else {
            None
        };
        let cgroup_swap_current = if std::path::Path::new("/sys/fs/cgroup/memory.swap.current").exists() {
            Some("/sys/fs/cgroup/memory.swap.current")
        } else {
            None
        };
        let cgroup_swap_max = if std::path::Path::new("/sys/fs/cgroup/memory.swap.max").exists() {
            Some("/sys/fs/cgroup/memory.swap.max")
        } else {
            None
        };

        if cgroup_memory_current.is_none() {
            tracing::warn!(
                target: "oom_watcher",
                "cgroup-v2 memory.current not found at /sys/fs/cgroup/memory.current — \
                 container memory fields will report null. Likely cause: cgroup-v1 host, \
                 non-Linux, or non-default cgroup hierarchy mount."
            );
        }

        Self {
            cgroup_memory_current,
            cgroup_memory_max,
            cgroup_swap_current,
            cgroup_swap_max,
        }
    }
}

impl Default for ProcSysProbe {
    fn default() -> Self {
        Self::new()
    }
}

impl SystemProbe for ProcSysProbe {
    fn read(&self) -> HostMemoryReading {
        let (mem_total, mem_available, swap_total, swap_free) = read_meminfo();
        let host_ram_total_bytes = mem_total;
        let host_ram_used_bytes = match (mem_total, mem_available) {
            (Some(t), Some(a)) => Some(t.saturating_sub(a)),
            _ => None,
        };
        let host_swap_total_bytes = swap_total;
        let host_swap_used_bytes = match (swap_total, swap_free) {
            (Some(t), Some(f)) => Some(t.saturating_sub(f)),
            _ => None,
        };

        HostMemoryReading {
            host_ram_used_bytes,
            host_ram_total_bytes,
            host_swap_used_bytes,
            host_swap_total_bytes,
            container_memory_current: self.cgroup_memory_current.and_then(read_cgroup_u64),
            container_memory_max: self.cgroup_memory_max.and_then(read_cgroup_max),
            container_swap_current: self.cgroup_swap_current.and_then(read_cgroup_u64),
            container_swap_max: self.cgroup_swap_max.and_then(read_cgroup_max),
        }
    }
}

/// Read `/proc/meminfo` once and return `(MemTotal, MemAvailable,
/// SwapTotal, SwapFree)` in bytes. Each field is `None` on parse
/// failure / non-Linux.
fn read_meminfo() -> (Option<u64>, Option<u64>, Option<u64>, Option<u64>) {
    #[cfg(target_os = "linux")]
    {
        let Ok(contents) = std::fs::read_to_string("/proc/meminfo") else {
            return (None, None, None, None);
        };
        let mut mem_total = None;
        let mut mem_available = None;
        let mut swap_total = None;
        let mut swap_free = None;
        for line in contents.lines() {
            if let Some(v) = parse_meminfo_line(line, "MemTotal:") {
                mem_total = Some(v);
            } else if let Some(v) = parse_meminfo_line(line, "MemAvailable:") {
                mem_available = Some(v);
            } else if let Some(v) = parse_meminfo_line(line, "SwapTotal:") {
                swap_total = Some(v);
            } else if let Some(v) = parse_meminfo_line(line, "SwapFree:") {
                swap_free = Some(v);
            }
        }
        (mem_total, mem_available, swap_total, swap_free)
    }
    #[cfg(not(target_os = "linux"))]
    {
        (None, None, None, None)
    }
}

/// Parse a single `/proc/meminfo` line of the form `"<key>: <N> kB"`
/// into bytes. Returns `None` when the prefix doesn't match or the
/// numeric body fails to parse.
fn parse_meminfo_line(line: &str, prefix: &str) -> Option<u64> {
    let rest = line.strip_prefix(prefix)?.trim();
    let kb_str = rest.strip_suffix("kB").or_else(|| rest.strip_suffix(" kB"))?;
    let kb: u64 = kb_str.trim().parse().ok()?;
    Some(kb * 1024)
}

/// Read a cgroup-v2 numeric file (e.g. `memory.current`). Returns
/// `None` on missing file, IO error, or parse failure.
fn read_cgroup_u64(path: &str) -> Option<u64> {
    let s = std::fs::read_to_string(path).ok()?;
    s.trim().parse::<u64>().ok()
}

/// Read a cgroup-v2 limit file (e.g. `memory.max`). Returns `None`
/// on missing file, the literal value `"max"` (no limit set), or
/// parse failure.
fn read_cgroup_max(path: &str) -> Option<u64> {
    let s = std::fs::read_to_string(path).ok()?;
    let trimmed = s.trim();
    if trimmed == "max" {
        return None;
    }
    trimmed.parse::<u64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_meminfo_line_extracts_kb_to_bytes() {
        let line = "MemTotal:       16384 kB";
        assert_eq!(parse_meminfo_line(line, "MemTotal:"), Some(16384 * 1024));
    }

    #[test]
    fn parse_meminfo_line_handles_no_space_before_unit() {
        // Some kernels emit `"MemTotal:       16384kB"`; the parser
        // must accept either form so the production probe doesn't
        // silently report 0 on those hosts.
        let line = "MemTotal:       16384kB";
        assert_eq!(parse_meminfo_line(line, "MemTotal:"), Some(16384 * 1024));
    }

    #[test]
    fn parse_meminfo_line_rejects_other_keys() {
        let line = "SwapTotal:      0 kB";
        assert_eq!(parse_meminfo_line(line, "MemTotal:"), None);
    }
}
