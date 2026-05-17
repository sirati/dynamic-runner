//! Host / cgroup memory probe trait + production `/proc` + `/sys/fs/cgroup`
//! implementation.
//!
//! Single concern: read raw memory values from the running kernel and
//! return them as a plain numeric record. The watcher consumes this
//! record; it never opens `/proc` or `/sys` itself.
//!
//! Tests inject a mock probe (see `mod tests` in `super`) so unit tests
//! don't depend on the host's cgroup-v2 layout.
//!
//! Kernel-OOM detection: when a `<workers>/memory.events` path is
//! supplied (via the nested workers cgroup the manager owns; see
//! [`crate::cgroup::NestedCgroupHandle`]), the probe parses the
//! `oom_kill <count>` line on each [`SystemProbe::read`] and surfaces
//! the cumulative counter in `kernel_oom_kill_count`. The watcher
//! converts the counter to a delta across samples; downstream
//! reclassifies a worker disconnect from `Recoverable` to
//! `ResourceExhausted(memory)` when an oom_kill landed in the same
//! sample window.

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
    /// Cumulative `oom_kill` count from the workers-subgroup
    /// `memory.events` file (cgroup v2). `None` when the probe was
    /// constructed without a workers-events path (no nested cgroup,
    /// graceful-fallback flat layout) or the read failed. The watcher
    /// computes a delta across samples and routes a worker disconnect
    /// reclassification when the delta is positive in the same window.
    pub kernel_oom_kill_count: Option<u64>,
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
    /// Absolute path to the workers cgroup `memory.events` file
    /// (`<workers>/memory.events`), if the manager materialised a
    /// nested workers subgroup. `None` for the flat-layout fallback;
    /// `kernel_oom_kill_count` then stays `None` on every read.
    workers_memory_events: Option<std::path::PathBuf>,
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
            workers_memory_events: None,
        }
    }

    /// Attach the workers cgroup `memory.events` path so subsequent
    /// reads populate `kernel_oom_kill_count`. Caller passes
    /// `<workers>/memory.events`; the probe stores the absolute path
    /// verbatim and reads it on each `read()` call. Pass `None` (or
    /// skip this call) to keep the kernel-oom field unpopulated —
    /// the watcher then never sees a positive delta and never
    /// reclassifies a disconnect.
    pub fn with_workers_memory_events(mut self, path: Option<std::path::PathBuf>) -> Self {
        self.workers_memory_events = path;
        self
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
            kernel_oom_kill_count: self
                .workers_memory_events
                .as_deref()
                .and_then(read_memory_events_oom_kill),
        }
    }
}

/// Read `<cgroup>/memory.events` and extract the cumulative `oom_kill`
/// counter. The file is a `<key> <value>` newline-delimited table
/// (`low <n>`, `high <n>`, `max <n>`, `oom <n>`, `oom_kill <n>`,
/// `oom_group_kill <n>`). Returns `None` on missing file, IO error,
/// or absent `oom_kill` line.
pub(crate) fn read_memory_events_oom_kill(path: &std::path::Path) -> Option<u64> {
    let contents = std::fs::read_to_string(path).ok()?;
    parse_memory_events_oom_kill(&contents)
}

/// Parse the `oom_kill <count>` line out of a `memory.events`-shaped
/// blob. Split into its own function so the unit test can exercise
/// the parser without touching the filesystem.
pub(crate) fn parse_memory_events_oom_kill(contents: &str) -> Option<u64> {
    for line in contents.lines() {
        let mut parts = line.split_ascii_whitespace();
        let Some(key) = parts.next() else { continue };
        if key != "oom_kill" {
            continue;
        }
        return parts.next().and_then(|v| v.parse::<u64>().ok());
    }
    None
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

    #[test]
    fn parse_memory_events_oom_kill_extracts_count() {
        // Real cgroup-v2 memory.events shape (Linux ≥ 5.2).
        let contents = "\
            low 0\n\
            high 0\n\
            max 0\n\
            oom 3\n\
            oom_kill 2\n\
            oom_group_kill 0\n";
        assert_eq!(parse_memory_events_oom_kill(contents), Some(2));
    }

    #[test]
    fn parse_memory_events_oom_kill_returns_none_when_absent() {
        // Older kernels (cgroup-v2 ≤ 5.1) omit oom_kill; the watcher
        // must accept that as "field unavailable" rather than 0.
        let contents = "low 0\nhigh 0\nmax 0\noom 0\n";
        assert_eq!(parse_memory_events_oom_kill(contents), None);
    }

    #[test]
    fn parse_memory_events_oom_kill_handles_trailing_blank_lines() {
        // Defensive: tolerant of empty lines in the file (some
        // pseudo-fs implementations emit them).
        let contents = "\n\noom_kill 5\n\n";
        assert_eq!(parse_memory_events_oom_kill(contents), Some(5));
    }
}
