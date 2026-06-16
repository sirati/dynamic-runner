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
    /// Cumulative CPU tick counts from the aggregate `cpu` line of
    /// `/proc/stat`. `None` on parse failure / non-Linux. Raw cumulative
    /// counters — the watcher diffs across sweeps to produce the busy
    /// fraction (`cpu_busy_milli` on the watcher snapshot). The aggregate
    /// "cpu" line sums across cores, so a 100% busy fraction reads as
    /// "every core at 100%" — the natural normalisation the #575
    /// observer aggregation wants.
    pub cpu_stat: Option<CpuStat>,
    /// Cumulative CPU ticks consumed by THIS process (utime + stime,
    /// fields 14 + 15 of `/proc/self/stat`, in USER_HZ — the same
    /// clock-tick unit the `/proc/stat` aggregate counts). Process-wide:
    /// the kernel sums every thread, so the in-process peer-mesh tasks
    /// are already included. `None` on parse failure / non-Linux. The
    /// watcher diffs this across sweeps over the SAME interval as
    /// `cpu_stat` and subtracts the framework's own share from the host
    /// busy fraction, so the reported host CPU reflects the WORKLOAD
    /// (worker subprocesses are CHILDREN — NOT counted here — so they
    /// stay in the reported figure).
    pub self_cpu_ticks: Option<u64>,
}

/// Cumulative tick counts from the aggregate `cpu` line of
/// `/proc/stat` — the substrate the OOM watcher's CPU-utilisation
/// derivation (#575) consumes. `total` is the sum of EVERY tick column
/// (user + nice + system + idle + iowait + irq + softirq + steal +
/// guest + guest_nice); `idle` is the sum of the idle-shaped columns
/// (`idle` + `iowait`). The watcher diffs across sweeps:
/// `busy_milli = (Δtotal - Δidle) / Δtotal * 100_000`.
///
/// `Copy` so the probe's `HostMemoryReading` keeps its zero-allocation,
/// flat-record shape across `apply_sweep`'s host reading.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CpuStat {
    /// Sum of EVERY tick column on the aggregate `cpu` line.
    pub total: u64,
    /// Sum of `idle + iowait` ticks (the kernel's idle bucket).
    pub idle: u64,
}

/// Trait the OOM watcher uses to read host + cgroup memory state.
///
/// One concern: hide the filesystem behind a trait so unit tests can
/// inject a deterministic mock. Production wires
/// [`ProcSysProbe`] (default) which reads the real `/proc/meminfo` and
/// `/sys/fs/cgroup/memory.*` paths.
///
/// `Send + Sync` so the watcher can hold the probe behind an [`Arc`]
/// and clone that handle into the `spawn_blocking` closure that runs
/// the per-sweep reads off the async runtime.
pub trait SystemProbe: Send + Sync {
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
        let cgroup_memory_current =
            if std::path::Path::new("/sys/fs/cgroup/memory.current").exists() {
                Some("/sys/fs/cgroup/memory.current")
            } else {
                None
            };
        let cgroup_memory_max = if std::path::Path::new("/sys/fs/cgroup/memory.max").exists() {
            Some("/sys/fs/cgroup/memory.max")
        } else {
            None
        };
        let cgroup_swap_current =
            if std::path::Path::new("/sys/fs/cgroup/memory.swap.current").exists() {
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
            cpu_stat: read_proc_stat_cpu(),
            self_cpu_ticks: read_proc_self_cpu(),
        }
    }
}

/// Read the aggregate `cpu` line of `/proc/stat` once and return its
/// `(total_ticks, idle_ticks)` decomposition as a [`CpuStat`]. `None`
/// on missing file, IO error, or unparseable header.
///
/// The aggregate line shape is `cpu <user> <nice> <system> <idle>
/// <iowait> <irq> <softirq> <steal> <guest> <guest_nice>` (Linux ≥ 2.6.33;
/// earlier kernels omit `steal`/`guest*`). The parser accepts any
/// length ≥ 4 and sums ALL present columns into `total`, plus the
/// `idle + iowait` pair into `idle` — extra columns on newer kernels
/// fold into `total` naturally.
pub(crate) fn read_proc_stat_cpu() -> Option<CpuStat> {
    #[cfg(target_os = "linux")]
    {
        let contents = std::fs::read_to_string("/proc/stat").ok()?;
        parse_proc_stat_cpu(&contents)
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

/// Parse the aggregate `cpu` line out of a `/proc/stat`-shaped blob.
/// Split into its own function so the unit test exercises the parser
/// without touching the filesystem.
pub(crate) fn parse_proc_stat_cpu(contents: &str) -> Option<CpuStat> {
    // The aggregate line is ALWAYS the first line of `/proc/stat`, and
    // it begins with `cpu ` (a space — the per-core lines begin with
    // `cpu0`, `cpu1`, … so the prefix check disambiguates without a
    // regex).
    let line = contents.lines().next()?;
    let rest = line.strip_prefix("cpu ")?.trim_start();
    let mut cols = rest.split_ascii_whitespace();
    let user: u64 = cols.next()?.parse().ok()?;
    let nice: u64 = cols.next()?.parse().ok()?;
    let system: u64 = cols.next()?.parse().ok()?;
    let idle: u64 = cols.next()?.parse().ok()?;
    let iowait: u64 = cols.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    // Sum the remaining columns (irq, softirq, steal, guest, guest_nice
    // on newer kernels) into total without naming each — a future
    // kernel column extension folds in automatically.
    let mut total = user
        .saturating_add(nice)
        .saturating_add(system)
        .saturating_add(idle)
        .saturating_add(iowait);
    for col in cols {
        if let Ok(v) = col.parse::<u64>() {
            total = total.saturating_add(v);
        }
    }
    Some(CpuStat {
        total,
        idle: idle.saturating_add(iowait),
    })
}

/// Derive the busy-fraction in milli-percent from two cumulative
/// readings (`prev` → `cur`). 100_000 = every core at 100% on the
/// aggregate `/proc/stat` line. Returns `None` when the delta is
/// non-positive (clock skew across a process restart, or two reads
/// landing inside the same tick) — the caller leaves the field
/// `None` rather than reporting zero.
pub(crate) fn cpu_busy_milli(prev: CpuStat, cur: CpuStat) -> Option<u32> {
    let dtotal = cur.total.saturating_sub(prev.total);
    let didle = cur.idle.saturating_sub(prev.idle);
    if dtotal == 0 {
        return None;
    }
    let busy = dtotal.saturating_sub(didle);
    // (busy / dtotal) * 100_000, computed in u128 to avoid the
    // intermediate `busy * 100_000` overflowing u64 on a long-running
    // host (a year of ticks per core × 100_000 still fits in u128 by
    // many orders of magnitude).
    let milli = (busy as u128).saturating_mul(100_000) / dtotal as u128;
    Some(milli.min(100_000) as u32)
}

/// Host busy fraction (milli-percent) with THIS process's own CPU
/// share subtracted — the #575 "host CPU of the WORKLOAD, not the
/// framework" figure.
///
/// Both fractions are taken over the SAME denominator `Δtotal` (the
/// sum-across-all-cpus aggregate `/proc/stat` delta), so they are
/// directly comparable and subtractable in the same unit:
/// `own_milli = Δself_ticks / Δtotal * 100_000`. `/proc/self`
/// utime+stime are in USER_HZ ticks, the `/proc/stat` aggregate total
/// is the same USER_HZ ticks summed across cpus, so the ratio matches
/// the host busy fraction's normalisation (100_000 = every core at
/// 100%).
///
/// `prev_self` / `cur_self` are `None` when `/proc/self/stat` was
/// unreadable on either sweep (or the first sweep has no prior) — the
/// fallback is to NOT subtract, reporting the host fraction as-is
/// rather than erroring. The net is clamped at 0 (the own share can
/// never legitimately exceed the host total, but a tick-boundary race
/// could momentarily; clamp keeps the field non-negative).
pub(crate) fn cpu_busy_milli_excl_self(
    prev: CpuStat,
    cur: CpuStat,
    prev_self: Option<u64>,
    cur_self: Option<u64>,
) -> Option<u32> {
    let host = cpu_busy_milli(prev, cur)?;
    let (Some(prev_self), Some(cur_self)) = (prev_self, cur_self) else {
        // No self readout on one of the two sweeps → report host as-is.
        return Some(host);
    };
    let dtotal = cur.total.saturating_sub(prev.total);
    if dtotal == 0 {
        // `cpu_busy_milli` already returned `None` for a zero host
        // delta, so this is unreachable; guard anyway against a /0.
        return Some(host);
    }
    let dself = cur_self.saturating_sub(prev_self);
    let own_milli = ((dself as u128).saturating_mul(100_000) / dtotal as u128).min(100_000) as u32;
    Some(host.saturating_sub(own_milli))
}

/// Read THIS process's cumulative CPU ticks (utime + stime) from
/// `/proc/self/stat`. `None` on missing file, IO error, or unparseable
/// shape / non-Linux.
pub(crate) fn read_proc_self_cpu() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let contents = std::fs::read_to_string("/proc/self/stat").ok()?;
        parse_proc_self_cpu(&contents)
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

/// Parse utime + stime (fields 14 + 15, 1-based) out of a
/// `/proc/self/stat`-shaped line and return their sum in clock ticks.
///
/// Field 2 (`comm`) is wrapped in parentheses and MAY contain spaces
/// and even close-parens, so a naive whitespace split mis-indexes every
/// later field. The robust parse: split on the LAST `)` — everything
/// after it is the space-separated tail starting at field 3 (`state`).
/// utime is the 12th token of that tail (field 14), stime the 13th
/// (field 15). Returns `None` if the line is malformed.
pub(crate) fn parse_proc_self_cpu(contents: &str) -> Option<u64> {
    let line = contents.lines().next()?;
    // Everything after the final ')' is field 3 onward, space-separated.
    let tail = &line[line.rfind(')')? + 1..];
    let mut cols = tail.split_ascii_whitespace();
    // Tail token 1 = field 3 (state). utime = field 14 = tail token 12;
    // stime = field 15 = tail token 13.
    let utime: u64 = cols.nth(11)?.parse().ok()?;
    let stime: u64 = cols.next()?.parse().ok()?;
    Some(utime.saturating_add(stime))
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

/// Parse a single `"<key>: <N> kB"` line into bytes — the shape both
/// `/proc/meminfo` and `/proc/<pid>/status` (`VmSwap:` et al.) use.
/// Returns `None` when the prefix doesn't match or the numeric body
/// fails to parse. `pub(crate)` so [`crate::monitor`]'s per-process
/// swap fallback reuses the same parser instead of growing a copy.
pub(crate) fn parse_meminfo_line(line: &str, prefix: &str) -> Option<u64> {
    let rest = line.strip_prefix(prefix)?.trim();
    let kb_str = rest
        .strip_suffix("kB")
        .or_else(|| rest.strip_suffix(" kB"))?;
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

    #[test]
    fn parse_proc_stat_cpu_extracts_total_and_idle() {
        // Real `/proc/stat` shape on Linux ≥ 2.6.33: cpu <user> <nice>
        // <system> <idle> <iowait> <irq> <softirq> <steal> <guest>
        // <guest_nice>, followed by per-core lines.
        let contents = "\
            cpu  100 5 30 800 10 1 2 0 0 0\n\
            cpu0 50 2 15 400 5 0 1 0 0 0\n\
            intr 1234\n";
        let stat = parse_proc_stat_cpu(contents).expect("aggregate line parses");
        // 100 + 5 + 30 + 800 + 10 + 1 + 2 + 0 + 0 + 0 = 948
        assert_eq!(stat.total, 948);
        // idle + iowait = 800 + 10 = 810
        assert_eq!(stat.idle, 810);
    }

    #[test]
    fn parse_proc_stat_cpu_tolerates_old_kernel_short_line() {
        // Pre-2.6.11 kernels (and some embedded fork) omit `iowait` and
        // the later columns. The parser must still produce a sensible
        // pair from the first four columns.
        let contents = "cpu  100 5 30 800\n";
        let stat = parse_proc_stat_cpu(contents).expect("aggregate line parses");
        assert_eq!(stat.total, 935);
        // No iowait column → idle is just `idle`.
        assert_eq!(stat.idle, 800);
    }

    #[test]
    fn parse_proc_stat_cpu_rejects_missing_aggregate_line() {
        let contents = "intr 1\nctxt 2\n";
        assert!(parse_proc_stat_cpu(contents).is_none());
    }

    #[test]
    fn cpu_busy_milli_computes_busy_fraction() {
        // 1 second on a 1-core box at 100 Hz: 100 ticks total, all
        // busy → 100_000 milli-percent.
        let prev = CpuStat { total: 0, idle: 0 };
        let cur = CpuStat {
            total: 100,
            idle: 0,
        };
        assert_eq!(cpu_busy_milli(prev, cur), Some(100_000));
    }

    #[test]
    fn cpu_busy_milli_half_busy() {
        // 50/100 busy → 50_000 milli-percent (= 50%).
        let prev = CpuStat { total: 0, idle: 0 };
        let cur = CpuStat {
            total: 100,
            idle: 50,
        };
        assert_eq!(cpu_busy_milli(prev, cur), Some(50_000));
    }

    #[test]
    fn cpu_busy_milli_returns_none_on_zero_delta() {
        // Same reading twice — no time has passed. Reporting zero
        // would lie ("CPU is 0% busy"); the convention is `None` so
        // the caller leaves the aggregation field unpopulated.
        let cur = CpuStat {
            total: 100,
            idle: 50,
        };
        assert_eq!(cpu_busy_milli(cur, cur), None);
    }

    #[test]
    fn cpu_busy_milli_clamps_at_100_000_on_idle_overflow() {
        // Defensive: clock-skew across a process restart (or a CRDT
        // restore of a stale prev) could leave `prev.idle > cur.idle`.
        // The saturating diff makes the busy share 100% of the delta;
        // the cap keeps the field inside [0, 100_000].
        let prev = CpuStat {
            total: 0,
            idle: 1000,
        };
        let cur = CpuStat {
            total: 100,
            idle: 0,
        };
        assert_eq!(cpu_busy_milli(prev, cur), Some(100_000));
    }

    #[test]
    fn parse_proc_self_cpu_sums_utime_and_stime() {
        // Real `/proc/self/stat` shape: pid (comm) state ppid ... where
        // utime is field 14 and stime field 15. comm here is "(cat)".
        // Fields after ')': 3=state .. 14=utime .. 15=stime.
        // 1   2     3 4 5 6 7 8  9  10 11 12 13 14 15 ...
        let contents =
            "1234 (cat) R 1 1234 1234 0 -1 4194304 100 0 0 0 42 17 0 0 20 0 1 0 999 0 0\n";
        // utime = 42, stime = 17 → sum 59.
        assert_eq!(parse_proc_self_cpu(contents), Some(59));
    }

    #[test]
    fn parse_proc_self_cpu_handles_comm_with_spaces_and_parens() {
        // comm can contain spaces and even close-parens — splitting on
        // the LAST ')' is what keeps the field index correct.
        let contents =
            "77 (weird ) name) S 1 77 77 0 -1 0 5 0 0 0 8 3 0 0 20 0 1 0 0 0 0\n";
        // After the final ')': "S 1 77 77 0 -1 0 5 0 0 0 8 3 ..."
        // token1=S(field3) ... token12=8(field14=utime) token13=3(stime)
        assert_eq!(parse_proc_self_cpu(contents), Some(11));
    }

    #[test]
    fn parse_proc_self_cpu_rejects_malformed() {
        assert!(parse_proc_self_cpu("no parens here").is_none());
        assert!(parse_proc_self_cpu("1 (c) R 1 2 3").is_none()); // too few fields
    }

    #[test]
    fn cpu_busy_milli_excl_self_subtracts_own_share() {
        // Host: 100 ticks total, all busy → host 100_000 milli.
        // Self consumed 30 of those 100 ticks → own = 30_000 milli.
        // Reported = 100_000 - 30_000 = 70_000.
        let prev = CpuStat { total: 0, idle: 0 };
        let cur = CpuStat { total: 100, idle: 0 };
        assert_eq!(
            cpu_busy_milli_excl_self(prev, cur, Some(0), Some(30)),
            Some(70_000)
        );
    }

    #[test]
    fn cpu_busy_milli_excl_self_matches_denominator_unit() {
        // 4-core box, 1s at 100 Hz → 400 total ticks. Host 50% busy
        // across all cores: busy 200 → host 50_000 milli. Self used 40
        // ticks over the SAME 400-tick denominator → own = 10_000.
        // Net = 40_000. Confirms own and host share the summed-across-
        // cpus Δtotal denominator.
        let prev = CpuStat { total: 0, idle: 0 };
        let cur = CpuStat {
            total: 400,
            idle: 200,
        };
        assert_eq!(
            cpu_busy_milli_excl_self(prev, cur, Some(100), Some(140)),
            Some(40_000)
        );
    }

    #[test]
    fn cpu_busy_milli_excl_self_clamps_at_zero() {
        // Pathological tick-boundary race: own delta exceeds host busy
        // delta. The net must clamp at 0, never go negative.
        let prev = CpuStat { total: 0, idle: 0 };
        let cur = CpuStat { total: 100, idle: 60 }; // host busy 40 → 40_000
        // self consumed 80 ticks → own 80_000 > 40_000.
        assert_eq!(
            cpu_busy_milli_excl_self(prev, cur, Some(0), Some(80)),
            Some(0)
        );
    }

    #[test]
    fn cpu_busy_milli_excl_self_falls_back_to_host_when_self_unreadable() {
        // /proc/self unreadable on one of the two sweeps → report the
        // host fraction as-is (do not subtract, do not error).
        let prev = CpuStat { total: 0, idle: 0 };
        let cur = CpuStat { total: 100, idle: 0 }; // host 100_000
        assert_eq!(
            cpu_busy_milli_excl_self(prev, cur, None, Some(30)),
            Some(100_000)
        );
        assert_eq!(
            cpu_busy_milli_excl_self(prev, cur, Some(0), None),
            Some(100_000)
        );
    }

    #[test]
    fn cpu_busy_milli_excl_self_none_on_zero_host_delta() {
        // No host time elapsed → None (first-sweep / same-tick contract),
        // regardless of the self readings.
        let cur = CpuStat { total: 100, idle: 50 };
        assert_eq!(cpu_busy_milli_excl_self(cur, cur, Some(0), Some(10)), None);
    }
}
