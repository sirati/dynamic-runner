//! Runtime resource detection + CLI-spec parsing exposed to Python.
//!
//! The framework historically did `multiprocessing.cpu_count()` and
//! `/proc/meminfo` parsing in Python (`python/dynamic_runner/system_resources.py`)
//! purely so it could compute integers it then handed straight back
//! to a `_rs.LocalManagerConfig(...)` / `_rs.SecondaryConfig(...)`
//! call. Same pattern as the psutil refactor: no Python-exclusive
//! content, runs once per dispatch — moved to Rust so the Python
//! layer is thin glue, not a `/proc/meminfo` parser.
//!
//! Exposed PyO3 functions:
//!   - `detect_logical_cpu_count() -> int`
//!   - `detect_total_memory_bytes() -> int`
//!   - `detect_available_memory_bytes() -> int`
//!   - `parse_cores(spec: str) -> int`
//!   - `parse_memory(spec: str) -> int`
//!   - `pick_free_port() -> int`
//!
//! cgroup v2 awareness: under containerised execution (SLURM
//! cgroup_v2 plugin, Kubernetes, podman/docker) the host's
//! `/proc/meminfo:MemTotal` and the kernel's `available_parallelism`
//! both ignore cgroup quotas. A worker that sized itself on those
//! numbers would happily try to use 64 GiB of RAM in a container
//! limited to 4 GiB and get OOM-killed. The `detect_*` functions
//! below clamp the host values against `cpu.max` / `memory.max`
//! walked from the leaf cgroup upward (the kernel enforces the
//! tightest constraint in the chain, so a single ancestor with a
//! tighter cap wins). cgroup v1 hosts and non-Linux platforms see
//! no clamp — `cgroup_v2_*_limit()` returns `None`.

use std::path::{Path, PathBuf};

use pyo3::prelude::*;

/// Number of logical CPUs available to the current process,
/// respecting both kernel cpuset (`available_parallelism`) AND the
/// cgroup v2 CPU bandwidth quota (`cpu.max`). The minimum of the
/// two wins: cpuset cuts the visible CPU set, cpu.max caps the
/// fraction of those CPUs we're allowed to actually use.
///
/// Falls back to `available_parallelism` only when no cgroup v2
/// quota applies (host run, cgroup v1, no quota set). Falls back
/// to 4 if even `available_parallelism` is unavailable.
pub(crate) fn detect_logical_cpu_count() -> u32 {
    let parallelism = std::thread::available_parallelism()
        .map(|n| n.get() as u32)
        .unwrap_or(4);
    match cgroup_v2_cpu_limit() {
        Some(quota_cpus) => parallelism.min(quota_cpus).max(1),
        None => parallelism,
    }
}

/// Total memory budget in bytes, clamped to the cgroup v2
/// `memory.max` chain when one applies. Falls back to
/// `/proc/meminfo:MemTotal` on the host when no cgroup limit is
/// in effect. Returns 0 only when both readings fail; downstream
/// code is expected to either treat 0 as "no memory budget"
/// (surfacing the misdetection as immediate scheduling failure)
/// or substitute an explicit fallback.
pub(crate) fn detect_total_memory_bytes() -> u64 {
    let host = read_meminfo_field("MemTotal:");
    match cgroup_v2_memory_limit() {
        Some(limit) if host == 0 => limit,
        Some(limit) => host.min(limit),
        None => host,
    }
}

/// Available memory in bytes. Inside a memory-limited cgroup,
/// returns `memory.max - memory.current` so the value reflects the
/// container's headroom rather than the host's free memory (which
/// would be misleadingly large). Outside a cgroup limit, falls
/// back to `/proc/meminfo:MemAvailable` and finally to
/// `MemTotal`.
pub(crate) fn detect_available_memory_bytes() -> u64 {
    if let Some(limit) = cgroup_v2_memory_limit() {
        let current = cgroup_v2_memory_current().unwrap_or(0);
        return limit.saturating_sub(current);
    }
    let avail = read_meminfo_field("MemAvailable:");
    if avail > 0 {
        avail
    } else {
        detect_total_memory_bytes()
    }
}

fn read_meminfo_field(prefix: &str) -> u64 {
    let Ok(content) = std::fs::read_to_string("/proc/meminfo") else {
        return 0;
    };
    for line in content.lines() {
        if let Some(rest) = line.strip_prefix(prefix) {
            // Format: "<prefix>       16384000 kB"
            if let Some(kb_str) = rest.split_whitespace().next() {
                if let Ok(kb) = kb_str.parse::<u64>() {
                    return kb * 1024;
                }
            }
        }
    }
    0
}

/// Parse `cpu.max` content into a CPU-count cap.
///
/// Format: `<quota_us> <period_us>` (e.g. `200000 100000` = 2 CPUs
/// of bandwidth per period) or `max <period>` (no quota). Returns
/// `None` when no quota applies. Quota fractions round UP to the
/// nearest integer CPU — a cgroup with quota=50000 period=100000
/// (half a CPU) gets a count of 1, because we can still spawn one
/// worker; the kernel will throttle its CPU time appropriately.
fn parse_cpu_max(content: &str) -> Option<u32> {
    let mut parts = content.split_whitespace();
    let quota_str = parts.next()?;
    if quota_str == "max" {
        return None;
    }
    let quota = quota_str.parse::<u64>().ok()?;
    let period = parts.next()?.parse::<u64>().ok()?;
    if period == 0 {
        return None;
    }
    let cpus = quota.div_ceil(period).max(1);
    Some(cpus.min(u32::MAX as u64) as u32)
}

/// Parse `memory.max` content into a byte cap, or `None` for `max`.
fn parse_memory_max(content: &str) -> Option<u64> {
    let s = content.trim();
    if s == "max" {
        return None;
    }
    s.parse::<u64>().ok()
}

/// Resolve the leaf cgroup v2 directory of the calling process.
///
/// Reads `/proc/self/cgroup` looking for the v2 hierarchy line
/// (`0::<path>`) and joins the path against `/sys/fs/cgroup`.
/// Returns `None` on a v1-only host, on a platform without
/// `/proc/self/cgroup`, or when the file shape is unexpected.
fn cgroup_v2_leaf() -> Option<PathBuf> {
    let content = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    for line in content.lines() {
        if let Some(rest) = line.strip_prefix("0::") {
            let rel = rest.trim_start_matches('/');
            return Some(Path::new("/sys/fs/cgroup").join(rel));
        }
    }
    None
}

/// Walk from `leaf` up to `root` (inclusive of both), reading
/// `<file>` at each level. The kernel enforces the tightest cap
/// in the chain, so we take the minimum of every parser-accepted
/// reading. `parse` returns `None` for "no cap at this level"
/// (e.g. `cpu.max == "max"`) and `Some(n)` for a concrete cap.
///
/// `root` and `leaf` are explicit so tests can drive the walk
/// against a synthetic tempdir tree. Callers that don't care
/// about test injection use the convenience wrappers below.
fn cgroup_v2_walk_min_in<T, F>(root: &Path, leaf: &Path, file: &str, parse: F) -> Option<T>
where
    T: Ord + Copy,
    F: Fn(&str) -> Option<T>,
{
    let mut current = leaf;
    let mut min: Option<T> = None;
    loop {
        let path = current.join(file);
        if let Ok(content) = std::fs::read_to_string(&path) {
            if let Some(v) = parse(&content) {
                min = Some(min.map_or(v, |m| m.min(v)));
            }
        }
        if current == root {
            break;
        }
        match current.parent() {
            Some(p) if p.starts_with(root) || p == root => current = p,
            _ => break,
        }
    }
    min
}

const CGROUP_V2_ROOT: &str = "/sys/fs/cgroup";

/// Effective cgroup v2 CPU cap (in CPUs) — minimum of `cpu.max`
/// at every level from the process's leaf cgroup up to
/// `/sys/fs/cgroup`. `None` means no cap applies.
fn cgroup_v2_cpu_limit() -> Option<u32> {
    let leaf = cgroup_v2_leaf()?;
    cgroup_v2_walk_min_in(Path::new(CGROUP_V2_ROOT), &leaf, "cpu.max", parse_cpu_max)
}

/// Effective cgroup v2 memory cap (in bytes) — minimum of
/// `memory.max` at every level. `None` means no cap applies.
fn cgroup_v2_memory_limit() -> Option<u64> {
    let leaf = cgroup_v2_leaf()?;
    cgroup_v2_walk_min_in(
        Path::new(CGROUP_V2_ROOT),
        &leaf,
        "memory.max",
        parse_memory_max,
    )
}

/// Current memory usage from the leaf cgroup's `memory.current`,
/// for headroom calculations in `detect_available_memory_bytes`.
/// Reads at the leaf only — usage at the leaf is what counts
/// against the chain's tightest cap.
fn cgroup_v2_memory_current() -> Option<u64> {
    let leaf = cgroup_v2_leaf()?;
    let content = std::fs::read_to_string(leaf.join("memory.current")).ok()?;
    content.trim().parse::<u64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_cpu_max_unlimited() {
        assert_eq!(parse_cpu_max("max 100000\n"), None);
    }

    #[test]
    fn parse_cpu_max_two_cpus() {
        assert_eq!(parse_cpu_max("200000 100000\n"), Some(2));
    }

    #[test]
    fn parse_cpu_max_half_cpu_rounds_up_to_one() {
        assert_eq!(parse_cpu_max("50000 100000"), Some(1));
    }

    #[test]
    fn parse_cpu_max_one_and_a_half_rounds_up_to_two() {
        assert_eq!(parse_cpu_max("150000 100000"), Some(2));
    }

    #[test]
    fn parse_cpu_max_garbage_returns_none() {
        assert_eq!(parse_cpu_max("garbage"), None);
        assert_eq!(parse_cpu_max(""), None);
        assert_eq!(parse_cpu_max("100000"), None);
        assert_eq!(parse_cpu_max("100000 0"), None);
    }

    #[test]
    fn parse_memory_max_unlimited() {
        assert_eq!(parse_memory_max("max\n"), None);
    }

    #[test]
    fn parse_memory_max_bytes() {
        assert_eq!(parse_memory_max("4294967296\n"), Some(4_294_967_296));
    }

    #[test]
    fn parse_memory_max_garbage_returns_none() {
        assert_eq!(parse_memory_max(""), None);
        assert_eq!(parse_memory_max("not_a_number"), None);
    }

    fn write_file(p: &Path, body: &str) {
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, body).unwrap();
    }

    #[test]
    fn walk_picks_tightest_cap_along_chain() {
        let root = tempfile::tempdir().unwrap();
        let r = root.path();
        let leaf = r.join("a/b/c");
        std::fs::create_dir_all(&leaf).unwrap();
        // Root: 8G; mid: 4G; leaf: max → effective is 4G.
        write_file(&r.join("memory.max"), "8589934592\n");
        write_file(&r.join("a/memory.max"), "8589934592\n");
        write_file(&r.join("a/b/memory.max"), "4294967296\n");
        write_file(&r.join("a/b/c/memory.max"), "max\n");
        let got = cgroup_v2_walk_min_in(r, &leaf, "memory.max", parse_memory_max);
        assert_eq!(got, Some(4_294_967_296));
    }

    #[test]
    fn walk_returns_none_when_no_caps_in_chain() {
        let root = tempfile::tempdir().unwrap();
        let r = root.path();
        let leaf = r.join("a/b");
        std::fs::create_dir_all(&leaf).unwrap();
        write_file(&r.join("memory.max"), "max\n");
        write_file(&r.join("a/memory.max"), "max\n");
        write_file(&r.join("a/b/memory.max"), "max\n");
        let got = cgroup_v2_walk_min_in(r, &leaf, "memory.max", parse_memory_max);
        assert_eq!(got, None);
    }

    #[test]
    fn walk_handles_missing_files_along_chain() {
        // cpu.max often isn't present at every level (subtree_control
        // gates which controllers are visible per cgroup). Missing
        // file at a level → skip; cap from the level that has it
        // should still apply.
        let root = tempfile::tempdir().unwrap();
        let r = root.path();
        let leaf = r.join("a/b/c");
        std::fs::create_dir_all(&leaf).unwrap();
        // Only `a/cpu.max` exists; root and leaf don't.
        write_file(&r.join("a/cpu.max"), "200000 100000\n");
        let got = cgroup_v2_walk_min_in(r, &leaf, "cpu.max", parse_cpu_max);
        assert_eq!(got, Some(2));
    }

    #[test]
    fn walk_terminates_at_root_even_if_leaf_outside_root() {
        // Defensive: if cgroup_v2_leaf() returned a path that
        // somehow didn't start under root, the loop should not
        // walk forever.
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let r = root.path();
        let leaf = outside.path();
        // Single-iteration walk: read leaf, current != root, parent
        // doesn't start_with root → break.
        let got = cgroup_v2_walk_min_in(r, leaf, "memory.max", parse_memory_max);
        assert_eq!(got, None);
    }
}

/// Parse a CLI cores spec into a concrete worker count.
///
/// Accepted forms:
///   - `"N"`     → N (clamped to ≥1).
///   - `"+N"`    → detected_cpu_count + N (clamped to ≤ detected).
///   - `"-N"`    → detected_cpu_count - N (clamped to ≥1).
///
/// Returns ValueError for any other shape.
#[pyfunction]
pub(crate) fn parse_cores(spec: &str) -> PyResult<u32> {
    let total = detect_logical_cpu_count();
    if let Some(rest) = spec.strip_prefix('+') {
        let delta = rest.parse::<u32>().map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "parse_cores: invalid +delta in {spec:?}: {e}"
            ))
        })?;
        Ok(total.saturating_add(delta).min(total))
    } else if let Some(rest) = spec.strip_prefix('-') {
        let delta = rest.parse::<u32>().map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "parse_cores: invalid -delta in {spec:?}: {e}"
            ))
        })?;
        Ok(total.saturating_sub(delta).max(1))
    } else {
        let n = spec.parse::<u32>().map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "parse_cores: expected integer or +N/-N, got {spec:?}: {e}"
            ))
        })?;
        Ok(n.max(1))
    }
}

/// Parse a CLI memory spec into bytes.
///
/// Accepted forms:
///   - `"NG"`    → N gigabytes (absolute).
///   - `"NM"`    → N megabytes (absolute).
///   - `"+NG"` / `"+NM"` → detected_available_bytes + N{G|M}.
///   - `"-NG"` / `"-NM"` → detected_available_bytes - N{G|M},
///                          floored at 1 GiB.
///
/// Suffix is required: a bare integer raises ValueError.
#[pyfunction]
pub(crate) fn parse_memory(spec: &str) -> PyResult<u64> {
    let (sign, rest) = if let Some(rest) = spec.strip_prefix('+') {
        (Some(1i64), rest)
    } else if let Some(rest) = spec.strip_prefix('-') {
        (Some(-1i64), rest)
    } else {
        (None, spec)
    };

    let bytes = if let Some(num) = rest.strip_suffix('G') {
        num.parse::<u64>()
            .map_err(|e| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "parse_memory: invalid number in {spec:?}: {e}"
                ))
            })?
            .checked_mul(1024 * 1024 * 1024)
            .ok_or_else(|| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "parse_memory: GB value overflows in {spec:?}"
                ))
            })?
    } else if let Some(num) = rest.strip_suffix('M') {
        num.parse::<u64>()
            .map_err(|e| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "parse_memory: invalid number in {spec:?}: {e}"
                ))
            })?
            .checked_mul(1024 * 1024)
            .ok_or_else(|| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "parse_memory: MB value overflows in {spec:?}"
                ))
            })?
    } else {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "parse_memory: spec must end with 'M' or 'G': {spec:?}"
        )));
    };

    match sign {
        None => Ok(bytes),
        Some(1) => Ok(detect_available_memory_bytes().saturating_add(bytes)),
        Some(-1) => Ok(detect_available_memory_bytes()
            .saturating_sub(bytes)
            .max(1024 * 1024 * 1024)),
        _ => unreachable!(),
    }
}

/// Bind to TCP port 0, read the OS-assigned port, drop the
/// listener. The caller (e.g. SLURM packaging pipeline) re-binds
/// the same port via the Rust primary coordinator after setting
/// up SSH `-R` forwarding to it; the temp listener is just to
/// claim a free port number.
#[pyfunction]
pub(crate) fn pick_free_port() -> PyResult<u16> {
    let listener = std::net::TcpListener::bind("0.0.0.0:0").map_err(|e| {
        pyo3::exceptions::PyOSError::new_err(format!("pick_free_port: bind failed: {e}"))
    })?;
    let port = listener
        .local_addr()
        .map_err(|e| {
            pyo3::exceptions::PyOSError::new_err(format!(
                "pick_free_port: local_addr failed: {e}"
            ))
        })?
        .port();
    drop(listener);
    Ok(port)
}
