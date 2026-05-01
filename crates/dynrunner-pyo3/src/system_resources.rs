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

use pyo3::prelude::*;

/// Number of logical CPUs visible to the current process. Under
/// cgroup CPU limits (e.g. SLURM `--cpus-per-task`) the kernel
/// reflects the allocated quota here, which is what we want — we
/// would over-spawn workers if we used the host's physical core
/// count instead. Falls back to 4 if the platform can't report it.
pub(crate) fn detect_logical_cpu_count() -> u32 {
    std::thread::available_parallelism()
        .map(|n| n.get() as u32)
        .unwrap_or(4)
}

/// Total RAM in bytes from `/proc/meminfo` `MemTotal:`. Returns 0
/// if `/proc/meminfo` is unavailable or unparseable; downstream
/// code is expected to either treat 0 as "no memory budget"
/// (surfacing the misdetection as immediate scheduling failure)
/// or substitute an explicit fallback.
pub(crate) fn detect_total_memory_bytes() -> u64 {
    read_meminfo_field("MemTotal:")
}

/// Available RAM in bytes from `/proc/meminfo` `MemAvailable:`,
/// falling back to `MemTotal:` when `MemAvailable` is missing
/// (older kernels). 0 on any read failure.
pub(crate) fn detect_available_memory_bytes() -> u64 {
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
