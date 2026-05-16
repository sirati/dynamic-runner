//! CLI-spec parsing for `cores` and `memory` flags.
//!
//! Per-machine semantic: each secondary resolves the spec against
//! its own host's detected CPU count / available memory, so a
//! `--cores -2` on a 32-core node yields 30 and on an 8-core node
//! yields 6. The primary does not pre-resolve and forward an
//! absolute count; the spec string is plumbed verbatim.

use pyo3::prelude::*;

use super::detection::{detect_available_memory_bytes, detect_logical_cpu_count};

/// Parse a CLI cores spec against an explicit `total`. Pure helper
/// used by both the PyO3-exported `parse_cores` and the in-crate
/// unit tests; takes the detected CPU count as input so tests can
/// drive deterministic values instead of asserting against the
/// host's actual CPU count.
///
/// Accepted forms (see `parse_cores` for the user-facing doc):
///   - `"0"`     → `total` (all-cores sentinel).
///   - `"N"`     → N (absolute, clamped to ≥1 for N≥1).
///   - `"+N"`    → total + N (clamped to ≤ total).
///   - `"-N"`    → total - N (clamped to ≥1).
///   - `"-0"`    → equivalent to `"0"`.
///
/// Returns `Err(msg)` for any other shape; callers wrap into a
/// PyValueError. `msg` does NOT include the `parse_cores: ` prefix
/// — that's the PyO3 wrapper's concern so callers can add their
/// own context.
fn resolve_cores_spec(spec: &str, total: u32) -> Result<u32, String> {
    if let Some(rest) = spec.strip_prefix('+') {
        let delta = rest
            .parse::<u32>()
            .map_err(|e| format!("invalid +delta in {spec:?}: {e}"))?;
        Ok(total.saturating_add(delta).min(total))
    } else if let Some(rest) = spec.strip_prefix('-') {
        let delta = rest
            .parse::<u32>()
            .map_err(|e| format!("invalid -delta in {spec:?}: {e}"))?;
        Ok(total.saturating_sub(delta).max(1))
    } else {
        let n = spec
            .parse::<u32>()
            .map_err(|e| format!("expected integer or +N/-N, got {spec:?}: {e}"))?;
        // "0" is the documented sentinel for "all available cores".
        // The clamp-to-≥1 only applies to positive specs.
        if n == 0 { Ok(total) } else { Ok(n) }
    }
}

/// Parse a CLI cores spec into a concrete worker count for the
/// machine the call runs on (per-machine semantic).
///
/// Accepted forms:
///   - `"0"`     → all detected cores (the all-cores sentinel).
///   - `"N"`     → N (absolute hard limit, clamped to ≥1 for N≥1).
///   - `"+N"`    → detected_cpu_count + N (clamped to ≤ detected).
///   - `"-N"`    → detected_cpu_count - N (clamped to ≥1).
///   - `"-0"`    → equivalent to `"0"` (offset-by-zero from detected).
///
/// Per-machine semantic: each secondary resolves the spec against
/// its own host's detected CPU count, so a `--cores -2` on a
/// 32-core node yields 30 and on an 8-core node yields 6. The
/// primary does not pre-resolve and forward an absolute count;
/// the spec string is plumbed verbatim to each secondary.
///
/// Returns ValueError for any other shape.
#[pyfunction]
pub(crate) fn parse_cores(spec: &str) -> PyResult<u32> {
    resolve_cores_spec(spec, detect_logical_cpu_count())
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("parse_cores: {e}")))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_cores_zero_means_all_available() {
        // Bare "0" is the sentinel for "all detected cores", not
        // "one worker". Pinning a deterministic `total` to decouple
        // the assertion from the host's actual CPU count.
        assert_eq!(resolve_cores_spec("0", 32).unwrap(), 32);
        assert_eq!(resolve_cores_spec("0", 4).unwrap(), 4);
    }

    #[test]
    fn resolve_cores_minus_zero_equivalent_to_zero() {
        // `"-0"` (subtract zero from detected) MUST yield the same
        // result as `"0"`. The CLI default historically used `"-0"`
        // and consumers expect both notations interchangeable.
        assert_eq!(resolve_cores_spec("-0", 32).unwrap(), 32);
        assert_eq!(resolve_cores_spec("-0", 4).unwrap(), 4);
    }

    #[test]
    fn resolve_cores_positive_absolute() {
        // Hard limit: user-supplied N stands as-is regardless of
        // detected. `--cores 2` = 2 workers on a 32-core box and
        // 2 workers on a 4-core box (per the per-machine spec).
        assert_eq!(resolve_cores_spec("2", 32).unwrap(), 2);
        assert_eq!(resolve_cores_spec("8", 4).unwrap(), 8);
    }

    #[test]
    fn resolve_cores_negative_offset_floors_at_one() {
        // `-N` where N >= total MUST floor to 1, not 0. Without the
        // floor the secondary would spawn 0 workers and then
        // deadlock at the wait-for-first-worker barrier.
        assert_eq!(resolve_cores_spec("-99", 4).unwrap(), 1);
        assert_eq!(resolve_cores_spec("-30", 32).unwrap(), 2);
    }

    #[test]
    fn resolve_cores_positive_offset_clamps_to_total() {
        // `+N` may not exceed detected — there's no spawning
        // workers we don't have hardware threads for.
        assert_eq!(resolve_cores_spec("+10", 4).unwrap(), 4);
        assert_eq!(resolve_cores_spec("+0", 32).unwrap(), 32);
    }

    #[test]
    fn resolve_cores_garbage_returns_err() {
        assert!(resolve_cores_spec("garbage", 32).is_err());
        assert!(resolve_cores_spec("+abc", 32).is_err());
        assert!(resolve_cores_spec("-xyz", 32).is_err());
        assert!(resolve_cores_spec("", 32).is_err());
    }
}
