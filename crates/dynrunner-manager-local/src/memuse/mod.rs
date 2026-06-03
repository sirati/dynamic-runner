//! Per-secondary aggregate memuse log — one CSV line per task
//! completion. Shared writer primitive so every dispatch path
//! (in-process `LocalManager`, SLURM secondary, multi-computer-local
//! secondary, in-process distributed secondary) appends to the
//! same file shape without duplicating the open/format/write
//! sequence at each caller.
//!
//! Concern boundary: this module owns ONLY the
//! filename → row-format → append logic. Decisions about WHEN
//! to log (which event, which call-site) and WHAT to log (which
//! resource map represents the actual usage) live with each
//! caller. The single entry point [`log_resource_usage`] takes a
//! ready-made path + the per-task descriptor and does the
//! filesystem work.

use std::io::Write;
use std::path::Path;

use dynrunner_core::{Identifier, ResourceKind, ResourceMap, TaskInfo};

/// Append one CSV line to the aggregate memuse log at `log_path`.
///
/// Row shape: `size,estimated_memory,actual_memory,filename,status`.
/// `status` is `"OK"` when the task succeeded, `"ERROR"` otherwise.
///
/// `binary = None` is a documented no-op (some completion paths
/// lose the `TaskInfo` before reaching the logger; the caller is
/// not expected to fabricate one). Open / write errors degrade to
/// a single `tracing::warn!` — the memuse log is a best-effort
/// observability artifact, never load-bearing for run correctness.
pub fn log_resource_usage<I: Identifier>(
    log_path: &Path,
    binary: Option<&TaskInfo<I>>,
    estimated: &ResourceMap,
    actual: &ResourceMap,
    errored: bool,
) {
    let Some(binary) = binary else { return };

    let status = if errored { "ERROR" } else { "OK" };
    let filename = binary
        .path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();

    let estimated_mem = estimated.get(&ResourceKind::memory());
    let actual_mem = actual.get(&ResourceKind::memory());

    match std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
    {
        Ok(mut f) => {
            let _ = writeln!(
                f,
                "{},{},{},{},{}",
                binary.size, estimated_mem, actual_mem, filename, status,
            );
        }
        Err(e) => {
            tracing::warn!(error = %e, "failed to write memuse log");
        }
    }
}

/// Compose the per-secondary memuse log path from a run-level
/// output dir and an optional operator-supplied explicit path.
///
/// Priority:
///   1. `explicit = Some(path)` → honour the operator's choice
///      verbatim. Lets tests and rare custom configs point the
///      log at a fixed location.
///   2. `output_dir = Some(dir)` → `dir/memuse.log` (the
///      symmetric default applied across every dispatch path).
///   3. Both `None` → `None` (logging disabled).
///
/// Single source of truth for the filename via
/// [`crate::memprofile::config::MEMUSE_LOG_FILENAME`].
pub fn derive_memuse_log_path(
    output_dir: Option<&Path>,
    explicit: Option<&Path>,
) -> Option<std::path::PathBuf> {
    if let Some(p) = explicit {
        return Some(p.to_path_buf());
    }
    output_dir.map(|d| d.join(crate::memprofile::config::MEMUSE_LOG_FILENAME))
}

#[cfg(test)]
mod tests {
    use super::derive_memuse_log_path;
    use std::path::{Path, PathBuf};

    #[test]
    fn no_output_dir_no_explicit_returns_none() {
        // Test fixtures legitimately want "don't write a log"; this
        // shape preserves the LocalManagerConfig.memuse_log_path
        // None=disabled semantic.
        assert!(derive_memuse_log_path(None, None).is_none());
    }

    #[test]
    fn output_dir_only_appends_default_filename() {
        let resolved = derive_memuse_log_path(Some(Path::new("/tmp/run")), None)
            .expect("output_dir alone must compose");
        assert_eq!(resolved, PathBuf::from("/tmp/run/memuse.log"));
    }

    #[test]
    fn explicit_overrides_output_dir_default() {
        let resolved = derive_memuse_log_path(
            Some(Path::new("/tmp/run")),
            Some(Path::new("/var/log/custom-memuse.csv")),
        )
        .expect("explicit must be honoured");
        assert_eq!(resolved, PathBuf::from("/var/log/custom-memuse.csv"));
    }

    #[test]
    fn explicit_alone_is_honoured() {
        // No output_dir but an explicit path — the operator
        // points the log at a fixed location independent of any
        // run-level dir.
        let resolved = derive_memuse_log_path(None, Some(Path::new("/var/log/custom.csv")))
            .expect("explicit alone must be honoured");
        assert_eq!(resolved, PathBuf::from("/var/log/custom.csv"));
    }
}
