//! Idempotent cgroup-v2 writes for the workers/ subgroup.
//!
//! Single concern: given a writable, memory-controller-bearing leaf
//! cgroup directory, materialise `<leaf>/workers/` with `memory.max`
//! tightened by `reserved_bytes` and `memory.swap.max` reset to
//! `"max"`. Idempotent — re-running against an already-prepared
//! subgroup is a no-op.
//!
//! The orchestration (cgroup-v2 detection, controller probing,
//! writability check, warn-line on graceful fallback) lives in
//! [`super`]; this file only does the writes once the orchestrator
//! has decided the writes are safe to attempt.

use std::path::{Path, PathBuf};

use super::error::CgroupSetupError;

/// Per-controller token written to `<leaf>/cgroup.subtree_control` to
/// delegate the controller into child cgroups. We need `memory` for
/// the tightened cap and `pids` so any subprocess-spawn counter the
/// workers might fork against is also accounted at the subgroup
/// level. Writing each controller as a separate call lets us
/// distinguish "already enabled" (silently ignored) from "real
/// failure" without parsing combined error shapes.
const CONTROLLERS: &[&str] = &["+memory", "+pids"];

/// Write `controller` to `<leaf>/cgroup.subtree_control`. The kernel
/// surfaces "already enabled" as `EBUSY` on some kernels and
/// `EINVAL` on others — both are swallowed; only unexpected
/// `ErrorKind` values propagate. Genuine permission failures and
/// missing-controller errors will surface as `PermissionDenied` /
/// `NotFound` and propagate via `?` to the caller.
fn enable_controller(leaf: &Path, controller: &str) -> Result<(), CgroupSetupError> {
    let path = leaf.join("cgroup.subtree_control");
    match std::fs::write(&path, controller) {
        Ok(()) => Ok(()),
        Err(e) => match e.kind() {
            // EBUSY / EINVAL on a controller that is already enabled
            // for this subtree. The kernel's documented behaviour
            // across versions is to surface one of the two; both mean
            // "no-op succeeded".
            std::io::ErrorKind::ResourceBusy | std::io::ErrorKind::InvalidInput => Ok(()),
            _ => Err(CgroupSetupError::Io(e)),
        },
    }
}

/// Parse the leaf's `memory.max` content into a byte cap or `None`
/// (the literal `"max"`). Mirrors the parser in
/// `dynrunner_pyo3::system_resources::detection`; we do not re-use
/// that one because it lives in a different crate and the parser is
/// a two-line trim-and-parse that we keep local to the cgroup module
/// so this concern stays self-contained.
fn parse_memory_max(content: &str) -> Option<u64> {
    let s = content.trim();
    if s == "max" {
        return None;
    }
    s.parse::<u64>().ok()
}

/// Read `<leaf>/memory.max` and compute the tightened cap for the
/// workers subgroup. Returns `Ok(None)` when the container has no
/// concrete cap (literal `"max"`) — at that point any value we wrote
/// would be artificially capping the workers, which is wrong: the
/// secondary container itself has no memory ceiling, so the workers
/// shouldn't inherit one.
///
/// Saturating subtraction guards against `reserved_bytes >
/// container_max` (operator misconfiguration); the result floors at
/// 0, which the kernel rejects, surfacing the misconfiguration as a
/// loud write error rather than silently nullifying the cap.
fn compute_workers_memory_max(leaf: &Path, reserved_bytes: u64) -> Result<Option<u64>, CgroupSetupError> {
    let content = std::fs::read_to_string(leaf.join("memory.max")).map_err(CgroupSetupError::Io)?;
    match parse_memory_max(&content) {
        None => Ok(None),
        Some(container_max) => Ok(Some(container_max.saturating_sub(reserved_bytes))),
    }
}

/// Materialise `<leaf>/workers/` with the tightened cap and swap
/// reset. Idempotent on every step:
///
///   1. `create_dir_all(workers_path)` — re-creating an existing
///      directory is a no-op.
///   2. `+memory +pids` writes to `cgroup.subtree_control`, each via
///      [`enable_controller`] which absorbs `EBUSY` / `EINVAL`.
///   3. `<workers>/memory.max` — computed via
///      [`compute_workers_memory_max`]. Skipped when the parent
///      container has no concrete cap (logged at info level).
///   4. `<workers>/memory.swap.max` — explicitly written as `"max"`
///      because cgroup-v2 children inherit `0` (no swap) by default
///      regardless of the parent's `--memory-swap=-1`. Per the
///      design contract: workers must have all swap available so
///      ResourceStealingScheduler's swap-aware budget math holds.
///
/// Returns the constructed workers path so the caller can hand it
/// (or its `cgroup.procs` child) to the spawn site.
pub(super) fn write_workers_subgroup(
    leaf: &Path,
    reserved_bytes: u64,
) -> Result<PathBuf, CgroupSetupError> {
    let workers_path = leaf.join("workers");
    std::fs::create_dir_all(&workers_path).map_err(CgroupSetupError::Io)?;

    for controller in CONTROLLERS {
        enable_controller(leaf, controller)?;
    }

    match compute_workers_memory_max(leaf, reserved_bytes)? {
        None => {
            tracing::info!(
                workers_path = %workers_path.display(),
                "parent cgroup has no concrete memory.max (literal \"max\"); \
                 skipping workers/memory.max so workers inherit the unbounded parent"
            );
        }
        Some(tightened) => {
            std::fs::write(workers_path.join("memory.max"), tightened.to_string())
                .map_err(CgroupSetupError::Io)?;
        }
    }

    // LOAD-BEARING: cgroup-v2 children's `memory.swap.max` defaults
    // to 0 (no swap) regardless of the parent's swap config. Write
    // `"max"` explicitly so the workers see whatever swap the
    // kernel exposes to the parent — the secondary's own
    // `--memory-swap=-1` is otherwise silently overridden.
    std::fs::write(workers_path.join("memory.swap.max"), "max").map_err(CgroupSetupError::Io)?;

    Ok(workers_path)
}

/// Append `pid` (decimal) to `<workers>/cgroup.procs`. The kernel
/// migrates the named pid into the workers subgroup at the moment
/// of the write — used in the `pre_exec` closure of every worker
/// `Command` so the child lands in the workers subgroup BEFORE
/// `execve(2)` returns.
///
/// `std::fs::write` opens with `O_WRONLY | O_CREAT | O_TRUNC`; the
/// kernel ignores the truncation flag on `cgroup.procs` (it's a
/// pseudo-file accepting append-style writes). The write is a single
/// syscall, fork-safe, and never allocates beyond a small stack
/// buffer for the decimal digits.
pub(super) fn write_attach_pid(workers_path: &Path, pid: u32) -> std::io::Result<()> {
    std::fs::write(workers_path.join("cgroup.procs"), pid.to_string())
}
