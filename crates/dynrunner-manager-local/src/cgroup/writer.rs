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
fn compute_workers_memory_max(
    leaf: &Path,
    reserved_bytes: u64,
) -> Result<Option<u64>, CgroupSetupError> {
    let content = std::fs::read_to_string(leaf.join("memory.max")).map_err(CgroupSetupError::Io)?;
    match parse_memory_max(&content) {
        None => Ok(None),
        Some(container_max) => Ok(Some(container_max.saturating_sub(reserved_bytes))),
    }
}

/// Materialise `<leaf>/workers/` with the tightened cap, swap reset,
/// and per-worker subtree delegation. Idempotent on every step:
///
///   1. `create_dir_all(workers_path)` — re-creating an existing
///      directory is a no-op.
///   2. `+memory +pids` writes to `<leaf>/cgroup.subtree_control`,
///      each via [`enable_controller`] which absorbs `EBUSY` /
///      `EINVAL`.
///   3. `<workers>/memory.max` — computed via
///      [`compute_workers_memory_max`]. Skipped when the parent
///      container has no concrete cap (logged at info level).
///   4. `<workers>/memory.swap.max` — explicitly written as `"max"`
///      because cgroup-v2 children inherit `0` (no swap) by default
///      regardless of the parent's `--memory-swap=-1`. Per the
///      design contract: workers must have all swap available so
///      ResourceStealingScheduler's swap-aware budget math holds.
///   5. `+memory +pids` writes to `<workers>/cgroup.subtree_control`
///      so per-worker children inherit those controllers and the
///      memprofile sampler can read each worker's `memory.current`.
///
/// **cgroup-v2 "no internal processes" invariant:** after step 5,
/// `<workers>/` becomes an interior node — the kernel rejects any
/// subsequent write to `<workers>/cgroup.procs` with `EBUSY`. From
/// that moment forward, worker PIDs MUST be attached to a per-worker
/// leaf `<workers>/worker-<id>/` (see
/// [`write_worker_subgroup`] + [`super::SubcgroupHandle::attach_pid`]).
/// The legacy [`write_attach_pid_to_workers`] helper is retained for
/// the LegacyFlat fallback path only.
///
/// **LegacyFlat fallback:** if `<workers>/cgroup.procs` already
/// contains pids (operator upgraded from the flat-cgroup version
/// mid-run), enabling subtree_control would fail with `EBUSY`.
/// Detection: a non-empty `<workers>/cgroup.procs` after the swap
/// write. Action: skip step 5 entirely and emit one warn line; the
/// caller continues to write pids into `<workers>/cgroup.procs` via
/// the legacy helper until the next clean restart. This phase keeps
/// the API shape unchanged for upstream callers; the tagged-result
/// surface (Nested vs LegacyFlat) is introduced in a later phase.
///
/// Returns the constructed workers path so the caller can hand it
/// (or its `cgroup.procs` child / per-worker leaf) to the spawn site.
pub(super) fn write_workers_subgroup(
    leaf: &Path,
    reserved_bytes: u64,
) -> Result<PathBuf, CgroupSetupError> {
    // cgroup-v2 "no internal processes" rule: a cgroup with
    // `subtree_control` set may NOT directly contain processes. Under
    // a bare `systemd-run --user --scope` the secondary IS the only
    // pid in the leaf, so the upcoming `enable_controller(leaf, ...)`
    // would hit EBUSY/EACCES. Self-move our pid into `<leaf>/secondary/`
    // first so the leaf is empty of procs. No-op under rootless-podman
    // (the secondary lives in a runtime-managed sub-cgroup already, so
    // `<leaf>/cgroup.procs` is empty when we get here).
    self_move_into_secondary_if_needed(leaf)?;

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

    if workers_cgroup_procs_has_pids(&workers_path)? {
        tracing::warn!(
            workers_path = %workers_path.display(),
            "workers/ has existing pids; subtree_control deferred — running in flat mode this run. \
             Per-worker memory observability will be unavailable until the next clean restart."
        );
        return Ok(workers_path);
    }

    for controller in CONTROLLERS {
        enable_controller(&workers_path, controller)?;
    }
    tracing::info!(
        workers_path = %workers_path.display(),
        "workers/ subtree_control enabled (+memory +pids); per-worker subgroups ready"
    );

    Ok(workers_path)
}

/// Materialise `<workers>/worker-<id>/` so the spawn site can attach
/// the worker's pid to a leaf cgroup (the parent `workers/` is an
/// interior node after [`write_workers_subgroup`] enables
/// subtree_control on it). Idempotent: `create_dir_all` is
/// re-entrant; the `memory.swap.max` write is overwrite.
///
/// Intentionally writes NO `memory.max`: per-worker enforcement is
/// out of scope; the aggregate cap lives on `workers/memory.max`.
/// `memory.swap.max=max` is written for the same load-bearing reason
/// documented on [`write_workers_subgroup`] — cgroup-v2 children
/// default to zero-swap and must be told otherwise explicitly.
///
/// Returns the absolute path to the worker leaf so the caller can
/// hand it (or its `cgroup.procs` child) to the spawn site.
pub(super) fn write_worker_subgroup(
    workers_path: &Path,
    worker_id: u32,
) -> Result<PathBuf, CgroupSetupError> {
    let worker_path = workers_path.join(format!("worker-{worker_id}"));
    std::fs::create_dir_all(&worker_path).map_err(CgroupSetupError::Io)?;
    std::fs::write(worker_path.join("memory.swap.max"), "max").map_err(CgroupSetupError::Io)?;
    Ok(worker_path)
}

/// Move our own pid into `<leaf>/secondary/` so the leaf becomes
/// empty of processes and the kernel allows the upcoming
/// `subtree_control` write on it. Required when the secondary IS the
/// only pid in the leaf (bare `systemd-run --user --scope` case);
/// no-op when the secondary is already nested below the leaf
/// (rootless-podman puts the container init in a runtime-managed
/// sub-cgroup, leaving `<leaf>/cgroup.procs` empty).
///
/// Behaviour by leaf state:
/// - `cgroup.procs` empty → no move; return `Ok(())`.
/// - `cgroup.procs` contains ONLY our own pid → mkdir
///   `<leaf>/secondary/` (idempotent), write our pid into its
///   `cgroup.procs`, return `Ok(())`. Re-running this on the same
///   leaf is itself a no-op because once we've moved, the leaf is
///   empty and we hit the first branch.
/// - `cgroup.procs` contains foreign pids → warn and return `Ok(())`
///   WITHOUT attempting the move (we don't own those processes).
///   The caller's subsequent `enable_controller(leaf, ...)` will
///   then fail with EBUSY/EACCES and surface as a real error; the
///   `secondary/` self-move is the only safe rescue we can offer.
///
/// We do NOT rmdir `<leaf>/secondary/` on shutdown: the kernel
/// preserves empty cgroups until the parent is removed, and the
/// `systemd-run` scope / podman container teardown removes the whole
/// tree at process exit. The orphan empty leaf is harmless.
fn self_move_into_secondary_if_needed(leaf: &Path) -> Result<(), CgroupSetupError> {
    let leaf_procs = leaf.join("cgroup.procs");
    let content = match std::fs::read_to_string(&leaf_procs) {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(CgroupSetupError::Io(e)),
    };

    let pids: Vec<u32> = content
        .lines()
        .filter_map(|line| line.trim().parse::<u32>().ok())
        .collect();

    if pids.is_empty() {
        return Ok(());
    }

    let self_pid = std::process::id();
    let foreign_pids: Vec<u32> = pids.iter().copied().filter(|&p| p != self_pid).collect();
    if !foreign_pids.is_empty() {
        tracing::warn!(
            leaf = %leaf.display(),
            foreign_pid_count = foreign_pids.len(),
            "leaf cgroup contains foreign pids; cannot self-move. \
             subtree_control write will likely fail and trip the orchestrator's fallback."
        );
        return Ok(());
    }

    let secondary_path = leaf.join("secondary");
    std::fs::create_dir_all(&secondary_path).map_err(CgroupSetupError::Io)?;
    std::fs::write(secondary_path.join("cgroup.procs"), self_pid.to_string())
        .map_err(CgroupSetupError::Io)?;
    tracing::info!(
        leaf = %leaf.display(),
        self_pid,
        "self-moved into <leaf>/secondary/ so leaf subtree_control is writable"
    );
    Ok(())
}

/// Read `<workers>/cgroup.procs`; return `true` if any non-whitespace
/// content is present (i.e. the kernel has at least one pid attached
/// to the workers subgroup). Used to detect the LegacyFlat upgrade
/// path — a non-empty workers/cgroup.procs at init time means the
/// previous run left pids attached at the workers/ level (flat
/// layout), so enabling subtree_control on workers/ would fail with
/// EBUSY per the cgroup-v2 "no internal processes" rule.
///
/// `NotFound` means the kernel has not yet created `cgroup.procs`
/// for a freshly-mkdir'd cgroup; treat as empty (no pids attached).
/// Any other I/O error propagates so callers see kernel/mountpoint
/// anomalies loudly.
fn workers_cgroup_procs_has_pids(workers_path: &Path) -> Result<bool, CgroupSetupError> {
    match std::fs::read_to_string(workers_path.join("cgroup.procs")) {
        Ok(content) => Ok(!content.trim().is_empty()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(CgroupSetupError::Io(e)),
    }
}

/// Append `pid` (decimal) to `<workers>/cgroup.procs` directly.
/// **LegacyFlat fallback path only** — once
/// [`write_workers_subgroup`] enables subtree_control on `workers/`,
/// this write fails with `EBUSY` per the cgroup-v2 "no internal
/// processes" rule. The fallback is taken only when subtree_control
/// is deferred (existing pids found at init); see the LegacyFlat
/// section of [`write_workers_subgroup`].
///
/// `std::fs::write` opens with `O_WRONLY | O_CREAT | O_TRUNC`; the
/// kernel ignores the truncation flag on `cgroup.procs` (it's a
/// pseudo-file accepting append-style writes). The write is a single
/// syscall, fork-safe, and never allocates beyond a small stack
/// buffer for the decimal digits.
///
/// Currently unused at the call-site level — the production caller
/// is wired in a later phase that introduces the tagged Nested /
/// LegacyFlat return surface on the orchestrator. Kept here behind
/// `pub(crate)` (and `#[allow(dead_code)]`) so the fallback writer
/// lives alongside its sibling subtree_control logic instead of
/// being parachuted in later from a separate module.
#[allow(dead_code)]
pub(crate) fn write_attach_pid_to_workers(workers_path: &Path, pid: u32) -> std::io::Result<()> {
    std::fs::write(workers_path.join("cgroup.procs"), pid.to_string())
}
