//! Nested cgroup-v2 workers/ subgroup setup.
//!
//! Single concern: detect the calling process's cgroup-v2 leaf, create
//! a `workers/` subgroup with a tightened `memory.max`, and expose
//! the path used to attach worker PIDs into it. The motivation is to
//! let a kernel cgroup-OOM event in the workers subgroup kill ONLY
//! the workers — leaving the secondary process (which stays in the
//! parent leaf) alive so the framework can observe the kill, requeue
//! the displaced task, and report cleanly.
//!
//! API surface (boundary the worker-pool init and worker-spawn sites
//! see):
//!
//! * [`setup_worker_cgroup`] — orchestrator. Accepts an explicit
//!   `cgroup_root` so tests can drive the flow against a tempdir-
//!   rooted fake `/sys/fs/cgroup`. Production callers go through
//!   [`setup_worker_cgroup_default`].
//! * [`NestedCgroupHandle`] — opaque RAII-style holder of the
//!   workers/ path. Carried on the worker pool; cloned into each
//!   `WorkerFactory` so the spawn site can attach the child PID
//!   without re-traversing the cgroup tree.
//! * [`attach_pid`] — single-call primitive that writes a pid to
//!   `<workers>/cgroup.procs`. Called from the spawn site's
//!   `pre_exec` closure after `fork(2)` and before `execve(2)`.
//!
//! Graceful fallback contract: any of (a) not cgroup-v2, (b) no
//! `memory` controller exposed on the leaf, (c) leaf not writable
//! returns `Ok(None)` plus a single-line `tracing::warn!` so an
//! operator running outside a delegated cgroup environment (host
//! development, CI, non-Linux) sees one log line and the flat
//! (pre-nested) layout proceeds unchanged. `Err(_)` is reserved for
//! unexpected I/O failures (corrupted `/proc`, transient sysfs read
//! errors); the caller treats it as fatal.

use std::path::{Path, PathBuf};

pub use self::error::CgroupSetupError;

mod error;
#[cfg(test)]
mod tests;
mod writer;

/// Production cgroup-v2 root. Tests inject a tempdir-rooted alternative
/// via [`setup_worker_cgroup`].
const CGROUP_V2_ROOT: &str = "/sys/fs/cgroup";

/// Opaque handle to the materialised `workers/` subgroup. Held on
/// the worker pool for the run lifetime; cloned/referenced by every
/// worker `Command` builder so the per-spawn `pre_exec` closure can
/// reach `cgroup.procs` without a fresh cgroup-tree walk.
#[derive(Debug, Clone)]
pub struct NestedCgroupHandle {
    /// Absolute path to the `<leaf>/workers/` directory the setup
    /// flow materialised. Used directly by [`attach_pid`] (which
    /// joins `cgroup.procs`) and exposed via [`Self::workers_path`]
    /// for callers that need the directory itself (e.g. teardown).
    workers_path: PathBuf,
}

impl NestedCgroupHandle {
    /// The absolute `<leaf>/workers/` path the setup flow created.
    /// Production callers shouldn't need this — they use
    /// [`attach_pid`] — but it's surfaced for diagnostic logging
    /// and tests that assert on the materialised layout.
    pub fn workers_path(&self) -> &Path {
        &self.workers_path
    }

    /// Test-only constructor. Allows downstream crate tests to
    /// build a handle from a tempdir-rooted fake workers/
    /// directory without exercising the orchestrator's
    /// `/proc/self/cgroup` walk. Production code constructs the
    /// handle exclusively via [`setup_worker_cgroup`] /
    /// [`setup_worker_cgroup_default`].
    #[doc(hidden)]
    pub fn from_workers_path_for_test(workers_path: PathBuf) -> Self {
        Self { workers_path }
    }
}

/// Production entry: orchestrate the nested-cgroup setup against the
/// real `/sys/fs/cgroup`. Wraps [`setup_worker_cgroup`] with the
/// production root constant so call sites stay at zero-argument
/// configuration.
///
/// `reserved_bytes`: how much of the container's `memory.max` to
/// withhold from the workers subgroup. Set to the per-secondary
/// memory budget reserved for the secondary process (estimator
/// scratch + per-secondary HashMaps etc.) so a worker memory blowup
/// never reaches the parent leaf's cap and so never OOM-kills the
/// secondary.
pub fn setup_worker_cgroup_default(
    reserved_bytes: u64,
) -> Result<Option<NestedCgroupHandle>, CgroupSetupError> {
    setup_worker_cgroup(Path::new(CGROUP_V2_ROOT), reserved_bytes)
}

/// Test-injectable entry: orchestrate setup against an arbitrary
/// `cgroup_root`. The flow:
///
///   1. Resolve `cgroup_v2_leaf(cgroup_root)`. `None` means the
///      caller is not under cgroup-v2 (or `/proc/self/cgroup` is
///      missing); fall back gracefully.
///   2. Probe `<leaf>/cgroup.controllers` for `memory`. Missing
///      means the parent hasn't delegated the memory controller
///      into our subtree; fall back gracefully.
///   3. Probe `<leaf>/cgroup.subtree_control` for write access. A
///      `PermissionDenied` / `ReadOnlyFilesystem` open means the
///      cgroup tree is not delegated to us; fall back gracefully.
///   4. On success, hand off to [`writer::write_workers_subgroup`]
///      which does the idempotent directory + memory.max + swap
///      writes and returns the absolute `workers/` path.
///
/// Returns `Ok(Some(handle))` on success, `Ok(None)` on the three
/// graceful-fallback conditions (each accompanied by a single
/// `tracing::warn!` line), and `Err(_)` on truly unexpected I/O
/// errors.
pub fn setup_worker_cgroup(
    cgroup_root: &Path,
    reserved_bytes: u64,
) -> Result<Option<NestedCgroupHandle>, CgroupSetupError> {
    let Some(leaf) = cgroup_v2_leaf(cgroup_root) else {
        tracing::warn!(
            "cgroup-v2 leaf not found (not running under cgroup-v2); workers will share the flat cgroup. \
             Operator hint: kernel-OOM in workers will reap the secondary too — \
             prefer a delegated cgroup-v2 environment (rootless podman with `--cgroup-manager=cgroupfs`)."
        );
        return Ok(None);
    };

    if !leaf_has_memory_controller(&leaf)? {
        tracing::warn!(
            leaf = %leaf.display(),
            "cgroup-v2 leaf does not expose the memory controller; workers will share the flat cgroup. \
             Operator hint: the parent cgroup must enable memory via `cgroup.subtree_control += memory`."
        );
        return Ok(None);
    }

    if !leaf_subtree_writable(&leaf) {
        tracing::warn!(
            leaf = %leaf.display(),
            "cgroup-v2 leaf subtree_control is not writable; workers will share the flat cgroup. \
             Operator hint: the cgroup tree must be delegated to the runtime user \
             (rootless podman + `Delegate=yes` on the user@.service, or `loginctl enable-linger`)."
        );
        return Ok(None);
    }

    let workers_path = writer::write_workers_subgroup(&leaf, reserved_bytes)?;
    tracing::info!(
        workers_path = %workers_path.display(),
        reserved_bytes,
        "nested workers cgroup ready; subprocesses will attach via pre_exec"
    );
    Ok(Some(NestedCgroupHandle { workers_path }))
}

/// Attach `pid` to the workers subgroup represented by `handle`.
/// Single-syscall primitive used from a spawn-side `pre_exec`
/// closure where the child has just `fork()`ed and needs to land in
/// the workers cgroup before `execve(2)`.
///
/// Returns `std::io::Error` on failure so the caller (a `pre_exec`
/// closure) can propagate the error through `Result<(), io::Error>`
/// — the kernel will then abort the exec and the parent's
/// `Command::spawn` returns the same error. Failures here are
/// near-impossible in production (the path was validated during
/// setup) but propagated rather than swallowed to surface kernel-
/// level regressions loudly.
pub fn attach_pid(handle: &NestedCgroupHandle, pid: u32) -> std::io::Result<()> {
    writer::write_attach_pid(&handle.workers_path, pid)
}

/// Resolve the calling process's cgroup-v2 leaf by reading
/// `/proc/self/cgroup` and joining the `0::<path>` line against
/// `cgroup_root`. Mirrors the equivalent helper in
/// `dynrunner_pyo3::system_resources::detection::cgroup_v2_leaf` —
/// kept local here (rather than re-exported) because the pyo3 crate
/// depends on this crate, not vice versa, and the helper is small
/// enough that duplicating it avoids creating a new tiny "cgroup
/// detection" crate just to share three lines.
fn cgroup_v2_leaf(cgroup_root: &Path) -> Option<PathBuf> {
    let content = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    for line in content.lines() {
        if let Some(rest) = line.strip_prefix("0::") {
            let rel = rest.trim_start_matches('/');
            return Some(cgroup_root.join(rel));
        }
    }
    None
}

/// `<leaf>/cgroup.controllers` is a whitespace-delimited list of
/// controllers the parent has delegated into this subtree. Returns
/// `Ok(false)` when `memory` is absent so the orchestrator can fall
/// back gracefully. Read failures (the file should exist on every
/// cgroup-v2 dir) propagate as `Err(_)` — those are kernel /
/// mountpoint anomalies, not the gracefully-handled "no memory
/// controller" condition.
fn leaf_has_memory_controller(leaf: &Path) -> Result<bool, CgroupSetupError> {
    let content =
        std::fs::read_to_string(leaf.join("cgroup.controllers")).map_err(CgroupSetupError::Io)?;
    Ok(content.split_whitespace().any(|c| c == "memory"))
}

/// Probe `<leaf>/cgroup.subtree_control` writability without modifying
/// it. Opens with `OpenOptions::write(true)` and immediately drops the
/// handle — the open itself fails with `PermissionDenied` or
/// `ReadOnlyFilesystem` when the cgroup tree is not delegated. Any
/// other `ErrorKind` (e.g. `NotFound`) also returns `false` so the
/// orchestrator falls back gracefully — a missing
/// `subtree_control` on a real cgroup-v2 leaf would be a kernel bug,
/// but the safer behaviour is "treat as not writable, proceed
/// without the nested subgroup".
fn leaf_subtree_writable(leaf: &Path) -> bool {
    std::fs::OpenOptions::new()
        .write(true)
        .open(leaf.join("cgroup.subtree_control"))
        .is_ok()
}
