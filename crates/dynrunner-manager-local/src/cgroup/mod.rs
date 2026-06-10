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
//! * [`NestedCgroupHandle`] — opaque holder of the
//!   workers/ path. Carried on the worker pool; cloned into each
//!   `WorkerFactory` so the spawn site can reach per-worker leaves
//!   without re-traversing the cgroup tree.
//! * [`prepare_worker_subgroup`] — per-worker leaf factory. Called
//!   once per OS worker process at spawn time; returns a
//!   [`SubcgroupHandle`] whose `Drop` best-effort `rmdir`s the leaf.
//! * [`SubcgroupHandle::procs_path`] — owned `PathBuf` for the leaf's
//!   `cgroup.procs`. The spawn site clones it into a `pre_exec` closure
//!   and writes the formatted pid (stack-allocated digits) into it
//!   post-fork. Replaces the previous top-level `attach_pid`
//!   primitive (deleted): the cgroup-v2 "no internal processes" rule
//!   forbids pids in `<workers>/cgroup.procs` once subtree_control is
//!   enabled on it. [`SubcgroupHandle::attach_pid`] is the equivalent
//!   parent-side convenience (not async-signal-safe).
//!
//! Graceful fallback contract: any of (a) not cgroup-v2, (b) no
//! `memory` controller exposed on the leaf, (c) leaf not writable,
//! (d) a permission/delegation refusal (`EACCES`/`EPERM`/`EROFS`)
//! surfacing from the subgroup writes themselves (the probe in (c)
//! can pass on a plain desktop session — the leaf's files are
//! user-owned under `user@.service` — while the kernel still refuses
//! the later mkdir / migration / controller writes without
//! `Delegate=yes`) returns `Ok(None)` plus a single-line
//! `tracing::warn!` so an operator running outside a delegated
//! cgroup environment (host development, CI, non-Linux) sees one log
//! line and the flat (pre-nested) layout proceeds unchanged.
//! Classification for (d) lives on
//! [`CgroupSetupError::is_permission_class`] — ONE owner; callers
//! never re-classify. `Err(_)` is reserved for genuinely unexpected
//! I/O failures (corrupted `/proc`, transient sysfs read errors);
//! the caller treats it as fatal.

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
/// per-worker subgroup factory call so the spawn site can reach the
/// per-worker leaves without a fresh cgroup-tree walk.
#[derive(Debug, Clone)]
pub struct NestedCgroupHandle {
    /// Absolute path to the `<leaf>/workers/` directory the setup
    /// flow materialised. Consumed by [`prepare_worker_subgroup`]
    /// (which `mkdir`s a `worker-<id>/` leaf beneath it) and
    /// exposed via [`Self::workers_path`] for callers that need the
    /// directory itself (e.g. teardown).
    workers_path: PathBuf,
}

impl NestedCgroupHandle {
    /// The absolute `<leaf>/workers/` path the setup flow created.
    /// Production callers shouldn't need this — they use
    /// [`prepare_worker_subgroup`] — but it's surfaced for diagnostic
    /// logging and tests that assert on the materialised layout.
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
/// Returns `Ok(Some(handle))` on success, `Ok(None)` on the
/// graceful-fallback conditions (each accompanied by a single
/// `tracing::warn!` line), and `Err(_)` on truly unexpected I/O
/// errors.
///
/// On top of [`try_setup_worker_cgroup`]'s probe-stage fallbacks,
/// this wrapper owns the permission/delegation classification of the
/// write phase: an `EACCES`/`EPERM`/`EROFS` escaping the flow (see
/// [`CgroupSetupError::is_permission_class`]) degrades to `Ok(None)`
/// plus one warn line instead of propagating — the identical condition the
/// SLURM-wrapper path already survives via the probe, hit later on a
/// plain desktop session where the probe passes but the kernel
/// refuses the writes without `Delegate=yes`.
pub fn setup_worker_cgroup(
    cgroup_root: &Path,
    reserved_bytes: u64,
) -> Result<Option<NestedCgroupHandle>, CgroupSetupError> {
    match try_setup_worker_cgroup(cgroup_root, reserved_bytes) {
        Err(e) if e.is_permission_class() => {
            tracing::warn!(
                error = %e,
                "cgroup-v2 workers subgroup writes were refused (permission/delegation); workers will share the flat cgroup. \
                 Operator hint: the cgroup tree must be delegated to the runtime user — \
                 run under a delegated scope (`systemd-run --user --scope -p Delegate=yes ...`), \
                 or rootless podman with `Delegate=yes` on the user@.service."
            );
            Ok(None)
        }
        other => other,
    }
}

/// Fallible setup flow behind [`setup_worker_cgroup`]'s
/// permission-classification wrapper. Probe-stage degradations
/// return `Ok(None)` directly; every other failure propagates as
/// `Err(_)` UNCLASSIFIED — the public wrapper is the single place
/// that decides which `Err` shapes degrade.
fn try_setup_worker_cgroup(
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

/// Handle to a per-worker sub-cgroup `<workers>/worker-<id>/`.
/// Constructed by [`prepare_worker_subgroup`]; carried on the worker
/// pool's `WorkerHandle` for the worker's lifetime so the memprofile
/// sampler can read `memory.current` without re-walking the cgroup
/// tree, and so [`Self::procs_path`] can hand the spawn site a
/// pre-computed `cgroup.procs` path to clone into its `pre_exec`
/// closure.
///
/// On `Drop`: best-effort `rmdir` of the leaf. Empty leaf (worker
/// exited cleanly, kernel reaped the `cgroup.procs` entry) →
/// succeeds. Non-empty (process still attached, e.g. zombie or hard
/// shutdown) → swallowed with one warn line. Already-gone (concurrent
/// teardown or kernel auto-removal) → silent.
///
/// Single concern: own one per-worker leaf's lifetime. Knows nothing
/// about the parent `workers/` setup, controller probing, or
/// `memory.max` math — those live entirely in the orchestrator path
/// ([`setup_worker_cgroup`] + [`writer::write_workers_subgroup`]).
#[derive(Debug)]
pub struct SubcgroupHandle {
    /// Absolute path to the `<workers>/worker-<id>/` directory the
    /// per-worker setup materialised. Carried so `procs_path` can
    /// hand the spawn site a pre-joined path without a re-walk and
    /// so `Drop` knows where to `rmdir`.
    cgroup_dir: PathBuf,
}

impl SubcgroupHandle {
    /// The absolute `<workers>/worker-<id>/` path. Surfaced for the
    /// memprofile sampler (which reads `memory.current`,
    /// `memory.swap.current`, `memory.stat` underneath this dir) and
    /// for diagnostic logging.
    pub fn cgroup_dir(&self) -> &Path {
        &self.cgroup_dir
    }

    /// Pre-computed `<cgroup_dir>/cgroup.procs` path. Built once in
    /// the parent so a `pre_exec` spawn-site closure can clone an
    /// owned `PathBuf` into its `'static` environment and then issue
    /// a single `std::fs::write(&procs, &digits[..n])` post-fork
    /// without further path manipulation. Returns an owned value
    /// (not a borrow) precisely so the move-into-closure pattern
    /// works without lifetime gymnastics.
    pub fn procs_path(&self) -> PathBuf {
        self.cgroup_dir.join("cgroup.procs")
    }

    /// Parent-side convenience: format `pid` and write it to the
    /// leaf's `cgroup.procs`. Equivalent to
    /// `std::fs::write(self.procs_path(), pid.to_string())`.
    ///
    /// **Do not call from `pre_exec`.** Both `pid.to_string()` and
    /// the inner `PathBuf` join allocate post-fork; that is fine for
    /// parent-side use (tests, diagnostic harness) but a pre_exec
    /// closure must use [`Self::procs_path`] from the parent, format
    /// the pid into a stack buffer post-fork, and call `std::fs::write`
    /// directly — see `subprocess_factory.rs` for the gold-standard
    /// pattern.
    pub fn attach_pid(&self, pid: u32) -> std::io::Result<()> {
        std::fs::write(self.procs_path(), pid.to_string())
    }

    /// Test seam mirroring
    /// [`NestedCgroupHandle::from_workers_path_for_test`]: callers
    /// (the memprofile manager-loop integration test, in particular)
    /// build a handle from an arbitrary tempdir-rooted directory so
    /// `cgroup_dir()` returns a path the sampler can read fake
    /// `memory.current` / `memory.stat` files from, without
    /// exercising the real cgroup-v2 setup.
    ///
    /// The handle's `Drop` will still attempt `remove_dir` on the
    /// given path; tests should pass a dir they're happy to see
    /// removed (or that the tempdir wrapper will clean up after a
    /// failed rmdir).
    #[doc(hidden)]
    pub fn from_cgroup_dir_for_test(cgroup_dir: PathBuf) -> Self {
        Self { cgroup_dir }
    }
}

impl Drop for SubcgroupHandle {
    /// Best-effort `rmdir` of the per-worker leaf. Three outcomes:
    ///
    ///   * Empty leaf (worker exited cleanly) → `rmdir` succeeds.
    ///   * Non-empty leaf (worker still attached: zombie, hard kill
    ///     before the kernel reaped `cgroup.procs`) → `ENOTEMPTY`,
    ///     swallowed with one warn line so an operator can find the
    ///     stale leaf manually if needed.
    ///   * Already gone (concurrent teardown, kernel auto-removal on
    ///     last-pid-exit) → `ENOENT`, silent.
    ///
    /// Other `ErrorKind`s (permission errors, transient sysfs
    /// failures) emit one warn line and proceed — `Drop` cannot
    /// propagate failures, but we surface them so they're not
    /// silently lost.
    fn drop(&mut self) {
        match std::fs::remove_dir(&self.cgroup_dir) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            // Real cgroup-v2 kernel returns EBUSY (ResourceBusy) when
            // a cgroup still contains processes; tmpfs (test fixtures)
            // returns ENOTEMPTY (DirectoryNotEmpty). Either way the
            // semantics are "leaf not empty, leave it for the operator".
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::DirectoryNotEmpty | std::io::ErrorKind::ResourceBusy
                ) =>
            {
                tracing::warn!(
                    cgroup_dir = %self.cgroup_dir.display(),
                    "subcgroup rmdir failed: leaf not empty (worker still attached?)"
                );
            }
            Err(e) => {
                tracing::warn!(
                    cgroup_dir = %self.cgroup_dir.display(),
                    error = %e,
                    "subcgroup rmdir failed"
                );
            }
        }
    }
}

/// Create `<workers>/worker-<id>/` and return a [`SubcgroupHandle`]
/// scoped to the worker's lifetime. Called by the pool spawn-site
/// once per OS worker process; the handle drops when the worker
/// exits, which attempts `rmdir` on the leaf.
///
/// `parent` is the existing handle pointing at `<workers>/`. The leaf
/// inherits `<workers>/memory.max` (no per-worker enforcement cap by
/// design — observability only). `memory.swap.max` is forced to
/// `"max"` on the leaf for the same load-bearing reason it's forced
/// on the parent (cgroup-v2 children default to zero-swap).
///
/// Idempotent: re-running with the same `worker_id` against an
/// already-materialised leaf is a no-op (`create_dir_all` re-entrant,
/// `memory.swap.max` write is overwrite).
pub fn prepare_worker_subgroup(
    parent: &NestedCgroupHandle,
    worker_id: u32,
) -> Result<SubcgroupHandle, CgroupSetupError> {
    let cgroup_dir = writer::write_worker_subgroup(parent.workers_path(), worker_id)?;
    Ok(SubcgroupHandle { cgroup_dir })
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
