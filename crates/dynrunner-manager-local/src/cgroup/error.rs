//! Error shape for the nested-cgroup setup flow.
//!
//! Single concern: discriminator types the caller can branch on. The
//! happy path returns `Ok(Some(handle))`; recoverable degradations
//! (cgroup v1 host, no memory controller, leaf not writable) return
//! `Ok(None)` from [`super::setup_worker_cgroup`] WITH a `tracing::warn!`
//! line emitted there. The error variants below cover unexpected I/O
//! failures only — corrupted `/proc/self/cgroup` shape, transient
//! sysfs read failures, etc. — so a caller that wants to discriminate
//! "fallback to flat layout" from "something is structurally wrong"
//! can match on `Err(CgroupSetupError::Io(_))` vs `Ok(None)`.
//!
//! `NotCgroupV2`, `NoMemoryController`, and `NotWritable` are
//! preserved as enum variants (rather than collapsed into "any
//! `Ok(None)` reason") because individual call sites may want a
//! programmatic signal in the future (e.g. structured metrics with a
//! `reason` field) without re-parsing the warn-line text. They are
//! NOT currently returned by the setup function — that function maps
//! them to `Ok(None)` + warn — but the surface stays available for a
//! caller that constructs the error explicitly (tests do this to
//! verify Display).

use thiserror::Error;

/// Reasons the nested-cgroup setup could not complete.
///
/// Constructed only when the caller wants to surface a structured
/// reason directly. [`super::setup_worker_cgroup`] uses
/// `tracing::warn!` + `Ok(None)` for the three "graceful degrade"
/// variants and reserves the `Err(_)` path for `Io` only.
#[derive(Debug, Error)]
pub enum CgroupSetupError {
    /// `/proc/self/cgroup` was readable but did not contain a v2
    /// (`0::`) line. Either a v1-only host or a non-Linux platform.
    #[error("not running under cgroup v2 (no 0:: line in /proc/self/cgroup)")]
    NotCgroupV2,

    /// The leaf cgroup directory exists but `cgroup.controllers`
    /// does not enumerate `memory` — the parent has not delegated
    /// the memory controller into our subtree. Without it, writing
    /// `memory.max` on a child cgroup would fail.
    #[error("cgroup v2 leaf {leaf} does not expose the memory controller")]
    NoMemoryController {
        /// The leaf path that lacked `memory`. Carried so the
        /// caller can put it in a structured log line.
        leaf: std::path::PathBuf,
    },

    /// `cgroup.subtree_control` rejected `O_WRONLY` open with
    /// `EACCES` / `EROFS`. Typical in non-delegated rootless setups
    /// where the user does not own the cgroup tree.
    #[error("cgroup v2 leaf {leaf} is not writable (subtree_control)")]
    NotWritable {
        /// The leaf path the writability probe targeted. Carried for
        /// the same reason as `NoMemoryController`.
        leaf: std::path::PathBuf,
    },

    /// An unexpected I/O error reading or writing the cgroup tree.
    /// Surfaces a corrupted `/proc` view, a partially-mounted
    /// `/sys/fs/cgroup`, or transient kernel failures. Not used for
    /// the three "fallback" variants above.
    #[error("cgroup I/O error: {0}")]
    Io(#[from] std::io::Error),
}

impl CgroupSetupError {
    /// Classify whether this failure is the PERMISSION/DELEGATION
    /// class: the kernel (or VFS) refused a cgroup write because the
    /// tree is not delegated to the runtime user. This is the
    /// condition an operator hits on a plain desktop session without
    /// `Delegate=yes` — the writability PROBE can pass (the leaf's
    /// `subtree_control` file is user-owned under `user@.service`
    /// delegation) while a later `mkdir` / `cgroup.procs` migration /
    /// controller write is still refused with `EACCES`/`EPERM`, or
    /// the whole mount is read-only (`EROFS`).
    ///
    /// SINGLE classification owner: [`super::setup_worker_cgroup`]
    /// consults this predicate to map the class onto the same
    /// graceful `Ok(None)` flat-cgroup degradation the probe-stage
    /// conditions take. Callers never re-classify.
    ///
    /// `Io` kinds: `PermissionDenied` covers both `EACCES` and
    /// `EPERM` (std maps both to that kind); `ReadOnlyFilesystem` is
    /// `EROFS`. `NotWritable` is the probe-stage spelling of the same
    /// condition, included for coherence should a caller construct it
    /// directly. `NotCgroupV2` / `NoMemoryController` are environment
    /// shape, not permission, and genuine I/O anomalies (corrupted
    /// `/proc`, `ENOENT` on kernel pseudo-files) stay outside the
    /// class so they remain fatal.
    pub fn is_permission_class(&self) -> bool {
        match self {
            CgroupSetupError::Io(e) => matches!(
                e.kind(),
                std::io::ErrorKind::PermissionDenied | std::io::ErrorKind::ReadOnlyFilesystem
            ),
            CgroupSetupError::NotWritable { .. } => true,
            CgroupSetupError::NotCgroupV2 | CgroupSetupError::NoMemoryController { .. } => false,
        }
    }
}
