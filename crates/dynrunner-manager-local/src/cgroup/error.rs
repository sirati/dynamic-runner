//! Error shape for the nested-cgroup setup flow.
//!
//! Single concern: discriminator types the caller can branch on.
//! [`super::setup_worker_cgroup`] itself is INFALLIBLE — every
//! internal failure degrades to the flat layout (`None`) with a
//! `tracing::warn!` line — so the variants below surface only through
//! the per-worker leaf factory ([`super::prepare_worker_subgroup`])
//! and the module-internal flow that feeds the orchestrator's
//! degrade-on-error policy. Because that warn line is the ONLY
//! diagnostic an operator gets, the `Io` variant carries the
//! operation and path that failed alongside the raw `io::Error`.
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
/// reason directly. [`super::setup_worker_cgroup`] never propagates
/// these — it degrades every failure to `None` + one warn line —
/// so the public surface they escape through is
/// [`super::prepare_worker_subgroup`].
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

    /// An I/O error reading or writing the cgroup tree. Carries the
    /// operation verb and the path it targeted because the degrade
    /// warn line in [`super::setup_worker_cgroup`] is the only place
    /// the failure surfaces — without them an errno like
    /// `EOPNOTSUPP` gives the operator nothing to act on.
    #[error("cgroup I/O error: {op} {path}: {source}")]
    Io {
        /// Short operation verb ("read", "write", "mkdir") naming
        /// what was attempted on `path`.
        op: &'static str,
        /// The file or directory the operation targeted.
        path: std::path::PathBuf,
        /// The raw OS error the operation returned.
        source: std::io::Error,
    },
}

impl CgroupSetupError {
    /// Constructor adaptor for `map_err`: capture the operation verb
    /// and target path up front, absorb the `io::Error` when (if) it
    /// happens. Keeps the ~dozen writer call sites at one line each:
    /// `.map_err(CgroupSetupError::io_at("write", &path))`.
    pub(super) fn io_at(
        op: &'static str,
        path: impl AsRef<std::path::Path>,
    ) -> impl FnOnce(std::io::Error) -> Self {
        let path = path.as_ref().to_path_buf();
        move |source| Self::Io { op, path, source }
    }
}
