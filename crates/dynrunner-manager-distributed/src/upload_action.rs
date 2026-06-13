//! The setup-task UPLOAD-ACTION port (#336 P1).
//!
//! ## The one concern
//! ONE seam: "given a setup task that carries an [`UploadFileRef`], upload
//! the file to the cluster". The role-agnostic executor core
//! ([`crate::setup_exec`]) crosses this seam when (and only when) a setup
//! task's action IS an upload; a setup task WITHOUT a ref keeps the #489
//! no-op-success behaviour (the pre-staged / mode-2 gate). The CONCRETE
//! transfer (gateway scp / layered upload / the per-blob #400 retry) is
//! owned ENTIRELY by the provider layer — a registered Rust→Python upload
//! callback bridged at the pyo3 boundary — exactly mirroring the
//! [`crate::observer::reconnect::TunnelReconnector`] /
//! [`crate::primary::respawn::SecondarySpawner`] boundary: the trait lives
//! here in `manager-distributed`, the provider binding lives outside
//! (`dynrunner-pyo3` → Python `job_manager`), and the role layer names
//! neither scp nor the gateway.
//!
//! ## Why a port (not a closure on `run_setup_action`)
//! `run_setup_action` is a SYNCHRONOUS pure function with no access to
//! coordinator state — it is the right shape for the no-op primitive but
//! cannot drive an upload (the upload is async + needs a registered
//! provider handle). So the upload is a separate, ASYNC port the coordinator
//! HOLDS as `Option<Arc<dyn UploadAction>>` and the executor core invokes
//! when a ref is present. A coordinator with no registered action treats an
//! upload-ref task as a permanent failure (it was asked to upload but has no
//! uploader) — distinct from the no-op-success a no-ref task gets.
//!
//! ## Retry split (#400 / owner decision 2026-06-14)
//! The bounded TRANSIENT retry lives in the provider (the Python
//! `retry_transient` helper the bulk-walk already uses) — it is NOT
//! re-implemented here. The Rust side only CLASSIFIES the provider's final
//! outcome: [`UploadError::Permanent`] → a non-recoverable setup terminal,
//! [`UploadError::Transient`] → the executor core's bounded OUTER retry
//! (kept minimal, for a whole-action transient the provider could not absorb
//! — e.g. a path re-established mid-relocation) before falling to a
//! permanent failure. Keeping the `Transient` variant on the port makes the
//! P4 early-start / preemptible-uploader work additive.

use std::sync::Arc;

use dynrunner_core::UploadFileRef;

/// The classified failure of an upload attempt the provider could not
/// complete. The provider (Python callback) owns the per-blob TRANSIENT
/// retry; this enum is how the FINAL outcome is reported back to the
/// Rust executor core for terminal classification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UploadError {
    /// The transfer failed in a way that MIGHT succeed on a later attempt
    /// (the provider exhausted its own per-blob retry, or the whole action
    /// hit a transient transport fault). The executor core may re-attempt a
    /// bounded number of times before giving up. The string is the
    /// operator-facing reason.
    Transient(String),
    /// The transfer failed in a way that will NOT succeed on retry (source
    /// missing, out-of-tree with no destination, a programming error). The
    /// executor core maps this straight to a non-recoverable setup terminal
    /// — no outer retry. The string is the operator-facing reason.
    Permanent(String),
}

impl UploadError {
    /// The operator-facing reason, regardless of class.
    pub fn reason(&self) -> &str {
        match self {
            UploadError::Transient(r) | UploadError::Permanent(r) => r,
        }
    }

    /// Whether a bounded outer retry is worthwhile for this failure.
    pub fn is_transient(&self) -> bool {
        matches!(self, UploadError::Transient(_))
    }
}

/// Port the setup-task executor crosses to upload one file to the cluster.
///
/// `#[async_trait(?Send)]` for the SAME provider physics that forces the
/// `?Send` bound on [`crate::observer::reconnect::TunnelReconnector`]: the
/// production binding drives ssh/scp subprocess work whose future is not
/// `Send`, and every role that hosts an in-process setup executor (primary
/// self-exec, secondary, observer twin) runs `LocalSet`-bound. The trait
/// object stays `Send + Sync` so an `Arc<dyn UploadAction>` is moveable and
/// can be carried across a relocation handoff onto the observer tail.
///
/// Single concern: "upload THIS file". The implementation owns HOW (resolve
/// the source, place it under the gateway srcbins dir or at the explicit
/// `dest`, retry transient faults); the executor core owns WHEN (a setup
/// task carrying a ref reaches its in-process executor) and reports the
/// terminal.
#[async_trait::async_trait(?Send)]
pub trait UploadAction: Send + Sync {
    /// Upload the file named by `file` to the cluster. `Ok(())` ⇒ the file
    /// is on the cluster (the executor originates `SetupCompleted`);
    /// `Err(UploadError)` ⇒ the transfer failed, classified
    /// transient-vs-permanent for the executor's retry/terminal decision.
    async fn upload(&self, file: &UploadFileRef) -> Result<(), UploadError>;
}

/// A registered upload-action handle a coordinator holds. `None` on a
/// coordinator that was never given an uploader (no setup task it hosts
/// carries an upload ref — e.g. a pure-compute secondary); `Some` on the
/// source-owning member (the submitter / observer) whose setup tasks DO
/// upload. Mirrors [`crate::observer::reconnect::ReconnectorHandle`].
pub type UploadActionHandle = Option<Arc<dyn UploadAction>>;

/// Bounded OUTER-retry cap for a whole-action transient the provider could
/// not absorb. Small on purpose: the provider's `retry_transient` already
/// owns the per-blob retry, so this is only the rare whole-action transient
/// (e.g. the transport path re-established mid-relocation). Total attempts
/// = this + 1.
pub(crate) const UPLOAD_OUTER_RETRIES: u32 = 2;
