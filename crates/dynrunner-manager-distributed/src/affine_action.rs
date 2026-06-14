//! The SecondaryAffine IMPORT-ACTION port (#497 P4).
//!
//! ## The one concern
//! ONE seam: "given a [`TaskKind::SecondaryAffine`](dynrunner_core::TaskKind)
//! task, run its per-secondary IMPORT once on THIS node". The secondary-local
//! run-once executor ([`crate::secondary::affine_exec`]) crosses this seam at
//! MOST ONCE per (node, affine task): the executor owns the run-once /
//! concurrency-gating bookkeeping; this port owns ONLY the concrete import
//! work (the consumer's `nix-store --import`, a toolchain build, a cache
//! prime). The CONCRETE import is owned ENTIRELY by the provider layer — a
//! registered Rust→Python import callback bridged at the pyo3 boundary —
//! exactly mirroring the [`crate::upload_action::UploadAction`] boundary
//! (#336 P1): the trait lives here in `manager-distributed`, the provider
//! binding lives outside (`dynrunner-pyo3` → Python `job_manager`), and the
//! role layer names neither `nix-store` nor the import command.
//!
//! ## Why a port (mirroring the upload action)
//! The import is async + needs a registered provider handle + has no place in
//! the synchronous setup primitive. So it is a separate, ASYNC port the
//! secondary coordinator HOLDS as `Option<Arc<dyn ImportAction<I>>>` and the
//! executor core invokes when a work task gates on a not-yet-locally-imported
//! SecondaryAffine dependency. A secondary with no registered action treats a
//! gating import as a permanent (non-recoverable) failure — it was asked to
//! import but has no importer.
//!
//! ## Why the trait is generic over `I` (not the method)
//! The action runs against a [`TaskInfo<I>`], which is generic over the
//! cluster [`Identifier`]. A generic METHOD would make the trait
//! object-UNSAFE (no `Arc<dyn …>`), so the generic rides the TRAIT (`trait
//! ImportAction<I>`) and the method stays non-generic — keeping the handle a
//! `dyn` object. The secondary is monomorphized at `SecondaryCoordinator<M, S,
//! E, I>`, so `I` is already fixed at the holding site; the upload action
//! avoids this only because its `UploadFileRef` argument is `I`-free.
//!
//! ## Failure classes (#495)
//! The import maps onto the cluster's three #495 failure classes, NOT the
//! upload's transient/permanent pair, because a failed import has the SAME
//! re-route options a failed work task does: a `Recoverable` import (e.g. a
//! transient NFS read fault the provider could not absorb, a node-local
//! resource pinch) lets the queued work task be RE-ROUTED to another
//! secondary that runs its OWN import; a `NonRecoverable` import (the source
//! is structurally un-importable) cascades the dependents non-recoverably; a
//! `Transient` import is re-attempted by the executor's bounded OUTER retry
//! before falling through to a `Recoverable` work-task failure. A failed
//! import NEVER marks the affine task locally-done (so a later assignment /
//! another secondary retries its own import — the done set is never poisoned).

use std::sync::Arc;

use dynrunner_core::{Identifier, TaskInfo};

/// The classified failure of an import attempt the provider could not
/// complete. The provider (Python callback) owns any per-step retry it can
/// absorb; this enum is how the FINAL outcome is reported back to the Rust
/// executor core for re-route classification, mapping onto the cluster's
/// three #495 failure classes (see the module docs for WHY the import uses
/// the #495 classes rather than the upload's transient/permanent pair).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImportError {
    /// The import failed in a way that MIGHT succeed on a later attempt ON
    /// THIS SAME NODE (the provider exhausted its own retry, or the whole
    /// action hit a transient fault). The executor core may re-attempt a
    /// bounded number of times before giving up. The string is the
    /// operator-facing reason.
    Transient(String),
    /// The import failed in a way that is unlikely to succeed again on THIS
    /// node but MIGHT on another (a node-local resource pinch, a path
    /// re-established mid-relocation elsewhere). The queued work task is
    /// failed as `ErrorType::Recoverable` so the authority can RE-ROUTE it to
    /// a secondary that runs its OWN import (#495). The string is the
    /// operator-facing reason.
    Recoverable(String),
    /// The import failed in a way that will NOT succeed anywhere (the source
    /// is structurally un-importable, a programming error). The queued work
    /// task is failed as `ErrorType::NonRecoverable` and its dependents
    /// cascade. The string is the operator-facing reason.
    NonRecoverable(String),
}

impl ImportError {
    /// The operator-facing reason, regardless of class.
    pub fn reason(&self) -> &str {
        match self {
            ImportError::Transient(r)
            | ImportError::Recoverable(r)
            | ImportError::NonRecoverable(r) => r,
        }
    }

    /// Whether a bounded outer retry ON THIS NODE is worthwhile for this
    /// failure (the `Transient` class only).
    pub fn is_transient(&self) -> bool {
        matches!(self, ImportError::Transient(_))
    }

    /// The cluster failure class the queued work task is failed with when the
    /// import gives up (`Transient` exhaustion folds into `Recoverable`: the
    /// import could not complete on THIS node, so re-route per #495). Used by
    /// the executor core to stamp the per-dependent `TaskFailed` terminal.
    pub fn error_type(&self) -> dynrunner_core::ErrorType {
        match self {
            // A transient the outer retry could not absorb is re-routable:
            // another node may import cleanly.
            ImportError::Transient(_) | ImportError::Recoverable(_) => {
                dynrunner_core::ErrorType::Recoverable
            }
            ImportError::NonRecoverable(_) => dynrunner_core::ErrorType::NonRecoverable,
        }
    }
}

/// Port the secondary-local affine executor crosses to run one SecondaryAffine
/// import on this node.
///
/// `#[async_trait(?Send)]` for the SAME provider physics that forces the
/// `?Send` bound on [`crate::upload_action::UploadAction`]: the production
/// binding drives subprocess work (e.g. `nix-store --import`) whose future is
/// not `Send`, and the secondary that hosts the in-process import runs
/// `LocalSet`-bound. The trait object stays `Send + Sync` so an
/// `Arc<dyn ImportAction<I>>` is moveable (carried across a relocation handoff
/// onto the observer tail).
///
/// Single concern: "import THIS task once". The implementation owns HOW
/// (resolve the payload, run the import command, retry any fault it can
/// absorb); the executor core owns WHEN (exactly once per (node, affine task),
/// gating all that node's dependent work tasks) and reports the queued
/// dependents' release / failure.
#[async_trait::async_trait(?Send)]
pub trait ImportAction<I: Identifier>: Send + Sync {
    /// Run the SecondaryAffine import named by `task` on THIS node. `Ok(())`
    /// ⇒ the import is locally done (the executor releases the queued
    /// dependent work tasks → `InFlight`); `Err(ImportError)` ⇒ the import
    /// failed, classified into the #495 failure classes for the executor's
    /// retry / re-route decision. The affine task is NEVER marked locally-done
    /// on `Err` (the done set is never poisoned).
    async fn import(&self, task: &TaskInfo<I>) -> Result<(), ImportError>;
}

/// A registered import-action handle a secondary holds. `None` on a secondary
/// that was never given an importer (no work task it runs gates on a
/// SecondaryAffine import); `Some` on a compute secondary whose dependent work
/// tasks DO require the per-secondary import. Mirrors
/// [`crate::upload_action::UploadActionHandle`].
pub type ImportActionHandle<I> = Option<Arc<dyn ImportAction<I>>>;

/// Bounded OUTER-retry cap for a whole-action transient the provider could not
/// absorb. Small on purpose (mirrors
/// [`crate::upload_action::UPLOAD_OUTER_RETRIES`]): the provider owns any
/// per-step retry, so this is only the rare whole-action transient. Total
/// attempts = this + 1.
pub(crate) const IMPORT_OUTER_RETRIES: u32 = 2;
