//! Observer-side IN-PROCESS executor for an assigned `TaskKind::Setup`
//! task — the twin of [`crate::secondary::setup_exec`].
//!
//! ## Why an observer twin
//! The framework's own auto-staging makes the SUBMITTER the setup-task
//! affinity member; after a bootstrap relocation the submitter runs as a
//! standalone OBSERVER (its primary role moved to a compute peer). So a
//! setup task whose affinity is the relocated submitter is assigned to the
//! OBSERVER, which must run it in-process and report the terminal — exactly
//! like a secondary executor. An observer is poolless by construction, which
//! is fine: a setup task never touches a worker pool.
//!
//! ## Module boundary (CLAUDE.md design-first)
//!   * Owner: the observer. The seam it crosses is the EXISTING inbound
//!     handler (`observer::coordinator::on_inbound`), which calls
//!     [`ObserverCoordinator::execute_setup_assignment`] for a
//!     `SetupAssignment` frame — a one-line delegate; no `if kind == Setup`
//!     scattered in the handler.
//!   * Execution body: the role-agnostic [`crate::setup_exec`] core, shared
//!     verbatim with the secondary twin (read task, run action, classify).
//!     This wrapper adds ONLY the observer-specific parts: resolving the
//!     `TaskInfo` from the observer's replicated `cluster_state` and
//!     reporting the terminal to the primary via the observer's
//!     `Destination::Primary` egress.
//!   * What callers see: a one-line `execute_setup_assignment(task_hash)`
//!     call yielding the report's send result.

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{Destination, DistributedMessage, timestamp_now};

use crate::observer::coordinator::ObserverCoordinator;
use crate::setup_exec::{SetupOutcome, execute_setup_with_upload};

impl<I: Identifier> ObserverCoordinator<I> {
    /// Execute an assigned setup task IN-PROCESS on the observer and report
    /// its terminal to the primary.
    ///
    /// Mirrors [`crate::secondary::SecondaryCoordinator::execute_setup_assignment`]:
    /// resolve the `TaskInfo` from the local CRDT mirror by hash, run the
    /// action via the shared executor core, and report a
    /// `DistributedMessage::SetupTerminal` to the primary (the authority
    /// originates the CRDT terminal — an observer holds zero authority). A
    /// hash absent from the local ledger is reported as a non-recoverable
    /// FAILURE so the primary settles it rather than leaving it in flight.
    pub(crate) async fn execute_setup_assignment(&mut self, task_hash: String) {
        // Resolve + CLONE the task out of the ledger so the `cluster_state`
        // borrow ends before the (async) upload path runs against
        // `self.upload_action()`.
        let task = self
            .cluster_state()
            .task_state(&task_hash)
            .map(|state| state.to_task_info());
        let outcome = match task {
            // The shared executor path (#336 P1): an upload-ref task uploads
            // via the registered action; a no-ref task keeps the #489 no-op.
            Some(task) => execute_setup_with_upload(&task, self.upload_action()).await,
            None => {
                tracing::warn!(
                    target: "dynrunner_setup",
                    task_hash = %task_hash,
                    "setup assignment for a task absent from the observer's ledger; \
                     reporting non-recoverable failure"
                );
                SetupOutcome::Failure(
                    "setup task not present in the executor's replicated ledger \
                     (assignment outran TaskAdded, or a concurrent removal)"
                        .to_string(),
                )
            }
        };
        let (success, error_message) = match outcome {
            SetupOutcome::Success => (true, String::new()),
            SetupOutcome::Failure(reason) => (false, reason),
        };
        let report = DistributedMessage::SetupTerminal {
            target: None,
            sender_id: self.node_id().to_string(),
            timestamp: timestamp_now(),
            secondary_id: self.node_id().to_string(),
            task_hash,
            success,
            error_message,
        };
        if let Err(e) = self.send_to(Destination::Primary, report).await {
            tracing::warn!(
                target: "dynrunner_setup",
                error = %e,
                "failed to report setup terminal to the primary; the primary's \
                 per-task deadline / death seam is the backstop"
            );
        }
    }
}
