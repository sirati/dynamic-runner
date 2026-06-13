//! Secondary-side IN-PROCESS executor for an assigned `TaskKind::Setup`
//! task.
//!
//! ## The one concern
//! Run an off-primary setup-task ASSIGNMENT in-process (zero-worker) on this
//! secondary and report its TERMINAL back to the primary. The secondary
//! holds NO authority — it does NOT originate the CRDT terminal; it reports
//! the outcome to the primary (exactly as it reports a worker terminal), and
//! the primary originates the authoritative `SetupCompleted` /
//! `TaskFailed { NonRecoverable }` mutation.
//!
//! ## Module boundary (CLAUDE.md design-first)
//!   * Owner: the secondary. The seam it crosses is the EXISTING inbound
//!     router (`secondary::dispatch::router::dispatch_message`), which calls
//!     [`Self::execute_setup_assignment`] for a `SetupAssignment` frame — a
//!     one-line delegate; no `if kind == Setup` in the router.
//!   * Execution body: the role-agnostic [`crate::setup_exec`] core (read
//!     the task, run the action, classify the outcome). This wrapper adds
//!     ONLY the secondary-specific parts: resolving the `TaskInfo` from the
//!     local replicated `cluster_state`, and reporting the terminal to the
//!     primary.
//!   * What callers see: a one-line `execute_setup_assignment(task_hash)`
//!     call. The router never learns how the task is run or reported.
//!
//! Reuses the zero-worker `Operational` state (a late-joiner / observer
//! already runs poolless): a setup task never touches the worker pool, so
//! the executor runs regardless of how many workers this node has.

use dynrunner_core::Identifier;
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::DistributedMessage;
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use super::SecondaryCoordinator;
use super::wire::timestamp_now;
use crate::setup_exec::{SetupOutcome, execute_setup, run_setup_action};

impl<M, S, E, I> SecondaryCoordinator<M, S, E, I>
where
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    /// Execute an assigned setup task IN-PROCESS and report its terminal to
    /// the primary.
    ///
    /// Resolves the `TaskInfo` from this node's replicated `cluster_state`
    /// (the assignment carries only the hash — a prior `TaskAdded` broadcast
    /// seeded the task on every replica), runs the action synchronously via
    /// the shared executor core, and reports the outcome as a
    /// `DistributedMessage::SetupTerminal` to the primary role. A
    /// hash this node does not know (a racing removal, or an assignment that
    /// outran the `TaskAdded`) is reported as a FAILURE so the primary
    /// settles it non-recoverably rather than leaving it stranded in flight.
    pub(in crate::secondary) async fn execute_setup_assignment(
        &mut self,
        task_hash: String,
    ) -> Result<(), String> {
        // Resolve the task from the local CRDT mirror by hash. The
        // `task_state` accessor returns the live `TaskState` whose `.task()`
        // is the `TaskInfo` we execute against.
        let outcome = match self.cluster_state.task_state(&task_hash) {
            Some(state) => {
                let task = state.task().clone();
                execute_setup(&task, run_setup_action)
            }
            None => {
                tracing::warn!(
                    task_hash = %task_hash,
                    "setup assignment for a task absent from the local ledger; \
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
            sender_id: self.config.secondary_id.clone(),
            timestamp: timestamp_now(),
            secondary_id: self.config.secondary_id.clone(),
            task_hash,
            success,
            error_message,
        };
        // Report to the primary role only — the authority owns the CRDT
        // origination + mesh propagation. A reporting secondary that also
        // originated would be a second CRDT writer (the work-split law).
        self.send_to_primary(report).await
    }
}
