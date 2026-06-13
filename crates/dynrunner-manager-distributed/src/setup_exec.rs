//! In-process, zero-worker EXECUTION of a `TaskKind::Setup` task.
//!
//! ## The one concern
//! Turn a setup-task ASSIGNMENT into a setup-task TERMINAL, IN-PROCESS, on
//! the task's executor-affinity member — never on a worker subprocess and
//! never through the worker pool. A setup task is invisible to every
//! worker-dispatch path (the scheduling seam); its execution is THIS
//! module, run synchronously on whichever coordinator hosts the affinity
//! member (primary self-exec, or an off-primary secondary / observer).
//!
//! ## Why a shared core (not a per-role body)
//! The execute-then-classify step is identical regardless of which
//! coordinator hosts it; only the TERMINAL ROUTING differs (the primary
//! originates the CRDT mutation directly; an off-primary member reports the
//! terminal to the primary like a worker terminal). So the role-agnostic
//! core lives here as a pure function over the task + an action seam, and
//! each coordinator wraps it with its own terminal sink. This keeps the
//! executor concern self-contained — no `if kind == Setup` scattered
//! through the routers, and no triplicated body across
//! secondary / observer / primary.
//!
//! ## API surface crossing the boundary
//!   * [`run_setup_action`] — the EXECUTION SEAM. For the setup-task
//!     PRIMITIVE (P2) the action is a no-op success: the primitive's job is
//!     to drive the assignment→exec→terminal lifecycle, not to define what a
//!     setup task DOES. The framework's own staging-via-setup-tasks (a later
//!     phase) and consumer setup tasks plug their real action in here; the
//!     seam takes the task by reference and returns `Result<(), String>`
//!     (`Ok` ⇒ success terminal, `Err(reason)` ⇒ non-recoverable failure
//!     terminal). Synchronous: a setup task runs to completion inside the
//!     coordinator loop iteration that received its assignment.
//!   * [`execute_setup`] — runs an action and classifies its result into a
//!     [`SetupOutcome`]. The callers (the role wrappers) see ONLY this: hand
//!     it the task + an action, get back Success or Failure. They never learn
//!     how the action runs.
//!
//! What callers see: a one-line `execute_setup(task, action)` call that
//! yields a [`SetupOutcome`]; the caller then routes the outcome through its
//! own role-appropriate terminal sink. No caller touches the action's
//! internals, and the action never touches coordinator state.

use dynrunner_core::{Identifier, TaskInfo};

/// The classified result of running a setup task's action in-process.
///
/// The role wrapper maps this onto a terminal: `Success` →
/// `ClusterMutation::SetupCompleted` (the setup-success terminal, counted in
/// the separate `setup_succeeded` bucket); `Failure` →
/// `ClusterMutation::TaskFailed { kind: NonRecoverable }` (the SAME terminal
/// the executor-death seam drives — a setup task is non-reassignable, so a
/// failed action is unrecoverable and its dependents cascade).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetupOutcome {
    /// The action completed cleanly.
    Success,
    /// The action failed; the string is the operator-facing reason carried
    /// onto the `TaskFailed` terminal.
    Failure(String),
}

/// The EXECUTION SEAM for the setup-task primitive (P2): run the action a
/// setup task represents and return success/failure.
///
/// For the primitive this is a NO-OP SUCCESS: the primitive owns the
/// assignment→exec→terminal lifecycle, not the definition of what a setup
/// task does. A setup task that reaches its in-process executor with no
/// further action wired SUCCEEDS — the framework's flagged
/// staging-via-setup-tasks path and consumer setup tasks supply their real
/// action by composing over this seam (they will route their concrete work
/// — a per-file upload, a toolchain build — through the same
/// `TaskInfo → Result<(), String>` shape the role wrappers call). Keeping the
/// default a clean success makes the primitive correct and testable on its
/// own (the failure path is exercised by injecting a failing action through
/// [`execute_setup`], and by the executor-death seam) without P2 reaching
/// into "what a setup task does".
pub fn run_setup_action<I: Identifier>(_task: &TaskInfo<I>) -> Result<(), String> {
    Ok(())
}

/// Run `action` against `task` and classify the result. The role wrappers
/// call this with [`run_setup_action`] (production) or an injected closure
/// (tests exercising the failure path); the wrapper then routes the
/// [`SetupOutcome`] through its own terminal sink.
pub fn execute_setup<I, F>(task: &TaskInfo<I>, action: F) -> SetupOutcome
where
    I: Identifier,
    F: FnOnce(&TaskInfo<I>) -> Result<(), String>,
{
    match action(task) {
        Ok(()) => SetupOutcome::Success,
        Err(reason) => SetupOutcome::Failure(reason),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dynrunner_core::{
        PhaseId, RunnerIdentifier, SoftPreferredSecondaries, TaskKind, TypeId,
    };
    use std::path::PathBuf;

    fn setup_task(name: &str, affinity: Option<&str>) -> TaskInfo<RunnerIdentifier> {
        TaskInfo {
            path: PathBuf::from(format!("/tasks/{name}")),
            size: 0,
            identifier: RunnerIdentifier::from(name),
            phase_id: PhaseId::from("p0"),
            type_id: TypeId::from("t0"),
            affinity_id: None,
            payload: serde_json::Value::Null,
            task_id: name.into(),
            task_depends_on: Vec::new(),
            preferred_secondaries: SoftPreferredSecondaries::default(),
            preferred_version: Default::default(),
            kind: TaskKind::Setup,
            setup_affinity: affinity.map(str::to_string),
            resolved_path: None,
        }
    }

    #[test]
    fn primitive_action_is_no_op_success() {
        // The setup-task PRIMITIVE: with no further action wired, a setup
        // task that reaches its in-process executor SUCCEEDS.
        let task = setup_task("setup-a", Some("member-1"));
        assert_eq!(run_setup_action(&task), Ok(()));
        let outcome = execute_setup(&task, run_setup_action);
        assert_eq!(outcome, SetupOutcome::Success);
    }

    #[test]
    fn failing_action_yields_failure_outcome_with_reason() {
        // The failure path: an injected failing action classifies to a
        // Failure carrying the reason the role wrapper puts on the
        // non-recoverable terminal.
        let task = setup_task("setup-b", None);
        let outcome = execute_setup(&task, |_| Err("boom".to_string()));
        assert_eq!(outcome, SetupOutcome::Failure("boom".to_string()));
    }
}
