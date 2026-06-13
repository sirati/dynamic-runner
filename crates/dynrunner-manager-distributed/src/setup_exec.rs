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

/// The ASYNC execution path the three role wrappers (primary self-exec,
/// secondary twin, observer twin) call for EVERY assigned setup task —
/// #336 P1's single entry point, layered cleanly over the #489 primitive.
///
/// The dispatch is by the task's ACTION shape, owned here so no role wrapper
/// learns it:
///   * The task carries an [`dynrunner_core::UploadFileRef`] AND an
///     `UploadAction` is registered ⇒ perform the upload (with a bounded
///     OUTER retry on a transient the provider could not absorb — see
///     [`crate::upload_action::UPLOAD_OUTER_RETRIES`]; the per-blob retry
///     lives in the provider, owner decision 2026-06-14). Provider
///     `Ok` ⇒ `Success`; `Permanent` ⇒ `Failure`; `Transient` exhausted ⇒
///     `Failure`.
///   * The task carries an `UploadFileRef` but NO action is registered ⇒
///     `Failure` (the executor was asked to upload but has no uploader; this
///     is a wiring error, surfaced loudly rather than silently no-op'd).
///   * The task carries NO ref ⇒ the unchanged #489 NO-OP success (the
///     pre-staged / mode-2 gate): `execute_setup(task, run_setup_action)`.
///
/// `action` is the coordinator's `UploadActionHandle` (`Option<Arc<…>>`); it
/// is consulted ONLY when the task carries a ref, so a no-ref task succeeds
/// regardless of whether an uploader is registered.
pub async fn execute_setup_with_upload<I>(
    task: &TaskInfo<I>,
    action: &crate::upload_action::UploadActionHandle,
) -> SetupOutcome
where
    I: Identifier,
{
    let Some(file) = task.upload_file.as_ref() else {
        // No upload ref: the #489 no-op gate, byte-for-byte unchanged.
        return execute_setup(task, run_setup_action);
    };
    let Some(uploader) = action.as_ref() else {
        return SetupOutcome::Failure(format!(
            "setup task '{}' carries an upload-file ref ({}) but no upload \
             action is registered on its executor member — wiring error",
            task.task_id,
            file.source.display(),
        ));
    };
    // Bounded OUTER retry: the provider owns the per-blob transient retry, so
    // this only re-attempts a WHOLE-action transient the provider could not
    // absorb. A permanent failure short-circuits immediately (no retry).
    let mut last_transient: Option<String> = None;
    for attempt in 0..=crate::upload_action::UPLOAD_OUTER_RETRIES {
        match uploader.upload(file).await {
            Ok(()) => return SetupOutcome::Success,
            Err(e) if e.is_transient() => {
                tracing::warn!(
                    target: "dynrunner_setup",
                    task_id = %task.task_id,
                    source = %file.source.display(),
                    attempt,
                    reason = %e.reason(),
                    "upload action hit a transient failure; re-attempting"
                );
                last_transient = Some(e.reason().to_string());
            }
            Err(e) => {
                return SetupOutcome::Failure(format!(
                    "upload of '{}' failed permanently: {}",
                    file.source.display(),
                    e.reason(),
                ));
            }
        }
    }
    SetupOutcome::Failure(format!(
        "upload of '{}' failed after {} transient attempts: {}",
        file.source.display(),
        crate::upload_action::UPLOAD_OUTER_RETRIES + 1,
        last_transient.unwrap_or_else(|| "unknown transient fault".to_string()),
    ))
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
            upload_file: None,
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

    // ── #336 P1: the upload-action execution path ──────────────────────

    use crate::upload_action::{UploadAction, UploadError, UPLOAD_OUTER_RETRIES};
    use dynrunner_core::UploadFileRef;
    use std::sync::{Arc, Mutex};

    /// A setup task carrying an upload-file ref (the upload-action case).
    fn upload_task(name: &str, source: &str) -> TaskInfo<RunnerIdentifier> {
        let mut t = setup_task(name, Some("submitter"));
        t.upload_file = Some(Box::new(UploadFileRef {
            source: PathBuf::from(source),
            dest: None,
        }));
        t
    }

    /// A stub `UploadAction` that records every file it was asked to upload
    /// and returns a SCRIPTED sequence of results (one per call). The script
    /// is consumed front-to-back; a call past the end defaults to `Ok`.
    /// Uses `Mutex` (not `RefCell`) because the trait is `Send + Sync` (so a
    /// real `Arc<dyn UploadAction>` survives the relocation handoff) — the
    /// stub honours that bound.
    struct StubUploader {
        calls: Mutex<Vec<PathBuf>>,
        script: Mutex<std::collections::VecDeque<Result<(), UploadError>>>,
    }

    impl StubUploader {
        fn new(script: Vec<Result<(), UploadError>>) -> Arc<Self> {
            Arc::new(Self {
                calls: Mutex::new(Vec::new()),
                script: Mutex::new(script.into()),
            })
        }
        fn call_count(&self) -> usize {
            self.calls.lock().unwrap().len()
        }
    }

    #[async_trait::async_trait(?Send)]
    impl UploadAction for StubUploader {
        async fn upload(&self, file: &UploadFileRef) -> Result<(), UploadError> {
            self.calls.lock().unwrap().push(file.source.clone());
            // Pop the next scripted result; default to Ok when exhausted.
            self.script.lock().unwrap().pop_front().unwrap_or(Ok(()))
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn no_upload_ref_keeps_the_no_op_success_gate() {
        // A setup task WITHOUT an upload ref no-op-succeeds — the #489
        // mode-2 gate, unchanged — and the registered action is NEVER
        // consulted (even when one is present).
        let task = setup_task("gate", Some("submitter"));
        let uploader = StubUploader::new(vec![Err(UploadError::Permanent("must not run".into()))]);
        let handle: crate::upload_action::UploadActionHandle = Some(uploader.clone());
        let outcome = execute_setup_with_upload(&task, &handle).await;
        assert_eq!(outcome, SetupOutcome::Success);
        assert_eq!(
            uploader.call_count(),
            0,
            "a no-ref task must never invoke the upload action"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn upload_ref_fires_callback_and_succeeds() {
        // A setup task carrying an upload ref invokes the action with THAT
        // file's source and succeeds on a clean upload.
        let task = upload_task("up-a", "/src/libfoo.a");
        let uploader = StubUploader::new(vec![Ok(())]);
        let handle: crate::upload_action::UploadActionHandle = Some(uploader.clone());
        let outcome = execute_setup_with_upload(&task, &handle).await;
        assert_eq!(outcome, SetupOutcome::Success);
        assert_eq!(uploader.call_count(), 1);
        assert_eq!(
            uploader.calls.lock().unwrap().as_slice(),
            &[PathBuf::from("/src/libfoo.a")],
            "the action is invoked with the task's upload-file source"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn transient_failures_retry_then_succeed() {
        // A transient fault the provider could not absorb is re-attempted by
        // the bounded OUTER retry; an eventual success terminates Success.
        let task = upload_task("up-b", "/src/x");
        let uploader = StubUploader::new(vec![
            Err(UploadError::Transient("blip 1".into())),
            Err(UploadError::Transient("blip 2".into())),
            Ok(()),
        ]);
        let handle: crate::upload_action::UploadActionHandle = Some(uploader.clone());
        let outcome = execute_setup_with_upload(&task, &handle).await;
        assert_eq!(outcome, SetupOutcome::Success);
        assert_eq!(
            uploader.call_count(),
            3,
            "two transient retries then success = three attempts"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn transient_failures_exhaust_then_fail() {
        // Transient faults beyond the bounded outer-retry budget terminate
        // Failure (NonRecoverable downstream). Total attempts =
        // UPLOAD_OUTER_RETRIES + 1.
        let always_transient: Vec<_> = (0..(UPLOAD_OUTER_RETRIES as usize + 5))
            .map(|i| Err(UploadError::Transient(format!("blip {i}"))))
            .collect();
        let task = upload_task("up-c", "/src/y");
        let uploader = StubUploader::new(always_transient);
        let handle: crate::upload_action::UploadActionHandle = Some(uploader.clone());
        let outcome = execute_setup_with_upload(&task, &handle).await;
        assert!(matches!(outcome, SetupOutcome::Failure(_)));
        assert_eq!(
            uploader.call_count(),
            UPLOAD_OUTER_RETRIES as usize + 1,
            "the outer retry caps at UPLOAD_OUTER_RETRIES + 1 attempts"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn permanent_failure_fails_without_retry() {
        // A permanent fault short-circuits immediately — NO outer retry.
        let task = upload_task("up-d", "/src/z");
        let uploader = StubUploader::new(vec![
            Err(UploadError::Permanent("source missing".into())),
            Ok(()), // would succeed on a (forbidden) retry
        ]);
        let handle: crate::upload_action::UploadActionHandle = Some(uploader.clone());
        let outcome = execute_setup_with_upload(&task, &handle).await;
        assert!(matches!(outcome, SetupOutcome::Failure(_)));
        assert_eq!(
            uploader.call_count(),
            1,
            "a permanent failure must not be retried"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn upload_ref_without_registered_action_is_a_wiring_failure() {
        // A task asks for an upload but the executor has no uploader: this
        // is a loud wiring error, NOT a silent no-op-success.
        let task = upload_task("up-e", "/src/w");
        let handle: crate::upload_action::UploadActionHandle = None;
        let outcome = execute_setup_with_upload(&task, &handle).await;
        match outcome {
            SetupOutcome::Failure(reason) => {
                assert!(
                    reason.contains("no upload action is registered"),
                    "the wiring-error reason must name the missing action; got: {reason}"
                );
            }
            other => panic!("expected a wiring Failure, got {other:?}"),
        }
    }
}
