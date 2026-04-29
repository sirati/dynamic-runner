//! Python `TaskDefinition` lifecycle-hook bridge.
//!
//! The manager core (`LocalManager`, `PrimaryCoordinator`) accepts
//! `FnMut` closures for `on_phase_start` / `on_phase_end`; the runner's
//! top-level run wrapper additionally invokes `on_run_start` /
//! `on_run_end` synchronously around the manager run. Every Python
//! manager pyclass needs the same pair of GIL-reacquiring closures, so
//! single-source them here.
//!
//! Error policy:
//! - `on_phase_start` / `on_phase_end` exceptions log and continue.
//!   Phase boundaries are not the place to surface fatal errors;
//!   exceptions out of the consumer's hook are a consumer bug, not a
//!   reason to abort an in-flight pool drain.
//! - `on_run_start` exceptions abort the run (see
//!   `run::run_local`/`run_primary`/`run_distributed`): the consumer's
//!   setup hasn't completed; dispatching items would race with
//!   half-built resources.
//! - `on_run_end` exceptions log and continue (the run is over; nothing
//!   to recover).

use pyo3::prelude::*;

use dynrunner_core::PhaseId;

/// Build an `on_phase_start` closure that re-acquires the GIL and calls
/// `task_definition.on_phase_start(phase_id)`.
///
/// The returned closure is `'static + Send` so it can be passed to the
/// manager's `process_binaries` / `run` whose closure types require both
/// (the manager runs the closure on its own LocalSet under
/// `py.detach`, off the GIL thread).
pub(crate) fn make_on_phase_start(
    task_definition: Py<PyAny>,
) -> impl FnMut(&PhaseId) + Send + 'static {
    move |phase_id: &PhaseId| {
        Python::attach(|py| {
            if let Err(e) = task_definition
                .bind(py)
                .call_method1("on_phase_start", (phase_id.as_str(),))
            {
                tracing::warn!(
                    error = %e,
                    phase = %phase_id,
                    "TaskDefinition.on_phase_start raised; continuing"
                );
            }
        });
    }
}

/// Build an `on_phase_end` closure that re-acquires the GIL and calls
/// `task_definition.on_phase_end(phase_id, completed, failed)`.
pub(crate) fn make_on_phase_end(
    task_definition: Py<PyAny>,
) -> impl FnMut(&PhaseId, u32, u32) + Send + 'static {
    move |phase_id: &PhaseId, completed: u32, failed: u32| {
        Python::attach(|py| {
            if let Err(e) = task_definition.bind(py).call_method1(
                "on_phase_end",
                (phase_id.as_str(), completed, failed),
            ) {
                tracing::warn!(
                    error = %e,
                    phase = %phase_id,
                    completed,
                    failed,
                    "TaskDefinition.on_phase_end raised; continuing"
                );
            }
        });
    }
}

/// Fire `task_definition.on_run_start(source_dir, output_dir, args)`
/// synchronously under the GIL. Any exception raised by the Python
/// callback propagates: the run hasn't started yet, so the consumer's
/// setup failure is fatal.
pub(crate) fn fire_on_run_start(
    task_definition: &Bound<'_, PyAny>,
    source_dir: &str,
    output_dir: &str,
    task_args: &Bound<'_, PyAny>,
) -> PyResult<()> {
    task_definition
        .call_method1("on_run_start", (source_dir, output_dir, task_args.clone()))
        .map(|_| ())
}

/// Fire `task_definition.on_run_end(success)` synchronously under the
/// GIL. Exceptions are logged and swallowed — the run has already
/// terminated; there is no recovery, and propagating would mask the
/// real outcome (success or the manager's own error).
pub(crate) fn fire_on_run_end(task_definition: &Bound<'_, PyAny>, success: bool) {
    if let Err(e) = task_definition.call_method1("on_run_end", (success,)) {
        tracing::warn!(
            error = %e,
            success,
            "TaskDefinition.on_run_end raised; ignoring (run already complete)"
        );
    }
}
