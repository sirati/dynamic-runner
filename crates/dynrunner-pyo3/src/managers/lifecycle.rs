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
use pyo3::types::PyDict;

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

/// Fire `task_definition.on_run_start(source_dir, output_dir, args[,
/// primary_handle])` synchronously under the GIL. Any exception raised
/// by the Python callback propagates: the run hasn't started yet, so
/// the consumer's setup failure is fatal.
///
/// `primary_handle` is the optional in-flight runtime handle the
/// modern task signature consumes. Caller policy decides who supplies
/// it:
/// - Primary-side dispatchers (`run_primary`, `run_distributed`, the
///   SLURM `drive_rust_primary`) pass `Some(coord.handle())` so the
///   task can drive `spawn_tasks(...)` from inside `on_run_start`.
/// - Secondary-side dispatchers (`run_secondary`) pass `None`; the
///   secondary holds no handle and the kwarg's omission keeps the
///   call shape positional-compatible.
///
/// # Backward compatibility
///
/// When `primary_handle` is `Some`, the call is invoked with the
/// `primary_handle=` kwarg. Legacy tasks whose `on_run_start`
/// signature does NOT accept the kwarg raise `TypeError: ... got an
/// unexpected keyword argument 'primary_handle'` from CPython's
/// arg-binding layer. That specific shape is detected by inspecting
/// the exception message for `"primary_handle"`; on match, we retry
/// without the kwarg so legacy tasks keep working. Other `TypeError`s
/// (raised from inside the task body) propagate unchanged — the
/// substring guard is the load-bearing filter that prevents
/// double-invocation of side-effect-bearing user code.
///
/// When `primary_handle` is `None`, the call goes through the
/// positional-only path with no compatibility dance.
pub(crate) fn fire_on_run_start(
    task_definition: &Bound<'_, PyAny>,
    source_dir: &str,
    output_dir: &str,
    task_args: &Bound<'_, PyAny>,
    primary_handle: Option<Py<PyAny>>,
) -> PyResult<()> {
    let py = task_definition.py();
    let positional = (source_dir, output_dir, task_args.clone());
    let Some(handle) = primary_handle else {
        return task_definition
            .call_method1("on_run_start", positional)
            .map(|_| ());
    };
    let kwargs = PyDict::new(py);
    kwargs.set_item("primary_handle", handle)?;
    match task_definition.call_method("on_run_start", positional.clone(), Some(&kwargs)) {
        Ok(_) => Ok(()),
        Err(e)
            if e.is_instance_of::<pyo3::exceptions::PyTypeError>(py)
                && e.value(py).to_string().contains("primary_handle") =>
        {
            // Legacy task signature without the kwarg. The arg-binding
            // layer rejected the call *before* the task body ran, so
            // retrying without the kwarg is safe (no double-execution of
            // user-visible side effects).
            task_definition
                .call_method1("on_run_start", positional)
                .map(|_| ())
        }
        Err(e) => Err(e),
    }
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

#[cfg(test)]
#[cfg(feature = "test-with-python")]
mod tests {
    //! Pins the `fire_on_run_start` kwarg contract:
    //!   1. Modern signatures receive `primary_handle` as a kwarg.
    //!   2. Legacy signatures fall back to the positional-only shape
    //!      without re-running the task body.
    //!   3. A `TypeError` raised from inside the task body — whose
    //!      message does NOT mention `primary_handle` — propagates
    //!      unchanged (the substring guard is the discriminator).
    //!
    //! Tests require an embedded CPython interpreter; gated behind the
    //! `test-with-python` feature. Invoke as:
    //!   `cargo test -p dynrunner-pyo3 --lib --no-default-features \
    //!        --features test-with-python lifecycle`
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static MODULE_COUNTER: AtomicUsize = AtomicUsize::new(0);

    /// Compile a one-off `mock_task_<n>` Python module exposing a
    /// recording `Task` class with the given `on_run_start` body.
    /// Returns the instance + the module globals dict so tests can
    /// inspect any recorded state afterwards.
    fn make_task(on_run_start_src: &str) -> (Py<PyAny>, Py<PyAny>) {
        let nonce = MODULE_COUNTER.fetch_add(1, Ordering::SeqCst);
        let module_name = format!("mock_lifecycle_task_{nonce}");
        let file_name = format!("{module_name}.py");
        let body = format!(
            "calls = []\n\
             class Task:\n    {}\n",
            on_run_start_src.replace('\n', "\n    "),
        );
        Python::attach(|py| {
            let module = PyModule::from_code(
                py,
                std::ffi::CString::new(body).unwrap().as_c_str(),
                std::ffi::CString::new(file_name).unwrap().as_c_str(),
                std::ffi::CString::new(module_name).unwrap().as_c_str(),
            )
            .expect("compile mock task module");
            let cls = module.getattr("Task").unwrap();
            let instance = cls.call0().unwrap().unbind();
            let globals = module.dict().unbind().into_any();
            (instance, globals)
        })
    }

    /// Modern task signature: `on_run_start(self, source_dir,
    /// output_dir, args, primary_handle=None)` records that the kwarg
    /// reached it. A `Py<PyAny>` sentinel substitutes for a real
    /// `PrimaryHandle` here — the bridge only forwards opaque
    /// `Py<PyAny>` so the test does not need a live handle.
    #[test]
    fn fire_on_run_start_passes_primary_handle_to_modern_signature() {
        let (task_obj, globals) = make_task(
            "def on_run_start(self, source_dir, output_dir, args, primary_handle=None):\n        \
                 calls.append((source_dir, output_dir, args, primary_handle))",
        );
        Python::attach(|py| {
            let task = task_obj.bind(py);
            // Use a Python int as the opaque handle sentinel.
            let sentinel: Py<PyAny> = 42i64.into_pyobject(py).unwrap().into_any().unbind();
            let args = py.None();
            fire_on_run_start(
                task,
                "/src",
                "/out",
                args.bind(py),
                Some(sentinel),
            )
            .expect("modern signature accepts primary_handle kwarg");
            let g = globals.bind(py);
            let calls = g
                .cast::<PyDict>()
                .unwrap()
                .get_item("calls")
                .unwrap()
                .unwrap();
            let list = calls.cast::<pyo3::types::PyList>().unwrap();
            assert_eq!(list.len(), 1);
            let entry = list.get_item(0).unwrap();
            let tuple = entry.cast::<pyo3::types::PyTuple>().unwrap();
            let captured_handle: i64 = tuple.get_item(3).unwrap().extract().unwrap();
            assert_eq!(captured_handle, 42, "kwarg value reached the task body");
        });
    }

    /// Legacy task signature: no `primary_handle` parameter. CPython's
    /// arg-binder raises `TypeError: ... got an unexpected keyword
    /// argument 'primary_handle'` *before* the body runs; the bridge
    /// must detect that, retry positional-only, and the body must run
    /// exactly once.
    #[test]
    fn fire_on_run_start_falls_back_to_positional_for_legacy_signature() {
        let (task_obj, globals) = make_task(
            "def on_run_start(self, source_dir, output_dir, args):\n        \
                 calls.append((source_dir, output_dir, args))",
        );
        Python::attach(|py| {
            let task = task_obj.bind(py);
            let sentinel: Py<PyAny> = 7i64.into_pyobject(py).unwrap().into_any().unbind();
            let args = py.None();
            fire_on_run_start(
                task,
                "/src",
                "/out",
                args.bind(py),
                Some(sentinel),
            )
            .expect("legacy signature falls back without surfacing the TypeError");
            let g = globals.bind(py);
            let calls = g
                .cast::<PyDict>()
                .unwrap()
                .get_item("calls")
                .unwrap()
                .unwrap();
            let list = calls.cast::<pyo3::types::PyList>().unwrap();
            assert_eq!(
                list.len(),
                1,
                "the body runs exactly once after the kwarg-binding TypeError",
            );
        });
    }

    /// A `TypeError` raised from INSIDE the task body whose message
    /// does not mention `primary_handle` must propagate unchanged —
    /// the substring guard is what distinguishes the kwarg-binding
    /// failure (safe to retry) from a real user-code error (must
    /// surface to abort the run).
    #[test]
    fn fire_on_run_start_propagates_unrelated_type_error() {
        let (task_obj, _globals) = make_task(
            "def on_run_start(self, source_dir, output_dir, args, primary_handle=None):\n        \
                 raise TypeError('something else broke')",
        );
        Python::attach(|py| {
            let task = task_obj.bind(py);
            let sentinel: Py<PyAny> = 1i64.into_pyobject(py).unwrap().into_any().unbind();
            let args = py.None();
            let err = fire_on_run_start(
                task,
                "/src",
                "/out",
                args.bind(py),
                Some(sentinel),
            )
            .expect_err("unrelated TypeError must propagate");
            assert!(err.is_instance_of::<pyo3::exceptions::PyTypeError>(py));
            assert!(err.value(py).to_string().contains("something else broke"));
        });
    }

    /// `None` for `primary_handle` skips the kwarg dance entirely
    /// and calls the legacy positional-only shape. Used by
    /// secondary-side dispatchers that have no handle to supply.
    #[test]
    fn fire_on_run_start_omits_kwarg_when_handle_is_none() {
        let (task_obj, globals) = make_task(
            "def on_run_start(self, source_dir, output_dir, args):\n        \
                 calls.append((source_dir, output_dir, args))",
        );
        Python::attach(|py| {
            let task = task_obj.bind(py);
            let args = py.None();
            fire_on_run_start(task, "/src", "/out", args.bind(py), None)
                .expect("positional-only call against legacy signature");
            let g = globals.bind(py);
            let calls = g
                .cast::<PyDict>()
                .unwrap()
                .get_item("calls")
                .unwrap()
                .unwrap();
            let list = calls.cast::<pyo3::types::PyList>().unwrap();
            assert_eq!(list.len(), 1);
        });
    }
}
